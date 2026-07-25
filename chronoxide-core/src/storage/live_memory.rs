use std::array;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const LIVE_MEMORY_CLASS_COUNT: usize = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LiveMemoryClass {
    CatalogRowsAndSymbols = 0,
    LivePostings = 1,
    FrozenPayload = 2,
    RunDescriptors = 3,
    PersistentRoots = 4,
    CandidateScratch = 5,
    SealedInventory = 6,
    QueryRetention = 7,
    Other = 8,
}

impl LiveMemoryClass {
    const ALL: [Self; LIVE_MEMORY_CLASS_COUNT] = [
        Self::CatalogRowsAndSymbols,
        Self::LivePostings,
        Self::FrozenPayload,
        Self::RunDescriptors,
        Self::PersistentRoots,
        Self::CandidateScratch,
        Self::SealedInventory,
        Self::QueryRetention,
        Self::Other,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveMemoryStats {
    pub limit_bytes: u64,
    pub charged_bytes: u64,
    pub peak_charged_bytes: u64,
    pub mutable_tail_used_bytes: u64,
    pub mutable_tail_capacity_bytes: u64,
    pub by_class: [(LiveMemoryClass, u64); LIVE_MEMORY_CLASS_COUNT],
}

#[derive(Debug)]
pub struct LiveMemoryGovernor {
    limit_bytes: u64,
    charged_bytes: AtomicU64,
    peak_charged_bytes: AtomicU64,
    mutable_tail_used_bytes: AtomicU64,
    mutable_tail_capacity_bytes: AtomicU64,
    by_class: [AtomicU64; LIVE_MEMORY_CLASS_COUNT],
}

impl LiveMemoryGovernor {
    pub fn new(limit_bytes: u64) -> io::Result<Arc<Self>> {
        if limit_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "live memory admission limit must be greater than zero",
            ));
        }
        Ok(Arc::new(Self {
            limit_bytes,
            charged_bytes: AtomicU64::new(0),
            peak_charged_bytes: AtomicU64::new(0),
            mutable_tail_used_bytes: AtomicU64::new(0),
            mutable_tail_capacity_bytes: AtomicU64::new(0),
            by_class: array::from_fn(|_| AtomicU64::new(0)),
        }))
    }

    pub fn try_charge(
        self: &Arc<Self>,
        class: LiveMemoryClass,
        bytes: u64,
    ) -> io::Result<LiveMemoryCharge> {
        let mut current = self.charged_bytes.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                io::Error::new(io::ErrorKind::OutOfMemory, "live memory charge overflow")
            })?;
            if next > self.limit_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!(
                        "live memory admission exceeded: current={current} requested={bytes} limit={}",
                        self.limit_bytes
                    ),
                ));
            }
            match self.charged_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.by_class[class.index()].fetch_add(bytes, Ordering::AcqRel);
                    self.peak_charged_bytes.fetch_max(next, Ordering::AcqRel);
                    return Ok(LiveMemoryCharge {
                        governor: Arc::clone(self),
                        class,
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub fn set_mutable_tail_bytes(&self, used_bytes: u64, capacity_bytes: u64) -> io::Result<()> {
        if used_bytes > capacity_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "live mutable tail used bytes exceed capacity",
            ));
        }
        self.mutable_tail_used_bytes
            .store(used_bytes, Ordering::Release);
        self.mutable_tail_capacity_bytes
            .store(capacity_bytes, Ordering::Release);
        Ok(())
    }

    pub fn stats(&self) -> LiveMemoryStats {
        LiveMemoryStats {
            limit_bytes: self.limit_bytes,
            charged_bytes: self.charged_bytes.load(Ordering::Acquire),
            peak_charged_bytes: self.peak_charged_bytes.load(Ordering::Acquire),
            mutable_tail_used_bytes: self.mutable_tail_used_bytes.load(Ordering::Acquire),
            mutable_tail_capacity_bytes: self.mutable_tail_capacity_bytes.load(Ordering::Acquire),
            by_class: array::from_fn(|index| {
                (
                    LiveMemoryClass::ALL[index],
                    self.by_class[index].load(Ordering::Acquire),
                )
            }),
        }
    }
}

#[derive(Debug)]
pub struct LiveMemoryCharge {
    governor: Arc<LiveMemoryGovernor>,
    class: LiveMemoryClass,
    bytes: u64,
}

impl LiveMemoryCharge {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn class(&self) -> LiveMemoryClass {
        self.class
    }

    pub fn reclassify(&mut self, class: LiveMemoryClass) {
        if class == self.class {
            return;
        }
        self.governor.by_class[self.class.index()].fetch_sub(self.bytes, Ordering::AcqRel);
        self.governor.by_class[class.index()].fetch_add(self.bytes, Ordering::AcqRel);
        self.class = class;
    }
}

impl Drop for LiveMemoryCharge {
    fn drop(&mut self) {
        self.governor.by_class[self.class.index()].fetch_sub(self.bytes, Ordering::AcqRel);
        self.governor
            .charged_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// Owns one physical allocation together with its one governor charge.
///
/// Cloning an `Arc<LiveMemoryPin<T>>` shares both value and charge, so view
/// generations do not double-count a physically shared page.
#[derive(Debug)]
pub struct LiveMemoryPin<T> {
    value: T,
    _charge: LiveMemoryCharge,
}

impl<T> LiveMemoryPin<T> {
    pub fn try_new(
        governor: &Arc<LiveMemoryGovernor>,
        class: LiveMemoryClass,
        bytes: u64,
        value: T,
    ) -> io::Result<Arc<Self>> {
        let charge = governor.try_charge(class, bytes)?;
        Ok(Arc::new(Self {
            value,
            _charge: charge,
        }))
    }

    pub fn get(&self) -> &T {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    #[test]
    fn charges_are_checked_reclassified_and_released() {
        let governor = LiveMemoryGovernor::new(100).unwrap();
        let mut first = governor
            .try_charge(LiveMemoryClass::CandidateScratch, 60)
            .unwrap();
        assert_eq!(
            governor
                .try_charge(LiveMemoryClass::FrozenPayload, 41)
                .unwrap_err()
                .kind(),
            io::ErrorKind::OutOfMemory
        );

        first.reclassify(LiveMemoryClass::PersistentRoots);
        let stats = governor.stats();
        assert_eq!(stats.charged_bytes, 60);
        assert_eq!(
            stats.by_class[LiveMemoryClass::CandidateScratch.index()].1,
            0
        );
        assert_eq!(
            stats.by_class[LiveMemoryClass::PersistentRoots.index()].1,
            60
        );

        drop(first);
        assert_eq!(governor.stats().charged_bytes, 0);
        assert_eq!(governor.stats().peak_charged_bytes, 60);
    }

    #[test]
    fn arc_pins_charge_shared_physical_bytes_once() {
        let governor = LiveMemoryGovernor::new(100).unwrap();
        let pin = LiveMemoryPin::try_new(
            &governor,
            LiveMemoryClass::FrozenPayload,
            80,
            vec![1_u8; 80],
        )
        .unwrap();
        let clone = Arc::clone(&pin);

        assert_eq!(governor.stats().charged_bytes, 80);
        drop(pin);
        assert_eq!(governor.stats().charged_bytes, 80);
        assert_eq!(clone.get().len(), 80);
        drop(clone);
        assert_eq!(governor.stats().charged_bytes, 0);
    }

    #[test]
    fn concurrent_admission_never_exceeds_the_limit() {
        let governor = LiveMemoryGovernor::new(64).unwrap();
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let governor = Arc::clone(&governor);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                governor
                    .try_charge(LiveMemoryClass::QueryRetention, 16)
                    .ok()
            }));
        }
        barrier.wait();
        let charges: Vec<_> = threads
            .into_iter()
            .filter_map(|thread| thread.join().unwrap())
            .collect();

        assert_eq!(charges.len(), 4);
        assert_eq!(governor.stats().charged_bytes, 64);
        drop(charges);
        assert_eq!(governor.stats().charged_bytes, 0);
    }

    #[test]
    fn mutable_tail_accounting_is_separate_and_validated() {
        let governor = LiveMemoryGovernor::new(1).unwrap();
        governor.set_mutable_tail_bytes(10, 20).unwrap();
        let stats = governor.stats();
        assert_eq!(stats.charged_bytes, 0);
        assert_eq!(stats.mutable_tail_used_bytes, 10);
        assert_eq!(stats.mutable_tail_capacity_bytes, 20);
        assert!(governor.set_mutable_tail_bytes(21, 20).is_err());
    }
}
