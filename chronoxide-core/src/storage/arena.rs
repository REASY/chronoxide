use std::io;
use std::{error, fmt};

#[derive(Debug, Clone, Copy)]
pub(crate) struct BufferRef {
    page: u32,
    offset: u32,
    len: u32,
}

impl BufferRef {
    pub(crate) fn len(self) -> usize {
        self.len as usize
    }
}

/// Read-only access to encoded head-block payloads.
///
/// Both mutable writer arenas and compact frozen arenas implement this
/// interface. Reads are checked because a `BufferRef` is descriptor data:
/// malformed or stale descriptors must be reported as invalid data rather
/// than panic or expose unused page capacity.
pub(crate) trait ArenaRead {
    fn slice(&self, buf_ref: BufferRef) -> io::Result<&[u8]>;
}

#[derive(Debug)]
struct ArenaPage {
    buf: Vec<u8>,
    used: usize,
}

impl ArenaPage {
    fn try_new(size: usize) -> io::Result<Self> {
        let mut buf = Vec::new();
        buf.try_reserve_exact(size).map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to reserve head block arena page: {error}"),
            )
        })?;
        // `try_reserve_exact` above makes this zero-fill allocation-free.
        buf.resize(size, 0);
        Ok(Self { buf, used: 0 })
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.used)
    }
}

#[derive(Debug)]
pub(crate) struct BlockArena {
    pages: Vec<ArenaPage>,
    /// Fixed page size, or the next geometric page size in adaptive mode.
    page_size: usize,
    max_page_size: usize,
    adaptive: bool,
    #[cfg(test)]
    fail_pair_write_call: Option<usize>,
}

impl BlockArena {
    /// Constructs the historical fixed-page arena.
    ///
    /// This constructor intentionally retains the disabled head's exact page
    /// sizing policy: every ordinary page is `page_size`, while an individual
    /// oversized write receives one `len`-sized page.
    pub(crate) fn new(page_size: usize) -> Self {
        let page_size = page_size.max(1);
        Self {
            pages: Vec::new(),
            page_size,
            max_page_size: page_size,
            adaptive: false,
            #[cfg(test)]
            fail_pair_write_call: None,
        }
    }

    /// Constructs a geometric arena for short-lived live head fragments.
    pub(crate) fn new_geometric(initial_page_size: usize, max_page_size: usize) -> Self {
        let max_page_size = max_page_size.max(1);
        Self {
            pages: Vec::new(),
            page_size: initial_page_size.max(1).min(max_page_size),
            max_page_size,
            adaptive: true,
            #[cfg(test)]
            fail_pair_write_call: None,
        }
    }

    pub(crate) fn uses_fallible_live_writes(&self) -> bool {
        self.adaptive
    }

    /// Checked allocation used at fallible integration boundaries.
    ///
    /// An error leaves the arena byte-for-byte unchanged. The existing
    /// infallible `write` API remains the hot-path contract for valid internal
    /// sizes, but delegates here so representability is never a debug-only
    /// condition or a narrowing cast.
    fn try_alloc(&mut self, len: usize) -> io::Result<BufferRef> {
        let len = len.max(1);
        u32::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "head block allocation length exceeds BufferRef",
            )
        })?;

        let needs_page = match self.pages.last() {
            Some(page) => {
                if page.used > page.buf.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "head block arena page used length exceeds its buffer",
                    ));
                }
                page.remaining() < len
            }
            None => true,
        };
        if needs_page {
            let page_index = self.pages.len();
            u32::try_from(page_index).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "head block arena page index exceeds BufferRef",
                )
            })?;
            let size = self.page_size.max(len);
            let buf_ref = checked_buffer_ref(page_index, 0, len)?;
            let next_page_size = if self.adaptive {
                self.page_size
                    .checked_mul(2)
                    .unwrap_or(self.max_page_size)
                    .min(self.max_page_size)
            } else {
                self.page_size
            };

            // Allocate the page and the directory slot before publishing
            // either. Failure leaves page membership, used offsets, and the
            // geometric successor unchanged.
            let page = ArenaPage::try_new(size)?;
            self.pages.try_reserve(1).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("failed to reserve head block arena page directory: {error}"),
                )
            })?;
            self.pages.push(page);
            self.page_size = next_page_size;
            self.pages[page_index].used = len;
            return Ok(buf_ref);
        }

        let page_index = self.pages.len() - 1;
        let offset = self.pages[page_index].used;
        let buf_ref = checked_buffer_ref(page_index, offset, len)?;
        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "head block allocation overflows",
            )
        })?;
        debug_assert!(end <= self.pages[page_index].buf.len());
        self.pages[page_index].used = end;
        Ok(buf_ref)
    }

    pub(crate) fn write(&mut self, data: &[u8]) -> BufferRef {
        self.try_write(data)
            .unwrap_or_else(|error| panic!("head block arena write is not representable: {error}"))
    }

    /// Checked write whose error path does not allocate or advance a page.
    pub(crate) fn try_write(&mut self, data: &[u8]) -> io::Result<BufferRef> {
        if data.is_empty() {
            return self.try_alloc(0);
        }
        let buf_ref = self.try_alloc(data.len())?;
        self.slice_mut(buf_ref).copy_from_slice(data);
        Ok(buf_ref)
    }

    /// Writes the two physical buffers of one live block transactionally.
    ///
    /// A failure on either write restores page membership, the preceding
    /// page's used prefix, and the geometric successor. Bytes copied beyond
    /// that restored used prefix are unreachable and overwritten by a retry.
    pub(crate) fn try_write_pair(
        &mut self,
        first: &[u8],
        second: &[u8],
    ) -> io::Result<(BufferRef, BufferRef)> {
        let page_count = self.pages.len();
        let last_used = self.pages.last().map_or(0, |page| page.used);
        let next_page_size = self.page_size;

        let first_ref = match self.try_pair_write(1, first) {
            Ok(buf_ref) => buf_ref,
            Err(error) => {
                self.rollback_pair_write(page_count, last_used, next_page_size);
                return Err(error);
            }
        };
        let second_ref = match self.try_pair_write(2, second) {
            Ok(buf_ref) => buf_ref,
            Err(error) => {
                self.rollback_pair_write(page_count, last_used, next_page_size);
                return Err(error);
            }
        };
        Ok((first_ref, second_ref))
    }

    fn try_pair_write(&mut self, _call: usize, data: &[u8]) -> io::Result<BufferRef> {
        #[cfg(test)]
        if self.fail_pair_write_call == Some(_call) {
            self.fail_pair_write_call = None;
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("injected live arena pair-write allocation failure at write {_call}"),
            ));
        }
        self.try_write(data)
    }

    fn rollback_pair_write(&mut self, page_count: usize, last_used: usize, next_page_size: usize) {
        self.pages.truncate(page_count);
        if let Some(page) = self.pages.last_mut() {
            page.used = last_used;
        }
        self.page_size = next_page_size;
    }

    pub(crate) fn slice_mut(&mut self, buf_ref: BufferRef) -> &mut [u8] {
        let page = &mut self.pages[buf_ref.page as usize];
        let offset = buf_ref.offset as usize;
        let len = buf_ref.len as usize;
        &mut page.buf[offset..offset + len]
    }

    pub(crate) fn total_capacity_bytes(&self) -> usize {
        self.pages
            .iter()
            .fold(0usize, |acc, page| acc.saturating_add(page.buf.len()))
    }

    pub(crate) fn total_used_bytes(&self) -> usize {
        self.pages
            .iter()
            .fold(0usize, |acc, page| acc.saturating_add(page.used))
    }

    pub(crate) fn slack_bytes(&self) -> usize {
        self.total_capacity_bytes()
            .saturating_sub(self.total_used_bytes())
    }

    pub(crate) fn page_count(&self) -> usize {
        self.pages.len()
    }

    #[cfg(test)]
    pub(crate) fn page_capacities(&self) -> Vec<usize> {
        self.pages.iter().map(|page| page.buf.len()).collect()
    }

    #[cfg(test)]
    pub(crate) fn next_page_size(&self) -> usize {
        self.page_size
    }

    #[cfg(test)]
    pub(crate) fn fail_pair_write_on_call(&mut self, call: usize) {
        assert!((1..=2).contains(&call));
        self.fail_pair_write_call = Some(call);
    }

    /// Attempts to consume the mutable arena into exact-used immutable pages.
    ///
    /// Validation completes before copying. On validation failure the error
    /// retains the original arena, allowing the publication transaction to
    /// restore its mutable window without losing encoded bytes.
    #[allow(dead_code, reason = "used by the opt-in live-query publication path")]
    pub(crate) fn try_freeze(self) -> Result<FrozenBlockArena, FreezeArenaError> {
        let mut used_bytes = 0usize;
        for (page_index, page) in self.pages.iter().enumerate() {
            if u32::try_from(page_index).is_err() {
                return Err(FreezeArenaError::new(
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "head block arena page index exceeds BufferRef",
                    ),
                    self,
                ));
            }
            if page.used > page.buf.len() {
                return Err(FreezeArenaError::new(
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "head block arena page used length exceeds its buffer",
                    ),
                    self,
                ));
            }
            used_bytes = match used_bytes.checked_add(page.used) {
                Some(total) => total,
                None => {
                    return Err(FreezeArenaError::new(
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "head block arena used-byte accounting overflows",
                        ),
                        self,
                    ));
                }
            };
        }

        let mut pages = Vec::new();
        if let Err(error) = pages.try_reserve_exact(self.pages.len()) {
            return Err(FreezeArenaError::new(
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    format!("failed to reserve frozen arena page directory: {error}"),
                ),
                self,
            ));
        }
        for page in &self.pages {
            let mut exact = Vec::new();
            if let Err(error) = exact.try_reserve_exact(page.used) {
                return Err(FreezeArenaError::new(
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        format!("failed to reserve exact frozen arena page: {error}"),
                    ),
                    self,
                ));
            }
            exact.extend_from_slice(&page.buf[..page.used]);
            pages.push(exact.into_boxed_slice());
        }
        Ok(FrozenBlockArena {
            pages: pages.into_boxed_slice(),
            used_bytes,
        })
    }
}

impl ArenaRead for BlockArena {
    fn slice(&self, buf_ref: BufferRef) -> io::Result<&[u8]> {
        let page = self.pages.get(buf_ref.page as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "head block buffer references a missing arena page",
            )
        })?;
        checked_slice(&page.buf, page.used, buf_ref)
    }
}

/// Immutable arena used by published live-query fragments.
///
/// Every boxed page is exactly its used length, so allocated and used payload
/// accounting are identical and no mutable-page slack is retained.
#[derive(Debug)]
#[allow(dead_code, reason = "used by the opt-in live-query publication path")]
pub(crate) struct FrozenBlockArena {
    pages: Box<[Box<[u8]>]>,
    used_bytes: usize,
}

#[allow(dead_code, reason = "used by the opt-in live-query publication path")]
impl FrozenBlockArena {
    pub(crate) fn total_allocated_bytes(&self) -> usize {
        self.used_bytes
    }

    pub(crate) fn total_used_bytes(&self) -> usize {
        self.used_bytes
    }

    pub(crate) fn page_count(&self) -> usize {
        self.pages.len()
    }
}

/// A failed freeze together with the unchanged mutable arena.
///
/// Keeping the arena in the error makes publication rollback explicit and
/// prevents a validation failure from dropping the only copy of the payload.
#[allow(dead_code, reason = "used by the opt-in live-query publication path")]
pub(crate) struct FreezeArenaError {
    source: io::Error,
    arena: BlockArena,
}

#[allow(dead_code, reason = "used by the opt-in live-query publication path")]
impl FreezeArenaError {
    fn new(source: io::Error, arena: BlockArena) -> Self {
        Self { source, arena }
    }

    pub(crate) fn error(&self) -> &io::Error {
        &self.source
    }

    pub(crate) fn into_parts(self) -> (io::Error, BlockArena) {
        (self.source, self.arena)
    }
}

impl fmt::Debug for FreezeArenaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FreezeArenaError")
            .field("source", &self.source)
            .field("page_count", &self.arena.pages.len())
            .finish()
    }
}

impl fmt::Display for FreezeArenaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl error::Error for FreezeArenaError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.source)
    }
}

impl ArenaRead for FrozenBlockArena {
    fn slice(&self, buf_ref: BufferRef) -> io::Result<&[u8]> {
        let page = self.pages.get(buf_ref.page as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "head block buffer references a missing frozen arena page",
            )
        })?;
        checked_slice(page, page.len(), buf_ref)
    }
}

fn checked_slice(buf: &[u8], used: usize, buf_ref: BufferRef) -> io::Result<&[u8]> {
    let offset = buf_ref.offset as usize;
    let len = buf_ref.len as usize;
    let end = offset.checked_add(len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "head block buffer range overflows",
        )
    })?;
    if used > buf.len() || end > used {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "head block buffer range exceeds used arena bytes",
        ));
    }
    Ok(&buf[offset..end])
}

fn checked_buffer_ref(page: usize, offset: usize, len: usize) -> io::Result<BufferRef> {
    Ok(BufferRef {
        page: u32::try_from(page).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "head block arena page index exceeds BufferRef",
            )
        })?,
        offset: u32::try_from(offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "head block arena offset exceeds BufferRef",
            )
        })?,
        len: u32::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "head block arena length exceeds BufferRef",
            )
        })?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_policy_retains_identical_page_sizing() {
        let mut arena = BlockArena::new(8);
        arena.try_write(&[1; 8]).unwrap();
        arena.try_write(&[2]).unwrap();
        arena.try_write(&[3; 9]).unwrap();

        assert_eq!(arena.page_capacities(), vec![8, 8, 9]);
        assert_eq!(arena.next_page_size(), 8);
    }

    #[test]
    fn geometric_policy_grows_to_cap_and_stays_capped() {
        let mut arena = BlockArena::new_geometric(4, 16);
        arena.try_write(&[1; 4]).unwrap();
        arena.try_write(&[2]).unwrap();
        arena.try_write(&[3; 8]).unwrap();
        arena.try_write(&[4; 16]).unwrap();

        assert_eq!(arena.page_capacities(), vec![4, 8, 16, 16]);
        assert_eq!(arena.next_page_size(), 16);
    }

    #[test]
    fn geometric_oversized_page_does_not_skip_the_next_ordinary_size() {
        let mut arena = BlockArena::new_geometric(4, 16);
        arena.try_write(&[1; 9]).unwrap();
        assert_eq!(arena.page_capacities(), vec![9]);
        assert_eq!(arena.next_page_size(), 8);

        arena.try_write(&[2]).unwrap();
        assert_eq!(arena.page_capacities(), vec![9, 8]);
        assert_eq!(arena.next_page_size(), 16);
    }

    #[test]
    fn live_geometric_policy_reaches_the_four_mib_cap() {
        const INITIAL: usize = 16 * 1024;
        const MAX: usize = 4 * 1024 * 1024;
        let mut arena = BlockArena::new_geometric(INITIAL, MAX);
        let mut expected = Vec::new();
        let mut page_size = INITIAL;
        loop {
            arena.try_alloc(page_size).unwrap();
            expected.push(page_size);
            if page_size == MAX {
                break;
            }
            page_size *= 2;
        }
        arena.try_alloc(MAX).unwrap();
        expected.push(MAX);

        assert_eq!(arena.page_capacities(), expected);
        assert_eq!(arena.next_page_size(), MAX);
        assert!(arena.page_capacities().iter().all(|size| *size <= MAX));
    }

    #[test]
    fn failed_page_allocation_preserves_existing_bytes_and_growth_state() {
        let mut arena = BlockArena::new_geometric(4, usize::MAX);
        let existing = arena.try_write(&[1, 2, 3, 4]).unwrap();
        arena.page_size = usize::MAX;

        let error = arena
            .try_write(&[5])
            .expect_err("capacity overflow must be reported instead of panicking");

        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(arena.page_count(), 1);
        assert_eq!(arena.total_capacity_bytes(), 4);
        assert_eq!(arena.total_used_bytes(), 4);
        assert_eq!(arena.next_page_size(), usize::MAX);
        assert_eq!(arena.slice(existing).unwrap(), &[1, 2, 3, 4]);
    }

    #[test]
    fn pair_write_first_allocation_failure_is_non_mutating_and_retryable() {
        let mut arena = BlockArena::new_geometric(4, 16);
        let existing = arena.try_write(&[1, 2]).unwrap();
        let baseline_pages = arena.page_capacities();
        let baseline_used = arena.total_used_bytes();
        let baseline_next = arena.next_page_size();

        arena.fail_pair_write_on_call(1);
        let error = arena
            .try_write_pair(&[3, 4], &[5, 6])
            .expect_err("injected first write must fail");
        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(arena.page_capacities(), baseline_pages);
        assert_eq!(arena.total_used_bytes(), baseline_used);
        assert_eq!(arena.next_page_size(), baseline_next);
        assert_eq!(arena.slice(existing).unwrap(), &[1, 2]);

        let (first, second) = arena.try_write_pair(&[3, 4], &[5, 6]).unwrap();
        assert_eq!(arena.slice(first).unwrap(), &[3, 4]);
        assert_eq!(arena.slice(second).unwrap(), &[5, 6]);
    }

    #[test]
    fn pair_write_second_allocation_failure_rolls_back_new_page_and_retries_exactly() {
        let mut arena = BlockArena::new_geometric(4, 16);
        let existing = arena.try_write(&[1, 2, 3, 4]).unwrap();
        let baseline_pages = arena.page_capacities();
        let baseline_used = arena.total_used_bytes();
        let baseline_next = arena.next_page_size();

        arena.fail_pair_write_on_call(2);
        let error = arena
            .try_write_pair(&[5; 8], &[6])
            .expect_err("injected second write must fail");
        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(arena.page_capacities(), baseline_pages);
        assert_eq!(arena.total_used_bytes(), baseline_used);
        assert_eq!(arena.next_page_size(), baseline_next);
        assert_eq!(arena.slice(existing).unwrap(), &[1, 2, 3, 4]);

        let (first, second) = arena.try_write_pair(&[5; 8], &[6]).unwrap();
        assert_eq!(arena.page_capacities(), vec![4, 8, 16]);
        assert_eq!(arena.slice(first).unwrap(), &[5; 8]);
        assert_eq!(arena.slice(second).unwrap(), &[6]);
    }

    #[test]
    fn freeze_preserves_page_and_offset_while_dropping_slack() {
        let mut arena = BlockArena::new(8);
        let first = arena.write(&[1, 2, 3]);
        let second = arena.write(&[4, 5, 6, 7]);
        let third = arena.write(&[8, 9, 10]);

        assert_eq!((first.page, first.offset, first.len), (0, 0, 3));
        assert_eq!((second.page, second.offset, second.len), (0, 3, 4));
        assert_eq!((third.page, third.offset, third.len), (1, 0, 3));
        assert_eq!(arena.total_capacity_bytes(), 16);
        assert_eq!(arena.total_used_bytes(), 10);

        let frozen = arena.try_freeze().unwrap();
        assert_eq!(frozen.page_count(), 2);
        assert_eq!(frozen.total_used_bytes(), 10);
        assert_eq!(frozen.total_allocated_bytes(), 10);
        assert_eq!(frozen.slice(first).unwrap(), &[1, 2, 3]);
        assert_eq!(frozen.slice(second).unwrap(), &[4, 5, 6, 7]);
        assert_eq!(frozen.slice(third).unwrap(), &[8, 9, 10]);
    }

    #[test]
    fn arena_reads_reject_missing_pages_and_ranges_outside_used_prefix() {
        let mut arena = BlockArena::new(8);
        let valid = arena.write(&[1, 2, 3]);
        assert_eq!(arena.slice(valid).unwrap(), &[1, 2, 3]);

        let unused_capacity = BufferRef {
            page: 0,
            offset: 3,
            len: 1,
        };
        let missing_page = BufferRef {
            page: 1,
            offset: 0,
            len: 1,
        };
        assert_eq!(
            arena.slice(unused_capacity).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            arena.slice(missing_page).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let frozen = arena.try_freeze().unwrap();
        assert_eq!(
            frozen.slice(unused_capacity).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            frozen.slice(missing_page).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn empty_arena_freezes_without_allocating_a_page() {
        let frozen = BlockArena::new(8).try_freeze().unwrap();
        assert_eq!(frozen.page_count(), 0);
        assert_eq!(frozen.total_allocated_bytes(), 0);
        assert_eq!(frozen.total_used_bytes(), 0);
    }

    #[test]
    fn adaptive_freeze_retains_only_exact_used_prefixes() {
        let mut arena = BlockArena::new_geometric(8, 32);
        let first = arena.try_write(&[1, 2, 3]).unwrap();
        let second = arena.try_write(&[4, 5, 6, 7, 8, 9]).unwrap();
        assert_eq!(arena.page_capacities(), vec![8, 16]);
        assert_eq!(arena.total_capacity_bytes(), 24);
        assert_eq!(arena.total_used_bytes(), 9);

        let frozen = arena.try_freeze().unwrap();
        assert_eq!(frozen.total_used_bytes(), 9);
        assert_eq!(frozen.total_allocated_bytes(), 9);
        assert_eq!(frozen.slice(first).unwrap(), &[1, 2, 3]);
        assert_eq!(frozen.slice(second).unwrap(), &[4, 5, 6, 7, 8, 9]);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn unrepresentable_allocation_is_rejected_without_mutating_the_arena() {
        let mut arena = BlockArena::new(8);
        let existing = arena.try_write(&[1, 2, 3]).unwrap();
        let page_count = arena.page_count();
        let capacity = arena.total_capacity_bytes();
        let used = arena.total_used_bytes();

        let error = arena
            .try_alloc(u32::MAX as usize + 1)
            .expect_err("oversized allocation must not narrow into BufferRef");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(arena.page_count(), page_count);
        assert_eq!(arena.total_capacity_bytes(), capacity);
        assert_eq!(arena.total_used_bytes(), used);
        assert_eq!(arena.slice(existing).unwrap(), &[1, 2, 3]);

        assert!(checked_buffer_ref(u32::MAX as usize + 1, 0, 1).is_err());
        assert!(checked_buffer_ref(0, u32::MAX as usize + 1, 1).is_err());
        assert!(checked_buffer_ref(0, 0, u32::MAX as usize + 1).is_err());
    }

    #[test]
    fn failed_freeze_returns_the_original_arena_for_transactional_rollback() {
        let mut arena = BlockArena::new(8);
        let existing = arena.try_write(&[1, 2, 3]).unwrap();
        arena.pages[0].used = arena.pages[0].buf.len() + 1;

        let write_error = arena
            .try_write(&[4])
            .expect_err("a corrupt last-page invariant must reject writes");
        assert_eq!(write_error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(arena.page_count(), 1);
        assert_eq!(arena.pages[0].used, 9);

        let freeze_error = arena
            .try_freeze()
            .expect_err("freeze must reject rather than clamp impossible used bytes");
        assert_eq!(freeze_error.error().kind(), io::ErrorKind::InvalidData);
        let (source, mut recovered) = freeze_error.into_parts();
        assert_eq!(source.kind(), io::ErrorKind::InvalidData);
        assert_eq!(recovered.page_count(), 1);
        assert_eq!(recovered.pages[0].used, 9);

        recovered.pages[0].used = 3;
        let frozen = recovered.try_freeze().unwrap();
        assert_eq!(frozen.slice(existing).unwrap(), &[1, 2, 3]);
        assert_eq!(frozen.total_allocated_bytes(), 3);
    }

    #[test]
    fn frozen_arena_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FrozenBlockArena>();
    }

    #[test]
    fn frozen_arena_supports_concurrent_checked_reads() {
        let mut arena = BlockArena::new(4);
        let first = arena.write(&[1, 2, 3]);
        let second = arena.write(&[4, 5, 6, 7, 8]);
        let frozen = std::sync::Arc::new(arena.try_freeze().unwrap());

        let readers: Vec<_> = (0..8)
            .map(|_| {
                let frozen = std::sync::Arc::clone(&frozen);
                std::thread::spawn(move || {
                    for _ in 0..1_000 {
                        assert_eq!(frozen.slice(first).unwrap(), &[1, 2, 3]);
                        assert_eq!(frozen.slice(second).unwrap(), &[4, 5, 6, 7, 8]);
                    }
                })
            })
            .collect();
        for reader in readers {
            reader.join().unwrap();
        }
    }
}
