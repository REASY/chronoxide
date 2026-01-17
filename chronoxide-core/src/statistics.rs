use std::time::Duration;
use tdigests::TDigest;

#[derive(Clone, Copy)]
pub struct Dist<V, M> {
    pub count: usize,
    pub min: V,
    pub max: V,
    pub mean: M,
    pub stddev: M,
    pub p50: V,
    pub p75: V,
    pub p95: V,
    pub p99: V,
}

pub type DistDuration = Dist<Duration, Duration>;
pub type DistU32 = Dist<u32, f64>;
pub type DistU64 = Dist<u64, f64>;

pub struct SummaryInput<T> {
    pub count: usize,
    pub min: T,
    pub max: T,
    pub mean: f64,
    pub stddev: f64,
    pub p50: T,
    pub p75: T,
    pub p95: T,
    pub p99: T,
}

pub trait StatDisplay {
    fn fmt_stat(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;

    fn to_string_stat(&self) -> String {
        struct Wrapper<'a, T: ?Sized>(&'a T);
        impl<'a, T: StatDisplay + ?Sized> std::fmt::Display for Wrapper<'a, T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt_stat(f)
            }
        }
        format!("{}", Wrapper(self))
    }
}

impl StatDisplay for Duration {
    fn fmt_stat(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl StatDisplay for u32 {
    fn fmt_stat(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl StatDisplay for u64 {
    fn fmt_stat(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl StatDisplay for f64 {
    fn fmt_stat(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.2}", self)
    }
}

impl<V: StatDisplay, M: StatDisplay> std::fmt::Display for Dist<V, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "n={} p50=", self.count)?;
        self.p50.fmt_stat(f)?;
        write!(f, " p75=")?;
        self.p75.fmt_stat(f)?;
        write!(f, " p95=")?;
        self.p95.fmt_stat(f)?;
        write!(f, " p99=")?;
        self.p99.fmt_stat(f)?;
        write!(f, " mean=")?;
        self.mean.fmt_stat(f)?;
        write!(f, " stddev=")?;
        self.stddev.fmt_stat(f)?;
        write!(f, " min=")?;
        self.min.fmt_stat(f)?;
        write!(f, " max=")?;
        self.max.fmt_stat(f)
    }
}

impl<V: StatDisplay, M: StatDisplay> Dist<V, M> {
    pub fn to_markdown_row(&self, name: &str) -> String {
        format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            name,
            self.count,
            self.mean.to_string_stat(),
            self.stddev.to_string_stat(),
            self.min.to_string_stat(),
            self.max.to_string_stat(),
            self.p50.to_string_stat(),
            self.p75.to_string_stat(),
            self.p95.to_string_stat(),
            self.p99.to_string_stat(),
        )
    }
}

pub trait StatValue: Copy + PartialOrd + 'static {
    const MIN: Self;
    const MAX: Self;
    type Summary;

    fn as_f64(self) -> f64;
    fn as_u128(self) -> u128;
    fn from_f64(v: f64) -> Self;
    fn create_summary(input: SummaryInput<Self>) -> Self::Summary;
}

impl StatValue for Duration {
    const MIN: Self = Duration::ZERO;
    const MAX: Self = Duration::MAX;
    type Summary = DistDuration;

    fn as_f64(self) -> f64 {
        self.as_nanos() as f64
    }

    fn as_u128(self) -> u128 {
        self.as_nanos()
    }

    fn from_f64(v: f64) -> Self {
        Duration::from_nanos(v as u64)
    }

    fn create_summary(input: SummaryInput<Self>) -> Self::Summary {
        DistDuration {
            count: input.count,
            min: input.min,
            max: input.max,
            mean: Duration::from_nanos(input.mean as u64),
            stddev: Duration::from_nanos(input.stddev as u64),
            p50: input.p50,
            p75: input.p75,
            p95: input.p95,
            p99: input.p99,
        }
    }
}

impl StatValue for u64 {
    const MIN: Self = u64::MIN;
    const MAX: Self = u64::MAX;
    type Summary = DistU64;

    fn as_f64(self) -> f64 {
        self as f64
    }

    fn as_u128(self) -> u128 {
        u128::from(self)
    }

    fn from_f64(v: f64) -> Self {
        v as u64
    }

    fn create_summary(input: SummaryInput<Self>) -> Self::Summary {
        DistU64 {
            count: input.count,
            min: input.min,
            max: input.max,
            mean: input.mean,
            stddev: input.stddev,
            p50: input.p50,
            p75: input.p75,
            p95: input.p95,
            p99: input.p99,
        }
    }
}

impl StatValue for u32 {
    const MIN: Self = u32::MIN;
    const MAX: Self = u32::MAX;
    type Summary = DistU32;

    fn as_f64(self) -> f64 {
        self as f64
    }

    fn as_u128(self) -> u128 {
        u128::from(self)
    }

    fn from_f64(v: f64) -> Self {
        v as u32
    }

    fn create_summary(input: SummaryInput<Self>) -> Self::Summary {
        DistU32 {
            count: input.count,
            min: input.min,
            max: input.max,
            mean: input.mean,
            stddev: input.stddev,
            p50: input.p50,
            p75: input.p75,
            p95: input.p95,
            p99: input.p99,
        }
    }
}

pub struct Stats<T> {
    tdigest: TDigestBuffered,
    sum: u128,
    sum_sq: f64,
    min: T,
    max: T,
    count: u64,
}

struct TDigestBuffered {
    digest: Option<TDigest>,
    buffer: Vec<f64>,
    buffer_capacity: usize,
    max_centroids: usize,
}

impl<T: StatValue> Stats<T> {
    pub fn new_tdigest(max_centroids: usize, buffer_capacity: usize) -> Self {
        Self {
            tdigest: TDigestBuffered::new(max_centroids, buffer_capacity),
            sum: 0,
            sum_sq: 0.0,
            min: T::MAX,
            max: T::MIN,
            count: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn insert(&mut self, value: T) {
        let v_f64 = value.as_f64();
        self.tdigest.insert(v_f64);
        self.sum += value.as_u128();
        self.sum_sq += v_f64 * v_f64;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.count += 1;
    }

    pub fn merge(&mut self, other: Self) {
        if other.count == 0 {
            return;
        }

        if self.count == 0 {
            self.min = other.min;
            self.max = other.max;
        } else {
            if other.min < self.min {
                self.min = other.min;
            }
            if other.max > self.max {
                self.max = other.max;
            }
        }

        self.tdigest.merge(other.tdigest);
        self.sum = self.sum.saturating_add(other.sum);
        self.sum_sq += other.sum_sq;
        self.count = self.count.saturating_add(other.count);
    }

    pub fn summarize(&self) -> Option<T::Summary> {
        if self.count == 0 {
            return None;
        }
        let count = self.count as usize;
        let mean = self.sum as f64 / self.count as f64;
        let variance = (self.sum_sq / self.count as f64) - (mean * mean);
        let stddev = if variance > 0.0 { variance.sqrt() } else { 0.0 };

        let digest = self.tdigest.digest_with_buffer()?;
        let q = |rank| T::from_f64(digest.estimate_quantile(rank));

        Some(T::create_summary(SummaryInput {
            count,
            min: self.min,
            max: self.max,
            mean,
            stddev,
            p50: q(0.50),
            p75: q(0.75),
            p95: q(0.95),
            p99: q(0.99),
        }))
    }
}

pub const DEFAULT_TDIGEST_MAX_CENTROIDS: usize = 200;
pub const DEFAULT_TDIGEST_BUFFER_CAPACITY: usize = 4_096;

const MAX_TDIGEST_MAX_CENTROIDS: usize = 10_000;
const MAX_TDIGEST_BUFFER_CAPACITY: usize = 1_000_000;

impl TDigestBuffered {
    fn new(max_centroids: usize, buffer_capacity: usize) -> Self {
        let max_centroids = max_centroids.clamp(3, MAX_TDIGEST_MAX_CENTROIDS);
        let buffer_capacity = buffer_capacity.clamp(1, MAX_TDIGEST_BUFFER_CAPACITY);
        Self {
            digest: None,
            buffer: Vec::with_capacity(buffer_capacity),
            buffer_capacity,
            max_centroids,
        }
    }

    fn insert(&mut self, value: f64) {
        self.buffer.push(value);
        if self.buffer.len() >= self.buffer_capacity {
            self.flush_buffer();
        }
    }

    fn merge(&mut self, other: Self) {
        self.buffer_capacity = self.buffer_capacity.max(other.buffer_capacity);
        self.max_centroids = self.max_centroids.max(other.max_centroids);

        self.merge_digest(other.digest);
        if !other.buffer.is_empty() {
            self.buffer.extend_from_slice(&other.buffer);
            if self.buffer.len() >= self.buffer_capacity {
                self.flush_buffer();
            }
        }
    }

    fn merge_digest(&mut self, other: Option<TDigest>) {
        let Some(other) = other else {
            return;
        };

        match self.digest.take() {
            None => {
                self.digest = Some(other);
            }
            Some(current) => {
                let mut merged = current.merge(&other);
                merged.compress(self.max_centroids);
                self.digest = Some(merged);
            }
        }
    }

    fn flush_buffer(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let buffer = std::mem::take(&mut self.buffer);
        let mut digest = TDigest::from_values(buffer);
        digest.compress(self.max_centroids);
        self.merge_digest(Some(digest));

        self.buffer = Vec::with_capacity(self.buffer_capacity);
    }

    fn digest_with_buffer(&self) -> Option<TDigest> {
        match (self.digest.as_ref(), self.buffer.is_empty()) {
            (None, true) => None,
            (None, false) => {
                let mut digest = TDigest::from_values(self.buffer.clone());
                digest.compress(self.max_centroids);
                Some(digest)
            }
            (Some(digest), true) => Some(digest.clone()),
            (Some(digest), false) => {
                let mut buffer_digest = TDigest::from_values(self.buffer.clone());
                buffer_digest.compress(self.max_centroids);
                let mut merged = digest.merge(&buffer_digest);
                merged.compress(self.max_centroids);
                Some(merged)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_u32_summarize_and_merge_keep_basic_invariants() {
        let mut a = Stats::<u32>::new_tdigest(1_000, 10);
        for v in 1..=999u32 {
            a.insert(v);
        }

        let s = a.summarize().unwrap();
        assert_eq!(s.count, 999);
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 999);
        assert!(s.mean >= 1.0 && s.mean <= 999.0);
        assert!(s.stddev > 0.0);
        assert!(s.p50 >= s.min && s.p50 <= s.max);
        assert!(s.p50 <= s.p75 && s.p75 <= s.p95 && s.p95 <= s.p99);

        // For 1..=999, mean is exactly 500.
        assert!((s.mean - 500.0).abs() < 1e-9);

        // Quantiles should be close for a smooth, dense distribution.
        assert!((s.p50 as i64 - 500).abs() <= 2);
        assert!((s.p75 as i64 - 750).abs() <= 5);
        assert!((s.p95 as i64 - 950).abs() <= 10);
        assert!(s.p99 >= 980 && s.p99 <= 999);

        let mut b = Stats::<u32>::new_tdigest(1_000, 10);
        for v in 1..=999u32 {
            b.insert(v);
        }
        a.merge(b);

        let s2 = a.summarize().unwrap();
        assert_eq!(s2.count, 1998);
        assert_eq!(s2.min, 1);
        assert_eq!(s2.max, 999);
        assert!((s2.mean - 500.0).abs() < 1e-9);
        assert!((s2.p50 as i64 - 500).abs() <= 2);
    }

    #[test]
    fn stats_is_empty_and_empty_merge_are_noops() {
        let mut a = Stats::<u64>::new_tdigest(200, 8);
        assert!(a.is_empty());
        assert_eq!(a.count(), 0);
        assert!(a.summarize().is_none());

        let b = Stats::<u64>::new_tdigest(200, 8);
        a.merge(b);
        assert!(a.is_empty());
        assert!(a.summarize().is_none());
    }

    #[test]
    fn stats_duration_tracks_min_max_and_mean() {
        let mut s = Stats::<Duration>::new_tdigest(200, 2);
        s.insert(Duration::from_nanos(10));
        s.insert(Duration::from_nanos(30));
        s.insert(Duration::from_nanos(20));

        let dist = s.summarize().unwrap();
        assert_eq!(dist.count, 3);
        assert_eq!(dist.min, Duration::from_nanos(10));
        assert_eq!(dist.max, Duration::from_nanos(30));
        assert_eq!(dist.mean, Duration::from_nanos(20));
        assert!(dist.stddev > Duration::from_nanos(0));
    }
}
