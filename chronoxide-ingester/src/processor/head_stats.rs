use chronoxide_core::statistics::{
    DEFAULT_TDIGEST_BUFFER_CAPACITY, DEFAULT_TDIGEST_MAX_CENTROIDS, DistDuration, DistU64, Stats,
};
use chronoxide_core::storage::head::HeadWindow;
use std::time::Duration;

#[derive(Clone)]
pub struct HeadBufferDistributions {
    pub call_latency: Option<DistDuration>,
    pub batch_sizes: Option<DistU64>,
    pub series_sample_counts: Option<DistU64>,
    pub blocks_per_series: Option<DistU64>,
    pub samples_per_block: Option<DistU64>,
}

#[derive(Clone)]
pub struct HeadBufferSeriesDensity {
    #[allow(dead_code)]
    pub series_total: u64,
    pub series_single_sample_count: u64,
    pub series_multi_sample_count: u64,
    pub series_single_sample_ratio: f64,
}

pub struct HeadBufferStats {
    call_latency: Stats<Duration>,
    batch_sizes: Stats<u64>,
    series_sample_counts: Stats<u64>,
    blocks_per_series: Stats<u64>,
    samples_per_block: Stats<u64>,
    series_single_sample_count: u64,
    series_multi_sample_count: u64,
}

impl HeadBufferStats {
    pub fn new() -> Self {
        Self {
            call_latency: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            batch_sizes: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            series_sample_counts: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            blocks_per_series: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            samples_per_block: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            series_single_sample_count: 0,
            series_multi_sample_count: 0,
        }
    }

    pub fn record_call(&mut self, elapsed: Duration, samples: usize, _flushed_windows: usize) {
        self.call_latency.insert(elapsed);
        self.batch_sizes.insert(samples as u64);
    }

    pub fn record_window(&mut self, window: &HeadWindow) {
        for sample_count in window.series_sample_counts() {
            self.series_sample_counts.insert(sample_count);
            if sample_count <= 1 {
                self.series_single_sample_count = self.series_single_sample_count.saturating_add(1);
            } else {
                self.series_multi_sample_count = self.series_multi_sample_count.saturating_add(1);
            }
        }
        for block_count in window.series_block_counts() {
            self.blocks_per_series.insert(block_count as u64);
        }
        let samples_per_block = &mut self.samples_per_block;
        window.for_each_block_sample(|count| samples_per_block.insert(count));
    }

    pub fn distributions(&self) -> HeadBufferDistributions {
        HeadBufferDistributions {
            call_latency: self.call_latency.summarize(),
            batch_sizes: self.batch_sizes.summarize(),
            series_sample_counts: self.series_sample_counts.summarize(),
            blocks_per_series: self.blocks_per_series.summarize(),
            samples_per_block: self.samples_per_block.summarize(),
        }
    }

    pub fn series_density(&self) -> Option<HeadBufferSeriesDensity> {
        let series_total = self.series_sample_counts.count();
        if series_total == 0 {
            return None;
        }

        let single = self.series_single_sample_count;
        let single_ratio = single as f64 / series_total as f64;

        Some(HeadBufferSeriesDensity {
            series_total,
            series_single_sample_count: single,
            series_multi_sample_count: self.series_multi_sample_count,
            series_single_sample_ratio: single_ratio,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronoxide_core::labels::SeriesRef;
    use chronoxide_core::storage::head::{
        FloatEncoding, HeadBuffer, HeadConfig, IntEncoding, SampleValue,
    };

    #[test]
    fn series_density_tracks_single_and_multi_sample_series() {
        let mut head = HeadBuffer::new(HeadConfig::new(
            Duration::from_secs(60),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ))
        .unwrap();

        head.record_sample(SeriesRef::new(0), 1_000, SampleValue::Float(1.0))
            .unwrap();
        head.record_sample(SeriesRef::new(1), 1_000, SampleValue::Float(1.0))
            .unwrap();
        head.record_sample(SeriesRef::new(1), 1_001, SampleValue::Float(2.0))
            .unwrap();

        let window = head.drain().expect("window");

        let mut stats = HeadBufferStats::new();
        stats.record_window(&window);

        let density = stats.series_density().expect("density");
        assert_eq!(density.series_total, 2);
        assert_eq!(density.series_single_sample_count, 1);
        assert_eq!(density.series_multi_sample_count, 1);
        assert!((density.series_single_sample_ratio - 0.5).abs() < 1e-12);

        let dist = stats.distributions();
        let series_dist = dist.series_sample_counts.expect("series dist");
        assert_eq!(series_dist.count, 2);
        assert_eq!(series_dist.min, 1);
        assert_eq!(series_dist.max, 2);
    }
}
