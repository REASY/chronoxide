use crate::statistics::{
    DEFAULT_TDIGEST_BUFFER_CAPACITY, DEFAULT_TDIGEST_MAX_CENTROIDS, DistDuration, DistU64, Stats,
};
use chronoxide_core::storage::head::HeadWindow;
use std::time::Duration;

#[derive(Clone)]
pub(super) struct HeadBufferDistributions {
    pub(super) call_latency: Option<DistDuration>,
    pub(super) batch_sizes: Option<DistU64>,
    pub(super) series_sample_counts: Option<DistU64>,
    pub(super) blocks_per_series: Option<DistU64>,
    pub(super) samples_per_block: Option<DistU64>,
}

#[derive(Clone)]
pub(super) struct HeadBufferSeriesDensity {
    #[allow(dead_code)]
    pub(super) series_total: u64,
    pub(super) series_single_sample_count: u64,
    pub(super) series_multi_sample_count: u64,
    pub(super) series_single_sample_ratio: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct HeadSeriesTableSummary {
    pub(super) windows: u64,
    pub(super) in_order_windows: u64,
    pub(super) in_order_rotations: u64,
    pub(super) out_of_order_windows: u64,
    pub(super) adaptive_windows: u64,
    pub(super) series_total: u64,
    pub(super) direct_pages_total: u64,
    pub(super) direct_series_total: u64,
    pub(super) sparse_pages_total: u64,
    pub(super) sparse_series_total: u64,
    pub(super) refs_above_paged_limit_total: u64,
    pub(super) direct_series_ratio: f64,
    pub(super) max_page_directory_len: u64,
    pub(super) max_page_directory_capacity: u64,
    pub(super) max_sparse_capacity: u64,
    pub(super) max_sparse_slot_capacity: u64,
    pub(super) max_direct_slot_index_bytes: u64,
    pub(super) max_direct_reverse_slot_capacity: u64,
    pub(super) max_direct_value_capacity: u64,
}

pub(super) struct HeadBufferStats {
    call_latency: Stats<Duration>,
    batch_sizes: Stats<u64>,
    series_sample_counts: Stats<u64>,
    blocks_per_series: Stats<u64>,
    samples_per_block: Stats<u64>,
    series_single_sample_count: u64,
    series_multi_sample_count: u64,
    series_table_windows: u64,
    in_order_windows: u64,
    in_order_rotations: u64,
    out_of_order_windows: u64,
    adaptive_series_table_windows: u64,
    series_table_series_total: u64,
    series_table_direct_pages_total: u64,
    series_table_direct_series_total: u64,
    series_table_sparse_pages_total: u64,
    series_table_sparse_series_total: u64,
    series_table_refs_above_paged_limit_total: u64,
    series_table_max_page_directory_len: u64,
    series_table_max_page_directory_capacity: u64,
    series_table_max_sparse_capacity: u64,
    series_table_max_sparse_slot_capacity: u64,
    series_table_max_direct_slot_index_bytes: u64,
    series_table_max_direct_reverse_slot_capacity: u64,
    series_table_max_direct_value_capacity: u64,
}

impl HeadBufferStats {
    pub(super) fn new() -> Self {
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
            series_table_windows: 0,
            in_order_windows: 0,
            in_order_rotations: 0,
            out_of_order_windows: 0,
            adaptive_series_table_windows: 0,
            series_table_series_total: 0,
            series_table_direct_pages_total: 0,
            series_table_direct_series_total: 0,
            series_table_sparse_pages_total: 0,
            series_table_sparse_series_total: 0,
            series_table_refs_above_paged_limit_total: 0,
            series_table_max_page_directory_len: 0,
            series_table_max_page_directory_capacity: 0,
            series_table_max_sparse_capacity: 0,
            series_table_max_sparse_slot_capacity: 0,
            series_table_max_direct_slot_index_bytes: 0,
            series_table_max_direct_reverse_slot_capacity: 0,
            series_table_max_direct_value_capacity: 0,
        }
    }

    pub(super) fn record_call(
        &mut self,
        elapsed: Duration,
        samples: usize,
        _flushed_windows: usize,
    ) {
        self.call_latency.insert(elapsed);
        self.batch_sizes.insert(samples as u64);
    }

    pub(super) fn record_window(&mut self, window: &HeadWindow) {
        let table = window.series_table_stats();
        self.series_table_windows = self.series_table_windows.saturating_add(1);
        if window.is_out_of_order() {
            self.out_of_order_windows = self.out_of_order_windows.saturating_add(1);
        } else {
            self.in_order_windows = self.in_order_windows.saturating_add(1);
        }
        self.adaptive_series_table_windows = self
            .adaptive_series_table_windows
            .saturating_add(u64::from(table.adaptive));
        self.series_table_series_total = self
            .series_table_series_total
            .saturating_add(table.series as u64);
        self.series_table_direct_pages_total = self
            .series_table_direct_pages_total
            .saturating_add(table.direct_pages as u64);
        self.series_table_direct_series_total = self
            .series_table_direct_series_total
            .saturating_add(table.direct_series as u64);
        self.series_table_sparse_pages_total = self
            .series_table_sparse_pages_total
            .saturating_add(table.sparse_pages as u64);
        self.series_table_sparse_series_total = self
            .series_table_sparse_series_total
            .saturating_add(table.sparse_series as u64);
        self.series_table_refs_above_paged_limit_total = self
            .series_table_refs_above_paged_limit_total
            .saturating_add(table.refs_above_paged_limit as u64);
        self.series_table_max_page_directory_len = self
            .series_table_max_page_directory_len
            .max(table.page_directory_len as u64);
        self.series_table_max_page_directory_capacity = self
            .series_table_max_page_directory_capacity
            .max(table.page_directory_capacity as u64);
        self.series_table_max_sparse_capacity = self
            .series_table_max_sparse_capacity
            .max(table.sparse_capacity as u64);
        self.series_table_max_sparse_slot_capacity = self
            .series_table_max_sparse_slot_capacity
            .max(table.sparse_slot_capacity as u64);
        self.series_table_max_direct_slot_index_bytes = self
            .series_table_max_direct_slot_index_bytes
            .max(table.direct_slot_index_bytes as u64);
        self.series_table_max_direct_reverse_slot_capacity = self
            .series_table_max_direct_reverse_slot_capacity
            .max(table.direct_reverse_slot_capacity as u64);
        self.series_table_max_direct_value_capacity = self
            .series_table_max_direct_value_capacity
            .max(table.direct_value_capacity as u64);

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

    /// Records a completed active window returned by head rotation.
    ///
    /// Shutdown drainage and OOO drainage deliberately use `record_window`
    /// directly so they cannot inflate the long-lived rotation counter.
    pub(super) fn record_rotated_window(&mut self, window: &HeadWindow) {
        debug_assert!(!window.is_out_of_order());
        self.in_order_rotations = self.in_order_rotations.saturating_add(1);
        self.record_window(window);
    }

    pub(super) fn distributions(&self) -> HeadBufferDistributions {
        HeadBufferDistributions {
            call_latency: self.call_latency.summarize(),
            batch_sizes: self.batch_sizes.summarize(),
            series_sample_counts: self.series_sample_counts.summarize(),
            blocks_per_series: self.blocks_per_series.summarize(),
            samples_per_block: self.samples_per_block.summarize(),
        }
    }

    pub(super) fn series_density(&self) -> Option<HeadBufferSeriesDensity> {
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

    pub(super) fn series_table_summary(&self) -> Option<HeadSeriesTableSummary> {
        if self.series_table_windows == 0 {
            return None;
        }
        let direct_series_ratio = if self.series_table_series_total == 0 {
            0.0
        } else {
            self.series_table_direct_series_total as f64 / self.series_table_series_total as f64
        };
        Some(HeadSeriesTableSummary {
            windows: self.series_table_windows,
            in_order_windows: self.in_order_windows,
            in_order_rotations: self.in_order_rotations,
            out_of_order_windows: self.out_of_order_windows,
            adaptive_windows: self.adaptive_series_table_windows,
            series_total: self.series_table_series_total,
            direct_pages_total: self.series_table_direct_pages_total,
            direct_series_total: self.series_table_direct_series_total,
            sparse_pages_total: self.series_table_sparse_pages_total,
            sparse_series_total: self.series_table_sparse_series_total,
            refs_above_paged_limit_total: self.series_table_refs_above_paged_limit_total,
            direct_series_ratio,
            max_page_directory_len: self.series_table_max_page_directory_len,
            max_page_directory_capacity: self.series_table_max_page_directory_capacity,
            max_sparse_capacity: self.series_table_max_sparse_capacity,
            max_sparse_slot_capacity: self.series_table_max_sparse_slot_capacity,
            max_direct_slot_index_bytes: self.series_table_max_direct_slot_index_bytes,
            max_direct_reverse_slot_capacity: self.series_table_max_direct_reverse_slot_capacity,
            max_direct_value_capacity: self.series_table_max_direct_value_capacity,
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
        let mut head = HeadBuffer::new(
            HeadConfig::new(
                Duration::from_secs(60),
                FloatEncoding::Gorilla,
                IntEncoding::DeltaZigZag,
            )
            .with_adaptive_series_table(false),
        )
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

        let table = stats.series_table_summary().expect("table summary");
        assert_eq!(table.windows, 1);
        assert_eq!(table.in_order_windows, 1);
        assert_eq!(table.in_order_rotations, 0);
        assert_eq!(table.out_of_order_windows, 0);
        assert_eq!(table.adaptive_windows, 0);
        assert_eq!(table.series_total, 2);
        assert_eq!(table.direct_pages_total, 0);
        assert_eq!(table.direct_series_total, 0);
        assert_eq!(table.sparse_series_total, 2);
        assert_eq!(table.direct_series_ratio, 0.0);
    }

    #[test]
    fn series_table_summary_tracks_adaptive_direct_pages() {
        let config = HeadConfig::new(
            Duration::from_secs(60),
            FloatEncoding::Raw,
            IntEncoding::Raw,
        )
        .with_adaptive_series_table(true);
        let mut head = HeadBuffer::new(config).unwrap();
        for raw in 0..128 {
            head.record_sample(
                SeriesRef::new(raw),
                1_000,
                SampleValue::Float(f64::from(raw)),
            )
            .unwrap();
        }

        let window = head.drain().unwrap();
        let mut stats = HeadBufferStats::new();
        stats.record_window(&window);

        let table = stats.series_table_summary().expect("table summary");
        assert_eq!(table.windows, 1);
        assert_eq!(table.in_order_windows, 1);
        assert_eq!(table.in_order_rotations, 0);
        assert_eq!(table.out_of_order_windows, 0);
        assert_eq!(table.adaptive_windows, 1);
        assert_eq!(table.series_total, 128);
        assert_eq!(table.direct_pages_total, 1);
        assert_eq!(table.direct_series_total, 128);
        assert_eq!(table.sparse_series_total, 0);
        assert_eq!(table.direct_series_ratio, 1.0);
        assert_eq!(table.max_direct_slot_index_bytes, 8 * 1024);
    }

    #[test]
    fn series_table_summary_distinguishes_rotated_and_ooo_windows() {
        let config = HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Raw,
            IntEncoding::Raw,
        )
        .with_out_of_order_time_window(Duration::from_secs(6));
        let mut head = HeadBuffer::new(config).unwrap();
        let series = SeriesRef::new(7);
        head.record_sample(series, 1_000, SampleValue::Float(1.0))
            .unwrap();
        let rotated = head
            .record_sample(series, 15_000, SampleValue::Float(15.0))
            .unwrap()
            .unwrap();
        head.record_sample(series, 12_000, SampleValue::Float(12.0))
            .unwrap();

        let mut stats = HeadBufferStats::new();
        stats.record_rotated_window(&rotated);
        for window in head.drain_windows() {
            stats.record_window(&window);
        }
        let summary = stats.series_table_summary().unwrap();
        assert_eq!(summary.windows, 3);
        assert_eq!(summary.in_order_windows, 2);
        assert_eq!(summary.in_order_rotations, 1);
        assert_eq!(summary.out_of_order_windows, 1);
    }
}
