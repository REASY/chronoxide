mod distribution;

pub use distribution::*;

use chronoxide_core::labels::{LabelSetStore, SeriesRef, U64IdentityHasher};
use hyperloglog::HyperLogLog;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::time::Duration;

const PER_KEY_TOP_VALUES: usize = 32;
const PER_KEY_TABLE_LIMIT: usize = 100;
const PER_KEY_VALUE_SAMPLE_MAX_BYTES: usize = 256;

// A deterministic HLL config for per-key cardinality estimation when the underlying store can't
// provide exact cardinalities.
//
// Note: the hyperloglog crate derives internal register size (p/m) from `error_rate` in a way that
// doesn't match the classic HLL "relative error ≈ 1.04 / sqrt(m)" parameterization, so we report
// the actual derived relative error in markdown.
const PER_KEY_CARDINALITY_HLL_ERROR_RATE: f64 = 0.02;
const PER_KEY_CARDINALITY_HLL_SEED: u128 = 0x6f746c705f6368726f6e6f78696465; // "otlp_chronoxide"

fn hll_relative_error() -> f64 {
    let sr = 1.04 / PER_KEY_CARDINALITY_HLL_ERROR_RATE;
    let p = (sr * sr).ln().ceil() as u32;
    if p >= 63 {
        return 0.0;
    }
    let m = 1u64 << p;
    1.04 / (m as f64).sqrt()
}

type U64BuildHasher = BuildHasherDefault<U64IdentityHasher>;
type U64HashMap<V> = HashMap<u64, V, U64BuildHasher>;
type U64HashSet = HashSet<u64, U64BuildHasher>;

fn hash_u64(bytes: &[u8]) -> u64 {
    let mut hasher = U64IdentityHasher::default();
    hasher.write(bytes);
    hasher.finish()
}

fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn escape_markdown_cell(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', "\\n")
}

#[derive(Clone)]
struct TopValue {
    value_hash: u64,
    count: u64,
    sample: Box<str>,
}

#[derive(Default)]
struct TopK {
    counters: Vec<TopValue>,
}

impl TopK {
    fn new() -> Self {
        Self {
            counters: Vec::with_capacity(PER_KEY_TOP_VALUES),
        }
    }

    fn observe(&mut self, value_hash: u64, value: &str) {
        if let Some(counter) = self
            .counters
            .iter_mut()
            .find(|counter| counter.value_hash == value_hash)
        {
            counter.count = counter.count.saturating_add(1);
            return;
        }

        if self.counters.len() < PER_KEY_TOP_VALUES {
            self.counters.push(TopValue {
                value_hash,
                count: 1,
                sample: truncate_utf8(value, PER_KEY_VALUE_SAMPLE_MAX_BYTES).into(),
            });
            return;
        }

        let (min_index, min_counter) = self
            .counters
            .iter()
            .enumerate()
            .min_by_key(|(_, counter)| counter.count)
            .expect("topk counters non-empty");

        let min_count = min_counter.count;
        self.counters[min_index] = TopValue {
            value_hash,
            count: min_count.saturating_add(1),
            sample: truncate_utf8(value, PER_KEY_VALUE_SAMPLE_MAX_BYTES).into(),
        };
    }
}

struct ExactTopValue<'a> {
    sample: &'a str,
    count: u64,
}

struct ExactTopValues {
    value_to_index: U64HashMap<usize>,
    samples: Vec<Box<str>>,
    counts: Vec<u64>,
}

impl ExactTopValues {
    fn from_candidates(topk: &TopK) -> Self {
        let mut value_to_index: U64HashMap<usize> = U64HashMap::default();
        let mut samples = Vec::with_capacity(topk.counters.len());
        let mut counts = Vec::with_capacity(topk.counters.len());

        for counter in &topk.counters {
            if value_to_index.contains_key(&counter.value_hash) {
                continue;
            }
            let index = samples.len();
            value_to_index.insert(counter.value_hash, index);
            samples.push(counter.sample.clone());
            counts.push(0);
        }

        Self {
            value_to_index,
            samples,
            counts,
        }
    }

    fn observe(&mut self, value_hash: u64) {
        let Some(&index) = self.value_to_index.get(&value_hash) else {
            return;
        };
        self.counts[index] = self.counts[index].saturating_add(1);
    }

    fn top(&self) -> Vec<ExactTopValue<'_>> {
        let mut indices: Vec<usize> = (0..self.counts.len()).collect();
        indices.sort_by(|&a, &b| self.counts[b].cmp(&self.counts[a]));
        indices
            .into_iter()
            .map(|i| ExactTopValue {
                sample: &self.samples[i],
                count: self.counts[i],
            })
            .collect()
    }
}

struct PerKeyStats {
    key: Box<str>,
    series_with_key: u64,
    cardinality_estimate: usize,
    cardinality_is_exact: bool,
    cardinality_estimator: Option<HyperLogLog>,
    top_values: TopK,
}

impl PerKeyStats {
    fn new(store: &impl LabelSetStore, key: &str) -> Self {
        match store.key_cardinality(key) {
            Some(cardinality) => Self {
                key: key.into(),
                series_with_key: 0,
                cardinality_estimate: cardinality,
                cardinality_is_exact: true,
                cardinality_estimator: None,
                top_values: TopK::new(),
            },
            None => Self {
                key: key.into(),
                series_with_key: 0,
                cardinality_estimate: 0,
                cardinality_is_exact: false,
                cardinality_estimator: Some(HyperLogLog::new_deterministic(
                    PER_KEY_CARDINALITY_HLL_ERROR_RATE,
                    PER_KEY_CARDINALITY_HLL_SEED,
                )),
                top_values: TopK::new(),
            },
        }
    }

    fn observe_value(&mut self, value_hash: u64, value: &str) {
        self.series_with_key = self.series_with_key.saturating_add(1);
        self.top_values.observe(value_hash, value);

        if let Some(hll) = self.cardinality_estimator.as_mut() {
            hll.insert_by_hash_value(value_hash);
        }
    }

    fn finalize_cardinality(&mut self) {
        let Some(hll) = self.cardinality_estimator.take() else {
            return;
        };

        let estimate = hll.len().round().max(0.0).min(usize::MAX as f64) as usize;
        let max = usize::try_from(self.series_with_key).unwrap_or(usize::MAX);

        self.cardinality_estimate = if self.series_with_key == 0 {
            0
        } else {
            estimate.min(max).max(1)
        };
    }

    fn cardinality_display(&self) -> String {
        if self.cardinality_is_exact {
            self.cardinality_estimate.to_string()
        } else {
            format!("~{}", self.cardinality_estimate)
        }
    }

    fn packed_width_bytes(&self) -> u8 {
        match self.cardinality_estimate {
            0 | 1 => 0,
            2..=256 => 1,
            257..=65_536 => 2,
            _ => 4,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopValueCount {
    pub sample: Box<str>,
    pub count: u64,
}

#[derive(Debug, Clone)]
pub struct KeyCoverageRow {
    pub key: Box<str>,
    pub series_with_key: u64,
    pub distinct_values_display: Box<str>,
    pub packed_width_bytes: u8,
    pub top_values: Vec<TopValueCount>,
}

#[derive(Debug, Clone)]
pub struct KeyCardinalityRow {
    pub key: Box<str>,
    pub distinct_values_display: Box<str>,
    pub packed_width_bytes: u8,
    pub series_with_key: u64,
}

#[derive(Debug, Clone)]
pub struct KeyColumn {
    pub key_hash: u64,
    pub key: Box<str>,
    pub cardinality_display: Box<str>,
    pub cardinality_for_sort: usize,
}

#[derive(Debug, Clone)]
pub struct TopMetricSeriesRow {
    pub metric_rank: usize,
    pub series_rank: usize,
    pub total_cardinality: usize,
    pub metric_name: Box<str>,
    pub key_hashes: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct TopMetricsByTotalKeyCardinalityReport {
    pub metrics_limit: usize,
    pub series_per_metric_limit: usize,
    pub columns: Vec<KeyColumn>,
    pub rows: Vec<TopMetricSeriesRow>,
}

#[derive(Debug, Clone)]
pub struct PerKeyValueStatsReport {
    pub series_total: usize,
    pub series_scanned: usize,
    pub top_metrics: Option<TopMetricsByTotalKeyCardinalityReport>,
    pub top_keys_by_series_coverage: Vec<KeyCoverageRow>,
    pub top_keys_by_cardinality: Vec<KeyCardinalityRow>,
}

impl PerKeyValueStatsReport {
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();

        if let Some(top_metrics) = &self.top_metrics {
            md.push_str(&top_metrics.to_markdown());
        }

        md.push_str("### Per-Key Cardinality & Value Frequency\n\n");
        md.push_str(&format!(
            "Computed by scanning the `LabelSetStore`: `series_scanned={}` out of `series_total={}`.\n\n",
            self.series_scanned, self.series_total
        ));
        md.push_str(
            "`packed_width_bytes` is the fixed width required for values in a fixed-width-per-keyset encoding (0/1/2/4 bytes).\n\n",
        );
        md.push_str(&format!(
            "Distinct Values uses `LabelSetStore::key_cardinality()` when available; otherwise it is estimated via HyperLogLog (~{:.1}% relative error, values prefixed with `~`).\n\n",
            hll_relative_error() * 100.0
        ));
        md.push_str(
            "Top values are computed via a 2-pass scan: pass 1 selects a bounded candidate set per key; pass 2 counts those candidates exactly.\n\n",
        );

        md.push_str("#### Top Keys by Series Coverage\n\n");
        md.push_str(
            "| Key | Series w/ Key | Distinct Values | packed_width_bytes | Top Values (2-pass exact) |\n",
        );
        md.push_str("|---|---:|---:|---:|---|\n");

        for row in &self.top_keys_by_series_coverage {
            let total = row.series_with_key.max(1) as f64;
            let mut top_values_cell = String::new();
            for (i, value) in row.top_values.iter().take(5).enumerate() {
                if i > 0 {
                    top_values_cell.push_str(", ");
                }
                let pct = (value.count as f64 / total) * 100.0;
                top_values_cell.push_str(&format!(
                    "{}={} ({:.1}%)",
                    escape_markdown_cell(&value.sample),
                    value.count,
                    pct
                ));
            }
            if top_values_cell.is_empty() {
                top_values_cell.push_str("n/a");
            }

            md.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                escape_markdown_cell(&row.key),
                row.series_with_key,
                row.distinct_values_display,
                row.packed_width_bytes,
                top_values_cell,
            ));
        }

        md.push('\n');

        md.push_str("#### Top Keys by Cardinality\n\n");
        md.push_str("| Key | Distinct Values | packed_width_bytes | Series w/ Key |\n");
        md.push_str("|---|---:|---:|---:|\n");
        for row in &self.top_keys_by_cardinality {
            md.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                escape_markdown_cell(&row.key),
                row.distinct_values_display,
                row.packed_width_bytes,
                row.series_with_key,
            ));
        }

        md.push('\n');
        md
    }
}

impl TopMetricsByTotalKeyCardinalityReport {
    fn to_markdown(&self) -> String {
        if self.rows.is_empty() {
            return String::new();
        }

        let mut md = String::new();
        md.push_str(&format!(
            "### Top {} Metrics by Total Key Cardinality (Top {} Series per Metric)\n\n",
            self.metrics_limit, self.series_per_metric_limit
        ));
        md.push_str(&format!(
            "Total Cardinality = Sum of the cardinality of every *tag key* in the series (excluding `__name__`).\nHigh values indicate series composed of many high-cardinality dimensions (e.g. IDs).\nKey cardinality comes from `LabelSetStore::key_cardinality()` when available; otherwise it is estimated via HyperLogLog (~{:.1}% relative error, values prefixed with `~`).\n\n",
            hll_relative_error() * 100.0
        ));

        // Render a separate table per metric to avoid a single ultra-wide table that is sparse
        // (many empty columns when unioning keys across unrelated metrics).
        let mut row_index = 0;
        while row_index < self.rows.len() {
            let metric_rank = self.rows[row_index].metric_rank;
            let metric_name = self.rows[row_index].metric_name.as_ref();

            let metric_start = row_index;
            while row_index < self.rows.len() && self.rows[row_index].metric_rank == metric_rank {
                row_index += 1;
            }
            let metric_rows = &self.rows[metric_start..row_index];

            let mut metric_key_hashes: U64HashSet = U64HashSet::default();
            for row in metric_rows {
                for key_hash in &row.key_hashes {
                    metric_key_hashes.insert(*key_hash);
                }
            }

            let metric_columns: Vec<&KeyColumn> = self
                .columns
                .iter()
                .filter(|col| metric_key_hashes.contains(&col.key_hash))
                .collect();

            md.push_str(&format!("#### {}. `{}`\n\n", metric_rank, metric_name));

            md.push_str("| Series Rank | Total Cardinality");
            for col in &metric_columns {
                md.push_str(&format!(" | {}", escape_markdown_cell(&col.key)));
            }
            md.push_str(" |\n");

            md.push_str("|---:|---:");
            for _ in &metric_columns {
                md.push_str("|---:");
            }
            md.push_str("|\n");

            for row in metric_rows {
                md.push_str(&format!(
                    "| {} | {}",
                    row.series_rank, row.total_cardinality
                ));
                for col in &metric_columns {
                    if row.key_hashes.binary_search(&col.key_hash).is_ok() {
                        md.push_str(&format!(" | {}", col.cardinality_display));
                    } else {
                        md.push_str(" | ");
                    }
                }
                md.push_str(" |\n");
            }

            md.push('\n');
        }

        md
    }
}

pub fn per_key_value_stats_from_store(
    store: &impl LabelSetStore,
    series_limit: Option<usize>,
) -> PerKeyValueStatsReport {
    let store_series = store.len().min(u32::MAX as usize);
    let scan_series = series_limit.unwrap_or(store_series).min(store_series);

    let mut per_key: U64HashMap<PerKeyStats> = U64HashMap::default();

    for series_index in 0..scan_series {
        let series = SeriesRef::new(series_index as u32);
        store.visit_labelset(series, |key, value| {
            let key_hash = hash_u64(key.as_bytes());
            let value_hash = hash_u64(value.as_bytes());
            let entry = per_key
                .entry(key_hash)
                .or_insert_with(|| PerKeyStats::new(store, key));
            entry.observe_value(value_hash, value);
        });
    }

    for stats in per_key.values_mut() {
        stats.finalize_cardinality();
    }

    let mut keys_by_series: Vec<u64> = per_key.keys().copied().collect();
    keys_by_series.sort_by(|a, b| {
        let a_stats = per_key.get(a).expect("key present");
        let b_stats = per_key.get(b).expect("key present");
        b_stats
            .series_with_key
            .cmp(&a_stats.series_with_key)
            .then_with(|| a_stats.key.cmp(&b_stats.key))
    });

    // Second pass: count TopK candidates exactly for the keys that will be printed in the
    // "Top Keys by Series Coverage" table.
    let mut exact_top_by_key: U64HashMap<ExactTopValues> = U64HashMap::default();
    for key_hash in keys_by_series.iter().take(PER_KEY_TABLE_LIMIT) {
        let stats = per_key.get(key_hash).expect("key present");
        exact_top_by_key.insert(
            *key_hash,
            ExactTopValues::from_candidates(&stats.top_values),
        );
    }

    if !exact_top_by_key.is_empty() {
        for series_index in 0..scan_series {
            let series = SeriesRef::new(series_index as u32);
            store.visit_labelset(series, |key, value| {
                let key_hash = hash_u64(key.as_bytes());
                let Some(top) = exact_top_by_key.get_mut(&key_hash) else {
                    return;
                };
                let value_hash = hash_u64(value.as_bytes());
                top.observe(value_hash);
            });
        }
    }

    let mut top_keys_by_series_coverage: Vec<KeyCoverageRow> = Vec::new();
    for key_hash in keys_by_series.iter().take(PER_KEY_TABLE_LIMIT) {
        let key_stats = per_key.get(key_hash).expect("key present");
        let mut top_values: Vec<TopValueCount> = Vec::new();
        if let Some(exact) = exact_top_by_key.get(key_hash) {
            for value in exact.top().into_iter().take(5) {
                top_values.push(TopValueCount {
                    sample: value.sample.to_string().into_boxed_str(),
                    count: value.count,
                });
            }
        }

        top_keys_by_series_coverage.push(KeyCoverageRow {
            key: key_stats.key.clone(),
            series_with_key: key_stats.series_with_key,
            distinct_values_display: key_stats.cardinality_display().into_boxed_str(),
            packed_width_bytes: key_stats.packed_width_bytes(),
            top_values,
        });
    }

    let mut keys_by_cardinality: Vec<u64> = per_key.keys().copied().collect();
    keys_by_cardinality.sort_by(|a, b| {
        let a_stats = per_key.get(a).expect("key present");
        let b_stats = per_key.get(b).expect("key present");

        let a_card = a_stats.cardinality_estimate;
        let b_card = b_stats.cardinality_estimate;

        b_card
            .cmp(&a_card)
            .then_with(|| b_stats.series_with_key.cmp(&a_stats.series_with_key))
            .then_with(|| a_stats.key.cmp(&b_stats.key))
    });

    let mut top_keys_by_cardinality: Vec<KeyCardinalityRow> = Vec::new();
    for key_hash in keys_by_cardinality.iter().take(PER_KEY_TABLE_LIMIT) {
        let key_stats = per_key.get(key_hash).expect("key present");
        top_keys_by_cardinality.push(KeyCardinalityRow {
            key: key_stats.key.clone(),
            distinct_values_display: key_stats.cardinality_display().into_boxed_str(),
            packed_width_bytes: key_stats.packed_width_bytes(),
            series_with_key: key_stats.series_with_key,
        });
    }

    let top_metrics = compute_top_metrics_by_total_key_cardinality(store, scan_series, &per_key);

    PerKeyValueStatsReport {
        series_total: store_series,
        series_scanned: scan_series,
        top_metrics,
        top_keys_by_series_coverage,
        top_keys_by_cardinality,
    }
}

fn compute_top_metrics_by_total_key_cardinality(
    store: &impl LabelSetStore,
    scan_series: usize,
    per_key: &U64HashMap<PerKeyStats>,
) -> Option<TopMetricsByTotalKeyCardinalityReport> {
    if scan_series == 0 {
        return None;
    }

    // Build lookup for key cardinality.
    let key_cardinality_for_score: U64HashMap<usize> = per_key
        .iter()
        .map(|(h, stats)| (*h, stats.cardinality_estimate))
        .collect();

    const TOP_METRICS_LIMIT: usize = 30;
    const TOP_SERIES_PER_METRIC_LIMIT: usize = 5;

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    struct SeriesScore {
        score: usize,
        series_index: u32,
    }

    struct MetricTopSeries {
        metric_name: Box<str>,
        series: std::collections::BinaryHeap<std::cmp::Reverse<SeriesScore>>,
    }

    let mut per_metric: U64HashMap<MetricTopSeries> = U64HashMap::default();

    for series_index in 0..scan_series {
        let series = SeriesRef::new(series_index as u32);

        let mut metric_name_for_new_metric: Option<String> = None;
        let mut metric_hash: Option<u64> = None;
        let mut score: usize = 0;

        store.visit_labelset(series, |key, value| {
            if key == "__name__" {
                let hash = hash_u64(value.as_bytes());
                metric_hash = Some(hash);
                if metric_name_for_new_metric.is_none() && !per_metric.contains_key(&hash) {
                    metric_name_for_new_metric = Some(value.to_string());
                }
                return;
            }

            let key_hash = hash_u64(key.as_bytes());
            if let Some(c) = key_cardinality_for_score.get(&key_hash) {
                score = score.saturating_add(*c);
            }
        });

        let metric_hash = metric_hash.unwrap_or_else(|| hash_u64(b"(unknown)"));
        let entry = per_metric
            .entry(metric_hash)
            .or_insert_with(|| MetricTopSeries {
                metric_name: metric_name_for_new_metric
                    .unwrap_or_else(|| "(unknown)".to_string())
                    .into_boxed_str(),
                series: std::collections::BinaryHeap::with_capacity(
                    TOP_SERIES_PER_METRIC_LIMIT + 1,
                ),
            });

        entry.series.push(std::cmp::Reverse(SeriesScore {
            score,
            series_index: series_index as u32,
        }));
        if entry.series.len() > TOP_SERIES_PER_METRIC_LIMIT {
            entry.series.pop();
        }
    }

    struct MetricRank {
        metric_name: Box<str>,
        metric_score: usize,
        top_series: Vec<SeriesScore>,
    }

    let mut metrics: Vec<MetricRank> = Vec::with_capacity(per_metric.len());
    for (_, metric) in per_metric {
        let mut top_series: Vec<SeriesScore> = metric
            .series
            .into_iter()
            .map(|std::cmp::Reverse(series)| series)
            .collect();
        top_series.sort_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| a.series_index.cmp(&b.series_index))
        });
        let metric_score = top_series.first().map(|s| s.score).unwrap_or_default();
        metrics.push(MetricRank {
            metric_name: metric.metric_name,
            metric_score,
            top_series,
        });
    }

    metrics.sort_by(|a, b| {
        b.metric_score
            .cmp(&a.metric_score)
            .then_with(|| a.metric_name.cmp(&b.metric_name))
    });
    metrics.truncate(TOP_METRICS_LIMIT);

    let mut rows: Vec<TopMetricSeriesRow> =
        Vec::with_capacity(metrics.len() * TOP_SERIES_PER_METRIC_LIMIT);
    let mut union_key_hashes: U64HashSet = U64HashSet::default();

    for (metric_rank, metric) in metrics.iter().enumerate() {
        for (series_rank, series_item) in metric.top_series.iter().enumerate() {
            let series = SeriesRef::new(series_item.series_index);
            let mut key_hashes: Vec<u64> = Vec::new();

            store.visit_labelset(series, |key, _value| {
                if key == "__name__" {
                    return;
                }
                let key_hash = hash_u64(key.as_bytes());
                union_key_hashes.insert(key_hash);
                key_hashes.push(key_hash);
            });

            key_hashes.sort_unstable();
            key_hashes.dedup();

            rows.push(TopMetricSeriesRow {
                metric_rank: metric_rank + 1,
                series_rank: series_rank + 1,
                total_cardinality: series_item.score,
                metric_name: metric.metric_name.clone(),
                key_hashes,
            });
        }
    }

    if rows.is_empty() {
        return None;
    }

    let mut columns: Vec<KeyColumn> = union_key_hashes
        .into_iter()
        .map(|key_hash| {
            let stats = per_key.get(&key_hash);
            let (key, cardinality_display) = match stats {
                Some(stats) => (stats.key.clone(), stats.cardinality_display().into()),
                None => ("(unknown)".into(), "n/a".into()),
            };
            let cardinality_for_sort = key_cardinality_for_score
                .get(&key_hash)
                .copied()
                .unwrap_or_default();
            KeyColumn {
                key_hash,
                key,
                cardinality_display,
                cardinality_for_sort,
            }
        })
        .collect();

    columns.sort_by(|a, b| {
        b.cardinality_for_sort
            .cmp(&a.cardinality_for_sort)
            .then_with(|| a.key.cmp(&b.key))
    });

    Some(TopMetricsByTotalKeyCardinalityReport {
        metrics_limit: TOP_METRICS_LIMIT,
        series_per_metric_limit: TOP_SERIES_PER_METRIC_LIMIT,
        columns,
        rows,
    })
}

pub fn per_key_value_stats_markdown_from_store(
    store: &impl LabelSetStore,
    series_limit: Option<usize>,
) -> String {
    per_key_value_stats_from_store(store, series_limit).to_markdown()
}

pub struct LabelTagStats {
    pub series_total: usize,
    pub series_scanned: usize,

    /// Avg key length per series (bytes/label).
    pub key_len: Stats<u32>,
    /// Avg value length per series (bytes/label).
    pub value_len: Stats<u32>,
    /// Max key length within a series (bytes).
    pub key_len_max: Stats<u32>,
    /// Max value length within a series (bytes).
    pub value_len_max: Stats<u32>,

    pub key_total_bytes_per_series: Stats<u64>,
    pub value_total_bytes_per_series: Stats<u64>,

    pub labels: Stats<u32>,
    pub labelset_bytes: Stats<u64>,
}

impl LabelTagStats {
    pub fn new() -> Self {
        Self {
            series_total: 0,
            series_scanned: 0,
            key_len: Stats::<u32>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            value_len: Stats::<u32>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            key_len_max: Stats::<u32>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            value_len_max: Stats::<u32>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            key_total_bytes_per_series: Stats::<u64>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            value_total_bytes_per_series: Stats::<u64>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            labels: Stats::<u32>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            labelset_bytes: Stats::<u64>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.series_total = self.series_total.max(other.series_total);
        self.series_scanned = self.series_scanned.saturating_add(other.series_scanned);

        self.key_len.merge(other.key_len);
        self.value_len.merge(other.value_len);
        self.key_len_max.merge(other.key_len_max);
        self.value_len_max.merge(other.value_len_max);
        self.key_total_bytes_per_series
            .merge(other.key_total_bytes_per_series);
        self.value_total_bytes_per_series
            .merge(other.value_total_bytes_per_series);
        self.labels.merge(other.labels);
        self.labelset_bytes.merge(other.labelset_bytes);
    }

    pub fn to_markdown(&self) -> String {
        let mut md = String::new();
        md.push_str("### Label Tag Statistics\n\n");
        md.push_str(&format!(
            "Computed by scanning the `LabelSetStore`: `series_scanned={}` out of `series_total={}`.\n\n",
            self.series_scanned, self.series_total
        ));
        md.push_str("| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |\n");
        md.push_str(
            "|---|---|---|---|---|---|---|---|---|---|
",
        );

        if let Some(s) = self.labels.summarize() {
            md.push_str(&s.to_markdown_row("Labels per Series"));
        }
        if let Some(s) = self.key_len.summarize() {
            md.push_str(&s.to_markdown_row("Avg Key Length/Series (B/label)"));
        }
        if let Some(s) = self.value_len.summarize() {
            md.push_str(&s.to_markdown_row("Avg Value Length/Series (B/label)"));
        }
        if let Some(s) = self.key_len_max.summarize() {
            md.push_str(&s.to_markdown_row("Key Len Max/Series (B)"));
        }
        if let Some(s) = self.value_len_max.summarize() {
            md.push_str(&s.to_markdown_row("Value Len Max/Series (B)"));
        }
        if let Some(s) = self.key_total_bytes_per_series.summarize() {
            md.push_str(&s.to_markdown_row("Key Total Bytes/Series"));
        }
        if let Some(s) = self.value_total_bytes_per_series.summarize() {
            md.push_str(&s.to_markdown_row("Value Total Bytes/Series"));
        }
        if let Some(s) = self.labelset_bytes.summarize() {
            md.push_str(&s.to_markdown_row("LabelSet Total Bytes/Series"));
        }

        md.push('\n');
        md
    }
}

impl Default for LabelTagStats {
    fn default() -> Self {
        Self::new()
    }
}

pub fn label_tag_stats_from_store(
    store: &(impl LabelSetStore + Sync),
    series_limit: Option<usize>,
) -> LabelTagStats {
    let store_series = store.len().min(u32::MAX as usize);
    let scan_series = series_limit.unwrap_or(store_series).min(store_series);

    let mut stats = if scan_series < 10_000 {
        let mut stats = LabelTagStats::new();
        for series_index in 0..scan_series {
            let series = SeriesRef::new(series_index as u32);

            let mut labels: u32 = 0;
            let mut key_bytes: u64 = 0;
            let mut value_bytes: u64 = 0;
            let mut key_len_max: u32 = 0;
            let mut value_len_max: u32 = 0;

            store.visit_labelset(series, |key, value| {
                labels = labels.saturating_add(1);

                let key_len = key.len().min(u32::MAX as usize) as u32;
                let value_len = value.len().min(u32::MAX as usize) as u32;

                key_bytes = key_bytes.saturating_add(key_len as u64);
                value_bytes = value_bytes.saturating_add(value_len as u64);
                key_len_max = key_len_max.max(key_len);
                value_len_max = value_len_max.max(value_len);
            });

            stats.labels.insert(labels);

            let labels_nonzero = labels.max(1) as u64;
            let avg_key_len = (key_bytes / labels_nonzero).min(u64::from(u32::MAX)) as u32;
            let avg_value_len = (value_bytes / labels_nonzero).min(u64::from(u32::MAX)) as u32;

            stats.key_len.insert(avg_key_len);
            stats.value_len.insert(avg_value_len);
            stats.key_len_max.insert(key_len_max);
            stats.value_len_max.insert(value_len_max);
            stats.key_total_bytes_per_series.insert(key_bytes);
            stats.value_total_bytes_per_series.insert(value_bytes);

            let labelset_bytes = key_bytes.saturating_add(value_bytes);
            stats.labelset_bytes.insert(labelset_bytes);
        }
        stats
    } else {
        use rayon::prelude::*;

        const CHUNK_SERIES: usize = 16_384;
        let chunk_count = scan_series.div_ceil(CHUNK_SERIES);

        let mut partials: Vec<(usize, LabelTagStats)> = (0..chunk_count)
            .into_par_iter()
            .map(|chunk_index| {
                let start = chunk_index * CHUNK_SERIES;
                let end = (start + CHUNK_SERIES).min(scan_series);

                let mut stats = LabelTagStats::new();
                for series_index in start..end {
                    let series = SeriesRef::new(series_index as u32);

                    let mut labels: u32 = 0;
                    let mut key_bytes: u64 = 0;
                    let mut value_bytes: u64 = 0;
                    let mut key_len_max: u32 = 0;
                    let mut value_len_max: u32 = 0;

                    store.visit_labelset(series, |key, value| {
                        labels = labels.saturating_add(1);

                        let key_len = key.len().min(u32::MAX as usize) as u32;
                        let value_len = value.len().min(u32::MAX as usize) as u32;

                        key_bytes = key_bytes.saturating_add(key_len as u64);
                        value_bytes = value_bytes.saturating_add(value_len as u64);
                        key_len_max = key_len_max.max(key_len);
                        value_len_max = value_len_max.max(value_len);
                    });

                    stats.labels.insert(labels);

                    let labels_nonzero = labels.max(1) as u64;
                    let avg_key_len = (key_bytes / labels_nonzero).min(u64::from(u32::MAX)) as u32;
                    let avg_value_len =
                        (value_bytes / labels_nonzero).min(u64::from(u32::MAX)) as u32;

                    stats.key_len.insert(avg_key_len);
                    stats.value_len.insert(avg_value_len);
                    stats.key_len_max.insert(key_len_max);
                    stats.value_len_max.insert(value_len_max);
                    stats.key_total_bytes_per_series.insert(key_bytes);
                    stats.value_total_bytes_per_series.insert(value_bytes);

                    let labelset_bytes = key_bytes.saturating_add(value_bytes);
                    stats.labelset_bytes.insert(labelset_bytes);
                }
                (chunk_index, stats)
            })
            .collect();

        partials.sort_by_key(|(chunk_index, _)| *chunk_index);

        let mut stats = LabelTagStats::new();
        for (_, part) in partials {
            stats.merge(part);
        }
        stats
    };

    stats.series_total = store_series;
    stats.series_scanned = scan_series;
    stats
}

pub struct LatencySamples {
    pub msg_total_ns: Stats<Duration>,
    pub dp_total_ns: Stats<Duration>,
    pub dp_intern_ns: Stats<Duration>,
    pub dp_build_ns: Stats<Duration>,
    pub datapoints_per_msg: Stats<u32>,
    pub msg_seen: u64,
    pub dp_seen: u64,
}

impl LatencySamples {
    pub fn new() -> Self {
        Self {
            msg_total_ns: Stats::<Duration>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            dp_total_ns: Stats::<Duration>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            dp_intern_ns: Stats::<Duration>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            dp_build_ns: Stats::<Duration>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            datapoints_per_msg: Stats::<u32>::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            msg_seen: 0,
            dp_seen: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.msg_total_ns.is_empty()
    }

    pub fn msg_sample_count(&self) -> usize {
        self.msg_total_ns.count() as usize
    }

    pub fn dp_sample_count(&self) -> usize {
        self.dp_total_ns.count() as usize
    }

    pub fn to_markdown(&self) -> String {
        let mut md = String::new();
        md.push_str("### Latency Statistics\n\n");
        md.push_str("| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |\n");
        md.push_str("|---|---|---|---|---|---|---|---|---|---|\n");

        if let Some(s) = self.msg_total_ns.summarize() {
            md.push_str(&s.to_markdown_row("Message Total"));
        }
        if let Some(s) = self.dp_total_ns.summarize() {
            md.push_str(&s.to_markdown_row("DP Total"));
        }
        if let Some(s) = self.dp_intern_ns.summarize() {
            md.push_str(&s.to_markdown_row("DP Intern"));
        }
        if let Some(s) = self.dp_build_ns.summarize() {
            md.push_str(&s.to_markdown_row("DP Build"));
        }
        if let Some(s) = self.datapoints_per_msg.summarize() {
            md.push_str(&s.to_markdown_row("DPs per Msg"));
        }

        md.push('\n');
        md
    }

    pub fn record(&mut self, total: Duration, intern: Duration, build: Duration, datapoints: u64) {
        let datapoints_u32 = datapoints.min(u64::from(u32::MAX)) as u32;

        self.msg_seen += 1;
        self.msg_total_ns.insert(total);
        self.datapoints_per_msg.insert(datapoints_u32);

        if datapoints == 0 {
            return;
        }

        let dp = datapoints_u32.max(1);
        let dp_total = total / dp;
        let dp_intern = intern / dp;
        let dp_build = build / dp;

        self.dp_seen = self.dp_seen.saturating_add(datapoints);
        self.dp_total_ns.insert(dp_total);
        self.dp_intern_ns.insert(dp_intern);
        self.dp_build_ns.insert(dp_build);
    }
}

impl Default for LatencySamples {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
