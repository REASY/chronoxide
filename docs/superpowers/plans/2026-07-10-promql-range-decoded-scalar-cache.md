# PromQL Range Decoded Scalar Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded, range-call-local decoded typed-scalar-lane cache and keep it only if paired real-replay benchmarks improve both targeted scalar queries by at least 10% without correctness, profile, memory, or control-workload regressions.

**Architecture:** First add result/corpus fingerprints and freeze pre-change result and error oracles without touching either range executor. Then add an exact-arena cache behind per-call and process-wide governors, split logical payload observation from physical reads, and thread the cache only through sealed session range evaluation. The cache experiment stays uncommitted until the final benchmark gate; if it misses, reverse only those cache changes and retain the measurement/oracle commits.

**Tech Stack:** Rust 2024, `sha2`, `allocator-api2`, sealed segment V7 readers, PromQL range evaluation, `serde_json`, macOS `/usr/bin/time -l`, Cargo tests.

---

## Working-Tree and Commit Boundaries

Work in the current branch, as requested. Do not create a worktree.

Do not stage or rewrite these pre-existing user changes:

- `chronoxide-core/src/storage/segment/query_promql.rs`
- `chronoxide-core/tests/prometheus_golden.rs`
- `chronoxide-core/tests/promql_query.rs`
- `docs/superpowers/specs/storage.md`
- smoke configs, `data/`, logs, and ingestion reports

Create these focused files:

- `chronoxide-core/src/storage/segment/query_fingerprint.rs`
- `chronoxide-core/src/storage/segment/corpus_fingerprint.rs`
- `chronoxide-core/src/storage/segment/range_scalar_cache.rs`
- `chronoxide-core/src/storage/segment/range_scalar_cache_tests.rs`
- `chronoxide-core/tests/support/promql_range_scalar_cache.rs`
- `chronoxide-core/tests/promql_range_prechange_oracle.rs`
- `chronoxide-core/tests/promql_range_scalar_cache_oracle.rs`
- `chronoxide-core/tests/promql_range_scalar_cache_errors.rs`
- `chronoxide-core/tests/promql_range_scalar_cache_lifecycle.rs`
- `docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-results-v1.json`
- `docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-errors-v1.json`

Modify only clean production files: workspace/core Cargo files and lockfile,
`segment/mod.rs`, `query_types.rs`, `query_context.rs`, `query_reader.rs`,
`query_store.rs`, `chunk/types.rs`, `chunk/reader.rs`, `chunk/codec.rs`, the
query benchmark binary, and its clean test module.

Tasks 1–4 are measurement/correctness infrastructure and receive focused commits.
Record the Task 4 checkpoint hash in both artifacts. Tasks 5–9 form one cache
experiment and remain uncommitted until Task 10 proves the gate.

### Task 1: Versioned Semantic Result Fingerprint

**Files:**

- Modify: `Cargo.toml`
- Modify: `chronoxide-core/Cargo.toml`
- Modify: `chronoxide-core/src/storage/segment/mod.rs`
- Create/Test: `chronoxide-core/src/storage/segment/query_fingerprint.rs`

- [ ] **Step 1: Add failing fingerprint tests**

Build `QueryExecution` values directly. Prove returned series/label order,
signed zero, ordinary/stale NaN payloads, raw metadata-vector length, reset hint,
start time, and temporality all affect the digest.

~~~rust
fn execution_with_one_sample(value_bits: u64) -> QueryExecution {
    QueryExecution {
        results: vec![SegmentQueryResult {
            series_id: 7,
            labels: shared_query_labels(vec![
                ("__name__".to_string(), "fingerprint_metric".to_string()),
            ]),
            samples: vec![(1_000, f64::from_bits(value_bits))],
            counter_reset_hints: Vec::new(),
            sample_start_times: Vec::new(),
            temporality: QueryResultTemporality::Unknown,
        }],
        stats: QueryStats::default(),
    }
}

#[test]
fn query_execution_semantic_fingerprint_changes_for_private_metadata() {
    let base = execution_with_one_sample(0.0_f64.to_bits());
    let mut changed = base.clone();
    changed.results[0].sample_start_times = vec![Some(1234)];
    assert_ne!(
        base.semantic_fingerprint_sha256(),
        changed.semantic_fingerprint_sha256()
    );
}

#[test]
fn query_execution_semantic_fingerprint_distinguishes_nan_payloads() {
    let left = execution_with_one_sample(0x7ff8_0000_0000_0042);
    let right = execution_with_one_sample(prometheus_stale_nan().to_bits());
    assert_ne!(
        left.semantic_fingerprint_sha256(),
        right.semantic_fingerprint_sha256()
    );
}
~~~

- [ ] **Step 2: Run and verify RED**

~~~sh
cargo test -p chronoxide-core query_execution_semantic_fingerprint -- --nocapture
~~~

Expected: compilation fails because the fingerprint method/type do not exist.

- [ ] **Step 3: Add `sha2` and implement the canonical fingerprint**

Add `sha2 = "0.10"` to workspace dependencies and
`sha2 = { workspace = true }` to core. Wire the module from `segment/mod.rs`.

~~~rust
pub const QUERY_EXECUTION_FINGERPRINT_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct QueryExecutionFingerprint([u8; 32]);

impl QueryExecutionFingerprint {
    pub fn as_bytes(&self) -> &[u8; 32] { &self.0 }

    pub fn to_hex(self) -> String {
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
        }
        out
    }
}

impl QueryExecution {
    pub fn semantic_fingerprint_sha256(&self) -> QueryExecutionFingerprint {
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"chronoxide/query-execution-fingerprint");
        digest.update(QUERY_EXECUTION_FINGERPRINT_VERSION.to_le_bytes());
        update_u64(&mut digest, self.results.len() as u64);
        for result in &self.results {
            update_result(&mut digest, result);
        }
        QueryExecutionFingerprint(digest.finalize().into())
    }
}
~~~

Encode lengths as `u64`, integers little-endian, labels/results in returned
order, values with `f64::to_bits()`, and enums through explicit `match` values.
Hash raw reset/start-time vector lengths; do not use normalized optional accessors.

- [ ] **Step 4: Verify and commit Task 1 only**

~~~sh
cargo test -p chronoxide-core query_execution_semantic_fingerprint -- --nocapture
cargo test -p chronoxide-core storage::segment --lib -- --nocapture
git diff --check
git add Cargo.toml Cargo.lock chronoxide-core/Cargo.toml \
  chronoxide-core/src/storage/segment/mod.rs \
  chronoxide-core/src/storage/segment/query_fingerprint.rs
git diff --cached --check
git commit -m "feat(query): add semantic execution fingerprints"
~~~

### Task 2: Segment-Corpus Fingerprint

**Files:**

- Modify: `chronoxide-core/src/storage/segment/mod.rs`
- Create/Test: `chronoxide-core/src/storage/segment/corpus_fingerprint.rs`

- [ ] **Step 1: Add failing corpus tests**

Create identical fixture stores with reversed directory enumeration and assert
equal identities. Mutate metadata, footer checksum metadata, and tracked file
length separately and assert the identity changes or validation fails.

~~~rust
fn open_fixture_store(starts: &[u64]) -> (tempfile::TempDir, SegmentStoreReader) {
    let tempdir = tempfile::tempdir().unwrap();
    for &start_ms in starts {
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(1))
            .with_deterministic_segment_ids(start_ms);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(start_ms, 1.0)],
                |visit| visit(METRIC_NAME_LABEL, "corpus_fixture"),
            )
            .unwrap();
        writer.flush().unwrap();
    }
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    (tempdir, store)
}

#[test]
fn segment_corpus_fingerprint_is_independent_of_directory_enumeration() {
    let (_left_dir, left) = open_fixture_store(&[2000, 1000]);
    let (_right_dir, right) = open_fixture_store(&[1000, 2000]);
    assert_eq!(
        left.corpus_fingerprint_sha256().unwrap(),
        right.corpus_fingerprint_sha256().unwrap()
    );
}
~~~

- [ ] **Step 2: Run and verify RED**

~~~sh
cargo test -p chronoxide-core segment_corpus_fingerprint -- --nocapture
~~~

Expected: the corpus API is missing.

- [ ] **Step 3: Implement the versioned identity**

~~~rust
pub const SEGMENT_CORPUS_FINGERPRINT_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SegmentCorpusFingerprint([u8; 32]);

impl SegmentCorpusFingerprint {
    pub fn as_bytes(&self) -> &[u8; 32] { &self.0 }

    pub fn to_hex(self) -> String {
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
        }
        out
    }
}

impl SegmentStoreReader {
    pub fn corpus_fingerprint_sha256(
        &self,
    ) -> io::Result<SegmentCorpusFingerprint> {
        let mut digest = Sha256::new();
        digest.update(b"chronoxide/segment-corpus-fingerprint");
        digest.update(SEGMENT_CORPUS_FINGERPRINT_VERSION.to_le_bytes());
        update_u64(&mut digest, self.segments.len() as u64);
        for segment in &self.segments {
            update_segment_id_and_meta(&mut digest, segment)?;
            update_sorted_footer_entries(&mut digest, segment)?;
        }
        Ok(SegmentCorpusFingerprint(digest.finalize().into()))
    }
}
~~~

Hash sorted selected inventory, segment ID, canonical `SegmentMeta` fields, and
footer entries sorted by filename: filename, stored length, and stored xxhash64.
Check actual lengths. Do not use the whole-file footer validator during RSS runs;
it loads tracked files into large transient vectors.

- [ ] **Step 4: Verify and commit**

~~~sh
cargo test -p chronoxide-core segment_corpus_fingerprint -- --nocapture
git diff --check
git add chronoxide-core/src/storage/segment/mod.rs \
  chronoxide-core/src/storage/segment/corpus_fingerprint.rs
git diff --cached --check
git commit -m "feat(query): fingerprint sealed segment corpora"
~~~

### Task 3: Reproducible Benchmark Output

**Files:**

- Modify: `chronoxide-ingester/src/bin/chronoxide-query.rs`
- Modify: `chronoxide-ingester/src/bin/chronoxide_query/tests.rs`

- [ ] **Step 1: Add failing CLI/report tests**

Test `--raw-output`, corpus identity in Markdown, per-run semantic fingerprints,
integer nanosecond durations, every `QueryStats` field, and odd/even medians.

~~~rust
#[test]
fn median_duration_averages_the_even_middle_pair() {
    assert_eq!(
        median_duration(vec![
            Duration::from_millis(40),
            Duration::from_millis(10),
            Duration::from_millis(30),
            Duration::from_millis(20),
        ]),
        Some(Duration::from_millis(25))
    );
}
~~~

- [ ] **Step 2: Run and verify RED**

~~~sh
cargo test -p chronoxide-ingester --bin chronoxide-query warm_median -- --nocapture
cargo test -p chronoxide-ingester --bin chronoxide-query raw_benchmark -- --nocapture
cargo test -p chronoxide-ingester --bin chronoxide-query corpus_fingerprint -- --nocapture
~~~

Expected: new CLI/report fields are absent.

- [ ] **Step 3: Implement raw report and fingerprints**

Add `raw_output: Option<PathBuf>` to args/config and these report fields:

~~~rust
struct QueryBenchmarkReport {
    corpus_fingerprint: SegmentCorpusFingerprint,
    corpus_fingerprint_duration: Duration,
    // retain existing fields
}

struct QueryBenchmarkResult {
    // retain existing fields
    semantic_fingerprint: QueryExecutionFingerprint,
}

#[derive(Serialize)]
struct QueryBenchmarkRawRunV1 {
    query: String,
    run_kind: &'static str,
    run_index: usize,
    duration_ns: u64,
    semantic_fingerprint_sha256: String,
    result_series: u64,
    result_samples: u64,
    stats: RawQueryStatsV1,
}

#[derive(Serialize)]
struct RawQueryStatsV1 {
    segments_considered: u64,
    segments_skipped_by_time: u64,
    segments_skipped_by_missing_equality: u64,
    segments_skipped_by_matcher_time_range: u64,
    segments_queried: u64,
    matched_series: u64,
    projected_series: u64,
    chunk_reads: u64,
    bytes_read: u64,
    samples_decoded: u64,
    typed_scalar_chunks_decoded: u64,
    typed_full_chunks_decoded: u64,
    regex_values_examined: u64,
    index_postings_reads: u64,
    index_postings_bytes_read: u64,
}
~~~

Mirror all limit/stat fields in local versioned raw structs using schema
`chronoxide.query-benchmark.raw/v1`. Compute the fingerprint before moving
results into the report.

- [ ] **Step 4: Implement warm median**

~~~rust
fn median_duration(mut values: Vec<Duration>) -> Option<Duration> {
    if values.is_empty() { return None; }
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        Some(values[middle])
    } else {
        Some(duration_div(values[middle - 1] + values[middle], 2))
    }
}
~~~

Render Warm Median beside mean/min/max. Preserve every raw repeat duration as
integer nanoseconds.

- [ ] **Step 5: Verify and commit**

~~~sh
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
git diff --check
git add chronoxide-ingester/src/bin/chronoxide-query.rs \
  chronoxide-ingester/src/bin/chronoxide_query/tests.rs
git diff --cached --check
git commit -m "perf(query): report reproducible range benchmark data"
~~~

### Task 4: Freeze the Independent Pre-Change Oracle

**Files:**

- Create: `chronoxide-core/tests/support/promql_range_scalar_cache.rs`
- Create: `chronoxide-core/tests/promql_range_prechange_oracle.rs`
- Create: `docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-results-v1.json`
- Create: `docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-errors-v1.json`

- [ ] **Step 1: Add explicit typed semantic fixtures**

Use public `SegmentWriter` typed-recording APIs. Assert explicit labels,
timestamps, value bits, stats, and errors for stale -> reset -> delta
continuation, missing sum vs ordinary NaN, mixed/unspecified temporality within
and across chunks/segments, start-time changes, `>2^53` counts, duplicate
keep-last, and offsets.

~~~rust
struct TypedRangeFixture {
    _tempdir: tempfile::TempDir,
    store: SegmentStoreReader,
}

fn write_stale_reset_delta_fixture() -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(60),
    )).unwrap();
    let value = |count, flags, reset_hint| HistogramValue {
        count,
        sum: Some(count as f64),
        min: None,
        max: None,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(0),
            flags,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint,
        },
        explicit_bounds: vec![1.0],
        bucket_counts: vec![count, 0],
    };
    writer.record_histogram_samples_ordered_with_label_visitor(
        SeriesRef::new(1),
        &[
            (10_000, value(2, 0, CounterResetHint::NotCounterReset)),
            (20_000, value(3, 0, CounterResetHint::NotCounterReset)),
            (30_000, value(0, OTLP_FLAG_NO_RECORDED_VALUE, CounterResetHint::Unknown)),
            (40_000, value(7, 0, CounterResetHint::CounterReset)),
            (50_000, value(4, 0, CounterResetHint::NotCounterReset)),
        ],
        |visit| visit(METRIC_NAME_LABEL, "cache"),
    ).unwrap();
    writer.flush().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    TypedRangeFixture { _tempdir: tempdir, store }
}

const LARGE_COUNT: u64 = (1_u64 << 53) + 1;
const ORDINARY_NAN_BITS: u64 = 0x7ff8_0000_0000_0042;

fn run_fixture_range(query: &str) -> QueryExecution {
    let fixture = write_stale_reset_delta_fixture();
    fixture
        .store
        .query_promql_range_with_limits(
            query,
            30_000,
            50_000,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap()
}

fn sample_bits(execution: &QueryExecution) -> Vec<(u64, u64)> {
    execution
        .results
        .iter()
        .flat_map(|result| result.samples.iter())
        .map(|(timestamp_ms, value)| (*timestamp_ms, value.to_bits()))
        .collect()
}

#[test]
fn prechange_stale_reset_delta_continuation_is_explicit() {
    let execution = run_fixture_range("last_over_time(cache_count[30s])");
    assert_eq!(sample_bits(&execution), vec![
        (30_000, 5.0_f64.to_bits()),
        (40_000, 7.0_f64.to_bits()),
        (50_000, 11.0_f64.to_bits()),
    ]);
}
~~~

- [ ] **Step 2: Add exact pre-change error rows**

~~~rust
#[derive(Serialize)]
struct ErrorOracleRow {
    id: String,
    api: &'static str,
    expression: String,
    start_ms: u64,
    end_ms: u64,
    step_ms: u64,
    chunk_order: &'static str,
    variant: &'static str,
    message: String,
}

fn error_variant(error: &PromqlQueryError) -> &'static str {
    match error {
        PromqlQueryError::Invalid(_) => "invalid",
        PromqlQueryError::Unsupported(_) => "unsupported",
        PromqlQueryError::LimitExceeded { .. } => "limit_exceeded",
        PromqlQueryError::Storage(_) => "storage",
    }
}
~~~

Record parse-before-bounds, zero-step-before-reversed-bounds, direct/session
mapping, and every corruption/series/chunk/byte/sample/projected-series
precedence row in both relevant chunk orders.

- [ ] **Step 3: Run the pre-change oracle**

~~~sh
cargo test -p chronoxide-core --test promql_range_prechange_oracle -- --nocapture
~~~

Expected: explicit semantic/error assertions pass on the current executor.

- [ ] **Step 4: Capture the replay baseline**

~~~sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query
./target/release/chronoxide-query \
  --segments-dir data/perf/segment-index-v7/segments-replay-v7-no-record-index \
  --output data/perf/segment-index-v7/full-replay-no-record-index/reports/promql-range-prechange-v1.md \
  --raw-output docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-results-v1.json \
  --start-ms 1782982800000 --end-ms 1782986400000 --step-ms 60000 \
  --benchmark-repeats 5 \
  --query 'rate(go_gc_duration_seconds_count[15m])' \
  --query 'sum by (service_name_x55e50a58f9befba7)(rate(go_gc_duration_seconds_count[15m]))' \
  --query 'histogram_quantile(0.95, sum by (service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257[15m])))'
~~~

Verify 15 runs, one corpus identity, stable per-expression fingerprints/stats,
and raw timings. Serialize the deterministic error rows to the versioned error
artifact through the same Rust fixture builder used by the test.

- [ ] **Step 5: Record identity, verify, and commit the checkpoint**

~~~sh
git rev-parse HEAD
shasum -a 256 target/release/chronoxide-query
rustc -Vv
cargo -V
sw_vers
git diff --check
git add chronoxide-core/tests/support/promql_range_scalar_cache.rs \
  chronoxide-core/tests/promql_range_prechange_oracle.rs \
  docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-results-v1.json \
  docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-errors-v1.json
git diff --cached --check
git commit -m "test(query): record pre-cache range oracle"
~~~

### Task 5: Exact Arenas and Process Governor

**Files:**

- Modify: `Cargo.toml`, `Cargo.lock`, `chronoxide-core/Cargo.toml`
- Modify: `chronoxide-core/src/storage/segment/mod.rs`
- Modify: `chronoxide-core/src/storage/chunk/types.rs`
- Create: `chronoxide-core/src/storage/segment/range_scalar_cache.rs`
- Create: `chronoxide-core/src/storage/segment/range_scalar_cache_tests.rs`

Do not commit Tasks 5–9.

- [ ] **Step 1: Add failing arena/governor tests**

Test exact layout charge, sorted insertion, partial-decode rollback, injected
first/second allocation failure, CAS admission, identical/conflicting
initialization, monotonic peak, and lease release.

Define `FailingAllocator` in the test module with shared
`Arc<AtomicUsize>` call count and `fail_on_call: usize`; its unsafe
`Allocator` implementation returns `AllocError` on that allocation call and
delegates all other allocation/deallocation to `allocator_api2::alloc::Global`.

~~~rust
#[test]
fn allocator_failure_releases_every_charge() {
    let allocator = FailingAllocator::fail_on_call(2);
    let governor = Arc::new(RangeScalarCacheGovernor::new(16 * MIB));
    let result = RangeScalarDecodeCache::try_new_in(8 * MIB, governor, allocator);
    assert!(result.is_err());
    assert_eq!(governor.stats().current_leased_bytes, 0);
}
~~~

- [ ] **Step 2: Run and verify RED**

~~~sh
cargo test -p chronoxide-core range_scalar_cache -- --nocapture
~~~

Expected: cache types do not exist.

- [ ] **Step 3: Add `allocator-api2` and exact arenas**

Add workspace/core dependency `allocator-api2 = "0.2.21"`.

~~~rust
const MIB: u64 = 1024 * 1024;
pub const DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES: u64 = 16 * MIB;
pub const MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES: u64 = 32 * MIB;
pub const DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES: u64 = 128 * MIB;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RangeScalarCacheConfigError {
    #[error("range scalar cache budget exceeds maximum: requested={requested_bytes} maximum={maximum_bytes}")]
    BudgetTooLarge { requested_bytes: u64, maximum_bytes: u64 },
    #[error("range scalar cache governor already initialized with a different limit: existing={existing_bytes} requested={requested_bytes}")]
    GovernorAlreadyInitialized { existing_bytes: u64, requested_bytes: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkScalarRecordHeader {
    pub series_ref: u32,
    pub kind: ChunkKind,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
    pub sample_count: u32,
}

struct ExactInitArena<T, A: Allocator = Global> {
    slots: allocator_api2::boxed::Box<[MaybeUninit<T>], A>,
    initialized: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RangeScalarCacheKey {
    segment_ordinal: usize,
    file_id: u8,
    chunk_offset: u64,
    chunk_len: u32,
    scalar_lane_offset: u32,
    scalar_lane_len: u32,
    projection: ChunkScalarProjection,
    chunk_kind: ChunkKind,
}

struct RangeScalarCacheEntry {
    key: RangeScalarCacheKey,
    header: ChunkScalarRecordHeader,
    samples_start: usize,
    samples_len: usize,
}

struct RangeScalarDecodeCache<A: Allocator = Global> {
    entries: ExactInitArena<RangeScalarCacheEntry, A>,
    samples: ExactInitArena<ChunkScalarSample, A>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RangeScalarCacheSummary {
    pub configured_budget_bytes: u64,
    pub governor_lease_bytes: u64,
    pub governor_refused: bool,
    pub allocation_refused: bool,
    pub entry_arena_charge_bytes: u64,
    pub sample_arena_charge_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub admitted_entries: u64,
    pub streaming_budget_bypasses: u64,
    pub unsupported_bypasses: u64,
    pub logical_hit_bytes: u64,
    pub logical_miss_or_bypass_bytes: u64,
    pub peak_retained_charge_bytes: u64,
    pub retained_charge_after_finalize: u64,
}

struct RangeScalarCacheCall<A: Allocator = Global> {
    summary: RangeScalarCacheSummary,
    governor: Arc<RangeScalarCacheGovernor>,
    allocator: A,
    lease: Option<RangeScalarCacheLease>,
    cache: Option<RangeScalarDecodeCache<A>>,
}

impl RangeScalarCacheCall<Global> {
    fn new(
        configured_budget_bytes: u64,
        governor: Arc<RangeScalarCacheGovernor>,
    ) -> Self {
        Self {
            summary: RangeScalarCacheSummary {
                configured_budget_bytes,
                ..RangeScalarCacheSummary::default()
            },
            governor,
            allocator: Global,
            lease: None,
            cache: None,
        }
    }
}

impl<A: Allocator> RangeScalarCacheCall<A> {
    fn finish(mut self) -> RangeScalarCacheSummary {
        self.cache.take();
        self.lease.take();
        self.summary.retained_charge_after_finalize = 0;
        self.summary
    }
}
~~~

Keep `MaybeUninit` operations inside `ExactInitArena`. Allocate exact entry/sample
slices once with `try_new_uninit_slice_in`; never grow them. Add
`PartialOrd`/`Ord` derives to `ChunkKind` and `ChunkScalarProjection` so the
complete key has a stable sorted order.

- [ ] **Step 4: Implement global admission**

~~~rust
struct RangeScalarCacheGovernor {
    limit_bytes: u64,
    current_leased_bytes: AtomicU64,
    peak_leased_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RangeScalarCacheGovernorStats {
    pub limit_bytes: u64,
    pub current_leased_bytes: u64,
    pub peak_leased_bytes: u64,
}

struct RangeScalarCacheLease {
    governor: Arc<RangeScalarCacheGovernor>,
    bytes: u64,
}

impl Drop for RangeScalarCacheLease {
    fn drop(&mut self) {
        self.governor
            .current_leased_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

static RANGE_SCALAR_CACHE_GOVERNOR:
    OnceLock<Arc<RangeScalarCacheGovernor>> = OnceLock::new();

impl RangeScalarCacheGovernor {
    fn new(limit_bytes: u64) -> Self {
        Self {
            limit_bytes,
            current_leased_bytes: AtomicU64::new(0),
            peak_leased_bytes: AtomicU64::new(0),
        }
    }

    fn try_acquire(self: &Arc<Self>, bytes: u64) -> Option<RangeScalarCacheLease> {
        let mut current = self.current_leased_bytes.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes)?;
            if next > self.limit_bytes { return None; }
            match self.current_leased_bytes.compare_exchange_weak(
                current, next, Ordering::AcqRel, Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.peak_leased_bytes.fetch_max(next, Ordering::AcqRel);
                    return Some(RangeScalarCacheLease {
                        governor: Arc::clone(self),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn stats(&self) -> RangeScalarCacheGovernorStats {
        RangeScalarCacheGovernorStats {
            limit_bytes: self.limit_bytes,
            current_leased_bytes: self.current_leased_bytes.load(Ordering::Acquire),
            peak_leased_bytes: self.peak_leased_bytes.load(Ordering::Acquire),
        }
    }
}

fn process_range_scalar_cache_governor() -> Arc<RangeScalarCacheGovernor> {
    Arc::clone(RANGE_SCALAR_CACHE_GOVERNOR.get_or_init(|| {
        Arc::new(RangeScalarCacheGovernor::new(
            DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES,
        ))
    }))
}

pub fn configure_range_scalar_cache_governor(
    limit_bytes: u64,
) -> Result<(), RangeScalarCacheConfigError> {
    if let Some(existing) = RANGE_SCALAR_CACHE_GOVERNOR.get() {
        return if existing.limit_bytes == limit_bytes {
            Ok(())
        } else {
            Err(RangeScalarCacheConfigError::GovernorAlreadyInitialized {
                existing_bytes: existing.limit_bytes,
                requested_bytes: limit_bytes,
            })
        };
    }
    let candidate = Arc::new(RangeScalarCacheGovernor::new(limit_bytes));
    match RANGE_SCALAR_CACHE_GOVERNOR.set(candidate) {
        Ok(()) => Ok(()),
        Err(_) => configure_range_scalar_cache_governor(limit_bytes),
    }
}

pub fn range_scalar_cache_governor_stats() -> RangeScalarCacheGovernorStats {
    process_range_scalar_cache_governor().stats()
}
~~~

Use checked CAS, `fetch_max`, and an `Arc`-owning lease whose `Drop` releases
bytes. Identical config is idempotent; conflicting config is typed. Provide
isolated injected governors for tests.

- [ ] **Step 5: Verify foundations**

~~~sh
cargo test -p chronoxide-core range_scalar_cache -- --nocapture
~~~

Expected: arena/key/governor tests pass and leased bytes finish at zero.

### Task 6: Split Logical Observation From Physical Reads

**Files:**

- Modify: `chronoxide-core/src/storage/segment/query_context.rs`
- Modify: `chronoxide-core/src/storage/segment/query_reader.rs`
- Test: `chronoxide-core/src/storage/segment/range_scalar_cache_tests.rs`

- [ ] **Step 1: Add a failing locality-equivalence test**

Use unordered, overlapping, contiguous, 4 KiB-gap, and 64 KiB-gap requests.
Compare the existing combined path with logical observation plus physical-only
read; assert every locality field and `chunk_payload_bytes` is identical.

- [ ] **Step 2: Run and verify RED**

~~~sh
cargo test -p chronoxide-core logical_chunk_observation_matches_combined_reader_profile -- --nocapture
~~~

- [ ] **Step 3: Implement the split with caching inactive**

~~~rust
pub(super) fn observe_chunk_payload_requests(
    &mut self,
    requests: &[ChunkPayloadRead],
);

pub(super) fn read_chunk_payload_batch_physical(
    &mut self,
    reader: &SegmentReader,
    requests: &[ChunkPayloadRead],
) -> io::Result<ChunkPayloadBatch>;
~~~

The observer owns all logical bytes/locality. The physical method owns duration
and actual spans/bytes only. Call both with the complete vector in cache-off mode.

- [ ] **Step 4: Verify equivalence**

~~~sh
cargo test -p chronoxide-core logical_chunk_observation_matches_combined_reader_profile -- --nocapture
cargo test -p chronoxide-core storage::chunk::tests::chunk_payload_batch -- --nocapture
~~~

### Task 7: Range-Call Lifecycle and Direct Delegation

**Files:**

- Modify: `chronoxide-core/src/storage/segment/query_types.rs`
- Modify: `chronoxide-core/src/storage/segment/query_context.rs`
- Modify: `chronoxide-core/src/storage/segment/query_store.rs`
- Create: `chronoxide-core/tests/promql_range_scalar_cache_lifecycle.rs`

- [ ] **Step 1: Add failing lifecycle/configuration tests**

Cover budgets 0..=32 MiB, rejection above 32 MiB, success followed by each
parse/bounds/error path replacing the summary, and direct/session ordering.

~~~rust
const MIB: u64 = 1024 * 1024;

assert_eq!(
    session.set_range_scalar_cache_budget_bytes(32 * MIB + 1),
    Err(RangeScalarCacheConfigError::BudgetTooLarge {
        requested_bytes: 32 * MIB + 1,
        maximum_bytes: 32 * MIB,
    })
);
~~~

- [ ] **Step 2: Run and verify RED**

~~~sh
cargo test -p chronoxide-core --test promql_range_scalar_cache_lifecycle -- --nocapture
~~~

- [ ] **Step 3: Add session state/accessors**

~~~rust
pub struct SegmentStoreQuerySession<'a> {
    // retain existing fields
    pub(super) range_scalar_cache_budget_bytes: u64,
    pub(super) range_scalar_cache_governor: Arc<RangeScalarCacheGovernor>,
    pub(super) last_range_scalar_cache_summary: Option<RangeScalarCacheSummary>,
}

pub fn set_range_scalar_cache_budget_bytes(
    &mut self,
    bytes: u64,
) -> Result<(), RangeScalarCacheConfigError>;

pub fn last_range_scalar_cache_summary(
    &self,
) -> Option<&RangeScalarCacheSummary>;
~~~

- [ ] **Step 4: Wrap the entire public session call**

Clear summary before parse. Avoid `?` outside the inner result closure so
`finish()` always publishes.

~~~rust
let mut cache_call = RangeScalarCacheCall::new(
    self.range_scalar_cache_budget_bytes,
    Arc::clone(&self.range_scalar_cache_governor),
);
let result = (|| {
    let query = parse_query(query)?;
    validate_promql_range_bounds(start_ms, end_ms, step_ms)?;
    self.execute_validated_promql_range_query(
        &query, start_ms, end_ms, step_ms, limits, &mut cache_call,
    )
})();
self.last_range_scalar_cache_summary = Some(cache_call.finish());
result
~~~

- [ ] **Step 5: Delegate direct sealed range APIs**

Preserve direct parse -> bounds -> session-open ordering, invoke the same parsed
session executor, remove only the duplicate sealed executor, and leave head
execution unchanged.

- [ ] **Step 6: Verify lifecycle/oracle**

~~~sh
cargo test -p chronoxide-core --test promql_range_scalar_cache_lifecycle -- --nocapture
cargo test -p chronoxide-core --test promql_range_prechange_oracle -- --nocapture
~~~

### Task 8: Cache-Aware Scalar-Lane Reading

**Files:**

- Modify: `chronoxide-core/src/storage/chunk/types.rs`
- Modify: `chronoxide-core/src/storage/chunk/reader.rs`
- Modify: `chronoxide-core/src/storage/chunk/codec.rs`
- Modify: `chronoxide-core/src/storage/segment/query_context.rs`
- Modify: `chronoxide-core/src/storage/segment/query_reader.rs`
- Create: `chronoxide-core/tests/promql_range_scalar_cache_oracle.rs`
- Create: `chronoxide-core/tests/promql_range_scalar_cache_errors.rs`

- [ ] **Step 1: Add one failing overlapping-step hit test**

In one top-level range call compare budget zero/nonzero results, fingerprint,
`QueryStats`, session stats, and normalized logical profile. Require misses,
admissions, hits, strict physical-byte reduction, and zero final charge.

- [ ] **Step 2: Run and verify RED**

~~~sh
cargo test -p chronoxide-core --test promql_range_scalar_cache_oracle range_scalar_cache_session -- --nocapture
~~~

Expected: no hits and no physical-byte reduction.

- [ ] **Step 3: Expose validated scalar header/callback**

~~~rust
pub(crate) fn for_each_indexed_scalar_projection_sample_with_header<F>(
    &self,
    entry: &ChunkIndexEntry,
    projection: ChunkScalarProjection,
    on_sample: F,
) -> io::Result<(ChunkScalarRecordHeader, u32)>
where
    F: FnMut(ChunkScalarSample) -> io::Result<()>;
~~~

Keep the existing callback wrapper. Validate body, checksum, trailing bytes, and
sample count before admission.

- [ ] **Step 4: Thread call state only through sealed session evaluation**

Pass `Option<&mut RangeScalarCacheCall>` through recursive session instant
evaluation and selector querying. Public instant/head paths pass `None`.
Enumerate sorted segments for the stable cache-key ordinal.

- [ ] **Step 5: Reuse the unchanged request vector**

Build/charge/observe all `(offset, len)` requests. Clear while retaining capacity,
rewalk planned chunks with stack keys, and repopulate misses/bypasses. Complete
the physical batch before processing. Hits iterate arena samples; misses reserve
the complete header count and commit only after validation.

- [ ] **Step 6: Preserve logical accounting/error order**

Keep chunk/byte charges during planning, typed chunk/sample charges after
successful processing, and projected-series charges after result construction.
Count summary hit/miss bytes only during classification, not the processing
lookup.

- [ ] **Step 7: Verify focused cache/errors**

~~~sh
cargo test -p chronoxide-core --test promql_range_scalar_cache_oracle -- --nocapture
cargo test -p chronoxide-core --test promql_range_scalar_cache_errors -- --nocapture
cargo test -p chronoxide-core range_scalar_cache -- --nocapture
~~~

### Task 9: Complete Matrix and Benchmark Cache Reporting

**Files:**

- Modify: `chronoxide-ingester/src/bin/chronoxide-query.rs`
- Modify: `chronoxide-ingester/src/bin/chronoxide_query/tests.rs`
- Expand: cache oracle/error/lifecycle and internal test files from Tasks 5–8

- [ ] **Step 1: Complete semantic/bypass fixtures**

Cover Histogram/ExponentialHistogram/Summary × count/sum, absent sum, ordinary
NaN vs stale, cumulative/delta/unspecified/mixed temporality, reset/start-time
boundaries, `>2^53` count, duplicates, offsets, no-lane fallback, and
`file_id != 0`. Compare explicit pre-change values and both modes' fingerprints,
stats, session stats, and logical profiles.

- [ ] **Step 2: Complete corruption/precedence fixtures**

Cover malformed lane offset/length, truncation, lane CRC, magic/version/body/
trailing bytes, fallback full-record CRC, every precedence row/order/hit state,
budget/table refusal, and injected allocation failure. Compare typed variant and
exact message with the Task 4 artifact.

- [ ] **Step 3: Complete lifecycle/concurrency**

With an isolated 16 MiB governor and six barrier-synchronized 8 MiB calls,
assert at most two leases, refused calls stream bit-exactly, global peak ≤16 MiB,
and all per-call/global current charges return to zero.

- [ ] **Step 4: Add CLI cache configuration/summary**

Add `--range-scalar-cache-max-bytes`, validate 0..=32 MiB and range-only use,
set each session budget, and emit every `RangeScalarCacheSummary` field in
Markdown/raw JSON.

- [ ] **Step 5: Run all tests before timing**

~~~sh
cargo test -p chronoxide-core --test promql_range_prechange_oracle -- --nocapture
cargo test -p chronoxide-core --test promql_range_scalar_cache_oracle -- --nocapture
cargo test -p chronoxide-core --test promql_range_scalar_cache_errors -- --nocapture
cargo test -p chronoxide-core --test promql_range_scalar_cache_lifecycle -- --nocapture
cargo test -p chronoxide-core range_scalar_cache -- --nocapture
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
cargo test -p chronoxide-core
git diff --check
~~~

Expected: zero failures. Do not commit the cache based only on tests.

### Task 10: Cap Sweep, Paired Benchmark, and Conditional Commit

**Files:**

- Generate untracked runtime reports under `data/perf/segment-index-v7/full-replay-no-record-index/reports/`.
- Add reviewed aggregate evidence to the committed result artifact only if the cache passes.

- [ ] **Step 1: Build one binary and record identity**

~~~sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query
git rev-parse HEAD
shasum -a 256 target/release/chronoxide-query
rustc -Vv
cargo -V
sw_vers
~~~

- [ ] **Step 2: Sweep 4, 8, 16, and 32 MiB**

For each budget, run the exact three-query 61-step workload with five repeats and
raw output. Choose the smallest budget whose scalar median is within two
percentage points of the best accepted budget. Record hits/misses/bypasses,
physical bytes, cache peak, and maximum RSS.

- [ ] **Step 3: Run nine fresh-process off/on pairs**

Alternate 0/candidate then candidate/0. Every process uses five repeats and
`/usr/bin/time -l`. Preserve every Markdown, raw JSON, and time output under
`data/`. Do not overlap replay, builds, sampling, or other heavy work.

- [ ] **Step 4: Evaluate the exact gate**

For each process/query, take the median of four warm runs; for each pair compute
`1 - on_median/off_median`. Require:

- median paired improvement ≥10% for both scalar queries;
- on faster in at least eight of nine pairs for both;
- native histogram median paired regression ≤3%;
- process-median CV ≤3% for both modes and every query;
- fingerprints/stats/errors match Task 4 artifacts;
- median paired RSS delta ≤ median cache peak + 4 MiB;
- physical reads/decode misses decrease; and
- every retained/global current charge finalizes at zero.

- [ ] **Step 5A: If it passes, run fresh verification and commit cache files only**

~~~sh
cargo test -p chronoxide-core --test promql_range_prechange_oracle -- --nocapture
cargo test -p chronoxide-core --test promql_range_scalar_cache_oracle -- --nocapture
cargo test -p chronoxide-core --test promql_range_scalar_cache_errors -- --nocapture
cargo test -p chronoxide-core --test promql_range_scalar_cache_lifecycle -- --nocapture
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
cargo test -p chronoxide-core
git diff --check
git add Cargo.toml Cargo.lock chronoxide-core/Cargo.toml \
  chronoxide-core/src/storage/chunk/types.rs \
  chronoxide-core/src/storage/chunk/reader.rs \
  chronoxide-core/src/storage/chunk/codec.rs \
  chronoxide-core/src/storage/segment/mod.rs \
  chronoxide-core/src/storage/segment/query_types.rs \
  chronoxide-core/src/storage/segment/query_context.rs \
  chronoxide-core/src/storage/segment/query_reader.rs \
  chronoxide-core/src/storage/segment/query_store.rs \
  chronoxide-core/src/storage/segment/range_scalar_cache.rs \
  chronoxide-core/src/storage/segment/range_scalar_cache_tests.rs \
  chronoxide-core/tests/support/promql_range_scalar_cache.rs \
  chronoxide-core/tests/promql_range_scalar_cache_oracle.rs \
  chronoxide-core/tests/promql_range_scalar_cache_errors.rs \
  chronoxide-core/tests/promql_range_scalar_cache_lifecycle.rs \
  chronoxide-ingester/src/bin/chronoxide-query.rs \
  chronoxide-ingester/src/bin/chronoxide_query/tests.rs
git diff --cached --check
git diff --cached --name-only
git commit -m "perf(query): reuse decoded scalar lanes across range steps"
~~~

Verify the staged list excludes all pre-existing dirty files and runtime reports.

- [ ] **Step 5B: If it fails, remove only uncommitted Tasks 5–9**

Use the Task 4 commit as the exact boundary. Reverse only files listed in Tasks
5–9, preserve user changes and Tasks 1–4 commits, and retain runtime evidence in
`data/`. Then run:

~~~sh
git diff -- Cargo.toml Cargo.lock chronoxide-core/Cargo.toml \
  chronoxide-core/src/storage/chunk/types.rs \
  chronoxide-core/src/storage/chunk/reader.rs \
  chronoxide-core/src/storage/chunk/codec.rs \
  chronoxide-core/src/storage/segment/mod.rs \
  chronoxide-core/src/storage/segment/query_types.rs \
  chronoxide-core/src/storage/segment/query_context.rs \
  chronoxide-core/src/storage/segment/query_reader.rs \
  chronoxide-core/src/storage/segment/query_store.rs \
  chronoxide-core/tests/support/promql_range_scalar_cache.rs \
  chronoxide-ingester/src/bin/chronoxide-query.rs \
  chronoxide-ingester/src/bin/chronoxide_query/tests.rs | git apply --reverse
git diff --check
cargo test -p chronoxide-core --test promql_range_prechange_oracle -- --nocapture
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
~~~

Delete the five untracked Task 5–9 cache files with `apply_patch` `Delete File`
operations: `range_scalar_cache.rs`, `range_scalar_cache_tests.rs`, and the three
`promql_range_scalar_cache_{oracle,errors,lifecycle}.rs` integration tests.

Expected: the branch remains at the instrumentation/oracle state with no cache
commit.
