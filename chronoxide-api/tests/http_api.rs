use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::CONTENT_TYPE},
};
use chronoxide_api::{ApiConfig, StoreOpenConfig, live_router, open_store, router};
use chronoxide_core::{
    labels::{KeyValueRef, LabelSetStore, SeriesRef, VersionedFlatInternedLabelSetStore},
    promql::METRIC_NAME_LABEL,
    storage::{
        head::{
            FloatEncoding, FrozenHeadReadView, HeadBuffer, HeadConfig, HeadReadView, IntEncoding,
            LiveSeriesCatalogBuilder, SampleValue,
        },
        io::{ChunkReadConfig, ChunkReadMode},
        live_memory::{LiveMemoryCharge, LiveMemoryClass, LiveMemoryGovernor},
        live_view::{LiveCommitCandidate, LiveQueryHandle, LiveQueryView, LiveStorageView},
        manifest::{
            ManifestCut, ManifestRecord, ManifestSegment, ManifestSnapshot, ManifestWriter,
            write_current,
        },
        segment::{
            QueryLabelMaterializationPolicy, QueryLimits, SegmentFile, SegmentReader,
            SegmentStorageSchema, SegmentStoreReader, SegmentStoreSchemaPolicy, SegmentWriter,
            SegmentWriterConfig,
        },
    },
};
use serde_json::Value;
use tower::ServiceExt;

fn write_test_corpus() -> tempfile::TempDir {
    write_test_corpus_with_schema(SegmentStoreSchemaPolicy::StrictSchema8)
}

fn write_test_corpus_with_schema(schema: SegmentStoreSchemaPolicy) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let config = match schema {
        SegmentStoreSchemaPolicy::StrictSchema7 => {
            config.with_storage_schema(SegmentStorageSchema::Schema7)
        }
        SegmentStoreSchemaPolicy::StrictSchema8 => {
            config.with_storage_schema(SegmentStorageSchema::Schema8)
        }
        SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb => {
            panic!("API test corpus helper supports only strict schema 7 or schema 8")
        }
    };
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(7),
            &[
                (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
                ("host".to_string(), "a".to_string()),
            ],
            &[(5_000, 1.5), (15_000, 2.5)],
        )
        .unwrap();
    writer.flush().unwrap();
    tempdir
}

fn test_config() -> ApiConfig {
    ApiConfig {
        query_limits: QueryLimits::production_default(),
        chunk_read_config: ChunkReadConfig {
            mode: ChunkReadMode::Pread,
            queue_depth: 1,
            payload_coalesce_max_gap_bytes:
                chronoxide_core::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        },
        experimental_cross_segment_chunk_reads: false,
        range_scalar_cache_max_bytes: 0,
        max_concurrent_queries: 2,
    }
}

const LIVE_RESPONSE_HEADERS: [&str; 6] = [
    "x-chronoxide-view-generation",
    "x-chronoxide-view-age-ms",
    "x-chronoxide-visible-message-sequence",
    "x-chronoxide-catalog-revision",
    "x-chronoxide-view-pin-wait-ns",
    "x-chronoxide-view-pin-held-ns",
];

fn numeric_header(response: &axum::response::Response, name: &str) -> u128 {
    response
        .headers()
        .get(name)
        .unwrap_or_else(|| panic!("missing {name} response header"))
        .to_str()
        .unwrap_or_else(|error| panic!("{name} response header is not ASCII: {error}"))
        .parse::<u128>()
        .unwrap_or_else(|error| panic!("{name} response header is not numeric: {error}"))
}

fn live_test_handle(
    memory_limit_bytes: u64,
) -> (
    tempfile::TempDir,
    Arc<LiveQueryHandle<LiveStorageView>>,
    Arc<LiveMemoryGovernor>,
) {
    live_test_handle_with_staleness(memory_limit_bytes, Duration::from_secs(10))
}

fn empty_live_payload(root: &Path) -> LiveStorageView {
    let sealed = Arc::new(
        SegmentStoreReader::open_manifest_snapshot(root, &ManifestSnapshot::absent()).unwrap(),
    );
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let head = Arc::new(
        HeadReadView::new(
            Arc::new(FrozenHeadReadView::default()),
            Arc::new(labels.snapshot().unwrap()),
        )
        .unwrap(),
    );
    LiveStorageView::new(sealed, head).unwrap()
}

fn commit_empty_live_view(
    handle: &LiveQueryHandle<LiveStorageView>,
    root: &Path,
    visible_message_sequence: u64,
) -> u64 {
    let payload = empty_live_payload(root);
    let base = handle.begin_commit().unwrap();
    let generation = base.next_generation;
    let view = Arc::new(
        LiveQueryView::new_storage(
            generation,
            Instant::now(),
            ManifestCut::Absent,
            visible_message_sequence,
            0,
            payload,
        )
        .unwrap(),
    );
    handle.commit(LiveCommitCandidate::new(base, view)).unwrap();
    generation
}

fn live_test_handle_with_staleness(
    memory_limit_bytes: u64,
    max_view_staleness: Duration,
) -> (
    tempfile::TempDir,
    Arc<LiveQueryHandle<LiveStorageView>>,
    Arc<LiveMemoryGovernor>,
) {
    let root = tempfile::tempdir().unwrap();
    let handle = LiveQueryHandle::new(max_view_staleness).unwrap();
    let governor = LiveMemoryGovernor::new(memory_limit_bytes).unwrap();
    handle
        .configure_query_admission(Arc::clone(&governor), 1)
        .unwrap();
    assert_eq!(commit_empty_live_view(&handle, root.path(), 0), 1);
    (root, handle, governor)
}

fn live_test_handle_with_nonempty_head() -> (
    tempfile::TempDir,
    Arc<LiveQueryHandle<LiveStorageView>>,
    Arc<LiveMemoryGovernor>,
) {
    let root = tempfile::tempdir().unwrap();
    let sealed = Arc::new(
        SegmentStoreReader::open_manifest_snapshot(root.path(), &ManifestSnapshot::absent())
            .unwrap(),
    );
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let series = labels
        .intern(&[
            KeyValueRef::from((METRIC_NAME_LABEL, "live_http_metric")),
            KeyValueRef::from(("host", "head-only")),
        ])
        .unwrap();
    let mut mutable_head = HeadBuffer::new(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ))
    .unwrap();
    mutable_head
        .record_sample(series, 5_000, SampleValue::Float(42.5))
        .unwrap();
    let samples = Arc::new(FrozenHeadReadView::from_owned(
        mutable_head.try_freeze_for_publication().unwrap(),
    ));
    let labels = Arc::new(labels.snapshot().unwrap());
    let mut catalog = LiveSeriesCatalogBuilder::new(Arc::clone(&labels), 1).unwrap();
    catalog
        .reconcile_sample_store(samples.sample_store())
        .unwrap();
    let head =
        Arc::new(HeadReadView::new_live(samples, Arc::new(catalog.finish().unwrap()), 1).unwrap());
    let catalog_revision = head.catalog_revision();
    let payload = LiveStorageView::new(sealed, head).unwrap();
    let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
    let governor = LiveMemoryGovernor::new(1024 * 1024).unwrap();
    handle
        .configure_query_admission(Arc::clone(&governor), 1)
        .unwrap();
    let base = handle.begin_commit().unwrap();
    let view = Arc::new(
        LiveQueryView::new_storage(
            base.next_generation,
            Instant::now(),
            ManifestCut::Absent,
            1,
            catalog_revision,
            payload,
        )
        .unwrap(),
    );
    handle.commit(LiveCommitCandidate::new(base, view)).unwrap();
    (root, handle, governor)
}

async fn body_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

async fn assert_text_response(
    response: axum::response::Response,
    expected_status: StatusCode,
    expected_body: &str,
) {
    assert_eq!(response.status(), expected_status);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "text/plain; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), expected_body.as_bytes());
}

async fn response_text(response: axum::response::Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

#[tokio::test]
async fn sealed_health_and_readiness_preserve_exact_legacy_responses() {
    let tempdir = write_test_corpus();
    let app = router(
        SegmentStoreReader::open(tempdir.path()).unwrap(),
        test_config(),
    )
    .unwrap();
    for uri in ["/-/healthy", "/-/ready"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_text_response(response, StatusCode::OK, "Chronoxide is Ready.\n").await;
    }
}

#[test]
fn live_router_rejects_a_handle_without_query_retention_admission() {
    let handle = LiveQueryHandle::new(Duration::from_secs(10)).unwrap();
    let error = match live_router(handle, test_config()) {
        Ok(_) => panic!("unconfigured live admission must fail at startup"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("query-retention admission"));
}

#[tokio::test]
async fn live_resource_pressure_returns_503_and_releases_without_invalidating_existing_pin() {
    let (_root, handle, governor) = live_test_handle(2);
    let app = live_router(Arc::clone(&handle), test_config()).unwrap();

    let retained = handle.try_pin_admitted(Instant::now()).unwrap();
    let pressure: LiveMemoryCharge = governor
        .try_charge(LiveMemoryClass::Other, 1)
        .expect("fill the remaining live-memory budget");

    for uri in ["/-/ready", "/api/v1/query?query=1%2B2&time=20"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("resource-pressure admission"),
            "unexpected pressure response: {}",
            String::from_utf8_lossy(&body)
        );
    }
    assert_eq!(retained.generation(), 1);
    assert_eq!(retained.catalog_revision(), 0);

    drop(pressure);
    let ready = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/-/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::OK);
    let query = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=1%2B2&time=20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(query.status(), StatusCode::OK);
    assert_eq!(query.headers()["x-chronoxide-view-generation"], "1");
    let body = body_json(query).await;
    assert_eq!(body["data"]["resultType"], "scalar");
    assert_eq!(body["data"]["result"][1], "3");

    drop(retained);
    assert_eq!(governor.stats().charged_bytes, 0);
}

#[tokio::test]
async fn live_http_query_reads_a_genuinely_nonempty_published_head() {
    let (_root, handle, _governor) = live_test_handle_with_nonempty_head();
    let app = live_router(handle, test_config()).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=live_http_metric&time=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(numeric_header(&response, "x-chronoxide-view-generation"), 1);
    let _view_age_ms = numeric_header(&response, "x-chronoxide-view-age-ms");
    assert_eq!(
        numeric_header(&response, "x-chronoxide-visible-message-sequence"),
        1
    );
    assert_eq!(
        numeric_header(&response, "x-chronoxide-catalog-revision"),
        1
    );
    let _pin_wait_ns = numeric_header(&response, "x-chronoxide-view-pin-wait-ns");
    let _pin_held_ns = numeric_header(&response, "x-chronoxide-view-pin-held-ns");
    let body = body_json(response).await;
    assert_eq!(body["data"]["resultType"], "vector");
    assert_eq!(body["data"]["result"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["data"]["result"][0]["metric"][METRIC_NAME_LABEL],
        "live_http_metric"
    );
    assert_eq!(body["data"]["result"][0]["metric"]["host"], "head-only");
    assert_eq!(body["data"]["result"][0]["value"][0], 5.0);
    assert_eq!(body["data"]["result"][0]["value"][1], "42.5");
}

#[tokio::test]
async fn live_http_readiness_covers_dirty_expiry_failure_recovery_and_independent_health() {
    let (root, handle, _governor) = live_test_handle_with_staleness(1024, Duration::from_secs(1));
    let app = live_router(Arc::clone(&handle), test_config()).unwrap();

    handle.mark_dirty(Instant::now()).unwrap();
    let ready_while_dirty = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/-/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_text_response(ready_while_dirty, StatusCode::OK, "Chronoxide is Ready.\n").await;

    assert_eq!(commit_empty_live_view(&handle, root.path(), 1), 2);
    let expired_at = Instant::now().checked_sub(Duration::from_secs(2)).unwrap();
    handle.mark_dirty(expired_at).unwrap();
    let expired = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/-/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(expired.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(response_text(expired).await.contains("live view is stale"));

    let expired_query = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=1%2B2&time=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(expired_query.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        response_text(expired_query)
            .await
            .contains("live view is stale")
    );

    let healthy_while_stale = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/-/healthy")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_text_response(
        healthy_while_stale,
        StatusCode::OK,
        "Chronoxide is Ready.\n",
    )
    .await;

    handle
        .mark_failed("injected manifest refresh failure")
        .unwrap();
    let failed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/-/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(failed.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        response_text(failed)
            .await
            .contains("injected manifest refresh failure")
    );

    let healthy_while_failed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/-/healthy")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_text_response(
        healthy_while_failed,
        StatusCode::OK,
        "Chronoxide is Ready.\n",
    )
    .await;

    assert_eq!(commit_empty_live_view(&handle, root.path(), 2), 3);
    let recovered = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/-/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_text_response(recovered, StatusCode::OK, "Chronoxide is Ready.\n").await;

    let recovered_query = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=1%2B2&time=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recovered_query.status(), StatusCode::OK);
    assert_eq!(
        recovered_query.headers()["x-chronoxide-view-generation"],
        "3"
    );
}

#[tokio::test]
async fn instant_http_result_matches_a_fresh_direct_core_session() {
    let tempdir = write_test_corpus();
    let direct_store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let config = test_config();
    let mut direct_session = direct_store.query_session().unwrap();
    direct_session
        .set_chunk_read_config(config.chunk_read_config.clone())
        .unwrap();
    direct_session
        .set_experimental_cross_segment_chunk_reads(config.experimental_cross_segment_chunk_reads);
    direct_session
        .set_range_scalar_cache_budget_bytes(config.range_scalar_cache_max_bytes)
        .unwrap();
    let direct = direct_session
        .query_promql_at_with_limits("cpu_usage", 20_000, QueryLimits::production_default())
        .unwrap();
    let direct_profile = direct_session.profile();
    let app = router(SegmentStoreReader::open(tempdir.path()).unwrap(), config).unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=cpu_usage&time=20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    for header in LIVE_RESPONSE_HEADERS {
        assert!(
            !response.headers().contains_key(header),
            "sealed-only response unexpectedly included {header}"
        );
    }
    assert!(response.headers().contains_key("server-timing"));
    assert!(
        response
            .headers()
            .contains_key("x-chronoxide-query-duration-ns")
    );
    let _: u128 = response
        .headers()
        .get("x-chronoxide-serialize-duration-ns")
        .expect("serialization duration header")
        .to_str()
        .expect("serialization duration is ASCII")
        .parse::<u128>()
        .expect("serialization duration is numeric");
    let stats: Value = serde_json::from_str(
        response
            .headers()
            .get("x-chronoxide-query-stats")
            .expect("query stats header")
            .to_str()
            .expect("query stats are ASCII"),
    )
    .expect("query stats are JSON");
    assert_eq!(
        stats,
        serde_json::json!({
            "segments_considered": direct.stats.segments_considered,
            "segments_skipped_by_time": direct.stats.segments_skipped_by_time,
            "segments_skipped_by_missing_equality":
                direct.stats.segments_skipped_by_missing_equality,
            "segments_skipped_by_matcher_time_range":
                direct.stats.segments_skipped_by_matcher_time_range,
            "segments_queried": direct.stats.segments_queried,
            "matched_series": direct.stats.matched_series,
            "projected_series": direct.stats.projected_series,
            "chunk_reads": direct.stats.chunk_reads,
            "bytes_read": direct.stats.bytes_read,
            "samples_decoded": direct.stats.samples_decoded,
            "typed_scalar_chunks_decoded": direct.stats.typed_scalar_chunks_decoded,
            "typed_full_chunks_decoded": direct.stats.typed_full_chunks_decoded,
            "regex_values_examined": direct.stats.regex_values_examined,
            "index_postings_reads": direct.stats.index_postings_reads,
            "index_postings_bytes_read": direct.stats.index_postings_bytes_read,
        })
    );
    let query_io: Value = serde_json::from_str(
        response
            .headers()
            .get("x-chronoxide-query-io")
            .expect("query I/O diagnostics header")
            .to_str()
            .expect("query I/O diagnostics are ASCII"),
    )
    .expect("query I/O diagnostics are JSON");
    assert_eq!(
        query_io,
        serde_json::json!({
            "chunk_payload_used_bytes": direct_profile.chunk_payload_bytes,
            "chunk_payload_read_bytes": direct_profile.chunk_payload_physical_bytes,
            "chunk_payload_physical_reads": direct_profile.chunk_payload_physical_reads,
            "series_entry_bytes": direct_profile.series_entry_bytes,
            "chunk_index_range_bytes": direct_profile.chunk_index_range_bytes,
            "exact_postings_bytes": direct_profile.exact_postings_bytes,
        })
    );
    let body = body_json(response).await;
    let result = &body["data"]["result"][0];
    let expected = &direct.results[0];
    let expected_labels: std::collections::BTreeMap<_, _> =
        expected.labels.to_vec().into_iter().collect();
    assert_eq!(result["metric"]["__name__"], expected_labels["__name__"]);
    assert_eq!(result["metric"]["host"], "a");
    assert_eq!(result["value"][0], expected.samples[0].0 as f64 / 1_000.0);
    assert_eq!(result["value"][1], expected.samples[0].1.to_string());
}

#[tokio::test]
async fn instant_http_demand_driven_aggregation_matches_forced_full_core_session() {
    let tempdir = write_test_corpus();
    let direct_store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut direct_session = direct_store.query_session().unwrap();
    direct_session.set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);
    let direct = direct_session
        .query_promql_at_with_limits(
            "sum by (host) (cpu_usage)",
            20_000,
            QueryLimits::production_default(),
        )
        .unwrap();
    let app = router(
        SegmentStoreReader::open(tempdir.path()).unwrap(),
        test_config(),
    )
    .unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=sum%20by%20%28host%29%20%28cpu_usage%29&time=20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    let result = &body["data"]["result"][0];
    let expected = &direct.results[0];
    let expected_labels: std::collections::BTreeMap<_, _> =
        expected.labels.to_vec().into_iter().collect();
    assert_eq!(result["metric"].as_object().unwrap().len(), 1);
    assert_eq!(result["metric"]["host"], expected_labels["host"]);
    assert_eq!(result["value"][0], expected.samples[0].0 as f64 / 1_000.0);
    assert_eq!(result["value"][1], expected.samples[0].1.to_string());
}

#[tokio::test]
async fn explicit_schema8_store_serves_label_postings_query_over_http() {
    let tempdir = write_test_corpus_with_schema(SegmentStoreSchemaPolicy::StrictSchema8);
    let store_config = StoreOpenConfig {
        storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
        ..StoreOpenConfig::default()
    };
    let direct = open_store(tempdir.path(), store_config.clone())
        .unwrap()
        .query_session()
        .unwrap()
        .query_promql_at_with_limits(
            "cpu_usage{host=\"a\"}",
            20_000,
            QueryLimits::production_default(),
        )
        .unwrap();
    let app = router(
        open_store(tempdir.path(), store_config).unwrap(),
        test_config(),
    )
    .unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=cpu_usage%7Bhost%3D%22a%22%7D&time=20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_json(response).await;
    let result = &body["data"]["result"][0];
    let expected = &direct.results[0];
    let expected_labels: std::collections::BTreeMap<_, _> =
        expected.labels.to_vec().into_iter().collect();
    assert_eq!(result["metric"]["__name__"], expected_labels["__name__"]);
    assert_eq!(result["metric"]["host"], expected_labels["host"]);
    assert_eq!(result["value"][0], expected.samples[0].0 as f64 / 1_000.0);
    assert_eq!(result["value"][1], expected.samples[0].1.to_string());
}

#[test]
fn schema8_is_the_default_and_schema7_requires_explicit_policy() {
    let tempdir = write_test_corpus_with_schema(SegmentStoreSchemaPolicy::StrictSchema7);
    assert_eq!(
        StoreOpenConfig::default().storage_schema_policy,
        SegmentStoreSchemaPolicy::StrictSchema8
    );

    let error = open_store(tempdir.path(), StoreOpenConfig::default())
        .err()
        .expect("the default schema-8 policy must reject a schema-7 corpus");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("StrictSchema8"));

    open_store(
        tempdir.path(),
        StoreOpenConfig {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..StoreOpenConfig::default()
        },
    )
    .expect("the explicit schema-7 policy must open the schema-7 corpus");
}

#[tokio::test]
async fn get_and_form_post_range_requests_have_identical_matrix_results() {
    let tempdir = write_test_corpus();
    let app = router(
        SegmentStoreReader::open(tempdir.path()).unwrap(),
        test_config(),
    )
    .unwrap();
    let get_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=cpu_usage&start=5&end=15&step=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let post_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query_range")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=cpu_usage&start=5&end=15&step=10"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(post_response.status(), StatusCode::OK);
    assert_eq!(
        body_json(get_response).await,
        body_json(post_response).await
    );
}

#[tokio::test]
async fn instant_scalar_and_bad_parameters_use_prometheus_envelopes() {
    let tempdir = write_test_corpus();
    let app = router(
        SegmentStoreReader::open(tempdir.path()).unwrap(),
        test_config(),
    )
    .unwrap();
    let scalar = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=1%2B2&time=20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(scalar.status(), StatusCode::OK);
    assert!(scalar.headers().contains_key("x-chronoxide-query-io"));
    let scalar = body_json(scalar).await;
    assert_eq!(scalar["data"]["resultType"], "scalar");
    assert_eq!(scalar["data"]["result"][1], "3");

    let invalid = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=cpu_usage&start=nope&end=15&step=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert!(!invalid.headers().contains_key("x-chronoxide-query-io"));
    let invalid = body_json(invalid).await;
    assert_eq!(invalid["status"], "error");
    assert_eq!(invalid["errorType"], "bad_data");
}

#[test]
fn open_store_uses_manifest_published_inventory() {
    let tempdir = write_test_corpus();
    let mut readers: Vec<_> = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| SegmentReader::open(entry.path()).unwrap())
        .collect();
    readers.sort_by_key(|reader| reader.meta().start_ms);
    assert_eq!(readers.len(), 2);

    let manifest_dir = tempdir.path().join("manifest");
    let mut manifest = ManifestWriter::create(&manifest_dir, 1).unwrap();
    let meta = readers[0].meta();
    manifest
        .append(&ManifestRecord::SegmentSealed(
            ManifestSegment::new(meta.segment_id.clone(), meta.start_ms, meta.end_ms, None)
                .unwrap(),
        ))
        .unwrap();
    manifest.sync_all().unwrap();
    write_current(&manifest_dir, manifest.file_name()).unwrap();

    let store = open_store(tempdir.path(), StoreOpenConfig::default()).unwrap();
    let results = store
        .query_session()
        .unwrap()
        .query_promql("cpu_usage", 0, 20_000)
        .unwrap();
    assert_eq!(results[0].samples, vec![(5_000, 1.5)]);
}

#[test]
fn open_store_honors_footer_validation_without_a_manifest() {
    let tempdir = write_test_corpus();
    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let mut chunks = fs::read(&chunks_path).unwrap();
    chunks[0] ^= 1;
    fs::write(chunks_path, chunks).unwrap();

    open_store(tempdir.path(), StoreOpenConfig::default())
        .expect("ordinary open does not hash complete tracked files");
    let error = open_store(
        tempdir.path(),
        StoreOpenConfig {
            validate_segment_footers: true,
            ..StoreOpenConfig::default()
        },
    )
    .err()
    .expect("explicit validation must hash a manifestless corpus");
    assert!(error.to_string().contains("complete validation"));
}
