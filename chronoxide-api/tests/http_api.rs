use std::{fs, time::Duration};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chronoxide_api::{ApiConfig, StoreOpenConfig, open_store, router};
use chronoxide_core::{
    labels::SeriesRef,
    promql::METRIC_NAME_LABEL,
    storage::{
        io::{ChunkReadConfig, ChunkReadMode},
        manifest::{ManifestRecord, ManifestSegment, ManifestWriter, write_current},
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
        },
        experimental_cross_segment_chunk_reads: false,
        range_scalar_cache_max_bytes: 0,
        max_concurrent_queries: 2,
    }
}

async fn body_json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn health_and_readiness_are_available() {
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
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn instant_http_result_matches_a_fresh_direct_core_session() {
    let tempdir = write_test_corpus();
    let direct_store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let direct = direct_store
        .query_session()
        .unwrap()
        .query_promql_at_with_limits("cpu_usage", 20_000, QueryLimits::production_default())
        .unwrap();
    let app = router(
        SegmentStoreReader::open(tempdir.path()).unwrap(),
        test_config(),
    )
    .unwrap();
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
    assert!(response.headers().contains_key("server-timing"));
    assert!(
        response
            .headers()
            .contains_key("x-chronoxide-query-duration-ns")
    );
    assert!(response.headers().contains_key("x-chronoxide-query-stats"));
    let body = body_json(response).await;
    let result = &body["data"]["result"][0];
    let expected = &direct.results[0];
    let expected_labels: std::collections::BTreeMap<_, _> =
        expected.labels.iter().cloned().collect();
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
        expected.labels.iter().cloned().collect();
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
        expected.labels.iter().cloned().collect();
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
