use super::*;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn kafka_config_defaults_and_env_assigned_partitions() {
    let _guard = ENV_LOCK.lock().unwrap();

    unsafe {
        std::env::set_var("KAFKA_BROKERS", "kafka-a:9092, kafka-b:9092");
        std::env::set_var("KAFKA_ASSIGNED_PARTITIONS", "1,2,3");
        std::env::set_var("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        std::env::set_var("KAFKA_SASL_MECHANISM", "PLAIN");
        std::env::set_var("KAFKA_SASL_USERNAME", "alice");
        std::env::set_var("KAFKA_SASL_PASSWORD", "secret");
        std::env::set_var("KAFKA_SSL_CA_LOCATION", "/tmp/ca.pem");
    }
    let cfg: KafkaConfig = toml::from_str("").unwrap();
    unsafe {
        std::env::remove_var("KAFKA_BROKERS");
        std::env::remove_var("KAFKA_ASSIGNED_PARTITIONS");
        std::env::remove_var("KAFKA_SECURITY_PROTOCOL");
        std::env::remove_var("KAFKA_SASL_MECHANISM");
        std::env::remove_var("KAFKA_SASL_USERNAME");
        std::env::remove_var("KAFKA_SASL_PASSWORD");
        std::env::remove_var("KAFKA_SSL_CA_LOCATION");
    }

    assert_eq!(
        cfg.brokers,
        vec!["kafka-a:9092".to_string(), "kafka-b:9092".to_string()]
    );
    assert_eq!(cfg.group_id, "chronoxide-ingester");
    assert_eq!(cfg.topic, "otlp_metrics");
    assert_eq!(cfg.client_id, "chronoxide-ingester");
    assert_eq!(cfg.assigned_partitions, Some(vec![1, 2, 3]));
    assert_eq!(cfg.security_protocol, Some("SASL_SSL".to_string()));
    assert_eq!(cfg.sasl_mechanism, Some("PLAIN".to_string()));
    assert_eq!(cfg.sasl_username, Some("alice".to_string()));
    assert_eq!(cfg.sasl_password, Some("secret".to_string()));
    assert_eq!(cfg.ssl_ca_location, Some("/tmp/ca.pem".to_string()));

    let consumer = cfg.to_kafka_consumer_config(None);
    assert_eq!(consumer.brokers, "kafka-a:9092,kafka-b:9092");
    assert_eq!(consumer.group_id, "chronoxide-ingester");
    assert_eq!(consumer.topic, "otlp_metrics");
    assert_eq!(consumer.client_id, "chronoxide-ingester");
    assert_eq!(consumer.assigned_partitions, Some(vec![1, 2, 3]));
    assert_eq!(consumer.security_protocol, Some("SASL_SSL".to_string()));
    assert_eq!(consumer.sasl_mechanism, Some("PLAIN".to_string()));
    assert_eq!(consumer.sasl_username, Some("alice".to_string()));
    assert_eq!(consumer.sasl_password, Some("secret".to_string()));
    assert_eq!(consumer.ssl_ca_location, Some("/tmp/ca.pem".to_string()));

    let consumer_override = cfg.to_kafka_consumer_config(Some("pw".to_string()));
    assert_eq!(consumer_override.sasl_password, Some("pw".to_string()));
}

#[test]
fn ingestion_config_labelset_store_env() {
    let _guard = ENV_LOCK.lock().unwrap();

    // Test with explicit env var
    unsafe {
        std::env::set_var("INGESTION_LABELSET_STORE", "key_set_dict_encoded");
    }
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();
    unsafe {
        std::env::remove_var("INGESTION_LABELSET_STORE");
    }

    assert!(matches!(
        cfg.labelset_store,
        LabelSetStoreKind::KeySetDictEncoded
    ));

    unsafe {
        std::env::set_var(
            "INGESTION_LABELSET_STORE",
            "experimental_flat_interned_paged",
        );
    }
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();
    unsafe {
        std::env::remove_var("INGESTION_LABELSET_STORE");
    }
    assert!(matches!(
        cfg.labelset_store,
        LabelSetStoreKind::ExperimentalFlatInternedPaged
    ));

    unsafe {
        std::env::set_var(
            "INGESTION_LABELSET_STORE",
            "experimental_flat_interned_symbol_id_hash",
        );
    }
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();
    unsafe {
        std::env::remove_var("INGESTION_LABELSET_STORE");
    }
    assert!(matches!(
        cfg.labelset_store,
        LabelSetStoreKind::FlatInterned
    ));

    unsafe {
        std::env::set_var(
            "INGESTION_LABELSET_STORE",
            "experimental_flat_interned_canonical_string_hash",
        );
    }
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();
    unsafe {
        std::env::remove_var("INGESTION_LABELSET_STORE");
    }
    assert!(matches!(
        cfg.labelset_store,
        LabelSetStoreKind::ExperimentalFlatInternedCanonicalStringHash
    ));

    unsafe {
        std::env::set_var(
            "INGESTION_LABELSET_STORE",
            "experimental_flat_interned_siphash",
        );
    }
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();
    unsafe {
        std::env::remove_var("INGESTION_LABELSET_STORE");
    }
    assert!(matches!(
        cfg.labelset_store,
        LabelSetStoreKind::ExperimentalFlatInternedSipHash
    ));

    unsafe {
        std::env::set_var(
            "INGESTION_LABELSET_STORE",
            "experimental_flat_interned_siphash_symbols",
        );
    }
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();
    unsafe {
        std::env::remove_var("INGESTION_LABELSET_STORE");
    }
    assert!(matches!(
        cfg.labelset_store,
        LabelSetStoreKind::ExperimentalFlatInternedSipHashSymbols
    ));

    // Test with case insensitivity
    unsafe {
        std::env::set_var("INGESTION_LABELSET_STORE", "NAIVE");
    }
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();
    unsafe {
        std::env::remove_var("INGESTION_LABELSET_STORE");
    }
    assert!(matches!(cfg.labelset_store, LabelSetStoreKind::Naive));

    // Test default when env var is not set
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();
    assert!(matches!(
        cfg.labelset_store,
        LabelSetStoreKind::FlatInterned
    ));
}

#[test]
fn labelset_store_kind_parses_snake_case() {
    #[derive(Deserialize)]
    struct Wrapper {
        kind: LabelSetStoreKind,
    }

    let wrapper: Wrapper = toml::from_str("kind = \"key_set_dict_encoded\"").unwrap();
    assert!(matches!(wrapper.kind, LabelSetStoreKind::KeySetDictEncoded));

    let wrapper: Wrapper = toml::from_str("kind = \"experimental_flat_interned_paged\"").unwrap();
    assert!(matches!(
        wrapper.kind,
        LabelSetStoreKind::ExperimentalFlatInternedPaged
    ));

    let wrapper: Wrapper =
        toml::from_str("kind = \"experimental_flat_interned_canonical_string_hash\"").unwrap();
    assert!(matches!(
        wrapper.kind,
        LabelSetStoreKind::ExperimentalFlatInternedCanonicalStringHash
    ));

    let wrapper: Wrapper =
        toml::from_str("kind = \"experimental_flat_interned_symbol_id_hash\"").unwrap();
    assert!(matches!(wrapper.kind, LabelSetStoreKind::FlatInterned));

    let wrapper: Wrapper = toml::from_str("kind = \"experimental_flat_interned_siphash\"").unwrap();
    assert!(matches!(
        wrapper.kind,
        LabelSetStoreKind::ExperimentalFlatInternedSipHash
    ));

    let wrapper: Wrapper =
        toml::from_str("kind = \"experimental_flat_interned_siphash_symbols\"").unwrap();
    assert!(matches!(
        wrapper.kind,
        LabelSetStoreKind::ExperimentalFlatInternedSipHashSymbols
    ));
}

#[test]
fn segment_writer_config_defaults() {
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();

    assert!(!cfg.segment_writer.enabled);
    assert_eq!(cfg.segment_writer.segments_dir, "data/segments");
    assert_eq!(cfg.segment_writer.segment_duration_secs, 900);
    assert_eq!(cfg.segment_writer.float_encoding, FloatEncoding::Gorilla);
    assert_eq!(cfg.segment_writer.int_encoding, IntEncoding::DeltaZigZag);
    assert_eq!(cfg.segment_writer.deterministic_id_seed, None);
    assert_eq!(cfg.segment_writer.storage_schema, StorageSchema::Schema8);
}

#[test]
fn segment_writer_config_parses_deterministic_id_seed() {
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false

            [segment_writer]
            enabled = true
            segment_duration_secs = 10
            deterministic_id_seed = 42
            storage_schema = "schema7"
        "#,
    )
    .unwrap();

    assert_eq!(cfg.segment_writer.deterministic_id_seed, Some(42));
    assert_eq!(cfg.segment_writer.storage_schema, StorageSchema::Schema7);
}

#[test]
fn segment_writer_config_parses_schema8_and_rejects_invalid_or_obsolete_selection() {
    let schema8: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false

            [segment_writer]
            enabled = true
            storage_schema = "schema8"
        "#,
    )
    .unwrap();
    assert_eq!(
        schema8.segment_writer.storage_schema,
        StorageSchema::Schema8
    );
    schema8.validate().unwrap();

    let invalid: Result<IngestionConfig, _> = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false

            [segment_writer]
            storage_schema = "schema6"
        "#,
    );
    assert!(invalid.is_err());

    for obsolete in [
        "experimental_schema7 = true",
        "experimental_schema8_adaptive_postings = true",
    ] {
        let input = format!(
            r#"
                max_event_age_secs = 60
                max_event_lead_secs = 60
                drop_outdated = false

                [segment_writer]
                {obsolete}
            "#
        );
        let error = toml::from_str::<IngestionConfig>(&input).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }
}

#[test]
fn ingestion_config_parses_replay_from_without_capture_to() {
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = true
            replay_from = "data/smoke/kafka-capture-001"
        "#,
    )
    .unwrap();

    assert_eq!(
        cfg.replay_from.as_deref(),
        Some("data/smoke/kafka-capture-001")
    );
    assert_eq!(cfg.capture_to, None);
    assert!(cfg.drop_outdated);
}

#[test]
fn app_config_validation_rejects_negative_event_lead() {
    let cfg: AppConfig = toml::from_str(
        r#"
            [kafka]

            [ingestion]
            max_event_age_secs = 60
            max_event_lead_secs = -5
            drop_outdated = true
        "#,
    )
    .unwrap();

    let err = cfg.validate().unwrap_err();

    assert!(err.contains("ingestion.max_event_lead_secs must be >= 0"));
    assert!(err.contains("allowed future skew"));
}

#[test]
fn deterministic_segment_writer_config_replays_same_directory_names() {
    use chronoxide_core::labels::SeriesRef;
    use chronoxide_core::storage::segment::SegmentWriter;
    use std::fs;
    use std::path::Path;

    fn config_for(path: &Path) -> SegmentWriterConfig {
        let toml = format!(
            r#"
                max_event_age_secs = 60
                max_event_lead_secs = 60
                drop_outdated = false

                [segment_writer]
                enabled = true
                segments_dir = "{}"
                segment_duration_secs = 10
                deterministic_id_seed = 42
            "#,
            path.display()
        );
        toml::from_str::<IngestionConfig>(&toml)
            .unwrap()
            .segment_writer
    }

    fn write_segment_names(path: &Path) -> Vec<String> {
        let mut writer = SegmentWriter::new(config_for(path).to_core_config().unwrap()).unwrap();
        writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
        writer
            .record_sample(SeriesRef::new(1), 11_000, 2.5)
            .unwrap();
        writer.flush().unwrap();

        let mut names: Vec<_> = fs::read_dir(path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with("seg-"))
            .collect();
        names.sort();
        names
    }

    let first = tempfile::tempdir().unwrap();
    let replay = tempfile::tempdir().unwrap();

    let first_names = write_segment_names(first.path());
    let replay_names = write_segment_names(replay.path());

    assert_eq!(first_names.len(), 2);
    assert_eq!(first_names, replay_names);
}

#[test]
fn head_buffer_config_defaults() {
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false
        "#,
    )
    .unwrap();

    assert!(!cfg.head_buffer.enabled);
    assert_eq!(cfg.head_buffer.window_duration_secs, 3600);
    assert_eq!(cfg.head_buffer.out_of_order_time_window_secs, 0);
    assert_eq!(cfg.head_buffer.float_encoding, FloatEncoding::Gorilla);
    assert!(cfg.head_buffer.compact_numeric_series);
    assert!(cfg.head_buffer.adaptive_series_table);
    assert!(cfg.head_buffer.adaptive_last_timestamp_table);
    let cfg: IngestionConfig = toml::from_str(
        r#"
            max_event_age_secs = 60
            max_event_lead_secs = 60
            drop_outdated = false

            [head_buffer]
            enabled = true
            out_of_order_time_window_secs = 1800
            compact_numeric_series = false
            adaptive_series_table = false
            adaptive_last_timestamp_table = false
        "#,
    )
    .unwrap();

    assert_eq!(cfg.head_buffer.out_of_order_time_window_secs, 1800);
    assert_eq!(cfg.head_buffer.int_encoding, IntEncoding::DeltaZigZag);
    assert_eq!(cfg.head_buffer.varlen_encoding, VarLenEncodingKind::Raw);
    assert!(!cfg.head_buffer.compact_numeric_series);
    assert!(!cfg.head_buffer.adaptive_series_table);
    assert!(!cfg.head_buffer.adaptive_last_timestamp_table);
}
