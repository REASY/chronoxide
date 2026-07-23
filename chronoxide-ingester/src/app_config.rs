use crate::ingester::KafkaConsumerConfig;
use crate::runtime::get_env_default;
use chronoxide_core::storage::head::{FloatEncoding, IntEncoding, VarLenEncodingKind};
use chronoxide_core::storage::segment::{
    SegmentStorageSchema as CoreSegmentStorageSchema,
    SegmentWriterConfig as CoreSegmentWriterConfig,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum LabelSetStoreKind {
    Naive,
    #[default]
    #[serde(alias = "experimental_flat_interned_symbol_id_hash")]
    FlatInterned,
    /// Experimental comparator using bounded key/value pages.
    ExperimentalFlatInternedPaged,
    /// Experimental control retaining the legacy canonical-string fingerprint.
    ExperimentalFlatInternedCanonicalStringHash,
    /// Experimental control retaining SipHash over interned symbol IDs.
    #[serde(rename = "experimental_flat_interned_siphash")]
    ExperimentalFlatInternedSipHash,
    /// Experimental control retaining SipHash for symbol fingerprints while
    /// keeping the normal AHash label-set fingerprint.
    #[serde(rename = "experimental_flat_interned_siphash_symbols")]
    ExperimentalFlatInternedSipHashSymbols,
    KeySetDictEncoded,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub ingestion: IngestionConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.ingestion.validate()
    }
}

impl KafkaConfig {
    fn default_brokers() -> Vec<String> {
        if let Some(value) = get_env_default("KAFKA_BROKERS") {
            let brokers: Vec<String> = value
                .split(',')
                .map(|entry| entry.trim())
                .filter(|entry| !entry.is_empty())
                .map(|entry| entry.to_string())
                .collect();
            if !brokers.is_empty() {
                return brokers;
            }
        }
        vec!["localhost:9092".to_string()]
    }
    fn default_group_id() -> String {
        "chronoxide-ingester".to_string()
    }
    fn default_topic() -> String {
        "otlp_metrics".to_string()
    }
    fn default_client_id() -> String {
        "chronoxide-ingester".to_string()
    }
    fn default_session_timeout_ms() -> i32 {
        10_000
    }
    fn default_enable_auto_commit() -> bool {
        false
    }
    fn default_auto_offset_reset() -> String {
        "earliest".to_string()
    }
    fn default_max_inflight() -> i32 {
        1
    }
    fn default_fetch_min_bytes() -> i32 {
        1
    }
    fn default_fetch_wait_max_ms() -> i32 {
        100
    }
    fn default_assigned_partitions() -> Option<Vec<i32>> {
        get_env_default("KAFKA_ASSIGNED_PARTITIONS").map(|value| {
            value
                .split(',')
                .map(|entry| entry.parse::<i32>().unwrap())
                .collect::<Vec<_>>()
        })
    }
    fn default_security_protocol() -> Option<String> {
        get_env_default("KAFKA_SECURITY_PROTOCOL")
    }
    fn default_sasl_mechanism() -> Option<String> {
        get_env_default("KAFKA_SASL_MECHANISM")
    }
    fn default_sasl_username() -> Option<String> {
        get_env_default("KAFKA_SASL_USERNAME")
    }
    fn default_sasl_password() -> Option<String> {
        get_env_default("KAFKA_SASL_PASSWORD")
    }
    fn default_ssl_ca_location() -> Option<String> {
        get_env_default("KAFKA_SSL_CA_LOCATION")
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct KafkaConfig {
    #[serde(default = "KafkaConfig::default_brokers")]
    pub brokers: Vec<String>,
    #[serde(default = "KafkaConfig::default_group_id")]
    pub group_id: String,
    #[serde(default = "KafkaConfig::default_topic")]
    pub topic: String,
    #[serde(default = "KafkaConfig::default_client_id")]
    pub client_id: String,

    #[serde(default = "KafkaConfig::default_assigned_partitions")]
    pub assigned_partitions: Option<Vec<i32>>,

    #[serde(default = "KafkaConfig::default_security_protocol")]
    pub security_protocol: Option<String>,
    #[serde(default = "KafkaConfig::default_sasl_mechanism")]
    pub sasl_mechanism: Option<String>,
    #[serde(default = "KafkaConfig::default_sasl_username")]
    pub sasl_username: Option<String>,
    #[serde(default = "KafkaConfig::default_sasl_password", skip_serializing)]
    pub sasl_password: Option<String>,
    #[serde(default = "KafkaConfig::default_ssl_ca_location")]
    pub ssl_ca_location: Option<String>,

    #[serde(default = "KafkaConfig::default_session_timeout_ms")]
    pub session_timeout_ms: i32,
    #[serde(default = "KafkaConfig::default_enable_auto_commit")]
    pub enable_auto_commit: bool,
    #[serde(default = "KafkaConfig::default_auto_offset_reset")]
    pub auto_offset_reset: String,
    #[serde(default = "KafkaConfig::default_max_inflight")]
    pub max_inflight: i32,
    #[serde(default = "KafkaConfig::default_fetch_min_bytes")]
    pub fetch_min_bytes: i32,
    #[serde(default = "KafkaConfig::default_fetch_wait_max_ms")]
    pub fetch_wait_max_ms: i32,
}

impl KafkaConfig {
    pub fn to_kafka_consumer_config(&self, password: Option<String>) -> KafkaConsumerConfig {
        KafkaConsumerConfig {
            brokers: self.brokers.join(","),
            group_id: self.group_id.clone(),
            topic: self.topic.clone(),
            client_id: self.client_id.clone(),
            assigned_partitions: self.assigned_partitions.clone(),
            security_protocol: self.security_protocol.clone(),
            sasl_mechanism: self.sasl_mechanism.clone(),
            sasl_username: self.sasl_username.clone(),
            sasl_password: password.or_else(|| self.sasl_password.clone()),
            ssl_ca_location: self.ssl_ca_location.clone(),
            session_timeout_ms: self.session_timeout_ms,
            enable_auto_commit: self.enable_auto_commit,
            auto_offset_reset: self.auto_offset_reset.clone(),
            max_inflight: self.max_inflight,
            fetch_min_bytes: self.fetch_min_bytes,
            fetch_wait_max_ms: self.fetch_wait_max_ms,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct IngestionConfig {
    pub max_event_age_secs: u64,
    pub max_event_lead_secs: i64,
    pub drop_outdated: bool,

    #[serde(default = "IngestionConfig::default_labelset_store")]
    pub labelset_store: LabelSetStoreKind,

    #[serde(default = "IngestionConfig::default_labelset_report_interval_secs")]
    pub labelset_report_interval_secs: u64,

    #[serde(default)]
    pub stop_after_messages: Option<u64>,

    #[serde(default)]
    pub replay_from: Option<String>,

    #[serde(default)]
    pub capture_to: Option<String>,

    #[serde(default)]
    pub capture_only: bool,

    #[serde(default)]
    pub head_buffer: HeadBufferConfig,

    #[serde(default)]
    pub segment_writer: SegmentWriterConfig,
}

impl IngestionConfig {
    fn default_labelset_store() -> LabelSetStoreKind {
        if let Some(val) = get_env_default("INGESTION_LABELSET_STORE") {
            match val.to_lowercase().as_str() {
                "naive" => return LabelSetStoreKind::Naive,
                "flat_interned" => return LabelSetStoreKind::FlatInterned,
                "experimental_flat_interned_paged" => {
                    return LabelSetStoreKind::ExperimentalFlatInternedPaged;
                }
                // Retain the old experiment spelling as an alias now that
                // interned-ID hashing is the normal flat-store behavior.
                "experimental_flat_interned_symbol_id_hash" => {
                    return LabelSetStoreKind::FlatInterned;
                }
                "experimental_flat_interned_canonical_string_hash" => {
                    return LabelSetStoreKind::ExperimentalFlatInternedCanonicalStringHash;
                }
                "experimental_flat_interned_siphash" => {
                    return LabelSetStoreKind::ExperimentalFlatInternedSipHash;
                }
                "experimental_flat_interned_siphash_symbols" => {
                    return LabelSetStoreKind::ExperimentalFlatInternedSipHashSymbols;
                }
                "key_set_dict_encoded" => return LabelSetStoreKind::KeySetDictEncoded,
                _ => {}
            }
        }
        LabelSetStoreKind::default()
    }

    fn default_labelset_report_interval_secs() -> u64 {
        10
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.max_event_lead_secs < 0 {
            return Err(format!(
                "ingestion.max_event_lead_secs must be >= 0; got {}. It is the allowed future skew after trusted captured_at_ms, not a required lag.",
                self.max_event_lead_secs
            ));
        }

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct HeadBufferConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "HeadBufferConfig::default_window_duration_secs")]
    pub window_duration_secs: u64,
    #[serde(default)]
    pub out_of_order_time_window_secs: u64,
    #[serde(default = "HeadBufferConfig::default_float_encoding")]
    pub float_encoding: FloatEncoding,
    #[serde(default = "HeadBufferConfig::default_int_encoding")]
    pub int_encoding: IntEncoding,
    #[serde(default = "HeadBufferConfig::default_varlen_encoding")]
    pub varlen_encoding: VarLenEncodingKind,
    /// Keep short Gorilla/Delta numeric series inline before allocating codec state.
    #[serde(default = "HeadBufferConfig::default_compact_numeric_series")]
    pub compact_numeric_series: bool,
    /// Promote dense SeriesRef pages to direct indexed head storage.
    #[serde(default = "HeadBufferConfig::default_adaptive_series_table")]
    pub adaptive_series_table: bool,
    /// Promote dense SeriesRef pages in the long-lived last-timestamp table.
    #[serde(default = "HeadBufferConfig::default_adaptive_last_timestamp_table")]
    pub adaptive_last_timestamp_table: bool,
}

impl Default for HeadBufferConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            window_duration_secs: Self::default_window_duration_secs(),
            out_of_order_time_window_secs: 0,
            float_encoding: Self::default_float_encoding(),
            int_encoding: Self::default_int_encoding(),
            varlen_encoding: Self::default_varlen_encoding(),
            compact_numeric_series: Self::default_compact_numeric_series(),
            adaptive_series_table: Self::default_adaptive_series_table(),
            adaptive_last_timestamp_table: Self::default_adaptive_last_timestamp_table(),
        }
    }
}

impl HeadBufferConfig {
    fn default_window_duration_secs() -> u64 {
        60 * 60
    }

    fn default_float_encoding() -> FloatEncoding {
        FloatEncoding::Gorilla
    }

    fn default_int_encoding() -> IntEncoding {
        IntEncoding::DeltaZigZag
    }

    fn default_varlen_encoding() -> VarLenEncodingKind {
        VarLenEncodingKind::Raw
    }

    fn default_compact_numeric_series() -> bool {
        true
    }

    fn default_adaptive_series_table() -> bool {
        true
    }

    fn default_adaptive_last_timestamp_table() -> bool {
        true
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageSchema {
    Schema7,
    #[default]
    Schema8,
}

impl StorageSchema {
    const fn to_core(self) -> CoreSegmentStorageSchema {
        match self {
            Self::Schema7 => CoreSegmentStorageSchema::Schema7,
            Self::Schema8 => CoreSegmentStorageSchema::Schema8,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SegmentWriterConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "SegmentWriterConfig::default_segments_dir")]
    pub segments_dir: String,
    #[serde(default = "SegmentWriterConfig::default_segment_duration_secs")]
    pub segment_duration_secs: u64,
    #[serde(default = "SegmentWriterConfig::default_float_encoding")]
    pub float_encoding: FloatEncoding,
    #[serde(default = "SegmentWriterConfig::default_int_encoding")]
    pub int_encoding: IntEncoding,
    #[serde(default = "SegmentWriterConfig::default_varlen_encoding")]
    pub varlen_encoding: VarLenEncodingKind,
    #[serde(default)]
    pub deterministic_id_seed: Option<u64>,
    #[serde(default)]
    pub storage_schema: StorageSchema,
}

impl Default for SegmentWriterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            segments_dir: Self::default_segments_dir(),
            segment_duration_secs: Self::default_segment_duration_secs(),
            float_encoding: Self::default_float_encoding(),
            int_encoding: Self::default_int_encoding(),
            varlen_encoding: Self::default_varlen_encoding(),
            deterministic_id_seed: None,
            storage_schema: StorageSchema::default(),
        }
    }
}

impl SegmentWriterConfig {
    fn default_segments_dir() -> String {
        "data/segments".to_string()
    }

    fn default_segment_duration_secs() -> u64 {
        15 * 60
    }

    fn default_float_encoding() -> FloatEncoding {
        FloatEncoding::Gorilla
    }

    fn default_int_encoding() -> IntEncoding {
        IntEncoding::DeltaZigZag
    }

    fn default_varlen_encoding() -> VarLenEncodingKind {
        VarLenEncodingKind::Raw
    }

    pub fn to_core_config(&self) -> Option<CoreSegmentWriterConfig> {
        if !self.enabled {
            return None;
        }

        let config = CoreSegmentWriterConfig::new(
            PathBuf::from(&self.segments_dir),
            Duration::from_secs(self.segment_duration_secs),
        )
        .with_storage_schema(self.storage_schema.to_core());
        Some(match self.deterministic_id_seed {
            Some(seed) => config.with_deterministic_segment_ids(seed),
            None => config,
        })
    }
}

#[cfg(test)]
mod tests {
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

        let wrapper: Wrapper =
            toml::from_str("kind = \"experimental_flat_interned_paged\"").unwrap();
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

        let wrapper: Wrapper =
            toml::from_str("kind = \"experimental_flat_interned_siphash\"").unwrap();
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
            let mut writer =
                SegmentWriter::new(config_for(path).to_core_config().unwrap()).unwrap();
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
}
