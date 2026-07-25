use crate::ingester::KafkaConsumerConfig;
use crate::runtime::get_env_default;
use chronoxide_core::storage::{
    head::{FloatEncoding, IntEncoding, VarLenEncodingKind},
    io::{ChunkReadMode, MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES},
    segment::{
        SegmentStorageSchema as CoreSegmentStorageSchema,
        SegmentWriterConfig as CoreSegmentWriterConfig, validate_range_scalar_cache_budget_bytes,
    },
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
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
    #[serde(default)]
    pub api: EmbeddedApiConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.ingestion.validate()?;
        self.api.validate(&self.ingestion)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddedApiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "EmbeddedApiConfig::default_listen")]
    pub listen: String,
    #[serde(default = "EmbeddedApiConfig::default_publish_interval_ms")]
    pub head_publish_interval_ms: u64,
    /// When omitted, this is `max(10 * head_publish_interval_ms, 10s)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_view_staleness_ms: Option<u64>,
    /// Required explicitly when live serving is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_memory_admission_bytes: Option<u64>,
    /// Uses `chronoxide-api::ApiConfig::default()` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_queries: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_max_series_matched: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_max_projected_series: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_max_chunks_read: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_max_bytes_read: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_max_samples: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex_max_expanded_values: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_read_mode: Option<EmbeddedChunkReadMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_read_queue_depth: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_payload_coalesce_max_gap_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experimental_cross_segment_chunk_reads: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_scalar_cache_max_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddedChunkReadMode {
    #[default]
    Auto,
    IoUring,
    Pread,
}

impl From<EmbeddedChunkReadMode> for ChunkReadMode {
    fn from(value: EmbeddedChunkReadMode) -> Self {
        match value {
            EmbeddedChunkReadMode::Auto => Self::Auto,
            EmbeddedChunkReadMode::IoUring => Self::IoUring,
            EmbeddedChunkReadMode::Pread => Self::Pread,
        }
    }
}

impl Default for EmbeddedApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: Self::default_listen(),
            head_publish_interval_ms: Self::default_publish_interval_ms(),
            max_view_staleness_ms: None,
            live_memory_admission_bytes: None,
            max_concurrent_queries: None,
            query_max_series_matched: None,
            query_max_projected_series: None,
            query_max_chunks_read: None,
            query_max_bytes_read: None,
            query_max_samples: None,
            regex_max_expanded_values: None,
            chunk_read_mode: None,
            chunk_read_queue_depth: None,
            chunk_payload_coalesce_max_gap_bytes: None,
            experimental_cross_segment_chunk_reads: None,
            range_scalar_cache_max_bytes: None,
        }
    }
}

impl EmbeddedApiConfig {
    fn default_listen() -> String {
        "127.0.0.1:9091".to_string()
    }

    const fn default_publish_interval_ms() -> u64 {
        1_000
    }

    pub fn resolved_max_view_staleness_ms(&self) -> Result<u64, String> {
        match self.max_view_staleness_ms {
            Some(value) => Ok(value),
            None => self
                .head_publish_interval_ms
                .checked_mul(10)
                .map(|value| value.max(10_000))
                .ok_or_else(|| "api default max_view_staleness_ms overflows u64".to_string()),
        }
    }

    /// Resolves the shared HTTP query configuration without changing the
    /// standalone API's defaults.
    pub fn to_api_config(&self) -> chronoxide_api::ApiConfig {
        let mut config = chronoxide_api::ApiConfig::default();
        if let Some(max_concurrent_queries) = self.max_concurrent_queries {
            config.max_concurrent_queries = max_concurrent_queries;
        }
        if let Some(value) = self.query_max_series_matched {
            config.query_limits.max_matched_series = Some(value);
        }
        if let Some(value) = self.query_max_projected_series {
            config.query_limits.max_projected_series = Some(value);
        }
        if let Some(value) = self.query_max_chunks_read {
            config.query_limits.max_chunk_reads = Some(value);
        }
        if let Some(value) = self.query_max_bytes_read {
            config.query_limits.max_bytes_read = Some(value);
        }
        if let Some(value) = self.query_max_samples {
            config.query_limits.max_samples_decoded = Some(value);
        }
        if let Some(value) = self.regex_max_expanded_values {
            config.query_limits.max_regex_values_examined = Some(value);
        }
        if let Some(value) = self.chunk_read_mode {
            config.chunk_read_config.mode = value.into();
        }
        if let Some(value) = self.chunk_read_queue_depth {
            config.chunk_read_config.queue_depth = value;
        }
        if let Some(value) = self.chunk_payload_coalesce_max_gap_bytes {
            config.chunk_read_config.payload_coalesce_max_gap_bytes = value;
        }
        if let Some(value) = self.experimental_cross_segment_chunk_reads {
            config.experimental_cross_segment_chunk_reads = value;
        }
        if let Some(value) = self.range_scalar_cache_max_bytes {
            config.range_scalar_cache_max_bytes = value;
        }
        config
    }

    fn validate(&self, ingestion: &IngestionConfig) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.head_publish_interval_ms == 0 {
            return Err("api.head_publish_interval_ms must be greater than zero".to_string());
        }
        let max_staleness = self.resolved_max_view_staleness_ms()?;
        if max_staleness < self.head_publish_interval_ms {
            return Err(format!(
                "api.max_view_staleness_ms ({max_staleness}) must be >= api.head_publish_interval_ms ({})",
                self.head_publish_interval_ms
            ));
        }
        match self.live_memory_admission_bytes {
            Some(value) if value > 0 => {}
            _ => {
                return Err(
                    "api.live_memory_admission_bytes must be explicitly configured and greater than zero when api.enabled=true"
                        .to_string(),
                );
            }
        }
        if self.max_concurrent_queries == Some(0) {
            return Err("api.max_concurrent_queries must be greater than zero".to_string());
        }
        if self.chunk_read_queue_depth == Some(0) {
            return Err("api.chunk_read_queue_depth must be greater than zero".to_string());
        }
        if self
            .chunk_payload_coalesce_max_gap_bytes
            .is_some_and(|value| value > MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES)
        {
            return Err(format!(
                "api.chunk_payload_coalesce_max_gap_bytes must be <= {MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES}"
            ));
        }
        if let Some(bytes) = self.range_scalar_cache_max_bytes {
            validate_range_scalar_cache_budget_bytes(bytes)
                .map_err(|error| format!("api.range_scalar_cache_max_bytes is invalid: {error}"))?;
        }
        self.listen
            .parse::<std::net::SocketAddr>()
            .map_err(|error| format!("api.listen is not a socket address: {error}"))?;
        if !ingestion.segment_writer.enabled {
            return Err(
                "api.enabled=true requires ingestion.segment_writer.enabled=true".to_string(),
            );
        }
        if ingestion.segment_writer.segment_duration_secs == 0 {
            return Err(
                "api.enabled=true requires ingestion.segment_writer.segment_duration_secs > 0"
                    .to_string(),
            );
        }
        if ingestion.segment_writer.storage_schema != StorageSchema::Schema8 {
            return Err(
                "api.enabled=true requires ingestion.segment_writer.storage_schema=\"schema8\""
                    .to_string(),
            );
        }
        if ingestion.capture_only {
            return Err(
                "api.enabled=true is incompatible with ingestion.capture_only=true".to_string(),
            );
        }
        if ingestion.labelset_store != LabelSetStoreKind::FlatInterned {
            return Err(
                "api.enabled=true requires ingestion.labelset_store=\"flat_interned\"".to_string(),
            );
        }
        Ok(())
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
mod tests;
