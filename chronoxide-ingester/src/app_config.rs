use crate::ingester::KafkaConsumerConfig;
use chronoxide_core::util::get_env_default;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LabelSetStoreKind {
    Naive,
    FlatInterned,
    KeySetDictEncoded,
}

impl Default for LabelSetStoreKind {
    fn default() -> Self {
        Self::FlatInterned
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub ingestion: IngestionConfig,
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
}

impl IngestionConfig {
    fn default_labelset_store() -> LabelSetStoreKind {
        if let Some(val) = get_env_default("INGESTION_LABELSET_STORE") {
            match val.to_lowercase().as_str() {
                "naive" => return LabelSetStoreKind::Naive,
                "flat_interned" => return LabelSetStoreKind::FlatInterned,
                "key_set_dict_encoded" => return LabelSetStoreKind::KeySetDictEncoded,
                _ => {}
            }
        }
        LabelSetStoreKind::default()
    }

    fn default_labelset_report_interval_secs() -> u64 {
        10
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
    }
}
