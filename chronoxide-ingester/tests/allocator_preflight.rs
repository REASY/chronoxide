use serde_json::Value;
use std::process::Command;

const RUNTIME_POLICY_PREFIX: &str = "CHRONOXIDE_ALLOCATOR_RUNTIME_POLICY_JSON=";

fn allocator_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_chronoxide-ingester"));
    command.env_clear();
    command
}

#[test]
fn preflight_proves_the_binary_global_allocator_with_a_live_allocation() {
    let mut command = allocator_command();
    command.arg("--allocator-preflight");
    let output = command.output().expect("run allocator preflight binary");
    assert!(
        output.status.success(),
        "preflight failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("preflight JSON");
    assert_eq!(value["schema"], "chronoxide/allocator-preflight/v3");

    if cfg!(all(
        feature = "jemalloc-stats",
        target_os = "linux",
        target_env = "gnu"
    )) {
        assert_eq!(value["rust_global_allocator"], "jemalloc");
        assert_eq!(value["global_allocator_probe"]["status"], "passed");
        assert_eq!(value["global_allocator_probe"]["passed"], true);
        let growth = value["global_allocator_probe"]["observed_allocated_growth_bytes"]
            .as_u64()
            .expect("probe growth");
        let minimum = value["global_allocator_probe"]["minimum_allocated_growth_bytes"]
            .as_u64()
            .expect("probe minimum");
        assert!(growth >= minimum);
    } else if cfg!(all(
        feature = "jemalloc",
        target_os = "linux",
        target_env = "gnu"
    )) {
        assert_eq!(value["rust_global_allocator"], "jemalloc");
        assert_eq!(
            value["global_allocator_probe"]["status"],
            "unavailable_without_jemalloc_stats"
        );
        assert!(value["global_allocator_probe"]["passed"].is_null());
        assert!(value["effective_policy"].is_null());
        assert_eq!(value["allocator_internal_telemetry"], "unavailable");
    } else {
        assert_eq!(value["rust_global_allocator"], "system");
        assert_eq!(
            value["global_allocator_probe"]["status"],
            "unavailable_for_system_allocator"
        );
        assert!(value["global_allocator_probe"]["passed"].is_null());
    }
}

#[test]
fn ordinary_startup_does_not_reinterpret_production_jemalloc_configuration() {
    let mut command = allocator_command();
    command
        .env("_RJEM_MALLOC_CONF", "metadata_thp:auto")
        .env("CONFIG_FILE", "/definitely/missing/chronoxide.toml");
    let output = command.output().expect("run ordinary ingester startup");
    assert!(
        !output.status.success(),
        "missing config unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("unsupported _RJEM_MALLOC_CONF option for the bounded allocator screen"),
        "ordinary startup applied the diagnostic-only bounded parser: {stderr}"
    );
    assert!(
        !stderr.contains(RUNTIME_POLICY_PREFIX),
        "ordinary startup emitted diagnostic allocator runtime evidence: {stderr}"
    );
}

#[test]
fn explicit_runtime_diagnostic_is_structured_and_feature_scoped() {
    let mut command = allocator_command();
    command
        .env("CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_RUNTIME_POLICY", "1")
        .env("_RJEM_MALLOC_CONF", "narenas:4")
        .env("CONFIG_FILE", "/definitely/missing/chronoxide.toml");
    let output = command.output().expect("run diagnostic ingester startup");
    assert!(
        !output.status.success(),
        "missing config unexpectedly succeeded"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let records = stderr
        .lines()
        .filter_map(|line| line.strip_prefix(RUNTIME_POLICY_PREFIX))
        .collect::<Vec<_>>();
    assert_eq!(
        records.len(),
        1,
        "expected exactly one allocator runtime record: {stderr}"
    );
    let value: Value = serde_json::from_str(records[0]).expect("allocator runtime JSON");
    assert_eq!(value["schema"], "chronoxide/allocator-runtime-policy/v1");
    assert_eq!(value["post_ingester_drop_hold_secs"], 0);
    assert_eq!(value["post_ingester_drop_checkpoint_enabled"], false);
    assert_eq!(value["post_ingester_drop_telemetry_enabled"], false);

    if cfg!(all(
        feature = "jemalloc-stats",
        target_os = "linux",
        target_env = "gnu"
    )) {
        assert_eq!(value["rust_global_allocator"], "jemalloc");
        assert_eq!(value["requested_policy_raw"], "narenas:4");
        assert_eq!(value["requested_policy_canonical"], "narenas:4");
        assert_eq!(value["effective_policy"]["narenas"], 4);
    } else if cfg!(all(
        feature = "jemalloc",
        target_os = "linux",
        target_env = "gnu"
    )) {
        assert_eq!(value["rust_global_allocator"], "jemalloc");
        assert!(value["requested_policy_raw"].is_null());
        assert!(value["requested_policy_canonical"].is_null());
        assert!(value["effective_policy"].is_null());
    } else {
        assert_eq!(value["rust_global_allocator"], "system");
        assert!(value["requested_policy_raw"].is_null());
        assert!(value["requested_policy_canonical"].is_null());
        assert!(value["effective_policy"].is_null());
    }
}

#[test]
fn runtime_diagnostic_trigger_rejects_noncanonical_values() {
    let mut command = allocator_command();
    command.env("CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_RUNTIME_POLICY", "true");
    let output = command.output().expect("run invalid diagnostic startup");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(
            "CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_RUNTIME_POLICY must be exactly 1 when enabled"
        )
    );
}

#[test]
fn bounded_policy_validation_is_stats_enabled_and_explicit() {
    let mut command = allocator_command();
    command
        .arg("--allocator-preflight")
        .env("_RJEM_MALLOC_CONF", "metadata_thp:auto");
    let output = command.output().expect("run scoped allocator preflight");
    if cfg!(all(
        feature = "jemalloc-stats",
        target_os = "linux",
        target_env = "gnu"
    )) {
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("unsupported _RJEM_MALLOC_CONF option for the bounded allocator screen")
        );
    } else {
        assert!(
            output.status.success(),
            "non-stats preflight changed production configuration semantics: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
