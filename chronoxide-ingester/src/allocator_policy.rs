//! Bounded allocator-policy diagnostics used by the Phase 5 replay screen.
//!
//! The normal ingestion hot path pays no allocator-control or checkpoint
//! cost. The existing Linux GNU `jemalloc` feature only selects the global
//! allocator. The diagnostic-only `jemalloc-stats` feature adds a fixed set
//! of startup control reads and release telemetry when an explicit diagnostic
//! surface is requested; the post-drop hold defaults to disabled in every
//! build.

use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const JEMALLOC_CONF_ENV: &str = "_RJEM_MALLOC_CONF";
pub const POST_DROP_HOLD_SECS_ENV: &str = "CHRONOXIDE_DIAGNOSTIC_POST_INGESTER_DROP_HOLD_SECS";
pub const POST_DROP_CHECKPOINT_ENV: &str = "CHRONOXIDE_DIAGNOSTIC_POST_INGESTER_DROP_CHECKPOINT";
pub const POST_DROP_TELEMETRY_ENV: &str = "CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_TELEMETRY";
pub const RUNTIME_POLICY_DIAGNOSTIC_ENV: &str = "CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_RUNTIME_POLICY";
pub const ALLOCATOR_PREFLIGHT_ARG: &str = "--allocator-preflight";
pub const MAX_POST_DROP_HOLD_SECS: u64 = 30;
pub const PREFLIGHT_SCHEMA: &str = "chronoxide/allocator-preflight/v3";
pub const RUNTIME_POLICY_SCHEMA: &str = "chronoxide/allocator-runtime-policy/v1";
pub const CHECKPOINT_SCHEMA: &str = "chronoxide/allocator-release-checkpoint/v1";
pub const TELEMETRY_SCHEMA: &str = "chronoxide/allocator-release-telemetry/v1";
#[cfg(all(feature = "jemalloc-stats", target_os = "linux", target_env = "gnu"))]
const GLOBAL_ALLOCATOR_PROBE_BYTES: usize = 64 * 1024 * 1024;
#[cfg(all(feature = "jemalloc-stats", target_os = "linux", target_env = "gnu"))]
const GLOBAL_ALLOCATOR_PROBE_MINIMUM_GROWTH_BYTES: usize = GLOBAL_ALLOCATOR_PROBE_BYTES * 3 / 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RustGlobalAllocator {
    System,
    Jemalloc,
}

impl RustGlobalAllocator {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Jemalloc => "jemalloc",
        }
    }
}

pub const fn rust_global_allocator() -> RustGlobalAllocator {
    if cfg!(all(
        feature = "jemalloc",
        target_os = "linux",
        target_env = "gnu"
    )) {
        RustGlobalAllocator::Jemalloc
    } else {
        RustGlobalAllocator::System
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct JemallocRequestedPolicy {
    pub abort_conf: Option<bool>,
    pub confirm_conf: Option<bool>,
    pub narenas: Option<u32>,
    pub dirty_decay_ms: Option<isize>,
    pub muzzy_decay_ms: Option<isize>,
    pub background_thread: Option<bool>,
    pub max_background_threads: Option<usize>,
    pub retain: Option<bool>,
}

impl JemallocRequestedPolicy {
    pub fn parse(value: &str) -> Result<Self, String> {
        if value.is_empty() {
            return Err(format!(
                "{JEMALLOC_CONF_ENV} must be unset for the jemalloc-default comparator, not set to an empty string"
            ));
        }
        if value.bytes().any(|byte| byte.is_ascii_whitespace()) {
            return Err(format!(
                "{JEMALLOC_CONF_ENV} must not contain whitespace: {value:?}"
            ));
        }

        let mut entries = BTreeMap::new();
        for entry in value.split(',') {
            let (key, raw_value) = entry.split_once(':').ok_or_else(|| {
                format!("invalid {JEMALLOC_CONF_ENV} entry without one ':' separator: {entry:?}")
            })?;
            if key.is_empty() || raw_value.is_empty() || raw_value.contains(':') {
                return Err(format!(
                    "invalid {JEMALLOC_CONF_ENV} key/value entry: {entry:?}"
                ));
            }
            if entries.insert(key, raw_value).is_some() {
                return Err(format!(
                    "duplicate {JEMALLOC_CONF_ENV} option is forbidden: {key}"
                ));
            }
        }

        let mut policy = Self::default();
        for (key, value) in entries {
            match key {
                "abort_conf" => policy.abort_conf = Some(parse_bool(key, value)?),
                "confirm_conf" => policy.confirm_conf = Some(parse_bool(key, value)?),
                "narenas" => {
                    let parsed = parse_u32(key, value)?;
                    if !(1..=64).contains(&parsed) {
                        return Err("narenas must be within the bounded range 1..=64".to_string());
                    }
                    policy.narenas = Some(parsed);
                }
                "dirty_decay_ms" => {
                    policy.dirty_decay_ms = Some(parse_decay(key, value)?);
                }
                "muzzy_decay_ms" => {
                    policy.muzzy_decay_ms = Some(parse_decay(key, value)?);
                }
                "background_thread" => {
                    policy.background_thread = Some(parse_bool(key, value)?);
                }
                "max_background_threads" => {
                    let parsed = value.parse::<usize>().map_err(|_| {
                        format!("{key} must be a base-10 positive integer; got {value:?}")
                    })?;
                    if !(1..=16).contains(&parsed) {
                        return Err(
                            "max_background_threads must be within the bounded range 1..=16"
                                .to_string(),
                        );
                    }
                    policy.max_background_threads = Some(parsed);
                }
                "retain" => policy.retain = Some(parse_bool(key, value)?),
                _ => {
                    return Err(format!(
                        "unsupported {JEMALLOC_CONF_ENV} option for the bounded allocator screen: {key}"
                    ));
                }
            }
        }

        if policy.max_background_threads.is_some() && policy.background_thread != Some(true) {
            return Err(
                "max_background_threads requires background_thread:true in the same policy"
                    .to_string(),
            );
        }
        Ok(policy)
    }

    pub fn canonical(&self) -> String {
        let mut entries = Vec::new();
        push_option(&mut entries, "abort_conf", self.abort_conf);
        push_option(&mut entries, "confirm_conf", self.confirm_conf);
        push_option(&mut entries, "narenas", self.narenas);
        push_option(&mut entries, "dirty_decay_ms", self.dirty_decay_ms);
        push_option(&mut entries, "muzzy_decay_ms", self.muzzy_decay_ms);
        push_option(&mut entries, "background_thread", self.background_thread);
        push_option(
            &mut entries,
            "max_background_threads",
            self.max_background_threads,
        );
        push_option(&mut entries, "retain", self.retain);
        entries.join(",")
    }
}

fn parse_bool(key: &str, value: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{key} must be true or false; got {value:?}")),
    }
}

fn parse_u32(key: &str, value: &str) -> Result<u32, String> {
    value
        .parse::<u32>()
        .map_err(|_| format!("{key} must be a base-10 unsigned integer; got {value:?}"))
}

fn parse_decay(key: &str, value: &str) -> Result<isize, String> {
    let parsed = value
        .parse::<isize>()
        .map_err(|_| format!("{key} must be an integer; got {value:?}"))?;
    if !(-1..=60_000).contains(&parsed) {
        return Err(format!(
            "{key} must be within the bounded range -1..=60000; got {parsed}"
        ));
    }
    Ok(parsed)
}

fn push_option<T: ToString>(entries: &mut Vec<String>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        entries.push(format!("{key}:{}", value.to_string()));
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JemallocEffectivePolicy {
    pub abort_conf: bool,
    pub confirm_conf: bool,
    pub narenas: u32,
    pub dirty_decay_ms: isize,
    pub muzzy_decay_ms: isize,
    pub background_thread: bool,
    pub max_background_threads: usize,
    pub retain: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GlobalAllocatorProbe {
    pub status: &'static str,
    pub allocation_bytes: Option<usize>,
    pub minimum_allocated_growth_bytes: Option<usize>,
    pub allocated_before_bytes: Option<usize>,
    pub allocated_while_live_bytes: Option<usize>,
    pub allocated_after_drop_bytes: Option<usize>,
    pub observed_allocated_growth_bytes: Option<usize>,
    pub passed: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct JemallocStatsSnapshot {
    epoch: u64,
    allocated_bytes: usize,
    active_bytes: usize,
    resident_bytes: usize,
    mapped_bytes: usize,
    retained_bytes: usize,
}

#[derive(Debug, Serialize)]
struct AllocatorTelemetryRecord<'a> {
    schema: &'static str,
    phase: &'a str,
    main_elapsed_ns: u128,
    unix_time_ns: u128,
    rust_global_allocator: RustGlobalAllocator,
    allocator_internal_telemetry: &'static str,
    epoch: Option<u64>,
    allocated_bytes: Option<usize>,
    active_bytes: Option<usize>,
    resident_bytes: Option<usize>,
    mapped_bytes: Option<usize>,
    retained_bytes: Option<usize>,
}

#[derive(Clone, Debug)]
struct PostDropDiagnostic {
    hold: Duration,
    checkpoint_path: Option<PathBuf>,
    telemetry_path: Option<PathBuf>,
}

impl PostDropDiagnostic {
    fn from_values(
        hold_value: Option<&str>,
        checkpoint_value: Option<&str>,
        telemetry_value: Option<&str>,
    ) -> Result<Self, String> {
        let hold_secs = match hold_value {
            None => 0,
            Some(value) => value.parse::<u64>().map_err(|_| {
                format!("{POST_DROP_HOLD_SECS_ENV} must be an integer in seconds; got {value:?}")
            })?,
        };
        if hold_secs > MAX_POST_DROP_HOLD_SECS {
            return Err(format!(
                "{POST_DROP_HOLD_SECS_ENV} must be <= {MAX_POST_DROP_HOLD_SECS}; got {hold_secs}"
            ));
        }

        let checkpoint_path = checkpoint_value.map(PathBuf::from);
        let telemetry_path = telemetry_value.map(PathBuf::from);
        match (hold_secs, checkpoint_path.as_ref(), telemetry_path.as_ref()) {
            (0, Some(_), _) => {
                return Err(format!(
                    "{POST_DROP_CHECKPOINT_ENV} requires a non-zero {POST_DROP_HOLD_SECS_ENV}"
                ));
            }
            (0, None, Some(_)) => {
                return Err(format!(
                    "{POST_DROP_TELEMETRY_ENV} requires a non-zero {POST_DROP_HOLD_SECS_ENV}"
                ));
            }
            (1.., None, _) => {
                return Err(format!(
                    "a non-zero {POST_DROP_HOLD_SECS_ENV} requires {POST_DROP_CHECKPOINT_ENV}"
                ));
            }
            (1.., Some(_), None) => {
                return Err(format!(
                    "a non-zero {POST_DROP_HOLD_SECS_ENV} requires {POST_DROP_TELEMETRY_ENV}"
                ));
            }
            _ => {}
        }
        if let Some(path) = checkpoint_path.as_ref() {
            validate_fresh_absolute_path(POST_DROP_CHECKPOINT_ENV, path)?;
        }
        if let Some(path) = telemetry_path.as_ref() {
            validate_fresh_absolute_path(POST_DROP_TELEMETRY_ENV, path)?;
        }
        if checkpoint_path == telemetry_path && checkpoint_path.is_some() {
            return Err(format!(
                "{POST_DROP_CHECKPOINT_ENV} and {POST_DROP_TELEMETRY_ENV} must use different paths"
            ));
        }
        Ok(Self {
            hold: Duration::from_secs(hold_secs),
            checkpoint_path,
            telemetry_path,
        })
    }
}

fn validate_fresh_absolute_path(name: &str, path: &Path) -> Result<(), String> {
    if !path.is_absolute() {
        return Err(format!(
            "{name} must be an absolute path: {}",
            path.display()
        ));
    }
    if path.exists() {
        return Err(format!(
            "{name} already exists; diagnostic output is never reused: {}",
            path.display()
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| format!("{name} has no parent directory: {}", path.display()))?;
    let metadata = parent
        .metadata()
        .map_err(|error| format!("cannot inspect {name} parent {}: {error}", parent.display()))?;
    if !metadata.is_dir() {
        return Err(format!(
            "{name} parent is not a directory: {}",
            parent.display()
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct AllocatorPreflight<'a> {
    pub schema: &'static str,
    pub rust_global_allocator: RustGlobalAllocator,
    pub jemalloc_conf_env: &'static str,
    pub requested_policy_raw: Option<&'a str>,
    pub requested_policy_canonical: Option<String>,
    pub effective_policy: Option<&'a JemallocEffectivePolicy>,
    pub global_allocator_probe: GlobalAllocatorProbe,
    pub allocator_internal_telemetry: &'static str,
    pub ld_preload_present: bool,
    pub malloc_conf_present: bool,
    pub post_ingester_drop_hold_secs: u64,
    pub post_ingester_drop_checkpoint_enabled: bool,
    pub post_ingester_drop_telemetry_enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct AllocatorRuntimeEvidence<'a> {
    pub schema: &'static str,
    pub rust_global_allocator: RustGlobalAllocator,
    pub jemalloc_conf_env: &'static str,
    pub requested_policy_raw: Option<&'a str>,
    pub requested_policy_canonical: Option<String>,
    pub effective_policy: Option<JemallocEffectivePolicy>,
    pub post_ingester_drop_hold_secs: u64,
    pub post_ingester_drop_checkpoint_enabled: bool,
    pub post_ingester_drop_telemetry_enabled: bool,
}

pub struct AllocatorRuntimePolicy {
    started: Instant,
    identity: RustGlobalAllocator,
    requested_raw: Option<String>,
    requested: Option<JemallocRequestedPolicy>,
    effective: Option<JemallocEffectivePolicy>,
    diagnostic: PostDropDiagnostic,
    ld_preload_present: bool,
    malloc_conf_present: bool,
    bounded_policy_diagnostics_enabled: bool,
    runtime_diagnostics_enabled: bool,
}

impl AllocatorRuntimePolicy {
    /// Construct the policy using a process-lifecycle start captured by the
    /// binary before it constructs the Tokio runtime.  This makes checkpoint
    /// elapsed time an honest main-entry-to-boundary timer.
    pub fn from_environment(
        started: Instant,
        allocator_preflight_requested: bool,
    ) -> Result<Self, String> {
        let identity = rust_global_allocator();
        let diagnostic = PostDropDiagnostic::from_values(
            read_unicode_environment(POST_DROP_HOLD_SECS_ENV)?.as_deref(),
            read_unicode_environment(POST_DROP_CHECKPOINT_ENV)?.as_deref(),
            read_unicode_environment(POST_DROP_TELEMETRY_ENV)?.as_deref(),
        )?;
        let runtime_policy_diagnostic = parse_explicit_diagnostic_flag(
            RUNTIME_POLICY_DIAGNOSTIC_ENV,
            read_unicode_environment(RUNTIME_POLICY_DIAGNOSTIC_ENV)?.as_deref(),
        )?;
        let runtime_diagnostics_enabled = runtime_policy_diagnostic || !diagnostic.hold.is_zero();
        let diagnostic_surface_requested =
            allocator_preflight_requested || runtime_diagnostics_enabled;
        let bounded_policy_diagnostics_enabled = diagnostic_surface_requested
            && cfg!(all(
                feature = "jemalloc-stats",
                target_os = "linux",
                target_env = "gnu"
            ));

        // `_RJEM_MALLOC_CONF` is jemalloc's production configuration surface.
        // Only the explicit stats-enabled Phase 5 diagnostic surface constrains
        // it to the bounded experiment policy. Ordinary system and plain
        // `jemalloc` startup must retain their historical behavior.
        let requested_raw = if bounded_policy_diagnostics_enabled {
            read_unicode_environment(JEMALLOC_CONF_ENV)?
        } else {
            None
        };
        let requested = match (identity, requested_raw.as_deref()) {
            (RustGlobalAllocator::System, Some(_)) => {
                return Err(format!(
                    "{JEMALLOC_CONF_ENV} is set, but this binary uses Rust's system allocator"
                ));
            }
            (RustGlobalAllocator::System, None) => None,
            (RustGlobalAllocator::Jemalloc, Some(value)) => {
                Some(JemallocRequestedPolicy::parse(value)?)
            }
            (RustGlobalAllocator::Jemalloc, None) => None,
        };

        let ld_preload_present = environment_has_nonempty_value("LD_PRELOAD");
        let malloc_conf_present = environment_has_nonempty_value("MALLOC_CONF");
        validate_ambient_allocator_environment(
            diagnostic_surface_requested,
            ld_preload_present,
            malloc_conf_present,
        )?;
        let effective = if bounded_policy_diagnostics_enabled {
            effective_jemalloc_policy()?
        } else {
            None
        };
        if let (Some(requested), Some(effective)) = (&requested, &effective) {
            verify_effective_policy(requested, effective)?;
        }

        Ok(Self {
            started,
            identity,
            requested_raw,
            requested,
            effective,
            diagnostic,
            ld_preload_present,
            malloc_conf_present,
            bounded_policy_diagnostics_enabled,
            runtime_diagnostics_enabled,
        })
    }

    pub const fn identity(&self) -> RustGlobalAllocator {
        self.identity
    }

    pub fn requested_raw(&self) -> Option<&str> {
        self.requested_raw.as_deref()
    }

    pub fn requested_canonical(&self) -> Option<String> {
        self.requested
            .as_ref()
            .map(JemallocRequestedPolicy::canonical)
    }

    pub fn effective(&self) -> Option<&JemallocEffectivePolicy> {
        self.effective.as_ref()
    }

    pub fn post_drop_hold_secs(&self) -> u64 {
        self.diagnostic.hold.as_secs()
    }

    pub const fn runtime_diagnostics_enabled(&self) -> bool {
        self.runtime_diagnostics_enabled
    }

    pub fn preflight(&self) -> Result<AllocatorPreflight<'_>, String> {
        Ok(AllocatorPreflight {
            schema: PREFLIGHT_SCHEMA,
            rust_global_allocator: self.identity,
            jemalloc_conf_env: JEMALLOC_CONF_ENV,
            requested_policy_raw: self.requested_raw(),
            requested_policy_canonical: self.requested_canonical(),
            effective_policy: self.effective(),
            global_allocator_probe: global_allocator_probe()?,
            allocator_internal_telemetry: if self.effective.is_some() {
                "fixed_startup_options_and_release_stats"
            } else {
                "unavailable"
            },
            ld_preload_present: self.ld_preload_present,
            malloc_conf_present: self.malloc_conf_present,
            post_ingester_drop_hold_secs: self.post_drop_hold_secs(),
            post_ingester_drop_checkpoint_enabled: self.diagnostic.checkpoint_path.is_some(),
            post_ingester_drop_telemetry_enabled: self.diagnostic.telemetry_path.is_some(),
        })
    }

    pub fn runtime_evidence(&self) -> Result<AllocatorRuntimeEvidence<'_>, String> {
        if !self.runtime_diagnostics_enabled {
            return Err("allocator runtime evidence requires an explicit diagnostic".to_string());
        }
        let runtime_effective = if self.bounded_policy_diagnostics_enabled {
            effective_jemalloc_policy()?
        } else {
            None
        };
        if runtime_effective != self.effective {
            return Err(format!(
                "jemalloc effective policy changed between startup preflight and measured runtime: startup {:?}, runtime {:?}",
                self.effective, runtime_effective
            ));
        }
        Ok(AllocatorRuntimeEvidence {
            schema: RUNTIME_POLICY_SCHEMA,
            rust_global_allocator: self.identity,
            jemalloc_conf_env: JEMALLOC_CONF_ENV,
            requested_policy_raw: self.requested_raw(),
            requested_policy_canonical: self.requested_canonical(),
            effective_policy: runtime_effective,
            post_ingester_drop_hold_secs: self.post_drop_hold_secs(),
            post_ingester_drop_checkpoint_enabled: self.diagnostic.checkpoint_path.is_some(),
            post_ingester_drop_telemetry_enabled: self.diagnostic.telemetry_path.is_some(),
        })
    }

    /// Emit the post-drop marker and hold only after the caller has left the
    /// scope that owns `Ingester`, its source, and its processor.
    pub fn hold_after_ingester_drop(&self) -> Result<(), String> {
        let Some(path) = self.diagnostic.checkpoint_path.as_ref() else {
            debug_assert!(self.diagnostic.hold.is_zero());
            debug_assert!(self.diagnostic.telemetry_path.is_none());
            return Ok(());
        };
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| {
                format!(
                    "cannot create allocator release checkpoint {}: {error}",
                    path.display()
                )
            })?;
        let mut writer = BufWriter::new(file);
        writeln!(
            writer,
            "schema\tphase\tmain_elapsed_ns\tunix_time_ns\thold_secs"
        )
        .map_err(checkpoint_error)?;
        self.write_checkpoint(&mut writer, "ingester_dropped")?;

        // The workload boundary is flushed above. All allocator stats work is
        // therefore diagnostic-only and outside the reported workload wall.
        // Capture both snapshots before opening or buffering the JSON output.
        // The telemetry writer therefore contaminates neither endpoint.
        // Refreshing jemalloc's epoch may itself do allocator work; that
        // unavoidable self-observation is part of the diagnostic uncertainty.
        let post_drop_telemetry = self.capture_allocator_telemetry("post_ingester_drop")?;
        std::thread::sleep(self.diagnostic.hold);
        let hold_complete_telemetry = self.capture_allocator_telemetry("hold_complete")?;
        let telemetry_path = self.diagnostic.telemetry_path.as_ref().ok_or_else(|| {
            format!("missing required diagnostic output: {POST_DROP_TELEMETRY_ENV}")
        })?;
        let telemetry_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(telemetry_path)
            .map_err(|error| {
                format!(
                    "cannot create allocator release telemetry {}: {error}",
                    telemetry_path.display()
                )
            })?;
        let mut telemetry_writer = BufWriter::new(telemetry_file);
        write_allocator_telemetry(&mut telemetry_writer, &post_drop_telemetry)?;
        write_allocator_telemetry(&mut telemetry_writer, &hold_complete_telemetry)?;
        self.write_checkpoint(&mut writer, "hold_complete")?;
        Ok(())
    }

    fn capture_allocator_telemetry<'a>(
        &self,
        phase: &'a str,
    ) -> Result<AllocatorTelemetryRecord<'a>, String> {
        let stats = jemalloc_stats_snapshot()?;
        Ok(AllocatorTelemetryRecord {
            schema: TELEMETRY_SCHEMA,
            phase,
            main_elapsed_ns: self.started.elapsed().as_nanos(),
            unix_time_ns: unix_time_ns("allocator telemetry")?,
            rust_global_allocator: self.identity,
            allocator_internal_telemetry: if stats.is_some() {
                "available"
            } else {
                "unavailable"
            },
            epoch: stats.as_ref().map(|value| value.epoch),
            allocated_bytes: stats.as_ref().map(|value| value.allocated_bytes),
            active_bytes: stats.as_ref().map(|value| value.active_bytes),
            resident_bytes: stats.as_ref().map(|value| value.resident_bytes),
            mapped_bytes: stats.as_ref().map(|value| value.mapped_bytes),
            retained_bytes: stats.as_ref().map(|value| value.retained_bytes),
        })
    }

    fn write_checkpoint(&self, writer: &mut BufWriter<File>, phase: &str) -> Result<(), String> {
        let unix_time_ns = unix_time_ns("allocator checkpoint")?;
        writeln!(
            writer,
            "{CHECKPOINT_SCHEMA}\t{phase}\t{}\t{unix_time_ns}\t{}",
            self.started.elapsed().as_nanos(),
            self.diagnostic.hold.as_secs()
        )
        .map_err(checkpoint_error)?;
        writer.flush().map_err(checkpoint_error)
    }
}

fn parse_explicit_diagnostic_flag(name: &str, value: Option<&str>) -> Result<bool, String> {
    match value {
        None => Ok(false),
        Some("1") => Ok(true),
        Some(value) => Err(format!(
            "{name} must be exactly 1 when enabled; got {value:?}"
        )),
    }
}

fn write_allocator_telemetry(
    writer: &mut BufWriter<File>,
    record: &AllocatorTelemetryRecord<'_>,
) -> Result<(), String> {
    serde_json::to_writer(&mut *writer, record)
        .map_err(|error| format!("cannot serialize allocator release telemetry: {error}"))?;
    writer.write_all(b"\n").map_err(telemetry_error)?;
    writer.flush().map_err(telemetry_error)
}

fn checkpoint_error(error: std::io::Error) -> String {
    format!("cannot write allocator release checkpoint: {error}")
}

fn telemetry_error(error: std::io::Error) -> String {
    format!("cannot write allocator release telemetry: {error}")
}

fn unix_time_ns(context: &str) -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes epoch at {context}: {error}"))
        .map(|value| value.as_nanos())
}

fn read_unicode_environment(name: &str) -> Result<Option<String>, String> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{name} must contain valid Unicode")),
    }
}

fn environment_has_nonempty_value(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| !value.is_empty())
}

fn validate_ambient_allocator_environment(
    diagnostic_enabled: bool,
    ld_preload_present: bool,
    malloc_conf_present: bool,
) -> Result<(), String> {
    if diagnostic_enabled && (ld_preload_present || malloc_conf_present) {
        return Err(format!(
            "diagnostic allocator runs forbid ambient allocator interposition/configuration; LD_PRELOAD present: {ld_preload_present}, MALLOC_CONF present: {malloc_conf_present}"
        ));
    }
    Ok(())
}

fn verify_effective_policy(
    requested: &JemallocRequestedPolicy,
    effective: &JemallocEffectivePolicy,
) -> Result<(), String> {
    macro_rules! verify {
        ($field:ident) => {
            if let Some(expected) = requested.$field
                && expected != effective.$field
            {
                return Err(format!(
                    "jemalloc ignored or changed {}: requested {:?}, effective {:?}",
                    stringify!($field),
                    expected,
                    effective.$field
                ));
            }
        };
    }
    verify!(abort_conf);
    verify!(confirm_conf);
    verify!(narenas);
    verify!(dirty_decay_ms);
    verify!(muzzy_decay_ms);
    verify!(background_thread);
    verify!(max_background_threads);
    verify!(retain);
    Ok(())
}

#[cfg(all(feature = "jemalloc-stats", target_os = "linux", target_env = "gnu"))]
mod jemalloc_introspection {
    use super::{JemallocEffectivePolicy, JemallocStatsSnapshot};
    use std::mem::{MaybeUninit, size_of};
    use std::ptr;

    pub(super) fn effective_policy() -> Result<JemallocEffectivePolicy, String> {
        Ok(JemallocEffectivePolicy {
            abort_conf: mallctl_read(b"opt.abort_conf\0")?,
            confirm_conf: mallctl_read(b"opt.confirm_conf\0")?,
            narenas: mallctl_read(b"opt.narenas\0")?,
            dirty_decay_ms: mallctl_read(b"opt.dirty_decay_ms\0")?,
            muzzy_decay_ms: mallctl_read(b"opt.muzzy_decay_ms\0")?,
            background_thread: mallctl_read(b"opt.background_thread\0")?,
            max_background_threads: mallctl_read(b"opt.max_background_threads\0")?,
            retain: mallctl_read(b"opt.retain\0")?,
        })
    }

    pub(super) fn stats_snapshot() -> Result<JemallocStatsSnapshot, String> {
        let epoch = mallctl_refresh_epoch()?;
        let snapshot = JemallocStatsSnapshot {
            epoch,
            allocated_bytes: mallctl_read(b"stats.allocated\0")?,
            active_bytes: mallctl_read(b"stats.active\0")?,
            resident_bytes: mallctl_read(b"stats.resident\0")?,
            mapped_bytes: mallctl_read(b"stats.mapped\0")?,
            retained_bytes: mallctl_read(b"stats.retained\0")?,
        };
        if snapshot.active_bytes < snapshot.allocated_bytes {
            return Err(format!(
                "jemalloc stats invariant failed: active {} < allocated {}",
                snapshot.active_bytes, snapshot.allocated_bytes
            ));
        }
        Ok(snapshot)
    }

    fn mallctl_refresh_epoch() -> Result<u64, String> {
        let new_epoch = 1_u64;
        let mut observed_epoch = MaybeUninit::<u64>::uninit();
        let mut observed_len = size_of::<u64>();
        // SAFETY: both epoch buffers are valid for one `u64`; the input is
        // immutable for the duration of the call, the output is writable, and
        // the static option name is NUL-terminated. Returned status and size
        // are checked before initialization is assumed.
        let status = unsafe {
            tikv_jemalloc_sys::mallctl(
                c"epoch".as_ptr(),
                observed_epoch.as_mut_ptr().cast(),
                &mut observed_len,
                ptr::from_ref(&new_epoch).cast_mut().cast(),
                size_of::<u64>(),
            )
        };
        if status != 0 {
            return Err(format!(
                "jemalloc mallctl epoch refresh failed with errno {status}"
            ));
        }
        if observed_len != size_of::<u64>() {
            return Err(format!(
                "jemalloc mallctl epoch refresh returned {observed_len} bytes, expected {}",
                size_of::<u64>()
            ));
        }
        // SAFETY: mallctl succeeded and reported the complete initialized
        // output size.
        Ok(unsafe { observed_epoch.assume_init() })
    }

    fn mallctl_read<T: Copy>(name: &'static [u8]) -> Result<T, String> {
        debug_assert_eq!(name.last(), Some(&0));
        let mut value = MaybeUninit::<T>::uninit();
        let mut value_len = size_of::<T>();
        // SAFETY: `name` is a static NUL-terminated byte string. `oldp` points
        // to exactly `value_len` writable bytes for the requested fixed-size
        // option; no write value is supplied. Success and returned size are
        // checked before initialization is assumed.
        let status = unsafe {
            tikv_jemalloc_sys::mallctl(
                name.as_ptr().cast(),
                value.as_mut_ptr().cast(),
                &mut value_len,
                ptr::null_mut(),
                0,
            )
        };
        if status != 0 {
            return Err(format!(
                "jemalloc mallctl read failed for {} with errno {status}",
                String::from_utf8_lossy(&name[..name.len().saturating_sub(1)])
            ));
        }
        if value_len != size_of::<T>() {
            return Err(format!(
                "jemalloc mallctl returned {} bytes for {}, expected {}",
                value_len,
                String::from_utf8_lossy(&name[..name.len().saturating_sub(1)]),
                size_of::<T>()
            ));
        }
        // SAFETY: mallctl succeeded and reported the complete initialized
        // size.
        Ok(unsafe { value.assume_init() })
    }
}

#[cfg(all(feature = "jemalloc-stats", target_os = "linux", target_env = "gnu"))]
fn effective_jemalloc_policy() -> Result<Option<JemallocEffectivePolicy>, String> {
    jemalloc_introspection::effective_policy().map(Some)
}

#[cfg(all(feature = "jemalloc-stats", target_os = "linux", target_env = "gnu"))]
fn global_allocator_probe() -> Result<GlobalAllocatorProbe, String> {
    let before = jemalloc_introspection::stats_snapshot()?.allocated_bytes;
    let mut allocation = vec![0_u8; GLOBAL_ALLOCATOR_PROBE_BYTES];
    for offset in (0..allocation.len()).step_by(4096) {
        // Touch each page and keep the allocation observably live through the
        // second mallctl snapshot.
        allocation[offset] = (offset / 4096) as u8;
    }
    std::hint::black_box(&allocation);
    let while_live = jemalloc_introspection::stats_snapshot()?.allocated_bytes;
    let observed_growth = while_live.saturating_sub(before);
    let passed = observed_growth >= GLOBAL_ALLOCATOR_PROBE_MINIMUM_GROWTH_BYTES;
    drop(allocation);
    let after_drop = jemalloc_introspection::stats_snapshot()?.allocated_bytes;
    if !passed {
        return Err(format!(
            "jemalloc global-allocation probe failed: a {GLOBAL_ALLOCATOR_PROBE_BYTES}-byte live Rust allocation increased stats.allocated by only {observed_growth} bytes; required at least {GLOBAL_ALLOCATOR_PROBE_MINIMUM_GROWTH_BYTES}"
        ));
    }
    Ok(GlobalAllocatorProbe {
        status: "passed",
        allocation_bytes: Some(GLOBAL_ALLOCATOR_PROBE_BYTES),
        minimum_allocated_growth_bytes: Some(GLOBAL_ALLOCATOR_PROBE_MINIMUM_GROWTH_BYTES),
        allocated_before_bytes: Some(before),
        allocated_while_live_bytes: Some(while_live),
        allocated_after_drop_bytes: Some(after_drop),
        observed_allocated_growth_bytes: Some(observed_growth),
        passed: Some(true),
    })
}

#[cfg(all(
    feature = "jemalloc",
    not(feature = "jemalloc-stats"),
    target_os = "linux",
    target_env = "gnu"
))]
fn global_allocator_probe() -> Result<GlobalAllocatorProbe, String> {
    Ok(GlobalAllocatorProbe {
        status: "unavailable_without_jemalloc_stats",
        allocation_bytes: None,
        minimum_allocated_growth_bytes: None,
        allocated_before_bytes: None,
        allocated_while_live_bytes: None,
        allocated_after_drop_bytes: None,
        observed_allocated_growth_bytes: None,
        passed: None,
    })
}

#[cfg(not(all(feature = "jemalloc", target_os = "linux", target_env = "gnu")))]
fn global_allocator_probe() -> Result<GlobalAllocatorProbe, String> {
    Ok(GlobalAllocatorProbe {
        status: "unavailable_for_system_allocator",
        allocation_bytes: None,
        minimum_allocated_growth_bytes: None,
        allocated_before_bytes: None,
        allocated_while_live_bytes: None,
        allocated_after_drop_bytes: None,
        observed_allocated_growth_bytes: None,
        passed: None,
    })
}

#[cfg(all(feature = "jemalloc-stats", target_os = "linux", target_env = "gnu"))]
fn jemalloc_stats_snapshot() -> Result<Option<JemallocStatsSnapshot>, String> {
    jemalloc_introspection::stats_snapshot().map(Some)
}

#[cfg(not(all(feature = "jemalloc-stats", target_os = "linux", target_env = "gnu")))]
fn jemalloc_stats_snapshot() -> Result<Option<JemallocStatsSnapshot>, String> {
    Ok(None)
}

#[cfg(not(all(feature = "jemalloc-stats", target_os = "linux", target_env = "gnu")))]
fn effective_jemalloc_policy() -> Result<Option<JemallocEffectivePolicy>, String> {
    Ok(None)
}

pub fn allocator_preflight_requested<I>(arguments: I) -> Result<bool, String>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let arguments = arguments
        .into_iter()
        .map(|value| value.as_ref().to_string())
        .collect::<Vec<_>>();
    if arguments.is_empty() {
        return Ok(false);
    }
    if arguments == [ALLOCATOR_PREFLIGHT_ARG] {
        return Ok(true);
    }
    if arguments
        .iter()
        .any(|value| value == ALLOCATOR_PREFLIGHT_ARG)
    {
        return Err(format!(
            "{ALLOCATOR_PREFLIGHT_ARG} must be the binary's only argument"
        ));
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_policy_parser_accepts_and_canonicalizes_the_screen_shape() {
        let policy = JemallocRequestedPolicy::parse(
            "narenas:2,confirm_conf:true,abort_conf:true,dirty_decay_ms:1000,\
             muzzy_decay_ms:0,background_thread:true,max_background_threads:1",
        )
        .unwrap();
        assert_eq!(
            policy.canonical(),
            "abort_conf:true,confirm_conf:true,narenas:2,dirty_decay_ms:1000,\
             muzzy_decay_ms:0,background_thread:true,max_background_threads:1"
        );
    }

    #[test]
    fn bounded_policy_parser_rejects_unknown_duplicate_and_unbounded_options() {
        for value in [
            "metadata_thp:auto",
            "narenas:2,narenas:4",
            "narenas:65",
            "dirty_decay_ms:60001",
            "background_thread:false,max_background_threads:1",
            "narenas:2, confirm_conf:true",
            "",
        ] {
            assert!(
                JemallocRequestedPolicy::parse(value).is_err(),
                "unexpectedly accepted {value:?}"
            );
        }
    }

    #[test]
    fn post_drop_diagnostic_is_zero_by_default_and_strict_when_enabled() {
        let disabled = PostDropDiagnostic::from_values(None, None, None).unwrap();
        assert!(disabled.hold.is_zero());
        assert!(disabled.checkpoint_path.is_none());
        assert!(disabled.telemetry_path.is_none());

        assert!(PostDropDiagnostic::from_values(Some("31"), None, None).is_err());
        assert!(PostDropDiagnostic::from_values(Some("30"), None, None).is_err());
        assert!(
            PostDropDiagnostic::from_values(Some("30"), Some("/tmp/checkpoint"), None).is_err()
        );
        assert!(
            PostDropDiagnostic::from_values(Some("0"), Some("/tmp/x"), Some("/tmp/y")).is_err()
        );
        assert!(PostDropDiagnostic::from_values(Some("x"), None, None).is_err());
    }

    #[test]
    fn allocator_preflight_argument_is_additive_and_unambiguous() {
        assert!(!allocator_preflight_requested(Vec::<String>::new()).unwrap());
        assert!(allocator_preflight_requested([ALLOCATOR_PREFLIGHT_ARG]).unwrap());
        assert!(allocator_preflight_requested([ALLOCATOR_PREFLIGHT_ARG, "extra"]).is_err());
        assert!(!allocator_preflight_requested(["--unrelated"]).unwrap());
    }

    #[test]
    fn requested_policy_must_match_effective_values() {
        let requested = JemallocRequestedPolicy::parse(
            "abort_conf:true,confirm_conf:true,narenas:2,dirty_decay_ms:1000,\
             muzzy_decay_ms:0,background_thread:true,max_background_threads:1",
        )
        .unwrap();
        let mut effective = JemallocEffectivePolicy {
            abort_conf: true,
            confirm_conf: true,
            narenas: 2,
            dirty_decay_ms: 1000,
            muzzy_decay_ms: 0,
            background_thread: true,
            max_background_threads: 1,
            retain: true,
        };
        verify_effective_policy(&requested, &effective).unwrap();
        effective.narenas = 4;
        assert!(verify_effective_policy(&requested, &effective).is_err());
    }

    #[test]
    fn diagnostic_allocator_environment_rejects_ambient_confounders() {
        validate_ambient_allocator_environment(false, true, true).unwrap();
        validate_ambient_allocator_environment(true, false, false).unwrap();
        assert!(validate_ambient_allocator_environment(true, true, false).is_err());
        assert!(validate_ambient_allocator_environment(true, false, true).is_err());
    }

    #[test]
    fn runtime_policy_diagnostic_flag_is_explicit_and_strict() {
        assert!(!parse_explicit_diagnostic_flag("TEST", None).unwrap());
        assert!(parse_explicit_diagnostic_flag("TEST", Some("1")).unwrap());
        for value in ["", "0", "true", "2"] {
            assert!(parse_explicit_diagnostic_flag("TEST", Some(value)).is_err());
        }
    }

    #[test]
    fn release_telemetry_has_exact_phases_and_explicit_availability() {
        let directory = tempfile::tempdir().unwrap();
        let checkpoint = directory.path().join("checkpoint.tsv");
        let telemetry = directory.path().join("telemetry.ndjson");
        let effective = effective_jemalloc_policy().unwrap();
        let policy = AllocatorRuntimePolicy {
            started: Instant::now(),
            identity: rust_global_allocator(),
            requested_raw: None,
            requested: None,
            effective,
            diagnostic: PostDropDiagnostic {
                hold: Duration::ZERO,
                checkpoint_path: Some(checkpoint.clone()),
                telemetry_path: Some(telemetry.clone()),
            },
            ld_preload_present: false,
            malloc_conf_present: false,
            bounded_policy_diagnostics_enabled: cfg!(all(
                feature = "jemalloc-stats",
                target_os = "linux",
                target_env = "gnu"
            )),
            runtime_diagnostics_enabled: true,
        };

        policy.hold_after_ingester_drop().unwrap();
        let checkpoint_lines = std::fs::read_to_string(checkpoint)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert_eq!(checkpoint_lines.len(), 3);
        let checkpoint_fields = checkpoint_lines
            .iter()
            .map(|line| line.split('\t').collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            checkpoint_fields[0],
            [
                "schema",
                "phase",
                "main_elapsed_ns",
                "unix_time_ns",
                "hold_secs",
            ]
        );
        assert_eq!(checkpoint_fields[1].len(), 5);
        assert_eq!(checkpoint_fields[2].len(), 5);
        assert_eq!(checkpoint_fields[1][0], CHECKPOINT_SCHEMA);
        assert_eq!(checkpoint_fields[2][0], CHECKPOINT_SCHEMA);
        assert_eq!(checkpoint_fields[1][1], "ingester_dropped");
        assert_eq!(checkpoint_fields[2][1], "hold_complete");
        for fields in &checkpoint_fields[1..] {
            assert!(fields[2].parse::<u128>().is_ok());
            assert!(fields[3].parse::<u128>().is_ok());
            assert_eq!(fields[4], "0");
        }
        let records = std::fs::read_to_string(telemetry)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["phase"], "post_ingester_drop");
        assert_eq!(records[1]["phase"], "hold_complete");
        if cfg!(all(
            feature = "jemalloc-stats",
            target_os = "linux",
            target_env = "gnu"
        )) {
            assert_eq!(rust_global_allocator(), RustGlobalAllocator::Jemalloc);
            assert_eq!(records[0]["allocator_internal_telemetry"], "available");
            assert!(records[0]["epoch"].as_u64().unwrap() > 0);
            assert!(records[1]["epoch"].as_u64().unwrap() > records[0]["epoch"].as_u64().unwrap());
            assert!(
                records[0]["active_bytes"].as_u64().unwrap()
                    >= records[0]["allocated_bytes"].as_u64().unwrap()
            );
        } else {
            assert_eq!(records[0]["allocator_internal_telemetry"], "unavailable");
            for key in [
                "epoch",
                "allocated_bytes",
                "active_bytes",
                "resident_bytes",
                "mapped_bytes",
                "retained_bytes",
            ] {
                assert!(records[0][key].is_null());
                assert!(records[1][key].is_null());
            }
        }
    }
}
