#!/usr/bin/env python3
"""Fail-closed admission helpers for the Phase 5 allocator full gate.

The shell runner owns process orchestration.  This module owns every decision:
it binds a completed screen, validates raw observations, recomputes both 4M
comparisons, verifies canonical footer/readback evidence, and admits the final
result from immutable raw leaves.  Reduced summaries are never authorities.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable


class GateError(ValueError):
    pass


PLAN_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-full-plan/v1"
SCREEN_BINDING_SCHEMA = "chronoxide/storage-vnext-phase5-screen-binding/v1"
BUILD_SCHEMA = "chronoxide/storage-vnext-phase5-no-stats-build/v1"
PREFLIGHT_SCHEMA = "chronoxide/storage-vnext-phase5-full-preflight/v1"
OBSERVATION_SCHEMA = "chronoxide/storage-vnext-phase5-full-observation/v1"
STAGE_SCHEMA = "chronoxide/storage-vnext-phase5-full-stage-decision/v1"
VALIDATION_SCHEMA = "chronoxide/storage-vnext-phase5-full-validation/v1"
AUTHORITY_SCHEMA = "chronoxide/storage-vnext-phase5-raw-authority/v2"
CAPACITY_SCHEMA = "chronoxide/storage-vnext-phase5-capacity/v1"
RUN_CAPACITY_SCHEMA = "chronoxide/storage-vnext-phase5-run-capacity/v1"
TOOLCHAIN_SCHEMA = "chronoxide/storage-vnext-phase5-toolchain-binding/v1"
FINAL_SCHEMA = "chronoxide/storage-vnext-phase5-full-final-decision/v1"
COMPLETION_SCHEMA = "chronoxide/storage-vnext-phase5-full-completion/v1"
ARTIFACT_SCHEMA = "chronoxide/storage-vnext-phase5-result-artifacts/v2"
FINAL_ADMISSION_SCHEMA = "chronoxide/storage-vnext-phase5-final-admission/v1"
GUARDIAN_SCHEMA = "chronoxide/storage-vnext-phase5-full-external-conflict-guardian/v5"
GUARDIAN_CONTROL_SCHEMA = "chronoxide/storage-vnext-phase5-full-guardian-control/v2"
GUARDIAN_CLEANUP_SCHEMA = "chronoxide/storage-vnext-phase5-full-guardian-cleanup/v1"
CONFLICT_SCAN_SCHEMA = "chronoxide/storage-vnext-phase5-full-conflict-scan/v1"
GUARDIAN_CADENCE_EDGE_ALLOWANCE_NS = 100_000_000

SCREEN_FINAL_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-final-decision/v3"
SCREEN_PLAN_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-screen-plan/v5"
APPLICATION_PREFLIGHT_SCHEMA = "chronoxide/allocator-preflight/v3"
APPLICATION_RUNTIME_SCHEMA = "chronoxide/allocator-runtime-policy/v1"
CHECKPOINT_SCHEMA = "chronoxide/allocator-release-checkpoint/v1"
TELEMETRY_SCHEMA = "chronoxide/allocator-release-telemetry/v1"
CORPUS_SCHEMA = "chronoxide/storage-vnext-phase1-corpus/v1"

SELECTED_POLICIES = ("J1", "J2", "J3")
EXPECTED_STAGE_SCHEDULES = {
    "stats": ["S", "C", "C", "S"],
    "no-stats": ["S", "N", "N", "S"],
}
EXPECTED_PERF_EVENTS = {
    "task-clock",
    "cycles",
    "instructions",
    "branches",
    "branch-misses",
    "cache-references",
    "cache-misses",
    "page-faults",
    "minor-faults",
    "major-faults",
    "context-switches",
    "cpu-migrations",
}
KIND_ORDER = ("float", "int64", "histogram", "exponential_histogram", "summary")
EXPECTED_FLOAT_ENCODINGS = {"gorilla"}
VALID_KIND_ENCODING_LAYOUTS = {
    ("float", "raw_f64"): "t0_interleaved_dt_value",
    ("float", "gorilla"): "t0_dt_then_values",
    ("int64", "raw_i64"): "t0_interleaved_dt_value",
    ("int64", "int_delta_zigzag"): "t0_dt_then_values",
    ("histogram", "schema_varlen"): "typed_scalar_lane_and_t0_dt_schema_varlen",
    (
        "exponential_histogram",
        "schema_varlen",
    ): "typed_scalar_lane_and_t0_dt_schema_varlen",
    ("summary", "schema_varlen"): "typed_scalar_lane_and_t0_dt_schema_varlen",
}
CHUNK_INVENTORY_ROW_FIELDS = {
    "kind",
    "encoding",
    "payload_layout",
    "chunks",
    "points",
    "indexed_bytes",
    "common_header_bytes",
    "scalar_lane_bytes",
    "payload_bytes",
    "timestamp_base_bytes",
    "timestamp_delta_bytes",
    "value_bytes",
    "point_count_histogram",
    "cadence_ms_histogram",
}
FLOAT_EVIDENCE_FIELDS = {
    "tie_rule",
    "chunks",
    "points",
    "existing_indexed_bytes",
    "existing_payload_bytes",
    "raw_f64_candidate_indexed_bytes",
    "raw_f64_candidate_payload_bytes",
    "gorilla_candidate_indexed_bytes",
    "gorilla_candidate_payload_bytes",
    "adaptive_min_indexed_bytes",
    "adaptive_min_payload_bytes",
    "raw_f64_wins",
    "gorilla_wins",
    "ties",
    "adaptive_raw_f64_selections",
    "adaptive_gorilla_selections",
    "repeated_xor_points",
    "reused_window_points",
    "new_window_points",
    "xor_significant_bits_histogram",
    "positive_zero_points",
    "negative_zero_points",
    "finite_nonzero_points",
    "positive_infinity_points",
    "negative_infinity_points",
    "ordinary_nan_points",
    "stale_nan_points",
}
TIMESTAMP_SHAPES = {
    "single_point",
    "constant_zero_step",
    "constant_positive_step",
    "variable_step",
}
TIMESTAMP_CANDIDATES = (
    "current_offset_uleb",
    "adjacent_delta_uleb",
    "delta_of_delta_zigzag_uleb128",
    "fixed_step_residual_bitpack",
)
RSS_KEYS = {
    "root_pid",
    "root_starttime_ticks",
    "interval_ms",
    "clock_ticks_per_second",
    "samples",
    "workload_samples",
    "post_drop_samples",
    "hold_complete_samples",
    "checkpoint_incomplete_samples",
    "peak_rss_kib",
    "peak_rss_anon_kib",
    "peak_rss_file_kib",
    "peak_vm_swap_kib",
    "peak_process_count",
    "workload_peak_rss_kib",
    "workload_peak_max_single_hwm_kib",
    "workload_boundary_max_single_hwm_kib",
    "post_drop_first_rss_kib",
    "post_drop_min_rss_kib",
    "post_drop_end_rss_kib",
    "post_drop_first_unix_time_ns",
    "post_drop_end_unix_time_ns",
    "workload_boundary_cpu_ticks",
    "workload_boundary_cpu_seconds",
    "workload_boundary_sample_window_start_unix_time_ns",
    "workload_boundary_sample_unix_time_ns",
    "elapsed_ns",
    "poll_monotonic_elapsed_ns",
    "maximum_poll_start_gap_ns",
    "maximum_allowed_poll_start_gap_ns",
    "control_path",
    "control_sha256",
    "rss_ready_marker_path",
    "rss_ready_marker_sha256",
    "rss_ready_created_sample",
    "rss_ready_created_monotonic_elapsed_ns",
    "launch_marker_path",
    "launch_marker_sha256",
    "launch_observed_sample",
    "launch_observed_monotonic_elapsed_ns",
    "launch_observed",
    "terminal_observation",
    "terminal_launch_observed",
    "handshake_violations",
    "complete",
}
TELEMETRY_KEYS = {
    "schema",
    "phase",
    "main_elapsed_ns",
    "unix_time_ns",
    "rust_global_allocator",
    "allocator_internal_telemetry",
    "epoch",
    "allocated_bytes",
    "active_bytes",
    "resident_bytes",
    "mapped_bytes",
    "retained_bytes",
}
EFFECTIVE_POLICY_KEYS = {
    "abort_conf",
    "confirm_conf",
    "narenas",
    "dirty_decay_ms",
    "muzzy_decay_ms",
    "background_thread",
    "max_background_threads",
    "retain",
}
PROMQL_READBACK_HEADER = [
    "Kind",
    "Query",
    "result_series",
    "result_samples",
    "matched_series",
    "projected_series",
    "chunk_reads",
    "bytes_read",
    "samples_decoded",
    "typed_scalar_chunks_decoded",
    "typed_full_chunks_decoded",
]


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8", errors="strict") as source:
        return json.load(source)


def write_json_exclusive(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as destination:
        json.dump(value, destination, indent=2, sort_keys=True)
        destination.write("\n")


def fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_json_read_only_atomic_exclusive(path: Path, value: Any) -> None:
    """Publish complete mode-0444 JSON without exposing a writable partial file."""
    parent = directory_non_symlink(path.parent, "atomic JSON parent")
    if path.exists() or path.is_symlink():
        raise GateError(f"refusing to reuse atomic JSON output: {path}")
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.tmp.", dir=parent
    )
    temporary = Path(temporary_name)
    descriptor_open = True
    try:
        with os.fdopen(descriptor, "wb") as destination:
            descriptor_open = False
            destination.write(payload)
            destination.flush()
            os.fchmod(destination.fileno(), 0o444)
            os.fsync(destination.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise GateError(f"refusing to reuse atomic JSON output: {path}") from error
        fsync_directory(parent)
    finally:
        if descriptor_open:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        else:
            fsync_directory(parent)
    published = regular_non_symlink(path, "atomic JSON output")
    if (
        stat.S_IMODE(published.stat().st_mode) != 0o444
        or published.read_bytes() != payload
    ):
        raise GateError("atomic JSON output differs from its finalized payload")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(16 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def require_exact_keys(value: Any, keys: set[str], path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GateError(f"{path} must be an object")
    if set(value) != keys:
        raise GateError(
            f"{path} keys differ: missing={sorted(keys - set(value))!r} "
            f"extra={sorted(set(value) - keys)!r}"
        )
    return value


def strict_int(value: Any, path: str, *, minimum: int | None = None) -> int:
    if type(value) is not int:
        raise GateError(f"{path} must be an integer")
    if minimum is not None and value < minimum:
        raise GateError(f"{path} must be >= {minimum}")
    return value


def strict_number(value: Any, path: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GateError(f"{path} must be numeric")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0):
        raise GateError(f"{path} must be finite{' and positive' if positive else ''}")
    return result


def strict_sha256(value: Any, path: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise GateError(f"{path} must be a lowercase SHA-256 digest")
    return value


def regular_non_symlink(path: Path, description: str) -> Path:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as error:
        raise GateError(f"{description} is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise GateError(f"{description} must be a regular non-symlink file: {path}")
    return path


def directory_non_symlink(path: Path, description: str) -> Path:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as error:
        raise GateError(f"{description} is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise GateError(f"{description} must be a non-symlink directory: {path}")
    return path


def validate_plan(path: Path) -> dict[str, Any]:
    plan = require_exact_keys(
        load_json(path),
        {"schema", "workload", "stages", "gate", "environment", "capacity", "build", "validation", "completion"},
        "$",
    )
    if plan["schema"] != PLAN_SCHEMA:
        raise GateError("full-gate plan schema mismatch")
    workload = require_exact_keys(
        plan["workload"],
        {
            "stop_after_messages",
            "post_ingester_drop_hold_secs",
            "rss_interval_ms",
            "storage_schema",
            "readback_sample_limit_per_kind",
            "capture_eviction_required",
            "max_capture_resident_bytes_after_evict",
        },
        "$.workload",
    )
    if workload != {
        "stop_after_messages": 4_000_000,
        "post_ingester_drop_hold_secs": 30,
        "rss_interval_ms": 100,
        "storage_schema": "schema8",
        "readback_sample_limit_per_kind": 2,
        "capture_eviction_required": True,
        "max_capture_resident_bytes_after_evict": 0,
    }:
        raise GateError("full-gate workload differs from the frozen 4M contract")
    stages = plan["stages"]
    if not isinstance(stages, list) or len(stages) != 2:
        raise GateError("full gate requires exactly two stages")
    expected_roles = {"stats": "jemalloc-stats", "no-stats": "jemalloc"}
    for index, raw in enumerate(stages):
        stage = require_exact_keys(
            raw, {"name", "candidate_binary_role", "schedule"}, f"$.stages[{index}]"
        )
        name = stage["name"]
        if name not in EXPECTED_STAGE_SCHEDULES:
            raise GateError(f"unknown full-gate stage: {name!r}")
        if stage["candidate_binary_role"] != expected_roles[name]:
            raise GateError(f"{name} candidate binary role changed")
        if stage["schedule"] != EXPECTED_STAGE_SCHEDULES[name]:
            raise GateError(f"{name} schedule must be counterbalanced")
    if [stage["name"] for stage in stages] != ["stats", "no-stats"]:
        raise GateError("stats-enabled stage must precede no-stats revalidation")
    gate = require_exact_keys(
        plan["gate"],
        {
            "minimum_workload_cpu_improvement_percent",
            "maximum_workload_peak_rss_regression_percent",
            "maximum_workload_hwm_regression_percent",
            "maximum_post_drop_end_rss_regression_percent",
            "maximum_pair_relative_spread_percent",
            "minimum_post_drop_rss_samples",
            "maximum_hold_elapsed_secs",
            "maximum_workload_cpu_boundary_uncertainty_intervals",
        },
        "$.gate",
    )
    expected_gate = {
        "minimum_workload_cpu_improvement_percent": 3.0,
        "maximum_workload_peak_rss_regression_percent": 5.0,
        "maximum_workload_hwm_regression_percent": 5.0,
        "maximum_post_drop_end_rss_regression_percent": 5.0,
        "maximum_pair_relative_spread_percent": 5.0,
        "minimum_post_drop_rss_samples": 20,
        "maximum_hold_elapsed_secs": 60,
        "maximum_workload_cpu_boundary_uncertainty_intervals": 1,
    }
    if gate != expected_gate:
        raise GateError("full-gate decision thresholds changed")
    environment = require_exact_keys(
        plan["environment"],
        {
            "locale",
            "timezone",
            "rust_log",
            "external_conflict_poll_interval_ms",
            "maximum_dirty_writeback_kib",
            "required_quiescent_samples",
            "quiescence_poll_interval_ms",
            "quiescence_timeout_secs",
        },
        "$.environment",
    )
    if environment != {
        "locale": "C",
        "timezone": "UTC",
        "rust_log": "chronoxide_ingester=info,chronoxide_core=warn",
        "external_conflict_poll_interval_ms": 100,
        "maximum_dirty_writeback_kib": 65_536,
        "required_quiescent_samples": 3,
        "quiescence_poll_interval_ms": 250,
        "quiescence_timeout_secs": 120,
    }:
        raise GateError("full-gate environment contract changed")
    capacity = require_exact_keys(
        plan["capacity"],
        {"retained_corpus_count", "additional_headroom_bytes", "build_headroom_bytes"},
        "$.capacity",
    )
    if capacity != {
        "retained_corpus_count": 8,
        "additional_headroom_bytes": 10 * 1024**3,
        "build_headroom_bytes": 10 * 1024**3,
    }:
        raise GateError("full-gate capacity contract changed")
    build = require_exact_keys(
        plan["build"], {"no_stats_command", "source_mode", "cargo_incremental"}, "$.build"
    )
    expected_build = (
        "cargo build --manifest-path Cargo.toml --locked --release "
        "--no-default-features --features jemalloc -p chronoxide-ingester "
        "--bin chronoxide-ingester"
    )
    if build != {
        "no_stats_command": expected_build,
        "source_mode": "completed-screen read-only Git archive extraction",
        "cargo_incremental": False,
    }:
        raise GateError("plain-jemalloc build contract changed")
    validation = require_exact_keys(
        plan["validation"],
        {
            "canonical_roles",
            "footer_validation_required",
            "exact_postings_required",
            "independent_readbacks_required",
            "expected_readback_queries",
            "expected_promql_rows",
        },
        "$.validation",
    )
    if validation != {
        "canonical_roles": ["stats-candidate", "no-stats-candidate"],
        "footer_validation_required": True,
        "exact_postings_required": True,
        "independent_readbacks_required": True,
        "expected_readback_queries": 38,
        "expected_promql_rows": 14,
    }:
        raise GateError("canonical validation contract changed")
    completion = require_exact_keys(
        plan["completion"],
        {
            "required_observations",
            "partial_runs_promotable",
            "production_promotion_authorized",
            "manual_review_required",
            "required_final_artifacts",
        },
        "$.completion",
    )
    if completion != {
        "required_observations": 8,
        "partial_runs_promotable": False,
        "production_promotion_authorized": False,
        "manual_review_required": True,
        "required_final_artifacts": [
            "comparisons/final-full-gate-decision.json",
            "metadata/result-artifact-files.nul",
            "metadata/result-directories.nul",
            "metadata/result-artifacts.tsv",
            "metadata/FINAL_SEAL_VALIDATED.json",
            "COMPLETE",
        ],
    }:
        raise GateError("full-gate completion contract changed")
    return plan


def parse_sha256_manifest(root: Path, manifest: Path) -> dict[str, str]:
    directory_non_symlink(root, "screen result root")
    regular_non_symlink(manifest, "screen artifact manifest")
    entries: dict[str, str] = {}
    for line_number, line in enumerate(
        manifest.read_text(encoding="utf-8", errors="strict").splitlines(), start=1
    ):
        match = re.fullmatch(r"([0-9a-f]{64}) [ *](.+)", line)
        if match is None:
            raise GateError(f"screen artifact manifest line {line_number} is malformed")
        digest, relative_text = match.groups()
        relative = Path(relative_text)
        if (
            relative.is_absolute()
            or not relative.parts
            or relative.as_posix() != relative_text
            or any(part in {"", ".", ".."} for part in relative.parts)
            or relative_text in entries
        ):
            raise GateError("screen artifact manifest contains an unsafe or duplicate path")
        candidate = root
        for part in relative.parts:
            candidate /= part
            if candidate.is_symlink():
                raise GateError(f"screen artifact traverses a symlink: {relative_text}")
        regular_non_symlink(candidate, f"screen artifact {relative_text}")
        if sha256_file(candidate) != digest:
            raise GateError(f"screen artifact digest changed: {relative_text}")
        entries[relative_text] = digest
    if not entries:
        raise GateError("screen artifact manifest is empty")
    return entries


def screen_binding(screen_root: Path, plan_path: Path) -> dict[str, Any]:
    validate_plan(plan_path)
    if not screen_root.is_absolute():
        raise GateError("screen result root must be absolute")
    root = directory_non_symlink(screen_root, "screen result root").resolve(strict=True)
    canonical = {
        "complete": root / "COMPLETE",
        "final": root / "comparisons/final-screen-decision.json",
        "summary": root / "comparisons/screen-summary.json",
        "artifacts": root / "metadata/result-artifacts.sha256",
        "artifact_files": root / "metadata/result-artifacts.nul",
        "artifact_directories": root / "metadata/result-directories.nul",
        "final_seal": root / "metadata/FINAL_SEAL_VALIDATED.json",
        "core_controls": root / "metadata/core-controls.json",
        "measurement_controls": root / "metadata/measurement-controls.json",
        "build": root / "metadata/build-provenance.json",
        "environment": root / "metadata/environment.txt",
        "source_seal": root / "metadata/source/formal-source-seal.json",
        "source_archive": root / "metadata/source/git-head.tar",
        "extracted_seal": root / "metadata/source/extracted-build-source-seal.json",
        "screen_gate": root / "metadata/harness/phase5_allocator_screen_gate.py",
        "screen_plan": root / "metadata/harness/phase5_allocator_screen_plan.json",
        "phase1_gate": root / "metadata/harness/phase1_replay_gate.py",
        "expectations": root / "metadata/harness/phase1_4m_expectations.json",
        "report_gate": root / "metadata/harness/ab_gate.py",
        "fadvise": root / "metadata/tools/fadvise-regular-dontneed",
        "system": root / "metadata/binaries/chronoxide-ingester-system",
        "stats": root / "metadata/binaries/chronoxide-ingester-jemalloc",
        "query": root / "metadata/binaries/chronoxide-query",
        "storage_verify": root / "metadata/binaries/chronoxide-storage-verify",
        "build_source": root / "build-source",
    }
    for name, path in canonical.items():
        if name == "build_source":
            directory_non_symlink(path, "screen extracted build source")
        else:
            regular_non_symlink(path, f"screen {name}")
    screen_validation = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-B",
            str(canonical["screen_gate"]),
            "validate-final-artifacts",
            "--result-root",
            str(root),
            "--stage",
            "complete",
        ],
        env={"LC_ALL": "C", "TZ": "UTC"},
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if screen_validation.returncode != 0:
        raise GateError(
            "completed screen's frozen final-artifact validator failed: "
            f"{screen_validation.stderr.strip()}"
        )
    if (
        canonical["complete"].read_bytes()
        != b"chronoxide/allocator-screen-complete/v1\n"
        or stat.S_IMODE(canonical["complete"].stat().st_mode) != 0o444
    ):
        raise GateError("screen COMPLETE marker content, version, or mode changed")
    entries = parse_sha256_manifest(root, canonical["artifacts"])
    required_artifacts = {
        str(path.relative_to(root))
        for name, path in canonical.items()
        if name
        not in {
            "complete",
            "artifacts",
            "artifact_files",
            "artifact_directories",
            "final_seal",
            "build_source",
        }
    }
    required_artifacts.update(
        {
            "build-source/docs/experiments/storage_vnext/phase5_allocator_full_gate.py",
            "build-source/docs/experiments/storage_vnext/phase5_allocator_full_plan.json",
            "build-source/docs/experiments/storage_vnext/phase5_allocator_full_run.sh",
            "build-source/docs/experiments/storage_vnext/test_phase5_allocator_full_gate.py",
        }
    )
    missing = sorted(required_artifacts - set(entries))
    if missing:
        raise GateError(f"screen artifact authority lacks full-gate/source inputs: {missing!r}")

    final = load_json(canonical["final"])
    if (
        not isinstance(final, dict)
        or final.get("schema") != SCREEN_FINAL_SCHEMA
        or final.get("screen_complete") is not True
        or final.get("canonical_validation_complete") is not True
        or final.get("production_promotion_authorized") is not False
        or final.get("run_count") != 10
    ):
        raise GateError("screen final decision is incomplete or promotional")
    selected = final.get("selected_full_gate_policy")
    if selected not in SELECTED_POLICIES:
        raise GateError("completed screen did not nominate one bounded policy")
    summary = load_json(canonical["summary"])
    if not isinstance(summary, dict) or summary.get("selected_full_gate_policy") != selected:
        raise GateError("screen summary and final decision nominate different policies")
    if selected not in summary.get("eligible_policies", []):
        raise GateError("screen nominated an ineligible policy")

    screen_plan = load_json(canonical["screen_plan"])
    if not isinstance(screen_plan, dict) or screen_plan.get("schema") != SCREEN_PLAN_SCHEMA:
        raise GateError("screen plan schema mismatch")
    policy = screen_plan.get("policies", {}).get(selected)
    if (
        not isinstance(policy, dict)
        or policy.get("binary_role") != "jemalloc"
        or policy.get("rust_global_allocator") != "jemalloc"
        or policy.get("comparator_only") is not False
        or not isinstance(policy.get("jemalloc_conf"), str)
    ):
        raise GateError("screen nomination lacks one bounded jemalloc policy")
    conf = policy["jemalloc_conf"]
    if "abort_conf:true" not in conf or "confirm_conf:true" not in conf:
        raise GateError("screen nomination lacks fail-closed jemalloc diagnostics")

    build = load_json(canonical["build"])
    binary_hashes = build.get("binary_sha256") if isinstance(build, dict) else None
    if not isinstance(binary_hashes, dict):
        raise GateError("screen build provenance lacks binary hashes")
    actual_binaries = {
        "system": sha256_file(canonical["system"]),
        "jemalloc": sha256_file(canonical["stats"]),
        "query": sha256_file(canonical["query"]),
        "storage_verify": sha256_file(canonical["storage_verify"]),
    }
    if binary_hashes != actual_binaries:
        raise GateError("screen preserved binaries differ from build provenance")
    if build.get("jemalloc_stats_enabled") is not True:
        raise GateError("screen candidate binary is not stats-enabled")
    expected_no_stats = validate_plan(plan_path)["build"]["no_stats_command"]
    if build.get("later_no_stats_revalidation_command") != expected_no_stats:
        raise GateError("screen did not preserve the exact plain-jemalloc build command")

    extracted = load_json(canonical["extracted_seal"])
    if not isinstance(extracted, dict):
        raise GateError("screen extracted-source seal is malformed")
    if extracted.get("source_root") != str(canonical["build_source"]):
        raise GateError("screen extracted-source root is non-canonical")
    if extracted.get("archive_path") != str(canonical["source_archive"]):
        raise GateError("screen extracted-source archive is non-canonical")
    if extracted.get("archive_sha256") != sha256_file(canonical["source_archive"]):
        raise GateError("screen source archive changed")
    for source_name in (
        "phase5_allocator_full_gate.py",
        "phase5_allocator_full_plan.json",
        "phase5_allocator_full_run.sh",
        "test_phase5_allocator_full_gate.py",
    ):
        archived_path = canonical["build_source"] / "docs/experiments/storage_vnext" / source_name
        current_path = plan_path.parent / source_name
        regular_non_symlink(archived_path, f"archived full-gate source {source_name}")
        regular_non_symlink(current_path, f"executing full-gate source {source_name}")
        if sha256_file(archived_path) != sha256_file(current_path):
            raise GateError(f"executing full-gate source differs from screen archive: {source_name}")

    expectations = load_json(canonical["expectations"])
    if expectations.get("stop_after_messages") != 4_000_000:
        raise GateError("screen frozen Phase 1 authority is not the 4M workload")
    return {
        "schema": SCREEN_BINDING_SCHEMA,
        "screen_root": str(root),
        "selected_policy": selected,
        "selected_jemalloc_conf": conf,
        "screen_final_decision_sha256": sha256_file(canonical["final"]),
        "screen_summary_sha256": sha256_file(canonical["summary"]),
        "screen_artifact_manifest_sha256": sha256_file(canonical["artifacts"]),
        "screen_file_inventory_sha256": sha256_file(canonical["artifact_files"]),
        "screen_directory_inventory_sha256": sha256_file(
            canonical["artifact_directories"]
        ),
        "screen_final_seal_sha256": sha256_file(canonical["final_seal"]),
        "screen_artifact_count": len(entries),
        "core_controls_sha256": sha256_file(canonical["core_controls"]),
        "measurement_controls_sha256": sha256_file(canonical["measurement_controls"]),
        "screen_build_provenance_sha256": sha256_file(canonical["build"]),
        "screen_environment_sha256": sha256_file(canonical["environment"]),
        "source_seal_sha256": sha256_file(canonical["source_seal"]),
        "source_archive_sha256": sha256_file(canonical["source_archive"]),
        "extracted_source_seal_sha256": sha256_file(canonical["extracted_seal"]),
        "extracted_source_manifest_sha256": strict_sha256(
            extracted.get("file_manifest_sha256"), "$.extracted.file_manifest_sha256"
        ),
        "git_head": build.get("git_head"),
        "git_head_tree": build.get("git_head_tree"),
        "phase1_expectations_sha256": sha256_file(canonical["expectations"]),
        "phase1_gate_sha256": sha256_file(canonical["phase1_gate"]),
        "report_gate_sha256": sha256_file(canonical["report_gate"]),
        "fadvise_sha256": sha256_file(canonical["fadvise"]),
        "screen_gate_sha256": sha256_file(canonical["screen_gate"]),
        "binary_sha256": actual_binaries,
        "build_source": str(canonical["build_source"]),
        "source_archive": str(canonical["source_archive"]),
        "production_promotion_authorized": False,
    }


def check_screen_binding(binding_path: Path, plan_path: Path, *, full: bool) -> dict[str, Any]:
    binding = load_json(binding_path)
    root = Path(binding.get("screen_root", "")) if isinstance(binding, dict) else Path()
    if full:
        current = screen_binding(root, plan_path)
        if current != binding:
            raise GateError("completed screen binding changed")
        return {"status": "pass", "full_artifact_revalidation": True}
    validate_plan(plan_path)
    required = {
        "schema",
        "screen_root",
        "selected_policy",
        "selected_jemalloc_conf",
        "screen_final_decision_sha256",
        "screen_summary_sha256",
        "screen_artifact_manifest_sha256",
        "screen_file_inventory_sha256",
        "screen_directory_inventory_sha256",
        "screen_final_seal_sha256",
        "screen_artifact_count",
        "core_controls_sha256",
        "measurement_controls_sha256",
        "screen_build_provenance_sha256",
        "screen_environment_sha256",
        "source_seal_sha256",
        "source_archive_sha256",
        "extracted_source_seal_sha256",
        "extracted_source_manifest_sha256",
        "git_head",
        "git_head_tree",
        "phase1_expectations_sha256",
        "phase1_gate_sha256",
        "report_gate_sha256",
        "fadvise_sha256",
        "screen_gate_sha256",
        "binary_sha256",
        "build_source",
        "source_archive",
        "production_promotion_authorized",
    }
    binding = require_exact_keys(binding, required, "$.screen_binding")
    if binding["schema"] != SCREEN_BINDING_SCHEMA:
        raise GateError("screen binding schema mismatch")
    screen = directory_non_symlink(Path(binding["screen_root"]), "bound screen root")
    fixed = {
        "comparisons/final-screen-decision.json": binding["screen_final_decision_sha256"],
        "comparisons/screen-summary.json": binding["screen_summary_sha256"],
        "metadata/result-artifacts.sha256": binding["screen_artifact_manifest_sha256"],
        "metadata/result-artifacts.nul": binding["screen_file_inventory_sha256"],
        "metadata/result-directories.nul": binding["screen_directory_inventory_sha256"],
        "metadata/FINAL_SEAL_VALIDATED.json": binding["screen_final_seal_sha256"],
        "metadata/core-controls.json": binding["core_controls_sha256"],
        "metadata/measurement-controls.json": binding["measurement_controls_sha256"],
        "metadata/build-provenance.json": binding["screen_build_provenance_sha256"],
        "metadata/environment.txt": binding["screen_environment_sha256"],
        "metadata/source/formal-source-seal.json": binding["source_seal_sha256"],
        "metadata/source/git-head.tar": binding["source_archive_sha256"],
        "metadata/source/extracted-build-source-seal.json": binding["extracted_source_seal_sha256"],
        "metadata/harness/phase1_4m_expectations.json": binding["phase1_expectations_sha256"],
        "metadata/harness/phase1_replay_gate.py": binding["phase1_gate_sha256"],
        "metadata/harness/ab_gate.py": binding["report_gate_sha256"],
        "metadata/tools/fadvise-regular-dontneed": binding["fadvise_sha256"],
        "metadata/harness/phase5_allocator_screen_gate.py": binding["screen_gate_sha256"],
    }
    binary_paths = {
        "system": "metadata/binaries/chronoxide-ingester-system",
        "jemalloc": "metadata/binaries/chronoxide-ingester-jemalloc",
        "query": "metadata/binaries/chronoxide-query",
        "storage_verify": "metadata/binaries/chronoxide-storage-verify",
    }
    for role, relative in binary_paths.items():
        fixed[relative] = binding["binary_sha256"][role]
    for relative, expected in fixed.items():
        path = screen / relative
        regular_non_symlink(path, f"bound screen input {relative}")
        if sha256_file(path) != expected:
            raise GateError(f"bound screen input changed: {relative}")
    complete = screen / "COMPLETE"
    regular_non_symlink(complete, "bound screen COMPLETE marker")
    if (
        complete.read_bytes() != b"chronoxide/allocator-screen-complete/v1\n"
        or stat.S_IMODE(complete.stat().st_mode) != 0o444
    ):
        raise GateError("bound screen COMPLETE marker changed")
    if binding["production_promotion_authorized"] is not False:
        raise GateError("screen binding became promotional")
    return {"status": "pass", "full_artifact_revalidation": False}


def capacity_evidence(result_parent: Path, expectations_path: Path, plan_path: Path) -> dict[str, Any]:
    plan = validate_plan(plan_path)
    directory_non_symlink(result_parent, "result parent")
    expectations = load_json(expectations_path)
    corpus_bytes = strict_int(
        expectations.get("corpus", {}).get("size_bytes"),
        "$.expectations.corpus.size_bytes",
        minimum=1,
    )
    capacity = plan["capacity"]
    required = (
        corpus_bytes * capacity["retained_corpus_count"]
        + capacity["additional_headroom_bytes"]
        + capacity["build_headroom_bytes"]
    )
    stats = os.statvfs(result_parent)
    available = stats.f_bavail * stats.f_frsize
    if available < required:
        raise GateError(
            f"insufficient result-filesystem capacity: available={available}, required={required}"
        )
    return {
        "schema": CAPACITY_SCHEMA,
        "result_parent": str(result_parent.resolve(strict=True)),
        "expected_corpus_size_bytes": corpus_bytes,
        "retained_corpus_count": capacity["retained_corpus_count"],
        "additional_headroom_bytes": capacity["additional_headroom_bytes"],
        "build_headroom_bytes": capacity["build_headroom_bytes"],
        "required_available_bytes": required,
        "observed_available_bytes": available,
        "passed": True,
    }


def validate_capacity_evidence(
    path: Path, result_root: Path, expectations_path: Path, plan_path: Path
) -> dict[str, Any]:
    value = require_exact_keys(
        load_json(path),
        {
            "schema",
            "result_parent",
            "expected_corpus_size_bytes",
            "retained_corpus_count",
            "additional_headroom_bytes",
            "build_headroom_bytes",
            "required_available_bytes",
            "observed_available_bytes",
            "passed",
        },
        "$.initial_capacity",
    )
    plan = validate_plan(plan_path)
    expectations = load_json(expectations_path)
    corpus_bytes = strict_int(
        expectations.get("corpus", {}).get("size_bytes"),
        "$.expectations.corpus.size_bytes",
        minimum=1,
    )
    capacity = plan["capacity"]
    required = (
        corpus_bytes * capacity["retained_corpus_count"]
        + capacity["additional_headroom_bytes"]
        + capacity["build_headroom_bytes"]
    )
    canonical_parent = str(result_root.resolve(strict=True).parent)
    if (
        value["schema"] != CAPACITY_SCHEMA
        or value["result_parent"] != canonical_parent
        or value["expected_corpus_size_bytes"] != corpus_bytes
        or value["retained_corpus_count"] != capacity["retained_corpus_count"]
        or value["additional_headroom_bytes"] != capacity["additional_headroom_bytes"]
        or value["build_headroom_bytes"] != capacity["build_headroom_bytes"]
        or value["required_available_bytes"] != required
        or strict_int(
            value["observed_available_bytes"],
            "$.initial_capacity.observed_available_bytes",
            minimum=0,
        )
        < required
        or value["passed"] is not True
    ):
        raise GateError("initial capacity evidence differs from the frozen formula")
    return value


def observation_ordinal(stage: str, position: int) -> int:
    if stage not in EXPECTED_STAGE_SCHEDULES or not 1 <= position <= 4:
        raise GateError("run capacity stage/position is outside the frozen schedule")
    return position if stage == "stats" else 4 + position


def run_capacity_requirements(
    stage: str,
    position: int,
    expectations_path: Path,
    plan_path: Path,
    first_corpus_summary: Path | None = None,
) -> dict[str, Any]:
    plan = validate_plan(plan_path)
    expectations = load_json(expectations_path)
    corpus_bytes = strict_int(
        expectations.get("corpus", {}).get("size_bytes"),
        "$.expectations.corpus.size_bytes",
        minimum=1,
    )
    ordinal = observation_ordinal(stage, position)
    if ordinal == 1:
        if first_corpus_summary is not None:
            raise GateError("first run capacity must not claim a prior observed corpus")
        first_observed_size = None
    else:
        if first_corpus_summary is None:
            raise GateError("later run capacity requires the first sealed corpus summary")
        first = require_exact_keys(
            load_json(first_corpus_summary),
            {"schema", "file_count", "size_bytes", "manifest_sha256"},
            "$.first_corpus_summary",
        )
        if first["schema"] != CORPUS_SCHEMA:
            raise GateError("first corpus summary schema changed")
        first_observed_size = strict_int(
            first["size_bytes"], "$.first_corpus_summary.size_bytes", minimum=1
        )
        strict_int(first["file_count"], "$.first_corpus_summary.file_count", minimum=1)
        strict_sha256(first["manifest_sha256"], "$.first_corpus_summary.manifest_sha256")
    capacity_corpus_bytes = max(corpus_bytes, first_observed_size or 0)
    retained = plan["capacity"]["retained_corpus_count"]
    if retained != 8:
        raise GateError("run capacity requires exactly eight retained observations")
    remaining_including_current = retained - ordinal + 1
    remaining_after_current = retained - ordinal
    operational = plan["capacity"]["additional_headroom_bytes"]
    return {
        "observation_ordinal": ordinal,
        "expected_corpus_size_bytes": corpus_bytes,
        "first_observed_corpus_size_bytes": first_observed_size,
        "capacity_corpus_size_bytes": capacity_corpus_bytes,
        "remaining_corpora_including_current": remaining_including_current,
        "remaining_corpora_after_current": remaining_after_current,
        "operational_headroom_bytes": operational,
        "launch_required_free_bytes": (
            capacity_corpus_bytes * remaining_including_current + operational
        ),
        "guardian_minimum_free_bytes": (
            capacity_corpus_bytes * remaining_after_current + operational
        ),
    }


def run_capacity_evidence(
    filesystem: Path,
    stage: str,
    position: int,
    expectations_path: Path,
    plan_path: Path,
    first_corpus_summary: Path | None = None,
) -> dict[str, Any]:
    filesystem = directory_non_symlink(filesystem, "run-capacity filesystem").resolve(
        strict=True
    )
    required = run_capacity_requirements(
        stage, position, expectations_path, plan_path, first_corpus_summary
    )
    stats = os.statvfs(filesystem)
    available = stats.f_bavail * stats.f_frsize
    if available < required["launch_required_free_bytes"]:
        raise GateError(
            "insufficient per-launch result-filesystem capacity: "
            f"available={available}, required={required['launch_required_free_bytes']}"
        )
    return {
        "schema": RUN_CAPACITY_SCHEMA,
        "filesystem": str(filesystem),
        "stage": stage,
        "position": position,
        **required,
        "build_headroom_is_prebuild_only": True,
        "observed_available_bytes": available,
        "passed": True,
    }


def validate_run_capacity(
    path: Path,
    stage: str,
    position: int,
    expectations_path: Path,
    plan_path: Path,
    first_corpus_summary: Path | None = None,
) -> dict[str, Any]:
    value = require_exact_keys(
        load_json(path),
        {
            "schema",
            "filesystem",
            "stage",
            "position",
            "observation_ordinal",
            "expected_corpus_size_bytes",
            "first_observed_corpus_size_bytes",
            "capacity_corpus_size_bytes",
            "remaining_corpora_including_current",
            "remaining_corpora_after_current",
            "operational_headroom_bytes",
            "launch_required_free_bytes",
            "guardian_minimum_free_bytes",
            "build_headroom_is_prebuild_only",
            "observed_available_bytes",
            "passed",
        },
        "$.run_capacity",
    )
    expected = run_capacity_requirements(
        stage, position, expectations_path, plan_path, first_corpus_summary
    )
    if (
        value["schema"] != RUN_CAPACITY_SCHEMA
        or value["stage"] != stage
        or value["position"] != position
        or not isinstance(value["filesystem"], str)
        or not Path(value["filesystem"]).is_absolute()
        or any(value[key] != expected[key] for key in expected)
        or value["build_headroom_is_prebuild_only"] is not True
        or strict_int(
            value["observed_available_bytes"],
            "$.run_capacity.observed_available_bytes",
            minimum=0,
        )
        < expected["launch_required_free_bytes"]
        or value["passed"] is not True
    ):
        raise GateError("per-launch capacity evidence differs from the frozen schedule")
    return value


def validate_capture_residency(
    path: Path,
    capture_inputs_path: Path,
    *,
    maximum_resident_bytes: int | None,
) -> dict[str, Any]:
    inputs = load_json(capture_inputs_path)
    capture = inputs.get("capture") if isinstance(inputs, dict) else None
    raw_files = inputs.get("capture_files") if isinstance(inputs, dict) else None
    if not isinstance(capture, str) or not Path(capture).is_absolute() or not isinstance(
        raw_files, list
    ):
        raise GateError("frozen capture-input authority is malformed")
    expected: dict[str, int] = {}
    for index, item in enumerate(raw_files):
        if not isinstance(item, dict) or set(item) != {"name", "size_bytes", "sha256"}:
            raise GateError(f"capture-input file row {index} is malformed")
        name = item["name"]
        if not isinstance(name, str) or Path(name).name != name or name in expected:
            raise GateError("capture-input file name is unsafe or duplicated")
        expected[str(Path(capture) / name)] = strict_int(
            item["size_bytes"], f"$.capture_files[{index}].size_bytes", minimum=1
        )
    observed: dict[str, dict[str, int]] = {}
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8", errors="strict").splitlines(), start=1
    ):
        fields = line.strip().split(None, 2)
        if (
            len(fields) != 3
            or re.fullmatch(r"[0-9]+", fields[0]) is None
            or re.fullmatch(r"[0-9]+", fields[1]) is None
        ):
            raise GateError(f"capture residency row {line_number} is malformed")
        resident, size = int(fields[0]), int(fields[1])
        file_path = fields[2]
        if (
            file_path in observed
            or file_path not in expected
            or size != expected[file_path]
            or resident > size
        ):
            raise GateError("capture residency file set or size differs from frozen inputs")
        observed[file_path] = {"resident_bytes": resident, "size_bytes": size}
    if set(observed) != set(expected):
        raise GateError("capture residency evidence does not cover the exact capture file set")
    total_resident = sum(item["resident_bytes"] for item in observed.values())
    if maximum_resident_bytes is not None and total_resident > maximum_resident_bytes:
        raise GateError(
            "capture residency exceeds the pre-launch ceiling: "
            f"observed={total_resident}, maximum={maximum_resident_bytes}"
        )
    return {
        "file_count": len(observed),
        "total_size_bytes": sum(item["size_bytes"] for item in observed.values()),
        "total_resident_bytes": total_resident,
        "maximum_resident_bytes": maximum_resident_bytes,
    }


def toolchain_binding(
    screen_environment_path: Path,
    build_source: Path,
    cargo_path: Path,
    rustc_path: Path,
    rustdoc_path: Path,
) -> dict[str, Any]:
    screen_environment = regular_non_symlink(
        screen_environment_path, "screen environment authority"
    )
    build_source = directory_non_symlink(
        build_source, "screen extracted build source"
    ).resolve(strict=True)
    if build_source.stat().st_mode & 0o222:
        raise GateError("screen extracted build source is writable during toolchain binding")
    proxies = {"cargo": cargo_path, "rustc": rustc_path, "rustdoc": rustdoc_path}
    resolved: dict[str, Path] = {}
    for role, path in proxies.items():
        if not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK):
            raise GateError(f"controlled Rust {role} proxy is not an absolute executable file")
        resolved[role] = path.resolve(strict=True)
    if len(set(resolved.values())) != 1:
        raise GateError("cargo/rustc/rustdoc do not resolve to one controlled rustup proxy")
    version_outputs: dict[str, str] = {}
    for role in ("rustc", "cargo"):
        completed = subprocess.run(
            [str(proxies[role]), "--version", "--verbose"],
            env={
                "HOME": str(Path.home()),
                "PATH": f"{Path.home()}/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "CARGO_HOME": f"{Path.home()}/.cargo",
                "RUSTUP_HOME": f"{Path.home()}/.rustup",
                "LC_ALL": "C",
                "TZ": "UTC",
            },
            text=True,
            cwd=build_source,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0 or completed.stderr:
            raise GateError(f"controlled {role} version probe failed: {completed.stderr.strip()}")
        version_outputs[role] = completed.stdout.rstrip("\n")
        if not version_outputs[role]:
            raise GateError(f"controlled {role} version probe returned no output")
    expected_block = (
        version_outputs["rustc"] + "\n" + version_outputs["cargo"] + "\n"
    )
    screen_text = screen_environment.read_text(encoding="utf-8", errors="strict")
    if screen_text.count(expected_block) != 1:
        raise GateError("current Rust toolchain versions differ from the screen-time authority")
    proxy = next(iter(resolved.values()))
    return {
        "schema": TOOLCHAIN_SCHEMA,
        "screen_environment_path": str(screen_environment.resolve(strict=True)),
        "screen_environment_sha256": sha256_file(screen_environment),
        "build_source": str(build_source),
        "proxy_paths": {role: str(path) for role, path in proxies.items()},
        "resolved_proxy_path": str(proxy),
        "resolved_proxy_sha256": sha256_file(proxy),
        "rustc_version_verbose": version_outputs["rustc"],
        "cargo_version_verbose": version_outputs["cargo"],
        "matches_screen_time_versions": True,
    }


def requested_effective_entries(conf: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for entry in conf.split(","):
        key, raw = entry.split(":", 1)
        if key not in EFFECTIVE_POLICY_KEYS or key in result:
            raise GateError("nominated jemalloc policy contains an unknown or duplicate key")
        if raw in {"true", "false"}:
            result[key] = raw == "true"
        elif re.fullmatch(r"-?[0-9]+", raw):
            result[key] = int(raw)
        else:
            raise GateError("nominated jemalloc policy contains a non-canonical value")
    return result


def validate_effective_policy(value: Any, conf: str, context: str) -> dict[str, Any]:
    effective = require_exact_keys(value, EFFECTIVE_POLICY_KEYS, context)
    for key in ("abort_conf", "confirm_conf", "background_thread", "retain"):
        if type(effective[key]) is not bool:
            raise GateError(f"{context}.{key} must be boolean")
    for key in ("narenas", "dirty_decay_ms", "muzzy_decay_ms", "max_background_threads"):
        if type(effective[key]) is not int:
            raise GateError(f"{context}.{key} must be integer")
    if effective["narenas"] < 1 or effective["max_background_threads"] < 1:
        raise GateError(f"{context} arena/thread counts must be positive")
    for key, expected in requested_effective_entries(conf).items():
        if type(effective[key]) is not type(expected) or effective[key] != expected:
            raise GateError(
                f"{context}.{key} differs from the nominated policy: "
                f"expected={expected!r}, actual={effective[key]!r}"
            )
    return effective


def unavailable_allocator_probe(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "allocation_bytes": None,
        "minimum_allocated_growth_bytes": None,
        "allocated_before_bytes": None,
        "allocated_while_live_bytes": None,
        "allocated_after_drop_bytes": None,
        "observed_allocated_growth_bytes": None,
        "passed": None,
    }


def validate_application_preflight(
    raw_path: Path,
    stderr_path: Path,
    role: str,
    binary_path: Path,
    screen_binding_path: Path,
) -> dict[str, Any]:
    if role not in {"system", "stats-candidate", "no-stats-candidate"}:
        raise GateError(f"unknown preflight role: {role}")
    binding = load_json(screen_binding_path)
    value = load_json(raw_path)
    expected_keys = {
        "schema",
        "rust_global_allocator",
        "jemalloc_conf_env",
        "requested_policy_raw",
        "requested_policy_canonical",
        "effective_policy",
        "global_allocator_probe",
        "allocator_internal_telemetry",
        "ld_preload_present",
        "malloc_conf_present",
        "post_ingester_drop_hold_secs",
        "post_ingester_drop_checkpoint_enabled",
        "post_ingester_drop_telemetry_enabled",
    }
    value = require_exact_keys(value, expected_keys, "$.application_preflight")
    if value["schema"] != APPLICATION_PREFLIGHT_SCHEMA:
        raise GateError("application preflight schema mismatch")
    if value["jemalloc_conf_env"] != "_RJEM_MALLOC_CONF":
        raise GateError("application preflight names the wrong jemalloc environment")
    if value["ld_preload_present"] is not False or value["malloc_conf_present"] is not False:
        raise GateError("application preflight observed an allocator interposer")
    if (
        value["post_ingester_drop_hold_secs"] != 0
        or value["post_ingester_drop_checkpoint_enabled"] is not False
        or value["post_ingester_drop_telemetry_enabled"] is not False
    ):
        raise GateError("preflight unexpectedly enabled the measured release hold")
    probe = require_exact_keys(
        value["global_allocator_probe"],
        {
            "status",
            "allocation_bytes",
            "minimum_allocated_growth_bytes",
            "allocated_before_bytes",
            "allocated_while_live_bytes",
            "allocated_after_drop_bytes",
            "observed_allocated_growth_bytes",
            "passed",
        },
        "$.application_preflight.global_allocator_probe",
    )
    if role == "system":
        if value["rust_global_allocator"] != "system":
            raise GateError("system preflight uses the wrong Rust allocator")
        if any(value[key] is not None for key in ("requested_policy_raw", "requested_policy_canonical", "effective_policy")):
            raise GateError("system preflight unexpectedly reports jemalloc policy")
        if value["allocator_internal_telemetry"] != "unavailable":
            raise GateError("system preflight invented allocator telemetry")
        if probe != unavailable_allocator_probe("unavailable_for_system_allocator"):
            raise GateError("system preflight allocator probe must be explicit unavailable")
        expected_hash = binding["binary_sha256"]["system"]
    elif role == "stats-candidate":
        if value["rust_global_allocator"] != "jemalloc":
            raise GateError("stats candidate preflight uses the wrong Rust allocator")
        if value["requested_policy_raw"] != binding["selected_jemalloc_conf"]:
            raise GateError("stats candidate preflight policy differs from screen nomination")
        if value["requested_policy_canonical"] != binding["selected_jemalloc_conf"]:
            raise GateError("stats candidate canonical policy differs from screen nomination")
        if value["allocator_internal_telemetry"] != "fixed_startup_options_and_release_stats":
            raise GateError("stats candidate preflight lacks allocator telemetry")
        if probe["status"] != "passed" or probe["passed"] is not True:
            raise GateError("stats candidate live global-allocation probe did not pass")
        for key in (
            "allocation_bytes",
            "minimum_allocated_growth_bytes",
            "allocated_before_bytes",
            "allocated_while_live_bytes",
            "allocated_after_drop_bytes",
            "observed_allocated_growth_bytes",
        ):
            strict_int(
                probe[key],
                f"$.application_preflight.global_allocator_probe.{key}",
                minimum=0,
            )
        if probe["allocation_bytes"] != 64 * 1024 * 1024:
            raise GateError("stats candidate allocation probe size changed")
        if probe["minimum_allocated_growth_bytes"] != 48 * 1024 * 1024:
            raise GateError("stats candidate allocation probe minimum growth changed")
        if (
            probe["allocated_while_live_bytes"] - probe["allocated_before_bytes"]
            != probe["observed_allocated_growth_bytes"]
        ):
            raise GateError("stats candidate allocation probe growth is not derived")
        if probe["observed_allocated_growth_bytes"] < probe["minimum_allocated_growth_bytes"]:
            raise GateError("stats candidate allocation probe growth is below its minimum")
        validate_effective_policy(
            value["effective_policy"],
            binding["selected_jemalloc_conf"],
            "$.application_preflight.effective_policy",
        )
        expected_hash = binding["binary_sha256"]["jemalloc"]
    else:
        if value["rust_global_allocator"] != "jemalloc":
            raise GateError("plain candidate preflight uses the wrong Rust allocator")
        if any(value[key] is not None for key in ("requested_policy_raw", "requested_policy_canonical", "effective_policy")):
            raise GateError("plain candidate compiled stats-enabled policy diagnostics")
        if value["allocator_internal_telemetry"] != "unavailable":
            raise GateError("plain candidate unexpectedly compiled allocator telemetry")
        if probe != unavailable_allocator_probe("unavailable_without_jemalloc_stats"):
            raise GateError("plain candidate allocator probe must be explicit no-stats unavailable")
        expected_hash = sha256_file(binary_path)
    stderr = stderr_path.read_text(encoding="utf-8", errors="strict")
    if role == "system":
        if "<jemalloc>:" in stderr:
            raise GateError("system preflight contains jemalloc confirmation output")
    else:
        validate_jemalloc_sources(stderr, binding["selected_jemalloc_conf"])
    actual_hash = sha256_file(binary_path)
    if actual_hash != expected_hash:
        raise GateError(f"{role} preflight binary hash changed")
    return {
        "schema": PREFLIGHT_SCHEMA,
        "role": role,
        "binary_sha256": actual_hash,
        "application_sha256": sha256_file(raw_path),
        "stderr_sha256": sha256_file(stderr_path),
        "rust_global_allocator": value["rust_global_allocator"],
        "jemalloc_stats_enabled": role == "stats-candidate",
        "selected_policy": binding["selected_policy"] if role != "system" else None,
        "selected_jemalloc_conf": binding["selected_jemalloc_conf"],
        "applied_jemalloc_conf": binding["selected_jemalloc_conf"] if role != "system" else None,
        "effective_policy": value["effective_policy"],
        "global_allocator_probe": probe,
        "production_promotion_authorized": False,
    }


def no_stats_build_provenance(
    screen_binding_path: Path,
    plan_path: Path,
    build_log_path: Path,
    binary_path: Path,
    preflight_path: Path,
    target_dir: Path,
    toolchain_path: Path,
    post_build_screen_validation_path: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path)
    binding = load_json(screen_binding_path)
    log_lines = build_log_path.read_text(encoding="utf-8", errors="strict").splitlines()
    command = plan["build"]["no_stats_command"]
    source_root = binding["build_source"]
    if not log_lines or log_lines[0] != f"COMMAND\t{command}":
        raise GateError("plain-jemalloc build log does not bind the exact command")
    if len(log_lines) < 2 or log_lines[1] != f"CWD\t{source_root}":
        raise GateError("plain-jemalloc build did not use the screen's extracted source")
    home = Path.home()
    build_path = (
        f"{home}/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    )
    expected_environment_line = (
        f"ENV\tHOME={home}\tPATH={build_path}\tCARGO_HOME={home}/.cargo\t"
        f"RUSTUP_HOME={home}/.rustup\tRUSTC={home}/.cargo/bin/rustc\t"
        f"RUSTDOC={home}/.cargo/bin/rustdoc\tLC_ALL=C\tTZ=UTC\t"
        f"CARGO_INCREMENTAL=0\tCARGO_TARGET_DIR={target_dir}"
    )
    if len(log_lines) < 3 or log_lines[2] != expected_environment_line:
        raise GateError("plain-jemalloc build environment differs from the frozen contract")
    toolchain = load_json(toolchain_path)
    proxy_paths = toolchain.get("proxy_paths") if isinstance(toolchain, dict) else None
    if not isinstance(proxy_paths, dict) or set(proxy_paths) != {"cargo", "rustc", "rustdoc"}:
        raise GateError("plain-jemalloc build lacks its exact toolchain binding")
    expected_proxy_paths = {
        role: f"{home}/.cargo/bin/{role}" for role in ("cargo", "rustc", "rustdoc")
    }
    if proxy_paths != expected_proxy_paths:
        raise GateError("plain-jemalloc build toolchain uses non-canonical proxy paths")
    current_toolchain = toolchain_binding(
        Path(binding["screen_root"]) / "metadata/environment.txt",
        Path(binding["build_source"]),
        Path(proxy_paths["cargo"]),
        Path(proxy_paths["rustc"]),
        Path(proxy_paths["rustdoc"]),
    )
    if toolchain != current_toolchain:
        raise GateError("plain-jemalloc build toolchain differs from the screen-time binding")
    post_build = load_json(post_build_screen_validation_path)
    if post_build != {"status": "pass", "full_artifact_revalidation": True}:
        raise GateError("plain-jemalloc build lacks immediate post-build screen revalidation")
    preflight = load_json(preflight_path)
    if preflight.get("role") != "no-stats-candidate":
        raise GateError("plain-jemalloc build lacks its no-stats preflight")
    binary_hash = sha256_file(binary_path)
    if preflight.get("binary_sha256") != binary_hash:
        raise GateError("plain-jemalloc preflight used another binary")
    if binary_hash in binding["binary_sha256"].values():
        raise GateError("plain-jemalloc binary aliases a screen comparator binary")
    source = Path(source_root)
    if source.resolve(strict=True) != Path(binding["screen_root"]) / "build-source":
        raise GateError("plain-jemalloc build source is non-canonical")
    if source.stat().st_mode & 0o222:
        raise GateError("screen extracted build source became writable")
    return {
        "schema": BUILD_SCHEMA,
        "screen_binding_sha256": sha256_file(screen_binding_path),
        "git_head": binding["git_head"],
        "git_head_tree": binding["git_head_tree"],
        "source_archive_sha256": binding["source_archive_sha256"],
        "extracted_source_seal_sha256": binding["extracted_source_seal_sha256"],
        "extracted_source_manifest_sha256": binding["extracted_source_manifest_sha256"],
        "source_root": source_root,
        "source_root_non_writable": True,
        "build_command": command,
        "build_log_sha256": sha256_file(build_log_path),
        "toolchain_binding_sha256": sha256_file(toolchain_path),
        "post_build_screen_validation_sha256": sha256_file(
            post_build_screen_validation_path
        ),
        "cargo_locked": True,
        "cargo_incremental": False,
        "features": ["jemalloc"],
        "jemalloc_stats_enabled": False,
        "target_dir": str(target_dir),
        "binary_sha256": binary_hash,
        "preflight_sha256": sha256_file(preflight_path),
        "production_promotion_authorized": False,
    }


def read_process_stat_identity(pid: int) -> dict[str, int | str] | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    close = raw.rfind(")")
    if close < 0:
        return None
    fields = raw[close + 2 :].split()
    try:
        state = fields[0]
        ppid = int(fields[1])
        starttime_ticks = int(fields[19])
    except (IndexError, ValueError):
        return None
    if len(state) != 1 or starttime_ticks < 1 or ppid < 0:
        return None
    return {
        "pid": pid,
        "ppid": ppid,
        "state": state,
        "starttime_ticks": starttime_ticks,
    }


def process_identity_is_running(identity: dict[str, int | str] | None) -> bool:
    return identity is not None and identity["state"] not in {"Z", "X", "x"}


def process_is_same_running(pid: int, starttime_ticks: int) -> bool:
    current = read_process_stat_identity(pid)
    return bool(
        process_identity_is_running(current)
        and current is not None
        and current["starttime_ticks"] == starttime_ticks
    )


def process_is_same_identity(pid: int, starttime_ticks: int) -> bool:
    current = read_process_stat_identity(pid)
    return bool(
        current is not None and current["starttime_ticks"] == starttime_ticks
    )


def read_process_children(pid: int) -> list[int]:
    try:
        return [
            int(child)
            for child in Path(f"/proc/{pid}/task/{pid}/children")
            .read_text(encoding="ascii")
            .split()
        ]
    except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
        return []


def process_tree_identity_bindings(
    root_pid: int, root_starttime_ticks: int | None = None
) -> dict[int, dict[str, int | str]]:
    if root_starttime_ticks is not None and not process_is_same_running(
        root_pid, root_starttime_ticks
    ):
        return {}
    pending: list[tuple[int, int | None, int | None]] = [
        (root_pid, None, root_starttime_ticks)
    ]
    observed: dict[int, dict[str, int | str]] = {}
    while pending:
        pid, expected_parent, expected_starttime_ticks = pending.pop()
        identity = read_process_stat_identity(pid)
        if (
            pid in observed
            or identity is None
            or (
                pid == root_pid
                and not process_identity_is_running(identity)
            )
            or (
                expected_parent is not None
                and identity["ppid"] != expected_parent
            )
            or (
                expected_starttime_ticks is not None
                and identity["starttime_ticks"] != expected_starttime_ticks
            )
        ):
            continue
        observed[pid] = identity
        # A measured descendant can remain under its live wrapper as a zombie
        # until that wrapper reaps it. Bind its PID, parent, and start time so
        # the guardian can distinguish it from a reused or reparented PID.
        # Only live descendants can still own children.
        if process_identity_is_running(identity):
            pending.extend((child, pid, None) for child in read_process_children(pid))
    return observed


def process_tree(root_pid: int, root_starttime_ticks: int | None = None) -> set[int]:
    return {
        pid
        for pid, identity in process_tree_identity_bindings(
            root_pid, root_starttime_ticks
        ).items()
        if process_identity_is_running(identity)
    }


def process_matches_identity_binding(
    identity: dict[str, int | str] | None,
    binding: dict[str, int | str],
) -> bool:
    return bool(
        identity is not None
        and identity["starttime_ticks"] == binding["starttime_ticks"]
        and identity["ppid"] == binding["ppid"]
    )


def process_binding_chain_is_current(
    pid: int,
    root_pid: int,
    identity_bindings: dict[int, dict[str, int | str]],
) -> bool:
    current_pid = pid
    visited: set[int] = set()
    while current_pid not in visited:
        visited.add(current_pid)
        binding = identity_bindings.get(current_pid)
        if binding is None or not process_matches_identity_binding(
            read_process_stat_identity(current_pid), binding
        ):
            return False
        if current_pid == root_pid:
            return True
        current_pid = int(binding["ppid"])
    return False


def process_identity(pid: int) -> tuple[str, str] | None:
    try:
        comm = Path(f"/proc/{pid}/comm").read_text(encoding="utf-8").strip()
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    return comm, raw.replace(b"\0", b" ").decode("utf-8", errors="replace").strip()


COMPILER_PROCESS_TOKEN = (
    r"(?:gcc|g\+\+|cc|c\+\+|clang|clang\+\+)"
    r"(?:[.-](?:real|[0-9][A-Za-z0-9_.-]*))*"
)
LINKER_PROCESS_TOKEN = (
    r"(?:ld(?:\.(?:lld|bfd|gold))?|lld|mold)"
    r"(?:-[0-9][A-Za-z0-9_.-]*)?"
)
NINJA_PROCESS_TOKEN = r"ninja(?:\.real|-[0-9][A-Za-z0-9_.-]*)?"
SOONG_PROCESS_TOKEN = r"(?:soong_ui|soong_build)(?:\.bash)?"
CONTAINER_CLIENT_PROCESS_TOKEN = (
    r"(?:docker|docker-buildx|docker-compose|buildctl|nerdctl|podman|buildah)"
)
FORBIDDEN_MEASUREMENT_COMM = re.compile(
    rf"^(?:cargo|cargo-nextest|rustc|rustdoc|clippy-driver|nextest|make|"
    rf"{NINJA_PROCESS_TOKEN}|cmake|meson|sccache|ccache|"
    rf"{CONTAINER_CLIENT_PROCESS_TOKEN}|emulator|adb|gradle|gradlew|"
    rf"GradleDaemon|{COMPILER_PROCESS_TOKEN}|"
    rf"cc1|cc1plus|{LINKER_PROCESS_TOKEN}|perf|heaptrack|valgrind.*|strace|"
    rf"ltrace|bpftrace|hotspot|qemu-system.*|qemu-kvm|chronoxide-.*|"
    rf"greptime.*|clickhouse.*|postgres.*|mysqld|influxd|victoria.*|"
    rf"vm(?:storage|select|agent)|mimir.*|thanos.*|cortex.*|prometheus|"
    rf"{SOONG_PROCESS_TOKEN}|ckati|kati|javac|kotlinc|metalava|aapt|aapt2|"
    rf"aidl|dex2oat|btop|htop|top)$",
    re.IGNORECASE,
)
FORBIDDEN_MEASUREMENT_COMMAND = re.compile(
    rf"(?:^|[/ ])(?:cargo(?:-nextest)?|rustc|rustdoc|clippy-driver|nextest|"
    rf"{NINJA_PROCESS_TOKEN}|{COMPILER_PROCESS_TOKEN}|{LINKER_PROCESS_TOKEN}|"
    rf"{SOONG_PROCESS_TOKEN}|ckati|kati|gradlew?|metalava|aapt2?|aidl|"
    rf"dex2oat)(?:$|[ /])|org\.gradle\.|GradleDaemon|GradleWorkerMain|"
    rf"gradle-worker|Android[/ ](?:SDK )?emulator",
    re.IGNORECASE,
)


def forbidden_process_reason(comm: str, command: str) -> str | None:
    name = Path(comm).name
    if FORBIDDEN_MEASUREMENT_COMM.fullmatch(name):
        return f"forbidden measurement process {name}"
    if FORBIDDEN_MEASUREMENT_COMMAND.search(command):
        return "forbidden build/emulator command"
    return None


def ancestor_pids(pid: int) -> set[int]:
    result: set[int] = set()
    current = pid
    while current > 1 and current not in result:
        result.add(current)
        try:
            raw = Path(f"/proc/{current}/stat").read_text(encoding="ascii")
            end = raw.rfind(")")
            fields = raw[end + 1 :].split()
            current = int(fields[1])
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError, IndexError):
            break
    return result


def scan_conflicts(
    *,
    allowed_root_pid: int | None = None,
    allowed_root_starttime_ticks: int | None = None,
) -> list[dict[str, Any]]:
    excluded = ancestor_pids(os.getpid())
    identity_bindings: dict[int, dict[str, int | str]] = {}
    if allowed_root_pid is not None:
        identity_bindings = process_tree_identity_bindings(
            allowed_root_pid, allowed_root_starttime_ticks
        )
    conflicts = []
    for raw_pid in sorted(Path("/proc").iterdir(), key=lambda path: path.name):
        if not raw_pid.name.isdigit():
            continue
        pid = int(raw_pid.name)
        if pid in excluded:
            continue
        current_stat: dict[str, int | str] | None = None
        binding = identity_bindings.get(pid)
        if binding is not None and allowed_root_pid is not None and (
            process_binding_chain_is_current(
                pid, allowed_root_pid, identity_bindings
            )
        ):
            continue
        if binding is not None:
            current_stat = read_process_stat_identity(pid)
        if (
            allowed_root_pid is not None
            and allowed_root_starttime_ticks is not None
            and pid == allowed_root_pid
            and process_is_same_identity(pid, allowed_root_starttime_ticks)
        ):
            continue
        identity = process_identity(pid)
        if identity is None:
            continue
        comm, command = identity
        reason = forbidden_process_reason(comm, command)
        if reason is not None:
            if current_stat is None:
                current_stat = read_process_stat_identity(pid)
            conflicts.append(
                {
                    "pid": pid,
                    "ppid": (
                        current_stat["ppid"] if current_stat is not None else None
                    ),
                    "state": (
                        current_stat["state"] if current_stat is not None else None
                    ),
                    "starttime_ticks": (
                        current_stat["starttime_ticks"]
                        if current_stat is not None
                        else None
                    ),
                    "comm": comm,
                    "command": command,
                    "reason": reason,
                }
            )
    return conflicts


def validate_conflict_scan(path: Path) -> dict[str, Any]:
    value = require_exact_keys(
        load_json(path), {"schema", "conflicts", "quiet"}, "$.conflict_scan"
    )
    if (
        value["schema"] != CONFLICT_SCAN_SCHEMA
        or value["conflicts"] != []
        or value["quiet"] is not True
    ):
        raise GateError("static quiet-host conflict scan did not pass exactly")
    return value


def validate_zero_exit_status(path: Path, description: str) -> None:
    status = regular_non_symlink(path, description)
    if status.read_bytes() != b"0\n":
        raise GateError(f"{description} is not exact successful status 0")


def guardian_maximum_allowed_gap_ns(interval_ms: int) -> int:
    if interval_ms < 1:
        raise GateError("guardian cadence interval must be positive")
    return interval_ms * 1_000_000 + GUARDIAN_CADENCE_EDGE_ALLOWANCE_NS


def derive_guardian_maximum_poll_start_gap_ns(
    timestamps: list[int], elapsed_ns: int
) -> int:
    boundaries = [0, *timestamps, elapsed_ns]
    return max(
        (later - earlier for earlier, later in zip(boundaries, boundaries[1:])),
        default=0,
    )


def require_running_process_identity(
    pid: int, description: str
) -> dict[str, int | str]:
    identity = read_process_stat_identity(pid)
    if not process_identity_is_running(identity):
        raise GateError(f"{description} is absent, zombie, or exited")
    assert identity is not None
    return identity


def create_empty_read_only_marker(path: Path, description: str) -> None:
    if not path.is_absolute():
        raise GateError(f"{description} path must be absolute")
    directory_non_symlink(path.parent, f"{description} parent")
    if path.exists() or path.is_symlink():
        raise GateError(f"refusing to reuse {description}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o444)
    os.close(descriptor)
    marker = regular_non_symlink(path, description)
    if marker.stat().st_size != 0 or stat.S_IMODE(marker.stat().st_mode) != 0o444:
        raise GateError(f"{description} is not exact empty mode 0444")


def validate_empty_read_only_marker(path: Path, description: str) -> Path:
    marker = regular_non_symlink(path, description)
    if marker.stat().st_size != 0 or stat.S_IMODE(marker.stat().st_mode) != 0o444:
        raise GateError(f"{description} must be exact empty mode 0444")
    return marker


def validate_guardian_control(
    path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    *,
    expected_root_pid: int | None = None,
    expected_guardian_pid: int | None = None,
    require_live: bool = False,
) -> dict[str, Any]:
    control_path = regular_non_symlink(path, "guardian launch control")
    if stat.S_IMODE(control_path.stat().st_mode) != 0o444:
        raise GateError("guardian launch control must have exact mode 0444")
    value = require_exact_keys(
        load_json(control_path),
        {
            "schema",
            "root_pid",
            "root_starttime_ticks",
            "guardian_pid",
            "guardian_starttime_ticks",
            "rss_monitor_pid",
            "rss_monitor_starttime_ticks",
            "rss_ready_marker",
            "interval_ms",
            "ready_marker",
            "launch_marker",
        },
        "$.guardian_control",
    )
    pids = {
        name: strict_int(value[name], f"$.guardian_control.{name}", minimum=1)
        for name in ("root_pid", "guardian_pid", "rss_monitor_pid")
    }
    if len(set(pids.values())) != 3:
        raise GateError("guardian launch control PIDs must be distinct")
    starttimes = {
        name: strict_int(
            value[f"{name}_starttime_ticks"],
            f"$.guardian_control.{name}_starttime_ticks",
            minimum=1,
        )
        for name in ("root", "guardian", "rss_monitor")
    }
    if (
        value["schema"] != GUARDIAN_CONTROL_SCHEMA
        or value["interval_ms"] != interval_ms
        or value["ready_marker"] != str(ready_path)
        or value["launch_marker"] != str(launch_path)
        or not ready_path.is_absolute()
        or not launch_path.is_absolute()
        or ready_path.parent != control_path.parent
        or launch_path.parent != control_path.parent
        or not isinstance(value["rss_ready_marker"], str)
        or not Path(value["rss_ready_marker"]).is_absolute()
        or Path(value["rss_ready_marker"]).parent != control_path.parent
        or expected_root_pid is not None
        and pids["root_pid"] != expected_root_pid
        or expected_guardian_pid is not None
        and pids["guardian_pid"] != expected_guardian_pid
    ):
        raise GateError("guardian launch control differs from the exact handshake")
    if require_live:
        dead = [
            name
            for name, pid in pids.items()
            if not process_is_same_running(pid, starttimes[name.removesuffix("_pid")])
        ]
        if dead:
            raise GateError(
                "guardian launch control has exited, zombie, or reused processes: "
                f"{dead!r}"
            )
    return value


def create_guardian_control(
    output: Path,
    ready_path: Path,
    launch_path: Path,
    root_pid: int,
    guardian_pid: int,
    rss_monitor_pid: int,
    interval_ms: int,
    rss_ready_path: Path,
) -> dict[str, Any]:
    if not rss_ready_path.is_absolute() or rss_ready_path.parent != output.parent:
        raise GateError("RSS ready marker must be absolute and beside guardian control")
    for marker, description in (
        (ready_path, "guardian ready marker"),
        (launch_path, "guardian launch marker"),
        (rss_ready_path, "RSS ready marker"),
    ):
        if marker.exists() or marker.is_symlink():
            raise GateError(f"refusing to reuse {description}")
    identities = {
        "root": require_running_process_identity(root_pid, "held measured root"),
        "guardian": require_running_process_identity(guardian_pid, "guardian"),
        "rss_monitor": require_running_process_identity(rss_monitor_pid, "RSS monitor"),
    }
    value = {
        "schema": GUARDIAN_CONTROL_SCHEMA,
        "root_pid": root_pid,
        "root_starttime_ticks": identities["root"]["starttime_ticks"],
        "guardian_pid": guardian_pid,
        "guardian_starttime_ticks": identities["guardian"]["starttime_ticks"],
        "rss_monitor_pid": rss_monitor_pid,
        "rss_monitor_starttime_ticks": identities["rss_monitor"]["starttime_ticks"],
        "rss_ready_marker": str(rss_ready_path),
        "interval_ms": interval_ms,
        "ready_marker": str(ready_path),
        "launch_marker": str(launch_path),
    }
    publish_json_read_only_atomic_exclusive(output, value)
    current = validate_guardian_control(
        output,
        ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=root_pid,
        expected_guardian_pid=guardian_pid,
        require_live=True,
    )
    if current != value:
        raise GateError("fresh guardian launch control failed self-validation")
    return value


def release_guardian_launch(
    control_path: Path, ready_path: Path, launch_path: Path, interval_ms: int
) -> dict[str, Any]:
    control = validate_guardian_control(
        control_path,
        ready_path,
        launch_path,
        interval_ms,
        require_live=True,
    )
    validate_empty_read_only_marker(ready_path, "guardian ready marker")
    create_empty_read_only_marker(launch_path, "guardian launch marker")
    return {
        "status": "released",
        "root_pid": control["root_pid"],
        "guardian_pid": control["guardian_pid"],
        "rss_monitor_pid": control["rss_monitor_pid"],
    }


def wait_for_guardian_ready(
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    timeout_ms: int,
) -> dict[str, Any]:
    if timeout_ms < interval_ms:
        raise GateError("guardian readiness timeout is shorter than one poll")
    deadline = time.monotonic() + timeout_ms / 1000
    while True:
        control = validate_guardian_control(
            control_path,
            ready_path,
            launch_path,
            interval_ms,
            require_live=True,
        )
        if launch_path.exists() or launch_path.is_symlink():
            raise GateError("guardian launch marker appeared before readiness")
        if ready_path.exists() or ready_path.is_symlink():
            validate_empty_read_only_marker(ready_path, "guardian ready marker")
            return {
                "status": "ready",
                "root_pid": control["root_pid"],
                "root_starttime_ticks": control["root_starttime_ticks"],
                "guardian_pid": control["guardian_pid"],
                "rss_monitor_pid": control["rss_monitor_pid"],
            }
        if time.monotonic() >= deadline:
            raise GateError("guardian did not become ready before the bounded timeout")
        time.sleep(0.01)


def snapshot_process_tree_identities(
    root_pid: int, root_starttime_ticks: int
) -> list[dict[str, int | str]]:
    identities: dict[int, dict[str, int | str]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        identity = read_process_stat_identity(int(entry.name))
        if identity is not None:
            identities[int(entry.name)] = identity
    root = identities.get(root_pid)
    if (
        not process_identity_is_running(root)
        or root is None
        or root["starttime_ticks"] != root_starttime_ticks
    ):
        return []
    depths = {root_pid: 0}
    changed = True
    while changed:
        changed = False
        for pid, identity in identities.items():
            parent = int(identity["ppid"])
            if parent in depths and pid not in depths:
                depths[pid] = depths[parent] + 1
                changed = True
    result = []
    for pid, depth in depths.items():
        identity = identities[pid]
        result.append({**identity, "depth": depth})
    return sorted(
        result,
        key=lambda value: (int(value["depth"]), int(value["pid"])),
        reverse=True,
    )


def process_identity_refusal(target: dict[str, int | str]) -> str | None:
    current = read_process_stat_identity(int(target["pid"]))
    if current is None:
        return "exited"
    if current["starttime_ticks"] != target["starttime_ticks"]:
        return "starttime_mismatch"
    if current["state"] in {"Z", "X", "x"}:
        return f"state_{current['state']}"
    return None


def wait_for_process_identities_exit(
    targets: list[dict[str, int | str]], timeout_seconds: float
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline and any(
        process_is_same_running(
            int(target["pid"]), int(target["starttime_ticks"])
        )
        for target in targets
    ):
        time.sleep(0.01)


def terminate_process_tree(
    root_pid: int, root_starttime_ticks: int
) -> dict[str, Any]:
    targets = [
        target
        for target in snapshot_process_tree_identities(root_pid, root_starttime_ticks)
        if target["pid"] != os.getpid()
    ]
    pids = [int(target["pid"]) for target in targets]
    term_sent: list[int] = []
    term_errors: list[dict[str, Any]] = []
    identity_refusals: list[dict[str, Any]] = []
    for target in targets:
        pid = int(target["pid"])
        refusal = process_identity_refusal(target)
        if refusal is not None:
            if refusal != "exited":
                identity_refusals.append(
                    {"pid": pid, "signal": "TERM", "reason": refusal}
                )
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            term_sent.append(pid)
        except ProcessLookupError:
            continue
        except PermissionError as error:
            term_errors.append({"pid": pid, "signal": "TERM", "error": str(error)})
    wait_for_process_identities_exit(targets, 0.5)
    kill_sent: list[int] = []
    kill_errors: list[dict[str, Any]] = []
    for target in targets:
        pid = int(target["pid"])
        refusal = process_identity_refusal(target)
        if refusal is not None:
            if refusal not in {"exited", "state_Z", "state_X", "state_x"}:
                identity_refusals.append(
                    {"pid": pid, "signal": "KILL", "reason": refusal}
                )
            continue
        try:
            os.kill(pid, signal.SIGKILL)
            kill_sent.append(pid)
        except ProcessLookupError:
            continue
        except PermissionError as error:
            kill_errors.append({"pid": pid, "signal": "KILL", "error": str(error)})
    wait_for_process_identities_exit(targets, 0.5)
    survivors = [
        int(target["pid"])
        for target in targets
        if process_is_same_running(
            int(target["pid"]), int(target["starttime_ticks"])
        )
    ]
    return {
        "attempted": True,
        "root_starttime_ticks": root_starttime_ticks,
        "target_processes": targets,
        "target_pids": pids,
        "term_sent_pids": term_sent,
        "term_errors": term_errors,
        "kill_sent_pids": kill_sent,
        "kill_errors": kill_errors,
        "identity_refusals": identity_refusals,
        "surviving_pids": survivors,
    }


def require_clean_termination(
    evidence: dict[str, Any], description: str
) -> dict[str, Any]:
    failures = {
        key: evidence.get(key, [])
        for key in ("term_errors", "kill_errors", "surviving_pids")
        if evidence.get(key)
    }
    if failures:
        raise GateError(f"{description} cleanup was incomplete: {failures!r}")
    return evidence


def cleanup_guardian_processes(
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> dict[str, Any]:
    """Stop the measured tree first, then its two monitors, using sealed identities."""
    control = validate_guardian_control(
        control_path,
        ready_path,
        launch_path,
        interval_ms,
        require_live=False,
    )
    terminations: dict[str, dict[str, Any]] = {}
    for role in ("root", "rss_monitor", "guardian"):
        terminations[role] = terminate_process_tree(
            control[f"{role}_pid"], control[f"{role}_starttime_ticks"]
        )
    result = {
        "schema": GUARDIAN_CLEANUP_SCHEMA,
        "control_path": str(control_path),
        "control_sha256": sha256_file(control_path),
        "termination_order": ["root", "rss_monitor", "guardian"],
        "terminations": terminations,
    }
    incomplete = {
        role: {
            key: evidence.get(key, [])
            for key in ("term_errors", "kill_errors", "surviving_pids")
            if evidence.get(key)
        }
        for role, evidence in terminations.items()
    }
    incomplete = {role: failures for role, failures in incomplete.items() if failures}
    if incomplete:
        raise GateError(f"guardian-controlled cleanup was incomplete: {incomplete!r}")
    return result


def monitor_conflicts(
    root_pid: int,
    output: Path,
    interval_ms: int,
    filesystem: Path,
    minimum_free_bytes: int,
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
) -> dict[str, Any]:
    if root_pid <= 0 or interval_ms < 10:
        raise GateError("invalid continuous guardian arguments")
    if output.exists() or output.is_symlink():
        raise GateError("continuous guardian output already exists")
    directory_non_symlink(filesystem, "guardian capacity filesystem")
    if minimum_free_bytes < 1:
        raise GateError("continuous guardian minimum free bytes must be positive")
    for marker, description in (
        (ready_path, "guardian ready marker"),
        (launch_path, "guardian launch marker"),
    ):
        if marker.exists() or marker.is_symlink():
            raise GateError(f"refusing to reuse {description}")
    initial_root_identity = require_running_process_identity(
        root_pid, "held measured root"
    )
    root_starttime_ticks = int(initial_root_identity["starttime_ticks"])
    control_deadline = time.monotonic() + 5.0
    while not control_path.exists() and not control_path.is_symlink():
        if not process_is_same_running(root_pid, root_starttime_ticks):
            raise GateError("held measured root exited before guardian control")
        if time.monotonic() >= control_deadline:
            raise GateError("guardian launch control was not created in time")
        time.sleep(0.005)
    control = validate_guardian_control(
        control_path,
        ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=root_pid,
        expected_guardian_pid=os.getpid(),
        require_live=True,
    )
    if control["root_starttime_ticks"] != root_starttime_ticks:
        raise GateError("guardian control bound a reused held-root PID")
    rss_ready_path = Path(control["rss_ready_marker"])
    rss_deadline = time.monotonic() + 5.0
    while not rss_ready_path.exists() and not rss_ready_path.is_symlink():
        if not process_is_same_running(
            control["rss_monitor_pid"], control["rss_monitor_starttime_ticks"]
        ):
            raise GateError("RSS monitor exited before its ready marker")
        if not process_is_same_running(root_pid, root_starttime_ticks):
            raise GateError("held measured root exited before RSS readiness")
        if time.monotonic() >= rss_deadline:
            raise GateError("RSS monitor did not become ready before timeout")
        time.sleep(0.005)
    validate_empty_read_only_marker(rss_ready_path, "RSS ready marker")
    started = time.monotonic_ns()
    maximum_allowed_gap_ns = guardian_maximum_allowed_gap_ns(interval_ms)
    poll_monotonic_elapsed_ns: list[int] = []
    polls = 0
    live_polls = 0
    terminal_poll_index: int | None = None
    root_seen = False
    ready_created_poll: int | None = None
    ready_created_monotonic_elapsed_ns: int | None = None
    launch_observed_poll: int | None = None
    launch_observed_monotonic_elapsed_ns: int | None = None
    launch_observed_root_bound = False
    handshake_violations: list[str] = []
    observed: dict[tuple[int, str, str], dict[str, Any]] = {}
    minimum_observed_free_bytes: int | None = None
    capacity_violations: list[dict[str, Any]] = []
    termination: dict[str, Any] = {
        "attempted": False,
        "root_starttime_ticks": root_starttime_ticks,
        "target_processes": [],
        "target_pids": [],
        "term_sent_pids": [],
        "term_errors": [],
        "kill_sent_pids": [],
        "kill_errors": [],
        "identity_refusals": [],
        "surviving_pids": [],
    }
    while True:
        allowed = process_tree(root_pid, root_starttime_ticks)
        terminal_poll = False
        if allowed:
            root_seen = True
            live_polls += 1
        elif root_seen:
            terminal_poll = True
        elif not process_is_same_running(root_pid, root_starttime_ticks):
            terminal_poll = True
        poll_monotonic_elapsed_ns.append(time.monotonic_ns() - started)
        for conflict in scan_conflicts(
            allowed_root_pid=root_pid,
            allowed_root_starttime_ticks=root_starttime_ticks,
        ):
            key = (conflict["pid"], conflict["comm"], conflict["command"])
            observed.setdefault(key, conflict)
        filesystem_stats = os.statvfs(filesystem)
        available = filesystem_stats.f_bavail * filesystem_stats.f_frsize
        minimum_observed_free_bytes = (
            available
            if minimum_observed_free_bytes is None
            else min(minimum_observed_free_bytes, available)
        )
        if available < minimum_free_bytes:
            capacity_violations.append(
                {
                    "poll": polls + 1,
                    "monotonic_elapsed_ns": time.monotonic_ns() - started,
                    "free_bytes": available,
                    "minimum_free_bytes": minimum_free_bytes,
                }
            )
        polls += 1
        if terminal_poll:
            terminal_poll_index = polls
        maximum_poll_start_gap_ns = derive_guardian_maximum_poll_start_gap_ns(
            poll_monotonic_elapsed_ns, poll_monotonic_elapsed_ns[-1]
        )
        failed_poll = bool(
            observed
            or capacity_violations
            or maximum_poll_start_gap_ns > maximum_allowed_gap_ns
        )
        if ready_created_poll is None:
            if launch_path.exists() or launch_path.is_symlink():
                handshake_violations.append("launch marker existed before readiness")
            elif not failed_poll and allowed:
                if not process_is_same_running(
                    control["rss_monitor_pid"],
                    control["rss_monitor_starttime_ticks"],
                ):
                    handshake_violations.append("RSS monitor exited before readiness")
                else:
                    try:
                        validate_empty_read_only_marker(
                            rss_ready_path, "RSS ready marker"
                        )
                    except GateError as error:
                        handshake_violations.append(str(error))
                    if not handshake_violations:
                        create_empty_read_only_marker(ready_path, "guardian ready marker")
                        ready_created_poll = polls
                        ready_created_monotonic_elapsed_ns = poll_monotonic_elapsed_ns[-1]
        else:
            try:
                validate_empty_read_only_marker(ready_path, "guardian ready marker")
            except GateError as error:
                handshake_violations.append(str(error))
            try:
                validate_empty_read_only_marker(rss_ready_path, "RSS ready marker")
            except GateError as error:
                handshake_violations.append(str(error))
            if launch_path.exists() or launch_path.is_symlink():
                try:
                    validate_empty_read_only_marker(
                        launch_path, "guardian launch marker"
                    )
                except GateError as error:
                    handshake_violations.append(str(error))
                else:
                    if launch_observed_poll is None:
                        launch_observed_poll = polls
                        launch_observed_monotonic_elapsed_ns = (
                            poll_monotonic_elapsed_ns[-1]
                        )
                        launch_observed_root_bound = bool(allowed)
        if (
            failed_poll
            or handshake_violations
        ):
            termination = terminate_process_tree(root_pid, root_starttime_ticks)
            break
        if terminal_poll:
            break
        time.sleep(interval_ms / 1000)
    elapsed_ns = time.monotonic_ns() - started
    maximum_poll_start_gap_ns = derive_guardian_maximum_poll_start_gap_ns(
        poll_monotonic_elapsed_ns, elapsed_ns
    )
    ready_marker_sha256: str | None = None
    launch_marker_sha256: str | None = None
    try:
        ready_marker_sha256 = sha256_file(
            validate_empty_read_only_marker(ready_path, "guardian ready marker")
        )
    except GateError as error:
        if str(error) not in handshake_violations:
            handshake_violations.append(str(error))
    try:
        launch_marker_sha256 = sha256_file(
            validate_empty_read_only_marker(launch_path, "guardian launch marker")
        )
    except GateError as error:
        if str(error) not in handshake_violations:
            handshake_violations.append(str(error))
    if launch_observed_poll is None:
        handshake_violations.append("guardian never observed the launch marker")
    elif not launch_observed_root_bound:
        handshake_violations.append(
            "guardian observed the launch marker only after the root stopped"
        )
    result = {
        "schema": GUARDIAN_SCHEMA,
        "root_pid": root_pid,
        "root_starttime_ticks": root_starttime_ticks,
        "guardian_pid": os.getpid(),
        "interval_ms": interval_ms,
        "polls": polls,
        "live_polls": live_polls,
        "terminal_poll": terminal_poll_index,
        "elapsed_ns": elapsed_ns,
        "poll_monotonic_elapsed_ns": poll_monotonic_elapsed_ns,
        "maximum_poll_start_gap_ns": maximum_poll_start_gap_ns,
        "maximum_allowed_poll_start_gap_ns": maximum_allowed_gap_ns,
        "control_path": str(control_path),
        "control_sha256": sha256_file(control_path),
        "ready_marker_path": str(ready_path),
        "ready_marker_sha256": ready_marker_sha256,
        "ready_created_poll": ready_created_poll,
        "ready_created_monotonic_elapsed_ns": ready_created_monotonic_elapsed_ns,
        "launch_marker_path": str(launch_path),
        "launch_marker_sha256": launch_marker_sha256,
        "launch_observed_poll": launch_observed_poll,
        "launch_observed_monotonic_elapsed_ns": launch_observed_monotonic_elapsed_ns,
        "launch_observed": launch_observed_poll is not None,
        "launch_observed_root_bound": launch_observed_root_bound,
        "handshake_violations": handshake_violations,
        "root_seen": root_seen,
        "filesystem": str(filesystem.resolve(strict=True)),
        "minimum_required_free_bytes": minimum_free_bytes,
        "minimum_observed_free_bytes": minimum_observed_free_bytes,
        "capacity_violations": capacity_violations,
        "conflicts": sorted(observed.values(), key=lambda value: value["pid"]),
        "termination": termination,
        "complete_and_conflict_free": (
            root_seen
            and live_polls >= 2
            and terminal_poll_index == polls
            and live_polls == terminal_poll_index - 1
            and maximum_poll_start_gap_ns <= maximum_allowed_gap_ns
            and ready_created_poll is not None
            and launch_observed_poll is not None
            and ready_created_poll < launch_observed_poll
            and launch_observed_poll < terminal_poll_index
            and not handshake_violations
            and not observed
            and not capacity_violations
        ),
    }
    write_json_exclusive(output, result)
    if observed:
        raise GateError(
            f"continuous quiet-host guardian observed a conflict: "
            f"{next(iter(observed.values()))!r}"
        )
    if capacity_violations:
        raise GateError(
            f"continuous capacity guardian exhausted its reserve: "
            f"{capacity_violations[0]!r}"
        )
    if handshake_violations:
        raise GateError(
            f"continuous guardian launch handshake failed: {handshake_violations!r}"
        )
    if maximum_poll_start_gap_ns > maximum_allowed_gap_ns:
        raise GateError(
            "continuous guardian missed its cadence: "
            f"maximum_gap_ns={maximum_poll_start_gap_ns}, "
            f"allowed_ns={maximum_allowed_gap_ns}"
        )
    if not root_seen or live_polls < 2:
        raise GateError(
            "continuous guardian did not observe the process for at least two cadence polls"
        )
    return result


def parse_checkpoint(checkpoint_path: Path, rss_path: Path, plan: dict[str, Any]) -> dict[str, Any]:
    lines = checkpoint_path.read_text(encoding="utf-8", errors="strict").splitlines()
    if lines[:1] != ["schema\tphase\tmain_elapsed_ns\tunix_time_ns\thold_secs"] or len(lines) != 3:
        raise GateError("release checkpoint must contain its exact header and two rows")
    rows = []
    for index, line in enumerate(lines[1:], start=1):
        fields = line.split("\t")
        if len(fields) != 5 or fields[0] != CHECKPOINT_SCHEMA:
            raise GateError(f"release checkpoint row {index} is malformed")
        if any(re.fullmatch(r"[0-9]+", value) is None for value in fields[2:]):
            raise GateError(f"release checkpoint row {index} has a non-integer field")
        rows.append(
            {
                "phase": fields[1],
                "main_elapsed_ns": int(fields[2]),
                "unix_time_ns": int(fields[3]),
                "hold_secs": int(fields[4]),
            }
        )
    if [row["phase"] for row in rows] != ["ingester_dropped", "hold_complete"]:
        raise GateError("release checkpoint phases changed")
    hold_secs = plan["workload"]["post_ingester_drop_hold_secs"]
    if any(row["hold_secs"] != hold_secs for row in rows):
        raise GateError("release checkpoint hold differs from the plan")
    elapsed = rows[1]["main_elapsed_ns"] - rows[0]["main_elapsed_ns"]
    wall_elapsed = rows[1]["unix_time_ns"] - rows[0]["unix_time_ns"]
    minimum_ns = hold_secs * 1_000_000_000
    maximum_ns = plan["gate"]["maximum_hold_elapsed_secs"] * 1_000_000_000
    if not minimum_ns <= elapsed <= maximum_ns or not minimum_ns <= wall_elapsed <= maximum_ns:
        raise GateError("release checkpoint hold elapsed time is outside bounds")
    rss = require_exact_keys(load_json(rss_path), RSS_KEYS, "$.rss")
    strict_int(rss["root_pid"], "$.rss.root_pid", minimum=1)
    if rss["interval_ms"] != plan["workload"]["rss_interval_ms"]:
        raise GateError("RSS interval differs from the plan")
    ticks_per_second = strict_int(rss["clock_ticks_per_second"], "$.rss.clock_ticks_per_second", minimum=1)
    for key in (
        "samples",
        "workload_samples",
        "post_drop_samples",
        "hold_complete_samples",
        "checkpoint_incomplete_samples",
    ):
        strict_int(rss[key], f"$.rss.{key}", minimum=0)
    if rss["workload_samples"] < 1 or rss["post_drop_samples"] < plan["gate"]["minimum_post_drop_rss_samples"]:
        raise GateError("RSS monitor lacks workload or post-drop phase coverage")
    if sum(
        rss[key]
        for key in (
            "workload_samples",
            "post_drop_samples",
            "hold_complete_samples",
            "checkpoint_incomplete_samples",
        )
    ) != rss["samples"]:
        raise GateError("RSS phase counts do not account for every sample")
    for key in (
        "peak_rss_kib",
        "workload_peak_rss_kib",
        "workload_peak_max_single_hwm_kib",
        "workload_boundary_max_single_hwm_kib",
        "post_drop_first_rss_kib",
        "post_drop_min_rss_kib",
        "post_drop_end_rss_kib",
        "workload_boundary_cpu_ticks",
    ):
        strict_int(rss[key], f"$.rss.{key}", minimum=1)
    if rss["peak_vm_swap_kib"] != 0:
        raise GateError("measured process tree used swap")
    cpu_seconds = strict_number(rss["workload_boundary_cpu_seconds"], "$.rss.workload_boundary_cpu_seconds", positive=True)
    if abs(cpu_seconds - rss["workload_boundary_cpu_ticks"] / ticks_per_second) > 1e-12:
        raise GateError("workload CPU seconds do not derive from ticks and CLK_TCK")
    first_post = strict_int(rss["post_drop_first_unix_time_ns"], "$.rss.post_drop_first_unix_time_ns", minimum=1)
    last_post = strict_int(rss["post_drop_end_unix_time_ns"], "$.rss.post_drop_end_unix_time_ns", minimum=1)
    if not rows[0]["unix_time_ns"] <= first_post <= last_post <= rows[1]["unix_time_ns"]:
        raise GateError("RSS post-drop phase escapes checkpoint bounds")
    boundary_start = strict_int(
        rss["workload_boundary_sample_window_start_unix_time_ns"],
        "$.rss.workload_boundary_sample_window_start_unix_time_ns",
        minimum=1,
    )
    boundary_end = strict_int(
        rss["workload_boundary_sample_unix_time_ns"],
        "$.rss.workload_boundary_sample_unix_time_ns",
        minimum=1,
    )
    if boundary_end != first_post or boundary_start > boundary_end or boundary_end < rows[0]["unix_time_ns"]:
        raise GateError("workload CPU boundary is not the first post-drop sample")
    uncertainty = max(
        abs(boundary_start - rows[0]["unix_time_ns"]),
        boundary_end - rows[0]["unix_time_ns"],
    )
    maximum_uncertainty = (
        plan["workload"]["rss_interval_ms"]
        * 1_000_000
        * plan["gate"]["maximum_workload_cpu_boundary_uncertainty_intervals"]
    )
    if uncertainty > maximum_uncertainty:
        raise GateError("workload CPU boundary uncertainty exceeds one interval")
    return {
        "workload_wall_ns": rows[0]["main_elapsed_ns"],
        "workload_cpu_ticks": rss["workload_boundary_cpu_ticks"],
        "workload_cpu_seconds": cpu_seconds,
        "clock_ticks_per_second": ticks_per_second,
        "workload_cpu_boundary_uncertainty_ns": uncertainty,
        "hold_elapsed_ns": elapsed,
        "hold_wall_elapsed_ns": wall_elapsed,
        "drop_main_elapsed_ns": rows[0]["main_elapsed_ns"],
        "hold_complete_main_elapsed_ns": rows[1]["main_elapsed_ns"],
        "drop_unix_time_ns": rows[0]["unix_time_ns"],
        "hold_complete_unix_time_ns": rows[1]["unix_time_ns"],
        "rss": rss,
    }


def parse_rss_samples(path: Path) -> list[dict[str, Any]]:
    expected = [
        "elapsed_ns",
        "sample_window_start_unix_time_ns",
        "unix_time_ns",
        "phase",
        "process_count",
        "process_cpu_ticks",
        "rss_kib",
        "rss_anon_kib",
        "rss_file_kib",
        "vm_swap_kib",
        "max_single_hwm_kib",
        "pids",
    ]
    rows = []
    with path.open(newline="", encoding="utf-8", errors="strict") as source:
        reader = csv.DictReader(source, delimiter="\t")
        if reader.fieldnames != expected:
            raise GateError("RSS sample table header changed")
        for line_number, raw in enumerate(reader, start=2):
            if None in raw or any(value is None for value in raw.values()):
                raise GateError(f"RSS sample row {line_number} is malformed")
            row: dict[str, Any] = {"phase": raw["phase"], "pids": raw["pids"]}
            for key in expected:
                if key in {"phase", "pids"}:
                    continue
                if re.fullmatch(r"[0-9]+", raw[key]) is None:
                    raise GateError(f"RSS sample row {line_number} {key} is not an integer")
                row[key] = int(raw[key])
            if row["phase"] not in {
                "workload",
                "post_drop_hold",
                "hold_complete",
                "checkpoint_incomplete",
                "terminal",
            }:
                raise GateError(f"RSS sample row {line_number} has an unknown phase")
            rows.append(row)
    if not rows:
        raise GateError("RSS sample table is empty")
    terminal_rows = [row for row in rows if row["phase"] == "terminal"]
    if len(terminal_rows) != 1 or rows[-1]["phase"] != "terminal":
        raise GateError("RSS sample table lacks one final terminal observation")
    terminal = terminal_rows[0]
    if (
        terminal["process_count"] != 0
        or terminal["process_cpu_ticks"] != 0
        or terminal["rss_kib"] != 0
        or terminal["rss_anon_kib"] != 0
        or terminal["rss_file_kib"] != 0
        or terminal["vm_swap_kib"] != 0
        or terminal["max_single_hwm_kib"] != 0
        or terminal["pids"] != "-"
    ):
        raise GateError("RSS terminal observation is malformed")
    return rows


def cross_check_rss_samples(rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    live_rows = [row for row in rows if row["phase"] != "terminal"]
    if len(live_rows) < 2:
        raise GateError("RSS raw samples contain fewer than two live observations")
    phase_counts = {
        phase: sum(row["phase"] == phase for row in live_rows)
        for phase in ("workload", "post_drop_hold", "hold_complete", "checkpoint_incomplete")
    }
    expected = {
        "samples": len(live_rows),
        "workload_samples": phase_counts["workload"],
        "post_drop_samples": phase_counts["post_drop_hold"],
        "hold_complete_samples": phase_counts["hold_complete"],
        "checkpoint_incomplete_samples": phase_counts["checkpoint_incomplete"],
        "peak_rss_kib": max(row["rss_kib"] for row in live_rows),
        "peak_rss_anon_kib": max(row["rss_anon_kib"] for row in live_rows),
        "peak_rss_file_kib": max(row["rss_file_kib"] for row in live_rows),
        "peak_vm_swap_kib": max(row["vm_swap_kib"] for row in live_rows),
        "peak_process_count": max(row["process_count"] for row in live_rows),
    }
    for key, value in expected.items():
        if summary[key] != value:
            raise GateError(f"RSS summary is not derived from raw samples: {key}")
    if (
        summary["terminal_observation"] is not True
        or summary["terminal_launch_observed"] is not True
    ):
        raise GateError("RSS summary did not retain its terminal launch observation")
    workload = [row for row in live_rows if row["phase"] == "workload"]
    post_drop = [row for row in live_rows if row["phase"] == "post_drop_hold"]
    if not workload or not post_drop:
        raise GateError("RSS raw samples lack required workload/post-drop phases")
    derived = {
        "workload_peak_rss_kib": max(row["rss_kib"] for row in workload),
        "workload_peak_max_single_hwm_kib": max(
            row["max_single_hwm_kib"] for row in workload
        ),
        "workload_boundary_max_single_hwm_kib": post_drop[0]["max_single_hwm_kib"],
        "post_drop_first_rss_kib": post_drop[0]["rss_kib"],
        "post_drop_min_rss_kib": min(row["rss_kib"] for row in post_drop),
        "post_drop_end_rss_kib": post_drop[-1]["rss_kib"],
        "post_drop_first_unix_time_ns": post_drop[0]["unix_time_ns"],
        "post_drop_end_unix_time_ns": post_drop[-1]["unix_time_ns"],
        "workload_boundary_cpu_ticks": post_drop[0]["process_cpu_ticks"],
        "workload_boundary_sample_window_start_unix_time_ns": post_drop[0]["sample_window_start_unix_time_ns"],
        "workload_boundary_sample_unix_time_ns": post_drop[0]["unix_time_ns"],
    }
    for key, value in derived.items():
        if summary[key] != value:
            raise GateError(f"RSS phase summary is not derived from raw samples: {key}")
    timestamps = [row["elapsed_ns"] for row in rows]
    saved_timestamps = summary["poll_monotonic_elapsed_ns"]
    if saved_timestamps != timestamps or len(timestamps) < 3:
        raise GateError(
            "RSS raw cadence timestamps differ or contain fewer than two live samples plus terminal"
        )
    for index, timestamp in enumerate(timestamps):
        strict_int(timestamp, f"$.rss.poll_monotonic_elapsed_ns[{index}]", minimum=0)
        if index > 0 and timestamp <= timestamps[index - 1]:
            raise GateError("RSS raw cadence timestamps are not strictly increasing")
    elapsed_ns = strict_int(summary["elapsed_ns"], "$.rss.elapsed_ns", minimum=1)
    if timestamps[-1] > elapsed_ns:
        raise GateError("RSS cadence timestamp exceeds monitor elapsed time")
    derived_gap = derive_guardian_maximum_poll_start_gap_ns(timestamps, elapsed_ns)
    interval_ms = strict_int(summary["interval_ms"], "$.rss.interval_ms", minimum=1)
    if interval_ms != 100:
        raise GateError("RSS cadence interval is not exact 100 ms")
    allowed_gap = guardian_maximum_allowed_gap_ns(interval_ms)
    ready_sample = strict_int(
        summary["rss_ready_created_sample"],
        "$.rss.rss_ready_created_sample",
        minimum=1,
    )
    launch_sample = strict_int(
        summary["launch_observed_sample"],
        "$.rss.launch_observed_sample",
        minimum=1,
    )
    ready_elapsed_ns = strict_int(
        summary["rss_ready_created_monotonic_elapsed_ns"],
        "$.rss.rss_ready_created_monotonic_elapsed_ns",
        minimum=0,
    )
    launch_elapsed_ns = strict_int(
        summary["launch_observed_monotonic_elapsed_ns"],
        "$.rss.launch_observed_monotonic_elapsed_ns",
        minimum=0,
    )
    if (
        strict_int(
            summary["maximum_poll_start_gap_ns"],
            "$.rss.maximum_poll_start_gap_ns",
            minimum=0,
        )
        != derived_gap
        or strict_int(
            summary["maximum_allowed_poll_start_gap_ns"],
            "$.rss.maximum_allowed_poll_start_gap_ns",
            minimum=1,
        )
        != allowed_gap
        or derived_gap > allowed_gap
        or ready_sample != 1
        or not 1 < launch_sample <= len(live_rows)
        or ready_elapsed_ns != timestamps[0]
        or launch_elapsed_ns != timestamps[launch_sample - 1]
        or summary["launch_observed"] is not True
        or summary["handshake_violations"] != []
        or summary["complete"] is not True
        or str(summary["root_pid"]) not in rows[0]["pids"].split(",")
    ):
        raise GateError("RSS held-launch cadence evidence is not exactly derived")


def validate_rss_handshake_evidence(
    summary: dict[str, Any],
    control_path: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> dict[str, Any]:
    root_pid = strict_int(summary["root_pid"], "$.rss.root_pid", minimum=1)
    root_starttime_ticks = strict_int(
        summary["root_starttime_ticks"],
        "$.rss.root_starttime_ticks",
        minimum=1,
    )
    control = validate_guardian_control(
        control_path,
        guardian_ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=root_pid,
    )
    rss_ready = validate_empty_read_only_marker(
        rss_ready_path, "RSS ready marker"
    )
    launch = validate_empty_read_only_marker(
        launch_path, "guardian launch marker"
    )
    if (
        control["rss_ready_marker"] != str(rss_ready_path)
        or root_starttime_ticks != control["root_starttime_ticks"]
        or summary["control_path"] != str(control_path)
        or summary["control_sha256"] != sha256_file(control_path)
        or summary["rss_ready_marker_path"] != str(rss_ready_path)
        or summary["rss_ready_marker_sha256"] != sha256_file(rss_ready)
        or summary["launch_marker_path"] != str(launch_path)
        or summary["launch_marker_sha256"] != sha256_file(launch)
    ):
        raise GateError("RSS held-launch identities and marker digests are not exact")
    return control


def validate_jemalloc_sources(text: str, expected_conf: str) -> None:
    expected_sources = [
        '<jemalloc>: malloc_conf #1 (string specified via --with-malloc-conf): ""',
        '<jemalloc>: malloc_conf #2 (string pointed to by the global variable malloc_conf): ""',
        '<jemalloc>: malloc_conf #3 ("name" of the file referenced by the symbolic link named /etc/malloc.conf): ""',
        '<jemalloc>: malloc_conf #4 (value of the environment variable MALLOC_CONF): '
        f'"{expected_conf}"',
        '<jemalloc>: malloc_conf #5 (string pointed to by the global variable malloc_conf_2_conf_harder): ""',
    ]
    observed_sources = [
        line
        for line in text.splitlines()
        if line.startswith("<jemalloc>: malloc_conf #")
    ]
    if observed_sources != expected_sources:
        raise GateError("jemalloc configuration sources #1..#5 differ from the nominated environment")
    if "Invalid conf" in text or "Malformed conf" in text:
        raise GateError("jemalloc reported an invalid selected policy")
    for entry in expected_conf.split(","):
        if text.splitlines().count(f"<jemalloc>: -- Set conf value: {entry}") != 1:
            raise GateError(f"jemalloc did not confirm exactly one selected-policy entry: {entry}")


def parse_runtime_log(
    path: Path,
    role: str,
    binding: dict[str, Any],
    preflight: dict[str, Any],
    plan: dict[str, Any],
) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8", errors="strict")
    prefix = "CHRONOXIDE_ALLOCATOR_RUNTIME_POLICY_JSON="
    records = [line[len(prefix) :] for line in text.splitlines() if line.startswith(prefix)]
    if len(records) != 1:
        raise GateError("measured replay must emit exactly one structured allocator runtime record")
    try:
        value = json.loads(records[0])
    except json.JSONDecodeError as error:
        raise GateError("allocator runtime record is not JSON") from error
    keys = {
        "schema",
        "rust_global_allocator",
        "jemalloc_conf_env",
        "requested_policy_raw",
        "requested_policy_canonical",
        "effective_policy",
        "post_ingester_drop_hold_secs",
        "post_ingester_drop_checkpoint_enabled",
        "post_ingester_drop_telemetry_enabled",
    }
    value = require_exact_keys(value, keys, "$.allocator_runtime")
    if value["schema"] != APPLICATION_RUNTIME_SCHEMA or value["jemalloc_conf_env"] != "_RJEM_MALLOC_CONF":
        raise GateError("allocator runtime schema or environment name changed")
    if (
        value["post_ingester_drop_hold_secs"]
        != plan["workload"]["post_ingester_drop_hold_secs"]
        or value["post_ingester_drop_checkpoint_enabled"] is not True
        or value["post_ingester_drop_telemetry_enabled"] is not True
    ):
        raise GateError("allocator runtime did not enable the exact release hold")
    if role == "stats-candidate":
        if (
            value["rust_global_allocator"] != "jemalloc"
            or value["requested_policy_raw"] != binding["selected_jemalloc_conf"]
            or value["requested_policy_canonical"] != binding["selected_jemalloc_conf"]
            or value["effective_policy"] != preflight["effective_policy"]
        ):
            raise GateError("stats candidate runtime differs from its screen-nominated policy")
        validate_effective_policy(
            value["effective_policy"],
            binding["selected_jemalloc_conf"],
            "$.allocator_runtime.effective_policy",
        )
        validate_jemalloc_sources(text, binding["selected_jemalloc_conf"])
    elif role == "no-stats-candidate":
        if value["rust_global_allocator"] != "jemalloc" or any(
            value[key] is not None
            for key in ("requested_policy_raw", "requested_policy_canonical", "effective_policy")
        ):
            raise GateError("plain candidate runtime compiled stats policy diagnostics")
        validate_jemalloc_sources(text, binding["selected_jemalloc_conf"])
    else:
        if value["rust_global_allocator"] != "system" or any(
            value[key] is not None
            for key in ("requested_policy_raw", "requested_policy_canonical", "effective_policy")
        ):
            raise GateError("system runtime unexpectedly reports jemalloc policy")
        if "<jemalloc>:" in text:
            raise GateError("system runtime contains jemalloc confirmation output")
    for marker in (
        "Ingester state dropped; beginning diagnostic allocator release hold",
        "Diagnostic allocator release hold complete",
    ):
        if text.count(marker) != 1:
            raise GateError(f"runtime log must contain exactly one lifecycle marker: {marker}")
    return value


def parse_telemetry(
    path: Path,
    role: str,
    checkpoint: dict[str, Any],
    rss_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    lines = [line for line in path.read_text(encoding="utf-8", errors="strict").splitlines() if line]
    if len(lines) != 2:
        raise GateError("allocator release telemetry must contain exactly two records")
    records = []
    for index, line in enumerate(lines):
        try:
            value = require_exact_keys(json.loads(line), TELEMETRY_KEYS, f"$.telemetry[{index}]")
        except json.JSONDecodeError as error:
            raise GateError(f"allocator telemetry row {index + 1} is not JSON") from error
        if value["schema"] != TELEMETRY_SCHEMA:
            raise GateError("allocator telemetry schema mismatch")
        records.append(value)
    if [record["phase"] for record in records] != ["post_ingester_drop", "hold_complete"]:
        raise GateError("allocator telemetry phases changed")
    expected_identity = "system" if role == "system" else "jemalloc"
    stat_fields = ("epoch", "allocated_bytes", "active_bytes", "resident_bytes", "mapped_bytes", "retained_bytes")
    for index, record in enumerate(records):
        if record["rust_global_allocator"] != expected_identity:
            raise GateError("allocator telemetry identity changed")
        for key in ("main_elapsed_ns", "unix_time_ns"):
            strict_int(record[key], f"$.telemetry[{index}].{key}", minimum=1)
        if role == "stats-candidate":
            if record["allocator_internal_telemetry"] != "available":
                raise GateError("stats candidate release telemetry is unavailable")
            for key in stat_fields:
                strict_int(record[key], f"$.telemetry[{index}].{key}", minimum=0)
            if record["active_bytes"] < record["allocated_bytes"]:
                raise GateError("stats candidate active bytes are below allocated bytes")
        else:
            if record["allocator_internal_telemetry"] != "unavailable" or any(
                record[key] is not None for key in stat_fields
            ):
                raise GateError("system/plain candidate telemetry fields must be explicit unavailable/null")
    if role == "stats-candidate" and records[1]["epoch"] <= records[0]["epoch"]:
        raise GateError("stats candidate telemetry epoch did not advance")
    if not (
        checkpoint["drop_main_elapsed_ns"]
        <= records[0]["main_elapsed_ns"]
        <= records[1]["main_elapsed_ns"]
        <= checkpoint["hold_complete_main_elapsed_ns"]
    ):
        raise GateError("allocator telemetry monotonic timestamps escape checkpoint bounds")
    if not (
        checkpoint["drop_unix_time_ns"]
        <= records[0]["unix_time_ns"]
        <= records[1]["unix_time_ns"]
        <= checkpoint["hold_complete_unix_time_ns"]
    ):
        raise GateError("allocator telemetry wall timestamps escape checkpoint bounds")
    post_drop = [row for row in rss_rows if row["phase"] == "post_drop_hold"]
    for record in records:
        nearest = min(post_drop, key=lambda row: abs(row["unix_time_ns"] - record["unix_time_ns"]))
        if abs(nearest["unix_time_ns"] - record["unix_time_ns"]) > 100_000_000:
            raise GateError("allocator telemetry has no external RSS sample within one interval")
    return {
        "records": records,
        "hold_complete_minus_post_drop_bytes": {
            key: (
                records[1][key] - records[0][key]
                if records[0][key] is not None and records[1][key] is not None
                else None
            )
            for key in stat_fields[1:]
        },
    }


def validate_guardian(
    path: Path,
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
    plan: dict[str, Any],
    expected_minimum_free_bytes: int,
) -> dict[str, Any]:
    value = require_exact_keys(
        load_json(path),
        {
            "schema",
            "root_pid",
            "root_starttime_ticks",
            "guardian_pid",
            "interval_ms",
            "polls",
            "live_polls",
            "terminal_poll",
            "elapsed_ns",
            "poll_monotonic_elapsed_ns",
            "maximum_poll_start_gap_ns",
            "maximum_allowed_poll_start_gap_ns",
            "control_path",
            "control_sha256",
            "ready_marker_path",
            "ready_marker_sha256",
            "ready_created_poll",
            "ready_created_monotonic_elapsed_ns",
            "launch_marker_path",
            "launch_marker_sha256",
            "launch_observed_poll",
            "launch_observed_monotonic_elapsed_ns",
            "launch_observed",
            "launch_observed_root_bound",
            "handshake_violations",
            "root_seen",
            "filesystem",
            "minimum_required_free_bytes",
            "minimum_observed_free_bytes",
            "capacity_violations",
            "conflicts",
            "termination",
            "complete_and_conflict_free",
        },
        "$.guardian",
    )
    polls = strict_int(value["polls"], "$.guardian.polls", minimum=2)
    live_polls = strict_int(
        value["live_polls"], "$.guardian.live_polls", minimum=2
    )
    terminal_poll = strict_int(
        value["terminal_poll"], "$.guardian.terminal_poll", minimum=2
    )
    elapsed_ns = strict_int(value["elapsed_ns"], "$.guardian.elapsed_ns", minimum=1)
    timestamps = value["poll_monotonic_elapsed_ns"]
    if not isinstance(timestamps, list) or len(timestamps) != polls:
        raise GateError("guardian cadence timestamp count differs from polls")
    previous: int | None = None
    for index, raw_timestamp in enumerate(timestamps):
        timestamp = strict_int(
            raw_timestamp,
            f"$.guardian.poll_monotonic_elapsed_ns[{index}]",
            minimum=0,
        )
        if timestamp > elapsed_ns:
            raise GateError("guardian cadence timestamp exceeds guardian elapsed time")
        if previous is not None and timestamp <= previous:
            raise GateError("guardian cadence timestamps are not strictly increasing")
        previous = timestamp
    derived_gap_ns = derive_guardian_maximum_poll_start_gap_ns(timestamps, elapsed_ns)
    expected_allowed_gap_ns = guardian_maximum_allowed_gap_ns(
        plan["environment"]["external_conflict_poll_interval_ms"]
    )
    if (
        strict_int(
            value["maximum_poll_start_gap_ns"],
            "$.guardian.maximum_poll_start_gap_ns",
            minimum=0,
        )
        != derived_gap_ns
        or strict_int(
            value["maximum_allowed_poll_start_gap_ns"],
            "$.guardian.maximum_allowed_poll_start_gap_ns",
            minimum=1,
        )
        != expected_allowed_gap_ns
    ):
        raise GateError("guardian cadence maximum gap is not exactly derived")
    if derived_gap_ns > expected_allowed_gap_ns:
        raise GateError("guardian cadence maximum gap exceeds its exact allowance")
    root_pid = strict_int(value["root_pid"], "$.guardian.root_pid", minimum=1)
    root_starttime_ticks = strict_int(
        value["root_starttime_ticks"],
        "$.guardian.root_starttime_ticks",
        minimum=1,
    )
    guardian_pid = strict_int(
        value["guardian_pid"], "$.guardian.guardian_pid", minimum=1
    )
    control = validate_guardian_control(
        control_path,
        ready_path,
        launch_path,
        plan["environment"]["external_conflict_poll_interval_ms"],
        expected_root_pid=root_pid,
        expected_guardian_pid=guardian_pid,
    )
    ready = validate_empty_read_only_marker(ready_path, "guardian ready marker")
    launch = validate_empty_read_only_marker(launch_path, "guardian launch marker")
    expected_rss_ready = control_path.with_name("rss-monitor-ready")
    if control["rss_ready_marker"] != str(expected_rss_ready):
        raise GateError("guardian control RSS ready marker path is not canonical")
    validate_empty_read_only_marker(expected_rss_ready, "RSS ready marker")
    ready_poll = strict_int(
        value["ready_created_poll"], "$.guardian.ready_created_poll", minimum=1
    )
    launch_poll = strict_int(
        value["launch_observed_poll"], "$.guardian.launch_observed_poll", minimum=1
    )
    ready_elapsed = strict_int(
        value["ready_created_monotonic_elapsed_ns"],
        "$.guardian.ready_created_monotonic_elapsed_ns",
        minimum=0,
    )
    launch_elapsed = strict_int(
        value["launch_observed_monotonic_elapsed_ns"],
        "$.guardian.launch_observed_monotonic_elapsed_ns",
        minimum=0,
    )
    if (
        ready_poll != 1
        or launch_poll <= ready_poll
        or launch_poll > polls
        or terminal_poll != polls
        or live_polls != terminal_poll - 1
        or live_polls != polls - 1
        or launch_poll >= terminal_poll
        or ready_elapsed != timestamps[ready_poll - 1]
        or launch_elapsed != timestamps[launch_poll - 1]
        or ready_elapsed >= launch_elapsed
        or value["launch_observed"] is not True
        or value["launch_observed_root_bound"] is not True
        or value["handshake_violations"] != []
        or value["control_path"] != str(control_path)
        or value["control_sha256"] != sha256_file(control_path)
        or value["ready_marker_path"] != str(ready_path)
        or value["ready_marker_sha256"] != sha256_file(ready)
        or value["launch_marker_path"] != str(launch_path)
        or value["launch_marker_sha256"] != sha256_file(launch)
        or control["rss_monitor_pid"] in {root_pid, guardian_pid}
        or control["root_starttime_ticks"] != root_starttime_ticks
    ):
        raise GateError("guardian held-launch handshake is not exact and causal")
    if (
        value["schema"] != GUARDIAN_SCHEMA
        or value["interval_ms"]
        != plan["environment"]["external_conflict_poll_interval_ms"]
        or value["root_seen"] is not True
        or not isinstance(value["filesystem"], str)
        or not Path(value["filesystem"]).is_absolute()
        or strict_int(
            value["minimum_required_free_bytes"],
            "$.guardian.minimum_required_free_bytes",
            minimum=1,
        )
        != expected_minimum_free_bytes
        or strict_int(
            value["minimum_observed_free_bytes"],
            "$.guardian.minimum_observed_free_bytes",
            minimum=0,
        )
        < value["minimum_required_free_bytes"]
        or value["capacity_violations"] != []
        or value["conflicts"] != []
        or value["termination"]
        != {
            "attempted": False,
            "root_starttime_ticks": root_starttime_ticks,
            "target_processes": [],
            "target_pids": [],
            "term_sent_pids": [],
            "term_errors": [],
            "kill_sent_pids": [],
            "kill_errors": [],
            "identity_refusals": [],
            "surviving_pids": [],
        }
        or value["complete_and_conflict_free"] is not True
    ):
        raise GateError("continuous quiet-host guardian did not pass")
    return value


def validate_quiescence(
    path: Path,
    samples_path: Path,
    plan: dict[str, Any],
    expected_corpus: Path,
    expected_fsynced_file_count: int,
) -> dict[str, Any]:
    value = require_exact_keys(
        load_json(path),
        {
            "schema",
            "corpus",
            "fsynced_file_count",
            "global_sync_called",
            "maximum_dirty_writeback_kib",
            "required_consecutive_samples",
            "interval_ms",
            "timeout_secs",
            "sample_count",
            "final_dirty_kib",
            "final_writeback_kib",
            "final_total_kib",
            "passed",
        },
        "$.quiescence",
    )
    environment = plan["environment"]
    expected_corpus = directory_non_symlink(
        expected_corpus, "quiescence corpus"
    ).resolve(strict=True)
    if (
        value["schema"]
        != "chronoxide/storage-vnext-phase5-writeback-quiescence/v1"
        or value["maximum_dirty_writeback_kib"]
        != environment["maximum_dirty_writeback_kib"]
        or value["required_consecutive_samples"]
        != environment["required_quiescent_samples"]
        or value["interval_ms"] != environment["quiescence_poll_interval_ms"]
        or value["timeout_secs"] != environment["quiescence_timeout_secs"]
        or value["global_sync_called"] is not True
        or value["corpus"] != str(expected_corpus)
        or value["fsynced_file_count"] != expected_fsynced_file_count
        or value["passed"] is not True
    ):
        raise GateError("writeback-quiescence contract did not pass")
    for key in (
        "fsynced_file_count",
        "sample_count",
        "final_dirty_kib",
        "final_writeback_kib",
        "final_total_kib",
    ):
        strict_int(value[key], f"$.quiescence.{key}", minimum=0)
    lines = samples_path.read_text(encoding="utf-8", errors="strict").splitlines()
    if lines[:1] != ["elapsed_ns\tdirty_kib\twriteback_kib\ttotal_kib\twithin_limit"]:
        raise GateError("writeback-quiescence sample header changed")
    rows: list[tuple[int, int, int, int, bool]] = []
    previous_elapsed: int | None = None
    for line_number, line in enumerate(lines[1:], start=2):
        fields = line.split("\t")
        if (
            len(fields) != 5
            or any(re.fullmatch(r"[0-9]+", field) is None for field in fields[:4])
            or fields[4] not in {"true", "false"}
        ):
            raise GateError(f"writeback-quiescence sample row {line_number} is malformed")
        elapsed, dirty, writeback, total = (int(field) for field in fields[:4])
        if previous_elapsed is not None and elapsed <= previous_elapsed:
            raise GateError("writeback-quiescence elapsed time is not strictly increasing")
        previous_elapsed = elapsed
        within = fields[4] == "true"
        if dirty + writeback != total or within != (
            total <= value["maximum_dirty_writeback_kib"]
        ):
            raise GateError("writeback-quiescence raw counters/within flag are not derived")
        rows.append((elapsed, dirty, writeback, total, within))
    if not rows:
        raise GateError("writeback-quiescence evidence contains no raw samples")
    consecutive = 0
    for row in rows:
        consecutive = consecutive + 1 if row[4] else 0
    last = rows[-1]
    if (
        value["sample_count"] != len(rows)
        or value["final_dirty_kib"] != last[1]
        or value["final_writeback_kib"] != last[2]
        or value["final_total_kib"] != last[3]
        or value["passed"] != (consecutive >= value["required_consecutive_samples"])
    ):
        raise GateError("writeback-quiescence summary is not derived from raw samples")
    if (
        value["final_total_kib"]
        != value["final_dirty_kib"] + value["final_writeback_kib"]
        or value["final_total_kib"] > value["maximum_dirty_writeback_kib"]
    ):
        raise GateError("writeback-quiescence counters are inconsistent")
    return value


def parse_gnu_time_raw(path: Path) -> dict[str, Any]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="strict").splitlines():
        stripped = line.strip()
        if ": " in stripped:
            key, raw = stripped.rsplit(": ", 1)
            values[key] = raw
    keys = {
        "user_seconds": "User time (seconds)",
        "system_seconds": "System time (seconds)",
        "elapsed": "Elapsed (wall clock) time (h:mm:ss or m:ss)",
        "max_rss_kib": "Maximum resident set size (kbytes)",
        "major_page_faults": "Major (requiring I/O) page faults",
        "minor_page_faults": "Minor (reclaiming a frame) page faults",
        "voluntary_context_switches": "Voluntary context switches",
        "involuntary_context_switches": "Involuntary context switches",
        "filesystem_inputs": "File system inputs",
        "filesystem_outputs": "File system outputs",
        "exit_status": "Exit status",
    }
    missing = [source for source in keys.values() if source not in values]
    if missing:
        raise GateError(f"raw GNU time report is missing fields: {missing!r}")
    result: dict[str, Any] = {}
    try:
        for target, source in keys.items():
            if target in {"user_seconds", "system_seconds"}:
                result[target] = float(values[source])
            elif target == "elapsed":
                result[target] = values[source]
            else:
                result[target] = int(values[source])
    except ValueError as error:
        raise GateError("raw GNU time report contains an invalid numeric field") from error
    result["cpu_percent"] = values.get("Percent of CPU this job got", "")
    return result


def parse_perf_stat_raw(path: Path) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="strict").splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) < 3:
            continue
        raw_value = fields[0].strip()
        event = fields[2].strip()
        if not event:
            continue
        events.append(
            {
                "event": event,
                "raw_value": raw_value,
                "unit": fields[1].strip(),
                "available": re.fullmatch(r"[0-9.]+", raw_value) is not None,
            }
        )
    if not events:
        raise GateError("raw perf-stat report contains no event rows")
    by_name = {event["event"]: event for event in events}
    if len(by_name) != len(events):
        raise GateError("raw perf-stat report contains duplicate events")
    for event in EXPECTED_PERF_EVENTS:
        if event not in by_name or by_name[event]["available"] is not True:
            raise GateError(f"raw perf-stat required event is missing/unavailable: {event}")
    return {"events": events}


def validate_perf_preflight(input_controls: Path) -> dict[str, float]:
    status_path = input_controls / "perf-stat-preflight.exit-status"
    raw_path = input_controls / "perf-stat-preflight.tsv"
    parsed_path = input_controls / "perf-stat-preflight.json"
    regular_non_symlink(status_path, "perf-stat preflight exit status")
    if status_path.read_bytes() != b"0\n":
        raise GateError("perf-stat preflight did not exit successfully")
    return validate_perf_evidence(raw_path, parsed_path)


def validate_timing(path: Path) -> dict[str, Any]:
    keys = {
        "user_seconds",
        "system_seconds",
        "elapsed",
        "max_rss_kib",
        "major_page_faults",
        "minor_page_faults",
        "voluntary_context_switches",
        "involuntary_context_switches",
        "filesystem_inputs",
        "filesystem_outputs",
        "exit_status",
        "cpu_percent",
    }
    value = require_exact_keys(load_json(path), keys, "$.time")
    for key in ("user_seconds", "system_seconds"):
        strict_number(value[key], f"$.time.{key}")
    for key in keys - {"user_seconds", "system_seconds", "elapsed", "cpu_percent"}:
        strict_int(value[key], f"$.time.{key}", minimum=0)
    if value["exit_status"] != 0 or value["max_rss_kib"] <= 0:
        raise GateError("GNU time reports a failed or empty replay")
    if not isinstance(value["elapsed"], str) or not value["elapsed"]:
        raise GateError("GNU time elapsed field is missing")
    return value


def validate_timing_evidence(raw_path: Path, parsed_path: Path) -> dict[str, Any]:
    if load_json(parsed_path) != parse_gnu_time_raw(raw_path):
        raise GateError("saved GNU time JSON is not derived from the raw report")
    return validate_timing(parsed_path)


def validate_perf(path: Path) -> dict[str, float]:
    value = require_exact_keys(load_json(path), {"events"}, "$.perf")
    events = value["events"]
    if not isinstance(events, list) or not events:
        raise GateError("perf stat document is empty")
    parsed: dict[str, float] = {}
    for index, raw in enumerate(events):
        item = require_exact_keys(
            raw, {"event", "raw_value", "unit", "available"}, f"$.perf.events[{index}]"
        )
        event = item["event"]
        if not isinstance(event, str) or event in parsed:
            raise GateError("perf stat contains an invalid or duplicate event")
        if item["available"] is not True or not isinstance(item["raw_value"], str):
            raise GateError(f"perf stat event is unavailable: {event}")
        try:
            parsed[event] = float(item["raw_value"])
        except ValueError as error:
            raise GateError(f"perf stat event is non-numeric: {event}") from error
        if not math.isfinite(parsed[event]) or parsed[event] < 0:
            raise GateError(f"perf stat event is invalid: {event}")
    missing = EXPECTED_PERF_EVENTS - set(parsed)
    if missing:
        raise GateError(f"perf stat lacks required counters: {sorted(missing)!r}")
    return {key: parsed[key] for key in sorted(EXPECTED_PERF_EVENTS)}


def validate_perf_evidence(raw_path: Path, parsed_path: Path) -> dict[str, float]:
    if load_json(parsed_path) != parse_perf_stat_raw(raw_path):
        raise GateError("saved perf-stat JSON is not derived from the raw TSV")
    return validate_perf(parsed_path)


def parse_replay_report_with_frozen_helper(
    report_path: Path, binding: dict[str, Any], scratch_root: Path
) -> dict[str, Any]:
    helper = Path(binding["screen_root"]) / "metadata/harness/ab_gate.py"
    regular_non_symlink(helper, "screen-frozen replay-report parser")
    if sha256_file(helper) != binding["report_gate_sha256"]:
        raise GateError("screen-frozen replay-report parser changed")
    scratch_root = directory_non_symlink(
        scratch_root, "raw-recompute scratch root"
    ).resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="full-gate-replay-", dir=scratch_root) as raw:
        output = Path(raw) / "replay-correctness.json"
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-S",
                "-B",
                str(helper),
                "replay-report",
                "--report",
                str(report_path),
                "--output",
                str(output),
            ],
            env={"LC_ALL": "C", "TZ": "UTC"},
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            raise GateError(
                "screen-frozen replay-report parser rejected raw ingestion report: "
                f"{completed.stderr.strip()}"
            )
        return load_json(output)


def validate_correctness(
    path: Path,
    expectations_path: Path,
    *,
    report_path: Path | None = None,
    binding: dict[str, Any] | None = None,
    scratch_root: Path | None = None,
) -> dict[str, Any]:
    actual = load_json(path)
    if report_path is not None:
        if binding is None or scratch_root is None:
            raise GateError("raw correctness recomputation lacks frozen helper context")
        recomputed = parse_replay_report_with_frozen_helper(
            report_path, binding, scratch_root
        )
        if actual != recomputed:
            raise GateError("saved replay correctness is not derived from the raw report")
    expected = load_json(expectations_path).get("replay_correctness")
    if actual != expected:
        raise GateError("4M replay correctness differs from the screen-frozen authority")
    if actual.get("general", {}).get("Total Messages") != 4_000_000:
        raise GateError("4M replay correctness has the wrong message count")
    return actual


def derive_corpus_artifacts(corpus: Path) -> tuple[bytes, bytes, dict[str, Any]]:
    corpus = directory_non_symlink(corpus, "segment corpus").resolve(strict=True)
    rows: list[tuple[str, int, str]] = []
    for root, directory_names, file_names in os.walk(corpus, followlinks=False):
        root_path = Path(root)
        directory_names.sort(key=os.fsencode)
        file_names.sort(key=os.fsencode)
        for name in directory_names:
            candidate = root_path / name
            if candidate.is_symlink():
                raise GateError(f"segment corpus contains a symlink directory: {candidate}")
        for name in file_names:
            candidate = root_path / name
            metadata = candidate.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise GateError(f"segment corpus contains a non-regular entry: {candidate}")
            relative = candidate.relative_to(corpus).as_posix()
            if any(character in relative for character in ("\t", "\n", "\r")):
                raise GateError(f"segment corpus contains an unsafe path: {relative!r}")
            rows.append((sha256_file(candidate), metadata.st_size, relative))
    rows.sort(key=lambda row: os.fsencode(row[2]))
    if not rows:
        raise GateError("segment corpus contains no payload files")
    manifest = "".join(
        f"{digest}  ./{relative}\n" for digest, _size, relative in rows
    ).encode()
    inventory = (
        "sha256\tsize_bytes\tpath\n"
        + "".join(
            f"{digest}\t{size}\t{relative}\n" for digest, size, relative in rows
        )
    ).encode()
    summary = {
        "schema": CORPUS_SCHEMA,
        "file_count": len(rows),
        "size_bytes": sum(size for _digest, size, _relative in rows),
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
    }
    return manifest, inventory, summary


def validate_corpus(
    path: Path,
    segments_manifest: Path,
    segments_inventory: Path | None = None,
    segments_directory: Path | None = None,
) -> dict[str, Any]:
    value = require_exact_keys(
        load_json(path), {"schema", "file_count", "size_bytes", "manifest_sha256"}, "$.corpus"
    )
    if value["schema"] != CORPUS_SCHEMA:
        raise GateError("corpus summary schema mismatch")
    strict_int(value["file_count"], "$.corpus.file_count", minimum=1)
    strict_int(value["size_bytes"], "$.corpus.size_bytes", minimum=1)
    strict_sha256(value["manifest_sha256"], "$.corpus.manifest_sha256")
    if sha256_file(segments_manifest) != value["manifest_sha256"]:
        raise GateError("corpus summary does not bind the raw segment manifest")
    inventory_path = segments_inventory or path.parent / "segments.tsv"
    corpus_path = segments_directory or path.parent / "segments"
    manifest_bytes, inventory_bytes, recomputed = derive_corpus_artifacts(corpus_path)
    if segments_manifest.read_bytes() != manifest_bytes:
        raise GateError("saved segment manifest is not derived from payload bytes")
    if inventory_path.read_bytes() != inventory_bytes:
        raise GateError("saved segment inventory is not derived from payload bytes")
    if value != recomputed:
        raise GateError("saved corpus summary is not derived from payload bytes")
    return value


def expected_observation_role(stage: str, position: int, plan: dict[str, Any]) -> tuple[str, str]:
    if stage not in EXPECTED_STAGE_SCHEDULES or not 1 <= position <= 4:
        raise GateError("observation stage/position is outside the frozen schedule")
    token = EXPECTED_STAGE_SCHEDULES[stage][position - 1]
    if token == "S":
        return token, "system"
    if stage == "stats" and token == "C":
        return token, "stats-candidate"
    if stage == "no-stats" and token == "N":
        return token, "no-stats-candidate"
    raise GateError("observation stage schedule is inconsistent")


def make_observation(
    *,
    stage: str,
    position: int,
    binary_path: Path,
    screen_binding_path: Path,
    no_stats_build_path: Path,
    preflight_path: Path,
    runtime_log_path: Path,
    checkpoint_path: Path,
    telemetry_path: Path,
    rss_path: Path,
    rss_samples_path: Path,
    timing_raw_path: Path,
    timing_path: Path,
    perf_raw_path: Path,
    perf_path: Path,
    guardian_path: Path,
    capacity_path: Path,
    pre_quiescence_samples_path: Path,
    pre_quiescence_path: Path,
    post_quiescence_samples_path: Path,
    post_quiescence_path: Path,
    replay_report_path: Path,
    correctness_path: Path,
    corpus_path: Path,
    segments_manifest_path: Path,
    segments_inventory_path: Path,
    capture_residency_before_path: Path,
    capture_residency_after_path: Path,
    capture_inputs_path: Path,
    expectations_path: Path,
    plan_path: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path)
    token, role = expected_observation_role(stage, position, plan)
    binding = load_json(screen_binding_path)
    no_stats_build = load_json(no_stats_build_path)
    run_root = runtime_log_path.parent
    scan_paths = {
        "processes_before": run_root / "processes-before.json",
        "processes_immediately_before_launch": run_root
        / "processes-immediately-before-launch.json",
        "processes_after": run_root / "processes-after.json",
    }
    for scan_path in scan_paths.values():
        validate_conflict_scan(scan_path)
    status_paths = {
        "replay_exit_status": run_root / "replay.exit-status",
        "rss_monitor_exit_status": run_root / "rss-monitor.exit-status",
        "guardian_exit_status": run_root / "external-conflict-guardian.exit-status",
    }
    for name, status_path in status_paths.items():
        validate_zero_exit_status(status_path, name.replace("_", " "))
    handshake_paths = {
        "guardian_control": run_root / "external-conflict-guardian-control.json",
        "guardian_ready": run_root / "external-conflict-guardian-ready",
        "guardian_launch": run_root / "external-conflict-guardian-launch",
        "rss_ready": run_root / "rss-monitor-ready",
    }
    expected_hash = (
        binding["binary_sha256"]["system"]
        if role == "system"
        else binding["binary_sha256"]["jemalloc"]
        if role == "stats-candidate"
        else no_stats_build["binary_sha256"]
    )
    if sha256_file(binary_path) != expected_hash:
        raise GateError("observation binary differs from its controlled role")
    preflight = load_json(preflight_path)
    if preflight.get("role") != role or preflight.get("binary_sha256") != expected_hash:
        raise GateError("observation preflight differs from its controlled role/binary")
    checkpoint = parse_checkpoint(checkpoint_path, rss_path, plan)
    rss_rows = parse_rss_samples(rss_samples_path)
    cross_check_rss_samples(rss_rows, checkpoint["rss"])
    runtime = parse_runtime_log(runtime_log_path, role, binding, preflight, plan)
    telemetry = parse_telemetry(telemetry_path, role, checkpoint, rss_rows)
    timing = validate_timing_evidence(timing_raw_path, timing_path)
    perf = validate_perf_evidence(perf_raw_path, perf_path)
    ordinal = observation_ordinal(stage, position)
    first_corpus_summary = (
        None
        if ordinal == 1
        else corpus_path.parents[1] / "stats-01-S" / "corpus-summary.json"
    )
    capacity = validate_run_capacity(
        capacity_path,
        stage,
        position,
        expectations_path,
        plan_path,
        first_corpus_summary,
    )
    guardian = validate_guardian(
        guardian_path,
        handshake_paths["guardian_control"],
        handshake_paths["guardian_ready"],
        handshake_paths["guardian_launch"],
        plan,
        capacity["guardian_minimum_free_bytes"],
    )
    expected_result_filesystem = str(corpus_path.parents[2].resolve(strict=True))
    if (
        capacity["filesystem"] != expected_result_filesystem
        or guardian["filesystem"] != expected_result_filesystem
    ):
        raise GateError("capacity evidence and guardian did not monitor RESULT_DIR")
    if guardian["root_pid"] != checkpoint["rss"]["root_pid"]:
        raise GateError("quiet-host guardian and RSS monitor observed different process roots")
    rss = checkpoint["rss"]
    validate_rss_handshake_evidence(
        rss,
        handshake_paths["guardian_control"],
        handshake_paths["guardian_ready"],
        handshake_paths["rss_ready"],
        handshake_paths["guardian_launch"],
        plan["workload"]["rss_interval_ms"],
    )
    capture_before = validate_capture_residency(
        capture_residency_before_path,
        capture_inputs_path,
        maximum_resident_bytes=plan["workload"]["max_capture_resident_bytes_after_evict"],
    )
    capture_after = validate_capture_residency(
        capture_residency_after_path,
        capture_inputs_path,
        maximum_resident_bytes=None,
    )
    configs_root = pre_quiescence_path.parents[2] / "configs"
    config_files, _config_directories = walk_regular_tree(configs_root)
    segment_files, _segment_directories = walk_regular_tree(corpus_path.parent / "segments")
    pre_quiescence = validate_quiescence(
        pre_quiescence_path,
        pre_quiescence_samples_path,
        plan,
        configs_root,
        len(config_files),
    )
    post_quiescence = validate_quiescence(
        post_quiescence_path,
        post_quiescence_samples_path,
        plan,
        corpus_path.parent / "segments",
        len(segment_files),
    )
    correctness = validate_correctness(
        correctness_path,
        expectations_path,
        report_path=replay_report_path,
        binding=binding,
        scratch_root=corpus_path.parents[2] / "build-target",
    )
    corpus = validate_corpus(
        corpus_path,
        segments_manifest_path,
        segments_inventory_path,
        corpus_path.parent / "segments",
    )
    raw_paths = {
        "preflight": preflight_path,
        "runtime_log": runtime_log_path,
        "checkpoint": checkpoint_path,
        "telemetry": telemetry_path,
        "rss_summary": rss_path,
        "rss_samples": rss_samples_path,
        "time_raw": timing_raw_path,
        "time": timing_path,
        "perf_raw": perf_raw_path,
        "perf": perf_path,
        "guardian": guardian_path,
        "run_capacity": capacity_path,
        "pre_quiescence_samples": pre_quiescence_samples_path,
        "pre_quiescence": pre_quiescence_path,
        "post_quiescence_samples": post_quiescence_samples_path,
        "post_quiescence": post_quiescence_path,
        "replay_report": replay_report_path,
        "correctness": correctness_path,
        "corpus": corpus_path,
        "segments_manifest": segments_manifest_path,
        "segments_inventory": segments_inventory_path,
        "capture_residency_before": capture_residency_before_path,
        "capture_residency_after": capture_residency_after_path,
        "capture_inputs": capture_inputs_path,
        **scan_paths,
        **status_paths,
        **handshake_paths,
    }
    return {
        "schema": OBSERVATION_SCHEMA,
        "stage": stage,
        "position": position,
        "schedule_token": token,
        "role": role,
        "selected_policy": binding["selected_policy"],
        "selected_jemalloc_conf": binding["selected_jemalloc_conf"],
        "applied_jemalloc_conf": (
            binding["selected_jemalloc_conf"] if role != "system" else None
        ),
        "binary_sha256": expected_hash,
        "screen_binding_sha256": sha256_file(screen_binding_path),
        "no_stats_build_sha256": sha256_file(no_stats_build_path),
        "preflight_sha256": sha256_file(preflight_path),
        "runtime_policy": runtime,
        "allocator_release_telemetry": telemetry,
        "workload_wall_ns": checkpoint["workload_wall_ns"],
        "workload_cpu_ticks": checkpoint["workload_cpu_ticks"],
        "workload_cpu_seconds": checkpoint["workload_cpu_seconds"],
        "clock_ticks_per_second": checkpoint["clock_ticks_per_second"],
        "workload_cpu_boundary_uncertainty_ns": checkpoint[
            "workload_cpu_boundary_uncertainty_ns"
        ],
        "hold_elapsed_ns": checkpoint["hold_elapsed_ns"],
        "rss": checkpoint["rss"],
        "gnu_time": timing,
        "perf": perf,
        "guardian": guardian,
        "run_capacity": capacity,
        "capture_residency_before": capture_before,
        "capture_residency_after": capture_after,
        "pre_quiescence": pre_quiescence,
        "post_quiescence": post_quiescence,
        "correctness": correctness,
        "correctness_sha256": sha256_file(correctness_path),
        "corpus": corpus,
        "raw_sha256": {name: sha256_file(path) for name, path in raw_paths.items()},
        "production_promotion_authorized": False,
    }


def pair_midpoint(values: list[float]) -> float:
    if len(values) != 2:
        raise GateError("comparison role must have exactly two counterbalanced observations")
    return sum(values) / 2.0


def pair_spread(values: list[float]) -> float:
    midpoint = pair_midpoint(values)
    if midpoint <= 0:
        raise GateError("comparison midpoint must be positive")
    return (max(values) - min(values)) / midpoint * 100.0


def compare_stage(observation_paths: list[Path], stage: str, plan_path: Path) -> dict[str, Any]:
    plan = validate_plan(plan_path)
    if stage not in EXPECTED_STAGE_SCHEDULES or len(observation_paths) != 4:
        raise GateError("stage comparison requires the exact four-observation schedule")
    observations = [load_json(path) for path in observation_paths]
    for position, observation in enumerate(observations, start=1):
        token, role = expected_observation_role(stage, position, plan)
        if (
            observation.get("schema") != OBSERVATION_SCHEMA
            or observation.get("stage") != stage
            or observation.get("position") != position
            or observation.get("schedule_token") != token
            or observation.get("role") != role
            or observation.get("production_promotion_authorized") is not False
        ):
            raise GateError("stage observation differs from the frozen schedule")
    first = observations[0]
    for observation in observations[1:]:
        for key in ("selected_policy", "selected_jemalloc_conf", "screen_binding_sha256", "no_stats_build_sha256", "correctness", "corpus"):
            if observation[key] != first[key]:
                raise GateError(f"stage observations differ for {key}")
    system = [value for value in observations if value["role"] == "system"]
    candidate_role = "stats-candidate" if stage == "stats" else "no-stats-candidate"
    candidate = [value for value in observations if value["role"] == candidate_role]
    metrics = {
        "workload_cpu_seconds": lambda item: item["workload_cpu_seconds"],
        "workload_peak_rss_kib": lambda item: item["rss"]["workload_peak_rss_kib"],
        "workload_boundary_hwm_kib": lambda item: item["rss"]["workload_boundary_max_single_hwm_kib"],
        "post_drop_end_rss_kib": lambda item: item["rss"]["post_drop_end_rss_kib"],
    }
    midpoints: dict[str, dict[str, float]] = {"system": {}, "candidate": {}}
    spreads: dict[str, dict[str, float]] = {"system": {}, "candidate": {}}
    for metric, getter in metrics.items():
        for role_name, values in (("system", system), ("candidate", candidate)):
            raw = [strict_number(getter(item), metric, positive=True) for item in values]
            midpoints[role_name][metric] = pair_midpoint(raw)
            spreads[role_name][metric] = pair_spread(raw)
    maximum_spread = plan["gate"]["maximum_pair_relative_spread_percent"]
    dispersion_pass = all(
        spread <= maximum_spread
        for role_spreads in spreads.values()
        for spread in role_spreads.values()
    )
    baseline = midpoints["system"]
    compared = midpoints["candidate"]
    cpu_improvement = (
        (baseline["workload_cpu_seconds"] - compared["workload_cpu_seconds"])
        / baseline["workload_cpu_seconds"]
        * 100.0
    )
    regressions = {
        "workload_peak_rss_regression_percent": (
            compared["workload_peak_rss_kib"] - baseline["workload_peak_rss_kib"]
        )
        / baseline["workload_peak_rss_kib"]
        * 100.0,
        "workload_hwm_regression_percent": (
            compared["workload_boundary_hwm_kib"] - baseline["workload_boundary_hwm_kib"]
        )
        / baseline["workload_boundary_hwm_kib"]
        * 100.0,
        "post_drop_end_rss_regression_percent": (
            compared["post_drop_end_rss_kib"] - baseline["post_drop_end_rss_kib"]
        )
        / baseline["post_drop_end_rss_kib"]
        * 100.0,
    }
    gate = plan["gate"]
    passed = (
        dispersion_pass
        and cpu_improvement >= gate["minimum_workload_cpu_improvement_percent"]
        and regressions["workload_peak_rss_regression_percent"]
        <= gate["maximum_workload_peak_rss_regression_percent"]
        and regressions["workload_hwm_regression_percent"]
        <= gate["maximum_workload_hwm_regression_percent"]
        and regressions["post_drop_end_rss_regression_percent"]
        <= gate["maximum_post_drop_end_rss_regression_percent"]
    )
    return {
        "schema": STAGE_SCHEMA,
        "stage": stage,
        "complete": True,
        "observation_count": 4,
        "schedule": EXPECTED_STAGE_SCHEDULES[stage],
        "selected_policy": first["selected_policy"],
        "selected_jemalloc_conf": first["selected_jemalloc_conf"],
        "candidate_role": candidate_role,
        "midpoints": midpoints,
        "pair_relative_spread_percent": spreads,
        "dispersion_pass": dispersion_pass,
        "workload_cpu_improvement_percent": cpu_improvement,
        **regressions,
        "thresholds": gate,
        "passed": passed,
        "observation_sha256": [sha256_file(path) for path in observation_paths],
        "production_promotion_authorized": False,
    }


def markdown_section(text: str, name: str) -> str:
    match = re.search(rf"^## {re.escape(name)}\s*$", text, re.MULTILINE)
    if match is None:
        raise GateError(f"readback report lacks section {name!r}")
    start = match.end()
    next_section = re.search(r"^## ", text[start:], re.MULTILINE)
    return text[start : start + next_section.start() if next_section else len(text)]


def markdown_rows(section: str) -> list[list[str]]:
    rows = []
    for line in section.splitlines():
        if not line.startswith("|") or not line.endswith("|"):
            continue
        cells = [cell.strip() for cell in line[1:-1].split("|")]
        if cells and all(re.fullmatch(r":?-+:?", cell) for cell in cells):
            continue
        rows.append(cells)
    return rows


def two_column_values(text: str, section_name: str) -> dict[str, str]:
    rows = markdown_rows(markdown_section(text, section_name))
    if not rows or rows[0] != ["Metric", "Value"]:
        raise GateError(f"readback section {section_name!r} lacks its exact table header")
    result: dict[str, str] = {}
    for row in rows[1:]:
        if len(row) != 2 or row[0] in result:
            raise GateError(f"readback section {section_name!r} is malformed")
        result[row[0]] = row[1]
    return result


def required_markdown_int(values: dict[str, str], key: str) -> int:
    raw = values.get(key)
    if raw is None or re.fullmatch(r"[0-9]+", raw) is None:
        raise GateError(f"readback report lacks integer metric {key!r}")
    return int(raw)


def _validate_inventory_histogram(
    value: Any, path: str, expected_observations: int
) -> dict[str, Any]:
    histogram = require_exact_keys(value, {"zero_count", "buckets"}, path)
    zero_count = strict_int(histogram["zero_count"], f"{path}.zero_count", minimum=0)
    buckets = histogram["buckets"]
    if not isinstance(buckets, list):
        raise GateError(f"{path}.buckets must be a list")
    normalized = []
    previous_upper = 0
    observations = zero_count
    for index, item in enumerate(buckets):
        item_path = f"{path}.buckets[{index}]"
        bucket = require_exact_keys(
            item, {"lower_inclusive", "upper_inclusive", "count"}, item_path
        )
        lower = strict_int(
            bucket["lower_inclusive"], f"{item_path}.lower_inclusive", minimum=1
        )
        upper = strict_int(
            bucket["upper_inclusive"], f"{item_path}.upper_inclusive", minimum=1
        )
        count = strict_int(bucket["count"], f"{item_path}.count", minimum=1)
        if lower & (lower - 1) or upper != 2 * lower - 1:
            raise GateError(f"{item_path} is not an exact power-of-two bucket")
        if lower <= previous_upper:
            raise GateError(f"{path}.buckets are not strictly ascending and disjoint")
        previous_upper = upper
        observations += count
        normalized.append(
            {
                "lower_inclusive": lower,
                "upper_inclusive": upper,
                "count": count,
            }
        )
    if observations != expected_observations:
        raise GateError(
            f"{path} observations do not reconcile: expected "
            f"{expected_observations}, found {observations}"
        )
    return {"zero_count": zero_count, "buckets": normalized}


def _validate_inventory_winner(value: Any, path: str) -> dict[str, int]:
    winner = require_exact_keys(value, {"chunks", "points"}, path)
    return {
        key: strict_int(winner[key], f"{path}.{key}", minimum=0)
        for key in ("chunks", "points")
    }


def _validate_timestamp_candidate(value: Any, path: str) -> dict[str, Any]:
    candidate = require_exact_keys(
        value, {"bytes", "unique_wins", "adaptive_selections"}, path
    )
    return {
        "bytes": strict_int(candidate["bytes"], f"{path}.bytes", minimum=0),
        "unique_wins": _validate_inventory_winner(
            candidate["unique_wins"], f"{path}.unique_wins"
        ),
        "adaptive_selections": _validate_inventory_winner(
            candidate["adaptive_selections"], f"{path}.adaptive_selections"
        ),
    }


def _validate_timestamp_evidence(value: Any, path: str) -> dict[str, Any]:
    evidence = require_exact_keys(
        value,
        {
            "chunks",
            "points",
            "adaptive_min_bytes",
            "tied_minima",
            *TIMESTAMP_CANDIDATES,
        },
        path,
    )
    parsed: dict[str, Any] = {
        "chunks": strict_int(evidence["chunks"], f"{path}.chunks", minimum=0),
        "points": strict_int(evidence["points"], f"{path}.points", minimum=0),
        "adaptive_min_bytes": strict_int(
            evidence["adaptive_min_bytes"],
            f"{path}.adaptive_min_bytes",
            minimum=0,
        ),
        "tied_minima": _validate_inventory_winner(
            evidence["tied_minima"], f"{path}.tied_minima"
        ),
    }
    parsed.update(
        {
            candidate: _validate_timestamp_candidate(
                evidence[candidate], f"{path}.{candidate}"
            )
            for candidate in TIMESTAMP_CANDIDATES
        }
    )
    selected_chunks = sum(
        parsed[candidate]["adaptive_selections"]["chunks"]
        for candidate in TIMESTAMP_CANDIDATES
    )
    selected_points = sum(
        parsed[candidate]["adaptive_selections"]["points"]
        for candidate in TIMESTAMP_CANDIDATES
    )
    if (selected_chunks, selected_points) != (parsed["chunks"], parsed["points"]):
        raise GateError(f"{path}: adaptive timestamp selections do not reconcile")
    unique_chunks = sum(
        parsed[candidate]["unique_wins"]["chunks"]
        for candidate in TIMESTAMP_CANDIDATES
    )
    unique_points = sum(
        parsed[candidate]["unique_wins"]["points"]
        for candidate in TIMESTAMP_CANDIDATES
    )
    ties = parsed["tied_minima"]
    if (unique_chunks + ties["chunks"], unique_points + ties["points"]) != (
        parsed["chunks"],
        parsed["points"],
    ):
        raise GateError(f"{path}: unique timestamp wins and ties do not reconcile")
    if parsed["adaptive_min_bytes"] > min(
        parsed[candidate]["bytes"] for candidate in TIMESTAMP_CANDIDATES
    ):
        raise GateError(f"{path}: adaptive timestamp bytes exceed an aggregate candidate")
    return parsed


def _timestamp_additive_totals(value: dict[str, Any]) -> dict[str, int]:
    totals = {
        "chunks": value["chunks"],
        "points": value["points"],
        "adaptive_min_bytes": value["adaptive_min_bytes"],
        "tied_minima.chunks": value["tied_minima"]["chunks"],
        "tied_minima.points": value["tied_minima"]["points"],
    }
    for candidate in TIMESTAMP_CANDIDATES:
        totals[f"{candidate}.bytes"] = value[candidate]["bytes"]
        for winner in ("unique_wins", "adaptive_selections"):
            totals[f"{candidate}.{winner}.chunks"] = value[candidate][winner][
                "chunks"
            ]
            totals[f"{candidate}.{winner}.points"] = value[candidate][winner][
                "points"
            ]
    return totals


def _reconcile_timestamp_breakdown(
    rows: list[dict[str, Any]], all_blocks: dict[str, Any], path: str
) -> None:
    expected = _timestamp_additive_totals(all_blocks)
    actual = {key: 0 for key in expected}
    for row in rows:
        for key, value in _timestamp_additive_totals(row).items():
            actual[key] += value
    mismatches = [key for key in expected if actual[key] != expected[key]]
    if mismatches:
        field = mismatches[0]
        raise GateError(
            f"{path}: additive timestamp field {field} does not reconcile: "
            f"expected {expected[field]}, found {actual[field]}"
        )


def validate_chunk_inventory(
    value: Any,
    *,
    chunks: int,
    samples: int,
    logical_chunk_bytes: int,
    chunks_by_kind: list[int],
    path: str,
) -> dict[str, Any]:
    inventory = require_exact_keys(
        value,
        {"layout", "by_kind_encoding", "raw_f64_vs_gorilla", "timestamp_candidates"},
        path,
    )
    if inventory["layout"] != "sealed_chunk_v1":
        raise GateError(f"{path}.layout differs from sealed_chunk_v1")
    rows = inventory["by_kind_encoding"]
    if not isinstance(rows, list) or not rows:
        raise GateError(f"{path}.by_kind_encoding must be a nonempty list")

    total_chunks = total_points = total_indexed_bytes = 0
    total_native_timestamp_bytes = 0
    inventory_chunks_by_kind = {kind: 0 for kind in KIND_ORDER}
    float_chunks = float_points = float_indexed = float_payload = 0
    float_encodings: set[str] = set()
    kind_encoding_keys: set[tuple[str, str]] = set()
    for index, item in enumerate(rows):
        item_path = f"{path}.by_kind_encoding[{index}]"
        row = require_exact_keys(item, CHUNK_INVENTORY_ROW_FIELDS, item_path)
        for key in ("kind", "encoding", "payload_layout"):
            if not isinstance(row[key], str) or not row[key]:
                raise GateError(f"{item_path}.{key} must be a nonempty string")
        kind_encoding = (row["kind"], row["encoding"])
        if kind_encoding in kind_encoding_keys:
            raise GateError(f"{path} has a duplicate kind/encoding row: {kind_encoding}")
        kind_encoding_keys.add(kind_encoding)
        expected_layout = VALID_KIND_ENCODING_LAYOUTS.get(kind_encoding)
        if expected_layout is None or row["payload_layout"] != expected_layout:
            raise GateError(f"{item_path} has an invalid kind/encoding/layout tuple")
        row_chunks = strict_int(row["chunks"], f"{item_path}.chunks", minimum=1)
        row_points = strict_int(row["points"], f"{item_path}.points", minimum=1)
        if row_points < row_chunks:
            raise GateError(f"{item_path} has fewer points than chunks")
        byte_fields = {
            key: strict_int(row[key], f"{item_path}.{key}", minimum=0)
            for key in (
                "indexed_bytes",
                "common_header_bytes",
                "scalar_lane_bytes",
                "payload_bytes",
                "timestamp_base_bytes",
                "timestamp_delta_bytes",
                "value_bytes",
            )
        }
        if byte_fields["common_header_bytes"] != row_chunks * 40:
            raise GateError(f"{item_path} common header bytes do not reconcile")
        if byte_fields["indexed_bytes"] != (
            byte_fields["common_header_bytes"]
            + byte_fields["scalar_lane_bytes"]
            + byte_fields["payload_bytes"]
        ):
            raise GateError(f"{item_path} indexed bytes do not reconcile")
        if byte_fields["payload_bytes"] != (
            byte_fields["timestamp_base_bytes"]
            + byte_fields["timestamp_delta_bytes"]
            + byte_fields["value_bytes"]
        ):
            raise GateError(f"{item_path} payload bytes do not reconcile")
        _validate_inventory_histogram(
            row["point_count_histogram"],
            f"{item_path}.point_count_histogram",
            row_chunks,
        )
        _validate_inventory_histogram(
            row["cadence_ms_histogram"],
            f"{item_path}.cadence_ms_histogram",
            row_points - row_chunks,
        )
        total_chunks += row_chunks
        total_points += row_points
        total_indexed_bytes += byte_fields["indexed_bytes"]
        total_native_timestamp_bytes += (
            byte_fields["timestamp_base_bytes"] + byte_fields["timestamp_delta_bytes"]
        )
        inventory_chunks_by_kind[row["kind"]] += row_chunks
        if row["kind"] == "float":
            float_chunks += row_chunks
            float_points += row_points
            float_indexed += byte_fields["indexed_bytes"]
            float_payload += byte_fields["payload_bytes"]
            float_encodings.add(row["encoding"])
    if (total_chunks, total_points) != (chunks, samples):
        raise GateError(f"{path} chunk/point totals differ from the verifier totals")
    if total_indexed_bytes != logical_chunk_bytes:
        raise GateError(f"{path} indexed bytes differ from logical_chunk_bytes")
    if [inventory_chunks_by_kind[kind] for kind in KIND_ORDER] != chunks_by_kind:
        raise GateError(f"{path} kind counts differ from chunks_by_kind")
    if float_encodings != EXPECTED_FLOAT_ENCODINGS:
        raise GateError(
            f"{path} Float encodings differ from the frozen Gorilla contract: "
            f"expected {sorted(EXPECTED_FLOAT_ENCODINGS)}, "
            f"found {sorted(float_encodings)}"
        )

    float_evidence = require_exact_keys(
        inventory["raw_f64_vs_gorilla"],
        FLOAT_EVIDENCE_FIELDS,
        f"{path}.raw_f64_vs_gorilla",
    )
    float_path = f"{path}.raw_f64_vs_gorilla"
    if not isinstance(float_evidence["tie_rule"], str) or "RAW_F64" not in float_evidence[
        "tie_rule"
    ]:
        raise GateError(f"{float_path}.tie_rule is not canonical")
    float_integer_fields = FLOAT_EVIDENCE_FIELDS - {
        "tie_rule",
        "raw_f64_wins",
        "gorilla_wins",
        "ties",
        "adaptive_raw_f64_selections",
        "adaptive_gorilla_selections",
        "xor_significant_bits_histogram",
    }
    float_integers = {
        key: strict_int(float_evidence[key], f"{float_path}.{key}", minimum=0)
        for key in float_integer_fields
    }
    winners = {
        key: _validate_inventory_winner(float_evidence[key], f"{float_path}.{key}")
        for key in (
            "raw_f64_wins",
            "gorilla_wins",
            "ties",
            "adaptive_raw_f64_selections",
            "adaptive_gorilla_selections",
        )
    }
    if (
        float_integers["chunks"],
        float_integers["points"],
        float_integers["existing_indexed_bytes"],
        float_integers["existing_payload_bytes"],
    ) != (float_chunks, float_points, float_indexed, float_payload):
        raise GateError(f"{float_path} totals do not reconcile with inventory rows")
    expected_header_bytes = 40 * float_chunks
    for prefix in (
        "existing",
        "raw_f64_candidate",
        "gorilla_candidate",
        "adaptive_min",
    ):
        if (
            float_integers[f"{prefix}_indexed_bytes"]
            - float_integers[f"{prefix}_payload_bytes"]
            != expected_header_bytes
        ):
            raise GateError(f"{float_path}.{prefix} bytes do not reconcile with headers")
    win_chunks = sum(
        winners[key]["chunks"] for key in ("raw_f64_wins", "gorilla_wins", "ties")
    )
    win_points = sum(
        winners[key]["points"] for key in ("raw_f64_wins", "gorilla_wins", "ties")
    )
    if (win_chunks, win_points) != (float_chunks, float_points):
        raise GateError(f"{float_path} winner totals do not reconcile")
    selected_chunks = sum(
        winners[key]["chunks"]
        for key in ("adaptive_raw_f64_selections", "adaptive_gorilla_selections")
    )
    selected_points = sum(
        winners[key]["points"]
        for key in ("adaptive_raw_f64_selections", "adaptive_gorilla_selections")
    )
    if (selected_chunks, selected_points) != (float_chunks, float_points):
        raise GateError(f"{float_path} adaptive selection totals do not reconcile")
    raw_wins = winners["raw_f64_wins"]
    gorilla_wins = winners["gorilla_wins"]
    ties = winners["ties"]
    if winners["adaptive_raw_f64_selections"] != {
        "chunks": raw_wins["chunks"] + ties["chunks"],
        "points": raw_wins["points"] + ties["points"],
    } or winners["adaptive_gorilla_selections"] != gorilla_wins:
        raise GateError(f"{float_path} adaptive selections violate the RawF64 tie rule")
    if float_integers["adaptive_min_indexed_bytes"] > min(
        float_integers["raw_f64_candidate_indexed_bytes"],
        float_integers["gorilla_candidate_indexed_bytes"],
    ) or float_integers["adaptive_min_payload_bytes"] > min(
        float_integers["raw_f64_candidate_payload_bytes"],
        float_integers["gorilla_candidate_payload_bytes"],
    ):
        raise GateError(f"{float_path} adaptive bytes exceed an aggregate candidate")
    xor_transitions = sum(
        float_integers[key]
        for key in ("repeated_xor_points", "reused_window_points", "new_window_points")
    )
    if xor_transitions != float_points - float_chunks:
        raise GateError(f"{float_path} XOR transition counts do not reconcile")
    _validate_inventory_histogram(
        float_evidence["xor_significant_bits_histogram"],
        f"{float_path}.xor_significant_bits_histogram",
        float_integers["reused_window_points"] + float_integers["new_window_points"],
    )
    classification_points = sum(
        float_integers[key]
        for key in (
            "positive_zero_points",
            "negative_zero_points",
            "finite_nonzero_points",
            "positive_infinity_points",
            "negative_infinity_points",
            "ordinary_nan_points",
            "stale_nan_points",
        )
    )
    if classification_points != float_points:
        raise GateError(f"{float_path} IEEE classifications do not reconcile")

    timestamp_path = f"{path}.timestamp_candidates"
    timestamp = require_exact_keys(
        inventory["timestamp_candidates"],
        {
            "scope",
            "tie_rule",
            "selector_bytes_included",
            "all_blocks",
            "by_shape",
            "by_kind_encoding",
        },
        timestamp_path,
    )
    if timestamp["selector_bytes_included"] is not False:
        raise GateError(f"{timestamp_path} unexpectedly includes selector bytes")
    for key in ("scope", "tie_rule"):
        if not isinstance(timestamp[key], str) or not timestamp[key]:
            raise GateError(f"{timestamp_path}.{key} must be a nonempty string")
    all_blocks = _validate_timestamp_evidence(
        timestamp["all_blocks"], f"{timestamp_path}.all_blocks"
    )
    if (all_blocks["chunks"], all_blocks["points"]) != (chunks, samples):
        raise GateError(f"{timestamp_path}.all_blocks totals do not reconcile")
    if (
        all_blocks["current_offset_uleb"]["bytes"]
        != total_native_timestamp_bytes
    ):
        raise GateError(f"{timestamp_path} current bytes differ from native timestamp bytes")
    by_shape = timestamp["by_shape"]
    if not isinstance(by_shape, list) or not by_shape:
        raise GateError(f"{timestamp_path}.by_shape must be a nonempty list")
    observed_shapes: set[str] = set()
    shape_evidence = []
    for index, item in enumerate(by_shape):
        item_path = f"{timestamp_path}.by_shape[{index}]"
        row = require_exact_keys(item, {"shape", "evidence"}, item_path)
        if row["shape"] not in TIMESTAMP_SHAPES or row["shape"] in observed_shapes:
            raise GateError(f"{item_path}.shape is unknown or duplicated")
        observed_shapes.add(row["shape"])
        shape_evidence.append(
            _validate_timestamp_evidence(row["evidence"], f"{item_path}.evidence")
        )
    if (
        sum(row["chunks"] for row in shape_evidence),
        sum(row["points"] for row in shape_evidence),
    ) != (chunks, samples):
        raise GateError(f"{timestamp_path}.by_shape totals do not reconcile")
    _reconcile_timestamp_breakdown(
        shape_evidence, all_blocks, f"{timestamp_path}.by_shape"
    )
    by_kind_encoding = timestamp["by_kind_encoding"]
    if not isinstance(by_kind_encoding, list) or not by_kind_encoding:
        raise GateError(f"{timestamp_path}.by_kind_encoding must be a nonempty list")
    timestamp_keys: set[tuple[str, str]] = set()
    kind_evidence = []
    for index, item in enumerate(by_kind_encoding):
        item_path = f"{timestamp_path}.by_kind_encoding[{index}]"
        row = require_exact_keys(item, {"kind", "encoding", "evidence"}, item_path)
        key = (row["kind"], row["encoding"])
        if (
            not all(isinstance(part, str) and part for part in key)
            or key in timestamp_keys
        ):
            raise GateError(f"{item_path} has an empty or duplicate key")
        timestamp_keys.add(key)
        kind_evidence.append(
            _validate_timestamp_evidence(row["evidence"], f"{item_path}.evidence")
        )
    if timestamp_keys != kind_encoding_keys:
        raise GateError(f"{timestamp_path} keys differ from inventory rows")
    if (
        sum(row["chunks"] for row in kind_evidence),
        sum(row["points"] for row in kind_evidence),
    ) != (chunks, samples):
        raise GateError(f"{timestamp_path}.by_kind_encoding totals do not reconcile")
    _reconcile_timestamp_breakdown(
        kind_evidence, all_blocks, f"{timestamp_path}.by_kind_encoding"
    )
    return inventory



def validate_storage_report(path: Path, expectations_path: Path) -> dict[str, Any]:
    value = load_json(path)
    expected = load_json(expectations_path).get("storage_verifier")
    if not isinstance(value, dict) or not isinstance(expected, dict):
        raise GateError("storage verifier report or expectation is malformed")
    required = {
        "schema_version",
        "footer_validation_enabled",
        "series_sample_per_segment",
        "verified_selection_fingerprint",
        "decoded_semantic_fingerprint",
        "segments",
        "corpus_series",
        "series",
        "chunks",
        "chunks_by_kind",
        "samples",
        "logical_chunk_bytes",
        "chunk_inventory",
        "exact_postings",
        "elapsed_ns",
        "metadata_read_calls",
        "metadata_read_bytes",
        "metadata_peak_retained_bytes",
        "metadata_peak_in_flight_bytes",
        "metadata_peak_open_files",
        "metadata_cache_hits",
        "metadata_cache_misses",
    }
    require_exact_keys(value, required, "$.storage")
    if value["schema_version"] != 8 or value["footer_validation_enabled"] is not True:
        raise GateError("canonical storage validation did not exhaustively validate schema 8 footers")
    if value["series_sample_per_segment"] is not None:
        raise GateError("canonical storage validation sampled series")
    semantic_keys = {
        "schema_version",
        "footer_validation_enabled",
        "series_sample_per_segment",
        "verified_selection_fingerprint",
        "decoded_semantic_fingerprint",
        "segments",
        "corpus_series",
        "series",
        "chunks",
        "chunks_by_kind",
        "samples",
        "logical_chunk_bytes",
        "chunk_inventory",
        "exact_postings",
    }
    semantic = {key: value[key] for key in semantic_keys}
    for key in ("verified_selection_fingerprint", "decoded_semantic_fingerprint"):
        if not isinstance(semantic[key], str) or re.fullmatch(
            r"[0-9a-f]{64}", semantic[key]
        ) is None:
            raise GateError(f"canonical storage {key} is invalid")
    chunks_by_kind = semantic["chunks_by_kind"]
    if not isinstance(chunks_by_kind, list) or len(chunks_by_kind) != len(KIND_ORDER):
        raise GateError("canonical storage chunks_by_kind has the wrong shape")
    parsed_chunks_by_kind = [
        strict_int(item, f"$.storage.chunks_by_kind[{index}]", minimum=0)
        for index, item in enumerate(chunks_by_kind)
    ]
    if sum(parsed_chunks_by_kind) != semantic["chunks"]:
        raise GateError("canonical storage chunks_by_kind does not sum to chunks")
    semantic["chunks_by_kind"] = parsed_chunks_by_kind
    semantic["chunk_inventory"] = validate_chunk_inventory(
        semantic["chunk_inventory"],
        chunks=semantic["chunks"],
        samples=semantic["samples"],
        logical_chunk_bytes=semantic["logical_chunk_bytes"],
        chunks_by_kind=parsed_chunks_by_kind,
        path="$.storage.chunk_inventory",
    )
    # The completed screen binds the capture and semantic 4M authority, but a
    # later source-bound storage-codec experiment may intentionally change the
    # encoded chunk-byte total.  Require every stable selection/count/postings
    # fact here and compare the complete report between the two allocator
    # shapes at final admission.
    stable_keys = {
        "schema_version",
        "footer_validation_enabled",
        "series_sample_per_segment",
        "verified_selection_fingerprint",
        "segments",
        "corpus_series",
        "series",
        "chunks",
        "chunks_by_kind",
        "samples",
        "exact_postings",
    }
    if any(semantic[key] != expected[key] for key in stable_keys):
        raise GateError(
            "canonical stable storage semantics differ from the screen-frozen 4M authority"
        )
    strict_int(semantic["logical_chunk_bytes"], "$.storage.logical_chunk_bytes", minimum=1)
    for key in (
        "elapsed_ns",
        "metadata_read_calls",
        "metadata_read_bytes",
        "metadata_peak_retained_bytes",
        "metadata_peak_in_flight_bytes",
        "metadata_peak_open_files",
    ):
        strict_int(value[key], f"$.storage.{key}", minimum=1)
    for key in ("metadata_cache_hits", "metadata_cache_misses"):
        strict_int(value[key], f"$.storage.{key}", minimum=0)
    return semantic


def check_storage_completeness(
    storage_path: Path, correctness_path: Path, expectations_path: Path
) -> dict[str, Any]:
    correctness = validate_correctness(correctness_path, expectations_path)
    storage = validate_storage_report(storage_path, expectations_path)
    expected_samples = correctness["general"]["Recorded Samples"]
    if storage["samples"] != expected_samples:
        raise GateError("canonical storage sample count differs from replay correctness")
    return {
        "complete": True,
        "recorded_samples": expected_samples,
        "storage_samples": storage["samples"],
        "decoded_semantic_fingerprint": storage[
            "decoded_semantic_fingerprint"
        ],
    }


def validate_readbacks(path: Path, expectations_path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8", errors="strict")
    verification = two_column_values(text, "Readback Verification")
    diagnostics = two_column_values(text, "Query Diagnostics")
    rows = markdown_rows(markdown_section(text, "PromQL Readbacks"))
    if not rows or rows[0] != PROMQL_READBACK_HEADER:
        raise GateError("PromQL readback table header changed")
    body = rows[1:]
    if any(len(row) != len(PROMQL_READBACK_HEADER) for row in body):
        raise GateError("PromQL readback table contains a malformed row")
    actual = {
        "expected_queries": required_markdown_int(diagnostics, "Expected Readback Queries"),
        "executed_queries": required_markdown_int(diagnostics, "Executed Readback Queries"),
        "skipped_queries": required_markdown_int(diagnostics, "Skipped Readback Queries"),
        "isolation_check_skips": required_markdown_int(diagnostics, "Isolation Check Skips"),
        "mismatches": required_markdown_int(verification, "Mismatches"),
        "promql_rows": len(body),
        "promql_rows_fingerprint_sha256": hashlib.sha256(
            json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest(),
    }
    checked = required_markdown_int(verification, "Checked Queries")
    if checked != actual["executed_queries"]:
        raise GateError("checked and executed readback counts differ")
    expected = load_json(expectations_path).get("readbacks")
    if actual != expected:
        raise GateError("canonical independent readbacks differ from the screen-frozen 4M authority")
    return actual


def canonical_validation(
    role: str,
    storage_path: Path,
    readbacks_path: Path,
    correctness_path: Path,
    corpus_path: Path,
    segments_manifest_path: Path,
    expectations_path: Path,
    binary_path: Path,
    screen_binding_path: Path,
    no_stats_build_path: Path,
) -> dict[str, Any]:
    if role not in {"stats-candidate", "no-stats-candidate"}:
        raise GateError("canonical validation role is invalid")
    validation_root = storage_path.parent
    scan_paths = {
        "before_storage": validation_root / "processes-before-storage.json",
        "before_readbacks": validation_root / "processes-before-readbacks.json",
        "after": validation_root / "processes-after.json",
    }
    for scan_path in scan_paths.values():
        validate_conflict_scan(scan_path)
    binding = load_json(screen_binding_path)
    no_stats = load_json(no_stats_build_path)
    expected_binary = (
        binding["binary_sha256"]["jemalloc"]
        if role == "stats-candidate"
        else no_stats["binary_sha256"]
    )
    if sha256_file(binary_path) != expected_binary:
        raise GateError("canonical validation refers to the wrong candidate binary")
    correctness = validate_correctness(correctness_path, expectations_path)
    corpus = validate_corpus(corpus_path, segments_manifest_path)
    storage = validate_storage_report(storage_path, expectations_path)
    if storage["samples"] != correctness["general"]["Recorded Samples"]:
        raise GateError("canonical storage sample count differs from replay correctness")
    readbacks = validate_readbacks(readbacks_path, expectations_path)
    return {
        "schema": VALIDATION_SCHEMA,
        "role": role,
        "complete": True,
        "binary_sha256": expected_binary,
        "screen_binding_sha256": sha256_file(screen_binding_path),
        "no_stats_build_sha256": sha256_file(no_stats_build_path),
        "correctness_sha256": sha256_file(correctness_path),
        "corpus": corpus,
        "corpus_summary_sha256": sha256_file(corpus_path),
        "segments_manifest_sha256": sha256_file(segments_manifest_path),
        "storage": storage,
        "storage_report_sha256": sha256_file(storage_path),
        "readbacks": readbacks,
        "readbacks_report_sha256": sha256_file(readbacks_path),
        "static_scan_sha256": {
            name: sha256_file(path) for name, path in scan_paths.items()
        },
        "production_promotion_authorized": False,
    }


def walk_regular_tree(root: Path) -> tuple[list[Path], list[Path]]:
    directory_non_symlink(root, "authority root")
    files: list[Path] = []
    directories: list[Path] = []

    def visit(directory: Path) -> None:
        directories.append(directory)
        try:
            with os.scandir(directory) as scanned:
                children = sorted(scanned, key=lambda entry: os.fsencode(entry.name))
        except OSError as error:
            raise GateError(f"cannot enumerate authority directory {directory}: {error}") from error
        for child in children:
            path = Path(child.path)
            try:
                mode = path.lstat().st_mode
            except OSError as error:
                raise GateError(f"cannot inspect authority path {path}: {error}") from error
            if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
                visit(path)
            elif stat.S_ISREG(mode) and not stat.S_ISLNK(mode):
                files.append(path)
            else:
                raise GateError(f"authority tree contains a symlink or special entry: {path}")

    visit(root)
    return files, directories


def create_raw_authority(root: Path, output: Path) -> dict[str, Any]:
    if output.exists() or output.is_symlink():
        raise GateError("raw authority output already exists")
    if root == output or output.is_relative_to(root):
        raise GateError("raw authority must be outside the sealed root")
    files, directories = walk_regular_tree(root)
    if not files:
        raise GateError("raw authority root contains no files")
    for path in files:
        executable = bool(path.stat().st_mode & 0o111)
        path.chmod(0o555 if executable else 0o444)
    for path in sorted(directories, key=lambda value: len(value.parts), reverse=True):
        path.chmod(0o555)
    files, directories = walk_regular_tree(root)
    rows: list[dict[str, Any]] = []
    for path in directories:
        relative = "." if path == root else path.relative_to(root).as_posix()
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode != 0o555:
            raise GateError(f"sealed raw directory has an invalid mode: {relative}")
        rows.append(
            {
                "kind": "directory",
                "path": relative,
                "mode": "0555",
                "size_bytes": "-",
                "sha256": "-",
            }
        )
    for path in files:
        relative = path.relative_to(root).as_posix()
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode not in {0o444, 0o555}:
            raise GateError(f"sealed raw file has an invalid mode: {relative}")
        rows.append(
            {
                "kind": "file",
                "path": relative,
                "mode": f"{mode:04o}",
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    rows.sort(key=lambda row: (os.fsencode(row["path"]), os.fsencode(row["kind"])))
    with output.open("x", encoding="utf-8") as destination:
        destination.write(f"schema\t{AUTHORITY_SCHEMA}\n")
        destination.write(f"root\t{root.resolve(strict=True)}\n")
        destination.write("kind\tpath\tmode\tsize_bytes\tsha256\n")
        for row in rows:
            destination.write(
                f"{row['kind']}\t{row['path']}\t{row['mode']}\t"
                f"{row['size_bytes']}\t{row['sha256']}\n"
            )
    output.chmod(0o444)
    created = {
        "schema": AUTHORITY_SCHEMA,
        "root": str(root.resolve(strict=True)),
        "file_count": len(files),
        "directory_count": len(directories),
        "authority_sha256": sha256_file(output),
    }
    checked = check_raw_authority(output)
    if checked != created:
        raise GateError("fresh raw authority failed immediate self-validation")
    return created


def check_raw_authority(path: Path) -> dict[str, Any]:
    regular_non_symlink(path, "raw authority")
    if stat.S_IMODE(path.stat().st_mode) != 0o444:
        raise GateError("raw authority must have exact mode 0444")
    lines = path.read_text(encoding="utf-8", errors="strict").splitlines()
    if len(lines) < 4 or lines[0] != f"schema\t{AUTHORITY_SCHEMA}" or not lines[1].startswith("root\t"):
        raise GateError("raw authority header is malformed")
    if lines[2] != "kind\tpath\tmode\tsize_bytes\tsha256":
        raise GateError("raw authority columns changed")
    root = Path(lines[1].split("\t", 1)[1])
    if not root.is_absolute():
        raise GateError("raw authority root is not absolute")
    files, directories = walk_regular_tree(root)
    actual_paths = {item.relative_to(root).as_posix(): item for item in files}
    actual_directories = {
        ("." if item == root else item.relative_to(root).as_posix()): item
        for item in directories
    }
    expected_paths: set[str] = set()
    expected_directories: set[str] = set()
    previous: tuple[bytes, bytes] | None = None
    for line_number, line in enumerate(lines[3:], start=4):
        fields = line.split("\t")
        if len(fields) != 5:
            raise GateError(f"raw authority row {line_number} is malformed")
        kind, relative, mode_text, size_text, digest = fields
        if kind not in {"directory", "file"}:
            raise GateError("raw authority contains an unknown entry kind")
        candidate = Path(relative)
        if (
            relative != "."
            and (
                candidate.is_absolute()
                or not candidate.parts
                or candidate.as_posix() != relative
                or any(part in {"", ".", ".."} for part in candidate.parts)
            )
        ):
            raise GateError("raw authority contains an unsafe path")
        encoded = (os.fsencode(relative), os.fsencode(kind))
        if previous is not None and encoded <= previous:
            raise GateError("raw authority paths are not in strict byte order")
        previous = encoded
        if kind == "directory":
            if (
                relative in expected_directories
                or relative not in actual_directories
                or mode_text != "0555"
                or size_text != "-"
                or digest != "-"
                or stat.S_IMODE(actual_directories[relative].stat().st_mode) != 0o555
            ):
                raise GateError(f"raw authority directory changed: {relative}")
            expected_directories.add(relative)
        else:
            if relative == "." or relative in expected_paths:
                raise GateError("raw authority contains a duplicate/invalid file path")
            expected_paths.add(relative)
            if mode_text not in {"0444", "0555"} or re.fullmatch(r"[0-9]+", size_text) is None:
                raise GateError("raw authority contains an invalid file mode or size")
            strict_sha256(digest, "$.raw_authority.sha256")
            current = actual_paths.get(relative)
            if current is None:
                raise GateError(f"raw authority file is missing: {relative}")
            if (
                stat.S_IMODE(current.stat().st_mode) != int(mode_text, 8)
                or current.stat().st_size != int(size_text)
                or sha256_file(current) != digest
            ):
                raise GateError(f"raw authority file changed: {relative}")
    if not expected_paths or expected_paths != set(actual_paths):
        raise GateError("raw authority file set is not exact")
    if expected_directories != set(actual_directories):
        raise GateError("raw authority directory set is not exact")
    return {
        "schema": AUTHORITY_SCHEMA,
        "root": str(root.resolve(strict=True)),
        "file_count": len(expected_paths),
        "directory_count": len(expected_directories),
        "authority_sha256": sha256_file(path),
    }


def run_label(stage: str, position: int) -> str:
    token = EXPECTED_STAGE_SCHEDULES[stage][position - 1]
    return f"{stage}-{position:02d}-{token}"


def recompute_preflights(
    result_root: Path,
    screen_binding_path: Path,
    system_binary: Path,
    stats_binary: Path,
    no_stats_binary: Path,
) -> dict[str, dict[str, Any]]:
    paths = {
        "system": system_binary,
        "stats-candidate": stats_binary,
        "no-stats-candidate": no_stats_binary,
    }
    result = {}
    for role, binary in paths.items():
        stem = role
        raw = result_root / "metadata/preflight" / f"{stem}.application.json"
        saved = result_root / "metadata/preflight" / f"{stem}.json"
        current = validate_application_preflight(
            raw,
            result_root / "metadata/preflight" / f"{stem}.stderr",
            role,
            binary,
            screen_binding_path,
        )
        if load_json(saved) != current:
            raise GateError(f"saved {role} preflight differs from raw application output")
        result[role] = current
    return result


def recompute_observations(
    result_root: Path,
    screen_binding_path: Path,
    no_stats_build_path: Path,
    system_binary: Path,
    stats_binary: Path,
    no_stats_binary: Path,
    expectations_path: Path,
    plan_path: Path,
) -> tuple[list[Path], list[Path], list[dict[str, Any]]]:
    stats_paths: list[Path] = []
    no_stats_paths: list[Path] = []
    values = []
    role_binary = {
        "system": system_binary,
        "stats-candidate": stats_binary,
        "no-stats-candidate": no_stats_binary,
    }
    for stage in ("stats", "no-stats"):
        for position in range(1, 5):
            _token, role = expected_observation_role(stage, position, validate_plan(plan_path))
            label = run_label(stage, position)
            root = result_root / "runs" / label
            reports = sorted(root.glob("ingestion_stats_*.md"), key=lambda path: os.fsencode(path.name))
            if len(reports) != 1 or reports[0].parent != root:
                raise GateError(f"raw run must contain exactly one ingestion report: {label}")
            authority = result_root / "metadata/raw-authorities" / f"{label}.tsv"
            checked = check_raw_authority(authority)
            if checked["root"] != str(root.resolve(strict=True)):
                raise GateError(f"raw authority root differs for {label}")
            saved_path = root / "observation.json"
            current = make_observation(
                stage=stage,
                position=position,
                binary_path=role_binary[role],
                screen_binding_path=screen_binding_path,
                no_stats_build_path=no_stats_build_path,
                preflight_path=result_root / "metadata/preflight" / f"{role}.json",
                runtime_log_path=root / "replay.log",
                checkpoint_path=root / "allocator-release-checkpoint.tsv",
                telemetry_path=root / "allocator-release-telemetry.ndjson",
                rss_path=root / "rss-summary.json",
                rss_samples_path=root / "rss-samples.tsv",
                timing_raw_path=root / "replay.time.txt",
                timing_path=root / "replay.time.json",
                perf_raw_path=root / "perf-stat.tsv",
                perf_path=root / "perf-stat.json",
                guardian_path=root / "external-conflict-guardian.json",
                capacity_path=root / "run-capacity.json",
                pre_quiescence_samples_path=root
                / "pre-run-writeback-quiescence-samples.tsv",
                pre_quiescence_path=root / "pre-run-writeback-quiescence.json",
                post_quiescence_samples_path=root
                / "post-run-writeback-quiescence-samples.tsv",
                post_quiescence_path=root / "post-run-writeback-quiescence.json",
                replay_report_path=reports[0],
                correctness_path=root / "replay-correctness.json",
                corpus_path=root / "corpus-summary.json",
                segments_manifest_path=root / "segments.sha256",
                segments_inventory_path=root / "segments.tsv",
                capture_residency_before_path=root / "capture-residency-before.tsv",
                capture_residency_after_path=root / "capture-residency-after.tsv",
                capture_inputs_path=result_root
                / "metadata/input-controls/capture-inputs-before.json",
                expectations_path=expectations_path,
                plan_path=plan_path,
            )
            if load_json(saved_path) != current:
                raise GateError(f"saved observation differs from raw evidence: {label}")
            (stats_paths if stage == "stats" else no_stats_paths).append(saved_path)
            values.append(current)
    return stats_paths, no_stats_paths, values


def recompute_validations(
    result_root: Path,
    screen_binding_path: Path,
    no_stats_build_path: Path,
    stats_binary: Path,
    no_stats_binary: Path,
    expectations_path: Path,
) -> dict[str, dict[str, Any]]:
    result = {}
    definitions = {
        "stats-candidate": (
            result_root / "validation/stats-candidate",
            result_root / "runs/stats-02-C",
            stats_binary,
        ),
        "no-stats-candidate": (
            result_root / "validation/no-stats-candidate",
            result_root / "runs/no-stats-02-N",
            no_stats_binary,
        ),
    }
    for role, (root, run_root, binary) in definitions.items():
        authority = result_root / "metadata/raw-authorities" / f"validation-{role}.tsv"
        checked = check_raw_authority(authority)
        if checked["root"] != str(root.resolve(strict=True)):
            raise GateError(f"canonical validation authority root differs for {role}")
        current = canonical_validation(
            role,
            root / "storage-verify.json",
            root / "readbacks.md",
            run_root / "replay-correctness.json",
            run_root / "corpus-summary.json",
            run_root / "segments.sha256",
            expectations_path,
            binary,
            screen_binding_path,
            no_stats_build_path,
        )
        if load_json(root / "validation.json") != current:
            raise GateError(f"saved canonical validation differs from raw reports: {role}")
        result[role] = current
    left = result["stats-candidate"]
    right = result["no-stats-candidate"]
    for key in ("correctness_sha256", "corpus", "segments_manifest_sha256", "storage", "readbacks"):
        if left[key] != right[key]:
            raise GateError(f"stats/no-stats canonical validation differs for {key}")
    return result


def admit_result(
    result_root: Path,
    screen_binding_path: Path,
    no_stats_build_path: Path,
    system_binary: Path,
    stats_binary: Path,
    no_stats_binary: Path,
    expectations_path: Path,
    plan_path: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path)
    result_root = directory_non_symlink(result_root, "full-gate result root").resolve(strict=True)
    binding = screen_binding(Path(load_json(screen_binding_path)["screen_root"]), plan_path)
    if load_json(screen_binding_path) != binding:
        raise GateError("saved screen binding differs from the completed screen authorities")
    validate_capacity_evidence(
        result_root / "metadata/input-controls/capacity.json",
        result_root,
        expectations_path,
        plan_path,
    )
    validate_conflict_scan(
        result_root / "metadata/input-controls/processes-after-build.json"
    )
    validate_perf_preflight(result_root / "metadata/input-controls")
    if (
        result_root / "metadata/input-controls/quiet-host-confirmed.txt"
    ).read_bytes() != b"1\n":
        raise GateError("formal result lacks exact QUIET_HOST_CONFIRMED=1 evidence")
    recompute_preflights(
        result_root, screen_binding_path, system_binary, stats_binary, no_stats_binary
    )
    build_log = result_root / "metadata/build/no-stats.log"
    current_build = no_stats_build_provenance(
        screen_binding_path,
        plan_path,
        build_log,
        no_stats_binary,
        result_root / "metadata/preflight/no-stats-candidate.json",
        result_root / "build-target",
        result_root / "metadata/build/toolchain-binding.json",
        result_root / "metadata/build/screen-validation-after-no-stats-build.json",
    )
    if load_json(no_stats_build_path) != current_build:
        raise GateError("saved no-stats build provenance differs from raw build evidence")
    stats_paths, no_stats_paths, observations = recompute_observations(
        result_root,
        screen_binding_path,
        no_stats_build_path,
        system_binary,
        stats_binary,
        no_stats_binary,
        expectations_path,
        plan_path,
    )
    baseline = observations[0]
    for observation in observations[1:]:
        if observation["correctness"] != baseline["correctness"] or observation["corpus"] != baseline["corpus"]:
            raise GateError("eight 4M observations are not semantically and byte equivalent")
    stats = compare_stage(stats_paths, "stats", plan_path)
    no_stats = compare_stage(no_stats_paths, "no-stats", plan_path)
    saved_stats = result_root / "comparisons/stats-stage-decision.json"
    saved_no_stats = result_root / "comparisons/no-stats-stage-decision.json"
    if load_json(saved_stats) != stats or load_json(saved_no_stats) != no_stats:
        raise GateError("saved stage decision differs from raw observations")
    validations = recompute_validations(
        result_root,
        screen_binding_path,
        no_stats_build_path,
        stats_binary,
        no_stats_binary,
        expectations_path,
    )
    capture_before = result_root / "metadata/input-controls/capture-inputs-before.json"
    capture_after = result_root / "metadata/final-controls/capture-inputs-after.json"
    if capture_before.read_bytes() != capture_after.read_bytes():
        raise GateError("source capture/config authority changed during the full gate")
    if sha256_file(expectations_path) != binding["phase1_expectations_sha256"]:
        raise GateError("full gate used a different Phase 1 expectations authority")
    authorities = {}
    for path in sorted((result_root / "metadata/raw-authorities").glob("*.tsv")):
        checked = check_raw_authority(path)
        authorities[path.name] = checked["authority_sha256"]
    required_authorities = {
        *(f"{run_label(stage, position)}.tsv" for stage in ("stats", "no-stats") for position in range(1, 5)),
        "validation-stats-candidate.tsv",
        "validation-no-stats-candidate.tsv",
        "harness.tsv",
        "configs.tsv",
        "preflight.tsv",
        "build.tsv",
        "binaries.tsv",
        "input-controls.tsv",
        "final-controls.tsv",
    }
    if not required_authorities <= set(authorities):
        raise GateError("full gate lacks one or more raw-leaf authorities")
    eligible = stats["passed"] and no_stats["passed"]
    return {
        "schema": FINAL_SCHEMA,
        "complete_raw_admission": True,
        "selected_policy": binding["selected_policy"],
        "selected_jemalloc_conf": binding["selected_jemalloc_conf"],
        "screen_binding_sha256": sha256_file(screen_binding_path),
        "screen_final_decision_sha256": binding["screen_final_decision_sha256"],
        "source_archive_sha256": binding["source_archive_sha256"],
        "git_head": binding["git_head"],
        "system_binary_sha256": binding["binary_sha256"]["system"],
        "stats_candidate_binary_sha256": binding["binary_sha256"]["jemalloc"],
        "no_stats_candidate_binary_sha256": current_build["binary_sha256"],
        "workload_messages_per_observation": 4_000_000,
        "observation_count": 8,
        "all_replay_correctness_identical": True,
        "all_corpus_manifests_identical": True,
        "stats_stage": stats,
        "no_stats_stage": no_stats,
        "canonical_validations": validations,
        "capture_inventory_sha256": sha256_file(capture_before),
        "raw_authority_sha256": authorities,
        "both_full_gates_passed": eligible,
        "eligible_for_manual_promotion_review": eligible,
        "manual_review_required": True,
        "partial_runs_promotable": False,
        "production_promotion_authorized": False,
        "harness_can_change_default_allocator": False,
        "decision_scope": (
            "source-bound stats-enabled and plain-jemalloc 4M evidence only; "
            "an explicit reviewed code/spec decision is still required"
        ),
        "plan_sha256": sha256_file(plan_path),
    }


FINAL_AUTHORITY_FILES = {
    "metadata/result-artifact-files.nul",
    "metadata/result-directories.nul",
    "metadata/result-artifacts.tsv",
    "metadata/FINAL_SEAL_VALIDATED.json",
}
RUN_ROOT_FILES = {
    "allocator-release-checkpoint.tsv",
    "allocator-release-telemetry.ndjson",
    "capture-residency-after.tsv",
    "capture-residency-before.tsv",
    "command.txt",
    "corpus-summary.json",
    "external-conflict-guardian.exit-status",
    "external-conflict-guardian-control.json",
    "external-conflict-guardian.json",
    "external-conflict-guardian-launch",
    "external-conflict-guardian.log",
    "external-conflict-guardian-ready",
    "observation.json",
    "perf-stat.json",
    "perf-stat.tsv",
    "post-run-writeback-quiescence-samples.tsv",
    "post-run-writeback-quiescence.json",
    "post-run-writeback-quiescence.log",
    "pre-run-writeback-quiescence-samples.tsv",
    "pre-run-writeback-quiescence.json",
    "pre-run-writeback-quiescence.log",
    "processes-after.json",
    "processes-before.json",
    "processes-immediately-before-launch.json",
    "replay-correctness.json",
    "replay.exit-status",
    "replay.log",
    "replay.time.json",
    "replay.time.txt",
    "rss-monitor.exit-status",
    "rss-monitor.log",
    "rss-monitor-ready",
    "rss-samples.tsv",
    "rss-summary.json",
    "run-capacity.json",
    "segments.sha256",
    "segments.tsv",
}
VALIDATION_ROOT_FILES = {
    "processes-after.json",
    "processes-before-readbacks.json",
    "processes-before-storage.json",
    "readbacks.log",
    "readbacks.md",
    "readbacks.time.txt",
    "storage-verify.json",
    "storage-verify.log",
    "storage-verify.time.txt",
    "validation.json",
}


def evidence_tree(root: Path) -> tuple[list[str], list[Path]]:
    root = directory_non_symlink(root, "full-gate result root").resolve(strict=True)
    build_target = directory_non_symlink(
        root / "build-target", "non-evidence Cargo build target"
    ).resolve(strict=True)
    directories: list[str] = []
    files: list[Path] = []

    def visit(directory: Path) -> None:
        try:
            with os.scandir(directory) as scanned:
                children = sorted(scanned, key=lambda entry: os.fsencode(entry.name))
        except OSError as error:
            raise GateError(f"cannot enumerate result evidence directory {directory}: {error}") from error
        for child in children:
            if any(character in child.name for character in ("\n", "\r", "\t")):
                raise GateError(f"unsafe result evidence path component: {child.name!r}")
            path = Path(child.path)
            mode = path.lstat().st_mode
            relative = path.relative_to(root).as_posix()
            if stat.S_ISLNK(mode):
                raise GateError(f"result evidence contains a symlink: {relative}")
            if stat.S_ISDIR(mode):
                directories.append(relative)
                if path.resolve(strict=True) != build_target:
                    visit(path)
            elif stat.S_ISREG(mode):
                files.append(path)
            else:
                raise GateError(f"result evidence contains a special entry: {relative}")

    visit(root)
    directories.sort(key=os.fsencode)
    files.sort(key=lambda path: os.fsencode(path.relative_to(root).as_posix()))
    return directories, files


def expected_run_labels() -> list[str]:
    return [
        run_label(stage, position)
        for stage in ("stats", "no-stats")
        for position in range(1, 5)
    ]


def validate_result_tree_shape(
    root: Path, directories: list[str], evidence_files: list[str]
) -> None:
    labels = expected_run_labels()
    base_directories = {
        "build-target",
        "comparisons",
        "configs",
        "metadata",
        "metadata/binaries",
        "metadata/build",
        "metadata/final-controls",
        "metadata/harness",
        "metadata/input-controls",
        "metadata/preflight",
        "metadata/raw-authorities",
        "runs",
        "validation",
        "validation/stats-candidate",
        "validation/no-stats-candidate",
        *(f"runs/{label}" for label in labels),
        *(f"runs/{label}/segments" for label in labels),
    }
    dynamic_segment_directories = {
        path
        for path in directories
        if any(path.startswith(f"runs/{label}/segments/") for label in labels)
    }
    if set(directories) - dynamic_segment_directories != base_directories:
        raise GateError("final result directory matrix is not exact")

    files = set(evidence_files)
    consumed = {"PARTIAL_UNLESS_COMPLETE.txt", "run-plan.tsv"}
    if not consumed <= files:
        raise GateError("final result root files are missing")
    config_files = {
        path
        for label in labels
        for path in (f"configs/{label}.toml", f"configs/{label}.render.json")
    }
    comparison_files = {
        "comparisons/stats-stage-decision.json",
        "comparisons/no-stats-stage-decision.json",
        "comparisons/final-full-gate-decision.json",
    }
    harness_files = {
        f"metadata/harness/{name}"
        for name in (
            "phase5_allocator_full_gate.py",
            "phase5_allocator_full_plan.json",
            "phase5_allocator_full_run.sh",
            "test_phase5_allocator_full_gate.py",
        )
    }
    preflight_files = {
        f"metadata/preflight/{role}{suffix}"
        for role in ("system", "stats-candidate", "no-stats-candidate")
        for suffix in (".application.json", ".stderr", ".json")
    }
    build_files = {
        f"metadata/build/{name}"
        for name in (
            "no-stats-build.json",
            "no-stats.elf-notes.txt",
            "no-stats.file.txt",
            "no-stats.log",
            "screen-validation-after-no-stats-build.json",
            "toolchain-binding.json",
        )
    }
    binary_files = {
        f"metadata/binaries/{name}"
        for name in (
            "chronoxide-ingester-system",
            "chronoxide-ingester-jemalloc-stats",
            "chronoxide-ingester-jemalloc",
            "chronoxide-query",
            "chronoxide-storage-verify",
        )
    }
    input_files = {
        f"metadata/input-controls/{name}"
        for name in (
            "capacity.json",
            "capture-inputs-before.json",
            "perf-stat-preflight.exit-status",
            "perf-stat-preflight.json",
            "perf-stat-preflight.log",
            "perf-stat-preflight.tsv",
            "processes-after-build.json",
            "python-interpreter.txt",
            "quiet-host-confirmed.txt",
            "run-note.txt",
            "screen-binding.json",
        )
    }
    final_control_files = {
        "metadata/final-controls/capture-inputs-after.json",
        "metadata/final-controls/finished-at.txt",
    }
    raw_authority_files = {
        f"metadata/raw-authorities/{name}.tsv"
        for name in (
            "harness",
            "preflight",
            "build",
            "binaries",
            "input-controls",
            "configs",
            "final-controls",
            "validation-stats-candidate",
            "validation-no-stats-candidate",
            *labels,
        )
    }
    fixed = (
        config_files
        | comparison_files
        | harness_files
        | preflight_files
        | build_files
        | binary_files
        | input_files
        | final_control_files
        | raw_authority_files
    )
    if not fixed <= files:
        raise GateError(f"final fixed file matrix is missing: {sorted(fixed - files)!r}")
    consumed |= fixed

    for label in labels:
        prefix = f"runs/{label}/"
        root_files = {
            path[len(prefix) :]
            for path in files
            if path.startswith(prefix) and "/" not in path[len(prefix) :]
        }
        reports = {
            path
            for path in root_files
            if re.fullmatch(r"ingestion_stats_[A-Za-z0-9_.-]+\.md", path)
        }
        if len(reports) != 1 or root_files != RUN_ROOT_FILES | reports:
            raise GateError(f"run root file matrix differs: {label}")
        consumed.update(prefix + path for path in root_files)
        segment_files = {path for path in files if path.startswith(prefix + "segments/")}
        if not segment_files:
            raise GateError(f"run segment corpus is empty: {label}")
        consumed.update(segment_files)
    for role in ("stats-candidate", "no-stats-candidate"):
        prefix = f"validation/{role}/"
        observed = {path[len(prefix) :] for path in files if path.startswith(prefix)}
        if observed != VALIDATION_ROOT_FILES:
            raise GateError(f"canonical validation file matrix differs: {role}")
        consumed.update(prefix + path for path in observed)
    if files != consumed:
        raise GateError(f"final result contains unexpected evidence files: {sorted(files - consumed)!r}")


def write_nul_inventory(path: Path, values: list[str]) -> None:
    with path.open("xb") as destination:
        destination.write(b"".join(os.fsencode(value) + b"\0" for value in values))


def parse_nul_inventory(path: Path, description: str) -> list[str]:
    regular_non_symlink(path, description)
    raw = path.read_bytes()
    if not raw or not raw.endswith(b"\0"):
        raise GateError(f"{description} is empty or not NUL terminated")
    try:
        values = [os.fsdecode(item) for item in raw[:-1].split(b"\0")]
    except UnicodeDecodeError as error:
        raise GateError(f"{description} contains invalid path bytes") from error
    if values != sorted(values, key=os.fsencode) or len(values) != len(set(values)):
        raise GateError(f"{description} is not unique strict byte order")
    for value in values:
        candidate = Path(value)
        if (
            candidate.is_absolute()
            or not candidate.parts
            or candidate.as_posix() != value
            or any(part in {"", ".", ".."} for part in candidate.parts)
        ):
            raise GateError(f"{description} contains an unsafe path")
    return values


def final_authority_paths(root: Path) -> dict[str, Path]:
    return {relative: root / relative for relative in FINAL_AUTHORITY_FILES}


def create_artifact_manifest(root: Path, output: Path) -> dict[str, Any]:
    root = directory_non_symlink(root, "full-gate result root").resolve(strict=True)
    canonical = final_authority_paths(root)
    if output != canonical["metadata/result-artifacts.tsv"]:
        raise GateError("result artifact manifest path is non-canonical")
    if any(path.exists() or path.is_symlink() for path in canonical.values()):
        raise GateError("final artifact authority already exists")
    if (root / "COMPLETE").exists() or (root / "COMPLETE").is_symlink():
        raise GateError("cannot seal an already completed result")
    directories, paths = evidence_tree(root)
    evidence_files = [path.relative_to(root).as_posix() for path in paths]
    validate_result_tree_shape(root, directories, evidence_files)
    for path in paths:
        executable = bool(path.stat().st_mode & 0o111)
        path.chmod(0o555 if executable else 0o444)
    for relative in sorted(directories, key=lambda value: len(Path(value).parts), reverse=True):
        if relative not in {"build-target", "metadata"}:
            (root / relative).chmod(0o555)
    paths.sort(key=lambda path: os.fsencode(path.relative_to(root).as_posix()))
    evidence_files = [path.relative_to(root).as_posix() for path in paths]
    files_path = canonical["metadata/result-artifact-files.nul"]
    directories_path = canonical["metadata/result-directories.nul"]
    write_nul_inventory(files_path, evidence_files)
    write_nul_inventory(directories_path, directories)
    with output.open("x", encoding="utf-8") as destination:
        destination.write(f"schema\t{ARTIFACT_SCHEMA}\n")
        destination.write(f"root\t{root}\n")
        destination.write("excluded_non_evidence\tbuild-target/**\n")
        destination.write("path\tmode\tsize_bytes\tsha256\n")
        for relative, path in zip(evidence_files, paths, strict=True):
            mode = stat.S_IMODE(path.stat().st_mode)
            if mode not in {0o444, 0o555}:
                raise GateError(f"result evidence file remains writable: {relative}")
            destination.write(
                f"{relative}\t{mode:04o}\t{path.stat().st_size}\t{sha256_file(path)}\n"
            )
    for path in (files_path, directories_path, output):
        path.chmod(0o444)
    return check_artifact_manifest(root, output, stage="precomplete")


def check_artifact_manifest(
    root: Path, path: Path, *, stage: str = "precomplete"
) -> dict[str, Any]:
    if stage not in {"precomplete", "complete"}:
        raise GateError("final artifact validation stage is invalid")
    root = directory_non_symlink(root, "full-gate result root").resolve(strict=True)
    canonical = final_authority_paths(root)
    if path != canonical["metadata/result-artifacts.tsv"]:
        raise GateError("result artifact manifest path changed")
    for relative in FINAL_AUTHORITY_FILES - {"metadata/FINAL_SEAL_VALIDATED.json"}:
        authority = regular_non_symlink(canonical[relative], f"final authority {relative}")
        if stat.S_IMODE(authority.stat().st_mode) != 0o444:
            raise GateError(f"final authority mode changed: {relative}")
    listed_files = parse_nul_inventory(
        canonical["metadata/result-artifact-files.nul"], "final file inventory"
    )
    listed_directories = parse_nul_inventory(
        canonical["metadata/result-directories.nul"], "final directory inventory"
    )
    directories, all_paths = evidence_tree(root)
    observed = [item.relative_to(root).as_posix() for item in all_paths]
    evidence_files = sorted(
        (
            relative
            for relative in observed
            if relative not in FINAL_AUTHORITY_FILES and relative != "COMPLETE"
        ),
        key=os.fsencode,
    )
    if evidence_files != listed_files:
        raise GateError("final file inventory does not exactly match the result tree")
    if directories != listed_directories:
        raise GateError("final directory inventory does not exactly match the result tree")
    validate_result_tree_shape(root, directories, listed_files)
    lines = path.read_text(encoding="utf-8", errors="strict").splitlines()
    if lines[:4] != [
        f"schema\t{ARTIFACT_SCHEMA}",
        f"root\t{root}",
        "excluded_non_evidence\tbuild-target/**",
        "path\tmode\tsize_bytes\tsha256",
    ]:
        raise GateError("result artifact manifest header changed")
    expected_lines = []
    for relative in listed_files:
        current = regular_non_symlink(root / relative, f"final evidence {relative}")
        mode = stat.S_IMODE(current.stat().st_mode)
        if mode not in {0o444, 0o555}:
            raise GateError(f"final evidence is writable: {relative}")
        expected_lines.append(
            f"{relative}\t{mode:04o}\t{current.stat().st_size}\t{sha256_file(current)}"
        )
    if lines[4:] != expected_lines:
        raise GateError("result artifact digest manifest is not exact")
    result = {
        "schema": ARTIFACT_SCHEMA,
        "stage": stage,
        "artifact_count": len(listed_files),
        "directory_count": len(listed_directories),
        "artifact_manifest_sha256": sha256_file(path),
        "file_inventory_sha256": sha256_file(
            canonical["metadata/result-artifact-files.nul"]
        ),
        "directory_inventory_sha256": sha256_file(
            canonical["metadata/result-directories.nul"]
        ),
        "excluded_non_evidence": ["build-target/**"],
    }
    certificate_path = canonical["metadata/FINAL_SEAL_VALIDATED.json"]
    if certificate_path.exists() or certificate_path.is_symlink():
        certificate_path = regular_non_symlink(
            certificate_path, "pre-completion final admission certificate"
        )
        if stat.S_IMODE(certificate_path.stat().st_mode) != 0o444:
            raise GateError("pre-completion final admission certificate is mutable")
        certificate = load_json(certificate_path)
        for key in (
            "artifact_manifest_sha256",
            "file_inventory_sha256",
            "directory_inventory_sha256",
            "artifact_count",
            "directory_count",
        ):
            if certificate.get(key) != result[key]:
                raise GateError("pre-completion certificate differs from artifact authorities")
        if (
            certificate.get("schema") != FINAL_ADMISSION_SCHEMA
            or certificate.get("stage") != "precomplete"
            or certificate.get("status") != "pass"
            or certificate.get("final_decision_sha256")
            != sha256_file(root / "comparisons/final-full-gate-decision.json")
            or certificate.get("production_promotion_authorized") is not False
        ):
            raise GateError("pre-completion final admission certificate is invalid")
    elif stage == "complete":
        raise GateError("completed result lacks its pre-completion admission certificate")
    complete_path = root / "COMPLETE"
    if stage == "precomplete":
        if complete_path.exists() or complete_path.is_symlink():
            raise GateError("COMPLETE exists during pre-completion admission")
    else:
        complete = regular_non_symlink(complete_path, "full-gate COMPLETE marker")
        if stat.S_IMODE(complete.stat().st_mode) != 0o444:
            raise GateError("full-gate COMPLETE marker is mutable")
        value = require_exact_keys(
            load_json(complete),
            {
                "schema",
                "final_decision_sha256",
                "precompletion_admission_sha256",
                "artifact_manifest_sha256",
                "file_inventory_sha256",
                "directory_inventory_sha256",
                "artifact_count",
                "directory_count",
                "production_promotion_authorized",
            },
            "$.complete",
        )
        expected_complete = {
            "schema": COMPLETION_SCHEMA,
            "final_decision_sha256": sha256_file(
                root / "comparisons/final-full-gate-decision.json"
            ),
            "precompletion_admission_sha256": sha256_file(certificate_path),
            "artifact_manifest_sha256": result["artifact_manifest_sha256"],
            "file_inventory_sha256": result["file_inventory_sha256"],
            "directory_inventory_sha256": result["directory_inventory_sha256"],
            "artifact_count": result["artifact_count"],
            "directory_count": result["directory_count"],
            "production_promotion_authorized": False,
        }
        if value != expected_complete:
            raise GateError("full-gate COMPLETE marker differs from final authorities")
        if stat.S_IMODE(root.stat().st_mode) != 0o555:
            raise GateError("completed result root remains writable")
        for relative in directories:
            if relative != "build-target" and stat.S_IMODE((root / relative).stat().st_mode) != 0o555:
                raise GateError(f"completed evidence directory remains writable: {relative}")
    return result


def finalize_result(
    result_root: Path,
    final_path: Path,
    artifact_path: Path,
    complete_path: Path,
    **admission_arguments: Any,
) -> dict[str, Any]:
    root = directory_non_symlink(result_root, "full-gate result root").resolve(strict=True)
    if complete_path != root / "COMPLETE" or complete_path.exists() or complete_path.is_symlink():
        raise GateError("COMPLETE path is non-canonical or already exists")
    recomputed = admit_result(result_root=root, **admission_arguments)
    if load_json(final_path) != recomputed:
        raise GateError("saved final decision differs from raw final admission")
    artifacts = check_artifact_manifest(root, artifact_path, stage="precomplete")
    certificate = {
        "schema": FINAL_ADMISSION_SCHEMA,
        "stage": "precomplete",
        "status": "pass",
        "final_decision_sha256": sha256_file(final_path),
        "artifact_manifest_sha256": artifacts["artifact_manifest_sha256"],
        "file_inventory_sha256": artifacts["file_inventory_sha256"],
        "directory_inventory_sha256": artifacts["directory_inventory_sha256"],
        "artifact_count": artifacts["artifact_count"],
        "directory_count": artifacts["directory_count"],
        "production_promotion_authorized": False,
    }
    certificate_path = root / "metadata/FINAL_SEAL_VALIDATED.json"
    write_json_exclusive(certificate_path, certificate)
    certificate_path.chmod(0o444)
    check_artifact_manifest(root, artifact_path, stage="precomplete")
    directories, _files = evidence_tree(root)
    for relative in sorted(directories, key=lambda value: len(Path(value).parts), reverse=True):
        if relative != "build-target":
            (root / relative).chmod(0o555)
    completion = {
        "schema": COMPLETION_SCHEMA,
        "final_decision_sha256": sha256_file(final_path),
        "precompletion_admission_sha256": sha256_file(certificate_path),
        "artifact_manifest_sha256": artifacts["artifact_manifest_sha256"],
        "file_inventory_sha256": artifacts["file_inventory_sha256"],
        "directory_inventory_sha256": artifacts["directory_inventory_sha256"],
        "artifact_count": artifacts["artifact_count"],
        "directory_count": artifacts["directory_count"],
        "production_promotion_authorized": False,
    }
    write_json_exclusive(complete_path, completion)
    complete_path.chmod(0o444)
    root.chmod(0o555)
    try:
        post_admission = admit_result(result_root=root, **admission_arguments)
        if post_admission != recomputed or load_json(final_path) != post_admission:
            raise GateError(
                "post-COMPLETE raw admission differs from the pre-completion decision"
            )
        check_artifact_manifest(root, artifact_path, stage="complete")
    except Exception:
        root.chmod(0o755)
        complete_path.unlink(missing_ok=True)
        raise
    return completion


def add_observation_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--stage", choices=("stats", "no-stats"), required=True)
    command.add_argument("--position", type=int, required=True)
    command.add_argument("--binary", type=Path, required=True)
    command.add_argument("--screen-binding", type=Path, required=True)
    command.add_argument("--no-stats-build", type=Path, required=True)
    command.add_argument("--preflight", type=Path, required=True)
    command.add_argument("--runtime-log", type=Path, required=True)
    command.add_argument("--checkpoint", type=Path, required=True)
    command.add_argument("--telemetry", type=Path, required=True)
    command.add_argument("--rss", type=Path, required=True)
    command.add_argument("--rss-samples", type=Path, required=True)
    command.add_argument("--time-raw", type=Path, required=True)
    command.add_argument("--time", type=Path, required=True)
    command.add_argument("--perf-raw", type=Path, required=True)
    command.add_argument("--perf", type=Path, required=True)
    command.add_argument("--guardian", type=Path, required=True)
    command.add_argument("--capacity", type=Path, required=True)
    command.add_argument("--pre-quiescence-samples", type=Path, required=True)
    command.add_argument("--pre-quiescence", type=Path, required=True)
    command.add_argument("--post-quiescence-samples", type=Path, required=True)
    command.add_argument("--post-quiescence", type=Path, required=True)
    command.add_argument("--replay-report", type=Path, required=True)
    command.add_argument("--correctness", type=Path, required=True)
    command.add_argument("--corpus", type=Path, required=True)
    command.add_argument("--segments-manifest", type=Path, required=True)
    command.add_argument("--segments-inventory", type=Path, required=True)
    command.add_argument("--capture-residency-before", type=Path, required=True)
    command.add_argument("--capture-residency-after", type=Path, required=True)
    command.add_argument("--capture-inputs", type=Path, required=True)
    command.add_argument("--expectations", type=Path, required=True)
    command.add_argument("--plan", type=Path, required=True)
    command.add_argument("--output", type=Path, required=True)


def add_admission_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--result-root", type=Path, required=True)
    command.add_argument("--screen-binding", type=Path, required=True)
    command.add_argument("--no-stats-build", type=Path, required=True)
    command.add_argument("--system-binary", type=Path, required=True)
    command.add_argument("--stats-binary", type=Path, required=True)
    command.add_argument("--no-stats-binary", type=Path, required=True)
    command.add_argument("--expectations", type=Path, required=True)
    command.add_argument("--plan", type=Path, required=True)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate-plan")
    validate.add_argument("--plan", type=Path, required=True)
    validate.add_argument("--output", type=Path)

    bind = commands.add_parser("bind-screen")
    bind.add_argument("--screen-result", type=Path, required=True)
    bind.add_argument("--plan", type=Path, required=True)
    bind.add_argument("--output", type=Path)

    check_binding = commands.add_parser("check-screen-binding")
    check_binding.add_argument("--binding", type=Path, required=True)
    check_binding.add_argument("--plan", type=Path, required=True)
    check_binding.add_argument("--full", action="store_true")
    check_binding.add_argument("--output", type=Path)

    capacity = commands.add_parser("check-capacity")
    capacity.add_argument("--result-parent", type=Path, required=True)
    capacity.add_argument("--expectations", type=Path, required=True)
    capacity.add_argument("--plan", type=Path, required=True)
    capacity.add_argument("--output", type=Path)

    run_capacity = commands.add_parser("check-run-capacity")
    run_capacity.add_argument("--filesystem", type=Path, required=True)
    run_capacity.add_argument("--stage", choices=("stats", "no-stats"), required=True)
    run_capacity.add_argument("--position", type=int, required=True)
    run_capacity.add_argument("--expectations", type=Path, required=True)
    run_capacity.add_argument("--plan", type=Path, required=True)
    run_capacity.add_argument("--first-corpus-summary", type=Path)
    run_capacity.add_argument("--output", type=Path, required=True)

    toolchain = commands.add_parser("bind-toolchain")
    toolchain.add_argument("--screen-environment", type=Path, required=True)
    toolchain.add_argument("--build-source", type=Path, required=True)
    toolchain.add_argument("--cargo", type=Path, required=True)
    toolchain.add_argument("--rustc", type=Path, required=True)
    toolchain.add_argument("--rustdoc", type=Path, required=True)
    toolchain.add_argument("--output", type=Path, required=True)

    scan = commands.add_parser("scan-conflicts")
    scan.add_argument("--output", type=Path)

    monitor = commands.add_parser("monitor-conflicts")
    monitor.add_argument("--pid", type=int, required=True)
    monitor.add_argument("--interval-ms", type=int, required=True)
    monitor.add_argument("--filesystem", type=Path, required=True)
    monitor.add_argument("--minimum-free-bytes", type=int, required=True)
    monitor.add_argument("--control", type=Path, required=True)
    monitor.add_argument("--ready", type=Path, required=True)
    monitor.add_argument("--launch", type=Path, required=True)
    monitor.add_argument("--output", type=Path, required=True)

    guardian_control = commands.add_parser("create-guardian-control")
    guardian_control.add_argument("--root-pid", type=int, required=True)
    guardian_control.add_argument("--guardian-pid", type=int, required=True)
    guardian_control.add_argument("--rss-monitor-pid", type=int, required=True)
    guardian_control.add_argument("--rss-ready", type=Path, required=True)
    guardian_control.add_argument("--interval-ms", type=int, required=True)
    guardian_control.add_argument("--ready", type=Path, required=True)
    guardian_control.add_argument("--launch", type=Path, required=True)
    guardian_control.add_argument("--output", type=Path, required=True)

    guardian_release = commands.add_parser("release-guardian-launch")
    guardian_release.add_argument("--control", type=Path, required=True)
    guardian_release.add_argument("--ready", type=Path, required=True)
    guardian_release.add_argument("--launch", type=Path, required=True)
    guardian_release.add_argument("--interval-ms", type=int, required=True)

    guardian_wait = commands.add_parser("wait-guardian-ready")
    guardian_wait.add_argument("--control", type=Path, required=True)
    guardian_wait.add_argument("--ready", type=Path, required=True)
    guardian_wait.add_argument("--launch", type=Path, required=True)
    guardian_wait.add_argument("--interval-ms", type=int, required=True)
    guardian_wait.add_argument("--timeout-ms", type=int, required=True)

    guardian_cleanup = commands.add_parser("cleanup-guardian-processes")
    guardian_cleanup.add_argument("--control", type=Path, required=True)
    guardian_cleanup.add_argument("--ready", type=Path, required=True)
    guardian_cleanup.add_argument("--launch", type=Path, required=True)
    guardian_cleanup.add_argument("--interval-ms", type=int, required=True)

    terminate = commands.add_parser("terminate-process-tree")
    terminate.add_argument("--root-pid", type=int, required=True)
    terminate.add_argument("--root-starttime-ticks", type=int, required=True)

    preflight = commands.add_parser("parse-preflight")
    preflight.add_argument("--raw", type=Path, required=True)
    preflight.add_argument("--stderr", type=Path, required=True)
    preflight.add_argument(
        "--role", choices=("system", "stats-candidate", "no-stats-candidate"), required=True
    )
    preflight.add_argument("--binary", type=Path, required=True)
    preflight.add_argument("--screen-binding", type=Path, required=True)
    preflight.add_argument("--output", type=Path, required=True)

    build = commands.add_parser("record-no-stats-build")
    build.add_argument("--screen-binding", type=Path, required=True)
    build.add_argument("--plan", type=Path, required=True)
    build.add_argument("--build-log", type=Path, required=True)
    build.add_argument("--binary", type=Path, required=True)
    build.add_argument("--preflight", type=Path, required=True)
    build.add_argument("--target-dir", type=Path, required=True)
    build.add_argument("--toolchain", type=Path, required=True)
    build.add_argument("--post-build-screen-validation", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)

    observation = commands.add_parser("make-observation")
    add_observation_arguments(observation)

    stage = commands.add_parser("compare-stage")
    stage.add_argument("--observation", type=Path, action="append", required=True)
    stage.add_argument("--stage", choices=("stats", "no-stats"), required=True)
    stage.add_argument("--plan", type=Path, required=True)
    stage.add_argument("--output", type=Path, required=True)

    completeness = commands.add_parser("check-storage-completeness")
    completeness.add_argument("--storage", type=Path, required=True)
    completeness.add_argument("--correctness", type=Path, required=True)
    completeness.add_argument("--expectations", type=Path, required=True)

    validation = commands.add_parser("validate-canonical")
    validation.add_argument(
        "--role", choices=("stats-candidate", "no-stats-candidate"), required=True
    )
    validation.add_argument("--storage", type=Path, required=True)
    validation.add_argument("--readbacks", type=Path, required=True)
    validation.add_argument("--correctness", type=Path, required=True)
    validation.add_argument("--corpus", type=Path, required=True)
    validation.add_argument("--segments-manifest", type=Path, required=True)
    validation.add_argument("--expectations", type=Path, required=True)
    validation.add_argument("--binary", type=Path, required=True)
    validation.add_argument("--screen-binding", type=Path, required=True)
    validation.add_argument("--no-stats-build", type=Path, required=True)
    validation.add_argument("--output", type=Path, required=True)

    seal_authority = commands.add_parser("seal-authority")
    seal_authority.add_argument("--root", type=Path, required=True)
    seal_authority.add_argument("--output", type=Path, required=True)

    check_authority = commands.add_parser("check-authority")
    check_authority.add_argument("--authority", type=Path, required=True)
    check_authority.add_argument("--output", type=Path)

    admit = commands.add_parser("admit-result")
    add_admission_arguments(admit)
    admit.add_argument("--output", type=Path, required=True)

    artifacts = commands.add_parser("seal-artifacts")
    artifacts.add_argument("--result-root", type=Path, required=True)
    artifacts.add_argument("--output", type=Path, required=True)

    validate_artifacts = commands.add_parser("validate-final-artifacts")
    validate_artifacts.add_argument("--result-root", type=Path, required=True)
    validate_artifacts.add_argument(
        "--stage", choices=("precomplete", "complete"), required=True
    )
    validate_artifacts.add_argument("--output", type=Path)

    finalize = commands.add_parser("finalize")
    add_admission_arguments(finalize)
    finalize.add_argument("--final-decision", type=Path, required=True)
    finalize.add_argument("--artifact-manifest", type=Path, required=True)
    finalize.add_argument("--complete", type=Path, required=True)
    return root


def output_or_print(value: Any, output: Path | None) -> None:
    if output is not None:
        write_json_exclusive(output, value)
    else:
        print(json.dumps(value, sort_keys=True))


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "validate-plan":
            output_or_print(validate_plan(args.plan), args.output)
        elif args.command == "bind-screen":
            output_or_print(screen_binding(args.screen_result, args.plan), args.output)
        elif args.command == "check-screen-binding":
            output_or_print(
                check_screen_binding(args.binding, args.plan, full=args.full), args.output
            )
        elif args.command == "check-capacity":
            output_or_print(
                capacity_evidence(args.result_parent, args.expectations, args.plan),
                args.output,
            )
        elif args.command == "check-run-capacity":
            output_or_print(
                run_capacity_evidence(
                    args.filesystem,
                    args.stage,
                    args.position,
                    args.expectations,
                    args.plan,
                    args.first_corpus_summary,
                ),
                args.output,
            )
        elif args.command == "bind-toolchain":
            output_or_print(
                toolchain_binding(
                    args.screen_environment,
                    args.build_source,
                    args.cargo,
                    args.rustc,
                    args.rustdoc,
                ),
                args.output,
            )
        elif args.command == "scan-conflicts":
            conflicts = scan_conflicts()
            result = {
                "schema": CONFLICT_SCAN_SCHEMA,
                "conflicts": conflicts,
                "quiet": not conflicts,
            }
            output_or_print(result, args.output)
            if conflicts:
                raise GateError(f"quiet-host preflight found conflicts: {conflicts!r}")
        elif args.command == "monitor-conflicts":
            monitor_conflicts(
                args.pid,
                args.output,
                args.interval_ms,
                args.filesystem,
                args.minimum_free_bytes,
                args.control,
                args.ready,
                args.launch,
            )
        elif args.command == "create-guardian-control":
            output_or_print(
                create_guardian_control(
                    args.output,
                    args.ready,
                    args.launch,
                    args.root_pid,
                    args.guardian_pid,
                    args.rss_monitor_pid,
                    args.interval_ms,
                    args.rss_ready,
                ),
                None,
            )
        elif args.command == "release-guardian-launch":
            output_or_print(
                release_guardian_launch(
                    args.control, args.ready, args.launch, args.interval_ms
                ),
                None,
            )
        elif args.command == "wait-guardian-ready":
            output_or_print(
                wait_for_guardian_ready(
                    args.control,
                    args.ready,
                    args.launch,
                    args.interval_ms,
                    args.timeout_ms,
                ),
                None,
            )
        elif args.command == "cleanup-guardian-processes":
            output_or_print(
                cleanup_guardian_processes(
                    args.control, args.ready, args.launch, args.interval_ms
                ),
                None,
            )
        elif args.command == "terminate-process-tree":
            termination = terminate_process_tree(
                args.root_pid, args.root_starttime_ticks
            )
            output_or_print(termination, None)
            require_clean_termination(termination, "identity-bound process tree")
        elif args.command == "parse-preflight":
            write_json_exclusive(
                args.output,
                validate_application_preflight(
                    args.raw,
                    args.stderr,
                    args.role,
                    args.binary,
                    args.screen_binding,
                ),
            )
        elif args.command == "record-no-stats-build":
            write_json_exclusive(
                args.output,
                no_stats_build_provenance(
                    args.screen_binding,
                    args.plan,
                    args.build_log,
                    args.binary,
                    args.preflight,
                    args.target_dir,
                    args.toolchain,
                    args.post_build_screen_validation,
                ),
            )
        elif args.command == "make-observation":
            write_json_exclusive(
                args.output,
                make_observation(
                    stage=args.stage,
                    position=args.position,
                    binary_path=args.binary,
                    screen_binding_path=args.screen_binding,
                    no_stats_build_path=args.no_stats_build,
                    preflight_path=args.preflight,
                    runtime_log_path=args.runtime_log,
                    checkpoint_path=args.checkpoint,
                    telemetry_path=args.telemetry,
                    rss_path=args.rss,
                    rss_samples_path=args.rss_samples,
                    timing_raw_path=args.time_raw,
                    timing_path=args.time,
                    perf_raw_path=args.perf_raw,
                    perf_path=args.perf,
                    guardian_path=args.guardian,
                    capacity_path=args.capacity,
                    pre_quiescence_samples_path=args.pre_quiescence_samples,
                    pre_quiescence_path=args.pre_quiescence,
                    post_quiescence_samples_path=args.post_quiescence_samples,
                    post_quiescence_path=args.post_quiescence,
                    replay_report_path=args.replay_report,
                    correctness_path=args.correctness,
                    corpus_path=args.corpus,
                    segments_manifest_path=args.segments_manifest,
                    segments_inventory_path=args.segments_inventory,
                    capture_residency_before_path=args.capture_residency_before,
                    capture_residency_after_path=args.capture_residency_after,
                    capture_inputs_path=args.capture_inputs,
                    expectations_path=args.expectations,
                    plan_path=args.plan,
                ),
            )
        elif args.command == "compare-stage":
            write_json_exclusive(
                args.output, compare_stage(args.observation, args.stage, args.plan)
            )
        elif args.command == "check-storage-completeness":
            print(
                json.dumps(
                    check_storage_completeness(
                        args.storage, args.correctness, args.expectations
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "validate-canonical":
            write_json_exclusive(
                args.output,
                canonical_validation(
                    args.role,
                    args.storage,
                    args.readbacks,
                    args.correctness,
                    args.corpus,
                    args.segments_manifest,
                    args.expectations,
                    args.binary,
                    args.screen_binding,
                    args.no_stats_build,
                ),
            )
        elif args.command == "seal-authority":
            print(json.dumps(create_raw_authority(args.root, args.output), sort_keys=True))
        elif args.command == "check-authority":
            output_or_print(check_raw_authority(args.authority), args.output)
        elif args.command == "admit-result":
            write_json_exclusive(
                args.output,
                admit_result(
                    args.result_root,
                    args.screen_binding,
                    args.no_stats_build,
                    args.system_binary,
                    args.stats_binary,
                    args.no_stats_binary,
                    args.expectations,
                    args.plan,
                ),
            )
        elif args.command == "seal-artifacts":
            print(
                json.dumps(
                    create_artifact_manifest(args.result_root, args.output), sort_keys=True
                )
            )
        elif args.command == "validate-final-artifacts":
            output_or_print(
                check_artifact_manifest(
                    args.result_root,
                    args.result_root / "metadata/result-artifacts.tsv",
                    stage=args.stage,
                ),
                args.output,
            )
        elif args.command == "finalize":
            completion = finalize_result(
                result_root=args.result_root,
                final_path=args.final_decision,
                artifact_path=args.artifact_manifest,
                complete_path=args.complete,
                screen_binding_path=args.screen_binding,
                no_stats_build_path=args.no_stats_build,
                system_binary=args.system_binary,
                stats_binary=args.stats_binary,
                no_stats_binary=args.no_stats_binary,
                expectations_path=args.expectations,
                plan_path=args.plan,
            )
            print(json.dumps(completion, sort_keys=True))
        else:
            raise AssertionError(args.command)
    except (
        GateError,
        OSError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        print(f"Phase 5 allocator full gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
