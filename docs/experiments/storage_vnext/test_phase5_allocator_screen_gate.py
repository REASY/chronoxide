#!/usr/bin/env python3

from __future__ import annotations

import ast
import copy
import hashlib
import json
import os
import py_compile
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import phase5_allocator_screen_gate as gate


HERE = Path(__file__).resolve().parent
PLAN = HERE / "phase5_allocator_screen_plan.json"
EXPECTATIONS = HERE / "phase1_4m_expectations.json"
PINNED_HELPERS = (
    "phase1_replay_gate.py",
    "ab_gate.py",
    "fadvise_regular_dontneed.c",
)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def copied_plan(root: Path) -> tuple[Path, Path]:
    plan = root / PLAN.name
    expectations = root / EXPECTATIONS.name
    shutil.copyfile(PLAN, plan)
    shutil.copyfile(EXPECTATIONS, expectations)
    for name in PINNED_HELPERS:
        shutil.copyfile(HERE / name, root / name)
    return plan, expectations


def executable(root: Path, name: str, payload: bytes) -> Path:
    path = root / name
    path.write_bytes(payload)
    path.chmod(0o555)
    return path


def application_preflight(policy: str) -> dict[str, object]:
    conf = gate.EXPECTED_CONFS[policy]
    if policy == "S":
        effective = None
        telemetry = "unavailable"
        allocator = "system"
    else:
        effective = {
            "abort_conf": False,
            "confirm_conf": False,
            "narenas": 8,
            "dirty_decay_ms": 10_000,
            "muzzy_decay_ms": 0,
            "background_thread": False,
            "max_background_threads": 4,
            "retain": True,
        }
        if conf is not None:
            effective.update(gate.requested_effective_entries(conf))
        telemetry = "fixed_startup_options_and_release_stats"
        allocator = "jemalloc"
    probe = (
        {
            "status": "unavailable_for_system_allocator",
            "allocation_bytes": None,
            "minimum_allocated_growth_bytes": None,
            "allocated_before_bytes": None,
            "allocated_while_live_bytes": None,
            "allocated_after_drop_bytes": None,
            "observed_allocated_growth_bytes": None,
            "passed": None,
        }
        if policy == "S"
        else {
            "status": "passed",
            "allocation_bytes": 64 * 1024 * 1024,
            "minimum_allocated_growth_bytes": 48 * 1024 * 1024,
            "allocated_before_bytes": 1024,
            "allocated_while_live_bytes": 64 * 1024 * 1024 + 1024,
            "allocated_after_drop_bytes": 1024,
            "observed_allocated_growth_bytes": 64 * 1024 * 1024,
            "passed": True,
        }
    )
    return {
        "schema": gate.PREFLIGHT_SCHEMA,
        "rust_global_allocator": allocator,
        "jemalloc_conf_env": "_RJEM_MALLOC_CONF",
        "requested_policy_raw": conf,
        "requested_policy_canonical": conf,
        "effective_policy": effective,
        "global_allocator_probe": probe,
        "allocator_internal_telemetry": telemetry,
        "ld_preload_present": False,
        "malloc_conf_present": False,
        "post_ingester_drop_hold_secs": 0,
        "post_ingester_drop_checkpoint_enabled": False,
        "post_ingester_drop_telemetry_enabled": False,
    }


def confirmation_text(policy: str) -> str:
    conf = gate.EXPECTED_CONFS[policy]
    if conf is None:
        return ""
    lines = [
        '<jemalloc>: malloc_conf #1 (string specified via --with-malloc-conf): ""',
        '<jemalloc>: malloc_conf #2 (string pointed to by the global variable malloc_conf): ""',
        '<jemalloc>: malloc_conf #3 ("name" of the file referenced by the symbolic link named /etc/malloc.conf): ""',
        '<jemalloc>: malloc_conf #4 (value of the environment variable MALLOC_CONF): '
        f'"{conf}"'
    ]
    lines.extend(f"<jemalloc>: -- Set conf value: {entry}" for entry in conf.split(","))
    lines.append(
        '<jemalloc>: malloc_conf #5 (string pointed to by the global variable malloc_conf_2_conf_harder): ""'
    )
    return "\n".join(lines) + "\n"


def j0_source_audit_text() -> str:
    return confirmation_text("J1").replace(
        gate.EXPECTED_CONFS["J1"], "abort_conf:true,confirm_conf:true"
    ).replace("<jemalloc>: -- Set conf value: narenas:4\n", "")


def runtime_log(policy: str) -> str:
    conf = gate.EXPECTED_CONFS[policy]
    allocator = "system" if policy == "S" else "jemalloc"
    runtime = {
        "schema": gate.RUNTIME_POLICY_SCHEMA,
        "rust_global_allocator": allocator,
        "jemalloc_conf_env": "_RJEM_MALLOC_CONF",
        "requested_policy_raw": conf,
        "requested_policy_canonical": conf,
        "effective_policy": application_preflight(policy)["effective_policy"],
        "post_ingester_drop_hold_secs": 30,
        "post_ingester_drop_checkpoint_enabled": True,
        "post_ingester_drop_telemetry_enabled": True,
    }
    return (
        confirmation_text(policy)
        + "CHRONOXIDE_ALLOCATOR_RUNTIME_POLICY_JSON="
        + json.dumps(runtime, separators=(",", ":"))
        + "\n"
        + "INFO Ingester state dropped; beginning diagnostic allocator release hold\n"
        + "INFO Diagnostic allocator release hold complete\n"
    )


def profile_runtime_log(policy: str) -> str:
    conf = gate.EXPECTED_CONFS[policy]
    runtime = {
        "schema": gate.RUNTIME_POLICY_SCHEMA,
        "rust_global_allocator": "jemalloc",
        "jemalloc_conf_env": "_RJEM_MALLOC_CONF",
        "requested_policy_raw": conf,
        "requested_policy_canonical": conf,
        "effective_policy": application_preflight(policy)["effective_policy"],
        "post_ingester_drop_hold_secs": 0,
        "post_ingester_drop_checkpoint_enabled": False,
        "post_ingester_drop_telemetry_enabled": False,
    }
    return (
        confirmation_text(policy)
        + "CHRONOXIDE_ALLOCATOR_RUNTIME_POLICY_JSON="
        + json.dumps(runtime, separators=(",", ":"))
        + "\n"
    )


def preflight_record(policy: str) -> dict[str, object]:
    allocator = "system" if policy == "S" else "jemalloc"
    return {
        "schema": gate.PREFLIGHT_RECORD_SCHEMA,
        "policy": policy,
        "binary_role": allocator,
        "binary_sha256": ("a" if policy == "S" else "b") * 64,
        "application": application_preflight(policy),
        "jemalloc_confirm_conf_verified": gate.EXPECTED_CONFS[policy] is not None,
        "jemalloc_config_sources_verified": policy != "S",
        "jemalloc_config_source_audit_sha256": None if policy == "S" else "9" * 64,
    }


def replay_correctness() -> dict[str, object]:
    return {
        "schema": "chronoxide/storage-vnext-replay-correctness/v2",
        "general": {
            "Total Messages": 250_000,
            "Total OTLP Metric Records": 10,
            "Total Unique Metrics (`__name__`)": 1,
            "Total Series (unique label sets)": 1,
            "Observed OTLP Datapoints": 100,
            "Accepted Datapoints": 90,
            "Skipped Non-Scalar": 0,
            "Recorded Samples": 88,
            "Missing Number Value": 2,
            "Invalid Typed Value": 0,
        },
        "datapoint_policy_totals": {
            "Observed": 100,
            "Time-Policy Accepted": 90,
            "Dropped Too Old": 5,
            "Dropped Too Future": 5,
            "Missing Timestamp": 0,
            "Rejected Total": 10,
        },
        "datapoint_storage_totals": {
            "Time-Policy Accepted": 90,
            "Recorded Samples": 88,
            "Missing Number Value": 2,
            "Invalid Typed Value": 0,
            "Accepted Not Recorded": 2,
        },
        "otlp_data_type_counts": {
            "Gauge": {
                "metric_records": 10,
                "observed_datapoints": 100,
                "accepted_datapoints": 90,
            },
            **{
                name: {
                    "metric_records": 0,
                    "observed_datapoints": 0,
                    "accepted_datapoints": 0,
                }
                for name in ("Sum", "Histogram", "Exponential Histogram", "Summary")
            },
        },
        "event_time_skew_ranges": {
            "All Timestamped": {"count": 100, "min_ms": -10, "max_ms": 10},
            "Accepted": {"count": 90, "min_ms": -5, "max_ms": 5},
            "Dropped Too Old": {"count": 5, "min_ms": -10, "max_ms": -6},
            "Dropped Too Future": {"count": 5, "min_ms": 6, "max_ms": 10},
        },
        "partition_watermarks": {
            "Tracked Messages": 250_000,
            "Tracked Datapoints": 100,
            "Missing Timestamp Messages": 0,
            "Missing Timestamp Datapoints": 0,
            "Overall Min TS": "2026-01-01T00:00:00Z",
            "Overall Max TS": "2026-01-01T00:00:01Z",
            "Overall Window": "00:00:01 (1000ms)",
        },
    }


def corpus_summary() -> dict[str, object]:
    return {
        "schema": gate.phase1.CORPUS_SUMMARY_SCHEMA,
        "file_count": 2,
        "size_bytes": 100,
        "manifest_sha256": "f" * 64,
    }


def root_only_guardian_fixture(
    root: Path, minimum_free_bytes: int
) -> tuple[Path, Path, Path, Path, dict[str, object]]:
    evidence_path = root / "guardian.json"
    control_path = root / "guardian-control.json"
    ready_path = root / "guardian-ready"
    launch_path = root / "guardian-launch"
    control = {
        "schema": gate.GUARDIAN_ROOT_CONTROL_SCHEMA,
        "root_pid": 123,
        "root_starttime_ticks": 123_000,
        "guardian_pid": 456,
        "guardian_starttime_ticks": 456_000,
        "interval_ms": 100,
        "ready_marker": str(ready_path),
        "launch_marker": str(launch_path),
    }
    write_json(control_path, control)
    control_path.chmod(0o444)
    for marker in (ready_path, launch_path):
        marker.touch()
        marker.chmod(0o444)
    value: dict[str, object] = {
        "schema": gate.GUARDIAN_SCHEMA,
        "root_pid": 123,
        "root_starttime_ticks": 123_000,
        "guardian_pid": 456,
        "interval_ms": 100,
        "polls": 3,
        "live_polls": 2,
        "terminal_poll": 3,
        "elapsed_ns": 202_000_000,
        "poll_monotonic_elapsed_ns": [1_000_000, 101_000_000, 201_000_000],
        "maximum_poll_start_gap_ns": 100_000_000,
        "maximum_allowed_poll_start_gap_ns": 200_000_000,
        "control_path": str(control_path),
        "control_sha256": gate.sha256_file(control_path),
        "ready_marker_path": str(ready_path),
        "ready_marker_sha256": gate.sha256_file(ready_path),
        "ready_created_poll": 1,
        "ready_created_monotonic_elapsed_ns": 1_000_000,
        "launch_marker_path": str(launch_path),
        "launch_marker_sha256": gate.sha256_file(launch_path),
        "launch_observed_poll": 2,
        "launch_observed_monotonic_elapsed_ns": 101_000_000,
        "launch_observed": True,
        "launch_observed_root_bound": True,
        "handshake_violations": [],
        "root_seen": True,
        "filesystem": str(root.resolve()),
        "minimum_free_bytes": minimum_free_bytes,
        "minimum_observed_free_bytes": minimum_free_bytes,
        "capacity_violations": [],
        "conflicts": [],
        "termination": {
            "attempted": False,
            "root_starttime_ticks": 123_000,
            "target_processes": [],
            "target_pids": [],
            "term_sent_pids": [],
            "term_errors": [],
            "kill_sent_pids": [],
            "kill_errors": [],
            "identity_refusals": [],
            "surviving_pids": [],
        },
        "complete_and_conflict_free": True,
    }
    return evidence_path, control_path, ready_path, launch_path, value


def capture_inventory() -> dict[str, object]:
    return {
        "capture": "/capture",
        "capture_manifest_sha256": "1" * 64,
        "capture_files": [
            {"name": "partition-1.capture", "size_bytes": 100, "sha256": "2" * 64}
        ],
        "config_template": "/config.toml",
        "config_template_sha256": "3" * 64,
        "stop_after_messages": 4_000_000,
    }


def source_seal_document() -> dict[str, object]:
    identity: dict[str, object] = {
        "git_head": "1" * 40,
        "git_head_tree": "2" * 40,
        "git_index_tree": "2" * 40,
        "tracked_input_count": 424,
        "tracked_input_manifest_sha256": "6" * 64,
        "git_index_flags_clear": True,
        "tracked_inputs_regular_files": True,
        "cargo_lock_sha256": "3" * 64,
        "tracked_cargo_configs": [
            {"path": ".cargo/config.toml", "sha256": "7" * 64, "size_bytes": 32}
        ],
        "ambient_cargo_configs_absent": True,
    }
    return {
        "schema": gate.SOURCE_SEAL_SCHEMA,
        "repo": "/tmp/chronoxide-source",
        **identity,
        "identity_sha256": hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "excluded_untracked_runtime_artifacts": [],
    }


def extracted_source_seal_document(
    source_root: str = "/tmp/build-source",
    archive_path: str = "/tmp/git-head.tar",
    archive_sha256: str = "e" * 64,
    live_source_seal_sha256: str = "8" * 64,
    live_source_identity_sha256: str = "9" * 64,
) -> dict[str, object]:
    return {
        "schema": gate.EXTRACTED_SOURCE_SEAL_SCHEMA,
        "repo": "/tmp/chronoxide-source",
        "source_root": source_root,
        "archive_path": archive_path,
        "archive_sha256": archive_sha256,
        "archive_size_bytes": 1024,
        "archive_embedded_commit": "1" * 40,
        "git_head": "1" * 40,
        "git_head_tree": "2" * 40,
        "git_object_format": "sha1",
        "live_source_seal_sha256": live_source_seal_sha256,
        "live_source_identity_sha256": live_source_identity_sha256,
        "file_count": 424,
        "directory_count": 20,
        "total_file_bytes": 1_000_000,
        "file_manifest_sha256": "f" * 64,
        "archive_tree_equivalent": True,
        "all_entries_non_writable": True,
        "cargo_configuration_exact": True,
        "manifest_path_reference_count": 3,
        "all_manifest_paths_within_source": True,
        "live_worktree_used_as_build_source": False,
    }


def write_artifact_manifest(root: Path) -> Path:
    manifest = root / "metadata/result-artifacts.sha256"
    rows = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path in {manifest, root / "COMPLETE"}:
            continue
        relative = path.relative_to(root).as_posix()
        rows.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {relative}\n")
    manifest.write_text("".join(rows), encoding="utf-8")
    manifest.chmod(0o444)
    return manifest


def write_frozen(path: Path, contents: bytes = b"frozen\n", mode: int = 0o444) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(contents)
    path.chmod(mode)
    return path


def write_control_seal(path: Path, inputs: list[Path]) -> Path:
    write_json(path, gate.control_seal(inputs))
    path.chmod(0o444)
    return path


def create_required_screen_control_seals(
    screen: Path,
    binaries: dict[str, Path],
    source: Path,
    source_archive: Path,
    extracted: Path,
    build: Path,
) -> tuple[Path, Path]:
    harness_dir = screen / "metadata/harness"
    harness_names = (
        "phase5_allocator_screen_run.sh",
        "phase5_allocator_profile_run.sh",
        "phase5_allocator_screen_gate.py",
        "phase5_allocator_screen_plan.json",
        "test_phase5_allocator_screen_gate.py",
        "phase1_replay_gate.py",
        "phase1_4m_expectations.json",
        "ab_gate.py",
        "fadvise_regular_dontneed.c",
        "README.md",
    )
    harness_files = [
        write_frozen(
            harness_dir / name,
            f"frozen {name}\n".encode(),
            0o555 if name.endswith("_run.sh") or name == "phase1_replay_gate.py" else 0o444,
        )
        for name in harness_names
    ]
    harness_seal = write_frozen(screen / "metadata/harness.sha256")
    validated_plan = write_frozen(screen / "metadata/validated-plan.json", b"{}\n")
    python_record = write_frozen(screen / "metadata/python-interpreter.txt")
    binaries_tsv = write_frozen(screen / "metadata/binaries.tsv")
    preserved = write_frozen(screen / "metadata/preserved-binaries.sha256")
    for path in (source, source_archive, extracted, build):
        path.chmod(0o444)
    core = screen / "metadata/core-controls.json"
    write_control_seal(
        core,
        [
            harness_seal,
            validated_plan,
            python_record,
            source,
            source_archive,
            extracted,
            binaries_tsv,
            preserved,
            build,
            *binaries.values(),
            *harness_files,
        ],
    )

    capture_inputs = write_frozen(screen / "metadata/capture-inputs-before.json", b"{}\n")
    config_template = write_frozen(screen / "metadata/config-template.toml")
    capture_manifest = write_frozen(screen / "metadata/capture-manifest.json", b"{}\n")
    run_plan = write_frozen(screen / "run-plan.tsv")
    rendered_configs = write_frozen(screen / "metadata/rendered-configs.sha256")
    fadvise = write_frozen(
        screen / "metadata/tools/fadvise-regular-dontneed", mode=0o555
    )
    fadvise_seal = write_frozen(
        screen / "metadata/tools/fadvise-regular-dontneed.sha256"
    )
    rendered_inputs: list[Path] = [
        write_frozen(screen / "configs/calibration-system.toml"),
        write_frozen(screen / "calibration/config-render.json", b"{}\n"),
    ]
    for run_index, policy in enumerate(gate.EXPECTED_SCHEDULE, start=1):
        label = f"run-{run_index:02d}-{policy}"
        rendered_inputs.extend(
            [
                write_frozen(screen / "configs" / f"{label}.toml"),
                write_frozen(screen / "runs" / label / "config-render.json", b"{}\n"),
            ]
        )
    measurement = screen / "metadata/measurement-controls.json"
    write_control_seal(
        measurement,
        [
            core,
            capture_inputs,
            config_template,
            capture_manifest,
            run_plan,
            rendered_configs,
            fadvise,
            fadvise_seal,
            *rendered_inputs,
        ],
    )
    return core, measurement


def inventory_winner(chunks: int = 0, points: int = 0) -> dict[str, int]:
    return {"chunks": chunks, "points": points}


def inventory_histogram(observations: int, lower: int = 1) -> dict[str, object]:
    return {
        "zero_count": 0,
        "buckets": (
            [
                {
                    "lower_inclusive": lower,
                    "upper_inclusive": 2 * lower - 1,
                    "count": observations,
                }
            ]
            if observations
            else []
        ),
    }


def timestamp_evidence(
    chunks: int = 1, points: int = 88, current_bytes: int = 95
) -> dict[str, object]:
    def candidate(size: int, selected: bool = False) -> dict[str, object]:
        selected_total = inventory_winner(chunks, points) if selected else inventory_winner()
        return {
            "bytes": size,
            "unique_wins": selected_total,
            "adaptive_selections": selected_total,
        }

    return {
        "chunks": chunks,
        "points": points,
        "current_offset_uleb": candidate(current_bytes),
        "adjacent_delta_uleb": candidate(max(current_bytes - 5, 0)),
        "delta_of_delta_zigzag_uleb128": candidate(max(current_bytes - 15, 0)),
        "fixed_step_residual_bitpack": candidate(max(current_bytes - 25, 0), True),
        "adaptive_min_bytes": max(current_bytes - 25, 0),
        "tied_minima": inventory_winner(),
    }


def chunk_inventory() -> dict[str, object]:
    chunks = 1
    points = 88
    payload = 799
    indexed = payload + 40
    evidence = {
        "tie_rule": "RAW_F64 wins equal payload-byte ties; then compare decode cost",
        "chunks": chunks,
        "points": points,
        "existing_indexed_bytes": indexed,
        "existing_payload_bytes": payload,
        "raw_f64_candidate_indexed_bytes": indexed,
        "raw_f64_candidate_payload_bytes": payload,
        "gorilla_candidate_indexed_bytes": 800,
        "gorilla_candidate_payload_bytes": 760,
        "adaptive_min_indexed_bytes": 800,
        "adaptive_min_payload_bytes": 760,
        "raw_f64_wins": inventory_winner(),
        "gorilla_wins": inventory_winner(chunks, points),
        "ties": inventory_winner(),
        "adaptive_raw_f64_selections": inventory_winner(),
        "adaptive_gorilla_selections": inventory_winner(chunks, points),
        "repeated_xor_points": 0,
        "reused_window_points": 0,
        "new_window_points": points - chunks,
        "xor_significant_bits_histogram": inventory_histogram(points - chunks),
        "positive_zero_points": 0,
        "negative_zero_points": 0,
        "finite_nonzero_points": points,
        "positive_infinity_points": 0,
        "negative_infinity_points": 0,
        "ordinary_nan_points": 0,
        "stale_nan_points": 0,
    }
    timestamp = timestamp_evidence(chunks, points, 95)
    return {
        "layout": "sealed_chunk_v1",
        "by_kind_encoding": [
            {
                "kind": "float",
                "encoding": "gorilla",
                "payload_layout": "t0_dt_then_values",
                "chunks": chunks,
                "points": points,
                "indexed_bytes": indexed,
                "common_header_bytes": 40,
                "scalar_lane_bytes": 0,
                "payload_bytes": payload,
                "timestamp_base_bytes": 8,
                "timestamp_delta_bytes": 87,
                "value_bytes": 704,
                "point_count_histogram": inventory_histogram(chunks, 64),
                "cadence_ms_histogram": inventory_histogram(points - chunks),
            }
        ],
        "raw_f64_vs_gorilla": evidence,
        "timestamp_candidates": {
            "scope": "native payload only",
            "tie_rule": "stable order",
            "selector_bytes_included": False,
            "all_blocks": timestamp,
            "by_shape": [{"shape": "variable_step", "evidence": timestamp}],
            "by_kind_encoding": [
                {"kind": "float", "encoding": "gorilla", "evidence": timestamp}
            ],
        },
    }


def storage_report() -> dict[str, object]:
    return {
        "schema_version": 8,
        "footer_validation_enabled": True,
        "series_sample_per_segment": None,
        "verified_selection_fingerprint": "a" * 64,
        "decoded_semantic_fingerprint": "c" * 64,
        "segments": 1,
        "corpus_series": 1,
        "series": 1,
        "chunks": 1,
        "chunks_by_kind": [1, 0, 0, 0, 0],
        "samples": 88,
        "logical_chunk_bytes": 839,
        "chunk_inventory": chunk_inventory(),
        "exact_postings": {
            "logical_fingerprint": "b" * 64,
            "lists": 1,
            "decoded_refs": 1,
            "encoded_bytes": 4,
        },
        "elapsed_ns": 1,
        "metadata_read_calls": 1,
        "metadata_read_bytes": 64,
        "metadata_peak_retained_bytes": 64,
        "metadata_peak_in_flight_bytes": 64,
        "metadata_peak_open_files": 1,
        "metadata_cache_hits": 0,
        "metadata_cache_misses": 1,
    }


def readback_report() -> str:
    rows = "\n".join(
        "| Float | `up{instance=\"%d\"}` | 1 | 1 | 1 | 1 | 1 | 8 | 1 | 0 | 0 |"
        % index
        for index in range(14)
    )
    return f"""# Query Smoke

## PromQL Readbacks

| Kind | Query | result_series | result_samples | matched_series | projected_series | chunk_reads | bytes_read | samples_decoded | typed_scalar_chunks_decoded | typed_full_chunks_decoded |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
{rows}

## Readback Verification

| Metric | Value |
| --- | ---: |
| Checked Queries | 40 |
| Mismatches | 0 |

## Query Diagnostics

| Metric | Value |
| --- | ---: |
| Expected Readback Queries | 40 |
| Executed Readback Queries | 40 |
| Skipped Readback Queries | 0 |
| Isolation Check Skips | 0 |
"""


def external_guardian() -> dict[str, object]:
    return {
        "schema": gate.GUARDIAN_SCHEMA,
        "root_pid": 123,
        "interval_ms": 100,
        "polls": 10,
        "elapsed_ns": 1_000_000_000,
        "conflicts": [],
        "filesystem": "/tmp",
        "minimum_free_bytes": 1,
        "minimum_observed_free_bytes": 2,
        "capacity_violations": [],
        "complete_and_conflict_free": True,
    }


def writeback_quiescence() -> dict[str, object]:
    return {
        "schema": "chronoxide/storage-vnext-phase5-writeback-quiescence/v1",
        "corpus": "/tmp/corpus",
        "fsynced_file_count": 2,
        "global_sync_called": True,
        "maximum_dirty_writeback_kib": 65_536,
        "required_consecutive_samples": 3,
        "interval_ms": 250,
        "timeout_secs": 120,
        "sample_count": 3,
        "final_dirty_kib": 10,
        "final_writeback_kib": 0,
        "final_total_kib": 10,
        "passed": True,
    }


def build_provenance() -> dict[str, object]:
    home = str(Path.home())
    target = "/tmp/build-target"
    return {
        "schema": gate.BUILD_PROVENANCE_SCHEMA,
        "git_head": "1" * 40,
        "git_head_tree": "2" * 40,
        "git_index_tree": "2" * 40,
        "source_worktree_clean": True,
        "source_seal_sha256": "8" * 64,
        "source_identity_sha256": "9" * 64,
        "build_source": {
            "mode": "read-only git archive HEAD extraction",
            "root": "/tmp/build-source",
            "archive_path": "/tmp/git-head.tar",
            "archive_sha256": "e" * 64,
            "archive_size_bytes": 1024,
            "archive_embedded_commit": "1" * 40,
            "extracted_source_seal_sha256": "0" * 64,
            "file_manifest_sha256": "f" * 64,
            "file_count": 424,
            "directory_count": 20,
            "total_file_bytes": 1_000_000,
            "archive_tree_equivalent": True,
            "all_entries_non_writable": True,
            "cargo_configuration_exact": True,
            "manifest_path_reference_count": 3,
            "all_manifest_paths_within_source": True,
            "live_worktree_used_as_build_source": False,
        },
        "tracked_input_count": 424,
        "tracked_input_manifest_sha256": "6" * 64,
        "git_index_flags_clear": True,
        "tracked_inputs_regular_files": True,
        "cargo_lock_sha256": "3" * 64,
        "tracked_cargo_config_sha256": "7" * 64,
        "tracked_cargo_rustflags": "-C target-cpu=native",
        "target_dir": target,
        "controlled_environment": {
            "HOME": home,
            "PATH": f"{home}/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "CARGO_HOME": f"{home}/.cargo",
            "RUSTUP_HOME": f"{home}/.rustup",
            "RUSTC": f"{home}/.cargo/bin/rustc",
            "RUSTDOC": f"{home}/.cargo/bin/rustdoc",
            "LC_ALL": "C",
            "TZ": "UTC",
            "CARGO_INCREMENTAL": "0",
            "CARGO_TARGET_DIR": target,
            "ambient_rustflags": False,
            "ambient_allocator_configuration": False,
        },
        "build_commands": {
            "system": "cargo build --manifest-path Cargo.toml --locked --release --no-default-features -p chronoxide-ingester --bin chronoxide-ingester --bin chronoxide-query --bin chronoxide-storage-verify",
            "jemalloc": "cargo build --manifest-path Cargo.toml --locked --release --no-default-features --features jemalloc-stats -p chronoxide-ingester --bin chronoxide-ingester",
        },
        "build_log_sha256": {"system": "4" * 64, "jemalloc": "5" * 64},
        "binary_sha256": {
            "system": "a" * 64,
            "jemalloc": "b" * 64,
            "query": "c" * 64,
            "storage_verify": "d" * 64,
        },
        "jemalloc_stats_enabled": True,
        "screen_jemalloc_feature": "jemalloc-stats",
        "later_no_stats_jemalloc_feature": "jemalloc",
        "later_no_stats_revalidation_command": "cargo build --manifest-path Cargo.toml --locked --release --no-default-features --features jemalloc -p chronoxide-ingester --bin chronoxide-ingester",
        "no_stats_production_build_validated": False,
    }


def rss_summary() -> dict[str, object]:
    return {
        "root_pid": 123,
        "interval_ms": 100,
        "clock_ticks_per_second": 100,
        "samples": 322,
        "workload_samples": 20,
        "post_drop_samples": 300,
        "hold_complete_samples": 2,
        "checkpoint_incomplete_samples": 0,
        "peak_rss_kib": 1_000,
        "peak_rss_anon_kib": 800,
        "peak_rss_file_kib": 200,
        "peak_vm_swap_kib": 0,
        "peak_process_count": 4,
        "workload_peak_rss_kib": 1_000,
        "workload_peak_max_single_hwm_kib": 1_100,
        "workload_boundary_max_single_hwm_kib": 1_100,
        "post_drop_first_rss_kib": 900,
        "post_drop_min_rss_kib": 500,
        "post_drop_end_rss_kib": 520,
        "post_drop_first_unix_time_ns": 100_100_000_000,
        "post_drop_end_unix_time_ns": 129_900_000_000,
        "workload_boundary_cpu_ticks": 10_000,
        "workload_boundary_cpu_seconds": 100.0,
        "workload_boundary_sample_window_start_unix_time_ns": 100_050_000_000,
        "workload_boundary_sample_unix_time_ns": 100_100_000_000,
    }


def allocator_telemetry_summary(policy: str) -> dict[str, object]:
    allocator = "system" if policy == "S" else "jemalloc"
    stats = {
        "allocated_bytes": (500, 100),
        "active_bytes": (600, 200),
        "resident_bytes": (800, 400),
        "mapped_bytes": (900, 700),
        "retained_bytes": (200, 100),
    }
    records = []
    for index, phase in enumerate(("post_ingester_drop", "hold_complete")):
        available = policy != "S"
        record = {
            "schema": gate.TELEMETRY_SCHEMA,
            "phase": phase,
            "main_elapsed_ns": 10_001_000_000 if index == 0 else 39_999_000_000,
            "unix_time_ns": 100_001_000_000 if index == 0 else 129_999_000_000,
            "rust_global_allocator": allocator,
            "allocator_internal_telemetry": "available" if available else "unavailable",
            "epoch": index + 1 if available else None,
        }
        record.update(
            {
                key: values[index] if available else None
                for key, values in stats.items()
            }
        )
        records.append(record)
    deltas = {
        key: values[1] - values[0] if policy != "S" else None
        for key, values in stats.items()
    }
    reconciliations = []
    for record in records:
        external = 900 * 1024
        resident = record["resident_bytes"]
        reconciliations.append(
            {
                "phase": record["phase"],
                "allocator_telemetry_unix_time_ns": record["unix_time_ns"],
                "external_rss_unix_time_ns": record["unix_time_ns"],
                "alignment_abs_ns": 0,
                "external_process_tree_rss_bytes": external,
                "jemalloc_resident_bytes": resident,
                "external_minus_jemalloc_resident_bytes": (
                    external - resident if resident is not None else None
                ),
                "measurement_relation": gate.ALLOCATOR_RSS_RELATION,
            }
        )
    return {
        "schema": gate.TELEMETRY_SUMMARY_SCHEMA,
        "policy": policy,
        "rust_global_allocator": allocator,
        "records": records,
        "checkpoint_bounds": {
            "drop_main_elapsed_ns": 10_000_000_000,
            "hold_complete_main_elapsed_ns": 40_000_000_000,
            "drop_unix_time_ns": 100_000_000_000,
            "hold_complete_unix_time_ns": 130_000_000_000,
        },
        "hold_complete_minus_post_drop_bytes": deltas,
        "external_rss_reconciliation": reconciliations,
        "measurement_relation": gate.ALLOCATOR_RSS_RELATION,
    }


def checkpoint_text(hold_end_ns: int = 40_000_000_000) -> str:
    schema = gate.CHECKPOINT_SCHEMA
    return (
        "schema\tphase\tmain_elapsed_ns\tunix_time_ns\thold_secs\n"
        f"{schema}\tingester_dropped\t10000000000\t100000000000\t30\n"
        f"{schema}\thold_complete\t{hold_end_ns}\t130000000000\t30\n"
    )


def rss_samples_text() -> str:
    rows = [
        [
            1,
            99_900_000_000,
            99_901_000_000,
            "workload",
            3,
            9_900,
            1_000,
            800,
            200,
            0,
            1_000,
            "1,2,3",
        ],
        [
            2,
            100_000_500_000,
            100_001_000_000,
            "post_drop_hold",
            3,
            10_000,
            900,
            700,
            200,
            0,
            1_000,
            "1,2,3",
        ],
        [
            3,
            129_998_500_000,
            129_999_000_000,
            "post_drop_hold",
            3,
            10_001,
            500,
            400,
            100,
            0,
            1_000,
            "1,2,3",
        ],
        [
            4,
            130_000_000_000,
            130_000_000_001,
            "terminal",
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            "-",
        ],
    ]
    return (
        "\t".join(gate.RSS_SAMPLE_COLUMNS)
        + "\n"
        + "\n".join("\t".join(str(value) for value in row) for row in rows)
        + "\n"
    )


def write_rss_release_fixture(
    root: Path,
    summary_path: Path,
    *,
    post_drop_samples: int = 301,
    first_post_drop_unix_time_ns: int = 100_050_000_000,
) -> dict[str, object]:
    """Write internally derived held-launch RSS evidence for parser tests."""
    samples_path = root / "rss-samples.tsv"
    control_path = root / "external-conflict-guardian-control.json"
    ready_path = root / "external-conflict-guardian-ready"
    rss_ready_path = root / "rss-monitor-ready"
    launch_path = root / "external-conflict-guardian-launch"
    for path in (control_path, ready_path, rss_ready_path, launch_path):
        if path.exists() or path.is_symlink():
            path.chmod(0o644)
            path.unlink()

    rows: list[dict[str, object]] = []
    rows.append(
        {
            "elapsed_ns": 0,
            "sample_window_start_unix_time_ns": 99_949_500_000,
            "unix_time_ns": 99_950_000_000,
            "phase": "workload",
            "process_count": 2,
            "process_cpu_ticks": 9_900,
            "rss_kib": 1_000,
            "rss_anon_kib": 800,
            "rss_file_kib": 200,
            "vm_swap_kib": 0,
            "max_single_hwm_kib": 1_100,
            "pids": "123,124",
        }
    )
    final_post_drop_unix_time_ns = 129_900_000_000
    for index in range(1, 302):
        unix_time_ns = first_post_drop_unix_time_ns + (
            (final_post_drop_unix_time_ns - first_post_drop_unix_time_ns)
            * (index - 1)
            // 300
        )
        rows.append(
            {
                "elapsed_ns": index * 100_000_000,
                "sample_window_start_unix_time_ns": unix_time_ns - 500_000,
                "unix_time_ns": unix_time_ns,
                "phase": (
                    "post_drop_hold" if index <= post_drop_samples else "hold_complete"
                ),
                "process_count": 2,
                "process_cpu_ticks": 9_999 + index,
                "rss_kib": max(500, 901 - index),
                "rss_anon_kib": max(400, 701 - index),
                "rss_file_kib": 200,
                "vm_swap_kib": 0,
                "max_single_hwm_kib": 1_100,
                "pids": "123,124",
            }
        )
    rows.append(
        {
            "elapsed_ns": int(rows[-1]["elapsed_ns"]) + 100_000_000,
            "sample_window_start_unix_time_ns": 130_000_000_000,
            "unix_time_ns": 130_000_000_001,
            "phase": "terminal",
            "process_count": 0,
            "process_cpu_ticks": 0,
            "rss_kib": 0,
            "rss_anon_kib": 0,
            "rss_file_kib": 0,
            "vm_swap_kib": 0,
            "max_single_hwm_kib": 0,
            "pids": "-",
        }
    )
    samples_path.write_text(
        "\t".join(gate.RSS_SAMPLE_COLUMNS)
        + "\n"
        + "\n".join(
            "\t".join(str(row[column]) for column in gate.RSS_SAMPLE_COLUMNS)
            for row in rows
        )
        + "\n",
        encoding="utf-8",
    )

    control = {
        "schema": gate.GUARDIAN_CONTROL_SCHEMA,
        "root_pid": 123,
        "root_starttime_ticks": 123_000,
        "guardian_pid": 456,
        "guardian_starttime_ticks": 456_000,
        "rss_monitor_pid": 789,
        "rss_monitor_starttime_ticks": 789_000,
        "rss_ready_marker": str(rss_ready_path),
        "interval_ms": 100,
        "ready_marker": str(ready_path),
        "launch_marker": str(launch_path),
    }
    write_json(control_path, control)
    control_path.chmod(0o444)
    for marker in (rss_ready_path, launch_path):
        marker.touch()
        marker.chmod(0o444)

    timestamps = [int(row["elapsed_ns"]) for row in rows]
    elapsed_ns = timestamps[-1] + 50_000_000
    summary = gate.summarize_rss_samples(rows, 123, 100, 100)
    summary.update(
        {
            "root_starttime_ticks": 123_000,
            "elapsed_ns": elapsed_ns,
            "poll_monotonic_elapsed_ns": timestamps,
            "maximum_poll_start_gap_ns": gate.derive_guardian_maximum_poll_start_gap_ns(
                timestamps, elapsed_ns
            ),
            "maximum_allowed_poll_start_gap_ns": gate.guardian_maximum_allowed_gap_ns(
                100
            ),
            "control_path": str(control_path),
            "control_sha256": gate.sha256_file(control_path),
            "rss_ready_marker_path": str(rss_ready_path),
            "rss_ready_marker_sha256": gate.sha256_file(rss_ready_path),
            "rss_ready_created_sample": 1,
            "rss_ready_created_monotonic_elapsed_ns": timestamps[0],
            "launch_marker_path": str(launch_path),
            "launch_marker_sha256": gate.sha256_file(launch_path),
            "launch_observed_sample": 2,
            "launch_observed_monotonic_elapsed_ns": timestamps[1],
            "launch_observed": True,
            "terminal_observation": True,
            "terminal_launch_observed": True,
            "handshake_violations": [],
            "complete": True,
        }
    )
    write_json(summary_path, summary)
    return summary


def synthetic_observation(run_index: int, policy: str) -> dict[str, object]:
    allocator = "system" if policy == "S" else "jemalloc"
    effective = application_preflight(policy)["effective_policy"]
    workload_cpu_ticks = 10_000 if policy == "S" else 9_500
    task_clock = 100.0 if policy == "S" else 120.0
    rss = rss_summary()
    rss["workload_boundary_cpu_ticks"] = workload_cpu_ticks
    rss["workload_boundary_cpu_seconds"] = workload_cpu_ticks / 100
    rss["peak_rss_kib"] = 1_000 if policy == "S" else 2_000
    rss["workload_peak_rss_kib"] = 1_000 if policy == "S" else 1_040
    rss["workload_peak_max_single_hwm_kib"] = 1_000 if policy == "S" else 1_040
    rss["workload_boundary_max_single_hwm_kib"] = 1_000 if policy == "S" else 1_040
    rss["post_drop_end_rss_kib"] = 500 if policy == "S" else 520
    return {
        "schema": gate.OBSERVATION_SCHEMA,
        "run_index": run_index,
        "block": 1 if run_index <= 5 else 2,
        "position": run_index if run_index <= 5 else run_index - 5,
        "policy": policy,
        "binary_role": allocator,
        "binary_sha256": ("a" if allocator == "system" else "b") * 64,
        "build_provenance_sha256": "8" * 64,
        "jemalloc_stats_enabled": True,
        "allocator_effective_policy": effective,
        "runtime_effective_policy": effective,
        "preflight_record_sha256": "c" * 64,
        "runtime_policy_record_sha256": "d" * 64,
        "allocator_telemetry_record_sha256": "0" * 64,
        "external_conflict_guardian_sha256": "1" * 64,
        "pre_run_writeback_quiescence_sha256": "6" * 64,
        "writeback_quiescence_sha256": "2" * 64,
        "workload_wall_ns": 10_000_000_000,
        "workload_cpu_ticks": workload_cpu_ticks,
        "workload_cpu_seconds": workload_cpu_ticks / 100,
        "clock_ticks_per_second": 100,
        "workload_cpu_boundary_uncertainty_ns": 100_000_000,
        "full_elapsed": "0:40.00",
        "full_user_seconds": 1.0,
        "full_system_seconds": 2.0,
        "time_max_rss_kib": rss["peak_rss_kib"],
        "perf": {event: task_clock for event in gate.EXPECTED_PERF_EVENTS},
        "external_conflict_guardian": external_guardian(),
        "pre_run_writeback_quiescence": writeback_quiescence(),
        "writeback_quiescence": writeback_quiescence(),
        "rss": rss,
        "hold_elapsed_ns": 30_000_000_000,
        "allocator_release_telemetry": allocator_telemetry_summary(policy),
        "corpus": corpus_summary(),
        "correctness_sha256": "e" * 64,
        "correctness": replay_correctness(),
    }


class Phase5AllocatorScreenGateTest(unittest.TestCase):
    def test_gate_loads_exact_phase1_source_and_ignores_valid_malicious_pyc(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            gate_copy = root / "phase5_allocator_screen_gate.py"
            phase1_copy = root / "phase1_replay_gate.py"
            shutil.copyfile(HERE / gate_copy.name, gate_copy)
            shutil.copyfile(HERE / "ab_gate.py", root / "ab_gate.py")
            real_source = (HERE / phase1_copy.name).read_bytes()
            prefix = b'CORPUS_SUMMARY_SCHEMA = "malicious-cache"\n'
            self.assertLess(len(prefix), len(real_source))
            malicious_source = prefix + b"#" * (len(real_source) - len(prefix) - 1) + b"\n"
            phase1_copy.write_bytes(malicious_source)
            fixed_timestamp = 1_700_000_000
            os.utime(phase1_copy, (fixed_timestamp, fixed_timestamp))
            py_compile.compile(str(phase1_copy), doraise=True)
            phase1_copy.write_bytes(real_source)
            os.utime(phase1_copy, (fixed_timestamp, fixed_timestamp))

            ambient_probe = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-c",
                    "import sys; sys.path.insert(0,sys.argv[1]); import phase1_replay_gate as module; print(module.CORPUS_SUMMARY_SCHEMA)",
                    str(root),
                ],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            )
            self.assertEqual(ambient_probe.stdout.strip(), "malicious-cache")

            exact_probe_code = (
                "import os,stat,sys; "
                "script=os.path.realpath(sys.argv[1]); "
                "mode=os.lstat(script).st_mode; "
                "assert stat.S_ISREG(mode) and not os.path.islink(script); "
                "namespace={'__name__':'probe','__file__':script,'__package__':None,'__cached__':None}; "
                "source=open(script,'rb').read(); "
                "exec(compile(source,script,'exec',dont_inherit=True),namespace); "
                "print(namespace['phase1'].CORPUS_SUMMARY_SCHEMA)"
            )
            exact_probe = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    "-B",
                    "-c",
                    exact_probe_code,
                    str(gate_copy),
                ],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            )
            self.assertEqual(
                exact_probe.stdout.strip(), gate.phase1.CORPUS_SUMMARY_SCHEMA
            )

    def test_control_seal_rejects_writable_or_mutated_fixed_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            first = write_frozen(root / "first.txt", b"first\n")
            second = write_frozen(root / "second.sh", b"#!/bin/sh\n", 0o555)
            seal = write_control_seal(root / "controls.json", [first, second])
            result = gate.check_control_seal(seal)
            self.assertEqual(result["input_count"], 2)

            first.chmod(0o644)
            first.write_text("changed\n", encoding="utf-8")
            first.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "fixed control seal changed"):
                gate.check_control_seal(seal)

            first.chmod(0o644)
            with self.assertRaisesRegex(gate.GateError, "exact mode"):
                gate.check_control_seal(seal)

    def test_rendered_config_gate_binds_record_hash_and_parameters(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            capture = root / "capture"
            capture.mkdir()
            segments = root / "segments"
            config = root / "run.toml"
            record = root / "render.json"
            config.write_text(
                "[ingestion]\n"
                f'replay_from = "{capture}"\n'
                "stop_after_messages = 250000\n"
                "[ingestion.segment_writer]\n"
                f'segments_dir = "{segments}"\n',
                encoding="utf-8",
            )
            write_json(
                record,
                {
                    "config": str(config),
                    "sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
                    "segments_dir": str(segments),
                    "stop_after_messages": 250_000,
                },
            )
            config.chmod(0o444)
            record.chmod(0o444)
            gate.check_rendered_config(record, config, capture, segments, 250_000)

            record.chmod(0o644)
            changed = json.loads(record.read_text(encoding="utf-8"))
            changed["stop_after_messages"] = 249_999
            write_json(record, changed)
            record.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "frozen path/hash/parameters"):
                gate.check_rendered_config(record, config, capture, segments, 250_000)

    def test_profile_stack_parsers_reject_summary_and_leaf_only_evidence(self) -> None:
        with self.assertRaisesRegex(gate.GateError, "multi-frame"):
            gate.parse_heaptrack_stack_evidence(
                "calls to allocation functions: 10\n"
            )
        with self.assertRaisesRegex(gate.GateError, "multi-frame"):
            gate.parse_heaptrack_stack_evidence("chronoxide_ingester::main 10\n")
        heaptrack = gate.parse_heaptrack_stack_evidence(
            "chronoxide_ingester::processor::process;alloc::alloc::alloc 10\n"
        )
        self.assertEqual(heaptrack["chronoxide_stack_count"], 1)

        perf_leaf = (
            "chronoxide-ingester 1 [000] 1.000: cycles:\n"
            "        0001 chronoxide_ingester::main (chronoxide-ingester)\n"
        )
        with self.assertRaisesRegex(gate.GateError, "multi-frame"):
            gate.parse_perf_script_stack_evidence(perf_leaf)
        perf = gate.parse_perf_script_stack_evidence(
            perf_leaf
            + "        0002 tokio::runtime::Runtime::block_on (libtokio.so)\n"
        )
        self.assertEqual(perf["chronoxide_stack_count"], 1)

    def test_frozen_plan_rejects_schedule_completion_and_helper_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            gate.validate_plan(plan, expectations)
            self.assertIsNone(gate.EXPECTED_CONFS["J0"])
            self.assertEqual(gate.EXPECTED_CONFS["J1"].split(",")[-1], "narenas:4")
            self.assertIn("narenas:4", gate.EXPECTED_CONFS["J2"])
            self.assertIn("muzzy_decay_ms:0", gate.EXPECTED_CONFS["J2"])
            self.assertIn("narenas:2", gate.EXPECTED_CONFS["J3"])

            original = json.loads(plan.read_text(encoding="utf-8"))
            changed = copy.deepcopy(original)
            changed["schedule"][1]["policy"] = "J3"
            write_json(plan, changed)
            with self.assertRaisesRegex(gate.GateError, "frozen mirror"):
                gate.validate_plan(plan, expectations)

            changed = copy.deepcopy(original)
            changed["completion_contract"]["partial_runs_promotable"] = True
            write_json(plan, changed)
            with self.assertRaisesRegex(gate.GateError, "completion contract"):
                gate.validate_plan(plan, expectations)

            write_json(plan, original)
            (root / PINNED_HELPERS[0]).write_text("changed", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "helper.*changed"):
                gate.validate_plan(plan, expectations)

    def test_frozen_plan_rejects_stale_4m_readback_count(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            changed = json.loads(plan.read_text(encoding="utf-8"))
            changed["workload"]["expected_readback_queries"] = 38
            write_json(plan, changed)

            with self.assertRaisesRegex(gate.GateError, "workload differs"):
                gate.validate_plan(plan, expectations)

    def test_preflight_requires_effective_mallctl_values_confirmation_and_binary_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            binary = executable(root, "jemalloc", b"jemalloc comparator")
            stdout = root / "stdout"
            stderr = root / "stderr"
            source_audit = root / "source-audit.stderr"
            write_json(stdout, application_preflight("J2"))
            stderr.write_text(confirmation_text("J2"), encoding="utf-8")
            source_audit.write_text(confirmation_text("J2"), encoding="utf-8")

            record = gate.parse_preflight(
                stdout, stderr, binary, plan, expectations, "J2", source_audit
            )
            self.assertEqual(record["schema"], gate.PREFLIGHT_RECORD_SCHEMA)
            self.assertEqual(
                record["binary_sha256"], hashlib.sha256(binary.read_bytes()).hexdigest()
            )
            self.assertEqual(record["application"]["effective_policy"]["narenas"], 4)

            mismatched = application_preflight("J2")
            mismatched["effective_policy"]["narenas"] = 2
            write_json(stdout, mismatched)
            with self.assertRaisesRegex(gate.GateError, "effective narenas differs"):
                gate.parse_preflight(
                    stdout, stderr, binary, plan, expectations, "J2", source_audit
                )

            write_json(stdout, application_preflight("J2"))
            stderr.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "confirm_conf"):
                gate.parse_preflight(
                    stdout, stderr, binary, plan, expectations, "J2", source_audit
                )

            write_json(stdout, application_preflight("J0"))
            source_audit.write_text(j0_source_audit_text(), encoding="utf-8")
            record = gate.parse_preflight(
                stdout, stderr, binary, plan, expectations, "J0", source_audit
            )
            self.assertIsNone(record["application"]["requested_policy_raw"])
            self.assertIsNotNone(record["application"]["effective_policy"])
            self.assertFalse(record["jemalloc_confirm_conf_verified"])
            self.assertTrue(record["jemalloc_config_sources_verified"])

    def test_system_preflight_has_explicitly_unavailable_allocator_internals(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            binary = executable(root, "system", b"system comparator")
            stdout = root / "stdout"
            stderr = root / "stderr"
            write_json(stdout, application_preflight("S"))
            stderr.write_text("", encoding="utf-8")
            record = gate.parse_preflight(
                stdout, stderr, binary, plan, expectations, "S"
            )
            self.assertIsNone(record["application"]["effective_policy"])
            self.assertFalse(record["jemalloc_confirm_conf_verified"])

            document = application_preflight("S")
            document["effective_policy"] = application_preflight("J0")[
                "effective_policy"
            ]
            write_json(stdout, document)
            with self.assertRaisesRegex(gate.GateError, "explicit null"):
                gate.parse_preflight(stdout, stderr, binary, plan, expectations, "S")

    def test_runtime_log_requires_run_specific_effective_policy_and_hold_markers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            log = root / "run.log"
            preflight = root / "preflight.json"
            write_json(preflight, preflight_record("J2"))
            log.write_text(runtime_log("J2"), encoding="utf-8")
            result = gate.gate_runtime_log(log, preflight, plan, expectations, "J2")
            self.assertEqual(result["effective_policy"]["narenas"], 4)

            write_json(preflight, preflight_record("J0"))
            log.write_text(runtime_log("J0"), encoding="utf-8")
            default_result = gate.gate_runtime_log(
                log, preflight, plan, expectations, "J0"
            )
            self.assertIsNone(default_result["jemalloc_conf"])
            self.assertFalse(default_result["jemalloc_confirm_conf"])
            self.assertIsNotNone(default_result["effective_policy"])

            write_json(preflight, preflight_record("J2"))
            log.write_text(
                runtime_log("J2").replace('"narenas":4', '"narenas":2'),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "eight-field preflight"):
                gate.gate_runtime_log(log, preflight, plan, expectations, "J2")

            log.write_text(
                runtime_log("J2").replace(
                    "INFO Diagnostic allocator release hold complete\n", ""
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "completed release-hold"):
                gate.gate_runtime_log(log, preflight, plan, expectations, "J2")

    def test_selected_perf_runtime_requires_preflight_and_exact_jemalloc_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            log = root / "profile.log"
            preflight = root / "preflight.json"
            write_json(preflight, preflight_record("J2"))
            log.write_text(profile_runtime_log("J2"), encoding="utf-8")
            evidence = gate.gate_profile_runtime_log(
                log, preflight, plan, expectations, "J2"
            )
            self.assertTrue(evidence["untimed_profile_runtime"])
            self.assertEqual(evidence["post_drop_hold_markers"], 0)

            log.write_text(
                profile_runtime_log("J2").replace(
                    'malloc_conf #1 (string specified via --with-malloc-conf): ""',
                    'malloc_conf #1 (string specified via --with-malloc-conf): "narenas:99"',
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "sources #1..#5"):
                gate.gate_profile_runtime_log(
                    log, preflight, plan, expectations, "J2"
                )

            log.write_text(profile_runtime_log("J2"), encoding="utf-8")
            changed = preflight_record("J2")
            changed["application"]["effective_policy"]["narenas"] = 2
            write_json(preflight, changed)
            with self.assertRaisesRegex(gate.GateError, "selected-policy preflight"):
                gate.gate_profile_runtime_log(
                    log, preflight, plan, expectations, "J2"
                )

    def test_runtime_parity_rejects_unrequested_field_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            log = root / "run.log"
            preflight = root / "preflight.json"
            for policy, old, new in (
                ("J0", '"narenas":8', '"narenas":9'),
                ("J2", '"retain":true', '"retain":false'),
            ):
                write_json(preflight, preflight_record(policy))
                text = runtime_log(policy).replace(old, new)
                self.assertNotEqual(text, runtime_log(policy))
                log.write_text(text, encoding="utf-8")
                with self.assertRaisesRegex(gate.GateError, "eight-field preflight"):
                    gate.gate_runtime_log(
                        log, preflight, plan, expectations, policy
                    )

    def test_all_jemalloc_configuration_sources_must_be_empty_except_environment(self) -> None:
        text = confirmation_text("J2")
        gate.validate_jemalloc_config_sources(text, gate.EXPECTED_CONFS["J2"])
        poisoned = text.replace(
            'malloc_conf #1 (string specified via --with-malloc-conf): ""',
            'malloc_conf #1 (string specified via --with-malloc-conf): "narenas:99"',
        )
        with self.assertRaisesRegex(gate.GateError, "sources #1..#5"):
            gate.validate_jemalloc_config_sources(
                poisoned, gate.EXPECTED_CONFS["J2"]
            )

    def test_checkpoint_excludes_hold_and_requires_external_phase_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            checkpoint = root / "checkpoint.tsv"
            rss = root / "rss.json"
            checkpoint.write_text(checkpoint_text(), encoding="utf-8")
            write_rss_release_fixture(root, rss)
            result = gate.parse_checkpoint(checkpoint, rss, plan, expectations)
            self.assertEqual(result["workload_wall_ns"], 10_000_000_000)
            self.assertEqual(result["hold_elapsed_ns"], 30_000_000_000)

            checkpoint.write_text(checkpoint_text(39_000_000_000), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "hold duration"):
                gate.parse_checkpoint(checkpoint, rss, plan, expectations)

            checkpoint.write_text(checkpoint_text(), encoding="utf-8")
            write_rss_release_fixture(root, rss, post_drop_samples=19)
            with self.assertRaisesRegex(gate.GateError, "enough post-drop"):
                gate.parse_checkpoint(checkpoint, rss, plan, expectations)

            write_rss_release_fixture(
                root, rss, first_post_drop_unix_time_ns=99_000_000_000
            )
            with self.assertRaisesRegex(gate.GateError, "before the Ingester-drop"):
                gate.parse_checkpoint(checkpoint, rss, plan, expectations)

            write_rss_release_fixture(
                root, rss, first_post_drop_unix_time_ns=100_100_000_001
            )
            with self.assertRaisesRegex(gate.GateError, "uncertainty exceeds"):
                gate.parse_checkpoint(checkpoint, rss, plan, expectations)

    def test_rss_release_evidence_rejects_cadence_identity_and_marker_mutations(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            samples_path = root / "rss-samples.tsv"
            summary_path = root / "rss.json"
            control_path = root / "external-conflict-guardian-control.json"
            rss_ready_path = root / "rss-monitor-ready"
            launch_path = root / "external-conflict-guardian-launch"

            def validate() -> dict[str, object]:
                return gate.validate_rss_release_evidence(
                    samples_path,
                    summary_path,
                    control_path,
                    rss_ready_path,
                    launch_path,
                    100,
                )

            def rewrite_rows(rows: list[dict[str, object]]) -> None:
                samples_path.write_text(
                    "\t".join(gate.RSS_SAMPLE_COLUMNS)
                    + "\n"
                    + "\n".join(
                        "\t".join(
                            str(row[column]) for column in gate.RSS_SAMPLE_COLUMNS
                        )
                        for row in rows
                    )
                    + "\n",
                    encoding="utf-8",
                )

            write_rss_release_fixture(root, summary_path)
            self.assertTrue(validate()["complete"])

            write_rss_release_fixture(root, summary_path)
            rows = gate.load_rss_samples(samples_path)
            rows[1]["elapsed_ns"] = rows[0]["elapsed_ns"]
            rewrite_rows(rows)
            changed = json.loads(summary_path.read_text(encoding="utf-8"))
            timestamps = [row["elapsed_ns"] for row in rows]
            changed["poll_monotonic_elapsed_ns"] = timestamps
            changed["maximum_poll_start_gap_ns"] = (
                gate.derive_guardian_maximum_poll_start_gap_ns(
                    timestamps, changed["elapsed_ns"]
                )
            )
            write_json(summary_path, changed)
            with self.assertRaisesRegex(gate.GateError, "strictly increasing"):
                validate()

            write_rss_release_fixture(root, summary_path)
            rows = gate.load_rss_samples(samples_path)
            for row in rows[150:]:
                row["elapsed_ns"] += 101_000_000
            rewrite_rows(rows)
            changed = json.loads(summary_path.read_text(encoding="utf-8"))
            timestamps = [row["elapsed_ns"] for row in rows]
            changed["poll_monotonic_elapsed_ns"] = timestamps
            changed["elapsed_ns"] += 101_000_000
            changed["maximum_poll_start_gap_ns"] = (
                gate.derive_guardian_maximum_poll_start_gap_ns(
                    timestamps, changed["elapsed_ns"]
                )
            )
            write_json(summary_path, changed)
            with self.assertRaisesRegex(gate.GateError, "maximum gap exceeds"):
                validate()

            write_rss_release_fixture(root, summary_path)
            changed = json.loads(summary_path.read_text(encoding="utf-8"))
            timestamps = changed["poll_monotonic_elapsed_ns"]
            changed["elapsed_ns"] = timestamps[-1] + 200_000_001
            changed["maximum_poll_start_gap_ns"] = 200_000_001
            write_json(summary_path, changed)
            with self.assertRaisesRegex(gate.GateError, "maximum gap exceeds"):
                validate()

            write_rss_release_fixture(root, summary_path)
            changed = json.loads(summary_path.read_text(encoding="utf-8"))
            changed["root_starttime_ticks"] += 1
            write_json(summary_path, changed)
            with self.assertRaisesRegex(gate.GateError, "exact bound role"):
                validate()

            write_rss_release_fixture(root, summary_path)
            rss_ready_path.chmod(0o644)
            with self.assertRaisesRegex(gate.GateError, "exact empty mode 0444"):
                validate()

            write_rss_release_fixture(root, summary_path)
            control_path.chmod(0o644)
            control = json.loads(control_path.read_text(encoding="utf-8"))
            control["schema"] = gate.GUARDIAN_ROOT_CONTROL_SCHEMA
            write_json(control_path, control)
            control_path.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "exact bound role"):
                validate()

    def test_allocator_telemetry_requires_epochs_null_system_fields_and_rss_alignment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            checkpoint = root / "checkpoint.tsv"
            rss = root / "rss.json"
            rss_samples = root / "rss.tsv"
            telemetry = root / "telemetry.ndjson"
            checkpoint.write_text(checkpoint_text(), encoding="utf-8")
            write_rss_release_fixture(root, rss)
            rss_samples.write_text(rss_samples_text(), encoding="utf-8")

            summary = allocator_telemetry_summary("J2")
            telemetry.write_text(
                "\n".join(json.dumps(row) for row in summary["records"]) + "\n",
                encoding="utf-8",
            )
            parsed = gate.parse_allocator_telemetry(
                telemetry,
                checkpoint,
                rss_samples,
                rss,
                plan,
                expectations,
                "J2",
            )
            self.assertEqual(parsed["records"][1]["epoch"], 2)
            self.assertEqual(
                parsed["hold_complete_minus_post_drop_bytes"]["resident_bytes"],
                -400,
            )
            self.assertEqual(
                parsed["external_rss_reconciliation"][0]["measurement_relation"],
                gate.ALLOCATOR_RSS_RELATION,
            )

            rows = copy.deepcopy(summary["records"])
            rows[1]["epoch"] = rows[0]["epoch"]
            telemetry.write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "epoch did not advance"):
                gate.parse_allocator_telemetry(
                    telemetry,
                    checkpoint,
                    rss_samples,
                    rss,
                    plan,
                    expectations,
                    "J2",
                )

            system = allocator_telemetry_summary("S")
            system["records"][0]["resident_bytes"] = 1
            telemetry.write_text(
                "\n".join(json.dumps(row) for row in system["records"]) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "explicit null"):
                gate.parse_allocator_telemetry(
                    telemetry,
                    checkpoint,
                    rss_samples,
                    rss,
                    plan,
                    expectations,
                    "S",
                )

    def test_compare_and_final_seal_require_all_runs_hashes_and_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            build_path = root / "build-provenance.json"
            write_json(build_path, build_provenance())
            build_digest = hashlib.sha256(build_path.read_bytes()).hexdigest()
            storage_path = root / "storage.json"
            readbacks_path = root / "readbacks.md"
            correctness_path = root / "correctness.json"
            corpus_path = root / "corpus.json"
            calibration_path = root / "calibration.json"
            calibration_storage_path = root / "calibration-storage.json"
            calibration_readbacks_path = root / "calibration-readbacks.md"
            calibration_correctness_path = root / "calibration-correctness.json"
            calibration_corpus_path = root / "calibration-corpus.json"
            capture_before_path = root / "capture-before.json"
            capture_after_path = root / "capture-after.json"
            write_json(storage_path, storage_report())
            readbacks_path.write_text(readback_report(), encoding="utf-8")
            write_json(correctness_path, replay_correctness())
            write_json(corpus_path, corpus_summary())
            write_json(calibration_storage_path, storage_report())
            calibration_readbacks_path.write_text(
                readback_report(), encoding="utf-8"
            )
            write_json(calibration_correctness_path, replay_correctness())
            write_json(calibration_corpus_path, corpus_summary())
            write_json(capture_before_path, capture_inventory())
            write_json(capture_after_path, capture_inventory())
            correctness_digest = hashlib.sha256(correctness_path.read_bytes()).hexdigest()
            write_json(
                calibration_path,
                gate.create_calibration(
                    calibration_storage_path,
                    calibration_readbacks_path,
                    calibration_correctness_path,
                    calibration_corpus_path,
                    build_path,
                    plan,
                    expectations,
                ),
            )
            observation_paths = []
            for run_index, policy in enumerate(gate.EXPECTED_SCHEDULE, start=1):
                path = root / f"observation-{run_index}.json"
                observation = synthetic_observation(run_index, policy)
                observation["build_provenance_sha256"] = build_digest
                observation["correctness_sha256"] = correctness_digest
                write_json(path, observation)
                observation_paths.append(path)

            summary = gate.compare_screen(observation_paths, plan, expectations)
            self.assertEqual(summary["run_count"], 10)
            self.assertNotIn("J0", summary["eligible_policies"])
            self.assertTrue(summary["candidates"]["J0"]["comparator_only"])
            self.assertEqual(summary["selected_full_gate_policy"], "J1")
            self.assertFalse(summary["partial_runs_promotable"])
            self.assertFalse(summary["candidates"]["J0"]["production_promotable"])
            self.assertEqual(
                summary["candidates"]["J0"]["workload_cpu_improvement_percent"],
                5.0,
            )
            self.assertEqual(
                summary["policy_medians"]["J0"]["total_lifecycle_task_clock"],
                120.0,
            )
            self.assertEqual(
                summary["policy_medians"]["J0"]["total_lifecycle_peak_rss_kib"],
                2_000.0,
            )

            summary_path = root / "summary.json"
            validation_path = root / "validation.json"
            write_json(summary_path, summary)
            write_json(
                validation_path,
                gate.gate_validation(
                    storage_path,
                    readbacks_path,
                    correctness_path,
                    corpus_path,
                    calibration_path,
                    calibration_storage_path,
                    calibration_readbacks_path,
                    calibration_correctness_path,
                    calibration_corpus_path,
                    build_path,
                    plan,
                    expectations,
                ),
            )
            sealed = gate.seal_screen(
                observation_paths,
                summary_path,
                validation_path,
                storage_path,
                readbacks_path,
                correctness_path,
                corpus_path,
                calibration_path,
                calibration_storage_path,
                calibration_readbacks_path,
                calibration_correctness_path,
                calibration_corpus_path,
                capture_before_path,
                capture_after_path,
                build_path,
                plan,
                expectations,
            )
            self.assertFalse(sealed["production_promotion_authorized"])

            mutated_storage = storage_report()
            mutated_storage["metadata_cache_hits"] = 1
            write_json(storage_path, mutated_storage)
            with self.assertRaisesRegex(
                gate.GateError, "recomputed raw storage/readback inputs"
            ):
                gate.seal_screen(
                    observation_paths,
                    summary_path,
                    validation_path,
                    storage_path,
                    readbacks_path,
                    correctness_path,
                    corpus_path,
                    calibration_path,
                    calibration_storage_path,
                    calibration_readbacks_path,
                    calibration_correctness_path,
                    calibration_corpus_path,
                    capture_before_path,
                    capture_after_path,
                    build_path,
                    plan,
                    expectations,
                )
            write_json(storage_path, storage_report())

            with self.assertRaisesRegex(gate.GateError, "exactly ten"):
                gate.compare_screen(observation_paths[:-1], plan, expectations)

            changed = json.loads(observation_paths[1].read_text(encoding="utf-8"))
            changed["binary_sha256"] = "9" * 64
            write_json(observation_paths[1], changed)
            with self.assertRaisesRegex(gate.GateError, "changed the jemalloc binary"):
                gate.compare_screen(observation_paths, plan, expectations)

            restored = synthetic_observation(2, "J0")
            restored["build_provenance_sha256"] = build_digest
            restored["correctness_sha256"] = correctness_digest
            write_json(observation_paths[1], restored)
            uncertain = synthetic_observation(2, "J0")
            uncertain["build_provenance_sha256"] = build_digest
            uncertain["correctness_sha256"] = correctness_digest
            uncertain["workload_cpu_boundary_uncertainty_ns"] = 100_000_001
            write_json(observation_paths[1], uncertain)
            with self.assertRaisesRegex(gate.GateError, "boundary is too uncertain"):
                gate.compare_screen(observation_paths, plan, expectations)

            restored = synthetic_observation(2, "J0")
            restored["build_provenance_sha256"] = build_digest
            restored["correctness_sha256"] = correctness_digest
            write_json(observation_paths[1], restored)
            invalid_validation = json.loads(validation_path.read_text(encoding="utf-8"))
            invalid_validation["readback_skipped"] = 1
            write_json(validation_path, invalid_validation)
            with self.assertRaisesRegex(gate.GateError, "differs from recomputed"):
                gate.seal_screen(
                    observation_paths,
                    summary_path,
                    validation_path,
                    storage_path,
                    readbacks_path,
                    correctness_path,
                    corpus_path,
                    calibration_path,
                    calibration_storage_path,
                    calibration_readbacks_path,
                    calibration_correctness_path,
                    calibration_corpus_path,
                    capture_before_path,
                    capture_after_path,
                    build_path,
                    plan,
                    expectations,
                )

    def test_canonical_validation_rejects_readback_skips(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            storage = root / "storage.json"
            readbacks = root / "readbacks.md"
            correctness = root / "correctness.json"
            corpus = root / "corpus.json"
            calibration = root / "calibration.json"
            build = root / "build.json"
            write_json(correctness, replay_correctness())
            write_json(storage, storage_report())
            write_json(corpus, corpus_summary())
            write_json(build, build_provenance())
            readbacks.write_text(readback_report(), encoding="utf-8")
            write_json(
                calibration,
                gate.create_calibration(
                    storage,
                    readbacks,
                    correctness,
                    corpus,
                    build,
                    PLAN,
                    EXPECTATIONS,
                ),
            )
            self.assertTrue(
                gate.gate_validation(
                    storage,
                    readbacks,
                    correctness,
                    corpus,
                    calibration,
                    storage,
                    readbacks,
                    correctness,
                    corpus,
                    build,
                    PLAN,
                    EXPECTATIONS,
                )["complete"]
            )
            readbacks.write_text(
                readback_report().replace(
                    "| Executed Readback Queries | 40 |",
                    "| Executed Readback Queries | 0 |",
                ).replace(
                    "| Skipped Readback Queries | 0 |",
                    "| Skipped Readback Queries | 1 |",
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "coverage is incomplete"):
                gate.gate_validation(
                    storage,
                    readbacks,
                    correctness,
                    corpus,
                    calibration,
                    storage,
                    readbacks,
                    correctness,
                    corpus,
                    build,
                    PLAN,
                    EXPECTATIONS,
                )

    def test_exact_250k_correctness_rejects_small_self_consistent_runs(self) -> None:
        value = replay_correctness()
        gate.validate_replay_correctness(value, 250_000)
        zero_old = copy.deepcopy(value)
        zero_old["datapoint_policy_totals"]["Dropped Too Old"] = 0
        zero_old["datapoint_policy_totals"]["Dropped Too Future"] = 10
        zero_old["event_time_skew_ranges"].pop("Dropped Too Old")
        zero_old["event_time_skew_ranges"]["Dropped Too Future"]["count"] = 10
        gate.validate_replay_correctness(zero_old, 250_000)
        stale_zero_row = copy.deepcopy(zero_old)
        stale_zero_row["event_time_skew_ranges"]["Dropped Too Old"] = {
            "count": 0,
            "min_ms": 0,
            "max_ms": 0,
        }
        with self.assertRaisesRegex(gate.GateError, "keys differ"):
            gate.validate_replay_correctness(stale_zero_row, 250_000)
        missing_positive_row = copy.deepcopy(value)
        missing_positive_row["event_time_skew_ranges"].pop("Dropped Too Old")
        with self.assertRaisesRegex(gate.GateError, "keys differ"):
            gate.validate_replay_correctness(missing_positive_row, 250_000)
        wrong_positive_count = copy.deepcopy(value)
        wrong_positive_count["event_time_skew_ranges"]["Dropped Too Old"]["count"] = 4
        with self.assertRaisesRegex(gate.GateError, "count differs"):
            gate.validate_replay_correctness(wrong_positive_count, 250_000)
        typed_invalid = copy.deepcopy(value)
        typed_invalid["general"]["Missing Number Value"] = 1
        typed_invalid["general"]["Invalid Typed Value"] = 1
        typed_invalid["datapoint_storage_totals"]["Missing Number Value"] = 1
        typed_invalid["datapoint_storage_totals"]["Invalid Typed Value"] = 1
        gate.validate_replay_correctness(typed_invalid, 250_000)
        typed_invalid["general"]["Invalid Typed Value"] = 0
        with self.assertRaisesRegex(gate.GateError, "invalid-typed counts differ"):
            gate.validate_replay_correctness(typed_invalid, 250_000)
        small = copy.deepcopy(value)
        small["general"]["Total Messages"] = 1
        small["partition_watermarks"]["Tracked Messages"] = 1
        with self.assertRaisesRegex(gate.GateError, "exactly 250000"):
            gate.validate_replay_correctness(small, 250_000)
        broken = copy.deepcopy(value)
        broken["datapoint_policy_totals"]["Rejected Total"] = 9
        with self.assertRaisesRegex(gate.GateError, "counter algebra"):
            gate.validate_replay_correctness(broken, 250_000)

    def test_storage_validation_is_exact_exhaustive_and_positive(self) -> None:
        gate.validate_storage_report(storage_report(), 88)
        raw_f64 = storage_report()
        raw_row = raw_f64["chunk_inventory"]["by_kind_encoding"][0]
        raw_row["encoding"] = "raw_f64"
        raw_row["payload_layout"] = "t0_interleaved_dt_value"
        raw_f64["chunk_inventory"]["timestamp_candidates"]["by_kind_encoding"][0][
            "encoding"
        ] = "raw_f64"
        with self.assertRaisesRegex(gate.GateError, "frozen Gorilla contract"):
            gate.validate_storage_report(raw_f64, 88)
        mutations = (
            (lambda value: value.update({"fabricated": 1}), "keys differ"),
            (
                lambda value: value.__setitem__("series_sample_per_segment", 1),
                "every corpus series",
            ),
            (lambda value: value.__setitem__("series", 2), "every corpus series"),
            (
                lambda value: value.__setitem__("chunks_by_kind", [0, 0, 0, 0, 0]),
                "do not sum",
            ),
            (
                lambda value: value.__setitem__("metadata_read_bytes", 0),
                "must be positive",
            ),
            (
                lambda value: value["exact_postings"].__setitem__("lists", 0),
                "must be >= 1",
            ),
            (
                lambda value: value.__setitem__(
                    "decoded_semantic_fingerprint", "not-a-digest"
                ),
                "decoded_semantic_fingerprint is invalid",
            ),
            (
                lambda value: value["chunk_inventory"]["by_kind_encoding"][0].__setitem__(
                    "indexed_bytes", 1
                ),
                "indexed bytes do not reconcile",
            ),
            (
                lambda value: value["chunk_inventory"]["timestamp_candidates"][
                    "all_blocks"
                ]["current_offset_uleb"].__setitem__("bytes", 94),
                "current bytes differ",
            ),
        )
        for mutate, message in mutations:
            value = copy.deepcopy(storage_report())
            mutate(value)
            with self.subTest(message=message), self.assertRaisesRegex(
                gate.GateError, message
            ):
                gate.validate_storage_report(value, 88)

    def test_storage_completeness_fails_before_readback_on_sample_shortfall(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            storage = root / "storage.json"
            correctness = root / "correctness.json"
            write_json(storage, storage_report())
            write_json(correctness, replay_correctness())
            self.assertTrue(
                gate.check_storage_completeness(
                    storage, correctness, PLAN, EXPECTATIONS
                )["complete"]
            )
            short = replay_correctness()
            short["general"]["Recorded Samples"] = 87
            short["general"]["Missing Number Value"] = 2
            short["general"]["Invalid Typed Value"] = 1
            short["datapoint_storage_totals"]["Recorded Samples"] = 87
            short["datapoint_storage_totals"]["Missing Number Value"] = 2
            short["datapoint_storage_totals"]["Invalid Typed Value"] = 1
            short["datapoint_storage_totals"]["Accepted Not Recorded"] = 3
            write_json(correctness, short)
            with self.assertRaisesRegex(gate.GateError, "storage sample count differs"):
                gate.check_storage_completeness(
                    storage, correctness, PLAN, EXPECTATIONS
                )

    def test_runner_checks_storage_completeness_before_each_readback(self) -> None:
        source = (HERE / "phase5_allocator_screen_run.sh").read_text(encoding="utf-8")
        self.assertEqual(source.count("check-storage-completeness"), 2)
        calibration_verify = source.index(
            '>"$CALIBRATION_DIR/storage-verify.json"'
        )
        calibration_check = source.index(
            'check-storage-completeness', calibration_verify
        )
        calibration_readback = source.index(
            '"$RUN_QUERY" --segments-dir "$CALIBRATION_SEGMENTS_DIR"',
            calibration_check,
        )
        self.assertLess(calibration_verify, calibration_check)
        self.assertLess(calibration_check, calibration_readback)
        canonical_verify = source.index('>"$VALIDATION_DIR/storage-verify.json"')
        canonical_check = source.index("check-storage-completeness", canonical_verify)
        canonical_readback = source.index(
            '"$RUN_QUERY" --segments-dir "$RUNS_DIR/run-01-S/segments"',
            canonical_check,
        )
        self.assertLess(canonical_verify, canonical_check)
        self.assertLess(canonical_check, canonical_readback)

    def test_250k_calibration_rejects_fabrication_and_row_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            storage = root / "storage.json"
            readbacks = root / "readbacks.md"
            correctness = root / "correctness.json"
            corpus = root / "corpus.json"
            build = root / "build.json"
            calibration = root / "calibration.json"
            write_json(storage, storage_report())
            readbacks.write_text(readback_report(), encoding="utf-8")
            write_json(correctness, replay_correctness())
            write_json(corpus, corpus_summary())
            write_json(build, build_provenance())
            expected = gate.create_calibration(
                storage,
                readbacks,
                correctness,
                corpus,
                build,
                PLAN,
                EXPECTATIONS,
            )
            fabricated = copy.deepcopy(expected)
            fabricated["promql_rows_fingerprint_sha256"] = "9" * 64
            write_json(calibration, fabricated)
            with self.assertRaisesRegex(gate.GateError, "differs from raw calibration"):
                gate.gate_validation(
                    storage,
                    readbacks,
                    correctness,
                    corpus,
                    calibration,
                    storage,
                    readbacks,
                    correctness,
                    corpus,
                    build,
                    PLAN,
                    EXPECTATIONS,
                )

            write_json(calibration, expected)
            mutated_rows = readback_report().replace(
                '`up{instance="0"}`', '`up{instance="mutated"}`'
            )
            final_readbacks = root / "final-readbacks.md"
            final_readbacks.write_text(mutated_rows, encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "pre-run 250k calibration"):
                gate.gate_validation(
                    storage,
                    final_readbacks,
                    correctness,
                    corpus,
                    calibration,
                    storage,
                    readbacks,
                    correctness,
                    corpus,
                    build,
                    PLAN,
                    EXPECTATIONS,
                )

            for field in ("decoded", "inventory"):
                with self.subTest(field=field):
                    final_storage_value = storage_report()
                    if field == "decoded":
                        final_storage_value["decoded_semantic_fingerprint"] = "d" * 64
                    else:
                        final_storage_value["chunk_inventory"]["timestamp_candidates"][
                            "scope"
                        ] = "same bytes, deliberately different bound inventory evidence"
                    final_storage = root / f"final-storage-{field}.json"
                    write_json(final_storage, final_storage_value)
                    with self.assertRaisesRegex(
                        gate.GateError, "pre-run 250k calibration"
                    ):
                        gate.gate_validation(
                            final_storage,
                            readbacks,
                            correctness,
                            corpus,
                            calibration,
                            storage,
                            readbacks,
                            correctness,
                            corpus,
                            build,
                            PLAN,
                            EXPECTATIONS,
                        )

    def test_heaptrack_profile_is_untimed_system_authority_and_rejects_loss(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            screen = root / "screen"
            profile = root / "profile"
            binary_dir = screen / "metadata/binaries"
            source_dir = screen / "metadata/source"
            build_source_dir = screen / "build-source"
            harness_dir = screen / "metadata/harness"
            calibration_dir = screen / "calibration"
            comparison_dir = screen / "comparisons"
            reference_dir = screen / "runs/run-01-S"
            for directory in (
                binary_dir,
                source_dir,
                build_source_dir / ".cargo",
                harness_dir,
                calibration_dir,
                comparison_dir,
                reference_dir,
                profile,
            ):
                directory.mkdir(parents=True, exist_ok=True)
            binaries = {
                "system": executable(
                    binary_dir,
                    "chronoxide-ingester-system",
                    b"frozen system allocator",
                ),
                "jemalloc": executable(
                    binary_dir,
                    "chronoxide-ingester-jemalloc",
                    b"frozen jemalloc allocator",
                ),
                "query": executable(binary_dir, "chronoxide-query", b"frozen query"),
                "storage_verify": executable(
                    binary_dir, "chronoxide-storage-verify", b"frozen verifier"
                ),
            }
            source = source_dir / "formal-source-seal.json"
            source_value = source_seal_document()
            write_json(source, source_value)
            source_archive = source_dir / "git-head.tar"
            source_archive.write_bytes(b"frozen git archive\n")
            source_archive.chmod(0o444)
            for relative, contents in (
                ("Cargo.toml", "[workspace]\n"),
                ("Cargo.lock", "# lock\n"),
                (".cargo/config.toml", "[build]\nrustflags = []\n"),
            ):
                path = build_source_dir / relative
                path.write_text(contents, encoding="utf-8")
                path.chmod(0o444)
            archive_digest = hashlib.sha256(source_archive.read_bytes()).hexdigest()
            extracted = source_dir / "extracted-build-source-seal.json"
            extracted_value = extracted_source_seal_document(
                source_root=str(build_source_dir),
                archive_path=str(source_archive),
                archive_sha256=archive_digest,
                live_source_seal_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                live_source_identity_sha256=str(source_value["identity_sha256"]),
            )
            extracted_value["archive_size_bytes"] = source_archive.stat().st_size
            write_json(extracted, extracted_value)
            build_value = build_provenance()
            build_value["source_seal_sha256"] = hashlib.sha256(
                source.read_bytes()
            ).hexdigest()
            build_value["source_identity_sha256"] = source_value["identity_sha256"]
            build_value["build_source"].update(
                {
                    "root": str(build_source_dir),
                    "archive_path": str(source_archive),
                    "archive_sha256": archive_digest,
                    "archive_size_bytes": source_archive.stat().st_size,
                    "extracted_source_seal_sha256": hashlib.sha256(
                        extracted.read_bytes()
                    ).hexdigest(),
                }
            )
            build_value["binary_sha256"] = {
                role: hashlib.sha256(path.read_bytes()).hexdigest()
                for role, path in binaries.items()
            }
            build = screen / "metadata/build-provenance.json"
            write_json(build, build_value)
            for name in (
                "phase5_allocator_screen_gate.py",
                "phase5_allocator_screen_plan.json",
                "phase1_4m_expectations.json",
            ):
                (harness_dir / name).write_text(f"frozen {name}\n", encoding="utf-8")
            manifest_bytes = b"same\n"
            manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
            corpus_value = corpus_summary()
            corpus_value["manifest_sha256"] = manifest_digest
            storage = profile / "storage.json"
            readbacks = profile / "readbacks.md"
            correctness = profile / "correctness.json"
            corpus = profile / "corpus.json"
            write_json(storage, storage_report())
            readbacks.write_text(readback_report(), encoding="utf-8")
            write_json(correctness, replay_correctness())
            write_json(corpus, corpus_value)
            reference_correctness = reference_dir / "replay-correctness.json"
            reference_corpus = reference_dir / "corpus-summary.json"
            reference_manifest = reference_dir / "segments.sha256"
            write_json(reference_correctness, replay_correctness())
            write_json(reference_corpus, corpus_value)
            reference_manifest.write_bytes(manifest_bytes)
            calibration_storage = calibration_dir / "storage-verify.json"
            calibration_readbacks = calibration_dir / "readbacks.md"
            calibration_correctness = calibration_dir / "replay-correctness.json"
            calibration_corpus = calibration_dir / "corpus-summary.json"
            write_json(calibration_storage, storage_report())
            calibration_readbacks.write_text(readback_report(), encoding="utf-8")
            write_json(calibration_correctness, replay_correctness())
            write_json(calibration_corpus, corpus_value)
            calibration = calibration_dir / "calibration.json"
            write_json(
                calibration,
                gate.create_calibration(
                    calibration_storage,
                    calibration_readbacks,
                    calibration_correctness,
                    calibration_corpus,
                    build,
                    PLAN,
                    EXPECTATIONS,
                ),
            )
            final = comparison_dir / "final-screen-decision.json"
            write_json(
                final,
                {
                    "schema": gate.FINAL_DECISION_SCHEMA,
                    "screen_complete": True,
                    "canonical_validation_complete": True,
                    "run_count": 10,
                    "production_promotion_authorized": False,
                    "selected_full_gate_policy": "J1",
                    "build_provenance_sha256": hashlib.sha256(
                        build.read_bytes()
                    ).hexdigest(),
                    "calibration_sha256": hashlib.sha256(
                        calibration.read_bytes()
                    ).hexdigest(),
                    "binary_sha256_by_role": {
                        "system": build_value["binary_sha256"]["system"],
                        "jemalloc": build_value["binary_sha256"]["jemalloc"],
                    },
                },
            )
            create_required_screen_control_seals(
                screen, binaries, source, source_archive, extracted, build
            )
            artifact_manifest = write_artifact_manifest(screen)
            complete = screen / "COMPLETE"
            complete.write_text(
                "chronoxide/allocator-screen-complete/v1\n", encoding="utf-8"
            )
            complete.chmod(0o444)
            profile_data = profile / "heaptrack.trace.zst"
            profiler_log = profile / "heaptrack.log"
            analysis = profile / "heaptrack-stacks.txt"
            lost = profile / "lost-events.txt"
            profile_manifest = profile / "profile.sha256"
            profile_data.write_bytes(b"trace")
            profiler_log.write_text(
                "heaptrack stats:\n\tallocations:\t10\n", encoding="utf-8"
            )
            analysis.write_text(
                "chronoxide_ingester::processor::process;alloc::alloc::alloc 10\n",
                encoding="utf-8",
            )
            lost.write_text("", encoding="utf-8")
            profile_manifest.write_bytes(manifest_bytes)

            evidence = gate.record_profile_evidence(
                "heaptrack",
                "S",
                binaries["system"],
                screen,
                artifact_manifest,
                binaries["system"],
                binaries["jemalloc"],
                binaries["query"],
                binaries["storage_verify"],
                profile_data,
                profiler_log,
                analysis,
                lost,
                profile_manifest,
                reference_manifest,
                correctness,
                reference_correctness,
                corpus,
                reference_corpus,
                storage,
                readbacks,
                calibration,
                calibration_storage,
                calibration_readbacks,
                calibration_correctness,
                calibration_corpus,
                final,
                complete,
                build,
                None,
                None,
                PLAN,
                EXPECTATIONS,
            )
            self.assertTrue(evidence["heap_allocation_stack_authority"])
            self.assertEqual(
                evidence["stack_evidence"]["format"],
                "heaptrack-collapsed-stacks/v1",
            )
            self.assertEqual(evidence["stack_evidence"]["chronoxide_stack_count"], 1)
            self.assertFalse(evidence["measurement_eligible"])
            self.assertFalse(evidence["a_b_timing_or_rss_evidence"])

            lost.write_text("PERF_RECORD_LOST 1\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "lost samples/events"):
                gate.record_profile_evidence(
                    "heaptrack",
                    "S",
                    binaries["system"],
                    screen,
                    artifact_manifest,
                    binaries["system"],
                    binaries["jemalloc"],
                    binaries["query"],
                    binaries["storage_verify"],
                    profile_data,
                    profiler_log,
                    analysis,
                    lost,
                    profile_manifest,
                    reference_manifest,
                    correctness,
                    reference_correctness,
                    corpus,
                    reference_corpus,
                    storage,
                    readbacks,
                    calibration,
                    calibration_storage,
                    calibration_readbacks,
                    calibration_correctness,
                    calibration_corpus,
                    final,
                    complete,
                    build,
                    None,
                    None,
                    PLAN,
                    EXPECTATIONS,
                )

            lost.write_text("", encoding="utf-8")
            binaries["storage_verify"].chmod(0o755)
            binaries["storage_verify"].write_bytes(b"mutated verifier")
            binaries["storage_verify"].chmod(0o555)
            with self.assertRaisesRegex(gate.GateError, "artifact digest changed"):
                gate.record_profile_evidence(
                    "heaptrack",
                    "S",
                    binaries["system"],
                    screen,
                    artifact_manifest,
                    binaries["system"],
                    binaries["jemalloc"],
                    binaries["query"],
                    binaries["storage_verify"],
                    profile_data,
                    profiler_log,
                    analysis,
                    lost,
                    profile_manifest,
                    reference_manifest,
                    correctness,
                    reference_correctness,
                    corpus,
                    reference_corpus,
                    storage,
                    readbacks,
                    calibration,
                    calibration_storage,
                    calibration_readbacks,
                    calibration_correctness,
                    calibration_corpus,
                    final,
                    complete,
                    build,
                    None,
                    None,
                    PLAN,
                    EXPECTATIONS,
                )

    def test_build_provenance_is_stats_enabled_and_fail_closed_for_production(self) -> None:
        provenance = build_provenance()
        gate.validate_build_provenance(provenance)
        changed = copy.deepcopy(provenance)
        changed["jemalloc_stats_enabled"] = False
        with self.assertRaisesRegex(gate.GateError, "stats-enabled"):
            gate.validate_build_provenance(changed)
        changed = copy.deepcopy(provenance)
        changed["no_stats_production_build_validated"] = True
        with self.assertRaisesRegex(gate.GateError, "no-stats"):
            gate.validate_build_provenance(changed)
        changed = copy.deepcopy(provenance)
        changed["build_source"]["live_worktree_used_as_build_source"] = True
        with self.assertRaisesRegex(gate.GateError, "mutable or live-worktree"):
            gate.validate_build_provenance(changed)
        changed = copy.deepcopy(provenance)
        changed["build_source"]["all_manifest_paths_within_source"] = False
        with self.assertRaisesRegex(gate.GateError, "mutable or live-worktree"):
            gate.validate_build_provenance(changed)

    def test_build_input_audit_rejects_hidden_flags_symlinks_and_gitlinks(self) -> None:
        def repository(root: Path) -> Path:
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(
                ["git", "-C", str(root), "config", "user.email", "test@example.com"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(root), "config", "user.name", "Test"],
                check=True,
            )
            (root / "input").write_text("input\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(root), "add", "input"], check=True)
            subprocess.run(
                ["git", "-C", str(root), "commit", "-qm", "test"], check=True
            )
            return root

        for flag in ("--assume-unchanged", "--skip-worktree"):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as temporary:
                repo = repository(Path(temporary))
                subprocess.run(
                    ["git", "-C", str(repo), "update-index", flag, "input"],
                    check=True,
                )
                with self.assertRaisesRegex(
                    gate.GateError, "assume-unchanged/skip-worktree"
                ):
                    gate.audit_tracked_build_inputs(repo)

        with tempfile.TemporaryDirectory() as temporary:
            repo = repository(Path(temporary))
            (repo / "link").symlink_to("input")
            subprocess.run(["git", "-C", str(repo), "add", "link"], check=True)
            with self.assertRaisesRegex(gate.GateError, "tracked symlink"):
                gate.audit_tracked_build_inputs(repo)

        with tempfile.TemporaryDirectory() as temporary:
            repo = repository(Path(temporary))
            commit = subprocess.run(
                ["git", "-C", str(repo), "rev-parse", "HEAD"],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
            ).stdout.strip()
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    f"160000,{commit},nested",
                ],
                check=True,
            )
            with self.assertRaisesRegex(gate.GateError, "tracked gitlink"):
                gate.audit_tracked_build_inputs(repo)

    def test_source_seal_rejects_untracked_ignored_and_changed_build_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.email", "test@example.com"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.name", "Test"],
                check=True,
            )
            (repo / "Cargo.lock").write_text("# lock\n", encoding="utf-8")
            (repo / "tracked.rs").write_text("pub const VALUE: u8 = 1;\n", encoding="utf-8")
            (repo / ".gitignore").write_text(
                "ignored.rs\n.cargo/config\n", encoding="utf-8"
            )
            subprocess.run(
                ["git", "-C", str(repo), "add", "Cargo.lock", "tracked.rs", ".gitignore"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repo), "commit", "-qm", "sealed"], check=True
            )
            sealed = gate.source_seal(repo)
            seal_path = root / "source-seal.json"
            write_json(seal_path, sealed)
            self.assertEqual(gate.check_source_seal(repo, seal_path)["status"], "pass")

            (repo / "build.rs").write_text("fn main() {}\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "untracked build input"):
                gate.source_seal(repo)
            (repo / "build.rs").unlink()

            (repo / "ignored.rs").write_text("pub const HIDDEN: bool = true;\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "ignored source/build input"):
                gate.source_seal(repo)
            (repo / "ignored.rs").unlink()

            (repo / ".cargo").mkdir()
            (repo / ".cargo/config").write_text("[build]\nrustflags=[]\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "ignored source/build input"):
                gate.source_seal(repo)
            (repo / ".cargo/config").unlink()
            (repo / ".cargo").rmdir()

            (repo / "tracked.rs").write_text("pub const VALUE: u8 = 2;\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "tracked.rs"], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "commit", "-qm", "changed"], check=True
            )
            with self.assertRaisesRegex(gate.GateError, "source seal changed"):
                gate.check_source_seal(repo, seal_path)

    def test_build_source_is_exact_read_only_git_archive_not_ignored_live_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.email", "test@example.com"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.name", "Test"],
                check=True,
            )
            (repo / "src").mkdir()
            (repo / ".cargo").mkdir()
            (repo / ".cargo/config.toml").write_text(
                '[build]\nrustflags = "-C target-cpu=native"\n', encoding="utf-8"
            )
            (repo / "Cargo.toml").write_text(
                '[package]\nname = "archive-test"\nversion = "0.1.0"\nedition = "2024"\n',
                encoding="utf-8",
            )
            (repo / "Cargo.lock").write_text("# lock\n", encoding="utf-8")
            (repo / ".gitignore").write_text("payload.bin\n", encoding="utf-8")
            (repo / "src/lib.rs").write_text(
                'pub const PAYLOAD: &[u8] = include_bytes!("../payload.bin");\n',
                encoding="utf-8",
            )
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "commit", "-qm", "sealed"], check=True
            )
            (repo / "payload.bin").write_bytes(b"ignored live-worktree payload")

            live_seal = root / "live-source-seal.json"
            write_json(live_seal, gate.source_seal(repo.resolve()))
            archive = root / "git-head.tar"
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "archive",
                    "--format=tar",
                    f"--output={archive}",
                    "HEAD",
                ],
                check=True,
            )
            archive.chmod(0o444)
            build_source = root / "build-source"
            extracted = gate.extract_git_archive(
                repo.resolve(), archive.resolve(), build_source.resolve(), live_seal
            )
            extracted_path = root / "extracted-source-seal.json"
            write_json(extracted_path, extracted)
            self.assertFalse((build_source / "payload.bin").exists())
            self.assertTrue(extracted["archive_tree_equivalent"])
            self.assertFalse(extracted["live_worktree_used_as_build_source"])
            self.assertEqual(
                gate.check_extracted_source_seal(
                    repo.resolve(),
                    build_source.resolve(),
                    archive.resolve(),
                    live_seal,
                    extracted_path,
                )["status"],
                "pass",
            )

            archive_bytes = archive.read_bytes()
            try:
                archive.chmod(0o644)
                archive.write_bytes(archive_bytes + b"mutated archive")
                archive.chmod(0o444)
                with self.assertRaises(gate.GateError):
                    gate.check_extracted_source_seal(
                        repo.resolve(),
                        build_source.resolve(),
                        archive.resolve(),
                        live_seal,
                        extracted_path,
                    )
            finally:
                archive.chmod(0o644)
                archive.write_bytes(archive_bytes)
                archive.chmod(0o444)

            try:
                build_source.chmod(0o755)
                injected = build_source / "payload.bin"
                injected.write_bytes(b"must never enter the build source")
                injected.chmod(0o444)
                build_source.chmod(0o555)
                with self.assertRaisesRegex(
                    gate.GateError, "path outside Git HEAD"
                ):
                    gate.check_extracted_source_seal(
                        repo.resolve(),
                        build_source.resolve(),
                        archive.resolve(),
                        live_seal,
                        extracted_path,
                    )
            finally:
                for path in sorted(
                    build_source.rglob("*"), key=lambda item: len(item.parts)
                ):
                    if path.is_dir():
                        path.chmod(0o755)
                    else:
                        path.chmod(0o644)
                build_source.chmod(0o755)

    def test_extracted_cargo_manifest_cannot_escape_the_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            outside = root / "outside"
            (source / ".cargo").mkdir(parents=True)
            outside.mkdir()
            (source / ".cargo/config.toml").write_text(
                '[build]\nrustflags = "-C target-cpu=native"\n', encoding="utf-8"
            )
            (source / "Cargo.toml").write_text(
                '[package]\nname = "escape"\nversion = "0.1.0"\n'
                '[dependencies]\noutside = { path = "../outside" }\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "escapes the sealed source"):
                gate.validate_snapshot_cargo_inputs(source.resolve())

    def test_executable_seal_rejects_writable_and_mutated_tools(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = {
                "system": executable(root, "system", b"system"),
                "jemalloc": executable(root, "jemalloc", b"jemalloc"),
                "query": executable(root, "query", b"query"),
                "storage_verify": executable(root, "verify", b"verify"),
            }
            value = build_provenance()
            value["binary_sha256"] = {
                role: hashlib.sha256(path.read_bytes()).hexdigest()
                for role, path in paths.items()
            }
            build = root / "build.json"
            write_json(build, value)
            gate.validate_executable_set(
                build,
                paths["system"],
                paths["jemalloc"],
                paths["query"],
                paths["storage_verify"],
            )
            paths["query"].chmod(0o755)
            with self.assertRaisesRegex(gate.GateError, "non-writable"):
                gate.validate_executable_set(
                    build,
                    paths["system"],
                    paths["jemalloc"],
                    paths["query"],
                    paths["storage_verify"],
                )
            paths["query"].write_bytes(b"changed")
            paths["query"].chmod(0o555)
            with self.assertRaisesRegex(gate.GateError, "executable seal changed"):
                gate.validate_executable_set(
                    build,
                    paths["system"],
                    paths["jemalloc"],
                    paths["query"],
                    paths["storage_verify"],
                )

    def test_capture_reinventory_rejects_mid_run_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            before = root / "before.json"
            after = root / "after.json"
            write_json(before, capture_inventory())
            write_json(after, capture_inventory())
            gate.validate_capture_reinventory(before, after)
            changed = capture_inventory()
            changed["capture_files"][0]["sha256"] = "9" * 64
            write_json(after, changed)
            with self.assertRaisesRegex(gate.GateError, "changed during"):
                gate.validate_capture_reinventory(before, after)

    def test_dispersion_prevents_advancement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plan, expectations = copied_plan(root)
            paths = []
            for run_index, policy in enumerate(gate.EXPECTED_SCHEDULE, start=1):
                observation = synthetic_observation(run_index, policy)
                if run_index == 8:
                    observation["workload_cpu_ticks"] = 8_000
                    observation["workload_cpu_seconds"] = 80.0
                    observation["rss"]["workload_boundary_cpu_ticks"] = 8_000
                    observation["rss"]["workload_boundary_cpu_seconds"] = 80.0
                path = root / f"observation-{run_index}.json"
                write_json(path, observation)
                paths.append(path)
            summary = gate.compare_screen(paths, plan, expectations)
            self.assertFalse(summary["candidates"]["J1"]["mirrored_pair_dispersion_pass"])
            self.assertFalse(summary["candidates"]["J1"]["eligible_for_full_gate"])

    def test_profile_capacity_control_accepts_default_and_custom_reserves(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            for name, reserve in (
                ("default.json", 16 * 1024 * 1024 * 1024),
                ("custom.json", 24 * 1024 * 1024 * 1024),
            ):
                with self.subTest(reserve=reserve):
                    path = root / name
                    created = gate.create_profile_capacity_control(path, reserve)
                    self.assertEqual(created["profile_min_free_bytes"], reserve)
                    self.assertEqual(path.stat().st_mode & 0o777, 0o444)
                    self.assertEqual(
                        gate.validate_profile_capacity_control(path, reserve), created
                    )

    def test_profile_capacity_control_rejects_missing_and_tampered_authority(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            missing = root / "missing.json"
            with self.assertRaisesRegex(gate.GateError, "is missing"):
                gate.validate_profile_capacity_control(missing)

            control = root / "capacity.json"
            reserve = 16 * 1024 * 1024 * 1024
            gate.create_profile_capacity_control(control, reserve)
            control.chmod(0o644)
            with self.assertRaisesRegex(gate.GateError, "exact mode 0444"):
                gate.validate_profile_capacity_control(control, reserve)
            control.write_text(
                json.dumps(
                    {
                        "schema": gate.PROFILE_CAPACITY_CONTROL_SCHEMA,
                        "profile_min_free_bytes": reserve + 1,
                    }
                ),
                encoding="utf-8",
            )
            control.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "configured reserve"):
                gate.validate_profile_capacity_control(control, reserve)

    def test_profile_capacity_control_rejects_malformed_documents(self) -> None:
        reserve = 16 * 1024 * 1024 * 1024
        malformed = {
            "wrong-schema": {
                "schema": "wrong",
                "profile_min_free_bytes": reserve,
            },
            "below-floor": {
                "schema": gate.PROFILE_CAPACITY_CONTROL_SCHEMA,
                "profile_min_free_bytes": gate.CAPACITY_RESERVE_BYTES - 1,
            },
            "boolean": {
                "schema": gate.PROFILE_CAPACITY_CONTROL_SCHEMA,
                "profile_min_free_bytes": True,
            },
            "extra-key": {
                "schema": gate.PROFILE_CAPACITY_CONTROL_SCHEMA,
                "profile_min_free_bytes": reserve,
                "unexpected": 1,
            },
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            for name, value in malformed.items():
                with self.subTest(name=name):
                    path = root / f"{name}.json"
                    write_json(path, value)
                    path.chmod(0o444)
                    with self.assertRaises(gate.GateError):
                        gate.validate_profile_capacity_control(path)

    def test_profile_control_seal_requires_capacity_authority_input(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            capacity = root / "profile-capacity-control.json"
            gate.create_profile_capacity_control(
                capacity, 16 * 1024 * 1024 * 1024
            )
            config = root / "config.toml"
            config.write_text("[config]\n", encoding="utf-8")
            config.chmod(0o444)
            seal = root / "controls.json"
            write_json(seal, gate.control_seal([config]))
            seal.chmod(0o444)
            gate.check_profile_control_seal(seal, {config})
            with self.assertRaisesRegex(gate.GateError, "inputs differ"):
                gate.check_profile_control_seal(seal, {config, capacity})

    def test_profile_guardian_rejects_threshold_not_derived_from_capacity_control(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            capacity_control = root / "profile-capacity-control.json"
            reserve = 16 * 1024 * 1024 * 1024
            gate.create_profile_capacity_control(capacity_control, reserve)
            reference_corpus = root / "corpus-summary.json"
            write_json(reference_corpus, corpus_summary())
            expected = gate.derive_profile_guardian_minimum_free_bytes(
                capacity_control, reference_corpus
            )
            self.assertEqual(expected, reserve + 100)
            evidence, control, ready, launch, value = root_only_guardian_fixture(
                root, expected - 1
            )
            value["minimum_observed_free_bytes"] = expected
            write_json(evidence, value)
            with self.assertRaisesRegex(gate.GateError, "guardian did not pass"):
                gate.validate_guardian_evidence(
                    evidence,
                    control,
                    ready,
                    launch,
                    100,
                    root,
                    expected,
                    False,
                )

    def test_continuous_guardian_detects_a_transient_forbidden_process(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            control = root / "guardian-control.json"
            ready = root / "guardian-ready"
            launch = root / "guardian-launch"
            output = root / "guardian.json"
            benchmark = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'while [[ ! -e "$1" ]]; do sleep 0.001; done; sleep 1',
                    "bash",
                    str(launch),
                ]
            )
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    gate.monitor_external_conflicts(
                        benchmark.pid,
                        output,
                        10,
                        root.resolve(),
                        1,
                        control,
                        ready,
                        launch,
                    )
                except BaseException as error:
                    failures.append(error)

            guardian = threading.Thread(target=monitor)
            conflict: subprocess.Popen[bytes] | None = None
            conflict_pid: list[int | None] = [None]
            original_identity = gate.proc_identity

            def only_test_conflict(pid: int) -> tuple[str, str] | None:
                if pid == conflict_pid[0]:
                    return original_identity(pid)
                return None

            try:
                with mock.patch.object(
                    gate, "proc_identity", side_effect=only_test_conflict
                ):
                    guardian.start()
                    gate.create_guardian_control(
                        control, ready, launch, benchmark.pid, os.getpid(), 10
                    )
                    gate.wait_for_guardian_ready(control, ready, launch, 10, 2_000)
                    gate.release_guardian_launch(control, ready, launch, 10)
                    conflict = subprocess.Popen(
                        [
                            "python3",
                            "-c",
                            "import ctypes,time; ctypes.CDLL(None).prctl(15,b'make',0,0,0); time.sleep(0.2)",
                        ]
                    )
                    conflict_pid[0] = conflict.pid
                    guardian.join(timeout=3)
                self.assertFalse(guardian.is_alive())
                self.assertTrue(
                    any("external measurement conflict" in str(error) for error in failures)
                )
                evidence = json.loads(output.read_text(encoding="utf-8"))
                self.assertFalse(evidence["complete_and_conflict_free"])
                self.assertTrue(evidence["conflicts"])
            finally:
                if conflict is not None:
                    conflict.wait()
                if benchmark.poll() is None:
                    benchmark.kill()
                benchmark.wait()
                guardian.join(timeout=1)

    def test_continuous_guardian_does_not_classify_bound_root_zombie(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            control = root / "guardian-control.json"
            ready = root / "guardian-ready"
            launch = root / "guardian-launch"
            output = root / "guardian.json"
            measured = subprocess.Popen(
                [
                    "python3",
                    "-c",
                    (
                        "import ctypes,pathlib,sys,time; "
                        "ctypes.CDLL(None).prctl(15,b'chronoxide-inge',0,0,0); "
                        "launch=pathlib.Path(sys.argv[1]); "
                        "\nwhile not launch.exists(): time.sleep(0.001); "
                        "\ntime.sleep(0.35)"
                    ),
                    str(launch),
                ]
            )
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    gate.monitor_external_conflicts(
                        measured.pid,
                        output,
                        50,
                        root.resolve(),
                        1,
                        control,
                        ready,
                        launch,
                    )
                except BaseException as error:
                    failures.append(error)

            guardian = threading.Thread(target=monitor)
            original_identity = gate.proc_identity

            def only_measured_root(pid: int) -> tuple[str, str] | None:
                if pid == measured.pid:
                    return original_identity(pid)
                return None

            try:
                with mock.patch.object(
                    gate, "proc_identity", side_effect=only_measured_root
                ):
                    guardian.start()
                    gate.create_guardian_control(
                        control, ready, launch, measured.pid, os.getpid(), 50
                    )
                    gate.wait_for_guardian_ready(
                        control, ready, launch, 50, 2_000
                    )
                    gate.release_guardian_launch(control, ready, launch, 50)
                    guardian.join(timeout=3)
                self.assertFalse(guardian.is_alive())
                self.assertEqual(failures, [])
                root_identity = gate.read_process_stat_identity(measured.pid)
                self.assertIsNotNone(root_identity)
                assert root_identity is not None
                self.assertEqual(root_identity["state"], "Z")
                self.assertEqual(
                    original_identity(measured.pid), ("chronoxide-inge", "")
                )
                evidence = json.loads(output.read_text(encoding="utf-8"))
                self.assertEqual(evidence["conflicts"], [])
                self.assertEqual(evidence["terminal_poll"], evidence["polls"])
                self.assertTrue(evidence["complete_and_conflict_free"])
            finally:
                if measured.poll() is None:
                    measured.kill()
                measured.wait()
                if guardian.is_alive():
                    guardian.join(timeout=1)

    def test_continuous_guardian_retains_bound_descendant_zombie_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            control = root / "guardian-control.json"
            ready = root / "guardian-ready"
            launch = root / "guardian-launch"
            output = root / "guardian.json"
            child_pid_path = root / "child.pid"
            zombie_ready = root / "zombie-ready"
            allow_reap = root / "allow-reap"
            measured = subprocess.Popen(
                [
                    "python3",
                    "-c",
                    """
import ctypes
import os
import pathlib
import sys
import time

launch = pathlib.Path(sys.argv[1])
child_pid_path = pathlib.Path(sys.argv[2])
zombie_ready = pathlib.Path(sys.argv[3])
allow_reap = pathlib.Path(sys.argv[4])
while not launch.exists():
    time.sleep(0.001)
wrapper = os.fork()
if wrapper == 0:
    child = os.fork()
    if child == 0:
        ctypes.CDLL(None).prctl(15, b"chronoxide-inge", 0, 0, 0)
        os._exit(0)
    child_pid_path.write_text(str(child), encoding="ascii")
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        try:
            raw = pathlib.Path(f"/proc/{child}/stat").read_text(encoding="ascii")
        except FileNotFoundError:
            break
        if raw[raw.rfind(")") + 2 :].split()[0] == "Z":
            zombie_ready.touch()
            break
        time.sleep(0.005)
    while not allow_reap.exists():
        time.sleep(0.001)
    os.waitpid(child, 0)
    os._exit(0)
os.waitpid(wrapper, 0)
time.sleep(0.1)
""",
                    str(launch),
                    str(child_pid_path),
                    str(zombie_ready),
                    str(allow_reap),
                ]
            )
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    gate.monitor_external_conflicts(
                        measured.pid,
                        output,
                        50,
                        root.resolve(),
                        1,
                        control,
                        ready,
                        launch,
                    )
                except BaseException as error:
                    failures.append(error)

            guardian = threading.Thread(target=monitor)
            original_identity = gate.proc_identity
            child_pid: list[int | None] = [None]

            def only_measured_child(pid: int) -> tuple[str, str] | None:
                if pid == child_pid[0]:
                    return original_identity(pid)
                return None

            try:
                with mock.patch.object(
                    gate, "proc_identity", side_effect=only_measured_child
                ):
                    guardian.start()
                    gate.create_guardian_control(
                        control, ready, launch, measured.pid, os.getpid(), 50
                    )
                    gate.wait_for_guardian_ready(
                        control, ready, launch, 50, 2_000
                    )
                    gate.release_guardian_launch(control, ready, launch, 50)
                    deadline = time.monotonic() + 2.0
                    while time.monotonic() < deadline and not child_pid_path.exists():
                        time.sleep(0.005)
                    self.assertTrue(child_pid_path.exists())
                    child_pid[0] = int(child_pid_path.read_text(encoding="ascii"))
                    while time.monotonic() < deadline and not zombie_ready.exists():
                        time.sleep(0.005)
                    self.assertTrue(zombie_ready.exists())
                    child_identity = gate.read_process_stat_identity(child_pid[0])
                    self.assertIsNotNone(child_identity)
                    assert child_identity is not None
                    self.assertEqual(child_identity["state"], "Z")
                    self.assertEqual(
                        original_identity(child_pid[0]), ("chronoxide-inge", "")
                    )
                    time.sleep(0.15)
                    allow_reap.touch()
                    guardian.join(timeout=3)
                self.assertFalse(guardian.is_alive())
                self.assertEqual(failures, [])
                evidence = json.loads(output.read_text(encoding="utf-8"))
                self.assertEqual(evidence["conflicts"], [])
                self.assertEqual(evidence["terminal_poll"], evidence["polls"])
                self.assertTrue(evidence["complete_and_conflict_free"])
            finally:
                allow_reap.touch(exist_ok=True)
                if measured.poll() is None:
                    measured.kill()
                measured.wait()
                if guardian.is_alive():
                    guardian.join(timeout=1)

    def test_root_only_guardian_rejects_middle_and_terminal_cadence_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            evidence_path = root / "guardian.json"
            control_path = root / "guardian-control.json"
            ready_path = root / "guardian-ready"
            launch_path = root / "guardian-launch"
            control = {
                "schema": gate.GUARDIAN_ROOT_CONTROL_SCHEMA,
                "root_pid": 123,
                "root_starttime_ticks": 123_000,
                "guardian_pid": 456,
                "guardian_starttime_ticks": 456_000,
                "interval_ms": 100,
                "ready_marker": str(ready_path),
                "launch_marker": str(launch_path),
            }
            write_json(control_path, control)
            control_path.chmod(0o444)
            for marker in (ready_path, launch_path):
                marker.touch()
                marker.chmod(0o444)
            valid = {
                "schema": gate.GUARDIAN_SCHEMA,
                "root_pid": 123,
                "root_starttime_ticks": 123_000,
                "guardian_pid": 456,
                "interval_ms": 100,
                "polls": 3,
                "live_polls": 2,
                "terminal_poll": 3,
                "elapsed_ns": 202_000_000,
                "poll_monotonic_elapsed_ns": [
                    1_000_000,
                    101_000_000,
                    201_000_000,
                ],
                "maximum_poll_start_gap_ns": 100_000_000,
                "maximum_allowed_poll_start_gap_ns": 200_000_000,
                "control_path": str(control_path),
                "control_sha256": gate.sha256_file(control_path),
                "ready_marker_path": str(ready_path),
                "ready_marker_sha256": gate.sha256_file(ready_path),
                "ready_created_poll": 1,
                "ready_created_monotonic_elapsed_ns": 1_000_000,
                "launch_marker_path": str(launch_path),
                "launch_marker_sha256": gate.sha256_file(launch_path),
                "launch_observed_poll": 2,
                "launch_observed_monotonic_elapsed_ns": 101_000_000,
                "launch_observed": True,
                "launch_observed_root_bound": True,
                "handshake_violations": [],
                "root_seen": True,
                "filesystem": str(root.resolve()),
                "minimum_free_bytes": 1,
                "minimum_observed_free_bytes": 2,
                "capacity_violations": [],
                "conflicts": [],
                "termination": {
                    "attempted": False,
                    "root_starttime_ticks": 123_000,
                    "target_processes": [],
                    "target_pids": [],
                    "term_sent_pids": [],
                    "term_errors": [],
                    "kill_sent_pids": [],
                    "kill_errors": [],
                    "identity_refusals": [],
                    "surviving_pids": [],
                },
                "complete_and_conflict_free": True,
            }
            write_json(evidence_path, valid)
            gate.validate_guardian_evidence(
                evidence_path,
                control_path,
                ready_path,
                launch_path,
                100,
                root,
                1,
                False,
            )

            changed = copy.deepcopy(valid)
            changed["polls"] = 4
            changed["terminal_poll"] = 4
            changed["poll_monotonic_elapsed_ns"] = [
                1_000_000,
                101_000_000,
                151_000_000,
                201_000_000,
            ]
            write_json(evidence_path, changed)
            with self.assertRaisesRegex(gate.GateError, "exact and causal"):
                gate.validate_guardian_evidence(
                    evidence_path,
                    control_path,
                    ready_path,
                    launch_path,
                    100,
                    root,
                    1,
                    False,
                )

            changed = copy.deepcopy(valid)
            changed["launch_observed_poll"] = 3
            changed["launch_observed_monotonic_elapsed_ns"] = 201_000_000
            write_json(evidence_path, changed)
            with self.assertRaisesRegex(gate.GateError, "exact and causal"):
                gate.validate_guardian_evidence(
                    evidence_path,
                    control_path,
                    ready_path,
                    launch_path,
                    100,
                    root,
                    1,
                    False,
                )

            changed = copy.deepcopy(valid)
            changed["elapsed_ns"] = 202_000_000
            changed["poll_monotonic_elapsed_ns"] = [
                1_000_000,
                201_000_001,
                201_000_002,
            ]
            changed["maximum_poll_start_gap_ns"] = 200_000_001
            changed["launch_observed_monotonic_elapsed_ns"] = 201_000_001
            write_json(evidence_path, changed)
            with self.assertRaisesRegex(gate.GateError, "maximum gap exceeds"):
                gate.validate_guardian_evidence(
                    evidence_path,
                    control_path,
                    ready_path,
                    launch_path,
                    100,
                    root,
                    1,
                    False,
                )

            changed = copy.deepcopy(valid)
            changed["elapsed_ns"] = 401_000_001
            changed["maximum_poll_start_gap_ns"] = 200_000_001
            write_json(evidence_path, changed)
            with self.assertRaisesRegex(gate.GateError, "maximum gap exceeds"):
                gate.validate_guardian_evidence(
                    evidence_path,
                    control_path,
                    ready_path,
                    launch_path,
                    100,
                    root,
                    1,
                    False,
                )

    def test_root_only_guardian_retains_fast_terminal_launch_but_rejects_admission(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            output = root / "guardian.json"
            control = root / "guardian-control.json"
            ready = root / "guardian-ready"
            launch = root / "guardian-launch"
            measured = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'while [[ ! -e "$1" ]]; do sleep 0.001; done; exec true',
                    "phase5-fast-root",
                    str(launch),
                ]
            )
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    gate.monitor_external_conflicts(
                        measured.pid,
                        output,
                        100,
                        root,
                        1,
                        control,
                        ready,
                        launch,
                    )
                except BaseException as error:
                    failures.append(error)

            guardian = threading.Thread(target=monitor)
            try:
                with mock.patch.object(gate, "proc_identity", return_value=None):
                    guardian.start()
                    gate.create_guardian_control(
                        control,
                        ready,
                        launch,
                        measured.pid,
                        os.getpid(),
                        100,
                    )
                    gate.wait_for_guardian_ready(control, ready, launch, 100, 5_000)
                    gate.release_guardian_launch(control, ready, launch, 100)
                    self.assertEqual(measured.wait(timeout=3), 0)
                    guardian.join(timeout=3)
                self.assertFalse(guardian.is_alive())
                self.assertEqual(len(failures), 1)
                self.assertIsInstance(failures[0], gate.GateError)
                evidence = json.loads(output.read_text(encoding="utf-8"))
                self.assertEqual(evidence["polls"], 2)
                self.assertEqual(evidence["live_polls"], 1)
                self.assertEqual(evidence["terminal_poll"], 2)
                self.assertEqual(evidence["launch_observed_poll"], 2)
                self.assertFalse(evidence["launch_observed_root_bound"])
                self.assertIn(
                    "only after the root stopped", " ".join(evidence["handshake_violations"])
                )
                self.assertFalse(evidence["complete_and_conflict_free"])
            finally:
                if measured.poll() is None:
                    measured.kill()
                    measured.wait()
                if guardian.is_alive():
                    guardian.join(timeout=1)

    def test_guardian_forbids_android_qemu_and_gradle_workers(self) -> None:
        for comm in (
            "qemu-system-aarch64",
            "qemu-system-x86_64",
            "qemu-kvm",
            "emulator",
            "adb",
            "gradle",
        ):
            with self.subTest(comm=comm):
                self.assertIsNotNone(gate.FORBIDDEN_MEASUREMENT_COMM.fullmatch(comm))
        for command in (
            "java org.gradle.launcher.daemon.bootstrap.GradleDaemon",
            "java worker.org.gradle.process.internal.worker.GradleWorkerMain",
            "/opt/android/gradle-worker process",
        ):
            with self.subTest(command=command):
                self.assertIsNotNone(gate.FORBIDDEN_MEASUREMENT_COMMAND.search(command))

    def test_guardian_forbids_exact_interactive_monitors_only(self) -> None:
        for comm in ("btop", "htop", "top", "BTOP", "HTOP", "TOP"):
            with self.subTest(comm=comm):
                self.assertIsNotNone(gate.FORBIDDEN_MEASUREMENT_COMM.fullmatch(comm))
        for comm in (
            "desktop",
            "laptop",
            "rooftop",
            "topology-agent",
            "btop-helper",
            "htopology",
        ):
            with self.subTest(comm=comm):
                self.assertIsNone(gate.FORBIDDEN_MEASUREMENT_COMM.fullmatch(comm))

    def test_guardian_forbids_container_clients_but_allows_daemons(self) -> None:
        for comm in (
            "docker",
            "docker-buildx",
            "docker-compose",
            "buildctl",
            "nerdctl",
            "podman",
            "buildah",
        ):
            with self.subTest(comm=comm):
                self.assertIsNotNone(gate.FORBIDDEN_MEASUREMENT_COMM.fullmatch(comm))
        for comm in (
            "dockerd",
            "buildkitd",
            "rootlesskit",
            "containerd",
            "docker-proxy",
            "containerd-shim",
            "containerd-shim-runc-v1",
            "containerd-shim-runc-v2",
            "buildkitd-report",
            "rootlesskit-helper",
        ):
            with self.subTest(allowed_comm=comm):
                self.assertIsNone(gate.FORBIDDEN_MEASUREMENT_COMM.fullmatch(comm))

    def test_guardian_excludes_bound_root_zombie_but_not_reused_pid(self) -> None:
        root_pid = 123
        root_starttime_ticks = 456
        root_entry = Path(f"/proc/{root_pid}")
        zombie = {
            "pid": root_pid,
            "ppid": 1,
            "state": "Z",
            "starttime_ticks": root_starttime_ticks,
        }
        forbidden_identity = ("chronoxide-inge", "")
        with (
            mock.patch.object(Path, "iterdir", return_value=[root_entry]),
            mock.patch.object(
                gate, "read_process_stat_identity", return_value=zombie
            ),
            mock.patch.object(
                gate, "proc_identity", return_value=forbidden_identity
            ) as proc_identity,
        ):
            self.assertEqual(
                gate.scan_guardian_conflicts(
                    {}, root_pid, root_starttime_ticks, 999, 7, 123_000
                ),
                [],
            )
        proc_identity.assert_not_called()

        reused = {**zombie, "state": "S", "starttime_ticks": 457}
        with (
            mock.patch.object(Path, "iterdir", return_value=[root_entry]),
            mock.patch.object(
                gate, "read_process_stat_identity", return_value=reused
            ),
            mock.patch.object(
                gate, "proc_identity", return_value=forbidden_identity
            ),
        ):
            self.assertEqual(
                gate.scan_guardian_conflicts(
                    {}, root_pid, root_starttime_ticks, 999, 7, 123_000
                ),
                [
                    {
                        "poll": 7,
                        "monotonic_elapsed_ns": 123_000,
                        "pid": root_pid,
                        "ppid": 1,
                        "state": "S",
                        "starttime_ticks": 457,
                        "comm": "chronoxide-inge",
                        "command": "",
                    }
                ],
            )

    def test_guardian_excludes_bound_descendant_zombie_but_not_reparented_pid(
        self,
    ) -> None:
        root_pid = 10
        root_starttime_ticks = 100
        wrapper_pid = 15
        child_pid = 20
        identities = {
            root_pid: {
                "pid": root_pid,
                "ppid": 1,
                "state": "S",
                "starttime_ticks": root_starttime_ticks,
            },
            wrapper_pid: {
                "pid": wrapper_pid,
                "ppid": root_pid,
                "state": "S",
                "starttime_ticks": 150,
            },
            child_pid: {
                "pid": child_pid,
                "ppid": wrapper_pid,
                "state": "Z",
                "starttime_ticks": 200,
            },
        }

        def children(pid: int) -> list[int]:
            return {root_pid: [wrapper_pid], wrapper_pid: [child_pid]}.get(pid, [])

        with (
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ),
            mock.patch.object(gate, "read_process_children", side_effect=children),
        ):
            bindings = gate.process_tree_identity_bindings(
                root_pid, root_starttime_ticks
            )
            allowed = gate.process_tree(root_pid, root_starttime_ticks)
        self.assertEqual(set(bindings), {root_pid, wrapper_pid, child_pid})
        self.assertEqual(allowed, {root_pid, wrapper_pid})

        child_entry = Path(f"/proc/{child_pid}")
        forbidden_identity = ("chronoxide-inge", "")
        with (
            mock.patch.object(Path, "iterdir", return_value=[child_entry]),
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ),
            mock.patch.object(
                gate, "proc_identity", return_value=forbidden_identity
            ) as proc_identity,
        ):
            self.assertEqual(
                gate.scan_guardian_conflicts(
                    bindings, root_pid, root_starttime_ticks, 999, 7, 123_000
                ),
                [],
            )
        proc_identity.assert_not_called()

        original_child = identities[child_pid]
        for changed_child in (
            {**original_child, "ppid": 999},
            {**original_child, "state": "S", "starttime_ticks": 201},
        ):
            identities[child_pid] = changed_child
            with (
                mock.patch.object(Path, "iterdir", return_value=[child_entry]),
                mock.patch.object(
                    gate,
                    "read_process_stat_identity",
                    side_effect=lambda pid: identities.get(pid),
                ),
                mock.patch.object(
                    gate, "proc_identity", return_value=forbidden_identity
                ),
            ):
                conflicts = gate.scan_guardian_conflicts(
                    bindings, root_pid, root_starttime_ticks, 999, 7, 123_000
                )
            self.assertEqual(
                [conflict["pid"] for conflict in conflicts], [child_pid]
            )

        identities[child_pid] = original_child
        identities[wrapper_pid] = {**identities[wrapper_pid], "ppid": 999}
        with (
            mock.patch.object(Path, "iterdir", return_value=[child_entry]),
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ),
            mock.patch.object(
                gate, "proc_identity", return_value=forbidden_identity
            ),
        ):
            broken_ancestry = gate.scan_guardian_conflicts(
                bindings, root_pid, root_starttime_ticks, 999, 7, 123_000
            )
        self.assertEqual(
            [conflict["pid"] for conflict in broken_ancestry], [child_pid]
        )

        identities[child_pid] = {**original_child, "ppid": 999}
        with (
            mock.patch.object(Path, "iterdir", return_value=[child_entry]),
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ),
            mock.patch.object(
                gate, "proc_identity", return_value=forbidden_identity
            ),
        ):
            external = gate.scan_guardian_conflicts(
                {}, root_pid, root_starttime_ticks, 999, 7, 123_000
            )
        self.assertEqual([conflict["pid"] for conflict in external], [child_pid])

    def test_static_process_snapshot_rejects_interactive_monitor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            snapshot = Path(temporary_directory) / "processes.txt"
            snapshot.write_text(
                "101 1 10.6 0.1 1024 00:01 R btop /usr/bin/btop\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "external measurement conflict"):
                gate.validate_process_snapshot(snapshot, set())
            snapshot.write_text(
                "101 1 0.0 0.1 1024 00:01 S topology-agent "
                "/usr/bin/topology-agent\n",
                encoding="utf-8",
            )
            self.assertEqual(
                gate.validate_process_snapshot(snapshot, set()),
                {"status": "pass", "rows": 1, "conflicts": []},
            )

    def test_guardian_forbids_phase4_adversarial_build_process_variants(self) -> None:
        for comm in (
            "cargo-nextest",
            "ninja.real",
            "ninja-1.12",
            "ld.bfd",
            "ld.gold",
            "clang-19.real",
            "gcc-14",
            "soong_ui",
            "soong_ui.bash",
            "soong_build",
            "ckati",
            "kati",
            "javac",
            "kotlinc",
            "metalava",
            "aapt2",
            "aidl",
            "dex2oat",
        ):
            with self.subTest(comm=comm):
                self.assertIsNotNone(gate.FORBIDDEN_MEASUREMENT_COMM.fullmatch(comm))
        for command in (
            "/home/u/.cargo/bin/cargo-nextest nextest run",
            "/src/prebuilts/build-tools/bin/ninja.real -C out",
            "/usr/bin/ld.bfd -o out",
            "/usr/bin/ld.gold -o out",
            "/opt/llvm/bin/clang-19.real -c x.cc",
            "/bin/bash /src/build/soong/soong_ui.bash --make-mode",
            "/src/build/soong/soong_build --top /src",
        ):
            with self.subTest(command=command):
                self.assertIsNotNone(gate.FORBIDDEN_MEASUREMENT_COMMAND.search(command))

    def test_static_process_snapshot_rejects_adversarial_command_variants(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            snapshot = Path(temporary_directory) / "processes.txt"
            snapshot.write_text(
                "101 1 90.0 0.1 1024 00:01 R bash /bin/bash "
                "/src/build/soong/soong_ui.bash --make-mode\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "external measurement conflict"):
                gate.validate_process_snapshot(snapshot, set())
            snapshot.write_text(
                "101 1 0.0 0.1 1024 00:01 S sleep /usr/bin/sleep 1\n",
                encoding="utf-8",
            )
            self.assertEqual(
                gate.validate_process_snapshot(snapshot, set()),
                {"status": "pass", "rows": 1, "conflicts": []},
            )

    def test_continuous_guardian_rejects_exhausted_disk_reserve(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            control = root / "guardian-control.json"
            ready = root / "guardian-ready"
            launch = root / "guardian-launch"
            output = root / "guardian.json"
            benchmark = subprocess.Popen(["/usr/bin/sleep", "30"])
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    gate.monitor_external_conflicts(
                        benchmark.pid,
                        output,
                        10,
                        root.resolve(),
                        shutil.disk_usage(root).total + 1,
                        control,
                        ready,
                        launch,
                    )
                except BaseException as error:
                    failures.append(error)

            guardian = threading.Thread(target=monitor)
            try:
                with mock.patch.object(gate, "proc_identity", return_value=None):
                    guardian.start()
                    gate.create_guardian_control(
                        control, ready, launch, benchmark.pid, os.getpid(), 10
                    )
                    guardian.join(timeout=3)
                self.assertFalse(guardian.is_alive())
                self.assertTrue(any("reserve exhausted" in str(error) for error in failures))
                evidence = json.loads(output.read_text())
                self.assertTrue(evidence["capacity_violations"])
                self.assertFalse(evidence["complete_and_conflict_free"])
            finally:
                if benchmark.poll() is None:
                    benchmark.kill()
                benchmark.wait()
                guardian.join(timeout=1)

    def test_corpus_sync_and_writeback_quiescence_records_the_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpus = root / "corpus"
            corpus.mkdir()
            (corpus / "segment").write_bytes(b"segment")
            summary = gate.sync_and_wait_writeback_quiescent(
                corpus.resolve(),
                root / "samples.tsv",
                root / "summary.json",
                10**12,
                1,
                10,
                1,
            )
            self.assertTrue(summary["global_sync_called"])
            self.assertTrue(summary["passed"])
            self.assertEqual(summary["fsynced_file_count"], 1)

    def test_quiescence_summary_is_independently_derived_from_raw_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            samples = root / "samples.tsv"
            summary = root / "summary.json"
            samples.write_text(
                "elapsed_ns\tdirty_kib\twriteback_kib\ttotal_kib\twithin_limit\n"
                "1\t7\t3\t10\ttrue\n"
                "2\t8\t2\t10\ttrue\n",
                encoding="utf-8",
            )
            write_json(
                summary,
                {
                    "schema": "chronoxide/storage-vnext-phase5-writeback-quiescence/v1",
                    "corpus": "/tmp/corpus",
                    "fsynced_file_count": 1,
                    "global_sync_called": True,
                    "maximum_dirty_writeback_kib": 10,
                    "required_consecutive_samples": 2,
                    "interval_ms": 250,
                    "timeout_secs": 120,
                    "sample_count": 2,
                    "final_dirty_kib": 8,
                    "final_writeback_kib": 2,
                    "final_total_kib": 10,
                    "passed": True,
                },
            )
            gate.validate_quiescence_evidence(samples, summary)
            value = json.loads(summary.read_text())
            value["final_total_kib"] = 9
            write_json(summary, value)
            with self.assertRaisesRegex(gate.GateError, "not derived"):
                gate.validate_quiescence_evidence(samples, summary)

    def test_corpus_recomputation_hashes_every_nested_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            corpus = root / "segments"
            (corpus / "0001").mkdir(parents=True)
            payload = corpus / "0001/payload.bin"
            payload.write_bytes(b"payload")
            gate.phase1.write_tree_manifest(
                corpus,
                root / "segments.sha256",
                root / "segments.tsv",
                root / "corpus-summary.json",
            )
            summary = gate.recompute_corpus_artifacts(root)
            self.assertEqual(summary["file_count"], 1)
            payload.write_bytes(b"changed")
            with self.assertRaisesRegex(gate.GateError, "manifest"):
                gate.recompute_corpus_artifacts(root)

    def test_immutable_evidence_tree_is_exact_and_detects_later_addition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            parent = Path(temporary_directory).resolve()
            root = parent / "validation"
            root.mkdir()
            for name in gate.VALIDATION_EVIDENCE_FILES:
                (root / name).write_text(f"{name}\n", encoding="utf-8")
            seal = parent / "validation-seal.json"
            gate.create_immutable_tree_seal(root, seal, "validation")
            self.assertEqual(root.stat().st_mode & 0o777, 0o555)
            self.assertTrue(
                all((root / name).stat().st_mode & 0o777 == 0o444 for name in gate.VALIDATION_EVIDENCE_FILES)
            )
            gate.validate_immutable_tree_seal(root, seal, "validation")
            root.chmod(0o755)
            (root / "unexpected.txt").write_text("surprise\n", encoding="utf-8")
            root.chmod(0o555)
            with self.assertRaisesRegex(gate.GateError, "matrix differs"):
                gate.validate_immutable_tree_seal(root, seal, "validation")

    def test_fail_closed_tree_inventory_rejects_symlinks_and_fifos(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            parent = Path(temporary_directory).resolve()
            linked = parent / "linked"
            linked.mkdir()
            (linked / "target").write_text("target", encoding="utf-8")
            (linked / "link").symlink_to(linked / "target")
            with self.assertRaisesRegex(gate.GateError, "symlink"):
                gate.fail_closed_tree_inventory(linked)
            fifo = parent / "fifo"
            fifo.mkdir()
            os.mkfifo(fifo / "pipe")
            with self.assertRaisesRegex(gate.GateError, "non-regular"):
                gate.fail_closed_tree_inventory(fifo)

            opaque = parent / "opaque"
            opaque.mkdir()
            build_target = opaque / "build-target"
            build_target.mkdir()
            (build_target / "librdkafka.so.1").write_bytes(b"native library")
            (build_target / "librdkafka.so").symlink_to("librdkafka.so.1")
            directories, files = gate.fail_closed_tree_inventory(
                opaque, excluded_subtrees={"build-target"}
            )
            self.assertEqual(directories, ["build-target"])
            self.assertEqual(files, [])

            opaque_link = parent / "opaque-link"
            opaque_link.mkdir()
            (opaque_link / "build-target").symlink_to(
                build_target, target_is_directory=True
            )
            with self.assertRaisesRegex(gate.GateError, "symlink"):
                gate.fail_closed_tree_inventory(
                    opaque_link, excluded_subtrees={"build-target"}
                )

            with self.assertRaisesRegex(gate.GateError, "unsafe excluded"):
                gate.fail_closed_tree_inventory(
                    opaque, excluded_subtrees={"../build-target"}
                )

    def test_final_matrix_rejects_unexpected_config_before_it_can_be_sealed(self) -> None:
        directories = sorted(
            {
                "build-source",
                "build-target",
                "calibration",
                "comparisons",
                "configs",
                "metadata",
                "runs",
                "validation",
            }
            | {
                f"runs/run-{index:02d}-{policy}"
                for index, policy in enumerate(gate.EXPECTED_SCHEDULE, start=1)
            },
            key=os.fsencode,
        )
        files = [
            "PARTIAL_UNLESS_COMPLETE.txt",
            "run-plan.tsv",
            "configs/surprise.toml",
        ]
        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaisesRegex(gate.GateError, "config"):
                gate.validate_final_artifact_matrix(
                    Path(temporary_directory).resolve(), directories, files
                )

    def test_profile_final_matrix_requires_capacity_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            (root / "metadata/raw-authorities").mkdir(parents=True)
            capacity = root / "metadata/profile-capacity-control.json"
            gate.create_profile_capacity_control(
                capacity, 16 * 1024 * 1024 * 1024
            )
            directories = [
                "configs",
                "heaptrack",
                "metadata",
                "metadata/raw-authorities",
            ]
            files = [
                "PROFILE_SCOPE.txt",
                "configs/heaptrack.toml",
                "metadata/capture-inputs-after.json",
                "metadata/capture-inputs-before.json",
                "metadata/final-raw-revalidation.json",
                "metadata/heaptrack-S-controls.json",
                "metadata/profile-capacity-control.json",
                "metadata/python-interpreter.txt",
                "metadata/run-note.txt",
                "metadata/raw-authorities/heaptrack.json",
            ]
            with mock.patch.object(gate, "validate_immutable_tree_seal"):
                gate.validate_profile_artifact_matrix(root, directories, files)
                without_capacity = [
                    path
                    for path in files
                    if path != "metadata/profile-capacity-control.json"
                ]
                with self.assertRaisesRegex(gate.GateError, "metadata root-file"):
                    gate.validate_profile_artifact_matrix(
                        root, directories, without_capacity
                    )

    def test_final_inventory_revalidates_after_versioned_complete_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory).resolve()
            metadata = root / "metadata"
            metadata.mkdir()
            build_target = root / "build-target"
            build_target.mkdir()
            (build_target / "librdkafka.so.1").write_bytes(b"native library")
            (build_target / "librdkafka.so").symlink_to("librdkafka.so.1")
            (root / "evidence.txt").write_text("evidence\n", encoding="utf-8")
            with mock.patch.object(gate, "validate_final_artifact_matrix"):
                gate.create_final_artifact_inventory(
                    root,
                    metadata / "result-artifacts.nul",
                    metadata / "result-directories.nul",
                    metadata / "result-artifacts.sha256",
                )
                precomplete = gate.validate_final_artifact_inventory(
                    root, "precomplete"
                )
                write_json(metadata / "FINAL_SEAL_VALIDATED.json", precomplete)
                (metadata / "FINAL_SEAL_VALIDATED.json").chmod(0o444)
                (root / "COMPLETE").write_text(
                    "chronoxide/allocator-screen-complete/v1\n", encoding="utf-8"
                )
                (root / "COMPLETE").chmod(0o444)
                complete = gate.validate_final_artifact_inventory(root, "complete")
                self.assertEqual(complete["status"], "pass")
                listed_files = gate.parse_nul_inventory(
                    metadata / "result-artifacts.nul", "test final file inventory"
                )
                listed_directories = gate.parse_nul_inventory(
                    metadata / "result-directories.nul",
                    "test final directory inventory",
                )
                self.assertNotIn("build-target/librdkafka.so", listed_files)
                self.assertNotIn("build-target/librdkafka.so.1", listed_files)
                self.assertIn("build-target", listed_directories)
                self.assertFalse(
                    any(path.startswith("build-target/") for path in listed_directories)
                )
                (root / "COMPLETE").chmod(0o644)
                (root / "unexpected.txt").write_text("unexpected\n", encoding="utf-8")
                (root / "COMPLETE").chmod(0o444)
                with self.assertRaisesRegex(gate.GateError, "exactly match"):
                    gate.validate_final_artifact_inventory(root, "complete")

    def test_external_rss_monitor_labels_a_live_process(self) -> None:
        self.assertEqual(
            gate.parse_process_cpu_ticks(
                "123 (worker ) with spaces) S 1 2 3 4 5 6 7 8 9 10 111 222"
            ),
            333,
        )
        self.assertIsNone(
            gate.parse_process_cpu_ticks(
                "123 (worker) Z 1 2 3 4 5 6 7 8 9 10 111 222"
            )
        )
        self.assertIsNone(
            gate.parse_process_cpu_ticks(
                "123 (worker) x 1 2 3 4 5 6 7 8 9 10 111 222"
            )
        )
        target = {"pid": 123, "ppid": 1, "state": "S", "starttime_ticks": 456}
        for dead_state in ("Z", "X", "x"):
            dead = {**target, "state": dead_state}
            with mock.patch.object(
                gate, "read_process_stat_identity", return_value=dead
            ):
                self.assertFalse(gate.process_is_same_running(123, 456))
                self.assertEqual(
                    gate.process_identity_refusal(target), f"state_{dead_state}"
                )
        reused = {**target, "starttime_ticks": 457}
        with mock.patch.object(
            gate, "read_process_stat_identity", return_value=reused
        ):
            self.assertFalse(gate.process_is_same_running(123, 456))
            self.assertEqual(
                gate.process_identity_refusal(target), "starttime_mismatch"
            )
        reparented = {**target, "ppid": 999}
        with mock.patch.object(
            gate, "read_process_stat_identity", return_value=reparented
        ):
            self.assertIsNone(gate.process_identity_refusal(target))
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            control = (root / "external-conflict-guardian-control.json").resolve()
            guardian_ready = (root / "external-conflict-guardian-ready").resolve()
            rss_ready = (root / "rss-monitor-ready").resolve()
            launch = (root / "external-conflict-guardian-launch").resolve()
            process = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'while [[ ! -e "$1" ]]; do sleep 0.001; done; sleep 0.25',
                    "bash",
                    str(launch),
                ]
            )
            guardian = subprocess.Popen(["sleep", "30"])
            summaries: list[dict[str, object]] = []
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    summaries.append(
                        gate.monitor_rss_release(
                            process.pid,
                            root / "checkpoint.tsv",
                            root / "rss-samples.tsv",
                            root / "rss.json",
                            100,
                            control,
                            rss_ready,
                            launch,
                        )
                    )
                except BaseException as error:
                    failures.append(error)

            monitor_thread = threading.Thread(target=monitor)
            try:
                monitor_thread.start()
                gate.create_guardian_control(
                    control,
                    guardian_ready,
                    launch,
                    process.pid,
                    guardian.pid,
                    100,
                    os.getpid(),
                    rss_ready,
                )
                deadline = time.monotonic() + 2
                while not rss_ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.005)
                self.assertTrue(rss_ready.exists())
                gate.create_empty_read_only_marker(launch, "guardian launch marker")
                process.wait(timeout=3)
                monitor_thread.join(timeout=3)
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()
                if guardian.poll() is None:
                    guardian.kill()
                guardian.wait()
                monitor_thread.join(timeout=1)
            self.assertFalse(monitor_thread.is_alive())
            self.assertEqual(failures, [])
            summary = summaries[0]
            self.assertGreater(summary["workload_samples"], 0)
            self.assertEqual(summary["post_drop_samples"], 0)
            self.assertTrue(summary["complete"])
            self.assertEqual(summary["rss_ready_created_sample"], 1)
            self.assertGreater(summary["launch_observed_sample"], 1)
            gate.validate_rss_release_evidence(
                root / "rss-samples.tsv",
                root / "rss.json",
                control,
                rss_ready,
                launch,
                100,
            )

    def test_external_rss_monitor_retains_fast_terminal_launch_but_rejects_admission(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            control = (root / "external-conflict-guardian-control.json").resolve()
            guardian_ready = (root / "external-conflict-guardian-ready").resolve()
            rss_ready = (root / "rss-monitor-ready").resolve()
            launch = (root / "external-conflict-guardian-launch").resolve()
            process = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'while [[ ! -e "$1" ]]; do sleep 0.001; done; exec true',
                    "phase5-fast-rss-root",
                    str(launch),
                ]
            )
            guardian = subprocess.Popen(["sleep", "30"])
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    gate.monitor_rss_release(
                        process.pid,
                        root / "checkpoint.tsv",
                        root / "rss-samples.tsv",
                        root / "rss.json",
                        100,
                        control,
                        rss_ready,
                        launch,
                    )
                except BaseException as error:
                    failures.append(error)

            monitor_thread = threading.Thread(target=monitor)
            try:
                monitor_thread.start()
                gate.create_guardian_control(
                    control,
                    guardian_ready,
                    launch,
                    process.pid,
                    guardian.pid,
                    100,
                    os.getpid(),
                    rss_ready,
                )
                deadline = time.monotonic() + 2
                while not rss_ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.005)
                self.assertTrue(rss_ready.exists())
                gate.create_empty_read_only_marker(launch, "guardian launch marker")
                self.assertEqual(process.wait(timeout=3), 0)
                monitor_thread.join(timeout=3)
                self.assertFalse(monitor_thread.is_alive())
                self.assertEqual(len(failures), 1)
                self.assertRegex(str(failures[0]), "fewer than two live process samples")
                rows = gate.load_rss_samples(root / "rss-samples.tsv")
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[-1]["phase"], "terminal")
                self.assertEqual(rows[-1]["process_count"], 0)
                summary = json.loads((root / "rss.json").read_text(encoding="utf-8"))
                self.assertEqual(summary["samples"], 1)
                self.assertTrue(summary["terminal_observation"])
                self.assertTrue(summary["terminal_launch_observed"])
                self.assertFalse(summary["launch_observed"])
                self.assertFalse(summary["complete"])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                if guardian.poll() is None:
                    guardian.kill()
                    guardian.wait()
                if monitor_thread.is_alive():
                    monitor_thread.join(timeout=1)

    def test_cleanup_tree_snapshot_is_parent_bound_and_depth_first(self) -> None:
        identities = {
            10: {"pid": 10, "ppid": 1, "state": "S", "starttime_ticks": 100},
            20: {"pid": 20, "ppid": 10, "state": "S", "starttime_ticks": 200},
            99: {"pid": 99, "ppid": 20, "state": "S", "starttime_ticks": 300},
        }
        proc_entries = [Path(f"/proc/{pid}") for pid in identities]

        def snapshot() -> list[dict[str, int | str]]:
            with (
                mock.patch.object(Path, "iterdir", return_value=proc_entries),
                mock.patch.object(
                    gate,
                    "read_process_stat_identity",
                    side_effect=lambda pid: identities.get(pid),
                ),
            ):
                return gate.snapshot_process_tree_identities(10, 100)

        self.assertEqual(
            [target["pid"] for target in snapshot()],
            [99, 20, 10],
        )
        identities[20] = {**identities[20], "ppid": 999}
        self.assertEqual(
            [target["pid"] for target in snapshot()],
            [10],
        )
        identities[10] = {**identities[10], "starttime_ticks": 101}
        self.assertEqual(snapshot(), [])

    def test_runner_exit_cleanup_preserves_status_pre_and_post_control(self) -> None:
        source = (HERE / "phase5_allocator_screen_run.sh").read_text(
            encoding="utf-8"
        )
        lifecycle_start = source.index("active_lifecycle=0")
        trap_line = 'trap \'cleanup_on_exit "$?"\' EXIT'
        lifecycle_end = source.index(trap_line, lifecycle_start) + len(trap_line)
        lifecycle_source = source[lifecycle_start:lifecycle_end]
        handler_start = lifecycle_source.index("cleanup_on_exit() {")
        handler_end = lifecycle_source.index("\n}", handler_start) + 2
        handler = lifecycle_source[handler_start:handler_end]
        self.assertIn('local exit_status="$1"', handler)
        self.assertLess(
            handler.index("trap - EXIT"), handler.index("stop_active_children")
        )
        self.assertIn('if [[ "$active_lifecycle" == 1 ]]', handler)
        self.assertNotIn("$(", handler)
        self.assertEqual(source.count("active_lifecycle=1"), 2)
        calibration_active = source.index("active_lifecycle=1")
        measured_active = source.index("active_lifecycle=1", calibration_active + 1)
        self.assertLess(lifecycle_end, calibration_active)
        self.assertLess(
            calibration_active,
            source.index("defer_cleanup_signals", calibration_active),
        )
        self.assertLess(
            measured_active,
            source.index("defer_cleanup_signals", measured_active),
        )

        shell = (
            "set -euo pipefail\n"
            "TRACE=$1\nRUN_DIR=$2\nCONTROL=$3\nMODE=$4\n"
            "FROZEN_GATE=/nonexistent\nGATE=/nonexistent\n"
            "note() { :; }\n"
            + lifecycle_source
            + r'''
python3() { exit 88; }
cleanup_python3() {
    printf 'cleanup:%s\n' "$*" >>"$TRACE"
    [[ "$MODE" != cleanup-fails ]] || return 71
    return 0
}
bounded_reap_job() {
    printf 'reap:%s:%s:%s\n' "$1" "$2" "$3" >>"$TRACE"
    return 42
}
die() { exit 23; }
active_run_dir="$RUN_DIR"
active_guardian_control="$CONTROL"
active_guardian_ready="$RUN_DIR/ready"
active_guardian_launch="$RUN_DIR/launch"
active_root_pid=101
active_root_starttime_ticks=1001
active_rss_pid=202
active_rss_starttime_ticks=2002
active_guardian_pid=303
active_guardian_starttime_ticks=3003
active_lifecycle=1
case "$MODE" in
    die) die ;;
    errexit) false ;;
    cleanup-fails) false ;;
    signal) cleanup_signal_exit ;;
    success) clear_active_processes ;;
esac
'''
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            cases = (
                ("pre-control-die", "die", False, 23),
                ("pre-control-signal", "signal", False, 130),
                ("post-control-errexit", "errexit", True, 1),
                ("post-control-cleanup-fails", "cleanup-fails", True, 1),
                ("success-cleared", "success", True, 0),
            )
            for label, mode, control_exists, expected_status in cases:
                with self.subTest(label=label):
                    run_dir = root / label
                    run_dir.mkdir()
                    trace = run_dir / "trace.tsv"
                    control = run_dir / "control.json"
                    if control_exists:
                        control.write_text("sealed\n", encoding="utf-8")
                    completed = subprocess.run(
                        [
                            "bash",
                            "-c",
                            shell,
                            "bash",
                            str(trace),
                            str(run_dir),
                            str(control),
                            mode,
                        ],
                        check=False,
                        capture_output=True,
                        text=True,
                    )
                    self.assertEqual(completed.returncode, expected_status)
                    lines = (
                        trace.read_text(encoding="utf-8").splitlines()
                        if trace.exists()
                        else []
                    )
                    if mode == "success":
                        self.assertEqual(lines, [])
                        continue
                    self.assertEqual(
                        [line for line in lines if line.startswith("reap:")],
                        [
                            "reap:root:101:1001",
                            "reap:rss-monitor:202:2002",
                            "reap:guardian:303:3003",
                        ],
                    )
                    if mode == "errexit":
                        cleanup_lines = [
                            line for line in lines if line.startswith("cleanup:")
                        ]
                        self.assertEqual(len(cleanup_lines), 1)
                        self.assertIn(
                            "cleanup-guardian-processes", cleanup_lines[0]
                        )
                        self.assertFalse(
                            any(line.startswith("stop:") for line in lines)
                        )
                    else:
                        expected_pids = ["101", "202", "303"]
                        self.assertEqual(
                            [
                                line.split(" --root-pid ", 1)[1].split(" ", 1)[0]
                                for line in lines
                                if "terminate-process-tree" in line
                            ],
                            expected_pids,
                        )
                        if mode == "cleanup-fails":
                            self.assertIn(
                                "cleanup-guardian-processes",
                                next(
                                    line
                                    for line in lines
                                    if line.startswith("cleanup:")
                                ),
                            )

    def test_runner_only_seals_after_screen_and_canonical_validation(self) -> None:
        source = (HERE / "phase5_allocator_screen_run.sh").read_text(encoding="utf-8")
        python_wrapper = source.index("python3() {")
        first_python_consumer = source.index('python3 "$GATE"')
        calibration = source.index('create-calibration')
        measured_schedule = source.index('observation_args=()', calibration)
        compare = source.index('compare-screen "${observation_args[@]}"')
        validation = source.index("gate-validation", compare)
        seal = source.index('seal-screen "${observation_args[@]}"', validation)
        complete = source.index(
            "'chronoxide/allocator-screen-complete/v1'", seal
        )
        self.assertLess(python_wrapper, first_python_consumer)
        self.assertLess(calibration, measured_schedule)
        self.assertLess(measured_schedule, compare)
        self.assertLess(compare, validation)
        self.assertLess(validation, seal)
        self.assertLess(seal, complete)
        self.assertIn("assert_policy_binary_unchanged", source)
        self.assertIn("monitor-rss-release", source)
        self.assertIn("parse-allocator-telemetry", source)
        self.assertIn("CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_TELEMETRY", source)
        self.assertIn("ambient LD_PRELOAD is forbidden", source)
        self.assertIn("ambient MALLOC_CONF is forbidden", source)
        self.assertIn("PARTIAL_UNLESS_COMPLETE", source)
        self.assertIn("--calibration-storage", source)
        self.assertIn("--storage \"$VALIDATION_DIR/storage-verify.json\"", source)
        self.assertIn("source-seal --repo", source)
        self.assertIn("check-source-seal", source)
        self.assertIn("extract-git-archive", source)
        self.assertIn("check-extracted-source-seal", source)
        self.assertIn("check-executable-set", source)
        self.assertIn("chmod -R a-w", source)
        self.assertIn("chmod 0555 -- \"$destination\"", source)
        self.assertIn("CORE_CONTROL_SEAL", source)
        self.assertIn("MEASUREMENT_CONTROL_SEAL", source)
        self.assertIn("check-rendered-config", source)
        self.assertIn("$FADVISE_BINARY", source)
        self.assertIn('"$PYTHON_BIN" -I -S -B', source)
        self.assertIn("PYTHON_FLAGS_PROBE", source)
        self.assertIn('str(int(value)) for value in', source)
        self.assertIn("PYTHON_RECORD", source)
        self.assertIn('source=open(script,"rb").read()', source)
        self.assertIn("python3_background() {", source)
        self.assertIn('exec "${command[@]}"', source)
        self.assertIn("verify_background_python_pid_binding", source)
        self.assertEqual(
            source.count('python3_background "$FROZEN_GATE" monitor'), 3
        )
        self.assertNotIn('python3 "$FROZEN_GATE" monitor', source)
        self.assertNotIn("sys.path.insert", source)
        self.assertNotIn("PYTHONPATH=", source)
        self.assertNotIn("command python3", source)
        self.assertEqual(source.count('cd "$BUILD_SOURCE"'), 2)
        self.assertEqual(source.count('"$CARGO_BIN" build'), 2)
        self.assertIn("--manifest-path Cargo.toml", source)
        self.assertIn("create-final-artifact-inventory", source)
        self.assertIn("revalidate-screen-from-raw", source)
        self.assertIn("--features jemalloc-stats", source)
        self.assertIn("NO_STATS_REVALIDATION_COMMAND", source)
        self.assertGreaterEqual(source.count("assert_experiment_seals"), 20)
        self.assertEqual(len(gate.EXPECTED_SCHEDULE), 10)
        self.assertEqual(source.count("release-guardian-launch"), 2)
        calibration_release = source.index("release-guardian-launch")
        measured_loop = source.index(
            'for schedule_row in "${schedule_rows[@]}"; do', calibration_release
        )
        measured_rss = source.index("monitor-rss-release", measured_loop)
        measured_control = source.index("create-guardian-control", measured_rss)
        measured_ready = source.index("wait-guardian-ready", measured_control)
        measured_release = source.index("release-guardian-launch", measured_ready)
        self.assertLess(calibration_release, measured_loop)
        self.assertLess(measured_loop, measured_rss)
        self.assertLess(measured_rss, measured_control)
        self.assertLess(measured_control, measured_ready)
        self.assertLess(measured_ready, measured_release)
        self.assertEqual(source.count('rss_ready="$run_dir/rss-monitor-ready"'), 1)
        self.assertIn('--rss-ready "$rss_ready"', source)
        self.assertIn(
            "expected_phase1_bytes * 10 / 4 + capacity_reserve_bytes", source
        )
        self.assertIn(
            '--minimum-free-bytes "$calibration_guardian_free_bytes"', source
        )
        self.assertIn(
            '"$state" != Z && "$state" != X && "$state" != x', source
        )
        self.assertIn(
            '"$state" == Z || "$state" == X || "$state" == x', source
        )
        self.assertIn(
            'identity="$(read_process_state_starttime_ticks "$1")" || return 1',
            source,
        )
        self.assertNotIn(
            '<<<"$(read_process_state_starttime_ticks', source
        )
        self.assertGreaterEqual(source.count("stat -c '%a' --"), 2)

    def test_raw_revalidation_rebuilds_observations_from_raw_checkpoints(self) -> None:
        source = (HERE / "phase5_allocator_screen_gate.py").read_text(
            encoding="utf-8"
        )
        module = ast.parse(source)
        revalidation = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "revalidate_screen_from_raw"
        )
        calls = [
            node
            for node in ast.walk(revalidation)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "make_observation"
        ]
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].args, [])
        keywords = {keyword.arg: keyword.value for keyword in calls[0].keywords}
        checkpoint = keywords["checkpoint_path"]
        self.assertIsInstance(checkpoint, ast.BinOp)
        assert isinstance(checkpoint, ast.BinOp)
        self.assertIsInstance(checkpoint.op, ast.Div)
        self.assertIsInstance(checkpoint.left, ast.Name)
        assert isinstance(checkpoint.left, ast.Name)
        self.assertEqual(checkpoint.left.id, "run")
        self.assertIsInstance(checkpoint.right, ast.Constant)
        assert isinstance(checkpoint.right, ast.Constant)
        self.assertEqual(
            checkpoint.right.value, "allocator-release-checkpoint.tsv"
        )

        observation_definition = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef) and node.name == "make_observation"
        )
        self.assertEqual(observation_definition.args.args, [])
        all_calls = [
            node
            for node in ast.walk(module)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "make_observation"
        ]
        self.assertEqual(len(all_calls), 2)
        self.assertTrue(all(not call.args for call in all_calls))

    def test_profile_runner_exit_cleanup_survives_wrapper_failure_and_preserves_status(
        self,
    ) -> None:
        source = (HERE / "phase5_allocator_profile_run.sh").read_text(
            encoding="utf-8"
        )
        lifecycle_start = source.index("active_lifecycle=0")
        trap_line = 'trap \'cleanup_on_exit "$?"\' EXIT'
        lifecycle_end = source.index(trap_line, lifecycle_start) + len(trap_line)
        lifecycle_source = source[lifecycle_start:lifecycle_end]
        handler_start = lifecycle_source.index("cleanup_on_exit() {")
        handler_end = lifecycle_source.index("\n}", handler_start) + 2
        handler = lifecycle_source[handler_start:handler_end]
        cleanup_start = lifecycle_source.index("cleanup_children() {")
        cleanup_end = lifecycle_source.index("\n}", cleanup_start) + 2
        cleanup = lifecycle_source[cleanup_start:cleanup_end]
        self.assertIn('local exit_status="$1"', handler)
        self.assertLess(handler.index("trap - EXIT"), handler.index("cleanup_children"))
        self.assertIn('if [[ "$active_lifecycle" == 1 ]]', handler)
        self.assertNotIn("$(", handler)
        self.assertIn('cleanup_python3 "$GATE" cleanup-guardian-processes', cleanup)
        self.assertNotIn('if python3 "$GATE"', cleanup)
        self.assertIn('cleanup_python3 "$GATE" terminate-process-tree', lifecycle_source)
        self.assertEqual(source.count("active_lifecycle=1"), 1)
        active = source.index("active_lifecycle=1")
        self.assertLess(lifecycle_end, active)
        self.assertLess(active, source.index("defer_cleanup_signals", active))

        shell = (
            "set -euo pipefail\n"
            "TRACE=$1\nRUN_DIR=$2\nCONTROL=$3\nMODE=$4\n"
            "GATE=/nonexistent\nPYTHON_BIN=/nonexistent\n"
            "PYTHON_SCRIPT_BOOTSTRAP=none\n"
            + lifecycle_source
            + r'''
python3() {
    printf 'wrapper-precheck:%s\n' "$MODE" >>"$TRACE"
    [[ "$MODE" != wrapper-pre ]] || exit 88
    printf 'wrapper-command:%s\n' "$MODE" >>"$TRACE"
    [[ "$MODE" != wrapper-post ]] || exit 89
    return 0
}
cleanup_python3() {
    printf 'cleanup:%s\n' "$*" >>"$TRACE"
    if [[ "$MODE" == cleanup-fails && "$*" == *cleanup-guardian-processes* ]]; then
        return 71
    fi
    return 0
}
bounded_reap_job() {
    printf 'reap:%s:%s:%s\n' "$1" "$2" "$3" >>"$TRACE"
    return 42
}
active_run_dir="$RUN_DIR"
active_guardian_control="$CONTROL"
active_guardian_ready="$RUN_DIR/ready"
active_guardian_launch="$RUN_DIR/launch"
active_root_pid=101
active_root_starttime_ticks=1001
active_guardian_pid=303
active_guardian_starttime_ticks=3003
active_lifecycle=1
case "$MODE" in
    wrapper-pre|wrapper-post) python3 "$GATE" noop ;;
    cleanup-fails) false ;;
    signal) cleanup_signal_exit ;;
    ordinary) exit 37 ;;
    success) clear_active_processes ;;
esac
'''
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            cases = (
                ("wrapper-pre", False, 88),
                ("wrapper-post", True, 89),
                ("cleanup-fails", True, 1),
                ("signal", True, 130),
                ("ordinary", False, 37),
                ("success", True, 0),
            )
            for mode, control_exists, expected_status in cases:
                with self.subTest(mode=mode):
                    run_dir = root / mode
                    run_dir.mkdir()
                    trace = run_dir / "trace.tsv"
                    control = run_dir / "control.json"
                    if control_exists:
                        control.write_text("sealed\n", encoding="utf-8")
                    completed = subprocess.run(
                        [
                            "bash",
                            "-c",
                            shell,
                            "bash",
                            str(trace),
                            str(run_dir),
                            str(control),
                            mode,
                        ],
                        check=False,
                        capture_output=True,
                        text=True,
                    )
                    self.assertEqual(
                        completed.returncode,
                        expected_status,
                        msg=f"stdout={completed.stdout!r} stderr={completed.stderr!r}",
                    )
                    lines = (
                        trace.read_text(encoding="utf-8").splitlines()
                        if trace.exists()
                        else []
                    )
                    if mode == "success":
                        self.assertEqual(lines, [])
                        continue
                    self.assertEqual(
                        [line for line in lines if line.startswith("reap:")],
                        ["reap:root:101:1001", "reap:guardian:303:3003"],
                    )
                    cleanup_lines = [
                        line for line in lines if line.startswith("cleanup:")
                    ]
                    if control_exists and mode != "cleanup-fails":
                        self.assertEqual(len(cleanup_lines), 1)
                        self.assertIn("cleanup-guardian-processes", cleanup_lines[0])
                    else:
                        terminated = [
                            line.split(" --root-pid ", 1)[1].split(" ", 1)[0]
                            for line in cleanup_lines
                            if "terminate-process-tree" in line
                        ]
                        self.assertEqual(terminated, ["101", "303"])
                        if mode == "cleanup-fails":
                            self.assertIn(
                                "cleanup-guardian-processes", cleanup_lines[0]
                            )

    def test_profile_runner_releases_heaptrack_and_optional_perf_from_one_held_path(
        self,
    ) -> None:
        source = (HERE / "phase5_allocator_profile_run.sh").read_text(
            encoding="utf-8"
        )
        function_start = source.index("run_profile_replay() {")
        function_end = source.index("\n}\n\nHEAPTRACK_DIR=", function_start)
        function = source[function_start:function_end]
        held = function.index(
            'while [[ ! -e "$guardian_launch" && ! -L "$guardian_launch" ]]'
        )
        guardian = function.index("monitor-external-conflicts", held)
        control = function.index("create-guardian-control", guardian)
        ready = function.index("wait-guardian-ready", control)
        release = function.index("release-guardian-launch", ready)
        self.assertLess(held, guardian)
        self.assertLess(guardian, control)
        self.assertLess(control, ready)
        self.assertLess(ready, release)
        self.assertEqual(function.count("release-guardian-launch"), 1)
        self.assertIn("run_profile_replay heaptrack S", source[function_end:])
        self.assertIn("run_profile_replay perf-record", source[function_end:])
        self.assertIn("if (( ENABLE_PERF_RECORD == 1 )); then", source[function_end:])
        self.assertNotIn("rss-monitor-ready", source)
        self.assertNotIn("--rss-monitor-pid", source)
        self.assertIn(
            '"$state" != Z && "$state" != X && "$state" != x', source
        )
        self.assertIn(
            '"$state" == Z || "$state" == X || "$state" == x', source
        )
        self.assertIn(
            'identity="$(read_process_state_starttime_ticks "$1")" || return 1',
            source,
        )
        self.assertNotIn(
            '<<<"$(read_process_state_starttime_ticks', source
        )
        self.assertIn("stat -c '%a' --", function)
        self.assertIn("python3_background() {", source)
        self.assertIn('exec "${command[@]}"', source)
        self.assertIn("verify_background_python_pid_binding", source)
        self.assertIn(
            'python3_background "$GATE" monitor-external-conflicts', function
        )
        self.assertNotIn('python3 "$GATE" monitor-external-conflicts', function)

    def test_runner_rejects_hardened_build_profiler_and_database_conflicts(self) -> None:
        source = (HERE / "phase5_allocator_screen_run.sh").read_text(encoding="utf-8")
        self.assertIn("check-process-snapshot", source)
        self.assertIn("monitor-external-conflicts", source)
        self.assertIn("--minimum-free-bytes", source)
        for checkpoint in (
            'check_measurement_conflicts "$run_dir/processes-before.txt"',
            'check_measurement_conflicts "$run_dir/processes-after.txt"',
            'check_measurement_conflicts "$VALIDATION_DIR/processes-before-storage-verify.txt"',
            'check_measurement_conflicts "$VALIDATION_DIR/processes-before-readbacks.txt"',
            'check_measurement_conflicts "$VALIDATION_DIR/processes-after.txt"',
        ):
            self.assertIn(checkpoint, source)

    def test_untimed_profile_harness_uses_system_heaptrack_and_lost_event_gate(self) -> None:
        source = (HERE / "phase5_allocator_profile_run.sh").read_text(
            encoding="utf-8"
        )
        self.assertLess(source.index("python3() {"), source.index('python3 "$GATE"'))
        self.assertIn('"$PYTHON_BIN" -I -S -B', source)
        self.assertIn("PYTHON_FLAGS_PROBE", source)
        self.assertIn('str(int(value)) for value in', source)
        self.assertIn("PYTHON_RECORD", source)
        self.assertIn('source=open(script,"rb").read()', source)
        self.assertNotIn("sys.path.insert", source)
        self.assertNotIn("PYTHONPATH=", source)
        self.assertNotIn("command python3", source)
        self.assertIn("heaptrack --record-only -o", source)
        self.assertIn("run_profile_replay heaptrack S", source)
        self.assertIn("record-profile-evidence", source)
        self.assertIn("SCREEN_ARTIFACT_MANIFEST", source)
        self.assertIn("validate-final-artifacts", source)
        self.assertIn("QUIET_HOST_CONFIRMED", source)
        self.assertIn("PROFILE_MIN_FREE_BYTES", source)
        self.assertIn(
            'PROFILE_MIN_FREE_BYTES="${PROFILE_MIN_FREE_BYTES:-17179869184}"', source
        )
        self.assertIn("create-profile-capacity-control", source)
        self.assertIn("check-profile-capacity-control", source)
        self.assertIn('--input "$PROFILE_CAPACITY_CONTROL"', source)
        self.assertIn("assert_profile_capacity_control", source)
        self.assertIn("monitor-external-conflicts", source)
        self.assertIn("seal-evidence-tree", source)
        self.assertIn("revalidate-profile-from-raw", source)
        self.assertIn("create-profile-artifact-inventory", source)
        self.assertIn("validate-profile-artifacts", source)
        self.assertIn("chronoxide/allocator-profile-complete/v1", source)
        self.assertIn("check-executable-set", source)
        self.assertIn("assert_screen_seal", source)
        seal_start = source.index("assert_screen_seal() {")
        seal_end = source.index(
            "assert_jemalloc_host_sources_absent\nassert_screen_seal initial-screen-input",
            seal_start,
        )
        seal_body = source[seal_start:seal_end]
        self.assertIn("check-source-seal", seal_body)
        self.assertIn("check-extracted-source-seal", seal_body)
        self.assertIn('--repo "$SEALED_REPO_ROOT"', seal_body)
        self.assertIn('--archive "$SOURCE_ARCHIVE"', seal_body)
        self.assertIn('--source-root "$BUILD_SOURCE"', seal_body)
        self.assertIn('--build-provenance "$BUILD_PROVENANCE"', seal_body)
        self.assertLess(
            source.index("assert_screen_seal initial-screen-input"),
            source.index('mkdir "$RESULT_DIR"'),
        )
        self.assertIn("FROZEN_PROFILE_RUNNER", source)
        self.assertIn("assert_profile_control_seal", source)
        self.assertIn("run_selected_policy_preflight", source)
        self.assertIn("gate-profile-runtime-log", source)
        self.assertIn("$REFERENCE_DIR/segments.sha256", source)
        self.assertIn("--selected-runtime-log", source)
        self.assertIn("--selected-preflight", source)
        self.assertIn("CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_RUNTIME_POLICY=1", source)
        self.assertIn('"$analysis" "$profile_dir/perf-script.log" "$profiler_log"', source)
        self.assertIn("events?|chunks?", source)
        self.assertIn("dropped[^[:cntrl:]]*samples?", source)
        self.assertIn("lost-events.txt", source)
        self.assertIn("Candidate-specific linked-", source)
        self.assertIn("ENABLE_PERF_RECORD", source)
        self.assertNotIn("perf stat", source)
        self.assertNotIn("monitor-rss-release", source)
        self.assertNotIn("/usr/bin/time", source)


if __name__ == "__main__":
    unittest.main()
