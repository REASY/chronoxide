#!/usr/bin/env python3

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import live_query_ingest_ab_gate as gate


def gate_publication_scale(
    root: Path, expected_messages: int, expected: dict
) -> dict:
    return gate.gate_publication_scale(
        root,
        expected_messages,
        expected,
        _test_only_allow_unisolated_validator=True,
    )


STATS = {field: 0 for field in gate.QUERY_STATS_FIELDS}
QUERY_IO = {field: 0 for field in gate.QUERY_IO_FIELDS}
TIMING_FIELDS = (
    "publication_duration_ns",
    "freeze_and_admission_ns",
    "seal_ns",
    "inventory_ns",
    "coverage_ns",
    "sample_root_ns",
    "catalog_ns",
    "owner_and_head_ns",
    "owner_validation_ns",
    "head_validation_ns",
    "root_build_ns",
    "begin_commit_root_lock_wait_ns",
    "begin_commit_root_lock_held_ns",
    "commit_root_lock_wait_ns",
    "commit_root_lock_held_ns",
    "old_root_arc_drop_ns",
    "post_commit_ns",
)
MAXIMUM_FIELDS = (
    "pending_fragment_count",
    "pending_estimated_bytes",
    "pending_arena_used_bytes",
    "pending_arena_allocated_bytes",
    "sample_keys",
    "sample_fragments",
    "catalog_active_series",
    "catalog_shared_label_snapshot_bytes",
    "catalog_index_bytes_if_unshared",
    "live_memory_limit_bytes",
    "live_memory_charged_bytes",
    "live_memory_peak_charged_bytes",
    "live_mutable_tail_used_bytes",
    "live_mutable_tail_capacity_bytes",
    "manifest_validated_offset",
)


def workload() -> dict:
    return {
        "schema": gate.WORKLOAD_SCHEMA,
        "description": "test",
        "client": {
            "readiness_timeout_ms": 1000,
            "request_timeout_ms": 1000,
            "inter_batch_delay_ms": 0,
            "parallelism": 2,
            "max_response_bytes": 1024,
            "minimum_successful_requests": 2,
            "minimum_requests_per_query": 2,
            "minimum_same_generation_groups_per_query": 1,
        },
        "queries": [
            {
                "name": "q",
                "mode": "instant",
                "query": "up",
                "time": "1",
                "require_nonempty": True,
                "require_empty": False,
            }
        ],
    }


def record(generation: int = 1, sequence: int = 10, digest: str = "a" * 64) -> dict:
    return {
        "schema": gate.CLIENT_SCHEMA,
        "query_name": "q",
        "mode": "instant",
        "generation": generation,
        "visible_message_sequence": sequence,
        "catalog_revision": 1,
        "response_data_sha256": digest,
        "cardinality": 1,
        "samples": 1,
        "query_stats": dict(STATS),
        "query_io": dict(QUERY_IO),
        "query_duration_ns": 10,
        "serialize_duration_ns": 2,
        "queue_duration_ns": 1,
        "view_age_ms": 3,
        "view_pin_wait_ns": 4,
        "view_pin_held_ns": 5,
        "client_elapsed_ns": 20,
        "client_started_monotonic_ns": 100,
        "client_completed_monotonic_ns": 120,
        "response_bytes": 100,
    }


def publication_line(
    generation: int,
    sequence: int,
    scale: int = 1,
    *,
    mode: str = "boundary",
    final_empty_fast_path: bool | None = None,
    base_scale: tuple[int, int, int] | None = None,
) -> str:
    fields = [
        'DEBUG chronoxide_live_metrics: event="publication" outcome="success"',
        f'mode="{mode}"',
        f"generation={generation}",
        f"visible_message_sequence={sequence}",
        "catalog_revision=1",
        "manifest_present=false",
    ]
    timing_values = {
        "publication_duration_ns": scale * 10,
        "owner_and_head_ns": scale * 2,
    }
    fields.extend(
        f"{name}={timing_values.get(name, scale)}"
        for name in TIMING_FIELDS
    )
    fields.extend(
        f"{name}={0 if mode == 'shutdown' and name in {'sample_keys', 'sample_fragments', 'catalog_active_series'} else scale}"
        for name in MAXIMUM_FIELDS
    )
    if final_empty_fast_path is not None:
        fields.append(
            f"final_empty_fast_path={'true' if final_empty_fast_path else 'false'}"
        )
    if base_scale is not None:
        fields.extend(
            (
                f"base_sample_keys={base_scale[0]}",
                f"base_sample_fragments={base_scale[1]}",
                f"base_catalog_active_series={base_scale[2]}",
            )
        )
    return " ".join(fields)


def replay_document(messages: int = 10) -> dict:
    return {
        "schema": "chronoxide/storage-vnext-replay-correctness/v2",
        "general": {"Total Messages": messages, "Recorded Samples": 10},
        "datapoint_policy_totals": {
            "Observed": 10,
            "Time-Policy Accepted": 10,
            "Dropped Too Old": 0,
            "Dropped Too Future": 0,
        },
        "datapoint_storage_totals": {
            "Recorded Samples": 10,
            "Accepted Not Recorded": 0,
        },
        "partition_watermarks": {"Tracked Messages": messages},
        "otlp_data_type_counts": {
            "Gauge": {
                "metric_records": 1,
                "observed_datapoints": 10,
                "accepted_datapoints": 10,
            }
        },
    }


def storage_verifier_document(samples: int = 8) -> dict:
    return {
        "schema_version": 8,
        "footer_validation_enabled": True,
        "series_sample_per_segment": None,
        "segments": 1,
        "corpus_series": 1,
        "series": 1,
        "chunks": 1,
        "chunks_by_kind": [1, 0, 0, 0, 0],
        "samples": samples,
        "verified_selection_fingerprint": "a" * 64,
        "decoded_semantic_fingerprint": "c" * 64,
        "exact_postings": {
            "logical_fingerprint": "b" * 64,
            "lists": 1,
            "decoded_refs": 1,
            "encoded_bytes": 1,
        },
    }


def write_shutdown_ab_fixture(
    root: Path,
    *,
    candidate: bool,
    ingester_payload: bytes,
    shutdown_scale: int,
    boundary_scale: int = 10,
    peak_rss_kib: int = 1_000,
    api_payload: bytes | None = None,
) -> None:
    (root / "metadata" / "binaries").mkdir(parents=True)
    (root / "runs" / "P").mkdir(parents=True)
    (root / "validation").mkdir()
    (root / "COMPLETE").touch()

    binary_payloads = {
        "chronoxide-ingester": ingester_payload,
        "chronoxide-query": b"same query",
        "chronoxide-storage-verify": b"same verifier",
    }
    if api_payload is not None:
        binary_payloads["chronoxide-api"] = api_payload
    manifest_lines = []
    for role, payload in binary_payloads.items():
        path = root / "metadata" / "binaries" / role
        path.write_bytes(payload)
        manifest_lines.append(
            f"{hashlib.sha256(payload).hexdigest()}  "
            f"{path.relative_to(root).as_posix()}\n"
        )
    (root / "metadata" / "binaries.sha256").write_text(
        "".join(manifest_lines), encoding="ascii"
    )

    run = root / "runs" / "P"
    (run / "ingester.exit-status").write_text("0\n", encoding="ascii")
    (run / "rss-monitor.exit-status").write_text("0\n", encoding="ascii")
    (run / "replay-correctness.json").write_text(
        json.dumps(replay_document()), encoding="utf-8"
    )
    segments = run / "segments"
    segments.mkdir()
    segment_payload = b"same sealed tree"
    (segments / "payload.bin").write_bytes(segment_payload)
    (run / "segments.sha256").write_text(
        f"{hashlib.sha256(segment_payload).hexdigest()}  ./payload.bin\n",
        encoding="ascii",
    )
    (run / "corpus-summary.json").write_text(
        json.dumps(
            {
                "schema": "chronoxide/storage-vnext-phase1-corpus/v1",
                "file_count": 1,
                "size_bytes": len(segment_payload),
                "manifest_sha256": hashlib.sha256(
                    (run / "segments.sha256").read_bytes()
                ).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    (run / "rss-summary.json").write_text(
        json.dumps(
            {
                "samples": 10,
                "aggregate_rss_kib": peak_rss_kib,
                "aggregate_vm_swap_kib": 0,
            }
        ),
        encoding="utf-8",
    )
    live_text = "\n".join(
        (
            publication_line(1, 5, scale=boundary_scale),
            publication_line(
                2,
                10,
                scale=shutdown_scale,
                mode="shutdown",
                final_empty_fast_path=True if candidate else None,
                base_scale=(100, 2, 90) if candidate else None,
            ),
        )
    )
    (run / "ingester.log").write_text(live_text, encoding="utf-8")
    (run / "live-log-summary.json").write_text(
        json.dumps(gate.parse_live_log_text(live_text, 10)), encoding="utf-8"
    )

    (root / "validation" / "storage-verify-gate.json").write_text(
        json.dumps(
            {
                "schema_version": 8,
                "segments": 1,
                "series": 1,
                "chunks": 1,
                "samples": 8,
                "verified_selection_fingerprint": "a" * 64,
                "decoded_semantic_fingerprint": "b" * 64,
                "exact_postings_fingerprint": "c" * 64,
            }
        ),
        encoding="utf-8",
    )
    (root / "validation" / "readbacks-gate.json").write_text(
        json.dumps(
            {
                "expected_queries": 2,
                "executed_queries": 2,
                "skipped_queries": 0,
                "isolation_check_skips": 0,
                "mismatches": 0,
            }
        ),
        encoding="utf-8",
    )


def gnu_time_text(elapsed: str = "0:01.10") -> str:
    return "\n".join(
        (
            "User time (seconds): 1.0",
            "System time (seconds): 0.1",
            "Percent of CPU this job got: 100%",
            f"Elapsed (wall clock) time (h:mm:ss or m:ss): {elapsed}",
            "Maximum resident set size (kbytes): 1000",
            "Major (requiring I/O) page faults: 0",
            "Minor (reclaiming a frame) page faults: 1",
            "Voluntary context switches: 2",
            "Involuntary context switches: 3",
            "File system inputs: 4",
            "File system outputs: 5",
            "Exit status: 0",
            "",
        )
    )


def write_host_process_evidence(run: Path) -> None:
    boot_id = "11111111-2222-3333-4444-555555555555"
    leader_pid = 100
    leader_starttime = 42
    interval_ms = gate.SCALE_HOST_PROCESS_SAMPLE_INTERVAL_MS
    header = {
        "kind": "header",
        "schema": gate.HOST_PROCESS_EVIDENCE_SCHEMA,
        "boot_id": boot_id,
        "clock_ticks_per_second": 100,
        "interval_ms": interval_ms,
        "abort_on_conflict": True,
        "max_vanished_per_scan": (
            gate.SCALE_HOST_PROCESS_MAX_VANISHED_PER_SCAN
        ),
        "max_vanished_ppm": gate.SCALE_HOST_PROCESS_MAX_VANISHED_PPM,
        "classifier_sha256": gate.canonical_sha256(
            gate._scale_process_classifier_contract()
        ),
        "proc_visibility": {
            "hidepid": 0,
            "nspid_depth": 1,
            "pid_namespace": "pid:[1]",
            "pid1_stat_visible": True,
            "pid1_starttime_ticks": 1,
        },
        "expected_session_id": leader_pid,
        "expected_leader_pid": leader_pid,
        "expected_leader_starttime_ticks": leader_starttime,
    }
    records = [gate._compact_json_line(header)]
    stream_hash = hashlib.sha256(records[0])
    first_started = 1_000_000_000
    samples = []
    for sequence in range(6):
        started = first_started + sequence * interval_ms * 1_000_000
        benign = {
            "pid": 1,
            "ppid": 0,
            "pgrp": 1,
            "session": 1,
            "starttime_ticks": 1,
            "state": "S",
            "comm": "init",
            "argv0": "/sbin/init",
        }
        processes = [benign]
        if sequence < 5:
            processes.append(
                {
                    "pid": leader_pid,
                    "ppid": 1,
                    "pgrp": leader_pid,
                    "session": leader_pid,
                    "starttime_ticks": leader_starttime,
                    "state": "T" if sequence == 0 else "S",
                    "comm": "sh" if sequence == 0 else "chronoxide-inge",
                    "argv0": (
                        "/bin/sh"
                        if sequence == 0
                        else "/frozen/chronoxide-ingester"
                    ),
                }
            )
        sample = {
            "kind": "sample",
            "sequence": sequence,
            "scan_started_boottime_ns": started,
            "scan_ended_boottime_ns": started + 10_000_000,
            "listed_pid_count": len(processes),
            "vanished_pid_count": 0,
            "process_count": len(processes),
            "processes": processes,
        }
        samples.append(sample)
        line = gate._compact_json_line(sample)
        records.append(line)
        stream_hash.update(line)
    records.append(
        gate._compact_json_line(
            {
                "kind": "footer",
                "sample_count": len(samples),
                "first_scan_started_boottime_ns": samples[0][
                    "scan_started_boottime_ns"
                ],
                "last_scan_ended_boottime_ns": samples[-1][
                    "scan_ended_boottime_ns"
                ],
                "stop_observed_boottime_ns": 2_200_000_000,
                "stream_sha256": stream_hash.hexdigest(),
            }
        )
    )
    (run / "host-process-samples.jsonl").write_bytes(b"".join(records))
    (run / "host-process-monitor-ready.json").write_text(
        json.dumps(
            {
                "schema": gate.HOST_PROCESS_READY_SCHEMA,
                "boot_id": boot_id,
                "expected_session_id": leader_pid,
                "expected_leader_pid": leader_pid,
                "expected_leader_starttime_ticks": leader_starttime,
                "first_sample_scan_ended_boottime_ns": samples[0][
                    "scan_ended_boottime_ns"
                ],
                "header_sha256": hashlib.sha256(records[0]).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    boundaries = {
        "start": (1_020_000_000, True),
        "end": (2_130_000_000, False),
    }
    for phase, (recorded, present) in boundaries.items():
        (run / f"host-process-{phase}.json").write_text(
            json.dumps(
                {
                    "schema": gate.HOST_PROCESS_BOUNDARY_SCHEMA,
                    "phase": phase,
                    "boot_id": boot_id,
                    "recorded_boottime_ns": recorded,
                    "expected_leader_pid": leader_pid,
                    "expected_leader_starttime_ticks": leader_starttime,
                    "expected_leader_present": present,
                }
            ),
            encoding="utf-8",
        )
    (run / "host-process-monitor.exit-status").write_text(
        "0\n", encoding="ascii"
    )
    (run / "host-process-monitor.log").write_text(
        "test monitor\n", encoding="utf-8"
    )
    (run / "host-process-monitor.time.txt").write_text(
        gnu_time_text("0:01.30"), encoding="utf-8"
    )


def rewrite_host_process_evidence(run: Path, mutate) -> None:
    path = run / "host-process-samples.jsonl"
    values = [
        json.loads(line)
        for line in path.read_text(encoding="ascii").splitlines()
    ]
    mutate(values)
    header = values[0]
    samples = [value for value in values[1:] if value.get("kind") == "sample"]
    records = [gate._compact_json_line(header)]
    stream_hash = hashlib.sha256(records[0])
    for sample in samples:
        sample["process_count"] = len(sample["processes"])
        sample["listed_pid_count"] = (
            sample["process_count"] + sample["vanished_pid_count"]
        )
        line = gate._compact_json_line(sample)
        records.append(line)
        stream_hash.update(line)
    end_boundary = json.loads(
        (run / "host-process-end.json").read_text(encoding="utf-8")
    )["recorded_boottime_ns"]
    stop_observed = max(
        end_boundary,
        samples[-2]["scan_ended_boottime_ns"],
    )
    records.append(
        gate._compact_json_line(
            {
                "kind": "footer",
                "sample_count": len(samples),
                "first_scan_started_boottime_ns": samples[0][
                    "scan_started_boottime_ns"
                ],
                "last_scan_ended_boottime_ns": samples[-1][
                    "scan_ended_boottime_ns"
                ],
                "stop_observed_boottime_ns": stop_observed,
                "stream_sha256": stream_hash.hexdigest(),
            }
        )
    )
    path.write_bytes(b"".join(records))


def write_result_artifact_manifest(root: Path) -> None:
    manifest = root / "metadata" / "result-artifacts.sha256"
    rows = []
    for top in ("configs", "metadata", "validation", "comparisons", "runs"):
        for path in (root / top).rglob("*"):
            if (
                not path.is_file()
                or path == manifest
                or path.is_relative_to(root / "runs" / "P" / "segments")
            ):
                continue
            relative = path.relative_to(root).as_posix()
            rows.append((relative, hashlib.sha256(path.read_bytes()).hexdigest()))
    for name in ("run-plan.tsv", "run-summary.tsv"):
        path = root / name
        if path.is_file():
            rows.append((name, hashlib.sha256(path.read_bytes()).hexdigest()))
    manifest.write_text(
        "".join(f"{digest}  {relative}\n" for relative, digest in sorted(rows)),
        encoding="ascii",
    )


def write_scale_fixture(
    root: Path,
    *,
    messages: int = 125_000,
    boundary_scale: int = 10,
) -> dict:
    write_shutdown_ab_fixture(
        root,
        candidate=True,
        ingester_payload=b"candidate",
        shutdown_scale=1,
        boundary_scale=boundary_scale,
        api_payload=b"api",
    )
    run = root / "runs" / "P"
    rss_rows = [
        (
            f"{sequence * 100_000_000}\t2026-01-01T00:00:00+00:00\t"
            "1\t1000\t800\t200\t0\t1000\t100\n"
        )
        for sequence in range(10)
    ]
    (run / "rss-samples.tsv").write_text(
        (
            "elapsed_ns\trecorded_at\tprocess_count\trss_kib\t"
            "rss_anon_kib\trss_file_kib\tvm_swap_kib\t"
            "max_single_hwm_kib\tpids\n"
            + "".join(rss_rows)
        ),
        encoding="utf-8",
    )
    (run / "rss-summary.json").write_text(
        json.dumps(
            {
                "root_pid": 100,
                "root_starttime_ticks": 42,
                "samples": 10,
                "interval_ms": 100,
                "aggregate_rss_kib": 1000,
                "aggregate_rss_anon_kib": 800,
                "aggregate_rss_file_kib": 200,
                "aggregate_vm_swap_kib": 0,
                "max_single_process_hwm_kib": 1000,
                "process_count": 1,
            }
        ),
        encoding="utf-8",
    )
    capture_path = "/capture"
    capture_file = {
        "name": "partition-1.capture",
        "sha256": "1" * 64,
        "size_bytes": 123,
    }
    capture_manifest = json.dumps(
        {
            "version": 2,
            "topic": "otlp_metrics",
            "compression": "zstd",
            "partitions": [
                {
                    "partition": 1,
                    "file_name": capture_file["name"],
                    "message_count": 1_000_000,
                    "total_uncompressed_payload_bytes": 100,
                    "total_compressed_payload_bytes": 50,
                }
            ],
        },
        separators=(",", ":"),
    ).encode()
    template = f"""
[kafka]
topic = "otlp_metrics"
[ingestion]
max_event_age_secs = 3600
max_event_lead_secs = 5
drop_outdated = true
labelset_store = "flat_interned"
replay_from = "{capture_path}"
capture_only = false
stop_after_messages = 1
[ingestion.head_buffer]
enabled = true
window_duration_secs = 3600
out_of_order_time_window_secs = 3600
float_encoding = "gorilla"
int_encoding = "delta_zig_zag"
varlen_encoding = "raw"
compact_numeric_series = true
adaptive_series_table = true
[ingestion.segment_writer]
enabled = true
segments_dir = "/old"
segment_duration_secs = 900
deterministic_id_seed = 42
storage_schema = "schema8"
float_encoding = "gorilla"
int_encoding = "delta_zig_zag"
varlen_encoding = "raw"
""".lstrip()
    (root / "metadata" / "capture-manifest.json").write_bytes(capture_manifest)
    (root / "metadata" / "config-template.toml").write_text(
        template, encoding="utf-8"
    )
    capture_manifest_sha256 = hashlib.sha256(capture_manifest).hexdigest()
    template_sha256 = hashlib.sha256(template.encode()).hexdigest()
    capacity_inputs = {
        "capture": capture_path,
        "capture_files": [capture_file],
        "capture_manifest_sha256": capture_manifest_sha256,
        "config_template": "/template.toml",
        "config_template_sha256": template_sha256,
        "stop_after_messages": 1_000_000,
    }
    (root / "metadata" / "capture-capacity.json").write_text(
        json.dumps(capacity_inputs),
        encoding="utf-8",
    )
    (root / "metadata" / "validated-inputs.json").write_text(
        json.dumps(
            {
                "schema": gate.SELECTED_INPUT_PREFIX_SCHEMA,
                **{
                    key: capacity_inputs[key]
                    for key in gate.SELECTED_INPUT_PREFIX_IDENTITY_FIELDS
                },
                "validated_capture_capacity_messages": 1_000_000,
                "stop_after_messages": messages,
            }
        ),
        encoding="utf-8",
    )
    (root / "metadata" / "cpusets.json").write_text(
        json.dumps(
            {
                "allowed": [0, 1, 2, 3],
                "client": [0, 1],
                "ingest": [2, 3],
            }
        ),
        encoding="utf-8",
    )
    settings = {
        "recorded_at": "now",
        "result_dir": str(root.resolve()),
        "capture": capture_path,
        "config_template": "/template.toml",
        "stop_after_messages": str(messages),
        "run_order": "P",
        "diagnostic_p_only": "1",
        "ingest_cpuset": "2-3",
        "client_cpuset": "0-1",
        "api_listen": "127.0.0.1:19091",
        "live_memory_admission_bytes": "1024",
        "publish_interval_ms": "1000",
        "max_view_staleness_ms": "600000",
        "max_concurrent_queries": "4",
        "range_scalar_cache_max_bytes": "0",
        "rss_interval_ms": "100",
        "host_process_sample_interval_ms": str(
            gate.SCALE_HOST_PROCESS_SAMPLE_INTERVAL_MS
        ),
        "perf_stat_mode": "required",
        "evict_capture": "1",
        "allow_noisy_host": "0",
        "readback_sample_limit_per_kind": "2",
        "max_host_load_per_cpu": "1.0",
        "max_cpu_psi_avg10": "10.0",
        "max_io_psi_avg10": "5.0",
        "max_memory_psi_avg10": "2.0",
        "run_note": "test scale run",
    }
    (root / "metadata" / "settings.txt").write_text(
        "".join(f"{key}={value}\n" for key, value in settings.items()),
        encoding="utf-8",
    )
    (root / "metadata" / "perf-preflight.txt").write_text(
        "perf preflight\n", encoding="utf-8"
    )
    (root / "metadata" / "PHYSICAL_SAMPLE_COUNT_COVERAGE_GAP").write_text(
        gate.SCALE_P_ONLY_COVERAGE_GAP + "\n", encoding="utf-8"
    )
    harness = root / "metadata" / "harness"
    harness.mkdir()
    (harness / "live_query_ingest_ab_gate.py").write_bytes(
        b"producer-frozen gate"
    )
    (harness / "phase1_replay_gate.py").write_bytes(
        b"producer-frozen phase1"
    )
    (harness / "live_query_ingest_ab_run.sh").write_bytes(
        b"producer-frozen runner"
    )

    config_dir = root / "configs"
    comparisons = root / "comparisons"
    config_dir.mkdir()
    comparisons.mkdir()
    (comparisons / "DPQ_GATE_NOT_APPLICABLE").write_text(
        "P only\n", encoding="utf-8"
    )
    config = template.replace(
        "stop_after_messages = 1", f"stop_after_messages = {messages}"
    ).replace(
        'segments_dir = "/old"',
        f'segments_dir = "{run / "segments"}"',
    )
    config += """
[api]
enabled = true
listen = "127.0.0.1:19091"
head_publish_interval_ms = 1000
max_view_staleness_ms = 600000
live_memory_admission_bytes = 1024
max_concurrent_queries = 4
query_max_series_matched = 1000000
query_max_projected_series = 2000000
query_max_chunks_read = 5000000
query_max_bytes_read = 2147483648
query_max_samples = 50000000
regex_max_expanded_values = 100000
chunk_read_mode = "pread"
chunk_read_queue_depth = 128
chunk_payload_coalesce_max_gap_bytes = 4096
experimental_cross_segment_chunk_reads = false
range_scalar_cache_max_bytes = 0
"""
    config_path = config_dir / "P.toml"
    config_path.write_text(config, encoding="utf-8")
    (run / "config-render.json").write_text(
        json.dumps(
            {
                "variant": "P",
                "api_enabled": True,
                "capture": capture_path,
                "segments_dir": str(run / "segments"),
                "stop_after_messages": messages,
                "config_sha256": hashlib.sha256(config.encode()).hexdigest(),
            }
        ),
        encoding="utf-8",
    )

    live_lines = []
    for generation in range(1, 11):
        sequence = messages * generation // 10
        live_lines.append(
            publication_line(
                generation,
                sequence,
                scale=boundary_scale,
            )
        )
        live_lines.append(
            'DEBUG chronoxide_live_metrics: event="message_boundary" '
            'outcome="success" ingestion_pause_ns=1'
        )
    live_lines.append(
        publication_line(
            11,
            messages,
            scale=1,
            mode="shutdown",
            final_empty_fast_path=True,
            base_scale=(100, 2, 90),
        )
    )
    live_text = "\n".join(live_lines)
    (run / "ingester.log").write_text(live_text, encoding="utf-8")
    (run / "live-log-summary.json").write_text(
        json.dumps(gate.parse_live_log_text(live_text, messages)),
        encoding="utf-8",
    )
    (run / "replay-correctness.json").write_text(
        json.dumps(replay_document(messages)), encoding="utf-8"
    )
    time_text = gnu_time_text()
    (run / "replay.time.txt").write_text(time_text, encoding="utf-8")
    (run / "replay.time.json").write_text(
        json.dumps(gate._parse_gnu_time_text(time_text, "test")),
        encoding="utf-8",
    )
    perf_text = "".join(
        f"1\t\t{event}\t\n" for event in gate.REQUIRED_PERF_EVENTS
    )
    (run / "perf-stat.tsv").write_text(perf_text, encoding="utf-8")
    (run / "perf-stat.json").write_text(
        json.dumps(gate._parse_perf_text(perf_text)), encoding="utf-8"
    )
    (run / "capture-residency-before.tsv").write_text(
        f"0 {capture_file['size_bytes']} {capture_path}/{capture_file['name']}\n",
        encoding="utf-8",
    )
    for name in ("processes-before.txt", "processes-after.txt"):
        (run / name).write_text(
            "1 0 0.0 1 S init /sbin/init\n", encoding="utf-8"
        )
    pressure_text = "\n".join(
        (
            "2026-01-01T00:00:00Z",
            "0.10 0.10 0.10 1/100 1",
            "/proc/pressure/cpu",
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=0",
            "full avg10=0.00 avg60=0.00 avg300=0.00 total=0",
            "/proc/pressure/io",
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=0",
            "full avg10=0.00 avg60=0.00 avg300=0.00 total=0",
            "/proc/pressure/memory",
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=0",
            "full avg10=0.00 avg60=0.00 avg300=0.00 total=0",
            "",
        )
    )
    for phase in ("before", "after"):
        (run / f"pressure-{phase}.txt").write_text(
            pressure_text, encoding="utf-8"
        )
    write_host_process_evidence(run)
    (root / "run-plan.tsv").write_text(
        "order\tvariant\tconfig\tsegments_dir\n", encoding="utf-8"
    )
    (root / "run-summary.tsv").write_text(
        "variant\telapsed\nP\t0:01.10\n", encoding="utf-8"
    )

    raw_storage = root / "validation" / "storage-verify.json"
    raw_storage.write_text(
        json.dumps(storage_verifier_document()), encoding="utf-8"
    )
    normalized_storage = gate.validate_storage_verifier(
        raw_storage,
        run / "replay-correctness.json",
        run / "ingester.log",
        require_writer_reconciliation=False,
    )
    (root / "validation" / "storage-verify-gate.json").write_text(
        json.dumps(normalized_storage), encoding="utf-8"
    )
    readback_queries = gate.SCALE_EXPECTED_READBACK_QUERIES[messages]
    readbacks = f"""
## Readback Verification
| Metric | Value |
|---|---|
| Checked Queries | {readback_queries} |
| Mismatches | 0 |
## Query Diagnostics
| Metric | Value |
|---|---|
| Expected Readback Queries | {readback_queries} |
| Executed Readback Queries | {readback_queries} |
| Skipped Readback Queries | 0 |
| Isolation Check Skips | 0 |
""".lstrip()
    (root / "validation" / "readbacks.md").write_text(
        readbacks, encoding="utf-8"
    )
    (root / "validation" / "readbacks-gate.json").write_text(
        json.dumps(gate.validate_readbacks(root / "validation" / "readbacks.md")),
        encoding="utf-8",
    )
    for name in ("storage-verify.time.txt", "readbacks.time.txt"):
        (root / "validation" / name).write_text(time_text, encoding="utf-8")
    write_result_artifact_manifest(root)

    binary_hashes = {}
    for role in (
        "chronoxide-ingester",
        "chronoxide-api",
        "chronoxide-query",
        "chronoxide-storage-verify",
    ):
        binary_hashes[role] = hashlib.sha256(
            (root / "metadata" / "binaries" / role).read_bytes()
        ).hexdigest()
    return {
        "binary_hashes": binary_hashes,
        "capture_manifest_sha256": capture_manifest_sha256,
        "capture_file": capture_file,
        "config_template_sha256": template_sha256,
        "ingest_cpuset": "2-3",
        "client_cpuset": "0-1",
        "api_listen": "127.0.0.1:19091",
        "live_memory_admission_bytes": 1024,
        "publish_interval_ms": 1000,
        "max_view_staleness_ms": 600000,
        "max_concurrent_queries": 4,
        "range_scalar_cache_max_bytes": 0,
        "rss_interval_ms": 100,
    }


def scale_gate_cli_arguments(
    root: Path,
    messages: int,
    expected: dict,
    output: Path,
) -> list[str]:
    binary_hashes = expected["binary_hashes"]
    capture_file = expected["capture_file"]
    return [
        "gate-publication-scale",
        "--root",
        str(root),
        "--expected-messages",
        str(messages),
        "--expected-ingester-sha256",
        binary_hashes["chronoxide-ingester"],
        "--expected-api-sha256",
        binary_hashes["chronoxide-api"],
        "--expected-query-sha256",
        binary_hashes["chronoxide-query"],
        "--expected-storage-verify-sha256",
        binary_hashes["chronoxide-storage-verify"],
        "--expected-capture-manifest-sha256",
        expected["capture_manifest_sha256"],
        "--expected-capture-file-name",
        capture_file["name"],
        "--expected-capture-file-sha256",
        capture_file["sha256"],
        "--expected-capture-file-size-bytes",
        str(capture_file["size_bytes"]),
        "--expected-config-template-sha256",
        expected["config_template_sha256"],
        "--expected-ingest-cpuset",
        expected["ingest_cpuset"],
        "--expected-client-cpuset",
        expected["client_cpuset"],
        "--expected-api-listen",
        expected["api_listen"],
        "--expected-live-memory-admission-bytes",
        str(expected["live_memory_admission_bytes"]),
        "--expected-publish-interval-ms",
        str(expected["publish_interval_ms"]),
        "--expected-max-view-staleness-ms",
        str(expected["max_view_staleness_ms"]),
        "--expected-max-concurrent-queries",
        str(expected["max_concurrent_queries"]),
        "--expected-range-scalar-cache-max-bytes",
        str(expected["range_scalar_cache_max_bytes"]),
        "--expected-rss-interval-ms",
        str(expected["rss_interval_ms"]),
        "--output",
        str(output),
    ]


class GateTests(unittest.TestCase):
    def test_bind_selected_input_prefix_records_capacity_and_selected_cut(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capacity = root / "capacity.json"
            output = root / "selected.json"
            capacity.write_text(
                json.dumps(
                    {
                        "capture": "/capture",
                        "capture_files": [{"name": "partition-1.capture"}],
                        "capture_manifest_sha256": "capture-hash",
                        "config_template": "/template",
                        "config_template_sha256": "template-hash",
                        "stop_after_messages": 4_000_000,
                    }
                ),
                encoding="utf-8",
            )

            selected = gate.bind_selected_input_prefix(
                capacity, 250_000, output
            )

            self.assertEqual(selected["stop_after_messages"], 250_000)
            self.assertEqual(
                selected["schema"], gate.SELECTED_INPUT_PREFIX_SCHEMA
            )
            self.assertEqual(
                selected["validated_capture_capacity_messages"], 4_000_000
            )
            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")), selected
            )

    def test_bind_selected_input_prefix_rejects_unvalidated_cut(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capacity = root / "capacity.json"
            capacity.write_text(
                json.dumps(
                    {
                        "capture": "/capture",
                        "capture_files": [{"name": "partition-1.capture"}],
                        "capture_manifest_sha256": "capture-hash",
                        "config_template": "/template",
                        "config_template_sha256": "template-hash",
                        "stop_after_messages": 125_000,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                gate.GateError, "exceeds validated capture capacity"
            ):
                gate.bind_selected_input_prefix(
                    capacity, 250_000, root / "selected.json"
                )

    def test_bind_selected_input_prefix_accepts_exact_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capacity = root / "capacity.json"
            capacity.write_text(
                json.dumps(
                    {
                        "capture": "/capture",
                        "capture_files": [{"name": "partition-1.capture"}],
                        "capture_manifest_sha256": "capture-hash",
                        "config_template": "/template",
                        "config_template_sha256": "template-hash",
                        "stop_after_messages": 250_000,
                    }
                ),
                encoding="utf-8",
            )

            selected = gate.bind_selected_input_prefix(
                capacity, 250_000, root / "selected.json"
            )

            self.assertEqual(
                selected["validated_capture_capacity_messages"], 250_000
            )
            self.assertEqual(selected["stop_after_messages"], 250_000)

    def test_bind_selected_input_prefix_rejects_malformed_capacity_shape(
        self,
    ) -> None:
        base = {
            "capture": "/capture",
            "capture_files": [{"name": "partition-1.capture"}],
            "capture_manifest_sha256": "capture-hash",
            "config_template": "/template",
            "config_template_sha256": "template-hash",
            "stop_after_messages": 250_000,
        }
        mutations = {
            "missing": {
                key: value for key, value in base.items() if key != "capture"
            },
            "extra": {**base, "unbound": True},
        }
        for label, value in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                capacity = root / "capacity.json"
                capacity.write_text(json.dumps(value), encoding="utf-8")
                with self.assertRaisesRegex(
                    gate.GateError, "unexpected shape"
                ):
                    gate.bind_selected_input_prefix(
                        capacity, 250_000, root / "selected.json"
                    )

    def test_bind_selected_input_prefix_rejects_invalid_counts(self) -> None:
        base = {
            "capture": "/capture",
            "capture_files": [{"name": "partition-1.capture"}],
            "capture_manifest_sha256": "capture-hash",
            "config_template": "/template",
            "config_template_sha256": "template-hash",
            "stop_after_messages": 250_000,
        }
        cases = (
            ("zero-capacity", 0, 1),
            ("bool-capacity", True, 1),
            ("zero-selected", 250_000, 0),
            ("bool-selected", 250_000, True),
        )
        for label, capacity_count, selected_count in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                capacity = root / "capacity.json"
                capacity.write_text(
                    json.dumps(
                        {**base, "stop_after_messages": capacity_count}
                    ),
                    encoding="utf-8",
                )
                with self.assertRaises(gate.GateError):
                    gate.bind_selected_input_prefix(
                        capacity, selected_count, root / "selected.json"
                    )

    def test_scale_validator_bootstrap_loads_exact_sources_in_isolation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            bundle = parent / "bundle"
            bundle.mkdir()
            for name in (
                "live_query_scale_validator_bootstrap.py",
                "live_query_ingest_ab_gate.py",
                "phase1_replay_gate.py",
            ):
                shutil.copyfile(HERE / name, bundle / name)
            without_flags = subprocess.run(
                [
                    sys.executable,
                    "-S",
                    "-B",
                    str(bundle / "live_query_scale_validator_bootstrap.py"),
                    "--help",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(without_flags.returncode, 0)
            self.assertIn(
                "requires Python -I -S -B", without_flags.stderr
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    "-B",
                    str(bundle / "live_query_scale_validator_bootstrap.py"),
                    "--help",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("gate-publication-scale", result.stdout)

            root = parent / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            with self.assertRaisesRegex(
                gate.GateError, "requires the exact isolated validator bootstrap"
            ):
                gate.gate_publication_scale(root, 250_000, expected)

            output = parent / "publication-scale-v2.json"
            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    "-B",
                    str(bundle / "live_query_scale_validator_bootstrap.py"),
                    *scale_gate_cli_arguments(
                        root, 250_000, expected, output
                    ),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            certificate = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(
                certificate["schema"],
                "chronoxide/live-query-publication-scale/v2",
            )
            validator = certificate["validator"]
            self.assertTrue(validator["authoritative"])
            self.assertEqual(
                validator["validation_kind"],
                "post-hoc-sealed-root-validation",
            )
            for field, name in (
                ("bootstrap", "live_query_scale_validator_bootstrap.py"),
                ("entrypoint", "live_query_ingest_ab_gate.py"),
                ("phase1", "phase1_replay_gate.py"),
            ):
                authority = validator["loaded_sources"][field]
                self.assertEqual(
                    authority["sha256"],
                    hashlib.sha256((bundle / name).read_bytes()).hexdigest(),
                )
            self.assertTrue(validator["python"]["isolated"])
            self.assertTrue(validator["python"]["no_site"])
            self.assertTrue(validator["python"]["dont_write_bytecode"])

    def test_runner_freezes_api_before_measured_arms(self) -> None:
        runner = (HERE / "live_query_ingest_ab_run.sh").read_text(encoding="utf-8")
        require = 'require_executable API_BIN "$API_BIN"'
        preserve = 'preserve_binary chronoxide-api "$API_BIN"'
        harness_copy = (
            'cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$file" '
            '"$METADATA_DIR/harness/$file"'
        )
        capacity_validation = 'python3 "$FROZEN_PHASE1" validate-inputs'
        prefix_binding = 'python3 "$FROZEN_GATE" bind-selected-input-prefix'
        config_render = 'python3 "$FROZEN_GATE" render-config'
        measured_run = 'run_variant "$variant"'
        self.assertIn('API_BIN="${API_BIN:-}"', runner)
        self.assertIn(require, runner)
        self.assertIn(preserve, runner)
        self.assertIn("allow_noisy_host=%s", runner)
        self.assertIn("readback_sample_limit_per_kind=%s", runner)
        self.assertIn("live_query_scale_validator_bootstrap.py", runner)
        self.assertIn(
            '--output "$METADATA_DIR/capture-capacity.json"', runner
        )
        self.assertIn("bind-selected-input-prefix", runner)
        self.assertIn(
            '--stop-after-messages "$STOP_AFTER_MESSAGES"', runner
        )
        ordered = [
            runner.index(harness_copy),
            runner.index(capacity_validation),
            runner.index(prefix_binding),
            runner.index(config_render),
            runner.index(measured_run),
        ]
        self.assertEqual(
            ordered,
            sorted(ordered),
            "runner must freeze the harness, bind input provenance, render "
            "configs, and only then start a measured arm",
        )
        self.assertIn(
            "P-only live-handoff validation has no independent per-window "
            "writer-row reconciliation",
            runner,
        )
        self.assertIn(
            "independent readbacks on P after the measured run",
            runner,
        )
        self.assertIn(
            "runs run-plan.tsv run-summary.tsv",
            runner,
        )
        barrier = runner.index("kill -STOP")
        monitor = runner.index("monitor-host-processes", barrier)
        start_boundary = runner.index("--phase start", monitor)
        release = runner.index('kill -CONT "$launcher_pid"', start_boundary)
        supervised_wait = runner.index(
            "wait -n -p completed_pid", release
        )
        end_boundary = runner.index("--phase end", supervised_wait)
        monitor_stop = runner.index(
            ': >"$run_dir/host-process-monitor-stop"', end_boundary
        )
        monitor_wait = runner.index(
            'wait "$host_monitor_pid"', monitor_stop
        )
        self.assertLess(barrier, monitor)
        self.assertLess(monitor, start_boundary)
        self.assertLess(start_boundary, release)
        self.assertLess(release, supervised_wait)
        self.assertLess(supervised_wait, end_boundary)
        self.assertLess(end_boundary, monitor_stop)
        self.assertLess(monitor_stop, monitor_wait)
        self.assertIn(
            "exec setsid taskset --cpu-list \"$CLIENT_CPUSET\"",
            runner,
        )
        self.assertIn(
            'host_monitor_conflict_args+=(--abort-on-conflict)',
            runner,
        )
        monitor_first = runner[
            runner.index(
                'if [[ "$completed_pid" == "$host_monitor_pid" ]]'
            ) : runner.index(
                '[[ "$completed_pid" == "$launcher_pid" ]]'
            )
        ]
        self.assertIn('kill -KILL -- "-$launcher_pid"', monitor_first)
        self.assertNotIn('kill -TERM -- "-$launcher_pid"', monitor_first)
        self.assertNotIn('kill -KILL -- "-$host_monitor_pid"', monitor_first)
        self.assertLess(runner.index(require), runner.index(preserve))
        self.assertLess(runner.index(preserve), runner.index(measured_run))

    def test_distribution_uses_nearest_rank(self) -> None:
        self.assertEqual(
            gate.distribution([1, 2, 3, 4, 100]),
            {"count": 5, "min": 1, "p50": 3, "p95": 100, "p99": 100, "max": 100},
        )

    def test_cpuset_parser_rejects_reversed_and_accepts_ranges(self) -> None:
        self.assertEqual(gate.parse_cpuset("1,3-5"), {1, 3, 4, 5})
        with self.assertRaises(gate.GateError):
            gate.parse_cpuset("5-3")

    def test_workload_validation_rejects_duplicate_query_names(self) -> None:
        value = workload()
        value["queries"].append(dict(value["queries"][0]))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workload.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(gate.GateError):
                gate.load_workload(path)

    def test_render_config_pins_all_api_controls_and_fresh_output(self) -> None:
        template_text = """
[kafka]
topic = "otlp_metrics"
[ingestion]
max_event_age_secs = 3600
max_event_lead_secs = 5
drop_outdated = true
labelset_store = "flat_interned"
replay_from = "/old/capture"
capture_only = false
stop_after_messages = 1
[ingestion.head_buffer]
enabled = true
window_duration_secs = 3600
out_of_order_time_window_secs = 3600
float_encoding = "gorilla"
int_encoding = "delta_zig_zag"
varlen_encoding = "raw"
compact_numeric_series = true
adaptive_series_table = true
[ingestion.segment_writer]
enabled = true
segments_dir = "/old/segments"
segment_duration_secs = 900
deterministic_id_seed = 42
storage_schema = "schema8"
float_encoding = "gorilla"
int_encoding = "delta_zig_zag"
varlen_encoding = "raw"
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template = root / "template.toml"
            output = root / "config.toml"
            capture = root / "capture"
            segments = root / "segments"
            template.write_text(template_text, encoding="utf-8")
            capture.mkdir()
            rendered = gate.render_config(
                template,
                output,
                capture,
                segments,
                10,
                "Q",
                "127.0.0.1:19091",
                1000,
                10000,
                1 << 30,
                4,
                0,
            )
            self.assertTrue(rendered["api_enabled"])
            with output.open("rb") as source:
                document = __import__("tomllib").load(source)
            self.assertEqual(document["api"]["chunk_read_mode"], "pread")
            self.assertEqual(document["api"]["query_max_series_matched"], 1_000_000)
            self.assertEqual(document["api"]["range_scalar_cache_max_bytes"], 0)
            with self.assertRaisesRegex(gate.GateError, "fresh"):
                gate.render_config(
                    template,
                    output,
                    capture,
                    segments,
                    10,
                    "Q",
                    "127.0.0.1:19091",
                    1000,
                    10000,
                    1 << 30,
                    4,
                    0,
                )

    def test_same_generation_requires_one_cut_and_one_result_shape(self) -> None:
        good_records = [record(), record(), record(2, 11), record(2, 11)]
        good = gate.validate_client_records(good_records, workload())
        self.assertEqual(good["same_generation_groups_per_query"], {"q": 2})
        changed_cut = [record(), record(sequence=11), record(2, 12), record(2, 12)]
        with self.assertRaisesRegex(gate.GateError, "maps to both"):
            gate.validate_client_records(changed_cut, workload())
        changed_result = [
            record(),
            record(digest="b" * 64),
            record(2, 11),
            record(2, 11),
        ]
        with self.assertRaisesRegex(gate.GateError, "same-generation"):
            gate.validate_client_records(changed_result, workload())

    def test_generation_cut_mapping_must_advance_and_not_regress(self) -> None:
        records = [record(1, 10), record(1, 10), record(2, 11), record(2, 11)]
        gate.validate_client_records(records, workload())
        records[-2]["visible_message_sequence"] = 9
        records[-1]["visible_message_sequence"] = 9
        with self.assertRaisesRegex(gate.GateError, "regressed"):
            gate.validate_client_records(records, workload())

    def test_log_parser_summarizes_publications_and_pauses(self) -> None:
        text = "\n".join(
            (
                publication_line(1, 5, 10),
                'DEBUG chronoxide_live_metrics: event="message_boundary" outcome="success" ingestion_pause_ns=7',
                publication_line(
                    2,
                    10,
                    20,
                    mode="shutdown",
                    final_empty_fast_path=True,
                    base_scale=(200, 20, 180),
                ),
                'DEBUG chronoxide_live_metrics: event="message_boundary" outcome="success" ingestion_pause_ns=9',
            )
        )
        summary = gate.parse_live_log_text(text, 10)
        self.assertEqual(summary["successful_publications"], 2)
        self.assertEqual(summary["boundary_publications"], 1)
        self.assertEqual(summary["ingestion_pause_ns"]["p50"], 7)
        self.assertEqual(
            summary["publication_timings_ns"]["publication_duration_ns"]["max"], 200
        )
        self.assertEqual(
            summary["boundary_publication_timings_ns"]["publication_duration_ns"][
                "max"
            ],
            100,
        )
        self.assertEqual(
            summary["shutdown_publication"]["timings_ns"]["post_seal_ns"], 180
        )
        self.assertEqual(
            summary["shutdown_publication"]["timings_ns"]["after_inventory_ns"],
            140,
        )
        self.assertTrue(
            summary["shutdown_publication"]["final_empty_fast_path"]
        )
        self.assertEqual(
            summary["shutdown_publication"]["base_scale"],
            {
                "base_sample_keys": 200,
                "base_sample_fragments": 20,
                "base_catalog_active_series": 180,
            },
        )
        self.assertEqual(summary["publication_maxima"]["catalog_active_series"], 10)

    def test_log_parser_rejects_final_cut_mismatch(self) -> None:
        with self.assertRaisesRegex(gate.GateError, "final"):
            gate.parse_live_log_text(
                publication_line(1, 9, mode="shutdown"), 10
            )

    def test_log_parser_requires_one_final_shutdown_publication(self) -> None:
        with self.assertRaisesRegex(gate.GateError, "exactly one"):
            gate.parse_live_log_text(publication_line(1, 10), 10)
        text = "\n".join(
            (
                publication_line(1, 5, mode="shutdown"),
                publication_line(2, 10, mode="shutdown"),
            )
        )
        with self.assertRaisesRegex(gate.GateError, "exactly one"):
            gate.parse_live_log_text(text, 10)
        text = "\n".join(
            (
                publication_line(2, 10),
                publication_line(1, 5, mode="shutdown"),
            )
        )
        with self.assertRaisesRegex(gate.GateError, "not the final"):
            gate.parse_live_log_text(text, 10)
        text = "\n".join(
            (
                publication_line(1, 5),
                publication_line(3, 10, mode="shutdown"),
                publication_line(2, 7),
            )
        )
        with self.assertRaisesRegex(gate.GateError, "last observed"):
            gate.parse_live_log_text(text, 10)

    def test_log_parser_accepts_historical_shutdown_without_optional_fields(
        self,
    ) -> None:
        summary = gate.parse_live_log_text(
            publication_line(1, 10, mode="shutdown"), 10
        )
        self.assertIsNone(
            summary["shutdown_publication"]["final_empty_fast_path"]
        )
        self.assertIsNone(summary["shutdown_publication"]["base_scale"])

    def test_log_parser_rejects_incomplete_base_scale_and_invalid_stage_totals(
        self,
    ) -> None:
        incomplete = (
            publication_line(1, 10, mode="shutdown")
            + " base_sample_keys=1 base_sample_fragments=1"
        )
        with self.assertRaisesRegex(gate.GateError, "incomplete base-scale"):
            gate.parse_live_log_text(incomplete, 10)
        invalid = publication_line(1, 10, mode="shutdown").replace(
            "publication_duration_ns=10", "publication_duration_ns=1"
        )
        with self.assertRaisesRegex(gate.GateError, "durations exceed"):
            gate.parse_live_log_text(invalid, 10)
        invalid_owner_substages = publication_line(
            1, 10, mode="shutdown"
        ).replace("owner_and_head_ns=2", "owner_and_head_ns=1")
        with self.assertRaisesRegex(gate.GateError, "owner/head substage"):
            gate.parse_live_log_text(invalid_owner_substages, 10)

    def test_replay_reconciliation_treats_omitted_zero_fields_as_zero(self) -> None:
        self.assertEqual(
            gate.validate_replay_document(replay_document(), 10)["general"][
                "Recorded Samples"
            ],
            10,
        )
        bad = replay_document()
        bad["datapoint_policy_totals"]["Observed"] = 11
        with self.assertRaisesRegex(gate.GateError, "reconcile"):
            gate.validate_replay_document(bad, 10)

    def test_storage_gate_distinguishes_head_writes_from_physical_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "storage.json"
            replay = root / "replay.json"
            log = root / "ingester.log"
            report.write_text(
                json.dumps(storage_verifier_document(8)), encoding="utf-8"
            )
            replay.write_text(json.dumps(replay_document()), encoding="utf-8")
            log.write_text(
                "Head window written start_ms=0 end_ms=10 datapoints=10 "
                "series=1 record_chunks=1 record_profile_samples=8 "
                "dropped_histogram_series=0 "
                "dropped_exponential_histogram_series=0 "
                "dropped_summary_series=0\n",
                encoding="utf-8",
            )
            result = gate.validate_storage_verifier(report, replay, log)
            self.assertEqual(result["samples"], 8)
            self.assertEqual(result["recorded_head_writes"], 10)
            self.assertEqual(result["recorded_writes_minus_physical_rows"], 2)
            self.assertTrue(result["writer_to_verifier_counts_reconciled"])
            self.assertFalse(result["capture_level_physical_sample_golden_gated"])

    def test_storage_gate_rejects_more_physical_rows_than_head_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "storage.json"
            replay = root / "replay.json"
            log = root / "ingester.log"
            report.write_text(
                json.dumps(storage_verifier_document(11)), encoding="utf-8"
            )
            replay.write_text(json.dumps(replay_document()), encoding="utf-8")
            log.write_text(
                "Head window written start_ms=0 end_ms=10 datapoints=10 "
                "series=1 record_chunks=1 record_profile_samples=11 "
                "dropped_histogram_series=0 "
                "dropped_exponential_histogram_series=0 "
                "dropped_summary_series=0\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "physical sample"):
                gate.validate_storage_verifier(report, replay, log)

    def test_storage_gate_accepts_exhaustive_live_handoff_without_writer_logs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "storage.json"
            replay = root / "replay.json"
            log = root / "ingester.log"
            report.write_text(
                json.dumps(storage_verifier_document(8)), encoding="utf-8"
            )
            replay.write_text(json.dumps(replay_document()), encoding="utf-8")
            log.write_text("live publisher sealed one final segment\n", encoding="utf-8")

            result = gate.validate_storage_verifier(
                report,
                replay,
                log,
                require_writer_reconciliation=False,
            )
            self.assertEqual(result["samples"], 8)
            self.assertEqual(result["recorded_head_writes"], 10)
            self.assertEqual(result["recorded_writes_minus_physical_rows"], 2)
            self.assertFalse(result["writer_to_verifier_counts_reconciled"])

    def test_parallel_batch_executes_concurrently(self) -> None:
        lock = threading.Lock()
        active = 0
        maximum = 0

        def requester(_query: dict[str, str]) -> dict:
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.03)
            with lock:
                active -= 1
            return {"ok": True}

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = gate.execute_parallel_batch(
                {"name": "q"}, 2, requester, executor
            )
        self.assertEqual(results, [{"ok": True}, {"ok": True}])
        self.assertEqual(maximum, 2)

    def test_run_set_gate_checks_exact_counters_trees_logs_and_client(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = root / "runs"
            configs = root / "configs"
            runs.mkdir()
            configs.mkdir()
            workload_path = root / "workload.json"
            workload_path.write_text(json.dumps(workload()), encoding="utf-8")
            replay = replay_document()
            live_text = "\n".join(
                (
                    publication_line(1, 5),
                    publication_line(2, 10, mode="shutdown"),
                    'DEBUG chronoxide_live_metrics: event="message_boundary" outcome="success" ingestion_pause_ns=1',
                )
            )
            for variant in ("D", "P", "Q"):
                run = runs / variant
                run.mkdir()
                (run / "ingester.exit-status").write_text("0\n", encoding="ascii")
                (run / "rss-monitor.exit-status").write_text("0\n", encoding="ascii")
                (run / "replay-correctness.json").write_text(
                    json.dumps(replay), encoding="utf-8"
                )
                (run / "segments.sha256").write_bytes(b"same tree\n")
                (run / "corpus-summary.json").write_text(
                    json.dumps({"file_count": 1, "size_bytes": 7, "manifest_sha256": "x"}),
                    encoding="utf-8",
                )
                (run / "replay.time.json").write_text(
                    json.dumps({"exit_status": 0}), encoding="utf-8"
                )
                (run / "rss-summary.json").write_text(
                    json.dumps({"samples": 1, "aggregate_vm_swap_kib": 0}),
                    encoding="utf-8",
                )
                (configs / f"{variant}.toml").write_text(
                    f"[api]\nenabled = {'false' if variant == 'D' else 'true'}\n"
                    "[ingestion.segment_writer]\n"
                    f"segments_dir = \"/tmp/{variant}\"\n",
                    encoding="utf-8",
                )
                if variant != "D":
                    (run / "ingester.log").write_text(live_text, encoding="utf-8")
                    (run / "live-log-summary.json").write_text(
                        json.dumps(gate.parse_live_log_text(live_text, 10)),
                        encoding="utf-8",
                    )
                if variant == "Q":
                    (run / "client.exit-status").write_text("0\n", encoding="ascii")
            records = [record(1, 5), record(1, 5), record(2, 10), record(2, 10)]
            (runs / "Q" / "client-records.jsonl").write_text(
                "".join(json.dumps(item) + "\n" for item in records),
                encoding="utf-8",
            )
            (runs / "Q" / "client-summary.json").write_text(
                json.dumps(gate.validate_client_records(records, workload())),
                encoding="utf-8",
            )
            summary = gate.gate_run_set(runs, workload_path, 10, False)
            self.assertTrue(summary["storage_trees_equal"])
            (runs / "Q" / "client.exit-status").write_text("1\n", encoding="ascii")
            with self.assertRaisesRegex(gate.GateError, "Q client"):
                gate.gate_run_set(runs, workload_path, 10, False)

    def test_shutdown_ab_gate_accepts_counterbalanced_exact_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            write_shutdown_ab_fixture(
                roots[0],
                candidate=False,
                ingester_payload=b"baseline",
                shutdown_scale=10,
            )
            write_shutdown_ab_fixture(
                roots[1],
                candidate=True,
                ingester_payload=b"candidate",
                shutdown_scale=1,
            )
            write_shutdown_ab_fixture(
                roots[2],
                candidate=True,
                ingester_payload=b"candidate",
                shutdown_scale=2,
            )
            write_shutdown_ab_fixture(
                roots[3],
                candidate=False,
                ingester_payload=b"baseline",
                shutdown_scale=11,
            )

            result = gate.gate_shutdown_ab(roots)

            self.assertEqual(result["schema"], gate.SHUTDOWN_AB_SCHEMA)
            self.assertTrue(result["complete"])
            self.assertEqual(result["order"], ["A1", "B1", "B2", "A2"])
            self.assertTrue(
                result["acceptance"]["candidate_shutdown_below_both_baselines"]
            )
            self.assertEqual(result["means"]["A"]["shutdown_publication_ns"], 105)
            self.assertEqual(result["means"]["B"]["shutdown_publication_ns"], 15)
            self.assertEqual(
                result["arms"]["B1"]["base_scale"]["base_sample_keys"], 100
            )

    def test_shutdown_ab_gate_rejects_correctness_or_fast_path_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            for index, root in enumerate(roots):
                candidate = index in (1, 2)
                write_shutdown_ab_fixture(
                    root,
                    candidate=candidate,
                    ingester_payload=b"candidate" if candidate else b"baseline",
                    shutdown_scale=1 if candidate else 10,
                )
            storage_path = (
                roots[2] / "validation" / "storage-verify-gate.json"
            )
            storage = json.loads(storage_path.read_text(encoding="utf-8"))
            storage["decoded_semantic_fingerprint"] = "e" * 64
            storage_path.write_text(json.dumps(storage), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "storage fingerprints"):
                gate.gate_shutdown_ab(roots)

        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            for index, root in enumerate(roots):
                candidate = index in (1, 2)
                write_shutdown_ab_fixture(
                    root,
                    candidate=candidate,
                    ingester_payload=b"candidate" if candidate else b"baseline",
                    shutdown_scale=1 if candidate else 10,
                )
            summary_path = roots[1] / "runs" / "P" / "live-log-summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["shutdown_publication"]["final_empty_fast_path"] = False
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "differs from raw log"):
                gate.gate_shutdown_ab(roots)

    def test_shutdown_ab_gate_rejects_regressions_and_duplicate_roots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            write_shutdown_ab_fixture(
                roots[0],
                candidate=False,
                ingester_payload=b"baseline",
                shutdown_scale=10,
            )
            write_shutdown_ab_fixture(
                roots[1],
                candidate=True,
                ingester_payload=b"candidate",
                shutdown_scale=11,
            )
            write_shutdown_ab_fixture(
                roots[2],
                candidate=True,
                ingester_payload=b"candidate",
                shutdown_scale=1,
            )
            write_shutdown_ab_fixture(
                roots[3],
                candidate=False,
                ingester_payload=b"baseline",
                shutdown_scale=10,
            )
            with self.assertRaisesRegex(gate.GateError, "not below both baseline"):
                gate.gate_shutdown_ab(roots)
            with self.assertRaisesRegex(gate.GateError, "must be distinct"):
                gate.gate_shutdown_ab([roots[0], roots[1], roots[1], roots[3]])

    def test_shutdown_ab_gate_rehashes_frozen_binaries_and_segments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            for index, root in enumerate(roots):
                candidate = index in (1, 2)
                write_shutdown_ab_fixture(
                    root,
                    candidate=candidate,
                    ingester_payload=b"candidate" if candidate else b"baseline",
                    shutdown_scale=1 if candidate else 10,
                )
            (
                roots[1]
                / "metadata"
                / "binaries"
                / "chronoxide-ingester"
            ).write_bytes(b"changed after manifest")
            with self.assertRaisesRegex(gate.GateError, "hash differs"):
                gate.gate_shutdown_ab(roots)

        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            for index, root in enumerate(roots):
                candidate = index in (1, 2)
                write_shutdown_ab_fixture(
                    root,
                    candidate=candidate,
                    ingester_payload=b"candidate" if candidate else b"baseline",
                    shutdown_scale=1 if candidate else 10,
                )
            (roots[1] / "runs" / "P" / "segments" / "payload.bin").write_bytes(
                b"changed after manifest"
            )
            with self.assertRaisesRegex(gate.GateError, "segment file hash"):
                gate.gate_shutdown_ab(roots)

    def test_shutdown_ab_gate_enforces_boundary_and_rss_limits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            for index, root in enumerate(roots):
                candidate = index in (1, 2)
                write_shutdown_ab_fixture(
                    root,
                    candidate=candidate,
                    ingester_payload=b"candidate" if candidate else b"baseline",
                    shutdown_scale=1 if candidate else 10,
                    boundary_scale=12 if index == 1 else 10,
                )
            with self.assertRaisesRegex(gate.GateError, "more than 10%"):
                gate.gate_shutdown_ab(roots)

        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            for index, root in enumerate(roots):
                candidate = index in (1, 2)
                write_shutdown_ab_fixture(
                    root,
                    candidate=candidate,
                    ingester_payload=b"candidate" if candidate else b"baseline",
                    shutdown_scale=1 if candidate else 10,
                    peak_rss_kib=1_100 if index == 1 else 1_000,
                )
            with self.assertRaisesRegex(gate.GateError, "more than 5%"):
                gate.gate_shutdown_ab(roots)

    def test_shutdown_ab_gate_requires_one_identical_optional_api(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            for index, root in enumerate(roots):
                candidate = index in (1, 2)
                write_shutdown_ab_fixture(
                    root,
                    candidate=candidate,
                    ingester_payload=b"candidate" if candidate else b"baseline",
                    shutdown_scale=1 if candidate else 10,
                    api_payload=b"same api",
                )
            result = gate.gate_shutdown_ab(roots)
            self.assertEqual(
                result["binary_hashes"]["api_sha256"],
                hashlib.sha256(b"same api").hexdigest(),
            )

        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            roots = [parent / label for label in ("A1", "B1", "B2", "A2")]
            for index, root in enumerate(roots):
                candidate = index in (1, 2)
                write_shutdown_ab_fixture(
                    root,
                    candidate=candidate,
                    ingester_payload=b"candidate" if candidate else b"baseline",
                    shutdown_scale=1 if candidate else 10,
                    api_payload=None if index == 2 else b"same api",
                )
            with self.assertRaisesRegex(gate.GateError, "presence differs"):
                gate.gate_shutdown_ab(roots)

    def test_publication_scale_gate_enforces_the_mandatory_limits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            result = gate_publication_scale(
                root, 250_000, expected
            )
            self.assertEqual(result["schema"], gate.PUBLICATION_SCALE_SCHEMA)
            self.assertTrue(result["complete"])
            self.assertEqual(result["gate_mode"], "mandatory-250k")
            self.assertTrue(result["acceptance"]["boundary_p95"])
            self.assertEqual(
                result["limits_ns"]["shutdown_post_commit_ns"],
                30_000_000_000,
            )
            validation = result["correctness"]["validation"]
            self.assertEqual(
                validation["expected_independent_readback_queries"], 30
            )
            self.assertEqual(validation["independent_readback_queries"], 30)
            self.assertEqual(
                result["validator"]["producer_frozen_entrypoint_sha256"],
                hashlib.sha256(b"producer-frozen gate").hexdigest(),
            )
            self.assertEqual(
                result["result_artifacts"]["complete_marker"],
                {
                    "size_bytes": 0,
                    "sha256": hashlib.sha256(b"").hexdigest(),
                },
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(
                root,
                messages=250_000,
                boundary_scale=1_100_000_000,
            )
            with self.assertRaisesRegex(gate.GateError, "boundary_p95"):
                gate_publication_scale(root, 250_000, expected)

    def test_publication_scale_gate_binds_selected_prefix_to_capacity(
        self,
    ) -> None:
        cases = {
            "missing-capacity": (
                "capture-capacity.json",
                "missing",
                "capture-capacity document.*missing",
            ),
            "identity-mismatch": (
                "capture-capacity.json",
                "identity",
                "selected input identity differs",
            ),
            "capacity-count-mismatch": (
                "capture-capacity.json",
                "capacity-count",
                "capacity differs",
            ),
            "malformed-capacity-shape": (
                "capture-capacity.json",
                "extra",
                "capture-capacity document has an unexpected shape",
            ),
            "wrong-selected-cut": (
                "validated-inputs.json",
                "selected-count",
                "selected input prefix differs",
            ),
            "wrong-selected-schema": (
                "validated-inputs.json",
                "schema",
                "selected-input-prefix document has an unexpected shape",
            ),
            "extra-selected-field": (
                "validated-inputs.json",
                "extra",
                "selected-input-prefix document has an unexpected shape",
            ),
            "missing-selected-field": (
                "validated-inputs.json",
                "missing-field",
                "selected-input-prefix document has an unexpected shape",
            ),
        }
        for label, (name, mutation, pattern) in cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "scale"
                expected = write_scale_fixture(root, messages=250_000)
                path = root / "metadata" / name
                if mutation == "missing":
                    path.unlink()
                else:
                    value = json.loads(path.read_text(encoding="utf-8"))
                    if mutation == "identity":
                        value["capture"] = "/different-capture"
                    elif mutation == "capacity-count":
                        value["stop_after_messages"] = 999_999
                    elif mutation == "selected-count":
                        value["stop_after_messages"] = 125_000
                    elif mutation == "schema":
                        value["schema"] = (
                            "chronoxide/live-query-selected-input-prefix/v2"
                        )
                    elif mutation == "extra":
                        value["unbound"] = True
                    elif mutation == "missing-field":
                        del value["schema"]
                    else:
                        self.fail(f"unknown test mutation {mutation}")
                    path.write_text(json.dumps(value), encoding="utf-8")
                write_result_artifact_manifest(root)

                with self.assertRaisesRegex(gate.GateError, pattern):
                    gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_rejects_mid_run_process_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)

            def add_external_clang(values) -> None:
                values[3]["processes"].append(
                    {
                        "pid": 200,
                        "ppid": 1,
                        "pgrp": 200,
                        "session": 200,
                        "starttime_ticks": 99,
                        "state": "R",
                        "comm": "clang++.real",
                        "argv0": "/toolchain/bin/clang++.real",
                    }
                )

            rewrite_host_process_evidence(root / "runs" / "P", add_external_clang)
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(
                gate.GateError, "continuous host monitor observed conflicting"
            ):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_allows_only_the_measured_session(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)

            def add_measured_perf(values) -> None:
                values[3]["processes"].append(
                    {
                        "pid": 201,
                        "ppid": 100,
                        "pgrp": 100,
                        "session": 100,
                        "starttime_ticks": 100,
                        "state": "S",
                        "comm": "perf",
                        "argv0": "/usr/bin/perf",
                    }
                )

            rewrite_host_process_evidence(root / "runs" / "P", add_measured_perf)
            write_result_artifact_manifest(root)
            result = gate_publication_scale(root, 250_000, expected)
            self.assertEqual(
                result["scale_context"]["host_evidence"][
                    "continuous_process_monitor"
                ]["conflict_observations"],
                0,
            )

    def test_mandatory_scale_gate_rejects_incomplete_monitor_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            evidence = root / "runs" / "P" / "host-process-samples.jsonl"
            lines = evidence.read_bytes().splitlines(keepends=True)
            evidence.write_bytes(b"".join(lines[:-1]))
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "incomplete"):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            status = (
                root
                / "runs"
                / "P"
                / "host-process-monitor.exit-status"
            )
            status.write_text("1\n", encoding="ascii")
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "exit status"):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_rejects_monitor_coverage_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)

            def create_gap(values) -> None:
                starts = (
                    1_000_000_000,
                    1_100_000_000,
                    1_200_000_000,
                    1_800_000_000,
                    1_900_000_000,
                    2_250_000_000,
                )
                samples = [
                    value for value in values if value.get("kind") == "sample"
                ]
                for value, started in zip(samples, starts, strict=True):
                    value["scan_started_boottime_ns"] = started
                    value["scan_ended_boottime_ns"] = started + 10_000_000

            rewrite_host_process_evidence(root / "runs" / "P", create_gap)
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "coverage gap"):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            end = root / "runs" / "P" / "host-process-end.json"
            value = json.loads(end.read_text(encoding="utf-8"))
            value["recorded_boottime_ns"] = 3_000_000_000
            end.write_text(json.dumps(value), encoding="utf-8")
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "do not enclose"):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_rejects_regressed_sample_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)

            def regress_time(values) -> None:
                samples = [
                    value for value in values if value.get("kind") == "sample"
                ]
                samples[3]["scan_started_boottime_ns"] = 1_400_000_000
                samples[3]["scan_ended_boottime_ns"] = 1_410_000_000

            rewrite_host_process_evidence(root / "runs" / "P", regress_time)
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(
                gate.GateError, "timestamps overlap or regress"
            ):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_bounds_vanished_pid_uncertainty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)

            def add_unclassified_pids(values) -> None:
                samples = [
                    value for value in values if value.get("kind") == "sample"
                ]
                samples[2]["vanished_pid_count"] = 1_000_000

            rewrite_host_process_evidence(
                root / "runs" / "P", add_unclassified_pids
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(
                gate.GateError, "uncertainty bound"
            ):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_requires_leader_interior_continuity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)

            def remove_interior_leader(values) -> None:
                samples = [
                    value for value in values if value.get("kind") == "sample"
                ]
                samples[2]["processes"] = [
                    process
                    for process in samples[2]["processes"]
                    if process["pid"] != 100
                ]

            rewrite_host_process_evidence(
                root / "runs" / "P", remove_interior_leader
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(
                gate.GateError, "disappeared and reappeared|missing inside"
            ):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_allows_final_one_way_leader_exit(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)

            def remove_tail_leader(values) -> None:
                samples = [
                    value for value in values if value.get("kind") == "sample"
                ]
                samples[4]["processes"] = [
                    process
                    for process in samples[4]["processes"]
                    if process["pid"] != 100
                ]

            rewrite_host_process_evidence(
                root / "runs" / "P", remove_tail_leader
            )
            write_result_artifact_manifest(root)
            result = gate_publication_scale(root, 250_000, expected)
            self.assertTrue(result["complete"])

    def test_mandatory_scale_gate_requires_pid1_in_every_scan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)

            def remove_pid1(values) -> None:
                samples = [
                    value for value in values if value.get("kind") == "sample"
                ]
                samples[2]["processes"] = [
                    process
                    for process in samples[2]["processes"]
                    if process["pid"] != 1
                ]

            rewrite_host_process_evidence(root / "runs" / "P", remove_pid1)
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "visible PID 1"):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_requires_post_stop_scan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            evidence = (
                root / "runs" / "P" / "host-process-samples.jsonl"
            )
            values = [
                json.loads(line)
                for line in evidence.read_text(encoding="ascii").splitlines()
            ]
            values[-1]["stop_observed_boottime_ns"] = 2_300_000_000
            evidence.write_bytes(
                b"".join(gate._compact_json_line(value) for value in values)
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "post-stop|after.*stop"):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_binds_monitor_time_to_raw_stream(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            monitor_time = (
                root / "runs" / "P" / "host-process-monitor.time.txt"
            )
            monitor_time.write_text(
                gnu_time_text("0:01.15"), encoding="utf-8"
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "raw sample stream"):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_binds_rss_to_measured_leader(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            rss_path = root / "runs" / "P" / "rss-summary.json"
            rss = json.loads(rss_path.read_text(encoding="utf-8"))
            rss["root_pid"] = 999
            rss_path.write_text(json.dumps(rss), encoding="utf-8")
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(
                gate.GateError, "does not reconcile with the measured leader"
            ):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_requires_full_rss_duration_coverage(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            run = root / "runs" / "P"
            raw_path = run / "rss-samples.tsv"
            lines = raw_path.read_text(encoding="utf-8").splitlines()
            raw_path.write_text("\n".join(lines[:2]) + "\n", encoding="utf-8")
            summary_path = run / "rss-summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["samples"] = 1
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(
                gate.GateError, "does not cover the measured duration"
            ):
                gate_publication_scale(root, 250_000, expected)

    def test_process_record_decodes_non_utf8_proc_fields_safely(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            proc_root = Path(directory)
            process = proc_root / "123"
            process.mkdir()
            tail = ["S", "1", "123", "123", *(["0"] * 15), "42"]
            (process / "stat").write_bytes(
                b"123 (bad"
                + bytes((0xFF,))
                + b"name) "
                + " ".join(tail).encode("ascii")
            )
            (process / "cmdline").write_bytes(
                b"/bin/tool" + bytes((0xFF, 0)) + b"--arg\0"
            )
            record = gate._proc_process_record(123, proc_root)
            self.assertIsNotNone(record)
            assert record is not None
            self.assertIn("\ufffd", record["comm"])
            self.assertIn("\ufffd", record["argv0"])
            self.assertEqual(record["starttime_ticks"], 42)

    def test_monitor_records_one_full_scan_after_observing_stop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stop = root / "stop"
            ready = root / "ready.json"
            output = root / "samples.jsonl"
            leader = {
                "pid": 100,
                "ppid": 1,
                "pgrp": 100,
                "session": 100,
                "starttime_ticks": 42,
                "state": "T",
                "comm": "sh",
                "argv0": "/bin/sh",
            }

            def sample(sequence: int) -> dict:
                started = 1_000 + sequence * 100
                if sequence == 0:
                    stop.touch()
                return {
                    "kind": "sample",
                    "sequence": sequence,
                    "scan_started_boottime_ns": started,
                    "scan_ended_boottime_ns": started + 10,
                    "listed_pid_count": 1,
                    "vanished_pid_count": 0,
                    "process_count": 1,
                    "processes": [leader],
                }

            visibility = {
                "hidepid": 0,
                "nspid_depth": 1,
                "pid_namespace": "pid:[1]",
                "pid1_stat_visible": True,
                "pid1_starttime_ticks": 1,
            }
            clock = iter((900, 1_050))
            with (
                mock.patch.object(
                    gate, "_proc_process_record", return_value=leader
                ),
                mock.patch.object(
                    gate,
                    "_boot_id",
                    return_value="11111111-2222-3333-4444-555555555555",
                ),
                mock.patch.object(
                    gate,
                    "_proc_visibility_contract",
                    return_value=visibility,
                ),
                mock.patch.object(
                    gate, "_host_process_sample", side_effect=sample
                ),
                mock.patch.object(
                    gate, "_clock_boottime_ns", side_effect=clock
                ),
            ):
                result = gate.monitor_host_processes(
                    expected_session_id=100,
                    interval_ms=250,
                    abort_on_conflict=True,
                    stop_file=stop,
                    ready_file=ready,
                    output=output,
                )
            values = [
                json.loads(line)
                for line in output.read_text(encoding="ascii").splitlines()
            ]
            samples = [
                value for value in values if value.get("kind") == "sample"
            ]
            footer = values[-1]
            self.assertEqual(len(samples), 2)
            self.assertEqual(result["samples"], 2)
            self.assertGreaterEqual(
                samples[-1]["scan_started_boottime_ns"],
                footer["stop_observed_boottime_ns"],
            )

    def test_monitor_aborts_immediately_on_external_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            leader = {
                "pid": 100,
                "ppid": 1,
                "pgrp": 100,
                "session": 100,
                "starttime_ticks": 42,
                "state": "T",
                "comm": "sh",
                "argv0": "/bin/sh",
            }
            cargo = {
                "pid": 200,
                "ppid": 1,
                "pgrp": 200,
                "session": 200,
                "starttime_ticks": 99,
                "state": "R",
                "comm": "cargo",
                "argv0": "/toolchain/bin/cargo",
            }
            sample = {
                "kind": "sample",
                "sequence": 0,
                "scan_started_boottime_ns": 1_000,
                "scan_ended_boottime_ns": 1_010,
                "listed_pid_count": 2,
                "vanished_pid_count": 0,
                "process_count": 2,
                "processes": [leader, cargo],
            }
            visibility = {
                "hidepid": 0,
                "nspid_depth": 1,
                "pid_namespace": "pid:[1]",
                "pid1_stat_visible": True,
                "pid1_starttime_ticks": 1,
            }
            with (
                mock.patch.object(
                    gate, "_proc_process_record", return_value=leader
                ),
                mock.patch.object(
                    gate,
                    "_boot_id",
                    return_value="11111111-2222-3333-4444-555555555555",
                ),
                mock.patch.object(
                    gate,
                    "_proc_visibility_contract",
                    return_value=visibility,
                ),
                mock.patch.object(
                    gate, "_host_process_sample", return_value=sample
                ),
                mock.patch.object(
                    gate, "_clock_boottime_ns", return_value=900
                ),
            ):
                with self.assertRaisesRegex(
                    gate.GateError, "external conflict.*cargo"
                ):
                    gate.monitor_host_processes(
                        expected_session_id=100,
                        interval_ms=250,
                        abort_on_conflict=True,
                        stop_file=root / "stop",
                        ready_file=root / "ready.json",
                        output=root / "samples.jsonl",
                    )
            self.assertFalse((root / "ready.json").exists())
            values = [
                json.loads(line)
                for line in (root / "samples.jsonl")
                .read_text(encoding="ascii")
                .splitlines()
            ]
            self.assertEqual(
                [value["kind"] for value in values],
                ["header", "sample"],
            )

    def test_monitor_noisy_policy_records_external_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stop = root / "stop"
            leader = {
                "pid": 100,
                "ppid": 1,
                "pgrp": 100,
                "session": 100,
                "starttime_ticks": 42,
                "state": "T",
                "comm": "sh",
                "argv0": "/bin/sh",
            }
            cargo = {
                "pid": 200,
                "ppid": 1,
                "pgrp": 200,
                "session": 200,
                "starttime_ticks": 99,
                "state": "R",
                "comm": "cargo",
                "argv0": "/toolchain/bin/cargo",
            }

            def sample(sequence: int) -> dict:
                if sequence == 0:
                    stop.touch()
                started = 1_000 + sequence * 100
                return {
                    "kind": "sample",
                    "sequence": sequence,
                    "scan_started_boottime_ns": started,
                    "scan_ended_boottime_ns": started + 10,
                    "listed_pid_count": 2,
                    "vanished_pid_count": 0,
                    "process_count": 2,
                    "processes": [leader, cargo],
                }

            visibility = {
                "hidepid": 0,
                "nspid_depth": 1,
                "pid_namespace": "pid:[1]",
                "pid1_stat_visible": True,
                "pid1_starttime_ticks": 1,
            }
            with (
                mock.patch.object(
                    gate, "_proc_process_record", return_value=leader
                ),
                mock.patch.object(
                    gate,
                    "_boot_id",
                    return_value="11111111-2222-3333-4444-555555555555",
                ),
                mock.patch.object(
                    gate,
                    "_proc_visibility_contract",
                    return_value=visibility,
                ),
                mock.patch.object(
                    gate, "_host_process_sample", side_effect=sample
                ),
                mock.patch.object(
                    gate,
                    "_clock_boottime_ns",
                    side_effect=iter((900, 1_050)),
                ),
            ):
                result = gate.monitor_host_processes(
                    expected_session_id=100,
                    interval_ms=250,
                    abort_on_conflict=False,
                    stop_file=stop,
                    ready_file=root / "ready.json",
                    output=root / "samples.jsonl",
                )
            self.assertEqual(result["samples"], 2)
            self.assertTrue((root / "ready.json").exists())

    def test_host_process_classifier_uses_comm_and_argv0(self) -> None:
        base = {
            "comm": "worker",
            "argv0": "/android/prebuilts/clang++.real",
        }
        self.assertTrue(gate._is_conflicting_scale_process(base))
        self.assertTrue(
            gate._is_conflicting_scale_process(
                {"comm": "soong_ui", "argv0": None}
            )
        )
        self.assertTrue(
            gate._is_conflicting_scale_process(
                {"comm": "chronoxide-inge", "argv0": None}
            )
        )
        self.assertTrue(
            gate._is_conflicting_scale_process(
                {"comm": "cc1plus", "argv0": None}
            )
        )
        self.assertTrue(
            gate._is_conflicting_scale_process(
                {"comm": "mold", "argv0": None}
            )
        )
        for name in ("memgraph", "qemu-aarch64"):
            with self.subTest(name=name):
                self.assertTrue(
                    gate._is_conflicting_scale_process(
                        {"comm": name, "argv0": None}
                    )
                )
        self.assertTrue(
            gate._is_conflicting_scale_process(
                {
                    "comm": "x86_64-linux-gn",
                    "argv0": "/usr/bin/x86_64-linux-gnu-gcc-14",
                }
            )
        )
        self.assertFalse(
            gate._is_conflicting_scale_process(
                {"comm": "kworker/0:1", "argv0": None}
            )
        )
        self.assertEqual(
            gate._scale_process_classifier_contract()["compiler_pattern"],
            gate.SCALE_CONFLICTING_COMPILER_PATTERN,
        )

    def test_gnu_elapsed_parser_is_exact_and_fail_closed(self) -> None:
        self.assertEqual(gate._gnu_elapsed_ns("6:58.21"), 418_210_000_000)
        self.assertEqual(
            gate._gnu_elapsed_ns("1:02:03.004"),
            3_723_004_000_000,
        )
        with self.assertRaises(gate.GateError):
            gate._gnu_elapsed_ns("1:99")
        with self.assertRaises(gate.GateError):
            gate._parse_gnu_time_text(
                gnu_time_text().replace(
                    "User time (seconds): 1.0",
                    "User time (seconds): nan",
                ),
                "non-finite test",
            )

    def test_publication_scale_gate_uses_only_smoke_limits_at_125k(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=125_000)
            summary_path = root / "runs" / "P" / "live-log-summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["shutdown_publication"]["timings_ns"][
                "sample_root_ns"
            ] = 20_000_000_000
            summary["shutdown_publication"]["timings_ns"][
                "catalog_ns"
            ] = 20_000_000_000
            # The gate reparses the raw log; modify both sources before resealing
            # the exact result-artifact manifest.
            log_path = root / "runs" / "P" / "ingester.log"
            text = log_path.read_text(encoding="utf-8")
            final = text.rsplit("\n", 1)[-1]
            final = re.sub(r"sample_root_ns=1\b", "sample_root_ns=20000000000", final)
            final = re.sub(r"catalog_ns=1\b", "catalog_ns=20000000000", final)
            log_path.write_text(
                text.rsplit("\n", 1)[0] + "\n" + final, encoding="utf-8"
            )
            summary_path.write_text(
                json.dumps(
                    gate.parse_live_log_text(
                        log_path.read_text(encoding="utf-8"), 125_000
                    )
                ),
                encoding="utf-8",
            )
            write_result_artifact_manifest(root)
            result = gate_publication_scale(root, 125_000, expected)
            self.assertEqual(result["gate_mode"], "smoke-125k")
            self.assertNotIn(
                "shutdown_sample_catalog_ns", result["limits_ns"]
            )
            self.assertEqual(
                result["acceptance"]["shutdown_post_commit"],
                "deferred-to-250k",
            )

    def test_publication_scale_gate_rejects_artifact_and_context_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root)
            (root / "validation" / "readbacks.md").write_text(
                "changed after sealing\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(gate.GateError, "result artifact"):
                gate_publication_scale(root, 125_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root)
            expected["binary_hashes"]["chronoxide-api"] = "f" * 64
            with self.assertRaisesRegex(gate.GateError, "accepted hashes"):
                gate_publication_scale(root, 125_000, expected)

    def test_publication_scale_gate_rejects_wrong_count_and_too_few_boundaries(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root)
            with self.assertRaisesRegex(gate.GateError, "exactly 125000 or 250000"):
                gate_publication_scale(root, 10, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root)
            log_path = root / "runs" / "P" / "ingester.log"
            lines = log_path.read_text(encoding="utf-8").splitlines()
            log_path.write_text(
                "\n".join(lines[-3:]) + "\n", encoding="utf-8"
            )
            summary_path = root / "runs" / "P" / "live-log-summary.json"
            summary_path.write_text(
                json.dumps(
                    gate.parse_live_log_text(
                        log_path.read_text(encoding="utf-8"), 125_000
                    )
                ),
                encoding="utf-8",
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "at least 10"):
                gate_publication_scale(root, 125_000, expected)

    def test_mandatory_scale_gate_requires_advancing_and_late_boundary_cuts(
        self,
    ) -> None:
        def rewrite_boundary_cuts(
            root: Path, messages: int, cut_for_generation
        ) -> None:
            log_path = root / "runs" / "P" / "ingester.log"
            lines = []
            for line in log_path.read_text(encoding="utf-8").splitlines():
                if 'mode="boundary"' in line:
                    generation = int(
                        re.search(r"\bgeneration=(\d+)\b", line).group(1)
                    )
                    line = re.sub(
                        r"\bvisible_message_sequence=\d+\b",
                        "visible_message_sequence="
                        f"{cut_for_generation(generation)}",
                        line,
                    )
                lines.append(line)
            text = "\n".join(lines)
            log_path.write_text(text, encoding="utf-8")
            (root / "runs" / "P" / "live-log-summary.json").write_text(
                json.dumps(gate.parse_live_log_text(text, messages)),
                encoding="utf-8",
            )
            write_result_artifact_manifest(root)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            rewrite_boundary_cuts(root, 250_000, lambda _generation: 1)
            with self.assertRaisesRegex(gate.GateError, "strictly advance"):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            rewrite_boundary_cuts(root, 250_000, lambda generation: generation)
            with self.assertRaisesRegex(gate.GateError, "90% late-cut"):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=125_000)
            rewrite_boundary_cuts(root, 125_000, lambda generation: generation)
            result = gate_publication_scale(root, 125_000, expected)
            self.assertEqual(
                result["acceptance"]["late_cut_min_percent"],
                "deferred-to-250k",
            )

    def test_mandatory_scale_gate_requires_quiet_bound_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            settings = root / "metadata" / "settings.txt"
            settings.write_text(
                settings.read_text(encoding="utf-8").replace(
                    "allow_noisy_host=0", "allow_noisy_host=1"
                ),
                encoding="utf-8",
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "quiet host"):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            (root / "runs" / "P" / "processes-before.txt").write_text(
                "42 1 100.0 1024 R java java -jar build.jar\n",
                encoding="utf-8",
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "conflicting processes"):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            pressure = root / "runs" / "P" / "pressure-before.txt"
            pressure.write_text(
                pressure.read_text(encoding="utf-8").replace(
                    "/proc/pressure/io\n"
                    "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
                    "full avg10=0.00",
                    "/proc/pressure/io\n"
                    "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
                    "full avg10=6.00",
                ),
                encoding="utf-8",
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "exceeds quiet-host"):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            pressure = root / "runs" / "P" / "pressure-after.txt"
            pressure.write_text(
                pressure.read_text(encoding="utf-8").replace(
                    "/proc/pressure/io\n"
                    "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
                    "full avg10=0.00",
                    "/proc/pressure/io\n"
                    "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
                    "full avg10=6.00",
                ),
                encoding="utf-8",
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(
                gate.GateError, "after io_psi_avg10=.*exceeds quiet-host"
            ):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            settings = root / "metadata" / "settings.txt"
            settings.write_text(
                settings.read_text(encoding="utf-8").replace(
                    "max_io_psi_avg10=5.0", "max_io_psi_avg10=999.0"
                ),
                encoding="utf-8",
            )
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "thresholds differ"):
                gate_publication_scale(root, 250_000, expected)

    def test_mandatory_scale_gate_binds_summaries_and_disclosures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            manifest = root / "metadata" / "result-artifacts.sha256"
            manifest.write_text(
                "".join(
                    line
                    for line in manifest.read_text(
                        encoding="ascii"
                    ).splitlines(keepends=True)
                    if not line.endswith(
                        ("  run-plan.tsv\n", "  run-summary.tsv\n")
                    )
                ),
                encoding="ascii",
            )
            with self.assertRaisesRegex(gate.GateError, "does not bind"):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            gap = root / "metadata" / "PHYSICAL_SAMPLE_COUNT_COVERAGE_GAP"
            gap.write_text("writer counts reconcile exactly\n", encoding="utf-8")
            write_result_artifact_manifest(root)
            with self.assertRaisesRegex(gate.GateError, "stale or misleading"):
                gate_publication_scale(root, 250_000, expected)

    def test_scale_gate_enforces_exact_prefix_readback_cardinality(self) -> None:
        mutations = {
            125_000: (25, 27),
            250_000: (26, 29, 31),
        }
        for messages, observed_counts in mutations.items():
            expected_count = gate.SCALE_EXPECTED_READBACK_QUERIES[messages]
            for observed_count in observed_counts:
                with self.subTest(
                    messages=messages, observed_count=observed_count
                ), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory) / "scale"
                    expected = write_scale_fixture(root, messages=messages)
                    raw = root / "validation" / "readbacks.md"
                    text = raw.read_text(encoding="utf-8")
                    for label in (
                        "Checked Queries",
                        "Expected Readback Queries",
                        "Executed Readback Queries",
                    ):
                        text = text.replace(
                            f"| {label} | {expected_count} |",
                            f"| {label} | {observed_count} |",
                        )
                    raw.write_text(text, encoding="utf-8")
                    normalized = gate.validate_readbacks(raw)
                    (
                        root / "validation" / "readbacks-gate.json"
                    ).write_text(json.dumps(normalized), encoding="utf-8")
                    write_result_artifact_manifest(root)
                    with self.assertRaisesRegex(
                        gate.GateError,
                        (
                            rf"{messages}-message scale requires exactly "
                            rf"{expected_count} .* observed {observed_count}"
                        ),
                    ):
                        gate_publication_scale(
                            root, messages, expected
                        )

    def test_scale_gate_requires_the_runner_empty_completion_marker(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            (root / "COMPLETE").write_bytes(b"not the runner marker")
            with self.assertRaisesRegex(
                gate.GateError, "completion marker is not the empty file"
            ):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            (root / "COMPLETE").unlink()
            with self.assertRaisesRegex(
                gate.GateError, "completion marker is missing"
            ):
                gate_publication_scale(root, 250_000, expected)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "scale"
            expected = write_scale_fixture(root, messages=250_000)
            complete = root / "COMPLETE"
            complete.unlink()
            complete.symlink_to(root / "run-summary.tsv")
            with self.assertRaisesRegex(
                gate.GateError,
                "completion marker must be a regular non-symlink file",
            ):
                gate_publication_scale(root, 250_000, expected)


if __name__ == "__main__":
    unittest.main()
