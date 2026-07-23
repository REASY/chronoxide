#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import phase6_codec_ab_gate as gate


def write_json(path: Path, value: object) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def admission_plan_fixture(
    result: Path, *, internal: bool = False
) -> dict[str, object]:
    return {
        "schema": gate.ADMISSION_PLAN_SCHEMA,
        "result_dir": str(result),
        "capture": "/capture",
        "repo": "/repo",
        "query_manifest": "/manifest.json",
        "config_template": "/config.toml",
        "validated_input_config_template": "/input-config.toml",
        "expectations": "/expectations.json",
        "binary_provenance_mode": "internal" if internal else "external-exploratory",
        "promotion_eligibility": (
            "formal_source_bound"
            if internal
            else "exploratory_non_promotable_external_binaries"
        ),
        "stop_after_messages": gate.PINNED_STOP_AFTER_MESSAGES,
        "replay_blocks": gate.PINNED_REPLAY_BLOCKS,
        "query_blocks": gate.PINNED_QUERY_BLOCKS,
        "benchmark_repeats": gate.FIXED_BENCHMARK_REPEATS,
        "rss_interval_ms": gate.FIXED_RSS_INTERVAL_MS,
        "guard_interval_ms": gate.FIXED_GUARD_INTERVAL_MS,
        "capacity_monitor_interval_ms": gate.FIXED_CAPACITY_MONITOR_INTERVAL_MS,
        "page_size_bytes": 4096,
        "max_capture_resident_bytes_after_evict": 0,
        "max_corpus_resident_bytes_after_evict": 0,
        "max_dirty_writeback_bytes": 67_108_864,
        "capacity_contract_sha256": "a" * 64,
        "readback_sample_limit_per_kind": 2,
        "rust_log": "chronoxide_ingester=info",
        "perf_stat_mode": "required" if internal else "off",
        "perf_binary": "-",
        "perf_binary_sha256": "-",
        "perf_version": "-",
        "chunk_read_queue_depth": 128,
        "query_label_arena_max_bytes": 536_870_912,
        "query_max_series_matched": 1_000_000,
        "query_max_projected_series": 2_000_000,
        "query_max_chunks_read": 5_000_000,
        "query_max_bytes_read": 2_147_483_648,
        "query_max_samples": 50_000_000,
        "regex_max_expanded_values": 100_000,
    }


def perf_rows() -> list[str]:
    rows = ["# exact Phase 6 perf fixture", ""]
    for index, event in enumerate(gate.PERF_REQUIRED_EVENTS):
        value = "1.25" if index == 0 else str(index - 1)
        unit = "msec" if index == 0 else ""
        rows.append(
            "\t".join((value, unit, event, "100000", "100.00", "", ""))
        )
    return rows


def settings_fixture(plan: dict[str, object]) -> dict[str, str]:
    return {
        "recorded_at": "2026-07-22T10:00:00+08:00",
        "dry_run": "0",
        "binary_provenance_mode": str(plan["binary_provenance_mode"]),
        "promotion_eligibility": str(plan["promotion_eligibility"]),
        "stop_after_messages": str(plan["stop_after_messages"]),
        "formal_build": (
            "--locked --release --no-default-features; one isolated target build "
            "from an exact read-only git archive HEAD snapshot when internal"
        ),
        "quiet_host_confirmed": "1",
        "rss_interval_ms": str(plan["rss_interval_ms"]),
        "conflict_guard_interval_ms": str(plan["guard_interval_ms"]),
        "conflict_precheck": "same classifier; exact PID ancestry exclusions only",
        "capacity_monitor_interval_ms": str(plan["capacity_monitor_interval_ms"]),
        "page_size_bytes": str(plan["page_size_bytes"]),
        "max_capture_resident_bytes_after_evict": str(
            plan["max_capture_resident_bytes_after_evict"]
        ),
        "max_corpus_resident_bytes_after_evict": str(
            plan["max_corpus_resident_bytes_after_evict"]
        ),
        "max_dirty_writeback_bytes": str(plan["max_dirty_writeback_bytes"]),
        "replay_launch": "held_until_root_starttime_bound_rss_and_capacity_first_samples",
        "replay_monitor_ready_markers": "distinct_immutable_atomic_mode_0444",
        "replay_monitor_cadence": "edge_inclusive_initial_sample_terminal_max_200ms",
        "capacity_operational_floor_bytes": "1",
        "capacity_build_source_result_allowance_bytes": "2",
        "capacity_schedule_safe_reserve_bytes": "3",
        "same_binary_runtime_control": (
            "head_buffer.float_encoding plus matching segment_writer.float_encoding"
        ),
        "replay_blocks": str(plan["replay_blocks"]),
        "query_blocks": str(plan["query_blocks"]),
        "schedule": "odd raw,gorilla,gorilla,raw; even reversed",
        "benchmark_repeats": f"{plan['benchmark_repeats']} (cold,warm,warm)",
        "storage_layout": "schema8",
        "query_backend": "pread",
        "query_payload_gap_bytes": "4096",
        "query_label_materialization": "demand-driven",
        "query_label_storage": "compact-ids",
        "query_label_arena_max_bytes": str(plan["query_label_arena_max_bytes"]),
        "query_instrumentation": "off",
        "chunk_read_queue_depth": str(plan["chunk_read_queue_depth"]),
        "query_max_series_matched": str(plan["query_max_series_matched"]),
        "query_max_projected_series": str(plan["query_max_projected_series"]),
        "query_max_chunks_read": str(plan["query_max_chunks_read"]),
        "query_max_bytes_read": str(plan["query_max_bytes_read"]),
        "query_max_samples": str(plan["query_max_samples"]),
        "regex_max_expanded_values": str(plan["regex_max_expanded_values"]),
        "range_scalar_cache_max_bytes": "manifest; Phase 6 entries use 0",
        "perf_stat_mode": str(plan["perf_stat_mode"]),
        "perf_binary": str(plan["perf_binary"]),
        "perf_binary_sha256": str(plan["perf_binary_sha256"]),
        "perf_version": str(plan["perf_version"]),
        "perf_events": ",".join(gate.PERF_REQUIRED_EVENTS),
        "footer_validation": "exhaustive verifier pass outside replay/query timing",
        "readback_sample_limit_per_kind": str(
            plan["readback_sample_limit_per_kind"]
        ),
        "readback_validation": "separate untimed independent oracle, zero skips required",
        "timestamp_runtime_ab": (
            "blocked: no versioned writer/reader selector; verifier candidate inventory only"
        ),
        "timestamp_evidence_scope": (
            "native payload; typed scalar-lane timestamps excluded"
        ),
        "rust_log": str(plan["rust_log"]),
        "run_note": "quiet test host",
    }


def stop_test_process(process: subprocess.Popen[object]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


def replay_monitor_fixture(root: Path) -> tuple[Path, Path, Path, Path]:
    control = root / "replay-monitor-control.json"
    rss_ready = root / "rss-monitor.ready"
    capacity_ready = root / "capacity-monitor.ready"
    launch = root / "replay.launch"
    value = {
        "schema": gate.REPLAY_MONITOR_CONTROL_SCHEMA,
        "root_pid": 100,
        "root_ppid": 10,
        "root_starttime_ticks": 1_000,
        "rss_monitor_pid": 101,
        "rss_monitor_ppid": 10,
        "rss_monitor_starttime_ticks": 1_001,
        "capacity_monitor_pid": 102,
        "capacity_monitor_ppid": 10,
        "capacity_monitor_starttime_ticks": 1_002,
        "interval_ms": 100,
        "rss_ready_marker": str(rss_ready),
        "capacity_ready_marker": str(capacity_ready),
        "launch_marker": str(launch),
    }
    write_json(control, value)
    for path in (control, rss_ready, capacity_ready, launch):
        if path != control:
            path.touch()
        path.chmod(0o444)
    return control, rss_ready, capacity_ready, launch


def pinned_capacity_contract() -> tuple[Path, Path, str, dict[str, object]]:
    repo = Path(__file__).resolve().parents[3]
    expectations = Path(__file__).with_name("phase1_4m_expectations.json")
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return (
        repo,
        expectations,
        head,
        gate.build_capacity_contract(expectations, repo, head, 2),
    )


def winner(chunks: int = 0, points: int = 0) -> dict[str, int]:
    return {"chunks": chunks, "points": points}


def histogram(observations: int, lower: int) -> dict[str, object]:
    return {
        "zero_count": 0,
        "buckets": [
            {
                "lower_inclusive": lower,
                "upper_inclusive": 2 * lower - 1,
                "count": observations,
            }
        ] if observations else [],
    }


def timestamp_evidence() -> dict[str, object]:
    def candidate(size: int, selected: bool = False) -> dict[str, object]:
        return {
            "bytes": size,
            "unique_wins": winner(2, 4) if selected else winner(),
            "adaptive_selections": winner(2, 4) if selected else winner(),
        }

    return {
        "chunks": 2,
        "points": 4,
        "current_offset_uleb": candidate(20),
        "adjacent_delta_uleb": candidate(18),
        "delta_of_delta_zigzag_uleb128": candidate(16),
        "fixed_step_residual_bitpack": candidate(14, True),
        "adaptive_min_bytes": 14,
        "tied_minima": winner(),
    }


def float_evidence(existing_indexed: int, existing_payload: int) -> dict[str, object]:
    result: dict[str, object] = {
        "tie_rule": "RAW_F64 wins equal payload-byte ties; then compare decode cost before activation",
        "chunks": 2,
        "points": 4,
        "existing_indexed_bytes": existing_indexed,
        "existing_payload_bytes": existing_payload,
        "raw_f64_candidate_indexed_bytes": 164,
        "raw_f64_candidate_payload_bytes": 84,
        "gorilla_candidate_indexed_bytes": 120,
        "gorilla_candidate_payload_bytes": 40,
        "adaptive_min_indexed_bytes": 120,
        "adaptive_min_payload_bytes": 40,
        "raw_f64_wins": winner(),
        "gorilla_wins": winner(2, 4),
        "ties": winner(),
        "adaptive_raw_f64_selections": winner(),
        "adaptive_gorilla_selections": winner(2, 4),
        "repeated_xor_points": 0,
        "reused_window_points": 1,
        "new_window_points": 1,
        "xor_significant_bits_histogram": histogram(2, 2),
        "positive_zero_points": 1,
        "negative_zero_points": 0,
        "finite_nonzero_points": 3,
        "positive_infinity_points": 0,
        "negative_infinity_points": 0,
        "ordinary_nan_points": 0,
        "stale_nan_points": 0,
    }
    self_keys = set(result)
    assert self_keys == gate.EXPECTED_FLOAT_EVIDENCE_KEYS
    return result


def verifier(codec: str) -> dict[str, object]:
    raw = codec == "raw"
    payload = 84 if raw else 40
    indexed = payload + 80
    encoding = "raw_f64" if raw else "gorilla"
    return {
        "schema_version": 8,
        "footer_validation_enabled": True,
        "series_sample_per_segment": None,
        "verified_selection_fingerprint": ("a" if raw else "b") * 64,
        "decoded_semantic_fingerprint": "c" * 64,
        "segments": 1,
        "corpus_series": 1,
        "series": 1,
        "chunks": 2,
        "chunks_by_kind": [2, 0, 0, 0, 0],
        "samples": 4,
        "logical_chunk_bytes": indexed,
        "chunk_inventory": {
            "layout": "sealed_chunk_v1",
            "by_kind_encoding": [
                {
                    "kind": "float",
                    "encoding": encoding,
                    "payload_layout": (
                        "t0_interleaved_dt_value" if raw else "t0_dt_then_values"
                    ),
                    "chunks": 2,
                    "points": 4,
                    "indexed_bytes": indexed,
                    "common_header_bytes": 80,
                    "scalar_lane_bytes": 0,
                    "payload_bytes": payload,
                    "timestamp_base_bytes": 16,
                    "timestamp_delta_bytes": 4,
                    "value_bytes": payload - 20,
                    "point_count_histogram": histogram(2, 2),
                    "cadence_ms_histogram": histogram(2, 1),
                }
            ],
            "raw_f64_vs_gorilla": float_evidence(indexed, payload),
            "timestamp_candidates": {
                "scope": "native payload only",
                "tie_rule": "stable order",
                "selector_bytes_included": False,
                "all_blocks": timestamp_evidence(),
                "by_shape": [{"shape": "variable_step", "evidence": timestamp_evidence()}],
                "by_kind_encoding": [
                    {"kind": "float", "encoding": encoding, "evidence": timestamp_evidence()}
                ],
            },
        },
        "exact_postings": {
            "logical_fingerprint": "d" * 64,
            "lists": 1,
            "decoded_refs": 1,
            "encoded_bytes": 1,
        },
        "elapsed_ns": 1,
        "metadata_read_calls": 1,
        "metadata_read_bytes": 1,
        "metadata_peak_retained_bytes": 1,
        "metadata_peak_in_flight_bytes": 1,
        "metadata_peak_open_files": 1,
        "metadata_cache_hits": 0,
        "metadata_cache_misses": 1,
    }


class Phase6CodecGateTests(unittest.TestCase):
    def test_admission_plan_pins_measurement_and_formal_perf_controls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            metadata = result / "metadata"
            metadata.mkdir(parents=True)
            path = metadata / "admission-plan.json"
            plan = admission_plan_fixture(result)
            write_json(path, plan)
            self.assertEqual(gate._load_admission_plan(result, path), plan)  # noqa: SLF001

            mutations = {
                "capture ceiling": ("max_capture_resident_bytes_after_evict", 1),
                "corpus ceiling": ("max_corpus_resident_bytes_after_evict", 1),
                "writeback ceiling": ("max_dirty_writeback_bytes", 67_108_865),
                "page size": ("page_size_bytes", 0),
                "repeats": ("benchmark_repeats", 2),
                "RSS interval": ("rss_interval_ms", 99),
                "label arena": ("query_label_arena_max_bytes", 536_870_911),
            }
            for name, (field, value) in mutations.items():
                with self.subTest(name=name):
                    mutated = dict(plan)
                    mutated[field] = value
                    write_json(path, mutated)
                    with self.assertRaises(gate.GateError):
                        gate._load_admission_plan(result, path)  # noqa: SLF001

            internal = admission_plan_fixture(result, internal=True)
            internal["perf_stat_mode"] = "auto"
            write_json(path, internal)
            with self.assertRaisesRegex(gate.GateError, "requires perf_stat_mode"):
                gate._load_admission_plan(result, path)  # noqa: SLF001

    def test_residency_evidence_reconstructs_paths_sums_and_page_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "one-byte.bin"
            second = root / "one-page.bin"
            first.write_bytes(b"x")
            second.write_bytes(b"x" * 4096)
            paths = root / "paths.nul"
            paths.write_bytes(os.fsencode(first) + b"\0" + os.fsencode(second) + b"\0")
            evidence = root / "residency.tsv"
            phase = "query-corpus-after-run"
            header = "\t".join(gate.RESIDENCY_EVIDENCE_FIELDS)
            canonical = [
                header,
                f"{phase}\t1\tfile\t4096\t1\t-\t{first}",
                f"{phase}\t2\tfile\t4096\t4096\t-\t{second}",
                f"{phase}\t3\ttotal\t8192\t4097\t-\t-",
            ]
            evidence.write_text("\n".join(canonical) + "\n", encoding="utf-8")
            result = gate.validate_residency_evidence(
                evidence, phase, paths, None, 4096
            )
            self.assertEqual(result["resident_bytes"], 8192)
            self.assertEqual(result["page_size_bytes"], 4096)

            mutations = {
                "page bound": (1, f"{phase}\t1\tfile\t4097\t1\t-\t{first}"),
                "page granularity": (1, f"{phase}\t1\tfile\t4095\t1\t-\t{first}"),
                "wrong path": (1, f"{phase}\t1\tfile\t4096\t1\t-\t{second}"),
                "wrong phase": (1, f"wrong\t1\tfile\t4096\t1\t-\t{first}"),
                "wrong size": (1, f"{phase}\t1\tfile\t4096\t2\t-\t{first}"),
                "wrong sum": (3, f"{phase}\t3\ttotal\t8191\t4097\t-\t-"),
                "wrong threshold": (2, f"{phase}\t2\tfile\t4096\t4096\t0\t{second}"),
            }
            for name, (index, replacement) in mutations.items():
                with self.subTest(name=name):
                    rows = list(canonical)
                    rows[index] = replacement
                    evidence.write_text("\n".join(rows) + "\n", encoding="utf-8")
                    with self.assertRaises(gate.GateError):
                        gate.validate_residency_evidence(
                            evidence, phase, paths, None, 4096
                        )

            evidence.write_text("not-the-header\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "TSV shape"):
                gate.validate_residency_evidence(evidence, phase, paths, None, 4096)

    def test_residency_zero_ceiling_rejects_one_resident_page(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = root / "payload.bin"
            payload.write_bytes(b"x")
            paths = root / "paths.nul"
            paths.write_bytes(os.fsencode(payload) + b"\0")
            evidence = root / "residency.tsv"
            phase = "replay-capture-after-evict"
            header = "\t".join(gate.RESIDENCY_EVIDENCE_FIELDS)
            evidence.write_text(
                f"{header}\n{phase}\t1\tfile\t0\t1\t0\t{payload}\n"
                f"{phase}\t2\ttotal\t0\t1\t0\t-\n",
                encoding="utf-8",
            )
            gate.validate_residency_evidence(evidence, phase, paths, 0, 4096)
            evidence.write_text(
                f"{header}\n{phase}\t1\tfile\t4096\t1\t0\t{payload}\n"
                f"{phase}\t2\ttotal\t4096\t1\t0\t-\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "ceiling by 4096 bytes"):
                gate.validate_residency_evidence(evidence, phase, paths, 0, 4096)

    def test_writeback_evidence_enforces_arithmetic_threshold_and_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "writeback.tsv"
            phase = "replay-before"
            header = "\t".join(gate.WRITEBACK_EVIDENCE_FIELDS)
            canonical = [
                header,
                f"{phase}\t1\t2026-07-22T10:00:00,000000000+08:00\t65537\t0\t67109888\t67108864\tretry",
                f"{phase}\t2\t2026-07-22T10:00:01,000000000+08:00\t65536\t0\t67108864\t67108864\tpass",
            ]
            path.write_text("\n".join(canonical) + "\n", encoding="utf-8")
            result = gate.validate_writeback_evidence(path, phase, 67_108_864)
            self.assertEqual(result["samples"], 2)
            self.assertEqual(result["final_total_bytes"], 67_108_864)

            mutations = {
                "wrong phase": (1, canonical[1].replace(phase, "wrong", 1)),
                "wrong arithmetic": (1, canonical[1].replace("67109888", "67109887")),
                "early pass": (1, canonical[1].replace("retry", "pass")),
                "terminal over": (
                    2,
                    canonical[2].replace("65536\t0\t67108864", "65537\t0\t67109888"),
                ),
                "wrong ceiling": (2, canonical[2].replace("67108864\tpass", "67108865\tpass")),
                "nonmonotonic time": (
                    2,
                    canonical[2].replace("10:00:01", "10:00:00"),
                ),
                "impossible time": (
                    2,
                    canonical[2].replace("2026-07-22", "2026-99-22"),
                ),
            }
            for name, (index, replacement) in mutations.items():
                with self.subTest(name=name):
                    rows = list(canonical)
                    rows[index] = replacement
                    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
                    with self.assertRaises(gate.GateError):
                        gate.validate_writeback_evidence(path, phase, 67_108_864)

            path.write_text(header + "\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "one and 30"):
                gate.validate_writeback_evidence(path, phase, 67_108_864)

    def test_measurement_reconstruction_covers_the_exact_dynamic_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory)
            plan = admission_plan_fixture(result)
            replay_rows = []
            for block in range(1, 3):
                codecs = ("raw", "gorilla", "gorilla", "raw") if block == 1 else (
                    "gorilla",
                    "raw",
                    "raw",
                    "gorilla",
                )
                for slot, codec in enumerate(codecs, 1):
                    label = f"replay-b{block:02d}-s{slot:02d}-{codec}"
                    replay_rows.append(
                        {"label": label, "run_dir": result / "replays" / label}
                    )
            query_rows = []
            for query_name in (
                "float_full_last",
                "float_scalar_rate_instant",
                "float_scalar_rate_range",
                "typed_scalar_lane_control",
                "typed_full_control",
            ):
                for replay_row in replay_rows:
                    codec = replay_row["label"].rsplit("-", 1)[1]
                    process_label = f"{query_name}-{replay_row['label'][7:]}"
                    query_rows.append(
                        {
                            "process_label": process_label,
                            "codec": codec,
                            "run_dir": result / "query-runs" / process_label,
                        }
                    )

            residency_result = {
                "phase": "ignored",
                "sha256": "a" * 64,
                "file_count": 1,
                "resident_bytes": 0,
                "size_bytes": 1,
                "ceiling_bytes": 0,
                "page_size_bytes": 4096,
                "status": "pass",
            }
            writeback_result = {
                "phase": "ignored",
                "sha256": "b" * 64,
                "samples": 1,
                "maximum_total_bytes": 0,
                "final_dirty_kib": 0,
                "final_writeback_kib": 0,
                "final_total_bytes": 0,
                "ceiling_bytes": 67_108_864,
                "status": "pass",
            }
            with (
                mock.patch.object(
                    gate,
                    "validate_residency_evidence",
                    return_value=residency_result,
                ) as residency,
                mock.patch.object(
                    gate,
                    "validate_writeback_evidence",
                    return_value=writeback_result,
                ) as writeback,
            ):
                document = gate._measurement_preconditions(  # noqa: SLF001
                    result, plan, replay_rows, query_rows
                )
            self.assertEqual(residency.call_count, 88)
            self.assertEqual(writeback.call_count, 50)
            self.assertEqual(
                document["counts"],
                {
                    "capture_residency_admissions": 8,
                    "corpus_residency_admissions": 40,
                    "corpus_residency_after_run_observations": 40,
                    "writeback_admissions": 50,
                },
            )

    def test_perf_parser_requires_exact_ordered_available_ten_event_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "perf.tsv"
            output = root / "perf.json"
            canonical = perf_rows()
            source.write_text("\n".join(canonical) + "\n", encoding="utf-8")
            gate.parse_perf(source, output)
            parsed = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(
                [item["event"] for item in parsed["events"]],
                list(gate.PERF_REQUIRED_EVENTS),
            )
            output.unlink()

            event_start = 2
            mutations: dict[str, list[str]] = {}
            mutations["missing"] = canonical[:event_start] + canonical[event_start + 1 :]
            mutations["duplicate"] = canonical + [canonical[event_start]]
            swapped = list(canonical)
            swapped[event_start], swapped[event_start + 1] = (
                swapped[event_start + 1],
                swapped[event_start],
            )
            mutations["reordered"] = swapped
            mutations["alias"] = [
                *canonical[: event_start + 1],
                canonical[event_start + 1].replace("cycles", "cpu-cycles"),
                *canonical[event_start + 2 :],
            ]
            mutations["modifier"] = [
                *canonical[: event_start + 1],
                canonical[event_start + 1].replace("cycles", "cycles:u"),
                *canonical[event_start + 2 :],
            ]
            mutations["PMU name"] = [
                *canonical[: event_start + 1],
                canonical[event_start + 1].replace("cycles", "cpu_core/cycles/"),
                *canonical[event_start + 2 :],
            ]
            mutations["unavailable"] = [
                *canonical[: event_start + 1],
                canonical[event_start + 1].replace("0\t\tcycles", "<not counted>\t\tcycles"),
                *canonical[event_start + 2 :],
            ]
            mutations["unsupported"] = [
                *canonical[: event_start + 1],
                canonical[event_start + 1].replace(
                    "0\t\tcycles", "<not supported>\t\tcycles"
                ),
                *canonical[event_start + 2 :],
            ]
            mutations["decimal count"] = [
                *canonical[: event_start + 1],
                canonical[event_start + 1].replace("0\t\tcycles", "0.5\t\tcycles"),
                *canonical[event_start + 2 :],
            ]
            mutations["short row"] = [*canonical, "1\t\tunknown"]
            mutations["runtime zero"] = [
                *canonical[:event_start],
                canonical[event_start].replace("\t100000\t", "\t0\t"),
                *canonical[event_start + 1 :],
            ]
            mutations["percent zero"] = [
                *canonical[:event_start],
                canonical[event_start].replace("\t100.00\t", "\t0.00\t"),
                *canonical[event_start + 1 :],
            ]
            mutations["metric payload"] = [
                *canonical[:event_start],
                canonical[event_start][:-1] + "metric\tunit",
                *canonical[event_start + 1 :],
            ]
            for name, rows in mutations.items():
                with self.subTest(name=name):
                    source.write_text("\n".join(rows) + "\n", encoding="utf-8")
                    with self.assertRaises(gate.GateError):
                        gate.parse_perf(source, output)
                    self.assertFalse(output.exists())

    def test_effective_perf_policy_is_fail_closed_for_formal_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory)
            metadata = result / "metadata"
            metadata.mkdir(parents=True)
            effective = metadata / "perf-effective.txt"
            exit_status = metadata / "perf-preflight.exit-status"
            plan = admission_plan_fixture(result, internal=True)
            effective.write_text("on\n", encoding="ascii")
            exit_status.write_text("0\n", encoding="ascii")
            self.assertTrue(gate._effective_perf_policy(result, plan))  # noqa: SLF001

            exit_status.write_text("1\n", encoding="ascii")
            with self.assertRaisesRegex(gate.GateError, "exactly zero"):
                gate._effective_perf_policy(result, plan)  # noqa: SLF001
            exit_status.write_text("0\n", encoding="ascii")
            effective.write_text("off\n", encoding="ascii")
            with self.assertRaisesRegex(gate.GateError, "required perf"):
                gate._effective_perf_policy(result, plan)  # noqa: SLF001

            exploratory = admission_plan_fixture(result)
            self.assertFalse(gate._effective_perf_policy(result, exploratory))  # noqa: SLF001

    def test_perf_tool_identity_binds_path_digest_and_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "perf-fixture"
            binary.write_text(
                "#!/bin/sh\nprintf '%s\\n' 'perf version fixture'\n",
                encoding="utf-8",
            )
            binary.chmod(0o755)
            digest = gate._sha256(binary)  # noqa: SLF001
            gate._validate_perf_tool_identity(  # noqa: SLF001
                "required", str(binary), digest, "perf version fixture"
            )
            with self.assertRaisesRegex(gate.GateError, "digest differs"):
                gate._validate_perf_tool_identity(  # noqa: SLF001
                    "required", str(binary), "0" * 64, "perf version fixture"
                )
            with self.assertRaisesRegex(gate.GateError, "version differs"):
                gate._validate_perf_tool_identity(  # noqa: SLF001
                    "required", str(binary), digest, "perf version wrong"
                )
            gate._validate_perf_tool_identity("off", "-", "-", "-")  # noqa: SLF001
            with self.assertRaisesRegex(gate.GateError, "explicit '-' tool tuple"):
                gate._validate_perf_tool_identity(  # noqa: SLF001
                    "off", str(binary), digest, "perf version fixture"
                )

    def test_saved_perf_evidence_rejects_raw_or_parsed_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            metadata = result / "metadata"
            replay_dir = result / "replays" / "replay"
            query_dir = result / "query-runs" / "query"
            for path in (metadata, replay_dir, query_dir):
                path.mkdir(parents=True)
            sources = [
                (metadata / "perf-preflight.tsv", metadata / "perf-preflight.json"),
                (replay_dir / "perf.tsv", replay_dir / "perf.json"),
                (query_dir / "perf.tsv", query_dir / "perf.json"),
            ]
            for raw, parsed in sources:
                raw.write_text("\n".join(perf_rows()) + "\n", encoding="utf-8")
                gate.parse_perf(raw, parsed)
            replay_rows = [{"run_dir": replay_dir}]
            query_rows = [{"run_dir": query_dir}]
            gate._validate_saved_perf_evidence(  # noqa: SLF001
                result, replay_rows, query_rows, True
            )

            sources[1][1].write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "strict raw reconstruction"):
                gate._validate_saved_perf_evidence(  # noqa: SLF001
                    result, replay_rows, query_rows, True
                )
            sources[1][1].unlink()
            gate.parse_perf(*sources[1])
            sources[2][0].write_text("malformed\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "seven-column"):
                gate._validate_saved_perf_evidence(  # noqa: SLF001
                    result, replay_rows, query_rows, True
                )

    def test_range_queries_require_disabled_scalar_cache(self) -> None:
        manifest = Path(gate.__file__).with_name("phase6_codec_queries.json")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mutated = json.loads(manifest.read_text(encoding="utf-8"))
            range_query = next(item for item in mutated["queries"] if item["mode"] == "range")
            range_query["range_scalar_cache_max_bytes"] = 1
            input_path = write_json(root / "queries.json", mutated)
            with self.assertRaisesRegex(gate.GateError, "require.*=0"):
                gate.normalize_manifest(
                    input_path, root / "queries.tsv", root / "normalized.json", 0
                )

    def test_settings_exactly_validate_every_persisted_claim(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory)
            metadata = result / "metadata"
            metadata.mkdir()
            plan = admission_plan_fixture(result)
            settings = settings_fixture(plan)
            (metadata / "run-note.txt").write_text(
                settings["run_note"] + "\n", encoding="utf-8"
            )

            def write_settings(value: dict[str, str]) -> None:
                (metadata / "settings.txt").write_text(
                    "".join(f"{key}={item}\n" for key, item in value.items()),
                    encoding="utf-8",
                )

            capacity = {
                "operational_floor_bytes": 1,
                "build_source_result_allowance_bytes": 2,
                "schedule": {"safe_corpus_reserve_bytes": 3},
            }
            write_settings(settings)
            with mock.patch.object(
                gate, "_load_capacity_contract", return_value=capacity
            ):
                gate._validate_settings(result, plan)  # noqa: SLF001
                for key in settings:
                    with self.subTest(key=key):
                        mutated = dict(settings)
                        mutated[key] = "tampered"
                        write_settings(mutated)
                        with self.assertRaises(gate.GateError):
                            gate._validate_settings(result, plan)  # noqa: SLF001

    def test_runner_binds_manifest_perf_page_and_tsv_validators(self) -> None:
        runner = Path(gate.__file__).with_name("phase6_codec_ab_run.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn('PERF_STAT_MODE="${PERF_STAT_MODE:-required}"', runner)
        self.assertIn('PERF_BIN="$(realpath -e -- "$PERF_BIN")"', runner)
        self.assertNotIn("\n    perf stat ", runner)
        self.assertGreaterEqual(runner.count('"$PERF_BIN" stat '), 3)
        self.assertIn("assert_perf_identity", runner)
        self.assertIn('[[ "$QUERY_MANIFEST" == "$DEFAULT_QUERY_MANIFEST" ]]', runner)
        self.assertIn('--page-size-bytes "$PAGE_SIZE_BYTES"', runner)
        self.assertEqual(runner.count("validate-writeback-evidence"), 3)
        self.assertEqual(runner.count("validate-residency-evidence"), 3)
        self.assertGreaterEqual(runner.count('--file "$run_dir/perf.tsv"'), 2)
        for artifact in (
            "perf-preflight.tsv",
            "perf-preflight.exit-status",
            "perf-effective.txt",
        ):
            self.assertIn(f'--file "$METADATA_DIR/{artifact}"', runner)
        settings_start = runner.index("    printf 'recorded_at=%s")
        settings_end = runner.index('} >"$METADATA_DIR/settings.txt"', settings_start)
        emitted_settings = set(
            re.findall(r"printf '([a-z0-9_]+)=", runner[settings_start:settings_end])
        )
        self.assertEqual(
            emitted_settings,
            set(settings_fixture(admission_plan_fixture(Path("/result")))),
        )

    def test_runner_disables_python_bytecode_for_frozen_harness(self) -> None:
        runner = (Path(__file__).resolve().parent / "phase6_codec_ab_run.sh").read_text(
            encoding="utf-8"
        )
        export = "export PYTHONDONTWRITEBYTECODE=1"
        self.assertIn(export, runner)
        self.assertLess(runner.index(export), runner.index('SCRIPT_DIR='))
        self.assertIn("export PYTHONNOUSERSITE=1", runner)
        self.assertIn('"$PYTHON_BIN" -I -S -B "$@"', runner)
        self.assertIn('"$PYTHON_BIN" -I -S -B -c \'sum(range(10000000))\'', runner)
        self.assertNotIn("perf-preflight.tsv\" -- true", runner)
        harness_block = runner.split("HARNESS_FILES=(", 1)[1].split(")", 1)[0]
        self.assertEqual(set(harness_block.split()), gate.FROZEN_HARNESS_FILES)
        self.assertIn(
            "rustup is outside the pinned formal build tool contract", runner
        )
        self.assertNotIn("command -v rustup", runner)

    def test_source_seal_rejects_dirty_and_untracked_build_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Phase 6 Test"], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.email", "phase6@example.invalid"],
                check=True,
            )
            (repo / "Cargo.lock").write_text("# lock\n", encoding="utf-8")
            (repo / ".gitignore").write_text(
                "ignored.rs\n.cargo/config\n", encoding="utf-8"
            )
            tracked = repo / "tracked.txt"
            tracked.write_text("sealed\n", encoding="utf-8")
            subprocess.run(
                ["git", "-C", str(repo), "add", ".gitignore", "Cargo.lock", "tracked.txt"],
                check=True,
            )
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "test seal"], check=True)
            seal = gate.source_seal(repo)
            seal_path = write_json(repo.parent / f"{repo.name}-seal.json", seal)
            self.assertEqual(gate.check_source_seal(repo, seal_path)["status"], "pass")
            tracked.write_text("dirty\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "clean tracked worktree"):
                gate.source_seal(repo)
            tracked.write_text("sealed\n", encoding="utf-8")
            untracked = repo / "build.rs"
            untracked.write_text("fn main() {}\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "untracked build inputs"):
                gate.source_seal(repo)
            untracked.unlink()
            (repo / "ignored.rs").write_text("pub const HIDDEN: bool = true;\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "ignored source/build input"):
                gate.source_seal(repo)
            (repo / "ignored.rs").unlink()
            (repo / ".cargo").mkdir()
            (repo / ".cargo" / "config").write_text(
                "[build]\nrustflags = ['--cfg', 'hidden']\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(gate.GateError, "ignored source/build input"):
                gate.source_seal(repo)

    def test_source_seal_rejects_hidden_index_flags_and_nonregular_modes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Phase 6 Test"], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.email", "phase6@example.invalid"],
                check=True,
            )
            (repo / "Cargo.lock").write_text("# lock\n", encoding="utf-8")
            tracked = repo / "tracked.txt"
            tracked.write_text("sealed\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "Cargo.lock", "tracked.txt"], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "test seal"], check=True)

            subprocess.run(
                ["git", "-C", str(repo), "update-index", "--assume-unchanged", "tracked.txt"],
                check=True,
            )
            tracked.write_text("hidden mutation\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "nonordinary Git index flag 'h'"):
                gate.source_seal(repo)
            subprocess.run(
                ["git", "-C", str(repo), "update-index", "--no-assume-unchanged", "tracked.txt"],
                check=True,
            )
            tracked.write_text("sealed\n", encoding="utf-8")

            subprocess.run(
                ["git", "-C", str(repo), "update-index", "--skip-worktree", "tracked.txt"],
                check=True,
            )
            with self.assertRaisesRegex(gate.GateError, "nonordinary Git index flag 'S'"):
                gate.source_seal(repo)
            subprocess.run(
                ["git", "-C", str(repo), "update-index", "--no-skip-worktree", "tracked.txt"],
                check=True,
            )

            link = repo / "tracked-link"
            link.symlink_to("tracked.txt")
            subprocess.run(["git", "-C", str(repo), "add", "tracked-link"], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "track symlink"], check=True)
            with self.assertRaisesRegex(gate.GateError, "unsupported tracked Git mode 120000"):
                gate.source_seal(repo)

    def test_read_only_git_archive_snapshot_excludes_ambient_inputs_and_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "repo"
            snapshot = root / "snapshot"
            archive = root / "source-head.tar"
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Phase 6 Test"], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.email", "phase6@example.invalid"],
                check=True,
            )
            (repo / ".gitignore").write_text("*.bin\n", encoding="utf-8")
            (repo / "Cargo.toml").write_text(
                '[package]\nname = "snapshot-fixture"\nversion = "0.1.0"\nedition = "2024"\n',
                encoding="utf-8",
            )
            (repo / "src").mkdir()
            source_text = (
                'const PAYLOAD: &[u8] = include_bytes!("../ambient.bin");\n'
                "fn main() { assert!(!PAYLOAD.is_empty()); }\n"
            )
            tracked = repo / "src" / "main.rs"
            tracked.write_text(source_text, encoding="utf-8")
            (repo / "ambient.bin").write_bytes(b"not part of HEAD")
            subprocess.run(
                ["cargo", "generate-lockfile", "--offline", "--manifest-path", str(repo / "Cargo.toml")],
                check=True,
            )
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "snapshot"], check=True)

            # The live-worktree build consumes the ignored input. The exact archive
            # build must not be able to consume it.
            subprocess.run(
                [
                    "cargo", "build", "--offline", "--locked",
                    "--manifest-path", str(repo / "Cargo.toml"),
                    "--target-dir", str(root / "live-target"),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            source_seal_path = write_json(root / "source-seal.json", gate.source_seal(repo))
            sealed_head = json.loads(source_seal_path.read_text(encoding="utf-8"))["head"]
            subprocess.run(
                [
                    "git", "-C", str(repo), "archive", "--format=tar",
                    f"--output={archive}", sealed_head,
                ],
                check=True,
            )
            snapshot.mkdir()
            subprocess.run(["tar", "-xf", str(archive), "-C", str(snapshot)], check=True)
            entries = list(snapshot.rglob("*"))
            for path in entries:
                if path.is_file() and not path.is_symlink():
                    path.chmod(0o555 if path.stat().st_mode & 0o100 else 0o444)
            for path in sorted((path for path in entries if path.is_dir()), reverse=True):
                path.chmod(0o555)
            snapshot.chmod(0o555)

            seal = gate.source_snapshot_seal(repo, snapshot, source_seal_path)
            self.assertNotIn("ambient.bin", {item["path"] for item in seal["files"]})
            seal_path = write_json(root / "snapshot-seal.json", seal)
            self.assertEqual(
                gate.check_source_snapshot_seal(
                    repo, snapshot, source_seal_path, seal_path
                )["status"],
                "pass",
            )
            snapshot_build = subprocess.run(
                [
                    "cargo", "build", "--offline", "--locked",
                    "--manifest-path", str(snapshot / "Cargo.toml"),
                    "--target-dir", str(root / "snapshot-target"),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(snapshot_build.returncode, 0)
            self.assertIn("ambient.bin", snapshot_build.stderr)

            snapshot_track = snapshot / "src" / "main.rs"
            snapshot_track.chmod(0o644)
            snapshot_track.write_text("mutated\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "source snapshot"):
                gate.check_source_snapshot_seal(
                    repo, snapshot, source_seal_path, seal_path
                )
            snapshot_track.write_text(source_text, encoding="utf-8")
            snapshot_track.chmod(0o444)

            snapshot.chmod(0o755)
            extra = snapshot / "ambient.bin"
            extra.write_bytes(b"injected")
            extra.chmod(0o444)
            snapshot.chmod(0o555)
            with self.assertRaisesRegex(gate.GateError, "path set differs"):
                gate.source_snapshot_seal(repo, snapshot, source_seal_path)
            snapshot.chmod(0o755)
            extra.unlink()
            link = snapshot / "injected-link"
            link.symlink_to("tracked.txt")
            snapshot.chmod(0o555)
            with self.assertRaisesRegex(gate.GateError, "contains a symlink"):
                gate.source_snapshot_seal(repo, snapshot, source_seal_path)
            snapshot.chmod(0o755)
            link.unlink()
            injected_directory = snapshot / "injected-directory"
            injected_directory.mkdir()
            injected_directory.chmod(0o555)
            snapshot.chmod(0o555)
            with self.assertRaisesRegex(gate.GateError, "directory set differs"):
                gate.source_snapshot_seal(repo, snapshot, source_seal_path)
            snapshot.chmod(0o755)
            injected_directory.rmdir()

            with self.assertRaisesRegex(gate.GateError, "root must be mode 0555"):
                gate.source_snapshot_seal(repo, snapshot, source_seal_path)
            for path in snapshot.rglob("*"):
                if path.is_dir():
                    path.chmod(0o755)

    def test_cargo_config_search_path_isolated_from_external_result_ancestors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = root / "result" / "build-source"
            cargo_home = root / "result" / "metadata" / "cargo-home"
            snapshot.mkdir(parents=True)
            cargo_home.mkdir(parents=True)
            self.assertEqual(
                gate.cargo_config_isolation(snapshot, cargo_home)["status"],
                "pass",
            )

            ambient = root / ".cargo" / "config.toml"
            ambient.parent.mkdir()
            ambient.write_text("[build]\nrustflags = ['--cfg', 'ambient']\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "ambient Cargo config"):
                gate.cargo_config_isolation(snapshot, cargo_home)
            ambient.unlink()
            ambient.parent.rmdir()

            cargo_config = cargo_home / "config"
            cargo_config.write_text("[build]\nrustc-wrapper = 'ambient'\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "ambient Cargo config"):
                gate.cargo_config_isolation(snapshot, cargo_home)
            cargo_config.unlink()

            cargo_config.symlink_to("missing-config")
            with self.assertRaisesRegex(gate.GateError, "ambient Cargo config"):
                gate.cargo_config_isolation(snapshot, cargo_home)

    def test_runner_builds_only_from_sealed_snapshot_and_rechecks_authorities(self) -> None:
        runner = Path(__file__).with_name("phase6_codec_ab_run.sh").read_text(encoding="utf-8")
        self.assertEqual(runner.count('cd "$BUILD_SOURCE_DIR"'), 2)
        self.assertNotIn(
            'cd "$REPO_ROOT"\n        env -i "${BUILD_ENV[@]}" "$CARGO_BIN"',
            runner,
        )
        self.assertNotIn(
            'cd "$REPO_ROOT"\n        env -i "${BUILD_ENV[@]}" "${BUILD_COMMAND[@]}"',
            runner,
        )
        self.assertIn('archive --format=tar --output="$SOURCE_ARCHIVE" "$SEALED_HEAD"', runner)
        self.assertIn("assert_harness_seal\n    assert_source_seal\n    assert_harness_seal", runner)
        self.assertIn("assert_control_inputs_seal", runner)
        self.assertIn("check-cargo-config-isolation", runner)
        self.assertIn("final-artifact-inventory", runner)
        self.assertIn("verify-final-artifact-seal", runner)
        self.assertIn("final-admission", runner)
        self.assertIn("write-raw-authorities", runner)
        self.assertIn("metadata/result-artifacts.nul", runner)
        self.assertNotIn("done < <(\n        {\n            find configs", runner)
        self.assertLess(
            runner.index('NORMALIZED_TSV="$RESULT_DIR/queries.tsv"'),
            runner.index('chmod 0444 -- "$NORMALIZED_TSV"'),
        )
        self.assertLess(
            runner.index('NORMALIZED_JSON="$RESULT_DIR/queries.normalized.json"'),
            runner.index('chmod 0444 -- "$NORMALIZED_TSV"'),
        )
        self.assertLess(
            runner.rindex("final-admission \\\n"),
            runner.index('cat >"$RESULT_DIR/TIMESTAMP_CODEC_AB_BLOCKED.txt"'),
        )
        replay_function = runner.index("run_replay()")
        self.assertLess(
            runner.index(
                'record_raw_authority "$run_dir/raw-leaves.json"', replay_function
            ),
            runner.index(
                'parse-time --input "$run_dir/replay.time.txt"', replay_function
            ),
        )
        self.assertLess(
            runner.index('write-raw-authorities\n'),
            runner.index("assert_experiment_seals finalization"),
        )
        self.assertLess(
            runner.index("RAW_GORILLA_COMPLETE_TIMESTAMP_AB_BLOCKED"),
            runner.rindex('>"$METADATA_DIR/result-artifacts.sha256"'),
        )
        self.assertIn(
            "sha256sum --check --strict metadata/result-artifacts.sha256",
            runner,
        )
        self.assertLess(
            runner.index("sha256sum --check --strict metadata/result-artifacts.sha256"),
            runner.index("verify-final-artifact-seal"),
        )
        self.assertNotIn("assert_experiment_seals final-inventory", runner)
        self.assertIn('RSS_INTERVAL_MS="${RSS_INTERVAL_MS:-100}"', runner)
        self.assertIn('[[ "$RSS_INTERVAL_MS" == "100" ]]', runner)
        self.assertLess(
            runner.index('[[ "$RSS_INTERVAL_MS" == "100" ]]'),
            runner.index('mkdir "$RESULT_DIR"'),
        )
        self.assertIn('GUARD_INTERVAL_MS="${GUARD_INTERVAL_MS:-100}"', runner)
        self.assertIn('[[ "$GUARD_INTERVAL_MS" == "100" ]]', runner)
        self.assertIn(
            'CAPACITY_MONITOR_INTERVAL_MS="${CAPACITY_MONITOR_INTERVAL_MS:-100}"',
            runner,
        )
        self.assertLess(
            runner.index("validate-inputs \\\n"), runner.index('mkdir "$RESULT_DIR"')
        )
        self.assertLess(
            runner.index("check-current-conflicts \\\n"),
            runner.index("guard-conflicts \\\n"),
        )
        self.assertIn("monitor-capacity \\\n", runner)
        self.assertIn("capacity-corpus-check.json", runner)
        self.assertIn("guardian-samples.tsv", runner)
        self.assertIn("wait_for_guardian_ready", runner)
        self.assertLess(
            runner.index("wait_for_guardian_ready\n"), runner.index("run_replay()")
        )

    def test_final_artifact_inventory_is_complete_and_fails_closed(self) -> None:
        finalized_matrix = mock.patch.object(
            gate, "_validate_finalized_artifact_matrix"
        )
        finalized_matrix.start()
        self.addCleanup(finalized_matrix.stop)
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            result.mkdir()
            for name in gate.FINAL_ARTIFACT_REQUIRED_DIRECTORIES:
                (result / name).mkdir()
            (result / "build-source").mkdir()
            (result / "build-target").mkdir()
            (result / "metadata" / "build" / "cargo-home").mkdir(parents=True)
            (result / "metadata" / "build" / "home").mkdir()
            harness = result / "metadata" / "harness"
            harness.mkdir()
            for name in gate.FROZEN_HARNESS_FILES:
                (harness / name).write_bytes(b"frozen")
            build_metadata = result / "metadata" / "build"
            for name in gate.INTERNAL_BUILD_FILES:
                (build_metadata / name).write_bytes(b"build evidence")
            (result / "RAW_GORILLA_COMPLETE_TIMESTAMP_AB_BLOCKED").write_bytes(b"")
            (result / "TIMESTAMP_CODEC_AB_BLOCKED.txt").write_text(
                "blocked\n", encoding="utf-8"
            )
            for name in (
                "queries.tsv",
                "queries.normalized.json",
                "replay-plan.tsv",
                "replay-index.tsv",
                "replay-summary.tsv",
                "query-index.tsv",
                "query-summary.tsv",
            ):
                (result / name).write_bytes(b"")
            admission = write_json(
                result / "metadata" / "final-admission.json",
                {
                    "schema": gate.FINAL_ADMISSION_SCHEMA,
                    "status": "pass",
                    "promotion_eligibility": "formal_source_bound",
                },
            )
            admission.chmod(0o444)
            (result / "build-source" / "tracked.rs").write_text(
                "fn main() {}\n", encoding="utf-8"
            )
            (result / "configs" / "raw.toml").write_text(
                "codec = 'raw'\n", encoding="utf-8"
            )
            (result / "metadata" / "build" / "cargo-home" / "cache.bin").write_bytes(
                b"cache"
            )
            (result / "build-target" / "binary").write_bytes(b"derived")

            paths = gate.final_artifact_paths(result)
            self.assertIn("RAW_GORILLA_COMPLETE_TIMESTAMP_AB_BLOCKED", paths)
            self.assertNotIn("build-source/tracked.rs", paths)
            self.assertIn("configs/raw.toml", paths)
            self.assertNotIn("build-target/binary", paths)
            self.assertNotIn("metadata/build/cargo-home/cache.bin", paths)
            self.assertEqual(paths, sorted(paths, key=lambda path: path.encode("utf-8")))

            unexpected_harness = harness / "unexpected.py"
            unexpected_harness.write_bytes(b"injected")
            with self.assertRaisesRegex(gate.GateError, "frozen harness.*differs"):
                gate.final_artifact_paths(result)
            unexpected_harness.unlink()

            unexpected_build_evidence = build_metadata / "unexpected-evidence.txt"
            unexpected_build_evidence.write_bytes(b"injected")
            with self.assertRaisesRegex(
                gate.GateError, "formal build metadata.*differs"
            ):
                gate.final_artifact_paths(result)
            unexpected_build_evidence.unlink()

            output = result / "metadata" / "result-artifacts.nul"
            gate.write_final_artifact_inventory(result, output)
            recorded = [
                item.decode("utf-8")
                for item in output.read_bytes().split(b"\0")
                if item
            ]
            self.assertEqual(recorded, paths)
            output.chmod(0o444)
            checksum = result / "metadata" / "result-artifacts.sha256"
            checksum.write_text(
                "".join(
                    f"{gate._sha256(result / relative)}  {relative}\n"  # noqa: SLF001
                    for relative in [*paths, "metadata/result-artifacts.nul"]
                ),
                encoding="utf-8",
            )
            checksum.chmod(0o444)
            gate.verify_final_artifact_seal(result)

            late_extra = result / "configs" / "late-extra.toml"
            late_extra.write_bytes(b"late")
            with self.assertRaisesRegex(gate.GateError, "differs from exact reinventory"):
                gate.verify_final_artifact_seal(result)
            late_extra.unlink()

            config = result / "configs" / "raw.toml"
            config.write_text("codec = 'tampered'\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "content changed"):
                gate.verify_final_artifact_seal(result)
            config.write_text("codec = 'raw'\n", encoding="utf-8")

            injected_link = result / "configs" / "injected-link"
            injected_link.symlink_to("raw.toml")
            with self.assertRaisesRegex(gate.GateError, "contains a symlink"):
                gate.final_artifact_paths(result)
            injected_link.unlink()

            unsupported = result / "unsupported-root"
            unsupported.mkdir()
            with self.assertRaisesRegex(gate.GateError, "unsupported root directory"):
                gate.final_artifact_paths(result)
            unsupported.rmdir()

            with mock.patch.object(
                gate.os,
                "scandir",
                side_effect=PermissionError("synthetic enumeration failure"),
            ):
                with self.assertRaisesRegex(gate.GateError, "cannot enumerate"):
                    gate.final_artifact_paths(result)

            alias = result.parent / "result-alias"
            alias.symlink_to(result, target_is_directory=True)
            with self.assertRaisesRegex(gate.GateError, "regular directory"):
                gate.final_artifact_paths(alias)

    def test_external_final_artifact_matrix_rejects_internal_build_trees(self) -> None:
        finalized_matrix = mock.patch.object(
            gate, "_validate_finalized_artifact_matrix"
        )
        finalized_matrix.start()
        self.addCleanup(finalized_matrix.stop)
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            result.mkdir()
            for name in gate.FINAL_ARTIFACT_REQUIRED_DIRECTORIES:
                (result / name).mkdir()
            harness = result / "metadata" / "harness"
            harness.mkdir()
            for name in gate.FROZEN_HARNESS_FILES:
                (harness / name).write_bytes(b"frozen")
            (result / "EXPLORATORY_EXTERNAL_BINARIES_NON_PROMOTABLE.txt").write_bytes(
                b"exploratory\n"
            )
            (result / "TIMESTAMP_CODEC_AB_BLOCKED.txt").write_bytes(b"blocked\n")
            for name in (
                "queries.tsv",
                "queries.normalized.json",
                "replay-plan.tsv",
                "replay-index.tsv",
                "replay-summary.tsv",
                "query-index.tsv",
                "query-summary.tsv",
            ):
                (result / name).write_bytes(b"")
            admission = write_json(
                result / "metadata" / "final-admission.json",
                {
                    "schema": gate.FINAL_ADMISSION_SCHEMA,
                    "status": "pass",
                    "promotion_eligibility": (
                        "exploratory_non_promotable_external_binaries"
                    ),
                },
            )
            admission.chmod(0o444)

            gate.final_artifact_paths(result)
            (result / "metadata" / "build").mkdir()
            with self.assertRaisesRegex(gate.GateError, "formal build metadata"):
                gate.final_artifact_paths(result)

    def test_capture_inventory_fails_closed_for_roots_entries_and_enumeration(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture = root / "capture"
            nested = capture / "nested"
            nested.mkdir(parents=True)
            nested.joinpath("capture.bin").write_bytes(b"capture")

            alias = root / "capture-alias"
            alias.symlink_to(capture, target_is_directory=True)
            with self.assertRaisesRegex(gate.GateError, "non-symlink directory"):
                gate.capture_inventory(alias, root / "alias.json", root / "alias.nul")

            real_scandir = os.scandir

            def synthetic_scandir(path: object):
                if Path(path) == nested:
                    raise PermissionError("synthetic nested denial")
                return real_scandir(path)

            with mock.patch.object(gate.os, "scandir", side_effect=synthetic_scandir):
                with self.assertRaisesRegex(gate.GateError, "cannot enumerate capture"):
                    gate.capture_inventory(
                        capture, root / "denied.json", root / "denied.nul"
                    )

            if hasattr(os, "mkfifo"):
                nested.joinpath("capture.bin").unlink()
                os.mkfifo(nested / "pipe")
                with self.assertRaisesRegex(gate.GateError, "not a regular file"):
                    gate.capture_inventory(
                        capture, root / "fifo.json", root / "fifo.nul"
                    )

    def test_raw_authority_detects_leaf_tamper_and_exact_contract_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            metadata = result / "metadata"
            run = result / "replays" / "one"
            metadata.mkdir(parents=True)
            run.mkdir(parents=True)
            leaf = run / "raw.log"
            leaf.write_bytes(b"original")
            seal = run / "raw-leaves.json"
            gate.raw_leaf_seal(result, [leaf], [], seal)
            digest = gate._sha256(seal)  # noqa: SLF001
            authority = metadata / "raw-authorities.tsv"
            checksum = metadata / "raw-authorities.sha256"
            gate.write_raw_authorities(
                result,
                [f"replays/one/raw-leaves.json={digest}"],
                authority,
                checksum,
            )
            with self.assertRaisesRegex(gate.GateError, "already exist"):
                gate.write_raw_authorities(
                    result,
                    [f"replays/one/raw-leaves.json={digest}"],
                    authority,
                    checksum,
                )
            self.assertEqual(stat.S_IMODE(authority.stat().st_mode), 0o444)
            self.assertEqual(stat.S_IMODE(checksum.stat().st_mode), 0o444)
            gate._check_checksum_manifest(checksum, {authority})  # noqa: SLF001
            gate._check_raw_authorities(  # noqa: SLF001
                result, authority, ["replays/one/raw-leaves.json"]
            )
            with self.assertRaisesRegex(gate.GateError, "order or set differs"):
                gate._check_raw_authorities(result, authority, [])  # noqa: SLF001
            gate._require_raw_seal_contract(  # noqa: SLF001
                result,
                "replays/one/raw-leaves.json",
                {"replays/one/raw.log"},
                set(),
            )

            with self.assertRaisesRegex(gate.GateError, "file contract differs"):
                gate._require_raw_seal_contract(  # noqa: SLF001
                    result,
                    "replays/one/raw-leaves.json",
                    {"replays/one/raw.log", "replays/one/missing.log"},
                    set(),
                )
            leaf.write_bytes(b"tampered")
            with self.assertRaisesRegex(gate.GateError, "size changed|content changed"):
                gate._check_raw_authorities(  # noqa: SLF001
                    result, authority, ["replays/one/raw-leaves.json"]
                )

    def test_rebuilt_artifact_comparison_and_exact_matrix_reject_tamper_and_extra(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            evidence = result / "comparisons"
            staging = result.parent / "staging"
            evidence.mkdir(parents=True)
            staging.mkdir()
            actual = evidence / "gate.json"
            rebuilt = staging / "gate.json"
            actual.write_bytes(b"stale")
            rebuilt.write_bytes(b"rebuilt")
            with self.assertRaisesRegex(gate.GateError, "independent reconstruction"):
                gate._compare_bytes(actual, rebuilt, [], result)  # noqa: SLF001
            actual.write_bytes(b"rebuilt")
            reconstructed: list[str] = []
            gate._compare_bytes(actual, rebuilt, reconstructed, result)  # noqa: SLF001
            self.assertEqual(reconstructed, ["comparisons/gate.json"])

            actual.unlink()
            with self.assertRaisesRegex(gate.GateError, "is missing"):
                gate._compare_bytes(actual, rebuilt, [], result)  # noqa: SLF001
            actual.write_bytes(b"rebuilt")

            (evidence / "extra.json").write_bytes(b"extra")
            with self.assertRaisesRegex(gate.GateError, "artifact set differs"):
                gate._require_exact_names(  # noqa: SLF001
                    evidence, "comparison directory", {"gate.json"}, set()
                )

    def test_finalized_layout_rejects_unexpected_nested_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            result.mkdir()
            for name in gate.FINAL_ARTIFACT_REQUIRED_DIRECTORIES:
                (result / name).mkdir()
            for name in gate.FINAL_ROOT_EVIDENCE_FILES | {
                "EXPLORATORY_EXTERNAL_BINARIES_NON_PROMOTABLE.txt",
                "TIMESTAMP_CODEC_AB_BLOCKED.txt",
            }:
                (result / name).write_bytes(b"")
            for name in gate.FINAL_COMPARISON_FILES:
                (result / "comparisons" / name).write_bytes(b"")
            metadata = result / "metadata"
            for name in gate.FINAL_METADATA_BASE_FILES | {"final-admission.json"}:
                (metadata / name).write_bytes(b"")
            for directory_name in ("binaries", "harness", "source"):
                (metadata / directory_name).mkdir()
            for name in gate.FINAL_BINARY_FILES:
                (metadata / "binaries" / name).write_bytes(b"")
            for name in gate.FROZEN_HARNESS_FILES:
                (metadata / "harness" / name).write_bytes(b"")
            for name in gate.FINAL_SOURCE_BASE_FILES:
                (metadata / "source" / name).write_bytes(b"")
            for name in gate.FINAL_INVENTORY_FILES:
                (result / "inventory" / name).write_bytes(b"")
            for codec in gate.CODECS:
                validation = result / "validation" / codec
                validation.mkdir()
                for name in gate.FINAL_VALIDATION_FILES:
                    (validation / name).write_bytes(b"")
            plan = {
                "binary_provenance_mode": "external-exploratory",
                "promotion_eligibility": (
                    "exploratory_non_promotable_external_binaries"
                ),
                "perf_stat_mode": "off",
            }

            gate._validate_final_admission_layout(  # noqa: SLF001
                result, plan, [], [], False, finalized=True
            )
            unexpected = metadata / "unexpected-evidence.txt"
            unexpected.write_bytes(b"injected")
            with self.assertRaisesRegex(
                gate.GateError, "metadata directory.*differs"
            ):
                gate._validate_final_admission_layout(  # noqa: SLF001
                    result, plan, [], [], False, finalized=True
                )
            unexpected.unlink()
            unexpected = result / "validation" / "raw" / "unexpected.json"
            unexpected.write_bytes(b"injected")
            with self.assertRaisesRegex(gate.GateError, "raw validation.*differs"):
                gate._validate_final_admission_layout(  # noqa: SLF001
                    result, plan, [], [], False, finalized=True
                )

    def test_runner_reinventories_capture_after_all_replays(self) -> None:
        runner = (
            Path(__file__)
            .with_name("phase6_codec_ab_run.sh")
            .read_text(encoding="utf-8")
        )
        replay_loop_end = 'done <"$RESULT_DIR/replay-plan.tsv"'
        after_inventory = "capture-after-replays.json"
        replay_comparison = "compare-replays"
        self.assertLess(runner.index(replay_loop_end), runner.index(after_inventory))
        self.assertLess(runner.index(after_inventory), runner.index(replay_comparison))
        self.assertIn(
            'cmp -s "$INVENTORY_DIR/capture.json" '
            '"$INVENTORY_DIR/capture-after-replays.json"',
            runner,
        )
        self.assertIn(
            'cmp -s "$INVENTORY_DIR/capture-files.nul" '
            '"$INVENTORY_DIR/capture-files-after-replays.nul"',
            runner,
        )

    def test_runtime_and_ambient_environment_contracts_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "chronoxide-query"
            binary.write_bytes(b"binary")
            binary.chmod(0o555)
            identity = gate.runtime_identity(
                binary,
                "query",
                ["LC_ALL=C", "TZ=UTC"],
                set(),
            )
            self.assertEqual(identity["role"], "query")
            self.assertEqual(len(identity["binary_sha256"]), 64)
            with self.assertRaisesRegex(gate.GateError, "sanitized contract"):
                gate.runtime_identity(binary, "query", ["LC_ALL=C", "TZ=UTC", "RUST_LOG=x"], set())
        forbidden = gate.forbidden_ambient_environment(
            {
                "PATH": "/bin",
                "CARGO_PROFILE_RELEASE_LTO": "true",
                "LD_PRELOAD": "x.so",
                "PYTHONHOME": "/ambient",
                "PYTHONPATH": "/ambient/modules",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONNOUSERSITE": "1",
            }
        )
        self.assertEqual(
            forbidden,
            ["CARGO_PROFILE_RELEASE_LTO", "LD_PRELOAD", "PYTHONHOME", "PYTHONPATH"],
        )

    def test_conflict_classifier_rejects_build_android_profiler_and_database_work(self) -> None:
        conflicts = (
            ("qemu-system-aar", "qemu-system-aarch64"),
            ("adb", "adb -L tcp:5037 server nodaemon"),
            ("java", "java org.gradle.launcher.daemon.bootstrap.GradleDaemon"),
            ("emulator", "/opt/android-sdk/emulator/emulator"),
            ("soong_ui", "/src/out/soong_ui --build-mode"),
            ("soong_build.bash", "/src/build/soong/soong_build.bash --top /src"),
            ("bash", "bash /src/build/soong/soong_ui.bash --make-mode"),
            ("java", "java -jar /src/android_build/tools/build.jar"),
            ("java", "java -jar /opt/system-qemu/tools.jar redroid"),
            ("cargo-nextest", "/home/u/.cargo/bin/cargo-nextest nextest run"),
            ("python3", "python3 /home/u/.cargo/bin/cargo-nextest nextest run"),
            ("ninja.real", "/src/prebuilts/build-tools/bin/ninja.real -C out"),
            ("ninja-1.12", "/usr/bin/ninja-1.12 -C out"),
            ("clang++.real", "/opt/llvm/bin/clang++.real -c x.cc"),
            ("clang-19.real", "/opt/llvm/bin/clang-19.real -c x.cc"),
            ("gcc-14", "/usr/bin/gcc-14 -c x.c"),
            ("cc1plus", "/usr/libexec/cc1plus x.cc"),
            ("ld.bfd", "/usr/bin/ld.bfd -o out"),
            ("ld.gold", "/usr/bin/ld.gold -o out"),
            ("ld.lld", "/usr/bin/ld.lld -o out"),
            ("lld-19", "/usr/bin/lld-19 -o out"),
            ("mold", "/usr/bin/mold -o out"),
            ("heaptrack", "heaptrack chronoxide-ingester"),
            ("valgrind.bin", "valgrind --tool=callgrind app"),
            ("strace", "strace -f app"),
            ("bpftrace", "bpftrace profile.bt"),
            ("postgres:writer", "postgres: writer process"),
            ("clickhouse-serv", "clickhouse-server --config config.xml"),
            ("greptime", "greptime standalone start"),
            ("mysqld", "mysqld --defaults-file config"),
            ("docker", "/usr/bin/docker build ."),
            ("docker-buildx", "/usr/libexec/docker/cli-plugins/docker-buildx build ."),
            ("docker-compose", "/usr/libexec/docker/cli-plugins/docker-compose up"),
            ("podman", "/usr/bin/podman build ."),
            ("buildah", "/usr/bin/buildah bud ."),
            ("buildctl", "/usr/bin/buildctl build"),
            ("nerdctl", "/usr/bin/nerdctl build ."),
        )
        for comm, command in conflicts:
            with self.subTest(comm=comm, command=command):
                self.assertTrue(gate._is_conflict_process(comm, command))

    def test_conflict_classifier_rejects_interactive_monitors(self) -> None:
        for monitor in (
            "top",
            "btop",
            "bpytop",
            "htop",
            "atop",
            "iotop",
            "iotop-c",
            "nmon",
            "glances",
            "powertop",
            "nvtop",
        ):
            with self.subTest(monitor=monitor):
                self.assertTrue(gate._is_conflict_process(monitor, f"/usr/bin/{monitor}"))

    def test_conflict_classifier_preserves_non_workload_exceptions(self) -> None:
        allowed = (
            ("python3", "python3 phase6_codec_ab_gate.py"),
            ("python3", "python3 /tmp/cargo-nextest-report.py"),
            ("python3", "python3 /tmp/ninja-report.py"),
            ("clangd", "/usr/bin/clangd --background-index"),
            ("clang-format", "/usr/bin/clang-format source.cc"),
            ("ldconfig", "/sbin/ldconfig -p"),
            ("dockerd", "/usr/bin/dockerd --host=unix:///run/user/1000/docker.sock"),
            ("buildkitd", "/usr/bin/buildkitd --rootless"),
            ("rootlesskit", "/usr/bin/rootlesskit --net=slirp4netns dockerd"),
            ("docker-proxy", "/usr/bin/docker-proxy -proto tcp"),
            ("containerd", "/usr/bin/containerd --config /etc/containerd/config.toml"),
            ("containerd-shim", "/usr/bin/containerd-shim -namespace moby"),
            ("containerd-shim-runc-v1", "/usr/bin/containerd-shim-runc-v1 -namespace moby"),
            ("containerd-shim-runc-v2", "/usr/bin/containerd-shim-runc-v2 -namespace moby"),
            ("buildkitd-report", "/usr/local/bin/buildkitd-report --json"),
            ("rootlesskit-helper", "/usr/local/bin/rootlesskit-helper"),
            ("java", "java -jar /opt/idea/lib/idea.jar"),
            ("postgres_export", "postgres_exporter --web.listen-address=:9187"),
            ("tmux: client", "tmux new -s artracer"),
        )
        for comm, command in allowed:
            with self.subTest(comm=comm, command=command):
                self.assertFalse(gate._is_conflict_process(comm, command))

    def test_conflict_precheck_uses_exact_ancestry_and_catches_short_lived_work(self) -> None:
        snapshot = {
            100: (1, "phase6-runner", "phase6-runner"),
            101: (100, "cargo", "cargo build"),
            200: (1, "cargo", "cargo build"),
            201: (200, "rustc", "rustc crate.rs"),
        }
        conflicts = gate._conflicts_in_snapshot(snapshot, 100)  # noqa: SLF001
        self.assertEqual([row[0] for row in conflicts], [200, 201])

        process = subprocess.Popen(
            ["bash", "-c", "exec -a cargo sleep 0.2"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.addCleanup(lambda: process.poll() is not None or process.terminate())
        short_snapshot = {process.pid: (1, "sleep", "cargo 0.2")}
        parent = gate._ProcessIdentity(999_999_999, 1, "S", 123)  # noqa: SLF001
        with (
            mock.patch.object(gate, "_process_snapshot", return_value=short_snapshot),
            mock.patch.object(gate, "_require_live_process_identity", return_value=parent),
        ):
            with self.assertRaisesRegex(gate.GateError, str(process.pid)):
                gate.check_current_conflicts(999_999_999)
        process.wait(timeout=2)

    def test_owned_tree_termination_signals_deepest_live_identity_first(self) -> None:
        identities = {
            100: gate._ProcessIdentity(100, 1, "S", 1_000),  # noqa: SLF001
            101: gate._ProcessIdentity(101, 100, "S", 1_001),  # noqa: SLF001
            102: gate._ProcessIdentity(102, 101, "R", 1_002),  # noqa: SLF001
            103: gate._ProcessIdentity(103, 100, "D", 1_003),  # noqa: SLF001
        }
        live = dict(identities)
        signals: list[tuple[int, object]] = []

        def fake_kill(pid: int, sig: object) -> None:
            signals.append((pid, sig))
            live.pop(pid, None)

        with (
            mock.patch.object(gate, "_process_identity_snapshot", return_value=identities),
            mock.patch.object(gate, "_read_process_identity", side_effect=lambda pid: live.get(pid)),
            mock.patch.object(gate.os, "getpid", return_value=999),
            mock.patch.object(gate.os, "kill", side_effect=fake_kill),
        ):
            gate._terminate_process_tree(100, identities[100])  # noqa: SLF001

        self.assertEqual(
            signals,
            [
                (102, gate.signal.SIGTERM),
                (103, gate.signal.SIGTERM),
                (101, gate.signal.SIGTERM),
                (100, gate.signal.SIGTERM),
            ],
        )

    def test_owned_tree_termination_never_signals_zombie_or_dead_state(self) -> None:
        for state in ("Z", "X", "x"):
            with self.subTest(state=state):
                identity = gate._ProcessIdentity(100, 1, state, 1_000)  # noqa: SLF001
                with (
                    mock.patch.object(
                        gate, "_process_identity_snapshot", return_value={100: identity}
                    ),
                    mock.patch.object(
                        gate, "_read_process_identity", return_value=identity
                    ),
                    mock.patch.object(gate.os, "getpid", return_value=999),
                    mock.patch.object(gate.os, "kill") as kill,
                ):
                    gate._terminate_process_tree(100, identity)  # noqa: SLF001
                kill.assert_not_called()

    def test_owned_tree_termination_accepts_process_disappearance(self) -> None:
        identity = gate._ProcessIdentity(100, 1, "S", 1_000)  # noqa: SLF001
        with (
            mock.patch.object(
                gate, "_process_identity_snapshot", return_value={100: identity}
            ),
            mock.patch.object(gate, "_read_process_identity", return_value=None),
            mock.patch.object(gate.os, "getpid", return_value=999),
            mock.patch.object(gate.os, "kill") as kill,
        ):
            gate._terminate_process_tree(100, identity)  # noqa: SLF001
        kill.assert_not_called()

    def test_owned_tree_termination_rejects_pid_reuse_after_term(self) -> None:
        captured = gate._ProcessIdentity(100, 1, "S", 1_000)  # noqa: SLF001
        reused = gate._ProcessIdentity(100, 1, "S", 2_000)  # noqa: SLF001
        observations = iter((captured, captured, reused, reused, reused, reused))
        with (
            mock.patch.object(
                gate, "_process_identity_snapshot", return_value={100: captured}
            ),
            mock.patch.object(
                gate, "_read_process_identity", side_effect=lambda _pid: next(observations)
            ),
            mock.patch.object(gate.os, "getpid", return_value=999),
            mock.patch.object(gate.os, "kill") as kill,
        ):
            with self.assertRaisesRegex(gate.GateError, "PID identity changed"):
                gate._terminate_process_tree(100, captured)  # noqa: SLF001
        kill.assert_called_once_with(100, gate.signal.SIGTERM)

    def test_capacity_root_liveness_rejects_reused_pid_identity(self) -> None:
        captured = gate._ProcessIdentity(100, 1, "S", 1_000)  # noqa: SLF001
        reused = gate._ProcessIdentity(100, 1, "S", 2_000)  # noqa: SLF001
        with mock.patch.object(gate, "_read_process_identity", return_value=reused):
            with self.assertRaisesRegex(gate.GateError, "refusing to follow reused"):
                gate._process_identity_is_running(  # noqa: SLF001
                    captured, "capacity monitor root"
                )

    def test_controlled_root_liveness_rejects_parent_change(self) -> None:
        captured = gate._ProcessIdentity(100, 10, "S", 1_000)  # noqa: SLF001
        reparented = gate._ProcessIdentity(100, 1, "S", 1_000)  # noqa: SLF001
        with mock.patch.object(gate, "_read_process_identity", return_value=reparented):
            with self.assertRaisesRegex(gate.GateError, "captured_ppid=10"):
                gate._process_identity_is_running(captured, "held replay root")  # noqa: SLF001

    def test_descendant_reparent_after_term_remains_safe_kill_target(self) -> None:
        root = gate._ProcessIdentity(100, 10, "S", 1_000)  # noqa: SLF001
        child = gate._ProcessIdentity(101, 100, "S", 1_001)  # noqa: SLF001
        live = {100: root, 101: child}
        signals: list[tuple[int, object]] = []

        def fake_kill(pid: int, sent_signal: object) -> None:
            signals.append((pid, sent_signal))
            if sent_signal == gate.signal.SIGTERM and pid == 101:
                live[101] = gate._ProcessIdentity(101, 1, "S", 1_001)  # noqa: SLF001
            else:
                live.pop(pid, None)

        with (
            mock.patch.object(
                gate, "_process_identity_snapshot", return_value={100: root, 101: child}
            ),
            mock.patch.object(
                gate, "_read_process_identity", side_effect=lambda pid: live.get(pid)
            ),
            mock.patch.object(gate.os, "getpid", return_value=999),
            mock.patch.object(gate.os, "kill", side_effect=fake_kill),
            mock.patch.object(gate.time, "monotonic", side_effect=(0.0, 2.0, 3.0, 4.0)),
        ):
            gate._terminate_process_tree(100, root)  # noqa: SLF001
        self.assertEqual(
            signals,
            [
                (101, gate.signal.SIGTERM),
                (100, gate.signal.SIGTERM),
                (101, gate.signal.SIGKILL),
            ],
        )

    def test_atomic_replay_control_and_markers_reject_mutation_or_partial_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            control, rss_ready, capacity_ready, launch = replay_monitor_fixture(root)
            value = json.loads(control.read_text())
            del value["capacity_monitor_starttime_ticks"]
            control.chmod(0o644)
            control.write_text(json.dumps(value), encoding="utf-8")
            control.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "partial shape"):
                gate._validate_replay_monitor_control(  # noqa: SLF001
                    control, rss_ready, capacity_ready, launch, 100
                )
            with mock.patch.object(gate.os, "kill") as kill:
                with self.assertRaisesRegex(gate.GateError, "partial shape"):
                    gate.cleanup_replay_processes(
                        control, rss_ready, capacity_ready, launch, 100
                    )
                kill.assert_not_called()

            control.unlink()
            control, rss_ready, capacity_ready, launch = replay_monitor_fixture(root)
            rss_ready.chmod(0o644)
            with self.assertRaisesRegex(gate.GateError, "mode 0444"):
                gate._monitor_handshake_evidence(  # noqa: SLF001
                    control,
                    rss_ready,
                    capacity_ready,
                    launch,
                    100,
                    "rss_monitor",
                    1,
                    1,
                    2,
                    100_000_001,
                )

    def test_control_publication_rejects_pid_reuse_without_signalling(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [
                root / "replay-monitor-control.json",
                root / "rss-monitor.ready",
                root / "capacity-monitor.ready",
                root / "replay.launch",
            ]
            reused = gate._ProcessIdentity(100, 10, "S", 9_999)  # noqa: SLF001
            with (
                mock.patch.object(gate, "_read_process_identity", return_value=reused),
                mock.patch.object(gate.os, "kill") as kill,
            ):
                with self.assertRaisesRegex(gate.GateError, "reused"):
                    gate.create_replay_monitor_control(
                        *paths,
                        100,
                        10,
                        1_000,
                        101,
                        10,
                        1_001,
                        102,
                        10,
                        1_002,
                        100,
                    )
            kill.assert_not_called()
            self.assertFalse(paths[0].exists())

    def test_edge_inclusive_cadence_covers_initial_and_terminal_gaps(self) -> None:
        allowed = 200_000_000
        self.assertEqual(
            gate._edge_inclusive_maximum_gap_ns(  # noqa: SLF001
                [allowed, allowed + 1], allowed + 2, "test"
            ),
            allowed,
        )
        self.assertEqual(
            gate._edge_inclusive_maximum_gap_ns(  # noqa: SLF001
                [1, 100_000_000], 300_000_001, "test"
            ),
            200_000_001,
        )

    def test_global_guardian_fails_when_parent_identity_is_lost(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            parent = gate._ProcessIdentity(100, 10, "S", 1_000)  # noqa: SLF001
            with (
                mock.patch.object(
                    gate, "_require_live_process_identity", return_value=parent
                ),
                mock.patch.object(
                    gate, "_process_identity_is_running", return_value=False
                ),
            ):
                with self.assertRaisesRegex(gate.GateError, "parent disappeared"):
                    gate.guard_conflicts(
                        100,
                        root / "stop",
                        root / "conflicts.tsv",
                        100,
                        root,
                        1,
                        root / "samples.tsv",
                        root / "summary.json",
                        root / "ready",
                    )

    def test_global_guardian_never_publishes_ready_after_parent_scan_loss(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready = root / "ready"
            parent = gate._ProcessIdentity(100, 10, "S", 1_000)  # noqa: SLF001
            with (
                mock.patch.object(
                    gate, "_require_live_process_identity", return_value=parent
                ),
                mock.patch.object(
                    gate, "_process_identity_is_running", side_effect=(True, False)
                ),
                mock.patch.object(gate, "_process_snapshot", return_value={}),
            ):
                with self.assertRaisesRegex(gate.GateError, "during its process scan"):
                    gate.guard_conflicts(
                        100,
                        root / "stop",
                        root / "conflicts.tsv",
                        100,
                        root,
                        1,
                        root / "samples.tsv",
                        root / "summary.json",
                        ready,
                    )
            self.assertFalse(ready.exists())

    def test_global_guardian_rejects_reused_parent_without_signalling_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            parent = gate._ProcessIdentity(100, 10, "S", 1_000)  # noqa: SLF001
            reused = gate._ProcessIdentity(100, 10, "S", 2_000)  # noqa: SLF001
            with (
                mock.patch.object(
                    gate, "_require_live_process_identity", return_value=parent
                ),
                mock.patch.object(gate, "_read_process_identity", return_value=reused),
                mock.patch.object(gate.os, "kill") as kill,
            ):
                with self.assertRaisesRegex(gate.GateError, "reused"):
                    gate.guard_conflicts(
                        100,
                        root / "stop",
                        root / "conflicts.tsv",
                        100,
                        root,
                        1,
                        root / "samples.tsv",
                        root / "summary.json",
                        root / "ready",
                    )
            kill.assert_not_called()

    def test_runner_holds_replay_until_both_monitors_and_seals_handshake(self) -> None:
        runner = Path(gate.__file__).with_name("phase6_codec_ab_run.sh").read_text()
        self.assertIn("python3_background() {", runner)
        self.assertIn('exec "$PYTHON_BIN" -I -S -B "$@"', runner)
        self.assertIn("verify_background_python_pid_binding", runner)
        for command in ("guard-conflicts", "monitor-rss", "monitor-capacity"):
            self.assertIn(f'python3_background "$FROZEN_GATE" {command}', runner)
            self.assertNotIn(f'python3 "$FROZEN_GATE" {command}', runner)
        replay = runner.index("run_replay()")
        held = runner.index('while [[ ! -e "$launch"', replay)
        rss = runner.index('monitor-rss \\\n', held)
        capacity = runner.index('monitor-capacity \\\n', rss)
        ready = runner.index("wait-replay-monitors-ready", capacity)
        release = runner.index("release-replay-launch", ready)
        self.assertLess(held, rss)
        self.assertLess(rss, capacity)
        self.assertLess(capacity, ready)
        self.assertLess(ready, release)
        for artifact in (
            "replay-monitor-control.json",
            "rss-monitor.ready",
            "capacity-monitor.ready",
            "replay.launch",
        ):
            self.assertGreaterEqual(runner.count(f'--file "$run_dir/{artifact}"'), 1)
        self.assertIn("cleanup_active_replay", runner)
        self.assertIn("bounded_reap_job", runner)

    def test_capacity_contract_derives_the_exact_pinned_bounds(self) -> None:
        _repo, _expectations, _head, contract = pinned_capacity_contract()
        self.assertEqual(contract["derivation"]["float_points"], 141_374_001)
        self.assertEqual(
            contract["derivation"]["corpus_bound_bytes"]["raw"], 6_700_306_904
        )
        self.assertEqual(contract["initial_required_free_bytes"], 86_198_590_644)

    def test_initial_capacity_one_byte_short_is_rejected(self) -> None:
        _repo, _expectations, _head, contract = pinned_capacity_contract()
        initial = contract["initial_required_free_bytes"]
        with mock.patch.object(
            gate,
            "_capacity_free_bytes",
            return_value=(Path("/capacity-test"), initial - 1, initial * 2),
        ):
            with self.assertRaisesRegex(gate.GateError, "short by 1 bytes"):
                gate.capacity_snapshot(Path("/unused"), initial, "prebuild")

    def test_per_replay_remaining_reserve_one_byte_short_is_rejected(self) -> None:
        _repo, _expectations, _head, contract = pinned_capacity_contract()
        future = contract["postbuild_required_free_bytes"]
        with mock.patch.object(
            gate,
            "_capacity_free_bytes",
            return_value=(Path("/capacity-test"), future - 1, future * 2),
        ):
            with self.assertRaisesRegex(gate.GateError, "short by 1 bytes"):
                gate.capacity_snapshot(
                    Path("/unused"), future, "replay-b01-s01-raw-before"
                )

    def test_corpus_one_byte_over_bound_is_rejected(self) -> None:
        _repo, _expectations, _head, contract = pinned_capacity_contract()
        raw_bound = contract["derivation"]["corpus_bound_bytes"]["raw"]
        with self.assertRaisesRegex(gate.GateError, "exceeds.*by 1 bytes"):
            gate._corpus_capacity_document(raw_bound + 1, raw_bound, "raw")  # noqa: SLF001

    def test_capacity_contract_reconstruction_rejects_mutation(self) -> None:
        repo, expectations, _head, contract = pinned_capacity_contract()
        with tempfile.TemporaryDirectory() as directory:
            metadata = Path(directory) / "metadata"
            harness = metadata / "harness"
            harness.mkdir(parents=True)
            (harness / expectations.name).write_bytes(expectations.read_bytes())
            write_json(metadata / "admission-plan.json", {"repo": str(repo)})
            contract_path = write_json(metadata / "capacity-contract.json", contract)
            self.assertEqual(gate._load_capacity_contract(contract_path), contract)  # noqa: SLF001
            mutated = json.loads(json.dumps(contract))
            mutated["derivation"]["corpus_bound_bytes"]["raw"] += 1
            contract_path.write_text(json.dumps(mutated), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "differs from its frozen facts"):
                gate._load_capacity_contract(contract_path)  # noqa: SLF001

    def test_capacity_and_guardian_cli_dispatches(self) -> None:
        repo = Path(__file__).resolve().parents[3]
        script = Path(gate.__file__).resolve()
        expectations = script.with_name("phase1_4m_expectations.json")
        head = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

        def invoke(*arguments: object) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [sys.executable, "-I", "-S", "-B", str(script), *(str(value) for value in arguments)],
                check=False,
                capture_output=True,
                text=True,
            )

        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            metadata = temporary / "metadata"
            harness = metadata / "harness"
            harness.mkdir(parents=True)
            (harness / expectations.name).write_bytes(expectations.read_bytes())
            write_json(metadata / "admission-plan.json", {"repo": str(repo)})
            contract = metadata / "capacity-contract.json"
            result = invoke(
                "capacity-contract",
                "--expectations",
                expectations,
                "--repo",
                repo,
                "--source-head",
                head,
                "--replay-blocks",
                2,
                "--output",
                contract,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            snapshot = temporary / "snapshot.json"
            result = invoke(
                "capacity-snapshot",
                "--filesystem",
                temporary,
                "--minimum-free-bytes",
                1,
                "--phase",
                "cli-test",
                "--output",
                snapshot,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            summary = write_json(
                temporary / "corpus-summary.json", {"size_bytes": 1}
            )
            corpus_check = temporary / "corpus-check.json"
            result = invoke(
                "check-corpus-capacity",
                "--summary",
                summary,
                "--contract",
                contract,
                "--codec",
                "raw",
                "--output",
                corpus_check,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            control_path = temporary / "replay-monitor-control.json"
            rss_ready = temporary / "rss-monitor.ready"
            capacity_ready = temporary / "capacity-monitor.ready"
            launch = temporary / "replay.launch"
            sleeper = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'while [[ ! -f "$1" ]]; do sleep 0.001; done; exec sleep 0.35',
                    "phase6-held-root",
                    str(launch),
                ]
            )
            self.addCleanup(stop_test_process, sleeper)
            monitor_output = temporary / "capacity-samples.tsv"
            monitor_summary = temporary / "capacity-monitor.json"
            rss_output = temporary / "rss-samples.tsv"
            rss_summary = temporary / "rss.json"
            common = [
                "--pid", str(sleeper.pid),
                "--interval-ms", "100",
                "--control", str(control_path),
                "--rss-ready", str(rss_ready),
                "--capacity-ready", str(capacity_ready),
                "--launch", str(launch),
            ]
            rss_process = subprocess.Popen(
                [
                    sys.executable, "-I", "-S", "-B", str(script), "monitor-rss",
                    *common,
                    "--output", str(rss_output),
                    "--summary", str(rss_summary),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.addCleanup(stop_test_process, rss_process)
            capacity_process = subprocess.Popen(
                [
                    sys.executable, "-I", "-S", "-B", str(script), "monitor-capacity",
                    *common,
                    "--filesystem", str(temporary),
                    "--minimum-free-bytes", "1",
                    "--output", str(monitor_output),
                    "--summary", str(monitor_summary),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.addCleanup(stop_test_process, capacity_process)
            root_identity = gate._require_live_process_identity(  # noqa: SLF001
                sleeper.pid, "test held root"
            )
            rss_identity = gate._require_live_process_identity(  # noqa: SLF001
                rss_process.pid, "test RSS monitor"
            )
            capacity_identity = gate._require_live_process_identity(  # noqa: SLF001
                capacity_process.pid, "test capacity monitor"
            )
            result = invoke(
                "create-replay-monitor-control",
                "--root-pid", sleeper.pid,
                "--root-ppid", root_identity.ppid,
                "--root-starttime-ticks", root_identity.starttime,
                "--rss-pid", rss_process.pid,
                "--rss-ppid", rss_identity.ppid,
                "--rss-starttime-ticks", rss_identity.starttime,
                "--capacity-pid", capacity_process.pid,
                "--capacity-ppid", capacity_identity.ppid,
                "--capacity-starttime-ticks", capacity_identity.starttime,
                "--interval-ms", 100,
                "--rss-ready", rss_ready,
                "--capacity-ready", capacity_ready,
                "--launch", launch,
                "--output", control_path,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            result = invoke(
                "wait-replay-monitors-ready",
                "--control", control_path,
                "--rss-ready", rss_ready,
                "--capacity-ready", capacity_ready,
                "--launch", launch,
                "--interval-ms", 100,
                "--timeout-ms", 5000,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            result = invoke(
                "release-replay-launch",
                "--control", control_path,
                "--rss-ready", rss_ready,
                "--capacity-ready", capacity_ready,
                "--launch", launch,
                "--interval-ms", 100,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(sleeper.wait(timeout=2), 0)
            rss_stdout, rss_stderr = rss_process.communicate(timeout=2)
            capacity_stdout, capacity_stderr = capacity_process.communicate(timeout=2)
            self.assertEqual(rss_process.returncode, 0, rss_stdout + rss_stderr)
            self.assertEqual(
                capacity_process.returncode, 0, capacity_stdout + capacity_stderr
            )

            precheck = temporary / "precheck.json"
            result = invoke(
                "check-current-conflicts", "--parent-pid", 1, "--output", precheck
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            stop = temporary / "guardian.stop"
            conflicts = temporary / "guardian-conflicts.tsv"
            samples = temporary / "guardian-samples.tsv"
            guardian_summary = temporary / "guardian.json"
            ready = temporary / "guardian.ready"
            guardian_process = subprocess.Popen(
                [
                    sys.executable, "-I", "-S", "-B", str(script),
                    "guard-conflicts", "--parent-pid", "1",
                    "--stop-file", str(stop), "--output", str(conflicts),
                    "--interval-ms", "100", "--filesystem", str(temporary),
                    "--minimum-free-bytes", "1", "--samples", str(samples),
                    "--summary", str(guardian_summary), "--ready-file", str(ready),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.addCleanup(stop_test_process, guardian_process)
            for _attempt in range(200):
                if ready.exists():
                    break
                self.assertIsNone(guardian_process.poll())
                time.sleep(0.01)
            gate._create_empty_read_only_marker(stop, "test guardian stop")  # noqa: SLF001
            guardian_stdout, guardian_stderr = guardian_process.communicate(timeout=2)
            self.assertEqual(
                guardian_process.returncode, 0, guardian_stdout + guardian_stderr
            )
            self.assertEqual(ready.read_bytes(), b"")
            self.assertEqual(json.loads(guardian_summary.read_text())["samples"], 2)

    def test_capacity_and_guardian_heartbeats_reject_over_gap_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            control, rss_ready, capacity_ready, launch = replay_monitor_fixture(root)
            capacity = root / "capacity.tsv"
            capacity.write_text(
                "event\telapsed_ns\troot_pid\troot_starttime_ticks\tfree_bytes\tlaunch_observed\n"
                "sample\t1\t100\t1000\t1000\tfalse\n"
                "sample\t200000002\t100\t1000\t1000\ttrue\n"
                "terminal\t200000002\t100\t1000\t1000\ttrue\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "capacity monitor cadence"):
                gate._capacity_summary_from_samples(  # noqa: SLF001
                    capacity,
                    root,
                    1,
                    100,
                    control,
                    rss_ready,
                    capacity_ready,
                    launch,
                )

            guardian = root / "guardian.tsv"
            guardian.write_text(
                "event\telapsed_ns\tparent_pid\tparent_ppid\tparent_starttime_ticks\tfree_bytes\tprocess_count\n"
                "sample\t1\t123\t1\t456\t1000\t10\n"
                "sample\t200000002\t123\t1\t456\t1000\t10\n"
                "terminal\t200000002\t123\t1\t456\t1000\t10\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "guardian cadence"):
                gate._guardian_summary_from_samples(  # noqa: SLF001
                    guardian, 123, root, 1, 100
                )

            terminal_gap = root / "guardian-terminal-gap.tsv"
            terminal_gap.write_text(
                "event\telapsed_ns\tparent_pid\tparent_ppid\tparent_starttime_ticks\tfree_bytes\tprocess_count\n"
                "sample\t1\t123\t1\t456\t1000\t10\n"
                "sample\t100000000\t123\t1\t456\t1000\t10\n"
                "terminal\t300000001\t123\t1\t456\t1000\t10\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "guardian cadence"):
                gate._guardian_summary_from_samples(  # noqa: SLF001
                    terminal_gap, 123, root, 1, 100
                )

    def test_fast_replay_terminal_retains_launch_but_is_not_admitted(self) -> None:
        script = Path(gate.__file__).resolve()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            control = root / "replay-monitor-control.json"
            rss_ready = root / "rss-monitor.ready"
            capacity_ready = root / "capacity-monitor.ready"
            launch = root / "replay.launch"
            rss_output = root / "rss-samples.tsv"
            rss_summary = root / "rss.json"
            capacity_output = root / "capacity-samples.tsv"
            capacity_summary = root / "capacity.json"
            measured = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'while [[ ! -e "$1" ]]; do sleep 0.001; done; exec true',
                    "phase6-fast-root",
                    str(launch),
                ]
            )
            common = [
                "--pid",
                str(measured.pid),
                "--interval-ms",
                "100",
                "--control",
                str(control),
                "--rss-ready",
                str(rss_ready),
                "--capacity-ready",
                str(capacity_ready),
                "--launch",
                str(launch),
            ]
            rss_monitor = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    "-B",
                    str(script),
                    "monitor-rss",
                    *common,
                    "--output",
                    str(rss_output),
                    "--summary",
                    str(rss_summary),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            capacity_monitor = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-S",
                    "-B",
                    str(script),
                    "monitor-capacity",
                    *common,
                    "--filesystem",
                    str(root),
                    "--minimum-free-bytes",
                    "1",
                    "--output",
                    str(capacity_output),
                    "--summary",
                    str(capacity_summary),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                measured_identity = gate._require_live_process_identity(  # noqa: SLF001
                    measured.pid, "test fast held root"
                )
                rss_identity = gate._require_live_process_identity(  # noqa: SLF001
                    rss_monitor.pid, "test fast RSS monitor"
                )
                capacity_identity = gate._require_live_process_identity(  # noqa: SLF001
                    capacity_monitor.pid, "test fast capacity monitor"
                )
                gate.create_replay_monitor_control(
                    control,
                    rss_ready,
                    capacity_ready,
                    launch,
                    measured_identity.pid,
                    measured_identity.ppid,
                    measured_identity.starttime,
                    rss_identity.pid,
                    rss_identity.ppid,
                    rss_identity.starttime,
                    capacity_identity.pid,
                    capacity_identity.ppid,
                    capacity_identity.starttime,
                    100,
                )
                gate.wait_replay_monitors_ready(
                    control, rss_ready, capacity_ready, launch, 100, 5_000
                )
                gate.release_replay_launch(
                    control, rss_ready, capacity_ready, launch, 100
                )
                self.assertEqual(measured.wait(timeout=3), 0)
                rss_stdout, rss_stderr = rss_monitor.communicate(timeout=3)
                capacity_stdout, capacity_stderr = capacity_monitor.communicate(
                    timeout=3
                )
                self.assertNotEqual(
                    rss_monitor.returncode, 0, rss_stdout + rss_stderr
                )
                self.assertNotEqual(
                    capacity_monitor.returncode,
                    0,
                    capacity_stdout + capacity_stderr,
                )
                self.assertIn("fewer than two cadence samples", rss_stderr)
                self.assertIn("fewer than two cadence samples", capacity_stderr)
                for samples in (rss_output, capacity_output):
                    lines = samples.read_text(encoding="utf-8").splitlines()
                    self.assertEqual(
                        [line.split("\t", 1)[0] for line in lines],
                        ["event", "sample", "terminal"],
                    )
                    self.assertTrue(lines[-1].endswith("\ttrue"))
                self.assertFalse(rss_summary.exists())
                self.assertFalse(capacity_summary.exists())
            finally:
                stop_test_process(measured)
                stop_test_process(rss_monitor)
                stop_test_process(capacity_monitor)

    def test_replay_monitor_sample_boundary_root_exit_becomes_terminal(self) -> None:
        for monitor_kind in ("rss", "capacity"):
            with self.subTest(monitor=monitor_kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                control_path = root / "control.json"
                control_path.write_bytes(b"sealed\n")
                rss_ready = root / "rss.ready"
                capacity_ready = root / "capacity.ready"
                launch = root / "launch"
                output = root / "samples.tsv"
                summary = root / "summary.json"
                root_identity = gate._ProcessIdentity(  # noqa: SLF001
                    100, 10, "S", 1_000
                )
                owned_root = gate._OwnedProcessIdentity(  # noqa: SLF001
                    100, 10, "S", 1_000, 0
                )
                control = {
                    "schema": gate.REPLAY_MONITOR_CONTROL_SCHEMA,
                    "root_pid": 100,
                    "root_ppid": 10,
                    "root_starttime_ticks": 1_000,
                    "rss_monitor_pid": 101,
                    "rss_monitor_ppid": 10,
                    "rss_monitor_starttime_ticks": 1_001,
                    "capacity_monitor_pid": 102,
                    "capacity_monitor_ppid": 10,
                    "capacity_monitor_starttime_ticks": 1_002,
                    "interval_ms": 100,
                    "rss_ready_marker": str(rss_ready),
                    "capacity_ready_marker": str(capacity_ready),
                    "launch_marker": str(launch),
                }
                root_checks = 0
                sample_boundary_checks = 0

                def process_is_running(
                    _identity: object, description: str
                ) -> bool:
                    nonlocal root_checks, sample_boundary_checks
                    if description in {
                        "RSS monitor root",
                        "capacity monitor root",
                    }:
                        root_checks += 1
                        return root_checks <= 2
                    if description.endswith("root at sample boundary"):
                        sample_boundary_checks += 1
                        if sample_boundary_checks == 1:
                            gate._create_empty_read_only_marker(  # noqa: SLF001
                                launch, "test replay launch marker"
                            )
                            return True
                        return False
                    if "monitor peer" in description:
                        return True
                    raise AssertionError(f"unexpected liveness check: {description}")

                common_patches = (
                    mock.patch.object(
                        gate,
                        "_require_live_process_identity",
                        return_value=root_identity,
                    ),
                    mock.patch.object(
                        gate,
                        "_validate_replay_monitor_control",
                        return_value=control,
                    ),
                    mock.patch.object(
                        gate,
                        "_process_identity_is_running",
                        side_effect=process_is_running,
                    ),
                    mock.patch.object(gate, "_terminate_process_tree"),
                )
                with (
                    common_patches[0],
                    common_patches[1],
                    common_patches[2],
                    common_patches[3] as terminate,
                ):
                    if monitor_kind == "rss":
                        metrics = {
                            "VmRSS": 10,
                            "VmHWM": 11,
                            "RssAnon": 8,
                            "RssFile": 2,
                            "VmSwap": 0,
                        }
                        with (
                            mock.patch.object(
                                gate,
                                "_capture_owned_process_tree",
                                return_value=(owned_root,),
                            ),
                            mock.patch.object(
                                gate,
                                "_status_kib_for_identity",
                                return_value=metrics,
                            ),
                            self.assertRaisesRegex(
                                gate.GateError, "fewer than two cadence samples"
                            ),
                        ):
                            gate.monitor_rss(
                                100,
                                output,
                                summary,
                                100,
                                control_path,
                                rss_ready,
                                capacity_ready,
                                launch,
                            )
                    else:
                        with (
                            mock.patch.object(
                                gate,
                                "_capacity_free_bytes",
                                return_value=(root, 1_000, 2_000),
                            ),
                            self.assertRaisesRegex(
                                gate.GateError, "fewer than two cadence samples"
                            ),
                        ):
                            gate.monitor_capacity(
                                100,
                                root,
                                1,
                                100,
                                output,
                                summary,
                                control_path,
                                rss_ready,
                                capacity_ready,
                                launch,
                            )
                events = [
                    line.split("\t", 1)[0]
                    for line in output.read_text(encoding="utf-8").splitlines()
                ]
                self.assertEqual(events, ["event", "sample", "terminal"])
                self.assertTrue(
                    output.read_text(encoding="utf-8")
                    .splitlines()[-1]
                    .endswith("\ttrue")
                )
                self.assertEqual(sample_boundary_checks, 2)
                self.assertFalse(summary.exists())
                terminate.assert_called_once_with(100, root_identity)

    def test_replay_monitors_still_fail_for_dead_peer_while_root_is_live(self) -> None:
        for monitor_kind in ("rss", "capacity"):
            with self.subTest(monitor=monitor_kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                control_path = root / "control.json"
                control_path.write_bytes(b"sealed\n")
                rss_ready = root / "rss.ready"
                capacity_ready = root / "capacity.ready"
                launch = root / "launch"
                output = root / "samples.tsv"
                summary = root / "summary.json"
                root_identity = gate._ProcessIdentity(  # noqa: SLF001
                    100, 10, "S", 1_000
                )
                control = {
                    "schema": gate.REPLAY_MONITOR_CONTROL_SCHEMA,
                    "root_pid": 100,
                    "root_ppid": 10,
                    "root_starttime_ticks": 1_000,
                    "rss_monitor_pid": 101,
                    "rss_monitor_ppid": 10,
                    "rss_monitor_starttime_ticks": 1_001,
                    "capacity_monitor_pid": 102,
                    "capacity_monitor_ppid": 10,
                    "capacity_monitor_starttime_ticks": 1_002,
                    "interval_ms": 100,
                    "rss_ready_marker": str(rss_ready),
                    "capacity_ready_marker": str(capacity_ready),
                    "launch_marker": str(launch),
                }
                failed_peer = (
                    "capacity_monitor" if monitor_kind == "rss" else "rss_monitor"
                )

                def process_is_running(
                    _identity: object, description: str
                ) -> bool:
                    if description.endswith(f"peer {failed_peer}"):
                        return False
                    return True

                with (
                    mock.patch.object(
                        gate,
                        "_require_live_process_identity",
                        return_value=root_identity,
                    ),
                    mock.patch.object(
                        gate,
                        "_validate_replay_monitor_control",
                        return_value=control,
                    ),
                    mock.patch.object(
                        gate,
                        "_process_identity_is_running",
                        side_effect=process_is_running,
                    ),
                    mock.patch.object(gate, "_terminate_process_tree") as terminate,
                    self.assertRaisesRegex(
                        gate.GateError, rf"peer {failed_peer} exited early"
                    ),
                ):
                    if monitor_kind == "rss":
                        gate.monitor_rss(
                            100,
                            output,
                            summary,
                            100,
                            control_path,
                            rss_ready,
                            capacity_ready,
                            launch,
                        )
                    else:
                        gate.monitor_capacity(
                            100,
                            root,
                            1,
                            100,
                            output,
                            summary,
                            control_path,
                            rss_ready,
                            capacity_ready,
                            launch,
                        )
                self.assertEqual(
                    output.read_text(encoding="utf-8").splitlines(),
                    [
                        (
                            "event\telapsed_ns\trecorded_at_ns\troot_pid\t"
                            "root_starttime_ticks\tprocess_count\trss_kib\t"
                            "rss_anon_kib\trss_file_kib\tvm_swap_kib\t"
                            "max_single_hwm_kib\tpids\tlaunch_observed"
                            if monitor_kind == "rss"
                            else "event\telapsed_ns\troot_pid\troot_starttime_ticks\tfree_bytes\tlaunch_observed"
                        )
                    ],
                )
                terminate.assert_called_once_with(100, root_identity)

    def test_render_config_changes_only_controlled_codec_and_paths(self) -> None:
        template_text = """
[ingestion]
replay_from = "/old"
stop_after_messages = 1
capture_only = false

[ingestion.head_buffer]
enabled = true
float_encoding = "gorilla"

[ingestion.segment_writer]
enabled = true
segments_dir = "/old/segments"
float_encoding = "gorilla"
storage_schema = "schema8"
deterministic_id_seed = 42
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture = root / "capture"
            capture.mkdir()
            template = root / "template.toml"
            template.write_text(template_text, encoding="utf-8")
            raw = gate.render_config(
                template, root / "raw.toml", capture, root / "raw-segments", 10, "raw"
            )
            gorilla = gate.render_config(
                template,
                root / "gorilla.toml",
                capture,
                root / "gorilla-segments",
                10,
                "gorilla",
            )
            self.assertEqual(raw["controlled_config_sha256"], gorilla["controlled_config_sha256"])
            self.assertIn('float_encoding = "raw"', (root / "raw.toml").read_text())
            self.assertIn('float_encoding = "gorilla"', (root / "gorilla.toml").read_text())

    def test_parse_seal_log_requires_head_window_telemetry(self) -> None:
        line = (
            "INFO Head window written datapoints=4 series=2 elapsed_ms=10 "
            "seal_decode_ms=2 record_samples_ms=3 record_wall_ms=3 "
            "record_chunk_append_ms=1 record_chunks=2 record_profile_samples=4 "
            "writer_flush_ms=4"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "replay.log"
            log.write_text(line + "\n", encoding="utf-8")
            output = root / "seal.json"
            gate.parse_seal_log(log, output)
            parsed = json.loads(output.read_text())
            self.assertEqual(parsed["head_windows"]["totals"]["writer_flush_ms"], 4)
            self.assertFalse(parsed["segment_stage_telemetry_available"])
            empty = root / "empty.log"
            empty.write_text("nothing\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "no Head window"):
                gate.parse_seal_log(empty, root / "unused.json")

    def test_verifier_gate_reconciles_candidates_and_blocks_timestamp_runtime_claim(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = write_json(root / "raw.json", verifier("raw"))
            gorilla = write_json(root / "gorilla.json", verifier("gorilla"))
            output = root / "comparison.json"
            gate.compare_verifiers(raw, gorilla, output)
            result = json.loads(output.read_text())
            self.assertEqual(result["status"], "pass")
            self.assertEqual(
                result["timestamp_runtime_ab_status"],
                "blocked_no_versioned_writer_or_reader_selector",
            )
            broken = verifier("gorilla")
            broken["chunk_inventory"]["timestamp_candidates"]["selector_bytes_included"] = True
            write_json(root / "broken.json", broken)
            with self.assertRaisesRegex(gate.GateError, "selector bytes"):
                gate.compare_verifiers(raw, root / "broken.json", root / "unused.json")

    def test_verifier_gate_rejects_inconsistent_inventory_evidence(self) -> None:
        mutations = (
            (
                "series-count",
                lambda value: value.__setitem__("series", value["corpus_series"] + 1),
                "series count differs",
            ),
            (
                "chunks-by-kind",
                lambda value: value.__setitem__("chunks_by_kind", [1, 1, 0, 0, 0]),
                "chunks_by_kind differs from chunk inventory",
            ),
            (
                "logical-chunk-bytes",
                lambda value: value.__setitem__(
                    "logical_chunk_bytes", value["logical_chunk_bytes"] + 1
                ),
                "indexed bytes differ from logical chunk bytes",
            ),
            (
                "histogram",
                lambda value: value["chunk_inventory"]["by_kind_encoding"][0][
                    "point_count_histogram"
                ]["buckets"][0].__setitem__("count", 1),
                "observations do not reconcile",
            ),
            (
                "xor-transition",
                lambda value: value["chunk_inventory"]["raw_f64_vs_gorilla"].__setitem__(
                    "new_window_points", 0
                ),
                "XOR transition counts",
            ),
            (
                "xor-histogram",
                lambda value: value["chunk_inventory"]["raw_f64_vs_gorilla"][
                    "xor_significant_bits_histogram"
                ]["buckets"][0].__setitem__("count", 1),
                "observations do not reconcile",
            ),
            (
                "float-tie-selection",
                lambda value: (
                    value["chunk_inventory"]["raw_f64_vs_gorilla"].__setitem__(
                        "gorilla_wins", winner(1, 2)
                    ),
                    value["chunk_inventory"]["raw_f64_vs_gorilla"].__setitem__(
                        "ties", winner(1, 2)
                    ),
                ),
                "adaptive RawF64 selections",
            ),
            (
                "float-encoding",
                lambda value: (
                    value["chunk_inventory"]["by_kind_encoding"][0].__setitem__(
                        "encoding", "raw_f64"
                    ),
                    value["chunk_inventory"]["by_kind_encoding"][0].__setitem__(
                        "payload_layout", "t0_interleaved_dt_value"
                    ),
                ),
                "Float encodings differ",
            ),
            (
                "timestamp-winners",
                lambda value: value["chunk_inventory"]["timestamp_candidates"]["all_blocks"][
                    "fixed_step_residual_bitpack"
                ].__setitem__("unique_wins", winner(1, 2)),
                "unique timestamp wins and ties",
            ),
            (
                "timestamp-current-bytes",
                lambda value: value["chunk_inventory"]["timestamp_candidates"]["all_blocks"][
                    "current_offset_uleb"
                ].__setitem__("bytes", 21),
                "current timestamp candidate bytes",
            ),
            (
                "timestamp-breakdown-bytes",
                lambda value: value["chunk_inventory"]["timestamp_candidates"][
                    "by_kind_encoding"
                ][0]["evidence"]["fixed_step_residual_bitpack"].__setitem__("bytes", 15),
                "additive timestamp field",
            ),
            (
                "float-header-bytes",
                lambda value: value["chunk_inventory"]["raw_f64_vs_gorilla"].__setitem__(
                    "gorilla_candidate_indexed_bytes", 121
                ),
                "indexed/payload bytes do not reconcile with headers",
            ),
            (
                "duplicate-shape",
                lambda value: value["chunk_inventory"]["timestamp_candidates"]["by_shape"].append(
                    value["chunk_inventory"]["timestamp_candidates"]["by_shape"][0]
                ),
                "duplicate timestamp shape",
            ),
            (
                "duplicate-kind-encoding",
                lambda value: value["chunk_inventory"]["timestamp_candidates"][
                    "by_kind_encoding"
                ].append(
                    value["chunk_inventory"]["timestamp_candidates"]["by_kind_encoding"][0]
                ),
                "duplicate timestamp kind/encoding",
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = write_json(root / "raw.json", verifier("raw"))
            for name, mutate, pattern in mutations:
                with self.subTest(name=name):
                    broken = verifier("gorilla")
                    mutate(broken)
                    path = write_json(root / f"{name}.json", broken)
                    with self.assertRaisesRegex(gate.GateError, pattern):
                        gate.compare_verifiers(raw, path, root / f"{name}-comparison.json")

    def test_replay_gate_enforces_abba_determinism_and_allowed_physical_differences(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            index = root / "index.tsv"
            fields = [
                "label", "block", "slot", "codec", "config_json", "correctness_json",
                "manifest_tsv", "corpus_summary_json", "time_json", "rss_json", "seal_json", "perf_json",
            ]
            rows = []
            for slot, codec in enumerate(("raw", "gorilla", "gorilla", "raw"), 1):
                label = f"run-{slot}-{codec}"
                config = write_json(
                    root / f"{label}-config.json",
                    {"codec": codec, "controlled_config_sha256": "e" * 64},
                )
                correctness = write_json(root / f"{label}-correct.json", {"same": True})
                manifest = root / f"{label}-manifest.tsv"
                with manifest.open("w", newline="", encoding="utf-8") as destination:
                    writer = csv.writer(destination, delimiter="\t", lineterminator="\n")
                    writer.writerow(("sha256", "size_bytes", "path"))
                    writer.writerow((("1" if codec == "raw" else "2") * 64, 10 if codec == "raw" else 8, "seg/chunks.bin"))
                    writer.writerow((("3" if codec == "raw" else "4") * 64, 2, "seg/footer.bin"))
                    writer.writerow(("5" * 64, 7, "seg/symbols.bin"))
                corpus = write_json(
                    root / f"{label}-corpus.json",
                    {
                        "file_count": 3,
                        "size_bytes": 19 if codec == "raw" else 17,
                        "manifest_sha256": ("6" if codec == "raw" else "7") * 64,
                    },
                )
                timing = write_json(
                    root / f"{label}-time.json",
                    {"exit_status": 0, "elapsed": "1.0", "user_seconds": 1.0, "system_seconds": 0.1, "max_rss_kib": 10},
                )
                rss = write_json(root / f"{label}-rss.json", {"aggregate_rss_kib": 11})
                seal = write_json(
                    root / f"{label}-seal.json",
                    {"head_windows": {"totals": {"elapsed_ms": 10, "seal_decode_ms": 2, "record_samples_ms": 3, "writer_flush_ms": 4}}},
                )
                rows.append(
                    {
                        "label": label,
                        "block": 1,
                        "slot": slot,
                        "codec": codec,
                        "config_json": config,
                        "correctness_json": correctness,
                        "manifest_tsv": manifest,
                        "corpus_summary_json": corpus,
                        "time_json": timing,
                        "rss_json": rss,
                        "seal_json": seal,
                        "perf_json": "-",
                    }
                )
            with index.open("w", newline="", encoding="utf-8") as destination:
                writer = csv.DictWriter(destination, fieldnames=fields, delimiter="\t", lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
            output = root / "comparison.json"
            gate.compare_replays(index, 1, output, root / "summary.tsv")
            self.assertEqual(json.loads(output.read_text())["status"], "pass")
            rows[0]["codec"] = "gorilla"
            bad = root / "bad.tsv"
            with bad.open("w", newline="", encoding="utf-8") as destination:
                writer = csv.DictWriter(destination, fieldnames=fields, delimiter="\t", lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaisesRegex(gate.GateError, "ABBA"):
                gate.compare_replays(bad, 1, root / "unused.json", root / "unused.tsv")
            rows[0]["codec"] = "raw"
            rows[3]["slot"] = 1
            duplicate = root / "duplicate.tsv"
            with duplicate.open("w", newline="", encoding="utf-8") as destination:
                writer = csv.DictWriter(destination, fieldnames=fields, delimiter="\t", lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaisesRegex(gate.GateError, "duplicate replay coordinate"):
                gate.compare_replays(
                    duplicate,
                    1,
                    root / "duplicate-comparison.json",
                    root / "duplicate-summary.tsv",
                )

    def test_query_gate_allows_only_logical_byte_stat_difference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = write_json(
                root / "manifest.json",
                {
                    "schema": gate.PHASE6_QUERY_NORMALIZED_SCHEMA,
                    "queries": [
                        {
                            "query_name": "empty",
                            "category": "empty-result-control",
                            "mode": "instant",
                            "start_ms": 0,
                            "end_ms": 1,
                            "step_ms": None,
                            "range_scalar_cache_max_bytes": None,
                            "boundaries": [],
                            "expression": "{missing=\"yes\"}",
                        }
                    ],
                },
            )
            fields = [
                "process_label", "query_name", "category", "mode", "block", "slot", "codec",
                "corpus", "raw_output", "max_rss_kib", "perf_json",
            ]
            rows = []
            for slot, codec in enumerate(("raw", "gorilla", "gorilla", "raw"), 1):
                rows.append(
                    {
                        "process_label": f"q-{slot}",
                        "query_name": "empty",
                        "category": "empty-result-control",
                        "mode": "instant",
                        "block": 1,
                        "slot": slot,
                        "codec": codec,
                        "corpus": str(root / codec),
                        "raw_output": str(root / f"q-{slot}.json"),
                        "max_rss_kib": 10,
                        "perf_json": "-",
                    }
                )
            index = root / "index.tsv"
            with index.open("w", newline="", encoding="utf-8") as destination:
                writer = csv.DictWriter(destination, fieldnames=fields, delimiter="\t", lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)

            stats = {field: 0 for field in gate.query.QUERY_STATS_FIELDS}

            def fake_validate(_row: dict[str, str], _query: dict[str, object], args: argparse.Namespace):
                codec = args.corpus.name
                fingerprint = ("8" if codec == "raw" else "9") * 64
                runs = []
                for run_index in range(3):
                    local_stats = dict(stats)
                    local_stats["bytes_read"] = 1 if codec == "raw" else 2
                    runs.append(
                        {
                            "run_index": run_index,
                            "run_kind": "cold" if run_index == 0 else "warm",
                            "duration_ns": 10,
                            "semantic_fingerprint": "a" * 64,
                            "portable_fingerprint": "b" * 64,
                            "result_series": 0,
                            "result_samples": 0,
                            "stats": local_stats,
                            "payload": {"logical_used_bytes": local_stats["bytes_read"], "physical_reads": 0, "physical_bytes": local_stats["bytes_read"]},
                            "scheduler": {},
                        }
                    )
                return fingerprint, runs

            args = argparse.Namespace(
                index=index,
                manifest=manifest,
                summary=root / "summary.tsv",
                output=root / "comparison.json",
                blocks=1,
                benchmark_repeats=3,
                queue_depth=128,
                label_materialization="demand-driven",
                max_matched_series=1,
                max_projected_series=1,
                max_chunk_reads=1,
                max_bytes_read=1,
                max_samples_decoded=1,
                max_regex_values_examined=1,
            )
            with mock.patch.object(gate.phase3, "validate_raw", side_effect=fake_validate):
                gate.compare_queries(args)
            result = json.loads(args.output.read_text())
            self.assertEqual(result["allowed_query_stats_differences"], ["bytes_read"])

    def test_query_gate_requires_typed_controls_and_equal_logical_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def run_case(
                suffix: str,
                category: str,
                scalar_chunks: int,
                full_chunks: int,
                raw_bytes: int,
                gorilla_bytes: int,
            ) -> argparse.Namespace:
                manifest = write_json(
                    root / f"{suffix}-manifest.json",
                    {
                        "schema": gate.PHASE6_QUERY_NORMALIZED_SCHEMA,
                        "queries": [
                            {
                                "query_name": "typed-control",
                                "category": category,
                                "mode": "instant",
                                "start_ms": 0,
                                "end_ms": 1,
                                "step_ms": None,
                                "range_scalar_cache_max_bytes": None,
                                "boundaries": [],
                                "expression": "typed_metric",
                            }
                        ],
                    },
                )
                fields = [
                    "process_label", "query_name", "category", "mode", "block", "slot",
                    "codec", "corpus", "raw_output", "max_rss_kib", "perf_json",
                ]
                rows = [
                    {
                        "process_label": f"{suffix}-{slot}",
                        "query_name": "typed-control",
                        "category": category,
                        "mode": "instant",
                        "block": 1,
                        "slot": slot,
                        "codec": codec,
                        "corpus": str(root / codec),
                        "raw_output": str(root / f"{suffix}-{slot}.json"),
                        "max_rss_kib": 10,
                        "perf_json": "-",
                    }
                    for slot, codec in enumerate(("raw", "gorilla", "gorilla", "raw"), 1)
                ]
                index = root / f"{suffix}-index.tsv"
                with index.open("w", newline="", encoding="utf-8") as destination:
                    writer = csv.DictWriter(
                        destination, fieldnames=fields, delimiter="\t", lineterminator="\n"
                    )
                    writer.writeheader()
                    writer.writerows(rows)

                stats = {field: 0 for field in gate.query.QUERY_STATS_FIELDS}

                def fake_validate(
                    _row: dict[str, str], _query: dict[str, object], args: argparse.Namespace
                ) -> tuple[str, list[dict[str, object]]]:
                    codec = args.corpus.name
                    local_bytes = raw_bytes if codec == "raw" else gorilla_bytes
                    fingerprint = ("8" if codec == "raw" else "9") * 64
                    runs = []
                    for run_index in range(3):
                        local_stats = dict(stats)
                        local_stats["bytes_read"] = local_bytes
                        local_stats["typed_scalar_chunks_decoded"] = scalar_chunks
                        local_stats["typed_full_chunks_decoded"] = full_chunks
                        runs.append(
                            {
                                "run_index": run_index,
                                "run_kind": "cold" if run_index == 0 else "warm",
                                "duration_ns": 10,
                                "semantic_fingerprint": "a" * 64,
                                "portable_fingerprint": "b" * 64,
                                "result_series": 1,
                                "result_samples": 1,
                                "stats": local_stats,
                                "payload": {
                                    "logical_used_bytes": local_bytes,
                                    "physical_reads": 1,
                                    "physical_bytes": local_bytes,
                                },
                                "scheduler": {},
                            }
                        )
                    return fingerprint, runs

                args = argparse.Namespace(
                    index=index,
                    manifest=manifest,
                    summary=root / f"{suffix}-summary.tsv",
                    output=root / f"{suffix}-comparison.json",
                    blocks=1,
                    benchmark_repeats=3,
                    queue_depth=128,
                    label_materialization="demand-driven",
                    max_matched_series=1,
                    max_projected_series=1,
                    max_chunk_reads=1,
                    max_bytes_read=10,
                    max_samples_decoded=1,
                    max_regex_values_examined=1,
                )
                with mock.patch.object(gate.phase3, "validate_raw", side_effect=fake_validate):
                    gate.compare_queries(args)
                return args

            passing = run_case(
                "scalar-pass", "typed-scalar-projection-control", 1, 0, 5, 5
            )
            comparison = json.loads(passing.output.read_text())
            self.assertTrue(comparison["typed_control_bytes_read_must_match"])
            with self.assertRaisesRegex(gate.GateError, r"QueryStats\.bytes_read"):
                run_case("scalar-bytes", "typed-scalar-projection-control", 1, 0, 5, 6)
            with self.assertRaisesRegex(gate.GateError, "decoded no full typed chunks"):
                run_case("full-missing", "native-histogram-full-control", 0, 0, 5, 5)


if __name__ == "__main__":
    unittest.main()
