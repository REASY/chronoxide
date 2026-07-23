#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock
from pathlib import Path


HERE = Path(__file__).resolve().parent
GATE_PATH = HERE / "phase5_allocator_full_gate.py"
PLAN_PATH = HERE / "phase5_allocator_full_plan.json"
EXPECTATIONS_PATH = HERE / "phase1_4m_expectations.json"

SPEC = importlib.util.spec_from_file_location("phase5_allocator_full_gate", GATE_PATH)
assert SPEC is not None and SPEC.loader is not None
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def inventory_winner(chunks: int = 0, points: int = 0) -> dict[str, int]:
    return {"chunks": chunks, "points": points}


def inventory_histogram(observations: int) -> dict[str, object]:
    return {
        "zero_count": 0,
        "buckets": (
            [{"lower_inclusive": 1, "upper_inclusive": 1, "count": observations}]
            if observations
            else []
        ),
    }


def timestamp_evidence(chunks: int, points: int) -> dict[str, object]:
    def candidate(selected: bool = False) -> dict[str, object]:
        return {
            "bytes": 0,
            "unique_wins": inventory_winner(),
            "adaptive_selections": (
                inventory_winner(chunks, points) if selected else inventory_winner()
            ),
        }

    return {
        "chunks": chunks,
        "points": points,
        "current_offset_uleb": candidate(True),
        "adjacent_delta_uleb": candidate(),
        "delta_of_delta_zigzag_uleb128": candidate(),
        "fixed_step_residual_bitpack": candidate(),
        "adaptive_min_bytes": 0,
        "tied_minima": inventory_winner(chunks, points),
    }


def chunk_inventory(chunks: int, points: int, indexed_bytes: int) -> dict[str, object]:
    payload_bytes = indexed_bytes - chunks * 40
    assert payload_bytes >= 0
    timestamp = timestamp_evidence(chunks, points)
    float_evidence = {
        "tie_rule": "RAW_F64 wins equal payload-byte ties; then compare decode cost",
        "chunks": chunks,
        "points": points,
        "existing_indexed_bytes": indexed_bytes,
        "existing_payload_bytes": payload_bytes,
        "raw_f64_candidate_indexed_bytes": indexed_bytes,
        "raw_f64_candidate_payload_bytes": payload_bytes,
        "gorilla_candidate_indexed_bytes": indexed_bytes,
        "gorilla_candidate_payload_bytes": payload_bytes,
        "adaptive_min_indexed_bytes": indexed_bytes,
        "adaptive_min_payload_bytes": payload_bytes,
        "raw_f64_wins": inventory_winner(),
        "gorilla_wins": inventory_winner(),
        "ties": inventory_winner(chunks, points),
        "adaptive_raw_f64_selections": inventory_winner(chunks, points),
        "adaptive_gorilla_selections": inventory_winner(),
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
    return {
        "layout": "sealed_chunk_v1",
        "by_kind_encoding": [
            {
                "kind": "float",
                "encoding": "gorilla",
                "payload_layout": "t0_dt_then_values",
                "chunks": chunks,
                "points": points,
                "indexed_bytes": indexed_bytes,
                "common_header_bytes": chunks * 40,
                "scalar_lane_bytes": 0,
                "payload_bytes": payload_bytes,
                "timestamp_base_bytes": 0,
                "timestamp_delta_bytes": 0,
                "value_bytes": payload_bytes,
                "point_count_histogram": inventory_histogram(chunks),
                "cadence_ms_histogram": inventory_histogram(points - chunks),
            }
        ],
        "raw_f64_vs_gorilla": float_evidence,
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


def current_storage_report(authority: dict[str, object]) -> dict[str, object]:
    value = copy.deepcopy(authority)
    chunks = value["chunks"]
    samples = value["samples"]
    logical_bytes = value["logical_chunk_bytes"]
    assert isinstance(chunks, int) and isinstance(samples, int)
    assert isinstance(logical_bytes, int)
    value["chunks_by_kind"] = [chunks, 0, 0, 0, 0]
    value["decoded_semantic_fingerprint"] = "e" * 64
    value["chunk_inventory"] = chunk_inventory(chunks, samples, logical_bytes)
    value.update(
        {
            "elapsed_ns": 1,
            "metadata_read_calls": 1,
            "metadata_read_bytes": 1,
            "metadata_peak_retained_bytes": 1,
            "metadata_peak_in_flight_bytes": 1,
            "metadata_peak_open_files": 1,
            "metadata_cache_hits": 0,
            "metadata_cache_misses": 1,
        }
    )
    return value


def copied_plan(root: Path) -> Path:
    path = root / "plan.json"
    path.write_bytes(PLAN_PATH.read_bytes())
    return path


def confirmation(conf: str) -> str:
    lines = [
        '<jemalloc>: malloc_conf #1 (string specified via --with-malloc-conf): ""',
        '<jemalloc>: malloc_conf #2 (string pointed to by the global variable malloc_conf): ""',
        '<jemalloc>: malloc_conf #3 ("name" of the file referenced by the symbolic link named /etc/malloc.conf): ""',
        f'<jemalloc>: malloc_conf #4 (value of the environment variable MALLOC_CONF): "{conf}"',
        '<jemalloc>: malloc_conf #5 (string pointed to by the global variable malloc_conf_2_conf_harder): ""',
    ]
    lines.extend(f"<jemalloc>: -- Set conf value: {entry}" for entry in conf.split(","))
    return "\n".join(lines) + "\n"


def synthetic_observation(stage: str, position: int, cpu: float, rss: int) -> dict[str, object]:
    token = gate.EXPECTED_STAGE_SCHEDULES[stage][position - 1]
    role = "system" if token == "S" else (
        "stats-candidate" if stage == "stats" else "no-stats-candidate"
    )
    return {
        "schema": gate.OBSERVATION_SCHEMA,
        "stage": stage,
        "position": position,
        "schedule_token": token,
        "role": role,
        "selected_policy": "J2",
        "selected_jemalloc_conf": "abort_conf:true,confirm_conf:true,narenas:4",
        "screen_binding_sha256": "a" * 64,
        "no_stats_build_sha256": "b" * 64,
        "correctness": {"messages": 4_000_000},
        "corpus": {"manifest_sha256": "c" * 64},
        "workload_cpu_seconds": cpu,
        "rss": {
            "workload_peak_rss_kib": rss,
            "workload_boundary_max_single_hwm_kib": rss,
            "post_drop_end_rss_kib": rss,
        },
        "production_promotion_authorized": False,
    }


def rss_summary() -> dict[str, object]:
    return {
        "root_pid": 123,
        "interval_ms": 100,
        "clock_ticks_per_second": 100,
        "samples": 22,
        "workload_samples": 1,
        "post_drop_samples": 20,
        "hold_complete_samples": 1,
        "checkpoint_incomplete_samples": 0,
        "peak_rss_kib": 1100,
        "peak_rss_anon_kib": 900,
        "peak_rss_file_kib": 200,
        "peak_vm_swap_kib": 0,
        "peak_process_count": 2,
        "workload_peak_rss_kib": 1000,
        "workload_peak_max_single_hwm_kib": 1000,
        "workload_boundary_max_single_hwm_kib": 1100,
        "post_drop_first_rss_kib": 900,
        "post_drop_min_rss_kib": 800,
        "post_drop_end_rss_kib": 800,
        "post_drop_first_unix_time_ns": 100_050_000_000,
        "post_drop_end_unix_time_ns": 129_900_000_000,
        "workload_boundary_cpu_ticks": 950,
        "workload_boundary_cpu_seconds": 9.5,
        "workload_boundary_sample_window_start_unix_time_ns": 100_000_000_000,
        "workload_boundary_sample_unix_time_ns": 100_050_000_000,
        "root_starttime_ticks": 123_000,
        "elapsed_ns": 150_000_000,
        "poll_monotonic_elapsed_ns": [0, 100_000_000],
        "maximum_poll_start_gap_ns": 100_000_000,
        "maximum_allowed_poll_start_gap_ns": 200_000_000,
        "control_path": "/tmp/external-conflict-guardian-control.json",
        "control_sha256": "d" * 64,
        "rss_ready_marker_path": "/tmp/rss-monitor-ready",
        "rss_ready_marker_sha256": "e" * 64,
        "rss_ready_created_sample": 1,
        "rss_ready_created_monotonic_elapsed_ns": 0,
        "launch_marker_path": "/tmp/external-conflict-guardian-launch",
        "launch_marker_sha256": "f" * 64,
        "launch_observed_sample": 2,
        "launch_observed_monotonic_elapsed_ns": 100_000_000,
        "launch_observed": True,
        "terminal_observation": True,
        "terminal_launch_observed": True,
        "handshake_violations": [],
        "complete": True,
    }


def direct_rss_evidence() -> tuple[list[dict[str, object]], dict[str, object]]:
    rows: list[dict[str, object]] = [
        {
            "elapsed_ns": 0,
            "sample_window_start_unix_time_ns": 99_899_000_000,
            "unix_time_ns": 99_900_000_000,
            "phase": "workload",
            "process_count": 2,
            "process_cpu_ticks": 900,
            "rss_kib": 1_000,
            "rss_anon_kib": 800,
            "rss_file_kib": 200,
            "vm_swap_kib": 0,
            "max_single_hwm_kib": 1_100,
            "pids": "123,124",
        },
        {
            "elapsed_ns": 100_000_000,
            "sample_window_start_unix_time_ns": 100_049_000_000,
            "unix_time_ns": 100_050_000_000,
            "phase": "post_drop_hold",
            "process_count": 2,
            "process_cpu_ticks": 950,
            "rss_kib": 900,
            "rss_anon_kib": 700,
            "rss_file_kib": 200,
            "vm_swap_kib": 0,
            "max_single_hwm_kib": 1_000,
            "pids": "123,124",
        },
        {
            "elapsed_ns": 200_000_000,
            "sample_window_start_unix_time_ns": 129_899_000_000,
            "unix_time_ns": 129_900_000_000,
            "phase": "post_drop_hold",
            "process_count": 2,
            "process_cpu_ticks": 951,
            "rss_kib": 800,
            "rss_anon_kib": 600,
            "rss_file_kib": 200,
            "vm_swap_kib": 0,
            "max_single_hwm_kib": 900,
            "pids": "123,124",
        },
        {
            "elapsed_ns": 300_000_000,
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
        },
    ]
    summary = rss_summary()
    summary.update(
        {
            "samples": 3,
            "workload_samples": 1,
            "post_drop_samples": 2,
            "hold_complete_samples": 0,
            "checkpoint_incomplete_samples": 0,
            "peak_rss_kib": 1_000,
            "peak_rss_anon_kib": 800,
            "peak_rss_file_kib": 200,
            "peak_vm_swap_kib": 0,
            "peak_process_count": 2,
            "workload_peak_rss_kib": 1_000,
            "workload_peak_max_single_hwm_kib": 1_100,
            "workload_boundary_max_single_hwm_kib": 1_000,
            "post_drop_first_rss_kib": 900,
            "post_drop_min_rss_kib": 800,
            "post_drop_end_rss_kib": 800,
            "post_drop_first_unix_time_ns": 100_050_000_000,
            "post_drop_end_unix_time_ns": 129_900_000_000,
            "workload_boundary_cpu_ticks": 950,
            "workload_boundary_sample_window_start_unix_time_ns": 100_049_000_000,
            "workload_boundary_sample_unix_time_ns": 100_050_000_000,
            "elapsed_ns": 350_000_000,
            "poll_monotonic_elapsed_ns": [
                0,
                100_000_000,
                200_000_000,
                300_000_000,
            ],
            "maximum_poll_start_gap_ns": 100_000_000,
            "maximum_allowed_poll_start_gap_ns": 200_000_000,
            "rss_ready_created_sample": 1,
            "rss_ready_created_monotonic_elapsed_ns": 0,
            "launch_observed_sample": 2,
            "launch_observed_monotonic_elapsed_ns": 100_000_000,
            "launch_observed": True,
            "terminal_observation": True,
            "terminal_launch_observed": True,
            "handshake_violations": [],
            "complete": True,
        }
    )
    return rows, summary


def stats_preflight(conf: str) -> dict[str, object]:
    before = 1_000_000
    growth = 50 * 1024 * 1024
    return {
        "schema": gate.APPLICATION_PREFLIGHT_SCHEMA,
        "rust_global_allocator": "jemalloc",
        "jemalloc_conf_env": "_RJEM_MALLOC_CONF",
        "requested_policy_raw": conf,
        "requested_policy_canonical": conf,
        "effective_policy": {
            "abort_conf": True,
            "confirm_conf": True,
            "narenas": 4,
            "dirty_decay_ms": 10000,
            "muzzy_decay_ms": 0,
            "background_thread": True,
            "max_background_threads": 1,
            "retain": True,
        },
        "global_allocator_probe": {
            "status": "passed",
            "allocation_bytes": 64 * 1024 * 1024,
            "minimum_allocated_growth_bytes": 48 * 1024 * 1024,
            "allocated_before_bytes": before,
            "allocated_while_live_bytes": before + growth,
            "allocated_after_drop_bytes": before + 4096,
            "observed_allocated_growth_bytes": growth,
            "passed": True,
        },
        "allocator_internal_telemetry": "fixed_startup_options_and_release_stats",
        "ld_preload_present": False,
        "malloc_conf_present": False,
        "post_ingester_drop_hold_secs": 0,
        "post_ingester_drop_checkpoint_enabled": False,
        "post_ingester_drop_telemetry_enabled": False,
    }


class FullGateTests(unittest.TestCase):
    def test_plan_is_exact_and_contains_no_helper_digest(self) -> None:
        plan = gate.validate_plan(PLAN_PATH)
        self.assertEqual(plan["stages"][0]["schedule"], ["S", "C", "C", "S"])
        self.assertEqual(plan["stages"][1]["schedule"], ["S", "N", "N", "S"])
        self.assertNotIn("sha256", PLAN_PATH.read_text(encoding="utf-8").lower())
        self.assertFalse(plan["completion"]["production_promotion_authorized"])

        with tempfile.TemporaryDirectory() as temporary:
            path = copied_plan(Path(temporary))
            value = json.loads(path.read_text(encoding="utf-8"))
            value["gate"]["minimum_workload_cpu_improvement_percent"] = 2.9
            write_json(path, value)
            with self.assertRaisesRegex(gate.GateError, "thresholds"):
                gate.validate_plan(path)

    def test_quiet_host_classifier_catches_real_world_variants(self) -> None:
        cases = [
            ("bash", "bash /src/build/soong/soong_ui.bash --make-mode"),
            ("cargo-nextest", "cargo-nextest nextest run"),
            ("ninja.real", "/usr/bin/ninja.real -C out"),
            ("ld.bfd", "/usr/bin/ld.bfd -o target"),
            ("gcc-14", "/usr/bin/gcc-14 -c x.c"),
            ("clang-19.real", "/usr/bin/clang-19.real -c x.c"),
            ("clang++.real", "/usr/bin/clang++.real -c x.cc"),
            ("ninja-1.12", "/usr/bin/ninja-1.12 -C out"),
            ("soong_ui.bash", "soong_ui.bash --make-mode"),
            ("ccache", "ccache clang -c x.c"),
            ("valgrind.bin", "valgrind.bin ./server"),
            ("vmstorage", "vmstorage -storageDataPath data"),
            ("javac", "javac Main.java"),
            ("aapt2", "aapt2 compile resources"),
            ("adb", "adb -L tcp:5037 fork-server server"),
            ("btop", "btop"),
            ("docker", "/usr/bin/docker build ."),
            ("docker-buildx", "/usr/libexec/docker/cli-plugins/docker-buildx build ."),
            ("docker-compose", "/usr/libexec/docker/cli-plugins/docker-compose up"),
            ("podman", "/usr/bin/podman build ."),
            ("buildah", "/usr/bin/buildah bud ."),
            ("buildctl", "/usr/bin/buildctl build"),
            ("nerdctl", "/usr/bin/nerdctl build ."),
        ]
        for comm, command in cases:
            with self.subTest(comm=comm):
                self.assertIsNotNone(gate.forbidden_process_reason(comm, command))
        self.assertIsNone(gate.forbidden_process_reason("python3", "python3 full_gate.py"))
        for comm, command in (
            ("dockerd", "/usr/bin/dockerd --host=unix:///run/user/1000/docker.sock"),
            ("buildkitd", "/usr/bin/buildkitd --rootless"),
            ("rootlesskit", "/usr/bin/rootlesskit --net=slirp4netns dockerd"),
            ("containerd", "/usr/bin/containerd --config /etc/containerd/config.toml"),
            ("docker-proxy", "/usr/bin/docker-proxy -proto tcp"),
            ("containerd-shim", "/usr/bin/containerd-shim -namespace moby"),
            (
                "containerd-shim-runc-v1",
                "/usr/bin/containerd-shim-runc-v1 -namespace moby",
            ),
            (
                "containerd-shim-runc-v2",
                "/usr/bin/containerd-shim-runc-v2 -namespace moby",
            ),
            ("buildkitd-report", "/usr/local/bin/buildkitd-report --json"),
            ("rootlesskit-helper", "/usr/local/bin/rootlesskit-helper"),
        ):
            with self.subTest(allowed_comm=comm):
                self.assertIsNone(gate.forbidden_process_reason(comm, command))

    def test_static_scan_and_process_status_require_exact_success(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            scan = root / "scan.json"
            valid = {
                "schema": gate.CONFLICT_SCAN_SCHEMA,
                "conflicts": [],
                "quiet": True,
            }
            write_json(scan, valid)
            self.assertEqual(gate.validate_conflict_scan(scan), valid)

            changed = dict(valid)
            changed["quiet"] = False
            write_json(scan, changed)
            with self.assertRaisesRegex(gate.GateError, "did not pass exactly"):
                gate.validate_conflict_scan(scan)

            changed = dict(valid)
            changed["unexpected"] = True
            write_json(scan, changed)
            with self.assertRaisesRegex(gate.GateError, "keys differ"):
                gate.validate_conflict_scan(scan)

            status = root / "replay.exit-status"
            status.write_bytes(b"0\n")
            gate.validate_zero_exit_status(status, "replay exit status")
            status.write_bytes(b"1\n")
            with self.assertRaisesRegex(gate.GateError, "successful status 0"):
                gate.validate_zero_exit_status(status, "replay exit status")

    def test_stage_comparison_requires_both_stability_and_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = copied_plan(root)
            paths = []
            for position, (cpu, rss) in enumerate(
                ((100.0, 1000), (95.0, 1020), (95.0, 1020), (100.0, 1000)),
                start=1,
            ):
                path = root / f"observation-{position}.json"
                write_json(path, synthetic_observation("stats", position, cpu, rss))
                paths.append(path)
            decision = gate.compare_stage(paths, "stats", plan)
            self.assertTrue(decision["passed"])
            self.assertEqual(decision["workload_cpu_improvement_percent"], 5.0)
            self.assertFalse(decision["production_promotion_authorized"])

            changed = json.loads(paths[2].read_text(encoding="utf-8"))
            changed["workload_cpu_seconds"] = 80.0
            write_json(paths[2], changed)
            unstable = gate.compare_stage(paths, "stats", plan)
            self.assertFalse(unstable["dispersion_pass"])
            self.assertFalse(unstable["passed"])

    def test_stage_comparison_rejects_schedule_or_semantic_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = copied_plan(root)
            paths = []
            for position in range(1, 5):
                path = root / f"observation-{position}.json"
                write_json(path, synthetic_observation("no-stats", position, 100.0, 1000))
                paths.append(path)
            changed = json.loads(paths[1].read_text(encoding="utf-8"))
            changed["schedule_token"] = "C"
            write_json(paths[1], changed)
            with self.assertRaisesRegex(gate.GateError, "schedule"):
                gate.compare_stage(paths, "no-stats", plan)

            write_json(paths[1], synthetic_observation("no-stats", 2, 100.0, 1000))
            changed = json.loads(paths[2].read_text(encoding="utf-8"))
            changed["corpus"]["manifest_sha256"] = "d" * 64
            write_json(paths[2], changed)
            with self.assertRaisesRegex(gate.GateError, "corpus"):
                gate.compare_stage(paths, "no-stats", plan)

    def test_checkpoint_and_raw_rss_cross_check(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            plan = gate.validate_plan(PLAN_PATH)
            checkpoint = root / "checkpoint.tsv"
            checkpoint.write_text(
                "schema\tphase\tmain_elapsed_ns\tunix_time_ns\thold_secs\n"
                f"{gate.CHECKPOINT_SCHEMA}\tingester_dropped\t10000000000\t100000000000\t30\n"
                f"{gate.CHECKPOINT_SCHEMA}\thold_complete\t40000000000\t130000000000\t30\n",
                encoding="utf-8",
            )
            rss_path = root / "rss.json"
            write_json(rss_path, rss_summary())
            parsed = gate.parse_checkpoint(checkpoint, rss_path, plan)
            self.assertEqual(parsed["workload_cpu_seconds"], 9.5)

            changed = rss_summary()
            changed["peak_vm_swap_kib"] = 1
            write_json(rss_path, changed)
            with self.assertRaisesRegex(gate.GateError, "swap"):
                gate.parse_checkpoint(checkpoint, rss_path, plan)

    def test_rss_evidence_rejects_cadence_identity_marker_and_role_mutations(
        self,
    ) -> None:
        rows, summary = direct_rss_evidence()
        gate.cross_check_rss_samples(rows, summary)

        changed_rows = copy.deepcopy(rows)
        changed_summary = copy.deepcopy(summary)
        changed_rows[1]["elapsed_ns"] = 0
        changed_summary["poll_monotonic_elapsed_ns"] = [
            0,
            0,
            200_000_000,
            300_000_000,
        ]
        with self.assertRaisesRegex(gate.GateError, "strictly increasing"):
            gate.cross_check_rss_samples(changed_rows, changed_summary)

        changed_rows = copy.deepcopy(rows)
        changed_summary = copy.deepcopy(summary)
        changed_rows[1]["elapsed_ns"] = 201_000_000
        changed_rows[2]["elapsed_ns"] = 301_000_000
        changed_rows[3]["elapsed_ns"] = 401_000_000
        changed_summary["poll_monotonic_elapsed_ns"] = [
            0,
            201_000_000,
            301_000_000,
            401_000_000,
        ]
        changed_summary["elapsed_ns"] = 451_000_000
        changed_summary["maximum_poll_start_gap_ns"] = 201_000_000
        with self.assertRaisesRegex(gate.GateError, "exactly derived"):
            gate.cross_check_rss_samples(changed_rows, changed_summary)

        changed_summary = copy.deepcopy(summary)
        changed_summary["elapsed_ns"] = 400_000_001
        changed_summary["maximum_poll_start_gap_ns"] = 200_000_001
        with self.assertRaisesRegex(gate.GateError, "exactly derived"):
            gate.cross_check_rss_samples(rows, changed_summary)

        changed_rows = copy.deepcopy(rows)
        changed_rows[0]["pids"] = "124"
        with self.assertRaisesRegex(gate.GateError, "exactly derived"):
            gate.cross_check_rss_samples(changed_rows, summary)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            control = root / "external-conflict-guardian-control.json"
            guardian_ready = root / "external-conflict-guardian-ready"
            rss_ready = root / "rss-monitor-ready"
            launch = root / "external-conflict-guardian-launch"
            control_value = {
                "schema": gate.GUARDIAN_CONTROL_SCHEMA,
                "root_pid": 123,
                "root_starttime_ticks": 123_000,
                "guardian_pid": 456,
                "guardian_starttime_ticks": 456_000,
                "rss_monitor_pid": 789,
                "rss_monitor_starttime_ticks": 789_000,
                "rss_ready_marker": str(rss_ready),
                "interval_ms": 100,
                "ready_marker": str(guardian_ready),
                "launch_marker": str(launch),
            }

            def write_handshake() -> dict[str, object]:
                for path in (control, rss_ready, launch):
                    if path.exists() or path.is_symlink():
                        path.chmod(0o644)
                        path.unlink()
                write_json(control, control_value)
                control.chmod(0o444)
                for marker in (rss_ready, launch):
                    marker.touch()
                    marker.chmod(0o444)
                bound = copy.deepcopy(summary)
                bound.update(
                    {
                        "control_path": str(control),
                        "control_sha256": gate.sha256_file(control),
                        "rss_ready_marker_path": str(rss_ready),
                        "rss_ready_marker_sha256": gate.sha256_file(rss_ready),
                        "launch_marker_path": str(launch),
                        "launch_marker_sha256": gate.sha256_file(launch),
                    }
                )
                return bound

            bound = write_handshake()
            gate.validate_rss_handshake_evidence(
                bound, control, guardian_ready, rss_ready, launch, 100
            )

            changed = write_handshake()
            changed["root_starttime_ticks"] = 123_001
            with self.assertRaisesRegex(gate.GateError, "identities"):
                gate.validate_rss_handshake_evidence(
                    changed, control, guardian_ready, rss_ready, launch, 100
                )

            changed = write_handshake()
            rss_ready.chmod(0o644)
            with self.assertRaisesRegex(gate.GateError, "exact empty mode 0444"):
                gate.validate_rss_handshake_evidence(
                    changed, control, guardian_ready, rss_ready, launch, 100
                )

            changed = write_handshake()
            control.chmod(0o644)
            wrong_role = copy.deepcopy(control_value)
            wrong_role["rss_monitor_pid"] = wrong_role["root_pid"]
            write_json(control, wrong_role)
            control.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "distinct"):
                gate.validate_rss_handshake_evidence(
                    changed, control, guardian_ready, rss_ready, launch, 100
                )

    def test_plain_preflight_requires_no_stats_and_confirmed_policy(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "binary"
            binary.write_bytes(b"plain-jemalloc")
            conf = "abort_conf:true,confirm_conf:true,narenas:4"
            binding = root / "binding.json"
            write_json(
                binding,
                {
                    "selected_policy": "J2",
                    "selected_jemalloc_conf": conf,
                    "binary_sha256": {
                        "system": "1" * 64,
                        "jemalloc": "2" * 64,
                        "query": "3" * 64,
                        "storage_verify": "4" * 64,
                    },
                },
            )
            raw = root / "preflight.json"
            probe = {
                "status": "unavailable_without_jemalloc_stats",
                "allocation_bytes": None,
                "minimum_allocated_growth_bytes": None,
                "allocated_before_bytes": None,
                "allocated_while_live_bytes": None,
                "allocated_after_drop_bytes": None,
                "observed_allocated_growth_bytes": None,
                "passed": None,
            }
            write_json(
                raw,
                {
                    "schema": gate.APPLICATION_PREFLIGHT_SCHEMA,
                    "rust_global_allocator": "jemalloc",
                    "jemalloc_conf_env": "_RJEM_MALLOC_CONF",
                    "requested_policy_raw": None,
                    "requested_policy_canonical": None,
                    "effective_policy": None,
                    "global_allocator_probe": probe,
                    "allocator_internal_telemetry": "unavailable",
                    "ld_preload_present": False,
                    "malloc_conf_present": False,
                    "post_ingester_drop_hold_secs": 0,
                    "post_ingester_drop_checkpoint_enabled": False,
                    "post_ingester_drop_telemetry_enabled": False,
                },
            )
            stderr = root / "preflight.stderr"
            stderr.write_text(confirmation(conf), encoding="utf-8")
            evidence = gate.validate_application_preflight(
                raw, stderr, "no-stats-candidate", binary, binding
            )
            self.assertFalse(evidence["jemalloc_stats_enabled"])

            changed = copy.deepcopy(json.loads(raw.read_text(encoding="utf-8")))
            changed["effective_policy"] = {"narenas": 4}
            write_json(raw, changed)
            with self.assertRaisesRegex(gate.GateError, "compiled stats"):
                gate.validate_application_preflight(
                    raw, stderr, "no-stats-candidate", binary, binding
                )

            write_json(raw, {**changed, "effective_policy": None})
            stderr.write_text(
                confirmation(conf).replace(
                    'malloc_conf #1 (string specified via --with-malloc-conf): ""',
                    'malloc_conf #1 (string specified via --with-malloc-conf): "narenas:99"',
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "sources"):
                gate.validate_application_preflight(
                    raw, stderr, "no-stats-candidate", binary, binding
                )

    def test_stats_preflight_and_runtime_require_full_policy_and_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "binary"
            binary.write_bytes(b"stats-jemalloc")
            conf = "abort_conf:true,confirm_conf:true,narenas:4"
            binding = root / "binding.json"
            write_json(
                binding,
                {
                    "selected_policy": "J2",
                    "selected_jemalloc_conf": conf,
                    "binary_sha256": {
                        "system": "1" * 64,
                        "jemalloc": gate.sha256_file(binary),
                        "query": "3" * 64,
                        "storage_verify": "4" * 64,
                    },
                },
            )
            raw = root / "preflight.json"
            stderr = root / "preflight.stderr"
            write_json(raw, stats_preflight(conf))
            stderr.write_text(confirmation(conf), encoding="utf-8")
            parsed = gate.validate_application_preflight(
                raw, stderr, "stats-candidate", binary, binding
            )
            self.assertEqual(parsed["effective_policy"]["narenas"], 4)

            changed = stats_preflight(conf)
            changed["global_allocator_probe"]["allocation_bytes"] = 1
            write_json(raw, changed)
            with self.assertRaisesRegex(gate.GateError, "allocation probe size"):
                gate.validate_application_preflight(
                    raw, stderr, "stats-candidate", binary, binding
                )

            write_json(raw, stats_preflight(conf))
            runtime = {
                "schema": gate.APPLICATION_RUNTIME_SCHEMA,
                "rust_global_allocator": "jemalloc",
                "jemalloc_conf_env": "_RJEM_MALLOC_CONF",
                "requested_policy_raw": conf,
                "requested_policy_canonical": conf,
                "effective_policy": parsed["effective_policy"],
                "post_ingester_drop_hold_secs": 30,
                "post_ingester_drop_checkpoint_enabled": True,
                "post_ingester_drop_telemetry_enabled": True,
            }
            runtime_path = root / "runtime.log"
            runtime_path.write_text(
                "CHRONOXIDE_ALLOCATOR_RUNTIME_POLICY_JSON="
                + json.dumps(runtime, separators=(",", ":"))
                + "\n"
                + confirmation(conf)
                + "Ingester state dropped; beginning diagnostic allocator release hold\n"
                + "Diagnostic allocator release hold complete\n",
                encoding="utf-8",
            )
            gate.parse_runtime_log(
                runtime_path, "stats-candidate", json.loads(binding.read_text()), parsed,
                gate.validate_plan(PLAN_PATH)
            )
            runtime_path.write_text(
                runtime_path.read_text(encoding="utf-8").replace(
                    "Diagnostic allocator release hold complete\n", ""
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "lifecycle marker"):
                gate.parse_runtime_log(
                    runtime_path,
                    "stats-candidate",
                    json.loads(binding.read_text()),
                    parsed,
                    gate.validate_plan(PLAN_PATH),
                )

    def test_guardian_capacity_violation_terminates_owned_process(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "guardian.json"
            child = subprocess.Popen(["sleep", "30"])
            rss = subprocess.Popen(["sleep", "30"])
            control = root / "external-conflict-guardian-control.json"
            ready = root / "external-conflict-guardian-ready"
            launch = root / "external-conflict-guardian-launch"
            rss_ready = root / "rss-monitor-ready"
            try:
                gate.create_guardian_control(
                    control,
                    ready,
                    launch,
                    child.pid,
                    os.getpid(),
                    rss.pid,
                    10,
                    rss_ready,
                )
                gate.create_empty_read_only_marker(rss_ready, "RSS ready marker")
                with mock.patch.object(gate, "scan_conflicts", return_value=[]):
                    with self.assertRaisesRegex(gate.GateError, "capacity guardian"):
                        gate.monitor_conflicts(
                            child.pid,
                            output,
                            10,
                            root,
                            1 << 62,
                            control,
                            ready,
                            launch,
                        )
                child.wait(timeout=3)
                evidence = json.loads(output.read_text(encoding="utf-8"))
                self.assertTrue(evidence["termination"]["attempted"])
                self.assertTrue(evidence["capacity_violations"])
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()
                if rss.poll() is None:
                    rss.kill()
                    rss.wait()

    def test_guardian_control_is_atomically_published_only_after_finalization(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            control = root / "external-conflict-guardian-control.json"
            ready = root / "external-conflict-guardian-ready"
            launch = root / "external-conflict-guardian-launch"
            rss_ready = root / "rss-monitor-ready"
            measured = subprocess.Popen(["sleep", "30"])
            rss = subprocess.Popen(["sleep", "30"])
            real_link = os.link
            linked_payloads: list[dict[str, object]] = []

            def inspect_finalized_source(
                source: str | os.PathLike[str],
                destination: str | os.PathLike[str],
                *args: object,
                **kwargs: object,
            ) -> None:
                source_path = Path(source)
                destination_path = Path(destination)
                self.assertEqual(destination_path, control)
                self.assertFalse(control.exists())
                self.assertEqual(stat.S_IMODE(source_path.stat().st_mode), 0o444)
                linked_payloads.append(json.loads(source_path.read_text(encoding="utf-8")))
                real_link(source, destination, *args, **kwargs)

            try:
                with mock.patch.object(gate.os, "link", side_effect=inspect_finalized_source):
                    value = gate.create_guardian_control(
                        control,
                        ready,
                        launch,
                        measured.pid,
                        os.getpid(),
                        rss.pid,
                        100,
                        rss_ready,
                    )
                self.assertEqual(linked_payloads, [value])
                self.assertEqual(json.loads(control.read_text(encoding="utf-8")), value)
                self.assertEqual(stat.S_IMODE(control.stat().st_mode), 0o444)
                self.assertEqual(list(root.glob(f".{control.name}.tmp.*")), [])

                original = control.read_bytes()
                with self.assertRaisesRegex(gate.GateError, "refusing to reuse"):
                    gate.create_guardian_control(
                        control,
                        ready,
                        launch,
                        measured.pid,
                        os.getpid(),
                        rss.pid,
                        100,
                        rss_ready,
                    )
                self.assertEqual(control.read_bytes(), original)
            finally:
                measured.kill()
                measured.wait()
                rss.kill()
                rss.wait()

    def test_guardian_held_launch_has_no_unobserved_measured_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "guardian.json"
            control = root / "external-conflict-guardian-control.json"
            ready = root / "external-conflict-guardian-ready"
            launch = root / "external-conflict-guardian-launch"
            rss_ready = root / "rss-monitor-ready"
            measured = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'while [[ ! -e "$1" ]]; do sleep 0.001; done; sleep 0.35',
                    "bash",
                    str(launch),
                ]
            )
            rss = subprocess.Popen(["sleep", "30"])
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    gate.monitor_conflicts(
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
                with mock.patch.object(gate, "scan_conflicts", return_value=[]):
                    guardian.start()
                    gate.create_guardian_control(
                        control,
                        ready,
                        launch,
                        measured.pid,
                        os.getpid(),
                        rss.pid,
                        100,
                        rss_ready,
                    )
                    gate.create_empty_read_only_marker(
                        rss_ready, "RSS ready marker"
                    )
                    gate.wait_for_guardian_ready(
                        control, ready, launch, 100, 5_000
                    )
                    self.assertIsNone(measured.poll())
                    gate.release_guardian_launch(control, ready, launch, 100)
                    measured.wait(timeout=3)
                    guardian.join(timeout=3)
                self.assertFalse(guardian.is_alive())
                self.assertEqual(failures, [])
                evidence = gate.validate_guardian(
                    output,
                    control,
                    ready,
                    launch,
                    gate.validate_plan(PLAN_PATH),
                    1,
                )
                self.assertLess(
                    evidence["ready_created_poll"], evidence["launch_observed_poll"]
                )
            finally:
                if measured.poll() is None:
                    measured.kill()
                    measured.wait()
                if rss.poll() is None:
                    rss.kill()
                    rss.wait()
                if guardian.is_alive():
                    guardian.join(timeout=1)

    def test_guardian_does_not_classify_bound_root_zombie(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "guardian.json"
            control = root / "external-conflict-guardian-control.json"
            ready = root / "external-conflict-guardian-ready"
            launch = root / "external-conflict-guardian-launch"
            rss_ready = root / "rss-monitor-ready"
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
            rss = subprocess.Popen(["sleep", "30"])
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    gate.monitor_conflicts(
                        measured.pid,
                        output,
                        50,
                        root,
                        1,
                        control,
                        ready,
                        launch,
                    )
                except BaseException as error:
                    failures.append(error)

            guardian = threading.Thread(target=monitor)
            original_identity = gate.process_identity

            def only_measured_root(pid: int) -> tuple[str, str] | None:
                if pid == measured.pid:
                    return original_identity(pid)
                return None

            try:
                with mock.patch.object(
                    gate, "process_identity", side_effect=only_measured_root
                ):
                    guardian.start()
                    gate.create_guardian_control(
                        control,
                        ready,
                        launch,
                        measured.pid,
                        os.getpid(),
                        rss.pid,
                        50,
                        rss_ready,
                    )
                    gate.create_empty_read_only_marker(
                        rss_ready, "RSS ready marker"
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
                if rss.poll() is None:
                    rss.kill()
                    rss.wait()

    def test_guardian_cadence_is_raw_derived_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "guardian.json"
            plan = gate.validate_plan(PLAN_PATH)
            control = root / "external-conflict-guardian-control.json"
            ready = root / "external-conflict-guardian-ready"
            launch = root / "external-conflict-guardian-launch"
            rss_ready = root / "rss-monitor-ready"
            control_value = {
                "schema": gate.GUARDIAN_CONTROL_SCHEMA,
                "root_pid": 123,
                "root_starttime_ticks": 123_000,
                "guardian_pid": 456,
                "guardian_starttime_ticks": 456_000,
                "rss_monitor_pid": 789,
                "rss_monitor_starttime_ticks": 789_000,
                "rss_ready_marker": str(rss_ready),
                "interval_ms": 100,
                "ready_marker": str(ready),
                "launch_marker": str(launch),
            }
            write_json(control, control_value)
            control.chmod(0o444)
            ready.write_bytes(b"")
            ready.chmod(0o444)
            launch.write_bytes(b"")
            launch.chmod(0o444)
            rss_ready.write_bytes(b"")
            rss_ready.chmod(0o444)
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
                "control_path": str(control),
                "control_sha256": gate.sha256_file(control),
                "ready_marker_path": str(ready),
                "ready_marker_sha256": gate.sha256_file(ready),
                "ready_created_poll": 1,
                "ready_created_monotonic_elapsed_ns": 1_000_000,
                "launch_marker_path": str(launch),
                "launch_marker_sha256": gate.sha256_file(launch),
                "launch_observed_poll": 2,
                "launch_observed_monotonic_elapsed_ns": 101_000_000,
                "launch_observed": True,
                "launch_observed_root_bound": True,
                "handshake_violations": [],
                "root_seen": True,
                "filesystem": str(root.resolve()),
                "minimum_required_free_bytes": 1,
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
            write_json(path, valid)
            self.assertEqual(
                gate.validate_guardian(path, control, ready, launch, plan, 1), valid
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
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "exact and causal"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            changed = copy.deepcopy(valid)
            changed["launch_observed_poll"] = 3
            changed["launch_observed_monotonic_elapsed_ns"] = 201_000_000
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "exact and causal"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            changed = copy.deepcopy(valid)
            changed["polls"] = 1
            changed["poll_monotonic_elapsed_ns"] = [1_000_000]
            changed["maximum_poll_start_gap_ns"] = 0
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "must be >= 2"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            changed = copy.deepcopy(valid)
            changed["polls"] = 4
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "timestamp count"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            changed = copy.deepcopy(valid)
            changed["poll_monotonic_elapsed_ns"] = [
                101_000_000,
                1_000_000,
                201_000_000,
            ]
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "strictly increasing"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            changed = copy.deepcopy(valid)
            changed["maximum_poll_start_gap_ns"] = 99_999_999
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "not exactly derived"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            changed = copy.deepcopy(valid)
            changed["elapsed_ns"] = 202_000_000
            changed["poll_monotonic_elapsed_ns"] = [
                1_000_000,
                201_000_001,
                201_000_002,
            ]
            changed["maximum_poll_start_gap_ns"] = 200_000_001
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "exceeds"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            changed = copy.deepcopy(valid)
            changed["elapsed_ns"] = 401_000_001
            changed["maximum_poll_start_gap_ns"] = 200_000_001
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "exceeds"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            write_json(path, valid)
            launch.unlink()
            with self.assertRaisesRegex(gate.GateError, "is missing"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)
            launch.write_bytes(b"")
            launch.chmod(0o444)
            ready.chmod(0o644)
            with self.assertRaisesRegex(gate.GateError, "mode 0444"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)
            ready.chmod(0o444)
            launch.chmod(0o644)
            launch.write_bytes(b"released")
            launch.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "empty mode 0444"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)
            launch.chmod(0o644)
            launch.write_bytes(b"")
            launch.chmod(0o444)

            changed_control = dict(control_value)
            changed_control["root_pid"] = 124
            control.chmod(0o644)
            write_json(control, changed_control)
            control.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "exact handshake"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            control.chmod(0o644)
            write_json(control, control_value)
            control.chmod(0o444)

            changed = copy.deepcopy(valid)
            changed["ready_created_poll"] = 2
            changed["ready_created_monotonic_elapsed_ns"] = 101_000_000
            changed["launch_observed_poll"] = 1
            changed["launch_observed_monotonic_elapsed_ns"] = 1_000_000
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "causal"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

            changed = copy.deepcopy(valid)
            changed["launch_observed"] = False
            write_json(path, changed)
            with self.assertRaisesRegex(gate.GateError, "causal"):
                gate.validate_guardian(path, control, ready, launch, plan, 1)

    def test_guardian_retains_fast_terminal_launch_but_rejects_admission(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "guardian.json"
            control = root / "external-conflict-guardian-control.json"
            ready = root / "external-conflict-guardian-ready"
            launch = root / "external-conflict-guardian-launch"
            rss_ready = root / "rss-monitor-ready"
            measured = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'while [[ ! -e "$1" ]]; do sleep 0.001; done; exec true',
                    "phase5-full-fast-root",
                    str(launch),
                ]
            )
            rss = subprocess.Popen(["sleep", "30"])
            failures: list[BaseException] = []

            def monitor() -> None:
                try:
                    gate.monitor_conflicts(
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
                with mock.patch.object(gate, "scan_conflicts", return_value=[]):
                    guardian.start()
                    gate.create_guardian_control(
                        control,
                        ready,
                        launch,
                        measured.pid,
                        os.getpid(),
                        rss.pid,
                        100,
                        rss_ready,
                    )
                    gate.create_empty_read_only_marker(rss_ready, "RSS ready marker")
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
                if rss.poll() is None:
                    rss.kill()
                    rss.wait()
                if guardian.is_alive():
                    guardian.join(timeout=1)

    def test_process_identity_liveness_rejects_zombie_and_pid_reuse(self) -> None:
        zombie = {
            "pid": 10,
            "ppid": 1,
            "state": "Z",
            "starttime_ticks": 100,
        }
        for dead_state in ("Z", "X", "x"):
            dead = {**zombie, "state": dead_state}
            with mock.patch.object(
                gate, "read_process_stat_identity", return_value=dead
            ):
                self.assertFalse(gate.process_is_same_running(10, 100))
                self.assertEqual(
                    gate.process_identity_refusal(dead), f"state_{dead_state}"
                )

        reused = {**zombie, "state": "S", "starttime_ticks": 101}
        with mock.patch.object(gate, "read_process_stat_identity", return_value=reused):
            self.assertFalse(gate.process_is_same_running(10, 100))

        target = {**zombie, "state": "S"}
        reparented = {**target, "ppid": 999}
        with mock.patch.object(
            gate, "read_process_stat_identity", return_value=reparented
        ):
            self.assertIsNone(gate.process_identity_refusal(target))

    def test_conflict_scan_excludes_bound_root_zombie_but_not_reused_pid(self) -> None:
        root_pid = 10
        root_starttime_ticks = 100
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
            mock.patch.object(gate, "ancestor_pids", return_value=set()),
            mock.patch.object(
                gate, "process_tree_identity_bindings", return_value={}
            ),
            mock.patch.object(
                gate, "read_process_stat_identity", return_value=zombie
            ),
            mock.patch.object(
                gate, "process_identity", return_value=forbidden_identity
            ) as process_identity,
        ):
            self.assertEqual(
                gate.scan_conflicts(
                    allowed_root_pid=root_pid,
                    allowed_root_starttime_ticks=root_starttime_ticks,
                ),
                [],
            )
        process_identity.assert_not_called()

        reused = {**zombie, "state": "S", "starttime_ticks": 101}
        with (
            mock.patch.object(Path, "iterdir", return_value=[root_entry]),
            mock.patch.object(gate, "ancestor_pids", return_value=set()),
            mock.patch.object(
                gate, "process_tree_identity_bindings", return_value={}
            ),
            mock.patch.object(
                gate, "read_process_stat_identity", return_value=reused
            ),
            mock.patch.object(
                gate, "process_identity", return_value=forbidden_identity
            ),
        ):
            conflicts = gate.scan_conflicts(
                allowed_root_pid=root_pid,
                allowed_root_starttime_ticks=root_starttime_ticks,
            )
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["pid"], root_pid)
        self.assertEqual(
            conflicts[0]["reason"],
            "forbidden measurement process chronoxide-inge",
        )

    def test_conflict_scan_excludes_bound_descendant_zombie_but_not_reparented_pid(
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

        child_entry = Path(f"/proc/{child_pid}")
        forbidden_identity = ("chronoxide-inge", "")
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

        with (
            mock.patch.object(Path, "iterdir", return_value=[child_entry]),
            mock.patch.object(gate, "ancestor_pids", return_value=set()),
            mock.patch.object(
                gate, "process_tree_identity_bindings", return_value=bindings
            ),
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ),
            mock.patch.object(
                gate, "process_identity", return_value=forbidden_identity
            ) as process_identity,
        ):
            self.assertEqual(
                gate.scan_conflicts(
                    allowed_root_pid=root_pid,
                    allowed_root_starttime_ticks=root_starttime_ticks,
                ),
                [],
            )
        process_identity.assert_not_called()

        original_child = identities[child_pid]
        for changed_child in (
            {**original_child, "ppid": 999},
            {**original_child, "state": "S", "starttime_ticks": 201},
        ):
            identities[child_pid] = changed_child
            with (
                mock.patch.object(Path, "iterdir", return_value=[child_entry]),
                mock.patch.object(gate, "ancestor_pids", return_value=set()),
                mock.patch.object(
                    gate, "process_tree_identity_bindings", return_value=bindings
                ),
                mock.patch.object(
                    gate,
                    "read_process_stat_identity",
                    side_effect=lambda pid: identities.get(pid),
                ),
                mock.patch.object(
                    gate, "process_identity", return_value=forbidden_identity
                ),
            ):
                conflicts = gate.scan_conflicts(
                    allowed_root_pid=root_pid,
                    allowed_root_starttime_ticks=root_starttime_ticks,
                )
            self.assertEqual(
                [conflict["pid"] for conflict in conflicts], [child_pid]
            )

        identities[child_pid] = original_child
        identities[wrapper_pid] = {**identities[wrapper_pid], "ppid": 999}
        with (
            mock.patch.object(Path, "iterdir", return_value=[child_entry]),
            mock.patch.object(gate, "ancestor_pids", return_value=set()),
            mock.patch.object(
                gate, "process_tree_identity_bindings", return_value=bindings
            ),
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ),
            mock.patch.object(
                gate, "process_identity", return_value=forbidden_identity
            ),
        ):
            broken_ancestry = gate.scan_conflicts(
                allowed_root_pid=root_pid,
                allowed_root_starttime_ticks=root_starttime_ticks,
            )
        self.assertEqual(
            [conflict["pid"] for conflict in broken_ancestry], [child_pid]
        )

        identities[child_pid] = {**original_child, "ppid": 999}
        with (
            mock.patch.object(Path, "iterdir", return_value=[child_entry]),
            mock.patch.object(gate, "ancestor_pids", return_value=set()),
            mock.patch.object(
                gate, "process_tree_identity_bindings", return_value={}
            ),
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ),
            mock.patch.object(
                gate, "process_identity", return_value=forbidden_identity
            ),
        ):
            external = gate.scan_conflicts(
                allowed_root_pid=root_pid,
                allowed_root_starttime_ticks=root_starttime_ticks,
            )
        self.assertEqual([conflict["pid"] for conflict in external], [child_pid])

    def test_process_tree_binds_root_starttime_and_each_child_parent(self) -> None:
        root = {"pid": 10, "ppid": 1, "state": "S", "starttime_ticks": 100}
        child = {"pid": 20, "ppid": 10, "state": "S", "starttime_ticks": 200}
        identities = {10: root, 20: child}
        with (
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ),
            mock.patch.object(
                gate,
                "read_process_children",
                side_effect=lambda pid: [20] if pid == 10 else [],
            ),
        ):
            self.assertEqual(gate.process_tree(10, 100), {10, 20})

        reused_child = {**child, "ppid": 999}
        identities[20] = reused_child
        with (
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ),
            mock.patch.object(
                gate,
                "read_process_children",
                side_effect=lambda pid: [20] if pid == 10 else [],
            ),
        ):
            self.assertEqual(gate.process_tree(10, 100), {10})

        reused_root = {**root, "starttime_ticks": 101}
        with (
            mock.patch.object(
                gate,
                "read_process_stat_identity",
                side_effect=[root, reused_root],
            ),
            mock.patch.object(gate, "read_process_children") as children,
        ):
            self.assertEqual(gate.process_tree(10, 100), set())
        children.assert_not_called()

    def test_termination_is_depth_first_and_refuses_reused_pids(self) -> None:
        targets = [
            {"pid": 99, "ppid": 5, "state": "S", "starttime_ticks": 300, "depth": 2},
            {"pid": 5, "ppid": 10, "state": "S", "starttime_ticks": 200, "depth": 1},
            {"pid": 10, "ppid": 1, "state": "S", "starttime_ticks": 100, "depth": 0},
        ]
        with (
            mock.patch.object(gate, "snapshot_process_tree_identities", return_value=targets),
            mock.patch.object(gate, "process_identity_refusal", return_value=None),
            mock.patch.object(gate, "wait_for_process_identities_exit"),
            mock.patch.object(gate.os, "kill") as kill,
        ):
            evidence = gate.terminate_process_tree(10, 100)
        self.assertEqual(
            kill.call_args_list,
            [
                mock.call(99, gate.signal.SIGTERM),
                mock.call(5, gate.signal.SIGTERM),
                mock.call(10, gate.signal.SIGTERM),
                mock.call(99, gate.signal.SIGKILL),
                mock.call(5, gate.signal.SIGKILL),
                mock.call(10, gate.signal.SIGKILL),
            ],
        )
        self.assertEqual(evidence["target_pids"], [99, 5, 10])

        current = {
            "pid": 10,
            "ppid": 1,
            "state": "S",
            "starttime_ticks": 101,
        }
        with (
            mock.patch.object(
                gate, "snapshot_process_tree_identities", return_value=[targets[-1]]
            ),
            mock.patch.object(gate, "read_process_stat_identity", return_value=current),
            mock.patch.object(gate, "wait_for_process_identities_exit"),
            mock.patch.object(gate.os, "kill") as kill,
        ):
            refused = gate.terminate_process_tree(10, 100)
        kill.assert_not_called()
        self.assertTrue(refused["identity_refusals"])
        self.assertTrue(
            all(
                item["reason"] == "starttime_mismatch"
                for item in refused["identity_refusals"]
            )
        )

    def test_guardian_cleanup_stops_measured_tree_before_monitors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            control_path = root / "control.json"
            ready = root / "ready"
            launch = root / "launch"
            control_path.write_text("sealed\n", encoding="utf-8")
            control = {
                "root_pid": 10,
                "root_starttime_ticks": 100,
                "rss_monitor_pid": 20,
                "rss_monitor_starttime_ticks": 200,
                "guardian_pid": 30,
                "guardian_starttime_ticks": 300,
            }
            with (
                mock.patch.object(
                    gate, "validate_guardian_control", return_value=control
                ) as validate,
                mock.patch.object(
                    gate,
                    "terminate_process_tree",
                    side_effect=lambda pid, starttime: {
                        "pid": pid,
                        "starttime_ticks": starttime,
                    },
                ) as terminate,
            ):
                result = gate.cleanup_guardian_processes(
                    control_path, ready, launch, 100
                )
            validate.assert_called_once_with(
                control_path,
                ready,
                launch,
                100,
                require_live=False,
            )
            self.assertEqual(
                terminate.call_args_list,
                [mock.call(10, 100), mock.call(20, 200), mock.call(30, 300)],
            )
            self.assertEqual(
                result["termination_order"], ["root", "rss_monitor", "guardian"]
            )

            incomplete = {"term_errors": [], "kill_errors": [], "surviving_pids": [10]}
            complete: dict[str, object] = {}
            with (
                mock.patch.object(
                    gate, "validate_guardian_control", return_value=control
                ),
                mock.patch.object(
                    gate,
                    "terminate_process_tree",
                    side_effect=[incomplete, complete, complete],
                ) as terminate,
                self.assertRaisesRegex(gate.GateError, "cleanup was incomplete"),
            ):
                gate.cleanup_guardian_processes(control_path, ready, launch, 100)
            self.assertEqual(terminate.call_count, 3)

    def test_guardian_cleanup_terminates_a_live_tree_and_both_monitors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            child_ready = root / "child-ready"
            measured = subprocess.Popen(
                [
                    "bash",
                    "-c",
                    'sleep 30 & printf "%s\\n" "$!" >"$1"; wait',
                    "bash",
                    str(child_ready),
                ]
            )
            rss = subprocess.Popen(["sleep", "30"])
            guardian = subprocess.Popen(["sleep", "30"])
            control = root / "control.json"
            ready = root / "ready"
            launch = root / "launch"
            rss_ready = root / "rss-monitor-ready"
            try:
                deadline = time.monotonic() + 2
                while not child_ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(child_ready.exists())
                measured_child = int(child_ready.read_text(encoding="ascii").strip())
                gate.create_guardian_control(
                    control,
                    ready,
                    launch,
                    measured.pid,
                    guardian.pid,
                    rss.pid,
                    100,
                    rss_ready,
                )
                evidence = gate.cleanup_guardian_processes(
                    control, ready, launch, 100
                )
                measured.wait(timeout=2)
                rss.wait(timeout=2)
                guardian.wait(timeout=2)
                self.assertEqual(
                    evidence["termination_order"],
                    ["root", "rss_monitor", "guardian"],
                )
                self.assertIn(
                    measured_child,
                    evidence["terminations"]["root"]["target_pids"],
                )
                self.assertEqual(
                    evidence["terminations"]["root"]["surviving_pids"], []
                )
            finally:
                for process in (measured, rss, guardian):
                    if process.poll() is None:
                        process.kill()
                    process.wait()

    def test_capacity_formula_tracks_remaining_and_first_observed_corpus(self) -> None:
        expected_size = json.loads(EXPECTATIONS_PATH.read_text())["corpus"]["size_bytes"]
        plan = gate.validate_plan(PLAN_PATH)
        initial_required = (
            expected_size * plan["capacity"]["retained_corpus_count"]
            + plan["capacity"]["additional_headroom_bytes"]
            + plan["capacity"]["build_headroom_bytes"]
        )
        self.assertEqual(initial_required, 66_029_355_648)
        first = gate.run_capacity_requirements(
            "stats", 1, EXPECTATIONS_PATH, PLAN_PATH
        )
        self.assertEqual(first["remaining_corpora_including_current"], 8)
        self.assertEqual(first["remaining_corpora_after_current"], 7)
        self.assertEqual(
            first["launch_required_free_bytes"],
            expected_size * 8 + 10 * 1024**3,
        )
        self.assertEqual(
            first["guardian_minimum_free_bytes"],
            expected_size * 7 + 10 * 1024**3,
        )
        with tempfile.TemporaryDirectory() as temporary:
            summary = Path(temporary) / "corpus.json"
            write_json(
                summary,
                {
                    "schema": gate.CORPUS_SCHEMA,
                    "file_count": 1,
                    "size_bytes": expected_size + 123,
                    "manifest_sha256": "a" * 64,
                },
            )
            later = gate.run_capacity_requirements(
                "stats", 2, EXPECTATIONS_PATH, PLAN_PATH, summary
            )
            self.assertEqual(later["capacity_corpus_size_bytes"], expected_size + 123)
            self.assertEqual(later["remaining_corpora_after_current"], 6)

    def test_raw_time_and_perf_reductions_reject_saved_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_time = root / "time.txt"
            raw_time.write_text(
                "User time (seconds): 1.25\n"
                "System time (seconds): 0.50\n"
                "Percent of CPU this job got: 90%\n"
                "Elapsed (wall clock) time (h:mm:ss or m:ss): 0:02.00\n"
                "Maximum resident set size (kbytes): 100\n"
                "Major (requiring I/O) page faults: 0\n"
                "Minor (reclaiming a frame) page faults: 1\n"
                "Voluntary context switches: 2\n"
                "Involuntary context switches: 3\n"
                "File system inputs: 4\n"
                "File system outputs: 5\n"
                "Exit status: 0\n",
                encoding="utf-8",
            )
            parsed_time = root / "time.json"
            write_json(parsed_time, gate.parse_gnu_time_raw(raw_time))
            gate.validate_timing_evidence(raw_time, parsed_time)
            value = json.loads(parsed_time.read_text())
            value["max_rss_kib"] += 1
            write_json(parsed_time, value)
            with self.assertRaisesRegex(gate.GateError, "not derived"):
                gate.validate_timing_evidence(raw_time, parsed_time)

            raw_perf = root / "perf.tsv"
            raw_perf.write_text(
                "".join(f"1\t\t{event}\n" for event in sorted(gate.EXPECTED_PERF_EVENTS)),
                encoding="utf-8",
            )
            parsed_perf = root / "perf.json"
            write_json(parsed_perf, gate.parse_perf_stat_raw(raw_perf))
            gate.validate_perf_evidence(raw_perf, parsed_perf)
            value = json.loads(parsed_perf.read_text())
            value["events"][0]["raw_value"] = "2"
            write_json(parsed_perf, value)
            with self.assertRaisesRegex(gate.GateError, "not derived"):
                gate.validate_perf_evidence(raw_perf, parsed_perf)

    def test_quiescence_and_corpus_reductions_reject_raw_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            corpus = root / "segments"
            corpus.mkdir()
            (corpus / "a.bin").write_bytes(b"a")
            manifest_bytes, inventory_bytes, summary = gate.derive_corpus_artifacts(corpus)
            manifest = root / "segments.sha256"
            inventory = root / "segments.tsv"
            summary_path = root / "corpus-summary.json"
            manifest.write_bytes(manifest_bytes)
            inventory.write_bytes(inventory_bytes)
            write_json(summary_path, summary)
            gate.validate_corpus(summary_path, manifest, inventory, corpus)
            (corpus / "a.bin").write_bytes(b"changed")
            with self.assertRaisesRegex(gate.GateError, "payload bytes"):
                gate.validate_corpus(summary_path, manifest, inventory, corpus)

            configs = root / "configs"
            configs.mkdir()
            (configs / "a.toml").write_text("x", encoding="utf-8")
            samples = root / "quiescence.tsv"
            samples.write_text(
                "elapsed_ns\tdirty_kib\twriteback_kib\ttotal_kib\twithin_limit\n"
                "1\t1\t1\t2\ttrue\n2\t1\t0\t1\ttrue\n3\t0\t0\t0\ttrue\n",
                encoding="utf-8",
            )
            quiescence = root / "quiescence.json"
            write_json(
                quiescence,
                {
                    "schema": "chronoxide/storage-vnext-phase5-writeback-quiescence/v1",
                    "corpus": str(configs.resolve()),
                    "fsynced_file_count": 1,
                    "global_sync_called": True,
                    "maximum_dirty_writeback_kib": 65536,
                    "required_consecutive_samples": 3,
                    "interval_ms": 250,
                    "timeout_secs": 120,
                    "sample_count": 3,
                    "final_dirty_kib": 0,
                    "final_writeback_kib": 0,
                    "final_total_kib": 0,
                    "passed": True,
                },
            )
            gate.validate_quiescence(
                quiescence, samples, gate.validate_plan(PLAN_PATH), configs, 1
            )
            changed = json.loads(quiescence.read_text(encoding="utf-8"))
            changed["maximum_dirty_writeback_kib"] += 1
            write_json(quiescence, changed)
            with self.assertRaisesRegex(gate.GateError, "contract"):
                gate.validate_quiescence(
                    quiescence, samples, gate.validate_plan(PLAN_PATH), configs, 1
                )
            changed["maximum_dirty_writeback_kib"] -= 1
            write_json(quiescence, changed)
            samples.write_text(samples.read_text().replace("3\t0\t0\t0", "3\t0\t1\t0"))
            with self.assertRaisesRegex(gate.GateError, "raw counters"):
                gate.validate_quiescence(
                    quiescence, samples, gate.validate_plan(PLAN_PATH), configs, 1
                )

    def test_replay_correctness_is_reparsed_by_frozen_helper(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            helper = root / "screen/metadata/harness/ab_gate.py"
            helper.parent.mkdir(parents=True)
            helper.write_text(
                "import argparse,json\nfrom pathlib import Path\n"
                "p=argparse.ArgumentParser(); p.add_argument('command'); "
                "p.add_argument('--report'); p.add_argument('--output'); a=p.parse_args(); "
                "Path(a.output).write_text(json.dumps(json.loads(Path(a.report).read_text())))\n",
                encoding="utf-8",
            )
            scratch = root / "scratch"
            scratch.mkdir()
            expected = {"general": {"Total Messages": 4_000_000}}
            report = root / "report.md"
            saved = root / "saved.json"
            expectations = root / "expectations.json"
            write_json(report, expected)
            write_json(saved, expected)
            write_json(expectations, {"replay_correctness": expected})
            binding = {
                "screen_root": str(root / "screen"),
                "report_gate_sha256": gate.sha256_file(helper),
            }
            gate.validate_correctness(
                saved,
                expectations,
                report_path=report,
                binding=binding,
                scratch_root=scratch,
            )
            write_json(report, {"general": {"Total Messages": 3_999_999}})
            with self.assertRaisesRegex(gate.GateError, "not derived"):
                gate.validate_correctness(
                    saved,
                    expectations,
                    report_path=report,
                    binding=binding,
                    scratch_root=scratch,
                )

    def test_capture_residency_is_bound_to_exact_frozen_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            capture = root / "capture"
            capture.mkdir()
            inputs = root / "inputs.json"
            write_json(
                inputs,
                {
                    "capture": str(capture),
                    "capture_files": [
                        {"name": "a.capture", "size_bytes": 10, "sha256": "a" * 64}
                    ],
                },
            )
            residency = root / "residency.tsv"
            residency.write_text(f"0 10 {capture / 'a.capture'}\n", encoding="utf-8")
            evidence = gate.validate_capture_residency(
                residency, inputs, maximum_resident_bytes=0
            )
            self.assertEqual(evidence["total_resident_bytes"], 0)
            residency.write_text(f"11 10 {capture / 'a.capture'}\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "file set or size"):
                gate.validate_capture_residency(
                    residency, inputs, maximum_resident_bytes=0
                )

    def test_raw_authority_has_exact_nested_coverage_and_rejects_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = parent / "raw"
            (root / "nested").mkdir(parents=True)
            (root / "a.txt").write_text("a\n", encoding="utf-8")
            (root / "nested/b.bin").write_bytes(b"b")
            authority = parent / "authority.tsv"
            evidence = gate.create_raw_authority(root, authority)
            self.assertEqual(evidence["file_count"], 2)
            self.assertEqual(gate.check_raw_authority(authority)["file_count"], 2)
            (root / "a.txt").chmod(0o644)
            (root / "a.txt").write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "changed|mode"):
                gate.check_raw_authority(authority)
            root.chmod(0o755)
            (root / "nested").chmod(0o755)

    def test_raw_authority_rejects_symlink_and_unexpected_nested_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = parent / "raw"
            root.mkdir()
            (root / "a").write_text("a", encoding="utf-8")
            (root / "link").symlink_to(root / "a")
            with self.assertRaisesRegex(gate.GateError, "symlink"):
                gate.create_raw_authority(root, parent / "authority.tsv")

            (root / "link").unlink()
            authority = parent / "authority.tsv"
            gate.create_raw_authority(root, authority)
            root.chmod(0o755)
            (root / "extra").write_text("x", encoding="utf-8")
            root.chmod(0o555)
            with self.assertRaisesRegex(gate.GateError, "not exact"):
                gate.check_raw_authority(authority)
            root.chmod(0o755)
            (root / "extra").unlink()
            (root / "empty-extra").mkdir()
            root.chmod(0o555)
            with self.assertRaisesRegex(gate.GateError, "directory set"):
                gate.check_raw_authority(authority)
            root.chmod(0o755)

    def test_final_artifact_authorities_reject_nested_additions_and_bad_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "result"
            (root / "metadata").mkdir(parents=True)
            (root / "build-target").mkdir()
            (root / "comparisons").mkdir()
            (root / "evidence.txt").write_text("evidence\n", encoding="utf-8")
            final_decision = root / "comparisons/final-full-gate-decision.json"
            final_decision.write_text("decision\n", encoding="utf-8")
            manifest = root / "metadata/result-artifacts.tsv"
            with mock.patch.object(gate, "validate_result_tree_shape", return_value=None):
                artifacts = gate.create_artifact_manifest(root, manifest)
                (root / "unexpected-empty").mkdir()
                with self.assertRaisesRegex(gate.GateError, "directory inventory"):
                    gate.check_artifact_manifest(root, manifest, stage="precomplete")
                (root / "unexpected-empty").rmdir()

                certificate = {
                    "schema": gate.FINAL_ADMISSION_SCHEMA,
                    "stage": "precomplete",
                    "status": "pass",
                    "final_decision_sha256": "0" * 64,
                    "artifact_manifest_sha256": artifacts["artifact_manifest_sha256"],
                    "file_inventory_sha256": artifacts["file_inventory_sha256"],
                    "directory_inventory_sha256": artifacts["directory_inventory_sha256"],
                    "artifact_count": artifacts["artifact_count"],
                    "directory_count": artifacts["directory_count"],
                    "production_promotion_authorized": False,
                }
                certificate["final_decision_sha256"] = gate.sha256_file(final_decision)
                certificate_path = root / "metadata/FINAL_SEAL_VALIDATED.json"
                write_json(certificate_path, certificate)
                certificate_path.chmod(0o444)
                gate.check_artifact_manifest(root, manifest, stage="precomplete")
                (root / "metadata").chmod(0o555)
                complete = {
                    "schema": gate.COMPLETION_SCHEMA,
                    "final_decision_sha256": gate.sha256_file(final_decision),
                    "precompletion_admission_sha256": gate.sha256_file(certificate_path),
                    "artifact_manifest_sha256": artifacts["artifact_manifest_sha256"],
                    "file_inventory_sha256": artifacts["file_inventory_sha256"],
                    "directory_inventory_sha256": artifacts["directory_inventory_sha256"],
                    "artifact_count": artifacts["artifact_count"],
                    "directory_count": artifacts["directory_count"],
                    "production_promotion_authorized": False,
                }
                complete_path = root / "COMPLETE"
                write_json(complete_path, complete)
                complete_path.chmod(0o444)
                root.chmod(0o555)
                gate.check_artifact_manifest(root, manifest, stage="complete")
                root.chmod(0o755)
                complete_path.chmod(0o644)
                with self.assertRaisesRegex(gate.GateError, "mutable"):
                    gate.check_artifact_manifest(root, manifest, stage="complete")
                changed_complete = dict(complete)
                changed_complete["production_promotion_authorized"] = True
                write_json(complete_path, changed_complete)
                complete_path.chmod(0o444)
                root.chmod(0o555)
                with self.assertRaisesRegex(gate.GateError, "differs"):
                    gate.check_artifact_manifest(root, manifest, stage="complete")
                root.chmod(0o755)
                complete_path.chmod(0o644)
                write_json(complete_path, complete)
                complete_path.chmod(0o444)
                (root / "unexpected-after-complete").write_text("x")
                root.chmod(0o555)
                with self.assertRaisesRegex(gate.GateError, "file inventory"):
                    gate.check_artifact_manifest(root, manifest, stage="complete")
                root.chmod(0o755)
                (root / "unexpected-after-complete").unlink()
                (root / "empty-after-complete").mkdir()
                root.chmod(0o555)
                with self.assertRaisesRegex(gate.GateError, "directory inventory"):
                    gate.check_artifact_manifest(root, manifest, stage="complete")
            root.chmod(0o755)
            (root / "metadata").chmod(0o755)
            (root / "comparisons").chmod(0o755)

    def test_runner_has_fail_fast_formal_controls(self) -> None:
        runner = (HERE / "phase5_allocator_full_run.sh").read_text(encoding="utf-8")
        self.assertIn("python3_background() {", runner)
        self.assertIn('exec "${command[@]}"', runner)
        self.assertIn("verify_background_python_pid_binding", runner)
        self.assertIn(
            'python3_background "$SCREEN_GATE" monitor-rss-release', runner
        )
        self.assertIn(
            'python3_background "$FROZEN_GATE" monitor-conflicts', runner
        )
        self.assertNotIn('python3 "$SCREEN_GATE" monitor-rss-release', runner)
        self.assertNotIn('python3 "$FROZEN_GATE" monitor-conflicts', runner)
        self.assertIn('CAPTURE="${CAPTURE:-}"', runner)
        self.assertIn('CONFIG_TEMPLATE="${CONFIG_TEMPLATE:-}"', runner)
        self.assertIn('QUIET_HOST_CONFIRMED=1', runner)
        self.assertIn("perf-stat-preflight.tsv", runner)
        self.assertIn("screen-validation-after-no-stats-build.json", runner)
        self.assertLess(
            runner.index('check-capacity --result-parent "$result_parent"'),
            runner.index('mkdir "$RESULT_DIR"'),
        )
        self.assertEqual(
            runner.count('--segments-dir "$run_dir/segments" --schema schema8'), 1
        )
        held = runner.index(
            'while [[ ! -e "$guardian_launch" && ! -L "$guardian_launch" ]]'
        )
        rss_monitor = runner.index("monitor-rss-release", held)
        monitor = runner.index('monitor-conflicts --pid "$launcher_pid"', rss_monitor)
        control = runner.index("create-guardian-control", monitor)
        ready = runner.index("wait-guardian-ready", control)
        release = runner.index("release-guardian-launch", ready)
        self.assertLess(held, rss_monitor)
        self.assertLess(rss_monitor, monitor)
        self.assertLess(monitor, control)
        self.assertLess(control, ready)
        self.assertLess(ready, release)
        cleanup = runner.index("cleanup-guardian-processes")
        fallback_root = runner.index(
            'stop_bound_tree root "$active_root_pid"', cleanup
        )
        fallback_rss = runner.index(
            'stop_bound_tree rss-monitor "$active_rss_pid"', fallback_root
        )
        fallback_guardian = runner.index(
            'stop_bound_tree guardian "$active_guardian_pid"', fallback_rss
        )
        self.assertLess(cleanup, fallback_root)
        self.assertLess(fallback_root, fallback_rss)
        self.assertLess(fallback_rss, fallback_guardian)
        self.assertLess(
            runner.index(
                'active_root_starttime_ticks="$(read_live_starttime_ticks '
                '"$launcher_pid")"'
            ),
            control,
        )
        self.assertNotIn(
            'for pid in "${active_children[@]}"; do kill -TERM', runner
        )
        self.assertNotIn("kill -TERM", runner)
        self.assertIn("for ((attempt = 0; attempt < 200; attempt++))", runner)
        self.assertIn("refusing to wait for reused $role PID $pid", runner)
        self.assertIn("timeout-live", runner)
        stop_bound_source = runner[
            runner.index("stop_bound_tree()") : runner.index(
                "clear_active_processes()"
            )
        ]
        self.assertNotIn("read_live_starttime_ticks", stop_bound_source)
        self.assertIn("unbound-signal-refused", stop_bound_source)

        launcher_pid = runner.index("local launcher_pid=$!", held)
        launcher_defer = runner.rfind("defer_cleanup_signals", 0, launcher_pid)
        launcher_binding = runner.index(
            'active_root_starttime_ticks="$(read_live_starttime_ticks ', launcher_pid
        )
        launcher_arm = runner.index("arm_cleanup_signals", launcher_binding)
        rss_spawn = runner.index("monitor-rss-release", launcher_arm)
        rss_defer = runner.rfind("defer_cleanup_signals", launcher_arm, rss_spawn)
        rss_pid = runner.index("local rss_pid=$!", rss_spawn)
        rss_binding = runner.index(
            'active_rss_starttime_ticks="$(read_live_starttime_ticks ', rss_pid
        )
        rss_arm = runner.index("arm_cleanup_signals", rss_binding)
        guardian_spawn = runner.index("monitor-conflicts --pid", rss_arm)
        guardian_defer = runner.rfind(
            "defer_cleanup_signals", rss_arm, guardian_spawn
        )
        guardian_pid = runner.index("local guardian_pid=$!", guardian_spawn)
        guardian_binding = runner.index(
            'active_guardian_starttime_ticks="$(read_live_starttime_ticks ',
            guardian_pid,
        )
        guardian_arm = runner.index("arm_cleanup_signals", guardian_binding)
        self.assertLess(launcher_defer, launcher_pid)
        self.assertLess(launcher_pid, launcher_binding)
        self.assertLess(launcher_binding, launcher_arm)
        self.assertLess(launcher_arm, rss_defer)
        self.assertLess(rss_defer, rss_pid)
        self.assertLess(rss_pid, rss_binding)
        self.assertLess(rss_binding, rss_arm)
        self.assertLess(rss_arm, guardian_defer)
        self.assertLess(guardian_defer, guardian_pid)
        self.assertLess(guardian_pid, guardian_binding)
        self.assertLess(guardian_binding, guardian_arm)
        self.assertIn(
            "if [[ \"$cleanup_signal_pending\" == 1 ]]", runner
        )
        self.assertIn('local rss_ready="$run_dir/rss-monitor-ready"', runner)
        self.assertGreaterEqual(runner.count('--rss-ready "$rss_ready"'), 2)
        self.assertIn('--control "$guardian_control"', runner)
        self.assertIn('--launch "$guardian_launch"', runner)
        self.assertIn(
            '"$state" != Z && "$state" != X && "$state" != x', runner
        )
        self.assertIn(
            '"$state" == Z || "$state" == X || "$state" == x', runner
        )
        self.assertIn(
            'identity="$(read_process_state_starttime_ticks "$1")" || return 1',
            runner,
        )
        self.assertNotIn(
            '<<<"$(read_process_state_starttime_ticks', runner
        )
        self.assertIn(
            '"$(stat -c \'%a\' -- "$guardian_launch")" == 444', runner
        )

    def test_runner_exit_cleanup_preserves_status_pre_and_post_control(self) -> None:
        source = (HERE / "phase5_allocator_full_run.sh").read_text(encoding="utf-8")
        lifecycle_start = source.index("active_lifecycle=0")
        trap_line = 'trap \'cleanup_on_exit "$?"\' EXIT'
        lifecycle_end = source.index(trap_line, lifecycle_start) + len(trap_line)
        lifecycle_source = source[lifecycle_start:lifecycle_end]
        handler_start = lifecycle_source.index("cleanup_on_exit() {")
        handler_end = lifecycle_source.index("\n}", handler_start) + 2
        handler = lifecycle_source[handler_start:handler_end]
        self.assertIn('local exit_status="$1"', handler)
        self.assertLess(handler.index("trap - EXIT"), handler.index("stop_children"))
        self.assertIn('if [[ "$active_lifecycle" == 1 ]]', handler)
        self.assertNotIn("$(", handler)
        self.assertEqual(source.count("active_lifecycle=1"), 1)
        active = source.index("active_lifecycle=1")
        self.assertLess(lifecycle_end, active)
        self.assertLess(active, source.index("defer_cleanup_signals", active))

        shell = (
            "set -euo pipefail\n"
            "TRACE=$1\nRUN_DIR=$2\nCONTROL=$3\nMODE=$4\n"
            "FROZEN_GATE=/nonexistent\n"
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

    def test_canonical_storage_and_readback_are_bound_to_frozen_expectations(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            expectations = json.loads(EXPECTATIONS_PATH.read_text(encoding="utf-8"))
            storage = current_storage_report(expectations["storage_verifier"])
            expectations["storage_verifier"]["chunks_by_kind"] = storage[
                "chunks_by_kind"
            ]
            local_expectations = root / "expectations.json"
            write_json(local_expectations, expectations)
            storage_path = root / "storage.json"
            write_json(storage_path, storage)
            self.assertEqual(
                gate.validate_storage_report(storage_path, local_expectations)["samples"],
                154_902_724,
            )
            raw_f64 = copy.deepcopy(storage)
            raw_row = raw_f64["chunk_inventory"]["by_kind_encoding"][0]
            raw_row["encoding"] = "raw_f64"
            raw_row["payload_layout"] = "t0_interleaved_dt_value"
            raw_f64["chunk_inventory"]["timestamp_candidates"][
                "by_kind_encoding"
            ][0]["encoding"] = "raw_f64"
            write_json(storage_path, raw_f64)
            with self.assertRaisesRegex(gate.GateError, "frozen Gorilla contract"):
                gate.validate_storage_report(storage_path, local_expectations)
            decoded = storage["decoded_semantic_fingerprint"]
            storage["decoded_semantic_fingerprint"] = "invalid"
            write_json(storage_path, storage)
            with self.assertRaisesRegex(gate.GateError, "decoded_semantic_fingerprint"):
                gate.validate_storage_report(storage_path, local_expectations)
            storage["decoded_semantic_fingerprint"] = decoded
            storage["chunk_inventory"]["by_kind_encoding"][0]["indexed_bytes"] -= 1
            write_json(storage_path, storage)
            with self.assertRaisesRegex(gate.GateError, "indexed bytes do not reconcile"):
                gate.validate_storage_report(storage_path, local_expectations)
            storage = current_storage_report(expectations["storage_verifier"])
            storage["verified_selection_fingerprint"] = "0" * 64
            write_json(storage_path, storage)
            with self.assertRaisesRegex(gate.GateError, "semantics"):
                gate.validate_storage_report(storage_path, local_expectations)

            rows = [
                ["Float", f"`up{{instance=\"{index}\"}}`", "1", "1", "1", "1", "1", "8", "1", "0", "0"]
                for index in range(14)
            ]
            fingerprint = hashlib.sha256(
                json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode()
            ).hexdigest()
            expectations["readbacks"] = {
                "expected_queries": 38,
                "executed_queries": 38,
                "skipped_queries": 0,
                "isolation_check_skips": 0,
                "mismatches": 0,
                "promql_rows": 14,
                "promql_rows_fingerprint_sha256": fingerprint,
            }
            write_json(local_expectations, expectations)
            report_rows = "\n".join("| " + " | ".join(row) + " |" for row in rows)
            report = root / "readbacks.md"
            report.write_text(
                "# Query Smoke\n\n## PromQL Readbacks\n\n"
                "| " + " | ".join(gate.PROMQL_READBACK_HEADER) + " |\n"
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n"
                f"{report_rows}\n\n"
                "## Readback Verification\n\n| Metric | Value |\n| --- | ---: |\n"
                "| Checked Queries | 38 |\n| Mismatches | 0 |\n\n"
                "## Query Diagnostics\n\n| Metric | Value |\n| --- | ---: |\n"
                "| Expected Readback Queries | 38 |\n| Executed Readback Queries | 38 |\n"
                "| Skipped Readback Queries | 0 |\n| Isolation Check Skips | 0 |\n",
                encoding="utf-8",
            )
            self.assertEqual(
                gate.validate_readbacks(report, local_expectations)["promql_rows"], 14
            )
            report.write_text(
                report.read_text(encoding="utf-8").replace(
                    "| Skipped Readback Queries | 0 |", "| Skipped Readback Queries | 1 |"
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "readbacks"):
                gate.validate_readbacks(report, local_expectations)

    def test_storage_completeness_rejects_replay_persistence_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            expectations = json.loads(EXPECTATIONS_PATH.read_text(encoding="utf-8"))
            storage = current_storage_report(expectations["storage_verifier"])
            expectations["storage_verifier"]["chunks_by_kind"] = storage[
                "chunks_by_kind"
            ]
            correctness = copy.deepcopy(expectations["replay_correctness"])
            storage_path = root / "storage.json"
            correctness_path = root / "correctness.json"
            expectations_path = root / "expectations.json"
            write_json(storage_path, storage)
            write_json(correctness_path, correctness)
            write_json(expectations_path, expectations)
            self.assertTrue(
                gate.check_storage_completeness(
                    storage_path, correctness_path, expectations_path
                )["complete"]
            )
            correctness["general"]["Recorded Samples"] -= 1
            expectations["replay_correctness"] = correctness
            write_json(correctness_path, correctness)
            write_json(expectations_path, expectations)
            with self.assertRaisesRegex(gate.GateError, "sample count differs"):
                gate.check_storage_completeness(
                    storage_path, correctness_path, expectations_path
                )

    def test_runner_checks_storage_completeness_before_readbacks(self) -> None:
        source = (HERE / "phase5_allocator_full_run.sh").read_text(encoding="utf-8")
        function_start = source.index("run_validation() {")
        function_end = source.index("\n}\nrun_validation stats-candidate", function_start)
        function = source[function_start:function_end]
        storage_verify = function.index('>"$validation_dir/storage-verify.json"')
        completeness = function.index("check-storage-completeness", storage_verify)
        readbacks = function.index(
            '"$RUN_QUERY" --segments-dir "$run_dir/segments"', completeness
        )
        self.assertLess(storage_verify, completeness)
        self.assertLess(completeness, readbacks)


if __name__ == "__main__":
    unittest.main()
