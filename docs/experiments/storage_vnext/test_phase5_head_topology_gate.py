#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import json
import os
import py_compile
import subprocess
import sys
import tarfile
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("phase5_head_topology_gate.py")
GATE = types.ModuleType("phase5_head_topology_gate")
GATE.__file__ = str(MODULE_PATH)
exec(compile(MODULE_PATH.read_bytes(), str(MODULE_PATH), "exec"), GATE.__dict__)


def series_values(adaptive: bool, *, skew: bool = False) -> dict[str, object]:
    direct_pages = 4 if adaptive and skew else (1 if adaptive else 0)
    direct_series = direct_pages * 128
    return {
        "windows": 4,
        "in_order_windows": 3,
        "in_order_rotations": 2,
        "out_of_order_windows": 1,
        "adaptive_windows": 4 if adaptive else 0,
        "series_total": 4096,
        "direct_pages_total": direct_pages,
        "direct_series_total": direct_series,
        "direct_series_ratio": direct_series / 4096,
        "sparse_pages_total": 7 if adaptive else 0,
        "sparse_series_total": 4096 - direct_series,
        "refs_above_paged_limit_total": 0,
        "max_page_directory_len": 8 if adaptive else 0,
        "max_page_directory_capacity": 8 if adaptive else 0,
        "max_sparse_capacity": 8192 if not adaptive else 4096,
        "max_sparse_slot_capacity": 64 if adaptive else 0,
        "max_direct_slot_index_bytes": direct_pages * 8192,
        "max_direct_reverse_slot_capacity": direct_series,
        "max_direct_value_capacity": direct_series,
    }


def last_values(adaptive: bool, *, skew: bool = False) -> dict[str, object]:
    dense_pages = 1 if adaptive and skew else 0
    dense_series = 2048 if dense_pages else 0
    return {
        "adaptive": adaptive,
        "series": 4096,
        "dense_pages": dense_pages,
        "dense_series": dense_series,
        "dense_series_ratio": dense_series / 4096,
        "sparse_pages": 8 if adaptive else 0,
        "sparse_series": 4096 - dense_series,
        "refs_above_paged_limit": 0,
        "page_directory_len": 8 if adaptive else 0,
        "page_directory_capacity": 8 if adaptive else 0,
        "sparse_capacity": 8192 if not adaptive else 4096,
        "paged_allocated_bytes": 65_536 if adaptive else 0,
    }


def markdown_report(
    path: Path,
    series_adaptive: bool,
    last_adaptive: bool,
    *,
    skew: bool = False,
) -> None:
    lines = ["# Ingestion report", "", "## Head Buffer Stats (by partition)", ""]
    for partition in range(16):
        lines.extend(
            [
                f"### Partition metrics:{partition}",
                "",
                "#### Series Table Structure",
                "",
                "| Metric | Value |",
                "|---|---:|",
            ]
        )
        for key, value in series_values(series_adaptive, skew=skew).items():
            rendered = f"{value:.6f}" if isinstance(value, float) else str(value)
            lines.append(f"| {key} | {rendered} |")
        lines.extend(
            [
                "",
                "#### Last Timestamp Table Structure",
                "",
                "| Metric | Value |",
                "|---|---:|",
            ]
        )
        for key, value in last_values(last_adaptive, skew=skew).items():
            if isinstance(value, bool):
                rendered = str(value).lower()
            elif isinstance(value, float):
                rendered = f"{value:.6f}"
            else:
                rendered = str(value)
            lines.append(f"| {key} | {rendered} |")
        lines.append("")
    lines.extend(["## Other Section", "", "ignored"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def structure(
    path: Path,
    series_adaptive: bool,
    last_adaptive: bool,
    *,
    skew: bool = False,
) -> dict[str, object]:
    markdown_report(path, series_adaptive, last_adaptive, skew=skew)
    return GATE.parse_head_report(path)


def write_json(path: Path, value: dict[str, object]) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def write_guardian_lifecycle_fixture(root: Path) -> None:
    guardian_ready = root / "guardian-ready"
    rss_ready = root / "rss-ready"
    launch = root / "launch"
    for marker in (guardian_ready, rss_ready, launch):
        marker.touch()
        marker.chmod(0o444)
    control_path = root / "lifecycle-control.json"
    control = {
        "schema": GATE.LIFECYCLE_CONTROL_SCHEMA,
        "root_pid": 10,
        "root_starttime_ticks": 100,
        "guardian_pid": 20,
        "guardian_starttime_ticks": 200,
        "rss_monitor_pid": 30,
        "rss_monitor_starttime_ticks": 300,
        "interval_ms": 100,
        "guardian_ready_marker": str(guardian_ready),
        "rss_ready_marker": str(rss_ready),
        "launch_marker": str(launch),
    }
    write_json(control_path, control)
    control_path.chmod(0o444)
    scan = root / "processes-immediately-before-launch.json"
    write_json(
        scan,
        {
            "schema": GATE.LIFECYCLE_CONFLICT_SCAN_SCHEMA,
            "conflicts": [],
            "quiet": True,
        },
    )
    scan.chmod(0o444)
    (root / "process-guardian.tsv").write_text(
        "poll_index\tmonotonic_elapsed_ns\trecorded_at\tpid\tppid\tstate\t"
        "starttime_ticks\tname\tcommand\n",
        encoding="utf-8",
    )
    timestamps = [1, 100_000_001, 200_000_001]
    (root / "disk-guardian.tsv").write_text(
        "poll_index\tmonotonic_elapsed_ns\trecorded_at\troot_running\t"
        "launch_observed\tfree_bytes\tminimum_free_bytes\n"
        "1\t1\t2026-07-22T00:00:00+00:00\ttrue\tfalse\t200\t100\n"
        "2\t100000001\t2026-07-22T00:00:00.1+00:00\ttrue\ttrue\t190\t100\n"
        "3\t200000001\t2026-07-22T00:00:00.2+00:00\tfalse\ttrue\t180\t100\n",
        encoding="utf-8",
    )
    write_json(
        root / "guardian-summary.json",
        {
            "schema": GATE.LIFECYCLE_GUARDIAN_SCHEMA,
            "root_pid": 10,
            "root_starttime_ticks": 100,
            "guardian_pid": 20,
            "interval_ms": 100,
            "polls": 3,
            "terminal_elapsed_ns": 200_000_002,
            "poll_monotonic_elapsed_ns": timestamps,
            "maximum_poll_start_gap_ns": 100_000_000,
            "maximum_allowed_poll_start_gap_ns": 200_000_000,
            "control_path": str(control_path),
            "control_sha256": GATE._sha256(control_path),
            "guardian_ready_marker": str(guardian_ready),
            "rss_ready_marker": str(rss_ready),
            "launch_marker": str(launch),
            "ready_created_poll": 1,
            "ready_created_monotonic_elapsed_ns": 1,
            "launch_observed_poll": 2,
            "launch_observed_monotonic_elapsed_ns": 100_000_001,
            "root_seen": True,
            "filesystem": str(root),
            "minimum_free_bytes": 100,
            "minimum_observed_free_bytes": 180,
            "capacity_violations": [],
            "conflicts": [],
            "handshake_violations": [],
            "termination": GATE._empty_lifecycle_termination(100),
            "complete_and_conflict_free": True,
        },
    )
    (root / "guardian.log").write_text("", encoding="utf-8")


def write_rss_lifecycle_fixture(root: Path) -> None:
    control_path = root / "lifecycle-control.json"
    timestamps = [1, 100_000_001, 200_000_001]
    (root / "rss-samples.tsv").write_text(
        "poll_index\tmonotonic_elapsed_ns\trecorded_at\troot_running\t"
        "launch_observed\tprocess_count\trss_kib\trss_anon_kib\t"
        "rss_file_kib\tvm_swap_kib\tmax_single_hwm_kib\tpids\n"
        "1\t1\t2026-07-22T00:00:00+00:00\ttrue\tfalse\t1\t10\t6\t4\t0\t12\t10\n"
        "2\t100000001\t2026-07-22T00:00:00.1+00:00\ttrue\ttrue\t2\t30\t18\t12\t1\t22\t10,11\n"
        "3\t200000001\t2026-07-22T00:00:00.2+00:00\tfalse\ttrue\t0\t0\t0\t0\t0\t0\t\n",
        encoding="utf-8",
    )
    write_json(
        root / "rss-summary.json",
        {
            "schema": GATE.LIFECYCLE_RSS_SCHEMA,
            "root_pid": 10,
            "root_starttime_ticks": 100,
            "rss_monitor_pid": 30,
            "samples": 3,
            "interval_ms": 100,
            "process_count": 2,
            "aggregate_rss_kib": 30,
            "aggregate_rss_anon_kib": 18,
            "aggregate_rss_file_kib": 12,
            "aggregate_vm_swap_kib": 1,
            "max_single_process_hwm_kib": 22,
            "terminal_elapsed_ns": 200_000_002,
            "poll_monotonic_elapsed_ns": timestamps,
            "maximum_poll_start_gap_ns": 100_000_000,
            "maximum_allowed_poll_start_gap_ns": 200_000_000,
            "control_path": str(control_path),
            "control_sha256": GATE._sha256(control_path),
            "guardian_ready_marker": str(root / "guardian-ready"),
            "rss_ready_marker": str(root / "rss-ready"),
            "launch_marker": str(root / "launch"),
            "ready_created_poll": 1,
            "ready_created_monotonic_elapsed_ns": 1,
            "launch_observed_poll": 2,
            "launch_observed_monotonic_elapsed_ns": 100_000_001,
            "root_seen": True,
            "handshake_violations": [],
            "termination": GATE._empty_lifecycle_termination(100),
            "complete": True,
        },
    )
    (root / "rss-monitor.log").write_text("", encoding="utf-8")


def storage_report(
    marker: str,
    *,
    samples: int = 100,
    physical_scale: int = 1,
    semantic_marker: str = "c",
) -> dict[str, object]:
    chunks_by_kind = [7 * physical_scale, 0, 2 * physical_scale, physical_scale, 0]
    chunks = sum(chunks_by_kind)
    return {
        "schema_version": 8,
        "footer_validation_enabled": True,
        "series_sample_per_segment": None,
        "verified_selection_fingerprint": marker * 64,
        "decoded_semantic_fingerprint": marker * 64,
        "topology_independent_decoded_semantic_fingerprint": semantic_marker * 64,
        "segments": physical_scale,
        "corpus_series": 8 * physical_scale,
        "series": 8 * physical_scale,
        "chunks": chunks,
        "chunks_by_kind": chunks_by_kind,
        "samples": samples,
        "logical_chunk_bytes": 4096 * physical_scale,
        "exact_postings": {
            "logical_fingerprint": marker * 64,
            "lists": 3 * physical_scale,
            "decoded_refs": 16 * physical_scale,
            "encoded_bytes": 64 * physical_scale,
        },
        "elapsed_ns": 1000 * physical_scale,
        "metadata_read_calls": 10 * physical_scale,
        "metadata_read_bytes": 1024 * physical_scale,
        "metadata_peak_retained_bytes": 512,
        "metadata_peak_in_flight_bytes": 1024,
        "metadata_peak_open_files": physical_scale,
        "metadata_cache_hits": 0,
        "metadata_cache_misses": 5 * physical_scale,
    }


def performance_fixture(
    root: Path,
    series_task_ratio: float,
    last_task_ratio: float,
    series_rss_ratio: float,
    last_rss_ratio: float,
) -> None:
    write_run_plan(root)
    for run in GATE.EXPECTED_RUNS:
        run_dir = root / "runs" / run
        run_dir.mkdir(parents=True)
        _topology, cell, series_adaptive, last_adaptive = GATE._run_factors(run)
        task_ratio = (series_task_ratio if series_adaptive else 1.0) * (
            last_task_ratio if last_adaptive else 1.0
        )
        rss_ratio = (series_rss_ratio if series_adaptive else 1.0) * (
            last_rss_ratio if last_adaptive else 1.0
        )
        write_json(
            run_dir / "perf-stat.json",
            {
                "events": [
                    {
                        "event": "task-clock",
                        "raw_value": str(100.0 * task_ratio),
                        "unit": "msec",
                        "available": True,
                    }
                ]
            },
        )
        write_json(
            run_dir / "replay.time.json",
            {"max_rss_kib": int(1000 * rss_ratio)},
        )
        write_json(
            run_dir / "rss-summary.json",
            {"aggregate_rss_kib": int(900 * rss_ratio)},
        )


def write_run_plan(root: Path) -> None:
    lines = [
        "order\trun\ttopology\tcell\tadaptive_series_table\t"
        "adaptive_last_timestamp_table\tcapture\tconfig\tsegments"
    ]
    for index, run in enumerate(GATE.EXPECTED_RUNS, 1):
        topology, cell, series_adaptive, last_adaptive = GATE._run_factors(run)
        lines.append(
            "\t".join(
                (
                    str(index),
                    run,
                    topology,
                    cell,
                    str(series_adaptive).lower(),
                    str(last_adaptive).lower(),
                    str(root / "captures" / topology),
                    str(root / "configs" / f"{run}.toml"),
                    str(root / "runs" / run / "segments"),
                )
            )
        )
    root.joinpath("run-plan.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_replay_summary(root: Path) -> None:
    lines = [
        "run\ttopology\tcell\tadaptive_series_table\tadaptive_last_timestamp_table\t"
        "elapsed\tuser_seconds\tsystem_seconds\tmax_rss_kib\t"
        "proc_peak_rss_kib\tcorpus_files\tcorpus_bytes\tmanifest_sha256"
    ]
    for run in GATE.EXPECTED_RUNS:
        topology, cell, series_adaptive, last_adaptive = GATE._run_factors(run)
        lines.append(
            "\t".join(
                (
                    run,
                    topology,
                    cell,
                    str(series_adaptive).lower(),
                    str(last_adaptive).lower(),
                    "0:01.00",
                    "1.0",
                    "0.1",
                    "100",
                    "90",
                    "7",
                    "4096",
                    "a" * 64,
                )
            )
        )
    root.joinpath("replay-summary.tsv").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def populate_minimal_final_result(
    root: Path, *, disposition: str = "defer", complete: bool = True
) -> set[str]:
    for name in (
        "build-source",
        "build-state",
        "build-target",
        "captures",
        "comparisons",
        "configs",
        "inventory",
        "metadata",
        "runs",
        "sizing",
        "validation",
    ):
        (root / name).mkdir()
    write_run_plan(root)
    write_replay_summary(root)
    evidence = root / "metadata" / "evidence.txt"
    evidence.write_text("sealed\n", encoding="utf-8")
    artifacts = {"run-plan.tsv", "replay-summary.tsv", "metadata/evidence.txt"}
    manifest = root / "metadata" / "result-artifacts.sha256"
    manifest.write_text(
        "".join(
            f"{GATE._sha256(root / relative)}  {relative}\n"
            for relative in sorted(artifacts)
        ),
        encoding="utf-8",
    )
    for marker in (
        "SIZING_GATE_PASSED",
        "SEED_CAPACITY_GATE_PASSED",
        GATE.PERFORMANCE_MARKERS[disposition],
    ):
        (root / marker).touch()
    if complete:
        evidence_result = {
            "schema": GATE.FINAL_SEAL_SCHEMA,
            "stage": "evidence",
            "artifact_count": len(artifacts),
            "manifest_sha256": GATE._sha256(manifest),
            "performance_disposition": disposition,
            "validated": True,
        }
        (root / "FINAL_SEAL_VALIDATED").write_text(
            json.dumps(evidence_result, sort_keys=True) + "\n", encoding="utf-8"
        )
        (root / "COMPLETE").write_text(GATE.COMPLETE_MARKER, encoding="utf-8")
    return artifacts


class HeadTopologyGateTests(unittest.TestCase):
    def test_source_seal_rejects_dirty_untracked_hidden_and_nonregular_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            repo = Path(raw)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "config", "user.name", "Phase 5 Test"], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.email", "phase5@example.invalid"],
                check=True,
            )
            (repo / "Cargo.lock").write_text("# lock\n", encoding="utf-8")
            (repo / ".gitignore").write_text("ignored.rs\n.cargo/config\n", encoding="utf-8")
            tracked = repo / "tracked.rs"
            tracked.write_text("pub const SEALED: bool = true;\n", encoding="utf-8")
            subprocess.run(
                ["git", "-C", str(repo), "add", "Cargo.lock", ".gitignore", "tracked.rs"],
                check=True,
            )
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "seal"], check=True)
            seal = GATE.source_seal(repo)
            seal_path = repo.parent / f"{repo.name}-seal.json"
            write_json(seal_path, seal)
            self.assertEqual(GATE.check_source_seal(repo, seal_path)["status"], "pass")

            tracked.write_text("dirty\n", encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "clean tracked worktree"):
                GATE.source_seal(repo)
            tracked.write_text("pub const SEALED: bool = true;\n", encoding="utf-8")

            (repo / "build.rs").write_text("fn main() {}\n", encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "untracked build inputs"):
                GATE.source_seal(repo)
            (repo / "build.rs").unlink()
            (repo / "ignored.rs").write_text("pub const HIDDEN: bool = true;\n", encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "ignored source/build input"):
                GATE.source_seal(repo)
            (repo / "ignored.rs").unlink()
            (repo / ".cargo").mkdir()
            (repo / ".cargo" / "config").write_text(
                "[build]\nrustflags = ['-C', 'target-cpu=native']\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(GATE.GateError, "ignored source/build input"):
                GATE.source_seal(repo)
            (repo / ".cargo" / "config").unlink()
            (repo / ".cargo").rmdir()

            subprocess.run(
                ["git", "-C", str(repo), "update-index", "--assume-unchanged", "tracked.rs"],
                check=True,
            )
            tracked.write_text("hidden mutation\n", encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "nonordinary Git index flag 'h'"):
                GATE.source_seal(repo)
            subprocess.run(
                ["git", "-C", str(repo), "update-index", "--no-assume-unchanged", "tracked.rs"],
                check=True,
            )
            tracked.write_text("pub const SEALED: bool = true;\n", encoding="utf-8")

            subprocess.run(
                ["git", "-C", str(repo), "update-index", "--skip-worktree", "tracked.rs"],
                check=True,
            )
            with self.assertRaisesRegex(GATE.GateError, "nonordinary Git index flag 'S'"):
                GATE.source_seal(repo)
            subprocess.run(
                ["git", "-C", str(repo), "update-index", "--no-skip-worktree", "tracked.rs"],
                check=True,
            )

            (repo / "tracked-link").symlink_to("tracked.rs")
            subprocess.run(["git", "-C", str(repo), "add", "tracked-link"], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "track link"], check=True)
            with self.assertRaisesRegex(GATE.GateError, "unsupported tracked Git mode 120000"):
                GATE.source_seal(repo)

    def test_archive_extraction_is_exact_read_only_and_rejects_unsafe_members(self) -> None:
        with tempfile.TemporaryDirectory(dir="/var/tmp") as raw:
            root = Path(raw)
            repo = root / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.name", "Phase 5 Test"],
                check=True,
            )
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "config",
                    "user.email",
                    "phase5@example.invalid",
                ],
                check=True,
            )
            (repo / "Cargo.lock").write_text("# lock\n", encoding="utf-8")
            (repo / ".gitignore").write_text("payload.bin\n", encoding="utf-8")
            (repo / "tracked.rs").write_text(
                'pub static BYTES: &[u8] = include_bytes!("payload.bin");\n',
                encoding="utf-8",
            )
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "archive"], check=True)
            (repo / "payload.bin").write_bytes(b"ambient ignored build input")

            archive = root / "source.tar"
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
            snapshot = root / "snapshot"
            archive_seal = GATE.extract_source_archive(repo, archive, snapshot)
            self.assertNotIn("payload.bin", {row["path"] for row in archive_seal["files"]})
            self.assertEqual(snapshot.stat().st_mode & 0o777, 0o555)
            self.assertEqual((snapshot / "tracked.rs").stat().st_mode & 0o777, 0o444)
            snapshot_seal = GATE.source_snapshot_seal(repo, snapshot)
            self.assertEqual(archive_seal["git_tree"], snapshot_seal["git_tree"])
            GATE._validate_archive_document(archive, archive_seal)
            GATE._validate_snapshot_document(snapshot, snapshot_seal)

            (snapshot / "tracked.rs").chmod(0o644)
            (snapshot / "tracked.rs").write_text("mutated\n", encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "mode differs|bytes differ"):
                GATE.source_snapshot_seal(repo, snapshot)

            unsafe = root / "unsafe.tar"
            with tarfile.open(unsafe, "w") as output:
                member = tarfile.TarInfo("../escape")
                payload = b"escape"
                member.size = len(payload)
                output.addfile(member, io.BytesIO(payload))
            unsafe.chmod(0o444)
            with self.assertRaisesRegex(GATE.GateError, "unsafe path"):
                GATE._validate_source_archive(repo, unsafe)

    def test_ambient_runtime_and_capture_contracts_fail_closed(self) -> None:
        forbidden = GATE.forbidden_ambient_environment(
            {
                "PATH": os.environ.get("PATH", ""),
                "CONFIG_FILE": "/tmp/config",
                "CARGO_PROFILE_RELEASE_LTO": "true",
                "JEMALLOC_SYS_WITH_MALLOC_CONF": "background_thread:true",
                "LD_PRELOAD": "allocator.so",
            }
        )
        self.assertEqual(
            forbidden,
            [
                "CARGO_PROFILE_RELEASE_LTO",
                "CONFIG_FILE",
                "JEMALLOC_SYS_WITH_MALLOC_CONF",
                "LD_PRELOAD",
            ],
        )
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            binary = root / "chronoxide-ingester"
            binary.write_bytes(b"binary")
            binary.chmod(0o555)
            identity = GATE.runtime_identity(
                binary,
                "ingester",
                ["LC_ALL=C", "TZ=UTC", "CONFIG_FILE=/config", "RUST_LOG=info"],
                ["--example"],
            )
            self.assertEqual(identity["role"], "ingester")
            identity_path = root / "runtime.json"
            write_json(identity_path, identity)
            GATE._validate_runtime_identity_document(
                identity_path,
                "ingester",
                binary,
                expected_environment={
                    "LC_ALL": "C",
                    "TZ": "UTC",
                    "CONFIG_FILE": "/config",
                    "RUST_LOG": "info",
                },
                expected_arguments=["--example"],
            )
            with self.assertRaisesRegex(GATE.GateError, "run plan"):
                GATE._validate_runtime_identity_document(
                    identity_path,
                    "ingester",
                    binary,
                    expected_environment={
                        "LC_ALL": "C",
                        "TZ": "UTC",
                        "CONFIG_FILE": "/fabricated",
                        "RUST_LOG": "info",
                    },
                    expected_arguments=["--example"],
                )
            with self.assertRaisesRegex(GATE.GateError, "argv"):
                GATE._validate_runtime_identity_document(
                    identity_path,
                    "ingester",
                    binary,
                    expected_arguments=[],
                )
            with self.assertRaisesRegex(GATE.GateError, "sanitized contract"):
                GATE.runtime_identity(binary, "query", ["LC_ALL=C", "TZ=UTC", "RUST_LOG=x"], [])

            capture = root / "capture"
            capture.mkdir()
            (capture / "part.capture").write_bytes(b"before")
            GATE.capture_inventory(capture, root / "before.json", root / "before.nul")
            (capture / "part.capture").write_bytes(b"after")
            GATE.capture_inventory(capture, root / "after.json", root / "after.nul")
            self.assertNotEqual(
                json.loads((root / "before.json").read_text(encoding="utf-8"))["files_sha256"],
                json.loads((root / "after.json").read_text(encoding="utf-8"))["files_sha256"],
            )

    def test_static_process_classifier_covers_build_profiler_database_and_variants(self) -> None:
        runner = MODULE_PATH.with_name("phase5_head_topology_run.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("validate-process-snapshot", runner)
        for name in (
            "cargo",
            "perf",
            "prometheus",
            "adb",
            "cargo-nextest",
            "ninja.real",
            "ld.bfd",
            "ld.gold",
            "clang++.real",
            "clang-19.real",
            "gcc-14",
            "soong_ui.bash",
            "btop",
            "htop",
            "top",
        ):
            self.assertTrue(GATE.is_forbidden_process(name, name), name)
        for command in (
            "bash /src/build/soong/soong_ui.bash --make-mode",
            "python /home/u/.cargo/bin/cargo-nextest nextest run",
            "bash /src/prebuilts/build-tools/bin/ninja.real -C out",
            "bash /usr/bin/ld.bfd -o output",
            "bash /android/prebuilts/clang++.real -c source.cc",
            "bash /opt/llvm/bin/clang-19.real -c source.cc",
            "worker Android SDK emulator launch",
        ):
            self.assertTrue(GATE.is_forbidden_process("bash", command), command)
        self.assertFalse(GATE.is_forbidden_process("bash", "bash worker.sh"))
        for lookalike in ("topic-worker", "toplevel", "htopology", "btopology", "adbd"):
            self.assertFalse(GATE.is_forbidden_process(lookalike, lookalike), lookalike)

    def test_static_and_continuous_process_evidence_is_readmitted_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            snapshot = root / "processes.txt"
            snapshot.write_text("10 1 bash bash worker.sh\n", encoding="utf-8")
            self.assertTrue(GATE.validate_process_snapshot(snapshot)["validated"])
            snapshot.write_text(
                "10 1 bash bash /src/build/soong/soong_ui.bash --make-mode\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(GATE.GateError, "measurement conflict"):
                GATE.validate_process_snapshot(snapshot)

            write_guardian_lifecycle_fixture(root)
            GATE._validate_guardian_evidence(root, "fixture")
            with (root / "process-guardian.tsv").open("a", encoding="utf-8") as output:
                output.write(
                    "2\t100000001\t2026-07-22T00:00:01+00:00\t20\t1\tS\t"
                    "200\tninja.real\tninja.real -C out\n"
                )
            with self.assertRaisesRegex(GATE.GateError, "recorded a conflict"):
                GATE._validate_guardian_evidence(root, "fixture")

    def test_rss_lifecycle_reconstructs_first_launch_and_terminal_edges(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            write_guardian_lifecycle_fixture(root)
            write_rss_lifecycle_fixture(root)
            summary = GATE._validate_rss_summary(
                root / "rss-samples.tsv", root / "rss-summary.json"
            )
            self.assertEqual(summary["aggregate_rss_kib"], 30)
            samples = root / "rss-samples.tsv"
            text = samples.read_text(encoding="utf-8")
            samples.write_text(
                text.replace(
                    "3\t200000001\t2026-07-22T00:00:00.2+00:00\tfalse",
                    "3\t200000001\t2026-07-22T00:00:00.2+00:00\ttrue",
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(GATE.GateError, "terminal|sequence|root"):
                GATE._validate_rss_summary(samples, root / "rss-summary.json")

    def test_guardian_rejects_first_and_terminal_cadence_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            write_guardian_lifecycle_fixture(root)
            summary_path = root / "guardian-summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["terminal_elapsed_ns"] = 500_000_001
            summary["maximum_poll_start_gap_ns"] = 300_000_000
            write_json(summary_path, summary)
            with self.assertRaisesRegex(GATE.GateError, "summary|cadence"):
                GATE._validate_guardian_evidence(root, "fixture")

    def test_runner_uses_bounded_prefixes_and_seed_derived_disk_budget(self) -> None:
        runner = MODULE_PATH.with_name("phase5_head_topology_run.sh").read_text(
            encoding="utf-8"
        )
        for contract in (
            "FULL_CAPTURE_COUNT=2",
            "BOUNDED_PREFIX_OUTPUT_COUNT=4",
            "MIN_SAFETY_RESERVE_BYTES=17179869184",
            'captures/determinism/$topology-$variant',
            'captures/$topology"',
            "uniform_seed_bytes",
            "skew_seed_bytes",
            "3 * uniform_seed_bytes + 3 * skew_seed_bytes",
            "SEED_CAPACITY_GATE_PASSED",
            "SIZING_MESSAGES=\"${SIZING_MESSAGES:-250000}\"",
            "topology-sizing.tsv",
            "transient_rewrite_headroom_bytes",
            "phase5_head_topology_guard.py",
            "PARTIAL_MEASUREMENT_GUARD_BLOCKED",
            "PARTIAL_DISK_BUDGET_BLOCKED",
            '"$SYNC_BIN" -f',
            "capture-writeback-before.tsv",
            "generated-capture-writeback.tsv",
            "cargo build --locked --release",
            "CARGO_INCREMENTAL=0",
            "source-seal",
            "git -C \"$REPO_ROOT\" archive",
            "extract-source-archive",
            "source-archive-seal.json",
            "source-snapshot-seal",
            'cd "$BUILD_SOURCE_DIR"',
            "runtime-identity",
            "capture-after-runs",
            "validate-final-seal",
            "--stage evidence",
            "--stage complete",
            "run-plan.tsv",
            "replay-summary.tsv",
        ):
            self.assertIn(contract, runner)
        self.assertNotIn("CORPUS_BYTES_PER_RUN", runner)
        self.assertIn("sum(range(10000000))", runner)
        self.assertNotIn("-- true", runner)

    def test_runner_holds_every_workload_until_both_monitors_are_ready(self) -> None:
        runner = MODULE_PATH.with_name("phase5_head_topology_run.sh").read_text(
            encoding="utf-8"
        )
        lifecycle = runner[runner.index("run_held_workload() {") :]
        held_spawn = lifecycle.index('while [[ ! -e "$launch"')
        rss_spawn = lifecycle.index('monitor-rss --root-pid "$root_pid"')
        guardian_spawn = lifecycle.index('monitor-guardian --root-pid "$root_pid"')
        control = lifecycle.index("create-control --output")
        ready = lifecycle.index("wait-ready --control")
        release = lifecycle.index("release-launch --control")
        self.assertLess(held_spawn, rss_spawn)
        self.assertLess(rss_spawn, guardian_spawn)
        self.assertLess(guardian_spawn, control)
        self.assertLess(control, ready)
        self.assertLess(ready, release)
        self.assertIn('"$(stat -c \'%a\' -- "$launch")" == 444', lifecycle)
        self.assertIn('--guardian-ready "$guardian_ready" --rss-ready "$rss_ready"', lifecycle)
        self.assertIn("trap 'cleanup_on_exit' EXIT", runner)
        self.assertIn("trap 'cleanup_signal_exit' HUP INT TERM", runner)
        self.assertIn("defer_cleanup_signals", lifecycle)
        self.assertIn("bind_live_starttime_ticks", lifecycle)
        self.assertIn(
            'identity="$(read_process_state_starttime_ticks "$1")" || return 1',
            runner,
        )
        self.assertNotIn(
            '<<<"$(read_process_state_starttime_ticks', runner
        )
        self.assertIn('"$state" != x', runner)
        self.assertIn('run_held_workload "$transform_label"', runner)
        self.assertIn('run_held_workload "sizing-$topology"', runner)
        self.assertIn('run_held_workload "$run"', runner)
        early_scan = runner.index("metadata/processes-before-transforms.json")
        first_transform = runner.index(
            'note "creating bounded independent Zstd-prefix transforms'
        )
        self.assertLess(early_scan, first_transform)
        guard = MODULE_PATH.with_name("phase5_head_topology_guard.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("terminate_owned_tree", guard)
        self.assertIn("terminate_root_from_control", guard)

    def test_exact_artifact_matrix_contains_every_successful_lifecycle(self) -> None:
        artifacts = GATE._formal_fixed_artifacts()
        lifecycle = {
            "lifecycle-control.json",
            "guardian-ready",
            "rss-ready",
            "launch",
            "guardian-summary.json",
            "disk-guardian.tsv",
            "process-guardian.tsv",
            "processes-immediately-before-launch.json",
            "rss-samples.tsv",
            "rss-summary.json",
            "guardian.exit-status",
            "rss-monitor.exit-status",
        }
        for run in GATE.EXPECTED_RUNS:
            self.assertTrue(
                {f"runs/{run}/{name}" for name in lifecycle} <= artifacts, run
            )
        for topology in GATE.TOPOLOGIES:
            self.assertTrue(
                {f"sizing/{topology}/{name}" for name in lifecycle} <= artifacts,
                topology,
            )
            self.assertIn(f"sizing/{topology}/disk-budget-before.tsv", artifacts)
        for label in GATE.TRANSFORM_LABELS:
            self.assertTrue(
                {
                    f"validation/transform-guards/{label}/{name}"
                    for name in lifecycle | {"workload.exit-status"}
                }
                <= artifacts,
                label,
            )
        self.assertIn("metadata/processes-before-transforms.json", artifacts)
        self.assertIn("validation/transform-capacity-plan.tsv", artifacts)
        self.assertFalse(any("guardian-violation" in path for path in artifacts))

    def test_runner_disables_python_bytecode_before_resolving_harness(self) -> None:
        runner = MODULE_PATH.with_name("phase5_head_topology_run.sh").read_text(
            encoding="utf-8"
        )
        script_dir = runner.index('SCRIPT_DIR=')
        for isolation in (
            "export PYTHONDONTWRITEBYTECODE=1",
            "export PYTHONNOUSERSITE=1",
            "unset PYTHONHOME PYTHONPATH PYTHONSTARTUP PYTHONUSERBASE",
            "python3() {",
            'command python3 -B -I -S "$@"',
        ):
            self.assertLess(runner.index(isolation), script_dir)
        self.assertIn('"$PYTHON_BIN" -B -I -S -c', runner)
        self.assertIn("check-frozen-harness", runner)
        self.assertIn('chmod 0555 -- "$RESULT_DIR/metadata/harness"', runner)
        self.assertNotIn('command python3 -I -S "$@"', runner)
        self.assertNotIn('"$PYTHON_BIN" -I -S -c', runner)
        self.assertIn("FINAL_ARTIFACT_PATHS=", runner)
        self.assertIn(
            '|| die "could not enumerate the complete formal artifact matrix"',
            runner,
        )

    def test_python_isolation_does_not_write_or_trust_sibling_bytecode(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            helper = root / "helper.py"
            helper.write_text("VALUE = 'safe'\n", encoding="utf-8")
            loader = root / "loader.py"
            loader.write_text(
                "import importlib.util, pathlib, sys\n"
                "path = pathlib.Path(sys.argv[1])\n"
                "spec = importlib.util.spec_from_file_location('helper', path)\n"
                "module = importlib.util.module_from_spec(spec)\n"
                "spec.loader.exec_module(module)\n",
                encoding="utf-8",
            )
            subprocess.run(
                [sys.executable, "-B", "-I", "-S", str(loader), str(helper)],
                check=True,
            )
            self.assertFalse((root / "__pycache__").exists())

            harness = root / "harness"
            harness.mkdir()
            for name in GATE.FROZEN_HARNESS_FILES:
                (harness / name).write_text("# frozen\n", encoding="utf-8")
            support = harness / "phase1_replay_gate.py"
            support.write_text("VALUE = 'evil'\n", encoding="utf-8")
            source_stat = support.stat()
            cache = Path(py_compile.compile(str(support), doraise=True))
            support.write_text("VALUE = 'safe'\n", encoding="utf-8")
            os.utime(
                support,
                ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns),
            )

            specification = importlib.util.spec_from_file_location("cached_support", support)
            self.assertIsNotNone(specification)
            self.assertIsNotNone(specification.loader)
            cached = importlib.util.module_from_spec(specification)
            specification.loader.exec_module(cached)
            self.assertEqual(cached.VALUE, "evil", "fixture must contain a valid planted cache")

            with mock.patch.object(
                GATE, "__file__", str(harness / "phase5_head_topology_gate.py")
            ):
                loaded = GATE._load_support_module("phase1_replay_gate.py")
            self.assertEqual(loaded.VALUE, "safe")

            for name in GATE.FROZEN_HARNESS_FILES:
                (harness / name).chmod(0o444)
            harness.chmod(0o555)
            with self.assertRaisesRegex(GATE.GateError, "path set differs"):
                GATE.check_frozen_harness(harness)

            harness.chmod(0o755)
            cache.unlink()
            cache.parent.rmdir()
            harness.chmod(0o555)
            result = GATE.check_frozen_harness(harness)
            self.assertTrue(result["cache_free"])
            self.assertTrue(result["read_only"])

    def test_filesystem_walk_errors_fail_closed(self) -> None:
        with self.assertRaisesRegex(GATE.GateError, "filesystem traversal failed: denied"):
            GATE._raise_walk_error(PermissionError("denied"))

    def test_run_plan_and_final_seal_reject_post_hoc_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            write_run_plan(root)
            plan = GATE.validate_run_plan(root, root / "run-plan.tsv")
            self.assertTrue(plan["validated"])
            lines = (root / "run-plan.tsv").read_text(encoding="utf-8").splitlines()
            lines[1], lines[2] = lines[2], lines[1]
            (root / "run-plan.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "predeclared matrix"):
                GATE.validate_run_plan(root, root / "run-plan.tsv")

            for child in list(root.iterdir()):
                if child.is_file():
                    child.unlink()
            artifacts = populate_minimal_final_result(root)
            patches = (
                mock.patch.object(GATE, "_formal_fixed_artifacts", return_value=artifacts),
                mock.patch.object(GATE, "_dynamic_formal_artifacts", return_value=set()),
                mock.patch.object(
                    GATE, "_validate_full_posthoc_contract", return_value="defer"
                ),
            )
            with patches[0], patches[1], patches[2]:
                self.assertTrue(GATE.validate_final_seal(root)["validated"])
                (root / "metadata" / "evidence.txt").write_text(
                    "tampered\n", encoding="utf-8"
                )
                with self.assertRaisesRegex(GATE.GateError, "digest mismatch"):
                    GATE.validate_final_seal(root)

    def test_final_contract_rejects_extra_fabricated_and_contradictory_evidence(self) -> None:
        def patches(artifacts: set[str], disposition: str = "defer") -> tuple[object, ...]:
            return (
                mock.patch.object(GATE, "_formal_fixed_artifacts", return_value=artifacts),
                mock.patch.object(GATE, "_dynamic_formal_artifacts", return_value=set()),
                mock.patch.object(
                    GATE, "_validate_full_posthoc_contract", return_value=disposition
                ),
            )

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artifacts = populate_minimal_final_result(root, complete=False)
            active = patches(artifacts)
            with active[0], active[1], active[2]:
                result = GATE.validate_final_seal(root, "evidence")
            self.assertEqual(result["stage"], "evidence")

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artifacts = populate_minimal_final_result(root)
            (root / "metadata" / "fabricated.txt").write_text("fake\n", encoding="utf-8")
            active = patches(artifacts)
            with active[0], active[1], active[2], self.assertRaisesRegex(
                GATE.GateError, "on-disk formal artifact matrix"
            ):
                GATE.validate_final_seal(root)

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artifacts = populate_minimal_final_result(root, disposition="promote")
            active = patches(artifacts, "defer")
            with active[0], active[1], active[2], self.assertRaisesRegex(
                GATE.GateError, "contradicts"
            ):
                GATE.validate_final_seal(root)

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artifacts = populate_minimal_final_result(root)
            (root / "PERFORMANCE_REJECT").touch()
            active = patches(artifacts)
            with active[0], active[1], active[2], self.assertRaisesRegex(
                GATE.GateError, "exactly one performance"
            ):
                GATE.validate_final_seal(root)

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artifacts = populate_minimal_final_result(root)
            (root / "FINAL_SEAL_VALIDATED").write_text("fabricated\n", encoding="utf-8")
            active = patches(artifacts)
            with active[0], active[1], active[2], self.assertRaisesRegex(
                GATE.GateError, "FINAL_SEAL_VALIDATED"
            ):
                GATE.validate_final_seal(root)

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artifacts = populate_minimal_final_result(root)
            (root / "unexpected-root").mkdir()
            active = patches(artifacts)
            with active[0], active[1], active[2], self.assertRaisesRegex(
                GATE.GateError, "root allowlist"
            ):
                GATE.validate_final_seal(root)

    def test_parse_and_matrix_accept_complete_sparse_promotion_rotation_ooo_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            documents: dict[tuple[str, str], Path] = {}
            for topology, skew in (("uniform", False), ("skew", True)):
                for cell, factors in GATE.CELL_FACTORS.items():
                    report = root / f"{topology}-{cell}.md"
                    parsed = structure(report, *factors, skew=skew)
                    parsed_path = root / f"{topology}-{cell}.json"
                    write_json(parsed_path, parsed)
                    documents[(topology, cell)] = parsed_path
            result = GATE.gate_matrix(
                *(
                    documents[(topology, cell)]
                    for topology in ("uniform", "skew")
                    for cell in GATE.CELL_FACTORS
                )
            )
            self.assertEqual(result["partition_count"], 16)
            self.assertTrue(all(result["coverage"].values()))
            self.assertEqual(result["uniform"]["out_of_order_windows"], 16)
            self.assertEqual(result["uniform"]["in_order_rotations"], 32)
            self.assertTrue(result["uniform"]["factor_isolation_exact"])

    def test_matrix_rejects_work_mismatch_and_missing_ooo(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cells = {
                cell: structure(root / f"{cell}.md", *factors, skew=True)
                for cell, factors in GATE.CELL_FACTORS.items()
            }
            cells["aa"]["partitions"]["metrics:0"]["series_table"]["series_total"] += 1
            with self.assertRaisesRegex(GATE.GateError, "work differs"):
                GATE._gate_factorial_topology("skew80-20", cells)

            cells = {
                cell: structure(root / f"{cell}-2.md", *factors, skew=True)
                for cell, factors in GATE.CELL_FACTORS.items()
            }
            for document in cells.values():
                for row in document["partitions"].values():
                    row["series_table"]["out_of_order_windows"] = 0
            with self.assertRaisesRegex(GATE.GateError, "OOO lane"):
                GATE._gate_factorial_topology("skew80-20", cells)

    def test_matrix_rejects_cross_factor_structure_perturbation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cells = {
                cell: structure(root / f"{cell}.md", *factors, skew=True)
                for cell, factors in GATE.CELL_FACTORS.items()
            }
            cells["pa"]["partitions"]["metrics:0"]["series_table"][
                "max_sparse_capacity"
            ] += 1
            with self.assertRaisesRegex(GATE.GateError, "last factor perturbed"):
                GATE._gate_factorial_topology("skew80-20", cells)

    def test_matrix_requires_two_real_rotations_not_one_rotation_plus_drain(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cells = {
                cell: structure(root / f"{cell}.md", *factors, skew=True)
                for cell, factors in GATE.CELL_FACTORS.items()
            }
            for document in cells.values():
                for row in document["partitions"].values():
                    # Three in-order windows include two completed rotations
                    # plus the final active-window drain. Reducing only the
                    # explicit event counter proves that drains cannot satisfy
                    # the long-lived gate.
                    row["series_table"]["in_order_rotations"] = 1
            with self.assertRaisesRegex(GATE.GateError, "long-lived rotation"):
                GATE._gate_factorial_topology("skew80-20", cells)

            for document in cells.values():
                for row in document["partitions"].values():
                    row["series_table"]["in_order_rotations"] = 2
            result = GATE._gate_factorial_topology("skew80-20", cells)
            self.assertEqual(result["in_order_rotations"], 32)

    def test_repartition_gate_checks_exact_layout_counts_and_source_identity(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            messages = 80
            common = {
                "schema": GATE.REPARTITION_SCHEMA,
                "input": "/capture",
                "partition_count": 16,
                "max_messages": messages,
                "topic": "metrics",
                "compression": "zstd",
                "messages": messages,
                "payload_bytes": messages * 10,
                "input_manifest_sha256": "1" * 64,
                "output_manifest_sha256": "2" * 64,
                "input_stream_sha256": "3" * 64,
                "output_stream_sha256": "4" * 64,
                "input_content_stream_sha256": "6" * 64,
                "output_content_stream_sha256": "6" * 64,
                "content_streams_equal": True,
                "output_tree_sha256": "5" * 64,
                "reopened_verification": True,
            }
            reports = []
            for layout in ("uniform", "skew80-20"):
                counts = [0] * 16
                ordinals: list[list[int]] = [[] for _ in range(16)]
                for ordinal in range(messages):
                    if layout == "uniform":
                        destination = ordinal % 16
                    elif ordinal % 5 != 4:
                        destination = 0
                    else:
                        destination = 1 + ((ordinal // 5) % 15)
                    counts[destination] += 1
                    ordinals[destination].append(ordinal)
                report = dict(common)
                report["layout"] = layout
                report["output"] = f"/output-{layout}"
                report["mapping_spec"] = (
                    GATE.UNIFORM_MAPPING_SPEC
                    if layout == "uniform"
                    else GATE.SKEW_MAPPING_SPEC
                )
                report["mapping_sha256"] = ("a" if layout == "uniform" else "b") * 64
                report["partitions"] = [
                    {
                        "partition": partition,
                        "message_count": count,
                        "payload_bytes": count * 10,
                        "first_global_ordinal": values[0],
                        "last_global_ordinal": values[-1],
                    }
                    for partition, (count, values) in enumerate(zip(counts, ordinals))
                ]
                path = root / f"{layout}.json"
                write_json(path, report)
                reports.append(path)
            result = GATE.gate_repartition(reports[0], reports[1], messages)
            self.assertTrue(result["verified"])

            repeated = json.loads(reports[0].read_text(encoding="utf-8"))
            repeated["output"] = "/output-repeat"
            repeat_path = root / "uniform-repeat.json"
            write_json(repeat_path, repeated)
            repeat_result = GATE.gate_repartition_repeat(reports[0], repeat_path)
            self.assertTrue(
                repeat_result["relative_names_lengths_and_file_bytes_deterministic"]
            )

            repeated["output_tree_sha256"] = "c" * 64
            write_json(repeat_path, repeated)
            with self.assertRaisesRegex(GATE.GateError, "output_tree_sha256"):
                GATE.gate_repartition_repeat(reports[0], repeat_path)

            broken = json.loads(reports[1].read_text(encoding="utf-8"))
            broken["partitions"][0]["message_count"] -= 1
            reports[1].write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "ordinal mapping"):
                GATE.gate_repartition(reports[0], reports[1], messages)

            write_json(reports[1], {**broken, "compression": "none"})
            with self.assertRaisesRegex(GATE.GateError, "Zstd"):
                GATE.gate_repartition(reports[0], reports[1], messages)

    def test_render_config_isolates_series_and_last_timestamp_factors(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            template = root / "template.toml"
            template.write_text(
                """
[kafka]

[ingestion]
replay_from = "/old"
stop_after_messages = 1

[ingestion.head_buffer]
adaptive_series_table = true

[ingestion.segment_writer]
segments_dir = "/old-segments"
""".lstrip(),
                encoding="utf-8",
            )
            output = root / "rendered.toml"
            result = GATE.render_config(
                template,
                output,
                Path("/capture"),
                Path("/segments"),
                4_000_000,
                True,
                False,
            )
            self.assertTrue(result["adaptive_series_table"])
            self.assertFalse(result["adaptive_last_timestamp_table"])
            with output.open("rb") as source:
                parsed = __import__("tomllib").load(source)
            head = parsed["ingestion"]["head_buffer"]
            self.assertTrue(head["adaptive_series_table"])
            self.assertFalse(head["adaptive_last_timestamp_table"])

    def test_storage_validation_compares_multisets_without_duplicate_winner_claims(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            uniform_path = root / "uniform.json"
            skew_path = root / "skew.json"
            expectations_path = root / "expectations.json"
            write_json(uniform_path, storage_report("a"))
            write_json(skew_path, storage_report("b", physical_scale=2))
            write_json(
                expectations_path,
                {
                    "schema": GATE.PHASE1_EXPECTATIONS_SCHEMA,
                    "storage_verifier": {"samples": 100},
                },
            )

            result = GATE.gate_storage_validation(
                uniform_path, skew_path, expectations_path
            )
            self.assertTrue(result["logical_sample_count_equal"])
            self.assertTrue(result["topology_independent_decoded_semantics_equal"])
            self.assertFalse(
                result["cross_topology_duplicate_winner_equivalence_claimed"]
            )
            self.assertFalse(
                result["ordered_decoded_semantic_identity_cross_topology_required"]
            )
            self.assertFalse(result["physical_layout_identity_required"])
            self.assertNotEqual(
                result["uniform"]["verified_selection_fingerprint"],
                result["skew80-20"]["verified_selection_fingerprint"],
            )
            # Model the duplicate-order limitation explicitly: the ordered-v2
            # fingerprints differ (as conflicting duplicate winners may), but
            # the topology-independent persisted-record multiset is equal.
            self.assertNotEqual(
                result["uniform"]["decoded_semantic_fingerprint"],
                result["skew80-20"]["decoded_semantic_fingerprint"],
            )

            broken = storage_report("b", samples=99, physical_scale=2)
            write_json(skew_path, broken)
            with self.assertRaisesRegex(GATE.GateError, "sample count mismatch"):
                GATE.gate_storage_validation(uniform_path, skew_path, expectations_path)

            broken = storage_report("b", physical_scale=2)
            broken["series_sample_per_segment"] = 1
            write_json(skew_path, broken)
            with self.assertRaisesRegex(GATE.GateError, "not an exhaustive"):
                GATE.gate_storage_validation(uniform_path, skew_path, expectations_path)

            write_json(skew_path, storage_report("b", semantic_marker="d"))
            with self.assertRaisesRegex(GATE.GateError, "topology-independent"):
                GATE.gate_storage_validation(uniform_path, skew_path, expectations_path)


    def test_parse_rejects_duplicate_partition_suffix_and_counter_drift(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            report = root / "report.md"
            markdown_report(report, True, True, skew=True)
            text = report.read_text(encoding="utf-8")
            report.write_text(
                text.replace("### Partition metrics:15", "### Partition other:15"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(GATE.GateError, "partitions 0..15"):
                GATE.parse_head_report(report)

            markdown_report(report, True, True, skew=True)
            text = report.read_text(encoding="utf-8")
            report.write_text(
                text.replace("| direct_series_ratio | 0.125000 |", "| direct_series_ratio | 0.5 |", 1),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(GATE.GateError, "ratio"):
                GATE.parse_head_report(report)

    def test_performance_gate_is_directional_but_never_promotes_unreplicated_cells(self) -> None:
        cases = [
            (0.95, 1.02, "directionally_better"),
            (1.06, 1.00, "directionally_worse"),
            (0.99, 1.02, "inconclusive"),
        ]
        for task_ratio, rss_ratio, expected_direction in cases:
            with self.subTest(expected=expected_direction), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                performance_fixture(
                    root, task_ratio, task_ratio, rss_ratio, rss_ratio
                )
                result = GATE.gate_performance(root)
                self.assertEqual(result["overall_disposition"], "defer")
                self.assertFalse(result["promotion_eligible"])
                self.assertEqual(result["production_default_conclusion"], "no_change")
                self.assertEqual(
                    set(result["factor_directions"].values()),
                    {expected_direction},
                )
                for topology in result["topologies"].values():
                    self.assertEqual(
                        {
                            factor["direction"]
                            for factor in topology["factors"].values()
                        },
                        {expected_direction},
                    )

    def test_performance_gate_attributes_the_two_factors_independently(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            performance_fixture(root, 0.95, 1.06, 1.0, 1.0)
            result = GATE.gate_performance(root)
            self.assertEqual(
                result["factor_directions"],
                {
                    "adaptive_series_table": "directionally_better",
                    "adaptive_last_timestamp_table": "directionally_worse",
                },
            )
            for topology in result["topologies"].values():
                self.assertEqual(
                    topology["factors"]["adaptive_series_table"]["direction"],
                    "directionally_better",
                )
                self.assertEqual(
                    topology["factors"]["adaptive_last_timestamp_table"][
                        "direction"
                    ],
                    "directionally_worse",
                )
                self.assertAlmostEqual(
                    topology["interaction"]["task_clock_ratio_of_ratios"], 1.0
                )


if __name__ == "__main__":
    unittest.main()
