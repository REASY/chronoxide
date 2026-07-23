#!/usr/bin/env python3

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import os
import py_compile
import re
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path


def load_exact_source_sibling(name: str, filename: str) -> types.ModuleType:
    path = Path(__file__).resolve(strict=True).parent / filename
    module = types.ModuleType(name)
    module.__file__ = str(path)
    module.__package__ = None
    module.__cached__ = None
    sys.modules[name] = module
    exec(
        compile(path.read_bytes(), str(path), "exec", dont_inherit=True),
        module.__dict__,
    )
    return module


gate = load_exact_source_sibling(
    "phase4_range_one_pass_gate", "phase4_range_one_pass_gate.py"
)
phase1_fixture = load_exact_source_sibling(
    "test_phase1_query_gate", "test_phase1_query_gate.py"
)
phase2_fixture = load_exact_source_sibling(
    "test_phase2_compact_ids_ab_gate", "test_phase2_compact_ids_ab_gate.py"
)


HERE = Path(__file__).resolve().parent
SOURCE_MANIFEST = HERE / "phase4_range_one_pass_queries.json"


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_tsv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=tuple(rows[0]),
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def seal_fixture_leaves(base: Path, manifest_name: str, names: tuple[str, ...]) -> None:
    for name in names:
        (base / name).chmod(0o444)
    manifest = base / manifest_name
    manifest.write_text(
        "".join(
            f"{gate.file_sha256(base / name)}  {name}\n" for name in names
        ),
        encoding="utf-8",
    )
    manifest.chmod(0o444)


def write_guardian_fixture(base: Path, prefix: str) -> None:
    for old in base.glob(f"{prefix}.guardian-*"):
        if old.is_file() and not old.is_symlink():
            old.chmod(0o644)
        old.unlink()
    control_path = base / f"{prefix}.guardian-control.json"
    ready = base / f"{prefix}.guardian-ready"
    launch = base / f"{prefix}.guardian-launch"
    samples = base / f"{prefix}.guardian-samples.tsv"
    conflicts = base / f"{prefix}.guardian-conflicts.tsv"
    summary = base / f"{prefix}.guardian-summary.json"
    immediate = base / f"{prefix}.guardian-immediate-conflicts.json"
    control = {
        "schema": gate.GUARDIAN_CONTROL_SCHEMA,
        "runner_pid": 10,
        "runner_starttime_ticks": 1000,
        "runner_ppid": 1,
        "root_pid": 11,
        "root_starttime_ticks": 1100,
        "root_ppid": 10,
        "guardian_pid": 12,
        "guardian_starttime_ticks": 1200,
        "guardian_ppid": 10,
        "interval_ms": gate.GUARDIAN_INTERVAL_MS,
        "ready_marker": str(ready),
        "launch_marker": str(launch),
    }
    write_json(control_path, control)
    samples.write_text(
        "\t".join(gate.GUARDIAN_SAMPLE_COLUMNS)
        + "\n"
        + "1\t50000000\t2026-07-22T00:00:00+00:00\ttrue\t1000\t1\ttrue\tS\t1100\t10\ttrue\t1200\t10\tfalse\t0\n"
        + "2\t150000000\t2026-07-22T00:00:00.100000+00:00\ttrue\t1000\t1\ttrue\tS\t1100\t10\ttrue\t1200\t10\ttrue\t0\n"
        + "3\t250000000\t2026-07-22T00:00:00.200000+00:00\ttrue\t1000\t1\tfalse\t-\t0\t-1\ttrue\t1200\t10\ttrue\t0\n",
        encoding="utf-8",
    )
    conflicts.write_text(
        "\t".join(gate.GUARDIAN_CONFLICT_COLUMNS) + "\n", encoding="utf-8"
    )
    write_json(
        immediate,
        {
            "schema": gate.CONFLICT_SCAN_SCHEMA,
            "conflicts": [],
            "quiet": True,
        },
    )
    termination = gate._empty_guardian_termination(1100)
    write_json(
        summary,
        {
            "schema": gate.GUARDIAN_SCHEMA,
            "runner_pid": 10,
            "runner_starttime_ticks": 1000,
            "runner_ppid": 1,
            "root_pid": 11,
            "root_starttime_ticks": 1100,
            "root_ppid": 10,
            "guardian_pid": 12,
            "guardian_starttime_ticks": 1200,
            "guardian_ppid": 10,
            "interval_ms": gate.GUARDIAN_INTERVAL_MS,
            "polls": 3,
            "terminal_elapsed_ns": 300000000,
            "poll_monotonic_elapsed_ns": [50000000, 150000000, 250000000],
            "maximum_poll_start_gap_ns": 100000000,
            "maximum_allowed_poll_start_gap_ns": 200000000,
            "control_path": str(control_path),
            "control_sha256": gate.file_sha256(control_path),
            "ready_marker": str(ready),
            "launch_marker": str(launch),
            "ready_created_poll": 1,
            "ready_created_monotonic_elapsed_ns": 50000000,
            "launch_observed_poll": 2,
            "launch_observed_monotonic_elapsed_ns": 150000000,
            "terminal_sample_poll": 3,
            "root_seen": True,
            "conflicts": [],
            "identity_violations": [],
            "handshake_violations": [],
            "termination": termination,
            "complete_and_conflict_free": True,
        },
    )
    (base / f"{prefix}.guardian-exit-status").write_bytes(b"0\n")
    (base / f"{prefix}.guardian.log").write_bytes(b"")
    ready.touch()
    launch.touch()
    for path in (control_path, ready, launch, samples, conflicts, summary, immediate):
        path.chmod(0o444)


def normalized_queries(root: Path) -> tuple[Path, list[dict[str, object]]]:
    normalized = root / "queries.normalized.json"
    gate.normalize_manifest(SOURCE_MANIFEST, root / "queries.tsv", normalized)
    return normalized, gate.read_manifest(normalized, SOURCE_MANIFEST)


def labels() -> dict[str, int]:
    value = {field: 0 for field in gate.phase2.LABEL_FIELDS}
    value.update(
        {
            "rows_integrity_checked": 2,
            "pairs_integrity_checked": 8,
            "rows_selectively_materialized": 2,
            "pairs_materialized": 4,
            "pairs_omitted": 4,
            "content_bytes_materialized": 32,
        }
    )
    return value


def scheduler(stats: dict[str, int]) -> dict[str, int]:
    spans = stats["chunk_reads"]
    physical_bytes = stats["bytes_read"]
    value = {field: 0 for field in gate.phase3.SCHEDULER_FIELDS}
    value.update(
        {
            "executions": 1,
            "pread_decisions": 1,
            "logical_requests": spans,
            "physical_spans": spans,
            "backend_submissions": spans,
            "submission_depth_sum": spans,
            "session_submission_depth_high_water": 1,
            "submission_depth_1": spans,
            "total_physical_bytes_executed": physical_bytes,
            "session_peak_in_flight_bytes_high_water": physical_bytes,
        }
    )
    return value


def range_execution(mode: str, query: dict[str, object]) -> dict[str, object]:
    one_pass = mode == gate.ONE_PASS_MODE
    return {
        "requested_mode": mode,
        "effective_mode": mode,
        "fallback_reason": None,
        "terminal_reason": None,
        "cache_bypassed": one_pass,
        "evaluation_count": query["expected_evaluation_count"],
        "union_start_ms": query["start_ms"] - query["window_ms"] if one_pass else None,
        "union_end_ms": query["end_ms"] if one_pass else None,
        "source_series": 2 if one_pass else 0,
        "source_samples": 20 if one_pass else 0,
        "estimated_retained_bytes_peak": 640 if one_pass else 0,
        "retained_bytes_after_finalize": 0,
        "preallocation_governed": False,
    }


def synthetic_raw(
    corpus: Path,
    query: dict[str, object],
    mode: str,
    *,
    duration_base: int = 10_000,
) -> dict[str, object]:
    one_pass = mode == gate.ONE_PASS_MODE
    work_multiplier = 1 if one_pass else int(query["expected_evaluation_count"])
    stats = {field: 0 for field in gate.common.QUERY_STATS_FIELDS}
    stats.update(
        {
            "segments_considered": work_multiplier,
            "segments_queried": work_multiplier,
            "matched_series": 2 * work_multiplier,
            "projected_series": 2 * work_multiplier,
            "chunk_reads": 2 * work_multiplier,
            "bytes_read": 16 * work_multiplier,
            "samples_decoded": 4 * work_multiplier,
            "index_postings_reads": work_multiplier,
            "index_postings_bytes_read": 8 * work_multiplier,
        }
    )
    fingerprint = hashlib.sha256(str(query["query_name"]).encode()).hexdigest()
    runs: list[dict[str, object]] = []
    for run_index in range(gate.BENCHMARK_REPEATS):
        duration = duration_base + run_index
        stages = {field: 0 for field in gate.common.QUERY_STAGE_FIELDS}
        stages["unclassified_ns"] = duration
        range_cache = phase1_fixture.range_cache(query)
        assert range_cache is not None
        if not one_pass:
            range_cache["unsupported_bypasses"] = stats["chunk_reads"]
            range_cache["logical_miss_or_bypass_bytes"] = stats["bytes_read"]
        runs.append(
            {
                "query": query["expression"],
                "run_kind": "cold" if run_index == 0 else "warm",
                "run_index": run_index,
                "duration_ns": duration,
                "post_query_fingerprint_ns": 1,
                "effective_start_ms": query["start_ms"],
                "effective_end_ms": query["end_ms"],
                "step_ms": query["step_ms"],
                "semantic_fingerprint_sha256": fingerprint,
                "portable_semantic_fingerprint_sha256": fingerprint,
                "result_series": 2,
                "result_samples": 2 * int(query["expected_evaluation_count"]),
                "stats": copy.deepcopy(stats),
                "payload_reads": {
                    "logical_used_bytes": stats["bytes_read"],
                    "physical_reads": stats["chunk_reads"],
                    "physical_bytes": stats["bytes_read"],
                },
                "symbol_reads": phase1_fixture.symbol_reads(),
                "label_materialization": labels(),
                "query_label_storage": phase2_fixture.compact_storage(),
                "query_stages": stages,
                "metadata_runtime": phase1_fixture.metadata_runtime(),
                "range_scalar_cache": range_cache,
                "chunk_read_scheduler": scheduler(stats),
                "range_execution": range_execution(mode, query),
            }
        )
    return {
        "schema": gate.RAW_SCHEMA,
        "corpus_fingerprint_sha256": "a" * 64,
        "corpus_fingerprint_duration_ns": 1,
        "configuration": {
            "segments_dir": str(corpus.resolve()),
            "start_ms": query["start_ms"],
            "end_ms": query["end_ms"],
            "mode": "query_range",
            "step_ms": query["step_ms"],
            "range_scalar_cache_max_bytes": 0,
            "chunk_read_mode": "pread",
            "chunk_read_queue_depth": gate.QUEUE_DEPTH,
            "chunk_payload_coalesce_max_gap_bytes": gate.COALESCE_GAP_BYTES,
            "experimental_cross_segment_chunk_reads": False,
            "label_materialization": "demand-driven",
            "query_label_storage": "compact-ids",
            "query_instrumentation": "off",
            "query_label_arena_max_bytes": gate.DEFAULT_ARENA_BYTES,
            "storage_layout": "schema8",
            "benchmark_repeats": gate.BENCHMARK_REPEATS,
            "queries": [query["expression"]],
            "prewarm_query_contexts": False,
            "prefetch_query_data": False,
            "exponential_histogram_bucket_boundaries": [],
            "requested_segment_footer_validation": False,
            "effective_segment_footer_validation": False,
            "range_execution_mode": mode,
        },
        "limits": {
            "max_matched_series": None,
            "max_projected_series": None,
            "max_chunk_reads": None,
            "max_bytes_read": None,
            "max_samples_decoded": None,
            "max_regex_values_examined": None,
        },
        "runs": runs,
    }


def inventory_document(corpus: Path) -> dict[str, object]:
    files = [
        {
            "path": "segment.bin",
            "size_bytes": 1,
            "sha256": hashlib.sha256(b"x").hexdigest(),
        }
    ]
    return {
        "schema": gate.common.INVENTORY_SCHEMA,
        "corpus": str(corpus.resolve()),
        "corpus_sha256": hashlib.sha256(gate.canonical_json(files).encode()).hexdigest(),
        "file_count": 1,
        "total_bytes": 1,
        "files": files,
    }


def fixture_corpus_contract(inventory: dict[str, object]) -> dict[str, object]:
    return {
        "phase1_segments_manifest_sha256": "b" * 64,
        "gate_inventory_sha256": inventory["corpus_sha256"],
        "query_corpus_fingerprint_sha256": "a" * 64,
        "file_count": inventory["file_count"],
        "total_bytes": inventory["total_bytes"],
        "dense_event_time_span_ms": gate.ACCEPTED_DENSE_EVENT_TIME_SPAN_MS,
    }


def compare_fixture(args: argparse.Namespace) -> None:
    gate.compare_results(args, args.expected_corpus)


def validate_fixture_result(args: argparse.Namespace, value: object) -> None:
    gate.validate_result_document(value, expected_corpus=args.expected_corpus)


def prepare_comparison(root: Path, *, seal_leaves: bool = False) -> argparse.Namespace:
    manifest, queries = normalized_queries(root)
    metadata = root / "metadata"
    inventory_dir = root / "inventory"
    validation = root / "validation"
    comparisons = root / "comparisons"
    harness = metadata / "harness"
    for directory in (metadata, inventory_dir, validation, comparisons, harness):
        directory.mkdir()
    (harness / "phase4_range_one_pass_queries.json").write_bytes(
        SOURCE_MANIFEST.read_bytes()
    )
    corpus = root / "corpus"
    corpus.mkdir()
    (corpus / "segment.bin").write_bytes(b"x")
    binary = metadata / "chronoxide-query"
    binary.write_bytes(b"same Phase 4 binary")
    binary_hash = gate.file_sha256(binary)
    runs_dir = root / "runs"
    runs_dir.mkdir()

    inventory = inventory_document(corpus)
    expected_corpus = fixture_corpus_contract(inventory)
    write_json(inventory_dir / "before.json", inventory)
    write_json(inventory_dir / "after.json", inventory)
    inventory_paths = (str(corpus / "segment.bin") + "\0").encode()
    (inventory_dir / "files.nul").write_bytes(inventory_paths)
    (inventory_dir / "files-after.nul").write_bytes(inventory_paths)
    footer_markdown = """- Storage Layout: schema8
- Requested Segment Footer Validation: true
- Effective Segment Footer Validation: true
"""
    readback_markdown = """- Storage Layout: schema8
| Expected Readback Queries | 1 |
| Executed Readback Queries | 1 |
| Skipped Readback Queries | 0 |
| Isolation Check Skips | 0 |
| Checked Queries | 1 |
| Mismatches | 0 |
| Multi-Step Range Readbacks Expected | 1 |
| Multi-Step Range Readbacks Executed | 1 |
| Multi-Step Range Readbacks Skipped | 0 |
"""
    (validation / "footer.md").write_text(footer_markdown, encoding="utf-8")
    (validation / "readbacks.md").write_text(readback_markdown, encoding="utf-8")
    write_json(
        validation / "footer.json",
        {
            "schema": gate.SMOKE_SCHEMA,
            "kind": "footer",
            "gate": "pass",
            "requested": True,
            "effective": True,
        },
    )
    write_json(
        validation / "readbacks.json",
        {
            "schema": gate.SMOKE_SCHEMA,
            "kind": "readback",
            "gate": "pass",
            "expected": 1,
            "executed": 1,
            "skipped": 0,
            "isolation_skips": 0,
            "checked": 1,
            "mismatches": 0,
            "multi_step_range_expected": 1,
            "multi_step_range_executed": 1,
            "multi_step_range_skipped": 0,
        },
    )

    index_rows: list[dict[str, object]] = []
    residency_rows: list[dict[str, object]] = []
    query_by_name = {query["query_name"]: query for query in queries}
    plan = gate.expected_plan(queries)
    write_tsv(root / "run-plan.tsv", plan)
    safe_snapshot = "1 0 0.0 init /sbin/init\n"
    for name in (
        "processes-before-footer.txt",
        "processes-immediate-before-footer.txt",
        "processes-before-readbacks.txt",
        "processes-immediate-before-readbacks.txt",
    ):
        (validation / name).write_text(safe_snapshot, encoding="utf-8")
    for name in ("footer.exit-status", "readbacks.exit-status"):
        (validation / name).write_bytes(b"0\n")
    for name in ("footer.time.txt", "footer.log", "readbacks.time.txt", "readbacks.log"):
        (validation / name).write_text("fixture\n", encoding="utf-8")
    write_guardian_fixture(validation, "footer")
    write_guardian_fixture(validation, "readbacks")
    if seal_leaves:
        seal_fixture_leaves(
            validation,
            "footer-leaves.sha256",
            tuple(
                sorted(
                    gate.guardian_success_leaf_names("footer")
                    | {
                "processes-before-footer.txt",
                "processes-immediate-before-footer.txt",
                "footer.time.txt",
                "footer.md",
                "footer.log",
                "footer.exit-status",
                    }
                )
            ),
        )
        seal_fixture_leaves(
            validation,
            "readbacks-leaves.sha256",
            tuple(
                sorted(
                    gate.guardian_success_leaf_names("readbacks")
                    | {
                "processes-before-readbacks.txt",
                "processes-immediate-before-readbacks.txt",
                "readbacks.time.txt",
                "readbacks.md",
                "readbacks.log",
                "readbacks.exit-status",
                    }
                )
            ),
        )
    for planned in plan:
        label = str(planned["process_label"])
        run_dir = runs_dir / label
        run_dir.mkdir()
        raw_path = run_dir / "raw.json"
        mode = str(planned["range_execution_mode"])
        duration = 5_000 if mode == gate.ONE_PASS_MODE else 10_000
        write_json(
            raw_path,
            synthetic_raw(
                corpus,
                query_by_name[str(planned["query_name"])],
                mode,
                duration_base=duration + int(planned["block"]),
            ),
        )
        argv = gate._expected_query_argv(binary, corpus, query_by_name[str(planned["query_name"])], mode, run_dir)
        (run_dir / "argv.nul").write_bytes(
            b"".join(argument.encode() + b"\0" for argument in argv)
        )
        (run_dir / "time.tsv").write_text(
            "process_wall_seconds\t1.0\n"
            "process_user_seconds\t0.5\n"
            "process_system_seconds\t0.1\n"
            f"max_rss_kib\t{100 if mode == gate.ONE_PASS_MODE else 110}\n"
            "exit_status\t0\n",
            encoding="utf-8",
        )
        (run_dir / "exit-status").write_bytes(b"0\n")
        for name in (
            "processes-before.txt",
            "processes-immediate-before.txt",
            "processes-after.txt",
        ):
            (run_dir / name).write_text(safe_snapshot, encoding="utf-8")
        for name in ("report.md", "query.log", "pressure-before.txt", "pressure-after.txt"):
            (run_dir / name).write_text("fixture\n", encoding="utf-8")
        (run_dir / "residency-after-evict.nul").write_bytes(
            f"0\0{inventory['total_bytes']}\0{corpus / 'segment.bin'}\0".encode()
        )
        (run_dir / "residency-after-run.nul").write_bytes(
            f"1\0{inventory['total_bytes']}\0{corpus / 'segment.bin'}\0".encode()
        )
        write_guardian_fixture(run_dir, "timed")
        if seal_leaves:
            seal_fixture_leaves(
                run_dir,
                "run-leaves.sha256",
                tuple(
                    sorted(
                        gate.guardian_success_leaf_names("timed")
                        | {
                    "argv.nul",
                    "processes-before.txt",
                    "processes-immediate-before.txt",
                    "pressure-before.txt",
                    "residency-after-evict.nul",
                    "raw.json",
                    "report.md",
                    "query.log",
                    "time.tsv",
                    "exit-status",
                    "pressure-after.txt",
                    "processes-after.txt",
                    "residency-after-run.nul",
                        }
                    )
                ),
            )
        index_rows.append(
            {
                **planned,
                "binary_sha256": binary_hash,
                "corpus": str(corpus.resolve()),
                "raw_output": str(raw_path.resolve()),
                "process_wall_seconds": "1.0",
                "process_user_seconds": "0.5",
                "process_system_seconds": "0.1",
                "max_rss_kib": 100 if mode == gate.ONE_PASS_MODE else 110,
            }
        )
        for phase, resident in (("after-evict", 0), ("after-run", 1)):
            residency_rows.append(
                {
                    "process_label": label,
                    "block": planned["block"],
                    "range_execution_mode": mode,
                    "phase": phase,
                    "file_count": 1,
                    "resident_bytes": resident,
                    "corpus_file_bytes": 1,
                }
            )
    write_tsv(root / "raw-index.tsv", index_rows)
    write_tsv(root / "residency-summary.tsv", residency_rows)
    run_note = metadata / "run-note.txt"
    run_note.write_text("quiet fixture host\n", encoding="utf-8")
    return argparse.Namespace(
        index=root / "raw-index.tsv",
        manifest=manifest,
        source_manifest=SOURCE_MANIFEST,
        inventory_before=inventory_dir / "before.json",
        inventory_after=inventory_dir / "after.json",
        residency=root / "residency-summary.tsv",
        footer_validation=validation / "footer.json",
        readback_validation=validation / "readbacks.json",
        summary=root / "summary.tsv",
        output=comparisons / "result-gate.json",
        binary=binary,
        corpus=corpus,
        runs_dir=runs_dir,
        max_resident_bytes_after_evict=0,
        quiet_host_confirmed=1,
        allow_noisy_host=0,
        run_note_file=run_note,
        expected_corpus=expected_corpus,
    )


class ManifestAndPlanTests(unittest.TestCase):
    def test_sealed_manifest_and_plan_are_exact_and_position_balanced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            normalized, queries = normalized_queries(root)
            self.assertEqual(len(queries), 4)
            plan = gate.expected_plan(queries)
            self.assertEqual(len(plan), 64)
            for query in queries:
                matching = [row for row in plan if row["query_name"] == query["query_name"]]
                for mode in gate.MODES:
                    self.assertEqual(
                        sum(row["range_execution_mode"] == mode for row in matching),
                        gate.PROCESSES_PER_ARM_PER_QUERY,
                    )
                    for position in range(1, 5):
                        self.assertEqual(
                            sum(
                                row["range_execution_mode"] == mode
                                and row["order_index"] == position
                                for row in matching
                            ),
                            2,
                        )
            self.assertEqual(gate.read_manifest(normalized, SOURCE_MANIFEST), queries)

    def test_sparse_ranges_cannot_be_reclassified_as_dense(self) -> None:
        document = json.loads(SOURCE_MANIFEST.read_text(encoding="utf-8"))
        document["queries"][2]["evidence_class"] = "dense-real-window"
        document["queries"][2]["dense_promotion_evidence"] = True
        with self.assertRaisesRegex(gate.GateError, "sealed matrix|corpus span"):
            gate._validate_source_manifest(document)

    def test_modified_manifest_bytes_are_not_accepted_by_cli_normalization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            modified = root / "manifest.json"
            document = json.loads(SOURCE_MANIFEST.read_text(encoding="utf-8"))
            document["description"] += " changed"
            write_json(modified, document)
            with self.assertRaisesRegex(gate.GateError, "bytes differ"):
                gate.normalize_manifest(modified, root / "q.tsv", root / "q.json")


class RawValidationTests(unittest.TestCase):
    def test_repeated_cache_contract_requires_the_observed_unsupported_path(self) -> None:
        report = phase1_fixture.range_cache(
            {
                "mode": "range",
                "range_scalar_cache_max_bytes": 0,
            }
        )
        assert report is not None
        report["unsupported_bypasses"] = 7
        report["logical_miss_or_bypass_bytes"] = 112
        gate.validate_range_cache_execution_contract(
            report, "repeated", 112, 7, "repeated-cache"
        )

        report["unsupported_bypasses"] = 0
        report["misses"] = 7
        report["streaming_budget_bypasses"] = 7
        with self.assertRaisesRegex(gate.GateError, "does not reconcile"):
            gate.validate_range_cache_execution_contract(
                report, "repeated", 112, 7, "repeated-cache"
            )

    def test_raw_v14_requires_unlimited_config_and_finalized_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, queries = normalized_queries(root)
            corpus = root / "corpus"
            corpus.mkdir()
            query = queries[0]
            raw_path = root / "raw.json"
            raw = synthetic_raw(corpus, query, gate.ONE_PASS_MODE)
            write_json(raw_path, raw)
            row = {
                "raw_output": str(raw_path),
                "range_execution_mode": gate.ONE_PASS_MODE,
            }
            expected_corpus = fixture_corpus_contract(inventory_document(corpus))
            fingerprint, runs = gate.validate_raw(
                row, query, corpus, expected_corpus
            )
            self.assertEqual(fingerprint, "a" * 64)
            self.assertEqual(len(runs), gate.BENCHMARK_REPEATS)
            self.assertEqual(runs[0]["range_execution"]["retained_bytes_after_finalize"], 0)

            bad_limits = copy.deepcopy(raw)
            bad_limits["limits"]["max_bytes_read"] = 100
            write_json(root / "bad-limits.json", bad_limits)
            row["raw_output"] = str(root / "bad-limits.json")
            with self.assertRaisesRegex(gate.GateError, "not unlimited"):
                gate.validate_raw(row, query, corpus, expected_corpus)

            leaked = copy.deepcopy(raw)
            leaked["runs"][0]["range_execution"]["retained_bytes_after_finalize"] = 1
            write_json(root / "leaked.json", leaked)
            row["raw_output"] = str(root / "leaked.json")
            with self.assertRaisesRegex(gate.GateError, "leaked"):
                gate.validate_raw(row, query, corpus, expected_corpus)

            terminated = copy.deepcopy(raw)
            terminated["runs"][0]["range_execution"]["terminal_reason"] = (
                "typed_source_observed_after_decode"
            )
            write_json(root / "terminated.json", terminated)
            row["raw_output"] = str(root / "terminated.json")
            with self.assertRaisesRegex(gate.GateError, "terminated"):
                gate.validate_raw(row, query, corpus, expected_corpus)

    def test_repeated_summary_cannot_claim_union_retention(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, queries = normalized_queries(root)
            summary = range_execution("repeated", queries[0])
            summary["source_samples"] = 1
            with self.assertRaisesRegex(gate.GateError, "union retention"):
                gate.validate_range_execution(summary, "repeated", queries[0], "summary")

    def test_successful_scalar_comparator_rejects_typed_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, queries = normalized_queries(root)
            corpus = root / "corpus"
            corpus.mkdir()
            query = queries[0]
            raw = synthetic_raw(corpus, query, gate.ONE_PASS_MODE)
            raw["runs"][0]["stats"]["typed_full_chunks_decoded"] = 1
            raw_path = root / "typed.json"
            write_json(raw_path, raw)
            contract = fixture_corpus_contract(inventory_document(corpus))
            with self.assertRaisesRegex(gate.GateError, "typed source chunks"):
                gate.validate_raw(
                    {
                        "raw_output": str(raw_path),
                        "range_execution_mode": gate.ONE_PASS_MODE,
                    },
                    query,
                    corpus,
                    contract,
                )

    def test_one_pass_cache_bypass_summary_rejects_cache_activity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, queries = normalized_queries(root)
            corpus = root / "corpus"
            corpus.mkdir()
            query = queries[0]
            raw = synthetic_raw(corpus, query, gate.ONE_PASS_MODE)
            raw["runs"][0]["range_scalar_cache"]["hits"] = 1
            raw_path = root / "cache.json"
            write_json(raw_path, raw)
            contract = fixture_corpus_contract(inventory_document(corpus))
            with self.assertRaisesRegex(gate.GateError, "hit, admission, or charge"):
                gate.validate_raw(
                    {
                        "raw_output": str(raw_path),
                        "range_execution_mode": gate.ONE_PASS_MODE,
                    },
                    query,
                    corpus,
                    contract,
                )


class EvidenceContractTests(unittest.TestCase):
    def test_runner_disables_python_bytecode_for_frozen_harness(self) -> None:
        runner = (HERE / "phase4_range_one_pass_run.sh").read_text(encoding="utf-8")
        export = "export PYTHONDONTWRITEBYTECODE=1"
        self.assertIn(export, runner)
        self.assertIn("export PYTHONNOUSERSITE=1", runner)
        self.assertIn('"$PYTHON_BIN" -I -S -B "$@"', runner)
        self.assertLess(runner.index(export), runner.index('SCRIPT_DIR='))

    def test_runner_keeps_the_preserved_binary_read_only_and_sealed(self) -> None:
        runner = (HERE / "phase4_range_one_pass_run.sh").read_text(encoding="utf-8")
        self.assertIn('chmod 0555 -- "$RUN_BIN"', runner)
        self.assertIn("assert_binary_seal()", runner)
        self.assertIn("assert_experiment_seals()", runner)
        self.assertIn("build --locked --release --target", runner)
        self.assertNotIn('QUERY_BIN=', runner)

    def test_formal_build_is_bound_to_the_query_cli_package(self) -> None:
        runner = (HERE / "phase4_range_one_pass_run.sh").read_text(encoding="utf-8")
        self.assertIn("-p chronoxide-query-cli --bin chronoxide-query", runner)

        gate_source = (HERE / "phase4_range_one_pass_gate.py").read_text(
            encoding="utf-8"
        )
        start = gate_source.index("def validate_build_provenance")
        end = gate_source.index("\ndef verify_seal", start)
        build_contract = gate_source[start:end]
        self.assertIn('"chronoxide-query-cli"', build_contract)
        self.assertNotIn('"chronoxide-ingester"', build_contract)

    def test_runner_names_known_forbidden_build_profiler_and_database_tools(self) -> None:
        classifier = (HERE / "phase4_range_one_pass_gate.py").read_text(encoding="utf-8")
        for command in (
            "cargo",
            "rustc",
            "make",
            "ninja",
            "docker",
            "podman",
            "buildah",
            "qemu-system",
            "qemu-kvm",
            "emulator",
            "adb",
            "GradleDaemon",
            "gcc",
            "clang",
            "perf",
            "heaptrack",
            "valgrind",
            "strace",
            "bpftrace",
            "hotspot",
            "chronoxide-",
            "greptime",
            "prometheus",
            "soong_ui",
            "soong_build",
            "ckati",
            "redroid",
            "artracer",
            "btop",
            "htop",
            "top",
        ):
            self.assertIn(command, classifier)

    def test_process_gate_rejects_busy_android_vm_adb_and_soong_but_not_idle_qemu(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "processes.txt"
            snapshot.write_text(
                "101 1 0.0 qemu-system-aarch64 /usr/bin/qemu-system-aarch64 -name idle\n",
                encoding="utf-8",
            )
            gate.validate_process_snapshot(snapshot, set())
            for line, expected in (
                (
                    "101 1 240.0 qemu-system-aarch64 /usr/bin/qemu-system-aarch64 /data/artracer-qemu-arm64 redroid\n",
                    "qemu-system-aarch64",
                ),
                ("102 1 0.0 adb adb -L tcp:localhost:5037 fork-server server\n", "adb"),
                ("103 1 0.0 soong_ui /src/build/soong/soong_ui --make-mode\n", "soong_ui"),
            ):
                snapshot.write_text(line, encoding="utf-8")
                with self.assertRaisesRegex(gate.GateError, expected):
                    gate.validate_process_snapshot(snapshot, set())

            for line, expected in (
                (
                    "104 1 90.0 bash /bin/bash "
                    "/src/build/soong/soong_ui.bash --make-mode\n",
                    "bash",
                ),
                (
                    "105 1 90.0 cargo-nextest "
                    "/home/u/.cargo/bin/cargo-nextest nextest run\n",
                    "cargo-nextest",
                ),
                (
                    "106 1 90.0 ninja.real "
                    "/src/prebuilts/build-tools/bin/ninja.real -C out\n",
                    "ninja.real",
                ),
                ("107 1 90.0 ld.bfd /usr/bin/ld.bfd -o out\n", "ld.bfd"),
                ("108 1 90.0 ld.gold /usr/bin/ld.gold -o out\n", "ld.gold"),
                (
                    "109 1 90.0 clang-19.real /opt/llvm/bin/clang-19.real -c x.cc\n",
                    "clang-19.real",
                ),
                ("110 1 10.0 btop btop\n", "btop"),
            ):
                snapshot.write_text(line, encoding="utf-8")
                with self.assertRaisesRegex(gate.GateError, re.escape(expected)):
                    gate.validate_process_snapshot(snapshot, set())

            snapshot.write_text(
                "111 1 0.0 topology-helper /usr/bin/topology-helper --idle\n",
                encoding="utf-8",
            )
            gate.validate_process_snapshot(snapshot, set())

    def test_process_guardian_reconstructs_held_launch_terminal_and_cadence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_guardian_fixture(root, "timed")
            gate.validate_guardian_prefix(root, "timed")

            samples = root / "timed.guardian-samples.tsv"
            samples.chmod(0o644)
            samples.write_bytes(
                samples.read_bytes().replace(
                    b"3\t250000000\t", b"3\t250000000\t"
                ).replace(
                    b"\tfalse\t-\t0\t-1\ttrue\t1200\t10\ttrue\t0\n",
                    b"\ttrue\tS\t1100\t10\ttrue\t1200\t10\ttrue\t0\n",
                )
            )
            samples.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "root-absent terminal"):
                gate.validate_guardian_prefix(root, "timed")

    def test_process_guardian_rejects_first_and_terminal_edge_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_guardian_fixture(root, "timed")
            samples = root / "timed.guardian-samples.tsv"
            samples.chmod(0o644)
            samples.write_bytes(
                samples.read_bytes().replace(b"1\t50000000\t", b"1\t250000000\t")
            )
            samples.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "cadence"):
                gate.validate_guardian_prefix(root, "timed")

            write_guardian_fixture(root, "timed")
            summary_path = root / "timed.guardian-summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["terminal_elapsed_ns"] = 500000001
            summary_path.chmod(0o644)
            write_json(summary_path, summary)
            summary_path.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "detached"):
                gate.validate_guardian_prefix(root, "timed")

    def test_process_guardian_rejects_marker_control_and_launch_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_guardian_fixture(root, "timed")
            ready = root / "timed.guardian-ready"
            ready.chmod(0o644)
            with self.assertRaisesRegex(gate.GateError, "0444"):
                gate.validate_guardian_prefix(root, "timed")

            write_guardian_fixture(root, "timed")
            launch = root / "timed.guardian-launch"
            launch.chmod(0o644)
            launch.write_bytes(b"released\n")
            launch.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "empty mode 0444"):
                gate.validate_guardian_prefix(root, "timed")

            write_guardian_fixture(root, "timed")
            ready = root / "timed.guardian-ready"
            ready.chmod(0o644)
            ready.unlink()
            ready.symlink_to(root / "timed.guardian-launch")
            with self.assertRaisesRegex(gate.GateError, "regular file"):
                gate.validate_guardian_prefix(root, "timed")

            write_guardian_fixture(root, "timed")
            control_path = root / "timed.guardian-control.json"
            control = json.loads(control_path.read_text(encoding="utf-8"))
            control["ready_marker"] = str(root / "wrong-ready")
            control_path.chmod(0o644)
            write_json(control_path, control)
            control_path.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "exact handshake"):
                gate.validate_guardian_prefix(root, "timed")

            write_guardian_fixture(root, "timed")
            control_path = root / "timed.guardian-control.json"
            control = json.loads(control_path.read_text(encoding="utf-8"))
            control["root_ppid"] = 99
            control_path.chmod(0o644)
            write_json(control_path, control)
            control_path.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "exact handshake"):
                gate.validate_guardian_prefix(root, "timed")

            write_guardian_fixture(root, "timed")
            samples = root / "timed.guardian-samples.tsv"
            samples.chmod(0o644)
            samples.write_bytes(
                samples.read_bytes().replace(
                    b"\tfalse\t-\t0\t-1\ttrue\t1200\t10\ttrue\t0\n",
                    b"\tfalse\tZ\t1100\t99\ttrue\t1200\t10\ttrue\t0\n",
                )
            )
            samples.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "changed identity"):
                gate.validate_guardian_prefix(root, "timed")

            write_guardian_fixture(root, "timed")
            samples = root / "timed.guardian-samples.tsv"
            samples.chmod(0o644)
            samples.write_bytes(samples.read_bytes().replace(b"\tfalse\t0\n", b"\ttrue\t0\n", 1))
            samples.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "strictly after readiness"):
                gate.validate_guardian_prefix(root, "timed")

    def test_process_guardian_rejects_transient_conflict_and_nonquiet_scan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_guardian_fixture(root, "timed")
            conflicts = root / "timed.guardian-conflicts.tsv"
            conflicts.chmod(0o644)
            with conflicts.open("a", encoding="utf-8") as destination:
                destination.write(
                    "2\t150000000\t2026-07-22T00:00:00.100000+00:00\t99\t1\tS\t9000\t1\tbtop\tbtop\n"
                )
            conflicts.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "transient conflict"):
                gate.validate_guardian_prefix(root, "timed")

            write_guardian_fixture(root, "timed")
            immediate = root / "timed.guardian-immediate-conflicts.json"
            immediate.chmod(0o644)
            write_json(
                immediate,
                {
                    "schema": gate.CONFLICT_SCAN_SCHEMA,
                    "quiet": False,
                    "conflicts": [{"name": "clang"}],
                },
            )
            immediate.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "not exactly quiet"):
                gate.validate_guardian_prefix(root, "timed")

    def test_runner_uses_one_held_identity_bound_lifecycle_for_every_workload(self) -> None:
        runner = (HERE / "phase4_range_one_pass_run.sh").read_text(encoding="utf-8")
        self.assertEqual(runner.count("run_held_workload "), 3)
        for required in (
            "trap 'cleanup_on_exit' EXIT",
            "trap 'cleanup_signal_exit' HUP INT TERM",
            "defer_cleanup_signals",
            "create-control --output",
            "wait-ready --control",
            "release-launch --control",
            '"$(stat -c \'%a\' -- "$launch")" == 444',
            "guardian-samples.tsv",
            "guardian-conflicts.tsv",
            "DISK_SPACE_CONFIRMED",
            "disk_capacity_admission=none-read-only-small-evidence-output",
        ):
            self.assertIn(required, runner)
        self.assertLess(
            runner.index("wait-ready --control"),
            runner.index("release-launch --control"),
        )
        self.assertIn("python3_background() {", runner)
        self.assertIn('exec "$PYTHON_BIN" -I -S -B "$@"', runner)
        self.assertIn(
            'python3_background "$FROZEN_GUARD_TOOL" monitor',
            runner,
        )
        self.assertIn("verify_background_python_pid_binding() {", runner)
        self.assertIn("verify_background_python_pid_binding\n", runner)
        self.assertNotIn('python3 "$FROZEN_GUARD_TOOL" monitor', runner)
        self.assertNotIn('read -r state starttime_ticks <<<"$(read_', runner)

    def test_final_inventory_is_fail_closed_for_directories_symlinks_and_fifos(self) -> None:
        def make_root(parent: Path, name: str) -> Path:
            root = parent / name
            root.mkdir()
            for required in gate.FINAL_REQUIRED_DIRECTORIES:
                (root / required).mkdir()
            (root / "COMPLETE").touch()
            return root

        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            clean = make_root(parent, "clean")
            files, _directories = gate.final_artifact_inventory(clean)
            self.assertEqual(files, ["COMPLETE"])
            root_link = parent / "clean-link"
            root_link.symlink_to(clean, target_is_directory=True)
            with self.assertRaisesRegex(gate.GateError, "root must not be a symlink"):
                gate.final_artifact_inventory(root_link)

            unsupported = make_root(parent, "unsupported")
            (unsupported / "surprise").mkdir()
            with self.assertRaisesRegex(gate.GateError, "unsupported root directory"):
                gate.final_artifact_inventory(unsupported)

            linked = make_root(parent, "linked")
            (linked / "metadata" / "link").symlink_to(linked / "COMPLETE")
            with self.assertRaisesRegex(gate.GateError, "symlink"):
                gate.final_artifact_inventory(linked)

            fifo = make_root(parent, "fifo")
            os.mkfifo(fifo / "metadata" / "pipe")
            with self.assertRaisesRegex(gate.GateError, "non-regular"):
                gate.final_artifact_inventory(fifo)

    def test_final_matrix_rejects_unexpected_nested_evidence_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            normalized_queries(root)
            harness = root / "metadata" / "harness"
            source = root / "metadata" / "source"
            harness.mkdir(parents=True)
            source.mkdir()
            (harness / "phase4_range_one_pass_queries.json").write_bytes(
                SOURCE_MANIFEST.read_bytes()
            )
            write_json(
                source / "source-snapshot-seal.json",
                {
                    "schema": gate.SOURCE_SNAPSHOT_SEAL_SCHEMA,
                    "repo": "/repo",
                    "snapshot": str(root / "build-source"),
                    "git_head": "1" * 40,
                    "git_tree": "2" * 40,
                    "source_seal_identity_sha256": "3" * 64,
                    "object_format": "sha1",
                    "file_count": 1,
                    "files": [
                        {
                            "path": "Cargo.lock",
                            "mode": "100644",
                            "object_id": "4" * 40,
                            "size_bytes": 1,
                        }
                    ],
                    "identity_sha256": "5" * 64,
                },
            )
            expected_files = (
                gate.expected_sealed_artifacts(root)
                - gate.FINAL_INVENTORY_AUTHORITY_FILES
            ) | {"build-source/Cargo.lock"}
            expected_directories = gate._artifact_parent_directories(expected_files)
            expected_directories.update(
                {
                    "build-source",
                    "metadata/build/home",
                    "metadata/build/cargo-home",
                }
            )
            files = sorted(expected_files, key=os.fsencode)
            directories = sorted(expected_directories, key=os.fsencode)
            gate.validate_final_artifact_matrix(root, files, directories)

            with self.assertRaisesRegex(gate.GateError, "unexpected evidence file"):
                gate.validate_final_artifact_matrix(
                    root,
                    sorted(
                        [*files, "metadata/unexpected-evidence.txt"], key=os.fsencode
                    ),
                    directories,
                )
            with self.assertRaisesRegex(gate.GateError, "unexpected evidence directory"):
                gate.validate_final_artifact_matrix(
                    root,
                    files,
                    sorted([*directories, "runs/unexpected-dir"], key=os.fsencode),
                )

            gate.validate_final_artifact_matrix(
                root,
                sorted([*files, "build-target/release/build.log"], key=os.fsencode),
                sorted(
                    [*directories, "build-target", "build-target/release"],
                    key=os.fsencode,
                ),
            )

    def test_exact_final_matrix_contains_all_sixty_six_lifecycles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _manifest, queries = normalized_queries(root)
            harness = root / "metadata" / "harness"
            harness.mkdir(parents=True)
            (harness / "phase4_range_one_pass_queries.json").write_bytes(
                SOURCE_MANIFEST.read_bytes()
            )
            expected = gate.expected_sealed_artifacts(root)
            plans = gate.expected_plan(queries)
            self.assertEqual(len(plans), 64)
            for process in plans:
                label = str(process["process_label"])
                for leaf in gate.guardian_success_leaf_names("timed"):
                    self.assertIn(f"runs/{label}/{leaf}", expected)
            for prefix in ("footer", "readbacks"):
                for leaf in gate.guardian_success_leaf_names(prefix):
                    self.assertIn(f"validation/{leaf}", expected)
            self.assertFalse(
                any(
                    "violation" in path
                    or "interrupted" in path
                    or "guardian-stop" in path
                    for path in expected
                )
            )

    def test_gate_loads_dependency_source_bytes_and_ignores_valid_malicious_pyc(self) -> None:
        dependencies = (
            "phase4_range_one_pass_gate.py",
            "phase1_query_gate.py",
            "phase2_compact_ids_ab_gate.py",
            "phase3_payload_coalescing_gate.py",
            "schema7_query_ab_gate.py",
            "schema8_query_ab_gate.py",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for filename in dependencies:
                (root / filename).write_bytes((HERE / filename).read_bytes())
            target = root / "schema7_query_ab_gate.py"
            original = target.read_bytes()
            target.write_text(
                "from pathlib import Path\nPath(__file__).with_name('PYC_EXECUTED').touch()\n",
                encoding="utf-8",
            )
            py_compile.compile(
                str(target),
                doraise=True,
                invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH,
            )
            target.write_bytes(original)
            completed = subprocess.run(
                [
                    "/usr/bin/python3",
                    "-I",
                    "-S",
                    "-B",
                    str(root / "phase4_range_one_pass_gate.py"),
                    "--help",
                ],
                check=False,
                capture_output=True,
                text=True,
                env={
                    "LC_ALL": "C",
                    "PYTHONDONTWRITEBYTECODE": "1",
                    "PYTHONNOUSERSITE": "1",
                },
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertFalse((root / "PYC_EXECUTED").exists())

    def test_source_snapshot_is_bound_to_one_clean_git_oid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory) / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.email", "fixture@example.com"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.name", "Fixture"],
                check=True,
            )
            (repo / "Cargo.lock").write_text("# fixture\n", encoding="utf-8")
            (repo / "src").mkdir()
            (repo / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "commit", "-qm", "fixture"], check=True
            )
            seal = gate.source_seal(repo)
            seal_path = Path(directory) / "source.json"
            write_json(seal_path, seal)
            archive = Path(directory) / "source.tar"
            subprocess.run(
                ["git", "-C", str(repo), "archive", "-o", str(archive), seal["head"]],
                check=True,
            )
            snapshot = Path(directory) / "snapshot"
            snapshot.mkdir()
            subprocess.run(["tar", "-xf", str(archive), "-C", str(snapshot)], check=True)
            for path in sorted(snapshot.rglob("*"), reverse=True):
                if path.is_dir():
                    path.chmod(0o555)
                else:
                    path.chmod(0o555 if os.access(path, os.X_OK) else 0o444)
            snapshot.chmod(0o555)
            snapshot_seal = gate.source_snapshot_seal(repo, snapshot, seal_path)
            self.assertEqual(snapshot_seal["git_head"], seal["head"])
            (repo / "src" / "main.rs").write_text("fn main(){panic!()}\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "clean tracked worktree"):
                gate.check_source_seal(repo, seal_path)

    def test_fixture_inventory_is_rejected_by_production_corpus_pin(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = root / "corpus"
            corpus.mkdir()
            path = root / "inventory.json"
            write_json(path, inventory_document(corpus))
            with self.assertRaisesRegex(gate.GateError, "audited Phase 1 corpus"):
                gate.load_inventory(path, corpus)

    def test_inventory_rejects_control_characters_in_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = root / "corpus"
            corpus.mkdir()
            document = inventory_document(corpus)
            document["files"][0]["path"] = "bad\tname"
            path = root / "inventory.json"
            write_json(path, document)
            with self.assertRaisesRegex(gate.GateError, "invalid relative path"):
                gate.load_inventory(
                    path, corpus, fixture_corpus_contract(inventory_document(corpus))
                )

    def test_readback_gate_requires_executed_multi_step_oracle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "readback.md"
            report.write_text(
                "- Storage Layout: schema8\n"
                "| Expected Readback Queries | 1 |\n"
                "| Executed Readback Queries | 1 |\n"
                "| Skipped Readback Queries | 0 |\n"
                "| Isolation Check Skips | 0 |\n"
                "| Checked Queries | 1 |\n"
                "| Mismatches | 0 |\n"
                "| Multi-Step Range Readbacks Expected | 1 |\n"
                "| Multi-Step Range Readbacks Executed | 0 |\n"
                "| Multi-Step Range Readbacks Skipped | 1 |\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "multi-step range oracle"):
                gate.validate_smoke_report("readback", report, root / "out.json")


class ComparisonTests(unittest.TestCase):
    def test_comparison_classifies_stats_and_forbids_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root)
            compare_fixture(args)
            result = json.loads(args.output.read_text(encoding="utf-8"))
            validate_fixture_result(args, result)
            self.assertEqual(result["production_promotion_verdict"], "forbidden")
            self.assertFalse(result["ordinary_query_stats_equivalent"])
            self.assertEqual(result["dense_24h_evidence_gate"], "missing")
            self.assertEqual(
                result["sparse_scheduler_control_query_names"],
                ["scalar_rate_sum_range_6h", "scalar_rate_sum_range_24h"],
            )
            classifications = {
                item["classification"] for item in result["query_stats_classification"]
            }
            self.assertIn("union-work-vs-repeated-logical-work", classifications)
            self.assertEqual(len(result["measurements"]), 4)

    def test_fingerprint_mismatch_fails_equivalence_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root)
            with args.index.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
            candidate = next(
                row for row in rows if row["range_execution_mode"] == gate.ONE_PASS_MODE
            )
            path = Path(candidate["raw_output"])
            raw = json.loads(path.read_text(encoding="utf-8"))
            raw["runs"][0]["semantic_fingerprint_sha256"] = "f" * 64
            write_json(path, raw)
            with self.assertRaisesRegex(gate.GateError, "fingerprints|shape/order"):
                compare_fixture(args)

    def test_result_validator_rejects_promotion_claim(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root)
            compare_fixture(args)
            result = json.loads(args.output.read_text(encoding="utf-8"))
            result["production_promotion_verdict"] = "promote"
            with self.assertRaisesRegex(gate.GateError, "forbid promotion"):
                validate_fixture_result(args, result)

    def test_formal_residency_bound_cannot_be_relaxed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root)
            args.max_resident_bytes_after_evict = 1
            with self.assertRaisesRegex(gate.GateError, "exactly zero"):
                compare_fixture(args)

    def test_noisy_host_override_is_forbidden(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root)
            args.allow_noisy_host = 1
            with self.assertRaisesRegex(gate.GateError, "noisy-host override"):
                compare_fixture(args)

    def test_cold_to_warm_storage_work_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root)
            with args.index.open(newline="", encoding="utf-8") as source:
                row = next(csv.DictReader(source, delimiter="\t"))
            raw_path = Path(row["raw_output"])
            raw = json.loads(raw_path.read_text(encoding="utf-8"))
            raw["runs"][1]["stats"]["bytes_read"] += 1
            raw["runs"][1]["payload_reads"]["logical_used_bytes"] += 1
            raw["runs"][1]["payload_reads"]["physical_bytes"] += 1
            raw["runs"][1]["chunk_read_scheduler"][
                "total_physical_bytes_executed"
            ] += 1
            raw["runs"][1]["range_scalar_cache"][
                "logical_miss_or_bypass_bytes"
            ] += 1
            write_json(raw_path, raw)
            with self.assertRaisesRegex(gate.GateError, "cold-to-warm"):
                compare_fixture(args)

    def test_reported_accounting_and_numeric_types_are_revalidated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root)
            compare_fixture(args)
            result = json.loads(args.output.read_text(encoding="utf-8"))
            result["measurements"][0]["arms"][gate.ONE_PASS_MODE][
                "cold_median_ns"
            ] = str(
                result["measurements"][0]["arms"][gate.ONE_PASS_MODE][
                    "cold_median_ns"
                ]
            )
            with self.assertRaisesRegex(gate.GateError, "finite non-negative"):
                validate_fixture_result(args, result)

            result = json.loads(args.output.read_text(encoding="utf-8"))
            result["non_query_stats_accounting_classification"][0][
                "repeated_sha256"
            ] = "0" * 64
            with self.assertRaisesRegex(gate.GateError, "detached from accounting"):
                validate_fixture_result(args, result)

            result = json.loads(args.output.read_text(encoding="utf-8"))
            result["measurements"][2]["dense_promotion_evidence"] = True
            result["measurements"][2]["evidence_class"] = "dense-real-window"
            with self.assertRaisesRegex(gate.GateError, "relabels"):
                validate_fixture_result(args, result)

    def test_leaf_recomputation_detects_detached_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root, seal_leaves=True)
            self.assertFalse((root / "metadata" / "query-manifest.input.json").exists())
            required = gate.expected_sealed_artifacts(root)
            self.assertIn(
                "metadata/harness/phase4_range_one_pass_queries.json", required
            )
            self.assertNotIn("metadata/query-manifest.input.json", required)
            compare_fixture(args)
            gate.verify_leaf_evidence(root, args.expected_corpus)
            args.summary.write_text("tampered\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "summary TSV is detached"):
                gate.verify_leaf_evidence(root, args.expected_corpus)

    def test_leaf_recomputation_rejects_detached_time_and_argv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root, seal_leaves=True)
            compare_fixture(args)
            first_run = next((root / "runs").iterdir())
            time_path = first_run / "time.tsv"
            original_time = time_path.read_bytes()
            time_path.chmod(0o644)
            time_path.write_bytes(
                original_time.replace(b"process_wall_seconds\t1.0", b"process_wall_seconds\t2.0")
            )
            time_path.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "does not match frozen file time.tsv"):
                gate.verify_leaf_evidence(root, args.expected_corpus)
            time_path.chmod(0o644)
            time_path.write_bytes(original_time)
            time_path.chmod(0o444)
            argv = first_run / "argv.nul"
            argv.chmod(0o644)
            argv.write_bytes(argv.read_bytes().replace(b"schema8", b"schema7", 1))
            argv.chmod(0o444)
            with self.assertRaisesRegex(gate.GateError, "does not match frozen file argv.nul"):
                gate.verify_leaf_evidence(root, args.expected_corpus)

    def test_leaf_recomputation_rejects_detached_raw_index(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = prepare_comparison(root, seal_leaves=True)
            compare_fixture(args)
            args.index.write_bytes(
                args.index.read_bytes().replace(b"\t1.0\t0.5\t", b"\t2.0\t0.5\t", 1)
            )
            with self.assertRaisesRegex(gate.GateError, "raw index is detached"):
                gate.verify_leaf_evidence(root, args.expected_corpus)


if __name__ == "__main__":
    unittest.main()
