#!/usr/bin/env python3

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import phase3_payload_coalescing_gate as gate
import test_phase1_query_gate as phase1_fixture


HERE = Path(__file__).resolve().parent
SOURCE_MANIFEST = HERE / "phase2_compact_ids_queries.json"
LIMITS = {
    "max_matched_series": 100,
    "max_projected_series": 200,
    "max_chunk_reads": 300,
    "max_bytes_read": 400,
    "max_samples_decoded": 500,
    "max_regex_values_examined": 600,
}


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_tsv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=tuple(rows[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source, delimiter="\t"))


def normalized_queries(root: Path) -> tuple[Path, list[dict[str, object]]]:
    normalized_tsv = root / "queries.tsv"
    normalized_json = root / "queries.json"
    gate.normalize_manifest(SOURCE_MANIFEST, normalized_tsv, normalized_json)
    return normalized_json, gate.read_manifest(normalized_json, SOURCE_MANIFEST)


def compact_no_result_storage() -> dict[str, int]:
    storage = {field: 0 for field in gate.LABEL_STORAGE_FIELDS}
    storage["compact_arena_budget_bytes"] = gate.DEFAULT_ARENA_BYTES
    return storage


def no_result_raw(
    corpus: Path,
    query: dict[str, object],
    *,
    backend: str = "pread",
    queue_depth: int = 128,
    gap: int = 0,
    repeats: int = gate.BENCHMARK_REPEATS,
) -> dict[str, object]:
    stats = {field: 0 for field in gate.common.QUERY_STATS_FIELDS}
    stats.update({"segments_considered": 1, "segments_skipped_by_missing_equality": 1})
    labels = {field: 0 for field in gate.LABEL_FIELDS}
    scheduler = {field: 0 for field in gate.SCHEDULER_FIELDS}
    fingerprint = hashlib.sha256(str(query["query_name"]).encode()).hexdigest()
    runs = []
    for run_index in range(repeats):
        duration_ns = 100 + run_index
        stages = {field: 0 for field in gate.common.QUERY_STAGE_FIELDS}
        stages["unclassified_ns"] = duration_ns
        runs.append(
            {
                "query": query["expression"],
                "run_kind": "cold" if run_index == 0 else "warm",
                "run_index": run_index,
                "duration_ns": duration_ns,
                "post_query_fingerprint_ns": 1,
                "effective_start_ms": query["start_ms"],
                "effective_end_ms": query["end_ms"],
                "step_ms": query["step_ms"],
                "semantic_fingerprint_sha256": fingerprint,
                "portable_semantic_fingerprint_sha256": fingerprint,
                "result_series": 0,
                "result_samples": 0,
                "stats": copy.deepcopy(stats),
                "payload_reads": {
                    "logical_used_bytes": 0,
                    "physical_reads": 0,
                    "physical_bytes": 0,
                },
                "symbol_reads": phase1_fixture.symbol_reads(),
                "label_materialization": copy.deepcopy(labels),
                "query_label_storage": compact_no_result_storage(),
                "query_stages": stages,
                "metadata_runtime": phase1_fixture.metadata_runtime(),
                "range_scalar_cache": None,
                "chunk_read_scheduler": copy.deepcopy(scheduler),
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
            "mode": "instant",
            "step_ms": None,
            "range_scalar_cache_max_bytes": None,
            "chunk_read_mode": gate.raw_backend_name(backend),
            "chunk_read_queue_depth": queue_depth,
            "chunk_payload_coalesce_max_gap_bytes": gap,
            "experimental_cross_segment_chunk_reads": False,
            "label_materialization": "demand-driven",
            "query_label_storage": "compact-ids",
            "query_instrumentation": "off",
            "query_label_arena_max_bytes": gate.DEFAULT_ARENA_BYTES,
            "storage_layout": "schema8",
            "benchmark_repeats": repeats,
            "queries": [query["expression"]],
            "prewarm_query_contexts": False,
            "prefetch_query_data": False,
            "exponential_histogram_bucket_boundaries": query["boundaries"],
            "requested_segment_footer_validation": False,
            "effective_segment_footer_validation": False,
        },
        "limits": copy.deepcopy(LIMITS),
        "runs": runs,
    }


def raw_args(corpus: Path, backend: str = "pread", queue_depth: int = 128) -> argparse.Namespace:
    return argparse.Namespace(
        corpus=corpus,
        backend=backend,
        queue_depth=queue_depth,
        arena_bytes=gate.DEFAULT_ARENA_BYTES,
        **LIMITS,
    )


def inventory_document(corpus: Path) -> dict[str, object]:
    files = [
        {
            "path": "segment.bin",
            "size_bytes": 1,
            "sha256": hashlib.sha256(b"x").hexdigest(),
        }
    ]
    canonical = json.dumps(files, separators=(",", ":"), sort_keys=True).encode()
    return {
        "schema": gate.common.INVENTORY_SCHEMA,
        "corpus": str(corpus.resolve()),
        "corpus_sha256": hashlib.sha256(canonical).hexdigest(),
        "file_count": 1,
        "total_bytes": 1,
        "files": files,
    }


def validated_runs(
    row: dict[str, str], query: dict[str, object], args: argparse.Namespace
) -> tuple[str, list[dict[str, object]]]:
    empty = query["category"] == "empty-result-control"
    gap = int(row["payload_coalesce_max_gap_bytes"])
    spans_by_gap = {0: 4, 256: 3, 1024: 2, 4096: 1}
    bytes_by_gap = {0: 40, 256: 50, 1024: 60, 4096: 70}
    stats = {field: 0 for field in gate.common.QUERY_STATS_FIELDS}
    if not empty:
        stats.update(
            {
                "segments_considered": 1,
                "segments_queried": 1,
                "matched_series": 2,
                "projected_series": 2,
                "chunk_reads": 4,
                "bytes_read": 40,
                "samples_decoded": 2,
            }
        )
    labels = {field: 0 for field in gate.LABEL_FIELDS}
    storage = compact_no_result_storage()
    semantic = hashlib.sha256(str(query["query_name"]).encode()).hexdigest()
    block = int(row["block"])
    runs: list[dict[str, object]] = []
    for run_index in range(gate.BENCHMARK_REPEATS):
        scheduler = {field: 0 for field in gate.SCHEDULER_FIELDS}
        payload = {
            "logical_used_bytes": 0,
            "physical_reads": 0,
            "physical_bytes": 0,
        }
        if not empty:
            spans = spans_by_gap[gap]
            physical_bytes = bytes_by_gap[gap]
            payload = {
                "logical_used_bytes": 40,
                "physical_reads": spans,
                "physical_bytes": physical_bytes,
            }
            scheduler.update(
                {
                    "executions": 1,
                    "logical_requests": 4,
                    "physical_spans": spans,
                    "total_physical_bytes_executed": physical_bytes,
                    "session_peak_in_flight_bytes_high_water": physical_bytes,
                }
            )
            if args.backend == "pread":
                scheduler.update(
                    {
                        "pread_decisions": 1,
                        "backend_submissions": spans,
                        "submission_depth_sum": spans,
                        "session_submission_depth_high_water": 1,
                        "submission_depth_1": spans,
                    }
                )
            else:
                bucket = {
                    1: "submission_depth_1",
                    2: "submission_depth_2_3",
                    3: "submission_depth_2_3",
                    4: "submission_depth_4_7",
                }[spans]
                scheduler.update(
                    {
                        "io_uring_decisions": 1,
                        "backend_submissions": 1,
                        "sqes_submitted": spans,
                        "submission_depth_sum": spans,
                        "session_submission_depth_high_water": spans,
                        bucket: 1,
                    }
                )
        duration_ns = block * 100 + (0 if run_index == 0 else run_index * 10)
        runs.append(
            {
                "run_index": run_index,
                "run_kind": "cold" if run_index == 0 else "warm",
                "duration_ns": duration_ns,
                "post_query_fingerprint_ns": 1,
                "semantic_fingerprint": semantic,
                "portable_fingerprint": semantic,
                "result_series": 0 if empty else 2,
                "result_samples": 0 if empty else 2,
                "stats": copy.deepcopy(stats),
                "payload": payload,
                "scheduler": scheduler,
                "labels": copy.deepcopy(labels),
                "label_storage": copy.deepcopy(storage),
                "symbols": {"page_validation_ns_delta": run_index},
                "metadata": {"stable": 1},
                "range_cache": None,
                "stages": {},
            }
        )
    return "a" * 64, runs


def prepare_comparison(root: Path, backend: str = "pread") -> argparse.Namespace:
    manifest, queries = normalized_queries(root)
    corpus = root / "corpus"
    corpus.mkdir()
    (corpus / "segment.bin").write_bytes(b"x")
    binary = root / "chronoxide-query"
    binary.write_bytes(b"same phase3 binary")
    binary_sha = gate.file_sha256(binary)
    runs_dir = root / "runs"
    runs_dir.mkdir()

    inventory = inventory_document(corpus)
    write_json(root / "before.json", inventory)
    write_json(root / "after.json", inventory)
    write_json(
        root / "footer.json",
        {
            "schema": gate.SMOKE_SCHEMA,
            "kind": "footer",
            "gate": "pass",
            "requested": True,
            "effective": True,
        },
    )
    write_json(
        root / "readback.json",
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
        },
    )
    write_json(
        root / "preflight.json",
        {
            "schema": gate.SMOKE_SCHEMA,
            "kind": "io_uring_preflight",
            "gate": "pass",
            "raw_schema": gate.RAW_SCHEMA,
            "chunk_read_mode": "io_uring",
            "queue_depth": 8,
            "binary_sha256": binary_sha,
            "preflight_raw_sha256": "b" * 64,
            "corpus": str(corpus.resolve()),
            "corpus_fingerprint_sha256": "a" * 64,
            "query_name": "no_result",
        },
    )

    plan = gate.expected_plan(queries, backend)
    index_rows: list[dict[str, object]] = []
    residency_rows: list[dict[str, object]] = []
    for planned in plan:
        run_dir = runs_dir / str(planned["process_label"])
        run_dir.mkdir()
        raw_path = run_dir / "raw.json"
        raw_path.write_text("{}\n", encoding="utf-8")
        index_rows.append(
            {
                **planned,
                "binary_sha256": binary_sha,
                "corpus": str(corpus.resolve()),
                "raw_output": str(raw_path.resolve()),
                "process_wall_seconds": "1.0",
                "process_user_seconds": "0.5",
                "process_system_seconds": "0.1",
                "max_rss_kib": 100,
            }
        )
        for phase, resident in (("after-evict", 0), ("after-run", 1)):
            residency_rows.append(
                {
                    "process_label": planned["process_label"],
                    "block": planned["block"],
                    "chunk_read_backend": backend,
                    "payload_coalesce_max_gap_bytes": planned[
                        "payload_coalesce_max_gap_bytes"
                    ],
                    "phase": phase,
                    "file_count": 1,
                    "resident_bytes": resident,
                    "corpus_file_bytes": 1,
                }
            )
    write_tsv(root / "index.tsv", index_rows)
    write_tsv(root / "residency.tsv", residency_rows)
    return argparse.Namespace(
        index=root / "index.tsv",
        manifest=manifest,
        source_manifest=SOURCE_MANIFEST,
        inventory_before=root / "before.json",
        inventory_after=root / "after.json",
        residency=root / "residency.tsv",
        footer_validation=root / "footer.json",
        readback_validation=root / "readback.json",
        io_uring_preflight=root / "preflight.json",
        summary=root / "summary.tsv",
        output=root / "result.json",
        binary=binary,
        corpus=corpus,
        runs_dir=runs_dir,
        backend=backend,
        queue_depth=128 if backend == "pread" else 8,
        preflight_queue_depth=8,
        arena_bytes=gate.DEFAULT_ARENA_BYTES,
        max_resident_bytes_after_evict=0,
        **LIMITS,
    )


def backend_result(
    backend: str, corpus: Path, correctness: str = "d" * 64
) -> dict[str, object]:
    queue_depth = 128 if backend == "pread" else 8
    accounting = [
        {
            "query_name": query_name,
            "run_index": run_index,
            "payload_coalesce_max_gap_bytes": gap,
            "nonphysical_correctness_sha256": correctness,
        }
        for query_name in gate.EXPECTED_QUERY_NAMES
        for run_index in range(gate.BENCHMARK_REPEATS)
        for gap in gate.GAPS
    ]
    measurements: list[dict[str, object]] = []
    for query_index, query_name in enumerate(gate.EXPECTED_QUERY_NAMES):
        empty = query_name == "no_result"
        for gap_index, gap in enumerate(gate.GAPS):
            spans = 0 if empty else (4, 3, 2, 1)[gap_index]
            logical_bytes = 0 if empty else 40
            physical_bytes = 0 if empty else (40, 50, 60, 70)[gap_index]
            scheduler = {field: 0 for field in gate.SCHEDULER_FIELDS}
            if not empty:
                scheduler.update(
                    {
                        "executions": 1,
                        "logical_requests": 4,
                        "physical_spans": spans,
                        "total_physical_bytes_executed": physical_bytes,
                        "session_peak_in_flight_bytes_high_water": physical_bytes,
                    }
                )
                if backend == "pread":
                    scheduler.update(
                        {
                            "pread_decisions": 1,
                            "backend_submissions": spans,
                            "submission_depth_sum": spans,
                            "session_submission_depth_high_water": 1,
                            "submission_depth_1": spans,
                        }
                    )
                else:
                    bucket = {
                        1: "submission_depth_1",
                        2: "submission_depth_2_3",
                        3: "submission_depth_2_3",
                        4: "submission_depth_4_7",
                    }[spans]
                    scheduler.update(
                        {
                            "io_uring_decisions": 1,
                            "backend_submissions": 1,
                            "sqes_submitted": spans,
                            "submission_depth_sum": spans,
                            "session_submission_depth_high_water": spans,
                            bucket: 1,
                        }
                    )
            backend_offset = 20 if backend == "io-uring" else 0
            base = 10_000 + query_index * 1_000 + gap_index * 100 + backend_offset
            cold = [base + block for block in range(gate.BLOCKS)]
            warm = [
                base - 100 + block * 10 + repeat
                for block in range(gate.BLOCKS)
                for repeat in range(gate.BENCHMARK_REPEATS - 1)
            ]
            process_warm_medians = [
                gate.median(
                    warm[
                        block * (gate.BENCHMARK_REPEATS - 1) :
                        (block + 1) * (gate.BENCHMARK_REPEATS - 1)
                    ],
                    "fixture warm",
                )
                for block in range(gate.BLOCKS)
            ]
            rss = [100 + query_index + gap_index + block for block in range(gate.BLOCKS)]
            measurements.append(
                {
                    "query_name": query_name,
                    "payload_coalesce_max_gap_bytes": gap,
                    "cold_duration_ns": cold,
                    "cold_median_ns": gate.median(cold, "fixture cold"),
                    "warm_duration_ns": warm,
                    "process_warm_median_ns": process_warm_medians,
                    "warm_median_ns": gate.median(process_warm_medians, "fixture warm"),
                    "process_max_rss_kib": rss,
                    "process_max_rss_median_kib": gate.median(rss, "fixture RSS"),
                    "accounting_by_run_index": [
                        {
                            "run_index": run_index,
                            "logical_used_bytes": logical_bytes,
                            "physical_spans": spans,
                            "physical_bytes": physical_bytes,
                            "scheduler": copy.deepcopy(scheduler),
                        }
                        for run_index in range(gate.BENCHMARK_REPEATS)
                    ],
                }
            )
    return {
        "schema": gate.RESULT_SCHEMA,
        "correctness_gate": "pass",
        "monotonic_physical_plan_gate": "pass",
        "backend": backend,
        "queue_depth": queue_depth,
        "gaps": list(gate.GAPS),
        "williams_square": [list(sequence) for sequence in gate.WILLIAMS_SQUARE],
        "blocks": gate.BLOCKS,
        "schedule_repetitions": 2,
        "processes_per_gap_per_query": gate.BLOCKS,
        "benchmark_repeats": gate.BENCHMARK_REPEATS,
        "query_label_storage": "compact-ids",
        "query_label_arena_max_bytes": gate.DEFAULT_ARENA_BYTES,
        "max_resident_bytes_after_evict": 0,
        "os_page_cache_eviction_gate": "pass",
        "warm_headline_observation_unit": "per-process median of two warm runs",
        "sealed_query_manifest_sha256": gate.SEALED_QUERY_MANIFEST_SHA256,
        "binary_sha256": "a" * 64,
        "corpus_inventory_sha256": "b" * 64,
        "query_corpus_fingerprint_sha256": "c" * 64,
        "io_uring_preflight": {
            "schema": gate.SMOKE_SCHEMA,
            "kind": "io_uring_preflight",
            "gate": "pass",
            "raw_schema": gate.RAW_SCHEMA,
            "chunk_read_mode": "io_uring",
            "queue_depth": 8,
            "binary_sha256": "a" * 64,
            "preflight_raw_sha256": ("e" if backend == "pread" else "f") * 64,
            "corpus": str(corpus.resolve()),
            "corpus_fingerprint_sha256": "c" * 64,
            "query_name": "no_result",
        },
        "nonphysical_accounting_by_query_run_gap": accounting,
        "exact_across_gaps": list(gate.EXACT_ACROSS_GAPS),
        "allowed_across_gap_differences": list(gate.ALLOWED_ACROSS_GAP_DIFFERENCES),
        "measurements": measurements,
    }


def write_sealed_backend_result(
    root: Path, backend: str, document: dict[str, object] | None = None
) -> Path:
    result_root = root / backend.replace("-", "_")
    comparisons = result_root / "comparisons"
    metadata = result_root / "metadata"
    comparisons.mkdir(parents=True)
    metadata.mkdir()
    corpus = root / "corpus"
    corpus.mkdir(exist_ok=True)
    result = comparisons / "result-gate.json"
    write_json(result, document if document is not None else backend_result(backend, corpus))
    (metadata / "result-artifacts.sha256").write_text(
        f"{gate.file_sha256(result)}  comparisons/result-gate.json\n",
        encoding="utf-8",
    )
    (result_root / "COMPLETE").write_bytes(b"")
    return result


class Phase3PayloadCoalescingGateTests(unittest.TestCase):
    def test_williams_plan_is_balanced_and_ordered(self) -> None:
        query = {"query_name": "q", "category": "control", "mode": "instant"}
        plan = gate.expected_plan([query], "pread")
        self.assertEqual(len(plan), gate.BLOCKS * len(gate.GAPS))
        for block in range(1, gate.BLOCKS + 1):
            rows = [row for row in plan if row["block"] == block]
            self.assertEqual(
                [row["payload_coalesce_max_gap_bytes"] for row in rows],
                list(gate.schedule_for_block(block)),
            )
            self.assertEqual([row["order_index"] for row in rows], [1, 2, 3, 4])
        for gap in gate.GAPS:
            self.assertEqual(
                sum(row["payload_coalesce_max_gap_bytes"] == gap for row in plan),
                gate.BLOCKS,
            )
            for order_index in range(1, 5):
                self.assertEqual(
                    sum(
                        row["payload_coalesce_max_gap_bytes"] == gap
                        and row["order_index"] == order_index
                        for row in plan
                    ),
                    2,
                )

    def test_raw_index_reordering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = prepare_comparison(Path(directory))
            rows = read_tsv(args.index)
            rows[0], rows[1] = rows[1], rows[0]
            write_tsv(args.index, rows)
            with mock.patch.object(gate, "validate_raw", side_effect=validated_runs):
                with self.assertRaisesRegex(gate.GateError, "row sequence"):
                    gate.compare_results(args)

    def test_duplicate_aliased_outside_and_symlink_raw_paths_are_rejected(self) -> None:
        cases = ("duplicate", "alias", "outside", "symlink")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                args = prepare_comparison(root)
                rows = read_tsv(args.index)
                if case == "duplicate":
                    rows[1]["raw_output"] = rows[0]["raw_output"]
                elif case == "alias":
                    expected = Path(rows[1]["raw_output"])
                    rows[1]["raw_output"] = str(
                        expected.parent / ".." / expected.parent.name / "raw.json"
                    )
                elif case == "outside":
                    outside = root / "outside.json"
                    outside.write_text("{}\n", encoding="utf-8")
                    rows[1]["raw_output"] = str(outside)
                else:
                    expected = Path(rows[1]["raw_output"])
                    target = root / "symlink-target.json"
                    target.write_text("{}\n", encoding="utf-8")
                    expected.unlink()
                    expected.symlink_to(target)
                write_tsv(args.index, rows)
                with mock.patch.object(gate, "validate_raw", side_effect=validated_runs):
                    with self.assertRaisesRegex(
                        gate.GateError, "raw_output|canonical|exactly"
                    ):
                        gate.compare_results(args)

    def test_no_result_allows_only_the_compact_budget_gauge(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, queries = normalized_queries(root)
            del manifest
            query = next(query for query in queries if query["query_name"] == "no_result")
            corpus = root / "corpus"
            corpus.mkdir()
            raw_path = root / "raw.json"
            raw = no_result_raw(corpus, query)
            write_json(raw_path, raw)
            row = {
                "raw_output": str(raw_path),
                "payload_coalesce_max_gap_bytes": "0",
            }
            fingerprint, runs = gate.validate_raw(row, query, raw_args(corpus))
            self.assertEqual(fingerprint, "a" * 64)
            self.assertEqual(
                runs[0]["label_storage"]["compact_arena_budget_bytes"],
                gate.DEFAULT_ARENA_BYTES,
            )

            active = copy.deepcopy(raw)
            active["runs"][0]["query_label_storage"]["compact_atom_lookups"] = 1
            active["runs"][0]["query_label_storage"]["compact_atom_hits"] = 1
            active_path = root / "active.json"
            write_json(active_path, active)
            row["raw_output"] = str(active_path)
            with self.assertRaisesRegex(gate.GateError, "no-result control"):
                gate.validate_raw(row, query, raw_args(corpus))

    def test_smoke_inventory_and_preflight_shapes_are_strict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = root / "corpus"
            corpus.mkdir()
            valid_smokes = {
                "footer": {
                    "schema": gate.SMOKE_SCHEMA,
                    "kind": "footer",
                    "gate": "pass",
                    "requested": True,
                    "effective": True,
                },
                "readback": {
                    "schema": gate.SMOKE_SCHEMA,
                    "kind": "readback",
                    "gate": "pass",
                    "expected": 1,
                    "executed": 1,
                    "skipped": 0,
                    "isolation_skips": 0,
                    "checked": 1,
                    "mismatches": 0,
                },
                "io_uring_preflight": {
                    "schema": gate.SMOKE_SCHEMA,
                    "kind": "io_uring_preflight",
                    "gate": "pass",
                    "raw_schema": gate.RAW_SCHEMA,
                    "chunk_read_mode": "io_uring",
                    "queue_depth": 8,
                    "binary_sha256": "a" * 64,
                    "preflight_raw_sha256": "b" * 64,
                    "corpus": str(corpus.resolve()),
                    "corpus_fingerprint_sha256": "c" * 64,
                    "query_name": "no_result",
                },
            }
            for kind, value in valid_smokes.items():
                path = root / f"{kind}.json"
                write_json(path, value)
                gate.validate_smoke_json(path, kind)
                tampered = {**value, "unexpected": 1}
                write_json(root / f"{kind}-bad.json", tampered)
                with self.assertRaisesRegex(gate.GateError, "invalid shape"):
                    gate.validate_smoke_json(root / f"{kind}-bad.json", kind)

            inventory = inventory_document(corpus)
            inventory_path = root / "inventory.json"
            write_json(inventory_path, inventory)
            gate.load_inventory(inventory_path, corpus)
            for field, value in (("file_count", 2), ("total_bytes", 2), ("corpus_sha256", "d" * 64)):
                tampered = copy.deepcopy(inventory)
                tampered[field] = value
                path = root / f"inventory-{field}.json"
                write_json(path, tampered)
                with self.assertRaises(gate.GateError):
                    gate.load_inventory(path, corpus)

    def test_preflight_raw_rejects_configuration_shape_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, queries = normalized_queries(root)
            del manifest
            query = next(query for query in queries if query["query_name"] == "no_result")
            corpus = root / "corpus"
            corpus.mkdir()
            binary = root / "chronoxide-query"
            binary.write_bytes(b"io_uring fixture")
            raw = no_result_raw(
                corpus, query, backend="io-uring", queue_depth=8, repeats=1
            )
            raw_path = root / "preflight.raw.json"
            write_json(raw_path, raw)
            gate.validate_io_uring_preflight(
                raw_path,
                binary,
                corpus,
                query,
                8,
                gate.DEFAULT_ARENA_BYTES,
                LIMITS,
                root / "preflight.json",
            )

            tampered = copy.deepcopy(raw)
            tampered["configuration"]["unexpected"] = True
            tampered_path = root / "preflight-tampered.raw.json"
            write_json(tampered_path, tampered)
            with self.assertRaisesRegex(gate.GateError, "invalid shape"):
                gate.validate_io_uring_preflight(
                    tampered_path,
                    binary,
                    corpus,
                    query,
                    8,
                    gate.DEFAULT_ARENA_BYTES,
                    LIMITS,
                    root / "unused.json",
                )

    def test_cold_warm_accounting_and_high_water_regressions_are_rejected(self) -> None:
        for case in ("stats", "high-water"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                args = prepare_comparison(Path(directory))

                def changed_runs(
                    row: dict[str, str], query: dict[str, object], namespace: argparse.Namespace
                ) -> tuple[str, list[dict[str, object]]]:
                    fingerprint, runs = validated_runs(row, query, namespace)
                    if row["process_label"] == read_tsv(args.index)[0]["process_label"]:
                        if case == "stats":
                            runs[1]["stats"]["bytes_read"] += 1
                        elif runs[1]["scheduler"]["physical_spans"]:
                            runs[1]["scheduler"][
                                "session_peak_in_flight_bytes_high_water"
                            ] = 0
                    return fingerprint, runs

                with mock.patch.object(gate, "validate_raw", side_effect=changed_runs):
                    with self.assertRaisesRegex(
                        gate.GateError, "changed cold-to-warm|decreased cold-to-warm"
                    ):
                        gate.compare_results(args)

    def test_io_uring_scheduler_rejects_bucket_and_high_water_inconsistency(self) -> None:
        scheduler = {field: 0 for field in gate.SCHEDULER_FIELDS}
        scheduler.update(
            {
                "executions": 1,
                "io_uring_decisions": 1,
                "logical_requests": 9,
                "physical_spans": 9,
                "backend_submissions": 2,
                "sqes_submitted": 9,
                "submission_depth_sum": 9,
                "session_submission_depth_high_water": 8,
                "submission_depth_1": 1,
                "submission_depth_8_plus": 1,
                "total_physical_bytes_executed": 90,
                "session_peak_in_flight_bytes_high_water": 80,
            }
        )
        payload = {"logical_used_bytes": 80, "physical_reads": 9, "physical_bytes": 90}
        stats = {"chunk_reads": 9}
        gate.validate_scheduler(scheduler, "io-uring", 8, payload, stats, "valid")

        invalid = copy.deepcopy(scheduler)
        invalid["session_submission_depth_high_water"] = 1
        with self.assertRaisesRegex(gate.GateError, "incompatible"):
            gate.validate_scheduler(invalid, "io-uring", 8, payload, stats, "invalid")
        invalid = copy.deepcopy(scheduler)
        invalid["backend_submissions"] = 3
        with self.assertRaisesRegex(gate.GateError, "buckets"):
            gate.validate_scheduler(invalid, "io-uring", 8, payload, stats, "invalid")
        invalid = copy.deepcopy(scheduler)
        invalid["session_peak_in_flight_bytes_high_water"] = 91
        with self.assertRaisesRegex(gate.GateError, "exceed"):
            gate.validate_scheduler(invalid, "io-uring", 8, payload, stats, "invalid")

    def test_warm_headline_uses_process_clustered_medians(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = prepare_comparison(Path(directory))
            with mock.patch.object(gate, "validate_raw", side_effect=validated_runs):
                gate.compare_results(args)
            result = json.loads(args.output.read_text(encoding="utf-8"))
            point = next(
                value
                for value in result["measurements"]
                if value["query_name"] == "broad_raw_count_selector"
                and value["payload_coalesce_max_gap_bytes"] == 0
            )
            self.assertEqual(
                point["process_warm_median_ns"],
                [115.0, 215.0, 315.0, 415.0, 515.0, 615.0, 715.0, 815.0],
            )
            self.assertEqual(point["warm_median_ns"], 465.0)
            self.assertEqual(len(point["warm_duration_ns"]), 16)

    def test_compare_backends_requires_matching_semantics_and_physical_plans(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pread_path = write_sealed_backend_result(root, "pread")
            io_path = write_sealed_backend_result(root, "io-uring")
            args = argparse.Namespace(
                pread_result=pread_path,
                io_uring_result=io_path,
                output=root / "comparison.json",
            )
            gate.compare_backends(args)
            compared = json.loads(args.output.read_text(encoding="utf-8"))
            self.assertEqual(compared["correctness_gate"], "pass")
            self.assertEqual(
                len(compared["paired_measurements"]),
                len(gate.EXPECTED_QUERY_NAMES) * len(gate.GAPS),
            )
            first = compared["paired_measurements"][0]
            self.assertEqual(len(first["paired_blocks"]), gate.BLOCKS)
            self.assertEqual(
                first["headline_medians"]["cold_duration_ns"][
                    "io_uring_minus_pread"
                ],
                20.0,
            )

            changed_root = root / "changed"
            changed_root.mkdir()
            changed = backend_result("io-uring", root / "corpus")
            changed["nonphysical_accounting_by_query_run_gap"][0][
                "nonphysical_correctness_sha256"
            ] = "e" * 64
            changed_path = write_sealed_backend_result(
                changed_root, "io-uring", changed
            )
            with self.assertRaisesRegex(gate.GateError, "nonphysical_accounting"):
                gate.compare_backends(
                    argparse.Namespace(
                        pread_result=pread_path,
                        io_uring_result=changed_path,
                        output=root / "unused.json",
                    )
                )

            changed_plan_root = root / "changed-plan"
            changed_plan_root.mkdir()
            changed_plan = backend_result("io-uring", root / "corpus")
            point = changed_plan["measurements"][3]
            for accounting in point["accounting_by_run_index"]:
                accounting["physical_bytes"] += 1
                accounting["scheduler"]["total_physical_bytes_executed"] += 1
                accounting["scheduler"][
                    "session_peak_in_flight_bytes_high_water"
                ] += 1
            changed_plan_path = write_sealed_backend_result(
                changed_plan_root, "io-uring", changed_plan
            )
            with self.assertRaisesRegex(gate.GateError, "payload planning"):
                gate.compare_backends(
                    argparse.Namespace(
                        pread_result=pread_path,
                        io_uring_result=changed_plan_path,
                        output=root / "unused-plan.json",
                    )
                )

    def test_backend_comparison_rejects_tampered_measurement_and_unsealed_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pread_path = write_sealed_backend_result(root, "pread")
            io_path = write_sealed_backend_result(root, "io-uring")

            tampered_root = root / "tampered"
            tampered_root.mkdir()
            tampered = backend_result("io-uring", root / "corpus")
            tampered["measurements"][0]["cold_median_ns"] += 1
            tampered_path = write_sealed_backend_result(
                tampered_root, "io-uring", tampered
            )
            with self.assertRaisesRegex(gate.GateError, "median"):
                gate.compare_backends(
                    argparse.Namespace(
                        pread_result=pread_path,
                        io_uring_result=tampered_path,
                        output=root / "tampered-output.json",
                    )
                )

            io_path.parent.parent.joinpath("COMPLETE").unlink()
            with self.assertRaisesRegex(gate.GateError, "COMPLETE"):
                gate.compare_backends(
                    argparse.Namespace(
                        pread_result=pread_path,
                        io_uring_result=io_path,
                        output=root / "unsealed-output.json",
                    )
                )
            io_path.parent.parent.joinpath("COMPLETE").write_bytes(b"")
            io_path.write_text(io_path.read_text(encoding="utf-8") + " ", encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "digest differs"):
                gate.compare_backends(
                    argparse.Namespace(
                        pread_result=pread_path,
                        io_uring_result=io_path,
                        output=root / "checksum-output.json",
                    )
                )

    def test_runner_preserves_phase3_measurement_contract(self) -> None:
        source = (HERE / "phase3_payload_coalescing_run.sh").read_text(
            encoding="utf-8"
        )
        for contract in (
            '[[ "$BACKEND" == "pread" || "$BACKEND" == "io-uring" ]]',
            'CHUNK_READ_QUEUE_DEPTH=128',
            'CHUNK_READ_QUEUE_DEPTH=8',
            '--chunk-read-mode "$backend"',
            '--chunk-read-queue-depth "$CHUNK_READ_QUEUE_DEPTH"',
            '--chunk-payload-coalesce-max-gap-bytes "$gap"',
            'MEMLOCK_SOFT_KIB="$(ulimit -S -l)"',
            'MEMLOCK_HARD_KIB="$(ulimit -H -l)"',
            '"$METADATA_DIR/memlock.txt"',
            '"$METADATA_DIR/io-uring-memlock-coverage-warning.txt"',
            'run_io_uring_preflight() {',
            'note "running real forced-io_uring setup preflight"',
            'run_io_uring_preflight',
            '[[ "$argument" != "--validate-segment-footers" ]]',
        ):
            self.assertIn(contract, source)

        result_gate = source.index('python3 "$FROZEN_GATE_TOOL" compare-results')
        checksum_write = source.index(
            ') >"$METADATA_DIR/result-artifacts.sha256"', result_gate
        )
        checksum_verify = source.index(
            'sha256sum -c metadata/result-artifacts.sha256', checksum_write
        )
        complete = source.index('touch "$RESULT_DIR/COMPLETE"', checksum_verify)
        self.assertLess(result_gate, checksum_write)
        self.assertLess(checksum_write, checksum_verify)
        self.assertLess(checksum_verify, complete)


if __name__ == "__main__":
    unittest.main()
