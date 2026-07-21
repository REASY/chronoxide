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

import phase3_payload_attribution_gate as gate
import test_phase1_query_gate as phase1_fixture
import test_phase2_compact_ids_ab_gate as phase2_fixture


HERE = Path(__file__).resolve().parent
SOURCE_MANIFEST = Path(__import__("phase3_payload_coalescing_gate").__file__).resolve().parent / "phase2_compact_ids_queries.json"
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


def detailed_stages(duration_ns: int = 100) -> dict[str, int]:
    stages = {field: 0 for field in gate.common.QUERY_STAGE_FIELDS}
    stages.update(
        {
            "payload_read_pipeline_combined_ns": 20,
            "payload_decode_projection_result_processing_combined_ns": 30,
            "exclusive_total_ns": 50,
            "unclassified_ns": duration_ns - 50,
        }
    )
    return stages


def scheduler(backend: str, spans: int, physical_bytes: int) -> dict[str, int]:
    value = {field: 0 for field in gate.phase3.SCHEDULER_FIELDS}
    value.update(
        {
            "executions": 1,
            "logical_requests": 4,
            "physical_spans": spans,
            "total_physical_bytes_executed": physical_bytes,
            "session_peak_in_flight_bytes_high_water": physical_bytes,
        }
    )
    if backend == "pread":
        value.update(
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
            4: "submission_depth_4_7",
        }[spans]
        value.update(
            {
                "io_uring_decisions": 1,
                "backend_submissions": 1,
                "sqes_submitted": spans,
                "submission_depth_sum": spans,
                "session_submission_depth_high_water": spans,
                bucket: 1,
            }
        )
    return value


def synthetic_raw(
    corpus: Path, query: dict[str, object], backend: str, gap: int
) -> dict[str, object]:
    spans = {0: 4, 1024: 2, 4096: 1}[gap]
    physical_bytes = {0: 40, 1024: 50, 4096: 60}[gap]
    stats = {field: 0 for field in gate.common.QUERY_STATS_FIELDS}
    stats.update(
        {
            "segments_considered": 1,
            "segments_queried": 1,
            "matched_series": 2,
            "projected_series": 2,
            "chunk_reads": 4,
            "bytes_read": 40,
            "samples_decoded": 2,
            "index_postings_reads": 1,
            "index_postings_bytes_read": 4,
        }
    )
    if query["category"] in gate.phase2.TYPED_FULL_CATEGORIES:
        stats["typed_full_chunks_decoded"] = 1
    labels = {field: 0 for field in gate.phase2.LABEL_FIELDS}
    labels.update(
        {
            "rows_integrity_checked": 2,
            "pairs_integrity_checked": 8,
            "content_bytes_materialized": 32,
        }
    )
    if query["category"] in gate.phase2.FULL_DEMAND_CATEGORIES:
        labels.update({"rows_full_materialized": 2, "pairs_materialized": 8})
    else:
        labels.update(
            {
                "rows_selectively_materialized": 2,
                "pairs_materialized": 4,
                "pairs_omitted": 4,
            }
        )
    fingerprint = hashlib.sha256(str(query["query_name"]).encode()).hexdigest()
    runs = []
    for run_index in range(gate.BENCHMARK_REPEATS):
        duration = 100 + run_index
        run_stages = detailed_stages(duration)
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
                "result_samples": 2,
                "stats": copy.deepcopy(stats),
                "payload_reads": {
                    "logical_used_bytes": 40,
                    "physical_reads": spans,
                    "physical_bytes": physical_bytes,
                },
                "symbol_reads": phase1_fixture.symbol_reads(),
                "label_materialization": copy.deepcopy(labels),
                "query_label_storage": phase2_fixture.compact_storage(),
                "query_stages": run_stages,
                "metadata_runtime": phase1_fixture.metadata_runtime(),
                "range_scalar_cache": phase1_fixture.range_cache(query),
                "chunk_read_scheduler": scheduler(backend, spans, physical_bytes),
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
            "mode": query["mode"] if query["mode"] == "instant" else "query_range",
            "step_ms": query["step_ms"],
            "range_scalar_cache_max_bytes": query["range_scalar_cache_max_bytes"],
            "chunk_read_mode": gate.phase3.raw_backend_name(backend),
            "chunk_read_queue_depth": gate.QUEUE_DEPTHS[backend],
            "chunk_payload_coalesce_max_gap_bytes": gap,
            "experimental_cross_segment_chunk_reads": False,
            "label_materialization": "demand-driven",
            "query_label_storage": "compact-ids",
            "query_instrumentation": "detailed",
            "query_label_arena_max_bytes": gate.ARENA_BYTES,
            "storage_layout": "schema8",
            "benchmark_repeats": gate.BENCHMARK_REPEATS,
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


def prepare(root: Path) -> argparse.Namespace:
    manifest = root / "queries.json"
    gate.write_manifest(SOURCE_MANIFEST, root / "queries.tsv", manifest)
    queries = gate.read_manifest(manifest, SOURCE_MANIFEST)
    query_by_name = {query["query_name"]: query for query in queries}
    plan = gate.expected_plan(queries)
    corpus = root / "corpus"
    corpus.mkdir()
    (corpus / "segment.bin").write_bytes(b"x")
    binary = root / "chronoxide-query"
    binary.write_bytes(b"same final binary")
    binary_hash = gate.file_sha256(binary)
    runs_dir = root / "runs"
    runs_dir.mkdir()

    gate.common.write_inventory(corpus, root / "before.json", root / "files.nul")
    gate.common.write_inventory(corpus, root / "after.json", root / "files-after.nul")
    index_rows: list[dict[str, object]] = []
    residency_rows: list[dict[str, object]] = []
    for planned in plan:
        run_dir = runs_dir / str(planned["process_label"])
        run_dir.mkdir()
        raw_path = run_dir / "raw.json"
        write_json(
            raw_path,
            synthetic_raw(
                corpus,
                query_by_name[str(planned["query_name"])],
                str(planned["chunk_read_backend"]),
                int(planned["payload_coalesce_max_gap_bytes"]),
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
                "max_rss_kib": 100,
            }
        )
        for phase, resident in (("after-evict", 0), ("after-run", 1)):
            residency_rows.append(
                {
                    "process_label": planned["process_label"],
                    "chunk_read_backend": planned["chunk_read_backend"],
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
        summary=root / "summary.tsv",
        output=root / "result.json",
        binary=binary,
        corpus=corpus,
        runs_dir=runs_dir,
        max_resident_bytes_after_evict=0,
        **LIMITS,
    )


class Phase3PayloadAttributionGateTests(unittest.TestCase):
    def test_manifest_and_plan_are_the_fixed_24_process_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "queries.json"
            gate.write_manifest(SOURCE_MANIFEST, root / "queries.tsv", manifest)
            queries = gate.read_manifest(manifest, SOURCE_MANIFEST)
            self.assertEqual(tuple(query["query_name"] for query in queries), gate.QUERY_NAMES)
            plan = gate.expected_plan(queries)
            self.assertEqual(len(plan), 24)
            self.assertEqual(
                {
                    (
                        row["query_name"],
                        row["chunk_read_backend"],
                        row["payload_coalesce_max_gap_bytes"],
                    )
                    for row in plan
                },
                {
                    (query, backend, gap)
                    for query in gate.QUERY_NAMES
                    for backend in gate.BACKENDS
                    for gap in gate.GAPS
                },
            )

    def test_detailed_stage_leaves_must_be_nonzero_and_bounded(self) -> None:
        stages = detailed_stages()
        self.assertEqual(
            gate.validate_detailed_stages(stages, 100, "valid")[
                "payload_read_pipeline_combined_ns"
            ],
            20,
        )
        for field in gate.REQUIRED_STAGE_LEAVES:
            invalid = copy.deepcopy(stages)
            invalid["exclusive_total_ns"] -= invalid[field]
            invalid["unclassified_ns"] += invalid[field]
            invalid[field] = 0
            with self.assertRaisesRegex(gate.GateError, "stage leaf"):
                gate.validate_detailed_stages(invalid, 100, "invalid")
        invalid = copy.deepcopy(stages)
        invalid["payload_read_pipeline_combined_ns"] = 101
        invalid["exclusive_total_ns"] = 131
        invalid["unclassified_ns"] = 0
        with self.assertRaisesRegex(gate.GateError, "exceed"):
            gate.validate_detailed_stages(invalid, 100, "invalid")

    def test_complete_synthetic_gate_emits_diagnostic_stage_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = prepare(Path(directory))
            gate.compare_results(args)
            result = json.loads(args.output.read_text(encoding="utf-8"))
            self.assertEqual(result["correctness_gate"], "pass")
            self.assertEqual(result["stage_attribution_gate"], "pass")
            self.assertIn("MUST NOT", result["timing_comparability"])
            self.assertEqual(result["process_count"], 24)
            self.assertEqual(result["evaluation_count"], 48)
            self.assertEqual(len(result["aggregate_stage_attribution"]), 24)
            first = result["aggregate_stage_attribution"][0]
            self.assertEqual(set(first["by_run_kind"]), {"cold", "warm"})
            self.assertNotIn("payload_read_pipeline_median_ns", first)
            self.assertEqual(
                first["by_run_kind"]["cold"][
                    "payload_read_pipeline_combined_ns"
                ],
                20,
            )
            with args.summary.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
            self.assertEqual(len(rows), 48)
            self.assertTrue(
                all(
                    row["timing_comparability"]
                    == "diagnostic-detailed-wall-non-comparable"
                    for row in rows
                )
            )

    def test_cross_backend_logical_change_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = prepare(Path(directory))
            with args.index.open(newline="", encoding="utf-8") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
            target = next(
                row
                for row in rows
                if row["chunk_read_backend"] == "io-uring"
                and row["payload_coalesce_max_gap_bytes"] == "1024"
            )
            raw_path = Path(target["raw_output"])
            raw = json.loads(raw_path.read_text(encoding="utf-8"))
            raw["runs"][0]["stats"]["bytes_read"] += 1
            raw["runs"][0]["payload_reads"]["logical_used_bytes"] += 1
            write_json(raw_path, raw)
            with self.assertRaisesRegex(gate.GateError, "logical accounting differs"):
                gate.compare_results(args)

    def test_runner_contract_is_observer_heavy_and_never_profiles_timed_processes(self) -> None:
        runner = (HERE / "phase3_payload_attribution_run.sh").read_text(encoding="utf-8")
        for required in (
            "--query-instrumentation detailed",
            "--benchmark-repeats \"$BENCHMARK_REPEATS\"",
            "--chunk-read-mode \"$backend\"",
            "--chunk-payload-coalesce-max-gap-bytes \"$gap\"",
            "fincore --bytes",
            "ulimit -l",
            "MUST NOT be compared",
            "sha256sum -c",
        ):
            self.assertIn(required, runner)
        timed_body = runner.split("/usr/bin/time \\\n", 1)[1].split("status=$?", 1)[0]
        self.assertNotIn("--validate-segment-footers", timed_body)
        self.assertNotIn("--verify-readbacks", timed_body)
        self.assertNotIn("perf ", timed_body)


if __name__ == "__main__":
    unittest.main()
