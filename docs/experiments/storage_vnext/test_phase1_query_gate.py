#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import phase1_query_gate as gate


def metadata_runtime() -> dict[str, object]:
    cache_classes = [
        {
            "class": "series_hot_page",
            "resident_admissions": 0,
            "resident_admission_refusals": 0,
            "resident_admission_bypasses": 0,
        }
    ]
    class_charges = [
        {"class": "series_hot_page", "in_flight_bytes": 0, "retained_bytes": 0}
    ]
    usage_charges = [
        {"usage": "series_hot_page", "in_flight_bytes": 0, "retained_bytes": 0}
    ]
    gauges = {
        "cache": {
            "resident_entries": 0,
            "live_allocations": 0,
            "active_loads": 0,
            "registered_artifacts": 1,
            "ledger_reserved_bytes": 0,
            "ledger_in_flight_bytes": 0,
            "ledger_retained_bytes": 0,
            "sticky_artifacts": 0,
            "sticky_charged_bytes": 0,
            "class_charges": class_charges,
        },
        "governor": {
            "retained_max_bytes": 1024,
            "in_flight_max_bytes": 1024,
            "retained_bytes": 0,
            "in_flight_bytes": 0,
            "usage_charges": usage_charges,
        },
        "file_manager": {
            "max_open_files": 64,
            "max_cached_open_files": 32,
            "open_files": 0,
            "occupied_open_slots": 0,
            "active_open_files": 0,
            "cached_open_files": 0,
            "opening_files": 0,
            "pending_open_files": 0,
            "preflighting_files": 0,
            "closing_files": 0,
            "active_leases": 0,
        },
    }
    return {
        "counters_delta": {
            "cache": {
                "hits": 0,
                "misses": 0,
                "evictions": 0,
                "single_flight_waits": 0,
                "successful_loads": 0,
                "failed_loads": 0,
                "corruption_detections": 0,
                "corruption_hits": 0,
                "resident_admissions": 0,
                "resident_admission_refusals": 0,
                "resident_admission_bypasses": 0,
                "class_admissions": cache_classes,
            },
            "governor": {"retained_refusals": 0, "in_flight_refusals": 0},
            "file_manager": {field: 0 for field in gate.FILE_COUNTER_FIELDS},
            "reads": {
                "issued": {"calls": 0, "bytes": 0},
                "unclassified": {"calls": 0, "bytes": 0},
                "by_file": [],
                "by_class": [],
            },
        },
        "start_gauges": gauges,
        "end_gauges": json.loads(json.dumps(gauges)),
        "lifetime_peaks_after_run": {
            "cache_class_charges": [
                {
                    "class": "series_hot_page",
                    "peak_in_flight_bytes": 0,
                    "peak_retained_bytes": 0,
                }
            ],
            "governor": {
                "peak_retained_bytes": 0,
                "peak_in_flight_bytes": 0,
                "usage_charges": [
                    {
                        "usage": "series_hot_page",
                        "peak_in_flight_bytes": 0,
                        "peak_retained_bytes": 0,
                    }
                ],
            },
            "file_manager": {field: 0 for field in gate.FILE_PEAK_FIELDS},
        },
    }


def symbol_reads() -> dict[str, object]:
    reads: dict[str, object] = {
        field: {"calls": 0, "bytes": 0}
        for field in (
            "legacy_eager_read_delta",
            "logical_returned_delta",
            "root_read_delta",
            "page_read_delta",
            "page_validation_delta",
        )
    }
    reads.update(
        {
            field: 0
            for field in gate.SYMBOL_READ_FIELDS
            if field not in reads
        }
    )
    return reads


def range_cache(query: dict[str, object]) -> dict[str, object] | None:
    if query["mode"] == "instant":
        return None
    report: dict[str, object] = {
        field: 0 for field in gate.RANGE_CACHE_FIELDS
    }
    for field in ("governor_refused", "allocation_refused", "layout_overflow"):
        report[field] = False
    report["configured_budget_bytes"] = query["range_scalar_cache_max_bytes"]
    return report


def semantic_key(query: dict[str, object]) -> str:
    return str(
        query.get("semantic_group")
        or query.get("equivalence_group")
        or query["query_name"]
    )


def raw_document(
    corpus: Path,
    query: dict[str, object],
    instrumentation: str,
) -> dict[str, object]:
    empty = query["expected_result"] == "empty"
    stats = {field: 0 for field in gate.common.QUERY_STATS_FIELDS}
    if not empty:
        stats.update(
            {
                "segments_considered": 1,
                "segments_queried": 1,
                "matched_series": 1,
                "projected_series": 1,
                "chunk_reads": 1,
                "bytes_read": 8,
                "samples_decoded": 2,
                "index_postings_reads": 1,
                "index_postings_bytes_read": 4,
            }
        )
        if query["decode_expectation"] == "scalar":
            stats["typed_scalar_chunks_decoded"] = 1
        elif query["decode_expectation"] == "full":
            stats["typed_full_chunks_decoded"] = 1
    labels = {field: 0 for field in gate.LABEL_FIELDS}
    if not empty:
        labels.update(
            {
                "rows_integrity_checked": 1,
                "pairs_integrity_checked": 4,
                "pairs_materialized": 2,
                "content_bytes_materialized": 16,
            }
        )
        if query["materialization_expectation"] == "selective":
            labels["rows_selectively_materialized"] = 1
            labels["pairs_omitted"] = 2
        else:
            labels["rows_full_materialized"] = 1
            labels["pairs_materialized"] = 4
    fingerprint = hashlib.sha256(semantic_key(query).encode()).hexdigest()
    runs = []
    for run_index in range(gate.BENCHMARK_REPEATS):
        duration_ns = 100 + run_index
        stages = {field: 0 for field in gate.common.QUERY_STAGE_FIELDS}
        if instrumentation == "detailed":
            stages["candidate_selection_ns"] = 1
            stages["exclusive_total_ns"] = 1
            stages["unclassified_ns"] = duration_ns - 1
        else:
            stages["unclassified_ns"] = duration_ns
        runs.append(
            {
                "query": query["expression"],
                "run_kind": "cold" if run_index == 0 else "warm",
                "run_index": run_index,
                "duration_ns": duration_ns,
                "post_query_fingerprint_ns": 7,
                "effective_start_ms": query["start_ms"],
                "effective_end_ms": query["end_ms"],
                "step_ms": query["step_ms"],
                "semantic_fingerprint_sha256": fingerprint,
                "portable_semantic_fingerprint_sha256": fingerprint,
                "result_series": 0 if empty else 1,
                "result_samples": 0 if empty else 2,
                "stats": stats,
                "payload_reads": {
                    "logical_used_bytes": stats["bytes_read"],
                    "physical_reads": 0 if empty else 1,
                    "physical_bytes": stats["bytes_read"],
                },
                "symbol_reads": symbol_reads(),
                "label_materialization": labels,
                "query_label_storage": {
                    "label_sets": 0 if empty else 1,
                    "atom_lookups": 0,
                    "atom_hits": 0,
                    "atom_misses": 0,
                    "unique_content_bytes": 0,
                },
                "query_stages": stages,
                "metadata_runtime": metadata_runtime(),
                "range_scalar_cache": range_cache(query),
            }
        )
    return {
        "schema": gate.RAW_SCHEMA,
        "corpus_fingerprint_sha256": "a" * 64,
        "corpus_fingerprint_duration_ns": 9,
        "configuration": {
            "segments_dir": str(corpus.resolve()),
            "start_ms": query["start_ms"],
            "end_ms": query["end_ms"],
            "mode": query["mode"] if query["mode"] == "instant" else "query_range",
            "step_ms": query["step_ms"],
            "range_scalar_cache_max_bytes": query["range_scalar_cache_max_bytes"],
            "chunk_read_mode": "pread",
            "chunk_read_queue_depth": 128,
            "experimental_cross_segment_chunk_reads": False,
            "label_materialization": query["label_materialization"],
            "query_label_storage": "owned-strings",
            "query_instrumentation": instrumentation,
            "storage_layout": "schema8",
            "benchmark_repeats": gate.BENCHMARK_REPEATS,
            "queries": [query["expression"]],
            "prewarm_query_contexts": False,
            "prefetch_query_data": False,
            "exponential_histogram_bucket_boundaries": [],
            "requested_segment_footer_validation": False,
            "effective_segment_footer_validation": False,
        },
        "limits": {
            "max_matched_series": 100,
            "max_projected_series": 200,
            "max_chunk_reads": 300,
            "max_bytes_read": 400,
            "max_samples_decoded": 500,
            "max_regex_values_examined": 600,
        },
        "runs": runs,
    }


def compare_args(root: Path) -> argparse.Namespace:
    return argparse.Namespace(
        index=root / "raw-index.tsv",
        manifest=root / "queries.normalized.json",
        inventory_before=root / "inventory-before.json",
        inventory_after=root / "inventory-after.json",
        residency=root / "residency.tsv",
        footer_validation=root / "footer.json",
        readback_validation=root / "readbacks.json",
        summary=root / "summary.tsv",
        output=root / "result.json",
        queue_depth=128,
        max_resident_bytes_after_evict=0,
        max_matched_series=100,
        max_projected_series=200,
        max_chunk_reads=300,
        max_bytes_read=400,
        max_samples_decoded=500,
        max_regex_values_examined=600,
    )


def prepare_comparison(root: Path) -> argparse.Namespace:
    manifest_path = Path(__file__).with_name("phase1_query_matrix.json")
    normalized = gate.normalize_manifest(manifest_path)
    (root / "queries.normalized.json").write_text(
        json.dumps(normalized), encoding="utf-8"
    )
    corpus = root / "corpus"
    corpus.mkdir()
    identity = normalized["corpus"]
    inventory = {
        "schema": gate.INVENTORY_SCHEMA,
        "corpus": str(corpus.resolve()),
        "file_count": identity["file_count"],
        "total_bytes": identity["total_bytes"],
        "segments_manifest_sha256": identity["segments_manifest_sha256"],
        "fixed_identity": identity,
        "corpus_sha256": "b" * 64,
        "files": [],
    }
    for name in ("inventory-before.json", "inventory-after.json"):
        (root / name).write_text(json.dumps(inventory), encoding="utf-8")
    (root / "footer.json").write_text(
        json.dumps(
            {
                "schema": gate.SMOKE_SCHEMA,
                "kind": "footer",
                "requested": True,
                "effective": True,
            }
        ),
        encoding="utf-8",
    )
    (root / "readbacks.json").write_text(
        json.dumps(
            {
                "schema": gate.SMOKE_SCHEMA,
                "kind": "readback",
                "expected": 38,
                "executed": 38,
                "checked": 38,
                "skipped": 0,
                "isolation_skips": 0,
                "mismatches": 0,
            }
        ),
        encoding="utf-8",
    )
    query_by_name = {query["query_name"]: query for query in normalized["queries"]}
    index_rows = []
    residency_rows = []
    for planned in gate.expected_plan(normalized["queries"]):
        raw_path = root / f"{planned['process_label']}.json"
        raw_path.write_text(
            json.dumps(
                raw_document(
                    corpus,
                    query_by_name[planned["query_name"]],
                    planned["query_instrumentation"],
                )
            ),
            encoding="utf-8",
        )
        index_rows.append(
            {
                **planned,
                "corpus": str(corpus.resolve()),
                "raw_output": str(raw_path),
                "max_rss_kib": 123,
            }
        )
        for phase, resident_bytes in (("after-evict", 0), ("after-run", 4096)):
            residency_rows.append(
                {
                    "process_label": planned["process_label"],
                    "abba_block": planned["abba_block"],
                    "query_instrumentation": planned["query_instrumentation"],
                    "phase": phase,
                    "file_count": identity["file_count"],
                    "resident_bytes": resident_bytes,
                    "corpus_file_bytes": identity["total_bytes"],
                }
            )
    with (root / "raw-index.tsv").open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=index_rows[0], delimiter="\t")
        writer.writeheader()
        writer.writerows(index_rows)
    with (root / "residency.tsv").open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=residency_rows[0], delimiter="\t")
        writer.writeheader()
        writer.writerows(residency_rows)
    return compare_args(root)


class Phase1QueryGateTest(unittest.TestCase):
    def test_fixed_manifest_normalizes_complete_matrix_and_schedule(self) -> None:
        manifest = Path(__file__).with_name("phase1_query_matrix.json")
        normalized = gate.normalize_manifest(manifest)
        self.assertEqual(
            tuple(query["query_name"] for query in normalized["queries"]),
            gate.EXPECTED_QUERY_NAMES,
        )
        self.assertEqual(len(gate.expected_plan(normalized["queries"])), 204)
        first = gate.expected_plan(normalized["queries"])[:12]
        self.assertEqual(
            [row["query_instrumentation"] for row in first],
            [mode for block in gate.ABBA_SCHEDULE for mode in block],
        )
        by_name = {query["query_name"]: query for query in normalized["queries"]}
        self.assertEqual(
            by_name["virtual_hist_count_rate_sum_range_cache_16m"]["range_scalar_cache_max_bytes"],
            16 * 1024 * 1024,
        )
        self.assertEqual(
            by_name["scalar_rate_sum_instant"]["expression"],
            "sum by (service_name_x55e50a58f9befba7)(rate(container_cpu_usage_seconds_total[15m]))",
        )
        self.assertEqual(
            by_name["scalar_rate_sum_range"]["materialization_expectation"],
            "selective",
        )
        self.assertEqual(
            by_name["virtual_hist_count_rate_sum_range_cache_off"]["materialization_expectation"],
            "full",
        )
        self.assertEqual(
            by_name["native_hist_p95_range"]["materialization_expectation"],
            "full",
        )
        self.assertEqual(
            by_name["native_exp_p95_range"]["materialization_expectation"],
            "full",
        )
        self.assertEqual(
            by_name["broad_raw_count_selector"]["start_ms"], 1782980113585
        )

    def test_fixed_manifest_rejects_even_valid_json_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(__file__).with_name("phase1_query_matrix.json")
            mutated = Path(temporary_directory) / "matrix.json"
            document = json.loads(source.read_text(encoding="utf-8"))
            document["description"] += " changed"
            mutated.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "sealed fixed matrix"):
                gate.normalize_manifest(mutated)

    def test_smoke_report_requires_exact_readback_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            report = root / "readback.md"
            report.write_text(
                "- Storage Layout: schema8\n"
                "| Expected Readback Queries | 38 |\n"
                "| Executed Readback Queries | 38 |\n"
                "| Skipped Readback Queries | 0 |\n"
                "| Isolation Check Skips | 0 |\n"
                "| Checked Queries | 38 |\n"
                "| Mismatches | 0 |\n",
                encoding="utf-8",
            )
            gate.validate_smoke_report("readback", report, root / "out.json")
            report.write_text(report.read_text().replace("| Skipped Readback Queries | 0 |", "| Skipped Readback Queries | 1 |"))
            with self.assertRaisesRegex(gate.GateError, "38 expected"):
                gate.validate_smoke_report("readback", report, root / "other.json")

    def test_complete_result_gate_accepts_fixed_synthetic_abba(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args = prepare_comparison(root)
            gate.compare_results(args)
            result = json.loads(args.output.read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "pass")
            self.assertEqual(result["completed_processes"], 204)
            self.assertEqual(result["completed_query_runs"], 612)
            with args.summary.open(encoding="utf-8") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
            self.assertEqual(len(rows), 612)
            self.assertIn("stage_candidate_selection_ns", rows[0])
            self.assertIn("metadata_runtime_json", rows[0])

    def test_result_gate_rejects_detailed_mode_without_stage_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args = prepare_comparison(root)
            with args.index.open(encoding="utf-8") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
            detailed = next(row for row in rows if row["query_instrumentation"] == "detailed")
            raw_path = Path(detailed["raw_output"])
            document = json.loads(raw_path.read_text(encoding="utf-8"))
            duration = document["runs"][0]["duration_ns"]
            document["runs"][0]["query_stages"] = {
                field: (duration if field == "unclassified_ns" else 0)
                for field in gate.common.QUERY_STAGE_FIELDS
            }
            raw_path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "recorded no stage work"):
                gate.compare_results(args)

    def test_result_gate_rejects_changed_corpus_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args = prepare_comparison(root)
            after = json.loads(args.inventory_after.read_text(encoding="utf-8"))
            after["total_bytes"] += 1
            args.inventory_after.write_text(json.dumps(after), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "changed during"):
                gate.compare_results(args)

    def test_runner_help_and_shell_syntax(self) -> None:
        runner = Path(__file__).with_name("phase1_query_run.sh")
        subprocess.run(["bash", "-n", str(runner)], check=True)
        completed = subprocess.run(
            ["bash", str(runner), "--help"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertIn("off,detailed,detailed,off", completed.stdout)
        self.assertIn("--dry-run", completed.stdout)


if __name__ == "__main__":
    unittest.main()
