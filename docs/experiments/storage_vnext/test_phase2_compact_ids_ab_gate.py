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

import phase2_compact_ids_ab_gate as gate
import test_phase1_query_gate as phase1_fixture


HERE = Path(__file__).resolve().parent


def owned_storage() -> dict[str, int]:
    value = {field: 0 for field in gate.LABEL_STORAGE_FIELDS}
    value["label_sets"] = 2
    return value


def compact_storage() -> dict[str, int]:
    value = {field: 0 for field in gate.LABEL_STORAGE_FIELDS}
    value.update(
        {
            "label_sets": 2,
            "compact_label_sets": 2,
            "compact_pairs": 4,
            "compact_source_symbol_translations": 8,
            "compact_source_symbol_translation_hits": 6,
            "compact_source_symbol_translation_misses": 2,
            "compact_atom_lookups": 4,
            "compact_atom_hits": 2,
            "compact_atom_misses": 2,
            "compact_unique_strings": 4,
            "compact_unique_content_bytes": 24,
            "compact_arena_budget_bytes": gate.DEFAULT_ARENA_BYTES,
            "compact_arena_current_bytes": 200,
            "compact_arena_peak_bytes": 240,
            "compact_atom_bytes": 80,
            "compact_pair_bytes": 32,
            "compact_hash_directory_bytes": 48,
            "compact_translation_bytes": 40,
            "compact_retained_bytes": 200,
        }
    )
    return value


def synthetic_raw(
    corpus: Path,
    query: dict[str, object],
    policy: str,
    duration_ns: int,
) -> dict[str, object]:
    empty = query["category"] == "empty-result-control"
    stats = {field: 0 for field in gate.common.QUERY_STATS_FIELDS}
    if not empty:
        stats.update(
            {
                "segments_considered": 1,
                "segments_queried": 1,
                "matched_series": 2,
                "projected_series": 2,
                "chunk_reads": 1,
                "bytes_read": 8,
                "samples_decoded": 2,
                "index_postings_reads": 1,
                "index_postings_bytes_read": 4,
            }
        )
        if query["category"] in gate.TYPED_FULL_CATEGORIES:
            stats["typed_full_chunks_decoded"] = 1
    labels = {field: 0 for field in gate.LABEL_FIELDS}
    if not empty:
        labels.update(
            {
                "rows_integrity_checked": 2,
                "pairs_integrity_checked": 8,
                "pairs_materialized": 8,
                "content_bytes_materialized": 32,
            }
        )
        if query["category"] in gate.SELECTIVE_CATEGORIES:
            labels.update(
                {
                    "rows_selectively_materialized": 2,
                    "pairs_materialized": 4,
                    "pairs_omitted": 4,
                }
            )
        else:
            labels["rows_full_materialized"] = 2
    fingerprint = hashlib.sha256(str(query["query_name"]).encode()).hexdigest()
    storage = (
        owned_storage()
        if policy == "owned-strings"
        else compact_storage()
    )
    if empty:
        storage = {field: 0 for field in gate.LABEL_STORAGE_FIELDS}
    runs = []
    for run_index in range(3):
        run_duration = duration_ns + run_index
        stages = {field: 0 for field in gate.common.QUERY_STAGE_FIELDS}
        stages["unclassified_ns"] = run_duration
        runs.append(
            {
                "query": query["expression"],
                "run_kind": "cold" if run_index == 0 else "warm",
                "run_index": run_index,
                "duration_ns": run_duration,
                "post_query_fingerprint_ns": 1,
                "effective_start_ms": query["start_ms"],
                "effective_end_ms": query["end_ms"],
                "step_ms": query["step_ms"],
                "semantic_fingerprint_sha256": fingerprint,
                "portable_semantic_fingerprint_sha256": fingerprint,
                "result_series": 0 if empty else 2,
                "result_samples": 0 if empty else 2,
                "stats": stats,
                "payload_reads": {
                    "logical_used_bytes": stats["bytes_read"],
                    "physical_reads": 0 if empty else 1,
                    "physical_bytes": stats["bytes_read"],
                },
                "symbol_reads": phase1_fixture.symbol_reads(),
                "label_materialization": labels,
                "query_label_storage": storage,
                "query_stages": stages,
                "metadata_runtime": phase1_fixture.metadata_runtime(),
                "range_scalar_cache": phase1_fixture.range_cache(query),
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
            "chunk_read_mode": "pread",
            "chunk_read_queue_depth": 128,
            "experimental_cross_segment_chunk_reads": False,
            "label_materialization": "demand-driven",
            "query_label_storage": policy,
            "query_instrumentation": "off",
            "query_label_arena_max_bytes": gate.DEFAULT_ARENA_BYTES,
            "storage_layout": "schema8",
            "benchmark_repeats": 3,
            "queries": [query["expression"]],
            "prewarm_query_contexts": False,
            "prefetch_query_data": False,
            "exponential_histogram_bucket_boundaries": query["boundaries"],
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


def prepare_synthetic_comparison(root: Path) -> argparse.Namespace:
    manifest = HERE / "phase2_compact_ids_queries.json"
    normalized_tsv = root / "queries.tsv"
    normalized_json = root / "queries.json"
    gate.normalize_manifest(manifest, normalized_tsv, normalized_json)
    queries = gate.read_manifest(normalized_json)
    query_by_name = {query["query_name"]: query for query in queries}

    corpus = root / "corpus"
    corpus.mkdir()
    binary = root / "chronoxide-query"
    binary.write_bytes(b"same release binary fixture")
    binary_sha = gate.file_sha256(binary)
    inventory = {
        "schema": gate.common.INVENTORY_SCHEMA,
        "corpus": str(corpus.resolve()),
        "corpus_sha256": "b" * 64,
        "file_count": 1,
        "total_bytes": 1,
        "files": [{"path": "segment", "size_bytes": 1, "sha256": "c" * 64}],
    }
    for name in ("before.json", "after.json"):
        (root / name).write_text(json.dumps(inventory), encoding="utf-8")
    for name, kind in (("footer.json", "footer"), ("readback.json", "readback")):
        (root / name).write_text(
            json.dumps(
                {"schema": gate.SMOKE_SCHEMA, "kind": kind, "gate": "pass"}
            ),
            encoding="utf-8",
        )

    index_rows = []
    residency_rows = []
    for planned in gate.expected_plan(queries, 1):
        policy = planned["query_label_storage"]
        raw_path = root / f"{planned['process_label']}.json"
        raw_path.write_text(
            json.dumps(
                synthetic_raw(
                    corpus,
                    query_by_name[planned["query_name"]],
                    policy,
                    90 if policy == "compact-ids" else 100,
                )
            ),
            encoding="utf-8",
        )
        index_rows.append(
            {
                **planned,
                "binary_sha256": binary_sha,
                "corpus": str(corpus.resolve()),
                "raw_output": str(raw_path),
                "process_wall_seconds": "1.0",
                "process_user_seconds": "0.5",
                "process_system_seconds": "0.1",
                "max_rss_kib": 90 if policy == "compact-ids" else 100,
            }
        )
        for phase, resident in (("after-evict", 0), ("after-run", 1)):
            residency_rows.append(
                {
                    "process_label": planned["process_label"],
                    "block": planned["block"],
                    "query_label_storage": policy,
                    "phase": phase,
                    "file_count": 1,
                    "resident_bytes": resident,
                    "corpus_file_bytes": 1,
                }
            )
    with (root / "index.tsv").open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=index_rows[0], delimiter="\t")
        writer.writeheader()
        writer.writerows(index_rows)
    with (root / "residency.tsv").open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=residency_rows[0], delimiter="\t")
        writer.writeheader()
        writer.writerows(residency_rows)
    return argparse.Namespace(
        index=root / "index.tsv",
        manifest=normalized_json,
        inventory_before=root / "before.json",
        inventory_after=root / "after.json",
        residency=root / "residency.tsv",
        footer_validation=root / "footer.json",
        readback_validation=root / "readback.json",
        summary=root / "summary.tsv",
        output=root / "result.json",
        binary=binary,
        corpus=corpus,
        broad_query_name="broad_raw_count_selector",
        blocks=1,
        benchmark_repeats=3,
        arena_bytes=gate.DEFAULT_ARENA_BYTES,
        queue_depth=128,
        max_resident_bytes_after_evict=0,
        max_matched_series=100,
        max_projected_series=200,
        max_chunk_reads=300,
        max_bytes_read=400,
        max_samples_decoded=500,
        max_regex_values_examined=600,
        broad_min_improvement_pct=0.0,
        broad_min_rss_improvement_pct=0.0,
        control_max_regression_pct=0.0,
        control_min_material_regression_ns=0,
        rss_max_regression_pct=0.0,
        rss_min_material_regression_kib=0,
    )


class Phase2CompactIdsGateTests(unittest.TestCase):
    def test_control_regressions_must_cross_relative_and_absolute_floors(self) -> None:
        self.assertTrue(gate.material_regression_passes(104.0, 100.0, 3.0, 5))
        self.assertTrue(gate.material_regression_passes(102.0, 100.0, 3.0, 0))
        self.assertFalse(gate.material_regression_passes(106.0, 100.0, 3.0, 5))

    def test_plan_counterbalances_abba_and_baab_positions(self) -> None:
        query = {
            "query_name": "q",
            "category": "broad-full-label-output",
            "mode": "instant",
        }
        plan = gate.expected_plan([query], 4)
        self.assertEqual(len(plan), 16)
        self.assertEqual(
            [row["query_label_storage"] for row in plan[:4]], list(gate.ABBA)
        )
        self.assertEqual(
            [row["query_label_storage"] for row in plan[4:8]], list(gate.BAAB)
        )
        for policy in gate.POLICIES:
            self.assertEqual(
                sum(row["query_label_storage"] == policy for row in plan), 8
            )

    def test_compact_accounting_reconciles_and_rejects_fallbacks(self) -> None:
        compact = compact_storage()
        self.assertEqual(
            gate.validate_label_storage(
                compact,
                "compact-ids",
                gate.DEFAULT_ARENA_BYTES,
                True,
                True,
                "fixture",
            ),
            compact,
        )
        projected = compact_storage()
        projected["compact_label_sets"] += 1
        self.assertEqual(
            gate.validate_label_storage(
                projected,
                "compact-ids",
                gate.DEFAULT_ARENA_BYTES,
                True,
                True,
                "projected fixture",
            ),
            projected,
        )
        self.assertEqual(
            gate.validate_label_storage(
                owned_storage(),
                "owned-strings",
                gate.DEFAULT_ARENA_BYTES,
                True,
                True,
                "fixture",
            ),
            owned_storage(),
        )

        broken = compact_storage()
        broken["compact_translation_bytes"] += 1
        with self.assertRaisesRegex(gate.GateError, "do not reconcile"):
            gate.validate_label_storage(
                broken, "compact-ids", gate.DEFAULT_ARENA_BYTES, True, True, "fixture"
            )
        for field, message in (
            ("compact_arena_admission_refusals", "refused"),
            ("compact_compatibility_materializations", "String materialization"),
        ):
            broken = compact_storage()
            broken[field] = 1
            with self.assertRaisesRegex(gate.GateError, message):
                gate.validate_label_storage(
                    broken,
                    "compact-ids",
                    gate.DEFAULT_ARENA_BYTES,
                    True,
                    True,
                    "fixture",
                )

        warm = compact_storage()
        warm["label_sets"] = 0
        warm["compact_label_sets"] = 0
        warm["compact_pairs"] = 0
        warm["compact_source_symbol_translations"] = 0
        warm["compact_source_symbol_translation_hits"] = 0
        warm["compact_source_symbol_translation_misses"] = 0
        warm["compact_atom_lookups"] = 0
        warm["compact_atom_hits"] = 0
        warm["compact_atom_misses"] = 0
        warm["compact_unique_strings"] = 0
        warm["compact_unique_content_bytes"] = 0
        self.assertEqual(
            gate.validate_label_storage(
                warm,
                "compact-ids",
                gate.DEFAULT_ARENA_BYTES,
                True,
                False,
                "warm fixture",
            ),
            warm,
        )

    def test_only_documented_symbol_and_metadata_paths_are_exempt(self) -> None:
        symbols = {
            "logical_returned_delta": {"calls": 2, "bytes": 20},
            "page_cache_hits_delta": 2,
            "page_validation_ns_delta": 10,
            "page_read_delta": {"calls": 1, "bytes": 10},
        }
        changed = copy.deepcopy(symbols)
        changed["logical_returned_delta"] = {"calls": 1, "bytes": 5}
        changed["page_cache_hits_delta"] = 1
        changed["page_validation_ns_delta"] = 999
        self.assertEqual(gate.comparable_symbols(symbols), gate.comparable_symbols(changed))
        changed["page_read_delta"]["bytes"] += 1
        self.assertNotEqual(gate.comparable_symbols(symbols), gate.comparable_symbols(changed))

        metadata = {"counters_delta": {"cache": {"hits": 4, "misses": 1}}}
        changed_metadata = copy.deepcopy(metadata)
        changed_metadata["counters_delta"]["cache"]["hits"] = 100
        self.assertEqual(
            gate.comparable_metadata(metadata), gate.comparable_metadata(changed_metadata)
        )
        changed_metadata["counters_delta"]["cache"]["misses"] += 1
        self.assertNotEqual(
            gate.comparable_metadata(metadata), gate.comparable_metadata(changed_metadata)
        )

    def test_manifest_and_runner_pin_the_phase2_contract(self) -> None:
        manifest = HERE / "phase2_compact_ids_queries.json"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output_tsv = root / "queries.tsv"
            output_json = root / "queries.json"
            gate.normalize_manifest(manifest, output_tsv, output_json)
            queries = gate.read_manifest(output_json)
        self.assertTrue(gate.REQUIRED_CATEGORIES.issubset({q["category"] for q in queries}))
        self.assertTrue(
            all(
                query["mode"] == "instant"
                or query["range_scalar_cache_max_bytes"] == 0
                for query in queries
            )
        )

        runner = (HERE / "phase2_compact_ids_ab_run.sh").read_text(encoding="utf-8")
        for fixed_contract in (
            "BLOCKS=\"${BLOCKS:-4}\"",
            "BENCHMARK_REPEATS=3",
            "QUERY_LABEL_ARENA_MAX_BYTES=536870912",
            "--label-materialization demand-driven",
            "--query-label-arena-max-bytes \"$QUERY_LABEL_ARENA_MAX_BYTES\"",
            "--query-instrumentation off",
            "--range-scalar-cache-max-bytes 0",
        ):
            self.assertIn(fixed_contract, runner)

    def test_readback_validation_is_generic_but_requires_complete_execution(self) -> None:
        report = """- Storage Layout: schema8
| Expected Readback Queries | 7 |
| Executed Readback Queries | 7 |
| Skipped Readback Queries | 0 |
| Isolation Check Skips | 0 |
| Checked Queries | 7 |
| Mismatches | 0 |
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            markdown = root / "readback.md"
            output = root / "readback.json"
            markdown.write_text(report, encoding="utf-8")
            gate.validate_smoke_report("readback", markdown, output)
            self.assertTrue(output.is_file())

            broken = root / "broken.md"
            broken.write_text(
                report.replace("| Skipped Readback Queries | 0 |", "| Skipped Readback Queries | 1 |"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "execute/check every"):
                gate.validate_smoke_report("readback", broken, root / "broken.json")

    def test_complete_gate_accepts_same_v11_binary_synthetic_abba(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = prepare_synthetic_comparison(root)
            gate.compare_results(args)
            result = json.loads(args.output.read_text(encoding="utf-8"))
            self.assertEqual(result["correctness_gate"], "pass")
            self.assertEqual(result["performance_gate"], "pass")
            self.assertEqual(
                result["schedule"],
                {
                    "odd_blocks": list(gate.ABBA),
                    "even_blocks": list(gate.BAAB),
                },
            )
            self.assertEqual(result["query_label_arena_max_bytes"], 512 * 1024 * 1024)
            with args.summary.open(encoding="utf-8") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
            self.assertEqual(len(rows), 11 * 4 * 3)
            self.assertIn("storage_compact_retained_bytes", rows[0])


if __name__ == "__main__":
    unittest.main()
