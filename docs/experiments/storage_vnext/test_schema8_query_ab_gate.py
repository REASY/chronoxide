#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import schema8_query_ab_gate as gate


QUERY = "sum(rate(metric_count[15m]))"


def query_stats(postings_bytes: int = 40) -> dict[str, int]:
    result = {field: 0 for field in gate.QUERY_STATS_FIELDS}
    result.update(
        {
            "segments_considered": 2,
            "segments_queried": 2,
            "matched_series": 3,
            "projected_series": 3,
            "chunk_reads": 2,
            "bytes_read": 80,
            "samples_decoded": 5,
            "index_postings_reads": 2,
            "index_postings_bytes_read": postings_bytes,
        }
    )
    return result


def label_materialization() -> dict[str, int]:
    return {
        "rows_integrity_checked": 3,
        "pairs_integrity_checked": 12,
        "rows_full_materialized": 3,
        "rows_selectively_materialized": 0,
        "pairs_materialized": 12,
        "pairs_omitted": 0,
        "content_bytes_materialized": 90,
    }


def query_label_storage() -> dict[str, int]:
    return {
        "label_sets": 3,
        "atom_lookups": 0,
        "atom_hits": 0,
        "atom_misses": 0,
        "unique_content_bytes": 0,
    }


def symbol_reads() -> dict[str, object]:
    return {"logical_returned_delta": {"calls": 3, "bytes": 90}}


def query_stages(duration_ns: int) -> dict[str, int]:
    stages = {field: 0 for field in gate.common.QUERY_STAGE_FIELDS}
    stages["unclassified_ns"] = duration_ns
    return stages


def raw_document(corpus: Path, layout: str, postings_bytes: int) -> dict[str, object]:
    runs = []
    for run_index in range(2):
        duration_ns = 100 + run_index
        runs.append(
            {
                "query": QUERY,
                "run_kind": "cold" if run_index == 0 else "warm",
                "run_index": run_index,
                "duration_ns": duration_ns,
                "post_query_fingerprint_ns": 7,
                "effective_start_ms": 0,
                "effective_end_ms": 1_000,
                "step_ms": None,
                "semantic_fingerprint_sha256": "a" * 64,
                "portable_semantic_fingerprint_sha256": "b" * 64,
                "result_series": 3,
                "result_samples": 3,
                "stats": query_stats(postings_bytes),
                "payload_reads": {
                    "logical_used_bytes": 80,
                    "physical_reads": 2,
                    "physical_bytes": 96,
                },
                "symbol_reads": symbol_reads(),
                "label_materialization": label_materialization(),
                "query_label_storage": query_label_storage(),
                "query_stages": query_stages(duration_ns),
                "metadata_runtime": {},
                "range_scalar_cache": None,
            }
        )
    return {
        "schema": gate.RAW_SCHEMA,
        "corpus_fingerprint_sha256": ("c" if layout == "schema7" else "d") * 64,
        "corpus_fingerprint_duration_ns": 9,
        "configuration": {
            "segments_dir": str(corpus.resolve()),
            "start_ms": 0,
            "end_ms": 1_000,
            "mode": "instant",
            "step_ms": None,
            "range_scalar_cache_max_bytes": None,
            "chunk_read_mode": "pread",
            "chunk_read_queue_depth": 128,
            "experimental_cross_segment_chunk_reads": False,
            "label_materialization": "full",
            "query_label_storage": "owned-strings",
            "query_instrumentation": "off",
            "storage_layout": layout,
            "benchmark_repeats": 2,
            "queries": [QUERY],
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
        summary=root / "summary.tsv",
        output=root / "equivalence.json",
        repeats=1,
        benchmark_repeats=2,
        queue_depth=128,
        label_materialization="full",
        max_matched_series=100,
        max_projected_series=200,
        max_chunk_reads=300,
        max_bytes_read=400,
        max_samples_decoded=500,
        max_regex_values_examined=600,
    )


def prepare_comparison(root: Path) -> tuple[argparse.Namespace, dict[str, Path]]:
    corpora = {layout: root / layout for layout in gate.LAYOUTS}
    for corpus in corpora.values():
        corpus.mkdir()
        corpus.joinpath("segment").write_bytes(b"segment")
    normalized = {
        "schema": gate.NORMALIZED_MANIFEST_SCHEMA,
        "queries": [
            {
                "query_name": "scalar-count",
                "category": "scalar-projection",
                "mode": "instant",
                "start_ms": 0,
                "end_ms": 1_000,
                "step_ms": None,
                "range_scalar_cache_max_bytes": None,
                "boundaries": [],
                "expression": QUERY,
            }
        ],
    }
    root.joinpath("queries.normalized.json").write_text(
        json.dumps(normalized), encoding="utf-8"
    )
    raw_paths: dict[str, Path] = {}
    rows = []
    for order_index, layout in enumerate(gate.LAYOUTS, 1):
        raw = root / f"{layout}.json"
        raw.write_text(
            json.dumps(raw_document(corpora[layout], layout, 40 if layout == "schema7" else 12)),
            encoding="utf-8",
        )
        raw_paths[layout] = raw
        rows.append(
            {
                "process_label": f"scalar-count-r01-0{order_index}-{layout}",
                "query_name": "scalar-count",
                "category": "scalar-projection",
                "mode": "instant",
                "repetition": "1",
                "order_index": str(order_index),
                "storage_layout": layout,
                "corpus": str(corpora[layout]),
                "raw_output": str(raw),
                "max_rss_kib": "123",
            }
        )
    with root.joinpath("raw-index.tsv").open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=rows[0], delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    return compare_args(root), raw_paths


class Schema8QueryAbGateTest(unittest.TestCase):
    def test_manifest_normalizes_mixed_instant_and_range_queries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            manifest = root / "queries.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema": gate.MANIFEST_SCHEMA,
                        "queries": [
                            {
                                "name": "instant-control",
                                "category": "metric-range-control",
                                "mode": "instant",
                                "time_ms": 1_000,
                                "chronoxide_query": "metric",
                            },
                            {
                                "name": "native-range",
                                "category": "native-histogram",
                                "mode": "range",
                                "start_ms": 100,
                                "end_ms": 1_000,
                                "step_ms": 100,
                                "chronoxide_query": QUERY,
                                "exponential_histogram_bucket_boundaries": [0.1, 1],
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            queries = gate.normalize_manifest(manifest, 0)
            self.assertEqual(queries[0]["range_scalar_cache_max_bytes"], None)
            self.assertEqual(queries[1]["range_scalar_cache_max_bytes"], 0)
            self.assertEqual(queries[1]["boundaries"], [0.1, 1.0])
            gate.write_normalized_manifest(queries, root / "queries.tsv", root / "queries.out.json")
            with root.joinpath("queries.tsv").open(encoding="utf-8") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
            self.assertEqual(rows[0]["step_ms"], "-")
            self.assertEqual(rows[1]["range_scalar_cache_max_bytes"], "0")

    def test_manifest_rejects_duplicate_query_names(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "queries.json"
            path.write_text(
                json.dumps(
                    [
                        {"name": "same", "mode": "instant", "time_ms": 1, "chronoxide_query": "a"},
                        {"name": "same", "mode": "instant", "time_ms": 1, "chronoxide_query": "b"},
                    ]
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.GateError, "duplicate query"):
                gate.normalize_manifest(path, 0)

    def test_compare_allows_only_encoded_postings_byte_difference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args, _ = prepare_comparison(root)
            gate.compare_results(args)
            result = json.loads(args.output.read_text(encoding="utf-8"))
            self.assertEqual(result["canonical_equivalence"], "pass")
            self.assertEqual(
                result["allowed_query_stats_differences"], ["index_postings_bytes_read"]
            )
            self.assertEqual(result["matching_runs_compared"], 2)

    def test_compare_rejects_postings_read_count_difference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args, raw_paths = prepare_comparison(root)
            document = json.loads(raw_paths["schema8"].read_text(encoding="utf-8"))
            document["runs"][0]["stats"]["index_postings_reads"] += 1
            raw_paths["schema8"].write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "index_postings_reads"):
                gate.compare_results(args)

    def test_compare_rejects_timed_footer_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args, raw_paths = prepare_comparison(root)
            document = json.loads(raw_paths["schema8"].read_text(encoding="utf-8"))
            document["configuration"]["requested_segment_footer_validation"] = True
            document["configuration"]["effective_segment_footer_validation"] = True
            raw_paths["schema8"].write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "timed configuration"):
                gate.compare_results(args)

    def test_runner_dry_run_builds_alternating_plan_with_one_binary(self) -> None:
        script = Path(__file__).with_name("schema8_query_ab_run.sh")
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            schema7 = root / "schema7"
            schema8 = root / "schema8"
            schema7.mkdir()
            schema8.mkdir()
            schema7.joinpath("file").write_bytes(b"seven")
            schema8.joinpath("file").write_bytes(b"eight")
            query_binary = root / "chronoxide-query"
            query_binary.write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' '--storage-layout --label-materialization --query-label-storage --range-scalar-cache-max-bytes owned-strings schema7 schema8'\n",
                encoding="utf-8",
            )
            query_binary.chmod(0o755)
            manifest = root / "queries.json"
            manifest.write_text(
                json.dumps(
                    [
                        {
                            "name": "control",
                            "mode": "instant",
                            "time_ms": 1_000,
                            "chronoxide_query": "metric",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            result = root / "result"
            environment = os.environ | {
                "SCHEMA7_DIR": str(schema7),
                "SCHEMA8_DIR": str(schema8),
                "QUERY_BIN": str(query_binary),
                "QUERY_MANIFEST": str(manifest),
                "RESULT_DIR": str(result),
                "RUN_NOTE": "dry-run test on a quiet host",
                "REPEATS": "2",
            }
            subprocess.run(
                ["bash", str(script), "--dry-run"],
                check=True,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertTrue(result.joinpath("DRY_RUN_COMPLETE").is_file())
            with result.joinpath("run-plan.tsv").open(encoding="utf-8") as source:
                plan = list(csv.DictReader(source, delimiter="\t"))
            self.assertEqual(
                [row["storage_layout"] for row in plan],
                ["schema7", "schema8", "schema8", "schema7"],
            )
            binary_hash_lines = result.joinpath("metadata/query-binary.sha256").read_text()
            self.assertEqual(len(binary_hash_lines.splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
