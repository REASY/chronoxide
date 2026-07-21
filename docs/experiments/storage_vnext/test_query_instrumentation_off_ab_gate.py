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

import query_instrumentation_off_ab_gate as gate


QUERY = 'last_over_time({__name__=~"^http_.*_count$"}[5m])'
REFERENCE_HASH = "a" * 64
CANDIDATE_HASH = "b" * 64


def query_stats() -> dict[str, int]:
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
            "index_postings_bytes_read": 40,
        }
    )
    return result


def labels() -> dict[str, int]:
    return {
        "rows_integrity_checked": 3,
        "pairs_integrity_checked": 12,
        "rows_full_materialized": 0,
        "rows_selectively_materialized": 3,
        "pairs_materialized": 6,
        "pairs_omitted": 6,
        "content_bytes_materialized": 45,
    }


def label_storage() -> dict[str, int]:
    return {
        "label_sets": 3,
        "atom_lookups": 0,
        "atom_hits": 0,
        "atom_misses": 0,
        "unique_content_bytes": 0,
    }


def stages(duration_ns: int) -> dict[str, int]:
    result = {field: 0 for field in gate.common.QUERY_STAGE_FIELDS}
    result["unclassified_ns"] = duration_ns
    return result


def raw_document(
    corpus: Path,
    role: str,
    cold_duration: int,
    warm_duration: int,
) -> dict[str, object]:
    configuration: dict[str, object] = {
        "segments_dir": str(corpus.resolve()),
        "start_ms": 0,
        "end_ms": 1_000,
        "mode": "instant",
        "step_ms": None,
        "range_scalar_cache_max_bytes": None,
        "chunk_read_mode": "pread",
        "chunk_read_queue_depth": 128,
        "experimental_cross_segment_chunk_reads": False,
        "label_materialization": "demand-driven",
        "query_label_storage": "owned-strings",
        "storage_layout": "schema8",
        "benchmark_repeats": 2,
        "queries": [QUERY],
        "prewarm_query_contexts": False,
        "prefetch_query_data": False,
        "exponential_histogram_bucket_boundaries": [],
        "requested_segment_footer_validation": False,
        "effective_segment_footer_validation": False,
    }
    if role == "candidate":
        configuration["query_instrumentation"] = "off"
    runs: list[dict[str, object]] = []
    for run_index, duration in enumerate((cold_duration, warm_duration)):
        run: dict[str, object] = {
            "query": QUERY,
            "run_kind": "cold" if run_index == 0 else "warm",
            "run_index": run_index,
            "duration_ns": duration,
            "effective_start_ms": 0,
            "effective_end_ms": 1_000,
            "step_ms": None,
            "semantic_fingerprint_sha256": "c" * 64,
            "portable_semantic_fingerprint_sha256": "d" * 64,
            "result_series": 3,
            "result_samples": 3,
            "stats": query_stats(),
            "payload_reads": {
                "logical_used_bytes": 80,
                "physical_reads": 2,
                "physical_bytes": 96,
            },
            "symbol_reads": {"logical_returned_delta": {"calls": 3, "bytes": 45}},
            "label_materialization": labels(),
            "query_label_storage": label_storage(),
            "range_scalar_cache": None,
        }
        if role == "candidate":
            run.update(
                {
                    "post_query_fingerprint_ns": 7,
                    "query_stages": stages(duration),
                    "metadata_runtime": {},
                }
            )
        runs.append(run)
    return {
        "schema": (
            gate.REFERENCE_RAW_SCHEMA
            if role == "reference"
            else gate.CANDIDATE_RAW_SCHEMA
        ),
        "corpus_fingerprint_sha256": "e" * 64,
        "corpus_fingerprint_duration_ns": 9,
        "configuration": configuration,
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
        binaries=root / "binaries.tsv",
        sources=root / "sources.tsv",
        corpus=root / "corpus",
        summary=root / "summary.tsv",
        output=root / "comparison.json",
        broad_query_name="broad-selector",
        blocks=1,
        benchmark_repeats=2,
        queue_depth=128,
        label_materialization="demand-driven",
        max_matched_series=100,
        max_projected_series=200,
        max_chunk_reads=300,
        max_bytes_read=400,
        max_samples_decoded=500,
        max_regex_values_examined=600,
        broad_max_regression_pct=3.0,
        general_max_regression_pct=5.0,
        rss_max_regression_pct=5.0,
    )


def write_tsv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=rows[0], delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def prepare_comparison(root: Path) -> tuple[argparse.Namespace, dict[int, Path]]:
    corpus = root / "corpus"
    corpus.mkdir()
    corpus.joinpath("segment").write_bytes(b"segment")
    manifest = {
        "schema": gate.manifest_gate.NORMALIZED_MANIFEST_SCHEMA,
        "queries": [
            {
                "query_name": "broad-selector",
                "category": "broad-regex",
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
        json.dumps(manifest), encoding="utf-8"
    )
    write_tsv(
        root / "binaries.tsv",
        [
            {
                "role": "reference",
                "source_path": "/source/reference",
                "preserved_path": "/frozen/reference",
                "sha256": REFERENCE_HASH,
            },
            {
                "role": "candidate",
                "source_path": "/source/candidate",
                "preserved_path": "/frozen/candidate",
                "sha256": CANDIDATE_HASH,
            },
        ],
    )
    write_tsv(
        root / "sources.tsv",
        [
            {
                "role": role,
                "source_root": f"/source/{role}",
                "head_commit": ("1" if role == "reference" else "2") * 40,
                "head_tree": ("3" if role == "reference" else "4") * 40,
                "source_state_sha256": ("5" if role == "reference" else "6") * 64,
                "tracked_patch_sha256": ("7" if role == "reference" else "8") * 64,
                "status_sha256": ("9" if role == "reference" else "0") * 64,
            }
            for role in gate.ROLES
        ],
    )

    rows: list[dict[str, object]] = []
    raw_paths: dict[int, Path] = {}
    for order_index, role in enumerate(gate.ABBA, 1):
        raw_path = root / f"raw-{order_index}-{role}.json"
        durations = (100, 80) if role == "reference" else (102, 81)
        raw_path.write_text(
            json.dumps(raw_document(corpus, role, *durations)), encoding="utf-8"
        )
        raw_paths[order_index] = raw_path
        rows.append(
            {
                "process_label": f"broad-selector-b01-{order_index:02d}-{role}",
                "query_name": "broad-selector",
                "category": "broad-regex",
                "mode": "instant",
                "block": 1,
                "order_index": order_index,
                "role": role,
                "binary_sha256": (
                    REFERENCE_HASH if role == "reference" else CANDIDATE_HASH
                ),
                "corpus": str(corpus),
                "raw_output": str(raw_path),
                "process_wall_seconds": "0.25",
                "process_user_seconds": "0.20",
                "process_system_seconds": "0.01",
                "max_rss_kib": 1000 if role == "reference" else 1020,
            }
        )
    write_tsv(root / "raw-index.tsv", rows)
    return compare_args(root), raw_paths


class QueryInstrumentationOffAbGateTest(unittest.TestCase):
    def test_passes_strict_correctness_and_regression_gates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args, _ = prepare_comparison(root)
            gate.compare_results(args)
            comparison = json.loads(args.output.read_text(encoding="utf-8"))
            self.assertEqual(comparison["correctness_gate"], "pass")
            self.assertEqual(comparison["performance_gate"], "pass")
            self.assertEqual(comparison["schedule"], list(gate.ABBA))
            self.assertEqual(
                comparison["thresholds"][
                    "broad_query_cold_and_warm_max_regression_pct"
                ],
                3.0,
            )
            with args.summary.open(encoding="utf-8") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
            self.assertEqual(len(rows), 8)
            self.assertIn("stats_index_postings_bytes_read", rows[0])
            self.assertEqual(rows[0]["payload_physical_bytes"], "96")

    def test_rejects_broad_warm_regression_above_three_percent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args, raw_paths = prepare_comparison(root)
            for order_index in (2, 3):
                document = json.loads(raw_paths[order_index].read_text(encoding="utf-8"))
                document["runs"][1]["duration_ns"] = 84
                document["runs"][1]["query_stages"] = stages(84)
                raw_paths[order_index].write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "warm latency ratio"):
                gate.compare_results(args)
            comparison = json.loads(args.output.read_text(encoding="utf-8"))
            self.assertEqual(comparison["performance_gate"], "fail")

    def test_rejects_semantic_fingerprint_difference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args, raw_paths = prepare_comparison(root)
            document = json.loads(raw_paths[2].read_text(encoding="utf-8"))
            document["runs"][0]["semantic_fingerprint_sha256"] = "f" * 64
            raw_paths[2].write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "semantic_fingerprint differs"):
                gate.compare_results(args)

    def test_rejects_public_query_stats_difference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args, raw_paths = prepare_comparison(root)
            document = json.loads(raw_paths[2].read_text(encoding="utf-8"))
            document["runs"][0]["stats"]["matched_series"] += 1
            raw_paths[2].write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "stats differs"):
                gate.compare_results(args)

    def test_rejects_nonzero_candidate_off_stages(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args, raw_paths = prepare_comparison(root)
            document = json.loads(raw_paths[2].read_text(encoding="utf-8"))
            query_stages = document["runs"][0]["query_stages"]
            query_stages["candidate_selection_ns"] = 1
            query_stages["exclusive_total_ns"] = 1
            query_stages["unclassified_ns"] -= 1
            raw_paths[2].write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "off-mode query"):
                gate.compare_results(args)

    def test_rejects_non_abba_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            args, _ = prepare_comparison(root)
            with args.index.open(encoding="utf-8") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
            rows[0]["role"] = "candidate"
            rows[0]["binary_sha256"] = CANDIDATE_HASH
            write_tsv(args.index, rows)
            with self.assertRaisesRegex(gate.GateError, "did not follow"):
                gate.compare_results(args)

    def test_runner_dry_run_freezes_binary_source_manifest_and_abba_plan(self) -> None:
        script = Path(__file__).with_name("query_instrumentation_off_ab_run.sh")
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpus = root / "corpus"
            corpus.mkdir()
            corpus.joinpath("segment").write_bytes(b"segment")
            source_root = root / "source"
            source_root.mkdir()
            subprocess.run(["git", "init", "-q"], cwd=source_root, check=True)
            source_root.joinpath("README").write_text("source\n", encoding="utf-8")
            subprocess.run(["git", "add", "README"], cwd=source_root, check=True)
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.com",
                    "commit",
                    "-qm",
                    "fixture",
                ],
                cwd=source_root,
                check=True,
            )
            archive_source = root / "reference-archive"
            archive_source.mkdir()
            archive_source.joinpath("README").write_text(
                "archived reference\n", encoding="utf-8"
            )
            reference = root / "reference-query"
            reference.write_text(
                "#!/usr/bin/env bash\n"
                "printf '%s\\n' '--storage-layout schema8 --label-materialization "
                "--query-label-storage owned-strings --range-scalar-cache-max-bytes "
                "--raw-output'\n",
                encoding="utf-8",
            )
            reference.chmod(0o755)
            candidate = root / "candidate-query"
            candidate.write_text(
                "#!/usr/bin/env bash\n"
                "printf '%s\\n' '--storage-layout schema8 --label-materialization "
                "--query-label-storage owned-strings --range-scalar-cache-max-bytes "
                "--raw-output --query-instrumentation off detailed candidate'\n",
                encoding="utf-8",
            )
            candidate.chmod(0o755)
            manifest = root / "queries.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema": gate.manifest_gate.MANIFEST_SCHEMA,
                        "queries": [
                            {
                                "name": "broad-selector",
                                "category": "broad-regex",
                                "mode": "instant",
                                "time_ms": 1_000,
                                "chronoxide_query": QUERY,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            result = root / "result"
            environment = os.environ | {
                "CORPUS_DIR": str(corpus),
                "REFERENCE_QUERY_BIN": str(reference),
                "CANDIDATE_QUERY_BIN": str(candidate),
                "REFERENCE_SOURCE_ROOT": str(archive_source),
                "REFERENCE_SOURCE_COMMIT": "1" * 40,
                "REFERENCE_SOURCE_TREE": "2" * 40,
                "CANDIDATE_SOURCE_ROOT": str(source_root),
                "QUERY_MANIFEST": str(manifest),
                "BROAD_QUERY_NAME": "broad-selector",
                "RESULT_DIR": str(result),
                "RUN_NOTE": "dry-run test on a quiet host",
                "BLOCKS": "1",
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
            self.assertEqual([row["role"] for row in plan], list(gate.ABBA))
            with result.joinpath("metadata/binaries.tsv").open(encoding="utf-8") as source:
                binaries = list(csv.DictReader(source, delimiter="\t"))
            self.assertEqual({row["role"] for row in binaries}, set(gate.ROLES))
            self.assertEqual(len({row["sha256"] for row in binaries}), 2)
            with result.joinpath("metadata/sources.tsv").open(encoding="utf-8") as source:
                sources = list(csv.DictReader(source, delimiter="\t"))
            self.assertEqual({row["role"] for row in sources}, set(gate.ROLES))
            self.assertTrue(
                result.joinpath(
                    "metadata/source-reference/archive-source-inventory.json"
                ).is_file()
            )
            settings = result.joinpath("metadata/settings.txt").read_text(encoding="utf-8")
            self.assertIn("schedule=reference,candidate,candidate,reference", settings)
            self.assertIn("candidate_query_instrumentation=off", settings)


if __name__ == "__main__":
    unittest.main()
