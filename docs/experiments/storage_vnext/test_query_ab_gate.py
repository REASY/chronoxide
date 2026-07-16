#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import stat
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path

import query_ab_gate


QUERY = "sum(rate(metric_count[15m]))"


def write_corpus(root: Path, symbols_version: int, changed_chunks: bool = False) -> None:
    segment = root / "seg-a"
    segment.mkdir(parents=True)
    segment.joinpath("symbols.bin").write_bytes(
        struct.pack("<IHH", query_ab_gate.SYMBOLS_MAGIC, symbols_version, 0)
        + b"symbol-layout"
    )
    segment.joinpath("footer.bin").write_bytes(f"footer-v{symbols_version}".encode())
    segment.joinpath("chunks.bin").write_bytes(b"changed" if changed_chunks else b"same")


def stats() -> dict[str, int]:
    values = {field: 0 for field in query_ab_gate.QUERY_STATS_FIELDS}
    values.update(
        {
            "segments_considered": 1,
            "segments_queried": 1,
            "matched_series": 2,
            "projected_series": 2,
            "chunk_reads": 1,
            "bytes_read": 50,
            "samples_decoded": 3,
            "typed_scalar_chunks_decoded": 1,
        }
    )
    return values


def symbols(format_name: str, cold: bool) -> dict[str, object]:
    value: dict[str, object] = {
        field: {"calls": 0, "bytes": 0} for field in query_ab_gate.READ_COUNT_FIELDS
    }
    value.update({field: 0 for field in query_ab_gate.SYMBOL_COUNTER_FIELDS})
    value.update({field: 0 for field in query_ab_gate.SYMBOL_RESOURCE_FIELDS})
    value["logical_returned_delta"] = (
        {"calls": 2, "bytes": 10} if cold else {"calls": 0, "bytes": 0}
    )
    value["retained_readers_after_run"] = 1
    value["source_file_bytes_after_run"] = 100
    if format_name == "v7":
        if cold:
            value["legacy_eager_read_delta"] = {"calls": 1, "bytes": 100}
        value["eager_dictionary_retained_charge_bytes_after_run"] = 80
        value["total_retained_charge_bytes_after_run"] = 80
    else:
        if cold:
            value["root_read_delta"] = {"calls": 1, "bytes": 20}
            value["page_read_delta"] = {"calls": 1, "bytes": 30}
            value["page_validation_delta"] = {"calls": 1, "bytes": 30}
            value["page_validation_ns_delta"] = 5
            value["page_cache_misses_delta"] = 1
        else:
            value["page_cache_hits_delta"] = 2
        value["retained_open_files_after_run"] = 1
        value["root_encoded_bytes_after_run"] = 20
        value["root_retained_charge_bytes_after_run"] = 25
        value["page_cache_charge_bytes_after_run"] = 35
        value["page_cache_max_bytes_after_run"] = 262_144
        value["total_retained_charge_bytes_after_run"] = 60
    return value


def raw_document(corpus: Path, format_name: str) -> dict[str, object]:
    runs = []
    for index, kind in enumerate(("cold", "warm")):
        runs.append(
            {
                "query": QUERY,
                "run_kind": kind,
                "run_index": index,
                "duration_ns": 100 + index,
                "effective_start_ms": 0,
                "effective_end_ms": 1_000,
                "step_ms": None,
                "semantic_fingerprint_sha256": "a" * 64,
                "portable_semantic_fingerprint_sha256": "b" * 64,
                "result_series": 2,
                "result_samples": 2,
                "stats": stats(),
                "payload_reads": {
                    "logical_used_bytes": 50,
                    "physical_reads": 1,
                    "physical_bytes": 64 if index == 0 else 56,
                },
                "symbol_reads": symbols(format_name, index == 0),
                "range_scalar_cache": None,
            }
        )
    return {
        "schema": query_ab_gate.RAW_SCHEMA,
        "corpus_fingerprint_sha256": ("c" if format_name == "v7" else "d") * 64,
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
            "experimental_storage_layout_ab": format_name == "v7",
            "benchmark_repeats": 2,
            "queries": [QUERY],
            "prewarm_query_contexts": False,
            "prefetch_query_data": False,
            "exponential_histogram_bucket_boundaries": [],
            "validate_segment_footers": False,
        },
        "limits": query_ab_gate.EXPECTED_LIMITS,
        "runs": runs,
    }


def query_range_document(
    corpus: Path, format_name: str, cache_budget: int
) -> dict[str, object]:
    document = raw_document(corpus, format_name)
    document["configuration"]["mode"] = "query_range"
    document["configuration"]["step_ms"] = 1_000
    document["configuration"]["range_scalar_cache_max_bytes"] = cache_budget
    for index, run in enumerate(document["runs"]):
        run["step_ms"] = 1_000
        cache: dict[str, object] = {
            field: False for field in query_ab_gate.RANGE_CACHE_BOOL_FIELDS
        }
        cache.update({field: 0 for field in query_ab_gate.RANGE_CACHE_COUNT_FIELDS})
        cache["configured_budget_bytes"] = cache_budget
        cache["governor_lease_bytes"] = 128
        cache["peak_retained_charge_bytes"] = 128
        cache["process_governor_limit_bytes"] = cache_budget
        cache["process_governor_peak_leased_bytes"] = 128
        if index == 0:
            cache["misses"] = 2
            cache["admitted_entries"] = 2
            cache["logical_miss_or_bypass_bytes"] = 50
        else:
            cache["hits"] = 2
            cache["logical_hit_bytes"] = 50
        run["range_scalar_cache"] = cache
    return document


def parse_args(
    raw: Path,
    output: Path,
    corpus: Path,
    format_name: str,
    repetition: int,
    order_index: int,
    step_ms: int | None = None,
    range_scalar_cache_max_bytes: int = 0,
) -> argparse.Namespace:
    return argparse.Namespace(
        raw=raw,
        output=output,
        process_label=f"r{repetition}-{format_name}",
        format=format_name,
        repetition=repetition,
        order_index=order_index,
        query_name="scalar_count",
        query=QUERY,
        corpus=corpus,
        max_rss_kib=123,
        start_ms=0,
        end_ms=1_000,
        step_ms=step_ms,
        range_scalar_cache_max_bytes=range_scalar_cache_max_bytes,
        queue_depth=128,
    )


class QueryAbGateTest(unittest.TestCase):
    def test_fadvise_helper_rejects_symlinks_and_fifos_without_blocking(self) -> None:
        script_dir = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            helper = root / "fadvise-regular-dontneed"
            subprocess.run(
                [
                    "cc",
                    "-O2",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-o",
                    str(helper),
                    str(script_dir / "fadvise_regular_dontneed.c"),
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            regular = root / "regular"
            regular.write_bytes(b"data")
            subprocess.run([str(helper), str(regular)], check=True, timeout=2)
            link = root / "link"
            link.symlink_to(regular.name)
            fifo = root / "fifo"
            os.mkfifo(fifo)
            rejected = subprocess.run(
                [str(helper), str(link), str(fifo)],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=2,
            )
        self.assertNotEqual(rejected.returncode, 0)

    def test_inventory_and_comparison_allow_only_symbols_and_footer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            v7 = root / "v7"
            vnext = root / "vnext"
            write_corpus(v7, 2)
            write_corpus(vnext, 3)
            v7_json = root / "v7.json"
            vnext_json = root / "vnext.json"
            query_ab_gate.write_inventory(v7, v7_json, root / "v7.nul")
            query_ab_gate.write_inventory(vnext, vnext_json, root / "vnext.nul")
            comparison = root / "comparison.json"
            query_ab_gate.compare_corpora(v7_json, vnext_json, comparison)
            document = json.loads(comparison.read_text(encoding="utf-8"))
            artifacts = {entry["artifact"] for entry in document["allowed_differences"]}
        self.assertEqual(artifacts, {"symbols.bin", "footer.bin"})

    def test_inventory_rejects_links_and_non_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            root.joinpath("regular").write_bytes(b"data")
            root.joinpath("link").symlink_to("regular")
            with self.assertRaisesRegex(query_ab_gate.GateError, "symbolic links"):
                query_ab_gate.inventory_corpus(root)
            root.joinpath("link").unlink()
            os.mkfifo(root / "fifo")
            with self.assertRaisesRegex(query_ab_gate.GateError, "non-file"):
                query_ab_gate.inventory_corpus(root)

    def test_comparison_rejects_changed_non_format_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            v7 = root / "v7"
            vnext = root / "vnext"
            write_corpus(v7, 2)
            write_corpus(vnext, 3, changed_chunks=True)
            v7_json = root / "v7.json"
            vnext_json = root / "vnext.json"
            query_ab_gate.write_inventory(v7, v7_json, root / "v7.nul")
            query_ab_gate.write_inventory(vnext, vnext_json, root / "vnext.nul")
            with self.assertRaisesRegex(query_ab_gate.GateError, "chunks.bin"):
                query_ab_gate.compare_corpora(v7_json, vnext_json, root / "comparison.json")

    def test_raw_and_cross_format_equivalence_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpora = {"v7": root / "v7", "vnext": root / "vnext"}
            for corpus in corpora.values():
                corpus.mkdir()
            parsed_paths = []
            for repetition in (1, 2):
                order = ("v7", "vnext") if repetition == 1 else ("vnext", "v7")
                for order_index, format_name in enumerate(order, start=1):
                    raw = root / f"raw-{repetition}-{format_name}.json"
                    raw.write_text(
                        json.dumps(raw_document(corpora[format_name], format_name)),
                        encoding="utf-8",
                    )
                    parsed = root / f"parsed-{repetition}-{format_name}.json"
                    query_ab_gate.parse_raw_result(
                        parse_args(
                            raw,
                            parsed,
                            corpora[format_name],
                            format_name,
                            repetition,
                            order_index,
                        )
                    )
                    parsed_paths.append(parsed)
            summary = root / "summary.tsv"
            comparison = root / "comparison.json"
            query_ab_gate.compare_results(
                parsed_paths, 2, ["scalar_count"], summary, comparison
            )
            rows = summary.read_text(encoding="utf-8").splitlines()
            result = json.loads(comparison.read_text(encoding="utf-8"))
            original = parsed_paths[-1].read_text(encoding="utf-8")
            changed = json.loads(parsed_paths[-1].read_text(encoding="utf-8"))
            changed["runs"][0]["symbol_reads"]["logical_returned_delta"]["calls"] += 1
            parsed_paths[-1].write_text(json.dumps(changed), encoding="utf-8")
            with self.assertRaisesRegex(
                query_ab_gate.GateError, "logical symbol work"
            ):
                query_ab_gate.compare_results(
                    parsed_paths,
                    2,
                    ["scalar_count"],
                    root / "changed-summary.tsv",
                    root / "changed-comparison.json",
                )
            parsed_paths[-1].write_text(original, encoding="utf-8")
            changed = json.loads(original)
            changed["runs"][1]["payload_reads"]["physical_bytes"] += 1
            parsed_paths[-1].write_text(json.dumps(changed), encoding="utf-8")
            with self.assertRaisesRegex(query_ab_gate.GateError, "payload reads"):
                query_ab_gate.compare_results(
                    parsed_paths,
                    2,
                    ["scalar_count"],
                    root / "payload-changed-summary.tsv",
                    root / "payload-changed-comparison.json",
                )
        self.assertEqual(len(rows), 1 + 8)
        self.assertEqual(result["canonical_equivalence"], "pass")
        accounting = result["queries"]["scalar_count"]["run_kind_accounting"]
        self.assertEqual(
            accounting["cold"]["logical_symbol_work"],
            {"calls": 2, "bytes": 10},
        )
        self.assertEqual(
            accounting["warm"]["logical_symbol_work"], {"calls": 0, "bytes": 0}
        )
        self.assertEqual(accounting["cold"]["payload_reads"]["physical_bytes"], 64)
        self.assertEqual(accounting["warm"]["payload_reads"]["physical_bytes"], 56)

    def test_range_accounting_compares_within_run_kind(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            cache_budget = 1_024
            corpora = {"v7": root / "v7", "vnext": root / "vnext"}
            for corpus in corpora.values():
                corpus.mkdir()
            parsed_paths = []
            for repetition in (1, 2):
                order = ("v7", "vnext") if repetition == 1 else ("vnext", "v7")
                for order_index, format_name in enumerate(order, start=1):
                    raw = root / f"range-{repetition}-{format_name}.json"
                    raw.write_text(
                        json.dumps(
                            query_range_document(
                                corpora[format_name], format_name, cache_budget
                            )
                        ),
                        encoding="utf-8",
                    )
                    parsed = root / f"range-parsed-{repetition}-{format_name}.json"
                    query_ab_gate.parse_raw_result(
                        parse_args(
                            raw,
                            parsed,
                            corpora[format_name],
                            format_name,
                            repetition,
                            order_index,
                            step_ms=1_000,
                            range_scalar_cache_max_bytes=cache_budget,
                        )
                    )
                    parsed_paths.append(parsed)
            comparison = root / "range-comparison.json"
            query_ab_gate.compare_results(
                parsed_paths,
                2,
                ["scalar_count"],
                root / "range-summary.tsv",
                comparison,
            )
            result = json.loads(comparison.read_text(encoding="utf-8"))
            changed = json.loads(parsed_paths[-1].read_text(encoding="utf-8"))
            changed["runs"][1]["range_scalar_cache"]["hits"] += 1
            parsed_paths[-1].write_text(json.dumps(changed), encoding="utf-8")
            with self.assertRaisesRegex(query_ab_gate.GateError, "range scalar cache"):
                query_ab_gate.compare_results(
                    parsed_paths,
                    2,
                    ["scalar_count"],
                    root / "range-changed-summary.tsv",
                    root / "range-changed-comparison.json",
                )
        accounting = result["queries"]["scalar_count"]["run_kind_accounting"]
        self.assertEqual(accounting["cold"]["range_scalar_cache"]["misses"], 2)
        self.assertEqual(accounting["warm"]["range_scalar_cache"]["hits"], 2)

    def test_raw_gate_rejects_empty_results_and_resource_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpus = root / "vnext"
            corpus.mkdir()
            document = raw_document(corpus, "vnext")
            document["runs"][0]["result_series"] = 0
            raw = root / "empty.json"
            raw.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(query_ab_gate.GateError, "result_series"):
                query_ab_gate.parse_raw_result(
                    parse_args(raw, root / "parsed-empty.json", corpus, "vnext", 1, 2)
                )
            document = raw_document(corpus, "vnext")
            document["runs"][0]["symbol_reads"]["resource_snapshot_errors_after_run"] = 1
            raw = root / "resource-error.json"
            raw.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(query_ab_gate.GateError, "resource snapshot"):
                query_ab_gate.parse_raw_result(
                    parse_args(raw, root / "parsed-error.json", corpus, "vnext", 1, 2)
                )

    def test_runner_dry_run_uses_synthetic_corpora_without_querying(self) -> None:
        script_dir = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            ab_root = root / "ab"
            v7 = ab_root / "runs" / "v7-a" / "segments"
            vnext = ab_root / "runs" / "vnext-a" / "segments"
            write_corpus(v7, 2)
            write_corpus(vnext, 3)
            ab_root.joinpath("COMPLETE").write_bytes(b"")
            fake_query = root / "chronoxide-query"
            fake_query.write_text(
                "#!/bin/sh\n"
                "if [ \"${1:-}\" = --help ]; then\n"
                "  echo --experimental-storage-layout-ab\n"
                "  exit 0\n"
                "fi\n"
                "echo query execution is forbidden in this fixture >&2\n"
                "exit 99\n",
                encoding="utf-8",
            )
            fake_query.chmod(fake_query.stat().st_mode | stat.S_IXUSR)
            result = root / "result"
            environment = os.environ.copy()
            environment.update(
                {
                    "AB_ROOT": str(ab_root),
                    "QUERY_BIN": str(fake_query),
                    "RESULT_DIR": str(result),
                    "END_MS": "1000",
                    "REPEATS": "2",
                    "QUERY_NAMES_OVERRIDE": "scalar_count",
                    "RUN_NOTE": "synthetic dry-run fixture",
                }
            )
            completed = subprocess.run(
                [str(script_dir / "query_ab_run.sh"), "--dry-run"],
                cwd=script_dir.parents[2],
                env=environment,
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if completed.returncode != 0:
                self.fail(f"dry run failed:\nstdout={completed.stdout}\nstderr={completed.stderr}")
            plan = result.joinpath("run-plan.tsv").read_text(encoding="utf-8")
            complete_exists = result.joinpath("DRY_RUN_COMPLETE").exists()
        self.assertTrue(complete_exists)
        self.assertEqual(plan.count("\n"), 1 + 4)
        self.assertIn("true", plan)
        self.assertIn("false", plan)


if __name__ == "__main__":
    unittest.main()
