#!/usr/bin/env python3
"""Strict inventory and equivalence gates for the Schema 6/Schema 7 query A/B."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any


RAW_SCHEMA = "chronoxide.query-benchmark.raw/v10"
INVENTORY_SCHEMA = "chronoxide/schema7-query-ab-inventory/v1"
QUERY_STATS_FIELDS = (
    "segments_considered",
    "segments_skipped_by_time",
    "segments_skipped_by_missing_equality",
    "segments_skipped_by_matcher_time_range",
    "segments_queried",
    "matched_series",
    "projected_series",
    "chunk_reads",
    "bytes_read",
    "samples_decoded",
    "typed_scalar_chunks_decoded",
    "typed_full_chunks_decoded",
    "regex_values_examined",
    "index_postings_reads",
    "index_postings_bytes_read",
)
CONFIGURATION_FIELDS = {
    "segments_dir",
    "start_ms",
    "end_ms",
    "mode",
    "step_ms",
    "range_scalar_cache_max_bytes",
    "chunk_read_mode",
    "chunk_read_queue_depth",
    "experimental_cross_segment_chunk_reads",
    "label_materialization",
    "query_label_storage",
    "query_instrumentation",
    "storage_layout",
    "benchmark_repeats",
    "queries",
    "prewarm_query_contexts",
    "prefetch_query_data",
    "exponential_histogram_bucket_boundaries",
    "requested_segment_footer_validation",
    "effective_segment_footer_validation",
}
QUERY_STAGE_FIELDS = {
    "canonical_row_decode_ns",
    "symbol_lookup_ns",
    "symbol_resolution_ns",
    "candidate_selection_ns",
    "canonical_identity_ns",
    "metadata_visit_overhead_ns",
    "matcher_evaluation_ns",
    "label_construction_ns",
    "locator_planning_ns",
    "payload_read_pipeline_combined_ns",
    "payload_decode_projection_result_processing_combined_ns",
    "source_merge_ns",
    "promql_grouping_evaluation_ns",
    "result_construction_ns",
    "exclusive_total_ns",
    "unclassified_ns",
}
QUERY_STAGE_LEAF_FIELDS = QUERY_STAGE_FIELDS - {
    "exclusive_total_ns",
    "unclassified_ns",
}
QUERY_LABEL_STORAGE_FIELDS = {
    "label_sets",
    "atom_lookups",
    "atom_hits",
    "atom_misses",
    "unique_content_bytes",
}


class GateError(ValueError):
    pass


def nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GateError(f"{name} must be a non-negative integer")
    return value


def positive_int(value: Any, name: str) -> int:
    value = nonnegative_int(value, name)
    if value == 0:
        raise GateError(f"{name} must be positive")
    return value


def hex_digest(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise GateError(f"{name} must be a lowercase SHA-256 digest")
    return value


def sha256_descriptor(descriptor: int) -> str:
    digest = hashlib.sha256()
    while block := os.read(descriptor, 1024 * 1024):
        digest.update(block)
    return digest.hexdigest()


def inventory_file(path: str, relative: str) -> dict[str, Any]:
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode):
        raise GateError(f"corpus entry is not a regular file: {relative!r}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise GateError(f"opened corpus entry is not regular: {relative!r}")
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise GateError(f"corpus file changed identity before hashing: {relative!r}")
        digest = sha256_descriptor(descriptor)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    before_identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity:
        raise GateError(f"corpus file changed while hashing: {relative!r}")
    return {"path": relative, "size_bytes": opened.st_size, "sha256": digest}


def inventory_corpus(corpus: Path) -> tuple[dict[str, Any], list[bytes]]:
    root = os.path.realpath(os.fspath(corpus))
    if not stat.S_ISDIR(os.lstat(root).st_mode):
        raise GateError(f"corpus is not a directory: {root!r}")
    entries: list[dict[str, Any]] = []
    absolute_paths: list[bytes] = []

    def visit(directory: str, relative_directory: str) -> None:
        children = sorted(os.scandir(directory), key=lambda entry: os.fsencode(entry.name))
        for child in children:
            relative = (
                child.name
                if not relative_directory
                else os.path.join(relative_directory, child.name)
            )
            metadata = child.stat(follow_symlinks=False)
            if stat.S_ISLNK(metadata.st_mode):
                raise GateError(f"symbolic links are forbidden in a corpus: {relative!r}")
            if stat.S_ISDIR(metadata.st_mode):
                visit(child.path, relative)
            elif stat.S_ISREG(metadata.st_mode):
                entries.append(inventory_file(child.path, relative))
                absolute_paths.append(os.fsencode(os.path.abspath(child.path)))
            else:
                raise GateError(f"non-file corpus entry is forbidden: {relative!r}")

    visit(root, "")
    if not entries:
        raise GateError("corpus contains no regular files")
    entries.sort(key=lambda entry: os.fsencode(entry["path"]))
    absolute_paths.sort()
    canonical = json.dumps(entries, separators=(",", ":"), sort_keys=True).encode()
    return (
        {
            "schema": INVENTORY_SCHEMA,
            "corpus": root,
            "corpus_sha256": hashlib.sha256(canonical).hexdigest(),
            "file_count": len(entries),
            "total_bytes": sum(entry["size_bytes"] for entry in entries),
            "files": entries,
        },
        absolute_paths,
    )


def write_inventory(corpus: Path, output: Path, paths_output: Path) -> None:
    inventory, paths = inventory_corpus(corpus)
    with output.open("x", encoding="utf-8") as destination:
        json.dump(inventory, destination, indent=2, sort_keys=True)
        destination.write("\n")
    with paths_output.open("xb") as destination:
        for path in paths:
            if b"\0" in path:
                raise GateError("a corpus path contains NUL")
            destination.write(path + b"\0")


def read_queries(path: Path) -> list[tuple[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if not rows or any(set(row) != {"query_name", "expression"} for row in rows):
        raise GateError("queries TSV has an invalid shape")
    result: list[tuple[str, str]] = []
    names: set[str] = set()
    for row in rows:
        name = row["query_name"]
        expression = row["expression"]
        if not name or not expression or name in names:
            raise GateError("queries TSV contains an empty or duplicate query")
        names.add(name)
        result.append((name, expression))
    return result


def read_index(path: Path) -> list[dict[str, str]]:
    expected = {
        "process_label",
        "repetition",
        "order_index",
        "storage_layout",
        "corpus",
        "raw_output",
        "max_rss_kib",
    }
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if any(set(row) != expected for row in rows):
        raise GateError("raw index TSV has an invalid shape")
    return rows


def validate_stats(value: Any, context: str) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != set(QUERY_STATS_FIELDS):
        raise GateError(f"{context} QueryStats fields differ from the canonical contract")
    return {field: nonnegative_int(value[field], f"{context}.stats.{field}") for field in QUERY_STATS_FIELDS}


def validate_query_label_storage(
    value: Any, context: str, policy: str
) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != QUERY_LABEL_STORAGE_FIELDS:
        raise GateError(f"{context} query-label storage counters differ from the v9 contract")
    counters = {
        field: nonnegative_int(value[field], f"{context}.query_label_storage.{field}")
        for field in QUERY_LABEL_STORAGE_FIELDS
    }
    if counters["atom_lookups"] != counters["atom_hits"] + counters["atom_misses"]:
        raise GateError(f"{context} query-label atom accounting is incomplete")
    if policy == "owned-strings" and any(
        counters[field]
        for field in (
            "atom_lookups",
            "atom_hits",
            "atom_misses",
            "unique_content_bytes",
        )
    ):
        raise GateError(f"{context} owned query labels unexpectedly report atom activity")
    return counters


def validate_query_stages(
    value: Any,
    instrumentation: str,
    duration_ns: int,
    context: str,
) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != QUERY_STAGE_FIELDS:
        raise GateError(f"{context} query-stage fields differ from the v10 contract")
    stages = {
        field: nonnegative_int(value[field], f"{context}.query_stages.{field}")
        for field in QUERY_STAGE_FIELDS
    }
    leaf_total = sum(stages[field] for field in QUERY_STAGE_LEAF_FIELDS)
    if stages["exclusive_total_ns"] != leaf_total:
        raise GateError(f"{context} exclusive query-stage total does not equal its leaves")
    if stages["exclusive_total_ns"] > duration_ns:
        raise GateError(f"{context} exclusive query stages exceed measured query duration")
    if stages["unclassified_ns"] != duration_ns - stages["exclusive_total_ns"]:
        raise GateError(f"{context} unclassified query duration is inconsistent")
    if instrumentation == "off" and stages["exclusive_total_ns"] != 0:
        raise GateError(f"{context} off-mode query unexpectedly reports detailed stages")
    return stages


def validate_raw(
    row: dict[str, str],
    queries: list[tuple[str, str]],
    args: argparse.Namespace,
) -> tuple[str, list[dict[str, Any]]]:
    raw_path = Path(row["raw_output"])
    with raw_path.open(encoding="utf-8") as source:
        document = json.load(source)
    if document.get("schema") != RAW_SCHEMA:
        raise GateError(f"{raw_path}: raw schema must be {RAW_SCHEMA}")
    configuration = document.get("configuration")
    if not isinstance(configuration, dict) or set(configuration) != CONFIGURATION_FIELDS:
        raise GateError(f"{raw_path}: configuration differs from the v10 contract")
    layout = row["storage_layout"]
    expected_configuration = {
        "segments_dir": os.path.realpath(row["corpus"]),
        "start_ms": args.start_ms,
        "end_ms": args.end_ms,
        "mode": "instant",
        "step_ms": None,
        "range_scalar_cache_max_bytes": None,
        "chunk_read_mode": "pread",
        "chunk_read_queue_depth": args.queue_depth,
        "experimental_cross_segment_chunk_reads": False,
        "label_materialization": "full",
        "query_label_storage": "owned-strings",
        "query_instrumentation": "off",
        "storage_layout": layout,
        "benchmark_repeats": args.benchmark_repeats,
        "queries": [expression for _, expression in queries],
        "prewarm_query_contexts": False,
        "prefetch_query_data": False,
        "exponential_histogram_bucket_boundaries": [],
        "requested_segment_footer_validation": True,
        "effective_segment_footer_validation": True,
    }
    if configuration != expected_configuration:
        raise GateError(
            f"{raw_path}: configuration differs from the pinned invocation: "
            f"expected={expected_configuration!r} actual={configuration!r}"
        )
    expected_limits = {
        "max_matched_series": args.max_matched_series,
        "max_projected_series": args.max_projected_series,
        "max_chunk_reads": args.max_chunk_reads,
        "max_bytes_read": args.max_bytes_read,
        "max_samples_decoded": args.max_samples_decoded,
        "max_regex_values_examined": args.max_regex_values_examined,
    }
    if document.get("limits") != expected_limits:
        raise GateError(f"{raw_path}: query limits differ from the pinned invocation")
    corpus_fingerprint = hex_digest(
        document.get("corpus_fingerprint_sha256"), f"{raw_path}.corpus_fingerprint"
    )
    runs = document.get("runs")
    expected_count = len(queries) * args.benchmark_repeats
    if not isinstance(runs, list) or len(runs) != expected_count:
        raise GateError(f"{raw_path}: expected {expected_count} query runs")
    validated: list[dict[str, Any]] = []
    offset = 0
    for query_name, expression in queries:
        for run_index in range(args.benchmark_repeats):
            run = runs[offset]
            offset += 1
            context = f"{raw_path}:{query_name}:{run_index}"
            expected_kind = "cold" if run_index == 0 else "warm"
            if run.get("query") != expression:
                raise GateError(f"{context}: expression differs from the query manifest")
            if run.get("run_index") != run_index or run.get("run_kind") != expected_kind:
                raise GateError(f"{context}: run index/kind is invalid")
            if run.get("effective_start_ms") != args.start_ms:
                raise GateError(f"{context}: effective start differs")
            if run.get("effective_end_ms") != args.end_ms or run.get("step_ms") is not None:
                raise GateError(f"{context}: effective end or instant-query mode differs")
            stats = validate_stats(run.get("stats"), context)
            payload = run.get("payload_reads")
            if not isinstance(payload, dict) or set(payload) != {
                "logical_used_bytes",
                "physical_reads",
                "physical_bytes",
            }:
                raise GateError(f"{context}: payload read counters have an invalid shape")
            payload = {
                field: nonnegative_int(payload[field], f"{context}.payload.{field}")
                for field in payload
            }
            if payload["logical_used_bytes"] != stats["bytes_read"]:
                raise GateError(f"{context}: payload used bytes differ from QueryStats")
            if payload["physical_bytes"] < payload["logical_used_bytes"]:
                raise GateError(f"{context}: physical payload bytes are below used bytes")
            if run.get("range_scalar_cache") is not None:
                raise GateError(f"{context}: instant query unexpectedly used the range cache")
            label_storage = validate_query_label_storage(
                run.get("query_label_storage"), context, "owned-strings"
            )
            duration_ns = positive_int(run.get("duration_ns"), f"{context}.duration_ns")
            nonnegative_int(
                run.get("post_query_fingerprint_ns"),
                f"{context}.post_query_fingerprint_ns",
            )
            validate_query_stages(
                run.get("query_stages"), "off", duration_ns, context
            )
            if not isinstance(run.get("metadata_runtime"), dict):
                raise GateError(f"{context}: metadata runtime report is missing")
            validated.append(
                {
                    "query_name": query_name,
                    "run_index": run_index,
                    "run_kind": expected_kind,
                    "duration_ns": duration_ns,
                    "semantic_fingerprint": hex_digest(
                        run.get("semantic_fingerprint_sha256"), f"{context}.semantic_fingerprint"
                    ),
                    "portable_fingerprint": hex_digest(
                        run.get("portable_semantic_fingerprint_sha256"),
                        f"{context}.portable_fingerprint",
                    ),
                    "result_series": nonnegative_int(run.get("result_series"), f"{context}.result_series"),
                    "result_samples": nonnegative_int(run.get("result_samples"), f"{context}.result_samples"),
                    "stats": stats,
                    "payload": payload,
                    "symbol_reads": run.get("symbol_reads"),
                    "query_label_storage": label_storage,
                }
            )
    return corpus_fingerprint, validated


def nested_count(value: Any, field: str, component: str) -> int:
    if not isinstance(value, dict):
        raise GateError("symbol read counters are missing")
    counter = value.get(field)
    if not isinstance(counter, dict):
        raise GateError(f"symbol read counter {field} is missing")
    return nonnegative_int(counter.get(component), f"symbol_reads.{field}.{component}")


def compare_results(args: argparse.Namespace) -> None:
    queries = read_queries(args.queries)
    rows = read_index(args.index)
    expected_processes = args.repeats * 2
    if len(rows) != expected_processes:
        raise GateError(f"expected {expected_processes} completed processes, found {len(rows)}")
    processes: dict[tuple[int, str], dict[str, Any]] = {}
    corpus_fingerprints = {"schema6-ab": set(), "schema7": set()}
    for row in rows:
        repetition = positive_int(int(row["repetition"]), "repetition")
        order_index = positive_int(int(row["order_index"]), "order_index")
        layout = row["storage_layout"]
        if layout not in corpus_fingerprints:
            raise GateError(f"unknown storage layout in raw index: {layout!r}")
        expected_order = (
            ("schema6-ab", "schema7") if repetition % 2 else ("schema7", "schema6-ab")
        )
        if order_index not in (1, 2) or layout != expected_order[order_index - 1]:
            raise GateError(f"layout order was not alternated for repetition {repetition}")
        key = (repetition, layout)
        if key in processes:
            raise GateError(f"duplicate completed process: {key!r}")
        fingerprint, runs = validate_raw(row, queries, args)
        corpus_fingerprints[layout].add(fingerprint)
        processes[key] = {
            "row": row,
            "runs": {(run["query_name"], run["run_index"]): run for run in runs},
        }
    for repetition in range(1, args.repeats + 1):
        for layout in ("schema6-ab", "schema7"):
            if (repetition, layout) not in processes:
                raise GateError(f"missing completed process for repetition {repetition} {layout}")
    if any(len(values) != 1 for values in corpus_fingerprints.values()):
        raise GateError("a same-layout corpus fingerprint changed across repetitions")

    comparisons: list[dict[str, Any]] = []
    for repetition in range(1, args.repeats + 1):
        baseline = processes[(repetition, "schema6-ab")]["runs"]
        candidate = processes[(repetition, "schema7")]["runs"]
        if baseline.keys() != candidate.keys():
            raise GateError(f"run identities differ in repetition {repetition}")
        for key in baseline:
            left = baseline[key]
            right = candidate[key]
            identity = f"repetition {repetition} query {key[0]} run {key[1]}"
            for field in (
                "semantic_fingerprint",
                "portable_fingerprint",
                "result_series",
                "result_samples",
                "stats",
            ):
                if left[field] != right[field]:
                    raise GateError(f"{identity}: {field} differs between Schema 6 and Schema 7")
            comparisons.append(
                {
                    "repetition": repetition,
                    "query_name": key[0],
                    "run_index": key[1],
                    "semantic_fingerprint": left["semantic_fingerprint"],
                    "portable_fingerprint": left["portable_fingerprint"],
                    "query_stats_sha256": hashlib.sha256(
                        json.dumps(left["stats"], separators=(",", ":"), sort_keys=True).encode()
                    ).hexdigest(),
                }
            )

    fields = [
        "process_label",
        "repetition",
        "order_index",
        "storage_layout",
        "query_name",
        "run_index",
        "run_kind",
        "duration_ns",
        "max_rss_kib",
        "result_series",
        "result_samples",
        "semantic_fingerprint",
        "portable_fingerprint",
        "query_stats_sha256",
        *(f"stats_{field}" for field in QUERY_STATS_FIELDS),
        "payload_used_bytes",
        "payload_physical_reads",
        "payload_physical_bytes",
        "payload_read_amplification",
        "symbol_logical_calls",
        "symbol_logical_bytes",
        "symbol_physical_calls",
        "symbol_physical_bytes",
    ]
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for repetition in range(1, args.repeats + 1):
            for layout in ("schema6-ab", "schema7"):
                process = processes[(repetition, layout)]
                index_row = process["row"]
                for query_name, _ in queries:
                    for run_index in range(args.benchmark_repeats):
                        run = process["runs"][(query_name, run_index)]
                        payload = run["payload"]
                        symbols = run["symbol_reads"]
                        physical_calls = sum(
                            nested_count(symbols, field, "calls")
                            for field in ("legacy_eager_read_delta", "root_read_delta", "page_read_delta")
                        )
                        physical_bytes = sum(
                            nested_count(symbols, field, "bytes")
                            for field in ("legacy_eager_read_delta", "root_read_delta", "page_read_delta")
                        )
                        logical_calls = nested_count(symbols, "logical_returned_delta", "calls")
                        logical_bytes = nested_count(symbols, "logical_returned_delta", "bytes")
                        stats_digest = hashlib.sha256(
                            json.dumps(run["stats"], separators=(",", ":"), sort_keys=True).encode()
                        ).hexdigest()
                        row: dict[str, Any] = {
                            "process_label": index_row["process_label"],
                            "repetition": repetition,
                            "order_index": index_row["order_index"],
                            "storage_layout": layout,
                            "query_name": query_name,
                            "run_index": run_index,
                            "run_kind": run["run_kind"],
                            "duration_ns": run["duration_ns"],
                            "max_rss_kib": index_row["max_rss_kib"],
                            "result_series": run["result_series"],
                            "result_samples": run["result_samples"],
                            "semantic_fingerprint": run["semantic_fingerprint"],
                            "portable_fingerprint": run["portable_fingerprint"],
                            "query_stats_sha256": stats_digest,
                            "payload_used_bytes": payload["logical_used_bytes"],
                            "payload_physical_reads": payload["physical_reads"],
                            "payload_physical_bytes": payload["physical_bytes"],
                            "payload_read_amplification": (
                                ""
                                if payload["logical_used_bytes"] == 0
                                else f"{payload['physical_bytes'] / payload['logical_used_bytes']:.6f}"
                            ),
                            "symbol_logical_calls": logical_calls,
                            "symbol_logical_bytes": logical_bytes,
                            "symbol_physical_calls": physical_calls,
                            "symbol_physical_bytes": physical_bytes,
                        }
                        row.update({f"stats_{field}": run["stats"][field] for field in QUERY_STATS_FIELDS})
                        writer.writerow(row)

    result = {
        "schema": "chronoxide/schema7-query-ab-equivalence/v1",
        "canonical_equivalence": "pass",
        "matching_runs_compared": len(comparisons),
        "repeats": args.repeats,
        "benchmark_repeats": args.benchmark_repeats,
        "query_names": [name for name, _ in queries],
        "corpus_fingerprints": {
            layout: next(iter(values)) for layout, values in corpus_fingerprints.items()
        },
        "comparisons": comparisons,
    }
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    inventory = commands.add_parser("inventory")
    inventory.add_argument("--corpus", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--paths-output", type=Path, required=True)
    compare = commands.add_parser("compare-results")
    compare.add_argument("--index", type=Path, required=True)
    compare.add_argument("--queries", type=Path, required=True)
    compare.add_argument("--summary", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--repeats", type=int, required=True)
    compare.add_argument("--benchmark-repeats", type=int, required=True)
    compare.add_argument("--start-ms", type=int, required=True)
    compare.add_argument("--end-ms", type=int, required=True)
    compare.add_argument("--queue-depth", type=int, required=True)
    compare.add_argument("--max-matched-series", type=int, required=True)
    compare.add_argument("--max-projected-series", type=int, required=True)
    compare.add_argument("--max-chunk-reads", type=int, required=True)
    compare.add_argument("--max-bytes-read", type=int, required=True)
    compare.add_argument("--max-samples-decoded", type=int, required=True)
    compare.add_argument("--max-regex-values-examined", type=int, required=True)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "inventory":
            write_inventory(args.corpus, args.output, args.paths_output)
        elif args.command == "compare-results":
            compare_results(args)
        else:
            raise AssertionError(args.command)
    except (GateError, OSError, TypeError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"schema7 query A/B gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
