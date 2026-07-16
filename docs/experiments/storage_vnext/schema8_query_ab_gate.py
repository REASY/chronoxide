#!/usr/bin/env python3
"""Manifest, inventory, and equivalence gates for the Schema 7/Schema 8 A/B."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any

import schema7_query_ab_gate as common


RAW_SCHEMA = common.RAW_SCHEMA
QUERY_STATS_FIELDS = common.QUERY_STATS_FIELDS
CONFIGURATION_FIELDS = common.CONFIGURATION_FIELDS
MANIFEST_SCHEMA = "chronoxide/schema7-schema8-query-manifest/v1"
NORMALIZED_MANIFEST_SCHEMA = "chronoxide/schema7-schema8-query-manifest-normalized/v1"
EQUIVALENCE_SCHEMA = "chronoxide/schema7-schema8-query-equivalence/v1"
LAYOUTS = ("schema7", "schema8")
ALLOWED_QUERY_STATS_DIFFERENCES = frozenset({"index_postings_bytes_read"})
PAYLOAD_FIELDS = frozenset(
    {"logical_used_bytes", "physical_reads", "physical_bytes"}
)
LABEL_MATERIALIZATION_FIELDS = frozenset(
    {
        "rows_integrity_checked",
        "pairs_integrity_checked",
        "rows_full_materialized",
        "rows_selectively_materialized",
        "pairs_materialized",
        "pairs_omitted",
        "content_bytes_materialized",
    }
)
RANGE_CACHE_REQUIRED_FIELDS = frozenset(
    {
        "configured_budget_bytes",
        "governor_lease_bytes",
        "governor_refused",
        "allocation_refused",
        "layout_overflow",
        "entry_arena_charge_bytes",
        "sample_arena_charge_bytes",
        "hits",
        "misses",
        "admitted_entries",
        "streaming_budget_bypasses",
        "unsupported_bypasses",
        "logical_hit_bytes",
        "logical_miss_or_bypass_bytes",
        "peak_retained_charge_bytes",
        "retained_charge_after_finalize",
        "process_governor_limit_bytes",
        "process_governor_current_leased_bytes",
        "process_governor_peak_leased_bytes",
    }
)
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class GateError(common.GateError):
    pass


def nonnegative_int(value: Any, name: str) -> int:
    try:
        return common.nonnegative_int(value, name)
    except common.GateError as error:
        raise GateError(str(error)) from error


def positive_int(value: Any, name: str) -> int:
    try:
        return common.positive_int(value, name)
    except common.GateError as error:
        raise GateError(str(error)) from error


def digest(value: Any, name: str) -> str:
    try:
        return common.hex_digest(value, name)
    except common.GateError as error:
        raise GateError(str(error)) from error


def checked_name(value: Any, name: str) -> str:
    if not isinstance(value, str) or not SAFE_NAME.fullmatch(value):
        raise GateError(f"{name} must match {SAFE_NAME.pattern}")
    return value


def checked_expression(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\t" in value
        or "\n" in value
        or "\r" in value
    ):
        raise GateError(f"{name} must be a non-empty, single-line, tab-free string")
    return value


def checked_boundaries(value: Any, name: str) -> list[float]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise GateError(f"{name} must be an array")
    result: list[float] = []
    for index, boundary in enumerate(value):
        if isinstance(boundary, bool) or not isinstance(boundary, (int, float)):
            raise GateError(f"{name}[{index}] must be numeric")
        converted = float(boundary)
        if not math.isfinite(converted):
            raise GateError(f"{name}[{index}] must be finite")
        result.append(converted)
    return result


def boundary_text(value: float) -> str:
    return format(value, ".17g")


def normalize_manifest(
    path: Path, default_range_cache_bytes: int
) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as source:
        document = json.load(source)
    if isinstance(document, list):
        raw_queries = document
    elif isinstance(document, dict):
        if document.get("schema") != MANIFEST_SCHEMA:
            raise GateError(f"manifest schema must be {MANIFEST_SCHEMA}")
        if set(document) - {"schema", "description", "queries"}:
            raise GateError("manifest has unknown top-level fields")
        raw_queries = document.get("queries")
    else:
        raise GateError("manifest must be an object or a legacy query array")
    if not isinstance(raw_queries, list) or not raw_queries:
        raise GateError("manifest must contain at least one query")

    normalized: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, raw in enumerate(raw_queries):
        context = f"queries[{index}]"
        if not isinstance(raw, dict):
            raise GateError(f"{context} must be an object")
        allowed = {
            "name",
            "category",
            "mode",
            "time_ms",
            "start_ms",
            "end_ms",
            "step_ms",
            "range_scalar_cache_max_bytes",
            "chronoxide_query",
            "exponential_histogram_bucket_boundaries",
        }
        unknown = set(raw) - allowed
        if unknown:
            raise GateError(f"{context} has unknown fields: {sorted(unknown)!r}")
        query_name = checked_name(raw.get("name"), f"{context}.name")
        if query_name in names:
            raise GateError(f"duplicate query name: {query_name}")
        names.add(query_name)
        category = checked_name(raw.get("category", "uncategorized"), f"{context}.category")
        expression = checked_expression(
            raw.get("chronoxide_query"), f"{context}.chronoxide_query"
        )
        mode = raw.get("mode")
        if mode not in ("instant", "range"):
            raise GateError(f"{context}.mode must be instant or range")
        boundaries = checked_boundaries(
            raw.get("exponential_histogram_bucket_boundaries"),
            f"{context}.exponential_histogram_bucket_boundaries",
        )
        if mode == "instant":
            forbidden = {
                "end_ms",
                "step_ms",
                "range_scalar_cache_max_bytes",
            }.intersection(raw)
            if forbidden:
                raise GateError(
                    f"{context}: instant query has range-only fields {sorted(forbidden)!r}"
                )
            end_ms = nonnegative_int(raw.get("time_ms"), f"{context}.time_ms")
            start_ms = nonnegative_int(raw.get("start_ms", 0), f"{context}.start_ms")
            if start_ms > end_ms:
                raise GateError(f"{context}: start_ms exceeds time_ms")
            step_ms = None
            cache_bytes = None
        else:
            if "time_ms" in raw:
                raise GateError(f"{context}: range query must not contain time_ms")
            start_ms = nonnegative_int(raw.get("start_ms"), f"{context}.start_ms")
            end_ms = nonnegative_int(raw.get("end_ms"), f"{context}.end_ms")
            step_ms = positive_int(raw.get("step_ms"), f"{context}.step_ms")
            if start_ms > end_ms:
                raise GateError(f"{context}: start_ms exceeds end_ms")
            evaluations = ((end_ms - start_ms) // step_ms) + 1
            if evaluations > 1_000_000:
                raise GateError(f"{context}: more than 1,000,000 range evaluations")
            cache_bytes = nonnegative_int(
                raw.get("range_scalar_cache_max_bytes", default_range_cache_bytes),
                f"{context}.range_scalar_cache_max_bytes",
            )
        normalized.append(
            {
                "query_name": query_name,
                "category": category,
                "mode": mode,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "step_ms": step_ms,
                "range_scalar_cache_max_bytes": cache_bytes,
                "boundaries": boundaries,
                "expression": expression,
            }
        )
    return normalized


def write_normalized_manifest(
    queries: list[dict[str, Any]], output_tsv: Path, output_json: Path
) -> None:
    fields = (
        "query_name",
        "category",
        "mode",
        "start_ms",
        "end_ms",
        "step_ms",
        "range_scalar_cache_max_bytes",
        "boundaries_csv",
        "expression",
    )
    with output_tsv.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=fields,
            delimiter="\t",
            lineterminator="\n",
            quotechar=None,
            quoting=csv.QUOTE_NONE,
        )
        writer.writeheader()
        for query in queries:
            writer.writerow(
                {
                    **{field: query[field] for field in fields[:5]},
                    "step_ms": "-" if query["step_ms"] is None else query["step_ms"],
                    "range_scalar_cache_max_bytes": (
                        "-"
                        if query["range_scalar_cache_max_bytes"] is None
                        else query["range_scalar_cache_max_bytes"]
                    ),
                    "boundaries_csv": (
                        "-"
                        if not query["boundaries"]
                        else ",".join(boundary_text(value) for value in query["boundaries"])
                    ),
                    "expression": query["expression"],
                }
            )
    with output_json.open("x", encoding="utf-8") as destination:
        json.dump(
            {"schema": NORMALIZED_MANIFEST_SCHEMA, "queries": queries},
            destination,
            indent=2,
            sort_keys=True,
        )
        destination.write("\n")


def read_normalized_manifest(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as source:
        document = json.load(source)
    if not isinstance(document, dict) or document.get("schema") != NORMALIZED_MANIFEST_SCHEMA:
        raise GateError("normalized manifest has the wrong schema")
    queries = document.get("queries")
    if not isinstance(queries, list) or not queries:
        raise GateError("normalized manifest has no queries")
    return queries


def read_raw_index(path: Path) -> list[dict[str, str]]:
    expected = {
        "process_label",
        "query_name",
        "category",
        "mode",
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


def validate_counter_object(value: Any, fields: frozenset[str], context: str) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != fields:
        raise GateError(f"{context} has an invalid shape")
    return {field: nonnegative_int(value[field], f"{context}.{field}") for field in fields}


def validate_range_cache(value: Any, query: dict[str, Any], context: str) -> Any:
    if query["mode"] == "instant":
        if value is not None:
            raise GateError(f"{context}: instant query unexpectedly has range-cache stats")
        return None
    if not isinstance(value, dict) or set(value) != RANGE_CACHE_REQUIRED_FIELDS:
        raise GateError(f"{context}: range-cache stats have an invalid shape")
    if value.get("configured_budget_bytes") != query["range_scalar_cache_max_bytes"]:
        raise GateError(f"{context}: range-cache budget differs from the manifest")
    for field, field_value in value.items():
        if field in {"governor_refused", "allocation_refused", "layout_overflow"}:
            if not isinstance(field_value, bool):
                raise GateError(f"{context}.range_scalar_cache.{field} must be boolean")
        else:
            nonnegative_int(field_value, f"{context}.range_scalar_cache.{field}")
    return value


def validate_raw(
    row: dict[str, str], query: dict[str, Any], args: argparse.Namespace
) -> tuple[str, list[dict[str, Any]]]:
    raw_path = Path(row["raw_output"])
    with raw_path.open(encoding="utf-8") as source:
        document = json.load(source)
    if document.get("schema") != RAW_SCHEMA:
        raise GateError(f"{raw_path}: raw schema must be {RAW_SCHEMA}")
    configuration = document.get("configuration")
    if not isinstance(configuration, dict) or set(configuration) != CONFIGURATION_FIELDS:
        raise GateError(f"{raw_path}: configuration differs from the v9 contract")
    expected_configuration = {
        "segments_dir": os.path.realpath(row["corpus"]),
        "start_ms": query["start_ms"],
        "end_ms": query["end_ms"],
        "mode": query["mode"] if query["mode"] == "instant" else "query_range",
        "step_ms": query["step_ms"],
        "range_scalar_cache_max_bytes": query["range_scalar_cache_max_bytes"],
        "chunk_read_mode": "pread",
        "chunk_read_queue_depth": args.queue_depth,
        "experimental_cross_segment_chunk_reads": False,
        "label_materialization": args.label_materialization,
        "query_label_storage": "owned-strings",
        "storage_layout": row["storage_layout"],
        "benchmark_repeats": args.benchmark_repeats,
        "queries": [query["expression"]],
        "prewarm_query_contexts": False,
        "prefetch_query_data": False,
        "exponential_histogram_bucket_boundaries": query["boundaries"],
        "requested_segment_footer_validation": False,
        "effective_segment_footer_validation": False,
    }
    if configuration != expected_configuration:
        raise GateError(
            f"{raw_path}: timed configuration differs from the pinned invocation: "
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
    corpus_fingerprint = digest(
        document.get("corpus_fingerprint_sha256"), f"{raw_path}.corpus_fingerprint"
    )
    runs = document.get("runs")
    if not isinstance(runs, list) or len(runs) != args.benchmark_repeats:
        raise GateError(f"{raw_path}: expected {args.benchmark_repeats} runs")
    validated: list[dict[str, Any]] = []
    for run_index, run in enumerate(runs):
        context = f"{raw_path}:{query['query_name']}:{run_index}"
        if not isinstance(run, dict):
            raise GateError(f"{context}: run must be an object")
        expected_kind = "cold" if run_index == 0 else "warm"
        if run.get("query") != query["expression"]:
            raise GateError(f"{context}: expression differs from the manifest")
        if run.get("run_index") != run_index or run.get("run_kind") != expected_kind:
            raise GateError(f"{context}: run index/kind is invalid")
        if (
            run.get("effective_start_ms") != query["start_ms"]
            or run.get("effective_end_ms") != query["end_ms"]
            or run.get("step_ms") != query["step_ms"]
        ):
            raise GateError(f"{context}: effective evaluation range differs")
        try:
            stats = common.validate_stats(run.get("stats"), context)
        except common.GateError as error:
            raise GateError(str(error)) from error
        payload = validate_counter_object(run.get("payload_reads"), PAYLOAD_FIELDS, f"{context}.payload")
        if payload["logical_used_bytes"] != stats["bytes_read"]:
            raise GateError(f"{context}: payload logical bytes differ from QueryStats")
        if payload["physical_bytes"] < payload["logical_used_bytes"]:
            raise GateError(f"{context}: physical payload bytes are below logical bytes")
        labels = validate_counter_object(
            run.get("label_materialization"),
            LABEL_MATERIALIZATION_FIELDS,
            f"{context}.label_materialization",
        )
        try:
            label_storage = common.validate_query_label_storage(
                run.get("query_label_storage"), context, "owned-strings"
            )
        except common.GateError as error:
            raise GateError(str(error)) from error
        cache = validate_range_cache(run.get("range_scalar_cache"), query, context)
        symbol_reads = run.get("symbol_reads")
        if not isinstance(symbol_reads, dict):
            raise GateError(f"{context}: symbol read counters are missing")
        validated.append(
            {
                "run_index": run_index,
                "run_kind": expected_kind,
                "duration_ns": positive_int(run.get("duration_ns"), f"{context}.duration_ns"),
                "semantic_fingerprint": digest(
                    run.get("semantic_fingerprint_sha256"), f"{context}.semantic_fingerprint"
                ),
                "portable_fingerprint": digest(
                    run.get("portable_semantic_fingerprint_sha256"),
                    f"{context}.portable_fingerprint",
                ),
                "result_series": nonnegative_int(run.get("result_series"), f"{context}.result_series"),
                "result_samples": nonnegative_int(run.get("result_samples"), f"{context}.result_samples"),
                "stats": stats,
                "payload": payload,
                "label_materialization": labels,
                "query_label_storage": label_storage,
                "range_scalar_cache": cache,
            }
        )
    return corpus_fingerprint, validated


def equivalent_stats(stats: dict[str, int]) -> dict[str, int]:
    return {
        field: value
        for field, value in stats.items()
        if field not in ALLOWED_QUERY_STATS_DIFFERENCES
    }


def stats_digest(stats: dict[str, int]) -> str:
    return hashlib.sha256(
        json.dumps(equivalent_stats(stats), separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def compare_results(args: argparse.Namespace) -> None:
    queries = read_normalized_manifest(args.manifest)
    query_by_name = {query["query_name"]: query for query in queries}
    rows = read_raw_index(args.index)
    expected_processes = len(queries) * args.repeats * len(LAYOUTS)
    if len(rows) != expected_processes:
        raise GateError(f"expected {expected_processes} completed processes, found {len(rows)}")

    processes: dict[tuple[str, int, str], dict[str, Any]] = {}
    corpus_fingerprints = {layout: set() for layout in LAYOUTS}
    process_labels: set[str] = set()
    for row in rows:
        query_name = row["query_name"]
        query = query_by_name.get(query_name)
        if query is None:
            raise GateError(f"raw index names unknown query {query_name!r}")
        if row["category"] != query["category"] or row["mode"] != query["mode"]:
            raise GateError(f"raw index metadata differs for query {query_name}")
        repetition = positive_int(int(row["repetition"]), "repetition")
        if repetition > args.repeats:
            raise GateError(f"repetition exceeds configured count: {repetition}")
        order_index = positive_int(int(row["order_index"]), "order_index")
        layout = row["storage_layout"]
        expected_order = LAYOUTS if repetition % 2 else tuple(reversed(LAYOUTS))
        if order_index not in (1, 2) or layout != expected_order[order_index - 1]:
            raise GateError(
                f"layout order was not alternated for query {query_name} repetition {repetition}"
            )
        if row["process_label"] in process_labels:
            raise GateError(f"duplicate process label: {row['process_label']}")
        process_labels.add(row["process_label"])
        key = (query_name, repetition, layout)
        if key in processes:
            raise GateError(f"duplicate completed process: {key!r}")
        fingerprint, runs = validate_raw(row, query, args)
        corpus_fingerprints[layout].add(fingerprint)
        processes[key] = {"row": row, "runs": {run["run_index"]: run for run in runs}}

    for query in queries:
        for repetition in range(1, args.repeats + 1):
            for layout in LAYOUTS:
                if (query["query_name"], repetition, layout) not in processes:
                    raise GateError(
                        f"missing process for {query['query_name']} repetition {repetition} {layout}"
                    )
    if any(len(values) != 1 for values in corpus_fingerprints.values()):
        raise GateError("a same-layout corpus fingerprint changed across repetitions")

    comparisons: list[dict[str, Any]] = []
    for query in queries:
        query_name = query["query_name"]
        for repetition in range(1, args.repeats + 1):
            baseline = processes[(query_name, repetition, "schema7")]["runs"]
            candidate = processes[(query_name, repetition, "schema8")]["runs"]
            if baseline.keys() != candidate.keys():
                raise GateError(f"run identities differ for {query_name} repetition {repetition}")
            for run_index in sorted(baseline):
                left = baseline[run_index]
                right = candidate[run_index]
                context = f"{query_name} repetition {repetition} run {run_index}"
                for field in (
                    "semantic_fingerprint",
                    "portable_fingerprint",
                    "result_series",
                    "result_samples",
                ):
                    if left[field] != right[field]:
                        raise GateError(f"{context}: {field} differs between Schema 7 and Schema 8")
                for field in QUERY_STATS_FIELDS:
                    if field in ALLOWED_QUERY_STATS_DIFFERENCES:
                        continue
                    if left["stats"][field] != right["stats"][field]:
                        raise GateError(
                            f"{context}: QueryStats.{field} differs between Schema 7 and Schema 8"
                        )
                comparisons.append(
                    {
                        "query_name": query_name,
                        "repetition": repetition,
                        "run_index": run_index,
                        "run_kind": left["run_kind"],
                        "semantic_fingerprint": left["semantic_fingerprint"],
                        "portable_fingerprint": left["portable_fingerprint"],
                        "equivalent_query_stats_sha256": stats_digest(left["stats"]),
                        "schema7_index_postings_bytes_read": left["stats"]["index_postings_bytes_read"],
                        "schema8_index_postings_bytes_read": right["stats"]["index_postings_bytes_read"],
                    }
                )

    summary_fields = [
        "process_label",
        "query_name",
        "category",
        "mode",
        "repetition",
        "order_index",
        "storage_layout",
        "run_index",
        "run_kind",
        "duration_ns",
        "max_rss_kib",
        "result_series",
        "result_samples",
        "semantic_fingerprint",
        "portable_fingerprint",
        "equivalent_query_stats_sha256",
        *(f"stats_{field}" for field in QUERY_STATS_FIELDS),
        "payload_logical_used_bytes",
        "payload_physical_reads",
        "payload_physical_bytes",
        "range_cache_configured_budget_bytes",
        "range_cache_hits",
        "range_cache_misses",
        "range_cache_unsupported_bypasses",
        "labels_pairs_integrity_checked",
        "labels_pairs_materialized",
        "labels_pairs_omitted",
    ]
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination, fieldnames=summary_fields, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for query in queries:
            query_name = query["query_name"]
            for repetition in range(1, args.repeats + 1):
                order = LAYOUTS if repetition % 2 else tuple(reversed(LAYOUTS))
                for layout in order:
                    process = processes[(query_name, repetition, layout)]
                    index_row = process["row"]
                    for run_index in range(args.benchmark_repeats):
                        run = process["runs"][run_index]
                        cache = run["range_scalar_cache"] or {}
                        row: dict[str, Any] = {
                            "process_label": index_row["process_label"],
                            "query_name": query_name,
                            "category": query["category"],
                            "mode": query["mode"],
                            "repetition": repetition,
                            "order_index": index_row["order_index"],
                            "storage_layout": layout,
                            "run_index": run_index,
                            "run_kind": run["run_kind"],
                            "duration_ns": run["duration_ns"],
                            "max_rss_kib": index_row["max_rss_kib"],
                            "result_series": run["result_series"],
                            "result_samples": run["result_samples"],
                            "semantic_fingerprint": run["semantic_fingerprint"],
                            "portable_fingerprint": run["portable_fingerprint"],
                            "equivalent_query_stats_sha256": stats_digest(run["stats"]),
                            "payload_logical_used_bytes": run["payload"]["logical_used_bytes"],
                            "payload_physical_reads": run["payload"]["physical_reads"],
                            "payload_physical_bytes": run["payload"]["physical_bytes"],
                            "range_cache_configured_budget_bytes": cache.get("configured_budget_bytes", ""),
                            "range_cache_hits": cache.get("hits", ""),
                            "range_cache_misses": cache.get("misses", ""),
                            "range_cache_unsupported_bypasses": cache.get("unsupported_bypasses", ""),
                            "labels_pairs_integrity_checked": run["label_materialization"]["pairs_integrity_checked"],
                            "labels_pairs_materialized": run["label_materialization"]["pairs_materialized"],
                            "labels_pairs_omitted": run["label_materialization"]["pairs_omitted"],
                        }
                        row.update(
                            {f"stats_{field}": run["stats"][field] for field in QUERY_STATS_FIELDS}
                        )
                        writer.writerow(row)

    result = {
        "schema": EQUIVALENCE_SCHEMA,
        "canonical_equivalence": "pass",
        "allowed_query_stats_differences": sorted(ALLOWED_QUERY_STATS_DIFFERENCES),
        "matching_runs_compared": len(comparisons),
        "repeats": args.repeats,
        "benchmark_repeats": args.benchmark_repeats,
        "query_names": [query["query_name"] for query in queries],
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
    manifest = commands.add_parser("normalize-manifest")
    manifest.add_argument("--input", type=Path, required=True)
    manifest.add_argument("--output-tsv", type=Path, required=True)
    manifest.add_argument("--output-json", type=Path, required=True)
    manifest.add_argument("--default-range-cache-bytes", type=int, required=True)
    inventory = commands.add_parser("inventory")
    inventory.add_argument("--corpus", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--paths-output", type=Path, required=True)
    compare = commands.add_parser("compare-results")
    compare.add_argument("--index", type=Path, required=True)
    compare.add_argument("--manifest", type=Path, required=True)
    compare.add_argument("--summary", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--repeats", type=int, required=True)
    compare.add_argument("--benchmark-repeats", type=int, required=True)
    compare.add_argument("--queue-depth", type=int, required=True)
    compare.add_argument("--label-materialization", choices=("full", "demand-driven"), required=True)
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
        if args.command == "normalize-manifest":
            default_cache = nonnegative_int(
                args.default_range_cache_bytes, "default range cache bytes"
            )
            write_normalized_manifest(
                normalize_manifest(args.input, default_cache),
                args.output_tsv,
                args.output_json,
            )
        elif args.command == "inventory":
            common.write_inventory(args.corpus, args.output, args.paths_output)
        elif args.command == "compare-results":
            positive_int(args.repeats, "repeats")
            positive_int(args.benchmark_repeats, "benchmark repeats")
            compare_results(args)
        else:
            raise AssertionError(args.command)
    except (
        GateError,
        common.GateError,
        OSError,
        TypeError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
    ) as error:
        print(f"schema7/schema8 query A/B gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
