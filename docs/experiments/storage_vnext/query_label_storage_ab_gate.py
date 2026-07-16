#!/usr/bin/env python3
"""Strict same-binary SharedAtoms/OwnedStrings Schema 8 A/B gates."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

import schema7_query_ab_gate as common
import schema8_query_ab_gate as schema8


POLICIES = ("owned-strings", "shared-atoms")
EQUIVALENCE_SCHEMA = "chronoxide/query-label-storage-equivalence/v1"
REQUIRED_CATEGORIES = frozenset(
    {
        "high-cardinality-full-label-selector",
        "metric-full-label-selector",
        "typed-scalar-range",
        "native-histogram-range",
        "native-exponential-histogram-range",
        "empty-result-control",
    }
)
CONFIGURATION_FIELDS = frozenset(
    {
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
        "storage_layout",
        "benchmark_repeats",
        "queries",
        "prewarm_query_contexts",
        "prefetch_query_data",
        "exponential_histogram_bucket_boundaries",
        "requested_segment_footer_validation",
        "effective_segment_footer_validation",
    }
)
RAW_DOCUMENT_FIELDS = frozenset(
    {
        "schema",
        "corpus_fingerprint_sha256",
        "corpus_fingerprint_duration_ns",
        "configuration",
        "limits",
        "runs",
    }
)
RAW_RUN_FIELDS = frozenset(
    {
        "query",
        "run_kind",
        "run_index",
        "duration_ns",
        "effective_start_ms",
        "effective_end_ms",
        "step_ms",
        "semantic_fingerprint_sha256",
        "portable_semantic_fingerprint_sha256",
        "result_series",
        "result_samples",
        "stats",
        "payload_reads",
        "symbol_reads",
        "label_materialization",
        "range_scalar_cache",
        "query_label_storage",
    }
)
READ_COUNT_FIELDS = frozenset({"calls", "bytes"})
SYMBOL_READ_FIELDS = frozenset(
    {
        "legacy_eager_read_delta",
        "logical_returned_delta",
        "root_read_delta",
        "page_read_delta",
        "page_validation_delta",
        "page_validation_ns_delta",
        "touched_corrupt_pages_delta",
        "page_cache_hits_delta",
        "page_cache_misses_delta",
        "page_cache_evictions_delta",
        "retained_readers_after_run",
        "retained_open_files_after_run",
        "source_file_bytes_after_run",
        "root_encoded_bytes_after_run",
        "root_retained_charge_bytes_after_run",
        "eager_dictionary_retained_charge_bytes_after_run",
        "page_cache_charge_bytes_after_run",
        "page_cache_max_bytes_after_run",
        "total_retained_charge_bytes_after_run",
        "resource_snapshot_errors_after_run",
    }
)
SYMBOL_READ_COUNT_FIELDS = frozenset(
    {
        "legacy_eager_read_delta",
        "logical_returned_delta",
        "root_read_delta",
        "page_read_delta",
        "page_validation_delta",
    }
)


class GateError(ValueError):
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


def nonnegative_decimal(value: str, name: str) -> float:
    try:
        converted = float(value)
    except (TypeError, ValueError) as error:
        raise GateError(f"{name} must be numeric") from error
    if not math.isfinite(converted) or converted < 0:
        raise GateError(f"{name} must be finite and non-negative")
    return converted


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def read_manifest(path: Path) -> list[dict[str, Any]]:
    queries = schema8.read_normalized_manifest(path)
    categories = {query["category"] for query in queries}
    missing = REQUIRED_CATEGORIES - categories
    if missing:
        raise GateError(f"manifest lacks required categories: {sorted(missing)!r}")
    for query in queries:
        if query["mode"] == "range" and query["range_scalar_cache_max_bytes"] != 0:
            raise GateError(f"{query['query_name']}: range scalar cache must be disabled")
    return queries


def read_index(path: Path) -> list[dict[str, str]]:
    expected = {
        "process_label",
        "query_name",
        "category",
        "mode",
        "repetition",
        "order_index",
        "query_label_storage",
        "corpus",
        "raw_output",
        "process_wall_seconds",
        "process_user_seconds",
        "process_system_seconds",
        "max_rss_kib",
    }
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if any(set(row) != expected for row in rows):
        raise GateError("raw index TSV has an invalid shape")
    return rows


def validate_counter_object(
    value: Any, fields: frozenset[str], context: str
) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != fields:
        raise GateError(f"{context} has an invalid shape")
    return {field: nonnegative_int(value[field], f"{context}.{field}") for field in fields}


def validate_read_count(value: Any, context: str) -> dict[str, int]:
    return validate_counter_object(value, READ_COUNT_FIELDS, context)


def validate_symbol_reads(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != SYMBOL_READ_FIELDS:
        raise GateError(f"{context} has an invalid shape")
    result: dict[str, Any] = {}
    for field in SYMBOL_READ_FIELDS:
        if field in SYMBOL_READ_COUNT_FIELDS:
            result[field] = validate_read_count(value[field], f"{context}.{field}")
        else:
            result[field] = nonnegative_int(value[field], f"{context}.{field}")
    if result["touched_corrupt_pages_delta"] != 0:
        raise GateError(f"{context} reports touched corrupt symbol pages")
    if result["resource_snapshot_errors_after_run"] != 0:
        raise GateError(f"{context} reports resource snapshot errors")
    return result


def comparable_symbol_reads(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != "page_validation_ns_delta"}


def validate_full_materialization(value: Any, context: str) -> dict[str, int]:
    labels = validate_counter_object(
        value, schema8.LABEL_MATERIALIZATION_FIELDS, context
    )
    if labels["rows_selectively_materialized"] != 0 or labels["pairs_omitted"] != 0:
        raise GateError(f"{context}: full mode reported selective materialization")
    if labels["rows_full_materialized"] != labels["rows_integrity_checked"]:
        raise GateError(f"{context}: not every integrity-checked row was materialized")
    if labels["pairs_materialized"] != labels["pairs_integrity_checked"]:
        raise GateError(f"{context}: not every integrity-checked pair was materialized")
    return labels


def validate_label_storage(value: Any, policy: str, context: str) -> dict[str, int]:
    try:
        counters = common.validate_query_label_storage(value, context, policy)
    except common.GateError as error:
        raise GateError(str(error)) from error
    if counters["atom_misses"] > counters["atom_lookups"]:
        raise GateError(f"{context}: atom misses exceed lookups")
    if counters["atom_misses"] == 0 and counters["unique_content_bytes"] != 0:
        raise GateError(f"{context}: unique bytes changed without atom misses")
    return counters


def add_counters(values: list[dict[str, int]]) -> dict[str, int]:
    return {
        field: sum(value[field] for value in values)
        for field in common.QUERY_LABEL_STORAGE_FIELDS
    }


def validate_process_atom_activity(
    policy: str, category: str, runs: list[dict[str, Any]], context: str
) -> dict[str, int]:
    totals = add_counters([run["query_label_storage"] for run in runs])
    if category == "empty-result-control":
        if any(totals.values()):
            raise GateError(f"{context}: empty-result control unexpectedly stored labels")
        if any(run["result_series"] or run["result_samples"] for run in runs):
            raise GateError(f"{context}: empty-result control returned data")
        return totals
    if not any(run["result_series"] for run in runs):
        raise GateError(f"{context}: stress query returned no series")
    if totals["label_sets"] == 0:
        raise GateError(f"{context}: result-bearing query stored no label sets")
    if policy == "shared-atoms":
        for field in ("atom_lookups", "atom_hits", "atom_misses", "unique_content_bytes"):
            if totals[field] == 0:
                raise GateError(f"{context}: shared-atoms reported no {field}")
    return totals


def validate_range_cache(value: Any, query: dict[str, Any], context: str) -> Any:
    try:
        result = schema8.validate_range_cache(value, query, context)
    except schema8.GateError as error:
        raise GateError(str(error)) from error
    if result is not None and result["configured_budget_bytes"] != 0:
        raise GateError(f"{context}: range scalar cache is not disabled")
    return result


def validate_raw(
    row: dict[str, str], query: dict[str, Any], args: argparse.Namespace
) -> tuple[str, list[dict[str, Any]]]:
    raw_path = Path(row["raw_output"])
    with raw_path.open(encoding="utf-8") as source:
        document = json.load(source)
    if not isinstance(document, dict) or set(document) != RAW_DOCUMENT_FIELDS:
        raise GateError(f"{raw_path}: raw document differs from the v9 contract")
    if document.get("schema") != common.RAW_SCHEMA:
        raise GateError(f"{raw_path}: raw schema must be {common.RAW_SCHEMA}")
    nonnegative_int(
        document.get("corpus_fingerprint_duration_ns"),
        f"{raw_path}.corpus_fingerprint_duration_ns",
    )
    configuration = document.get("configuration")
    if not isinstance(configuration, dict) or set(configuration) != CONFIGURATION_FIELDS:
        raise GateError(f"{raw_path}: configuration differs from the v9 contract")
    policy = row["query_label_storage"]
    expected_configuration = {
        "segments_dir": os.path.realpath(args.corpus),
        "start_ms": query["start_ms"],
        "end_ms": query["end_ms"],
        "mode": query["mode"] if query["mode"] == "instant" else "query_range",
        "step_ms": query["step_ms"],
        "range_scalar_cache_max_bytes": query["range_scalar_cache_max_bytes"],
        "chunk_read_mode": "pread",
        "chunk_read_queue_depth": args.queue_depth,
        "experimental_cross_segment_chunk_reads": False,
        "label_materialization": "full",
        "query_label_storage": policy,
        "storage_layout": "schema8",
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
        if not isinstance(run, dict) or set(run) != RAW_RUN_FIELDS:
            raise GateError(f"{context}: run differs from the v9 contract")
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
        payload = validate_counter_object(
            run.get("payload_reads"), schema8.PAYLOAD_FIELDS, f"{context}.payload_reads"
        )
        if payload["logical_used_bytes"] != stats["bytes_read"]:
            raise GateError(f"{context}: payload logical bytes differ from QueryStats")
        if payload["physical_bytes"] < payload["logical_used_bytes"]:
            raise GateError(f"{context}: physical payload bytes are below logical bytes")
        labels = validate_full_materialization(
            run.get("label_materialization"), f"{context}.label_materialization"
        )
        label_storage = validate_label_storage(
            run.get("query_label_storage"), policy, context
        )
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
                "result_series": nonnegative_int(
                    run.get("result_series"), f"{context}.result_series"
                ),
                "result_samples": nonnegative_int(
                    run.get("result_samples"), f"{context}.result_samples"
                ),
                "stats": stats,
                "payload": payload,
                "labels": labels,
                "query_label_storage": label_storage,
                "symbols": validate_symbol_reads(
                    run.get("symbol_reads"), f"{context}.symbol_reads"
                ),
                "range_cache": validate_range_cache(
                    run.get("range_scalar_cache"), query, context
                ),
            }
        )
    validate_process_atom_activity(policy, query["category"], validated, str(raw_path))
    return corpus_fingerprint, validated


def exact_equal(left: Any, right: Any, field: str, context: str) -> None:
    if left != right:
        raise GateError(f"{context}: {field} differs between OwnedStrings and SharedAtoms")


def compare_equivalent_runs(
    owned: dict[str, Any], shared: dict[str, Any], context: str
) -> None:
    for field in (
        "semantic_fingerprint",
        "portable_fingerprint",
        "result_series",
        "result_samples",
        "stats",
        "payload",
        "labels",
        "range_cache",
    ):
        exact_equal(owned[field], shared[field], field, context)
    exact_equal(
        comparable_symbol_reads(owned["symbols"]),
        comparable_symbol_reads(shared["symbols"]),
        "non-timing symbol/integrity counters",
        context,
    )


def payload_amplification(payload: dict[str, int]) -> str:
    logical = payload["logical_used_bytes"]
    if logical == 0:
        return ""
    return f"{payload['physical_bytes'] / logical:.6f}"


def compare_results(args: argparse.Namespace) -> None:
    queries = read_manifest(args.manifest)
    query_by_name = {query["query_name"]: query for query in queries}
    rows = read_index(args.index)
    expected_processes = len(queries) * args.repeats * len(POLICIES)
    if len(rows) != expected_processes:
        raise GateError(f"expected {expected_processes} processes, found {len(rows)}")
    processes: dict[tuple[str, int, str], dict[str, Any]] = {}
    process_labels: set[str] = set()
    corpus_fingerprints: set[str] = set()
    for row in rows:
        query = query_by_name.get(row["query_name"])
        if query is None:
            raise GateError(f"raw index names unknown query {row['query_name']!r}")
        if row["category"] != query["category"] or row["mode"] != query["mode"]:
            raise GateError(f"raw index metadata differs for query {row['query_name']}")
        if os.path.realpath(row["corpus"]) != os.path.realpath(args.corpus):
            raise GateError(f"{row['process_label']}: corpus differs from the pinned corpus")
        repetition = positive_int(int(row["repetition"]), "repetition")
        if repetition > args.repeats:
            raise GateError(f"repetition exceeds configured count: {repetition}")
        order_index = positive_int(int(row["order_index"]), "order_index")
        policy = row["query_label_storage"]
        expected_order = POLICIES if repetition % 2 else tuple(reversed(POLICIES))
        if order_index not in (1, 2) or policy != expected_order[order_index - 1]:
            raise GateError(
                f"policy order was not alternated for {row['query_name']} repetition {repetition}"
            )
        if row["process_label"] in process_labels:
            raise GateError(f"duplicate process label: {row['process_label']}")
        process_labels.add(row["process_label"])
        for field in (
            "process_wall_seconds",
            "process_user_seconds",
            "process_system_seconds",
        ):
            nonnegative_decimal(row[field], f"{row['process_label']}.{field}")
        positive_int(int(row["max_rss_kib"]), f"{row['process_label']}.max_rss_kib")
        key = (row["query_name"], repetition, policy)
        if key in processes:
            raise GateError(f"duplicate completed process: {key!r}")
        fingerprint, runs = validate_raw(row, query, args)
        corpus_fingerprints.add(fingerprint)
        processes[key] = {"row": row, "runs": {run["run_index"]: run for run in runs}}
    if len(corpus_fingerprints) != 1:
        raise GateError("the Schema 8 corpus fingerprint changed during the A/B")

    comparisons: list[dict[str, Any]] = []
    deterministic_storage: dict[tuple[str, str, int], dict[str, int]] = {}
    for query in queries:
        query_name = query["query_name"]
        for repetition in range(1, args.repeats + 1):
            owned = processes[(query_name, repetition, "owned-strings")]["runs"]
            shared = processes[(query_name, repetition, "shared-atoms")]["runs"]
            exact_equal(owned.keys(), shared.keys(), "run identities", query_name)
            for run_index in sorted(owned):
                context = f"{query_name} repetition {repetition} run {run_index}"
                compare_equivalent_runs(owned[run_index], shared[run_index], context)
                for policy, run in (("owned-strings", owned[run_index]), ("shared-atoms", shared[run_index])):
                    stable_key = (query_name, policy, run_index)
                    prior = deterministic_storage.setdefault(
                        stable_key, run["query_label_storage"]
                    )
                    exact_equal(prior, run["query_label_storage"], "per-policy atom counters across repetitions", context)
                comparisons.append(
                    {
                        "query_name": query_name,
                        "repetition": repetition,
                        "run_index": run_index,
                        "run_kind": owned[run_index]["run_kind"],
                        "semantic_fingerprint": owned[run_index]["semantic_fingerprint"],
                        "portable_fingerprint": owned[run_index]["portable_fingerprint"],
                        "query_stats_sha256": canonical_digest(owned[run_index]["stats"]),
                        "payload_reads_sha256": canonical_digest(owned[run_index]["payload"]),
                        "label_integrity_sha256": canonical_digest(owned[run_index]["labels"]),
                        "symbol_counters_sha256": canonical_digest(
                            comparable_symbol_reads(owned[run_index]["symbols"])
                        ),
                        "owned_label_storage": owned[run_index]["query_label_storage"],
                        "shared_label_storage": shared[run_index]["query_label_storage"],
                    }
                )

    summary_fields = [
        "process_label", "query_name", "category", "mode", "repetition", "order_index",
        "query_label_storage", "run_index", "run_kind", "duration_ns",
        "process_wall_seconds", "process_user_seconds", "process_system_seconds", "max_rss_kib",
        "result_series", "result_samples", "semantic_fingerprint", "portable_fingerprint",
        "query_stats_sha256", *(f"stats_{field}" for field in common.QUERY_STATS_FIELDS),
        "payload_logical_used_bytes", "payload_physical_reads", "payload_physical_bytes",
        "payload_read_used_amplification", *(f"labels_{field}" for field in sorted(schema8.LABEL_MATERIALIZATION_FIELDS)),
        *(f"label_storage_{field}" for field in sorted(common.QUERY_LABEL_STORAGE_FIELDS)),
        "range_cache_configured_budget_bytes", "range_cache_hits", "range_cache_misses",
        "range_cache_peak_retained_charge_bytes", "range_cache_retained_charge_after_finalize",
        "range_cache_process_governor_peak_leased_bytes", "symbols_page_read_calls",
        "symbols_page_read_bytes", "symbols_page_validation_calls", "symbols_page_validation_bytes",
        "symbols_page_cache_hits", "symbols_page_cache_misses", "symbols_total_retained_charge_bytes",
    ]
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=summary_fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for query in queries:
            for repetition in range(1, args.repeats + 1):
                order = POLICIES if repetition % 2 else tuple(reversed(POLICIES))
                for policy in order:
                    process = processes[(query["query_name"], repetition, policy)]
                    index_row = process["row"]
                    for run_index in range(args.benchmark_repeats):
                        run = process["runs"][run_index]
                        cache = run["range_cache"] or {}
                        symbols = run["symbols"]
                        row: dict[str, Any] = {
                            "process_label": index_row["process_label"], "query_name": query["query_name"],
                            "category": query["category"], "mode": query["mode"], "repetition": repetition,
                            "order_index": index_row["order_index"], "query_label_storage": policy,
                            "run_index": run_index, "run_kind": run["run_kind"], "duration_ns": run["duration_ns"],
                            "process_wall_seconds": index_row["process_wall_seconds"],
                            "process_user_seconds": index_row["process_user_seconds"],
                            "process_system_seconds": index_row["process_system_seconds"],
                            "max_rss_kib": index_row["max_rss_kib"], "result_series": run["result_series"],
                            "result_samples": run["result_samples"], "semantic_fingerprint": run["semantic_fingerprint"],
                            "portable_fingerprint": run["portable_fingerprint"], "query_stats_sha256": canonical_digest(run["stats"]),
                            "payload_logical_used_bytes": run["payload"]["logical_used_bytes"],
                            "payload_physical_reads": run["payload"]["physical_reads"],
                            "payload_physical_bytes": run["payload"]["physical_bytes"],
                            "payload_read_used_amplification": payload_amplification(run["payload"]),
                            "range_cache_configured_budget_bytes": cache.get("configured_budget_bytes", ""),
                            "range_cache_hits": cache.get("hits", ""), "range_cache_misses": cache.get("misses", ""),
                            "range_cache_peak_retained_charge_bytes": cache.get("peak_retained_charge_bytes", ""),
                            "range_cache_retained_charge_after_finalize": cache.get("retained_charge_after_finalize", ""),
                            "range_cache_process_governor_peak_leased_bytes": cache.get("process_governor_peak_leased_bytes", ""),
                            "symbols_page_read_calls": symbols["page_read_delta"]["calls"],
                            "symbols_page_read_bytes": symbols["page_read_delta"]["bytes"],
                            "symbols_page_validation_calls": symbols["page_validation_delta"]["calls"],
                            "symbols_page_validation_bytes": symbols["page_validation_delta"]["bytes"],
                            "symbols_page_cache_hits": symbols["page_cache_hits_delta"],
                            "symbols_page_cache_misses": symbols["page_cache_misses_delta"],
                            "symbols_total_retained_charge_bytes": symbols["total_retained_charge_bytes_after_run"],
                        }
                        row.update({f"stats_{field}": run["stats"][field] for field in common.QUERY_STATS_FIELDS})
                        row.update({f"labels_{field}": run["labels"][field] for field in schema8.LABEL_MATERIALIZATION_FIELDS})
                        row.update({f"label_storage_{field}": run["query_label_storage"][field] for field in common.QUERY_LABEL_STORAGE_FIELDS})
                        writer.writerow(row)

    result = {
        "schema": EQUIVALENCE_SCHEMA,
        "canonical_equivalence": "pass",
        "exact_gates": [
            "semantic_and_portable_fingerprints", "result_shape", "all_QueryStats_fields",
            "payload_logical_and_physical_counters", "full_label_materialization_and_integrity_counters",
            "range_scalar_cache_counters", "non_timing_symbol_read_integrity_and_cache_counters",
        ],
        "intentional_policy_differences": [
            "query_label_storage_atom_counters", "duration", "process_CPU", "maximum_RSS",
            "symbol_page_validation_elapsed_nanoseconds",
        ],
        "atom_reconciliation": "every run requires atom_lookups == atom_hits + atom_misses; owned atom activity is zero; shared stress queries require hits, misses, and unique bytes",
        "matching_runs_compared": len(comparisons), "repeats": args.repeats,
        "benchmark_repeats": args.benchmark_repeats,
        "corpus_fingerprint": next(iter(corpus_fingerprints)),
        "query_names": [query["query_name"] for query in queries], "comparisons": comparisons,
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
    compare.add_argument("--corpus", type=Path, required=True)
    compare.add_argument("--repeats", type=int, required=True)
    compare.add_argument("--benchmark-repeats", type=int, required=True)
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
        if args.command == "normalize-manifest":
            default_cache = nonnegative_int(args.default_range_cache_bytes, "default range cache bytes")
            queries = schema8.normalize_manifest(args.input, default_cache)
            categories = {query["category"] for query in queries}
            missing = REQUIRED_CATEGORIES - categories
            if missing:
                raise GateError(f"manifest lacks required categories: {sorted(missing)!r}")
            if any(query["mode"] == "range" and query["range_scalar_cache_max_bytes"] != 0 for query in queries):
                raise GateError("every range query must disable the scalar range cache")
            schema8.write_normalized_manifest(queries, args.output_tsv, args.output_json)
        elif args.command == "inventory":
            common.write_inventory(args.corpus, args.output, args.paths_output)
        elif args.command == "compare-results":
            positive_int(args.repeats, "repeats")
            if positive_int(args.benchmark_repeats, "benchmark repeats") != 2:
                raise GateError("benchmark repeats must be exactly 2")
            compare_results(args)
        else:
            raise AssertionError(args.command)
    except (
        GateError, common.GateError, schema8.GateError, OSError, TypeError,
        ValueError, KeyError, json.JSONDecodeError,
    ) as error:
        print(f"query-label-storage A/B gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
