#!/usr/bin/env python3
"""Strict pre-instrumentation/current-Off same-corpus query A/B gate.

The reference binary emits raw schema v9.  The candidate binary emits raw
schema v10 and must report ``query_instrumentation=off`` with zero detailed
stage time.  This gate deliberately compares only one immutable Schema 8
corpus and treats every public semantic/counter difference as a failure.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import statistics
import sys
from pathlib import Path
from typing import Any

import schema7_query_ab_gate as common
import schema8_query_ab_gate as manifest_gate


REFERENCE_RAW_SCHEMA = "chronoxide.query-benchmark.raw/v9"
CANDIDATE_RAW_SCHEMA = "chronoxide.query-benchmark.raw/v10"
COMPARISON_SCHEMA = "chronoxide/query-instrumentation-off-ab/v1"
ROLES = ("reference", "candidate")
ABBA = ("reference", "candidate", "candidate", "reference")
QUERY_STATS_FIELDS = common.QUERY_STATS_FIELDS

DOCUMENT_FIELDS = {
    "schema",
    "corpus_fingerprint_sha256",
    "corpus_fingerprint_duration_ns",
    "configuration",
    "limits",
    "runs",
}
CONFIGURATION_V9_FIELDS = common.CONFIGURATION_FIELDS - {"query_instrumentation"}
CONFIGURATION_V10_FIELDS = common.CONFIGURATION_FIELDS
RUN_V9_FIELDS = {
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
    "query_label_storage",
    "range_scalar_cache",
}
RUN_V10_FIELDS = RUN_V9_FIELDS | {
    "post_query_fingerprint_ns",
    "query_stages",
    "metadata_runtime",
}
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
RANGE_CACHE_STABLE_FIELDS = frozenset(
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
    }
)
RANGE_CACHE_REFERENCE_FIELDS = RANGE_CACHE_STABLE_FIELDS | {
    "process_governor_peak_leased_bytes"
}
RANGE_CACHE_CANDIDATE_FIELDS = RANGE_CACHE_STABLE_FIELDS | {
    "process_governor_lifetime_peak_leased_bytes"
}
INDEX_FIELDS = {
    "process_label",
    "query_name",
    "category",
    "mode",
    "block",
    "order_index",
    "role",
    "binary_sha256",
    "corpus",
    "raw_output",
    "process_wall_seconds",
    "process_user_seconds",
    "process_system_seconds",
    "max_rss_kib",
}
BINARY_FIELDS = {"role", "source_path", "preserved_path", "sha256"}
SOURCE_FIELDS = {
    "role",
    "source_root",
    "head_commit",
    "head_tree",
    "source_state_sha256",
    "tracked_patch_sha256",
    "status_sha256",
}


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


def finite_nonnegative_float(value: Any, name: str) -> float:
    try:
        converted = float(value)
    except (TypeError, ValueError) as error:
        raise GateError(f"{name} must be a finite non-negative number") from error
    if not math.isfinite(converted) or converted < 0:
        raise GateError(f"{name} must be a finite non-negative number")
    return converted


def read_tsv(path: Path, fields: set[str], context: str) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if not rows or any(set(row) != fields for row in rows):
        raise GateError(f"{context} TSV has an invalid shape")
    return rows


def validate_provenance(
    binaries_path: Path, sources_path: Path
) -> dict[str, str]:
    binary_rows = read_tsv(binaries_path, BINARY_FIELDS, "binary provenance")
    source_rows = read_tsv(sources_path, SOURCE_FIELDS, "source provenance")
    if len(binary_rows) != len(ROLES) or {row["role"] for row in binary_rows} != set(ROLES):
        raise GateError("binary provenance must contain reference and candidate exactly once")
    if len(source_rows) != len(ROLES) or {row["role"] for row in source_rows} != set(ROLES):
        raise GateError("source provenance must contain reference and candidate exactly once")

    binary_hashes: dict[str, str] = {}
    for row in binary_rows:
        role = row["role"]
        binary_hashes[role] = digest(row["sha256"], f"{role} binary sha256")
        if not os.path.isabs(row["source_path"]) or not os.path.isabs(row["preserved_path"]):
            raise GateError(f"{role} binary paths must be absolute")
    if len(set(binary_hashes.values())) != len(ROLES):
        raise GateError("reference and candidate binaries must have distinct SHA-256 digests")

    for row in source_rows:
        role = row["role"]
        if not os.path.isabs(row["source_root"]):
            raise GateError(f"{role} source root must be absolute")
        if len(row["head_commit"]) != 40 or any(
            character not in "0123456789abcdef" for character in row["head_commit"]
        ):
            raise GateError(f"{role} head commit must be a lowercase Git object id")
        if len(row["head_tree"]) != 40 or any(
            character not in "0123456789abcdef" for character in row["head_tree"]
        ):
            raise GateError(f"{role} head tree must be a lowercase Git object id")
        for field in ("source_state_sha256", "tracked_patch_sha256", "status_sha256"):
            digest(row[field], f"{role} {field}")
    return binary_hashes


def validate_counter_object(
    value: Any, fields: frozenset[str], context: str
) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != fields:
        raise GateError(f"{context} has an invalid shape")
    return {
        field: nonnegative_int(value[field], f"{context}.{field}")
        for field in fields
    }


def validate_range_cache(
    value: Any, role: str, query: dict[str, Any], context: str
) -> dict[str, Any] | None:
    if query["mode"] == "instant":
        if value is not None:
            raise GateError(f"{context}: instant query unexpectedly has range-cache stats")
        return None
    fields = (
        RANGE_CACHE_REFERENCE_FIELDS
        if role == "reference"
        else RANGE_CACHE_CANDIDATE_FIELDS
    )
    if not isinstance(value, dict) or set(value) != fields:
        raise GateError(f"{context}: range-cache stats have an invalid {role} shape")
    if value["configured_budget_bytes"] != query["range_scalar_cache_max_bytes"]:
        raise GateError(f"{context}: range-cache budget differs from the manifest")
    normalized: dict[str, Any] = {}
    for field, field_value in value.items():
        if field in {"governor_refused", "allocation_refused", "layout_overflow"}:
            if not isinstance(field_value, bool):
                raise GateError(f"{context}.range_scalar_cache.{field} must be boolean")
            normalized[field] = field_value
        else:
            normalized[field] = nonnegative_int(
                field_value, f"{context}.range_scalar_cache.{field}"
            )
    peak_field = (
        "process_governor_peak_leased_bytes"
        if role == "reference"
        else "process_governor_lifetime_peak_leased_bytes"
    )
    normalized["process_governor_lifetime_peak_leased_bytes"] = normalized.pop(
        peak_field
    )
    return normalized


def expected_configuration(
    row: dict[str, str], query: dict[str, Any], args: argparse.Namespace
) -> dict[str, Any]:
    result = {
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
        "storage_layout": "schema8",
        "benchmark_repeats": args.benchmark_repeats,
        "queries": [query["expression"]],
        "prewarm_query_contexts": False,
        "prefetch_query_data": False,
        "exponential_histogram_bucket_boundaries": query["boundaries"],
        "requested_segment_footer_validation": False,
        "effective_segment_footer_validation": False,
    }
    if row["role"] == "candidate":
        result["query_instrumentation"] = "off"
    return result


def validate_raw(
    row: dict[str, str], query: dict[str, Any], args: argparse.Namespace
) -> tuple[str, list[dict[str, Any]]]:
    raw_path = Path(row["raw_output"])
    with raw_path.open(encoding="utf-8") as source:
        document = json.load(source)
    if not isinstance(document, dict) or set(document) != DOCUMENT_FIELDS:
        raise GateError(f"{raw_path}: raw document has an invalid shape")
    role = row["role"]
    expected_schema = REFERENCE_RAW_SCHEMA if role == "reference" else CANDIDATE_RAW_SCHEMA
    if document["schema"] != expected_schema:
        raise GateError(f"{raw_path}: {role} raw schema must be {expected_schema}")
    configuration = document["configuration"]
    configuration_fields = (
        CONFIGURATION_V9_FIELDS if role == "reference" else CONFIGURATION_V10_FIELDS
    )
    if not isinstance(configuration, dict) or set(configuration) != configuration_fields:
        raise GateError(f"{raw_path}: configuration differs from the {role} contract")
    expected = expected_configuration(row, query, args)
    if configuration != expected:
        raise GateError(
            f"{raw_path}: timed configuration differs from the pinned invocation: "
            f"expected={expected!r} actual={configuration!r}"
        )
    expected_limits = {
        "max_matched_series": args.max_matched_series,
        "max_projected_series": args.max_projected_series,
        "max_chunk_reads": args.max_chunk_reads,
        "max_bytes_read": args.max_bytes_read,
        "max_samples_decoded": args.max_samples_decoded,
        "max_regex_values_examined": args.max_regex_values_examined,
    }
    if document["limits"] != expected_limits:
        raise GateError(f"{raw_path}: query limits differ from the pinned invocation")
    fingerprint = digest(
        document["corpus_fingerprint_sha256"], f"{raw_path}.corpus_fingerprint"
    )
    nonnegative_int(
        document["corpus_fingerprint_duration_ns"],
        f"{raw_path}.corpus_fingerprint_duration_ns",
    )
    runs = document["runs"]
    if not isinstance(runs, list) or len(runs) != args.benchmark_repeats:
        raise GateError(f"{raw_path}: expected {args.benchmark_repeats} runs")

    validated: list[dict[str, Any]] = []
    expected_run_fields = RUN_V9_FIELDS if role == "reference" else RUN_V10_FIELDS
    for run_index, run in enumerate(runs):
        context = f"{raw_path}:{query['query_name']}:{run_index}"
        if not isinstance(run, dict) or set(run) != expected_run_fields:
            raise GateError(f"{context}: run has an invalid {role} shape")
        expected_kind = "cold" if run_index == 0 else "warm"
        if run["query"] != query["expression"]:
            raise GateError(f"{context}: expression differs from the manifest")
        if run["run_index"] != run_index or run["run_kind"] != expected_kind:
            raise GateError(f"{context}: run index/kind is invalid")
        if (
            run["effective_start_ms"] != query["start_ms"]
            or run["effective_end_ms"] != query["end_ms"]
            or run["step_ms"] != query["step_ms"]
        ):
            raise GateError(f"{context}: effective evaluation range differs")
        try:
            stats = common.validate_stats(run["stats"], context)
            label_storage = common.validate_query_label_storage(
                run["query_label_storage"], context, "owned-strings"
            )
        except common.GateError as error:
            raise GateError(str(error)) from error
        payload = validate_counter_object(
            run["payload_reads"], PAYLOAD_FIELDS, f"{context}.payload_reads"
        )
        if payload["logical_used_bytes"] != stats["bytes_read"]:
            raise GateError(f"{context}: payload logical bytes differ from QueryStats")
        if payload["physical_bytes"] < payload["logical_used_bytes"]:
            raise GateError(f"{context}: physical payload bytes are below logical bytes")
        labels = validate_counter_object(
            run["label_materialization"],
            LABEL_MATERIALIZATION_FIELDS,
            f"{context}.label_materialization",
        )
        if not isinstance(run["symbol_reads"], dict):
            raise GateError(f"{context}: symbol read counters are missing")
        duration_ns = positive_int(run["duration_ns"], f"{context}.duration_ns")
        if role == "candidate":
            nonnegative_int(
                run["post_query_fingerprint_ns"],
                f"{context}.post_query_fingerprint_ns",
            )
            try:
                common.validate_query_stages(
                    run["query_stages"], "off", duration_ns, context
                )
            except common.GateError as error:
                raise GateError(str(error)) from error
            if not isinstance(run["metadata_runtime"], dict):
                raise GateError(f"{context}: metadata runtime report is missing")
        validated.append(
            {
                "run_index": run_index,
                "run_kind": expected_kind,
                "duration_ns": duration_ns,
                "semantic_fingerprint": digest(
                    run["semantic_fingerprint_sha256"],
                    f"{context}.semantic_fingerprint",
                ),
                "portable_fingerprint": digest(
                    run["portable_semantic_fingerprint_sha256"],
                    f"{context}.portable_fingerprint",
                ),
                "result_series": nonnegative_int(
                    run["result_series"], f"{context}.result_series"
                ),
                "result_samples": nonnegative_int(
                    run["result_samples"], f"{context}.result_samples"
                ),
                "stats": stats,
                "payload": payload,
                "labels": labels,
                "label_storage": label_storage,
                "range_cache": validate_range_cache(
                    run["range_scalar_cache"], role, query, context
                ),
            }
        )
    return fingerprint, validated


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def ratio(candidate: float, reference: float, context: str) -> float:
    if reference <= 0:
        raise GateError(f"{context}: reference median must be positive")
    return candidate / reference


def median(values: list[int | float], context: str) -> float:
    if not values:
        raise GateError(f"{context}: no observations")
    return float(statistics.median(values))


def compare_results(args: argparse.Namespace) -> None:
    queries = manifest_gate.read_normalized_manifest(args.manifest)
    query_by_name = {query["query_name"]: query for query in queries}
    if args.broad_query_name not in query_by_name:
        raise GateError(
            f"broad query {args.broad_query_name!r} is absent from the frozen manifest"
        )
    binary_hashes = validate_provenance(args.binaries, args.sources)
    rows = read_tsv(args.index, INDEX_FIELDS, "raw index")
    expected_processes = len(queries) * args.blocks * len(ABBA)
    if len(rows) != expected_processes:
        raise GateError(
            f"expected {expected_processes} completed processes, found {len(rows)}"
        )

    processes: dict[tuple[str, int, int], dict[str, Any]] = {}
    process_labels: set[str] = set()
    corpus_fingerprints: set[str] = set()
    for row in rows:
        query_name = row["query_name"]
        query = query_by_name.get(query_name)
        if query is None:
            raise GateError(f"raw index names unknown query {query_name!r}")
        if row["category"] != query["category"] or row["mode"] != query["mode"]:
            raise GateError(f"raw index metadata differs for query {query_name}")
        block = positive_int(int(row["block"]), "block")
        order_index = positive_int(int(row["order_index"]), "order_index")
        if block > args.blocks or order_index > len(ABBA):
            raise GateError(f"invalid block/order for {query_name}: {block}/{order_index}")
        role = row["role"]
        if role != ABBA[order_index - 1]:
            raise GateError(
                f"{query_name} block {block} did not follow "
                "reference-candidate-candidate-reference"
            )
        if row["binary_sha256"] != binary_hashes[role]:
            raise GateError(f"{row['process_label']}: binary digest differs from provenance")
        if os.path.realpath(row["corpus"]) != os.path.realpath(args.corpus):
            raise GateError(f"{row['process_label']}: corpus differs from the pinned corpus")
        if row["process_label"] in process_labels:
            raise GateError(f"duplicate process label: {row['process_label']}")
        process_labels.add(row["process_label"])
        key = (query_name, block, order_index)
        if key in processes:
            raise GateError(f"duplicate completed process: {key!r}")
        max_rss_kib = positive_int(int(row["max_rss_kib"]), "max_rss_kib")
        process_times = {
            field: finite_nonnegative_float(row[field], f"{row['process_label']}.{field}")
            for field in (
                "process_wall_seconds",
                "process_user_seconds",
                "process_system_seconds",
            )
        }
        fingerprint, runs = validate_raw(row, query, args)
        corpus_fingerprints.add(fingerprint)
        processes[key] = {
            "index": row,
            "role": role,
            "runs": runs,
            "max_rss_kib": max_rss_kib,
            **process_times,
        }

    if len(corpus_fingerprints) != 1:
        raise GateError(
            "the query binary corpus fingerprint changed across the same-corpus A/B"
        )
    for query in queries:
        for block in range(1, args.blocks + 1):
            for order_index in range(1, len(ABBA) + 1):
                if (query["query_name"], block, order_index) not in processes:
                    raise GateError(
                        f"missing process for {query['query_name']} block {block} order {order_index}"
                    )

    # Correctness is global rather than merely pairwise: all ABBA observations
    # for a given logical run must have one canonical result and counter shape.
    correctness_fields = (
        "semantic_fingerprint",
        "portable_fingerprint",
        "result_series",
        "result_samples",
        "stats",
        "payload",
        "labels",
        "label_storage",
        "range_cache",
    )
    correctness_digests: dict[tuple[str, int], str] = {}
    for query in queries:
        query_name = query["query_name"]
        for run_index in range(args.benchmark_repeats):
            observations = [
                process["runs"][run_index]
                for key, process in processes.items()
                if key[0] == query_name
            ]
            canonical = {
                field: observations[0][field] for field in correctness_fields
            }
            for observation in observations[1:]:
                for field in correctness_fields:
                    if observation[field] != canonical[field]:
                        raise GateError(
                            f"{query_name} run {run_index}: {field} differs "
                            "across binaries/ABBA observations"
                        )
            correctness_digests[(query_name, run_index)] = canonical_digest(canonical)

    summary_fields = [
        "process_label",
        "query_name",
        "category",
        "mode",
        "block",
        "order_index",
        "role",
        "binary_sha256",
        "run_index",
        "run_kind",
        "duration_ns",
        "process_wall_seconds",
        "process_user_seconds",
        "process_system_seconds",
        "max_rss_kib",
        "result_series",
        "result_samples",
        "semantic_fingerprint",
        "portable_fingerprint",
        "correctness_sha256",
        *(f"stats_{field}" for field in QUERY_STATS_FIELDS),
        "payload_logical_used_bytes",
        "payload_physical_reads",
        "payload_physical_bytes",
        "payload_read_over_used",
    ]
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination, fieldnames=summary_fields, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for query in queries:
            query_name = query["query_name"]
            for block in range(1, args.blocks + 1):
                for order_index in range(1, len(ABBA) + 1):
                    process = processes[(query_name, block, order_index)]
                    index_row = process["index"]
                    for run in process["runs"]:
                        payload = run["payload"]
                        logical = payload["logical_used_bytes"]
                        row: dict[str, Any] = {
                            field: index_row[field]
                            for field in (
                                "process_label",
                                "query_name",
                                "category",
                                "mode",
                                "block",
                                "order_index",
                                "role",
                                "binary_sha256",
                            )
                        }
                        row.update(
                            {
                                "run_index": run["run_index"],
                                "run_kind": run["run_kind"],
                                "duration_ns": run["duration_ns"],
                                "process_wall_seconds": process["process_wall_seconds"],
                                "process_user_seconds": process["process_user_seconds"],
                                "process_system_seconds": process["process_system_seconds"],
                                "max_rss_kib": process["max_rss_kib"],
                                "result_series": run["result_series"],
                                "result_samples": run["result_samples"],
                                "semantic_fingerprint": run["semantic_fingerprint"],
                                "portable_fingerprint": run["portable_fingerprint"],
                                "correctness_sha256": correctness_digests[
                                    (query_name, run["run_index"])
                                ],
                                "payload_logical_used_bytes": logical,
                                "payload_physical_reads": payload["physical_reads"],
                                "payload_physical_bytes": payload["physical_bytes"],
                                "payload_read_over_used": (
                                    "" if logical == 0 else payload["physical_bytes"] / logical
                                ),
                            }
                        )
                        row.update(
                            {
                                f"stats_{field}": run["stats"][field]
                                for field in QUERY_STATS_FIELDS
                            }
                        )
                        writer.writerow(row)

    performance: list[dict[str, Any]] = []
    failures: list[str] = []
    for query in queries:
        query_name = query["query_name"]
        for run_kind in ("cold", "warm"):
            role_values: dict[str, list[int]] = {role: [] for role in ROLES}
            for key, process in processes.items():
                if key[0] != query_name:
                    continue
                role_values[process["role"]].extend(
                    run["duration_ns"]
                    for run in process["runs"]
                    if run["run_kind"] == run_kind
                )
            reference_median = median(
                role_values["reference"], f"{query_name} {run_kind} reference"
            )
            candidate_median = median(
                role_values["candidate"], f"{query_name} {run_kind} candidate"
            )
            measured_ratio = ratio(
                candidate_median, reference_median, f"{query_name} {run_kind}"
            )
            threshold_pct = (
                args.broad_max_regression_pct
                if query_name == args.broad_query_name
                else args.general_max_regression_pct
            )
            passed = measured_ratio <= 1.0 + threshold_pct / 100.0
            if not passed:
                failures.append(
                    f"{query_name} {run_kind} latency ratio {measured_ratio:.6f} exceeds "
                    f"{1.0 + threshold_pct / 100.0:.6f}"
                )
            performance.append(
                {
                    "metric": "query_duration_ns",
                    "query_name": query_name,
                    "run_kind": run_kind,
                    "reference_observations": role_values["reference"],
                    "candidate_observations": role_values["candidate"],
                    "reference_median": reference_median,
                    "candidate_median": candidate_median,
                    "candidate_over_reference": measured_ratio,
                    "max_regression_pct": threshold_pct,
                    "gate": "pass" if passed else "fail",
                }
            )

        rss_values: dict[str, list[int]] = {role: [] for role in ROLES}
        for key, process in processes.items():
            if key[0] == query_name:
                rss_values[process["role"]].append(process["max_rss_kib"])
        reference_rss = median(rss_values["reference"], f"{query_name} RSS reference")
        candidate_rss = median(rss_values["candidate"], f"{query_name} RSS candidate")
        rss_ratio = ratio(candidate_rss, reference_rss, f"{query_name} RSS")
        rss_passed = rss_ratio <= 1.0 + args.rss_max_regression_pct / 100.0
        if not rss_passed:
            failures.append(
                f"{query_name} RSS ratio {rss_ratio:.6f} exceeds "
                f"{1.0 + args.rss_max_regression_pct / 100.0:.6f}"
            )
        performance.append(
            {
                "metric": "process_max_rss_kib",
                "query_name": query_name,
                "run_kind": None,
                "reference_observations": rss_values["reference"],
                "candidate_observations": rss_values["candidate"],
                "reference_median": reference_rss,
                "candidate_median": candidate_rss,
                "candidate_over_reference": rss_ratio,
                "max_regression_pct": args.rss_max_regression_pct,
                "gate": "pass" if rss_passed else "fail",
            }
        )

    result = {
        "schema": COMPARISON_SCHEMA,
        "correctness_gate": "pass",
        "performance_gate": "pass" if not failures else "fail",
        "schedule": list(ABBA),
        "blocks": args.blocks,
        "benchmark_repeats": args.benchmark_repeats,
        "broad_query_name": args.broad_query_name,
        "thresholds": {
            "broad_query_cold_and_warm_max_regression_pct": args.broad_max_regression_pct,
            "other_query_cold_and_warm_max_regression_pct": args.general_max_regression_pct,
            "per_query_process_median_rss_max_regression_pct": args.rss_max_regression_pct,
        },
        "binary_sha256": binary_hashes,
        "corpus_fingerprint_sha256": next(iter(corpus_fingerprints)),
        "query_names": [query["query_name"] for query in queries],
        "performance": performance,
        "failures": failures,
    }
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")
    if failures:
        raise GateError("performance regression gate failed: " + "; ".join(failures))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    normalize = commands.add_parser("normalize-manifest")
    normalize.add_argument("--input", type=Path, required=True)
    normalize.add_argument("--output-tsv", type=Path, required=True)
    normalize.add_argument("--output-json", type=Path, required=True)
    normalize.add_argument("--default-range-cache-bytes", type=int, required=True)

    inventory = commands.add_parser("inventory")
    inventory.add_argument("--corpus", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--paths-output", type=Path, required=True)

    compare = commands.add_parser("compare-results")
    compare.add_argument("--index", type=Path, required=True)
    compare.add_argument("--manifest", type=Path, required=True)
    compare.add_argument("--binaries", type=Path, required=True)
    compare.add_argument("--sources", type=Path, required=True)
    compare.add_argument("--corpus", type=Path, required=True)
    compare.add_argument("--summary", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--broad-query-name", required=True)
    compare.add_argument("--blocks", type=int, required=True)
    compare.add_argument("--benchmark-repeats", type=int, required=True)
    compare.add_argument("--queue-depth", type=int, required=True)
    compare.add_argument(
        "--label-materialization",
        choices=("full", "demand-driven"),
        required=True,
    )
    compare.add_argument("--max-matched-series", type=int, required=True)
    compare.add_argument("--max-projected-series", type=int, required=True)
    compare.add_argument("--max-chunk-reads", type=int, required=True)
    compare.add_argument("--max-bytes-read", type=int, required=True)
    compare.add_argument("--max-samples-decoded", type=int, required=True)
    compare.add_argument("--max-regex-values-examined", type=int, required=True)
    compare.add_argument("--broad-max-regression-pct", type=float, required=True)
    compare.add_argument("--general-max-regression-pct", type=float, required=True)
    compare.add_argument("--rss-max-regression-pct", type=float, required=True)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "normalize-manifest":
            default_cache = nonnegative_int(
                args.default_range_cache_bytes, "default range cache bytes"
            )
            manifest_gate.write_normalized_manifest(
                manifest_gate.normalize_manifest(args.input, default_cache),
                args.output_tsv,
                args.output_json,
            )
        elif args.command == "inventory":
            common.write_inventory(args.corpus, args.output, args.paths_output)
        elif args.command == "compare-results":
            positive_int(args.blocks, "blocks")
            if args.benchmark_repeats < 2:
                raise GateError("benchmark repeats must be at least two")
            for name in (
                "broad_max_regression_pct",
                "general_max_regression_pct",
                "rss_max_regression_pct",
            ):
                finite_nonnegative_float(getattr(args, name), name)
            compare_results(args)
        else:
            raise AssertionError(args.command)
    except (GateError, common.GateError, manifest_gate.GateError, OSError, ValueError) as error:
        print(f"query instrumentation Off A/B gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
