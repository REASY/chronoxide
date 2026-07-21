#!/usr/bin/env python3
"""Strict same-v11-binary CompactIds/OwnedStrings real-corpus A/B gate."""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import os
import re
import statistics
import sys
from pathlib import Path
from typing import Any

import phase1_query_gate as phase1
import schema7_query_ab_gate as common
import schema8_query_ab_gate as manifest_gate


RAW_SCHEMA = "chronoxide.query-benchmark.raw/v11"
RESULT_SCHEMA = "chronoxide/storage-vnext-phase2-compact-ids-ab/v2"
SMOKE_SCHEMA = "chronoxide/storage-vnext-phase2-smoke-validation/v1"
POLICIES = ("owned-strings", "compact-ids")
ABBA = ("owned-strings", "compact-ids", "compact-ids", "owned-strings")
BAAB = ("compact-ids", "owned-strings", "owned-strings", "compact-ids")
DEFAULT_ARENA_BYTES = 512 * 1024 * 1024

REQUIRED_CATEGORIES = frozenset(
    {
        "broad-full-label-output",
        "equality-full-demand",
        "sparse-regex-full-demand",
        "negative-matcher-full-demand",
        "empty-result-control",
        "scalar-instant-selective",
        "scalar-range-selective",
        "native-histogram-count-selective",
        "native-histogram-full-control",
        "native-exponential-histogram-count-selective",
        "native-exponential-histogram-full-control",
    }
)
FULL_DEMAND_CATEGORIES = frozenset(
    {
        "broad-full-label-output",
        "equality-full-demand",
        "sparse-regex-full-demand",
        "negative-matcher-full-demand",
        "native-histogram-full-control",
        "native-exponential-histogram-full-control",
    }
)
SELECTIVE_CATEGORIES = frozenset(
    {
        "scalar-instant-selective",
        "scalar-range-selective",
        "native-histogram-count-selective",
        "native-exponential-histogram-count-selective",
    }
)
TYPED_FULL_CATEGORIES = frozenset(
    {
        "native-histogram-count-selective",
        "native-histogram-full-control",
        "native-exponential-histogram-count-selective",
        "native-exponential-histogram-full-control",
    }
)
DOCUMENT_FIELDS = {
    "schema",
    "corpus_fingerprint_sha256",
    "corpus_fingerprint_duration_ns",
    "configuration",
    "limits",
    "runs",
}
CONFIGURATION_FIELDS = common.CONFIGURATION_FIELDS | {"query_label_arena_max_bytes"}
RUN_FIELDS = {
    "query",
    "run_kind",
    "run_index",
    "duration_ns",
    "post_query_fingerprint_ns",
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
    "query_stages",
    "metadata_runtime",
    "range_scalar_cache",
}
PAYLOAD_FIELDS = frozenset({"logical_used_bytes", "physical_reads", "physical_bytes"})
LABEL_FIELDS = frozenset(
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
LABEL_STORAGE_FIELDS = frozenset(
    {
        "label_sets",
        "atom_lookups",
        "atom_hits",
        "atom_misses",
        "unique_content_bytes",
        "compact_label_sets",
        "compact_pairs",
        "compact_source_symbol_translations",
        "compact_source_symbol_translation_hits",
        "compact_source_symbol_translation_misses",
        "compact_atom_lookups",
        "compact_atom_hits",
        "compact_atom_misses",
        "compact_unique_strings",
        "compact_unique_content_bytes",
        "compact_arena_budget_bytes",
        "compact_arena_current_bytes",
        "compact_arena_peak_bytes",
        "compact_atom_bytes",
        "compact_pair_bytes",
        "compact_hash_directory_bytes",
        "compact_translation_bytes",
        "compact_retained_bytes",
        "compact_arena_admission_refusals",
        "compact_compatibility_materializations",
    }
)
COMPACT_FIELDS = frozenset(field for field in LABEL_STORAGE_FIELDS if field.startswith("compact_"))
LEGACY_ATOM_FIELDS = frozenset(
    {"atom_lookups", "atom_hits", "atom_misses", "unique_content_bytes"}
)
INDEX_FIELDS = {
    "process_label",
    "query_name",
    "category",
    "mode",
    "block",
    "order_index",
    "query_label_storage",
    "binary_sha256",
    "corpus",
    "raw_output",
    "process_wall_seconds",
    "process_user_seconds",
    "process_system_seconds",
    "max_rss_kib",
}
RESIDENCY_FIELDS = {
    "process_label",
    "block",
    "query_label_storage",
    "phase",
    "file_count",
    "resident_bytes",
    "corpus_file_bytes",
}

# CompactIds translates each distinct generation-local source symbol once and
# then reuses its ID. Consequently only the logical-return/cache-hit counts and
# elapsed validation time may differ. Physical page reads, validation calls and
# bytes, corruption counters, retention, and all other symbol state remain exact.
ALLOWED_SYMBOL_PATHS = {
    "logical_returned_delta": "distinct source-symbol translation replaces repeated string returns",
    "page_cache_hits_delta": "translation-table hits change repeated symbol-page cache hits",
    "page_validation_ns_delta": "elapsed validation time is non-semantic timing",
}
# Source-symbol translation can avoid repeated metadata cache lookups. No other
# metadata counter, read breakdown, gauge, or lifetime peak is exempt.
ALLOWED_METADATA_PATHS = {
    "counters_delta.cache.hits": "compact source-symbol translation changes repeated cache hits"
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


def finite_nonnegative(value: Any, name: str) -> float:
    try:
        converted = float(value)
    except (TypeError, ValueError) as error:
        raise GateError(f"{name} must be numeric") from error
    if not math.isfinite(converted) or converted < 0:
        raise GateError(f"{name} must be finite and non-negative")
    return converted


def exact_object(value: Any, fields: set[str] | frozenset[str], context: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(fields):
        raise GateError(f"{context} has an invalid shape")
    return value


def numeric_object(
    value: Any, fields: set[str] | frozenset[str], context: str
) -> dict[str, int]:
    obj = exact_object(value, fields, context)
    return {field: nonnegative_int(obj[field], f"{context}.{field}") for field in fields}


def canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            hasher.update(block)
    return hasher.hexdigest()


def read_manifest(path: Path) -> list[dict[str, Any]]:
    queries = manifest_gate.read_normalized_manifest(path)
    categories = {query["category"] for query in queries}
    missing = REQUIRED_CATEGORIES - categories
    if missing:
        raise GateError(f"manifest lacks required categories: {sorted(missing)!r}")
    for query in queries:
        if query["mode"] == "range" and query["range_scalar_cache_max_bytes"] != 0:
            raise GateError(f"{query['query_name']}: range scalar cache must be disabled")
    return queries


def normalize_manifest(input_path: Path, output_tsv: Path, output_json: Path) -> None:
    queries = manifest_gate.normalize_manifest(input_path, 0)
    categories = {query["category"] for query in queries}
    missing = REQUIRED_CATEGORIES - categories
    if missing:
        raise GateError(f"manifest lacks required categories: {sorted(missing)!r}")
    if any(
        query["mode"] == "range" and query["range_scalar_cache_max_bytes"] != 0
        for query in queries
    ):
        raise GateError("every range query must disable the scalar range cache")
    manifest_gate.write_normalized_manifest(queries, output_tsv, output_json)


def expected_plan(queries: list[dict[str, Any]], blocks: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for query in queries:
        for block in range(1, blocks + 1):
            schedule = ABBA if block % 2 == 1 else BAAB
            for order_index, policy in enumerate(schedule, 1):
                rows.append(
                    {
                        "process_label": (
                            f"{query['query_name']}-b{block:02d}-{order_index:02d}-{policy}"
                        ),
                        "query_name": query["query_name"],
                        "category": query["category"],
                        "mode": query["mode"],
                        "block": block,
                        "order_index": order_index,
                        "query_label_storage": policy,
                    }
                )
    return rows


def write_plan(manifest: Path, output: Path, blocks: int) -> None:
    queries = read_manifest(manifest)
    rows = expected_plan(queries, positive_int(blocks, "blocks"))
    with output.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination, fieldnames=tuple(rows[0]), delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def parse_markdown_metric(markdown: str, label: str) -> int:
    matches = re.findall(
        rf"^\| {re.escape(label)} \| ([0-9]+) \|$", markdown, re.MULTILINE
    )
    if len(matches) != 1:
        raise GateError(f"smoke report must contain exactly one {label!r} metric")
    return int(matches[0])


def validate_smoke_report(kind: str, report: Path, output: Path) -> None:
    markdown = report.read_text(encoding="utf-8")
    if "- Storage Layout: schema8" not in markdown:
        raise GateError("smoke report is not Schema 8")
    result: dict[str, Any] = {"schema": SMOKE_SCHEMA, "kind": kind, "gate": "pass"}
    if kind == "footer":
        if (
            "- Requested Segment Footer Validation: true" not in markdown
            or "- Effective Segment Footer Validation: true" not in markdown
        ):
            raise GateError("footer validation was not requested and effective")
        result.update({"requested": True, "effective": True})
    elif kind == "readback":
        metrics = {
            key: parse_markdown_metric(markdown, label)
            for key, label in (
                ("expected", "Expected Readback Queries"),
                ("executed", "Executed Readback Queries"),
                ("skipped", "Skipped Readback Queries"),
                ("isolation_skips", "Isolation Check Skips"),
                ("checked", "Checked Queries"),
                ("mismatches", "Mismatches"),
            )
        }
        if metrics["expected"] <= 0:
            raise GateError("readback oracle executed no expected cases")
        if not (
            metrics["executed"] == metrics["expected"]
            and metrics["checked"] == metrics["expected"]
            and metrics["skipped"] == 0
            and metrics["isolation_skips"] == 0
            and metrics["mismatches"] == 0
        ):
            raise GateError("readback oracle must execute/check every expected case with no skips or mismatches")
        result.update(metrics)
    else:
        raise GateError(f"unknown smoke report kind: {kind}")
    with output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def validate_label_materialization(
    value: Any, category: str, context: str
) -> dict[str, int]:
    labels = numeric_object(value, LABEL_FIELDS, context)
    if category in FULL_DEMAND_CATEGORIES:
        if labels["rows_selectively_materialized"] or labels["pairs_omitted"]:
            raise GateError(f"{context}: full-demand path omitted source labels")
        if labels["rows_full_materialized"] != labels["rows_integrity_checked"]:
            raise GateError(f"{context}: full-demand row accounting is incomplete")
        if labels["pairs_materialized"] != labels["pairs_integrity_checked"]:
            raise GateError(f"{context}: full-demand pair accounting is incomplete")
    elif category in SELECTIVE_CATEGORIES:
        if not labels["rows_selectively_materialized"] or not labels["pairs_omitted"]:
            raise GateError(f"{context}: selective path did not omit any source labels")
    return labels


def validate_label_storage(
    value: Any,
    policy: str,
    arena_bytes: int,
    result_bearing: bool,
    first_run: bool,
    context: str,
) -> dict[str, int]:
    counters = numeric_object(value, LABEL_STORAGE_FIELDS, context)
    if counters["atom_lookups"] != counters["atom_hits"] + counters["atom_misses"]:
        raise GateError(f"{context}: legacy atom accounting does not reconcile")
    if counters["compact_atom_lookups"] != (
        counters["compact_atom_hits"] + counters["compact_atom_misses"]
    ):
        raise GateError(f"{context}: compact atom accounting does not reconcile")
    if counters["compact_source_symbol_translations"] != (
        counters["compact_source_symbol_translation_hits"]
        + counters["compact_source_symbol_translation_misses"]
    ):
        raise GateError(f"{context}: compact source-symbol translations do not reconcile")
    categorized = sum(
        counters[field]
        for field in (
            "compact_atom_bytes",
            "compact_pair_bytes",
            "compact_hash_directory_bytes",
            "compact_translation_bytes",
        )
    )
    if (
        counters["compact_retained_bytes"] != categorized
        or counters["compact_arena_current_bytes"] != categorized
    ):
        raise GateError(f"{context}: compact retained-byte categories do not reconcile")
    if not (
        counters["compact_arena_current_bytes"]
        <= counters["compact_arena_peak_bytes"]
        <= counters["compact_arena_budget_bytes"]
    ):
        raise GateError(f"{context}: compact arena current/peak/budget ordering is invalid")
    if counters["compact_arena_admission_refusals"]:
        raise GateError(f"{context}: compact arena refused an admission")
    if counters["compact_compatibility_materializations"]:
        raise GateError(f"{context}: compact labels fell back to String materialization")

    if policy == "owned-strings":
        if any(counters[field] for field in LEGACY_ATOM_FIELDS | COMPACT_FIELDS):
            raise GateError(f"{context}: OwnedStrings reported atom/compact activity")
    elif policy == "compact-ids":
        if any(counters[field] for field in LEGACY_ATOM_FIELDS):
            raise GateError(f"{context}: CompactIds reported SharedAtoms activity")
        if counters["compact_arena_budget_bytes"] not in (0, arena_bytes):
            raise GateError(f"{context}: compact arena counter has the wrong budget")
        # `label_sets` counts calls entering the query-label interner. Compact
        # execution can additionally create governed pair-only projections
        # (for example after matcher/control labels are removed), so its
        # allocation count may be greater but must never be smaller.
        if counters["compact_label_sets"] < counters["label_sets"]:
            raise GateError(f"{context}: compact label-set count is below logical inputs")
        if result_bearing:
            if not counters["compact_arena_current_bytes"]:
                raise GateError(f"{context}: result-bearing CompactIds run lacks arena activity")
            # Label-storage counters are per-execution deltas while retained
            # arena charges are point-in-time gauges. The first execution in a
            # fresh process must populate labels and atoms. Warm executions may
            # reuse a cached query context/result and legitimately report zero
            # activity deltas while retaining the governed arena gauges.
            first_run_fields = (
                "label_sets",
                "compact_label_sets",
                "compact_pairs",
                "compact_atom_lookups",
                "compact_unique_strings",
            )
            if first_run and any(not counters[field] for field in first_run_fields):
                raise GateError(f"{context}: first CompactIds run did not populate the arena")
            if counters["compact_arena_budget_bytes"] != arena_bytes:
                raise GateError(f"{context}: active compact arena does not report 512 MiB budget")
    else:
        raise GateError(f"{context}: unknown label policy {policy!r}")
    return counters


def validate_raw(
    row: dict[str, str], query: dict[str, Any], args: argparse.Namespace
) -> tuple[str, list[dict[str, Any]]]:
    raw_path = Path(row["raw_output"])
    document = json.loads(raw_path.read_text(encoding="utf-8"))
    exact_object(document, DOCUMENT_FIELDS, str(raw_path))
    if document["schema"] != RAW_SCHEMA:
        raise GateError(f"{raw_path}: raw schema must be {RAW_SCHEMA}")
    nonnegative_int(
        document["corpus_fingerprint_duration_ns"],
        f"{raw_path}.corpus_fingerprint_duration_ns",
    )
    configuration = exact_object(
        document["configuration"], CONFIGURATION_FIELDS, f"{raw_path}.configuration"
    )
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
        "label_materialization": "demand-driven",
        "query_label_storage": row["query_label_storage"],
        "query_instrumentation": "off",
        "query_label_arena_max_bytes": args.arena_bytes,
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
        raise GateError(f"{raw_path}: timed configuration differs from the fixed invocation")
    expected_limits = {
        "max_matched_series": args.max_matched_series,
        "max_projected_series": args.max_projected_series,
        "max_chunk_reads": args.max_chunk_reads,
        "max_bytes_read": args.max_bytes_read,
        "max_samples_decoded": args.max_samples_decoded,
        "max_regex_values_examined": args.max_regex_values_examined,
    }
    if document["limits"] != expected_limits:
        raise GateError(f"{raw_path}: query limits differ from the fixed invocation")
    fingerprint = digest(
        document["corpus_fingerprint_sha256"], f"{raw_path}.corpus_fingerprint"
    )
    runs = document["runs"]
    if not isinstance(runs, list) or len(runs) != args.benchmark_repeats:
        raise GateError(f"{raw_path}: expected exactly {args.benchmark_repeats} runs")

    validated: list[dict[str, Any]] = []
    for run_index, run_value in enumerate(runs):
        context = f"{raw_path}:{query['query_name']}:{run_index}"
        run = exact_object(run_value, RUN_FIELDS, context)
        run_kind = "cold" if run_index == 0 else "warm"
        if run["query"] != query["expression"]:
            raise GateError(f"{context}: expression differs from the manifest")
        if run["run_index"] != run_index or run["run_kind"] != run_kind:
            raise GateError(f"{context}: run index/kind is invalid")
        if (
            run["effective_start_ms"] != query["start_ms"]
            or run["effective_end_ms"] != query["end_ms"]
            or run["step_ms"] != query["step_ms"]
        ):
            raise GateError(f"{context}: effective evaluation range differs")
        duration_ns = positive_int(run["duration_ns"], f"{context}.duration_ns")
        nonnegative_int(
            run["post_query_fingerprint_ns"], f"{context}.post_query_fingerprint_ns"
        )
        try:
            stats = common.validate_stats(run["stats"], context)
            stages = common.validate_query_stages(
                run["query_stages"], "off", duration_ns, context
            )
            symbols = phase1.validate_symbol_reads(
                run["symbol_reads"], f"{context}.symbol_reads"
            )
            metadata = phase1.validate_metadata_runtime(
                run["metadata_runtime"], f"{context}.metadata_runtime"
            )
            range_cache = phase1.validate_range_cache(
                run["range_scalar_cache"], query, context
            )
        except (common.GateError, phase1.GateError) as error:
            raise GateError(str(error)) from error
        payload = numeric_object(run["payload_reads"], PAYLOAD_FIELDS, f"{context}.payload")
        if payload["logical_used_bytes"] != stats["bytes_read"]:
            raise GateError(f"{context}: payload logical bytes differ from QueryStats")
        if payload["physical_bytes"] < payload["logical_used_bytes"]:
            raise GateError(f"{context}: physical payload bytes are below logical bytes")
        labels = validate_label_materialization(
            run["label_materialization"], query["category"], f"{context}.labels"
        )
        result_series = nonnegative_int(run["result_series"], f"{context}.result_series")
        result_samples = nonnegative_int(run["result_samples"], f"{context}.result_samples")
        result_bearing = query["category"] != "empty-result-control"
        if result_bearing and (not result_series or not result_samples):
            raise GateError(f"{context}: representative query returned an empty result")
        if not result_bearing and (
            result_series or result_samples or any(payload.values())
        ):
            raise GateError(f"{context}: empty-result control returned/read payload data")
        if query["category"] in TYPED_FULL_CATEGORIES and not stats["typed_full_chunks_decoded"]:
            raise GateError(f"{context}: typed native query decoded no full chunks")
        storage = validate_label_storage(
            run["query_label_storage"],
            row["query_label_storage"],
            args.arena_bytes,
            result_bearing,
            run_index == 0,
            f"{context}.query_label_storage",
        )
        validated.append(
            {
                "run_index": run_index,
                "run_kind": run_kind,
                "duration_ns": duration_ns,
                "semantic_fingerprint": digest(
                    run["semantic_fingerprint_sha256"], f"{context}.semantic_fingerprint"
                ),
                "portable_fingerprint": digest(
                    run["portable_semantic_fingerprint_sha256"],
                    f"{context}.portable_fingerprint",
                ),
                "result_series": result_series,
                "result_samples": result_samples,
                "stats": stats,
                "payload": payload,
                "labels": labels,
                "label_storage": storage,
                "symbols": symbols,
                "metadata": metadata,
                "range_cache": range_cache,
                "stages": stages,
            }
        )
    return fingerprint, validated


def comparable_symbols(value: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(value)
    result["logical_returned_delta"] = {"calls": 0, "bytes": 0}
    result["page_cache_hits_delta"] = 0
    result["page_validation_ns_delta"] = 0
    return result


def comparable_metadata(value: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(value)
    result["counters_delta"]["cache"]["hits"] = 0
    return result


def read_tsv(path: Path, fields: set[str], context: str) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if not rows or any(set(row) != fields for row in rows):
        raise GateError(f"{context} TSV has an invalid shape")
    return rows


def load_inventory(path: Path, corpus: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema") != common.INVENTORY_SCHEMA:
        raise GateError(f"{path}: inventory schema differs")
    if os.path.realpath(document.get("corpus", "")) != os.path.realpath(corpus):
        raise GateError(f"{path}: inventory names a different corpus")
    positive_int(document.get("file_count"), f"{path}.file_count")
    positive_int(document.get("total_bytes"), f"{path}.total_bytes")
    digest(document.get("corpus_sha256"), f"{path}.corpus_sha256")
    return document


def validate_residency(
    path: Path,
    plan_by_label: dict[str, dict[str, Any]],
    inventory: dict[str, Any],
    max_after_evict: int,
) -> None:
    rows = read_tsv(path, RESIDENCY_FIELDS, "residency summary")
    seen: set[tuple[str, str]] = set()
    for row in rows:
        label = row["process_label"]
        plan = plan_by_label.get(label)
        phase = row["phase"]
        if plan is None or phase not in {"after-evict", "after-run"}:
            raise GateError("residency summary contains an unknown process/phase")
        if int(row["block"]) != plan["block"] or row["query_label_storage"] != plan["query_label_storage"]:
            raise GateError(f"{label}: residency metadata differs from the run plan")
        key = (label, phase)
        if key in seen:
            raise GateError(f"duplicate residency row: {key!r}")
        seen.add(key)
        if positive_int(int(row["file_count"]), f"{label}.file_count") != inventory["file_count"]:
            raise GateError(f"{label}: residency file count differs from inventory")
        if nonnegative_int(int(row["corpus_file_bytes"]), f"{label}.corpus_file_bytes") != inventory["total_bytes"]:
            raise GateError(f"{label}: residency corpus bytes differ from inventory")
        resident = nonnegative_int(int(row["resident_bytes"]), f"{label}.resident_bytes")
        if phase == "after-evict" and resident > max_after_evict:
            raise GateError(f"{label}: {resident} bytes resident after eviction")
    expected = {
        (label, phase)
        for label in plan_by_label
        for phase in ("after-evict", "after-run")
    }
    if seen != expected:
        raise GateError("residency summary is incomplete")


def validate_smoke_json(path: Path, kind: str) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema") != SMOKE_SCHEMA or value.get("kind") != kind or value.get("gate") != "pass":
        raise GateError(f"{path}: {kind} validation evidence is invalid")


def median(values: list[int | float], context: str) -> float:
    if not values:
        raise GateError(f"{context}: no observations")
    return float(statistics.median(values))


def ratio(candidate: float, reference: float, context: str) -> float:
    if reference <= 0:
        raise GateError(f"{context}: OwnedStrings median must be positive")
    return candidate / reference


def material_regression_passes(
    candidate: float,
    reference: float,
    maximum_regression_pct: float,
    minimum_material_regression: int,
) -> bool:
    """Reject only regressions that exceed both relative and absolute floors."""
    relative_limit = reference * (1.0 + maximum_regression_pct / 100.0)
    absolute_regression = candidate - reference
    return candidate <= relative_limit or absolute_regression <= minimum_material_regression


def compare_results(args: argparse.Namespace) -> None:
    if args.arena_bytes != DEFAULT_ARENA_BYTES:
        raise GateError("Phase 2 compact arena must be exactly 512 MiB")
    queries = read_manifest(args.manifest)
    query_by_name = {query["query_name"]: query for query in queries}
    if args.broad_query_name not in query_by_name:
        raise GateError("the named broad query is absent from the manifest")
    if query_by_name[args.broad_query_name]["category"] != "broad-full-label-output":
        raise GateError("the named broad query is not the broad full-label category")
    plan = expected_plan(queries, args.blocks)
    plan_by_label = {row["process_label"]: row for row in plan}

    before = load_inventory(args.inventory_before, args.corpus)
    after = load_inventory(args.inventory_after, args.corpus)
    if before != after:
        raise GateError("corpus inventory changed during the experiment")
    validate_smoke_json(args.footer_validation, "footer")
    validate_smoke_json(args.readback_validation, "readback")
    validate_residency(args.residency, plan_by_label, before, args.max_resident_bytes_after_evict)

    binary_hash = file_sha256(args.binary)
    rows = read_tsv(args.index, INDEX_FIELDS, "raw index")
    if len(rows) != len(plan):
        raise GateError(f"expected {len(plan)} completed processes, found {len(rows)}")
    processes: dict[tuple[str, int, int], dict[str, Any]] = {}
    corpus_fingerprints: set[str] = set()
    seen_labels: set[str] = set()
    for row in rows:
        label = row["process_label"]
        expected = plan_by_label.get(label)
        if expected is None or label in seen_labels:
            raise GateError(f"unknown or duplicate process label: {label!r}")
        seen_labels.add(label)
        for field in ("query_name", "category", "mode", "query_label_storage"):
            if row[field] != str(expected[field]):
                raise GateError(f"{label}: raw-index {field} differs from the run plan")
        block = positive_int(int(row["block"]), f"{label}.block")
        order_index = positive_int(int(row["order_index"]), f"{label}.order_index")
        if block != expected["block"] or order_index != expected["order_index"]:
            raise GateError(
                f"{label}: raw-index block/order differs from the counterbalanced plan"
            )
        if row["binary_sha256"] != binary_hash:
            raise GateError(f"{label}: process did not use the one preserved binary")
        if os.path.realpath(row["corpus"]) != os.path.realpath(args.corpus):
            raise GateError(f"{label}: process used a different corpus")
        process_times = {
            field: finite_nonnegative(row[field], f"{label}.{field}")
            for field in (
                "process_wall_seconds",
                "process_user_seconds",
                "process_system_seconds",
            )
        }
        max_rss = positive_int(int(row["max_rss_kib"]), f"{label}.max_rss_kib")
        fingerprint, runs = validate_raw(row, query_by_name[row["query_name"]], args)
        corpus_fingerprints.add(fingerprint)
        key = (row["query_name"], block, order_index)
        if key in processes:
            raise GateError(f"duplicate completed process: {key!r}")
        processes[key] = {
            "index": row,
            "policy": row["query_label_storage"],
            "runs": runs,
            "max_rss_kib": max_rss,
            **process_times,
        }
    if seen_labels != set(plan_by_label):
        raise GateError("completed-process set differs from the counterbalanced plan")
    if len(corpus_fingerprints) != 1:
        raise GateError("query-reported corpus fingerprint changed across processes")

    exact_fields = (
        "semantic_fingerprint",
        "portable_fingerprint",
        "result_series",
        "result_samples",
        "stats",
        "payload",
        "labels",
        "range_cache",
    )
    correctness_digests: dict[tuple[str, int], str] = {}
    storage_canonical: dict[tuple[str, str, int], dict[str, int]] = {}
    allowed_differences_seen: dict[str, bool] = {
        **{f"symbol_reads.{path}": False for path in ALLOWED_SYMBOL_PATHS},
        **{f"metadata_runtime.{path}": False for path in ALLOWED_METADATA_PATHS},
    }
    for query in queries:
        query_name = query["query_name"]
        fingerprint_pairs: set[tuple[str, str, int, int]] = set()
        for run_index in range(args.benchmark_repeats):
            observations = [
                process["runs"][run_index]
                for key, process in processes.items()
                if key[0] == query_name
            ]
            canonical = observations[0]
            for observation in observations[1:]:
                for field in exact_fields:
                    if observation[field] != canonical[field]:
                        raise GateError(
                            f"{query_name} run {run_index}: {field} differs across ABBA arms"
                        )
                if comparable_symbols(observation["symbols"]) != comparable_symbols(canonical["symbols"]):
                    raise GateError(
                        f"{query_name} run {run_index}: non-exempt symbol counters differ"
                    )
                if comparable_metadata(observation["metadata"]) != comparable_metadata(canonical["metadata"]):
                    raise GateError(
                        f"{query_name} run {run_index}: non-exempt metadata counters differ"
                    )
            correctness_digests[(query_name, run_index)] = canonical_digest(
                {field: canonical[field] for field in exact_fields}
            )
            for observation in observations:
                fingerprint_pairs.add(
                    (
                        observation["semantic_fingerprint"],
                        observation["portable_fingerprint"],
                        observation["result_series"],
                        observation["result_samples"],
                    )
                )
        if len(fingerprint_pairs) != 1:
            raise GateError(f"{query_name}: result fingerprint/shape changed cold-to-warm")

        for key, process in processes.items():
            if key[0] != query_name:
                continue
            for run in process["runs"]:
                stable_key = (query_name, process["policy"], run["run_index"])
                prior = storage_canonical.setdefault(stable_key, run["label_storage"])
                if prior != run["label_storage"]:
                    raise GateError(
                        f"{query_name} {process['policy']} run {run['run_index']}: compact accounting is nondeterministic"
                    )

        for run_index in range(args.benchmark_repeats):
            owned = next(
                process["runs"][run_index]
                for key, process in processes.items()
                if key[0] == query_name and process["policy"] == "owned-strings"
            )
            compact = next(
                process["runs"][run_index]
                for key, process in processes.items()
                if key[0] == query_name and process["policy"] == "compact-ids"
            )
            if owned["symbols"]["logical_returned_delta"] != compact["symbols"]["logical_returned_delta"]:
                allowed_differences_seen["symbol_reads.logical_returned_delta"] = True
            if owned["symbols"]["page_cache_hits_delta"] != compact["symbols"]["page_cache_hits_delta"]:
                allowed_differences_seen["symbol_reads.page_cache_hits_delta"] = True
            if owned["symbols"]["page_validation_ns_delta"] != compact["symbols"]["page_validation_ns_delta"]:
                allowed_differences_seen["symbol_reads.page_validation_ns_delta"] = True
            if owned["metadata"]["counters_delta"]["cache"]["hits"] != compact["metadata"]["counters_delta"]["cache"]["hits"]:
                allowed_differences_seen["metadata_runtime.counters_delta.cache.hits"] = True

    summary_fields = [
        "process_label",
        "query_name",
        "category",
        "mode",
        "block",
        "order_index",
        "query_label_storage",
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
        *(f"stats_{field}" for field in common.QUERY_STATS_FIELDS),
        *(f"labels_{field}" for field in sorted(LABEL_FIELDS)),
        *(f"storage_{field}" for field in sorted(LABEL_STORAGE_FIELDS)),
        "payload_logical_used_bytes",
        "payload_physical_reads",
        "payload_physical_bytes",
        "payload_read_used_amplification",
        "symbols_logical_calls",
        "symbols_logical_bytes",
        "symbols_page_cache_hits",
        "symbols_page_validation_ns",
        "metadata_cache_hits",
    ]
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination, fieldnames=summary_fields, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for plan_row in plan:
            process = processes[
                (plan_row["query_name"], plan_row["block"], plan_row["order_index"])
            ]
            index_row = process["index"]
            for run in process["runs"]:
                logical = run["payload"]["logical_used_bytes"]
                row: dict[str, Any] = {
                    field: index_row[field]
                    for field in (
                        "process_label",
                        "query_name",
                        "category",
                        "mode",
                        "block",
                        "order_index",
                        "query_label_storage",
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
                            (plan_row["query_name"], run["run_index"])
                        ],
                        "payload_logical_used_bytes": logical,
                        "payload_physical_reads": run["payload"]["physical_reads"],
                        "payload_physical_bytes": run["payload"]["physical_bytes"],
                        "payload_read_used_amplification": (
                            "" if not logical else run["payload"]["physical_bytes"] / logical
                        ),
                        "symbols_logical_calls": run["symbols"]["logical_returned_delta"]["calls"],
                        "symbols_logical_bytes": run["symbols"]["logical_returned_delta"]["bytes"],
                        "symbols_page_cache_hits": run["symbols"]["page_cache_hits_delta"],
                        "symbols_page_validation_ns": run["symbols"]["page_validation_ns_delta"],
                        "metadata_cache_hits": run["metadata"]["counters_delta"]["cache"]["hits"],
                    }
                )
                row.update({f"stats_{field}": run["stats"][field] for field in common.QUERY_STATS_FIELDS})
                row.update({f"labels_{field}": run["labels"][field] for field in LABEL_FIELDS})
                row.update({f"storage_{field}": run["label_storage"][field] for field in LABEL_STORAGE_FIELDS})
                writer.writerow(row)

    performance: list[dict[str, Any]] = []
    failures: list[str] = []
    for query in queries:
        query_name = query["query_name"]
        for run_kind in ("cold", "warm"):
            observations: dict[str, list[int]] = {policy: [] for policy in POLICIES}
            for key, process in processes.items():
                if key[0] == query_name:
                    observations[process["policy"]].extend(
                        run["duration_ns"]
                        for run in process["runs"]
                        if run["run_kind"] == run_kind
                    )
            owned_median = median(observations["owned-strings"], f"{query_name} owned {run_kind}")
            compact_median = median(observations["compact-ids"], f"{query_name} compact {run_kind}")
            measured_ratio = ratio(compact_median, owned_median, f"{query_name} {run_kind}")
            if query_name == args.broad_query_name:
                threshold = 1.0 - args.broad_min_improvement_pct / 100.0
                threshold_name = "minimum_improvement_pct"
                threshold_value = args.broad_min_improvement_pct
                passed = measured_ratio <= threshold
                absolute_limit: dict[str, int | float] = {}
            else:
                threshold = 1.0 + args.control_max_regression_pct / 100.0
                threshold_name = "maximum_regression_pct"
                threshold_value = args.control_max_regression_pct
                passed = material_regression_passes(
                    compact_median,
                    owned_median,
                    args.control_max_regression_pct,
                    args.control_min_material_regression_ns,
                )
                absolute_limit = {
                    "minimum_material_regression_ns": args.control_min_material_regression_ns,
                    "absolute_regression_ns": compact_median - owned_median,
                }
            if not passed:
                failures.append(
                    f"{query_name} {run_kind} compact/owned {measured_ratio:.6f} exceeds "
                    f"{threshold:.6f} and absolute regression "
                    f"{compact_median - owned_median:.0f} ns exceeds "
                    f"{args.control_min_material_regression_ns} ns"
                )
            performance.append(
                {
                    "metric": "query_duration_ns",
                    "query_name": query_name,
                    "run_kind": run_kind,
                    "owned_observations": observations["owned-strings"],
                    "compact_observations": observations["compact-ids"],
                    "owned_median": owned_median,
                    "compact_median": compact_median,
                    "compact_over_owned": measured_ratio,
                    threshold_name: threshold_value,
                    **absolute_limit,
                    "gate": "pass" if passed else "fail",
                }
            )

        rss: dict[str, list[int]] = {policy: [] for policy in POLICIES}
        for key, process in processes.items():
            if key[0] == query_name:
                rss[process["policy"]].append(process["max_rss_kib"])
        owned_rss = median(rss["owned-strings"], f"{query_name} owned RSS")
        compact_rss = median(rss["compact-ids"], f"{query_name} compact RSS")
        rss_ratio = ratio(compact_rss, owned_rss, f"{query_name} RSS")
        if query_name == args.broad_query_name:
            rss_threshold = 1.0 - args.broad_min_rss_improvement_pct / 100.0
            rss_limit = {"minimum_improvement_pct": args.broad_min_rss_improvement_pct}
            rss_passed = rss_ratio <= rss_threshold
        else:
            rss_threshold = 1.0 + args.rss_max_regression_pct / 100.0
            rss_limit = {
                "maximum_regression_pct": args.rss_max_regression_pct,
                "minimum_material_regression_kib": args.rss_min_material_regression_kib,
                "absolute_regression_kib": compact_rss - owned_rss,
            }
            rss_passed = material_regression_passes(
                compact_rss,
                owned_rss,
                args.rss_max_regression_pct,
                args.rss_min_material_regression_kib,
            )
        if not rss_passed:
            failures.append(
                f"{query_name} RSS compact/owned {rss_ratio:.6f} exceeds "
                f"{rss_threshold:.6f} and absolute regression "
                f"{compact_rss - owned_rss:.0f} KiB exceeds "
                f"{args.rss_min_material_regression_kib} KiB"
            )
        performance.append(
            {
                "metric": "process_max_rss_kib",
                "query_name": query_name,
                "run_kind": None,
                "owned_observations": rss["owned-strings"],
                "compact_observations": rss["compact-ids"],
                "owned_median": owned_rss,
                "compact_median": compact_rss,
                "compact_over_owned": rss_ratio,
                **rss_limit,
                "gate": "pass" if rss_passed else "fail",
            }
        )

    result = {
        "schema": RESULT_SCHEMA,
        "correctness_gate": "pass",
        "performance_gate": "pass" if not failures else "fail",
        "schedule": {
            "odd_blocks": list(ABBA),
            "even_blocks": list(BAAB),
        },
        "blocks": args.blocks,
        "processes_per_arm_per_query": args.blocks * 2,
        "benchmark_repeats": args.benchmark_repeats,
        "query_label_arena_max_bytes": args.arena_bytes,
        "label_materialization": "demand-driven",
        "range_scalar_cache_max_bytes": 0,
        "binary_sha256": binary_hash,
        "corpus_inventory_sha256": before["corpus_sha256"],
        "query_corpus_fingerprint_sha256": next(iter(corpus_fingerprints)),
        "exact_equivalence": [
            "semantic_and_portable_fingerprints",
            "result_series_and_samples",
            "all_QueryStats_fields",
            "logical_and_physical_payload_counters",
            "label_materialization_and_integrity_counters",
            "range_scalar_cache_counters",
            "all_non_exempt_metadata_and_symbol_counters",
        ],
        "allowed_policy_counter_differences": {
            **{f"symbol_reads.{key}": reason for key, reason in ALLOWED_SYMBOL_PATHS.items()},
            **{f"metadata_runtime.{key}": reason for key, reason in ALLOWED_METADATA_PATHS.items()},
        },
        "allowed_differences_observed": allowed_differences_seen,
        "non_semantic_measurements": [
            "query duration and post-fingerprint duration",
            "off-mode query_stages.unclassified_ns (it equals query duration)",
            "process wall/user/system time and maximum RSS",
            "corpus fingerprint elapsed time",
            "per-policy query-label storage representation counters",
        ],
        "compact_accounting_gate": (
            "atom and translation totals reconcile; retained categories equal current charge; "
            "current <= peak <= 512 MiB; zero admission refusals and compatibility materializations"
        ),
        "thresholds": {
            "broad_cold_and_warm_min_improvement_pct": args.broad_min_improvement_pct,
            "broad_process_rss_min_improvement_pct": args.broad_min_rss_improvement_pct,
            "control_cold_and_warm_max_regression_pct": args.control_max_regression_pct,
            "control_min_material_regression_ns": args.control_min_material_regression_ns,
            "control_process_rss_max_regression_pct": args.rss_max_regression_pct,
            "control_process_rss_min_material_regression_kib": args.rss_min_material_regression_kib,
        },
        "performance": performance,
        "failures": failures,
    }
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")
    if failures:
        raise GateError("Phase 2 promotion gate failed: " + "; ".join(failures))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    normalize = commands.add_parser("normalize-manifest")
    normalize.add_argument("--input", type=Path, required=True)
    normalize.add_argument("--output-tsv", type=Path, required=True)
    normalize.add_argument("--output-json", type=Path, required=True)

    plan = commands.add_parser("write-plan")
    plan.add_argument("--manifest", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--blocks", type=int, required=True)

    inventory = commands.add_parser("inventory")
    inventory.add_argument("--corpus", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--paths-output", type=Path, required=True)

    smoke = commands.add_parser("validate-smoke-report")
    smoke.add_argument("--kind", choices=("footer", "readback"), required=True)
    smoke.add_argument("--report", type=Path, required=True)
    smoke.add_argument("--output", type=Path, required=True)

    compare = commands.add_parser("compare-results")
    compare.add_argument("--index", type=Path, required=True)
    compare.add_argument("--manifest", type=Path, required=True)
    compare.add_argument("--inventory-before", type=Path, required=True)
    compare.add_argument("--inventory-after", type=Path, required=True)
    compare.add_argument("--residency", type=Path, required=True)
    compare.add_argument("--footer-validation", type=Path, required=True)
    compare.add_argument("--readback-validation", type=Path, required=True)
    compare.add_argument("--summary", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--binary", type=Path, required=True)
    compare.add_argument("--corpus", type=Path, required=True)
    compare.add_argument("--broad-query-name", required=True)
    compare.add_argument("--blocks", type=int, required=True)
    compare.add_argument("--benchmark-repeats", type=int, required=True)
    compare.add_argument("--arena-bytes", type=int, required=True)
    compare.add_argument("--queue-depth", type=int, required=True)
    compare.add_argument("--max-resident-bytes-after-evict", type=int, required=True)
    compare.add_argument("--max-matched-series", type=int, required=True)
    compare.add_argument("--max-projected-series", type=int, required=True)
    compare.add_argument("--max-chunk-reads", type=int, required=True)
    compare.add_argument("--max-bytes-read", type=int, required=True)
    compare.add_argument("--max-samples-decoded", type=int, required=True)
    compare.add_argument("--max-regex-values-examined", type=int, required=True)
    compare.add_argument("--broad-min-improvement-pct", type=float, required=True)
    compare.add_argument("--broad-min-rss-improvement-pct", type=float, required=True)
    compare.add_argument("--control-max-regression-pct", type=float, required=True)
    compare.add_argument("--control-min-material-regression-ns", type=int, required=True)
    compare.add_argument("--rss-max-regression-pct", type=float, required=True)
    compare.add_argument("--rss-min-material-regression-kib", type=int, required=True)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "normalize-manifest":
            normalize_manifest(args.input, args.output_tsv, args.output_json)
        elif args.command == "write-plan":
            write_plan(args.manifest, args.output, args.blocks)
        elif args.command == "inventory":
            common.write_inventory(args.corpus, args.output, args.paths_output)
        elif args.command == "validate-smoke-report":
            validate_smoke_report(args.kind, args.report, args.output)
        elif args.command == "compare-results":
            positive_int(args.blocks, "blocks")
            if args.benchmark_repeats != 3:
                raise GateError("benchmark repeats must be exactly 3 (one cold plus two warm)")
            nonnegative_int(args.max_resident_bytes_after_evict, "max resident bytes")
            nonnegative_int(
                args.control_min_material_regression_ns,
                "control minimum material regression ns",
            )
            nonnegative_int(
                args.rss_min_material_regression_kib,
                "RSS minimum material regression KiB",
            )
            for name in (
                "broad_min_improvement_pct",
                "broad_min_rss_improvement_pct",
                "control_max_regression_pct",
                "rss_max_regression_pct",
            ):
                finite_nonnegative(getattr(args, name), name)
            compare_results(args)
        else:
            raise AssertionError(args.command)
    except (
        GateError,
        common.GateError,
        manifest_gate.GateError,
        phase1.GateError,
        OSError,
        TypeError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
    ) as error:
        print(f"Phase 2 CompactIds A/B gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
