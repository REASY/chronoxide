#!/usr/bin/env python3
"""Strict contract and result gate for the Schema 8 Phase 1 query baseline."""

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


MANIFEST_SCHEMA = "chronoxide/storage-vnext-phase1-query-manifest/v1"
NORMALIZED_SCHEMA = "chronoxide/storage-vnext-phase1-query-manifest-normalized/v1"
INVENTORY_SCHEMA = "chronoxide/storage-vnext-phase1-query-inventory/v1"
RESULT_SCHEMA = "chronoxide/storage-vnext-phase1-query-results/v1"
SMOKE_SCHEMA = "chronoxide/storage-vnext-phase1-smoke-validation/v1"
RAW_SCHEMA = common.RAW_SCHEMA
FIXED_MANIFEST_SHA256 = "7da7c63e8044cc19f5b49a87890200b042527094020a26e72ffb3d3173526b8f"
EXPECTED_QUERY_NAMES = (
    "broad_raw_count_selector",
    "equality_last",
    "sparse_regex_last",
    "negative_matcher_last",
    "no_result",
    "scalar_rate_sum_instant",
    "scalar_rate_sum_range",
    "virtual_hist_count_rate_sum_range_cache_off",
    "virtual_hist_count_rate_sum_range_cache_16m",
    "native_hist_count_range",
    "native_hist_p95_range",
    "native_exp_count_range",
    "native_exp_p95_range",
    "scalar_rate_sum_instant_full_control",
    "scalar_rate_sum_range_full_control",
    "native_hist_count_range_full_control",
    "native_exp_count_range_full_control",
)
ABBA_SCHEDULE = (
    ("off", "detailed", "detailed", "off"),
    ("detailed", "off", "off", "detailed"),
    ("off", "detailed", "detailed", "off"),
)
BENCHMARK_REPEATS = 3
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
RAW_DOCUMENT_FIELDS = {
    "schema",
    "corpus_fingerprint_sha256",
    "corpus_fingerprint_duration_ns",
    "configuration",
    "limits",
    "runs",
}
RAW_RUN_FIELDS = {
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
PAYLOAD_FIELDS = {"logical_used_bytes", "physical_reads", "physical_bytes"}
LABEL_FIELDS = {
    "rows_integrity_checked",
    "pairs_integrity_checked",
    "rows_full_materialized",
    "rows_selectively_materialized",
    "pairs_materialized",
    "pairs_omitted",
    "content_bytes_materialized",
}
READ_COUNT_FIELDS = {"calls", "bytes"}
SYMBOL_READ_FIELDS = {
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
RANGE_CACHE_FIELDS = {
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
    "process_governor_lifetime_peak_leased_bytes",
}
METADATA_TOP_FIELDS = {
    "counters_delta",
    "start_gauges",
    "end_gauges",
    "lifetime_peaks_after_run",
}
METADATA_COUNTER_FIELDS = {"cache", "governor", "file_manager", "reads"}
CACHE_COUNTER_FIELDS = {
    "hits",
    "misses",
    "evictions",
    "single_flight_waits",
    "successful_loads",
    "failed_loads",
    "corruption_detections",
    "corruption_hits",
    "resident_admissions",
    "resident_admission_refusals",
    "resident_admission_bypasses",
    "class_admissions",
}
GOVERNOR_COUNTER_FIELDS = {"retained_refusals", "in_flight_refusals"}
FILE_COUNTER_FIELDS = {
    "preflight_calls",
    "successful_preflights",
    "preflight_failures",
    "acquire_calls",
    "successful_acquires",
    "requested_handles",
    "deduplicated_handles",
    "descriptor_opens",
    "descriptor_closes",
    "descriptor_reuses",
    "lease_clones",
    "idle_evictions",
    "capacity_waits",
    "capacity_refusals",
    "open_failures",
    "structural_replacements",
    "acquisition_rollbacks",
}
READ_DELTA_FIELDS = {"issued", "unclassified", "by_file", "by_class"}
GAUGE_TOP_FIELDS = {"cache", "governor", "file_manager"}
CACHE_GAUGE_FIELDS = {
    "resident_entries",
    "live_allocations",
    "active_loads",
    "registered_artifacts",
    "ledger_reserved_bytes",
    "ledger_in_flight_bytes",
    "ledger_retained_bytes",
    "sticky_artifacts",
    "sticky_charged_bytes",
    "class_charges",
}
GOVERNOR_GAUGE_FIELDS = {
    "retained_max_bytes",
    "in_flight_max_bytes",
    "retained_bytes",
    "in_flight_bytes",
    "usage_charges",
}
FILE_GAUGE_FIELDS = {
    "max_open_files",
    "max_cached_open_files",
    "open_files",
    "occupied_open_slots",
    "active_open_files",
    "cached_open_files",
    "opening_files",
    "pending_open_files",
    "preflighting_files",
    "closing_files",
    "active_leases",
}
LIFETIME_FIELDS = {"cache_class_charges", "governor", "file_manager"}
GOVERNOR_PEAK_FIELDS = {
    "peak_retained_bytes",
    "peak_in_flight_bytes",
    "usage_charges",
}
FILE_PEAK_FIELDS = {
    "peak_open_files",
    "peak_occupied_open_slots",
    "peak_active_open_files",
    "peak_cached_open_files",
    "peak_active_leases",
    "peak_preflighting_files",
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


def exact_object(value: Any, fields: set[str], context: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise GateError(f"{context} has an invalid shape")
    return value


def numeric_object(value: Any, fields: set[str], context: str) -> dict[str, int]:
    obj = exact_object(value, fields, context)
    return {field: nonnegative_int(obj[field], f"{context}.{field}") for field in fields}


def checked_name(value: Any, context: str) -> str:
    if not isinstance(value, str) or not SAFE_NAME.fullmatch(value):
        raise GateError(f"{context} is not a safe name")
    return value


def checked_text(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value or any(char in value for char in "\t\r\n"):
        raise GateError(f"{context} must be non-empty, single-line, and tab-free")
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def load_manifest(path: Path, require_fixed_digest: bool = True) -> dict[str, Any]:
    raw = path.read_bytes()
    if require_fixed_digest and hashlib.sha256(raw).hexdigest() != FIXED_MANIFEST_SHA256:
        raise GateError("Phase 1 query manifest bytes differ from the sealed fixed matrix")
    document = json.loads(raw)
    exact_object(document, {"schema", "description", "corpus", "queries"}, "manifest")
    if document["schema"] != MANIFEST_SCHEMA:
        raise GateError(f"manifest schema must be {MANIFEST_SCHEMA}")
    checked_text(document["description"], "manifest.description")
    corpus = exact_object(
        document["corpus"],
        {"file_count", "total_bytes", "segments_manifest_sha256"},
        "manifest.corpus",
    )
    positive_int(corpus["file_count"], "manifest.corpus.file_count")
    positive_int(corpus["total_bytes"], "manifest.corpus.total_bytes")
    digest(corpus["segments_manifest_sha256"], "manifest.corpus.segments_manifest_sha256")
    queries = document["queries"]
    if not isinstance(queries, list) or len(queries) != len(EXPECTED_QUERY_NAMES):
        raise GateError(f"fixed matrix must contain {len(EXPECTED_QUERY_NAMES)} queries")
    if tuple(query.get("name") for query in queries if isinstance(query, dict)) != EXPECTED_QUERY_NAMES:
        raise GateError("fixed matrix query names or order changed")
    return document


def normalize_manifest(path: Path) -> dict[str, Any]:
    document = load_manifest(path)
    normalized: list[dict[str, Any]] = []
    allowed = {
        "name",
        "category",
        "mode",
        "time_ms",
        "start_ms",
        "end_ms",
        "step_ms",
        "range_scalar_cache_max_bytes",
        "label_materialization",
        "materialization_expectation",
        "decode_expectation",
        "expected_result",
        "equivalence_group",
        "semantic_group",
        "chronoxide_query",
    }
    for index, raw in enumerate(document["queries"]):
        context = f"queries[{index}]"
        if not isinstance(raw, dict) or set(raw) - allowed:
            raise GateError(f"{context} is not an allowed query object")
        name = checked_name(raw.get("name"), f"{context}.name")
        category = checked_name(raw.get("category"), f"{context}.category")
        expression = checked_text(raw.get("chronoxide_query"), f"{context}.chronoxide_query")
        mode = raw.get("mode")
        if mode not in {"instant", "range"}:
            raise GateError(f"{context}.mode must be instant or range")
        if raw.get("label_materialization") not in {"full", "demand-driven"}:
            raise GateError(f"{context}.label_materialization is invalid")
        if raw.get("materialization_expectation") not in {"full", "selective", "empty"}:
            raise GateError(f"{context}.materialization_expectation is invalid")
        if raw.get("decode_expectation") not in {"any", "scalar", "full", "none"}:
            raise GateError(f"{context}.decode_expectation is invalid")
        if raw.get("expected_result") not in {"empty", "nonempty"}:
            raise GateError(f"{context}.expected_result is invalid")
        if mode == "instant":
            if {"end_ms", "step_ms", "range_scalar_cache_max_bytes"}.intersection(raw):
                raise GateError(f"{context} has range-only fields")
            end_ms = nonnegative_int(raw.get("time_ms"), f"{context}.time_ms")
            start_ms = nonnegative_int(raw.get("start_ms", 0), f"{context}.start_ms")
            step_ms = None
            cache_bytes = None
        else:
            if "time_ms" in raw:
                raise GateError(f"{context} range query contains time_ms")
            start_ms = nonnegative_int(raw.get("start_ms"), f"{context}.start_ms")
            end_ms = nonnegative_int(raw.get("end_ms"), f"{context}.end_ms")
            step_ms = positive_int(raw.get("step_ms"), f"{context}.step_ms")
            cache_bytes = nonnegative_int(
                raw.get("range_scalar_cache_max_bytes"),
                f"{context}.range_scalar_cache_max_bytes",
            )
            if ((end_ms - start_ms) // step_ms) + 1 < 2:
                raise GateError(f"{context} is not a multi-step range query")
        if start_ms > end_ms:
            raise GateError(f"{context}.start_ms exceeds end_ms")
        groups: dict[str, str | None] = {}
        for field in ("equivalence_group", "semantic_group"):
            value = raw.get(field)
            groups[field] = None if value is None else checked_name(value, f"{context}.{field}")
        normalized.append(
            {
                "query_name": name,
                "category": category,
                "mode": mode,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "step_ms": step_ms,
                "range_scalar_cache_max_bytes": cache_bytes,
                "label_materialization": raw["label_materialization"],
                "materialization_expectation": raw["materialization_expectation"],
                "decode_expectation": raw["decode_expectation"],
                "expected_result": raw["expected_result"],
                **groups,
                "boundaries": [],
                "expression": expression,
            }
        )
    return {
        "schema": NORMALIZED_SCHEMA,
        "source_manifest_sha256": FIXED_MANIFEST_SHA256,
        "corpus": document["corpus"],
        "queries": normalized,
    }


def write_normalized(document: dict[str, Any], output_json: Path, output_tsv: Path) -> None:
    with output_json.open("x", encoding="utf-8") as destination:
        json.dump(document, destination, indent=2, sort_keys=True)
        destination.write("\n")
    fields = (
        "query_name",
        "category",
        "mode",
        "start_ms",
        "end_ms",
        "step_ms",
        "range_scalar_cache_max_bytes",
        "label_materialization",
        "expression",
    )
    with output_tsv.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=fields,
            delimiter="\t",
            lineterminator="\n",
            quoting=csv.QUOTE_NONE,
            quotechar=None,
        )
        writer.writeheader()
        for query in document["queries"]:
            writer.writerow(
                {
                    field: (
                        "-"
                        if query[field] is None
                        else query[field]
                    )
                    for field in fields
                }
            )


def read_normalized(path: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    exact_object(
        document,
        {"schema", "source_manifest_sha256", "corpus", "queries"},
        "normalized manifest",
    )
    if document["schema"] != NORMALIZED_SCHEMA:
        raise GateError("normalized manifest schema differs")
    if document["source_manifest_sha256"] != FIXED_MANIFEST_SHA256:
        raise GateError("normalized manifest is not derived from the fixed matrix")
    if tuple(query.get("query_name") for query in document["queries"]) != EXPECTED_QUERY_NAMES:
        raise GateError("normalized query matrix differs from the fixed matrix")
    return document


def manifest_file_bytes(files: list[dict[str, Any]]) -> bytes:
    lines = []
    for entry in files:
        path = entry["path"]
        if any(character in path for character in "\\\r\n"):
            raise GateError("Phase 1 corpus paths may not contain backslash or newline")
        lines.append(f"{entry['sha256']}  ./{path}\n".encode())
    return b"".join(lines)


def phase1_inventory(corpus: Path, normalized_manifest: Path) -> tuple[dict[str, Any], list[bytes]]:
    manifest = read_normalized(normalized_manifest)
    try:
        base, paths = common.inventory_corpus(corpus)
    except common.GateError as error:
        raise GateError(str(error)) from error
    manifest_digest = hashlib.sha256(manifest_file_bytes(base["files"])).hexdigest()
    expected = manifest["corpus"]
    actual = {
        "file_count": base["file_count"],
        "total_bytes": base["total_bytes"],
        "segments_manifest_sha256": manifest_digest,
    }
    if actual != expected:
        raise GateError(f"corpus differs from fixed Phase 1 identity: expected={expected!r} actual={actual!r}")
    return (
        {
            **base,
            "schema": INVENTORY_SCHEMA,
            "segments_manifest_sha256": manifest_digest,
            "fixed_identity": expected,
        },
        paths,
    )


def write_inventory(corpus: Path, manifest: Path, output: Path, paths_output: Path) -> None:
    inventory, paths = phase1_inventory(corpus, manifest)
    with output.open("x", encoding="utf-8") as destination:
        json.dump(inventory, destination, indent=2, sort_keys=True)
        destination.write("\n")
    with paths_output.open("xb") as destination:
        for path in paths:
            destination.write(path + b"\0")


def expected_plan(queries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for query in queries:
        for block, schedule in enumerate(ABBA_SCHEDULE, 1):
            for order_index, instrumentation in enumerate(schedule, 1):
                process_label = f"{query['query_name']}-b{block:02d}-{order_index:02d}-{instrumentation}"
                rows.append(
                    {
                        "process_label": process_label,
                        "query_name": query["query_name"],
                        "category": query["category"],
                        "mode": query["mode"],
                        "label_materialization": query["label_materialization"],
                        "abba_block": block,
                        "order_index": order_index,
                        "query_instrumentation": instrumentation,
                    }
                )
    return rows


def write_plan(manifest: Path, output: Path) -> None:
    queries = read_normalized(manifest)["queries"]
    fields = tuple(expected_plan(queries)[0])
    with output.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(expected_plan(queries))


def validate_named_numeric_list(
    value: Any, fields: set[str], name_field: str, context: str
) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise GateError(f"{context} must be an array")
    names: set[str] = set()
    result = []
    for index, item in enumerate(value):
        item_context = f"{context}[{index}]"
        obj = exact_object(item, fields | {name_field}, item_context)
        name = checked_text(obj[name_field], f"{item_context}.{name_field}")
        if name in names:
            raise GateError(f"{context} has duplicate {name_field} {name!r}")
        names.add(name)
        result.append(
            {
                name_field: name,
                **{
                    field: nonnegative_int(obj[field], f"{item_context}.{field}")
                    for field in fields
                },
            }
        )
    return result


def validate_metadata_runtime(value: Any, context: str) -> dict[str, Any]:
    report = exact_object(value, METADATA_TOP_FIELDS, context)
    counters = exact_object(report["counters_delta"], METADATA_COUNTER_FIELDS, f"{context}.counters_delta")
    cache = exact_object(counters["cache"], CACHE_COUNTER_FIELDS, f"{context}.counters_delta.cache")
    for field in CACHE_COUNTER_FIELDS - {"class_admissions"}:
        nonnegative_int(cache[field], f"{context}.counters_delta.cache.{field}")
    validate_named_numeric_list(
        cache["class_admissions"],
        {"resident_admissions", "resident_admission_refusals", "resident_admission_bypasses"},
        "class",
        f"{context}.counters_delta.cache.class_admissions",
    )
    numeric_object(counters["governor"], GOVERNOR_COUNTER_FIELDS, f"{context}.counters_delta.governor")
    numeric_object(counters["file_manager"], FILE_COUNTER_FIELDS, f"{context}.counters_delta.file_manager")
    reads = exact_object(counters["reads"], READ_DELTA_FIELDS, f"{context}.counters_delta.reads")
    numeric_object(reads["issued"], READ_COUNT_FIELDS, f"{context}.counters_delta.reads.issued")
    numeric_object(reads["unclassified"], READ_COUNT_FIELDS, f"{context}.counters_delta.reads.unclassified")
    validate_named_numeric_list(reads["by_file"], READ_COUNT_FIELDS, "file", f"{context}.counters_delta.reads.by_file")
    validate_named_numeric_list(reads["by_class"], READ_COUNT_FIELDS, "class", f"{context}.counters_delta.reads.by_class")

    for boundary in ("start_gauges", "end_gauges"):
        gauges = exact_object(report[boundary], GAUGE_TOP_FIELDS, f"{context}.{boundary}")
        cache_gauge = exact_object(gauges["cache"], CACHE_GAUGE_FIELDS, f"{context}.{boundary}.cache")
        for field in CACHE_GAUGE_FIELDS - {"class_charges"}:
            nonnegative_int(cache_gauge[field], f"{context}.{boundary}.cache.{field}")
        validate_named_numeric_list(
            cache_gauge["class_charges"],
            {"in_flight_bytes", "retained_bytes"},
            "class",
            f"{context}.{boundary}.cache.class_charges",
        )
        governor = exact_object(gauges["governor"], GOVERNOR_GAUGE_FIELDS, f"{context}.{boundary}.governor")
        for field in GOVERNOR_GAUGE_FIELDS - {"usage_charges"}:
            nonnegative_int(governor[field], f"{context}.{boundary}.governor.{field}")
        validate_named_numeric_list(
            governor["usage_charges"],
            {"in_flight_bytes", "retained_bytes"},
            "usage",
            f"{context}.{boundary}.governor.usage_charges",
        )
        if governor["retained_bytes"] > governor["retained_max_bytes"]:
            raise GateError(f"{context}.{boundary} retained governor exceeds its limit")
        if governor["in_flight_bytes"] > governor["in_flight_max_bytes"]:
            raise GateError(f"{context}.{boundary} in-flight governor exceeds its limit")
        file_manager = numeric_object(
            gauges["file_manager"], FILE_GAUGE_FIELDS, f"{context}.{boundary}.file_manager"
        )
        if file_manager["open_files"] > file_manager["max_open_files"]:
            raise GateError(f"{context}.{boundary} open files exceed their limit")

    peaks = exact_object(report["lifetime_peaks_after_run"], LIFETIME_FIELDS, f"{context}.lifetime_peaks_after_run")
    validate_named_numeric_list(
        peaks["cache_class_charges"],
        {"peak_in_flight_bytes", "peak_retained_bytes"},
        "class",
        f"{context}.lifetime_peaks_after_run.cache_class_charges",
    )
    governor_peaks = exact_object(
        peaks["governor"], GOVERNOR_PEAK_FIELDS, f"{context}.lifetime_peaks_after_run.governor"
    )
    for field in GOVERNOR_PEAK_FIELDS - {"usage_charges"}:
        nonnegative_int(governor_peaks[field], f"{context}.lifetime_peaks_after_run.governor.{field}")
    validate_named_numeric_list(
        governor_peaks["usage_charges"],
        {"peak_in_flight_bytes", "peak_retained_bytes"},
        "usage",
        f"{context}.lifetime_peaks_after_run.governor.usage_charges",
    )
    numeric_object(peaks["file_manager"], FILE_PEAK_FIELDS, f"{context}.lifetime_peaks_after_run.file_manager")
    return report


def validate_symbol_reads(value: Any, context: str) -> dict[str, Any]:
    report = exact_object(value, SYMBOL_READ_FIELDS, context)
    for field in (
        "legacy_eager_read_delta",
        "logical_returned_delta",
        "root_read_delta",
        "page_read_delta",
        "page_validation_delta",
    ):
        numeric_object(report[field], READ_COUNT_FIELDS, f"{context}.{field}")
    for field in SYMBOL_READ_FIELDS - {
        "legacy_eager_read_delta",
        "logical_returned_delta",
        "root_read_delta",
        "page_read_delta",
        "page_validation_delta",
    }:
        nonnegative_int(report[field], f"{context}.{field}")
    if report["legacy_eager_read_delta"] != {"calls": 0, "bytes": 0}:
        raise GateError(f"{context} unexpectedly exercised legacy eager symbols")
    if report["touched_corrupt_pages_delta"] != 0:
        raise GateError(f"{context} touched corrupt symbol pages")
    if report["resource_snapshot_errors_after_run"] != 0:
        raise GateError(f"{context} has symbol resource snapshot errors")
    retained_sum = (
        report["root_retained_charge_bytes_after_run"]
        + report["eager_dictionary_retained_charge_bytes_after_run"]
        + report["page_cache_charge_bytes_after_run"]
    )
    if report["total_retained_charge_bytes_after_run"] != retained_sum:
        raise GateError(f"{context} retained symbol charge is inconsistent")
    return report


def validate_range_cache(value: Any, query: dict[str, Any], context: str) -> dict[str, Any] | None:
    if query["mode"] == "instant":
        if value is not None:
            raise GateError(f"{context} instant query has range-cache stats")
        return None
    report = exact_object(value, RANGE_CACHE_FIELDS, f"{context}.range_scalar_cache")
    for field in RANGE_CACHE_FIELDS:
        if field in {"governor_refused", "allocation_refused", "layout_overflow"}:
            if not isinstance(report[field], bool):
                raise GateError(f"{context}.range_scalar_cache.{field} must be boolean")
        else:
            nonnegative_int(report[field], f"{context}.range_scalar_cache.{field}")
    if report["configured_budget_bytes"] != query["range_scalar_cache_max_bytes"]:
        raise GateError(f"{context} range-cache budget differs from the fixed matrix")
    if report["retained_charge_after_finalize"] != 0:
        raise GateError(f"{context} leaked retained range-cache charge after finalize")
    return report


def validate_raw(
    row: dict[str, str], query: dict[str, Any], args: argparse.Namespace
) -> tuple[str, list[dict[str, Any]]]:
    raw_path = Path(row["raw_output"])
    document = json.loads(raw_path.read_text(encoding="utf-8"))
    exact_object(document, RAW_DOCUMENT_FIELDS, f"{raw_path}")
    if document["schema"] != RAW_SCHEMA:
        raise GateError(f"{raw_path} raw schema must be {RAW_SCHEMA}")
    configuration = exact_object(
        document["configuration"], common.CONFIGURATION_FIELDS, f"{raw_path}.configuration"
    )
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
        "label_materialization": query["label_materialization"],
        "query_label_storage": "owned-strings",
        "query_instrumentation": row["query_instrumentation"],
        "storage_layout": "schema8",
        "benchmark_repeats": BENCHMARK_REPEATS,
        "queries": [query["expression"]],
        "prewarm_query_contexts": False,
        "prefetch_query_data": False,
        "exponential_histogram_bucket_boundaries": [],
        "requested_segment_footer_validation": False,
        "effective_segment_footer_validation": False,
    }
    if configuration != expected_configuration:
        raise GateError(f"{raw_path} timed configuration differs from the fixed invocation")
    expected_limits = {
        "max_matched_series": args.max_matched_series,
        "max_projected_series": args.max_projected_series,
        "max_chunk_reads": args.max_chunk_reads,
        "max_bytes_read": args.max_bytes_read,
        "max_samples_decoded": args.max_samples_decoded,
        "max_regex_values_examined": args.max_regex_values_examined,
    }
    if document["limits"] != expected_limits:
        raise GateError(f"{raw_path} query limits differ from the fixed invocation")
    corpus_fingerprint = digest(document["corpus_fingerprint_sha256"], f"{raw_path}.corpus_fingerprint")
    nonnegative_int(document["corpus_fingerprint_duration_ns"], f"{raw_path}.corpus_fingerprint_duration_ns")
    runs = document["runs"]
    if not isinstance(runs, list) or len(runs) != BENCHMARK_REPEATS:
        raise GateError(f"{raw_path} must contain exactly {BENCHMARK_REPEATS} runs")
    validated = []
    for run_index, run in enumerate(runs):
        context = f"{raw_path}:{query['query_name']}:{run_index}"
        exact_object(run, RAW_RUN_FIELDS, context)
        expected_kind = "cold" if run_index == 0 else "warm"
        if run["query"] != query["expression"]:
            raise GateError(f"{context} expression differs")
        if run["run_index"] != run_index or run["run_kind"] != expected_kind:
            raise GateError(f"{context} run index/kind differs")
        if (
            run["effective_start_ms"] != query["start_ms"]
            or run["effective_end_ms"] != query["end_ms"]
            or run["step_ms"] != query["step_ms"]
        ):
            raise GateError(f"{context} effective evaluation range differs")
        duration_ns = positive_int(run["duration_ns"], f"{context}.duration_ns")
        post_fingerprint_ns = nonnegative_int(
            run["post_query_fingerprint_ns"], f"{context}.post_query_fingerprint_ns"
        )
        try:
            stages = common.validate_query_stages(
                run["query_stages"], row["query_instrumentation"], duration_ns, context
            )
            stats = common.validate_stats(run["stats"], context)
            label_storage = common.validate_query_label_storage(
                run["query_label_storage"], context, "owned-strings"
            )
        except common.GateError as error:
            raise GateError(str(error)) from error
        if row["query_instrumentation"] == "detailed" and stages["exclusive_total_ns"] == 0:
            raise GateError(f"{context} detailed instrumentation recorded no stage work")
        payload = numeric_object(run["payload_reads"], PAYLOAD_FIELDS, f"{context}.payload_reads")
        if payload["logical_used_bytes"] != stats["bytes_read"]:
            raise GateError(f"{context} payload logical bytes differ from QueryStats")
        if payload["physical_bytes"] < payload["logical_used_bytes"]:
            raise GateError(f"{context} physical payload bytes are below logical bytes")
        labels = numeric_object(run["label_materialization"], LABEL_FIELDS, f"{context}.label_materialization")
        symbol_reads = validate_symbol_reads(run["symbol_reads"], f"{context}.symbol_reads")
        cache = validate_range_cache(run["range_scalar_cache"], query, context)
        metadata = validate_metadata_runtime(run["metadata_runtime"], f"{context}.metadata_runtime")
        result_series = nonnegative_int(run["result_series"], f"{context}.result_series")
        result_samples = nonnegative_int(run["result_samples"], f"{context}.result_samples")
        if query["expected_result"] == "empty":
            if result_series or result_samples or any(payload.values()):
                raise GateError(f"{context} fixed no-result control returned or read payload data")
        elif result_series == 0 or result_samples == 0:
            raise GateError(f"{context} fixed coverage query returned an empty result")
        expectation = query["materialization_expectation"]
        if expectation == "selective" and (
            labels["rows_selectively_materialized"] == 0 or labels["pairs_omitted"] == 0
        ):
            raise GateError(f"{context} did not exercise selective materialization")
        if expectation == "full" and (
            labels["rows_selectively_materialized"] != 0 or labels["pairs_omitted"] != 0
        ):
            raise GateError(f"{context} fixed full-demand path omitted labels")
        decode = query["decode_expectation"]
        if decode == "scalar" and stats["typed_scalar_chunks_decoded"] == 0:
            raise GateError(f"{context} did not exercise typed scalar decode")
        if decode == "full" and stats["typed_full_chunks_decoded"] == 0:
            raise GateError(f"{context} did not exercise typed full decode")
        validated.append(
            {
                "run_index": run_index,
                "run_kind": expected_kind,
                "duration_ns": duration_ns,
                "post_query_fingerprint_ns": post_fingerprint_ns,
                "semantic_fingerprint": digest(run["semantic_fingerprint_sha256"], f"{context}.semantic_fingerprint"),
                "portable_fingerprint": digest(
                    run["portable_semantic_fingerprint_sha256"], f"{context}.portable_fingerprint"
                ),
                "result_series": result_series,
                "result_samples": result_samples,
                "stats": stats,
                "payload": payload,
                "labels": labels,
                "label_storage": label_storage,
                "symbol_reads": symbol_reads,
                "range_cache": cache,
                "stages": stages,
                "metadata_runtime": metadata,
            }
        )
    return corpus_fingerprint, validated


def read_index(path: Path) -> list[dict[str, str]]:
    expected = {
        "process_label",
        "query_name",
        "category",
        "mode",
        "label_materialization",
        "abba_block",
        "order_index",
        "query_instrumentation",
        "corpus",
        "raw_output",
        "max_rss_kib",
    }
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if any(set(row) != expected for row in rows):
        raise GateError("raw index TSV has an invalid shape")
    return rows


def validate_residency(
    path: Path,
    process_labels: set[str],
    inventory: dict[str, Any],
    max_after_evict: int,
) -> None:
    expected_fields = {
        "process_label",
        "abba_block",
        "query_instrumentation",
        "phase",
        "file_count",
        "resident_bytes",
        "corpus_file_bytes",
    }
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if any(set(row) != expected_fields for row in rows):
        raise GateError("residency summary TSV has an invalid shape")
    seen: set[tuple[str, str]] = set()
    for row in rows:
        label = row["process_label"]
        phase = row["phase"]
        if label not in process_labels or phase not in {"after-evict", "after-run"}:
            raise GateError("residency summary contains an unknown process or phase")
        key = (label, phase)
        if key in seen:
            raise GateError(f"duplicate residency row: {key!r}")
        seen.add(key)
        if positive_int(int(row["file_count"]), "residency file count") != inventory["file_count"]:
            raise GateError(f"{label} residency file count differs from inventory")
        if nonnegative_int(int(row["corpus_file_bytes"]), "residency corpus bytes") != inventory["total_bytes"]:
            raise GateError(f"{label} residency bytes differ from inventory")
        resident = nonnegative_int(int(row["resident_bytes"]), "resident bytes")
        if phase == "after-evict" and resident > max_after_evict:
            raise GateError(f"{label} has {resident} resident bytes after eviction; limit is {max_after_evict}")
    expected = {(label, phase) for label in process_labels for phase in ("after-evict", "after-run")}
    if seen != expected:
        raise GateError("residency summary is incomplete")


def parse_markdown_metric(markdown: str, label: str) -> int:
    matches = re.findall(rf"^\| {re.escape(label)} \| ([0-9]+) \|$", markdown, re.MULTILINE)
    if len(matches) != 1:
        raise GateError(f"smoke report must contain exactly one {label!r} metric")
    return int(matches[0])


def validate_smoke_report(kind: str, report: Path, output: Path) -> None:
    markdown = report.read_text(encoding="utf-8")
    if "- Storage Layout: schema8" not in markdown:
        raise GateError("smoke report is not Schema 8")
    result: dict[str, Any] = {"schema": SMOKE_SCHEMA, "kind": kind}
    if kind == "footer":
        if (
            "- Requested Segment Footer Validation: true" not in markdown
            or "- Effective Segment Footer Validation: true" not in markdown
        ):
            raise GateError("footer validation was not requested and effective")
        result["requested"] = True
        result["effective"] = True
    elif kind == "readback":
        expected = parse_markdown_metric(markdown, "Expected Readback Queries")
        executed = parse_markdown_metric(markdown, "Executed Readback Queries")
        skipped = parse_markdown_metric(markdown, "Skipped Readback Queries")
        isolation_skips = parse_markdown_metric(markdown, "Isolation Check Skips")
        checked = parse_markdown_metric(markdown, "Checked Queries")
        mismatches = parse_markdown_metric(markdown, "Mismatches")
        if (expected, executed, skipped, isolation_skips, checked, mismatches) != (38, 38, 0, 0, 38, 0):
            raise GateError("readback verification must be exactly 38 expected/executed/checked with zero skips and mismatches")
        result.update(
            {
                "expected": expected,
                "executed": executed,
                "skipped": skipped,
                "isolation_skips": isolation_skips,
                "checked": checked,
                "mismatches": mismatches,
            }
        )
    else:
        raise GateError(f"unknown smoke report kind: {kind}")
    with output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def query_stats_digest(stats: dict[str, int]) -> str:
    return hashlib.sha256(canonical_json(stats).encode()).hexdigest()


def compare_results(args: argparse.Namespace) -> None:
    manifest = read_normalized(args.manifest)
    queries = manifest["queries"]
    query_by_name = {query["query_name"]: query for query in queries}
    plan = expected_plan(queries)
    plan_by_label = {row["process_label"]: row for row in plan}
    rows = read_index(args.index)
    if len(rows) != len(plan):
        raise GateError(f"expected {len(plan)} completed processes, found {len(rows)}")
    inventory_before = json.loads(args.inventory_before.read_text(encoding="utf-8"))
    inventory_after = json.loads(args.inventory_after.read_text(encoding="utf-8"))
    if inventory_before != inventory_after:
        raise GateError("corpus inventory changed during Phase 1 query measurement")
    if inventory_before.get("schema") != INVENTORY_SCHEMA:
        raise GateError("Phase 1 inventory has the wrong schema")
    inventory_identity = {
        "file_count": inventory_before.get("file_count"),
        "total_bytes": inventory_before.get("total_bytes"),
        "segments_manifest_sha256": inventory_before.get("segments_manifest_sha256"),
    }
    if (
        inventory_identity != manifest["corpus"]
        or inventory_before.get("fixed_identity") != manifest["corpus"]
    ):
        raise GateError("Phase 1 inventory does not match the fixed corpus identity")
    process_labels: set[str] = set()
    processes: dict[str, dict[str, Any]] = {}
    fingerprints: set[str] = set()
    for row in rows:
        label = row["process_label"]
        expected = plan_by_label.get(label)
        if expected is None or label in process_labels:
            raise GateError(f"unknown or duplicate process label: {label!r}")
        process_labels.add(label)
        for field in (
            "query_name",
            "category",
            "mode",
            "label_materialization",
            "query_instrumentation",
        ):
            if row[field] != str(expected[field]):
                raise GateError(f"{label} raw-index field {field} differs from the fixed plan")
        for field in ("abba_block", "order_index"):
            if positive_int(int(row[field]), f"{label}.{field}") != expected[field]:
                raise GateError(f"{label} raw-index field {field} differs from the fixed plan")
        if os.path.realpath(row["corpus"]) != inventory_before["corpus"]:
            raise GateError(f"{label} corpus differs from the inventoried corpus")
        max_rss = positive_int(int(row["max_rss_kib"]), f"{label}.max_rss_kib")
        fingerprint, runs = validate_raw(row, query_by_name[row["query_name"]], args)
        fingerprints.add(fingerprint)
        processes[label] = {"row": row, "runs": runs, "max_rss_kib": max_rss}
    if process_labels != set(plan_by_label):
        raise GateError("completed process set differs from the fixed ABBA plan")
    if len(fingerprints) != 1:
        raise GateError("query binary corpus fingerprint changed between processes")
    validate_residency(
        args.residency,
        process_labels,
        inventory_before,
        nonnegative_int(args.max_resident_bytes_after_evict, "max resident bytes after evict"),
    )
    footer = json.loads(args.footer_validation.read_text(encoding="utf-8"))
    readback = json.loads(args.readback_validation.read_text(encoding="utf-8"))
    if footer != {"schema": SMOKE_SCHEMA, "kind": "footer", "requested": True, "effective": True}:
        raise GateError("footer validation artifact is invalid")
    if (
        readback.get("schema") != SMOKE_SCHEMA
        or readback.get("kind") != "readback"
        or [readback.get(field) for field in ("expected", "executed", "checked", "skipped", "isolation_skips", "mismatches")]
        != [38, 38, 38, 0, 0, 0]
    ):
        raise GateError("readback validation artifact is invalid")

    canonical: dict[tuple[str, int], dict[str, Any]] = {}
    for query in queries:
        name = query["query_name"]
        matching = [processes[row["process_label"]] for row in plan if row["query_name"] == name]
        for run_index in range(BENCHMARK_REPEATS):
            candidates = [process["runs"][run_index] for process in matching]
            first = candidates[0]
            for candidate in candidates[1:]:
                for field in (
                    "semantic_fingerprint",
                    "portable_fingerprint",
                    "result_series",
                    "result_samples",
                    "stats",
                ):
                    if candidate[field] != first[field]:
                        raise GateError(f"{name} run {run_index} {field} differs across Off/Detailed ABBA")
            canonical[(name, run_index)] = first
        if len({canonical[(name, run_index)]["semantic_fingerprint"] for run_index in range(BENCHMARK_REPEATS)}) != 1:
            raise GateError(f"{name} semantic fingerprint differs between cold and warm runs")
        if len({canonical[(name, run_index)]["portable_fingerprint"] for run_index in range(BENCHMARK_REPEATS)}) != 1:
            raise GateError(f"{name} portable fingerprint differs between cold and warm runs")

    equivalence_groups: dict[str, list[str]] = {}
    semantic_groups: dict[str, list[str]] = {}
    for query in queries:
        if query["equivalence_group"]:
            equivalence_groups.setdefault(query["equivalence_group"], []).append(query["query_name"])
        if query["semantic_group"]:
            semantic_groups.setdefault(query["semantic_group"], []).append(query["query_name"])
    for group, names in equivalence_groups.items():
        if len(names) < 2:
            raise GateError(f"equivalence group {group!r} has no comparator")
        for run_index in range(BENCHMARK_REPEATS):
            first = canonical[(names[0], run_index)]
            for name in names[1:]:
                candidate = canonical[(name, run_index)]
                for field in (
                    "semantic_fingerprint",
                    "portable_fingerprint",
                    "result_series",
                    "result_samples",
                    "stats",
                ):
                    if candidate[field] != first[field]:
                        raise GateError(f"equivalence group {group} run {run_index} differs in {field}")
    for group, names in semantic_groups.items():
        if len(names) < 2:
            raise GateError(f"semantic group {group!r} has no comparator")
        for run_index in range(BENCHMARK_REPEATS):
            first = canonical[(names[0], run_index)]
            for name in names[1:]:
                candidate = canonical[(name, run_index)]
                for field in ("semantic_fingerprint", "portable_fingerprint", "result_series", "result_samples"):
                    if candidate[field] != first[field]:
                        raise GateError(f"semantic group {group} run {run_index} differs in {field}")

    stage_fields = sorted(common.QUERY_STAGE_FIELDS)
    summary_fields = [
        "process_label",
        "query_name",
        "category",
        "mode",
        "label_materialization",
        "abba_block",
        "order_index",
        "query_instrumentation",
        "run_index",
        "run_kind",
        "duration_ns",
        "post_query_fingerprint_ns",
        "max_rss_kib",
        "result_series",
        "result_samples",
        "semantic_fingerprint",
        "portable_fingerprint",
        "query_stats_sha256",
        *(f"stats_{field}" for field in common.QUERY_STATS_FIELDS),
        "payload_logical_used_bytes",
        "payload_physical_reads",
        "payload_physical_bytes",
        "payload_read_amplification",
        *(f"stage_{field}" for field in stage_fields),
        "label_materialization_json",
        "query_label_storage_json",
        "symbol_reads_json",
        "metadata_runtime_json",
        "range_scalar_cache_json",
    ]
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=summary_fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for planned in plan:
            process = processes[planned["process_label"]]
            for run in process["runs"]:
                payload = run["payload"]
                row: dict[str, Any] = {
                    **planned,
                    "run_index": run["run_index"],
                    "run_kind": run["run_kind"],
                    "duration_ns": run["duration_ns"],
                    "post_query_fingerprint_ns": run["post_query_fingerprint_ns"],
                    "max_rss_kib": process["max_rss_kib"],
                    "result_series": run["result_series"],
                    "result_samples": run["result_samples"],
                    "semantic_fingerprint": run["semantic_fingerprint"],
                    "portable_fingerprint": run["portable_fingerprint"],
                    "query_stats_sha256": query_stats_digest(run["stats"]),
                    "payload_logical_used_bytes": payload["logical_used_bytes"],
                    "payload_physical_reads": payload["physical_reads"],
                    "payload_physical_bytes": payload["physical_bytes"],
                    "payload_read_amplification": (
                        ""
                        if payload["logical_used_bytes"] == 0
                        else f"{payload['physical_bytes'] / payload['logical_used_bytes']:.9f}"
                    ),
                    "label_materialization_json": canonical_json(run["labels"]),
                    "query_label_storage_json": canonical_json(run["label_storage"]),
                    "symbol_reads_json": canonical_json(run["symbol_reads"]),
                    "metadata_runtime_json": canonical_json(run["metadata_runtime"]),
                    "range_scalar_cache_json": (
                        "" if run["range_cache"] is None else canonical_json(run["range_cache"])
                    ),
                }
                row.update({f"stats_{field}": run["stats"][field] for field in common.QUERY_STATS_FIELDS})
                row.update({f"stage_{field}": run["stages"][field] for field in stage_fields})
                writer.writerow(row)

    result = {
        "schema": RESULT_SCHEMA,
        "status": "pass",
        "fixed_manifest_sha256": FIXED_MANIFEST_SHA256,
        "query_names": list(EXPECTED_QUERY_NAMES),
        "abba_schedule": [list(block) for block in ABBA_SCHEDULE],
        "benchmark_repeats": BENCHMARK_REPEATS,
        "completed_processes": len(processes),
        "completed_query_runs": len(processes) * BENCHMARK_REPEATS,
        "corpus_inventory_sha256": inventory_before["segments_manifest_sha256"],
        "query_corpus_fingerprint_sha256": next(iter(fingerprints)),
        "footer_validation": "pass",
        "readback_validation": "38/38; zero skips; zero mismatches",
        "off_detailed_fingerprint_and_query_stats_equivalence": "pass",
        "full_demand_control_equivalence": "pass",
        "range_cache_semantic_equivalence": "pass",
        "page_cache_claim": "all inventoried files were at or below the configured residency threshold immediately before each fresh process; startup may touch files before the timed query",
    }
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    normalize = commands.add_parser("normalize-manifest")
    normalize.add_argument("--input", type=Path, required=True)
    normalize.add_argument("--output-json", type=Path, required=True)
    normalize.add_argument("--output-tsv", type=Path, required=True)
    plan = commands.add_parser("write-plan")
    plan.add_argument("--manifest", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    inventory = commands.add_parser("inventory")
    inventory.add_argument("--corpus", type=Path, required=True)
    inventory.add_argument("--manifest", type=Path, required=True)
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
    compare.add_argument("--queue-depth", type=int, required=True)
    compare.add_argument("--max-resident-bytes-after-evict", type=int, required=True)
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
            write_normalized(normalize_manifest(args.input), args.output_json, args.output_tsv)
        elif args.command == "write-plan":
            write_plan(args.manifest, args.output)
        elif args.command == "inventory":
            write_inventory(args.corpus, args.manifest, args.output, args.paths_output)
        elif args.command == "validate-smoke-report":
            validate_smoke_report(args.kind, args.report, args.output)
        elif args.command == "compare-results":
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
        print(f"Phase 1 query gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
