#!/usr/bin/env python3
"""Strict same-binary Phase 3 payload-coalescing benchmark gate."""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import os
import re
import stat
import statistics
import sys
from pathlib import Path
from typing import Any

import phase1_query_gate as phase1
import phase2_compact_ids_ab_gate as phase2
import schema7_query_ab_gate as common
import schema8_query_ab_gate as manifest_gate


RAW_SCHEMA = "chronoxide.query-benchmark.raw/v13"
RESULT_SCHEMA = "chronoxide/storage-vnext-phase3-payload-coalescing/v2"
BACKEND_COMPARISON_SCHEMA = (
    "chronoxide/storage-vnext-phase3-payload-coalescing-backend-comparison/v4"
)
NORMALIZED_SCHEMA = "chronoxide/storage-vnext-phase3-query-manifest-normalized/v1"
SMOKE_SCHEMA = "chronoxide/storage-vnext-phase3-smoke-validation/v1"
SEALED_QUERY_MANIFEST_SHA256 = (
    "3420740cc3e5eb38e82ca53b58d6d1a075b9007380b8745e0193ec18236a07e7"
)
GAPS = (0, 256, 1024, 4096)
WILLIAMS_SQUARE = (
    (0, 256, 4096, 1024),
    (256, 1024, 0, 4096),
    (1024, 4096, 256, 0),
    (4096, 0, 1024, 256),
)
BLOCKS = 8
BENCHMARK_REPEATS = 3
BACKENDS = ("pread", "io-uring")
DEFAULT_ARENA_BYTES = 512 * 1024 * 1024
EXPECTED_QUERY_NAMES = (
    "broad_raw_count_selector",
    "equality_last",
    "sparse_regex_last",
    "negative_matcher_last",
    "no_result",
    "scalar_rate_sum_instant",
    "scalar_rate_sum_range",
    "native_hist_count_range",
    "native_hist_p95_range",
    "native_exp_count_range",
    "native_exp_p95_range",
)

DOCUMENT_FIELDS = phase2.DOCUMENT_FIELDS
CONFIGURATION_FIELDS = phase2.CONFIGURATION_FIELDS | {
    "chunk_payload_coalesce_max_gap_bytes"
}
RUN_FIELDS = phase2.RUN_FIELDS | {"chunk_read_scheduler"}
PAYLOAD_FIELDS = phase2.PAYLOAD_FIELDS
LABEL_FIELDS = phase2.LABEL_FIELDS
LABEL_STORAGE_FIELDS = phase2.LABEL_STORAGE_FIELDS
SCHEDULER_FIELDS = frozenset(
    {
        "executions",
        "pread_decisions",
        "io_uring_decisions",
        "logical_requests",
        "physical_spans",
        "backend_submissions",
        "sqes_submitted",
        "submission_depth_sum",
        "session_submission_depth_high_water",
        "submission_depth_1",
        "submission_depth_2_3",
        "submission_depth_4_7",
        "submission_depth_8_plus",
        "total_physical_bytes_executed",
        "session_peak_in_flight_bytes_high_water",
    }
)
SCHEDULER_SESSION_HIGH_WATER_FIELDS = frozenset(
    {
        "session_submission_depth_high_water",
        "session_peak_in_flight_bytes_high_water",
    }
)
SCHEDULER_COUNTER_FIELDS = SCHEDULER_FIELDS - SCHEDULER_SESSION_HIGH_WATER_FIELDS
INDEX_FIELDS = {
    "process_label",
    "query_name",
    "category",
    "mode",
    "block",
    "order_index",
    "chunk_read_backend",
    "payload_coalesce_max_gap_bytes",
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
    "chunk_read_backend",
    "payload_coalesce_max_gap_bytes",
    "phase",
    "file_count",
    "resident_bytes",
    "corpus_file_bytes",
}
INVENTORY_FIELDS = {
    "schema",
    "corpus",
    "corpus_sha256",
    "file_count",
    "total_bytes",
    "files",
}
INVENTORY_FILE_FIELDS = {"path", "size_bytes", "sha256"}
FOOTER_SMOKE_FIELDS = {"schema", "kind", "gate", "requested", "effective"}
READBACK_SMOKE_FIELDS = {
    "schema",
    "kind",
    "gate",
    "expected",
    "executed",
    "skipped",
    "isolation_skips",
    "checked",
    "mismatches",
}
PREFLIGHT_SMOKE_FIELDS = {
    "schema",
    "kind",
    "gate",
    "raw_schema",
    "chunk_read_mode",
    "queue_depth",
    "binary_sha256",
    "preflight_raw_sha256",
    "corpus",
    "corpus_fingerprint_sha256",
    "query_name",
}
BACKEND_ACCOUNTING_FIELDS = {
    "query_name",
    "run_index",
    "payload_coalesce_max_gap_bytes",
    "nonphysical_correctness_sha256",
}
MEASUREMENT_FIELDS = {
    "query_name",
    "payload_coalesce_max_gap_bytes",
    "cold_duration_ns",
    "cold_median_ns",
    "warm_duration_ns",
    "process_warm_median_ns",
    "warm_median_ns",
    "process_max_rss_kib",
    "process_max_rss_median_kib",
    "accounting_by_run_index",
}
MEASUREMENT_ACCOUNTING_FIELDS = {
    "run_index",
    "logical_used_bytes",
    "physical_spans",
    "physical_bytes",
    "scheduler",
}
EXACT_ACROSS_GAPS = (
    "semantic_and_portable_fingerprints",
    "result_series_and_samples",
    "all_QueryStats_fields",
    "logical_payload_bytes_and_requests",
    "label_materialization_and_compact_arena_accounting",
    "range_scalar_cache_accounting",
    "all_non-timing_symbol_and_metadata_accounting",
)
ALLOWED_ACROSS_GAP_DIFFERENCES = (
    "physical payload spans and bytes",
    "chunk-read scheduler physical/backend/submission accounting",
    "query, fingerprint, process, and symbol-page-validation time",
    "process maximum RSS",
)
RESULT_RELATIVE_PATH = Path("comparisons/result-gate.json")
RESULT_CHECKSUM_RELATIVE_PATH = Path("metadata/result-artifacts.sha256")
RESULT_COMPLETE_RELATIVE_PATH = Path("COMPLETE")
SHA256_MANIFEST_LINE = re.compile(r"^([0-9a-f]{64})  (.+)$")
RESULT_FIELDS = {
    "schema",
    "correctness_gate",
    "monotonic_physical_plan_gate",
    "backend",
    "queue_depth",
    "gaps",
    "williams_square",
    "blocks",
    "schedule_repetitions",
    "processes_per_gap_per_query",
    "benchmark_repeats",
    "query_label_storage",
    "query_label_arena_max_bytes",
    "max_resident_bytes_after_evict",
    "os_page_cache_eviction_gate",
    "warm_headline_observation_unit",
    "sealed_query_manifest_sha256",
    "binary_sha256",
    "corpus_inventory_sha256",
    "query_corpus_fingerprint_sha256",
    "io_uring_preflight",
    "nonphysical_accounting_by_query_run_gap",
    "exact_across_gaps",
    "allowed_across_gap_differences",
    "measurements",
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


def exact_object(
    value: Any, fields: set[str] | frozenset[str], context: str
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(fields):
        raise GateError(f"{context} has an invalid shape")
    return value


def numeric_object(
    value: Any, fields: set[str] | frozenset[str], context: str
) -> dict[str, int]:
    obj = exact_object(value, fields, context)
    return {
        field: nonnegative_int(obj[field], f"{context}.{field}") for field in fields
    }


def canonical_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            hasher.update(block)
    return hasher.hexdigest()


def canonical_directory(path: Path, context: str) -> Path:
    if not path.is_absolute():
        raise GateError(f"{context} must be absolute")
    absolute = Path(os.path.abspath(path))
    canonical = Path(os.path.realpath(path))
    if absolute != canonical:
        raise GateError(f"{context} must be canonical and contain no symlink components")
    metadata = os.lstat(canonical)
    if not stat.S_ISDIR(metadata.st_mode):
        raise GateError(f"{context} must be a directory")
    return canonical


def canonical_regular_file(path: Path, expected: Path, context: str) -> Path:
    if not path.is_absolute() or path != expected:
        raise GateError(f"{context} must be exactly {expected}")
    absolute = Path(os.path.abspath(path))
    canonical = Path(os.path.realpath(path))
    if absolute != canonical:
        raise GateError(f"{context} must be canonical and contain no symlink components")
    metadata = os.lstat(canonical)
    if not stat.S_ISREG(metadata.st_mode):
        raise GateError(f"{context} must be a regular non-symlink file")
    return canonical


def raw_backend_name(backend: str) -> str:
    if backend == "pread":
        return "pread"
    if backend == "io-uring":
        return "io_uring"
    raise GateError(f"unsupported forced backend: {backend!r}")


def load_source_manifest(path: Path) -> list[dict[str, Any]]:
    if file_sha256(path) != SEALED_QUERY_MANIFEST_SHA256:
        raise GateError("query manifest bytes differ from the sealed Phase 2 matrix")
    try:
        queries = manifest_gate.normalize_manifest(path, 0)
    except manifest_gate.GateError as error:
        raise GateError(str(error)) from error
    if tuple(query["query_name"] for query in queries) != EXPECTED_QUERY_NAMES:
        raise GateError("sealed query names or order differ from the Phase 3 contract")
    categories = {query["category"] for query in queries}
    if not phase2.REQUIRED_CATEGORIES.issubset(categories):
        missing = sorted(phase2.REQUIRED_CATEGORIES - categories)
        raise GateError(f"sealed matrix lacks required categories: {missing!r}")
    for query in queries:
        if query["mode"] == "range" and query["range_scalar_cache_max_bytes"] != 0:
            raise GateError(f"{query['query_name']}: range scalar cache must be disabled")
    return queries


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
                        else ",".join(
                            manifest_gate.boundary_text(value)
                            for value in query["boundaries"]
                        )
                    ),
                    "expression": query["expression"],
                }
            )
    with output_json.open("x", encoding="utf-8") as destination:
        json.dump(
            {
                "schema": NORMALIZED_SCHEMA,
                "source_manifest_sha256": SEALED_QUERY_MANIFEST_SHA256,
                "queries": queries,
            },
            destination,
            indent=2,
            sort_keys=True,
        )
        destination.write("\n")


def normalize_manifest(input_path: Path, output_tsv: Path, output_json: Path) -> None:
    write_normalized_manifest(load_source_manifest(input_path), output_tsv, output_json)


def read_manifest(path: Path, source_manifest: Path) -> list[dict[str, Any]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    exact_object(
        document,
        {"schema", "source_manifest_sha256", "queries"},
        "normalized manifest",
    )
    if document["schema"] != NORMALIZED_SCHEMA:
        raise GateError("normalized manifest has the wrong schema")
    if document["source_manifest_sha256"] != SEALED_QUERY_MANIFEST_SHA256:
        raise GateError("normalized manifest does not name the sealed Phase 2 matrix")
    expected = load_source_manifest(source_manifest)
    if document["queries"] != expected:
        raise GateError("normalized manifest differs from the sealed source matrix")
    return expected


def schedule_for_block(block: int) -> tuple[int, int, int, int]:
    if block < 1 or block > BLOCKS:
        raise GateError(f"block must be in 1..={BLOCKS}")
    return WILLIAMS_SQUARE[(block - 1) % len(WILLIAMS_SQUARE)]


def expected_plan(
    queries: list[dict[str, Any]], backend: str
) -> list[dict[str, Any]]:
    raw_backend_name(backend)
    rows: list[dict[str, Any]] = []
    for query in queries:
        for block in range(1, BLOCKS + 1):
            for order_index, gap in enumerate(schedule_for_block(block), 1):
                rows.append(
                    {
                        "process_label": (
                            f"{query['query_name']}-b{block:02d}-{order_index:02d}-gap{gap:04d}"
                        ),
                        "query_name": query["query_name"],
                        "category": query["category"],
                        "mode": query["mode"],
                        "block": block,
                        "order_index": order_index,
                        "chunk_read_backend": backend,
                        "payload_coalesce_max_gap_bytes": gap,
                    }
                )
    return rows


def write_plan(
    manifest: Path, source_manifest: Path, output: Path, backend: str
) -> None:
    rows = expected_plan(read_manifest(manifest, source_manifest), backend)
    with output.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=tuple(rows[0]),
            delimiter="\t",
            lineterminator="\n",
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
        if metrics["expected"] <= 0 or not (
            metrics["executed"] == metrics["expected"]
            and metrics["checked"] == metrics["expected"]
            and metrics["skipped"] == 0
            and metrics["isolation_skips"] == 0
            and metrics["mismatches"] == 0
        ):
            raise GateError(
                "readback oracle must execute/check every expected case with no skips or mismatches"
            )
        result.update(metrics)
    else:
        raise GateError(f"unknown smoke report kind: {kind}")
    with output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def storage_activity_values(storage: dict[str, int]) -> dict[str, int]:
    return {
        field: value
        for field, value in storage.items()
        if field != "compact_arena_budget_bytes"
    }


def limit_configuration(args: argparse.Namespace) -> dict[str, int]:
    return {
        "max_matched_series": args.max_matched_series,
        "max_projected_series": args.max_projected_series,
        "max_chunk_reads": args.max_chunk_reads,
        "max_bytes_read": args.max_bytes_read,
        "max_samples_decoded": args.max_samples_decoded,
        "max_regex_values_examined": args.max_regex_values_examined,
    }


def validate_io_uring_preflight(
    raw_path: Path,
    binary: Path,
    corpus: Path,
    query: dict[str, Any],
    expected_queue_depth: int,
    arena_bytes: int,
    limits: dict[str, int],
    output: Path,
) -> None:
    if query["category"] != "empty-result-control" or query["mode"] != "instant":
        raise GateError("io_uring preflight query must be the sealed no-result instant control")
    canonical_regular_file(raw_path, raw_path, "io_uring preflight raw")
    document = json.loads(raw_path.read_text(encoding="utf-8"))
    exact_object(document, DOCUMENT_FIELDS, str(raw_path))
    if document["schema"] != RAW_SCHEMA:
        raise GateError(f"io_uring preflight raw schema must be {RAW_SCHEMA}")
    nonnegative_int(
        document["corpus_fingerprint_duration_ns"],
        "io_uring preflight corpus_fingerprint_duration_ns",
    )
    corpus_fingerprint = digest(
        document["corpus_fingerprint_sha256"],
        "io_uring preflight corpus_fingerprint_sha256",
    )
    configuration = exact_object(
        document["configuration"], CONFIGURATION_FIELDS, "io_uring preflight configuration"
    )
    expected_configuration = {
        "segments_dir": os.path.realpath(corpus),
        "start_ms": query["start_ms"],
        "end_ms": query["end_ms"],
        "mode": "instant",
        "step_ms": None,
        "range_scalar_cache_max_bytes": None,
        "chunk_read_mode": "io_uring",
        "chunk_read_queue_depth": expected_queue_depth,
        "chunk_payload_coalesce_max_gap_bytes": 0,
        "experimental_cross_segment_chunk_reads": False,
        "label_materialization": "demand-driven",
        "query_label_storage": "compact-ids",
        "query_instrumentation": "off",
        "query_label_arena_max_bytes": arena_bytes,
        "storage_layout": "schema8",
        "benchmark_repeats": 1,
        "queries": [query["expression"]],
        "prewarm_query_contexts": False,
        "prefetch_query_data": False,
        "exponential_histogram_bucket_boundaries": query["boundaries"],
        "requested_segment_footer_validation": False,
        "effective_segment_footer_validation": False,
    }
    if configuration != expected_configuration:
        raise GateError("io_uring preflight configuration differs from the fixed invocation")
    if document["limits"] != limits:
        raise GateError("io_uring preflight query limits differ from the fixed invocation")
    runs = document["runs"]
    if not isinstance(runs, list) or len(runs) != 1:
        raise GateError("io_uring preflight raw output must contain one run")
    run = exact_object(runs[0], RUN_FIELDS, "io_uring preflight run")
    if run["run_index"] != 0 or run["run_kind"] != "cold":
        raise GateError("io_uring preflight run index/kind is invalid")
    if run["query"] != query["expression"]:
        raise GateError("io_uring preflight expression differs from the sealed no-result query")
    if (
        run["effective_start_ms"] != query["start_ms"]
        or run["effective_end_ms"] != query["end_ms"]
        or run["step_ms"] is not None
    ):
        raise GateError("io_uring preflight evaluation range differs from the sealed query")
    duration_ns = positive_int(run["duration_ns"], "io_uring preflight duration_ns")
    nonnegative_int(
        run["post_query_fingerprint_ns"], "io_uring preflight post_query_fingerprint_ns"
    )
    digest(run["semantic_fingerprint_sha256"], "io_uring preflight semantic fingerprint")
    digest(
        run["portable_semantic_fingerprint_sha256"],
        "io_uring preflight portable semantic fingerprint",
    )
    try:
        stats = common.validate_stats(run["stats"], "io_uring preflight")
        common.validate_query_stages(
            run["query_stages"], "off", duration_ns, "io_uring preflight"
        )
        phase1.validate_symbol_reads(run["symbol_reads"], "io_uring preflight symbol_reads")
        phase1.validate_metadata_runtime(
            run["metadata_runtime"], "io_uring preflight metadata_runtime"
        )
        phase1.validate_range_cache(
            run["range_scalar_cache"], query, "io_uring preflight"
        )
    except (common.GateError, phase1.GateError) as error:
        raise GateError(str(error)) from error
    payload = numeric_object(run["payload_reads"], PAYLOAD_FIELDS, "preflight payload")
    labels = phase2.validate_label_materialization(
        run["label_materialization"], query["category"], "io_uring preflight labels"
    )
    storage = phase2.validate_label_storage(
        run["query_label_storage"],
        "compact-ids",
        arena_bytes,
        False,
        True,
        "io_uring preflight query_label_storage",
    )
    scheduler = validate_scheduler(
        run["chunk_read_scheduler"],
        "io-uring",
        expected_queue_depth,
        payload,
        stats,
        "io_uring preflight scheduler",
    )
    if run["result_series"] != 0 or run["result_samples"] != 0:
        raise GateError("io_uring preflight no-result expression returned data")
    payload_stats = (
        "matched_series",
        "projected_series",
        "chunk_reads",
        "bytes_read",
        "samples_decoded",
        "typed_scalar_chunks_decoded",
        "typed_full_chunks_decoded",
    )
    if (
        any(payload.values())
        or any(scheduler.values())
        or any(labels.values())
        or any(storage_activity_values(storage).values())
        or any(stats[field] for field in payload_stats)
    ):
        raise GateError("io_uring preflight no-result expression performed payload work")
    result = {
        "schema": SMOKE_SCHEMA,
        "kind": "io_uring_preflight",
        "gate": "pass",
        "raw_schema": RAW_SCHEMA,
        "chunk_read_mode": "io_uring",
        "queue_depth": expected_queue_depth,
        "binary_sha256": file_sha256(binary),
        "preflight_raw_sha256": file_sha256(raw_path),
        "corpus": os.path.realpath(corpus),
        "corpus_fingerprint_sha256": corpus_fingerprint,
        "query_name": query["query_name"],
    }
    with output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def validate_scheduler(
    value: Any,
    backend: str,
    queue_depth: int,
    payload: dict[str, int],
    stats: dict[str, int],
    context: str,
) -> dict[str, int]:
    scheduler = numeric_object(value, SCHEDULER_FIELDS, context)
    if scheduler["executions"] != (
        scheduler["pread_decisions"] + scheduler["io_uring_decisions"]
    ):
        raise GateError(f"{context}: backend decisions do not equal executions")
    if scheduler["logical_requests"] != stats["chunk_reads"]:
        raise GateError(f"{context}: scheduler logical requests differ from QueryStats")
    if scheduler["physical_spans"] != payload["physical_reads"]:
        raise GateError(f"{context}: scheduler spans differ from payload physical reads")
    if scheduler["total_physical_bytes_executed"] != payload["physical_bytes"]:
        raise GateError(f"{context}: scheduler executed bytes differ from payload bytes")
    if scheduler["physical_spans"] > scheduler["logical_requests"]:
        raise GateError(f"{context}: physical spans exceed logical requests")
    if (
        scheduler["session_peak_in_flight_bytes_high_water"]
        > scheduler["total_physical_bytes_executed"]
    ):
        raise GateError(f"{context}: peak in-flight bytes exceed total executed bytes")

    spans = scheduler["physical_spans"]
    if spans == 0:
        if any(scheduler.values()):
            raise GateError(f"{context}: zero-span scheduler accounting is not all zero")
        return scheduler
    if (
        scheduler["executions"] == 0
        or scheduler["session_peak_in_flight_bytes_high_water"] == 0
    ):
        raise GateError(f"{context}: payload work lacks scheduler execution/peak evidence")
    if scheduler["backend_submissions"] < scheduler["executions"]:
        raise GateError(f"{context}: backend submissions are below executions")

    if backend == "pread":
        expected = {
            "pread_decisions": scheduler["executions"],
            "io_uring_decisions": 0,
            "backend_submissions": spans,
            "sqes_submitted": 0,
            "submission_depth_sum": spans,
            "session_submission_depth_high_water": 1,
            "submission_depth_1": spans,
            "submission_depth_2_3": 0,
            "submission_depth_4_7": 0,
            "submission_depth_8_plus": 0,
        }
        for field, expected_value in expected.items():
            if scheduler[field] != expected_value:
                raise GateError(f"{context}: forced pread invariant failed for {field}")
    elif backend == "io-uring":
        if scheduler["pread_decisions"] != 0:
            raise GateError(f"{context}: forced io_uring made a pread decision")
        if scheduler["io_uring_decisions"] != scheduler["executions"]:
            raise GateError(f"{context}: forced io_uring decision count is invalid")
        if scheduler["sqes_submitted"] != spans:
            raise GateError(f"{context}: io_uring SQEs differ from physical spans")
        if scheduler["submission_depth_sum"] != spans:
            raise GateError(f"{context}: io_uring depth sum differs from physical spans")
        buckets = (
            scheduler["submission_depth_1"],
            scheduler["submission_depth_2_3"],
            scheduler["submission_depth_4_7"],
            scheduler["submission_depth_8_plus"],
        )
        if sum(buckets) != scheduler["backend_submissions"]:
            raise GateError(f"{context}: io_uring submission buckets do not reconcile")
        high_water = scheduler["session_submission_depth_high_water"]
        if not (1 <= high_water <= queue_depth):
            raise GateError(f"{context}: io_uring maximum depth exceeds queue depth")
        bucket_ranges = (
            (buckets[0], 1, 1),
            (buckets[1], 2, min(3, queue_depth)),
            (buckets[2], 4, min(7, queue_depth)),
            (buckets[3], 8, queue_depth),
        )
        highest_range = next(
            ((low, high) for count, low, high in reversed(bucket_ranges) if count),
            None,
        )
        if (
            highest_range is None
            or highest_range[1] < highest_range[0]
            or not (highest_range[0] <= high_water <= highest_range[1])
        ):
            raise GateError(
                f"{context}: io_uring maximum depth is incompatible with its highest bucket"
            )
        minimum_depth_sum = buckets[0] + 2 * buckets[1] + 4 * buckets[2] + 8 * buckets[3]
        maximum_depth_sum = (
            buckets[0]
            + 3 * buckets[1]
            + 7 * buckets[2]
            + queue_depth * buckets[3]
        )
        if buckets[3] and queue_depth < 8:
            raise GateError(f"{context}: depth-8 bucket used below queue depth 8")
        if not (minimum_depth_sum <= spans <= maximum_depth_sum):
            raise GateError(f"{context}: io_uring depth buckets cannot explain SQEs")
    else:
        raise GateError(f"{context}: unsupported backend {backend!r}")
    return scheduler


def comparable_symbols(value: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(value)
    result["page_validation_ns_delta"] = 0
    return result


def non_payload_signature(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "semantic_fingerprint": run["semantic_fingerprint"],
        "portable_fingerprint": run["portable_fingerprint"],
        "result_series": run["result_series"],
        "result_samples": run["result_samples"],
        "stats": run["stats"],
        "payload_logical_used_bytes": run["payload"]["logical_used_bytes"],
        "scheduler_logical_requests": run["scheduler"]["logical_requests"],
        "labels": run["labels"],
        "label_storage": run["label_storage"],
        "symbols": comparable_symbols(run["symbols"]),
        "metadata": run["metadata"],
        "range_cache": run["range_cache"],
    }


def physical_signature(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "physical_reads": run["payload"]["physical_reads"],
        "physical_bytes": run["payload"]["physical_bytes"],
        "scheduler": run["scheduler"],
    }


def repeated_accounting_signature(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "stats": run["stats"],
        "payload": run["payload"],
        "scheduler_counters": {
            field: run["scheduler"][field] for field in SCHEDULER_COUNTER_FIELDS
        },
    }


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
        "chunk_read_mode": raw_backend_name(args.backend),
        "chunk_read_queue_depth": args.queue_depth,
        "chunk_payload_coalesce_max_gap_bytes": int(
            row["payload_coalesce_max_gap_bytes"]
        ),
        "experimental_cross_segment_chunk_reads": False,
        "label_materialization": "demand-driven",
        "query_label_storage": "compact-ids",
        "query_instrumentation": "off",
        "query_label_arena_max_bytes": args.arena_bytes,
        "storage_layout": "schema8",
        "benchmark_repeats": BENCHMARK_REPEATS,
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
    if not isinstance(runs, list) or len(runs) != BENCHMARK_REPEATS:
        raise GateError(f"{raw_path}: expected exactly {BENCHMARK_REPEATS} runs")

    validated: list[dict[str, Any]] = []
    for run_index, run_value in enumerate(runs):
        context = f"{raw_path}:{query['query_name']}:{run_index}"
        run = exact_object(run_value, RUN_FIELDS, context)
        run_kind = "cold" if run_index == 0 else "warm"
        if run["query"] != query["expression"]:
            raise GateError(f"{context}: expression differs from the sealed matrix")
        if run["run_index"] != run_index or run["run_kind"] != run_kind:
            raise GateError(f"{context}: run index/kind is invalid")
        if (
            run["effective_start_ms"] != query["start_ms"]
            or run["effective_end_ms"] != query["end_ms"]
            or run["step_ms"] != query["step_ms"]
        ):
            raise GateError(f"{context}: evaluation range differs from the sealed matrix")
        duration_ns = positive_int(run["duration_ns"], f"{context}.duration_ns")
        post_fingerprint_ns = nonnegative_int(
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
        payload = numeric_object(
            run["payload_reads"], PAYLOAD_FIELDS, f"{context}.payload_reads"
        )
        if payload["logical_used_bytes"] != stats["bytes_read"]:
            raise GateError(f"{context}: logical payload bytes differ from QueryStats")
        if payload["physical_bytes"] < payload["logical_used_bytes"]:
            raise GateError(f"{context}: physical payload bytes are below logical bytes")
        labels = phase2.validate_label_materialization(
            run["label_materialization"], query["category"], f"{context}.labels"
        )
        result_series = nonnegative_int(run["result_series"], f"{context}.result_series")
        result_samples = nonnegative_int(run["result_samples"], f"{context}.result_samples")
        result_bearing = query["category"] != "empty-result-control"
        storage = phase2.validate_label_storage(
            run["query_label_storage"],
            "compact-ids",
            args.arena_bytes,
            result_bearing,
            run_index == 0,
            f"{context}.query_label_storage",
        )
        scheduler = validate_scheduler(
            run["chunk_read_scheduler"],
            args.backend,
            args.queue_depth,
            payload,
            stats,
            f"{context}.chunk_read_scheduler",
        )
        if result_bearing:
            if not result_series or not result_samples:
                raise GateError(f"{context}: representative query returned an empty result")
        else:
            payload_stats = (
                "matched_series",
                "projected_series",
                "chunk_reads",
                "bytes_read",
                "samples_decoded",
                "typed_scalar_chunks_decoded",
                "typed_full_chunks_decoded",
            )
            if (
                result_series
                or result_samples
                or any(payload.values())
                or any(scheduler.values())
                or any(labels.values())
                or any(storage_activity_values(storage).values())
                or any(stats[field] for field in payload_stats)
            ):
                raise GateError(f"{context}: no-result control has non-zero payload/result state")
        if (
            query["category"] in phase2.TYPED_FULL_CATEGORIES
            and not stats["typed_full_chunks_decoded"]
        ):
            raise GateError(f"{context}: typed native query decoded no full chunks")
        validated.append(
            {
                "run_index": run_index,
                "run_kind": run_kind,
                "duration_ns": duration_ns,
                "post_query_fingerprint_ns": post_fingerprint_ns,
                "semantic_fingerprint": digest(
                    run["semantic_fingerprint_sha256"],
                    f"{context}.semantic_fingerprint",
                ),
                "portable_fingerprint": digest(
                    run["portable_semantic_fingerprint_sha256"],
                    f"{context}.portable_fingerprint",
                ),
                "result_series": result_series,
                "result_samples": result_samples,
                "stats": stats,
                "payload": payload,
                "scheduler": scheduler,
                "labels": labels,
                "label_storage": storage,
                "symbols": symbols,
                "metadata": metadata,
                "range_cache": range_cache,
                "stages": stages,
            }
        )
    return fingerprint, validated


def read_tsv(path: Path, fields: set[str], context: str) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if not rows or any(set(row) != fields for row in rows):
        raise GateError(f"{context} TSV has an invalid shape")
    return rows


def load_inventory(path: Path, corpus: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    exact_object(document, INVENTORY_FIELDS, str(path))
    if document["schema"] != common.INVENTORY_SCHEMA:
        raise GateError(f"{path}: inventory schema differs")
    if document["corpus"] != os.path.realpath(corpus):
        raise GateError(f"{path}: inventory names a different corpus")
    files_value = document["files"]
    if not isinstance(files_value, list) or not files_value:
        raise GateError(f"{path}: inventory files must be a non-empty array")
    files: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for index, value in enumerate(files_value):
        context = f"{path}.files[{index}]"
        entry = exact_object(value, INVENTORY_FILE_FIELDS, context)
        relative = entry["path"]
        if (
            not isinstance(relative, str)
            or not relative
            or "\0" in relative
            or os.path.isabs(relative)
            or os.path.normpath(relative) != relative
            or relative in seen_paths
            or ".." in Path(relative).parts
        ):
            raise GateError(f"{context}.path is not a unique canonical relative path")
        seen_paths.add(relative)
        files.append(
            {
                "path": relative,
                "size_bytes": nonnegative_int(entry["size_bytes"], f"{context}.size_bytes"),
                "sha256": digest(entry["sha256"], f"{context}.sha256"),
            }
        )
    expected_order = sorted(files, key=lambda entry: os.fsencode(entry["path"]))
    if files != expected_order:
        raise GateError(f"{path}: inventory files are not canonically ordered")
    if positive_int(document["file_count"], f"{path}.file_count") != len(files):
        raise GateError(f"{path}: inventory file_count does not match files")
    total_bytes = sum(entry["size_bytes"] for entry in files)
    if positive_int(document["total_bytes"], f"{path}.total_bytes") != total_bytes:
        raise GateError(f"{path}: inventory total_bytes does not match files")
    canonical = json.dumps(files, separators=(",", ":"), sort_keys=True).encode()
    expected_digest = hashlib.sha256(canonical).hexdigest()
    if digest(document["corpus_sha256"], f"{path}.corpus_sha256") != expected_digest:
        raise GateError(f"{path}: inventory corpus_sha256 does not match files")
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
        if (
            int(row["block"]) != plan["block"]
            or row["chunk_read_backend"] != plan["chunk_read_backend"]
            or int(row["payload_coalesce_max_gap_bytes"])
            != plan["payload_coalesce_max_gap_bytes"]
        ):
            raise GateError(f"{label}: residency metadata differs from the run plan")
        key = (label, phase)
        if key in seen:
            raise GateError(f"duplicate residency row: {key!r}")
        seen.add(key)
        if positive_int(int(row["file_count"]), f"{label}.file_count") != inventory[
            "file_count"
        ]:
            raise GateError(f"{label}: residency file count differs from inventory")
        if nonnegative_int(
            int(row["corpus_file_bytes"]), f"{label}.corpus_file_bytes"
        ) != inventory["total_bytes"]:
            raise GateError(f"{label}: residency bytes differ from inventory")
        resident = nonnegative_int(int(row["resident_bytes"]), f"{label}.resident_bytes")
        if resident > inventory["total_bytes"]:
            raise GateError(f"{label}: resident bytes exceed corpus bytes")
        if phase == "after-evict" and resident > max_after_evict:
            raise GateError(f"{label}: {resident} bytes resident after eviction")
    expected = {
        (label, phase)
        for label in plan_by_label
        for phase in ("after-evict", "after-run")
    }
    if seen != expected:
        raise GateError("residency summary is incomplete")


def validate_smoke_value(
    value: Any, kind: str, context: str
) -> dict[str, Any]:
    fields = {
        "footer": FOOTER_SMOKE_FIELDS,
        "readback": READBACK_SMOKE_FIELDS,
        "io_uring_preflight": PREFLIGHT_SMOKE_FIELDS,
    }.get(kind)
    if fields is None:
        raise GateError(f"unknown smoke JSON kind: {kind}")
    exact_object(value, fields, f"{context} {kind} validation evidence")
    if value["schema"] != SMOKE_SCHEMA or value["kind"] != kind or value["gate"] != "pass":
        raise GateError(f"{context}: {kind} validation evidence is invalid")
    if kind == "footer":
        if value["requested"] is not True or value["effective"] is not True:
            raise GateError(f"{context}: footer validation was not requested and effective")
    elif kind == "readback":
        metrics = {
            field: nonnegative_int(value[field], f"{context}.{field}")
            for field in READBACK_SMOKE_FIELDS - {"schema", "kind", "gate"}
        }
        if metrics["expected"] == 0 or not (
            metrics["executed"] == metrics["expected"]
            and metrics["checked"] == metrics["expected"]
            and metrics["skipped"] == 0
            and metrics["isolation_skips"] == 0
            and metrics["mismatches"] == 0
        ):
            raise GateError(f"{context}: readback evidence has skips, omissions, or mismatches")
    else:
        digest(value["binary_sha256"], f"{context}.binary_sha256")
        digest(value["preflight_raw_sha256"], f"{context}.preflight_raw_sha256")
        digest(
            value["corpus_fingerprint_sha256"],
            f"{context}.corpus_fingerprint_sha256",
        )
        positive_int(value["queue_depth"], f"{context}.queue_depth")
        corpus = value["corpus"]
        if (
            value["raw_schema"] != RAW_SCHEMA
            or value["chunk_read_mode"] != "io_uring"
            or value["query_name"] != "no_result"
            or not isinstance(corpus, str)
            or not corpus.startswith("/")
            or os.path.realpath(corpus) != corpus
        ):
            raise GateError(f"{context}: io_uring preflight contract is invalid")
    return value


def validate_smoke_json(path: Path, kind: str) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return validate_smoke_value(value, kind, str(path))


def validate_gap_monotonic(
    observations: dict[int, dict[str, Any]], context: str
) -> None:
    if set(observations) != set(GAPS):
        raise GateError(f"{context}: missing fixed gap observations")
    prior_reads: int | None = None
    prior_bytes: int | None = None
    for gap in GAPS:
        payload = observations[gap]["payload"]
        reads = payload["physical_reads"]
        physical_bytes = payload["physical_bytes"]
        if prior_reads is not None and reads > prior_reads:
            raise GateError(
                f"{context}: physical spans increased from the preceding gap at {gap}"
            )
        if prior_bytes is not None and physical_bytes < prior_bytes:
            raise GateError(
                f"{context}: physical bytes decreased from the preceding gap at {gap}"
            )
        prior_reads = reads
        prior_bytes = physical_bytes


def median(values: list[int | float], context: str) -> float:
    if not values:
        raise GateError(f"{context}: no observations")
    return float(statistics.median(values))


def compare_results(args: argparse.Namespace) -> None:
    if args.backend not in BACKENDS:
        raise GateError("backend must be forced pread or io-uring")
    if args.arena_bytes != DEFAULT_ARENA_BYTES:
        raise GateError("Phase 3 CompactIds arena must be exactly 512 MiB")
    queries = read_manifest(args.manifest, args.source_manifest)
    query_by_name = {query["query_name"]: query for query in queries}
    plan = expected_plan(queries, args.backend)
    plan_by_label = {row["process_label"]: row for row in plan}
    runs_dir = canonical_directory(args.runs_dir, "runs directory")

    before = load_inventory(args.inventory_before, args.corpus)
    after = load_inventory(args.inventory_after, args.corpus)
    if before != after:
        raise GateError("corpus inventory changed during the experiment")
    validate_smoke_json(args.footer_validation, "footer")
    validate_smoke_json(args.readback_validation, "readback")
    preflight = validate_smoke_json(args.io_uring_preflight, "io_uring_preflight")
    validate_residency(
        args.residency, plan_by_label, before, args.max_resident_bytes_after_evict
    )

    binary_hash = file_sha256(args.binary)
    if preflight.get("binary_sha256") != binary_hash:
        raise GateError("io_uring preflight used a different binary")
    if preflight.get("raw_schema") != RAW_SCHEMA or preflight.get(
        "chunk_read_mode"
    ) != "io_uring":
        raise GateError("io_uring preflight evidence has the wrong raw/backend contract")
    if (
        preflight["corpus"] != os.path.realpath(args.corpus)
        or preflight["query_name"] != "no_result"
    ):
        raise GateError("io_uring preflight used a different corpus or control query")
    if (
        positive_int(preflight.get("queue_depth"), "preflight queue depth")
        != args.preflight_queue_depth
    ):
        raise GateError("io_uring preflight queue depth differs from the invocation")

    rows = read_tsv(args.index, INDEX_FIELDS, "raw index")
    if len(rows) != len(plan):
        raise GateError(f"expected {len(plan)} completed processes, found {len(rows)}")
    if [row["process_label"] for row in rows] != [row["process_label"] for row in plan]:
        raise GateError("raw-index row sequence differs from the Williams run plan")
    processes: dict[tuple[str, int, int], dict[str, Any]] = {}
    fingerprints: set[str] = set()
    seen_labels: set[str] = set()
    raw_paths: set[Path] = set()
    for row in rows:
        label = row["process_label"]
        expected = plan_by_label.get(label)
        if expected is None or label in seen_labels:
            raise GateError(f"unknown or duplicate process label: {label!r}")
        seen_labels.add(label)
        for field in (
            "query_name",
            "category",
            "mode",
            "chunk_read_backend",
        ):
            if row[field] != str(expected[field]):
                raise GateError(f"{label}: raw-index {field} differs from the plan")
        block = positive_int(int(row["block"]), f"{label}.block")
        order_index = positive_int(int(row["order_index"]), f"{label}.order_index")
        gap = nonnegative_int(
            int(row["payload_coalesce_max_gap_bytes"]), f"{label}.gap"
        )
        if (
            block != expected["block"]
            or order_index != expected["order_index"]
            or gap != expected["payload_coalesce_max_gap_bytes"]
        ):
            raise GateError(f"{label}: block/order/gap differs from the Williams plan")
        if row["binary_sha256"] != binary_hash:
            raise GateError(f"{label}: process did not use the preserved binary")
        if os.path.realpath(row["corpus"]) != os.path.realpath(args.corpus):
            raise GateError(f"{label}: process used a different corpus")
        expected_raw = runs_dir / label / "raw.json"
        if row["raw_output"] != os.fspath(expected_raw):
            raise GateError(f"{label}.raw_output must be exactly {expected_raw}")
        raw_path = canonical_regular_file(
            Path(row["raw_output"]), expected_raw, f"{label}.raw_output"
        )
        if raw_path in raw_paths:
            raise GateError(f"{label}: raw output path is reused by another process")
        raw_paths.add(raw_path)
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
        fingerprints.add(fingerprint)
        key = (row["query_name"], block, order_index)
        if key in processes:
            raise GateError(f"duplicate completed process: {key!r}")
        processes[key] = {
            "index": row,
            "gap": gap,
            "runs": runs,
            "max_rss_kib": max_rss,
            **process_times,
        }
    if seen_labels != set(plan_by_label):
        raise GateError("completed-process set differs from the Williams plan")
    if len(fingerprints) != 1:
        raise GateError("query-reported corpus fingerprint changed across processes")
    query_fingerprint = next(iter(fingerprints))
    if preflight["corpus_fingerprint_sha256"] != query_fingerprint:
        raise GateError("io_uring preflight used a different query corpus fingerprint")

    for process in processes.values():
        label = process["index"]["process_label"]
        baseline = repeated_accounting_signature(process["runs"][0])
        prior_high_waters = {
            field: process["runs"][0]["scheduler"][field]
            for field in SCHEDULER_SESSION_HIGH_WATER_FIELDS
        }
        for run in process["runs"][1:]:
            if repeated_accounting_signature(run) != baseline:
                raise GateError(
                    f"{label}: public QueryStats or payload/scheduler accounting changed cold-to-warm"
                )
            for field in SCHEDULER_SESSION_HIGH_WATER_FIELDS:
                high_water = run["scheduler"][field]
                if high_water < prior_high_waters[field]:
                    raise GateError(f"{label}: {field} decreased cold-to-warm")
                prior_high_waters[field] = high_water

    correctness_digests: dict[tuple[str, int], str] = {}
    canonical_points: dict[tuple[str, int, int], dict[str, Any]] = {}
    for query in queries:
        query_name = query["query_name"]
        result_shapes: set[tuple[str, str, int, int]] = set()
        for run_index in range(BENCHMARK_REPEATS):
            observations = [
                process["runs"][run_index]
                for key, process in processes.items()
                if key[0] == query_name
            ]
            baseline = non_payload_signature(observations[0])
            for observation in observations[1:]:
                if non_payload_signature(observation) != baseline:
                    raise GateError(
                        f"{query_name} run {run_index}: non-payload accounting differs across gaps"
                    )
            correctness_digests[(query_name, run_index)] = canonical_digest(baseline)
            by_gap: dict[int, list[dict[str, Any]]] = {gap: [] for gap in GAPS}
            for key, process in processes.items():
                if key[0] == query_name:
                    by_gap[process["gap"]].append(process["runs"][run_index])
            for gap, gap_runs in by_gap.items():
                if len(gap_runs) != BLOCKS:
                    raise GateError(
                        f"{query_name} gap {gap} run {run_index}: expected {BLOCKS} observations"
                    )
                physical = physical_signature(gap_runs[0])
                if any(physical_signature(run) != physical for run in gap_runs[1:]):
                    raise GateError(
                        f"{query_name} gap {gap} run {run_index}: physical/scheduler accounting is nondeterministic"
                    )
                canonical_points[(query_name, run_index, gap)] = gap_runs[0]
            validate_gap_monotonic(
                {
                    gap: canonical_points[(query_name, run_index, gap)]
                    for gap in GAPS
                },
                f"{query_name} run {run_index}",
            )
            for observation in observations:
                result_shapes.add(
                    (
                        observation["semantic_fingerprint"],
                        observation["portable_fingerprint"],
                        observation["result_series"],
                        observation["result_samples"],
                    )
                )
        if len(result_shapes) != 1:
            raise GateError(f"{query_name}: result fingerprint/shape changed cold-to-warm")

    backend_accounting = [
        {
            "query_name": query["query_name"],
            "run_index": run_index,
            "payload_coalesce_max_gap_bytes": gap,
            "nonphysical_correctness_sha256": correctness_digests[
                (query["query_name"], run_index)
            ],
        }
        for query in queries
        for run_index in range(BENCHMARK_REPEATS)
        for gap in GAPS
    ]

    summary_fields = [
        "process_label",
        "query_name",
        "category",
        "mode",
        "block",
        "order_index",
        "chunk_read_backend",
        "payload_coalesce_max_gap_bytes",
        "binary_sha256",
        "run_index",
        "run_kind",
        "duration_ns",
        "post_query_fingerprint_ns",
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
        "payload_logical_used_bytes",
        "payload_physical_reads",
        "payload_physical_bytes",
        "payload_read_used_amplification",
        *(f"scheduler_{field}" for field in sorted(SCHEDULER_FIELDS)),
        "label_materialization_json",
        "query_label_storage_json",
        "symbol_reads_json",
        "metadata_runtime_json",
        "range_scalar_cache_json",
    ]
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=summary_fields,
            delimiter="\t",
            lineterminator="\n",
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
                        "chunk_read_backend",
                        "payload_coalesce_max_gap_bytes",
                        "binary_sha256",
                    )
                }
                row.update(
                    {
                        "run_index": run["run_index"],
                        "run_kind": run["run_kind"],
                        "duration_ns": run["duration_ns"],
                        "post_query_fingerprint_ns": run["post_query_fingerprint_ns"],
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
                            ""
                            if not logical
                            else run["payload"]["physical_bytes"] / logical
                        ),
                        "label_materialization_json": canonical_json(run["labels"]),
                        "query_label_storage_json": canonical_json(
                            run["label_storage"]
                        ),
                        "symbol_reads_json": canonical_json(run["symbols"]),
                        "metadata_runtime_json": canonical_json(run["metadata"]),
                        "range_scalar_cache_json": (
                            ""
                            if run["range_cache"] is None
                            else canonical_json(run["range_cache"])
                        ),
                    }
                )
                row.update(
                    {f"stats_{field}": run["stats"][field] for field in common.QUERY_STATS_FIELDS}
                )
                row.update(
                    {
                        f"scheduler_{field}": run["scheduler"][field]
                        for field in SCHEDULER_FIELDS
                    }
                )
                writer.writerow(row)

    measurements: list[dict[str, Any]] = []
    for query in queries:
        query_name = query["query_name"]
        for gap in GAPS:
            matching = sorted(
                (
                    process
                    for key, process in processes.items()
                    if key[0] == query_name and process["gap"] == gap
                ),
                key=lambda process: (
                    int(process["index"]["block"]),
                    int(process["index"]["order_index"]),
                ),
            )
            cold = [process["runs"][0]["duration_ns"] for process in matching]
            warm = [
                run["duration_ns"]
                for process in matching
                for run in process["runs"]
                if run["run_kind"] == "warm"
            ]
            process_warm_medians = [
                median(
                    [
                        run["duration_ns"]
                        for run in process["runs"]
                        if run["run_kind"] == "warm"
                    ],
                    f"{query_name} gap {gap} process warm",
                )
                for process in matching
            ]
            rss = [process["max_rss_kib"] for process in matching]
            points = [
                {
                    "run_index": run_index,
                    "logical_used_bytes": canonical_points[
                        (query_name, run_index, gap)
                    ]["payload"]["logical_used_bytes"],
                    "physical_spans": canonical_points[
                        (query_name, run_index, gap)
                    ]["payload"]["physical_reads"],
                    "physical_bytes": canonical_points[
                        (query_name, run_index, gap)
                    ]["payload"]["physical_bytes"],
                    "scheduler": canonical_points[(query_name, run_index, gap)][
                        "scheduler"
                    ],
                }
                for run_index in range(BENCHMARK_REPEATS)
            ]
            measurements.append(
                {
                    "query_name": query_name,
                    "payload_coalesce_max_gap_bytes": gap,
                    "cold_duration_ns": cold,
                    "cold_median_ns": median(cold, f"{query_name} gap {gap} cold"),
                    "warm_duration_ns": warm,
                    "process_warm_median_ns": process_warm_medians,
                    "warm_median_ns": median(
                        process_warm_medians,
                        f"{query_name} gap {gap} process-clustered warm",
                    ),
                    "process_max_rss_kib": rss,
                    "process_max_rss_median_kib": median(
                        rss, f"{query_name} gap {gap} RSS"
                    ),
                    "accounting_by_run_index": points,
                }
            )

    result = {
        "schema": RESULT_SCHEMA,
        "correctness_gate": "pass",
        "monotonic_physical_plan_gate": "pass",
        "backend": args.backend,
        "queue_depth": args.queue_depth,
        "gaps": list(GAPS),
        "williams_square": [list(sequence) for sequence in WILLIAMS_SQUARE],
        "blocks": BLOCKS,
        "schedule_repetitions": 2,
        "processes_per_gap_per_query": BLOCKS,
        "benchmark_repeats": BENCHMARK_REPEATS,
        "query_label_storage": "compact-ids",
        "query_label_arena_max_bytes": args.arena_bytes,
        "max_resident_bytes_after_evict": args.max_resident_bytes_after_evict,
        "os_page_cache_eviction_gate": "pass",
        "warm_headline_observation_unit": "per-process median of two warm runs",
        "sealed_query_manifest_sha256": SEALED_QUERY_MANIFEST_SHA256,
        "binary_sha256": binary_hash,
        "corpus_inventory_sha256": before["corpus_sha256"],
        "query_corpus_fingerprint_sha256": query_fingerprint,
        "io_uring_preflight": preflight,
        "nonphysical_accounting_by_query_run_gap": backend_accounting,
        "exact_across_gaps": list(EXACT_ACROSS_GAPS),
        "allowed_across_gap_differences": list(ALLOWED_ACROSS_GAP_DIFFERENCES),
        "measurements": measurements,
    }
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def finite_json_number(value: Any, context: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise GateError(f"{context} must be a finite non-negative JSON number")
    return value


def numeric_sequence(
    value: Any, length: int, context: str, *, positive: bool = False
) -> list[int]:
    if not isinstance(value, list) or len(value) != length:
        raise GateError(f"{context} must contain exactly {length} observations")
    validator = positive_int if positive else nonnegative_int
    return [validator(item, f"{context}[{index}]") for index, item in enumerate(value)]


def validate_reported_median(
    value: Any, observations: list[int | float], context: str
) -> int | float:
    reported = finite_json_number(value, context)
    expected = statistics.median(observations)
    if reported != expected:
        raise GateError(f"{context} does not equal the median of its observations")
    return reported


def load_sealed_result_payload(
    path: Path, expected_backend: str
) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise GateError(f"{expected_backend} result path must be absolute")
    result_root = canonical_directory(path.parent.parent, f"{expected_backend} result root")
    expected_result = result_root / RESULT_RELATIVE_PATH
    canonical_regular_file(path, expected_result, f"{expected_backend} result")

    complete = result_root / RESULT_COMPLETE_RELATIVE_PATH
    if not complete.exists():
        raise GateError(f"{expected_backend} result root lacks COMPLETE")
    canonical_regular_file(complete, complete, f"{expected_backend} COMPLETE marker")

    checksum_manifest = result_root / RESULT_CHECKSUM_RELATIVE_PATH
    if not checksum_manifest.exists():
        raise GateError(f"{expected_backend} result root lacks result-artifacts.sha256")
    canonical_regular_file(
        checksum_manifest,
        checksum_manifest,
        f"{expected_backend} result checksum manifest",
    )
    checksums: dict[str, str] = {}
    for line_number, line in enumerate(
        checksum_manifest.read_text(encoding="utf-8").splitlines(), start=1
    ):
        match = SHA256_MANIFEST_LINE.fullmatch(line)
        if match is None:
            raise GateError(
                f"{checksum_manifest}:{line_number}: malformed SHA-256 manifest line"
            )
        expected_sha256, relative_text = match.groups()
        relative = Path(relative_text)
        if (
            relative.is_absolute()
            or relative_text != relative.as_posix()
            or any(part in ("", ".", "..") for part in relative.parts)
        ):
            raise GateError(
                f"{checksum_manifest}:{line_number}: checksum path is not canonical relative"
            )
        if relative_text in checksums:
            raise GateError(
                f"{checksum_manifest}:{line_number}: duplicate checksum path {relative_text!r}"
            )
        checksums[relative_text] = expected_sha256

    result_relative_text = RESULT_RELATIVE_PATH.as_posix()
    sealed_sha256 = checksums.get(result_relative_text)
    if sealed_sha256 is None:
        raise GateError(
            f"{checksum_manifest}: lacks checksum for {result_relative_text}"
        )
    payload = path.read_bytes()
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if actual_sha256 != sealed_sha256:
        raise GateError(f"{path}: digest differs from result-artifacts.sha256")
    document = json.loads(payload)
    return document, actual_sha256


def validate_measurements(
    value: Any, backend: str, queue_depth: int, context: str
) -> dict[tuple[str, int], dict[str, Any]]:
    if not isinstance(value, list):
        raise GateError(f"{context} must be a list")
    expected_coordinates = [
        (query_name, gap) for query_name in EXPECTED_QUERY_NAMES for gap in GAPS
    ]
    if len(value) != len(expected_coordinates):
        raise GateError(
            f"{context} must contain exactly {len(expected_coordinates)} coordinates"
        )

    by_coordinate: dict[tuple[str, int], dict[str, Any]] = {}
    for index, (measurement_value, expected_coordinate) in enumerate(
        zip(value, expected_coordinates, strict=True)
    ):
        measurement_context = f"{context}[{index}]"
        measurement = exact_object(
            measurement_value, MEASUREMENT_FIELDS, measurement_context
        )
        query_name = measurement["query_name"]
        gap = measurement["payload_coalesce_max_gap_bytes"]
        if (query_name, gap) != expected_coordinate:
            raise GateError(
                f"{measurement_context}: measurement coordinates are incomplete or reordered"
            )

        cold = numeric_sequence(
            measurement["cold_duration_ns"],
            BLOCKS,
            f"{measurement_context}.cold_duration_ns",
            positive=True,
        )
        warm = numeric_sequence(
            measurement["warm_duration_ns"],
            BLOCKS * (BENCHMARK_REPEATS - 1),
            f"{measurement_context}.warm_duration_ns",
            positive=True,
        )
        warm_medians_value = measurement["process_warm_median_ns"]
        if not isinstance(warm_medians_value, list) or len(warm_medians_value) != BLOCKS:
            raise GateError(
                f"{measurement_context}.process_warm_median_ns must contain exactly {BLOCKS} observations"
            )
        warm_medians = [
            finite_json_number(item, f"{measurement_context}.process_warm_median_ns[{position}]")
            for position, item in enumerate(warm_medians_value)
        ]
        warm_runs_per_process = BENCHMARK_REPEATS - 1
        for block_index, reported in enumerate(warm_medians):
            start = block_index * warm_runs_per_process
            expected = statistics.median(warm[start : start + warm_runs_per_process])
            if reported != expected:
                raise GateError(
                    f"{measurement_context}.process_warm_median_ns[{block_index}] "
                    "does not equal its process-cluster median"
                )
        rss = numeric_sequence(
            measurement["process_max_rss_kib"],
            BLOCKS,
            f"{measurement_context}.process_max_rss_kib",
            positive=True,
        )
        validate_reported_median(
            measurement["cold_median_ns"], cold, f"{measurement_context}.cold_median_ns"
        )
        validate_reported_median(
            measurement["warm_median_ns"],
            warm_medians,
            f"{measurement_context}.warm_median_ns",
        )
        validate_reported_median(
            measurement["process_max_rss_median_kib"],
            rss,
            f"{measurement_context}.process_max_rss_median_kib",
        )

        accounting_value = measurement["accounting_by_run_index"]
        if not isinstance(accounting_value, list) or len(accounting_value) != BENCHMARK_REPEATS:
            raise GateError(
                f"{measurement_context}.accounting_by_run_index must contain exactly "
                f"{BENCHMARK_REPEATS} runs"
            )
        accounting: list[dict[str, Any]] = []
        for run_index, point_value in enumerate(accounting_value):
            point_context = f"{measurement_context}.accounting_by_run_index[{run_index}]"
            point = exact_object(
                point_value, MEASUREMENT_ACCOUNTING_FIELDS, point_context
            )
            if point["run_index"] != run_index:
                raise GateError(f"{point_context}.run_index is invalid")
            logical_used_bytes = nonnegative_int(
                point["logical_used_bytes"], f"{point_context}.logical_used_bytes"
            )
            physical_spans = nonnegative_int(
                point["physical_spans"], f"{point_context}.physical_spans"
            )
            physical_bytes = nonnegative_int(
                point["physical_bytes"], f"{point_context}.physical_bytes"
            )
            if physical_bytes < logical_used_bytes:
                raise GateError(f"{point_context}: physical bytes are below logical used bytes")
            if gap == 0 and physical_bytes != logical_used_bytes:
                raise GateError(f"{point_context}: gap zero must not over-read payload bytes")
            if logical_used_bytes == 0 and (physical_spans != 0 or physical_bytes != 0):
                raise GateError(f"{point_context}: zero logical bytes performed physical work")
            scheduler_value = numeric_object(
                point["scheduler"], SCHEDULER_FIELDS, f"{point_context}.scheduler"
            )
            payload = {
                "logical_used_bytes": logical_used_bytes,
                "physical_reads": physical_spans,
                "physical_bytes": physical_bytes,
            }
            scheduler = validate_scheduler(
                scheduler_value,
                backend,
                queue_depth,
                payload,
                {"chunk_reads": scheduler_value["logical_requests"]},
                f"{point_context}.scheduler",
            )
            accounting.append(
                {
                    "run_index": run_index,
                    "logical_used_bytes": logical_used_bytes,
                    "physical_spans": physical_spans,
                    "physical_bytes": physical_bytes,
                    "scheduler": scheduler,
                }
            )

        baseline = accounting[0]
        prior_high_waters = {
            field: baseline["scheduler"][field]
            for field in SCHEDULER_SESSION_HIGH_WATER_FIELDS
        }
        for run_index, point in enumerate(accounting[1:], start=1):
            for field in ("logical_used_bytes", "physical_spans", "physical_bytes"):
                if point[field] != baseline[field]:
                    raise GateError(
                        f"{measurement_context}: {field} changed across run indexes"
                    )
            for field in SCHEDULER_COUNTER_FIELDS:
                if point["scheduler"][field] != baseline["scheduler"][field]:
                    raise GateError(
                        f"{measurement_context}: scheduler {field} changed across run indexes"
                    )
            for field in SCHEDULER_SESSION_HIGH_WATER_FIELDS:
                high_water = point["scheduler"][field]
                if high_water < prior_high_waters[field]:
                    raise GateError(
                        f"{measurement_context}: scheduler {field} decreased at run {run_index}"
                    )
                prior_high_waters[field] = high_water

        measurement["accounting_by_run_index"] = accounting
        by_coordinate[(query_name, gap)] = measurement

    for query_name in EXPECTED_QUERY_NAMES:
        for run_index in range(BENCHMARK_REPEATS):
            points = {
                gap: by_coordinate[(query_name, gap)]["accounting_by_run_index"][
                    run_index
                ]
                for gap in GAPS
            }
            baseline = points[GAPS[0]]
            for gap, point in points.items():
                if (
                    point["logical_used_bytes"] != baseline["logical_used_bytes"]
                    or point["scheduler"]["logical_requests"]
                    != baseline["scheduler"]["logical_requests"]
                ):
                    raise GateError(
                        f"{context}: {query_name} run {run_index} logical accounting "
                        f"changed at gap {gap}"
                    )
            validate_gap_monotonic(
                {
                    gap: {
                        "payload": {
                            "physical_reads": point["physical_spans"],
                            "physical_bytes": point["physical_bytes"],
                        }
                    }
                    for gap, point in points.items()
                },
                f"{context}: {query_name} run {run_index}",
            )
    return by_coordinate


def load_backend_result(
    path: Path, expected_backend: str
) -> tuple[dict[str, Any], str, dict[tuple[str, int], dict[str, Any]]]:
    document, result_sha256 = load_sealed_result_payload(path, expected_backend)
    exact_object(document, RESULT_FIELDS, f"{path} backend result")
    if (
        document.get("schema") != RESULT_SCHEMA
        or document.get("correctness_gate") != "pass"
        or document.get("monotonic_physical_plan_gate") != "pass"
        or document.get("backend") != expected_backend
    ):
        raise GateError(f"{path}: backend result gate or identity is invalid")
    for field in (
        "binary_sha256",
        "corpus_inventory_sha256",
        "query_corpus_fingerprint_sha256",
        "sealed_query_manifest_sha256",
    ):
        digest(document.get(field), f"{path}.{field}")
    if document["sealed_query_manifest_sha256"] != SEALED_QUERY_MANIFEST_SHA256:
        raise GateError(f"{path}: backend result used a different sealed query manifest")
    if document.get("gaps") != list(GAPS):
        raise GateError(f"{path}: backend result used a different gap matrix")
    if (
        document.get("williams_square") != [list(sequence) for sequence in WILLIAMS_SQUARE]
        or document.get("blocks") != BLOCKS
        or document.get("schedule_repetitions") != 2
        or document.get("processes_per_gap_per_query") != BLOCKS
        or document.get("benchmark_repeats") != BENCHMARK_REPEATS
    ):
        raise GateError(f"{path}: backend result used a different repeat schedule")
    if (
        document.get("query_label_storage") != "compact-ids"
        or document.get("query_label_arena_max_bytes") != DEFAULT_ARENA_BYTES
        or document.get("warm_headline_observation_unit")
        != "per-process median of two warm runs"
        or document.get("exact_across_gaps") != list(EXACT_ACROSS_GAPS)
        or document.get("allowed_across_gap_differences")
        != list(ALLOWED_ACROSS_GAP_DIFFERENCES)
    ):
        raise GateError(f"{path}: backend result used a different query-label policy")
    queue_depth = positive_int(document.get("queue_depth"), f"{path}.queue_depth")
    nonnegative_int(
        document.get("max_resident_bytes_after_evict"),
        f"{path}.max_resident_bytes_after_evict",
    )
    if document.get("os_page_cache_eviction_gate") != "pass":
        raise GateError(f"{path}: backend result lacks OS page-cache eviction evidence")
    preflight = validate_smoke_value(
        document.get("io_uring_preflight"),
        "io_uring_preflight",
        f"{path}.io_uring_preflight",
    )
    if (
        preflight["binary_sha256"] != document["binary_sha256"]
        or preflight["corpus_fingerprint_sha256"]
        != document["query_corpus_fingerprint_sha256"]
    ):
        raise GateError(f"{path}: embedded io_uring preflight identity is invalid")
    accounting_value = document.get("nonphysical_accounting_by_query_run_gap")
    if not isinstance(accounting_value, list):
        raise GateError(f"{path}: backend result lacks nonphysical accounting")
    accounting: list[dict[str, Any]] = []
    for index, value in enumerate(accounting_value):
        context = f"{path}.nonphysical_accounting[{index}]"
        entry = exact_object(value, BACKEND_ACCOUNTING_FIELDS, context)
        query_name = entry["query_name"]
        if not isinstance(query_name, str) or query_name not in EXPECTED_QUERY_NAMES:
            raise GateError(f"{context}.query_name is invalid")
        accounting.append(
            {
                "query_name": query_name,
                "run_index": nonnegative_int(entry["run_index"], f"{context}.run_index"),
                "payload_coalesce_max_gap_bytes": nonnegative_int(
                    entry["payload_coalesce_max_gap_bytes"], f"{context}.gap"
                ),
                "nonphysical_correctness_sha256": digest(
                    entry["nonphysical_correctness_sha256"],
                    f"{context}.nonphysical_correctness_sha256",
                ),
            }
        )
    expected_coordinates = [
        (query_name, run_index, gap)
        for query_name in EXPECTED_QUERY_NAMES
        for run_index in range(BENCHMARK_REPEATS)
        for gap in GAPS
    ]
    actual_coordinates = [
        (
            entry["query_name"],
            entry["run_index"],
            entry["payload_coalesce_max_gap_bytes"],
        )
        for entry in accounting
    ]
    if actual_coordinates != expected_coordinates:
        raise GateError(f"{path}: nonphysical accounting coordinates are incomplete or reordered")
    document["nonphysical_accounting_by_query_run_gap"] = accounting
    measurements = validate_measurements(
        document.get("measurements"), expected_backend, queue_depth, f"{path}.measurements"
    )
    return document, result_sha256, measurements


def paired_number(pread: int | float, io_uring: int | float) -> dict[str, Any]:
    return {
        "pread": pread,
        "io_uring": io_uring,
        "io_uring_minus_pread": io_uring - pread,
        "io_uring_vs_pread_percent": 100.0 * (io_uring / pread - 1.0),
    }


def pair_measurements(
    pread: dict[tuple[str, int], dict[str, Any]],
    io_uring: dict[tuple[str, int], dict[str, Any]],
) -> list[dict[str, Any]]:
    paired: list[dict[str, Any]] = []
    for query_name in EXPECTED_QUERY_NAMES:
        for gap in GAPS:
            pread_measurement = pread[(query_name, gap)]
            io_measurement = io_uring[(query_name, gap)]
            paired_blocks = [
                {
                    "block": block + 1,
                    "cold_duration_ns": paired_number(
                        pread_measurement["cold_duration_ns"][block],
                        io_measurement["cold_duration_ns"][block],
                    ),
                    "process_warm_median_ns": paired_number(
                        pread_measurement["process_warm_median_ns"][block],
                        io_measurement["process_warm_median_ns"][block],
                    ),
                    "process_max_rss_kib": paired_number(
                        pread_measurement["process_max_rss_kib"][block],
                        io_measurement["process_max_rss_kib"][block],
                    ),
                }
                for block in range(BLOCKS)
            ]
            accounting: list[dict[str, Any]] = []
            for run_index in range(BENCHMARK_REPEATS):
                pread_point = pread_measurement["accounting_by_run_index"][run_index]
                io_point = io_measurement["accounting_by_run_index"][run_index]
                if (
                    pread_point["logical_used_bytes"] != io_point["logical_used_bytes"]
                    or pread_point["scheduler"]["logical_requests"]
                    != io_point["scheduler"]["logical_requests"]
                ):
                    raise GateError(
                        f"{query_name} gap {gap} run {run_index}: logical accounting differs across backends"
                    )
                if (
                    pread_point["physical_spans"] != io_point["physical_spans"]
                    or pread_point["physical_bytes"] != io_point["physical_bytes"]
                ):
                    raise GateError(
                        f"{query_name} gap {gap} run {run_index}: payload planning differs across backends"
                    )
                accounting.append(
                    {
                        "run_index": run_index,
                        "pread": pread_point,
                        "io_uring": io_point,
                        "io_uring_minus_pread": {
                            "logical_used_bytes": io_point["logical_used_bytes"]
                            - pread_point["logical_used_bytes"],
                            "physical_spans": io_point["physical_spans"]
                            - pread_point["physical_spans"],
                            "physical_bytes": io_point["physical_bytes"]
                            - pread_point["physical_bytes"],
                            "scheduler": {
                                field: io_point["scheduler"][field]
                                - pread_point["scheduler"][field]
                                for field in sorted(SCHEDULER_FIELDS)
                            },
                        },
                    }
                )
            paired.append(
                {
                    "query_name": query_name,
                    "payload_coalesce_max_gap_bytes": gap,
                    "headline_medians": {
                        "cold_duration_ns": paired_number(
                            pread_measurement["cold_median_ns"],
                            io_measurement["cold_median_ns"],
                        ),
                        "process_warm_median_ns": paired_number(
                            pread_measurement["warm_median_ns"],
                            io_measurement["warm_median_ns"],
                        ),
                        "process_max_rss_kib": paired_number(
                            pread_measurement["process_max_rss_median_kib"],
                            io_measurement["process_max_rss_median_kib"],
                        ),
                    },
                    "paired_blocks": paired_blocks,
                    "accounting_by_run_index": accounting,
                }
            )
    return paired


def compare_backends(args: argparse.Namespace) -> None:
    pread, pread_sha256, pread_measurements = load_backend_result(
        args.pread_result, "pread"
    )
    io_uring, io_uring_sha256, io_uring_measurements = load_backend_result(
        args.io_uring_result, "io-uring"
    )
    exact_fields = (
        "binary_sha256",
        "corpus_inventory_sha256",
        "query_corpus_fingerprint_sha256",
        "sealed_query_manifest_sha256",
        "gaps",
        "williams_square",
        "blocks",
        "schedule_repetitions",
        "processes_per_gap_per_query",
        "benchmark_repeats",
        "query_label_storage",
        "query_label_arena_max_bytes",
        "max_resident_bytes_after_evict",
        "os_page_cache_eviction_gate",
        "warm_headline_observation_unit",
        "nonphysical_accounting_by_query_run_gap",
    )
    for field in exact_fields:
        if pread.get(field) != io_uring.get(field):
            raise GateError(f"backend results differ in required exact field {field}")
    preflight_exact_fields = PREFLIGHT_SMOKE_FIELDS - {"preflight_raw_sha256"}
    for field in preflight_exact_fields:
        if pread["io_uring_preflight"][field] != io_uring["io_uring_preflight"][field]:
            raise GateError(f"backend embedded preflights differ in required field {field}")
    paired_measurements = pair_measurements(pread_measurements, io_uring_measurements)
    result = {
        "schema": BACKEND_COMPARISON_SCHEMA,
        "correctness_gate": "pass",
        "pread_result_sha256": pread_sha256,
        "io_uring_result_sha256": io_uring_sha256,
        "binary_sha256": pread["binary_sha256"],
        "corpus_inventory_sha256": pread["corpus_inventory_sha256"],
        "query_corpus_fingerprint_sha256": pread["query_corpus_fingerprint_sha256"],
        "sealed_query_manifest_sha256": pread["sealed_query_manifest_sha256"],
        "gaps": pread["gaps"],
        "exact_across_backends": [
            "same preserved binary and fingerprinted corpus",
            "same sealed query and gap matrices",
            "semantic and portable fingerprints and result shapes",
            "all QueryStats and logical payload accounting",
            "physical payload span and byte plans",
            "label, symbol, metadata, and range-cache non-timing accounting",
        ],
        "allowed_across_backend_differences": [
            "scheduler backend, submission, and high-water accounting",
            "latency, process CPU, and maximum RSS",
        ],
        "paired_measurements": paired_measurements,
    }
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    normalize = commands.add_parser("normalize-manifest")
    normalize.add_argument("--input", type=Path, required=True)
    normalize.add_argument("--output-tsv", type=Path, required=True)
    normalize.add_argument("--output-json", type=Path, required=True)

    plan = commands.add_parser("write-plan")
    plan.add_argument("--manifest", type=Path, required=True)
    plan.add_argument("--source-manifest", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--backend", choices=BACKENDS, required=True)

    inventory = commands.add_parser("inventory")
    inventory.add_argument("--corpus", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--paths-output", type=Path, required=True)

    smoke = commands.add_parser("validate-smoke-report")
    smoke.add_argument("--kind", choices=("footer", "readback"), required=True)
    smoke.add_argument("--report", type=Path, required=True)
    smoke.add_argument("--output", type=Path, required=True)

    preflight = commands.add_parser("validate-io-uring-preflight")
    preflight.add_argument("--raw", type=Path, required=True)
    preflight.add_argument("--binary", type=Path, required=True)
    preflight.add_argument("--corpus", type=Path, required=True)
    preflight.add_argument("--manifest", type=Path, required=True)
    preflight.add_argument("--source-manifest", type=Path, required=True)
    preflight.add_argument("--query-name", required=True)
    preflight.add_argument("--expected-queue-depth", type=int, required=True)
    preflight.add_argument("--arena-bytes", type=int, required=True)
    preflight.add_argument("--max-matched-series", type=int, required=True)
    preflight.add_argument("--max-projected-series", type=int, required=True)
    preflight.add_argument("--max-chunk-reads", type=int, required=True)
    preflight.add_argument("--max-bytes-read", type=int, required=True)
    preflight.add_argument("--max-samples-decoded", type=int, required=True)
    preflight.add_argument("--max-regex-values-examined", type=int, required=True)
    preflight.add_argument("--output", type=Path, required=True)

    compare = commands.add_parser("compare-results")
    compare.add_argument("--index", type=Path, required=True)
    compare.add_argument("--manifest", type=Path, required=True)
    compare.add_argument("--source-manifest", type=Path, required=True)
    compare.add_argument("--inventory-before", type=Path, required=True)
    compare.add_argument("--inventory-after", type=Path, required=True)
    compare.add_argument("--residency", type=Path, required=True)
    compare.add_argument("--footer-validation", type=Path, required=True)
    compare.add_argument("--readback-validation", type=Path, required=True)
    compare.add_argument("--io-uring-preflight", type=Path, required=True)
    compare.add_argument("--summary", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--binary", type=Path, required=True)
    compare.add_argument("--corpus", type=Path, required=True)
    compare.add_argument("--runs-dir", type=Path, required=True)
    compare.add_argument("--backend", choices=BACKENDS, required=True)
    compare.add_argument("--queue-depth", type=int, required=True)
    compare.add_argument("--preflight-queue-depth", type=int, required=True)
    compare.add_argument("--arena-bytes", type=int, required=True)
    compare.add_argument("--max-resident-bytes-after-evict", type=int, required=True)
    compare.add_argument("--max-matched-series", type=int, required=True)
    compare.add_argument("--max-projected-series", type=int, required=True)
    compare.add_argument("--max-chunk-reads", type=int, required=True)
    compare.add_argument("--max-bytes-read", type=int, required=True)
    compare.add_argument("--max-samples-decoded", type=int, required=True)
    compare.add_argument("--max-regex-values-examined", type=int, required=True)

    backend_compare = commands.add_parser("compare-backends")
    backend_compare.add_argument("--pread-result", type=Path, required=True)
    backend_compare.add_argument("--io-uring-result", type=Path, required=True)
    backend_compare.add_argument("--output", type=Path, required=True)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "normalize-manifest":
            normalize_manifest(args.input, args.output_tsv, args.output_json)
        elif args.command == "write-plan":
            write_plan(args.manifest, args.source_manifest, args.output, args.backend)
        elif args.command == "inventory":
            common.write_inventory(args.corpus, args.output, args.paths_output)
        elif args.command == "validate-smoke-report":
            validate_smoke_report(args.kind, args.report, args.output)
        elif args.command == "validate-io-uring-preflight":
            positive_int(args.expected_queue_depth, "preflight queue depth")
            if args.arena_bytes != DEFAULT_ARENA_BYTES:
                raise GateError("io_uring preflight CompactIds arena must be exactly 512 MiB")
            limits = limit_configuration(args)
            for field, value in limits.items():
                nonnegative_int(value, f"preflight limits.{field}")
            queries = read_manifest(args.manifest, args.source_manifest)
            matching = [query for query in queries if query["query_name"] == args.query_name]
            if len(matching) != 1:
                raise GateError("io_uring preflight query name is absent or duplicated")
            validate_io_uring_preflight(
                args.raw,
                args.binary,
                args.corpus,
                matching[0],
                args.expected_queue_depth,
                args.arena_bytes,
                limits,
                args.output,
            )
        elif args.command == "compare-results":
            positive_int(args.queue_depth, "queue depth")
            positive_int(args.preflight_queue_depth, "preflight queue depth")
            nonnegative_int(args.max_resident_bytes_after_evict, "max resident bytes")
            compare_results(args)
        elif args.command == "compare-backends":
            compare_backends(args)
        else:
            raise AssertionError(args.command)
    except (
        GateError,
        common.GateError,
        manifest_gate.GateError,
        phase1.GateError,
        phase2.GateError,
        OSError,
        TypeError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
    ) as error:
        print(f"Phase 3 payload-coalescing gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
