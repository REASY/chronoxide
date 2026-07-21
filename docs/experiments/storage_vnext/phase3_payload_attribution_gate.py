#!/usr/bin/env python3
"""Strict gate for the observer-heavy Phase 3 payload attribution sweep."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import phase1_query_gate as phase1
import phase2_compact_ids_ab_gate as phase2
import phase3_payload_coalescing_gate as phase3
import schema7_query_ab_gate as common


SCHEMA = "chronoxide/storage-vnext-phase3-payload-attribution/v2"
NORMALIZED_SCHEMA = (
    "chronoxide/storage-vnext-phase3-payload-attribution-manifest/v1"
)
RAW_SCHEMA = phase3.RAW_SCHEMA
SEALED_QUERY_MANIFEST_SHA256 = phase3.SEALED_QUERY_MANIFEST_SHA256
QUERY_NAMES = (
    "broad_raw_count_selector",
    "scalar_rate_sum_instant",
    "scalar_rate_sum_range",
    "native_hist_count_range",
)
BACKENDS = ("pread", "io-uring")
GAPS = (0, 1024, 4096)
BENCHMARK_REPEATS = 2
QUEUE_DEPTHS = {"pread": 128, "io-uring": 8}
ARENA_BYTES = 512 * 1024 * 1024
REQUIRED_STAGE_LEAVES = (
    "payload_read_pipeline_combined_ns",
    "payload_decode_projection_result_processing_combined_ns",
)
INDEX_FIELDS = {
    "process_label",
    "query_name",
    "category",
    "mode",
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
    "chunk_read_backend",
    "payload_coalesce_max_gap_bytes",
    "phase",
    "file_count",
    "resident_bytes",
    "corpus_file_bytes",
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


def finite_nonnegative(value: Any, name: str) -> float:
    try:
        converted = float(value)
    except (TypeError, ValueError) as error:
        raise GateError(f"{name} must be numeric") from error
    if not (converted >= 0.0 and converted < float("inf")):
        raise GateError(f"{name} must be finite and non-negative")
    return converted


def exact_object(
    value: Any, fields: set[str] | frozenset[str], context: str
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(fields):
        raise GateError(f"{context} has an invalid shape")
    return value


def digest(value: Any, name: str) -> str:
    try:
        return phase3.digest(value, name)
    except phase3.GateError as error:
        raise GateError(str(error)) from error


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            hasher.update(block)
    return hasher.hexdigest()


def selected_source_queries(path: Path) -> list[dict[str, Any]]:
    try:
        all_queries = phase3.load_source_manifest(path)
    except phase3.GateError as error:
        raise GateError(str(error)) from error
    by_name = {query["query_name"]: query for query in all_queries}
    if any(name not in by_name for name in QUERY_NAMES):
        raise GateError("sealed manifest lacks an attribution query")
    queries = [by_name[name] for name in QUERY_NAMES]
    if tuple(query["query_name"] for query in queries) != QUERY_NAMES:
        raise GateError("attribution query order differs from the fixed contract")
    for query in queries:
        if query["mode"] == "range" and query["range_scalar_cache_max_bytes"] != 0:
            raise GateError(f"{query['query_name']}: range scalar cache must be disabled")
    return queries


def write_manifest(
    input_path: Path, output_tsv: Path, output_json: Path
) -> None:
    queries = selected_source_queries(input_path)
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
                    "query_name": query["query_name"],
                    "category": query["category"],
                    "mode": query["mode"],
                    "start_ms": query["start_ms"],
                    "end_ms": query["end_ms"],
                    "step_ms": "-" if query["step_ms"] is None else query["step_ms"],
                    "range_scalar_cache_max_bytes": (
                        "-"
                        if query["range_scalar_cache_max_bytes"] is None
                        else query["range_scalar_cache_max_bytes"]
                    ),
                    "boundaries_csv": (
                        "-"
                        if not query["boundaries"]
                        else ",".join(str(value) for value in query["boundaries"])
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


def read_manifest(path: Path, source_manifest: Path) -> list[dict[str, Any]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    exact_object(
        document,
        {"schema", "source_manifest_sha256", "queries"},
        str(path),
    )
    if (
        document["schema"] != NORMALIZED_SCHEMA
        or document["source_manifest_sha256"] != SEALED_QUERY_MANIFEST_SHA256
    ):
        raise GateError("normalized attribution manifest identity differs")
    expected = selected_source_queries(source_manifest)
    if document["queries"] != expected:
        raise GateError("normalized attribution queries differ from the sealed source")
    return expected


def expected_plan(queries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for query in queries:
        order_index = 0
        for backend in BACKENDS:
            for gap in GAPS:
                order_index += 1
                backend_label = backend.replace("-", "_")
                rows.append(
                    {
                        "process_label": (
                            f"{query['query_name']}-{backend_label}-gap{gap:04d}"
                        ),
                        "query_name": query["query_name"],
                        "category": query["category"],
                        "mode": query["mode"],
                        "order_index": order_index,
                        "chunk_read_backend": backend,
                        "payload_coalesce_max_gap_bytes": gap,
                    }
                )
    return rows


def write_plan(manifest: Path, source_manifest: Path, output: Path) -> None:
    rows = expected_plan(read_manifest(manifest, source_manifest))
    with output.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=tuple(rows[0]),
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def validate_detailed_stages(
    value: Any, duration_ns: int, context: str
) -> dict[str, int]:
    try:
        stages = common.validate_query_stages(value, "detailed", duration_ns, context)
    except common.GateError as error:
        raise GateError(str(error)) from error
    if stages["exclusive_total_ns"] == 0:
        raise GateError(f"{context}: Detailed stage attribution is all zero")
    for field in REQUIRED_STAGE_LEAVES:
        if stages[field] == 0:
            raise GateError(f"{context}: required Detailed stage leaf {field} is zero")
        if stages[field] > duration_ns:
            raise GateError(f"{context}: Detailed stage leaf {field} exceeds query wall")
    return stages


def expected_limits(args: argparse.Namespace) -> dict[str, int]:
    return {
        "max_matched_series": args.max_matched_series,
        "max_projected_series": args.max_projected_series,
        "max_chunk_reads": args.max_chunk_reads,
        "max_bytes_read": args.max_bytes_read,
        "max_samples_decoded": args.max_samples_decoded,
        "max_regex_values_examined": args.max_regex_values_examined,
    }


def validate_raw(
    row: dict[str, str], query: dict[str, Any], args: argparse.Namespace
) -> tuple[str, list[dict[str, Any]]]:
    raw_path = Path(row["raw_output"])
    document = json.loads(raw_path.read_text(encoding="utf-8"))
    exact_object(document, phase3.DOCUMENT_FIELDS, str(raw_path))
    if document["schema"] != RAW_SCHEMA:
        raise GateError(f"{raw_path}: raw schema must be {RAW_SCHEMA}")
    nonnegative_int(
        document["corpus_fingerprint_duration_ns"],
        f"{raw_path}.corpus_fingerprint_duration_ns",
    )
    fingerprint = digest(
        document["corpus_fingerprint_sha256"], f"{raw_path}.corpus_fingerprint"
    )
    backend = row["chunk_read_backend"]
    gap = int(row["payload_coalesce_max_gap_bytes"])
    configuration = exact_object(
        document["configuration"], phase3.CONFIGURATION_FIELDS, f"{raw_path}.configuration"
    )
    expected_configuration = {
        "segments_dir": os.path.realpath(args.corpus),
        "start_ms": query["start_ms"],
        "end_ms": query["end_ms"],
        "mode": query["mode"] if query["mode"] == "instant" else "query_range",
        "step_ms": query["step_ms"],
        "range_scalar_cache_max_bytes": query["range_scalar_cache_max_bytes"],
        "chunk_read_mode": phase3.raw_backend_name(backend),
        "chunk_read_queue_depth": QUEUE_DEPTHS[backend],
        "chunk_payload_coalesce_max_gap_bytes": gap,
        "experimental_cross_segment_chunk_reads": False,
        "label_materialization": "demand-driven",
        "query_label_storage": "compact-ids",
        "query_instrumentation": "detailed",
        "query_label_arena_max_bytes": ARENA_BYTES,
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
        raise GateError(f"{raw_path}: configuration differs from the attribution contract")
    if document["limits"] != expected_limits(args):
        raise GateError(f"{raw_path}: query limits differ from the attribution contract")
    runs_value = document["runs"]
    if not isinstance(runs_value, list) or len(runs_value) != BENCHMARK_REPEATS:
        raise GateError(f"{raw_path}: expected one cold and one warm run")

    validated: list[dict[str, Any]] = []
    for run_index, run_value in enumerate(runs_value):
        context = f"{raw_path}:{query['query_name']}:{run_index}"
        run = exact_object(run_value, phase3.RUN_FIELDS, context)
        run_kind = "cold" if run_index == 0 else "warm"
        if run["run_index"] != run_index or run["run_kind"] != run_kind:
            raise GateError(f"{context}: run index/kind differs")
        if run["query"] != query["expression"]:
            raise GateError(f"{context}: query differs from the sealed manifest")
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
            symbols = phase1.validate_symbol_reads(
                run["symbol_reads"], f"{context}.symbol_reads"
            )
            metadata = phase1.validate_metadata_runtime(
                run["metadata_runtime"], f"{context}.metadata_runtime"
            )
            range_cache = phase1.validate_range_cache(
                run["range_scalar_cache"], query, context
            )
            labels = phase2.validate_label_materialization(
                run["label_materialization"], query["category"], f"{context}.labels"
            )
            label_storage = phase2.validate_label_storage(
                run["query_label_storage"],
                "compact-ids",
                ARENA_BYTES,
                True,
                run_index == 0,
                f"{context}.query_label_storage",
            )
        except (phase1.GateError, phase2.GateError, common.GateError) as error:
            raise GateError(str(error)) from error
        stages = validate_detailed_stages(run["query_stages"], duration_ns, context)
        payload = phase3.numeric_object(
            run["payload_reads"], phase3.PAYLOAD_FIELDS, f"{context}.payload_reads"
        )
        if payload["logical_used_bytes"] != stats["bytes_read"]:
            raise GateError(f"{context}: logical payload bytes differ from QueryStats")
        if payload["physical_bytes"] < payload["logical_used_bytes"]:
            raise GateError(f"{context}: physical payload bytes are below logical bytes")
        scheduler = phase3.validate_scheduler(
            run["chunk_read_scheduler"],
            backend,
            QUEUE_DEPTHS[backend],
            payload,
            stats,
            f"{context}.chunk_read_scheduler",
        )
        result_series = positive_int(run["result_series"], f"{context}.result_series")
        result_samples = positive_int(run["result_samples"], f"{context}.result_samples")
        if query["category"] in phase2.TYPED_FULL_CATEGORIES and not stats[
            "typed_full_chunks_decoded"
        ]:
            raise GateError(f"{context}: typed query decoded no full chunks")
        validated.append(
            {
                "run_index": run_index,
                "run_kind": run_kind,
                "duration_ns": duration_ns,
                "semantic_fingerprint": digest(
                    run["semantic_fingerprint_sha256"], f"{context}.semantic"
                ),
                "portable_fingerprint": digest(
                    run["portable_semantic_fingerprint_sha256"], f"{context}.portable"
                ),
                "result_series": result_series,
                "result_samples": result_samples,
                "stats": stats,
                "payload": payload,
                "scheduler": scheduler,
                "stages": stages,
                "symbols": symbols,
                "metadata": metadata,
                "range_cache": range_cache,
                "labels": labels,
                "label_storage": label_storage,
            }
        )
    return fingerprint, validated


def equivalence_signature(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "semantic_fingerprint": run["semantic_fingerprint"],
        "portable_fingerprint": run["portable_fingerprint"],
        "result_series": run["result_series"],
        "result_samples": run["result_samples"],
        "stats": run["stats"],
        "logical_used_bytes": run["payload"]["logical_used_bytes"],
        "logical_requests": run["scheduler"]["logical_requests"],
    }


def read_tsv(path: Path, fields: set[str], context: str) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if not rows or any(set(row) != fields for row in rows):
        raise GateError(f"{context} TSV has an invalid shape")
    return rows


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
            raise GateError("residency contains an unknown process or phase")
        if (
            row["chunk_read_backend"] != plan["chunk_read_backend"]
            or int(row["payload_coalesce_max_gap_bytes"])
            != plan["payload_coalesce_max_gap_bytes"]
        ):
            raise GateError(f"{label}: residency metadata differs from the plan")
        key = (label, phase)
        if key in seen:
            raise GateError(f"duplicate residency row: {key!r}")
        seen.add(key)
        if int(row["file_count"]) != inventory["file_count"]:
            raise GateError(f"{label}: residency file count differs from inventory")
        if int(row["corpus_file_bytes"]) != inventory["total_bytes"]:
            raise GateError(f"{label}: residency corpus bytes differ from inventory")
        resident = nonnegative_int(int(row["resident_bytes"]), f"{label}.resident_bytes")
        if resident > inventory["total_bytes"]:
            raise GateError(f"{label}: resident bytes exceed corpus bytes")
        if phase == "after-evict" and resident > max_after_evict:
            raise GateError(f"{label}: resident bytes exceed the eviction bound")
    expected = {
        (label, phase)
        for label in plan_by_label
        for phase in ("after-evict", "after-run")
    }
    if seen != expected:
        raise GateError("residency evidence is incomplete")


def compare_results(args: argparse.Namespace) -> None:
    queries = read_manifest(args.manifest, args.source_manifest)
    query_by_name = {query["query_name"]: query for query in queries}
    plan = expected_plan(queries)
    plan_by_label = {row["process_label"]: row for row in plan}
    try:
        runs_dir = phase3.canonical_directory(args.runs_dir, "runs directory")
        before = phase3.load_inventory(args.inventory_before, args.corpus)
        after = phase3.load_inventory(args.inventory_after, args.corpus)
    except phase3.GateError as error:
        raise GateError(str(error)) from error
    if before != after:
        raise GateError("corpus inventory changed during attribution")
    validate_residency(
        args.residency, plan_by_label, before, args.max_resident_bytes_after_evict
    )
    rows = read_tsv(args.index, INDEX_FIELDS, "raw index")
    if [row["process_label"] for row in rows] != [row["process_label"] for row in plan]:
        raise GateError("raw-index sequence differs from the fixed attribution plan")

    binary_hash = file_sha256(args.binary)
    fingerprints: set[str] = set()
    processes: dict[str, dict[str, Any]] = {}
    raw_paths: set[Path] = set()
    for row, expected in zip(rows, plan, strict=True):
        label = row["process_label"]
        for field in (
            "query_name",
            "category",
            "mode",
            "chunk_read_backend",
            "payload_coalesce_max_gap_bytes",
            "order_index",
        ):
            if str(row[field]) != str(expected[field]):
                raise GateError(f"{label}: raw index differs from the fixed plan")
        if row["binary_sha256"] != binary_hash:
            raise GateError(f"{label}: process used a different binary")
        if os.path.realpath(row["corpus"]) != os.path.realpath(args.corpus):
            raise GateError(f"{label}: process used a different corpus")
        expected_raw = runs_dir / label / "raw.json"
        try:
            raw_path = phase3.canonical_regular_file(
                Path(row["raw_output"]), expected_raw, f"{label}.raw_output"
            )
        except phase3.GateError as error:
            raise GateError(str(error)) from error
        if raw_path in raw_paths:
            raise GateError(f"{label}: raw output path is reused")
        raw_paths.add(raw_path)
        process_times = {
            field: finite_nonnegative(row[field], f"{label}.{field}")
            for field in (
                "process_wall_seconds",
                "process_user_seconds",
                "process_system_seconds",
            )
        }
        max_rss_kib = positive_int(int(row["max_rss_kib"]), f"{label}.max_rss_kib")
        fingerprint, runs = validate_raw(row, query_by_name[row["query_name"]], args)
        fingerprints.add(fingerprint)
        processes[label] = {
            "index": row,
            "runs": runs,
            "max_rss_kib": max_rss_kib,
            **process_times,
        }
    if len(processes) != len(plan) or len(fingerprints) != 1:
        raise GateError("process set or query corpus fingerprint is inconsistent")

    equivalence: dict[tuple[str, int], str] = {}
    for query in queries:
        query_name = query["query_name"]
        for run_index in range(BENCHMARK_REPEATS):
            matching = [
                process["runs"][run_index]
                for process in processes.values()
                if process["index"]["query_name"] == query_name
            ]
            baseline = equivalence_signature(matching[0])
            if any(equivalence_signature(run) != baseline for run in matching[1:]):
                raise GateError(
                    f"{query_name} run {run_index}: semantic or logical accounting differs across gaps/backends"
                )
            equivalence[(query_name, run_index)] = hashlib.sha256(
                json.dumps(baseline, separators=(",", ":"), sort_keys=True).encode()
            ).hexdigest()
        for backend in BACKENDS:
            for run_index in range(BENCHMARK_REPEATS):
                prior_reads: int | None = None
                prior_bytes: int | None = None
                for gap in GAPS:
                    label = next(
                        row["process_label"]
                        for row in plan
                        if row["query_name"] == query_name
                        and row["chunk_read_backend"] == backend
                        and row["payload_coalesce_max_gap_bytes"] == gap
                    )
                    payload = processes[label]["runs"][run_index]["payload"]
                    reads = payload["physical_reads"]
                    physical_bytes = payload["physical_bytes"]
                    if prior_reads is not None and reads > prior_reads:
                        raise GateError(f"{query_name}/{backend}: physical reads increased")
                    if prior_bytes is not None and physical_bytes < prior_bytes:
                        raise GateError(f"{query_name}/{backend}: physical bytes decreased")
                    prior_reads, prior_bytes = reads, physical_bytes

    summary_fields = (
        "process_label",
        "query_name",
        "run_index",
        "run_kind",
        "chunk_read_backend",
        "payload_coalesce_max_gap_bytes",
        "timing_comparability",
        "diagnostic_duration_ns",
        "payload_read_pipeline_combined_ns",
        "payload_decode_projection_result_processing_combined_ns",
        "exclusive_total_ns",
        "unclassified_ns",
        "payload_logical_used_bytes",
        "payload_physical_reads",
        "payload_physical_bytes",
        "scheduler_logical_requests",
        "max_rss_kib",
        "equivalence_sha256",
    )
    observations: list[dict[str, Any]] = []
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=summary_fields,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        for plan_row in plan:
            process = processes[plan_row["process_label"]]
            for run in process["runs"]:
                observation = {
                    "process_label": plan_row["process_label"],
                    "query_name": plan_row["query_name"],
                    "run_index": run["run_index"],
                    "run_kind": run["run_kind"],
                    "chunk_read_backend": plan_row["chunk_read_backend"],
                    "payload_coalesce_max_gap_bytes": plan_row[
                        "payload_coalesce_max_gap_bytes"
                    ],
                    "timing_comparability": "diagnostic-detailed-wall-non-comparable",
                    "diagnostic_duration_ns": run["duration_ns"],
                    "payload_read_pipeline_combined_ns": run["stages"][
                        "payload_read_pipeline_combined_ns"
                    ],
                    "payload_decode_projection_result_processing_combined_ns": run[
                        "stages"
                    ]["payload_decode_projection_result_processing_combined_ns"],
                    "exclusive_total_ns": run["stages"]["exclusive_total_ns"],
                    "unclassified_ns": run["stages"]["unclassified_ns"],
                    "payload_logical_used_bytes": run["payload"]["logical_used_bytes"],
                    "payload_physical_reads": run["payload"]["physical_reads"],
                    "payload_physical_bytes": run["payload"]["physical_bytes"],
                    "scheduler_logical_requests": run["scheduler"]["logical_requests"],
                    "max_rss_kib": process["max_rss_kib"],
                    "equivalence_sha256": equivalence[
                        (plan_row["query_name"], run["run_index"])
                    ],
                }
                writer.writerow(observation)
                observations.append(observation)

    aggregate: list[dict[str, Any]] = []
    for query_name in QUERY_NAMES:
        for backend in BACKENDS:
            for gap in GAPS:
                matching = [
                    row
                    for row in observations
                    if row["query_name"] == query_name
                    and row["chunk_read_backend"] == backend
                    and row["payload_coalesce_max_gap_bytes"] == gap
                ]
                by_run_kind = {row["run_kind"]: row for row in matching}
                if len(by_run_kind) != len(matching) or set(by_run_kind) != {
                    "cold",
                    "warm",
                }:
                    raise GateError(
                        f"{query_name}/{backend}/gap {gap}: expected one cold and one warm attribution observation"
                    )
                aggregate.append(
                    {
                        "query_name": query_name,
                        "chunk_read_backend": backend,
                        "payload_coalesce_max_gap_bytes": gap,
                        "by_run_kind": {
                            run_kind: {
                                "payload_read_pipeline_combined_ns": by_run_kind[
                                    run_kind
                                ]["payload_read_pipeline_combined_ns"],
                                "payload_decode_projection_result_processing_combined_ns": by_run_kind[
                                    run_kind
                                ][
                                    "payload_decode_projection_result_processing_combined_ns"
                                ],
                                "exclusive_total_ns": by_run_kind[run_kind][
                                    "exclusive_total_ns"
                                ],
                                "unclassified_ns": by_run_kind[run_kind][
                                    "unclassified_ns"
                                ],
                            }
                            for run_kind in ("cold", "warm")
                        },
                    }
                )

    result = {
        "schema": SCHEMA,
        "correctness_gate": "pass",
        "stage_attribution_gate": "pass",
        "timing_comparability": (
            "observer-heavy Detailed attribution; query/process wall values are diagnostic "
            "and MUST NOT be compared with instrumentation-off headline latency"
        ),
        "query_names": list(QUERY_NAMES),
        "backends": list(BACKENDS),
        "queue_depths": QUEUE_DEPTHS,
        "gaps": list(GAPS),
        "benchmark_repeats": BENCHMARK_REPEATS,
        "process_count": len(plan),
        "evaluation_count": len(observations),
        "binary_sha256": binary_hash,
        "corpus_inventory_sha256": before["corpus_sha256"],
        "query_corpus_fingerprint_sha256": next(iter(fingerprints)),
        "sealed_query_manifest_sha256": SEALED_QUERY_MANIFEST_SHA256,
        "max_resident_bytes_after_evict": args.max_resident_bytes_after_evict,
        "required_nonzero_stage_leaves": list(REQUIRED_STAGE_LEAVES),
        "aggregate_stage_attribution": aggregate,
        "observations": observations,
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
    inventory = commands.add_parser("inventory")
    inventory.add_argument("--corpus", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--paths-output", type=Path, required=True)
    compare = commands.add_parser("compare-results")
    compare.add_argument("--index", type=Path, required=True)
    compare.add_argument("--manifest", type=Path, required=True)
    compare.add_argument("--source-manifest", type=Path, required=True)
    compare.add_argument("--inventory-before", type=Path, required=True)
    compare.add_argument("--inventory-after", type=Path, required=True)
    compare.add_argument("--residency", type=Path, required=True)
    compare.add_argument("--summary", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--binary", type=Path, required=True)
    compare.add_argument("--corpus", type=Path, required=True)
    compare.add_argument("--runs-dir", type=Path, required=True)
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
            write_manifest(args.input, args.output_tsv, args.output_json)
        elif args.command == "write-plan":
            write_plan(args.manifest, args.source_manifest, args.output)
        elif args.command == "inventory":
            common.write_inventory(args.corpus, args.output, args.paths_output)
        elif args.command == "compare-results":
            nonnegative_int(
                args.max_resident_bytes_after_evict, "max resident bytes after evict"
            )
            for field, value in expected_limits(args).items():
                positive_int(value, f"limits.{field}")
            compare_results(args)
        else:
            raise AssertionError(args.command)
    except (
        GateError,
        phase1.GateError,
        phase2.GateError,
        phase3.GateError,
        common.GateError,
        OSError,
        TypeError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
    ) as error:
        print(f"Phase 3 payload attribution gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
