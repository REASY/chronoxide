#!/usr/bin/env python3
"""Strict inventory and result gates for the same-binary storage query A/B."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import stat
import struct
import sys
from pathlib import Path
from typing import Any, Iterable


RAW_SCHEMA = "chronoxide.query-benchmark.raw/v5"
INVENTORY_SCHEMA = "chronoxide/storage-query-ab-inventory/v1"
PARSED_SCHEMA = "chronoxide/storage-query-ab-result/v1"
SYMBOLS_MAGIC = int.from_bytes(b"SYMB", "little")

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

READ_COUNT_FIELDS = (
    "legacy_eager_read_delta",
    "logical_returned_delta",
    "root_read_delta",
    "page_read_delta",
    "page_validation_delta",
)

SYMBOL_COUNTER_FIELDS = (
    "page_validation_ns_delta",
    "touched_corrupt_pages_delta",
    "page_cache_hits_delta",
    "page_cache_misses_delta",
    "page_cache_evictions_delta",
)

SYMBOL_RESOURCE_FIELDS = (
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
)

EXPECTED_LIMITS = {
    "max_matched_series": 1_000_000,
    "max_projected_series": 2_000_000,
    "max_chunk_reads": 5_000_000,
    "max_bytes_read": 2 * 1024 * 1024 * 1024,
    "max_samples_decoded": 50_000_000,
    "max_regex_values_examined": 100_000,
}

RANGE_CACHE_BOOL_FIELDS = (
    "governor_refused",
    "allocation_refused",
    "layout_overflow",
)

RANGE_CACHE_COUNT_FIELDS = (
    "configured_budget_bytes",
    "governor_lease_bytes",
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
)


class GateError(ValueError):
    pass


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GateError(f"{name} must be a non-negative integer")
    return value


def _positive_int(value: Any, name: str) -> int:
    value = _nonnegative_int(value, name)
    if value == 0:
        raise GateError(f"{name} must be positive")
    return value


def _sha256_file_descriptor(descriptor: int) -> str:
    digest = hashlib.sha256()
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
    return digest.hexdigest()


def _inventory_file(path: str, relative: str) -> dict[str, Any]:
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode):
        raise GateError(f"corpus entry is not a regular file: {relative!r}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise GateError(f"cannot safely open corpus file {relative!r}: {error}") from None
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise GateError(f"opened corpus entry is not regular: {relative!r}")
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise GateError(f"corpus file changed identity before hashing: {relative!r}")
        digest = _sha256_file_descriptor(descriptor)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    stable_before = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
    stable_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if stable_before != stable_after:
        raise GateError(f"corpus file changed while hashing: {relative!r}")
    return {"path": relative, "size_bytes": opened.st_size, "sha256": digest}


def inventory_corpus(corpus: Path) -> tuple[dict[str, Any], list[bytes]]:
    root = os.path.realpath(os.fspath(corpus))
    root_metadata = os.lstat(root)
    if not stat.S_ISDIR(root_metadata.st_mode):
        raise GateError(f"corpus is not a directory: {root!r}")
    entries: list[dict[str, Any]] = []
    absolute_paths: list[bytes] = []

    def visit(directory: str, relative_directory: str) -> None:
        try:
            children = list(os.scandir(directory))
        except OSError as error:
            raise GateError(f"cannot scan corpus directory {relative_directory!r}: {error}") from None
        children.sort(key=lambda entry: os.fsencode(entry.name))
        for child in children:
            relative = (
                child.name
                if not relative_directory
                else os.path.join(relative_directory, child.name)
            )
            try:
                metadata = child.stat(follow_symlinks=False)
            except OSError as error:
                raise GateError(f"cannot stat corpus entry {relative!r}: {error}") from None
            if stat.S_ISLNK(metadata.st_mode):
                raise GateError(f"symbolic links are forbidden in a corpus: {relative!r}")
            if stat.S_ISDIR(metadata.st_mode):
                visit(child.path, relative)
            elif stat.S_ISREG(metadata.st_mode):
                entries.append(_inventory_file(child.path, relative))
                absolute_paths.append(os.fsencode(os.path.abspath(child.path)))
            else:
                raise GateError(f"non-file corpus entry is forbidden: {relative!r}")

    visit(root, "")
    if not entries:
        raise GateError("corpus contains no regular files")
    entries.sort(key=lambda entry: os.fsencode(entry["path"]))
    absolute_paths.sort()
    return (
        {"schema": INVENTORY_SCHEMA, "corpus": root, "files": entries},
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
            destination.write(path)
            destination.write(b"\0")


def _load_inventory(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        document = json.load(source)
    if document.get("schema") != INVENTORY_SCHEMA:
        raise GateError(f"unsupported inventory schema: {path}")
    files = document.get("files")
    if not isinstance(files, list) or not files:
        raise GateError(f"inventory has no files: {path}")
    return document


def _inventory_map(document: dict[str, Any]) -> dict[str, tuple[str, int]]:
    result: dict[str, tuple[str, int]] = {}
    for entry in document["files"]:
        relative = entry.get("path")
        digest = entry.get("sha256")
        size = entry.get("size_bytes")
        if not isinstance(relative, str) or not isinstance(digest, str):
            raise GateError("inventory contains a malformed file entry")
        _nonnegative_int(size, f"inventory size for {relative!r}")
        if relative in result:
            raise GateError(f"inventory contains duplicate path: {relative!r}")
        result[relative] = (digest, size)
    return result


def _verify_symbol_version(document: dict[str, Any], expected_version: int) -> None:
    root = document["corpus"]
    symbols = [entry for entry in document["files"] if Path(entry["path"]).name == "symbols.bin"]
    if not symbols:
        raise GateError("corpus has no symbols.bin files")
    for entry in symbols:
        path = os.path.join(root, entry["path"])
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            header = os.read(descriptor, 8)
        finally:
            os.close(descriptor)
        if len(header) != 8:
            raise GateError(f"truncated symbols header: {entry['path']!r}")
        magic, version, flags_value = struct.unpack("<IHH", header)
        if magic != SYMBOLS_MAGIC or flags_value != 0 or version != expected_version:
            raise GateError(
                f"unexpected symbols header for {entry['path']!r}: "
                f"magic={magic:#x} version={version} flags={flags_value}"
            )


def compare_corpora(baseline_path: Path, candidate_path: Path, output: Path) -> None:
    baseline_document = _load_inventory(baseline_path)
    candidate_document = _load_inventory(candidate_path)
    _verify_symbol_version(baseline_document, 2)
    _verify_symbol_version(candidate_document, 3)
    baseline = _inventory_map(baseline_document)
    candidate = _inventory_map(candidate_document)
    if baseline.keys() != candidate.keys():
        raise GateError(
            "corpus relative paths differ: "
            f"baseline_only={sorted(baseline.keys() - candidate.keys())!r} "
            f"candidate_only={sorted(candidate.keys() - baseline.keys())!r}"
        )
    allowed = {"symbols.bin", "footer.bin"}
    differences: list[dict[str, Any]] = []
    observed: set[str] = set()
    for relative in sorted(baseline, key=os.fsencode):
        if baseline[relative] == candidate[relative]:
            continue
        artifact = Path(relative).name
        if artifact not in allowed:
            raise GateError(f"non-symbol/footer artifact differs: {relative!r}")
        observed.add(artifact)
        differences.append(
            {
                "path": relative,
                "artifact": artifact,
                "baseline_sha256": baseline[relative][0],
                "baseline_size_bytes": baseline[relative][1],
                "candidate_sha256": candidate[relative][0],
                "candidate_size_bytes": candidate[relative][1],
            }
        )
    missing = allowed - observed
    if missing:
        raise GateError("expected format differences are absent: " + ", ".join(sorted(missing)))
    result = {
        "schema": "chronoxide/storage-query-ab-corpus-comparison/v1",
        "baseline_corpus": baseline_document["corpus"],
        "candidate_corpus": candidate_document["corpus"],
        "identical_non_format_files": len(baseline) - len(differences),
        "allowed_differences": differences,
    }
    with output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def _validate_read_count(symbol_reads: dict[str, Any], field: str) -> dict[str, int]:
    count = symbol_reads.get(field)
    if not isinstance(count, dict) or set(count) != {"calls", "bytes"}:
        raise GateError(f"symbol_reads.{field} has an invalid shape")
    return {
        "calls": _nonnegative_int(count["calls"], f"symbol_reads.{field}.calls"),
        "bytes": _nonnegative_int(count["bytes"], f"symbol_reads.{field}.bytes"),
    }


def _validate_symbol_reads(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GateError("run.symbol_reads is missing or invalid")
    expected = set(READ_COUNT_FIELDS) | set(SYMBOL_COUNTER_FIELDS) | set(SYMBOL_RESOURCE_FIELDS)
    if set(value) != expected:
        raise GateError(
            f"run.symbol_reads fields differ from v5 contract: "
            f"missing={sorted(expected - set(value))!r} extra={sorted(set(value) - expected)!r}"
        )
    validated: dict[str, Any] = {
        field: _validate_read_count(value, field) for field in READ_COUNT_FIELDS
    }
    for field in SYMBOL_COUNTER_FIELDS + SYMBOL_RESOURCE_FIELDS:
        validated[field] = _nonnegative_int(value[field], f"symbol_reads.{field}")
    if validated["touched_corrupt_pages_delta"] != 0:
        raise GateError("query touched a corrupt symbol page")
    if validated["resource_snapshot_errors_after_run"] != 0:
        raise GateError("symbol resource snapshot reported errors")
    expected_total = (
        validated["root_retained_charge_bytes_after_run"]
        + validated["eager_dictionary_retained_charge_bytes_after_run"]
        + validated["page_cache_charge_bytes_after_run"]
    )
    if validated["total_retained_charge_bytes_after_run"] != expected_total:
        raise GateError("symbol retained-charge components do not equal total charge")
    if (
        validated["page_cache_charge_bytes_after_run"]
        > validated["page_cache_max_bytes_after_run"]
    ):
        raise GateError("symbol page-cache charge exceeds its reported maximum")
    physical_calls = sum(
        validated[field]["calls"]
        for field in ("legacy_eager_read_delta", "root_read_delta", "page_read_delta")
    )
    physical_bytes = sum(
        validated[field]["bytes"]
        for field in ("legacy_eager_read_delta", "root_read_delta", "page_read_delta")
    )
    logical_bytes = validated["logical_returned_delta"]["bytes"]
    validated["physical_read_calls"] = physical_calls
    validated["physical_read_bytes"] = physical_bytes
    validated["read_amplification_numerator"] = physical_bytes
    validated["read_amplification_denominator"] = logical_bytes
    return validated


def _validate_payload_reads(value: Any, stats: dict[str, int]) -> dict[str, int]:
    expected = {"logical_used_bytes", "physical_reads", "physical_bytes"}
    if not isinstance(value, dict) or set(value) != expected:
        raise GateError("run.payload_reads has an invalid v5 shape")
    result = {
        field: _nonnegative_int(value[field], f"payload_reads.{field}") for field in expected
    }
    if result["logical_used_bytes"] != stats["bytes_read"]:
        raise GateError("payload logical-used bytes differ from QueryStats.bytes_read")
    if result["physical_bytes"] < result["logical_used_bytes"]:
        raise GateError("payload physical bytes are smaller than logical-used bytes")
    result["read_amplification_numerator"] = result["physical_bytes"]
    result["read_amplification_denominator"] = result["logical_used_bytes"]
    return result


def _validate_stats(value: Any) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != set(QUERY_STATS_FIELDS):
        raise GateError("run.stats fields differ from canonical QueryStats contract")
    return {field: _nonnegative_int(value[field], f"stats.{field}") for field in QUERY_STATS_FIELDS}


def _validate_range_cache(value: Any, expected_budget: int) -> dict[str, Any]:
    expected = set(RANGE_CACHE_BOOL_FIELDS) | set(RANGE_CACHE_COUNT_FIELDS)
    if not isinstance(value, dict) or set(value) != expected:
        raise GateError("run.range_scalar_cache fields differ from the raw v5 contract")
    validated: dict[str, Any] = {}
    for field in RANGE_CACHE_BOOL_FIELDS:
        if not isinstance(value[field], bool):
            raise GateError(f"range_scalar_cache.{field} must be boolean")
        validated[field] = value[field]
    for field in RANGE_CACHE_COUNT_FIELDS:
        validated[field] = _nonnegative_int(
            value[field], f"range_scalar_cache.{field}"
        )
    if validated["configured_budget_bytes"] != expected_budget:
        raise GateError("range scalar cache budget differs from the pinned value")
    return validated


def _expected_configuration(args: argparse.Namespace) -> dict[str, Any]:
    range_budget = args.range_scalar_cache_max_bytes if args.step_ms is not None else None
    return {
        "segments_dir": os.path.realpath(os.fspath(args.corpus)),
        "start_ms": args.start_ms,
        "end_ms": args.end_ms,
        "mode": "query_range" if args.step_ms is not None else "instant",
        "step_ms": args.step_ms,
        "range_scalar_cache_max_bytes": range_budget,
        "chunk_read_mode": "pread",
        "chunk_read_queue_depth": args.queue_depth,
        "experimental_cross_segment_chunk_reads": False,
        "experimental_storage_layout_ab": args.format == "v7",
        "benchmark_repeats": 2,
        "queries": [args.query],
        "prewarm_query_contexts": False,
        "prefetch_query_data": False,
        "exponential_histogram_bucket_boundaries": [],
        "validate_segment_footers": False,
    }


def parse_raw_result(args: argparse.Namespace) -> None:
    with args.raw.open(encoding="utf-8") as source:
        document = json.load(source)
    if document.get("schema") != RAW_SCHEMA:
        raise GateError(f"raw benchmark schema must be {RAW_SCHEMA}")
    expected_configuration = _expected_configuration(args)
    if document.get("configuration") != expected_configuration:
        raise GateError(
            "raw benchmark configuration differs from the pinned invocation: "
            f"expected={expected_configuration!r} actual={document.get('configuration')!r}"
        )
    if document.get("limits") != EXPECTED_LIMITS:
        raise GateError("raw benchmark query limits differ from the pinned production limits")
    fingerprint = document.get("corpus_fingerprint_sha256")
    if not isinstance(fingerprint, str) or len(fingerprint) != 64:
        raise GateError("raw benchmark corpus fingerprint is invalid")
    runs = document.get("runs")
    if not isinstance(runs, list) or len(runs) != 2:
        raise GateError("each process must contain exactly one cold and one warm run")
    parsed_runs: list[dict[str, Any]] = []
    for index, expected_kind in enumerate(("cold", "warm")):
        run = runs[index]
        if run.get("query") != args.query:
            raise GateError("raw run query differs from the expected expression")
        if run.get("run_kind") != expected_kind or run.get("run_index") != index:
            raise GateError("raw runs must be ordered cold index 0, warm index 1")
        if run.get("effective_start_ms") != args.start_ms:
            raise GateError("raw run effective start differs from the requested start")
        if run.get("effective_end_ms") != args.end_ms:
            raise GateError("raw run effective end differs from the requested end")
        if run.get("step_ms") != args.step_ms:
            raise GateError("raw run step differs from the requested step")
        stats = _validate_stats(run.get("stats"))
        result_series = _positive_int(run.get("result_series"), "run.result_series")
        result_samples = _positive_int(run.get("result_samples"), "run.result_samples")
        semantic = run.get("semantic_fingerprint_sha256")
        portable = run.get("portable_semantic_fingerprint_sha256")
        if not isinstance(semantic, str) or len(semantic) != 64:
            raise GateError("run semantic fingerprint is invalid")
        if not isinstance(portable, str) or len(portable) != 64:
            raise GateError("run portable semantic fingerprint is invalid")
        range_cache = run.get("range_scalar_cache")
        if args.step_ms is None:
            if range_cache is not None:
                raise GateError("instant query unexpectedly reported range scalar cache state")
        else:
            range_cache = _validate_range_cache(
                range_cache, args.range_scalar_cache_max_bytes
            )
        parsed_runs.append(
            {
                "run_kind": expected_kind,
                "run_index": index,
                "duration_ns": _positive_int(run.get("duration_ns"), "run.duration_ns"),
                "effective_start_ms": _nonnegative_int(
                    run.get("effective_start_ms"), "run.effective_start_ms"
                ),
                "effective_end_ms": _nonnegative_int(
                    run.get("effective_end_ms"), "run.effective_end_ms"
                ),
                "step_ms": run.get("step_ms"),
                "semantic_fingerprint_sha256": semantic,
                "portable_semantic_fingerprint_sha256": portable,
                "result_series": result_series,
                "result_samples": result_samples,
                "stats": stats,
                "payload_reads": _validate_payload_reads(run.get("payload_reads"), stats),
                "symbol_reads": _validate_symbol_reads(run.get("symbol_reads")),
                "range_scalar_cache": range_cache,
            }
        )
    raw_hash = hashlib.sha256(args.raw.read_bytes()).hexdigest()
    result = {
        "schema": PARSED_SCHEMA,
        "process_label": args.process_label,
        "format": args.format,
        "repetition": args.repetition,
        "order_index": args.order_index,
        "query_name": args.query_name,
        "query": args.query,
        "max_rss_kib": args.max_rss_kib,
        "raw_sha256": raw_hash,
        "corpus_fingerprint_sha256": fingerprint,
        "runs": parsed_runs,
    }
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def _load_parsed_results(paths: Iterable[Path]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for path in paths:
        with path.open(encoding="utf-8") as source:
            result = json.load(source)
        if result.get("schema") != PARSED_SCHEMA:
            raise GateError(f"unsupported parsed-result schema: {path}")
        results.append(result)
    return results


def _canonical_signature(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "effective_start_ms": run["effective_start_ms"],
        "effective_end_ms": run["effective_end_ms"],
        "step_ms": run["step_ms"],
        "semantic_fingerprint_sha256": run["semantic_fingerprint_sha256"],
        "portable_semantic_fingerprint_sha256": run[
            "portable_semantic_fingerprint_sha256"
        ],
        "result_series": run["result_series"],
        "result_samples": run["result_samples"],
        "stats": run["stats"],
    }


def _stats_digest(stats: dict[str, int]) -> str:
    canonical = json.dumps(stats, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(canonical).hexdigest()


def _ratio(numerator: int, denominator: int) -> str:
    return "" if denominator == 0 else f"{numerator / denominator:.6f}"


def compare_results(
    paths: list[Path],
    repeats: int,
    query_names: list[str],
    summary_path: Path,
    output_path: Path,
) -> None:
    if repeats <= 0 or repeats % 2:
        raise GateError("comparison repeats must be a positive even integer")
    results = _load_parsed_results(paths)
    expected_processes = repeats * len(query_names) * 2
    if len(results) != expected_processes:
        raise GateError(
            f"expected {expected_processes} parsed processes, found {len(results)}"
        )
    by_key: dict[tuple[str, int, str], dict[str, Any]] = {}
    labels: set[str] = set()
    for result in results:
        label = result["process_label"]
        if label in labels:
            raise GateError(f"duplicate process label: {label}")
        labels.add(label)
        key = (result["query_name"], result["repetition"], result["format"])
        if key in by_key:
            raise GateError(f"duplicate query/repetition/format result: {key!r}")
        by_key[key] = result

    comparison_queries: dict[str, Any] = {}
    for query_name in query_names:
        signatures: set[str] = set()
        run_kind_accounting_signatures: dict[str, dict[str, set[str]]] = {
            accounting: {"cold": set(), "warm": set()}
            for accounting in (
                "payload_reads",
                "range_scalar_cache",
                "logical_symbol_work",
            )
        }
        corpus_fingerprints: dict[str, set[str]] = {"v7": set(), "vnext": set()}
        format_symbol_totals = {
            "v7": {"legacy": 0, "root": 0, "page": 0},
            "vnext": {"legacy": 0, "root": 0, "page": 0},
        }
        for repetition in range(1, repeats + 1):
            expected_order = ("v7", "vnext") if repetition % 2 else ("vnext", "v7")
            for expected_index, format_name in enumerate(expected_order, start=1):
                key = (query_name, repetition, format_name)
                if key not in by_key:
                    raise GateError(f"missing process result: {key!r}")
                result = by_key[key]
                if result["order_index"] != expected_index:
                    raise GateError(f"format order was not alternated for {key!r}")
                corpus_fingerprints[format_name].add(
                    result["corpus_fingerprint_sha256"]
                )
                for run in result["runs"]:
                    signatures.add(
                        json.dumps(_canonical_signature(run), separators=(",", ":"), sort_keys=True)
                    )
                    symbols = run["symbol_reads"]
                    run_kind = run["run_kind"]
                    run_kind_accounting_signatures["payload_reads"][run_kind].add(
                        json.dumps(
                            run["payload_reads"],
                            separators=(",", ":"),
                            sort_keys=True,
                        )
                    )
                    run_kind_accounting_signatures["range_scalar_cache"][run_kind].add(
                        json.dumps(
                            run["range_scalar_cache"],
                            separators=(",", ":"),
                            sort_keys=True,
                        )
                    )
                    run_kind_accounting_signatures["logical_symbol_work"][run_kind].add(
                        json.dumps(
                            symbols["logical_returned_delta"],
                            separators=(",", ":"),
                            sort_keys=True,
                        )
                    )
                    format_symbol_totals[format_name]["legacy"] += symbols[
                        "legacy_eager_read_delta"
                    ]["bytes"]
                    format_symbol_totals[format_name]["root"] += symbols["root_read_delta"][
                        "bytes"
                    ]
                    format_symbol_totals[format_name]["page"] += symbols["page_read_delta"][
                        "bytes"
                    ]
                    if symbols["source_file_bytes_after_run"] <= 0:
                        raise GateError("symbol source-file gauge is zero after a nonempty query")
                    if format_name == "v7":
                        if symbols["root_retained_charge_bytes_after_run"] != 0:
                            raise GateError("v7 eager backend unexpectedly retains a v3 root")
                        if symbols["page_cache_max_bytes_after_run"] != 0:
                            raise GateError("v7 eager backend unexpectedly reports a page cache")
                        if symbols["eager_dictionary_retained_charge_bytes_after_run"] <= 0:
                            raise GateError("v7 eager backend reports no retained dictionary charge")
                    else:
                        if symbols["eager_dictionary_retained_charge_bytes_after_run"] != 0:
                            raise GateError("vNext paged backend unexpectedly retains an eager dictionary")
                        if symbols["root_retained_charge_bytes_after_run"] <= 0:
                            raise GateError("vNext paged backend reports no retained root charge")
                        if symbols["page_cache_max_bytes_after_run"] <= 0:
                            raise GateError("vNext paged backend reports no page-cache capacity")
        if len(signatures) != 1:
            raise GateError(
                f"effective schedule, semantic fingerprints, result shapes, or canonical "
                f"QueryStats differ for {query_name}"
            )
        for accounting, by_run_kind in run_kind_accounting_signatures.items():
            for run_kind, accounting_signatures in by_run_kind.items():
                if len(accounting_signatures) != 1:
                    raise GateError(
                        f"{accounting.replace('_', ' ')} differs across layouts or "
                        f"repetitions for {query_name} {run_kind} runs"
                    )
        if any(len(values) != 1 for values in corpus_fingerprints.values()):
            raise GateError(f"same-format corpus fingerprint changed for {query_name}")
        if corpus_fingerprints["v7"] == corpus_fingerprints["vnext"]:
            raise GateError("v7 and vNext corpus fingerprints unexpectedly match")
        if format_symbol_totals["v7"]["legacy"] <= 0:
            raise GateError("v7 eager backend issued no legacy symbol read")
        if format_symbol_totals["v7"]["root"] or format_symbol_totals["v7"]["page"]:
            raise GateError("v7 eager backend issued v3 root/page reads")
        if format_symbol_totals["vnext"]["legacy"]:
            raise GateError("vNext paged backend issued a legacy eager read")
        if format_symbol_totals["vnext"]["root"] <= 0 or format_symbol_totals["vnext"]["page"] <= 0:
            raise GateError("vNext paged backend did not exercise both root and page reads")
        comparison_queries[query_name] = {
            "canonical_signature_sha256": hashlib.sha256(next(iter(signatures)).encode()).hexdigest(),
            "corpus_fingerprints": {
                format_name: next(iter(values))
                for format_name, values in corpus_fingerprints.items()
            },
            "run_kind_accounting": {
                run_kind: {
                    accounting: json.loads(next(iter(by_run_kind[run_kind])))
                    for accounting, by_run_kind in run_kind_accounting_signatures.items()
                }
                for run_kind in ("cold", "warm")
            },
            "symbol_physical_bytes": format_symbol_totals,
        }

    fields = [
        "process_label",
        "query_name",
        "format",
        "repetition",
        "order_index",
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
        "symbol_logical_values",
        "symbol_logical_bytes",
        "symbol_physical_reads",
        "symbol_physical_bytes",
        "symbol_read_amplification",
        *SYMBOL_COUNTER_FIELDS,
        *SYMBOL_RESOURCE_FIELDS,
    ]
    with summary_path.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for result in sorted(
            results,
            key=lambda item: (
                item["query_name"],
                item["repetition"],
                item["order_index"],
            ),
        ):
            for run in result["runs"]:
                symbols = run["symbol_reads"]
                payload = run["payload_reads"]
                row: dict[str, Any] = {
                    "process_label": result["process_label"],
                    "query_name": result["query_name"],
                    "format": result["format"],
                    "repetition": result["repetition"],
                    "order_index": result["order_index"],
                    "run_kind": run["run_kind"],
                    "duration_ns": run["duration_ns"],
                    "max_rss_kib": result["max_rss_kib"],
                    "result_series": run["result_series"],
                    "result_samples": run["result_samples"],
                    "semantic_fingerprint": run["semantic_fingerprint_sha256"],
                    "portable_fingerprint": run["portable_semantic_fingerprint_sha256"],
                    "query_stats_sha256": _stats_digest(run["stats"]),
                    "payload_used_bytes": payload["logical_used_bytes"],
                    "payload_physical_reads": payload["physical_reads"],
                    "payload_physical_bytes": payload["physical_bytes"],
                    "payload_read_amplification": _ratio(
                        payload["physical_bytes"], payload["logical_used_bytes"]
                    ),
                    "symbol_logical_values": symbols["logical_returned_delta"]["calls"],
                    "symbol_logical_bytes": symbols["logical_returned_delta"]["bytes"],
                    "symbol_physical_reads": symbols["physical_read_calls"],
                    "symbol_physical_bytes": symbols["physical_read_bytes"],
                    "symbol_read_amplification": _ratio(
                        symbols["physical_read_bytes"],
                        symbols["logical_returned_delta"]["bytes"],
                    ),
                }
                for field in QUERY_STATS_FIELDS:
                    row[f"stats_{field}"] = run["stats"][field]
                for field in SYMBOL_COUNTER_FIELDS + SYMBOL_RESOURCE_FIELDS:
                    row[field] = symbols[field]
                writer.writerow(row)

    output = {
        "schema": "chronoxide/storage-query-ab-comparison/v1",
        "process_count": len(results),
        "repeats": repeats,
        "query_names": query_names,
        "queries": comparison_queries,
        "canonical_equivalence": "pass",
    }
    with output_path.open("x", encoding="utf-8") as destination:
        json.dump(output, destination, indent=2, sort_keys=True)
        destination.write("\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    inventory = commands.add_parser("inventory")
    inventory.add_argument("--corpus", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--paths-output", type=Path, required=True)

    compare = commands.add_parser("compare-corpora")
    compare.add_argument("--baseline", type=Path, required=True)
    compare.add_argument("--candidate", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)

    raw = commands.add_parser("parse-raw")
    raw.add_argument("--raw", type=Path, required=True)
    raw.add_argument("--output", type=Path, required=True)
    raw.add_argument("--process-label", required=True)
    raw.add_argument("--format", choices=("v7", "vnext"), required=True)
    raw.add_argument("--repetition", type=int, required=True)
    raw.add_argument("--order-index", type=int, choices=(1, 2), required=True)
    raw.add_argument("--query-name", required=True)
    raw.add_argument("--query", required=True)
    raw.add_argument("--corpus", type=Path, required=True)
    raw.add_argument("--max-rss-kib", type=int, required=True)
    raw.add_argument("--start-ms", type=int, required=True)
    raw.add_argument("--end-ms", type=int, required=True)
    raw.add_argument("--step-ms", type=int)
    raw.add_argument("--range-scalar-cache-max-bytes", type=int, default=0)
    raw.add_argument("--queue-depth", type=int, required=True)

    results = commands.add_parser("compare-results")
    results.add_argument("--input", type=Path, action="append", required=True)
    results.add_argument("--repeats", type=int, required=True)
    results.add_argument("--query-name", action="append", required=True)
    results.add_argument("--summary", type=Path, required=True)
    results.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "inventory":
            write_inventory(args.corpus, args.output, args.paths_output)
        elif args.command == "compare-corpora":
            compare_corpora(args.baseline, args.candidate, args.output)
        elif args.command == "parse-raw":
            parse_raw_result(args)
        elif args.command == "compare-results":
            compare_results(args.input, args.repeats, args.query_name, args.summary, args.output)
        else:
            raise AssertionError(args.command)
    except (GateError, OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"storage query A/B gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
