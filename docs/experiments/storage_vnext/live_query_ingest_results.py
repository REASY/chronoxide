#!/usr/bin/env python3
"""Deterministically summarize COMPLETE live-query ingestion D/P/Q results.

This is deliberately a read-only consumer of result roots.  It independently
reconciles the raw GNU-time, perf-stat, RSS, live-publication, and HTTP-client
artifacts with the JSON summaries emitted by the experiment harness before it
reports any measurements.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any, Iterable, Sequence


SCHEMA = "chronoxide/live-query-ingest-results/v1"
CLIENT_SCHEMA = "chronoxide/live-query-ingest-client/v1"
RUN_SET_SCHEMA = "chronoxide/live-query-ingest-ab/v1"
VARIANTS = ("D", "P", "Q")
PERCENTILES = ("count", "min", "p50", "p95", "p99", "max")
QUERY_STATS_FIELDS = {
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
}
QUERY_IO_FIELDS = {
    "chunk_payload_used_bytes",
    "chunk_payload_read_bytes",
    "chunk_payload_physical_reads",
    "series_entry_bytes",
    "chunk_index_range_bytes",
    "exact_postings_bytes",
}
CLIENT_DURATION_FIELDS = (
    "client_elapsed_ns",
    "query_duration_ns",
    "serialize_duration_ns",
    "queue_duration_ns",
    "view_age_ms",
    "view_pin_wait_ns",
    "view_pin_held_ns",
)
PUBLICATION_TIMING_FIELDS = (
    "publication_duration_ns",
    "freeze_and_admission_ns",
    "seal_ns",
    "inventory_ns",
    "coverage_ns",
    "sample_root_ns",
    "catalog_ns",
    "owner_and_head_ns",
    "owner_validation_ns",
    "head_validation_ns",
    "root_build_ns",
    "begin_commit_root_lock_wait_ns",
    "begin_commit_root_lock_held_ns",
    "commit_root_lock_wait_ns",
    "commit_root_lock_held_ns",
    "old_root_arc_drop_ns",
    "post_commit_ns",
)
PUBLICATION_MAXIMUM_FIELDS = (
    "pending_fragment_count",
    "pending_estimated_bytes",
    "pending_arena_used_bytes",
    "pending_arena_allocated_bytes",
    "sample_keys",
    "sample_fragments",
    "catalog_active_series",
    "catalog_shared_label_snapshot_bytes",
    "catalog_index_bytes_if_unshared",
    "live_memory_limit_bytes",
    "live_memory_charged_bytes",
    "live_memory_peak_charged_bytes",
    "live_mutable_tail_used_bytes",
    "live_mutable_tail_capacity_bytes",
    "manifest_validated_offset",
)
RSS_COLUMNS = {
    "aggregate_rss_kib": "rss_kib",
    "aggregate_rss_anon_kib": "rss_anon_kib",
    "aggregate_rss_file_kib": "rss_file_kib",
    "aggregate_vm_swap_kib": "vm_swap_kib",
    "max_single_process_hwm_kib": "max_single_hwm_kib",
    "process_count": "process_count",
}
TIME_FIELDS = {
    "user_seconds": "User time (seconds)",
    "system_seconds": "System time (seconds)",
    "elapsed": "Elapsed (wall clock) time (h:mm:ss or m:ss)",
    "max_rss_kib": "Maximum resident set size (kbytes)",
    "major_page_faults": "Major (requiring I/O) page faults",
    "minor_page_faults": "Minor (reclaiming a frame) page faults",
    "voluntary_context_switches": "Voluntary context switches",
    "involuntary_context_switches": "Involuntary context switches",
    "filesystem_inputs": "File system inputs",
    "filesystem_outputs": "File system outputs",
    "exit_status": "Exit status",
}
DELTA_METRICS = (
    "elapsed_seconds",
    "messages_per_second",
    "time_cpu_seconds",
    "time_cpu_utilization_percent",
    "proc_tree_peak_rss_kib",
    "cycles",
    "instructions",
    "ipc",
    "cache_misses",
    "cache_misses_per_million_instructions",
    "context_switches",
    "context_switches_per_second",
    "cpu_migrations",
    "page_faults",
)
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


class SummaryError(ValueError):
    pass


def _require_file(path: Path) -> Path:
    if not path.is_file() or path.is_symlink():
        raise SummaryError(f"required regular file is missing: {path}")
    return path


def _load_object(path: Path) -> dict[str, Any]:
    _require_file(path)
    try:
        with path.open(encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        raise SummaryError(f"cannot read JSON object {path}: {error}") from error
    if not isinstance(value, dict):
        raise SummaryError(f"expected a JSON object: {path}")
    return value


def _nonnegative_int(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SummaryError(f"{context} must be a non-negative integer")
    return value


def _positive_int(value: Any, context: str) -> int:
    value = _nonnegative_int(value, context)
    if value == 0:
        raise SummaryError(f"{context} must be greater than zero")
    return value


def _decimal(value: Any, context: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise SummaryError(f"{context} is not numeric: {value!r}") from error
    if not result.is_finite():
        raise SummaryError(f"{context} must be finite")
    return result


def _json_number(value: Decimal | None, places: int = 6) -> int | float | None:
    if value is None:
        return None
    quantizer = Decimal(1).scaleb(-places)
    rounded = value.quantize(quantizer)
    if rounded == rounded.to_integral():
        return int(rounded)
    return float(rounded)


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def distribution(values: Sequence[int]) -> dict[str, int]:
    if not values:
        return {name: 0 for name in PERCENTILES}
    ordered = sorted(values)

    def percentile(numerator: int) -> int:
        rank = max(1, (len(ordered) * numerator + 99) // 100)
        return ordered[min(rank, len(ordered)) - 1]

    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": percentile(50),
        "p95": percentile(95),
        "p99": percentile(99),
        "max": ordered[-1],
    }


def _numeric_distribution(values: Sequence[Decimal]) -> dict[str, int | float]:
    if not values:
        return {name: 0 for name in PERCENTILES}
    ordered = sorted(values)

    def percentile(numerator: int) -> Decimal:
        rank = max(1, (len(ordered) * numerator + 99) // 100)
        return ordered[min(rank, len(ordered)) - 1]

    return {
        "count": len(ordered),
        "min": _json_number(ordered[0]),
        "p50": _json_number(percentile(50)),
        "p95": _json_number(percentile(95)),
        "p99": _json_number(percentile(99)),
        "max": _json_number(ordered[-1]),
    }


def parse_elapsed_seconds(value: str) -> Decimal:
    """Parse GNU-time elapsed syntax without losing its recorded precision."""

    fields = value.strip().split(":")
    if not 1 <= len(fields) <= 3 or any(not field for field in fields):
        raise SummaryError(f"malformed GNU-time elapsed value: {value!r}")
    try:
        seconds = Decimal(fields[-1])
        minutes = Decimal(fields[-2]) if len(fields) >= 2 else Decimal(0)
        hours = Decimal(fields[-3]) if len(fields) == 3 else Decimal(0)
    except InvalidOperation as error:
        raise SummaryError(f"malformed GNU-time elapsed value: {value!r}") from error
    if seconds < 0 or seconds >= 60 or minutes < 0 or minutes >= 60 or hours < 0:
        raise SummaryError(f"out-of-range GNU-time elapsed value: {value!r}")
    result = hours * 3600 + minutes * 60 + seconds
    if result <= 0:
        raise SummaryError("GNU-time elapsed duration must be positive")
    return result


def _read_key_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for number, line in enumerate(
        _require_file(path).read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line:
            continue
        if "=" not in line:
            raise SummaryError(f"{path}:{number}: expected key=value")
        key, value = line.split("=", 1)
        if not key or key in values:
            raise SummaryError(f"{path}:{number}: duplicate or empty key {key!r}")
        values[key] = value
    return values


def _parse_run_plan(path: Path) -> tuple[list[str], dict[str, int]]:
    with _require_file(path).open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if not rows or set(rows[0]) != {"order", "variant", "config", "segments_dir"}:
        raise SummaryError(f"unexpected run-plan schema: {path}")
    if len(rows) != 3:
        raise SummaryError("run-plan must contain exactly three variants")
    ordered: list[tuple[int, str]] = []
    for row in rows:
        try:
            position = int(row["order"])
        except ValueError as error:
            raise SummaryError("run-plan order must be an integer") from error
        ordered.append((position, row["variant"]))
    ordered.sort()
    if [position for position, _variant in ordered] != [1, 2, 3]:
        raise SummaryError("run-plan positions must be exactly 1, 2, 3")
    variants = [variant for _position, variant in ordered]
    if set(variants) != set(VARIANTS):
        raise SummaryError("run-plan must contain D, P, and Q exactly once")
    return variants, {variant: position for position, variant in ordered}


def _validate_gates(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], int]:
    complete = root / "COMPLETE"
    if not complete.is_file() or complete.is_symlink():
        raise SummaryError(f"result root is not COMPLETE: {root}")
    run_gate = _load_object(root / "comparisons" / "dpq-gate.json")
    if (
        run_gate.get("schema") != RUN_SET_SCHEMA
        or run_gate.get("complete") is not True
        or run_gate.get("storage_trees_equal") is not True
        or run_gate.get("replay_counters_equal") is not True
        or run_gate.get("live_head_only_observed") is not True
    ):
        raise SummaryError(f"D/P/Q correctness gate is not a complete pass: {root}")
    messages = _positive_int(run_gate.get("expected_messages"), "expected messages")

    storage = _load_object(root / "validation" / "storage-verify-gate.json")
    if storage.get("schema_version") != 8:
        raise SummaryError("storage verifier gate did not pass Schema 8")
    segments = _positive_int(storage.get("segments"), "verified segments")
    samples = _positive_int(storage.get("samples"), "verified samples")
    for field in (
        "verified_selection_fingerprint",
        "exact_postings_fingerprint",
    ):
        if re.fullmatch(r"[0-9a-f]{64}", str(storage.get(field, ""))) is None:
            raise SummaryError(f"storage verifier gate lacks {field}")
    recorded = _positive_int(
        storage.get("recorded_head_writes"), "recorded head writes"
    )
    collapsed = _nonnegative_int(
        storage.get("recorded_writes_minus_physical_rows"),
        "recorded writes minus physical rows",
    )
    if recorded < samples or recorded - samples != collapsed:
        raise SummaryError("storage writer/physical-row counts do not reconcile")
    strong_reconciliation = storage.get(
        "writer_to_verifier_counts_reconciled"
    )
    if strong_reconciliation is None:
        if storage.get("physical_sample_count_exactly_gated") is not False:
            raise SummaryError("legacy storage gate lacks its physical-count gap")
        strong_reconciliation = False
        gate_revision = "legacy-one-sided"
        capture_golden = False
    else:
        if strong_reconciliation is not True:
            raise SummaryError("storage writer/verifier reconciliation did not pass")
        gate_revision = "writer-verifier-reconciled"
        capture_golden = storage.get(
            "capture_level_physical_sample_golden_gated"
        )
        if not isinstance(capture_golden, bool):
            raise SummaryError("storage gate lacks capture-level golden status")
        _positive_int(storage.get("series"), "verified series")
        _positive_int(storage.get("chunks"), "verified chunks")
        if re.fullmatch(
            r"[0-9a-f]{64}",
            str(storage.get("decoded_semantic_fingerprint", "")),
        ) is None:
            raise SummaryError("storage gate lacks decoded semantic fingerprint")
        marker = root / "metadata" / (
            "CAPTURE_LEVEL_PHYSICAL_SAMPLE_GOLDEN_GATED"
            if capture_golden
            else "PHYSICAL_SAMPLE_COUNT_COVERAGE_GAP"
        )
        if not marker.is_file() or marker.is_symlink():
            raise SummaryError("storage physical-count coverage marker is missing")
    normalized_storage = {
        **storage,
        "segments": segments,
        "samples": samples,
        "recorded_head_writes": recorded,
        "recorded_writes_minus_physical_rows": collapsed,
        "writer_to_verifier_counts_reconciled": strong_reconciliation,
        "capture_level_physical_sample_golden_gated": capture_golden,
        "gate_revision": gate_revision,
    }

    readbacks = _load_object(root / "validation" / "readbacks-gate.json")
    expected = _positive_int(readbacks.get("expected_queries"), "expected readbacks")
    executed = _positive_int(readbacks.get("executed_queries"), "executed readbacks")
    if expected != executed:
        raise SummaryError("readback gate did not execute every expected query")
    for field in ("skipped_queries", "isolation_check_skips", "mismatches"):
        if _nonnegative_int(readbacks.get(field), f"readback {field}") != 0:
            raise SummaryError(f"readback gate has nonzero {field}")
    return run_gate, normalized_storage, messages


def _verify_artifact_manifest(root: Path) -> str:
    manifest_path = _require_file(root / "metadata" / "result-artifacts.sha256")
    recorded: dict[str, str] = {}
    for number, line in enumerate(
        manifest_path.read_text(encoding="utf-8").splitlines(), 1
    ):
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if match is None:
            raise SummaryError(f"malformed artifact checksum at line {number}")
        relative = Path(match.group(2))
        if relative.is_absolute() or any(
            part in ("", ".", "..") for part in relative.parts
        ):
            raise SummaryError(f"artifact checksum path escapes its root: {relative}")
        name = relative.as_posix()
        if name in recorded:
            raise SummaryError(f"duplicate artifact checksum path: {name}")
        recorded[name] = match.group(1)
    if not recorded:
        raise SummaryError("artifact checksum manifest is empty")

    current: dict[str, Path] = {}
    for top in ("configs", "metadata", "validation", "comparisons", "runs"):
        directory = root / top
        if not directory.is_dir() or directory.is_symlink():
            raise SummaryError(f"artifact directory is missing: {directory}")
        for path in directory.rglob("*"):
            relative = path.relative_to(root)
            parts = relative.parts
            if (
                relative.as_posix() == "metadata/result-artifacts.sha256"
                or (
                    len(parts) >= 4
                    and parts[0] == "runs"
                    and parts[2] == "segments"
                )
            ):
                continue
            if path.is_symlink():
                raise SummaryError(f"result artifacts contain a symlink: {relative}")
            if path.is_file():
                current[relative.as_posix()] = path
            elif not path.is_dir():
                raise SummaryError(f"result artifact is not regular: {relative}")
    for name in ("run-plan.tsv", "run-summary.tsv"):
        if name not in recorded:
            continue
        path = root / name
        if not path.is_file() or path.is_symlink():
            raise SummaryError(f"result artifact is missing: {name}")
        current[name] = path
    if set(current) != set(recorded):
        missing = sorted(set(recorded) - set(current))
        extra = sorted(set(current) - set(recorded))
        raise SummaryError(
            f"artifact checksum file set mismatch: missing={missing}, extra={extra}"
        )
    for name, path in sorted(current.items()):
        if _sha256_file(path) != recorded[name]:
            raise SummaryError(f"artifact checksum mismatch: {name}")
    return _sha256_file(manifest_path)


def _binary_hashes(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for number, line in enumerate(
        _require_file(path).read_text(encoding="utf-8").splitlines(), 1
    ):
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if match is None:
            raise SummaryError(f"malformed binary checksum at line {number}")
        role = Path(match.group(2)).name
        if role in result:
            raise SummaryError(f"duplicate preserved binary role: {role}")
        binary = _require_file(path.parent / "binaries" / role)
        if _sha256_file(binary) != match.group(1):
            raise SummaryError(f"preserved binary checksum mismatch: {role}")
        result[role] = match.group(1)
    required = {
        "chronoxide-ingester",
        "chronoxide-query",
        "chronoxide-storage-verify",
    }
    allowed = {*required, "chronoxide-api"}
    if not required.issubset(result) or not set(result).issubset(allowed):
        raise SummaryError("preserved binary checksum set is incomplete")
    return dict(sorted(result.items()))


def _provenance(root: Path, settings: dict[str, str], messages: int) -> dict[str, Any]:
    inputs = _load_object(root / "metadata" / "validated-inputs.json")
    _positive_int(
        inputs.get("stop_after_messages"), "validated corpus reference messages"
    )
    try:
        configured_messages = int(settings.get("stop_after_messages", ""))
    except ValueError as error:
        raise SummaryError("settings stop_after_messages is not an integer") from error
    if configured_messages != messages:
        raise SummaryError("settings and D/P/Q gate disagree on message count")
    normalized_inputs = {
        key: value
        for key, value in inputs.items()
        if key not in {"capture", "config_template"}
    }
    stable_settings = {
        key: value
        for key, value in settings.items()
        if key
        not in {
            "recorded_at",
            "result_dir",
            "capture",
            "config_template",
            "run_order",
            "run_note",
        }
    }
    cpusets = _load_object(root / "metadata" / "cpusets.json")
    environment = _require_file(root / "metadata" / "environment.txt").read_text(
        encoding="utf-8"
    ).splitlines()
    if len(environment) < 2 or not environment[1]:
        raise SummaryError("environment evidence lacks uname identity")
    normalized = {
        "validated_inputs": normalized_inputs,
        "binaries_sha256": _binary_hashes(root / "metadata" / "binaries.sha256"),
        "workload": _load_object(root / "metadata" / "workload.json"),
        "settings": stable_settings,
        "cpusets": cpusets,
        "host_uname": environment[1],
    }
    return {
        "fingerprint_sha256": _canonical_sha256(normalized),
        "normalized": normalized,
    }


def _parse_time(run: Path, messages: int) -> dict[str, Any]:
    raw_values: dict[str, str] = {}
    for line in _require_file(run / "replay.time.txt").read_text(
        encoding="utf-8"
    ).splitlines():
        stripped = line.strip()
        if ": " in stripped:
            key, value = stripped.rsplit(": ", 1)
            raw_values[key] = value
    missing = [source for source in TIME_FIELDS.values() if source not in raw_values]
    if missing:
        raise SummaryError(f"GNU-time report is missing fields: {missing!r}")
    recorded = _load_object(run / "replay.time.json")
    parsed: dict[str, Any] = {}
    for target, source in TIME_FIELDS.items():
        raw = raw_values[source]
        if target in {"user_seconds", "system_seconds"}:
            parsed[target] = _decimal(raw, source)
            if parsed[target] != _decimal(recorded.get(target), f"recorded {target}"):
                raise SummaryError(f"GNU-time raw/JSON mismatch for {target}")
        elif target == "elapsed":
            parsed[target] = raw
            if recorded.get(target) != raw:
                raise SummaryError("GNU-time raw/JSON mismatch for elapsed")
        else:
            try:
                parsed[target] = int(raw)
            except ValueError as error:
                raise SummaryError(f"GNU-time field is not integer: {source}") from error
            if recorded.get(target) != parsed[target]:
                raise SummaryError(f"GNU-time raw/JSON mismatch for {target}")
    if parsed["exit_status"] != 0:
        raise SummaryError("GNU time observed a nonzero replay exit")
    elapsed = parse_elapsed_seconds(parsed["elapsed"])
    elapsed_ns = int(elapsed * Decimal(1_000_000_000))
    if Decimal(elapsed_ns) != elapsed * Decimal(1_000_000_000):
        raise SummaryError("GNU-time elapsed precision is finer than a nanosecond")
    cpu_seconds = parsed["user_seconds"] + parsed["system_seconds"]
    with localcontext() as context:
        context.prec = 50
        rate = Decimal(messages) / elapsed
        cpu_percent = cpu_seconds * Decimal(100) / elapsed
    return {
        "elapsed_text": parsed["elapsed"],
        "elapsed_ns": elapsed_ns,
        "elapsed_seconds": format(elapsed, "f"),
        "messages_per_second": _json_number(rate),
        "user_seconds": _json_number(parsed["user_seconds"]),
        "system_seconds": _json_number(parsed["system_seconds"]),
        "time_cpu_seconds": _json_number(cpu_seconds),
        "time_cpu_utilization_percent": _json_number(cpu_percent),
        "time_max_rss_kib": parsed["max_rss_kib"],
        "major_page_faults": parsed["major_page_faults"],
        "minor_page_faults": parsed["minor_page_faults"],
        "voluntary_context_switches": parsed["voluntary_context_switches"],
        "involuntary_context_switches": parsed["involuntary_context_switches"],
        "filesystem_inputs": parsed["filesystem_inputs"],
        "filesystem_outputs": parsed["filesystem_outputs"],
        "cpu_percent_text": str(recorded.get("cpu_percent", "")),
    }


def _parse_rss(run: Path) -> dict[str, Any]:
    with _require_file(run / "rss-samples.tsv").open(
        newline="", encoding="utf-8"
    ) as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if not rows:
        raise SummaryError(f"RSS sample file is empty: {run}")
    expected_columns = {
        "elapsed_ns",
        "recorded_at",
        "process_count",
        "rss_kib",
        "rss_anon_kib",
        "rss_file_kib",
        "vm_swap_kib",
        "max_single_hwm_kib",
        "pids",
    }
    if set(rows[0]) != expected_columns:
        raise SummaryError("RSS sample file has an unexpected schema")
    maxima = {field: 0 for field in RSS_COLUMNS}
    prior_elapsed = -1
    for index, row in enumerate(rows):
        try:
            elapsed = int(row["elapsed_ns"])
            values = {
                summary: int(row[column])
                for summary, column in RSS_COLUMNS.items()
            }
        except ValueError as error:
            raise SummaryError(f"RSS sample {index} has a non-integer value") from error
        if elapsed < 0 or elapsed < prior_elapsed or any(value < 0 for value in values.values()):
            raise SummaryError(f"RSS sample {index} is negative or out of order")
        prior_elapsed = elapsed
        for field, value in values.items():
            maxima[field] = max(maxima[field], value)
    recorded = _load_object(run / "rss-summary.json")
    if recorded.get("samples") != len(rows):
        raise SummaryError("RSS raw/summary sample count mismatch")
    for field, value in maxima.items():
        if recorded.get(field) != value:
            raise SummaryError(f"RSS raw/summary mismatch for {field}")
    if maxima["aggregate_vm_swap_kib"] != 0:
        raise SummaryError("measured process tree used swap")
    return {
        "samples": len(rows),
        "interval_ms": _positive_int(recorded.get("interval_ms"), "RSS interval"),
        **maxima,
    }


def _parse_perf(run: Path, required: bool, elapsed_ns: int) -> dict[str, Any]:
    json_path = run / "perf-stat.json"
    raw_path = run / "perf-stat.tsv"
    if not json_path.exists() and not raw_path.exists() and not required:
        return {"available": False, "coverage_gap": "perf stat disabled"}
    recorded = _load_object(json_path)
    raw_events: list[dict[str, Any]] = []
    for line in _require_file(raw_path).read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) < 3 or not fields[2].strip():
            continue
        raw_value = fields[0].strip()
        raw_events.append(
            {
                "event": fields[2].strip(),
                "raw_value": raw_value,
                "unit": fields[1].strip(),
                "available": re.fullmatch(r"[0-9.]+", raw_value) is not None,
            }
        )
    if not raw_events or recorded.get("events") != raw_events:
        raise SummaryError("perf-stat raw/JSON summary mismatch")
    by_name = {row["event"]: row for row in raw_events}
    required_events = (
        "task-clock",
        "cycles",
        "instructions",
        "cache-misses",
        "context-switches",
        "cpu-migrations",
        "page-faults",
    )
    if required:
        for event in required_events:
            if event not in by_name or by_name[event]["available"] is not True:
                raise SummaryError(f"required perf event is unavailable: {event}")

    def event(name: str) -> Decimal | None:
        row = by_name.get(name)
        if row is None or row["available"] is not True:
            return None
        return _decimal(row["raw_value"], f"perf {name}")

    task_clock = event("task-clock")
    cycles = event("cycles")
    instructions = event("instructions")
    misses = event("cache-misses")
    switches = event("context-switches")
    migrations = event("cpu-migrations")
    faults = event("page-faults")
    with localcontext() as context:
        context.prec = 50
        ipc = instructions / cycles if instructions is not None and cycles else None
        misses_per_million = (
            misses * Decimal(1_000_000) / instructions
            if misses is not None and instructions
            else None
        )
        elapsed_seconds = Decimal(elapsed_ns) / Decimal(1_000_000_000)
        switches_per_second = (
            switches / elapsed_seconds if switches is not None else None
        )
        cpu_utilized = (
            task_clock * Decimal(1_000_000) / Decimal(elapsed_ns)
            if task_clock is not None
            else None
        )
    return {
        "available": True,
        "events": {
            row["event"]: {
                "raw_value": row["raw_value"],
                "unit": row["unit"],
                "available": row["available"],
            }
            for row in sorted(raw_events, key=lambda item: item["event"])
        },
        "task_clock_ms": _json_number(task_clock),
        "cpus_utilized": _json_number(cpu_utilized),
        "cycles": _json_number(cycles),
        "instructions": _json_number(instructions),
        "ipc": _json_number(ipc),
        "cache_misses": _json_number(misses),
        "cache_misses_per_million_instructions": _json_number(misses_per_million),
        "context_switches": _json_number(switches),
        "context_switches_per_second": _json_number(switches_per_second),
        "cpu_migrations": _json_number(migrations),
        "page_faults": _json_number(faults),
    }


def _log_uint(line: str, field: str) -> int:
    match = re.search(rf"(?:^|\s){re.escape(field)}=(\d+)(?:\s|$)", line)
    if match is None:
        raise SummaryError(f"live metric event is missing {field}")
    return int(match.group(1))


def _log_bool(line: str, field: str) -> bool:
    match = re.search(rf"(?:^|\s){re.escape(field)}=(true|false)(?:\s|$)", line)
    if match is None:
        raise SummaryError(f"live metric event is missing {field}")
    return match.group(1) == "true"


def _log_optional_uint(line: str, field: str) -> int | None:
    match = re.search(rf"(?:^|\s){re.escape(field)}=(\d+)(?:\s|$)", line)
    if match is not None:
        return int(match.group(1))
    if re.search(rf"(?:^|\s){re.escape(field)}=", line):
        raise SummaryError(f"live metric event has malformed {field}")
    return None


def _log_optional_bool(line: str, field: str) -> bool | None:
    match = re.search(rf"(?:^|\s){re.escape(field)}=(true|false)(?:\s|$)", line)
    if match is not None:
        return match.group(1) == "true"
    if re.search(rf"(?:^|\s){re.escape(field)}=", line):
        raise SummaryError(f"live metric event has malformed {field}")
    return None


def _log_optional_publication_mode(line: str) -> str | None:
    match = re.search(r'(?:^|\s)mode="(boundary|shutdown)"(?:\s|$)', line)
    if match is not None:
        return match.group(1)
    if re.search(r"(?:^|\s)mode=", line):
        raise SummaryError("live publication event has malformed mode")
    return None


def _parse_live_log(run: Path, expected_messages: int) -> dict[str, Any]:
    text = _require_file(run / "ingester.log").read_text(encoding="utf-8")
    publications: list[dict[str, Any]] = []
    pauses: list[int] = []
    successful_message_boundaries = 0
    failed_message_boundaries = 0
    unclassified_message_boundaries = 0
    timings = {field: [] for field in PUBLICATION_TIMING_FIELDS}
    boundary_timings = {field: [] for field in PUBLICATION_TIMING_FIELDS}
    maxima = {field: 0 for field in PUBLICATION_MAXIMUM_FIELDS}
    for raw_line in text.splitlines():
        line = ANSI_ESCAPE.sub("", raw_line)
        if "chronoxide_live_metrics" not in line:
            continue
        if re.search(r'\bevent="publication"', line) and re.search(
            r'\boutcome="success"', line
        ):
            mode = _log_optional_publication_mode(line)
            publication_timings = {
                field: _log_uint(line, field)
                for field in PUBLICATION_TIMING_FIELDS
            }
            if (
                publication_timings["owner_validation_ns"]
                + publication_timings["head_validation_ns"]
                > publication_timings["owner_and_head_ns"]
            ):
                raise SummaryError(
                    "live publication owner/head substage durations exceed "
                    "their enclosing duration"
                )
            publication_scale = {
                field: _log_uint(line, field)
                for field in PUBLICATION_MAXIMUM_FIELDS
            }
            base_scale = {
                field: _log_optional_uint(line, field)
                for field in (
                    "base_sample_keys",
                    "base_sample_fragments",
                    "base_catalog_active_series",
                )
            }
            present_base_fields = sum(value is not None for value in base_scale.values())
            if present_base_fields not in (0, len(base_scale)):
                raise SummaryError(
                    "live publication event has an incomplete base-scale observation"
                )
            final_empty_fast_path = _log_optional_bool(
                line, "final_empty_fast_path"
            )
            if mode == "boundary" and final_empty_fast_path is True:
                raise SummaryError(
                    "boundary publication cannot report the final empty fast path"
                )
            publications.append(
                {
                    "mode": mode,
                    "generation": _log_uint(line, "generation"),
                    "visible_message_sequence": _log_uint(
                        line, "visible_message_sequence"
                    ),
                    "catalog_revision": _log_uint(line, "catalog_revision"),
                    "manifest_present": _log_bool(line, "manifest_present"),
                    "manifest_validated_offset": publication_scale[
                        "manifest_validated_offset"
                    ],
                    "timings_ns": publication_timings,
                    "scale": publication_scale,
                    "final_empty_fast_path": final_empty_fast_path,
                    "base_scale": (
                        None if present_base_fields == 0 else base_scale
                    ),
                }
            )
            for field in PUBLICATION_TIMING_FIELDS:
                timings[field].append(publication_timings[field])
                if mode == "boundary":
                    boundary_timings[field].append(publication_timings[field])
            for field in PUBLICATION_MAXIMUM_FIELDS:
                maxima[field] = max(maxima[field], publication_scale[field])
        if re.search(r'\bevent="message_boundary"', line):
            outcome = re.search(
                r'(?:^|\s)outcome="(success|failure)"(?:\s|$)', line
            )
            if outcome is None:
                if re.search(r"(?:^|\s)outcome=", line):
                    raise SummaryError(
                        "live message-boundary event has malformed outcome"
                    )
                unclassified_message_boundaries += 1
            elif outcome.group(1) == "success":
                successful_message_boundaries += 1
            else:
                failed_message_boundaries += 1
            pauses.append(_log_uint(line, "ingestion_pause_ns"))
    if not publications or not pauses:
        raise SummaryError("live ingester log lacks publication or pause observations")
    if "Live view publication failed" in text:
        raise SummaryError("live ingester log contains a failed publication")
    modern = [publication["mode"] is not None for publication in publications]
    if any(modern) and not all(modern):
        raise SummaryError("live publication log mixes legacy and mode-tagged events")
    if all(modern) and unclassified_message_boundaries:
        raise SummaryError(
            "mode-tagged live publication log has a message-boundary event "
            "without an outcome"
        )
    by_generation: dict[int, tuple[int, int]] = {}
    manifest: dict[int, tuple[bool, int]] = {}
    for publication in publications:
        generation = publication["generation"]
        sequence = publication["visible_message_sequence"]
        revision = publication["catalog_revision"]
        if generation in by_generation:
            raise SummaryError(f"duplicate successful publication generation {generation}")
        by_generation[generation] = (sequence, revision)
        manifest[generation] = (
            publication["manifest_present"],
            publication["manifest_validated_offset"],
        )
    ordered = sorted(by_generation.items())
    for (previous_generation, previous), (generation, current) in zip(
        ordered, ordered[1:]
    ):
        if (
            generation <= previous_generation
            or current[0] < previous[0]
            or current[1] < previous[1]
        ):
            raise SummaryError("publication generation/message cut regressed")
    if ordered[-1][1][0] != expected_messages:
        raise SummaryError("last publication does not expose the final replay message")
    common = {
        "successful_publications": len(publications),
        "message_boundary_observations": len(pauses),
        "first_generation": ordered[0][0],
        "last_generation": ordered[-1][0],
        "first_visible_message_sequence": ordered[0][1][0],
        "last_visible_message_sequence": ordered[-1][1][0],
        "first_catalog_revision": ordered[0][1][1],
        "last_catalog_revision": ordered[-1][1][1],
        "ingestion_pause_ns": distribution(pauses),
        "publication_timings_ns": {
            field: distribution(values) for field, values in timings.items()
        },
        "publication_maxima": maxima,
        "generation_message_sequence": [
            {
                "generation": generation,
                "visible_message_sequence": cut[0],
                "catalog_revision": cut[1],
                "manifest_present": manifest[generation][0],
                "manifest_validated_offset": manifest[generation][1],
            }
            for generation, cut in ordered
        ],
        "mapping_sha256": _canonical_sha256(ordered),
    }
    if not any(modern):
        result = common
    else:
        shutdowns = [
            publication
            for publication in publications
            if publication["mode"] == "shutdown"
        ]
        if len(shutdowns) != 1:
            raise SummaryError(
                "live publication log must contain exactly one shutdown publication"
            )
        shutdown = shutdowns[0]
        if publications[-1] is not shutdown:
            raise SummaryError("shutdown publication is not the last publication")
        if shutdown["generation"] != ordered[-1][0]:
            raise SummaryError("shutdown publication is not the final generation")
        for field in ("sample_keys", "sample_fragments", "catalog_active_series"):
            if shutdown["scale"][field] != 0:
                raise SummaryError(
                    f"shutdown publication retained non-empty final {field}"
                )
        publication_duration = shutdown["timings_ns"]["publication_duration_ns"]
        seal_duration = shutdown["timings_ns"]["seal_ns"]
        if seal_duration > publication_duration:
            raise SummaryError("shutdown seal duration exceeds publication duration")
        pre_cleanup = sum(
            shutdown["timings_ns"][field]
            for field in ("freeze_and_admission_ns", "seal_ns", "inventory_ns")
        )
        if pre_cleanup > publication_duration:
            raise SummaryError(
                "shutdown freeze, seal, and inventory exceed publication duration"
            )
        shutdown_timings = dict(shutdown["timings_ns"])
        shutdown_timings["post_seal_ns"] = publication_duration - seal_duration
        shutdown_timings["after_inventory_ns"] = publication_duration - pre_cleanup
        result = {
            **common,
            "boundary_publications": len(publications) - 1,
            "successful_message_boundary_observations": (
                successful_message_boundaries
            ),
            "failed_message_boundary_observations": failed_message_boundaries,
            "boundary_publication_timings_ns": {
                field: distribution(values)
                for field, values in boundary_timings.items()
            },
            "shutdown_publication": {
                "generation": shutdown["generation"],
                "visible_message_sequence": shutdown["visible_message_sequence"],
                "catalog_revision": shutdown["catalog_revision"],
                "manifest_present": shutdown["manifest_present"],
                "manifest_validated_offset": shutdown[
                    "manifest_validated_offset"
                ],
                "final_empty_fast_path": shutdown["final_empty_fast_path"],
                "base_scale": shutdown["base_scale"],
                "final_scale": {
                    field: shutdown["scale"][field]
                    for field in (
                        "sample_keys",
                        "sample_fragments",
                        "catalog_active_series",
                    )
                },
                "timings_ns": shutdown_timings,
            },
        }
    if _load_object(run / "live-log-summary.json") != result:
        raise SummaryError("live-log raw/JSON summary mismatch")
    return result


def _sum_fields(
    records: Sequence[dict[str, Any]], field: str, expected: set[str]
) -> dict[str, int]:
    totals = {name: 0 for name in expected}
    for index, record in enumerate(records):
        values = record.get(field)
        if not isinstance(values, dict) or set(values) != expected:
            raise SummaryError(f"client record {index} has malformed {field}")
        for name, value in values.items():
            totals[name] += _nonnegative_int(value, f"{field}.{name}")
    return totals


def _means(totals: dict[str, int], count: int) -> dict[str, int | float]:
    return {
        field: _json_number(Decimal(value) / Decimal(count))
        for field, value in sorted(totals.items())
    }


def _amplification(query_io: dict[str, int]) -> dict[str, Any]:
    used = query_io["chunk_payload_used_bytes"]
    read = query_io["chunk_payload_read_bytes"]
    return {
        "logical_payload_used_bytes": used,
        "coalesced_payload_read_bytes": read,
        "read_used_amplification": (
            None if used == 0 else _json_number(Decimal(read) / Decimal(used))
        ),
        "undefined_reason": (
            "no logical chunk payload bytes were used" if used == 0 else None
        ),
    }


def _client_record_group(records: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {
            "requests": 0,
            "durations": {
                field: distribution([]) for field in (*CLIENT_DURATION_FIELDS, "response_bytes")
            },
            "query_stats_totals": {field: 0 for field in sorted(QUERY_STATS_FIELDS)},
            "query_stats_per_request_mean": None,
            "query_io_totals": {field: 0 for field in sorted(QUERY_IO_FIELDS)},
            "query_io_per_request_mean": None,
            "payload_read_amplification": _amplification(
                {field: 0 for field in QUERY_IO_FIELDS}
            ),
        }
    durations = {
        field: distribution(
            [
                _nonnegative_int(record.get(field), f"client record {field}")
                for record in records
            ]
        )
        for field in (*CLIENT_DURATION_FIELDS, "response_bytes")
    }
    stats = _sum_fields(records, "query_stats", QUERY_STATS_FIELDS)
    query_io = _sum_fields(records, "query_io", QUERY_IO_FIELDS)
    return {
        "requests": len(records),
        "durations": durations,
        "query_stats_totals": dict(sorted(stats.items())),
        "query_stats_per_request_mean": _means(stats, len(records)),
        "query_io_totals": dict(sorted(query_io.items())),
        "query_io_per_request_mean": _means(query_io, len(records)),
        "payload_read_amplification": _amplification(query_io),
    }


def _parse_client(root: Path) -> dict[str, Any]:
    run = root / "runs" / "Q"
    records: list[dict[str, Any]] = []
    for number, line in enumerate(
        _require_file(run / "client-records.jsonl").read_text(
            encoding="utf-8"
        ).splitlines(),
        1,
    ):
        if not line:
            raise SummaryError(f"blank client record at line {number}")
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise SummaryError(f"malformed client record at line {number}") from error
        if not isinstance(value, dict) or value.get("schema") != CLIENT_SCHEMA:
            raise SummaryError(f"client record {number} has an unsupported schema")
        records.append(value)
    if not records:
        raise SummaryError("client record stream is empty")

    workload = _load_object(root / "metadata" / "workload.json")
    queries = workload.get("queries")
    if not isinstance(queries, list) or not queries:
        raise SummaryError("recorded workload has no queries")
    expected_names = {
        str(query.get("name")) for query in queries if isinstance(query, dict)
    }
    if len(expected_names) != len(queries):
        raise SummaryError("recorded workload has duplicate or malformed query names")

    starts: list[int] = []
    completions: list[int] = []
    indexed_by_query: dict[str, list[tuple[int, int, dict[str, Any]]]] = {
        name: [] for name in expected_names
    }
    for index, record in enumerate(records):
        name = record.get("query_name")
        if name not in expected_names:
            raise SummaryError(f"client record {index} names an unknown query")
        for field in (*CLIENT_DURATION_FIELDS, "response_bytes"):
            _nonnegative_int(record.get(field), f"client record {index} {field}")
        started = _positive_int(
            record.get("client_started_monotonic_ns"), f"client record {index} start"
        )
        completed = _positive_int(
            record.get("client_completed_monotonic_ns"),
            f"client record {index} completion",
        )
        if completed < started:
            raise SummaryError(f"client record {index} completes before it starts")
        starts.append(started)
        completions.append(completed)
        indexed_by_query[name].append((started, index, record))
    observation_span = max(completions) - min(starts)
    if observation_span <= 0:
        raise SummaryError("client observation span must be positive")

    overall = _client_record_group(records)
    per_query: dict[str, Any] = {}
    for name in sorted(indexed_by_query):
        ordered = [
            record
            for _started, _index, record in sorted(indexed_by_query[name])
        ]
        if not ordered:
            raise SummaryError(f"client did not execute workload query {name}")
        per_query[name] = {
            **_client_record_group(ordered),
            "first_observation": _client_record_group(ordered[:1]),
            "subsequent_observations": _client_record_group(ordered[1:]),
            "first_vs_subsequent_interpretation": (
                "Observation order only; the parallel closed-loop client does not "
                "establish cold/warm cache state, and the first observation is not "
                "an isolated request."
            ),
        }
    with localcontext() as context:
        context.prec = 50
        achieved_qps = Decimal(len(records)) * Decimal(1_000_000_000) / Decimal(
            observation_span
        )
    result = {
        "successful_requests": len(records),
        "closed_loop_observation_span_ns": observation_span,
        "closed_loop_achieved_requests_per_second": _json_number(achieved_qps),
        "overall": overall,
        "per_query": per_query,
        "records_fingerprint_sha256": _canonical_sha256(records),
    }

    recorded = _load_object(run / "client-summary.json")
    checks = {
        "successful_requests": result["successful_requests"],
        "closed_loop_observation_span_ns": observation_span,
        "records_fingerprint_sha256": result["records_fingerprint_sha256"],
        "durations": {
            field: overall["durations"][field] for field in CLIENT_DURATION_FIELDS
        },
        "query_stats_totals": overall["query_stats_totals"],
        "query_io_totals": overall["query_io_totals"],
    }
    for field, expected in checks.items():
        if recorded.get(field) != expected:
            raise SummaryError(f"client raw/JSON summary mismatch for {field}")
    recorded_qps = recorded.get("closed_loop_achieved_requests_per_second")
    if isinstance(recorded_qps, bool) or not isinstance(recorded_qps, (int, float)):
        raise SummaryError("recorded achieved QPS is not numeric")
    expected_recorded_qps = len(records) * 1_000_000_000 / observation_span
    if recorded_qps != expected_recorded_qps:
        raise SummaryError("client raw/JSON summary mismatch for achieved QPS")
    recorded_latency = recorded.get("per_query_latency")
    if not isinstance(recorded_latency, dict):
        raise SummaryError("client JSON summary lacks per-query latency")
    for name, query in per_query.items():
        expected = {
            field: {
                "count": query["durations"][field]["count"],
                "p50": query["durations"][field]["p50"],
                "p95": query["durations"][field]["p95"],
            }
            for field in ("client_elapsed_ns", "query_duration_ns")
        }
        if recorded_latency.get(name) != expected:
            raise SummaryError(
                f"client raw/JSON summary mismatch for per-query latency {name}"
            )
    return result


def _variant_metric(variant: dict[str, Any], name: str) -> Decimal | None:
    if name in variant["time"]:
        return _decimal(variant["time"][name], name)
    if name == "proc_tree_peak_rss_kib":
        return Decimal(variant["rss"]["aggregate_rss_kib"])
    value = variant["perf"].get(name)
    return None if value is None else _decimal(value, name)


def _delta(
    before: dict[str, Any], after: dict[str, Any]
) -> dict[str, dict[str, int | float | None]]:
    result: dict[str, dict[str, int | float | None]] = {}
    for name in DELTA_METRICS:
        base = _variant_metric(before, name)
        candidate = _variant_metric(after, name)
        if base is None or candidate is None:
            result[name] = {"absolute": None, "percent_vs_base": None}
            continue
        absolute = candidate - base
        percent = None if base == 0 else absolute * Decimal(100) / base
        result[name] = {
            "absolute": _json_number(absolute),
            "percent_vs_base": _json_number(percent),
        }
    return result


def summarize_root(root: Path) -> dict[str, Any]:
    root = root.resolve()
    if not root.is_dir() or root.is_symlink():
        raise SummaryError(f"result root must be a directory: {root}")
    gate, storage_validation, messages = _validate_gates(root)
    artifact_manifest_sha256 = _verify_artifact_manifest(root)
    settings = _read_key_values(root / "metadata" / "settings.txt")
    provenance = _provenance(root, settings, messages)
    order, positions = _parse_run_plan(root / "run-plan.tsv")
    if settings.get("run_order", "").split(",") != order:
        raise SummaryError("settings and run-plan disagree about run order")
    perf_required = gate.get("perf_required")
    if not isinstance(perf_required, bool):
        raise SummaryError("D/P/Q gate lacks a boolean perf_required")

    variants: dict[str, Any] = {}
    for variant in VARIANTS:
        run = root / "runs" / variant
        timing = _parse_time(run, messages)
        rss = _parse_rss(run)
        perf = _parse_perf(run, perf_required, timing["elapsed_ns"])
        variants[variant] = {
            "position": positions[variant],
            "time": timing,
            "rss": rss,
            "perf": perf,
        }
        if variant != "D":
            variants[variant]["live_publication"] = _parse_live_log(run, messages)
    variants["Q"]["client"] = _parse_client(root)
    return {
        "root": str(root),
        "name": root.name,
        "order": order,
        "order_text": ",".join(order),
        "positions": positions,
        "expected_messages": messages,
        "perf_required": perf_required,
        "capture_cache_eviction": settings.get("evict_capture") == "1",
        "run_note": settings.get("run_note", ""),
        "artifact_manifest_sha256": artifact_manifest_sha256,
        "experiment_provenance": provenance,
        "storage_validation": storage_validation,
        "variants": variants,
        "deltas": {
            "D_to_P": _delta(variants["D"], variants["P"]),
            "P_to_Q": _delta(variants["P"], variants["Q"]),
        },
        "correctness": {
            "complete_marker": True,
            "dpq_gate_complete": True,
            "storage_trees_equal": True,
            "replay_counters_equal": True,
            "storage_schema8_validated": True,
            "storage_writer_to_verifier_counts_reconciled": storage_validation[
                "writer_to_verifier_counts_reconciled"
            ],
            "capture_level_physical_sample_golden_gated": storage_validation[
                "capture_level_physical_sample_golden_gated"
            ],
            "independent_readbacks_complete": True,
            "live_head_only_observed": True,
        },
    }


def _aggregate(roots: Sequence[dict[str, Any]]) -> dict[str, Any]:
    position_counts = {
        variant: {str(position): 0 for position in (1, 2, 3)}
        for variant in VARIANTS
    }
    order_counts: dict[str, int] = {}
    for root in roots:
        order_counts[root["order_text"]] = order_counts.get(root["order_text"], 0) + 1
        for variant in VARIANTS:
            position_counts[variant][str(root["positions"][variant])] += 1
    all_position_counts = [
        count
        for variant in VARIANTS
        for count in position_counts[variant].values()
    ]
    counterbalanced = (
        bool(all_position_counts)
        and min(all_position_counts) > 0
        and len(set(all_position_counts)) == 1
    )
    by_variant: dict[str, Any] = {}
    for variant in VARIANTS:
        by_variant[variant] = {}
        for metric in DELTA_METRICS:
            values = [
                value
                for root in roots
                if (value := _variant_metric(root["variants"][variant], metric))
                is not None
            ]
            by_variant[variant][metric] = _numeric_distribution(values)
    delta_distributions: dict[str, Any] = {}
    for comparison in ("D_to_P", "P_to_Q"):
        delta_distributions[comparison] = {}
        for metric in DELTA_METRICS:
            absolute = [
                _decimal(root["deltas"][comparison][metric]["absolute"], metric)
                for root in roots
                if root["deltas"][comparison][metric]["absolute"] is not None
            ]
            percent = [
                _decimal(root["deltas"][comparison][metric]["percent_vs_base"], metric)
                for root in roots
                if root["deltas"][comparison][metric]["percent_vs_base"] is not None
            ]
            delta_distributions[comparison][metric] = {
                "absolute": _numeric_distribution(absolute),
                "percent_vs_base": _numeric_distribution(percent),
            }
    return {
        "root_count": len(roots),
        "order_counts": dict(sorted(order_counts.items())),
        "variant_position_counts": position_counts,
        "position_counterbalanced": counterbalanced,
        "by_variant": by_variant,
        "deltas": delta_distributions,
    }


def summarize(roots: Iterable[Path]) -> dict[str, Any]:
    provided = [path.resolve() for path in roots]
    if not provided:
        raise SummaryError("at least one result root is required")
    if len(set(provided)) != len(provided):
        raise SummaryError("the same result root was provided more than once")
    resolved = sorted(provided, key=str)
    summaries = [summarize_root(root) for root in resolved]
    fingerprints = {
        root["experiment_provenance"]["fingerprint_sha256"] for root in summaries
    }
    if len(fingerprints) != 1:
        raise SummaryError(
            "result roots are not comparable: frozen inputs, binaries, workload, "
            "settings, CPU sets, or host identity differ"
        )
    return {
        "schema": SCHEMA,
        "roots": summaries,
        "aggregate": _aggregate(summaries),
        "interpretation": {
            "variant_D": "Live publication disabled.",
            "variant_P": (
                "Live publication enabled with no HTTP client; includes DEBUG "
                "publication-observer event construction and output."
            ),
            "variant_Q": (
                "Live publication plus a parallel closed-loop HTTP client; achieved "
                "request rate is measured, not prescribed."
            ),
            "closed_loop_limitation": (
                "Latency is subject to coordinated-omission bias and does not establish "
                "open-loop capacity or a saturation limit."
            ),
            "instrumentation_limitation": (
                "P and Q measure instrumented publication. No uninstrumented "
                "publication-overhead claim is made."
            ),
            "first_vs_subsequent_limitation": (
                "Earliest-start-versus-later rows mean request-start order only, not "
                "cold versus warm cache state; requests are issued in parallel pairs. "
                "Cold/warm live-query latency is unmeasured."
            ),
            "position_limitation": (
                "A position-independent conclusion requires every variant to be "
                "observed in every run position."
            ),
            "payload_io_scope": (
                "Payload-read bytes are process-issued coalesced file spans, not "
                "storage-device traffic or operating-system cache misses."
            ),
        },
    }


def _fmt_number(value: Any, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{float(value):,.{digits}f}"


def _ms(value_ns: Any) -> str:
    return "n/a" if value_ns is None else f"{int(value_ns) / 1_000_000:.3f}"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def render_markdown(document: dict[str, Any]) -> str:
    lines = [
        "# Live-query ingestion D/P/Q results",
        "",
        "All listed roots carry `COMPLETE`, passed the D/P/Q equality gate, "
        "Schema 8 footer/exact-postings validation, and independent readbacks. "
        "Raw measurement streams were reconciled against their JSON summaries.",
        "",
        "## Interpretation limits",
        "",
        "- Q is a parallel **closed-loop** workload. Its achieved QPS is measured, "
        "not prescribed; latency is subject to coordinated omission and is not an "
        "open-loop capacity result.",
        "- P and Q include DEBUG publication-observer construction and output. This "
        "is instrumented publication cost, not an uninstrumented lower bound.",
        "- “Earliest-start” and “later” mean request-start order only. They do not "
        "prove cold/warm cache state because requests run in parallel pairs; "
        "cold/warm live-query latency is unmeasured.",
        "- Payload-read bytes are process-issued coalesced spans, not storage-device "
        "traffic or OS cache misses.",
        "- Recorded head writes precede equal-timestamp last-write-wins compaction, "
        "so they may exceed physical rows. The per-root storage section states "
        "whether writer-to-verifier counts and a capture-level golden were gated.",
        "",
        "## Run order and position coverage",
        "",
        "| Result root | Order | D position | P position | Q position | Capture eviction |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for root in document["roots"]:
        lines.append(
            f"| `{_escape(root['name'])}` | {root['order_text']} | "
            f"{root['positions']['D']} | {root['positions']['P']} | "
            f"{root['positions']['Q']} | "
            f"{'yes' if root['capture_cache_eviction'] else 'no'} |"
        )
    aggregate = document["aggregate"]
    lines.extend(
        [
            "",
            (
                f"Position counterbalanced: **"
                f"{'yes' if aggregate['position_counterbalanced'] else 'no'}**. "
                "A position-independent conclusion requires D, P, and Q in every "
                "position."
            ),
        ]
    )

    for root in document["roots"]:
        storage = root["storage_validation"]
        collapsed_percent = (
            Decimal(storage["recorded_writes_minus_physical_rows"])
            * Decimal(100)
            / Decimal(storage["recorded_head_writes"])
        )
        lines.extend(
            [
                "",
                f"## `{_escape(root['name'])}`",
                "",
                f"Order: **{root['order_text']}**; messages per arm: "
                f"**{root['expected_messages']:,}**.",
                "",
                "### Storage row reconciliation",
                "",
                f"Recorded head writes: **{storage['recorded_head_writes']:,}**; "
                f"physical rows: **{storage['samples']:,}**; LWW-collapsed delta: "
                f"**{storage['recorded_writes_minus_physical_rows']:,} "
                f"({_fmt_number(collapsed_percent)}%)**.",
                "",
                f"Writer-to-verifier count reconciliation: **"
                f"{'yes' if storage['writer_to_verifier_counts_reconciled'] else 'no'}**; "
                f"capture-level physical-row golden: **"
                f"{'yes' if storage['capture_level_physical_sample_golden_gated'] else 'no'}**; "
                f"gate revision: `{storage['gate_revision']}`.",
                "",
                "### Replay performance",
                "",
                "| Arm | Pos | Elapsed (s) | msg/s | CPU s | CPU/wall | "
                "Peak tree RSS KiB | Instructions | IPC | Cache miss/M insn | Ctx/s |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for variant in VARIANTS:
            row = root["variants"][variant]
            timing, rss, perf = row["time"], row["rss"], row["perf"]
            lines.append(
                f"| {variant} | {row['position']} | {timing['elapsed_seconds']} | "
                f"{_fmt_number(timing['messages_per_second'])} | "
                f"{_fmt_number(timing['time_cpu_seconds'])} | "
                f"{_fmt_number(timing['time_cpu_utilization_percent'])}% | "
                f"{rss['aggregate_rss_kib']:,} | "
                f"{_fmt_number(perf.get('instructions'), 0)} | "
                f"{_fmt_number(perf.get('ipc'), 3)} | "
                f"{_fmt_number(perf.get('cache_misses_per_million_instructions'))} | "
                f"{_fmt_number(perf.get('context_switches_per_second'))} |"
            )
        lines.extend(
            [
                "",
                "### Arm deltas",
                "",
                "| Comparison | Metric | Absolute | % vs base |",
                "|---|---|---:|---:|",
            ]
        )
        for comparison, label in (("D_to_P", "D → P"), ("P_to_Q", "P → Q")):
            for metric in (
                "elapsed_seconds",
                "messages_per_second",
                "time_cpu_seconds",
                "proc_tree_peak_rss_kib",
                "instructions",
                "cache_misses",
                "context_switches",
            ):
                value = root["deltas"][comparison][metric]
                lines.append(
                    f"| {label} | `{metric}` | "
                    f"{_fmt_number(value['absolute'])} | "
                    f"{_fmt_number(value['percent_vs_base'])}% |"
                )
        for variant in ("P", "Q"):
            live = root["variants"][variant]["live_publication"]
            pause = live["ingestion_pause_ns"]
            boundary_count = live.get("boundary_publications")
            timing_source = live.get(
                "boundary_publication_timings_ns",
                live["publication_timings_ns"],
            )
            lines.extend(
                [
                    "",
                    f"### {variant} publication and ingestion pauses",
                    "",
                    (
                        f"Successful publications: "
                        f"**{live['successful_publications']:,}**; "
                        + (
                            f"ordinary boundary publications: "
                            f"**{boundary_count:,}**; "
                            if isinstance(boundary_count, int)
                            else ""
                        )
                        + "message-boundary observations: "
                        + f"**{live['message_boundary_observations']:,}**."
                    ),
                    "",
                    "| Timing | p50 ms | p95 ms | p99 ms | max ms |",
                    "|---|---:|---:|---:|---:|",
                    f"| ingestion pause | {_ms(pause['p50'])} | "
                    f"{_ms(pause['p95'])} | {_ms(pause['p99'])} | "
                    f"{_ms(pause['max'])} |",
                ]
            )
            for field in PUBLICATION_TIMING_FIELDS:
                values = timing_source[field]
                lines.append(
                    f"| `{field}` | {_ms(values['p50'])} | "
                    f"{_ms(values['p95'])} | {_ms(values['p99'])} | "
                    f"{_ms(values['max'])} |"
                )
            shutdown = live.get("shutdown_publication")
            if isinstance(shutdown, dict):
                shutdown_timings = shutdown["timings_ns"]
                lines.extend(
                    [
                        "",
                        "Final shutdown publication:",
                        "",
                        "| Property | Value |",
                        "|---|---:|",
                        f"| exact empty-head fast path | "
                        f"**{shutdown['final_empty_fast_path']}** |",
                        f"| total | {_ms(shutdown_timings['publication_duration_ns'])} ms |",
                        f"| seal | {_ms(shutdown_timings['seal_ns'])} ms |",
                        f"| non-seal | {_ms(shutdown_timings['post_seal_ns'])} ms |",
                        f"| sample root | {_ms(shutdown_timings['sample_root_ns'])} ms |",
                        f"| catalog | {_ms(shutdown_timings['catalog_ns'])} ms |",
                        f"| post-commit | {_ms(shutdown_timings['post_commit_ns'])} ms |",
                    ]
                )
                base_scale = shutdown.get("base_scale")
                if isinstance(base_scale, dict):
                    lines.extend(
                        [
                            "",
                            "Pre-shutdown live root: "
                            f"**{base_scale['base_sample_keys']:,} sample keys**, "
                            f"**{base_scale['base_sample_fragments']:,} fragments**, "
                            f"and **{base_scale['base_catalog_active_series']:,} "
                            "active series**.",
                        ]
                    )
            maxima = live["publication_maxima"]
            lines.extend(
                [
                    "",
                    "Memory maxima: "
                    f"charged **{maxima['live_memory_charged_bytes']:,} B**, "
                    f"peak charged **{maxima['live_memory_peak_charged_bytes']:,} B**, "
                    f"mutable-tail used/capacity "
                    f"**{maxima['live_mutable_tail_used_bytes']:,}/"
                    f"{maxima['live_mutable_tail_capacity_bytes']:,} B**, "
                    f"pending estimated **{maxima['pending_estimated_bytes']:,} B**.",
                ]
            )
        client = root["variants"]["Q"]["client"]
        overall = client["overall"]
        lines.extend(
            [
                "",
                "### Q closed-loop client",
                "",
                f"Requests: **{client['successful_requests']:,}**; observation span: "
                f"**{client['closed_loop_observation_span_ns'] / 1e9:.3f} s**; "
                f"achieved rate: "
                f"**{_fmt_number(client['closed_loop_achieved_requests_per_second'])} "
                "requests/s**.",
                "",
                "| Timing | p50 | p95 | p99 | max |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for field in CLIENT_DURATION_FIELDS:
            values = overall["durations"][field]
            unit = "ms" if field == "view_age_ms" else "ns"
            if unit == "ns":
                cells = [_ms(values[key]) for key in ("p50", "p95", "p99", "max")]
                label = f"`{field}` (ms)"
            else:
                cells = [_fmt_number(values[key]) for key in ("p50", "p95", "p99", "max")]
                label = f"`{field}`"
            lines.append(f"| {label} | {' | '.join(cells)} |")
        lines.extend(
            [
                "",
                "### Q per-query latency and I/O",
                "",
                "| Query | n | Client p50/p95 ms | Query p50/p95 ms | "
                "Earliest-start/later client p50 ms | Used/read bytes | Read/used | "
                "Physical reads | Matched series | Chunk reads |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for name, query in client["per_query"].items():
            durations = query["durations"]
            first = query["first_observation"]["durations"]["client_elapsed_ns"]
            later = query["subsequent_observations"]["durations"]["client_elapsed_ns"]
            io = query["query_io_totals"]
            amplification = query["payload_read_amplification"]
            stats = query["query_stats_totals"]
            lines.append(
                f"| `{_escape(name)}` | {query['requests']} | "
                f"{_ms(durations['client_elapsed_ns']['p50'])}/"
                f"{_ms(durations['client_elapsed_ns']['p95'])} | "
                f"{_ms(durations['query_duration_ns']['p50'])}/"
                f"{_ms(durations['query_duration_ns']['p95'])} | "
                f"{_ms(first['p50'])}/{_ms(later['p50'])} | "
                f"{io['chunk_payload_used_bytes']:,}/"
                f"{io['chunk_payload_read_bytes']:,} | "
                f"{_fmt_number(amplification['read_used_amplification'], 3)} | "
                f"{io['chunk_payload_physical_reads']:,} | "
                f"{stats['matched_series']:,} | {stats['chunk_reads']:,} |"
            )
    lines.append("")
    return "\n".join(lines)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _write_exclusive(path: Path, content: str) -> None:
    if not path.parent.is_dir():
        raise SummaryError(f"output parent does not exist: {path.parent}")
    try:
        with path.open("x", encoding="utf-8") as destination:
            destination.write(content)
    except FileExistsError as error:
        raise SummaryError(f"refusing to overwrite output: {path}") from error


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--root",
        type=Path,
        action="append",
        required=True,
        help="COMPLETE D/P/Q result root; repeat for position counterbalancing",
    )
    result.add_argument("--json-output", type=Path, required=True)
    result.add_argument("--markdown-output", type=Path, required=True)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        roots = [root.resolve() for root in args.root]
        outputs = [args.json_output.resolve(), args.markdown_output.resolve()]
        if outputs[0] == outputs[1]:
            raise SummaryError("JSON and Markdown outputs must be different paths")
        for output in outputs:
            if any(_inside(output, root) for root in roots):
                raise SummaryError("outputs must not be written inside a result root")
            if output.exists():
                raise SummaryError(f"refusing to overwrite output: {output}")
        document = summarize(roots)
        json_text = json.dumps(
            document, ensure_ascii=False, indent=2, sort_keys=True
        ) + "\n"
        markdown = render_markdown(document)
        _write_exclusive(outputs[0], json_text)
        _write_exclusive(outputs[1], markdown)
    except (OSError, SummaryError) as error:
        print(f"live-query results: {error}", file=sys.stderr)
        return 2
    print(json.dumps({"json": str(outputs[0]), "markdown": str(outputs[1])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
