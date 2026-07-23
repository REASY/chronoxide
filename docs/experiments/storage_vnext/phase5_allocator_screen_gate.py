#!/usr/bin/env python3
"""Strict helpers for the Phase 5 bounded allocator screen."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import types
from pathlib import Path
from typing import Any


def load_exact_source_sibling(name: str, filename: str) -> types.ModuleType:
    parent = Path(__file__).resolve(strict=True).parent
    path = parent / filename
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"required Python sibling is not an exact source file: {path}")
    source = path.read_bytes()
    module = types.ModuleType(name)
    module.__file__ = str(path)
    module.__package__ = None
    module.__cached__ = None
    exec(compile(source, str(path), "exec", dont_inherit=True), module.__dict__)
    return module


phase1 = load_exact_source_sibling(
    "_chronoxide_phase1_replay_gate_sealed", "phase1_replay_gate.py"
)
report_gate = load_exact_source_sibling(
    "_chronoxide_ab_gate_sealed", "ab_gate.py"
)


class GateError(ValueError):
    pass


PLAN_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-screen-plan/v5"
PREFLIGHT_SCHEMA = "chronoxide/allocator-preflight/v3"
RUNTIME_POLICY_SCHEMA = "chronoxide/allocator-runtime-policy/v1"
PREFLIGHT_RECORD_SCHEMA = (
    "chronoxide/storage-vnext-phase5-allocator-preflight-record/v3"
)
SOURCE_SEAL_SCHEMA = "chronoxide/storage-vnext-phase5-source-seal/v1"
EXTRACTED_SOURCE_SEAL_SCHEMA = (
    "chronoxide/storage-vnext-phase5-extracted-source-seal/v1"
)
BUILD_PROVENANCE_SCHEMA = "chronoxide/storage-vnext-phase5-build-provenance/v4"
CONTROL_SEAL_SCHEMA = "chronoxide/storage-vnext-phase5-control-seal/v1"
CHECKPOINT_SCHEMA = "chronoxide/allocator-release-checkpoint/v1"
TELEMETRY_SCHEMA = "chronoxide/allocator-release-telemetry/v1"
TELEMETRY_SUMMARY_SCHEMA = (
    "chronoxide/storage-vnext-phase5-allocator-telemetry-summary/v1"
)
GUARDIAN_SCHEMA = "chronoxide/storage-vnext-phase5-external-conflict-guardian/v5"
GUARDIAN_CONTROL_SCHEMA = (
    "chronoxide/storage-vnext-phase5-external-conflict-guardian-control/v2"
)
GUARDIAN_ROOT_CONTROL_SCHEMA = (
    "chronoxide/storage-vnext-phase5-external-conflict-root-control/v1"
)
GUARDIAN_CLEANUP_SCHEMA = (
    "chronoxide/storage-vnext-phase5-external-conflict-guardian-cleanup/v1"
)
GUARDIAN_CADENCE_EDGE_ALLOWANCE_NS = 100_000_000
IMMUTABLE_TREE_SEAL_SCHEMA = (
    "chronoxide/storage-vnext-phase5-immutable-evidence-tree/v1"
)
FINAL_INVENTORY_SCHEMA = "chronoxide/storage-vnext-phase5-final-inventory/v1"
FINAL_VALIDATION_SCHEMA = "chronoxide/storage-vnext-phase5-final-validation/v1"
FINAL_INVENTORY_AUTHORITY_FILES = {
    "metadata/result-artifacts.nul",
    "metadata/result-directories.nul",
    "metadata/result-artifacts.sha256",
    "metadata/FINAL_SEAL_VALIDATED.json",
}
PROFILE_INVENTORY_AUTHORITY_FILES = {
    "metadata/artifacts.nul",
    "metadata/directories.nul",
    "metadata/artifacts.sha256",
    "metadata/FINAL_SEAL_VALIDATED.json",
}
OBSERVATION_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-observation/v3"
SUMMARY_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-screen-summary/v3"
CALIBRATION_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-calibration/v1"
VALIDATION_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-validation/v3"
FINAL_DECISION_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-final-decision/v3"
PROFILE_EVIDENCE_SCHEMA = "chronoxide/storage-vnext-phase5-allocator-profile/v3"
PROFILE_CAPACITY_CONTROL_SCHEMA = (
    "chronoxide/storage-vnext-phase5-profile-capacity-control/v1"
)
POLICY_ORDER = ["S", "J0", "J1", "J2", "J3"]
EXPECTED_SCHEDULE = ["S", "J0", "J1", "J2", "J3", "J3", "J2", "J1", "J0", "S"]
CAPACITY_RESERVE_BYTES = 8 * 1024 * 1024 * 1024
EXPECTED_CONFS = {
    "S": None,
    "J0": None,
    "J1": "abort_conf:true,confirm_conf:true,narenas:4",
    "J2": (
        "abort_conf:true,confirm_conf:true,narenas:4,dirty_decay_ms:1000,"
        "muzzy_decay_ms:0,background_thread:true,max_background_threads:1"
    ),
    "J3": (
        "abort_conf:true,confirm_conf:true,narenas:2,dirty_decay_ms:1000,"
        "muzzy_decay_ms:0,background_thread:true,max_background_threads:1"
    ),
}
EXPECTED_PERF_EVENTS = [
    "task-clock",
    "cycles",
    "instructions",
    "branches",
    "branch-misses",
    "cache-references",
    "cache-misses",
    "page-faults",
    "minor-faults",
    "major-faults",
    "context-switches",
    "cpu-migrations",
]
RSS_SAMPLE_COLUMNS = [
    "elapsed_ns",
    "sample_window_start_unix_time_ns",
    "unix_time_ns",
    "phase",
    "process_count",
    "process_cpu_ticks",
    "rss_kib",
    "rss_anon_kib",
    "rss_file_kib",
    "vm_swap_kib",
    "max_single_hwm_kib",
    "pids",
]
ALLOCATOR_RSS_RELATION = (
    "non-equivalent: jemalloc stats.resident covers allocator-resident pages in the "
    "ingester; external RSS covers the sampled launcher process tree"
)
PREFLIGHT_APPLICATION_KEYS = {
    "schema",
    "rust_global_allocator",
    "jemalloc_conf_env",
    "requested_policy_raw",
    "requested_policy_canonical",
    "effective_policy",
    "global_allocator_probe",
    "allocator_internal_telemetry",
    "ld_preload_present",
    "malloc_conf_present",
    "post_ingester_drop_hold_secs",
    "post_ingester_drop_checkpoint_enabled",
    "post_ingester_drop_telemetry_enabled",
}
PREFLIGHT_RECORD_KEYS = {
    "schema",
    "policy",
    "binary_role",
    "binary_sha256",
    "application",
    "jemalloc_confirm_conf_verified",
    "jemalloc_config_sources_verified",
    "jemalloc_config_source_audit_sha256",
}
EFFECTIVE_POLICY_KEYS = {
    "abort_conf",
    "confirm_conf",
    "narenas",
    "dirty_decay_ms",
    "muzzy_decay_ms",
    "background_thread",
    "max_background_threads",
    "retain",
}
PROMQL_READBACK_HEADER = [
    "Kind",
    "Query",
    "result_series",
    "result_samples",
    "matched_series",
    "projected_series",
    "chunk_reads",
    "bytes_read",
    "samples_decoded",
    "typed_scalar_chunks_decoded",
    "typed_full_chunks_decoded",
]
STORAGE_REPORT_FIELDS = {
    "schema_version",
    "footer_validation_enabled",
    "series_sample_per_segment",
    "verified_selection_fingerprint",
    "decoded_semantic_fingerprint",
    "segments",
    "corpus_series",
    "series",
    "chunks",
    "chunks_by_kind",
    "samples",
    "logical_chunk_bytes",
    "chunk_inventory",
    "exact_postings",
    "elapsed_ns",
    "metadata_read_calls",
    "metadata_read_bytes",
    "metadata_peak_retained_bytes",
    "metadata_peak_in_flight_bytes",
    "metadata_peak_open_files",
    "metadata_cache_hits",
    "metadata_cache_misses",
}
STORAGE_INTEGER_FIELDS = {
    "segments",
    "corpus_series",
    "series",
    "chunks",
    "samples",
    "logical_chunk_bytes",
    "elapsed_ns",
    "metadata_read_calls",
    "metadata_read_bytes",
    "metadata_peak_retained_bytes",
    "metadata_peak_in_flight_bytes",
    "metadata_peak_open_files",
    "metadata_cache_hits",
    "metadata_cache_misses",
}
EXACT_POSTINGS_FIELDS = {
    "logical_fingerprint",
    "lists",
    "decoded_refs",
    "encoded_bytes",
}
KIND_ORDER = ("float", "int64", "histogram", "exponential_histogram", "summary")
EXPECTED_FLOAT_ENCODINGS = {"gorilla"}
VALID_KIND_ENCODING_LAYOUTS = {
    ("float", "raw_f64"): "t0_interleaved_dt_value",
    ("float", "gorilla"): "t0_dt_then_values",
    ("int64", "raw_i64"): "t0_interleaved_dt_value",
    ("int64", "int_delta_zigzag"): "t0_dt_then_values",
    ("histogram", "schema_varlen"): "typed_scalar_lane_and_t0_dt_schema_varlen",
    (
        "exponential_histogram",
        "schema_varlen",
    ): "typed_scalar_lane_and_t0_dt_schema_varlen",
    ("summary", "schema_varlen"): "typed_scalar_lane_and_t0_dt_schema_varlen",
}
CHUNK_INVENTORY_ROW_FIELDS = {
    "kind",
    "encoding",
    "payload_layout",
    "chunks",
    "points",
    "indexed_bytes",
    "common_header_bytes",
    "scalar_lane_bytes",
    "payload_bytes",
    "timestamp_base_bytes",
    "timestamp_delta_bytes",
    "value_bytes",
    "point_count_histogram",
    "cadence_ms_histogram",
}
FLOAT_EVIDENCE_FIELDS = {
    "tie_rule",
    "chunks",
    "points",
    "existing_indexed_bytes",
    "existing_payload_bytes",
    "raw_f64_candidate_indexed_bytes",
    "raw_f64_candidate_payload_bytes",
    "gorilla_candidate_indexed_bytes",
    "gorilla_candidate_payload_bytes",
    "adaptive_min_indexed_bytes",
    "adaptive_min_payload_bytes",
    "raw_f64_wins",
    "gorilla_wins",
    "ties",
    "adaptive_raw_f64_selections",
    "adaptive_gorilla_selections",
    "repeated_xor_points",
    "reused_window_points",
    "new_window_points",
    "xor_significant_bits_histogram",
    "positive_zero_points",
    "negative_zero_points",
    "finite_nonzero_points",
    "positive_infinity_points",
    "negative_infinity_points",
    "ordinary_nan_points",
    "stale_nan_points",
}
TIMESTAMP_SHAPES = {
    "single_point",
    "constant_zero_step",
    "constant_positive_step",
    "variable_step",
}
TIMESTAMP_CANDIDATES = (
    "current_offset_uleb",
    "adjacent_delta_uleb",
    "delta_of_delta_zigzag_uleb128",
    "fixed_step_residual_bitpack",
)


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def write_json_exclusive(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as destination:
        json.dump(value, destination, indent=2, sort_keys=True)
        destination.write("\n")


def regular_non_symlink(path: Path, description: str) -> Path:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as error:
        raise GateError(f"{description} is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise GateError(f"{description} must be a regular non-symlink file: {path}")
    return path


def directory_non_symlink(path: Path, description: str) -> Path:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as error:
        raise GateError(f"{description} is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise GateError(f"{description} must be a non-symlink directory: {path}")
    return path


def fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_json_read_only_atomic_exclusive(path: Path, value: Any) -> None:
    parent = directory_non_symlink(path.parent, "atomic JSON parent")
    if path.exists() or path.is_symlink():
        raise GateError(f"refusing to reuse atomic JSON output: {path}")
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.tmp.", dir=parent
    )
    temporary = Path(temporary_name)
    descriptor_open = True
    try:
        with os.fdopen(descriptor, "wb") as destination:
            descriptor_open = False
            destination.write(payload)
            destination.flush()
            os.fchmod(destination.fileno(), 0o444)
            os.fsync(destination.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise GateError(f"refusing to reuse atomic JSON output: {path}") from error
        fsync_directory(parent)
    finally:
        if descriptor_open:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        else:
            fsync_directory(parent)
    published = regular_non_symlink(path, "atomic JSON output")
    if (
        stat.S_IMODE(published.stat().st_mode) != 0o444
        or published.read_bytes() != payload
    ):
        raise GateError("atomic JSON output differs from its finalized payload")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(16 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def validate_zero_exit_status(path: Path, description: str) -> None:
    status = regular_non_symlink(path, description)
    if status.read_bytes() != b"0\n":
        raise GateError(f"{description} must be exact 0\\n")


def control_seal(input_paths: list[Path]) -> dict[str, Any]:
    if not input_paths:
        raise GateError("control seal requires at least one input")
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for supplied in input_paths:
        if not supplied.is_absolute() or supplied.is_symlink() or not supplied.is_file():
            raise GateError(
                f"control input must be an absolute regular non-symlink file: {supplied}"
            )
        path = supplied.resolve(strict=True)
        path_text = str(path)
        if path_text in seen:
            raise GateError(f"control seal contains a duplicate input: {path}")
        seen.add(path_text)
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode not in {0o444, 0o555}:
            raise GateError(
                f"control input must have exact mode 0444 or 0555: {path} is {mode:04o}"
            )
        entries.append(
            {
                "path": path_text,
                "mode": f"{mode:04o}",
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    entries.sort(key=lambda entry: os.fsencode(entry["path"]))
    identity_sha256 = hashlib.sha256(
        json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "schema": CONTROL_SEAL_SCHEMA,
        "input_count": len(entries),
        "inputs": entries,
        "identity_sha256": identity_sha256,
    }


def validate_control_seal_document(value: Any) -> dict[str, Any]:
    seal = require_exact_keys(
        value,
        {"schema", "input_count", "inputs", "identity_sha256"},
        "$.control_seal",
    )
    if seal["schema"] != CONTROL_SEAL_SCHEMA:
        raise GateError("control seal schema mismatch")
    count = strict_int(seal["input_count"], "$.control_seal.input_count", minimum=1)
    inputs = seal["inputs"]
    if not isinstance(inputs, list) or len(inputs) != count:
        raise GateError("control seal input count differs from its inventory")
    previous: bytes | None = None
    seen: set[str] = set()
    for index, value in enumerate(inputs):
        entry = require_exact_keys(
            value,
            {"path", "mode", "size_bytes", "sha256"},
            f"$.control_seal.inputs[{index}]",
        )
        path = entry["path"]
        if not isinstance(path, str) or not Path(path).is_absolute() or path in seen:
            raise GateError("control seal contains a non-absolute or duplicate path")
        encoded = os.fsencode(path)
        if previous is not None and encoded <= previous:
            raise GateError("control seal paths are not in strict byte order")
        previous = encoded
        seen.add(path)
        if entry["mode"] not in {"0444", "0555"}:
            raise GateError("control seal contains a writable or invalid mode")
        strict_int(
            entry["size_bytes"],
            f"$.control_seal.inputs[{index}].size_bytes",
            minimum=0,
        )
        if not isinstance(entry["sha256"], str) or re.fullmatch(
            r"[0-9a-f]{64}", entry["sha256"]
        ) is None:
            raise GateError("control seal contains an invalid SHA-256")
    identity = hashlib.sha256(
        json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if seal["identity_sha256"] != identity:
        raise GateError("control seal identity digest is inconsistent")
    return seal


def check_control_seal(seal_path: Path) -> dict[str, Any]:
    if not seal_path.is_absolute() or seal_path.is_symlink() or not seal_path.is_file():
        raise GateError("control seal authority must be an absolute regular non-symlink file")
    if stat.S_IMODE(seal_path.stat().st_mode) != 0o444:
        raise GateError("control seal authority must have exact mode 0444")
    expected = validate_control_seal_document(load_json(seal_path))
    current = validate_control_seal_document(
        control_seal([Path(entry["path"]) for entry in expected["inputs"]])
    )
    if current != expected:
        differing = sorted(key for key in expected if expected[key] != current[key])
        raise GateError(f"fixed control seal changed: {differing!r}")
    return {
        "status": "pass",
        "control_seal_sha256": sha256_file(seal_path),
        "identity_sha256": current["identity_sha256"],
        "input_count": current["input_count"],
    }


def check_profile_control_seal(
    seal_path: Path, expected_inputs: set[Path]
) -> dict[str, Any]:
    checked = check_control_seal(seal_path)
    document = validate_control_seal_document(load_json(seal_path))
    observed = {entry["path"] for entry in document["inputs"]}
    expected = {str(path.resolve(strict=True)) for path in expected_inputs}
    if observed != expected:
        missing = sorted(expected - observed, key=os.fsencode)
        extra = sorted(observed - expected, key=os.fsencode)
        raise GateError(
            "profile control seal inputs differ; "
            f"missing={missing!r}, extra={extra!r}"
        )
    return checked


def check_rendered_config(
    record_path: Path,
    config_path: Path,
    capture_path: Path,
    segments_dir: Path,
    stop_after_messages: int,
) -> dict[str, Any]:
    for path, label in ((record_path, "render record"), (config_path, "rendered config")):
        if not path.is_absolute() or path.is_symlink() or not path.is_file():
            raise GateError(f"{label} must be an absolute regular non-symlink file")
        if stat.S_IMODE(path.stat().st_mode) != 0o444:
            raise GateError(f"{label} must have exact mode 0444")
    if not capture_path.is_absolute() or capture_path.is_symlink() or not capture_path.is_dir():
        raise GateError("rendered config capture must be an absolute non-symlink directory")
    if not segments_dir.is_absolute() or segments_dir.exists() or segments_dir.is_symlink():
        raise GateError("rendered config segment destination must be a fresh absolute path")
    expected_stop = strict_int(stop_after_messages, "stop_after_messages", minimum=1)
    record = require_exact_keys(
        load_json(record_path),
        {"config", "sha256", "segments_dir", "stop_after_messages"},
        "$.rendered_config",
    )
    expected_config = str(config_path.resolve(strict=True))
    if record != {
        "config": expected_config,
        "sha256": sha256_file(config_path),
        "segments_dir": str(segments_dir),
        "stop_after_messages": expected_stop,
    }:
        raise GateError("rendered config record differs from its frozen path/hash/parameters")
    try:
        document = tomllib.loads(config_path.read_text(encoding="utf-8", errors="strict"))
        ingestion = document["ingestion"]
        writer = ingestion["segment_writer"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise GateError("rendered config is malformed") from error
    if (
        ingestion.get("replay_from") != str(capture_path.resolve(strict=True))
        or ingestion.get("stop_after_messages") != expected_stop
        or writer.get("segments_dir") != str(segments_dir)
    ):
        raise GateError("rendered config body differs from its frozen replay parameters")
    return {
        "status": "pass",
        "config_sha256": record["sha256"],
        "record_sha256": sha256_file(record_path),
    }


def usable_profile_frame(frame: str) -> bool:
    normalized = frame.strip()
    if not normalized or normalized.lower() in {
        "??",
        "[unknown]",
        "unknown",
        "<unknown>",
    }:
        return False
    return re.search(r"[A-Za-z_]", normalized) is not None


def parse_heaptrack_stack_evidence(analysis: str) -> dict[str, Any]:
    stack_count = 0
    multi_frame_stack_count = 0
    chronoxide_stack_count = 0
    maximum_frame_depth = 0
    attributed_allocations = 0
    for line_number, raw_line in enumerate(analysis.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        match = re.fullmatch(r"(.+?)\s+([0-9][0-9,]*)", line)
        if match is None:
            raise GateError(
                f"heaptrack collapsed-stack line {line_number} is malformed"
            )
        count = int(match.group(2).replace(",", ""))
        if count <= 0:
            raise GateError("heaptrack collapsed stack has a non-positive allocation count")
        frames = [frame.strip() for frame in match.group(1).split(";")]
        if any(not frame for frame in frames):
            raise GateError("heaptrack collapsed stack contains an empty frame")
        usable = [frame for frame in frames if usable_profile_frame(frame)]
        stack_count += 1
        attributed_allocations += count
        maximum_frame_depth = max(maximum_frame_depth, len(usable))
        if len(usable) >= 2:
            multi_frame_stack_count += 1
            if any(re.search(r"(?i)chronoxide(?:[-_:]|$)", frame) for frame in usable):
                chronoxide_stack_count += 1
    if multi_frame_stack_count == 0:
        raise GateError("heaptrack profile lacks a usable multi-frame allocation stack")
    if chronoxide_stack_count == 0:
        raise GateError("heaptrack profile lacks a multi-frame Chronoxide allocation stack")
    return {
        "format": "heaptrack-collapsed-stacks/v1",
        "stack_count": stack_count,
        "multi_frame_stack_count": multi_frame_stack_count,
        "chronoxide_stack_count": chronoxide_stack_count,
        "maximum_frame_depth": maximum_frame_depth,
        "attributed_events": attributed_allocations,
    }


def parse_perf_script_stack_evidence(analysis: str) -> dict[str, Any]:
    frame_pattern = re.compile(
        r"^\s+([0-9a-fA-F]+)\s+(.+?)\s+\(([^()]*)\)\s*$"
    )
    stack_count = 0
    multi_frame_stack_count = 0
    chronoxide_stack_count = 0
    maximum_frame_depth = 0
    for block in re.split(r"\n\s*\n", analysis.strip()):
        if not block.strip():
            continue
        frames: list[tuple[str, str]] = []
        for line in block.splitlines():
            match = frame_pattern.fullmatch(line)
            if match is None:
                continue
            symbol = match.group(2).strip()
            dso = match.group(3).strip()
            if usable_profile_frame(symbol):
                frames.append((symbol, dso))
        if not frames:
            continue
        stack_count += 1
        maximum_frame_depth = max(maximum_frame_depth, len(frames))
        if len(frames) >= 2:
            multi_frame_stack_count += 1
            if any(
                re.search(r"(?i)chronoxide(?:[-_:]|$)", f"{symbol} {dso}")
                for symbol, dso in frames
            ):
                chronoxide_stack_count += 1
    if multi_frame_stack_count == 0:
        raise GateError("perf profile lacks a usable multi-frame callchain")
    if chronoxide_stack_count == 0:
        raise GateError("perf profile lacks a multi-frame Chronoxide callchain")
    return {
        "format": "perf-script-callchains/v1",
        "stack_count": stack_count,
        "multi_frame_stack_count": multi_frame_stack_count,
        "chronoxide_stack_count": chronoxide_stack_count,
        "maximum_frame_depth": maximum_frame_depth,
        "attributed_events": stack_count,
    }


def strict_int(value: Any, path: str, *, minimum: int | None = None) -> int:
    if type(value) is not int:
        raise GateError(f"{path} must be an integer")
    if minimum is not None and value < minimum:
        raise GateError(f"{path} must be >= {minimum}; got {value}")
    return value


def strict_number(value: Any, path: str) -> float:
    if type(value) not in (int, float):
        raise GateError(f"{path} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise GateError(f"{path} must be finite")
    return number


def require_exact_keys(value: Any, keys: set[str], path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GateError(f"{path} must be an object")
    actual = set(value)
    if actual != keys:
        raise GateError(
            f"{path} keys differ; missing={sorted(keys - actual)!r}, "
            f"extra={sorted(actual - keys)!r}"
        )
    return value


def validate_profile_capacity_control(
    path: Path, expected_profile_min_free_bytes: int | None = None
) -> dict[str, Any]:
    control_path = regular_non_symlink(path, "profile capacity control")
    if stat.S_IMODE(control_path.stat().st_mode) != 0o444:
        raise GateError("profile capacity control must have exact mode 0444")
    value = require_exact_keys(
        load_json(control_path),
        {"schema", "profile_min_free_bytes"},
        "$.profile_capacity_control",
    )
    if value["schema"] != PROFILE_CAPACITY_CONTROL_SCHEMA:
        raise GateError("profile capacity control schema mismatch")
    reserve = strict_int(
        value["profile_min_free_bytes"],
        "$.profile_capacity_control.profile_min_free_bytes",
        minimum=CAPACITY_RESERVE_BYTES,
    )
    if (
        expected_profile_min_free_bytes is not None
        and reserve != expected_profile_min_free_bytes
    ):
        raise GateError("profile capacity control differs from the configured reserve")
    return value


def create_profile_capacity_control(
    output: Path, profile_min_free_bytes: int
) -> dict[str, Any]:
    if not output.is_absolute():
        raise GateError("profile capacity control path must be absolute")
    reserve = strict_int(
        profile_min_free_bytes,
        "$.profile_capacity_control.profile_min_free_bytes",
        minimum=CAPACITY_RESERVE_BYTES,
    )
    value = {
        "schema": PROFILE_CAPACITY_CONTROL_SCHEMA,
        "profile_min_free_bytes": reserve,
    }
    publish_json_read_only_atomic_exclusive(output, value)
    current = validate_profile_capacity_control(output, reserve)
    if current != value:
        raise GateError("fresh profile capacity control failed self-validation")
    return value


def derive_profile_guardian_minimum_free_bytes(
    capacity_control_path: Path, reference_corpus_path: Path
) -> int:
    control = validate_profile_capacity_control(capacity_control_path)
    reference = validate_corpus_summary(
        load_json(reference_corpus_path), "$.profile_reference_corpus"
    )
    return reference["size_bytes"] + control["profile_min_free_bytes"]


def validate_plan(plan_path: Path, phase1_expectations: Path) -> dict[str, Any]:
    plan = require_exact_keys(
        load_json(plan_path),
        {
            "schema",
            "phase1_expectations_sha256",
            "harness_dependencies",
            "workload",
            "build_contract",
            "environment_contract",
            "quiescence_contract",
            "calibration_contract",
            "profiling_contract",
            "perf_stat_events",
            "policies",
            "schedule",
            "screen_candidate_gate",
            "timing_contract",
            "completion_contract",
        },
        "$",
    )
    if plan["schema"] != PLAN_SCHEMA:
        raise GateError(f"unsupported plan schema: {plan['schema']!r}")
    expected_digest = plan["phase1_expectations_sha256"]
    if not isinstance(expected_digest, str) or not re.fullmatch(
        r"[0-9a-f]{64}", expected_digest
    ):
        raise GateError("phase1_expectations_sha256 must be a lowercase SHA-256")
    actual_digest = sha256_file(phase1_expectations)
    if actual_digest != expected_digest:
        raise GateError(
            "Phase 1 expectations helper changed: "
            f"expected {expected_digest}, got {actual_digest}"
        )
    dependencies = require_exact_keys(
        plan["harness_dependencies"],
        {"phase1_replay_gate.py", "ab_gate.py", "fadvise_regular_dontneed.c"},
        "$.harness_dependencies",
    )
    for name, expected in dependencies.items():
        if not isinstance(expected, str) or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
            raise GateError(f"invalid frozen helper digest for {name}")
        helper = plan_path.parent / name
        actual = sha256_file(helper)
        if actual != expected:
            raise GateError(
                f"frozen helper {name} changed: expected {expected}, got {actual}"
            )

    workload = require_exact_keys(
        plan["workload"],
        {
            "stop_after_messages",
            "post_ingester_drop_hold_secs",
            "rss_interval_ms",
            "readback_sample_limit_per_kind",
            "storage_schema",
            "capture_eviction_required",
            "max_capture_resident_bytes_after_evict",
            "allocator_release_telemetry_required",
            "expected_readback_queries",
            "expected_promql_rows",
        },
        "$.workload",
    )
    expected_workload = {
        "stop_after_messages": 250_000,
        "post_ingester_drop_hold_secs": 30,
        "rss_interval_ms": 100,
        "readback_sample_limit_per_kind": 2,
        "storage_schema": "schema8",
        "capture_eviction_required": True,
        "max_capture_resident_bytes_after_evict": 0,
        "allocator_release_telemetry_required": True,
        "expected_readback_queries": 40,
        "expected_promql_rows": 14,
    }
    if workload != expected_workload:
        raise GateError(
            f"workload differs from the frozen 250k screen: {workload!r}"
        )
    if plan["perf_stat_events"] != EXPECTED_PERF_EVENTS:
        raise GateError("perf_stat_events differs from the frozen event set")

    calibration_contract = require_exact_keys(
        plan["calibration_contract"],
        {
            "required",
            "source_allocator",
            "source_workload",
            "stop_after_messages",
            "storage_verification",
            "promql_fingerprint_source",
            "require_corpus_identity_with_measured_runs",
            "require_raw_report_sha256",
            "four_million_fingerprint_reuse_forbidden",
        },
        "$.calibration_contract",
    )
    if calibration_contract != {
        "required": True,
        "source_allocator": "system",
        "source_workload": "fresh untimed replay before measured schedule",
        "stop_after_messages": 250_000,
        "storage_verification": (
            "schema8 exhaustive series/footer/exact-postings"
        ),
        "promql_fingerprint_source": (
            "canonical rows from the raw 250k readback report"
        ),
        "require_corpus_identity_with_measured_runs": True,
        "require_raw_report_sha256": True,
        "four_million_fingerprint_reuse_forbidden": True,
    }:
        raise GateError("calibration contract differs from the frozen 250k contract")

    profiling_contract = require_exact_keys(
        plan["profiling_contract"],
        {
            "measurement_eligible",
            "heap_authority",
            "candidate_specific_jemalloc_heap_profiling",
            "perf_record_optional",
            "require_zero_lost_events",
            "require_profile_corpus_identity",
            "require_profile_correctness_identity",
            "require_completed_screen_artifact_seal",
            "require_all_executable_hashes",
            "selected_perf_requires_full_policy_preflight",
        },
        "$.profiling_contract",
    )
    if profiling_contract != {
        "measurement_eligible": False,
        "heap_authority": "heaptrack over frozen system-allocator binary",
        "candidate_specific_jemalloc_heap_profiling": "deferred",
        "perf_record_optional": True,
        "require_zero_lost_events": True,
        "require_profile_corpus_identity": True,
        "require_profile_correctness_identity": True,
        "require_completed_screen_artifact_seal": True,
        "require_all_executable_hashes": True,
        "selected_perf_requires_full_policy_preflight": True,
    }:
        raise GateError("profiling contract differs from the frozen untimed contract")

    build_contract = require_exact_keys(
        plan["build_contract"],
        {
            "same_clean_commit",
            "cargo_locked",
            "cargo_incremental",
            "jemalloc_stats_enabled",
            "screen_jemalloc_feature",
            "later_no_stats_jemalloc_feature",
            "build_source_mode",
            "live_worktree_build_forbidden",
            "require_archive_tree_equivalence",
            "require_read_only_extracted_source",
            "system_command",
            "jemalloc_command",
            "later_no_stats_revalidation_command",
        },
        "$.build_contract",
    )
    expected_system_command = (
        "cargo build --manifest-path Cargo.toml --locked --release --no-default-features "
        "-p chronoxide-ingester -p chronoxide-query-cli "
        "--bin chronoxide-ingester --bin chronoxide-query "
        "--bin chronoxide-storage-verify"
    )
    expected_jemalloc_command = (
        "cargo build --manifest-path Cargo.toml --locked --release --no-default-features "
        "--features jemalloc-stats "
        "-p chronoxide-ingester --bin chronoxide-ingester"
    )
    expected_no_stats_command = (
        "cargo build --manifest-path Cargo.toml --locked --release --no-default-features "
        "--features jemalloc "
        "-p chronoxide-ingester --bin chronoxide-ingester"
    )
    if build_contract != {
        "same_clean_commit": True,
        "cargo_locked": True,
        "cargo_incremental": False,
        "jemalloc_stats_enabled": True,
        "screen_jemalloc_feature": "jemalloc-stats",
        "later_no_stats_jemalloc_feature": "jemalloc",
        "build_source_mode": "read-only git archive HEAD extraction",
        "live_worktree_build_forbidden": True,
        "require_archive_tree_equivalence": True,
        "require_read_only_extracted_source": True,
        "system_command": expected_system_command,
        "jemalloc_command": expected_jemalloc_command,
        "later_no_stats_revalidation_command": expected_no_stats_command,
    }:
        raise GateError("build contract differs from the frozen controlled build")

    if require_exact_keys(
        plan["environment_contract"],
        {
            "locale",
            "timezone",
            "rust_log",
            "runtime_environment_mode",
            "require_all_jemalloc_config_sources_audited",
            "external_conflict_poll_interval_ms",
        },
        "$.environment_contract",
    ) != {
        "locale": "C",
        "timezone": "UTC",
        "rust_log": "chronoxide_ingester=info,chronoxide_core=warn",
        "runtime_environment_mode": "env-i allowlist",
        "require_all_jemalloc_config_sources_audited": True,
        "external_conflict_poll_interval_ms": 100,
    }:
        raise GateError("environment contract differs from the frozen allowlist")

    if require_exact_keys(
        plan["quiescence_contract"],
        {
            "sync_corpus_after_each_run",
            "maximum_dirty_writeback_kib",
            "required_consecutive_samples",
            "poll_interval_ms",
            "timeout_secs",
        },
        "$.quiescence_contract",
    ) != {
        "sync_corpus_after_each_run": True,
        "maximum_dirty_writeback_kib": 65_536,
        "required_consecutive_samples": 3,
        "poll_interval_ms": 250,
        "timeout_secs": 120,
    }:
        raise GateError("writeback quiescence contract differs from the frozen plan")

    policies = require_exact_keys(plan["policies"], set(POLICY_ORDER), "$.policies")
    for policy_name in POLICY_ORDER:
        policy = require_exact_keys(
            policies[policy_name],
            {"binary_role", "rust_global_allocator", "jemalloc_conf", "comparator_only"},
            f"$.policies.{policy_name}",
        )
        expected_allocator = "system" if policy_name == "S" else "jemalloc"
        if policy["binary_role"] != expected_allocator:
            raise GateError(f"{policy_name} has the wrong binary_role")
        if policy["rust_global_allocator"] != expected_allocator:
            raise GateError(f"{policy_name} has the wrong allocator identity")
        if policy["jemalloc_conf"] != EXPECTED_CONFS[policy_name]:
            raise GateError(f"{policy_name} has a changed jemalloc policy")
        if policy["comparator_only"] is not (policy_name in {"S", "J0"}):
            raise GateError(f"{policy_name} has the wrong comparator-only role")

    schedule = plan["schedule"]
    if not isinstance(schedule, list) or len(schedule) != len(EXPECTED_SCHEDULE):
        raise GateError("schedule must contain exactly ten mirrored runs")
    for index, (row, expected_policy) in enumerate(
        zip(schedule, EXPECTED_SCHEDULE, strict=True), start=1
    ):
        row = require_exact_keys(
            row, {"run_index", "block", "position", "policy"}, f"$.schedule[{index - 1}]"
        )
        expected_block = 1 if index <= 5 else 2
        expected_position = index if index <= 5 else index - 5
        if row != {
            "run_index": index,
            "block": expected_block,
            "position": expected_position,
            "policy": expected_policy,
        }:
            raise GateError(f"schedule row {index} violates the frozen mirror")

    candidate_gate = require_exact_keys(
        plan["screen_candidate_gate"],
        {
            "minimum_workload_cpu_improvement_percent",
            "maximum_workload_peak_rss_regression_percent",
            "maximum_workload_hwm_regression_percent",
            "maximum_post_drop_end_rss_regression_percent",
            "maximum_mirrored_pair_relative_spread_percent",
            "minimum_post_drop_rss_samples",
            "maximum_hold_elapsed_secs",
            "maximum_workload_cpu_boundary_uncertainty_intervals",
        },
        "$.screen_candidate_gate",
    )
    expected_gate = {
        "minimum_workload_cpu_improvement_percent": 3.0,
        "maximum_workload_peak_rss_regression_percent": 5.0,
        "maximum_workload_hwm_regression_percent": 5.0,
        "maximum_post_drop_end_rss_regression_percent": 5.0,
        "maximum_mirrored_pair_relative_spread_percent": 5.0,
        "minimum_post_drop_rss_samples": 20,
        "maximum_hold_elapsed_secs": 60,
        "maximum_workload_cpu_boundary_uncertainty_intervals": 1,
    }
    if candidate_gate != expected_gate:
        raise GateError("screen candidate thresholds differ from the frozen plan")

    timing = require_exact_keys(
        plan["timing_contract"],
        {
            "workload_wall_source",
            "workload_cpu_source",
            "workload_cpu_boundary_uncertainty",
            "workload_rss_source",
            "gnu_time_scope",
            "perf_stat_scope",
            "rss_scope",
        },
        "$.timing_contract",
    )
    if timing != {
        "workload_wall_source": (
            "main-entry through ingester_dropped checkpoint main_elapsed_ns, "
            "including Tokio runtime construction"
        ),
        "workload_cpu_source": (
            "external proc process-tree utime+stime at first post-drop sample"
        ),
        "workload_cpu_boundary_uncertainty": "at most one rss sampling interval",
        "workload_rss_source": "external proc workload-phase process-tree peak",
        "gnu_time_scope": "complete process including the post-drop hold",
        "perf_stat_scope": "complete process including the post-drop hold",
        "rss_scope": "complete process tree, externally sampled from proc",
    }:
        raise GateError("timing contract differs from the frozen plan")

    completion = require_exact_keys(
        plan["completion_contract"],
        {
            "required_run_count",
            "require_all_corpora_and_correctness_identical",
            "require_canonical_storage_validation",
            "require_canonical_readback_validation",
            "required_final_artifacts",
            "partial_runs_promotable",
            "decision_scope",
            "require_stats_enabled_full_4m_gate",
            "require_no_stats_revalidation_before_production",
            "production_promotion_authorized",
        },
        "$.completion_contract",
    )
    if completion != {
        "required_run_count": 10,
        "require_all_corpora_and_correctness_identical": True,
        "require_canonical_storage_validation": True,
        "require_canonical_readback_validation": True,
        "required_final_artifacts": [
            "comparisons/final-screen-decision.json",
            "COMPLETE",
        ],
        "partial_runs_promotable": False,
        "decision_scope": (
            "stats-enabled 250k screen nominates at most one bounded policy for a "
            "later stats-enabled 4M gate; no production promotion"
        ),
        "require_stats_enabled_full_4m_gate": True,
        "require_no_stats_revalidation_before_production": True,
        "production_promotion_authorized": False,
    }:
        raise GateError("completion contract differs from the frozen plan")
    return plan


def executable_sha256(path: Path) -> str:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise GateError(f"comparator binary must be an absolute non-symlink file: {path}")
    mode = path.stat().st_mode
    if mode & 0o111 == 0:
        raise GateError(f"comparator binary is not executable: {path}")
    if mode & 0o222:
        raise GateError(f"preserved comparator binary must be non-writable: {path}")
    return sha256_file(path)


def git_output(repo: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "")
        raise GateError(
            f"git {' '.join(arguments)} failed for {repo}: {detail}"
        ) from error
    return completed.stdout.strip()


def git_bytes(repo: Path, *arguments: str) -> bytes:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = os.fsdecode(getattr(error, "stderr", b""))
        raise GateError(
            f"git {' '.join(arguments)} failed for {repo}: {detail}"
        ) from error
    return completed.stdout


def excluded_untracked_runtime_artifact(path: str) -> bool:
    return (
        "/__pycache__/" in f"/{path}"
        or path.endswith(".pyc")
        or (
            path.startswith("chronoxide-ingester/")
            and Path(path).name.startswith("ingestion_stats_")
            and path.endswith(".md")
        )
    )


def ignored_build_input_candidate(path: str) -> bool:
    if excluded_untracked_runtime_artifact(path) or path.startswith("target/"):
        return False
    candidate = Path(path)
    if path in {".cargo/config", ".cargo/config.toml"} or path.endswith(
        ("/.cargo/config", "/.cargo/config.toml")
    ):
        return True
    return candidate.name in {"Cargo.toml", "Cargo.lock", "build.rs"} or candidate.suffix in {
        ".S",
        ".asm",
        ".c",
        ".cc",
        ".cfg",
        ".cpp",
        ".h",
        ".hpp",
        ".inc",
        ".json",
        ".ld",
        ".proto",
        ".py",
        ".rs",
        ".s",
        ".sh",
        ".toml",
        ".yaml",
        ".yml",
    }


def audit_tracked_build_inputs(repo: Path) -> dict[str, Any]:
    tagged = [
        row
        for row in git_bytes(repo, "ls-files", "-v", "-z").split(b"\0")
        if row
    ]
    hidden = [os.fsdecode(row[2:]) for row in tagged if not row.startswith(b"H ")]
    if hidden:
        raise GateError(
            "controlled build rejects assume-unchanged/skip-worktree index flags: "
            f"{hidden[:8]!r}"
        )

    staged = git_bytes(repo, "ls-files", "--stage", "-z")
    records = [row for row in staged.split(b"\0") if row]
    if len(records) != len(tagged):
        raise GateError("controlled build tracked-input inventories disagree")
    for record in records:
        try:
            metadata, raw_path = record.split(b"\t", 1)
            mode, _object_id, stage = metadata.split(b" ", 2)
        except ValueError as error:
            raise GateError("controlled build has a malformed tracked input") from error
        path = os.fsdecode(raw_path)
        if stage != b"0":
            raise GateError(f"controlled build has an unmerged tracked input: {path}")
        if mode not in {b"100644", b"100755"}:
            kind = {
                b"120000": "symlink",
                b"160000": "gitlink",
            }.get(mode, f"mode {os.fsdecode(mode)}")
            raise GateError(f"controlled build rejects tracked {kind} input: {path}")
        candidate = repo / path
        if candidate.is_symlink() or not candidate.is_file():
            raise GateError(
                f"controlled build tracked input is not a regular file: {path}"
            )
    return {
        "tracked_input_count": len(records),
        "tracked_input_manifest_sha256": hashlib.sha256(staged).hexdigest(),
        "git_index_flags_clear": True,
        "tracked_inputs_regular_files": True,
    }


def ensure_ambient_cargo_configs_absent(repo: Path) -> None:
    candidates: set[Path] = set()
    for base in (Path.home(), *repo.parents):
        if base == repo:
            continue
        candidates.add(base / ".cargo/config")
        candidates.add(base / ".cargo/config.toml")
    for candidate in sorted(candidates):
        if candidate.exists() or candidate.is_symlink():
            raise GateError(f"ambient Cargo configuration is forbidden: {candidate}")


def source_seal(repo: Path) -> dict[str, Any]:
    if not repo.is_absolute() or repo.is_symlink() or not repo.is_dir():
        raise GateError("formal source repository must be an absolute non-symlink directory")
    repo = repo.resolve(strict=True)
    if git_output(repo, "rev-parse", "--show-toplevel") != str(repo):
        raise GateError("formal source repository is not the Git worktree root")
    if git_output(repo, "status", "--porcelain=v2", "--untracked-files=no"):
        raise GateError("formal source-bound build requires a clean tracked worktree and index")

    tracked_inputs = audit_tracked_build_inputs(repo)
    untracked = [
        os.fsdecode(item)
        for item in git_bytes(
            repo, "ls-files", "--others", "--exclude-standard", "-z"
        ).split(b"\0")
        if item
    ]
    disallowed_untracked = [
        path for path in untracked if not excluded_untracked_runtime_artifact(path)
    ]
    if disallowed_untracked:
        raise GateError(
            "formal source-bound build rejects untracked build input: "
            f"{disallowed_untracked[0]}"
        )
    ignored = [
        os.fsdecode(item)
        for item in git_bytes(
            repo,
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "-z",
        ).split(b"\0")
        if item
    ]
    ignored_build_inputs = [path for path in ignored if ignored_build_input_candidate(path)]
    if ignored_build_inputs:
        raise GateError(
            "formal source-bound build rejects ignored source/build input: "
            f"{ignored_build_inputs[0]}"
        )

    cargo_lock = repo / "Cargo.lock"
    if not cargo_lock.is_file() or cargo_lock.is_symlink():
        raise GateError("formal source-bound build requires a regular Cargo.lock")
    if git_output(repo, "ls-files", "--error-unmatch", "Cargo.lock") != "Cargo.lock":
        raise GateError("formal source-bound build requires a tracked Cargo.lock")
    tracked = [
        os.fsdecode(item)
        for item in git_bytes(repo, "ls-files", "-z").split(b"\0")
        if item
    ]
    cargo_configs = []
    for relative in tracked:
        if relative not in {".cargo/config", ".cargo/config.toml"} and not relative.endswith(
            ("/.cargo/config", "/.cargo/config.toml")
        ):
            continue
        path = repo / relative
        if not path.is_file() or path.is_symlink():
            raise GateError(f"tracked Cargo configuration is not regular: {relative}")
        cargo_configs.append(
            {
                "path": relative,
                "sha256": sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
        )
    ensure_ambient_cargo_configs_absent(repo)

    head = git_output(repo, "rev-parse", "HEAD")
    head_tree = git_output(repo, "rev-parse", "HEAD^{tree}")
    index_tree = git_output(repo, "write-tree")
    if head_tree != index_tree:
        raise GateError("formal source index tree differs from the HEAD tree")
    identity = {
        "git_head": head,
        "git_head_tree": head_tree,
        "git_index_tree": index_tree,
        **tracked_inputs,
        "cargo_lock_sha256": sha256_file(cargo_lock),
        "tracked_cargo_configs": cargo_configs,
        "ambient_cargo_configs_absent": True,
    }
    identity_sha256 = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "schema": SOURCE_SEAL_SCHEMA,
        "repo": str(repo),
        **identity,
        "identity_sha256": identity_sha256,
        "excluded_untracked_runtime_artifacts": sorted(untracked),
    }


def validate_source_seal_document(value: Any) -> dict[str, Any]:
    seal = require_exact_keys(
        value,
        {
            "schema",
            "repo",
            "git_head",
            "git_head_tree",
            "git_index_tree",
            "tracked_input_count",
            "tracked_input_manifest_sha256",
            "git_index_flags_clear",
            "tracked_inputs_regular_files",
            "cargo_lock_sha256",
            "tracked_cargo_configs",
            "ambient_cargo_configs_absent",
            "identity_sha256",
            "excluded_untracked_runtime_artifacts",
        },
        "$.source_seal",
    )
    if seal["schema"] != SOURCE_SEAL_SCHEMA:
        raise GateError("source seal schema mismatch")
    if not isinstance(seal["repo"], str) or not Path(seal["repo"]).is_absolute():
        raise GateError("source seal repository path is not absolute")
    for key in (
        "git_head",
        "git_head_tree",
        "git_index_tree",
        "tracked_input_manifest_sha256",
        "cargo_lock_sha256",
        "identity_sha256",
    ):
        if not isinstance(seal[key], str) or re.fullmatch(r"[0-9a-f]{40,64}", seal[key]) is None:
            raise GateError(f"source seal {key} is invalid")
    strict_int(seal["tracked_input_count"], "$.source_seal.tracked_input_count", minimum=1)
    if seal["git_head_tree"] != seal["git_index_tree"]:
        raise GateError("source seal does not bind one Git tree")
    if (
        seal["git_index_flags_clear"] is not True
        or seal["tracked_inputs_regular_files"] is not True
        or seal["ambient_cargo_configs_absent"] is not True
    ):
        raise GateError("source seal permits hidden, non-regular, or ambient inputs")
    configs = seal["tracked_cargo_configs"]
    if not isinstance(configs, list):
        raise GateError("source seal Cargo configurations must be a list")
    for index, item in enumerate(configs):
        config = require_exact_keys(
            item, {"path", "sha256", "size_bytes"}, f"$.source_seal.tracked_cargo_configs[{index}]"
        )
        if config["path"] not in {".cargo/config", ".cargo/config.toml"} and not config[
            "path"
        ].endswith(("/.cargo/config", "/.cargo/config.toml")):
            raise GateError("source seal contains an unexpected Cargo configuration")
        if not isinstance(config["sha256"], str) or re.fullmatch(
            r"[0-9a-f]{64}", config["sha256"]
        ) is None:
            raise GateError("source seal Cargo configuration hash is invalid")
        strict_int(config["size_bytes"], "$.source_seal.cargo_config.size_bytes", minimum=1)
    excluded = seal["excluded_untracked_runtime_artifacts"]
    if not isinstance(excluded, list) or any(
        not isinstance(path, str) or not excluded_untracked_runtime_artifact(path)
        for path in excluded
    ):
        raise GateError("source seal excluded-untracked list is invalid")
    identity = {
        key: seal[key]
        for key in (
            "git_head",
            "git_head_tree",
            "git_index_tree",
            "tracked_input_count",
            "tracked_input_manifest_sha256",
            "git_index_flags_clear",
            "tracked_inputs_regular_files",
            "cargo_lock_sha256",
            "tracked_cargo_configs",
            "ambient_cargo_configs_absent",
        )
    }
    expected_identity = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if seal["identity_sha256"] != expected_identity:
        raise GateError("source seal identity digest is inconsistent")
    return seal


def check_source_seal(repo: Path, seal_path: Path) -> dict[str, Any]:
    expected = validate_source_seal_document(load_json(seal_path))
    current = validate_source_seal_document(source_seal(repo))
    if current != expected:
        differing = sorted(key for key in expected if expected[key] != current[key])
        raise GateError(f"formal source seal changed: {differing!r}")
    return {
        "status": "pass",
        "identity_sha256": current["identity_sha256"],
        "source_seal_sha256": sha256_file(seal_path),
    }


def git_head_file_inventory(repo: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for record in git_bytes(
        repo, "ls-tree", "-r", "-z", "--full-tree", "HEAD"
    ).split(b"\0"):
        if not record:
            continue
        try:
            metadata, raw_path = record.split(b"\t", 1)
            mode, object_type, object_id = metadata.split(b" ", 2)
        except ValueError as error:
            raise GateError("Git HEAD contains a malformed tree entry") from error
        path = os.fsdecode(raw_path)
        relative = Path(path)
        if (
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
            or relative.as_posix() != path
        ):
            raise GateError(f"Git HEAD contains an unsafe path: {path!r}")
        if object_type != b"blob" or mode not in {b"100644", b"100755"}:
            raise GateError(f"Git HEAD build input is not a regular file: {path}")
        if path in result:
            raise GateError(f"Git HEAD contains a duplicate path: {path}")
        result[path] = {
            "mode": os.fsdecode(mode),
            "object_id": os.fsdecode(object_id),
        }
    if not result:
        raise GateError("Git HEAD build-source inventory is empty")
    return result


def git_blob_object_id(data: bytes, object_format: str) -> str:
    if object_format not in {"sha1", "sha256"}:
        raise GateError(f"unsupported Git object format: {object_format!r}")
    digest = hashlib.new(object_format)
    digest.update(f"blob {len(data)}\0".encode())
    digest.update(data)
    return digest.hexdigest()


def archive_embedded_commit(archive_path: Path) -> str:
    try:
        with archive_path.open("rb") as source:
            completed = subprocess.run(
                ["git", "get-tar-commit-id"],
                check=True,
                stdin=source,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "")
        raise GateError(
            f"Git archive has no valid embedded commit: {detail}"
        ) from error
    commit = completed.stdout.strip()
    if re.fullmatch(r"[0-9a-f]{40,64}", commit) is None:
        raise GateError("Git archive embedded commit is invalid")
    return commit


def ensure_build_cwd_cargo_configs_absent(source_root: Path) -> None:
    candidates: set[Path] = {
        Path.home() / ".cargo/config",
        Path.home() / ".cargo/config.toml",
    }
    for ancestor in source_root.parents:
        candidates.add(ancestor / ".cargo/config")
        candidates.add(ancestor / ".cargo/config.toml")
    for candidate in sorted(candidates):
        if candidate.exists() or candidate.is_symlink():
            raise GateError(
                f"build-source ancestor Cargo configuration is forbidden: {candidate}"
            )


def validate_snapshot_cargo_inputs(source_root: Path) -> int:
    config_path = source_root / ".cargo/config.toml"
    if config_path.is_symlink() or not config_path.is_file():
        raise GateError("extracted build source lacks tracked root .cargo/config.toml")
    try:
        config = tomllib.loads(config_path.read_text(encoding="utf-8", errors="strict"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise GateError("extracted Cargo configuration is malformed") from error
    if config != {"build": {"rustflags": "-C target-cpu=native"}}:
        raise GateError("extracted Cargo configuration has an unexpected build input")

    references = 0
    for manifest_path in sorted(source_root.rglob("Cargo.toml")):
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise GateError("extracted Cargo manifest is not a regular file")
        try:
            manifest = tomllib.loads(
                manifest_path.read_text(encoding="utf-8", errors="strict")
            )
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise GateError(f"extracted Cargo manifest is malformed: {manifest_path}") from error

        def visit(value: Any, location: str) -> None:
            nonlocal references
            if isinstance(value, dict):
                for key, child in value.items():
                    if key == "path" and isinstance(child, str):
                        references += 1
                        candidate = Path(child)
                        candidate = (
                            candidate
                            if candidate.is_absolute()
                            else manifest_path.parent / candidate
                        )
                        try:
                            resolved = candidate.resolve(strict=True)
                        except OSError as error:
                            raise GateError(
                                f"extracted Cargo path does not exist at {location}.path: {child!r}"
                            ) from error
                        if resolved != source_root and not resolved.is_relative_to(source_root):
                            raise GateError(
                                f"extracted Cargo path escapes the sealed source at {location}.path: {child!r}"
                            )
                    visit(child, f"{location}.{key}")
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    visit(child, f"{location}[{index}]")

        visit(manifest, manifest_path.relative_to(source_root).as_posix())
    return references


def extracted_source_seal(
    repo: Path,
    source_root: Path,
    archive_path: Path,
    live_source_seal_path: Path,
) -> dict[str, Any]:
    live_check = check_source_seal(repo, live_source_seal_path)
    live = validate_source_seal_document(load_json(live_source_seal_path))
    if not source_root.is_absolute() or source_root.is_symlink() or not source_root.is_dir():
        raise GateError("extracted build source must be an absolute non-symlink directory")
    source_root = source_root.resolve(strict=True)
    repo = repo.resolve(strict=True)
    if source_root == repo or source_root.is_relative_to(repo):
        raise GateError("extracted build source must be outside the live worktree")
    if not archive_path.is_absolute() or archive_path.is_symlink() or not archive_path.is_file():
        raise GateError("Git source archive must be an absolute non-symlink regular file")
    archive_path = archive_path.resolve(strict=True)
    if archive_path == repo or archive_path.is_relative_to(repo):
        raise GateError("Git source archive must be outside the live worktree")
    if archive_path.stat().st_mode & 0o222:
        raise GateError("Git source archive must be non-writable")
    embedded_commit = archive_embedded_commit(archive_path)
    if embedded_commit != live["git_head"]:
        raise GateError("Git source archive does not embed the sealed HEAD commit")
    ensure_build_cwd_cargo_configs_absent(source_root)

    expected = git_head_file_inventory(repo)
    object_format = git_output(repo, "rev-parse", "--show-object-format")
    entries: list[dict[str, Any]] = []
    directory_count = 0

    def visit(directory: Path, relative_directory: Path) -> None:
        nonlocal directory_count
        directory_mode = directory.lstat().st_mode
        if not stat.S_ISDIR(directory_mode) or directory.is_symlink():
            raise GateError("extracted build source contains a non-directory path component")
        if directory_mode & 0o222:
            raise GateError(f"extracted build-source directory is writable: {directory}")
        directory_count += 1
        with os.scandir(directory) as scanned:
            children = sorted(scanned, key=lambda entry: os.fsencode(entry.name))
        for child in children:
            path = Path(child.path)
            relative = relative_directory / child.name
            relative_text = relative.as_posix()
            mode = path.lstat().st_mode
            if stat.S_ISDIR(mode) and not path.is_symlink():
                visit(path, relative)
                continue
            if not stat.S_ISREG(mode) or path.is_symlink():
                raise GateError(
                    f"extracted build source contains a symlink or non-regular entry: {relative_text}"
                )
            if mode & 0o222:
                raise GateError(f"extracted build-source file is writable: {relative_text}")
            expected_entry = expected.get(relative_text)
            if expected_entry is None:
                raise GateError(
                    f"extracted build source contains a path outside Git HEAD: {relative_text}"
                )
            normalized_mode = "100755" if mode & 0o111 else "100644"
            if normalized_mode != expected_entry["mode"]:
                raise GateError(
                    f"extracted build-source mode differs from Git HEAD: {relative_text}"
                )
            data = path.read_bytes()
            object_id = git_blob_object_id(data, object_format)
            if object_id != expected_entry["object_id"]:
                raise GateError(
                    f"extracted build-source content differs from Git HEAD: {relative_text}"
                )
            entries.append(
                {
                    "path": relative_text,
                    "mode": normalized_mode,
                    "size_bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "git_object_id": object_id,
                }
            )

    visit(source_root, Path())
    entries.sort(key=lambda entry: os.fsencode(entry["path"]))
    actual_paths = {entry["path"] for entry in entries}
    if actual_paths != set(expected):
        missing = sorted(set(expected) - actual_paths)
        raise GateError(
            f"extracted build source is missing Git HEAD paths: {missing[:8]!r}"
        )
    manifest_path_reference_count = validate_snapshot_cargo_inputs(source_root)
    manifest_bytes = json.dumps(
        entries, sort_keys=True, separators=(",", ":")
    ).encode()
    return {
        "schema": EXTRACTED_SOURCE_SEAL_SCHEMA,
        "repo": str(repo),
        "source_root": str(source_root),
        "archive_path": str(archive_path),
        "archive_sha256": sha256_file(archive_path),
        "archive_size_bytes": archive_path.stat().st_size,
        "archive_embedded_commit": embedded_commit,
        "git_head": live["git_head"],
        "git_head_tree": live["git_head_tree"],
        "git_object_format": object_format,
        "live_source_seal_sha256": live_check["source_seal_sha256"],
        "live_source_identity_sha256": live["identity_sha256"],
        "file_count": len(entries),
        "directory_count": directory_count,
        "total_file_bytes": sum(entry["size_bytes"] for entry in entries),
        "file_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "archive_tree_equivalent": True,
        "all_entries_non_writable": True,
        "cargo_configuration_exact": True,
        "manifest_path_reference_count": manifest_path_reference_count,
        "all_manifest_paths_within_source": True,
        "live_worktree_used_as_build_source": False,
    }


def validate_extracted_source_seal_document(value: Any) -> dict[str, Any]:
    seal = require_exact_keys(
        value,
        {
            "schema",
            "repo",
            "source_root",
            "archive_path",
            "archive_sha256",
            "archive_size_bytes",
            "archive_embedded_commit",
            "git_head",
            "git_head_tree",
            "git_object_format",
            "live_source_seal_sha256",
            "live_source_identity_sha256",
            "file_count",
            "directory_count",
            "total_file_bytes",
            "file_manifest_sha256",
            "archive_tree_equivalent",
            "all_entries_non_writable",
            "cargo_configuration_exact",
            "manifest_path_reference_count",
            "all_manifest_paths_within_source",
            "live_worktree_used_as_build_source",
        },
        "$.extracted_source_seal",
    )
    if seal["schema"] != EXTRACTED_SOURCE_SEAL_SCHEMA:
        raise GateError("extracted source seal schema mismatch")
    for key in ("repo", "source_root", "archive_path"):
        if not isinstance(seal[key], str) or not Path(seal[key]).is_absolute():
            raise GateError(f"extracted source seal {key} is not absolute")
    repo_path = Path(seal["repo"])
    source_path = Path(seal["source_root"])
    archive_path = Path(seal["archive_path"])
    if source_path == repo_path or source_path.is_relative_to(repo_path):
        raise GateError("extracted source seal uses the live worktree as build source")
    if archive_path == repo_path or archive_path.is_relative_to(repo_path):
        raise GateError("extracted source archive is inside the live worktree")
    for key in (
        "archive_sha256",
        "live_source_seal_sha256",
        "live_source_identity_sha256",
        "file_manifest_sha256",
    ):
        if not isinstance(seal[key], str) or re.fullmatch(r"[0-9a-f]{64}", seal[key]) is None:
            raise GateError(f"extracted source seal {key} is invalid")
    for key in ("archive_embedded_commit", "git_head", "git_head_tree"):
        if not isinstance(seal[key], str) or re.fullmatch(r"[0-9a-f]{40,64}", seal[key]) is None:
            raise GateError(f"extracted source seal {key} is invalid")
    if seal["archive_embedded_commit"] != seal["git_head"]:
        raise GateError("extracted source seal archive commit differs from Git HEAD")
    if seal["git_object_format"] not in {"sha1", "sha256"}:
        raise GateError("extracted source seal Git object format is invalid")
    strict_int(seal["archive_size_bytes"], "$.extracted_source_seal.archive_size_bytes", minimum=1)
    strict_int(seal["file_count"], "$.extracted_source_seal.file_count", minimum=1)
    strict_int(seal["directory_count"], "$.extracted_source_seal.directory_count", minimum=1)
    strict_int(seal["total_file_bytes"], "$.extracted_source_seal.total_file_bytes", minimum=1)
    strict_int(
        seal["manifest_path_reference_count"],
        "$.extracted_source_seal.manifest_path_reference_count",
        minimum=0,
    )
    if (
        seal["archive_tree_equivalent"] is not True
        or seal["all_entries_non_writable"] is not True
        or seal["cargo_configuration_exact"] is not True
        or seal["all_manifest_paths_within_source"] is not True
        or seal["live_worktree_used_as_build_source"] is not False
    ):
        raise GateError("extracted source seal permits an unsealed or live build source")
    return seal


def check_extracted_source_seal(
    repo: Path,
    source_root: Path,
    archive_path: Path,
    live_source_seal_path: Path,
    extracted_source_seal_path: Path,
    build_provenance_path: Path | None = None,
) -> dict[str, Any]:
    expected = validate_extracted_source_seal_document(
        load_json(extracted_source_seal_path)
    )
    current = validate_extracted_source_seal_document(
        extracted_source_seal(repo, source_root, archive_path, live_source_seal_path)
    )
    if current != expected:
        differing = sorted(key for key in expected if expected[key] != current[key])
        raise GateError(f"extracted build-source seal changed: {differing!r}")
    if build_provenance_path is not None:
        build = validate_build_provenance(load_json(build_provenance_path))
        if (
            build["source_seal_sha256"] != sha256_file(live_source_seal_path)
            or build["source_identity_sha256"] != current["live_source_identity_sha256"]
            or build["build_source"]["extracted_source_seal_sha256"]
            != sha256_file(extracted_source_seal_path)
            or build["build_source"]["archive_sha256"] != current["archive_sha256"]
            or build["build_source"]["file_manifest_sha256"]
            != current["file_manifest_sha256"]
        ):
            raise GateError("current source seals differ from controlled build provenance")
    return {
        "status": "pass",
        "extracted_source_seal_sha256": sha256_file(extracted_source_seal_path),
        "file_manifest_sha256": current["file_manifest_sha256"],
        "archive_sha256": current["archive_sha256"],
    }


def extract_git_archive(
    repo: Path,
    archive_path: Path,
    source_root: Path,
    live_source_seal_path: Path,
) -> dict[str, Any]:
    live = validate_source_seal_document(load_json(live_source_seal_path))
    check_source_seal(repo, live_source_seal_path)
    if source_root.exists() or source_root.is_symlink():
        raise GateError("fresh extracted build-source destination already exists")
    if not source_root.is_absolute() or not source_root.parent.is_dir():
        raise GateError("extracted build-source destination must have an absolute existing parent")
    repo = repo.resolve(strict=True)
    if source_root == repo or source_root.is_relative_to(repo):
        raise GateError("extracted build-source destination must be outside the live worktree")
    if not archive_path.is_absolute() or archive_path.is_symlink() or not archive_path.is_file():
        raise GateError("Git source archive must be an absolute non-symlink regular file")
    if archive_path.stat().st_mode & 0o222:
        raise GateError("Git source archive must be non-writable before extraction")
    if archive_embedded_commit(archive_path) != live["git_head"]:
        raise GateError("Git source archive does not embed the sealed HEAD commit")
    expected = git_head_file_inventory(repo)
    object_format = git_output(repo, "rev-parse", "--show-object-format")
    expected_directories = {
        parent.as_posix()
        for path in expected
        for parent in Path(path).parents
        if parent != Path(".")
    }
    members: dict[str, tarfile.TarInfo] = {}
    with tarfile.open(archive_path, mode="r:") as archive:
        for member in archive.getmembers():
            relative = Path(member.name)
            if (
                relative.is_absolute()
                or not relative.parts
                or any(part in {"", ".", ".."} for part in relative.parts)
                or relative.as_posix() != member.name.rstrip("/")
            ):
                raise GateError(f"Git archive contains an unsafe path: {member.name!r}")
            path = relative.as_posix()
            if path in members:
                raise GateError(f"Git archive contains a duplicate path: {path}")
            if member.isdir():
                if path not in expected_directories:
                    raise GateError(f"Git archive contains an unexpected directory: {path}")
            elif member.isfile():
                expected_entry = expected.get(path)
                if expected_entry is None:
                    raise GateError(f"Git archive contains a file outside Git HEAD: {path}")
                normalized_mode = "100755" if member.mode & 0o111 else "100644"
                if normalized_mode != expected_entry["mode"]:
                    raise GateError(f"Git archive file mode differs from Git HEAD: {path}")
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise GateError(f"Git archive file cannot be decoded: {path}")
                data = extracted.read()
                if git_blob_object_id(data, object_format) != expected_entry["object_id"]:
                    raise GateError(f"Git archive file content differs from Git HEAD: {path}")
            else:
                raise GateError(f"Git archive contains a link or non-regular entry: {path}")
            members[path] = member
        archived_files = {path for path, member in members.items() if member.isfile()}
        if archived_files != set(expected):
            missing = sorted(set(expected) - archived_files)
            raise GateError(f"Git archive is missing Git HEAD paths: {missing[:8]!r}")

        source_root.mkdir(mode=0o755)
        for directory in sorted(expected_directories, key=lambda path: (path.count("/"), path)):
            (source_root / directory).mkdir(mode=0o755)
        for path in sorted(expected):
            member = members[path]
            extracted = archive.extractfile(member)
            if extracted is None:
                raise GateError(f"Git archive file cannot be decoded: {path}")
            destination = source_root / path
            with destination.open("xb") as output:
                while block := extracted.read(1024 * 1024):
                    output.write(block)
            destination.chmod(0o555 if expected[path]["mode"] == "100755" else 0o444)
    directories = [source_root, *(path for path in source_root.rglob("*") if path.is_dir())]
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        directory.chmod(directory.stat().st_mode & ~0o222)
    return extracted_source_seal(repo, source_root, archive_path, live_source_seal_path)


def record_build_provenance(
    repo: Path,
    target_dir: Path,
    source_seal_path: Path,
    build_source_path: Path,
    source_archive_path: Path,
    extracted_source_seal_path: Path,
    system_binary: Path,
    jemalloc_binary: Path,
    query_binary: Path,
    storage_verify_binary: Path,
    system_log: Path,
    jemalloc_log: Path,
    plan_path: Path,
    phase1_expectations: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    if not repo.is_absolute() or repo.is_symlink() or not repo.is_dir():
        raise GateError("controlled build repository must be an absolute non-symlink directory")
    if git_output(repo, "rev-parse", "--show-toplevel") != str(repo):
        raise GateError("controlled build repository is not the worktree root")
    checked_source = check_source_seal(repo, source_seal_path)
    source_document = validate_source_seal_document(load_json(source_seal_path))
    checked_extracted = check_extracted_source_seal(
        repo,
        build_source_path,
        source_archive_path,
        source_seal_path,
        extracted_source_seal_path,
    )
    extracted_document = validate_extracted_source_seal_document(
        load_json(extracted_source_seal_path)
    )
    tracked_inputs = audit_tracked_build_inputs(repo)
    status = git_output(repo, "status", "--porcelain=v2", "--untracked-files=normal")
    if status:
        raise GateError("controlled allocator builds require one clean source worktree")
    head = git_output(repo, "rev-parse", "HEAD")
    head_tree = git_output(repo, "rev-parse", "HEAD^{tree}")
    index_tree = git_output(repo, "write-tree")
    if head_tree != index_tree:
        raise GateError("controlled build index tree differs from the HEAD tree")
    if extracted_document["git_head"] != head or extracted_document["git_head_tree"] != head_tree:
        raise GateError("extracted build source differs from the controlled Git commit/tree")
    cargo_lock = repo / "Cargo.lock"
    if not cargo_lock.is_file() or cargo_lock.is_symlink():
        raise GateError("controlled build requires a regular Cargo.lock")
    cargo_config = repo / ".cargo/config.toml"
    if not cargo_config.is_file() or cargo_config.is_symlink():
        raise GateError("controlled build requires the tracked .cargo/config.toml")
    if git_output(repo, "ls-files", "--error-unmatch", ".cargo/config.toml") != ".cargo/config.toml":
        raise GateError("controlled Cargo configuration is not tracked")
    if [item["path"] for item in source_document["tracked_cargo_configs"]] != [
        ".cargo/config.toml"
    ]:
        raise GateError("controlled build requires exactly one tracked root .cargo/config.toml")
    cargo_configuration = tomllib.loads(cargo_config.read_text(encoding="utf-8"))
    tracked_rustflags = cargo_configuration.get("build", {}).get("rustflags")
    if tracked_rustflags != "-C target-cpu=native":
        raise GateError("tracked Cargo rustflags differ from the frozen native build")
    if not target_dir.is_absolute() or target_dir.is_symlink() or not target_dir.is_dir():
        raise GateError("controlled build target directory is invalid")
    target_dir = target_dir.resolve(strict=True)
    build_source_path = build_source_path.resolve(strict=True)
    if target_dir == build_source_path or target_dir.is_relative_to(build_source_path):
        raise GateError("controlled build target directory must be outside extracted source")
    if target_dir == repo or target_dir.is_relative_to(repo):
        raise GateError("controlled build target directory must be outside the live worktree")
    home = Path.home()
    build_path = (
        f"{home}/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    )
    rustc = home / ".cargo/bin/rustc"
    rustdoc = home / ".cargo/bin/rustdoc"
    expected_environment_line = (
        f"ENV\tHOME={home}\tPATH={build_path}\tCARGO_HOME={home}/.cargo\t"
        f"RUSTUP_HOME={home}/.rustup\tRUSTC={rustc}\tRUSTDOC={rustdoc}\t"
        f"LC_ALL=C\tTZ=UTC\tCARGO_INCREMENTAL=0\tCARGO_TARGET_DIR={target_dir}"
    )
    for log, command in (
        (system_log, plan["build_contract"]["system_command"]),
        (jemalloc_log, plan["build_contract"]["jemalloc_command"]),
    ):
        lines = log.read_text(encoding="utf-8", errors="strict").splitlines()
        if not lines or lines[0] != f"COMMAND\t{command}":
            raise GateError(f"controlled build log does not bind exact command: {log}")
        if len(lines) < 2 or lines[1] != f"CWD\t{build_source_path}":
            raise GateError(f"controlled build log does not bind extracted-source CWD: {log}")
        if len(lines) < 3 or lines[2] != expected_environment_line:
            raise GateError(f"controlled build log does not bind sanitized environment: {log}")
    binary_sha256 = {
        "system": executable_sha256(system_binary),
        "jemalloc": executable_sha256(jemalloc_binary),
        "query": executable_sha256(query_binary),
        "storage_verify": executable_sha256(storage_verify_binary),
    }
    if binary_sha256["system"] == binary_sha256["jemalloc"]:
        raise GateError("controlled system and jemalloc binaries have identical hashes")
    return {
        "schema": BUILD_PROVENANCE_SCHEMA,
        "git_head": head,
        "git_head_tree": head_tree,
        "git_index_tree": index_tree,
        "source_worktree_clean": True,
        "source_seal_sha256": checked_source["source_seal_sha256"],
        "source_identity_sha256": source_document["identity_sha256"],
        "build_source": {
            "mode": "read-only git archive HEAD extraction",
            "root": str(build_source_path),
            "archive_path": str(source_archive_path.resolve(strict=True)),
            "archive_sha256": checked_extracted["archive_sha256"],
            "archive_size_bytes": extracted_document["archive_size_bytes"],
            "archive_embedded_commit": extracted_document["archive_embedded_commit"],
            "extracted_source_seal_sha256": checked_extracted[
                "extracted_source_seal_sha256"
            ],
            "file_manifest_sha256": checked_extracted["file_manifest_sha256"],
            "file_count": extracted_document["file_count"],
            "directory_count": extracted_document["directory_count"],
            "total_file_bytes": extracted_document["total_file_bytes"],
            "archive_tree_equivalent": True,
            "all_entries_non_writable": True,
            "cargo_configuration_exact": True,
            "manifest_path_reference_count": extracted_document[
                "manifest_path_reference_count"
            ],
            "all_manifest_paths_within_source": True,
            "live_worktree_used_as_build_source": False,
        },
        **tracked_inputs,
        "cargo_lock_sha256": sha256_file(cargo_lock),
        "tracked_cargo_config_sha256": sha256_file(cargo_config),
        "tracked_cargo_rustflags": tracked_rustflags,
        "target_dir": str(target_dir),
        "controlled_environment": {
            "HOME": str(home),
            "PATH": build_path,
            "CARGO_HOME": f"{home}/.cargo",
            "RUSTUP_HOME": f"{home}/.rustup",
            "RUSTC": str(rustc),
            "RUSTDOC": str(rustdoc),
            "LC_ALL": "C",
            "TZ": "UTC",
            "CARGO_INCREMENTAL": "0",
            "CARGO_TARGET_DIR": str(target_dir),
            "ambient_rustflags": False,
            "ambient_allocator_configuration": False,
        },
        "build_commands": {
            "system": plan["build_contract"]["system_command"],
            "jemalloc": plan["build_contract"]["jemalloc_command"],
        },
        "build_log_sha256": {
            "system": sha256_file(system_log),
            "jemalloc": sha256_file(jemalloc_log),
        },
        "binary_sha256": binary_sha256,
        "jemalloc_stats_enabled": True,
        "screen_jemalloc_feature": plan["build_contract"]["screen_jemalloc_feature"],
        "later_no_stats_jemalloc_feature": plan["build_contract"][
            "later_no_stats_jemalloc_feature"
        ],
        "later_no_stats_revalidation_command": plan["build_contract"][
            "later_no_stats_revalidation_command"
        ],
        "no_stats_production_build_validated": False,
    }


def validate_build_provenance(value: Any) -> dict[str, Any]:
    provenance = require_exact_keys(
        value,
        {
            "schema",
            "git_head",
            "git_head_tree",
            "git_index_tree",
            "source_worktree_clean",
            "source_seal_sha256",
            "source_identity_sha256",
            "build_source",
            "tracked_input_count",
            "tracked_input_manifest_sha256",
            "git_index_flags_clear",
            "tracked_inputs_regular_files",
            "cargo_lock_sha256",
            "tracked_cargo_config_sha256",
            "tracked_cargo_rustflags",
            "target_dir",
            "controlled_environment",
            "build_commands",
            "build_log_sha256",
            "binary_sha256",
            "jemalloc_stats_enabled",
            "screen_jemalloc_feature",
            "later_no_stats_jemalloc_feature",
            "later_no_stats_revalidation_command",
            "no_stats_production_build_validated",
        },
        "$.build_provenance",
    )
    if provenance["schema"] != BUILD_PROVENANCE_SCHEMA:
        raise GateError("build provenance schema mismatch")
    for key in (
        "git_head",
        "git_head_tree",
        "git_index_tree",
        "cargo_lock_sha256",
        "tracked_cargo_config_sha256",
        "tracked_input_manifest_sha256",
        "source_seal_sha256",
        "source_identity_sha256",
    ):
        if not isinstance(provenance[key], str) or re.fullmatch(r"[0-9a-f]{40,64}", provenance[key]) is None:
            raise GateError(f"build provenance {key} is invalid")
    if provenance["git_head_tree"] != provenance["git_index_tree"]:
        raise GateError("build provenance does not bind one clean tree")
    if provenance["source_worktree_clean"] is not True:
        raise GateError("build provenance does not prove a clean worktree")
    build_source = require_exact_keys(
        provenance["build_source"],
        {
            "mode",
            "root",
            "archive_path",
            "archive_sha256",
            "archive_size_bytes",
            "archive_embedded_commit",
            "extracted_source_seal_sha256",
            "file_manifest_sha256",
            "file_count",
            "directory_count",
            "total_file_bytes",
            "archive_tree_equivalent",
            "all_entries_non_writable",
            "cargo_configuration_exact",
            "manifest_path_reference_count",
            "all_manifest_paths_within_source",
            "live_worktree_used_as_build_source",
        },
        "$.build_provenance.build_source",
    )
    if build_source["mode"] != "read-only git archive HEAD extraction":
        raise GateError("build provenance does not use the frozen extracted-source mode")
    for key in ("root", "archive_path"):
        if not isinstance(build_source[key], str) or not Path(build_source[key]).is_absolute():
            raise GateError(f"build provenance build-source {key} is not absolute")
    for key in ("archive_sha256", "extracted_source_seal_sha256", "file_manifest_sha256"):
        if not isinstance(build_source[key], str) or re.fullmatch(r"[0-9a-f]{64}", build_source[key]) is None:
            raise GateError(f"build provenance build-source {key} is invalid")
    if build_source["archive_embedded_commit"] != provenance["git_head"]:
        raise GateError("build provenance source archive does not embed Git HEAD")
    for key in ("archive_size_bytes", "file_count", "directory_count", "total_file_bytes"):
        strict_int(build_source[key], f"$.build_provenance.build_source.{key}", minimum=1)
    strict_int(
        build_source["manifest_path_reference_count"],
        "$.build_provenance.build_source.manifest_path_reference_count",
        minimum=0,
    )
    if (
        build_source["archive_tree_equivalent"] is not True
        or build_source["all_entries_non_writable"] is not True
        or build_source["cargo_configuration_exact"] is not True
        or build_source["all_manifest_paths_within_source"] is not True
        or build_source["live_worktree_used_as_build_source"] is not False
    ):
        raise GateError("build provenance permits a mutable or live-worktree build source")
    strict_int(
        provenance["tracked_input_count"],
        "$.build_provenance.tracked_input_count",
        minimum=1,
    )
    if (
        provenance["git_index_flags_clear"] is not True
        or provenance["tracked_inputs_regular_files"] is not True
    ):
        raise GateError("build provenance permits hidden or non-regular inputs")
    if provenance["tracked_cargo_rustflags"] != "-C target-cpu=native":
        raise GateError("build provenance tracked Cargo rustflags differ")
    hashes = require_exact_keys(
        provenance["binary_sha256"],
        {"system", "jemalloc", "query", "storage_verify"},
        "$.build_provenance.binary_sha256",
    )
    for role, digest in hashes.items():
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise GateError(f"build provenance binary hash is invalid for {role}")
    if hashes["system"] == hashes["jemalloc"]:
        raise GateError("build provenance allocator binary hashes are identical")
    target_dir = provenance["target_dir"]
    if not isinstance(target_dir, str) or not Path(target_dir).is_absolute():
        raise GateError("build provenance target directory is not absolute")
    target_path = Path(target_dir)
    source_path = Path(build_source["root"])
    if target_path == source_path or target_path.is_relative_to(source_path):
        raise GateError("build provenance target directory overlaps extracted source")
    controlled_environment = require_exact_keys(
        provenance["controlled_environment"],
        {
            "HOME",
            "PATH",
            "CARGO_HOME",
            "RUSTUP_HOME",
            "RUSTC",
            "RUSTDOC",
            "LC_ALL",
            "TZ",
            "CARGO_INCREMENTAL",
            "CARGO_TARGET_DIR",
            "ambient_rustflags",
            "ambient_allocator_configuration",
        },
        "$.build_provenance.controlled_environment",
    )
    build_home = controlled_environment["HOME"]
    if not isinstance(build_home, str) or not Path(build_home).is_absolute():
        raise GateError("build provenance HOME is not absolute")
    expected_path = (
        f"{build_home}/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    )
    if controlled_environment != {
        "HOME": build_home,
        "PATH": expected_path,
        "CARGO_HOME": f"{build_home}/.cargo",
        "RUSTUP_HOME": f"{build_home}/.rustup",
        "RUSTC": f"{build_home}/.cargo/bin/rustc",
        "RUSTDOC": f"{build_home}/.cargo/bin/rustdoc",
        "LC_ALL": "C",
        "TZ": "UTC",
        "CARGO_INCREMENTAL": "0",
        "CARGO_TARGET_DIR": target_dir,
        "ambient_rustflags": False,
        "ambient_allocator_configuration": False,
    }:
        raise GateError("build provenance environment differs from the sanitized contract")
    commands = require_exact_keys(
        provenance["build_commands"], {"system", "jemalloc"}, "$.build_provenance.build_commands"
    )
    if commands != {
        "system": (
            "cargo build --manifest-path Cargo.toml --locked --release "
            "--no-default-features -p chronoxide-ingester -p chronoxide-query-cli "
            "--bin chronoxide-ingester --bin chronoxide-query --bin chronoxide-storage-verify"
        ),
        "jemalloc": (
            "cargo build --manifest-path Cargo.toml --locked --release "
            "--no-default-features --features jemalloc-stats "
            "-p chronoxide-ingester --bin chronoxide-ingester"
        ),
    }:
        raise GateError("build provenance commands differ from the controlled contract")
    for section_name in ("build_log_sha256",):
        section = require_exact_keys(
            provenance[section_name], {"system", "jemalloc"}, f"$.build_provenance.{section_name}"
        )
        for role, digest in section.items():
            if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise GateError(f"build provenance {section_name} hash is invalid for {role}")
    if provenance["jemalloc_stats_enabled"] is not True:
        raise GateError("this screen requires the explicitly stats-enabled jemalloc build")
    if provenance["screen_jemalloc_feature"] != "jemalloc-stats":
        raise GateError("screen build provenance must use the diagnostic jemalloc-stats feature")
    if provenance["later_no_stats_jemalloc_feature"] != "jemalloc":
        raise GateError("build provenance lost the later no-stats jemalloc feature path")
    if provenance["later_no_stats_revalidation_command"] != (
        "cargo build --manifest-path Cargo.toml --locked --release "
        "--no-default-features --features jemalloc "
        "-p chronoxide-ingester --bin chronoxide-ingester"
    ):
        raise GateError("build provenance lost the later no-stats revalidation command")
    if provenance["no_stats_production_build_validated"] is not False:
        raise GateError("250k screen must not claim no-stats production validation")
    return provenance


def validate_executable_set(
    build_provenance_path: Path,
    system_binary_path: Path,
    jemalloc_binary_path: Path,
    query_binary_path: Path,
    storage_verify_binary_path: Path,
) -> dict[str, Any]:
    build = validate_build_provenance(load_json(build_provenance_path))
    binaries = {
        "system": system_binary_path,
        "jemalloc": jemalloc_binary_path,
        "query": query_binary_path,
        "storage_verify": storage_verify_binary_path,
    }
    observed = {role: executable_sha256(path) for role, path in binaries.items()}
    if observed != build["binary_sha256"]:
        differing = sorted(
            role
            for role in observed
            if observed[role] != build["binary_sha256"][role]
        )
        raise GateError(f"preserved executable seal changed: {differing!r}")
    return {
        "status": "pass",
        "build_provenance_sha256": sha256_file(build_provenance_path),
        "binary_sha256": observed,
        "all_preserved_binaries_non_writable": True,
    }


CALIBRATION_EVIDENCE_FILES = {
    "FROZEN_BEFORE_MEASURED_SCHEDULE",
    "calibration.json",
    "capture-residency-before.tsv",
    "config-render.json",
    "corpus-summary.json",
    "external-conflict-guardian.exit-status",
    "external-conflict-guardian-control.json",
    "external-conflict-guardian-launch",
    "external-conflict-guardian-ready",
    "external-conflict-guardian.json",
    "external-conflict-guardian.log",
    "processes-after.txt",
    "processes-before.txt",
    "raw-inputs.sha256",
    "readbacks.log",
    "readbacks.md",
    "replay-correctness.json",
    "replay.exit-status",
    "replay.log",
    "segments.sha256",
    "segments.tsv",
    "storage-verify.json",
    "storage-verify.log",
    "writeback-quiescence-samples.tsv",
    "writeback-quiescence.json",
    "writeback-quiescence.log",
}
RUN_EVIDENCE_FILES = {
    "allocator-release-checkpoint.tsv",
    "allocator-release-summary.json",
    "allocator-release-telemetry.ndjson",
    "allocator-runtime-log.json",
    "allocator-telemetry-summary.json",
    "capture-residency-after.tsv",
    "capture-residency-before.tsv",
    "config-render.json",
    "corpus-summary.json",
    "external-conflict-guardian.exit-status",
    "external-conflict-guardian-control.json",
    "external-conflict-guardian-launch",
    "external-conflict-guardian-ready",
    "external-conflict-guardian.json",
    "external-conflict-guardian.log",
    "observation.json",
    "perf-stat.json",
    "perf-stat.tsv",
    "pre-run-writeback-quiescence-samples.tsv",
    "pre-run-writeback-quiescence.json",
    "pre-run-writeback-quiescence.log",
    "pressure-after.txt",
    "pressure-before.txt",
    "processes-after.txt",
    "processes-before.txt",
    "processes-immediately-before-launch.txt",
    "replay-correctness.json",
    "replay.exit-status",
    "replay.log",
    "replay.time.json",
    "replay.time.txt",
    "rss-monitor.exit-status",
    "rss-monitor.log",
    "rss-monitor-ready",
    "rss-samples.tsv",
    "rss-summary.json",
    "segments.sha256",
    "segments.tsv",
    "writeback-quiescence-samples.tsv",
    "writeback-quiescence.json",
    "writeback-quiescence.log",
}
VALIDATION_EVIDENCE_FILES = {
    "processes-after.txt",
    "processes-before-readbacks.txt",
    "processes-before-storage-verify.txt",
    "readbacks.log",
    "readbacks.md",
    "readbacks.time.txt",
    "storage-verify.json",
    "storage-verify.log",
    "storage-verify.time.txt",
    "validation-summary.json",
}
PROFILE_COMMON_EVIDENCE_FILES = {
    "config-render.json",
    "corpus-summary.json",
    "external-conflict-guardian.exit-status",
    "external-conflict-guardian-control.json",
    "external-conflict-guardian-launch",
    "external-conflict-guardian-ready",
    "external-conflict-guardian.json",
    "external-conflict-guardian.log",
    "lost-events.txt",
    "processes-before.txt",
    "profile-evidence.json",
    "readbacks.log",
    "readbacks.md",
    "replay-correctness.json",
    "replay.exit-status",
    "replay.log",
    "segments.sha256",
    "segments.tsv",
    "storage-verify.json",
    "storage-verify.log",
}
PROFILE_HEAPTRACK_EVIDENCE_FILES = PROFILE_COMMON_EVIDENCE_FILES | {
    "heaptrack-print.log",
    "heaptrack-stacks.txt",
    "heaptrack-summary.txt",
    "heaptrack.log",
    "heaptrack.trace.zst",
}
PROFILE_PERF_EVIDENCE_FILES = PROFILE_COMMON_EVIDENCE_FILES | {
    "perf-record.log",
    "perf-report.log",
    "perf-script.log",
    "perf-script.txt",
    "perf-summary.txt",
    "perf.data",
}
PROFILE_PERF_SELECTED_EVIDENCE_FILES = PROFILE_PERF_EVIDENCE_FILES | {
    "selected-preflight.json",
    "selected-preflight.stderr",
    "selected-preflight.stdout",
    "selected-runtime-combined.log",
    "selected-runtime-policy.json",
}


def fail_closed_tree_inventory(
    root_path: Path, *, excluded_subtrees: set[str] | None = None
) -> tuple[list[str], list[Path]]:
    if (
        not root_path.is_absolute()
        or root_path.is_symlink()
        or not root_path.is_dir()
    ):
        raise GateError("evidence root must be an absolute non-symlink directory")
    root = root_path.resolve(strict=True)
    excluded = excluded_subtrees or set()
    for relative_text in excluded:
        relative = Path(relative_text)
        if (
            relative.is_absolute()
            or not relative.parts
            or relative.as_posix() != relative_text
            or any(part in {"", ".", ".."} for part in relative.parts)
        ):
            raise GateError(f"unsafe excluded evidence subtree: {relative_text!r}")
    directories: list[str] = []
    files: list[Path] = []

    def visit(directory: Path, relative: Path) -> None:
        try:
            with os.scandir(directory) as scanner:
                entries = sorted(scanner, key=lambda item: os.fsencode(item.name))
        except OSError as error:
            raise GateError(f"cannot enumerate evidence directory: {directory}") from error
        for entry in entries:
            if any(character in entry.name for character in ("\n", "\r", "\t")):
                raise GateError(f"unsafe evidence path component: {entry.name!r}")
            path = directory / entry.name
            child_relative = relative / entry.name
            try:
                metadata = path.lstat()
            except OSError as error:
                raise GateError(f"cannot inspect evidence path: {path}") from error
            relative_text = child_relative.as_posix()
            if stat.S_ISLNK(metadata.st_mode):
                raise GateError(f"evidence tree contains a symlink: {relative_text}")
            if stat.S_ISDIR(metadata.st_mode):
                directories.append(relative_text)
                if relative_text not in excluded:
                    visit(path, child_relative)
            elif stat.S_ISREG(metadata.st_mode):
                files.append(path)
            else:
                raise GateError(
                    f"evidence tree contains a non-regular entry: {relative_text}"
                )

    visit(root, Path())
    directories.sort(key=os.fsencode)
    files.sort(key=lambda path: os.fsencode(path.relative_to(root).as_posix()))
    return directories, files


def validate_evidence_tree_shape(
    root: Path, kind: str, directories: list[str], files: list[Path]
) -> None:
    relative_files = {path.relative_to(root).as_posix() for path in files}
    if kind == "calibration":
        expected = CALIBRATION_EVIDENCE_FILES
    elif kind == "run":
        expected = RUN_EVIDENCE_FILES
    elif kind == "validation":
        expected = VALIDATION_EVIDENCE_FILES
    elif kind == "profile-heaptrack":
        expected = PROFILE_HEAPTRACK_EVIDENCE_FILES
    elif kind == "profile-perf-system":
        expected = PROFILE_PERF_EVIDENCE_FILES
    elif kind == "profile-perf-selected":
        expected = PROFILE_PERF_SELECTED_EVIDENCE_FILES
    else:
        raise GateError(f"unknown immutable evidence-tree kind: {kind}")
    reports = sorted(
        path
        for path in relative_files
        if "/" not in path and re.fullmatch(r"ingestion_stats_[A-Za-z0-9_.-]+\.md", path)
    )
    corpus_kinds = {
        "calibration",
        "run",
        "profile-heaptrack",
        "profile-perf-system",
        "profile-perf-selected",
    }
    if kind in corpus_kinds:
        if len(reports) != 1:
            raise GateError(f"{kind} evidence must contain exactly one ingestion report")
        allowed_root_files = expected | {reports[0]}
        if "segments" not in directories:
            raise GateError(f"{kind} evidence lacks its segments directory")
        unexpected_directories = sorted(
            directory for directory in directories if not (
                directory == "segments" or directory.startswith("segments/")
            )
        )
        if unexpected_directories:
            raise GateError(
                f"{kind} evidence contains unexpected directories: {unexpected_directories!r}"
            )
        observed_root_files = {path for path in relative_files if "/" not in path}
        segment_files = {path for path in relative_files if path.startswith("segments/")}
        if not segment_files:
            raise GateError(f"{kind} evidence segment corpus is empty")
        unexpected_nested = sorted(
            path for path in relative_files if "/" in path and not path.startswith("segments/")
        )
        if unexpected_nested:
            raise GateError(
                f"{kind} evidence contains unexpected nested files: {unexpected_nested!r}"
            )
    else:
        allowed_root_files = expected
        observed_root_files = relative_files
        if directories:
            raise GateError(
                f"validation evidence contains unexpected directories: {directories!r}"
            )
    missing = sorted(allowed_root_files - observed_root_files)
    extra = sorted(observed_root_files - allowed_root_files)
    if missing or extra:
        raise GateError(
            f"{kind} evidence matrix differs; missing={missing!r}, extra={extra!r}"
        )


def recompute_corpus_artifacts(evidence_root: Path) -> dict[str, Any]:
    corpus = evidence_root / "segments"
    directories, files = fail_closed_tree_inventory(corpus)
    del directories
    rows: list[tuple[str, int, str]] = []
    for path in files:
        relative = path.relative_to(corpus).as_posix()
        rows.append((sha256_file(path), path.stat().st_size, relative))
    if not rows:
        raise GateError("segment corpus contains no regular files")
    manifest = "".join(
        f"{digest}  ./{relative}\n" for digest, _size, relative in rows
    ).encode()
    inventory = (
        "sha256\tsize_bytes\tpath\n"
        + "".join(
            f"{digest}\t{size}\t{relative}\n" for digest, size, relative in rows
        )
    ).encode()
    summary = {
        "schema": phase1.CORPUS_SUMMARY_SCHEMA,
        "file_count": len(rows),
        "size_bytes": sum(size for _digest, size, _relative in rows),
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
    }
    if (evidence_root / "segments.sha256").read_bytes() != manifest:
        raise GateError("segment manifest does not independently derive from payloads")
    if (evidence_root / "segments.tsv").read_bytes() != inventory:
        raise GateError("segment inventory does not independently derive from payloads")
    if load_json(evidence_root / "corpus-summary.json") != summary:
        raise GateError("corpus summary does not independently derive from payloads")
    return summary


def recompute_replay_correctness(evidence_root: Path) -> dict[str, Any]:
    reports = sorted(evidence_root.glob("ingestion_stats_*.md"))
    if len(reports) != 1 or reports[0].is_symlink() or not reports[0].is_file():
        raise GateError("evidence must contain exactly one regular ingestion report")
    recomputed = report_gate.parse_replay_report(reports[0])
    if load_json(evidence_root / "replay-correctness.json") != recomputed:
        raise GateError("replay correctness does not independently derive from raw report")
    return recomputed


def create_immutable_tree_seal(root_path: Path, output_path: Path, kind: str) -> dict[str, Any]:
    if output_path.exists() or output_path.is_symlink():
        raise GateError("refusing to reuse immutable tree-seal output")
    root = root_path.resolve(strict=True)
    directories, files = fail_closed_tree_inventory(root)
    validate_evidence_tree_shape(root, kind, directories, files)
    if kind in {
        "calibration",
        "run",
        "profile-heaptrack",
        "profile-perf-system",
        "profile-perf-selected",
    }:
        recompute_corpus_artifacts(root)
        recompute_replay_correctness(root)
    for path in files:
        path.chmod(0o444)
    for relative in sorted(directories, key=lambda value: len(Path(value).parts), reverse=True):
        (root / relative).chmod(0o555)
    root.chmod(0o555)
    directories, files = fail_closed_tree_inventory(root)
    validate_evidence_tree_shape(root, kind, directories, files)
    seal = {
        "schema": IMMUTABLE_TREE_SEAL_SCHEMA,
        "kind": kind,
        "root": str(root),
        "directories": [{"path": path, "mode": "0555"} for path in directories],
        "files": [
            {
                "path": path.relative_to(root).as_posix(),
                "mode": "0444",
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for path in files
        ],
    }
    write_json_exclusive(output_path, seal)
    output_path.chmod(0o444)
    validate_immutable_tree_seal(root, output_path, kind)
    return seal


def validate_immutable_tree_seal(
    root_path: Path, seal_path: Path, expected_kind: str | None = None
) -> dict[str, Any]:
    if seal_path.is_symlink() or not seal_path.is_file():
        raise GateError("immutable tree seal must be a regular non-symlink file")
    if stat.S_IMODE(seal_path.stat().st_mode) != 0o444:
        raise GateError("immutable tree seal must have exact mode 0444")
    seal = require_exact_keys(
        load_json(seal_path),
        {"schema", "kind", "root", "directories", "files"},
        "$immutable_tree_seal",
    )
    if seal["schema"] != IMMUTABLE_TREE_SEAL_SCHEMA:
        raise GateError("immutable tree seal schema mismatch")
    if expected_kind is not None and seal["kind"] != expected_kind:
        raise GateError("immutable tree seal kind mismatch")
    root = root_path.resolve(strict=True)
    if seal["root"] != str(root) or stat.S_IMODE(root.stat().st_mode) != 0o555:
        raise GateError("immutable tree seal root identity or mode changed")
    directories, files = fail_closed_tree_inventory(root)
    validate_evidence_tree_shape(root, seal["kind"], directories, files)
    expected_directories = [{"path": path, "mode": "0555"} for path in directories]
    if seal["directories"] != expected_directories:
        raise GateError("immutable tree directory inventory changed")
    observed_files = []
    for path in files:
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode != 0o444:
            raise GateError(f"immutable evidence file mode changed: {path}")
        observed_files.append(
            {
                "path": path.relative_to(root).as_posix(),
                "mode": "0444",
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    if seal["files"] != observed_files:
        raise GateError("immutable tree file inventory or content changed")
    if seal["kind"] in {
        "calibration",
        "run",
        "profile-heaptrack",
        "profile-perf-system",
        "profile-perf-selected",
    }:
        recompute_corpus_artifacts(root)
        recompute_replay_correctness(root)
    return {
        "status": "pass",
        "kind": seal["kind"],
        "root": str(root),
        "directory_count": len(directories),
        "file_count": len(files),
        "seal_sha256": sha256_file(seal_path),
    }


def validate_quiescence_evidence(samples_path: Path, summary_path: Path) -> dict[str, Any]:
    summary = load_json(summary_path)
    lines = samples_path.read_text(encoding="utf-8", errors="strict").splitlines()
    if lines[:1] != ["elapsed_ns\tdirty_kib\twriteback_kib\ttotal_kib\twithin_limit"]:
        raise GateError("writeback-quiescence sample header changed")
    rows: list[tuple[int, int, int, int, bool]] = []
    for line_number, line in enumerate(lines[1:], start=2):
        fields = line.split("\t")
        if len(fields) != 5 or not all(
            re.fullmatch(r"[0-9]+", value) for value in fields[:4]
        ) or fields[4] not in {"true", "false"}:
            raise GateError(f"writeback-quiescence sample row {line_number} is malformed")
        elapsed, dirty, writeback, total = (int(value) for value in fields[:4])
        if dirty + writeback != total:
            raise GateError("writeback-quiescence total does not derive from raw counters")
        rows.append((elapsed, dirty, writeback, total, fields[4] == "true"))
    if not rows:
        raise GateError("writeback-quiescence evidence contains no samples")
    maximum = strict_int(
        summary.get("maximum_dirty_writeback_kib"),
        "$.quiescence.maximum_dirty_writeback_kib",
        minimum=0,
    )
    required = strict_int(
        summary.get("required_consecutive_samples"),
        "$.quiescence.required_consecutive_samples",
        minimum=1,
    )
    consecutive = 0
    for _elapsed, _dirty, _writeback, total, within in rows:
        if within != (total <= maximum):
            raise GateError("writeback-quiescence within-limit flag is not derived")
        consecutive = consecutive + 1 if within else 0
    last = rows[-1]
    expected = {
        "sample_count": len(rows),
        "final_dirty_kib": last[1],
        "final_writeback_kib": last[2],
        "final_total_kib": last[3],
        "passed": consecutive >= required,
    }
    for key, value in expected.items():
        if summary.get(key) != value:
            raise GateError(f"writeback-quiescence summary field is not derived: {key}")
    if summary.get("global_sync_called") is not True or summary.get("passed") is not True:
        raise GateError("writeback-quiescence evidence did not pass")
    return summary


def revalidate_screen_from_raw(
    result_root: Path, plan_path: Path, phase1_expectations: Path
) -> dict[str, Any]:
    if (
        not result_root.is_absolute()
        or result_root.is_symlink()
        or not result_root.is_dir()
    ):
        raise GateError("screen result root must be an absolute non-symlink directory")
    root = result_root.resolve(strict=True)
    plan = validate_plan(plan_path, phase1_expectations)
    expected_phase1 = load_json(phase1_expectations)
    expected_phase1_corpus_bytes = strict_int(
        expected_phase1.get("corpus", {}).get("size_bytes")
        if isinstance(expected_phase1, dict)
        and isinstance(expected_phase1.get("corpus"), dict)
        else None,
        "$.phase1_expectations.corpus.size_bytes",
        minimum=1,
    )
    metadata = root / "metadata"
    authorities = metadata / "raw-authorities"
    calibration_root = root / "calibration"
    validation_root = root / "validation"
    for control_name in ("core-controls.json", "measurement-controls.json"):
        check_control_seal(metadata / control_name)
    source_seal_path = metadata / "source/formal-source-seal.json"
    source_seal_document = validate_source_seal_document(load_json(source_seal_path))
    repo = Path(source_seal_document["repo"])
    check_source_seal(repo, source_seal_path)
    check_extracted_source_seal(
        repo,
        root / "build-source",
        metadata / "source/git-head.tar",
        source_seal_path,
        metadata / "source/extracted-build-source-seal.json",
        metadata / "build-provenance.json",
    )
    validate_executable_set(
        metadata / "build-provenance.json",
        metadata / "binaries/chronoxide-ingester-system",
        metadata / "binaries/chronoxide-ingester-jemalloc",
        metadata / "binaries/chronoxide-query",
        metadata / "binaries/chronoxide-storage-verify",
    )
    validate_capture_reinventory(
        metadata / "capture-inputs-before.json",
        metadata / "capture-inputs-after.json",
    )
    validate_immutable_tree_seal(
        calibration_root, authorities / "calibration.json", "calibration"
    )
    validate_immutable_tree_seal(
        validation_root, authorities / "validation.json", "validation"
    )
    for snapshot in ("processes-before.txt", "processes-after.txt"):
        validate_process_snapshot(calibration_root / snapshot, set())
    for snapshot in (
        "processes-before-storage-verify.txt",
        "processes-before-readbacks.txt",
        "processes-after.txt",
    ):
        validate_process_snapshot(validation_root / snapshot, set())
    build = metadata / "build-provenance.json"
    binaries = {
        "system": metadata / "binaries/chronoxide-ingester-system",
        "jemalloc": metadata / "binaries/chronoxide-ingester-jemalloc",
    }
    preflight_root = metadata / "preflight"
    for policy in POLICY_ORDER:
        binary_role = "system" if policy == "S" else "jemalloc"
        source_audit = None
        if policy != "S":
            source_audit = preflight_root / f"{policy}.source-audit.stderr"
        recomputed_preflight = parse_preflight(
            preflight_root / f"{policy}.stdout",
            preflight_root / f"{policy}.stderr",
            binaries[binary_role],
            plan_path,
            phase1_expectations,
            policy,
            source_audit,
        )
        if recomputed_preflight != load_json(preflight_root / f"{policy}.json"):
            raise GateError(f"{policy} preflight record is not derived from raw output")

    calibration = create_calibration(
        calibration_root / "storage-verify.json",
        calibration_root / "readbacks.md",
        calibration_root / "replay-correctness.json",
        calibration_root / "corpus-summary.json",
        build,
        plan_path,
        phase1_expectations,
    )
    if calibration != load_json(calibration_root / "calibration.json"):
        raise GateError("calibration record is not derived from raw evidence")
    validate_guardian_evidence(
        calibration_root / "external-conflict-guardian.json",
        calibration_root / "external-conflict-guardian-control.json",
        calibration_root / "external-conflict-guardian-ready",
        calibration_root / "external-conflict-guardian-launch",
        plan["environment_contract"]["external_conflict_poll_interval_ms"],
        root,
        expected_phase1_corpus_bytes * 10 // 4 + CAPACITY_RESERVE_BYTES,
        False,
    )
    validate_zero_exit_status(
        calibration_root / "replay.exit-status", "calibration replay exit status"
    )
    validate_zero_exit_status(
        calibration_root / "external-conflict-guardian.exit-status",
        "calibration guardian exit status",
    )
    validate_quiescence_evidence(
        calibration_root / "writeback-quiescence-samples.tsv",
        calibration_root / "writeback-quiescence.json",
    )

    observations: list[Path] = []
    for run_index, policy in enumerate(EXPECTED_SCHEDULE, start=1):
        label = f"run-{run_index:02d}-{policy}"
        run = root / "runs" / label
        validate_immutable_tree_seal(run, authorities / f"{label}.json", "run")
        for snapshot in (
            "processes-before.txt",
            "processes-immediately-before-launch.txt",
            "processes-after.txt",
        ):
            validate_process_snapshot(run / snapshot, set())
        validate_rss_release_evidence(
            run / "rss-samples.tsv",
            run / "rss-summary.json",
            run / "external-conflict-guardian-control.json",
            run / "rss-monitor-ready",
            run / "external-conflict-guardian-launch",
            plan["workload"]["rss_interval_ms"],
        )
        checkpoint = parse_checkpoint(
            run / "allocator-release-checkpoint.tsv",
            run / "rss-summary.json",
            plan_path,
            phase1_expectations,
        )
        if checkpoint != load_json(run / "allocator-release-summary.json"):
            raise GateError(f"{label} checkpoint summary is not derived")
        telemetry = parse_allocator_telemetry(
            run / "allocator-release-telemetry.ndjson",
            run / "allocator-release-checkpoint.tsv",
            run / "rss-samples.tsv",
            run / "rss-summary.json",
            plan_path,
            phase1_expectations,
            policy,
        )
        if telemetry != load_json(run / "allocator-telemetry-summary.json"):
            raise GateError(f"{label} allocator telemetry summary is not derived")
        runtime = gate_runtime_log(
            run / "replay.log",
            preflight_root / f"{policy}.json",
            plan_path,
            phase1_expectations,
            policy,
        )
        if runtime != load_json(run / "allocator-runtime-log.json"):
            raise GateError(f"{label} runtime policy is not derived from replay log")
        validate_quiescence_evidence(
            run / "pre-run-writeback-quiescence-samples.tsv",
            run / "pre-run-writeback-quiescence.json",
        )
        validate_quiescence_evidence(
            run / "writeback-quiescence-samples.tsv",
            run / "writeback-quiescence.json",
        )
        with tempfile.TemporaryDirectory(prefix="chronoxide-phase5-revalidate-") as temporary:
            temporary_root = Path(temporary)
            parsed_time = phase1.parse_gnu_time(
                run / "replay.time.txt", temporary_root / "time.json"
            )
            parsed_perf = phase1.parse_perf_stat(
                run / "perf-stat.tsv",
                temporary_root / "perf.json",
                EXPECTED_PERF_EVENTS,
            )
        if parsed_time != load_json(run / "replay.time.json"):
            raise GateError(f"{label} GNU-time summary is not derived")
        if parsed_perf != load_json(run / "perf-stat.json"):
            raise GateError(f"{label} perf-stat summary is not derived")
        observed = make_observation(
            run_index=run_index,
            policy_name=policy,
            plan_path=plan_path,
            phase1_expectations=phase1_expectations,
            build_provenance_path=build,
            preflight_path=preflight_root / f"{policy}.json",
            binary_path=binaries["system" if policy == "S" else "jemalloc"],
            runtime_policy_path=run / "allocator-runtime-log.json",
            allocator_telemetry_path=run / "allocator-telemetry-summary.json",
            checkpoint_path=run / "allocator-release-checkpoint.tsv",
            rss_path=run / "rss-summary.json",
            time_path=run / "replay.time.json",
            perf_path=run / "perf-stat.json",
            guardian_path=run / "external-conflict-guardian.json",
            pre_quiescence_path=run / "pre-run-writeback-quiescence.json",
            quiescence_path=run / "writeback-quiescence.json",
            correctness_path=run / "replay-correctness.json",
            corpus_path=run / "corpus-summary.json",
        )
        observation_path = run / "observation.json"
        if observed != load_json(observation_path):
            raise GateError(f"{label} observation is not derived from raw evidence")
        expected_reserve = (
            calibration["corpus"]["size_bytes"] * (10 - run_index)
            + CAPACITY_RESERVE_BYTES
        )
        if observed["external_conflict_guardian"].get(
            "minimum_free_bytes"
        ) != expected_reserve or observed["external_conflict_guardian"].get(
            "filesystem"
        ) != str(root):
            raise GateError(f"{label} continuous capacity reserve changed")
        observations.append(observation_path)

    reference = root / "runs/run-01-S"
    for observation_path in observations[1:]:
        run = observation_path.parent
        if (run / "segments.sha256").read_bytes() != (
            reference / "segments.sha256"
        ).read_bytes():
            raise GateError(f"{run.name} corpus differs from run-01-S")
        if (run / "replay-correctness.json").read_bytes() != (
            reference / "replay-correctness.json"
        ).read_bytes():
            raise GateError(f"{run.name} replay correctness differs from run-01-S")
    screen_summary = compare_screen(observations, plan_path, phase1_expectations)
    screen_summary_path = root / "comparisons/screen-summary.json"
    if screen_summary != load_json(screen_summary_path):
        raise GateError("screen summary is not derived from raw run evidence")
    validation = gate_validation(
        validation_root / "storage-verify.json",
        validation_root / "readbacks.md",
        reference / "replay-correctness.json",
        reference / "corpus-summary.json",
        calibration_root / "calibration.json",
        calibration_root / "storage-verify.json",
        calibration_root / "readbacks.md",
        calibration_root / "replay-correctness.json",
        calibration_root / "corpus-summary.json",
        build,
        plan_path,
        phase1_expectations,
    )
    validation_path = validation_root / "validation-summary.json"
    if validation != load_json(validation_path):
        raise GateError("validation summary is not derived from raw verifier/readbacks")
    final = seal_screen(
        observations,
        screen_summary_path,
        validation_path,
        validation_root / "storage-verify.json",
        validation_root / "readbacks.md",
        reference / "replay-correctness.json",
        reference / "corpus-summary.json",
        calibration_root / "calibration.json",
        calibration_root / "storage-verify.json",
        calibration_root / "readbacks.md",
        calibration_root / "replay-correctness.json",
        calibration_root / "corpus-summary.json",
        metadata / "capture-inputs-before.json",
        metadata / "capture-inputs-after.json",
        build,
        plan_path,
        phase1_expectations,
    )
    final_path = root / "comparisons/final-screen-decision.json"
    if final != load_json(final_path):
        raise GateError("final screen decision is not derived from raw evidence")
    return {
        "schema": "chronoxide/storage-vnext-phase5-raw-revalidation/v1",
        "status": "pass",
        "run_count": len(observations),
        "corpus_file_count": calibration["corpus"]["file_count"],
        "corpus_size_bytes": calibration["corpus"]["size_bytes"],
        "screen_summary_sha256": sha256_file(screen_summary_path),
        "validation_sha256": sha256_file(validation_path),
        "final_decision_sha256": sha256_file(final_path),
        "selected_full_gate_policy": final["selected_full_gate_policy"],
    }


def parse_nul_inventory(path: Path, context: str) -> list[str]:
    raw = path.read_bytes()
    if not raw or not raw.endswith(b"\0"):
        raise GateError(f"{context} must be a non-empty NUL-terminated inventory")
    try:
        values = [item.decode("utf-8", errors="strict") for item in raw[:-1].split(b"\0")]
    except UnicodeDecodeError as error:
        raise GateError(f"{context} is not valid UTF-8") from error
    for value in values:
        relative = Path(value)
        if (
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
            or relative.as_posix() != value
        ):
            raise GateError(f"{context} contains an unsafe path: {value!r}")
    if values != sorted(set(values), key=os.fsencode):
        raise GateError(f"{context} is not bytewise sorted and unique")
    return values


def validate_final_artifact_matrix(
    root: Path, directories: list[str], evidence_files: list[str]
) -> None:
    top_directories = {path for path in directories if "/" not in path}
    expected_top_directories = {
        "build-source",
        "build-target",
        "calibration",
        "comparisons",
        "configs",
        "metadata",
        "runs",
        "validation",
    }
    if top_directories != expected_top_directories:
        raise GateError(
            "final root directory matrix differs; "
            f"missing={sorted(expected_top_directories - top_directories)!r}, "
            f"extra={sorted(top_directories - expected_top_directories)!r}"
        )
    root_files = {path for path in evidence_files if "/" not in path}
    if root_files != {"PARTIAL_UNLESS_COMPLETE.txt", "run-plan.tsv"}:
        raise GateError(f"final root file matrix differs: {sorted(root_files)!r}")
    expected_runs = {
        f"runs/run-{index:02d}-{policy}"
        for index, policy in enumerate(EXPECTED_SCHEDULE, start=1)
    }
    observed_runs = {
        path for path in directories if path.startswith("runs/") and path.count("/") == 1
    }
    if observed_runs != expected_runs:
        raise GateError("final measured-run directory matrix differs")
    expected_configs = {"configs/calibration-system.toml"} | {
        f"configs/run-{index:02d}-{policy}.toml"
        for index, policy in enumerate(EXPECTED_SCHEDULE, start=1)
    }
    observed_configs = {path for path in evidence_files if path.startswith("configs/")}
    if observed_configs != expected_configs:
        raise GateError("final rendered-config file matrix differs")
    expected_comparisons = {
        "comparisons/determinism.txt",
        "comparisons/final-screen-decision.json",
        "comparisons/screen-summary.json",
    }
    observed_comparisons = {
        path for path in evidence_files if path.startswith("comparisons/")
    }
    if observed_comparisons != expected_comparisons:
        raise GateError("final comparison file matrix differs")
    expected_metadata_directories = {
        "metadata/binaries",
        "metadata/build",
        "metadata/harness",
        "metadata/preflight",
        "metadata/raw-authorities",
        "metadata/source",
        "metadata/tools",
    }
    observed_metadata_directories = {
        path
        for path in directories
        if path.startswith("metadata/") and path.count("/") == 1
    }
    if observed_metadata_directories != expected_metadata_directories:
        raise GateError("final metadata directory matrix differs")
    expected_metadata_root_files = {
        "metadata/binaries.tsv",
        "metadata/build-provenance.json",
        "metadata/capture-inputs-after.json",
        "metadata/capture-inputs-before-after.sha256",
        "metadata/capture-inputs-before.json",
        "metadata/capture-manifest.json",
        "metadata/chronoxide-ingester-jemalloc.elf-notes.txt",
        "metadata/chronoxide-ingester-jemalloc.file.txt",
        "metadata/chronoxide-ingester-system.elf-notes.txt",
        "metadata/chronoxide-ingester-system.file.txt",
        "metadata/chronoxide-query.elf-notes.txt",
        "metadata/chronoxide-query.file.txt",
        "metadata/chronoxide-query.help.txt",
        "metadata/chronoxide-storage-verify.elf-notes.txt",
        "metadata/chronoxide-storage-verify.file.txt",
        "metadata/chronoxide-storage-verify.help.txt",
        "metadata/config-template.toml",
        "metadata/core-controls.json",
        "metadata/environment.txt",
        "metadata/final-raw-revalidation.json",
        "metadata/harness.sha256",
        "metadata/measurement-controls.json",
        "metadata/perf-stat-preflight.exit-status",
        "metadata/perf-stat-preflight.json",
        "metadata/perf-stat-preflight.log",
        "metadata/perf-stat-preflight.tsv",
        "metadata/preserved-binaries.sha256",
        "metadata/processes-at-plan.txt",
        "metadata/python-interpreter.txt",
        "metadata/rendered-configs.sha256",
        "metadata/run-note.txt",
        "metadata/seal-checks.tsv",
        "metadata/settings.txt",
        "metadata/validated-plan.json",
    }
    observed_metadata_root_files = {
        path
        for path in evidence_files
        if path.startswith("metadata/") and path.count("/") == 1
    }
    if observed_metadata_root_files != expected_metadata_root_files:
        raise GateError("final metadata root-file matrix differs")
    expected_authorities = {
        "metadata/raw-authorities/calibration.json",
        "metadata/raw-authorities/validation.json",
    } | {
        f"metadata/raw-authorities/run-{index:02d}-{policy}.json"
        for index, policy in enumerate(EXPECTED_SCHEDULE, start=1)
    }
    observed_authorities = {
        path for path in evidence_files if path.startswith("metadata/raw-authorities/")
    }
    if observed_authorities != expected_authorities:
        raise GateError("final immutable raw-authority matrix differs")
    expected_preflight = {
        "metadata/preflight/S.json",
        "metadata/preflight/S.stderr",
        "metadata/preflight/S.stdout",
    }
    for policy in ("J0", "J1", "J2", "J3"):
        expected_preflight.update(
            {
                f"metadata/preflight/{policy}.json",
                f"metadata/preflight/{policy}.source-audit.stderr",
                f"metadata/preflight/{policy}.stderr",
                f"metadata/preflight/{policy}.stdout",
            }
        )
    expected_preflight.add("metadata/preflight/J0.source-audit.stdout")
    observed_preflight = {
        path for path in evidence_files if path.startswith("metadata/preflight/")
    }
    if observed_preflight != expected_preflight:
        raise GateError("final allocator-preflight file matrix differs")
    expected_binary_files = {
        "metadata/binaries/chronoxide-ingester-jemalloc",
        "metadata/binaries/chronoxide-ingester-system",
        "metadata/binaries/chronoxide-query",
        "metadata/binaries/chronoxide-storage-verify",
    }
    observed_binary_files = {
        path for path in evidence_files if path.startswith("metadata/binaries/")
    }
    if observed_binary_files != expected_binary_files:
        raise GateError("final preserved-binary file matrix differs")
    expected_build_files = {
        "metadata/build/executable-check-final.json",
        "metadata/build/extracted-source-check-after-jemalloc-build.json",
        "metadata/build/extracted-source-check-after-system-build.json",
        "metadata/build/extracted-source-check-before-jemalloc-build.json",
        "metadata/build/extracted-source-check-before-system-build.json",
        "metadata/build/extracted-source-check-final.json",
        "metadata/build/jemalloc.log",
        "metadata/build/source-check-after-jemalloc-build.json",
        "metadata/build/source-check-after-system-build.json",
        "metadata/build/source-check-before-jemalloc-build.json",
        "metadata/build/source-check-before-system-build.json",
        "metadata/build/source-check-final.json",
        "metadata/build/system.log",
    }
    expected_harness_files = {
        f"metadata/harness/{name}"
        for name in (
            "README.md",
            "ab_gate.py",
            "fadvise_regular_dontneed.c",
            "phase1_4m_expectations.json",
            "phase1_replay_gate.py",
            "phase5_allocator_profile_run.sh",
            "phase5_allocator_screen_gate.py",
            "phase5_allocator_screen_plan.json",
            "phase5_allocator_screen_run.sh",
            "test_phase5_allocator_screen_gate.py",
        )
    }
    expected_source_files = {
        f"metadata/source/{name}"
        for name in (
            "extracted-build-source-seal.json",
            "extracted-source-check-before-builds.json",
            "formal-source-seal.json",
            "git-head.tar",
            "git-head.txt",
            "git-remotes.txt",
            "git-status.txt",
            "source-check-after-archive.json",
            "source-check-before-archive.json",
            "source-state.sha256",
            "tracked-combined.patch",
            "tracked-index.patch",
            "tracked-index.txt",
            "tracked-working-tree.sha256.nul",
            "tracked-worktree.patch",
            "untracked-paths.txt",
            "untracked-working-tree.sha256.nul",
        )
    }
    expected_tools = {
        "metadata/tools/fadvise-regular-dontneed",
        "metadata/tools/fadvise-regular-dontneed.sha256",
    }
    for prefix, expected in (
        ("metadata/build/", expected_build_files),
        ("metadata/harness/", expected_harness_files),
        ("metadata/source/", expected_source_files),
        ("metadata/tools/", expected_tools),
    ):
        observed = {path for path in evidence_files if path.startswith(prefix)}
        if observed != expected:
            raise GateError(f"final {prefix.rstrip('/')} file matrix differs")
    for kind, subtree in (
        ("calibration", root / "calibration"),
        ("validation", root / "validation"),
    ):
        validate_immutable_tree_seal(
            subtree, root / f"metadata/raw-authorities/{kind}.json", kind
        )
    for index, policy in enumerate(EXPECTED_SCHEDULE, start=1):
        label = f"run-{index:02d}-{policy}"
        validate_immutable_tree_seal(
            root / "runs" / label,
            root / "metadata/raw-authorities" / f"{label}.json",
            "run",
        )


def create_final_artifact_inventory(
    result_root: Path,
    files_path: Path,
    directories_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    root = result_root.resolve(strict=True)
    canonical = {
        "files": root / "metadata/result-artifacts.nul",
        "directories": root / "metadata/result-directories.nul",
        "manifest": root / "metadata/result-artifacts.sha256",
    }
    supplied = {"files": files_path, "directories": directories_path, "manifest": manifest_path}
    if any(path.exists() or path.is_symlink() for path in supplied.values()):
        raise GateError("refusing to reuse final inventory authority")
    if any(path.absolute() != canonical[name] for name, path in supplied.items()):
        raise GateError("final inventory authority path is not canonical")
    if (root / "COMPLETE").exists() or (root / "COMPLETE").is_symlink():
        raise GateError("COMPLETE must not exist while final inventory is created")
    directories, paths = fail_closed_tree_inventory(
        root, excluded_subtrees={"build-target"}
    )
    evidence_files = [path.relative_to(root).as_posix() for path in paths]
    if any(path in FINAL_INVENTORY_AUTHORITY_FILES for path in evidence_files):
        raise GateError("final inventory authority unexpectedly pre-exists")
    validate_final_artifact_matrix(root, directories, evidence_files)
    with files_path.open("xb") as destination:
        destination.write(b"".join(os.fsencode(path) + b"\0" for path in evidence_files))
    with directories_path.open("xb") as destination:
        destination.write(b"".join(os.fsencode(path) + b"\0" for path in directories))
    with manifest_path.open("x", encoding="utf-8") as destination:
        for relative, path in zip(evidence_files, paths, strict=True):
            destination.write(f"{sha256_file(path)}  {relative}\n")
    for path in supplied.values():
        path.chmod(0o444)
    return validate_final_artifact_inventory(root, "precomplete")


def validate_final_artifact_inventory(result_root: Path, stage: str) -> dict[str, Any]:
    if stage not in {"precomplete", "complete"}:
        raise GateError("final inventory stage must be precomplete or complete")
    root = result_root.resolve(strict=True)
    authority_paths = {
        relative: root / relative for relative in FINAL_INVENTORY_AUTHORITY_FILES
    }
    required = FINAL_INVENTORY_AUTHORITY_FILES - {"metadata/FINAL_SEAL_VALIDATED.json"}
    for relative in required:
        path = authority_paths[relative]
        if path.is_symlink() or not path.is_file() or stat.S_IMODE(path.stat().st_mode) != 0o444:
            raise GateError(f"final inventory authority changed type or mode: {relative}")
    listed_files = parse_nul_inventory(
        authority_paths["metadata/result-artifacts.nul"], "final file inventory"
    )
    listed_directories = parse_nul_inventory(
        authority_paths["metadata/result-directories.nul"], "final directory inventory"
    )
    directories, all_paths = fail_closed_tree_inventory(
        root, excluded_subtrees={"build-target"}
    )
    observed_files = [path.relative_to(root).as_posix() for path in all_paths]
    complete_path = root / "COMPLETE"
    if stage == "precomplete":
        if complete_path.exists() or complete_path.is_symlink():
            raise GateError("COMPLETE exists before final pre-completion admission")
    else:
        if complete_path.is_symlink() or not complete_path.is_file():
            raise GateError("COMPLETE is missing or is not a regular file")
        if complete_path.read_bytes() != b"chronoxide/allocator-screen-complete/v1\n":
            raise GateError("COMPLETE marker content or version changed")
        if stat.S_IMODE(complete_path.stat().st_mode) != 0o444:
            raise GateError("COMPLETE marker must have exact mode 0444")
    actual_evidence = sorted(
        (
            path
            for path in observed_files
            if path not in FINAL_INVENTORY_AUTHORITY_FILES and path != "COMPLETE"
        ),
        key=os.fsencode,
    )
    if actual_evidence != listed_files:
        raise GateError("final file inventory does not exactly match the result tree")
    if directories != listed_directories:
        raise GateError("final directory inventory does not exactly match the result tree")
    validate_final_artifact_matrix(root, directories, listed_files)
    manifest_lines = authority_paths[
        "metadata/result-artifacts.sha256"
    ].read_text(encoding="utf-8", errors="strict").splitlines()
    expected_manifest_lines = []
    for relative in listed_files:
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise GateError(f"final artifact is missing or not regular: {relative}")
        expected_manifest_lines.append(f"{sha256_file(path)}  {relative}")
    if manifest_lines != expected_manifest_lines:
        raise GateError("final artifact digest manifest is not exact")
    final_seal = authority_paths["metadata/FINAL_SEAL_VALIDATED.json"]
    if stage == "complete":
        if (
            final_seal.is_symlink()
            or not final_seal.is_file()
            or stat.S_IMODE(final_seal.stat().st_mode) != 0o444
        ):
            raise GateError("pre-completion final validation record is missing or mutable")
        saved = load_json(final_seal)
        if (
            saved.get("schema") != FINAL_VALIDATION_SCHEMA
            or saved.get("stage") != "precomplete"
            or saved.get("status") != "pass"
            or saved.get("artifact_manifest_sha256")
            != sha256_file(authority_paths["metadata/result-artifacts.sha256"])
        ):
            raise GateError("pre-completion final validation record is invalid")
    return {
        "schema": FINAL_VALIDATION_SCHEMA,
        "stage": stage,
        "status": "pass",
        "artifact_count": len(listed_files),
        "directory_count": len(listed_directories),
        "artifact_manifest_sha256": sha256_file(
            authority_paths["metadata/result-artifacts.sha256"]
        ),
        "file_inventory_sha256": sha256_file(
            authority_paths["metadata/result-artifacts.nul"]
        ),
        "directory_inventory_sha256": sha256_file(
            authority_paths["metadata/result-directories.nul"]
        ),
    }


def revalidate_profile_from_raw(
    result_root: Path, screen_result: Path
) -> dict[str, Any]:
    root = result_root.resolve(strict=True)
    screen = screen_result.resolve(strict=True)
    harness = screen / "metadata/harness"
    plan_path = harness / "phase5_allocator_screen_plan.json"
    expectations = harness / "phase1_4m_expectations.json"
    profile_plan = validate_plan(plan_path, expectations)
    metadata = root / "metadata"
    authorities = metadata / "raw-authorities"
    capacity_control_path = metadata / "profile-capacity-control.json"
    capacity_control = validate_profile_capacity_control(capacity_control_path)
    reference = screen / "runs/run-01-S"
    calibration_root = screen / "calibration"
    build = screen / "metadata/build-provenance.json"
    binaries = {
        "S": screen / "metadata/binaries/chronoxide-ingester-system",
        "J": screen / "metadata/binaries/chronoxide-ingester-jemalloc",
    }
    common = {
        "screen_result_path": screen,
        "artifact_manifest_path": screen / "metadata/result-artifacts.sha256",
        "system_binary_path": binaries["S"],
        "jemalloc_binary_path": binaries["J"],
        "query_binary_path": screen / "metadata/binaries/chronoxide-query",
        "storage_verify_binary_path": screen
        / "metadata/binaries/chronoxide-storage-verify",
        "reference_manifest_path": reference / "segments.sha256",
        "reference_correctness_path": reference / "replay-correctness.json",
        "reference_corpus_path": reference / "corpus-summary.json",
        "calibration_path": calibration_root / "calibration.json",
        "calibration_storage_path": calibration_root / "storage-verify.json",
        "calibration_readbacks_path": calibration_root / "readbacks.md",
        "calibration_correctness_path": calibration_root / "replay-correctness.json",
        "calibration_corpus_path": calibration_root / "corpus-summary.json",
        "final_decision_path": screen / "comparisons/final-screen-decision.json",
        "complete_marker_path": screen / "COMPLETE",
        "build_provenance_path": build,
        "plan_path": plan_path,
        "phase1_expectations": expectations,
    }
    results: dict[str, Any] = {}
    profile_specs = [("heaptrack", root / "heaptrack", "profile-heaptrack")]
    if (root / "perf-record").is_dir():
        saved_perf = load_json(root / "perf-record/profile-evidence.json")
        perf_policy = saved_perf.get("policy")
        perf_kind = "profile-perf-system" if perf_policy == "S" else "profile-perf-selected"
        profile_specs.append(("perf-record", root / "perf-record", perf_kind))
    for profile_kind, profile, seal_kind in profile_specs:
        validate_immutable_tree_seal(
            profile, authorities / f"{profile_kind}.json", seal_kind
        )
        saved = load_json(profile / "profile-evidence.json")
        policy = saved.get("policy")
        if policy not in POLICY_ORDER:
            raise GateError(f"{profile_kind} saved profile policy is invalid")
        profile_control_seal = metadata / f"{profile_kind}-{policy}-controls.json"
        profile_control_validation = check_profile_control_seal(
            profile_control_seal,
            {
                root / f"configs/{profile_kind}.toml",
                profile / "config-render.json",
                metadata / "capture-inputs-before.json",
                capacity_control_path,
                metadata / "python-interpreter.txt",
                harness / "phase5_allocator_profile_run.sh",
            },
        )
        validate_process_snapshot(profile / "processes-before.txt", set())
        guardian_minimum_free_bytes = derive_profile_guardian_minimum_free_bytes(
            capacity_control_path, common["reference_corpus_path"]
        )
        validate_guardian_evidence(
            profile / "external-conflict-guardian.json",
            profile / "external-conflict-guardian-control.json",
            profile / "external-conflict-guardian-ready",
            profile / "external-conflict-guardian-launch",
            profile_plan["environment_contract"][
                "external_conflict_poll_interval_ms"
            ],
            root,
            guardian_minimum_free_bytes,
            False,
        )
        validate_zero_exit_status(
            profile / "replay.exit-status", f"{profile_kind} replay exit status"
        )
        validate_zero_exit_status(
            profile / "external-conflict-guardian.exit-status",
            f"{profile_kind} guardian exit status",
        )
        if profile_kind == "heaptrack":
            profile_data = profile / "heaptrack.trace.zst"
            profiler_log = profile / "heaptrack.log"
            analysis = profile / "heaptrack-stacks.txt"
        else:
            profile_data = profile / "perf.data"
            profiler_log = profile / "perf-record.log"
            analysis = profile / "perf-script.txt"
        selected_runtime = None
        selected_preflight = None
        if profile_kind == "perf-record" and policy != "S":
            selected_runtime = profile / "selected-runtime-combined.log"
            selected_preflight = profile / "selected-preflight.json"
        recomputed = record_profile_evidence(
            profile_kind,
            policy,
            binaries["S" if policy == "S" else "J"],
            common["screen_result_path"],
            common["artifact_manifest_path"],
            common["system_binary_path"],
            common["jemalloc_binary_path"],
            common["query_binary_path"],
            common["storage_verify_binary_path"],
            profile_data,
            profiler_log,
            analysis,
            profile / "lost-events.txt",
            profile / "segments.sha256",
            common["reference_manifest_path"],
            profile / "replay-correctness.json",
            common["reference_correctness_path"],
            profile / "corpus-summary.json",
            common["reference_corpus_path"],
            profile / "storage-verify.json",
            profile / "readbacks.md",
            common["calibration_path"],
            common["calibration_storage_path"],
            common["calibration_readbacks_path"],
            common["calibration_correctness_path"],
            common["calibration_corpus_path"],
            common["final_decision_path"],
            common["complete_marker_path"],
            common["build_provenance_path"],
            selected_runtime,
            selected_preflight,
            common["plan_path"],
            common["phase1_expectations"],
        )
        if recomputed != saved:
            raise GateError(f"{profile_kind} evidence is not derived from raw files")
        results[profile_kind] = {
            "policy": policy,
            "profile_evidence_sha256": sha256_file(profile / "profile-evidence.json"),
            "profile_control_seal_sha256": profile_control_validation[
                "control_seal_sha256"
            ],
            "corpus_size_bytes": recomputed["corpus"]["size_bytes"],
            "guardian_minimum_free_bytes": guardian_minimum_free_bytes,
        }
    return {
        "schema": "chronoxide/storage-vnext-phase5-profile-raw-revalidation/v2",
        "status": "pass",
        "screen_result": str(screen),
        "screen_artifact_manifest_sha256": sha256_file(
            screen / "metadata/result-artifacts.sha256"
        ),
        "profile_capacity_control_sha256": sha256_file(capacity_control_path),
        "profile_min_free_bytes": capacity_control["profile_min_free_bytes"],
        "profiles": results,
    }


def validate_profile_artifact_matrix(
    root: Path, directories: list[str], evidence_files: list[str]
) -> None:
    has_perf = "perf-record" in directories
    expected_top = {"configs", "heaptrack", "metadata"}
    if has_perf:
        expected_top.add("perf-record")
    observed_top = {path for path in directories if "/" not in path}
    if observed_top != expected_top:
        raise GateError("profile root directory matrix differs")
    if {path for path in evidence_files if "/" not in path} != {"PROFILE_SCOPE.txt"}:
        raise GateError("profile root file matrix differs")
    expected_configs = {"configs/heaptrack.toml"}
    if has_perf:
        expected_configs.add("configs/perf-record.toml")
    if {path for path in evidence_files if path.startswith("configs/")} != expected_configs:
        raise GateError("profile config matrix differs")
    metadata_directories = {
        path
        for path in directories
        if path.startswith("metadata/") and path.count("/") == 1
    }
    if metadata_directories != {"metadata/raw-authorities"}:
        raise GateError("profile metadata directory matrix differs")
    expected_metadata = {
        "metadata/capture-inputs-after.json",
        "metadata/capture-inputs-before.json",
        "metadata/final-raw-revalidation.json",
        "metadata/heaptrack-S-controls.json",
        "metadata/profile-capacity-control.json",
        "metadata/python-interpreter.txt",
        "metadata/run-note.txt",
    }
    validate_profile_capacity_control(root / "metadata/profile-capacity-control.json")
    expected_authorities = {"metadata/raw-authorities/heaptrack.json"}
    validate_immutable_tree_seal(
        root / "heaptrack",
        root / "metadata/raw-authorities/heaptrack.json",
        "profile-heaptrack",
    )
    if has_perf:
        evidence = load_json(root / "perf-record/profile-evidence.json")
        policy = evidence.get("policy")
        if policy not in POLICY_ORDER:
            raise GateError("profile perf policy is invalid")
        expected_metadata.add(f"metadata/perf-record-{policy}-controls.json")
        expected_authorities.add("metadata/raw-authorities/perf-record.json")
        seal_kind = "profile-perf-system" if policy == "S" else "profile-perf-selected"
        validate_immutable_tree_seal(
            root / "perf-record",
            root / "metadata/raw-authorities/perf-record.json",
            seal_kind,
        )
    observed_metadata = {
        path
        for path in evidence_files
        if path.startswith("metadata/") and path.count("/") == 1
    }
    if observed_metadata != expected_metadata:
        raise GateError("profile metadata root-file matrix differs")
    observed_authorities = {
        path for path in evidence_files if path.startswith("metadata/raw-authorities/")
    }
    if observed_authorities != expected_authorities:
        raise GateError("profile raw-authority matrix differs")


def create_profile_artifact_inventory(
    result_root: Path, files_path: Path, directories_path: Path, manifest_path: Path
) -> dict[str, Any]:
    root = result_root.resolve(strict=True)
    canonical = {
        "files": root / "metadata/artifacts.nul",
        "directories": root / "metadata/directories.nul",
        "manifest": root / "metadata/artifacts.sha256",
    }
    supplied = {"files": files_path, "directories": directories_path, "manifest": manifest_path}
    if any(path.exists() or path.is_symlink() for path in supplied.values()):
        raise GateError("refusing to reuse profile inventory authority")
    if any(path.absolute() != canonical[name] for name, path in supplied.items()):
        raise GateError("profile inventory authority path is not canonical")
    if (root / "COMPLETE").exists() or (root / "COMPLETE").is_symlink():
        raise GateError("profile COMPLETE must not exist while inventory is created")
    directories, paths = fail_closed_tree_inventory(root)
    evidence_files = [path.relative_to(root).as_posix() for path in paths]
    validate_profile_artifact_matrix(root, directories, evidence_files)
    with files_path.open("xb") as destination:
        destination.write(b"".join(os.fsencode(path) + b"\0" for path in evidence_files))
    with directories_path.open("xb") as destination:
        destination.write(b"".join(os.fsencode(path) + b"\0" for path in directories))
    with manifest_path.open("x", encoding="utf-8") as destination:
        for relative, path in zip(evidence_files, paths, strict=True):
            destination.write(f"{sha256_file(path)}  {relative}\n")
    for path in supplied.values():
        path.chmod(0o444)
    return validate_profile_artifact_inventory(root, "precomplete")


def validate_profile_artifact_inventory(result_root: Path, stage: str) -> dict[str, Any]:
    if stage not in {"precomplete", "complete"}:
        raise GateError("profile inventory stage must be precomplete or complete")
    root = result_root.resolve(strict=True)
    authorities = {
        relative: root / relative for relative in PROFILE_INVENTORY_AUTHORITY_FILES
    }
    for relative in PROFILE_INVENTORY_AUTHORITY_FILES - {
        "metadata/FINAL_SEAL_VALIDATED.json"
    }:
        path = authorities[relative]
        if path.is_symlink() or not path.is_file() or stat.S_IMODE(path.stat().st_mode) != 0o444:
            raise GateError(f"profile inventory authority changed: {relative}")
    listed_files = parse_nul_inventory(
        authorities["metadata/artifacts.nul"], "profile file inventory"
    )
    listed_directories = parse_nul_inventory(
        authorities["metadata/directories.nul"], "profile directory inventory"
    )
    directories, paths = fail_closed_tree_inventory(root)
    observed = [path.relative_to(root).as_posix() for path in paths]
    complete = root / "COMPLETE"
    if stage == "precomplete":
        if complete.exists() or complete.is_symlink():
            raise GateError("profile COMPLETE exists before admission")
    else:
        if (
            complete.is_symlink()
            or not complete.is_file()
            or complete.read_bytes() != b"chronoxide/allocator-profile-complete/v1\n"
            or stat.S_IMODE(complete.stat().st_mode) != 0o444
        ):
            raise GateError("profile COMPLETE marker is invalid")
    actual = sorted(
        (
            path
            for path in observed
            if path not in PROFILE_INVENTORY_AUTHORITY_FILES and path != "COMPLETE"
        ),
        key=os.fsencode,
    )
    if actual != listed_files or directories != listed_directories:
        raise GateError("profile exact file or directory inventory changed")
    validate_profile_artifact_matrix(root, directories, listed_files)
    expected_manifest = [f"{sha256_file(root / path)}  {path}" for path in listed_files]
    if authorities["metadata/artifacts.sha256"].read_text(
        encoding="utf-8", errors="strict"
    ).splitlines() != expected_manifest:
        raise GateError("profile artifact digest manifest changed")
    final_seal = authorities["metadata/FINAL_SEAL_VALIDATED.json"]
    if stage == "complete":
        if final_seal.is_symlink() or not final_seal.is_file() or stat.S_IMODE(final_seal.stat().st_mode) != 0o444:
            raise GateError("profile pre-completion validation record is missing")
        saved = load_json(final_seal)
        if (
            saved.get("schema") != FINAL_VALIDATION_SCHEMA
            or saved.get("stage") != "profile-precomplete"
            or saved.get("status") != "pass"
            or saved.get("artifact_manifest_sha256")
            != sha256_file(authorities["metadata/artifacts.sha256"])
        ):
            raise GateError("profile pre-completion validation record is invalid")
    return {
        "schema": FINAL_VALIDATION_SCHEMA,
        "stage": "profile-precomplete" if stage == "precomplete" else "profile-complete",
        "status": "pass",
        "artifact_count": len(listed_files),
        "directory_count": len(listed_directories),
        "artifact_manifest_sha256": sha256_file(
            authorities["metadata/artifacts.sha256"]
        ),
        "file_inventory_sha256": sha256_file(authorities["metadata/artifacts.nul"]),
        "directory_inventory_sha256": sha256_file(
            authorities["metadata/directories.nul"]
        ),
    }


def validate_artifact_manifest(
    result_root: Path,
    manifest_path: Path,
    required_paths: set[str],
) -> dict[str, Any]:
    if not result_root.is_absolute() or result_root.is_symlink() or not result_root.is_dir():
        raise GateError("completed screen root must be an absolute non-symlink directory")
    root = result_root.resolve(strict=True)
    expected_manifest = root / "metadata/result-artifacts.sha256"
    if manifest_path.resolve(strict=True) != expected_manifest:
        raise GateError("completed screen artifact manifest is not at its canonical path")
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise GateError("completed screen artifact manifest must be a regular file")
    if stat.S_IMODE(manifest_path.stat().st_mode) != 0o444:
        raise GateError("completed screen artifact manifest must have exact mode 0444")
    entries: dict[str, str] = {}
    for line_number, line in enumerate(
        manifest_path.read_text(encoding="utf-8", errors="strict").splitlines(),
        start=1,
    ):
        match = re.fullmatch(r"([0-9a-f]{64}) [ *](.+)", line)
        if match is None:
            raise GateError(
                f"completed screen artifact manifest line {line_number} is malformed"
            )
        digest, relative_text = match.groups()
        relative = Path(relative_text)
        if (
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
            or relative.as_posix() != relative_text
        ):
            raise GateError("completed screen artifact manifest contains an unsafe path")
        if relative_text in entries:
            raise GateError("completed screen artifact manifest contains a duplicate path")
        candidate = root
        for part in relative.parts:
            candidate /= part
            if candidate.is_symlink():
                raise GateError(
                    f"completed screen artifact path traverses a symlink: {relative_text}"
                )
        try:
            mode = candidate.lstat().st_mode
        except FileNotFoundError as error:
            raise GateError(
                f"completed screen artifact is missing: {relative_text}"
            ) from error
        if not stat.S_ISREG(mode):
            raise GateError(
                f"completed screen artifact is not a regular file: {relative_text}"
            )
        if sha256_file(candidate) != digest:
            raise GateError(
                f"completed screen artifact digest changed: {relative_text}"
            )
        entries[relative_text] = digest
    if not entries:
        raise GateError("completed screen artifact manifest is empty")
    missing = sorted(required_paths - set(entries))
    if missing:
        raise GateError(
            f"completed screen artifact manifest lacks required paths: {missing!r}"
        )
    return {
        "artifact_manifest_sha256": sha256_file(manifest_path),
        "artifact_count": len(entries),
        "required_paths_present": True,
    }


def validate_completed_screen_artifacts(
    screen_result_path: Path,
    artifact_manifest_path: Path,
    complete_marker_path: Path,
    final_decision_path: Path,
    calibration_path: Path,
    build_provenance_path: Path,
    system_binary_path: Path,
    jemalloc_binary_path: Path,
    query_binary_path: Path,
    storage_verify_binary_path: Path,
) -> dict[str, Any]:
    root = screen_result_path.resolve(strict=True)
    canonical = {
        "artifact_manifest": root / "metadata/result-artifacts.sha256",
        "complete": root / "COMPLETE",
        "final": root / "comparisons/final-screen-decision.json",
        "calibration": root / "calibration/calibration.json",
        "build": root / "metadata/build-provenance.json",
        "core_controls": root / "metadata/core-controls.json",
        "measurement_controls": root / "metadata/measurement-controls.json",
        "harness_seal": root / "metadata/harness.sha256",
        "python": root / "metadata/python-interpreter.txt",
        "preserved_binary_seal": root / "metadata/preserved-binaries.sha256",
        "rendered_config_seal": root / "metadata/rendered-configs.sha256",
        "run_plan": root / "run-plan.tsv",
        "fadvise": root / "metadata/tools/fadvise-regular-dontneed",
        "fadvise_seal": root / "metadata/tools/fadvise-regular-dontneed.sha256",
        "source_seal": root / "metadata/source/formal-source-seal.json",
        "source_archive": root / "metadata/source/git-head.tar",
        "extracted_source_seal": root
        / "metadata/source/extracted-build-source-seal.json",
        "system": root / "metadata/binaries/chronoxide-ingester-system",
        "jemalloc": root / "metadata/binaries/chronoxide-ingester-jemalloc",
        "query": root / "metadata/binaries/chronoxide-query",
        "storage_verify": root / "metadata/binaries/chronoxide-storage-verify",
    }
    supplied = {
        "artifact_manifest": artifact_manifest_path,
        "complete": complete_marker_path,
        "final": final_decision_path,
        "calibration": calibration_path,
        "build": build_provenance_path,
        "system": system_binary_path,
        "jemalloc": jemalloc_binary_path,
        "query": query_binary_path,
        "storage_verify": storage_verify_binary_path,
    }
    for role, path in supplied.items():
        try:
            resolved = path.resolve(strict=True)
        except FileNotFoundError as error:
            raise GateError(f"completed screen {role} input is missing: {path}") from error
        if resolved != canonical[role]:
            raise GateError(f"completed screen {role} input is not at its canonical path")
    if (
        complete_marker_path.is_symlink()
        or not complete_marker_path.is_file()
        or complete_marker_path.read_bytes()
        != b"chronoxide/allocator-screen-complete/v1\n"
        or stat.S_IMODE(complete_marker_path.stat().st_mode) != 0o444
    ):
        raise GateError("completed screen lacks the exact versioned COMPLETE marker")

    required = {
        str(path.relative_to(root))
        for key, path in canonical.items()
        if key not in {"artifact_manifest", "complete"}
    }
    required.update(
        {
            "runs/run-01-S/segments.sha256",
            "runs/run-01-S/replay-correctness.json",
            "runs/run-01-S/corpus-summary.json",
            "metadata/harness/phase5_allocator_screen_gate.py",
            "metadata/harness/phase5_allocator_screen_plan.json",
            "metadata/harness/phase1_4m_expectations.json",
            "build-source/Cargo.toml",
            "build-source/Cargo.lock",
            "build-source/.cargo/config.toml",
        }
    )
    artifact = validate_artifact_manifest(root, artifact_manifest_path, required)
    core_controls = check_control_seal(canonical["core_controls"])
    measurement_controls = check_control_seal(canonical["measurement_controls"])
    core_control_paths = {
        entry["path"]
        for entry in validate_control_seal_document(
            load_json(canonical["core_controls"])
        )["inputs"]
    }
    required_core_controls = {
        str(path)
        for path in (
            canonical["harness_seal"],
            root / "metadata/validated-plan.json",
            canonical["python"],
            canonical["source_seal"],
            canonical["source_archive"],
            canonical["extracted_source_seal"],
            root / "metadata/binaries.tsv",
            canonical["preserved_binary_seal"],
            canonical["build"],
            canonical["system"],
            canonical["jemalloc"],
            canonical["query"],
            canonical["storage_verify"],
            *(
                root / "metadata/harness" / name
                for name in (
                    "phase5_allocator_screen_run.sh",
                    "phase5_allocator_profile_run.sh",
                    "phase5_allocator_screen_gate.py",
                    "phase5_allocator_screen_plan.json",
                    "test_phase5_allocator_screen_gate.py",
                    "phase1_replay_gate.py",
                    "phase1_4m_expectations.json",
                    "ab_gate.py",
                    "fadvise_regular_dontneed.c",
                    "README.md",
                )
            ),
        )
    }
    missing_core_controls = sorted(required_core_controls - core_control_paths)
    if missing_core_controls:
        raise GateError(
            "completed screen core-control seal lacks required inputs: "
            f"{missing_core_controls!r}"
        )
    measurement_control_paths = {
        entry["path"]
        for entry in validate_control_seal_document(
            load_json(canonical["measurement_controls"])
        )["inputs"]
    }
    schedule_labels = [
        f"run-{run_index:02d}-{policy}"
        for run_index, policy in enumerate(EXPECTED_SCHEDULE, start=1)
    ]
    required_measurement_controls = {
        str(path)
        for path in (
            canonical["core_controls"],
            root / "metadata/capture-inputs-before.json",
            root / "metadata/config-template.toml",
            root / "metadata/capture-manifest.json",
            canonical["run_plan"],
            canonical["rendered_config_seal"],
            canonical["fadvise"],
            canonical["fadvise_seal"],
            root / "configs/calibration-system.toml",
            root / "calibration/config-render.json",
            *(root / "configs" / f"{label}.toml" for label in schedule_labels),
            *(root / "runs" / label / "config-render.json" for label in schedule_labels),
        )
    }
    missing_measurement_controls = sorted(
        required_measurement_controls - measurement_control_paths
    )
    if missing_measurement_controls:
        raise GateError(
            "completed screen measurement-control seal lacks required inputs: "
            f"{missing_measurement_controls!r}"
        )
    build = validate_build_provenance(load_json(build_provenance_path))
    source = validate_source_seal_document(load_json(canonical["source_seal"]))
    extracted = validate_extracted_source_seal_document(
        load_json(canonical["extracted_source_seal"])
    )
    if canonical["source_archive"].stat().st_mode & 0o222:
        raise GateError("completed screen source archive is writable")
    if build["source_seal_sha256"] != sha256_file(canonical["source_seal"]):
        raise GateError("completed screen build provenance has the wrong source seal")
    if build["source_identity_sha256"] != source["identity_sha256"]:
        raise GateError("completed screen build provenance has the wrong source identity")
    if build["build_source"]["root"] != str(root / "build-source") or extracted[
        "source_root"
    ] != str(root / "build-source"):
        raise GateError("completed screen build provenance has a non-canonical source root")
    if build["build_source"]["archive_path"] != str(
        canonical["source_archive"]
    ) or extracted["archive_path"] != str(canonical["source_archive"]):
        raise GateError("completed screen build provenance has a non-canonical source archive")
    if build["build_source"]["archive_sha256"] != sha256_file(
        canonical["source_archive"]
    ):
        raise GateError("completed screen build provenance has the wrong source archive")
    if build["build_source"]["archive_size_bytes"] != canonical[
        "source_archive"
    ].stat().st_size or extracted["archive_size_bytes"] != canonical[
        "source_archive"
    ].stat().st_size:
        raise GateError("completed screen source-archive size binding changed")
    if build["build_source"]["extracted_source_seal_sha256"] != sha256_file(
        canonical["extracted_source_seal"]
    ):
        raise GateError("completed screen build provenance has the wrong extracted-source seal")
    if (
        extracted["archive_sha256"] != build["build_source"]["archive_sha256"]
        or extracted["file_manifest_sha256"]
        != build["build_source"]["file_manifest_sha256"]
        or extracted["manifest_path_reference_count"]
        != build["build_source"]["manifest_path_reference_count"]
        or extracted["live_source_seal_sha256"] != build["source_seal_sha256"]
        or extracted["live_source_identity_sha256"]
        != build["source_identity_sha256"]
        or extracted["git_head"] != build["git_head"]
        or extracted["git_head_tree"] != build["git_head_tree"]
    ):
        raise GateError("completed screen extracted source is not bound to build provenance")
    executables = validate_executable_set(
        build_provenance_path,
        system_binary_path,
        jemalloc_binary_path,
        query_binary_path,
        storage_verify_binary_path,
    )
    final = load_json(final_decision_path)
    if (
        not isinstance(final, dict)
        or final.get("schema") != FINAL_DECISION_SCHEMA
        or final.get("screen_complete") is not True
        or final.get("canonical_validation_complete") is not True
        or final.get("production_promotion_authorized") is not False
        or final.get("run_count") != 10
    ):
        raise GateError("completed screen final decision is incomplete or promotional")
    if final.get("build_provenance_sha256") != sha256_file(build_provenance_path):
        raise GateError("completed screen final decision has the wrong build provenance")
    if final.get("calibration_sha256") != sha256_file(calibration_path):
        raise GateError("completed screen final decision has the wrong calibration")
    if final.get("binary_sha256_by_role") != {
        role: build["binary_sha256"][role] for role in ("system", "jemalloc")
    }:
        raise GateError("completed screen final decision has changed allocator binaries")
    return {
        "status": "pass",
        **artifact,
        "screen_final_decision_sha256": sha256_file(final_decision_path),
        "build_provenance_sha256": sha256_file(build_provenance_path),
        "source_seal_sha256": sha256_file(canonical["source_seal"]),
        "source_identity_sha256": source["identity_sha256"],
        "source_archive_sha256": extracted["archive_sha256"],
        "extracted_source_seal_sha256": sha256_file(
            canonical["extracted_source_seal"]
        ),
        "extracted_source_manifest_sha256": extracted["file_manifest_sha256"],
        "core_control_identity_sha256": core_controls["identity_sha256"],
        "measurement_control_identity_sha256": measurement_controls["identity_sha256"],
        "binary_sha256": executables["binary_sha256"],
    }


def requested_effective_entries(conf: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for entry in conf.split(","):
        key, raw_value = entry.split(":", 1)
        result[key] = (
            raw_value == "true"
            if raw_value in ("true", "false")
            else int(raw_value)
        )
    return result


def validate_jemalloc_config_sources(stderr: str, expected_environment: str) -> None:
    expected_sources = [
        '<jemalloc>: malloc_conf #1 (string specified via --with-malloc-conf): ""',
        '<jemalloc>: malloc_conf #2 (string pointed to by the global variable malloc_conf): ""',
        '<jemalloc>: malloc_conf #3 ("name" of the file referenced by the symbolic link named /etc/malloc.conf): ""',
        '<jemalloc>: malloc_conf #4 (value of the environment variable MALLOC_CONF): '
        f'"{expected_environment}"',
        '<jemalloc>: malloc_conf #5 (string pointed to by the global variable malloc_conf_2_conf_harder): ""',
    ]
    source_lines = [
        line for line in stderr.splitlines() if line.startswith("<jemalloc>: malloc_conf #")
    ]
    if source_lines != expected_sources:
        raise GateError(
            "jemalloc configuration sources #1..#5 were not exactly audited; "
            f"expected {expected_sources!r}, got {source_lines!r}"
        )
    if "Invalid conf" in stderr or "Malformed conf" in stderr:
        raise GateError("jemalloc reported invalid configuration during source audit")


def validate_application_preflight(
    observed_value: Any,
    expected: dict[str, Any],
    *,
    context: str,
    stderr: str | None,
) -> dict[str, Any] | None:
    observed = require_exact_keys(observed_value, PREFLIGHT_APPLICATION_KEYS, context)
    if observed["schema"] != PREFLIGHT_SCHEMA:
        raise GateError(f"{context} schema mismatch")
    if observed["rust_global_allocator"] != expected["rust_global_allocator"]:
        raise GateError(f"{context} binary identity mismatch")
    if observed["jemalloc_conf_env"] != "_RJEM_MALLOC_CONF":
        raise GateError(f"{context} reports the wrong jemalloc environment name")
    if observed["ld_preload_present"] is not False:
        raise GateError("LD_PRELOAD must be absent for the linked-allocator screen")
    if observed["malloc_conf_present"] is not False:
        raise GateError("unprefixed MALLOC_CONF must be absent")
    if type(observed["post_ingester_drop_hold_secs"]) is not int:
        raise GateError(f"{context} hold duration must be an integer")
    if observed["post_ingester_drop_hold_secs"] != 0:
        raise GateError("preflight must run without the diagnostic hold")
    if observed["post_ingester_drop_checkpoint_enabled"] is not False:
        raise GateError("preflight must run without a checkpoint output")
    if observed["post_ingester_drop_telemetry_enabled"] is not False:
        raise GateError("preflight must run without allocator telemetry output")

    probe = require_exact_keys(
        observed["global_allocator_probe"],
        {
            "status",
            "allocation_bytes",
            "minimum_allocated_growth_bytes",
            "allocated_before_bytes",
            "allocated_while_live_bytes",
            "allocated_after_drop_bytes",
            "observed_allocated_growth_bytes",
            "passed",
        },
        f"{context}.global_allocator_probe",
    )

    conf = expected["jemalloc_conf"]
    if expected["rust_global_allocator"] == "system":
        if observed["requested_policy_raw"] is not None:
            raise GateError("system preflight unexpectedly reports jemalloc policy bytes")
        if observed["requested_policy_canonical"] is not None:
            raise GateError("system preflight unexpectedly canonicalized jemalloc policy")
        if observed["effective_policy"] is not None:
            raise GateError("system allocator internals must be explicit null")
        if observed["allocator_internal_telemetry"] != "unavailable":
            raise GateError("system allocator telemetry availability is overstated")
        if probe != {
            "status": "unavailable_for_system_allocator",
            "allocation_bytes": None,
            "minimum_allocated_growth_bytes": None,
            "allocated_before_bytes": None,
            "allocated_while_live_bytes": None,
            "allocated_after_drop_bytes": None,
            "observed_allocated_growth_bytes": None,
            "passed": None,
        }:
            raise GateError("system global-allocation probe must be explicit unavailable")
        if stderr is not None and "<jemalloc>:" in stderr:
            raise GateError("system binary emitted jemalloc confirmation output")
        return None

    if observed["requested_policy_raw"] != conf:
        raise GateError("jemalloc raw policy differs from the frozen policy")
    if observed["requested_policy_canonical"] != conf:
        raise GateError("jemalloc canonical policy differs from the frozen policy")
    if (
        observed["allocator_internal_telemetry"]
        != "fixed_startup_options_and_release_stats"
    ):
        raise GateError("jemalloc telemetry scope is not the fixed startup option set")
    if probe["status"] != "passed" or probe["passed"] is not True:
        raise GateError("jemalloc binary did not pass its live global-allocation probe")
    for key in (
        "allocation_bytes",
        "minimum_allocated_growth_bytes",
        "allocated_before_bytes",
        "allocated_while_live_bytes",
        "allocated_after_drop_bytes",
        "observed_allocated_growth_bytes",
    ):
        strict_int(probe[key], f"{context}.global_allocator_probe.{key}", minimum=0)
    if probe["allocation_bytes"] != 64 * 1024 * 1024:
        raise GateError("jemalloc global-allocation probe has a changed allocation size")
    if probe["minimum_allocated_growth_bytes"] != 48 * 1024 * 1024:
        raise GateError("jemalloc global-allocation probe has a changed minimum growth")
    if probe["allocated_while_live_bytes"] - probe["allocated_before_bytes"] != probe[
        "observed_allocated_growth_bytes"
    ]:
        raise GateError("jemalloc global-allocation probe growth does not derive from snapshots")
    if probe["observed_allocated_growth_bytes"] < probe["minimum_allocated_growth_bytes"]:
        raise GateError("jemalloc global-allocation probe growth is below its minimum")
    effective = require_exact_keys(
        observed["effective_policy"], EFFECTIVE_POLICY_KEYS, f"{context}.effective_policy"
    )
    for key in (
        "abort_conf",
        "confirm_conf",
        "background_thread",
        "retain",
    ):
        if type(effective[key]) is not bool:
            raise GateError(f"jemalloc effective {key} must be a boolean")
    for key in ("narenas", "dirty_decay_ms", "muzzy_decay_ms", "max_background_threads"):
        if type(effective[key]) is not int:
            raise GateError(f"jemalloc effective {key} must be an integer")
    if effective["narenas"] < 1 or effective["max_background_threads"] < 1:
        raise GateError("jemalloc effective arena/thread counts must be positive")
    requested_entries = requested_effective_entries(conf) if conf is not None else {}
    for key, expected_value in requested_entries.items():
        if type(effective[key]) is not type(expected_value) or effective[key] != expected_value:
            raise GateError(
                f"jemalloc effective {key} differs: expected {expected_value!r}, "
                f"got {effective[key]!r}"
            )
    if stderr is not None and conf is not None:
        source_line = (
            '<jemalloc>: malloc_conf #4 (value of the environment variable MALLOC_CONF): '
            f'"{conf}"'
        )
        if source_line not in stderr:
            raise GateError("jemalloc confirm_conf did not echo the exact environment policy")
        for entry in conf.split(","):
            if f"<jemalloc>: -- Set conf value: {entry}" not in stderr:
                raise GateError(f"jemalloc confirm_conf did not confirm {entry}")
        if "Invalid conf" in stderr or "Malformed conf" in stderr:
            raise GateError("jemalloc reported an invalid runtime policy")
    elif stderr is not None and "<jemalloc>:" in stderr:
        raise GateError("unset-default J0 emitted unexpected jemalloc configuration output")
    return effective


def parse_preflight(
    stdout_path: Path,
    stderr_path: Path,
    binary_path: Path,
    plan_path: Path,
    phase1_expectations: Path,
    policy_name: str,
    source_audit_stderr_path: Path | None = None,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    if policy_name not in POLICY_ORDER:
        raise GateError(f"unknown allocator policy: {policy_name}")
    lines = [line for line in stdout_path.read_text(encoding="utf-8").splitlines() if line]
    if len(lines) != 1:
        raise GateError("allocator preflight stdout must contain exactly one JSON line")
    try:
        observed = json.loads(lines[0])
    except json.JSONDecodeError as error:
        raise GateError(f"allocator preflight is not JSON: {error}") from error
    expected = plan["policies"][policy_name]
    validate_application_preflight(
        observed,
        expected,
        context="$preflight.application",
        stderr=stderr_path.read_text(encoding="utf-8"),
    )
    source_audit_sha256 = None
    config_sources_verified = False
    if policy_name != "S":
        if source_audit_stderr_path is None:
            raise GateError("jemalloc preflight requires a separate all-source audit")
        audit_text = source_audit_stderr_path.read_text(encoding="utf-8")
        audit_environment = expected["jemalloc_conf"] or "abort_conf:true,confirm_conf:true"
        validate_jemalloc_config_sources(audit_text, audit_environment)
        source_audit_sha256 = sha256_file(source_audit_stderr_path)
        config_sources_verified = True
    elif source_audit_stderr_path is not None:
        raise GateError("system preflight must not provide a jemalloc source audit")
    return {
        "schema": PREFLIGHT_RECORD_SCHEMA,
        "policy": policy_name,
        "binary_role": expected["binary_role"],
        "binary_sha256": executable_sha256(binary_path),
        "application": observed,
        "jemalloc_confirm_conf_verified": expected["jemalloc_conf"] is not None,
        "jemalloc_config_sources_verified": config_sources_verified,
        "jemalloc_config_source_audit_sha256": source_audit_sha256,
    }


def gate_runtime_log(
    log_path: Path,
    preflight_path: Path,
    plan_path: Path,
    phase1_expectations: Path,
    policy_name: str,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    if policy_name not in POLICY_ORDER:
        raise GateError(f"unknown allocator policy: {policy_name}")
    expected = plan["policies"][policy_name]
    text = log_path.read_text(encoding="utf-8", errors="strict")
    prefix = "CHRONOXIDE_ALLOCATOR_RUNTIME_POLICY_JSON="
    policy_lines = [line[len(prefix) :] for line in text.splitlines() if line.startswith(prefix)]
    if len(policy_lines) != 1:
        raise GateError("runtime log must have exactly one structured allocator-policy record")
    try:
        runtime = require_exact_keys(
            json.loads(policy_lines[0]),
            {
                "schema",
                "rust_global_allocator",
                "jemalloc_conf_env",
                "requested_policy_raw",
                "requested_policy_canonical",
                "effective_policy",
                "post_ingester_drop_hold_secs",
                "post_ingester_drop_checkpoint_enabled",
                "post_ingester_drop_telemetry_enabled",
            },
            "$.runtime_allocator_policy",
        )
    except json.JSONDecodeError as error:
        raise GateError("structured runtime allocator policy is invalid JSON") from error
    if runtime["schema"] != RUNTIME_POLICY_SCHEMA:
        raise GateError("structured runtime allocator policy schema mismatch")
    if runtime["rust_global_allocator"] != expected["rust_global_allocator"]:
        raise GateError("runtime allocator identity does not match the planned binary")
    if runtime["jemalloc_conf_env"] != "_RJEM_MALLOC_CONF":
        raise GateError("runtime allocator record reports the wrong environment key")
    if runtime["requested_policy_raw"] != expected["jemalloc_conf"]:
        raise GateError("runtime allocator raw policy differs from the plan")
    if runtime["requested_policy_canonical"] != expected["jemalloc_conf"]:
        raise GateError("runtime allocator canonical policy differs from the plan")
    expected_hold = plan["workload"]["post_ingester_drop_hold_secs"]
    if runtime["post_ingester_drop_hold_secs"] != expected_hold:
        raise GateError("runtime allocator record has the wrong diagnostic hold")
    if runtime["post_ingester_drop_checkpoint_enabled"] is not True:
        raise GateError("runtime allocator record does not enable its checkpoint")
    if runtime["post_ingester_drop_telemetry_enabled"] is not True:
        raise GateError("runtime allocator record does not enable release telemetry")

    preflight = require_exact_keys(
        load_json(preflight_path), PREFLIGHT_RECORD_KEYS, "$.preflight_record"
    )
    if preflight["schema"] != PREFLIGHT_RECORD_SCHEMA or preflight["policy"] != policy_name:
        raise GateError("runtime gate received the wrong preflight record")
    preflight_effective = preflight["application"]["effective_policy"]
    if runtime["effective_policy"] != preflight_effective:
        raise GateError(
            "runtime effective policy differs from the full eight-field preflight snapshot"
        )
    if runtime["effective_policy"] is not None:
        require_exact_keys(
            runtime["effective_policy"],
            EFFECTIVE_POLICY_KEYS,
            "$.runtime_allocator_policy.effective_policy",
        )
    if "Ingester state dropped; beginning diagnostic allocator release hold" not in text:
        raise GateError("runtime log has no post-Ingester-drop hold marker")
    if "Diagnostic allocator release hold complete" not in text:
        raise GateError("runtime log has no completed release-hold marker")
    conf = expected["jemalloc_conf"]
    if policy_name == "S":
        if "<jemalloc>:" in text:
            raise GateError("system replay emitted jemalloc runtime output")
        if runtime["effective_policy"] is not None:
            raise GateError("system runtime effective policy must be explicit null")
    else:
        if conf is None:
            if "<jemalloc>:" in text:
                raise GateError(
                    "unset-default J0 emitted unexpected jemalloc configuration output"
                )
        else:
            validate_jemalloc_config_sources(text, conf)
            for entry in conf.split(","):
                if f"<jemalloc>: -- Set conf value: {entry}" not in text:
                    raise GateError(f"replay log lacks jemalloc confirmation for {entry}")
        if "Invalid conf" in text or "Malformed conf" in text:
            raise GateError("jemalloc reported an invalid policy during replay")
    return {
        "policy": policy_name,
        "rust_global_allocator": expected["rust_global_allocator"],
        "jemalloc_conf": conf,
        "structured_runtime_policy": runtime,
        "jemalloc_confirm_conf": conf is not None,
        "effective_policy": runtime["effective_policy"],
        "full_effective_policy_matches_preflight": True,
        "post_drop_hold_markers": 2,
    }


def gate_profile_runtime_log(
    log_path: Path,
    preflight_path: Path,
    plan_path: Path,
    phase1_expectations: Path,
    policy_name: str,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    if policy_name not in {"J1", "J2", "J3"}:
        raise GateError("selected-policy profile runtime gate requires J1, J2, or J3")
    expected = plan["policies"][policy_name]
    text = log_path.read_text(encoding="utf-8", errors="strict")
    prefix = "CHRONOXIDE_ALLOCATOR_RUNTIME_POLICY_JSON="
    policy_lines = [
        line[len(prefix) :] for line in text.splitlines() if line.startswith(prefix)
    ]
    if len(policy_lines) != 1:
        raise GateError(
            "profile runtime log must have exactly one structured allocator-policy record"
        )
    try:
        runtime = require_exact_keys(
            json.loads(policy_lines[0]),
            {
                "schema",
                "rust_global_allocator",
                "jemalloc_conf_env",
                "requested_policy_raw",
                "requested_policy_canonical",
                "effective_policy",
                "post_ingester_drop_hold_secs",
                "post_ingester_drop_checkpoint_enabled",
                "post_ingester_drop_telemetry_enabled",
            },
            "$.profile_runtime_allocator_policy",
        )
    except json.JSONDecodeError as error:
        raise GateError("profile runtime allocator policy is invalid JSON") from error
    if (
        runtime["schema"] != RUNTIME_POLICY_SCHEMA
        or runtime["rust_global_allocator"] != "jemalloc"
        or runtime["jemalloc_conf_env"] != "_RJEM_MALLOC_CONF"
        or runtime["requested_policy_raw"] != expected["jemalloc_conf"]
        or runtime["requested_policy_canonical"] != expected["jemalloc_conf"]
        or runtime["post_ingester_drop_hold_secs"] != 0
        or runtime["post_ingester_drop_checkpoint_enabled"] is not False
        or runtime["post_ingester_drop_telemetry_enabled"] is not False
    ):
        raise GateError("profile runtime allocator policy differs from the untimed contract")
    preflight = require_exact_keys(
        load_json(preflight_path), PREFLIGHT_RECORD_KEYS, "$.profile_preflight_record"
    )
    if (
        preflight["schema"] != PREFLIGHT_RECORD_SCHEMA
        or preflight["policy"] != policy_name
        or preflight["binary_role"] != "jemalloc"
        or preflight["jemalloc_confirm_conf_verified"] is not True
        or preflight["jemalloc_config_sources_verified"] is not True
        or runtime["effective_policy"] != preflight["application"]["effective_policy"]
    ):
        raise GateError("profile runtime policy differs from its selected-policy preflight")
    require_exact_keys(
        runtime["effective_policy"],
        EFFECTIVE_POLICY_KEYS,
        "$.profile_runtime_allocator_policy.effective_policy",
    )
    conf = expected["jemalloc_conf"]
    if conf is None:
        raise GateError("selected-policy profile unexpectedly has no jemalloc policy")
    validate_jemalloc_config_sources(text, conf)
    for entry in conf.split(","):
        if f"<jemalloc>: -- Set conf value: {entry}" not in text:
            raise GateError(f"profile runtime log lacks jemalloc confirmation for {entry}")
    if "Invalid conf" in text or "Malformed conf" in text:
        raise GateError("jemalloc reported an invalid selected profile policy")
    if (
        "Ingester state dropped; beginning diagnostic allocator release hold" in text
        or "Diagnostic allocator release hold complete" in text
    ):
        raise GateError("untimed profile unexpectedly enabled the allocator release hold")
    return {
        "policy": policy_name,
        "rust_global_allocator": "jemalloc",
        "jemalloc_conf": conf,
        "structured_runtime_policy": runtime,
        "jemalloc_confirm_conf": True,
        "effective_policy": runtime["effective_policy"],
        "full_effective_policy_matches_preflight": True,
        "post_drop_hold_markers": 0,
        "untimed_profile_runtime": True,
    }


def read_process_stat_identity(pid: int) -> dict[str, int | str] | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    close = raw.rfind(")")
    if close < 0:
        return None
    fields = raw[close + 2 :].split()
    try:
        state = fields[0]
        ppid = int(fields[1])
        starttime_ticks = int(fields[19])
    except (IndexError, ValueError):
        return None
    if len(state) != 1 or ppid < 0 or starttime_ticks < 1:
        return None
    return {
        "pid": pid,
        "ppid": ppid,
        "state": state,
        "starttime_ticks": starttime_ticks,
    }


def process_identity_is_running(identity: dict[str, int | str] | None) -> bool:
    return identity is not None and identity["state"] not in {"Z", "X", "x"}


def process_is_same_running(pid: int, starttime_ticks: int) -> bool:
    current = read_process_stat_identity(pid)
    return bool(
        process_identity_is_running(current)
        and current is not None
        and current["starttime_ticks"] == starttime_ticks
    )


def process_is_same_identity(pid: int, starttime_ticks: int) -> bool:
    current = read_process_stat_identity(pid)
    return bool(
        current is not None and current["starttime_ticks"] == starttime_ticks
    )


def require_running_process_identity(
    pid: int, description: str
) -> dict[str, int | str]:
    identity = read_process_stat_identity(pid)
    if not process_identity_is_running(identity):
        raise GateError(f"{description} is absent, zombie, or exited")
    assert identity is not None
    return identity


def read_process_children(pid: int) -> list[int]:
    try:
        return [
            int(child)
            for child in Path(f"/proc/{pid}/task/{pid}/children")
            .read_text(encoding="ascii")
            .split()
        ]
    except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
        return []


def process_tree_identity_bindings(
    root_pid: int, root_starttime_ticks: int | None = None
) -> dict[int, dict[str, int | str]]:
    if root_starttime_ticks is not None and not process_is_same_running(
        root_pid, root_starttime_ticks
    ):
        return {}
    pending: list[tuple[int, int | None, int | None]] = [
        (root_pid, None, root_starttime_ticks)
    ]
    observed: dict[int, dict[str, int | str]] = {}
    while pending:
        pid, expected_parent, expected_starttime_ticks = pending.pop()
        identity = read_process_stat_identity(pid)
        if (
            pid in observed
            or identity is None
            or (
                pid == root_pid
                and not process_identity_is_running(identity)
            )
            or (
                expected_parent is not None
                and identity["ppid"] != expected_parent
            )
            or (
                expected_starttime_ticks is not None
                and identity["starttime_ticks"] != expected_starttime_ticks
            )
        ):
            continue
        observed[pid] = identity
        # A measured descendant can remain under its live wrapper as a zombie
        # until that wrapper reaps it. Bind its PID, parent, and start time so
        # the guardian can distinguish it from a reused or reparented PID.
        # Only live descendants can still own children.
        if process_identity_is_running(identity):
            pending.extend((child, pid, None) for child in read_process_children(pid))
    return observed


def process_tree(root_pid: int, root_starttime_ticks: int | None = None) -> set[int]:
    return {
        pid
        for pid, identity in process_tree_identity_bindings(
            root_pid, root_starttime_ticks
        ).items()
        if process_identity_is_running(identity)
    }


def process_matches_identity_binding(
    identity: dict[str, int | str] | None,
    binding: dict[str, int | str],
) -> bool:
    return bool(
        identity is not None
        and identity["starttime_ticks"] == binding["starttime_ticks"]
        and identity["ppid"] == binding["ppid"]
    )


def process_binding_chain_is_current(
    pid: int,
    root_pid: int,
    identity_bindings: dict[int, dict[str, int | str]],
) -> bool:
    current_pid = pid
    visited: set[int] = set()
    while current_pid not in visited:
        visited.add(current_pid)
        binding = identity_bindings.get(current_pid)
        if binding is None or not process_matches_identity_binding(
            read_process_stat_identity(current_pid), binding
        ):
            return False
        if current_pid == root_pid:
            return True
        current_pid = int(binding["ppid"])
    return False


COMPILER_PROCESS_TOKEN = (
    r"(?:gcc|g\+\+|cc|c\+\+|clang|clang\+\+)"
    r"(?:[.-](?:real|[0-9][A-Za-z0-9_.-]*))*"
)
LINKER_PROCESS_TOKEN = (
    r"(?:ld(?:\.(?:lld|bfd|gold))?|lld|mold)"
    r"(?:-[0-9][A-Za-z0-9_.-]*)?"
)
NINJA_PROCESS_TOKEN = r"ninja(?:\.real|-[0-9][A-Za-z0-9_.-]*)?"
SOONG_PROCESS_TOKEN = r"(?:soong_ui|soong_build)(?:\.bash)?"
CONTAINER_CLIENT_PROCESS_TOKEN = (
    r"(?:docker|docker-buildx|docker-compose|buildctl|nerdctl|podman|buildah)"
)
FORBIDDEN_MEASUREMENT_COMM = re.compile(
    rf"^(?:cargo|cargo-nextest|rustc|rustdoc|clippy-driver|nextest|make|"
    rf"{NINJA_PROCESS_TOKEN}|cmake|meson|sccache|ccache|"
    rf"{CONTAINER_CLIENT_PROCESS_TOKEN}|"
    rf"emulator|adb|gradle|gradlew|GradleDaemon|{COMPILER_PROCESS_TOKEN}|"
    rf"cc1|cc1plus|{LINKER_PROCESS_TOKEN}|perf|heaptrack|valgrind.*|strace|"
    rf"ltrace|bpftrace|hotspot|qemu-system.*|qemu-kvm|chronoxide-.*|"
    rf"greptime.*|clickhouse.*|postgres.*|mysqld|influxd|victoria.*|"
    rf"vm(?:storage|select|agent)|mimir.*|thanos.*|cortex.*|prometheus|"
    rf"{SOONG_PROCESS_TOKEN}|ckati|kati|javac|kotlinc|metalava|aapt|aapt2|"
    rf"aidl|dex2oat|btop|htop|top)$",
    re.IGNORECASE,
)
FORBIDDEN_MEASUREMENT_COMMAND = re.compile(
    rf"(?:^|[/ ])(?:cargo(?:-nextest)?|rustc|rustdoc|clippy-driver|nextest|"
    rf"{NINJA_PROCESS_TOKEN}|{COMPILER_PROCESS_TOKEN}|{LINKER_PROCESS_TOKEN}|"
    rf"{SOONG_PROCESS_TOKEN}|ckati|kati|gradlew?|metalava|aapt2?|aidl|"
    rf"dex2oat)(?:$|[ /])|org\.gradle\.|GradleDaemon|GradleWorkerMain|"
    rf"gradle-worker|Android[/ ](?:SDK )?emulator",
    re.IGNORECASE,
)


def proc_identity(pid: int) -> tuple[str, str] | None:
    try:
        comm = Path(f"/proc/{pid}/comm").read_text(encoding="utf-8").strip()
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    return comm, raw.replace(b"\0", b" ").decode("utf-8", errors="replace").strip()


def scan_guardian_conflicts(
    identity_bindings: dict[int, dict[str, int | str]],
    root_pid: int,
    root_starttime_ticks: int,
    guardian_pid: int,
    poll: int,
    monotonic_elapsed_ns: int,
) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == guardian_pid:
            continue
        current_stat: dict[str, int | str] | None = None
        binding = identity_bindings.get(pid)
        if binding is not None and process_binding_chain_is_current(
            pid, root_pid, identity_bindings
        ):
            continue
        if binding is not None:
            current_stat = read_process_stat_identity(pid)
        if pid == root_pid and process_is_same_identity(
            root_pid, root_starttime_ticks
        ):
            continue
        identity = proc_identity(pid)
        if identity is None:
            continue
        comm, command = identity
        if FORBIDDEN_MEASUREMENT_COMM.fullmatch(
            comm
        ) or FORBIDDEN_MEASUREMENT_COMMAND.search(command):
            if current_stat is None:
                current_stat = read_process_stat_identity(pid)
            conflicts.append(
                {
                    "poll": poll,
                    "monotonic_elapsed_ns": monotonic_elapsed_ns,
                    "pid": pid,
                    "ppid": (
                        current_stat["ppid"] if current_stat is not None else None
                    ),
                    "state": (
                        current_stat["state"] if current_stat is not None else None
                    ),
                    "starttime_ticks": (
                        current_stat["starttime_ticks"]
                        if current_stat is not None
                        else None
                    ),
                    "comm": comm,
                    "command": command,
                }
            )
    return conflicts


def validate_process_snapshot(snapshot_path: Path, allowed_pids: set[int]) -> dict[str, Any]:
    if snapshot_path.is_symlink() or not snapshot_path.is_file():
        raise GateError("process snapshot must be a regular non-symlink file")
    rows = 0
    conflicts: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        snapshot_path.read_text(encoding="utf-8", errors="strict").splitlines(),
        start=1,
    ):
        fields = line.strip().split(None, 8)
        if len(fields) != 9:
            raise GateError(f"process snapshot line {line_number} has an invalid shape")
        pid_text, parent_text, cpu_text, memory_text, rss_text, _elapsed, _state, comm, command = fields
        try:
            pid = int(pid_text)
            int(parent_text)
            cpu = float(cpu_text)
            memory = float(memory_text)
            int(rss_text)
        except ValueError as error:
            raise GateError(
                f"process snapshot line {line_number} has an invalid numeric field"
            ) from error
        if not math.isfinite(cpu) or cpu < 0 or not math.isfinite(memory) or memory < 0:
            raise GateError(f"process snapshot line {line_number} has invalid utilization")
        rows += 1
        if pid in allowed_pids:
            continue
        if FORBIDDEN_MEASUREMENT_COMM.fullmatch(
            comm
        ) or FORBIDDEN_MEASUREMENT_COMMAND.search(command):
            conflicts.append(
                {"line": line_number, "pid": pid, "comm": comm, "command": command}
            )
    if rows == 0:
        raise GateError("process snapshot is empty")
    if conflicts:
        raise GateError(f"external measurement conflict observed: {conflicts[0]!r}")
    return {"status": "pass", "rows": rows, "conflicts": []}


def guardian_maximum_allowed_gap_ns(interval_ms: int) -> int:
    if interval_ms < 1:
        raise GateError("guardian cadence interval must be positive")
    return interval_ms * 1_000_000 + GUARDIAN_CADENCE_EDGE_ALLOWANCE_NS


def derive_guardian_maximum_poll_start_gap_ns(
    timestamps: list[int], elapsed_ns: int
) -> int:
    boundaries = [0, *timestamps, elapsed_ns]
    return max(
        (later - earlier for earlier, later in zip(boundaries, boundaries[1:])),
        default=0,
    )


def create_empty_read_only_marker(path: Path, description: str) -> None:
    if not path.is_absolute():
        raise GateError(f"{description} path must be absolute")
    directory_non_symlink(path.parent, f"{description} parent")
    if path.exists() or path.is_symlink():
        raise GateError(f"refusing to reuse {description}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o444)
    os.close(descriptor)
    marker = regular_non_symlink(path, description)
    if marker.stat().st_size != 0 or stat.S_IMODE(marker.stat().st_mode) != 0o444:
        raise GateError(f"{description} is not exact empty mode 0444")


def validate_empty_read_only_marker(path: Path, description: str) -> Path:
    marker = regular_non_symlink(path, description)
    if marker.stat().st_size != 0 or stat.S_IMODE(marker.stat().st_mode) != 0o444:
        raise GateError(f"{description} must be exact empty mode 0444")
    return marker


def validate_guardian_control(
    path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    *,
    expected_root_pid: int | None = None,
    expected_guardian_pid: int | None = None,
    require_live: bool = False,
    require_rss_monitor: bool | None = None,
) -> dict[str, Any]:
    control_path = regular_non_symlink(path, "guardian launch control")
    if stat.S_IMODE(control_path.stat().st_mode) != 0o444:
        raise GateError("guardian launch control must have exact mode 0444")
    raw = load_json(control_path)
    if not isinstance(raw, dict):
        raise GateError("guardian launch control must be an object")
    has_rss = raw.get("schema") == GUARDIAN_CONTROL_SCHEMA
    expected_keys = {
        "schema",
        "root_pid",
        "root_starttime_ticks",
        "guardian_pid",
        "guardian_starttime_ticks",
        "interval_ms",
        "ready_marker",
        "launch_marker",
    }
    if has_rss:
        expected_keys |= {
            "rss_monitor_pid",
            "rss_monitor_starttime_ticks",
            "rss_ready_marker",
        }
    value = require_exact_keys(raw, expected_keys, "$.guardian_control")
    if value["schema"] not in {GUARDIAN_CONTROL_SCHEMA, GUARDIAN_ROOT_CONTROL_SCHEMA}:
        raise GateError("guardian launch control schema mismatch")
    if require_rss_monitor is not None and has_rss != require_rss_monitor:
        raise GateError("guardian launch control role set differs")
    roles = ["root", "guardian", *( ["rss_monitor"] if has_rss else [])]
    pids = {
        role: strict_int(value[f"{role}_pid"], f"$.guardian_control.{role}_pid", minimum=1)
        for role in roles
    }
    starttimes = {
        role: strict_int(
            value[f"{role}_starttime_ticks"],
            f"$.guardian_control.{role}_starttime_ticks",
            minimum=1,
        )
        for role in roles
    }
    if len(set(pids.values())) != len(pids):
        raise GateError("guardian launch control PIDs must be distinct")
    if (
        value["interval_ms"] != interval_ms
        or value["ready_marker"] != str(ready_path)
        or value["launch_marker"] != str(launch_path)
        or not ready_path.is_absolute()
        or not launch_path.is_absolute()
        or ready_path.parent != control_path.parent
        or launch_path.parent != control_path.parent
        or has_rss
        and (
            not isinstance(value["rss_ready_marker"], str)
            or not Path(value["rss_ready_marker"]).is_absolute()
            or Path(value["rss_ready_marker"]).parent != control_path.parent
        )
        or expected_root_pid is not None
        and pids["root"] != expected_root_pid
        or expected_guardian_pid is not None
        and pids["guardian"] != expected_guardian_pid
    ):
        raise GateError("guardian launch control differs from the exact handshake")
    if require_live:
        dead = [
            role
            for role in roles
            if not process_is_same_running(pids[role], starttimes[role])
        ]
        if dead:
            raise GateError(
                "guardian launch control has exited, zombie, or reused processes: "
                f"{dead!r}"
            )
    return value


def create_guardian_control(
    output: Path,
    ready_path: Path,
    launch_path: Path,
    root_pid: int,
    guardian_pid: int,
    interval_ms: int,
    rss_monitor_pid: int | None = None,
    rss_ready_path: Path | None = None,
) -> dict[str, Any]:
    for marker, description in (
        (ready_path, "guardian ready marker"),
        (launch_path, "guardian launch marker"),
    ):
        if marker.exists() or marker.is_symlink():
            raise GateError(f"refusing to reuse {description}")
    identities = {
        "root": require_running_process_identity(root_pid, "held measured root"),
        "guardian": require_running_process_identity(guardian_pid, "guardian"),
    }
    if rss_monitor_pid is not None:
        if (
            rss_ready_path is None
            or not rss_ready_path.is_absolute()
            or rss_ready_path.parent != output.parent
        ):
            raise GateError("RSS-bearing guardian control needs an absolute ready marker")
        if rss_ready_path.exists() or rss_ready_path.is_symlink():
            raise GateError("refusing to reuse RSS ready marker")
        identities["rss_monitor"] = require_running_process_identity(
            rss_monitor_pid, "RSS monitor"
        )
    elif rss_ready_path is not None:
        raise GateError("root-only guardian control cannot bind an RSS ready marker")
    value = {
        "schema": (
            GUARDIAN_CONTROL_SCHEMA
            if rss_monitor_pid is not None
            else GUARDIAN_ROOT_CONTROL_SCHEMA
        ),
        "root_pid": root_pid,
        "root_starttime_ticks": identities["root"]["starttime_ticks"],
        "guardian_pid": guardian_pid,
        "guardian_starttime_ticks": identities["guardian"]["starttime_ticks"],
        "interval_ms": interval_ms,
        "ready_marker": str(ready_path),
        "launch_marker": str(launch_path),
    }
    if rss_monitor_pid is not None:
        value.update(
            {
                "rss_monitor_pid": rss_monitor_pid,
                "rss_monitor_starttime_ticks": identities["rss_monitor"][
                    "starttime_ticks"
                ],
                "rss_ready_marker": str(rss_ready_path),
            }
        )
    publish_json_read_only_atomic_exclusive(output, value)
    current = validate_guardian_control(
        output,
        ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=root_pid,
        expected_guardian_pid=guardian_pid,
        require_live=True,
        require_rss_monitor=rss_monitor_pid is not None,
    )
    if current != value:
        raise GateError("fresh guardian launch control failed self-validation")
    return value


def wait_for_guardian_ready(
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    timeout_ms: int,
) -> dict[str, Any]:
    if timeout_ms < interval_ms:
        raise GateError("guardian readiness timeout is shorter than one poll")
    deadline = time.monotonic() + timeout_ms / 1000
    while True:
        control = validate_guardian_control(
            control_path, ready_path, launch_path, interval_ms, require_live=True
        )
        if launch_path.exists() or launch_path.is_symlink():
            raise GateError("guardian launch marker appeared before readiness")
        if ready_path.exists() or ready_path.is_symlink():
            validate_empty_read_only_marker(ready_path, "guardian ready marker")
            return {"status": "ready", "root_pid": control["root_pid"]}
        if time.monotonic() >= deadline:
            raise GateError("guardian did not become ready before the bounded timeout")
        time.sleep(0.01)


def release_guardian_launch(
    control_path: Path, ready_path: Path, launch_path: Path, interval_ms: int
) -> dict[str, Any]:
    control = validate_guardian_control(
        control_path, ready_path, launch_path, interval_ms, require_live=True
    )
    validate_empty_read_only_marker(ready_path, "guardian ready marker")
    create_empty_read_only_marker(launch_path, "guardian launch marker")
    return {"status": "released", "root_pid": control["root_pid"]}


def snapshot_process_tree_identities(
    root_pid: int, root_starttime_ticks: int
) -> list[dict[str, int | str]]:
    identities: dict[int, dict[str, int | str]] = {}
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            identity = read_process_stat_identity(int(entry.name))
            if identity is not None:
                identities[int(entry.name)] = identity
    root = identities.get(root_pid)
    if (
        not process_identity_is_running(root)
        or root is None
        or root["starttime_ticks"] != root_starttime_ticks
    ):
        return []
    depths = {root_pid: 0}
    changed = True
    while changed:
        changed = False
        for pid, identity in identities.items():
            parent = int(identity["ppid"])
            if parent in depths and pid not in depths:
                depths[pid] = depths[parent] + 1
                changed = True
    return sorted(
        ({**identities[pid], "depth": depth} for pid, depth in depths.items()),
        key=lambda value: (int(value["depth"]), int(value["pid"])),
        reverse=True,
    )


def process_identity_refusal(target: dict[str, int | str]) -> str | None:
    current = read_process_stat_identity(int(target["pid"]))
    if current is None:
        return "exited"
    if current["starttime_ticks"] != target["starttime_ticks"]:
        return "starttime_mismatch"
    if current["state"] in {"Z", "X", "x"}:
        return f"state_{current['state']}"
    return None


def wait_for_process_identities_exit(
    targets: list[dict[str, int | str]], timeout_seconds: float
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline and any(
        process_is_same_running(int(target["pid"]), int(target["starttime_ticks"]))
        for target in targets
    ):
        time.sleep(0.01)


def terminate_process_tree(
    root_pid: int, root_starttime_ticks: int
) -> dict[str, Any]:
    targets = [
        target
        for target in snapshot_process_tree_identities(root_pid, root_starttime_ticks)
        if target["pid"] != os.getpid()
    ]
    term_sent: list[int] = []
    kill_sent: list[int] = []
    term_errors: list[dict[str, Any]] = []
    kill_errors: list[dict[str, Any]] = []
    identity_refusals: list[dict[str, Any]] = []
    for signal_name, signal_value, sent, errors in (
        ("TERM", signal.SIGTERM, term_sent, term_errors),
        ("KILL", signal.SIGKILL, kill_sent, kill_errors),
    ):
        if signal_name == "KILL":
            wait_for_process_identities_exit(targets, 0.5)
        for target in targets:
            pid = int(target["pid"])
            refusal = process_identity_refusal(target)
            if refusal is not None:
                if refusal not in {"exited", "state_Z", "state_X", "state_x"}:
                    identity_refusals.append(
                        {"pid": pid, "signal": signal_name, "reason": refusal}
                    )
                continue
            try:
                os.kill(pid, signal_value)
                sent.append(pid)
            except ProcessLookupError:
                continue
            except PermissionError as error:
                errors.append(
                    {"pid": pid, "signal": signal_name, "error": str(error)}
                )
    wait_for_process_identities_exit(targets, 0.5)
    survivors = [
        int(target["pid"])
        for target in targets
        if process_is_same_running(int(target["pid"]), int(target["starttime_ticks"]))
    ]
    return {
        "attempted": True,
        "root_starttime_ticks": root_starttime_ticks,
        "target_processes": targets,
        "target_pids": [int(target["pid"]) for target in targets],
        "term_sent_pids": term_sent,
        "term_errors": term_errors,
        "kill_sent_pids": kill_sent,
        "kill_errors": kill_errors,
        "identity_refusals": identity_refusals,
        "surviving_pids": survivors,
    }


def cleanup_guardian_processes(
    control_path: Path, ready_path: Path, launch_path: Path, interval_ms: int
) -> dict[str, Any]:
    control = validate_guardian_control(
        control_path, ready_path, launch_path, interval_ms, require_live=False
    )
    roles = ["root"]
    if control["schema"] == GUARDIAN_CONTROL_SCHEMA:
        roles.append("rss_monitor")
    roles.append("guardian")
    terminations = {
        role: terminate_process_tree(
            control[f"{role}_pid"], control[f"{role}_starttime_ticks"]
        )
        for role in roles
    }
    incomplete = {
        role: {
            key: evidence[key]
            for key in ("term_errors", "kill_errors", "surviving_pids")
            if evidence[key]
        }
        for role, evidence in terminations.items()
    }
    incomplete = {role: value for role, value in incomplete.items() if value}
    if incomplete:
        raise GateError(f"guardian-controlled cleanup was incomplete: {incomplete!r}")
    return {
        "schema": GUARDIAN_CLEANUP_SCHEMA,
        "control_path": str(control_path),
        "control_sha256": sha256_file(control_path),
        "termination_order": roles,
        "terminations": terminations,
    }


def require_clean_termination(evidence: dict[str, Any], description: str) -> None:
    failures = {
        key: evidence[key]
        for key in ("term_errors", "kill_errors", "surviving_pids")
        if evidence[key]
    }
    if failures:
        raise GateError(f"{description} cleanup was incomplete: {failures!r}")


def monitor_external_conflicts(
    root_pid: int,
    output_path: Path,
    interval_ms: int,
    filesystem_path: Path,
    minimum_free_bytes: int,
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
) -> dict[str, Any]:
    if root_pid <= 0 or interval_ms < 10 or minimum_free_bytes < 1:
        raise GateError("external-conflict guardian arguments are invalid")
    if output_path.exists() or output_path.is_symlink():
        raise GateError("refusing to reuse external-conflict guardian output")
    if not filesystem_path.is_absolute():
        raise GateError("guardian filesystem path must be absolute")
    filesystem = str(
        directory_non_symlink(filesystem_path, "guardian filesystem").resolve(
            strict=True
        )
    )
    for marker, description in (
        (ready_path, "guardian ready marker"),
        (launch_path, "guardian launch marker"),
    ):
        if marker.exists() or marker.is_symlink():
            raise GateError(f"refusing to reuse {description}")
    initial_root = require_running_process_identity(root_pid, "held measured root")
    root_starttime_ticks = int(initial_root["starttime_ticks"])
    deadline = time.monotonic() + 5.0
    while not control_path.exists() and not control_path.is_symlink():
        if not process_is_same_running(root_pid, root_starttime_ticks):
            raise GateError("held measured root exited before guardian control")
        if time.monotonic() >= deadline:
            raise GateError("guardian launch control was not created in time")
        time.sleep(0.005)
    control = validate_guardian_control(
        control_path,
        ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=root_pid,
        expected_guardian_pid=os.getpid(),
        require_live=True,
    )
    if control["root_starttime_ticks"] != root_starttime_ticks:
        raise GateError("guardian control bound a reused held-root PID")
    if control["schema"] == GUARDIAN_CONTROL_SCHEMA:
        rss_ready_path = Path(control["rss_ready_marker"])
        rss_deadline = time.monotonic() + 5.0
        while not rss_ready_path.exists() and not rss_ready_path.is_symlink():
            if not process_is_same_running(
                control["rss_monitor_pid"],
                control["rss_monitor_starttime_ticks"],
            ):
                raise GateError("RSS monitor exited before its ready marker")
            if not process_is_same_running(root_pid, root_starttime_ticks):
                raise GateError("held measured root exited before RSS readiness")
            if time.monotonic() >= rss_deadline:
                raise GateError("RSS monitor did not become ready before timeout")
            time.sleep(0.005)
        validate_empty_read_only_marker(rss_ready_path, "RSS ready marker")
    started = time.monotonic_ns()
    allowed_gap_ns = guardian_maximum_allowed_gap_ns(interval_ms)
    timestamps: list[int] = []
    polls = 0
    live_polls = 0
    terminal_poll_index: int | None = None
    root_seen = False
    ready_poll: int | None = None
    ready_elapsed: int | None = None
    launch_poll: int | None = None
    launch_elapsed: int | None = None
    launch_observed_root_bound = False
    handshake_violations: list[str] = []
    conflicts: list[dict[str, Any]] = []
    capacity_violations: list[dict[str, Any]] = []
    minimum_observed_free_bytes: int | None = None
    termination: dict[str, Any] = {
        "attempted": False,
        "root_starttime_ticks": root_starttime_ticks,
        "target_processes": [],
        "target_pids": [],
        "term_sent_pids": [],
        "term_errors": [],
        "kill_sent_pids": [],
        "kill_errors": [],
        "identity_refusals": [],
        "surviving_pids": [],
    }
    while True:
        identity_bindings = process_tree_identity_bindings(
            root_pid, root_starttime_ticks
        )
        allowed = {
            pid
            for pid, identity in identity_bindings.items()
            if process_identity_is_running(identity)
        }
        terminal_poll = False
        if allowed:
            root_seen = True
            live_polls += 1
        elif root_seen or not process_is_same_running(root_pid, root_starttime_ticks):
            terminal_poll = True
        timestamps.append(time.monotonic_ns() - started)
        polls += 1
        if terminal_poll:
            terminal_poll_index = polls
        conflicts.extend(
            scan_guardian_conflicts(
                identity_bindings,
                root_pid,
                root_starttime_ticks,
                os.getpid(),
                polls,
                timestamps[-1],
            )
        )
        filesystem_stats = os.statvfs(filesystem_path)
        free_bytes = filesystem_stats.f_bavail * filesystem_stats.f_frsize
        minimum_observed_free_bytes = (
            free_bytes
            if minimum_observed_free_bytes is None
            else min(minimum_observed_free_bytes, free_bytes)
        )
        if free_bytes < minimum_free_bytes:
            capacity_violations.append(
                {
                    "poll": polls,
                    "monotonic_elapsed_ns": timestamps[-1],
                    "free_bytes": free_bytes,
                    "minimum_free_bytes": minimum_free_bytes,
                }
            )
        failed = bool(
            conflicts
            or capacity_violations
            or derive_guardian_maximum_poll_start_gap_ns(timestamps, timestamps[-1])
            > allowed_gap_ns
        )
        if ready_poll is None:
            if launch_path.exists() or launch_path.is_symlink():
                handshake_violations.append("launch marker existed before readiness")
            elif not failed and allowed:
                if control["schema"] == GUARDIAN_CONTROL_SCHEMA:
                    try:
                        validate_empty_read_only_marker(
                            Path(control["rss_ready_marker"]), "RSS ready marker"
                        )
                    except GateError as error:
                        handshake_violations.append(str(error))
                if not handshake_violations:
                    create_empty_read_only_marker(ready_path, "guardian ready marker")
                    ready_poll = polls
                    ready_elapsed = timestamps[-1]
        else:
            try:
                validate_empty_read_only_marker(ready_path, "guardian ready marker")
            except GateError as error:
                handshake_violations.append(str(error))
            if control["schema"] == GUARDIAN_CONTROL_SCHEMA:
                try:
                    validate_empty_read_only_marker(
                        Path(control["rss_ready_marker"]), "RSS ready marker"
                    )
                except GateError as error:
                    handshake_violations.append(str(error))
            if launch_path.exists() or launch_path.is_symlink():
                try:
                    validate_empty_read_only_marker(
                        launch_path, "guardian launch marker"
                    )
                except GateError as error:
                    handshake_violations.append(str(error))
                else:
                    if launch_poll is None:
                        launch_poll = polls
                        launch_elapsed = timestamps[-1]
                        launch_observed_root_bound = bool(allowed)
        if failed or handshake_violations:
            termination = terminate_process_tree(root_pid, root_starttime_ticks)
            break
        if terminal_poll:
            break
        time.sleep(interval_ms / 1000)
    elapsed_ns = time.monotonic_ns() - started
    maximum_gap_ns = derive_guardian_maximum_poll_start_gap_ns(timestamps, elapsed_ns)
    ready_sha: str | None = None
    launch_sha: str | None = None
    try:
        ready_sha = sha256_file(
            validate_empty_read_only_marker(ready_path, "guardian ready marker")
        )
    except GateError as error:
        handshake_violations.append(str(error))
    try:
        launch_sha = sha256_file(
            validate_empty_read_only_marker(launch_path, "guardian launch marker")
        )
    except GateError as error:
        handshake_violations.append(str(error))
    if launch_poll is None:
        handshake_violations.append("guardian never observed the launch marker")
    elif not launch_observed_root_bound:
        handshake_violations.append(
            "guardian observed the launch marker only after the root stopped"
        )
    result = {
        "schema": GUARDIAN_SCHEMA,
        "root_pid": root_pid,
        "root_starttime_ticks": root_starttime_ticks,
        "guardian_pid": os.getpid(),
        "interval_ms": interval_ms,
        "polls": polls,
        "live_polls": live_polls,
        "terminal_poll": terminal_poll_index,
        "elapsed_ns": elapsed_ns,
        "poll_monotonic_elapsed_ns": timestamps,
        "maximum_poll_start_gap_ns": maximum_gap_ns,
        "maximum_allowed_poll_start_gap_ns": allowed_gap_ns,
        "control_path": str(control_path),
        "control_sha256": sha256_file(control_path),
        "ready_marker_path": str(ready_path),
        "ready_marker_sha256": ready_sha,
        "ready_created_poll": ready_poll,
        "ready_created_monotonic_elapsed_ns": ready_elapsed,
        "launch_marker_path": str(launch_path),
        "launch_marker_sha256": launch_sha,
        "launch_observed_poll": launch_poll,
        "launch_observed_monotonic_elapsed_ns": launch_elapsed,
        "launch_observed": launch_poll is not None,
        "launch_observed_root_bound": launch_observed_root_bound,
        "handshake_violations": handshake_violations,
        "root_seen": root_seen,
        "filesystem": filesystem,
        "minimum_free_bytes": minimum_free_bytes,
        "minimum_observed_free_bytes": minimum_observed_free_bytes,
        "capacity_violations": capacity_violations,
        "conflicts": conflicts,
        "termination": termination,
        "complete_and_conflict_free": (
            root_seen
            and live_polls >= 2
            and terminal_poll_index == polls
            and live_polls == terminal_poll_index - 1
            and maximum_gap_ns <= allowed_gap_ns
            and ready_poll is not None
            and launch_poll is not None
            and ready_poll < launch_poll
            and launch_poll < terminal_poll_index
            and not handshake_violations
            and not conflicts
            and not capacity_violations
        ),
    }
    write_json_exclusive(output_path, result)
    if conflicts:
        raise GateError(f"external measurement conflict observed: {conflicts[0]!r}")
    if capacity_violations:
        raise GateError(f"result filesystem reserve exhausted: {capacity_violations[0]!r}")
    if handshake_violations:
        raise GateError(f"guardian held-launch handshake failed: {handshake_violations!r}")
    if maximum_gap_ns > allowed_gap_ns:
        raise GateError("guardian cadence maximum gap exceeds its exact allowance")
    if not root_seen or live_polls < 2:
        raise GateError("guardian observed fewer than two live process polls")
    return result


def validate_guardian_evidence(
    path: Path,
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    expected_filesystem: Path,
    expected_minimum_free_bytes: int,
    require_rss_monitor: bool,
) -> dict[str, Any]:
    value = require_exact_keys(
        load_json(path),
        {
            "schema",
            "root_pid",
            "root_starttime_ticks",
            "guardian_pid",
            "interval_ms",
            "polls",
            "live_polls",
            "terminal_poll",
            "elapsed_ns",
            "poll_monotonic_elapsed_ns",
            "maximum_poll_start_gap_ns",
            "maximum_allowed_poll_start_gap_ns",
            "control_path",
            "control_sha256",
            "ready_marker_path",
            "ready_marker_sha256",
            "ready_created_poll",
            "ready_created_monotonic_elapsed_ns",
            "launch_marker_path",
            "launch_marker_sha256",
            "launch_observed_poll",
            "launch_observed_monotonic_elapsed_ns",
            "launch_observed",
            "launch_observed_root_bound",
            "handshake_violations",
            "root_seen",
            "filesystem",
            "minimum_free_bytes",
            "minimum_observed_free_bytes",
            "capacity_violations",
            "conflicts",
            "termination",
            "complete_and_conflict_free",
        },
        "$.guardian",
    )
    polls = strict_int(value["polls"], "$.guardian.polls", minimum=2)
    live_polls = strict_int(
        value["live_polls"], "$.guardian.live_polls", minimum=2
    )
    terminal_poll = strict_int(
        value["terminal_poll"], "$.guardian.terminal_poll", minimum=2
    )
    elapsed_ns = strict_int(value["elapsed_ns"], "$.guardian.elapsed_ns", minimum=1)
    timestamps = value["poll_monotonic_elapsed_ns"]
    if not isinstance(timestamps, list) or len(timestamps) != polls:
        raise GateError("guardian cadence timestamp count differs from polls")
    previous: int | None = None
    for index, raw in enumerate(timestamps):
        timestamp = strict_int(
            raw, f"$.guardian.poll_monotonic_elapsed_ns[{index}]", minimum=0
        )
        if timestamp > elapsed_ns:
            raise GateError("guardian cadence timestamp exceeds guardian elapsed time")
        if previous is not None and timestamp <= previous:
            raise GateError("guardian cadence timestamps are not strictly increasing")
        previous = timestamp
    derived_gap = derive_guardian_maximum_poll_start_gap_ns(timestamps, elapsed_ns)
    allowed_gap = guardian_maximum_allowed_gap_ns(interval_ms)
    if (
        strict_int(
            value["maximum_poll_start_gap_ns"],
            "$.guardian.maximum_poll_start_gap_ns",
            minimum=0,
        )
        != derived_gap
        or strict_int(
            value["maximum_allowed_poll_start_gap_ns"],
            "$.guardian.maximum_allowed_poll_start_gap_ns",
            minimum=1,
        )
        != allowed_gap
    ):
        raise GateError("guardian cadence maximum gap is not exactly derived")
    if derived_gap > allowed_gap:
        raise GateError("guardian cadence maximum gap exceeds its exact allowance")
    root_pid = strict_int(value["root_pid"], "$.guardian.root_pid", minimum=1)
    root_starttime = strict_int(
        value["root_starttime_ticks"],
        "$.guardian.root_starttime_ticks",
        minimum=1,
    )
    guardian_pid = strict_int(
        value["guardian_pid"], "$.guardian.guardian_pid", minimum=1
    )
    control = validate_guardian_control(
        control_path,
        ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=root_pid,
        expected_guardian_pid=guardian_pid,
        require_rss_monitor=require_rss_monitor,
    )
    ready = validate_empty_read_only_marker(ready_path, "guardian ready marker")
    launch = validate_empty_read_only_marker(launch_path, "guardian launch marker")
    if require_rss_monitor:
        expected_rss_ready = control_path.with_name("rss-monitor-ready")
        if control["rss_ready_marker"] != str(expected_rss_ready):
            raise GateError("guardian control RSS ready marker path is not canonical")
        validate_empty_read_only_marker(expected_rss_ready, "RSS ready marker")
    ready_poll = strict_int(
        value["ready_created_poll"], "$.guardian.ready_created_poll", minimum=1
    )
    launch_poll = strict_int(
        value["launch_observed_poll"], "$.guardian.launch_observed_poll", minimum=1
    )
    ready_elapsed = strict_int(
        value["ready_created_monotonic_elapsed_ns"],
        "$.guardian.ready_created_monotonic_elapsed_ns",
        minimum=0,
    )
    launch_elapsed = strict_int(
        value["launch_observed_monotonic_elapsed_ns"],
        "$.guardian.launch_observed_monotonic_elapsed_ns",
        minimum=0,
    )
    if (
        ready_poll != 1
        or launch_poll <= ready_poll
        or launch_poll > polls
        or terminal_poll != polls
        or live_polls != terminal_poll - 1
        or live_polls != polls - 1
        or launch_poll >= terminal_poll
        or ready_elapsed != timestamps[ready_poll - 1]
        or launch_elapsed != timestamps[launch_poll - 1]
        or ready_elapsed >= launch_elapsed
        or value["launch_observed"] is not True
        or value["launch_observed_root_bound"] is not True
        or value["handshake_violations"] != []
        or value["control_path"] != str(control_path)
        or value["control_sha256"] != sha256_file(control_path)
        or value["ready_marker_path"] != str(ready_path)
        or value["ready_marker_sha256"] != sha256_file(ready)
        or value["launch_marker_path"] != str(launch_path)
        or value["launch_marker_sha256"] != sha256_file(launch)
        or control["root_starttime_ticks"] != root_starttime
    ):
        raise GateError("guardian held-launch handshake is not exact and causal")
    expected_filesystem = directory_non_symlink(
        expected_filesystem, "expected guardian filesystem"
    ).resolve(strict=True)
    empty_termination = {
        "attempted": False,
        "root_starttime_ticks": root_starttime,
        "target_processes": [],
        "target_pids": [],
        "term_sent_pids": [],
        "term_errors": [],
        "kill_sent_pids": [],
        "kill_errors": [],
        "identity_refusals": [],
        "surviving_pids": [],
    }
    if (
        value["schema"] != GUARDIAN_SCHEMA
        or value["interval_ms"] != interval_ms
        or value["root_seen"] is not True
        or value["filesystem"] != str(expected_filesystem)
        or strict_int(
            value["minimum_free_bytes"],
            "$.guardian.minimum_free_bytes",
            minimum=1,
        )
        != expected_minimum_free_bytes
        or strict_int(
            value["minimum_observed_free_bytes"],
            "$.guardian.minimum_observed_free_bytes",
            minimum=0,
        )
        < expected_minimum_free_bytes
        or value["capacity_violations"] != []
        or value["conflicts"] != []
        or value["termination"] != empty_termination
        or value["complete_and_conflict_free"] is not True
    ):
        raise GateError("continuous quiet-host/capacity guardian did not pass")
    return value


def meminfo_dirty_writeback_kib() -> tuple[int, int]:
    values: dict[str, int] = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"(Dirty|Writeback):\s+([0-9]+) kB", line)
        if match:
            values[match.group(1)] = int(match.group(2))
    if set(values) != {"Dirty", "Writeback"}:
        raise GateError("/proc/meminfo lacks Dirty or Writeback counters")
    return values["Dirty"], values["Writeback"]


def sync_and_wait_writeback_quiescent(
    corpus: Path,
    samples_path: Path,
    summary_path: Path,
    maximum_kib: int,
    consecutive_required: int,
    interval_ms: int,
    timeout_secs: int,
) -> dict[str, Any]:
    if not corpus.is_absolute() or corpus.is_symlink() or not corpus.is_dir():
        raise GateError("quiescence corpus must be an absolute non-symlink directory")
    if samples_path.exists() or summary_path.exists():
        raise GateError("refusing to reuse writeback-quiescence output")
    if maximum_kib < 0 or consecutive_required < 1 or interval_ms < 10 or timeout_secs < 1:
        raise GateError("writeback-quiescence arguments are invalid")
    files = sorted(path for path in corpus.rglob("*") if path.is_file())
    directories = sorted(
        (path for path in corpus.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for path in files:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    for path in [*directories, corpus]:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    os.sync()

    started = time.monotonic_ns()
    deadline = started + timeout_secs * 1_000_000_000
    consecutive = 0
    samples = 0
    last_dirty = last_writeback = 0
    with samples_path.open("x", encoding="utf-8") as destination:
        destination.write("elapsed_ns\tdirty_kib\twriteback_kib\ttotal_kib\twithin_limit\n")
        while time.monotonic_ns() <= deadline:
            dirty, writeback = meminfo_dirty_writeback_kib()
            total = dirty + writeback
            within = total <= maximum_kib
            consecutive = consecutive + 1 if within else 0
            samples += 1
            last_dirty, last_writeback = dirty, writeback
            destination.write(
                f"{time.monotonic_ns() - started}\t{dirty}\t{writeback}\t{total}\t"
                f"{str(within).lower()}\n"
            )
            destination.flush()
            if consecutive >= consecutive_required:
                break
            time.sleep(interval_ms / 1000.0)
    passed = consecutive >= consecutive_required
    result = {
        "schema": "chronoxide/storage-vnext-phase5-writeback-quiescence/v1",
        "corpus": str(corpus),
        "fsynced_file_count": len(files),
        "global_sync_called": True,
        "maximum_dirty_writeback_kib": maximum_kib,
        "required_consecutive_samples": consecutive_required,
        "interval_ms": interval_ms,
        "timeout_secs": timeout_secs,
        "sample_count": samples,
        "final_dirty_kib": last_dirty,
        "final_writeback_kib": last_writeback,
        "final_total_kib": last_dirty + last_writeback,
        "passed": passed,
    }
    write_json_exclusive(summary_path, result)
    if not passed:
        raise GateError("Dirty+Writeback did not reach the frozen quiescence threshold")
    return result


def status_kib(pid: int) -> dict[str, int] | None:
    try:
        lines = Path(f"/proc/{pid}/status").read_text(encoding="ascii").splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    wanted = {"VmRSS", "VmHWM", "RssAnon", "RssFile", "VmSwap"}
    result = {name: 0 for name in wanted}
    for line in lines:
        fields = line.split()
        if (
            fields
            and fields[0] == "State:"
            and len(fields) >= 2
            and fields[1] in {"Z", "X", "x"}
        ):
            return None
        key = fields[0].rstrip(":") if fields else ""
        if key in wanted and len(fields) >= 2:
            result[key] = int(fields[1])
    return result


def process_cpu_ticks(pid: int) -> int | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    return parse_process_cpu_ticks(raw)


def parse_process_cpu_ticks(raw: str) -> int | None:
    # `comm` is parenthesized and may contain spaces or `)` characters. The
    # final `)` unambiguously terminates field 2; the remainder starts at the
    # field-3 process state. Fields 14 and 15 are this process's utime/stime.
    command_end = raw.rfind(")")
    if command_end < 0:
        return None
    fields = raw[command_end + 1 :].split()
    if len(fields) < 13 or fields[0] in {"Z", "X", "x"}:
        return None
    try:
        return int(fields[11]) + int(fields[12])
    except ValueError:
        return None


def checkpoint_phase(path: Path) -> str:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "workload"
    phases = [line.split("\t")[1] for line in text.splitlines()[1:] if "\t" in line]
    if "hold_complete" in phases:
        return "hold_complete"
    if "ingester_dropped" in phases:
        return "post_drop_hold"
    return "checkpoint_incomplete"


def summarize_rss_samples(
    samples: list[dict[str, Any]],
    root_pid: int,
    interval_ms: int,
    clock_ticks_per_second: int,
) -> dict[str, Any]:
    live_samples = [sample for sample in samples if sample["phase"] != "terminal"]
    if not live_samples:
        raise GateError("RSS summary cannot be derived from zero samples")
    post_drop = [
        sample for sample in live_samples if sample["phase"] == "post_drop_hold"
    ]
    workload = [sample for sample in live_samples if sample["phase"] == "workload"]
    hold_complete = [
        sample for sample in live_samples if sample["phase"] == "hold_complete"
    ]
    checkpoint_incomplete = [
        sample
        for sample in live_samples
        if sample["phase"] == "checkpoint_incomplete"
    ]
    return {
        "root_pid": root_pid,
        "interval_ms": interval_ms,
        "clock_ticks_per_second": clock_ticks_per_second,
        "samples": len(live_samples),
        "workload_samples": len(workload),
        "post_drop_samples": len(post_drop),
        "hold_complete_samples": len(hold_complete),
        "checkpoint_incomplete_samples": len(checkpoint_incomplete),
        "peak_rss_kib": max(sample["rss_kib"] for sample in live_samples),
        "peak_rss_anon_kib": max(
            sample["rss_anon_kib"] for sample in live_samples
        ),
        "peak_rss_file_kib": max(
            sample["rss_file_kib"] for sample in live_samples
        ),
        "peak_vm_swap_kib": max(
            sample["vm_swap_kib"] for sample in live_samples
        ),
        "peak_process_count": max(
            sample["process_count"] for sample in live_samples
        ),
        "workload_peak_rss_kib": max(
            (sample["rss_kib"] for sample in workload), default=0
        ),
        "workload_peak_max_single_hwm_kib": max(
            (sample["max_single_hwm_kib"] for sample in workload), default=0
        ),
        "workload_boundary_max_single_hwm_kib": (
            post_drop[0]["max_single_hwm_kib"] if post_drop else None
        ),
        "post_drop_first_rss_kib": post_drop[0]["rss_kib"] if post_drop else None,
        "post_drop_min_rss_kib": min(
            (sample["rss_kib"] for sample in post_drop), default=None
        ),
        "post_drop_end_rss_kib": post_drop[-1]["rss_kib"] if post_drop else None,
        "post_drop_first_unix_time_ns": (
            post_drop[0]["unix_time_ns"] if post_drop else None
        ),
        "post_drop_end_unix_time_ns": (
            post_drop[-1]["unix_time_ns"] if post_drop else None
        ),
        "workload_boundary_cpu_ticks": (
            post_drop[0]["process_cpu_ticks"] if post_drop else None
        ),
        "workload_boundary_cpu_seconds": (
            post_drop[0]["process_cpu_ticks"] / clock_ticks_per_second
            if post_drop
            else None
        ),
        "workload_boundary_sample_window_start_unix_time_ns": (
            post_drop[0]["sample_window_start_unix_time_ns"] if post_drop else None
        ),
        "workload_boundary_sample_unix_time_ns": (
            post_drop[0]["unix_time_ns"] if post_drop else None
        ),
    }


def validate_rss_monitor_control(
    control_path: Path,
    root_pid: int,
    root_starttime_ticks: int,
    rss_monitor_pid: int,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    *,
    require_live: bool,
) -> dict[str, Any]:
    path = regular_non_symlink(control_path, "RSS monitor guardian control")
    if stat.S_IMODE(path.stat().st_mode) != 0o444:
        raise GateError("RSS monitor guardian control must have exact mode 0444")
    value = require_exact_keys(
        load_json(path),
        {
            "schema",
            "root_pid",
            "root_starttime_ticks",
            "guardian_pid",
            "guardian_starttime_ticks",
            "rss_monitor_pid",
            "rss_monitor_starttime_ticks",
            "rss_ready_marker",
            "interval_ms",
            "ready_marker",
            "launch_marker",
        },
        "$.rss_monitor_control",
    )
    allowed_schemas = {
        GUARDIAN_CONTROL_SCHEMA,
        "chronoxide/storage-vnext-phase5-full-guardian-control/v2",
    }
    if (
        value["schema"] not in allowed_schemas
        or value["root_pid"] != root_pid
        or value["root_starttime_ticks"] != root_starttime_ticks
        or value["rss_monitor_pid"] != rss_monitor_pid
        or value["rss_ready_marker"] != str(rss_ready_path)
        or value["launch_marker"] != str(launch_path)
        or value["interval_ms"] != interval_ms
        or not rss_ready_path.is_absolute()
        or not launch_path.is_absolute()
        or rss_ready_path.parent != path.parent
        or launch_path.parent != path.parent
    ):
        raise GateError("RSS monitor control differs from its exact bound role")
    for key in (
        "root_pid",
        "root_starttime_ticks",
        "guardian_pid",
        "guardian_starttime_ticks",
        "rss_monitor_pid",
        "rss_monitor_starttime_ticks",
    ):
        strict_int(value[key], f"$.rss_monitor_control.{key}", minimum=1)
    if len({value["root_pid"], value["guardian_pid"], value["rss_monitor_pid"]}) != 3:
        raise GateError("RSS monitor control PIDs are not distinct")
    if require_live:
        for role in ("root", "guardian", "rss_monitor"):
            if not process_is_same_running(
                value[f"{role}_pid"], value[f"{role}_starttime_ticks"]
            ):
                raise GateError(f"RSS monitor control {role} identity is not live")
    return value


def monitor_rss_release(
    pid: int,
    checkpoint: Path,
    output_path: Path,
    summary_path: Path,
    interval_ms: int,
    control_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
) -> dict[str, Any]:
    if interval_ms != 100:
        raise GateError("RSS sampling interval must be exactly 100 milliseconds")
    if output_path.exists() or summary_path.exists():
        raise GateError("refusing to reuse RSS monitor output")
    if rss_ready_path.exists() or rss_ready_path.is_symlink():
        raise GateError("refusing to reuse RSS ready marker")
    initial_root = require_running_process_identity(pid, "held RSS root")
    root_starttime_ticks = int(initial_root["starttime_ticks"])
    control_deadline = time.monotonic() + 5.0
    while not control_path.exists() and not control_path.is_symlink():
        if not process_is_same_running(pid, root_starttime_ticks):
            raise GateError("held RSS root exited before guardian control")
        if time.monotonic() >= control_deadline:
            raise GateError("RSS monitor guardian control was not created in time")
        time.sleep(0.005)
    control = validate_rss_monitor_control(
        control_path,
        pid,
        root_starttime_ticks,
        os.getpid(),
        rss_ready_path,
        launch_path,
        interval_ms,
        require_live=True,
    )
    clock_ticks_per_second = os.sysconf("SC_CLK_TCK")
    if type(clock_ticks_per_second) is not int or clock_ticks_per_second <= 0:
        raise GateError("sysconf(SC_CLK_TCK) did not return a positive integer")
    started = time.monotonic_ns()
    interval_ns = interval_ms * 1_000_000
    next_sample_ns = started
    samples: list[dict[str, Any]] = []
    ready_sample: int | None = None
    ready_elapsed: int | None = None
    launch_sample: int | None = None
    launch_elapsed: int | None = None
    terminal_observed = False
    terminal_launch_observed = False
    handshake_violations: list[str] = []
    with output_path.open("x", encoding="utf-8") as destination:
        destination.write(
            "elapsed_ns\tsample_window_start_unix_time_ns\tunix_time_ns\tphase\t"
            "process_count\tprocess_cpu_ticks\trss_kib\trss_anon_kib\t"
            "rss_file_kib\tvm_swap_kib\tmax_single_hwm_kib\tpids\n"
        )
        while True:
            sample_started_elapsed_ns = time.monotonic_ns() - started
            window_start_unix_time_ns = time.time_ns()
            metrics = []
            for item in sorted(process_tree(pid, root_starttime_ticks)):
                status = status_kib(item)
                ticks = process_cpu_ticks(item)
                if status is not None and ticks is not None:
                    metrics.append((item, status, ticks))
            terminal_poll = not metrics
            terminal_observed = terminal_observed or terminal_poll
            phase = "terminal" if terminal_poll else checkpoint_phase(checkpoint)
            sample: dict[str, Any] = {
                "elapsed_ns": sample_started_elapsed_ns,
                "sample_window_start_unix_time_ns": window_start_unix_time_ns,
                "unix_time_ns": time.time_ns(),
                "phase": phase,
                "process_count": len(metrics),
                "process_cpu_ticks": sum(ticks for _item, _status, ticks in metrics),
                "rss_kib": sum(value["VmRSS"] for _item, value, _ticks in metrics),
                "rss_anon_kib": sum(
                    value["RssAnon"] for _item, value, _ticks in metrics
                ),
                "rss_file_kib": sum(
                    value["RssFile"] for _item, value, _ticks in metrics
                ),
                "vm_swap_kib": sum(
                    value["VmSwap"] for _item, value, _ticks in metrics
                ),
                "max_single_hwm_kib": max(
                    (value["VmHWM"] for _item, value, _ticks in metrics), default=0
                ),
                "pids": (
                    ",".join(str(item) for item, _value, _ticks in metrics)
                    if metrics
                    else "-"
                ),
            }
            if ready_sample is None:
                if launch_path.exists() or launch_path.is_symlink():
                    handshake_violations.append("launch marker existed before RSS readiness")
                elif not terminal_poll and pid in {
                    item for item, _status, _ticks in metrics
                }:
                    create_empty_read_only_marker(rss_ready_path, "RSS ready marker")
                    ready_sample = len(samples) + 1
                    ready_elapsed = sample_started_elapsed_ns
                else:
                    handshake_violations.append(
                        "first RSS cadence observation did not bind the live root"
                    )
            else:
                try:
                    validate_empty_read_only_marker(rss_ready_path, "RSS ready marker")
                except GateError as error:
                    handshake_violations.append(str(error))
                if launch_path.exists() or launch_path.is_symlink():
                    try:
                        validate_empty_read_only_marker(
                            launch_path, "guardian launch marker"
                        )
                    except GateError as error:
                        handshake_violations.append(str(error))
                    else:
                        if terminal_poll:
                            terminal_launch_observed = True
                        elif launch_sample is None:
                            launch_sample = len(samples) + 1
                            launch_elapsed = sample_started_elapsed_ns
            samples.append(sample)
            destination.write(
                "{elapsed_ns}\t{sample_window_start_unix_time_ns}\t{unix_time_ns}\t"
                "{phase}\t{process_count}\t{process_cpu_ticks}\t{rss_kib}\t"
                "{rss_anon_kib}\t{rss_file_kib}\t{vm_swap_kib}\t"
                "{max_single_hwm_kib}\t{pids}\n".format(**sample)
            )
            destination.flush()
            if handshake_violations:
                break
            if terminal_poll:
                break
            next_sample_ns += interval_ns
            remaining_ns = next_sample_ns - time.monotonic_ns()
            if remaining_ns > 0:
                time.sleep(remaining_ns / 1_000_000_000)
            else:
                next_sample_ns = time.monotonic_ns()
    elapsed_ns = time.monotonic_ns() - started
    timestamps = [sample["elapsed_ns"] for sample in samples]
    maximum_gap_ns = derive_guardian_maximum_poll_start_gap_ns(timestamps, elapsed_ns)
    allowed_gap_ns = guardian_maximum_allowed_gap_ns(interval_ms)
    summary = summarize_rss_samples(samples, pid, interval_ms, clock_ticks_per_second)
    rss_ready_sha: str | None = None
    launch_sha: str | None = None
    try:
        rss_ready_sha = sha256_file(
            validate_empty_read_only_marker(rss_ready_path, "RSS ready marker")
        )
    except GateError as error:
        handshake_violations.append(str(error))
    try:
        launch_sha = sha256_file(
            validate_empty_read_only_marker(launch_path, "guardian launch marker")
        )
    except GateError as error:
        handshake_violations.append(str(error))
    if launch_sample is None:
        if terminal_launch_observed:
            handshake_violations.append(
                "RSS monitor observed the launch marker only after the root stopped"
            )
        else:
            handshake_violations.append("RSS monitor never observed the launch marker")
    summary.update(
        {
            "root_starttime_ticks": root_starttime_ticks,
            "elapsed_ns": elapsed_ns,
            "poll_monotonic_elapsed_ns": timestamps,
            "maximum_poll_start_gap_ns": maximum_gap_ns,
            "maximum_allowed_poll_start_gap_ns": allowed_gap_ns,
            "control_path": str(control_path),
            "control_sha256": sha256_file(control_path),
            "rss_ready_marker_path": str(rss_ready_path),
            "rss_ready_marker_sha256": rss_ready_sha,
            "rss_ready_created_sample": ready_sample,
            "rss_ready_created_monotonic_elapsed_ns": ready_elapsed,
            "launch_marker_path": str(launch_path),
            "launch_marker_sha256": launch_sha,
            "launch_observed_sample": launch_sample,
            "launch_observed_monotonic_elapsed_ns": launch_elapsed,
            "launch_observed": launch_sample is not None,
            "terminal_observation": terminal_observed,
            "terminal_launch_observed": terminal_launch_observed,
            "handshake_violations": handshake_violations,
            "complete": (
                summary["samples"] >= 2
                and terminal_observed
                and maximum_gap_ns <= allowed_gap_ns
                and ready_sample == 1
                and launch_sample is not None
                and launch_sample > ready_sample
                and not handshake_violations
            ),
        }
    )
    write_json_exclusive(summary_path, summary)
    if summary["samples"] < 2:
        raise GateError("RSS monitor observed fewer than two live process samples")
    if maximum_gap_ns > allowed_gap_ns:
        raise GateError("RSS monitor cadence maximum gap exceeds its exact allowance")
    if handshake_violations:
        raise GateError(f"RSS held-launch handshake failed: {handshake_violations!r}")
    return summary


def validate_rss_release_evidence(
    samples_path: Path,
    summary_path: Path,
    control_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> dict[str, Any]:
    samples = load_rss_samples(samples_path)
    saved = load_json(summary_path)
    if not isinstance(saved, dict):
        raise GateError("RSS summary must be an object")
    root_pid = strict_int(saved.get("root_pid"), "$.rss.root_pid", minimum=1)
    root_starttime = strict_int(
        saved.get("root_starttime_ticks"),
        "$.rss.root_starttime_ticks",
        minimum=1,
    )
    clock_ticks = strict_int(
        saved.get("clock_ticks_per_second"),
        "$.rss.clock_ticks_per_second",
        minimum=1,
    )
    baseline = summarize_rss_samples(samples, root_pid, interval_ms, clock_ticks)
    extra_keys = {
        "root_starttime_ticks",
        "elapsed_ns",
        "poll_monotonic_elapsed_ns",
        "maximum_poll_start_gap_ns",
        "maximum_allowed_poll_start_gap_ns",
        "control_path",
        "control_sha256",
        "rss_ready_marker_path",
        "rss_ready_marker_sha256",
        "rss_ready_created_sample",
        "rss_ready_created_monotonic_elapsed_ns",
        "launch_marker_path",
        "launch_marker_sha256",
        "launch_observed_sample",
        "launch_observed_monotonic_elapsed_ns",
        "launch_observed",
        "terminal_observation",
        "terminal_launch_observed",
        "handshake_violations",
        "complete",
    }
    require_exact_keys(saved, set(baseline) | extra_keys, "$.rss")
    for key, value in baseline.items():
        if saved[key] != value:
            raise GateError(f"RSS summary field {key} is not derived from raw samples")
    if saved["interval_ms"] != interval_ms or interval_ms != 100:
        raise GateError("RSS summary interval is not exact 100 ms")
    timestamps = saved["poll_monotonic_elapsed_ns"]
    raw_timestamps = [sample["elapsed_ns"] for sample in samples]
    if timestamps != raw_timestamps or len(timestamps) < 3:
        raise GateError(
            "RSS raw timestamps differ or contain fewer than two live samples plus terminal"
        )
    previous: int | None = None
    for index, timestamp in enumerate(timestamps):
        strict_int(timestamp, f"$.rss.poll_monotonic_elapsed_ns[{index}]", minimum=0)
        if previous is not None and timestamp <= previous:
            raise GateError("RSS raw sample timestamps are not strictly increasing")
        previous = timestamp
    elapsed_ns = strict_int(saved["elapsed_ns"], "$.rss.elapsed_ns", minimum=1)
    if timestamps[-1] > elapsed_ns:
        raise GateError("RSS sample timestamp exceeds RSS monitor elapsed time")
    derived_gap = derive_guardian_maximum_poll_start_gap_ns(timestamps, elapsed_ns)
    allowed_gap = guardian_maximum_allowed_gap_ns(interval_ms)
    if (
        saved["maximum_poll_start_gap_ns"] != derived_gap
        or saved["maximum_allowed_poll_start_gap_ns"] != allowed_gap
    ):
        raise GateError("RSS cadence maximum gap is not exactly derived")
    if derived_gap > allowed_gap:
        raise GateError("RSS cadence maximum gap exceeds its exact allowance")
    raw_control = load_json(control_path)
    if not isinstance(raw_control, dict):
        raise GateError("RSS guardian control must be an object")
    control = validate_rss_monitor_control(
        control_path,
        root_pid,
        root_starttime,
        strict_int(
            raw_control.get("rss_monitor_pid"),
            "$.rss_control.rss_monitor_pid",
            minimum=1,
        ),
        rss_ready_path,
        launch_path,
        interval_ms,
        require_live=False,
    )
    rss_ready = validate_empty_read_only_marker(rss_ready_path, "RSS ready marker")
    launch = validate_empty_read_only_marker(launch_path, "guardian launch marker")
    ready_sample = strict_int(
        saved["rss_ready_created_sample"],
        "$.rss.rss_ready_created_sample",
        minimum=1,
    )
    launch_sample = strict_int(
        saved["launch_observed_sample"],
        "$.rss.launch_observed_sample",
        minimum=1,
    )
    if (
        control["root_starttime_ticks"] != root_starttime
        or ready_sample != 1
        or launch_sample <= ready_sample
        or launch_sample > baseline["samples"]
        or saved["rss_ready_created_monotonic_elapsed_ns"]
        != timestamps[ready_sample - 1]
        or saved["launch_observed_monotonic_elapsed_ns"]
        != timestamps[launch_sample - 1]
        or saved["control_path"] != str(control_path)
        or saved["control_sha256"] != sha256_file(control_path)
        or saved["rss_ready_marker_path"] != str(rss_ready_path)
        or saved["rss_ready_marker_sha256"] != sha256_file(rss_ready)
        or saved["launch_marker_path"] != str(launch_path)
        or saved["launch_marker_sha256"] != sha256_file(launch)
        or saved["launch_observed"] is not True
        or saved["terminal_observation"] is not True
        or saved["terminal_launch_observed"] is not True
        or saved["handshake_violations"] != []
        or saved["complete"] is not True
        or str(root_pid) not in samples[0]["pids"].split(",")
    ):
        raise GateError("RSS held-launch evidence is not exact and causal")
    return saved


def parse_checkpoint(
    checkpoint_path: Path,
    rss_path: Path,
    plan_path: Path,
    phase1_expectations: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    lines = checkpoint_path.read_text(encoding="utf-8").splitlines()
    if lines[:1] != ["schema\tphase\tmain_elapsed_ns\tunix_time_ns\thold_secs"]:
        raise GateError("release checkpoint header is missing or changed")
    if len(lines) != 3:
        raise GateError("release checkpoint must contain exactly two phase rows")
    rows = []
    for index, line in enumerate(lines[1:], start=1):
        fields = line.split("\t")
        if len(fields) != 5:
            raise GateError(f"checkpoint row {index} must contain five fields")
        schema, phase, elapsed, unix_time, hold_secs = fields
        if schema != CHECKPOINT_SCHEMA:
            raise GateError(f"checkpoint row {index} schema mismatch")
        if not all(re.fullmatch(r"[0-9]+", value) for value in (elapsed, unix_time, hold_secs)):
            raise GateError(f"checkpoint row {index} has a non-integer field")
        rows.append(
            {
                "phase": phase,
                "main_elapsed_ns": int(elapsed),
                "unix_time_ns": int(unix_time),
                "hold_secs": int(hold_secs),
            }
        )
    if [row["phase"] for row in rows] != ["ingester_dropped", "hold_complete"]:
        raise GateError("release checkpoint phases are missing, duplicated, or reordered")
    expected_hold = plan["workload"]["post_ingester_drop_hold_secs"]
    if any(row["hold_secs"] != expected_hold for row in rows):
        raise GateError("release checkpoint hold duration differs from the frozen plan")
    elapsed_ns = rows[1]["main_elapsed_ns"] - rows[0]["main_elapsed_ns"]
    unix_elapsed_ns = rows[1]["unix_time_ns"] - rows[0]["unix_time_ns"]
    minimum_ns = expected_hold * 1_000_000_000
    maximum_ns = (
        plan["screen_candidate_gate"]["maximum_hold_elapsed_secs"] * 1_000_000_000
    )
    if not minimum_ns <= elapsed_ns <= maximum_ns:
        raise GateError(f"monotonic hold duration is outside bounds: {elapsed_ns} ns")
    if not minimum_ns <= unix_elapsed_ns <= maximum_ns:
        raise GateError(f"wall-clock hold duration is outside bounds: {unix_elapsed_ns} ns")
    rss = validate_rss_release_evidence(
        rss_path.with_name("rss-samples.tsv"),
        rss_path,
        rss_path.with_name("external-conflict-guardian-control.json"),
        rss_path.with_name("rss-monitor-ready"),
        rss_path.with_name("external-conflict-guardian-launch"),
        plan["workload"]["rss_interval_ms"],
    )
    if rss["interval_ms"] != plan["workload"]["rss_interval_ms"]:
        raise GateError("RSS interval differs from the frozen plan")
    strict_int(rss["root_pid"], "$.rss.root_pid", minimum=1)
    strict_int(rss["samples"], "$.rss.samples", minimum=1)
    clock_ticks = strict_int(
        rss["clock_ticks_per_second"], "$.rss.clock_ticks_per_second", minimum=1
    )
    if strict_int(rss["workload_samples"], "$.rss.workload_samples", minimum=0) < 1:
        raise GateError("RSS monitor did not observe the measured workload phase")
    minimum_samples = plan["screen_candidate_gate"]["minimum_post_drop_rss_samples"]
    if strict_int(rss["post_drop_samples"], "$.rss.post_drop_samples", minimum=0) < minimum_samples:
        raise GateError("RSS monitor did not observe enough post-drop hold samples")
    for key in (
        "peak_rss_kib",
        "workload_peak_rss_kib",
        "workload_peak_max_single_hwm_kib",
        "workload_boundary_max_single_hwm_kib",
        "post_drop_first_rss_kib",
        "post_drop_min_rss_kib",
        "post_drop_end_rss_kib",
    ):
        strict_int(rss[key], f"$.rss.{key}", minimum=1)
    for key in ("hold_complete_samples", "checkpoint_incomplete_samples"):
        strict_int(rss[key], f"$.rss.{key}", minimum=0)
    phase_sample_total = (
        rss["workload_samples"]
        + rss["post_drop_samples"]
        + rss["hold_complete_samples"]
        + rss["checkpoint_incomplete_samples"]
    )
    if phase_sample_total != rss["samples"]:
        raise GateError("RSS phase counts do not account for every external sample")
    first_post_drop_ns = strict_int(
        rss["post_drop_first_unix_time_ns"],
        "$.rss.post_drop_first_unix_time_ns",
        minimum=1,
    )
    end_post_drop_ns = strict_int(
        rss["post_drop_end_unix_time_ns"],
        "$.rss.post_drop_end_unix_time_ns",
        minimum=1,
    )
    if not rows[0]["unix_time_ns"] <= first_post_drop_ns <= end_post_drop_ns:
        raise GateError("RSS post-drop phase begins before the Ingester-drop checkpoint")
    if end_post_drop_ns > rows[1]["unix_time_ns"]:
        raise GateError("RSS post-drop phase extends beyond the hold-complete checkpoint")
    workload_cpu_ticks = strict_int(
        rss["workload_boundary_cpu_ticks"],
        "$.rss.workload_boundary_cpu_ticks",
        minimum=1,
    )
    workload_cpu_seconds = strict_number(
        rss["workload_boundary_cpu_seconds"],
        "$.rss.workload_boundary_cpu_seconds",
    )
    expected_cpu_seconds = workload_cpu_ticks / clock_ticks
    if abs(workload_cpu_seconds - expected_cpu_seconds) > 1e-12:
        raise GateError("workload CPU seconds do not exactly derive from ticks/CLK_TCK")
    boundary_window_start_ns = strict_int(
        rss["workload_boundary_sample_window_start_unix_time_ns"],
        "$.rss.workload_boundary_sample_window_start_unix_time_ns",
        minimum=1,
    )
    boundary_sample_ns = strict_int(
        rss["workload_boundary_sample_unix_time_ns"],
        "$.rss.workload_boundary_sample_unix_time_ns",
        minimum=1,
    )
    if boundary_sample_ns != first_post_drop_ns:
        raise GateError("workload CPU boundary is not the first post-drop RSS sample")
    if not boundary_window_start_ns <= boundary_sample_ns:
        raise GateError("workload CPU boundary sample window is reversed")
    if boundary_sample_ns < rows[0]["unix_time_ns"]:
        raise GateError("workload CPU boundary sample precedes the Ingester drop")
    boundary_uncertainty_ns = max(
        abs(boundary_window_start_ns - rows[0]["unix_time_ns"]),
        boundary_sample_ns - rows[0]["unix_time_ns"],
    )
    maximum_boundary_uncertainty_ns = (
        plan["workload"]["rss_interval_ms"]
        * 1_000_000
        * plan["screen_candidate_gate"][
            "maximum_workload_cpu_boundary_uncertainty_intervals"
        ]
    )
    if boundary_uncertainty_ns > maximum_boundary_uncertainty_ns:
        raise GateError(
            "workload CPU boundary uncertainty exceeds one RSS sampling interval: "
            f"{boundary_uncertainty_ns} > {maximum_boundary_uncertainty_ns} ns"
        )
    return {
        "workload_wall_ns": rows[0]["main_elapsed_ns"],
        "workload_cpu_ticks": workload_cpu_ticks,
        "workload_cpu_seconds": workload_cpu_seconds,
        "clock_ticks_per_second": clock_ticks,
        "workload_cpu_boundary_uncertainty_ns": boundary_uncertainty_ns,
        "hold_elapsed_ns": elapsed_ns,
        "hold_wall_elapsed_ns": unix_elapsed_ns,
        "drop_unix_time_ns": rows[0]["unix_time_ns"],
        "hold_complete_unix_time_ns": rows[1]["unix_time_ns"],
        "drop_main_elapsed_ns": rows[0]["main_elapsed_ns"],
        "hold_complete_main_elapsed_ns": rows[1]["main_elapsed_ns"],
        "rss": rss,
    }


def load_rss_samples(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source, delimiter="\t")
        if reader.fieldnames != RSS_SAMPLE_COLUMNS:
            raise GateError("RSS sample columns are missing, reordered, or changed")
        rows = []
        for line_number, raw in enumerate(reader, start=2):
            if None in raw or any(value is None for value in raw.values()):
                raise GateError(f"RSS sample row {line_number} is malformed")
            row: dict[str, Any] = {"phase": raw["phase"], "pids": raw["pids"]}
            for key in RSS_SAMPLE_COLUMNS:
                if key in ("phase", "pids"):
                    continue
                value = raw[key]
                if re.fullmatch(r"[0-9]+", value) is None:
                    raise GateError(f"RSS sample row {line_number} {key} is not an integer")
                row[key] = int(value)
            if row["phase"] not in {
                "workload",
                "post_drop_hold",
                "hold_complete",
                "checkpoint_incomplete",
                "terminal",
            }:
                raise GateError(f"RSS sample row {line_number} has an unknown phase")
            rows.append(row)
    if not rows:
        raise GateError("RSS sample file contains no observations")
    terminal_rows = [row for row in rows if row["phase"] == "terminal"]
    if len(terminal_rows) != 1 or rows[-1]["phase"] != "terminal":
        raise GateError("RSS sample file lacks one final terminal observation")
    terminal = terminal_rows[0]
    if (
        terminal["process_count"] != 0
        or terminal["process_cpu_ticks"] != 0
        or terminal["rss_kib"] != 0
        or terminal["rss_anon_kib"] != 0
        or terminal["rss_file_kib"] != 0
        or terminal["vm_swap_kib"] != 0
        or terminal["max_single_hwm_kib"] != 0
        or terminal["pids"] != "-"
    ):
        raise GateError("RSS terminal observation is malformed")
    return rows


def parse_allocator_telemetry(
    telemetry_path: Path,
    checkpoint_path: Path,
    rss_samples_path: Path,
    rss_summary_path: Path,
    plan_path: Path,
    phase1_expectations: Path,
    policy_name: str,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    if policy_name not in POLICY_ORDER:
        raise GateError(f"unknown allocator policy: {policy_name}")
    checkpoint = parse_checkpoint(
        checkpoint_path, rss_summary_path, plan_path, phase1_expectations
    )
    lines = [
        line for line in telemetry_path.read_text(encoding="utf-8").splitlines() if line
    ]
    if len(lines) != 2:
        raise GateError("allocator release telemetry must contain exactly two JSON rows")
    telemetry_keys = {
        "schema",
        "phase",
        "main_elapsed_ns",
        "unix_time_ns",
        "rust_global_allocator",
        "allocator_internal_telemetry",
        "epoch",
        "allocated_bytes",
        "active_bytes",
        "resident_bytes",
        "mapped_bytes",
        "retained_bytes",
    }
    records = []
    for index, line in enumerate(lines):
        try:
            record = require_exact_keys(
                json.loads(line), telemetry_keys, f"$telemetry[{index}]"
            )
        except json.JSONDecodeError as error:
            raise GateError(f"allocator telemetry row {index + 1} is not JSON") from error
        if record["schema"] != TELEMETRY_SCHEMA:
            raise GateError(f"allocator telemetry row {index + 1} schema mismatch")
        records.append(record)
    if [record["phase"] for record in records] != [
        "post_ingester_drop",
        "hold_complete",
    ]:
        raise GateError("allocator telemetry phases are missing, duplicated, or reordered")
    expected = plan["policies"][policy_name]
    for index, record in enumerate(records):
        if record["rust_global_allocator"] != expected["rust_global_allocator"]:
            raise GateError(f"allocator telemetry row {index + 1} identity mismatch")
        strict_int(
            record["main_elapsed_ns"],
            f"$.telemetry[{index}].main_elapsed_ns",
            minimum=1,
        )
        strict_int(
            record["unix_time_ns"],
            f"$.telemetry[{index}].unix_time_ns",
            minimum=1,
        )
        stat_keys = (
            "epoch",
            "allocated_bytes",
            "active_bytes",
            "resident_bytes",
            "mapped_bytes",
            "retained_bytes",
        )
        if policy_name == "S":
            if record["allocator_internal_telemetry"] != "unavailable":
                raise GateError("system allocator telemetry must be explicit unavailable")
            if any(record[key] is not None for key in stat_keys):
                raise GateError("system allocator telemetry fields must be explicit null")
        else:
            if record["allocator_internal_telemetry"] != "available":
                raise GateError("jemalloc release telemetry is unavailable")
            strict_int(
                record["epoch"], f"$.telemetry[{index}].epoch", minimum=1
            )
            for key in stat_keys[1:]:
                strict_int(record[key], f"$.telemetry[{index}].{key}", minimum=0)
            if record["active_bytes"] < record["allocated_bytes"]:
                raise GateError("jemalloc telemetry active bytes are below allocated bytes")

    if not (
        checkpoint["drop_main_elapsed_ns"]
        <= records[0]["main_elapsed_ns"]
        <= records[1]["main_elapsed_ns"]
        <= checkpoint["hold_complete_main_elapsed_ns"]
    ):
        raise GateError("allocator telemetry monotonic timestamps escape checkpoint bounds")
    if not (
        checkpoint["drop_unix_time_ns"]
        <= records[0]["unix_time_ns"]
        <= records[1]["unix_time_ns"]
        <= checkpoint["hold_complete_unix_time_ns"]
    ):
        raise GateError("allocator telemetry wall timestamps escape checkpoint bounds")
    if policy_name != "S" and records[1]["epoch"] <= records[0]["epoch"]:
        raise GateError("jemalloc telemetry epoch did not advance between snapshots")

    post_drop_samples = [
        row for row in load_rss_samples(rss_samples_path) if row["phase"] == "post_drop_hold"
    ]
    if not post_drop_samples:
        raise GateError("no post-drop RSS sample is available for allocator reconciliation")
    maximum_alignment_ns = plan["workload"]["rss_interval_ms"] * 1_000_000
    relation = ALLOCATOR_RSS_RELATION
    reconciliation = []
    for record in records:
        sample = min(
            post_drop_samples,
            key=lambda row: abs(row["unix_time_ns"] - record["unix_time_ns"]),
        )
        alignment_ns = abs(sample["unix_time_ns"] - record["unix_time_ns"])
        if alignment_ns > maximum_alignment_ns:
            raise GateError(
                f"allocator telemetry {record['phase']} has no RSS sample within one interval"
            )
        external_rss_bytes = sample["rss_kib"] * 1024
        resident = record["resident_bytes"]
        reconciliation.append(
            {
                "phase": record["phase"],
                "allocator_telemetry_unix_time_ns": record["unix_time_ns"],
                "external_rss_unix_time_ns": sample["unix_time_ns"],
                "alignment_abs_ns": alignment_ns,
                "external_process_tree_rss_bytes": external_rss_bytes,
                "jemalloc_resident_bytes": resident,
                "external_minus_jemalloc_resident_bytes": (
                    external_rss_bytes - resident if resident is not None else None
                ),
                "measurement_relation": relation,
            }
        )
    delta_keys = (
        "allocated_bytes",
        "active_bytes",
        "resident_bytes",
        "mapped_bytes",
        "retained_bytes",
    )
    deltas = {
        key: (
            records[1][key] - records[0][key]
            if records[0][key] is not None and records[1][key] is not None
            else None
        )
        for key in delta_keys
    }
    return {
        "schema": TELEMETRY_SUMMARY_SCHEMA,
        "policy": policy_name,
        "rust_global_allocator": expected["rust_global_allocator"],
        "records": records,
        "checkpoint_bounds": {
            "drop_main_elapsed_ns": checkpoint["drop_main_elapsed_ns"],
            "hold_complete_main_elapsed_ns": checkpoint[
                "hold_complete_main_elapsed_ns"
            ],
            "drop_unix_time_ns": checkpoint["drop_unix_time_ns"],
            "hold_complete_unix_time_ns": checkpoint["hold_complete_unix_time_ns"],
        },
        "hold_complete_minus_post_drop_bytes": deltas,
        "external_rss_reconciliation": reconciliation,
        "measurement_relation": relation,
    }


def perf_values(perf_document: dict[str, Any]) -> dict[str, float]:
    events = perf_document.get("events")
    if not isinstance(events, list):
        raise GateError("perf document has no events list")
    values = {}
    for row in events:
        if not isinstance(row, dict) or row.get("available") is not True:
            raise GateError("perf event is unavailable or malformed")
        event = row.get("event")
        raw = row.get("raw_value")
        if event in EXPECTED_PERF_EVENTS:
            if not isinstance(raw, str) or re.fullmatch(r"[0-9.]+", raw) is None:
                raise GateError(f"perf event {event!r} has invalid value {raw!r}")
            values[event] = float(raw)
    missing = [event for event in EXPECTED_PERF_EVENTS if event not in values]
    if missing:
        raise GateError(f"perf document is missing required events: {missing!r}")
    return values


def validate_replay_correctness(value: Any, expected_messages: int) -> dict[str, Any]:
    correctness = require_exact_keys(
        value,
        {
            "schema",
            "general",
            "datapoint_policy_totals",
            "datapoint_storage_totals",
            "otlp_data_type_counts",
            "event_time_skew_ranges",
            "partition_watermarks",
        },
        "$.correctness",
    )
    if correctness["schema"] != "chronoxide/storage-vnext-replay-correctness/v2":
        raise GateError("replay correctness schema mismatch")
    general = require_exact_keys(
        correctness["general"],
        {
            "Total Messages",
            "Total OTLP Metric Records",
            "Total Unique Metrics (`__name__`)",
            "Total Series (unique label sets)",
            "Observed OTLP Datapoints",
            "Accepted Datapoints",
            "Skipped Non-Scalar",
            "Recorded Samples",
            "Missing Number Value",
            "Invalid Typed Value",
        },
        "$.correctness.general",
    )
    for key, item in general.items():
        strict_int(item, f"$.correctness.general.{key}", minimum=0)
    if general["Total Messages"] != expected_messages:
        raise GateError(
            f"replay correctness Total Messages must be exactly {expected_messages}"
        )
    policy = require_exact_keys(
        correctness["datapoint_policy_totals"],
        {
            "Observed",
            "Time-Policy Accepted",
            "Dropped Too Old",
            "Dropped Too Future",
            "Missing Timestamp",
            "Rejected Total",
        },
        "$.correctness.datapoint_policy_totals",
    )
    storage = require_exact_keys(
        correctness["datapoint_storage_totals"],
        {
            "Time-Policy Accepted",
            "Recorded Samples",
            "Missing Number Value",
            "Invalid Typed Value",
            "Accepted Not Recorded",
        },
        "$.correctness.datapoint_storage_totals",
    )
    for section_name, section in (("policy", policy), ("storage", storage)):
        for key, item in section.items():
            strict_int(item, f"$.correctness.{section_name}.{key}", minimum=0)
    rejected_sum = (
        policy["Dropped Too Old"]
        + policy["Dropped Too Future"]
        + policy["Missing Timestamp"]
    )
    if policy["Rejected Total"] != rejected_sum:
        raise GateError("replay rejected-total counter algebra failed")
    if policy["Observed"] != policy["Time-Policy Accepted"] + rejected_sum:
        raise GateError("replay observed/accepted/rejected counter algebra failed")
    if general["Observed OTLP Datapoints"] != policy["Observed"]:
        raise GateError("general and policy observed datapoints differ")
    if general["Accepted Datapoints"] != policy["Time-Policy Accepted"]:
        raise GateError("general and policy accepted datapoints differ")
    if storage["Time-Policy Accepted"] != general["Accepted Datapoints"]:
        raise GateError("storage and general accepted datapoints differ")
    if storage["Recorded Samples"] != general["Recorded Samples"]:
        raise GateError("storage and general recorded samples differ")
    if storage["Missing Number Value"] != general["Missing Number Value"]:
        raise GateError("storage and general missing-number counts differ")
    if storage["Invalid Typed Value"] != general["Invalid Typed Value"]:
        raise GateError("storage and general invalid-typed counts differ")
    if storage["Accepted Not Recorded"] != (
        storage["Time-Policy Accepted"] - storage["Recorded Samples"]
    ):
        raise GateError("accepted-not-recorded counter algebra failed")
    if storage["Accepted Not Recorded"] != (
        storage["Missing Number Value"] + storage["Invalid Typed Value"]
    ):
        raise GateError(
            "accepted-not-recorded does not equal missing-number plus invalid-typed"
        )

    types = require_exact_keys(
        correctness["otlp_data_type_counts"],
        {"Gauge", "Sum", "Histogram", "Exponential Histogram", "Summary"},
        "$.correctness.otlp_data_type_counts",
    )
    type_rows = []
    for type_name, row_value in types.items():
        row = require_exact_keys(
            row_value,
            {"metric_records", "observed_datapoints", "accepted_datapoints"},
            f"$.correctness.otlp_data_type_counts.{type_name}",
        )
        for key, item in row.items():
            strict_int(item, f"$.correctness.otlp_data_type_counts.{type_name}.{key}", minimum=0)
        type_rows.append(row)
    if sum(row["metric_records"] for row in type_rows) != general["Total OTLP Metric Records"]:
        raise GateError("per-type metric-record counts do not sum to the general total")
    if sum(row["observed_datapoints"] for row in type_rows) != general["Observed OTLP Datapoints"]:
        raise GateError("per-type observed datapoints do not sum to the general total")
    if sum(row["accepted_datapoints"] for row in type_rows) != general["Accepted Datapoints"]:
        raise GateError("per-type accepted datapoints do not sum to the general total")

    expected_skew_counts = {
        "All Timestamped": policy["Observed"] - policy["Missing Timestamp"],
        "Accepted": policy["Time-Policy Accepted"],
        "Dropped Too Old": policy["Dropped Too Old"],
        "Dropped Too Future": policy["Dropped Too Future"],
    }
    expected_skew_rows = {
        name for name, count in expected_skew_counts.items() if count > 0
    }
    skew = require_exact_keys(
        correctness["event_time_skew_ranges"],
        expected_skew_rows,
        "$.correctness.event_time_skew_ranges",
    )
    for name, row_value in skew.items():
        row = require_exact_keys(row_value, {"count", "min_ms", "max_ms"}, f"$.correctness.skew.{name}")
        strict_int(row["count"], f"$.correctness.skew.{name}.count", minimum=1)
        if type(row["min_ms"]) is not int or type(row["max_ms"]) is not int:
            raise GateError("event-time skew bounds must be integers")
        if row["min_ms"] > row["max_ms"]:
            raise GateError("event-time skew range is reversed")
        if row["count"] != expected_skew_counts[name]:
            raise GateError(
                f"event-time skew count differs from policy counters: {name}"
            )

    watermarks = require_exact_keys(
        correctness["partition_watermarks"],
        {
            "Tracked Messages",
            "Tracked Datapoints",
            "Missing Timestamp Messages",
            "Missing Timestamp Datapoints",
            "Overall Min TS",
            "Overall Max TS",
            "Overall Window",
        },
        "$.correctness.partition_watermarks",
    )
    for key in (
        "Tracked Messages",
        "Tracked Datapoints",
        "Missing Timestamp Messages",
        "Missing Timestamp Datapoints",
    ):
        strict_int(watermarks[key], f"$.correctness.partition_watermarks.{key}", minimum=0)
    if watermarks["Tracked Messages"] != general["Total Messages"]:
        raise GateError("tracked-message count differs from total messages")
    if watermarks["Tracked Datapoints"] != general["Observed OTLP Datapoints"]:
        raise GateError("tracked-datapoint count differs from observed datapoints")
    if watermarks["Missing Timestamp Datapoints"] != policy["Missing Timestamp"]:
        raise GateError("missing-timestamp watermark and policy counts differ")
    for key in ("Overall Min TS", "Overall Max TS", "Overall Window"):
        if not isinstance(watermarks[key], str) or not watermarks[key]:
            raise GateError(f"partition watermark {key} is empty")
    return correctness


def make_observation(
    *,
    run_index: int,
    policy_name: str,
    plan_path: Path,
    phase1_expectations: Path,
    build_provenance_path: Path,
    preflight_path: Path,
    binary_path: Path,
    runtime_policy_path: Path,
    allocator_telemetry_path: Path,
    checkpoint_path: Path,
    rss_path: Path,
    time_path: Path,
    perf_path: Path,
    guardian_path: Path,
    pre_quiescence_path: Path,
    quiescence_path: Path,
    correctness_path: Path,
    corpus_path: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    if not 1 <= run_index <= len(plan["schedule"]):
        raise GateError("run_index is outside the frozen schedule")
    row = plan["schedule"][run_index - 1]
    if row["policy"] != policy_name:
        raise GateError("observation policy does not match the frozen run index")
    expected = plan["policies"][policy_name]
    build_provenance = validate_build_provenance(load_json(build_provenance_path))
    preflight = require_exact_keys(
        load_json(preflight_path), PREFLIGHT_RECORD_KEYS, "$preflight_record"
    )
    if preflight["schema"] != PREFLIGHT_RECORD_SCHEMA:
        raise GateError("run preflight record schema mismatch")
    if preflight["policy"] != policy_name:
        raise GateError("run preflight policy mismatch")
    if preflight["binary_role"] != expected["binary_role"]:
        raise GateError("run preflight binary role mismatch")
    if not isinstance(preflight["binary_sha256"], str) or re.fullmatch(
        r"[0-9a-f]{64}", preflight["binary_sha256"]
    ) is None:
        raise GateError("run preflight binary hash is invalid")
    current_binary_sha256 = executable_sha256(binary_path)
    if preflight["binary_sha256"] != current_binary_sha256:
        raise GateError("comparator binary changed after allocator preflight")
    if build_provenance["binary_sha256"][expected["binary_role"]] != current_binary_sha256:
        raise GateError("comparator binary is not the hash-bound controlled build")
    effective = validate_application_preflight(
        preflight["application"],
        expected,
        context="$preflight_record.application",
        stderr=None,
    )
    if (
        type(preflight["jemalloc_confirm_conf_verified"]) is not bool
        or preflight["jemalloc_confirm_conf_verified"]
        != (expected["jemalloc_conf"] is not None)
    ):
        raise GateError("preflight confirm_conf evidence does not match allocator role")
    expected_sources_verified = policy_name != "S"
    if preflight["jemalloc_config_sources_verified"] is not expected_sources_verified:
        raise GateError("preflight all-source audit does not match allocator role")
    source_audit_sha256 = preflight["jemalloc_config_source_audit_sha256"]
    if expected_sources_verified:
        if not isinstance(source_audit_sha256, str) or re.fullmatch(
            r"[0-9a-f]{64}", source_audit_sha256
        ) is None:
            raise GateError("jemalloc all-source audit hash is invalid")
    elif source_audit_sha256 is not None:
        raise GateError("system preflight invented a jemalloc source-audit hash")

    runtime_policy = require_exact_keys(
        load_json(runtime_policy_path),
        {
            "policy",
            "rust_global_allocator",
            "jemalloc_conf",
            "structured_runtime_policy",
            "jemalloc_confirm_conf",
            "effective_policy",
            "full_effective_policy_matches_preflight",
            "post_drop_hold_markers",
        },
        "$runtime_policy",
    )
    if (
        runtime_policy["policy"] != policy_name
        or runtime_policy["rust_global_allocator"] != expected["rust_global_allocator"]
        or runtime_policy["jemalloc_conf"] != expected["jemalloc_conf"]
        or runtime_policy["jemalloc_confirm_conf"] is not (expected["jemalloc_conf"] is not None)
        or runtime_policy["effective_policy"] != effective
        or runtime_policy["full_effective_policy_matches_preflight"] is not True
        or runtime_policy["post_drop_hold_markers"] != 2
    ):
        raise GateError("runtime allocator evidence differs from the frozen policy")
    structured_runtime = runtime_policy["structured_runtime_policy"]
    if structured_runtime.get("effective_policy") != effective:
        raise GateError("structured runtime effective policy differs from preflight")
    checkpoint = parse_checkpoint(
        checkpoint_path, rss_path, plan_path, phase1_expectations
    )
    allocator_telemetry = require_exact_keys(
        load_json(allocator_telemetry_path),
        {
            "schema",
            "policy",
            "rust_global_allocator",
            "records",
            "checkpoint_bounds",
            "hold_complete_minus_post_drop_bytes",
            "external_rss_reconciliation",
            "measurement_relation",
        },
        "$allocator_telemetry",
    )
    if allocator_telemetry["schema"] != TELEMETRY_SUMMARY_SCHEMA:
        raise GateError("allocator telemetry summary schema mismatch")
    if allocator_telemetry["policy"] != policy_name:
        raise GateError("allocator telemetry summary policy mismatch")
    if (
        allocator_telemetry["rust_global_allocator"]
        != expected["rust_global_allocator"]
    ):
        raise GateError("allocator telemetry summary identity mismatch")
    if not isinstance(allocator_telemetry["records"], list) or len(
        allocator_telemetry["records"]
    ) != 2:
        raise GateError("allocator telemetry summary does not contain two snapshots")
    validate_observation_telemetry(
        allocator_telemetry, policy_name, expected, plan, run_index - 1
    )
    telemetry_bounds = allocator_telemetry["checkpoint_bounds"]
    if telemetry_bounds["drop_main_elapsed_ns"] != checkpoint["workload_wall_ns"]:
        raise GateError("allocator telemetry does not begin outside the workload boundary")
    if (
        telemetry_bounds["hold_complete_main_elapsed_ns"]
        - telemetry_bounds["drop_main_elapsed_ns"]
        != checkpoint["hold_elapsed_ns"]
    ):
        raise GateError("allocator telemetry checkpoint duration mismatch")
    timing = load_json(time_path)
    if timing.get("exit_status") != 0:
        raise GateError("GNU time reports a nonzero replay exit status")
    perf = perf_values(load_json(perf_path))
    guardian_corpus = validate_corpus_summary(
        load_json(corpus_path), "$.guardian_corpus"
    )
    expected_guardian_free_bytes = (
        guardian_corpus["size_bytes"] * (10 - run_index) + CAPACITY_RESERVE_BYTES
    )
    guardian = validate_guardian_evidence(
        guardian_path,
        guardian_path.with_name("external-conflict-guardian-control.json"),
        guardian_path.with_name("external-conflict-guardian-ready"),
        guardian_path.with_name("external-conflict-guardian-launch"),
        plan["environment_contract"]["external_conflict_poll_interval_ms"],
        guardian_path.parents[2],
        expected_guardian_free_bytes,
        True,
    )
    for filename, description in (
        ("replay.exit-status", "replay exit status"),
        ("rss-monitor.exit-status", "RSS monitor exit status"),
        ("external-conflict-guardian.exit-status", "guardian exit status"),
    ):
        validate_zero_exit_status(guardian_path.with_name(filename), description)
    quiescence = require_exact_keys(
        load_json(quiescence_path),
        {
            "schema",
            "corpus",
            "fsynced_file_count",
            "global_sync_called",
            "maximum_dirty_writeback_kib",
            "required_consecutive_samples",
            "interval_ms",
            "timeout_secs",
            "sample_count",
            "final_dirty_kib",
            "final_writeback_kib",
            "final_total_kib",
            "passed",
        },
        "$.writeback_quiescence",
    )
    quiescence_contract = plan["quiescence_contract"]
    if (
        quiescence["schema"]
        != "chronoxide/storage-vnext-phase5-writeback-quiescence/v1"
        or quiescence["maximum_dirty_writeback_kib"]
        != quiescence_contract["maximum_dirty_writeback_kib"]
        or quiescence["required_consecutive_samples"]
        != quiescence_contract["required_consecutive_samples"]
        or quiescence["interval_ms"] != quiescence_contract["poll_interval_ms"]
        or quiescence["timeout_secs"] != quiescence_contract["timeout_secs"]
        or quiescence["global_sync_called"] is not True
        or quiescence["passed"] is not True
    ):
        raise GateError("per-run corpus sync/writeback quiescence did not pass")
    if quiescence["final_total_kib"] != (
        quiescence["final_dirty_kib"] + quiescence["final_writeback_kib"]
    ) or quiescence["final_total_kib"] > quiescence["maximum_dirty_writeback_kib"]:
        raise GateError("writeback-quiescence final counters are inconsistent")
    pre_quiescence = require_exact_keys(
        load_json(pre_quiescence_path), set(quiescence), "$.pre_run_writeback_quiescence"
    )
    for key in (
        "schema",
        "maximum_dirty_writeback_kib",
        "required_consecutive_samples",
        "interval_ms",
        "timeout_secs",
        "global_sync_called",
        "passed",
    ):
        if pre_quiescence[key] != quiescence[key]:
            raise GateError(f"pre-run writeback quiescence differs for {key}")
    if pre_quiescence["final_total_kib"] != (
        pre_quiescence["final_dirty_kib"] + pre_quiescence["final_writeback_kib"]
    ) or pre_quiescence["final_total_kib"] > pre_quiescence[
        "maximum_dirty_writeback_kib"
    ]:
        raise GateError("pre-run writeback-quiescence final counters are inconsistent")
    correctness = validate_replay_correctness(
        load_json(correctness_path), plan["workload"]["stop_after_messages"]
    )
    corpus = require_exact_keys(
        load_json(corpus_path),
        {"schema", "file_count", "size_bytes", "manifest_sha256"},
        "$corpus",
    )
    if corpus["schema"] != phase1.CORPUS_SUMMARY_SCHEMA:
        raise GateError("corpus summary schema mismatch")
    strict_int(corpus["file_count"], "$.corpus.file_count", minimum=1)
    strict_int(corpus["size_bytes"], "$.corpus.size_bytes", minimum=1)
    if not isinstance(corpus["manifest_sha256"], str) or re.fullmatch(
        r"[0-9a-f]{64}", corpus["manifest_sha256"]
    ) is None:
        raise GateError("corpus manifest digest is invalid")
    return {
        "schema": OBSERVATION_SCHEMA,
        "run_index": run_index,
        "block": row["block"],
        "position": row["position"],
        "policy": policy_name,
        "binary_role": expected["binary_role"],
        "binary_sha256": current_binary_sha256,
        "build_provenance_sha256": sha256_file(build_provenance_path),
        "jemalloc_stats_enabled": build_provenance["jemalloc_stats_enabled"],
        "allocator_effective_policy": effective,
        "runtime_effective_policy": runtime_policy["effective_policy"],
        "preflight_record_sha256": sha256_file(preflight_path),
        "runtime_policy_record_sha256": sha256_file(runtime_policy_path),
        "allocator_telemetry_record_sha256": sha256_file(allocator_telemetry_path),
        "external_conflict_guardian_sha256": sha256_file(guardian_path),
        "pre_run_writeback_quiescence_sha256": sha256_file(pre_quiescence_path),
        "writeback_quiescence_sha256": sha256_file(quiescence_path),
        "workload_wall_ns": checkpoint["workload_wall_ns"],
        "workload_cpu_ticks": checkpoint["workload_cpu_ticks"],
        "workload_cpu_seconds": checkpoint["workload_cpu_seconds"],
        "clock_ticks_per_second": checkpoint["clock_ticks_per_second"],
        "workload_cpu_boundary_uncertainty_ns": checkpoint[
            "workload_cpu_boundary_uncertainty_ns"
        ],
        "full_elapsed": timing["elapsed"],
        "full_user_seconds": strict_number(timing["user_seconds"], "$.time.user_seconds"),
        "full_system_seconds": strict_number(timing["system_seconds"], "$.time.system_seconds"),
        "time_max_rss_kib": strict_int(timing["max_rss_kib"], "$.time.max_rss_kib", minimum=1),
        "perf": perf,
        "external_conflict_guardian": guardian,
        "pre_run_writeback_quiescence": pre_quiescence,
        "writeback_quiescence": quiescence,
        "rss": checkpoint["rss"],
        "hold_elapsed_ns": checkpoint["hold_elapsed_ns"],
        "allocator_release_telemetry": allocator_telemetry,
        "corpus": corpus,
        "correctness_sha256": sha256_file(correctness_path),
        "correctness": correctness,
    }


def median_pair(values: list[float]) -> float:
    if len(values) != 2:
        raise GateError(f"expected exactly two mirrored observations; got {len(values)}")
    return sum(values) / 2.0


def optional_median_pair(values: list[int | None]) -> float | None:
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise GateError("allocator telemetry availability changed between mirrored runs")
    return median_pair([float(value) for value in values if value is not None])


def pair_relative_spread_percent(values: list[float]) -> float:
    center = median_pair(values)
    if center == 0:
        return 0.0 if values[0] == values[1] else math.inf
    return abs(values[0] - values[1]) / abs(center) * 100.0


def validate_observation_telemetry(
    value: Any,
    policy_name: str,
    expected_policy: dict[str, Any],
    plan: dict[str, Any],
    observation_index: int,
) -> list[dict[str, Any]]:
    context = f"$observations[{observation_index}].allocator_release_telemetry"
    telemetry = require_exact_keys(
        value,
        {
            "schema",
            "policy",
            "rust_global_allocator",
            "records",
            "checkpoint_bounds",
            "hold_complete_minus_post_drop_bytes",
            "external_rss_reconciliation",
            "measurement_relation",
        },
        context,
    )
    if telemetry["schema"] != TELEMETRY_SUMMARY_SCHEMA:
        raise GateError(f"observation {observation_index + 1} telemetry schema mismatch")
    if telemetry["policy"] != policy_name:
        raise GateError(f"observation {observation_index + 1} telemetry policy mismatch")
    if telemetry["rust_global_allocator"] != expected_policy["rust_global_allocator"]:
        raise GateError(f"observation {observation_index + 1} telemetry identity mismatch")
    if telemetry["measurement_relation"] != ALLOCATOR_RSS_RELATION:
        raise GateError(f"observation {observation_index + 1} RSS relation changed")
    bounds = require_exact_keys(
        telemetry["checkpoint_bounds"],
        {
            "drop_main_elapsed_ns",
            "hold_complete_main_elapsed_ns",
            "drop_unix_time_ns",
            "hold_complete_unix_time_ns",
        },
        f"{context}.checkpoint_bounds",
    )
    for key, item in bounds.items():
        strict_int(item, f"{context}.checkpoint_bounds.{key}", minimum=1)
    records = telemetry["records"]
    if not isinstance(records, list) or len(records) != 2:
        raise GateError(f"observation {observation_index + 1} lacks two telemetry rows")
    record_keys = {
        "schema",
        "phase",
        "main_elapsed_ns",
        "unix_time_ns",
        "rust_global_allocator",
        "allocator_internal_telemetry",
        "epoch",
        "allocated_bytes",
        "active_bytes",
        "resident_bytes",
        "mapped_bytes",
        "retained_bytes",
    }
    stat_keys = (
        "allocated_bytes",
        "active_bytes",
        "resident_bytes",
        "mapped_bytes",
        "retained_bytes",
    )
    for record_index, record_value in enumerate(records):
        record = require_exact_keys(
            record_value, record_keys, f"{context}.records[{record_index}]"
        )
        if record["schema"] != TELEMETRY_SCHEMA:
            raise GateError(f"observation {observation_index + 1} telemetry row schema mismatch")
        if record["rust_global_allocator"] != expected_policy["rust_global_allocator"]:
            raise GateError(f"observation {observation_index + 1} telemetry row identity mismatch")
        strict_int(
            record["main_elapsed_ns"],
            f"{context}.records[{record_index}].main_elapsed_ns",
            minimum=1,
        )
        strict_int(
            record["unix_time_ns"],
            f"{context}.records[{record_index}].unix_time_ns",
            minimum=1,
        )
        if policy_name == "S":
            if record["allocator_internal_telemetry"] != "unavailable":
                raise GateError("system allocator telemetry is not explicit unavailable")
            if record["epoch"] is not None or any(record[key] is not None for key in stat_keys):
                raise GateError("system allocator telemetry contains invented internal values")
        else:
            if record["allocator_internal_telemetry"] != "available":
                raise GateError("jemalloc observation lacks internal release telemetry")
            strict_int(
                record["epoch"],
                f"{context}.records[{record_index}].epoch",
                minimum=1,
            )
            for key in stat_keys:
                strict_int(
                    record[key],
                    f"{context}.records[{record_index}].{key}",
                    minimum=0,
                )
            if record["active_bytes"] < record["allocated_bytes"]:
                raise GateError("jemalloc observation active bytes are below allocated bytes")
    if [record["phase"] for record in records] != [
        "post_ingester_drop",
        "hold_complete",
    ]:
        raise GateError(f"observation {observation_index + 1} telemetry phases differ")
    if not (
        bounds["drop_main_elapsed_ns"]
        <= records[0]["main_elapsed_ns"]
        <= records[1]["main_elapsed_ns"]
        <= bounds["hold_complete_main_elapsed_ns"]
    ) or not (
        bounds["drop_unix_time_ns"]
        <= records[0]["unix_time_ns"]
        <= records[1]["unix_time_ns"]
        <= bounds["hold_complete_unix_time_ns"]
    ):
        raise GateError(f"observation {observation_index + 1} telemetry escapes bounds")
    if policy_name != "S" and records[1]["epoch"] <= records[0]["epoch"]:
        raise GateError(f"observation {observation_index + 1} telemetry epoch did not advance")

    deltas = require_exact_keys(
        telemetry["hold_complete_minus_post_drop_bytes"],
        set(stat_keys),
        f"{context}.hold_complete_minus_post_drop_bytes",
    )
    for key in stat_keys:
        expected_delta = (
            None
            if records[0][key] is None
            else records[1][key] - records[0][key]
        )
        if deltas[key] != expected_delta:
            raise GateError(
                f"observation {observation_index + 1} telemetry {key} delta differs"
            )
    reconciliation = telemetry["external_rss_reconciliation"]
    if not isinstance(reconciliation, list) or len(reconciliation) != 2:
        raise GateError(f"observation {observation_index + 1} lacks RSS reconciliation")
    reconciliation_keys = {
        "phase",
        "allocator_telemetry_unix_time_ns",
        "external_rss_unix_time_ns",
        "alignment_abs_ns",
        "external_process_tree_rss_bytes",
        "jemalloc_resident_bytes",
        "external_minus_jemalloc_resident_bytes",
        "measurement_relation",
    }
    maximum_alignment_ns = plan["workload"]["rss_interval_ms"] * 1_000_000
    for record, row_value in zip(records, reconciliation, strict=True):
        row = require_exact_keys(row_value, reconciliation_keys, f"{context}.reconciliation")
        if row["phase"] != record["phase"]:
            raise GateError(f"observation {observation_index + 1} reconciliation phase differs")
        if row["allocator_telemetry_unix_time_ns"] != record["unix_time_ns"]:
            raise GateError(f"observation {observation_index + 1} reconciliation time differs")
        external_time = strict_int(
            row["external_rss_unix_time_ns"], f"{context}.external_rss_time", minimum=1
        )
        alignment = strict_int(row["alignment_abs_ns"], f"{context}.alignment", minimum=0)
        if alignment != abs(external_time - record["unix_time_ns"]):
            raise GateError(f"observation {observation_index + 1} RSS alignment differs")
        if alignment > maximum_alignment_ns:
            raise GateError(f"observation {observation_index + 1} RSS alignment is too distant")
        external = strict_int(
            row["external_process_tree_rss_bytes"], f"{context}.external_rss", minimum=1
        )
        if row["jemalloc_resident_bytes"] != record["resident_bytes"]:
            raise GateError(f"observation {observation_index + 1} resident reconciliation differs")
        expected_difference = (
            None
            if record["resident_bytes"] is None
            else external - record["resident_bytes"]
        )
        if row["external_minus_jemalloc_resident_bytes"] != expected_difference:
            raise GateError(f"observation {observation_index + 1} RSS difference differs")
        if row["measurement_relation"] != ALLOCATOR_RSS_RELATION:
            raise GateError(f"observation {observation_index + 1} RSS relation differs")
    return records


def compare_screen(
    observation_paths: list[Path], plan_path: Path, phase1_expectations: Path
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    if len(observation_paths) != len(plan["schedule"]):
        raise GateError("screen must supply exactly ten observations")
    observations = [load_json(path) for path in observation_paths]
    observations.sort(key=lambda item: item.get("run_index", -1))
    baseline_corpus = None
    baseline_correctness = None
    by_policy: dict[str, list[dict[str, Any]]] = {name: [] for name in POLICY_ORDER}
    binary_hashes: dict[str, str] = {}
    build_provenance_hashes: set[str] = set()
    correctness_file_hashes: set[str] = set()
    observed_clock_ticks: set[int] = set()
    observation_keys = {
        "schema",
        "run_index",
        "block",
        "position",
        "policy",
        "binary_role",
        "binary_sha256",
        "build_provenance_sha256",
        "jemalloc_stats_enabled",
        "allocator_effective_policy",
        "runtime_effective_policy",
        "preflight_record_sha256",
        "runtime_policy_record_sha256",
        "allocator_telemetry_record_sha256",
        "external_conflict_guardian_sha256",
        "pre_run_writeback_quiescence_sha256",
        "writeback_quiescence_sha256",
        "workload_wall_ns",
        "workload_cpu_ticks",
        "workload_cpu_seconds",
        "clock_ticks_per_second",
        "workload_cpu_boundary_uncertainty_ns",
        "full_elapsed",
        "full_user_seconds",
        "full_system_seconds",
        "time_max_rss_kib",
        "perf",
        "external_conflict_guardian",
        "pre_run_writeback_quiescence",
        "writeback_quiescence",
        "rss",
        "hold_elapsed_ns",
        "allocator_release_telemetry",
        "corpus",
        "correctness_sha256",
        "correctness",
    }
    for index, (observed, schedule) in enumerate(
        zip(observations, plan["schedule"], strict=True), start=1
    ):
        require_exact_keys(observed, observation_keys, f"$observations[{index - 1}]")
        if observed.get("schema") != OBSERVATION_SCHEMA:
            raise GateError(f"observation {index} schema mismatch")
        for key in ("run_index", "block", "position", "policy"):
            if observed.get(key) != schedule[key]:
                raise GateError(f"observation {index} differs from schedule field {key}")
        expected_policy = plan["policies"][schedule["policy"]]
        if observed["binary_role"] != expected_policy["binary_role"]:
            raise GateError(f"observation {index} binary role differs from the plan")
        binary_sha256 = observed["binary_sha256"]
        if not isinstance(binary_sha256, str) or re.fullmatch(
            r"[0-9a-f]{64}", binary_sha256
        ) is None:
            raise GateError(f"observation {index} binary hash is invalid")
        previous_hash = binary_hashes.setdefault(observed["binary_role"], binary_sha256)
        if previous_hash != binary_sha256:
            raise GateError(
                f"observation {index} changed the {observed['binary_role']} binary hash"
            )
        for digest_key in (
            "build_provenance_sha256",
            "preflight_record_sha256",
            "runtime_policy_record_sha256",
            "allocator_telemetry_record_sha256",
            "external_conflict_guardian_sha256",
            "pre_run_writeback_quiescence_sha256",
            "writeback_quiescence_sha256",
            "correctness_sha256",
        ):
            if not isinstance(observed[digest_key], str) or re.fullmatch(
                r"[0-9a-f]{64}", observed[digest_key]
            ) is None:
                raise GateError(f"observation {index} {digest_key} is invalid")
        build_provenance_hashes.add(observed["build_provenance_sha256"])
        correctness_file_hashes.add(observed["correctness_sha256"])
        if observed["jemalloc_stats_enabled"] is not True:
            raise GateError(f"observation {index} is not from the stats-enabled screen build")
        if observed["external_conflict_guardian"].get("complete_and_conflict_free") is not True:
            raise GateError(f"observation {index} external-conflict guardian failed")
        if observed["external_conflict_guardian"].get("conflicts") != []:
            raise GateError(f"observation {index} external-conflict guardian found a conflict")
        if observed["writeback_quiescence"].get("passed") is not True:
            raise GateError(f"observation {index} writeback quiescence failed")
        if observed["pre_run_writeback_quiescence"].get("passed") is not True:
            raise GateError(f"observation {index} pre-run writeback quiescence failed")
        expected_requested_effective = (
            None
            if expected_policy["jemalloc_conf"] is None
            else requested_effective_entries(expected_policy["jemalloc_conf"])
        )
        effective = observed["allocator_effective_policy"]
        if observed["runtime_effective_policy"] != effective:
            raise GateError(
                f"observation {index} full runtime effective policy differs from preflight"
            )
        if expected_policy["rust_global_allocator"] == "system":
            if effective is not None:
                raise GateError(f"observation {index} overstates system allocator internals")
        else:
            effective = require_exact_keys(
                effective,
                EFFECTIVE_POLICY_KEYS,
                f"$observations[{index - 1}].allocator_effective_policy",
            )
            for key in (
                "abort_conf",
                "confirm_conf",
                "background_thread",
                "retain",
            ):
                if type(effective[key]) is not bool:
                    raise GateError(
                        f"observation {index} effective jemalloc {key} must be boolean"
                    )
            for key in (
                "narenas",
                "dirty_decay_ms",
                "muzzy_decay_ms",
                "max_background_threads",
            ):
                if type(effective[key]) is not int:
                    raise GateError(
                        f"observation {index} effective jemalloc {key} must be integer"
                    )
            if effective["narenas"] < 1 or effective["max_background_threads"] < 1:
                raise GateError(
                    f"observation {index} effective jemalloc counts must be positive"
                )
            for key, value in (expected_requested_effective or {}).items():
                if type(effective[key]) is not type(value) or effective[key] != value:
                    raise GateError(
                        f"observation {index} effective jemalloc {key} differs"
                    )
        workload_cpu_ticks = strict_int(
            observed["workload_cpu_ticks"],
            f"$observations[{index - 1}].workload_cpu_ticks",
            minimum=1,
        )
        clock_ticks = strict_int(
            observed["clock_ticks_per_second"],
            f"$observations[{index - 1}].clock_ticks_per_second",
            minimum=1,
        )
        observed_clock_ticks.add(clock_ticks)
        workload_cpu_seconds = strict_number(
            observed["workload_cpu_seconds"],
            f"$observations[{index - 1}].workload_cpu_seconds",
        )
        if abs(workload_cpu_seconds - workload_cpu_ticks / clock_ticks) > 1e-12:
            raise GateError(f"observation {index} workload CPU does not derive from ticks")
        maximum_uncertainty = (
            plan["workload"]["rss_interval_ms"]
            * 1_000_000
            * plan["screen_candidate_gate"][
                "maximum_workload_cpu_boundary_uncertainty_intervals"
            ]
        )
        if strict_int(
            observed["workload_cpu_boundary_uncertainty_ns"],
            f"$observations[{index - 1}].workload_cpu_boundary_uncertainty_ns",
            minimum=0,
        ) > maximum_uncertainty:
            raise GateError(f"observation {index} workload CPU boundary is too uncertain")
        strict_int(
            observed["rss"].get("workload_peak_rss_kib"),
            f"$observations[{index - 1}].rss.workload_peak_rss_kib",
            minimum=1,
        )
        strict_int(
            observed["rss"].get("workload_boundary_max_single_hwm_kib"),
            f"$observations[{index - 1}].rss.workload_boundary_max_single_hwm_kib",
            minimum=1,
        )
        strict_int(
            observed["rss"].get("workload_peak_max_single_hwm_kib"),
            f"$observations[{index - 1}].rss.workload_peak_max_single_hwm_kib",
            minimum=1,
        )
        validate_observation_telemetry(
            observed["allocator_release_telemetry"],
            schedule["policy"],
            expected_policy,
            plan,
            index - 1,
        )
        telemetry_bounds = observed["allocator_release_telemetry"]["checkpoint_bounds"]
        if telemetry_bounds["drop_main_elapsed_ns"] != observed["workload_wall_ns"]:
            raise GateError(f"observation {index} telemetry/workload boundary differs")
        if (
            telemetry_bounds["hold_complete_main_elapsed_ns"]
            - telemetry_bounds["drop_main_elapsed_ns"]
            != observed["hold_elapsed_ns"]
        ):
            raise GateError(f"observation {index} telemetry/hold duration differs")
        rss = observed["rss"]
        if (
            rss.get("workload_boundary_cpu_ticks") != workload_cpu_ticks
            or rss.get("clock_ticks_per_second") != clock_ticks
            or rss.get("workload_boundary_cpu_seconds") != workload_cpu_seconds
        ):
            raise GateError(f"observation {index} RSS/workload CPU record differs")
        rss_window_start = strict_int(
            rss.get("workload_boundary_sample_window_start_unix_time_ns"),
            f"$observations[{index - 1}].rss.boundary_window_start",
            minimum=1,
        )
        rss_boundary_time = strict_int(
            rss.get("workload_boundary_sample_unix_time_ns"),
            f"$observations[{index - 1}].rss.boundary_time",
            minimum=1,
        )
        exact_uncertainty = max(
            abs(rss_window_start - telemetry_bounds["drop_unix_time_ns"]),
            abs(rss_boundary_time - telemetry_bounds["drop_unix_time_ns"]),
        )
        if exact_uncertainty != observed["workload_cpu_boundary_uncertainty_ns"]:
            raise GateError(f"observation {index} CPU boundary uncertainty differs")
        validate_replay_correctness(
            observed.get("correctness"), plan["workload"]["stop_after_messages"]
        )
        if baseline_corpus is None:
            baseline_corpus = observed.get("corpus")
            baseline_correctness = observed.get("correctness")
        else:
            if observed.get("corpus") != baseline_corpus:
                raise GateError(f"observation {index} corpus differs from run 1")
            if observed.get("correctness") != baseline_correctness:
                raise GateError(f"observation {index} replay correctness differs from run 1")
        if observed.get("rss", {}).get("peak_vm_swap_kib") != 0:
            raise GateError(f"observation {index} used swap")
        by_policy[schedule["policy"]].append(observed)

    if set(binary_hashes) != {"system", "jemalloc"}:
        raise GateError("screen did not preserve both allocator binary roles")
    if binary_hashes["system"] == binary_hashes["jemalloc"]:
        raise GateError("system and jemalloc comparator hashes must differ")
    if len(observed_clock_ticks) != 1:
        raise GateError("CLK_TCK changed between allocator observations")
    if len(build_provenance_hashes) != 1:
        raise GateError("allocator observations do not share one hash-bound controlled build")
    if len(correctness_file_hashes) != 1:
        raise GateError("allocator observations do not share byte-identical correctness evidence")

    policy_summary = {}
    telemetry_stat_keys = (
        "allocated_bytes",
        "active_bytes",
        "resident_bytes",
        "mapped_bytes",
        "retained_bytes",
    )
    for policy_name in POLICY_ORDER:
        rows = by_policy[policy_name]
        pair_spread = {
            "workload_cpu_seconds": pair_relative_spread_percent(
                [row["workload_cpu_seconds"] for row in rows]
            ),
            "workload_peak_rss_kib": pair_relative_spread_percent(
                [float(row["rss"]["workload_peak_rss_kib"]) for row in rows]
            ),
            "workload_boundary_max_single_hwm_kib": pair_relative_spread_percent(
                [
                    float(row["rss"]["workload_boundary_max_single_hwm_kib"])
                    for row in rows
                ]
            ),
            "post_drop_end_rss_kib": pair_relative_spread_percent(
                [float(row["rss"]["post_drop_end_rss_kib"]) for row in rows]
            ),
        }
        post_drop_stats = {
            key: optional_median_pair(
                [row["allocator_release_telemetry"]["records"][0][key] for row in rows]
            )
            for key in telemetry_stat_keys
        }
        hold_complete_stats = {
            key: optional_median_pair(
                [row["allocator_release_telemetry"]["records"][1][key] for row in rows]
            )
            for key in telemetry_stat_keys
        }
        release_deltas = {
            key: optional_median_pair(
                [
                    row["allocator_release_telemetry"][
                        "hold_complete_minus_post_drop_bytes"
                    ][key]
                    for row in rows
                ]
            )
            for key in telemetry_stat_keys
        }
        policy_summary[policy_name] = {
            "workload_wall_ns": median_pair([row["workload_wall_ns"] for row in rows]),
            "workload_cpu_seconds": median_pair(
                [row["workload_cpu_seconds"] for row in rows]
            ),
            "clock_ticks_per_second": next(iter(observed_clock_ticks)),
            "workload_cpu_boundary_uncertainty_ns": max(
                row["workload_cpu_boundary_uncertainty_ns"] for row in rows
            ),
            "total_lifecycle_task_clock": median_pair(
                [row["perf"]["task-clock"] for row in rows]
            ),
            "total_lifecycle_cycles": median_pair(
                [row["perf"]["cycles"] for row in rows]
            ),
            "total_lifecycle_instructions": median_pair(
                [row["perf"]["instructions"] for row in rows]
            ),
            "workload_peak_rss_kib": median_pair(
                [row["rss"]["workload_peak_rss_kib"] for row in rows]
            ),
            "workload_boundary_max_single_hwm_kib": median_pair(
                [row["rss"]["workload_boundary_max_single_hwm_kib"] for row in rows]
            ),
            "total_lifecycle_peak_rss_kib": median_pair(
                [row["rss"]["peak_rss_kib"] for row in rows]
            ),
            "post_drop_end_rss_kib": median_pair(
                [row["rss"]["post_drop_end_rss_kib"] for row in rows]
            ),
            "total_lifecycle_minor_faults": median_pair(
                [row["perf"]["minor-faults"] for row in rows]
            ),
            "total_lifecycle_major_faults": median_pair(
                [row["perf"]["major-faults"] for row in rows]
            ),
            "allocator_release": {
                "post_ingester_drop": post_drop_stats,
                "hold_complete": hold_complete_stats,
                "hold_complete_minus_post_drop_bytes": release_deltas,
                "measurement_relation": ALLOCATOR_RSS_RELATION,
            },
            "mirrored_pair_relative_spread_percent": pair_spread,
            "mirrored_pair_dispersion_pass": all(
                value
                <= plan["screen_candidate_gate"][
                    "maximum_mirrored_pair_relative_spread_percent"
                ]
                for value in pair_spread.values()
            ),
        }
    system = policy_summary["S"]
    for denominator in (
        "workload_cpu_seconds",
        "workload_peak_rss_kib",
        "workload_boundary_max_single_hwm_kib",
        "post_drop_end_rss_kib",
    ):
        if system[denominator] <= 0:
            raise GateError(f"system baseline {denominator} must be positive")
    gate = plan["screen_candidate_gate"]
    candidates = {}
    for policy_name in POLICY_ORDER[1:]:
        current = policy_summary[policy_name]
        workload_cpu_improvement = (
            (system["workload_cpu_seconds"] - current["workload_cpu_seconds"])
            / system["workload_cpu_seconds"]
            * 100.0
        )
        workload_peak_rss_regression = (
            (current["workload_peak_rss_kib"] - system["workload_peak_rss_kib"])
            / system["workload_peak_rss_kib"]
            * 100.0
        )
        workload_hwm_regression = (
            current["workload_boundary_max_single_hwm_kib"]
            - system["workload_boundary_max_single_hwm_kib"]
        ) / system["workload_boundary_max_single_hwm_kib"] * 100.0
        end_rss_regression = (
            current["post_drop_end_rss_kib"] - system["post_drop_end_rss_kib"]
        ) / system["post_drop_end_rss_kib"] * 100.0
        comparator_only = plan["policies"][policy_name]["comparator_only"]
        dispersion_pass = (
            system["mirrored_pair_dispersion_pass"]
            and current["mirrored_pair_dispersion_pass"]
        )
        eligible = (
            not comparator_only
            and dispersion_pass
            and workload_cpu_improvement
            >= gate["minimum_workload_cpu_improvement_percent"]
            and workload_peak_rss_regression
            <= gate["maximum_workload_peak_rss_regression_percent"]
            and workload_hwm_regression
            <= gate["maximum_workload_hwm_regression_percent"]
            and end_rss_regression
            <= gate["maximum_post_drop_end_rss_regression_percent"]
        )
        candidates[policy_name] = {
            "comparator_only": comparator_only,
            "workload_cpu_improvement_percent": workload_cpu_improvement,
            "workload_peak_rss_regression_percent": workload_peak_rss_regression,
            "workload_hwm_regression_percent": workload_hwm_regression,
            "post_drop_end_rss_regression_percent": end_rss_regression,
            "mirrored_pair_dispersion_pass": dispersion_pass,
            "eligible_for_full_gate": eligible,
            "ineligibility_reason": (
                "unset jemalloc default is comparator-only"
                if comparator_only
                else None if eligible else "one or more frozen screen thresholds failed"
            ),
            "production_promotable": False,
        }
    eligible_policies = [
        name for name in POLICY_ORDER[1:] if candidates[name]["eligible_for_full_gate"]
    ]
    selected_policy = None
    if eligible_policies:
        selected_policy = min(
            eligible_policies,
            key=lambda name: (
                -candidates[name]["workload_cpu_improvement_percent"],
                candidates[name]["workload_hwm_regression_percent"],
                candidates[name]["workload_peak_rss_regression_percent"],
                POLICY_ORDER.index(name),
            ),
        )
    return {
        "schema": SUMMARY_SCHEMA,
        "complete_screen": True,
        "run_count": len(observations),
        "binary_sha256_by_role": binary_hashes,
        "corpus": baseline_corpus,
        "correctness": baseline_correctness,
        "correctness_sha256": hashlib.sha256(
            json.dumps(baseline_correctness, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "correctness_file_sha256": next(iter(correctness_file_hashes)),
        "build_provenance_sha256": next(iter(build_provenance_hashes)),
        "policy_medians": policy_summary,
        "candidates": candidates,
        "eligible_policies": eligible_policies,
        "selected_full_gate_policy": selected_policy,
        "deterministic_advancement_rule": (
            "among eligible bounded policies J1..J3: greatest workload CPU improvement; "
            "then lowest workload HWM regression; then lowest workload RSS regression; "
            "then frozen policy order"
        ),
        "jemalloc_stats_enabled": True,
        "partial_runs_promotable": False,
        "decision_scope": plan["completion_contract"]["decision_scope"],
    }


def validate_corpus_summary(value: Any, path: str) -> dict[str, Any]:
    corpus = require_exact_keys(
        value,
        {"schema", "file_count", "size_bytes", "manifest_sha256"},
        path,
    )
    if corpus["schema"] != phase1.CORPUS_SUMMARY_SCHEMA:
        raise GateError(f"{path} schema mismatch")
    strict_int(corpus["file_count"], f"{path}.file_count", minimum=1)
    strict_int(corpus["size_bytes"], f"{path}.size_bytes", minimum=1)
    if not isinstance(corpus["manifest_sha256"], str) or re.fullmatch(
        r"[0-9a-f]{64}", corpus["manifest_sha256"]
    ) is None:
        raise GateError(f"{path}.manifest_sha256 is invalid")
    return corpus


def validate_capture_reinventory(before_path: Path, after_path: Path) -> dict[str, Any]:
    if sha256_file(before_path) != sha256_file(after_path):
        raise GateError("source capture/config inventory changed during the experiment")
    value = require_exact_keys(
        load_json(before_path),
        {
            "capture",
            "capture_manifest_sha256",
            "capture_files",
            "config_template",
            "config_template_sha256",
            "stop_after_messages",
        },
        "$.capture_inventory",
    )
    for field in ("capture", "config_template"):
        if not isinstance(value[field], str) or not Path(value[field]).is_absolute():
            raise GateError(f"capture inventory {field} must be absolute")
    for field in ("capture_manifest_sha256", "config_template_sha256"):
        if not isinstance(value[field], str) or re.fullmatch(
            r"[0-9a-f]{64}", value[field]
        ) is None:
            raise GateError(f"capture inventory {field} is invalid")
    strict_int(
        value["stop_after_messages"],
        "$.capture_inventory.stop_after_messages",
        minimum=1,
    )
    files = value["capture_files"]
    if not isinstance(files, list) or not files:
        raise GateError("capture inventory has no partition files")
    names = []
    for index, item in enumerate(files):
        row = require_exact_keys(
            item, {"name", "size_bytes", "sha256"}, f"$.capture_files[{index}]"
        )
        if not isinstance(row["name"], str) or Path(row["name"]).name != row["name"]:
            raise GateError("capture inventory contains an unsafe partition name")
        strict_int(row["size_bytes"], f"$.capture_files[{index}].size_bytes", minimum=1)
        if not isinstance(row["sha256"], str) or re.fullmatch(
            r"[0-9a-f]{64}", row["sha256"]
        ) is None:
            raise GateError("capture inventory contains an invalid partition hash")
        names.append(row["name"])
    if len(names) != len(set(names)):
        raise GateError("capture inventory contains duplicate partition names")
    return {
        "sha256": sha256_file(before_path),
        "capture_manifest_sha256": value["capture_manifest_sha256"],
        "capture_file_count": len(files),
        "capture_files": files,
        "config_template_sha256": value["config_template_sha256"],
    }


def _validate_inventory_histogram(
    value: Any, path: str, expected_observations: int
) -> dict[str, Any]:
    histogram = require_exact_keys(value, {"zero_count", "buckets"}, path)
    zero_count = strict_int(histogram["zero_count"], f"{path}.zero_count", minimum=0)
    buckets = histogram["buckets"]
    if not isinstance(buckets, list):
        raise GateError(f"{path}.buckets must be a list")
    normalized = []
    previous_upper = 0
    observations = zero_count
    for index, item in enumerate(buckets):
        item_path = f"{path}.buckets[{index}]"
        bucket = require_exact_keys(
            item, {"lower_inclusive", "upper_inclusive", "count"}, item_path
        )
        lower = strict_int(
            bucket["lower_inclusive"], f"{item_path}.lower_inclusive", minimum=1
        )
        upper = strict_int(
            bucket["upper_inclusive"], f"{item_path}.upper_inclusive", minimum=1
        )
        count = strict_int(bucket["count"], f"{item_path}.count", minimum=1)
        if lower & (lower - 1) or upper != 2 * lower - 1:
            raise GateError(f"{item_path} is not an exact power-of-two bucket")
        if lower <= previous_upper:
            raise GateError(f"{path}.buckets are not strictly ascending and disjoint")
        previous_upper = upper
        observations += count
        normalized.append(
            {
                "lower_inclusive": lower,
                "upper_inclusive": upper,
                "count": count,
            }
        )
    if observations != expected_observations:
        raise GateError(
            f"{path} observations do not reconcile: expected "
            f"{expected_observations}, found {observations}"
        )
    return {"zero_count": zero_count, "buckets": normalized}


def _validate_inventory_winner(value: Any, path: str) -> dict[str, int]:
    winner = require_exact_keys(value, {"chunks", "points"}, path)
    return {
        key: strict_int(winner[key], f"{path}.{key}", minimum=0)
        for key in ("chunks", "points")
    }


def _validate_timestamp_candidate(value: Any, path: str) -> dict[str, Any]:
    candidate = require_exact_keys(
        value, {"bytes", "unique_wins", "adaptive_selections"}, path
    )
    return {
        "bytes": strict_int(candidate["bytes"], f"{path}.bytes", minimum=0),
        "unique_wins": _validate_inventory_winner(
            candidate["unique_wins"], f"{path}.unique_wins"
        ),
        "adaptive_selections": _validate_inventory_winner(
            candidate["adaptive_selections"], f"{path}.adaptive_selections"
        ),
    }


def _validate_timestamp_evidence(value: Any, path: str) -> dict[str, Any]:
    evidence = require_exact_keys(
        value,
        {
            "chunks",
            "points",
            "adaptive_min_bytes",
            "tied_minima",
            *TIMESTAMP_CANDIDATES,
        },
        path,
    )
    parsed: dict[str, Any] = {
        "chunks": strict_int(evidence["chunks"], f"{path}.chunks", minimum=0),
        "points": strict_int(evidence["points"], f"{path}.points", minimum=0),
        "adaptive_min_bytes": strict_int(
            evidence["adaptive_min_bytes"],
            f"{path}.adaptive_min_bytes",
            minimum=0,
        ),
        "tied_minima": _validate_inventory_winner(
            evidence["tied_minima"], f"{path}.tied_minima"
        ),
    }
    parsed.update(
        {
            candidate: _validate_timestamp_candidate(
                evidence[candidate], f"{path}.{candidate}"
            )
            for candidate in TIMESTAMP_CANDIDATES
        }
    )
    selected_chunks = sum(
        parsed[candidate]["adaptive_selections"]["chunks"]
        for candidate in TIMESTAMP_CANDIDATES
    )
    selected_points = sum(
        parsed[candidate]["adaptive_selections"]["points"]
        for candidate in TIMESTAMP_CANDIDATES
    )
    if (selected_chunks, selected_points) != (parsed["chunks"], parsed["points"]):
        raise GateError(f"{path}: adaptive timestamp selections do not reconcile")
    unique_chunks = sum(
        parsed[candidate]["unique_wins"]["chunks"]
        for candidate in TIMESTAMP_CANDIDATES
    )
    unique_points = sum(
        parsed[candidate]["unique_wins"]["points"]
        for candidate in TIMESTAMP_CANDIDATES
    )
    ties = parsed["tied_minima"]
    if (unique_chunks + ties["chunks"], unique_points + ties["points"]) != (
        parsed["chunks"],
        parsed["points"],
    ):
        raise GateError(f"{path}: unique timestamp wins and ties do not reconcile")
    if parsed["adaptive_min_bytes"] > min(
        parsed[candidate]["bytes"] for candidate in TIMESTAMP_CANDIDATES
    ):
        raise GateError(f"{path}: adaptive timestamp bytes exceed an aggregate candidate")
    return parsed


def _timestamp_additive_totals(value: dict[str, Any]) -> dict[str, int]:
    totals = {
        "chunks": value["chunks"],
        "points": value["points"],
        "adaptive_min_bytes": value["adaptive_min_bytes"],
        "tied_minima.chunks": value["tied_minima"]["chunks"],
        "tied_minima.points": value["tied_minima"]["points"],
    }
    for candidate in TIMESTAMP_CANDIDATES:
        totals[f"{candidate}.bytes"] = value[candidate]["bytes"]
        for winner in ("unique_wins", "adaptive_selections"):
            totals[f"{candidate}.{winner}.chunks"] = value[candidate][winner][
                "chunks"
            ]
            totals[f"{candidate}.{winner}.points"] = value[candidate][winner][
                "points"
            ]
    return totals


def _reconcile_timestamp_breakdown(
    rows: list[dict[str, Any]], all_blocks: dict[str, Any], path: str
) -> None:
    expected = _timestamp_additive_totals(all_blocks)
    actual = {key: 0 for key in expected}
    for row in rows:
        for key, value in _timestamp_additive_totals(row).items():
            actual[key] += value
    mismatches = [key for key in expected if actual[key] != expected[key]]
    if mismatches:
        field = mismatches[0]
        raise GateError(
            f"{path}: additive timestamp field {field} does not reconcile: "
            f"expected {expected[field]}, found {actual[field]}"
        )


def validate_chunk_inventory(
    value: Any,
    *,
    chunks: int,
    samples: int,
    logical_chunk_bytes: int,
    chunks_by_kind: list[int],
    path: str,
) -> dict[str, Any]:
    inventory = require_exact_keys(
        value,
        {"layout", "by_kind_encoding", "raw_f64_vs_gorilla", "timestamp_candidates"},
        path,
    )
    if inventory["layout"] != "sealed_chunk_v1":
        raise GateError(f"{path}.layout differs from sealed_chunk_v1")
    rows = inventory["by_kind_encoding"]
    if not isinstance(rows, list) or not rows:
        raise GateError(f"{path}.by_kind_encoding must be a nonempty list")

    total_chunks = total_points = total_indexed_bytes = 0
    total_native_timestamp_bytes = 0
    inventory_chunks_by_kind = {kind: 0 for kind in KIND_ORDER}
    float_chunks = float_points = float_indexed = float_payload = 0
    float_encodings: set[str] = set()
    kind_encoding_keys: set[tuple[str, str]] = set()
    for index, item in enumerate(rows):
        item_path = f"{path}.by_kind_encoding[{index}]"
        row = require_exact_keys(item, CHUNK_INVENTORY_ROW_FIELDS, item_path)
        for key in ("kind", "encoding", "payload_layout"):
            if not isinstance(row[key], str) or not row[key]:
                raise GateError(f"{item_path}.{key} must be a nonempty string")
        kind_encoding = (row["kind"], row["encoding"])
        if kind_encoding in kind_encoding_keys:
            raise GateError(f"{path} has a duplicate kind/encoding row: {kind_encoding}")
        kind_encoding_keys.add(kind_encoding)
        expected_layout = VALID_KIND_ENCODING_LAYOUTS.get(kind_encoding)
        if expected_layout is None or row["payload_layout"] != expected_layout:
            raise GateError(f"{item_path} has an invalid kind/encoding/layout tuple")
        row_chunks = strict_int(row["chunks"], f"{item_path}.chunks", minimum=1)
        row_points = strict_int(row["points"], f"{item_path}.points", minimum=1)
        if row_points < row_chunks:
            raise GateError(f"{item_path} has fewer points than chunks")
        byte_fields = {
            key: strict_int(row[key], f"{item_path}.{key}", minimum=0)
            for key in (
                "indexed_bytes",
                "common_header_bytes",
                "scalar_lane_bytes",
                "payload_bytes",
                "timestamp_base_bytes",
                "timestamp_delta_bytes",
                "value_bytes",
            )
        }
        if byte_fields["common_header_bytes"] != row_chunks * 40:
            raise GateError(f"{item_path} common header bytes do not reconcile")
        if byte_fields["indexed_bytes"] != (
            byte_fields["common_header_bytes"]
            + byte_fields["scalar_lane_bytes"]
            + byte_fields["payload_bytes"]
        ):
            raise GateError(f"{item_path} indexed bytes do not reconcile")
        if byte_fields["payload_bytes"] != (
            byte_fields["timestamp_base_bytes"]
            + byte_fields["timestamp_delta_bytes"]
            + byte_fields["value_bytes"]
        ):
            raise GateError(f"{item_path} payload bytes do not reconcile")
        _validate_inventory_histogram(
            row["point_count_histogram"],
            f"{item_path}.point_count_histogram",
            row_chunks,
        )
        _validate_inventory_histogram(
            row["cadence_ms_histogram"],
            f"{item_path}.cadence_ms_histogram",
            row_points - row_chunks,
        )
        total_chunks += row_chunks
        total_points += row_points
        total_indexed_bytes += byte_fields["indexed_bytes"]
        total_native_timestamp_bytes += (
            byte_fields["timestamp_base_bytes"] + byte_fields["timestamp_delta_bytes"]
        )
        inventory_chunks_by_kind[row["kind"]] += row_chunks
        if row["kind"] == "float":
            float_chunks += row_chunks
            float_points += row_points
            float_indexed += byte_fields["indexed_bytes"]
            float_payload += byte_fields["payload_bytes"]
            float_encodings.add(row["encoding"])
    if (total_chunks, total_points) != (chunks, samples):
        raise GateError(f"{path} chunk/point totals differ from the verifier totals")
    if total_indexed_bytes != logical_chunk_bytes:
        raise GateError(f"{path} indexed bytes differ from logical_chunk_bytes")
    if [inventory_chunks_by_kind[kind] for kind in KIND_ORDER] != chunks_by_kind:
        raise GateError(f"{path} kind counts differ from chunks_by_kind")
    if float_encodings != EXPECTED_FLOAT_ENCODINGS:
        raise GateError(
            f"{path} Float encodings differ from the frozen Gorilla contract: "
            f"expected {sorted(EXPECTED_FLOAT_ENCODINGS)}, "
            f"found {sorted(float_encodings)}"
        )

    float_evidence = require_exact_keys(
        inventory["raw_f64_vs_gorilla"],
        FLOAT_EVIDENCE_FIELDS,
        f"{path}.raw_f64_vs_gorilla",
    )
    float_path = f"{path}.raw_f64_vs_gorilla"
    if not isinstance(float_evidence["tie_rule"], str) or "RAW_F64" not in float_evidence[
        "tie_rule"
    ]:
        raise GateError(f"{float_path}.tie_rule is not canonical")
    float_integer_fields = FLOAT_EVIDENCE_FIELDS - {
        "tie_rule",
        "raw_f64_wins",
        "gorilla_wins",
        "ties",
        "adaptive_raw_f64_selections",
        "adaptive_gorilla_selections",
        "xor_significant_bits_histogram",
    }
    float_integers = {
        key: strict_int(float_evidence[key], f"{float_path}.{key}", minimum=0)
        for key in float_integer_fields
    }
    winners = {
        key: _validate_inventory_winner(float_evidence[key], f"{float_path}.{key}")
        for key in (
            "raw_f64_wins",
            "gorilla_wins",
            "ties",
            "adaptive_raw_f64_selections",
            "adaptive_gorilla_selections",
        )
    }
    if (
        float_integers["chunks"],
        float_integers["points"],
        float_integers["existing_indexed_bytes"],
        float_integers["existing_payload_bytes"],
    ) != (float_chunks, float_points, float_indexed, float_payload):
        raise GateError(f"{float_path} totals do not reconcile with inventory rows")
    expected_header_bytes = 40 * float_chunks
    for prefix in (
        "existing",
        "raw_f64_candidate",
        "gorilla_candidate",
        "adaptive_min",
    ):
        if (
            float_integers[f"{prefix}_indexed_bytes"]
            - float_integers[f"{prefix}_payload_bytes"]
            != expected_header_bytes
        ):
            raise GateError(f"{float_path}.{prefix} bytes do not reconcile with headers")
    win_chunks = sum(
        winners[key]["chunks"] for key in ("raw_f64_wins", "gorilla_wins", "ties")
    )
    win_points = sum(
        winners[key]["points"] for key in ("raw_f64_wins", "gorilla_wins", "ties")
    )
    if (win_chunks, win_points) != (float_chunks, float_points):
        raise GateError(f"{float_path} winner totals do not reconcile")
    selected_chunks = sum(
        winners[key]["chunks"]
        for key in ("adaptive_raw_f64_selections", "adaptive_gorilla_selections")
    )
    selected_points = sum(
        winners[key]["points"]
        for key in ("adaptive_raw_f64_selections", "adaptive_gorilla_selections")
    )
    if (selected_chunks, selected_points) != (float_chunks, float_points):
        raise GateError(f"{float_path} adaptive selection totals do not reconcile")
    raw_wins = winners["raw_f64_wins"]
    gorilla_wins = winners["gorilla_wins"]
    ties = winners["ties"]
    if winners["adaptive_raw_f64_selections"] != {
        "chunks": raw_wins["chunks"] + ties["chunks"],
        "points": raw_wins["points"] + ties["points"],
    } or winners["adaptive_gorilla_selections"] != gorilla_wins:
        raise GateError(f"{float_path} adaptive selections violate the RawF64 tie rule")
    if float_integers["adaptive_min_indexed_bytes"] > min(
        float_integers["raw_f64_candidate_indexed_bytes"],
        float_integers["gorilla_candidate_indexed_bytes"],
    ) or float_integers["adaptive_min_payload_bytes"] > min(
        float_integers["raw_f64_candidate_payload_bytes"],
        float_integers["gorilla_candidate_payload_bytes"],
    ):
        raise GateError(f"{float_path} adaptive bytes exceed an aggregate candidate")
    xor_transitions = sum(
        float_integers[key]
        for key in ("repeated_xor_points", "reused_window_points", "new_window_points")
    )
    if xor_transitions != float_points - float_chunks:
        raise GateError(f"{float_path} XOR transition counts do not reconcile")
    _validate_inventory_histogram(
        float_evidence["xor_significant_bits_histogram"],
        f"{float_path}.xor_significant_bits_histogram",
        float_integers["reused_window_points"] + float_integers["new_window_points"],
    )
    classification_points = sum(
        float_integers[key]
        for key in (
            "positive_zero_points",
            "negative_zero_points",
            "finite_nonzero_points",
            "positive_infinity_points",
            "negative_infinity_points",
            "ordinary_nan_points",
            "stale_nan_points",
        )
    )
    if classification_points != float_points:
        raise GateError(f"{float_path} IEEE classifications do not reconcile")

    timestamp_path = f"{path}.timestamp_candidates"
    timestamp = require_exact_keys(
        inventory["timestamp_candidates"],
        {
            "scope",
            "tie_rule",
            "selector_bytes_included",
            "all_blocks",
            "by_shape",
            "by_kind_encoding",
        },
        timestamp_path,
    )
    if timestamp["selector_bytes_included"] is not False:
        raise GateError(f"{timestamp_path} unexpectedly includes selector bytes")
    for key in ("scope", "tie_rule"):
        if not isinstance(timestamp[key], str) or not timestamp[key]:
            raise GateError(f"{timestamp_path}.{key} must be a nonempty string")
    all_blocks = _validate_timestamp_evidence(
        timestamp["all_blocks"], f"{timestamp_path}.all_blocks"
    )
    if (all_blocks["chunks"], all_blocks["points"]) != (chunks, samples):
        raise GateError(f"{timestamp_path}.all_blocks totals do not reconcile")
    if (
        all_blocks["current_offset_uleb"]["bytes"]
        != total_native_timestamp_bytes
    ):
        raise GateError(f"{timestamp_path} current bytes differ from native timestamp bytes")
    by_shape = timestamp["by_shape"]
    if not isinstance(by_shape, list) or not by_shape:
        raise GateError(f"{timestamp_path}.by_shape must be a nonempty list")
    observed_shapes: set[str] = set()
    shape_evidence = []
    for index, item in enumerate(by_shape):
        item_path = f"{timestamp_path}.by_shape[{index}]"
        row = require_exact_keys(item, {"shape", "evidence"}, item_path)
        if row["shape"] not in TIMESTAMP_SHAPES or row["shape"] in observed_shapes:
            raise GateError(f"{item_path}.shape is unknown or duplicated")
        observed_shapes.add(row["shape"])
        shape_evidence.append(
            _validate_timestamp_evidence(row["evidence"], f"{item_path}.evidence")
        )
    if (
        sum(row["chunks"] for row in shape_evidence),
        sum(row["points"] for row in shape_evidence),
    ) != (chunks, samples):
        raise GateError(f"{timestamp_path}.by_shape totals do not reconcile")
    _reconcile_timestamp_breakdown(
        shape_evidence, all_blocks, f"{timestamp_path}.by_shape"
    )
    by_kind_encoding = timestamp["by_kind_encoding"]
    if not isinstance(by_kind_encoding, list) or not by_kind_encoding:
        raise GateError(f"{timestamp_path}.by_kind_encoding must be a nonempty list")
    timestamp_keys: set[tuple[str, str]] = set()
    kind_evidence = []
    for index, item in enumerate(by_kind_encoding):
        item_path = f"{timestamp_path}.by_kind_encoding[{index}]"
        row = require_exact_keys(item, {"kind", "encoding", "evidence"}, item_path)
        key = (row["kind"], row["encoding"])
        if (
            not all(isinstance(part, str) and part for part in key)
            or key in timestamp_keys
        ):
            raise GateError(f"{item_path} has an empty or duplicate key")
        timestamp_keys.add(key)
        kind_evidence.append(
            _validate_timestamp_evidence(row["evidence"], f"{item_path}.evidence")
        )
    if timestamp_keys != kind_encoding_keys:
        raise GateError(f"{timestamp_path} keys differ from inventory rows")
    if (
        sum(row["chunks"] for row in kind_evidence),
        sum(row["points"] for row in kind_evidence),
    ) != (chunks, samples):
        raise GateError(f"{timestamp_path}.by_kind_encoding totals do not reconcile")
    _reconcile_timestamp_breakdown(
        kind_evidence, all_blocks, f"{timestamp_path}.by_kind_encoding"
    )
    return inventory


def validate_storage_report(value: Any, expected_samples: int) -> dict[str, Any]:
    storage = require_exact_keys(value, STORAGE_REPORT_FIELDS, "$.storage")
    if storage["schema_version"] != 8:
        raise GateError("storage verification schema_version must be 8")
    if storage["footer_validation_enabled"] is not True:
        raise GateError("storage verification must enable footer validation")
    if storage["series_sample_per_segment"] is not None:
        raise GateError("storage verification must decode every corpus series")

    integers = {
        field: strict_int(storage[field], f"$.storage.{field}", minimum=0)
        for field in STORAGE_INTEGER_FIELDS
    }
    for field in (
        "segments",
        "corpus_series",
        "series",
        "chunks",
        "samples",
        "logical_chunk_bytes",
        "elapsed_ns",
        "metadata_read_calls",
        "metadata_read_bytes",
        "metadata_peak_retained_bytes",
        "metadata_peak_in_flight_bytes",
        "metadata_peak_open_files",
    ):
        if integers[field] == 0:
            raise GateError(f"storage verification {field} must be positive")
    if integers["series"] != integers["corpus_series"]:
        raise GateError("storage verification did not decode every corpus series")
    if integers["samples"] != expected_samples:
        raise GateError("storage sample count differs from exact replay correctness")

    chunks_by_kind = storage["chunks_by_kind"]
    if not isinstance(chunks_by_kind, list) or len(chunks_by_kind) != 5:
        raise GateError("storage chunks_by_kind must contain exactly five lanes")
    parsed_chunks_by_kind = [
        strict_int(value, f"$.storage.chunks_by_kind[{index}]", minimum=0)
        for index, value in enumerate(chunks_by_kind)
    ]
    if sum(parsed_chunks_by_kind) != integers["chunks"]:
        raise GateError("storage chunk-kind counts do not sum to chunks")

    fingerprints = {}
    for field in (
        "verified_selection_fingerprint",
        "decoded_semantic_fingerprint",
    ):
        fingerprint = storage[field]
        if not isinstance(fingerprint, str) or re.fullmatch(
            r"[0-9a-f]{64}", fingerprint
        ) is None:
            raise GateError(f"storage verification {field} is invalid")
        fingerprints[field] = fingerprint

    chunk_inventory = validate_chunk_inventory(
        storage["chunk_inventory"],
        chunks=integers["chunks"],
        samples=integers["samples"],
        logical_chunk_bytes=integers["logical_chunk_bytes"],
        chunks_by_kind=parsed_chunks_by_kind,
        path="$.storage.chunk_inventory",
    )

    exact = require_exact_keys(
        storage["exact_postings"], EXACT_POSTINGS_FIELDS, "$.storage.exact_postings"
    )
    exact_fingerprint = exact["logical_fingerprint"]
    if not isinstance(exact_fingerprint, str) or re.fullmatch(
        r"[0-9a-f]{64}", exact_fingerprint
    ) is None:
        raise GateError("storage exact-postings fingerprint is invalid")
    exact_counts = {
        field: strict_int(exact[field], f"$.storage.exact_postings.{field}", minimum=1)
        for field in ("lists", "decoded_refs", "encoded_bytes")
    }
    return {
        **fingerprints,
        "segments": integers["segments"],
        "corpus_series": integers["corpus_series"],
        "series": integers["series"],
        "chunks": integers["chunks"],
        "chunks_by_kind": parsed_chunks_by_kind,
        "samples": integers["samples"],
        "logical_chunk_bytes": integers["logical_chunk_bytes"],
        "chunk_inventory": chunk_inventory,
        "metadata_read_calls": integers["metadata_read_calls"],
        "metadata_read_bytes": integers["metadata_read_bytes"],
        "exact_postings": {
            "logical_fingerprint": exact_fingerprint,
            **exact_counts,
        },
    }


def validate_readback_report(text: str, plan: dict[str, Any]) -> dict[str, Any]:
    verification = phase1._two_column_values(text, "Readback Verification")
    diagnostics = phase1._two_column_values(text, "Query Diagnostics")
    expected = phase1._required_integer(diagnostics, "Expected Readback Queries")
    executed = phase1._required_integer(diagnostics, "Executed Readback Queries")
    skipped = phase1._required_integer(diagnostics, "Skipped Readback Queries")
    isolation = phase1._required_integer(diagnostics, "Isolation Check Skips")
    checked = phase1._required_integer(verification, "Checked Queries")
    mismatches = phase1._required_integer(verification, "Mismatches")
    expected_queries = plan["workload"]["expected_readback_queries"]
    if expected != expected_queries or executed != expected or checked != executed:
        raise GateError("readback expected/executed/checked coverage is incomplete")
    if skipped != 0 or isolation != 0 or mismatches != 0:
        raise GateError("readback verification contains a skip or mismatch")

    rows = phase1._markdown_rows(phase1._section(text, "PromQL Readbacks"))
    if not rows or rows[0] != PROMQL_READBACK_HEADER:
        raise GateError("PromQL readback table has no exact header")
    promql_rows = rows[1:]
    if any(len(row) != len(PROMQL_READBACK_HEADER) for row in promql_rows):
        raise GateError("PromQL readback table contains a malformed row")
    if len(promql_rows) != plan["workload"]["expected_promql_rows"]:
        raise GateError("PromQL readback row count differs from the frozen 250k contract")
    return {
        "readback_expected": expected,
        "readback_executed": executed,
        "readback_skipped": skipped,
        "readback_isolation_skips": isolation,
        "readback_mismatches": mismatches,
        "promql_rows": len(promql_rows),
        "promql_rows_fingerprint_sha256": hashlib.sha256(
            json.dumps(
                promql_rows, separators=(",", ":"), ensure_ascii=False
            ).encode()
        ).hexdigest(),
    }


def raw_validation_evidence(
    storage_path: Path,
    readbacks_path: Path,
    correctness_path: Path,
    corpus_path: Path,
    plan: dict[str, Any],
) -> dict[str, Any]:
    correctness = validate_replay_correctness(
        load_json(correctness_path), plan["workload"]["stop_after_messages"]
    )
    storage = validate_storage_report(
        load_json(storage_path), correctness["general"]["Recorded Samples"]
    )
    readbacks = validate_readback_report(
        readbacks_path.read_text(encoding="utf-8"), plan
    )
    corpus = validate_corpus_summary(load_json(corpus_path), "$.corpus")
    return {
        "storage_report_sha256": sha256_file(storage_path),
        "readbacks_report_sha256": sha256_file(readbacks_path),
        "correctness_sha256": sha256_file(correctness_path),
        "corpus_summary_sha256": sha256_file(corpus_path),
        "corpus": corpus,
        "storage_selection_fingerprint": storage[
            "verified_selection_fingerprint"
        ],
        "storage_decoded_semantic_fingerprint": storage[
            "decoded_semantic_fingerprint"
        ],
        "postings_fingerprint": storage["exact_postings"]["logical_fingerprint"],
        "segments": storage["segments"],
        "corpus_series": storage["corpus_series"],
        "chunks": storage["chunks"],
        "chunks_by_kind": storage["chunks_by_kind"],
        "logical_chunk_bytes": storage["logical_chunk_bytes"],
        "chunk_inventory": storage["chunk_inventory"],
        "metadata_read_calls": storage["metadata_read_calls"],
        "metadata_read_bytes": storage["metadata_read_bytes"],
        "exact_postings": storage["exact_postings"],
        **readbacks,
        "recorded_samples": correctness["general"]["Recorded Samples"],
    }


def check_storage_completeness(
    storage_path: Path,
    correctness_path: Path,
    plan_path: Path,
    phase1_expectations: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    correctness = validate_replay_correctness(
        load_json(correctness_path), plan["workload"]["stop_after_messages"]
    )
    expected_samples = correctness["general"]["Recorded Samples"]
    storage = validate_storage_report(load_json(storage_path), expected_samples)
    return {
        "complete": True,
        "recorded_samples": expected_samples,
        "storage_samples": storage["samples"],
        "decoded_semantic_fingerprint": storage[
            "decoded_semantic_fingerprint"
        ],
    }


def create_calibration(
    storage_path: Path,
    readbacks_path: Path,
    correctness_path: Path,
    corpus_path: Path,
    build_provenance_path: Path,
    plan_path: Path,
    phase1_expectations: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    build = validate_build_provenance(load_json(build_provenance_path))
    evidence = raw_validation_evidence(
        storage_path, readbacks_path, correctness_path, corpus_path, plan
    )
    return {
        "schema": CALIBRATION_SCHEMA,
        "complete": True,
        "measurement_eligible": False,
        "source_allocator": "system",
        "source_workload": plan["calibration_contract"]["source_workload"],
        "workload_messages": plan["workload"]["stop_after_messages"],
        "fingerprint_derived_from_raw_250k_report": True,
        "four_million_fingerprint_reused": False,
        "plan_sha256": sha256_file(plan_path),
        "build_provenance_sha256": sha256_file(build_provenance_path),
        "binary_sha256": {
            role: build["binary_sha256"][role]
            for role in ("system", "query", "storage_verify")
        },
        **evidence,
    }


def validate_calibration(
    value: Any, recomputed: dict[str, Any], plan: dict[str, Any]
) -> dict[str, Any]:
    calibration = require_exact_keys(value, set(recomputed), "$.calibration")
    if calibration != recomputed:
        differing = sorted(
            key for key in calibration if calibration[key] != recomputed[key]
        )
        raise GateError(
            "saved 250k calibration differs from raw calibration inputs at "
            f"{differing!r}"
        )
    if (
        calibration["schema"] != CALIBRATION_SCHEMA
        or calibration["complete"] is not True
        or calibration["measurement_eligible"] is not False
        or calibration["source_allocator"] != "system"
        or calibration["workload_messages"]
        != plan["calibration_contract"]["stop_after_messages"]
        or calibration["fingerprint_derived_from_raw_250k_report"] is not True
        or calibration["four_million_fingerprint_reused"] is not False
    ):
        raise GateError("saved calibration violates the pre-run 250k contract")
    return calibration


def gate_validation(
    storage_path: Path,
    readbacks_path: Path,
    correctness_path: Path,
    corpus_path: Path,
    calibration_path: Path,
    calibration_storage_path: Path,
    calibration_readbacks_path: Path,
    calibration_correctness_path: Path,
    calibration_corpus_path: Path,
    build_provenance_path: Path,
    plan_path: Path,
    phase1_expectations: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    recomputed_calibration = create_calibration(
        calibration_storage_path,
        calibration_readbacks_path,
        calibration_correctness_path,
        calibration_corpus_path,
        build_provenance_path,
        plan_path,
        phase1_expectations,
    )
    calibration = validate_calibration(
        load_json(calibration_path), recomputed_calibration, plan
    )
    evidence = raw_validation_evidence(
        storage_path, readbacks_path, correctness_path, corpus_path, plan
    )
    for field in (
        "correctness_sha256",
        "corpus_summary_sha256",
        "corpus",
        "storage_selection_fingerprint",
        "storage_decoded_semantic_fingerprint",
        "postings_fingerprint",
        "segments",
        "corpus_series",
        "chunks",
        "chunks_by_kind",
        "logical_chunk_bytes",
        "chunk_inventory",
        "exact_postings",
        "readback_expected",
        "readback_executed",
        "readback_skipped",
        "readback_isolation_skips",
        "readback_mismatches",
        "promql_rows",
        "promql_rows_fingerprint_sha256",
        "recorded_samples",
    ):
        if evidence[field] != calibration[field]:
            raise GateError(
                f"canonical validation {field} differs from pre-run 250k calibration"
            )
    return {
        "schema": VALIDATION_SCHEMA,
        "complete": True,
        "calibration_sha256": sha256_file(calibration_path),
        "build_provenance_sha256": sha256_file(build_provenance_path),
        **evidence,
    }


def record_profile_evidence(
    profile_kind: str,
    policy_name: str,
    binary_path: Path,
    screen_result_path: Path,
    screen_artifact_manifest_path: Path,
    system_binary_path: Path,
    jemalloc_binary_path: Path,
    query_binary_path: Path,
    storage_verify_binary_path: Path,
    profile_data_path: Path,
    profiler_log_path: Path,
    analysis_path: Path,
    lost_events_path: Path,
    profile_manifest_path: Path,
    reference_manifest_path: Path,
    profile_correctness_path: Path,
    reference_correctness_path: Path,
    profile_corpus_path: Path,
    reference_corpus_path: Path,
    storage_path: Path,
    readbacks_path: Path,
    calibration_path: Path,
    calibration_storage_path: Path,
    calibration_readbacks_path: Path,
    calibration_correctness_path: Path,
    calibration_corpus_path: Path,
    final_decision_path: Path,
    complete_marker_path: Path,
    build_provenance_path: Path,
    selected_runtime_log_path: Path | None,
    selected_preflight_path: Path | None,
    plan_path: Path,
    phase1_expectations: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    build = validate_build_provenance(load_json(build_provenance_path))
    completed_screen = validate_completed_screen_artifacts(
        screen_result_path,
        screen_artifact_manifest_path,
        complete_marker_path,
        final_decision_path,
        calibration_path,
        build_provenance_path,
        system_binary_path,
        jemalloc_binary_path,
        query_binary_path,
        storage_verify_binary_path,
    )
    canonical_reference_paths = {
        "manifest": screen_result_path / "runs/run-01-S/segments.sha256",
        "correctness": screen_result_path / "runs/run-01-S/replay-correctness.json",
        "corpus": screen_result_path / "runs/run-01-S/corpus-summary.json",
    }
    supplied_reference_paths = {
        "manifest": reference_manifest_path,
        "correctness": reference_correctness_path,
        "corpus": reference_corpus_path,
    }
    for name, path in supplied_reference_paths.items():
        if path.resolve(strict=True) != canonical_reference_paths[name].resolve(strict=True):
            raise GateError(f"profile {name} reference is not canonical run-01-S evidence")
    if profile_kind not in {"heaptrack", "perf-record"}:
        raise GateError(f"unsupported profile kind: {profile_kind!r}")
    if policy_name not in POLICY_ORDER:
        raise GateError(f"unsupported profile policy: {policy_name!r}")

    final = load_json(final_decision_path)
    if (
        not isinstance(final, dict)
        or final.get("schema") != FINAL_DECISION_SCHEMA
        or final.get("screen_complete") is not True
        or final.get("canonical_validation_complete") is not True
        or final.get("production_promotion_authorized") is not False
    ):
        raise GateError("profile input is not a complete non-promotional screen")
    if final.get("build_provenance_sha256") != sha256_file(build_provenance_path):
        raise GateError("profile screen decision is not bound to build provenance")
    if final.get("calibration_sha256") != sha256_file(calibration_path):
        raise GateError("profile screen decision is not bound to the 250k calibration")
    if final.get("binary_sha256_by_role") != {
        role: build["binary_sha256"][role] for role in ("system", "jemalloc")
    }:
        raise GateError("profile screen decision has changed comparator binaries")
    if profile_kind == "heaptrack":
        if policy_name != "S":
            raise GateError(
                "heaptrack allocation-stack authority requires the system allocator"
            )
        expected_role = "system"
    else:
        selected = final.get("selected_full_gate_policy")
        if policy_name != "S" and policy_name != selected:
            raise GateError(
                "perf-record policy must be system or the selected screen policy"
            )
        expected_role = "system" if policy_name == "S" else "jemalloc"
    expected_binary_path = (
        system_binary_path if expected_role == "system" else jemalloc_binary_path
    )
    if binary_path.resolve(strict=True) != expected_binary_path.resolve(strict=True):
        raise GateError("profile binary is not the canonical preserved screen executable")
    binary_hash = executable_sha256(binary_path)
    if binary_hash != build["binary_sha256"][expected_role]:
        raise GateError("profile binary differs from the frozen controlled build")

    selected_runtime_sha256 = None
    selected_preflight_sha256 = None
    if profile_kind == "perf-record" and policy_name != "S":
        if selected_runtime_log_path is None or selected_preflight_path is None:
            raise GateError(
                "selected-policy perf profile requires preflight and runtime-policy evidence"
            )
        gate_profile_runtime_log(
            selected_runtime_log_path,
            selected_preflight_path,
            plan_path,
            phase1_expectations,
            policy_name,
        )
        selected_runtime_sha256 = sha256_file(selected_runtime_log_path)
        selected_preflight_sha256 = sha256_file(selected_preflight_path)
    elif selected_runtime_log_path is not None or selected_preflight_path is not None:
        raise GateError("system/Heaptrack profile must not invent selected-policy evidence")

    profile_correctness = validate_replay_correctness(
        load_json(profile_correctness_path), plan["workload"]["stop_after_messages"]
    )
    reference_correctness = validate_replay_correctness(
        load_json(reference_correctness_path), plan["workload"]["stop_after_messages"]
    )
    if profile_correctness != reference_correctness or sha256_file(
        profile_correctness_path
    ) != sha256_file(reference_correctness_path):
        raise GateError("profile replay correctness differs from the measured reference")
    profile_corpus = validate_corpus_summary(
        load_json(profile_corpus_path), "$.profile_corpus"
    )
    reference_corpus = validate_corpus_summary(
        load_json(reference_corpus_path), "$.reference_corpus"
    )
    if profile_corpus != reference_corpus:
        raise GateError("profile corpus summary differs from the measured reference")
    if sha256_file(profile_manifest_path) != sha256_file(reference_manifest_path):
        raise GateError("profile corpus manifest bytes differ from the measured reference")
    if sha256_file(profile_manifest_path) != profile_corpus["manifest_sha256"]:
        raise GateError("profile corpus manifest bytes do not match its corpus summary")
    if sha256_file(reference_manifest_path) != reference_corpus["manifest_sha256"]:
        raise GateError("reference corpus manifest bytes do not match its corpus summary")

    validation = gate_validation(
        storage_path,
        readbacks_path,
        profile_correctness_path,
        profile_corpus_path,
        calibration_path,
        calibration_storage_path,
        calibration_readbacks_path,
        calibration_correctness_path,
        calibration_corpus_path,
        build_provenance_path,
        plan_path,
        phase1_expectations,
    )
    if not profile_data_path.is_file() or profile_data_path.is_symlink():
        raise GateError("profile data must be a non-symlink regular file")
    profile_data_bytes = strict_int(
        profile_data_path.stat().st_size, "$.profile_data_bytes", minimum=1
    )
    for path, label in (
        (profiler_log_path, "profiler log"),
        (analysis_path, "profile stack evidence"),
        (lost_events_path, "lost-event evidence"),
    ):
        if not path.is_file() or path.is_symlink():
            raise GateError(f"{label} must be a non-symlink regular file")
    profiler_log = profiler_log_path.read_text(encoding="utf-8", errors="strict")
    analysis = analysis_path.read_text(encoding="utf-8", errors="strict")
    lost_events = lost_events_path.read_text(encoding="utf-8", errors="strict")
    if lost_events.strip():
        raise GateError("profile contains lost samples/events")
    failure_pattern = re.compile(
        r"(?:^|\n)(?:ERROR:|Segmentation fault)|failed to initialize|"
        r"zero samples|no samples",
        re.IGNORECASE,
    )
    if failure_pattern.search(profiler_log) or failure_pattern.search(analysis):
        raise GateError("profiler log or analysis reports a failed/incomplete profile")
    if profile_kind == "heaptrack":
        allocation_match = re.search(
            r"(?m)^\s*allocations:\s*([0-9][0-9,]*)\s*$", profiler_log
        )
        if allocation_match is None:
            raise GateError("heaptrack log lacks its allocation count")
        sampled_events = int(allocation_match.group(1).replace(",", ""))
        if sampled_events == 0:
            raise GateError("heaptrack profile contains zero allocations")
        stack_evidence = parse_heaptrack_stack_evidence(analysis)
    else:
        stack_evidence = parse_perf_script_stack_evidence(analysis)
        sampled_events = stack_evidence["attributed_events"]

    return {
        "schema": PROFILE_EVIDENCE_SCHEMA,
        "complete": True,
        "measurement_eligible": False,
        "profile_kind": profile_kind,
        "policy": policy_name,
        "binary_role": expected_role,
        "binary_sha256": binary_hash,
        "profile_data_sha256": sha256_file(profile_data_path),
        "profile_data_bytes": profile_data_bytes,
        "profiler_log_sha256": sha256_file(profiler_log_path),
        "analysis_sha256": sha256_file(analysis_path),
        "lost_events_sha256": sha256_file(lost_events_path),
        "lost_events": 0,
        "sampled_events": sampled_events,
        "stack_evidence": stack_evidence,
        "correctness_sha256": sha256_file(profile_correctness_path),
        "corpus": profile_corpus,
        "corpus_manifest_sha256": sha256_file(profile_manifest_path),
        "canonical_validation_sha256": hashlib.sha256(
            json.dumps(validation, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "screen_final_decision_sha256": sha256_file(final_decision_path),
        "screen_artifact_manifest_sha256": completed_screen[
            "artifact_manifest_sha256"
        ],
        "screen_artifact_count": completed_screen["artifact_count"],
        "build_provenance_sha256": sha256_file(build_provenance_path),
        "source_seal_sha256": completed_screen["source_seal_sha256"],
        "source_archive_sha256": completed_screen["source_archive_sha256"],
        "extracted_source_seal_sha256": completed_screen[
            "extracted_source_seal_sha256"
        ],
        "extracted_source_manifest_sha256": completed_screen[
            "extracted_source_manifest_sha256"
        ],
        "all_screen_executable_hashes": completed_screen["binary_sha256"],
        "selected_policy_runtime_log_sha256": selected_runtime_sha256,
        "selected_policy_preflight_sha256": selected_preflight_sha256,
        "heap_allocation_stack_authority": (
            profile_kind == "heaptrack" and policy_name == "S"
        ),
        "candidate_specific_jemalloc_heap_profiling": "deferred",
        "a_b_timing_or_rss_evidence": False,
    }


def seal_screen(
    observation_paths: list[Path],
    screen_summary_path: Path,
    validation_path: Path,
    storage_path: Path,
    readbacks_path: Path,
    correctness_path: Path,
    corpus_path: Path,
    calibration_path: Path,
    calibration_storage_path: Path,
    calibration_readbacks_path: Path,
    calibration_correctness_path: Path,
    calibration_corpus_path: Path,
    capture_inputs_before_path: Path,
    capture_inputs_after_path: Path,
    build_provenance_path: Path,
    plan_path: Path,
    phase1_expectations: Path,
) -> dict[str, Any]:
    plan = validate_plan(plan_path, phase1_expectations)
    recomputed = compare_screen(observation_paths, plan_path, phase1_expectations)
    if load_json(screen_summary_path) != recomputed:
        raise GateError("saved screen summary differs from the ten gated observations")
    build_provenance = validate_build_provenance(load_json(build_provenance_path))
    if sha256_file(build_provenance_path) != recomputed["build_provenance_sha256"]:
        raise GateError("screen summary is not bound to the supplied controlled build")
    for role in ("system", "jemalloc"):
        if build_provenance["binary_sha256"][role] != recomputed[
            "binary_sha256_by_role"
        ][role]:
            raise GateError(f"sealed {role} binary differs from controlled build provenance")
    recomputed_validation = gate_validation(
        storage_path,
        readbacks_path,
        correctness_path,
        corpus_path,
        calibration_path,
        calibration_storage_path,
        calibration_readbacks_path,
        calibration_correctness_path,
        calibration_corpus_path,
        build_provenance_path,
        plan_path,
        phase1_expectations,
    )
    validation = load_json(validation_path)
    if validation != recomputed_validation:
        raise GateError(
            "saved canonical validation differs from recomputed raw storage/readback inputs"
        )
    if validation["correctness_sha256"] != recomputed["correctness_file_sha256"]:
        raise GateError("canonical validation correctness bytes differ from measured runs")
    if validation["corpus"] != recomputed["corpus"]:
        raise GateError("canonical validation corpus differs from measured runs")
    if validation["recorded_samples"] != recomputed["correctness"]["general"][
        "Recorded Samples"
    ]:
        raise GateError("canonical validation sample count differs from measured runs")
    capture_inventory = validate_capture_reinventory(
        capture_inputs_before_path, capture_inputs_after_path
    )
    return {
        "schema": FINAL_DECISION_SCHEMA,
        "screen_complete": True,
        "canonical_validation_complete": True,
        "run_count": recomputed["run_count"],
        "binary_sha256_by_role": recomputed["binary_sha256_by_role"],
        "eligible_screen_policies": recomputed["eligible_policies"],
        "selected_full_gate_policy": recomputed["selected_full_gate_policy"],
        "j0_comparator_only": True,
        "screen_build_jemalloc_stats_enabled": True,
        "stats_enabled_full_4m_gate_required": (
            recomputed["selected_full_gate_policy"] is not None
        ),
        "no_stats_revalidation_required_before_production": (
            recomputed["selected_full_gate_policy"] is not None
        ),
        "no_stats_production_build_validated": False,
        "production_promotion_authorized": False,
        "partial_runs_promotable": plan["completion_contract"][
            "partial_runs_promotable"
        ],
        "decision_scope": plan["completion_contract"]["decision_scope"],
        "screen_summary_sha256": sha256_file(screen_summary_path),
        "validation_sha256": sha256_file(validation_path),
        "calibration_sha256": sha256_file(calibration_path),
        "raw_storage_report_sha256": validation["storage_report_sha256"],
        "raw_readbacks_report_sha256": validation["readbacks_report_sha256"],
        "source_capture_inventory": capture_inventory,
        "build_provenance_sha256": sha256_file(build_provenance_path),
    }


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate-plan")
    validate.add_argument("--plan", type=Path, required=True)
    validate.add_argument("--phase1-expectations", type=Path, required=True)
    validate.add_argument("--output", type=Path)

    create_controls = commands.add_parser("create-control-seal")
    create_controls.add_argument("--input", action="append", type=Path, required=True)
    create_controls.add_argument("--output", type=Path, required=True)

    check_controls = commands.add_parser("check-control-seal")
    check_controls.add_argument("--seal", type=Path, required=True)
    check_controls.add_argument("--output", type=Path)

    rendered_config = commands.add_parser("check-rendered-config")
    rendered_config.add_argument("--record", type=Path, required=True)
    rendered_config.add_argument("--config", type=Path, required=True)
    rendered_config.add_argument("--capture", type=Path, required=True)
    rendered_config.add_argument("--segments-dir", type=Path, required=True)
    rendered_config.add_argument("--stop-after-messages", type=int, required=True)
    rendered_config.add_argument("--output", type=Path)

    source = commands.add_parser("source-seal")
    source.add_argument("--repo", type=Path, required=True)
    source.add_argument("--output", type=Path, required=True)

    source_check = commands.add_parser("check-source-seal")
    source_check.add_argument("--repo", type=Path, required=True)
    source_check.add_argument("--seal", type=Path, required=True)
    source_check.add_argument("--output", type=Path)

    extracted = commands.add_parser("extract-git-archive")
    extracted.add_argument("--repo", type=Path, required=True)
    extracted.add_argument("--archive", type=Path, required=True)
    extracted.add_argument("--source-root", type=Path, required=True)
    extracted.add_argument("--live-source-seal", type=Path, required=True)
    extracted.add_argument("--output", type=Path, required=True)

    extracted_check = commands.add_parser("check-extracted-source-seal")
    extracted_check.add_argument("--repo", type=Path, required=True)
    extracted_check.add_argument("--archive", type=Path, required=True)
    extracted_check.add_argument("--source-root", type=Path, required=True)
    extracted_check.add_argument("--live-source-seal", type=Path, required=True)
    extracted_check.add_argument("--seal", type=Path, required=True)
    extracted_check.add_argument("--build-provenance", type=Path)
    extracted_check.add_argument("--output", type=Path)

    executable_check = commands.add_parser("check-executable-set")
    executable_check.add_argument("--build-provenance", type=Path, required=True)
    executable_check.add_argument("--system-binary", type=Path, required=True)
    executable_check.add_argument("--jemalloc-binary", type=Path, required=True)
    executable_check.add_argument("--query-binary", type=Path, required=True)
    executable_check.add_argument("--storage-verify-binary", type=Path, required=True)
    executable_check.add_argument("--output", type=Path)

    build = commands.add_parser("record-build-provenance")
    build.add_argument("--repo", type=Path, required=True)
    build.add_argument("--target-dir", type=Path, required=True)
    build.add_argument("--source-seal", type=Path, required=True)
    build.add_argument("--build-source", type=Path, required=True)
    build.add_argument("--source-archive", type=Path, required=True)
    build.add_argument("--extracted-source-seal", type=Path, required=True)
    build.add_argument("--system-binary", type=Path, required=True)
    build.add_argument("--jemalloc-binary", type=Path, required=True)
    build.add_argument("--query-binary", type=Path, required=True)
    build.add_argument("--storage-verify-binary", type=Path, required=True)
    build.add_argument("--system-log", type=Path, required=True)
    build.add_argument("--jemalloc-log", type=Path, required=True)
    build.add_argument("--plan", type=Path, required=True)
    build.add_argument("--phase1-expectations", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)

    preflight = commands.add_parser("parse-preflight")
    preflight.add_argument("--stdout", type=Path, required=True)
    preflight.add_argument("--stderr", type=Path, required=True)
    preflight.add_argument("--source-audit-stderr", type=Path)
    preflight.add_argument("--binary", type=Path, required=True)
    preflight.add_argument("--plan", type=Path, required=True)
    preflight.add_argument("--phase1-expectations", type=Path, required=True)
    preflight.add_argument("--policy", required=True)
    preflight.add_argument("--output", type=Path)

    runtime_log = commands.add_parser("gate-runtime-log")
    runtime_log.add_argument("--log", type=Path, required=True)
    runtime_log.add_argument("--preflight", type=Path, required=True)
    runtime_log.add_argument("--plan", type=Path, required=True)
    runtime_log.add_argument("--phase1-expectations", type=Path, required=True)
    runtime_log.add_argument("--policy", required=True)
    runtime_log.add_argument("--output", type=Path)

    profile_runtime_log = commands.add_parser("gate-profile-runtime-log")
    profile_runtime_log.add_argument("--log", type=Path, required=True)
    profile_runtime_log.add_argument("--preflight", type=Path, required=True)
    profile_runtime_log.add_argument("--plan", type=Path, required=True)
    profile_runtime_log.add_argument("--phase1-expectations", type=Path, required=True)
    profile_runtime_log.add_argument("--policy", required=True)
    profile_runtime_log.add_argument("--output", type=Path)

    monitor = commands.add_parser("monitor-rss-release")
    monitor.add_argument("--pid", type=int, required=True)
    monitor.add_argument("--checkpoint", type=Path, required=True)
    monitor.add_argument("--output", type=Path, required=True)
    monitor.add_argument("--summary", type=Path, required=True)
    monitor.add_argument("--interval-ms", type=int, required=True)
    monitor.add_argument("--control", type=Path, required=True)
    monitor.add_argument("--rss-ready", type=Path, required=True)
    monitor.add_argument("--launch", type=Path, required=True)

    guardian = commands.add_parser("monitor-external-conflicts")
    guardian.add_argument("--pid", type=int, required=True)
    guardian.add_argument("--output", type=Path, required=True)
    guardian.add_argument("--interval-ms", type=int, required=True)
    guardian.add_argument("--filesystem", type=Path, required=True)
    guardian.add_argument("--minimum-free-bytes", type=int, required=True)
    guardian.add_argument("--control", type=Path, required=True)
    guardian.add_argument("--ready", type=Path, required=True)
    guardian.add_argument("--launch", type=Path, required=True)

    guardian_control = commands.add_parser("create-guardian-control")
    guardian_control.add_argument("--root-pid", type=int, required=True)
    guardian_control.add_argument("--guardian-pid", type=int, required=True)
    guardian_control.add_argument("--rss-monitor-pid", type=int)
    guardian_control.add_argument("--rss-ready", type=Path)
    guardian_control.add_argument("--interval-ms", type=int, required=True)
    guardian_control.add_argument("--ready", type=Path, required=True)
    guardian_control.add_argument("--launch", type=Path, required=True)
    guardian_control.add_argument("--output", type=Path, required=True)

    guardian_wait = commands.add_parser("wait-guardian-ready")
    guardian_wait.add_argument("--control", type=Path, required=True)
    guardian_wait.add_argument("--ready", type=Path, required=True)
    guardian_wait.add_argument("--launch", type=Path, required=True)
    guardian_wait.add_argument("--interval-ms", type=int, required=True)
    guardian_wait.add_argument("--timeout-ms", type=int, required=True)

    guardian_release = commands.add_parser("release-guardian-launch")
    guardian_release.add_argument("--control", type=Path, required=True)
    guardian_release.add_argument("--ready", type=Path, required=True)
    guardian_release.add_argument("--launch", type=Path, required=True)
    guardian_release.add_argument("--interval-ms", type=int, required=True)

    guardian_cleanup = commands.add_parser("cleanup-guardian-processes")
    guardian_cleanup.add_argument("--control", type=Path, required=True)
    guardian_cleanup.add_argument("--ready", type=Path, required=True)
    guardian_cleanup.add_argument("--launch", type=Path, required=True)
    guardian_cleanup.add_argument("--interval-ms", type=int, required=True)

    terminate = commands.add_parser("terminate-process-tree")
    terminate.add_argument("--root-pid", type=int, required=True)
    terminate.add_argument("--root-starttime-ticks", type=int, required=True)

    process_snapshot = commands.add_parser("check-process-snapshot")
    process_snapshot.add_argument("--snapshot", type=Path, required=True)
    process_snapshot.add_argument("--allow-pid", action="append", type=int, default=[])
    process_snapshot.add_argument("--output", type=Path)

    tree_seal = commands.add_parser("seal-evidence-tree")
    tree_seal.add_argument("--root", type=Path, required=True)
    tree_seal.add_argument(
        "--kind",
        choices=(
            "calibration",
            "run",
            "validation",
            "profile-heaptrack",
            "profile-perf-system",
            "profile-perf-selected",
        ),
        required=True,
    )
    tree_seal.add_argument("--output", type=Path, required=True)

    tree_seal_check = commands.add_parser("check-evidence-tree-seal")
    tree_seal_check.add_argument("--root", type=Path, required=True)
    tree_seal_check.add_argument(
        "--kind",
        choices=(
            "calibration",
            "run",
            "validation",
            "profile-heaptrack",
            "profile-perf-system",
            "profile-perf-selected",
        ),
        required=True,
    )
    tree_seal_check.add_argument("--seal", type=Path, required=True)
    tree_seal_check.add_argument("--output", type=Path)

    raw_revalidation = commands.add_parser("revalidate-screen-from-raw")
    raw_revalidation.add_argument("--result-root", type=Path, required=True)
    raw_revalidation.add_argument("--plan", type=Path, required=True)
    raw_revalidation.add_argument(
        "--phase1-expectations", type=Path, required=True
    )
    raw_revalidation.add_argument("--output", type=Path)

    final_inventory = commands.add_parser("create-final-artifact-inventory")
    final_inventory.add_argument("--result-root", type=Path, required=True)
    final_inventory.add_argument("--files", type=Path, required=True)
    final_inventory.add_argument("--directories", type=Path, required=True)
    final_inventory.add_argument("--manifest", type=Path, required=True)

    final_inventory_check = commands.add_parser("validate-final-artifacts")
    final_inventory_check.add_argument("--result-root", type=Path, required=True)
    final_inventory_check.add_argument(
        "--stage", choices=("precomplete", "complete"), required=True
    )
    final_inventory_check.add_argument("--output", type=Path)

    profile_revalidation = commands.add_parser("revalidate-profile-from-raw")
    profile_revalidation.add_argument("--result-root", type=Path, required=True)
    profile_revalidation.add_argument("--screen-result", type=Path, required=True)
    profile_revalidation.add_argument("--output", type=Path)

    profile_capacity_create = commands.add_parser("create-profile-capacity-control")
    profile_capacity_create.add_argument(
        "--profile-min-free-bytes", type=int, required=True
    )
    profile_capacity_create.add_argument("--output", type=Path, required=True)

    profile_capacity_check = commands.add_parser("check-profile-capacity-control")
    profile_capacity_check.add_argument("--control", type=Path, required=True)
    profile_capacity_check.add_argument(
        "--expected-profile-min-free-bytes", type=int, required=True
    )

    profile_inventory = commands.add_parser("create-profile-artifact-inventory")
    profile_inventory.add_argument("--result-root", type=Path, required=True)
    profile_inventory.add_argument("--files", type=Path, required=True)
    profile_inventory.add_argument("--directories", type=Path, required=True)
    profile_inventory.add_argument("--manifest", type=Path, required=True)

    profile_inventory_check = commands.add_parser("validate-profile-artifacts")
    profile_inventory_check.add_argument("--result-root", type=Path, required=True)
    profile_inventory_check.add_argument(
        "--stage", choices=("precomplete", "complete"), required=True
    )
    profile_inventory_check.add_argument("--output", type=Path)

    quiescence = commands.add_parser("sync-and-wait-writeback-quiescent")
    quiescence.add_argument("--corpus", type=Path, required=True)
    quiescence.add_argument("--samples", type=Path, required=True)
    quiescence.add_argument("--summary", type=Path, required=True)
    quiescence.add_argument("--maximum-kib", type=int, required=True)
    quiescence.add_argument("--consecutive", type=int, required=True)
    quiescence.add_argument("--interval-ms", type=int, required=True)
    quiescence.add_argument("--timeout-secs", type=int, required=True)

    checkpoint = commands.add_parser("parse-checkpoint")
    checkpoint.add_argument("--checkpoint", type=Path, required=True)
    checkpoint.add_argument("--rss", type=Path, required=True)
    checkpoint.add_argument("--plan", type=Path, required=True)
    checkpoint.add_argument("--phase1-expectations", type=Path, required=True)
    checkpoint.add_argument("--output", type=Path)

    telemetry = commands.add_parser("parse-allocator-telemetry")
    telemetry.add_argument("--telemetry", type=Path, required=True)
    telemetry.add_argument("--checkpoint", type=Path, required=True)
    telemetry.add_argument("--rss-samples", type=Path, required=True)
    telemetry.add_argument("--rss-summary", type=Path, required=True)
    telemetry.add_argument("--plan", type=Path, required=True)
    telemetry.add_argument("--phase1-expectations", type=Path, required=True)
    telemetry.add_argument("--policy", required=True)
    telemetry.add_argument("--output", type=Path)

    observation = commands.add_parser("make-observation")
    observation.add_argument("--run-index", type=int, required=True)
    observation.add_argument("--policy", required=True)
    observation.add_argument("--plan", type=Path, required=True)
    observation.add_argument("--phase1-expectations", type=Path, required=True)
    observation.add_argument("--build-provenance", type=Path, required=True)
    observation.add_argument("--preflight", type=Path, required=True)
    observation.add_argument("--binary", type=Path, required=True)
    observation.add_argument("--runtime-policy", type=Path, required=True)
    observation.add_argument("--allocator-telemetry", type=Path, required=True)
    observation.add_argument("--checkpoint", type=Path, required=True)
    observation.add_argument("--rss", type=Path, required=True)
    observation.add_argument("--time", type=Path, required=True)
    observation.add_argument("--perf", type=Path, required=True)
    observation.add_argument("--guardian", type=Path, required=True)
    observation.add_argument("--pre-quiescence", type=Path, required=True)
    observation.add_argument("--quiescence", type=Path, required=True)
    observation.add_argument("--correctness", type=Path, required=True)
    observation.add_argument("--corpus", type=Path, required=True)
    observation.add_argument("--output", type=Path, required=True)

    compare = commands.add_parser("compare-screen")
    compare.add_argument("--observation", action="append", type=Path, required=True)
    compare.add_argument("--plan", type=Path, required=True)
    compare.add_argument("--phase1-expectations", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)

    completeness = commands.add_parser("check-storage-completeness")
    completeness.add_argument("--storage", type=Path, required=True)
    completeness.add_argument("--correctness", type=Path, required=True)
    completeness.add_argument("--plan", type=Path, required=True)
    completeness.add_argument(
        "--phase1-expectations", type=Path, required=True
    )

    calibration = commands.add_parser("create-calibration")
    calibration.add_argument("--storage", type=Path, required=True)
    calibration.add_argument("--readbacks", type=Path, required=True)
    calibration.add_argument("--correctness", type=Path, required=True)
    calibration.add_argument("--corpus", type=Path, required=True)
    calibration.add_argument("--build-provenance", type=Path, required=True)
    calibration.add_argument("--plan", type=Path, required=True)
    calibration.add_argument("--phase1-expectations", type=Path, required=True)
    calibration.add_argument("--output", type=Path, required=True)

    validation = commands.add_parser("gate-validation")
    validation.add_argument("--storage", type=Path, required=True)
    validation.add_argument("--readbacks", type=Path, required=True)
    validation.add_argument("--correctness", type=Path, required=True)
    validation.add_argument("--corpus", type=Path, required=True)
    validation.add_argument("--calibration", type=Path, required=True)
    validation.add_argument("--calibration-storage", type=Path, required=True)
    validation.add_argument("--calibration-readbacks", type=Path, required=True)
    validation.add_argument("--calibration-correctness", type=Path, required=True)
    validation.add_argument("--calibration-corpus", type=Path, required=True)
    validation.add_argument("--build-provenance", type=Path, required=True)
    validation.add_argument("--plan", type=Path, required=True)
    validation.add_argument("--phase1-expectations", type=Path, required=True)
    validation.add_argument("--output", type=Path, required=True)

    seal = commands.add_parser("seal-screen")
    seal.add_argument("--observation", action="append", type=Path, required=True)
    seal.add_argument("--screen-summary", type=Path, required=True)
    seal.add_argument("--validation", type=Path, required=True)
    seal.add_argument("--storage", type=Path, required=True)
    seal.add_argument("--readbacks", type=Path, required=True)
    seal.add_argument("--correctness", type=Path, required=True)
    seal.add_argument("--corpus", type=Path, required=True)
    seal.add_argument("--calibration", type=Path, required=True)
    seal.add_argument("--calibration-storage", type=Path, required=True)
    seal.add_argument("--calibration-readbacks", type=Path, required=True)
    seal.add_argument("--calibration-correctness", type=Path, required=True)
    seal.add_argument("--calibration-corpus", type=Path, required=True)
    seal.add_argument("--capture-inputs-before", type=Path, required=True)
    seal.add_argument("--capture-inputs-after", type=Path, required=True)
    seal.add_argument("--build-provenance", type=Path, required=True)
    seal.add_argument("--plan", type=Path, required=True)
    seal.add_argument("--phase1-expectations", type=Path, required=True)
    seal.add_argument("--output", type=Path, required=True)

    profile = commands.add_parser("record-profile-evidence")
    profile.add_argument("--profile-kind", required=True)
    profile.add_argument("--policy", required=True)
    profile.add_argument("--binary", type=Path, required=True)
    profile.add_argument("--screen-result", type=Path, required=True)
    profile.add_argument("--screen-artifact-manifest", type=Path, required=True)
    profile.add_argument("--system-binary", type=Path, required=True)
    profile.add_argument("--jemalloc-binary", type=Path, required=True)
    profile.add_argument("--query-binary", type=Path, required=True)
    profile.add_argument("--storage-verify-binary", type=Path, required=True)
    profile.add_argument("--profile-data", type=Path, required=True)
    profile.add_argument("--profiler-log", type=Path, required=True)
    profile.add_argument("--analysis", type=Path, required=True)
    profile.add_argument("--lost-events", type=Path, required=True)
    profile.add_argument("--profile-manifest", type=Path, required=True)
    profile.add_argument("--reference-manifest", type=Path, required=True)
    profile.add_argument("--profile-correctness", type=Path, required=True)
    profile.add_argument("--reference-correctness", type=Path, required=True)
    profile.add_argument("--profile-corpus", type=Path, required=True)
    profile.add_argument("--reference-corpus", type=Path, required=True)
    profile.add_argument("--storage", type=Path, required=True)
    profile.add_argument("--readbacks", type=Path, required=True)
    profile.add_argument("--calibration", type=Path, required=True)
    profile.add_argument("--calibration-storage", type=Path, required=True)
    profile.add_argument("--calibration-readbacks", type=Path, required=True)
    profile.add_argument("--calibration-correctness", type=Path, required=True)
    profile.add_argument("--calibration-corpus", type=Path, required=True)
    profile.add_argument("--final-decision", type=Path, required=True)
    profile.add_argument("--complete-marker", type=Path, required=True)
    profile.add_argument("--build-provenance", type=Path, required=True)
    profile.add_argument("--selected-runtime-log", type=Path)
    profile.add_argument("--selected-preflight", type=Path)
    profile.add_argument("--plan", type=Path, required=True)
    profile.add_argument("--phase1-expectations", type=Path, required=True)
    profile.add_argument("--output", type=Path, required=True)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "validate-plan":
            result = validate_plan(args.plan, args.phase1_expectations)
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "create-control-seal":
            write_json_exclusive(args.output, control_seal(args.input))
        elif args.command == "check-control-seal":
            result = check_control_seal(args.seal)
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "check-rendered-config":
            result = check_rendered_config(
                args.record,
                args.config,
                args.capture,
                args.segments_dir,
                args.stop_after_messages,
            )
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "source-seal":
            write_json_exclusive(args.output, source_seal(args.repo))
        elif args.command == "check-source-seal":
            result = check_source_seal(args.repo, args.seal)
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "extract-git-archive":
            write_json_exclusive(
                args.output,
                extract_git_archive(
                    args.repo,
                    args.archive,
                    args.source_root,
                    args.live_source_seal,
                ),
            )
        elif args.command == "check-extracted-source-seal":
            result = check_extracted_source_seal(
                args.repo,
                args.source_root,
                args.archive,
                args.live_source_seal,
                args.seal,
                args.build_provenance,
            )
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "check-executable-set":
            result = validate_executable_set(
                args.build_provenance,
                args.system_binary,
                args.jemalloc_binary,
                args.query_binary,
                args.storage_verify_binary,
            )
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "record-build-provenance":
            write_json_exclusive(
                args.output,
                record_build_provenance(
                    args.repo,
                    args.target_dir,
                    args.source_seal,
                    args.build_source,
                    args.source_archive,
                    args.extracted_source_seal,
                    args.system_binary,
                    args.jemalloc_binary,
                    args.query_binary,
                    args.storage_verify_binary,
                    args.system_log,
                    args.jemalloc_log,
                    args.plan,
                    args.phase1_expectations,
                ),
            )
        elif args.command == "parse-preflight":
            result = parse_preflight(
                args.stdout,
                args.stderr,
                args.binary,
                args.plan,
                args.phase1_expectations,
                args.policy,
                args.source_audit_stderr,
            )
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "monitor-rss-release":
            print(
                json.dumps(
                    monitor_rss_release(
                        args.pid,
                        args.checkpoint,
                        args.output,
                        args.summary,
                        args.interval_ms,
                        args.control,
                        args.rss_ready,
                        args.launch,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "monitor-external-conflicts":
            print(
                json.dumps(
                    monitor_external_conflicts(
                        args.pid,
                        args.output,
                        args.interval_ms,
                        args.filesystem,
                        args.minimum_free_bytes,
                        args.control,
                        args.ready,
                        args.launch,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "create-guardian-control":
            print(
                json.dumps(
                    create_guardian_control(
                        args.output,
                        args.ready,
                        args.launch,
                        args.root_pid,
                        args.guardian_pid,
                        args.interval_ms,
                        args.rss_monitor_pid,
                        args.rss_ready,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "wait-guardian-ready":
            print(
                json.dumps(
                    wait_for_guardian_ready(
                        args.control,
                        args.ready,
                        args.launch,
                        args.interval_ms,
                        args.timeout_ms,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "release-guardian-launch":
            print(
                json.dumps(
                    release_guardian_launch(
                        args.control, args.ready, args.launch, args.interval_ms
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "cleanup-guardian-processes":
            print(
                json.dumps(
                    cleanup_guardian_processes(
                        args.control, args.ready, args.launch, args.interval_ms
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "terminate-process-tree":
            termination = terminate_process_tree(
                args.root_pid, args.root_starttime_ticks
            )
            print(json.dumps(termination, sort_keys=True))
            require_clean_termination(termination, "identity-bound process tree")
        elif args.command == "check-process-snapshot":
            result = validate_process_snapshot(args.snapshot, set(args.allow_pid))
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "seal-evidence-tree":
            create_immutable_tree_seal(args.root, args.output, args.kind)
        elif args.command == "check-evidence-tree-seal":
            result = validate_immutable_tree_seal(args.root, args.seal, args.kind)
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "revalidate-screen-from-raw":
            result = revalidate_screen_from_raw(
                args.result_root, args.plan, args.phase1_expectations
            )
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "create-final-artifact-inventory":
            print(
                json.dumps(
                    create_final_artifact_inventory(
                        args.result_root, args.files, args.directories, args.manifest
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "validate-final-artifacts":
            result = validate_final_artifact_inventory(args.result_root, args.stage)
            if args.output:
                write_json_exclusive(args.output, result)
                args.output.chmod(0o444)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "revalidate-profile-from-raw":
            result = revalidate_profile_from_raw(args.result_root, args.screen_result)
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "create-profile-capacity-control":
            print(
                json.dumps(
                    create_profile_capacity_control(
                        args.output, args.profile_min_free_bytes
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "check-profile-capacity-control":
            print(
                json.dumps(
                    validate_profile_capacity_control(
                        args.control, args.expected_profile_min_free_bytes
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "create-profile-artifact-inventory":
            print(
                json.dumps(
                    create_profile_artifact_inventory(
                        args.result_root, args.files, args.directories, args.manifest
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "validate-profile-artifacts":
            result = validate_profile_artifact_inventory(args.result_root, args.stage)
            if args.output:
                write_json_exclusive(args.output, result)
                args.output.chmod(0o444)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "sync-and-wait-writeback-quiescent":
            print(
                json.dumps(
                    sync_and_wait_writeback_quiescent(
                        args.corpus,
                        args.samples,
                        args.summary,
                        args.maximum_kib,
                        args.consecutive,
                        args.interval_ms,
                        args.timeout_secs,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "gate-runtime-log":
            result = gate_runtime_log(
                args.log,
                args.preflight,
                args.plan,
                args.phase1_expectations,
                args.policy,
            )
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "gate-profile-runtime-log":
            result = gate_profile_runtime_log(
                args.log,
                args.preflight,
                args.plan,
                args.phase1_expectations,
                args.policy,
            )
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "parse-checkpoint":
            result = parse_checkpoint(
                args.checkpoint,
                args.rss,
                args.plan,
                args.phase1_expectations,
            )
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "parse-allocator-telemetry":
            result = parse_allocator_telemetry(
                args.telemetry,
                args.checkpoint,
                args.rss_samples,
                args.rss_summary,
                args.plan,
                args.phase1_expectations,
                args.policy,
            )
            if args.output:
                write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True))
        elif args.command == "make-observation":
            result = make_observation(
                run_index=args.run_index,
                policy_name=args.policy,
                plan_path=args.plan,
                phase1_expectations=args.phase1_expectations,
                build_provenance_path=args.build_provenance,
                preflight_path=args.preflight,
                binary_path=args.binary,
                runtime_policy_path=args.runtime_policy,
                allocator_telemetry_path=args.allocator_telemetry,
                checkpoint_path=args.checkpoint,
                rss_path=args.rss,
                time_path=args.time,
                perf_path=args.perf,
                guardian_path=args.guardian,
                pre_quiescence_path=args.pre_quiescence,
                quiescence_path=args.quiescence,
                correctness_path=args.correctness,
                corpus_path=args.corpus,
            )
            write_json_exclusive(args.output, result)
        elif args.command == "compare-screen":
            write_json_exclusive(
                args.output,
                compare_screen(args.observation, args.plan, args.phase1_expectations),
            )
        elif args.command == "check-storage-completeness":
            print(
                json.dumps(
                    check_storage_completeness(
                        args.storage,
                        args.correctness,
                        args.plan,
                        args.phase1_expectations,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "create-calibration":
            write_json_exclusive(
                args.output,
                create_calibration(
                    args.storage,
                    args.readbacks,
                    args.correctness,
                    args.corpus,
                    args.build_provenance,
                    args.plan,
                    args.phase1_expectations,
                ),
            )
        elif args.command == "gate-validation":
            write_json_exclusive(
                args.output,
                gate_validation(
                    args.storage,
                    args.readbacks,
                    args.correctness,
                    args.corpus,
                    args.calibration,
                    args.calibration_storage,
                    args.calibration_readbacks,
                    args.calibration_correctness,
                    args.calibration_corpus,
                    args.build_provenance,
                    args.plan,
                    args.phase1_expectations,
                ),
            )
        elif args.command == "seal-screen":
            write_json_exclusive(
                args.output,
                seal_screen(
                    args.observation,
                    args.screen_summary,
                    args.validation,
                    args.storage,
                    args.readbacks,
                    args.correctness,
                    args.corpus,
                    args.calibration,
                    args.calibration_storage,
                    args.calibration_readbacks,
                    args.calibration_correctness,
                    args.calibration_corpus,
                    args.capture_inputs_before,
                    args.capture_inputs_after,
                    args.build_provenance,
                    args.plan,
                    args.phase1_expectations,
                ),
            )
        elif args.command == "record-profile-evidence":
            write_json_exclusive(
                args.output,
                record_profile_evidence(
                    args.profile_kind,
                    args.policy,
                    args.binary,
                    args.screen_result,
                    args.screen_artifact_manifest,
                    args.system_binary,
                    args.jemalloc_binary,
                    args.query_binary,
                    args.storage_verify_binary,
                    args.profile_data,
                    args.profiler_log,
                    args.analysis,
                    args.lost_events,
                    args.profile_manifest,
                    args.reference_manifest,
                    args.profile_correctness,
                    args.reference_correctness,
                    args.profile_corpus,
                    args.reference_corpus,
                    args.storage,
                    args.readbacks,
                    args.calibration,
                    args.calibration_storage,
                    args.calibration_readbacks,
                    args.calibration_correctness,
                    args.calibration_corpus,
                    args.final_decision,
                    args.complete_marker,
                    args.build_provenance,
                    args.selected_runtime_log,
                    args.selected_preflight,
                    args.plan,
                    args.phase1_expectations,
                ),
            )
        else:
            raise AssertionError(args.command)
    except (
        GateError,
        phase1.GateError,
        OSError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        print(f"Phase 5 allocator gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
