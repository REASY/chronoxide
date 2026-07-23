#!/usr/bin/env python3
"""Strict helpers and equivalence gates for the Phase 6 codec experiment."""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal
import hashlib
import io
import json
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ab_gate
import phase1_replay_gate as replay
import phase3_payload_coalescing_gate as phase3
import schema7_query_ab_gate as query_common
import schema8_query_ab_gate as query


PHASE6_QUERY_MANIFEST_SCHEMA = "chronoxide/phase6-codec-query-manifest/v1"
PHASE6_QUERY_NORMALIZED_SCHEMA = "chronoxide/phase6-codec-query-manifest-normalized/v1"
CODECS = ("raw", "gorilla")
REPLAY_COMPARISON_SCHEMA = "chronoxide/phase6-codec-replay-comparison/v1"
VERIFIER_COMPARISON_SCHEMA = "chronoxide/phase6-codec-verifier-comparison/v1"
QUERY_COMPARISON_SCHEMA = "chronoxide/phase6-codec-query-comparison/v1"
ARTIFACT_INVENTORY_SCHEMA = "chronoxide/phase6-codec-artifact-inventory/v1"
CAPTURE_INVENTORY_SCHEMA = "chronoxide/phase6-codec-capture-inventory/v1"
SOURCE_SEAL_SCHEMA = "chronoxide/phase6-source-seal/v1"
SOURCE_SNAPSHOT_SEAL_SCHEMA = "chronoxide/phase6-source-snapshot-seal/v1"
CARGO_CONFIG_ISOLATION_SCHEMA = "chronoxide/phase6-cargo-config-isolation/v1"
RUNTIME_IDENTITY_SCHEMA = "chronoxide/phase6-runtime-identity/v1"
ADMISSION_PLAN_SCHEMA = "chronoxide/phase6-final-admission-plan/v2"
RAW_LEAF_SEAL_SCHEMA = "chronoxide/phase6-raw-leaf-seal/v1"
FINAL_ADMISSION_SCHEMA = "chronoxide/phase6-final-admission/v2"
MEASUREMENT_PRECONDITIONS_SCHEMA = (
    "chronoxide/phase6-measurement-preconditions/v1"
)
INVOCATION_SCHEMA = "chronoxide/phase6-process-invocation/v1"
CAPACITY_CONTRACT_SCHEMA = "chronoxide/phase6-capacity-contract/v1"
CAPACITY_SNAPSHOT_SCHEMA = "chronoxide/phase6-capacity-snapshot/v1"
CAPACITY_MONITOR_SCHEMA = "chronoxide/phase6-capacity-monitor/v2"
GUARD_PRECHECK_SCHEMA = "chronoxide/phase6-conflict-precheck/v2"
GUARD_SAMPLE_SCHEMA = "chronoxide/phase6-conflict-guardian-samples/v2"
REPLAY_MONITOR_CONTROL_SCHEMA = "chronoxide/phase6-replay-monitor-control/v1"
RSS_MONITOR_SCHEMA = "chronoxide/phase6-rss-monitor/v2"
FIXED_GUARD_INTERVAL_MS = 100
GUARD_MAX_GAP_MULTIPLIER = 2
FIXED_CAPACITY_MONITOR_INTERVAL_MS = 100
CAPACITY_MONITOR_MAX_GAP_MULTIPLIER = 2
FIXED_BENCHMARK_REPEATS = 3
FIXED_RSS_INTERVAL_MS = 100
FIXED_QUERY_LABEL_ARENA_MAX_BYTES = 512 * 1024**2
FIXED_MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT = 0
FIXED_MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT = 0
FIXED_MAX_DIRTY_WRITEBACK_BYTES = 64 * 1024**2
PINNED_STOP_AFTER_MESSAGES = 4_000_000
PINNED_REPLAY_BLOCKS = 2
PINNED_QUERY_BLOCKS = 2
PINNED_EXPECTATIONS_SHA256 = (
    "b1a883950c1a303346261e3097195d6f3256d10dd814e2bbd3b49f98f5eedaa6"
)
COMMON_CHUNK_HEADER_BYTES = 40
RAW_F64_VALUE_BYTES = 8
CAPACITY_SAFETY_NUMERATOR = 11
CAPACITY_SAFETY_DENOMINATOR = 10
CAPACITY_OPERATIONAL_FLOOR_BYTES = 20 * 1024**3
CAPACITY_BUILD_SOURCE_RESULT_ALLOWANCE_BYTES = 10 * 1024**3
CAPACITY_LAYOUT_AUTHORITIES = {
    "chronoxide-core/src/storage/chunk/types.rs": (
        "261efca9baac0b1c98f2b0fc45d91cba698a8f44cf551fbc9e74a637f4685b38"
    ),
    "chronoxide-core/src/storage/chunk/writer.rs": (
        "d7f20509da3e4d45ab5e23aa198ae236f51c9185947cff7c205b127e4c640b11"
    ),
    "docs/superpowers/specs/storage.md": (
        "32e9e51ca4be71f1fc95e9464c3d9840767f4db3f6969fbd14a15b748b398d4c"
    ),
}
EXPECTED_VERIFIER_KEYS = {
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
EXPECTED_FLOAT_EVIDENCE_KEYS = {
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
FLOAT_EXISTING_FIELDS = {"existing_indexed_bytes", "existing_payload_bytes"}
ALLOWED_CROSS_CODEC_ARTIFACTS = {
    "chunks.bin",
    "ooo_chunks.bin",
    "series.bin",
    "chunk_index.bin",
    "footer.bin",
}
ALLOWED_QUERY_STATS_DIFFERENCES = {"bytes_read"}
TYPED_CONTROL_CATEGORIES = {
    "typed-scalar-projection-control",
    "native-histogram-full-control",
}
PERF_REQUIRED_EVENTS = (
    "task-clock",
    "cycles",
    "instructions",
    "branches",
    "branch-misses",
    "cache-references",
    "cache-misses",
    "page-faults",
    "context-switches",
    "cpu-migrations",
)
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
KIND_ORDER = ("float", "int64", "histogram", "exponential_histogram", "summary")
VALID_KIND_ENCODING_LAYOUTS = {
    ("float", "raw_f64"): "t0_interleaved_dt_value",
    ("float", "gorilla"): "t0_dt_then_values",
    ("int64", "raw_i64"): "t0_interleaved_dt_value",
    ("int64", "int_delta_zigzag"): "t0_dt_then_values",
    ("histogram", "schema_varlen"): "typed_scalar_lane_and_t0_dt_schema_varlen",
    ("exponential_histogram", "schema_varlen"): "typed_scalar_lane_and_t0_dt_schema_varlen",
    ("summary", "schema_varlen"): "typed_scalar_lane_and_t0_dt_schema_varlen",
}


class GateError(ValueError):
    pass


def _canonical_directory(path: Path, name: str) -> Path:
    if not path.is_absolute():
        raise GateError(f"{name} must be absolute: {path}")
    try:
        metadata = path.lstat()
    except OSError as error:
        raise GateError(f"cannot inspect {name} {path}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise GateError(f"{name} must be a non-symlink directory: {path}")
    return path.resolve(strict=True)


def _canonical_file(path: Path, name: str) -> Path:
    if not path.is_absolute():
        raise GateError(f"{name} must be absolute: {path}")
    _regular_file(path, name)
    return path.resolve(strict=True)


def _git(repo: Path, *arguments: str, binary: bool = False) -> str | bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            check=True,
            capture_output=True,
            text=not binary,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "")
        raise GateError(f"git {' '.join(arguments)} failed for {repo}: {detail}") from error
    return result.stdout


def _is_excluded_runtime_artifact(path: str) -> bool:
    return (
        (path.startswith("chronoxide-ingester/") and Path(path).name.startswith("ingestion_stats_") and path.endswith(".md"))
        or "/__pycache__/" in f"/{path}"
        or path.endswith(".pyc")
    )


def _is_ignored_build_input_candidate(path: str) -> bool:
    if _is_excluded_runtime_artifact(path) or path.startswith("target/"):
        return False
    candidate = Path(path)
    is_cargo_config = path == ".cargo/config" or path.endswith("/.cargo/config")
    return (
        is_cargo_config
        or candidate.name in {"Cargo.toml", "Cargo.lock", "build.rs"}
        or candidate.suffix
        in {
            ".c",
            ".cc",
            ".cfg",
            ".cpp",
            ".h",
            ".hpp",
            ".json",
            ".proto",
            ".rs",
            ".toml",
        }
    )


def source_seal(repo: Path) -> dict[str, Any]:
    repo = _canonical_directory(repo, "source root")
    root = Path(str(_git(repo, "rev-parse", "--show-toplevel")).strip())
    if root != repo:
        raise GateError(f"source root is not the Git worktree root: {repo}")
    dirty = str(_git(repo, "status", "--porcelain=v1", "--untracked-files=no")).strip()
    if dirty:
        raise GateError("formal source-bound build requires a clean tracked worktree and index")
    tracked_flags_output = bytes(_git(repo, "ls-files", "-v", "-z", binary=True))
    for entry in (item for item in tracked_flags_output.split(b"\0") if item):
        if len(entry) < 3 or entry[1:2] != b" ":
            raise GateError("git ls-files -v returned a malformed tracked-file entry")
        flag = chr(entry[0])
        if flag != "H":
            path = entry[2:].decode("utf-8")
            raise GateError(
                f"formal source-bound build rejects nonordinary Git index flag {flag!r}: {path}"
            )
    untracked_output = bytes(_git(repo, "ls-files", "--others", "--exclude-standard", "-z", binary=True))
    untracked = [item.decode("utf-8") for item in untracked_output.split(b"\0") if item]
    disallowed = [path for path in untracked if not _is_excluded_runtime_artifact(path)]
    if disallowed:
        raise GateError(f"formal source-bound build rejects untracked build inputs: {disallowed[0]}")
    ignored_output = bytes(
        _git(repo, "ls-files", "--others", "--ignored", "--exclude-standard", "-z", binary=True)
    )
    ignored = [item.decode("utf-8") for item in ignored_output.split(b"\0") if item]
    ignored_build_inputs = [path for path in ignored if _is_ignored_build_input_candidate(path)]
    if ignored_build_inputs:
        raise GateError(
            f"formal source-bound build rejects ignored source/build input: {ignored_build_inputs[0]}"
        )
    cargo_lock = repo / "Cargo.lock"
    _regular_file(cargo_lock, "Cargo.lock")
    try:
        _git(repo, "ls-files", "--error-unmatch", "Cargo.lock")
    except GateError as error:
        raise GateError("Cargo.lock must be tracked for a formal source-bound build") from error
    tracked_output = bytes(_git(repo, "ls-files", "-z", binary=True))
    tracked = [item.decode("utf-8") for item in tracked_output.split(b"\0") if item]
    tracked_index = bytes(_git(repo, "ls-files", "-s", "-z", binary=True))
    for entry in (item for item in tracked_index.split(b"\0") if item):
        try:
            metadata, path_bytes = entry.split(b"\t", 1)
            mode, _object_id, stage = metadata.split(b" ", 2)
        except ValueError as error:
            raise GateError("git ls-files -s returned a malformed tracked-file entry") from error
        path = path_bytes.decode("utf-8")
        if mode not in {b"100644", b"100755"}:
            raise GateError(
                f"formal source-bound build rejects unsupported tracked Git mode "
                f"{mode.decode('ascii', errors='replace')}: {path}"
            )
        if stage != b"0":
            raise GateError(f"formal source-bound build rejects nonzero Git index stage: {path}")
    cargo_configs = []
    for relative in tracked:
        if relative.endswith("/.cargo/config") or relative.endswith("/.cargo/config.toml") or relative in {
            ".cargo/config", ".cargo/config.toml"
        }:
            path = repo / relative
            _regular_file(path, f"Cargo config {relative}")
            cargo_configs.append(
                {"path": relative, "sha256": _sha256(path), "size_bytes": path.stat().st_size}
            )
    parent = repo.parent
    while True:
        for name in ("config", "config.toml"):
            ambient = parent / ".cargo" / name
            if ambient.exists():
                raise GateError(f"ambient ancestor Cargo config is forbidden: {ambient}")
        if parent == parent.parent:
            break
        parent = parent.parent
    head = str(_git(repo, "rev-parse", "HEAD")).strip()
    tree = str(_git(repo, "rev-parse", "HEAD^{tree}")).strip()
    if not re.fullmatch(r"[0-9a-f]{40,64}", head) or not re.fullmatch(r"[0-9a-f]{40,64}", tree):
        raise GateError("Git HEAD or tree object id has an invalid shape")
    identity = {
        "head": head,
        "tree": tree,
        "tracked_index_sha256": hashlib.sha256(tracked_index).hexdigest(),
        "tracked_file_count": len(tracked),
        "cargo_lock_sha256": _sha256(cargo_lock),
        "cargo_configs": cargo_configs,
    }
    identity_sha256 = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return {
        "schema": SOURCE_SEAL_SCHEMA,
        "repo": str(repo),
        **identity,
        "identity_sha256": identity_sha256,
        "excluded_untracked_runtime_artifacts": sorted(untracked),
    }


def check_source_seal(repo: Path, seal_path: Path) -> dict[str, Any]:
    expected = _load_json(seal_path)
    current = source_seal(repo)
    for key in (
        "schema", "repo", "head", "tree", "tracked_index_sha256", "tracked_file_count",
        "cargo_lock_sha256", "cargo_configs", "identity_sha256",
    ):
        if expected.get(key) != current[key]:
            raise GateError(f"source seal changed: {key}")
    return {"status": "pass", "identity_sha256": current["identity_sha256"]}


def _git_blob_oid(path: Path, object_format: str) -> str:
    try:
        digest = hashlib.new(object_format)
    except ValueError as error:
        raise GateError(f"unsupported Git object format: {object_format}") from error
    size = path.stat().st_size
    digest.update(f"blob {size}\0".encode())
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def source_snapshot_seal(
    repo: Path, snapshot: Path, source_seal_path: Path
) -> dict[str, Any]:
    repo = _canonical_directory(repo, "source root")
    snapshot = _canonical_directory(snapshot, "source snapshot")
    if stat.S_IMODE(snapshot.stat().st_mode) != 0o555:
        raise GateError("source snapshot root must be mode 0555")

    check_source_seal(repo, source_seal_path)
    source_document = _load_json(source_seal_path)
    sealed_head = source_document.get("head")
    sealed_tree = source_document.get("tree")
    if not isinstance(sealed_head, str) or not re.fullmatch(r"[0-9a-f]{40,64}", sealed_head):
        raise GateError("formal source seal has an invalid HEAD object id")
    if not isinstance(sealed_tree, str) or not re.fullmatch(r"[0-9a-f]{40,64}", sealed_tree):
        raise GateError("formal source seal has an invalid tree object id")
    observed_tree = str(_git(repo, "rev-parse", f"{sealed_head}^{{tree}}")).strip()
    if observed_tree != sealed_tree:
        raise GateError("formal source seal HEAD and tree are not cross-bound")

    object_format = str(_git(repo, "rev-parse", "--show-object-format")).strip()
    if object_format not in {"sha1", "sha256"}:
        raise GateError(f"unsupported Git object format: {object_format}")
    tree_entries = bytes(
        _git(repo, "ls-tree", "-r", "-z", "--full-tree", sealed_head, binary=True)
    )
    expected: dict[str, tuple[str, str]] = {}
    expected_directories: set[str] = set()
    for entry in (item for item in tree_entries.split(b"\0") if item):
        try:
            metadata, path_bytes = entry.split(b"\t", 1)
            mode_bytes, object_type, object_id_bytes = metadata.split(b" ", 2)
        except ValueError as error:
            raise GateError("git ls-tree returned a malformed snapshot entry") from error
        path = path_bytes.decode("utf-8")
        mode = mode_bytes.decode("ascii")
        object_id = object_id_bytes.decode("ascii")
        if object_type != b"blob" or mode not in {"100644", "100755"}:
            raise GateError(
                f"unsupported tracked snapshot entry: {mode} {object_type!r} {path}"
            )
        if path in expected:
            raise GateError(f"duplicate tracked snapshot path: {path}")
        expected[path] = (mode, object_id)
        parent = PurePosixPath(path).parent
        while parent != PurePosixPath("."):
            expected_directories.add(parent.as_posix())
            parent = parent.parent

    observed: dict[str, Path] = {}
    observed_directories: set[str] = set()
    pending = [Path()]
    while pending:
        relative_directory = pending.pop()
        directory = snapshot / relative_directory
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: os.fsencode(entry.name))
        except OSError as error:
            raise GateError(
                f"cannot enumerate source snapshot directory {directory}: {error}"
            ) from error
        child_directories: list[Path] = []
        for entry in entries:
            relative_path = relative_directory / entry.name
            if relative_path.is_absolute() or any(
                part in ("", ".", "..") for part in relative_path.parts
            ):
                raise GateError(f"source snapshot path escapes its root: {relative_path}")
            relative = relative_path.as_posix()
            candidate = snapshot / relative_path
            try:
                metadata = entry.stat(follow_symlinks=False)
            except OSError as error:
                raise GateError(
                    f"cannot inspect source snapshot entry {candidate}: {error}"
                ) from error
            if stat.S_ISLNK(metadata.st_mode):
                raise GateError(f"source snapshot contains a symlink: {relative}")
            if stat.S_ISDIR(metadata.st_mode):
                if stat.S_IMODE(metadata.st_mode) != 0o555:
                    raise GateError(
                        f"source snapshot directory is not mode 0555: {relative}"
                    )
                observed_directories.add(relative)
                child_directories.append(relative_path)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise GateError(
                    f"source snapshot contains a non-regular entry: {relative}"
                )
            observed[relative] = candidate
        pending.extend(reversed(child_directories))
    if set(observed) != set(expected):
        missing = sorted(set(expected) - set(observed))
        extra = sorted(set(observed) - set(expected))
        raise GateError(
            f"source snapshot path set differs from Git HEAD: missing={missing[:1]} extra={extra[:1]}"
        )
    if observed_directories != expected_directories:
        missing = sorted(expected_directories - observed_directories)
        extra = sorted(observed_directories - expected_directories)
        raise GateError(
            "source snapshot directory set differs from Git HEAD: "
            f"missing={missing[:1]} extra={extra[:1]}"
        )

    files = []
    for relative in sorted(expected):
        expected_mode, expected_object_id = expected[relative]
        path = observed[relative]
        file_mode = stat.S_IMODE(path.stat().st_mode)
        required_file_mode = 0o555 if expected_mode == "100755" else 0o444
        if file_mode != required_file_mode:
            raise GateError(
                f"source snapshot file mode differs for {relative}: "
                f"expected {required_file_mode:04o}, observed {file_mode:04o}"
            )
        executable = bool(file_mode & stat.S_IXUSR)
        observed_mode = "100755" if executable else "100644"
        if observed_mode != expected_mode:
            raise GateError(f"source snapshot executable mode differs for {relative}")
        observed_object_id = _git_blob_oid(path, object_format)
        if observed_object_id != expected_object_id:
            raise GateError(f"source snapshot bytes differ from Git for {relative}")
        files.append(
            {
                "path": relative,
                "mode": expected_mode,
                "object_id": expected_object_id,
                "size_bytes": path.stat().st_size,
            }
        )
    identity = {
        "git_head": sealed_head,
        "git_tree": sealed_tree,
        "source_seal_identity_sha256": source_document["identity_sha256"],
        "object_format": object_format,
        "file_count": len(files),
        "files": files,
    }
    identity_sha256 = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return {
        "schema": SOURCE_SNAPSHOT_SEAL_SCHEMA,
        "repo": str(repo),
        "snapshot": str(snapshot),
        **identity,
        "identity_sha256": identity_sha256,
    }


def check_source_snapshot_seal(
    repo: Path,
    snapshot: Path,
    source_seal_path: Path,
    seal_path: Path,
) -> dict[str, Any]:
    expected = _load_json(seal_path)
    current = source_snapshot_seal(repo, snapshot, source_seal_path)
    if expected != current:
        raise GateError("read-only source snapshot seal changed")
    return {"status": "pass", "identity_sha256": current["identity_sha256"]}


def cargo_config_isolation(snapshot: Path, cargo_home: Path) -> dict[str, Any]:
    snapshot = _canonical_directory(snapshot, "source snapshot")
    cargo_home = _canonical_directory(cargo_home, "CARGO_HOME")

    checked: list[str] = []
    bases = [cargo_home]
    ancestor = snapshot.parent
    while True:
        bases.append(ancestor / ".cargo")
        if ancestor == ancestor.parent:
            break
        ancestor = ancestor.parent
    for base in bases:
        for name in ("config", "config.toml"):
            candidate = base / name
            checked.append(str(candidate))
            if os.path.lexists(candidate):
                raise GateError(f"ambient Cargo config is forbidden: {candidate}")
    return {
        "schema": CARGO_CONFIG_ISOLATION_SCHEMA,
        "status": "pass",
        "snapshot": str(snapshot),
        "cargo_home": str(cargo_home),
        "checked_paths": checked,
    }


FINAL_ARTIFACT_REQUIRED_DIRECTORIES = {
    "comparisons",
    "configs",
    "inventory",
    "metadata",
    "query-runs",
    "replays",
    "validation",
}
FINAL_ARTIFACT_OPTIONAL_DIRECTORIES = {"build-source", "build-target"}
FINAL_ARTIFACT_EXCLUDED_DIRECTORIES = {
    "build-source",
    "build-target",
    "metadata/build/cargo-home",
    "metadata/build/home",
}
FINAL_ARTIFACT_EXCLUDED_FILES = {
    "metadata/result-artifacts.nul",
    "metadata/result-artifacts.sha256",
}
FROZEN_HARNESS_FILES = {
    "2026-07-22-phase6-codec-results.md",
    "ab_gate.py",
    "fadvise_regular_dontneed.c",
    "phase1_query_gate.py",
    "phase1_4m_expectations.json",
    "phase1_replay_gate.py",
    "phase2_compact_ids_ab_gate.py",
    "phase3_payload_coalescing_gate.py",
    "phase6_codec_ab_gate.py",
    "phase6_codec_ab_run.sh",
    "phase6_codec_queries.json",
    "schema7_query_ab_gate.py",
    "schema8_query_ab_gate.py",
    "test_phase6_codec_ab_gate.py",
}
INTERNAL_BUILD_FILES = {
    "build-command.txt",
    "build-environment.tsv",
    "build.exit-status",
    "build.log",
    "cargo-config-isolation-after-build.json",
    "cargo-config-isolation-after-metadata.json",
    "cargo-config-isolation-before-metadata.json",
    "cargo-config-isolation-final.json",
    "cargo-metadata.json",
    "cargo-version.txt",
    "rustc-version.txt",
    "rustup-active-toolchain.txt",
    "source-archive-check-after-build.txt",
    "source-archive-check-before-build.txt",
    "source-archive-check-final.txt",
    "source-check-after-build.json",
    "source-check-before-build.json",
    "source-check-final.json",
    "source-snapshot-check-after-build.json",
    "source-snapshot-check-before-build.json",
    "source-snapshot-check-final.json",
    "tool-binaries.sha256",
    "tool-paths.tsv",
}
INTERNAL_BUILD_DIRECTORIES = {"cargo-home", "home"}
FINAL_ROOT_EVIDENCE_FILES = {
    "queries.normalized.json",
    "queries.tsv",
    "query-index.tsv",
    "query-summary.tsv",
    "replay-index.tsv",
    "replay-plan.tsv",
    "replay-summary.tsv",
}
FINAL_COMPARISON_FILES = {
    "query-equivalence.json",
    "replay-equivalence.json",
    "verifier-equivalence-and-codec-inventory.json",
}
FINAL_METADATA_BASE_FILES = {
    "admission-plan.json",
    "binaries.tsv",
    "capacity-contract.json",
    "capacity-final.json",
    "capacity-postbuild.json",
    "capacity-prebuild.json",
    "config-template.sha256",
    "config-template.toml",
    "controlled-inputs.sha256",
    "environment.txt",
    "fadvise-regular-dontneed",
    "fadvise.sha256",
    "final-raw-leaves.json",
    "guardian-conflicts.tsv",
    "guardian.log",
    "guardian-precheck.json",
    "guardian.ready",
    "guardian-samples.tsv",
    "guardian.json",
    "guardian.stop",
    "harness.sha256",
    "perf-effective.txt",
    "preserved-binaries.sha256",
    "raw-authorities.sha256",
    "raw-authorities.tsv",
    "rendered-configs.sha256",
    "run-note.txt",
    "seal-checks.tsv",
    "settings.txt",
    "validated-inputs.json",
}
FINAL_BINARY_FILES = {
    "chronoxide-ingester",
    "chronoxide-query",
    "chronoxide-storage-verify",
}
FINAL_SOURCE_BASE_FILES = {
    "git-commit.txt",
    "git-status.txt",
    "git-tree.txt",
    "tracked-index.txt",
    "tracked-source.patch",
    "untracked-files.nul",
    "untracked-files.tsv",
}
FINAL_SOURCE_INTERNAL_FILES = {
    "formal-source-seal.json",
    "source-head.tar",
    "source-head.tar.sha256",
    "source-snapshot-seal.json",
}
FINAL_INVENTORY_FILES = {
    "capture-after-replays.json",
    "capture-files-after-replays.nul",
    "capture-files.nul",
    "capture.json",
    "gorilla-after.json",
    "gorilla-before.json",
    "gorilla-files-after.nul",
    "gorilla-files.nul",
    "raw-after.json",
    "raw-before.json",
    "raw-files-after.nul",
    "raw-files.nul",
}
FINAL_REPLAY_RUN_FILES = {
    "artifacts.json",
    "capture-residency-before.tsv",
    "capacity-after.json",
    "capacity-before.json",
    "capacity-corpus-check.json",
    "capacity-monitor.exit-status",
    "capacity-monitor.log",
    "capacity-monitor.ready",
    "capacity-samples.tsv",
    "capacity.json",
    "config.json",
    "corpus-summary.json",
    "invocation.json",
    "pressure-after.txt",
    "pressure-before.txt",
    "raw-leaves.json",
    "replay-correctness.json",
    "replay.exit-status",
    "replay-monitor-control.json",
    "replay.launch",
    "replay.log",
    "replay.time.json",
    "replay.time.txt",
    "rss-monitor.exit-status",
    "rss-monitor.log",
    "rss-monitor.ready",
    "rss-samples.tsv",
    "rss.json",
    "runtime-identity.json",
    "seal.json",
    "segments.sha256",
    "segments.tsv",
    "writeback-before.tsv",
}
FINAL_QUERY_RUN_FILES = {
    "exit-status",
    "invocation.json",
    "pressure-after.txt",
    "pressure-before.txt",
    "query.log",
    "raw-leaves.json",
    "raw.json",
    "report.md",
    "residency-after-evict.tsv",
    "residency-after-run.tsv",
    "runtime-identity.json",
    "time.txt",
    "writeback-before.tsv",
}
FINAL_VALIDATION_FILES = {
    "readback-invocation.json",
    "readback-raw-leaves.json",
    "readbacks.exit-status",
    "readbacks.json",
    "readbacks.log",
    "readbacks.md",
    "readbacks.runtime-identity.json",
    "readbacks.time.txt",
    "storage-verify.exit-status",
    "storage-verify-invocation.json",
    "storage-verify-raw-leaves.json",
    "storage-verify.json",
    "storage-verify.log",
    "storage-verify.runtime-identity.json",
    "storage-verify.time.json",
    "storage-verify.time.txt",
    "writeback-before-verifier.tsv",
}


def _validate_fixed_nested_layout(result_dir: Path, internal_build: bool) -> None:
    """Reject unplanned evidence outside the few explicitly dynamic build trees."""
    _require_exact_names(
        result_dir / "metadata" / "harness",
        "frozen harness directory",
        FROZEN_HARNESS_FILES,
        set(),
    )
    build_directory = result_dir / "metadata" / "build"
    if internal_build:
        _require_exact_names(
            build_directory,
            "formal build metadata directory",
            INTERNAL_BUILD_FILES,
            INTERNAL_BUILD_DIRECTORIES,
        )
    elif os.path.lexists(build_directory):
        raise GateError(
            "external-binary evidence contains unexpected formal build metadata"
        )


def final_artifact_paths(result_dir: Path) -> list[str]:
    if not result_dir.is_absolute():
        raise GateError("final result root must be an absolute non-symlink directory")
    try:
        root_metadata = result_dir.lstat()
    except OSError as error:
        raise GateError(f"cannot inspect final result root: {error}") from error
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise GateError("final result root must be a regular directory")
    result_dir = _canonical_directory(result_dir, "final result root")

    paths: list[str] = []

    def visit(directory: Path, relative_directory: str) -> None:
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
        except OSError as error:
            context = relative_directory or "."
            raise GateError(
                f"cannot enumerate final artifact directory {context}: {error}"
            ) from error
        for entry in entries:
            relative = (
                f"{relative_directory}/{entry.name}"
                if relative_directory
                else entry.name
            )
            try:
                relative.encode("utf-8")
            except UnicodeEncodeError as error:
                raise GateError(
                    "final artifact contains a non-UTF-8 path"
                ) from error
            if any(
                character in relative
                for character in ("\0", "\n", "\r", "\t", "\\")
            ):
                raise GateError(
                    f"final artifact contains an unsafe path: {relative!r}"
                )
            try:
                entry_stat = entry.stat(follow_symlinks=False)
            except OSError as error:
                raise GateError(
                    f"cannot stat final artifact {relative}: {error}"
                ) from error
            if stat.S_ISLNK(entry_stat.st_mode):
                raise GateError(f"final artifact contains a symlink: {relative}")
            if stat.S_ISDIR(entry_stat.st_mode):
                if not relative_directory and relative not in (
                    FINAL_ARTIFACT_REQUIRED_DIRECTORIES
                    | FINAL_ARTIFACT_OPTIONAL_DIRECTORIES
                ):
                    raise GateError(
                        f"final artifact contains an unsupported root directory: {relative}"
                    )
                if relative not in FINAL_ARTIFACT_EXCLUDED_DIRECTORIES:
                    visit(Path(entry.path), relative)
            elif stat.S_ISREG(entry_stat.st_mode):
                if relative not in FINAL_ARTIFACT_EXCLUDED_FILES:
                    paths.append(relative)
            else:
                raise GateError(
                    f"final artifact contains a non-regular entry: {relative}"
                )

    visit(result_dir, "")
    root_files, present_directories = _directory_entry_names(
        result_dir, "final result root"
    )
    admission_path = result_dir / "metadata" / "final-admission.json"
    _require_mode(admission_path, 0o444, "final admission result")
    admission = _load_json(admission_path)
    if (
        not isinstance(admission, dict)
        or admission.get("schema") != FINAL_ADMISSION_SCHEMA
        or admission.get("status") != "pass"
    ):
        raise GateError("final artifact lacks a passing final admission result")
    common_root_files = {
        "queries.tsv",
        "queries.normalized.json",
        "replay-plan.tsv",
        "replay-index.tsv",
        "replay-summary.tsv",
        "query-index.tsv",
        "query-summary.tsv",
        "TIMESTAMP_CODEC_AB_BLOCKED.txt",
    }
    if admission.get("promotion_eligibility") == "formal_source_bound":
        internal_build = True
        expected_root_files = common_root_files | {
            "RAW_GORILLA_COMPLETE_TIMESTAMP_AB_BLOCKED"
        }
    elif (
        admission.get("promotion_eligibility")
        == "exploratory_non_promotable_external_binaries"
    ):
        internal_build = False
        expected_root_files = common_root_files | {
            "EXPLORATORY_EXTERNAL_BINARIES_NON_PROMOTABLE.txt"
        }
    else:
        raise GateError("final admission has invalid promotion eligibility")
    expected_directories = FINAL_ARTIFACT_REQUIRED_DIRECTORIES | (
        FINAL_ARTIFACT_OPTIONAL_DIRECTORIES if internal_build else set()
    )
    if present_directories != expected_directories:
        raise GateError(
            "final root directory matrix differs: "
            f"missing={sorted(expected_directories - present_directories)!r} "
            f"extra={sorted(present_directories - expected_directories)!r}"
        )
    _validate_fixed_nested_layout(result_dir, internal_build)
    if root_files != expected_root_files:
        raise GateError(
            "final root marker/artifact matrix differs: "
            f"missing={sorted(expected_root_files - root_files)!r} "
            f"extra={sorted(root_files - expected_root_files)!r}"
        )
    _validate_finalized_artifact_matrix(result_dir, admission)
    if len(set(paths)) != len(paths):
        raise GateError("final artifact inventory contains duplicate paths")
    paths.sort(key=lambda path: path.encode("utf-8"))
    return paths


def write_final_artifact_inventory(result_dir: Path, output: Path) -> None:
    if not result_dir.is_absolute():
        raise GateError("final result root must be absolute")
    expected_output = result_dir / "metadata" / "result-artifacts.nul"
    if output.absolute() != expected_output.absolute():
        raise GateError(f"final artifact inventory output must be {expected_output}")
    if os.path.lexists(output):
        raise GateError("final artifact inventory output already exists")
    paths = final_artifact_paths(result_dir)
    with output.open("xb") as destination:
        for path in paths:
            destination.write(path.encode("utf-8") + b"\0")


def verify_final_artifact_seal(result_dir: Path) -> None:
    """Reinventory and rehash the finished result against its exact seal."""
    result_dir = _canonical_directory(result_dir, "sealed final result root")
    inventory = result_dir / "metadata" / "result-artifacts.nul"
    checksum = result_dir / "metadata" / "result-artifacts.sha256"
    _require_mode(inventory, 0o444, "final artifact inventory")
    _require_mode(checksum, 0o444, "final artifact checksum authority")
    encoded_inventory = inventory.read_bytes()
    if not encoded_inventory or not encoded_inventory.endswith(b"\0"):
        raise GateError("final artifact inventory is empty or lacks a NUL terminator")
    try:
        observed = [
            field.decode("utf-8")
            for field in encoded_inventory.removesuffix(b"\0").split(b"\0")
        ]
    except UnicodeDecodeError as error:
        raise GateError("final artifact inventory contains a non-UTF-8 path") from error
    if any(not path for path in observed) or len(observed) != len(set(observed)):
        raise GateError("final artifact inventory contains an empty or duplicate path")
    current = final_artifact_paths(result_dir)
    if observed != current:
        raise GateError("final artifact inventory differs from exact reinventory")

    expected_paths = [*observed, "metadata/result-artifacts.nul"]
    try:
        lines = checksum.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError as error:
        raise GateError("final artifact checksum authority is not UTF-8") from error
    if len(lines) != len(expected_paths):
        raise GateError("final artifact checksum authority has the wrong row count")
    for line, relative in zip(lines, expected_paths, strict=True):
        match = re.fullmatch(r"([0-9a-f]{64})  ([^\0\n\r\t]+)", line)
        if match is None or match.group(2) != relative:
            raise GateError(
                "final artifact checksum authority differs from exact inventory"
            )
        path = result_dir / relative
        _regular_file(path, "sealed final artifact")
        if _sha256(path) != match.group(1):
            raise GateError(f"sealed final artifact content changed: {relative}")


FORBIDDEN_AMBIENT_ENV_EXACT = {
    "AR", "CC", "CFLAGS", "CONFIG_FILE", "CXX", "CXXFLAGS", "DYLD_INSERT_LIBRARIES",
    "GLIBC_TUNABLES", "LD_LIBRARY_PATH", "LD_PRELOAD", "LDFLAGS", "MALLOC_CONF",
    "RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTFLAGS", "RUSTDOCFLAGS",
    "RUSTUP_TOOLCHAIN", "RUST_LOG",
}
FORBIDDEN_AMBIENT_ENV_PREFIXES = ("CARGO_", "JEMALLOC_", "MIMALLOC_", "SCCACHE_")


def forbidden_ambient_environment(environment: dict[str, str]) -> list[str]:
    return sorted(
        name
        for name in environment
        if name in FORBIDDEN_AMBIENT_ENV_EXACT
        or any(name.startswith(prefix) for prefix in FORBIDDEN_AMBIENT_ENV_PREFIXES)
        or (
            name.startswith("PYTHON")
            and name not in {"PYTHONDONTWRITEBYTECODE", "PYTHONNOUSERSITE"}
        )
    )


def runtime_identity(
    binary: Path,
    role: str,
    assignments: list[str],
    normalized_names: set[str],
) -> dict[str, Any]:
    _regular_file(binary, f"{role} binary")
    environment: dict[str, str] = {}
    for assignment in assignments:
        if "=" not in assignment:
            raise GateError(f"runtime environment assignment lacks '=': {assignment}")
        name, value = assignment.split("=", 1)
        if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", name) or name in environment:
            raise GateError(f"invalid or duplicate runtime environment name: {name}")
        environment[name] = value
    expected_names = {"LC_ALL", "TZ"}
    if role == "ingester":
        expected_names |= {"CONFIG_FILE", "RUST_LOG"}
    elif role not in {"query", "verifier"}:
        raise GateError(f"unknown runtime role: {role}")
    if set(environment) != expected_names:
        raise GateError(f"{role} runtime environment differs from the sanitized contract")
    if not normalized_names <= set(environment):
        raise GateError("normalized runtime environment names are not present")
    canonical = {
        name: (f"<{name}>" if name in normalized_names else value)
        for name, value in environment.items()
    }
    exact_sha256 = hashlib.sha256(
        json.dumps(environment, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    canonical_sha256 = hashlib.sha256(
        json.dumps(canonical, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return {
        "schema": RUNTIME_IDENTITY_SCHEMA,
        "role": role,
        "binary": str(binary.resolve(strict=True)),
        "binary_sha256": _sha256(binary),
        "environment": environment,
        "environment_sha256": exact_sha256,
        "controlled_environment_sha256": canonical_sha256,
        "normalized_environment_names": sorted(normalized_names),
    }


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def _write_json(path: Path, value: Any) -> None:
    if path.exists():
        raise GateError(f"refusing to reuse output: {path}")
    with path.open("x", encoding="utf-8") as destination:
        json.dump(value, destination, indent=2, sort_keys=True)
        destination.write("\n")


def _publish_read_only_bytes_atomic_exclusive(path: Path, value: bytes) -> None:
    """Publish immutable control bytes without exposing a partial file."""
    if not path.is_absolute():
        raise GateError(f"atomic authority path must be absolute: {path}")
    parent = _canonical_directory(path.parent, "atomic authority parent")
    if os.path.lexists(path):
        raise GateError(f"refusing to reuse atomic authority: {path}")
    descriptor, temporary_name = tempfile.mkstemp(
        dir=parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as destination:
            destination.write(value)
            destination.flush()
            os.fchmod(destination.fileno(), 0o444)
            os.fsync(destination.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as error:
            raise GateError(f"refusing to replace atomic authority: {path}") from error
        directory_descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _publish_read_only_json_atomic_exclusive(path: Path, value: Any) -> None:
    encoded = json.dumps(value, indent=2, sort_keys=True).encode() + b"\n"
    _publish_read_only_bytes_atomic_exclusive(path, encoded)


def _create_empty_read_only_marker(path: Path, description: str) -> None:
    try:
        _publish_read_only_bytes_atomic_exclusive(path, b"")
    except GateError as error:
        raise GateError(f"cannot publish {description}: {error}") from error


def _validate_empty_read_only_marker(path: Path, description: str) -> Path:
    candidate = _canonical_file(path, description)
    metadata = candidate.stat()
    if metadata.st_size != 0 or stat.S_IMODE(metadata.st_mode) != 0o444:
        raise GateError(f"{description} must be exact empty mode 0444")
    return candidate


def _nonnegative(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GateError(f"{name} must be a non-negative integer")
    return value


def _positive(value: Any, name: str) -> int:
    value = _nonnegative(value, name)
    if value == 0:
        raise GateError(f"{name} must be positive")
    return value


def _digest(value: Any, name: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise GateError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _ceil_ratio(value: int, numerator: int, denominator: int) -> int:
    value = _nonnegative(value, "capacity ratio value")
    numerator = _positive(numerator, "capacity ratio numerator")
    denominator = _positive(denominator, "capacity ratio denominator")
    return (value * numerator + denominator - 1) // denominator


def _capacity_expectation_facts(expectations_path: Path) -> dict[str, int]:
    """Extract only pinned facts that make the codec bound derivable.

    The expectations digest is itself pinned. The additional structural checks
    make the proof legible and fail closed if a future expectations document
    preserves the digest constant incorrectly while changing its meaning.
    """
    _regular_file(expectations_path, "Phase 1 capacity expectations")
    digest = _sha256(expectations_path)
    if digest != PINNED_EXPECTATIONS_SHA256:
        raise GateError(
            "Phase 1 capacity expectations are not the pinned four-million-message authority"
        )
    document = _load_json(expectations_path)
    try:
        stop_after_messages = document["stop_after_messages"]
        baseline = document["corpus"]["size_bytes"]
        correctness = document["replay_correctness"]
        recorded = correctness["general"]["Recorded Samples"]
        types = correctness["otlp_data_type_counts"]
        histogram = types["Histogram"]["accepted_datapoints"]
        exponential_histogram = types["Exponential Histogram"]["accepted_datapoints"]
        summary = types["Summary"]["accepted_datapoints"]
        verifier = document["storage_verifier"]
        verifier_samples = verifier["samples"]
        chunks_by_kind = verifier["chunks_by_kind"]
    except (KeyError, TypeError) as error:
        raise GateError(f"pinned capacity expectations lack a required fact: {error}") from error
    for name, value in {
        "stop_after_messages": stop_after_messages,
        "baseline_gorilla_corpus_bytes": baseline,
        "recorded_samples": recorded,
        "histogram_samples": histogram,
        "exponential_histogram_samples": exponential_histogram,
        "summary_samples": summary,
        "verifier_samples": verifier_samples,
    }.items():
        _positive(value, name)
    if stop_after_messages != PINNED_STOP_AFTER_MESSAGES:
        raise GateError("capacity expectations do not describe the pinned message prefix")
    if verifier_samples != recorded:
        raise GateError("capacity expectations disagree on recorded/verifier samples")
    if (
        not isinstance(chunks_by_kind, list)
        or len(chunks_by_kind) != 5
        or any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in chunks_by_kind)
        or chunks_by_kind[1] != 0
    ):
        raise GateError("capacity proof requires the pinned verifier to report zero Int64 chunks")
    typed = histogram + exponential_histogram + summary
    if typed >= recorded:
        raise GateError("typed sample total cannot derive a positive Float point count")
    float_points = recorded - typed
    return {
        "stop_after_messages": stop_after_messages,
        "baseline_gorilla_corpus_bytes": baseline,
        "recorded_samples": recorded,
        "histogram_samples": histogram,
        "exponential_histogram_samples": exponential_histogram,
        "summary_samples": summary,
        "typed_samples": typed,
        "float_points": float_points,
        "int64_chunks": chunks_by_kind[1],
    }


def _capacity_layout_authorities(repo: Path, source_head: str) -> dict[str, str]:
    repo = _canonical_directory(repo, "capacity source root")
    if re.fullmatch(r"[0-9a-f]{40,64}", source_head) is None:
        raise GateError("capacity source HEAD must be a lowercase Git object ID")
    resolved = str(_git(repo, "rev-parse", f"{source_head}^{{commit}}")).strip()
    if resolved != source_head:
        raise GateError("capacity source HEAD is not the exact resolved commit")
    observed: dict[str, str] = {}
    contents: dict[str, bytes] = {}
    for relative, expected_digest in CAPACITY_LAYOUT_AUTHORITIES.items():
        data = bytes(_git(repo, "show", f"{source_head}:{relative}", binary=True))
        digest = hashlib.sha256(data).hexdigest()
        if digest != expected_digest:
            raise GateError(
                f"capacity layout authority changed and requires a new reviewed bound: {relative}"
            )
        observed[relative] = digest
        contents[relative] = data
    types_text = contents["chronoxide-core/src/storage/chunk/types.rs"].decode("utf-8")
    writer_text = contents["chronoxide-core/src/storage/chunk/writer.rs"].decode("utf-8")
    spec_text = contents["docs/superpowers/specs/storage.md"].decode("utf-8")
    if (
        "pub(crate) const CHUNK_HEADER_LEN: usize = 40;" not in types_text
        or "payload.extend_from_slice(&value.to_le_bytes());" not in writer_text
        or "let value_buf = encode_gorilla_values(&values)?;" not in writer_text
        or "Each candidate consists of the common 40-byte header" not in spec_text
        or "eight bytes per RAW_F64 value" not in spec_text
    ):
        raise GateError("current on-disk Float framing no longer proves the pinned capacity bound")
    return observed


def build_capacity_contract(
    expectations_path: Path,
    repo: Path,
    source_head: str,
    replay_blocks: int,
) -> dict[str, Any]:
    if replay_blocks != PINNED_REPLAY_BLOCKS:
        raise GateError(
            f"capacity contract requires replay_blocks={PINNED_REPLAY_BLOCKS}"
        )
    facts = _capacity_expectation_facts(expectations_path)
    layout_authorities = _capacity_layout_authorities(repo, source_head)
    raw_value_bytes = facts["float_points"] * RAW_F64_VALUE_BYTES
    gorilla_bound = facts["baseline_gorilla_corpus_bytes"]
    # The baseline already contains a non-negative Gorilla value stream. The
    # current, pinned framing keeps the same common 40-byte header, t0/delta
    # bytes, typed payloads, indexes and metadata; replacing only that stream
    # with 8*N Raw values is therefore bounded by baseline + 8*N.
    raw_bound = gorilla_bound + raw_value_bytes
    safe = {
        "raw": _ceil_ratio(
            raw_bound, CAPACITY_SAFETY_NUMERATOR, CAPACITY_SAFETY_DENOMINATOR
        ),
        "gorilla": _ceil_ratio(
            gorilla_bound, CAPACITY_SAFETY_NUMERATOR, CAPACITY_SAFETY_DENOMINATOR
        ),
    }
    codec_runs = {"raw": replay_blocks * 2, "gorilla": replay_blocks * 2}
    scheduled = sum(safe[codec] * count for codec, count in codec_runs.items())
    postbuild = scheduled + CAPACITY_OPERATIONAL_FLOOR_BYTES
    initial = postbuild + CAPACITY_BUILD_SOURCE_RESULT_ALLOWANCE_BYTES
    return {
        "schema": CAPACITY_CONTRACT_SCHEMA,
        "expectations_sha256": PINNED_EXPECTATIONS_SHA256,
        "source_head": source_head,
        "layout_authorities": layout_authorities,
        "derivation": {
            **facts,
            "common_chunk_header_bytes": COMMON_CHUNK_HEADER_BYTES,
            "raw_f64_value_bytes_per_point": RAW_F64_VALUE_BYTES,
            "raw_f64_value_bytes": raw_value_bytes,
            "corpus_bound_bytes": {"raw": raw_bound, "gorilla": gorilla_bound},
            "safety_numerator": CAPACITY_SAFETY_NUMERATOR,
            "safety_denominator": CAPACITY_SAFETY_DENOMINATOR,
            "safe_corpus_reserve_bytes": safe,
        },
        "schedule": {
            "replay_blocks": replay_blocks,
            "runs_per_block": 4,
            "codec_runs": codec_runs,
            "safe_corpus_reserve_bytes": scheduled,
        },
        "operational_floor_bytes": CAPACITY_OPERATIONAL_FLOOR_BYTES,
        "build_source_result_allowance_bytes": (
            CAPACITY_BUILD_SOURCE_RESULT_ALLOWANCE_BYTES
        ),
        "postbuild_required_free_bytes": postbuild,
        "initial_required_free_bytes": initial,
    }


def _load_capacity_contract(path: Path) -> dict[str, Any]:
    document = _load_json(path)
    if not isinstance(document, dict) or document.get("schema") != CAPACITY_CONTRACT_SCHEMA:
        raise GateError("capacity contract has an invalid schema or shape")
    try:
        expected = build_capacity_contract(
            path.parent / "harness" / "phase1_4m_expectations.json",
            Path(_load_json(path.parent / "admission-plan.json")["repo"]),
            document["source_head"],
            document["schedule"]["replay_blocks"],
        )
    except (KeyError, TypeError) as error:
        raise GateError(f"capacity contract lacks a required field: {error}") from error
    if document != expected:
        raise GateError("capacity contract differs from its frozen facts and source framing")
    return document


def _capacity_free_bytes(filesystem: Path) -> tuple[Path, int, int]:
    filesystem = _canonical_directory(filesystem, "capacity filesystem path")
    values = os.statvfs(filesystem)
    fragment_size = _positive(values.f_frsize, "filesystem fragment size")
    free_bytes = _nonnegative(values.f_bavail, "filesystem available blocks") * fragment_size
    total_bytes = _positive(values.f_blocks, "filesystem total blocks") * fragment_size
    return filesystem, free_bytes, total_bytes


def _require_capacity(free_bytes: int, minimum_free_bytes: int, phase: str) -> None:
    free_bytes = _nonnegative(free_bytes, f"{phase} free bytes")
    minimum_free_bytes = _positive(minimum_free_bytes, f"{phase} minimum free bytes")
    if free_bytes < minimum_free_bytes:
        raise GateError(
            f"{phase} capacity is short by {minimum_free_bytes - free_bytes} bytes: "
            f"free={free_bytes} required={minimum_free_bytes}"
        )


def capacity_snapshot(
    filesystem: Path, minimum_free_bytes: int, phase: str
) -> dict[str, Any]:
    if not phase or re.fullmatch(r"[A-Za-z0-9_.-]+", phase) is None:
        raise GateError("capacity snapshot phase is invalid")
    filesystem, free_bytes, total_bytes = _capacity_free_bytes(filesystem)
    _require_capacity(free_bytes, minimum_free_bytes, phase)
    return {
        "schema": CAPACITY_SNAPSHOT_SCHEMA,
        "phase": phase,
        "filesystem": str(filesystem),
        "recorded_at_ns": time.time_ns(),
        "free_bytes": free_bytes,
        "total_bytes": total_bytes,
        "minimum_free_bytes": minimum_free_bytes,
        "headroom_bytes": free_bytes - minimum_free_bytes,
        "status": "pass",
    }


def _validate_capacity_snapshot(
    document: Any,
    *,
    phase: str,
    filesystem: Path,
    minimum_free_bytes: int,
) -> int:
    fields = {
        "schema",
        "phase",
        "filesystem",
        "recorded_at_ns",
        "free_bytes",
        "total_bytes",
        "minimum_free_bytes",
        "headroom_bytes",
        "status",
    }
    if not isinstance(document, dict) or set(document) != fields:
        raise GateError(f"capacity snapshot has an invalid shape: {phase}")
    free = _nonnegative(document["free_bytes"], f"{phase}.free_bytes")
    total = _positive(document["total_bytes"], f"{phase}.total_bytes")
    recorded = _positive(document["recorded_at_ns"], f"{phase}.recorded_at_ns")
    del recorded
    if (
        document["schema"] != CAPACITY_SNAPSHOT_SCHEMA
        or document["phase"] != phase
        or document["filesystem"] != str(filesystem)
        or document["minimum_free_bytes"] != minimum_free_bytes
        or document["headroom_bytes"] != free - minimum_free_bytes
        or document["status"] != "pass"
        or free > total
    ):
        raise GateError(f"capacity snapshot contract differs: {phase}")
    _require_capacity(free, minimum_free_bytes, phase)
    return free


def check_corpus_capacity(
    summary_path: Path, contract_path: Path, codec: str
) -> dict[str, Any]:
    if codec not in CODECS:
        raise GateError(f"unknown capacity codec: {codec}")
    contract = _load_capacity_contract(contract_path)
    summary = _load_json(summary_path)
    try:
        actual = _nonnegative(summary["size_bytes"], "corpus size_bytes")
        bound = contract["derivation"]["corpus_bound_bytes"][codec]
    except (KeyError, TypeError) as error:
        raise GateError(f"corpus capacity evidence lacks a required field: {error}") from error
    return _corpus_capacity_document(actual, bound, codec)


def _corpus_capacity_document(actual: int, bound: int, codec: str) -> dict[str, Any]:
    actual = _nonnegative(actual, "corpus actual bytes")
    bound = _positive(bound, "corpus bound bytes")
    if actual > bound:
        raise GateError(
            f"{codec} corpus exceeds its mathematical bound by {actual - bound} bytes"
        )
    return {
        "schema": "chronoxide/phase6-corpus-capacity-check/v1",
        "codec": codec,
        "actual_bytes": actual,
        "bound_bytes": bound,
        "headroom_bytes": bound - actual,
        "status": "pass",
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(16 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _regular_file(path: Path, name: str) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as error:
        raise GateError(f"{name} is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise GateError(f"{name} must be a regular non-symlink file: {path}")


def _result_relative(path: Path, result_dir: Path, name: str) -> str:
    try:
        relative = path.absolute().relative_to(result_dir.absolute())
    except ValueError as error:
        raise GateError(f"{name} escapes the result root: {path}") from error
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in ("", ".", "..") for part in relative.parts)
    ):
        raise GateError(f"{name} has an unsafe result-relative path: {path}")
    relative_text = relative.as_posix()
    if "\0" in relative_text or "\n" in relative_text or "\t" in relative_text:
        raise GateError(f"{name} has an unsafe result-relative path: {relative_text!r}")
    return relative_text


def raw_leaf_seal(
    result_dir: Path,
    files: list[Path],
    trees: list[Path],
    output: Path,
) -> None:
    """Seal process outputs before any gate-relevant transform consumes them."""
    result_dir = _canonical_directory(result_dir, "raw leaf result root")
    if not files and not trees:
        raise GateError("raw leaf seal requires at least one file or tree")
    entries: dict[str, dict[str, Any]] = {}

    def add(path: Path, label: str) -> None:
        _regular_file(path, label)
        relative = _result_relative(path, result_dir, label)
        if relative in entries:
            raise GateError(f"duplicate raw leaf path: {relative}")
        metadata = path.stat(follow_symlinks=False)
        entries[relative] = {
            "path": relative,
            "size_bytes": metadata.st_size,
            "sha256": _sha256(path),
        }

    for path in files:
        add(path, "raw leaf")
    tree_roots: list[str] = []
    for tree in trees:
        tree = tree.absolute()
        tree_relative = _result_relative(tree, result_dir, "raw leaf tree")
        if tree_relative in tree_roots:
            raise GateError(f"duplicate raw leaf tree: {tree_relative}")
        tree_roots.append(tree_relative)
        for _relative, path in replay._corpus_files(tree):  # noqa: SLF001 - frozen helper
            add(path, "raw tree leaf")
    canonical_entries = [
        entries[key] for key in sorted(entries, key=lambda item: item.encode())
    ]
    _write_json(
        output,
        {
            "schema": RAW_LEAF_SEAL_SCHEMA,
            "result_dir": str(result_dir),
            "trees": sorted(tree_roots, key=lambda item: item.encode()),
            "file_count": len(canonical_entries),
            "total_bytes": sum(item["size_bytes"] for item in canonical_entries),
            "files": canonical_entries,
        },
    )
    output.chmod(0o444)


def write_invocation(
    binary: Path,
    role: str,
    arguments: list[str],
    assignments: list[str],
    output: Path,
) -> None:
    _regular_file(binary, f"{role} binary")
    environment: dict[str, str] = {}
    for assignment in assignments:
        if "=" not in assignment:
            raise GateError(
                f"invocation environment assignment lacks '=': {assignment}"
            )
        name, value = assignment.split("=", 1)
        if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", name) or name in environment:
            raise GateError(f"invalid or duplicate invocation environment name: {name}")
        environment[name] = value
    if any("\0" in argument for argument in arguments):
        raise GateError("invocation argument contains NUL")
    resolved = binary.resolve(strict=True)
    _write_json(
        output,
        {
            "schema": INVOCATION_SCHEMA,
            "role": role,
            "binary": str(resolved),
            "binary_sha256": _sha256(resolved),
            "argv": [str(resolved), *arguments],
            "environment": environment,
        },
    )
    output.chmod(0o444)


def write_raw_authorities(
    result_dir: Path,
    entries: list[str],
    output: Path,
    checksum_output: Path,
) -> None:
    result_dir = _canonical_directory(result_dir, "raw authority result root")
    expected_output = result_dir / "metadata" / "raw-authorities.tsv"
    expected_checksum = result_dir / "metadata" / "raw-authorities.sha256"
    if (
        output.absolute() != expected_output.absolute()
        or checksum_output.absolute() != expected_checksum.absolute()
    ):
        raise GateError("raw authority outputs are not in their canonical result paths")
    rows: list[tuple[str, str]] = []
    observed: set[str] = set()
    for encoded in entries:
        relative, separator, digest = encoded.rpartition("=")
        if not separator or relative in observed:
            raise GateError("raw authority entry is malformed or duplicated")
        pure = PurePosixPath(relative)
        if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
            raise GateError(f"raw authority entry has an unsafe path: {relative!r}")
        digest = _digest(digest, f"{relative}.sha256")
        seal = result_dir / Path(*pure.parts)
        _regular_file(seal, "raw leaf seal")
        if _sha256(seal) != digest:
            raise GateError(f"raw authority creation-time digest differs: {relative}")
        seal_document = _load_json(seal)
        if (
            not isinstance(seal_document, dict)
            or seal_document.get("schema") != RAW_LEAF_SEAL_SCHEMA
            or seal_document.get("result_dir") != str(result_dir)
        ):
            raise GateError(f"raw authority references an invalid seal: {relative}")
        observed.add(relative)
        rows.append((relative, digest))
    if not rows:
        raise GateError("raw authority requires at least one seal")
    if os.path.lexists(output) or os.path.lexists(checksum_output):
        raise GateError("raw authority outputs already exist")
    with output.open("x", encoding="utf-8", newline="") as destination:
        destination.write("path\tsha256\n")
        for relative, digest in rows:
            destination.write(f"{relative}\t{digest}\n")
    output.chmod(0o444)
    authority_digest = _sha256(output)
    with checksum_output.open("x", encoding="utf-8") as destination:
        destination.write(f"{authority_digest}  {output}\n")
    checksum_output.chmod(0o444)


def check_raw_leaf_seal(result_dir: Path, seal_path: Path) -> None:
    expected = _load_json(seal_path)
    if not isinstance(expected, dict) or expected.get("schema") != RAW_LEAF_SEAL_SCHEMA:
        raise GateError(f"raw leaf seal has the wrong schema: {seal_path}")
    if expected.get("result_dir") != str(result_dir.resolve(strict=True)):
        raise GateError(f"raw leaf seal result binding differs: {seal_path}")
    files = expected.get("files")
    trees = expected.get("trees")
    if not isinstance(files, list) or not isinstance(trees, list):
        raise GateError(f"raw leaf seal has an invalid shape: {seal_path}")
    declared_paths: set[str] = set()
    for entry in files:
        if not isinstance(entry, dict) or set(entry) != {
            "path",
            "size_bytes",
            "sha256",
        }:
            raise GateError(f"raw leaf seal entry has an invalid shape: {seal_path}")
        relative = entry["path"]
        if not isinstance(relative, str):
            raise GateError(f"raw leaf seal path is invalid: {seal_path}")
        pure = PurePosixPath(relative)
        if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
            raise GateError(f"raw leaf seal path is unsafe: {relative!r}")
        if relative in declared_paths:
            raise GateError(f"raw leaf seal contains a duplicate path: {relative}")
        declared_paths.add(relative)
        path = result_dir / Path(*pure.parts)
        _regular_file(path, "sealed raw leaf")
        metadata = path.stat(follow_symlinks=False)
        if metadata.st_size != _nonnegative(
            entry["size_bytes"], f"{relative}.size_bytes"
        ):
            raise GateError(f"sealed raw leaf size changed: {relative}")
        if _sha256(path) != _digest(entry["sha256"], f"{relative}.sha256"):
            raise GateError(f"sealed raw leaf content changed: {relative}")
    actual_tree_files: set[str] = set()
    for relative in trees:
        if not isinstance(relative, str):
            raise GateError(f"raw leaf tree path is invalid: {seal_path}")
        pure = PurePosixPath(relative)
        if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
            raise GateError(f"raw leaf tree path is unsafe: {relative!r}")
        tree = result_dir / Path(*pure.parts)
        for _child_relative, path in replay._corpus_files(tree):  # noqa: SLF001
            actual_tree_files.add(_result_relative(path, result_dir, "raw tree leaf"))
    declared_tree_files = {
        relative
        for relative in declared_paths
        if any(relative == tree or relative.startswith(f"{tree}/") for tree in trees)
    }
    if actual_tree_files != declared_tree_files:
        missing = sorted(declared_tree_files - actual_tree_files)
        extra = sorted(actual_tree_files - declared_tree_files)
        raise GateError(
            f"sealed raw tree path set changed: missing={missing[:1]!r} extra={extra[:1]!r}"
        )
    if expected.get("file_count") != len(files):
        raise GateError(f"raw leaf seal file count differs: {seal_path}")
    if expected.get("total_bytes") != sum(entry["size_bytes"] for entry in files):
        raise GateError(f"raw leaf seal byte total differs: {seal_path}")


ADMISSION_PLAN_FIELDS = {
    "schema",
    "result_dir",
    "capture",
    "repo",
    "query_manifest",
    "config_template",
    "validated_input_config_template",
    "expectations",
    "binary_provenance_mode",
    "promotion_eligibility",
    "stop_after_messages",
    "replay_blocks",
    "query_blocks",
    "benchmark_repeats",
    "rss_interval_ms",
    "guard_interval_ms",
    "capacity_monitor_interval_ms",
    "page_size_bytes",
    "max_capture_resident_bytes_after_evict",
    "max_corpus_resident_bytes_after_evict",
    "max_dirty_writeback_bytes",
    "capacity_contract_sha256",
    "readback_sample_limit_per_kind",
    "rust_log",
    "perf_stat_mode",
    "perf_binary",
    "perf_binary_sha256",
    "perf_version",
    "chunk_read_queue_depth",
    "query_label_arena_max_bytes",
    "query_max_series_matched",
    "query_max_projected_series",
    "query_max_chunks_read",
    "query_max_bytes_read",
    "query_max_samples",
    "regex_max_expanded_values",
}


def _validate_perf_tool_identity(
    mode: str, binary: Any, binary_sha256: Any, version: Any
) -> None:
    if mode == "off":
        if (binary, binary_sha256, version) != ("-", "-", "-"):
            raise GateError("disabled perf mode requires the explicit '-' tool tuple")
        return
    if not isinstance(binary, str) or not binary:
        raise GateError("enabled perf mode lacks a perf binary path")
    candidate = Path(binary)
    canonical = _canonical_file(candidate, "perf binary")
    if binary != str(canonical) or not os.access(canonical, os.X_OK):
        raise GateError("perf binary must be a canonical absolute executable")
    digest = _digest(binary_sha256, "perf_binary_sha256")
    if _sha256(canonical) != digest:
        raise GateError("perf binary digest differs from the admission plan")
    if (
        not isinstance(version, str)
        or not version
        or any(character in version for character in ("\0", "\n", "\r", "\t"))
    ):
        raise GateError("perf version must be one non-empty safe line")
    try:
        completed = subprocess.run(
            [str(canonical), "--version"],
            check=False,
            capture_output=True,
            text=True,
            env={"LC_ALL": "C", "TZ": "UTC"},
        )
    except OSError as error:
        raise GateError(f"cannot execute the admission-plan perf binary: {error}") from error
    if completed.returncode != 0 or completed.stdout.splitlines() != [version]:
        raise GateError("perf binary version differs from the admission plan")


def write_admission_plan(args: argparse.Namespace) -> None:
    result_dir = _canonical_directory(args.result_dir, "admission-plan result root")
    capture = _canonical_directory(args.capture, "admission-plan capture root")
    repo = _canonical_directory(args.repo, "admission-plan source root")
    query_manifest = _canonical_file(
        args.query_manifest, "admission-plan query manifest"
    )
    config_template = _canonical_file(
        args.config_template, "admission-plan config template"
    )
    validated_input_config_template = _canonical_file(
        args.validated_input_config_template,
        "admission-plan validated input config template",
    )
    expectations = _canonical_file(args.expectations, "admission-plan expectations")
    if args.binary_provenance_mode == "internal":
        expected_eligibility = "formal_source_bound"
    else:
        expected_eligibility = "exploratory_non_promotable_external_binaries"
    if args.promotion_eligibility != expected_eligibility:
        raise GateError("admission plan provenance and promotion eligibility disagree")
    if args.binary_provenance_mode == "internal" and args.perf_stat_mode != "required":
        raise GateError("formal source-bound admission requires perf_stat_mode=required")
    positive_fields = {
        "stop_after_messages": args.stop_after_messages,
        "replay_blocks": args.replay_blocks,
        "query_blocks": args.query_blocks,
        "benchmark_repeats": args.benchmark_repeats,
        "rss_interval_ms": args.rss_interval_ms,
        "guard_interval_ms": args.guard_interval_ms,
        "capacity_monitor_interval_ms": args.capacity_monitor_interval_ms,
        "page_size_bytes": args.page_size_bytes,
        "readback_sample_limit_per_kind": args.readback_sample_limit_per_kind,
        "chunk_read_queue_depth": args.chunk_read_queue_depth,
        "query_label_arena_max_bytes": args.query_label_arena_max_bytes,
        "query_max_series_matched": args.query_max_series_matched,
        "query_max_projected_series": args.query_max_projected_series,
        "query_max_chunks_read": args.query_max_chunks_read,
        "query_max_bytes_read": args.query_max_bytes_read,
        "query_max_samples": args.query_max_samples,
        "regex_max_expanded_values": args.regex_max_expanded_values,
    }
    for name, value in positive_fields.items():
        _positive(value, name)
    fixed_measurement_preconditions = {
        "max_capture_resident_bytes_after_evict": (
            args.max_capture_resident_bytes_after_evict,
            FIXED_MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT,
        ),
        "max_corpus_resident_bytes_after_evict": (
            args.max_corpus_resident_bytes_after_evict,
            FIXED_MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT,
        ),
        "max_dirty_writeback_bytes": (
            args.max_dirty_writeback_bytes,
            FIXED_MAX_DIRTY_WRITEBACK_BYTES,
        ),
    }
    for name, (value, expected) in fixed_measurement_preconditions.items():
        _nonnegative(value, name)
        if value != expected:
            raise GateError(
                f"admission plan {name} must be exactly {expected}"
            )
    if args.stop_after_messages != PINNED_STOP_AFTER_MESSAGES:
        raise GateError("admission plan must use the pinned four-million-message prefix")
    if args.replay_blocks != PINNED_REPLAY_BLOCKS:
        raise GateError("admission plan must use two replay blocks")
    if args.query_blocks != PINNED_QUERY_BLOCKS:
        raise GateError("admission plan must use two query blocks")
    if args.benchmark_repeats != FIXED_BENCHMARK_REPEATS:
        raise GateError("admission plan must use three benchmark repeats")
    if args.rss_interval_ms != FIXED_RSS_INTERVAL_MS:
        raise GateError("admission plan RSS interval differs from 100 ms")
    if args.guard_interval_ms != FIXED_GUARD_INTERVAL_MS:
        raise GateError("admission plan conflict guard interval differs from 100 ms")
    if args.capacity_monitor_interval_ms != FIXED_CAPACITY_MONITOR_INTERVAL_MS:
        raise GateError("admission plan capacity monitor interval differs from 100 ms")
    if args.query_label_arena_max_bytes != FIXED_QUERY_LABEL_ARENA_MAX_BYTES:
        raise GateError("admission plan CompactIds arena differs from 512 MiB")
    capacity_contract_sha256 = _digest(
        args.capacity_contract_sha256, "capacity_contract_sha256"
    )
    _validate_perf_tool_identity(
        args.perf_stat_mode,
        args.perf_binary,
        args.perf_binary_sha256,
        args.perf_version,
    )
    _write_json(
        args.output,
        {
            "schema": ADMISSION_PLAN_SCHEMA,
            "result_dir": str(result_dir),
            "capture": str(capture),
            "repo": str(repo),
            "query_manifest": str(query_manifest),
            "config_template": str(config_template),
            "validated_input_config_template": str(validated_input_config_template),
            "expectations": str(expectations),
            "binary_provenance_mode": args.binary_provenance_mode,
            "promotion_eligibility": args.promotion_eligibility,
            **positive_fields,
            **{
                name: value
                for name, (value, _expected) in fixed_measurement_preconditions.items()
            },
            "capacity_contract_sha256": capacity_contract_sha256,
            "rust_log": args.rust_log,
            "perf_stat_mode": args.perf_stat_mode,
            "perf_binary": args.perf_binary,
            "perf_binary_sha256": args.perf_binary_sha256,
            "perf_version": args.perf_version,
        },
    )


def _load_admission_plan(result_dir: Path, path: Path) -> dict[str, Any]:
    expected_path = result_dir / "metadata" / "admission-plan.json"
    if path.absolute() != expected_path.absolute():
        raise GateError(f"final admission plan must be {expected_path}")
    plan = _load_json(path)
    if not isinstance(plan, dict) or set(plan) != ADMISSION_PLAN_FIELDS:
        raise GateError("final admission plan has an invalid shape")
    if plan.get("schema") != ADMISSION_PLAN_SCHEMA:
        raise GateError("final admission plan has the wrong schema")
    if plan.get("result_dir") != str(result_dir):
        raise GateError("final admission plan is bound to a different result root")
    for name in ADMISSION_PLAN_FIELDS - {
        "schema",
        "result_dir",
        "capture",
        "repo",
        "query_manifest",
        "config_template",
        "validated_input_config_template",
        "expectations",
        "binary_provenance_mode",
        "promotion_eligibility",
        "rust_log",
        "perf_stat_mode",
        "perf_binary",
        "perf_binary_sha256",
        "perf_version",
        "capacity_contract_sha256",
        "max_capture_resident_bytes_after_evict",
        "max_corpus_resident_bytes_after_evict",
        "max_dirty_writeback_bytes",
    }:
        _positive(plan[name], name)
    fixed_measurement_preconditions = {
        "max_capture_resident_bytes_after_evict": (
            FIXED_MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT
        ),
        "max_corpus_resident_bytes_after_evict": (
            FIXED_MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT
        ),
        "max_dirty_writeback_bytes": FIXED_MAX_DIRTY_WRITEBACK_BYTES,
    }
    for name, expected in fixed_measurement_preconditions.items():
        value = _nonnegative(plan[name], name)
        if value != expected:
            raise GateError(f"final admission plan {name} must be exactly {expected}")
    if plan["binary_provenance_mode"] not in {"internal", "external-exploratory"}:
        raise GateError("final admission plan has an invalid binary provenance mode")
    expected_eligibility = (
        "formal_source_bound"
        if plan["binary_provenance_mode"] == "internal"
        else "exploratory_non_promotable_external_binaries"
    )
    if plan["promotion_eligibility"] != expected_eligibility:
        raise GateError("final admission plan has invalid promotion eligibility")
    if plan["perf_stat_mode"] not in {"off", "auto", "required"}:
        raise GateError("final admission plan has an invalid perf mode")
    if plan["binary_provenance_mode"] == "internal" and plan["perf_stat_mode"] != "required":
        raise GateError("formal source-bound admission requires perf_stat_mode=required")
    _validate_perf_tool_identity(
        plan["perf_stat_mode"],
        plan["perf_binary"],
        plan["perf_binary_sha256"],
        plan["perf_version"],
    )
    if plan["stop_after_messages"] != PINNED_STOP_AFTER_MESSAGES:
        raise GateError("final admission plan does not use the pinned message prefix")
    if plan["replay_blocks"] != PINNED_REPLAY_BLOCKS:
        raise GateError("final admission plan does not use two replay blocks")
    if plan["query_blocks"] != PINNED_QUERY_BLOCKS:
        raise GateError("final admission plan does not use two query blocks")
    if plan["benchmark_repeats"] != FIXED_BENCHMARK_REPEATS:
        raise GateError("final admission plan does not use three benchmark repeats")
    if plan["rss_interval_ms"] != FIXED_RSS_INTERVAL_MS:
        raise GateError("final admission plan has the wrong RSS interval")
    if plan["guard_interval_ms"] != FIXED_GUARD_INTERVAL_MS:
        raise GateError("final admission plan has the wrong conflict guard interval")
    if plan["capacity_monitor_interval_ms"] != FIXED_CAPACITY_MONITOR_INTERVAL_MS:
        raise GateError("final admission plan has the wrong capacity monitor interval")
    if plan["query_label_arena_max_bytes"] != FIXED_QUERY_LABEL_ARENA_MAX_BYTES:
        raise GateError("final admission plan has the wrong CompactIds arena")
    _digest(plan["capacity_contract_sha256"], "capacity_contract_sha256")
    return plan


def _replace_assignment(lines: list[str], section: str, key: str, value: str) -> None:
    current = ""
    matches: list[int] = []
    table = re.compile(r"^\s*\[([^]]+)]\s*(?:#.*)?$")
    assignment = re.compile(rf"^\s*{re.escape(key)}\s*=")
    for index, line in enumerate(lines):
        table_match = table.match(line.rstrip("\n"))
        if table_match:
            current = table_match.group(1).strip()
        elif current == section and assignment.match(line):
            matches.append(index)
    if len(matches) != 1:
        raise GateError(f"template must contain exactly one {section}.{key}; found {len(matches)}")
    newline = "\n" if lines[matches[0]].endswith("\n") else ""
    lines[matches[0]] = f"{key} = {value}{newline}"


def _config_contract(document: dict[str, Any], codec: str) -> dict[str, Any]:
    if codec not in CODECS:
        raise GateError(f"unknown float codec: {codec}")
    try:
        ingestion = document["ingestion"]
        head = ingestion["head_buffer"]
        writer = ingestion["segment_writer"]
    except (KeyError, TypeError) as error:
        raise GateError(f"configuration lacks a required table: {error}") from error
    required = {
        "ingestion.capture_only": (ingestion.get("capture_only"), False),
        "ingestion.head_buffer.enabled": (head.get("enabled"), True),
        "ingestion.head_buffer.float_encoding": (head.get("float_encoding"), codec),
        "ingestion.segment_writer.enabled": (writer.get("enabled"), True),
        "ingestion.segment_writer.float_encoding": (writer.get("float_encoding"), codec),
        "ingestion.segment_writer.storage_schema": (writer.get("storage_schema"), "schema8"),
    }
    for name, (actual, expected) in required.items():
        if actual != expected:
            raise GateError(f"{name} must be {expected!r}; got {actual!r}")
    seed = writer.get("deterministic_id_seed")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise GateError("ingestion.segment_writer.deterministic_id_seed must be an integer")
    return {"ingestion": ingestion, "head": head, "writer": writer}


def _controlled_config(document: dict[str, Any]) -> dict[str, Any]:
    clone = json.loads(json.dumps(document))
    ingestion = clone["ingestion"]
    ingestion["replay_from"] = "<capture>"
    ingestion["stop_after_messages"] = "<stop-after-messages>"
    ingestion["head_buffer"]["float_encoding"] = "<codec>"
    writer = ingestion["segment_writer"]
    writer["segments_dir"] = "<segments-dir>"
    writer["float_encoding"] = "<codec>"
    return clone


def _rendered_config(
    template: Path,
    output: Path,
    capture: Path,
    segments_dir: Path,
    stop_after_messages: int,
    codec: str,
) -> tuple[bytes, dict[str, Any]]:
    _positive(stop_after_messages, "stop_after_messages")
    lines = template.read_text(encoding="utf-8").splitlines(keepends=True)
    _replace_assignment(lines, "ingestion", "replay_from", json.dumps(str(capture)))
    _replace_assignment(
        lines, "ingestion", "stop_after_messages", str(stop_after_messages)
    )
    _replace_assignment(
        lines, "ingestion.head_buffer", "float_encoding", json.dumps(codec)
    )
    _replace_assignment(
        lines, "ingestion.segment_writer", "segments_dir", json.dumps(str(segments_dir))
    )
    _replace_assignment(
        lines, "ingestion.segment_writer", "float_encoding", json.dumps(codec)
    )
    rendered = "".join(lines).encode()
    document = tomllib.loads(rendered.decode("utf-8"))
    tables = _config_contract(document, codec)
    if tables["ingestion"].get("replay_from") != str(capture):
        raise GateError("rendered replay_from differs from the selected capture")
    if tables["ingestion"].get("stop_after_messages") != stop_after_messages:
        raise GateError("rendered stop_after_messages differs")
    if tables["writer"].get("segments_dir") != str(segments_dir):
        raise GateError("rendered segments_dir differs")
    controlled = _controlled_config(document)
    controlled_sha256 = hashlib.sha256(
        json.dumps(controlled, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return rendered, {
        "schema": "chronoxide/phase6-codec-rendered-config/v1",
        "config": str(output),
        "sha256": hashlib.sha256(rendered).hexdigest(),
        "codec": codec,
        "capture": str(capture),
        "segments_dir": str(segments_dir),
        "stop_after_messages": stop_after_messages,
        "controlled_config_sha256": controlled_sha256,
    }


def render_config(
    template: Path,
    output: Path,
    capture: Path,
    segments_dir: Path,
    stop_after_messages: int,
    codec: str,
) -> dict[str, Any]:
    if output.exists() or segments_dir.exists():
        raise GateError("rendered config and segment output must both be fresh")
    rendered, metadata = _rendered_config(
        template,
        output,
        capture,
        segments_dir,
        stop_after_messages,
        codec,
    )
    with output.open("xb") as destination:
        destination.write(rendered)
    return metadata


def capture_inventory(capture: Path, output: Path, paths_output: Path) -> None:
    if not capture.is_absolute():
        raise GateError("capture must be an absolute non-symlink directory")
    try:
        capture_metadata = capture.lstat()
    except OSError as error:
        raise GateError(
            f"cannot inspect capture directory {capture}: {error}"
        ) from error
    if stat.S_ISLNK(capture_metadata.st_mode) or not stat.S_ISDIR(
        capture_metadata.st_mode
    ):
        raise GateError("capture must be an absolute non-symlink directory")
    rows: list[dict[str, Any]] = []
    paths: list[Path] = []
    pending = [Path()]
    while pending:
        relative_directory = pending.pop()
        directory = capture / relative_directory
        try:
            with os.scandir(directory) as entries:
                ordered_entries = sorted(
                    entries, key=lambda entry: os.fsencode(entry.name)
                )
        except OSError as error:
            raise GateError(
                f"cannot enumerate capture directory {directory}: {error}"
            ) from error
        child_directories: list[Path] = []
        for entry in ordered_entries:
            relative = relative_directory / entry.name
            if relative.is_absolute() or any(
                part in ("", ".", "..") for part in relative.parts
            ):
                raise GateError(f"capture path escapes its root: {relative!s}")
            path = capture / relative
            try:
                metadata = entry.stat(follow_symlinks=False)
            except OSError as error:
                raise GateError(
                    f"cannot inspect capture entry {path}: {error}"
                ) from error
            if stat.S_ISLNK(metadata.st_mode):
                raise GateError(f"capture contains a symbolic link: {path}")
            if stat.S_ISDIR(metadata.st_mode):
                child_directories.append(relative)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise GateError(f"capture entry is not a regular file: {path}")
            relative_text = relative.as_posix()
            if "\n" in relative_text or "\t" in relative_text:
                raise GateError("capture path contains a tab or newline")
            rows.append(
                {
                    "path": relative_text,
                    "size_bytes": metadata.st_size,
                    "sha256": _sha256(path),
                }
            )
            paths.append(path)
        pending.extend(reversed(child_directories))
    rows.sort(key=lambda row: row["path"].encode())
    paths.sort(key=lambda path: path.relative_to(capture).as_posix().encode())
    if not rows:
        raise GateError("capture contains no regular files")
    canonical = json.dumps(rows, separators=(",", ":"), sort_keys=True).encode()
    _write_json(
        output,
        {
            "schema": CAPTURE_INVENTORY_SCHEMA,
            "capture": str(capture),
            "file_count": len(rows),
            "total_bytes": sum(row["size_bytes"] for row in rows),
            "files_sha256": hashlib.sha256(canonical).hexdigest(),
            "files": rows,
        },
    )
    if paths_output.exists():
        raise GateError(f"refusing to reuse output: {paths_output}")
    with paths_output.open("xb") as destination:
        for path in paths:
            destination.write(os.fsencode(path))
            destination.write(b"\0")


def artifact_inventory(corpus: Path, output: Path) -> None:
    by_name: dict[str, dict[str, int]] = {}
    file_count = 0
    total_bytes = 0
    for _relative, path in replay._corpus_files(corpus):  # noqa: SLF001 - frozen local helper
        size = path.stat().st_size
        row = by_name.setdefault(path.name, {"files": 0, "bytes": 0})
        row["files"] += 1
        row["bytes"] += size
        file_count += 1
        total_bytes += size
    _write_json(
        output,
        {
            "schema": ARTIFACT_INVENTORY_SCHEMA,
            "corpus": str(corpus),
            "file_count": file_count,
            "total_bytes": total_bytes,
            "by_basename": dict(sorted(by_name.items())),
        },
    )


def parse_replay_report(report: Path, output: Path) -> None:
    _write_json(output, ab_gate.parse_replay_report(report))


def _parse_fields(line: str) -> dict[str, int]:
    fields: dict[str, int] = {}
    for key, value in re.findall(r"\b([a-z][a-z0-9_]*)=([0-9]+)\b", line):
        fields[key] = int(value)
    return fields


def parse_seal_log(log: Path, output: Path) -> None:
    window_required = {
        "datapoints",
        "series",
        "elapsed_ms",
        "seal_decode_ms",
        "record_samples_ms",
        "record_wall_ms",
        "record_chunk_append_ms",
        "record_chunks",
        "record_profile_samples",
        "writer_flush_ms",
    }
    segment_required = {
        "datapoints",
        "series",
        "elapsed_ms",
        "chunks_flush_ms",
        "chunk_index_ms",
        "series_ms",
        "indexes_ms",
        "footer_ms",
        "publish_ms",
        "total_bytes",
        "data_bytes",
        "metadata_bytes",
        "chunks_bytes",
    }
    windows: list[dict[str, int]] = []
    segments: list[dict[str, int]] = []
    for line in log.read_text(encoding="utf-8", errors="strict").splitlines():
        if "Head window written" in line:
            fields = _parse_fields(line)
            missing = window_required - fields.keys()
            if missing:
                raise GateError(f"Head window log row lacks fields: {sorted(missing)}")
            windows.append(fields)
        elif "Segment flushed" in line:
            fields = _parse_fields(line)
            missing = segment_required - fields.keys()
            if missing:
                raise GateError(f"segment flush log row lacks fields: {sorted(missing)}")
            segments.append(fields)
    if not windows:
        raise GateError("replay log contains no Head window written telemetry")

    def totals(rows: list[dict[str, int]]) -> dict[str, int]:
        names = sorted({name for row in rows for name in row})
        return {name: sum(row.get(name, 0) for row in rows) for name in names}

    _write_json(
        output,
        {
            "schema": "chronoxide/phase6-codec-seal-telemetry/v1",
            "head_windows": {"count": len(windows), "totals": totals(windows)},
            "segments": {"count": len(segments), "totals": totals(segments)},
            "segment_stage_telemetry_available": bool(segments),
        },
    )


def _read_manifest(path: Path) -> dict[str, tuple[str, int]]:
    with path.open(newline="", encoding="utf-8") as source:
        rows = csv.DictReader(source, delimiter="\t")
        if rows.fieldnames != ["sha256", "size_bytes", "path"]:
            raise GateError(f"invalid corpus manifest header: {path}")
        result: dict[str, tuple[str, int]] = {}
        for row in rows:
            relative = row["path"]
            if relative in result:
                raise GateError(f"duplicate corpus path: {relative}")
            result[relative] = (_digest(row["sha256"], f"{path}:{relative}"), int(row["size_bytes"]))
    return result


def _read_tsv(path: Path, expected: list[str]) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source, delimiter="\t")
        rows = list(reader)
    if reader.fieldnames != expected or any(list(row) != expected for row in rows):
        raise GateError(f"{path} has an invalid TSV shape")
    return rows


RESIDENCY_EVIDENCE_FIELDS = [
    "phase",
    "sequence",
    "row_kind",
    "resident_bytes",
    "size_bytes",
    "ceiling_bytes",
    "path",
]
WRITEBACK_EVIDENCE_FIELDS = [
    "phase",
    "attempt",
    "recorded_at",
    "dirty_kib",
    "writeback_kib",
    "total_bytes",
    "ceiling_bytes",
    "status",
]


def _canonical_decimal(value: str, name: str) -> int:
    if not re.fullmatch(r"0|[1-9][0-9]*", value):
        raise GateError(f"{name} must be a canonical non-negative decimal integer")
    return int(value)


def _read_nul_inventory_paths(path: Path) -> list[Path]:
    _regular_file(path, "residency path inventory")
    encoded = path.read_bytes()
    if not encoded or not encoded.endswith(b"\0"):
        raise GateError("residency path inventory is empty or lacks a NUL terminator")
    fields = encoded.removesuffix(b"\0").split(b"\0")
    if not fields or any(not field for field in fields):
        raise GateError("residency path inventory contains an empty path")
    paths: list[Path] = []
    observed: set[str] = set()
    for field in fields:
        try:
            value = field.decode("utf-8")
        except UnicodeDecodeError as error:
            raise GateError("residency path inventory contains non-UTF-8 data") from error
        if any(character in value for character in ("\0", "\n", "\r", "\t")):
            raise GateError("residency path inventory contains an unsafe path")
        candidate = Path(value)
        if not candidate.is_absolute() or value in observed:
            raise GateError("residency path inventory contains a relative or duplicate path")
        _regular_file(candidate, "inventoried residency file")
        if str(candidate.resolve(strict=True)) != value:
            raise GateError("residency path inventory contains a non-canonical path")
        observed.add(value)
        paths.append(candidate)
    return paths


def validate_residency_evidence(
    path: Path,
    phase: str,
    paths_file: Path,
    ceiling_bytes: int | None,
    page_size_bytes: int,
) -> dict[str, Any]:
    """Reconstruct one fincore snapshot from its authoritative path inventory."""
    _regular_file(path, "residency evidence")
    if not phase or any(character in phase for character in ("\0", "\n", "\r", "\t")):
        raise GateError("residency phase is empty or unsafe")
    if ceiling_bytes is not None:
        ceiling_bytes = _nonnegative(ceiling_bytes, "residency ceiling bytes")
    page_size_bytes = _positive(page_size_bytes, "residency page size bytes")
    ceiling_text = "-" if ceiling_bytes is None else str(ceiling_bytes)
    expected_paths = _read_nul_inventory_paths(paths_file)
    rows = _read_tsv(path, RESIDENCY_EVIDENCE_FIELDS)
    if len(rows) != len(expected_paths) + 1:
        raise GateError("residency evidence row count differs from its path inventory")
    resident_total = 0
    size_total = 0
    for sequence, (row, expected_path) in enumerate(
        zip(rows[:-1], expected_paths, strict=True), 1
    ):
        if (
            row["phase"] != phase
            or row["sequence"] != str(sequence)
            or row["row_kind"] != "file"
            or row["ceiling_bytes"] != ceiling_text
            or row["path"] != str(expected_path)
        ):
            raise GateError("residency evidence phase, sequence, ceiling, or path differs")
        resident = _canonical_decimal(
            row["resident_bytes"], f"{phase} residency row {sequence} resident_bytes"
        )
        size = _canonical_decimal(
            row["size_bytes"], f"{phase} residency row {sequence} size_bytes"
        )
        if size != expected_path.stat(follow_symlinks=False).st_size:
            raise GateError("residency evidence file size differs from the inventoried file")
        maximum_resident = (
            0
            if size == 0
            else ((size + page_size_bytes - 1) // page_size_bytes) * page_size_bytes
        )
        if resident % page_size_bytes or resident > maximum_resident:
            raise GateError(
                "residency evidence is not page-granular or exceeds its "
                "page-rounded file size"
            )
        resident_total += resident
        size_total += size
    total_row = rows[-1]
    if (
        total_row["phase"] != phase
        or total_row["sequence"] != str(len(rows))
        or total_row["row_kind"] != "total"
        or total_row["ceiling_bytes"] != ceiling_text
        or total_row["path"] != "-"
        or _canonical_decimal(
            total_row["resident_bytes"], f"{phase} total resident_bytes"
        )
        != resident_total
        or _canonical_decimal(total_row["size_bytes"], f"{phase} total size_bytes")
        != size_total
    ):
        raise GateError("residency evidence total row differs from reconstructed sums")
    if ceiling_bytes is not None and resident_total > ceiling_bytes:
        raise GateError(
            f"{phase} resident bytes exceed the admission ceiling by "
            f"{resident_total - ceiling_bytes} bytes"
        )
    return {
        "phase": phase,
        "sha256": _sha256(path),
        "file_count": len(expected_paths),
        "resident_bytes": resident_total,
        "size_bytes": size_total,
        "ceiling_bytes": ceiling_bytes,
        "page_size_bytes": page_size_bytes,
        "status": "observed" if ceiling_bytes is None else "pass",
    }


def validate_writeback_evidence(
    path: Path, phase: str, ceiling_bytes: int
) -> dict[str, Any]:
    """Validate that polling stopped on the first Dirty+Writeback passing sample."""
    _regular_file(path, "writeback evidence")
    if not phase or any(character in phase for character in ("\0", "\n", "\r", "\t")):
        raise GateError("writeback phase is empty or unsafe")
    ceiling_bytes = _nonnegative(ceiling_bytes, "writeback ceiling bytes")
    rows = _read_tsv(path, WRITEBACK_EVIDENCE_FIELDS)
    if not rows or len(rows) > 30:
        raise GateError("writeback evidence must contain between one and 30 samples")
    maximum_total = 0
    final_dirty = 0
    final_writeback = 0
    final_total = 0
    previous_recorded_at: datetime | None = None
    timestamp_pattern = re.compile(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2},"
        r"[0-9]{9}[+-][0-9]{2}:[0-9]{2}"
    )
    for attempt, row in enumerate(rows, 1):
        if (
            row["phase"] != phase
            or row["attempt"] != str(attempt)
            or row["ceiling_bytes"] != str(ceiling_bytes)
            or not timestamp_pattern.fullmatch(row["recorded_at"])
        ):
            raise GateError("writeback evidence phase, attempt, timestamp, or ceiling differs")
        try:
            recorded_at = datetime.fromisoformat(row["recorded_at"].replace(",", "."))
        except ValueError as error:
            raise GateError("writeback evidence timestamp is not a real datetime") from error
        if recorded_at.utcoffset() is None:
            raise GateError("writeback evidence timestamp lacks a UTC offset")
        if previous_recorded_at is not None and recorded_at <= previous_recorded_at:
            raise GateError("writeback evidence timestamps are not strictly increasing")
        previous_recorded_at = recorded_at
        dirty = _canonical_decimal(row["dirty_kib"], f"{phase} attempt {attempt} Dirty")
        writeback = _canonical_decimal(
            row["writeback_kib"], f"{phase} attempt {attempt} Writeback"
        )
        total = _canonical_decimal(
            row["total_bytes"], f"{phase} attempt {attempt} total bytes"
        )
        if total != (dirty + writeback) * 1024:
            raise GateError("writeback evidence total does not match Dirty+Writeback")
        final = attempt == len(rows)
        expected_status = "pass" if final else "retry"
        if row["status"] != expected_status:
            raise GateError("writeback evidence has an invalid terminal status sequence")
        if final and total > ceiling_bytes:
            raise GateError("writeback terminal sample exceeds its admission ceiling")
        if not final and total <= ceiling_bytes:
            raise GateError("writeback polling continued after a passing sample")
        maximum_total = max(maximum_total, total)
        final_dirty = dirty
        final_writeback = writeback
        final_total = total
    return {
        "phase": phase,
        "sha256": _sha256(path),
        "samples": len(rows),
        "maximum_total_bytes": maximum_total,
        "final_dirty_kib": final_dirty,
        "final_writeback_kib": final_writeback,
        "final_total_bytes": final_total,
        "ceiling_bytes": ceiling_bytes,
        "status": "pass",
    }


def _expected_abba(block: int) -> tuple[str, str, str, str]:
    return ("raw", "gorilla", "gorilla", "raw") if block % 2 else (
        "gorilla",
        "raw",
        "raw",
        "gorilla",
    )


def compare_replays(index: Path, blocks: int, output: Path, summary: Path) -> None:
    fields = [
        "label",
        "block",
        "slot",
        "codec",
        "config_json",
        "correctness_json",
        "manifest_tsv",
        "corpus_summary_json",
        "time_json",
        "rss_json",
        "seal_json",
        "perf_json",
    ]
    rows = _read_tsv(index, fields)
    expected_count = blocks * 4
    if len(rows) != expected_count:
        raise GateError(f"expected {expected_count} replay rows, found {len(rows)}")
    labels: set[str] = set()
    coordinates: set[tuple[int, int]] = set()
    correctness: Any | None = None
    controlled_config: str | None = None
    manifests: dict[str, list[dict[str, tuple[str, int]]]] = {codec: [] for codec in CODECS}
    manifest_digests: dict[str, set[str]] = {codec: set() for codec in CODECS}
    representative: dict[str, dict[str, str]] = {}
    summary_rows: list[dict[str, Any]] = []
    for row in rows:
        label = row["label"]
        if label in labels:
            raise GateError(f"duplicate replay label: {label}")
        labels.add(label)
        block = _positive(int(row["block"]), "block")
        slot = _positive(int(row["slot"]), "slot")
        if block > blocks or slot > 4:
            raise GateError(f"replay coordinate is outside the declared schedule: {label}")
        coordinate = (block, slot)
        if coordinate in coordinates:
            raise GateError(f"duplicate replay coordinate: block={block}, slot={slot}")
        coordinates.add(coordinate)
        codec = row["codec"]
        if codec != _expected_abba(block)[slot - 1]:
            raise GateError(f"replay schedule is not counterbalanced ABBA at {label}")
        config = _load_json(Path(row["config_json"]))
        if config.get("codec") != codec:
            raise GateError(f"rendered config codec differs at {label}")
        config_fingerprint = _digest(config.get("controlled_config_sha256"), f"{label}.config")
        if controlled_config is None:
            controlled_config = config_fingerprint
        elif config_fingerprint != controlled_config:
            raise GateError(f"controlled configuration differs at {label}")
        actual_correctness = _load_json(Path(row["correctness_json"]))
        if correctness is None:
            correctness = actual_correctness
        elif actual_correctness != correctness:
            raise GateError(f"replay correctness counters differ at {label}")
        corpus_summary = _load_json(Path(row["corpus_summary_json"]))
        manifest_digest = _digest(corpus_summary.get("manifest_sha256"), f"{label}.manifest")
        manifest_digests[codec].add(manifest_digest)
        manifest = _read_manifest(Path(row["manifest_tsv"]))
        manifests[codec].append(manifest)
        representative.setdefault(codec, row)
        timing = _load_json(Path(row["time_json"]))
        rss = _load_json(Path(row["rss_json"]))
        seal = _load_json(Path(row["seal_json"]))
        perf = None if row["perf_json"] == "-" else _load_json(Path(row["perf_json"]))
        if timing.get("exit_status") != 0:
            raise GateError(f"GNU time reports nonzero status at {label}")
        summary_rows.append(
            {
                "label": label,
                "block": block,
                "slot": slot,
                "codec": codec,
                "elapsed": timing["elapsed"],
                "user_seconds": timing["user_seconds"],
                "system_seconds": timing["system_seconds"],
                "time_max_rss_kib": timing["max_rss_kib"],
                "process_tree_peak_rss_kib": rss["aggregate_rss_kib"],
                "corpus_files": corpus_summary["file_count"],
                "corpus_bytes": corpus_summary["size_bytes"],
                "corpus_manifest_sha256": manifest_digest,
                "head_window_elapsed_ms": seal["head_windows"]["totals"]["elapsed_ms"],
                "seal_decode_ms": seal["head_windows"]["totals"]["seal_decode_ms"],
                "record_samples_ms": seal["head_windows"]["totals"]["record_samples_ms"],
                "writer_flush_ms": seal["head_windows"]["totals"]["writer_flush_ms"],
                "perf_available": perf is not None,
            }
        )
    expected_coordinates = {
        (block, slot)
        for block in range(1, blocks + 1)
        for slot in range(1, 5)
    }
    if coordinates != expected_coordinates:
        raise GateError("replay schedule does not cover every declared (block, slot) coordinate")
    for codec, values in manifest_digests.items():
        if len(values) != 1:
            raise GateError(f"{codec} replay corpora are not byte deterministic")
        first = manifests[codec][0]
        if any(value != first for value in manifests[codec][1:]):
            raise GateError(f"{codec} replay file manifests differ")
    raw_manifest = manifests["raw"][0]
    gorilla_manifest = manifests["gorilla"][0]
    if raw_manifest.keys() != gorilla_manifest.keys():
        raise GateError("Raw and Gorilla corpora have different path sets or segment IDs")
    differences: list[dict[str, Any]] = []
    for relative in sorted(raw_manifest):
        if raw_manifest[relative] == gorilla_manifest[relative]:
            continue
        artifact = Path(relative).name
        if artifact not in ALLOWED_CROSS_CODEC_ARTIFACTS:
            raise GateError(f"unexpected Raw/Gorilla byte difference: {relative}")
        differences.append(
            {
                "path": relative,
                "artifact": artifact,
                "raw_sha256": raw_manifest[relative][0],
                "raw_bytes": raw_manifest[relative][1],
                "gorilla_sha256": gorilla_manifest[relative][0],
                "gorilla_bytes": gorilla_manifest[relative][1],
            }
        )
    if not any(row["artifact"] == "chunks.bin" for row in differences):
        raise GateError("Raw/Gorilla replay did not change any chunks.bin")
    summary_fields = list(summary_rows[0])
    with summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=summary_fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(summary_rows)
    _write_json(
        output,
        {
            "schema": REPLAY_COMPARISON_SCHEMA,
            "status": "pass",
            "blocks": blocks,
            "schedule": "odd=raw,gorilla,gorilla,raw; even=reversed",
            "controlled_config_sha256": controlled_config,
            "replay_correctness_sha256": hashlib.sha256(
                json.dumps(correctness, separators=(",", ":"), sort_keys=True).encode()
            ).hexdigest(),
            "deterministic_corpus_sha256": {
                codec: next(iter(values)) for codec, values in manifest_digests.items()
            },
            "representative_labels": {codec: row["label"] for codec, row in representative.items()},
            "cross_codec_differences": differences,
        },
    )


def _winner(value: Any, name: str) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != {"chunks", "points"}:
        raise GateError(f"{name} has an invalid winner-total shape")
    return {key: _nonnegative(item, f"{name}.{key}") for key, item in value.items()}


def _validate_histogram(value: Any, name: str, expected_observations: int) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"zero_count", "buckets"}:
        raise GateError(f"{name} has an invalid power-of-two histogram shape")
    zero_count = _nonnegative(value["zero_count"], f"{name}.zero_count")
    buckets = value["buckets"]
    if not isinstance(buckets, list):
        raise GateError(f"{name}.buckets must be a list")
    normalized: list[dict[str, int]] = []
    previous_upper = 0
    observations = zero_count
    for index, bucket in enumerate(buckets):
        bucket_name = f"{name}.buckets[{index}]"
        if not isinstance(bucket, dict) or set(bucket) != {
            "lower_inclusive", "upper_inclusive", "count"
        }:
            raise GateError(f"{bucket_name} has an invalid shape")
        lower = _positive(bucket["lower_inclusive"], f"{bucket_name}.lower_inclusive")
        upper = _positive(bucket["upper_inclusive"], f"{bucket_name}.upper_inclusive")
        count = _positive(bucket["count"], f"{bucket_name}.count")
        if lower & (lower - 1):
            raise GateError(f"{bucket_name}.lower_inclusive is not a power of two")
        if upper != 2 * lower - 1:
            raise GateError(f"{bucket_name} is not an exact [2^n, 2^(n+1)-1] bucket")
        if lower <= previous_upper:
            raise GateError(f"{name}.buckets are not strictly ascending and disjoint")
        previous_upper = upper
        observations += count
        normalized.append(
            {"lower_inclusive": lower, "upper_inclusive": upper, "count": count}
        )
    if observations != expected_observations:
        raise GateError(
            f"{name} observations do not reconcile: expected {expected_observations}, "
            f"found {observations}"
        )
    return {"zero_count": zero_count, "buckets": normalized}


def _validate_candidate(candidate: Any, name: str) -> dict[str, Any]:
    required = {"bytes", "unique_wins", "adaptive_selections"}
    if not isinstance(candidate, dict) or set(candidate) != required:
        raise GateError(f"{name} has an invalid timestamp-candidate shape")
    return {
        "bytes": _nonnegative(candidate["bytes"], f"{name}.bytes"),
        "unique_wins": _winner(candidate["unique_wins"], f"{name}.unique_wins"),
        "adaptive_selections": _winner(candidate["adaptive_selections"], f"{name}.adaptive_selections"),
    }


def _validate_timestamp_evidence(value: Any, name: str) -> dict[str, Any]:
    candidates = TIMESTAMP_CANDIDATES
    required = {"chunks", "points", "adaptive_min_bytes", "tied_minima", *candidates}
    if not isinstance(value, dict) or set(value) != required:
        raise GateError(f"{name} has an invalid timestamp evidence shape")
    result = {
        "chunks": _nonnegative(value["chunks"], f"{name}.chunks"),
        "points": _nonnegative(value["points"], f"{name}.points"),
        "adaptive_min_bytes": _nonnegative(value["adaptive_min_bytes"], f"{name}.adaptive_min_bytes"),
        "tied_minima": _winner(value["tied_minima"], f"{name}.tied_minima"),
    }
    result.update({candidate: _validate_candidate(value[candidate], f"{name}.{candidate}") for candidate in candidates})
    selected_chunks = sum(result[candidate]["adaptive_selections"]["chunks"] for candidate in candidates)
    selected_points = sum(result[candidate]["adaptive_selections"]["points"] for candidate in candidates)
    if (selected_chunks, selected_points) != (result["chunks"], result["points"]):
        raise GateError(f"{name}: adaptive timestamp selections do not reconcile")
    unique_chunks = sum(result[candidate]["unique_wins"]["chunks"] for candidate in candidates)
    unique_points = sum(result[candidate]["unique_wins"]["points"] for candidate in candidates)
    tied = result["tied_minima"]
    if (unique_chunks + tied["chunks"], unique_points + tied["points"]) != (
        result["chunks"],
        result["points"],
    ):
        raise GateError(f"{name}: unique timestamp wins and ties do not reconcile")
    if result["adaptive_min_bytes"] > min(result[candidate]["bytes"] for candidate in candidates):
        raise GateError(f"{name}: adaptive timestamp bytes exceed an aggregate candidate")
    return result


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
            totals[f"{candidate}.{winner}.chunks"] = value[candidate][winner]["chunks"]
            totals[f"{candidate}.{winner}.points"] = value[candidate][winner]["points"]
    return totals


def _reconcile_timestamp_breakdown(
    rows: list[dict[str, Any]], all_blocks: dict[str, Any], name: str
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
            f"{name}: additive timestamp field {field} does not reconcile: "
            f"expected {expected[field]}, found {actual[field]}"
        )


def _validate_verifier(path: Path, codec: str) -> dict[str, Any]:
    document = _load_json(path)
    if not isinstance(document, dict) or set(document) != EXPECTED_VERIFIER_KEYS:
        raise GateError(f"{path}: verifier report shape differs from the Phase 6 contract")
    if document["schema_version"] != 8 or document["footer_validation_enabled"] is not True:
        raise GateError(f"{path}: verifier did not exhaustively validate Schema 8 footers")
    if document["series_sample_per_segment"] is not None:
        raise GateError(f"{path}: verifier used a series sample limit")
    for key in ("segments", "corpus_series", "series", "chunks", "samples", "logical_chunk_bytes"):
        _positive(document[key], f"{path}.{key}")
    if document["series"] != document["corpus_series"]:
        raise GateError(f"{path}: exhaustive verifier series count differs from corpus series")
    chunks_by_kind = document["chunks_by_kind"]
    if not isinstance(chunks_by_kind, list) or len(chunks_by_kind) != len(KIND_ORDER):
        raise GateError(f"{path}: chunks_by_kind must contain exactly five kind counts")
    chunks_by_kind = [
        _nonnegative(value, f"{path}.chunks_by_kind[{index}]")
        for index, value in enumerate(chunks_by_kind)
    ]
    if sum(chunks_by_kind) != document["chunks"]:
        raise GateError(f"{path}: chunks_by_kind total differs from verifier chunks")
    _digest(document["verified_selection_fingerprint"], f"{path}.verified_selection_fingerprint")
    _digest(document["decoded_semantic_fingerprint"], f"{path}.decoded_semantic_fingerprint")
    exact = document["exact_postings"]
    if not isinstance(exact, dict) or set(exact) != {"logical_fingerprint", "lists", "decoded_refs", "encoded_bytes"}:
        raise GateError(f"{path}: exact postings were not exhaustively verified")
    _digest(exact["logical_fingerprint"], f"{path}.exact_postings.logical_fingerprint")
    for key in ("lists", "decoded_refs", "encoded_bytes"):
        _positive(exact[key], f"{path}.exact_postings.{key}")
    inventory = document["chunk_inventory"]
    if not isinstance(inventory, dict) or set(inventory) != {
        "layout", "by_kind_encoding", "raw_f64_vs_gorilla", "timestamp_candidates"
    }:
        raise GateError(f"{path}: chunk inventory has an invalid shape")
    if inventory["layout"] != "sealed_chunk_v1":
        raise GateError(f"{path}: chunk inventory layout differs")
    rows = inventory["by_kind_encoding"]
    if not isinstance(rows, list) or not rows:
        raise GateError(f"{path}: chunk inventory is empty")
    row_keys = {
        "kind", "encoding", "payload_layout", "chunks", "points", "indexed_bytes",
        "common_header_bytes", "scalar_lane_bytes", "payload_bytes", "timestamp_base_bytes",
        "timestamp_delta_bytes", "value_bytes", "point_count_histogram", "cadence_ms_histogram",
    }
    total_chunks = total_points = 0
    total_indexed_bytes = 0
    total_native_timestamp_bytes = 0
    inventory_chunks_by_kind = {kind: 0 for kind in KIND_ORDER}
    float_chunks = float_points = float_indexed = float_payload = 0
    float_encodings: set[str] = set()
    kind_encoding_keys: set[tuple[str, str]] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or set(row) != row_keys:
            raise GateError(f"{path}: chunk inventory row {index} has an invalid shape")
        for key in ("kind", "encoding", "payload_layout"):
            if not isinstance(row[key], str) or not row[key]:
                raise GateError(f"{path}: chunk inventory row {index} has an empty {key}")
        kind_encoding = (row["kind"], row["encoding"])
        if kind_encoding in kind_encoding_keys:
            raise GateError(f"{path}: duplicate chunk inventory kind/encoding row: {kind_encoding}")
        kind_encoding_keys.add(kind_encoding)
        expected_layout = VALID_KIND_ENCODING_LAYOUTS.get(kind_encoding)
        if expected_layout is None or row["payload_layout"] != expected_layout:
            raise GateError(
                f"{path}: invalid kind/encoding/payload-layout tuple: "
                f"{kind_encoding + (row['payload_layout'],)}"
            )
        chunks = _positive(row["chunks"], f"{path}.chunk_inventory[{index}].chunks")
        points = _positive(row["points"], f"{path}.chunk_inventory[{index}].points")
        if points < chunks:
            raise GateError(f"{path}: chunk inventory row {index} has fewer points than chunks")
        for key in (
            "indexed_bytes", "common_header_bytes", "scalar_lane_bytes", "payload_bytes",
            "timestamp_base_bytes", "timestamp_delta_bytes", "value_bytes",
        ):
            _nonnegative(row[key], f"{path}.chunk_inventory[{index}].{key}")
        if row["common_header_bytes"] != chunks * 40:
            raise GateError(f"{path}: common header bytes do not reconcile")
        if row["indexed_bytes"] != row["common_header_bytes"] + row["scalar_lane_bytes"] + row["payload_bytes"]:
            raise GateError(f"{path}: indexed chunk bytes do not reconcile")
        if row["payload_bytes"] != row["timestamp_base_bytes"] + row["timestamp_delta_bytes"] + row["value_bytes"]:
            raise GateError(f"{path}: native payload partition does not reconcile")
        _validate_histogram(
            row["point_count_histogram"],
            f"{path}.chunk_inventory[{index}].point_count_histogram",
            chunks,
        )
        _validate_histogram(
            row["cadence_ms_histogram"],
            f"{path}.chunk_inventory[{index}].cadence_ms_histogram",
            points - chunks,
        )
        total_chunks += chunks
        total_points += points
        total_indexed_bytes += row["indexed_bytes"]
        total_native_timestamp_bytes += row["timestamp_base_bytes"] + row["timestamp_delta_bytes"]
        inventory_chunks_by_kind[row["kind"]] += chunks
        if row["kind"] == "float":
            float_chunks += chunks
            float_points += points
            float_indexed += row["indexed_bytes"]
            float_payload += row["payload_bytes"]
            float_encodings.add(row["encoding"])
    if (total_chunks, total_points) != (document["chunks"], document["samples"]):
        raise GateError(f"{path}: inventory totals differ from verifier totals")
    if total_indexed_bytes != document["logical_chunk_bytes"]:
        raise GateError(f"{path}: inventory indexed bytes differ from logical chunk bytes")
    if [inventory_chunks_by_kind[kind] for kind in KIND_ORDER] != chunks_by_kind:
        raise GateError(f"{path}: chunks_by_kind differs from chunk inventory kind counts")
    evidence = inventory["raw_f64_vs_gorilla"]
    if not isinstance(evidence, dict) or set(evidence) != EXPECTED_FLOAT_EVIDENCE_KEYS:
        raise GateError(f"{path}: Float candidate evidence has an invalid shape")
    if not isinstance(evidence["tie_rule"], str) or "RAW_F64" not in evidence["tie_rule"]:
        raise GateError(f"{path}: Float candidate tie rule is not canonical")
    for key in EXPECTED_FLOAT_EVIDENCE_KEYS - {
        "tie_rule", "raw_f64_wins", "gorilla_wins", "ties",
        "adaptive_raw_f64_selections", "adaptive_gorilla_selections", "xor_significant_bits_histogram",
    }:
        _nonnegative(evidence[key], f"{path}.raw_f64_vs_gorilla.{key}")
    for key in ("raw_f64_wins", "gorilla_wins", "ties", "adaptive_raw_f64_selections", "adaptive_gorilla_selections"):
        _winner(evidence[key], f"{path}.raw_f64_vs_gorilla.{key}")
    if (evidence["chunks"], evidence["points"], evidence["existing_indexed_bytes"], evidence["existing_payload_bytes"]) != (
        float_chunks, float_points, float_indexed, float_payload
    ):
        raise GateError(f"{path}: Float evidence does not reconcile with the chunk inventory")
    expected_float_header_bytes = 40 * float_chunks
    for prefix in ("existing", "raw_f64_candidate", "gorilla_candidate", "adaptive_min"):
        indexed_bytes = evidence[f"{prefix}_indexed_bytes"]
        payload_bytes = evidence[f"{prefix}_payload_bytes"]
        if indexed_bytes - payload_bytes != expected_float_header_bytes:
            raise GateError(
                f"{path}: {prefix} Float indexed/payload bytes do not reconcile with headers"
            )
    win_chunks = sum(evidence[key]["chunks"] for key in ("raw_f64_wins", "gorilla_wins", "ties"))
    win_points = sum(evidence[key]["points"] for key in ("raw_f64_wins", "gorilla_wins", "ties"))
    if (win_chunks, win_points) != (float_chunks, float_points):
        raise GateError(f"{path}: Float winner totals do not reconcile")
    selected_chunks = sum(evidence[key]["chunks"] for key in ("adaptive_raw_f64_selections", "adaptive_gorilla_selections"))
    selected_points = sum(evidence[key]["points"] for key in ("adaptive_raw_f64_selections", "adaptive_gorilla_selections"))
    if (selected_chunks, selected_points) != (float_chunks, float_points):
        raise GateError(f"{path}: adaptive Float selections do not reconcile")
    raw_wins = evidence["raw_f64_wins"]
    gorilla_wins = evidence["gorilla_wins"]
    ties = evidence["ties"]
    adaptive_raw = evidence["adaptive_raw_f64_selections"]
    adaptive_gorilla = evidence["adaptive_gorilla_selections"]
    if adaptive_raw != {
        "chunks": raw_wins["chunks"] + ties["chunks"],
        "points": raw_wins["points"] + ties["points"],
    }:
        raise GateError(f"{path}: adaptive RawF64 selections violate the RawF64 tie rule")
    if adaptive_gorilla != gorilla_wins:
        raise GateError(f"{path}: adaptive Gorilla selections do not equal Gorilla wins")
    if evidence["adaptive_min_indexed_bytes"] > min(
        evidence["raw_f64_candidate_indexed_bytes"],
        evidence["gorilla_candidate_indexed_bytes"],
    ):
        raise GateError(f"{path}: adaptive Float indexed bytes exceed an aggregate candidate")
    if evidence["adaptive_min_payload_bytes"] > min(
        evidence["raw_f64_candidate_payload_bytes"],
        evidence["gorilla_candidate_payload_bytes"],
    ):
        raise GateError(f"{path}: adaptive Float payload bytes exceed an aggregate candidate")
    xor_transitions = (
        evidence["repeated_xor_points"]
        + evidence["reused_window_points"]
        + evidence["new_window_points"]
    )
    if xor_transitions != float_points - float_chunks:
        raise GateError(f"{path}: Float XOR transition counts do not reconcile")
    _validate_histogram(
        evidence["xor_significant_bits_histogram"],
        f"{path}.raw_f64_vs_gorilla.xor_significant_bits_histogram",
        evidence["reused_window_points"] + evidence["new_window_points"],
    )
    classification_points = sum(
        evidence[key] for key in (
            "positive_zero_points", "negative_zero_points", "finite_nonzero_points",
            "positive_infinity_points", "negative_infinity_points", "ordinary_nan_points", "stale_nan_points",
        )
    )
    if classification_points != float_points:
        raise GateError(f"{path}: IEEE value classifications do not reconcile")
    expected_float_encodings = {"raw_f64"} if codec == "raw" else {"gorilla"}
    if float_encodings != expected_float_encodings:
        raise GateError(
            f"{path}: {codec} replay Float encodings differ: "
            f"expected {sorted(expected_float_encodings)}, found {sorted(float_encodings)}"
        )
    timestamp = inventory["timestamp_candidates"]
    if not isinstance(timestamp, dict) or set(timestamp) != {
        "scope", "tie_rule", "selector_bytes_included", "all_blocks", "by_shape", "by_kind_encoding"
    }:
        raise GateError(f"{path}: timestamp evidence has an invalid shape")
    if timestamp["selector_bytes_included"] is not False:
        raise GateError(f"{path}: timestamp evidence unexpectedly includes selector bytes")
    if not isinstance(timestamp["scope"], str) or not timestamp["scope"]:
        raise GateError(f"{path}: timestamp evidence scope is missing")
    if not isinstance(timestamp["tie_rule"], str) or not timestamp["tie_rule"]:
        raise GateError(f"{path}: timestamp tie rule is missing")
    all_blocks = _validate_timestamp_evidence(timestamp["all_blocks"], f"{path}.timestamp.all_blocks")
    if (all_blocks["chunks"], all_blocks["points"]) != (document["chunks"], document["samples"]):
        raise GateError(f"{path}: timestamp all-block totals do not reconcile")
    if all_blocks["current_offset_uleb"]["bytes"] != total_native_timestamp_bytes:
        raise GateError(f"{path}: current timestamp candidate bytes differ from native payload bytes")
    by_shape = timestamp["by_shape"]
    if not isinstance(by_shape, list) or not by_shape:
        raise GateError(f"{path}: timestamp shape evidence is empty")
    shape_chunks = shape_points = 0
    observed_shapes: set[str] = set()
    shape_evidence: list[dict[str, Any]] = []
    for index, item in enumerate(by_shape):
        if not isinstance(item, dict) or set(item) != {"shape", "evidence"}:
            raise GateError(f"{path}: timestamp shape row {index} is malformed")
        shape = item["shape"]
        if shape not in TIMESTAMP_SHAPES:
            raise GateError(f"{path}: timestamp shape row {index} has an unknown shape")
        if shape in observed_shapes:
            raise GateError(f"{path}: duplicate timestamp shape row: {shape}")
        observed_shapes.add(shape)
        parsed = _validate_timestamp_evidence(item["evidence"], f"{path}.timestamp.by_shape[{index}]")
        shape_evidence.append(parsed)
        shape_chunks += parsed["chunks"]
        shape_points += parsed["points"]
    if (shape_chunks, shape_points) != (document["chunks"], document["samples"]):
        raise GateError(f"{path}: timestamp shape totals do not reconcile")
    _reconcile_timestamp_breakdown(shape_evidence, all_blocks, f"{path}.timestamp.by_shape")
    by_kind_encoding = timestamp["by_kind_encoding"]
    if not isinstance(by_kind_encoding, list) or not by_kind_encoding:
        raise GateError(f"{path}: timestamp kind/encoding evidence is empty")
    timestamp_keys: set[tuple[str, str]] = set()
    timestamp_chunks = timestamp_points = 0
    kind_encoding_evidence: list[dict[str, Any]] = []
    for index, item in enumerate(by_kind_encoding):
        if not isinstance(item, dict) or set(item) != {"kind", "encoding", "evidence"}:
            raise GateError(f"{path}: timestamp kind/encoding row {index} is malformed")
        kind = item["kind"]
        encoding = item["encoding"]
        if not isinstance(kind, str) or not kind or not isinstance(encoding, str) or not encoding:
            raise GateError(f"{path}: timestamp kind/encoding row {index} has an empty key")
        key = (kind, encoding)
        if key in timestamp_keys:
            raise GateError(f"{path}: duplicate timestamp kind/encoding row: {key}")
        timestamp_keys.add(key)
        parsed = _validate_timestamp_evidence(
            item["evidence"], f"{path}.timestamp.by_kind_encoding[{index}]"
        )
        kind_encoding_evidence.append(parsed)
        timestamp_chunks += parsed["chunks"]
        timestamp_points += parsed["points"]
    if timestamp_keys != kind_encoding_keys:
        raise GateError(f"{path}: timestamp and chunk-inventory kind/encoding keys differ")
    if (timestamp_chunks, timestamp_points) != (document["chunks"], document["samples"]):
        raise GateError(f"{path}: timestamp kind/encoding totals do not reconcile")
    _reconcile_timestamp_breakdown(
        kind_encoding_evidence, all_blocks, f"{path}.timestamp.by_kind_encoding"
    )
    return document


def _canonical_float_evidence(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key not in FLOAT_EXISTING_FIELDS}


def _non_float_inventory_rows(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(
        (row for row in inventory["by_kind_encoding"] if row["kind"] != "float"),
        key=lambda row: (row["kind"], row["encoding"]),
    )


def _canonical_timestamp_by_kind(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in inventory["timestamp_candidates"]["by_kind_encoding"]:
        rows.append(
            {
                "kind": row["kind"],
                "encoding": "<float-codec>" if row["kind"] == "float" else row["encoding"],
                "evidence": row["evidence"],
            }
        )
    return sorted(rows, key=lambda row: (row["kind"], row["encoding"]))


def compare_verifiers(raw_path: Path, gorilla_path: Path, output: Path) -> None:
    raw = _validate_verifier(raw_path, "raw")
    gorilla = _validate_verifier(gorilla_path, "gorilla")
    equality_fields = (
        "decoded_semantic_fingerprint", "segments", "corpus_series", "series",
        "chunks", "chunks_by_kind", "samples",
    )
    for field in equality_fields:
        if raw[field] != gorilla[field]:
            raise GateError(f"verifier semantic field differs across codecs: {field}")
    if raw["verified_selection_fingerprint"] == gorilla["verified_selection_fingerprint"]:
        raise GateError("physical verified-selection fingerprints unexpectedly match")
    if raw["exact_postings"] != gorilla["exact_postings"]:
        raise GateError("exact postings evidence differs across codecs")
    raw_inventory = raw["chunk_inventory"]
    gorilla_inventory = gorilla["chunk_inventory"]
    if _canonical_float_evidence(raw_inventory["raw_f64_vs_gorilla"]) != _canonical_float_evidence(
        gorilla_inventory["raw_f64_vs_gorilla"]
    ):
        raise GateError("canonical Raw/Gorilla candidate evidence differs across corpora")
    if _non_float_inventory_rows(raw_inventory) != _non_float_inventory_rows(gorilla_inventory):
        raise GateError("non-Float chunk inventory differs across Raw/Gorilla corpora")
    for field in ("scope", "tie_rule", "selector_bytes_included", "all_blocks", "by_shape"):
        if raw_inventory["timestamp_candidates"][field] != gorilla_inventory["timestamp_candidates"][field]:
            raise GateError(f"timestamp evidence differs across corpora: {field}")
    if _canonical_timestamp_by_kind(raw_inventory) != _canonical_timestamp_by_kind(gorilla_inventory):
        raise GateError("canonical timestamp kind/encoding evidence differs across corpora")
    candidate = raw_inventory["raw_f64_vs_gorilla"]
    timestamp = raw_inventory["timestamp_candidates"]
    _write_json(
        output,
        {
            "schema": VERIFIER_COMPARISON_SCHEMA,
            "status": "pass",
            "decoded_semantic_fingerprint": raw["decoded_semantic_fingerprint"],
            "raw_verified_selection_fingerprint": raw["verified_selection_fingerprint"],
            "gorilla_verified_selection_fingerprint": gorilla["verified_selection_fingerprint"],
            "chunks": raw["chunks"],
            "samples": raw["samples"],
            "raw_logical_chunk_bytes": raw["logical_chunk_bytes"],
            "gorilla_logical_chunk_bytes": gorilla["logical_chunk_bytes"],
            "raw_f64_vs_gorilla": candidate,
            "timestamp_candidates": timestamp,
            "timestamp_runtime_ab_status": "blocked_no_versioned_writer_or_reader_selector",
            "timestamp_scope_note": "native payload only; typed scalar-lane timestamp bytes are excluded",
        },
    )


def check_readback(report: Path, output: Path) -> None:
    result = ab_gate.parse_readback(report, None, None, None)
    if result["status"] != "pass" or result["skipped_queries"] != 0:
        raise GateError("readback oracle did not complete with zero skips")
    text = report.read_text(encoding="utf-8")
    rows = ab_gate._markdown_rows(ab_gate._section(text, "PromQL Readbacks"))  # noqa: SLF001
    if not rows or rows[0][0] != "Kind":
        raise GateError("readback report lacks the PromQL result table")
    result["promql_rows"] = len(rows) - 1
    result["promql_rows_fingerprint_sha256"] = hashlib.sha256(
        json.dumps(rows[1:], separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    _write_json(output, result)


def normalize_manifest(input_path: Path, output_tsv: Path, output_json: Path, default_cache: int) -> None:
    original = query.MANIFEST_SCHEMA
    try:
        query.MANIFEST_SCHEMA = PHASE6_QUERY_MANIFEST_SCHEMA
        normalized = query.normalize_manifest(input_path, default_cache)
    finally:
        query.MANIFEST_SCHEMA = original
    _validate_phase6_query_cache_policy(normalized)
    query.write_normalized_manifest(normalized, output_tsv, output_json)
    document = _load_json(output_json)
    document["schema"] = PHASE6_QUERY_NORMALIZED_SCHEMA
    output_json.unlink()
    _write_json(output_json, document)


def _validate_phase6_query_cache_policy(queries: list[dict[str, Any]]) -> None:
    for item in queries:
        if (
            item.get("mode") == "range"
            and item.get("range_scalar_cache_max_bytes") != 0
        ):
            raise GateError(
                "Phase 6 range queries require range_scalar_cache_max_bytes=0"
            )


def read_normalized_manifest(path: Path) -> list[dict[str, Any]]:
    document = _load_json(path)
    if not isinstance(document, dict) or document.get("schema") != PHASE6_QUERY_NORMALIZED_SCHEMA:
        raise GateError("normalized Phase 6 query manifest has the wrong schema")
    queries = document.get("queries")
    if not isinstance(queries, list) or not queries:
        raise GateError("normalized Phase 6 query manifest is empty")
    _validate_phase6_query_cache_policy(queries)
    return queries


def _query_stats_digest(stats: dict[str, int]) -> str:
    reduced = {key: value for key, value in stats.items() if key not in ALLOWED_QUERY_STATS_DIFFERENCES}
    return hashlib.sha256(json.dumps(reduced, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


def compare_queries(args: argparse.Namespace) -> None:
    fields = [
        "process_label", "query_name", "category", "mode", "block", "slot", "codec",
        "corpus", "raw_output", "max_rss_kib", "perf_json",
    ]
    rows = _read_tsv(args.index, fields)
    queries = read_normalized_manifest(args.manifest)
    by_name = {item["query_name"]: item for item in queries}
    expected = len(queries) * args.blocks * 4
    if len(rows) != expected:
        raise GateError(f"expected {expected} query processes, found {len(rows)}")
    if args.benchmark_repeats != phase3.BENCHMARK_REPEATS:
        raise GateError(
            f"current raw-v13 validation requires exactly {phase3.BENCHMARK_REPEATS} cold/warm evaluations"
        )
    if args.label_materialization != "demand-driven":
        raise GateError("Phase 6 fixes demand-driven label materialization")
    processes: dict[tuple[str, int, str, int], dict[str, Any]] = {}
    corpus_fingerprints = {codec: set() for codec in CODECS}
    labels: set[str] = set()
    for row in rows:
        name = row["query_name"]
        query_spec = by_name.get(name)
        if query_spec is None:
            raise GateError(f"query index names unknown query: {name}")
        if (row["category"], row["mode"]) != (query_spec["category"], query_spec["mode"]):
            raise GateError(f"query metadata differs for {name}")
        block = _positive(int(row["block"]), "query block")
        slot = _positive(int(row["slot"]), "query slot")
        if block > args.blocks or slot > 4:
            raise GateError("query coordinate exceeds schedule")
        codec = row["codec"]
        if codec != _expected_abba(block)[slot - 1]:
            raise GateError(f"query schedule is not counterbalanced ABBA: {row['process_label']}")
        if row["process_label"] in labels:
            raise GateError(f"duplicate query process label: {row['process_label']}")
        labels.add(row["process_label"])
        fake_row = {
            "raw_output": row["raw_output"],
            "payload_coalesce_max_gap_bytes": "4096",
        }
        validation_args = argparse.Namespace(
            corpus=Path(row["corpus"]),
            backend="pread",
            queue_depth=args.queue_depth,
            arena_bytes=phase3.DEFAULT_ARENA_BYTES,
            max_matched_series=args.max_matched_series,
            max_projected_series=args.max_projected_series,
            max_chunk_reads=args.max_chunk_reads,
            max_bytes_read=args.max_bytes_read,
            max_samples_decoded=args.max_samples_decoded,
            max_regex_values_examined=args.max_regex_values_examined,
        )
        try:
            fingerprint, runs = phase3.validate_raw(fake_row, query_spec, validation_args)
        except (phase3.GateError, query.GateError, query_common.GateError) as error:
            raise GateError(str(error)) from error
        if query_spec["category"] == "typed-scalar-projection-control":
            for run in runs:
                if run["stats"]["typed_scalar_chunks_decoded"] == 0:
                    raise GateError(f"{name}: typed scalar control decoded no scalar lanes")
                if run["stats"]["typed_full_chunks_decoded"] != 0:
                    raise GateError(f"{name}: typed scalar control unexpectedly decoded full typed chunks")
        if query_spec["category"] == "native-histogram-full-control":
            for run in runs:
                if run["stats"]["typed_full_chunks_decoded"] == 0:
                    raise GateError(f"{name}: typed full control decoded no full typed chunks")
        if query_spec["category"] in {
            "equality-full-demand",
            "scalar-instant-selective",
            "scalar-range-selective",
        }:
            for run in runs:
                if run["stats"]["chunk_reads"] == 0 or run["stats"]["samples_decoded"] == 0:
                    raise GateError(f"{name}: Float decode control performed no payload decode")
        corpus_fingerprints[codec].add(fingerprint)
        key = (name, block, codec, slot)
        if key in processes:
            raise GateError(f"duplicate query process coordinate: {key}")
        processes[key] = {"row": row, "runs": runs}
    if any(len(values) != 1 for values in corpus_fingerprints.values()):
        raise GateError("a codec corpus fingerprint changed during query measurement")
    comparisons: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    for query_spec in queries:
        name = query_spec["query_name"]
        for block in range(1, args.blocks + 1):
            raw_slots = [slot for slot, codec in enumerate(_expected_abba(block), 1) if codec == "raw"]
            gorilla_slots = [slot for slot, codec in enumerate(_expected_abba(block), 1) if codec == "gorilla"]
            for pair_index, (raw_slot, gorilla_slot) in enumerate(zip(raw_slots, gorilla_slots, strict=True), 1):
                raw_process = processes[(name, block, "raw", raw_slot)]
                gorilla_process = processes[(name, block, "gorilla", gorilla_slot)]
                for run_index in range(args.benchmark_repeats):
                    left = raw_process["runs"][run_index]
                    right = gorilla_process["runs"][run_index]
                    context = f"{name}/block-{block}/pair-{pair_index}/run-{run_index}"
                    for field in ("semantic_fingerprint", "portable_fingerprint", "result_series", "result_samples"):
                        if left[field] != right[field]:
                            raise GateError(f"{context}: {field} differs across codecs")
                    for field in query.QUERY_STATS_FIELDS:
                        difference_allowed = (
                            field in ALLOWED_QUERY_STATS_DIFFERENCES
                            and query_spec["category"] not in TYPED_CONTROL_CATEGORIES
                        )
                        if not difference_allowed and left["stats"][field] != right["stats"][field]:
                            raise GateError(f"{context}: QueryStats.{field} differs across codecs")
                    comparisons.append(
                        {
                            "query_name": name,
                            "block": block,
                            "pair": pair_index,
                            "run_index": run_index,
                            "run_kind": left["run_kind"],
                            "semantic_fingerprint": left["semantic_fingerprint"],
                            "portable_fingerprint": left["portable_fingerprint"],
                            "query_stats_without_bytes_sha256": _query_stats_digest(left["stats"]),
                            "raw_bytes_read": left["stats"]["bytes_read"],
                            "gorilla_bytes_read": right["stats"]["bytes_read"],
                        }
                    )
            for codec, slots in (("raw", raw_slots), ("gorilla", gorilla_slots)):
                for slot in slots:
                    process = processes[(name, block, codec, slot)]
                    index_row = process["row"]
                    for run in process["runs"]:
                        summary_rows.append(
                            {
                                "process_label": index_row["process_label"],
                                "query_name": name,
                                "category": query_spec["category"],
                                "mode": query_spec["mode"],
                                "block": block,
                                "slot": slot,
                                "codec": codec,
                                "run_index": run["run_index"],
                                "run_kind": run["run_kind"],
                                "duration_ns": run["duration_ns"],
                                "max_rss_kib": int(index_row["max_rss_kib"]),
                                "result_series": run["result_series"],
                                "result_samples": run["result_samples"],
                                "semantic_fingerprint": run["semantic_fingerprint"],
                                "portable_fingerprint": run["portable_fingerprint"],
                                "chunk_reads": run["stats"]["chunk_reads"],
                                "bytes_read": run["stats"]["bytes_read"],
                                "samples_decoded": run["stats"]["samples_decoded"],
                                "typed_scalar_chunks_decoded": run["stats"]["typed_scalar_chunks_decoded"],
                                "typed_full_chunks_decoded": run["stats"]["typed_full_chunks_decoded"],
                                "physical_reads": run["payload"]["physical_reads"],
                                "physical_bytes": run["payload"]["physical_bytes"],
                                "read_used_amplification": (
                                    run["payload"]["physical_bytes"] / run["payload"]["logical_used_bytes"]
                                    if run["payload"]["logical_used_bytes"] else 0.0
                                ),
                                "perf_available": index_row["perf_json"] != "-",
                            }
                        )
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=list(summary_rows[0]), delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(summary_rows)
    _write_json(
        args.output,
        {
            "schema": QUERY_COMPARISON_SCHEMA,
            "status": "pass",
            "blocks": args.blocks,
            "benchmark_repeats": args.benchmark_repeats,
            "allowed_query_stats_differences": sorted(ALLOWED_QUERY_STATS_DIFFERENCES),
            "typed_control_bytes_read_must_match": True,
            "corpus_fingerprints": {codec: next(iter(values)) for codec, values in corpus_fingerprints.items()},
            "comparisons": comparisons,
        },
    )


def parse_perf(input_path: Path, output: Path) -> None:
    observed: list[list[str]] = []
    for line in input_path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) != 7:
            raise GateError("perf stat row does not have the exact seven-column shape")
        observed.append(fields)
    observed_names = [fields[2] for fields in observed]
    if observed_names != list(PERF_REQUIRED_EVENTS):
        raise GateError(
            "perf stat event order/set differs from the exact Phase 6 contract: "
            f"{observed_names!r}"
        )
    decimal_pattern = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?")
    unsigned_pattern = re.compile(r"0|[1-9][0-9]*")
    positive_pattern = re.compile(r"[1-9][0-9]*")
    for index, fields in enumerate(observed):
        raw_value, unit, event, runtime_ns, running_percent, metric, metric_unit = fields
        if metric or metric_unit:
            raise GateError("perf stat metric columns must be empty")
        if not positive_pattern.fullmatch(runtime_ns):
            raise GateError(f"perf stat runtime is not a canonical positive integer: {event}")
        if not decimal_pattern.fullmatch(running_percent) or not (
            Decimal(0) < Decimal(running_percent) <= Decimal(100)
        ):
            raise GateError(f"perf stat running percentage is invalid: {event}")
        if index == 0:
            if unit != "msec" or not decimal_pattern.fullmatch(raw_value) or Decimal(
                raw_value
            ) <= Decimal(0):
                raise GateError("perf stat task-clock value or unit is invalid")
        elif unit or not unsigned_pattern.fullmatch(raw_value):
            raise GateError(
                f"perf stat counter is unavailable or non-canonical: {event}={raw_value!r}"
            )
    replay.parse_perf_stat(input_path, output, list(PERF_REQUIRED_EVENTS))


def _read_zero_status(path: Path, name: str) -> None:
    _regular_file(path, name)
    if path.read_text(encoding="ascii") != "0\n":
        raise GateError(f"{name} is not exactly zero: {path}")


def _effective_perf_policy(result_dir: Path, plan: dict[str, Any]) -> bool:
    perf_text = (result_dir / "metadata" / "perf-effective.txt").read_text(
        encoding="ascii"
    )
    if perf_text not in {"on\n", "off\n"}:
        raise GateError("perf-effective.txt must contain exactly on or off")
    perf_enabled = perf_text == "on\n"
    mode = plan["perf_stat_mode"]
    if mode == "required" and not perf_enabled:
        raise GateError("required perf mode did not produce usable counters")
    if mode == "off" and perf_enabled:
        raise GateError("disabled perf mode unexpectedly produced usable counters")
    if plan["binary_provenance_mode"] == "internal" and (
        mode != "required" or not perf_enabled
    ):
        raise GateError("formal source-bound completion requires effective perf")
    if perf_enabled:
        _read_zero_status(
            result_dir / "metadata" / "perf-preflight.exit-status",
            "perf preflight exit status",
        )
    return perf_enabled


def _require_mode(path: Path, expected: int, name: str) -> None:
    _regular_file(path, name)
    actual = stat.S_IMODE(path.lstat().st_mode)
    if actual != expected:
        raise GateError(
            f"{name} mode differs: expected={expected:04o} actual={actual:04o}: {path}"
        )


def _single_ingestion_report(run_dir: Path) -> Path:
    try:
        with os.scandir(run_dir) as iterator:
            entries = list(iterator)
    except OSError as error:
        raise GateError(
            f"cannot enumerate replay output directory {run_dir}: {error}"
        ) from error
    reports: list[Path] = []
    for entry in entries:
        if re.fullmatch(r"ingestion_stats_.*\.md", entry.name):
            try:
                metadata = entry.stat(follow_symlinks=False)
            except OSError as error:
                raise GateError(
                    f"cannot inspect ingestion report {entry.path}: {error}"
                ) from error
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise GateError(f"ingestion report is not a regular file: {entry.path}")
            reports.append(Path(entry.path))
    if len(reports) != 1:
        raise GateError(
            f"{run_dir} must contain exactly one ingestion report; found {len(reports)}"
        )
    return reports[0]


def _directory_entry_names(directory: Path, name: str) -> tuple[set[str], set[str]]:
    try:
        with os.scandir(directory) as iterator:
            entries = list(iterator)
    except OSError as error:
        raise GateError(f"cannot enumerate {name} {directory}: {error}") from error
    files: set[str] = set()
    directories: set[str] = set()
    for entry in entries:
        try:
            metadata = entry.stat(follow_symlinks=False)
        except OSError as error:
            raise GateError(
                f"cannot inspect {name} entry {entry.path}: {error}"
            ) from error
        if stat.S_ISLNK(metadata.st_mode):
            raise GateError(f"{name} contains a symbolic link: {entry.path}")
        if stat.S_ISREG(metadata.st_mode):
            files.add(entry.name)
        elif stat.S_ISDIR(metadata.st_mode):
            directories.add(entry.name)
        else:
            raise GateError(f"{name} contains a non-regular entry: {entry.path}")
    return files, directories


def _require_exact_names(
    directory: Path,
    name: str,
    expected_files: set[str],
    expected_directories: set[str],
) -> None:
    files, directories = _directory_entry_names(directory, name)
    if files != expected_files or directories != expected_directories:
        raise GateError(
            f"{name} artifact set differs: "
            f"missing_files={sorted(expected_files - files)!r} "
            f"extra_files={sorted(files - expected_files)!r} "
            f"missing_directories={sorted(expected_directories - directories)!r} "
            f"extra_directories={sorted(directories - expected_directories)!r}"
        )


def _compare_bytes(
    actual: Path, rebuilt: Path, reconstructed: list[str], result_dir: Path
) -> None:
    _regular_file(actual, "derived artifact")
    _regular_file(rebuilt, "rebuilt artifact")
    if actual.read_bytes() != rebuilt.read_bytes():
        relative = _result_relative(actual, result_dir, "derived artifact")
        raise GateError(
            f"derived artifact differs from independent reconstruction: {relative}"
        )
    reconstructed.append(_result_relative(actual, result_dir, "derived artifact"))


def _write_tsv_bytes(fields: list[str], rows: list[dict[str, Any]]) -> bytes:
    destination = io.StringIO(newline="")
    writer = csv.DictWriter(
        destination,
        fieldnames=fields,
        delimiter="\t",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)
    return destination.getvalue().encode()


def _write_exclusive_bytes(path: Path, value: bytes) -> None:
    if os.path.lexists(path):
        raise GateError(f"refusing to reuse output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as destination:
        destination.write(value)


def _rss_summary_from_samples(
    path: Path,
    interval_ms: int,
    control_path: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
) -> dict[str, Any]:
    fields = [
        "event",
        "elapsed_ns",
        "recorded_at_ns",
        "root_pid",
        "root_starttime_ticks",
        "process_count",
        "rss_kib",
        "rss_anon_kib",
        "rss_file_kib",
        "vm_swap_kib",
        "max_single_hwm_kib",
        "pids",
        "launch_observed",
    ]
    rows = _read_tsv(path, fields)
    sample_rows = [row for row in rows if row["event"] == "sample"]
    terminal_rows = [row for row in rows if row["event"] == "terminal"]
    if len(sample_rows) < 2:
        raise GateError("RSS monitor produced fewer than two cadence samples")
    if len(terminal_rows) != 1 or rows[-1]["event"] != "terminal":
        raise GateError("RSS monitor lacks one final terminal boundary")
    if any(row["event"] not in {"sample", "terminal"} for row in rows):
        raise GateError("RSS monitor contains an unknown event")
    control = _validate_replay_monitor_control(
        control_path, rss_ready, capacity_ready, launch, interval_ms
    )
    maxima = {
        "aggregate_rss_kib": 0,
        "aggregate_rss_anon_kib": 0,
        "aggregate_rss_file_kib": 0,
        "aggregate_vm_swap_kib": 0,
        "max_single_process_hwm_kib": 0,
        "process_count": 0,
    }
    mapping = {
        "rss_kib": "aggregate_rss_kib",
        "rss_anon_kib": "aggregate_rss_anon_kib",
        "rss_file_kib": "aggregate_rss_file_kib",
        "vm_swap_kib": "aggregate_vm_swap_kib",
        "max_single_hwm_kib": "max_single_process_hwm_kib",
        "process_count": "process_count",
    }
    timestamps: list[int] = []
    launch_observations: list[bool] = []
    for row in sample_rows:
        elapsed = _nonnegative(int(row["elapsed_ns"]), "RSS elapsed_ns")
        _positive(int(row["recorded_at_ns"]), "RSS recorded_at_ns")
        if (
            int(row["root_pid"]) != control["root_pid"]
            or int(row["root_starttime_ticks"])
            != control["root_starttime_ticks"]
        ):
            raise GateError("RSS sample is not bound to the controlled root")
        pids = row["pids"].split(",")
        process_count = _positive(int(row["process_count"]), "RSS process_count")
        if len(pids) != process_count or any(
            not re.fullmatch(r"[1-9][0-9]*", pid) for pid in pids
        ):
            raise GateError(f"RSS sample PID set is invalid: {path}")
        if control["root_pid"] not in {int(pid) for pid in pids}:
            raise GateError("RSS sample omitted the controlled root")
        if row["launch_observed"] not in {"true", "false"}:
            raise GateError("RSS launch observation is malformed")
        timestamps.append(elapsed)
        launch_observations.append(row["launch_observed"] == "true")
        for source, target in mapping.items():
            value = _nonnegative(int(row[source]), f"RSS {source}")
            maxima[target] = max(maxima[target], value)
    terminal = terminal_rows[0]
    elapsed_ns = _nonnegative(int(terminal["elapsed_ns"]), "RSS terminal timestamp")
    _positive(int(terminal["recorded_at_ns"]), "RSS terminal wall timestamp")
    if (
        int(terminal["root_pid"]) != control["root_pid"]
        or int(terminal["root_starttime_ticks"])
        != control["root_starttime_ticks"]
        or any(
            int(terminal[field]) != 0
            for field in (
                "process_count",
                "rss_kib",
                "rss_anon_kib",
                "rss_file_kib",
                "vm_swap_kib",
                "max_single_hwm_kib",
            )
        )
        or terminal["pids"] != "-"
        or terminal["launch_observed"] != "true"
    ):
        raise GateError("RSS terminal boundary is malformed or unbound")
    try:
        launch_index = launch_observations.index(True) + 1
    except ValueError as error:
        raise GateError("RSS monitor never observed the replay launch") from error
    if not all(launch_observations[launch_index - 1 :]):
        raise GateError("RSS launch observation is not monotonic")
    maximum_gap_ns = _edge_inclusive_maximum_gap_ns(
        timestamps, elapsed_ns, "RSS monitor"
    )
    allowed_gap_ns = interval_ms * CAPACITY_MONITOR_MAX_GAP_MULTIPLIER * 1_000_000
    if maximum_gap_ns > allowed_gap_ns:
        raise GateError("RSS monitor cadence gap exceeds 200 ms")
    return {
        "schema": RSS_MONITOR_SCHEMA,
        **_monitor_handshake_evidence(
            control_path,
            rss_ready,
            capacity_ready,
            launch,
            interval_ms,
            "rss_monitor",
            1,
            timestamps[0],
            launch_index,
            timestamps[launch_index - 1],
        ),
        "samples": len(sample_rows),
        "interval_ms": interval_ms,
        "elapsed_ns": elapsed_ns,
        "first_elapsed_ns": timestamps[0],
        "last_sample_elapsed_ns": timestamps[-1],
        "maximum_gap_ns": maximum_gap_ns,
        "maximum_allowed_gap_ns": allowed_gap_ns,
        **maxima,
        "status": "pass",
    }


def _check_checksum_manifest(
    path: Path, expected_paths: set[Path] | None = None
) -> None:
    _regular_file(path, "checksum authority")
    declared: set[Path] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if match is None:
            raise GateError(f"checksum authority has an invalid row: {path}")
        candidate = Path(match.group(2))
        if not candidate.is_absolute() or candidate in declared:
            raise GateError(
                f"checksum authority has an unsafe or duplicate path: {candidate}"
            )
        declared.add(candidate)
        _regular_file(candidate, "checksummed artifact")
        if _sha256(candidate) != match.group(1):
            raise GateError(f"checksummed artifact changed: {candidate}")
    if not declared:
        raise GateError(f"checksum authority is empty: {path}")
    if expected_paths is not None and declared != expected_paths:
        raise GateError(f"checksum authority path set differs: {path}")


def _validate_binary_provenance_table(result_dir: Path) -> None:
    rows = _read_tsv(
        result_dir / "metadata" / "binaries.tsv",
        ["role", "source", "preserved", "sha256"],
    )
    expected_roles = [
        "chronoxide-ingester",
        "chronoxide-query",
        "chronoxide-storage-verify",
    ]
    if [row["role"] for row in rows] != expected_roles:
        raise GateError("binary provenance role order or set differs")
    for row in rows:
        role = row["role"]
        source = Path(row["source"])
        preserved = Path(row["preserved"])
        expected_preserved = result_dir / "metadata" / "binaries" / role
        if preserved != expected_preserved:
            raise GateError(f"binary provenance preserved path differs: {role}")
        if not source.is_absolute() or "\n" in row["source"] or "\t" in row["source"]:
            raise GateError(f"binary provenance source path is invalid: {role}")
        _regular_file(preserved, f"{role} preserved binary")
        digest = _digest(row["sha256"], f"{role}.sha256")
        if _sha256(preserved) != digest:
            raise GateError(f"binary provenance digest differs: {role}")


def _validate_settings(result_dir: Path, plan: dict[str, Any]) -> None:
    settings: dict[str, str] = {}
    for line in (result_dir / "metadata" / "settings.txt").read_text(
        encoding="utf-8"
    ).splitlines():
        key, separator, value = line.partition("=")
        if not separator or not key or key in settings:
            raise GateError("settings evidence has a malformed or duplicate row")
        settings[key] = value
    expected_keys = {
        "recorded_at",
        "dry_run",
        "binary_provenance_mode",
        "promotion_eligibility",
        "stop_after_messages",
        "formal_build",
        "quiet_host_confirmed",
        "rss_interval_ms",
        "conflict_guard_interval_ms",
        "conflict_precheck",
        "capacity_monitor_interval_ms",
        "page_size_bytes",
        "max_capture_resident_bytes_after_evict",
        "max_corpus_resident_bytes_after_evict",
        "max_dirty_writeback_bytes",
        "replay_launch",
        "replay_monitor_ready_markers",
        "replay_monitor_cadence",
        "capacity_operational_floor_bytes",
        "capacity_build_source_result_allowance_bytes",
        "capacity_schedule_safe_reserve_bytes",
        "same_binary_runtime_control",
        "replay_blocks",
        "query_blocks",
        "schedule",
        "benchmark_repeats",
        "storage_layout",
        "query_backend",
        "query_payload_gap_bytes",
        "query_label_materialization",
        "query_label_storage",
        "query_label_arena_max_bytes",
        "query_instrumentation",
        "chunk_read_queue_depth",
        "query_max_series_matched",
        "query_max_projected_series",
        "query_max_chunks_read",
        "query_max_bytes_read",
        "query_max_samples",
        "regex_max_expanded_values",
        "range_scalar_cache_max_bytes",
        "perf_stat_mode",
        "perf_binary",
        "perf_binary_sha256",
        "perf_version",
        "perf_events",
        "footer_validation",
        "readback_sample_limit_per_kind",
        "readback_validation",
        "timestamp_runtime_ab",
        "timestamp_evidence_scope",
        "rust_log",
        "run_note",
    }
    recorded_at = settings.get("recorded_at", "")
    if set(settings) != expected_keys or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
        r"[+-][0-9]{2}:[0-9]{2}",
        recorded_at,
    ):
        raise GateError("settings evidence shape differs")
    try:
        parsed_recorded_at = datetime.fromisoformat(recorded_at)
    except ValueError as error:
        raise GateError("settings recorded_at is not a real datetime") from error
    if parsed_recorded_at.utcoffset() is None:
        raise GateError("settings recorded_at lacks a UTC offset")
    run_note = (result_dir / "metadata" / "run-note.txt").read_text(
        encoding="utf-8"
    )
    if not run_note.endswith("\n") or "\n" in run_note[:-1] or "\t" in run_note:
        raise GateError("run note evidence is malformed")
    expected = {
        "dry_run": "0",
        "binary_provenance_mode": plan["binary_provenance_mode"],
        "promotion_eligibility": plan["promotion_eligibility"],
        "stop_after_messages": str(plan["stop_after_messages"]),
        "formal_build": (
            "--locked --release --no-default-features; one isolated target build "
            "from an exact read-only git archive HEAD snapshot when internal"
        ),
        "quiet_host_confirmed": "1",
        "rss_interval_ms": str(plan["rss_interval_ms"]),
        "conflict_guard_interval_ms": str(plan["guard_interval_ms"]),
        "conflict_precheck": "same classifier; exact PID ancestry exclusions only",
        "capacity_monitor_interval_ms": str(plan["capacity_monitor_interval_ms"]),
        "page_size_bytes": str(plan["page_size_bytes"]),
        "max_capture_resident_bytes_after_evict": str(
            plan["max_capture_resident_bytes_after_evict"]
        ),
        "max_corpus_resident_bytes_after_evict": str(
            plan["max_corpus_resident_bytes_after_evict"]
        ),
        "max_dirty_writeback_bytes": str(plan["max_dirty_writeback_bytes"]),
        "replay_launch": (
            "held_until_root_starttime_bound_rss_and_capacity_first_samples"
        ),
        "replay_monitor_ready_markers": "distinct_immutable_atomic_mode_0444",
        "replay_monitor_cadence": (
            "edge_inclusive_initial_sample_terminal_max_200ms"
        ),
        "replay_blocks": str(plan["replay_blocks"]),
        "query_blocks": str(plan["query_blocks"]),
        "schedule": "odd raw,gorilla,gorilla,raw; even reversed",
        "benchmark_repeats": f"{plan['benchmark_repeats']} (cold,warm,warm)",
        "storage_layout": "schema8",
        "query_backend": "pread",
        "query_payload_gap_bytes": "4096",
        "query_label_materialization": "demand-driven",
        "query_label_storage": "compact-ids",
        "query_label_arena_max_bytes": str(plan["query_label_arena_max_bytes"]),
        "query_instrumentation": "off",
        "chunk_read_queue_depth": str(plan["chunk_read_queue_depth"]),
        "query_max_series_matched": str(plan["query_max_series_matched"]),
        "query_max_projected_series": str(plan["query_max_projected_series"]),
        "query_max_chunks_read": str(plan["query_max_chunks_read"]),
        "query_max_bytes_read": str(plan["query_max_bytes_read"]),
        "query_max_samples": str(plan["query_max_samples"]),
        "regex_max_expanded_values": str(plan["regex_max_expanded_values"]),
        "range_scalar_cache_max_bytes": "manifest; Phase 6 entries use 0",
        "perf_stat_mode": plan["perf_stat_mode"],
        "perf_binary": plan["perf_binary"],
        "perf_binary_sha256": plan["perf_binary_sha256"],
        "perf_version": plan["perf_version"],
        "perf_events": ",".join(PERF_REQUIRED_EVENTS),
        "footer_validation": (
            "exhaustive verifier pass outside replay/query timing"
        ),
        "readback_sample_limit_per_kind": str(
            plan["readback_sample_limit_per_kind"]
        ),
        "readback_validation": (
            "separate untimed independent oracle, zero skips required"
        ),
        "same_binary_runtime_control": (
            "head_buffer.float_encoding plus matching "
            "segment_writer.float_encoding"
        ),
        "timestamp_runtime_ab": (
            "blocked: no versioned writer/reader selector; verifier candidate "
            "inventory only"
        ),
        "timestamp_evidence_scope": (
            "native payload; typed scalar-lane timestamps excluded"
        ),
        "rust_log": plan["rust_log"],
        "run_note": run_note[:-1],
    }
    capacity = _load_capacity_contract(result_dir / "metadata" / "capacity-contract.json")
    expected.update(
        {
            "capacity_operational_floor_bytes": str(
                capacity["operational_floor_bytes"]
            ),
            "capacity_build_source_result_allowance_bytes": str(
                capacity["build_source_result_allowance_bytes"]
            ),
            "capacity_schedule_safe_reserve_bytes": str(
                capacity["schedule"]["safe_corpus_reserve_bytes"]
            ),
        }
    )
    for key, value in expected.items():
        if settings.get(key) != value:
            raise GateError(f"settings evidence differs for {key}")


def _validate_seal_check_log(
    result_dir: Path,
    plan: dict[str, Any],
    replay_rows: list[dict[str, Any]],
    query_rows: list[dict[str, Any]],
) -> None:
    rows = _read_tsv(
        result_dir / "metadata" / "seal-checks.tsv",
        ["recorded_at", "context", "promotion_eligibility"],
    )
    contexts = [
        "initial-preserved-binaries",
        "after-query-help",
        "before-verifier-help",
        "after-verifier-help",
        "before-static-input-transforms",
        "after-static-input-transforms",
        "after-config-rendering",
        "after-perf-preflight",
    ]
    for row in replay_rows:
        label = row["label"]
        contexts.extend(
            [
                f"{label}-before-replay",
                f"{label}-after-replay",
                f"{label}-after-replay-transforms",
            ]
        )
    contexts.extend(["after-final-capture-inventory", "after-replay-comparison"])
    for codec in CODECS:
        contexts.extend([f"{codec}-before-verifier", f"{codec}-after-verifier"])
    contexts.append("after-verifier-comparison")
    for codec in CODECS:
        contexts.extend([f"{codec}-before-readbacks", f"{codec}-after-readbacks"])
    for row in query_rows:
        label = row["process_label"]
        contexts.extend(
            [
                f"{label}-before-query",
                f"{label}-after-query",
                f"{label}-after-query-transforms",
            ]
        )
    contexts.extend(
        [
            "after-query-comparison",
            "after-final-corpus-inventory",
            "finalization",
            "final-authorities",
        ]
    )
    if [row["context"] for row in rows] != contexts:
        raise GateError("source/binary/control seal-check context order or set differs")
    for row in rows:
        if (
            not row["recorded_at"]
            or row["promotion_eligibility"] != plan["promotion_eligibility"]
        ):
            raise GateError(
                "seal-check evidence has an invalid timestamp or eligibility"
            )


def _check_raw_authorities(
    result_dir: Path,
    authority_path: Path,
    expected_seals: list[str],
) -> None:
    rows = _read_tsv(authority_path, ["path", "sha256"])
    found: list[str] = []
    observed: set[str] = set()
    for row in rows:
        relative = row["path"]
        if relative in observed:
            raise GateError(f"duplicate raw leaf authority: {relative}")
        observed.add(relative)
        found.append(relative)
        pure = PurePosixPath(relative)
        if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
            raise GateError(f"raw leaf authority path is unsafe: {relative!r}")
        seal = result_dir / Path(*pure.parts)
        _regular_file(seal, "raw leaf seal")
        if stat.S_IMODE(seal.lstat().st_mode) != 0o444:
            raise GateError(f"raw leaf seal is not read-only: {relative}")
        if _sha256(seal) != _digest(row["sha256"], f"{relative}.sha256"):
            raise GateError(f"raw leaf seal authority changed: {relative}")
        check_raw_leaf_seal(result_dir, seal)
    if found != expected_seals:
        raise GateError(
            "raw leaf authority order or set differs: "
            f"expected={expected_seals!r} actual={found!r}"
        )


def _require_raw_seal_contract(
    result_dir: Path,
    seal_relative: str,
    explicit_files: set[str],
    trees: set[str],
) -> None:
    document = _load_json(result_dir / seal_relative)
    declared_trees = document.get("trees")
    declared_files = document.get("files")
    if not isinstance(declared_trees, list) or set(declared_trees) != trees:
        raise GateError(f"raw leaf seal tree contract differs: {seal_relative}")
    if not isinstance(declared_files, list):
        raise GateError(f"raw leaf seal file contract is invalid: {seal_relative}")
    expected_files = set(explicit_files)
    for relative in trees:
        tree = result_dir / relative
        for _child_relative, path in replay._corpus_files(tree):  # noqa: SLF001
            expected_files.add(_result_relative(path, result_dir, "raw tree leaf"))
    actual_files = {
        entry.get("path")
        for entry in declared_files
        if isinstance(entry, dict) and isinstance(entry.get("path"), str)
    }
    if len(actual_files) != len(declared_files) or actual_files != expected_files:
        raise GateError(
            f"raw leaf seal file contract differs: {seal_relative}; "
            f"missing={sorted(expected_files - actual_files)!r} "
            f"extra={sorted(actual_files - expected_files)!r}"
        )


def _replay_schedule(result_dir: Path, blocks: int) -> list[dict[str, Any]]:
    contract = _load_capacity_contract(result_dir / "metadata" / "capacity-contract.json")
    if blocks != contract["schedule"]["replay_blocks"]:
        raise GateError("replay schedule differs from the capacity contract")
    safe = contract["derivation"]["safe_corpus_reserve_bytes"]
    bounds = contract["derivation"]["corpus_bound_bytes"]
    floor = contract["operational_floor_bytes"]
    remaining = dict(contract["schedule"]["codec_runs"])
    rows: list[dict[str, Any]] = []
    for block in range(1, blocks + 1):
        for slot, codec in enumerate(_expected_abba(block), 1):
            label = f"replay-b{block:02d}-s{slot:02d}-{codec}"
            run_dir = result_dir / "replays" / label
            remaining[codec] -= 1
            if remaining[codec] < 0:
                raise GateError("capacity schedule codec count underflow")
            future_reserve = sum(safe[name] * remaining[name] for name in CODECS)
            monitor_floor = future_reserve + floor
            rows.append(
                {
                    "label": label,
                    "ordinal": len(rows) + 1,
                    "block": block,
                    "slot": slot,
                    "codec": codec,
                    "run_dir": run_dir,
                    "config": result_dir / "configs" / f"{label}.toml",
                    "segments": run_dir / "segments",
                    "codec_bound_bytes": bounds[codec],
                    "codec_safe_reserve_bytes": safe[codec],
                    "future_safe_reserve_bytes": future_reserve,
                    "pre_required_free_bytes": safe[codec] + monitor_floor,
                    "monitor_minimum_free_bytes": monitor_floor,
                }
            )
    if any(remaining.values()):
        raise GateError("capacity schedule did not consume every pinned codec run")
    return rows


def _representative_corpora(replay_rows: list[dict[str, Any]]) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for row in replay_rows:
        result.setdefault(row["codec"], row["segments"])
    if set(result) != set(CODECS):
        raise GateError("replay schedule has no representative corpus for each codec")
    return result


def _query_schedule(
    result_dir: Path,
    queries: list[dict[str, Any]],
    blocks: int,
    corpora: dict[str, Path],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for query_spec in queries:
        name = query_spec["query_name"]
        if not isinstance(name, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.-]*", name
        ):
            raise GateError(f"query name is not path safe: {name!r}")
        for block in range(1, blocks + 1):
            for slot, codec in enumerate(_expected_abba(block), 1):
                label = f"{name}-b{block:02d}-s{slot:02d}-{codec}"
                rows.append(
                    {
                        "process_label": label,
                        "query": query_spec,
                        "block": block,
                        "slot": slot,
                        "codec": codec,
                        "corpus": corpora[codec],
                        "run_dir": result_dir / "query-runs" / label,
                    }
                )
    return rows


def _validate_final_admission_layout(
    result_dir: Path,
    plan: dict[str, Any],
    replay_rows: list[dict[str, Any]],
    query_rows: list[dict[str, Any]],
    perf_enabled: bool,
    *,
    finalized: bool = False,
) -> None:
    internal_build = plan["binary_provenance_mode"] == "internal"
    root_directories = set(FINAL_ARTIFACT_REQUIRED_DIRECTORIES)
    if internal_build:
        root_directories |= FINAL_ARTIFACT_OPTIONAL_DIRECTORIES
    root_files = set(FINAL_ROOT_EVIDENCE_FILES)
    if finalized:
        root_files.add("TIMESTAMP_CODEC_AB_BLOCKED.txt")
        if plan["promotion_eligibility"] == "formal_source_bound":
            root_files.add("RAW_GORILLA_COMPLETE_TIMESTAMP_AB_BLOCKED")
        else:
            root_files.add("EXPLORATORY_EXTERNAL_BINARIES_NON_PROMOTABLE.txt")
    _require_exact_names(
        result_dir,
        "finalized result root" if finalized else "final admission root",
        root_files,
        root_directories,
    )
    _validate_fixed_nested_layout(result_dir, internal_build)
    _require_exact_names(
        result_dir / "configs",
        "rendered config directory",
        {f"{row['label']}.toml" for row in replay_rows},
        set(),
    )
    _require_exact_names(
        result_dir / "comparisons",
        "comparison directory",
        FINAL_COMPARISON_FILES,
        set(),
    )
    metadata_files = set(FINAL_METADATA_BASE_FILES)
    if finalized:
        metadata_files.add("final-admission.json")
        inventory_authorities = {
            name
            for name in ("result-artifacts.nul", "result-artifacts.sha256")
            if os.path.lexists(result_dir / "metadata" / name)
        }
        if (
            "result-artifacts.sha256" in inventory_authorities
            and "result-artifacts.nul" not in inventory_authorities
        ):
            raise GateError(
                "final checksum authority exists without its artifact inventory"
            )
        metadata_files |= inventory_authorities
    if plan["perf_stat_mode"] != "off":
        metadata_files |= {
            "perf-preflight.exit-status",
            "perf-preflight.log",
            "perf-preflight.tsv",
        }
        preflight_status_text = (
            result_dir / "metadata" / "perf-preflight.exit-status"
        ).read_text(encoding="ascii")
        if re.fullmatch(r"[0-9]+\n", preflight_status_text) is None:
            raise GateError("perf preflight exit status is malformed")
        if int(preflight_status_text) == 0:
            metadata_files.add("perf-preflight-parse.log")
        if perf_enabled:
            metadata_files.add("perf-preflight.json")
    metadata_directories = {"binaries", "harness", "source"}
    if plan["binary_provenance_mode"] == "internal":
        metadata_directories.add("build")
    _require_exact_names(
        result_dir / "metadata",
        (
            "metadata directory in finalized result"
            if finalized
            else "metadata directory before final admission"
        ),
        metadata_files,
        metadata_directories,
    )
    _require_exact_names(
        result_dir / "metadata" / "binaries",
        "preserved binary directory",
        FINAL_BINARY_FILES,
        set(),
    )
    source_files = set(FINAL_SOURCE_BASE_FILES)
    if plan["binary_provenance_mode"] == "internal":
        source_files |= FINAL_SOURCE_INTERNAL_FILES
    _require_exact_names(
        result_dir / "metadata" / "source",
        "source provenance directory",
        source_files,
        set(),
    )
    _require_exact_names(
        result_dir / "inventory",
        "inventory directory",
        FINAL_INVENTORY_FILES,
        set(),
    )
    _require_exact_names(
        result_dir / "replays",
        "replay directory",
        set(),
        {row["label"] for row in replay_rows},
    )
    replay_files = set(FINAL_REPLAY_RUN_FILES)
    if perf_enabled:
        replay_files |= {"perf.tsv", "perf.json"}
    for row in replay_rows:
        report = _single_ingestion_report(row["run_dir"])
        _require_exact_names(
            row["run_dir"],
            f"replay run {row['label']}",
            replay_files | {report.name},
            {"segments"},
        )
    _require_exact_names(
        result_dir / "query-runs",
        "query run directory",
        set(),
        {row["process_label"] for row in query_rows},
    )
    query_files = set(FINAL_QUERY_RUN_FILES)
    if perf_enabled:
        query_files |= {"perf.tsv", "perf.json"}
    for row in query_rows:
        _require_exact_names(
            row["run_dir"], f"query run {row['process_label']}", query_files, set()
        )
    _require_exact_names(
        result_dir / "validation", "validation directory", set(), set(CODECS)
    )
    for codec in CODECS:
        _require_exact_names(
            result_dir / "validation" / codec,
            f"{codec} validation directory",
            FINAL_VALIDATION_FILES,
            set(),
        )


def _query_arguments(row: dict[str, Any], plan: dict[str, Any]) -> list[str]:
    query_spec = row["query"]
    run_dir = row["run_dir"]
    arguments = [
        "--segments-dir",
        str(row["corpus"]),
        "--storage-layout",
        "schema8",
        "--label-materialization",
        "demand-driven",
        "--query-label-storage",
        "compact-ids",
        "--query-label-arena-max-bytes",
        str(plan["query_label_arena_max_bytes"]),
        "--query-instrumentation",
        "off",
        "--start-ms",
        str(query_spec["start_ms"]),
        "--end-ms",
        str(query_spec["end_ms"]),
        "--benchmark-repeats",
        str(plan["benchmark_repeats"]),
        "--chunk-read-mode",
        "pread",
        "--chunk-read-queue-depth",
        str(plan["chunk_read_queue_depth"]),
        "--chunk-payload-coalesce-max-gap-bytes",
        "4096",
        "--query-max-series-matched",
        str(plan["query_max_series_matched"]),
        "--query-max-projected-series",
        str(plan["query_max_projected_series"]),
        "--query-max-chunks-read",
        str(plan["query_max_chunks_read"]),
        "--query-max-bytes-read",
        str(plan["query_max_bytes_read"]),
        "--query-max-samples",
        str(plan["query_max_samples"]),
        "--regex-max-expanded-values",
        str(plan["regex_max_expanded_values"]),
        "--output",
        str(run_dir / "report.md"),
        "--raw-output",
        str(run_dir / "raw.json"),
        "--query",
        query_spec["expression"],
    ]
    if query_spec["mode"] == "range":
        arguments.extend(
            [
                "--step-ms",
                str(query_spec["step_ms"]),
                "--range-scalar-cache-max-bytes",
                str(query_spec["range_scalar_cache_max_bytes"]),
            ]
        )
    for boundary in query_spec["boundaries"]:
        arguments.extend(
            ["--exponential-histogram-bucket-boundary", query.boundary_text(boundary)]
        )
    return arguments


def _validate_guard_precheck(path: Path) -> _ProcessIdentity:
    document = _load_json(path)
    fields = {
        "schema",
        "recorded_at_ns",
        "parent_pid",
        "parent_ppid",
        "parent_starttime_ticks",
        "conflicts",
        "status",
    }
    if (
        not isinstance(document, dict)
        or set(document) != fields
        or document.get("schema") != GUARD_PRECHECK_SCHEMA
        or document.get("conflicts") != []
        or document.get("status") != "pass"
    ):
        raise GateError("conflict precheck evidence is malformed or recorded a workload")
    _positive(document["recorded_at_ns"], "conflict precheck timestamp")
    return _ProcessIdentity(
        pid=_positive(document["parent_pid"], "conflict precheck parent PID"),
        ppid=_nonnegative(document["parent_ppid"], "conflict precheck parent PPID"),
        state="?",
        starttime=_positive(
            document["parent_starttime_ticks"],
            "conflict precheck parent starttime",
        ),
    )


def _validate_capacity_evidence(
    result_dir: Path,
    plan: dict[str, Any],
    replay_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    contract_path = result_dir / "metadata" / "capacity-contract.json"
    contract = _load_capacity_contract(contract_path)
    if _sha256(contract_path) != plan["capacity_contract_sha256"]:
        raise GateError("admission plan capacity contract digest differs")
    recorded_head = (result_dir / "metadata" / "source" / "git-commit.txt").read_text(
        encoding="ascii"
    )
    if recorded_head != f"{contract['source_head']}\n":
        raise GateError("capacity framing authority is not bound to the recorded HEAD")
    if plan["binary_provenance_mode"] == "internal":
        source_seal = _load_json(
            result_dir / "metadata" / "source" / "formal-source-seal.json"
        )
        if source_seal.get("head") != contract["source_head"]:
            raise GateError("capacity framing authority is not bound to the formal source seal")
    filesystem = result_dir.parent.resolve(strict=True)
    prebuild_free = _validate_capacity_snapshot(
        _load_json(result_dir / "metadata" / "capacity-prebuild.json"),
        phase="prebuild",
        filesystem=filesystem,
        minimum_free_bytes=contract["initial_required_free_bytes"],
    )
    postbuild_free = _validate_capacity_snapshot(
        _load_json(result_dir / "metadata" / "capacity-postbuild.json"),
        phase="postbuild",
        filesystem=filesystem,
        minimum_free_bytes=contract["postbuild_required_free_bytes"],
    )
    final_free = _validate_capacity_snapshot(
        _load_json(result_dir / "metadata" / "capacity-final.json"),
        phase="final",
        filesystem=filesystem,
        minimum_free_bytes=contract["operational_floor_bytes"],
    )
    _validate_guard_precheck(result_dir / "metadata" / "guardian-precheck.json")

    total_samples = 0
    minimum_observed: int | None = None
    maximum_corpus = {codec: 0 for codec in CODECS}
    for row in replay_rows:
        label = row["label"]
        run_dir = row["run_dir"]
        _validate_capacity_snapshot(
            _load_json(run_dir / "capacity-before.json"),
            phase=f"{label}-before",
            filesystem=filesystem,
            minimum_free_bytes=row["pre_required_free_bytes"],
        )
        _validate_capacity_snapshot(
            _load_json(run_dir / "capacity-after.json"),
            phase=f"{label}-after",
            filesystem=filesystem,
            minimum_free_bytes=row["monitor_minimum_free_bytes"],
        )
        _read_zero_status(
            run_dir / "capacity-monitor.exit-status",
            f"{label} capacity monitor exit status",
        )
        if (run_dir / "capacity-monitor.log").read_bytes():
            raise GateError(f"capacity monitor log is not empty: {label}")
        actual_capacity = _load_json(run_dir / "capacity.json")
        if not isinstance(actual_capacity, dict):
            raise GateError(f"capacity summary is not an object: {label}")
        expected_capacity = _capacity_summary_from_samples(
            run_dir / "capacity-samples.tsv",
            filesystem,
            row["monitor_minimum_free_bytes"],
            plan["capacity_monitor_interval_ms"],
            run_dir / "replay-monitor-control.json",
            run_dir / "rss-monitor.ready",
            run_dir / "capacity-monitor.ready",
            run_dir / "replay.launch",
        )
        if actual_capacity != expected_capacity:
            raise GateError(f"capacity monitor summary differs from raw samples: {label}")
        rss = _load_json(run_dir / "rss.json")
        if rss.get("root_pid") != expected_capacity["root_pid"]:
            raise GateError(f"capacity and RSS monitors observed different roots: {label}")
        total_samples += expected_capacity["samples"]
        observed = expected_capacity["minimum_observed_free_bytes"]
        minimum_observed = observed if minimum_observed is None else min(minimum_observed, observed)

        expected_corpus = check_corpus_capacity(
            run_dir / "corpus-summary.json", contract_path, row["codec"]
        )
        if _load_json(run_dir / "capacity-corpus-check.json") != expected_corpus:
            raise GateError(f"corpus capacity check differs from reconstruction: {label}")
        if expected_corpus["bound_bytes"] != row["codec_bound_bytes"]:
            raise GateError(f"replay plan corpus bound differs: {label}")
        maximum_corpus[row["codec"]] = max(
            maximum_corpus[row["codec"]], expected_corpus["actual_bytes"]
        )
    if minimum_observed is None:
        raise GateError("capacity evidence contains no replay monitor samples")
    return {
        "contract_sha256": _sha256(contract_path),
        "initial_required_free_bytes": contract["initial_required_free_bytes"],
        "postbuild_required_free_bytes": contract["postbuild_required_free_bytes"],
        "operational_floor_bytes": contract["operational_floor_bytes"],
        "prebuild_free_bytes": prebuild_free,
        "postbuild_free_bytes": postbuild_free,
        "final_free_bytes": final_free,
        "replay_monitor_samples": total_samples,
        "minimum_replay_observed_free_bytes": minimum_observed,
        "maximum_corpus_bytes": maximum_corpus,
        "status": "pass",
    }


def _validate_guardian_evidence(
    result_dir: Path, plan: dict[str, Any]
) -> dict[str, Any]:
    actual = _load_json(result_dir / "metadata" / "guardian.json")
    if not isinstance(actual, dict):
        raise GateError("conflict guardian cadence summary is not an object")
    parent_pid = _positive(actual.get("parent_pid"), "conflict guardian parent PID")
    precheck_identity = _validate_guard_precheck(
        result_dir / "metadata" / "guardian-precheck.json"
    )
    if (
        parent_pid != precheck_identity.pid
        or actual.get("parent_ppid") != precheck_identity.ppid
        or actual.get("parent_starttime_ticks") != precheck_identity.starttime
    ):
        raise GateError("conflict precheck and continuous guardian parent differ")
    expected = _guardian_summary_from_samples(
        result_dir / "metadata" / "guardian-samples.tsv",
        parent_pid,
        result_dir.parent.resolve(strict=True),
        _load_capacity_contract(
            result_dir / "metadata" / "capacity-contract.json"
        )["operational_floor_bytes"],
        plan["guard_interval_ms"],
    )
    if actual != expected:
        raise GateError("conflict guardian cadence summary differs from raw samples")
    return expected


def _measurement_preconditions(
    result_dir: Path,
    plan: dict[str, Any],
    replay_rows: list[dict[str, Any]],
    query_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Parse every sealed cache-residency and writeback precondition record."""
    residency: list[dict[str, Any]] = []
    writeback: list[dict[str, Any]] = []

    def add_residency(
        evidence: Path,
        phase: str,
        paths_file: Path,
        ceiling_bytes: int | None,
        scope: str,
        observation: str,
    ) -> None:
        residency.append(
            {
                "artifact": _result_relative(
                    evidence, result_dir, "residency evidence"
                ),
                "scope": scope,
                "observation": observation,
                **validate_residency_evidence(
                    evidence,
                    phase,
                    paths_file,
                    ceiling_bytes,
                    plan["page_size_bytes"],
                ),
            }
        )

    def add_writeback(evidence: Path, phase: str, scope: str) -> None:
        writeback.append(
            {
                "artifact": _result_relative(
                    evidence, result_dir, "writeback evidence"
                ),
                "scope": scope,
                **validate_writeback_evidence(
                    evidence, phase, plan["max_dirty_writeback_bytes"]
                ),
            }
        )

    capture_paths = result_dir / "inventory" / "capture-files.nul"
    for row in replay_rows:
        run_dir = row["run_dir"]
        label = row["label"]
        add_residency(
            run_dir / "capture-residency-before.tsv",
            f"{label}-capture-after-evict",
            capture_paths,
            plan["max_capture_resident_bytes_after_evict"],
            "capture",
            "after_evict",
        )
        add_writeback(
            run_dir / "writeback-before.tsv", f"{label}-before-replay", "replay"
        )

    for codec in CODECS:
        add_writeback(
            result_dir / "validation" / codec / "writeback-before-verifier.tsv",
            f"{codec}-before-verifier",
            "verifier",
        )

    for row in query_rows:
        run_dir = row["run_dir"]
        process_label = row["process_label"]
        paths_file = result_dir / "inventory" / f"{row['codec']}-files.nul"
        add_writeback(
            run_dir / "writeback-before.tsv",
            f"{process_label}-before-query",
            "query",
        )
        add_residency(
            run_dir / "residency-after-evict.tsv",
            f"{process_label}-corpus-after-evict",
            paths_file,
            plan["max_corpus_resident_bytes_after_evict"],
            "corpus",
            "after_evict",
        )
        add_residency(
            run_dir / "residency-after-run.tsv",
            f"{process_label}-corpus-after-run",
            paths_file,
            None,
            "corpus",
            "after_run",
        )

    return {
        "schema": MEASUREMENT_PRECONDITIONS_SCHEMA,
        "status": "pass",
        "controls": {
            "max_capture_resident_bytes_after_evict": plan[
                "max_capture_resident_bytes_after_evict"
            ],
            "max_corpus_resident_bytes_after_evict": plan[
                "max_corpus_resident_bytes_after_evict"
            ],
            "max_dirty_writeback_bytes": plan["max_dirty_writeback_bytes"],
            "page_size_bytes": plan["page_size_bytes"],
        },
        "counts": {
            "capture_residency_admissions": len(replay_rows),
            "corpus_residency_admissions": len(query_rows),
            "corpus_residency_after_run_observations": len(query_rows),
            "writeback_admissions": len(replay_rows) + len(CODECS) + len(query_rows),
        },
        "residency_evidence": residency,
        "writeback_evidence": writeback,
    }


def _validate_saved_perf_evidence(
    result_dir: Path,
    replay_rows: list[dict[str, Any]],
    query_rows: list[dict[str, Any]],
    perf_enabled: bool,
) -> None:
    if not perf_enabled:
        return
    sources = [
        (
            result_dir / "metadata" / "perf-preflight.tsv",
            result_dir / "metadata" / "perf-preflight.json",
        )
    ]
    sources.extend(
        (row["run_dir"] / "perf.tsv", row["run_dir"] / "perf.json")
        for row in replay_rows
    )
    sources.extend(
        (row["run_dir"] / "perf.tsv", row["run_dir"] / "perf.json")
        for row in query_rows
    )
    with tempfile.TemporaryDirectory(
        prefix="phase6-final-perf-", dir=result_dir.parent
    ) as temporary:
        staging = Path(temporary)
        for index, (raw, parsed) in enumerate(sources):
            rebuilt = staging / f"{index:03d}.json"
            parse_perf(raw, rebuilt)
            if parsed.read_bytes() != rebuilt.read_bytes():
                raise GateError(
                    "saved perf evidence differs from strict raw reconstruction: "
                    f"{_result_relative(parsed, result_dir, 'perf evidence')}"
                )


def _validate_finalized_artifact_matrix(
    result_dir: Path, admission: dict[str, Any]
) -> None:
    """Recheck the complete admitted layout before constructing its final seal."""
    expected_admission_fields = {
        "schema",
        "status",
        "promotion_eligibility",
        "raw_authority_sha256",
        "raw_seals_verified",
        "derived_artifacts_reconstructed",
        "derived_artifact_paths",
        "capacity",
        "measurement_preconditions",
    }
    if set(admission) != expected_admission_fields:
        raise GateError("final admission result has an invalid shape")
    plan = _load_admission_plan(
        result_dir, result_dir / "metadata" / "admission-plan.json"
    )
    if admission["promotion_eligibility"] != plan["promotion_eligibility"]:
        raise GateError("final admission promotion eligibility differs from its plan")
    perf_enabled = _effective_perf_policy(result_dir, plan)
    replay_rows = _replay_schedule(result_dir, plan["replay_blocks"])
    corpora = _representative_corpora(replay_rows)
    queries = read_normalized_manifest(result_dir / "queries.normalized.json")
    query_rows = _query_schedule(result_dir, queries, plan["query_blocks"], corpora)
    _validate_final_admission_layout(
        result_dir,
        plan,
        replay_rows,
        query_rows,
        perf_enabled,
        finalized=True,
    )
    if admission["capacity"] != _validate_capacity_evidence(
        result_dir, plan, replay_rows
    ):
        raise GateError("final admission capacity reconstruction differs")
    _validate_guardian_evidence(result_dir, plan)

    raw_seals = [f"replays/{row['label']}/raw-leaves.json" for row in replay_rows]
    raw_seals.extend(
        f"validation/{codec}/storage-verify-raw-leaves.json" for codec in CODECS
    )
    raw_seals.extend(
        f"validation/{codec}/readback-raw-leaves.json" for codec in CODECS
    )
    raw_seals.extend(
        f"query-runs/{row['process_label']}/raw-leaves.json" for row in query_rows
    )
    raw_seals.append("metadata/final-raw-leaves.json")
    if admission["raw_seals_verified"] != len(raw_seals):
        raise GateError("final admission raw seal count differs from the exact schedule")
    raw_authority = result_dir / "metadata" / "raw-authorities.tsv"
    raw_authority_checksum = result_dir / "metadata" / "raw-authorities.sha256"
    _check_checksum_manifest(raw_authority_checksum, {raw_authority})
    if admission["raw_authority_sha256"] != _sha256(raw_authority):
        raise GateError("final admission raw authority digest differs")
    _check_raw_authorities(result_dir, raw_authority, raw_seals)
    _validate_saved_perf_evidence(
        result_dir, replay_rows, query_rows, perf_enabled
    )
    if admission["measurement_preconditions"] != _measurement_preconditions(
        result_dir, plan, replay_rows, query_rows
    ):
        raise GateError("final admission measurement preconditions differ")


def final_admission(result_dir: Path, plan_path: Path, output: Path) -> None:
    """Rebuild gate evidence from sealed leaves before any completion marker exists."""
    if not result_dir.is_absolute():
        raise GateError("final admission result root must be absolute")
    try:
        root_metadata = result_dir.lstat()
    except OSError as error:
        raise GateError(
            f"cannot inspect final admission result root: {error}"
        ) from error
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise GateError("final admission result root must be a non-symlink directory")
    result_dir = _canonical_directory(result_dir, "final admission result root")
    expected_output = result_dir / "metadata" / "final-admission.json"
    if output.absolute() != expected_output.absolute():
        raise GateError(f"final admission output must be {expected_output}")
    if os.path.lexists(output):
        raise GateError("final admission output already exists")
    for marker in (
        "RAW_GORILLA_COMPLETE_TIMESTAMP_AB_BLOCKED",
        "EXPLORATORY_EXTERNAL_BINARIES_NON_PROMOTABLE.txt",
        "TIMESTAMP_CODEC_AB_BLOCKED.txt",
    ):
        if os.path.lexists(result_dir / marker):
            raise GateError(
                f"completion marker or note exists before final admission: {marker}"
            )

    plan = _load_admission_plan(result_dir, plan_path)
    capture = Path(plan["capture"])
    repo = Path(plan["repo"])
    query_manifest = Path(plan["query_manifest"])
    config_template = Path(plan["config_template"])
    validated_input_config_template = Path(plan["validated_input_config_template"])
    expectations = Path(plan["expectations"])
    if config_template != result_dir / "metadata" / "config-template.toml":
        raise GateError(
            "admission plan config template is not the preserved controlled copy"
        )
    if (
        query_manifest
        != result_dir / "metadata" / "harness" / "phase6_codec_queries.json"
    ):
        raise GateError("admission plan query manifest is not the frozen harness copy")
    if expectations != result_dir / "metadata" / "harness" / "phase1_4m_expectations.json":
        raise GateError("admission plan expectations are not the frozen harness copy")
    _regular_file(config_template, "admission config template")
    _regular_file(
        validated_input_config_template, "admission validated input config template"
    )
    if validated_input_config_template.read_bytes() != config_template.read_bytes():
        raise GateError("validated input config changed from its preserved controlled copy")
    _regular_file(query_manifest, "admission query manifest")
    _regular_file(expectations, "admission capacity expectations")

    perf_enabled = _effective_perf_policy(result_dir, plan)

    replay_rows = _replay_schedule(result_dir, plan["replay_blocks"])
    corpora = _representative_corpora(replay_rows)

    reconstructed: list[str] = []
    with tempfile.TemporaryDirectory(
        prefix="phase6-final-admission-", dir=result_dir.parent
    ) as temporary:
        staging = Path(temporary)

        contract_document = _load_json(
            result_dir / "metadata" / "capacity-contract.json"
        )
        if not isinstance(contract_document, dict):
            raise GateError("capacity contract is not an object")
        rebuilt_capacity_contract = staging / "capacity-contract.json"
        _write_json(
            rebuilt_capacity_contract,
            build_capacity_contract(
                expectations,
                repo,
                contract_document.get("source_head"),
                plan["replay_blocks"],
            ),
        )
        _compare_bytes(
            result_dir / "metadata" / "capacity-contract.json",
            rebuilt_capacity_contract,
            reconstructed,
            result_dir,
        )
        rebuilt_validated_inputs = staging / "validated-inputs.json"
        _write_json(
            rebuilt_validated_inputs,
            replay.validate_inputs(
                capture, validated_input_config_template, expectations
            ),
        )
        _compare_bytes(
            result_dir / "metadata" / "validated-inputs.json",
            rebuilt_validated_inputs,
            reconstructed,
            result_dir,
        )

        normalized_tsv = staging / "queries.tsv"
        normalized_json = staging / "queries.normalized.json"
        normalize_manifest(query_manifest, normalized_tsv, normalized_json, 0)
        _compare_bytes(
            result_dir / "queries.tsv", normalized_tsv, reconstructed, result_dir
        )
        _compare_bytes(
            result_dir / "queries.normalized.json",
            normalized_json,
            reconstructed,
            result_dir,
        )
        queries = read_normalized_manifest(normalized_json)
        query_rows = _query_schedule(result_dir, queries, plan["query_blocks"], corpora)
        _validate_final_admission_layout(
            result_dir,
            plan,
            replay_rows,
            query_rows,
            perf_enabled,
        )
        _validate_binary_provenance_table(result_dir)
        _validate_settings(result_dir, plan)
        _validate_seal_check_log(result_dir, plan, replay_rows, query_rows)
        capacity_evidence = _validate_capacity_evidence(result_dir, plan, replay_rows)

        config_paths = {row["config"] for row in replay_rows}
        _check_checksum_manifest(
            result_dir / "metadata" / "config-template.sha256",
            {config_template},
        )
        _check_checksum_manifest(
            result_dir / "metadata" / "rendered-configs.sha256",
            config_paths,
        )
        binary_paths = {
            result_dir / "metadata" / "binaries" / name
            for name in (
                "chronoxide-ingester",
                "chronoxide-query",
                "chronoxide-storage-verify",
            )
        }
        _check_checksum_manifest(
            result_dir / "metadata" / "preserved-binaries.sha256",
            binary_paths,
        )
        for binary_path in binary_paths:
            _require_mode(binary_path, 0o555, "preserved binary")
        _require_mode(
            result_dir / "metadata" / "preserved-binaries.sha256",
            0o444,
            "preserved binary checksum authority",
        )
        harness_dir = result_dir / "metadata" / "harness"
        harness_files, harness_directories = _directory_entry_names(
            harness_dir, "frozen harness"
        )
        if harness_directories:
            raise GateError("frozen harness contains unexpected directories")
        _check_checksum_manifest(
            result_dir / "metadata" / "harness.sha256",
            {harness_dir / name for name in harness_files},
        )
        if stat.S_IMODE(harness_dir.lstat().st_mode) != 0o555:
            raise GateError("frozen harness directory is not read-only")
        for name in harness_files:
            mode = stat.S_IMODE((harness_dir / name).lstat().st_mode)
            if mode not in {0o444, 0o555}:
                raise GateError(f"frozen harness file has an invalid mode: {name}")
        _require_mode(
            result_dir / "metadata" / "harness.sha256",
            0o444,
            "frozen harness checksum authority",
        )
        controlled_paths = {
            config_template,
            result_dir / "metadata" / "config-template.sha256",
            result_dir / "metadata" / "fadvise.sha256",
            result_dir / "metadata" / "rendered-configs.sha256",
            result_dir / "metadata" / "capacity-contract.json",
            result_dir / "metadata" / "validated-inputs.json",
            result_dir / "metadata" / "fadvise-regular-dontneed",
            result_dir / "metadata" / "admission-plan.json",
            result_dir / "queries.tsv",
            result_dir / "queries.normalized.json",
            result_dir / "replay-plan.tsv",
            *config_paths,
        }
        _check_checksum_manifest(
            result_dir / "metadata" / "controlled-inputs.sha256",
            controlled_paths,
        )
        for controlled_path in controlled_paths:
            expected_mode = (
                0o555
                if controlled_path
                == result_dir / "metadata" / "fadvise-regular-dontneed"
                else 0o444
            )
            _require_mode(controlled_path, expected_mode, "controlled experiment input")
        _require_mode(
            result_dir / "metadata" / "controlled-inputs.sha256",
            0o444,
            "controlled input checksum authority",
        )
        _check_checksum_manifest(
            result_dir / "metadata" / "fadvise.sha256",
            {result_dir / "metadata" / "fadvise-regular-dontneed"},
        )

        raw_seals = [f"replays/{row['label']}/raw-leaves.json" for row in replay_rows]
        raw_seals.extend(
            f"validation/{codec}/storage-verify-raw-leaves.json" for codec in CODECS
        )
        raw_seals.extend(
            f"validation/{codec}/readback-raw-leaves.json" for codec in CODECS
        )
        raw_seals.extend(
            f"query-runs/{row['process_label']}/raw-leaves.json" for row in query_rows
        )
        raw_seals.append("metadata/final-raw-leaves.json")
        raw_authority = result_dir / "metadata" / "raw-authorities.tsv"
        raw_authority_checksum = result_dir / "metadata" / "raw-authorities.sha256"
        if stat.S_IMODE(raw_authority.lstat().st_mode) != 0o444:
            raise GateError("raw-authorities.tsv is not read-only")
        if stat.S_IMODE(raw_authority_checksum.lstat().st_mode) != 0o444:
            raise GateError("raw-authorities.sha256 is not read-only")
        _check_checksum_manifest(raw_authority_checksum, {raw_authority})
        _check_raw_authorities(result_dir, raw_authority, raw_seals)

        replay_plan_fields = [
            "label",
            "ordinal",
            "block",
            "slot",
            "codec",
            "config",
            "segments_dir",
            "codec_bound_bytes",
            "codec_safe_reserve_bytes",
            "future_safe_reserve_bytes",
            "pre_required_free_bytes",
            "monitor_minimum_free_bytes",
        ]
        replay_plan_rows = [
            {
                "label": row["label"],
                "ordinal": row["ordinal"],
                "block": row["block"],
                "slot": row["slot"],
                "codec": row["codec"],
                "config": row["config"],
                "segments_dir": row["segments"],
                "codec_bound_bytes": row["codec_bound_bytes"],
                "codec_safe_reserve_bytes": row["codec_safe_reserve_bytes"],
                "future_safe_reserve_bytes": row["future_safe_reserve_bytes"],
                "pre_required_free_bytes": row["pre_required_free_bytes"],
                "monitor_minimum_free_bytes": row["monitor_minimum_free_bytes"],
            }
            for row in replay_rows
        ]
        rebuilt_replay_plan = staging / "replay-plan.tsv"
        _write_exclusive_bytes(
            rebuilt_replay_plan,
            _write_tsv_bytes(replay_plan_fields, replay_plan_rows),
        )
        _compare_bytes(
            result_dir / "replay-plan.tsv",
            rebuilt_replay_plan,
            reconstructed,
            result_dir,
        )

        capture_json = staging / "capture.json"
        capture_paths = staging / "capture-files.nul"
        capture_inventory(capture, capture_json, capture_paths)
        for actual in (
            result_dir / "inventory" / "capture.json",
            result_dir / "inventory" / "capture-after-replays.json",
        ):
            _compare_bytes(actual, capture_json, reconstructed, result_dir)
        for actual in (
            result_dir / "inventory" / "capture-files.nul",
            result_dir / "inventory" / "capture-files-after-replays.nul",
        ):
            _compare_bytes(actual, capture_paths, reconstructed, result_dir)

        replay_index_fields = [
            "label",
            "block",
            "slot",
            "codec",
            "config_json",
            "correctness_json",
            "manifest_tsv",
            "corpus_summary_json",
            "time_json",
            "rss_json",
            "seal_json",
            "perf_json",
        ]
        replay_index_rows: list[dict[str, Any]] = []
        ingester = result_dir / "metadata" / "binaries" / "chronoxide-ingester"
        for row in replay_rows:
            label = row["label"]
            run_dir = row["run_dir"]
            report = _single_ingestion_report(run_dir)
            prefix = f"replays/{label}"
            raw_files = {
                f"{prefix}/{name}"
                for name in (
                    "capture-residency-before.tsv",
                    "capacity-after.json",
                    "capacity-before.json",
                    "capacity-monitor.exit-status",
                    "capacity-monitor.log",
                    "capacity-monitor.ready",
                    "capacity-samples.tsv",
                    "invocation.json",
                    "pressure-after.txt",
                    "pressure-before.txt",
                    "replay.exit-status",
                    "replay-monitor-control.json",
                    "replay.launch",
                    "replay.log",
                    "replay.time.txt",
                    "rss-monitor.exit-status",
                    "rss-monitor.log",
                    "rss-monitor.ready",
                    "rss-samples.tsv",
                    "runtime-identity.json",
                    "writeback-before.tsv",
                )
            }
            raw_files.add(_result_relative(report, result_dir, "ingestion report"))
            if perf_enabled:
                raw_files.add(f"{prefix}/perf.tsv")
            _require_raw_seal_contract(
                result_dir,
                f"{prefix}/raw-leaves.json",
                raw_files,
                {f"{prefix}/segments"},
            )
            _read_zero_status(run_dir / "replay.exit-status", "replay exit status")
            _read_zero_status(
                run_dir / "rss-monitor.exit-status", "RSS monitor exit status"
            )
            if (run_dir / "rss-monitor.log").read_bytes():
                raise GateError(f"RSS monitor log is not empty: {label}")

            rendered, config_metadata = _rendered_config(
                config_template,
                row["config"],
                capture,
                row["segments"],
                plan["stop_after_messages"],
                row["codec"],
            )
            rebuilt_config = staging / "configs" / f"{label}.toml"
            _write_exclusive_bytes(rebuilt_config, rendered)
            _compare_bytes(row["config"], rebuilt_config, reconstructed, result_dir)
            rebuilt_config_json = staging / label / "config.json"
            _write_exclusive_bytes(
                rebuilt_config_json,
                (json.dumps(config_metadata, sort_keys=True) + "\n").encode(),
            )
            _compare_bytes(
                run_dir / "config.json", rebuilt_config_json, reconstructed, result_dir
            )

            rebuilt_runtime = staging / label / "runtime-identity.json"
            _write_json(
                rebuilt_runtime,
                runtime_identity(
                    ingester,
                    "ingester",
                    [
                        "LC_ALL=C",
                        "TZ=UTC",
                        f"CONFIG_FILE={row['config']}",
                        f"RUST_LOG={plan['rust_log']}",
                    ],
                    {"CONFIG_FILE"},
                ),
            )
            _require_mode(
                run_dir / "runtime-identity.json", 0o444, "replay runtime identity"
            )
            _compare_bytes(
                run_dir / "runtime-identity.json",
                rebuilt_runtime,
                reconstructed,
                result_dir,
            )
            rebuilt_invocation = staging / label / "invocation.json"
            write_invocation(
                ingester,
                "ingester",
                [],
                [
                    "LC_ALL=C",
                    "TZ=UTC",
                    f"CONFIG_FILE={row['config']}",
                    f"RUST_LOG={plan['rust_log']}",
                ],
                rebuilt_invocation,
            )
            _require_mode(run_dir / "invocation.json", 0o444, "replay invocation")
            _compare_bytes(
                run_dir / "invocation.json",
                rebuilt_invocation,
                reconstructed,
                result_dir,
            )

            rebuilt_time = staging / label / "replay.time.json"
            replay.parse_gnu_time(run_dir / "replay.time.txt", rebuilt_time)
            _compare_bytes(
                run_dir / "replay.time.json", rebuilt_time, reconstructed, result_dir
            )
            if perf_enabled:
                rebuilt_perf = staging / label / "perf.json"
                parse_perf(run_dir / "perf.tsv", rebuilt_perf)
                _compare_bytes(
                    run_dir / "perf.json", rebuilt_perf, reconstructed, result_dir
                )
            rebuilt_correctness = staging / label / "replay-correctness.json"
            parse_replay_report(report, rebuilt_correctness)
            _compare_bytes(
                run_dir / "replay-correctness.json",
                rebuilt_correctness,
                reconstructed,
                result_dir,
            )
            rebuilt_seal = staging / label / "seal.json"
            parse_seal_log(run_dir / "replay.log", rebuilt_seal)
            _compare_bytes(
                run_dir / "seal.json", rebuilt_seal, reconstructed, result_dir
            )
            rebuilt_manifest = staging / label / "segments.sha256"
            rebuilt_inventory = staging / label / "segments.tsv"
            rebuilt_summary = staging / label / "corpus-summary.json"
            replay.write_tree_manifest(
                row["segments"],
                rebuilt_manifest,
                rebuilt_inventory,
                rebuilt_summary,
            )
            for actual, rebuilt in (
                (run_dir / "segments.sha256", rebuilt_manifest),
                (run_dir / "segments.tsv", rebuilt_inventory),
                (run_dir / "corpus-summary.json", rebuilt_summary),
            ):
                _compare_bytes(actual, rebuilt, reconstructed, result_dir)
            rebuilt_artifacts = staging / label / "artifacts.json"
            artifact_inventory(row["segments"], rebuilt_artifacts)
            _compare_bytes(
                run_dir / "artifacts.json", rebuilt_artifacts, reconstructed, result_dir
            )

            rss_from_samples = _rss_summary_from_samples(
                run_dir / "rss-samples.tsv",
                plan["rss_interval_ms"],
                run_dir / "replay-monitor-control.json",
                run_dir / "rss-monitor.ready",
                run_dir / "capacity-monitor.ready",
                run_dir / "replay.launch",
            )
            actual_rss = _load_json(run_dir / "rss.json")
            if not isinstance(actual_rss, dict):
                raise GateError(f"RSS summary has an invalid shape: {label}")
            rebuilt_rss = staging / label / "rss.json"
            _write_json(rebuilt_rss, rss_from_samples)
            _compare_bytes(run_dir / "rss.json", rebuilt_rss, reconstructed, result_dir)

            actual_capacity = _load_json(run_dir / "capacity.json")
            rebuilt_capacity = staging / label / "capacity.json"
            _write_json(
                rebuilt_capacity,
                _capacity_summary_from_samples(
                    run_dir / "capacity-samples.tsv",
                    result_dir.parent.resolve(strict=True),
                    row["monitor_minimum_free_bytes"],
                    plan["capacity_monitor_interval_ms"],
                    run_dir / "replay-monitor-control.json",
                    run_dir / "rss-monitor.ready",
                    run_dir / "capacity-monitor.ready",
                    run_dir / "replay.launch",
                ),
            )
            _compare_bytes(
                run_dir / "capacity.json", rebuilt_capacity, reconstructed, result_dir
            )
            rebuilt_corpus_capacity = staging / label / "capacity-corpus-check.json"
            _write_json(
                rebuilt_corpus_capacity,
                check_corpus_capacity(
                    run_dir / "corpus-summary.json",
                    result_dir / "metadata" / "capacity-contract.json",
                    row["codec"],
                ),
            )
            _compare_bytes(
                run_dir / "capacity-corpus-check.json",
                rebuilt_corpus_capacity,
                reconstructed,
                result_dir,
            )

            replay_index_rows.append(
                {
                    "label": label,
                    "block": row["block"],
                    "slot": row["slot"],
                    "codec": row["codec"],
                    "config_json": run_dir / "config.json",
                    "correctness_json": run_dir / "replay-correctness.json",
                    "manifest_tsv": run_dir / "segments.tsv",
                    "corpus_summary_json": run_dir / "corpus-summary.json",
                    "time_json": run_dir / "replay.time.json",
                    "rss_json": run_dir / "rss.json",
                    "seal_json": run_dir / "seal.json",
                    "perf_json": run_dir / "perf.json" if perf_enabled else "-",
                }
            )

        rebuilt_replay_index = staging / "replay-index.tsv"
        _write_exclusive_bytes(
            rebuilt_replay_index,
            _write_tsv_bytes(replay_index_fields, replay_index_rows),
        )
        _compare_bytes(
            result_dir / "replay-index.tsv",
            rebuilt_replay_index,
            reconstructed,
            result_dir,
        )
        rebuilt_replay_comparison = staging / "replay-equivalence.json"
        rebuilt_replay_summary = staging / "replay-summary.tsv"
        compare_replays(
            rebuilt_replay_index,
            plan["replay_blocks"],
            rebuilt_replay_comparison,
            rebuilt_replay_summary,
        )
        _compare_bytes(
            result_dir / "comparisons" / "replay-equivalence.json",
            rebuilt_replay_comparison,
            reconstructed,
            result_dir,
        )
        _compare_bytes(
            result_dir / "replay-summary.tsv",
            rebuilt_replay_summary,
            reconstructed,
            result_dir,
        )

        query_binary = result_dir / "metadata" / "binaries" / "chronoxide-query"
        verifier_binary = (
            result_dir / "metadata" / "binaries" / "chronoxide-storage-verify"
        )
        rebuilt_verifier_inputs: dict[str, Path] = {}
        rebuilt_readbacks: dict[str, Path] = {}
        for codec in CODECS:
            validation = result_dir / "validation" / codec
            prefix = f"validation/{codec}"
            verifier_files = {
                f"{prefix}/{name}"
                for name in (
                    "storage-verify.exit-status",
                    "storage-verify-invocation.json",
                    "storage-verify.json",
                    "storage-verify.log",
                    "storage-verify.runtime-identity.json",
                    "storage-verify.time.txt",
                    "writeback-before-verifier.tsv",
                )
            }
            _require_raw_seal_contract(
                result_dir,
                f"{prefix}/storage-verify-raw-leaves.json",
                verifier_files,
                set(),
            )
            _read_zero_status(
                validation / "storage-verify.exit-status",
                f"{codec} verifier exit status",
            )
            rebuilt_runtime = staging / "validation" / codec / "storage-runtime.json"
            rebuilt_runtime.parent.mkdir(parents=True, exist_ok=True)
            _write_json(
                rebuilt_runtime,
                runtime_identity(
                    verifier_binary, "verifier", ["LC_ALL=C", "TZ=UTC"], set()
                ),
            )
            _require_mode(
                validation / "storage-verify.runtime-identity.json",
                0o444,
                "verifier runtime identity",
            )
            _compare_bytes(
                validation / "storage-verify.runtime-identity.json",
                rebuilt_runtime,
                reconstructed,
                result_dir,
            )
            verifier_arguments = [
                "--segments-dir",
                str(corpora[codec]),
                "--schema",
                "schema8",
                "--validate-segment-footers",
                "--verify-exact-postings",
            ]
            rebuilt_invocation = (
                staging / "validation" / codec / "storage-invocation.json"
            )
            write_invocation(
                verifier_binary,
                "verifier",
                verifier_arguments,
                ["LC_ALL=C", "TZ=UTC"],
                rebuilt_invocation,
            )
            _require_mode(
                validation / "storage-verify-invocation.json",
                0o444,
                "verifier invocation",
            )
            _compare_bytes(
                validation / "storage-verify-invocation.json",
                rebuilt_invocation,
                reconstructed,
                result_dir,
            )
            rebuilt_time = staging / "validation" / codec / "storage-time.json"
            verifier_timing = replay.parse_gnu_time(
                validation / "storage-verify.time.txt", rebuilt_time
            )
            if verifier_timing["exit_status"] != 0:
                raise GateError(f"{codec} verifier GNU time status is nonzero")
            _compare_bytes(
                validation / "storage-verify.time.json",
                rebuilt_time,
                reconstructed,
                result_dir,
            )
            rebuilt_verifier_inputs[codec] = validation / "storage-verify.json"

            readback_files = {
                f"{prefix}/{name}"
                for name in (
                    "readback-invocation.json",
                    "readbacks.exit-status",
                    "readbacks.log",
                    "readbacks.md",
                    "readbacks.runtime-identity.json",
                    "readbacks.time.txt",
                )
            }
            _require_raw_seal_contract(
                result_dir,
                f"{prefix}/readback-raw-leaves.json",
                readback_files,
                set(),
            )
            _read_zero_status(
                validation / "readbacks.exit-status", f"{codec} readback exit status"
            )
            rebuilt_runtime = staging / "validation" / codec / "readback-runtime.json"
            _write_json(
                rebuilt_runtime,
                runtime_identity(query_binary, "query", ["LC_ALL=C", "TZ=UTC"], set()),
            )
            _require_mode(
                validation / "readbacks.runtime-identity.json",
                0o444,
                "readback runtime identity",
            )
            _compare_bytes(
                validation / "readbacks.runtime-identity.json",
                rebuilt_runtime,
                reconstructed,
                result_dir,
            )
            readback_arguments = [
                "--segments-dir",
                str(corpora[codec]),
                "--storage-layout",
                "schema8",
                "--sample-limit-per-kind",
                str(plan["readback_sample_limit_per_kind"]),
                "--verify-readbacks",
                "--output",
                str(validation / "readbacks.md"),
            ]
            rebuilt_invocation = (
                staging / "validation" / codec / "readback-invocation.json"
            )
            write_invocation(
                query_binary,
                "query",
                readback_arguments,
                ["LC_ALL=C", "TZ=UTC"],
                rebuilt_invocation,
            )
            _require_mode(
                validation / "readback-invocation.json",
                0o444,
                "readback invocation",
            )
            _compare_bytes(
                validation / "readback-invocation.json",
                rebuilt_invocation,
                reconstructed,
                result_dir,
            )
            rebuilt_readback = staging / "validation" / codec / "readbacks.json"
            check_readback(validation / "readbacks.md", rebuilt_readback)
            _compare_bytes(
                validation / "readbacks.json",
                rebuilt_readback,
                reconstructed,
                result_dir,
            )
            rebuilt_readbacks[codec] = rebuilt_readback
            readback_time = staging / "validation" / codec / "readback-time.json"
            readback_timing = replay.parse_gnu_time(
                validation / "readbacks.time.txt", readback_time
            )
            if readback_timing["exit_status"] != 0:
                raise GateError(f"{codec} readback GNU time status is nonzero")

        if (
            rebuilt_readbacks["raw"].read_bytes()
            != rebuilt_readbacks["gorilla"].read_bytes()
        ):
            raise GateError("independently rebuilt Raw/Gorilla readbacks differ")
        rebuilt_verifier_comparison = staging / "verifier-comparison.json"
        compare_verifiers(
            rebuilt_verifier_inputs["raw"],
            rebuilt_verifier_inputs["gorilla"],
            rebuilt_verifier_comparison,
        )
        _compare_bytes(
            result_dir
            / "comparisons"
            / "verifier-equivalence-and-codec-inventory.json",
            rebuilt_verifier_comparison,
            reconstructed,
            result_dir,
        )

        for codec in CODECS:
            rebuilt_inventory = staging / "inventory" / f"{codec}.json"
            rebuilt_paths = staging / "inventory" / f"{codec}.nul"
            rebuilt_inventory.parent.mkdir(parents=True, exist_ok=True)
            query_common.write_inventory(
                corpora[codec], rebuilt_inventory, rebuilt_paths
            )
            for actual in (
                result_dir / "inventory" / f"{codec}-before.json",
                result_dir / "inventory" / f"{codec}-after.json",
            ):
                _compare_bytes(actual, rebuilt_inventory, reconstructed, result_dir)
            for actual in (
                result_dir / "inventory" / f"{codec}-files.nul",
                result_dir / "inventory" / f"{codec}-files-after.nul",
            ):
                _compare_bytes(actual, rebuilt_paths, reconstructed, result_dir)

        query_index_fields = [
            "process_label",
            "query_name",
            "category",
            "mode",
            "block",
            "slot",
            "codec",
            "corpus",
            "raw_output",
            "max_rss_kib",
            "perf_json",
        ]
        query_index_rows: list[dict[str, Any]] = []
        for row in query_rows:
            run_dir = row["run_dir"]
            prefix = f"query-runs/{row['process_label']}"
            raw_files = {
                f"{prefix}/{name}"
                for name in (
                    "exit-status",
                    "invocation.json",
                    "pressure-after.txt",
                    "pressure-before.txt",
                    "query.log",
                    "raw.json",
                    "report.md",
                    "residency-after-evict.tsv",
                    "residency-after-run.tsv",
                    "runtime-identity.json",
                    "time.txt",
                    "writeback-before.tsv",
                )
            }
            if perf_enabled:
                raw_files.add(f"{prefix}/perf.tsv")
            _require_raw_seal_contract(
                result_dir,
                f"{prefix}/raw-leaves.json",
                raw_files,
                set(),
            )
            _read_zero_status(run_dir / "exit-status", "query exit status")
            rebuilt_runtime = (
                staging / "queries" / row["process_label"] / "runtime.json"
            )
            rebuilt_runtime.parent.mkdir(parents=True, exist_ok=True)
            _write_json(
                rebuilt_runtime,
                runtime_identity(query_binary, "query", ["LC_ALL=C", "TZ=UTC"], set()),
            )
            _require_mode(
                run_dir / "runtime-identity.json", 0o444, "query runtime identity"
            )
            _compare_bytes(
                run_dir / "runtime-identity.json",
                rebuilt_runtime,
                reconstructed,
                result_dir,
            )
            rebuilt_invocation = (
                staging / "queries" / row["process_label"] / "invocation.json"
            )
            write_invocation(
                query_binary,
                "query",
                _query_arguments(row, plan),
                ["LC_ALL=C", "TZ=UTC"],
                rebuilt_invocation,
            )
            _require_mode(run_dir / "invocation.json", 0o444, "query invocation")
            _compare_bytes(
                run_dir / "invocation.json",
                rebuilt_invocation,
                reconstructed,
                result_dir,
            )
            if perf_enabled:
                rebuilt_perf = staging / "queries" / row["process_label"] / "perf.json"
                parse_perf(run_dir / "perf.tsv", rebuilt_perf)
                _compare_bytes(
                    run_dir / "perf.json", rebuilt_perf, reconstructed, result_dir
                )
            parsed_time = staging / "queries" / row["process_label"] / "time.json"
            timing = replay.parse_gnu_time(run_dir / "time.txt", parsed_time)
            if timing["exit_status"] != 0:
                raise GateError(
                    f"query GNU time status is nonzero: {row['process_label']}"
                )
            max_rss = _positive(timing["max_rss_kib"], "query max RSS")
            query_spec = row["query"]
            query_index_rows.append(
                {
                    "process_label": row["process_label"],
                    "query_name": query_spec["query_name"],
                    "category": query_spec["category"],
                    "mode": query_spec["mode"],
                    "block": row["block"],
                    "slot": row["slot"],
                    "codec": row["codec"],
                    "corpus": row["corpus"],
                    "raw_output": run_dir / "raw.json",
                    "max_rss_kib": max_rss,
                    "perf_json": run_dir / "perf.json" if perf_enabled else "-",
                }
            )
        rebuilt_query_index = staging / "query-index.tsv"
        _write_exclusive_bytes(
            rebuilt_query_index,
            _write_tsv_bytes(query_index_fields, query_index_rows),
        )
        _compare_bytes(
            result_dir / "query-index.tsv",
            rebuilt_query_index,
            reconstructed,
            result_dir,
        )
        rebuilt_query_comparison = staging / "query-equivalence.json"
        rebuilt_query_summary = staging / "query-summary.tsv"
        compare_queries(
            argparse.Namespace(
                index=rebuilt_query_index,
                manifest=normalized_json,
                summary=rebuilt_query_summary,
                output=rebuilt_query_comparison,
                blocks=plan["query_blocks"],
                benchmark_repeats=plan["benchmark_repeats"],
                queue_depth=plan["chunk_read_queue_depth"],
                label_materialization="demand-driven",
                max_matched_series=plan["query_max_series_matched"],
                max_projected_series=plan["query_max_projected_series"],
                max_chunk_reads=plan["query_max_chunks_read"],
                max_bytes_read=plan["query_max_bytes_read"],
                max_samples_decoded=plan["query_max_samples"],
                max_regex_values_examined=plan["regex_max_expanded_values"],
            )
        )
        _compare_bytes(
            result_dir / "comparisons" / "query-equivalence.json",
            rebuilt_query_comparison,
            reconstructed,
            result_dir,
        )
        _compare_bytes(
            result_dir / "query-summary.tsv",
            rebuilt_query_summary,
            reconstructed,
            result_dir,
        )

        global_raw_files = {
            "metadata/capacity-final.json",
            "metadata/capacity-postbuild.json",
            "metadata/capacity-prebuild.json",
            "metadata/environment.txt",
            "metadata/guardian-conflicts.tsv",
            "metadata/guardian.log",
            "metadata/guardian-precheck.json",
            "metadata/guardian.ready",
            "metadata/guardian-samples.tsv",
            "metadata/guardian.stop",
            "metadata/perf-effective.txt",
        }
        if plan["perf_stat_mode"] != "off":
            global_raw_files |= {
                "metadata/perf-preflight.exit-status",
                "metadata/perf-preflight.log",
                "metadata/perf-preflight.tsv",
            }
        _require_raw_seal_contract(
            result_dir,
            "metadata/final-raw-leaves.json",
            global_raw_files,
            set(),
        )
        guardian_header = "recorded_at_ns\tpid\tppid\tcomm\tcmdline\n"
        if (result_dir / "metadata" / "guardian-conflicts.tsv").read_text(
            encoding="utf-8"
        ) != guardian_header:
            raise GateError(
                "conflict guardian recorded a workload or malformed evidence"
            )
        if (result_dir / "metadata" / "guardian.log").read_bytes():
            raise GateError("conflict guardian log is not empty")
        _validate_empty_read_only_marker(
            result_dir / "metadata" / "guardian.stop",
            "conflict guardian stop marker",
        )
        _validate_empty_read_only_marker(
            result_dir / "metadata" / "guardian.ready",
            "conflict guardian ready marker",
        )
        actual_guardian = _load_json(result_dir / "metadata" / "guardian.json")
        if not isinstance(actual_guardian, dict):
            raise GateError("conflict guardian cadence summary is not an object")
        guardian_parent = _positive(
            actual_guardian.get("parent_pid"), "conflict guardian parent PID"
        )
        guardian_precheck = _validate_guard_precheck(
            result_dir / "metadata" / "guardian-precheck.json"
        )
        if (
            guardian_parent != guardian_precheck.pid
            or actual_guardian.get("parent_ppid") != guardian_precheck.ppid
            or actual_guardian.get("parent_starttime_ticks")
            != guardian_precheck.starttime
        ):
            raise GateError("conflict precheck and continuous guardian parent differ")
        rebuilt_guardian = staging / "guardian.json"
        _write_json(
            rebuilt_guardian,
            _guardian_summary_from_samples(
                result_dir / "metadata" / "guardian-samples.tsv",
                guardian_parent,
                result_dir.parent.resolve(strict=True),
                _load_capacity_contract(
                    result_dir / "metadata" / "capacity-contract.json"
                )["operational_floor_bytes"],
                plan["guard_interval_ms"],
            ),
        )
        _compare_bytes(
            result_dir / "metadata" / "guardian.json",
            rebuilt_guardian,
            reconstructed,
            result_dir,
        )
        preflight_json = result_dir / "metadata" / "perf-preflight.json"
        if perf_enabled:
            rebuilt_preflight = staging / "perf-preflight.json"
            parse_perf(
                result_dir / "metadata" / "perf-preflight.tsv", rebuilt_preflight
            )
            _compare_bytes(preflight_json, rebuilt_preflight, reconstructed, result_dir)
        elif preflight_json.exists():
            raise GateError(
                "disabled effective perf unexpectedly has parsed preflight evidence"
            )

        if plan["binary_provenance_mode"] == "internal":
            source_dir = result_dir / "metadata" / "source"
            source_seal_path = source_dir / "formal-source-seal.json"
            snapshot_seal_path = source_dir / "source-snapshot-seal.json"
            archive_manifest = source_dir / "source-head.tar.sha256"
            _check_checksum_manifest(archive_manifest, {source_dir / "source-head.tar"})
            source_check = check_source_seal(repo, source_seal_path)
            snapshot_check = check_source_snapshot_seal(
                repo,
                result_dir / "build-source",
                source_seal_path,
                snapshot_seal_path,
            )
            cargo_check = cargo_config_isolation(
                result_dir / "build-source",
                result_dir / "metadata" / "build" / "cargo-home",
            )
            for actual, expected in (
                (
                    result_dir / "metadata" / "build" / "source-check-final.json",
                    source_check,
                ),
                (
                    result_dir
                    / "metadata"
                    / "build"
                    / "source-snapshot-check-final.json",
                    snapshot_check,
                ),
                (
                    result_dir
                    / "metadata"
                    / "build"
                    / "cargo-config-isolation-final.json",
                    cargo_check,
                ),
            ):
                if _load_json(actual) != expected:
                    raise GateError(
                        "final source-derived evidence differs from independent reconstruction: "
                        f"{_result_relative(actual, result_dir, 'source evidence')}"
                    )
                reconstructed.append(
                    _result_relative(actual, result_dir, "source evidence")
                )

        measurement_preconditions = _measurement_preconditions(
            result_dir, plan, replay_rows, query_rows
        )

        # Rehash every authority-bound raw leaf after all independent parsing and hashing.
        _check_checksum_manifest(raw_authority_checksum, {raw_authority})
        _check_raw_authorities(result_dir, raw_authority, raw_seals)
        capture_json_after = staging / "capture-final.json"
        capture_paths_after = staging / "capture-files-final.nul"
        capture_inventory(capture, capture_json_after, capture_paths_after)
        if (
            capture_json_after.read_bytes() != capture_json.read_bytes()
            or capture_paths_after.read_bytes() != capture_paths.read_bytes()
        ):
            raise GateError("capture changed during final admission")

    _write_json(
        output,
        {
            "schema": FINAL_ADMISSION_SCHEMA,
            "status": "pass",
            "promotion_eligibility": plan["promotion_eligibility"],
            "raw_authority_sha256": _sha256(
                result_dir / "metadata" / "raw-authorities.tsv"
            ),
            "raw_seals_verified": len(raw_seals),
            "derived_artifacts_reconstructed": len(reconstructed),
            "derived_artifact_paths": sorted(
                reconstructed, key=lambda item: item.encode()
            ),
            "capacity": capacity_evidence,
            "measurement_preconditions": measurement_preconditions,
        },
    )
    output.chmod(0o444)


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
INTERACTIVE_MONITOR_PROCESS_TOKEN = (
    r"(?:top|btop|bpytop|htop|atop|iotop|iotop-c|nmon|glances|powertop|nvtop)"
)
FORBIDDEN_PROCESS_NAMES = re.compile(
    rf"^(?:cargo|cargo-nextest|rustc|rustdoc|clippy-driver|nextest|make|"
    rf"{NINJA_PROCESS_TOKEN}|cmake|meson|sccache|ccache|"
    rf"{CONTAINER_CLIENT_PROCESS_TOKEN}|"
    rf"emulator|adb|android-studio|studio64|gradle|gradlew|GradleDaemon|"
    rf"{COMPILER_PROCESS_TOKEN}|"
    rf"cc1|cc1plus|{LINKER_PROCESS_TOKEN}|perf|heaptrack|valgrind.*|strace|"
    rf"ltrace|bpftrace|hotspot|chronoxide-.*|greptime.*|clickhouse.*|"
    rf"postgres(?::.*)?|mysqld|mariadbd|influxd|victoria.*|"
    rf"vm(?:storage|select|agent)|mimir.*|thanos.*|cortex.*|prometheus|"
    rf"mongod|cockroach|scylla|cassandra|redis-server|{SOONG_PROCESS_TOKEN}|"
    rf"ckati|kati|javac|kotlinc|metalava|aapt|aapt2|aidl|dex2oat|"
    rf"qemu-kvm|qemu-system.*|{INTERACTIVE_MONITOR_PROCESS_TOKEN})$",
    re.IGNORECASE,
)
FORBIDDEN_PROCESS_COMMAND = re.compile(
    rf"(?:^|[/ ])(?:cargo(?:-nextest)?|rustc|rustdoc|clippy-driver|nextest|"
    rf"{NINJA_PROCESS_TOKEN}|{COMPILER_PROCESS_TOKEN}|{LINKER_PROCESS_TOKEN}|"
    rf"{SOONG_PROCESS_TOKEN}|ckati|kati|gradlew?|metalava|aapt2?|aidl|"
    rf"dex2oat)(?:$|[ /])",
    re.IGNORECASE,
)
ANDROID_VM_PROCESS_COMMAND = re.compile(
    r"(?:org\.gradle\.|gradleworker|gradle-daemon|/android-studio|studio64|"
    r"/emulator|qemu-system-|redroid|artracer|cuttlefish|android[-_/ ]|"
    r"system-qemu|goldfish|ranchu)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _ProcessIdentity:
    pid: int
    ppid: int
    state: str
    starttime: int


@dataclass(frozen=True)
class _OwnedProcessIdentity:
    pid: int
    ppid: int
    state: str
    starttime: int
    depth: int


_DEAD_PROCESS_STATES = {"Z", "X", "x"}


def _is_conflict_process(comm: str, cmdline: str) -> bool:
    """Classify external work that invalidates a controlled measurement.

    Exact process-name matching covers profilers, databases, and interactive
    monitors without rejecting unrelated commands that merely mention them.
    The bounded command matcher additionally catches build tools launched
    through shell, Python, or Java wrappers. Generic Java/IDE work remains an
    operator-confirmed noise condition unless its command identifies an
    Android or Gradle workload. Phase 6 deliberately retains its conservative
    rule that every qemu-system/qemu-kvm process is a conflict.
    """
    if FORBIDDEN_PROCESS_NAMES.fullmatch(comm.strip()) is not None:
        return True
    command = cmdline.strip()
    if FORBIDDEN_PROCESS_COMMAND.search(command) is not None:
        return True
    lowered_name = comm.strip().casefold()
    qemu_or_java = (
        lowered_name == "java"
        or lowered_name == "qemu-kvm"
        or lowered_name.startswith("qemu-system")
    )
    return qemu_or_java and ANDROID_VM_PROCESS_COMMAND.search(command) is not None


def _process_snapshot() -> dict[int, tuple[int, str, str]]:
    result: dict[int, tuple[int, str, str]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat_text = (entry / "stat").read_text(encoding="ascii")
            close = stat_text.rfind(")")
            if close < 0:
                continue
            after_comm = stat_text[close + 2 :].split()
            ppid = int(after_comm[1])
            comm = (entry / "comm").read_text(encoding="utf-8").strip()
            cmdline = (
                (entry / "cmdline")
                .read_bytes()
                .replace(b"\0", b" ")
                .decode("utf-8", errors="replace")
                .strip()
            )
            result[int(entry.name)] = (ppid, comm, cmdline)
        except (
            FileNotFoundError,
            PermissionError,
            ProcessLookupError,
            ValueError,
            IndexError,
        ):
            continue
    return result


def _descendant_processes(
    processes: dict[int, tuple[int, str, str]], parent_pid: int
) -> set[int]:
    # Exclusion is based only on exact observed ancestry. Command names and
    # arguments never grant an exemption to a lookalike external workload.
    allowed = {parent_pid}
    changed = True
    while changed:
        changed = False
        for pid, (ppid, _comm, _cmdline) in processes.items():
            if ppid in allowed and pid not in allowed:
                allowed.add(pid)
                changed = True
    return allowed


def _conflicts_in_snapshot(
    processes: dict[int, tuple[int, str, str]], parent_pid: int
) -> list[tuple[int, int, str, str]]:
    allowed = _descendant_processes(processes, parent_pid)
    return sorted(
        (
            (pid, ppid, comm, cmdline)
            for pid, (ppid, comm, cmdline) in processes.items()
            if pid not in allowed and _is_conflict_process(comm, cmdline)
        ),
        key=lambda row: row[0],
    )


def check_current_conflicts(parent_pid: int) -> dict[str, Any]:
    _positive(parent_pid, "conflict precheck parent PID")
    parent_identity = _require_live_process_identity(
        parent_pid, "conflict precheck parent"
    )
    conflicts = _conflicts_in_snapshot(_process_snapshot(), parent_pid)
    if conflicts:
        pid, _ppid, comm, _cmdline = conflicts[0]
        raise GateError(
            f"conflict precheck detected unrelated workload pid={pid} comm={comm}"
        )
    if not _process_identity_is_running(
        parent_identity, "conflict precheck parent"
    ):
        raise GateError("conflict precheck parent disappeared during its scan")
    return {
        "schema": GUARD_PRECHECK_SCHEMA,
        "recorded_at_ns": time.time_ns(),
        "parent_pid": parent_pid,
        "parent_ppid": parent_identity.ppid,
        "parent_starttime_ticks": parent_identity.starttime,
        "conflicts": [],
        "status": "pass",
    }


def _parse_process_identity(pid: int, stat_text: str) -> _ProcessIdentity:
    open_paren = stat_text.find("(")
    close_paren = stat_text.rfind(")")
    if open_paren <= 0 or close_paren <= open_paren:
        raise GateError(f"malformed /proc stat identity for pid={pid}")
    try:
        declared_pid = int(stat_text[:open_paren].strip())
        fields = stat_text[close_paren + 1 :].split()
        state = fields[0]
        ppid = int(fields[1])
        starttime = int(fields[19])
    except (IndexError, ValueError) as error:
        raise GateError(f"malformed /proc stat identity for pid={pid}") from error
    if declared_pid != pid or len(state) != 1 or ppid < 0 or starttime < 0:
        raise GateError(f"invalid /proc stat identity for pid={pid}")
    return _ProcessIdentity(
        pid=pid,
        ppid=ppid,
        state=state,
        starttime=starttime,
    )


def _read_process_identity(pid: int) -> _ProcessIdentity | None:
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text(
            encoding="ascii", errors="replace"
        )
    except (FileNotFoundError, ProcessLookupError):
        return None
    except PermissionError as error:
        raise GateError(f"cannot inspect process identity pid={pid}: {error}") from error
    return _parse_process_identity(pid, stat_text)


def _process_identity_snapshot() -> dict[int, _ProcessIdentity]:
    result: dict[int, _ProcessIdentity] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            identity = _read_process_identity(pid)
        except GateError:
            # Only exact identities that were successfully captured may enter
            # the owned tree and become signal candidates.
            continue
        if identity is not None:
            result[pid] = identity
    return result


def _capture_owned_process_tree(
    root_pid: int,
    expected_root: _ProcessIdentity | None = None,
) -> tuple[_OwnedProcessIdentity, ...]:
    processes = _process_identity_snapshot()
    root = processes.get(root_pid)
    if root is None:
        root = _read_process_identity(root_pid)
        if root is None:
            return ()
        processes[root_pid] = root
    if expected_root is not None:
        if expected_root.pid != root_pid:
            raise GateError("owned process root identity has the wrong PID")
        if (
            root.ppid != expected_root.ppid
            or root.starttime != expected_root.starttime
        ):
            raise GateError(
                "owned process root PID identity changed; refusing to signal "
                f"pid={root_pid} captured_ppid={expected_root.ppid} "
                f"observed_ppid={root.ppid} "
                f"captured_starttime={expected_root.starttime} "
                f"observed_starttime={root.starttime}"
            )

    depths = {root_pid: 0}
    changed = True
    while changed:
        changed = False
        for pid, identity in processes.items():
            if identity.ppid in depths and pid not in depths:
                depths[pid] = depths[identity.ppid] + 1
                changed = True

    return tuple(
        sorted(
            (
                _OwnedProcessIdentity(
                    pid=pid,
                    ppid=processes[pid].ppid,
                    state=processes[pid].state,
                    starttime=processes[pid].starttime,
                    depth=depth,
                )
                for pid, depth in depths.items()
                if pid != os.getpid()
            ),
            key=lambda identity: (identity.depth, identity.pid),
            reverse=True,
        )
    )


def _matching_live_process(
    expected: _OwnedProcessIdentity,
    failures: set[str],
) -> bool:
    try:
        observed = _read_process_identity(expected.pid)
    except GateError as error:
        failures.add(str(error))
        return False
    if observed is None:
        return False
    # Descendants may be reparented after a deeper process is signalled.  The
    # captured PPID proves ancestry at snapshot time; starttime is the stable
    # token for later TERM/KILL and bounded-wait checks.
    if observed.starttime != expected.starttime:
        failures.add(
            "PID identity changed; refusing to signal "
            f"pid={expected.pid} depth={expected.depth} "
            f"captured_ppid={expected.ppid} observed_ppid={observed.ppid} "
            f"captured_starttime={expected.starttime} "
            f"observed_starttime={observed.starttime}"
        )
        return False
    return observed.state not in _DEAD_PROCESS_STATES


def _signal_owned_processes(
    ordered: tuple[_OwnedProcessIdentity, ...],
    sig: signal.Signals,
    failures: set[str],
) -> None:
    for identity in ordered:
        if not _matching_live_process(identity, failures):
            continue
        try:
            os.kill(identity.pid, sig)
        except ProcessLookupError:
            continue
        except PermissionError as error:
            failures.add(
                f"cannot signal owned pid={identity.pid} depth={identity.depth}: {error}"
            )


def _terminate_process_tree(
    root_pid: int,
    expected_root: _ProcessIdentity | None = None,
) -> None:
    ordered = _capture_owned_process_tree(root_pid, expected_root)
    failures: set[str] = set()
    for identity in ordered:
        _matching_live_process(identity, failures)
    if failures:
        raise GateError(
            "owned process tree termination failed safely before signaling; "
            + "; ".join(sorted(failures))
        )
    for sig in (signal.SIGTERM, signal.SIGKILL):
        _signal_owned_processes(ordered, sig, failures)
        deadline = time.monotonic() + (1.0 if sig == signal.SIGTERM else 0.5)
        while time.monotonic() < deadline:
            live = [
                identity
                for identity in ordered
                if _matching_live_process(identity, failures)
            ]
            if not live:
                break
            time.sleep(0.02)
    survivors = [
        identity.pid
        for identity in ordered
        if _matching_live_process(identity, failures)
    ]
    if survivors:
        failures.add(f"owned processes survived bounded TERM/KILL waits: {survivors}")
    if failures:
        raise GateError(
            "owned process tree termination failed safely; "
            + "; ".join(sorted(failures))
        )


def _require_live_process_identity(pid: int, description: str) -> _ProcessIdentity:
    identity = _read_process_identity(pid)
    if identity is None or identity.state in _DEAD_PROCESS_STATES:
        raise GateError(f"{description} pid={pid} is not live")
    return identity


def _process_identity_is_running(
    expected: _ProcessIdentity,
    description: str,
) -> bool:
    observed = _read_process_identity(expected.pid)
    if observed is None:
        return False
    if (
        observed.ppid != expected.ppid
        or observed.starttime != expected.starttime
    ):
        raise GateError(
            f"{description} PID identity changed; refusing to follow reused "
            f"pid={expected.pid} captured_ppid={expected.ppid} "
            f"observed_ppid={observed.ppid} "
            f"captured_starttime={expected.starttime} "
            f"observed_starttime={observed.starttime}"
        )
    return observed.state not in _DEAD_PROCESS_STATES


def _identity_from_control(
    control: dict[str, Any], role: str
) -> _ProcessIdentity:
    return _ProcessIdentity(
        pid=_positive(control[f"{role}_pid"], f"replay control {role} PID"),
        ppid=_positive(control[f"{role}_ppid"], f"replay control {role} PPID"),
        state="?",
        starttime=_positive(
            control[f"{role}_starttime_ticks"],
            f"replay control {role} starttime",
        ),
    )


def _validate_replay_monitor_control(
    path: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
    interval_ms: int,
    *,
    expected_root_pid: int | None = None,
    expected_rss_pid: int | None = None,
    expected_capacity_pid: int | None = None,
    require_live: bool = False,
) -> dict[str, Any]:
    control_path = _canonical_file(path, "replay monitor control")
    if stat.S_IMODE(control_path.stat().st_mode) != 0o444:
        raise GateError("replay monitor control must have exact mode 0444")
    value = _load_json(control_path)
    expected_keys = {
        "schema",
        "root_pid",
        "root_ppid",
        "root_starttime_ticks",
        "rss_monitor_pid",
        "rss_monitor_ppid",
        "rss_monitor_starttime_ticks",
        "capacity_monitor_pid",
        "capacity_monitor_ppid",
        "capacity_monitor_starttime_ticks",
        "interval_ms",
        "rss_ready_marker",
        "capacity_ready_marker",
        "launch_marker",
    }
    if not isinstance(value, dict) or set(value) != expected_keys:
        raise GateError("replay monitor control has an invalid or partial shape")
    roles = ("root", "rss_monitor", "capacity_monitor")
    identities = {role: _identity_from_control(value, role) for role in roles}
    if len({identity.pid for identity in identities.values()}) != len(roles):
        raise GateError("replay monitor control PIDs must be distinct")
    marker_paths = (rss_ready, capacity_ready, launch)
    if (
        value["schema"] != REPLAY_MONITOR_CONTROL_SCHEMA
        or value["interval_ms"] != interval_ms
        or interval_ms != FIXED_CAPACITY_MONITOR_INTERVAL_MS
        or value["rss_ready_marker"] != str(rss_ready)
        or value["capacity_ready_marker"] != str(capacity_ready)
        or value["launch_marker"] != str(launch)
        or any(not marker.is_absolute() for marker in marker_paths)
        or any(marker.parent != control_path.parent for marker in marker_paths)
        or len({control_path, *marker_paths}) != 4
        or expected_root_pid is not None
        and identities["root"].pid != expected_root_pid
        or expected_rss_pid is not None
        and identities["rss_monitor"].pid != expected_rss_pid
        or expected_capacity_pid is not None
        and identities["capacity_monitor"].pid != expected_capacity_pid
    ):
        raise GateError("replay monitor control differs from its exact handshake")
    if require_live:
        dead: list[str] = []
        for role, identity in identities.items():
            try:
                running = _process_identity_is_running(
                    identity, f"replay control {role}"
                )
            except GateError as error:
                raise GateError(f"replay monitor control identity failure: {error}") from error
            if not running:
                dead.append(role)
        if dead:
            raise GateError(
                "replay monitor control contains exited or zombie roles: "
                f"{dead}"
            )
    return value


def create_replay_monitor_control(
    output: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
    root_pid: int,
    root_ppid: int,
    root_starttime_ticks: int,
    rss_pid: int,
    rss_ppid: int,
    rss_starttime_ticks: int,
    capacity_pid: int,
    capacity_ppid: int,
    capacity_starttime_ticks: int,
    interval_ms: int,
) -> dict[str, Any]:
    if interval_ms != FIXED_CAPACITY_MONITOR_INTERVAL_MS:
        raise GateError("replay lifecycle interval must be exactly 100 ms")
    for candidate, description in (
        (output, "replay monitor control"),
        (rss_ready, "RSS ready marker"),
        (capacity_ready, "capacity ready marker"),
        (launch, "replay launch marker"),
    ):
        if os.path.lexists(candidate):
            raise GateError(f"refusing to reuse {description}")
    identities = {
        "root": _ProcessIdentity(
            _positive(root_pid, "held replay root PID"),
            _positive(root_ppid, "held replay root PPID"),
            "?",
            _positive(root_starttime_ticks, "held replay root starttime"),
        ),
        "rss_monitor": _ProcessIdentity(
            _positive(rss_pid, "RSS monitor PID"),
            _positive(rss_ppid, "RSS monitor PPID"),
            "?",
            _positive(rss_starttime_ticks, "RSS monitor starttime"),
        ),
        "capacity_monitor": _ProcessIdentity(
            _positive(capacity_pid, "capacity monitor PID"),
            _positive(capacity_ppid, "capacity monitor PPID"),
            "?",
            _positive(capacity_starttime_ticks, "capacity monitor starttime"),
        ),
    }
    if len({identity.pid for identity in identities.values()}) != 3:
        raise GateError("replay lifecycle roles must have distinct PIDs")
    for role, identity in identities.items():
        if not _process_identity_is_running(identity, f"replay control {role}"):
            raise GateError(f"replay control {role} exited before publication")
    value: dict[str, Any] = {
        "schema": REPLAY_MONITOR_CONTROL_SCHEMA,
        "interval_ms": interval_ms,
        "rss_ready_marker": str(rss_ready),
        "capacity_ready_marker": str(capacity_ready),
        "launch_marker": str(launch),
    }
    for role, identity in identities.items():
        value[f"{role}_pid"] = identity.pid
        value[f"{role}_ppid"] = identity.ppid
        value[f"{role}_starttime_ticks"] = identity.starttime
    _publish_read_only_json_atomic_exclusive(output, value)
    current = _validate_replay_monitor_control(
        output,
        rss_ready,
        capacity_ready,
        launch,
        interval_ms,
        expected_root_pid=root_pid,
        expected_rss_pid=rss_pid,
        expected_capacity_pid=capacity_pid,
        require_live=True,
    )
    if current != value:
        raise GateError("fresh replay monitor control failed self-validation")
    return value


def wait_replay_monitors_ready(
    control_path: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
    interval_ms: int,
    timeout_ms: int,
) -> dict[str, Any]:
    if timeout_ms < interval_ms:
        raise GateError("replay monitor readiness timeout is too short")
    deadline = time.monotonic() + timeout_ms / 1000
    while True:
        control = _validate_replay_monitor_control(
            control_path,
            rss_ready,
            capacity_ready,
            launch,
            interval_ms,
            require_live=True,
        )
        if os.path.lexists(launch):
            raise GateError("replay launch marker appeared before both monitors were ready")
        ready = []
        for marker, description in (
            (rss_ready, "RSS ready marker"),
            (capacity_ready, "capacity ready marker"),
        ):
            if os.path.lexists(marker):
                _validate_empty_read_only_marker(marker, description)
                ready.append(description)
        if len(ready) == 2:
            return {
                "status": "ready",
                "root_pid": control["root_pid"],
                "root_starttime_ticks": control["root_starttime_ticks"],
            }
        if time.monotonic() >= deadline:
            raise GateError("both replay monitors did not become ready within five seconds")
        time.sleep(0.01)


def release_replay_launch(
    control_path: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
    interval_ms: int,
) -> dict[str, Any]:
    control = _validate_replay_monitor_control(
        control_path,
        rss_ready,
        capacity_ready,
        launch,
        interval_ms,
        require_live=True,
    )
    _validate_empty_read_only_marker(rss_ready, "RSS ready marker")
    _validate_empty_read_only_marker(capacity_ready, "capacity ready marker")
    if os.path.lexists(launch):
        raise GateError("refusing to reuse replay launch marker")
    _create_empty_read_only_marker(launch, "replay launch marker")
    return {"status": "released", "root_pid": control["root_pid"]}


def cleanup_replay_processes(
    control_path: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
    interval_ms: int,
) -> dict[str, Any]:
    control = _validate_replay_monitor_control(
        control_path,
        rss_ready,
        capacity_ready,
        launch,
        interval_ms,
        require_live=False,
    )
    roles = ("root", "rss_monitor", "capacity_monitor")
    failures: dict[str, str] = {}
    for role in roles:
        identity = _identity_from_control(control, role)
        try:
            _terminate_process_tree(identity.pid, identity)
        except GateError as error:
            failures[role] = str(error)
    if failures:
        raise GateError(f"replay lifecycle cleanup was incomplete: {failures}")
    return {
        "status": "pass",
        "control_sha256": _sha256(control_path),
        "termination_order": list(roles),
    }


def _edge_inclusive_maximum_gap_ns(
    timestamps: list[int], elapsed_ns: int, description: str
) -> int:
    if elapsed_ns < 0 or any(timestamp < 0 for timestamp in timestamps):
        raise GateError(f"{description} cadence contains a negative timestamp")
    if any(right <= left for left, right in zip(timestamps, timestamps[1:])):
        raise GateError(f"{description} timestamps are not strictly increasing")
    if timestamps and timestamps[-1] > elapsed_ns:
        raise GateError(f"{description} terminal boundary precedes its final sample")
    boundaries = [0, *timestamps, elapsed_ns]
    return max(
        (right - left for left, right in zip(boundaries, boundaries[1:])),
        default=elapsed_ns,
    )


def _monitor_handshake_evidence(
    control_path: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
    interval_ms: int,
    role: str,
    ready_sample: int,
    ready_elapsed_ns: int,
    launch_sample: int,
    launch_elapsed_ns: int,
) -> dict[str, Any]:
    control = _validate_replay_monitor_control(
        control_path,
        rss_ready,
        capacity_ready,
        launch,
        interval_ms,
        require_live=False,
    )
    marker = rss_ready if role == "rss_monitor" else capacity_ready
    marker_description = "RSS ready marker" if role == "rss_monitor" else "capacity ready marker"
    _validate_empty_read_only_marker(marker, marker_description)
    _validate_empty_read_only_marker(launch, "replay launch marker")
    if ready_sample != 1 or launch_sample <= ready_sample:
        raise GateError(f"{role} launch handshake is not causally ordered")
    return {
        "root_pid": control["root_pid"],
        "root_ppid": control["root_ppid"],
        "root_starttime_ticks": control["root_starttime_ticks"],
        "control_path": str(control_path),
        "control_sha256": _sha256(control_path),
        "ready_marker_path": str(marker),
        "ready_marker_sha256": _sha256(marker),
        "ready_created_sample": ready_sample,
        "ready_created_elapsed_ns": ready_elapsed_ns,
        "launch_marker_path": str(launch),
        "launch_marker_sha256": _sha256(launch),
        "launch_observed_sample": launch_sample,
        "launch_observed_elapsed_ns": launch_elapsed_ns,
        "launch_observed": True,
    }


def _status_kib_for_identity(identity: _OwnedProcessIdentity) -> dict[str, int] | None:
    before = _read_process_identity(identity.pid)
    if before is None:
        return None
    if before.starttime != identity.starttime:
        raise GateError(f"RSS descendant PID was reused: pid={identity.pid}")
    if before.state in _DEAD_PROCESS_STATES:
        return None
    try:
        lines = Path(f"/proc/{identity.pid}/status").read_text(
            encoding="ascii", errors="strict"
        ).splitlines()
    except (FileNotFoundError, ProcessLookupError):
        return None
    wanted = {"VmRSS", "VmHWM", "RssAnon", "RssFile", "VmSwap"}
    result = {name: 0 for name in wanted}
    for line in lines:
        fields = line.split()
        key = fields[0].rstrip(":") if fields else ""
        if key in wanted and len(fields) >= 2:
            result[key] = _nonnegative(int(fields[1]), f"RSS {key}")
    after = _read_process_identity(identity.pid)
    if after is None:
        return None
    if after.starttime != identity.starttime:
        raise GateError(f"RSS descendant PID changed during sampling: pid={identity.pid}")
    if after.state in _DEAD_PROCESS_STATES:
        return None
    return result


def monitor_rss(
    root_pid: int,
    output: Path,
    summary: Path,
    interval_ms: int,
    control_path: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
) -> None:
    if interval_ms != FIXED_CAPACITY_MONITOR_INTERVAL_MS:
        raise GateError("RSS monitor interval must be exactly 100 ms")
    if os.path.lexists(output) or os.path.lexists(summary):
        raise GateError("refusing to reuse RSS monitor output")
    root_identity = _require_live_process_identity(root_pid, "RSS monitor root")
    control_deadline = time.monotonic() + 5.0
    while not os.path.lexists(control_path):
        if not _process_identity_is_running(root_identity, "held RSS root"):
            raise GateError("held RSS root exited before monitor control")
        if time.monotonic() >= control_deadline:
            raise GateError("RSS monitor control was not published within five seconds")
        time.sleep(0.005)
    control = _validate_replay_monitor_control(
        control_path,
        rss_ready,
        capacity_ready,
        launch,
        interval_ms,
        expected_root_pid=root_pid,
        expected_rss_pid=os.getpid(),
        require_live=True,
    )
    if control["root_starttime_ticks"] != root_identity.starttime:
        raise GateError("RSS monitor control rebound the held replay root")
    started = time.monotonic_ns()
    next_sample_ns = started
    timestamps: list[int] = []
    launch_seen = False
    try:
        with output.open("x", encoding="utf-8") as destination:
            destination.write(
                "event\telapsed_ns\trecorded_at_ns\troot_pid\t"
                "root_starttime_ticks\tprocess_count\trss_kib\trss_anon_kib\t"
                "rss_file_kib\tvm_swap_kib\tmax_single_hwm_kib\tpids\t"
                "launch_observed\n"
            )
            destination.flush()
            while True:
                current = _validate_replay_monitor_control(
                    control_path,
                    rss_ready,
                    capacity_ready,
                    launch,
                    interval_ms,
                )
                if current != control:
                    raise GateError("RSS monitor control bytes changed")
                root_running = _process_identity_is_running(
                    root_identity, "RSS monitor root"
                )
                elapsed_ns = time.monotonic_ns() - started
                if os.path.lexists(launch):
                    _validate_empty_read_only_marker(launch, "replay launch marker")
                    launch_seen = True
                elif launch_seen:
                    raise GateError("replay launch marker disappeared")
                if not timestamps and launch_seen:
                    raise GateError("replay launched before the first RSS sample")
                if os.path.lexists(rss_ready):
                    _validate_empty_read_only_marker(rss_ready, "RSS ready marker")
                elif timestamps:
                    raise GateError("RSS ready marker disappeared")
                if not root_running:
                    break
                root_stopped_during_peer_check = False
                for role in ("rss_monitor", "capacity_monitor"):
                    identity = _identity_from_control(control, role)
                    if not _process_identity_is_running(
                        identity, f"RSS monitor peer {role}"
                    ):
                        if not _process_identity_is_running(
                            root_identity,
                            "RSS monitor root at peer-failure boundary",
                        ):
                            root_stopped_during_peer_check = True
                            break
                        raise GateError(f"RSS monitor peer {role} exited early")
                if root_stopped_during_peer_check:
                    break
                tree = _capture_owned_process_tree(root_pid, root_identity)
                statuses: list[tuple[int, dict[str, int]]] = []
                for identity in sorted(tree, key=lambda item: item.pid):
                    status_values = _status_kib_for_identity(identity)
                    if status_values is not None:
                        statuses.append((identity.pid, status_values))
                if not statuses or root_pid not in {pid for pid, _status in statuses}:
                    if not _process_identity_is_running(
                        root_identity,
                        "RSS monitor root at missing-sample boundary",
                    ):
                        break
                    raise GateError("RSS monitor could not sample its bound live root")
                if not _process_identity_is_running(
                    root_identity, "RSS monitor root at sample boundary"
                ):
                    break
                metrics = {
                    "process_count": len(statuses),
                    "rss_kib": sum(value["VmRSS"] for _pid, value in statuses),
                    "rss_anon_kib": sum(
                        value["RssAnon"] for _pid, value in statuses
                    ),
                    "rss_file_kib": sum(
                        value["RssFile"] for _pid, value in statuses
                    ),
                    "vm_swap_kib": sum(
                        value["VmSwap"] for _pid, value in statuses
                    ),
                    "max_single_hwm_kib": max(
                        value["VmHWM"] for _pid, value in statuses
                    ),
                    "pids": ",".join(str(pid) for pid, _value in statuses),
                }
                timestamps.append(elapsed_ns)
                destination.write(
                    f"sample\t{elapsed_ns}\t{time.time_ns()}\t{root_identity.pid}\t"
                    f"{root_identity.starttime}\t{metrics['process_count']}\t"
                    f"{metrics['rss_kib']}\t{metrics['rss_anon_kib']}\t"
                    f"{metrics['rss_file_kib']}\t{metrics['vm_swap_kib']}\t"
                    f"{metrics['max_single_hwm_kib']}\t{metrics['pids']}\t"
                    f"{'true' if launch_seen else 'false'}\n"
                )
                destination.flush()
                if _edge_inclusive_maximum_gap_ns(
                    timestamps, elapsed_ns, "RSS monitor"
                ) > interval_ms * CAPACITY_MONITOR_MAX_GAP_MULTIPLIER * 1_000_000:
                    raise GateError("RSS monitor cadence gap exceeded 200 ms")
                if len(timestamps) == 1:
                    _create_empty_read_only_marker(rss_ready, "RSS ready marker")
                next_sample_ns += interval_ms * 1_000_000
                remaining_ns = next_sample_ns - time.monotonic_ns()
                if remaining_ns > 0:
                    time.sleep(remaining_ns / 1_000_000_000)
                else:
                    next_sample_ns = time.monotonic_ns()
            terminal_ns = time.monotonic_ns() - started
            destination.write(
                f"terminal\t{terminal_ns}\t{time.time_ns()}\t{root_identity.pid}\t"
                f"{root_identity.starttime}\t0\t0\t0\t0\t0\t0\t-\t"
                f"{'true' if launch_seen else 'false'}\n"
            )
            destination.flush()
        _write_json(
            summary,
            _rss_summary_from_samples(
                output,
                interval_ms,
                control_path,
                rss_ready,
                capacity_ready,
                launch,
            ),
        )
    except (GateError, OSError, ValueError) as error:
        try:
            _terminate_process_tree(root_pid, root_identity)
        except GateError as cleanup_error:
            raise GateError(
                f"RSS monitor failed: {error}; safe cleanup refused: {cleanup_error}"
            ) from error
        raise


def _capacity_summary_from_samples(
    samples_path: Path,
    filesystem: Path,
    minimum_free_bytes: int,
    interval_ms: int,
    control_path: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
) -> dict[str, Any]:
    rows = _read_tsv(
        samples_path,
        [
            "event",
            "elapsed_ns",
            "root_pid",
            "root_starttime_ticks",
            "free_bytes",
            "launch_observed",
        ],
    )
    sample_rows = [row for row in rows if row["event"] == "sample"]
    terminal_rows = [row for row in rows if row["event"] == "terminal"]
    if len(sample_rows) < 2:
        raise GateError("capacity monitor produced fewer than two cadence samples")
    if len(terminal_rows) != 1 or rows[-1]["event"] != "terminal":
        raise GateError("capacity monitor lacks one final terminal boundary")
    if any(row["event"] not in {"sample", "terminal"} for row in rows):
        raise GateError("capacity monitor contains an unknown event")
    control = _validate_replay_monitor_control(
        control_path, rss_ready, capacity_ready, launch, interval_ms
    )
    free_values: list[int] = []
    timestamps: list[int] = []
    launch_observations: list[bool] = []
    for row in sample_rows:
        recorded = _nonnegative(int(row["elapsed_ns"]), "capacity sample timestamp")
        free = _nonnegative(int(row["free_bytes"]), "capacity sample free bytes")
        if (
            int(row["root_pid"]) != control["root_pid"]
            or int(row["root_starttime_ticks"])
            != control["root_starttime_ticks"]
        ):
            raise GateError("capacity sample is not bound to the controlled root")
        _require_capacity(free, minimum_free_bytes, "capacity monitor")
        if row["launch_observed"] not in {"true", "false"}:
            raise GateError("capacity sample launch observation is malformed")
        timestamps.append(recorded)
        free_values.append(free)
        launch_observations.append(row["launch_observed"] == "true")
    terminal = terminal_rows[0]
    elapsed_ns = _nonnegative(
        int(terminal["elapsed_ns"]), "capacity terminal timestamp"
    )
    terminal_free = _nonnegative(
        int(terminal["free_bytes"]), "capacity terminal free bytes"
    )
    if (
        int(terminal["root_pid"]) != control["root_pid"]
        or int(terminal["root_starttime_ticks"])
        != control["root_starttime_ticks"]
    ):
        raise GateError("capacity terminal boundary is not bound to the controlled root")
    _require_capacity(terminal_free, minimum_free_bytes, "capacity monitor terminal")
    if terminal["launch_observed"] != "true":
        raise GateError("capacity terminal boundary did not retain launch observation")
    try:
        launch_index = launch_observations.index(True) + 1
    except ValueError as error:
        raise GateError("capacity monitor never observed the replay launch") from error
    if any(launch_observations[: launch_index - 1]) or not all(
        launch_observations[launch_index - 1 :]
    ):
        raise GateError("capacity launch observation is not monotonic")
    maximum_gap_ns = _edge_inclusive_maximum_gap_ns(
        timestamps, elapsed_ns, "capacity monitor"
    )
    allowed_gap_ns = (
        interval_ms * CAPACITY_MONITOR_MAX_GAP_MULTIPLIER * 1_000_000
    )
    if maximum_gap_ns > allowed_gap_ns:
        raise GateError("capacity monitor cadence gap exceeds 200 ms")
    return {
        "schema": CAPACITY_MONITOR_SCHEMA,
        **_monitor_handshake_evidence(
            control_path,
            rss_ready,
            capacity_ready,
            launch,
            interval_ms,
            "capacity_monitor",
            1,
            timestamps[0],
            launch_index,
            timestamps[launch_index - 1],
        ),
        "filesystem": str(filesystem),
        "interval_ms": interval_ms,
        "minimum_free_bytes": minimum_free_bytes,
        "maximum_allowed_gap_ns": allowed_gap_ns,
        "samples": len(free_values),
        "elapsed_ns": elapsed_ns,
        "first_elapsed_ns": timestamps[0],
        "last_sample_elapsed_ns": timestamps[-1],
        "maximum_gap_ns": maximum_gap_ns,
        "minimum_observed_free_bytes": min([*free_values, terminal_free]),
        "status": "pass",
    }


def monitor_capacity(
    root_pid: int,
    filesystem: Path,
    minimum_free_bytes: int,
    interval_ms: int,
    output: Path,
    summary: Path,
    control_path: Path,
    rss_ready: Path,
    capacity_ready: Path,
    launch: Path,
) -> None:
    if interval_ms != FIXED_CAPACITY_MONITOR_INTERVAL_MS:
        raise GateError(
            f"capacity monitor interval must be exactly {FIXED_CAPACITY_MONITOR_INTERVAL_MS} ms"
        )
    _positive(root_pid, "capacity monitor root PID")
    minimum_free_bytes = _positive(minimum_free_bytes, "capacity monitor floor")
    filesystem = _canonical_directory(filesystem, "capacity monitor filesystem")
    if os.path.lexists(output) or os.path.lexists(summary):
        raise GateError("refusing to reuse capacity monitor output")
    root_identity = _require_live_process_identity(
        root_pid, "capacity monitor root"
    )
    control_deadline = time.monotonic() + 5.0
    while not os.path.lexists(control_path):
        if not _process_identity_is_running(root_identity, "held capacity root"):
            raise GateError("held capacity root exited before monitor control")
        if time.monotonic() >= control_deadline:
            raise GateError("capacity monitor control was not published within five seconds")
        time.sleep(0.005)
    control = _validate_replay_monitor_control(
        control_path,
        rss_ready,
        capacity_ready,
        launch,
        interval_ms,
        expected_root_pid=root_pid,
        expected_capacity_pid=os.getpid(),
        require_live=True,
    )
    if control["root_starttime_ticks"] != root_identity.starttime:
        raise GateError("capacity monitor control rebound the held replay root")
    started = time.monotonic_ns()
    next_sample_ns = started
    timestamps: list[int] = []
    launch_seen = False
    try:
        with output.open("x", encoding="utf-8") as destination:
            destination.write(
                "event\telapsed_ns\troot_pid\troot_starttime_ticks\tfree_bytes\t"
                "launch_observed\n"
            )
            destination.flush()
            while True:
                current = _validate_replay_monitor_control(
                    control_path,
                    rss_ready,
                    capacity_ready,
                    launch,
                    interval_ms,
                )
                if current != control:
                    raise GateError("capacity monitor control bytes changed")
                root_running = _process_identity_is_running(
                    root_identity, "capacity monitor root"
                )
                elapsed_ns = time.monotonic_ns() - started
                if os.path.lexists(launch):
                    _validate_empty_read_only_marker(launch, "replay launch marker")
                    launch_seen = True
                elif launch_seen:
                    raise GateError("replay launch marker disappeared")
                if not timestamps and launch_seen:
                    raise GateError("replay launched before the first capacity sample")
                if os.path.lexists(capacity_ready):
                    _validate_empty_read_only_marker(
                        capacity_ready, "capacity ready marker"
                    )
                elif timestamps:
                    raise GateError("capacity ready marker disappeared")
                if not root_running:
                    break
                root_stopped_during_peer_check = False
                for role in ("rss_monitor", "capacity_monitor"):
                    identity = _identity_from_control(control, role)
                    if not _process_identity_is_running(
                        identity, f"capacity monitor peer {role}"
                    ):
                        if not _process_identity_is_running(
                            root_identity,
                            "capacity monitor root at peer-failure boundary",
                        ):
                            root_stopped_during_peer_check = True
                            break
                        raise GateError(f"capacity monitor peer {role} exited early")
                if root_stopped_during_peer_check:
                    break
                _filesystem, free_bytes, _total_bytes = _capacity_free_bytes(filesystem)
                if not _process_identity_is_running(
                    root_identity, "capacity monitor root at sample boundary"
                ):
                    break
                timestamps.append(elapsed_ns)
                destination.write(
                    f"sample\t{elapsed_ns}\t{root_identity.pid}\t"
                    f"{root_identity.starttime}\t{free_bytes}\t"
                    f"{'true' if launch_seen else 'false'}\n"
                )
                destination.flush()
                maximum_gap_ns = _edge_inclusive_maximum_gap_ns(
                    timestamps, elapsed_ns, "capacity monitor"
                )
                if maximum_gap_ns > (
                    interval_ms * CAPACITY_MONITOR_MAX_GAP_MULTIPLIER * 1_000_000
                ):
                    raise GateError("capacity monitor cadence gap exceeded 200 ms")
                _require_capacity(free_bytes, minimum_free_bytes, "capacity monitor")
                if len(timestamps) == 1:
                    _create_empty_read_only_marker(
                        capacity_ready, "capacity ready marker"
                    )
                next_sample_ns += interval_ms * 1_000_000
                remaining_ns = next_sample_ns - time.monotonic_ns()
                if remaining_ns > 0:
                    time.sleep(remaining_ns / 1_000_000_000)
                else:
                    next_sample_ns = time.monotonic_ns()
            terminal_ns = time.monotonic_ns() - started
            _filesystem, free_bytes, _total_bytes = _capacity_free_bytes(filesystem)
            destination.write(
                f"terminal\t{terminal_ns}\t{root_identity.pid}\t"
                f"{root_identity.starttime}\t{free_bytes}\t"
                f"{'true' if launch_seen else 'false'}\n"
            )
            destination.flush()
            _require_capacity(
                free_bytes, minimum_free_bytes, "capacity monitor terminal"
            )
        _write_json(
            summary,
            _capacity_summary_from_samples(
                output,
                filesystem,
                minimum_free_bytes,
                interval_ms,
                control_path,
                rss_ready,
                capacity_ready,
                launch,
            ),
        )
    except (GateError, OSError, ValueError) as error:
        try:
            _terminate_process_tree(root_pid, root_identity)
        except GateError as cleanup_error:
            raise GateError(
                f"capacity monitor failed: {error}; safe cleanup refused: {cleanup_error}"
            ) from error
        raise


def _guardian_summary_from_samples(
    samples_path: Path,
    parent_pid: int,
    filesystem: Path,
    minimum_free_bytes: int,
    interval_ms: int,
) -> dict[str, Any]:
    rows = _read_tsv(
        samples_path,
        [
            "event",
            "elapsed_ns",
            "parent_pid",
            "parent_ppid",
            "parent_starttime_ticks",
            "free_bytes",
            "process_count",
        ],
    )
    sample_rows = [row for row in rows if row["event"] == "sample"]
    terminal_rows = [row for row in rows if row["event"] == "terminal"]
    if len(sample_rows) < 2:
        raise GateError("conflict guardian produced fewer than two cadence samples")
    if len(terminal_rows) != 1 or rows[-1]["event"] != "terminal":
        raise GateError("conflict guardian lacks one final terminal boundary")
    if any(row["event"] not in {"sample", "terminal"} for row in rows):
        raise GateError("conflict guardian contains an unknown event")
    timestamps: list[int] = []
    free_values: list[int] = []
    parent_ppid: int | None = None
    parent_starttime: int | None = None
    for row in rows:
        recorded = _nonnegative(int(row["elapsed_ns"]), "guardian event timestamp")
        free = _nonnegative(int(row["free_bytes"]), "guardian sample free bytes")
        _nonnegative(int(row["process_count"]), "guardian sample process count")
        if int(row["parent_pid"]) != parent_pid:
            raise GateError("conflict guardian heartbeat parent PID changed")
        row_parent_ppid = _nonnegative(
            int(row["parent_ppid"]), "guardian parent PPID"
        )
        row_parent_starttime = _positive(
            int(row["parent_starttime_ticks"]), "guardian parent starttime"
        )
        if parent_ppid is None:
            parent_ppid = row_parent_ppid
            parent_starttime = row_parent_starttime
        elif (
            row_parent_ppid != parent_ppid
            or row_parent_starttime != parent_starttime
        ):
            raise GateError("conflict guardian heartbeat parent identity changed")
        _require_capacity(free, minimum_free_bytes, "conflict guardian")
        if row["event"] == "sample":
            timestamps.append(recorded)
        free_values.append(free)
    elapsed_ns = _nonnegative(
        int(terminal_rows[0]["elapsed_ns"]), "guardian terminal timestamp"
    )
    maximum_gap_ns = _edge_inclusive_maximum_gap_ns(
        timestamps, elapsed_ns, "conflict guardian"
    )
    allowed_gap_ns = interval_ms * GUARD_MAX_GAP_MULTIPLIER * 1_000_000
    if maximum_gap_ns > allowed_gap_ns:
        raise GateError(
            "conflict guardian cadence gap exceeds the fixed fail-closed allowance"
        )
    return {
        "schema": GUARD_SAMPLE_SCHEMA,
        "parent_pid": _positive(parent_pid, "guardian parent PID"),
        "parent_ppid": parent_ppid,
        "parent_starttime_ticks": parent_starttime,
        "filesystem": str(filesystem),
        "interval_ms": interval_ms,
        "maximum_allowed_gap_ns": allowed_gap_ns,
        "minimum_free_bytes": minimum_free_bytes,
        "samples": len(sample_rows),
        "elapsed_ns": elapsed_ns,
        "first_elapsed_ns": timestamps[0],
        "last_sample_elapsed_ns": timestamps[-1],
        "maximum_gap_ns": maximum_gap_ns,
        "minimum_observed_free_bytes": min(free_values),
        "status": "pass",
    }


def guard_conflicts(
    parent_pid: int,
    stop_file: Path,
    output: Path,
    interval_ms: int,
    filesystem: Path,
    minimum_free_bytes: int,
    samples: Path,
    summary: Path,
    ready_file: Path,
) -> None:
    if interval_ms != FIXED_GUARD_INTERVAL_MS:
        raise GateError(
            f"conflict guardian interval must be exactly {FIXED_GUARD_INTERVAL_MS} ms"
        )
    filesystem = _canonical_directory(filesystem, "conflict guardian filesystem")
    minimum_free_bytes = _positive(minimum_free_bytes, "conflict guardian floor")
    if any(
        os.path.lexists(path)
        for path in (output, samples, summary, ready_file, stop_file)
    ):
        raise GateError("refusing to reuse conflict guardian output")
    parent_identity = _require_live_process_identity(
        parent_pid, "conflict guardian parent"
    )
    started = time.monotonic_ns()
    with output.open("x", encoding="utf-8") as destination, samples.open(
        "x", encoding="utf-8"
    ) as sample_destination:
        destination.write("recorded_at_ns\tpid\tppid\tcomm\tcmdline\n")
        destination.flush()
        sample_destination.write(
            "event\telapsed_ns\tparent_pid\tparent_ppid\tparent_starttime_ticks\t"
            "free_bytes\tprocess_count\n"
        )
        sample_destination.flush()
        sample_count = 0
        sample_elapsed_ns: list[int] = []
        allowed_gap_ns = interval_ms * GUARD_MAX_GAP_MULTIPLIER * 1_000_000
        while True:
            if not _process_identity_is_running(
                parent_identity, "conflict guardian parent"
            ):
                raise GateError(
                    "conflict guardian parent disappeared before the guarded interval ended"
                )
            processes = _process_snapshot()
            if not _process_identity_is_running(
                parent_identity, "conflict guardian parent"
            ):
                raise GateError(
                    "conflict guardian parent disappeared during its process scan"
                )
            conflicts = _conflicts_in_snapshot(processes, parent_pid)
            if conflicts:
                now = time.time_ns()
                for pid, ppid, comm, cmdline in conflicts:
                    destination.write(f"{now}\t{pid}\t{ppid}\t{comm}\t{cmdline}\n")
                destination.flush()
                _terminate_process_tree(parent_pid, parent_identity)
                raise GateError("measurement conflict guardian detected an unrelated workload")
            _filesystem, free_bytes, _total_bytes = _capacity_free_bytes(filesystem)
            if not _process_identity_is_running(
                parent_identity, "conflict guardian parent"
            ):
                raise GateError(
                    "conflict guardian parent disappeared during its capacity scan"
                )
            if free_bytes < minimum_free_bytes:
                _terminate_process_tree(parent_pid, parent_identity)
                raise GateError(
                    "measurement conflict guardian observed the operational disk floor crossing"
                )
            now = time.monotonic_ns() - started
            sample_destination.write(
                f"sample\t{now}\t{parent_identity.pid}\t{parent_identity.ppid}\t"
                f"{parent_identity.starttime}\t{free_bytes}\t{len(processes)}\n"
            )
            sample_destination.flush()
            sample_elapsed_ns.append(now)
            if _edge_inclusive_maximum_gap_ns(
                sample_elapsed_ns,
                now,
                "conflict guardian",
            ) > allowed_gap_ns:
                _terminate_process_tree(parent_pid, parent_identity)
                raise GateError("conflict guardian cadence gap exceeded 200 ms")
            sample_count += 1
            if sample_count == 1:
                _create_empty_read_only_marker(
                    ready_file, "conflict guardian ready marker"
                )
            else:
                _validate_empty_read_only_marker(
                    ready_file, "conflict guardian ready marker"
                )
            if os.path.lexists(stop_file) and sample_count >= 2:
                _validate_empty_read_only_marker(
                    stop_file, "conflict guardian stop marker"
                )
                break
            time.sleep(interval_ms / 1000)
        if not _process_identity_is_running(
            parent_identity, "conflict guardian parent"
        ):
            raise GateError("conflict guardian parent disappeared at terminal boundary")
        processes = _process_snapshot()
        if not _process_identity_is_running(
            parent_identity, "conflict guardian parent"
        ):
            raise GateError("conflict guardian parent disappeared during terminal scan")
        conflicts = _conflicts_in_snapshot(processes, parent_pid)
        if conflicts:
            now = time.time_ns()
            for pid, ppid, comm, cmdline in conflicts:
                destination.write(f"{now}\t{pid}\t{ppid}\t{comm}\t{cmdline}\n")
            destination.flush()
            _terminate_process_tree(parent_pid, parent_identity)
            raise GateError(
                "measurement conflict guardian detected an unrelated workload at terminal boundary"
            )
        _filesystem, free_bytes, _total_bytes = _capacity_free_bytes(filesystem)
        if free_bytes < minimum_free_bytes:
            _terminate_process_tree(parent_pid, parent_identity)
            raise GateError(
                "measurement conflict guardian observed the operational disk floor crossing at terminal boundary"
            )
        if not _process_identity_is_running(
            parent_identity, "conflict guardian parent"
        ):
            raise GateError("conflict guardian parent disappeared during terminal capacity scan")
        terminal_ns = time.monotonic_ns() - started
        sample_destination.write(
            f"terminal\t{terminal_ns}\t{parent_identity.pid}\t"
            f"{parent_identity.ppid}\t{parent_identity.starttime}\t"
            f"{free_bytes}\t{len(processes)}\n"
        )
        sample_destination.flush()
    _write_json(
        summary,
        _guardian_summary_from_samples(
            samples, parent_pid, filesystem, minimum_free_bytes, interval_ms
        ),
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    seal_source = commands.add_parser("source-seal")
    seal_source.add_argument("--repo", type=Path, required=True)
    seal_source.add_argument("--output", type=Path, required=True)
    check_source = commands.add_parser("check-source-seal")
    check_source.add_argument("--repo", type=Path, required=True)
    check_source.add_argument("--seal", type=Path, required=True)
    seal_snapshot = commands.add_parser("source-snapshot-seal")
    seal_snapshot.add_argument("--repo", type=Path, required=True)
    seal_snapshot.add_argument("--snapshot", type=Path, required=True)
    seal_snapshot.add_argument("--source-seal", type=Path, required=True)
    seal_snapshot.add_argument("--output", type=Path, required=True)
    check_snapshot = commands.add_parser("check-source-snapshot-seal")
    check_snapshot.add_argument("--repo", type=Path, required=True)
    check_snapshot.add_argument("--snapshot", type=Path, required=True)
    check_snapshot.add_argument("--source-seal", type=Path, required=True)
    check_snapshot.add_argument("--seal", type=Path, required=True)
    cargo_isolation = commands.add_parser("check-cargo-config-isolation")
    cargo_isolation.add_argument("--snapshot", type=Path, required=True)
    cargo_isolation.add_argument("--cargo-home", type=Path, required=True)
    final_inventory = commands.add_parser("final-artifact-inventory")
    final_inventory.add_argument("--result-dir", type=Path, required=True)
    final_inventory.add_argument("--output", type=Path, required=True)
    final_verify = commands.add_parser("verify-final-artifact-seal")
    final_verify.add_argument("--result-dir", type=Path, required=True)
    admission_plan = commands.add_parser("admission-plan")
    admission_plan.add_argument("--output", type=Path, required=True)
    admission_plan.add_argument("--result-dir", type=Path, required=True)
    admission_plan.add_argument("--capture", type=Path, required=True)
    admission_plan.add_argument("--repo", type=Path, required=True)
    admission_plan.add_argument("--query-manifest", type=Path, required=True)
    admission_plan.add_argument("--config-template", type=Path, required=True)
    admission_plan.add_argument(
        "--validated-input-config-template", type=Path, required=True
    )
    admission_plan.add_argument("--expectations", type=Path, required=True)
    admission_plan.add_argument(
        "--binary-provenance-mode",
        choices=("internal", "external-exploratory"),
        required=True,
    )
    admission_plan.add_argument("--promotion-eligibility", required=True)
    admission_plan.add_argument("--stop-after-messages", type=int, required=True)
    admission_plan.add_argument("--replay-blocks", type=int, required=True)
    admission_plan.add_argument("--query-blocks", type=int, required=True)
    admission_plan.add_argument("--benchmark-repeats", type=int, required=True)
    admission_plan.add_argument("--rss-interval-ms", type=int, required=True)
    admission_plan.add_argument("--guard-interval-ms", type=int, required=True)
    admission_plan.add_argument(
        "--capacity-monitor-interval-ms", type=int, required=True
    )
    admission_plan.add_argument("--page-size-bytes", type=int, required=True)
    admission_plan.add_argument(
        "--max-capture-resident-bytes-after-evict", type=int, required=True
    )
    admission_plan.add_argument(
        "--max-corpus-resident-bytes-after-evict", type=int, required=True
    )
    admission_plan.add_argument(
        "--max-dirty-writeback-bytes", type=int, required=True
    )
    admission_plan.add_argument("--capacity-contract-sha256", required=True)
    admission_plan.add_argument(
        "--readback-sample-limit-per-kind", type=int, required=True
    )
    admission_plan.add_argument("--rust-log", required=True)
    admission_plan.add_argument(
        "--perf-stat-mode", choices=("off", "auto", "required"), required=True
    )
    admission_plan.add_argument("--perf-binary", required=True)
    admission_plan.add_argument("--perf-binary-sha256", required=True)
    admission_plan.add_argument("--perf-version", required=True)
    admission_plan.add_argument("--chunk-read-queue-depth", type=int, required=True)
    admission_plan.add_argument(
        "--query-label-arena-max-bytes", type=int, required=True
    )
    admission_plan.add_argument("--query-max-series-matched", type=int, required=True)
    admission_plan.add_argument("--query-max-projected-series", type=int, required=True)
    admission_plan.add_argument("--query-max-chunks-read", type=int, required=True)
    admission_plan.add_argument("--query-max-bytes-read", type=int, required=True)
    admission_plan.add_argument("--query-max-samples", type=int, required=True)
    admission_plan.add_argument("--regex-max-expanded-values", type=int, required=True)
    invocation = commands.add_parser("write-invocation")
    invocation.add_argument("--binary", type=Path, required=True)
    invocation.add_argument(
        "--role", choices=("ingester", "query", "verifier"), required=True
    )
    invocation.add_argument("--arg", action="append", default=[])
    invocation.add_argument("--env", action="append", default=[])
    invocation.add_argument("--output", type=Path, required=True)
    raw_seal = commands.add_parser("raw-leaf-seal")
    raw_seal.add_argument("--result-dir", type=Path, required=True)
    raw_seal.add_argument("--file", type=Path, action="append", default=[])
    raw_seal.add_argument("--tree", type=Path, action="append", default=[])
    raw_seal.add_argument("--output", type=Path, required=True)
    residency = commands.add_parser("validate-residency-evidence")
    residency.add_argument("--input", type=Path, required=True)
    residency.add_argument("--phase", required=True)
    residency.add_argument("--paths", type=Path, required=True)
    residency.add_argument("--ceiling-bytes", type=int)
    residency.add_argument("--page-size-bytes", type=int, required=True)
    writeback = commands.add_parser("validate-writeback-evidence")
    writeback.add_argument("--input", type=Path, required=True)
    writeback.add_argument("--phase", required=True)
    writeback.add_argument("--ceiling-bytes", type=int, required=True)
    raw_authorities = commands.add_parser("write-raw-authorities")
    raw_authorities.add_argument("--result-dir", type=Path, required=True)
    raw_authorities.add_argument("--entry", action="append", default=[])
    raw_authorities.add_argument("--output", type=Path, required=True)
    raw_authorities.add_argument("--checksum-output", type=Path, required=True)
    report = commands.add_parser("find-replay-report")
    report.add_argument("--run-dir", type=Path, required=True)
    admission = commands.add_parser("final-admission")
    admission.add_argument("--result-dir", type=Path, required=True)
    admission.add_argument("--plan", type=Path, required=True)
    admission.add_argument("--output", type=Path, required=True)
    commands.add_parser("check-ambient-env")
    runtime = commands.add_parser("runtime-identity")
    runtime.add_argument("--binary", type=Path, required=True)
    runtime.add_argument(
        "--role", choices=("ingester", "query", "verifier"), required=True
    )
    runtime.add_argument("--env", action="append", default=[])
    runtime.add_argument("--normalize-env", action="append", default=[])
    runtime.add_argument("--output", type=Path, required=True)
    render = commands.add_parser("render-config")
    render.add_argument("--template", type=Path, required=True)
    render.add_argument("--output", type=Path, required=True)
    render.add_argument("--capture", type=Path, required=True)
    render.add_argument("--segments-dir", type=Path, required=True)
    render.add_argument("--stop-after-messages", type=int, required=True)
    render.add_argument("--codec", choices=CODECS, required=True)
    capture = commands.add_parser("capture-inventory")
    capture.add_argument("--capture", type=Path, required=True)
    capture.add_argument("--output", type=Path, required=True)
    capture.add_argument("--paths-output", type=Path, required=True)
    tree = commands.add_parser("tree-manifest")
    tree.add_argument("--corpus", type=Path, required=True)
    tree.add_argument("--manifest", type=Path, required=True)
    tree.add_argument("--inventory", type=Path, required=True)
    tree.add_argument("--summary", type=Path, required=True)
    artifacts = commands.add_parser("artifact-inventory")
    artifacts.add_argument("--corpus", type=Path, required=True)
    artifacts.add_argument("--output", type=Path, required=True)
    replay_report = commands.add_parser("replay-report")
    replay_report.add_argument("--report", type=Path, required=True)
    replay_report.add_argument("--output", type=Path, required=True)
    seal = commands.add_parser("parse-seal-log")
    seal.add_argument("--log", type=Path, required=True)
    seal.add_argument("--output", type=Path, required=True)
    timing = commands.add_parser("parse-time")
    timing.add_argument("--input", type=Path, required=True)
    timing.add_argument("--output", type=Path, required=True)
    perf = commands.add_parser("parse-perf")
    perf.add_argument("--input", type=Path, required=True)
    perf.add_argument("--output", type=Path, required=True)
    rss = commands.add_parser("monitor-rss")
    rss.add_argument("--pid", type=int, required=True)
    rss.add_argument("--output", type=Path, required=True)
    rss.add_argument("--summary", type=Path, required=True)
    rss.add_argument("--interval-ms", type=int, default=100)
    rss.add_argument("--control", type=Path, required=True)
    rss.add_argument("--rss-ready", type=Path, required=True)
    rss.add_argument("--capacity-ready", type=Path, required=True)
    rss.add_argument("--launch", type=Path, required=True)
    capacity_contract = commands.add_parser("capacity-contract")
    capacity_contract.add_argument("--expectations", type=Path, required=True)
    capacity_contract.add_argument("--repo", type=Path, required=True)
    capacity_contract.add_argument("--source-head", required=True)
    capacity_contract.add_argument("--replay-blocks", type=int, required=True)
    capacity_contract.add_argument("--output", type=Path)
    capacity_snapshot_parser = commands.add_parser("capacity-snapshot")
    capacity_snapshot_parser.add_argument("--filesystem", type=Path, required=True)
    capacity_snapshot_parser.add_argument(
        "--minimum-free-bytes", type=int, required=True
    )
    capacity_snapshot_parser.add_argument("--phase", required=True)
    capacity_snapshot_parser.add_argument("--output", type=Path)
    capacity_monitor = commands.add_parser("monitor-capacity")
    capacity_monitor.add_argument("--pid", type=int, required=True)
    capacity_monitor.add_argument("--filesystem", type=Path, required=True)
    capacity_monitor.add_argument("--minimum-free-bytes", type=int, required=True)
    capacity_monitor.add_argument("--interval-ms", type=int, default=100)
    capacity_monitor.add_argument("--output", type=Path, required=True)
    capacity_monitor.add_argument("--summary", type=Path, required=True)
    capacity_monitor.add_argument("--control", type=Path, required=True)
    capacity_monitor.add_argument("--rss-ready", type=Path, required=True)
    capacity_monitor.add_argument("--capacity-ready", type=Path, required=True)
    capacity_monitor.add_argument("--launch", type=Path, required=True)
    replay_control = commands.add_parser("create-replay-monitor-control")
    replay_control.add_argument("--root-pid", type=int, required=True)
    replay_control.add_argument("--root-ppid", type=int, required=True)
    replay_control.add_argument("--root-starttime-ticks", type=int, required=True)
    replay_control.add_argument("--rss-pid", type=int, required=True)
    replay_control.add_argument("--rss-ppid", type=int, required=True)
    replay_control.add_argument("--rss-starttime-ticks", type=int, required=True)
    replay_control.add_argument("--capacity-pid", type=int, required=True)
    replay_control.add_argument("--capacity-ppid", type=int, required=True)
    replay_control.add_argument(
        "--capacity-starttime-ticks", type=int, required=True
    )
    replay_control.add_argument("--interval-ms", type=int, required=True)
    replay_control.add_argument("--rss-ready", type=Path, required=True)
    replay_control.add_argument("--capacity-ready", type=Path, required=True)
    replay_control.add_argument("--launch", type=Path, required=True)
    replay_control.add_argument("--output", type=Path, required=True)
    replay_wait = commands.add_parser("wait-replay-monitors-ready")
    replay_wait.add_argument("--control", type=Path, required=True)
    replay_wait.add_argument("--rss-ready", type=Path, required=True)
    replay_wait.add_argument("--capacity-ready", type=Path, required=True)
    replay_wait.add_argument("--launch", type=Path, required=True)
    replay_wait.add_argument("--interval-ms", type=int, required=True)
    replay_wait.add_argument("--timeout-ms", type=int, required=True)
    replay_release = commands.add_parser("release-replay-launch")
    replay_release.add_argument("--control", type=Path, required=True)
    replay_release.add_argument("--rss-ready", type=Path, required=True)
    replay_release.add_argument("--capacity-ready", type=Path, required=True)
    replay_release.add_argument("--launch", type=Path, required=True)
    replay_release.add_argument("--interval-ms", type=int, required=True)
    replay_cleanup = commands.add_parser("cleanup-replay-processes")
    replay_cleanup.add_argument("--control", type=Path, required=True)
    replay_cleanup.add_argument("--rss-ready", type=Path, required=True)
    replay_cleanup.add_argument("--capacity-ready", type=Path, required=True)
    replay_cleanup.add_argument("--launch", type=Path, required=True)
    replay_cleanup.add_argument("--interval-ms", type=int, required=True)
    terminate = commands.add_parser("terminate-process-tree")
    terminate.add_argument("--root-pid", type=int, required=True)
    terminate.add_argument("--root-ppid", type=int, required=True)
    terminate.add_argument("--root-starttime-ticks", type=int, required=True)
    marker = commands.add_parser("create-empty-marker")
    marker.add_argument("--output", type=Path, required=True)
    corpus_capacity = commands.add_parser("check-corpus-capacity")
    corpus_capacity.add_argument("--summary", type=Path, required=True)
    corpus_capacity.add_argument("--contract", type=Path, required=True)
    corpus_capacity.add_argument("--codec", choices=CODECS, required=True)
    corpus_capacity.add_argument("--output", type=Path)
    compare_replay = commands.add_parser("compare-replays")
    compare_replay.add_argument("--index", type=Path, required=True)
    compare_replay.add_argument("--blocks", type=int, required=True)
    compare_replay.add_argument("--output", type=Path, required=True)
    compare_replay.add_argument("--summary", type=Path, required=True)
    verifier = commands.add_parser("compare-verifiers")
    verifier.add_argument("--raw", type=Path, required=True)
    verifier.add_argument("--gorilla", type=Path, required=True)
    verifier.add_argument("--output", type=Path, required=True)
    readback = commands.add_parser("check-readback")
    readback.add_argument("--report", type=Path, required=True)
    readback.add_argument("--output", type=Path, required=True)
    manifest = commands.add_parser("normalize-query-manifest")
    manifest.add_argument("--input", type=Path, required=True)
    manifest.add_argument("--output-tsv", type=Path, required=True)
    manifest.add_argument("--output-json", type=Path, required=True)
    manifest.add_argument("--default-range-cache-bytes", type=int, default=0)
    inventory = commands.add_parser("query-inventory")
    inventory.add_argument("--corpus", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--paths-output", type=Path, required=True)
    compare_query = commands.add_parser("compare-queries")
    compare_query.add_argument("--index", type=Path, required=True)
    compare_query.add_argument("--manifest", type=Path, required=True)
    compare_query.add_argument("--summary", type=Path, required=True)
    compare_query.add_argument("--output", type=Path, required=True)
    compare_query.add_argument("--blocks", type=int, required=True)
    compare_query.add_argument("--benchmark-repeats", type=int, required=True)
    compare_query.add_argument("--queue-depth", type=int, required=True)
    compare_query.add_argument("--label-materialization", choices=("full", "demand-driven"), required=True)
    compare_query.add_argument("--max-matched-series", type=int, required=True)
    compare_query.add_argument("--max-projected-series", type=int, required=True)
    compare_query.add_argument("--max-chunk-reads", type=int, required=True)
    compare_query.add_argument("--max-bytes-read", type=int, required=True)
    compare_query.add_argument("--max-samples-decoded", type=int, required=True)
    compare_query.add_argument("--max-regex-values-examined", type=int, required=True)
    guardian = commands.add_parser("guard-conflicts")
    guardian.add_argument("--parent-pid", type=int, required=True)
    guardian.add_argument("--stop-file", type=Path, required=True)
    guardian.add_argument("--output", type=Path, required=True)
    guardian.add_argument("--interval-ms", type=int, default=100)
    guardian.add_argument("--filesystem", type=Path, required=True)
    guardian.add_argument("--minimum-free-bytes", type=int, required=True)
    guardian.add_argument("--samples", type=Path, required=True)
    guardian.add_argument("--summary", type=Path, required=True)
    guardian.add_argument("--ready-file", type=Path, required=True)
    precheck = commands.add_parser("check-current-conflicts")
    precheck.add_argument("--parent-pid", type=int, required=True)
    precheck.add_argument("--output", type=Path)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "source-seal":
            _write_json(args.output, source_seal(args.repo))
        elif args.command == "check-source-seal":
            print(json.dumps(check_source_seal(args.repo, args.seal), sort_keys=True))
        elif args.command == "source-snapshot-seal":
            _write_json(
                args.output,
                source_snapshot_seal(args.repo, args.snapshot, args.source_seal),
            )
        elif args.command == "check-source-snapshot-seal":
            print(
                json.dumps(
                    check_source_snapshot_seal(
                        args.repo,
                        args.snapshot,
                        args.source_seal,
                        args.seal,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "check-cargo-config-isolation":
            print(
                json.dumps(
                    cargo_config_isolation(args.snapshot, args.cargo_home),
                    sort_keys=True,
                )
            )
        elif args.command == "final-artifact-inventory":
            write_final_artifact_inventory(args.result_dir, args.output)
        elif args.command == "verify-final-artifact-seal":
            verify_final_artifact_seal(args.result_dir)
        elif args.command == "admission-plan":
            write_admission_plan(args)
        elif args.command == "write-invocation":
            write_invocation(args.binary, args.role, args.arg, args.env, args.output)
        elif args.command == "raw-leaf-seal":
            raw_leaf_seal(args.result_dir, args.file, args.tree, args.output)
        elif args.command == "validate-residency-evidence":
            print(
                json.dumps(
                    validate_residency_evidence(
                        args.input,
                        args.phase,
                        args.paths,
                        args.ceiling_bytes,
                        args.page_size_bytes,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "validate-writeback-evidence":
            print(
                json.dumps(
                    validate_writeback_evidence(
                        args.input, args.phase, args.ceiling_bytes
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "write-raw-authorities":
            write_raw_authorities(
                args.result_dir,
                args.entry,
                args.output,
                args.checksum_output,
            )
        elif args.command == "find-replay-report":
            print(_single_ingestion_report(args.run_dir))
        elif args.command == "final-admission":
            final_admission(args.result_dir, args.plan, args.output)
        elif args.command == "check-ambient-env":
            forbidden = forbidden_ambient_environment(dict(os.environ))
            if forbidden:
                raise GateError(f"forbidden ambient build/runtime variables: {', '.join(forbidden)}")
            print(json.dumps({"status": "pass", "forbidden_variables": []}, sort_keys=True))
        elif args.command == "runtime-identity":
            _write_json(
                args.output,
                runtime_identity(args.binary, args.role, args.env, set(args.normalize_env)),
            )
        elif args.command == "render-config":
            print(json.dumps(render_config(args.template, args.output, args.capture, args.segments_dir, args.stop_after_messages, args.codec), sort_keys=True))
        elif args.command == "capture-inventory":
            capture_inventory(args.capture, args.output, args.paths_output)
        elif args.command == "tree-manifest":
            replay.write_tree_manifest(args.corpus, args.manifest, args.inventory, args.summary)
        elif args.command == "artifact-inventory":
            artifact_inventory(args.corpus, args.output)
        elif args.command == "replay-report":
            parse_replay_report(args.report, args.output)
        elif args.command == "parse-seal-log":
            parse_seal_log(args.log, args.output)
        elif args.command == "parse-time":
            replay.parse_gnu_time(args.input, args.output)
        elif args.command == "parse-perf":
            parse_perf(args.input, args.output)
        elif args.command == "monitor-rss":
            monitor_rss(
                args.pid,
                args.output,
                args.summary,
                args.interval_ms,
                args.control,
                args.rss_ready,
                args.capacity_ready,
                args.launch,
            )
        elif args.command == "capacity-contract":
            document = build_capacity_contract(
                args.expectations, args.repo, args.source_head, args.replay_blocks
            )
            if args.output:
                _write_json(args.output, document)
            else:
                print(json.dumps(document, indent=2, sort_keys=True))
        elif args.command == "capacity-snapshot":
            document = capacity_snapshot(
                args.filesystem, args.minimum_free_bytes, args.phase
            )
            if args.output:
                _write_json(args.output, document)
            else:
                print(json.dumps(document, indent=2, sort_keys=True))
        elif args.command == "monitor-capacity":
            monitor_capacity(
                args.pid,
                args.filesystem,
                args.minimum_free_bytes,
                args.interval_ms,
                args.output,
                args.summary,
                args.control,
                args.rss_ready,
                args.capacity_ready,
                args.launch,
            )
        elif args.command == "create-replay-monitor-control":
            print(
                json.dumps(
                    create_replay_monitor_control(
                        args.output,
                        args.rss_ready,
                        args.capacity_ready,
                        args.launch,
                        args.root_pid,
                        args.root_ppid,
                        args.root_starttime_ticks,
                        args.rss_pid,
                        args.rss_ppid,
                        args.rss_starttime_ticks,
                        args.capacity_pid,
                        args.capacity_ppid,
                        args.capacity_starttime_ticks,
                        args.interval_ms,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "wait-replay-monitors-ready":
            print(
                json.dumps(
                    wait_replay_monitors_ready(
                        args.control,
                        args.rss_ready,
                        args.capacity_ready,
                        args.launch,
                        args.interval_ms,
                        args.timeout_ms,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "release-replay-launch":
            print(
                json.dumps(
                    release_replay_launch(
                        args.control,
                        args.rss_ready,
                        args.capacity_ready,
                        args.launch,
                        args.interval_ms,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "cleanup-replay-processes":
            print(
                json.dumps(
                    cleanup_replay_processes(
                        args.control,
                        args.rss_ready,
                        args.capacity_ready,
                        args.launch,
                        args.interval_ms,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "terminate-process-tree":
            _terminate_process_tree(
                args.root_pid,
                _ProcessIdentity(
                    args.root_pid,
                    args.root_ppid,
                    "?",
                    args.root_starttime_ticks,
                ),
            )
            print(json.dumps({"status": "pass"}, sort_keys=True))
        elif args.command == "create-empty-marker":
            _create_empty_read_only_marker(args.output, "empty marker")
            print(json.dumps({"status": "pass"}, sort_keys=True))
        elif args.command == "check-corpus-capacity":
            document = check_corpus_capacity(
                args.summary, args.contract, args.codec
            )
            if args.output:
                _write_json(args.output, document)
            else:
                print(json.dumps(document, indent=2, sort_keys=True))
        elif args.command == "compare-replays":
            compare_replays(args.index, _positive(args.blocks, "blocks"), args.output, args.summary)
        elif args.command == "compare-verifiers":
            compare_verifiers(args.raw, args.gorilla, args.output)
        elif args.command == "check-readback":
            check_readback(args.report, args.output)
        elif args.command == "normalize-query-manifest":
            normalize_manifest(args.input, args.output_tsv, args.output_json, _nonnegative(args.default_range_cache_bytes, "default cache"))
        elif args.command == "query-inventory":
            query_common.write_inventory(args.corpus, args.output, args.paths_output)
        elif args.command == "compare-queries":
            compare_queries(args)
        elif args.command == "guard-conflicts":
            guard_conflicts(
                args.parent_pid,
                args.stop_file,
                args.output,
                args.interval_ms,
                args.filesystem,
                args.minimum_free_bytes,
                args.samples,
                args.summary,
                args.ready_file,
            )
        elif args.command == "check-current-conflicts":
            document = check_current_conflicts(args.parent_pid)
            if args.output:
                _write_json(args.output, document)
            else:
                print(json.dumps(document, indent=2, sort_keys=True))
        else:
            raise AssertionError(args.command)
    except (
        GateError,
        replay.GateError,
        ab_gate.GateError,
        query.GateError,
        query_common.GateError,
        phase3.GateError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
    ) as error:
        print(f"Phase 6 codec gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
