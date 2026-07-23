#!/usr/bin/env python3
"""Strict deterministic gates for the Phase 5 multi-partition head experiment."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shlex
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import types
from pathlib import Path, PurePosixPath
from typing import Any


REPARTITION_SCHEMA = "chronoxide-capture-repartition-v2"
STRUCTURE_SCHEMA = "chronoxide/head-topology-structure/v1"
SUMMARY_SCHEMA = "chronoxide/head-topology-factorial-matrix/v2"
STORAGE_VALIDATION_SCHEMA = "chronoxide/head-topology-storage-validation/v2"
PERFORMANCE_SCHEMA = "chronoxide/head-topology-factorial-performance/v2"
SOURCE_SEAL_SCHEMA = "chronoxide/head-topology-source-seal/v1"
SOURCE_ARCHIVE_SEAL_SCHEMA = "chronoxide/head-topology-source-archive-seal/v1"
SOURCE_SNAPSHOT_SEAL_SCHEMA = "chronoxide/head-topology-source-snapshot-seal/v1"
RUNTIME_IDENTITY_SCHEMA = "chronoxide/head-topology-runtime-identity/v1"
CAPTURE_INVENTORY_SCHEMA = "chronoxide/head-topology-capture-inventory/v1"
RUN_PLAN_SCHEMA = "chronoxide/head-topology-factorial-run-plan/v2"
REPLAY_SUMMARY_SCHEMA = "chronoxide/head-topology-factorial-replay-summary/v2"
FINAL_SEAL_SCHEMA = "chronoxide/head-topology-final-seal/v1"
LIFECYCLE_CONTROL_SCHEMA = "chronoxide/head-topology-guardian-control/v1"
LIFECYCLE_GUARDIAN_SCHEMA = "chronoxide/head-topology-guardian/v2"
LIFECYCLE_RSS_SCHEMA = "chronoxide/head-topology-rss-monitor/v2"
LIFECYCLE_CONFLICT_SCAN_SCHEMA = "chronoxide/head-topology-conflict-scan/v1"
LIFECYCLE_CADENCE_INTERVAL_MS = 100
LIFECYCLE_CADENCE_EDGE_ALLOWANCE_NS = 100_000_000
COMPLETE_MARKER = "chronoxide/head-topology-complete/v1\n"
PHASE1_EXPECTATIONS_SCHEMA = "chronoxide/storage-vnext-phase1-expectations/v1"
FROZEN_HARNESS_SCHEMA = "chronoxide/head-topology-frozen-harness/v1"
FROZEN_HARNESS_FILES = (
    "ab_gate.py",
    "fadvise_regular_dontneed.c",
    "phase1_4m_expectations.json",
    "phase1_replay_gate.py",
    "phase5_head_topology_gate.py",
    "phase5_head_topology_guard.py",
    "phase5_head_topology_run.sh",
    "test_phase5_head_topology_gate.py",
    "test_phase5_head_topology_guard.py",
)
PARTITION_COUNT = 16
PROMOTION_TASK_CLOCK_GEOMEAN_MAX = 0.97
PROMOTION_TASK_CLOCK_PAIR_MAX = 1.03
PROMOTION_RSS_GEOMEAN_MAX = 1.05
PROMOTION_RSS_PAIR_MAX = 1.10
REJECTION_TASK_CLOCK_GEOMEAN_MIN = 1.03
REJECTION_TASK_CLOCK_PAIR_MIN = 1.05
REJECTION_RSS_GEOMEAN_MIN = 1.10
REJECTION_RSS_PAIR_MIN = 1.15
UNIFORM_MAPPING_SPEC = "destination_partition = global_ordinal % partition_count"
SKEW_MAPPING_SPEC = (
    "global_ordinal % 5 in 0..=3 -> partition 0; every fifth record -> "
    "1 + ((global_ordinal / 5) % (partition_count - 1))"
)
REPARTITION_PARTITION_FIELDS = {
    "partition",
    "message_count",
    "payload_bytes",
    "first_global_ordinal",
    "last_global_ordinal",
}

STORAGE_REPORT_FIELDS = {
    "schema_version",
    "footer_validation_enabled",
    "series_sample_per_segment",
    "verified_selection_fingerprint",
    "decoded_semantic_fingerprint",
    "topology_independent_decoded_semantic_fingerprint",
    "segments",
    "corpus_series",
    "series",
    "chunks",
    "chunks_by_kind",
    "samples",
    "logical_chunk_bytes",
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

SERIES_INTEGER_FIELDS = {
    "windows",
    "in_order_windows",
    "in_order_rotations",
    "out_of_order_windows",
    "adaptive_windows",
    "series_total",
    "direct_pages_total",
    "direct_series_total",
    "sparse_pages_total",
    "sparse_series_total",
    "refs_above_paged_limit_total",
    "max_page_directory_len",
    "max_page_directory_capacity",
    "max_sparse_capacity",
    "max_sparse_slot_capacity",
    "max_direct_slot_index_bytes",
    "max_direct_reverse_slot_capacity",
    "max_direct_value_capacity",
}
LAST_INTEGER_FIELDS = {
    "series",
    "dense_pages",
    "dense_series",
    "sparse_pages",
    "sparse_series",
    "refs_above_paged_limit",
    "page_directory_len",
    "page_directory_capacity",
    "sparse_capacity",
    "paged_allocated_bytes",
}
PAIR_SERIES_IDENTITY_FIELDS = {
    "windows",
    "in_order_windows",
    "in_order_rotations",
    "out_of_order_windows",
    "series_total",
    "refs_above_paged_limit_total",
}
PAIR_LAST_IDENTITY_FIELDS = {"series", "refs_above_paged_limit"}
CELL_FACTORS = {
    # Cell names are ordered (series-table, last-timestamp-table).
    "pp": (False, False),
    "ap": (True, False),
    "pa": (False, True),
    "aa": (True, True),
}
EXPECTED_RUNS = (
    # The two plain/plain cells seed the dynamic disk bound. The remaining
    # cells complete one full 2x2 factorial per topology without increasing
    # the historical eight-corpus disk envelope.
    "uniform-pp-01",
    "skew80-20-pp-01",
    "uniform-ap-01",
    "uniform-pa-01",
    "uniform-aa-01",
    "skew80-20-pa-01",
    "skew80-20-ap-01",
    "skew80-20-aa-01",
)
TOPOLOGIES = ("uniform", "skew80-20")
TRANSFORM_LABELS = (
    "prefix-uniform-a",
    "prefix-uniform-b",
    "prefix-skew80-20-a",
    "prefix-skew80-20-b",
    "full-uniform",
    "full-skew80-20",
)
CAPTURE_LABELS = (
    "uniform",
    "skew80-20",
    "determinism-uniform-a",
    "determinism-uniform-b",
    "determinism-skew80-20-a",
    "determinism-skew80-20-b",
)
PERF_EVENTS = (
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
PERFORMANCE_MARKERS = {
    "promote": "PERFORMANCE_PROMOTE",
    "reject": "PERFORMANCE_REJECT",
    "defer": "PERFORMANCE_DEFER",
}
FORBIDDEN_AMBIENT_ENV_EXACT = {
    "AR",
    "CC",
    "CFLAGS",
    "CONFIG_FILE",
    "CXX",
    "CXXFLAGS",
    "DYLD_INSERT_LIBRARIES",
    "GLIBC_TUNABLES",
    "LD_LIBRARY_PATH",
    "LD_PRELOAD",
    "LDFLAGS",
    "MALLOC_CONF",
    "RUSTC",
    "RUSTC_WRAPPER",
    "RUSTC_WORKSPACE_WRAPPER",
    "RUSTDOCFLAGS",
    "RUSTFLAGS",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "RUST_LOG",
}
FORBIDDEN_AMBIENT_ENV_PREFIXES = (
    "CARGO_",
    "JEMALLOC_",
    "MALLOC_",
    "MIMALLOC_",
    "SCCACHE_",
    "TCMALLOC_",
)

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
FORBIDDEN_PROCESS_NAMES = re.compile(
    rf"^(?:cargo|cargo-nextest|rustc|rustdoc|clippy-driver|nextest|make|"
    rf"{NINJA_PROCESS_TOKEN}|cmake|meson|sccache|ccache|docker|podman|buildah|"
    rf"emulator|adb|gradle|gradlew|GradleDaemon|java|javac|mvn|ctest|pytest|"
    rf"{COMPILER_PROCESS_TOKEN}|cc1|cc1plus|{LINKER_PROCESS_TOKEN}|perf|"
    rf"heaptrack|valgrind.*|strace|ltrace|bpftrace|hotspot|chronoxide-.*|"
    rf"greptime.*|clickhouse.*|postgres.*|mysqld|influxd|victoria.*|"
    rf"vm(?:storage|select|agent)|mimir.*|thanos.*|cortex.*|prometheus|"
    rf"qemu-kvm|qemu-system.*|{SOONG_PROCESS_TOKEN}|ckati|kati|kotlinc|"
    rf"metalava|aapt|aapt2|aidl|dex2oat|btop|htop|top)$",
    re.IGNORECASE,
)
FORBIDDEN_PROCESS_COMMAND = re.compile(
    rf"(?:^|[/ ])(?:cargo(?:-nextest)?|rustc|rustdoc|clippy-driver|nextest|"
    rf"{NINJA_PROCESS_TOKEN}|{COMPILER_PROCESS_TOKEN}|{LINKER_PROCESS_TOKEN}|"
    rf"{SOONG_PROCESS_TOKEN}|ckati|kati|gradlew?|metalava|aapt2?|aidl|"
    rf"dex2oat)(?:$|[ /])|Android[/ ](?:SDK )?emulator",
    re.IGNORECASE,
)
FORBIDDEN_PROCESS_COMMAND_MARKERS = (
    "org.gradle.",
    "GradleWorkerMain",
    "com.android.build.gradle",
    "/Android/Sdk/emulator/",
)


class GateError(RuntimeError):
    pass


def _raise_walk_error(error: OSError) -> None:
    raise GateError(f"filesystem traversal failed: {error}") from error


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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(16 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _regular_file(path: Path, label: str, *, executable: bool = False) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as error:
        raise GateError(f"{label} is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise GateError(f"{label} must be a regular non-symlink file: {path}")
    if executable and mode & 0o111 == 0:
        raise GateError(f"{label} must be executable: {path}")


def check_frozen_harness(harness: Path) -> dict[str, Any]:
    """Require one immutable, cache-free set of formal harness sources."""
    if not harness.is_absolute():
        raise GateError("frozen harness path must be absolute")
    try:
        harness_mode = harness.lstat().st_mode
    except FileNotFoundError as error:
        raise GateError(f"frozen harness is missing: {harness}") from error
    if stat.S_ISLNK(harness_mode) or not stat.S_ISDIR(harness_mode):
        raise GateError("frozen harness must be a non-symlink directory")
    resolved = harness.resolve(strict=True)
    if resolved != harness:
        raise GateError("frozen harness path must not traverse a symlink")
    if stat.S_IMODE(harness_mode) != 0o555:
        raise GateError("frozen harness directory must be mode 0555")

    try:
        entries = list(harness.iterdir())
    except OSError as error:
        raise GateError(f"frozen harness enumeration failed: {error}") from error
    observed = {entry.name for entry in entries}
    expected = set(FROZEN_HARNESS_FILES)
    if observed != expected:
        raise GateError(
            "frozen harness path set differs: "
            f"missing={sorted(expected - observed)} extra={sorted(observed - expected)}"
        )
    for name in FROZEN_HARNESS_FILES:
        path = harness / name
        _regular_file(path, f"frozen harness entry {name}")
        if stat.S_IMODE(path.stat().st_mode) != 0o444:
            raise GateError(f"frozen harness entry must be mode 0444: {name}")
    return {
        "schema": FROZEN_HARNESS_SCHEMA,
        "file_count": len(FROZEN_HARNESS_FILES),
        "cache_free": True,
        "read_only": True,
        "status": "pass",
    }


def _is_excluded_runtime_artifact(path: str) -> bool:
    return (
        path.startswith("chronoxide-ingester/")
        and Path(path).name.startswith("ingestion_stats_")
        and path.endswith(".md")
    ) or "/__pycache__/" in f"/{path}" or path.endswith(".pyc")


def _is_ignored_build_input_candidate(path: str) -> bool:
    if _is_excluded_runtime_artifact(path) or path.startswith("target/"):
        return False
    candidate = Path(path)
    return (
        path == ".cargo/config"
        or path.endswith("/.cargo/config")
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
    repo = repo.resolve(strict=True)
    root = Path(str(_git(repo, "rev-parse", "--show-toplevel")).strip())
    if root != repo:
        raise GateError(f"source root is not the Git worktree root: {repo}")
    if str(_git(repo, "status", "--porcelain=v1", "--untracked-files=no")).strip():
        raise GateError("formal source-bound build requires a clean tracked worktree and index")

    flags_output = bytes(_git(repo, "ls-files", "-v", "-z", binary=True))
    for entry in (item for item in flags_output.split(b"\0") if item):
        if len(entry) < 3 or entry[1:2] != b" ":
            raise GateError("git ls-files -v returned a malformed tracked-file entry")
        flag = chr(entry[0])
        if flag != "H":
            path = entry[2:].decode("utf-8")
            raise GateError(
                f"formal source-bound build rejects nonordinary Git index flag {flag!r}: {path}"
            )

    untracked_output = bytes(
        _git(repo, "ls-files", "--others", "--exclude-standard", "-z", binary=True)
    )
    untracked = [item.decode("utf-8") for item in untracked_output.split(b"\0") if item]
    disallowed = [path for path in untracked if not _is_excluded_runtime_artifact(path)]
    if disallowed:
        raise GateError(f"formal source-bound build rejects untracked build inputs: {disallowed[0]}")
    ignored_output = bytes(
        _git(
            repo,
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "-z",
            binary=True,
        )
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
    tracked_modes: dict[str, str] = {}
    for entry in (item for item in tracked_index.split(b"\0") if item):
        try:
            metadata, path_bytes = entry.split(b"\t", 1)
            mode, _object_id, stage = metadata.split(b" ", 2)
        except ValueError as error:
            raise GateError("git ls-files -s returned a malformed tracked-file entry") from error
        path = path_bytes.decode("utf-8")
        if mode not in {b"100644", b"100755"}:
            raise GateError(
                "formal source-bound build rejects unsupported tracked Git mode "
                f"{mode.decode('ascii', errors='replace')}: {path}"
            )
        if stage != b"0":
            raise GateError(f"formal source-bound build rejects nonzero Git index stage: {path}")
        tracked_modes[path] = mode.decode("ascii")
    if set(tracked_modes) != set(tracked):
        raise GateError("tracked index entries disagree with the tracked path set")

    worktree_digest = hashlib.sha256(b"chronoxide-phase5-tracked-worktree-v1\0")
    cargo_configs = []
    for relative in tracked:
        path = repo / relative
        _regular_file(path, f"tracked file {relative}")
        path_bytes = relative.encode("utf-8")
        file_digest = _sha256(path)
        worktree_digest.update(len(path_bytes).to_bytes(8, "little"))
        worktree_digest.update(path_bytes)
        worktree_digest.update(tracked_modes[relative].encode("ascii"))
        worktree_digest.update(path.stat().st_size.to_bytes(8, "little"))
        worktree_digest.update(bytes.fromhex(file_digest))
        if relative.endswith("/.cargo/config") or relative.endswith(
            "/.cargo/config.toml"
        ) or relative in {".cargo/config", ".cargo/config.toml"}:
            cargo_configs.append(
                {"path": relative, "sha256": file_digest, "size_bytes": path.stat().st_size}
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
    if not re.fullmatch(r"[0-9a-f]{40,64}", head) or not re.fullmatch(
        r"[0-9a-f]{40,64}", tree
    ):
        raise GateError("Git HEAD or tree object id has an invalid shape")
    identity = {
        "head": head,
        "tree": tree,
        "tracked_index_sha256": hashlib.sha256(tracked_index).hexdigest(),
        "tracked_worktree_sha256": worktree_digest.hexdigest(),
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
    expected = _read_json(seal_path)
    current = source_seal(repo)
    for key in (
        "schema",
        "repo",
        "head",
        "tree",
        "tracked_index_sha256",
        "tracked_worktree_sha256",
        "tracked_file_count",
        "cargo_lock_sha256",
        "cargo_configs",
        "identity_sha256",
    ):
        if expected.get(key) != current[key]:
            raise GateError(f"source seal changed: {key}")
    return {"status": "pass", "identity_sha256": current["identity_sha256"]}


def _git_head_files(repo: Path) -> tuple[str, list[dict[str, Any]]]:
    object_format = str(_git(repo, "rev-parse", "--show-object-format")).strip()
    if object_format not in {"sha1", "sha256"}:
        raise GateError(f"unsupported Git object format: {object_format}")
    output = bytes(
        _git(repo, "ls-tree", "-r", "-z", "--full-tree", "HEAD", binary=True)
    )
    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in (item for item in output.split(b"\0") if item):
        try:
            metadata, path_bytes = entry.split(b"\t", 1)
            mode_bytes, kind, object_id_bytes = metadata.split(b" ", 2)
            relative = path_bytes.decode("utf-8")
            mode = mode_bytes.decode("ascii")
            object_id = object_id_bytes.decode("ascii")
        except (UnicodeDecodeError, ValueError) as error:
            raise GateError("git ls-tree returned a malformed HEAD entry") from error
        if kind != b"blob" or mode not in {"100644", "100755"}:
            raise GateError(
                f"formal archive rejects unsupported HEAD entry: {mode} "
                f"{kind.decode('ascii', errors='replace')} {relative}"
            )
        if relative in seen:
            raise GateError(f"formal archive contains duplicate HEAD path: {relative}")
        seen.add(relative)
        files.append({"path": relative, "mode": mode, "object_id": object_id})
    if not files:
        raise GateError("Git HEAD contains no regular files")
    return object_format, files


def _git_blob_oid_from_stream(
    source: Any, size: int, object_format: str, destination: Any | None = None
) -> str:
    try:
        digest = hashlib.new(object_format)
    except ValueError as error:
        raise GateError(f"unsupported Git object format: {object_format}") from error
    digest.update(f"blob {size}\0".encode())
    observed = 0
    while block := source.read(1024 * 1024):
        observed += len(block)
        digest.update(block)
        if destination is not None:
            destination.write(block)
    if observed != size:
        raise GateError(f"archive member length differs: expected {size}, observed {observed}")
    return digest.hexdigest()


def _safe_archive_name(name: str) -> str:
    candidate = PurePosixPath(name)
    if (
        not name
        or name.startswith("/")
        or candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in candidate.parts)
        or "\0" in name
        or "\n" in name
        or "\r" in name
        or "\t" in name
    ):
        raise GateError(f"source archive contains an unsafe path: {name!r}")
    normalized = candidate.as_posix()
    if normalized != name.rstrip("/"):
        raise GateError(f"source archive path is not canonical: {name!r}")
    return normalized


def _validate_source_archive(repo: Path, archive: Path) -> dict[str, Any]:
    repo = repo.resolve(strict=True)
    archive = archive.resolve(strict=True)
    _regular_file(archive, "formal source archive")
    if stat.S_IMODE(archive.stat().st_mode) != 0o444:
        raise GateError("formal source archive must be mode 0444")
    object_format, head_files = _git_head_files(repo)
    expected = {row["path"]: row for row in head_files}
    expected_directories = {
        parent.as_posix()
        for relative in expected
        for parent in PurePosixPath(relative).parents
        if parent.as_posix() != "."
    }
    observed_files: dict[str, dict[str, Any]] = {}
    observed_directories: set[str] = set()
    try:
        source = tarfile.open(archive, mode="r:")
    except (OSError, tarfile.TarError) as error:
        raise GateError(f"formal source archive is not a valid uncompressed tar: {error}") from error
    with source:
        for member in source.getmembers():
            relative = _safe_archive_name(member.name)
            if member.mode & 0o7000:
                raise GateError(f"source archive member has privileged mode bits: {relative}")
            if member.isdir():
                if relative in observed_directories or relative in observed_files:
                    raise GateError(f"source archive contains a duplicate path: {relative}")
                observed_directories.add(relative)
                continue
            if not member.isfile() or member.islnk() or member.issym():
                raise GateError(f"source archive contains a non-regular entry: {relative}")
            if relative in observed_files or relative in observed_directories:
                raise GateError(f"source archive contains a duplicate path: {relative}")
            expected_row = expected.get(relative)
            if expected_row is None:
                raise GateError(f"source archive contains a path absent from HEAD: {relative}")
            executable = bool(member.mode & 0o111)
            archive_mode = "100755" if executable else "100644"
            if archive_mode != expected_row["mode"]:
                raise GateError(f"source archive executable mode differs from HEAD: {relative}")
            extracted = source.extractfile(member)
            if extracted is None:
                raise GateError(f"source archive regular member is unreadable: {relative}")
            with extracted:
                object_id = _git_blob_oid_from_stream(
                    extracted, member.size, object_format
                )
            if object_id != expected_row["object_id"]:
                raise GateError(f"source archive bytes differ from HEAD: {relative}")
            observed_files[relative] = {
                **expected_row,
                "size_bytes": member.size,
            }
    if set(observed_files) != set(expected):
        missing = sorted(set(expected) - set(observed_files))
        extra = sorted(set(observed_files) - set(expected))
        raise GateError(
            f"source archive file set differs from HEAD: missing={missing[:1]} extra={extra[:1]}"
        )
    if observed_directories != expected_directories:
        missing = sorted(expected_directories - observed_directories)
        extra = sorted(observed_directories - expected_directories)
        raise GateError(
            "source archive directory set differs from HEAD: "
            f"missing={missing[:1]} extra={extra[:1]}"
        )
    files = [observed_files[path] for path in sorted(observed_files)]
    identity = {
        "git_head": str(_git(repo, "rev-parse", "HEAD")).strip(),
        "git_tree": str(_git(repo, "rev-parse", "HEAD^{tree}")).strip(),
        "object_format": object_format,
        "archive_sha256": _sha256(archive),
        "archive_size_bytes": archive.stat().st_size,
        "file_count": len(files),
        "files": files,
    }
    identity_sha256 = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return {
        "schema": SOURCE_ARCHIVE_SEAL_SCHEMA,
        "repo": str(repo),
        "archive": str(archive),
        **identity,
        "identity_sha256": identity_sha256,
    }


def extract_source_archive(
    repo: Path, archive: Path, destination: Path
) -> dict[str, Any]:
    seal = _validate_source_archive(repo, archive)
    if destination.exists() or destination.is_symlink():
        raise GateError(f"source snapshot destination must be new: {destination}")
    if not destination.is_absolute():
        raise GateError("source snapshot destination must be absolute")
    destination.mkdir(mode=0o700)
    with tarfile.open(archive, mode="r:") as source:
        for member in source.getmembers():
            relative = _safe_archive_name(member.name)
            target = destination.joinpath(*PurePosixPath(relative).parts)
            if member.isdir():
                target.mkdir(mode=0o700, parents=True, exist_ok=True)
                continue
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            extracted = source.extractfile(member)
            if extracted is None:
                raise GateError(f"source archive regular member is unreadable: {relative}")
            descriptor = os.open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            with extracted, os.fdopen(descriptor, "wb") as output:
                object_id = _git_blob_oid_from_stream(
                    extracted, member.size, seal["object_format"], output
                )
            expected = next(row for row in seal["files"] if row["path"] == relative)
            if object_id != expected["object_id"]:
                raise GateError(f"extracted source bytes differ from HEAD: {relative}")
            target.chmod(0o555 if expected["mode"] == "100755" else 0o444)
    directories = [destination] + [path for path in destination.rglob("*") if path.is_dir()]
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        directory.chmod(0o555)
    return seal


def check_source_archive_seal(repo: Path, archive: Path, seal_path: Path) -> dict[str, Any]:
    expected = _read_json(seal_path)
    current = _validate_source_archive(repo, archive)
    if expected != current:
        raise GateError("formal source archive seal changed")
    return {"status": "pass", "identity_sha256": current["identity_sha256"]}


def _git_blob_oid(path: Path, object_format: str) -> str:
    with path.open("rb") as source:
        return _git_blob_oid_from_stream(source, path.stat().st_size, object_format)


def source_snapshot_seal(repo: Path, snapshot: Path) -> dict[str, Any]:
    repo = repo.resolve(strict=True)
    snapshot = snapshot.resolve(strict=True)
    if not snapshot.is_dir() or snapshot.is_symlink() or not snapshot.is_absolute():
        raise GateError("source snapshot must be an absolute non-symlink directory")
    if snapshot == repo or repo in snapshot.parents:
        raise GateError("source snapshot must be outside the live Git worktree")
    if stat.S_IMODE(snapshot.stat().st_mode) != 0o555:
        raise GateError("source snapshot root must be mode 0555")

    parent = snapshot.parent
    while True:
        for name in ("config", "config.toml"):
            ambient = parent / ".cargo" / name
            if ambient.exists() or ambient.is_symlink():
                raise GateError(f"ambient source-snapshot ancestor Cargo config is forbidden: {ambient}")
        for name in ("rust-toolchain", "rust-toolchain.toml"):
            ambient = parent / name
            if ambient.exists() or ambient.is_symlink():
                raise GateError(f"ambient source-snapshot ancestor toolchain file is forbidden: {ambient}")
        if parent == parent.parent:
            break
        parent = parent.parent

    object_format, head_files = _git_head_files(repo)
    expected = {row["path"]: row for row in head_files}
    observed: dict[str, Path] = {}
    for candidate in snapshot.rglob("*"):
        relative = candidate.relative_to(snapshot).as_posix()
        if candidate.is_symlink():
            raise GateError(f"source snapshot contains a symlink: {relative}")
        if candidate.is_dir():
            if stat.S_IMODE(candidate.stat().st_mode) != 0o555:
                raise GateError(f"source snapshot directory is not mode 0555: {relative}")
            continue
        if not candidate.is_file():
            raise GateError(f"source snapshot contains a non-regular entry: {relative}")
        if relative in observed:
            raise GateError(f"source snapshot contains a duplicate path: {relative}")
        observed[relative] = candidate
    if set(observed) != set(expected):
        missing = sorted(set(expected) - set(observed))
        extra = sorted(set(observed) - set(expected))
        raise GateError(
            f"source snapshot path set differs from HEAD: missing={missing[:1]} extra={extra[:1]}"
        )
    files: list[dict[str, Any]] = []
    for relative in sorted(expected):
        path = observed[relative]
        expected_row = expected[relative]
        required_mode = 0o555 if expected_row["mode"] == "100755" else 0o444
        if stat.S_IMODE(path.stat().st_mode) != required_mode:
            raise GateError(
                f"source snapshot file mode differs for {relative}: expected {required_mode:04o}"
            )
        if _git_blob_oid(path, object_format) != expected_row["object_id"]:
            raise GateError(f"source snapshot bytes differ from HEAD: {relative}")
        files.append({**expected_row, "size_bytes": path.stat().st_size})
    identity = {
        "git_head": str(_git(repo, "rev-parse", "HEAD")).strip(),
        "git_tree": str(_git(repo, "rev-parse", "HEAD^{tree}")).strip(),
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
    repo: Path, snapshot: Path, seal_path: Path
) -> dict[str, Any]:
    expected = _read_json(seal_path)
    current = source_snapshot_seal(repo, snapshot)
    if expected != current:
        raise GateError("read-only source snapshot seal changed")
    return {"status": "pass", "identity_sha256": current["identity_sha256"]}


def forbidden_ambient_environment(environment: dict[str, str]) -> list[str]:
    return sorted(
        name
        for name in environment
        if name in FORBIDDEN_AMBIENT_ENV_EXACT
        or any(name.startswith(prefix) for prefix in FORBIDDEN_AMBIENT_ENV_PREFIXES)
    )


def is_forbidden_process(name: str, command: str) -> bool:
    return (
        FORBIDDEN_PROCESS_NAMES.fullmatch(name) is not None
        or FORBIDDEN_PROCESS_COMMAND.search(command) is not None
        or any(marker in command for marker in FORBIDDEN_PROCESS_COMMAND_MARKERS)
    )


def validate_process_snapshot(path: Path) -> dict[str, Any]:
    _regular_file(path, "process snapshot")
    rows = path.read_text(encoding="utf-8").splitlines()
    if not rows:
        raise GateError("process snapshot is empty")
    parsed = 0
    for line_number, line in enumerate(rows, 1):
        fields = line.strip().split(None, 3)
        if len(fields) < 3:
            raise GateError(f"process snapshot line {line_number} has an invalid shape")
        try:
            pid = int(fields[0])
            parent_pid = int(fields[1])
        except ValueError as error:
            raise GateError(
                f"process snapshot line {line_number} has an invalid pid"
            ) from error
        if pid <= 0 or parent_pid < 0:
            raise GateError(f"process snapshot line {line_number} has an invalid pid")
        name = fields[2]
        command = fields[3] if len(fields) == 4 else name
        if is_forbidden_process(name, command):
            raise GateError(
                f"measurement conflict in process snapshot: pid={pid} comm={name}"
            )
        parsed += 1
    return {"processes": parsed, "conflicts": 0, "validated": True}


def _lifecycle_maximum_allowed_gap_ns(interval_ms: int) -> int:
    if interval_ms != LIFECYCLE_CADENCE_INTERVAL_MS:
        raise GateError("formal lifecycle cadence must be exactly 100 ms")
    return interval_ms * 1_000_000 + LIFECYCLE_CADENCE_EDGE_ALLOWANCE_NS


def _lifecycle_maximum_gap_ns(timestamps: list[int], terminal_elapsed_ns: int) -> int:
    if terminal_elapsed_ns < 0 or any(value < 0 for value in timestamps):
        raise GateError("lifecycle cadence timestamps must be non-negative")
    if any(later <= earlier for earlier, later in zip(timestamps, timestamps[1:])):
        raise GateError("lifecycle cadence timestamps are not strictly increasing")
    if timestamps and timestamps[-1] > terminal_elapsed_ns:
        raise GateError("lifecycle terminal edge precedes the last sample")
    boundaries = [0, *timestamps, terminal_elapsed_ns]
    return max(
        (later - earlier for earlier, later in zip(boundaries, boundaries[1:])),
        default=0,
    )


def _validate_empty_read_only_marker(path: Path, label: str) -> None:
    _regular_file(path, label)
    if path.stat().st_size != 0 or stat.S_IMODE(path.stat().st_mode) != 0o444:
        raise GateError(f"{label} must be an exact empty mode-0444 marker")


def _validate_conflict_scan(path: Path, label: str) -> dict[str, Any]:
    document = _read_json(path)
    _regular_file(path, label)
    _require_exact_keys(document, {"schema", "conflicts", "quiet"}, label)
    if (
        document["schema"] != LIFECYCLE_CONFLICT_SCAN_SCHEMA
        or document["conflicts"] != []
        or document["quiet"] is not True
        or stat.S_IMODE(path.stat().st_mode) != 0o444
    ):
        raise GateError(f"{label} is not exact successful quiet-host evidence")
    return document


def _validate_lifecycle_control(root: Path, label: str) -> dict[str, Any]:
    path = root / "lifecycle-control.json"
    document = _read_json(path)
    _regular_file(path, f"{label} control")
    keys = {
        "schema",
        "root_pid",
        "root_starttime_ticks",
        "guardian_pid",
        "guardian_starttime_ticks",
        "rss_monitor_pid",
        "rss_monitor_starttime_ticks",
        "interval_ms",
        "guardian_ready_marker",
        "rss_ready_marker",
        "launch_marker",
    }
    _require_exact_keys(document, keys, f"{label} lifecycle control")
    roles = ("root", "guardian", "rss_monitor")
    pids = [
        _strict_int(document[f"{role}_pid"], f"{label} {role} pid")
        for role in roles
    ]
    starttimes = [
        _strict_int(
            document[f"{role}_starttime_ticks"], f"{label} {role} starttime"
        )
        for role in roles
    ]
    expected_paths = {
        "guardian_ready_marker": root / "guardian-ready",
        "rss_ready_marker": root / "rss-ready",
        "launch_marker": root / "launch",
    }
    if (
        document["schema"] != LIFECYCLE_CONTROL_SCHEMA
        or document["interval_ms"] != LIFECYCLE_CADENCE_INTERVAL_MS
        or any(value <= 1 for value in pids)
        or any(value < 1 for value in starttimes)
        or len(set(pids)) != len(pids)
        or any(document[name] != str(path) for name, path in expected_paths.items())
        or stat.S_IMODE(path.stat().st_mode) != 0o444
    ):
        raise GateError(f"{label} lifecycle control differs from the exact binding")
    for name, marker in expected_paths.items():
        _validate_empty_read_only_marker(marker, f"{label} {name}")
    return document


def _empty_lifecycle_termination(root_starttime_ticks: int) -> dict[str, Any]:
    return {
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


def _validate_guardian_evidence(
    root: Path,
    label: str,
    *,
    expected_minimum_free_bytes: int | None = None,
    expected_filesystem: Path | None = None,
) -> dict[str, Any]:
    control = _validate_lifecycle_control(root, label)
    _validate_conflict_scan(
        root / "processes-immediately-before-launch.json",
        f"{label} immediate conflict scan",
    )
    process_rows = _read_tsv(root / "process-guardian.tsv", f"{label} process guardian")
    process_header = [
        "poll_index",
        "monotonic_elapsed_ns",
        "recorded_at",
        "pid",
        "ppid",
        "state",
        "starttime_ticks",
        "name",
        "command",
    ]
    if process_rows != [process_header]:
        raise GateError(f"{label} process guardian recorded a conflict or malformed evidence")
    disk_rows = _read_tsv(root / "disk-guardian.tsv", f"{label} disk guardian")
    disk_header = [
        "poll_index",
        "monotonic_elapsed_ns",
        "recorded_at",
        "root_running",
        "launch_observed",
        "free_bytes",
        "minimum_free_bytes",
    ]
    if not disk_rows or disk_rows[0] != disk_header or len(disk_rows) < 3:
        raise GateError(f"{label} guardian lacks two samples including a terminal edge")
    timestamps: list[int] = []
    free_values: list[int] = []
    minimum_values: list[int] = []
    root_states: list[bool] = []
    launch_states: list[bool] = []
    for expected_poll, row in enumerate(disk_rows[1:], 1):
        if len(row) != len(disk_header) or row[0] != str(expected_poll) or not row[2]:
            raise GateError(f"{label} guardian row is malformed")
        timestamps.append(_strict_decimal_int(row[1], f"{label} guardian elapsed"))
        if row[3] not in {"true", "false"} or row[4] not in {"true", "false"}:
            raise GateError(f"{label} guardian state flags are malformed")
        root_states.append(row[3] == "true")
        launch_states.append(row[4] == "true")
        free_values.append(
            _strict_decimal_int(row[5], f"{label} guardian free bytes", positive=True)
        )
        minimum_values.append(
            _strict_decimal_int(row[6], f"{label} guardian minimum bytes", positive=True)
        )
    if (
        not all(root_states[:-1])
        or root_states[-1]
        or launch_states[0]
        or not any(launch_states[1:])
        or launch_states != sorted(launch_states)
        or len(set(minimum_values)) != 1
        or any(free < minimum for free, minimum in zip(free_values, minimum_values))
        or expected_minimum_free_bytes is not None
        and minimum_values[0] != expected_minimum_free_bytes
    ):
        raise GateError(f"{label} guardian lifecycle/reserve rows differ")
    summary = _read_json(root / "guardian-summary.json")
    summary_keys = {
        "schema",
        "root_pid",
        "root_starttime_ticks",
        "guardian_pid",
        "interval_ms",
        "polls",
        "terminal_elapsed_ns",
        "poll_monotonic_elapsed_ns",
        "maximum_poll_start_gap_ns",
        "maximum_allowed_poll_start_gap_ns",
        "control_path",
        "control_sha256",
        "guardian_ready_marker",
        "rss_ready_marker",
        "launch_marker",
        "ready_created_poll",
        "ready_created_monotonic_elapsed_ns",
        "launch_observed_poll",
        "launch_observed_monotonic_elapsed_ns",
        "root_seen",
        "filesystem",
        "minimum_free_bytes",
        "minimum_observed_free_bytes",
        "capacity_violations",
        "conflicts",
        "handshake_violations",
        "termination",
        "complete_and_conflict_free",
    }
    _require_exact_keys(summary, summary_keys, f"{label} guardian summary")
    terminal = _strict_int(summary["terminal_elapsed_ns"], f"{label} terminal elapsed")
    derived_gap = _lifecycle_maximum_gap_ns(timestamps, terminal)
    first_launch_poll = launch_states.index(True) + 1
    if (
        summary["schema"] != LIFECYCLE_GUARDIAN_SCHEMA
        or summary["root_pid"] != control["root_pid"]
        or summary["root_starttime_ticks"] != control["root_starttime_ticks"]
        or summary["guardian_pid"] != control["guardian_pid"]
        or summary["interval_ms"] != LIFECYCLE_CADENCE_INTERVAL_MS
        or summary["polls"] != len(timestamps)
        or summary["poll_monotonic_elapsed_ns"] != timestamps
        or summary["maximum_poll_start_gap_ns"] != derived_gap
        or summary["maximum_allowed_poll_start_gap_ns"]
        != _lifecycle_maximum_allowed_gap_ns(LIFECYCLE_CADENCE_INTERVAL_MS)
        or derived_gap > summary["maximum_allowed_poll_start_gap_ns"]
        or summary["control_path"] != str(root / "lifecycle-control.json")
        or summary["control_sha256"] != _sha256(root / "lifecycle-control.json")
        or summary["guardian_ready_marker"] != str(root / "guardian-ready")
        or summary["rss_ready_marker"] != str(root / "rss-ready")
        or summary["launch_marker"] != str(root / "launch")
        or summary["ready_created_poll"] != 1
        or summary["ready_created_monotonic_elapsed_ns"] != timestamps[0]
        or summary["launch_observed_poll"] != first_launch_poll
        or summary["launch_observed_monotonic_elapsed_ns"]
        != timestamps[first_launch_poll - 1]
        or summary["root_seen"] is not True
        or expected_filesystem is not None
        and summary["filesystem"] != str(expected_filesystem.resolve(strict=True))
        or summary["minimum_free_bytes"] != minimum_values[0]
        or summary["minimum_observed_free_bytes"] != min(free_values)
        or summary["capacity_violations"] != []
        or summary["conflicts"] != []
        or summary["handshake_violations"] != []
        or summary["termination"]
        != _empty_lifecycle_termination(control["root_starttime_ticks"])
        or summary["complete_and_conflict_free"] is not True
    ):
        raise GateError(f"{label} guardian summary is not reconstructed by raw evidence")
    if (root / "guardian.log").stat().st_size != 0:
        raise GateError(f"{label} successful guardian log is not empty")
    return summary


def runtime_identity(
    binary: Path, role: str, assignments: list[str], arguments: list[str]
) -> dict[str, Any]:
    _regular_file(binary, f"{role} binary", executable=True)
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
    elif role not in {"repartition", "query", "verifier"}:
        raise GateError(f"unknown runtime role: {role}")
    if set(environment) != expected_names:
        raise GateError(f"{role} runtime environment differs from the sanitized contract")
    if any("\0" in argument or "\n" in argument for argument in arguments):
        raise GateError("runtime argument contains a forbidden control character")
    environment_sha256 = hashlib.sha256(
        json.dumps(environment, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    arguments_sha256 = hashlib.sha256(
        json.dumps(arguments, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "schema": RUNTIME_IDENTITY_SCHEMA,
        "role": role,
        "binary": str(binary.resolve(strict=True)),
        "binary_sha256": _sha256(binary),
        "environment": environment,
        "environment_sha256": environment_sha256,
        "arguments": arguments,
        "arguments_sha256": arguments_sha256,
    }


def capture_inventory(capture: Path, output: Path, paths_output: Path) -> None:
    if not capture.is_absolute() or not capture.is_dir() or capture.is_symlink():
        raise GateError("capture must be an absolute non-symlink directory")
    rows: list[dict[str, Any]] = []
    paths: list[Path] = []
    for root, dirs, files in os.walk(
        capture, followlinks=False, onerror=_raise_walk_error
    ):
        root_path = Path(root)
        for name in dirs:
            if (root_path / name).is_symlink():
                raise GateError(f"capture contains symlink directory: {root_path / name}")
        for name in files:
            path = root_path / name
            _regular_file(path, "capture entry")
            relative = path.relative_to(capture).as_posix()
            if "\n" in relative or "\t" in relative:
                raise GateError("capture path contains a tab or newline")
            rows.append(
                {"path": relative, "size_bytes": path.stat().st_size, "sha256": _sha256(path)}
            )
            paths.append(path)
    rows.sort(key=lambda row: row["path"].encode())
    paths.sort(key=lambda path: path.relative_to(capture).as_posix().encode())
    if not rows:
        raise GateError("capture contains no regular files")
    canonical = json.dumps(rows, separators=(",", ":"), sort_keys=True).encode()
    _write_json_exclusive(
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


def _read_tsv(path: Path, label: str) -> list[list[str]]:
    _regular_file(path, label)
    with path.open(encoding="utf-8", newline="") as source:
        try:
            rows = list(csv.reader(source, delimiter="\t", strict=True))
        except csv.Error as error:
            raise GateError(f"{label} is malformed TSV: {error}") from error
    if any(any("\n" in field or "\r" in field for field in row) for row in rows):
        raise GateError(f"{label} contains embedded newlines")
    return rows


def _run_factors(run: str) -> tuple[str, str, bool, bool]:
    topology = "uniform" if run.startswith("uniform-") else "skew80-20"
    prefix = f"{topology}-"
    if not run.startswith(prefix) or not run.endswith("-01"):
        raise GateError(f"run name is outside the factorial matrix: {run}")
    cell = run[len(prefix) : -3]
    try:
        series_adaptive, last_adaptive = CELL_FACTORS[cell]
    except KeyError as error:
        raise GateError(f"run names an unknown factorial cell: {run}") from error
    return topology, cell, series_adaptive, last_adaptive


def validate_run_plan(result_dir: Path, plan: Path) -> dict[str, Any]:
    result_dir = result_dir.resolve(strict=True)
    rows = _read_tsv(plan, "run plan")
    expected_header = [
        "order",
        "run",
        "topology",
        "cell",
        "adaptive_series_table",
        "adaptive_last_timestamp_table",
        "capture",
        "config",
        "segments",
    ]
    if not rows or rows[0] != expected_header:
        raise GateError("run plan header differs")
    if len(rows) != len(EXPECTED_RUNS) + 1:
        raise GateError("run plan row count differs")
    for ordinal, (row, run) in enumerate(zip(rows[1:], EXPECTED_RUNS, strict=True), 1):
        topology, cell, series_adaptive, last_adaptive = _run_factors(run)
        expected = [
            str(ordinal),
            run,
            topology,
            cell,
            str(series_adaptive).lower(),
            str(last_adaptive).lower(),
            str(result_dir / "captures" / topology),
            str(result_dir / "configs" / f"{run}.toml"),
            str(result_dir / "runs" / run / "segments"),
        ]
        if row != expected:
            raise GateError(f"run plan row {ordinal} differs from the predeclared matrix")
    return {
        "schema": RUN_PLAN_SCHEMA,
        "runs": len(EXPECTED_RUNS),
        "plan_sha256": _sha256(plan),
        "validated": True,
    }


def _nonnegative_number(value: str, label: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise GateError(f"{label} must be numeric") from error
    if not math.isfinite(parsed) or parsed < 0:
        raise GateError(f"{label} must be finite and non-negative")
    return parsed


def validate_replay_summary(summary: Path) -> dict[str, Any]:
    rows = _read_tsv(summary, "replay summary")
    expected_header = [
        "run",
        "topology",
        "cell",
        "adaptive_series_table",
        "adaptive_last_timestamp_table",
        "elapsed",
        "user_seconds",
        "system_seconds",
        "max_rss_kib",
        "proc_peak_rss_kib",
        "corpus_files",
        "corpus_bytes",
        "manifest_sha256",
    ]
    if not rows or rows[0] != expected_header:
        raise GateError("replay summary header differs")
    if len(rows) != len(EXPECTED_RUNS) + 1:
        raise GateError("replay summary row count differs")
    for ordinal, (row, run) in enumerate(zip(rows[1:], EXPECTED_RUNS, strict=True), 1):
        if len(row) != len(expected_header):
            raise GateError(f"replay summary row {ordinal} has the wrong width")
        topology, cell, series_adaptive, last_adaptive = _run_factors(run)
        if row[:5] != [
            run,
            topology,
            cell,
            str(series_adaptive).lower(),
            str(last_adaptive).lower(),
        ]:
            raise GateError(f"replay summary row {ordinal} identity/order differs")
        if not row[5] or "\t" in row[5]:
            raise GateError(f"replay summary row {ordinal} elapsed value is empty")
        _nonnegative_number(row[6], f"replay summary row {ordinal} user_seconds")
        _nonnegative_number(row[7], f"replay summary row {ordinal} system_seconds")
        for column, label in zip(row[8:12], expected_header[8:12], strict=True):
            if not re.fullmatch(r"[1-9][0-9]*", column):
                raise GateError(f"replay summary row {ordinal} {label} must be positive")
        if not re.fullmatch(r"[0-9a-f]{64}", row[12]):
            raise GateError(f"replay summary row {ordinal} manifest digest is invalid")
    return {
        "schema": REPLAY_SUMMARY_SCHEMA,
        "runs": len(EXPECTED_RUNS),
        "summary_sha256": _sha256(summary),
        "validated": True,
    }


def _validate_runtime_identity_document(
    path: Path,
    role: str,
    expected_binary: Path,
    *,
    expected_environment: dict[str, str] | None = None,
    expected_arguments: list[str] | None = None,
) -> None:
    document = _read_json(path)
    expected_keys = {
        "schema",
        "role",
        "binary",
        "binary_sha256",
        "environment",
        "environment_sha256",
        "arguments",
        "arguments_sha256",
    }
    _require_exact_keys(document, expected_keys, f"runtime identity {path}")
    if document["schema"] != RUNTIME_IDENTITY_SCHEMA or document["role"] != role:
        raise GateError(f"runtime identity schema/role differs: {path}")
    expected_binary = expected_binary.resolve(strict=True)
    if document["binary"] != str(expected_binary):
        raise GateError(f"runtime identity names the wrong preserved binary: {path}")
    if document["binary_sha256"] != _sha256(expected_binary):
        raise GateError(f"runtime identity binary digest differs: {path}")
    environment = document["environment"]
    if not isinstance(environment, dict) or not all(
        isinstance(name, str) and isinstance(value, str)
        for name, value in environment.items()
    ):
        raise GateError(f"runtime identity environment is invalid: {path}")
    expected_names = {"LC_ALL", "TZ"}
    if role == "ingester":
        expected_names |= {"CONFIG_FILE", "RUST_LOG"}
    if set(environment) != expected_names:
        raise GateError(f"runtime identity environment names differ: {path}")
    environment_sha256 = hashlib.sha256(
        json.dumps(environment, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    if document["environment_sha256"] != environment_sha256:
        raise GateError(f"runtime identity environment digest differs: {path}")
    if expected_environment is not None and environment != expected_environment:
        raise GateError(f"runtime identity environment differs from its run plan: {path}")
    arguments = document["arguments"]
    if not isinstance(arguments, list) or not all(isinstance(value, str) for value in arguments):
        raise GateError(f"runtime identity arguments are invalid: {path}")
    arguments_sha256 = hashlib.sha256(
        json.dumps(arguments, separators=(",", ":")).encode()
    ).hexdigest()
    if document["arguments_sha256"] != arguments_sha256:
        raise GateError(f"runtime identity argument digest differs: {path}")
    if expected_arguments is not None and arguments != expected_arguments:
        raise GateError(f"runtime identity argv differs from its run plan: {path}")


def _formal_fixed_artifacts() -> set[str]:
    artifacts = {
        "run-plan.tsv",
        "replay-summary.tsv",
        "metadata/binaries.sha256",
        "metadata/binaries.tsv",
        "metadata/config-template.sha256",
        "metadata/config-template.toml",
        "metadata/configs.sha256",
        "metadata/disk-budget.tsv",
        "metadata/environment.txt",
        "metadata/harness.sha256",
        "metadata/perf-preflight.json",
        "metadata/perf-preflight.tsv",
        "metadata/processes-before-transforms.json",
        "metadata/processes-at-plan.txt",
        "metadata/processes-before-final-seal.txt",
        "metadata/replay-summary.sha256",
        "metadata/run-note.txt",
        "metadata/run-plan.sha256",
        "metadata/seal-checks.tsv",
        "metadata/settings.txt",
        "metadata/build/build-command.txt",
        "metadata/build/build-contract.txt",
        "metadata/build/build-environment.tsv",
        "metadata/build/build.exit-status",
        "metadata/build/build.log",
        "metadata/build/cargo-metadata.json",
        "metadata/build/cargo-version.txt",
        "metadata/build/cc-version.txt",
        "metadata/build/rustc-version.txt",
        "metadata/build/rustup-active-toolchain.txt",
        "metadata/build/source-archive-check-after-build.json",
        "metadata/build/source-archive-check-before-build.json",
        "metadata/build/source-archive-check-final.json",
        "metadata/build/source-check-after-build.json",
        "metadata/build/source-check-before-build.json",
        "metadata/build/source-check-final.json",
        "metadata/build/source-snapshot-check-after-build.json",
        "metadata/build/source-snapshot-check-before-build.json",
        "metadata/build/source-snapshot-check-final.json",
        "metadata/build/tool-binaries.sha256",
        "metadata/build/tool-paths.tsv",
        "metadata/source/formal-source-seal.json",
        "metadata/source/git-revision.txt",
        "metadata/source/git-status.txt",
        "metadata/source/git-tree.txt",
        "metadata/source/source-archive-seal.json",
        "metadata/source/source-head.tar",
        "metadata/source/source-snapshot-seal.json",
        "metadata/source/tracked-files.sha256",
        "metadata/source/tracked-index.txt",
        "metadata/source/tracked.patch",
        "metadata/tools/fadvise-regular-dontneed",
        "metadata/tools/fadvise-regular-dontneed.sha256",
        "metadata/tools/runtime-tool-binaries.sha256",
        "metadata/tools/runtime-tool-paths.tsv",
        "comparisons/head-structure-matrix.json",
        "comparisons/performance-decision.json",
        "comparisons/repartition-matrix.json",
        "comparisons/repartition-prefix-matrix.json",
        "comparisons/repartition-skew80-20-repeat.json",
        "comparisons/repartition-uniform-repeat.json",
        "comparisons/replay-summary-validation.json",
        "comparisons/run-plan-validation.json",
        "comparisons/seed-dynamic-disk-budget.tsv",
        "comparisons/storage-validation.json",
        "comparisons/topology-sizing.tsv",
        "validation/determinism-prefix-sizes.tsv",
        "validation/full-capture-sizes.tsv",
        "validation/generated-capture-writeback.tsv",
        "validation/transform-capacity-plan.tsv",
    }
    for name in (
        "phase5_head_topology_run.sh",
        "phase5_head_topology_gate.py",
        "phase5_head_topology_guard.py",
        "test_phase5_head_topology_gate.py",
        "test_phase5_head_topology_guard.py",
        "phase1_replay_gate.py",
        "phase1_4m_expectations.json",
        "ab_gate.py",
        "fadvise_regular_dontneed.c",
    ):
        artifacts.add(f"metadata/harness/{name}")
    for binary in (
        "chronoxide-ingester",
        "chronoxide-capture-repartition",
        "chronoxide-query",
        "chronoxide-storage-verify",
    ):
        artifacts.add(f"metadata/binaries/{binary}")
    for index, run in enumerate(EXPECTED_RUNS, 1):
        del index
        artifacts.add(f"configs/{run}.toml")
        run_root = f"runs/{run}"
        for name in (
            "capture-residency-before.tsv",
            "capture-writeback-before.tsv",
            "config-render.json",
            "corpus-summary.json",
            "disk-budget-before.tsv",
            "disk-guardian.tsv",
            "guardian.exit-status",
            "guardian.log",
            "guardian-ready",
            "guardian-summary.json",
            "head-structure.json",
            "perf-stat.json",
            "perf-stat.tsv",
            "pressure-after.txt",
            "pressure-before.txt",
            "process-guardian.tsv",
            "processes-immediately-before-launch.json",
            "processes-after.txt",
            "processes-before.txt",
            "replay-correctness.json",
            "replay.exit-status",
            "replay.log",
            "replay.time.json",
            "replay.time.txt",
            "rss-monitor.exit-status",
            "rss-monitor.log",
            "rss-ready",
            "rss-samples.tsv",
            "rss-summary.json",
            "launch",
            "lifecycle-control.json",
            "runtime-identity.json",
            "segments.sha256",
            "segments.tsv",
        ):
            artifacts.add(f"{run_root}/{name}")
    for topology in TOPOLOGIES:
        artifacts.add(f"configs/sizing-{topology}.toml")
        sizing_root = f"sizing/{topology}"
        for name in (
            "config-render.json",
            "corpus-summary.json",
            "disk-budget-before.tsv",
            "disk-guardian.tsv",
            "guardian.exit-status",
            "guardian.log",
            "guardian-ready",
            "guardian-summary.json",
            "head-structure.json",
            "performance-disabled.json",
            "process-guardian.tsv",
            "processes-immediately-before-launch.json",
            "processes-after.txt",
            "processes-before.txt",
            "replay-correctness.json",
            "replay.exit-status",
            "replay.log",
            "rss-monitor.exit-status",
            "rss-monitor.log",
            "rss-ready",
            "rss-samples.tsv",
            "rss-summary.json",
            "launch",
            "lifecycle-control.json",
            "runtime-identity.json",
            "segments.sha256",
            "segments.tsv",
        ):
            artifacts.add(f"{sizing_root}/{name}")
        artifacts.update(
            {
                f"validation/repartition-{topology}.json",
                f"validation/repartition-{topology}.runtime-identity.json",
                f"validation/repartition-{topology}.stdout.json",
                f"validation/{topology}/readbacks.json",
                f"validation/{topology}/readbacks.log",
                f"validation/{topology}/readbacks.md",
                f"validation/{topology}/readbacks.runtime-identity.json",
                f"validation/{topology}/storage-verify.json",
                f"validation/{topology}/storage-verify.log",
                f"validation/{topology}/storage-verify.runtime-identity.json",
            }
        )
        for variant in ("a", "b"):
            artifacts.update(
                {
                    f"validation/repartition-prefix-{topology}-{variant}.json",
                    f"validation/repartition-prefix-{topology}-{variant}.runtime-identity.json",
                    f"validation/repartition-prefix-{topology}-{variant}.stdout.json",
                }
            )
    lifecycle_files = {
        "disk-guardian.tsv",
        "guardian.exit-status",
        "guardian.log",
        "guardian-ready",
        "guardian-summary.json",
        "launch",
        "lifecycle-control.json",
        "process-guardian.tsv",
        "processes-immediately-before-launch.json",
        "rss-monitor.exit-status",
        "rss-monitor.log",
        "rss-ready",
        "rss-samples.tsv",
        "rss-summary.json",
        "workload.exit-status",
    }
    for label in TRANSFORM_LABELS:
        artifacts.update(
            f"validation/transform-guards/{label}/{name}"
            for name in lifecycle_files
        )
    for label in ("source-before", "source-after-transforms", "source-capture-after-runs"):
        artifacts.add(f"inventory/{label}.json")
        artifacts.add(f"inventory/{label}-files.nul")
    for label in CAPTURE_LABELS:
        for suffix in ("before-runs", "capture-after-runs"):
            artifacts.add(f"inventory/{label}-{suffix}.json")
            artifacts.add(f"inventory/{label}-{suffix}-files.nul")
    return artifacts


def _safe_result_relative(relative: str) -> None:
    candidate = PurePosixPath(relative)
    if (
        not relative
        or relative.startswith("/")
        or candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in candidate.parts)
        or candidate.as_posix() != relative
        or "\0" in relative
        or "\n" in relative
        or "\r" in relative
        or "\t" in relative
    ):
        raise GateError(f"final artifact seal contains an unsafe path: {relative!r}")


def _read_final_manifest(result_dir: Path) -> tuple[Path, dict[str, str]]:
    manifest = result_dir / "metadata" / "result-artifacts.sha256"
    _regular_file(manifest, "final artifact seal")
    sealed: dict[str, str] = {}
    line_pattern = re.compile(r"^([0-9a-f]{64})  (.+)$")
    for line in manifest.read_text(encoding="utf-8").splitlines():
        match = line_pattern.fullmatch(line)
        if match is None:
            raise GateError("final artifact seal contains a malformed line")
        digest, relative = match.groups()
        _safe_result_relative(relative)
        if relative in sealed:
            raise GateError(f"final artifact seal contains a duplicate path: {relative}")
        if relative not in {"run-plan.tsv", "replay-summary.tsv"} and relative.split("/", 1)[0] not in {
            "metadata",
            "configs",
            "validation",
            "comparisons",
            "inventory",
            "sizing",
            "runs",
        }:
            raise GateError(f"final artifact seal contains a forbidden root: {relative}")
        sealed[relative] = digest
    if not sealed:
        raise GateError("final artifact seal is empty")
    for relative, digest in sealed.items():
        path = result_dir / relative
        _regular_file(path, f"sealed artifact {relative}")
        if _sha256(path) != digest:
            raise GateError(f"final artifact seal digest mismatch: {relative}")
    return manifest, sealed


def _dynamic_formal_artifacts(result_dir: Path) -> set[str]:
    artifacts: set[str] = set()
    ingestion_pattern = re.compile(r"ingestion_stats_[0-9]{8}_[0-9]{6}[.]md")
    for root_relative in [*(f"runs/{run}" for run in EXPECTED_RUNS), *(f"sizing/{topology}" for topology in TOPOLOGIES)]:
        root = result_dir / root_relative
        reports = [
            path
            for path in root.iterdir()
            if path.is_file() and not path.is_symlink() and ingestion_pattern.fullmatch(path.name)
        ]
        if len(reports) != 1:
            raise GateError(f"{root_relative} must contain exactly one canonical ingestion report")
        artifacts.add(f"{root_relative}/{reports[0].name}")
        manifest_rows = _read_tsv(root / "segments.tsv", f"{root_relative} segment inventory")
        if not manifest_rows or manifest_rows[0] != ["sha256", "size_bytes", "path"]:
            raise GateError(f"{root_relative} segment inventory header differs")
        if len(manifest_rows) == 1:
            raise GateError(f"{root_relative} segment inventory is empty")
        seen: set[str] = set()
        for row in manifest_rows[1:]:
            if len(row) != 3 or not re.fullmatch(r"[0-9a-f]{64}", row[0]):
                raise GateError(f"{root_relative} segment inventory row is malformed")
            relative = row[2]
            _safe_result_relative(relative)
            if relative in seen or not re.fullmatch(r"0|[1-9][0-9]*", row[1]):
                raise GateError(f"{root_relative} segment inventory row is duplicate/invalid")
            seen.add(relative)
            artifacts.add(f"{root_relative}/segments/{relative}")
    return artifacts


def _validate_result_roots(result_dir: Path, stage: str, performance_marker: str) -> None:
    expected_directories = {
        "build-source",
        "build-state",
        "build-target",
        "captures",
        "comparisons",
        "configs",
        "inventory",
        "metadata",
        "runs",
        "sizing",
        "validation",
    }
    expected_files = {
        "run-plan.tsv",
        "replay-summary.tsv",
        "SIZING_GATE_PASSED",
        "SEED_CAPACITY_GATE_PASSED",
        performance_marker,
    }
    if stage == "complete":
        expected_files.update({"FINAL_SEAL_VALIDATED", "COMPLETE"})
    observed_directories: set[str] = set()
    observed_files: set[str] = set()
    for path in result_dir.iterdir():
        if path.is_symlink():
            raise GateError(f"formal result root contains a symlink: {path.name}")
        if path.is_dir():
            observed_directories.add(path.name)
        elif path.is_file():
            observed_files.add(path.name)
        else:
            raise GateError(f"formal result root contains a non-regular entry: {path.name}")
    if observed_directories != expected_directories or observed_files != expected_files:
        raise GateError(
            "formal result root allowlist differs: "
            f"missing_dirs={sorted(expected_directories - observed_directories)} "
            f"extra_dirs={sorted(observed_directories - expected_directories)} "
            f"missing_files={sorted(expected_files - observed_files)} "
            f"extra_files={sorted(observed_files - expected_files)}"
        )
    for marker in ("SIZING_GATE_PASSED", "SEED_CAPACITY_GATE_PASSED", performance_marker):
        path = result_dir / marker
        _regular_file(path, marker)
        if path.stat().st_size != 0:
            raise GateError(f"formal stage marker must be empty: {marker}")


def _validate_sealed_directory_matrix(
    result_dir: Path, expected_artifacts: set[str]
) -> None:
    sealed_roots = ("metadata", "configs", "validation", "comparisons", "inventory", "sizing", "runs")
    expected_directories = set(sealed_roots)
    for relative in expected_artifacts:
        path = PurePosixPath(relative)
        for parent in path.parents:
            if parent.as_posix() == ".":
                continue
            if parent.parts[0] in sealed_roots:
                expected_directories.add(parent.as_posix())
    observed_directories: set[str] = set()
    for root_name in sealed_roots:
        root = result_dir / root_name
        if not root.is_dir() or root.is_symlink():
            raise GateError(f"sealed result root is missing or not a real directory: {root_name}")
        observed_directories.add(root_name)
        for walk_root, directories, _files in os.walk(
            root, followlinks=False, onerror=_raise_walk_error
        ):
            walk_path = Path(walk_root)
            for name in directories:
                path = walk_path / name
                if path.is_symlink():
                    raise GateError(f"sealed result contains a symlink directory: {path}")
                observed_directories.add(path.relative_to(result_dir).as_posix())
    if observed_directories != expected_directories:
        raise GateError(
            "sealed directory matrix differs: "
            f"missing={sorted(expected_directories - observed_directories)[:3]} "
            f"extra={sorted(observed_directories - expected_directories)[:3]}"
        )


def _load_support_module(filename: str) -> Any:
    if filename not in FROZEN_HARNESS_FILES:
        raise GateError(f"support gate is outside the frozen harness allowlist: {filename}")
    path = Path(__file__).resolve().with_name(filename)
    _regular_file(path, f"support gate {filename}")
    module_name = f"chronoxide_phase5_support_{path.stem}_{hashlib.sha256(str(path).encode()).hexdigest()[:12]}"
    module = types.ModuleType(module_name)
    module.__file__ = str(path)
    module.__package__ = ""
    try:
        # Compile the sealed source bytes directly. `-B` prevents cache writes,
        # but CPython may still read an existing `.pyc`; bypassing importlib's
        # cache loader prevents an unsealed sibling cache from becoming an
        # executable authority even under a concurrent or post-hoc injection.
        code = compile(path.read_bytes(), str(path), "exec", dont_inherit=True)
        exec(code, module.__dict__)
    except Exception as error:
        raise GateError(f"support gate failed to load: {path}: {error}") from error
    return module


def _require_json_equal(path: Path, expected: dict[str, Any], label: str) -> None:
    actual = _read_json(path)
    if actual != expected:
        raise GateError(f"{label} differs from a fresh post-hoc gate result")


def _read_settings(path: Path) -> dict[str, str]:
    _regular_file(path, "formal settings")
    settings: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            raise GateError("formal settings contain a malformed line")
        name, value = line.split("=", 1)
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name) or name in settings:
            raise GateError(f"formal settings contain an invalid/duplicate name: {name!r}")
        settings[name] = value
    return settings


def _read_sha256_manifest(path: Path, label: str) -> dict[str, str]:
    _regular_file(path, label)
    result: dict[str, str] = {}
    pattern = re.compile(r"^([0-9a-f]{64})  (.+)$")
    for line in path.read_text(encoding="utf-8").splitlines():
        match = pattern.fullmatch(line)
        if match is None:
            raise GateError(f"{label} contains a malformed line")
        digest, name = match.groups()
        if name in result:
            raise GateError(f"{label} contains a duplicate path")
        result[name] = digest
    if not result:
        raise GateError(f"{label} is empty")
    return result


def _validate_named_sha256_manifest(
    manifest: Path, expected_paths: list[Path], label: str
) -> None:
    rows = _read_sha256_manifest(manifest, label)
    expected = {str(path): _sha256(path) for path in expected_paths}
    if rows != expected:
        raise GateError(f"{label} differs from its named files")


def _zero_exit_status(path: Path, label: str) -> None:
    _regular_file(path, label)
    if path.read_text(encoding="ascii") != "0\n":
        raise GateError(f"{label} is not an exact successful exit status")


def _validate_rendered_config(
    path: Path,
    *,
    capture: Path,
    segments: Path,
    messages: int,
    series_adaptive: bool,
    last_adaptive: bool,
) -> dict[str, Any]:
    with path.open("rb") as source:
        document = tomllib.load(source)
    ingestion = document.get("ingestion")
    if not isinstance(ingestion, dict):
        raise GateError(f"rendered config lacks ingestion table: {path}")
    head = ingestion.get("head_buffer")
    writer = ingestion.get("segment_writer")
    if not isinstance(head, dict) or not isinstance(writer, dict):
        raise GateError(f"rendered config lacks head/writer table: {path}")
    if (
        ingestion.get("replay_from") != str(capture)
        or ingestion.get("stop_after_messages") != messages
        or writer.get("segments_dir") != str(segments)
        or head.get("adaptive_series_table") is not series_adaptive
        or head.get("adaptive_last_timestamp_table") is not last_adaptive
    ):
        raise GateError(f"rendered config differs from its run plan: {path}")
    return {
        "config": str(path.resolve(strict=True)),
        "capture": str(capture),
        "segments_dir": str(segments),
        "messages": messages,
        "adaptive_series_table": series_adaptive,
        "adaptive_last_timestamp_table": last_adaptive,
        "sha256": _sha256(path),
    }


def _validate_corpus_evidence(
    result_dir: Path, root_relative: str, sealed: dict[str, str]
) -> dict[str, Any]:
    root = result_dir / root_relative
    rows = _read_tsv(root / "segments.tsv", f"{root_relative} segment inventory")
    if not rows or rows[0] != ["sha256", "size_bytes", "path"] or len(rows) == 1:
        raise GateError(f"{root_relative} segment inventory differs")
    inventory: list[tuple[str, int, str]] = []
    seen: set[str] = set()
    for row in rows[1:]:
        if (
            len(row) != 3
            or not re.fullmatch(r"[0-9a-f]{64}", row[0])
            or not re.fullmatch(r"0|[1-9][0-9]*", row[1])
        ):
            raise GateError(f"{root_relative} segment inventory row is malformed")
        _safe_result_relative(row[2])
        if row[2] in seen:
            raise GateError(f"{root_relative} segment inventory contains a duplicate path")
        seen.add(row[2])
        inventory.append((row[0], int(row[1]), row[2]))
    expected_manifest = "".join(
        f"{digest}  ./{relative}\n" for digest, _size, relative in inventory
    ).encode()
    if (root / "segments.sha256").read_bytes() != expected_manifest:
        raise GateError(f"{root_relative} segment byte manifest differs from its inventory")
    segment_root = root / "segments"
    if not segment_root.is_dir() or segment_root.is_symlink():
        raise GateError(f"{root_relative} segment root is missing")
    observed: set[str] = set()
    for walk_root, directories, files in os.walk(
        segment_root, followlinks=False, onerror=_raise_walk_error
    ):
        walk_path = Path(walk_root)
        for name in directories:
            if (walk_path / name).is_symlink():
                raise GateError(f"{root_relative} segment tree contains a symlink directory")
        for name in files:
            path = walk_path / name
            relative = path.relative_to(segment_root).as_posix()
            _regular_file(path, f"{root_relative} segment {relative}")
            observed.add(relative)
    if observed != seen:
        raise GateError(f"{root_relative} segment tree path set differs from its inventory")
    for digest, size, relative in inventory:
        path = segment_root / relative
        if path.stat().st_size != size:
            raise GateError(f"{root_relative} segment size differs: {relative}")
        sealed_relative = f"{root_relative}/segments/{relative}"
        if sealed.get(sealed_relative) != digest:
            raise GateError(f"{root_relative} segment seal differs from its corpus manifest")
    summary = _read_json(root / "corpus-summary.json")
    expected_summary = {
        "schema": "chronoxide/storage-vnext-phase1-corpus/v1",
        "file_count": len(inventory),
        "size_bytes": sum(size for _digest, size, _relative in inventory),
        "manifest_sha256": hashlib.sha256(expected_manifest).hexdigest(),
    }
    if summary != expected_summary:
        raise GateError(f"{root_relative} corpus summary differs from sealed raw files")
    return summary


def _validate_rss_summary(samples_path: Path, summary_path: Path) -> dict[str, Any]:
    root = samples_path.parent
    control = _validate_lifecycle_control(root, "RSS lifecycle")
    rows = _read_tsv(samples_path, "RSS samples")
    header = [
        "poll_index",
        "monotonic_elapsed_ns",
        "recorded_at",
        "root_running",
        "launch_observed",
        "process_count",
        "rss_kib",
        "rss_anon_kib",
        "rss_file_kib",
        "vm_swap_kib",
        "max_single_hwm_kib",
        "pids",
    ]
    if not rows or rows[0] != header or len(rows) < 3:
        raise GateError("RSS sample table lacks two samples including a terminal edge")
    maxima = {
        "process_count": 0,
        "aggregate_rss_kib": 0,
        "aggregate_rss_anon_kib": 0,
        "aggregate_rss_file_kib": 0,
        "aggregate_vm_swap_kib": 0,
        "max_single_process_hwm_kib": 0,
    }
    mapping = {
        "process_count": 5,
        "aggregate_rss_kib": 6,
        "aggregate_rss_anon_kib": 7,
        "aggregate_rss_file_kib": 8,
        "aggregate_vm_swap_kib": 9,
        "max_single_process_hwm_kib": 10,
    }
    parsed_rows: list[list[str]] = []
    timestamps: list[int] = []
    root_states: list[bool] = []
    launch_states: list[bool] = []
    for ordinal, row in enumerate(rows[1:], 1):
        if len(row) != len(header) or any(
            not re.fullmatch(r"0|[1-9][0-9]*", row[index])
            for index in (0, 1, 5, 6, 7, 8, 9, 10)
        ):
            raise GateError(f"RSS sample row {ordinal} is malformed")
        if row[0] != str(ordinal) or not row[2] or row[3] not in {
            "true",
            "false",
        } or row[4] not in {"true", "false"}:
            raise GateError(f"RSS sample row {ordinal} lifecycle identity is malformed")
        timestamps.append(int(row[1]))
        root_states.append(row[3] == "true")
        launch_states.append(row[4] == "true")
        pids = row[11].split(",") if row[11] else []
        if any(not re.fullmatch(r"[1-9][0-9]*", pid) for pid in pids):
            raise GateError(f"RSS sample row {ordinal} PID list is malformed")
        if len(pids) != int(row[5]):
            raise GateError(f"RSS sample row {ordinal} process count differs")
        if row[3] == "true" and str(control["root_pid"]) not in pids:
            raise GateError(f"RSS sample row {ordinal} omits the identity-bound root")
        if row[3] == "false" and (pids or any(int(row[index]) for index in range(5, 11))):
            raise GateError(f"RSS terminal sample is not empty")
        for name, index in mapping.items():
            maxima[name] = max(maxima[name], int(row[index]))
        parsed_rows.append(row)
    if (
        not all(root_states[:-1])
        or root_states[-1]
        or launch_states[0]
        or not any(launch_states[1:])
        or launch_states != sorted(launch_states)
    ):
        raise GateError("RSS held-launch or terminal-edge sequence differs")
    summary = _read_json(summary_path)
    required = {
        "schema",
        "root_pid",
        "root_starttime_ticks",
        "rss_monitor_pid",
        "samples",
        "interval_ms",
        *maxima,
        "terminal_elapsed_ns",
        "poll_monotonic_elapsed_ns",
        "maximum_poll_start_gap_ns",
        "maximum_allowed_poll_start_gap_ns",
        "control_path",
        "control_sha256",
        "guardian_ready_marker",
        "rss_ready_marker",
        "launch_marker",
        "ready_created_poll",
        "ready_created_monotonic_elapsed_ns",
        "launch_observed_poll",
        "launch_observed_monotonic_elapsed_ns",
        "root_seen",
        "handshake_violations",
        "termination",
        "complete",
    }
    _require_exact_keys(summary, required, "RSS summary")
    terminal = _strict_int(summary["terminal_elapsed_ns"], "RSS terminal elapsed")
    derived_gap = _lifecycle_maximum_gap_ns(timestamps, terminal)
    first_launch_poll = launch_states.index(True) + 1
    if (
        summary["schema"] != LIFECYCLE_RSS_SCHEMA
        or summary["samples"] != len(parsed_rows)
        or summary["root_pid"] != control["root_pid"]
        or summary["root_starttime_ticks"] != control["root_starttime_ticks"]
        or summary["rss_monitor_pid"] != control["rss_monitor_pid"]
        or summary["interval_ms"] != LIFECYCLE_CADENCE_INTERVAL_MS
        or summary["poll_monotonic_elapsed_ns"] != timestamps
        or summary["maximum_poll_start_gap_ns"] != derived_gap
        or summary["maximum_allowed_poll_start_gap_ns"]
        != _lifecycle_maximum_allowed_gap_ns(LIFECYCLE_CADENCE_INTERVAL_MS)
        or derived_gap > summary["maximum_allowed_poll_start_gap_ns"]
        or summary["control_path"] != str(root / "lifecycle-control.json")
        or summary["control_sha256"] != _sha256(root / "lifecycle-control.json")
        or summary["guardian_ready_marker"] != str(root / "guardian-ready")
        or summary["rss_ready_marker"] != str(root / "rss-ready")
        or summary["launch_marker"] != str(root / "launch")
        or summary["ready_created_poll"] != 1
        or summary["ready_created_monotonic_elapsed_ns"] != timestamps[0]
        or summary["launch_observed_poll"] != first_launch_poll
        or summary["launch_observed_monotonic_elapsed_ns"]
        != timestamps[first_launch_poll - 1]
        or summary["root_seen"] is not True
        or summary["handshake_violations"] != []
        or summary["termination"]
        != _empty_lifecycle_termination(control["root_starttime_ticks"])
        or summary["complete"] is not True
    ):
        raise GateError("RSS summary is not reconstructed by raw lifecycle evidence")
    for name, expected in maxima.items():
        if _strict_int(summary[name], f"RSS summary {name}") != expected:
            raise GateError(f"RSS summary maximum differs from raw samples: {name}")
    if (root / "rss-monitor.log").stat().st_size != 0:
        raise GateError("successful RSS monitor log is not empty")
    return summary


def _validate_capture_inventory_document(path: Path, paths_path: Path) -> dict[str, Any]:
    document = _read_json(path)
    _require_exact_keys(
        document,
        {"schema", "capture", "file_count", "total_bytes", "files_sha256", "files"},
        f"capture inventory {path}",
    )
    if document["schema"] != CAPTURE_INVENTORY_SCHEMA:
        raise GateError(f"capture inventory schema differs: {path}")
    capture = document["capture"]
    files = document["files"]
    if not isinstance(capture, str) or not Path(capture).is_absolute() or not isinstance(files, list):
        raise GateError(f"capture inventory identity differs: {path}")
    canonical_rows: list[dict[str, Any]] = []
    previous: bytes | None = None
    path_bytes = bytearray()
    for ordinal, row in enumerate(files, 1):
        if not isinstance(row, dict):
            raise GateError(f"capture inventory row {ordinal} is not an object")
        _require_exact_keys(row, {"path", "size_bytes", "sha256"}, f"capture row {ordinal}")
        relative = row["path"]
        if not isinstance(relative, str):
            raise GateError(f"capture inventory row {ordinal} path is invalid")
        _safe_result_relative(relative)
        encoded = relative.encode()
        if previous is not None and encoded <= previous:
            raise GateError(f"capture inventory rows are not uniquely byte-sorted: {path}")
        previous = encoded
        size = _strict_int(row["size_bytes"], f"capture inventory row {ordinal} size")
        digest = row["sha256"]
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise GateError(f"capture inventory row {ordinal} digest is invalid")
        canonical_rows.append({"path": relative, "size_bytes": size, "sha256": digest})
        path_bytes.extend(os.fsencode(Path(capture) / relative))
        path_bytes.append(0)
    canonical = json.dumps(canonical_rows, separators=(",", ":"), sort_keys=True).encode()
    if (
        not files
        or document["file_count"] != len(files)
        or document["total_bytes"] != sum(row["size_bytes"] for row in canonical_rows)
        or document["files_sha256"] != hashlib.sha256(canonical).hexdigest()
        or paths_path.read_bytes() != bytes(path_bytes)
    ):
        raise GateError(f"capture inventory counters/path list differ: {path}")
    return document


def _validate_archive_document(archive: Path, document: dict[str, Any]) -> None:
    _require_exact_keys(
        document,
        {
            "schema",
            "repo",
            "archive",
            "git_head",
            "git_tree",
            "object_format",
            "archive_sha256",
            "archive_size_bytes",
            "file_count",
            "files",
            "identity_sha256",
        },
        "source archive seal",
    )
    if (
        document["schema"] != SOURCE_ARCHIVE_SEAL_SCHEMA
        or document["archive"] != str(archive.resolve(strict=True))
        or document["archive_sha256"] != _sha256(archive)
        or document["archive_size_bytes"] != archive.stat().st_size
        or document["object_format"] not in {"sha1", "sha256"}
    ):
        raise GateError("source archive seal identity differs")
    files = document["files"]
    if not isinstance(files, list) or document["file_count"] != len(files) or not files:
        raise GateError("source archive seal file count differs")
    expected: dict[str, dict[str, Any]] = {}
    for row in files:
        if not isinstance(row, dict):
            raise GateError("source archive seal file row is not an object")
        _require_exact_keys(row, {"path", "mode", "object_id", "size_bytes"}, "archive file")
        relative = row["path"]
        if not isinstance(relative, str):
            raise GateError("source archive seal path is invalid")
        _safe_result_relative(relative)
        if (
            relative in expected
            or row["mode"] not in {"100644", "100755"}
            or not isinstance(row["object_id"], str)
            or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", row["object_id"])
        ):
            raise GateError(f"source archive seal file row differs: {relative}")
        _strict_int(row["size_bytes"], f"source archive size {relative}")
        expected[relative] = row
    if list(expected) != sorted(expected):
        raise GateError("source archive seal files are not canonically sorted")
    expected_directories = {
        parent.as_posix()
        for relative in expected
        for parent in PurePosixPath(relative).parents
        if parent.as_posix() != "."
    }
    observed_files: set[str] = set()
    observed_directories: set[str] = set()
    with tarfile.open(archive, mode="r:") as source:
        for member in source.getmembers():
            relative = _safe_archive_name(member.name)
            if member.mode & 0o7000:
                raise GateError(f"source archive member has privileged mode bits: {relative}")
            if member.isdir():
                if relative in observed_directories or relative in observed_files:
                    raise GateError(f"source archive contains a duplicate path: {relative}")
                observed_directories.add(relative)
                continue
            if not member.isfile() or member.islnk() or member.issym() or relative in observed_files:
                raise GateError(f"source archive contains a duplicate/non-regular path: {relative}")
            row = expected.get(relative)
            if row is None or member.size != row["size_bytes"]:
                raise GateError(f"source archive member differs from its seal: {relative}")
            mode = "100755" if member.mode & 0o111 else "100644"
            if mode != row["mode"]:
                raise GateError(f"source archive mode differs from its seal: {relative}")
            source_file = source.extractfile(member)
            if source_file is None:
                raise GateError(f"source archive member is unreadable: {relative}")
            with source_file:
                object_id = _git_blob_oid_from_stream(
                    source_file, member.size, document["object_format"]
                )
            if object_id != row["object_id"]:
                raise GateError(f"source archive bytes differ from its seal: {relative}")
            observed_files.add(relative)
    if observed_files != set(expected) or observed_directories != expected_directories:
        raise GateError("source archive path graph differs from its seal")
    identity = {
        key: document[key]
        for key in (
            "git_head",
            "git_tree",
            "object_format",
            "archive_sha256",
            "archive_size_bytes",
            "file_count",
            "files",
        )
    }
    digest = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    if document["identity_sha256"] != digest:
        raise GateError("source archive identity digest differs")


def _validate_snapshot_document(snapshot: Path, document: dict[str, Any]) -> None:
    _require_exact_keys(
        document,
        {
            "schema",
            "repo",
            "snapshot",
            "git_head",
            "git_tree",
            "object_format",
            "file_count",
            "files",
            "identity_sha256",
        },
        "source snapshot seal",
    )
    if (
        document["schema"] != SOURCE_SNAPSHOT_SEAL_SCHEMA
        or document["snapshot"] != str(snapshot.resolve(strict=True))
        or not snapshot.is_dir()
        or snapshot.is_symlink()
        or stat.S_IMODE(snapshot.stat().st_mode) != 0o555
    ):
        raise GateError("source snapshot seal identity differs")
    files = document["files"]
    if not isinstance(files, list) or document["file_count"] != len(files) or not files:
        raise GateError("source snapshot seal file count differs")
    expected = {row.get("path"): row for row in files if isinstance(row, dict)}
    if len(expected) != len(files) or None in expected or list(expected) != sorted(expected):
        raise GateError("source snapshot seal paths are invalid/duplicate/unsorted")
    observed: dict[str, Path] = {}
    for path in snapshot.rglob("*"):
        relative = path.relative_to(snapshot).as_posix()
        if path.is_symlink():
            raise GateError(f"source snapshot contains a symlink: {relative}")
        if path.is_dir():
            if stat.S_IMODE(path.stat().st_mode) != 0o555:
                raise GateError(f"source snapshot directory mode differs: {relative}")
            continue
        if not path.is_file():
            raise GateError(f"source snapshot contains a non-regular entry: {relative}")
        observed[relative] = path
    if set(observed) != set(expected):
        raise GateError("source snapshot path set differs from its seal")
    for relative, path in observed.items():
        row = expected[relative]
        _require_exact_keys(row, {"path", "mode", "object_id", "size_bytes"}, "snapshot file")
        required_mode = 0o555 if row["mode"] == "100755" else 0o444
        if (
            row["mode"] not in {"100644", "100755"}
            or stat.S_IMODE(path.stat().st_mode) != required_mode
            or path.stat().st_size != row["size_bytes"]
            or _git_blob_oid(path, document["object_format"]) != row["object_id"]
        ):
            raise GateError(f"source snapshot file differs from its seal: {relative}")
    identity = {
        key: document[key]
        for key in ("git_head", "git_tree", "object_format", "file_count", "files")
    }
    digest = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    if document["identity_sha256"] != digest:
        raise GateError("source snapshot identity digest differs")


def _validate_provenance_evidence(result_dir: Path) -> None:
    source_root = result_dir / "metadata" / "source"
    build_root = result_dir / "metadata" / "build"
    source_seal_path = source_root / "formal-source-seal.json"
    archive_path = source_root / "source-head.tar"
    archive_seal_path = source_root / "source-archive-seal.json"
    snapshot_path = result_dir / "build-source"
    snapshot_seal_path = source_root / "source-snapshot-seal.json"
    source_document = _read_json(source_seal_path)
    repo_value = source_document.get("repo")
    if not isinstance(repo_value, str) or not Path(repo_value).is_absolute():
        raise GateError("formal source seal names an invalid repository")
    if source_document.get("schema") != SOURCE_SEAL_SCHEMA:
        raise GateError("formal source seal schema differs")
    source_identity = {
        key: source_document[key]
        for key in (
            "head",
            "tree",
            "tracked_index_sha256",
            "tracked_worktree_sha256",
            "tracked_file_count",
            "cargo_lock_sha256",
            "cargo_configs",
        )
    }
    source_identity_sha256 = hashlib.sha256(
        json.dumps(source_identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    if source_document.get("identity_sha256") != source_identity_sha256:
        raise GateError("formal source identity digest differs")
    archive_document = _read_json(archive_seal_path)
    snapshot_document = _read_json(snapshot_seal_path)
    _validate_archive_document(archive_path, archive_document)
    _validate_snapshot_document(snapshot_path, snapshot_document)
    source_check = {"status": "pass", "identity_sha256": source_identity_sha256}
    archive_check = {
        "status": "pass",
        "identity_sha256": archive_document["identity_sha256"],
    }
    snapshot_check = {
        "status": "pass",
        "identity_sha256": snapshot_document["identity_sha256"],
    }
    for phase in ("before-build", "after-build", "final"):
        _require_json_equal(
            build_root / f"source-check-{phase}.json",
            source_check,
            f"live source check {phase}",
        )
        _require_json_equal(
            build_root / f"source-archive-check-{phase}.json",
            archive_check,
            f"source archive check {phase}",
        )
        _require_json_equal(
            build_root / f"source-snapshot-check-{phase}.json",
            snapshot_check,
            f"source snapshot check {phase}",
        )
    for key in ("git_head", "git_tree", "object_format", "file_count", "files"):
        if archive_document.get(key) != snapshot_document.get(key):
            raise GateError(f"source archive/snapshot identities differ at {key}")
    if source_document.get("head") != archive_document.get("git_head") or source_document.get(
        "tree"
    ) != archive_document.get("git_tree"):
        raise GateError("live source, archive, and snapshot revisions differ")
    harness_root = result_dir / "metadata" / "harness"
    check_frozen_harness(harness_root)
    expected_harness_manifest = bytearray()
    for name in FROZEN_HARNESS_FILES:
        frozen = harness_root / name
        snapshot_source = (
            snapshot_path / "docs" / "experiments" / "storage_vnext" / name
        )
        _regular_file(frozen, f"frozen harness {name}")
        _regular_file(snapshot_source, f"snapshot harness {name}")
        if frozen.read_bytes() != snapshot_source.read_bytes():
            raise GateError(f"frozen harness differs from the build snapshot: {name}")
        expected_harness_manifest.extend(
            f"{_sha256(frozen)}  ./{name}\n".encode()
        )
    if (result_dir / "metadata" / "harness.sha256").read_bytes() != bytes(
        expected_harness_manifest
    ):
        raise GateError("frozen harness checksum manifest differs")
    template = result_dir / "metadata" / "config-template.toml"
    expectations = _read_json(harness_root / "phase1_4m_expectations.json")
    if expectations.get("config_template_sha256") != _sha256(template):
        raise GateError("frozen config template differs from pinned expectations")
    expected_template_manifest = f"{_sha256(template)}  {template}\n".encode()
    if (result_dir / "metadata" / "config-template.sha256").read_bytes() != expected_template_manifest:
        raise GateError("frozen config template checksum manifest differs")


def _validate_build_and_binary_evidence(result_dir: Path) -> None:
    build_root = result_dir / "metadata" / "build"
    _zero_exit_status(build_root / "build.exit-status", "formal build")
    environment_rows = _read_tsv(build_root / "build-environment.tsv", "build environment")
    if not environment_rows or environment_rows[0] != ["name", "value"]:
        raise GateError("build environment header differs")
    environment: dict[str, str] = {}
    for row in environment_rows[1:]:
        if len(row) != 2 or row[0] in environment:
            raise GateError("build environment contains a malformed/duplicate row")
        environment[row[0]] = row[1]
    expected_names = {
        "PATH",
        "HOME",
        "RUSTUP_HOME",
        "CARGO_HOME",
        "CARGO_TARGET_DIR",
        "CARGO_INCREMENTAL",
        "CARGO_TERM_COLOR",
        "LC_ALL",
        "TZ",
        "SOURCE_DATE_EPOCH",
        "RUSTFLAGS",
        "RUSTDOCFLAGS",
        "features",
        "profile",
        "target",
    }
    if set(environment) != expected_names:
        raise GateError("build environment names differ from the isolated contract")
    expected_paths = {
        "HOME": result_dir / "build-state" / "home",
        "CARGO_HOME": result_dir / "build-state" / "cargo-home",
        "CARGO_TARGET_DIR": result_dir / "build-target",
    }
    for name, path in expected_paths.items():
        if environment[name] != str(path) or not path.is_dir() or path.is_symlink():
            raise GateError(f"isolated build path differs: {name}")
    if (
        environment["CARGO_INCREMENTAL"] != "0"
        or environment["CARGO_TERM_COLOR"] != "never"
        or environment["LC_ALL"] != "C"
        or environment["TZ"] != "UTC"
        or environment["RUSTFLAGS"] != "<unset>"
        or environment["RUSTDOCFLAGS"] != "<unset>"
        or environment["features"] != "no-default-features"
        or environment["profile"] != "release"
        or not re.fullmatch(r"[1-9][0-9]*", environment["SOURCE_DATE_EPOCH"])
        or not re.fullmatch(r"[A-Za-z0-9_.-]+", environment["target"])
    ):
        raise GateError("build environment values differ from the isolated contract")
    metadata = _read_json(build_root / "cargo-metadata.json")
    if metadata.get("workspace_root") != str(result_dir / "build-source"):
        raise GateError("Cargo metadata workspace root is not the read-only source snapshot")
    if metadata.get("target_directory") != str(result_dir / "build-target"):
        raise GateError("Cargo metadata target directory differs from the isolated target")
    packages = metadata.get("packages")
    if not isinstance(packages, list) or not packages:
        raise GateError("Cargo metadata contains no packages")
    source_root = result_dir / "build-source"
    for package in packages:
        if not isinstance(package, dict) or not isinstance(package.get("manifest_path"), str):
            raise GateError("Cargo metadata package is malformed")
        manifest_path = Path(package["manifest_path"])
        try:
            manifest_path.relative_to(source_root)
        except ValueError as error:
            raise GateError("Cargo metadata references a manifest outside the source snapshot") from error
    tool_rows = _read_tsv(build_root / "tool-paths.tsv", "build tool paths")
    if (
        len(tool_rows) != 6
        or any(len(row) != 2 for row in tool_rows)
        or len({row[0] for row in tool_rows}) != len(tool_rows)
    ):
        raise GateError("build tool path table is malformed")
    tools = {row[0]: row[1] for row in tool_rows}
    if set(tools) != {"cargo", "rustc", "cc", "rustup", "rustc-toolchain", "cargo-toolchain"}:
        raise GateError("build tool path set differs")
    command = shlex.split((build_root / "build-command.txt").read_text(encoding="utf-8"))
    expected_command = [
        tools["cargo"],
        "build",
        "--locked",
        "--release",
        "--no-default-features",
        "--target",
        environment["target"],
        "-p",
        "chronoxide-ingester",
        "--bin",
        "chronoxide-ingester",
        "--bin",
        "chronoxide-capture-repartition",
        "--bin",
        "chronoxide-query",
        "--bin",
        "chronoxide-storage-verify",
    ]
    if command != expected_command:
        raise GateError("formal build argv differs from the source-bound contract")

    binary_rows = _read_tsv(result_dir / "metadata" / "binaries.tsv", "binary provenance")
    if not binary_rows or binary_rows[0] != ["role", "source", "preserved", "sha256"]:
        raise GateError("binary provenance header differs")
    expected_binaries = (
        "chronoxide-ingester",
        "chronoxide-capture-repartition",
        "chronoxide-query",
        "chronoxide-storage-verify",
    )
    if len(binary_rows) != len(expected_binaries) + 1:
        raise GateError("binary provenance row count differs")
    release_root = result_dir / "build-target" / environment["target"] / "release"
    binary_manifest = bytearray()
    for row, binary in zip(binary_rows[1:], expected_binaries, strict=True):
        preserved = result_dir / "metadata" / "binaries" / binary
        if row != [binary, str(release_root / binary), str(preserved), _sha256(preserved)]:
            raise GateError(f"binary provenance differs: {binary}")
        if stat.S_IMODE(preserved.stat().st_mode) != 0o555:
            raise GateError(f"preserved binary is not exact mode 0555: {binary}")
        source = release_root / binary
        _regular_file(source, f"built release binary {binary}", executable=True)
        if _sha256(source) != row[3]:
            raise GateError(f"built/preserved binary bytes differ: {binary}")
        binary_manifest.extend(f"{row[3]}  {preserved}\n".encode())
    if (result_dir / "metadata" / "binaries.sha256").read_bytes() != bytes(
        binary_manifest
    ):
        raise GateError("preserved binary checksum manifest differs")


def _validate_capture_inventories(
    result_dir: Path, settings: dict[str, str]
) -> None:
    inventory_root = result_dir / "inventory"
    source_documents = []
    for label in ("source-before", "source-after-transforms", "source-capture-after-runs"):
        source_documents.append(
            _validate_capture_inventory_document(
                inventory_root / f"{label}.json",
                inventory_root / f"{label}-files.nul",
            )
        )
    if source_documents[1:] != source_documents[:1] * 2:
        raise GateError("source capture inventories differ across the formal experiment")
    if source_documents[0]["capture"] != settings.get("capture"):
        raise GateError("source capture inventory is not bound to formal settings")
    derived: dict[str, dict[str, Any]] = {}
    for label in CAPTURE_LABELS:
        before = _validate_capture_inventory_document(
            inventory_root / f"{label}-before-runs.json",
            inventory_root / f"{label}-before-runs-files.nul",
        )
        after = _validate_capture_inventory_document(
            inventory_root / f"{label}-capture-after-runs.json",
            inventory_root / f"{label}-capture-after-runs-files.nul",
        )
        if before != after:
            raise GateError(f"derived capture inventory differs: {label}")
        expected_capture = (
            result_dir / "captures" / "determinism" / label.removeprefix("determinism-")
            if label.startswith("determinism-")
            else result_dir / "captures" / label
        )
        if before["capture"] != str(expected_capture):
            raise GateError(f"derived capture inventory path differs: {label}")
        derived[label] = before

    writeback_rows = _read_tsv(
        result_dir / "validation" / "generated-capture-writeback.tsv",
        "generated capture writeback",
    )
    if not writeback_rows or writeback_rows[0] != [
        "purpose",
        "file",
        "bytes",
        "writeback_completed_at",
    ]:
        raise GateError("generated capture writeback header differs")
    expected_writebacks: list[tuple[str, str, int]] = []
    for topology in TOPOLOGIES:
        for variant in ("a", "b"):
            label = f"determinism-{topology}-{variant}"
            for row in derived[label]["files"]:
                expected_writebacks.append(
                    (
                        label,
                        str(Path(derived[label]["capture"]) / row["path"]),
                        row["size_bytes"],
                    )
                )
        label = topology
        for row in derived[label]["files"]:
            expected_writebacks.append(
                (
                    f"full-{topology}",
                    str(Path(derived[label]["capture"]) / row["path"]),
                    row["size_bytes"],
                )
            )
    if len(writeback_rows) != len(expected_writebacks) + 1:
        raise GateError("generated capture writeback row count differs")
    for row, expected in zip(writeback_rows[1:], expected_writebacks, strict=True):
        if (
            len(row) != 4
            or row[:3] != [expected[0], expected[1], str(expected[2])]
            or not row[3]
        ):
            raise GateError("generated capture writeback row differs from inventories")

    plan_rows = _read_tsv(result_dir / "run-plan.tsv", "run plan")
    for row in plan_rows[1:]:
        run, topology = row[1], row[2]
        inventory = derived[topology]
        run_root = result_dir / "runs" / run
        replay_writeback = _read_tsv(
            run_root / "capture-writeback-before.tsv", f"{run} capture writeback"
        )
        if not replay_writeback or replay_writeback[0] != [
            "file",
            "bytes",
            "writeback_completed_at",
        ] or len(replay_writeback) != len(inventory["files"]) + 1:
            raise GateError(f"{run} capture writeback matrix differs")
        for actual, expected in zip(
            replay_writeback[1:], inventory["files"], strict=True
        ):
            expected_path = str(Path(inventory["capture"]) / expected["path"])
            if (
                len(actual) != 3
                or actual[:2] != [expected_path, str(expected["size_bytes"])]
                or not actual[2]
            ):
                raise GateError(f"{run} capture writeback differs from inventory")
        expected_capture_files = [
            (
                str(Path(inventory["capture"]) / expected["path"]),
                expected["size_bytes"],
            )
            for expected in inventory["files"]
            if expected["path"].endswith(".capture")
        ]
        residency_lines = (
            run_root / "capture-residency-before.tsv"
        ).read_text(encoding="utf-8").splitlines()
        if len(residency_lines) != len(expected_capture_files):
            raise GateError(f"{run} capture residency row count differs")
        for line, (expected_path, expected_size) in zip(
            residency_lines, expected_capture_files, strict=True
        ):
            fields = line.split(maxsplit=2)
            if fields != ["0", str(expected_size), expected_path]:
                raise GateError(f"{run} capture residency is nonzero or misbound")


def _one_ingestion_report(root: Path) -> Path:
    pattern = re.compile(r"ingestion_stats_[0-9]{8}_[0-9]{6}[.]md")
    reports = [
        path
        for path in root.iterdir()
        if path.is_file() and not path.is_symlink() and pattern.fullmatch(path.name)
    ]
    if len(reports) != 1:
        raise GateError(f"replay must contain exactly one canonical ingestion report: {root}")
    return reports[0]


def _validate_repartition_evidence(
    result_dir: Path, settings: dict[str, str]
) -> None:
    binary = result_dir / "metadata" / "binaries" / "chronoxide-capture-repartition"
    source_capture = settings.get("capture")
    if not source_capture or not Path(source_capture).is_absolute():
        raise GateError("formal settings lack an absolute source capture")
    try:
        full_messages = int(settings["messages"])
        prefix_messages = int(settings["determinism_prefix_messages"])
    except (KeyError, ValueError) as error:
        raise GateError("formal settings lack repartition message counts") from error
    reports: dict[tuple[str, str], Path] = {}
    for topology in TOPOLOGIES:
        for variant in ("a", "b"):
            label = f"prefix-{topology}-{variant}"
            report_path = result_dir / "validation" / f"repartition-{label}.json"
            output = result_dir / "captures" / "determinism" / f"{topology}-{variant}"
            arguments = [
                "--input",
                source_capture,
                "--output",
                str(output),
                "--report",
                str(report_path),
                "--layout",
                topology,
                "--partitions",
                "16",
                "--max-messages",
                str(prefix_messages),
            ]
            _validate_runtime_identity_document(
                result_dir
                / "validation"
                / f"repartition-{label}.runtime-identity.json",
                "repartition",
                binary,
                expected_environment={"LC_ALL": "C", "TZ": "UTC"},
                expected_arguments=arguments,
            )
            report = _read_json(report_path)
            if report.get("input") != source_capture or report.get("output") != str(output):
                raise GateError(f"repartition report is not bound to runtime argv: {label}")
            if _read_json(
                result_dir / "validation" / f"repartition-{label}.stdout.json"
            ) != report:
                raise GateError(f"repartition stdout/report differ: {label}")
            reports[(topology, variant)] = report_path
        repeat = gate_repartition_repeat(reports[(topology, "a")], reports[(topology, "b")])
        _require_json_equal(
            result_dir / "comparisons" / f"repartition-{topology}-repeat.json",
            repeat,
            f"repartition repeat gate {topology}",
        )
    prefix_matrix = gate_repartition(
        reports[("uniform", "a")], reports[("skew80-20", "a")], prefix_messages
    )
    _require_json_equal(
        result_dir / "comparisons" / "repartition-prefix-matrix.json",
        prefix_matrix,
        "repartition prefix matrix",
    )

    full_reports: dict[str, Path] = {}
    for topology in TOPOLOGIES:
        report_path = result_dir / "validation" / f"repartition-{topology}.json"
        output = result_dir / "captures" / topology
        arguments = [
            "--input",
            source_capture,
            "--output",
            str(output),
            "--report",
            str(report_path),
            "--layout",
            topology,
            "--partitions",
            "16",
            "--max-messages",
            str(full_messages),
        ]
        _validate_runtime_identity_document(
            result_dir / "validation" / f"repartition-{topology}.runtime-identity.json",
            "repartition",
            binary,
            expected_environment={"LC_ALL": "C", "TZ": "UTC"},
            expected_arguments=arguments,
        )
        report = _read_json(report_path)
        if report.get("input") != source_capture or report.get("output") != str(output):
            raise GateError(f"full repartition report is not bound to runtime argv: {topology}")
        if _read_json(
            result_dir / "validation" / f"repartition-{topology}.stdout.json"
        ) != report:
            raise GateError(f"full repartition stdout/report differ: {topology}")
        full_reports[topology] = report_path
    matrix = gate_repartition(
        full_reports["uniform"], full_reports["skew80-20"], full_messages
    )
    _require_json_equal(
        result_dir / "comparisons" / "repartition-matrix.json",
        matrix,
        "full repartition matrix",
    )


def _parse_time_fresh(phase1: Any, input_path: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="chronoxide-phase5-time-") as raw:
        output = Path(raw) / "time.json"
        return phase1.parse_gnu_time(input_path, output)


def _parse_perf_fresh(phase1: Any, input_path: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="chronoxide-phase5-perf-") as raw:
        output = Path(raw) / "perf.json"
        return phase1.parse_perf_stat(input_path, output, list(PERF_EVENTS))


def _validate_replay_and_sizing_evidence(
    result_dir: Path, sealed: dict[str, str], settings: dict[str, str]
) -> None:
    phase1 = _load_support_module("phase1_replay_gate.py")
    report_gate = _load_support_module("ab_gate.py")
    expectations = Path(__file__).resolve().with_name("phase1_4m_expectations.json")
    ingester = result_dir / "metadata" / "binaries" / "chronoxide-ingester"
    try:
        messages = int(settings["messages"])
        sizing_messages = int(settings["sizing_messages"])
        rss_interval_ms = int(settings["rss_interval_ms"])
        guard_interval_ms = int(settings["guard_interval_ms"])
    except (KeyError, ValueError) as error:
        raise GateError("formal settings lack replay counts/interval") from error
    rust_log = settings.get("rust_log_value")
    if (
        not rust_log
        or rss_interval_ms != LIFECYCLE_CADENCE_INTERVAL_MS
        or guard_interval_ms != LIFECYCLE_CADENCE_INTERVAL_MS
    ):
        raise GateError("formal settings lack the sanitized runtime/cadence contract")
    plan_rows = _read_tsv(result_dir / "run-plan.tsv", "run plan")
    summary_rows = _read_tsv(result_dir / "replay-summary.tsv", "replay summary")
    structures: dict[tuple[str, str], Path] = {}
    baseline_corpus: dict[str, bytes] = {}
    baseline_correctness: dict[str, dict[str, Any]] = {}
    expected_capacity_state = (
        ("seed", 2, 4, 4),
        ("seed", 1, 3, 4),
        ("dynamic", 0, 3, 3),
        ("dynamic", 0, 2, 3),
        ("dynamic", 0, 1, 3),
        ("dynamic", 0, 0, 3),
        ("dynamic", 0, 0, 2),
        ("dynamic", 0, 0, 1),
    )
    for plan_row, summary_row, expected_run, expected_capacity in zip(
        plan_rows[1:],
        summary_rows[1:],
        EXPECTED_RUNS,
        expected_capacity_state,
        strict=True,
    ):
        (
            _order,
            run,
            topology,
            cell,
            series_text,
            last_text,
            capture_text,
            config_text,
            segments_text,
        ) = plan_row
        if run != expected_run:
            raise GateError("run plan changed during post-hoc replay validation")
        run_root = result_dir / "runs" / run
        config = Path(config_text)
        capture = Path(capture_text)
        segments = Path(segments_text)
        rendered = _validate_rendered_config(
            config,
            capture=capture,
            segments=segments,
            messages=messages,
            series_adaptive=series_text == "true",
            last_adaptive=last_text == "true",
        )
        _require_json_equal(run_root / "config-render.json", rendered, f"config render {run}")
        _validate_runtime_identity_document(
            run_root / "runtime-identity.json",
            "ingester",
            ingester,
            expected_environment={
                "LC_ALL": "C",
                "TZ": "UTC",
                "CONFIG_FILE": config_text,
                "RUST_LOG": rust_log,
            },
            expected_arguments=[],
        )
        for filename, label in (
            ("replay.exit-status", "replay"),
            ("rss-monitor.exit-status", "RSS monitor"),
            ("guardian.exit-status", "measurement guardian"),
        ):
            _zero_exit_status(run_root / filename, f"{run} {label}")
        validate_process_snapshot(run_root / "processes-before.txt")
        validate_process_snapshot(run_root / "processes-after.txt")
        budget_rows = _read_tsv(run_root / "disk-budget-before.tsv", f"{run} disk budget")
        budget_header = [
            "stage",
            "available_bytes",
            "required_bytes",
            "guard_minimum_free_bytes",
            "seed_replays_remaining",
            "uniform_replays_remaining",
            "uniform_corpus_bound_bytes",
            "skew_replays_remaining",
            "skew_corpus_bound_bytes",
            "transient_rewrite_headroom_bytes",
            "harness_overhead_bytes",
            "safety_reserve_bytes",
        ]
        if len(budget_rows) != 2 or budget_rows[0] != budget_header or len(budget_rows[1]) != len(budget_header):
            raise GateError(f"{run} disk budget shape differs")
        budget = budget_rows[1]
        stage, seed_remaining, uniform_remaining, skew_remaining = expected_capacity
        if budget[0] != stage or [budget[4], budget[5], budget[7]] != [
            str(seed_remaining),
            str(uniform_remaining),
            str(skew_remaining),
        ]:
            raise GateError(f"{run} disk budget sequence differs")
        numeric = [
            _strict_decimal_int(value, f"{run} disk budget {name}")
            for name, value in zip(budget_header[1:], budget[1:], strict=True)
        ]
        (
            available,
            required,
            guardian_minimum,
            _seed_remaining,
            uniform_remaining_value,
            uniform_bound,
            skew_remaining_value,
            skew_bound,
            transient,
            harness,
            safety,
        ) = numeric
        current_bound = uniform_bound if topology == "uniform" else skew_bound
        if (
            required
            != uniform_remaining_value * uniform_bound
            + skew_remaining_value * skew_bound
            + transient
            + harness
            + safety
            or guardian_minimum != required - current_bound - transient
            or available < required
            or harness != int(settings["harness_overhead_bytes"])
            or safety != int(settings["safety_reserve_bytes"])
        ):
            raise GateError(f"{run} disk budget arithmetic/admission differs")
        _validate_guardian_evidence(
            run_root,
            run,
            expected_minimum_free_bytes=guardian_minimum,
            expected_filesystem=result_dir,
        )
        if (run_root / "replay.log").stat().st_size == 0:
            raise GateError(f"{run} raw replay log is empty")
        timing = _parse_time_fresh(phase1, run_root / "replay.time.txt")
        _require_json_equal(run_root / "replay.time.json", timing, f"GNU time parse {run}")
        perf = _parse_perf_fresh(phase1, run_root / "perf-stat.tsv")
        _require_json_equal(run_root / "perf-stat.json", perf, f"perf parse {run}")
        rss = _validate_rss_summary(run_root / "rss-samples.tsv", run_root / "rss-summary.json")
        if rss["interval_ms"] != rss_interval_ms:
            raise GateError(f"{run} RSS interval differs from formal settings")
        report = _one_ingestion_report(run_root)
        correctness = report_gate.parse_replay_report(report)
        _require_json_equal(
            run_root / "replay-correctness.json", correctness, f"replay correctness {run}"
        )
        try:
            phase1.gate_document(
                run_root / "replay-correctness.json", expectations, "replay_correctness"
            )
        except Exception as error:
            raise GateError(f"{run} replay correctness fails pinned expectations: {error}") from error
        structure = parse_head_report(report)
        _require_json_equal(run_root / "head-structure.json", structure, f"head parse {run}")
        structures[(topology, cell)] = run_root / "head-structure.json"
        corpus = _validate_corpus_evidence(result_dir, f"runs/{run}", sealed)
        expected_summary = [
            run,
            topology,
            cell,
            series_text,
            last_text,
            str(timing["elapsed"]),
            str(timing["user_seconds"]),
            str(timing["system_seconds"]),
            str(timing["max_rss_kib"]),
            str(rss["aggregate_rss_kib"]),
            str(corpus["file_count"]),
            str(corpus["size_bytes"]),
            str(corpus["manifest_sha256"]),
        ]
        if summary_row != expected_summary:
            raise GateError(f"replay summary row is not bound to raw evidence: {run}")
        manifest_bytes = (run_root / "segments.sha256").read_bytes()
        if topology not in baseline_corpus:
            baseline_corpus[topology] = manifest_bytes
            baseline_correctness[topology] = correctness
        elif manifest_bytes != baseline_corpus[topology] or correctness != baseline_correctness[topology]:
            raise GateError(f"same-topology replay semantics/corpus differ: {run}")

    matrix = gate_matrix(
        *(structures[(topology, cell)] for topology in TOPOLOGIES for cell in CELL_FACTORS)
    )
    _require_json_equal(
        result_dir / "comparisons" / "head-structure-matrix.json",
        matrix,
        "head structure matrix",
    )

    for topology in TOPOLOGIES:
        sizing_root = result_dir / "sizing" / topology
        config = result_dir / "configs" / f"sizing-{topology}.toml"
        capture = result_dir / "captures" / topology
        segments = sizing_root / "segments"
        rendered = _validate_rendered_config(
            config,
            capture=capture,
            segments=segments,
            messages=sizing_messages,
            series_adaptive=False,
            last_adaptive=False,
        )
        _require_json_equal(
            sizing_root / "config-render.json", rendered, f"sizing config render {topology}"
        )
        _validate_runtime_identity_document(
            sizing_root / "runtime-identity.json",
            "ingester",
            ingester,
            expected_environment={
                "LC_ALL": "C",
                "TZ": "UTC",
                "CONFIG_FILE": str(config),
                "RUST_LOG": rust_log,
            },
            expected_arguments=[],
        )
        _zero_exit_status(sizing_root / "replay.exit-status", f"{topology} sizing replay")
        _zero_exit_status(sizing_root / "guardian.exit-status", f"{topology} sizing guardian")
        _zero_exit_status(
            sizing_root / "rss-monitor.exit-status", f"{topology} sizing RSS monitor"
        )
        validate_process_snapshot(sizing_root / "processes-before.txt")
        validate_process_snapshot(sizing_root / "processes-after.txt")
        sizing_index = TOPOLOGIES.index(topology)
        sizing_budget_rows = _read_tsv(
            sizing_root / "disk-budget-before.tsv", f"{topology} sizing disk budget"
        )
        sizing_budget_header = [
            "topology",
            "available_before_bytes",
            "required_before_bytes",
            "guardian_minimum_free_bytes",
            "sizing_replays_remaining",
            "corpus_upper_bound_bytes",
            "transient_headroom_bytes",
            "harness_overhead_bytes",
            "safety_reserve_bytes",
        ]
        if (
            len(sizing_budget_rows) != 2
            or sizing_budget_rows[0] != sizing_budget_header
            or len(sizing_budget_rows[1]) != len(sizing_budget_header)
            or sizing_budget_rows[1][0] != topology
        ):
            raise GateError(f"{topology} sizing disk budget shape differs")
        sizing_budget = [
            _strict_decimal_int(value, f"{topology} sizing disk budget {name}")
            for name, value in zip(
                sizing_budget_header[1:], sizing_budget_rows[1][1:], strict=True
            )
        ]
        (
            sizing_available,
            sizing_required,
            sizing_minimum,
            sizing_remaining,
            sizing_bound,
            sizing_transient,
            sizing_harness,
            sizing_safety,
        ) = sizing_budget
        expected_sizing_remaining = len(TOPOLOGIES) - sizing_index
        if (
            sizing_remaining != expected_sizing_remaining
            or sizing_bound
            != int(settings["sizing_corpus_upper_bound_bytes_each"])
            or sizing_transient != int(settings["sizing_transient_headroom_bytes"])
            or sizing_harness != int(settings["harness_overhead_bytes"])
            or sizing_safety != int(settings["safety_reserve_bytes"])
            or sizing_required
            != sizing_remaining * sizing_bound
            + sizing_transient
            + sizing_harness
            + sizing_safety
            or sizing_minimum
            != (sizing_remaining - 1) * sizing_bound
            + sizing_harness
            + sizing_safety
            or sizing_available < sizing_required
        ):
            raise GateError(f"{topology} sizing disk budget arithmetic/admission differs")
        _validate_guardian_evidence(
            sizing_root,
            f"{topology} sizing",
            expected_minimum_free_bytes=sizing_minimum,
            expected_filesystem=result_dir,
        )
        sizing_rss = _validate_rss_summary(
            sizing_root / "rss-samples.tsv", sizing_root / "rss-summary.json"
        )
        if sizing_rss["interval_ms"] != rss_interval_ms:
            raise GateError(f"{topology} sizing RSS interval differs")
        if (sizing_root / "replay.log").stat().st_size == 0:
            raise GateError(f"{topology} sizing raw replay log is empty")
        report = _one_ingestion_report(sizing_root)
        correctness = report_gate.parse_replay_report(report)
        _require_json_equal(
            sizing_root / "replay-correctness.json",
            correctness,
            f"sizing replay correctness {topology}",
        )
        structure = parse_head_report(report)
        _require_json_equal(
            sizing_root / "head-structure.json", structure, f"sizing head parse {topology}"
        )
        _validate_corpus_evidence(result_dir, f"sizing/{topology}", sealed)
        _require_json_equal(
            sizing_root / "performance-disabled.json",
            {
                "schema": "chronoxide/head-topology-performance-disabled/v1",
                "perf_enabled": False,
                "reason": "untimed_capacity_sizing",
            },
            f"sizing performance-off marker {topology}",
        )

    replay_gate = validate_replay_summary(result_dir / "replay-summary.tsv")
    _require_json_equal(
        result_dir / "comparisons" / "replay-summary-validation.json",
        replay_gate,
        "replay summary gate",
    )


def _validate_storage_readback_and_performance_evidence(result_dir: Path) -> str:
    phase1 = _load_support_module("phase1_replay_gate.py")
    expectations = Path(__file__).resolve().with_name("phase1_4m_expectations.json")
    verifier = result_dir / "metadata" / "binaries" / "chronoxide-storage-verify"
    query = result_dir / "metadata" / "binaries" / "chronoxide-query"
    storage_paths: dict[str, Path] = {}
    for topology in TOPOLOGIES:
        validation = result_dir / "validation" / topology
        corpus = result_dir / "runs" / f"{topology}-aa-01" / "segments"
        verifier_arguments = [
            "--segments-dir",
            str(corpus),
            "--schema",
            "schema8",
            "--validate-segment-footers",
            "--verify-exact-postings",
            "--decoded-semantic-fingerprint",
        ]
        _validate_runtime_identity_document(
            validation / "storage-verify.runtime-identity.json",
            "verifier",
            verifier,
            expected_environment={"LC_ALL": "C", "TZ": "UTC"},
            expected_arguments=verifier_arguments,
        )
        storage_path = validation / "storage-verify.json"
        storage_paths[topology] = storage_path
        _storage_report(storage_path, _read_json(expectations)["storage_verifier"]["samples"])

        report = validation / "readbacks.md"
        query_arguments = [
            "--segments-dir",
            str(corpus),
            "--storage-layout",
            "schema8",
            "--sample-limit-per-kind",
            "2",
            "--verify-readbacks",
            "--output",
            str(report),
        ]
        _validate_runtime_identity_document(
            validation / "readbacks.runtime-identity.json",
            "query",
            query,
            expected_environment={"LC_ALL": "C", "TZ": "UTC"},
            expected_arguments=query_arguments,
        )
        try:
            parsed = phase1.gate_readbacks(report, expectations, None)
        except Exception as error:
            raise GateError(f"{topology} readback report fails the pinned gate: {error}") from error
        _require_json_equal(
            validation / "readbacks.json", parsed, f"readback gate {topology}"
        )
    storage = gate_storage_validation(
        storage_paths["uniform"], storage_paths["skew80-20"], expectations
    )
    _require_json_equal(
        result_dir / "comparisons" / "storage-validation.json",
        storage,
        "storage validation gate",
    )
    # Each topology is an independent workload stratum. The readback oracle
    # must pass in both, but its output is deliberately not compared across
    # strata: an order-independent persisted-record multiset cannot prove
    # duplicate-winner/query equivalence after repartitioning.

    preflight = _parse_perf_fresh(phase1, result_dir / "metadata" / "perf-preflight.tsv")
    _require_json_equal(
        result_dir / "metadata" / "perf-preflight.json", preflight, "perf preflight"
    )
    performance = gate_performance(result_dir)
    _require_json_equal(
        result_dir / "comparisons" / "performance-decision.json",
        performance,
        "performance decision gate",
    )
    disposition = performance["overall_disposition"]
    if disposition not in PERFORMANCE_MARKERS:
        raise GateError("performance gate emitted an unknown disposition")
    return disposition


def _validate_control_tables(result_dir: Path, settings: dict[str, str]) -> None:
    prefix_rows = _read_tsv(
        result_dir / "validation" / "determinism-prefix-sizes.tsv",
        "determinism prefix sizes",
    )
    if not prefix_rows or prefix_rows[0] != [
        "topology",
        "variant",
        "messages",
        "actual_bytes",
        "upper_bound_bytes",
    ]:
        raise GateError("determinism prefix size header differs")
    expected_prefix = [(topology, variant) for topology in TOPOLOGIES for variant in ("a", "b")]
    if len(prefix_rows) != len(expected_prefix) + 1:
        raise GateError("determinism prefix size row count differs")
    for row, (topology, variant) in zip(prefix_rows[1:], expected_prefix, strict=True):
        if len(row) != 5 or row[:3] != [
            topology,
            variant,
            settings["determinism_prefix_messages"],
        ]:
            raise GateError("determinism prefix size row identity differs")
        actual = _strict_decimal_int(row[3], "determinism prefix actual bytes", positive=True)
        bound = _strict_decimal_int(row[4], "determinism prefix bound", positive=True)
        if str(bound) != settings["determinism_prefix_output_bound_bytes"] or actual > bound:
            raise GateError("determinism prefix size exceeds/differs from its bound")

    full_rows = _read_tsv(
        result_dir / "validation" / "full-capture-sizes.tsv", "full capture sizes"
    )
    if not full_rows or full_rows[0] != [
        "topology",
        "messages",
        "actual_bytes",
        "upper_bound_bytes",
    ] or len(full_rows) != len(TOPOLOGIES) + 1:
        raise GateError("full capture size matrix differs")
    for row, topology in zip(full_rows[1:], TOPOLOGIES, strict=True):
        if len(row) != 4 or row[:2] != [topology, settings["messages"]]:
            raise GateError("full capture size row identity differs")
        actual = _strict_decimal_int(row[2], "full capture actual bytes", positive=True)
        bound = _strict_decimal_int(row[3], "full capture bound", positive=True)
        expected_bound = int(settings["source_capture_bytes"]) + int(
            settings["full_capture_layout_overhead_bytes_each"]
        )
        if actual > bound or bound != expected_bound:
            raise GateError("full capture exceeds/differs from its declared bound")

    transform_rows = _read_tsv(
        result_dir / "validation" / "transform-capacity-plan.tsv",
        "transform capacity plan",
    )
    transform_header = [
        "order",
        "label",
        "output_upper_bound_bytes",
        "remaining_before_bytes",
        "remaining_after_bytes",
        "available_before_bytes",
        "required_before_bytes",
        "guardian_minimum_free_bytes",
    ]
    if (
        not transform_rows
        or transform_rows[0] != transform_header
        or len(transform_rows) != len(TRANSFORM_LABELS) + 1
    ):
        raise GateError("transform capacity plan shape differs")
    try:
        prefix_bound = int(settings["determinism_prefix_output_bound_bytes"])
        full_bound = int(settings["source_capture_bytes"]) + int(
            settings["full_capture_layout_overhead_bytes_each"]
        )
        base_reserve = (
            int(settings["sizing_corpus_estimate_bytes"])
            + int(settings["sizing_transient_headroom_bytes"])
            + int(settings["harness_overhead_bytes"])
            + int(settings["safety_reserve_bytes"])
        )
    except (KeyError, ValueError) as error:
        raise GateError("formal settings lack transform capacity inputs") from error
    expected_bounds = [prefix_bound] * 4 + [full_bound] * 2
    remaining = sum(expected_bounds)
    for order, (row, label, bound) in enumerate(
        zip(transform_rows[1:], TRANSFORM_LABELS, expected_bounds, strict=True), 1
    ):
        if len(row) != len(transform_header) or row[:2] != [str(order), label]:
            raise GateError("transform capacity row identity differs")
        numeric = [
            _strict_decimal_int(value, f"transform capacity {name}")
            for name, value in zip(transform_header[2:], row[2:], strict=True)
        ]
        observed_bound, before, after, available, required, minimum = numeric
        if (
            observed_bound != bound
            or before != remaining
            or after != before - bound
            or minimum != after + base_reserve
            or required != bound + minimum
            or available < required
        ):
            raise GateError("transform capacity arithmetic/admission differs")
        lifecycle_root = result_dir / "validation" / "transform-guards" / label
        for filename, description in (
            ("workload.exit-status", "transform workload"),
            ("rss-monitor.exit-status", "transform RSS monitor"),
            ("guardian.exit-status", "transform guardian"),
        ):
            _zero_exit_status(lifecycle_root / filename, f"{label} {description}")
        _validate_guardian_evidence(
            lifecycle_root,
            label,
            expected_minimum_free_bytes=minimum,
            expected_filesystem=result_dir,
        )
        rss = _validate_rss_summary(
            lifecycle_root / "rss-samples.tsv",
            lifecycle_root / "rss-summary.json",
        )
        if rss["interval_ms"] != LIFECYCLE_CADENCE_INTERVAL_MS:
            raise GateError(f"{label} transform RSS cadence differs")
        remaining = after
    if remaining != 0:
        raise GateError("transform capacity reserve did not drain exactly")

    sizing_rows = _read_tsv(
        result_dir / "comparisons" / "topology-sizing.tsv", "topology sizing"
    )
    sizing_header = [
        "topology",
        "messages",
        "corpus_bytes",
        "max_chunks_bytes",
        "full_scale",
        "safety_multiplier",
        "formal_corpus_upper_bound_bytes",
        "transient_rewrite_upper_bound_bytes",
    ]
    if not sizing_rows or sizing_rows[0] != sizing_header or len(sizing_rows) != 3:
        raise GateError("topology sizing matrix differs")
    for row, topology in zip(sizing_rows[1:], TOPOLOGIES, strict=True):
        if len(row) != len(sizing_header) or row[:2] != [topology, settings["sizing_messages"]]:
            raise GateError("topology sizing row identity differs")
        values = [
            _strict_decimal_int(value, f"topology sizing {name}", positive=True)
            for name, value in zip(sizing_header[2:], row[2:], strict=True)
        ]
        corpus, max_chunks, full_scale, multiplier, formal_bound, transient_bound = values
        messages = int(settings["messages"])
        sizing_messages = int(settings["sizing_messages"])
        if (
            full_scale != (messages + sizing_messages - 1) // sizing_messages
            or multiplier != int(settings["sizing_safety_multiplier"])
            or formal_bound != corpus * full_scale * multiplier
            or transient_bound < max_chunks * full_scale * multiplier
        ):
            raise GateError("topology sizing arithmetic differs")

    seed_rows = _read_tsv(
        result_dir / "comparisons" / "seed-dynamic-disk-budget.tsv",
        "seed dynamic disk budget",
    )
    seed_header = [
        "uniform_seed_bytes",
        "skew_seed_bytes",
        "uniform_remaining",
        "skew_remaining",
        "remaining_corpus_bytes",
        "transient_rewrite_headroom_bytes",
        "harness_overhead_bytes",
        "safety_reserve_bytes",
        "required_free_bytes",
        "available_bytes",
    ]
    if len(seed_rows) != 2 or seed_rows[0] != seed_header or len(seed_rows[1]) != 10:
        raise GateError("seed dynamic disk budget shape differs")
    if seed_rows[1][2:4] != ["3", "3"]:
        raise GateError("seed dynamic disk budget remaining-run counts differ")
    numeric = [
        _strict_decimal_int(value, f"seed disk budget column {index}", positive=True)
        for index, value in enumerate(seed_rows[1])
    ]
    if numeric[4] != 3 * numeric[0] + 3 * numeric[1] or numeric[8] > numeric[9]:
        raise GateError("seed dynamic disk budget arithmetic/availability differs")


def _strict_decimal_int(value: str, label: str, *, positive: bool = False) -> int:
    pattern = r"[1-9][0-9]*" if positive else r"0|[1-9][0-9]*"
    if not re.fullmatch(pattern, value):
        raise GateError(f"{label} must be a canonical decimal integer")
    return int(value)


def _validate_seal_check_log(result_dir: Path) -> None:
    expected = ["initial-preserved-binaries", "after-preserved-binary-help-probes", "after-run-plan-and-config-rendering", "before-source-capture-inventory", "after-source-capture-inventory"]
    for topology in TOPOLOGIES:
        for variant in ("a", "b"):
            expected.extend(
                (f"before-prefix-transform-{topology}-{variant}", f"after-prefix-transform-{topology}-{variant}")
            )
    for topology in TOPOLOGIES:
        expected.extend((f"before-full-transform-{topology}", f"after-full-transform-{topology}"))
    expected.extend(("before-derived-capture-inventories", "after-derived-capture-inventories", "after-fadvise-tool-build"))
    for topology in TOPOLOGIES:
        expected.extend((f"before-sizing-{topology}", f"after-sizing-{topology}"))
    for run in EXPECTED_RUNS:
        expected.extend((f"{run}-before-replay", f"{run}-after-replay"))
    expected.append("after-replay-summary-validation")
    for topology in TOPOLOGIES:
        expected.extend(
            (
                f"{topology}-before-storage-verifier",
                f"{topology}-after-storage-verifier",
                f"{topology}-before-readbacks",
                f"{topology}-after-readbacks",
            )
        )
    expected.extend(("before-capture-after-runs-inventories", "after-capture-after-runs-inventories", "before-final-artifact-seal"))
    rows = _read_tsv(result_dir / "metadata" / "seal-checks.tsv", "seal check log")
    if not rows or rows[0] != ["recorded_at", "context"] or len(rows) != len(expected) + 1:
        raise GateError("seal check log shape differs")
    if [row[1] for row in rows[1:] if len(row) == 2] != expected or any(
        len(row) != 2 or not row[0] for row in rows[1:]
    ):
        raise GateError("seal check log contexts differ from the full experiment sequence")


def _validate_full_posthoc_contract(
    result_dir: Path,
    sealed: dict[str, str],
    run_plan: dict[str, Any],
    replay_summary: dict[str, Any],
) -> str:
    _validate_named_sha256_manifest(
        result_dir / "metadata" / "run-plan.sha256",
        [result_dir / "run-plan.tsv"],
        "run plan checksum manifest",
    )
    _validate_named_sha256_manifest(
        result_dir / "metadata" / "replay-summary.sha256",
        [result_dir / "replay-summary.tsv"],
        "replay summary checksum manifest",
    )
    config_paths = [result_dir / "configs" / f"{run}.toml" for run in EXPECTED_RUNS]
    config_paths.extend(
        result_dir / "configs" / f"sizing-{topology}.toml"
        for topology in TOPOLOGIES
    )
    _validate_named_sha256_manifest(
        result_dir / "metadata" / "configs.sha256",
        config_paths,
        "rendered config checksum manifest",
    )
    _validate_named_sha256_manifest(
        result_dir / "metadata" / "tools" / "fadvise-regular-dontneed.sha256",
        [result_dir / "metadata" / "tools" / "fadvise-regular-dontneed"],
        "fadvise helper checksum manifest",
    )
    _require_json_equal(
        result_dir / "comparisons" / "run-plan-validation.json",
        run_plan,
        "run plan gate",
    )
    _require_json_equal(
        result_dir / "comparisons" / "replay-summary-validation.json",
        replay_summary,
        "replay summary gate",
    )
    settings = _read_settings(result_dir / "metadata" / "settings.txt")
    if (
        settings.get("result_dir") != str(result_dir)
        or settings.get("build_source_dir") != str(result_dir / "build-source")
        or settings.get("source_archive")
        != str(result_dir / "metadata" / "source" / "source-head.tar")
        or settings.get("source_snapshot_seal")
        != str(result_dir / "metadata" / "source" / "source-snapshot-seal.json")
    ):
        raise GateError("formal settings are not bound to this result/provenance root")
    _validate_conflict_scan(
        result_dir / "metadata" / "processes-before-transforms.json",
        "pre-transform quiet-host conflict scan",
    )
    _validate_provenance_evidence(result_dir)
    _validate_build_and_binary_evidence(result_dir)
    _validate_control_tables(result_dir, settings)
    _validate_repartition_evidence(result_dir, settings)
    _validate_capture_inventories(result_dir, settings)
    _validate_replay_and_sizing_evidence(result_dir, sealed, settings)
    disposition = _validate_storage_readback_and_performance_evidence(result_dir)
    validate_process_snapshot(result_dir / "metadata" / "processes-before-final-seal.txt")
    _validate_seal_check_log(result_dir)
    return disposition


def validate_final_seal(result_dir: Path, stage: str = "complete") -> dict[str, Any]:
    if stage not in {"evidence", "complete"}:
        raise GateError(f"unknown final seal validation stage: {stage}")
    result_dir = result_dir.resolve(strict=True)
    if not result_dir.is_dir() or result_dir.is_symlink():
        raise GateError("formal result must be an absolute non-symlink directory")
    present_markers = [
        marker
        for marker in PERFORMANCE_MARKERS.values()
        if (result_dir / marker).exists() or (result_dir / marker).is_symlink()
    ]
    if len(present_markers) != 1:
        raise GateError(
            "formal result must contain exactly one performance disposition marker"
        )
    performance_marker = present_markers[0]
    _validate_result_roots(result_dir, stage, performance_marker)
    run_plan = validate_run_plan(result_dir, result_dir / "run-plan.tsv")
    replay_summary = validate_replay_summary(result_dir / "replay-summary.tsv")
    manifest, sealed = _read_final_manifest(result_dir)
    expected_artifacts = _formal_fixed_artifacts() | _dynamic_formal_artifacts(result_dir)
    if set(sealed) != expected_artifacts:
        raise GateError(
            "exact formal artifact matrix differs: "
            f"missing={sorted(expected_artifacts - set(sealed))[:5]} "
            f"extra={sorted(set(sealed) - expected_artifacts)[:5]}"
        )
    observed_artifacts: set[str] = {"run-plan.tsv", "replay-summary.tsv"}
    for root_name in (
        "metadata",
        "configs",
        "validation",
        "comparisons",
        "inventory",
        "sizing",
        "runs",
    ):
        for walk_root, directories, files in os.walk(
            result_dir / root_name, followlinks=False, onerror=_raise_walk_error
        ):
            walk_path = Path(walk_root)
            for name in directories:
                if (walk_path / name).is_symlink():
                    raise GateError(f"sealed artifact tree contains a symlink directory: {walk_path / name}")
            for name in files:
                path = walk_path / name
                if path == manifest:
                    continue
                relative = path.relative_to(result_dir).as_posix()
                _regular_file(path, f"sealed artifact {relative}")
                observed_artifacts.add(relative)
    if observed_artifacts != expected_artifacts:
        raise GateError(
            "on-disk formal artifact matrix differs from its seal: "
            f"missing={sorted(expected_artifacts - observed_artifacts)[:5]} "
            f"extra={sorted(observed_artifacts - expected_artifacts)[:5]}"
        )
    _validate_sealed_directory_matrix(result_dir, expected_artifacts)
    disposition = _validate_full_posthoc_contract(
        result_dir, sealed, run_plan, replay_summary
    )
    if PERFORMANCE_MARKERS[disposition] != performance_marker:
        raise GateError("performance marker contradicts the freshly recomputed decision")
    evidence_result = {
        "schema": FINAL_SEAL_SCHEMA,
        "stage": "evidence",
        "artifact_count": len(sealed),
        "manifest_sha256": _sha256(manifest),
        "performance_disposition": disposition,
        "validated": True,
    }
    if stage == "complete":
        marker = result_dir / "FINAL_SEAL_VALIDATED"
        expected_marker = (json.dumps(evidence_result, sort_keys=True) + "\n").encode()
        if marker.read_bytes() != expected_marker:
            raise GateError("FINAL_SEAL_VALIDATED does not exactly name the evidence-stage result")
        if (result_dir / "COMPLETE").read_text(encoding="utf-8") != COMPLETE_MARKER:
            raise GateError("COMPLETE marker is absent, ambiguous, or malformed")
    return {**evidence_result, "stage": stage}


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise GateError(f"JSON root must be an object: {path}")
    return value


def _write_json_exclusive(path: Path, value: dict[str, Any]) -> None:
    with path.open("x", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")


def _require_exact_keys(value: dict[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        raise GateError(
            f"{label} keys differ: missing={sorted(expected - actual)} "
            f"extra={sorted(actual - expected)}"
        )


def _strict_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GateError(f"{label} must be a non-negative integer")
    return value


def _require_ratio(actual: float, numerator: int, denominator: int, label: str) -> None:
    expected = numerator / denominator if denominator else 0.0
    # Markdown telemetry is rendered with six fractional digits.
    if abs(actual - expected) > 0.000001:
        raise GateError(f"{label} disagrees with its exact counters")


def gate_repartition(uniform_path: Path, skew_path: Path, messages: int) -> dict[str, Any]:
    if messages <= 0:
        raise GateError("messages must be positive")
    reports = {
        "uniform": _read_json(uniform_path),
        "skew80-20": _read_json(skew_path),
    }
    required = {
        "schema",
        "input",
        "output",
        "layout",
        "mapping_spec",
        "partition_count",
        "max_messages",
        "topic",
        "compression",
        "messages",
        "payload_bytes",
        "partitions",
        "input_manifest_sha256",
        "output_manifest_sha256",
        "input_stream_sha256",
        "output_stream_sha256",
        "input_content_stream_sha256",
        "output_content_stream_sha256",
        "content_streams_equal",
        "mapping_sha256",
        "output_tree_sha256",
        "reopened_verification",
    }
    for layout, report in reports.items():
        _require_exact_keys(report, required, f"{layout} repartition report")
        if report["schema"] != REPARTITION_SCHEMA or report["layout"] != layout:
            raise GateError(f"{layout} report schema/layout mismatch")
        partition_count = _strict_int(
            report["partition_count"], f"{layout}.partition_count"
        )
        if partition_count != PARTITION_COUNT:
            raise GateError(f"{layout} must use exactly {PARTITION_COUNT} partitions")
        expected_mapping_spec = (
            UNIFORM_MAPPING_SPEC if layout == "uniform" else SKEW_MAPPING_SPEC
        )
        if report["mapping_spec"] != expected_mapping_spec:
            raise GateError(f"{layout} mapping specification differs")
        if report["compression"] != "zstd":
            raise GateError(f"{layout} determinism evidence must retain Zstd compression")
        if (
            _strict_int(report["messages"], f"{layout}.messages") != messages
            or _strict_int(report["max_messages"], f"{layout}.max_messages") != messages
        ):
            raise GateError(f"{layout} did not transform the exact selected prefix")
        payload_bytes = _strict_int(report["payload_bytes"], f"{layout}.payload_bytes")
        if payload_bytes == 0:
            raise GateError(f"{layout} transformed no payload bytes")
        if report["reopened_verification"] is not True:
            raise GateError(f"{layout} lacks reopened byte-preservation verification")
        if report["content_streams_equal"] is not True:
            raise GateError(f"{layout} lacks canonical content-stream equality")
        for field in (
            "input_manifest_sha256",
            "output_manifest_sha256",
            "input_stream_sha256",
            "output_stream_sha256",
            "input_content_stream_sha256",
            "output_content_stream_sha256",
            "mapping_sha256",
            "output_tree_sha256",
        ):
            if not re.fullmatch(r"[0-9a-f]{64}", report[field]):
                raise GateError(f"{layout}.{field} is not a lowercase SHA-256")
        if (
            report["input_content_stream_sha256"]
            != report["output_content_stream_sha256"]
        ):
            raise GateError(f"{layout} canonical input/output content hashes differ")
        partitions = report["partitions"]
        if not isinstance(partitions, list) or len(partitions) != PARTITION_COUNT:
            raise GateError(f"{layout} must report all {PARTITION_COUNT} partitions")
        expected_counts = [0] * PARTITION_COUNT
        expected_first: list[int | None] = [None] * PARTITION_COUNT
        expected_last: list[int | None] = [None] * PARTITION_COUNT
        for ordinal in range(messages):
            if layout == "uniform":
                destination = ordinal % PARTITION_COUNT
            elif ordinal % 5 != 4:
                destination = 0
            else:
                destination = 1 + ((ordinal // 5) % (PARTITION_COUNT - 1))
            expected_counts[destination] += 1
            if expected_first[destination] is None:
                expected_first[destination] = ordinal
            expected_last[destination] = ordinal

        observed_total = 0
        observed_payload_bytes = 0
        for partition, row in enumerate(partitions):
            if not isinstance(row, dict):
                raise GateError(f"{layout} partition row {partition} must be an object")
            _require_exact_keys(
                row, REPARTITION_PARTITION_FIELDS, f"{layout} partition row {partition}"
            )
            row_partition = _strict_int(
                row["partition"], f"{layout}.p{partition}.partition"
            )
            if row_partition != partition:
                raise GateError(f"{layout} partition rows are not canonical")
            count = _strict_int(row["message_count"], f"{layout}.p{partition}.messages")
            partition_payload_bytes = _strict_int(
                row["payload_bytes"], f"{layout}.p{partition}.payload_bytes"
            )
            first = _strict_int(
                row["first_global_ordinal"],
                f"{layout}.p{partition}.first_global_ordinal",
            )
            last = _strict_int(
                row["last_global_ordinal"],
                f"{layout}.p{partition}.last_global_ordinal",
            )
            observed_total += count
            observed_payload_bytes += partition_payload_bytes
            if count == 0:
                raise GateError(f"{layout} partition {partition} is empty")
            if (
                count != expected_counts[partition]
                or first != expected_first[partition]
                or last != expected_last[partition]
            ):
                raise GateError(
                    f"{layout} partition {partition} violates the specified ordinal mapping"
                )
        if observed_total != messages:
            raise GateError(f"{layout} partition counts do not sum to messages")
        if observed_payload_bytes != payload_bytes:
            raise GateError(f"{layout} partition payload bytes do not sum to payload_bytes")

    uniform = reports["uniform"]
    skew = reports["skew80-20"]
    for field in (
        "input",
        "topic",
        "compression",
        "messages",
        "payload_bytes",
        "input_manifest_sha256",
        "input_stream_sha256",
        "input_content_stream_sha256",
    ):
        if uniform[field] != skew[field]:
            raise GateError(f"uniform/skew source identity differs at {field}")
    if uniform["mapping_sha256"] == skew["mapping_sha256"]:
        raise GateError("uniform and skew mapping fingerprints unexpectedly match")
    return {
        "schema": REPARTITION_SCHEMA,
        "messages": messages,
        "partition_count": PARTITION_COUNT,
        "input_manifest_sha256": uniform["input_manifest_sha256"],
        "input_stream_sha256": uniform["input_stream_sha256"],
        "content_stream_sha256": uniform["input_content_stream_sha256"],
        "payload_bytes": uniform["payload_bytes"],
        "uniform_mapping_sha256": uniform["mapping_sha256"],
        "skew_mapping_sha256": skew["mapping_sha256"],
        "verified": True,
    }


def gate_repartition_repeat(first_path: Path, second_path: Path) -> dict[str, Any]:
    first = _read_json(first_path)
    second = _read_json(second_path)
    identity_fields = {
        "schema",
        "input",
        "layout",
        "mapping_spec",
        "partition_count",
        "max_messages",
        "topic",
        "compression",
        "messages",
        "payload_bytes",
        "partitions",
        "input_manifest_sha256",
        "output_manifest_sha256",
        "input_stream_sha256",
        "output_stream_sha256",
        "input_content_stream_sha256",
        "output_content_stream_sha256",
        "content_streams_equal",
        "mapping_sha256",
        "output_tree_sha256",
        "reopened_verification",
    }
    _require_exact_keys(first, identity_fields | {"output"}, "first repeat report")
    _require_exact_keys(second, identity_fields | {"output"}, "second repeat report")
    for field in identity_fields:
        if first[field] != second[field]:
            raise GateError(f"repeated repartition differs at {field}")
    if first["output"] == second["output"]:
        raise GateError("repeated repartitions must use different fresh output paths")
    if first["reopened_verification"] is not True:
        raise GateError("repeated repartitions lack reopened verification")
    return {
        "schema": REPARTITION_SCHEMA,
        "layout": first["layout"],
        "messages": first["messages"],
        "output_tree_sha256": first["output_tree_sha256"],
        "mapping_sha256": first["mapping_sha256"],
        "relative_names_lengths_and_file_bytes_deterministic": True,
    }


def _storage_report(path: Path, expected_samples: int) -> dict[str, Any]:
    report = _read_json(path)
    _require_exact_keys(report, STORAGE_REPORT_FIELDS, f"storage report {path}")
    if report["schema_version"] != 8:
        raise GateError(f"storage report must verify schema 8: {path}")
    if report["footer_validation_enabled"] is not True:
        raise GateError(f"storage report lacks footer validation: {path}")
    if report["series_sample_per_segment"] is not None:
        raise GateError(f"storage report is not an exhaustive series decode: {path}")
    fingerprint = report["verified_selection_fingerprint"]
    if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise GateError(f"storage selection fingerprint is invalid: {path}")
    semantic_fingerprint = report["decoded_semantic_fingerprint"]
    if not isinstance(semantic_fingerprint, str) or not re.fullmatch(
        r"[0-9a-f]{64}", semantic_fingerprint
    ):
        raise GateError(f"decoded semantic fingerprint is invalid: {path}")
    topology_independent_fingerprint = report[
        "topology_independent_decoded_semantic_fingerprint"
    ]
    if not isinstance(topology_independent_fingerprint, str) or not re.fullmatch(
        r"[0-9a-f]{64}", topology_independent_fingerprint
    ):
        raise GateError(
            f"topology-independent decoded semantic fingerprint is invalid: {path}"
        )

    integers = {
        field: _strict_int(report[field], f"{path}.{field}")
        for field in STORAGE_INTEGER_FIELDS
    }
    if integers["segments"] == 0 or integers["series"] == 0 or integers["chunks"] == 0:
        raise GateError(f"storage report unexpectedly verifies an empty corpus: {path}")
    if integers["series"] != integers["corpus_series"]:
        raise GateError(f"storage report did not decode every corpus series: {path}")
    if integers["samples"] != expected_samples:
        raise GateError(
            f"storage sample count mismatch in {path}: "
            f"expected {expected_samples}, got {integers['samples']}"
        )
    if integers["logical_chunk_bytes"] == 0:
        raise GateError(f"storage report has no logical chunk bytes: {path}")
    if integers["metadata_read_calls"] == 0 or integers["metadata_read_bytes"] == 0:
        raise GateError(f"storage report lacks metadata read evidence: {path}")

    chunks_by_kind = report["chunks_by_kind"]
    if not isinstance(chunks_by_kind, list) or len(chunks_by_kind) != 5:
        raise GateError(f"storage report chunks_by_kind must contain five lanes: {path}")
    parsed_chunks_by_kind = [
        _strict_int(value, f"{path}.chunks_by_kind[{index}]")
        for index, value in enumerate(chunks_by_kind)
    ]
    if sum(parsed_chunks_by_kind) != integers["chunks"]:
        raise GateError(f"storage report chunk-kind counts do not sum to chunks: {path}")

    exact = report["exact_postings"]
    if not isinstance(exact, dict):
        raise GateError(f"storage report lacks exhaustive exact-postings evidence: {path}")
    _require_exact_keys(exact, EXACT_POSTINGS_FIELDS, f"exact postings {path}")
    exact_fingerprint = exact["logical_fingerprint"]
    if not isinstance(exact_fingerprint, str) or not re.fullmatch(
        r"[0-9a-f]{64}", exact_fingerprint
    ):
        raise GateError(f"exact-postings fingerprint is invalid: {path}")
    parsed_exact = {
        field: _strict_int(exact[field], f"{path}.exact_postings.{field}")
        for field in ("lists", "decoded_refs", "encoded_bytes")
    }
    if not all(parsed_exact.values()):
        raise GateError(f"storage report has empty exact-postings evidence: {path}")

    return {
        "verified_selection_fingerprint": fingerprint,
        "decoded_semantic_fingerprint": semantic_fingerprint,
        "topology_independent_decoded_semantic_fingerprint": (
            topology_independent_fingerprint
        ),
        "segments": integers["segments"],
        "corpus_series": integers["corpus_series"],
        "chunks": integers["chunks"],
        "chunks_by_kind": parsed_chunks_by_kind,
        "samples": integers["samples"],
        "logical_chunk_bytes": integers["logical_chunk_bytes"],
        "exact_postings": {
            "logical_fingerprint": exact_fingerprint,
            **parsed_exact,
        },
    }


def gate_storage_validation(
    uniform_path: Path, skew_path: Path, expectations_path: Path
) -> dict[str, Any]:
    expectations = _read_json(expectations_path)
    if expectations.get("schema") != PHASE1_EXPECTATIONS_SCHEMA:
        raise GateError("unsupported Phase 1 expectations schema")
    storage_expectations = expectations.get("storage_verifier")
    if not isinstance(storage_expectations, dict):
        raise GateError("Phase 1 expectations lack storage_verifier")
    expected_samples = _strict_int(
        storage_expectations.get("samples"), "storage_verifier.samples"
    )
    if expected_samples == 0:
        raise GateError("expected storage sample count must be positive")

    uniform = _storage_report(uniform_path, expected_samples)
    skew = _storage_report(skew_path, expected_samples)
    if (
        uniform["topology_independent_decoded_semantic_fingerprint"]
        != skew["topology_independent_decoded_semantic_fingerprint"]
    ):
        raise GateError(
            "uniform/skew topology-independent decoded semantic fingerprints differ"
        )
    return {
        "schema": STORAGE_VALIDATION_SCHEMA,
        "expected_samples": expected_samples,
        "logical_sample_count_equal": uniform["samples"] == skew["samples"],
        "topology_independent_decoded_semantics_equal": True,
        "cross_topology_duplicate_winner_equivalence_claimed": False,
        "ordered_decoded_semantic_identity_cross_topology_required": False,
        "physical_layout_identity_required": False,
        "uniform": uniform,
        "skew80-20": skew,
    }


def _positive_number(value: Any, label: str) -> float:
    if isinstance(value, str) and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value):
        parsed = float(value)
    elif not isinstance(value, bool) and isinstance(value, (int, float)):
        parsed = float(value)
    else:
        raise GateError(f"{label} must be numeric")
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise GateError(f"{label} must be finite and positive")
    return parsed


def _run_measurement(result_dir: Path, run: str) -> dict[str, float]:
    root = result_dir / "runs" / run
    perf = _read_json(root / "perf-stat.json")
    events = perf.get("events")
    if not isinstance(events, list):
        raise GateError(f"{run} perf report lacks events")
    task_rows = [
        row
        for row in events
        if isinstance(row, dict) and row.get("event") == "task-clock"
    ]
    if len(task_rows) != 1 or task_rows[0].get("available") is not True:
        raise GateError(f"{run} requires one available task-clock event")
    task_clock = _positive_number(task_rows[0].get("raw_value"), f"{run}.task-clock")
    task_unit = task_rows[0].get("unit")
    if task_unit not in {"msec", "milliseconds"}:
        raise GateError(f"{run} task-clock unit is unsupported: {task_unit!r}")

    timing = _read_json(root / "replay.time.json")
    rss = _read_json(root / "rss-summary.json")
    time_rss = _positive_number(timing.get("max_rss_kib"), f"{run}.time.max_rss_kib")
    tree_rss = _positive_number(
        rss.get("aggregate_rss_kib"), f"{run}.rss.aggregate_rss_kib"
    )
    return {
        "task_clock_msec": task_clock,
        # The process tree sampler catches descendants while GNU time remains
        # an independent check; use the conservative maximum.
        "peak_rss_kib": max(time_rss, tree_rss),
    }


def _directional_factor(
    task_ratios: dict[str, float], rss_ratios: dict[str, float]
) -> dict[str, Any]:
    task_values = list(task_ratios.values())
    rss_values = list(rss_ratios.values())
    if len(task_values) != 2 or len(rss_values) != 2:
        raise GateError("each factorial main effect requires exactly two conditional contrasts")
    task_geomean = math.sqrt(task_values[0] * task_values[1])
    rss_geomean = math.sqrt(rss_values[0] * rss_values[1])
    directionally_better = (
        task_geomean <= PROMOTION_TASK_CLOCK_GEOMEAN_MAX
        and max(task_values) <= PROMOTION_TASK_CLOCK_PAIR_MAX
        and rss_geomean <= PROMOTION_RSS_GEOMEAN_MAX
        and max(rss_values) <= PROMOTION_RSS_PAIR_MAX
    )
    directionally_worse = (
        task_geomean >= REJECTION_TASK_CLOCK_GEOMEAN_MIN
        or max(task_values) >= REJECTION_TASK_CLOCK_PAIR_MIN
        or rss_geomean >= REJECTION_RSS_GEOMEAN_MIN
        or max(rss_values) >= REJECTION_RSS_PAIR_MIN
    )
    direction = (
        "directionally_better"
        if directionally_better
        else ("directionally_worse" if directionally_worse else "inconclusive")
    )
    return {
        "direction": direction,
        "conditional_adaptive_over_plain": {
            "task_clock": task_ratios,
            "peak_rss": rss_ratios,
        },
        "geometric_mean_adaptive_over_plain": {
            "task_clock": task_geomean,
            "peak_rss": rss_geomean,
        },
    }


def _classify_topology(result_dir: Path, topology: str) -> dict[str, Any]:
    measurements = {
        cell: _run_measurement(result_dir, f"{topology}-{cell}-01")
        for cell in CELL_FACTORS
    }

    def ratio(numerator: str, denominator: str, field: str) -> float:
        return measurements[numerator][field] / measurements[denominator][field]

    series = _directional_factor(
        {
            "last_plain": ratio("ap", "pp", "task_clock_msec"),
            "last_adaptive": ratio("aa", "pa", "task_clock_msec"),
        },
        {
            "last_plain": ratio("ap", "pp", "peak_rss_kib"),
            "last_adaptive": ratio("aa", "pa", "peak_rss_kib"),
        },
    )
    last = _directional_factor(
        {
            "series_plain": ratio("pa", "pp", "task_clock_msec"),
            "series_adaptive": ratio("aa", "ap", "task_clock_msec"),
        },
        {
            "series_plain": ratio("pa", "pp", "peak_rss_kib"),
            "series_adaptive": ratio("aa", "ap", "peak_rss_kib"),
        },
    )
    interaction = {
        "task_clock_ratio_of_ratios": (
            measurements["aa"]["task_clock_msec"]
            * measurements["pp"]["task_clock_msec"]
            / (
                measurements["ap"]["task_clock_msec"]
                * measurements["pa"]["task_clock_msec"]
            )
        ),
        "peak_rss_ratio_of_ratios": (
            measurements["aa"]["peak_rss_kib"]
            * measurements["pp"]["peak_rss_kib"]
            / (
                measurements["ap"]["peak_rss_kib"]
                * measurements["pa"]["peak_rss_kib"]
            )
        ),
    }
    return {
        "measurements": measurements,
        "factors": {
            "adaptive_series_table": series,
            "adaptive_last_timestamp_table": last,
        },
        "interaction": interaction,
    }


def gate_performance(result_dir: Path) -> dict[str, Any]:
    validate_run_plan(result_dir, result_dir / "run-plan.tsv")

    topologies = {
        topology: _classify_topology(result_dir, topology)
        for topology in ("uniform", "skew80-20")
    }
    factor_directions: dict[str, str] = {}
    for factor in ("adaptive_series_table", "adaptive_last_timestamp_table"):
        directions = {
            topologies[topology]["factors"][factor]["direction"]
            for topology in TOPOLOGIES
        }
        factor_directions[factor] = (
            "directionally_better"
            if directions == {"directionally_better"}
            else (
                "directionally_worse"
                if "directionally_worse" in directions
                else "inconclusive"
            )
        )
    return {
        "schema": PERFORMANCE_SCHEMA,
        # One observation per cell estimates isolated main effects and their
        # interaction, but does not estimate same-cell replay variance. It is
        # intentionally impossible for this eight-replay screen to promote a
        # production default.
        "overall_disposition": "defer",
        "promotion_eligible": False,
        "non_promotion_reason": "one_unreplicated_observation_per_factorial_cell",
        "production_default_conclusion": "no_change",
        "factor_directions": factor_directions,
        "thresholds": {
            "promotion_task_clock_geomean_max": PROMOTION_TASK_CLOCK_GEOMEAN_MAX,
            "promotion_task_clock_pair_max": PROMOTION_TASK_CLOCK_PAIR_MAX,
            "promotion_rss_geomean_max": PROMOTION_RSS_GEOMEAN_MAX,
            "promotion_rss_pair_max": PROMOTION_RSS_PAIR_MAX,
            "rejection_task_clock_geomean_min": REJECTION_TASK_CLOCK_GEOMEAN_MIN,
            "rejection_task_clock_pair_min": REJECTION_TASK_CLOCK_PAIR_MIN,
            "rejection_rss_geomean_min": REJECTION_RSS_GEOMEAN_MIN,
            "rejection_rss_pair_min": REJECTION_RSS_PAIR_MIN,
        },
        "topologies": topologies,
    }


def _parse_table(lines: list[str], start: int, expected_heading: str) -> tuple[dict[str, str], int]:
    if lines[start].strip() != expected_heading:
        raise GateError(f"expected heading {expected_heading!r}")
    index = start + 1
    while index < len(lines) and not lines[index].startswith("#### "):
        index += 1
    values: dict[str, str] = {}
    row = re.compile(r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|$")
    for line in lines[start + 1 : index]:
        match = row.match(line.strip())
        if not match:
            continue
        key, value = match.groups()
        if key in {"Metric", "---"} or set(key) == {"-"}:
            continue
        if key in values:
            raise GateError(f"duplicate {expected_heading} metric: {key}")
        values[key] = value
    return values, index


def _parse_integer_table(
    raw: dict[str, str], integer_fields: set[str], float_field: str, label: str
) -> dict[str, Any]:
    expected = integer_fields | {float_field}
    _require_exact_keys(raw, expected, label)
    parsed: dict[str, Any] = {}
    for field in integer_fields:
        if not re.fullmatch(r"[0-9]+", raw[field]):
            raise GateError(f"{label}.{field} is not an integer")
        parsed[field] = int(raw[field])
    try:
        ratio = float(raw[float_field])
    except ValueError as error:
        raise GateError(f"{label}.{float_field} is not a float") from error
    if not 0.0 <= ratio <= 1.0:
        raise GateError(f"{label}.{float_field} is outside [0,1]")
    parsed[float_field] = ratio
    return parsed


def parse_head_report(report: Path) -> dict[str, Any]:
    lines = report.read_text(encoding="utf-8").splitlines()
    try:
        section_start = lines.index("## Head Buffer Stats (by partition)") + 1
    except ValueError as error:
        raise GateError("report lacks Head Buffer Stats section") from error
    section_end = next(
        (index for index in range(section_start, len(lines)) if lines[index].startswith("## ")),
        len(lines),
    )
    lines = lines[section_start:section_end]
    partition_heading = re.compile(r"^### Partition (.+)$")
    partitions: dict[str, dict[str, Any]] = {}
    index = 0
    while index < len(lines):
        match = partition_heading.match(lines[index])
        if not match:
            index += 1
            continue
        partition = match.group(1)
        if partition in partitions:
            raise GateError(f"duplicate partition section: {partition}")
        next_partition = next(
            (
                candidate
                for candidate in range(index + 1, len(lines))
                if partition_heading.match(lines[candidate])
            ),
            len(lines),
        )
        block = lines[index + 1 : next_partition]
        try:
            series_heading = block.index("#### Series Table Structure")
            last_heading = block.index("#### Last Timestamp Table Structure")
        except ValueError as error:
            raise GateError(f"partition {partition} lacks structural tables") from error
        series_raw, _ = _parse_table(block, series_heading, "#### Series Table Structure")
        last_raw, _ = _parse_table(
            block, last_heading, "#### Last Timestamp Table Structure"
        )
        series = _parse_integer_table(
            series_raw, SERIES_INTEGER_FIELDS, "direct_series_ratio", f"{partition}.series"
        )
        last_adaptive = last_raw.pop("adaptive", None)
        if last_adaptive not in {"true", "false"}:
            raise GateError(f"{partition}.last.adaptive must be true or false")
        last = _parse_integer_table(
            last_raw, LAST_INTEGER_FIELDS, "dense_series_ratio", f"{partition}.last"
        )
        last["adaptive"] = last_adaptive == "true"
        if series["windows"] != (
            series["in_order_windows"] + series["out_of_order_windows"]
        ):
            raise GateError(f"{partition}.series window lane counts do not sum")
        if series["in_order_rotations"] > series["in_order_windows"]:
            raise GateError(f"{partition}.series rotations exceed in-order windows")
        if series["adaptive_windows"] > series["windows"]:
            raise GateError(f"{partition}.series adaptive window count is impossible")
        if series["direct_series_total"] + series["sparse_series_total"] != series[
            "series_total"
        ]:
            raise GateError(f"{partition}.series direct+sparse does not equal series")
        if series["refs_above_paged_limit_total"] > series["sparse_series_total"]:
            raise GateError(f"{partition}.series high refs exceed sparse series")
        if series["max_page_directory_len"] > series["max_page_directory_capacity"]:
            raise GateError(f"{partition}.series page directory length exceeds capacity")
        direct_slot_bytes = series["max_direct_slot_index_bytes"]
        if direct_slot_bytes % 8192 != 0 or direct_slot_bytes > (
            series["direct_pages_total"] * 8192
        ):
            raise GateError(f"{partition}.series direct slot bytes disagree with pages")
        if (series["direct_pages_total"] == 0) != (
            series["direct_series_total"] == 0
        ):
            raise GateError(f"{partition}.series direct page/series presence differs")
        if (series["direct_pages_total"] == 0) != (direct_slot_bytes == 0):
            raise GateError(f"{partition}.series direct page/slot presence differs")
        _require_ratio(
            series["direct_series_ratio"],
            series["direct_series_total"],
            series["series_total"],
            f"{partition}.series.direct_series_ratio",
        )
        if last["dense_series"] + last["sparse_series"] != last["series"]:
            raise GateError(f"{partition}.last dense+sparse does not equal series")
        if last["refs_above_paged_limit"] > last["sparse_series"]:
            raise GateError(f"{partition}.last high refs exceed sparse series")
        if last["page_directory_len"] > last["page_directory_capacity"]:
            raise GateError(f"{partition}.last page directory length exceeds capacity")
        _require_ratio(
            last["dense_series_ratio"],
            last["dense_series"],
            last["series"],
            f"{partition}.last.dense_series_ratio",
        )
        if not last["adaptive"] and any(
            last[field]
            for field in (
                "dense_pages",
                "dense_series",
                "page_directory_len",
                "page_directory_capacity",
                "paged_allocated_bytes",
            )
        ):
            raise GateError(f"{partition}.last plain table has adaptive allocation")
        partitions[partition] = {"series_table": series, "last_timestamp_table": last}
        index = next_partition
    if len(partitions) != PARTITION_COUNT:
        raise GateError(
            f"head report must contain exactly {PARTITION_COUNT} partitions; got {len(partitions)}"
        )
    parsed_partition_ids: list[tuple[str, int]] = []
    for partition in partitions:
        topic, separator, suffix = partition.rpartition(":")
        if not separator or not topic or not re.fullmatch(r"[0-9]+", suffix):
            raise GateError(f"partition identity is not topic:n: {partition}")
        parsed_partition_ids.append((topic, int(suffix)))
    topics = {topic for topic, _ in parsed_partition_ids}
    suffixes = sorted(suffix for _, suffix in parsed_partition_ids)
    if len(topics) != 1 or suffixes != list(range(PARTITION_COUNT)):
        raise GateError("head report must contain one topic with partitions 0..15 exactly")
    return {"schema": STRUCTURE_SCHEMA, "report": str(report.resolve()), "partitions": partitions}


def _validate_series_factor(
    topology: str, partition: str, series: dict[str, Any], adaptive: bool
) -> None:
    if adaptive:
        if series["adaptive_windows"] != series["windows"]:
            raise GateError(
                f"{topology}:{partition}: adaptive series comparator was not effective"
            )
        return
    if series["adaptive_windows"] != 0:
        raise GateError(f"{topology}:{partition}: plain series table was adaptive")
    if series["direct_pages_total"] != 0 or series["direct_series_total"] != 0:
        raise GateError(f"{topology}:{partition}: plain series table used direct pages")
    if any(
        series[field]
        for field in (
            "sparse_pages_total",
            "max_page_directory_len",
            "max_page_directory_capacity",
            "max_sparse_slot_capacity",
            "max_direct_slot_index_bytes",
            "max_direct_reverse_slot_capacity",
            "max_direct_value_capacity",
        )
    ):
        raise GateError(f"{topology}:{partition}: plain series table has page allocation")
    if series["sparse_series_total"] != series["series_total"]:
        raise GateError(f"{topology}:{partition}: plain series table lost sparse series")


def _validate_last_factor(
    topology: str, partition: str, last: dict[str, Any], adaptive: bool
) -> None:
    if adaptive:
        if not last["adaptive"]:
            raise GateError(
                f"{topology}:{partition}: adaptive timestamp comparator was not effective"
            )
        return
    if last["adaptive"] or any(
        last[field]
        for field in (
            "dense_pages",
            "dense_series",
            "sparse_pages",
            "page_directory_len",
            "page_directory_capacity",
            "paged_allocated_bytes",
        )
    ):
        raise GateError(
            f"{topology}:{partition}: plain timestamp table has page allocation"
        )


def _gate_factorial_topology(
    topology: str, cells: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    if set(cells) != set(CELL_FACTORS):
        raise GateError(f"{topology}: factorial cell set differs")
    for cell, document in cells.items():
        if document.get("schema") != STRUCTURE_SCHEMA:
            raise GateError(f"{topology}:{cell}: unexpected structure schema")
        if not isinstance(document.get("partitions"), dict):
            raise GateError(f"{topology}:{cell}: structure partitions are invalid")
    baseline_partitions = cells["pp"]["partitions"]
    if any(set(document["partitions"]) != set(baseline_partitions) for document in cells.values()):
        raise GateError(f"{topology}: factorial partition identities differ")
    totals = {
        "partitions": len(baseline_partitions),
        "windows": 0,
        "in_order_windows": 0,
        "in_order_rotations": 0,
        "out_of_order_windows": 0,
        "adaptive_direct_pages": 0,
        "adaptive_sparse_pages": 0,
        "adaptive_last_dense_pages": 0,
        "adaptive_last_sparse_pages": 0,
        "factor_isolation_exact": True,
    }
    for partition in sorted(baseline_partitions):
        rows = {cell: cells[cell]["partitions"][partition] for cell in CELL_FACTORS}
        for cell, (series_adaptive, last_adaptive) in CELL_FACTORS.items():
            _validate_series_factor(
                topology, partition, rows[cell]["series_table"], series_adaptive
            )
            _validate_last_factor(
                topology,
                partition,
                rows[cell]["last_timestamp_table"],
                last_adaptive,
            )
        baseline_series = rows["pp"]["series_table"]
        baseline_last = rows["pp"]["last_timestamp_table"]
        for cell, row in rows.items():
            for field in PAIR_SERIES_IDENTITY_FIELDS:
                if row["series_table"][field] != baseline_series[field]:
                    raise GateError(
                        f"{topology}:{partition}:{cell}: series work differs at {field}"
                    )
            for field in PAIR_LAST_IDENTITY_FIELDS:
                if row["last_timestamp_table"][field] != baseline_last[field]:
                    raise GateError(
                        f"{topology}:{partition}:{cell}: timestamp work differs at {field}"
                    )
        # Changing the other factor must leave this factor's complete parsed
        # structure snapshot value-for-value equivalent.
        if rows["pp"]["series_table"] != rows["pa"]["series_table"]:
            raise GateError(f"{topology}:{partition}: last factor perturbed plain series table")
        if rows["ap"]["series_table"] != rows["aa"]["series_table"]:
            raise GateError(f"{topology}:{partition}: last factor perturbed adaptive series table")
        if rows["pp"]["last_timestamp_table"] != rows["ap"]["last_timestamp_table"]:
            raise GateError(
                f"{topology}:{partition}: series factor perturbed plain timestamp table"
            )
        if rows["pa"]["last_timestamp_table"] != rows["aa"]["last_timestamp_table"]:
            raise GateError(
                f"{topology}:{partition}: series factor perturbed adaptive timestamp table"
            )
        adaptive_series = rows["aa"]["series_table"]
        adaptive_last = rows["aa"]["last_timestamp_table"]
        if adaptive_series["series_total"] == 0 or adaptive_last["series"] == 0:
            raise GateError(f"{topology}:{partition}: partition observed no series")
        if adaptive_series["in_order_rotations"] < 2:
            raise GateError(f"{topology}:{partition}: long-lived rotation coverage is absent")
        partition_number = int(partition.rpartition(":")[2])
        if topology == "uniform":
            if (
                adaptive_series["sparse_pages_total"] == 0
                or adaptive_series["sparse_series_total"] == 0
                or adaptive_last["sparse_pages"] == 0
                or adaptive_last["sparse_series"] == 0
            ):
                raise GateError(
                    f"{topology}:{partition}: uniform partition lacks sparse coverage"
                )
        elif partition_number == 0:
            if (
                adaptive_series["direct_pages_total"] == 0
                or adaptive_series["direct_series_total"] == 0
                or adaptive_last["dense_pages"] == 0
                or adaptive_last["dense_series"] == 0
            ):
                raise GateError(f"{topology}:{partition}: hot partition did not promote")
            if (
                adaptive_series["sparse_pages_total"] == 0
                or adaptive_series["sparse_series_total"] == 0
                or adaptive_last["sparse_pages"] == 0
                or adaptive_last["sparse_series"] == 0
            ):
                raise GateError(f"{topology}:{partition}: hot partition lacks sparse residue")
        elif (
            adaptive_series["sparse_pages_total"] == 0
            or adaptive_series["sparse_series_total"] == 0
            or adaptive_last["sparse_pages"] == 0
            or adaptive_last["sparse_series"] == 0
        ):
            raise GateError(f"{topology}:{partition}: cold partition lacks sparse coverage")
        totals["windows"] += adaptive_series["windows"]
        totals["in_order_windows"] += adaptive_series["in_order_windows"]
        totals["in_order_rotations"] += adaptive_series["in_order_rotations"]
        totals["out_of_order_windows"] += adaptive_series["out_of_order_windows"]
        totals["adaptive_direct_pages"] += adaptive_series["direct_pages_total"]
        totals["adaptive_sparse_pages"] += adaptive_series["sparse_pages_total"]
        totals["adaptive_last_dense_pages"] += adaptive_last["dense_pages"]
        totals["adaptive_last_sparse_pages"] += adaptive_last["sparse_pages"]
    if totals["out_of_order_windows"] == 0:
        raise GateError(f"{topology}: OOO lane coverage is absent")
    return totals


def gate_structure_repeat(first_path: Path, second_path: Path) -> dict[str, Any]:
    first = _read_json(first_path)
    second = _read_json(second_path)
    if first.get("schema") != STRUCTURE_SCHEMA or second.get("schema") != STRUCTURE_SCHEMA:
        raise GateError("repeated structure report schema differs")
    if first.get("report") == second.get("report"):
        raise GateError("repeated structure reports must come from different run paths")
    if first.get("partitions") != second.get("partitions"):
        raise GateError("repeated structure telemetry differs")
    return {
        "schema": STRUCTURE_SCHEMA,
        "partition_count": len(first["partitions"]),
        "structure_deterministic": True,
    }


def gate_matrix(
    uniform_pp_path: Path,
    uniform_ap_path: Path,
    uniform_pa_path: Path,
    uniform_aa_path: Path,
    skew_pp_path: Path,
    skew_ap_path: Path,
    skew_pa_path: Path,
    skew_aa_path: Path,
) -> dict[str, Any]:
    uniform = _gate_factorial_topology(
        "uniform",
        {
            "pp": _read_json(uniform_pp_path),
            "ap": _read_json(uniform_ap_path),
            "pa": _read_json(uniform_pa_path),
            "aa": _read_json(uniform_aa_path),
        },
    )
    skew = _gate_factorial_topology(
        "skew80-20",
        {
            "pp": _read_json(skew_pp_path),
            "ap": _read_json(skew_ap_path),
            "pa": _read_json(skew_pa_path),
            "aa": _read_json(skew_aa_path),
        },
    )
    if uniform["adaptive_sparse_pages"] == 0 or uniform["adaptive_last_sparse_pages"] == 0:
        raise GateError("uniform topology did not exercise strided sparse pages")
    if skew["adaptive_direct_pages"] == 0 or skew["adaptive_last_dense_pages"] == 0:
        raise GateError("skew topology did not cross both adaptive promotion thresholds")
    if skew["adaptive_sparse_pages"] == 0 or skew["adaptive_last_sparse_pages"] == 0:
        raise GateError("skew topology lacks residual sparse-page coverage")
    return {
        "schema": SUMMARY_SCHEMA,
        "partition_count": PARTITION_COUNT,
        "uniform": uniform,
        "skew80-20": skew,
        "coverage": {
            "strided_sparse_pages": True,
            "series_table_promotion": True,
            "last_timestamp_promotion": True,
            "long_lived_rotations": True,
            "out_of_order_lanes": True,
            "factorial_work_identity": True,
            "cross_factor_structure_isolation": True,
        },
    }


def _set_assignment(
    lines: list[str], section: str, key: str, value: str, *, insert_if_missing: bool = False
) -> None:
    section_pattern = re.compile(r"^\s*\[([^]]+)]\s*(?:#.*)?$")
    key_pattern = re.compile(rf"^\s*{re.escape(key)}\s*=")
    active = ""
    matches: list[int] = []
    section_end: int | None = None
    found_section = False
    for index, line in enumerate(lines):
        match = section_pattern.match(line.rstrip("\n"))
        if match:
            if active == section and section_end is None:
                section_end = index
            active = match.group(1).strip()
            found_section = found_section or active == section
            continue
        if active == section and key_pattern.match(line):
            matches.append(index)
    if active == section and section_end is None:
        section_end = len(lines)
    if len(matches) == 1:
        newline = "\n" if lines[matches[0]].endswith("\n") else ""
        lines[matches[0]] = f"{key} = {value}{newline}"
        return
    if matches:
        raise GateError(f"template contains duplicate {section}.{key}")
    if not insert_if_missing:
        raise GateError(f"template lacks required {section}.{key}")
    if not found_section or section_end is None:
        raise GateError(f"template lacks required section [{section}]")
    lines.insert(section_end, f"{key} = {value}\n")


def render_config(
    template: Path,
    output: Path,
    capture: Path,
    segments_dir: Path,
    messages: int,
    series_adaptive: bool,
    last_adaptive: bool,
) -> dict[str, Any]:
    if output.exists() or segments_dir.exists():
        raise GateError("config and segment output paths must be new")
    lines = template.read_text(encoding="utf-8").splitlines(keepends=True)
    _set_assignment(lines, "ingestion", "replay_from", json.dumps(str(capture)))
    _set_assignment(lines, "ingestion", "stop_after_messages", str(messages))
    _set_assignment(
        lines, "ingestion.segment_writer", "segments_dir", json.dumps(str(segments_dir))
    )
    series_bool = "true" if series_adaptive else "false"
    last_bool = "true" if last_adaptive else "false"
    _set_assignment(
        lines, "ingestion.head_buffer", "adaptive_series_table", series_bool
    )
    _set_assignment(
        lines,
        "ingestion.head_buffer",
        "adaptive_last_timestamp_table",
        last_bool,
        insert_if_missing=True,
    )
    with output.open("x", encoding="utf-8") as destination:
        destination.write("".join(lines))
    with output.open("rb") as source:
        parsed = tomllib.load(source)
    ingestion = parsed.get("ingestion", {})
    head = ingestion.get("head_buffer", {})
    writer = ingestion.get("segment_writer", {})
    if (
        ingestion.get("replay_from") != str(capture)
        or ingestion.get("stop_after_messages") != messages
        or writer.get("segments_dir") != str(segments_dir)
        or head.get("adaptive_series_table") is not series_adaptive
        or head.get("adaptive_last_timestamp_table") is not last_adaptive
    ):
        raise GateError("rendered configuration failed round-trip validation")
    return {
        "config": str(output.resolve()),
        "capture": str(capture),
        "segments_dir": str(segments_dir),
        "messages": messages,
        "adaptive_series_table": series_adaptive,
        "adaptive_last_timestamp_table": last_adaptive,
        "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    source = commands.add_parser("source-seal")
    source.add_argument("--repo", type=Path, required=True)
    source.add_argument("--output", type=Path, required=True)
    check_source = commands.add_parser("check-source-seal")
    check_source.add_argument("--repo", type=Path, required=True)
    check_source.add_argument("--seal", type=Path, required=True)
    extract_archive = commands.add_parser("extract-source-archive")
    extract_archive.add_argument("--repo", type=Path, required=True)
    extract_archive.add_argument("--archive", type=Path, required=True)
    extract_archive.add_argument("--destination", type=Path, required=True)
    extract_archive.add_argument("--output", type=Path, required=True)
    check_archive = commands.add_parser("check-source-archive-seal")
    check_archive.add_argument("--repo", type=Path, required=True)
    check_archive.add_argument("--archive", type=Path, required=True)
    check_archive.add_argument("--seal", type=Path, required=True)
    snapshot = commands.add_parser("source-snapshot-seal")
    snapshot.add_argument("--repo", type=Path, required=True)
    snapshot.add_argument("--snapshot", type=Path, required=True)
    snapshot.add_argument("--output", type=Path, required=True)
    check_snapshot = commands.add_parser("check-source-snapshot-seal")
    check_snapshot.add_argument("--repo", type=Path, required=True)
    check_snapshot.add_argument("--snapshot", type=Path, required=True)
    check_snapshot.add_argument("--seal", type=Path, required=True)
    frozen_harness = commands.add_parser("check-frozen-harness")
    frozen_harness.add_argument("--harness", type=Path, required=True)
    commands.add_parser("check-ambient-env")
    runtime = commands.add_parser("runtime-identity")
    runtime.add_argument("--binary", type=Path, required=True)
    runtime.add_argument(
        "--role", choices=("ingester", "repartition", "query", "verifier"), required=True
    )
    runtime.add_argument("--env", action="append", default=[])
    runtime.add_argument("--arg", action="append", default=[])
    runtime.add_argument("--output", type=Path, required=True)
    inventory = commands.add_parser("capture-inventory")
    inventory.add_argument("--capture", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--paths-output", type=Path, required=True)
    process_snapshot = commands.add_parser("validate-process-snapshot")
    process_snapshot.add_argument("--snapshot", type=Path, required=True)
    run_plan = commands.add_parser("validate-run-plan")
    run_plan.add_argument("--result-dir", type=Path, required=True)
    run_plan.add_argument("--plan", type=Path, required=True)
    run_plan.add_argument("--output", type=Path, required=True)
    replay_summary = commands.add_parser("validate-replay-summary")
    replay_summary.add_argument("--summary", type=Path, required=True)
    replay_summary.add_argument("--output", type=Path, required=True)
    final_seal = commands.add_parser("validate-final-seal")
    final_seal.add_argument("--result-dir", type=Path, required=True)
    final_seal.add_argument(
        "--stage", choices=("evidence", "complete"), default="complete"
    )
    repartition = commands.add_parser("gate-repartition")
    repartition.add_argument("--uniform", type=Path, required=True)
    repartition.add_argument("--skew", type=Path, required=True)
    repartition.add_argument("--messages", type=int, required=True)
    repartition.add_argument("--output", type=Path, required=True)
    repeat = commands.add_parser("gate-repartition-repeat")
    repeat.add_argument("--first", type=Path, required=True)
    repeat.add_argument("--second", type=Path, required=True)
    repeat.add_argument("--output", type=Path, required=True)
    parse = commands.add_parser("parse-head-report")
    parse.add_argument("--report", type=Path, required=True)
    parse.add_argument("--output", type=Path, required=True)
    matrix = commands.add_parser("gate-matrix")
    for topology in ("uniform", "skew"):
        for cell in CELL_FACTORS:
            matrix.add_argument(f"--{topology}-{cell}", type=Path, required=True)
    matrix.add_argument("--output", type=Path, required=True)
    repeat_structure = commands.add_parser("gate-structure-repeat")
    repeat_structure.add_argument("--first", type=Path, required=True)
    repeat_structure.add_argument("--second", type=Path, required=True)
    repeat_structure.add_argument("--output", type=Path, required=True)
    storage = commands.add_parser("gate-storage-validation")
    storage.add_argument("--uniform", type=Path, required=True)
    storage.add_argument("--skew", type=Path, required=True)
    storage.add_argument("--expectations", type=Path, required=True)
    storage.add_argument("--output", type=Path, required=True)
    performance = commands.add_parser("gate-performance")
    performance.add_argument("--result-dir", type=Path, required=True)
    performance.add_argument("--output", type=Path, required=True)
    render = commands.add_parser("render-config")
    render.add_argument("--template", type=Path, required=True)
    render.add_argument("--output", type=Path, required=True)
    render.add_argument("--capture", type=Path, required=True)
    render.add_argument("--segments-dir", type=Path, required=True)
    render.add_argument("--messages", type=int, required=True)
    render.add_argument("--series-mode", choices=("plain", "adaptive"), required=True)
    render.add_argument("--last-mode", choices=("plain", "adaptive"), required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "source-seal":
            _write_json_exclusive(args.output, source_seal(args.repo))
        elif args.command == "check-source-seal":
            print(json.dumps(check_source_seal(args.repo, args.seal), sort_keys=True))
        elif args.command == "extract-source-archive":
            _write_json_exclusive(
                args.output,
                extract_source_archive(args.repo, args.archive, args.destination),
            )
        elif args.command == "check-source-archive-seal":
            print(
                json.dumps(
                    check_source_archive_seal(args.repo, args.archive, args.seal),
                    sort_keys=True,
                )
            )
        elif args.command == "source-snapshot-seal":
            _write_json_exclusive(
                args.output, source_snapshot_seal(args.repo, args.snapshot)
            )
        elif args.command == "check-source-snapshot-seal":
            print(
                json.dumps(
                    check_source_snapshot_seal(args.repo, args.snapshot, args.seal),
                    sort_keys=True,
                )
            )
        elif args.command == "check-frozen-harness":
            print(json.dumps(check_frozen_harness(args.harness), sort_keys=True))
        elif args.command == "check-ambient-env":
            forbidden = forbidden_ambient_environment(dict(os.environ))
            if forbidden:
                raise GateError(
                    "forbidden ambient build/runtime environment variables: "
                    + ", ".join(forbidden)
                )
            print(json.dumps({"status": "pass", "forbidden": []}, sort_keys=True))
        elif args.command == "runtime-identity":
            _write_json_exclusive(
                args.output,
                runtime_identity(args.binary, args.role, args.env, args.arg),
            )
        elif args.command == "capture-inventory":
            capture_inventory(args.capture, args.output, args.paths_output)
        elif args.command == "validate-process-snapshot":
            print(json.dumps(validate_process_snapshot(args.snapshot), sort_keys=True))
        elif args.command == "validate-run-plan":
            _write_json_exclusive(
                args.output, validate_run_plan(args.result_dir, args.plan)
            )
        elif args.command == "validate-replay-summary":
            _write_json_exclusive(args.output, validate_replay_summary(args.summary))
        elif args.command == "validate-final-seal":
            print(
                json.dumps(
                    validate_final_seal(args.result_dir, args.stage), sort_keys=True
                )
            )
        elif args.command == "gate-repartition":
            value = gate_repartition(args.uniform, args.skew, args.messages)
            _write_json_exclusive(args.output, value)
        elif args.command == "gate-repartition-repeat":
            _write_json_exclusive(
                args.output, gate_repartition_repeat(args.first, args.second)
            )
        elif args.command == "parse-head-report":
            _write_json_exclusive(args.output, parse_head_report(args.report))
        elif args.command == "gate-matrix":
            value = gate_matrix(
                *(
                    getattr(args, f"{topology}_{cell}")
                    for topology in ("uniform", "skew")
                    for cell in CELL_FACTORS
                )
            )
            _write_json_exclusive(args.output, value)
        elif args.command == "gate-structure-repeat":
            _write_json_exclusive(
                args.output, gate_structure_repeat(args.first, args.second)
            )
        elif args.command == "gate-storage-validation":
            _write_json_exclusive(
                args.output,
                gate_storage_validation(args.uniform, args.skew, args.expectations),
            )
        elif args.command == "gate-performance":
            _write_json_exclusive(args.output, gate_performance(args.result_dir))
        elif args.command == "render-config":
            print(
                json.dumps(
                    render_config(
                        args.template,
                        args.output,
                        args.capture,
                        args.segments_dir,
                        args.messages,
                        args.series_mode == "adaptive",
                        args.last_mode == "adaptive",
                    ),
                    sort_keys=True,
                )
            )
        else:
            raise AssertionError(args.command)
    except (GateError, OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        print(f"Phase 5 head topology gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
