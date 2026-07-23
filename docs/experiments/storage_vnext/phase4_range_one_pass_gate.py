#!/usr/bin/env python3
"""Strict Phase 4 repeated versus one-pass-assume-scalar diagnostic gate."""

from __future__ import annotations

import argparse
import copy
import csv
import datetime as dt
import hashlib
import json
import math
import os
import re
import stat
import statistics
import subprocess
import sys
import tempfile
import types
from pathlib import Path, PurePosixPath
from typing import Any

def load_exact_source_sibling(name: str, filename: str) -> types.ModuleType:
    """Compile a sealed sibling's source bytes directly; never consult .pyc."""
    parent = Path(__file__).resolve(strict=True).parent
    path = parent / filename
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"required Python sibling is not an exact source file: {path}")
    module = types.ModuleType(name)
    module.__file__ = str(path)
    module.__package__ = None
    module.__cached__ = None
    sys.modules[name] = module
    exec(
        compile(path.read_bytes(), str(path), "exec", dont_inherit=True),
        module.__dict__,
    )
    return module


common = load_exact_source_sibling("schema7_query_ab_gate", "schema7_query_ab_gate.py")
_schema8 = load_exact_source_sibling("schema8_query_ab_gate", "schema8_query_ab_gate.py")
phase1 = load_exact_source_sibling("phase1_query_gate", "phase1_query_gate.py")
phase2 = load_exact_source_sibling(
    "phase2_compact_ids_ab_gate", "phase2_compact_ids_ab_gate.py"
)
phase3 = load_exact_source_sibling(
    "phase3_payload_coalescing_gate", "phase3_payload_coalescing_gate.py"
)


RAW_SCHEMA = "chronoxide.query-benchmark.raw/v14"
MANIFEST_SCHEMA = "chronoxide/storage-vnext-phase4-range-one-pass-manifest/v1"
NORMALIZED_SCHEMA = "chronoxide/storage-vnext-phase4-range-one-pass-normalized/v1"
RESULT_SCHEMA = "chronoxide/storage-vnext-phase4-range-one-pass-comparison/v1"
SMOKE_SCHEMA = "chronoxide/storage-vnext-phase4-smoke-validation/v1"
SOURCE_SEAL_SCHEMA = "chronoxide/phase4-source-seal/v1"
SOURCE_SNAPSHOT_SEAL_SCHEMA = "chronoxide/phase4-source-snapshot-seal/v1"
CARGO_CONFIG_ISOLATION_SCHEMA = "chronoxide/phase4-cargo-config-isolation/v1"
SEALED_QUERY_MANIFEST_SHA256 = (
    "f61e0c1d4ef40963f54ec7bd74827e4c991727e40b173e935a27bbd25f0a9dd6"
)
PHASE1_SEGMENTS_MANIFEST_SHA256 = (
    "8b0789e2f6c404a144e0d2e87f152a83e9f0bedb9c5ab2c6512608056cae3289"
)
AUDITED_GATE_INVENTORY_SHA256 = (
    "28547c0fc2b738eb58948400602640c017844cd57bd49917bffdf100a6e14a0b"
)
AUDITED_QUERY_CORPUS_FINGERPRINT_SHA256 = (
    "7e5cf252e5df9bdb786e1b9deb9248f09667962ac559f339ba47312c5c0e3ca3"
)
AUDITED_CORPUS_FILE_COUNT = 66
AUDITED_CORPUS_TOTAL_BYTES = 5_569_314_896

SUM_EXPRESSION = (
    "sum by (service_name_x55e50a58f9befba7)"
    "(rate(container_cpu_usage_seconds_total[15m]))"
)
COUNT_EXPRESSION = (
    "count by (service_name_x55e50a58f9befba7)"
    "(rate(container_cpu_usage_seconds_total[15m]))"
)
ACCEPTED_DENSE_EVENT_TIME_SPAN_MS = 4_500_000
AUDITED_CORPUS_CONTRACT = {
    "phase1_segments_manifest_sha256": PHASE1_SEGMENTS_MANIFEST_SHA256,
    "gate_inventory_sha256": AUDITED_GATE_INVENTORY_SHA256,
    "query_corpus_fingerprint_sha256": AUDITED_QUERY_CORPUS_FINGERPRINT_SHA256,
    "file_count": AUDITED_CORPUS_FILE_COUNT,
    "total_bytes": AUDITED_CORPUS_TOTAL_BYTES,
    "dense_event_time_span_ms": ACCEPTED_DENSE_EVENT_TIME_SPAN_MS,
}
QUERY_SPECS = (
    (
        "scalar_rate_sum_range_30m",
        1_782_978_613_585,
        1_800_000,
        7,
        "dense-real-window",
        True,
        SUM_EXPRESSION,
    ),
    (
        "scalar_rate_count_range_30m",
        1_782_978_613_585,
        1_800_000,
        7,
        "dense-real-window",
        True,
        COUNT_EXPRESSION,
    ),
    (
        "scalar_rate_sum_range_6h",
        1_782_958_813_585,
        21_600_000,
        73,
        "sparse-scheduler-control",
        False,
        SUM_EXPRESSION,
    ),
    (
        "scalar_rate_sum_range_24h",
        1_782_894_013_585,
        86_400_000,
        289,
        "sparse-scheduler-control",
        False,
        SUM_EXPRESSION,
    ),
)
END_MS = 1_782_980_413_585
STEP_MS = 300_000
WINDOW_MS = 900_000
ONE_PASS_MODE = "one-pass-assume-scalar"
MODES = ("repeated", ONE_PASS_MODE)
ABBA = ("repeated", ONE_PASS_MODE, ONE_PASS_MODE, "repeated")
BAAB = (ONE_PASS_MODE, "repeated", "repeated", ONE_PASS_MODE)
BLOCKS = 4
BENCHMARK_REPEATS = 3
PROCESSES_PER_ARM_PER_QUERY = 8
DEFAULT_ARENA_BYTES = 512 * 1024 * 1024
QUEUE_DEPTH = 128
COALESCE_GAP_BYTES = 4096
GUARDIAN_CONTROL_SCHEMA = "chronoxide/phase4-range-one-pass-guardian-control/v1"
GUARDIAN_SCHEMA = "chronoxide/phase4-range-one-pass-guardian/v1"
CONFLICT_SCAN_SCHEMA = "chronoxide/phase4-range-one-pass-conflict-scan/v1"
GUARDIAN_INTERVAL_MS = 100
GUARDIAN_EDGE_ALLOWANCE_NS = 100_000_000
GUARDIAN_SAMPLE_COLUMNS = (
    "poll_index",
    "monotonic_elapsed_ns",
    "recorded_at",
    "runner_running",
    "runner_starttime_ticks",
    "runner_ppid",
    "root_running",
    "root_state",
    "root_starttime_ticks",
    "root_ppid",
    "guardian_running",
    "guardian_starttime_ticks",
    "guardian_ppid",
    "launch_observed",
    "conflict_count",
)
GUARDIAN_CONFLICT_COLUMNS = (
    "poll_index",
    "monotonic_elapsed_ns",
    "recorded_at",
    "pid",
    "ppid",
    "state",
    "starttime_ticks",
    "cpu_percent",
    "name",
    "command",
)

DOCUMENT_FIELDS = phase3.DOCUMENT_FIELDS
CONFIGURATION_FIELDS = phase3.CONFIGURATION_FIELDS | {"range_execution_mode"}
RUN_FIELDS = phase3.RUN_FIELDS | {"range_execution"}
RANGE_EXECUTION_FIELDS = {
    "requested_mode",
    "effective_mode",
    "fallback_reason",
    "terminal_reason",
    "cache_bypassed",
    "evaluation_count",
    "union_start_ms",
    "union_end_ms",
    "source_series",
    "source_samples",
    "estimated_retained_bytes_peak",
    "retained_bytes_after_finalize",
    "preallocation_governed",
}
INDEX_FIELDS = {
    "process_label",
    "query_name",
    "evidence_class",
    "block",
    "order_index",
    "range_execution_mode",
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
    "range_execution_mode",
    "phase",
    "file_count",
    "resident_bytes",
    "corpus_file_bytes",
}
RESULT_FIELDS = {
    "schema",
    "correctness_gate",
    "result_equivalence_gate",
    "ordinary_query_stats_policy",
    "ordinary_query_stats_equivalent",
    "query_stats_classification",
    "non_query_stats_accounting_classification",
    "binary_sha256",
    "phase1_segments_manifest_sha256",
    "corpus_inventory_sha256",
    "corpus_file_count",
    "corpus_total_bytes",
    "query_corpus_fingerprint_sha256",
    "raw_schema",
    "fixed_configuration",
    "blocks",
    "schedule",
    "processes_per_arm_per_query",
    "benchmark_repeats",
    "warm_headline_observation_unit",
    "max_resident_bytes_after_evict",
    "max_observed_resident_bytes_after_evict",
    "max_observed_resident_bytes_after_run",
    "os_page_cache_eviction_gate",
    "quiet_host_confirmed",
    "allow_noisy_host",
    "measurement_host_status",
    "run_note_sha256",
    "multi_step_range_readback_gate",
    "multi_step_range_readbacks_expected",
    "multi_step_range_readbacks_executed",
    "multi_step_range_readbacks_skipped",
    "dense_promotion_evidence_query_names",
    "sparse_scheduler_control_query_names",
    "dense_24h_evidence_gate",
    "preallocation_governance_gate",
    "production_promotion_verdict",
    "candidate_disposition",
    "promotion_blockers",
    "measurements",
}
INVENTORY_FIELDS = phase3.INVENTORY_FIELDS
INVENTORY_FILE_FIELDS = phase3.INVENTORY_FILE_FIELDS
SHA256_LINE = re.compile(r"^([0-9a-f]{64})  (.+)$")
SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
UNSAFE_TEXT = re.compile(r"[\x00\t\r\n]")
NON_QUERY_ACCOUNTING_COMPONENTS = (
    "payload",
    "scheduler",
    "labels",
    "label_storage",
    "symbols",
    "metadata",
    "range_cache",
    "stages",
    "range_execution",
)
RANGE_CACHE_BOOLEAN_FIELDS = {
    "governor_refused",
    "allocation_refused",
    "layout_overflow",
}
RANGE_CACHE_ZERO_CHARGE_FIELDS = {
    "governor_lease_bytes",
    "entry_arena_charge_bytes",
    "sample_arena_charge_bytes",
    "hits",
    "admitted_entries",
    "logical_hit_bytes",
    "peak_retained_charge_bytes",
    "retained_charge_after_finalize",
    "process_governor_current_leased_bytes",
    "process_governor_lifetime_peak_leased_bytes",
}


class GateError(ValueError):
    """A reproducibility, correctness, or evidence-classification failure."""


def _regular_file(path: Path, name: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise GateError(f"{name} must be a regular file: {path}")


def _load_json(path: Path) -> Any:
    _regular_file(path, str(path))
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as destination:
        json.dump(value, destination, indent=2, sort_keys=True)
        destination.write("\n")


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
        raise GateError(
            f"git {' '.join(arguments)} failed for {repo}: {detail}"
        ) from error
    return result.stdout


def _is_excluded_runtime_artifact(path: str) -> bool:
    return (
        (
            path.startswith("chronoxide-ingester/")
            and Path(path).name.startswith("ingestion_stats_")
            and path.endswith(".md")
        )
        or "/__pycache__/" in f"/{path}"
        or path.endswith(".pyc")
    )


def _is_ignored_build_input_candidate(path: str) -> bool:
    if _is_excluded_runtime_artifact(path) or path.startswith("target/"):
        return False
    candidate = Path(path)
    return (
        path in {".cargo/config", ".cargo/config.toml"}
        or path.endswith("/.cargo/config")
        or path.endswith("/.cargo/config.toml")
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
    if repo.is_symlink():
        raise GateError("source repository root must not be a symlink")
    repo = repo.resolve(strict=True)
    root = Path(str(_git(repo, "rev-parse", "--show-toplevel")).strip())
    if root != repo:
        raise GateError(f"source root is not the Git worktree root: {repo}")
    dirty = str(
        _git(repo, "status", "--porcelain=v1", "--untracked-files=no")
    ).strip()
    if dirty:
        raise GateError(
            "formal source-bound build requires a clean tracked worktree and index"
        )
    flags = bytes(_git(repo, "ls-files", "-v", "-z", binary=True))
    for entry in (item for item in flags.split(b"\0") if item):
        if len(entry) < 3 or entry[1:2] != b" ":
            raise GateError("git ls-files -v returned a malformed entry")
        if entry[:1] != b"H":
            raise GateError(
                "formal source-bound build rejects nonordinary Git index flag "
                f"{entry[:1].decode(errors='replace')!r}: "
                f"{entry[2:].decode('utf-8')}"
            )
    untracked_raw = bytes(
        _git(repo, "ls-files", "--others", "--exclude-standard", "-z", binary=True)
    )
    untracked = [item.decode("utf-8") for item in untracked_raw.split(b"\0") if item]
    disallowed = [path for path in untracked if not _is_excluded_runtime_artifact(path)]
    if disallowed:
        raise GateError(
            f"formal source-bound build rejects untracked input: {disallowed[0]}"
        )
    ignored_raw = bytes(
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
    ignored = [item.decode("utf-8") for item in ignored_raw.split(b"\0") if item]
    ignored_inputs = [path for path in ignored if _is_ignored_build_input_candidate(path)]
    if ignored_inputs:
        raise GateError(
            f"formal source-bound build rejects ignored source/build input: {ignored_inputs[0]}"
        )
    cargo_lock = repo / "Cargo.lock"
    _regular_file(cargo_lock, "Cargo.lock")
    try:
        _git(repo, "ls-files", "--error-unmatch", "Cargo.lock")
    except GateError as error:
        raise GateError("Cargo.lock must be tracked") from error
    tracked_raw = bytes(_git(repo, "ls-files", "-z", binary=True))
    tracked = [item.decode("utf-8") for item in tracked_raw.split(b"\0") if item]
    tracked_index = bytes(_git(repo, "ls-files", "-s", "-z", binary=True))
    for entry in (item for item in tracked_index.split(b"\0") if item):
        try:
            metadata, path_bytes = entry.split(b"\t", 1)
            mode, _object_id, stage = metadata.split(b" ", 2)
        except ValueError as error:
            raise GateError("git ls-files -s returned a malformed entry") from error
        path = path_bytes.decode("utf-8")
        if mode not in {b"100644", b"100755"} or stage != b"0":
            raise GateError(
                f"formal source-bound build rejects unsupported index entry: {path}"
            )
    cargo_configs: list[dict[str, Any]] = []
    for relative in tracked:
        if relative in {".cargo/config", ".cargo/config.toml"} or relative.endswith(
            ("/.cargo/config", "/.cargo/config.toml")
        ):
            path = repo / relative
            _regular_file(path, f"Cargo config {relative}")
            cargo_configs.append(
                {
                    "path": relative,
                    "sha256": file_sha256(path),
                    "size_bytes": path.stat().st_size,
                }
            )
    ancestor = repo.parent
    while True:
        for name in ("config", "config.toml"):
            ambient = ancestor / ".cargo" / name
            if os.path.lexists(ambient):
                raise GateError(f"ambient ancestor Cargo config is forbidden: {ambient}")
        if ancestor == ancestor.parent:
            break
        ancestor = ancestor.parent
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
        "tracked_file_count": len(tracked),
        "cargo_lock_sha256": file_sha256(cargo_lock),
        "cargo_configs": cargo_configs,
    }
    return {
        "schema": SOURCE_SEAL_SCHEMA,
        "repo": str(repo),
        **identity,
        "identity_sha256": hashlib.sha256(
            canonical_json(identity).encode()
        ).hexdigest(),
        "excluded_untracked_runtime_artifacts": sorted(untracked),
    }


def check_source_seal(repo: Path, seal_path: Path) -> dict[str, Any]:
    expected = _load_json(seal_path)
    current = source_seal(repo)
    for key in (
        "schema",
        "repo",
        "head",
        "tree",
        "tracked_index_sha256",
        "tracked_file_count",
        "cargo_lock_sha256",
        "cargo_configs",
        "identity_sha256",
    ):
        if expected.get(key) != current[key]:
            raise GateError(f"source seal changed: {key}")
    return {"status": "pass", "identity_sha256": current["identity_sha256"]}


def _git_blob_oid(path: Path, object_format: str) -> str:
    try:
        result = hashlib.new(object_format)
    except ValueError as error:
        raise GateError(f"unsupported Git object format: {object_format}") from error
    result.update(f"blob {path.stat().st_size}\0".encode())
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            result.update(block)
    return result.hexdigest()


def source_snapshot_seal(
    repo: Path, snapshot: Path, source_seal_path: Path
) -> dict[str, Any]:
    if repo.is_symlink() or snapshot.is_symlink():
        raise GateError("source repository and snapshot roots must not be symlinks")
    repo = repo.resolve(strict=True)
    snapshot = snapshot.resolve(strict=True)
    if not snapshot.is_dir():
        raise GateError("source snapshot must be a regular directory")
    if stat.S_IMODE(snapshot.stat().st_mode) != 0o555:
        raise GateError("source snapshot root must be mode 0555")
    check_source_seal(repo, source_seal_path)
    source_document = _load_json(source_seal_path)
    head = source_document.get("head")
    tree = source_document.get("tree")
    if not isinstance(head, str) or not re.fullmatch(r"[0-9a-f]{40,64}", head):
        raise GateError("source seal has an invalid HEAD")
    if str(_git(repo, "rev-parse", f"{head}^{{tree}}")).strip() != tree:
        raise GateError("source seal HEAD and tree are not cross-bound")
    object_format = str(_git(repo, "rev-parse", "--show-object-format")).strip()
    if object_format not in {"sha1", "sha256"}:
        raise GateError(f"unsupported Git object format: {object_format}")
    tree_entries = bytes(
        _git(repo, "ls-tree", "-r", "-z", "--full-tree", head, binary=True)
    )
    expected: dict[str, tuple[str, str]] = {}
    expected_directories: set[str] = set()
    for entry in (item for item in tree_entries.split(b"\0") if item):
        try:
            metadata, path_bytes = entry.split(b"\t", 1)
            mode_raw, object_type, object_id_raw = metadata.split(b" ", 2)
        except ValueError as error:
            raise GateError("git ls-tree returned a malformed entry") from error
        relative = path_bytes.decode("utf-8")
        mode = mode_raw.decode("ascii")
        if object_type != b"blob" or mode not in {"100644", "100755"}:
            raise GateError(f"unsupported tracked snapshot entry: {relative}")
        expected[relative] = (mode, object_id_raw.decode("ascii"))
        parent = PurePosixPath(relative).parent
        while parent != PurePosixPath("."):
            expected_directories.add(parent.as_posix())
            parent = parent.parent
    observed: dict[str, Path] = {}
    observed_directories: set[str] = set()
    for candidate in snapshot.rglob("*"):
        relative = candidate.relative_to(snapshot).as_posix()
        if candidate.is_symlink():
            raise GateError(f"source snapshot contains a symlink: {relative}")
        candidate_stat = candidate.stat()
        if stat.S_ISDIR(candidate_stat.st_mode):
            if stat.S_IMODE(candidate_stat.st_mode) != 0o555:
                raise GateError(f"source snapshot directory is not mode 0555: {relative}")
            observed_directories.add(relative)
        elif stat.S_ISREG(candidate_stat.st_mode):
            observed[relative] = candidate
        else:
            raise GateError(f"source snapshot contains a non-regular entry: {relative}")
    if set(observed) != set(expected) or observed_directories != expected_directories:
        raise GateError("source snapshot path set differs from sealed Git HEAD")
    files: list[dict[str, Any]] = []
    for relative in sorted(expected):
        expected_mode, expected_oid = expected[relative]
        path = observed[relative]
        required_mode = 0o555 if expected_mode == "100755" else 0o444
        if stat.S_IMODE(path.stat().st_mode) != required_mode:
            raise GateError(f"source snapshot mode differs for {relative}")
        if _git_blob_oid(path, object_format) != expected_oid:
            raise GateError(f"source snapshot bytes differ from Git for {relative}")
        files.append(
            {
                "path": relative,
                "mode": expected_mode,
                "object_id": expected_oid,
                "size_bytes": path.stat().st_size,
            }
        )
    identity = {
        "git_head": head,
        "git_tree": tree,
        "source_seal_identity_sha256": source_document["identity_sha256"],
        "object_format": object_format,
        "file_count": len(files),
        "files": files,
    }
    return {
        "schema": SOURCE_SNAPSHOT_SEAL_SCHEMA,
        "repo": str(repo),
        "snapshot": str(snapshot),
        **identity,
        "identity_sha256": hashlib.sha256(
            canonical_json(identity).encode()
        ).hexdigest(),
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
    if snapshot.is_symlink() or cargo_home.is_symlink():
        raise GateError("source snapshot and CARGO_HOME roots must not be symlinks")
    snapshot = snapshot.resolve(strict=True)
    cargo_home = cargo_home.resolve(strict=True)
    for path, label in ((snapshot, "source snapshot"), (cargo_home, "CARGO_HOME")):
        if path.is_symlink() or not path.is_dir():
            raise GateError(f"{label} must be a regular directory")
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
    "RUSTFLAGS",
    "RUSTDOCFLAGS",
    "RUSTUP_TOOLCHAIN",
    "RUST_LOG",
}
FORBIDDEN_AMBIENT_ENV_PREFIXES = (
    "CARGO_",
    "JEMALLOC_",
    "MIMALLOC_",
    "SCCACHE_",
)


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
    rf"emulator|adb|gradle|gradlew|GradleDaemon|{COMPILER_PROCESS_TOKEN}|"
    rf"cc1|cc1plus|{LINKER_PROCESS_TOKEN}|perf|heaptrack|valgrind.*|strace|"
    rf"ltrace|bpftrace|hotspot|chronoxide-.*|greptime.*|clickhouse.*|"
    rf"postgres.*|mysqld|influxd|victoria.*|vm(?:storage|select|agent)|"
    rf"mimir.*|thanos.*|cortex.*|prometheus|{SOONG_PROCESS_TOKEN}|ckati|"
    rf"kati|javac|kotlinc|metalava|aapt|aapt2|aidl|dex2oat|btop|htop|top)$",
    re.IGNORECASE,
)
FORBIDDEN_PROCESS_COMMAND = re.compile(
    rf"(?:^|[/ ])(?:cargo(?:-nextest)?|rustc|rustdoc|clippy-driver|nextest|"
    rf"{NINJA_PROCESS_TOKEN}|{COMPILER_PROCESS_TOKEN}|{LINKER_PROCESS_TOKEN}|"
    rf"{SOONG_PROCESS_TOKEN}|ckati|kati|gradlew?|metalava|aapt2?|aidl|"
    rf"dex2oat)(?:$|[ /])|org\.gradle\.|GradleDaemon|GradleWorkerMain|"
    rf"gradle-worker|Android[/ ](?:SDK )?emulator",
    re.IGNORECASE,
)
ANDROID_VM_PROCESS_COMMAND = re.compile(
    r"(?:redroid|artracer|cuttlefish|android[-_/ ]|system-qemu|goldfish|ranchu)",
    re.IGNORECASE,
)


def is_forbidden_process(
    command_name: str, command: str, cpu_percent: float
) -> bool:
    qemu_or_java = command_name.casefold().startswith(
        "qemu-system"
    ) or command_name.casefold() in {"qemu-kvm", "java"}
    special_conflict = qemu_or_java and (
        cpu_percent >= 5.0
        or ANDROID_VM_PROCESS_COMMAND.search(command) is not None
        or FORBIDDEN_PROCESS_COMMAND.search(command) is not None
    )
    return bool(
        FORBIDDEN_PROCESS_NAMES.fullmatch(command_name)
        or FORBIDDEN_PROCESS_COMMAND.search(command)
        or special_conflict
    )


def _parse_process_rows(
    lines: list[str], context: str
) -> list[tuple[int, int, float, str, str]]:
    rows: list[tuple[int, int, float, str, str]] = []
    for line_number, line in enumerate(lines, start=1):
        parts = line.strip().split(None, 4)
        if len(parts) < 4:
            raise GateError(
                f"{context} line {line_number} has an invalid shape"
            )
        try:
            pid = int(parts[0])
            parent_pid = int(parts[1])
            cpu_percent = float(parts[2])
        except ValueError as error:
            raise GateError(
                f"{context} line {line_number} has an invalid numeric field"
            ) from error
        if not math.isfinite(cpu_percent) or cpu_percent < 0:
            raise GateError(
                f"{context} line {line_number} has an invalid CPU percentage"
            )
        command_name = parts[3]
        command = parts[4] if len(parts) == 5 else command_name
        rows.append((pid, parent_pid, cpu_percent, command_name, command))
    if not rows:
        raise GateError(f"{context} is empty")
    return rows


def _validate_process_rows(
    rows: list[tuple[int, int, float, str, str]],
    allowed_pids: set[int],
    allowed_root_pids: set[int] | None = None,
) -> None:
    allowed = set(allowed_pids)
    roots = set() if allowed_root_pids is None else set(allowed_root_pids)
    allowed.update(roots)
    changed = True
    while changed:
        changed = False
        for pid, parent_pid, _cpu, _name, _command in rows:
            if parent_pid in allowed and pid not in allowed:
                allowed.add(pid)
                changed = True
    for pid, _parent_pid, cpu_percent, command_name, command in rows:
        if pid not in allowed and is_forbidden_process(
            command_name, command, cpu_percent
        ):
            raise GateError(
                f"measurement conflict in process snapshot: pid={pid} comm={command_name}"
            )


def validate_process_snapshot(path: Path, allowed_pids: set[int]) -> None:
    _regular_file(path, "process snapshot")
    lines = path.read_text(encoding="utf-8").splitlines()
    _validate_process_rows(
        _parse_process_rows(lines, "process snapshot"), allowed_pids
    )


def _strict_guardian_int(value: Any, context: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise GateError(f"{context} must be an integer >= {minimum}")
    return value


def _canonical_decimal_int(value: str, context: str, *, minimum: int = 0) -> int:
    if not re.fullmatch(r"0|[1-9][0-9]*", value):
        raise GateError(f"{context} must be a canonical decimal integer")
    parsed = int(value)
    if parsed < minimum:
        raise GateError(f"{context} must be >= {minimum}")
    return parsed


def _canonical_guardian_ppid(value: str, context: str) -> int:
    if value == "-1":
        return -1
    return _canonical_decimal_int(value, context, minimum=1)


def _guardian_bool(value: str, context: str) -> bool:
    if value not in {"true", "false"}:
        raise GateError(f"{context} must be true or false")
    return value == "true"


def _read_ordered_tsv(
    path: Path,
    columns: tuple[str, ...],
    context: str,
    *,
    allow_empty: bool = False,
) -> list[dict[str, str]]:
    _regular_file(path, context)
    try:
        with path.open(newline="", encoding="utf-8", errors="strict") as source:
            reader = csv.DictReader(source, delimiter="\t")
            if tuple(reader.fieldnames or ()) != columns:
                raise GateError(f"{context} has a noncanonical TSV header")
            rows = list(reader)
    except UnicodeDecodeError as error:
        raise GateError(f"{context} is not strict UTF-8") from error
    if not allow_empty and not rows:
        raise GateError(f"{context} has no data rows")
    if any(tuple(row) != columns or None in row.values() for row in rows):
        raise GateError(f"{context} has a malformed TSV row")
    return rows


def _validate_guardian_marker(path: Path, context: str) -> None:
    _regular_file(path, context)
    if path.stat().st_size != 0 or stat.S_IMODE(path.stat().st_mode) != 0o444:
        raise GateError(f"{context} must be exact empty mode 0444")


def _guardian_maximum_gap_ns(
    timestamps: list[int], terminal_elapsed_ns: int
) -> int:
    if terminal_elapsed_ns < 0 or any(value < 0 for value in timestamps):
        raise GateError("guardian cadence timestamps must be non-negative")
    if any(later <= earlier for earlier, later in zip(timestamps, timestamps[1:])):
        raise GateError("guardian cadence timestamps must increase strictly")
    if timestamps and timestamps[-1] > terminal_elapsed_ns:
        raise GateError("guardian terminal elapsed time precedes its final sample")
    boundaries = [0, *timestamps, terminal_elapsed_ns]
    return max(
        (later - earlier for earlier, later in zip(boundaries, boundaries[1:])),
        default=0,
    )


def _empty_guardian_termination(root_starttime_ticks: int) -> dict[str, Any]:
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


def validate_conflict_scan(path: Path) -> None:
    _regular_file(path, "immediate process conflict scan")
    if stat.S_IMODE(path.stat().st_mode) != 0o444:
        raise GateError("immediate process conflict scan must have exact mode 0444")
    value = exact_object(
        _load_json(path),
        {"schema", "conflicts", "quiet"},
        "immediate process conflict scan",
    )
    if (
        value["schema"] != CONFLICT_SCAN_SCHEMA
        or value["quiet"] is not True
        or value["conflicts"] != []
    ):
        raise GateError("immediate process conflict scan was not exactly quiet")


def _validate_guardian_control(
    control_path: Path, ready: Path, launch: Path
) -> dict[str, Any]:
    _regular_file(control_path, "process guardian control")
    if stat.S_IMODE(control_path.stat().st_mode) != 0o444:
        raise GateError("process guardian control must have exact mode 0444")
    fields = {
        "schema",
        "runner_pid",
        "runner_starttime_ticks",
        "runner_ppid",
        "root_pid",
        "root_starttime_ticks",
        "root_ppid",
        "guardian_pid",
        "guardian_starttime_ticks",
        "guardian_ppid",
        "interval_ms",
        "ready_marker",
        "launch_marker",
    }
    value = exact_object(_load_json(control_path), fields, "process guardian control")
    roles = ("runner", "root", "guardian")
    pids = {
        role: _strict_guardian_int(
            value[f"{role}_pid"], f"guardian control {role}_pid", minimum=2
        )
        for role in roles
    }
    for role in roles:
        _strict_guardian_int(
            value[f"{role}_starttime_ticks"],
            f"guardian control {role}_starttime_ticks",
            minimum=1,
        )
    runner_ppid = _strict_guardian_int(
        value["runner_ppid"], "guardian control runner_ppid", minimum=1
    )
    root_ppid = _strict_guardian_int(
        value["root_ppid"], "guardian control root_ppid", minimum=2
    )
    guardian_ppid = _strict_guardian_int(
        value["guardian_ppid"], "guardian control guardian_ppid", minimum=2
    )
    if (
        value["schema"] != GUARDIAN_CONTROL_SCHEMA
        or value["interval_ms"] != GUARDIAN_INTERVAL_MS
        or len(set(pids.values())) != len(pids)
        or runner_ppid in pids.values()
        or root_ppid != pids["runner"]
        or guardian_ppid != pids["runner"]
        or value["ready_marker"] != str(ready)
        or value["launch_marker"] != str(launch)
        or not ready.is_absolute()
        or not launch.is_absolute()
        or ready.parent != control_path.parent
        or launch.parent != control_path.parent
    ):
        raise GateError("process guardian control differs from the exact handshake")
    return value


def validate_guardian_evidence(
    samples_path: Path,
    conflicts_path: Path,
    summary_path: Path,
    control_path: Path,
    exit_status: Path,
    ready: Path,
    launch: Path,
    immediate_conflicts: Path,
) -> None:
    validate_conflict_scan(immediate_conflicts)
    _validate_guardian_marker(ready, "guardian ready marker")
    _validate_guardian_marker(launch, "guardian launch marker")
    _regular_file(exit_status, "guardian exit status")
    if exit_status.read_bytes() != b"0\n":
        raise GateError("process guardian did not exit successfully")
    control = _validate_guardian_control(control_path, ready, launch)
    for evidence_path, context in (
        (samples_path, "process guardian samples"),
        (conflicts_path, "process guardian conflicts"),
        (summary_path, "process guardian summary"),
    ):
        _regular_file(evidence_path, context)
        if stat.S_IMODE(evidence_path.stat().st_mode) != 0o444:
            raise GateError(f"{context} must have exact mode 0444")
    conflict_rows = _read_ordered_tsv(
        conflicts_path,
        GUARDIAN_CONFLICT_COLUMNS,
        "process guardian conflicts",
        allow_empty=True,
    )
    if conflict_rows:
        first = conflict_rows[0]
        raise GateError(
            "process guardian recorded a transient conflict: "
            f"pid={first['pid']} name={first['name']}"
        )
    rows = _read_ordered_tsv(
        samples_path, GUARDIAN_SAMPLE_COLUMNS, "process guardian samples"
    )
    if len(rows) < 2:
        raise GateError("process guardian requires at least two raw samples")
    timestamps: list[int] = []
    launch_values: list[bool] = []
    root_values: list[bool] = []
    runner_pid = int(control["runner_pid"])
    for index, row in enumerate(rows, start=1):
        if _canonical_decimal_int(
            row["poll_index"], f"guardian sample {index} poll_index", minimum=1
        ) != index:
            raise GateError("process guardian poll indexes are not contiguous")
        elapsed = _canonical_decimal_int(
            row["monotonic_elapsed_ns"],
            f"guardian sample {index} monotonic elapsed",
        )
        timestamps.append(elapsed)
        try:
            recorded = dt.datetime.fromisoformat(row["recorded_at"])
        except ValueError as error:
            raise GateError("process guardian recorded_at is not ISO-8601") from error
        if recorded.tzinfo is None:
            raise GateError("process guardian recorded_at must include a timezone")
        runner_running = _guardian_bool(
            row["runner_running"], f"guardian sample {index} runner_running"
        )
        root_running = _guardian_bool(
            row["root_running"], f"guardian sample {index} root_running"
        )
        guardian_running = _guardian_bool(
            row["guardian_running"], f"guardian sample {index} guardian_running"
        )
        launch_observed = _guardian_bool(
            row["launch_observed"], f"guardian sample {index} launch_observed"
        )
        current_runner_ppid = _canonical_guardian_ppid(
            row["runner_ppid"], f"guardian sample {index} runner_ppid"
        )
        current_runner_start = _canonical_decimal_int(
            row["runner_starttime_ticks"],
            f"guardian sample {index} runner_starttime_ticks",
            minimum=1,
        )
        current_root_ppid = _canonical_guardian_ppid(
            row["root_ppid"], f"guardian sample {index} root_ppid"
        )
        current_root_start = _canonical_decimal_int(
            row["root_starttime_ticks"],
            f"guardian sample {index} root_starttime_ticks",
        )
        current_guardian_ppid = _canonical_guardian_ppid(
            row["guardian_ppid"], f"guardian sample {index} guardian_ppid"
        )
        current_guardian_start = _canonical_decimal_int(
            row["guardian_starttime_ticks"],
            f"guardian sample {index} guardian_starttime_ticks",
            minimum=1,
        )
        if (
            not runner_running
            or not guardian_running
            or current_runner_start != control["runner_starttime_ticks"]
            or current_runner_ppid != control["runner_ppid"]
            or current_guardian_start != control["guardian_starttime_ticks"]
            or current_guardian_ppid != runner_pid
            or _canonical_decimal_int(
                row["conflict_count"],
                f"guardian sample {index} conflict_count",
            )
            != 0
        ):
            raise GateError("process guardian raw identity or conflict evidence differs")
        state = row["root_state"]
        if root_running:
            if len(state) != 1 or state in {"Z", "X", "x"}:
                raise GateError("live guardian root sample has an invalid state")
            if (
                current_root_start != control["root_starttime_ticks"]
                or current_root_ppid != runner_pid
            ):
                raise GateError("live guardian root sample has a changed identity")
        elif state not in {"-", "Z", "X", "x"}:
            raise GateError("terminal guardian root sample has an invalid state")
        elif (
            state == "-"
            and (current_root_start != 0 or current_root_ppid != -1)
        ) or (
            state in {"Z", "X", "x"}
            and (
                current_root_start != control["root_starttime_ticks"]
                or current_root_ppid != runner_pid
            )
        ):
            raise GateError("terminal guardian root sample has a changed identity")
        root_values.append(root_running)
        launch_values.append(launch_observed)
    if not all(root_values[:-1]) or root_values[-1]:
        raise GateError("guardian lacks one final root-absent terminal sample")
    if launch_values[0] or not any(launch_values[1:]):
        raise GateError("guardian launch was not observed strictly after readiness")
    first_launch = launch_values.index(True) + 1
    if not all(launch_values[first_launch - 1 :]):
        raise GateError("guardian launch observation is not monotonic")
    summary_fields = {
        "schema",
        "runner_pid",
        "runner_starttime_ticks",
        "runner_ppid",
        "root_pid",
        "root_starttime_ticks",
        "root_ppid",
        "guardian_pid",
        "guardian_starttime_ticks",
        "guardian_ppid",
        "interval_ms",
        "polls",
        "terminal_elapsed_ns",
        "poll_monotonic_elapsed_ns",
        "maximum_poll_start_gap_ns",
        "maximum_allowed_poll_start_gap_ns",
        "control_path",
        "control_sha256",
        "ready_marker",
        "launch_marker",
        "ready_created_poll",
        "ready_created_monotonic_elapsed_ns",
        "launch_observed_poll",
        "launch_observed_monotonic_elapsed_ns",
        "terminal_sample_poll",
        "root_seen",
        "conflicts",
        "identity_violations",
        "handshake_violations",
        "termination",
        "complete_and_conflict_free",
    }
    summary = exact_object(
        _load_json(summary_path), summary_fields, "process guardian summary"
    )
    for field, minimum in (
        ("polls", 2),
        ("terminal_elapsed_ns", 0),
        ("maximum_poll_start_gap_ns", 0),
        ("maximum_allowed_poll_start_gap_ns", 1),
        ("ready_created_poll", 1),
        ("ready_created_monotonic_elapsed_ns", 0),
        ("launch_observed_poll", 2),
        ("launch_observed_monotonic_elapsed_ns", 0),
        ("terminal_sample_poll", 2),
    ):
        _strict_guardian_int(summary[field], f"guardian summary {field}", minimum=minimum)
    if (
        not isinstance(summary["poll_monotonic_elapsed_ns"], list)
        or any(type(value) is not int or value < 0 for value in summary["poll_monotonic_elapsed_ns"])
    ):
        raise GateError("guardian summary poll timestamps must be non-negative integers")
    termination = exact_object(
        summary["termination"],
        set(_empty_guardian_termination(int(control["root_starttime_ticks"]))),
        "guardian summary termination",
    )
    if type(termination["attempted"]) is not bool:
        raise GateError("guardian summary termination attempted must be boolean")
    for field in (
        "target_processes",
        "target_pids",
        "term_sent_pids",
        "term_errors",
        "kill_sent_pids",
        "kill_errors",
        "identity_refusals",
        "surviving_pids",
    ):
        if not isinstance(termination[field], list):
            raise GateError(f"guardian summary termination {field} must be a list")
    terminal_elapsed = _strict_guardian_int(
        summary["terminal_elapsed_ns"], "guardian terminal elapsed"
    )
    maximum_gap = _guardian_maximum_gap_ns(timestamps, terminal_elapsed)
    maximum_allowed = (
        GUARDIAN_INTERVAL_MS * 1_000_000 + GUARDIAN_EDGE_ALLOWANCE_NS
    )
    for field in (
        "runner_pid",
        "runner_starttime_ticks",
        "runner_ppid",
        "root_pid",
        "root_starttime_ticks",
        "root_ppid",
        "guardian_pid",
        "guardian_starttime_ticks",
        "guardian_ppid",
        "interval_ms",
    ):
        if summary[field] != control[field]:
            raise GateError(f"guardian summary {field} differs from control")
    expected_termination = _empty_guardian_termination(
        int(control["root_starttime_ticks"])
    )
    if (
        summary["schema"] != GUARDIAN_SCHEMA
        or summary["polls"] != len(rows)
        or summary["poll_monotonic_elapsed_ns"] != timestamps
        or summary["maximum_poll_start_gap_ns"] != maximum_gap
        or summary["maximum_allowed_poll_start_gap_ns"] != maximum_allowed
        or maximum_gap > maximum_allowed
        or summary["control_path"] != str(control_path)
        or summary["control_sha256"] != file_sha256(control_path)
        or summary["ready_marker"] != str(ready)
        or summary["launch_marker"] != str(launch)
        or summary["ready_created_poll"] != 1
        or summary["ready_created_monotonic_elapsed_ns"] != timestamps[0]
        or summary["launch_observed_poll"] != first_launch
        or summary["launch_observed_monotonic_elapsed_ns"]
        != timestamps[first_launch - 1]
        or summary["terminal_sample_poll"] != len(rows)
        or summary["root_seen"] is not True
        or summary["conflicts"] != []
        or summary["identity_violations"] != []
        or summary["handshake_violations"] != []
        or termination != expected_termination
        or summary["complete_and_conflict_free"] is not True
    ):
        raise GateError("process guardian summary is detached from raw lifecycle evidence")


FINAL_REQUIRED_DIRECTORIES = {
    "comparisons",
    "inventory",
    "metadata",
    "runs",
    "validation",
}
FINAL_OPTIONAL_DIRECTORIES = {"build-source", "build-target"}
FINAL_ROOT_FILES = {
    "COMPLETE",
    "queries.tsv",
    "queries.normalized.json",
    "run-plan.tsv",
    "raw-index.tsv",
    "residency-summary.tsv",
    "summary.tsv",
}
FINAL_INVENTORY_EXCLUDED_FILES = {
    "metadata/result-artifacts.nul",
    "metadata/result-directories.nul",
    "metadata/result-artifacts.sha256",
}
FINAL_INVENTORY_AUTHORITY_FILES = {
    "metadata/result-artifacts.nul",
    "metadata/result-directories.nul",
}
FINAL_DYNAMIC_NON_EVIDENCE_SUBTREES = {
    "build-target",
    "metadata/build/cargo-home",
}


def final_artifact_inventory(result_dir: Path) -> tuple[list[str], list[str]]:
    if result_dir.is_symlink():
        raise GateError("final result root must not be a symlink")
    root = result_dir.resolve(strict=True)
    if root != Path(os.path.abspath(result_dir)) or not root.is_dir():
        raise GateError("final result root must be a regular directory")
    files: list[str] = []
    directories: list[str] = []

    def visit(directory: Path, relative_directory: str) -> None:
        try:
            with os.scandir(directory) as iterator:
                entries = sorted(iterator, key=lambda entry: os.fsencode(entry.name))
        except OSError as error:
            raise GateError(
                f"cannot enumerate final artifact directory "
                f"{relative_directory or '.'}: {error}"
            ) from error
        for entry in entries:
            relative = (
                f"{relative_directory}/{entry.name}"
                if relative_directory
                else entry.name
            )
            if entry.is_symlink():
                raise GateError(f"final artifact contains a symlink: {relative}")
            try:
                entry_stat = entry.stat(follow_symlinks=False)
            except OSError as error:
                raise GateError(f"cannot stat final artifact {relative}: {error}") from error
            if stat.S_ISDIR(entry_stat.st_mode):
                if not relative_directory and relative not in (
                    FINAL_REQUIRED_DIRECTORIES | FINAL_OPTIONAL_DIRECTORIES
                ):
                    raise GateError(
                        f"final artifact contains an unsupported root directory: {relative}"
                    )
                directories.append(relative)
                visit(Path(entry.path), relative)
            elif stat.S_ISREG(entry_stat.st_mode):
                if not relative_directory and relative not in FINAL_ROOT_FILES:
                    raise GateError(
                        f"final artifact contains an unsupported root file: {relative}"
                    )
                if relative not in FINAL_INVENTORY_EXCLUDED_FILES:
                    files.append(relative)
            else:
                raise GateError(
                    f"final artifact contains a non-regular entry: {relative}"
                )

    visit(root, "")
    present_root_directories = {
        relative for relative in directories if "/" not in relative
    }
    missing = sorted(FINAL_REQUIRED_DIRECTORIES - present_root_directories)
    if missing:
        raise GateError(f"final artifact is missing required directory: {missing[0]}")
    files.sort(key=os.fsencode)
    directories.sort(key=os.fsencode)
    if len(files) != len(set(files)) or len(directories) != len(set(directories)):
        raise GateError("final artifact traversal produced duplicate paths")
    return files, directories


def write_final_artifact_inventory(
    result_dir: Path, files_output: Path, directories_output: Path
) -> None:
    if result_dir.is_symlink():
        raise GateError("final result root must not be a symlink")
    root = result_dir.resolve(strict=True)
    expected_files = root / "metadata" / "result-artifacts.nul"
    expected_directories = root / "metadata" / "result-directories.nul"
    if files_output.parent.resolve(strict=True) / files_output.name != expected_files:
        raise GateError(f"final file inventory output must be {expected_files}")
    if (
        directories_output.parent.resolve(strict=True) / directories_output.name
        != expected_directories
    ):
        raise GateError(
            f"final directory inventory output must be {expected_directories}"
        )
    if os.path.lexists(files_output) or os.path.lexists(directories_output):
        raise GateError("final artifact inventory output already exists")
    files, directories = final_artifact_inventory(root)
    validate_final_artifact_matrix(root, files, directories)
    with files_output.open("xb") as destination:
        for relative in files:
            destination.write(relative.encode("utf-8") + b"\0")
    with directories_output.open("xb") as destination:
        for relative in directories:
            destination.write(relative.encode("utf-8") + b"\0")


def nonnegative_int(value: Any, context: str) -> int:
    try:
        return common.nonnegative_int(value, context)
    except common.GateError as error:
        raise GateError(str(error)) from error


def positive_int(value: Any, context: str) -> int:
    try:
        return common.positive_int(value, context)
    except common.GateError as error:
        raise GateError(str(error)) from error


def digest(value: Any, context: str) -> str:
    try:
        return common.hex_digest(value, context)
    except common.GateError as error:
        raise GateError(str(error)) from error


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
    return {field: nonnegative_int(obj[field], f"{context}.{field}") for field in fields}


def finite_nonnegative(value: Any, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GateError(f"{context} must be a finite non-negative number")
    try:
        converted = float(value)
    except OverflowError as error:
        raise GateError(f"{context} must be a finite non-negative number") from error
    if not math.isfinite(converted) or converted < 0:
        raise GateError(f"{context} must be a finite non-negative number")
    return converted


def finite_number(value: Any, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GateError(f"{context} must be a finite number")
    try:
        converted = float(value)
    except OverflowError as error:
        raise GateError(f"{context} must be a finite number") from error
    if not math.isfinite(converted):
        raise GateError(f"{context} must be a finite number")
    return converted


def finite_tsv_number(value: str, context: str) -> float:
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value):
        raise GateError(f"{context} must be a canonical finite non-negative number")
    converted = float(value)
    if not math.isfinite(converted):
        raise GateError(f"{context} must be a canonical finite non-negative number")
    return converted


def canonical_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def file_sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            result.update(block)
    return result.hexdigest()


def checked_name(value: Any, context: str) -> str:
    if not isinstance(value, str) or not SAFE_NAME.fullmatch(value):
        raise GateError(f"{context} is not a safe name")
    return value


def checked_text(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value or UNSAFE_TEXT.search(value):
        raise GateError(f"{context} must be non-empty and single-line")
    return value


def checked_bool(value: Any, context: str) -> bool:
    if not isinstance(value, bool):
        raise GateError(f"{context} must be boolean")
    return value


def accepted_corpus_contract(value: Any, context: str) -> dict[str, Any]:
    contract = exact_object(value, set(AUDITED_CORPUS_CONTRACT), context)
    normalized = {
        "phase1_segments_manifest_sha256": digest(
            contract["phase1_segments_manifest_sha256"],
            f"{context}.phase1_segments_manifest_sha256",
        ),
        "gate_inventory_sha256": digest(
            contract["gate_inventory_sha256"],
            f"{context}.gate_inventory_sha256",
        ),
        "query_corpus_fingerprint_sha256": digest(
            contract["query_corpus_fingerprint_sha256"],
            f"{context}.query_corpus_fingerprint_sha256",
        ),
        "file_count": positive_int(contract["file_count"], f"{context}.file_count"),
        "total_bytes": positive_int(contract["total_bytes"], f"{context}.total_bytes"),
        "dense_event_time_span_ms": positive_int(
            contract["dense_event_time_span_ms"],
            f"{context}.dense_event_time_span_ms",
        ),
    }
    if normalized != AUDITED_CORPUS_CONTRACT:
        raise GateError(f"{context} differs from the audited Phase 1 corpus")
    return normalized


def _validate_source_manifest(document: Any) -> list[dict[str, Any]]:
    root = exact_object(
        document,
        {
            "schema",
            "description",
            "accepted_corpus",
            "expressions",
            "queries",
        },
        "manifest",
    )
    if root["schema"] != MANIFEST_SCHEMA:
        raise GateError(f"manifest schema must be {MANIFEST_SCHEMA}")
    checked_text(root["description"], "manifest.description")
    accepted_corpus_contract(root["accepted_corpus"], "manifest.accepted_corpus")
    if root["expressions"] != {"sum": SUM_EXPRESSION, "count": COUNT_EXPRESSION}:
        raise GateError("manifest expressions differ from the exact Phase 4 shapes")
    queries = root["queries"]
    if not isinstance(queries, list) or len(queries) != len(QUERY_SPECS):
        raise GateError("manifest must contain exactly the 30m, 6h, and 24h ranges")
    fields = {
        "name",
        "mode",
        "start_ms",
        "end_ms",
        "step_ms",
        "window_ms",
        "outer_range_ms",
        "expected_evaluation_count",
        "range_scalar_cache_max_bytes",
        "evidence_class",
        "dense_promotion_evidence",
        "chronoxide_query",
    }
    normalized: list[dict[str, Any]] = []
    for index, (value, expected) in enumerate(zip(queries, QUERY_SPECS, strict=True)):
        context = f"manifest.queries[{index}]"
        query = exact_object(value, fields, context)
        name, start_ms, outer_ms, evaluations, evidence, dense, expression = expected
        if checked_name(query["name"], f"{context}.name") != name:
            raise GateError(f"{context}: query name/order differs from the sealed matrix")
        expected_values = {
            "mode": "range",
            "start_ms": start_ms,
            "end_ms": END_MS,
            "step_ms": STEP_MS,
            "window_ms": WINDOW_MS,
            "outer_range_ms": outer_ms,
            "expected_evaluation_count": evaluations,
            "range_scalar_cache_max_bytes": 0,
            "evidence_class": evidence,
            "dense_promotion_evidence": dense,
            "chronoxide_query": expression,
        }
        for field, expected_value in expected_values.items():
            if query[field] != expected_value:
                raise GateError(f"{context}.{field} differs from the sealed matrix")
        if query["end_ms"] - query["start_ms"] != query["outer_range_ms"]:
            raise GateError(f"{context}: outer range does not match its bounds")
        scheduled = (query["outer_range_ms"] // query["step_ms"]) + 1
        if scheduled != query["expected_evaluation_count"]:
            raise GateError(f"{context}: evaluation count does not match the schedule")
        required_union_span = query["outer_range_ms"] + query["window_ms"]
        within_dense_span = required_union_span <= ACCEPTED_DENSE_EVENT_TIME_SPAN_MS
        if query["dense_promotion_evidence"] != within_dense_span:
            raise GateError(
                f"{context}: dense evidence classification contradicts the corpus span"
            )
        if within_dense_span != (query["evidence_class"] == "dense-real-window"):
            raise GateError(f"{context}: evidence class contradicts the corpus span")
        normalized.append(
            {
                "query_name": name,
                "mode": "range",
                "start_ms": start_ms,
                "end_ms": END_MS,
                "step_ms": STEP_MS,
                "window_ms": WINDOW_MS,
                "outer_range_ms": outer_ms,
                "expected_evaluation_count": evaluations,
                "range_scalar_cache_max_bytes": 0,
                "evidence_class": evidence,
                "dense_promotion_evidence": dense,
                "expression": expression,
            }
        )
    return normalized


def load_source_manifest(path: Path, *, require_fixed_digest: bool = True) -> list[dict[str, Any]]:
    raw = path.read_bytes()
    if require_fixed_digest and hashlib.sha256(raw).hexdigest() != SEALED_QUERY_MANIFEST_SHA256:
        raise GateError("Phase 4 query manifest bytes differ from the sealed matrix")
    return _validate_source_manifest(json.loads(raw))


def normalized_document(queries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema": NORMALIZED_SCHEMA,
        "accepted_corpus": copy.deepcopy(AUDITED_CORPUS_CONTRACT),
        "queries": queries,
    }


def normalize_manifest(input_path: Path, output_tsv: Path, output_json: Path) -> None:
    queries = load_source_manifest(input_path)
    document = normalized_document(queries)
    fields = tuple(queries[0])
    with output_tsv.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination, fieldnames=fields, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(queries)
    with output_json.open("x", encoding="utf-8") as destination:
        json.dump(document, destination, indent=2, sort_keys=True)
        destination.write("\n")


def read_manifest(path: Path, source_manifest: Path) -> list[dict[str, Any]]:
    expected = normalized_document(load_source_manifest(source_manifest))
    observed = json.loads(path.read_text(encoding="utf-8"))
    if observed != expected:
        raise GateError("normalized manifest differs from the sealed source manifest")
    return copy.deepcopy(expected["queries"])


def schedule_for_block(block: int) -> tuple[str, str, str, str]:
    positive_int(block, "block")
    return ABBA if block % 2 else BAAB


def expected_plan(queries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for query in queries:
        for block in range(1, BLOCKS + 1):
            for order_index, mode in enumerate(schedule_for_block(block), start=1):
                rows.append(
                    {
                        "process_label": (
                            f"{query['query_name']}-b{block:02d}-p{order_index}-{mode}"
                        ),
                        "query_name": query["query_name"],
                        "evidence_class": query["evidence_class"],
                        "block": block,
                        "order_index": order_index,
                        "range_execution_mode": mode,
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
                (
                    "multi_step_range_expected",
                    "Multi-Step Range Readbacks Expected",
                ),
                (
                    "multi_step_range_executed",
                    "Multi-Step Range Readbacks Executed",
                ),
                (
                    "multi_step_range_skipped",
                    "Multi-Step Range Readbacks Skipped",
                ),
            )
        }
        if metrics["expected"] <= 0 or not (
            metrics["executed"] == metrics["expected"]
            and metrics["checked"] == metrics["expected"]
            and metrics["skipped"] == 0
            and metrics["isolation_skips"] == 0
            and metrics["mismatches"] == 0
            and metrics["multi_step_range_expected"] > 0
            and metrics["multi_step_range_executed"]
            == metrics["multi_step_range_expected"]
            and metrics["multi_step_range_skipped"] == 0
        ):
            raise GateError(
                "readback evidence contains skips, omissions, mismatches, or no executed multi-step range oracle"
            )
        result.update(metrics)
    else:
        raise GateError(f"unknown smoke report kind: {kind}")
    with output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def validate_smoke_json(path: Path, kind: str) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema") != SMOKE_SCHEMA:
        raise GateError(f"{path}: invalid Phase 4 smoke evidence")
    if value.get("kind") != kind or value.get("gate") != "pass":
        raise GateError(f"{path}: wrong or failed smoke evidence")
    if kind == "footer":
        exact_object(value, {"schema", "kind", "gate", "requested", "effective"}, str(path))
        if value["requested"] is not True or value["effective"] is not True:
            raise GateError(f"{path}: footer validation was not effective")
    elif kind == "readback":
        fields = {
            "schema",
            "kind",
            "gate",
            "expected",
            "executed",
            "skipped",
            "isolation_skips",
            "checked",
            "mismatches",
            "multi_step_range_expected",
            "multi_step_range_executed",
            "multi_step_range_skipped",
        }
        exact_object(value, fields, str(path))
        metrics = {field: nonnegative_int(value[field], f"{path}.{field}") for field in fields - {"schema", "kind", "gate"}}
        if metrics["expected"] == 0 or not (
            metrics["executed"] == metrics["expected"]
            and metrics["checked"] == metrics["expected"]
            and metrics["skipped"] == 0
            and metrics["isolation_skips"] == 0
            and metrics["mismatches"] == 0
            and metrics["multi_step_range_expected"] > 0
            and metrics["multi_step_range_executed"]
            == metrics["multi_step_range_expected"]
            and metrics["multi_step_range_skipped"] == 0
        ):
            raise GateError(f"{path}: incomplete independent readback coverage")
    else:
        raise GateError(f"unknown smoke evidence kind: {kind}")
    return value


def load_inventory(
    path: Path,
    corpus: Path,
    expected_corpus: dict[str, Any] = AUDITED_CORPUS_CONTRACT,
) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    exact_object(document, INVENTORY_FIELDS, str(path))
    if document["schema"] != common.INVENTORY_SCHEMA:
        raise GateError(f"{path}: inventory schema differs")
    if document["corpus"] != os.path.realpath(corpus):
        raise GateError(f"{path}: inventory names another corpus")
    values = document["files"]
    if not isinstance(values, list) or not values:
        raise GateError(f"{path}: inventory must contain files")
    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        context = f"{path}.files[{index}]"
        entry = exact_object(value, INVENTORY_FILE_FIELDS, context)
        relative = entry["path"]
        if (
            not isinstance(relative, str)
            or not relative
            or UNSAFE_TEXT.search(relative)
            or relative in seen
            or os.path.isabs(relative)
            or os.path.normpath(relative) != relative
            or ".." in Path(relative).parts
        ):
            raise GateError(f"{context}: invalid relative path")
        seen.add(relative)
        files.append(
            {
                "path": relative,
                "size_bytes": nonnegative_int(entry["size_bytes"], f"{context}.size_bytes"),
                "sha256": digest(entry["sha256"], f"{context}.sha256"),
            }
        )
    if files != sorted(files, key=lambda entry: os.fsencode(entry["path"])):
        raise GateError(f"{path}: inventory is not canonically ordered")
    if positive_int(document["file_count"], f"{path}.file_count") != len(files):
        raise GateError(f"{path}: inventory file count differs")
    total = sum(entry["size_bytes"] for entry in files)
    if positive_int(document["total_bytes"], f"{path}.total_bytes") != total:
        raise GateError(f"{path}: inventory total bytes differs")
    expected = hashlib.sha256(canonical_json(files).encode()).hexdigest()
    if digest(document["corpus_sha256"], f"{path}.corpus_sha256") != expected:
        raise GateError(f"{path}: inventory digest differs from its file list")
    if document["corpus_sha256"] != expected_corpus["gate_inventory_sha256"]:
        raise GateError(f"{path}: inventory is not the audited Phase 1 corpus")
    if document["file_count"] != expected_corpus["file_count"]:
        raise GateError(f"{path}: audited corpus file count differs")
    if document["total_bytes"] != expected_corpus["total_bytes"]:
        raise GateError(f"{path}: audited corpus byte count differs")
    return document


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
) -> dict[str, int]:
    if max_after_evict != 0:
        raise GateError("the formal Phase 4 residency bound must be exactly zero bytes")
    rows = read_tsv(path, RESIDENCY_FIELDS, "residency summary")
    seen: set[tuple[str, str]] = set()
    observed = {"after-evict": 0, "after-run": 0}
    for row in rows:
        label = row["process_label"]
        planned = plan_by_label.get(label)
        phase = row["phase"]
        if planned is None or phase not in {"after-evict", "after-run"}:
            raise GateError("residency summary contains an unknown process or phase")
        if (
            int(row["block"]) != planned["block"]
            or row["range_execution_mode"] != planned["range_execution_mode"]
        ):
            raise GateError(f"{label}: residency metadata differs from the plan")
        key = (label, phase)
        if key in seen:
            raise GateError(f"duplicate residency row: {key!r}")
        seen.add(key)
        if positive_int(int(row["file_count"]), f"{label}.file_count") != inventory["file_count"]:
            raise GateError(f"{label}: residency file count differs from inventory")
        if nonnegative_int(int(row["corpus_file_bytes"]), f"{label}.corpus_file_bytes") != inventory["total_bytes"]:
            raise GateError(f"{label}: residency bytes differ from inventory")
        resident = nonnegative_int(int(row["resident_bytes"]), f"{label}.resident_bytes")
        if resident > inventory["total_bytes"]:
            raise GateError(f"{label}: resident bytes exceed corpus bytes")
        if phase == "after-evict" and resident > max_after_evict:
            raise GateError(f"{label}: {resident} bytes remained resident after eviction")
        observed[phase] = max(observed[phase], resident)
    expected = {(label, phase) for label in plan_by_label for phase in ("after-evict", "after-run")}
    if seen != expected:
        raise GateError("residency summary is incomplete")
    return observed


def validate_range_execution(
    value: Any, mode: str, query: dict[str, Any], context: str
) -> dict[str, Any]:
    summary = exact_object(value, RANGE_EXECUTION_FIELDS, context)
    if summary["requested_mode"] != mode or summary["effective_mode"] != mode:
        raise GateError(f"{context}: requested/effective execution mode differs")
    if summary["fallback_reason"] is not None:
        raise GateError(f"{context}: comparator unexpectedly fell back")
    if summary["terminal_reason"] is not None:
        raise GateError(f"{context}: comparator terminated after union decode")
    cache_bypassed = checked_bool(summary["cache_bypassed"], f"{context}.cache_bypassed")
    if cache_bypassed is not (mode == ONE_PASS_MODE):
        raise GateError(f"{context}: scalar-cache bypass accounting differs")
    evaluation_count = positive_int(
        summary["evaluation_count"], f"{context}.evaluation_count"
    )
    if evaluation_count != query["expected_evaluation_count"]:
        raise GateError(f"{context}: evaluation count differs from the range schedule")
    if summary["preallocation_governed"] is not False:
        raise GateError(
            f"{context}: this diagnostic contract requires honest ungoverned preallocation"
        )
    for field in (
        "source_series",
        "source_samples",
        "estimated_retained_bytes_peak",
        "retained_bytes_after_finalize",
    ):
        nonnegative_int(summary[field], f"{context}.{field}")
    if summary["retained_bytes_after_finalize"] != 0:
        raise GateError(f"{context}: retained bytes leaked past finalization")
    if mode == "repeated":
        if summary["union_start_ms"] is not None or summary["union_end_ms"] is not None:
            raise GateError(f"{context}: repeated execution reported union bounds")
        if any(
            summary[field]
            for field in (
                "source_series",
                "source_samples",
                "estimated_retained_bytes_peak",
            )
        ):
            raise GateError(f"{context}: repeated execution reported union retention")
    else:
        expected_union_start = query["start_ms"] - query["window_ms"]
        union_start_ms = nonnegative_int(
            summary["union_start_ms"], f"{context}.union_start_ms"
        )
        union_end_ms = nonnegative_int(
            summary["union_end_ms"], f"{context}.union_end_ms"
        )
        if union_start_ms != expected_union_start or union_end_ms != query["end_ms"]:
            raise GateError(
                f"{context}: one-pass-assume-scalar union bounds differ from the exact window"
            )
        if (
            summary["source_series"] == 0
            or summary["source_samples"] == 0
            or summary["estimated_retained_bytes_peak"] == 0
        ):
            raise GateError(
                f"{context}: one-pass-assume-scalar execution lacks retained-work evidence"
            )
    return copy.deepcopy(summary)


def validate_range_cache_execution_contract(
    report: dict[str, Any],
    mode: str,
    logical_payload_bytes: int,
    logical_chunk_reads: int,
    context: str,
) -> None:
    if report["configured_budget_bytes"] != 0:
        raise GateError(f"{context}: scalar range cache budget must be zero")
    if any(report[field] for field in RANGE_CACHE_BOOLEAN_FIELDS):
        raise GateError(f"{context}: zero-budget range cache reported a refusal/error")
    if any(report[field] for field in RANGE_CACHE_ZERO_CHARGE_FIELDS):
        raise GateError(f"{context}: zero-budget range cache reported a hit, admission, or charge")

    if mode == ONE_PASS_MODE:
        if any(
            report[field]
            for field in (
                "misses",
                "streaming_budget_bypasses",
                "unsupported_bypasses",
                "logical_miss_or_bypass_bytes",
            )
        ):
            raise GateError(
                f"{context}: one-pass cache_bypassed summary contradicts cache activity"
            )
    elif not (
        logical_chunk_reads > 0
        and report["misses"] == 0
        and report["streaming_budget_bypasses"] == 0
        and report["unsupported_bypasses"] == logical_chunk_reads
        and report["logical_miss_or_bypass_bytes"] == logical_payload_bytes
    ):
        raise GateError(
            f"{context}: repeated zero-budget cache accounting does not reconcile"
        )


def comparable_symbols(value: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(value)
    # This is the sole timing field in the symbol-read report. Its counters and
    # charges remain part of exact accounting.
    result["page_validation_ns_delta"] = 0
    return result


def stage_accounting(value: dict[str, int]) -> dict[str, Any]:
    return {
        "instrumentation": "off",
        "non_duration_fields": {
            field: value[field]
            for field in sorted(common.QUERY_STAGE_FIELDS - {"unclassified_ns"})
        },
    }


def payload_accounting(value: dict[str, int]) -> dict[str, Any]:
    logical = value["logical_used_bytes"]
    if logical == 0:
        raise GateError("Phase 4 payload accounting requires non-zero logical bytes")
    return {
        **copy.deepcopy(value),
        "read_used_amplification": value["physical_bytes"] / logical,
    }


def complete_accounting(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "stats": copy.deepcopy(run["stats"]),
        "payload": payload_accounting(run["payload"]),
        "scheduler": copy.deepcopy(run["scheduler"]),
        "labels": copy.deepcopy(run["labels"]),
        "label_storage": copy.deepcopy(run["label_storage"]),
        "symbols": comparable_symbols(run["symbols"]),
        "metadata": copy.deepcopy(run["metadata"]),
        "range_cache": copy.deepcopy(run["range_cache"]),
        "stages": stage_accounting(run["stages"]),
        "range_execution": copy.deepcopy(run["range_execution"]),
    }


def repeated_work_signature(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "stats": run["stats"],
        "payload": run["payload"],
        "scheduler_counters": {
            field: run["scheduler"][field]
            for field in phase3.SCHEDULER_COUNTER_FIELDS
        },
        "range_cache": run["range_cache"],
        "range_execution": run["range_execution"],
    }


def validate_raw(
    row: dict[str, str],
    query: dict[str, Any],
    corpus: Path,
    expected_corpus: dict[str, Any] = AUDITED_CORPUS_CONTRACT,
) -> tuple[str, list[dict[str, Any]]]:
    raw_path = Path(row["raw_output"])
    document = json.loads(raw_path.read_text(encoding="utf-8"))
    exact_object(document, DOCUMENT_FIELDS, str(raw_path))
    if document["schema"] != RAW_SCHEMA:
        raise GateError(f"{raw_path}: raw schema must be {RAW_SCHEMA}")
    nonnegative_int(document["corpus_fingerprint_duration_ns"], f"{raw_path}.fingerprint_duration")
    configuration = exact_object(document["configuration"], CONFIGURATION_FIELDS, f"{raw_path}.configuration")
    for field in (
        "start_ms",
        "end_ms",
        "step_ms",
        "range_scalar_cache_max_bytes",
        "chunk_read_queue_depth",
        "chunk_payload_coalesce_max_gap_bytes",
        "query_label_arena_max_bytes",
        "benchmark_repeats",
    ):
        nonnegative_int(configuration[field], f"{raw_path}.configuration.{field}")
    expected_configuration = {
        "segments_dir": os.path.realpath(corpus),
        "start_ms": query["start_ms"],
        "end_ms": query["end_ms"],
        "mode": "query_range",
        "step_ms": query["step_ms"],
        "range_scalar_cache_max_bytes": 0,
        "chunk_read_mode": "pread",
        "chunk_read_queue_depth": QUEUE_DEPTH,
        "chunk_payload_coalesce_max_gap_bytes": COALESCE_GAP_BYTES,
        "experimental_cross_segment_chunk_reads": False,
        "label_materialization": "demand-driven",
        "query_label_storage": "compact-ids",
        "query_instrumentation": "off",
        "query_label_arena_max_bytes": DEFAULT_ARENA_BYTES,
        "storage_layout": "schema8",
        "benchmark_repeats": BENCHMARK_REPEATS,
        "queries": [query["expression"]],
        "prewarm_query_contexts": False,
        "prefetch_query_data": False,
        "exponential_histogram_bucket_boundaries": [],
        "requested_segment_footer_validation": False,
        "effective_segment_footer_validation": False,
        "range_execution_mode": row["range_execution_mode"],
    }
    if configuration != expected_configuration:
        raise GateError(f"{raw_path}: timed configuration differs from the fixed invocation")
    expected_limits = {
        "max_matched_series": None,
        "max_projected_series": None,
        "max_chunk_reads": None,
        "max_bytes_read": None,
        "max_samples_decoded": None,
        "max_regex_values_examined": None,
    }
    if document["limits"] != expected_limits:
        raise GateError(f"{raw_path}: Phase 4 timed query was not unlimited")
    fingerprint = digest(document["corpus_fingerprint_sha256"], f"{raw_path}.corpus_fingerprint")
    if fingerprint != expected_corpus["query_corpus_fingerprint_sha256"]:
        raise GateError(f"{raw_path}: query fingerprint is not the audited Phase 1 corpus")
    runs = document["runs"]
    if not isinstance(runs, list) or len(runs) != BENCHMARK_REPEATS:
        raise GateError(f"{raw_path}: expected exactly {BENCHMARK_REPEATS} runs")

    validated: list[dict[str, Any]] = []
    for run_index, value in enumerate(runs):
        context = f"{raw_path}:{query['query_name']}:{run_index}"
        run = exact_object(value, RUN_FIELDS, context)
        expected_kind = "cold" if run_index == 0 else "warm"
        if run["query"] != query["expression"]:
            raise GateError(f"{context}: expression differs from the manifest")
        observed_run_index = nonnegative_int(run["run_index"], f"{context}.run_index")
        if observed_run_index != run_index or run["run_kind"] != expected_kind:
            raise GateError(f"{context}: run index/kind differs")
        observed_start_ms = nonnegative_int(
            run["effective_start_ms"], f"{context}.effective_start_ms"
        )
        observed_end_ms = nonnegative_int(
            run["effective_end_ms"], f"{context}.effective_end_ms"
        )
        observed_step_ms = positive_int(run["step_ms"], f"{context}.step_ms")
        if (
            observed_start_ms != query["start_ms"]
            or observed_end_ms != query["end_ms"]
            or observed_step_ms != query["step_ms"]
        ):
            raise GateError(f"{context}: effective range differs from the manifest")
        duration = positive_int(run["duration_ns"], f"{context}.duration_ns")
        nonnegative_int(run["post_query_fingerprint_ns"], f"{context}.post_query_fingerprint_ns")
        try:
            stats = common.validate_stats(run["stats"], context)
            stages = common.validate_query_stages(run["query_stages"], "off", duration, context)
            symbols = phase1.validate_symbol_reads(run["symbol_reads"], f"{context}.symbol_reads")
            metadata = phase1.validate_metadata_runtime(run["metadata_runtime"], f"{context}.metadata_runtime")
            range_cache = phase1.validate_range_cache(run["range_scalar_cache"], query, context)
        except (common.GateError, phase1.GateError) as error:
            raise GateError(str(error)) from error
        payload = numeric_object(run["payload_reads"], phase3.PAYLOAD_FIELDS, f"{context}.payload_reads")
        if payload["logical_used_bytes"] != stats["bytes_read"]:
            raise GateError(f"{context}: payload logical bytes differ from QueryStats")
        if payload["physical_bytes"] < payload["logical_used_bytes"]:
            raise GateError(f"{context}: physical payload bytes are below logical bytes")
        if not stats["chunk_reads"] or not stats["samples_decoded"]:
            raise GateError(f"{context}: exact scalar range performed no storage work")
        if stats["typed_scalar_chunks_decoded"] or stats["typed_full_chunks_decoded"]:
            raise GateError(
                f"{context}: successful scalar-only comparator observed typed source chunks"
            )
        if range_cache is None:
            raise GateError(f"{context}: range query lacks range-cache accounting")
        validate_range_cache_execution_contract(
            range_cache,
            row["range_execution_mode"],
            stats["bytes_read"],
            stats["chunk_reads"],
            f"{context}.range_scalar_cache",
        )
        try:
            scheduler = phase3.validate_scheduler(
                run["chunk_read_scheduler"], "pread", QUEUE_DEPTH, payload, stats, f"{context}.scheduler"
            )
            labels = phase2.validate_label_materialization(
                run["label_materialization"], "scalar-range-selective", f"{context}.labels"
            )
            storage = phase2.validate_label_storage(
                run["query_label_storage"],
                "compact-ids",
                DEFAULT_ARENA_BYTES,
                True,
                run_index == 0,
                f"{context}.query_label_storage",
            )
        except (phase2.GateError, phase3.GateError) as error:
            raise GateError(str(error)) from error
        result_series = positive_int(run["result_series"], f"{context}.result_series")
        result_samples = positive_int(run["result_samples"], f"{context}.result_samples")
        range_execution = validate_range_execution(
            run["range_execution"], row["range_execution_mode"], query, f"{context}.range_execution"
        )
        validated.append(
            {
                "run_index": run_index,
                "run_kind": expected_kind,
                "duration_ns": duration,
                "post_query_fingerprint_ns": nonnegative_int(
                    run["post_query_fingerprint_ns"], f"{context}.post_query_fingerprint_ns"
                ),
                "semantic_fingerprint": digest(run["semantic_fingerprint_sha256"], f"{context}.semantic_fingerprint"),
                "portable_fingerprint": digest(run["portable_semantic_fingerprint_sha256"], f"{context}.portable_fingerprint"),
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
                "range_execution": range_execution,
            }
        )
    return fingerprint, validated


def deterministic_accounting(run: dict[str, Any]) -> dict[str, Any]:
    return complete_accounting(run)


def result_signature(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "semantic_fingerprint": run["semantic_fingerprint"],
        "portable_fingerprint": run["portable_fingerprint"],
        "result_series": run["result_series"],
        "result_samples": run["result_samples"],
    }


def median(values: list[int | float], context: str) -> float:
    if not values:
        raise GateError(f"{context}: no observations")
    return float(statistics.median(values))


def improvement_pct(reference: float, candidate: float, context: str) -> float:
    if reference <= 0:
        raise GateError(f"{context}: reference must be positive")
    return ((reference - candidate) / reference) * 100.0


def query_contract_from_spec(spec: tuple[Any, ...]) -> dict[str, Any]:
    name, start_ms, outer_ms, evaluations, evidence, dense, expression = spec
    return {
        "query_name": name,
        "mode": "range",
        "start_ms": start_ms,
        "end_ms": END_MS,
        "step_ms": STEP_MS,
        "window_ms": WINDOW_MS,
        "outer_range_ms": outer_ms,
        "expected_evaluation_count": evaluations,
        "range_scalar_cache_max_bytes": 0,
        "evidence_class": evidence,
        "dense_promotion_evidence": dense,
        "expression": expression,
    }


def validate_reported_accounting(
    value: Any,
    mode: str,
    query: dict[str, Any],
    run_index: int,
    context: str,
) -> dict[str, Any]:
    fields = {
        "run_index",
        "stats",
        *NON_QUERY_ACCOUNTING_COMPONENTS,
    }
    accounting = exact_object(value, fields, context)
    if nonnegative_int(accounting["run_index"], f"{context}.run_index") != run_index:
        raise GateError(f"{context}: accounting run index differs")
    try:
        stats = common.validate_stats(accounting["stats"], context)
        labels = phase2.validate_label_materialization(
            accounting["labels"], "scalar-range-selective", f"{context}.labels"
        )
        storage = phase2.validate_label_storage(
            accounting["label_storage"],
            "compact-ids",
            DEFAULT_ARENA_BYTES,
            True,
            run_index == 0,
            f"{context}.label_storage",
        )
        symbols = phase1.validate_symbol_reads(
            accounting["symbols"], f"{context}.symbols"
        )
        metadata = phase1.validate_metadata_runtime(
            accounting["metadata"], f"{context}.metadata"
        )
        range_cache = phase1.validate_range_cache(
            accounting["range_cache"], query, context
        )
    except (common.GateError, phase1.GateError, phase2.GateError) as error:
        raise GateError(str(error)) from error
    if stats["typed_scalar_chunks_decoded"] or stats["typed_full_chunks_decoded"]:
        raise GateError(f"{context}: reported successful accounting contains typed chunks")
    if range_cache is None:
        raise GateError(f"{context}: reported range-cache accounting is missing")
    validate_range_cache_execution_contract(
        range_cache,
        mode,
        stats["bytes_read"],
        stats["chunk_reads"],
        f"{context}.range_cache",
    )

    payload_value = exact_object(
        accounting["payload"],
        set(phase3.PAYLOAD_FIELDS) | {"read_used_amplification"},
        f"{context}.payload",
    )
    payload = {
        field: nonnegative_int(payload_value[field], f"{context}.payload.{field}")
        for field in phase3.PAYLOAD_FIELDS
    }
    if payload["logical_used_bytes"] == 0:
        raise GateError(f"{context}: payload logical bytes must be positive")
    if payload["logical_used_bytes"] != stats["bytes_read"]:
        raise GateError(f"{context}: payload logical bytes differ from QueryStats")
    if payload["physical_bytes"] < payload["logical_used_bytes"]:
        raise GateError(f"{context}: physical payload bytes are below logical bytes")
    amplification = finite_nonnegative(
        payload_value["read_used_amplification"],
        f"{context}.payload.read_used_amplification",
    )
    expected_amplification = payload["physical_bytes"] / payload["logical_used_bytes"]
    if amplification != expected_amplification:
        raise GateError(f"{context}: payload read/used amplification is incorrect")
    try:
        scheduler = phase3.validate_scheduler(
            accounting["scheduler"],
            "pread",
            QUEUE_DEPTH,
            payload,
            stats,
            f"{context}.scheduler",
        )
    except phase3.GateError as error:
        raise GateError(str(error)) from error

    stages = exact_object(
        accounting["stages"],
        {"instrumentation", "non_duration_fields"},
        f"{context}.stages",
    )
    if stages["instrumentation"] != "off":
        raise GateError(f"{context}: reported query stages are not off-mode")
    non_duration_fields = numeric_object(
        stages["non_duration_fields"],
        common.QUERY_STAGE_FIELDS - {"unclassified_ns"},
        f"{context}.stages.non_duration_fields",
    )
    if any(non_duration_fields.values()):
        raise GateError(f"{context}: off-mode reported detailed query-stage timing")
    range_execution = validate_range_execution(
        accounting["range_execution"], mode, query, f"{context}.range_execution"
    )
    return {
        "run_index": run_index,
        "stats": stats,
        "payload": {**payload, "read_used_amplification": amplification},
        "scheduler": scheduler,
        "labels": labels,
        "label_storage": storage,
        "symbols": symbols,
        "metadata": metadata,
        "range_cache": range_cache,
        "stages": {
            "instrumentation": "off",
            "non_duration_fields": non_duration_fields,
        },
        "range_execution": range_execution,
    }


def validate_result_document(
    value: Any,
    context: str = "result",
    expected_corpus: dict[str, Any] = AUDITED_CORPUS_CONTRACT,
) -> dict[str, Any]:
    result = exact_object(value, RESULT_FIELDS, context)
    if result["schema"] != RESULT_SCHEMA:
        raise GateError(f"{context}: result schema differs")
    if result["correctness_gate"] != "pass" or result["result_equivalence_gate"] != "pass":
        raise GateError(f"{context}: correctness/result equivalence did not pass")
    if result["ordinary_query_stats_policy"] != "classified-intended-difference":
        raise GateError(f"{context}: QueryStats differences are not explicitly classified")
    if result["ordinary_query_stats_equivalent"] is not False:
        raise GateError(f"{context}: result must not claim ordinary QueryStats equivalence")
    digest(result["binary_sha256"], f"{context}.binary_sha256")
    if (
        digest(
            result["phase1_segments_manifest_sha256"],
            f"{context}.phase1_segments_manifest_sha256",
        )
        != expected_corpus["phase1_segments_manifest_sha256"]
    ):
        raise GateError(f"{context}: Phase 1 manifest provenance differs")
    if (
        digest(result["corpus_inventory_sha256"], f"{context}.corpus_inventory_sha256")
        != expected_corpus["gate_inventory_sha256"]
    ):
        raise GateError(f"{context}: audited corpus inventory digest differs")
    if positive_int(result["corpus_file_count"], f"{context}.corpus_file_count") != expected_corpus["file_count"]:
        raise GateError(f"{context}: audited corpus file count differs")
    if positive_int(result["corpus_total_bytes"], f"{context}.corpus_total_bytes") != expected_corpus["total_bytes"]:
        raise GateError(f"{context}: audited corpus byte count differs")
    if digest(
        result["query_corpus_fingerprint_sha256"],
        f"{context}.query_corpus_fingerprint_sha256",
    ) != expected_corpus["query_corpus_fingerprint_sha256"]:
        raise GateError(f"{context}: audited query corpus fingerprint differs")
    if result["raw_schema"] != RAW_SCHEMA:
        raise GateError(f"{context}: raw schema differs from the Phase 4 contract")
    expected_configuration = {
        "storage_layout": "schema8",
        "chunk_read_mode": "pread",
        "chunk_read_queue_depth": QUEUE_DEPTH,
        "chunk_payload_coalesce_max_gap_bytes": COALESCE_GAP_BYTES,
        "label_materialization": "demand-driven",
        "query_label_storage": "compact-ids",
        "query_label_arena_max_bytes": DEFAULT_ARENA_BYTES,
        "query_instrumentation": "off",
        "range_scalar_cache_max_bytes": 0,
        "query_limits": "unlimited",
    }
    if result["fixed_configuration"] != expected_configuration:
        raise GateError(f"{context}: fixed configuration differs")
    if result["blocks"] != BLOCKS or result["schedule"] != {
        "odd_blocks": list(ABBA),
        "even_blocks": list(BAAB),
    }:
        raise GateError(f"{context}: counterbalanced schedule differs")
    if result["processes_per_arm_per_query"] != PROCESSES_PER_ARM_PER_QUERY:
        raise GateError(f"{context}: process count per arm differs")
    if result["benchmark_repeats"] != BENCHMARK_REPEATS:
        raise GateError(f"{context}: benchmark repeat count differs")
    if result["warm_headline_observation_unit"] != (
        "per-process median of two warm runs"
    ):
        raise GateError(f"{context}: warm observation unit differs")
    if nonnegative_int(
        result["max_resident_bytes_after_evict"],
        f"{context}.max_resident_bytes_after_evict",
    ) != 0:
        raise GateError(f"{context}: formal eviction bound must be zero")
    if nonnegative_int(
        result["max_observed_resident_bytes_after_evict"],
        f"{context}.max_observed_resident_bytes_after_evict",
    ) != 0:
        raise GateError(f"{context}: corpus pages remained resident after eviction")
    after_run_resident = nonnegative_int(
        result["max_observed_resident_bytes_after_run"],
        f"{context}.max_observed_resident_bytes_after_run",
    )
    if after_run_resident > expected_corpus["total_bytes"]:
        raise GateError(f"{context}: after-run residency exceeds corpus bytes")
    if result["os_page_cache_eviction_gate"] != "pass":
        raise GateError(f"{context}: page-cache eviction gate did not pass")
    quiet = checked_bool(result["quiet_host_confirmed"], f"{context}.quiet_host_confirmed")
    noisy = checked_bool(result["allow_noisy_host"], f"{context}.allow_noisy_host")
    if not quiet:
        raise GateError(f"{context}: measurement host was not explicitly confirmed")
    if noisy:
        raise GateError(f"{context}: noisy-host override is forbidden")
    if result["measurement_host_status"] != "quiet-confirmed":
        raise GateError(f"{context}: host-noise classification differs")
    digest(result["run_note_sha256"], f"{context}.run_note_sha256")
    multi_expected = positive_int(
        result["multi_step_range_readbacks_expected"],
        f"{context}.multi_step_range_readbacks_expected",
    )
    multi_executed = positive_int(
        result["multi_step_range_readbacks_executed"],
        f"{context}.multi_step_range_readbacks_executed",
    )
    multi_skipped = nonnegative_int(
        result["multi_step_range_readbacks_skipped"],
        f"{context}.multi_step_range_readbacks_skipped",
    )
    if (
        result["multi_step_range_readback_gate"] != "pass"
        or multi_executed != multi_expected
        or multi_skipped != 0
    ):
        raise GateError(f"{context}: independent multi-step range oracle did not fully execute")

    classifications = result["query_stats_classification"]
    expected_classification_count = (
        len(QUERY_SPECS) * BENCHMARK_REPEATS * len(common.QUERY_STATS_FIELDS)
    )
    if (
        not isinstance(classifications, list)
        or len(classifications) != expected_classification_count
    ):
        raise GateError(f"{context}: QueryStats classification matrix is incomplete")
    classification_fields = {
        "query_name",
        "run_index",
        "field",
        "repeated",
        "one_pass",
        "delta_one_pass_minus_repeated",
        "classification",
    }
    expected_coordinates = [
        (spec[0], run_index, field)
        for spec in QUERY_SPECS
        for run_index in range(BENCHMARK_REPEATS)
        for field in common.QUERY_STATS_FIELDS
    ]
    for index, (classification, coordinate) in enumerate(
        zip(classifications, expected_coordinates, strict=True)
    ):
        entry = exact_object(
            classification, classification_fields, f"{context}.classification[{index}]"
        )
        if (entry["query_name"], entry["run_index"], entry["field"]) != coordinate:
            raise GateError(f"{context}: QueryStats classification order differs")
        repeated = nonnegative_int(
            entry["repeated"], f"{context}.classification[{index}].repeated"
        )
        one_pass = nonnegative_int(
            entry["one_pass"], f"{context}.classification[{index}].one_pass"
        )
        if one_pass > repeated:
            raise GateError(f"{context}: classified one-pass work exceeds repeated work")
        if entry["delta_one_pass_minus_repeated"] != one_pass - repeated:
            raise GateError(f"{context}: classified QueryStats delta is incorrect")
        expected_class = (
            "equal" if one_pass == repeated else "union-work-vs-repeated-logical-work"
        )
        if entry["classification"] != expected_class:
            raise GateError(f"{context}: QueryStats difference is misclassified")
    dense_names = [QUERY_SPECS[0][0], QUERY_SPECS[1][0]]
    sparse_names = [spec[0] for spec in QUERY_SPECS[2:]]
    if result["dense_promotion_evidence_query_names"] != dense_names:
        raise GateError(f"{context}: dense evidence names are invalid")
    if result["sparse_scheduler_control_query_names"] != sparse_names:
        raise GateError(f"{context}: sparse controls were relabeled")
    if result["dense_24h_evidence_gate"] != "missing":
        raise GateError(f"{context}: current corpus cannot supply dense 24h evidence")
    if result["preallocation_governance_gate"] != "failed-ungoverned-diagnostic":
        raise GateError(f"{context}: ungoverned preallocation is not reported")
    if result["production_promotion_verdict"] != "forbidden":
        raise GateError(f"{context}: this diagnostic artifact must forbid promotion")
    if result["candidate_disposition"] != "defer":
        raise GateError(f"{context}: current candidate disposition must be defer")
    blockers = result["promotion_blockers"]
    if not isinstance(blockers, list) or set(blockers) != {
        "one-pass-assume-scalar union preallocation is estimated but not governed",
        "finite QueryLimits and their error precedence are not exercised",
        "ordinary QueryStats describe union work rather than repeated per-step work",
        "the current corpus has no dense 24-hour event-time range",
    }:
        raise GateError(f"{context}: promotion blockers are incomplete")
    measurements = result["measurements"]
    if not isinstance(measurements, list) or len(measurements) != len(QUERY_SPECS):
        raise GateError(f"{context}: measurement matrix is incomplete")
    reported_accounting: dict[tuple[str, str, int], dict[str, Any]] = {}
    for index, (measurement, spec) in enumerate(
        zip(measurements, QUERY_SPECS, strict=True)
    ):
        measurement = exact_object(
            measurement,
            {
                "query_name",
                "outer_range_ms",
                "expected_evaluation_count",
                "evidence_class",
                "dense_promotion_evidence",
                "arms",
                "one_pass_vs_repeated",
            },
            f"{context}.measurements[{index}]",
        )
        name, _start, outer_ms, evaluations, evidence, dense, _expression = spec
        query_contract = query_contract_from_spec(spec)
        expected = {
            "query_name": name,
            "outer_range_ms": outer_ms,
            "expected_evaluation_count": evaluations,
            "evidence_class": evidence,
            "dense_promotion_evidence": dense,
        }
        for field, expected_value in expected.items():
            if measurement.get(field) != expected_value:
                raise GateError(
                    f"{context}: measurements[{index}].{field} relabels the sealed evidence"
                )
        if set(measurement.get("arms", {})) != set(MODES):
            raise GateError(f"{context}: measurements[{index}] has incomplete arms")
        arm_fields = {
            "cold_duration_ns",
            "cold_median_ns",
            "warm_duration_ns",
            "process_warm_median_ns",
            "warm_median_ns",
            "process_max_rss_kib",
            "process_max_rss_median_kib",
            "accounting_by_run_index",
        }
        for mode in MODES:
            arm = exact_object(
                measurement["arms"][mode],
                arm_fields,
                f"{context}.measurements[{index}].arms.{mode}",
            )
            sequences = (
                ("cold_duration_ns", PROCESSES_PER_ARM_PER_QUERY),
                (
                    "warm_duration_ns",
                    PROCESSES_PER_ARM_PER_QUERY * (BENCHMARK_REPEATS - 1),
                ),
                ("process_warm_median_ns", PROCESSES_PER_ARM_PER_QUERY),
                ("process_max_rss_kib", PROCESSES_PER_ARM_PER_QUERY),
            )
            for field, length in sequences:
                sequence = arm[field]
                if not isinstance(sequence, list) or len(sequence) != length:
                    raise GateError(
                        f"{context}: measurements[{index}].{mode}.{field} is incomplete"
                    )
                for value_index, item in enumerate(sequence):
                    if field == "process_warm_median_ns":
                        finite_nonnegative(
                            item,
                            f"{context}.measurements[{index}].{mode}.{field}[{value_index}]",
                        )
                    else:
                        positive_int(
                            item,
                            f"{context}.measurements[{index}].{mode}.{field}[{value_index}]",
                        )
            reported_medians = (
                ("cold_median_ns", "cold_duration_ns"),
                ("warm_median_ns", "process_warm_median_ns"),
                ("process_max_rss_median_kib", "process_max_rss_kib"),
            )
            for median_field, sequence_field in reported_medians:
                observed = finite_nonnegative(
                    arm[median_field],
                    f"{context}.measurements[{index}].{mode}.{median_field}",
                )
                expected_median = median(
                    arm[sequence_field],
                    f"{context}.measurements[{index}].{mode}.{sequence_field}",
                )
                if observed != expected_median:
                    raise GateError(
                        f"{context}: measurements[{index}].{mode}.{median_field} is incorrect"
                    )
            accounting = arm["accounting_by_run_index"]
            if not isinstance(accounting, list) or len(accounting) != BENCHMARK_REPEATS:
                raise GateError(
                    f"{context}: measurements[{index}].{mode} accounting is incomplete"
                )
            for run_index, accounting_value in enumerate(accounting):
                validated_accounting = validate_reported_accounting(
                    accounting_value,
                    mode,
                    query_contract,
                    run_index,
                    f"{context}.measurements[{index}].{mode}.accounting[{run_index}]",
                )
                reported_accounting[(name, mode, run_index)] = validated_accounting
        comparison = measurement.get("one_pass_vs_repeated")
        if (
            not isinstance(comparison, dict)
            or set(comparison)
            != {
                "cold_latency_improvement_pct",
                "warm_latency_improvement_pct",
                "rss_improvement_pct",
                "verdict",
            }
            or comparison.get("verdict") != "diagnostic-only-no-promotion"
        ):
            raise GateError(
                f"{context}: measurements[{index}] makes a per-query promotion claim"
            )
        metric_pairs = (
            ("cold_latency_improvement_pct", "cold_median_ns"),
            ("warm_latency_improvement_pct", "warm_median_ns"),
            ("rss_improvement_pct", "process_max_rss_median_kib"),
        )
        for comparison_field, arm_field in metric_pairs:
            expected_improvement = improvement_pct(
                measurement["arms"]["repeated"][arm_field],
                measurement["arms"][ONE_PASS_MODE][arm_field],
                f"{context}.measurements[{index}].{comparison_field}",
            )
            observed_improvement = finite_number(
                comparison[comparison_field],
                f"{context}.measurements[{index}].{comparison_field}",
            )
            if not math.isclose(
                observed_improvement,
                expected_improvement,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                raise GateError(
                    f"{context}: measurements[{index}].{comparison_field} is incorrect"
                )

    for classification in classifications:
        coordinate = (
            classification["query_name"],
            "repeated",
            classification["run_index"],
        )
        repeated_accounting = reported_accounting[coordinate]
        one_pass_accounting = reported_accounting[
            (
                classification["query_name"],
                ONE_PASS_MODE,
                classification["run_index"],
            )
        ]
        field = classification["field"]
        if (
            classification["repeated"] != repeated_accounting["stats"][field]
            or classification["one_pass"] != one_pass_accounting["stats"][field]
        ):
            raise GateError(
                f"{context}: QueryStats classification is detached from reported accounting"
            )

    non_query_classifications = result["non_query_stats_accounting_classification"]
    expected_non_query_coordinates = [
        (spec[0], run_index, component)
        for spec in QUERY_SPECS
        for run_index in range(BENCHMARK_REPEATS)
        for component in NON_QUERY_ACCOUNTING_COMPONENTS
    ]
    fields = {
        "query_name",
        "run_index",
        "component",
        "repeated_sha256",
        "one_pass_sha256",
        "classification",
    }
    if (
        not isinstance(non_query_classifications, list)
        or len(non_query_classifications) != len(expected_non_query_coordinates)
    ):
        raise GateError(f"{context}: non-QueryStats classification matrix is incomplete")
    for index, (classification, coordinate) in enumerate(
        zip(non_query_classifications, expected_non_query_coordinates, strict=True)
    ):
        entry = exact_object(
            classification,
            fields,
            f"{context}.non_query_classification[{index}]",
        )
        if (entry["query_name"], entry["run_index"], entry["component"]) != coordinate:
            raise GateError(f"{context}: non-QueryStats classification order differs")
        query_name, run_index, component = coordinate
        repeated_value = reported_accounting[(query_name, "repeated", run_index)][component]
        one_pass_value = reported_accounting[(query_name, ONE_PASS_MODE, run_index)][component]
        repeated_sha = canonical_digest(repeated_value)
        one_pass_sha = canonical_digest(one_pass_value)
        if (
            digest(entry["repeated_sha256"], f"{context}.non_query.repeated")
            != repeated_sha
            or digest(entry["one_pass_sha256"], f"{context}.non_query.one_pass")
            != one_pass_sha
        ):
            raise GateError(
                f"{context}: non-QueryStats classification is detached from accounting"
            )
        expected_class = (
            "equal"
            if repeated_value == one_pass_value
            else "execution-strategy-accounting-difference"
        )
        if entry["classification"] != expected_class:
            raise GateError(f"{context}: non-QueryStats accounting is misclassified")
    return result


def compare_results(
    args: argparse.Namespace,
    expected_corpus: dict[str, Any] = AUDITED_CORPUS_CONTRACT,
) -> None:
    queries = read_manifest(args.manifest, args.source_manifest)
    query_by_name = {query["query_name"]: query for query in queries}
    plan = expected_plan(queries)
    plan_by_label = {row["process_label"]: row for row in plan}
    before = load_inventory(args.inventory_before, args.corpus, expected_corpus)
    after = load_inventory(args.inventory_after, args.corpus, expected_corpus)
    if before != after:
        raise GateError("corpus inventory changed during the experiment")
    validate_smoke_json(args.footer_validation, "footer")
    readback = validate_smoke_json(args.readback_validation, "readback")
    residency = validate_residency(
        args.residency,
        plan_by_label,
        before,
        args.max_resident_bytes_after_evict,
    )
    if args.quiet_host_confirmed not in (0, 1) or args.allow_noisy_host not in (0, 1):
        raise GateError("host confirmation values must be zero or one")
    quiet_host_confirmed = bool(args.quiet_host_confirmed)
    allow_noisy_host = bool(args.allow_noisy_host)
    if not quiet_host_confirmed:
        raise GateError("formal measurement requires quiet-host confirmation")
    if allow_noisy_host:
        raise GateError("noisy-host override is forbidden for formal evidence")
    run_note_bytes = args.run_note_file.read_bytes()
    if not run_note_bytes.endswith(b"\n") or run_note_bytes.endswith(b"\n\n"):
        raise GateError("run note must end in exactly one newline")
    run_note = checked_text(run_note_bytes[:-1].decode("utf-8"), "run note")
    run_note_sha256 = hashlib.sha256((run_note + "\n").encode()).hexdigest()

    binary_hash = file_sha256(args.binary)
    rows = read_tsv(args.index, INDEX_FIELDS, "raw index")
    if len(rows) != len(plan):
        raise GateError(f"expected {len(plan)} processes, found {len(rows)}")
    if [row["process_label"] for row in rows] != [row["process_label"] for row in plan]:
        raise GateError("raw-index sequence differs from the counterbalanced plan")
    processes: dict[tuple[str, int, int], dict[str, Any]] = {}
    fingerprints: set[str] = set()
    for row, planned in zip(rows, plan, strict=True):
        label = row["process_label"]
        for field in ("query_name", "evidence_class", "range_execution_mode"):
            if row[field] != str(planned[field]):
                raise GateError(f"{label}: raw-index {field} differs from the plan")
        if int(row["block"]) != planned["block"] or int(row["order_index"]) != planned["order_index"]:
            raise GateError(f"{label}: raw-index block/order differs from the plan")
        if row["binary_sha256"] != binary_hash:
            raise GateError(f"{label}: raw-index names another binary")
        if row["corpus"] != os.path.realpath(args.corpus):
            raise GateError(f"{label}: raw-index names another corpus")
        expected_raw = args.runs_dir.resolve() / label / "raw.json"
        raw_path = Path(row["raw_output"])
        if raw_path.resolve() != expected_raw or not raw_path.is_file() or raw_path.is_symlink():
            raise GateError(f"{label}: raw output is not the canonical run artifact")
        wall = finite_tsv_number(row["process_wall_seconds"], f"{label}.wall")
        user = finite_tsv_number(row["process_user_seconds"], f"{label}.user")
        system = finite_tsv_number(row["process_system_seconds"], f"{label}.system")
        rss = positive_int(int(row["max_rss_kib"]), f"{label}.max_rss_kib")
        fingerprint, runs = validate_raw(
            row,
            query_by_name[row["query_name"]],
            args.corpus,
            expected_corpus,
        )
        fingerprints.add(fingerprint)
        processes[(row["query_name"], int(row["block"]), int(row["order_index"]))] = {
            "index": row,
            "wall": wall,
            "user": user,
            "system": system,
            "rss": rss,
            "runs": runs,
        }
    if len(fingerprints) != 1:
        raise GateError("query processes observed different corpus fingerprints")

    for process in processes.values():
        label = process["index"]["process_label"]
        baseline = repeated_work_signature(process["runs"][0])
        for run in process["runs"][1:]:
            if repeated_work_signature(run) != baseline:
                raise GateError(
                    f"{label}: storage work or range summary changed cold-to-warm"
                )

    classifications: list[dict[str, Any]] = []
    non_query_classifications: list[dict[str, Any]] = []
    canonical_runs: dict[tuple[str, str, int], dict[str, Any]] = {}
    for query in queries:
        query_name = query["query_name"]
        signatures: set[str] = set()
        for run_index in range(BENCHMARK_REPEATS):
            for mode in MODES:
                observations = [
                    process["runs"][run_index]
                    for process in processes.values()
                    if process["index"]["query_name"] == query_name
                    and process["index"]["range_execution_mode"] == mode
                ]
                if len(observations) != PROCESSES_PER_ARM_PER_QUERY:
                    raise GateError(f"{query_name} {mode}: arm is incomplete")
                accounting = deterministic_accounting(observations[0])
                if any(deterministic_accounting(value) != accounting for value in observations[1:]):
                    raise GateError(f"{query_name} {mode} run {run_index}: accounting is nondeterministic")
                canonical_runs[(query_name, mode, run_index)] = observations[0]
                signatures.update(canonical_json(result_signature(value)) for value in observations)
            repeated = canonical_runs[(query_name, "repeated", run_index)]["stats"]
            one_pass = canonical_runs[(query_name, ONE_PASS_MODE, run_index)]["stats"]
            for field in common.QUERY_STATS_FIELDS:
                if one_pass[field] > repeated[field]:
                    raise GateError(
                        f"{query_name} run {run_index}: one-pass-assume-scalar {field} exceeds repeated work"
                    )
                classifications.append(
                    {
                        "query_name": query_name,
                        "run_index": run_index,
                        "field": field,
                        "repeated": repeated[field],
                        "one_pass": one_pass[field],
                        "delta_one_pass_minus_repeated": one_pass[field] - repeated[field],
                        "classification": (
                            "equal" if one_pass[field] == repeated[field] else "union-work-vs-repeated-logical-work"
                        ),
                    }
                )
            repeated_accounting = complete_accounting(
                canonical_runs[(query_name, "repeated", run_index)]
            )
            one_pass_accounting = complete_accounting(
                canonical_runs[(query_name, ONE_PASS_MODE, run_index)]
            )
            for component in NON_QUERY_ACCOUNTING_COMPONENTS:
                repeated_value = repeated_accounting[component]
                one_pass_value = one_pass_accounting[component]
                non_query_classifications.append(
                    {
                        "query_name": query_name,
                        "run_index": run_index,
                        "component": component,
                        "repeated_sha256": canonical_digest(repeated_value),
                        "one_pass_sha256": canonical_digest(one_pass_value),
                        "classification": (
                            "equal"
                            if repeated_value == one_pass_value
                            else "execution-strategy-accounting-difference"
                        ),
                    }
                )
        if len(signatures) != 1:
            raise GateError(
                f"{query_name}: exact/portable fingerprints or result shape/order differ across arms/runs"
            )

    summary_fields = [
        "process_label",
        "query_name",
        "evidence_class",
        "block",
        "order_index",
        "range_execution_mode",
        "binary_sha256",
        "run_index",
        "run_kind",
        "duration_ns",
        "process_wall_seconds",
        "process_user_seconds",
        "process_system_seconds",
        "max_rss_kib",
        "semantic_fingerprint",
        "portable_fingerprint",
        "result_series",
        "result_samples",
        *(f"stats_{field}" for field in common.QUERY_STATS_FIELDS),
        "payload_logical_used_bytes",
        "payload_physical_reads",
        "payload_physical_bytes",
        "payload_read_used_amplification",
        "labels_json",
        "label_storage_json",
        "symbols_json",
        "metadata_json",
        "range_cache_json",
        "stages_json",
        "range_execution_json",
        "scheduler_json",
    ]
    with args.summary.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=summary_fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for planned in plan:
            process = processes[(planned["query_name"], planned["block"], planned["order_index"])]
            for run in process["runs"]:
                row: dict[str, Any] = {
                    field: process["index"][field]
                    for field in (
                        "process_label",
                        "query_name",
                        "evidence_class",
                        "block",
                        "order_index",
                        "range_execution_mode",
                        "binary_sha256",
                    )
                }
                row.update(
                    {
                        "run_index": run["run_index"],
                        "run_kind": run["run_kind"],
                        "duration_ns": run["duration_ns"],
                        "process_wall_seconds": process["wall"],
                        "process_user_seconds": process["user"],
                        "process_system_seconds": process["system"],
                        "max_rss_kib": process["rss"],
                        "semantic_fingerprint": run["semantic_fingerprint"],
                        "portable_fingerprint": run["portable_fingerprint"],
                        "result_series": run["result_series"],
                        "result_samples": run["result_samples"],
                        "payload_logical_used_bytes": run["payload"]["logical_used_bytes"],
                        "payload_physical_reads": run["payload"]["physical_reads"],
                        "payload_physical_bytes": run["payload"]["physical_bytes"],
                        "payload_read_used_amplification": (
                            run["payload"]["physical_bytes"]
                            / run["payload"]["logical_used_bytes"]
                        ),
                        "labels_json": canonical_json(run["labels"]),
                        "label_storage_json": canonical_json(run["label_storage"]),
                        "symbols_json": canonical_json(comparable_symbols(run["symbols"])),
                        "metadata_json": canonical_json(run["metadata"]),
                        "range_cache_json": canonical_json(run["range_cache"]),
                        "stages_json": canonical_json(stage_accounting(run["stages"])),
                        "range_execution_json": canonical_json(run["range_execution"]),
                        "scheduler_json": canonical_json(run["scheduler"]),
                    }
                )
                row.update({f"stats_{field}": run["stats"][field] for field in common.QUERY_STATS_FIELDS})
                writer.writerow(row)

    measurements: list[dict[str, Any]] = []
    for query in queries:
        arms: dict[str, dict[str, Any]] = {}
        for mode in MODES:
            matching = sorted(
                (
                    process
                    for process in processes.values()
                    if process["index"]["query_name"] == query["query_name"]
                    and process["index"]["range_execution_mode"] == mode
                ),
                key=lambda process: (int(process["index"]["block"]), int(process["index"]["order_index"])),
            )
            cold = [process["runs"][0]["duration_ns"] for process in matching]
            warm_all = [run["duration_ns"] for process in matching for run in process["runs"][1:]]
            warm_per_process = [median([run["duration_ns"] for run in process["runs"][1:]], "process warm") for process in matching]
            rss = [process["rss"] for process in matching]
            arms[mode] = {
                "cold_duration_ns": cold,
                "cold_median_ns": median(cold, f"{query['query_name']} {mode} cold"),
                "warm_duration_ns": warm_all,
                "process_warm_median_ns": warm_per_process,
                "warm_median_ns": median(warm_per_process, f"{query['query_name']} {mode} warm"),
                "process_max_rss_kib": rss,
                "process_max_rss_median_kib": median(rss, f"{query['query_name']} {mode} RSS"),
                "accounting_by_run_index": [
                    {
                        "run_index": run_index,
                        **deterministic_accounting(
                            canonical_runs[(query["query_name"], mode, run_index)]
                        ),
                    }
                    for run_index in range(BENCHMARK_REPEATS)
                ],
            }
        measurements.append(
            {
                "query_name": query["query_name"],
                "outer_range_ms": query["outer_range_ms"],
                "expected_evaluation_count": query["expected_evaluation_count"],
                "evidence_class": query["evidence_class"],
                "dense_promotion_evidence": query["dense_promotion_evidence"],
                "arms": arms,
                "one_pass_vs_repeated": {
                    "cold_latency_improvement_pct": improvement_pct(arms["repeated"]["cold_median_ns"], arms[ONE_PASS_MODE]["cold_median_ns"], "cold latency"),
                    "warm_latency_improvement_pct": improvement_pct(arms["repeated"]["warm_median_ns"], arms[ONE_PASS_MODE]["warm_median_ns"], "warm latency"),
                    "rss_improvement_pct": improvement_pct(arms["repeated"]["process_max_rss_median_kib"], arms[ONE_PASS_MODE]["process_max_rss_median_kib"], "RSS"),
                    "verdict": "diagnostic-only-no-promotion",
                },
            }
        )

    result = {
        "schema": RESULT_SCHEMA,
        "correctness_gate": "pass",
        "result_equivalence_gate": "pass",
        "ordinary_query_stats_policy": "classified-intended-difference",
        "ordinary_query_stats_equivalent": False,
        "query_stats_classification": classifications,
        "non_query_stats_accounting_classification": non_query_classifications,
        "binary_sha256": binary_hash,
        "phase1_segments_manifest_sha256": expected_corpus[
            "phase1_segments_manifest_sha256"
        ],
        "corpus_inventory_sha256": before["corpus_sha256"],
        "corpus_file_count": before["file_count"],
        "corpus_total_bytes": before["total_bytes"],
        "query_corpus_fingerprint_sha256": next(iter(fingerprints)),
        "raw_schema": RAW_SCHEMA,
        "fixed_configuration": {
            "storage_layout": "schema8",
            "chunk_read_mode": "pread",
            "chunk_read_queue_depth": QUEUE_DEPTH,
            "chunk_payload_coalesce_max_gap_bytes": COALESCE_GAP_BYTES,
            "label_materialization": "demand-driven",
            "query_label_storage": "compact-ids",
            "query_label_arena_max_bytes": DEFAULT_ARENA_BYTES,
            "query_instrumentation": "off",
            "range_scalar_cache_max_bytes": 0,
            "query_limits": "unlimited",
        },
        "blocks": BLOCKS,
        "schedule": {"odd_blocks": list(ABBA), "even_blocks": list(BAAB)},
        "processes_per_arm_per_query": PROCESSES_PER_ARM_PER_QUERY,
        "benchmark_repeats": BENCHMARK_REPEATS,
        "warm_headline_observation_unit": "per-process median of two warm runs",
        "max_resident_bytes_after_evict": args.max_resident_bytes_after_evict,
        "max_observed_resident_bytes_after_evict": residency["after-evict"],
        "max_observed_resident_bytes_after_run": residency["after-run"],
        "os_page_cache_eviction_gate": "pass",
        "quiet_host_confirmed": quiet_host_confirmed,
        "allow_noisy_host": allow_noisy_host,
        "measurement_host_status": "quiet-confirmed",
        "run_note_sha256": run_note_sha256,
        "multi_step_range_readback_gate": "pass",
        "multi_step_range_readbacks_expected": readback[
            "multi_step_range_expected"
        ],
        "multi_step_range_readbacks_executed": readback[
            "multi_step_range_executed"
        ],
        "multi_step_range_readbacks_skipped": readback[
            "multi_step_range_skipped"
        ],
        "dense_promotion_evidence_query_names": [QUERY_SPECS[0][0], QUERY_SPECS[1][0]],
        "sparse_scheduler_control_query_names": [spec[0] for spec in QUERY_SPECS[2:]],
        "dense_24h_evidence_gate": "missing",
        "preallocation_governance_gate": "failed-ungoverned-diagnostic",
        "production_promotion_verdict": "forbidden",
        "candidate_disposition": "defer",
        "promotion_blockers": [
            "one-pass-assume-scalar union preallocation is estimated but not governed",
            "finite QueryLimits and their error precedence are not exercised",
            "ordinary QueryStats describe union work rather than repeated per-step work",
            "the current corpus has no dense 24-hour event-time range",
        ],
        "measurements": measurements,
    }
    validate_result_document(result, expected_corpus=expected_corpus)
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")


def validate_result(
    path: Path,
    expected_corpus: dict[str, Any] = AUDITED_CORPUS_CONTRACT,
) -> dict[str, Any]:
    return validate_result_document(
        json.loads(path.read_text(encoding="utf-8")),
        str(path),
        expected_corpus,
    )


def _require_same_bytes(actual: Path, expected: Path, context: str) -> None:
    if actual.read_bytes() != expected.read_bytes():
        raise GateError(f"{context} is detached from its leaf evidence")


def _decode_nul_fields(path: Path, context: str) -> list[str]:
    raw = path.read_bytes()
    if not raw or not raw.endswith(b"\0"):
        raise GateError(f"{context} must be a non-empty NUL-terminated record stream")
    try:
        values = [item.decode("utf-8") for item in raw[:-1].split(b"\0")]
    except UnicodeDecodeError as error:
        raise GateError(f"{context} is not UTF-8") from error
    if any(not value or "\0" in value for value in values):
        raise GateError(f"{context} contains an empty or invalid field")
    return values


def _expected_query_argv(
    binary: Path,
    corpus: Path,
    query: dict[str, Any],
    execution_mode: str,
    run_dir: Path,
) -> list[str]:
    return [
        str(binary),
        "--segments-dir",
        str(corpus),
        "--storage-layout",
        "schema8",
        "--label-materialization",
        "demand-driven",
        "--query-label-storage",
        "compact-ids",
        "--query-label-arena-max-bytes",
        str(DEFAULT_ARENA_BYTES),
        "--query-instrumentation",
        "off",
        "--start-ms",
        str(query["start_ms"]),
        "--end-ms",
        str(query["end_ms"]),
        "--step-ms",
        str(query["step_ms"]),
        "--benchmark-repeats",
        str(BENCHMARK_REPEATS),
        "--chunk-read-mode",
        "pread",
        "--chunk-read-queue-depth",
        str(QUEUE_DEPTH),
        "--chunk-payload-coalesce-max-gap-bytes",
        str(COALESCE_GAP_BYTES),
        "--range-scalar-cache-max-bytes",
        "0",
        "--range-execution-mode",
        execution_mode,
        "--query-unlimited",
        "--output",
        str(run_dir / "report.md"),
        "--raw-output",
        str(run_dir / "raw.json"),
        "--query",
        str(query["expression"]),
    ]


def _read_time_tsv(path: Path) -> dict[str, str]:
    expected_keys = (
        "process_wall_seconds",
        "process_user_seconds",
        "process_system_seconds",
        "max_rss_kib",
        "exit_status",
    )
    rows = path.read_text(encoding="utf-8").splitlines()
    if len(rows) != len(expected_keys):
        raise GateError(f"{path}: time evidence has an invalid row count")
    values: dict[str, str] = {}
    for row, expected_key in zip(rows, expected_keys, strict=True):
        fields = row.split("\t")
        if len(fields) != 2 or fields[0] != expected_key:
            raise GateError(f"{path}: time evidence has an invalid field order")
        values[fields[0]] = fields[1]
    for key in expected_keys[:3]:
        finite_tsv_number(values[key], f"{path}.{key}")
    if not re.fullmatch(r"[1-9][0-9]*", values["max_rss_kib"]):
        raise GateError(f"{path}: max_rss_kib must be a positive integer")
    if values["exit_status"] != "0":
        raise GateError(f"{path}: timed process did not exit successfully")
    return values


def _expected_inventory_paths(inventory: dict[str, Any]) -> list[str]:
    corpus = Path(inventory["corpus"])
    return [str(corpus / entry["path"]) for entry in inventory["files"]]


def _residency_detail(
    path: Path, inventory: dict[str, Any], context: str
) -> tuple[int, int, int]:
    fields = _decode_nul_fields(path, context)
    if len(fields) % 3:
        raise GateError(f"{context} must contain resident/size/path triples")
    expected_files = _expected_inventory_paths(inventory)
    if len(fields) // 3 != len(expected_files):
        raise GateError(f"{context} file count differs from inventory")
    total_resident = 0
    total_size = 0
    for index, expected_path in enumerate(expected_files):
        resident_text, size_text, observed_path = fields[index * 3 : index * 3 + 3]
        if not resident_text.isdecimal() or not size_text.isdecimal():
            raise GateError(f"{context} contains a non-numeric residency field")
        if observed_path != expected_path:
            raise GateError(f"{context} path order differs from inventory")
        size = int(size_text)
        expected_size = inventory["files"][index]["size_bytes"]
        if size != expected_size:
            raise GateError(f"{context} size differs from inventory")
        resident = int(resident_text)
        page_size = os.sysconf("SC_PAGE_SIZE")
        rounded_size = ((size + page_size - 1) // page_size) * page_size
        if resident > rounded_size:
            raise GateError(f"{context} resident bytes exceed page-rounded file size")
        total_resident += resident
        total_size += size
    return len(expected_files), total_resident, total_size


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=tuple(rows[0]),
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def validate_process_leaf_authority(
    base: Path, manifest_name: str, leaf_names: set[str]
) -> None:
    manifest = base / manifest_name
    if (
        manifest.is_symlink()
        or not manifest.is_file()
        or stat.S_IMODE(manifest.stat().st_mode) != 0o444
    ):
        raise GateError(f"process leaf authority is not read-only: {manifest}")
    validate_relative_hash_manifest(base, manifest, leaf_names)
    for leaf_name in leaf_names:
        leaf = base / leaf_name
        if stat.S_IMODE(leaf.stat().st_mode) != 0o444:
            raise GateError(f"sealed process leaf is not read-only: {leaf}")


def guardian_success_leaf_names(prefix: str) -> set[str]:
    return {
        f"{prefix}.guardian-immediate-conflicts.json",
        f"{prefix}.guardian-control.json",
        f"{prefix}.guardian-ready",
        f"{prefix}.guardian-launch",
        f"{prefix}.guardian-samples.tsv",
        f"{prefix}.guardian-conflicts.tsv",
        f"{prefix}.guardian-summary.json",
        f"{prefix}.guardian.log",
        f"{prefix}.guardian-exit-status",
    }


def validate_guardian_prefix(base: Path, prefix: str) -> None:
    guardian_log = base / f"{prefix}.guardian.log"
    _regular_file(guardian_log, "successful process guardian log")
    if guardian_log.stat().st_size != 0:
        raise GateError("successful process guardian log must be empty")
    validate_guardian_evidence(
        base / f"{prefix}.guardian-samples.tsv",
        base / f"{prefix}.guardian-conflicts.tsv",
        base / f"{prefix}.guardian-summary.json",
        base / f"{prefix}.guardian-control.json",
        base / f"{prefix}.guardian-exit-status",
        base / f"{prefix}.guardian-ready",
        base / f"{prefix}.guardian-launch",
        base / f"{prefix}.guardian-immediate-conflicts.json",
    )


def verify_leaf_evidence(
    result_dir: Path,
    expected_corpus: dict[str, Any] = AUDITED_CORPUS_CONTRACT,
) -> None:
    """Rebuild every decision-bearing derivative from immutable leaf records."""
    if result_dir.is_symlink():
        raise GateError("leaf-evidence result root must not be a symlink")
    root = result_dir.resolve(strict=True)
    metadata = root / "metadata"
    validation = root / "validation"
    validate_process_leaf_authority(
        validation,
        "footer-leaves.sha256",
        guardian_success_leaf_names("footer")
        | {
            "processes-before-footer.txt",
            "processes-immediate-before-footer.txt",
            "footer.time.txt",
            "footer.md",
            "footer.log",
            "footer.exit-status",
        },
    )
    for prefix in ("footer", "readbacks"):
        validate_guardian_prefix(validation, prefix)
    validate_process_leaf_authority(
        validation,
        "readbacks-leaves.sha256",
        guardian_success_leaf_names("readbacks")
        | {
            "processes-before-readbacks.txt",
            "processes-immediate-before-readbacks.txt",
            "readbacks.time.txt",
            "readbacks.md",
            "readbacks.log",
            "readbacks.exit-status",
        },
    )
    source_manifest = metadata / "harness" / "phase4_range_one_pass_queries.json"
    if os.path.lexists(metadata / "query-manifest.input.json"):
        raise GateError("obsolete duplicate query-manifest.input.json is forbidden")
    queries = read_manifest(root / "queries.normalized.json", source_manifest)
    query_by_name = {query["query_name"]: query for query in queries}
    plan = expected_plan(queries)
    inventory_before = load_inventory(
        root / "inventory" / "before.json",
        Path(json.loads((root / "inventory" / "before.json").read_text())["corpus"]),
        expected_corpus,
    )
    inventory_after = load_inventory(
        root / "inventory" / "after.json",
        Path(json.loads((root / "inventory" / "after.json").read_text())["corpus"]),
        expected_corpus,
    )
    if inventory_before != inventory_after:
        raise GateError("before/after corpus inventories differ")
    expected_paths = _expected_inventory_paths(inventory_before)
    expected_paths_bytes = b"".join(
        os.fsencode(path) + b"\0" for path in expected_paths
    )
    for path in (
        root / "inventory" / "files.nul",
        root / "inventory" / "files-after.nul",
    ):
        if path.read_bytes() != expected_paths_bytes:
            raise GateError(f"{path}: path stream is detached from inventory JSON")

    binary = metadata / "chronoxide-query"
    binary_hash = file_sha256(binary)
    corpus = Path(inventory_before["corpus"])
    index_rows: list[dict[str, Any]] = []
    residency_rows: list[dict[str, Any]] = []
    for planned in plan:
        label = planned["process_label"]
        run_dir = root / "runs" / label
        validate_process_leaf_authority(
            run_dir,
            "run-leaves.sha256",
            guardian_success_leaf_names("timed")
            | {
                "argv.nul",
                "processes-before.txt",
                "processes-immediate-before.txt",
                "pressure-before.txt",
                "residency-after-evict.nul",
                "raw.json",
                "report.md",
                "query.log",
                "time.tsv",
                "exit-status",
                "pressure-after.txt",
                "processes-after.txt",
                "residency-after-run.nul",
            },
        )
        validate_guardian_prefix(run_dir, "timed")
        query = query_by_name[planned["query_name"]]
        expected_argv = _expected_query_argv(
            binary,
            corpus,
            query,
            planned["range_execution_mode"],
            run_dir,
        )
        observed_argv = _decode_nul_fields(run_dir / "argv.nul", f"{label} argv")
        if observed_argv != expected_argv:
            raise GateError(f"{label}: argv is detached from the fixed run contract")
        time_values = _read_time_tsv(run_dir / "time.tsv")
        if (run_dir / "exit-status").read_bytes() != b"0\n":
            raise GateError(f"{label}: exit-status is not zero")
        for snapshot_name in (
            "processes-before.txt",
            "processes-immediate-before.txt",
            "processes-after.txt",
        ):
            validate_process_snapshot(run_dir / snapshot_name, set())
        index_rows.append(
            {
                **planned,
                "binary_sha256": binary_hash,
                "corpus": str(corpus),
                "raw_output": str(run_dir / "raw.json"),
                "process_wall_seconds": time_values["process_wall_seconds"],
                "process_user_seconds": time_values["process_user_seconds"],
                "process_system_seconds": time_values["process_system_seconds"],
                "max_rss_kib": time_values["max_rss_kib"],
            }
        )
        for phase, filename in (
            ("after-evict", "residency-after-evict.nul"),
            ("after-run", "residency-after-run.nul"),
        ):
            file_count, resident_bytes, corpus_bytes = _residency_detail(
                run_dir / filename, inventory_before, f"{label} {phase} residency"
            )
            residency_rows.append(
                {
                    "process_label": label,
                    "block": planned["block"],
                    "range_execution_mode": planned["range_execution_mode"],
                    "phase": phase,
                    "file_count": file_count,
                    "resident_bytes": resident_bytes,
                    "corpus_file_bytes": corpus_bytes,
                }
            )
    for snapshot in (
        root / "validation" / "processes-before-footer.txt",
        root / "validation" / "processes-immediate-before-footer.txt",
        root / "validation" / "processes-before-readbacks.txt",
        root / "validation" / "processes-immediate-before-readbacks.txt",
    ):
        validate_process_snapshot(snapshot, set())
    for status in (
        root / "validation" / "footer.exit-status",
        root / "validation" / "readbacks.exit-status",
    ):
        if status.read_bytes() != b"0\n":
            raise GateError(f"validation process did not exit successfully: {status}")

    with tempfile.TemporaryDirectory(prefix="phase4-final-verify-") as directory:
        temporary = Path(directory)
        generated_tsv = temporary / "queries.tsv"
        generated_json = temporary / "queries.json"
        normalize_manifest(source_manifest, generated_tsv, generated_json)
        _require_same_bytes(root / "queries.tsv", generated_tsv, "normalized TSV")
        _require_same_bytes(
            root / "queries.normalized.json", generated_json, "normalized JSON"
        )
        generated_plan = temporary / "run-plan.tsv"
        write_plan(generated_json, source_manifest, generated_plan)
        _require_same_bytes(root / "run-plan.tsv", generated_plan, "run plan")
        generated_index = temporary / "raw-index.tsv"
        generated_residency = temporary / "residency-summary.tsv"
        _write_rows(generated_index, index_rows)
        _write_rows(generated_residency, residency_rows)
        _require_same_bytes(root / "raw-index.tsv", generated_index, "raw index")
        _require_same_bytes(
            root / "residency-summary.tsv",
            generated_residency,
            "residency summary",
        )
        for kind in ("footer", "readback"):
            generated_smoke = temporary / f"{kind}.json"
            validate_smoke_report(
                kind,
                root / "validation" / ("footer.md" if kind == "footer" else "readbacks.md"),
                generated_smoke,
            )
            _require_same_bytes(
                root / "validation" / ("footer.json" if kind == "footer" else "readbacks.json"),
                generated_smoke,
                f"{kind} validation JSON",
            )
        generated_result = temporary / "result.json"
        generated_summary = temporary / "summary.tsv"
        compare_args = argparse.Namespace(
            index=generated_index,
            manifest=generated_json,
            source_manifest=source_manifest,
            inventory_before=root / "inventory" / "before.json",
            inventory_after=root / "inventory" / "after.json",
            residency=generated_residency,
            footer_validation=root / "validation" / "footer.json",
            readback_validation=root / "validation" / "readbacks.json",
            summary=generated_summary,
            output=generated_result,
            binary=binary,
            corpus=corpus,
            runs_dir=root / "runs",
            max_resident_bytes_after_evict=0,
            quiet_host_confirmed=1,
            allow_noisy_host=0,
            run_note_file=metadata / "run-note.txt",
        )
        compare_results(compare_args, expected_corpus)
        _require_same_bytes(
            root / "comparisons" / "result-gate.json",
            generated_result,
            "result gate JSON",
        )
        _require_same_bytes(root / "summary.tsv", generated_summary, "summary TSV")


def expected_sealed_artifacts(root: Path) -> set[str]:
    metadata = {
        "chronoxide-query",
        "query-help.txt",
        "query-binary.sha256",
        "query-binary.stat.txt",
        "harness.sha256",
        "fadvise-regular-dontneed",
        "fadvise.sha256",
        "controlled-inputs.sha256",
        "settings.txt",
        "run-note.txt",
        "environment.txt",
        "seal-checks.tsv",
        "result-artifacts.nul",
        "result-directories.nul",
    }
    expected = {f"metadata/{name}" for name in metadata}
    harness = {
        "phase4_range_one_pass_run.sh",
        "phase4_range_one_pass_gate.py",
        "phase4_range_one_pass_guard.py",
        "phase4_range_one_pass_queries.json",
        "phase4_range_one_pass_plan.md",
        "test_phase4_range_one_pass_gate.py",
        "test_phase4_range_one_pass_guard.py",
        "phase3_payload_coalescing_gate.py",
        "phase2_compact_ids_ab_gate.py",
        "schema8_query_ab_gate.py",
        "schema7_query_ab_gate.py",
        "phase1_query_gate.py",
        "fadvise_regular_dontneed.c",
    }
    expected.update(f"metadata/harness/{name}" for name in harness)
    expected.update(
        {
            "metadata/source/formal-source-seal.json",
            "metadata/source/source-snapshot-seal.json",
            "metadata/source/source-head.tar",
            "metadata/source/source-head.tar.sha256",
            "metadata/build/build-environment.tsv",
            "metadata/build/build-argv.nul",
            "metadata/build/build.log",
            "metadata/build/build.exit-status",
            "metadata/build/cargo-version.txt",
            "metadata/build/rustc-version.txt",
            "metadata/build/rustup-active-toolchain.txt",
            "metadata/build/tool-paths.tsv",
            "metadata/build/tool-binaries.sha256",
            "metadata/build/cargo-metadata.json",
            "metadata/build/cargo-config-isolation-before-metadata.json",
            "metadata/build/cargo-config-isolation-after-metadata.json",
            "metadata/build/cargo-config-isolation-after-build.json",
            "metadata/build/cargo-config-isolation-final.json",
            "metadata/build/source-check-before-build.json",
            "metadata/build/source-check-after-build.json",
            "metadata/build/source-check-final.json",
            "metadata/build/source-snapshot-check-before-build.json",
            "metadata/build/source-snapshot-check-after-build.json",
            "metadata/build/source-snapshot-check-final.json",
            "metadata/build/source-archive-check-before-build.txt",
            "metadata/build/source-archive-check-after-build.txt",
            "metadata/build/source-archive-check-final.txt",
            "metadata/build/build-input-provenance.sha256",
            "metadata/build/build-provenance.sha256",
        }
    )
    expected.update(
        {
            "COMPLETE",
            "comparisons/result-gate.json",
            "summary.tsv",
            "raw-index.tsv",
            "residency-summary.tsv",
            "queries.tsv",
            "queries.normalized.json",
            "run-plan.tsv",
            "inventory/before.json",
            "inventory/before.sha256",
            "inventory/files.nul",
            "inventory/after.json",
            "inventory/after.sha256",
            "inventory/files-after.nul",
            "validation/processes-before-footer.txt",
            "validation/processes-immediate-before-footer.txt",
            "validation/footer.guardian-immediate-conflicts.json",
            "validation/footer.guardian-control.json",
            "validation/footer.guardian-exit-status",
            "validation/footer.guardian-ready",
            "validation/footer.guardian-launch",
            "validation/footer.guardian-samples.tsv",
            "validation/footer.guardian-conflicts.tsv",
            "validation/footer.guardian-summary.json",
            "validation/footer.guardian.log",
            "validation/footer.time.txt",
            "validation/footer.md",
            "validation/footer.log",
            "validation/footer.exit-status",
            "validation/footer.json",
            "validation/footer-leaves.sha256",
            "validation/processes-before-readbacks.txt",
            "validation/processes-immediate-before-readbacks.txt",
            "validation/readbacks.guardian-immediate-conflicts.json",
            "validation/readbacks.guardian-control.json",
            "validation/readbacks.guardian-exit-status",
            "validation/readbacks.guardian-ready",
            "validation/readbacks.guardian-launch",
            "validation/readbacks.guardian-samples.tsv",
            "validation/readbacks.guardian-conflicts.tsv",
            "validation/readbacks.guardian-summary.json",
            "validation/readbacks.guardian.log",
            "validation/readbacks.time.txt",
            "validation/readbacks.md",
            "validation/readbacks.log",
            "validation/readbacks.exit-status",
            "validation/readbacks.json",
            "validation/readbacks-leaves.sha256",
        }
    )
    queries = read_manifest(
        root / "queries.normalized.json",
        root
        / "metadata"
        / "harness"
        / "phase4_range_one_pass_queries.json",
    )
    per_process = {
        "argv.nul",
        "processes-before.txt",
        "processes-immediate-before.txt",
        "timed.guardian-immediate-conflicts.json",
        "timed.guardian-control.json",
        "timed.guardian-exit-status",
        "timed.guardian-ready",
        "timed.guardian-launch",
        "timed.guardian-samples.tsv",
        "timed.guardian-conflicts.tsv",
        "timed.guardian-summary.json",
        "timed.guardian.log",
        "pressure-before.txt",
        "residency-after-evict.nul",
        "raw.json",
        "report.md",
        "query.log",
        "time.tsv",
        "exit-status",
        "pressure-after.txt",
        "processes-after.txt",
        "residency-after-run.nul",
        "run-leaves.sha256",
    }
    for process in expected_plan(queries):
        label = process["process_label"]
        expected.update(f"runs/{label}/{name}" for name in per_process)
    return expected


def _checked_final_relative_path(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value or UNSAFE_TEXT.search(value):
        raise GateError(f"{context} is not a safe relative artifact path")
    path = PurePosixPath(value)
    if path.is_absolute() or path.as_posix() != value or ".." in path.parts:
        raise GateError(f"{context} is not a canonical relative artifact path")
    return value


def expected_source_snapshot_membership(root: Path) -> tuple[set[str], set[str]]:
    seal_path = root / "metadata" / "source" / "source-snapshot-seal.json"
    document = exact_object(
        _load_json(seal_path),
        {
            "schema",
            "repo",
            "snapshot",
            "git_head",
            "git_tree",
            "source_seal_identity_sha256",
            "object_format",
            "file_count",
            "files",
            "identity_sha256",
        },
        "source snapshot membership seal",
    )
    if document["schema"] != SOURCE_SNAPSHOT_SEAL_SCHEMA:
        raise GateError("source snapshot membership seal has the wrong schema")
    if document["snapshot"] != str(root / "build-source"):
        raise GateError("source snapshot membership seal names another snapshot")
    files = document["files"]
    if not isinstance(files, list) or not files:
        raise GateError("source snapshot membership seal contains no files")
    if positive_int(
        document["file_count"], "source snapshot membership file_count"
    ) != len(files):
        raise GateError("source snapshot membership file count differs")

    expected_files: set[str] = set()
    expected_directories = {"build-source"}
    observed_relative: list[str] = []
    for index, value in enumerate(files):
        entry = exact_object(
            value,
            {"path", "mode", "object_id", "size_bytes"},
            f"source snapshot membership files[{index}]",
        )
        relative = _checked_final_relative_path(
            entry["path"], f"source snapshot membership files[{index}].path"
        )
        if entry["mode"] not in {"100644", "100755"}:
            raise GateError("source snapshot membership contains an invalid file mode")
        if not isinstance(entry["object_id"], str) or not re.fullmatch(
            r"[0-9a-f]{40}|[0-9a-f]{64}", entry["object_id"]
        ):
            raise GateError("source snapshot membership contains an invalid object id")
        nonnegative_int(
            entry["size_bytes"],
            f"source snapshot membership files[{index}].size_bytes",
        )
        observed_relative.append(relative)
        expected_files.add(f"build-source/{relative}")
        parent = PurePosixPath(relative).parent
        while parent != PurePosixPath("."):
            expected_directories.add(f"build-source/{parent.as_posix()}")
            parent = parent.parent
    if len(expected_files) != len(files):
        raise GateError("source snapshot membership contains a duplicate path")
    if observed_relative != sorted(observed_relative):
        raise GateError("source snapshot membership is not canonically ordered")
    return expected_files, expected_directories


def _artifact_parent_directories(files: set[str]) -> set[str]:
    directories: set[str] = set()
    for relative in files:
        parent = PurePosixPath(relative).parent
        while parent != PurePosixPath("."):
            directories.add(parent.as_posix())
            parent = parent.parent
    return directories


def _in_dynamic_non_evidence_subtree(relative: str) -> bool:
    return any(
        relative == prefix or relative.startswith(f"{prefix}/")
        for prefix in FINAL_DYNAMIC_NON_EVIDENCE_SUBTREES
    )


def validate_final_artifact_matrix(
    root: Path, observed_files: list[str], observed_directories: list[str]
) -> None:
    """Require exact evidence paths; only named build/cache subtrees are dynamic."""
    expected_files = expected_sealed_artifacts(root) - FINAL_INVENTORY_AUTHORITY_FILES
    snapshot_files, snapshot_directories = expected_source_snapshot_membership(root)
    expected_files.update(snapshot_files)
    expected_directories = _artifact_parent_directories(expected_files)
    expected_directories.update(snapshot_directories)
    expected_directories.update(
        {
            "metadata/build/home",
            "metadata/build/cargo-home",
        }
    )

    observed_file_set = set(observed_files)
    observed_directory_set = set(observed_directories)
    expected_files.update(
        relative
        for relative in observed_file_set
        if _in_dynamic_non_evidence_subtree(relative)
    )
    expected_directories.update(
        relative
        for relative in observed_directory_set
        if _in_dynamic_non_evidence_subtree(relative)
    )
    unexpected_files = sorted(observed_file_set - expected_files, key=os.fsencode)
    missing_files = sorted(expected_files - observed_file_set, key=os.fsencode)
    unexpected_directories = sorted(
        observed_directory_set - expected_directories, key=os.fsencode
    )
    missing_directories = sorted(
        expected_directories - observed_directory_set, key=os.fsencode
    )
    if unexpected_files:
        raise GateError(
            "final artifact contains an unexpected evidence file: "
            f"{unexpected_files[0]}"
        )
    if missing_files:
        raise GateError(
            f"final artifact is missing an exact evidence file: {missing_files[0]}"
        )
    if unexpected_directories:
        raise GateError(
            "final artifact contains an unexpected evidence directory: "
            f"{unexpected_directories[0]}"
        )
    if missing_directories:
        raise GateError(
            "final artifact is missing an exact evidence directory: "
            f"{missing_directories[0]}"
        )


def validate_relative_hash_manifest(
    base: Path, path: Path, expected_names: set[str]
) -> None:
    manifest_name = path.name
    observed: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = SHA256_LINE.fullmatch(line)
        if match is None:
            raise GateError(f"{manifest_name} contains an invalid checksum line")
        expected, recorded_path = match.groups()
        candidate = Path(recorded_path)
        if (
            UNSAFE_TEXT.search(recorded_path)
            or candidate.is_absolute()
            or os.path.normpath(recorded_path) != recorded_path
            or ".." in candidate.parts
        ):
            raise GateError(f"{manifest_name} contains an unsafe path")
        if recorded_path in observed:
            raise GateError(f"{manifest_name} contains a duplicate path")
        observed[recorded_path] = expected
    if set(observed) != expected_names:
        raise GateError(f"{manifest_name} does not cover its exact frozen input set")
    for name, expected in observed.items():
        candidate = base / name
        if candidate.is_symlink() or not candidate.is_file() or file_sha256(candidate) != expected:
            raise GateError(f"{manifest_name} does not match frozen file {name}")


def validate_inner_hash_manifest(
    metadata: Path, manifest_name: str, expected_names: set[str]
) -> None:
    validate_relative_hash_manifest(
        metadata, metadata / manifest_name, expected_names
    )


BUILD_PROVENANCE_PATHS = {
    "metadata/chronoxide-query",
    "metadata/source/formal-source-seal.json",
    "metadata/source/source-snapshot-seal.json",
    "metadata/source/source-head.tar",
    "metadata/source/source-head.tar.sha256",
    "metadata/build/build-environment.tsv",
    "metadata/build/build-argv.nul",
    "metadata/build/build.log",
    "metadata/build/build.exit-status",
    "metadata/build/cargo-version.txt",
    "metadata/build/rustc-version.txt",
    "metadata/build/rustup-active-toolchain.txt",
    "metadata/build/tool-paths.tsv",
    "metadata/build/tool-binaries.sha256",
    "metadata/build/cargo-metadata.json",
    "metadata/build/cargo-config-isolation-before-metadata.json",
    "metadata/build/cargo-config-isolation-after-metadata.json",
    "metadata/build/cargo-config-isolation-after-build.json",
    "metadata/build/cargo-config-isolation-final.json",
    "metadata/build/source-check-before-build.json",
    "metadata/build/source-check-after-build.json",
    "metadata/build/source-check-final.json",
    "metadata/build/source-snapshot-check-before-build.json",
    "metadata/build/source-snapshot-check-after-build.json",
    "metadata/build/source-snapshot-check-final.json",
    "metadata/build/source-archive-check-before-build.txt",
    "metadata/build/source-archive-check-after-build.txt",
    "metadata/build/source-archive-check-final.txt",
    "metadata/build/build-input-provenance.sha256",
}
BUILD_INPUT_PROVENANCE_PATHS = BUILD_PROVENANCE_PATHS - {
    "metadata/build/cargo-config-isolation-final.json",
    "metadata/build/source-check-final.json",
    "metadata/build/source-snapshot-check-final.json",
    "metadata/build/source-archive-check-final.txt",
    "metadata/build/build-input-provenance.sha256",
}


def validate_build_provenance(root: Path) -> None:
    build = root / "metadata" / "build"
    validate_relative_hash_manifest(
        root,
        build / "build-input-provenance.sha256",
        BUILD_INPUT_PROVENANCE_PATHS,
    )
    validate_relative_hash_manifest(
        root, build / "build-provenance.sha256", BUILD_PROVENANCE_PATHS
    )
    archive = root / "metadata" / "source" / "source-head.tar"
    archive_manifest = root / "metadata" / "source" / "source-head.tar.sha256"
    archive_lines = archive_manifest.read_text(encoding="utf-8").splitlines()
    if len(archive_lines) != 1:
        raise GateError("source archive checksum authority has an invalid shape")
    archive_match = SHA256_LINE.fullmatch(archive_lines[0])
    if (
        archive_match is None
        or archive_match.group(2) != str(archive)
        or archive_match.group(1) != file_sha256(archive)
    ):
        raise GateError("source archive checksum authority is detached from archive")
    if (build / "build.exit-status").read_bytes() != b"0\n":
        raise GateError("formal source-bound build did not exit successfully")
    environment_rows = read_tsv(
        build / "build-environment.tsv", {"name", "value"}, "build environment"
    )
    environment = {row["name"]: row["value"] for row in environment_rows}
    required_environment = {
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
        "profile",
        "target",
        "features",
    }
    if set(environment) != required_environment or len(environment_rows) != len(
        required_environment
    ):
        raise GateError("formal build environment has an invalid exact shape")
    if (
        environment["CARGO_INCREMENTAL"] != "0"
        or environment["CARGO_TERM_COLOR"] != "never"
        or environment["LC_ALL"] != "C"
        or environment["TZ"] != "UTC"
        or environment["profile"] != "release"
        or environment["features"] != "default"
        or not re.fullmatch(r"[A-Za-z0-9_.-]+", environment["target"])
        or not environment["SOURCE_DATE_EPOCH"].isdecimal()
    ):
        raise GateError("formal build environment differs from the fixed contract")
    tool_rows = read_tsv(
        build / "tool-paths.tsv", {"name", "path"}, "build tool paths"
    )
    tools = {row["name"]: Path(row["path"]) for row in tool_rows}
    if set(tools) != {"cargo", "rustc", "rustup"} or len(tool_rows) != 3:
        raise GateError("formal build tool paths have an invalid exact shape")
    for name, path in tools.items():
        if not path.is_absolute() or path.is_symlink() or not path.is_file():
            raise GateError(f"formal build {name} path is no longer a regular file")
    tool_hash_lines = (build / "tool-binaries.sha256").read_text(
        encoding="utf-8"
    ).splitlines()
    if len(tool_hash_lines) != 3:
        raise GateError("formal build tool checksum file has an invalid shape")
    observed_tool_hashes: dict[str, str] = {}
    for line in tool_hash_lines:
        match = SHA256_LINE.fullmatch(line)
        if match is None:
            raise GateError("formal build tool checksum file has an invalid line")
        checksum, recorded_path = match.groups()
        observed_tool_hashes[recorded_path] = checksum
    if set(observed_tool_hashes) != {str(path) for path in tools.values()}:
        raise GateError("formal build tool checksum paths differ from tool-paths.tsv")
    for path_text, checksum in observed_tool_hashes.items():
        if file_sha256(Path(path_text)) != checksum:
            raise GateError(f"formal build tool changed: {path_text}")
    expected_argv = [
        str(tools["cargo"]),
        "build",
        "--locked",
        "--release",
        "--target",
        environment["target"],
        "-p",
        "chronoxide-query-cli",
        "--bin",
        "chronoxide-query",
    ]
    if _decode_nul_fields(build / "build-argv.nul", "formal build argv") != expected_argv:
        raise GateError("formal build argv differs from the exact build contract")
    cargo_metadata = _load_json(build / "cargo-metadata.json")
    if (
        not isinstance(cargo_metadata, dict)
        or cargo_metadata.get("workspace_root") != str(root / "build-source")
        or not isinstance(cargo_metadata.get("packages"), list)
        or not any(
            package.get("name") == "chronoxide-query-cli"
            for package in cargo_metadata["packages"]
            if isinstance(package, dict)
        )
    ):
        raise GateError("cargo metadata is detached from the sealed source snapshot")


def verify_seal(
    result_dir: Path,
    expected_corpus: dict[str, Any] = AUDITED_CORPUS_CONTRACT,
) -> None:
    if result_dir.is_symlink():
        raise GateError("sealed result root must not be a symlink")
    root = result_dir.resolve()
    if root != Path(os.path.abspath(result_dir)) or not root.is_dir() or not (root / "COMPLETE").is_file():
        raise GateError("sealed result must be a directory containing COMPLETE")
    metadata = root / "metadata"
    files_inventory = metadata / "result-artifacts.nul"
    directories_inventory = metadata / "result-directories.nul"
    manifest_path = metadata / "result-artifacts.sha256"
    for path in (files_inventory, directories_inventory, manifest_path):
        if path.is_symlink() or not path.is_file():
            raise GateError(f"sealed result lacks final inventory authority: {path.name}")
    observed_files = _decode_nul_fields(files_inventory, "final file inventory")
    observed_directories = _decode_nul_fields(
        directories_inventory, "final directory inventory"
    )
    current_files, current_directories = final_artifact_inventory(root)
    if observed_files != current_files:
        raise GateError("final file inventory differs from fail-closed traversal")
    if observed_directories != current_directories:
        raise GateError("final directory inventory differs from fail-closed traversal")
    validate_final_artifact_matrix(root, current_files, current_directories)
    lines = manifest_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise GateError("result-artifacts checksum manifest is empty")
    seen: set[str] = set()
    for line in lines:
        match = SHA256_LINE.fullmatch(line)
        if match is None:
            raise GateError("result-artifacts checksum manifest has an invalid line")
        expected, relative = match.groups()
        candidate = Path(relative)
        if (
            candidate.is_absolute()
            or os.path.normpath(relative) != relative
            or ".." in candidate.parts
            or UNSAFE_TEXT.search(relative)
            or relative in seen
        ):
            raise GateError("result-artifacts checksum manifest has an unsafe path")
        seen.add(relative)
        artifact = root / candidate
        if artifact.is_symlink() or not artifact.is_file() or file_sha256(artifact) != expected:
            raise GateError(f"sealed artifact is missing or changed: {relative}")
    expected_hashed = set(observed_files) | {
        "metadata/result-artifacts.nul",
        "metadata/result-directories.nul",
    }
    if seen != expected_hashed:
        raise GateError("result checksum manifest differs from the exact file inventory")
    required = expected_sealed_artifacts(root)
    if not required <= expected_hashed:
        missing = sorted(required - expected_hashed)
        raise GateError(
            "result checksum manifest does not cover all canonical evidence: "
            + ", ".join(missing[:3])
        )
    result = validate_result(
        root / "comparisons" / "result-gate.json", expected_corpus
    )
    if file_sha256(metadata / "chronoxide-query") != result["binary_sha256"]:
        raise GateError("sealed query binary differs from the measured binary digest")
    harness = metadata / "harness"
    source_manifest = harness / "phase4_range_one_pass_queries.json"
    if file_sha256(source_manifest) != SEALED_QUERY_MANIFEST_SHA256:
        raise GateError("sealed input manifest differs from the fixed Phase 4 manifest")
    if file_sha256(metadata / "run-note.txt") != result["run_note_sha256"]:
        raise GateError("sealed run note differs from the result provenance")
    validate_inner_hash_manifest(
        metadata, "query-binary.sha256", {"chronoxide-query"}
    )
    validate_inner_hash_manifest(
        metadata, "fadvise.sha256", {"fadvise-regular-dontneed"}
    )
    validate_inner_hash_manifest(
        metadata,
        "harness.sha256",
        {
            "harness/phase4_range_one_pass_run.sh",
            "harness/phase4_range_one_pass_gate.py",
            "harness/phase4_range_one_pass_guard.py",
            "harness/phase4_range_one_pass_queries.json",
            "harness/phase4_range_one_pass_plan.md",
            "harness/test_phase4_range_one_pass_gate.py",
            "harness/test_phase4_range_one_pass_guard.py",
            "harness/phase3_payload_coalescing_gate.py",
            "harness/phase2_compact_ids_ab_gate.py",
            "harness/schema8_query_ab_gate.py",
            "harness/schema7_query_ab_gate.py",
            "harness/phase1_query_gate.py",
            "harness/fadvise_regular_dontneed.c",
        },
    )
    validate_relative_hash_manifest(
        root,
        metadata / "controlled-inputs.sha256",
        {
            "metadata/chronoxide-query",
            "metadata/fadvise-regular-dontneed",
            "metadata/harness/phase4_range_one_pass_queries.json",
            "queries.tsv",
            "queries.normalized.json",
            "run-plan.tsv",
            "inventory/before.json",
            "inventory/files.nul",
        },
    )
    source = metadata / "source"
    source_seal_path = source / "formal-source-seal.json"
    source_document = _load_json(source_seal_path)
    repo = Path(source_document["repo"])
    snapshot = root / "build-source"
    check_source_seal(repo, source_seal_path)
    check_source_snapshot_seal(
        repo,
        snapshot,
        source_seal_path,
        source / "source-snapshot-seal.json",
    )
    cargo_config_isolation(snapshot, metadata / "build" / "cargo-home")
    validate_build_provenance(root)
    verify_leaf_evidence(root, expected_corpus)


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

    commands.add_parser("check-ambient-env")

    process_snapshot = commands.add_parser("validate-process-snapshot")
    process_snapshot.add_argument("--snapshot", type=Path, required=True)
    process_snapshot.add_argument("--allow-pid", type=int, action="append", default=[])

    process_guardian = commands.add_parser("validate-process-guardian")
    process_guardian.add_argument("--samples", type=Path, required=True)
    process_guardian.add_argument("--conflicts", type=Path, required=True)
    process_guardian.add_argument("--summary", type=Path, required=True)
    process_guardian.add_argument("--control", type=Path, required=True)
    process_guardian.add_argument("--exit-status", type=Path, required=True)
    process_guardian.add_argument("--ready", type=Path, required=True)
    process_guardian.add_argument("--launch", type=Path, required=True)
    process_guardian.add_argument(
        "--immediate-conflicts", type=Path, required=True
    )

    final_inventory = commands.add_parser("final-artifact-inventory")
    final_inventory.add_argument("--result-dir", type=Path, required=True)
    final_inventory.add_argument("--files-output", type=Path, required=True)
    final_inventory.add_argument("--directories-output", type=Path, required=True)

    leaf = commands.add_parser("verify-leaf-evidence")
    leaf.add_argument("--result-dir", type=Path, required=True)

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

    smoke = commands.add_parser("validate-smoke-report")
    smoke.add_argument("--kind", choices=("footer", "readback"), required=True)
    smoke.add_argument("--report", type=Path, required=True)
    smoke.add_argument("--output", type=Path, required=True)

    compare = commands.add_parser("compare-results")
    compare.add_argument("--index", type=Path, required=True)
    compare.add_argument("--manifest", type=Path, required=True)
    compare.add_argument("--source-manifest", type=Path, required=True)
    compare.add_argument("--inventory-before", type=Path, required=True)
    compare.add_argument("--inventory-after", type=Path, required=True)
    compare.add_argument("--residency", type=Path, required=True)
    compare.add_argument("--footer-validation", type=Path, required=True)
    compare.add_argument("--readback-validation", type=Path, required=True)
    compare.add_argument("--summary", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--binary", type=Path, required=True)
    compare.add_argument("--corpus", type=Path, required=True)
    compare.add_argument("--runs-dir", type=Path, required=True)
    compare.add_argument("--max-resident-bytes-after-evict", type=int, required=True)
    compare.add_argument("--quiet-host-confirmed", type=int, required=True)
    compare.add_argument("--allow-noisy-host", type=int, required=True)
    compare.add_argument("--run-note-file", type=Path, required=True)

    validate = commands.add_parser("validate-result")
    validate.add_argument("--result", type=Path, required=True)

    seal = commands.add_parser("verify-seal")
    seal.add_argument("--result-dir", type=Path, required=True)
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
                        args.repo, args.snapshot, args.source_seal, args.seal
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
        elif args.command == "check-ambient-env":
            forbidden = forbidden_ambient_environment(dict(os.environ))
            if forbidden:
                raise GateError(
                    "forbidden ambient build/runtime variables: "
                    + ", ".join(forbidden)
                )
        elif args.command == "validate-process-snapshot":
            validate_process_snapshot(args.snapshot, set(args.allow_pid))
        elif args.command == "validate-process-guardian":
            validate_guardian_evidence(
                args.samples,
                args.conflicts,
                args.summary,
                args.control,
                args.exit_status,
                args.ready,
                args.launch,
                args.immediate_conflicts,
            )
        elif args.command == "final-artifact-inventory":
            write_final_artifact_inventory(
                args.result_dir, args.files_output, args.directories_output
            )
        elif args.command == "verify-leaf-evidence":
            verify_leaf_evidence(args.result_dir)
        elif args.command == "normalize-manifest":
            normalize_manifest(args.input, args.output_tsv, args.output_json)
        elif args.command == "write-plan":
            write_plan(args.manifest, args.source_manifest, args.output)
        elif args.command == "inventory":
            common.write_inventory(args.corpus, args.output, args.paths_output)
            load_inventory(args.output, args.corpus)
        elif args.command == "validate-smoke-report":
            validate_smoke_report(args.kind, args.report, args.output)
        elif args.command == "compare-results":
            nonnegative_int(args.max_resident_bytes_after_evict, "max resident bytes")
            compare_results(args)
        elif args.command == "validate-result":
            validate_result(args.result)
        elif args.command == "verify-seal":
            verify_seal(args.result_dir)
        else:
            raise AssertionError(args.command)
    except (
        GateError,
        common.GateError,
        phase1.GateError,
        phase2.GateError,
        phase3.GateError,
        json.JSONDecodeError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        print(f"Phase 4 range one-pass-assume-scalar gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
