#!/usr/bin/env python3
"""Continuous fail-closed disk and host-conflict guard for Phase 5 replay."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import signal
import stat
import tempfile
import time
from pathlib import Path
from typing import Any, NamedTuple


CONTROL_SCHEMA = "chronoxide/head-topology-guardian-control/v1"
CLEANUP_SCHEMA = "chronoxide/head-topology-guardian-cleanup/v1"
GUARDIAN_SCHEMA = "chronoxide/head-topology-guardian/v2"
RSS_SCHEMA = "chronoxide/head-topology-rss-monitor/v2"
CONFLICT_SCAN_SCHEMA = "chronoxide/head-topology-conflict-scan/v1"
CADENCE_EDGE_ALLOWANCE_NS = 100_000_000


class GuardError(RuntimeError):
    """Fail-closed lifecycle or guardian contract violation."""


FORBIDDEN_EXACT = {
    "cargo",
    "cargo-nextest",
    "rustc",
    "rustdoc",
    "clippy-driver",
    "nextest",
    "make",
    "ninja",
    "ninja.real",
    "cmake",
    "meson",
    "sccache",
    "ccache",
    "docker",
    "podman",
    "buildah",
    "gcc",
    "g++",
    "cc",
    "cc1",
    "cc1plus",
    "clang",
    "clang++",
    "ld",
    "ld.lld",
    "ld.bfd",
    "ld.gold",
    "lld",
    "mold",
    "perf",
    "heaptrack",
    "strace",
    "ltrace",
    "bpftrace",
    "hotspot",
    "java",
    "javac",
    "gradle",
    "gradlew",
    "GradleDaemon",
    "mvn",
    "ctest",
    "pytest",
    "soong_ui",
    "soong_ui.bash",
    "soong_build",
    "soong_build.bash",
    "ckati",
    "kati",
    "kotlinc",
    "metalava",
    "aapt",
    "aapt2",
    "aidl",
    "dex2oat",
    "btop",
    "htop",
    "top",
    "mysqld",
    "influxd",
    "prometheus",
    "vmstorage",
    "vmselect",
    "vmagent",
    "qemu-kvm",
    "emulator",
    "adb",
    "btop",
    "htop",
    "top",
}
FORBIDDEN_PREFIXES = (
    "valgrind",
    "chronoxide-",
    "greptime",
    "clickhouse",
    "postgres",
    "victoria",
    "mimir",
    "thanos",
    "cortex",
    "qemu-system",
)
FORBIDDEN_EXACT_CASEFOLD = frozenset(value.casefold() for value in FORBIDDEN_EXACT)
FORBIDDEN_PREFIXES_CASEFOLD = tuple(
    value.casefold() for value in FORBIDDEN_PREFIXES
)
FORBIDDEN_COMMAND_MARKERS = (
    "org.gradle.",
    "GradleWorkerMain",
    "com.android.build.gradle",
    "/Android/Sdk/emulator/",
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
FORBIDDEN_VARIANT_NAME = re.compile(
    rf"^(?:cargo(?:-nextest)?|nextest|{NINJA_PROCESS_TOKEN}|"
    rf"{COMPILER_PROCESS_TOKEN}|{LINKER_PROCESS_TOKEN}|{SOONG_PROCESS_TOKEN})$",
    re.IGNORECASE,
)
FORBIDDEN_VARIANT_COMMAND = re.compile(
    rf"(?:^|[/ ])(?:cargo(?:-nextest)?|rustc|rustdoc|clippy-driver|nextest|"
    rf"{NINJA_PROCESS_TOKEN}|"
    rf"{COMPILER_PROCESS_TOKEN}|{LINKER_PROCESS_TOKEN}|{SOONG_PROCESS_TOKEN}|"
    rf"ckati|kati|gradlew?|metalava|aapt2?|aidl|dex2oat)(?:$|[ /])",
    re.IGNORECASE,
)
ANDROID_EMULATOR_COMMAND = re.compile(
    r"Android[/ ](?:SDK )?emulator", re.IGNORECASE
)


class Process(NamedTuple):
    pid: int
    ppid: int
    name: str
    command: str
    state: str = "S"
    starttime_ticks: int = 1


def _regular_non_symlink(path: Path, description: str) -> Path:
    if path.is_symlink() or not path.is_file():
        raise GuardError(f"{description} must be a regular non-symlink file")
    return path


def _directory_non_symlink(path: Path, description: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise GuardError(f"{description} must be a non-symlink directory")
    return path


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def publish_json_read_only_atomic_exclusive(path: Path, value: Any) -> None:
    """Publish complete mode-0444 JSON without a writable partial target."""
    parent = _directory_non_symlink(path.parent, "atomic JSON parent")
    if path.exists() or path.is_symlink():
        raise GuardError(f"refusing to reuse atomic JSON output: {path}")
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
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
            raise GuardError(f"refusing to reuse atomic JSON output: {path}") from error
        _fsync_directory(parent)
    finally:
        if descriptor_open:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        else:
            _fsync_directory(parent)
    published = _regular_non_symlink(path, "atomic JSON output")
    if stat.S_IMODE(published.stat().st_mode) != 0o444 or published.read_bytes() != payload:
        raise GuardError("atomic JSON output differs from its finalized payload")


def read_process_stat_identity(pid: int) -> dict[str, int | str] | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
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


def require_running_process_identity(pid: int, description: str) -> dict[str, int | str]:
    identity = read_process_stat_identity(pid)
    if not process_identity_is_running(identity):
        raise GuardError(f"{description} is absent, zombie, or exited")
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
    except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError, OSError):
        return []


def process_tree(root_pid: int, root_starttime_ticks: int | None = None) -> set[int]:
    if root_starttime_ticks is not None and not process_is_same_running(
        root_pid, root_starttime_ticks
    ):
        return set()
    pending: list[tuple[int, int | None, int | None]] = [
        (root_pid, None, root_starttime_ticks)
    ]
    observed: set[int] = set()
    while pending:
        pid, expected_parent, expected_starttime = pending.pop()
        identity = read_process_stat_identity(pid)
        if (
            pid in observed
            or not process_identity_is_running(identity)
            or identity is None
            or expected_parent is not None
            and identity["ppid"] != expected_parent
            or expected_starttime is not None
            and identity["starttime_ticks"] != expected_starttime
        ):
            continue
        observed.add(pid)
        pending.extend((child, pid, None) for child in read_process_children(pid))
    return observed


def guardian_maximum_allowed_gap_ns(interval_ms: int) -> int:
    if interval_ms < 1:
        raise GuardError("guardian cadence interval must be positive")
    return interval_ms * 1_000_000 + CADENCE_EDGE_ALLOWANCE_NS


def derive_guardian_maximum_poll_start_gap_ns(
    timestamps: list[int], elapsed_ns: int
) -> int:
    if elapsed_ns < 0 or any(value < 0 for value in timestamps):
        raise GuardError("guardian cadence timestamps must be non-negative")
    if any(later <= earlier for earlier, later in zip(timestamps, timestamps[1:])):
        raise GuardError("guardian cadence timestamps must increase strictly")
    if timestamps and timestamps[-1] > elapsed_ns:
        raise GuardError("guardian cadence timestamp exceeds terminal elapsed time")
    boundaries = [0, *timestamps, elapsed_ns]
    return max(
        (later - earlier for earlier, later in zip(boundaries, boundaries[1:])),
        default=0,
    )


def create_empty_read_only_marker(path: Path, description: str) -> None:
    if not path.is_absolute():
        raise GuardError(f"{description} path must be absolute")
    _directory_non_symlink(path.parent, f"{description} parent")
    if path.exists() or path.is_symlink():
        raise GuardError(f"refusing to reuse {description}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o444)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)
    validate_empty_read_only_marker(path, description)


def validate_empty_read_only_marker(path: Path, description: str) -> Path:
    marker = _regular_non_symlink(path, description)
    if marker.stat().st_size != 0 or stat.S_IMODE(marker.stat().st_mode) != 0o444:
        raise GuardError(f"{description} must be exact empty mode 0444")
    return marker


def validate_control(
    path: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    *,
    expected_root_pid: int | None = None,
    expected_guardian_pid: int | None = None,
    expected_rss_monitor_pid: int | None = None,
    require_live: bool = False,
) -> dict[str, Any]:
    control_path = _regular_non_symlink(path, "guardian control")
    if stat.S_IMODE(control_path.stat().st_mode) != 0o444:
        raise GuardError("guardian control must have exact mode 0444")
    try:
        value = json.loads(control_path.read_text(encoding="utf-8", errors="strict"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise GuardError("guardian control is not strict JSON") from error
    expected_keys = {
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
    if not isinstance(value, dict) or set(value) != expected_keys:
        raise GuardError("guardian control keys differ from the exact contract")
    roles = ("root", "guardian", "rss_monitor")
    identity_fields = {
        *(f"{role}_pid" for role in roles),
        *(f"{role}_starttime_ticks" for role in roles),
    }
    if any(type(value[field]) is not int for field in identity_fields):
        raise GuardError("guardian control identities must be exact integers")
    try:
        pids = {role: int(value[f"{role}_pid"]) for role in roles}
        starttimes = {
            role: int(value[f"{role}_starttime_ticks"]) for role in roles
        }
    except (TypeError, ValueError) as error:
        raise GuardError("guardian control identities are invalid") from error
    if (
        value["schema"] != CONTROL_SCHEMA
        or type(value["interval_ms"]) is not int
        or value["interval_ms"] != interval_ms
        or interval_ms != 100
        or any(pid <= 1 for pid in pids.values())
        or any(starttime < 1 for starttime in starttimes.values())
        or len(set(pids.values())) != len(pids)
        or value["guardian_ready_marker"] != str(guardian_ready_path)
        or value["rss_ready_marker"] != str(rss_ready_path)
        or value["launch_marker"] != str(launch_path)
        or any(
            not marker.is_absolute() or marker.parent != control_path.parent
            for marker in (guardian_ready_path, rss_ready_path, launch_path)
        )
        or expected_root_pid is not None
        and pids["root"] != expected_root_pid
        or expected_guardian_pid is not None
        and pids["guardian"] != expected_guardian_pid
        or expected_rss_monitor_pid is not None
        and pids["rss_monitor"] != expected_rss_monitor_pid
    ):
        raise GuardError("guardian control differs from the exact handshake")
    if require_live:
        dead = [
            role
            for role in roles
            if not process_is_same_running(pids[role], starttimes[role])
        ]
        if dead:
            raise GuardError(
                "guardian control has exited, zombie, or reused processes: "
                f"{dead!r}"
            )
    return value


def create_control(
    output: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    root_pid: int,
    guardian_pid: int,
    rss_monitor_pid: int,
    interval_ms: int,
) -> dict[str, Any]:
    if interval_ms != 100:
        raise GuardError("formal guardian and RSS cadence is fixed at 100 ms")
    for marker, description in (
        (guardian_ready_path, "guardian ready marker"),
        (rss_ready_path, "RSS ready marker"),
        (launch_path, "launch marker"),
    ):
        if marker.exists() or marker.is_symlink():
            raise GuardError(f"refusing to reuse {description}")
    identities = {
        "root": require_running_process_identity(root_pid, "held workload root"),
        "guardian": require_running_process_identity(guardian_pid, "guardian"),
        "rss_monitor": require_running_process_identity(
            rss_monitor_pid, "RSS monitor"
        ),
    }
    value: dict[str, Any] = {
        "schema": CONTROL_SCHEMA,
        "root_pid": root_pid,
        "root_starttime_ticks": identities["root"]["starttime_ticks"],
        "guardian_pid": guardian_pid,
        "guardian_starttime_ticks": identities["guardian"]["starttime_ticks"],
        "rss_monitor_pid": rss_monitor_pid,
        "rss_monitor_starttime_ticks": identities["rss_monitor"]["starttime_ticks"],
        "interval_ms": interval_ms,
        "guardian_ready_marker": str(guardian_ready_path),
        "rss_ready_marker": str(rss_ready_path),
        "launch_marker": str(launch_path),
    }
    publish_json_read_only_atomic_exclusive(output, value)
    observed = validate_control(
        output,
        guardian_ready_path,
        rss_ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=root_pid,
        expected_guardian_pid=guardian_pid,
        expected_rss_monitor_pid=rss_monitor_pid,
        require_live=True,
    )
    if observed != value:
        raise GuardError("fresh guardian control failed self-validation")
    return value


def wait_ready(
    control_path: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    timeout_ms: int,
) -> dict[str, Any]:
    if timeout_ms < interval_ms:
        raise GuardError("readiness timeout is shorter than one poll")
    deadline = time.monotonic() + timeout_ms / 1000
    while True:
        control = validate_control(
            control_path,
            guardian_ready_path,
            rss_ready_path,
            launch_path,
            interval_ms,
            require_live=True,
        )
        if launch_path.exists() or launch_path.is_symlink():
            raise GuardError("launch marker appeared before both monitors were ready")
        ready = []
        for marker, description in (
            (guardian_ready_path, "guardian ready marker"),
            (rss_ready_path, "RSS ready marker"),
        ):
            if marker.exists() or marker.is_symlink():
                validate_empty_read_only_marker(marker, description)
                ready.append(description)
        if len(ready) == 2:
            return {"status": "ready", "root_pid": control["root_pid"]}
        if time.monotonic() >= deadline:
            raise GuardError("both monitors did not become ready before timeout")
        time.sleep(0.01)


def release_launch(
    control_path: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> dict[str, Any]:
    control = validate_control(
        control_path,
        guardian_ready_path,
        rss_ready_path,
        launch_path,
        interval_ms,
        require_live=True,
    )
    validate_empty_read_only_marker(guardian_ready_path, "guardian ready marker")
    validate_empty_read_only_marker(rss_ready_path, "RSS ready marker")
    create_empty_read_only_marker(launch_path, "launch marker")
    return {"status": "released", "root_pid": control["root_pid"]}


def is_forbidden(name: str, command: str = "") -> bool:
    normalized = name.casefold()
    return (
        normalized in FORBIDDEN_EXACT_CASEFOLD
        or normalized.startswith(FORBIDDEN_PREFIXES_CASEFOLD)
        or any(marker in command for marker in FORBIDDEN_COMMAND_MARKERS)
        or FORBIDDEN_VARIANT_NAME.fullmatch(name) is not None
        or FORBIDDEN_VARIANT_COMMAND.search(command) is not None
        or ANDROID_EMULATOR_COMMAND.search(command) is not None
    )


def read_processes() -> dict[int, Process]:
    processes: dict[int, Process] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            status = (entry / "status").read_text(encoding="utf-8")
            fields = {}
            for line in status.splitlines():
                if ":" in line:
                    key, value = line.split(":", 1)
                    fields[key] = value.strip()
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", errors="replace"
            )
            pid = int(entry.name)
            identity = read_process_stat_identity(pid)
            if identity is None:
                continue
            processes[pid] = Process(
                pid=pid,
                ppid=int(fields["PPid"]),
                name=fields["Name"],
                command=command,
                state=str(identity["state"]),
                starttime_ticks=int(identity["starttime_ticks"]),
            )
        except (FileNotFoundError, PermissionError, KeyError, ValueError, OSError):
            continue
    return processes


def descendants(processes: dict[int, Process], root_pid: int) -> set[int]:
    root = processes.get(root_pid)
    if root is None or root.state in {"Z", "X", "x"}:
        return set()
    found = {root_pid}
    changed = True
    while changed:
        changed = False
        for process in processes.values():
            if (
                process.pid not in found
                and process.ppid in found
                and process.state not in {"Z", "X", "x"}
            ):
                found.add(process.pid)
                changed = True
    return found


def snapshot_process_tree_identities(
    root_pid: int, root_starttime_ticks: int
) -> list[dict[str, int | str]]:
    identities: dict[int, dict[str, int | str]] = {}
    try:
        entries = list(Path("/proc").iterdir())
    except OSError:
        return []
    for entry in entries:
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
            if not process_identity_is_running(identity):
                continue
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


def terminate_process_tree(root_pid: int, root_starttime_ticks: int) -> dict[str, Any]:
    """Terminate a frozen process tree deepest-first, refusing changed identities."""
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


def cleanup_controlled_processes(
    control_path: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> dict[str, Any]:
    control = validate_control(
        control_path,
        guardian_ready_path,
        rss_ready_path,
        launch_path,
        interval_ms,
        require_live=False,
    )
    roles = ["root", "rss_monitor", "guardian"]
    terminations = {
        role: terminate_process_tree(
            int(control[f"{role}_pid"]),
            int(control[f"{role}_starttime_ticks"]),
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
    incomplete = {role: failures for role, failures in incomplete.items() if failures}
    if incomplete:
        raise GuardError(f"guardian-controlled cleanup was incomplete: {incomplete!r}")
    return {
        "schema": CLEANUP_SCHEMA,
        "control_path": str(control_path),
        "control_sha256": sha256_file(control_path),
        "termination_order": roles,
        "terminations": terminations,
    }


def terminate_root_from_control(
    control_path: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    expected_root_pid: int,
) -> dict[str, Any]:
    """Fail closed using only the already-sealed root identity."""
    control = validate_control(
        control_path,
        guardian_ready_path,
        rss_ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=expected_root_pid,
        require_live=False,
    )
    return terminate_process_tree(
        int(control["root_pid"]), int(control["root_starttime_ticks"])
    )


def write_violation(path: Path, reason: str, detail: dict[str, object]) -> None:
    value = {
        "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "reason": reason,
        **detail,
    }
    publish_json_read_only_atomic_exclusive(path, value)


def ancestor_pids(pid: int) -> set[int]:
    result: set[int] = set()
    current = pid
    while current > 1 and current not in result:
        result.add(current)
        identity = read_process_stat_identity(current)
        if identity is None:
            break
        current = int(identity["ppid"])
    return result


def scan_conflicts(
    *, root_pid: int | None = None, root_starttime_ticks: int | None = None
) -> list[dict[str, Any]]:
    excluded = ancestor_pids(os.getpid())
    if root_pid is not None:
        excluded.update(process_tree(root_pid, root_starttime_ticks))
    conflicts: list[dict[str, Any]] = []
    for process in read_processes().values():
        if process.pid in excluded or not is_forbidden(process.name, process.command):
            continue
        conflicts.append(
            {
                "pid": process.pid,
                "ppid": process.ppid,
                "state": process.state,
                "starttime_ticks": process.starttime_ticks,
                "name": process.name,
                "command": process.command,
            }
        )
    return sorted(conflicts, key=lambda value: value["pid"])


def record_conflict_scan(output: Path) -> dict[str, Any]:
    conflicts = scan_conflicts()
    value = {
        "schema": CONFLICT_SCAN_SCHEMA,
        "conflicts": conflicts,
        "quiet": not conflicts,
    }
    publish_json_read_only_atomic_exclusive(output, value)
    if conflicts:
        raise GuardError(f"measurement conflict detected: {conflicts[0]!r}")
    return value


def _wait_for_bound_control(
    *,
    root_pid: int,
    role: str,
    control_path: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> tuple[dict[str, Any], int]:
    initial = require_running_process_identity(root_pid, "held workload root")
    root_starttime_ticks = int(initial["starttime_ticks"])
    deadline = time.monotonic() + 5.0
    while not control_path.exists() and not control_path.is_symlink():
        if not process_is_same_running(root_pid, root_starttime_ticks):
            raise GuardError("held workload root exited before control publication")
        if time.monotonic() >= deadline:
            raise GuardError("guardian control was not published before timeout")
        time.sleep(0.005)
    expected = {f"expected_{role}_pid": os.getpid()}
    control = validate_control(
        control_path,
        guardian_ready_path,
        rss_ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=root_pid,
        require_live=True,
        **expected,
    )
    if control["root_starttime_ticks"] != root_starttime_ticks:
        raise GuardError("control bound a reused held-root PID")
    return control, root_starttime_ticks


def _launch_observed(
    launch_path: Path, violations: list[str], description: str
) -> bool:
    if not (launch_path.exists() or launch_path.is_symlink()):
        return False
    try:
        validate_empty_read_only_marker(launch_path, "launch marker")
    except GuardError as error:
        violations.append(f"{description}: {error}")
        return False
    return True


def _empty_termination(root_starttime_ticks: int) -> dict[str, Any]:
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


def monitor_guardian(
    root_pid: int,
    filesystem: Path,
    minimum_free_bytes: int,
    disk_log: Path,
    process_log: Path,
    violation: Path,
    summary_path: Path,
    control_path: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> dict[str, Any]:
    if root_pid <= 1 or minimum_free_bytes <= 0 or interval_ms != 100:
        raise GuardError("formal guardian requires valid identities and 100 ms cadence")
    filesystem = _directory_non_symlink(filesystem, "guardian filesystem").resolve()
    for output in (disk_log, process_log, violation, summary_path):
        if output.exists() or output.is_symlink():
            raise GuardError(f"refusing to reuse guardian output: {output}")
    control, root_starttime_ticks = _wait_for_bound_control(
        root_pid=root_pid,
        role="guardian",
        control_path=control_path,
        guardian_ready_path=guardian_ready_path,
        rss_ready_path=rss_ready_path,
        launch_path=launch_path,
        interval_ms=interval_ms,
    )

    with disk_log.open("x", encoding="utf-8") as disks, process_log.open(
        "x", encoding="utf-8"
    ) as conflicts:
        disks.write(
            "poll_index\tmonotonic_elapsed_ns\trecorded_at\troot_running\t"
            "launch_observed\tfree_bytes\tminimum_free_bytes\n"
        )
        conflicts.write(
            "poll_index\tmonotonic_elapsed_ns\trecorded_at\tpid\tppid\tstate\t"
            "starttime_ticks\tname\tcommand\n"
        )
        started = time.monotonic_ns()
        timestamps: list[int] = []
        minimum_observed_free_bytes: int | None = None
        handshake_violations: list[str] = []
        observed_conflicts: list[dict[str, Any]] = []
        capacity_violations: list[dict[str, Any]] = []
        ready_poll: int | None = None
        ready_elapsed: int | None = None
        launch_poll: int | None = None
        launch_elapsed: int | None = None
        root_seen = False
        termination = _empty_termination(root_starttime_ticks)
        next_poll_ns = started
        while True:
            now = time.monotonic_ns()
            if now < next_poll_ns:
                time.sleep((next_poll_ns - now) / 1_000_000_000)
            elapsed = time.monotonic_ns() - started
            timestamps.append(elapsed)
            poll = len(timestamps)
            running = process_is_same_running(root_pid, root_starttime_ticks)
            root_seen = root_seen or running
            launch_seen = _launch_observed(
                launch_path, handshake_violations, "guardian launch observation"
            )
            if launch_seen and launch_poll is None:
                launch_poll = poll
                launch_elapsed = elapsed
            recorded_at = dt.datetime.now(dt.timezone.utc).isoformat()
            filesystem_stats = os.statvfs(filesystem)
            free_bytes = filesystem_stats.f_bavail * filesystem_stats.f_frsize
            minimum_observed_free_bytes = (
                free_bytes
                if minimum_observed_free_bytes is None
                else min(minimum_observed_free_bytes, free_bytes)
            )
            disks.write(
                f"{poll}\t{elapsed}\t{recorded_at}\t{str(running).lower()}\t"
                f"{str(launch_seen).lower()}\t{free_bytes}\t{minimum_free_bytes}\n"
            )
            disks.flush()
            if poll == 1:
                os.fsync(disks.fileno())
            if free_bytes < minimum_free_bytes:
                capacity_violations.append(
                    {"poll": poll, "free_bytes": free_bytes, "minimum_free_bytes": minimum_free_bytes}
                )
            current_conflicts = scan_conflicts(
                root_pid=root_pid, root_starttime_ticks=root_starttime_ticks
            )
            if current_conflicts:
                for process in current_conflicts:
                    command = str(process["command"]).replace("\t", " ").replace("\n", " ")
                    conflicts.write(
                        f"{poll}\t{elapsed}\t{recorded_at}\t{process['pid']}\t"
                        f"{process['ppid']}\t{process['state']}\t"
                        f"{process['starttime_ticks']}\t{process['name']}\t{command}\n"
                    )
                conflicts.flush()
                observed_conflicts.extend(current_conflicts)
            maximum_gap = derive_guardian_maximum_poll_start_gap_ns(timestamps, elapsed)
            if ready_poll is None:
                if launch_seen:
                    handshake_violations.append("launch marker existed before guardian readiness")
                elif running and not current_conflicts and not capacity_violations and maximum_gap <= guardian_maximum_allowed_gap_ns(interval_ms):
                    create_empty_read_only_marker(
                        guardian_ready_path, "guardian ready marker"
                    )
                    ready_poll = poll
                    ready_elapsed = elapsed
            else:
                try:
                    validate_empty_read_only_marker(
                        guardian_ready_path, "guardian ready marker"
                    )
                except GuardError as error:
                    handshake_violations.append(str(error))
            if current_conflicts or capacity_violations or handshake_violations or maximum_gap > guardian_maximum_allowed_gap_ns(interval_ms):
                termination = terminate_process_tree(root_pid, root_starttime_ticks)
                reason = (
                    "measurement-conflict"
                    if current_conflicts
                    else "disk-reserve"
                    if capacity_violations
                    else "guardian-handshake-or-cadence"
                )
                write_violation(
                    violation,
                    reason,
                    {
                        "poll": poll,
                        "conflicts": current_conflicts,
                        "capacity_violations": capacity_violations,
                        "handshake_violations": handshake_violations,
                    },
                )
                break
            if not running:
                break
            next_poll_ns += interval_ms * 1_000_000
    terminal_elapsed = time.monotonic_ns() - started
    maximum_gap = derive_guardian_maximum_poll_start_gap_ns(timestamps, terminal_elapsed)
    if launch_poll is None:
        handshake_violations.append("guardian never observed the launch marker")
    complete = bool(
        root_seen
        and len(timestamps) >= 2
        and ready_poll == 1
        and launch_poll is not None
        and launch_poll > ready_poll
        and timestamps[-1] <= terminal_elapsed
        and maximum_gap <= guardian_maximum_allowed_gap_ns(interval_ms)
        and not handshake_violations
        and not observed_conflicts
        and not capacity_violations
        and termination["attempted"] is False
    )
    result = {
        "schema": GUARDIAN_SCHEMA,
        "root_pid": root_pid,
        "root_starttime_ticks": root_starttime_ticks,
        "guardian_pid": os.getpid(),
        "interval_ms": interval_ms,
        "polls": len(timestamps),
        "terminal_elapsed_ns": terminal_elapsed,
        "poll_monotonic_elapsed_ns": timestamps,
        "maximum_poll_start_gap_ns": maximum_gap,
        "maximum_allowed_poll_start_gap_ns": guardian_maximum_allowed_gap_ns(interval_ms),
        "control_path": str(control_path),
        "control_sha256": sha256_file(control_path),
        "guardian_ready_marker": str(guardian_ready_path),
        "rss_ready_marker": str(rss_ready_path),
        "launch_marker": str(launch_path),
        "ready_created_poll": ready_poll,
        "ready_created_monotonic_elapsed_ns": ready_elapsed,
        "launch_observed_poll": launch_poll,
        "launch_observed_monotonic_elapsed_ns": launch_elapsed,
        "root_seen": root_seen,
        "filesystem": str(filesystem),
        "minimum_free_bytes": minimum_free_bytes,
        "minimum_observed_free_bytes": minimum_observed_free_bytes,
        "capacity_violations": capacity_violations,
        "conflicts": observed_conflicts,
        "handshake_violations": handshake_violations,
        "termination": termination,
        "complete_and_conflict_free": complete,
    }
    with summary_path.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")
    if not complete:
        raise GuardError("continuous guardian did not complete its exact lifecycle")
    return result


def _status_kib(pid: int) -> dict[str, int] | None:
    try:
        rows = Path(f"/proc/{pid}/status").read_text(encoding="ascii").splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
        return None
    wanted = {"VmRSS", "VmHWM", "RssAnon", "RssFile", "VmSwap"}
    result = {name: 0 for name in wanted}
    for row in rows:
        fields = row.split()
        if fields and fields[0] == "State:" and len(fields) > 1 and fields[1] in {"Z", "X", "x"}:
            return None
        key = fields[0].rstrip(":") if fields else ""
        if key in wanted and len(fields) >= 2:
            try:
                result[key] = int(fields[1])
            except ValueError:
                return None
    return result


def monitor_rss(
    root_pid: int,
    samples_path: Path,
    summary_path: Path,
    control_path: Path,
    guardian_ready_path: Path,
    rss_ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> dict[str, Any]:
    if root_pid <= 1 or interval_ms != 100:
        raise GuardError("formal RSS monitor requires 100 ms cadence")
    for output in (samples_path, summary_path):
        if output.exists() or output.is_symlink():
            raise GuardError(f"refusing to reuse RSS output: {output}")
    control, root_starttime_ticks = _wait_for_bound_control(
        root_pid=root_pid,
        role="rss_monitor",
        control_path=control_path,
        guardian_ready_path=guardian_ready_path,
        rss_ready_path=rss_ready_path,
        launch_path=launch_path,
        interval_ms=interval_ms,
    )
    del control
    maxima = {
        "process_count": 0,
        "aggregate_rss_kib": 0,
        "aggregate_rss_anon_kib": 0,
        "aggregate_rss_file_kib": 0,
        "aggregate_vm_swap_kib": 0,
        "max_single_process_hwm_kib": 0,
    }
    timestamps: list[int] = []
    handshake_violations: list[str] = []
    ready_poll: int | None = None
    ready_elapsed: int | None = None
    launch_poll: int | None = None
    launch_elapsed: int | None = None
    root_seen = False
    started = time.monotonic_ns()
    next_poll_ns = started
    termination = _empty_termination(root_starttime_ticks)
    with samples_path.open("x", encoding="utf-8") as destination:
        destination.write(
            "poll_index\tmonotonic_elapsed_ns\trecorded_at\troot_running\t"
            "launch_observed\tprocess_count\trss_kib\trss_anon_kib\t"
            "rss_file_kib\tvm_swap_kib\tmax_single_hwm_kib\tpids\n"
        )
        while True:
            now = time.monotonic_ns()
            if now < next_poll_ns:
                time.sleep((next_poll_ns - now) / 1_000_000_000)
            elapsed = time.monotonic_ns() - started
            timestamps.append(elapsed)
            poll = len(timestamps)
            running = process_is_same_running(root_pid, root_starttime_ticks)
            root_seen = root_seen or running
            launch_seen = _launch_observed(
                launch_path, handshake_violations, "RSS launch observation"
            )
            if launch_seen and launch_poll is None:
                launch_poll = poll
                launch_elapsed = elapsed
            pids = sorted(process_tree(root_pid, root_starttime_ticks)) if running else []
            statuses = [(pid, _status_kib(pid)) for pid in pids]
            statuses = [(pid, value) for pid, value in statuses if value is not None]
            values = [value for _pid, value in statuses]
            sample = {
                "process_count": len(values),
                "aggregate_rss_kib": sum(value["VmRSS"] for value in values),
                "aggregate_rss_anon_kib": sum(value["RssAnon"] for value in values),
                "aggregate_rss_file_kib": sum(value["RssFile"] for value in values),
                "aggregate_vm_swap_kib": sum(value["VmSwap"] for value in values),
                "max_single_process_hwm_kib": max(
                    (value["VmHWM"] for value in values), default=0
                ),
            }
            for name, value in sample.items():
                maxima[name] = max(maxima[name], value)
            destination.write(
                f"{poll}\t{elapsed}\t{dt.datetime.now(dt.timezone.utc).isoformat()}\t"
                f"{str(running).lower()}\t{str(launch_seen).lower()}\t"
                f"{sample['process_count']}\t{sample['aggregate_rss_kib']}\t"
                f"{sample['aggregate_rss_anon_kib']}\t"
                f"{sample['aggregate_rss_file_kib']}\t{sample['aggregate_vm_swap_kib']}\t"
                f"{sample['max_single_process_hwm_kib']}\t"
                f"{','.join(str(pid) for pid, _value in statuses)}\n"
            )
            destination.flush()
            if poll == 1:
                os.fsync(destination.fileno())
            maximum_gap = derive_guardian_maximum_poll_start_gap_ns(timestamps, elapsed)
            if ready_poll is None:
                if launch_seen:
                    handshake_violations.append("launch marker existed before RSS readiness")
                elif running and root_pid in {pid for pid, _value in statuses}:
                    create_empty_read_only_marker(rss_ready_path, "RSS ready marker")
                    ready_poll = poll
                    ready_elapsed = elapsed
                else:
                    handshake_violations.append("first RSS sample did not bind the live root")
            else:
                try:
                    validate_empty_read_only_marker(rss_ready_path, "RSS ready marker")
                except GuardError as error:
                    handshake_violations.append(str(error))
            if handshake_violations or maximum_gap > guardian_maximum_allowed_gap_ns(interval_ms):
                termination = terminate_process_tree(root_pid, root_starttime_ticks)
                break
            if not running:
                break
            next_poll_ns += interval_ms * 1_000_000
    terminal_elapsed = time.monotonic_ns() - started
    maximum_gap = derive_guardian_maximum_poll_start_gap_ns(timestamps, terminal_elapsed)
    if launch_poll is None:
        handshake_violations.append("RSS monitor never observed the launch marker")
    complete = bool(
        root_seen
        and len(timestamps) >= 2
        and ready_poll == 1
        and launch_poll is not None
        and launch_poll > ready_poll
        and maximum_gap <= guardian_maximum_allowed_gap_ns(interval_ms)
        and not handshake_violations
        and termination["attempted"] is False
    )
    result = {
        "schema": RSS_SCHEMA,
        "root_pid": root_pid,
        "root_starttime_ticks": root_starttime_ticks,
        "rss_monitor_pid": os.getpid(),
        "samples": len(timestamps),
        "interval_ms": interval_ms,
        **maxima,
        "terminal_elapsed_ns": terminal_elapsed,
        "poll_monotonic_elapsed_ns": timestamps,
        "maximum_poll_start_gap_ns": maximum_gap,
        "maximum_allowed_poll_start_gap_ns": guardian_maximum_allowed_gap_ns(interval_ms),
        "control_path": str(control_path),
        "control_sha256": sha256_file(control_path),
        "guardian_ready_marker": str(guardian_ready_path),
        "rss_ready_marker": str(rss_ready_path),
        "launch_marker": str(launch_path),
        "ready_created_poll": ready_poll,
        "ready_created_monotonic_elapsed_ns": ready_elapsed,
        "launch_observed_poll": launch_poll,
        "launch_observed_monotonic_elapsed_ns": launch_elapsed,
        "root_seen": root_seen,
        "handshake_violations": handshake_violations,
        "termination": termination,
        "complete": complete,
    }
    with summary_path.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")
    if not complete:
        raise GuardError("RSS monitor did not complete its exact lifecycle")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("scan-conflicts").add_argument(
        "--output", type=Path, required=True
    )
    create = commands.add_parser("create-control")
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--guardian-ready", type=Path, required=True)
    create.add_argument("--rss-ready", type=Path, required=True)
    create.add_argument("--launch", type=Path, required=True)
    create.add_argument("--root-pid", type=int, required=True)
    create.add_argument("--guardian-pid", type=int, required=True)
    create.add_argument("--rss-monitor-pid", type=int, required=True)
    create.add_argument("--interval-ms", type=int, required=True)
    ready = commands.add_parser("wait-ready")
    ready.add_argument("--control", type=Path, required=True)
    ready.add_argument("--guardian-ready", type=Path, required=True)
    ready.add_argument("--rss-ready", type=Path, required=True)
    ready.add_argument("--launch", type=Path, required=True)
    ready.add_argument("--interval-ms", type=int, required=True)
    ready.add_argument("--timeout-ms", type=int, default=5000)
    release = commands.add_parser("release-launch")
    release.add_argument("--control", type=Path, required=True)
    release.add_argument("--guardian-ready", type=Path, required=True)
    release.add_argument("--rss-ready", type=Path, required=True)
    release.add_argument("--launch", type=Path, required=True)
    release.add_argument("--interval-ms", type=int, required=True)
    guardian = commands.add_parser("monitor-guardian")
    guardian.add_argument("--root-pid", type=int, required=True)
    guardian.add_argument("--filesystem", type=Path, required=True)
    guardian.add_argument("--minimum-free-bytes", type=int, required=True)
    guardian.add_argument("--disk-log", type=Path, required=True)
    guardian.add_argument("--process-log", type=Path, required=True)
    guardian.add_argument("--violation", type=Path, required=True)
    guardian.add_argument("--summary", type=Path, required=True)
    guardian.add_argument("--control", type=Path, required=True)
    guardian.add_argument("--guardian-ready", type=Path, required=True)
    guardian.add_argument("--rss-ready", type=Path, required=True)
    guardian.add_argument("--launch", type=Path, required=True)
    guardian.add_argument("--interval-ms", type=int, required=True)
    rss = commands.add_parser("monitor-rss")
    rss.add_argument("--root-pid", type=int, required=True)
    rss.add_argument("--samples", type=Path, required=True)
    rss.add_argument("--summary", type=Path, required=True)
    rss.add_argument("--control", type=Path, required=True)
    rss.add_argument("--guardian-ready", type=Path, required=True)
    rss.add_argument("--rss-ready", type=Path, required=True)
    rss.add_argument("--launch", type=Path, required=True)
    rss.add_argument("--interval-ms", type=int, required=True)
    cleanup = commands.add_parser("cleanup-control")
    cleanup.add_argument("--control", type=Path, required=True)
    cleanup.add_argument("--guardian-ready", type=Path, required=True)
    cleanup.add_argument("--rss-ready", type=Path, required=True)
    cleanup.add_argument("--launch", type=Path, required=True)
    cleanup.add_argument("--interval-ms", type=int, required=True)
    terminate = commands.add_parser("terminate-tree")
    terminate.add_argument("--root-pid", type=int, required=True)
    terminate.add_argument("--root-starttime-ticks", type=int, required=True)
    args = parser.parse_args()
    try:
        if args.command == "scan-conflicts":
            result = record_conflict_scan(args.output)
        elif args.command == "create-control":
            result = create_control(
                args.output,
                args.guardian_ready,
                args.rss_ready,
                args.launch,
                args.root_pid,
                args.guardian_pid,
                args.rss_monitor_pid,
                args.interval_ms,
            )
        elif args.command == "wait-ready":
            result = wait_ready(
                args.control,
                args.guardian_ready,
                args.rss_ready,
                args.launch,
                args.interval_ms,
                args.timeout_ms,
            )
        elif args.command == "release-launch":
            result = release_launch(
                args.control,
                args.guardian_ready,
                args.rss_ready,
                args.launch,
                args.interval_ms,
            )
        elif args.command == "monitor-guardian":
            result = monitor_guardian(
                args.root_pid,
                args.filesystem,
                args.minimum_free_bytes,
                args.disk_log,
                args.process_log,
                args.violation,
                args.summary,
                args.control,
                args.guardian_ready,
                args.rss_ready,
                args.launch,
                args.interval_ms,
            )
        elif args.command == "monitor-rss":
            result = monitor_rss(
                args.root_pid,
                args.samples,
                args.summary,
                args.control,
                args.guardian_ready,
                args.rss_ready,
                args.launch,
                args.interval_ms,
            )
        elif args.command == "cleanup-control":
            result = cleanup_controlled_processes(
                args.control,
                args.guardian_ready,
                args.rss_ready,
                args.launch,
                args.interval_ms,
            )
        elif args.command == "terminate-tree":
            result = terminate_process_tree(
                args.root_pid, args.root_starttime_ticks
            )
        else:  # pragma: no cover - argparse makes this unreachable.
            raise GuardError("unknown guardian command")
        print(json.dumps(result, sort_keys=True))
        return 0
    except (GuardError, OSError, ValueError) as error:
        print(f"Phase 5 guardian: {error}", file=__import__("sys").stderr)
        if args.command == "monitor-guardian":
            try:
                if not args.violation.exists() and not args.violation.is_symlink():
                    write_violation(
                        args.violation, "guardian-error", {"error": str(error)}
                    )
            except (GuardError, OSError, ValueError):
                pass
        if args.command in {"monitor-guardian", "monitor-rss"}:
            try:
                terminate_root_from_control(
                    args.control,
                    args.guardian_ready,
                    args.rss_ready,
                    args.launch,
                    args.interval_ms,
                    args.root_pid,
                )
            except (GuardError, OSError, ValueError) as cleanup_error:
                print(
                    f"Phase 5 guardian fail-closed cleanup: {cleanup_error}",
                    file=__import__("sys").stderr,
                )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
