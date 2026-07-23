#!/usr/bin/env python3
"""Identity-bound held-launch process guardian for the Phase 4 comparator."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import re
import signal
import stat
import tempfile
import time
from pathlib import Path
from typing import Any, NamedTuple


CONTROL_SCHEMA = "chronoxide/phase4-range-one-pass-guardian-control/v1"
GUARDIAN_SCHEMA = "chronoxide/phase4-range-one-pass-guardian/v1"
CLEANUP_SCHEMA = "chronoxide/phase4-range-one-pass-guardian-cleanup/v1"
CONFLICT_SCAN_SCHEMA = "chronoxide/phase4-range-one-pass-conflict-scan/v1"
CADENCE_INTERVAL_MS = 100
CADENCE_EDGE_ALLOWANCE_NS = 100_000_000
DEAD_STATES = {"Z", "X", "x"}

SAMPLE_COLUMNS = (
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
CONFLICT_COLUMNS = (
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


class GuardError(RuntimeError):
    """A fail-closed lifecycle or quiet-host violation."""


class Process(NamedTuple):
    pid: int
    ppid: int
    name: str
    command: str
    state: str = "S"
    starttime_ticks: int = 1
    cpu_percent: float = 0.0


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


def _regular_non_symlink(path: Path, description: str) -> Path:
    if path.is_symlink() or not path.is_file():
        raise GuardError(f"{description} must be a regular non-symlink file")
    return path


def _directory_non_symlink(path: Path, description: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise GuardError(f"{description} must be a non-symlink directory")
    return path


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
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
    if not path.is_absolute():
        raise GuardError("atomic JSON path must be absolute")
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
    if stat.S_IMODE(published.stat().st_mode) != 0o444:
        raise GuardError("atomic JSON output must have exact mode 0444")
    if published.read_bytes() != payload:
        raise GuardError("atomic JSON output differs from its finalized payload")


def create_empty_read_only_marker(path: Path, description: str) -> None:
    if not path.is_absolute():
        raise GuardError(f"{description} path must be absolute")
    parent = _directory_non_symlink(path.parent, f"{description} parent")
    if path.exists() or path.is_symlink():
        raise GuardError(f"refusing to reuse {description}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.tmp.", dir=parent
    )
    temporary = Path(temporary_name)
    descriptor_open = True
    try:
        with os.fdopen(descriptor, "wb") as destination:
            descriptor_open = False
            # Creation modes are filtered through the runner's umask.  Reassert
            # the exact evidence mode before publication.
            os.fchmod(destination.fileno(), 0o444)
            os.fsync(destination.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise GuardError(f"refusing to reuse {description}") from error
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
    validate_empty_read_only_marker(path, description)


def validate_empty_read_only_marker(path: Path, description: str) -> Path:
    marker = _regular_non_symlink(path, description)
    if marker.stat().st_size != 0 or stat.S_IMODE(marker.stat().st_mode) != 0o444:
        raise GuardError(f"{description} must be exact empty mode 0444")
    return marker


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
    return identity is not None and identity["state"] not in DEAD_STATES


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
    if current["state"] in DEAD_STATES:
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
    """Freeze descendants, then identity-check every deepest-first signal."""
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
                errors.append({"pid": pid, "signal": signal_name, "error": str(error)})
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


def guardian_maximum_allowed_gap_ns(interval_ms: int) -> int:
    if interval_ms != CADENCE_INTERVAL_MS:
        raise GuardError("formal guardian cadence is fixed at 100 ms")
    return interval_ms * 1_000_000 + CADENCE_EDGE_ALLOWANCE_NS


def derive_guardian_maximum_poll_start_gap_ns(
    timestamps: list[int], terminal_elapsed_ns: int
) -> int:
    if terminal_elapsed_ns < 0 or any(value < 0 for value in timestamps):
        raise GuardError("guardian cadence timestamps must be non-negative")
    if any(later <= earlier for earlier, later in zip(timestamps, timestamps[1:])):
        raise GuardError("guardian cadence timestamps must increase strictly")
    if timestamps and timestamps[-1] > terminal_elapsed_ns:
        raise GuardError("guardian sample exceeds terminal elapsed time")
    boundaries = [0, *timestamps, terminal_elapsed_ns]
    return max(
        (later - earlier for earlier, later in zip(boundaries, boundaries[1:])),
        default=0,
    )


def _process_cpu_percent(stat_fields: list[str], uptime_seconds: float) -> float:
    try:
        clock_ticks = int(os.sysconf("SC_CLK_TCK"))
        consumed = int(stat_fields[11]) + int(stat_fields[12])
        started = int(stat_fields[19])
    except (IndexError, TypeError, ValueError, OSError):
        return 0.0
    elapsed_ticks = uptime_seconds * clock_ticks - started
    if clock_ticks <= 0 or elapsed_ticks <= 0:
        return 0.0
    value = consumed * 100.0 / elapsed_ticks
    return value if math.isfinite(value) and value >= 0 else 0.0


def read_processes() -> dict[int, Process]:
    try:
        uptime_seconds = float(Path("/proc/uptime").read_text(encoding="ascii").split()[0])
        entries = list(Path("/proc").iterdir())
    except (OSError, ValueError, IndexError) as error:
        raise GuardError("could not enumerate the Linux process table") from error
    processes: dict[int, Process] = {}
    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            raw_stat = (entry / "stat").read_text(encoding="ascii")
            close = raw_stat.rfind(")")
            if close < 0:
                raise GuardError(f"process {pid} has malformed stat evidence")
            stat_fields = raw_stat[close + 2 :].split()
            identity = read_process_stat_identity(pid)
            if identity is None:
                if entry.exists():
                    raise GuardError(f"could not bind process {pid} identity")
                continue
            if int(stat_fields[19]) != identity["starttime_ticks"]:
                raise GuardError(f"process {pid} changed identity during snapshot")
            name = (entry / "comm").read_text(encoding="utf-8").strip()
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", errors="replace"
            ).strip()
            final_identity = read_process_stat_identity(pid)
            if final_identity is None:
                continue
            if final_identity["starttime_ticks"] != identity["starttime_ticks"]:
                raise GuardError(f"process {pid} changed identity during snapshot")
            processes[pid] = Process(
                pid=pid,
                ppid=int(final_identity["ppid"]),
                name=name,
                command=command,
                state=str(final_identity["state"]),
                starttime_ticks=int(final_identity["starttime_ticks"]),
                cpu_percent=_process_cpu_percent(stat_fields, uptime_seconds),
            )
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError as error:
            raise GuardError(f"permission denied while reading process {pid}") from error
        except (IndexError, ValueError, OSError) as error:
            if entry.exists():
                raise GuardError(f"could not read stable process {pid} evidence") from error
    return processes


def is_forbidden_process(process: Process) -> bool:
    qemu_or_java = process.name.casefold().startswith("qemu-system") or process.name.casefold() in {
        "qemu-kvm",
        "java",
    }
    special_conflict = qemu_or_java and (
        process.cpu_percent >= 5.0
        or ANDROID_VM_PROCESS_COMMAND.search(process.command) is not None
        or FORBIDDEN_PROCESS_COMMAND.search(process.command) is not None
    )
    return bool(
        FORBIDDEN_PROCESS_NAMES.fullmatch(process.name)
        or FORBIDDEN_PROCESS_COMMAND.search(process.command)
        or special_conflict
    )


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
        if process.pid in excluded or process.state in DEAD_STATES:
            continue
        if is_forbidden_process(process):
            conflicts.append(
                {
                    "pid": process.pid,
                    "ppid": process.ppid,
                    "state": process.state,
                    "starttime_ticks": process.starttime_ticks,
                    "cpu_percent": process.cpu_percent,
                    "name": process.name,
                    "command": process.command,
                }
            )
    return sorted(conflicts, key=lambda value: int(value["pid"]))


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


def validate_control(
    path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    *,
    expected_runner_pid: int | None = None,
    expected_root_pid: int | None = None,
    expected_guardian_pid: int | None = None,
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
    if not isinstance(value, dict) or set(value) != expected_keys:
        raise GuardError("guardian control keys differ from the exact contract")
    roles = ("runner", "root", "guardian")
    integer_fields = {
        *(f"{role}_pid" for role in roles),
        *(f"{role}_starttime_ticks" for role in roles),
        *(f"{role}_ppid" for role in roles),
        "interval_ms",
    }
    if any(type(value[field]) is not int for field in integer_fields):
        raise GuardError("guardian control identity fields must be exact integers")
    pids = {role: int(value[f"{role}_pid"]) for role in roles}
    starts = {role: int(value[f"{role}_starttime_ticks"]) for role in roles}
    if (
        value["schema"] != CONTROL_SCHEMA
        or interval_ms != CADENCE_INTERVAL_MS
        or value["interval_ms"] != interval_ms
        or any(pid <= 1 for pid in pids.values())
        or any(start < 1 for start in starts.values())
        or value["runner_ppid"] < 1
        or value["root_ppid"] != pids["runner"]
        or value["guardian_ppid"] != pids["runner"]
        or len(set(pids.values())) != 3
        or value["ready_marker"] != str(ready_path)
        or value["launch_marker"] != str(launch_path)
        or not ready_path.is_absolute()
        or not launch_path.is_absolute()
        or ready_path.parent != control_path.parent
        or launch_path.parent != control_path.parent
        or expected_runner_pid is not None
        and pids["runner"] != expected_runner_pid
        or expected_root_pid is not None
        and pids["root"] != expected_root_pid
        or expected_guardian_pid is not None
        and pids["guardian"] != expected_guardian_pid
    ):
        raise GuardError("guardian control differs from the exact handshake")
    if require_live:
        for role in roles:
            current = read_process_stat_identity(pids[role])
            if (
                not process_identity_is_running(current)
                or current is None
                or current["starttime_ticks"] != starts[role]
                or current["ppid"] != value[f"{role}_ppid"]
            ):
                raise GuardError(f"guardian control {role} identity is no longer live")
    return value


def create_control(
    output: Path,
    ready_path: Path,
    launch_path: Path,
    runner_pid: int,
    root_pid: int,
    guardian_pid: int,
    interval_ms: int,
) -> dict[str, Any]:
    if interval_ms != CADENCE_INTERVAL_MS:
        raise GuardError("formal guardian cadence is fixed at 100 ms")
    for marker, description in (
        (ready_path, "guardian ready marker"),
        (launch_path, "launch marker"),
    ):
        if marker.exists() or marker.is_symlink():
            raise GuardError(f"refusing to reuse {description}")
    identities = {
        "runner": require_running_process_identity(runner_pid, "runner"),
        "root": require_running_process_identity(root_pid, "held workload root"),
        "guardian": require_running_process_identity(guardian_pid, "guardian"),
    }
    if identities["root"]["ppid"] != runner_pid:
        raise GuardError("held workload root is not a direct runner child")
    if identities["guardian"]["ppid"] != runner_pid:
        raise GuardError("guardian is not a direct runner child")
    value: dict[str, Any] = {
        "schema": CONTROL_SCHEMA,
        "runner_pid": runner_pid,
        "runner_starttime_ticks": identities["runner"]["starttime_ticks"],
        "runner_ppid": identities["runner"]["ppid"],
        "root_pid": root_pid,
        "root_starttime_ticks": identities["root"]["starttime_ticks"],
        "root_ppid": identities["root"]["ppid"],
        "guardian_pid": guardian_pid,
        "guardian_starttime_ticks": identities["guardian"]["starttime_ticks"],
        "guardian_ppid": identities["guardian"]["ppid"],
        "interval_ms": interval_ms,
        "ready_marker": str(ready_path),
        "launch_marker": str(launch_path),
    }
    publish_json_read_only_atomic_exclusive(output, value)
    observed = validate_control(
        output,
        ready_path,
        launch_path,
        interval_ms,
        expected_runner_pid=runner_pid,
        expected_root_pid=root_pid,
        expected_guardian_pid=guardian_pid,
        require_live=True,
    )
    if observed != value:
        raise GuardError("fresh guardian control failed self-validation")
    return value


def wait_ready(
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    timeout_ms: int,
) -> dict[str, Any]:
    if timeout_ms < interval_ms:
        raise GuardError("guardian readiness timeout is shorter than one poll")
    deadline = time.monotonic() + timeout_ms / 1000
    while True:
        control = validate_control(
            control_path,
            ready_path,
            launch_path,
            interval_ms,
            require_live=True,
        )
        if launch_path.exists() or launch_path.is_symlink():
            raise GuardError("launch marker appeared before guardian readiness")
        if ready_path.exists() or ready_path.is_symlink():
            validate_empty_read_only_marker(ready_path, "guardian ready marker")
            return {"status": "ready", "root_pid": control["root_pid"]}
        if time.monotonic() >= deadline:
            raise GuardError("guardian did not become ready before timeout")
        time.sleep(0.01)


def release_launch(
    control_path: Path, ready_path: Path, launch_path: Path, interval_ms: int
) -> dict[str, Any]:
    control = validate_control(
        control_path,
        ready_path,
        launch_path,
        interval_ms,
        require_live=True,
    )
    validate_empty_read_only_marker(ready_path, "guardian ready marker")
    create_empty_read_only_marker(launch_path, "launch marker")
    return {"status": "released", "root_pid": control["root_pid"]}


def _wait_for_bound_control(
    *,
    runner_pid: int,
    root_pid: int,
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> tuple[dict[str, Any], int, int]:
    runner = require_running_process_identity(runner_pid, "runner")
    root = require_running_process_identity(root_pid, "held workload root")
    runner_start = int(runner["starttime_ticks"])
    root_start = int(root["starttime_ticks"])
    if root["ppid"] != runner_pid:
        raise GuardError("held workload root is not parented by the runner")
    deadline = time.monotonic() + 5.0
    while not control_path.exists() and not control_path.is_symlink():
        if not process_is_same_running(runner_pid, runner_start):
            raise GuardError("runner exited before control publication")
        if not process_is_same_running(root_pid, root_start):
            raise GuardError("held workload root exited before control publication")
        if time.monotonic() >= deadline:
            raise GuardError("guardian control was not published before timeout")
        time.sleep(0.005)
    control = validate_control(
        control_path,
        ready_path,
        launch_path,
        interval_ms,
        expected_runner_pid=runner_pid,
        expected_root_pid=root_pid,
        expected_guardian_pid=os.getpid(),
        require_live=True,
    )
    if (
        control["runner_starttime_ticks"] != runner_start
        or control["root_starttime_ticks"] != root_start
    ):
        raise GuardError("control bound a reused runner or held-root PID")
    return control, runner_start, root_start


def _observe_launch(launch_path: Path, violations: list[str]) -> bool:
    if not (launch_path.exists() or launch_path.is_symlink()):
        return False
    try:
        validate_empty_read_only_marker(launch_path, "launch marker")
    except GuardError as error:
        violations.append(str(error))
        return False
    return True


def _identity_status(
    pid: int, starttime_ticks: int, expected_ppid: int
) -> tuple[bool, str, int, int, str | None]:
    identity = read_process_stat_identity(pid)
    if identity is None:
        return False, "-", 0, -1, None
    state = str(identity["state"])
    ppid = int(identity["ppid"])
    current_starttime = int(identity["starttime_ticks"])
    if current_starttime != starttime_ticks:
        return False, state, current_starttime, ppid, "starttime_mismatch"
    if state in DEAD_STATES:
        return False, state, current_starttime, ppid, None
    if ppid != expected_ppid:
        return (
            False,
            state,
            current_starttime,
            ppid,
            f"ppid_mismatch:{ppid}!={expected_ppid}",
        )
    return True, state, current_starttime, ppid, None


def _clean_field(value: object) -> str:
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")


def monitor_guardian(
    runner_pid: int,
    root_pid: int,
    samples_path: Path,
    conflicts_path: Path,
    summary_path: Path,
    control_path: Path,
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
) -> dict[str, Any]:
    if runner_pid <= 1 or root_pid <= 1 or interval_ms != CADENCE_INTERVAL_MS:
        raise GuardError("formal guardian requires valid PIDs and 100 ms cadence")
    lifecycle_paths = (
        samples_path,
        conflicts_path,
        summary_path,
        control_path,
        ready_path,
        launch_path,
    )
    if any(not path.is_absolute() for path in lifecycle_paths) or any(
        path.parent != control_path.parent for path in lifecycle_paths
    ):
        raise GuardError("guardian lifecycle paths must be absolute siblings")
    _directory_non_symlink(control_path.parent, "guardian lifecycle parent")
    for output in (samples_path, conflicts_path, summary_path):
        if output.exists() or output.is_symlink():
            raise GuardError(f"refusing to reuse guardian output: {output}")
    for marker, description in (
        (ready_path, "guardian ready marker"),
        (launch_path, "launch marker"),
    ):
        if marker.exists() or marker.is_symlink():
            raise GuardError(f"refusing to reuse {description}")
    control, runner_start, root_start = _wait_for_bound_control(
        runner_pid=runner_pid,
        root_pid=root_pid,
        control_path=control_path,
        ready_path=ready_path,
        launch_path=launch_path,
        interval_ms=interval_ms,
    )
    guardian_start = int(control["guardian_starttime_ticks"])
    runner_ppid = int(control["runner_ppid"])
    root_ppid = int(control["root_ppid"])
    guardian_ppid = int(control["guardian_ppid"])
    allowed_gap = guardian_maximum_allowed_gap_ns(interval_ms)
    timestamps: list[int] = []
    ready_poll: int | None = None
    ready_elapsed: int | None = None
    launch_poll: int | None = None
    launch_elapsed: int | None = None
    root_seen = False
    terminal_poll: int | None = None
    observed_conflicts: list[dict[str, Any]] = []
    identity_violations: list[str] = []
    handshake_violations: list[str] = []
    termination = _empty_termination(root_start)
    started = time.monotonic_ns()
    terminal_elapsed = 0
    next_poll_ns = started
    with samples_path.open("x", encoding="utf-8") as samples, conflicts_path.open(
        "x", encoding="utf-8"
    ) as conflicts:
        samples.write("\t".join(SAMPLE_COLUMNS) + "\n")
        conflicts.write("\t".join(CONFLICT_COLUMNS) + "\n")
        while True:
            now = time.monotonic_ns()
            if now < next_poll_ns:
                time.sleep((next_poll_ns - now) / 1_000_000_000)
            elapsed = time.monotonic_ns() - started
            timestamps.append(elapsed)
            poll = len(timestamps)
            recorded_at = dt.datetime.now(dt.timezone.utc).isoformat()
            (
                runner_running,
                _runner_state,
                current_runner_start,
                current_runner_ppid,
                runner_error,
            ) = _identity_status(runner_pid, runner_start, runner_ppid)
            (
                root_running,
                root_state,
                current_root_start,
                current_root_ppid,
                root_error,
            ) = _identity_status(root_pid, root_start, root_ppid)
            (
                guardian_running,
                _guardian_state,
                current_guardian_start,
                current_guardian_ppid,
                guardian_error,
            ) = _identity_status(os.getpid(), guardian_start, guardian_ppid)
            root_seen = root_seen or root_running
            launch_seen = _observe_launch(launch_path, handshake_violations)
            if launch_seen and launch_poll is None:
                launch_poll = poll
                launch_elapsed = elapsed
            current_conflicts = scan_conflicts(
                root_pid=root_pid, root_starttime_ticks=root_start
            )
            for process in current_conflicts:
                conflicts.write(
                    "\t".join(
                        (
                            str(poll),
                            str(elapsed),
                            recorded_at,
                            str(process["pid"]),
                            str(process["ppid"]),
                            str(process["state"]),
                            str(process["starttime_ticks"]),
                            format(float(process["cpu_percent"]), ".17g"),
                            _clean_field(process["name"]),
                            _clean_field(process["command"]),
                        )
                    )
                    + "\n"
                )
            observed_conflicts.extend(current_conflicts)
            for role, error in (
                ("runner", runner_error),
                ("root", root_error),
                ("guardian", guardian_error),
            ):
                if error is not None:
                    identity_violations.append(f"{role}:{error}")
            samples.write(
                "\t".join(
                    (
                        str(poll),
                        str(elapsed),
                        recorded_at,
                        str(runner_running).lower(),
                        str(current_runner_start),
                        str(current_runner_ppid),
                        str(root_running).lower(),
                        root_state,
                        str(current_root_start),
                        str(current_root_ppid),
                        str(guardian_running).lower(),
                        str(current_guardian_start),
                        str(current_guardian_ppid),
                        str(launch_seen).lower(),
                        str(len(current_conflicts)),
                    )
                )
                + "\n"
            )
            samples.flush()
            conflicts.flush()
            if poll == 1:
                os.fsync(samples.fileno())
                os.fsync(conflicts.fileno())
            maximum_gap = derive_guardian_maximum_poll_start_gap_ns(
                timestamps, elapsed
            )
            if ready_poll is None:
                if launch_seen:
                    handshake_violations.append(
                        "launch marker existed before guardian readiness"
                    )
                elif (
                    runner_running
                    and root_running
                    and guardian_running
                    and not current_conflicts
                    and not identity_violations
                    and maximum_gap <= allowed_gap
                ):
                    create_empty_read_only_marker(ready_path, "guardian ready marker")
                    ready_poll = poll
                    ready_elapsed = elapsed
            else:
                try:
                    validate_empty_read_only_marker(ready_path, "guardian ready marker")
                except GuardError as error:
                    handshake_violations.append(str(error))
            failure = bool(
                current_conflicts
                or identity_violations
                or handshake_violations
                or maximum_gap > allowed_gap
                or not runner_running
                or not guardian_running
            )
            if failure:
                termination = terminate_process_tree(root_pid, root_start)
            if not root_running:
                terminal_poll = poll
                if not launch_seen:
                    handshake_violations.append(
                        "held workload root exited before launch observation"
                    )
                terminal_elapsed = time.monotonic_ns() - started
                samples.flush()
                conflicts.flush()
                os.fsync(samples.fileno())
                os.fsync(conflicts.fileno())
                os.fchmod(samples.fileno(), 0o444)
                os.fchmod(conflicts.fileno(), 0o444)
                break
            if failure:
                next_poll_ns = time.monotonic_ns()
            else:
                next_poll_ns += interval_ms * 1_000_000
    _fsync_directory(samples_path.parent)
    maximum_gap = derive_guardian_maximum_poll_start_gap_ns(
        timestamps, terminal_elapsed
    )
    if launch_poll is None:
        handshake_violations.append("guardian never observed the launch marker")
    complete = bool(
        root_seen
        and len(timestamps) >= 2
        and terminal_poll == len(timestamps)
        and ready_poll == 1
        and launch_poll is not None
        and launch_poll > ready_poll
        and maximum_gap <= allowed_gap
        and not observed_conflicts
        and not identity_violations
        and not handshake_violations
        and termination["attempted"] is False
    )
    result = {
        "schema": GUARDIAN_SCHEMA,
        "runner_pid": runner_pid,
        "runner_starttime_ticks": runner_start,
        "runner_ppid": runner_ppid,
        "root_pid": root_pid,
        "root_starttime_ticks": root_start,
        "root_ppid": root_ppid,
        "guardian_pid": os.getpid(),
        "guardian_starttime_ticks": guardian_start,
        "guardian_ppid": guardian_ppid,
        "interval_ms": interval_ms,
        "polls": len(timestamps),
        "terminal_elapsed_ns": terminal_elapsed,
        "poll_monotonic_elapsed_ns": timestamps,
        "maximum_poll_start_gap_ns": maximum_gap,
        "maximum_allowed_poll_start_gap_ns": allowed_gap,
        "control_path": str(control_path),
        "control_sha256": sha256_file(control_path),
        "ready_marker": str(ready_path),
        "launch_marker": str(launch_path),
        "ready_created_poll": ready_poll,
        "ready_created_monotonic_elapsed_ns": ready_elapsed,
        "launch_observed_poll": launch_poll,
        "launch_observed_monotonic_elapsed_ns": launch_elapsed,
        "terminal_sample_poll": terminal_poll,
        "root_seen": root_seen,
        "conflicts": observed_conflicts,
        "identity_violations": identity_violations,
        "handshake_violations": handshake_violations,
        "termination": termination,
        "complete_and_conflict_free": complete,
    }
    publish_json_read_only_atomic_exclusive(summary_path, result)
    if not complete:
        raise GuardError("continuous guardian did not complete its exact lifecycle")
    return result


def cleanup_controlled_processes(
    control_path: Path, ready_path: Path, launch_path: Path, interval_ms: int
) -> dict[str, Any]:
    control = validate_control(
        control_path, ready_path, launch_path, interval_ms, require_live=False
    )
    roles = ["root", "guardian"]
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
    incomplete = {role: value for role, value in incomplete.items() if value}
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
    ready_path: Path,
    launch_path: Path,
    interval_ms: int,
    expected_root_pid: int,
) -> dict[str, Any]:
    """Fail closed using only the already-published root identity."""
    control = validate_control(
        control_path,
        ready_path,
        launch_path,
        interval_ms,
        expected_root_pid=expected_root_pid,
        require_live=False,
    )
    return terminate_process_tree(
        int(control["root_pid"]), int(control["root_starttime_ticks"])
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("scan-conflicts").add_argument(
        "--output", type=Path, required=True
    )
    create = commands.add_parser("create-control")
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--ready", type=Path, required=True)
    create.add_argument("--launch", type=Path, required=True)
    create.add_argument("--runner-pid", type=int, required=True)
    create.add_argument("--root-pid", type=int, required=True)
    create.add_argument("--guardian-pid", type=int, required=True)
    create.add_argument("--interval-ms", type=int, required=True)
    ready = commands.add_parser("wait-ready")
    ready.add_argument("--control", type=Path, required=True)
    ready.add_argument("--ready", type=Path, required=True)
    ready.add_argument("--launch", type=Path, required=True)
    ready.add_argument("--interval-ms", type=int, required=True)
    ready.add_argument("--timeout-ms", type=int, required=True)
    release = commands.add_parser("release-launch")
    release.add_argument("--control", type=Path, required=True)
    release.add_argument("--ready", type=Path, required=True)
    release.add_argument("--launch", type=Path, required=True)
    release.add_argument("--interval-ms", type=int, required=True)
    monitor = commands.add_parser("monitor")
    monitor.add_argument("--runner-pid", type=int, required=True)
    monitor.add_argument("--root-pid", type=int, required=True)
    monitor.add_argument("--samples", type=Path, required=True)
    monitor.add_argument("--conflicts", type=Path, required=True)
    monitor.add_argument("--summary", type=Path, required=True)
    monitor.add_argument("--control", type=Path, required=True)
    monitor.add_argument("--ready", type=Path, required=True)
    monitor.add_argument("--launch", type=Path, required=True)
    monitor.add_argument("--interval-ms", type=int, required=True)
    cleanup = commands.add_parser("cleanup-control")
    cleanup.add_argument("--control", type=Path, required=True)
    cleanup.add_argument("--ready", type=Path, required=True)
    cleanup.add_argument("--launch", type=Path, required=True)
    cleanup.add_argument("--interval-ms", type=int, required=True)
    terminate = commands.add_parser("terminate-tree")
    terminate.add_argument("--root-pid", type=int, required=True)
    terminate.add_argument("--root-starttime-ticks", type=int, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "scan-conflicts":
            value = record_conflict_scan(args.output)
        elif args.command == "create-control":
            value = create_control(
                args.output,
                args.ready,
                args.launch,
                args.runner_pid,
                args.root_pid,
                args.guardian_pid,
                args.interval_ms,
            )
        elif args.command == "wait-ready":
            value = wait_ready(
                args.control,
                args.ready,
                args.launch,
                args.interval_ms,
                args.timeout_ms,
            )
        elif args.command == "release-launch":
            value = release_launch(
                args.control, args.ready, args.launch, args.interval_ms
            )
        elif args.command == "monitor":
            value = monitor_guardian(
                args.runner_pid,
                args.root_pid,
                args.samples,
                args.conflicts,
                args.summary,
                args.control,
                args.ready,
                args.launch,
                args.interval_ms,
            )
        elif args.command == "cleanup-control":
            value = cleanup_controlled_processes(
                args.control, args.ready, args.launch, args.interval_ms
            )
        elif args.command == "terminate-tree":
            value = terminate_process_tree(
                args.root_pid, args.root_starttime_ticks
            )
        else:  # pragma: no cover
            raise GuardError(f"unsupported command: {args.command}")
    except (GuardError, OSError, ValueError) as error:
        if args.command == "monitor":
            try:
                terminate_root_from_control(
                    args.control,
                    args.ready,
                    args.launch,
                    args.interval_ms,
                    args.root_pid,
                )
            except (GuardError, OSError, ValueError):
                pass
        print(f"phase4 range guardian: {error}", file=os.sys.stderr)
        return 1
    print(json.dumps(value, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
