#!/usr/bin/env python3
"""Fail-closed helpers for the live-query D/P/Q ingestion screen.

D disables live publication, P enables publication without an HTTP client, and
Q enables publication with a controlled HTTP query client.  The module reuses
the Phase 1 tree, timing, RSS, and Markdown parsing primitives, but deliberately
does not import Phase 1's fixed four-million-message expectations.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import platform
import re
import stat
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

import phase1_replay_gate as phase1


class GateError(ValueError):
    pass


WORKLOAD_SCHEMA = "chronoxide/live-query-ingest-workload/v1"
CLIENT_SCHEMA = "chronoxide/live-query-ingest-client/v1"
RUN_SET_SCHEMA = "chronoxide/live-query-ingest-ab/v1"
SHUTDOWN_AB_SCHEMA = "chronoxide/live-query-shutdown-ab/v1"
PUBLICATION_SCALE_SCHEMA = "chronoxide/live-query-publication-scale/v2"
SELECTED_INPUT_PREFIX_SCHEMA = (
    "chronoxide/live-query-selected-input-prefix/v1"
)
PUBLICATION_SCALE_BOOTSTRAP_SCHEMA = (
    "chronoxide/live-query-publication-scale-bootstrap/v1"
)
HOST_PROCESS_EVIDENCE_SCHEMA = "chronoxide/live-query-host-process-evidence/v1"
HOST_PROCESS_READY_SCHEMA = "chronoxide/live-query-host-process-ready/v1"
HOST_PROCESS_BOUNDARY_SCHEMA = "chronoxide/live-query-host-process-boundary/v1"
SCALE_MESSAGE_COUNTS = {125_000: "smoke-125k", 250_000: "mandatory-250k"}
SCALE_COMMON_LIMITS_NS = {
    "boundary_p95_ns": 10_000_000_000,
    "boundary_max_ns": 15_000_000_000,
    "shutdown_post_seal_ns": 60_000_000_000,
}
SCALE_MANDATORY_LIMITS_NS = {
    "shutdown_sample_catalog_ns": 10_000_000_000,
    "shutdown_post_commit_ns": 30_000_000_000,
}
SCALE_MANDATORY_LATE_CUT_MIN_PERCENT = 90
SCALE_HOST_PROCESS_SAMPLE_INTERVAL_MS = 250
SCALE_HOST_PROCESS_MAX_GAP_MULTIPLIER = 2
SCALE_HOST_PROCESS_BOUNDARY_SLACK_NS = 50_000_000
SCALE_RSS_MAX_GAP_MULTIPLIER = 2
SCALE_HOST_PROCESS_MAX_VANISHED_PER_SCAN = 8
SCALE_HOST_PROCESS_MAX_VANISHED_PPM = 1_000
SCALE_CONFLICTING_COMPILER_PATTERN = (
    r"(?:[A-Za-z0-9_]+-)*"
    r"(?:cc|c\+\+|clang|clang\+\+|gcc|g\+\+)"
    r"(?:[.-][A-Za-z0-9_.+-]+)?"
)
SCALE_EXPECTED_READBACK_QUERIES = {
    125_000: 26,
    250_000: 30,
}
SCALE_READBACK_CONTRACT_AMENDMENT = (
    "The v1 validator incorrectly reused the 125k prefix's 26-query "
    "readback cardinality for the 250k prefix. The immutable 250k report "
    "contains 30 expected, executed, and checked queries with zero skips, "
    "isolation skips, or mismatches."
)
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
try:
    _CHRONOXIDE_SCALE_VALIDATOR_BOOTSTRAP
except NameError:
    _CHRONOXIDE_SCALE_VALIDATOR_BOOTSTRAP = None
SCALE_P_ONLY_COVERAGE_GAP = (
    "No version-matched capture-level physical-row golden exists for this "
    "prefix. P-only live-handoff validation has no independent per-window "
    "writer-row reconciliation; the exhaustive Schema 8 footer/postings "
    "verifier and independent readbacks remain authoritative for the "
    "persisted corpus."
)
SCALE_QUIET_HOST_LIMITS = {
    "max_host_load_per_cpu": 1.0,
    "max_cpu_psi_avg10": 10.0,
    "max_io_psi_avg10": 5.0,
    "max_memory_psi_avg10": 2.0,
}
SCALE_CONFLICTING_PROCESS_NAMES = {
    "bazel",
    "buildah",
    "c++",
    "cargo",
    "cc",
    "cc1",
    "cc1plus",
    "clang",
    "clang++",
    "clang++.real",
    "cmake",
    "collect2",
    "dd",
    "docker",
    "fio",
    "g++",
    "gcc",
    "gmake",
    "gradle",
    "gradlew",
    "java",
    "javac",
    "ld",
    "ld.gold",
    "ld.lld",
    "lld",
    "make",
    "memgraph",
    "mold",
    "mvn",
    "mvnw",
    "ninja",
    "ninja-build",
    "perf",
    "podman",
    "prometheus",
    "qemu-aarch64",
    "rsync",
    "rustc",
    "soong_ui",
    "stress",
    "stress-ng",
    "sysbench",
}
SCALE_CONFLICTING_PROCESS_PREFIXES = (
    "chronoxide-",
    "greptime",
    "qemu-system",
)
REQUIRED_PERF_EVENTS = (
    "task-clock",
    "cycles",
    "instructions",
    "cache-misses",
    "context-switches",
    "cpu-migrations",
    "page-faults",
)
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
SAFE_NAME = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
VALIDATED_CAPTURE_CAPACITY_FIELDS = frozenset(
    {
        "capture",
        "capture_files",
        "capture_manifest_sha256",
        "config_template",
        "config_template_sha256",
        "stop_after_messages",
    }
)
SELECTED_INPUT_PREFIX_IDENTITY_FIELDS = (
    "capture",
    "capture_files",
    "capture_manifest_sha256",
    "config_template",
    "config_template_sha256",
)
SELECTED_INPUT_PREFIX_FIELDS = frozenset(
    {
        "schema",
        *SELECTED_INPUT_PREFIX_IDENTITY_FIELDS,
        "validated_capture_capacity_messages",
        "stop_after_messages",
    }
)


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def write_json_exclusive(path: Path, value: Any) -> None:
    phase1._write_json_exclusive(path, value)


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def distribution(values: list[int]) -> dict[str, int]:
    if not values:
        return {"count": 0, "min": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
    ordered = sorted(values)

    def percentile(numerator: int) -> int:
        # Nearest-rank percentile, with the rank clamped to this non-empty set.
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


def nonnegative_int(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GateError(f"{context} must be a non-negative integer")
    return value


def positive_int(value: Any, context: str) -> int:
    result = nonnegative_int(value, context)
    if result == 0:
        raise GateError(f"{context} must be greater than zero")
    return result


def bind_selected_input_prefix(
    validated_capacity_path: Path,
    stop_after_messages: int,
    output: Path,
) -> dict[str, Any]:
    """Bind a previously validated capture capacity to one selected prefix."""
    capacity = load_json(validated_capacity_path)
    if (
        not isinstance(capacity, dict)
        or set(capacity) != VALIDATED_CAPTURE_CAPACITY_FIELDS
    ):
        raise GateError("validated capture-capacity document has an unexpected shape")
    validated_capacity_messages = positive_int(
        capacity["stop_after_messages"], "validated capture capacity"
    )
    selected_messages = positive_int(
        stop_after_messages, "selected input prefix"
    )
    if selected_messages > validated_capacity_messages:
        raise GateError("selected input prefix exceeds validated capture capacity")
    result = {
        "schema": SELECTED_INPUT_PREFIX_SCHEMA,
        **{
            key: capacity[key]
            for key in SELECTED_INPUT_PREFIX_IDENTITY_FIELDS
        },
    }
    result["validated_capture_capacity_messages"] = (
        validated_capacity_messages
    )
    result["stop_after_messages"] = selected_messages
    write_json_exclusive(output, result)
    return result


def _clock_boottime_ns() -> int:
    return time.clock_gettime_ns(time.CLOCK_BOOTTIME)


def _boot_id(proc_root: Path = Path("/proc")) -> str:
    value = (proc_root / "sys/kernel/random/boot_id").read_text(
        encoding="ascii"
    ).strip()
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12}",
        value,
    ) is None:
        raise GateError("host process monitor observed a malformed boot ID")
    return value


def _scale_process_classifier_contract() -> dict[str, Any]:
    return {
        "names": sorted(SCALE_CONFLICTING_PROCESS_NAMES),
        "prefixes": list(SCALE_CONFLICTING_PROCESS_PREFIXES),
        "compiler_pattern": SCALE_CONFLICTING_COMPILER_PATTERN,
    }


def _proc_visibility_contract(
    proc_root: Path = Path("/proc"),
) -> dict[str, Any]:
    try:
        mountinfo = (proc_root / "self/mountinfo").read_text(
            encoding="utf-8", errors="replace"
        )
        status = (proc_root / "self/status").read_text(
            encoding="utf-8", errors="replace"
        )
        self_namespace = os.readlink(proc_root / "self/ns/pid")
    except OSError as error:
        raise GateError(
            "host process monitor cannot prove /proc process visibility"
        ) from error

    proc_mount_options: set[str] | None = None
    for line in mountinfo.splitlines():
        before, separator, after = line.partition(" - ")
        before_fields = before.split()
        after_fields = after.split()
        if (
            separator
            and len(before_fields) >= 6
            and len(after_fields) >= 3
            and before_fields[4] == str(proc_root)
            and after_fields[0] == "proc"
        ):
            proc_mount_options = set(before_fields[5].split(","))
            proc_mount_options.update(after_fields[2].split(","))
            break
    if proc_mount_options is None:
        raise GateError("host process monitor cannot find the /proc mount")
    hidepid_values = {
        option.partition("=")[2]
        for option in proc_mount_options
        if option.startswith("hidepid=")
    }
    if hidepid_values - {"0"} or len(hidepid_values) > 1:
        raise GateError(
            "host process monitor requires /proc hidepid=0 visibility"
        )

    nspid_fields = None
    for line in status.splitlines():
        if line.startswith("NSpid:"):
            nspid_fields = line.partition(":")[2].split()
            break
    if (
        nspid_fields is None
        or len(nspid_fields) != 1
        or not nspid_fields[0].isdigit()
        or re.fullmatch(r"pid:\[[0-9]+\]", self_namespace) is None
    ):
        raise GateError(
            "host process monitor requires one visible PID-namespace level"
        )
    pid1 = _proc_process_record(1, proc_root)
    if pid1 is None:
        raise GateError("host process monitor cannot read visible PID 1")
    return {
        "hidepid": 0,
        "nspid_depth": 1,
        "pid_namespace": self_namespace,
        "pid1_stat_visible": True,
        "pid1_starttime_ticks": pid1["starttime_ticks"],
    }


def _proc_process_record(
    pid: int, proc_root: Path = Path("/proc")
) -> dict[str, Any] | None:
    stat_path = proc_root / str(pid) / "stat"
    try:
        stat_text = stat_path.read_bytes().decode("utf-8", errors="replace")
    except (FileNotFoundError, ProcessLookupError):
        return None
    except OSError as error:
        raise GateError(
            f"host process monitor cannot read /proc/{pid}/stat"
        ) from error
    opening = stat_text.find("(")
    closing = stat_text.rfind(")")
    if opening <= 0 or closing <= opening or stat_text[:opening].strip() != str(pid):
        raise GateError(f"host process monitor cannot parse /proc/{pid}/stat")
    tail = stat_text[closing + 1 :].split()
    if len(tail) < 20:
        raise GateError(f"host process monitor found a short /proc/{pid}/stat")
    try:
        record = {
            "pid": pid,
            "comm": stat_text[opening + 1 : closing],
            "state": tail[0],
            "ppid": int(tail[1]),
            "pgrp": int(tail[2]),
            "session": int(tail[3]),
            "starttime_ticks": int(tail[19]),
        }
    except ValueError as error:
        raise GateError(
            f"host process monitor found malformed numeric fields for PID {pid}"
        ) from error
    try:
        with (proc_root / str(pid) / "cmdline").open("rb") as source:
            command = source.read(8193)
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        command = b""
    except OSError as error:
        raise GateError(
            f"host process monitor cannot read /proc/{pid}/cmdline"
        ) from error
    if len(command) > 8192 and b"\0" not in command[:8192]:
        raise GateError(
            f"host process monitor cannot bound /proc/{pid}/cmdline argv0"
        )
    argv0 = command.split(b"\0", 1)[0]
    record["argv0"] = (
        argv0.decode("utf-8", errors="replace") if argv0 else None
    )
    return record


def _proc_process_snapshot(
    proc_root: Path = Path("/proc"),
) -> tuple[list[dict[str, Any]], int, int]:
    pids = sorted(
        int(entry.name)
        for entry in proc_root.iterdir()
        if entry.name.isascii() and entry.name.isdigit()
    )
    processes = []
    vanished = 0
    for pid in pids:
        record = _proc_process_record(pid, proc_root)
        if record is None:
            vanished += 1
        else:
            processes.append(record)
    return processes, len(pids), vanished


def _compact_json_line(value: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        + b"\n"
    )


def _host_process_sample(sequence: int) -> dict[str, Any]:
    scan_started = _clock_boottime_ns()
    processes, listed, vanished = _proc_process_snapshot()
    scan_ended = _clock_boottime_ns()
    return {
        "kind": "sample",
        "sequence": sequence,
        "scan_started_boottime_ns": scan_started,
        "scan_ended_boottime_ns": scan_ended,
        "listed_pid_count": listed,
        "vanished_pid_count": vanished,
        "process_count": len(processes),
        "processes": processes,
    }


def monitor_host_processes(
    *,
    expected_session_id: int,
    interval_ms: int,
    abort_on_conflict: bool,
    stop_file: Path,
    ready_file: Path,
    output: Path,
) -> dict[str, Any]:
    expected_session_id = positive_int(
        expected_session_id, "host process monitor expected session"
    )
    interval_ms = positive_int(interval_ms, "host process monitor interval")
    if not isinstance(abort_on_conflict, bool):
        raise GateError("host process monitor conflict policy is malformed")
    if stop_file.exists() or stop_file.is_symlink():
        raise GateError("host process monitor stop file already exists")
    leader = _proc_process_record(expected_session_id)
    if leader is None:
        raise GateError("host process monitor cannot find the measured leader")
    if (
        leader["pid"] != leader["pgrp"]
        or leader["pid"] != leader["session"]
        or leader["state"] not in {"T", "t"}
    ):
        raise GateError(
            "host process monitor measured leader is not a stopped "
            "session/group leader"
        )
    boot_id = _boot_id()
    proc_visibility = _proc_visibility_contract()
    header = {
        "kind": "header",
        "schema": HOST_PROCESS_EVIDENCE_SCHEMA,
        "boot_id": boot_id,
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
        "interval_ms": interval_ms,
        "abort_on_conflict": abort_on_conflict,
        "max_vanished_per_scan": (
            SCALE_HOST_PROCESS_MAX_VANISHED_PER_SCAN
        ),
        "max_vanished_ppm": SCALE_HOST_PROCESS_MAX_VANISHED_PPM,
        "classifier_sha256": canonical_sha256(
            _scale_process_classifier_contract()
        ),
        "proc_visibility": proc_visibility,
        "expected_session_id": expected_session_id,
        "expected_leader_pid": leader["pid"],
        "expected_leader_starttime_ticks": leader["starttime_ticks"],
    }
    stream_hash = hashlib.sha256()
    sample_count = 0
    first_sample: dict[str, Any] | None = None
    last_sample: dict[str, Any] | None = None
    stop_observed_boottime_ns: int | None = None
    interval_ns = interval_ms * 1_000_000
    with output.open("xb") as destination:
        header_line = _compact_json_line(header)
        destination.write(header_line)
        stream_hash.update(header_line)
        next_deadline = _clock_boottime_ns()
        while True:
            sample = _host_process_sample(sample_count)
            sample_line = _compact_json_line(sample)
            destination.write(sample_line)
            destination.flush()
            stream_hash.update(sample_line)
            first_sample = sample if first_sample is None else first_sample
            last_sample = sample
            sample_count += 1
            conflicting_processes = [
                process
                for process in sample["processes"]
                if process["session"] != expected_session_id
                and _is_conflicting_scale_process(process)
            ]
            if abort_on_conflict and conflicting_processes:
                os.fsync(destination.fileno())
                conflict = conflicting_processes[0]
                raise GateError(
                    "host process monitor observed external conflict "
                    f"pid={conflict['pid']} "
                    f"names={sorted(_process_names(conflict))!r}"
                )
            if sample_count == 1:
                os.fsync(destination.fileno())
                write_json_exclusive(
                    ready_file,
                    {
                        "schema": HOST_PROCESS_READY_SCHEMA,
                        "boot_id": boot_id,
                        "expected_session_id": expected_session_id,
                        "expected_leader_pid": leader["pid"],
                        "expected_leader_starttime_ticks": leader[
                            "starttime_ticks"
                        ],
                        "first_sample_scan_ended_boottime_ns": sample[
                            "scan_ended_boottime_ns"
                        ],
                        "header_sha256": hashlib.sha256(header_line).hexdigest(),
                    },
                )
            if stop_observed_boottime_ns is not None:
                break
            if stop_file.exists():
                stop_observed_boottime_ns = _clock_boottime_ns()
                continue
            next_deadline += interval_ns
            remaining_ns = next_deadline - _clock_boottime_ns()
            if remaining_ns > 0:
                time.sleep(remaining_ns / 1_000_000_000)
        assert (
            first_sample is not None
            and last_sample is not None
            and stop_observed_boottime_ns is not None
        )
        footer = {
            "kind": "footer",
            "sample_count": sample_count,
            "first_scan_started_boottime_ns": first_sample[
                "scan_started_boottime_ns"
            ],
            "last_scan_ended_boottime_ns": last_sample[
                "scan_ended_boottime_ns"
            ],
            "stop_observed_boottime_ns": stop_observed_boottime_ns,
            "stream_sha256": stream_hash.hexdigest(),
        }
        destination.write(_compact_json_line(footer))
        destination.flush()
        os.fsync(destination.fileno())
    return {
        "samples": sample_count,
        "first_scan_started_boottime_ns": footer[
            "first_scan_started_boottime_ns"
        ],
        "last_scan_ended_boottime_ns": footer[
            "last_scan_ended_boottime_ns"
        ],
        "stop_observed_boottime_ns": footer[
            "stop_observed_boottime_ns"
        ],
    }


def record_host_process_boundary(
    *,
    phase: str,
    expected_leader_pid: int,
    output: Path,
    start_boundary: Path | None,
) -> dict[str, Any]:
    expected_leader_pid = positive_int(
        expected_leader_pid, "host process boundary leader PID"
    )
    boot_id = _boot_id()
    leader = _proc_process_record(expected_leader_pid)
    if phase == "start":
        if start_boundary is not None:
            raise GateError("host process start boundary has an unexpected input")
        if (
            leader is None
            or leader["pgrp"] != expected_leader_pid
            or leader["session"] != expected_leader_pid
            or leader["state"] not in {"T", "t"}
        ):
            raise GateError(
                "host process start boundary leader is not stopped and isolated"
            )
        starttime_ticks = leader["starttime_ticks"]
        leader_present = True
    elif phase == "end":
        if start_boundary is None:
            raise GateError("host process end boundary lacks its start boundary")
        start = load_json(start_boundary)
        if (
            not isinstance(start, dict)
            or start.get("schema") != HOST_PROCESS_BOUNDARY_SCHEMA
            or start.get("phase") != "start"
            or start.get("boot_id") != boot_id
            or start.get("expected_leader_pid") != expected_leader_pid
        ):
            raise GateError("host process end boundary has a mismatched start")
        starttime_ticks = positive_int(
            start.get("expected_leader_starttime_ticks"),
            "host process start-boundary leader start time",
        )
        leader_present = (
            leader is not None and leader["starttime_ticks"] == starttime_ticks
        )
        if leader_present:
            raise GateError(
                "host process end boundary still observes the measured leader"
            )
    else:
        raise GateError(f"unknown host process boundary phase {phase!r}")
    value = {
        "schema": HOST_PROCESS_BOUNDARY_SCHEMA,
        "phase": phase,
        "boot_id": boot_id,
        "recorded_boottime_ns": _clock_boottime_ns(),
        "expected_leader_pid": expected_leader_pid,
        "expected_leader_starttime_ticks": starttime_ticks,
        "expected_leader_present": leader_present,
    }
    write_json_exclusive(output, value)
    return value


def _process_names(record: dict[str, Any]) -> set[str]:
    result = {record["comm"]}
    argv0 = record.get("argv0")
    if isinstance(argv0, str) and argv0:
        result.add(Path(argv0).name)
    return result


def _is_conflicting_scale_process(record: dict[str, Any]) -> bool:
    for name in _process_names(record):
        if (
            name in SCALE_CONFLICTING_PROCESS_NAMES
            or name.startswith(SCALE_CONFLICTING_PROCESS_PREFIXES)
            or re.fullmatch(SCALE_CONFLICTING_COMPILER_PATTERN, name)
        ):
            return True
    return False


def load_workload(path: Path) -> dict[str, Any]:
    value = load_json(path)
    if not isinstance(value, dict) or value.get("schema") != WORKLOAD_SCHEMA:
        raise GateError(f"unsupported workload schema in {path}")
    client = value.get("client")
    queries = value.get("queries")
    if not isinstance(client, dict) or not isinstance(queries, list) or not queries:
        raise GateError("workload requires a client object and non-empty queries")
    normalized_client = {
        "readiness_timeout_ms": positive_int(
            client.get("readiness_timeout_ms"), "client.readiness_timeout_ms"
        ),
        "request_timeout_ms": positive_int(
            client.get("request_timeout_ms"), "client.request_timeout_ms"
        ),
        "inter_batch_delay_ms": nonnegative_int(
            client.get("inter_batch_delay_ms"), "client.inter_batch_delay_ms"
        ),
        "parallelism": positive_int(
            client.get("parallelism"), "client.parallelism"
        ),
        "max_response_bytes": positive_int(
            client.get("max_response_bytes"), "client.max_response_bytes"
        ),
        "minimum_successful_requests": positive_int(
            client.get("minimum_successful_requests"),
            "client.minimum_successful_requests",
        ),
        "minimum_requests_per_query": positive_int(
            client.get("minimum_requests_per_query"),
            "client.minimum_requests_per_query",
        ),
        "minimum_same_generation_groups_per_query": positive_int(
            client.get("minimum_same_generation_groups_per_query"),
            "client.minimum_same_generation_groups_per_query",
        ),
    }
    if normalized_client["parallelism"] < 2:
        raise GateError("client.parallelism must be at least two")

    normalized_queries: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, query in enumerate(queries):
        context = f"queries[{index}]"
        if not isinstance(query, dict):
            raise GateError(f"{context} must be an object")
        name = query.get("name")
        mode = query.get("mode")
        expression = query.get("query")
        if not isinstance(name, str) or SAFE_NAME.fullmatch(name) is None:
            raise GateError(f"{context}.name is unsafe")
        if name in names:
            raise GateError(f"duplicate query name: {name}")
        names.add(name)
        if not isinstance(expression, str) or not expression or "\n" in expression:
            raise GateError(f"{context}.query must be one non-empty line")
        if mode == "instant":
            if set(query) != {
                "name",
                "mode",
                "query",
                "time",
                "require_nonempty",
                "require_empty",
            }:
                raise GateError(f"{context} has invalid instant-query fields")
            timestamp = query.get("time")
            if not isinstance(timestamp, str) or not timestamp:
                raise GateError(f"{context}.time must be a non-empty string")
            if not isinstance(query.get("require_nonempty"), bool):
                raise GateError(f"{context}.require_nonempty must be boolean")
            if not isinstance(query.get("require_empty"), bool):
                raise GateError(f"{context}.require_empty must be boolean")
            if query["require_nonempty"] and query["require_empty"]:
                raise GateError(f"{context} cannot require both empty and non-empty")
            normalized_queries.append(
                {
                    "name": name,
                    "mode": mode,
                    "query": expression,
                    "time": timestamp,
                    "require_nonempty": query["require_nonempty"],
                    "require_empty": query["require_empty"],
                }
            )
        elif mode == "range":
            if set(query) != {
                "name",
                "mode",
                "query",
                "start",
                "end",
                "step",
                "require_nonempty",
                "require_empty",
            }:
                raise GateError(f"{context} has invalid range-query fields")
            fields = {}
            for field in ("start", "end", "step"):
                item = query.get(field)
                if not isinstance(item, str) or not item:
                    raise GateError(f"{context}.{field} must be a non-empty string")
                fields[field] = item
            if not isinstance(query.get("require_nonempty"), bool):
                raise GateError(f"{context}.require_nonempty must be boolean")
            if not isinstance(query.get("require_empty"), bool):
                raise GateError(f"{context}.require_empty must be boolean")
            if query["require_nonempty"] and query["require_empty"]:
                raise GateError(f"{context} cannot require both empty and non-empty")
            normalized_queries.append(
                {
                    "name": name,
                    "mode": mode,
                    "query": expression,
                    "require_nonempty": query["require_nonempty"],
                    "require_empty": query["require_empty"],
                    **fields,
                }
            )
        else:
            raise GateError(f"{context}.mode must be instant or range")
    return {
        "schema": WORKLOAD_SCHEMA,
        "description": str(value.get("description", "")),
        "client": normalized_client,
        "queries": normalized_queries,
    }


def parse_cpuset(text: str) -> set[int]:
    if not text or re.fullmatch(r"[0-9,-]+", text) is None:
        raise GateError(f"invalid CPU set: {text!r}")
    result: set[int] = set()
    for item in text.split(","):
        if not item:
            raise GateError(f"invalid CPU set: {text!r}")
        if "-" in item:
            fields = item.split("-")
            if len(fields) != 2:
                raise GateError(f"invalid CPU range: {item!r}")
            start, end = map(int, fields)
            if end < start:
                raise GateError(f"reversed CPU range: {item!r}")
            result.update(range(start, end + 1))
        else:
            result.add(int(item))
    if not result:
        raise GateError("CPU set is empty")
    return result


def validate_cpusets(ingest: str, client: str) -> dict[str, Any]:
    ingest_set = parse_cpuset(ingest)
    client_set = parse_cpuset(client)
    overlap = ingest_set & client_set
    if overlap:
        raise GateError(f"ingest and client CPU sets overlap: {sorted(overlap)}")
    allowed = set(os.sched_getaffinity(0))
    unavailable = (ingest_set | client_set) - allowed
    if unavailable:
        raise GateError(
            f"CPU sets include CPUs outside this process affinity: {sorted(unavailable)}"
        )
    return {
        "ingest": sorted(ingest_set),
        "client": sorted(client_set),
        "allowed": sorted(allowed),
    }


def _set_table_values(
    text: str, table: str, assignments: dict[str, str]
) -> str:
    lines = text.splitlines(keepends=True)
    table_pattern = re.compile(r"^\s*\[([^]]+)]\s*(?:#.*)?$")
    starts: list[int] = []
    for index, line in enumerate(lines):
        match = table_pattern.match(line.rstrip("\n"))
        if match and match.group(1).strip() == table:
            starts.append(index)
    if len(starts) > 1:
        raise GateError(f"configuration contains duplicate [{table}] tables")
    if not starts:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.extend(["\n", f"[{table}]\n"])
        lines.extend(f"{key} = {value}\n" for key, value in assignments.items())
        return "".join(lines)

    start = starts[0]
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if table_pattern.match(lines[index].rstrip("\n")):
            end = index
            break
    for key, rendered in assignments.items():
        pattern = re.compile(rf"^\s*{re.escape(key)}\s*=")
        matches = [index for index in range(start + 1, end) if pattern.match(lines[index])]
        if len(matches) > 1:
            raise GateError(f"configuration contains duplicate {table}.{key}")
        if matches:
            lines[matches[0]] = f"{key} = {rendered}\n"
        else:
            lines.insert(end, f"{key} = {rendered}\n")
            end += 1
    return "".join(lines)


def render_config(
    template: Path,
    output: Path,
    capture: Path,
    segments_dir: Path,
    stop_after_messages: int,
    variant: str,
    listen: str,
    publish_interval_ms: int,
    max_staleness_ms: int,
    memory_admission_bytes: int,
    max_concurrent_queries: int,
    range_cache_bytes: int,
) -> dict[str, Any]:
    if variant not in {"D", "P", "Q"}:
        raise GateError("variant must be D, P, or Q")
    if output.exists() or segments_dir.exists():
        raise GateError("configuration output and segment root must both be fresh")
    if not output.is_absolute() or not segments_dir.is_absolute():
        raise GateError("configuration output and segment root must be absolute")
    lines = template.read_text(encoding="utf-8").splitlines(keepends=True)
    phase1._replace_assignment(
        lines, "ingestion", "replay_from", json.dumps(str(capture))
    )
    phase1._replace_assignment(
        lines, "ingestion", "stop_after_messages", str(stop_after_messages)
    )
    phase1._replace_assignment(
        lines,
        "ingestion.segment_writer",
        "segments_dir",
        json.dumps(str(segments_dir)),
    )
    rendered = _set_table_values(
        "".join(lines),
        "api",
        {
            "enabled": "true" if variant != "D" else "false",
            "listen": json.dumps(listen),
            "head_publish_interval_ms": str(positive_int(publish_interval_ms, "publish interval")),
            "max_view_staleness_ms": str(positive_int(max_staleness_ms, "max staleness")),
            "live_memory_admission_bytes": str(
                positive_int(memory_admission_bytes, "memory admission")
            ),
            "max_concurrent_queries": str(
                positive_int(max_concurrent_queries, "max concurrent queries")
            ),
            "query_max_series_matched": "1000000",
            "query_max_projected_series": "2000000",
            "query_max_chunks_read": "5000000",
            "query_max_bytes_read": "2147483648",
            "query_max_samples": "50000000",
            "regex_max_expanded_values": "100000",
            "chunk_read_mode": json.dumps("pread"),
            "chunk_read_queue_depth": "128",
            "chunk_payload_coalesce_max_gap_bytes": "4096",
            "experimental_cross_segment_chunk_reads": "false",
            "range_scalar_cache_max_bytes": str(
                nonnegative_int(range_cache_bytes, "range cache bytes")
            ),
        },
    )
    with output.open("x", encoding="utf-8") as destination:
        destination.write(rendered)
    with output.open("rb") as source:
        document = tomllib.load(source)
    phase1._config_contract(document)
    ingestion = document["ingestion"]
    writer = ingestion["segment_writer"]
    api = document["api"]
    if ingestion.get("capture_to") not in (None, ""):
        raise GateError("rendered configuration must not write a capture")
    if ingestion.get("replay_from") != str(capture):
        raise GateError("rendered replay path differs from the selected capture")
    if ingestion.get("stop_after_messages") != stop_after_messages:
        raise GateError("rendered message limit differs from the screen limit")
    if writer.get("segments_dir") != str(segments_dir):
        raise GateError("rendered segment root differs from the fresh run root")
    if api.get("enabled") != (variant != "D"):
        raise GateError("rendered API mode differs from the selected variant")
    expected_api = {
        "head_publish_interval_ms": publish_interval_ms,
        "max_view_staleness_ms": max_staleness_ms,
        "live_memory_admission_bytes": memory_admission_bytes,
        "max_concurrent_queries": max_concurrent_queries,
        "query_max_series_matched": 1_000_000,
        "query_max_projected_series": 2_000_000,
        "query_max_chunks_read": 5_000_000,
        "query_max_bytes_read": 2_147_483_648,
        "query_max_samples": 50_000_000,
        "regex_max_expanded_values": 100_000,
        "chunk_read_mode": "pread",
        "chunk_read_queue_depth": 128,
        "chunk_payload_coalesce_max_gap_bytes": 4096,
        "experimental_cross_segment_chunk_reads": False,
        "range_scalar_cache_max_bytes": range_cache_bytes,
    }
    for key, expected in expected_api.items():
        if api.get(key) != expected:
            raise GateError(f"rendered api.{key} differs from the explicit control")
    return {
        "variant": variant,
        "api_enabled": api["enabled"],
        "capture": str(capture),
        "segments_dir": str(segments_dir),
        "stop_after_messages": stop_after_messages,
        "config_sha256": phase1._sha256_file(output),
    }


def _header_uint(headers: Any, name: str) -> int:
    value = headers.get(name)
    if value is None or re.fullmatch(r"[0-9]+", value) is None:
        raise GateError(f"successful live response is missing integer header {name}")
    return int(value)


def _cardinality(data: dict[str, Any]) -> tuple[int, int]:
    result_type = data.get("resultType")
    result = data.get("result")
    if result_type == "scalar":
        return (0, 0) if result is None else (1, 1)
    if result_type == "vector" and isinstance(result, list):
        return len(result), len(result)
    if result_type == "matrix" and isinstance(result, list):
        samples = 0
        for row in result:
            if not isinstance(row, dict) or not isinstance(row.get("values"), list):
                raise GateError("matrix response contains a malformed row")
            samples += len(row["values"])
        return len(result), samples
    raise GateError(f"unsupported or malformed Prometheus result type: {result_type!r}")


def _query_url(base_url: str, query: dict[str, Any]) -> str:
    if query["mode"] == "instant":
        path = "/api/v1/query"
        parameters = {"query": query["query"], "time": query["time"]}
    else:
        path = "/api/v1/query_range"
        parameters = {
            "query": query["query"],
            "start": query["start"],
            "end": query["end"],
            "step": query["step"],
        }
    return f"{base_url.rstrip('/')}{path}?{urllib.parse.urlencode(parameters)}"


def request_query(
    base_url: str,
    query: dict[str, Any],
    timeout_ms: int,
    max_response_bytes: int,
) -> dict[str, Any]:
    started = time.monotonic_ns()
    request = urllib.request.Request(
        _query_url(base_url, query),
        headers={"Accept": "application/json", "Connection": "close"},
    )
    with urllib.request.urlopen(request, timeout=timeout_ms / 1000) as response:
        status = response.status
        body = response.read(max_response_bytes + 1)
        headers = response.headers
    if status != 200:
        raise GateError(f"{query['name']} returned HTTP {status}")
    if len(body) > max_response_bytes:
        raise GateError(f"{query['name']} exceeded the response byte limit")
    document = json.loads(body)
    if not isinstance(document, dict) or document.get("status") != "success":
        raise GateError(f"{query['name']} returned a non-success envelope")
    data = document.get("data")
    if not isinstance(data, dict):
        raise GateError(f"{query['name']} returned malformed data")
    stats_text = headers.get("x-chronoxide-query-stats")
    if stats_text is None:
        raise GateError("successful response is missing x-chronoxide-query-stats")
    stats = json.loads(stats_text)
    if not isinstance(stats, dict) or set(stats) != QUERY_STATS_FIELDS:
        raise GateError("query-stats header has an unexpected shape")
    stats = {
        key: nonnegative_int(value, f"query stats {key}") for key, value in stats.items()
    }
    query_io_text = headers.get("x-chronoxide-query-io")
    if query_io_text is None:
        raise GateError("successful response is missing x-chronoxide-query-io")
    query_io = json.loads(query_io_text)
    if not isinstance(query_io, dict) or set(query_io) != QUERY_IO_FIELDS:
        raise GateError("query-I/O header has an unexpected shape")
    query_io = {
        key: nonnegative_int(value, f"query I/O {key}")
        for key, value in query_io.items()
    }
    server_timing = headers.get("server-timing", "")
    queue_match = re.search(r"(?:^|,\s*)queue;dur=([0-9]+(?:\.[0-9]+)?)", server_timing)
    if queue_match is None:
        raise GateError("successful response is missing queue Server-Timing")
    queue_duration_ns = int(float(queue_match.group(1)) * 1_000_000)
    cardinality, samples = _cardinality(data)
    return {
        "schema": CLIENT_SCHEMA,
        "query_name": query["name"],
        "mode": query["mode"],
        "generation": positive_int(
            _header_uint(headers, "x-chronoxide-view-generation"), "generation"
        ),
        "visible_message_sequence": nonnegative_int(
            _header_uint(headers, "x-chronoxide-visible-message-sequence"),
            "visible message sequence",
        ),
        "catalog_revision": nonnegative_int(
            _header_uint(headers, "x-chronoxide-catalog-revision"),
            "catalog revision",
        ),
        "response_data_sha256": canonical_sha256(data),
        "cardinality": cardinality,
        "samples": samples,
        "query_stats": stats,
        "query_io": query_io,
        "query_duration_ns": _header_uint(
            headers, "x-chronoxide-query-duration-ns"
        ),
        "serialize_duration_ns": _header_uint(
            headers, "x-chronoxide-serialize-duration-ns"
        ),
        "queue_duration_ns": queue_duration_ns,
        "view_age_ms": _header_uint(headers, "x-chronoxide-view-age-ms"),
        "view_pin_wait_ns": _header_uint(
            headers, "x-chronoxide-view-pin-wait-ns"
        ),
        "view_pin_held_ns": _header_uint(
            headers, "x-chronoxide-view-pin-held-ns"
        ),
        "client_elapsed_ns": time.monotonic_ns() - started,
        "client_started_monotonic_ns": started,
        "client_completed_monotonic_ns": time.monotonic_ns(),
        "response_bytes": len(body),
    }


def execute_parallel_batch(
    query: dict[str, Any],
    parallelism: int,
    requester: Callable[[dict[str, Any]], dict[str, Any]],
    executor: concurrent.futures.ThreadPoolExecutor,
) -> list[dict[str, Any]]:
    futures = [executor.submit(requester, query) for _ in range(parallelism)]
    return [future.result() for future in futures]


def validate_client_records(
    records: list[dict[str, Any]], workload: dict[str, Any]
) -> dict[str, Any]:
    expected_names = {query["name"] for query in workload["queries"]}
    if len(records) < workload["client"]["minimum_successful_requests"]:
        raise GateError("client recorded too few successful responses")
    generation_to_cut: dict[int, tuple[int, int]] = {}
    signatures: dict[tuple[int, str], set[str]] = {}
    counts: dict[tuple[int, str], int] = {}
    per_query = {name: 0 for name in expected_names}
    nonempty_per_query = {name: 0 for name in expected_names}
    duration_fields = (
        "client_elapsed_ns",
        "query_duration_ns",
        "serialize_duration_ns",
        "queue_duration_ns",
        "view_age_ms",
        "view_pin_wait_ns",
        "view_pin_held_ns",
    )
    durations = {field: [] for field in duration_fields}
    per_query_client_ns = {name: [] for name in expected_names}
    per_query_query_ns = {name: [] for name in expected_names}
    query_stats_totals = {field: 0 for field in QUERY_STATS_FIELDS}
    query_io_totals = {field: 0 for field in QUERY_IO_FIELDS}
    client_started: list[int] = []
    client_completed: list[int] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict) or record.get("schema") != CLIENT_SCHEMA:
            raise GateError(f"client record {index} has an invalid schema")
        name = record.get("query_name")
        if name not in expected_names:
            raise GateError(f"client record {index} has an unknown query")
        generation = positive_int(record.get("generation"), f"record {index} generation")
        sequence = nonnegative_int(
            record.get("visible_message_sequence"), f"record {index} sequence"
        )
        catalog_revision = nonnegative_int(
            record.get("catalog_revision"), f"record {index} catalog revision"
        )
        cut = (sequence, catalog_revision)
        prior = generation_to_cut.setdefault(generation, cut)
        if prior != cut:
            raise GateError(
                f"generation {generation} maps to both cut {prior} and {cut}"
            )
        stats = record.get("query_stats")
        if not isinstance(stats, dict) or set(stats) != QUERY_STATS_FIELDS:
            raise GateError(f"record {index} has malformed query stats")
        for key, value in stats.items():
            nonnegative_int(value, f"record {index} query stats {key}")
            query_stats_totals[key] += value
        query_io = record.get("query_io")
        if not isinstance(query_io, dict) or set(query_io) != QUERY_IO_FIELDS:
            raise GateError(f"record {index} has malformed query I/O")
        for key, value in query_io.items():
            query_io_totals[key] += nonnegative_int(
                value, f"record {index} query I/O {key}"
            )
        for field in duration_fields:
            durations[field].append(
                nonnegative_int(record.get(field), f"record {index} {field}")
            )
        started_ns = positive_int(
            record.get("client_started_monotonic_ns"),
            f"record {index} client start",
        )
        completed_ns = positive_int(
            record.get("client_completed_monotonic_ns"),
            f"record {index} client completion",
        )
        if completed_ns < started_ns:
            raise GateError(f"record {index} client completion precedes its start")
        client_started.append(started_ns)
        client_completed.append(completed_ns)
        signature = canonical_sha256(
            {
                "response_data_sha256": record.get("response_data_sha256"),
                "cardinality": nonnegative_int(
                    record.get("cardinality"), f"record {index} cardinality"
                ),
                "samples": nonnegative_int(record.get("samples"), f"record {index} samples"),
                "stats": stats,
            }
        )
        group = (generation, name)
        signatures.setdefault(group, set()).add(signature)
        counts[group] = counts.get(group, 0) + 1
        per_query[name] += 1
        if record["cardinality"] > 0:
            nonempty_per_query[name] += 1
        per_query_client_ns[name].append(record["client_elapsed_ns"])
        per_query_query_ns[name].append(record["query_duration_ns"])
    for group, observed in signatures.items():
        if len(observed) != 1:
            raise GateError(
                "same-generation result/cardinality/stats changed for "
                f"generation={group[0]} query={group[1]}"
            )
    ordered = sorted(generation_to_cut.items())
    for (previous_generation, previous_cut), (generation, cut) in zip(
        ordered, ordered[1:]
    ):
        if (
            generation <= previous_generation
            or cut[0] < previous_cut[0]
            or cut[1] < previous_cut[1]
        ):
            raise GateError("HTTP generation/message-cut mapping regressed")
    if len(ordered) < 2 or len({cut[0] for _generation, cut in ordered}) < 2:
        raise GateError("client must observe at least two generations and message cuts")
    minimum_per_query = workload["client"]["minimum_requests_per_query"]
    minimum_groups = workload["client"]["minimum_same_generation_groups_per_query"]
    repeated_groups: dict[str, int] = {}
    for name in sorted(expected_names):
        if per_query[name] < minimum_per_query:
            raise GateError(f"query {name} has too few successful responses")
        repeated_groups[name] = sum(
            1
            for (generation, query_name), count in counts.items()
            if query_name == name and generation > 0 and count >= 2
        )
        if repeated_groups[name] < minimum_groups:
            raise GateError(
                f"query {name} lacks same-generation repeated observations"
            )
        query = next(item for item in workload["queries"] if item["name"] == name)
        if query["require_nonempty"] and nonempty_per_query[name] == 0:
            raise GateError(f"designated query {name} never returned a non-empty result")
        if query["require_empty"] and nonempty_per_query[name] != 0:
            raise GateError(f"designated empty-control query {name} returned data")
    observation_span_ns = max(client_completed) - min(client_started)
    return {
        "schema": CLIENT_SCHEMA,
        "successful_requests": len(records),
        "generations": len(generation_to_cut),
        "message_cuts": len({cut[0] for _generation, cut in ordered}),
        "first_generation": ordered[0][0],
        "last_generation": ordered[-1][0],
        "first_visible_message_sequence": ordered[0][1][0],
        "last_visible_message_sequence": ordered[-1][1][0],
        "first_catalog_revision": ordered[0][1][1],
        "last_catalog_revision": ordered[-1][1][1],
        "requests_per_query": per_query,
        "nonempty_requests_per_query": nonempty_per_query,
        "same_generation_groups_per_query": repeated_groups,
        "durations": {
            field: distribution(values) for field, values in durations.items()
        },
        "per_query_latency": {
            name: {
                "client_elapsed_ns": {
                    "count": len(per_query_client_ns[name]),
                    "p50": distribution(per_query_client_ns[name])["p50"],
                    "p95": distribution(per_query_client_ns[name])["p95"],
                },
                "query_duration_ns": {
                    "count": len(per_query_query_ns[name]),
                    "p50": distribution(per_query_query_ns[name])["p50"],
                    "p95": distribution(per_query_query_ns[name])["p95"],
                },
            }
            for name in sorted(expected_names)
        },
        "query_stats_totals": query_stats_totals,
        "query_io_totals": query_io_totals,
        "closed_loop_observation_span_ns": observation_span_ns,
        "closed_loop_achieved_requests_per_second": (
            0.0
            if observation_span_ns == 0
            else len(records) * 1_000_000_000 / observation_span_ns
        ),
        "records_fingerprint_sha256": canonical_sha256(records),
    }


def _wait_ready(base_url: str, stop_file: Path, timeout_ms: int) -> None:
    deadline = time.monotonic() + timeout_ms / 1000
    url = f"{base_url.rstrip('/')}/-/ready"
    while time.monotonic() < deadline:
        if stop_file.exists():
            raise GateError("ingester stopped before the live API became ready")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.05)
    raise GateError("live API readiness timed out")


def run_client(
    base_url: str,
    workload_path: Path,
    records_path: Path,
    summary_path: Path,
    stop_file: Path,
) -> dict[str, Any]:
    workload = load_workload(workload_path)
    client = workload["client"]
    if records_path.exists() or summary_path.exists():
        raise GateError("refusing to reuse client output")
    _wait_ready(base_url, stop_file, client["readiness_timeout_ms"])
    records: list[dict[str, Any]] = []
    with records_path.open("x", encoding="utf-8") as destination:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=client["parallelism"]
        ) as executor:
            while not stop_file.exists():
                for query in workload["queries"]:
                    if stop_file.exists():
                        break
                    requester = lambda item: request_query(
                        base_url,
                        item,
                        client["request_timeout_ms"],
                        client["max_response_bytes"],
                    )
                    try:
                        batch = execute_parallel_batch(
                            query, client["parallelism"], requester, executor
                        )
                    except (
                        GateError,
                        OSError,
                        TimeoutError,
                        urllib.error.URLError,
                        json.JSONDecodeError,
                    ):
                        for _ in range(20):
                            if stop_file.exists():
                                break
                            time.sleep(0.05)
                        if stop_file.exists():
                            break
                        raise
                    for record in batch:
                        destination.write(
                            json.dumps(record, separators=(",", ":"), sort_keys=True)
                            + "\n"
                        )
                        records.append(record)
                    destination.flush()
                    if client["inter_batch_delay_ms"]:
                        time.sleep(client["inter_batch_delay_ms"] / 1000)
    summary = validate_client_records(records, workload)
    write_json_exclusive(summary_path, summary)
    return summary


def read_client_records(path: Path) -> list[dict[str, Any]]:
    records = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line:
            raise GateError(f"blank client record at line {line_number}")
        value = json.loads(line)
        if not isinstance(value, dict):
            raise GateError(f"client record line {line_number} is not an object")
        records.append(value)
    return records


def _log_uint(line: str, field: str) -> int:
    match = re.search(rf"(?:^|\s){re.escape(field)}=(\d+)(?:\s|$)", line)
    if match is None:
        raise GateError(f"live metric event is missing {field}")
    return int(match.group(1))


def _log_bool(line: str, field: str) -> bool:
    match = re.search(rf"(?:^|\s){re.escape(field)}=(true|false)(?:\s|$)", line)
    if match is None:
        raise GateError(f"live metric event is missing {field}")
    return match.group(1) == "true"


def _log_optional_uint(line: str, field: str) -> int | None:
    match = re.search(rf"(?:^|\s){re.escape(field)}=(\d+)(?:\s|$)", line)
    if match is not None:
        return int(match.group(1))
    if re.search(rf"(?:^|\s){re.escape(field)}=", line):
        raise GateError(f"live metric event has malformed {field}")
    return None


def _log_optional_bool(line: str, field: str) -> bool | None:
    match = re.search(rf"(?:^|\s){re.escape(field)}=(true|false)(?:\s|$)", line)
    if match is not None:
        return match.group(1) == "true"
    if re.search(rf"(?:^|\s){re.escape(field)}=", line):
        raise GateError(f"live metric event has malformed {field}")
    return None


def _log_publication_mode(line: str) -> str:
    match = re.search(r'(?:^|\s)mode="(boundary|shutdown)"(?:\s|$)', line)
    if match is None:
        raise GateError("live publication event has missing or malformed mode")
    return match.group(1)


def parse_live_log_text(text: str, expected_messages: int | None = None) -> dict[str, Any]:
    publications: list[dict[str, Any]] = []
    pauses: list[int] = []
    successful_message_boundaries = 0
    failed_message_boundaries = 0
    timing_fields = (
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
    maximum_fields = (
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
    timings = {field: [] for field in timing_fields}
    boundary_timings = {field: [] for field in timing_fields}
    maxima = {field: 0 for field in maximum_fields}
    for raw_line in text.splitlines():
        line = ANSI_ESCAPE.sub("", raw_line)
        if "chronoxide_live_metrics" not in line:
            continue
        if re.search(r'\bevent="publication"', line) and re.search(
            r'\boutcome="success"', line
        ):
            mode = _log_publication_mode(line)
            publication_timings = {
                field: _log_uint(line, field) for field in timing_fields
            }
            if (
                publication_timings["owner_validation_ns"]
                + publication_timings["head_validation_ns"]
                > publication_timings["owner_and_head_ns"]
            ):
                raise GateError(
                    "live publication owner/head substage durations exceed "
                    "their enclosing duration"
                )
            publication_scale = {
                field: _log_uint(line, field) for field in maximum_fields
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
                raise GateError(
                    "live publication event has an incomplete base-scale observation"
                )
            final_empty_fast_path = _log_optional_bool(
                line, "final_empty_fast_path"
            )
            if mode == "boundary" and final_empty_fast_path is True:
                raise GateError(
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
            for field in timing_fields:
                timings[field].append(publication_timings[field])
                if mode == "boundary":
                    boundary_timings[field].append(publication_timings[field])
            for field in maximum_fields:
                maxima[field] = max(maxima[field], publication_scale[field])
        if re.search(r'\bevent="message_boundary"', line):
            outcome = re.search(r'(?:^|\s)outcome="(success|failure)"(?:\s|$)', line)
            if outcome is None:
                raise GateError(
                    "live message-boundary event has missing or malformed outcome"
                )
            if outcome.group(1) == "success":
                successful_message_boundaries += 1
            else:
                failed_message_boundaries += 1
            pauses.append(_log_uint(line, "ingestion_pause_ns"))
    if not publications:
        raise GateError("live ingester log contains no successful publication event")
    if "Live view publication failed" in text:
        raise GateError("live ingester log contains a failed publication")
    shutdowns = [
        publication
        for publication in publications
        if publication["mode"] == "shutdown"
    ]
    if len(shutdowns) != 1:
        raise GateError(
            "live publication log must contain exactly one successful shutdown publication"
        )
    shutdown = shutdowns[0]
    if publications[-1] is not shutdown:
        raise GateError("shutdown publication is not the last observed publication")
    by_generation: dict[int, tuple[int, int]] = {}
    manifest_by_generation: dict[int, tuple[bool, int]] = {}
    for publication in publications:
        generation = publication["generation"]
        sequence = publication["visible_message_sequence"]
        catalog_revision = publication["catalog_revision"]
        if generation in by_generation:
            raise GateError(f"duplicate successful publication generation {generation}")
        by_generation[generation] = (sequence, catalog_revision)
        manifest_by_generation[generation] = (
            publication["manifest_present"],
            publication["manifest_validated_offset"],
        )
    ordered = sorted(by_generation.items())
    for (previous_generation, previous_cut), (generation, cut) in zip(
        ordered, ordered[1:]
    ):
        if (
            generation <= previous_generation
            or cut[0] < previous_cut[0]
            or cut[1] < previous_cut[1]
        ):
            raise GateError("publication log generation/message cut regressed")
    if shutdown["generation"] != ordered[-1][0]:
        raise GateError("shutdown publication is not the final successful generation")
    for field in ("sample_keys", "sample_fragments", "catalog_active_series"):
        if shutdown["scale"][field] != 0:
            raise GateError(
                f"shutdown publication retained non-empty final {field}"
            )
    if expected_messages is not None and ordered[-1][1][0] != expected_messages:
        raise GateError(
            "final successful publication message cut does not equal the replay limit"
        )
    publication_duration = shutdown["timings_ns"]["publication_duration_ns"]
    seal_duration = shutdown["timings_ns"]["seal_ns"]
    if seal_duration > publication_duration:
        raise GateError("shutdown seal duration exceeds total publication duration")
    pre_cleanup = sum(
        shutdown["timings_ns"][field]
        for field in ("freeze_and_admission_ns", "seal_ns", "inventory_ns")
    )
    if pre_cleanup > publication_duration:
        raise GateError(
            "shutdown freeze, seal, and inventory durations exceed total publication duration"
        )
    shutdown_timings = dict(shutdown["timings_ns"])
    shutdown_timings["post_seal_ns"] = publication_duration - seal_duration
    shutdown_timings["after_inventory_ns"] = publication_duration - pre_cleanup
    return {
        "successful_publications": len(publications),
        "boundary_publications": len(publications) - 1,
        "message_boundary_observations": len(pauses),
        "successful_message_boundary_observations": successful_message_boundaries,
        "failed_message_boundary_observations": failed_message_boundaries,
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
        "boundary_publication_timings_ns": {
            field: distribution(values)
            for field, values in boundary_timings.items()
        },
        "shutdown_publication": {
            "generation": shutdown["generation"],
            "visible_message_sequence": shutdown["visible_message_sequence"],
            "catalog_revision": shutdown["catalog_revision"],
            "manifest_present": shutdown["manifest_present"],
            "manifest_validated_offset": shutdown["manifest_validated_offset"],
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
        "publication_maxima": maxima,
        "generation_message_sequence": [
            {
                "generation": generation,
                "visible_message_sequence": cut[0],
                "catalog_revision": cut[1],
                "manifest_present": manifest_by_generation[generation][0],
                "manifest_validated_offset": manifest_by_generation[generation][1],
            }
            for generation, cut in ordered
        ],
        "mapping_sha256": canonical_sha256(ordered),
    }


def validate_replay_document(value: Any, expected_messages: int) -> dict[str, Any]:
    if not isinstance(value, dict) or not str(value.get("schema", "")).startswith(
        "chronoxide/storage-vnext-replay-correctness/v"
    ):
        raise GateError("replay correctness document has an unsupported schema")
    general = value.get("general")
    policy = value.get("datapoint_policy_totals")
    storage = value.get("datapoint_storage_totals")
    watermarks = value.get("partition_watermarks")
    types = value.get("otlp_data_type_counts")
    if not all(isinstance(item, dict) for item in (general, policy, storage, watermarks, types)):
        raise GateError("replay correctness document is incomplete")
    messages = positive_int(general.get("Total Messages"), "Total Messages")
    if messages != expected_messages:
        raise GateError(
            f"replay processed {messages} messages, expected exactly {expected_messages}"
        )
    observed = nonnegative_int(policy.get("Observed"), "policy observed")
    accepted = nonnegative_int(
        policy.get("Time-Policy Accepted"), "policy accepted"
    )
    rejected = sum(
        nonnegative_int(policy.get(key, 0), f"policy {key}")
        for key in ("Dropped Too Old", "Dropped Too Future", "Missing Timestamp")
    )
    if observed != accepted + rejected:
        raise GateError("observed datapoints do not reconcile with policy outcomes")
    recorded = positive_int(storage.get("Recorded Samples"), "recorded samples")
    not_recorded = nonnegative_int(
        storage.get("Accepted Not Recorded"), "accepted not recorded"
    )
    missing = nonnegative_int(
        storage.get("Missing Number Value", 0), "missing number value"
    )
    invalid = nonnegative_int(
        storage.get("Invalid Typed Value", 0), "invalid typed value"
    )
    if accepted != recorded + not_recorded or not_recorded != missing + invalid:
        raise GateError("accepted datapoints do not reconcile with storage outcomes")
    if general.get("Recorded Samples") != recorded:
        raise GateError("general and storage recorded-sample counts disagree")
    if watermarks.get("Tracked Messages") != messages:
        raise GateError("tracked message count differs from total messages")
    if sum(row["observed_datapoints"] for row in types.values()) != observed:
        raise GateError("per-type observed datapoints do not reconcile")
    if sum(row["accepted_datapoints"] for row in types.values()) != accepted:
        raise GateError("per-type accepted datapoints do not reconcile")
    return value


def validate_readbacks(report: Path) -> dict[str, Any]:
    text = report.read_text(encoding="utf-8")
    verification = phase1._two_column_values(text, "Readback Verification")
    diagnostics = phase1._two_column_values(text, "Query Diagnostics")
    expected = phase1._required_integer(diagnostics, "Expected Readback Queries")
    executed = phase1._required_integer(diagnostics, "Executed Readback Queries")
    skipped = phase1._required_integer(diagnostics, "Skipped Readback Queries")
    isolation = phase1._required_integer(diagnostics, "Isolation Check Skips")
    checked = phase1._required_integer(verification, "Checked Queries")
    mismatches = phase1._required_integer(verification, "Mismatches")
    if expected == 0 or executed != expected or checked != executed:
        raise GateError("readback execution coverage is incomplete")
    if skipped or isolation or mismatches:
        raise GateError("readback verification contains a skip or mismatch")
    return {
        "expected_queries": expected,
        "executed_queries": executed,
        "skipped_queries": skipped,
        "isolation_check_skips": isolation,
        "mismatches": mismatches,
    }


def parse_head_window_writes(log: Path) -> dict[str, int]:
    rows = []
    for line in log.read_text(encoding="utf-8").splitlines():
        if "Head window written " not in line:
            continue
        row = {
            field: _log_uint(line, field)
            for field in (
                "start_ms",
                "end_ms",
                "datapoints",
                "series",
                "record_chunks",
                "record_profile_samples",
                "dropped_histogram_series",
                "dropped_exponential_histogram_series",
                "dropped_summary_series",
            )
        }
        if row["end_ms"] <= row["start_ms"]:
            raise GateError("D head-window log contains an invalid event-time range")
        rows.append(row)
    if not rows:
        raise GateError("D ingester log contains no completed head-window write")
    dropped_fields = (
        "dropped_histogram_series",
        "dropped_exponential_histogram_series",
        "dropped_summary_series",
    )
    for field in dropped_fields:
        if sum(row[field] for row in rows) != 0:
            raise GateError(f"D head-window writer reported nonzero {field}")
    return {
        "segments": len(rows),
        "recorded_head_writes": sum(row["datapoints"] for row in rows),
        "series_rows": sum(row["series"] for row in rows),
        "chunks": sum(row["record_chunks"] for row in rows),
        "physical_samples": sum(row["record_profile_samples"] for row in rows),
        "dropped_typed_series": sum(
            row[field] for row in rows for field in dropped_fields
        ),
    }


def validate_storage_verifier(
    report: Path,
    replay_correctness: Path,
    ingester_log: Path,
    *,
    require_writer_reconciliation: bool = True,
) -> dict[str, Any]:
    value = load_json(report)
    replay_value = load_json(replay_correctness)
    replay = validate_replay_document(
        replay_value, replay_value["general"]["Total Messages"]
    )
    writer = (
        parse_head_window_writes(ingester_log)
        if require_writer_reconciliation
        else None
    )
    if not isinstance(value, dict) or value.get("schema_version") != 8:
        raise GateError("storage verifier did not validate Schema 8")
    if value.get("footer_validation_enabled") is not True:
        raise GateError("storage footer validation was not enabled")
    if value.get("series_sample_per_segment") is not None:
        raise GateError("storage verifier used a sampled rather than exhaustive selection")
    segments = positive_int(value.get("segments"), "verifier segments")
    corpus_series = positive_int(value.get("corpus_series"), "verifier corpus series")
    selected_series = positive_int(value.get("series"), "verifier selected series")
    chunks = positive_int(value.get("chunks"), "verifier chunks")
    samples = positive_int(value.get("samples"), "verifier samples")
    recorded_head_writes = replay["general"]["Recorded Samples"]
    if samples > recorded_head_writes:
        raise GateError("verified physical sample count exceeds recorded head writes")
    if selected_series != corpus_series:
        raise GateError("exhaustive verifier did not select every corpus series")
    if writer is not None:
        if writer["recorded_head_writes"] != recorded_head_writes:
            raise GateError("D head-window inputs differ from replay recorded head writes")
        if writer["segments"] != segments:
            raise GateError("D head-window writes differ from verified segment count")
        if writer["series_rows"] != corpus_series:
            raise GateError("D writer and exhaustive verifier series counts differ")
        if writer["chunks"] != chunks:
            raise GateError("D writer and verifier chunk counts differ")
        if writer["physical_samples"] != samples:
            raise GateError("D writer and verifier physical sample counts differ")
    chunks_by_kind = value.get("chunks_by_kind")
    if (
        not isinstance(chunks_by_kind, list)
        or len(chunks_by_kind) != 5
        or any(not isinstance(item, int) or item < 0 for item in chunks_by_kind)
        or sum(chunks_by_kind) != chunks
    ):
        raise GateError("verifier chunks-by-kind do not reconcile")
    fingerprint = value.get("verified_selection_fingerprint")
    if not isinstance(fingerprint, str) or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None:
        raise GateError("storage verifier selection fingerprint is missing or malformed")
    semantic_fingerprint = value.get("decoded_semantic_fingerprint")
    if (
        not isinstance(semantic_fingerprint, str)
        or re.fullmatch(r"[0-9a-f]{64}", semantic_fingerprint) is None
    ):
        raise GateError("storage verifier semantic fingerprint is missing or malformed")
    exact = value.get("exact_postings")
    exact_fingerprint = exact.get("logical_fingerprint") if isinstance(exact, dict) else None
    if (
        not isinstance(exact_fingerprint, str)
        or re.fullmatch(r"[0-9a-f]{64}", exact_fingerprint) is None
    ):
        raise GateError("storage verifier did not produce exact-postings evidence")
    for field in ("lists", "decoded_refs", "encoded_bytes"):
        positive_int(exact.get(field), f"exact postings {field}")
    return {
        "schema_version": 8,
        "segments": segments,
        "series": selected_series,
        "chunks": chunks,
        "samples": samples,
        "recorded_head_writes": recorded_head_writes,
        "recorded_writes_minus_physical_rows": recorded_head_writes - samples,
        "writer_to_verifier_counts_reconciled": writer is not None,
        "capture_level_physical_sample_golden_gated": False,
        "verified_selection_fingerprint": fingerprint,
        "decoded_semantic_fingerprint": semantic_fingerprint,
        "exact_postings_fingerprint": exact_fingerprint,
    }


def _read_zero_status(path: Path, context: str) -> None:
    value = path.read_text(encoding="ascii").strip()
    if value != "0":
        raise GateError(f"{context} exit status is not zero: {value!r}")


def _require_regular_file(path: Path, context: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise GateError(f"{context} is missing: {path}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise GateError(f"{context} must be a regular non-symlink file: {path}")


def _require_directory(path: Path, context: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise GateError(f"{context} is missing: {path}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise GateError(f"{context} must be a non-symlink directory: {path}")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _loaded_source_authority(
    value: Any,
    *,
    field: str,
    actual_path: Path,
) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"path", "sha256"}:
        raise GateError(f"publication-scale bootstrap {field} authority is malformed")
    path_value = value.get("path")
    digest = value.get("sha256")
    if not isinstance(path_value, str) or not isinstance(digest, str):
        raise GateError(f"publication-scale bootstrap {field} authority is malformed")
    path = Path(path_value)
    if not path.is_absolute() or path.resolve() != actual_path:
        raise GateError(
            f"publication-scale bootstrap {field} path differs from execution"
        )
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise GateError(
            f"publication-scale bootstrap {field} digest is malformed"
        )
    _require_regular_file(path, f"publication-scale bootstrap {field}")
    if _file_sha256(path) != digest:
        raise GateError(
            f"publication-scale bootstrap {field} changed after it was loaded"
        )
    return {"path": str(path), "sha256": digest}


def _publication_scale_validator_provenance(
    root: Path,
    *,
    test_only_allow_unisolated: bool,
) -> dict[str, Any]:
    entrypoint = Path(__file__).resolve()
    imported_phase1 = Path(phase1.__file__).resolve()
    python_executable = Path(sys.executable).resolve()
    bootstrap = _CHRONOXIDE_SCALE_VALIDATOR_BOOTSTRAP
    if bootstrap is None and test_only_allow_unisolated:
        bootstrap_path = (
            entrypoint.parent / "live_query_scale_validator_bootstrap.py"
        ).resolve()
        bootstrap = {
            "schema": PUBLICATION_SCALE_BOOTSTRAP_SCHEMA,
            "bootstrap": {
                "path": str(bootstrap_path),
                "sha256": _file_sha256(bootstrap_path),
            },
            "entrypoint": {
                "path": str(entrypoint),
                "sha256": _file_sha256(entrypoint),
            },
            "phase1": {
                "path": str(imported_phase1),
                "sha256": _file_sha256(imported_phase1),
            },
        }
    if not isinstance(bootstrap, dict) or set(bootstrap) != {
        "schema",
        "bootstrap",
        "entrypoint",
        "phase1",
    }:
        raise GateError(
            "publication-scale v2 requires the exact isolated validator bootstrap"
        )
    if bootstrap.get("schema") != PUBLICATION_SCALE_BOOTSTRAP_SCHEMA:
        raise GateError("publication-scale bootstrap schema is unsupported")
    if not test_only_allow_unisolated and not (
        sys.flags.isolated
        and sys.flags.no_site
        and sys.flags.dont_write_bytecode
    ):
        raise GateError("publication-scale v2 requires Python -I -S -B")
    bootstrap_path_value = bootstrap["bootstrap"]
    if not isinstance(bootstrap_path_value, dict):
        raise GateError("publication-scale bootstrap authority is malformed")
    bootstrap_path_text = bootstrap_path_value.get("path")
    if not isinstance(bootstrap_path_text, str):
        raise GateError("publication-scale bootstrap path is malformed")
    bootstrap_path = Path(bootstrap_path_text).resolve()
    expected_bundle_paths = {
        "bootstrap": bootstrap_path.parent
        / "live_query_scale_validator_bootstrap.py",
        "entrypoint": bootstrap_path.parent / "live_query_ingest_ab_gate.py",
        "phase1": bootstrap_path.parent / "phase1_replay_gate.py",
    }
    if (
        bootstrap_path != expected_bundle_paths["bootstrap"]
        or entrypoint != expected_bundle_paths["entrypoint"]
        or imported_phase1 != expected_bundle_paths["phase1"]
    ):
        raise GateError(
            "publication-scale validator sources do not form the exact bundle"
        )
    loaded_sources = {
        "bootstrap": _loaded_source_authority(
            bootstrap["bootstrap"],
            field="bootstrap",
            actual_path=bootstrap_path,
        ),
        "entrypoint": _loaded_source_authority(
            bootstrap["entrypoint"],
            field="entrypoint",
            actual_path=entrypoint,
        ),
        "phase1": _loaded_source_authority(
            bootstrap["phase1"],
            field="phase1",
            actual_path=imported_phase1,
        ),
    }
    producer_harness = root / "metadata" / "harness"
    producer_gate = producer_harness / "live_query_ingest_ab_gate.py"
    producer_phase1 = producer_harness / "phase1_replay_gate.py"
    producer_runner = producer_harness / "live_query_ingest_ab_run.sh"
    for path, context in (
        (python_executable, "publication-scale Python executable"),
        (producer_gate, "publication-scale producer-frozen gate"),
        (producer_phase1, "publication-scale producer-frozen Phase 1 gate"),
        (producer_runner, "publication-scale producer-frozen runner"),
    ):
        _require_regular_file(path, context)
    return {
        "validation_kind": (
            "unit-test-unisolated"
            if test_only_allow_unisolated
            else "post-hoc-sealed-root-validation"
        ),
        "authoritative": not test_only_allow_unisolated,
        "contract_amendment": SCALE_READBACK_CONTRACT_AMENDMENT,
        "loaded_sources": loaded_sources,
        "entrypoint": str(entrypoint),
        "entrypoint_sha256": loaded_sources["entrypoint"]["sha256"],
        "imported_phase1": str(imported_phase1),
        "imported_phase1_sha256": loaded_sources["phase1"]["sha256"],
        "bootstrap": str(bootstrap_path),
        "bootstrap_sha256": loaded_sources["bootstrap"]["sha256"],
        "producer_frozen_entrypoint_sha256": _file_sha256(producer_gate),
        "producer_frozen_phase1_sha256": _file_sha256(producer_phase1),
        "producer_frozen_runner_sha256": _file_sha256(producer_runner),
        "python": {
            "executable": str(python_executable),
            "executable_sha256": _file_sha256(python_executable),
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
            "isolated": bool(sys.flags.isolated),
            "no_site": bool(sys.flags.no_site),
            "dont_write_bytecode": bool(sys.flags.dont_write_bytecode),
        },
    }


def _binary_hashes(root: Path, label: str) -> dict[str, str]:
    manifest = root / "metadata" / "binaries.sha256"
    _require_regular_file(manifest, f"{label} binary manifest")
    required_roles = {
        "chronoxide-ingester",
        "chronoxide-query",
        "chronoxide-storage-verify",
    }
    allowed_roles = {*required_roles, "chronoxide-api"}
    result: dict[str, str] = {}
    for line_number, line in enumerate(
        manifest.read_text(encoding="utf-8").splitlines(), 1
    ):
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if match is None:
            raise GateError(
                f"{label} binary manifest line {line_number} is malformed"
            )
        role = Path(match.group(2)).name
        if role not in allowed_roles:
            raise GateError(f"{label} binary manifest has unexpected role {role!r}")
        if role in result:
            raise GateError(f"{label} binary manifest repeats role {role!r}")
        result[role] = match.group(1)
    if not required_roles.issubset(result):
        missing = sorted(required_roles - set(result))
        raise GateError(f"{label} binary manifest is missing roles: {missing}")
    for role in result:
        binary = root / "metadata" / "binaries" / role
        _require_regular_file(binary, f"{label} frozen {role} binary")
        if _file_sha256(binary) != result[role]:
            raise GateError(
                f"{label} frozen {role} hash differs from its binary manifest"
            )
    return result


def _sha256_field(value: Any, context: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise GateError(f"{context} is missing or malformed")
    return value


def _validate_segments_manifest(
    run: Path, label: str
) -> tuple[bytes, int, int]:
    manifest = run / "segments.sha256"
    _require_regular_file(manifest, f"{label} segment manifest")
    data = manifest.read_bytes()
    try:
        lines = data.decode("ascii").splitlines()
    except UnicodeDecodeError as error:
        raise GateError(f"{label} segment manifest is not ASCII") from error
    if not lines:
        raise GateError(f"{label} segment manifest is empty")

    segments = run / "segments"
    _require_directory(segments, f"{label} segment tree")
    expected: dict[str, str] = {}
    previous_path: str | None = None
    for line_number, line in enumerate(lines, 1):
        match = re.fullmatch(r"([0-9a-f]{64})  \./(.+)", line)
        if match is None:
            raise GateError(
                f"{label} segment manifest line {line_number} is malformed"
            )
        relative = match.group(2)
        parts = relative.split("/")
        if any(part in ("", ".", "..") for part in parts):
            raise GateError(
                f"{label} segment manifest line {line_number} has an unsafe path"
            )
        if previous_path is not None and relative <= previous_path:
            raise GateError(f"{label} segment manifest paths are not ordered")
        previous_path = relative
        expected[relative] = match.group(1)

    actual: set[str] = set()
    for directory, names, files in os.walk(segments, followlinks=False):
        directory_path = Path(directory)
        for name in names:
            path = directory_path / name
            if path.is_symlink():
                raise GateError(f"{label} segment tree contains a directory symlink")
        for name in files:
            path = directory_path / name
            _require_regular_file(path, f"{label} segment file")
            relative = path.relative_to(segments).as_posix()
            actual.add(relative)
    if actual != set(expected):
        raise GateError(f"{label} segment manifest does not cover the exact file tree")
    for relative, digest in expected.items():
        if _file_sha256(segments.joinpath(*relative.split("/"))) != digest:
            raise GateError(f"{label} segment file hash differs from its manifest")
    size_bytes = sum(
        segments.joinpath(*relative.split("/")).stat().st_size
        for relative in expected
    )
    return data, len(expected), size_bytes


def _load_shutdown_ab_arm(
    root: Path, label: str, expected_messages: int | None
) -> dict[str, Any]:
    _require_directory(root, f"{label} result root")
    _require_regular_file(root / "COMPLETE", f"{label} completion marker")
    run = root / "runs" / "P"
    _require_directory(run, f"{label} P run")
    for absent_variant in ("D", "Q"):
        path = root / "runs" / absent_variant
        if path.exists() or path.is_symlink():
            raise GateError(f"{label} unexpectedly contains a {absent_variant} run")

    evidence_files = (
        "ingester.exit-status",
        "rss-monitor.exit-status",
        "ingester.log",
        "live-log-summary.json",
        "replay-correctness.json",
        "segments.sha256",
        "corpus-summary.json",
        "rss-summary.json",
    )
    for name in evidence_files:
        _require_regular_file(run / name, f"{label} {name}")
    _require_regular_file(
        root / "validation" / "storage-verify-gate.json",
        f"{label} storage verification gate",
    )
    _require_regular_file(
        root / "validation" / "readbacks-gate.json",
        f"{label} readback gate",
    )

    _read_zero_status(run / "ingester.exit-status", f"{label} ingester")
    _read_zero_status(run / "rss-monitor.exit-status", f"{label} RSS monitor")
    replay = load_json(run / "replay-correctness.json")
    if expected_messages is None:
        general = replay.get("general") if isinstance(replay, dict) else None
        if not isinstance(general, dict):
            raise GateError(f"{label} replay correctness document is incomplete")
        expected_messages = positive_int(
            general.get("Total Messages"), f"{label} Total Messages"
        )
    replay = validate_replay_document(replay, expected_messages)

    observed_live_log = parse_live_log_text(
        (run / "ingester.log").read_text(encoding="utf-8"),
        expected_messages,
    )
    recorded_live_log = load_json(run / "live-log-summary.json")
    comparable_live_log = dict(observed_live_log)
    derived_boundary_fields = (
        "successful_message_boundary_observations",
        "failed_message_boundary_observations",
    )
    recorded_derived = [
        field in recorded_live_log
        for field in derived_boundary_fields
    ] if isinstance(recorded_live_log, dict) else []
    if any(recorded_derived) and not all(recorded_derived):
        raise GateError(f"{label} live-log summary has partial boundary outcomes")
    if all(recorded_derived):
        pass
    else:
        for field in derived_boundary_fields:
            comparable_live_log.pop(field)
    if comparable_live_log != recorded_live_log:
        raise GateError(f"{label} live-log summary differs from raw log")

    (
        segments_manifest,
        segment_file_count,
        segment_size_bytes,
    ) = _validate_segments_manifest(run, label)

    corpus = load_json(run / "corpus-summary.json")
    if (
        not isinstance(corpus, dict)
        or corpus.get("schema") != phase1.CORPUS_SUMMARY_SCHEMA
    ):
        raise GateError(f"{label} corpus manifest is not an object")
    corpus_file_count = positive_int(
        corpus.get("file_count"), f"{label} corpus file count"
    )
    if corpus_file_count != segment_file_count:
        raise GateError(
            f"{label} corpus file count differs from the segment manifest"
        )
    corpus_size_bytes = positive_int(
        corpus.get("size_bytes"), f"{label} corpus size"
    )
    if corpus_size_bytes != segment_size_bytes:
        raise GateError(f"{label} corpus size differs from the segment tree")
    corpus_fingerprint = _sha256_field(
        corpus.get("manifest_sha256"), f"{label} corpus manifest fingerprint"
    )
    observed_corpus_fingerprint = hashlib.sha256(segments_manifest).hexdigest()
    if corpus_fingerprint != observed_corpus_fingerprint:
        raise GateError(
            f"{label} corpus fingerprint differs from the segment manifest"
        )

    storage = load_json(root / "validation" / "storage-verify-gate.json")
    if not isinstance(storage, dict) or storage.get("schema_version") != 8:
        raise GateError(f"{label} storage verification did not validate Schema 8")
    for field in ("segments", "series", "chunks", "samples"):
        positive_int(storage.get(field), f"{label} verified {field}")
    storage_fingerprints = {
        field: _sha256_field(storage.get(field), f"{label} storage {field}")
        for field in (
            "verified_selection_fingerprint",
            "decoded_semantic_fingerprint",
            "exact_postings_fingerprint",
        )
    }

    readbacks = load_json(root / "validation" / "readbacks-gate.json")
    if not isinstance(readbacks, dict):
        raise GateError(f"{label} readback gate is not an object")
    expected_queries = positive_int(
        readbacks.get("expected_queries"), f"{label} expected readback queries"
    )
    executed_queries = nonnegative_int(
        readbacks.get("executed_queries"), f"{label} executed readback queries"
    )
    skipped_queries = nonnegative_int(
        readbacks.get("skipped_queries"), f"{label} skipped readback queries"
    )
    isolation_skips = nonnegative_int(
        readbacks.get("isolation_check_skips"),
        f"{label} readback isolation-check skips",
    )
    mismatches = nonnegative_int(
        readbacks.get("mismatches"), f"{label} readback mismatches"
    )
    if (
        executed_queries != expected_queries
        or skipped_queries != 0
        or isolation_skips != 0
        or mismatches != 0
    ):
        raise GateError(f"{label} readback verification is incomplete or incorrect")

    rss = load_json(run / "rss-summary.json")
    if not isinstance(rss, dict):
        raise GateError(f"{label} RSS summary is not an object")
    positive_int(rss.get("samples"), f"{label} RSS samples")
    if nonnegative_int(
        rss.get("aggregate_vm_swap_kib"), f"{label} process-tree swap"
    ) != 0:
        raise GateError(f"{label} measured process tree used swap")
    peak_rss_kib = positive_int(
        rss.get("aggregate_rss_kib"), f"{label} peak process-tree RSS"
    )

    shutdown = observed_live_log["shutdown_publication"]
    shutdown_timings = shutdown["timings_ns"]
    boundary_duration = observed_live_log["boundary_publication_timings_ns"][
        "publication_duration_ns"
    ]
    positive_int(
        boundary_duration.get("count"), f"{label} boundary publication count"
    )
    metrics = {
        "shutdown_publication_ns": positive_int(
            shutdown_timings.get("publication_duration_ns"),
            f"{label} shutdown publication duration",
        ),
        "shutdown_post_seal_ns": positive_int(
            shutdown_timings.get("post_seal_ns"),
            f"{label} shutdown post-seal duration",
        ),
        "shutdown_sample_catalog_ns": positive_int(
            nonnegative_int(
                shutdown_timings.get("sample_root_ns"),
                f"{label} shutdown sample-root duration",
            )
            + nonnegative_int(
                shutdown_timings.get("catalog_ns"),
                f"{label} shutdown catalog duration",
            ),
            f"{label} shutdown sample-root plus catalog duration",
        ),
        "boundary_p95_ns": positive_int(
            boundary_duration.get("p95"), f"{label} boundary publication p95"
        ),
        "boundary_p50_ns": positive_int(
            boundary_duration.get("p50"), f"{label} boundary publication p50"
        ),
        "boundary_max_ns": positive_int(
            boundary_duration.get("max"), f"{label} boundary publication maximum"
        ),
        "shutdown_seal_ns": nonnegative_int(
            shutdown_timings.get("seal_ns"), f"{label} shutdown seal duration"
        ),
        "shutdown_sample_root_ns": nonnegative_int(
            shutdown_timings.get("sample_root_ns"),
            f"{label} shutdown sample-root duration",
        ),
        "shutdown_catalog_ns": nonnegative_int(
            shutdown_timings.get("catalog_ns"),
            f"{label} shutdown catalog duration",
        ),
        "shutdown_post_commit_ns": nonnegative_int(
            shutdown_timings.get("post_commit_ns"),
            f"{label} shutdown post-commit duration",
        ),
        "peak_rss_kib": peak_rss_kib,
    }
    return {
        "root": str(root.resolve()),
        "expected_messages": expected_messages,
        "binary_hashes": _binary_hashes(root, label),
        "replay": replay,
        "segments_manifest": segments_manifest,
        "corpus": corpus,
        "corpus_fingerprint": corpus_fingerprint,
        "storage_fingerprints": storage_fingerprints,
        "readbacks": {
            "expected_queries": expected_queries,
            "executed_queries": executed_queries,
            "skipped_queries": skipped_queries,
            "isolation_check_skips": isolation_skips,
            "mismatches": mismatches,
        },
        "final_empty_fast_path": shutdown["final_empty_fast_path"],
        "base_scale": shutdown["base_scale"],
        "metrics": metrics,
    }


def _mean(values: list[int]) -> int | float:
    total = sum(values)
    quotient, remainder = divmod(total, len(values))
    return quotient if remainder == 0 else total / len(values)


def _validate_result_artifacts(
    root: Path, label: str, expected_messages: int
) -> dict[str, Any]:
    complete = root / "COMPLETE"
    _require_regular_file(complete, f"{label} completion marker")
    complete_size = complete.stat().st_size
    complete_sha256 = _file_sha256(complete)
    if complete_size != 0 or complete_sha256 != EMPTY_SHA256:
        raise GateError(
            f"{label} completion marker is not the empty file created by the runner"
        )
    manifest = root / "metadata" / "result-artifacts.sha256"
    _require_regular_file(manifest, f"{label} result-artifact manifest")
    root_artifacts = {"run-plan.tsv", "run-summary.tsv"}
    expected: dict[str, str] = {}
    previous: str | None = None
    for line_number, line in enumerate(
        manifest.read_text(encoding="ascii").splitlines(), 1
    ):
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if match is None:
            raise GateError(
                f"{label} result-artifact manifest line {line_number} is malformed"
            )
        relative = match.group(2)
        parts = relative.split("/")
        allowed_root_artifact = relative in root_artifacts
        if (
            (
                not allowed_root_artifact
                and parts[0]
                not in {"configs", "metadata", "validation", "comparisons", "runs"}
            )
            or any(part in ("", ".", "..") for part in parts)
            or relative == "metadata/result-artifacts.sha256"
            or (len(parts) >= 4 and parts[:3] == ["runs", "P", "segments"])
        ):
            raise GateError(
                f"{label} result-artifact manifest has unsafe path {relative!r}"
            )
        if previous is not None and relative <= previous:
            raise GateError(f"{label} result-artifact paths are not ordered")
        previous = relative
        expected[relative] = match.group(1)
    if not expected:
        raise GateError(f"{label} result-artifact manifest is empty")
    bound_root_artifacts = root_artifacts & set(expected)
    if expected_messages == 250_000 and bound_root_artifacts != root_artifacts:
        raise GateError(
            f"{label} mandatory scale manifest does not bind run-plan.tsv "
            "and run-summary.tsv"
        )

    actual: set[str] = set()
    for top in ("configs", "metadata", "validation", "comparisons", "runs"):
        top_path = root / top
        _require_directory(top_path, f"{label} {top} directory")
        for directory, names, files in os.walk(top_path, followlinks=False):
            directory_path = Path(directory)
            relative_directory = directory_path.relative_to(root)
            if relative_directory.parts[:3] == ("runs", "P", "segments"):
                names[:] = []
                continue
            for name in names:
                if (directory_path / name).is_symlink():
                    raise GateError(
                        f"{label} result-artifact tree contains a directory symlink"
                    )
            for name in files:
                path = directory_path / name
                relative = path.relative_to(root).as_posix()
                if relative == "metadata/result-artifacts.sha256":
                    continue
                _require_regular_file(path, f"{label} result artifact")
                actual.add(relative)
    for relative in bound_root_artifacts:
        path = root / relative
        _require_regular_file(path, f"{label} result artifact")
        actual.add(relative)
    if actual != set(expected):
        missing = sorted(set(expected) - actual)
        extra = sorted(actual - set(expected))
        raise GateError(
            f"{label} result-artifact manifest file set differs; "
            f"missing={missing!r} extra={extra!r}"
        )
    for relative, digest in expected.items():
        if _file_sha256(root.joinpath(*relative.split("/"))) != digest:
            raise GateError(
                f"{label} result artifact {relative!r} differs from its manifest"
            )
    return {
        "files": len(expected),
        "manifest_sha256": _file_sha256(manifest),
        "root_summaries_bound": bound_root_artifacts == root_artifacts,
        "complete_marker": {
            "size_bytes": complete_size,
            "sha256": complete_sha256,
        },
    }


def _settings(path: Path) -> dict[str, str]:
    _require_regular_file(path, "scale settings")
    result: dict[str, str] = {}
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if "=" not in line:
            raise GateError(f"scale settings line {line_number} is malformed")
        key, value = line.split("=", 1)
        if re.fullmatch(r"[a-z][a-z0-9_]*", key) is None or key in result:
            raise GateError(f"scale settings line {line_number} has an invalid key")
        result[key] = value
    return result


def _settings_int(settings: dict[str, str], key: str) -> int:
    value = settings.get(key)
    if value is None or re.fullmatch(r"[0-9]+", value) is None:
        raise GateError(f"scale setting {key} is missing or malformed")
    return int(value)


def _settings_float(settings: dict[str, str], key: str) -> float:
    value = settings.get(key)
    if value is None or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value) is None:
        raise GateError(f"scale setting {key} is missing or malformed")
    return float(value)


def _scale_process_conflicts(path: Path) -> list[dict[str, Any]]:
    _require_regular_file(path, f"scale process snapshot {path.name}")
    conflicts = []
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise GateError(f"scale process snapshot {path.name} is empty")
    for line_number, line in enumerate(lines, 1):
        fields = line.split(maxsplit=6)
        if len(fields) < 6:
            raise GateError(
                f"scale process snapshot {path.name} line "
                f"{line_number} is malformed"
            )
        pid, command = fields[0], fields[5]
        if re.fullmatch(r"[0-9]+", pid) is None:
            raise GateError(
                f"scale process snapshot {path.name} has malformed PID"
            )
        record = {
            "comm": command,
            "argv0": (
                fields[6].split(maxsplit=1)[0] if len(fields) == 7 else None
            ),
        }
        if _is_conflicting_scale_process(record):
            conflicts.append({"pid": int(pid), "command": command})
    return conflicts


def _pressure_avg10(line: str, context: str) -> float:
    match = re.search(r"(?:^|\s)avg10=([0-9]+(?:\.[0-9]+)?)(?:\s|$)", line)
    if match is None:
        raise GateError(f"scale pressure snapshot lacks {context} avg10")
    return float(match.group(1))


def _parse_pressure_snapshot(path: Path) -> dict[str, float]:
    _require_regular_file(path, f"scale pressure snapshot {path.name}")
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2:
        raise GateError(f"scale pressure snapshot {path.name} is incomplete")
    load_fields = lines[1].split()
    if not load_fields or re.fullmatch(
        r"[0-9]+(?:\.[0-9]+)?", load_fields[0]
    ) is None:
        raise GateError(f"scale pressure snapshot {path.name} has invalid load")
    sections: dict[str, dict[str, str]] = {}
    index = 2
    while index < len(lines):
        header = lines[index]
        if header.startswith("/proc/pressure/"):
            kind = header.rsplit("/", 1)[-1]
            rows: dict[str, str] = {}
            index += 1
            while index < len(lines) and not lines[index].startswith("/proc/"):
                row = lines[index]
                if row:
                    rows[row.split(maxsplit=1)[0]] = row
                index += 1
            sections[kind] = rows
            continue
        index += 1
    try:
        cpu = sections["cpu"]["some"]
        io = sections["io"]["full"]
        memory = sections["memory"]["full"]
    except KeyError as error:
        raise GateError(
            f"scale pressure snapshot {path.name} lacks required PSI rows"
        ) from error
    return {
        "load_one": float(load_fields[0]),
        "cpu_psi_avg10": _pressure_avg10(cpu, "CPU some"),
        "io_psi_avg10": _pressure_avg10(io, "I/O full"),
        "memory_psi_avg10": _pressure_avg10(memory, "memory full"),
    }


def _validate_quiet_scale_host(
    root: Path,
    settings: dict[str, str],
    allowed_cpu_count: int,
) -> dict[str, Any]:
    if settings.get("allow_noisy_host") != "0":
        raise GateError("mandatory 250k scale run allowed a noisy host")
    run = root / "runs" / "P"
    conflicts = {
        name: _scale_process_conflicts(run / name)
        for name in ("processes-before.txt", "processes-after.txt")
    }
    if any(conflicts.values()):
        raise GateError(
            f"mandatory 250k scale run overlaps conflicting processes: "
            f"{conflicts!r}"
        )
    pressure = {
        phase: _parse_pressure_snapshot(run / f"pressure-{phase}.txt")
        for phase in ("before", "after")
    }
    configured = {
        key: _settings_float(settings, key)
        for key in SCALE_QUIET_HOST_LIMITS
    }
    if configured != SCALE_QUIET_HOST_LIMITS:
        raise GateError(
            "mandatory 250k scale quiet-host thresholds differ from the "
            "predeclared contract"
        )
    limits = {
        "load_one": (
            allowed_cpu_count
            * SCALE_QUIET_HOST_LIMITS["max_host_load_per_cpu"]
        ),
        "cpu_psi_avg10": SCALE_QUIET_HOST_LIMITS["max_cpu_psi_avg10"],
        "io_psi_avg10": SCALE_QUIET_HOST_LIMITS["max_io_psi_avg10"],
        "memory_psi_avg10": SCALE_QUIET_HOST_LIMITS[
            "max_memory_psi_avg10"
        ],
    }
    for phase, snapshot in pressure.items():
        for field, limit in limits.items():
            if snapshot[field] > limit:
                raise GateError(
                    f"mandatory 250k scale {phase} {field}="
                    f"{snapshot[field]} exceeds quiet-host limit {limit}"
                )
    return {
        "allow_noisy_host": False,
        "process_conflicts_before": 0,
        "process_conflicts_after": 0,
        "pressure_before": pressure["before"],
        "pressure_after": pressure["after"],
        "pressure_limits": limits,
    }


def _gnu_elapsed_ns(value: Any) -> int:
    if not isinstance(value, str):
        raise GateError("scale GNU time elapsed value is not text")
    parts = value.split(":")
    if len(parts) not in (2, 3):
        raise GateError("scale GNU time elapsed value has an invalid shape")
    seconds_text = parts[-1]
    if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", seconds_text) is None:
        raise GateError("scale GNU time elapsed seconds are malformed")
    seconds_whole, separator, fraction = seconds_text.partition(".")
    if int(seconds_whole) >= 60:
        raise GateError("scale GNU time elapsed seconds are out of range")
    if len(parts) == 2:
        hours = 0
        minutes_text = parts[0]
    else:
        hours_text, minutes_text = parts[:2]
        if re.fullmatch(r"[0-9]+", hours_text) is None:
            raise GateError("scale GNU time elapsed hours are malformed")
        hours = int(hours_text)
    if re.fullmatch(r"[0-9]+", minutes_text) is None:
        raise GateError("scale GNU time elapsed minutes are malformed")
    minutes = int(minutes_text)
    if len(parts) == 3 and minutes >= 60:
        raise GateError("scale GNU time elapsed minutes are out of range")
    fraction_ns = int((fraction + "000000000")[:9]) if separator else 0
    result = (
        (hours * 3600 + minutes * 60 + int(seconds_whole)) * 1_000_000_000
        + fraction_ns
    )
    return positive_int(result, "scale GNU time elapsed duration")


def _host_process_record(value: Any, context: str) -> dict[str, Any]:
    expected = {
        "pid",
        "ppid",
        "pgrp",
        "session",
        "starttime_ticks",
        "state",
        "comm",
        "argv0",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise GateError(f"{context} has an unexpected process-record shape")
    result = {
        field: (
            positive_int(value[field], f"{context}.{field}")
            if field in {"pid", "starttime_ticks"}
            else nonnegative_int(value[field], f"{context}.{field}")
        )
        for field in ("pid", "ppid", "pgrp", "session", "starttime_ticks")
    }
    state = value["state"]
    comm = value["comm"]
    argv0 = value["argv0"]
    if not isinstance(state, str) or len(state) != 1:
        raise GateError(f"{context}.state is malformed")
    if not isinstance(comm, str) or not comm:
        raise GateError(f"{context}.comm is malformed")
    if argv0 is not None and not isinstance(argv0, str):
        raise GateError(f"{context}.argv0 is malformed")
    result.update({"state": state, "comm": comm, "argv0": argv0})
    return result


def _host_proc_visibility(value: Any) -> dict[str, Any]:
    expected = {
        "hidepid",
        "nspid_depth",
        "pid_namespace",
        "pid1_stat_visible",
        "pid1_starttime_ticks",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise GateError(
            "scale host process visibility contract is malformed"
        )
    namespace = value["pid_namespace"]
    if (
        type(value["hidepid"]) is not int
        or value["hidepid"] != 0
        or type(value["nspid_depth"]) is not int
        or value["nspid_depth"] != 1
        or not isinstance(namespace, str)
        or re.fullmatch(r"pid:\[[0-9]+\]", namespace) is None
        or value["pid1_stat_visible"] is not True
    ):
        raise GateError(
            "scale host process visibility differs from the bare-host "
            "measurement contract"
        )
    positive_int(
        value["pid1_starttime_ticks"],
        "scale host process visible PID 1 start time",
    )
    return value


def _host_process_boundary(
    path: Path,
    *,
    phase: str,
    header: dict[str, Any],
) -> dict[str, Any]:
    _require_regular_file(path, f"scale host process {phase} boundary")
    value = load_json(path)
    expected = {
        "schema",
        "phase",
        "boot_id",
        "recorded_boottime_ns",
        "expected_leader_pid",
        "expected_leader_starttime_ticks",
        "expected_leader_present",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise GateError(f"scale host process {phase} boundary is malformed")
    leader_pid = positive_int(
        value["expected_leader_pid"],
        f"scale host process {phase} boundary leader",
    )
    leader_starttime = positive_int(
        value["expected_leader_starttime_ticks"],
        f"scale host process {phase} boundary leader start time",
    )
    if (
        value["schema"] != HOST_PROCESS_BOUNDARY_SCHEMA
        or value["phase"] != phase
        or value["boot_id"] != header["boot_id"]
        or leader_pid != header["expected_leader_pid"]
        or leader_starttime
        != header["expected_leader_starttime_ticks"]
        or not isinstance(value["expected_leader_present"], bool)
        or value["expected_leader_present"] is not (phase == "start")
    ):
        raise GateError(f"scale host process {phase} boundary differs from monitor")
    positive_int(
        value["recorded_boottime_ns"],
        f"scale host process {phase} boundary time",
    )
    return value


def _validate_continuous_host_process_evidence(
    run: Path,
    *,
    elapsed: Any,
    expected_interval_ms: int,
) -> dict[str, Any]:
    if expected_interval_ms != SCALE_HOST_PROCESS_SAMPLE_INTERVAL_MS:
        raise GateError(
            "mandatory 250k host-process sampling interval differs from the "
            "predeclared contract"
        )
    evidence = run / "host-process-samples.jsonl"
    ready_path = run / "host-process-monitor-ready.json"
    status_path = run / "host-process-monitor.exit-status"
    monitor_time_path = run / "host-process-monitor.time.txt"
    _require_regular_file(evidence, "scale continuous host process evidence")
    _require_regular_file(ready_path, "scale host process monitor readiness")
    _require_regular_file(status_path, "scale host process monitor status")
    _require_regular_file(
        monitor_time_path, "scale host process monitor GNU time"
    )
    _read_zero_status(status_path, "scale host process monitor")
    monitor_time = _parse_gnu_time_text(
        monitor_time_path.read_text(encoding="utf-8"),
        "scale host process monitor",
    )

    header: dict[str, Any] | None = None
    footer: dict[str, Any] | None = None
    header_line: bytes | None = None
    stream_hash = hashlib.sha256()
    samples: list[dict[str, Any]] = []
    expected_session_running = False
    conflict_observations = 0
    process_observations = 0
    vanished_pid_observations = 0
    maximum_vanished_pid_count = 0
    listed_pid_observations = 0
    with evidence.open("rb") as source:
        for line_number, raw_line in enumerate(source, 1):
            if not raw_line.endswith(b"\n"):
                raise GateError(
                    "scale host process evidence has an unterminated record"
                )
            try:
                value = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise GateError(
                    f"scale host process evidence line {line_number} is malformed"
                ) from error
            if not isinstance(value, dict):
                raise GateError(
                    f"scale host process evidence line {line_number} is not an object"
                )
            kind = value.get("kind")
            if footer is not None:
                raise GateError("scale host process evidence continues after footer")
            if header is None:
                expected_header = {
                    "kind",
                    "schema",
                    "boot_id",
                    "clock_ticks_per_second",
                    "interval_ms",
                    "abort_on_conflict",
                    "max_vanished_per_scan",
                    "max_vanished_ppm",
                    "classifier_sha256",
                    "proc_visibility",
                    "expected_session_id",
                    "expected_leader_pid",
                    "expected_leader_starttime_ticks",
                }
                if kind != "header" or set(value) != expected_header:
                    raise GateError("scale host process evidence has no valid header")
                interval_value = positive_int(
                    value["interval_ms"],
                    "scale host process sampling interval",
                )
                expected_session = positive_int(
                    value["expected_session_id"],
                    "scale host process expected session",
                )
                expected_leader = positive_int(
                    value["expected_leader_pid"],
                    "scale host process expected leader",
                )
                max_vanished_per_scan = positive_int(
                    value["max_vanished_per_scan"],
                    "scale host process maximum vanished PIDs per scan",
                )
                max_vanished_ppm = positive_int(
                    value["max_vanished_ppm"],
                    "scale host process maximum vanished PID rate",
                )
                if (
                    value["schema"] != HOST_PROCESS_EVIDENCE_SCHEMA
                    or interval_value != expected_interval_ms
                    or value["abort_on_conflict"] is not True
                    or expected_session != expected_leader
                    or max_vanished_per_scan
                    != SCALE_HOST_PROCESS_MAX_VANISHED_PER_SCAN
                    or max_vanished_ppm
                    != SCALE_HOST_PROCESS_MAX_VANISHED_PPM
                    or value["classifier_sha256"]
                    != canonical_sha256(_scale_process_classifier_contract())
                ):
                    raise GateError(
                        "scale host process evidence header differs from contract"
                    )
                if not isinstance(value["boot_id"], str) or re.fullmatch(
                    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                    r"[0-9a-f]{4}-[0-9a-f]{12}",
                    value["boot_id"],
                ) is None:
                    raise GateError(
                        "scale host process evidence has malformed boot ID"
                    )
                positive_int(
                    value["clock_ticks_per_second"],
                    "scale host process clock tick rate",
                )
                positive_int(
                    value["expected_leader_starttime_ticks"],
                    "scale host process leader start time",
                )
                _host_proc_visibility(value["proc_visibility"])
                header = value
                header_line = raw_line
                stream_hash.update(raw_line)
                continue
            if kind == "footer":
                expected_footer = {
                    "kind",
                    "sample_count",
                    "first_scan_started_boottime_ns",
                    "last_scan_ended_boottime_ns",
                    "stop_observed_boottime_ns",
                    "stream_sha256",
                }
                if set(value) != expected_footer:
                    raise GateError("scale host process footer is malformed")
                footer = value
                continue
            expected_sample = {
                "kind",
                "sequence",
                "scan_started_boottime_ns",
                "scan_ended_boottime_ns",
                "listed_pid_count",
                "vanished_pid_count",
                "process_count",
                "processes",
            }
            if kind != "sample" or set(value) != expected_sample:
                raise GateError(
                    f"scale host process sample {len(samples)} is malformed"
                )
            sequence = nonnegative_int(
                value["sequence"], "scale host process sample sequence"
            )
            if sequence != len(samples):
                raise GateError(
                    "scale host process sample sequences are not contiguous"
                )
            started = positive_int(
                value["scan_started_boottime_ns"],
                "scale host process sample start",
            )
            ended = positive_int(
                value["scan_ended_boottime_ns"],
                "scale host process sample end",
            )
            if ended < started:
                raise GateError("scale host process sample time regressed")
            listed = nonnegative_int(
                value["listed_pid_count"],
                "scale host process listed PID count",
            )
            listed_pid_observations += listed
            vanished = nonnegative_int(
                value["vanished_pid_count"],
                "scale host process vanished PID count",
            )
            vanished_pid_observations += vanished
            maximum_vanished_pid_count = max(
                maximum_vanished_pid_count, vanished
            )
            count = nonnegative_int(
                value["process_count"], "scale host process count"
            )
            processes = value["processes"]
            if (
                not isinstance(processes, list)
                or count != len(processes)
                or listed != count + vanished
            ):
                raise GateError(
                    "scale host process sample counts do not reconcile"
                )
            seen_pids = set()
            expected_session_count = 0
            leader_stopped = False
            exact_leader_present = False
            pid1_identity_present = False
            for index, raw_process in enumerate(processes):
                process = _host_process_record(
                    raw_process,
                    f"scale host process sample {sequence} record {index}",
                )
                if process["pid"] in seen_pids:
                    raise GateError(
                        "scale host process sample repeats a PID"
                    )
                seen_pids.add(process["pid"])
                process_observations += 1
                if (
                    process["pid"] == 1
                    and process["starttime_ticks"]
                    == header["proc_visibility"][
                        "pid1_starttime_ticks"
                    ]
                ):
                    pid1_identity_present = True
                if (
                    process["pid"] == header["expected_leader_pid"]
                    and process["starttime_ticks"]
                    == header["expected_leader_starttime_ticks"]
                ):
                    if (
                        process["pgrp"] != header["expected_leader_pid"]
                        or process["session"] != header["expected_session_id"]
                    ):
                        raise GateError(
                            "scale host process measured leader changed "
                            "session identity"
                        )
                    exact_leader_present = True
                if process["session"] == header["expected_session_id"]:
                    expected_session_count += 1
                    if (
                        process["pid"] == header["expected_leader_pid"]
                        and process["starttime_ticks"]
                        == header["expected_leader_starttime_ticks"]
                        and process["state"] in {"T", "t"}
                    ):
                        leader_stopped = True
                    if process["state"] not in {"T", "t"}:
                        expected_session_running = True
                elif _is_conflicting_scale_process(process):
                    conflict_observations += 1
                    raise GateError(
                        "mandatory 250k continuous host monitor observed "
                        f"conflicting process {sorted(_process_names(process))!r} "
                        f"at sample {sequence}"
                    )
            if not pid1_identity_present:
                raise GateError(
                    "scale host process scan lost the visible PID 1 identity"
                )
            if sequence == 0 and not leader_stopped:
                raise GateError(
                    "scale host process first sample does not contain the "
                    "stopped measured leader identity"
                )
            samples.append(
                {
                    "started": started,
                    "ended": ended,
                    "expected_session_count": expected_session_count,
                    "exact_leader_present": exact_leader_present,
                }
            )
            stream_hash.update(raw_line)
    if header is None or header_line is None or footer is None or not samples:
        raise GateError("scale host process evidence is incomplete")
    if (
        positive_int(footer["sample_count"], "scale host process footer count")
        != len(samples)
        or footer["first_scan_started_boottime_ns"] != samples[0]["started"]
        or footer["last_scan_ended_boottime_ns"] != samples[-1]["ended"]
        or footer["stream_sha256"] != stream_hash.hexdigest()
    ):
        raise GateError("scale host process footer does not reconcile")
    stop_observed_boottime_ns = positive_int(
        footer["stop_observed_boottime_ns"],
        "scale host process stop observation",
    )
    if (
        maximum_vanished_pid_count
        > SCALE_HOST_PROCESS_MAX_VANISHED_PER_SCAN
        or listed_pid_observations == 0
        or vanished_pid_observations * 1_000_000
        > listed_pid_observations * SCALE_HOST_PROCESS_MAX_VANISHED_PPM
    ):
        raise GateError(
            "scale host process unclassified vanished-PID observations "
            "exceed the predeclared uncertainty bound"
        )
    ready = load_json(ready_path)
    expected_ready = {
        "schema": HOST_PROCESS_READY_SCHEMA,
        "boot_id": header["boot_id"],
        "expected_session_id": header["expected_session_id"],
        "expected_leader_pid": header["expected_leader_pid"],
        "expected_leader_starttime_ticks": header[
            "expected_leader_starttime_ticks"
        ],
        "first_sample_scan_ended_boottime_ns": samples[0]["ended"],
        "header_sha256": hashlib.sha256(header_line).hexdigest(),
    }
    if ready != expected_ready:
        raise GateError("scale host process readiness does not reconcile")
    start = _host_process_boundary(
        run / "host-process-start.json", phase="start", header=header
    )
    end = _host_process_boundary(
        run / "host-process-end.json", phase="end", header=header
    )
    if (
        start["recorded_boottime_ns"] > end["recorded_boottime_ns"]
        or samples[0]["ended"] > start["recorded_boottime_ns"]
        or samples[-1]["started"] < end["recorded_boottime_ns"]
    ):
        raise GateError(
            "scale host process samples do not enclose measured boundaries"
        )
    if (
        len(samples) < 2
        or stop_observed_boottime_ns < end["recorded_boottime_ns"]
        or stop_observed_boottime_ns < samples[-2]["ended"]
        or stop_observed_boottime_ns > samples[-1]["started"]
    ):
        raise GateError(
            "scale host process final scan did not begin after the measured "
            "stop observation"
        )
    if samples[-1]["expected_session_count"] != 0:
        raise GateError(
            "scale host process final sample still contains measured-session "
            "processes"
        )
    interval_ns = expected_interval_ms * 1_000_000
    allowed_gap_ns = (
        interval_ns * SCALE_HOST_PROCESS_MAX_GAP_MULTIPLIER
    )
    leader_disappeared = False
    for sample in samples:
        leader_present = sample["exact_leader_present"]
        if leader_disappeared and leader_present:
            raise GateError(
                "scale host process measured leader disappeared and reappeared"
            )
        leader_disappeared = leader_disappeared or not leader_present
        scan_wholly_inside_measurement = (
            sample["started"] >= start["recorded_boottime_ns"]
            and sample["ended"]
            + allowed_gap_ns
            + SCALE_HOST_PROCESS_BOUNDARY_SLACK_NS
            <= end["recorded_boottime_ns"]
        )
        if scan_wholly_inside_measurement and not leader_present:
            raise GateError(
                "scale host process measured leader is missing inside the "
                "measured interval"
            )
        if (
            sample["started"] >= end["recorded_boottime_ns"]
            and leader_present
        ):
            raise GateError(
                "scale host process measured leader remains after the end "
                "boundary"
            )
    if not expected_session_running:
        raise GateError(
            "scale host process evidence never observed the running measured session"
        )
    maximum_gap_ns = 0
    maximum_scan_ns = 0
    previous_started: int | None = None
    previous_ended: int | None = None
    for sample in samples:
        maximum_scan_ns = max(
            maximum_scan_ns, sample["ended"] - sample["started"]
        )
        if previous_started is not None:
            assert previous_ended is not None
            if sample["started"] < previous_ended:
                raise GateError(
                    "scale host process sample timestamps overlap or regress"
                )
            maximum_gap_ns = max(
                maximum_gap_ns, sample["started"] - previous_started
            )
        previous_started = sample["started"]
        previous_ended = sample["ended"]
    if maximum_gap_ns > allowed_gap_ns or maximum_scan_ns > allowed_gap_ns:
        raise GateError(
            "scale continuous host-process sampling cadence has a coverage gap"
        )
    elapsed_ns = _gnu_elapsed_ns(elapsed)
    boundary_duration_ns = (
        end["recorded_boottime_ns"] - start["recorded_boottime_ns"]
    )
    if (
        boundary_duration_ns + SCALE_HOST_PROCESS_BOUNDARY_SLACK_NS
        < elapsed_ns
    ):
        raise GateError(
            "scale host process measured boundaries are shorter than GNU time"
        )
    monitor_elapsed_ns = _gnu_elapsed_ns(monitor_time["elapsed"])
    stream_duration_ns = samples[-1]["ended"] - samples[0]["started"]
    if (
        monitor_elapsed_ns + SCALE_HOST_PROCESS_BOUNDARY_SLACK_NS
        < boundary_duration_ns
        or monitor_elapsed_ns + SCALE_HOST_PROCESS_BOUNDARY_SLACK_NS
        < stream_duration_ns
    ):
        raise GateError(
            "scale host process monitor GNU time does not cover its raw "
            "sample stream and measured boundaries"
        )
    return {
        "schema": HOST_PROCESS_EVIDENCE_SCHEMA,
        "detection_claim": (
            "no recognized conflicting process observed with at most "
            f"{allowed_gap_ns / 1_000_000:.0f} ms between scan starts; "
            "unclassified process-exit races remained within the "
            "predeclared count and rate bounds"
        ),
        "sample_interval_ms": expected_interval_ms,
        "proc_visibility": header["proc_visibility"],
        "maximum_allowed_vanished_per_scan": (
            SCALE_HOST_PROCESS_MAX_VANISHED_PER_SCAN
        ),
        "maximum_allowed_vanished_rate_ppm": (
            SCALE_HOST_PROCESS_MAX_VANISHED_PPM
        ),
        "expected_leader_pid": header["expected_leader_pid"],
        "expected_leader_starttime_ticks": header[
            "expected_leader_starttime_ticks"
        ],
        "samples": len(samples),
        "process_observations": process_observations,
        "listed_pid_observations": listed_pid_observations,
        "conflict_observations": conflict_observations,
        "vanished_pid_observations": vanished_pid_observations,
        "maximum_vanished_pid_count": maximum_vanished_pid_count,
        "vanished_pid_rate_ppm": (
            vanished_pid_observations * 1_000_000
            // listed_pid_observations
        ),
        "first_scan_started_boottime_ns": samples[0]["started"],
        "last_scan_ended_boottime_ns": samples[-1]["ended"],
        "start_boundary_boottime_ns": start["recorded_boottime_ns"],
        "end_boundary_boottime_ns": end["recorded_boottime_ns"],
        "stop_observed_boottime_ns": stop_observed_boottime_ns,
        "maximum_scan_start_gap_ns": maximum_gap_ns,
        "maximum_scan_duration_ns": maximum_scan_ns,
        "measured_boundary_duration_ns": boundary_duration_ns,
        "sample_stream_duration_ns": stream_duration_ns,
        "gnu_time_elapsed_ns": elapsed_ns,
        "monitor_cost": {
            "elapsed_ns": monitor_elapsed_ns,
            "user_seconds": monitor_time["user_seconds"],
            "system_seconds": monitor_time["system_seconds"],
            "max_rss_kib": monitor_time["max_rss_kib"],
        },
    }


def _validate_scale_rss_evidence(
    run: Path,
    *,
    expected_root_pid: int,
    expected_root_starttime_ticks: int,
    expected_interval_ms: int,
    expected_duration_ns: int,
) -> dict[str, Any]:
    raw_path = run / "rss-samples.tsv"
    summary_path = run / "rss-summary.json"
    _require_regular_file(raw_path, "scale raw RSS samples")
    _require_regular_file(summary_path, "scale RSS summary")
    lines = raw_path.read_text(encoding="utf-8").splitlines()
    expected_header = (
        "elapsed_ns\trecorded_at\tprocess_count\trss_kib\trss_anon_kib\t"
        "rss_file_kib\tvm_swap_kib\tmax_single_hwm_kib\tpids"
    )
    if not lines or lines[0] != expected_header:
        raise GateError("scale raw RSS samples have a malformed header")

    maxima = {
        "aggregate_rss_kib": 0,
        "aggregate_rss_anon_kib": 0,
        "aggregate_rss_file_kib": 0,
        "aggregate_vm_swap_kib": 0,
        "max_single_process_hwm_kib": 0,
        "process_count": 0,
    }
    previous_elapsed: int | None = None
    maximum_elapsed_gap_ns = 0
    first_elapsed_ns: int | None = None
    last_elapsed_ns: int | None = None
    for row_number, line in enumerate(lines[1:], 1):
        fields = line.split("\t")
        if len(fields) != 9 or not fields[1]:
            raise GateError(
                f"scale raw RSS sample {row_number} is malformed"
            )
        try:
            elapsed_raw = int(fields[0])
            numeric_fields = [int(field) for field in fields[2:8]]
        except ValueError as error:
            raise GateError(
                f"scale raw RSS sample {row_number} has malformed numbers"
            ) from error
        elapsed = nonnegative_int(
            elapsed_raw, f"scale raw RSS sample {row_number} elapsed"
        )
        if previous_elapsed is not None and elapsed <= previous_elapsed:
            raise GateError("scale raw RSS elapsed times do not advance")
        if previous_elapsed is not None:
            maximum_elapsed_gap_ns = max(
                maximum_elapsed_gap_ns, elapsed - previous_elapsed
            )
        first_elapsed_ns = elapsed if first_elapsed_ns is None else first_elapsed_ns
        last_elapsed_ns = elapsed
        previous_elapsed = elapsed
        values = [
            nonnegative_int(
                value, f"scale raw RSS sample {row_number} field"
            )
            for value in numeric_fields
        ]
        process_count, rss, rss_anon, rss_file, swap, hwm = values
        try:
            pids = [int(value) for value in fields[8].split(",")]
        except ValueError as error:
            raise GateError(
                f"scale raw RSS sample {row_number} PID list is malformed"
            ) from error
        if (
            not pids
            or any(pid <= 0 for pid in pids)
            or len(pids) != len(set(pids))
            or pids != sorted(pids)
            or process_count != len(pids)
            or expected_root_pid not in pids
        ):
            raise GateError(
                f"scale raw RSS sample {row_number} does not bind the "
                "measured process tree"
            )
        observed = {
            "aggregate_rss_kib": rss,
            "aggregate_rss_anon_kib": rss_anon,
            "aggregate_rss_file_kib": rss_file,
            "aggregate_vm_swap_kib": swap,
            "max_single_process_hwm_kib": hwm,
            "process_count": process_count,
        }
        for key, value in observed.items():
            maxima[key] = max(maxima[key], value)

    summary = load_json(summary_path)
    expected_summary_keys = {
        "root_pid",
        "root_starttime_ticks",
        "samples",
        "interval_ms",
        *maxima,
    }
    if not isinstance(summary, dict) or set(summary) != expected_summary_keys:
        raise GateError("scale RSS summary has an unexpected shape")
    if (
        positive_int(summary["root_pid"], "scale RSS root PID")
        != expected_root_pid
        or positive_int(
            summary["root_starttime_ticks"],
            "scale RSS root PID start time",
        )
        != expected_root_starttime_ticks
        or positive_int(summary["interval_ms"], "scale RSS interval")
        != expected_interval_ms
        or positive_int(summary["samples"], "scale RSS sample count")
        != len(lines) - 1
        or any(summary[key] != value for key, value in maxima.items())
    ):
        raise GateError(
            "scale RSS summary does not reconcile with the measured leader "
            "and raw samples"
        )
    assert first_elapsed_ns is not None and last_elapsed_ns is not None
    interval_ns = expected_interval_ms * 1_000_000
    allowed_gap_ns = interval_ns * SCALE_RSS_MAX_GAP_MULTIPLIER
    if (
        first_elapsed_ns > interval_ns + SCALE_HOST_PROCESS_BOUNDARY_SLACK_NS
        or maximum_elapsed_gap_ns > allowed_gap_ns
        or last_elapsed_ns + allowed_gap_ns
        + SCALE_HOST_PROCESS_BOUNDARY_SLACK_NS
        < expected_duration_ns
    ):
        raise GateError(
            "scale raw RSS sampling cadence does not cover the measured "
            "duration"
        )
    return {
        "root_pid": expected_root_pid,
        "root_starttime_ticks": expected_root_starttime_ticks,
        "interval_ms": expected_interval_ms,
        "samples": len(lines) - 1,
        "first_elapsed_ns": first_elapsed_ns,
        "last_elapsed_ns": last_elapsed_ns,
        "maximum_elapsed_gap_ns": maximum_elapsed_gap_ns,
        "allowed_elapsed_gap_ns": allowed_gap_ns,
        "expected_duration_ns": expected_duration_ns,
        **maxima,
    }


def _parse_gnu_time_text(text: str, context: str) -> dict[str, Any]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if ": " in stripped:
            key, value = stripped.rsplit(": ", 1)
            values[key] = value
    keys = {
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
    missing = [source for source in keys.values() if source not in values]
    if missing:
        raise GateError(f"{context} GNU time report is missing fields: {missing!r}")
    result: dict[str, Any] = {}
    for target, source in keys.items():
        value = values[source]
        try:
            if target in {"user_seconds", "system_seconds"}:
                parsed = float(value)
                if not math.isfinite(parsed) or parsed < 0:
                    raise ValueError
                result[target] = parsed
            elif target == "elapsed":
                result[target] = value
            else:
                parsed = int(value)
                if parsed < 0:
                    raise ValueError
                result[target] = parsed
        except ValueError as error:
            raise GateError(f"{context} GNU time field {source!r} is malformed") from error
    result["cpu_percent"] = values.get("Percent of CPU this job got", "")
    if result["exit_status"] != 0:
        raise GateError(f"{context} GNU time exit status is not zero")
    return result


def _parse_perf_text(text: str) -> dict[str, Any]:
    events = []
    observed: set[str] = set()
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) < 3 or not fields[2].strip():
            continue
        event = fields[2].strip()
        if event in observed:
            raise GateError(f"scale perf report repeats event {event!r}")
        observed.add(event)
        raw_value = fields[0].strip()
        events.append(
            {
                "event": event,
                "raw_value": raw_value,
                "unit": fields[1].strip(),
                "available": re.fullmatch(r"[0-9.]+", raw_value) is not None,
            }
        )
    if set(REQUIRED_PERF_EVENTS) - observed:
        raise GateError("scale perf report is missing required events")
    if any(
        not item["available"]
        for item in events
        if item["event"] in REQUIRED_PERF_EVENTS
    ):
        raise GateError("scale perf report has an unavailable required event")
    return {"events": events}


def _validate_scale_context(
    root: Path,
    expected_messages: int,
    expected: dict[str, Any],
) -> dict[str, Any]:
    required = {
        "binary_hashes",
        "capture_manifest_sha256",
        "capture_file",
        "config_template_sha256",
        "ingest_cpuset",
        "client_cpuset",
        "api_listen",
        "live_memory_admission_bytes",
        "publish_interval_ms",
        "max_view_staleness_ms",
        "max_concurrent_queries",
        "range_scalar_cache_max_bytes",
        "rss_interval_ms",
    }
    if set(expected) != required:
        raise GateError("scale expectations have an unexpected shape")
    binary_hashes = expected["binary_hashes"]
    required_roles = {
        "chronoxide-ingester",
        "chronoxide-api",
        "chronoxide-query",
        "chronoxide-storage-verify",
    }
    if not isinstance(binary_hashes, dict) or set(binary_hashes) != required_roles:
        raise GateError("scale expectations require hashes for all four binary roles")
    binary_hashes = {
        role: _sha256_field(digest, f"expected {role} hash")
        for role, digest in binary_hashes.items()
    }
    expected_capture = expected["capture_file"]
    if not isinstance(expected_capture, dict) or set(expected_capture) != {
        "name",
        "sha256",
        "size_bytes",
    }:
        raise GateError("scale expected capture file has an unexpected shape")
    capture_name = expected_capture["name"]
    if not isinstance(capture_name, str) or Path(capture_name).name != capture_name:
        raise GateError("scale expected capture filename is unsafe")
    expected_capture = {
        "name": capture_name,
        "sha256": _sha256_field(
            expected_capture["sha256"], "expected capture file hash"
        ),
        "size_bytes": positive_int(
            expected_capture["size_bytes"], "expected capture file size"
        ),
    }
    expected_capture_manifest = _sha256_field(
        expected["capture_manifest_sha256"], "expected capture manifest hash"
    )
    expected_template = _sha256_field(
        expected["config_template_sha256"], "expected config-template hash"
    )
    expected_ingest = parse_cpuset(expected["ingest_cpuset"])
    expected_client = parse_cpuset(expected["client_cpuset"])
    if expected_ingest & expected_client:
        raise GateError("expected scale CPU sets overlap")
    expected_controls = {
        key: (
            nonnegative_int(expected[key], f"expected scale {key}")
            if key == "range_scalar_cache_max_bytes"
            else positive_int(expected[key], f"expected scale {key}")
        )
        for key in (
            "live_memory_admission_bytes",
            "publish_interval_ms",
            "max_view_staleness_ms",
            "max_concurrent_queries",
            "range_scalar_cache_max_bytes",
            "rss_interval_ms",
        )
    }
    api_listen = expected["api_listen"]
    if not isinstance(api_listen, str) or not api_listen:
        raise GateError("expected scale API listen address is missing")

    settings = _settings(root / "metadata" / "settings.txt")
    expected_settings = {
        "result_dir": str(root.resolve()),
        "stop_after_messages": str(expected_messages),
        "run_order": "P",
        "diagnostic_p_only": "1",
        "api_listen": api_listen,
        "live_memory_admission_bytes": str(
            expected_controls["live_memory_admission_bytes"]
        ),
        "publish_interval_ms": str(expected_controls["publish_interval_ms"]),
        "max_view_staleness_ms": str(
            expected_controls["max_view_staleness_ms"]
        ),
        "max_concurrent_queries": str(
            expected_controls["max_concurrent_queries"]
        ),
        "range_scalar_cache_max_bytes": str(
            expected_controls["range_scalar_cache_max_bytes"]
        ),
        "rss_interval_ms": str(expected_controls["rss_interval_ms"]),
        "perf_stat_mode": "required",
        "evict_capture": "1",
    }
    for key, value in expected_settings.items():
        if settings.get(key) != value:
            raise GateError(
                f"scale setting {key} differs from the accepted value"
            )
    allow_noisy_host = settings.get("allow_noisy_host")
    readback_sample_limit = settings.get("readback_sample_limit_per_kind")
    host_process_sample_interval = settings.get(
        "host_process_sample_interval_ms"
    )
    if expected_messages == 250_000:
        if allow_noisy_host != "0":
            raise GateError(
                "mandatory 250k scale settings do not require a quiet host"
            )
        if readback_sample_limit != "2":
            raise GateError(
                "mandatory 250k scale readback sample limit is not two per kind"
            )
        if host_process_sample_interval != str(
            SCALE_HOST_PROCESS_SAMPLE_INTERVAL_MS
        ):
            raise GateError(
                "mandatory 250k host-process sampling interval differs from "
                "the predeclared contract"
            )
    else:
        if allow_noisy_host not in (None, "0", "1"):
            raise GateError("125k scale noisy-host setting is malformed")
        if readback_sample_limit not in (None, "2"):
            raise GateError("125k scale readback sample limit is not two per kind")
        if (
            host_process_sample_interval is not None
            and host_process_sample_interval
            != str(SCALE_HOST_PROCESS_SAMPLE_INTERVAL_MS)
        ):
            raise GateError("125k host-process sampling interval is malformed")
    if parse_cpuset(settings.get("ingest_cpuset", "")) != expected_ingest:
        raise GateError("scale ingest CPU set differs from the accepted set")
    if parse_cpuset(settings.get("client_cpuset", "")) != expected_client:
        raise GateError("scale client CPU set differs from the accepted set")
    if not settings.get("run_note"):
        raise GateError("scale run note is empty")

    copied_template = root / "metadata" / "config-template.toml"
    _require_regular_file(copied_template, "scale copied config template")
    if _file_sha256(copied_template) != expected_template:
        raise GateError("scale copied config-template hash is not accepted")
    with copied_template.open("rb") as source:
        phase1._config_contract(tomllib.load(source))
    copied_capture_manifest = root / "metadata" / "capture-manifest.json"
    _require_regular_file(copied_capture_manifest, "scale copied capture manifest")
    if _file_sha256(copied_capture_manifest) != expected_capture_manifest:
        raise GateError("scale copied capture-manifest hash is not accepted")

    capacity_path = root / "metadata" / "capture-capacity.json"
    selected_path = root / "metadata" / "validated-inputs.json"
    _require_regular_file(
        capacity_path, "scale validated capture-capacity document"
    )
    _require_regular_file(
        selected_path, "scale selected-input-prefix document"
    )
    capacity_inputs = load_json(capacity_path)
    if (
        not isinstance(capacity_inputs, dict)
        or set(capacity_inputs) != VALIDATED_CAPTURE_CAPACITY_FIELDS
    ):
        raise GateError(
            "scale validated capture-capacity document has an unexpected shape"
        )
    inputs = load_json(selected_path)
    if (
        not isinstance(inputs, dict)
        or set(inputs) != SELECTED_INPUT_PREFIX_FIELDS
        or inputs.get("schema") != SELECTED_INPUT_PREFIX_SCHEMA
    ):
        raise GateError(
            "scale selected-input-prefix document has an unexpected shape"
        )
    for field in SELECTED_INPUT_PREFIX_IDENTITY_FIELDS:
        if inputs[field] != capacity_inputs[field]:
            raise GateError(
                "scale selected input identity differs from validated "
                f"capture capacity at {field}"
            )
    if inputs.get("capture_manifest_sha256") != expected_capture_manifest:
        raise GateError("scale validated capture manifest is not accepted")
    if inputs.get("config_template_sha256") != expected_template:
        raise GateError("scale validated config template is not accepted")
    if inputs.get("capture_files") != [expected_capture]:
        raise GateError("scale validated capture file is not accepted")
    selected_input_prefix_messages = positive_int(
        inputs["stop_after_messages"],
        "scale selected input prefix",
    )
    if selected_input_prefix_messages != expected_messages:
        raise GateError("scale selected input prefix differs from the run")
    capacity_messages = positive_int(
        capacity_inputs["stop_after_messages"],
        "scale validated capture-capacity prefix",
    )
    validated_capture_capacity_messages = positive_int(
        inputs["validated_capture_capacity_messages"],
        "scale validated capture capacity",
    )
    if validated_capture_capacity_messages != capacity_messages:
        raise GateError(
            "scale selected input capacity differs from the validated "
            "capture-capacity document"
        )
    if selected_input_prefix_messages > capacity_messages:
        raise GateError("scale validated capture is shorter than the selected prefix")
    capture_path = inputs.get("capture")
    template_path = inputs.get("config_template")
    if (
        not isinstance(capture_path, str)
        or not isinstance(template_path, str)
        or settings.get("capture") != capture_path
        or settings.get("config_template") != template_path
    ):
        raise GateError("scale settings and validated input paths differ")

    cpusets = load_json(root / "metadata" / "cpusets.json")
    if (
        not isinstance(cpusets, dict)
        or set(cpusets) != {"allowed", "client", "ingest"}
        or cpusets.get("ingest") != sorted(expected_ingest)
        or cpusets.get("client") != sorted(expected_client)
    ):
        raise GateError("scale normalized CPU sets differ from the accepted sets")
    allowed = cpusets.get("allowed")
    if (
        not isinstance(allowed, list)
        or any(isinstance(item, bool) or not isinstance(item, int) for item in allowed)
        or allowed != sorted(set(allowed))
        or not (expected_ingest | expected_client).issubset(allowed)
    ):
        raise GateError("scale normalized allowed CPU set is malformed")
    host_evidence: dict[str, Any]
    if expected_messages == 250_000:
        host_evidence = _validate_quiet_scale_host(
            root, settings, len(allowed)
        )
    else:
        host_evidence = {
            "allow_noisy_host": (
                None if allow_noisy_host is None else allow_noisy_host == "1"
            ),
            "quiet_host_required": False,
        }

    config_path = root / "configs" / "P.toml"
    _require_regular_file(config_path, "scale P configuration")
    with config_path.open("rb") as source:
        config = tomllib.load(source)
    contract = phase1._config_contract(config)
    ingestion = contract["ingestion"]
    writer = contract["writer"]
    api = config.get("api")
    if not isinstance(api, dict):
        raise GateError("scale P configuration has no API table")
    segments_dir = root / "runs" / "P" / "segments"
    config_checks = {
        "ingestion.replay_from": (ingestion.get("replay_from"), capture_path),
        "ingestion.stop_after_messages": (
            ingestion.get("stop_after_messages"),
            expected_messages,
        ),
        "ingestion.segment_writer.segments_dir": (
            writer.get("segments_dir"),
            str(segments_dir),
        ),
        "api.enabled": (api.get("enabled"), True),
        "api.listen": (api.get("listen"), api_listen),
        "api.head_publish_interval_ms": (
            api.get("head_publish_interval_ms"),
            expected_controls["publish_interval_ms"],
        ),
        "api.max_view_staleness_ms": (
            api.get("max_view_staleness_ms"),
            expected_controls["max_view_staleness_ms"],
        ),
        "api.live_memory_admission_bytes": (
            api.get("live_memory_admission_bytes"),
            expected_controls["live_memory_admission_bytes"],
        ),
        "api.max_concurrent_queries": (
            api.get("max_concurrent_queries"),
            expected_controls["max_concurrent_queries"],
        ),
        "api.range_scalar_cache_max_bytes": (
            api.get("range_scalar_cache_max_bytes"),
            expected_controls["range_scalar_cache_max_bytes"],
        ),
    }
    for field, (actual, accepted) in config_checks.items():
        if actual != accepted:
            raise GateError(f"scale {field} differs from the accepted value")
    fixed_api = {
        "query_max_series_matched": 1_000_000,
        "query_max_projected_series": 2_000_000,
        "query_max_chunks_read": 5_000_000,
        "query_max_bytes_read": 2_147_483_648,
        "query_max_samples": 50_000_000,
        "regex_max_expanded_values": 100_000,
        "chunk_read_mode": "pread",
        "chunk_read_queue_depth": 128,
        "chunk_payload_coalesce_max_gap_bytes": 4096,
        "experimental_cross_segment_chunk_reads": False,
    }
    for field, accepted in fixed_api.items():
        if api.get(field) != accepted:
            raise GateError(f"scale api.{field} differs from the accepted value")
    render = load_json(root / "runs" / "P" / "config-render.json")
    expected_render = {
        "variant": "P",
        "api_enabled": True,
        "capture": capture_path,
        "segments_dir": str(segments_dir),
        "stop_after_messages": expected_messages,
        "config_sha256": _file_sha256(config_path),
    }
    if render != expected_render:
        raise GateError("scale config-render evidence differs from P.toml")

    observed_hashes = _binary_hashes(root, "scale")
    if observed_hashes != binary_hashes:
        raise GateError("scale frozen binary hashes differ from the accepted hashes")

    run = root / "runs" / "P"
    raw_time = run / "replay.time.txt"
    normalized_time = run / "replay.time.json"
    _require_regular_file(raw_time, "scale raw replay time")
    _require_regular_file(normalized_time, "scale normalized replay time")
    time_value = _parse_gnu_time_text(
        raw_time.read_text(encoding="utf-8"), "scale replay"
    )
    if load_json(normalized_time) != time_value:
        raise GateError("scale normalized replay time differs from the raw report")
    if expected_messages == 250_000:
        continuous_process_monitor = (
            _validate_continuous_host_process_evidence(
                run,
                elapsed=time_value["elapsed"],
                expected_interval_ms=SCALE_HOST_PROCESS_SAMPLE_INTERVAL_MS,
            )
        )
        host_evidence["continuous_process_monitor"] = (
            continuous_process_monitor
        )
        host_evidence["rss_process_tree_binding"] = (
            _validate_scale_rss_evidence(
                run,
                expected_root_pid=continuous_process_monitor[
                    "expected_leader_pid"
                ],
                expected_root_starttime_ticks=continuous_process_monitor[
                    "expected_leader_starttime_ticks"
                ],
                expected_interval_ms=expected_controls["rss_interval_ms"],
                expected_duration_ns=continuous_process_monitor[
                    "measured_boundary_duration_ns"
                ],
            )
        )
    raw_perf = run / "perf-stat.tsv"
    normalized_perf = run / "perf-stat.json"
    _require_regular_file(raw_perf, "scale raw perf stat")
    _require_regular_file(normalized_perf, "scale normalized perf stat")
    perf_value = _parse_perf_text(raw_perf.read_text(encoding="utf-8"))
    if load_json(normalized_perf) != perf_value:
        raise GateError("scale normalized perf stat differs from the raw report")
    for gap in ("PERF_COVERAGE_GAP", "CAPTURE_CACHE_COVERAGE_GAP"):
        if (root / "metadata" / gap).exists():
            raise GateError(f"scale result contains {gap}")
    residency = run / "capture-residency-before.tsv"
    _require_regular_file(residency, "scale capture residency evidence")
    residency_rows = residency.read_text(encoding="utf-8").splitlines()
    if len(residency_rows) != 1:
        raise GateError("scale capture residency must cover exactly one capture file")
    fields = residency_rows[0].split(maxsplit=2)
    if (
        len(fields) != 3
        or fields[0] != "0"
        or fields[1] != str(expected_capture["size_bytes"])
        or Path(fields[2]).name != expected_capture["name"]
        or str(Path(fields[2]).parent) != capture_path
    ):
        raise GateError("scale capture residency evidence differs from accepted input")

    return {
        "mode": SCALE_MESSAGE_COUNTS[expected_messages],
        "binary_hashes": observed_hashes,
        "capture_manifest_sha256": expected_capture_manifest,
        "capture_file": expected_capture,
        "validated_capture_capacity_messages": (
            validated_capture_capacity_messages
        ),
        "selected_input_prefix_messages": selected_input_prefix_messages,
        "config_template_sha256": expected_template,
        "config_sha256": expected_render["config_sha256"],
        "ingest_cpus": sorted(expected_ingest),
        "client_cpus": sorted(expected_client),
        "api_listen": api_listen,
        **expected_controls,
        "perf_required": True,
        "capture_resident_bytes_before_run": 0,
        "readback_sample_limit_per_kind": (
            None
            if readback_sample_limit is None
            else int(readback_sample_limit)
        ),
        "host_evidence": host_evidence,
        "time": time_value,
        "perf": perf_value,
        "settings_sha256": _file_sha256(root / "metadata" / "settings.txt"),
    }


def _validate_scale_storage_and_readbacks(
    root: Path, run: Path, expected_messages: int
) -> dict[str, Any]:
    validation = root / "validation"
    raw_storage = validation / "storage-verify.json"
    normalized_storage = validation / "storage-verify-gate.json"
    raw_readbacks = validation / "readbacks.md"
    normalized_readbacks = validation / "readbacks-gate.json"
    for path, context in (
        (raw_storage, "raw storage verifier"),
        (normalized_storage, "normalized storage verifier"),
        (raw_readbacks, "raw independent readbacks"),
        (normalized_readbacks, "normalized independent readbacks"),
    ):
        _require_regular_file(path, f"scale {context}")
    observed_storage = validate_storage_verifier(
        raw_storage,
        run / "replay-correctness.json",
        run / "ingester.log",
        require_writer_reconciliation=False,
    )
    if load_json(normalized_storage) != observed_storage:
        raise GateError(
            "scale normalized storage gate differs from the raw verifier report"
        )
    observed_readbacks = validate_readbacks(raw_readbacks)
    expected_readback_queries = SCALE_EXPECTED_READBACK_QUERIES[
        expected_messages
    ]
    if observed_readbacks["expected_queries"] != expected_readback_queries:
        raise GateError(
            f"{expected_messages}-message scale requires exactly "
            f"{expected_readback_queries} independent readback queries; "
            f"observed {observed_readbacks['expected_queries']}"
        )
    if load_json(normalized_readbacks) != observed_readbacks:
        raise GateError(
            "scale normalized readback gate differs from the raw readback report"
        )
    for name in ("storage-verify.time.txt", "readbacks.time.txt"):
        path = validation / name
        _require_regular_file(path, f"scale {name}")
        _parse_gnu_time_text(path.read_text(encoding="utf-8"), f"scale {name}")
    coverage_gap = root / "metadata" / "PHYSICAL_SAMPLE_COUNT_COVERAGE_GAP"
    _require_regular_file(
        coverage_gap, "scale prefix physical-sample coverage disclosure"
    )
    if (
        expected_messages == 250_000
        and coverage_gap.read_text(encoding="utf-8").strip()
        != SCALE_P_ONLY_COVERAGE_GAP
    ):
        raise GateError(
            "mandatory 250k physical-sample coverage disclosure is stale "
            "or misleading"
        )
    if (root / "metadata" / "CAPTURE_LEVEL_PHYSICAL_SAMPLE_GOLDEN_GATED").exists():
        raise GateError("scale prefix cannot claim the 4M physical-sample golden")
    return {
        "schema_version": observed_storage["schema_version"],
        "footer_validation_enabled": True,
        "exact_postings_fingerprint": observed_storage[
            "exact_postings_fingerprint"
        ],
        "verified_selection_fingerprint": observed_storage[
            "verified_selection_fingerprint"
        ],
        "decoded_semantic_fingerprint": observed_storage[
            "decoded_semantic_fingerprint"
        ],
        "writer_to_verifier_counts_reconciled": observed_storage[
            "writer_to_verifier_counts_reconciled"
        ],
        "capture_level_physical_sample_golden_gated": observed_storage[
            "capture_level_physical_sample_golden_gated"
        ],
        "expected_independent_readback_queries": expected_readback_queries,
        "independent_readback_queries": observed_readbacks["executed_queries"],
        "readbacks": observed_readbacks,
    }


def gate_shutdown_ab(roots: list[Path]) -> dict[str, Any]:
    labels = ("A1", "B1", "B2", "A2")
    if len(roots) != len(labels):
        raise GateError("shutdown A/B gate requires roots in A1,B1,B2,A2 order")
    resolved_roots = [root.resolve() for root in roots]
    if len(set(resolved_roots)) != len(resolved_roots):
        raise GateError("shutdown A/B gate result roots must be distinct")

    arms: dict[str, dict[str, Any]] = {}
    expected_messages: int | None = None
    for label, root in zip(labels, roots):
        arm = _load_shutdown_ab_arm(root, label, expected_messages)
        expected_messages = arm["expected_messages"]
        arms[label] = arm
    assert expected_messages is not None

    a1_hashes = arms["A1"]["binary_hashes"]
    a2_hashes = arms["A2"]["binary_hashes"]
    b1_hashes = arms["B1"]["binary_hashes"]
    b2_hashes = arms["B2"]["binary_hashes"]
    ingester_role = "chronoxide-ingester"
    if a1_hashes[ingester_role] != a2_hashes[ingester_role]:
        raise GateError("baseline A ingester hashes differ")
    if b1_hashes[ingester_role] != b2_hashes[ingester_role]:
        raise GateError("candidate B ingester hashes differ")
    if a1_hashes[ingester_role] == b1_hashes[ingester_role]:
        raise GateError("baseline and candidate ingester hashes are identical")
    for role in ("chronoxide-query", "chronoxide-storage-verify", "chronoxide-api"):
        present = [role in arms[label]["binary_hashes"] for label in labels]
        if any(present) and not all(present):
            raise GateError(f"{role} presence differs across shutdown A/B arms")
        if not any(present):
            continue
        hashes = {arms[label]["binary_hashes"][role] for label in labels}
        if len(hashes) != 1:
            raise GateError(f"{role} hash differs across shutdown A/B arms")

    reference = arms["A1"]
    for label in labels[1:]:
        arm = arms[label]
        if arm["segments_manifest"] != reference["segments_manifest"]:
            raise GateError(f"{label} segment manifest differs byte-for-byte from A1")
        if arm["replay"] != reference["replay"]:
            raise GateError(f"{label} replay correctness JSON differs from A1")
        if arm["corpus"] != reference["corpus"]:
            raise GateError(f"{label} corpus manifest differs from A1")
        if arm["storage_fingerprints"] != reference["storage_fingerprints"]:
            raise GateError(f"{label} storage fingerprints differ from A1")

    for label in ("A1", "A2"):
        if arms[label]["final_empty_fast_path"] not in (None, False):
            raise GateError(f"{label} baseline unexpectedly used the empty fast path")
    for label in ("B1", "B2"):
        arm = arms[label]
        if arm["final_empty_fast_path"] is not True:
            raise GateError(f"{label} candidate did not use the empty fast path")
        base_scale = arm["base_scale"]
        if not isinstance(base_scale, dict):
            raise GateError(f"{label} candidate has no base-scale observation")
        for field in (
            "base_sample_keys",
            "base_sample_fragments",
            "base_catalog_active_series",
        ):
            positive_int(base_scale.get(field), f"{label} {field}")

    lower_is_better = (
        "shutdown_publication_ns",
        "shutdown_post_seal_ns",
        "shutdown_sample_catalog_ns",
    )
    for metric in lower_is_better:
        worst_candidate = max(arms[label]["metrics"][metric] for label in ("B1", "B2"))
        best_baseline = min(arms[label]["metrics"][metric] for label in ("A1", "A2"))
        if worst_candidate >= best_baseline:
            raise GateError(
                f"candidate {metric} is not below both baseline observations"
            )

    for metric in ("boundary_p95_ns", "boundary_max_ns"):
        worst_baseline = max(
            arms[label]["metrics"][metric] for label in ("A1", "A2")
        )
        for label in ("B1", "B2"):
            if arms[label]["metrics"][metric] * 100 > worst_baseline * 110:
                raise GateError(
                    f"{label} {metric} regresses by more than 10% versus "
                    "the worst baseline"
                )
    worst_baseline_rss = max(
        arms[label]["metrics"]["peak_rss_kib"] for label in ("A1", "A2")
    )
    for label in ("B1", "B2"):
        if arms[label]["metrics"]["peak_rss_kib"] * 100 > worst_baseline_rss * 105:
            raise GateError(
                f"{label} peak RSS regresses by more than 5% versus the worst baseline"
            )

    metric_names = tuple(reference["metrics"])
    means = {
        variant: {
            metric: _mean(
                [arms[label]["metrics"][metric] for label in variant_labels]
            )
            for metric in metric_names
        }
        for variant, variant_labels in (
            ("A", ("A1", "A2")),
            ("B", ("B1", "B2")),
        )
    }
    output_arms = {
        label: {
            "root": arms[label]["root"],
            "variant": label[0],
            "ingester_sha256": arms[label]["binary_hashes"][ingester_role],
            "final_empty_fast_path": arms[label]["final_empty_fast_path"],
            "base_scale": arms[label]["base_scale"],
            "metrics": arms[label]["metrics"],
        }
        for label in labels
    }
    return {
        "schema": SHUTDOWN_AB_SCHEMA,
        "complete": True,
        "order": list(labels),
        "expected_messages": expected_messages,
        "binary_hashes": {
            "baseline_ingester_sha256": a1_hashes[ingester_role],
            "candidate_ingester_sha256": b1_hashes[ingester_role],
            "api_sha256": a1_hashes.get("chronoxide-api"),
            "query_sha256": a1_hashes["chronoxide-query"],
            "storage_verify_sha256": a1_hashes["chronoxide-storage-verify"],
        },
        "correctness": {
            "segments_sha256_equal": True,
            "replay_correctness_equal": True,
            "corpus_manifest_equal": True,
            "storage_fingerprints_equal": True,
            "readbacks_complete": True,
            "corpus_manifest_sha256": reference["corpus_fingerprint"],
            **reference["storage_fingerprints"],
        },
        "arms": output_arms,
        "means": means,
        "acceptance": {
            "candidate_shutdown_below_both_baselines": True,
            "candidate_post_seal_below_both_baselines": True,
            "candidate_sample_catalog_below_both_baselines": True,
            "candidate_boundary_p95_within_10_percent": True,
            "candidate_boundary_max_within_10_percent": True,
            "candidate_peak_rss_within_5_percent": True,
        },
    }


def gate_publication_scale(
    root: Path,
    expected_messages: int,
    expected: dict[str, Any],
    *,
    _test_only_allow_unisolated_validator: bool = False,
) -> dict[str, Any]:
    expected_messages = positive_int(expected_messages, "expected messages")
    if expected_messages not in SCALE_MESSAGE_COUNTS:
        raise GateError(
            "publication scale gate accepts exactly 125000 or 250000 messages"
        )
    result_artifacts = _validate_result_artifacts(
        root, "scale", expected_messages
    )
    scale_context = _validate_scale_context(root, expected_messages, expected)
    arm = _load_shutdown_ab_arm(root, "scale", expected_messages)
    if arm["binary_hashes"] != scale_context["binary_hashes"]:
        raise GateError("scale loaded binary hashes differ from accepted context")
    raw_live_log = parse_live_log_text(
        (root / "runs" / "P" / "ingester.log").read_text(encoding="utf-8"),
        expected_messages,
    )
    boundary_publications = positive_int(
        raw_live_log.get("boundary_publications"),
        "scale successful ordinary publication count",
    )
    successful_boundaries = positive_int(
        raw_live_log.get("successful_message_boundary_observations"),
        "scale successful message-boundary observation count",
    )
    failed_boundaries = nonnegative_int(
        raw_live_log.get("failed_message_boundary_observations"),
        "scale failed message-boundary observation count",
    )
    if boundary_publications < 10:
        raise GateError("scale requires at least 10 successful ordinary publications")
    if successful_boundaries != boundary_publications:
        raise GateError(
            "scale successful ordinary publications and message-boundary "
            "observations differ"
        )
    if failed_boundaries != 0:
        raise GateError("scale contains a failed message-boundary publication")
    shutdown_generation = raw_live_log["shutdown_publication"]["generation"]
    boundary_cuts = [
        positive_int(
            item.get("visible_message_sequence"),
            "scale ordinary publication message cut",
        )
        for item in raw_live_log["generation_message_sequence"]
        if item.get("generation") != shutdown_generation
    ]
    if len(boundary_cuts) != boundary_publications:
        raise GateError(
            "scale ordinary publication cuts do not reconcile with the "
            "publication count"
        )
    if any(
        current <= previous
        for previous, current in zip(boundary_cuts, boundary_cuts[1:])
    ):
        raise GateError(
            "scale ordinary publication message cuts do not strictly advance"
        )
    last_boundary_cut = boundary_cuts[-1]
    if (
        expected_messages == 250_000
        and last_boundary_cut * 100
        < expected_messages * SCALE_MANDATORY_LATE_CUT_MIN_PERCENT
    ):
        raise GateError(
            "mandatory 250k scale ordinary publications do not reach the "
            f"predeclared {SCALE_MANDATORY_LATE_CUT_MIN_PERCENT}% late-cut "
            "coverage threshold"
        )
    if arm["final_empty_fast_path"] is not True:
        raise GateError("scale candidate did not use the final empty fast path")
    base_scale = arm["base_scale"]
    if not isinstance(base_scale, dict):
        raise GateError("scale candidate has no final base-scale observation")
    for field in (
        "base_sample_keys",
        "base_sample_fragments",
        "base_catalog_active_series",
    ):
        positive_int(base_scale.get(field), f"scale {field}")

    limits_ns = dict(SCALE_COMMON_LIMITS_NS)
    if expected_messages == 250_000:
        limits_ns.update(SCALE_MANDATORY_LIMITS_NS)
    metrics = arm["metrics"]
    for metric, limit in limits_ns.items():
        if metrics[metric] > limit:
            raise GateError(
                f"scale {metric}={metrics[metric]} exceeds limit {limit}"
            )
    validation = _validate_scale_storage_and_readbacks(
        root, root / "runs" / "P", expected_messages
    )
    validator = _publication_scale_validator_provenance(
        root,
        test_only_allow_unisolated=_test_only_allow_unisolated_validator,
    )
    return {
        "schema": PUBLICATION_SCALE_SCHEMA,
        "complete": True,
        "root": arm["root"],
        "gate_mode": SCALE_MESSAGE_COUNTS[expected_messages],
        "expected_messages": expected_messages,
        "binary_hashes": arm["binary_hashes"],
        "scale_context": scale_context,
        "result_artifacts": result_artifacts,
        "validator": validator,
        "correctness": {
            "replay_correctness_valid": True,
            "segment_manifest_valid": True,
            "schema8_footer_exact_postings_valid": True,
            "readbacks_complete": True,
            "corpus_manifest_sha256": arm["corpus_fingerprint"],
            "validation": validation,
            **arm["storage_fingerprints"],
        },
        "final_empty_fast_path": True,
        "base_scale": base_scale,
        "metrics": metrics,
        "limits_ns": limits_ns,
        "acceptance": {
            "boundary_p95": True,
            "boundary_max": True,
            "shutdown_non_seal": True,
            "shutdown_sample_root_plus_catalog": (
                True if expected_messages == 250_000 else "deferred-to-250k"
            ),
            "shutdown_post_commit": (
                True if expected_messages == 250_000 else "deferred-to-250k"
            ),
            "successful_ordinary_publications": boundary_publications,
            "last_ordinary_message_cut": last_boundary_cut,
            "late_cut_min_percent": (
                SCALE_MANDATORY_LATE_CUT_MIN_PERCENT
                if expected_messages == 250_000
                else "deferred-to-250k"
            ),
            "successful_message_boundary_observations": successful_boundaries,
            "failed_message_boundary_observations": failed_boundaries,
            "peak_rss_observed_kib": metrics["peak_rss_kib"],
        },
    }


def gate_run_set(
    runs_root: Path,
    workload_path: Path,
    expected_messages: int,
    perf_required: bool,
    phase1_expectations_path: Path | None = None,
) -> dict[str, Any]:
    workload = load_workload(workload_path)
    normalized_configs: dict[str, Any] = {}
    correctness: dict[str, Any] = {}
    manifests: dict[str, bytes] = {}
    corpus_summaries: dict[str, Any] = {}
    live_logs: dict[str, Any] = {}
    for variant in ("D", "P", "Q"):
        root = runs_root / variant
        config_path = runs_root.parent / "configs" / f"{variant}.toml"
        with config_path.open("rb") as source:
            config = tomllib.load(source)
        config["api"]["enabled"] = False
        config["ingestion"]["segment_writer"]["segments_dir"] = "<fresh-run-root>"
        normalized_configs[variant] = config
        _read_zero_status(root / "ingester.exit-status", f"{variant} ingester")
        _read_zero_status(root / "rss-monitor.exit-status", f"{variant} RSS monitor")
        if variant == "Q":
            _read_zero_status(root / "client.exit-status", "Q client")
        correctness[variant] = validate_replay_document(
            load_json(root / "replay-correctness.json"), expected_messages
        )
        manifests[variant] = (root / "segments.sha256").read_bytes()
        corpus_summaries[variant] = load_json(root / "corpus-summary.json")
        timing = load_json(root / "replay.time.json")
        if timing.get("exit_status") != 0:
            raise GateError(f"{variant} GNU time did not observe a clean exit")
        rss = load_json(root / "rss-summary.json")
        if nonnegative_int(rss.get("samples"), f"{variant} RSS samples") == 0:
            raise GateError(f"{variant} RSS monitor has no samples")
        if nonnegative_int(
            rss.get("aggregate_vm_swap_kib"), f"{variant} process-tree swap"
        ) != 0:
            raise GateError(f"{variant} measured process tree used swap")
        if perf_required:
            perf = load_json(root / "perf-stat.json")
            if not isinstance(perf.get("events"), list) or not perf["events"]:
                raise GateError(f"{variant} perf stat evidence is empty")
        if variant != "D":
            observed = parse_live_log_text(
                (root / "ingester.log").read_text(encoding="utf-8"),
                expected_messages,
            )
            recorded = load_json(root / "live-log-summary.json")
            if observed != recorded:
                raise GateError(f"{variant} live-log summary differs from raw log")
            live_logs[variant] = observed
    for variant in ("P", "Q"):
        difference = phase1._difference(
            normalized_configs[variant], normalized_configs["D"]
        )
        if difference:
            raise GateError(
                f"{variant} config differs from D beyond api.enabled/segments_dir: "
                f"{difference}"
            )
        difference = phase1._difference(correctness[variant], correctness["D"])
        if difference:
            raise GateError(f"{variant} replay counters differ from D: {difference}")
        if manifests[variant] != manifests["D"]:
            raise GateError(f"{variant} storage tree differs byte-for-byte from D")
        difference = phase1._difference(
            corpus_summaries[variant], corpus_summaries["D"]
        )
        if difference:
            raise GateError(f"{variant} corpus summary differs from D: {difference}")
    phase1_reference_gated = False
    if phase1_expectations_path is not None:
        phase1_expected = phase1._expectations(phase1_expectations_path)
        if expected_messages == phase1_expected["stop_after_messages"]:
            expected_replay = phase1_expected["replay_correctness"]
            normalized_replay = json.loads(json.dumps(correctness["D"]))
            # The current parser adds only reconciled zero-valued fields and a
            # schema bump to the historical v1 document. Compare the complete
            # historical object as a required subset after normalizing that
            # known schema evolution; all current counters were already
            # reconciled above.
            normalized_replay["schema"] = expected_replay["schema"]
            difference = phase1._subset_difference(
                normalized_replay, expected_replay
            )
            if difference:
                raise GateError(f"4M Phase1 replay expectation mismatch: {difference}")
            difference = phase1._difference(
                corpus_summaries["D"], phase1_expected["corpus"]
            )
            if difference:
                raise GateError(f"4M Phase1 corpus expectation mismatch: {difference}")
            phase1_reference_gated = True
    client_records = read_client_records(runs_root / "Q" / "client-records.jsonl")
    client = validate_client_records(client_records, workload)
    if client != load_json(runs_root / "Q" / "client-summary.json"):
        raise GateError("Q client summary differs from raw records")
    published_q_mapping = {
        item["generation"]: (
            item["visible_message_sequence"],
            item["catalog_revision"],
        )
        for item in live_logs["Q"]["generation_message_sequence"]
    }
    for record in client_records:
        generation = record["generation"]
        cut = (record["visible_message_sequence"], record["catalog_revision"])
        if published_q_mapping.get(generation) != cut:
            raise GateError(
                "Q HTTP response generation/message/catalog cut is absent from publication log"
            )
    live_head_only_observed = any(
        record["cardinality"] > 0
        and record["visible_message_sequence"] < expected_messages
        and record["query_stats"]["segments_queried"] == 0
        for record in client_records
        if next(
            query
            for query in workload["queries"]
            if query["name"] == record["query_name"]
        )["require_nonempty"]
    )
    if not live_head_only_observed:
        raise GateError(
            "Q observed no designated non-empty pre-final result with zero sealed "
            "segments queried"
        )
    return {
        "schema": RUN_SET_SCHEMA,
        "complete": True,
        "expected_messages": expected_messages,
        "recorded_samples": correctness["D"]["general"]["Recorded Samples"],
        "corpus": corpus_summaries["D"],
        "replay_correctness_sha256": canonical_sha256(correctness["D"]),
        "storage_trees_equal": True,
        "replay_counters_equal": True,
        "live_logs": live_logs,
        "client": client,
        "live_head_only_observed": live_head_only_observed,
        "perf_required": perf_required,
        "phase1_reference_gated": phase1_reference_gated,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    commands = result.add_subparsers(dest="command", required=True)

    workload = commands.add_parser("validate-workload")
    workload.add_argument("--workload", type=Path, required=True)
    workload.add_argument("--output", type=Path)

    cpus = commands.add_parser("validate-cpusets")
    cpus.add_argument("--ingest", required=True)
    cpus.add_argument("--client", required=True)
    cpus.add_argument("--output", type=Path)

    selected_prefix = commands.add_parser("bind-selected-input-prefix")
    selected_prefix.add_argument(
        "--validated-capacity", type=Path, required=True
    )
    selected_prefix.add_argument(
        "--stop-after-messages", type=int, required=True
    )
    selected_prefix.add_argument("--output", type=Path, required=True)

    render = commands.add_parser("render-config")
    render.add_argument("--template", type=Path, required=True)
    render.add_argument("--output", type=Path, required=True)
    render.add_argument("--capture", type=Path, required=True)
    render.add_argument("--segments-dir", type=Path, required=True)
    render.add_argument("--stop-after-messages", type=int, required=True)
    render.add_argument("--variant", choices=("D", "P", "Q"), required=True)
    render.add_argument("--listen", required=True)
    render.add_argument("--publish-interval-ms", type=int, required=True)
    render.add_argument("--max-staleness-ms", type=int, required=True)
    render.add_argument("--memory-admission-bytes", type=int, required=True)
    render.add_argument("--max-concurrent-queries", type=int, required=True)
    render.add_argument("--range-cache-bytes", type=int, required=True)

    client = commands.add_parser("client")
    client.add_argument("--base-url", required=True)
    client.add_argument("--workload", type=Path, required=True)
    client.add_argument("--records", type=Path, required=True)
    client.add_argument("--summary", type=Path, required=True)
    client.add_argument("--stop-file", type=Path, required=True)

    host_monitor = commands.add_parser("monitor-host-processes")
    host_monitor.add_argument("--expected-session-id", type=int, required=True)
    host_monitor.add_argument("--interval-ms", type=int, required=True)
    host_monitor.add_argument("--abort-on-conflict", action="store_true")
    host_monitor.add_argument("--stop-file", type=Path, required=True)
    host_monitor.add_argument("--ready-file", type=Path, required=True)
    host_monitor.add_argument("--output", type=Path, required=True)

    host_boundary = commands.add_parser("record-host-process-boundary")
    host_boundary.add_argument("--phase", choices=("start", "end"), required=True)
    host_boundary.add_argument("--expected-leader-pid", type=int, required=True)
    host_boundary.add_argument("--start-boundary", type=Path)
    host_boundary.add_argument("--output", type=Path, required=True)

    live_log = commands.add_parser("parse-live-log")
    live_log.add_argument("--log", type=Path, required=True)
    live_log.add_argument("--expected-messages", type=int, required=True)
    live_log.add_argument("--output", type=Path, required=True)

    run_set = commands.add_parser("gate-run-set")
    run_set.add_argument("--runs-root", type=Path, required=True)
    run_set.add_argument("--workload", type=Path, required=True)
    run_set.add_argument("--expected-messages", type=int, required=True)
    run_set.add_argument("--perf-required", action="store_true")
    run_set.add_argument("--phase1-expectations", type=Path)
    run_set.add_argument("--output", type=Path, required=True)

    shutdown_ab = commands.add_parser("gate-shutdown-ab")
    shutdown_ab.add_argument(
        "--roots",
        type=Path,
        nargs=4,
        required=True,
        metavar=("A1", "B1", "B2", "A2"),
    )
    shutdown_ab.add_argument("--output", type=Path, required=True)

    publication_scale = commands.add_parser("gate-publication-scale")
    publication_scale.add_argument("--root", type=Path, required=True)
    publication_scale.add_argument(
        "--expected-messages", type=int, required=True
    )
    for role in ("ingester", "api", "query", "storage-verify"):
        publication_scale.add_argument(
            f"--expected-{role}-sha256", required=True
        )
    publication_scale.add_argument(
        "--expected-capture-manifest-sha256", required=True
    )
    publication_scale.add_argument("--expected-capture-file-name", required=True)
    publication_scale.add_argument("--expected-capture-file-sha256", required=True)
    publication_scale.add_argument(
        "--expected-capture-file-size-bytes", type=int, required=True
    )
    publication_scale.add_argument(
        "--expected-config-template-sha256", required=True
    )
    publication_scale.add_argument("--expected-ingest-cpuset", required=True)
    publication_scale.add_argument("--expected-client-cpuset", required=True)
    publication_scale.add_argument("--expected-api-listen", required=True)
    publication_scale.add_argument(
        "--expected-live-memory-admission-bytes", type=int, required=True
    )
    publication_scale.add_argument(
        "--expected-publish-interval-ms", type=int, required=True
    )
    publication_scale.add_argument(
        "--expected-max-view-staleness-ms", type=int, required=True
    )
    publication_scale.add_argument(
        "--expected-max-concurrent-queries", type=int, required=True
    )
    publication_scale.add_argument(
        "--expected-range-scalar-cache-max-bytes", type=int, required=True
    )
    publication_scale.add_argument(
        "--expected-rss-interval-ms", type=int, required=True
    )
    publication_scale.add_argument("--output", type=Path, required=True)

    readbacks = commands.add_parser("gate-readbacks")
    readbacks.add_argument("--report", type=Path, required=True)
    readbacks.add_argument("--output", type=Path, required=True)

    storage = commands.add_parser("gate-storage")
    storage.add_argument("--report", type=Path, required=True)
    storage.add_argument("--replay-correctness", type=Path, required=True)
    storage.add_argument("--ingester-log", type=Path, required=True)
    storage.add_argument(
        "--live-handoff",
        action="store_true",
        help=(
            "validate an exhaustive live-handoff corpus without disabled-mode "
            "per-window writer-count logs"
        ),
    )
    storage.add_argument("--output", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "validate-workload":
            value = load_workload(args.workload)
            if args.output:
                write_json_exclusive(args.output, value)
            else:
                print(json.dumps(value, indent=2, sort_keys=True))
        elif args.command == "validate-cpusets":
            value = validate_cpusets(args.ingest, args.client)
            if args.output:
                write_json_exclusive(args.output, value)
            else:
                print(json.dumps(value, sort_keys=True))
        elif args.command == "bind-selected-input-prefix":
            print(
                json.dumps(
                    bind_selected_input_prefix(
                        args.validated_capacity,
                        args.stop_after_messages,
                        args.output,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "render-config":
            print(
                json.dumps(
                    render_config(
                        args.template,
                        args.output,
                        args.capture,
                        args.segments_dir,
                        args.stop_after_messages,
                        args.variant,
                        args.listen,
                        args.publish_interval_ms,
                        args.max_staleness_ms,
                        args.memory_admission_bytes,
                        args.max_concurrent_queries,
                        args.range_cache_bytes,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "client":
            print(
                json.dumps(
                    run_client(
                        args.base_url,
                        args.workload,
                        args.records,
                        args.summary,
                        args.stop_file,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "monitor-host-processes":
            print(
                json.dumps(
                    monitor_host_processes(
                        expected_session_id=args.expected_session_id,
                        interval_ms=args.interval_ms,
                        abort_on_conflict=args.abort_on_conflict,
                        stop_file=args.stop_file,
                        ready_file=args.ready_file,
                        output=args.output,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "record-host-process-boundary":
            print(
                json.dumps(
                    record_host_process_boundary(
                        phase=args.phase,
                        expected_leader_pid=args.expected_leader_pid,
                        output=args.output,
                        start_boundary=args.start_boundary,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "parse-live-log":
            value = parse_live_log_text(
                args.log.read_text(encoding="utf-8"), args.expected_messages
            )
            write_json_exclusive(args.output, value)
            print(json.dumps(value, sort_keys=True))
        elif args.command == "gate-run-set":
            value = gate_run_set(
                args.runs_root,
                args.workload,
                args.expected_messages,
                args.perf_required,
                args.phase1_expectations,
            )
            write_json_exclusive(args.output, value)
            print(json.dumps(value, sort_keys=True))
        elif args.command == "gate-shutdown-ab":
            value = gate_shutdown_ab(args.roots)
            write_json_exclusive(args.output, value)
            print(json.dumps(value, sort_keys=True))
        elif args.command == "gate-publication-scale":
            value = gate_publication_scale(
                args.root,
                args.expected_messages,
                {
                    "binary_hashes": {
                        "chronoxide-ingester": args.expected_ingester_sha256,
                        "chronoxide-api": args.expected_api_sha256,
                        "chronoxide-query": args.expected_query_sha256,
                        "chronoxide-storage-verify": (
                            args.expected_storage_verify_sha256
                        ),
                    },
                    "capture_manifest_sha256": (
                        args.expected_capture_manifest_sha256
                    ),
                    "capture_file": {
                        "name": args.expected_capture_file_name,
                        "sha256": args.expected_capture_file_sha256,
                        "size_bytes": args.expected_capture_file_size_bytes,
                    },
                    "config_template_sha256": (
                        args.expected_config_template_sha256
                    ),
                    "ingest_cpuset": args.expected_ingest_cpuset,
                    "client_cpuset": args.expected_client_cpuset,
                    "api_listen": args.expected_api_listen,
                    "live_memory_admission_bytes": (
                        args.expected_live_memory_admission_bytes
                    ),
                    "publish_interval_ms": args.expected_publish_interval_ms,
                    "max_view_staleness_ms": (
                        args.expected_max_view_staleness_ms
                    ),
                    "max_concurrent_queries": (
                        args.expected_max_concurrent_queries
                    ),
                    "range_scalar_cache_max_bytes": (
                        args.expected_range_scalar_cache_max_bytes
                    ),
                    "rss_interval_ms": args.expected_rss_interval_ms,
                },
            )
            write_json_exclusive(args.output, value)
            print(json.dumps(value, sort_keys=True))
        elif args.command == "gate-readbacks":
            value = validate_readbacks(args.report)
            write_json_exclusive(args.output, value)
            print(json.dumps(value, sort_keys=True))
        elif args.command == "gate-storage":
            value = validate_storage_verifier(
                args.report,
                args.replay_correctness,
                args.ingester_log,
                require_writer_reconciliation=not args.live_handoff,
            )
            write_json_exclusive(args.output, value)
            print(json.dumps(value, sort_keys=True))
        else:
            raise AssertionError(args.command)
    except (
        GateError,
        OSError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
        urllib.error.URLError,
    ) as error:
        print(f"live-query ingestion A/B gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
