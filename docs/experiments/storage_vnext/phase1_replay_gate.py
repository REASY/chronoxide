#!/usr/bin/env python3
"""Strict helpers for the storage-vNext Phase 1 four-million-message replay."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import stat
import sys
import time
import tomllib
from pathlib import Path
from typing import Any


class GateError(ValueError):
    pass


EXPECTATIONS_SCHEMA = "chronoxide/storage-vnext-phase1-expectations/v1"
CORPUS_SUMMARY_SCHEMA = "chronoxide/storage-vnext-phase1-corpus/v1"


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def _write_json_exclusive(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as destination:
        json.dump(value, destination, indent=2, sort_keys=True)
        destination.write("\n")


def _expectations(path: Path) -> dict[str, Any]:
    value = _load_json(path)
    if not isinstance(value, dict) or value.get("schema") != EXPECTATIONS_SCHEMA:
        raise GateError(f"unsupported expectations schema in {path}")
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(16 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _regular_file(path: Path, description: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise GateError(f"{description} is missing: {path}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise GateError(f"{description} must be a regular non-symlink file: {path}")


def _config_contract(document: dict[str, Any]) -> dict[str, Any]:
    try:
        ingestion = document["ingestion"]
        head = ingestion["head_buffer"]
        writer = ingestion["segment_writer"]
    except (KeyError, TypeError) as error:
        raise GateError(f"missing required configuration table: {error}") from error

    expected_ingestion = {
        "max_event_age_secs": 3600,
        "max_event_lead_secs": 5,
        "drop_outdated": True,
        "labelset_store": "flat_interned",
        "capture_only": False,
    }
    expected_head = {
        "enabled": True,
        "window_duration_secs": 3600,
        "out_of_order_time_window_secs": 3600,
        "float_encoding": "gorilla",
        "int_encoding": "delta_zig_zag",
        "varlen_encoding": "raw",
        "compact_numeric_series": True,
        "adaptive_series_table": True,
    }
    expected_writer = {
        "enabled": True,
        "segment_duration_secs": 900,
        "deterministic_id_seed": 42,
        "storage_schema": "schema8",
        "float_encoding": "gorilla",
        "int_encoding": "delta_zig_zag",
        "varlen_encoding": "raw",
    }
    for table_name, table, expected in (
        ("ingestion", ingestion, expected_ingestion),
        ("ingestion.head_buffer", head, expected_head),
        ("ingestion.segment_writer", writer, expected_writer),
    ):
        for key, value in expected.items():
            if table.get(key) != value:
                raise GateError(
                    f"{table_name}.{key} must be {value!r}; got {table.get(key)!r}"
                )
    return {"ingestion": ingestion, "head": head, "writer": writer}


def validate_inputs(capture: Path, template: Path, expectations_path: Path) -> dict[str, Any]:
    expected = _expectations(expectations_path)
    if not capture.is_absolute() or not capture.is_dir():
        raise GateError(f"capture must be an absolute directory: {capture}")
    _regular_file(template, "configuration template")
    if not template.is_absolute():
        raise GateError(f"configuration template must be absolute: {template}")

    template_hash = _sha256_file(template)
    if template_hash != expected["config_template_sha256"]:
        raise GateError(
            "configuration template hash mismatch: "
            f"expected {expected['config_template_sha256']}, got {template_hash}"
        )
    with template.open("rb") as source:
        _config_contract(tomllib.load(source))

    manifest = capture / "manifest.json"
    _regular_file(manifest, "capture manifest")
    manifest_hash = _sha256_file(manifest)
    if manifest_hash != expected["capture"]["manifest_sha256"]:
        raise GateError(
            "capture manifest hash mismatch: "
            f"expected {expected['capture']['manifest_sha256']}, got {manifest_hash}"
        )
    manifest_document = _load_json(manifest)
    if manifest_document.get("version") != 2:
        raise GateError("capture manifest version must be 2")
    partitions = manifest_document.get("partitions")
    if not isinstance(partitions, list):
        raise GateError("capture manifest partitions must be a list")
    manifest_files = []
    for partition in partitions:
        if not isinstance(partition, dict):
            raise GateError("capture manifest partition must be an object")
        name = partition.get("file_name")
        if not isinstance(name, str) or Path(name).name != name:
            raise GateError(f"unsafe capture partition filename: {name!r}")
        if partition.get("message_count", 0) < expected["stop_after_messages"]:
            raise GateError(f"capture partition {name} is shorter than the pinned prefix")
        manifest_files.append(name)

    expected_files = expected["capture"]["files"]
    expected_names = [item["name"] for item in expected_files]
    actual_names = sorted(path.name for path in capture.glob("*.capture"))
    if sorted(manifest_files) != sorted(expected_names) or actual_names != sorted(expected_names):
        raise GateError(
            "capture file set mismatch: "
            f"expected {sorted(expected_names)!r}, manifest {sorted(manifest_files)!r}, "
            f"directory {actual_names!r}"
        )

    observed_files = []
    for item in expected_files:
        path = capture / item["name"]
        _regular_file(path, "capture partition")
        size = path.stat().st_size
        if size != item["size_bytes"]:
            raise GateError(
                f"capture size mismatch for {item['name']}: "
                f"expected {item['size_bytes']}, got {size}"
            )
        digest = _sha256_file(path)
        if digest != item["sha256"]:
            raise GateError(
                f"capture hash mismatch for {item['name']}: "
                f"expected {item['sha256']}, got {digest}"
            )
        observed_files.append({"name": item["name"], "size_bytes": size, "sha256": digest})

    return {
        "capture": str(capture.resolve()),
        "capture_manifest_sha256": manifest_hash,
        "capture_files": observed_files,
        "config_template": str(template.resolve()),
        "config_template_sha256": template_hash,
        "stop_after_messages": expected["stop_after_messages"],
    }


def _replace_assignment(
    lines: list[str], section_name: str, key: str, rendered_value: str
) -> None:
    section = ""
    matches: list[int] = []
    table_pattern = re.compile(r"^\s*\[([^]]+)]\s*(?:#.*)?$")
    key_pattern = re.compile(rf"^\s*{re.escape(key)}\s*=")
    for index, line in enumerate(lines):
        table_match = table_pattern.match(line.rstrip("\n"))
        if table_match:
            section = table_match.group(1).strip()
            continue
        if section == section_name and key_pattern.match(line):
            matches.append(index)
    if len(matches) != 1:
        raise GateError(
            f"template must contain exactly one {section_name}.{key}; found {len(matches)}"
        )
    newline = "\n" if lines[matches[0]].endswith("\n") else ""
    lines[matches[0]] = f"{key} = {rendered_value}{newline}"


def render_config(
    template: Path,
    output: Path,
    capture: Path,
    segments_dir: Path,
    stop_after_messages: int,
) -> dict[str, Any]:
    if output.exists():
        raise GateError(f"configuration output already exists: {output}")
    if segments_dir.exists():
        raise GateError(f"segment output already exists: {segments_dir}")
    lines = template.read_text(encoding="utf-8").splitlines(keepends=True)
    _replace_assignment(lines, "ingestion", "replay_from", json.dumps(str(capture)))
    _replace_assignment(lines, "ingestion", "stop_after_messages", str(stop_after_messages))
    _replace_assignment(
        lines, "ingestion.segment_writer", "segments_dir", json.dumps(str(segments_dir))
    )
    rendered = "".join(lines)
    with output.open("x", encoding="utf-8") as destination:
        destination.write(rendered)
    with output.open("rb") as source:
        tables = _config_contract(tomllib.load(source))
    if tables["ingestion"].get("replay_from") != str(capture):
        raise GateError("rendered replay_from does not match the selected capture")
    if tables["ingestion"].get("stop_after_messages") != stop_after_messages:
        raise GateError("rendered stop_after_messages does not match the pinned prefix")
    if tables["writer"].get("segments_dir") != str(segments_dir):
        raise GateError("rendered segments_dir does not match the fresh run directory")
    return {
        "config": str(output),
        "sha256": _sha256_file(output),
        "segments_dir": str(segments_dir),
        "stop_after_messages": stop_after_messages,
    }


def _corpus_files(corpus: Path) -> list[tuple[str, Path]]:
    if not corpus.is_absolute():
        raise GateError(f"corpus must be an absolute non-symlink directory: {corpus}")
    try:
        corpus_metadata = corpus.lstat()
    except OSError as error:
        raise GateError(f"cannot inspect corpus directory {corpus}: {error}") from error
    if stat.S_ISLNK(corpus_metadata.st_mode) or not stat.S_ISDIR(
        corpus_metadata.st_mode
    ):
        raise GateError(f"corpus must be an absolute non-symlink directory: {corpus}")

    files: list[tuple[str, Path]] = []
    pending = [Path()]
    while pending:
        relative_directory = pending.pop()
        directory = corpus / relative_directory
        try:
            with os.scandir(directory) as entries:
                ordered_entries = sorted(
                    entries, key=lambda entry: os.fsencode(entry.name)
                )
        except OSError as error:
            raise GateError(
                f"cannot enumerate corpus directory {directory}: {error}"
            ) from error
        child_directories: list[Path] = []
        for entry in ordered_entries:
            relative = relative_directory / entry.name
            if relative.is_absolute() or any(
                part in ("", ".", "..") for part in relative.parts
            ):
                raise GateError(f"corpus path escapes its root: {relative!s}")
            path = corpus / relative
            try:
                metadata = entry.stat(follow_symlinks=False)
            except OSError as error:
                raise GateError(
                    f"cannot inspect corpus entry {path}: {error}"
                ) from error
            if stat.S_ISLNK(metadata.st_mode):
                raise GateError(f"corpus contains a symbolic link: {path}")
            if stat.S_ISDIR(metadata.st_mode):
                child_directories.append(relative)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise GateError(f"corpus entry is not a regular file: {path}")
            relative_text = relative.as_posix()
            if "\t" in relative_text or "\n" in relative_text:
                raise GateError(
                    f"corpus path contains a tab or newline: {relative_text!r}"
                )
            files.append((relative_text, path))
        pending.extend(reversed(child_directories))
    files.sort(key=lambda item: item[0].encode())
    if not files:
        raise GateError(f"corpus contains no files: {corpus}")
    return files


def write_tree_manifest(
    corpus: Path, manifest_path: Path, inventory_path: Path, summary_path: Path
) -> dict[str, Any]:
    for output in (manifest_path, inventory_path, summary_path):
        if output.exists():
            raise GateError(f"refusing to reuse corpus output: {output}")
    rows = []
    for relative, path in _corpus_files(corpus):
        rows.append((_sha256_file(path), path.stat().st_size, relative))
    manifest_bytes = "".join(
        f"{digest}  ./{relative}\n" for digest, _size, relative in rows
    ).encode()
    manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
    total_bytes = sum(size for _digest, size, _relative in rows)
    with manifest_path.open("xb") as destination:
        destination.write(manifest_bytes)
    with inventory_path.open("x", encoding="utf-8") as destination:
        destination.write("sha256\tsize_bytes\tpath\n")
        for digest, size, relative in rows:
            destination.write(f"{digest}\t{size}\t{relative}\n")
    summary = {
        "schema": CORPUS_SUMMARY_SCHEMA,
        "file_count": len(rows),
        "size_bytes": total_bytes,
        "manifest_sha256": manifest_digest,
    }
    _write_json_exclusive(summary_path, summary)
    return summary


def _difference(actual: Any, expected: Any, path: str = "$") -> str | None:
    if type(actual) is not type(expected):
        return f"{path}: expected type {type(expected).__name__}, got {type(actual).__name__}"
    if isinstance(expected, dict):
        if actual.keys() != expected.keys():
            missing = sorted(expected.keys() - actual.keys())
            extra = sorted(actual.keys() - expected.keys())
            return f"{path}: object keys differ; missing={missing!r}, extra={extra!r}"
        for key in expected:
            result = _difference(actual[key], expected[key], f"{path}.{key}")
            if result:
                return result
        return None
    if isinstance(expected, list):
        if len(actual) != len(expected):
            return f"{path}: expected {len(expected)} items, got {len(actual)}"
        for index, value in enumerate(expected):
            result = _difference(actual[index], value, f"{path}[{index}]")
            if result:
                return result
        return None
    if actual != expected:
        return f"{path}: expected {expected!r}, got {actual!r}"
    return None


def _subset_difference(actual: Any, expected: Any, path: str = "$") -> str | None:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return f"{path}: expected object, got {type(actual).__name__}"
        for key, value in expected.items():
            if key not in actual:
                return f"{path}: missing key {key!r}"
            result = _subset_difference(actual[key], value, f"{path}.{key}")
            if result:
                return result
        return None
    return _difference(actual, expected, path)


def gate_document(actual_path: Path, expectations_path: Path, expected_key: str) -> None:
    actual = _load_json(actual_path)
    expected = _expectations(expectations_path)[expected_key]
    difference = _difference(actual, expected)
    if difference:
        raise GateError(f"{expected_key} mismatch: {difference}")


def gate_verifier(actual_path: Path, expectations_path: Path) -> None:
    actual = _load_json(actual_path)
    expected = _expectations(expectations_path)["storage_verifier"]
    difference = _subset_difference(actual, expected)
    if difference:
        raise GateError(f"storage verifier mismatch: {difference}")


def _section(text: str, title: str) -> str:
    match = re.search(rf"(?m)^## {re.escape(title)}\s*$", text)
    if match is None:
        raise GateError(f"missing markdown section: {title}")
    following = text[match.end() :]
    next_section = re.search(r"(?m)^## ", following)
    return following if next_section is None else following[: next_section.start()]


def _markdown_rows(section: str) -> list[list[str]]:
    rows = []
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if cells and not all(re.fullmatch(r":?-+:?", cell) for cell in cells):
            rows.append(cells)
    return rows


def _two_column_values(text: str, title: str) -> dict[str, str]:
    values = {}
    for cells in _markdown_rows(_section(text, title)):
        if len(cells) == 2 and cells != ["Metric", "Value"]:
            values[cells[0]] = cells[1]
    return values


def _required_integer(values: dict[str, str], key: str) -> int:
    value = values.get(key)
    if value is None or re.fullmatch(r"[0-9]+", value) is None:
        raise GateError(f"missing or invalid readback metric {key!r}: {value!r}")
    return int(value)


def gate_readbacks(
    report_path: Path, expectations_path: Path, output_path: Path | None
) -> dict[str, Any]:
    text = report_path.read_text(encoding="utf-8")
    verification = _two_column_values(text, "Readback Verification")
    diagnostics = _two_column_values(text, "Query Diagnostics")
    rows = _markdown_rows(_section(text, "PromQL Readbacks"))
    if not rows or rows[0][0] != "Kind":
        raise GateError("PromQL Readbacks table is missing its expected header")
    rows = rows[1:]
    rows_fingerprint = hashlib.sha256(
        json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    actual = {
        "expected_queries": _required_integer(diagnostics, "Expected Readback Queries"),
        "executed_queries": _required_integer(diagnostics, "Executed Readback Queries"),
        "skipped_queries": _required_integer(diagnostics, "Skipped Readback Queries"),
        "isolation_check_skips": _required_integer(diagnostics, "Isolation Check Skips"),
        "mismatches": _required_integer(verification, "Mismatches"),
        "promql_rows": len(rows),
        "promql_rows_fingerprint_sha256": rows_fingerprint,
    }
    checked = _required_integer(verification, "Checked Queries")
    if checked != actual["executed_queries"]:
        raise GateError("checked and executed readback query counts differ")
    expected = _expectations(expectations_path)["readbacks"]
    difference = _difference(actual, expected)
    if difference:
        raise GateError(f"readback mismatch: {difference}")
    if output_path is not None:
        _write_json_exclusive(output_path, actual)
    return actual


def parse_gnu_time(input_path: Path, output_path: Path) -> dict[str, Any]:
    values: dict[str, str] = {}
    for line in input_path.read_text(encoding="utf-8").splitlines():
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
        raise GateError(f"GNU time report is missing fields: {missing!r}")
    result: dict[str, Any] = {}
    for target, source in keys.items():
        value = values[source]
        if target in {"user_seconds", "system_seconds"}:
            result[target] = float(value)
        elif target == "elapsed":
            result[target] = value
        else:
            result[target] = int(value)
    result["cpu_percent"] = values.get("Percent of CPU this job got", "")
    _write_json_exclusive(output_path, result)
    return result


def parse_perf_stat(
    input_path: Path, output_path: Path, required_events: list[str] | None = None
) -> dict[str, Any]:
    events = []
    for line in input_path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) < 3:
            continue
        raw_value = fields[0].strip()
        event = fields[2].strip()
        if not event:
            continue
        events.append(
            {
                "event": event,
                "raw_value": raw_value,
                "unit": fields[1].strip(),
                "available": re.fullmatch(r"[0-9.]+", raw_value) is not None,
            }
        )
    if not events:
        raise GateError(f"perf stat report contains no event rows: {input_path}")
    by_name = {item["event"]: item for item in events}
    for event in required_events or []:
        if event not in by_name:
            raise GateError(f"perf stat report is missing required event: {event}")
        if not by_name[event]["available"]:
            raise GateError(
                f"perf stat required event is unavailable: {event}="
                f"{by_name[event]['raw_value']!r}"
            )
    result = {"events": events}
    _write_json_exclusive(output_path, result)
    return result


def run_summary_row(
    label: str,
    kind: str,
    time_path: Path,
    rss_path: Path,
    corpus_path: Path,
    perf_status: str,
) -> str:
    timing = _load_json(time_path)
    rss = _load_json(rss_path)
    corpus = _load_json(corpus_path)
    return "\t".join(
        map(
            str,
            (
                label,
                kind,
                timing["elapsed"],
                timing["user_seconds"],
                timing["system_seconds"],
                timing["max_rss_kib"],
                rss["aggregate_rss_kib"],
                rss["aggregate_rss_anon_kib"],
                rss["aggregate_rss_file_kib"],
                rss["aggregate_vm_swap_kib"],
                corpus["file_count"],
                corpus["size_bytes"],
                corpus["manifest_sha256"],
                perf_status,
            ),
        )
    )


def _process_tree(root_pid: int) -> set[int]:
    pending = [root_pid]
    observed: set[int] = set()
    while pending:
        pid = pending.pop()
        if pid in observed or not Path(f"/proc/{pid}").exists():
            continue
        observed.add(pid)
        children_path = Path(f"/proc/{pid}/task/{pid}/children")
        try:
            children = children_path.read_text(encoding="ascii").split()
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            children = []
        pending.extend(int(child) for child in children)
    return observed


def _status_kib(pid: int) -> dict[str, int] | None:
    try:
        lines = Path(f"/proc/{pid}/status").read_text(encoding="ascii").splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    wanted = {"VmRSS", "VmHWM", "RssAnon", "RssFile", "VmSwap"}
    result = {name: 0 for name in wanted}
    for line in lines:
        fields = line.split()
        if fields and fields[0] == "State:" and len(fields) >= 2 and fields[1] == "Z":
            return None
        if fields and fields[0].rstrip(":") in wanted and len(fields) >= 2:
            result[fields[0].rstrip(":")] = int(fields[1])
    return result


def monitor_rss(
    pid: int, output_path: Path, summary_path: Path, interval_ms: int
) -> dict[str, Any]:
    if interval_ms < 10:
        raise GateError("RSS sampling interval must be at least 10 milliseconds")
    if output_path.exists() or summary_path.exists():
        raise GateError("refusing to reuse RSS monitor output")
    started = time.monotonic_ns()
    samples = 0
    maxima = {
        "aggregate_rss_kib": 0,
        "aggregate_rss_anon_kib": 0,
        "aggregate_rss_file_kib": 0,
        "aggregate_vm_swap_kib": 0,
        "max_single_process_hwm_kib": 0,
        "process_count": 0,
    }
    with output_path.open("x", encoding="utf-8") as destination:
        destination.write(
            "elapsed_ns\trecorded_at\tprocess_count\trss_kib\trss_anon_kib\t"
            "rss_file_kib\tvm_swap_kib\tmax_single_hwm_kib\tpids\n"
        )
        while True:
            pids = _process_tree(pid)
            statuses = [(item, _status_kib(item)) for item in sorted(pids)]
            statuses = [(item, value) for item, value in statuses if value is not None]
            if not statuses:
                break
            aggregates = {
                "aggregate_rss_kib": sum(value["VmRSS"] for _item, value in statuses),
                "aggregate_rss_anon_kib": sum(
                    value["RssAnon"] for _item, value in statuses
                ),
                "aggregate_rss_file_kib": sum(
                    value["RssFile"] for _item, value in statuses
                ),
                "aggregate_vm_swap_kib": sum(
                    value["VmSwap"] for _item, value in statuses
                ),
                "max_single_process_hwm_kib": max(
                    value["VmHWM"] for _item, value in statuses
                ),
                "process_count": len(statuses),
            }
            for key, value in aggregates.items():
                maxima[key] = max(maxima[key], value)
            elapsed = time.monotonic_ns() - started
            recorded_at = dt.datetime.now(dt.timezone.utc).isoformat()
            destination.write(
                f"{elapsed}\t{recorded_at}\t{len(statuses)}\t"
                f"{aggregates['aggregate_rss_kib']}\t"
                f"{aggregates['aggregate_rss_anon_kib']}\t"
                f"{aggregates['aggregate_rss_file_kib']}\t"
                f"{aggregates['aggregate_vm_swap_kib']}\t"
                f"{aggregates['max_single_process_hwm_kib']}\t"
                f"{','.join(str(item) for item, _value in statuses)}\n"
            )
            destination.flush()
            samples += 1
            time.sleep(interval_ms / 1000)
    if samples == 0:
        raise GateError(f"RSS monitor observed no live process for PID {pid}")
    summary = {"root_pid": pid, "samples": samples, "interval_ms": interval_ms, **maxima}
    _write_json_exclusive(summary_path, summary)
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate-inputs")
    validate.add_argument("--capture", type=Path, required=True)
    validate.add_argument("--template", type=Path, required=True)
    validate.add_argument("--expectations", type=Path, required=True)
    validate.add_argument("--output", type=Path)

    render = subparsers.add_parser("render-config")
    render.add_argument("--template", type=Path, required=True)
    render.add_argument("--output", type=Path, required=True)
    render.add_argument("--capture", type=Path, required=True)
    render.add_argument("--segments-dir", type=Path, required=True)
    render.add_argument("--stop-after-messages", type=int, required=True)

    tree = subparsers.add_parser("tree-manifest")
    tree.add_argument("--corpus", type=Path, required=True)
    tree.add_argument("--manifest", type=Path, required=True)
    tree.add_argument("--inventory", type=Path, required=True)
    tree.add_argument("--summary", type=Path, required=True)

    correctness = subparsers.add_parser("gate-correctness")
    correctness.add_argument("--actual", type=Path, required=True)
    correctness.add_argument("--expectations", type=Path, required=True)

    corpus = subparsers.add_parser("gate-corpus")
    corpus.add_argument("--actual", type=Path, required=True)
    corpus.add_argument("--expectations", type=Path, required=True)

    verifier = subparsers.add_parser("gate-verifier")
    verifier.add_argument("--actual", type=Path, required=True)
    verifier.add_argument("--expectations", type=Path, required=True)

    readbacks = subparsers.add_parser("gate-readbacks")
    readbacks.add_argument("--report", type=Path, required=True)
    readbacks.add_argument("--expectations", type=Path, required=True)
    readbacks.add_argument("--output", type=Path)

    time_parser = subparsers.add_parser("parse-time")
    time_parser.add_argument("--input", type=Path, required=True)
    time_parser.add_argument("--output", type=Path, required=True)

    perf_parser = subparsers.add_parser("parse-perf-stat")
    perf_parser.add_argument("--input", type=Path, required=True)
    perf_parser.add_argument("--output", type=Path, required=True)
    perf_parser.add_argument("--require-event", action="append", default=[])

    rss = subparsers.add_parser("monitor-rss")
    rss.add_argument("--pid", type=int, required=True)
    rss.add_argument("--output", type=Path, required=True)
    rss.add_argument("--summary", type=Path, required=True)
    rss.add_argument("--interval-ms", type=int, default=100)

    summary = subparsers.add_parser("run-summary")
    summary.add_argument("--label", required=True)
    summary.add_argument("--kind", required=True)
    summary.add_argument("--time", type=Path, required=True)
    summary.add_argument("--rss", type=Path, required=True)
    summary.add_argument("--corpus", type=Path, required=True)
    summary.add_argument("--perf-status", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "validate-inputs":
            result = validate_inputs(args.capture, args.template, args.expectations)
            if args.output:
                _write_json_exclusive(args.output, result)
            else:
                print(json.dumps(result, indent=2, sort_keys=True))
        elif args.command == "render-config":
            print(
                json.dumps(
                    render_config(
                        args.template,
                        args.output,
                        args.capture,
                        args.segments_dir,
                        args.stop_after_messages,
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "tree-manifest":
            print(
                json.dumps(
                    write_tree_manifest(
                        args.corpus, args.manifest, args.inventory, args.summary
                    ),
                    sort_keys=True,
                )
            )
        elif args.command == "gate-correctness":
            gate_document(args.actual, args.expectations, "replay_correctness")
        elif args.command == "gate-corpus":
            gate_document(args.actual, args.expectations, "corpus")
        elif args.command == "gate-verifier":
            gate_verifier(args.actual, args.expectations)
        elif args.command == "gate-readbacks":
            print(
                json.dumps(
                    gate_readbacks(args.report, args.expectations, args.output),
                    sort_keys=True,
                )
            )
        elif args.command == "parse-time":
            print(json.dumps(parse_gnu_time(args.input, args.output), sort_keys=True))
        elif args.command == "parse-perf-stat":
            print(
                json.dumps(
                    parse_perf_stat(args.input, args.output, args.require_event),
                    sort_keys=True,
                )
            )
        elif args.command == "monitor-rss":
            print(
                json.dumps(
                    monitor_rss(args.pid, args.output, args.summary, args.interval_ms),
                    sort_keys=True,
                )
            )
        elif args.command == "run-summary":
            print(
                run_summary_row(
                    args.label,
                    args.kind,
                    args.time,
                    args.rss,
                    args.corpus,
                    args.perf_status,
                )
            )
        else:
            raise AssertionError(args.command)
    except (GateError, OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        print(f"Phase 1 replay gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
