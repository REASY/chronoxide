#!/usr/bin/env python3
"""Parse and enforce correctness gates for the storage-format A/B harness."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any


class GateError(ValueError):
    pass


def _section(text: str, title: str) -> str:
    match = re.search(rf"(?m)^## {re.escape(title)}\s*$", text)
    if match is None:
        raise GateError(f"missing markdown section: {title}")
    following = text[match.end() :]
    next_section = re.search(r"(?m)^## ", following)
    if next_section is not None:
        following = following[: next_section.start()]
    return following


def _markdown_rows(section: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if not cells or all(re.fullmatch(r":?-+:?", cell) for cell in cells):
            continue
        rows.append(cells)
    return rows


def _two_column_values(text: str, title: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for cells in _markdown_rows(_section(text, title)):
        if len(cells) != 2 or cells == ["Metric", "Value"]:
            continue
        values[cells[0]] = cells[1]
    return values


def _integer(value: str, name: str) -> int:
    if not re.fullmatch(r"[0-9]+", value):
        raise GateError(f"{name} is not a non-negative integer: {value!r}")
    return int(value)


def _required(values: dict[str, str], name: str) -> str:
    try:
        return values[name]
    except KeyError:
        raise GateError(f"missing required metric: {name}") from None


def parse_replay_report(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    general_values = _two_column_values(text, "General Stats")
    general_names = (
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
    )
    general = {
        name: _integer(_required(general_values, name), f"General Stats/{name}")
        for name in general_names
    }

    policy: dict[str, int] = {}
    for cells in _markdown_rows(_section(text, "Datapoint Policy Counts")):
        if len(cells) != 3 or cells[0] == "Outcome":
            continue
        policy[cells[0]] = _integer(cells[1], f"Datapoint Policy Counts/{cells[0]}")
    for name in (
        "Observed",
        "Time-Policy Accepted",
        "Dropped Too Old",
        "Dropped Too Future",
        "Missing Timestamp",
        "Rejected Total",
    ):
        _required({key: str(value) for key, value in policy.items()}, name)

    storage: dict[str, int] = {}
    for cells in _markdown_rows(_section(text, "Datapoint Storage Counts")):
        if len(cells) != 3 or cells[0] == "Outcome":
            continue
        storage[cells[0]] = _integer(cells[1], f"Datapoint Storage Counts/{cells[0]}")
    for name in (
        "Time-Policy Accepted",
        "Recorded Samples",
        "Missing Number Value",
        "Invalid Typed Value",
        "Accepted Not Recorded",
    ):
        _required({key: str(value) for key, value in storage.items()}, name)

    datapoint_types: dict[str, dict[str, int]] = {}
    for cells in _markdown_rows(_section(text, "OTLP Data Type Counts")):
        if len(cells) != 4 or cells[0] == "Type":
            continue
        datapoint_types[cells[0]] = {
            "metric_records": _integer(cells[1], f"OTLP Data Type Counts/{cells[0]}/records"),
            "observed_datapoints": _integer(
                cells[2], f"OTLP Data Type Counts/{cells[0]}/observed"
            ),
            "accepted_datapoints": _integer(
                cells[3], f"OTLP Data Type Counts/{cells[0]}/accepted"
            ),
        }
    for name in ("Gauge", "Sum", "Histogram", "Exponential Histogram", "Summary"):
        if name not in datapoint_types:
            raise GateError(f"missing OTLP data type row: {name}")

    event_time_skew: dict[str, dict[str, int]] = {}
    for cells in _markdown_rows(_section(text, "Event Time Skew")):
        if len(cells) != 10 or cells[0] == "Metric":
            continue
        event_time_skew[cells[0]] = {
            "count": _integer(cells[1], f"Event Time Skew/{cells[0]}/count"),
            "min_ms": int(cells[4]),
            "max_ms": int(cells[5]),
        }
    expected_skew_counts = {
        "All Timestamped": policy["Observed"] - policy["Missing Timestamp"],
        "Accepted": policy["Time-Policy Accepted"],
        "Dropped Too Old": policy["Dropped Too Old"],
        "Dropped Too Future": policy["Dropped Too Future"],
    }
    expected_skew_rows = {
        name for name, count in expected_skew_counts.items() if count > 0
    }
    actual_skew_rows = set(event_time_skew)
    if actual_skew_rows != expected_skew_rows:
        raise GateError(
            "event-time skew rows differ from non-zero policy outcomes; "
            f"missing={sorted(expected_skew_rows - actual_skew_rows)!r}, "
            f"extra={sorted(actual_skew_rows - expected_skew_rows)!r}"
        )
    for name, row in event_time_skew.items():
        if row["count"] != expected_skew_counts[name]:
            raise GateError(
                f"event-time skew count differs from policy outcome: {name}"
            )
        if row["min_ms"] > row["max_ms"]:
            raise GateError(f"event-time skew range is reversed: {name}")

    watermark_values = _two_column_values(text, "Partition Watermarks")
    watermark_integer_names = (
        "Tracked Messages",
        "Tracked Datapoints",
        "Missing Timestamp Messages",
        "Missing Timestamp Datapoints",
    )
    partition_watermarks: dict[str, Any] = {
        name: _integer(
            _required(watermark_values, name), f"Partition Watermarks/{name}"
        )
        for name in watermark_integer_names
    }
    for name in ("Overall Min TS", "Overall Max TS", "Overall Window"):
        partition_watermarks[name] = _required(watermark_values, name)

    if general["Observed OTLP Datapoints"] != policy["Observed"]:
        raise GateError("general and policy observed-datapoint counters disagree")
    if general["Accepted Datapoints"] != policy["Time-Policy Accepted"]:
        raise GateError("general and policy accepted-datapoint counters disagree")
    if general["Recorded Samples"] != storage["Recorded Samples"]:
        raise GateError("general and storage recorded-sample counters disagree")
    if general["Missing Number Value"] != storage["Missing Number Value"]:
        raise GateError("general and storage missing-number-value counters disagree")
    if general["Invalid Typed Value"] != storage["Invalid Typed Value"]:
        raise GateError("general and storage invalid-typed-value counters disagree")
    if storage["Accepted Not Recorded"] != (
        storage["Missing Number Value"] + storage["Invalid Typed Value"]
    ):
        raise GateError(
            "accepted-not-recorded differs from missing-number plus invalid-typed values"
        )
    if general["Total Messages"] != partition_watermarks["Tracked Messages"]:
        raise GateError("total and partition-tracked message counters disagree")

    return {
        "schema": "chronoxide/storage-vnext-replay-correctness/v2",
        "general": general,
        "datapoint_policy_totals": policy,
        "datapoint_storage_totals": storage,
        "otlp_data_type_counts": datapoint_types,
        "event_time_skew_ranges": event_time_skew,
        "partition_watermarks": partition_watermarks,
    }


def write_replay_json(report: Path, output: Path) -> None:
    parsed = parse_replay_report(report)
    with output.open("x", encoding="utf-8") as destination:
        json.dump(parsed, destination, indent=2, sort_keys=True)
        destination.write("\n")


def replay_summary(label: str, implementation: str, parsed_path: Path) -> str:
    parsed = json.loads(parsed_path.read_text(encoding="utf-8"))
    canonical = json.dumps(parsed, separators=(",", ":"), sort_keys=True).encode()
    fingerprint = hashlib.sha256(canonical).hexdigest()
    general = parsed["general"]
    policy = parsed["datapoint_policy_totals"]
    watermark = parsed["partition_watermarks"]
    accepted_skew = parsed["event_time_skew_ranges"]["Accepted"]
    return "\t".join(
        map(
            str,
            (
                label,
                implementation,
                fingerprint,
                general["Total Messages"],
                general["Observed OTLP Datapoints"],
                general["Accepted Datapoints"],
                general["Recorded Samples"],
                policy["Dropped Too Old"],
                policy["Dropped Too Future"],
                policy["Missing Timestamp"],
                watermark["Overall Min TS"],
                watermark["Overall Max TS"],
                accepted_skew["min_ms"],
                accepted_skew["max_ms"],
            ),
        )
    )


def parse_readback(
    path: Path,
    waiver_kind: str | None,
    waiver_count: int | None,
    waiver_reason: str | None,
) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    verification = _two_column_values(text, "Readback Verification")
    diagnostics = _two_column_values(text, "Query Diagnostics")
    checked = _integer(_required(verification, "Checked Queries"), "Checked Queries")
    mismatches = _integer(_required(verification, "Mismatches"), "Mismatches")
    expected = _integer(
        _required(diagnostics, "Expected Readback Queries"), "Expected Readback Queries"
    )
    executed = _integer(
        _required(diagnostics, "Executed Readback Queries"), "Executed Readback Queries"
    )
    skipped = _integer(
        _required(diagnostics, "Skipped Readback Queries"), "Skipped Readback Queries"
    )
    isolation_skips = _integer(
        _required(diagnostics, "Isolation Check Skips"), "Isolation Check Skips"
    )
    if checked != executed:
        raise GateError(f"checked ({checked}) and executed ({executed}) queries differ")
    if executed <= 0:
        raise GateError("readback gate executed zero queries")
    if expected != executed + skipped:
        raise GateError("expected queries do not equal executed plus skipped queries")
    if mismatches != 0:
        raise GateError(f"readback report contains {mismatches} mismatches")
    status = "pass"
    if skipped:
        if waiver_kind != "isolation_check":
            raise GateError(
                "skipped readbacks require --skip-waiver-kind isolation_check"
            )
        if waiver_count is None or waiver_count != skipped:
            raise GateError(
                f"skip waiver count must exactly equal observed skips ({skipped})"
            )
        if isolation_skips != skipped:
            raise GateError(
                "isolation_check waiver cannot cover non-isolation readback skips"
            )
        if waiver_reason is None or not waiver_reason.strip():
            raise GateError("skipped readbacks require a non-empty waiver reason")
        status = "coverage_gap_waived"
    elif isolation_skips != 0:
        raise GateError("isolation skips are nonzero while skipped queries are zero")
    return {
        "expected_queries": expected,
        "executed_queries": executed,
        "skipped_queries": skipped,
        "isolation_check_skips": isolation_skips,
        "mismatches": mismatches,
        "status": status,
        "waiver_kind": waiver_kind or "",
        "waiver_count": waiver_count if waiver_count is not None else "",
        "waiver_reason": waiver_reason or "",
    }


def readback_summary(
    label: str,
    implementation: str,
    report: Path,
    waiver_kind: str | None,
    waiver_count: int | None,
    waiver_reason: str | None,
) -> str:
    parsed = parse_readback(report, waiver_kind, waiver_count, waiver_reason)
    return "\t".join(
        map(
            str,
            (
                label,
                implementation,
                parsed["expected_queries"],
                parsed["executed_queries"],
                parsed["skipped_queries"],
                parsed["isolation_check_skips"],
                parsed["mismatches"],
                parsed["status"],
                parsed["waiver_kind"],
                parsed["waiver_count"],
                parsed["waiver_reason"],
            ),
        )
    )


def semantic_summary(label: str, implementation: str, path: Path) -> str:
    with path.open(encoding="utf-8") as source:
        document = json.load(source)
    runs = document.get("runs")
    if not isinstance(runs, list) or len(runs) != 1:
        raise GateError("semantic benchmark must contain exactly one run")
    run = runs[0]
    result_series = int(run["result_series"])
    result_samples = int(run["result_samples"])
    if result_series <= 0 or result_samples <= 0:
        raise GateError(
            "semantic query returned no results; both result_series and result_samples must be positive"
        )
    return "\t".join(
        map(
            str,
            (
                label,
                implementation,
                document["corpus_fingerprint_sha256"],
                run["semantic_fingerprint_sha256"],
                run["portable_semantic_fingerprint_sha256"],
                result_series,
                result_samples,
            ),
        )
    )


def _read_file_manifest(path: Path) -> dict[str, tuple[str, int]]:
    with path.open(newline="", encoding="utf-8") as source:
        rows = csv.DictReader(source, delimiter="\t")
        if rows.fieldnames != ["sha256", "size_bytes", "path"]:
            raise GateError(f"invalid file-manifest header: {path}")
        result: dict[str, tuple[str, int]] = {}
        for row in rows:
            relative = row["path"]
            if relative in result:
                raise GateError(f"duplicate file-manifest path: {relative}")
            result[relative] = (row["sha256"], int(row["size_bytes"]))
    return result


def compare_cross_format_files(
    baseline_path: Path,
    candidate_path: Path,
    output_path: Path,
    allowed_artifacts: set[str],
    required_different_artifacts: set[str],
) -> None:
    baseline = _read_file_manifest(baseline_path)
    candidate = _read_file_manifest(candidate_path)
    if baseline.keys() != candidate.keys():
        missing = sorted(baseline.keys() - candidate.keys())
        extra = sorted(candidate.keys() - baseline.keys())
        raise GateError(
            f"cross-format file paths differ; missing={missing!r}, extra={extra!r}"
        )
    differences: list[tuple[str, str, str, int, str, int]] = []
    observed_artifacts: set[str] = set()
    for relative in sorted(baseline):
        baseline_hash, baseline_size = baseline[relative]
        candidate_hash, candidate_size = candidate[relative]
        if (baseline_hash, baseline_size) == (candidate_hash, candidate_size):
            continue
        artifact = Path(relative).name
        if artifact not in allowed_artifacts:
            raise GateError(f"unexpected cross-format byte difference: {relative}")
        observed_artifacts.add(artifact)
        differences.append(
            (
                relative,
                artifact,
                baseline_hash,
                baseline_size,
                candidate_hash,
                candidate_size,
            )
        )
    missing_required = required_different_artifacts - observed_artifacts
    if missing_required:
        raise GateError(
            "required cross-format differences were absent for: "
            + ", ".join(sorted(missing_required))
        )
    with output_path.open("x", newline="", encoding="utf-8") as destination:
        writer = csv.writer(destination, delimiter="\t", lineterminator="\n")
        writer.writerow(
            (
                "path",
                "artifact",
                "baseline_sha256",
                "baseline_size_bytes",
                "candidate_sha256",
                "candidate_size_bytes",
                "allowance",
            )
        )
        for difference in differences:
            writer.writerow((*difference, "storage-vNext symbols/footer format change"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    replay = subparsers.add_parser("replay-report")
    replay.add_argument("--report", type=Path, required=True)
    replay.add_argument("--output", type=Path, required=True)

    replay_row = subparsers.add_parser("replay-summary")
    replay_row.add_argument("--label", required=True)
    replay_row.add_argument("--implementation", required=True)
    replay_row.add_argument("--parsed", type=Path, required=True)

    readback = subparsers.add_parser("readback")
    readback.add_argument("--label", required=True)
    readback.add_argument("--implementation", required=True)
    readback.add_argument("--report", type=Path, required=True)
    readback.add_argument("--skip-waiver-kind")
    readback.add_argument("--skip-waiver-count", type=int)
    readback.add_argument("--skip-waiver-reason")

    semantic = subparsers.add_parser("semantic")
    semantic.add_argument("--label", required=True)
    semantic.add_argument("--implementation", required=True)
    semantic.add_argument("--raw", type=Path, required=True)

    cross_files = subparsers.add_parser("cross-format-files")
    cross_files.add_argument("--baseline", type=Path, required=True)
    cross_files.add_argument("--candidate", type=Path, required=True)
    cross_files.add_argument("--output", type=Path, required=True)
    cross_files.add_argument("--allow-artifact", action="append", default=[])
    cross_files.add_argument("--require-difference", action="append", default=[])
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "replay-report":
            write_replay_json(args.report, args.output)
        elif args.command == "replay-summary":
            print(replay_summary(args.label, args.implementation, args.parsed))
        elif args.command == "readback":
            print(
                readback_summary(
                    args.label,
                    args.implementation,
                    args.report,
                    args.skip_waiver_kind,
                    args.skip_waiver_count,
                    args.skip_waiver_reason,
                )
            )
        elif args.command == "semantic":
            print(semantic_summary(args.label, args.implementation, args.raw))
        elif args.command == "cross-format-files":
            compare_cross_format_files(
                args.baseline,
                args.candidate,
                args.output,
                set(args.allow_artifact),
                set(args.require_difference),
            )
        else:
            raise AssertionError(args.command)
    except (GateError, KeyError, TypeError, ValueError, OSError, json.JSONDecodeError) as error:
        print(f"storage format A/B gate: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
