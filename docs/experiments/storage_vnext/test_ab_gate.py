#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

import ab_gate


REPLAY_REPORT = """# Ingestion Statistics

## General Stats

| Metric | Value |
|---|---|
| Total Messages | 20 |
| Total OTLP Metric Records | 50 |
| Total Unique Metrics (`__name__`) | 5 |
| Total Series (unique label sets) | 12 |
| Observed OTLP Datapoints | 100 |
| Accepted Datapoints | 96 |
| Total Processing Time | 9.99s |
| Total Intern Time | 8.88s |
| Skipped Non-Scalar | 2 |
| Recorded Samples | 95 |
| Missing Number Value | 1 |
| Invalid Typed Value | 0 |

## Datapoint Policy Counts

| Outcome | Total | Window |
|---|---:|---:|
| Observed | 100 | 0 |
| Time-Policy Accepted | 96 | 0 |
| Dropped Too Old | 3 | 0 |
| Dropped Too Future | 1 | 0 |
| Missing Timestamp | 0 | 0 |
| Rejected Total | 4 | 0 |

## Datapoint Storage Counts

| Outcome | Total | Window |
|---|---:|---:|
| Time-Policy Accepted | 96 | 0 |
| Recorded Samples | 95 | 0 |
| Missing Number Value | 1 | 0 |
| Invalid Typed Value | 0 | 0 |
| Accepted Not Recorded | 1 | 0 |

## Event Time Skew

| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |
|---|---|---|---|---|---|---|---|---|---|
| All Timestamped | 100 | -5.0 | 2.0 | -100 | 20 | -2 | -1 | 1 | 2 |
| Accepted | 96 | -3.0 | 1.0 | -50 | 5 | -2 | -1 | 1 | 2 |
| Dropped Too Old | 3 | -90.0 | 1.0 | -100 | -80 | -90 | -90 | -80 | -80 |
| Dropped Too Future | 1 | 20.0 | 0.0 | 20 | 20 | 20 | 20 | 20 | 20 |

## OTLP Data Type Counts

| Type | Metric Records | Observed Datapoints | Accepted Datapoints |
|---|---:|---:|---:|
| Gauge | 10 | 20 | 18 |
| Sum | 10 | 20 | 19 |
| Histogram | 10 | 20 | 20 |
| Exponential Histogram | 10 | 20 | 20 |
| Summary | 10 | 20 | 19 |

## Partition Watermarks

| Metric | Value |
|---|---|
| Tracked Messages | 20 |
| Tracked Datapoints | 100 |
| Missing Timestamp Messages | 0 |
| Missing Timestamp Datapoints | 0 |
| Overall Min TS | 2026-07-02T08:04:06.257Z |
| Overall Max TS | 2026-07-02T08:20:13.606Z |
| Overall Window | 00:16:07.349 (967349ms) |

### Latency Statistics

| Metric | Count | Mean |
|---|---:|---:|
| Message Total | 20 | 999s |
"""


def readback_report(executed: int = 26, skipped: int = 16, mismatches: int = 0) -> str:
    expected = executed + skipped
    return f"""# Query Smoke

## Readback Verification

| Metric | Value |
| --- | ---: |
| Checked Queries | {executed} |
| Mismatches | {mismatches} |

## Query Diagnostics

| Phase | Duration |
| --- | ---: |
| Store Open | 99s |

| Metric | Value |
| --- | ---: |
| Expected Readback Queries | {expected} |
| Executed Readback Queries | {executed} |
| Skipped Readback Queries | {skipped} |
| Isolation Check Skips | {skipped} |
"""


class AbGateTest(unittest.TestCase):
    def write(self, directory: Path, name: str, contents: str) -> Path:
        path = directory / name
        path.write_text(contents, encoding="utf-8")
        return path

    def test_replay_parser_retains_counters_and_ranges_but_ignores_latency(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first = ab_gate.parse_replay_report(self.write(root, "first.md", REPLAY_REPORT))
            second = ab_gate.parse_replay_report(
                self.write(root, "second.md", REPLAY_REPORT.replace("9.99s", "1234s"))
            )
        self.assertEqual(first, second)
        self.assertEqual(first["general"]["Accepted Datapoints"], 96)
        self.assertEqual(first["event_time_skew_ranges"]["Accepted"]["min_ms"], -50)
        self.assertEqual(
            first["partition_watermarks"]["Overall Max TS"],
            "2026-07-02T08:20:13.606Z",
        )

    def test_replay_parser_omits_exactly_zero_skew_outcomes(self) -> None:
        zero_old_report = (
            REPLAY_REPORT.replace(
                "| Dropped Too Old | 3 | 0 |", "| Dropped Too Old | 0 | 0 |"
            )
            .replace(
                "| Dropped Too Future | 1 | 0 |",
                "| Dropped Too Future | 4 | 0 |",
            )
            .replace(
                "| Dropped Too Old | 3 | -90.0 | 1.0 | -100 | -80 | -90 | -90 | -80 | -80 |\n",
                "",
            )
            .replace(
                "| Dropped Too Future | 1 | 20.0 | 0.0 | 20 | 20 | 20 | 20 | 20 | 20 |",
                "| Dropped Too Future | 4 | 20.0 | 0.0 | 20 | 20 | 20 | 20 | 20 | 20 |",
            )
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            parsed = ab_gate.parse_replay_report(
                self.write(root, "zero-old.md", zero_old_report)
            )
            self.assertNotIn("Dropped Too Old", parsed["event_time_skew_ranges"])

            stale_zero_row = zero_old_report.replace(
                "| Dropped Too Future | 4 | 20.0 | 0.0 | 20 | 20 | 20 | 20 | 20 | 20 |",
                "| Dropped Too Old | 0 | 0.0 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 |\n"
                "| Dropped Too Future | 4 | 20.0 | 0.0 | 20 | 20 | 20 | 20 | 20 | 20 |",
            )
            with self.assertRaisesRegex(ab_gate.GateError, "non-zero policy outcomes"):
                ab_gate.parse_replay_report(
                    self.write(root, "stale-zero-row.md", stale_zero_row)
                )

            missing_positive_row = REPLAY_REPORT.replace(
                "| Dropped Too Old | 3 | -90.0 | 1.0 | -100 | -80 | -90 | -90 | -80 | -80 |\n",
                "",
            )
            with self.assertRaisesRegex(ab_gate.GateError, "non-zero policy outcomes"):
                ab_gate.parse_replay_report(
                    self.write(root, "missing-positive-row.md", missing_positive_row)
                )

            wrong_positive_count = REPLAY_REPORT.replace(
                "| Dropped Too Old | 3 | -90.0 | 1.0 | -100 | -80 | -90 | -90 | -80 | -80 |",
                "| Dropped Too Old | 2 | -90.0 | 1.0 | -100 | -80 | -90 | -90 | -80 | -80 |",
            )
            with self.assertRaisesRegex(ab_gate.GateError, "count differs"):
                ab_gate.parse_replay_report(
                    self.write(root, "wrong-positive-count.md", wrong_positive_count)
                )

    def test_readback_requires_an_explicit_exact_isolation_waiver(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            report = self.write(Path(temporary_directory), "readback.md", readback_report())
            with self.assertRaisesRegex(ab_gate.GateError, "require.*isolation_check"):
                ab_gate.parse_readback(report, None, None, None)
            with self.assertRaisesRegex(ab_gate.GateError, "exactly equal"):
                ab_gate.parse_readback(report, "isolation_check", 15, "prefix isolation")
            parsed = ab_gate.parse_readback(
                report, "isolation_check", 16, "prefix isolation safety check"
            )
        self.assertEqual(parsed["status"], "coverage_gap_waived")
        self.assertEqual(parsed["executed_queries"], 26)

    def test_readback_rejects_zero_execution_and_mismatches(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            zero = self.write(root, "zero.md", readback_report(executed=0, skipped=0))
            mismatch = self.write(
                root, "mismatch.md", readback_report(executed=1, skipped=0, mismatches=1)
            )
            with self.assertRaisesRegex(ab_gate.GateError, "zero queries"):
                ab_gate.parse_readback(zero, None, None, None)
            with self.assertRaisesRegex(ab_gate.GateError, "contains 1 mismatches"):
                ab_gate.parse_readback(mismatch, None, None, None)

    def test_semantic_gate_requires_nonempty_results(self) -> None:
        document = {
            "corpus_fingerprint_sha256": "corpus",
            "runs": [
                {
                    "semantic_fingerprint_sha256": "semantic",
                    "portable_semantic_fingerprint_sha256": "portable",
                    "result_series": 0,
                    "result_samples": 0,
                }
            ],
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "semantic.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(ab_gate.GateError, "returned no results"):
                ab_gate.semantic_summary("v7-a", "v7", path)
            document["runs"][0]["result_series"] = 2
            document["runs"][0]["result_samples"] = 2
            path.write_text(json.dumps(document), encoding="utf-8")
            row = ab_gate.semantic_summary("v7-a", "v7", path)
        self.assertTrue(row.endswith("\t2\t2"))

    def test_cross_format_gate_only_allows_symbols_and_footer_differences(self) -> None:
        header = ("sha256", "size_bytes", "path")
        baseline_rows = (
            ("same", 10, "seg-a/chunks.bin"),
            ("old-symbols", 20, "seg-a/symbols.bin"),
            ("old-footer", 30, "seg-a/footer.bin"),
        )
        candidate_rows = (
            ("same", 10, "seg-a/chunks.bin"),
            ("new-symbols", 18, "seg-a/symbols.bin"),
            ("new-footer", 30, "seg-a/footer.bin"),
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            baseline = root / "baseline.tsv"
            candidate = root / "candidate.tsv"
            for path, rows in ((baseline, baseline_rows), (candidate, candidate_rows)):
                with path.open("w", newline="", encoding="utf-8") as destination:
                    writer = csv.writer(destination, delimiter="\t", lineterminator="\n")
                    writer.writerow(header)
                    writer.writerows(rows)
            output = root / "allowed.tsv"
            ab_gate.compare_cross_format_files(
                baseline,
                candidate,
                output,
                {"symbols.bin", "footer.bin"},
                {"symbols.bin", "footer.bin"},
            )
            allowed = output.read_text(encoding="utf-8")
            candidate_rows = tuple(
                ("changed", size, path) if path.endswith("chunks.bin") else (sha, size, path)
                for sha, size, path in candidate_rows
            )
            with candidate.open("w", newline="", encoding="utf-8") as destination:
                writer = csv.writer(destination, delimiter="\t", lineterminator="\n")
                writer.writerow(header)
                writer.writerows(candidate_rows)
            with self.assertRaisesRegex(ab_gate.GateError, "unexpected.*chunks.bin"):
                ab_gate.compare_cross_format_files(
                    baseline,
                    candidate,
                    root / "rejected.tsv",
                    {"symbols.bin", "footer.bin"},
                    {"symbols.bin", "footer.bin"},
                )
        self.assertIn("seg-a/symbols.bin", allowed)
        self.assertIn("seg-a/footer.bin", allowed)
        self.assertNotIn("seg-a/chunks.bin", allowed)


if __name__ == "__main__":
    unittest.main()
