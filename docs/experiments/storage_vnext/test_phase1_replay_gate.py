#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import phase1_replay_gate as gate


CONFIG = """[kafka]
topic = "otlp_metrics"

[ingestion]
max_event_age_secs = 3600
max_event_lead_secs = 5
drop_outdated = true
labelset_store = "flat_interned"
replay_from = "/old/capture"
capture_only = false
labelset_report_interval_secs = 10
stop_after_messages = 1000000

[ingestion.head_buffer]
enabled = true
window_duration_secs = 3600
out_of_order_time_window_secs = 3600
float_encoding = "gorilla"
int_encoding = "delta_zig_zag"
varlen_encoding = "raw"
compact_numeric_series = true
adaptive_series_table = true

[ingestion.segment_writer]
enabled = true
segments_dir = "/old/segments"
segment_duration_secs = 900
deterministic_id_seed = 42
storage_schema = "schema8"
float_encoding = "gorilla"
int_encoding = "delta_zig_zag"
varlen_encoding = "raw"
"""


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def minimum_expectations(template_hash: str) -> dict[str, object]:
    return {
        "schema": gate.EXPECTATIONS_SCHEMA,
        "capture": {"manifest_sha256": "", "files": []},
        "config_template_sha256": template_hash,
        "stop_after_messages": 4,
        "corpus": {},
        "replay_correctness": {},
        "storage_verifier": {},
        "readbacks": {},
    }


class Phase1ReplayGateTest(unittest.TestCase):
    def test_validate_inputs_and_render_config_pin_the_workload(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            capture = root / "capture"
            capture.mkdir()
            partition = capture / "partition-1.capture"
            partition.write_bytes(b"capture bytes")
            manifest_document = {
                "version": 2,
                "partitions": [
                    {
                        "partition": 1,
                        "file_name": partition.name,
                        "message_count": 10,
                    }
                ],
            }
            manifest = capture / "manifest.json"
            write_json(manifest, manifest_document)
            template = root / "template.toml"
            template.write_text(CONFIG, encoding="utf-8")
            expected = minimum_expectations(hashlib.sha256(CONFIG.encode()).hexdigest())
            expected["capture"] = {
                "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
                "files": [
                    {
                        "name": partition.name,
                        "sha256": hashlib.sha256(partition.read_bytes()).hexdigest(),
                        "size_bytes": partition.stat().st_size,
                    }
                ],
            }
            expectations = root / "expected.json"
            write_json(expectations, expected)

            observed = gate.validate_inputs(capture, template, expectations)
            output = root / "rendered.toml"
            segments = root / "fresh" / "segments"
            gate.render_config(template, output, capture, segments, 4)

            self.assertEqual(observed["stop_after_messages"], 4)
            with output.open("rb") as source:
                document = gate.tomllib.load(source)
            self.assertEqual(document["ingestion"]["replay_from"], str(capture))
            self.assertEqual(document["ingestion"]["stop_after_messages"], 4)
            self.assertEqual(
                document["ingestion"]["segment_writer"]["segments_dir"], str(segments)
            )
            with self.assertRaisesRegex(gate.GateError, "already exists"):
                gate.render_config(template, output, capture, segments, 4)

    def test_tree_manifest_uses_historical_sha256sum_format(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpus = root / "corpus"
            corpus.mkdir()
            corpus.joinpath("z.bin").write_bytes(b"z")
            nested = corpus / "seg"
            nested.mkdir()
            nested.joinpath("a.bin").write_bytes(b"a")
            manifest = root / "segments.sha256"
            inventory = root / "segments.tsv"
            summary_path = root / "summary.json"

            summary = gate.write_tree_manifest(corpus, manifest, inventory, summary_path)
            a_hash = hashlib.sha256(b"a").hexdigest()
            z_hash = hashlib.sha256(b"z").hexdigest()
            expected_manifest = f"{a_hash}  ./seg/a.bin\n{z_hash}  ./z.bin\n"

            self.assertEqual(manifest.read_text(encoding="utf-8"), expected_manifest)
            self.assertEqual(
                summary["manifest_sha256"],
                hashlib.sha256(expected_manifest.encode()).hexdigest(),
            )
            self.assertEqual(summary["size_bytes"], 2)
            self.assertEqual(summary["file_count"], 2)

    def test_tree_manifest_rejects_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpus = root / "corpus"
            corpus.mkdir()
            corpus.joinpath("file").write_bytes(b"data")
            os.symlink("file", corpus / "alias")
            with self.assertRaisesRegex(gate.GateError, "symbolic link"):
                gate.write_tree_manifest(
                    corpus,
                    root / "manifest",
                    root / "inventory",
                    root / "summary",
                )

    def test_corpus_walk_fails_closed_on_nested_enumeration_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpus = root / "corpus"
            nested = corpus / "nested"
            nested.mkdir(parents=True)
            nested.joinpath("chunk.bin").write_bytes(b"chunk")
            real_scandir = os.scandir

            def synthetic_scandir(path: object):
                if Path(path) == nested:
                    raise PermissionError("synthetic nested denial")
                return real_scandir(path)

            with mock.patch.object(gate.os, "scandir", side_effect=synthetic_scandir):
                with self.assertRaisesRegex(gate.GateError, "cannot enumerate corpus"):
                    gate._corpus_files(corpus)  # noqa: SLF001

    @unittest.skipUnless(hasattr(os, "mkfifo"), "FIFO creation requires POSIX")
    def test_corpus_walk_rejects_non_regular_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory) / "corpus"
            corpus.mkdir()
            os.mkfifo(corpus / "pipe")
            with self.assertRaisesRegex(gate.GateError, "not a regular file"):
                gate._corpus_files(corpus)  # noqa: SLF001

    def test_corpus_walk_rejects_symlink_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            corpus = root / "corpus"
            corpus.mkdir()
            corpus.joinpath("chunk.bin").write_bytes(b"chunk")
            alias = root / "alias"
            alias.symlink_to(corpus, target_is_directory=True)
            with self.assertRaisesRegex(gate.GateError, "non-symlink directory"):
                gate._corpus_files(alias)  # noqa: SLF001

    def test_corpus_walk_rejects_an_escaping_enumerator_name(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            corpus = Path(temporary_directory) / "corpus"
            corpus.mkdir()

            class SyntheticEntry:
                name = "../outside"
                path = str(corpus / name)

            class SyntheticScan:
                def __enter__(self):
                    return iter([SyntheticEntry()])

                def __exit__(self, *_arguments: object) -> None:
                    return None

            with mock.patch.object(gate.os, "scandir", return_value=SyntheticScan()):
                with self.assertRaisesRegex(gate.GateError, "escapes its root"):
                    gate._corpus_files(corpus)  # noqa: SLF001

    def test_exact_correctness_gate_names_the_first_difference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            expected = minimum_expectations("0" * 64)
            expected["replay_correctness"] = {"general": {"Total Messages": 4}}
            expectations = root / "expected.json"
            actual = root / "actual.json"
            write_json(expectations, expected)
            write_json(actual, {"general": {"Total Messages": 3}})
            with self.assertRaisesRegex(gate.GateError, r"Total Messages.*expected 4"):
                gate.gate_document(actual, expectations, "replay_correctness")

    def test_verifier_gate_ignores_runtime_counters_but_pins_fingerprints(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            expected = minimum_expectations("0" * 64)
            expected["storage_verifier"] = {
                "schema_version": 8,
                "verified_selection_fingerprint": "a" * 64,
                "exact_postings": {"logical_fingerprint": "b" * 64},
            }
            expectations = root / "expected.json"
            actual = root / "actual.json"
            write_json(expectations, expected)
            write_json(
                actual,
                {
                    "schema_version": 8,
                    "verified_selection_fingerprint": "a" * 64,
                    "exact_postings": {"logical_fingerprint": "b" * 64},
                    "elapsed_ns": 123,
                },
            )
            gate.gate_verifier(actual, expectations)
            document = json.loads(actual.read_text(encoding="utf-8"))
            document["verified_selection_fingerprint"] = "c" * 64
            write_json(actual, document)
            with self.assertRaisesRegex(gate.GateError, "verified_selection_fingerprint"):
                gate.gate_verifier(actual, expectations)

    def test_readback_gate_pins_counts_and_canonical_rows(self) -> None:
        report = """# Smoke

## PromQL Readbacks

| Kind | Query | result_series |
| --- | --- | ---: |
| Float | `{__name__="metric"}` | 1 |

## Readback Verification

| Metric | Value |
| --- | ---: |
| Checked Queries | 1 |
| Mismatches | 0 |

## Query Diagnostics

| Metric | Value |
| --- | ---: |
| Expected Readback Queries | 1 |
| Executed Readback Queries | 1 |
| Skipped Readback Queries | 0 |
| Isolation Check Skips | 0 |
"""
        rows = [["Float", "`{__name__=\"metric\"}`", "1"]]
        fingerprint = hashlib.sha256(
            json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            expected = minimum_expectations("0" * 64)
            expected["readbacks"] = {
                "expected_queries": 1,
                "executed_queries": 1,
                "skipped_queries": 0,
                "isolation_check_skips": 0,
                "mismatches": 0,
                "promql_rows": 1,
                "promql_rows_fingerprint_sha256": fingerprint,
            }
            expectations = root / "expected.json"
            report_path = root / "report.md"
            write_json(expectations, expected)
            report_path.write_text(report, encoding="utf-8")
            observed = gate.gate_readbacks(report_path, expectations, None)
            self.assertEqual(observed["promql_rows_fingerprint_sha256"], fingerprint)

    def test_gnu_time_parser_retains_machine_metrics(self) -> None:
        report = """Command being timed: "true"
User time (seconds): 1.25
System time (seconds): 0.50
Percent of CPU this job got: 99%
Elapsed (wall clock) time (h:mm:ss or m:ss): 0:01.80
Maximum resident set size (kbytes): 1234
Major (requiring I/O) page faults: 2
Minor (reclaiming a frame) page faults: 3
Voluntary context switches: 4
Involuntary context switches: 5
File system inputs: 6
File system outputs: 7
Exit status: 0
"""
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "time.txt"
            output = root / "time.json"
            source.write_text(report, encoding="utf-8")
            parsed = gate.parse_gnu_time(source, output)
            self.assertEqual(parsed["elapsed"], "0:01.80")
            self.assertEqual(parsed["max_rss_kib"], 1234)
            self.assertEqual(parsed["filesystem_outputs"], 7)

    def test_perf_parser_rejects_an_unavailable_required_counter(self) -> None:
        report = """# started
1.5\tmsec\ttask-clock\t100\t100.00
<not counted>\t\tcache-misses\t0\t0.00
"""
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = root / "perf.tsv"
            source.write_text(report, encoding="utf-8")
            with self.assertRaisesRegex(gate.GateError, "unavailable.*cache-misses"):
                gate.parse_perf_stat(
                    source, root / "perf.json", ["task-clock", "cache-misses"]
                )

    def test_rss_monitor_observes_a_process_until_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            process = subprocess.Popen(["sleep", "0.05"])
            try:
                summary = gate.monitor_rss(
                    process.pid, root / "rss.tsv", root / "rss.json", 10
                )
            finally:
                process.wait()
            self.assertGreater(summary["samples"], 0)
            self.assertGreaterEqual(summary["process_count"], 1)
            self.assertGreater(summary["root_starttime_ticks"], 0)
            self.assertIn("rss_kib", (root / "rss.tsv").read_text(encoding="utf-8"))

    def test_rss_monitor_rejects_root_pid_reuse(self) -> None:
        status = {
            "VmRSS": 10,
            "VmHWM": 10,
            "RssAnon": 8,
            "RssFile": 2,
            "VmSwap": 0,
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with (
                mock.patch.object(
                    gate,
                    "_process_starttime_ticks",
                    side_effect=(100, 100, 101),
                ),
                mock.patch.object(
                    gate, "_process_tree", return_value={123}
                ),
                mock.patch.object(gate, "_status_kib", return_value=status),
            ):
                with self.assertRaisesRegex(
                    gate.GateError, "root PID identity changed"
                ):
                    gate.monitor_rss(
                        123,
                        root / "rss.tsv",
                        root / "rss.json",
                        10,
                    )


if __name__ == "__main__":
    unittest.main()
