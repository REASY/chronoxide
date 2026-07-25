#!/usr/bin/env python3

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import live_query_prefix_oracle as oracle


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def stats(segments_queried: int) -> dict[str, int]:
    return {
        field: (
            segments_queried
            if field in {"segments_considered", "segments_queried"}
            else 1
            if field in {"matched_series", "projected_series", "samples_decoded"}
            else 0
        )
        for field in oracle.QUERY_STATS_FIELDS
    }


def query_io() -> dict[str, int]:
    return {field: 0 for field in oracle.QUERY_IO_FIELDS}


class PrefixOracleTests(unittest.TestCase):
    def make_result(self, root: Path) -> tuple[Path, dict[str, object]]:
        result = root / "measured"
        (result / "comparisons").mkdir(parents=True)
        (result / "metadata/harness").mkdir(parents=True)
        (result / "runs/Q").mkdir(parents=True)
        (result / "configs").mkdir(parents=True)
        (result / "COMPLETE").touch()
        write_json(
            result / "comparisons/dpq-gate.json",
            {
                "schema": oracle.RUN_SET_SCHEMA,
                "complete": True,
                "storage_trees_equal": True,
                "replay_counters_equal": True,
                "live_head_only_observed": True,
                "expected_messages": 100,
            },
        )
        query = {
            "name": "head",
            "mode": "instant",
            "query": 'last_over_time(foo{service="a"}[1m])',
            "time": "1234.5",
            "require_nonempty": True,
            "require_empty": False,
        }
        write_json(
            result / "metadata/harness/live_query_ingest_queries.json",
            {
                "schema": oracle.WORKLOAD_SCHEMA,
                "client": {},
                "queries": [query],
            },
        )
        data = {
            "resultType": "vector",
            "result": [{"metric": {"service": "a"}, "value": [1234.5, "7"]}],
        }
        record = {
            "schema": oracle.CLIENT_SCHEMA,
            "query_name": "head",
            "mode": "instant",
            "generation": 2,
            "visible_message_sequence": 40,
            "catalog_revision": 9,
            "response_data_sha256": oracle.canonical_sha256(data),
            "cardinality": 1,
            "samples": 1,
            "query_stats": stats(0),
        }
        records = [record, dict(record)]
        records_path = result / "runs/Q/client-records.jsonl"
        records_path.write_text(
            "".join(json.dumps(item, sort_keys=True) + "\n" for item in records),
            encoding="utf-8",
        )
        write_json(
            result / "runs/Q/client-summary.json",
            {"records_fingerprint_sha256": oracle.canonical_sha256(records)},
        )
        (result / "runs/Q/ingester.log").write_text(
            "INFO chronoxide_live_metrics "
            'event="publication" outcome="success" generation=2 '
            "visible_message_sequence=40 catalog_revision=9\n",
            encoding="utf-8",
        )
        write_json(
            result / "metadata/validated-inputs.json",
            {"capture": "/tmp/frozen-capture"},
        )
        (result / "configs/Q.toml").write_text(
            self.q_config(result / "runs/Q/segments"), encoding="utf-8"
        )
        return result, record

    @staticmethod
    def q_config(segments: Path) -> str:
        return f"""[kafka]
brokers = "unused"
group_id = "unused"
topics = ["unused"]

[ingestion]
replay_from = "/tmp/frozen-capture"
stop_after_messages = 100

[ingestion.segment_writer]
enabled = true
segments_dir = "{segments}"
storage_schema = "schema8"

[api]
enabled = true
listen = "127.0.0.1:19091"
head_publish_interval_ms = 1000
max_view_staleness_ms = 10000
live_memory_admission_bytes = 1024
max_concurrent_queries = 4
query_max_series_matched = 1000000
query_max_projected_series = 2000000
query_max_chunks_read = 5000000
query_max_bytes_read = 2147483648
query_max_samples = 50000000
regex_max_expanded_values = 100000
chunk_read_mode = "pread"
chunk_read_queue_depth = 128
chunk_payload_coalesce_max_gap_bytes = 4096
experimental_cross_segment_chunk_reads = false
range_scalar_cache_max_bytes = 0
"""

    def test_canonical_hash_retains_result_array_order(self) -> None:
        left = {"result": [{"metric": {"x": "1"}}, {"metric": {"x": "2"}}]}
        right = {"result": list(reversed(left["result"]))}
        self.assertNotEqual(
            oracle.canonical_sha256(left), oracle.canonical_sha256(right)
        )

    def test_selects_observed_nonempty_head_only_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result, record = self.make_result(root)
            output = root / "selection.json"
            selected = oracle.select_candidate(result, output)
            self.assertEqual(selected["visible_message_sequence"], 40)
            self.assertEqual(selected["generation"], 2)
            self.assertEqual(selected["live_response"]["response_data_sha256"], record["response_data_sha256"])
            self.assertEqual(
                selected["head_only_evidence"]["same_generation_query_observations"],
                2,
            )
            self.assertFalse(selected["independent_promql_evaluator"])
            self.assertTrue(selected["ordering_sensitive_fail_closed"])

    def test_selection_rejects_non_head_result(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result, _record = self.make_result(root)
            records_path = result / "runs/Q/client-records.jsonl"
            records = [
                json.loads(line)
                for line in records_path.read_text(encoding="utf-8").splitlines()
            ]
            for record in records:
                record["query_stats"] = stats(1)
            records_path.write_text(
                "".join(json.dumps(item, sort_keys=True) + "\n" for item in records),
                encoding="utf-8",
            )
            write_json(
                result / "runs/Q/client-summary.json",
                {"records_fingerprint_sha256": oracle.canonical_sha256(records)},
            )
            with self.assertRaisesRegex(oracle.GateError, "zero sealed segments"):
                oracle.select_candidate(result, root / "selection.json")

    def test_prefix_config_changes_only_three_controls_and_forces_zero_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result, _record = self.make_result(root)
            selection_path = root / "selection.json"
            oracle.select_candidate(result, selection_path)
            segments = root / "oracle/segments"
            output = root / "oracle/prefix.toml"
            gate = root / "oracle/config-gate.json"
            value = oracle.render_prefix_config(
                result, selection_path, segments, output, gate
            )
            self.assertEqual(
                set(value["exact_changed_fields"]),
                {
                    "api.enabled",
                    "ingestion.segment_writer.segments_dir",
                    "ingestion.stop_after_messages",
                },
            )
            with output.open("rb") as source:
                config = __import__("tomllib").load(source)
            self.assertFalse(config["api"]["enabled"])
            self.assertEqual(config["api"]["range_scalar_cache_max_bytes"], 0)
            self.assertEqual(config["ingestion"]["stop_after_messages"], 40)

    def test_prefix_replay_counters_must_reconcile(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result, _record = self.make_result(root)
            selection_path = root / "selection.json"
            oracle.select_candidate(result, selection_path)
            replay = {
                "schema": oracle.REPLAY_REPORT_SCHEMA,
                "general": {"Total Messages": 40, "Recorded Samples": 7},
                "datapoint_policy_totals": {
                    "Observed": 8,
                    "Time-Policy Accepted": 7,
                    "Missing Timestamp": 1,
                },
                "datapoint_storage_totals": {
                    "Recorded Samples": 7,
                    "Accepted Not Recorded": 0,
                    "Missing Number Value": 0,
                    "Invalid Typed Value": 0,
                },
                "partition_watermarks": {"Tracked Messages": 40},
                "otlp_data_type_counts": {
                    "Gauge": {
                        "observed_datapoints": 8,
                        "accepted_datapoints": 7,
                    }
                },
            }
            replay_path = root / "replay.json"
            corpus_path = root / "corpus.json"
            write_json(replay_path, replay)
            write_json(
                corpus_path,
                {
                    "schema": oracle.CORPUS_SUMMARY_SCHEMA,
                    "file_count": 8,
                    "size_bytes": 100,
                    "manifest_sha256": "a" * 64,
                },
            )
            value = oracle.validate_prefix_replay(
                selection_path, replay_path, corpus_path, root / "replay-gate.json"
            )
            self.assertEqual(value["exact_message_prefix"], 40)
            self.assertEqual(value["recorded_samples"], 7)
            replay["general"]["Total Messages"] = 41
            write_json(root / "bad-replay.json", replay)
            with self.assertRaisesRegex(oracle.GateError, "exact selected"):
                oracle.validate_prefix_replay(
                    selection_path,
                    root / "bad-replay.json",
                    corpus_path,
                    root / "unused.json",
                )

    def test_sealed_http_result_matches_live_hash_and_uses_segments(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result, _record = self.make_result(root)
            selection_path = root / "selection.json"
            selection = oracle.select_candidate(result, selection_path)
            response_data = {
                "resultType": "vector",
                "result": [{"metric": {"service": "a"}, "value": [1234.5, "7"]}],
            }
            observed_paths: list[str] = []

            class Handler(BaseHTTPRequestHandler):
                def do_GET(self) -> None:
                    observed_paths.append(self.path)
                    if self.path == "/-/ready":
                        self.send_response(200)
                        self.end_headers()
                        return
                    body = json.dumps(
                        {"status": "success", "data": response_data},
                        separators=(",", ":"),
                    ).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header(
                        "x-chronoxide-query-stats",
                        json.dumps(stats(1), separators=(",", ":")),
                    )
                    self.send_header(
                        "x-chronoxide-query-io",
                        json.dumps(query_io(), separators=(",", ":")),
                    )
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def log_message(self, _format: str, *_args: object) -> None:
                    pass

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                value = oracle.query_sealed(
                    f"http://127.0.0.1:{server.server_port}",
                    selection_path,
                    root / "body.json",
                    root / "headers.json",
                    root / "comparison.json",
                    1000,
                    1_000_000,
                )
            finally:
                server.shutdown()
                server.server_close()
                thread.join()
            self.assertTrue(value["complete"])
            self.assertEqual(
                value["fresh_sealed_prefix"]["response_data_sha256"],
                selection["live_response"]["response_data_sha256"],
            )
            query_path = next(path for path in observed_paths if path != "/-/ready")
            parameters = parse_qs(urlparse(query_path).query)
            self.assertEqual(parameters["query"], [selection["http_parameters"]["query"]])
            self.assertEqual(parameters["time"], [selection["http_parameters"]["time"]])
            self.assertEqual(
                value["fresh_sealed_prefix"]["query_stats"]["segments_queried"], 1
            )

    def test_api_args_copy_q_controls_and_force_schema_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = root / "prefix.toml"
            config.write_text(
                self.q_config(root / "segments").replace(
                    "[api]\nenabled = true", "[api]\nenabled = false", 1
                ),
                encoding="utf-8",
            )
            controls = oracle._api_controls(config)
            self.assertEqual(controls["range_scalar_cache_max_bytes"], 0)
            self.assertEqual(controls["chunk_read_mode"], "pread")

    def test_final_gate_requires_zero_stage_exits_and_expected_api_stop(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = {
                "selection": root / "selection.json",
                "config": root / "config.json",
                "replay": root / "replay.json",
                "comparison": root / "comparison.json",
                "termination": root / "termination.json",
            }
            write_json(
                paths["selection"],
                {
                    "schema": oracle.SELECTION_SCHEMA,
                    "visible_message_sequence": 40,
                    "query_name": "head",
                    "generation": 2,
                    "mode": "instant",
                    "http_parameters": {"query": "foo", "time": "1"},
                    "live_response": {
                        "response_data_sha256": "a" * 64,
                        "cardinality": 1,
                        "samples": 1,
                    },
                },
            )
            write_json(
                paths["config"],
                {
                    "schema": oracle.CONFIG_SCHEMA,
                    "complete": True,
                    "selected_message_prefix": 40,
                },
            )
            write_json(
                paths["replay"],
                {
                    "schema": oracle.REPLAY_SCHEMA,
                    "complete": True,
                    "exact_message_prefix": 40,
                },
            )
            write_json(
                paths["comparison"],
                {
                    "schema": oracle.COMPARISON_SCHEMA,
                    "complete": True,
                    "exact_http_path": "/api/v1/query",
                    "exact_http_parameters": {"query": "foo", "time": "1"},
                    "matches": {
                        "response_data_sha256": True,
                        "cardinality": True,
                        "samples": True,
                    },
                    "fresh_sealed_prefix": {
                        "response_data_sha256": "a" * 64,
                        "cardinality": 1,
                        "samples": 1,
                        "query_stats": {"segments_queried": 1},
                    },
                    "live_head": {
                        "query_name": "head",
                        "generation": 2,
                        "visible_message_sequence": 40,
                        "response_data_sha256": "a" * 64,
                        "cardinality": 1,
                        "samples": 1,
                    },
                },
            )
            write_json(
                paths["termination"],
                {
                    "expected": True,
                    "signal": "SIGTERM",
                    "shell_status": 143,
                },
            )
            statuses = [root / name for name in ("replay.status", "query.status", "api.status")]
            for path in statuses:
                path.write_text("0\n", encoding="utf-8")
            value = oracle.gate_final(
                paths["selection"],
                paths["config"],
                paths["replay"],
                paths["comparison"],
                statuses[0],
                statuses[1],
                statuses[2],
                paths["termination"],
                root / "final.json",
            )
            self.assertTrue(value["complete"])
            statuses[1].write_text("2\n", encoding="utf-8")
            with self.assertRaisesRegex(oracle.GateError, "did not exit zero"):
                oracle.gate_final(
                    paths["selection"],
                    paths["config"],
                    paths["replay"],
                    paths["comparison"],
                    statuses[0],
                    statuses[1],
                    statuses[2],
                    paths["termination"],
                    root / "unused.json",
                )


if __name__ == "__main__":
    unittest.main()
