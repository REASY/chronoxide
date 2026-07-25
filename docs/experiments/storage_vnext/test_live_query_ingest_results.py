#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import live_query_ingest_results as results
import live_query_ingest_ab_gate as ingest_gate


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def write_artifact_manifest(root: Path) -> None:
    manifest = root / "metadata" / "result-artifacts.sha256"
    paths = []
    for top in ("configs", "metadata", "validation", "comparisons", "runs"):
        for path in (root / top).rglob("*"):
            relative = path.relative_to(root)
            parts = relative.parts
            if (
                relative.as_posix() == "metadata/result-artifacts.sha256"
                or (
                    len(parts) >= 4
                    and parts[0] == "runs"
                    and parts[2] == "segments"
                )
            ):
                continue
            if path.is_file():
                paths.append((relative.as_posix(), path))
    for name in ("run-plan.tsv", "run-summary.tsv"):
        path = root / name
        if path.is_file():
            paths.append((name, path))
    manifest.write_text(
        "".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {name}\n"
            for name, path in sorted(paths)
        ),
        encoding="utf-8",
    )


def time_artifacts(run: Path, elapsed: str, user: str, system: str) -> None:
    raw = {
        "User time (seconds)": user,
        "System time (seconds)": system,
        "Percent of CPU this job got": "15%",
        "Elapsed (wall clock) time (h:mm:ss or m:ss)": elapsed,
        "Maximum resident set size (kbytes)": "100",
        "Major (requiring I/O) page faults": "1",
        "Minor (reclaiming a frame) page faults": "2",
        "Voluntary context switches": "3",
        "Involuntary context switches": "4",
        "File system inputs": "5",
        "File system outputs": "6",
        "Exit status": "0",
    }
    (run / "replay.time.txt").write_text(
        "\n".join(f"\t{key}: {value}" for key, value in raw.items()) + "\n",
        encoding="utf-8",
    )
    write_json(
        run / "replay.time.json",
        {
            "user_seconds": float(user),
            "system_seconds": float(system),
            "elapsed": elapsed,
            "max_rss_kib": 100,
            "major_page_faults": 1,
            "minor_page_faults": 2,
            "voluntary_context_switches": 3,
            "involuntary_context_switches": 4,
            "filesystem_inputs": 5,
            "filesystem_outputs": 6,
            "exit_status": 0,
            "cpu_percent": "15%",
        },
    )


def rss_artifacts(run: Path, scale: int) -> None:
    (run / "rss-samples.tsv").write_text(
        "elapsed_ns\trecorded_at\tprocess_count\trss_kib\trss_anon_kib\t"
        "rss_file_kib\tvm_swap_kib\tmax_single_hwm_kib\tpids\n"
        f"1\t2026-01-01T00:00:00+00:00\t1\t{10 * scale}\t"
        f"{6 * scale}\t{4 * scale}\t0\t{8 * scale}\t1\n"
        f"2\t2026-01-01T00:00:01+00:00\t2\t{20 * scale}\t"
        f"{12 * scale}\t{8 * scale}\t0\t{16 * scale}\t1,2\n",
        encoding="utf-8",
    )
    write_json(
        run / "rss-summary.json",
        {
            "root_pid": 1,
            "samples": 2,
            "interval_ms": 100,
            "aggregate_rss_kib": 20 * scale,
            "aggregate_rss_anon_kib": 12 * scale,
            "aggregate_rss_file_kib": 8 * scale,
            "aggregate_vm_swap_kib": 0,
            "max_single_process_hwm_kib": 16 * scale,
            "process_count": 2,
        },
    )


def perf_artifacts(run: Path, scale: int) -> None:
    values = (
        ("100.0", "msec", "task-clock"),
        (str(1_000 * scale), "", "cycles"),
        (str(2_000 * scale), "", "instructions"),
        (str(20 * scale), "", "cache-misses"),
        (str(30 * scale), "", "context-switches"),
        (str(2 * scale), "", "cpu-migrations"),
        (str(40 * scale), "", "page-faults"),
    )
    (run / "perf-stat.tsv").write_text(
        "".join(f"{value}\t{unit}\t{event}\n" for value, unit, event in values),
        encoding="utf-8",
    )
    write_json(
        run / "perf-stat.json",
        {
            "events": [
                {
                    "event": event,
                    "raw_value": value,
                    "unit": unit,
                    "available": True,
                }
                for value, unit, event in values
            ]
        },
    )


def publication_line(
    generation: int,
    sequence: int,
    scale: int,
    *,
    mode: str | None = None,
    final_empty_fast_path: bool | None = None,
    base_scale: tuple[int, int, int] | None = None,
) -> str:
    fields = [
        'DEBUG chronoxide_live_metrics: event="publication" outcome="success"',
        f"generation={generation}",
        f"visible_message_sequence={sequence}",
        f"catalog_revision={generation}",
        "manifest_present=false",
    ]
    if mode is not None:
        fields.append(f'mode="{mode}"')
    timing_values = {"owner_and_head_ns": 2 * scale}
    if mode is not None:
        timing_values["publication_duration_ns"] = 10 * scale
    fields.extend(
        f"{field}={timing_values.get(field, scale)}"
        for field in results.PUBLICATION_TIMING_FIELDS
    )
    fields.extend(
        f"{field}={0 if mode == 'shutdown' and field in {'sample_keys', 'sample_fragments', 'catalog_active_series'} else scale}"
        for field in results.PUBLICATION_MAXIMUM_FIELDS
    )
    if final_empty_fast_path is not None:
        fields.append(
            "final_empty_fast_path="
            f"{'true' if final_empty_fast_path else 'false'}"
        )
    if base_scale is not None:
        fields.extend(
            (
                f"base_sample_keys={base_scale[0]}",
                f"base_sample_fragments={base_scale[1]}",
                f"base_catalog_active_series={base_scale[2]}",
            )
        )
    return " ".join(fields)


def live_artifacts(run: Path) -> None:
    lines = [
        publication_line(1, 500, 1),
        'DEBUG chronoxide_live_metrics: event="message_boundary" ingestion_pause_ns=3',
        publication_line(2, 1000, 2),
        'DEBUG chronoxide_live_metrics: event="message_boundary" ingestion_pause_ns=4',
    ]
    (run / "ingester.log").write_text("\n".join(lines) + "\n", encoding="utf-8")
    ordered = [(1, (500, 1)), (2, (1000, 2))]
    write_json(
        run / "live-log-summary.json",
        {
            "successful_publications": 2,
            "message_boundary_observations": 2,
            "first_generation": 1,
            "last_generation": 2,
            "first_visible_message_sequence": 500,
            "last_visible_message_sequence": 1000,
            "first_catalog_revision": 1,
            "last_catalog_revision": 2,
            "ingestion_pause_ns": results.distribution([3, 4]),
            "publication_timings_ns": {
                field: results.distribution(
                    [2, 4] if field == "owner_and_head_ns" else [1, 2]
                )
                for field in results.PUBLICATION_TIMING_FIELDS
            },
            "publication_maxima": {
                field: 2 for field in results.PUBLICATION_MAXIMUM_FIELDS
            },
            "generation_message_sequence": [
                {
                    "generation": 1,
                    "visible_message_sequence": 500,
                    "catalog_revision": 1,
                    "manifest_present": False,
                    "manifest_validated_offset": 1,
                },
                {
                    "generation": 2,
                    "visible_message_sequence": 1000,
                    "catalog_revision": 2,
                    "manifest_present": False,
                    "manifest_validated_offset": 2,
                },
            ],
            "mapping_sha256": results._canonical_sha256(ordered),
        },
    )


def client_record(index: int, started: int, elapsed: int) -> dict:
    stats = {field: 0 for field in results.QUERY_STATS_FIELDS}
    stats["matched_series"] = index
    stats["chunk_reads"] = index * 2
    query_io = {field: 0 for field in results.QUERY_IO_FIELDS}
    query_io.update(
        {
            "chunk_payload_used_bytes": 100,
            "chunk_payload_read_bytes": 200,
            "chunk_payload_physical_reads": 1,
        }
    )
    return {
        "schema": results.CLIENT_SCHEMA,
        "query_name": "query-a",
        "mode": "instant",
        "generation": 1 if index < 3 else 2,
        "visible_message_sequence": 500 if index < 3 else 1000,
        "catalog_revision": 1 if index < 3 else 2,
        "response_data_sha256": "a" * 64,
        "cardinality": 1,
        "samples": 1,
        "query_stats": stats,
        "query_io": query_io,
        "query_duration_ns": elapsed // 2,
        "serialize_duration_ns": 2,
        "queue_duration_ns": 1,
        "view_age_ms": index,
        "view_pin_wait_ns": 4,
        "view_pin_held_ns": 5,
        "client_elapsed_ns": elapsed,
        "client_started_monotonic_ns": started,
        "client_completed_monotonic_ns": started + elapsed,
        "response_bytes": 100 + index,
    }


def client_artifacts(root: Path) -> None:
    run = root / "runs" / "Q"
    records = [
        client_record(1, 100, 10),
        client_record(2, 120, 20),
        client_record(3, 200, 30),
        client_record(4, 240, 40),
    ]
    (run / "client-records.jsonl").write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    overall = results._client_record_group(records)
    per_query_latency = {
        "query-a": {
            field: {
                "count": overall["durations"][field]["count"],
                "p50": overall["durations"][field]["p50"],
                "p95": overall["durations"][field]["p95"],
            }
            for field in ("client_elapsed_ns", "query_duration_ns")
        }
    }
    span = 280 - 100
    write_json(
        run / "client-summary.json",
        {
            "schema": results.CLIENT_SCHEMA,
            "successful_requests": len(records),
            "closed_loop_observation_span_ns": span,
            "closed_loop_achieved_requests_per_second": (
                len(records) * 1_000_000_000 / span
            ),
            "durations": {
                field: overall["durations"][field]
                for field in results.CLIENT_DURATION_FIELDS
            },
            "query_stats_totals": overall["query_stats_totals"],
            "query_io_totals": overall["query_io_totals"],
            "per_query_latency": per_query_latency,
            "records_fingerprint_sha256": results._canonical_sha256(records),
        },
    )


def complete_root(
    parent: Path, order: str = "D,P,Q", *, include_api: bool = False
) -> Path:
    root = parent / "result"
    for variant in results.VARIANTS:
        (root / "runs" / variant).mkdir(parents=True)
    (root / "configs").mkdir()
    (root / "metadata").mkdir()
    (root / "comparisons").mkdir()
    (root / "validation").mkdir()
    write_json(
        root / "comparisons" / "dpq-gate.json",
        {
            "schema": results.RUN_SET_SCHEMA,
            "complete": True,
            "expected_messages": 1000,
            "storage_trees_equal": True,
            "replay_counters_equal": True,
            "live_head_only_observed": True,
            "perf_required": True,
        },
    )
    write_json(
        root / "validation" / "storage-verify-gate.json",
        {
            "schema_version": 8,
            "segments": 1,
            "series": 1,
            "chunks": 1,
            "samples": 8,
            "recorded_head_writes": 10,
            "recorded_writes_minus_physical_rows": 2,
            "writer_to_verifier_counts_reconciled": True,
            "capture_level_physical_sample_golden_gated": False,
            "verified_selection_fingerprint": "a" * 64,
            "decoded_semantic_fingerprint": "c" * 64,
            "exact_postings_fingerprint": "b" * 64,
        },
    )
    (root / "metadata" / "PHYSICAL_SAMPLE_COUNT_COVERAGE_GAP").write_text(
        "test coverage gap\n", encoding="utf-8"
    )
    write_json(
        root / "validation" / "readbacks-gate.json",
        {
            "expected_queries": 1,
            "executed_queries": 1,
            "skipped_queries": 0,
            "isolation_check_skips": 0,
            "mismatches": 0,
        },
    )
    (root / "metadata" / "settings.txt").write_text(
        f"run_order={order}\nstop_after_messages=1000\nevict_capture=1\n"
        "run_note=quiet test\n",
        encoding="utf-8",
    )
    write_json(
        root / "metadata" / "validated-inputs.json",
        {
            "capture": "/capture",
            "capture_manifest_sha256": "c" * 64,
            "capture_files": [{"name": "input.capture", "sha256": "d" * 64}],
            "config_template": "/config.toml",
            "config_template_sha256": "e" * 64,
            "stop_after_messages": 4_000_000,
        },
    )
    write_json(
        root / "metadata" / "cpusets.json",
        {"ingest": [0, 1], "client": [2, 3], "allowed": [0, 1, 2, 3]},
    )
    (root / "metadata" / "environment.txt").write_text(
        "2026-01-01T00:00:00+00:00\nLinux test-host 1 test\n",
        encoding="utf-8",
    )
    binaries = root / "metadata" / "binaries"
    binaries.mkdir()
    binary_lines = []
    roles = [
        "chronoxide-ingester",
        "chronoxide-query",
        "chronoxide-storage-verify",
    ]
    if include_api:
        roles.append("chronoxide-api")
    for role in roles:
        path = binaries / role
        path.write_bytes(f"{role}\n".encode())
        binary_lines.append(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path}\n"
        )
    (root / "metadata" / "binaries.sha256").write_text(
        "".join(binary_lines), encoding="utf-8"
    )
    write_json(
        root / "metadata" / "workload.json",
        {"queries": [{"name": "query-a"}]},
    )
    variants = order.split(",")
    (root / "run-plan.tsv").write_text(
        "order\tvariant\tconfig\tsegments_dir\n"
        + "".join(
            f"{position}\t{variant}\t/config/{variant}\t/segments/{variant}\n"
            for position, variant in enumerate(variants, 1)
        ),
        encoding="utf-8",
    )
    elapsed = {"D": "0:10.00", "P": "0:12.50", "Q": "0:20.00"}
    for scale, variant in enumerate(results.VARIANTS, 1):
        run = root / "runs" / variant
        time_artifacts(run, elapsed[variant], str(scale), "0.5")
        rss_artifacts(run, scale)
        perf_artifacts(run, scale)
        if variant != "D":
            live_artifacts(run)
    client_artifacts(root)
    write_artifact_manifest(root)
    (root / "COMPLETE").touch()
    return root


class ResultsTest(unittest.TestCase):
    def test_elapsed_parser_is_exact_and_strict(self) -> None:
        self.assertEqual(results.parse_elapsed_seconds("1:02:03.25"), 3723.25)
        self.assertEqual(results.parse_elapsed_seconds("2:03.5"), 123.5)
        with self.assertRaises(results.SummaryError):
            results.parse_elapsed_seconds("1:60.0")
        with self.assertRaises(results.SummaryError):
            results.parse_elapsed_seconds("0:00")

    def test_distribution_uses_nearest_rank(self) -> None:
        self.assertEqual(
            results.distribution([40, 10, 30, 20]),
            {"count": 4, "min": 10, "p50": 20, "p95": 40, "p99": 40, "max": 40},
        )

    def test_complete_root_is_reconciled_and_summarized(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = complete_root(Path(temporary))
            document = results.summarize([root])
            summary = document["roots"][0]
            self.assertEqual(summary["order_text"], "D,P,Q")
            self.assertEqual(summary["variants"]["D"]["time"]["elapsed_ns"], 10_000_000_000)
            self.assertEqual(
                summary["variants"]["D"]["time"]["messages_per_second"], 100
            )
            self.assertEqual(
                summary["deltas"]["D_to_P"]["elapsed_seconds"]["absolute"], 2.5
            )
            self.assertEqual(
                summary["variants"]["Q"]["client"]["overall"][
                    "payload_read_amplification"
                ]["read_used_amplification"],
                2,
            )
            query = summary["variants"]["Q"]["client"]["per_query"]["query-a"]
            self.assertEqual(
                query["first_observation"]["durations"]["client_elapsed_ns"]["p50"],
                10,
            )
            self.assertEqual(
                query["subsequent_observations"]["durations"]["client_elapsed_ns"][
                    "p50"
                ],
                30,
            )
            self.assertFalse(document["aggregate"]["position_counterbalanced"])
            self.assertTrue(
                summary["storage_validation"][
                    "writer_to_verifier_counts_reconciled"
                ]
            )
            self.assertFalse(
                summary["storage_validation"][
                    "capture_level_physical_sample_golden_gated"
                ]
            )
            markdown = results.render_markdown(document)
            self.assertIn("closed-loop", markdown)
            self.assertIn("instrumented publication cost", markdown)
            self.assertIn("Storage row reconciliation", markdown)
            self.assertIn("capture-level physical-row golden: **no**", markdown)
            self.assertIn("D,P,Q", markdown)
            self.assertEqual(document, results.summarize([root]))

    def test_complete_root_accepts_a_pre_timing_api_binary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = complete_root(Path(temporary), include_api=True)
            results.summarize([root])
            hashes = results._binary_hashes(
                root / "metadata" / "binaries.sha256"
            )
            self.assertIn("chronoxide-api", hashes)

    def test_current_live_log_schema_is_independently_reconciled(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary)
            text = "\n".join(
                (
                    publication_line(1, 500, 2, mode="boundary"),
                    'DEBUG chronoxide_live_metrics: event="message_boundary" '
                    'outcome="success" ingestion_pause_ns=3',
                    publication_line(
                        2,
                        1000,
                        1,
                        mode="shutdown",
                        final_empty_fast_path=True,
                        base_scale=(100, 2, 90),
                    ),
                )
            )
            (run / "ingester.log").write_text(text, encoding="utf-8")
            write_json(
                run / "live-log-summary.json",
                ingest_gate.parse_live_log_text(text, 1000),
            )
            parsed = results._parse_live_log(run, 1000)
            self.assertEqual(parsed["boundary_publications"], 1)
            self.assertEqual(
                parsed["successful_message_boundary_observations"], 1
            )
            self.assertEqual(parsed["failed_message_boundary_observations"], 0)
            self.assertTrue(
                parsed["shutdown_publication"]["final_empty_fast_path"]
            )
            self.assertEqual(
                parsed["shutdown_publication"]["base_scale"][
                    "base_sample_keys"
                ],
                100,
            )

    def test_missing_complete_or_gate_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = complete_root(Path(temporary))
            (root / "COMPLETE").unlink()
            with self.assertRaisesRegex(results.SummaryError, "not COMPLETE"):
                results.summarize([root])
            (root / "COMPLETE").touch()
            (root / "comparisons" / "dpq-gate.json").unlink()
            with self.assertRaises(results.SummaryError):
                results.summarize([root])

    def test_legacy_one_sided_storage_gate_surfaces_coverage_gap(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = complete_root(Path(temporary))
            write_json(
                root / "validation" / "storage-verify-gate.json",
                {
                    "schema_version": 8,
                    "segments": 1,
                    "samples": 8,
                    "recorded_head_writes": 10,
                    "recorded_writes_minus_physical_rows": 2,
                    "physical_sample_count_exactly_gated": False,
                    "verified_selection_fingerprint": "a" * 64,
                    "exact_postings_fingerprint": "b" * 64,
                },
            )
            (root / "metadata" / "PHYSICAL_SAMPLE_COUNT_COVERAGE_GAP").unlink()
            write_artifact_manifest(root)
            storage = results.summarize([root])["roots"][0][
                "storage_validation"
            ]
            self.assertEqual(storage["gate_revision"], "legacy-one-sided")
            self.assertFalse(storage["writer_to_verifier_counts_reconciled"])
            self.assertFalse(
                storage["capture_level_physical_sample_golden_gated"]
            )

    def test_three_cyclic_roots_are_position_counterbalanced(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            roots = [
                complete_root(parent / "a", "D,P,Q"),
                complete_root(parent / "b", "P,Q,D"),
                complete_root(parent / "c", "Q,D,P"),
            ]
            document = results.summarize(reversed(roots))
            self.assertTrue(document["aggregate"]["position_counterbalanced"])
            self.assertEqual(
                [root["order_text"] for root in document["roots"]],
                ["D,P,Q", "P,Q,D", "Q,D,P"],
            )
            self.assertEqual(
                document["aggregate"]["variant_position_counts"]["D"],
                {"1": 1, "2": 1, "3": 1},
            )
            self.assertEqual(document, results.summarize(roots))

    def test_unequal_position_counts_are_not_counterbalanced(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            roots = [
                complete_root(parent / "a", "D,P,Q"),
                complete_root(parent / "b", "D,P,Q"),
                complete_root(parent / "c", "P,Q,D"),
                complete_root(parent / "d", "Q,D,P"),
            ]
            self.assertFalse(
                results.summarize(roots)["aggregate"]["position_counterbalanced"]
            )

    def test_cross_root_provenance_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            first = complete_root(parent / "a", "D,P,Q")
            second = complete_root(parent / "b", "P,Q,D")
            inputs_path = second / "metadata" / "validated-inputs.json"
            inputs = json.loads(inputs_path.read_text(encoding="utf-8"))
            inputs["config_template_sha256"] = "f" * 64
            write_json(inputs_path, inputs)
            write_artifact_manifest(second)
            with self.assertRaisesRegex(results.SummaryError, "not comparable"):
                results.summarize([first, second])

    def test_artifact_tampering_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = complete_root(Path(temporary))
            summary_path = root / "runs" / "P" / "rss-summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["aggregate_rss_kib"] += 1
            write_json(summary_path, summary)
            with self.assertRaisesRegex(results.SummaryError, "artifact checksum"):
                results.summarize([root])

    def test_coordinated_raw_summary_mismatch_still_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = complete_root(Path(temporary))
            summary_path = root / "runs" / "P" / "rss-summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["aggregate_rss_kib"] += 1
            write_json(summary_path, summary)
            write_artifact_manifest(root)
            with self.assertRaisesRegex(results.SummaryError, "RSS raw/summary"):
                results.summarize([root])

    def test_cli_writes_both_outputs_outside_result_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = complete_root(parent)
            json_output = parent / "results.json"
            markdown_output = parent / "results.md"
            self.assertEqual(
                results.main(
                    [
                        "--root",
                        str(root),
                        "--json-output",
                        str(json_output),
                        "--markdown-output",
                        str(markdown_output),
                    ]
                ),
                0,
            )
            self.assertEqual(json.loads(json_output.read_text())["schema"], results.SCHEMA)
            self.assertIn("# Live-query ingestion", markdown_output.read_text())
            self.assertEqual(
                results.main(
                    [
                        "--root",
                        str(root),
                        "--json-output",
                        str(root / "bad.json"),
                        "--markdown-output",
                        str(parent / "other.md"),
                    ]
                ),
                2,
            )


if __name__ == "__main__":
    unittest.main()
