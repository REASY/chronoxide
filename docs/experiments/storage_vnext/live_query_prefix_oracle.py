#!/usr/bin/env python3
"""Fail-closed post-measurement head-versus-sealed prefix oracle.

This is deliberately not part of the measured D/P/Q process graph.  It proves
that one non-empty response which the live query telemetry attributes entirely
to the head is reproduced by a fresh, API-disabled replay of the exact visible
message prefix after that prefix is sealed.

The sealed HTTP server and the embedded live server use the same PromQL
evaluator.  This is therefore an independent storage-path/intermediate-value
oracle, not an independent PromQL semantic oracle.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import socket
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


class GateError(ValueError):
    pass


SELECTION_SCHEMA = "chronoxide/live-query-prefix-oracle-selection/v1"
CONFIG_SCHEMA = "chronoxide/live-query-prefix-oracle-config/v1"
REPLAY_SCHEMA = "chronoxide/live-query-prefix-oracle-replay/v1"
COMPARISON_SCHEMA = "chronoxide/live-query-prefix-oracle-comparison/v1"
FINAL_SCHEMA = "chronoxide/live-query-prefix-oracle/v1"
CLIENT_SCHEMA = "chronoxide/live-query-ingest-client/v1"
WORKLOAD_SCHEMA = "chronoxide/live-query-ingest-workload/v1"
RUN_SET_SCHEMA = "chronoxide/live-query-ingest-ab/v1"
REPLAY_REPORT_SCHEMA = "chronoxide/storage-vnext-replay-correctness/v2"
CORPUS_SUMMARY_SCHEMA = "chronoxide/storage-vnext-phase1-corpus/v1"
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
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
HEX_SHA256 = re.compile(r"[0-9a-f]{64}")


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def write_json_exclusive(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as destination:
        json.dump(value, destination, ensure_ascii=False, indent=2, sort_keys=True)
        destination.write("\n")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    """Match the measured client: object-key canonical, array-order sensitive."""
    payload = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def nonnegative_int(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GateError(f"{context} must be a non-negative integer")
    return value


def positive_int(value: Any, context: str) -> int:
    result = nonnegative_int(value, context)
    if result == 0:
        raise GateError(f"{context} must be greater than zero")
    return result


def regular_file(path: Path, context: str) -> None:
    if not path.is_file() or path.is_symlink():
        raise GateError(f"{context} must be a regular non-symlink file: {path}")


def _load_workload(path: Path) -> dict[str, Any]:
    value = load_json(path)
    if not isinstance(value, dict) or value.get("schema") != WORKLOAD_SCHEMA:
        raise GateError("frozen workload has an unsupported schema")
    queries = value.get("queries")
    if not isinstance(queries, list) or not queries:
        raise GateError("frozen workload contains no queries")
    by_name: dict[str, dict[str, Any]] = {}
    for index, query in enumerate(queries):
        if not isinstance(query, dict):
            raise GateError(f"workload query {index} is not an object")
        name = query.get("name")
        mode = query.get("mode")
        expression = query.get("query")
        if not isinstance(name, str) or not name or name in by_name:
            raise GateError(f"workload query {index} has an invalid/duplicate name")
        if mode not in {"instant", "range"}:
            raise GateError(f"workload query {name} has an invalid mode")
        if not isinstance(expression, str) or not expression or "\n" in expression:
            raise GateError(f"workload query {name} has an invalid expression")
        if not isinstance(query.get("require_nonempty"), bool):
            raise GateError(f"workload query {name} lacks require_nonempty")
        required = ("time",) if mode == "instant" else ("start", "end", "step")
        if any(not isinstance(query.get(field), str) or not query[field] for field in required):
            raise GateError(f"workload query {name} lacks exact HTTP time parameters")
        by_name[name] = query
    return {"document": value, "queries": by_name}


def _load_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line:
            raise GateError(f"blank client record at line {line_number}")
        value = json.loads(line)
        if not isinstance(value, dict) or value.get("schema") != CLIENT_SCHEMA:
            raise GateError(f"client record {line_number} has an invalid schema")
        records.append(value)
    if not records:
        raise GateError("Q client records are empty")
    return records


def _log_uint(line: str, field: str) -> int:
    match = re.search(rf"(?:^|\s){re.escape(field)}=(\d+)(?:\s|$)", line)
    if match is None:
        raise GateError(f"successful publication event lacks {field}")
    return int(match.group(1))


def _publication_mapping(path: Path) -> dict[int, tuple[int, int]]:
    mapping: dict[int, tuple[int, int]] = {}
    text = path.read_text(encoding="utf-8")
    if "Live view publication failed" in text:
        raise GateError("Q log contains a live publication failure")
    for raw_line in text.splitlines():
        line = ANSI_ESCAPE.sub("", raw_line)
        if (
            "chronoxide_live_metrics" not in line
            or re.search(r'\bevent="publication"', line) is None
            or re.search(r'\boutcome="success"', line) is None
        ):
            continue
        generation = positive_int(_log_uint(line, "generation"), "generation")
        cut = (
            nonnegative_int(
                _log_uint(line, "visible_message_sequence"), "message sequence"
            ),
            nonnegative_int(_log_uint(line, "catalog_revision"), "catalog revision"),
        )
        if generation in mapping:
            raise GateError(f"duplicate successful publication generation {generation}")
        mapping[generation] = cut
    if not mapping:
        raise GateError("Q log has no successful publication")
    ordered = sorted(mapping.items())
    for (prior_generation, prior_cut), (generation, cut) in zip(ordered, ordered[1:]):
        if generation <= prior_generation or cut[0] < prior_cut[0] or cut[1] < prior_cut[1]:
            raise GateError("Q publication generation/cut mapping regressed")
    return mapping


def _query_parameters(query: dict[str, Any]) -> dict[str, str]:
    if query["mode"] == "instant":
        return {"query": query["query"], "time": query["time"]}
    return {
        "query": query["query"],
        "start": query["start"],
        "end": query["end"],
        "step": query["step"],
    }


def select_candidate(
    result_root: Path, output: Path, requested_query: str | None = None
) -> dict[str, Any]:
    result_root = result_root.resolve()
    if not result_root.is_dir() or not (result_root / "COMPLETE").is_file():
        raise GateError("measured D/P/Q result root is absent or incomplete")
    gate_path = result_root / "comparisons/dpq-gate.json"
    workload_path = result_root / "metadata/harness/live_query_ingest_queries.json"
    records_path = result_root / "runs/Q/client-records.jsonl"
    summary_path = result_root / "runs/Q/client-summary.json"
    log_path = result_root / "runs/Q/ingester.log"
    inputs_path = result_root / "metadata/validated-inputs.json"
    q_config_path = result_root / "configs/Q.toml"
    for path, context in (
        (gate_path, "D/P/Q gate"),
        (workload_path, "frozen workload"),
        (records_path, "Q records"),
        (summary_path, "Q client summary"),
        (log_path, "Q ingester log"),
        (inputs_path, "validated inputs"),
        (q_config_path, "Q configuration"),
    ):
        regular_file(path, context)

    run_gate = load_json(gate_path)
    if (
        not isinstance(run_gate, dict)
        or run_gate.get("schema") != RUN_SET_SCHEMA
        or run_gate.get("complete") is not True
        or run_gate.get("storage_trees_equal") is not True
        or run_gate.get("replay_counters_equal") is not True
        or run_gate.get("live_head_only_observed") is not True
    ):
        raise GateError("measured D/P/Q gate is incomplete or failed")
    expected_messages = positive_int(
        run_gate.get("expected_messages"), "D/P/Q expected messages"
    )
    workload = _load_workload(workload_path)
    if requested_query is not None and requested_query not in workload["queries"]:
        raise GateError(f"requested query is absent from workload: {requested_query}")
    records = _load_records(records_path)
    summary = load_json(summary_path)
    if not isinstance(summary, dict) or summary.get("records_fingerprint_sha256") != canonical_sha256(records):
        raise GateError("Q client record fingerprint differs from its frozen summary")
    mapping = _publication_mapping(log_path)

    signatures: dict[tuple[int, str], set[str]] = {}
    group_counts: dict[tuple[int, str], int] = {}
    candidates: list[tuple[int, int, str, int, dict[str, Any], dict[str, Any]]] = []
    for ordinal, record in enumerate(records, 1):
        name = record.get("query_name")
        query = workload["queries"].get(name)
        if query is None:
            raise GateError(f"Q record {ordinal} names an unknown query")
        if record.get("mode") != query["mode"]:
            raise GateError(f"Q record {ordinal} mode differs from frozen workload")
        generation = positive_int(record.get("generation"), f"record {ordinal} generation")
        sequence = nonnegative_int(
            record.get("visible_message_sequence"), f"record {ordinal} sequence"
        )
        catalog_revision = nonnegative_int(
            record.get("catalog_revision"), f"record {ordinal} catalog revision"
        )
        if mapping.get(generation) != (sequence, catalog_revision):
            raise GateError(
                f"Q record {ordinal} cut is absent from raw successful publication log"
            )
        digest = record.get("response_data_sha256")
        if not isinstance(digest, str) or HEX_SHA256.fullmatch(digest) is None:
            raise GateError(f"Q record {ordinal} has an invalid data hash")
        stats = record.get("query_stats")
        if not isinstance(stats, dict) or set(stats) != QUERY_STATS_FIELDS:
            raise GateError(f"Q record {ordinal} has malformed complete QueryStats")
        for field, value in stats.items():
            nonnegative_int(value, f"record {ordinal} QueryStats.{field}")
        cardinality = nonnegative_int(
            record.get("cardinality"), f"record {ordinal} cardinality"
        )
        samples = nonnegative_int(record.get("samples"), f"record {ordinal} samples")
        signature = canonical_sha256(
            {
                "response_data_sha256": digest,
                "cardinality": cardinality,
                "samples": samples,
                "query_stats": stats,
            }
        )
        group = (generation, name)
        signatures.setdefault(group, set()).add(signature)
        group_counts[group] = group_counts.get(group, 0) + 1
        if (
            query["require_nonempty"]
            and (requested_query is None or name == requested_query)
            and 0 < sequence < expected_messages
            and cardinality > 0
            and samples > 0
            and stats["segments_queried"] == 0
            and stats["matched_series"] > 0
            and stats["samples_decoded"] > 0
        ):
            candidates.append((sequence, generation, name, ordinal, record, query))
    changed_groups = [group for group, values in signatures.items() if len(values) != 1]
    if changed_groups:
        raise GateError(f"same-generation Q records changed: {changed_groups[:3]}")
    if not candidates:
        raise GateError(
            "no pre-final designated non-empty Q record has zero sealed segments "
            "queried plus positive matched-series and sample-decode work"
        )
    sequence, generation, name, ordinal, record, query = min(candidates)
    validated_inputs = load_json(inputs_path)
    capture = validated_inputs.get("capture") if isinstance(validated_inputs, dict) else None
    if not isinstance(capture, str) or not Path(capture).is_absolute():
        raise GateError("validated input provenance lacks an absolute capture path")
    result = {
        "schema": SELECTION_SCHEMA,
        "oracle_kind": "head-vs-sealed-storage-path",
        "independent_promql_evaluator": False,
        "canonical_hash_contract": (
            "JSON object keys sorted; Prometheus result arrays retain server order"
        ),
        "ordering_sensitive_fail_closed": True,
        "measured_result_root": str(result_root),
        "measured_expected_messages": expected_messages,
        "capture": capture,
        "visible_message_sequence": sequence,
        "generation": generation,
        "catalog_revision": record["catalog_revision"],
        "query_name": name,
        "mode": query["mode"],
        "http_parameters": _query_parameters(query),
        "live_response": {
            "response_data_sha256": record["response_data_sha256"],
            "cardinality": record["cardinality"],
            "samples": record["samples"],
            "query_stats": record["query_stats"],
        },
        "head_only_evidence": {
            "nonempty": True,
            "pre_final": True,
            "segments_queried": 0,
            "matched_series_positive": True,
            "samples_decoded_positive": True,
            "publication_log_cut_matches": True,
            "same_generation_query_observations": group_counts[(generation, name)],
            "client_record_line": ordinal,
        },
        "provenance": {
            "dpq_gate_sha256": sha256_file(gate_path),
            "workload_sha256": sha256_file(workload_path),
            "client_records_sha256": sha256_file(records_path),
            "client_summary_sha256": sha256_file(summary_path),
            "q_ingester_log_sha256": sha256_file(log_path),
            "q_config_sha256": sha256_file(q_config_path),
            "validated_inputs_sha256": sha256_file(inputs_path),
        },
    }
    write_json_exclusive(output, result)
    return result


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
            f"configuration must contain exactly one {section_name}.{key}"
        )
    lines[matches[0]] = f"{key} = {rendered_value}\n"


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    if not isinstance(value, dict):
        return {prefix: value}
    result: dict[str, Any] = {}
    for key in sorted(value):
        path = f"{prefix}.{key}" if prefix else key
        result.update(_flatten(value[key], path))
    return result


def render_prefix_config(
    result_root: Path,
    selection_path: Path,
    segments_dir: Path,
    output: Path,
    gate_output: Path,
    q_config_override: Path | None = None,
) -> dict[str, Any]:
    result_root = result_root.resolve()
    selection = load_json(selection_path)
    if (
        not isinstance(selection, dict)
        or selection.get("schema") != SELECTION_SCHEMA
        or selection.get("measured_result_root") != str(result_root)
    ):
        raise GateError("selection does not belong to the measured result root")
    if not output.is_absolute() or not segments_dir.is_absolute():
        raise GateError("prefix config and segment root must be absolute")
    if output.exists() or segments_dir.exists():
        raise GateError("prefix config and segment root must both be fresh")
    try:
        segments_dir.resolve().relative_to(result_root)
    except ValueError:
        pass
    else:
        raise GateError("oracle segment root must not be inside measured D/P/Q result")
    q_config_path = (
        result_root / "configs/Q.toml"
        if q_config_override is None
        else q_config_override.resolve()
    )
    regular_file(q_config_path, "frozen Q config")
    selected_q_hash = selection.get("provenance", {}).get("q_config_sha256")
    if (
        not isinstance(selected_q_hash, str)
        or sha256_file(q_config_path) != selected_q_hash
    ):
        raise GateError("Q config changed after the live-record selection")
    lines = q_config_path.read_text(encoding="utf-8").splitlines(keepends=True)
    cut = positive_int(
        selection.get("visible_message_sequence"), "selected message prefix"
    )
    _replace_assignment(lines, "ingestion", "stop_after_messages", str(cut))
    _replace_assignment(
        lines,
        "ingestion.segment_writer",
        "segments_dir",
        json.dumps(str(segments_dir)),
    )
    _replace_assignment(lines, "api", "enabled", "false")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as destination:
        destination.write("".join(lines))
    with q_config_path.open("rb") as source:
        measured = tomllib.load(source)
    with output.open("rb") as source:
        oracle = tomllib.load(source)
    measured_flat = _flatten(measured)
    oracle_flat = _flatten(oracle)
    changed = {
        key: {"measured": measured_flat.get(key), "oracle": oracle_flat.get(key)}
        for key in sorted(set(measured_flat) | set(oracle_flat))
        if measured_flat.get(key) != oracle_flat.get(key)
    }
    allowed = {
        "api.enabled",
        "ingestion.segment_writer.segments_dir",
        "ingestion.stop_after_messages",
    }
    if set(changed) != allowed:
        raise GateError(
            f"prefix config differs from Q outside the exact controls: {sorted(changed)}"
        )
    ingestion = oracle.get("ingestion")
    api = oracle.get("api")
    measured_ingestion = measured.get("ingestion")
    if not isinstance(ingestion, dict) or not isinstance(api, dict):
        raise GateError("prefix config lacks ingestion/api tables")
    if not isinstance(measured_ingestion, dict) or measured_ingestion.get(
        "stop_after_messages"
    ) != selection.get("measured_expected_messages"):
        raise GateError("frozen Q config does not match the measured final message cut")
    writer = ingestion.get("segment_writer")
    if not isinstance(writer, dict):
        raise GateError("prefix config lacks segment writer")
    if (
        ingestion.get("replay_from") != selection.get("capture")
        or ingestion.get("capture_to") not in (None, "")
        or ingestion.get("stop_after_messages") != cut
        or writer.get("enabled") is not True
        or writer.get("storage_schema") != "schema8"
        or writer.get("segments_dir") != str(segments_dir)
        or api.get("enabled") is not False
        or api.get("range_scalar_cache_max_bytes") != 0
    ):
        raise GateError("prefix config violates replay/sealed/cache controls")
    result = {
        "schema": CONFIG_SCHEMA,
        "complete": True,
        "selected_message_prefix": cut,
        "api_enabled": False,
        "range_scalar_cache_max_bytes": 0,
        "segments_dir": str(segments_dir),
        "config_sha256": sha256_file(output),
        "measured_q_config_sha256": sha256_file(q_config_path),
        "exact_changed_fields": changed,
    }
    write_json_exclusive(gate_output, result)
    return result


def validate_prefix_replay(
    selection_path: Path,
    replay_path: Path,
    corpus_summary_path: Path,
    output: Path,
) -> dict[str, Any]:
    selection = load_json(selection_path)
    replay = load_json(replay_path)
    corpus = load_json(corpus_summary_path)
    if not isinstance(selection, dict) or selection.get("schema") != SELECTION_SCHEMA:
        raise GateError("invalid prefix selection")
    if not isinstance(replay, dict) or replay.get("schema") != REPLAY_REPORT_SCHEMA:
        raise GateError("prefix replay report has an unsupported schema")
    general = replay.get("general")
    policy = replay.get("datapoint_policy_totals")
    storage = replay.get("datapoint_storage_totals")
    watermarks = replay.get("partition_watermarks")
    types = replay.get("otlp_data_type_counts")
    if not all(
        isinstance(value, dict)
        for value in (general, policy, storage, watermarks, types)
    ):
        raise GateError("prefix replay correctness report is incomplete")
    cut = positive_int(selection.get("visible_message_sequence"), "selected prefix")
    if positive_int(general.get("Total Messages"), "Total Messages") != cut:
        raise GateError("prefix replay did not stop at the exact selected message cut")
    observed = nonnegative_int(policy.get("Observed"), "policy observed")
    accepted = nonnegative_int(policy.get("Time-Policy Accepted"), "policy accepted")
    rejected = sum(
        nonnegative_int(policy.get(key, 0), f"policy {key}")
        for key in ("Dropped Too Old", "Dropped Too Future", "Missing Timestamp")
    )
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
    if (
        observed != accepted + rejected
        or accepted != recorded + not_recorded
        or not_recorded != missing + invalid
        or general.get("Recorded Samples") != recorded
        or watermarks.get("Tracked Messages") != cut
        or sum(row["observed_datapoints"] for row in types.values()) != observed
        or sum(row["accepted_datapoints"] for row in types.values()) != accepted
    ):
        raise GateError("prefix replay counters do not reconcile")
    if (
        not isinstance(corpus, dict)
        or corpus.get("schema") != CORPUS_SUMMARY_SCHEMA
        or positive_int(corpus.get("file_count"), "corpus file count") == 0
        or positive_int(corpus.get("size_bytes"), "corpus size") == 0
        or not isinstance(corpus.get("manifest_sha256"), str)
        or HEX_SHA256.fullmatch(corpus["manifest_sha256"]) is None
    ):
        raise GateError("prefix replay produced no validated sealed corpus")
    result = {
        "schema": REPLAY_SCHEMA,
        "complete": True,
        "exact_message_prefix": cut,
        "recorded_samples": recorded,
        "observed_datapoints": observed,
        "accepted_datapoints": accepted,
        "corpus": corpus,
        "replay_correctness_sha256": sha256_file(replay_path),
    }
    write_json_exclusive(output, result)
    return result


def _api_controls(config_path: Path) -> dict[str, Any]:
    with config_path.open("rb") as source:
        config = tomllib.load(source)
    api = config.get("api")
    if not isinstance(api, dict) or api.get("enabled") is not False:
        raise GateError("sealed API controls must come from the API-disabled prefix config")
    required = {
        "max_concurrent_queries": int,
        "query_max_series_matched": int,
        "query_max_projected_series": int,
        "query_max_chunks_read": int,
        "query_max_bytes_read": int,
        "query_max_samples": int,
        "regex_max_expanded_values": int,
        "chunk_read_mode": str,
        "chunk_read_queue_depth": int,
        "chunk_payload_coalesce_max_gap_bytes": int,
        "experimental_cross_segment_chunk_reads": bool,
        "range_scalar_cache_max_bytes": int,
    }
    for key, expected_type in required.items():
        value = api.get(key)
        if isinstance(value, bool) != (expected_type is bool) or not isinstance(
            value, expected_type
        ):
            raise GateError(f"prefix api.{key} is absent or has the wrong type")
    if api["range_scalar_cache_max_bytes"] != 0:
        raise GateError("prefix oracle requires a zero-byte range scalar cache")
    return api


def emit_api_args(config_path: Path, segments_dir: Path, listen: str) -> None:
    controls = _api_controls(config_path)
    with config_path.open("rb") as source:
        config = tomllib.load(source)
    configured_segments = config.get("ingestion", {}).get("segment_writer", {}).get(
        "segments_dir"
    )
    if configured_segments != str(segments_dir):
        raise GateError("sealed API segment root differs from the prefix replay config")
    host, _port = _parse_listen(listen)
    if host not in {"127.0.0.1", "::1", "localhost"}:
        raise GateError("oracle API must listen on loopback")
    args = [
        "--segments-dir",
        str(segments_dir),
        "--listen",
        listen,
        "--chunk-read-mode",
        controls["chunk_read_mode"],
        "--chunk-read-queue-depth",
        str(controls["chunk_read_queue_depth"]),
        "--chunk-payload-coalesce-max-gap-bytes",
        str(controls["chunk_payload_coalesce_max_gap_bytes"]),
        "--query-max-series-matched",
        str(controls["query_max_series_matched"]),
        "--query-max-projected-series",
        str(controls["query_max_projected_series"]),
        "--query-max-chunks-read",
        str(controls["query_max_chunks_read"]),
        "--query-max-bytes-read",
        str(controls["query_max_bytes_read"]),
        "--query-max-samples",
        str(controls["query_max_samples"]),
        "--query-max-regex-values-examined",
        str(controls["regex_max_expanded_values"]),
        "--range-scalar-cache-max-bytes",
        "0",
        "--max-concurrent-queries",
        str(controls["max_concurrent_queries"]),
        "--storage-schema",
        "schema8",
        "--validate-segment-footers",
    ]
    if controls["experimental_cross_segment_chunk_reads"]:
        args.append("--experimental-cross-segment-chunk-reads")
    sys.stdout.buffer.write(b"".join(arg.encode() + b"\0" for arg in args))


def _parse_listen(listen: str) -> tuple[str, int]:
    if listen.startswith("["):
        match = re.fullmatch(r"\[([^]]+)]:(\d+)", listen)
    else:
        match = re.fullmatch(r"([^:]+):(\d+)", listen)
    if match is None:
        raise GateError(f"invalid listen address: {listen}")
    port = int(match.group(2))
    if not 0 < port < 65536:
        raise GateError("listen port is out of range")
    return match.group(1), port


def check_listen_free(listen: str) -> None:
    host, port = _parse_listen(listen)
    with socket.socket(
        socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM
    ) as listener:
        listener.bind((host, port))


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
    raise GateError(f"unsupported Prometheus result type: {result_type!r}")


def _wait_ready(base_url: str, timeout_ms: int) -> None:
    deadline = time.monotonic() + timeout_ms / 1000
    url = f"{base_url.rstrip('/')}/-/ready"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.05)
    raise GateError("sealed API readiness timed out")


def query_sealed(
    base_url: str,
    selection_path: Path,
    body_output: Path,
    headers_output: Path,
    comparison_output: Path,
    timeout_ms: int,
    max_response_bytes: int,
) -> dict[str, Any]:
    selection = load_json(selection_path)
    if not isinstance(selection, dict) or selection.get("schema") != SELECTION_SCHEMA:
        raise GateError("invalid prefix selection")
    parameters = selection.get("http_parameters")
    if not isinstance(parameters, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in parameters.items()
    ):
        raise GateError("selection lacks exact HTTP parameters")
    path = "/api/v1/query" if selection.get("mode") == "instant" else "/api/v1/query_range"
    expected_keys = (
        {"query", "time"}
        if selection.get("mode") == "instant"
        else {"query", "start", "end", "step"}
    )
    if set(parameters) != expected_keys:
        raise GateError("selection has wrong exact HTTP parameter shape")
    _wait_ready(base_url, timeout_ms)
    url = (
        f"{base_url.rstrip('/')}{path}?"
        f"{urllib.parse.urlencode(parameters)}"
    )
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "Connection": "close"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_ms / 1000) as response:
            status = response.status
            body = response.read(max_response_bytes + 1)
            headers = {key.lower(): value for key, value in response.headers.items()}
    except urllib.error.HTTPError as error:
        body = error.read(max_response_bytes + 1)
        with body_output.open("xb") as destination:
            destination.write(body)
        raise GateError(f"sealed query returned HTTP {error.code}") from error
    with body_output.open("xb") as destination:
        destination.write(body)
    if status != 200 or len(body) > max_response_bytes:
        raise GateError("sealed query failed or exceeded response limit")
    write_json_exclusive(headers_output, headers)
    document = json.loads(body)
    if not isinstance(document, dict) or document.get("status") != "success":
        raise GateError("sealed query returned a non-success Prometheus envelope")
    data = document.get("data")
    if not isinstance(data, dict):
        raise GateError("sealed query returned malformed Prometheus data")
    live_only_headers = {
        "x-chronoxide-view-generation",
        "x-chronoxide-visible-message-sequence",
        "x-chronoxide-catalog-revision",
        "x-chronoxide-view-age-ms",
        "x-chronoxide-view-pin-wait-ns",
        "x-chronoxide-view-pin-held-ns",
    }
    if live_only_headers.intersection(headers):
        raise GateError("oracle accidentally queried a live/head-capable endpoint")
    stats_text = headers.get("x-chronoxide-query-stats")
    query_io_text = headers.get("x-chronoxide-query-io")
    if stats_text is None or query_io_text is None:
        raise GateError("sealed response lacks query stats or query I/O")
    stats = json.loads(stats_text)
    query_io = json.loads(query_io_text)
    if not isinstance(stats, dict) or set(stats) != QUERY_STATS_FIELDS:
        raise GateError("sealed response has malformed complete QueryStats")
    if not isinstance(query_io, dict) or set(query_io) != QUERY_IO_FIELDS:
        raise GateError("sealed response has malformed query I/O")
    for field, value in stats.items():
        nonnegative_int(value, f"sealed QueryStats.{field}")
    for field, value in query_io.items():
        nonnegative_int(value, f"sealed query I/O.{field}")
    cardinality, samples = _cardinality(data)
    digest = canonical_sha256(data)
    expected = selection["live_response"]
    matches = {
        "response_data_sha256": digest == expected["response_data_sha256"],
        "cardinality": cardinality == expected["cardinality"],
        "samples": samples == expected["samples"],
    }
    complete = (
        all(matches.values())
        and cardinality > 0
        and samples > 0
        and stats["segments_queried"] > 0
    )
    result = {
        "schema": COMPARISON_SCHEMA,
        "complete": complete,
        "oracle_kind": "head-vs-sealed-storage-path",
        "independent_promql_evaluator": False,
        "ordering_sensitive_fail_closed": True,
        "exact_http_path": path,
        "exact_http_parameters": parameters,
        "live_head": {
            "query_name": selection["query_name"],
            "generation": selection["generation"],
            "response_data_sha256": expected["response_data_sha256"],
            "cardinality": expected["cardinality"],
            "samples": expected["samples"],
            "segments_queried": expected["query_stats"]["segments_queried"],
            "visible_message_sequence": selection["visible_message_sequence"],
        },
        "fresh_sealed_prefix": {
            "response_data_sha256": digest,
            "cardinality": cardinality,
            "samples": samples,
            "query_stats": stats,
            "query_io": query_io,
            "response_bytes": len(body),
        },
        "matches": matches,
        "interpretation": (
            "Equality is head-versus-sealed storage-path evidence. The two paths "
            "share the Chronoxide PromQL evaluator. Array ordering is intentionally "
            "retained, so mismatch fails closed and may be ordering-only."
        ),
    }
    write_json_exclusive(comparison_output, result)
    if not complete:
        raise GateError("fresh sealed prefix did not exactly reproduce the live head result")
    return result


def _read_status(path: Path, context: str) -> int:
    regular_file(path, context)
    text = path.read_text(encoding="utf-8").strip()
    if re.fullmatch(r"[0-9]+", text) is None:
        raise GateError(f"{context} is malformed")
    return int(text)


def gate_final(
    selection_path: Path,
    config_gate_path: Path,
    replay_gate_path: Path,
    comparison_path: Path,
    replay_status_path: Path,
    query_status_path: Path,
    supervisor_status_path: Path,
    termination_path: Path,
    output: Path,
) -> dict[str, Any]:
    selection = load_json(selection_path)
    config = load_json(config_gate_path)
    replay = load_json(replay_gate_path)
    comparison = load_json(comparison_path)
    termination = load_json(termination_path)
    if (
        selection.get("schema") != SELECTION_SCHEMA
        or config.get("schema") != CONFIG_SCHEMA
        or replay.get("schema") != REPLAY_SCHEMA
        or comparison.get("schema") != COMPARISON_SCHEMA
        or config.get("complete") is not True
        or replay.get("complete") is not True
        or comparison.get("complete") is not True
    ):
        raise GateError("oracle stage gate is incomplete")
    statuses = {
        "prefix_replay": _read_status(replay_status_path, "prefix replay status"),
        "sealed_query": _read_status(query_status_path, "sealed query status"),
        "api_supervisor": _read_status(
            supervisor_status_path, "API supervisor status"
        ),
    }
    if any(value != 0 for value in statuses.values()):
        raise GateError(f"an oracle stage did not exit zero: {statuses}")
    cut = selection.get("visible_message_sequence")
    mode = selection.get("mode")
    expected_path = "/api/v1/query" if mode == "instant" else "/api/v1/query_range"
    matches = comparison.get("matches")
    expected_live = selection.get("live_response", {})
    fresh = comparison.get("fresh_sealed_prefix", {})
    fresh_stats = fresh.get("query_stats", {}) if isinstance(fresh, dict) else {}
    if (
        config.get("selected_message_prefix") != cut
        or replay.get("exact_message_prefix") != cut
        or comparison.get("exact_http_path") != expected_path
        or comparison.get("exact_http_parameters") != selection.get("http_parameters")
        or comparison.get("live_head", {}).get("query_name")
        != selection.get("query_name")
        or comparison.get("live_head", {}).get("generation")
        != selection.get("generation")
        or comparison.get("live_head", {}).get("visible_message_sequence") != cut
        or comparison.get("live_head", {}).get("response_data_sha256")
        != expected_live.get("response_data_sha256")
        or comparison.get("live_head", {}).get("cardinality")
        != expected_live.get("cardinality")
        or comparison.get("live_head", {}).get("samples")
        != expected_live.get("samples")
        or fresh.get("response_data_sha256")
        != expected_live.get("response_data_sha256")
        or fresh.get("cardinality") != expected_live.get("cardinality")
        or fresh.get("samples") != expected_live.get("samples")
        or not isinstance(fresh_stats, dict)
        or fresh_stats.get("segments_queried", 0) <= 0
        or not isinstance(matches, dict)
        or set(matches) != {"response_data_sha256", "cardinality", "samples"}
        or any(value is not True for value in matches.values())
    ):
        raise GateError("oracle stage documents are not bound to one selection/cut/query")
    if (
        not isinstance(termination, dict)
        or termination.get("expected") is not True
        or termination.get("signal") != "SIGTERM"
        or termination.get("shell_status") != 143
    ):
        raise GateError("sealed API child did not terminate with the expected SIGTERM")
    result = {
        "schema": FINAL_SCHEMA,
        "complete": True,
        "oracle_kind": "head-vs-sealed-storage-path",
        "independent_promql_evaluator": False,
        "ordering_sensitive_fail_closed": True,
        "exact_message_prefix": selection["visible_message_sequence"],
        "query_name": selection["query_name"],
        "stage_exit_statuses": statuses,
        "api_child_termination": termination,
        "response_data_sha256": comparison["fresh_sealed_prefix"][
            "response_data_sha256"
        ],
        "cardinality": comparison["fresh_sealed_prefix"]["cardinality"],
        "samples": comparison["fresh_sealed_prefix"]["samples"],
        "all_exact_comparisons_match": all(comparison["matches"].values()),
    }
    write_json_exclusive(output, result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    select = subparsers.add_parser("select")
    select.add_argument("--result-root", type=Path, required=True)
    select.add_argument("--output", type=Path, required=True)
    select.add_argument("--query-name")

    render = subparsers.add_parser("render-prefix-config")
    render.add_argument("--result-root", type=Path, required=True)
    render.add_argument("--selection", type=Path, required=True)
    render.add_argument("--segments-dir", type=Path, required=True)
    render.add_argument("--output", type=Path, required=True)
    render.add_argument("--gate-output", type=Path, required=True)
    render.add_argument("--q-config", type=Path)

    replay = subparsers.add_parser("validate-prefix-replay")
    replay.add_argument("--selection", type=Path, required=True)
    replay.add_argument("--replay", type=Path, required=True)
    replay.add_argument("--corpus-summary", type=Path, required=True)
    replay.add_argument("--output", type=Path, required=True)

    api_args = subparsers.add_parser("emit-api-args")
    api_args.add_argument("--config", type=Path, required=True)
    api_args.add_argument("--segments-dir", type=Path, required=True)
    api_args.add_argument("--listen", required=True)

    listen = subparsers.add_parser("check-listen-free")
    listen.add_argument("--listen", required=True)

    query = subparsers.add_parser("query-sealed")
    query.add_argument("--base-url", required=True)
    query.add_argument("--selection", type=Path, required=True)
    query.add_argument("--body-output", type=Path, required=True)
    query.add_argument("--headers-output", type=Path, required=True)
    query.add_argument("--comparison-output", type=Path, required=True)
    query.add_argument("--timeout-ms", type=int, default=30000)
    query.add_argument("--max-response-bytes", type=int, default=67108864)

    final = subparsers.add_parser("gate-final")
    final.add_argument("--selection", type=Path, required=True)
    final.add_argument("--config-gate", type=Path, required=True)
    final.add_argument("--replay-gate", type=Path, required=True)
    final.add_argument("--comparison", type=Path, required=True)
    final.add_argument("--replay-status", type=Path, required=True)
    final.add_argument("--query-status", type=Path, required=True)
    final.add_argument("--supervisor-status", type=Path, required=True)
    final.add_argument("--termination", type=Path, required=True)
    final.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "select":
            result = select_candidate(args.result_root, args.output, args.query_name)
            print(json.dumps(result, sort_keys=True))
        elif args.command == "render-prefix-config":
            result = render_prefix_config(
                args.result_root,
                args.selection,
                args.segments_dir,
                args.output,
                args.gate_output,
                args.q_config,
            )
            print(json.dumps(result, sort_keys=True))
        elif args.command == "validate-prefix-replay":
            result = validate_prefix_replay(
                args.selection, args.replay, args.corpus_summary, args.output
            )
            print(json.dumps(result, sort_keys=True))
        elif args.command == "emit-api-args":
            emit_api_args(args.config, args.segments_dir, args.listen)
        elif args.command == "check-listen-free":
            check_listen_free(args.listen)
        elif args.command == "query-sealed":
            if args.timeout_ms <= 0 or args.max_response_bytes <= 0:
                raise GateError("query timeout/response limit must be positive")
            result = query_sealed(
                args.base_url,
                args.selection,
                args.body_output,
                args.headers_output,
                args.comparison_output,
                args.timeout_ms,
                args.max_response_bytes,
            )
            print(json.dumps(result, sort_keys=True))
        elif args.command == "gate-final":
            result = gate_final(
                args.selection,
                args.config_gate,
                args.replay_gate,
                args.comparison,
                args.replay_status,
                args.query_status,
                args.supervisor_status,
                args.termination,
                args.output,
            )
            print(json.dumps(result, sort_keys=True))
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
        print(f"live query prefix oracle: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
