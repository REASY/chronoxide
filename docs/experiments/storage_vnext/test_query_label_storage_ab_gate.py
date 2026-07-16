#!/usr/bin/env python3

from __future__ import annotations

import copy
import unittest

import query_label_storage_ab_gate as gate


def full_labels() -> dict[str, int]:
    return {
        "rows_integrity_checked": 2,
        "pairs_integrity_checked": 8,
        "rows_full_materialized": 2,
        "rows_selectively_materialized": 0,
        "pairs_materialized": 8,
        "pairs_omitted": 0,
        "content_bytes_materialized": 80,
    }


def symbols() -> dict[str, object]:
    value: dict[str, object] = {
        field: 0 for field in gate.SYMBOL_READ_FIELDS - gate.SYMBOL_READ_COUNT_FIELDS
    }
    value.update(
        {field: {"calls": 1, "bytes": 2} for field in gate.SYMBOL_READ_COUNT_FIELDS}
    )
    return value


def equivalent_run() -> dict[str, object]:
    return {
        "semantic_fingerprint": "a" * 64,
        "portable_fingerprint": "b" * 64,
        "result_series": 2,
        "result_samples": 2,
        "stats": {"bytes_read": 4},
        "payload": {"logical_used_bytes": 4, "physical_reads": 1, "physical_bytes": 4},
        "labels": full_labels(),
        "range_cache": None,
        "symbols": symbols(),
    }


class QueryLabelStorageGateTests(unittest.TestCase):
    def test_full_materialization_requires_every_checked_pair(self) -> None:
        self.assertEqual(
            gate.validate_full_materialization(full_labels(), "fixture"), full_labels()
        )
        broken = full_labels()
        broken["pairs_materialized"] -= 1
        with self.assertRaisesRegex(gate.GateError, "not every integrity-checked pair"):
            gate.validate_full_materialization(broken, "fixture")

    def test_atom_accounting_is_reconciled_and_owned_activity_is_rejected(self) -> None:
        shared = {
            "label_sets": 2,
            "atom_lookups": 8,
            "atom_hits": 5,
            "atom_misses": 3,
            "unique_content_bytes": 20,
        }
        self.assertEqual(
            gate.validate_label_storage(shared, "shared-atoms", "fixture"), shared
        )
        broken = dict(shared)
        broken["atom_hits"] -= 1
        with self.assertRaisesRegex(gate.GateError, "accounting is incomplete"):
            gate.validate_label_storage(broken, "shared-atoms", "fixture")
        with self.assertRaisesRegex(gate.GateError, "owned query labels"):
            gate.validate_label_storage(shared, "owned-strings", "fixture")

    def test_page_validation_time_is_the_only_symbol_counter_exception(self) -> None:
        owned = equivalent_run()
        shared = copy.deepcopy(owned)
        shared["symbols"]["page_validation_ns_delta"] = 999
        gate.compare_equivalent_runs(owned, shared, "fixture")
        shared["symbols"]["page_read_delta"]["bytes"] += 1
        with self.assertRaisesRegex(gate.GateError, "symbol/integrity"):
            gate.compare_equivalent_runs(owned, shared, "fixture")

    def test_empty_result_control_must_not_touch_label_storage(self) -> None:
        run = {
            "query_label_storage": {
                "label_sets": 0,
                "atom_lookups": 0,
                "atom_hits": 0,
                "atom_misses": 0,
                "unique_content_bytes": 0,
            },
            "result_series": 0,
            "result_samples": 0,
        }
        totals = gate.validate_process_atom_activity(
            "shared-atoms", "empty-result-control", [run], "fixture"
        )
        self.assertFalse(any(totals.values()))


if __name__ == "__main__":
    unittest.main()
