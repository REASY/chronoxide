#!/usr/bin/env python3

from __future__ import annotations

import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("phase5_head_topology_guard.py")
GUARD = types.ModuleType("phase5_head_topology_guard")
GUARD.__file__ = str(MODULE_PATH)
exec(compile(MODULE_PATH.read_bytes(), str(MODULE_PATH), "exec"), GUARD.__dict__)


class HeadTopologyGuardTests(unittest.TestCase):
    def test_forbidden_process_contract(self) -> None:
        for name in (
            "cargo",
            "perf",
            "chronoxide-query",
            "postgres:writer",
            "java",
            "qemu-system-aar",
            "qemu-kvm",
            "emulator",
            "adb",
            "cargo-nextest",
            "ninja.real",
            "ld.bfd",
            "ld.gold",
            "clang++.real",
            "clang-19.real",
            "gcc-14",
            "soong_ui.bash",
            "btop",
            "htop",
            "top",
            "BTOP",
            "Chronoxide-query",
        ):
            self.assertTrue(GUARD.is_forbidden(name), name)
        for command in (
            "bash /src/build/soong/soong_ui.bash --make-mode",
            "python /home/u/.cargo/bin/cargo-nextest nextest run",
            "bash /src/prebuilts/build-tools/bin/ninja.real -C out",
            "bash /usr/bin/ld.bfd -o output",
            "bash /android/prebuilts/clang++.real -c source.cc",
            "bash /opt/llvm/bin/clang-19.real -c source.cc",
            "worker Android SDK emulator launch",
        ):
            self.assertTrue(GUARD.is_forbidden("bash", command), command)
        self.assertTrue(
            GUARD.is_forbidden(
                "worker", "java org.gradle.process.internal.worker.GradleWorkerMain"
            )
        )
        self.assertTrue(
            GUARD.is_forbidden("worker", "java com.android.build.gradle.internal.tasks.Workers")
        )
        for name in ("bash", "python3", "systemd", "codex", "topology-helper"):
            self.assertFalse(GUARD.is_forbidden(name), name)

    def test_lookalike_process_names_remain_permitted(self) -> None:
        for name in ("topic-worker", "toplevel", "htopology", "btopology", "adbd"):
            self.assertFalse(GUARD.is_forbidden(name, name), name)

    def test_descendants_exclude_unrelated_processes(self) -> None:
        process = GUARD.Process
        processes = {
            10: process(10, 1, "bash", "bash"),
            11: process(11, 10, "perf", "perf"),
            12: process(12, 11, "chronoxide", "chronoxide"),
            20: process(20, 1, "cargo", "cargo"),
        }
        self.assertEqual(GUARD.descendants(processes, 10), {10, 11, 12})

    def test_cadence_derivation_includes_first_and_terminal_edges(self) -> None:
        self.assertEqual(
            GUARD.derive_guardian_maximum_poll_start_gap_ns(
                [90_000_000, 190_000_000], 270_000_000
            ),
            100_000_000,
        )
        self.assertEqual(GUARD.guardian_maximum_allowed_gap_ns(100), 200_000_000)
        with self.assertRaisesRegex(GUARD.GuardError, "strictly"):
            GUARD.derive_guardian_maximum_poll_start_gap_ns([1, 1], 2)
        with self.assertRaisesRegex(GUARD.GuardError, "terminal"):
            GUARD.derive_guardian_maximum_poll_start_gap_ns([3], 2)

    def test_process_tree_rejects_reuse_zombies_and_parent_races(self) -> None:
        identities = {
            10: {"pid": 10, "ppid": 1, "state": "S", "starttime_ticks": 100},
            11: {"pid": 11, "ppid": 10, "state": "S", "starttime_ticks": 110},
            12: {"pid": 12, "ppid": 10, "state": "Z", "starttime_ticks": 120},
            13: {"pid": 13, "ppid": 99, "state": "S", "starttime_ticks": 130},
            14: {"pid": 14, "ppid": 10, "state": "x", "starttime_ticks": 140},
        }
        children = {10: [11, 12, 13, 14], 11: [], 12: [], 13: [], 14: []}
        with (
            mock.patch.object(GUARD, "read_process_stat_identity", side_effect=identities.get),
            mock.patch.object(GUARD, "read_process_children", side_effect=lambda pid: children[pid]),
        ):
            self.assertEqual(GUARD.process_tree(10, 100), {10, 11})
            self.assertEqual(GUARD.process_tree(10, 999), set())
        self.assertFalse(
            GUARD.process_identity_is_running(
                {"pid": 14, "ppid": 10, "state": "x", "starttime_ticks": 140}
            )
        )

    def test_snapshot_is_deepest_first_and_excludes_dead_nodes(self) -> None:
        identities = {
            10: {"pid": 10, "ppid": 1, "state": "S", "starttime_ticks": 100},
            11: {"pid": 11, "ppid": 10, "state": "S", "starttime_ticks": 110},
            12: {"pid": 12, "ppid": 11, "state": "S", "starttime_ticks": 120},
            13: {"pid": 13, "ppid": 11, "state": "Z", "starttime_ticks": 130},
        }
        entries = [types.SimpleNamespace(name=str(pid)) for pid in identities]
        with (
            mock.patch.object(GUARD.Path, "iterdir", return_value=entries),
            mock.patch.object(GUARD, "read_process_stat_identity", side_effect=identities.get),
        ):
            snapshot = GUARD.snapshot_process_tree_identities(10, 100)
        self.assertEqual([row["pid"] for row in snapshot], [12, 11, 10])
        self.assertEqual([row["depth"] for row in snapshot], [2, 1, 0])

    def test_termination_revalidates_identity_before_every_signal(self) -> None:
        targets = [
            {"pid": 12, "ppid": 11, "state": "S", "starttime_ticks": 120, "depth": 2},
            {"pid": 11, "ppid": 10, "state": "S", "starttime_ticks": 110, "depth": 1},
            {"pid": 10, "ppid": 1, "state": "S", "starttime_ticks": 100, "depth": 0},
        ]
        current = {
            12: {"pid": 12, "ppid": 11, "state": "Z", "starttime_ticks": 120},
            11: {"pid": 11, "ppid": 10, "state": "S", "starttime_ticks": 999},
            10: {"pid": 10, "ppid": 1, "state": "S", "starttime_ticks": 100},
        }
        sent: list[tuple[int, int]] = []
        with (
            mock.patch.object(GUARD, "snapshot_process_tree_identities", return_value=targets),
            mock.patch.object(GUARD, "read_process_stat_identity", side_effect=current.get),
            mock.patch.object(GUARD, "wait_for_process_identities_exit"),
            mock.patch.object(GUARD.os, "kill", side_effect=lambda pid, sig: sent.append((pid, sig))),
        ):
            evidence = GUARD.terminate_process_tree(10, 100)
        self.assertEqual([pid for pid, _signal in sent], [10, 10])
        self.assertEqual(
            evidence["identity_refusals"],
            [
                {"pid": 11, "signal": "TERM", "reason": "starttime_mismatch"},
                {"pid": 11, "signal": "KILL", "reason": "starttime_mismatch"},
            ],
        )

    def test_control_is_atomic_read_only_and_requires_both_ready_markers(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            control = root / "control.json"
            guardian_ready = root / "guardian-ready"
            rss_ready = root / "rss-ready"
            launch = root / "launch"
            identities = {
                pid: {"pid": pid, "ppid": 1, "state": "S", "starttime_ticks": pid * 10}
                for pid in (10, 20, 30)
            }
            with mock.patch.object(
                GUARD, "read_process_stat_identity", side_effect=identities.get
            ):
                value = GUARD.create_control(
                    control, guardian_ready, rss_ready, launch, 10, 20, 30, 100
                )
            self.assertEqual(control.stat().st_mode & 0o777, 0o444)
            self.assertEqual(value["root_starttime_ticks"], 100)
            GUARD.create_empty_read_only_marker(guardian_ready, "guardian ready marker")
            with mock.patch.object(
                GUARD, "validate_control", return_value=value
            ), self.assertRaisesRegex(GUARD.GuardError, "both monitors"):
                GUARD.wait_ready(
                    control, guardian_ready, rss_ready, launch, 100, 100
                )
            GUARD.create_empty_read_only_marker(rss_ready, "RSS ready marker")
            with mock.patch.object(
                GUARD, "validate_control", return_value=value
            ):
                released = GUARD.release_launch(
                    control, guardian_ready, rss_ready, launch, 100
                )
            self.assertEqual(released["status"], "released")
            self.assertEqual(launch.stat().st_mode & 0o777, 0o444)

    def test_wait_ready_rejects_an_early_launch_marker(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            for name in ("guardian-ready", "launch"):
                GUARD.create_empty_read_only_marker(root / name, name)
            with (
                mock.patch.object(
                    GUARD, "validate_control", return_value={"root_pid": 10}
                ),
                self.assertRaisesRegex(GUARD.GuardError, "before both monitors"),
            ):
                GUARD.wait_ready(
                    root / "control.json",
                    root / "guardian-ready",
                    root / "rss-ready",
                    root / "launch",
                    100,
                    100,
                )

    def test_cleanup_order_is_root_then_rss_then_guardian(self) -> None:
        control = {
            "root_pid": 10,
            "root_starttime_ticks": 100,
            "rss_monitor_pid": 30,
            "rss_monitor_starttime_ticks": 300,
            "guardian_pid": 20,
            "guardian_starttime_ticks": 200,
        }
        calls: list[tuple[int, int]] = []
        complete = {
            "term_errors": [],
            "kill_errors": [],
            "surviving_pids": [],
        }
        with (
            mock.patch.object(GUARD, "validate_control", return_value=control),
            mock.patch.object(
                GUARD,
                "terminate_process_tree",
                side_effect=lambda pid, start: calls.append((pid, start)) or complete,
            ),
            mock.patch.object(GUARD, "sha256_file", return_value="0" * 64),
        ):
            evidence = GUARD.cleanup_controlled_processes(
                Path("/control"),
                Path("/guardian-ready"),
                Path("/rss-ready"),
                Path("/launch"),
                100,
            )
        self.assertEqual(calls, [(10, 100), (30, 300), (20, 200)])
        self.assertEqual(evidence["termination_order"], ["root", "rss_monitor", "guardian"])

    def test_early_conflict_scan_is_published_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            output = Path(raw) / "conflicts.json"
            conflict = {
                "pid": 20,
                "ppid": 1,
                "state": "S",
                "starttime_ticks": 200,
                "name": "btop",
                "command": "btop",
            }
            with (
                mock.patch.object(GUARD, "scan_conflicts", return_value=[conflict]),
                self.assertRaisesRegex(GUARD.GuardError, "conflict"),
            ):
                GUARD.record_conflict_scan(output)
            document = json.loads(output.read_text(encoding="utf-8"))
            self.assertFalse(document["quiet"])
            self.assertEqual(document["conflicts"], [conflict])
            self.assertEqual(output.stat().st_mode & 0o777, 0o444)

    def test_rss_monitor_emits_ready_before_launch_and_a_terminal_sample(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            control = root / "control.json"
            control.write_text("sealed\n", encoding="utf-8")
            status = {
                "VmRSS": 10,
                "VmHWM": 12,
                "RssAnon": 6,
                "RssFile": 4,
                "VmSwap": 0,
            }
            with (
                mock.patch.object(
                    GUARD,
                    "_wait_for_bound_control",
                    return_value=({"root_starttime_ticks": 100}, 100),
                ),
                mock.patch.object(
                    GUARD.time,
                    "monotonic_ns",
                    side_effect=[0, 0, 1, 100_000_000, 100_000_001, 100_000_002],
                ),
                mock.patch.object(
                    GUARD, "process_is_same_running", side_effect=[True, False]
                ),
                mock.patch.object(GUARD, "_launch_observed", side_effect=[False, True]),
                mock.patch.object(GUARD, "process_tree", return_value={10}),
                mock.patch.object(GUARD, "_status_kib", return_value=status),
            ):
                summary = GUARD.monitor_rss(
                    10,
                    root / "rss.tsv",
                    root / "rss.json",
                    control,
                    root / "guardian-ready",
                    root / "rss-ready",
                    root / "launch",
                    100,
                )
            self.assertTrue(summary["complete"])
            self.assertEqual(summary["ready_created_poll"], 1)
            self.assertEqual(summary["launch_observed_poll"], 2)
            rows = (root / "rss.tsv").read_text(encoding="utf-8").splitlines()
            self.assertIn("\ttrue\tfalse\t", rows[1])
            self.assertIn("\tfalse\ttrue\t0\t0\t0\t0\t0\t0\t", rows[2])

    def test_disk_violation_uses_control_captured_root_identity(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            control = root / "control.json"
            control.write_text("sealed-control\n", encoding="utf-8")
            termination = GUARD._empty_termination(123)
            termination["attempted"] = True
            with (
                mock.patch.object(
                    GUARD,
                    "_wait_for_bound_control",
                    return_value=({"root_starttime_ticks": 123}, 123),
                ),
                mock.patch.object(GUARD, "process_is_same_running", return_value=True),
                mock.patch.object(GUARD, "scan_conflicts", return_value=[]),
                mock.patch.object(
                    GUARD.os,
                    "statvfs",
                    return_value=types.SimpleNamespace(f_bavail=1, f_frsize=1),
                ),
                mock.patch.object(
                    GUARD, "terminate_process_tree", return_value=termination
                ) as terminate,
                self.assertRaisesRegex(GUARD.GuardError, "exact lifecycle"),
            ):
                GUARD.monitor_guardian(
                    10,
                    root,
                    100,
                    root / "disk.tsv",
                    root / "process.tsv",
                    root / "violation.json",
                    root / "summary.json",
                    control,
                    root / "guardian-ready",
                    root / "rss-ready",
                    root / "launch",
                    100,
                )
            terminate.assert_called_once_with(10, 123)
            violation = json.loads(
                (root / "violation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(violation["reason"], "disk-reserve")


if __name__ == "__main__":
    unittest.main()
