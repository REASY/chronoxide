#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import signal
import stat
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


def load_exact_source_sibling(name: str, filename: str) -> types.ModuleType:
    path = Path(__file__).resolve(strict=True).parent / filename
    module = types.ModuleType(name)
    module.__file__ = str(path)
    module.__package__ = None
    module.__cached__ = None
    sys.modules[name] = module
    exec(
        compile(path.read_bytes(), str(path), "exec", dont_inherit=True),
        module.__dict__,
    )
    return module


guard = load_exact_source_sibling(
    "phase4_range_one_pass_guard", "phase4_range_one_pass_guard.py"
)


def identity(pid: int, ppid: int, starttime: int, state: str = "S") -> dict[str, int | str]:
    return {
        "pid": pid,
        "ppid": ppid,
        "state": state,
        "starttime_ticks": starttime,
    }


class ClassifierTests(unittest.TestCase):
    def test_exact_build_monitor_and_android_conflicts_preserve_lookalikes(self) -> None:
        forbidden = (
            guard.Process(1, 0, "btop", "btop"),
            guard.Process(2, 0, "HTOP", "HTOP"),
            guard.Process(3, 0, "top", "top"),
            guard.Process(4, 0, "clang-19.real", "/opt/clang-19.real -c x.cc"),
            guard.Process(5, 0, "bash", "/src/build/soong/soong_ui.bash --make-mode"),
            guard.Process(6, 0, "adb", "adb devices"),
            guard.Process(7, 0, "emulator", "/Android/Sdk/emulator/emulator @pixel"),
        )
        for process in forbidden:
            with self.subTest(process=process):
                self.assertTrue(guard.is_forbidden_process(process))
        for name in ("topic-worker", "toplevel", "htopology", "btopology", "adbd"):
            with self.subTest(name=name):
                self.assertFalse(
                    guard.is_forbidden_process(guard.Process(20, 1, name, name))
                )
        self.assertFalse(
            guard.is_forbidden_process(
                guard.Process(30, 1, "qemu-system-aarch64", "qemu-system-aarch64 -name idle")
            )
        )
        self.assertTrue(
            guard.is_forbidden_process(
                guard.Process(
                    31,
                    1,
                    "qemu-system-aarch64",
                    "qemu-system-aarch64 -name android-ranchu",
                )
            )
        )

    def test_process_table_enumeration_failure_is_not_a_quiet_sample(self) -> None:
        with mock.patch.object(Path, "iterdir", side_effect=OSError("unavailable")):
            with self.assertRaisesRegex(guard.GuardError, "enumerate"):
                guard.read_processes()


class IdentityAndCleanupTests(unittest.TestCase):
    def test_running_identity_rejects_pid_reuse_and_z_x_states(self) -> None:
        for state in ("Z", "X", "x"):
            self.assertFalse(guard.process_identity_is_running(identity(9, 1, 10, state)))
        with mock.patch.object(
            guard, "read_process_stat_identity", return_value=identity(9, 1, 11)
        ):
            self.assertFalse(guard.process_is_same_running(9, 10))
        with mock.patch.object(
            guard, "read_process_stat_identity", return_value=identity(9, 1, 10)
        ):
            self.assertTrue(guard.process_is_same_running(9, 10))

    def test_tree_snapshot_is_deepest_first_and_excludes_dead_descendants(self) -> None:
        entries = [Path("/proc/10"), Path("/proc/11"), Path("/proc/12"), Path("/proc/13")]
        identities = {
            10: identity(10, 1, 100),
            11: identity(11, 10, 110),
            12: identity(12, 11, 120),
            13: identity(13, 10, 130, "x"),
        }
        original_iterdir = Path.iterdir

        def iterdir(path: Path):
            if path == Path("/proc"):
                return iter(entries)
            return original_iterdir(path)

        with mock.patch.object(Path, "iterdir", iterdir), mock.patch.object(
            guard,
            "read_process_stat_identity",
            side_effect=lambda pid: identities.get(pid),
        ):
            snapshot = guard.snapshot_process_tree_identities(10, 100)
        self.assertEqual([row["pid"] for row in snapshot], [12, 11, 10])
        self.assertEqual([row["depth"] for row in snapshot], [2, 1, 0])

    def test_signal_revalidation_allows_reparented_child_and_refuses_reused_pid(self) -> None:
        targets = [
            {**identity(11, 10, 110), "depth": 1},
            {**identity(10, 1, 100), "depth": 0},
        ]

        def current(pid: int):
            if pid == 11:
                return identity(11, 1, 110)  # reparented, but exact PID/starttime
            return identity(10, 1, 999)  # reused root PID

        signals: list[tuple[int, signal.Signals]] = []
        with mock.patch.object(
            guard, "snapshot_process_tree_identities", return_value=targets
        ), mock.patch.object(
            guard, "read_process_stat_identity", side_effect=current
        ), mock.patch.object(
            guard, "wait_for_process_identities_exit"
        ), mock.patch.object(
            guard.os, "kill", side_effect=lambda pid, sig: signals.append((pid, sig))
        ):
            result = guard.terminate_process_tree(10, 100)
        self.assertEqual(
            signals,
            [(11, signal.SIGTERM), (11, signal.SIGKILL)],
        )
        self.assertEqual(
            result["identity_refusals"],
            [
                {"pid": 10, "signal": "TERM", "reason": "starttime_mismatch"},
                {"pid": 10, "signal": "KILL", "reason": "starttime_mismatch"},
            ],
        )

    def test_controlled_cleanup_is_root_first(self) -> None:
        control = {
            "root_pid": 11,
            "root_starttime_ticks": 110,
            "guardian_pid": 12,
            "guardian_starttime_ticks": 120,
        }
        calls: list[tuple[int, int]] = []
        clean = guard._empty_termination(1)

        def terminate(pid: int, start: int):
            calls.append((pid, start))
            return {**clean, "root_starttime_ticks": start}

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            control_path = path / "control.json"
            control_path.write_bytes(b"{}\n")
            with mock.patch.object(guard, "validate_control", return_value=control), mock.patch.object(
                guard, "terminate_process_tree", side_effect=terminate
            ):
                result = guard.cleanup_controlled_processes(
                    control_path,
                    path / "ready",
                    path / "launch",
                    guard.CADENCE_INTERVAL_MS,
                )
        self.assertEqual(calls, [(11, 110), (12, 120)])
        self.assertEqual(result["termination_order"], ["root", "guardian"])

    def test_monitor_error_cleanup_uses_control_captured_starttime(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with mock.patch.object(
                guard,
                "validate_control",
                return_value={"root_pid": 11, "root_starttime_ticks": 777},
            ), mock.patch.object(
                guard, "terminate_process_tree", return_value={"attempted": True}
            ) as terminate:
                guard.terminate_root_from_control(
                    root / "control",
                    root / "ready",
                    root / "launch",
                    guard.CADENCE_INTERVAL_MS,
                    11,
                )
        terminate.assert_called_once_with(11, 777)


class HandshakeTests(unittest.TestCase):
    def test_control_is_atomic_exclusive_mode_0444_and_binds_ppids(self) -> None:
        identities = {
            10: identity(10, 1, 100),
            11: identity(11, 10, 110),
            12: identity(12, 10, 120),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            control = root / "control.json"
            ready = root / "ready"
            launch = root / "launch"
            with mock.patch.object(
                guard,
                "read_process_stat_identity",
                side_effect=lambda pid: identities.get(pid),
            ):
                value = guard.create_control(
                    control,
                    ready,
                    launch,
                    10,
                    11,
                    12,
                    guard.CADENCE_INTERVAL_MS,
                )
                with self.assertRaisesRegex(guard.GuardError, "reuse"):
                    guard.create_control(
                        control,
                        ready,
                        launch,
                        10,
                        11,
                        12,
                        guard.CADENCE_INTERVAL_MS,
                    )
            self.assertEqual(stat.S_IMODE(control.stat().st_mode), 0o444)
            self.assertEqual(json.loads(control.read_text()), value)
            self.assertEqual(value["root_ppid"], value["runner_pid"])
            self.assertEqual(value["guardian_ppid"], value["runner_pid"])

    def test_wait_ready_rejects_early_launch_and_release_checks_marker_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            control = root / "control"
            ready = root / "ready"
            launch = root / "launch"
            control.write_text("{}\n", encoding="utf-8")
            guard.create_empty_read_only_marker(launch, "launch marker")
            with mock.patch.object(guard, "validate_control", return_value={"root_pid": 11}):
                with self.assertRaisesRegex(guard.GuardError, "before guardian readiness"):
                    guard.wait_ready(
                        control,
                        ready,
                        launch,
                        guard.CADENCE_INTERVAL_MS,
                        1000,
                    )
            launch.chmod(0o644)
            launch.unlink()
            ready.write_bytes(b"")
            ready.chmod(0o644)
            with mock.patch.object(guard, "validate_control", return_value={"root_pid": 11}):
                with self.assertRaisesRegex(guard.GuardError, "0444"):
                    guard.release_launch(
                        control, ready, launch, guard.CADENCE_INTERVAL_MS
                    )

    def test_marker_mode_is_exact_under_a_restrictive_umask(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory).resolve() / "marker"
            previous_umask = os.umask(0o077)
            try:
                guard.create_empty_read_only_marker(marker, "test marker")
            finally:
                os.umask(previous_umask)
            self.assertEqual(stat.S_IMODE(marker.stat().st_mode), 0o444)

    def test_edge_inclusive_cadence_includes_first_and_terminal_boundaries(self) -> None:
        self.assertEqual(
            guard.derive_guardian_maximum_poll_start_gap_ns(
                [50_000_000, 150_000_000, 250_000_000], 300_000_000
            ),
            100_000_000,
        )
        self.assertEqual(
            guard.derive_guardian_maximum_poll_start_gap_ns(
                [200_000_000, 300_000_000], 500_000_000
            ),
            200_000_000,
        )
        with self.assertRaisesRegex(guard.GuardError, "increase strictly"):
            guard.derive_guardian_maximum_poll_start_gap_ns([1, 1], 2)


class MonitorTests(unittest.TestCase):
    def _control(self, root: Path) -> dict[str, object]:
        return {
            "schema": guard.CONTROL_SCHEMA,
            "runner_pid": 10,
            "runner_starttime_ticks": 100,
            "runner_ppid": 1,
            "root_pid": 11,
            "root_starttime_ticks": 110,
            "root_ppid": 10,
            "guardian_pid": os.getpid(),
            "guardian_starttime_ticks": 120,
            "guardian_ppid": 10,
            "interval_ms": guard.CADENCE_INTERVAL_MS,
            "ready_marker": str(root / "ready"),
            "launch_marker": str(root / "launch"),
        }

    def test_monitor_fsyncs_first_clean_sample_before_ready_and_records_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            control_path = root / "control.json"
            control_path.write_text("{}\n", encoding="utf-8")
            control_path.chmod(0o444)
            control = self._control(root)
            root_calls = 0
            events: list[str] = []

            def status(pid: int, expected_start: int, expected_ppid: int):
                nonlocal root_calls
                if pid == 11:
                    root_calls += 1
                    return (
                        (True, "S", expected_start, expected_ppid, None)
                        if root_calls == 1
                        else (False, "-", 0, -1, None)
                    )
                return True, "S", expected_start, expected_ppid, None

            original_marker = guard.create_empty_read_only_marker

            def marker(path: Path, description: str):
                events.append(f"marker:{description}")
                return original_marker(path, description)

            monotonic = iter([0, 0, 50_000_000, 100_000_000, 150_000_000, 200_000_000])
            with mock.patch.object(
                guard, "_wait_for_bound_control", return_value=(control, 100, 110)
            ), mock.patch.object(
                guard, "_identity_status", side_effect=status
            ), mock.patch.object(
                guard, "_observe_launch", side_effect=[False, True]
            ), mock.patch.object(
                guard, "scan_conflicts", return_value=[]
            ), mock.patch.object(
                guard.time, "monotonic_ns", side_effect=lambda: next(monotonic)
            ), mock.patch.object(
                guard.time, "sleep"
            ), mock.patch.object(
                guard.os, "fsync", side_effect=lambda _fd: events.append("fsync")
            ), mock.patch.object(
                guard, "create_empty_read_only_marker", side_effect=marker
            ):
                result = guard.monitor_guardian(
                    10,
                    11,
                    root / "samples.tsv",
                    root / "conflicts.tsv",
                    root / "summary.json",
                    control_path,
                    root / "ready",
                    root / "launch",
                    guard.CADENCE_INTERVAL_MS,
                )
            self.assertTrue(result["complete_and_conflict_free"])
            self.assertEqual(result["terminal_sample_poll"], 2)
            self.assertLess(events.index("fsync"), events.index("marker:guardian ready marker"))
            rows = (root / "samples.tsv").read_text(encoding="utf-8").splitlines()
            self.assertIn("\ttrue\tS\t110\t10\t", rows[1])
            self.assertIn("\tfalse\t-\t0\t-1\t", rows[2])
            self.assertEqual(stat.S_IMODE((root / "ready").stat().st_mode), 0o444)
            self.assertEqual(stat.S_IMODE((root / "summary.json").stat().st_mode), 0o444)

    def test_transient_build_conflict_terminates_control_bound_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            control_path = root / "control.json"
            control_path.write_text("{}\n", encoding="utf-8")
            control_path.chmod(0o444)
            control = self._control(root)
            root_calls = 0

            def status(pid: int, expected_start: int, expected_ppid: int):
                nonlocal root_calls
                if pid == 11:
                    root_calls += 1
                    return (
                        (True, "S", expected_start, expected_ppid, None)
                        if root_calls == 1
                        else (False, "-", 0, -1, None)
                    )
                return True, "S", expected_start, expected_ppid, None

            conflict = {
                "pid": 99,
                "ppid": 1,
                "state": "S",
                "starttime_ticks": 999,
                "cpu_percent": 1.0,
                "name": "clang-19.real",
                "command": "/opt/clang-19.real -c x.cc",
            }
            termination = {
                **guard._empty_termination(110),
                "attempted": True,
                "target_pids": [11],
            }
            monotonic = iter(
                [0, 0, 50_000_000, 55_000_000, 60_000_000, 70_000_000, 80_000_000]
            )
            with mock.patch.object(
                guard, "_wait_for_bound_control", return_value=(control, 100, 110)
            ), mock.patch.object(
                guard, "_identity_status", side_effect=status
            ), mock.patch.object(
                guard, "_observe_launch", side_effect=[False, False]
            ), mock.patch.object(
                guard, "scan_conflicts", side_effect=[[conflict], []]
            ), mock.patch.object(
                guard, "terminate_process_tree", return_value=termination
            ) as terminate, mock.patch.object(
                guard.time, "monotonic_ns", side_effect=lambda: next(monotonic)
            ), mock.patch.object(guard.time, "sleep"):
                with self.assertRaisesRegex(guard.GuardError, "exact lifecycle"):
                    guard.monitor_guardian(
                        10,
                        11,
                        root / "samples.tsv",
                        root / "conflicts.tsv",
                        root / "summary.json",
                        control_path,
                        root / "ready",
                        root / "launch",
                        guard.CADENCE_INTERVAL_MS,
                    )
            terminate.assert_called_once_with(11, 110)
            summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["conflicts"], [conflict])
            self.assertTrue(summary["termination"]["attempted"])


if __name__ == "__main__":
    unittest.main()
