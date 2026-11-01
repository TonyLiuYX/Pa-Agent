import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import weakref
from unittest.mock import patch

from connectors import DemoProvider
from runtime.config import RuntimeConfig
from runtime.runner import Runner
from runtime.soak import REQUIRED_SECONDS, SoakFailure, check_core, core_manifest, invariants, percentiles, run_soak


class SoakTests(unittest.TestCase):
    def test_short_run_never_claims_ten_hour_acceptance(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary) / "smoke"
            report = run_soak(directory, duration_seconds=.3, pace=.01, fault_interval_seconds=10, status_interval_seconds=.1,
                              burst_size=5, demo_minutes=15)
            self.assertEqual(report["status"], "smoke_passed")
            self.assertEqual(report["required_seconds"], 36000)
            self.assertEqual(REQUIRED_SECONDS, 36000)
            self.assertLess(report["observation_seconds"], 36000)
            self.assertTrue(report["replay"]["passed"])
            self.assertGreater(report["counters"]["raw_received"], 0)
            self.assertEqual(report["graphs"], 84)
            self.assertEqual(len(report["symbols"]), 5)
            self.assertTrue((directory / "soak-status.json").is_file())
            self.assertEqual(json.loads((directory / "soak-report.json").read_text())["status"], "smoke_passed")
            self.assertGreater(report["elapsed_seconds"], report["observation_seconds"])

    def test_changed_core_fails_without_continuing_same_clock(self):
        expected = core_manifest()
        changed = {"sha256": "changed", "files": {**expected["files"], "runtime/runner.py": "changed"}}
        with patch("runtime.soak.core_manifest", return_value=changed):
            with self.assertRaisesRegex(SoakFailure, "core_version_changed"):
                check_core(expected)

    def test_restart_releases_old_runner_before_constructing_replacement(self):
        previous = []
        def construct(*args, **kwargs):
            self.assertTrue(all(reference() is None for reference in previous))
            runner = Runner(*args, **kwargs)
            previous.append(weakref.ref(runner))
            return runner
        with TemporaryDirectory() as temporary, patch("runtime.soak.Runner", side_effect=construct):
            report = run_soak(Path(temporary)/"restart",duration_seconds=1.5,pace=0,
                              fault_interval_seconds=.02,status_interval_seconds=.2,
                              burst_size=3,demo_minutes=15)
        self.assertGreater(report["counters"]["restarts"],0)
        self.assertGreater(len(previous),1)
        self.assertTrue(all(reference() is None for reference in previous))
        self.assertTrue(report["replay"]["passed"])

    def test_failure_status_retains_evidence(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary) / "fail"
            with patch("runtime.soak.check_core", side_effect=SoakFailure("core_version_changed:unit-test")):
                with self.assertRaises(SoakFailure):
                    run_soak(directory, duration_seconds=.1, pace=0)
            report = json.loads((directory / "soak-report.json").read_text())
            self.assertEqual(report["status"], "failed")
            self.assertFalse(report["core_unchanged"])
            self.assertEqual(report["required_seconds"], 36000)
            self.assertTrue((directory / "soak-faults.jsonl").is_file())

    def test_rss_failure_retains_failing_sample_and_previous_observations(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary) / "rss-fail"
            with patch("runtime.soak.rss_bytes", side_effect=[40 * 1024 * 1024, 1025 * 1024 * 1024]):
                with self.assertRaisesRegex(SoakFailure, "rss_budget_exceeded"):
                    run_soak(directory, duration_seconds=1, pace=.01, status_interval_seconds=.01)
            report = json.loads((directory / "soak-report.json").read_text())
            self.assertEqual(report["status"], "failed")
            self.assertTrue(report["core_unchanged"])
            self.assertTrue(report["checks"]["passed"])
            self.assertEqual(len(report["resource_samples"]), 2)
            self.assertEqual(report["resource_samples"][0]["rss_bytes"], 40 * 1024 * 1024)
            self.assertEqual(report["resource"]["rss_bytes"], 1025 * 1024 * 1024)
            self.assertEqual(report["resource_samples"][-1], report["resource"])
            self.assertEqual(report["rss_budget_failure"]["observed_rss_bytes"], report["resource"]["rss_bytes"])
            self.assertEqual(report["rss_budget_failure"]["limit_rss_bytes"], 1024 * 1024 * 1024)
            self.assertGreater(report["processed_cursor"], 0)
            self.assertEqual(report["processed_cursor"], report["journal_events"])
            self.assertEqual(json.loads((directory / "soak-status.json").read_text()), report)

    def test_cent_ledger_and_fill_identity_invariants(self):
        with TemporaryDirectory() as temporary:
            runner = Runner(temporary, RuntimeConfig(pace_seconds=0))
            try:
                for event in DemoProvider(minutes=30).events():
                    runner.ingest(event, "demo")
                self.assertTrue(invariants(runner)["passed"])
                runner.paper.balance_cents += 1
                with self.assertRaisesRegex(SoakFailure, "ledger_balance"):
                    invariants(runner)
                runner.paper.balance_cents -= 1
                with runner.store.transaction() as conn:
                    row = conn.execute("SELECT event_seq,payload,identity FROM records WHERE topic='fills' LIMIT 1").fetchone()
                    self.assertIsNotNone(row)
                    runner.store.record(conn, "fills", row[0], 99999, json.loads(row[1]), row[2])
                with self.assertRaisesRegex(SoakFailure, "duplicate_or_missing_fills"):
                    invariants(runner)
            finally:
                runner.close()

    def test_fresh_directory_and_nonfinite_options(self):
        for option in (True, float("nan"), float("inf"), -1, 0):
            with self.subTest(option=option), TemporaryDirectory() as temporary:
                with self.assertRaises(ValueError):
                    run_soak(temporary, duration_seconds=option)
        with TemporaryDirectory() as temporary:
            (Path(temporary) / "soak-status.json").write_text("{}")
            with self.assertRaises(FileExistsError):
                run_soak(temporary, duration_seconds=.1)

    def test_bounded_percentiles(self):
        self.assertEqual(percentiles([]), {"p50_ms": None, "p95_ms": None, "p99_ms": None})
        self.assertEqual(percentiles([.001, .002, .003, .004]), {"p50_ms": 2.0, "p95_ms": 4.0, "p99_ms": 4.0})


if __name__ == "__main__":
    unittest.main()
