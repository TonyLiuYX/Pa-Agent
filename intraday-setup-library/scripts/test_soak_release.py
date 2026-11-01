"""Release-gate assertions with synthetic metadata, never a duration claim."""
from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
import unittest

from verify_soak import database_evidence, validate_report
from runtime.soak import FAULT_KINDS
from connectors import DEFAULT_SYMBOLS


def assertion_fixture():
    core = {"sha256": "fixture_only", "files": {}}
    report = {"status": "passed", "required_seconds": 36000, "requested_seconds": 36000,
              "observation_seconds": 36000, "elapsed_seconds": 36005,
              "started_at": "2020-01-01T00:00:00+00:00", "ended_at": "2020-01-01T10:00:05+00:00",
              "source_kind": "synthetic", "execution": "paper_only", "market_validation": "not_run",
              "symbols": list(DEFAULT_SYMBOLS), "graphs": 84, "core_unchanged": True, "core": core,
              "required_faults_completed": True, "fault_counts": dict.fromkeys(FAULT_KINDS, 1),
              "counters": {"sessions_started": 2, "faults_completed": 6},
              "checks": {"passed": True, "sqlite_integrity": "ok", "balance_cents": 10000000},
              "processed_cursor": 100, "journal_events": 100,
              "replay": {"passed": True, "processed_cursor": 100, "seconds": 5,
                         "account": {"balance_cents": 10000000}},
              "settings": {"max_rss_mb": 1024, "rss_growth_allowance_mb": 128},
              "resource_samples": [{"rss_bytes": 100000000, "disk_free_bytes": 200000000} for _ in range(10)],
              "resource": {"backlog": 0}, "late_rss_growth_bytes": 0}
    return report, core


class SoakReleaseGateTests(unittest.TestCase):
    def test_readonly_database_gate_binds_checkpoint_and_all_derived_records(self):
        from connectors import DemoProvider
        from runtime.config import RuntimeConfig
        from runtime.runner import Runner
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            runner = Runner(directory, RuntimeConfig(mode="demo"))
            try:
                for event in DemoProvider(minutes=2).events():
                    runner.ingest(event, "release_gate_fixture")
                runner.checkpoint()
                identity = runner._identity
            finally:
                runner.close()
            evidence = database_evidence(directory / "runtime.sqlite3", identity)
            self.assertGreater(evidence["processed_cursor"], 0)
            self.assertEqual(len(evidence["derived_sha256"]), 64)
            with sqlite3.connect(directory / "runtime.sqlite3") as conn:
                conn.execute("UPDATE records SET payload='{}' WHERE id=(SELECT MIN(id) FROM records)")
            with self.assertRaises(ValueError):
                database_evidence(directory / "runtime.sqlite3", identity)

    def test_assertion_shape_is_separate_from_artifact_acceptance(self):
        report, core = assertion_fixture()
        self.assertIsNone(validate_report(report, core))

    def test_incomplete_short_changed_or_unverified_runs_are_rejected(self):
        source, core = assertion_fixture()
        edits = [lambda r: r.update(status="interrupted"),
                 lambda r: r.update(observation_seconds=35999),
                 lambda r: r.update(core_unchanged=False),
                 lambda r: r.update(core={}),
                 lambda r: r["fault_counts"].update(before_commit=0),
                 lambda r: r["counters"].update(sessions_started=1),
                 lambda r: r["replay"].update(passed=False),
                 lambda r: r.update(journal_events=101),
                 lambda r: r.update(ended_at="2020-01-01T00:01:05+00:00"),
                 lambda r: r.update(market_validation="passed"),
                 lambda r: r["resource"].update(backlog=1)]
        for index, edit in enumerate(edits):
            with self.subTest(rejection=index):
                report = deepcopy(source)
                edit(report)
                with self.assertRaises(ValueError):
                    validate_report(report, core)

    def test_resource_growth_and_invalid_numeric_evidence_are_rejected(self):
        source, core = assertion_fixture()
        for bad in (True, float("nan"), float("inf"), "36000"):
            with self.subTest(value=repr(bad)):
                report = deepcopy(source)
                report["observation_seconds"] = bad
                with self.assertRaises(ValueError):
                    validate_report(report, core)
        report = deepcopy(source)
        report["resource_samples"][-1]["rss_bytes"] = 2 * 1024 * 1024 * 1024
        with self.assertRaises(ValueError):
            validate_report(report, core)
        report = deepcopy(source)
        report["late_rss_growth_bytes"] = 129 * 1024 * 1024
        with self.assertRaises(ValueError):
            validate_report(report, core)


if __name__ == "__main__":
    unittest.main()
