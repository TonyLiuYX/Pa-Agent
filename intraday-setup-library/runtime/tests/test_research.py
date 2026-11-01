"""Committed research reports: manual labels, scoped predictions and no fake scores."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from connectors import DemoProvider
from recognition.models import MarketEvent
from runtime.config import RuntimeConfig
from runtime.research import committed_research_report
from runtime.runner import Runner

OPEN = datetime(2026, 9, 30, 13, 30, tzinfo=timezone.utc)


class ResearchTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.runner = Runner(self.temporary.name, RuntimeConfig(symbols=("SPY",), pace_seconds=0))
        self.runner.ingest(next(DemoProvider(symbols=["SPY"]).events()), "synthetic_fixture")
        self.spec = next(spec for spec in self.runner.engine.graph.specs if spec["setup_id"] == "SET-0005" and spec["variant_id"] == "base")
        self.event_index = 0

    def tearDown(self):
        self.runner.close()
        self.temporary.cleanup()

    def label(self, case, start=0, end=3, positive=True, **extra):
        return {"case_id": case, "setup_id": "SET-0005", "variant_id": "base", "symbol": "SPY", "venue": "SYNTHETIC",
                "direction": 1, "positive": positive, "start": (OPEN + timedelta(minutes=start)).isoformat(),
                "end": (OPEN + timedelta(minutes=end)).isoformat(), "source_kind": "synthetic", "source": "authored_fixture",
                "group_id": "fixture-day-family", "split": "test", "annotator": "unit_reviewer", "data_source": "synthetic_fixture",
                "rule_version": self.spec["rule_version"], **extra}

    def prediction(self, pid, minute, **extra):
        at = (OPEN + timedelta(minutes=minute)).isoformat()
        return {"detection_id": pid, "setup_id": "SET-0005", "variant_id": "base", "symbol": "SPY", "venue": "SYNTHETIC",
                "direction": 1, "state": "triggered", "data_status": "complete", "missing": [], "scale_seconds": 60,
                "evidence_cutoff": at, "identified_at": at, "rule_version": self.spec["rule_version"],
                "source_version": self.spec["source_version"], "source_sha256": self.spec["source_sha256"], **extra}

    def commit_predictions(self, predictions):
        self.event_index += 1
        ev = MarketEvent.from_dict({"event_id": "authored-report-event-" + str(self.event_index), "symbol": "SPY", "venue": "SYNTHETIC",
            "kind": "status", "event_time": (OPEN + timedelta(minutes=30 + self.event_index)).isoformat(),
            "arrival_time": (OPEN + timedelta(minutes=30 + self.event_index)).isoformat(), "payload": {"gap": False, "stream": "quote"}})
        outputs = [SimpleNamespace(to_dict=lambda value=value: dict(value)) for value in predictions]
        # Preserve Runner's real atomic publication path; the authored detections
        # test report matching, not the recognition accuracy of this setup.
        with patch.object(self.runner.engine, "feed", return_value=outputs):
            self.runner.ingest(ev, "authored_detection_fixture")

    def test_no_labels_is_not_run_even_when_predictions_exist(self):
        self.commit_predictions([self.prediction("unannotated", 1)])
        report = committed_research_report(self.runner)
        self.assertEqual(report["recognition_evaluation"]["status"], "not_run")
        self.assertIsNone(report["recognition_evaluation"]["metrics"])
        self.assertEqual(report["experimental_account"]["market_validation"], "not_run")
        self.assertEqual(report["snapshot"]["processed_cursor"], self.runner.store.meta("processed_cursor"))
        self.assertEqual(len(report["snapshot"]["detection_sha256"]), 64)
        self.assertIsNone(report["snapshot"]["committed_detection_sha256"])
        self.assertFalse(report["snapshot"]["detection_scan_performed"])

    def test_tp_fp_fn_scopes_and_source_identity(self):
        for row in (self.label("positive"), self.label("missed", 4, 5), self.label("negative", 6, 7, False)):
            self.runner.submit_label(row)
        self.commit_predictions([self.prediction("tp", 1), self.prediction("fp", 6.5), self.prediction("outside", 20),
                                 self.prediction("other_direction", 2, direction=-1)])
        report = committed_research_report(self.runner)
        scored = report["recognition_evaluation"]
        self.assertEqual(scored["status"], "completed")
        group = scored["metrics"]["groups"][0]
        self.assertEqual((group["triggered"]["tp"], group["triggered"]["fp"], group["triggered"]["fn"]), (1, 1, 1))
        self.assertEqual(group["source_kind"], "synthetic")
        self.assertEqual(group["split"], "test")
        self.assertEqual(group["acceptance"], "synthetic_logic_only")
        self.assertEqual(report["snapshot"]["related_predictions"], 2)
        self.assertEqual(report["snapshot"]["scanned_predictions"], 4)

    def test_complete_outer_coverage_counts_prediction_outside_trigger_subwindow(self):
        at = lambda minute: (OPEN + timedelta(minutes=minute)).isoformat()
        self.runner.submit_label(self.label("complete", 0, 10, complete_coverage=True,
            stage_windows={"triggered": {"start": at(0), "end": at(3)}}))
        self.commit_predictions([self.prediction("tp", 1), self.prediction("extra", 8), self.prediction("outside", 15)])
        report = committed_research_report(self.runner)["recognition_evaluation"]
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["coverage"][0]["coverage_id"], "complete:complete")
        self.assertEqual(report["metrics"]["groups"][0]["triggered"]["fp"], 1)

    def test_stage_windows_are_evaluated_independently(self):
        at = lambda minute: (OPEN + timedelta(minutes=minute)).isoformat()
        self.runner.submit_label(self.label("stages", 0, 10, stage_windows={
            "candidate": {"start": at(0), "end": at(3)}, "formed": {"start": at(4), "end": at(7)},
            "triggered": {"start": at(8), "end": at(10)}}))
        self.commit_predictions([self.prediction("same", 1, state="candidate"), self.prediction("same", 5, state="formed"), self.prediction("same", 9)])
        group = committed_research_report(self.runner)["recognition_evaluation"]["metrics"]["groups"][0]
        self.assertEqual([group[stage]["tp"] for stage in ("candidate", "confirmed", "triggered")], [1, 1, 1])

    def test_disputed_and_data_ineligible_cases_are_separate(self):
        self.runner.submit_label(self.label("disputed", disputed=True))
        self.runner.submit_label(self.label("ineligible", 4, 5, data_eligible=False))
        self.commit_predictions([self.prediction("excluded", 1)])
        with patch("runtime.research._predictions", side_effect=AssertionError("excluded labels must not scan predictions")):
            report = committed_research_report(self.runner)["recognition_evaluation"]
        self.assertEqual(report["status"], "not_run")
        self.assertEqual(report["disputed_cases"], ["disputed"])
        self.assertEqual(report["data_ineligible_cases"], ["ineligible"])
        self.assertIsNone(report["metrics"])

    def test_complete_coverage_overlap_is_explicit_failure(self):
        self.runner.submit_label(self.label("coverage", 0, 10, complete_coverage=True))
        self.runner.submit_label(self.label("disputed", 6, 7, disputed=True))
        report = committed_research_report(self.runner)["recognition_evaluation"]
        self.assertEqual(report["status"], "invalid_annotations")
        self.assertEqual(report["coverage_id"], "coverage:complete")
        self.assertIn("overlaps disputed", report["reason"])
        self.assertIsNone(report["metrics"])

    def test_annotation_and_prediction_versions_must_match(self):
        self.runner.submit_label(self.label("case"))
        self.commit_predictions([self.prediction("wrong-rule", 1, rule_version="obsolete-rule")])
        result = committed_research_report(self.runner)["recognition_evaluation"]
        self.assertEqual(result["status"], "version_mismatch")
        self.assertIsNone(result["metrics"])
        self.runner.submit_label(self.label("case", rule_version="obsolete-rule"))
        self.assertEqual(committed_research_report(self.runner)["recognition_evaluation"]["status"], "version_mismatch")

    def test_mixed_algorithm_versions_cannot_share_a_score(self):
        self.runner.submit_label(self.label("case"))
        self.commit_predictions([self.prediction("a", 1, algorithm_sha256="a" * 64),
                                 self.prediction("b", 2, algorithm_sha256="b" * 64)])
        result = committed_research_report(self.runner)["recognition_evaluation"]
        self.assertEqual(result["status"], "version_mismatch")
        self.assertIn("versions", result["reason"])
        self.assertIsNone(result["metrics"])

    def test_committed_visibility_and_latest_revision_invalidate_cache(self):
        self.runner.submit_label(self.label("case"))
        initial = committed_research_report(self.runner)
        seq, _ = self.runner.store.append("label", "local", self.label("case", positive=False), "pending-label")
        pending = committed_research_report(self.runner)
        self.assertEqual(pending["snapshot"], initial["snapshot"])
        with self.runner.store.transaction() as connection:
            connection.execute("UPDATE meta SET value=? WHERE key='account'", (json.dumps({"balance_cents": -1}),))
            self.assertEqual(committed_research_report(self.runner)["experimental_account"], initial["experimental_account"])
            # Restore before committing the unrelated transaction.
            connection.execute("UPDATE meta SET value=? WHERE key='account'", (json.dumps(initial["experimental_account"]["account"]),))
        self.runner.drain()
        changed = committed_research_report(self.runner)
        self.assertGreater(changed["snapshot"]["processed_cursor"], initial["snapshot"]["processed_cursor"])
        self.assertNotEqual(changed["snapshot"]["label_sha256"], initial["snapshot"]["label_sha256"])
        self.assertEqual(changed["recognition_evaluation"]["metrics"]["groups"][0]["positive_cases"], 0)

    def test_caller_read_snapshot_is_preserved_and_not_closed(self):
        self.runner.submit_label(self.label("initial"))
        connection = sqlite3.connect(self.runner.store.path, isolation_level=None)
        try:
            connection.execute("BEGIN")
            connection.execute("SELECT COUNT(*) FROM records").fetchone()  # pin this read snapshot
            before = committed_research_report(self.runner, connection=connection,
                                               status={"health": {"captured": True}, "source": {"uncommitted": True}})
            self.runner.submit_label(self.label("later", 4, 5))
            pinned = committed_research_report(self.runner, connection=connection)
            self.assertEqual(pinned["snapshot"], before["snapshot"])
            self.assertTrue(connection.in_transaction)
            self.assertEqual(pinned["recognition_evaluation"]["label_count"], 1)
            self.assertEqual(before["runtime"]["health"], {"captured": True})
            self.assertEqual(before["runtime"]["source"], {})
            self.assertEqual(committed_research_report(self.runner)["recognition_evaluation"]["label_count"], 2)
        finally:
            connection.execute("ROLLBACK")
            connection.close()

    def test_capacity_limits_block_scoring_without_silent_truncation(self):
        for case in ("a", "b", "c"):
            self.runner.submit_label(self.label(case))
        with patch("runtime.research.LABEL_LIMIT", 2):
            report = committed_research_report(self.runner)["recognition_evaluation"]
        self.assertEqual(report["status"], "capacity_blocked")
        self.assertEqual(report["label_count"], 3)
        self.assertIsNone(report["metrics"])
        self.commit_predictions([self.prediction("P" + str(index), 1) for index in range(4)] +
                                [self.prediction("outside" + str(index), 20) for index in range(20)])
        with patch("runtime.research.PREDICTION_LIMIT", 3):
            report = committed_research_report(self.runner)
        self.assertEqual(report["recognition_evaluation"]["status"], "capacity_blocked")
        self.assertEqual(report["snapshot"]["related_predictions"], 4)
        self.assertEqual(report["snapshot"]["scanned_predictions"], 24)
        self.assertIsNone(report["recognition_evaluation"]["metrics"])

    def test_returned_metrics_cannot_mutate_the_single_entry_cache(self):
        self.runner.submit_label(self.label("case"))
        first = committed_research_report(self.runner)
        first["recognition_evaluation"]["metrics"]["groups"].clear()
        self.assertEqual(len(committed_research_report(self.runner)["recognition_evaluation"]["metrics"]["groups"]), 1)

    def test_control_cursor_refresh_reuses_unchanged_scoring_inputs(self):
        self.runner.submit_label(self.label("case"))
        before = committed_research_report(self.runner)
        self.runner.submit_control("pause")
        with patch("runtime.research._predictions", side_effect=AssertionError("unchanged scoring inputs must reuse cache")):
            after = committed_research_report(self.runner)
        self.assertGreater(after["snapshot"]["processed_cursor"], before["snapshot"]["processed_cursor"])
        self.assertEqual(after["runtime"]["state"], "PAUSED")
        self.assertEqual(after["recognition_evaluation"]["metrics"], before["recognition_evaluation"]["metrics"])

    def test_variant_and_split_groups_are_separate(self):
        variant = next(spec for spec in self.runner.engine.graph.specs if spec["variant_id"] != "base")
        self.runner.submit_label(self.label("base-test"))
        self.runner.submit_label(self.label("base-development", 4, 5, split="development", group_id="different-family"))
        self.runner.submit_label(self.label("variant", setup_id=variant["setup_id"], variant_id=variant["variant_id"], rule_version=variant["rule_version"]))
        self.commit_predictions([self.prediction("base-test", 1), self.prediction("base-development", 4.5),
            self.prediction("variant", 1, setup_id=variant["setup_id"], variant_id=variant["variant_id"], rule_version=variant["rule_version"],
                            source_version=variant["source_version"], source_sha256=variant["source_sha256"])])
        reports = committed_research_report(self.runner)["recognition_evaluation"]["metrics"]["groups"]
        self.assertEqual(len(reports), 3)
        self.assertEqual({(row["variant_id"], row["split"]) for row in reports},
                         {("base", "test"), ("base", "development"), (variant["variant_id"], "test")})
        self.assertTrue(all(row["triggered"]["tp"] == 1 for row in reports))


if __name__ == "__main__":
    unittest.main()
