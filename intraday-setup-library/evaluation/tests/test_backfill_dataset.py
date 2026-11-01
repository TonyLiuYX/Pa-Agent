"""Actual independent analysis artifacts cannot silently become partial scores."""
from datetime import timedelta
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from evaluation.dataset import evaluate_dataset, freeze_dataset, save_dataset
from evaluation.tests.raw_scenarios import price_scenarios
from evaluation.tests.support import OPEN, bar
from recognition.specs import ROOT
from runtime.backfill import analyze_backfill
from runtime.config import RuntimeConfig
from runtime.store import Store


class BackfillDatasetTests(unittest.TestCase):
    def test_actual_completed_analysis_scores_and_two_blocked_kinds_fail_both_clis(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            scenario = next(s for s in price_scenarios() if s.scenario_id == "hikkake-long")
            reports = []
            for name, events, limits in (
                    ("completed", scenario.events, {}),
                    ("capacity_blocked", scenario.events, {"max_input_bytes": 1}),
                    ("context_blocked", [bar(1, 10, 10.2, 9.9, 10.1)], {})):
                source = root / (name + "-source")
                store = Store(source)
                try:
                    store.set_meta("configuration", RuntimeConfig(mode="replay", source_kind="synthetic", symbols=("SYN",)).to_dict())
                    for event in events:
                        seq, _ = store.append("backfill", "synthetic_raw_input", event.to_dict(), event.event_id)
                        with store.transaction() as connection:
                            store.set_meta("processed_cursor", seq, connection)
                            connection.execute("UPDATE journal SET processed=1 WHERE seq=?", (seq,))
                    output = root / (name + "-analysis")
                    report = analyze_backfill(source, output, **limits)
                finally:
                    store.close()
                reports.append((name, report, output))
            for name, report, output in reports:
                with self.subTest(name=name):
                    row = {"case_id": name, "setup_id": "SET-0005", "variant_id": "base", "symbol": "SYN", "venue": "SYN",
                        "direction": 1, "positive": True, "start": OPEN.isoformat(),
                        "end": (OPEN + timedelta(minutes=5)).isoformat(), "trigger_time": (OPEN + timedelta(minutes=4)).isoformat(),
                        "group_id": name, "split": "test", "source_kind": "synthetic", "source": "hand_authored_raw_hikkake_fixture",
                        "annotator": "synthetic_test", "data_source": "original_price_scenarios_hikkake_long", "rule_version": report["rule_version"]}
                    manifest = freeze_dataset([row], dataset_id=name, created_at="2026-10-03T12:00:00Z",
                        **{key: report["input_provenance"][key] for key in (
                            "data_sha256", "rule_sha256", "configuration_sha256", "algorithm_sha256")})
                    frozen = root / (name + "-frozen.json")
                    save_dataset(frozen, manifest)
                    predictions = [json.loads(line) for line in (output / "detections.jsonl").read_text().splitlines()]
                    if name == "completed":
                        scored = evaluate_dataset(predictions, manifest, run_report=output / "run.json")
                        self.assertEqual(scored["groups"][0]["triggered"]["tp"], 1)
                        self.assertEqual(scored["input_provenance"]["raw_event_count"], 5)
                    else:
                        with self.assertRaisesRegex(ValueError, "failed or blocked"):
                            evaluate_dataset(predictions, manifest, run_report=output / "run.json")
                    for package in ("evaluation.dataset", "recognition"):
                        scored_path = root / (name + "-" + package + "-score.json")
                        command = [sys.executable, "-m", package, "evaluate",
                            "--manifest" if package == "evaluation.dataset" else "--dataset", str(frozen),
                            "--detections", str(output / "detections.jsonl"), "--run-report", str(output / "run.json"),
                            "--output", str(scored_path)]
                        completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
                        if name == "completed":
                            self.assertEqual(completed.returncode, 0, completed.stderr)
                            self.assertEqual(json.loads(scored_path.read_text())["groups"][0]["triggered"]["tp"], 1)
                        else:
                            self.assertNotEqual(completed.returncode, 0)
                            self.assertIn("failed or blocked", completed.stderr)
                            self.assertFalse(scored_path.exists(), "a blocked analysis must not write a score artifact")


if __name__ == "__main__":
    unittest.main()
