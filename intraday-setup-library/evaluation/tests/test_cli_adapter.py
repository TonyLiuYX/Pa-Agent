import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from recognition.adapters import convert_bars
from recognition.config import RecognitionConfig
from recognition.data import read_events
from recognition.specs import ROOT, catalog_document
from recognition.validation import validate_specs
from evaluation.metrics import evaluate
from .support import session
from .test_micro_metrics import label, prediction


class IntegrationTests(unittest.TestCase):
    def test_catalog_schema_feature_contracts_and_all_graphs(self):
        report = validate_specs(check_generated=False)
        self.assertEqual(report["errors"], [])
        self.assertEqual((report["setup_count"], report["graph_count"], report["variant_count"]), (50, 84, 34))
        self.assertEqual(len(catalog_document()["variant_graphs"]), 34)

    def test_csv_adapter_calendar_arrival_sort_and_no_overwrite(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            calendar = root / "session.jsonl"
            calendar.write_text(json.dumps(session().to_dict()) + "\n")
            csv = root / "bars.csv"
            csv.write_text("symbol,venue,event_time,arrival_time,open,high,low,close,volume,notional\n"
                           "SYN,SYN,2026-09-30T09:31:00-04:00,2026-09-30T09:31:02-04:00,100,101,99,100,10,1000\n")
            out = root / "events.jsonl"
            result = convert_bars(csv, calendar, out)
            rows = list(read_events(out))
            self.assertEqual(result["events"], 2)
            self.assertEqual([e.kind for e in rows], ["session", "bar"])
            self.assertEqual((rows[1].arrival_time - rows[1].event_time).total_seconds(), 2)
            with self.assertRaises(FileExistsError):
                convert_bars(csv, calendar, out)

    def test_cli_replay_evaluation_and_run_hashes(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            def run(*args):
                return subprocess.run([sys.executable, "-m", "recognition", *map(str, args)], cwd=ROOT, capture_output=True, text=True)
            created = run("demo", "--output", root / "input")
            self.assertEqual(created.returncode, 0, created.stderr)
            replay = run("replay", "--input", root / "input/events.jsonl", "--output", root / "run", "--setups", "SET-0005", "SET-0049", "--variants")
            self.assertEqual(replay.returncode, 0, replay.stderr)
            report = json.loads((root / "run/run.json").read_text())
            self.assertEqual(report["events"], 391)
            self.assertEqual(report["unfinished_at_eof"], [])
            self.assertEqual(len(report["input_sha256"]), 64)
            self.assertEqual(len(report["algorithm_sha256"]), 64)
            self.assertEqual(len(report["coverage"]), 4)
            scored = run("evaluate", "--detections", root / "run/detections.jsonl", "--labels", root / "input/labels.jsonl", "--output", root / "evaluation.json")
            self.assertEqual(scored.returncode, 0, scored.stderr)
            metrics = json.loads((root / "evaluation.json").read_text())
            self.assertEqual(metrics["market_validation"], "not_run")
            self.assertTrue(all(r["triggered"]["tp"] == 1 for r in metrics["groups"]))
            self.assertNotEqual(run("replay", "--input", root / "input/events.jsonl", "--output", root / "run").returncode, 0)

    def test_ineligible_annotations_are_reported_separately(self):
        missing = label("missing-depth", data_eligible=False)
        report = evaluate([prediction("P", 1)], [missing])
        self.assertEqual(report["groups"], [])
        self.assertEqual(report["data_ineligible_cases"], ["missing-depth"])

    def test_configuration_rejects_nonfinite_or_rigid_rule_override(self):
        with self.assertRaises(ValueError):
            RecognitionConfig(footprint_min_volume=float("inf")).validate()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text('{"hikkake_deadline": 100}')
            with self.assertRaises(TypeError):
                RecognitionConfig.load(path)
