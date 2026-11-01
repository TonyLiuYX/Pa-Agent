import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from evaluation.dataset import freeze_dataset, verify_dataset, evaluate_dataset
from evaluation.metrics import evaluate, validate_labels, validate_coverage
from .test_micro_metrics import label, prediction


class ExtendedEvaluationTests(unittest.TestCase):
    def coverage(self, **extra):
        row = label("unused", start=0, end=20)
        row.pop("positive"); row.pop("case_id")
        row.update(coverage_id="c1", complete=True)
        row.update(extra)
        return row

    def test_direction_bool_is_not_integer(self):
        row = label("one"); row["direction"] = True
        with self.assertRaises(ValueError): validate_labels([row])
        bad = prediction("p", 1); bad["direction"] = True
        with self.assertRaises(ValueError): evaluate([bad], [label("one")])

    def test_trigger_timestamp_must_be_valid_and_inside_window(self):
        for value in ("bad", "2026-09-30T12:00:00", prediction("p", 5)["evidence_cutoff"]):
            with self.subTest(value=value):
                with self.assertRaises(ValueError): validate_labels([label("one", trigger_time=value)])

    def test_disputed_labels_do_not_bypass_split_lock(self):
        row = label("two", disputed=True); row["split"] = "development"
        with self.assertRaises(ValueError): validate_labels([label("one"), row])

    def test_complete_coverage_counts_absence_as_false_positive(self):
        stats = evaluate([prediction("a", 1), prediction("b", 10), prediction("outside", 25)],
                         [label("one")], [self.coverage()])["groups"][0]["triggered"]
        self.assertEqual((stats["tp"], stats["fp"], stats["ignored_unannotated_detections"]), (1, 1, 1))
        self.assertAlmostEqual(stats["annotated_hours"], 1 / 3)

    def test_different_algorithm_versions_are_not_merged(self):
        with self.assertRaises(ValueError):
            evaluate([prediction("a", 1, algorithm_sha256="a"*64),
                      prediction("b", 2, algorithm_sha256="b"*64)], [label("one")])

    def test_source_kind_is_required_when_evaluation_sources_are_mixed(self):
        market=label("market");market["source_kind"]="real_market";market["group_id"]="market-day-family"
        with self.assertRaises(ValueError): evaluate([prediction("p", 1)], [label("synthetic"), market])
        report=evaluate([prediction("p", 1, source_kind="synthetic")], [label("synthetic"), market])
        self.assertEqual({r["source_kind"]:r["triggered"]["tp"] for r in report["groups"]}, {"synthetic":1,"real_market":0})

    def test_coverage_without_positive_or_negative_cases_still_scores(self):
        stats = evaluate([prediction("p", 10)], [], [self.coverage()])["groups"][0]["triggered"]
        self.assertEqual((stats["tp"], stats["fp"], stats["fn"]), (0, 1, 0))

    def test_stage_windows_are_independent(self):
        row = label("one", start=0, end=10, stage_windows={
            "candidate": {"start": prediction("p", 0)["evidence_cutoff"], "end": prediction("p", 3)["evidence_cutoff"]},
            "formed": {"start": prediction("p", 4)["evidence_cutoff"], "end": prediction("p", 7)["evidence_cutoff"]},
            "triggered": {"start": prediction("p", 8)["evidence_cutoff"], "end": prediction("p", 10)["evidence_cutoff"]}})
        events = [prediction("p", 1), prediction("p", 5), prediction("p", 9)]
        events[0]["state"] = "candidate"; events[1]["state"] = "formed"
        group = evaluate(events, [row])["groups"][0]
        self.assertEqual([group[k]["tp"] for k in ("candidate", "confirmed", "triggered")], [1, 1, 1])

    def test_incomplete_coverage_and_split_leakage_rejected(self):
        with self.assertRaises(ValueError): validate_coverage([self.coverage(complete=False)])
        with self.assertRaises(ValueError): validate_coverage([self.coverage(split="development")], [label("one")])
        with self.assertRaises(ValueError): validate_coverage([self.coverage()], [label("one", disputed=True)])

    def test_annotation_complete_coverage_is_explicit_and_eligible(self):
        validate_labels([label("complete",complete_coverage=True)])
        for extra in ({"complete_coverage":1},{"complete_coverage":True,"disputed":True},
                      {"complete_coverage":True,"data_eligible":False}):
            with self.subTest(extra=extra):
                with self.assertRaises(ValueError): validate_labels([label("bad",**extra)])

    def test_freeze_and_tamper_detection(self):
        row = label("one", annotator="reviewer", rule_version="1", data_source="fixture")
        manifest = freeze_dataset([row], [self.coverage()], dataset_id="fixture", data_sha256="a" * 64,
            rule_sha256="b" * 64, configuration_sha256="c" * 64, algorithm_sha256="d" * 64,
            created_at="2026-10-03T12:00:00-04:00")
        self.assertTrue(verify_dataset(manifest))
        pred=prediction("p", 1, algorithm_sha256="d"*64, configuration_sha256="c"*64)
        with self.assertRaisesRegex(ValueError, "run-report"):
            evaluate_dataset([pred], manifest)
        with self.assertRaises(ValueError): evaluate_dataset([prediction("p", 1)], manifest)
        edited = copy.deepcopy(manifest); edited["labels"][0]["positive"] = False
        with self.assertRaises(ValueError): verify_dataset(edited)


class DatasetProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.pred = prediction("p", 1, algorithm_sha256="d"*64, configuration_sha256="c"*64,
            rule_version="1", source_version="1", source_kind="synthetic")
        event = dict(event_id="raw-1", symbol="SYN", venue="SYN", kind="trade",
            event_time=self.pred["evidence_cutoff"], arrival_time=self.pred["identified_at"],
            quality="complete", payload={"price":100,"size":1,"side":"unknown"})
        self.write("events.jsonl", json.dumps(event)+"\n")
        self.write("rules.json", json.dumps({"schema_version":"1.0.0", "rules":[dict(
            setup_id="SET-0005",variant_id="base",rule_version="1",source_version="1",stages=[])]}))
        self.write("detections.jsonl", json.dumps(self.pred)+"\n")
        self.provenance = dict(data_file="events.jsonl",rule_file="rules.json",detections_file="detections.jsonl",
            data_sha256=self.sha("events.jsonl"),rule_sha256=self.sha("rules.json"),
            detections_sha256=self.sha("detections.jsonl"),algorithm_sha256="d"*64,
            configuration_sha256="c"*64,scope="Synthetic unit artifact binding",processed_cursor=1)
        self.report = {"run_id":"synthetic-proof", "input_provenance":self.provenance}
        self.save_report()
        self.manifest = freeze_dataset([label("one",annotator="reviewer",rule_version="1",data_source="fixture")],
            dataset_id="fixture",data_sha256=self.provenance["data_sha256"],rule_sha256=self.provenance["rule_sha256"],
            configuration_sha256="c"*64,algorithm_sha256="d"*64,created_at="2026-10-03T12:00:00-04:00")

    def write(self, name, text): (self.root/name).write_text(text,encoding="utf-8")
    def sha(self, name): return hashlib.sha256((self.root/name).read_bytes()).hexdigest()
    def save_report(self): self.write("run.json",json.dumps(self.report))
    def score(self, predictions=None, **extra):
        return evaluate_dataset([self.pred] if predictions is None else predictions,self.manifest,
            run_report=self.root/"run.json",**extra)

    def test_actual_artifacts_and_output_batch_are_bound(self):
        report = self.score()
        self.assertEqual(report["groups"][0]["triggered"]["tp"],1)
        self.assertEqual(report["input_provenance"]["run_id"],"synthetic-proof")
        self.assertEqual(report["input_provenance"]["detections_sha256"],self.sha("detections.jsonl"))
        self.assertEqual(len(report["input_file_sha256"]),4)

    def test_declared_failed_or_blocked_reports_cannot_turn_absence_into_misses(self):
        for status in ("failed", "blocked", "FAILED", " blocked "):
            with self.subTest(status=status):
                self.report["status"] = status
                self.save_report()
                with self.assertRaisesRegex(ValueError, "failed or blocked"):
                    self.score()

    def test_completion_fields_require_actual_boolean_true_in_both_locations(self):
        for location in (self.report, self.provenance):
            for field in ("input_selection_complete", "analysis_completed"):
                for value in (False, None, 0, 1, "true", [], {}):
                    with self.subTest(top=location is self.report, field=field, value=value):
                        location[field] = value
                        self.save_report()
                        with self.assertRaisesRegex(ValueError, "boolean true"):
                            self.score()
                location[field] = True
                self.save_report()
                self.assertEqual(self.score()["input_provenance"]["status"], "verified")
                location.pop(field)

    def test_backfill_requires_completed_status_and_explicit_completion_fields(self):
        self.report["analysis_kind"] = "independent_backfill_research"
        self.provenance["scope"] = "independent_backfill_research"
        for fields in ({"status": "completed"}, {"status": "passed"}, {"status": {"run_id": "runtime"}}):
            with self.subTest(fields=fields):
                self.report.update(fields)
                self.save_report()
                with self.assertRaisesRegex(ValueError, "explicit complete input"):
                    self.score()
        self.report["status"] = "completed"
        self.provenance.update(input_selection_complete=True, analysis_completed=True)
        self.save_report()
        self.assertEqual(self.score()["input_provenance"]["status"], "verified")
        self.report.pop("analysis_kind")
        self.provenance.pop("analysis_completed")
        self.save_report()
        with self.assertRaisesRegex(ValueError, "explicit complete input"):
            self.score()

    def test_legacy_passed_replay_and_committed_runtime_status_remain_compatible(self):
        self.report.pop("run_id")
        for status in ("passed", {"run_id": "committed-runtime", "state": "RUNNING"}):
            with self.subTest(status=status):
                self.report["status"] = status
                self.save_report()
                proof = self.score()["input_provenance"]
                self.assertEqual(proof["status"], "verified")
                self.assertEqual(proof["run_id"], "committed-runtime" if isinstance(status, dict) else None)

    def test_modified_raw_rules_or_detection_bytes_are_rejected(self):
        for name in ("events.jsonl","rules.json","detections.jsonl"):
            with self.subTest(name=name):
                original=(self.root/name).read_text()
                self.write(name,original+" ")
                with self.assertRaisesRegex(ValueError,"artifact hash"): self.score()
                self.write(name,original)

    def test_declared_other_input_hash_cannot_override_frozen_evidence(self):
        for key in ("data_sha256","rule_sha256","algorithm_sha256","configuration_sha256"):
            with self.subTest(key=key):
                original=self.provenance[key];self.provenance[key]="f"*64;self.save_report()
                with self.assertRaisesRegex(ValueError,"frozen dataset"): self.score()
                self.provenance[key]=original;self.save_report()

    def test_prediction_subset_or_unrelated_output_batch_is_rejected(self):
        for batch in ([],[prediction("other",1,algorithm_sha256="d"*64,configuration_sha256="c"*64)]):
            with self.subTest(batch=batch):
                with self.assertRaisesRegex(ValueError,"prediction batch"): self.score(batch)

    def test_missing_provenance_and_escaping_artifact_declarations_rejected(self):
        self.report.pop("input_provenance");self.save_report()
        with self.assertRaisesRegex(ValueError,"lacks input_provenance"): self.score()
        self.report["input_provenance"]=self.provenance
        self.provenance["data_file"]="../events.jsonl";self.save_report()
        with self.assertRaisesRegex(ValueError,"relative"): self.score()

    def test_actual_rule_identity_versions_are_checked(self):
        self.pred["rule_version"]="wrong";self.write("detections.jsonl",json.dumps(self.pred)+"\n")
        self.provenance["detections_sha256"]=self.sha("detections.jsonl");self.save_report()
        with self.assertRaisesRegex(ValueError,"actual rules"): self.score()

    def test_explicit_relocated_artifacts_keep_hash_binding(self):
        relocated=self.root/"moved";relocated.mkdir()
        for name in ("events.jsonl","rules.json","detections.jsonl"):
            (self.root/name).rename(relocated/name)
        report=self.score(events=relocated/"events.jsonl",rules=relocated/"rules.json",detections=relocated/"detections.jsonl")
        self.assertEqual(report["input_provenance"]["raw_event_count"],1)

    def test_non_event_input_is_rejected_even_with_consistent_hash_claims(self):
        self.write("events.jsonl",json.dumps({"unrelated":"document"})+"\n")
        self.provenance["data_sha256"]=self.sha("events.jsonl");self.save_report()
        self.manifest=freeze_dataset(self.manifest["labels"],dataset_id="fixture",
            data_sha256=self.provenance["data_sha256"],rule_sha256=self.provenance["rule_sha256"],
            configuration_sha256="c"*64,algorithm_sha256="d"*64,created_at="2026-10-03T12:00:00-04:00")
        with self.assertRaises((KeyError,ValueError)): self.score()

    def test_replay_cli_exports_provenance_for_frozen_evaluation(self):
        import subprocess
        import sys
        from evaluation.demo import create_demo
        source=self.root/"demo";create_demo(source)
        output=self.root/"replayed"
        result=subprocess.run([sys.executable,"-m","recognition","replay","--input",str(source/"events.jsonl"),
            "--output",str(output),"--setups","SET-0005"],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        run=json.loads((output/"run.json").read_text())
        provenance=run["input_provenance"]
        rows=[json.loads(line) for line in (output/"detections.jsonl").read_text().splitlines() if line.strip()]
        labels=[json.loads((source/"labels.jsonl").read_text().splitlines()[0])]
        rules=json.loads((output/"rules.json").read_text())["rules"]
        labels[0].update(annotator="unit-reviewer",data_source="synthetic-demo",rule_version=rules[0]["rule_version"])
        manifest=freeze_dataset(labels,dataset_id="cli-proof",created_at="2026-10-03T12:00:00-04:00",
            **{key:provenance[key] for key in ("data_sha256","rule_sha256","configuration_sha256","algorithm_sha256")})
        report=evaluate_dataset(rows,manifest,run_report=output/"run.json")
        self.assertEqual(report["input_provenance"]["raw_event_count"],391)
        self.assertGreater(report["input_provenance"]["detection_count"],0)
        frozen=self.root/"frozen.json";frozen.write_text(json.dumps(manifest))
        score=subprocess.run([sys.executable,"-m","recognition","evaluate","--dataset",str(frozen),
            "--detections",str(output/"detections.jsonl"),"--run-report",str(output/"run.json"),
            "--output",str(output/"locked-evaluation.json")],capture_output=True,text=True)
        self.assertEqual(score.returncode,0,score.stderr)
        self.assertEqual(json.loads((output/"locked-evaluation.json").read_text())["input_provenance"]["status"],"verified")

    def test_runtime_export_artifacts_support_frozen_evaluation(self):
        from connectors import DemoProvider
        from runtime.config import RuntimeConfig
        from runtime.runner import Runner
        from runtime.export import export_run
        runner=Runner(self.root/"runtime",RuntimeConfig(symbols=("SPY",),pace_seconds=0))
        try:
            for event in DemoProvider(symbols=("SPY",),minutes=12,pace_seconds=0).events(): runner.ingest(event,"demo")
            output=Path(export_run(runner,self.root/"exported")["directory"])
            run=json.loads((output/"run.json").read_text());p=run["input_provenance"]
            rules=json.loads((output/"rules.json").read_text())["rules"]
            rule=next(r for r in rules if r["setup_id"]=="SET-0005" and r["variant_id"]=="base")
            row=label("runtime-binding",annotator="unit-reviewer",data_source="synthetic-runtime",
                rule_version=rule["rule_version"])
            row.update(symbol="SPY",venue="IEX")
            manifest=freeze_dataset([row],dataset_id="runtime-proof",created_at="2026-10-03T12:00:00-04:00",
                **{key:p[key] for key in ("data_sha256","rule_sha256","configuration_sha256","algorithm_sha256")})
            rows=[json.loads(line) for line in (output/"detections.jsonl").read_text().splitlines() if line.strip()]
            result=evaluate_dataset(rows,manifest,run_report=output/"run.json")
            self.assertEqual(result["input_provenance"]["run_id"],runner.run_id)
            self.assertEqual(result["input_provenance"]["processed_cursor"],runner.cursor)
            self.assertEqual(result["input_provenance"]["detection_count"],len(rows))
            self.assertGreater(result["input_provenance"]["raw_event_count"],0)
        finally:
            runner.close()


if __name__ == "__main__": unittest.main()
