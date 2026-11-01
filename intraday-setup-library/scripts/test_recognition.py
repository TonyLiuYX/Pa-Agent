"""Run research-engine regressions and save the evidence and its exact scope.
Report mutations require a subsequent build to refresh manifest hashes.
"""
import argparse
import collections
from datetime import datetime, timezone
import hashlib
import json
import platform
from pathlib import Path
import sys
import time
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from release_support import release_metadata


class RecordedResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.passed = []
        self.subtests_passed = 0

    def addSuccess(self, test):
        self.passed.append(test.id())
        super().addSuccess(test)

    def addSubTest(self, test, subtest, error):
        if error is None:
            self.subtests_passed += 1
        super().addSubTest(test, subtest, error)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--require-jsonschema", action="store_true")
    ap.add_argument("--baseline-zip", type=Path, help="optional prior release for immutable setup-source comparison")
    args = ap.parse_args()
    from recognition.validation import validate_specs
    from recognition.provenance import algorithm_sha256, file_sha256
    from evaluation.tests.graph_cases import CASES
    from evaluation.tests.test_variants import variant_cases
    from evaluation.tests.raw_scenarios import coverage_matrix
    from runtime.runner import runtime_hash
    from runtime.soak import core_manifest
    start = time.perf_counter()
    frozen_core=core_manifest()
    suite = unittest.defaultTestLoader.discover(str(ROOT / "evaluation/tests"), top_level_dir=str(ROOT))
    result = unittest.TextTestRunner(verbosity=1, resultclass=RecordedResult).run(suite)
    contracts = validate_specs(check_generated=False, require_jsonschema=args.require_jsonschema)
    algorithm=algorithm_sha256()
    raw=coverage_matrix()
    negatives=[control for graph in raw["graphs"].values() for control in graph["negative_scenarios"]]
    raw.update(algorithm_sha256=algorithm,status="passed" if raw["complete"] else "failed",
        raw_negative_controls_run=len(negatives),raw_negative_controls_passed=sum(control["passed"] for control in negatives),
        negative_condition_types=dict(collections.Counter(control["condition_broken"] for control in negatives)),
        negative_verification_levels=dict(collections.Counter(control["verification_level"] for control in negatives)),
        negative_conditions_by_level={level:dict(collections.Counter(control["condition_broken"] for control in negatives if control["verification_level"]==level)) for level in sorted({control["verification_level"] for control in negatives})},
        frozen_fixture_sha256=file_sha256(ROOT/"evaluation/tests/raw_fixtures.json"),
        frozen_negative_design_sha256=file_sha256(ROOT/"evaluation/tests/raw_negative_designs.json"),
        public_price_proxy_semantic_boundary={"graph":"SET-0027:public_price_proxy",
            "observable_boundary_status":raw["graphs"]["SET-0027:public_price_proxy"]["variant_boundary_status"],
            "evidence":raw["graphs"]["SET-0027:public_price_proxy"]["variant_boundary_scenarios"],
            "scope":"Independent identity and declared public-price-proxy semantics; executable price predicates intentionally equal the base. This is not distinct microstructure evidence or market validation."},
        scope="Actual MarketEvent paths through all layers. Base known-false core predicate witnesses and variant supplemental absence/data/geometry controls are counted separately. Mechanical variant boundaries exclude the public-price proxy's not-applicable observable boundary. All remain synthetic engineering evidence.")
    audit=json.loads((ROOT/"recognition/audit_links.json").read_text())
    audit_summary={key:value for key,value in audit.items() if key!="cases"}
    audit_summary.update(algorithm_sha256=algorithm,mapping_sha256=file_sha256(ROOT/"recognition/audit_links.json"),
        verification_levels=dict(collections.Counter(row["verification_level"] for row in audit["cases"])),
        dispositions=dict(collections.Counter(row["disposition"] for row in audit["cases"])),
        cases=[{**{key:value for key,value in row.items() if key!="frozen_input"},
                "frozen_input_sha256":hashlib.sha256(json.dumps(row["frozen_input"],sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode()).hexdigest(),
                "frozen_input_kind":row["frozen_input"]["kind"]} for row in audit["cases"]])
    # Compare this release's source records with the existing delivered manifest.
    manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
    baseline_archive = None
    if args.baseline_zip:
        with zipfile.ZipFile(args.baseline_zip) as archive:
            manifest = json.loads(archive.read(ROOT.name + "/manifest.json"))
        baseline_archive = {"file": args.baseline_zip.name, "sha256": file_sha256(args.baseline_zip)}
    changed = [f["path"] for f in manifest["files"] if f["path"].startswith("setups/SET-")
               and file_sha256(ROOT / f["path"]) != f["sha256"]]
    release=release_metadata(ROOT)
    core_unchanged=core_manifest()==frozen_core
    report = {"version":release["version"],"content_date":release["content_date"],"status": "passed" if result.wasSuccessful() and core_unchanged and raw["complete"] and not contracts["errors"] and not changed else "failed",
        "executed_at_utc": datetime.now(timezone.utc).isoformat(), "python": platform.python_version(),
        "duration_seconds": round(time.perf_counter() - start, 3), "algorithm_sha256": algorithm,
        "runtime_sha256":runtime_hash(),"core_manifest":frozen_core,"core_unchanged_during_tests":core_unchanged,
        "test_methods_run": result.testsRun, "subtests_passed": result.subtests_passed,
        "passed": result.passed, "failures": [{"test": t.id(), "error": e} for t, e in result.failures + result.errors],
        "skipped":[{"test":test.id(),"reason":reason} for test,reason in result.skipped],
        "contracts": contracts,
        "feature_narrative_coverage": {"base": len(CASES), "variants": len(variant_cases()),
            "scope": "Hand-authored normalized-feature paths exercise graph order; they do not prove raw detector accuracy for all 84 graphs."},
        "raw_pipeline_coverage":raw,"original_audit_evidence_mapping":audit_summary,
        "checkpoint_codec_compatibility":{"checkpoint_version":1,
            "test_methods_run":sum(".CheckpointCodecCompatibilityTests." in name for name in result.passed),
            "passed":[name for name in result.passed if ".CheckpointCodecCompatibilityTests." in name],
            "fixture":{"path":"evaluation/tests/fixtures/checkpoint_v1.json",
                "bytes":(ROOT/"evaluation/tests/fixtures/checkpoint_v1.json").stat().st_size,
                "sha256":file_sha256(ROOT/"evaluation/tests/fixtures/checkpoint_v1.json")},
            "scope":"Frozen v1 serialized prefix and checksum byte compatibility, same suffix recognition, per-call encode memo lifetime, immutable string pooling and independent restored mutable containers; production identity checks remain enforced."},
        "source_records": {"compared_manifest_version": manifest["version"], "baseline_archive": baseline_archive,
            "source_count": sum(f["path"].startswith("setups/SET-") for f in manifest["files"]), "changed_paths": changed},
        "market_validation": {"status": "not_run", "real_market_labels": 0,
            "candidate_recall": None, "confirmed_recall": None, "confirmed_precision": None,
            "note": "Synthetic/raw fixture and CLI regressions are separate from historical US-market validation."}}
    out = ROOT / "reports/recognition-validation.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "tests": result.testsRun, "subtests_passed": result.subtests_passed,
        "base_graphs": len(CASES), "variant_graphs": len(variant_cases()), "report": str(out)}, ensure_ascii=False))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
