"""Record actual connector, simulated execution, runtime and release tests.

No live credentials or external broker orders are used by this test runner.
Report changes require a subsequent build and manifest validation.
"""
import argparse
from datetime import datetime,timezone
import importlib.metadata
import json
from pathlib import Path
import platform
import sys
import time
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from release_support import release_metadata
from test_recognition import RecordedResult


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-jsonschema",action="store_true")
    args=parser.parse_args()
    if args.require_jsonschema:
        try:import jsonschema
        except ImportError:raise SystemExit("jsonschema is required but unavailable")
    from runtime.runner import runtime_hash
    from runtime.soak import core_manifest
    from recognition.provenance import algorithm_sha256
    release=release_metadata(ROOT)
    frozen_core=core_manifest()
    loader=unittest.defaultTestLoader
    suite=unittest.TestSuite([loader.discover(str(ROOT/"runtime/tests"),top_level_dir=str(ROOT)),
        loader.loadTestsFromName("connectors.tests"),loader.loadTestsFromName("paper.tests"),
        loader.loadTestsFromName("test_release"),loader.loadTestsFromName("test_soak_release")])
    started=time.perf_counter()
    result=unittest.TextTestRunner(verbosity=1,resultclass=RecordedResult).run(suite)
    current_core=core_manifest();core_unchanged=current_core==frozen_core
    report={"version":release["version"],"content_date":release["content_date"],
        "status":"passed" if result.wasSuccessful() and core_unchanged else "failed","executed_at_utc":datetime.now(timezone.utc).isoformat(),
        "python":platform.python_version(),"duration_seconds":round(time.perf_counter()-started,3),
        "algorithm_sha256":algorithm_sha256(),"runtime_sha256":runtime_hash(),
        "core_manifest":frozen_core,"core_unchanged_during_tests":core_unchanged,
        "execution_model_version":release["execution_model_version"],"test_methods_run":result.testsRun,
        "subtests_passed":result.subtests_passed,"passed":result.passed,
        "skipped":[{"test":test.id(),"reason":reason} for test,reason in result.skipped],
        "failures":[{"test":test.id(),"error":error} for test,error in result.failures+result.errors],
        "live_connection_validation":"not_run","real_market_labels":0,"market_validation":"not_run",
        "ten_hour_soak_validation":"not_run","scope":"Synthetic connector, paper execution, persistence/recovery, contracts and release-boundary engineering tests; no real orders or real-market performance verification."}
    output=ROOT/"reports/runtime-validation.json";output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps({"status":report["status"],"tests":result.testsRun,"skipped":len(result.skipped),"report":str(output)},ensure_ascii=False))
    return 0 if report["status"]=="passed" else 1


if __name__=="__main__":sys.exit(main())
