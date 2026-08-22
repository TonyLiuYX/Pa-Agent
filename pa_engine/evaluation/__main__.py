"""CLI entry point for ``python -m pa_engine.evaluation``."""

from __future__ import annotations

import argparse
import json
import sys

from .harness import EvaluationError, run_offline_evaluation, write_report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="G029 deterministic offline paper-runtime evaluation")
    parser.add_argument("--fixture-dir", default=None)
    parser.add_argument("--output", default=None, help="optional machine-readable JSON report path")
    args = parser.parse_args(argv)
    try:
        report = run_offline_evaluation(fixture_dir=args.fixture_dir)
    except EvaluationError as exc:
        print(json.dumps({"status": "REJECT", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 1
    if args.output:
        write_report(report, args.output)
    print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0 if report.status == "ACCEPT" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
