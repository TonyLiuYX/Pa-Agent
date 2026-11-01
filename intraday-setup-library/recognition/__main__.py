"""python3 -m recognition: catalog, validate, demo, replay, evaluate."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from .data import read_events
from .engine import RecognitionEngine
from .specs import ROOT, catalog_document, catalog_markdown


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="command", required=True)
    cat = sub.add_parser("catalog", help="generate source-linked 50-rule catalog")
    cat.add_argument("--output", type=Path, default=ROOT / "recognition" / "catalog.json")
    validation = sub.add_parser("validate", help="validate all rule graphs and their provenance")
    validation.add_argument("--require-jsonschema", action="store_true")
    convert = sub.add_parser("convert-bars", help="CSV bars plus explicit calendar to canonical JSONL")
    convert.add_argument("--csv", type=Path, required=True)
    convert.add_argument("--sessions", type=Path, required=True)
    convert.add_argument("--output", type=Path, required=True)
    demo = sub.add_parser("demo", help="generate synthetic, non-market example")
    demo.add_argument("--output", type=Path, required=True)
    replay = sub.add_parser("replay", help="arrival-ordered causal JSONL replay")
    replay.add_argument("--input", type=Path, required=True)
    replay.add_argument("--output", type=Path, required=True)
    replay.add_argument("--setups", nargs="+")
    replay.add_argument("--variants", action="store_true")
    replay.add_argument("--config", type=Path)
    scoring = sub.add_parser("evaluate", help="evaluate only annotated event windows")
    scoring.add_argument("--detections", type=Path, required=True)
    scoring.add_argument("--labels", type=Path)
    scoring.add_argument("--coverage", type=Path)
    scoring.add_argument("--dataset", type=Path, help="verified frozen dataset manifest")
    scoring.add_argument("--run-report", type=Path, help="actual replay/runtime run.json required for a frozen dataset")
    scoring.add_argument("--events", type=Path, help="relocated run input artifact")
    scoring.add_argument("--rules", type=Path, help="relocated run rules artifact")
    scoring.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    if args.command == "catalog":
        document = catalog_document()
        write_json(args.output, document)
        args.output.with_suffix(".md").write_text(catalog_markdown(document), encoding="utf-8")
        print(json.dumps({"setups": 50, "output": str(args.output)}, ensure_ascii=False))
    elif args.command == "validate":
        from .validation import validate_specs
        result = validate_specs(require_jsonschema=args.require_jsonschema)
        print(json.dumps(result, ensure_ascii=False))
        if result["errors"]:
            raise SystemExit(1)
    elif args.command == "convert-bars":
        from .adapters import convert_bars
        print(json.dumps(convert_bars(args.csv, args.sessions, args.output), ensure_ascii=False))
    elif args.command == "demo":
        from evaluation.demo import create_demo
        print(json.dumps(create_demo(args.output), ensure_ascii=False))
    elif args.command == "replay":
        args.output.mkdir(parents=True, exist_ok=True)
        target = args.output / "detections.jsonl"
        temporary = args.output / "detections.partial.jsonl"
        if any((args.output / name).exists() for name in ("detections.jsonl", "detections.partial.jsonl", "events.jsonl", "rules.json", "run.json")):
            raise FileExistsError("use a new output directory for each run")
        from .config import RecognitionConfig
        config = RecognitionConfig.load(args.config) if args.config else RecognitionConfig()
        engine = RecognitionEngine(args.setups, args.variants, config)
        from .provenance import file_sha256
        import sys
        input_hash = file_sha256(args.input)
        count = transitions = 0
        try:
            with temporary.open("w", encoding="utf-8") as handle:
                for event in read_events(args.input):
                    count += 1
                    for detection in engine.feed(event):
                        handle.write(json.dumps(detection.to_dict(), ensure_ascii=False, allow_nan=False) + "\n")
                        transitions += 1
            if input_hash != file_sha256(args.input):
                raise ValueError("input changed during replay")
        except (ValueError, KeyError, TypeError) as exc:
            write_json(args.output / "run.json", {"status": "failed", "events_processed": count,
                       "input_sha256": input_hash, "error": str(exc), "partial_output": temporary.name})
            raise SystemExit(str(exc)) from exc
        temporary.replace(target)
        import shutil
        input_target = args.output / "events.jsonl"
        shutil.copyfile(args.input, input_target)
        if file_sha256(input_target) != input_hash:
            raise ValueError("input changed while copying provenance evidence")
        rule_target = args.output / "rules.json"
        write_json(rule_target, {"schema_version": "1.0.0", "rules": engine.graph.specs})
        report = {"status": "passed", "input": str(args.input.resolve()), "input_sha256": input_hash,
                  "algorithm_sha256": engine.graph.algorithm_sha256, "python_version": sys.version,
                  "events": count, "transitions": transitions,
                  "coverage": engine.coverage(), "audit": engine.audit,
                  "unfinished_at_eof": engine.unfinished(),
                  "configuration_sha256": config.fingerprint(),
                  "input_provenance": {"data_file": input_target.name, "rule_file": rule_target.name,
                      "detections_file": target.name, "data_sha256": input_hash,
                      "rule_sha256": file_sha256(rule_target), "detections_sha256": file_sha256(target),
                      "algorithm_sha256": engine.graph.algorithm_sha256,
                      "configuration_sha256": config.fingerprint(), "processed_cursor": count,
                      "scope": "Arrival-ordered recognition replay; exact raw events, configured rule collection and complete emitted detection batch."},
                  "note": "Replay is not a strategy backtest; real-market labels are required for validation."}
        write_json(args.output / "run.json", report)
        print(json.dumps({"events": count, "transitions": transitions, "output": str(args.output)}, ensure_ascii=False))
    elif args.command == "evaluate":
        from evaluation.metrics import evaluate
        from .provenance import file_sha256
        read = lambda path: [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if bool(args.labels)==bool(args.dataset) or args.dataset and args.coverage:
            ap.error("provide --labels [--coverage] or --dataset")
        if args.dataset and not args.run_report:
            ap.error("--dataset requires --run-report with actual events/rules/detections artifacts")
        if not args.dataset and any((args.run_report, args.events, args.rules)):
            ap.error("--run-report/--events/--rules apply to --dataset evaluation")
        paths=[args.detections]+[p for p in (args.labels,args.coverage,args.dataset) if p]
        hashes={str(p.resolve()):file_sha256(p) for p in paths}
        predictions=read(args.detections)
        if args.dataset:
            from evaluation.dataset import evaluate_dataset
            report=evaluate_dataset(predictions,json.loads(args.dataset.read_text(encoding="utf-8")),
                run_report=args.run_report, events=args.events, rules=args.rules, detections=args.detections)
        else:
            report=evaluate(predictions,read(args.labels),read(args.coverage) if args.coverage else ())
        if any(file_sha256(p)!=hashes[str(p.resolve())] for p in paths):
            raise ValueError("evaluation input changed during scoring")
        report["input_file_sha256"]={**report.get("input_file_sha256",{}),**hashes}
        write_json(args.output, report)
        print(json.dumps({"groups": len(report["groups"]), "market_validation": report["market_validation"], "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
