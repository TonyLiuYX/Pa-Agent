"""Immutable dataset manifests: canonical evidence hashes and grouped split locks."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .metrics import split_group, validate_coverage, validate_labels


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def freeze_dataset(labels, coverage=(), *, dataset_id, data_sha256, rule_sha256,
                   configuration_sha256, algorithm_sha256, created_at):
    """Freeze reviewed annotations and explicit source/configuration identities.

    Pure function: callers decide where to save; no source data is copied.
    """
    from recognition.models import timestamp
    timestamp(created_at)
    if not isinstance(dataset_id, str) or not dataset_id:
        raise ValueError("dataset_id is required")
    hashes = dict(data_sha256=data_sha256, rule_sha256=rule_sha256,
                  configuration_sha256=configuration_sha256, algorithm_sha256=algorithm_sha256)
    for key, value in hashes.items():
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"{key}: lowercase SHA-256 required")
    labels = json.loads(json.dumps(list(labels), allow_nan=False))
    coverage = json.loads(json.dumps(list(coverage), allow_nan=False))
    validate_labels(labels)
    validate_coverage(coverage, labels)
    for row in labels:
        for key in ("annotator", "rule_version", "data_source"):
            if not isinstance(row.get(key), str) or not row[key]:
                raise ValueError(f"frozen labels require {key}")
        row["split"] = row.get("split", split_group(row["group_id"]))
    for row in coverage:
        row["split"] = row.get("split", split_group(row["group_id"]))
    labels.sort(key=lambda row: row["case_id"])
    coverage.sort(key=lambda row: row["coverage_id"])
    manifest = dict(dataset_version="1.0.0", dataset_id=dataset_id, created_at=created_at,
                    **hashes, labels_sha256=canonical_hash(labels), coverage_sha256=canonical_hash(coverage),
                    labels=labels, coverage=coverage,
                    split_lock={r["group_id"]: r["split"] for r in labels + coverage},
                    source_kinds=sorted({r["source_kind"] for r in labels + coverage}),
                    market_validation="not_run")
    manifest["manifest_sha256"] = canonical_hash(manifest)
    return manifest


def verify_dataset(manifest):
    """Reject edited manifests and annotations before any scoring run."""
    row = dict(manifest)
    expected = row.pop("manifest_sha256", None)
    if canonical_hash(row) != expected:
        raise ValueError("dataset manifest hash mismatch")
    if row.get("dataset_version") != "1.0.0":
        raise ValueError("unsupported dataset version")
    validate_labels(row["labels"])
    validate_coverage(row["coverage"], row["labels"])
    if canonical_hash(row["labels"]) != row["labels_sha256"] or canonical_hash(row["coverage"]) != row["coverage_sha256"]:
        raise ValueError("dataset evidence hash mismatch")
    for item in row["labels"] + row["coverage"]:
        if row["split_lock"].get(item["group_id"]) != item["split"]:
            raise ValueError("frozen split changed")
    return True


def save_dataset(path, manifest):
    """Exclusive create prevents accidental replacement of locked test evidence."""
    verify_dataset(manifest)
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")


def verify_run_provenance(predictions, manifest, run_report, *, events=None, rules=None, detections=None):
    """Bind scoring to actual exported input and output bytes, not hash claims.

    A run report is provenance evidence, not a signature or correctness proof.
    Its file declarations must resolve to real artifacts and agree with the
    frozen dataset. Explicit locations support relocating exported artifacts.
    """
    if run_report is None:
        raise ValueError("frozen dataset evaluation requires --run-report and actual input artifacts")
    report_path = Path(run_report).resolve(strict=True)
    report_bytes = report_path.read_bytes()
    report = json.loads(report_bytes)
    if not isinstance(report, dict):
        raise ValueError("run report must be an object")
    provenance = report.get("input_provenance")
    if not isinstance(provenance, dict):
        raise ValueError("run report lacks input_provenance")
    status = report.get("status")
    if isinstance(status, str) and status.strip().lower() in {"failed", "blocked"}:
        raise ValueError("failed or blocked recognition runs cannot be scored as complete inputs")
    for document in (report, provenance):
        for field in ("input_selection_complete", "analysis_completed"):
            if field in document and document[field] is not True:
                raise ValueError(f"run provenance {field} must be the boolean true before scoring")
    if report.get("analysis_kind") == "independent_backfill_research" or provenance.get("scope") in {
            "independent_backfill_research", "blocked_backfill_selection"}:
        if status != "completed" or any(provenance.get(field) is not True for field in (
                "input_selection_complete", "analysis_completed")):
            raise ValueError("independent backfill scoring requires completed recognition and explicit complete input selection")
    for key in ("algorithm_sha256", "configuration_sha256", "data_sha256", "rule_sha256"):
        if provenance.get(key) != manifest[key]:
            raise ValueError(f"run provenance {key} differs from frozen dataset")
    if not isinstance(provenance.get("scope"), str) or not provenance["scope"]:
        raise ValueError("run provenance scope is required")
    cursor = provenance.get("processed_cursor")
    if cursor is not None and (type(cursor) is not int or cursor < 0):
        raise ValueError("processed_cursor must be a nonnegative integer")
    from recognition.provenance import file_sha256
    paths, hashes = {}, {}
    for kind, override in (("data", events), ("rule", rules), ("detections", detections)):
        declared = provenance.get(kind + "_file")
        if not isinstance(declared, str) or not declared:
            raise ValueError(f"run provenance {kind}_file is required")
        relative = Path(declared)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("provenance artifact declarations must be relative to the run report")
        path = Path(override).resolve(strict=True) if override else (report_path.parent / relative).resolve(strict=True)
        if not override and not path.is_relative_to(report_path.parent):
            raise ValueError("provenance artifact escapes run report directory")
        actual = file_sha256(path)
        if actual != provenance.get(kind + "_sha256"):
            raise ValueError(f"actual {kind} artifact hash differs from run provenance")
        paths[kind], hashes[kind] = path, actual
    output_count = 0
    with paths["detections"].open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            row = json.loads(line)
            if output_count >= len(predictions) or canonical_hash(row) != canonical_hash(predictions[output_count]):
                raise ValueError("prediction batch differs from provenance detections artifact")
            output_count += 1
    if output_count != len(predictions):
        raise ValueError("prediction batch differs from provenance detections artifact")
    with paths["rule"].open(encoding="utf-8") as stream:
        document = json.load(stream)
    if not isinstance(document, dict) or document.get("schema_version") != "1.0.0" or not isinstance(document.get("rules"), list) or not document["rules"]:
        raise ValueError("rules artifact must contain a versioned nonempty rules collection")
    rule_index = {}
    for rule in document["rules"]:
        if not isinstance(rule, dict) or not all(isinstance(rule.get(key), str) and rule[key] for key in ("setup_id", "variant_id", "rule_version", "source_version")):
            raise ValueError("rules artifact contains an incomplete rule identity")
        identity = (rule["setup_id"], rule["variant_id"])
        if identity in rule_index:
            raise ValueError("rules artifact contains duplicate rule identities")
        rule_index[identity] = rule
    from itertools import chain
    for row in chain(predictions, manifest["labels"], manifest["coverage"]):
        identity = (row.get("setup_id"), row.get("variant_id", "base"))
        rule = rule_index.get(identity)
        if rule is None:
            raise ValueError("evaluated setup/variant is absent from actual rules artifact")
        for key in ("rule_version", "source_version"):
            if key in row and row[key] != rule[key]:
                raise ValueError(f"evaluated {key} differs from actual rules artifact")
    # Parse the actual raw input too, so a non-event file cannot masquerade as
    # the evidence behind an otherwise internally consistent hash declaration.
    from recognition.models import MarketEvent
    event_count = 0
    with paths["data"].open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            MarketEvent.from_dict(json.loads(line))
            event_count += 1
    run_id = report.get("run_id") or (status.get("run_id") if isinstance(status, dict) else None)
    proof = dict(status="verified", run_report_sha256=hashlib.sha256(report_bytes).hexdigest(),
        run_id=run_id,
        scope=provenance["scope"], processed_cursor=cursor,
        data_sha256=hashes["data"], rule_sha256=hashes["rule"], detections_sha256=hashes["detections"],
        algorithm_sha256=provenance["algorithm_sha256"], configuration_sha256=provenance["configuration_sha256"],
        raw_event_count=event_count, rule_count=len(rule_index), detection_count=output_count)
    guards = {report_path: proof["run_report_sha256"], **{paths[k]: hashes[k] for k in paths}}
    return proof, guards


def evaluate_dataset(predictions, manifest, *, run_report=None, events=None, rules=None, detections=None):
    from .metrics import evaluate
    verify_dataset(manifest)
    predictions = list(predictions)
    proof, guards = verify_run_provenance(predictions, manifest, run_report,
        events=events, rules=rules, detections=detections)
    for prediction in predictions:
        for key in ("algorithm_sha256", "configuration_sha256"):
            if prediction.get(key) != manifest[key]:
                raise ValueError(f"prediction {key} differs from frozen dataset")
    result = evaluate(predictions, manifest["labels"], manifest["coverage"])
    result["dataset_id"] = manifest["dataset_id"]
    result["dataset_manifest_sha256"] = manifest["manifest_sha256"]
    result["input_sha256"] = {k: manifest[k] for k in
        ("data_sha256", "rule_sha256", "configuration_sha256", "algorithm_sha256")}
    result["input_provenance"] = proof
    result["input_file_sha256"] = {str(path): sha for path, sha in guards.items()}
    from recognition.provenance import file_sha256
    if any(file_sha256(path) != sha for path, sha in guards.items()):
        raise ValueError("provenance input changed during scoring")
    return result


def main():
    """python3 -m evaluation.dataset freeze|verify|evaluate."""
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--labels", required=True, type=Path)
    freeze.add_argument("--coverage", type=Path)
    freeze.add_argument("--output", required=True, type=Path)
    freeze.add_argument("--dataset-id", required=True)
    freeze.add_argument("--created-at", required=True)
    for name in ("data", "rule", "configuration", "algorithm"):
        freeze.add_argument(f"--{name}-sha256", required=True)
    for name in ("verify", "evaluate"):
        command = sub.add_parser(name)
        command.add_argument("--manifest", required=True, type=Path)
        if name == "evaluate":
            command.add_argument("--detections", required=True, type=Path)
            command.add_argument("--output", required=True, type=Path)
            command.add_argument("--run-report", required=True, type=Path)
            command.add_argument("--events", type=Path, help="explicit location of the declared raw input artifact")
            command.add_argument("--rules", type=Path, help="explicit location of the declared rules artifact")
    args = parser.parse_args()
    def rows(path):
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path else []
    if args.command == "freeze":
        manifest = freeze_dataset(rows(args.labels), rows(args.coverage), dataset_id=args.dataset_id,
            created_at=args.created_at, data_sha256=args.data_sha256, rule_sha256=args.rule_sha256,
            configuration_sha256=args.configuration_sha256, algorithm_sha256=args.algorithm_sha256)
        save_dataset(args.output, manifest)
        print(json.dumps({"dataset_id": manifest["dataset_id"], "manifest_sha256": manifest["manifest_sha256"]}))
    else:
        manifest = json.loads(args.manifest.read_text())
        verify_dataset(manifest)
        if args.command == "verify":
            print(json.dumps({"status":"verified", "dataset_id":manifest["dataset_id"]}))
        else:
            report = evaluate_dataset(rows(args.detections), manifest, run_report=args.run_report,
                events=args.events, rules=args.rules, detections=args.detections)
            with args.output.open("x", encoding="utf-8") as stream:
                json.dump(report, stream, ensure_ascii=False, indent=2, allow_nan=False)
                stream.write("\n")
            print(json.dumps({"status":"evaluated", "output":str(args.output)}))


if __name__ == "__main__":
    main()
