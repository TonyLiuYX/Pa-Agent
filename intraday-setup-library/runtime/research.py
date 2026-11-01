"""Bounded, committed annotation evaluation for the local reports API/export."""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from urllib.parse import quote

from evaluation.metrics import evaluate, validate_labels, validate_coverage
from recognition.models import timestamp
from .config import canonical

LABEL_LIMIT = 1000
PREDICTION_LIMIT = 10000
PAGE_SIZE = 500
SCOPE_FIELDS = ("setup_id", "variant_id", "symbol", "venue", "direction")


def _meta(connection, key, default=None):
    row = connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else default


def _scope(row):
    return tuple(row.get(field, "base" if field == "variant_id" else None) for field in SCOPE_FIELDS)


def _coverage(label):
    row = {key: label[key] for key in (*SCOPE_FIELDS, "start", "end", "group_id", "source", "source_kind") if key in label}
    row.setdefault("variant_id", "base")
    for key in ("split", "regime", "rule_version"):
        if key in label:
            row[key] = label[key]
    return {**row, "coverage_id": label["case_id"] + ":complete", "complete": True}


def _evaluation_base():
    return {"kind": "recognition_evaluation", "title": "识别评价", "status": "not_run", "metrics": None,
            "market_validation": "not_run", "scope": "event recognition within committed annotated windows",
            "limits": {"labels": LABEL_LIMIT, "related_predictions": PREDICTION_LIMIT},
            "disputed_cases": [], "data_ineligible_cases": [], "coverage": [], "version_mismatches": []}


def _hash_row(digest, row):
    digest.update(canonical(row).encode() + b"\n")


def _labels(connection, processed, outbox):
    # The committed latest revision for each case; the mutable labels table is
    # deliberately not used because the records carry the provenance cursor.
    query = """SELECT id,event_seq,payload FROM records WHERE id IN
        (SELECT MAX(id) FROM records WHERE topic='labels' AND event_seq<=? AND id<=? GROUP BY identity)
        AND id>? ORDER BY id LIMIT ?"""
    rows, count, after = [], 0, 0
    digest = hashlib.sha256()
    while True:
        page = connection.execute(query, (processed, outbox, after, PAGE_SIZE)).fetchall()
        if not page:
            break
        for record in page:
            value = json.loads(record[2])
            _hash_row(digest, {"record_id": record[0], "event_seq": record[1], "label": value})
            count += 1
            if count <= LABEL_LIMIT:
                rows.append(value)
            after = record[0]
    return rows, count, digest.hexdigest()


def _predictions(connection, processed, outbox, labels, source_kind):
    windows = {}
    for label in labels:
        windows.setdefault(_scope(label), []).append((timestamp(label["start"]), timestamp(label["end"]), label["source_kind"]))
    rows, scanned, related, after = [], 0, 0, 0
    full_digest, input_digest = hashlib.sha256(), hashlib.sha256()
    invalid = []
    while True:
        page = connection.execute("SELECT id,event_seq,payload FROM records WHERE topic='detections' AND event_seq<=? "
                                  "AND id<=? AND id>? ORDER BY id LIMIT ?", (processed, outbox, after, PAGE_SIZE)).fetchall()
        if not page:
            break
        for record in page:
            value = json.loads(record[2])
            evidence = {"record_id": record[0], "event_seq": record[1], "prediction": value}
            _hash_row(full_digest, evidence)
            scanned += 1
            after = record[0]
            scope_windows = windows.get(_scope(value))
            if not scope_windows:
                continue
            try:
                at = timestamp(value["evidence_cutoff"])
            except (KeyError, TypeError, ValueError, AttributeError):
                # A malformed prediction with matching identity cannot be silently
                # dismissed as outside the label window.
                if len(invalid) < LABEL_LIMIT:
                    invalid.append({"record_id": record[0], "reason": "invalid_evidence_cutoff"})
                continue
            matching = [kind for start, end, kind in scope_windows if start <= at <= end]
            if not matching:
                continue
            kind = value.get("source_kind", source_kind)
            if kind not in {"synthetic", "real_market"}:
                if len(invalid) < LABEL_LIMIT:
                    invalid.append({"record_id": record[0], "reason": "unknown_prediction_source_kind"})
                continue
            if kind not in matching:
                continue
            related += 1
            _hash_row(input_digest, evidence)
            if related <= PREDICTION_LIMIT:
                rows.append({**value, "source_kind": kind, "variant_id": value.get("variant_id", "base")})
    return rows, {"scanned_predictions": scanned, "related_predictions": related,
                  "detection_sha256": input_digest.hexdigest(), "committed_detection_sha256": full_digest.hexdigest(),
                  "invalid_predictions": invalid}


def _score(labels, predictions, registered_versions, source_kind, label_count):
    result = _evaluation_base()
    result.update(label_count=label_count, source_kind=source_kind, related_prediction_count=len(predictions))
    if not labels:
        result["reason"] = "no_committed_labels"
        return result
    result["disputed_cases"] = sorted(row["case_id"] for row in labels if row.get("disputed"))
    result["data_ineligible_cases"] = sorted(row["case_id"] for row in labels if not row.get("data_eligible", True))
    try:
        validate_labels(labels)
        for row in labels:
            if "complete_coverage" in row and type(row["complete_coverage"]) is not bool:
                raise ValueError("complete_coverage must be boolean: " + row["case_id"])
            if row.get("complete_coverage") and (row.get("disputed") or not row.get("data_eligible", True)):
                raise ValueError("complete coverage requires an eligible undisputed case: " + row["case_id"])
        eligible = [row for row in labels if not row.get("disputed") and row.get("data_eligible", True)]
        coverage = [_coverage(row) for row in eligible if row.get("complete_coverage") is True]
        result["coverage"] = coverage
        # Include excluded cases in coverage validation: an overlapping dispute
        # invalidates the explicit complete claim rather than reducing it silently.
        for row in coverage:
            try:
                validate_coverage([row], labels)
            except ValueError as exc:
                result.update(status="invalid_annotations", reason=str(exc), coverage_id=row["coverage_id"])
                return result
        result["eligible_label_count"] = len(eligible)
        if not eligible:
            result["reason"] = "no_eligible_committed_labels"
            return result
        mismatches = []
        for row in eligible:
            key = (row["setup_id"], row.get("variant_id", "base"))
            expected = registered_versions.get(key)
            if not row.get("rule_version") or row.get("rule_version") != expected:
                mismatches.append({"case_id": row["case_id"], "label_rule_version": row.get("rule_version"),
                                   "registered_rule_version": expected})
        version_windows = {}
        for row in eligible:
            version_windows.setdefault((_scope(row), row["source_kind"]), []).append(
                (timestamp(row["start"]), timestamp(row["end"]), row))
        for prediction in predictions:
            cutoff = timestamp(prediction["evidence_cutoff"])
            matching = [row for start, end, row in version_windows.get((_scope(prediction), prediction["source_kind"]), [])
                        if start <= cutoff <= end]
            for row in matching:
                if prediction.get("rule_version") != row.get("rule_version"):
                    mismatches.append({"case_id": row["case_id"], "detection_id": prediction.get("detection_id"),
                                       "label_rule_version": row.get("rule_version"),
                                       "detection_rule_version": prediction.get("rule_version")})
                    if len(mismatches) >= LABEL_LIMIT:
                        break
            if len(mismatches) >= LABEL_LIMIT:
                break
        if mismatches:
            result.update(status="version_mismatch", reason="annotation_and_detection_rule_versions_must_agree",
                          version_mismatches=mismatches[:LABEL_LIMIT])
            return result
        try:
            metrics = evaluate(predictions, eligible, coverage)
        except ValueError as exc:
            if "versions" in str(exc):
                result.update(status="version_mismatch", reason=str(exc))
                return result
            raise
        metrics["disputed_cases"] = result["disputed_cases"]
        metrics["data_ineligible_cases"] = result["data_ineligible_cases"]
        result.update(status="completed", metrics=metrics)
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        result.update(status="invalid_annotations", reason=str(exc))
    return result


def committed_research_report(runner, *, connection=None, status=None):
    """Return bounded metrics and account evidence from one committed SQLite view.

    The dedicated read connection also excludes a caller's uncommitted transaction
    on Store.conn. A single-entry score cache never substitutes live processor
    memory for committed account/status metadata.
    """
    owns_connection = connection is None
    try:
        with runner.store.lock:
            if owns_connection:
                uri = "file:" + quote(str(runner.store.path), safe="/") + "?mode=ro"
                connection = sqlite3.connect(uri, uri=True, isolation_level=None)
                connection.row_factory = sqlite3.Row
                connection.execute("BEGIN")
            elif not connection.in_transaction:
                raise ValueError("caller research connection must pin a read transaction")
            processed = _meta(connection, "processed_cursor", 0)
            outbox = connection.execute("SELECT COALESCE(MAX(id),0) FROM records WHERE event_seq<=?", (processed,)).fetchone()[0]
            maxima = {topic: connection.execute("SELECT COALESCE(MAX(id),0) FROM records WHERE topic=? AND event_seq<=? AND id<=?",
                        (topic, processed, outbox)).fetchone()[0] for topic in ("detections", "labels")}
            identity = _meta(connection, "identity", {})
            configuration = _meta(connection, "configuration", {})
            mode = configuration.get("mode", "unknown")
            source_kind = "synthetic" if mode == "demo" else "real_market" if mode == "live" else configuration.get("source_kind", "unknown")
            account = _meta(connection, "account", {})
            state = _meta(connection, "state", "READY")
            runtime = {**copy.deepcopy(status or {}), "kind": "runtime", "title": "运行与工程", "status": state, "state": state,
                       "run_id": _meta(connection, "run_id"), "mode": mode, "source_kind": source_kind,
                       "processed_cursor": processed, "outbox_cursor": outbox, "identity": identity,
                       "watchlist": configuration.get("symbols", []), "coverage": _meta(connection, "coverage", {}),
                       "source": _meta(connection, "source", {}), "metrics": _meta(connection, "metrics", {}),
                       "connection_state": _meta(connection, "connection_state", "UNKNOWN"),
                       "market_validation": "not_run", "scope": "committed engineering state and capability coverage"}
            experimental = {"kind": "experimental_account", "title": "实验模拟账户", "account": account,
                            "positions": _meta(connection, "positions", []), "execution": "paper_only",
                            "execution_model": account.get("execution_model"), "execution_version": account.get("execution_version"),
                            "execution_sha256": account.get("execution_sha256"), "market_validation": "not_run",
                            "scope": "experimental_overlay_not_source_strategy"}
            key = (maxima["detections"], maxima["labels"], canonical(identity), source_kind, LABEL_LIMIT, PREDICTION_LIMIT)
            cached = getattr(runner, "_committed_research_cache", None)
            registered = {(spec["setup_id"], spec["variant_id"]): spec["rule_version"] for spec in runner.engine.graph.specs}
        if cached is not None and cached["key"] == key:
            score, inputs = copy.deepcopy(cached["score"]), copy.deepcopy(cached["inputs"])
        else:
            labels, label_count, label_hash = _labels(connection, processed, outbox)
            inputs = {"label_count": label_count, "label_sha256": label_hash,
                      "labels_hash_scope": "latest committed label revision per case, including record provenance"}
            if label_count > LABEL_LIMIT:
                score = _evaluation_base()
                score.update(status="capacity_blocked", reason="committed_label_limit_exceeded", label_count=label_count)
                inputs.update(detection_sha256=None, committed_detection_sha256=None, detection_hash_complete=False)
            else:
                try:
                    validate_labels(labels)
                    eligible = [row for row in labels if not row.get("disputed") and row.get("data_eligible", True)]
                    if eligible:
                        predictions, prediction_inputs = _predictions(connection, processed, outbox, eligible, source_kind)
                    else:
                        predictions = []
                        prediction_inputs = {"scanned_predictions": 0, "related_predictions": 0,
                                             "detection_sha256": hashlib.sha256().hexdigest(),
                                             "committed_detection_sha256": None, "invalid_predictions": []}
                    inputs.update(prediction_inputs, detection_hash_complete=True,
                                  detection_hash_scope="related prediction records inside eligible outer annotation windows",
                                  committed_detection_hash_complete=bool(eligible), detection_scan_performed=bool(eligible))
                    if prediction_inputs["related_predictions"] > PREDICTION_LIMIT:
                        score = _evaluation_base()
                        score.update(status="capacity_blocked", reason="related_prediction_limit_exceeded",
                                     label_count=label_count, related_prediction_count=prediction_inputs["related_predictions"])
                    elif prediction_inputs["invalid_predictions"]:
                        score = _evaluation_base()
                        score.update(status="invalid_predictions", reason="related_prediction_provenance_is_invalid",
                                     invalid_predictions=prediction_inputs["invalid_predictions"])
                    else:
                        score = _score(labels, predictions, registered, source_kind, label_count)
                except (KeyError, TypeError, ValueError, AttributeError) as exc:
                    score = _evaluation_base()
                    score.update(status="invalid_annotations", reason=str(exc))
                    inputs.update(detection_sha256=None, committed_detection_sha256=None, detection_hash_complete=False)
            with runner.store.lock:
                runner._committed_research_cache = {"key": key, "score": copy.deepcopy(score), "inputs": copy.deepcopy(inputs)}
        snapshot = {"run_id": runtime["run_id"], "identity": identity, "processed_cursor": processed,
                    "outbox_cursor": outbox, "max_detection_record_id": maxima["detections"], "max_label_record_id": maxima["labels"],
                    "source_kind": source_kind, "market_validation": "not_run", **inputs}
        score["snapshot"] = copy.deepcopy(snapshot)
        return {"runtime": runtime, "recognition_evaluation": score, "experimental_account": experimental, "snapshot": snapshot}
    finally:
        if owns_connection and connection is not None:
            try:
                connection.execute("ROLLBACK")
            finally:
                connection.close()
