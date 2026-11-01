"""One-to-one event matching on explicitly annotated intervals only."""
from __future__ import annotations

from collections import defaultdict
import hashlib
import math
from recognition.models import timestamp


def wilson(successes, trials):
    if not trials:
        return None
    z, p = 1.96, successes / trials
    den = 1 + z * z / trials
    center = (p + z * z / (2 * trials)) / den
    margin = z / den * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials))
    return [round(max(0, center - margin), 4), round(min(1, center + margin), 4)]


def split_group(group):
    # The input group must identify the day AND correlated event family; all
    # symbols, scales and variants of that group retain one split.
    value = int(hashlib.sha256(group.encode()).hexdigest()[:8], 16) % 10
    return "development" if value < 6 else "validation" if value < 8 else "test"


def _time(value, name):
    try:
        return timestamp(value)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError(f"{name}: offset-aware ISO timestamp required") from exc


def _window(row, stage=None):
    window = row.get("stage_windows", {}).get(stage, row) if stage else row
    return _time(window["start"], "start"), _time(window["end"], "end")


def validate_labels(labels):
    ids = set()
    groups = {}
    for row in labels:
        for key in ("case_id", "setup_id", "symbol", "venue", "group_id", "source"):
            if not isinstance(row.get(key), str) or not row[key]:
                raise ValueError(f"labels require nonempty {key}")
        if row["case_id"] in ids:
            raise ValueError("duplicate label case_id")
        ids.add(row["case_id"])
        if row["source_kind"] not in {"synthetic", "real_market"}:
            raise ValueError("labels must identify synthetic vs real_market")
        if type(row["positive"]) is not bool or type(row["direction"]) is not int or row["direction"] not in {-1, 1}:
            raise ValueError("invalid label outcome or direction")
        if "data_eligible" in row and type(row["data_eligible"]) is not bool:
            raise ValueError("data_eligible must be boolean")
        if not row.get("group_id") or not row.get("source"):
            raise ValueError("labels require group_id and source provenance")
        start, end = _window(row)
        if start > end:
            raise ValueError("reversed annotation window")
        if "disputed" in row and type(row["disputed"]) is not bool:
            raise ValueError("disputed must be boolean")
        if "complete_coverage" in row and type(row["complete_coverage"]) is not bool:
            raise ValueError("complete_coverage must be boolean")
        if row.get("complete_coverage") and (row.get("disputed") or not row.get("data_eligible", True)):
            raise ValueError("complete_coverage requires undisputed data-eligible annotations")
        if "trigger_time" in row and row["trigger_time"] is not None:
            if not start <= _time(row["trigger_time"], "trigger_time") <= end:
                raise ValueError("trigger_time must belong to annotation window")
        windows = row.get("stage_windows", {})
        if not isinstance(windows, dict) or set(windows) - {"candidate", "formed", "triggered"}:
            raise ValueError("unknown annotation stage")
        for stage in windows:
            lo, hi = _window(row, stage)
            if not start <= lo <= hi <= end:
                raise ValueError("stage window must belong to annotation window")
        split = row.get("split", split_group(row["group_id"]))
        if split not in {"development", "validation", "test"}:
            raise ValueError("unknown data split")
        if row["group_id"] in groups and groups[row["group_id"]] != split:
            raise ValueError("correlated group leaks across splits")
        groups[row["group_id"]] = split


def _matches(prediction, label, stage=None):
    lo, hi = _window(label, stage)
    return all(prediction.get(k) == label.get(k, "base") for k in ("setup_id", "variant_id", "symbol", "venue", "direction")) \
        and lo <= _time(prediction["evidence_cutoff"], "evidence_cutoff") <= hi


def validate_coverage(coverage, labels=()):
    """Complete coverage declares absence meaningful, not just sampled cases."""
    groups = {l["group_id"]: l.get("split", split_group(l["group_id"])) for l in labels}
    ids = set()
    for row in coverage:
        if not row.get("coverage_id") or row["coverage_id"] in ids:
            raise ValueError("coverage requires unique coverage_id")
        ids.add(row["coverage_id"])
        if row.get("complete") is not True:
            raise ValueError("only explicitly complete annotation coverage is eligible")
        for key in ("setup_id", "symbol", "venue", "group_id", "source"):
            if not isinstance(row.get(key), str) or not row[key]:
                raise ValueError(f"coverage requires {key}")
        if row.get("source_kind") not in {"synthetic", "real_market"}:
            raise ValueError("coverage requires source_kind")
        if "direction" in row and (type(row["direction"]) is not int or row["direction"] not in {-1, 1}):
            raise ValueError("invalid coverage direction")
        if _window(row)[0] > _window(row)[1]:
            raise ValueError("reversed coverage window")
        if row.get("stage") not in {None, "candidate", "formed", "triggered"}:
            raise ValueError("invalid coverage stage")
        split = row.get("split", split_group(row["group_id"]))
        if split not in {"development", "validation", "test"} or row["group_id"] in groups and groups[row["group_id"]] != split:
            raise ValueError("coverage split leaks across correlated group")
        groups[row["group_id"]] = split
        for label in labels:
            if not label.get("disputed") and label.get("data_eligible", True):
                continue
            if all(row.get(k, "base") == label.get(k, "base") for k in ("setup_id", "variant_id", "symbol", "venue", "source_kind")):
                lo, hi = _window(row)
                start, end = _window(label)
                same_direction = "direction" not in row or row["direction"] == label["direction"]
                if same_direction and max(lo, start) <= min(hi, end):
                    raise ValueError("complete coverage overlaps disputed or data-ineligible annotation")


def _covered(prediction, row, stage):
    lo, hi = _window(row)
    return row.get("stage") in {None, stage} and all(
        prediction.get(k, "base") == row.get(k, "base") for k in ("setup_id", "variant_id", "symbol", "venue")) \
        and ("direction" not in row or row["direction"] == prediction["direction"]) \
        and lo <= _time(prediction["evidence_cutoff"], "evidence_cutoff") <= hi


def _counts(predictions, labels, states, stage=None, coverage=()):
    predictions = [p for p in predictions if p["state"] in states and p["data_status"] == "complete"]
    first = {}
    for p in sorted(predictions, key=lambda p: (_time(p["evidence_cutoff"], "evidence_cutoff"), p["detection_id"])):
        first.setdefault(p["detection_id"], p)
    predictions = list(first.values())
    positive = sorted((l for l in labels if l["positive"]), key=lambda l: (l["end"], l["start"], l["case_id"]))
    edges = {i: [j for j, p in enumerate(predictions) if _matches(p, label, stage)] for i, label in enumerate(positive)}
    owners = {}
    def augment(label_index, visited):
        for prediction_index in edges[label_index]:
            if prediction_index in visited:
                continue
            visited.add(prediction_index)
            if prediction_index not in owners or augment(owners[prediction_index], visited):
                owners[prediction_index] = label_index
                return True
        return False
    for i in range(len(positive)):
        augment(i, set())
    matched = set(owners.values())
    used = {predictions[j]["detection_id"] for j in owners}
    delays = [(_time(predictions[j]["identified_at"], "identified_at") - _time(positive[i].get("trigger_time") or _window(positive[i], stage)[0], "trigger_time")).total_seconds()
              for j, i in owners.items()]
    eligible = lambda p: any(_matches(p, l, stage) for l in labels) or any(_covered(p, c, stage) for c in coverage)
    extra = [p for p in predictions if p["detection_id"] not in used and eligible(p)]
    unannotated = [p for p in predictions if not eligible(p)]
    tp, fn, fp = len(matched), len(positive) - len(matched), len(extra)
    intervals = sorted([_window(l, stage) for l in labels] + [_window(c) for c in coverage if c.get("stage") in {None, stage}])
    merged = []
    for lo, hi in intervals:
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
        else:
            merged.append((lo, hi))
    hours = sum((hi - lo).total_seconds() for lo, hi in merged) / 3600
    return dict(tp=tp, fp=fp, fn=fn, recall=tp / (tp + fn) if tp + fn else None,
                precision=tp / (tp + fp) if tp + fp else None,
                recall_95ci=wilson(tp, tp + fn), precision_95ci=wilson(tp, tp + fp),
                false_positives_per_annotated_hour=fp / hours if hours else None,
                confirmation_delay_seconds=sorted(delays), ignored_unannotated_detections=len(unannotated),
                annotated_hours=hours)


def _group(row):
    return (row["setup_id"], row.get("variant_id", "base"), row["source_kind"],
            row.get("split", split_group(row["group_id"])), row["symbol"], row["venue"],
            row.get("regime", "unspecified"))


def evaluate(predictions, labels, coverage=None):
    """Backward-compatible sampled-window evaluation, with optional full coverage."""
    predictions, labels, coverage = list(predictions), list(labels), list(coverage or [])
    source_kinds = {row["source_kind"] for row in labels + coverage}
    for prediction in predictions:
        if "source_kind" in prediction and prediction["source_kind"] not in {"synthetic", "real_market"}:
            raise ValueError("invalid prediction source_kind")
        if len(source_kinds) > 1 and "source_kind" not in prediction:
            raise ValueError("mixed-source evaluation requires prediction source_kind")
        if type(prediction.get("direction")) is not int or prediction["direction"] not in {-1, 1}:
            raise ValueError("prediction direction must be integer +1/-1")
        if prediction.get("state") not in {"candidate", "formed", "triggered", "invalidated", "expired", "completed"}:
            raise ValueError("invalid prediction state")
        if prediction.get("data_status") not in {"complete", "partial", "unavailable"}:
            raise ValueError("invalid prediction data_status")
        for field in ("detection_id", "setup_id", "variant_id", "symbol", "venue"):
            if not isinstance(prediction.get(field), str) or not prediction[field]:
                raise ValueError(f"predictions require {field}")
        _time(prediction["evidence_cutoff"], "evidence_cutoff")
        _time(prediction["identified_at"], "identified_at")
    validate_labels(labels)
    validate_coverage(coverage, labels)
    versions = defaultdict(set)
    for row in predictions + labels:
        for field in ("algorithm_sha256", "configuration_sha256", "rule_version", "source_version"):
            if row.get(field):
                versions[(row["setup_id"], row.get("variant_id", "base"), field)].add(row[field])
    if any(len(values) > 1 for values in versions.values()):
        raise ValueError("mixed rule/source/algorithm/configuration versions require separate evaluation runs")
    groups = defaultdict(list)
    coverage_groups = defaultdict(list)
    disputed = []
    ineligible = []
    for l in labels:
        if l.get("disputed"):
            disputed.append(l["case_id"])
            continue
        if not l.get("data_eligible", True):
            ineligible.append(l["case_id"])
            continue
        key = _group(l)
        groups[key].append(l)
    for c in coverage:
        key = _group(c)
        groups.setdefault(key, [])
        coverage_groups[key].append(c)
    reports = []
    for key, rows in sorted(groups.items()):
        sample = [p for p in predictions if p["setup_id"] == key[0] and p["variant_id"] == key[1]
                  and p["symbol"] == key[4] and p["venue"] == key[5]
                  and ("source_kind" not in p or p["source_kind"] == key[2])]
        candidate = _counts(sample, rows, {"candidate", "formed", "triggered"}, "candidate", coverage_groups[key])
        confirmed = _counts(sample, rows, {"formed", "triggered"}, "formed", coverage_groups[key])
        triggered = _counts(sample, rows, {"triggered"}, "triggered", coverage_groups[key])
        positive, negative = sum(l["positive"] for l in rows), sum(not l["positive"] for l in rows)
        enough = key[2] == "real_market" and key[3] == "test" and positive >= 10 and negative >= 10
        meets = enough and candidate["recall"] is not None and candidate["recall"] >= .95 \
            and confirmed["recall"] is not None and confirmed["recall"] >= .85 \
            and confirmed["precision"] is not None and confirmed["precision"] >= .85
        reports.append(dict(setup_id=key[0], variant_id=key[1], source_kind=key[2], split=key[3],
            symbol=key[4], venue=key[5], regime=key[6], positive_cases=positive, negative_cases=negative,
            candidate=candidate, confirmed=confirmed, triggered=triggered,
            acceptance="target_met_on_this_slice" if meets else "synthetic_logic_only" if key[2] == "synthetic" else "insufficient_sample" if not enough else "target_not_met"))
    return {"report_version": "0.2.0", "groups": reports, "disputed_cases": disputed, "data_ineligible_cases": ineligible,
            "scope": "event recognition only; no returns, orders, fills or profitability verification",
            "sampling_note": "unmatched predictions inside complete coverage are false positives; outside coverage or sampled windows they remain unannotated",
            "targets": {"candidate_recall": .95, "confirmed_recall": .85, "confirmed_precision": .85},
            "market_validation": "not_run" if not any(l["source_kind"] == "real_market" for l in labels) else "slice_reports_only"}
