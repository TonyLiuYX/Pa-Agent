"""Incremental rule graph with immutable, evidence-bearing state transitions."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import timedelta
import hashlib
import json

from .data import SymbolData
from .features import Frame, make_frame
from .models import DetectionEvent, MarketEvent, timestamp
from .specs import load_specs, RULE_VERSION
from .config import RecognitionConfig
from .numeric import clamp


def condition(test, values, captures):
    name, operation, target = test
    actual = values.get(name)
    target = captures.get(target[1:]) if isinstance(target, str) and target.startswith("@") else target
    if actual is None or target is None:
        return None, actual, target
    operations = {"eq": lambda: actual == target, "gt": lambda: actual > target,
                  "ge": lambda: actual >= target, "lt": lambda: actual < target,
                  "le": lambda: actual <= target}
    if operation not in operations:
        raise ValueError(f"unknown predicate operation: {operation}")
    return bool(operations[operation]()), actual, target


@dataclass
class Candidate:
    detection_id: str
    spec: dict
    symbol: str
    venue: str
    direction: int
    seconds: int
    start: object
    last_stage: object
    captured: dict
    step: int = 1
    bars_since_step: int = 0
    state: str = "candidate"
    conditions: list = field(default_factory=list)
    related: list = field(default_factory=list)
    sources: set = field(default_factory=set)
    scales: set = field(default_factory=set)
    unresolved: set = field(default_factory=set)
    risk_breaches: int = 0
    capture_times: dict = field(default_factory=dict)
    capture_features: dict = field(default_factory=dict)
    trigger_recorded: bool = False


class GraphRecognizer:
    """Rule interpreter is independent of ingestion, permitting isolated graph tests."""
    def __init__(self, specs=None, config=None, runtime_policy=None):
        self.specs = specs if specs is not None else load_specs()
        self.config = (config or RecognitionConfig()).validate()
        from .provenance import algorithm_sha256
        self.algorithm_sha256 = algorithm_sha256()
        self.active: list[Candidate] = []
        self.archive: list[Candidate] = []
        self.edges = {}
        self.reentries = {}  # invalidated sampled episodes awaiting a later valid entry
        self.transitions = []
        self.pruned = []  # no silent candidate limits
        self.runtime_policy = runtime_policy
        self.coverage_stats = {}
        self.evidence_truncations = 0

    def _tests(self, stage, values, captures, frame):
        rows = []
        for test in stage["tests"]:
            result, actual, expected = condition(test, values, captures)
            rows.append({"stage": stage["name"], "feature": test[0], "operation": test[1],
                         "actual": actual, "expected": expected,
                         "result": "unknown" if result is None else "satisfied" if result else "not_satisfied",
                         "observed_at": frame.bar.end.isoformat(), "source_ids": frame.sources,
                         "provenance": "editor_operationalization"})
        return all(r["result"] == "satisfied" for r in rows), rows

    def _capture(self, stage, values, target, candidate=None, frame=None):
        for name, feature in stage["capture"].items():
            if values.get(feature) is None:
                return False
        for name, feature in stage["capture"].items():
            target[name] = values[feature]
            if candidate is not None:
                candidate.capture_times[name] = frame.bar.end
                candidate.capture_features[name] = feature
        return True

    @staticmethod
    def _edge_key(candidate):
        return (candidate.spec["setup_id"], candidate.spec["variant_id"], candidate.symbol,
                candidate.venue, candidate.direction, candidate.seconds,
                candidate.captured.get("key_reference"))

    @staticmethod
    def _same_frame(spec, step):
        # Older two-condition variants retain their explicit simultaneous policy.
        return spec["stages"][step].get("same_frame", False) or (step == 1 and (
            spec.get("co_temporal", False) or spec["setup_id"] in {"SET-0048", "SET-0049"}))

    @staticmethod
    def _entry_risk_valid(spec, values, captures):
        if spec.get("invalidation_mode") != "sampled_close":
            return True
        price, risk = values.get("directional_price"), captures.get("risk")
        return price is not None and risk is not None and risk < price

    def _trigger_uncertainty(self, candidate, frame, values, stage):
        # A final OHLC signal endpoint is only available when that bar closes.
        # Its own high/low cannot establish a subsequent break of that signal.
        same_signal = any(isinstance(test[2], str) and test[2].startswith("@")
            and candidate.capture_features.get(test[2][1:]) == "signal_edge"
            and candidate.capture_times.get(test[2][1:]) == frame.bar.end
            for test in stage["tests"])
        trigger, risk = candidate.captured.get("trigger"), candidate.captured.get("risk")
        mode = candidate.spec.get("invalidation_mode", "extreme_break")
        low = values.get("directional_low")
        threshold = risk if mode == "atr_touch" or risk is None else risk - values.get("tolerance", 0)
        breaks_risk = lambda price: price <= threshold if mode == "atr_touch" else price < threshold
        breached = mode not in {"sampled_close", "acceptance_close"} and low is not None and risk is not None and breaks_risk(low)
        path = values.get("price_path")
        endpoint_ops = [test[1] for test in stage["tests"] if test[2] == "@trigger"]
        reaches_trigger = lambda price: price > trigger if "gt" in endpoint_ops else price >= trigger
        trigger_index = next((i for i, p in enumerate(path or []) if trigger is not None and reaches_trigger(p)), None)
        risk_indices = [i for i, p in enumerate(path or []) if risk is not None and breaks_risk(p)]
        unordered_risk = trigger is not None and breached and (trigger_index is None or not risk_indices)
        if same_signal or unordered_risk:
            candidate.unresolved.add("intrabar_order_unknown")
            if same_signal:
                candidate.conditions.append({"stage": stage["name"], "feature": "intrabar_order",
                    "operation": "eq", "actual": None, "expected": "signal_available_before_break",
                    "result": "unknown", "observed_at": frame.bar.end.isoformat(),
                    "source_ids": frame.sources, "provenance": "editor_operationalization"})
        if breached and not same_signal and trigger_index is not None:
            later_risk = next((i for i in risk_indices if i > trigger_index), None)
            if later_risk is not None:
                return {"stage": "结构失效", "feature": "risk", "operation": "le" if mode == "atr_touch" else "lt",
                    "actual": path[later_risk], "expected": threshold, "result": "satisfied",
                    "observed_at": frame.bar.end.isoformat(), "source_ids": frame.sources,
                    "provenance": "editor_operationalization"}
        return None

    def _advance(self, candidate, frame, outputs):
        """Complete a pending stage, then only explicitly simultaneous successors."""
        continuation_cutoff = candidate.last_stage
        while candidate.step < len(candidate.spec["stages"]):
            stage = candidate.spec["stages"][candidate.step]
            ordered_stop = None
            values = self._values(frame, candidate, observation_after=continuation_cutoff)
            satisfied, checks = self._tests(stage, values, candidate.captured, frame)
            missing = set(candidate.spec["requirements"]) - frame.capabilities
            if not satisfied or missing:
                break
            captures = dict(candidate.captured)
            if not self._capture(stage, values, captures):
                break
            if candidate.step == len(candidate.spec["stages"]) - 1 and not self._entry_risk_valid(
                    candidate.spec, values, captures):
                break
            self._capture(stage, values, candidate.captured, candidate, frame)
            candidate.conditions.extend(checks)
            candidate.step += 1
            candidate.last_stage = frame.bar.end
            candidate.bars_since_step = 0
            if candidate.step == len(candidate.spec["stages"]):
                candidate.state = "triggered"
                ordered_stop = self._trigger_uncertainty(candidate, frame, values, stage)
            elif candidate.step == len(candidate.spec["stages"]) - 1:
                candidate.state = "formed"
            outputs.append(self._output(candidate, frame))
            if candidate.state == "triggered" and ordered_stop is not None:
                candidate.state = "invalidated"
                outputs.append(self._output(candidate, frame, [ordered_stop]))
            if candidate.step == len(candidate.spec["stages"]) or not self._same_frame(
                    candidate.spec, candidate.step):
                break

    def _values(self, frame, candidate=None, direction=None, observation_after=None):
        d = candidate.direction if candidate else direction
        values = dict(frame.values[d])
        if candidate and frame.seconds != candidate.seconds and frame.seconds in candidate.spec.get("continuation_scales", []):
            observations = values.get("price_observations")
            if observations is not None:
                cutoff = observation_after if observation_after is not None else candidate.last_stage
                prices = [row["price"] for row in observations if timestamp(row["time"]) > cutoff]
                # An event window can overlap a bar-based anchor. Only actual
                # prices received after that frozen stage may advance or stop it.
                values.update(directional_price=prices[-1] if prices else None,
                              directional_extreme=max(prices) if prices else None,
                              directional_low=min(prices) if prices else None,
                              price_path=prices)
        if candidate and candidate.captured.get("key_reference") is not None:
            context = next((c for c in frame.key_hypotheses.get(d, []) if c["key"] == candidate.captured["key_reference"]), None)
            if context:
                values.update(context)
        if candidate and candidate.captured.get("level") is not None:
            level = candidate.captured["level"]
            tolerance = values.get("tolerance", 0)
            price, low = values.get("directional_price"), values.get("directional_low")
            if price is not None and low is not None:
                values["captured_retest"] = low <= level + tolerance and price >= level - tolerance
                # Acceptance must concern THIS frozen reference, not an arbitrary
                # overlapping set of bars elsewhere in the same direction.
                closes = values.get("recent_directional_closes")
                values["accepted"] = len(closes) >= 3 and all(p > level + tolerance for p in closes) if closes is not None else None
                values["strict_hold"] = low >= level - values.get("tick_size", 0)
                values["old_extreme_retest"] = abs(low - level) <= 1.5 * max(tolerance, values.get("noise_scale", tolerance))
                values["retest_beyond"] = low < level - tolerance
                values["retest_inside"] = low > level + tolerance
                values["retest_equal"] = abs(low - level) <= tolerance
                if candidate.spec["setup_id"] == "SET-0041":
                    path = values.get("price_path")
                    values["thin_hold"] = bool(path and min(path) >= level - tolerance and price >= level - tolerance)
        if candidate:
            values["parent_id"] = candidate.captured.get("parent_id")
        return values

    def _contexts(self, spec, frame):
        for d in spec["directions"]:
            contexts = frame.key_hypotheses.get(d, []) if spec["setup_id"] in {"SET-0003", "SET-0010", "SET-0016", "SET-0039"} else []
            unique = {c["key"]: c for c in contexts}
            for context in unique.values() if unique else [{}]:
                yield d, context

    def _output(self, candidate, frame, extra=(), missing=None):
        spec = candidate.spec
        if self.runtime_policy:
            limit = self.runtime_policy.max_evidence_sources
            truncated = (len(candidate.sources) > limit or len(frame.sources) > limit
                         or any(len(row.get("source_ids", [])) > limit for row in candidate.conditions))
            if len(candidate.sources) > limit:
                candidate.sources = set(sorted(candidate.sources)[:limit])
            # Replace rows rather than modifying dictionaries referenced by an
            # already emitted immutable detection. Full evidence stays in the log.
            candidate.conditions = [{**row, "source_ids": row.get("source_ids", [])[:limit]}
                                    for row in candidate.conditions]
            extra = [{**row, "source_ids": row.get("source_ids", [])[:limit]} for row in extra]
            if truncated:
                if "evidence_source_examples_truncated" not in candidate.unresolved:
                    self.evidence_truncations += 1
                candidate.unresolved.add("evidence_source_examples_truncated")
            example_sources = frame.sources[:limit]
        else:
            example_sources = frame.sources
        missing = sorted(set(missing if missing is not None else set(spec["requirements"]) - frame.capabilities))
        missing = sorted(set(missing) | candidate.unresolved)
        if frame.quality:
            missing += ["quality:" + x for x in frame.quality if x != "warmup"]
        values = frame.values[candidate.direction]
        soft = []
        weights = {"trend": 1, "impulse": 1, "compression": .5}
        if spec["family"] in {"flow", "flow_profile", "footprint"}:
            weights = {"flow_activity": 1, "impulse": .5, "trend": .5}
        for name, weight in weights.items():
            value = values.get(name)
            soft.append({"stage": "增强证据", "feature": name, "operation": "score", "actual": value,
                         "expected": None, "result": "unknown" if value is None else "measured",
                         "observed_at": frame.bar.end.isoformat(), "source_ids": example_sources,
                         "provenance": "editor_operationalization", "weight": weight})
        tests = candidate.conditions + list(extra)
        known = sum(c["result"] != "unknown" for c in tests)
        passed = sum(c["result"] == "satisfied" for c in tests)
        total_core = sum(len(stage["tests"]) for stage in spec["stages"])
        soft_score = sum(clamp(s["actual"] / (2 if s["feature"] == "flow_activity" else 1)) * s["weight"]
                         for s in soft if s["actual"] is not None) / sum(weights.values())
        row = DetectionEvent(
            detection_id=candidate.detection_id, setup_id=spec["setup_id"], variant_id=spec["variant_id"],
            symbol=frame.symbol, venue=frame.venue, direction=candidate.direction,
            scale_seconds=candidate.seconds, state=candidate.state,
            identified_at=frame.arrived_at.isoformat(), evidence_cutoff=frame.bar.end.isoformat(),
            anchor={"started_at": candidate.start.isoformat(), "captures": dict(candidate.captured),
                    "scales": sorted(candidate.scales), "source_ids": sorted(candidate.sources)},
            conditions=tests + soft, structure_score=round(70 * min(1, passed / max(1, total_core)) + 30 * soft_score, 2),
            evidence_score=round(100 * known / max(1, len(tests)) * len(set(spec["requirements"]) & frame.capabilities)
                                 / max(1, len(set(spec["requirements"]))), 2),
            data_status="partial" if missing else "complete", missing=missing,
            source_version=spec["source_version"], source_sha256=spec["source_sha256"], rule_version=RULE_VERSION,
            market_status="migration_unvalidated" if frame.market in {"equity", "ETF"} and not any(
                "股票" in m or "ETF" in m or "SPY" in m or "AAPL" in m or "美股" in m for m in spec["source_markets"])
                else "source_context_unvalidated", related_ids=list(candidate.related),
            algorithm_sha256=self.algorithm_sha256, configuration_sha256=self.config.fingerprint(),
            observation_kind=frame.observation_kind,
        )
        self.transitions.append(row)
        key = spec["setup_id"] + ":" + spec["variant_id"]
        stats = self.coverage_stats.setdefault(key, {"transition_count": 0, "triggered": 0, "missing_observed": set()})
        stats["transition_count"] += 1
        # A trigger is emitted once at the final stage; repeated sampled-risk
        # observations are not additional triggered episodes.
        if row.state == "triggered" and not candidate.trigger_recorded:
            stats["triggered"] += 1
            candidate.trigger_recorded = True
        stats["missing_observed"].update(row.missing)
        if self.runtime_policy and len(self.transitions) > self.runtime_policy.max_transitions:
            del self.transitions[:-self.runtime_policy.max_transitions]
        return row

    def process(self, frame: Frame, allow_new=True):
        outputs = []
        compatible = lambda spec: (spec["variant_id"] == "source_open") == (frame.observation_kind == "open_price")
        relevant = [c for c in self.active if c.symbol == frame.symbol and c.venue == frame.venue
                    and (c.seconds == frame.seconds or frame.seconds in c.spec.get("continuation_scales", []))
                    and compatible(c.spec)]
        for c in relevant:
            if c.state in {"invalidated", "expired", "completed"}:
                continue
            if set(frame.quality) - {"warmup"}:
                c.unresolved.add("context_data_disruption")
                if c.state != "triggered":
                    c.state = "expired"
                    outputs.append(self._output(c, frame))
                    continue
            values = self._values(frame, c)
            c.sources.update(frame.sources)
            if frame.bar.end <= c.last_stage:
                continue
            c.scales.add(frame.seconds)
            c.bars_since_step += 1
            ttl = c.spec["ttl_minutes"] * 60
            expiry = (frame.bar.end - c.start).total_seconds() > ttl
            stage = c.spec["stages"][c.step] if c.step < len(c.spec["stages"]) else None
            deadline = stage.get("deadline_bars") if stage else None
            expiry |= deadline is not None and c.bars_since_step > deadline
            if expiry and c.state != "triggered":
                c.state = "expired"
                outputs.append(self._output(c, frame))
                continue
            risks = c.captured.get("risk")
            logical_floor = c.captured.get("logical_floor")
            if c.state != "triggered" and logical_floor is not None and values.get("directional_price") is not None and values["directional_price"] < logical_floor - values.get("tolerance", 0):
                c.state = "invalidated"
                outputs.append(self._output(c, frame))
                continue
            if c.state == "triggered":
                sampled_risk = c.spec["setup_id"] == "SET-0048"
                mode = c.spec.get("invalidation_mode", "extreme_break")
                risk_value = values.get("directional_price") if mode in {"sampled_close", "acceptance_close"} else values.get("directional_low")
                if sampled_risk:
                    if not values.get("half_hour"):
                        continue
                    field = "same_band" if c.spec["variant_id"] == "same_side_band" else "vwap_band" if c.spec["variant_id"] == "same_side_vwap" else "opposite_band"
                    risks = values.get(field)
                    if risks is not None:
                        c.captured["risk"] = risks
                    else:
                        c.unresolved.add("sampled_risk_unknown")
                        outputs.append(self._output(c, frame, [{"stage": "采样保护位", "feature": field,
                            "operation": "lt", "actual": None, "expected": risk_value,
                            "result": "unknown", "observed_at": frame.bar.end.isoformat(),
                            "source_ids": frame.sources, "provenance": "editor_operationalization"}]))
                        continue
                breached = risks is not None and risk_value is not None and (risk_value <= risks if mode in {"atr_touch", "sampled_close"} else risk_value < risks - values.get("tolerance", 0))
                c.risk_breaches = c.risk_breaches + 1 if breached else 0
                if breached and (mode != "acceptance_close" or c.risk_breaches >= 2):
                    c.state = "invalidated"
                    if c.spec.get("reentry_after_invalidation"):
                        self.reentries[self._edge_key(c)] = frame.bar.end
                    outputs.append(self._output(c, frame, [{"stage": "结构失效", "feature": "risk",
                        "operation": "le" if mode in {"atr_touch", "sampled_close"} else "lt",
                        "actual": risk_value, "expected": risks if mode in {"atr_touch", "sampled_close"} else risks - values.get("tolerance", 0), "result": "satisfied",
                        "observed_at": frame.bar.end.isoformat(), "source_ids": frame.sources, "provenance": "editor_operationalization"}]))
                continue
            if any(condition(test, values, c.captured)[0] is True for test in c.spec["cancel"]):
                c.state = "invalidated"
                outputs.append(self._output(c, frame))
                continue
            if frame.quality and set(frame.quality) != {"warmup"}:
                continue
            self._advance(c, frame, outputs)
        # First-condition edges generate candidates rather than one per bar.
        for spec in self.specs if allow_new else []:
            if frame.seconds not in spec["scales"]:
                continue
            if not compatible(spec):
                continue
            for direction, context in self._contexts(spec, frame):
                values = self._values(frame, direction=direction) | context
                if spec["setup_id"] == "SET-0048" and not values.get("half_hour"):
                    continue
                stage = spec["stages"][0]
                satisfied, checks = self._tests(stage, values, {}, frame)
                broad = all(check["result"] == "satisfied" or (
                    check["feature"] in {"trend", "impulse", "compression"} and check["operation"] in {"gt", "ge"}
                    and check["actual"] is not None and check["actual"] >= check["expected"] - self.config.candidate_slack)
                    for check in checks)
                key = (spec["setup_id"], spec["variant_id"], frame.symbol, frame.venue, direction, frame.seconds, context.get("key"))
                was = self.edges.get(key, False)
                self.edges[key] = broad if not (set(frame.quality) - {"warmup"}) else False
                reentry_time = self.reentries.get(key)
                reentry = reentry_time is not None and frame.bar.end > reentry_time
                same_episode = spec.get("reentry_after_invalidation") and any(
                    self._edge_key(c) == key and c.state not in {"expired", "invalidated", "completed"}
                    for c in self.active)
                if not broad or (was and not reentry) or same_episode or (
                        reentry_time is not None and frame.bar.end <= reentry_time) or (
                        frame.quality and set(frame.quality) != {"warmup"}):
                    continue
                captures = {}
                if not self._capture(stage, values, captures):
                    if spec.get("invalidation_mode") == "sampled_close":
                        self.edges[key] = False
                    continue
                if not self._entry_risk_valid(spec, values, captures):
                    # A sampled entry needs an actual protective side. An unusable
                    # risk is not a successfully consumed outside-band episode.
                    self.edges[key] = False
                    continue
                if context:
                    captures["key_reference"] = context["key"]
                anchor = json.dumps({k: v for k, v in captures.items() if k in {"level", "trigger", "risk", "logical_floor"}}, sort_keys=True)
                identity = "|".join(map(str, (*key[:5], frame.bar.end.isoformat(), anchor)))
                cid = "DET-" + hashlib.sha256(identity.encode()).hexdigest()[:20]
                # Merge only matching time AND anchors. Distinct cycles stay distinct.
                equivalent = next((c for c in self.active if c.detection_id == cid and c.state not in {"expired", "invalidated", "completed"}), None)
                if equivalent:
                    equivalent.scales.add(frame.seconds)
                    equivalent.sources.update(frame.sources)
                    continue
                c = Candidate(cid, spec, frame.symbol, frame.venue, direction, frame.seconds,
                              frame.bar.end, frame.bar.end, captures, conditions=checks,
                              sources=set(frame.sources), scales={frame.seconds},
                              capture_times={name: frame.bar.end for name in stage["capture"]},
                              capture_features=dict(stage["capture"]))
                available = not (set(spec["requirements"]) - frame.capabilities)
                if not satisfied or not available:
                    c.step = 0
                    c.conditions = []
                if values.get("parent_id"):
                    c.captured["parent_id"] = values["parent_id"]
                    c.related.append(values["parent_id"])
                c.related.extend(p.detection_id for p in self.active if p.symbol == c.symbol and p.venue == c.venue
                    and p.spec["setup_id"] != c.spec["setup_id"] and p.start == c.start
                    and bool(p.sources & c.sources))
                c.related = sorted(set(c.related))
                if len(spec["stages"]) == 2 and satisfied and available:
                    c.state = "formed" if not (set(spec["requirements"]) - frame.capabilities) else "candidate"
                if self.runtime_policy and len(self.active) >= self.runtime_policy.max_active:
                    from .policy import CapacityExceeded
                    raise CapacityExceeded("active candidate limit reached; no active candidate was removed")
                self.active.append(c)
                outputs.append(self._output(c, frame, checks if c.step == 0 else ()))
                if satisfied and available and c.step < len(spec["stages"]) and self._same_frame(spec, c.step):
                    self._advance(c, frame, outputs)
                if reentry:
                    self.reentries.pop(key, None)
        finished = [c for c in self.active if c.state in {"invalidated", "expired", "completed"}]
        self.archive.extend(finished)
        self.active = [c for c in self.active if c not in finished]
        return outputs

    def complete_session(self, frame):
        outputs = []
        for c in list(self.active):
            if c.symbol == frame.symbol and c.venue == frame.venue:
                c.state = "completed" if c.state == "triggered" else "expired"
                outputs.append(self._output(c, frame))
                self.active.remove(c)
                self.archive.append(c)
        for key in list(self.edges):
            if key[2:4] == (frame.symbol, frame.venue):
                del self.edges[key]
        for key in list(self.reentries):
            if key[2:4] == (frame.symbol, frame.venue):
                del self.reentries[key]
        return outputs


class RecognitionEngine:
    def __init__(self, setup_ids=None, include_variants=False, config=None, runtime_policy=None):
        self.config = (config or RecognitionConfig()).validate()
        self.runtime_policy = runtime_policy.validate() if runtime_policy else None
        self.graph = GraphRecognizer(load_specs(setup_ids, include_variants), self.config, self.runtime_policy)
        self.universe = {}
        self.seen = {}
        self.arrival_watermark = None
        self.last_frames = {}
        self.scale_frames = {}
        self.audit = []
        self.runtime_metrics = {"seen_cache_evictions": 0, "stream_cache_evictions": 0, "evidence_truncations": 0}

    def _checkpoint_state(self):
        """Small envelope of live state references, shared by both writers."""
        return {"config": self.config, "runtime_policy": self.runtime_policy,
                "graph": {k: v for k, v in vars(self.graph).items() if k not in {"config", "specs", "runtime_policy", "algorithm_sha256"}},
                **{k: v for k, v in vars(self).items() if k not in {"config", "graph", "runtime_policy"}}}

    def _checkpoint_metadata(self):
        from .checkpoint import CHECKPOINT_VERSION, digest
        return {"checkpoint_version": CHECKPOINT_VERSION, "algorithm_sha256": self.graph.algorithm_sha256,
                "rule_version": RULE_VERSION, "specs_sha256": digest(self.graph.specs),
                "configuration_sha256": self.config.fingerprint(), "specs": self.graph.specs}

    def snapshot(self):
        """JSON-compatible checkpoint, including active episodes and evidence.

        The durable runner supplies its own log hash and processed sequence in
        an outer envelope. Restore is deliberately strict about code versions.
        """
        from .checkpoint import digest, encode
        state = encode(self._checkpoint_state())
        return {**self._checkpoint_metadata(), "state_sha256": digest(state), "state": state}

    def write_snapshot_json(self, handle):
        """Write canonical(snapshot()) directly to a trusted text handle.

        The single writer must keep this engine unchanged during both passes.
        The ASCII state digest remains v1-compatible; the final envelope uses
        UTF-8 JSON text, without an intermediate encoded state tree.
        """
        from .checkpoint import encode_stream
        raw = self._checkpoint_state()
        state_hash = hashlib.sha256()
        for chunk in encode_stream(raw, ensure_ascii=True):
            state_hash.update(chunk.encode("ascii"))
        metadata = {**self._checkpoint_metadata(), "state_sha256": state_hash.hexdigest()}
        ordinary = json.JSONEncoder(sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False, allow_nan=False)
        handle.write("{")
        for index, name in enumerate(sorted((*metadata, "state"))):
            if index:
                handle.write(",")
            handle.write(json.dumps(name) + ":")
            chunks = encode_stream(raw, ensure_ascii=False) if name == "state" else ordinary.iterencode(metadata[name])
            for chunk in chunks:
                handle.write(chunk)
        handle.write("}")

    def snapshot_json(self):
        """Return the same canonical bytes as snapshot(), without its AST."""
        from io import StringIO
        handle = StringIO()
        self.write_snapshot_json(handle)
        return handle.getvalue()

    @classmethod
    def restore(cls, snapshot, *, runtime_policy=None):
        from .checkpoint import CHECKPOINT_VERSION, digest, decode
        from .provenance import algorithm_sha256
        if snapshot.get("checkpoint_version") != CHECKPOINT_VERSION:
            raise ValueError("unsupported recognition checkpoint version")
        if snapshot.get("algorithm_sha256") != algorithm_sha256() or snapshot.get("rule_version") != RULE_VERSION:
            raise ValueError("recognition checkpoint algorithm/rule version mismatch; replay required")
        if digest(snapshot["state"]) != snapshot["state_sha256"] or digest(snapshot["specs"]) != snapshot["specs_sha256"]:
            raise ValueError("recognition checkpoint integrity mismatch")
        identities = {(s["setup_id"], s["variant_id"]) for s in snapshot["specs"]}
        specs = [s for s in load_specs(include_variants=True) if (s["setup_id"], s["variant_id"]) in identities]
        if digest(specs) != snapshot["specs_sha256"]:
            raise ValueError("recognition checkpoint specification mismatch; replay required")
        state = decode(snapshot["state"])
        config = state.pop("config").validate()
        if config.fingerprint() != snapshot["configuration_sha256"]:
            raise ValueError("recognition checkpoint configuration mismatch")
        policy = state.pop("runtime_policy")
        if runtime_policy is not None and runtime_policy != policy:
            raise ValueError("recognition checkpoint runtime policy mismatch; replay required")
        if policy:
            policy.validate()
        obj = cls(config=config, runtime_policy=policy)
        obj.graph = GraphRecognizer(specs, config, policy)
        graph_state = state.pop("graph")
        expected = set(vars(obj.graph)) - {"config", "specs", "runtime_policy", "algorithm_sha256"}
        if set(graph_state) != expected or set(state) != set(vars(obj)) - {"config", "graph", "runtime_policy"}:
            raise ValueError("recognition checkpoint state shape mismatch")
        vars(obj.graph).update(graph_state)
        vars(obj).update(state)
        return obj

    def feed(self, event: MarketEvent):
        event.validate()
        if self.runtime_policy and event.kind == "session":
            self.runtime_policy.preflight_session(event.payload)
        event = replace(event, event_time=timestamp(event.event_time), arrival_time=timestamp(event.arrival_time))
        signature = hashlib.sha256(json.dumps(event.to_dict(), sort_keys=True).encode()).hexdigest()
        if event.event_id in self.seen:
            if self.seen[event.event_id] != signature:
                raise ValueError("event_id collision with changed payload")
            return []
        if self.arrival_watermark and event.arrival_time < self.arrival_watermark:
            raise ValueError("replay must be ordered by arrival time, not price/event time")
        self.seen[event.event_id] = signature
        self.arrival_watermark = event.arrival_time
        key = (event.symbol, event.venue)
        data = self.universe.setdefault(key, SymbolData(*key, config=self.config))
        if event.kind == "session" and key in self.last_frames:
            last = self.last_frames[key]
            if last.bar.end < data.close_time:
                self.audit.append({"symbol": event.symbol, "reason": "incomplete_prior_session"})
                last = replace(last, quality=[*last.quality, "incomplete_prior_session"])
            outputs = self.graph.complete_session(replace(last, arrived_at=event.arrival_time))
        else:
            outputs = []
        if event.kind == "session":
            self.scale_frames = {k: v for k, v in self.scale_frames.items() if k[:2] != key}
            self.last_frames.pop(key, None)
        closed = data.update(event)
        stream = {"trade": data.trades, "depth": data.depths, "quote": data.quotes}.get(event.kind)
        accepted = stream is not None and bool(stream) and stream[-1].event_id == event.event_id
        if accepted and event.kind in {"trade", "quote"} and any(s["scales"] == [0] for s in self.graph.specs):
            point = self._point_frame(data, event)
            if point and data.open_time <= point.bar.end < data.close_time:
                outputs.extend(self.graph.process(point))
                self.last_frames[key] = point
                self.scale_frames[(*key, 0, "open_price")] = point
        if accepted and event.kind in {"trade", "depth"} and any(
                0 in s["scales"] + s.get("continuation_scales", []) and s["variant_id"] != "source_open"
                for s in self.graph.specs):
            micro = self._micro_frame(data, event)
            if micro:
                self._parents(micro)
                outputs.extend(self.graph.process(micro))
                self.last_frames[key] = micro
                self.scale_frames[(*key, 0, "event_window")] = micro
        closing_frame = None
        for seconds, bar in closed:
            if not data.open_time < bar.end <= data.close_time:
                continue
            frame = make_frame(data, seconds, bar, event.arrival_time, self.universe)
            self._parents(frame)
            self.last_frames[key] = frame
            self.scale_frames[(*key, seconds, "bar")] = frame
            if frame.bar.end < data.close_time:
                outputs.extend(self.graph.process(frame))
            else:
                outputs.extend(self.graph.process(frame, allow_new=False))
                closing_frame = frame
        if closing_frame:
            outputs.extend(self.graph.complete_session(closing_frame))
        self._bound_runtime(event)
        return outputs

    def _bound_runtime(self, event):
        policy = self.runtime_policy
        if not policy:
            return
        if len(self.universe) > policy.max_symbols:
            from .policy import CapacityExceeded
            raise CapacityExceeded("symbol capacity reached; no stream was removed")
        if max(len(self.graph.edges), len(self.graph.reentries)) > policy.max_session_records:
            from .policy import CapacityExceeded
            raise CapacityExceeded("candidate edge capacity reached; no edge or reentry was removed")
        while len(self.seen) > policy.max_seen:
            self.seen.pop(next(iter(self.seen)))
            self.runtime_metrics["seen_cache_evictions"] += 1
        self.audit = self.audit[-policy.max_audit:]
        # Recent archived episodes can still be causal parents. Retain all of
        # them; reaching that capacity stops the runtime instead of losing them.
        def recent_parent(candidate):
            if (candidate.state not in {"triggered", "invalidated"} or candidate.unresolved
                    or candidate.captured.get("risk") is None or candidate.spec["setup_id"] == "SET-0019"):
                return False
            frame = self.last_frames.get((candidate.symbol, candidate.venue))
            cutoff = frame.bar.end - timedelta(seconds=1800) if frame else candidate.last_stage
            return candidate.last_stage >= cutoff
        parents = [c for c in self.graph.archive if recent_parent(c)]
        stale = [c for c in self.graph.archive if not recent_parent(c)]
        if len(parents) > policy.max_archive:
            from .policy import CapacityExceeded
            raise CapacityExceeded("recent parent archive capacity reached; replay with larger capacity")
        self.graph.archive = stale[-max(0, policy.max_archive - len(parents)):] + parents if len(parents) < policy.max_archive else parents
        for data in self.universe.values():
            if max(len(data.news), len(data.consensus), len(data.profiles), len(data.buckets),
                   sum(len(prices) for prices in data.tpo.values())) > policy.max_session_records:
                from .policy import CapacityExceeded
                raise CapacityExceeded("session evidence capacity reached; no cumulative profile was discarded")
            for name in ("trades", "quotes", "depths"):
                stream = getattr(data, name)
                if len(stream) > policy.max_stream_events:
                    removed = len(stream) - policy.max_stream_events
                    while len(stream) > policy.max_stream_events:
                        stream.popleft()
                    data.bad.add("runtime_cache_truncated_" + name)
                    data.last_disruption[{"trades": "trade", "quotes": "quote", "depths": "depth"}[name]] = event.event_time
                    self.runtime_metrics["stream_cache_evictions"] += removed
            for seconds in data.bars:
                if len(data.bars[seconds]) > policy.max_bars:
                    data.note_bar_truncation(seconds, data.bars[seconds][:-policy.max_bars])
                    del data.bars[seconds][:-policy.max_bars]
                    data.bad.add("runtime_bar_cache_truncated")
                    data.last_disruption["bar"] = event.event_time
            data.profile_samples = data.profile_samples[-policy.max_bars:]
            for collection in (data.delta_samples, data.vwap_samples):
                for seconds in collection:
                    collection[seconds] = collection[seconds][-policy.max_bars:]
        for candidate in self.graph.active:
            if len(candidate.sources) > policy.max_evidence_sources:
                candidate.sources = set(sorted(candidate.sources)[:policy.max_evidence_sources])
                if "evidence_source_examples_truncated" not in candidate.unresolved:
                    self.graph.evidence_truncations += 1
                candidate.unresolved.add("evidence_source_examples_truncated")
        self.runtime_metrics["evidence_truncations"] = self.graph.evidence_truncations

    def _point_frame(self, data, event):
        """Source-open observation uses an actually received trade/quote, not a
        backfilled open price learned at the end of the first minute."""
        from .models import Bar
        p = event.payload
        price = p["price"] if event.kind == "trade" else (p["bid"] + p["ask"]) / 2
        vp, tp = data.reference("prior_volume_profile"), data.reference("prior_tpo")
        vp = vp if vp and vp.get("kind") == "volume" and vp.get("venue") == data.venue else None
        tp = tp if tp and tp.get("kind") == "tpo" and tp.get("venue") == data.venue else None
        caps = {"price_observation"}
        if vp:
            caps.add("prior_volume_profile")
        if tp:
            caps.add("prior_tpo")
        if all(data.reference(k) is not None for k in ("prev_high", "prev_low", "prev_close")):
            caps.add("prior_session")
        values = {}
        for d in (1, -1):
            tail = vp.get("low" if d == 1 else "high") if vp else None
            fraction = vp.get("lower_tail_fraction" if d == 1 else "upper_tail_fraction") if vp else None
            scale = max(data.tick, (data.reference("atr14", data.tick * 100)) / 20)
            values[d] = {"prior_tail_open": tail is not None and fraction is not None and fraction < .1 and abs(price - tail) < 2 * scale,
                         "prior_tail_risk": d * tail if tail is not None else None,
                         "instant_open": event.event_time == data.open_time,
                         "price_basis": "trade" if event.kind == "trade" else "quote_mid",
                         "directional_low": d * price, "directional_price": d * price,
                         "tolerance": data.tick, "directional_extreme": d * price}
        bar = Bar(event.event_time, event.event_time, price, price, price, price, 0, None, (event.event_id,))
        quality = [] if event.quality == "complete" else ["partial_price_observation"]
        if data.halted:
            quality.append("halted")
        return Frame(data.symbol, data.venue, 0, bar, event.arrival_time, values, caps,
                     quality, [event.event_id, data.session["session_event_id"]], data.session.get("asset_class", "equity"), "open_price")

    def _micro_frame(self, data, event):
        from .features import event_window, _coverage
        from .models import Bar
        from .numeric import SwingTracker, volatility
        now = event.event_time
        if not data.open_time <= now < data.close_time:
            return None
        previous = data.bars[0][-1] if data.bars[0] else None
        if previous and (now - previous.end).total_seconds() < self.config.event_window_seconds:
            return None
        start = previous.end if previous else now - timedelta(seconds=self.config.event_window_seconds)
        ticks = [t for t in event_window(data.trades, start, now, event.arrival_time) if t.event_time > start]
        if not ticks:
            return None
        opening = previous.close if previous else ticks[0].payload["price"]
        prices = [opening, *(t.payload["price"] for t in ticks)]
        complete = _coverage(data, "trades", now, (now - start).total_seconds())
        bar = Bar(start, now, opening, max(prices), min(prices), prices[-1], sum(t.payload["size"] for t in ticks),
                  sum(t.payload["price"] * t.payload["size"] for t in ticks), tuple(t.event_id for t in ticks),
                  "complete" if complete else "partial")
        scale = volatility(data.bars[0], data.tick)
        for factor in self.config.swing_factors:
            data.swings.setdefault((0, factor), SwingTracker(factor)).push(bar, scale)
        data.bars[0].append(bar)
        if len(data.bars[0]) > 3000:
            data.note_bar_truncation(0, data.bars[0][:-3000])
        data.bars[0] = data.bars[0][-3000:]
        data.delta_samples[0].append((bar, data.cvd if data.cvd_valid else None))
        data.delta_samples[0] = data.delta_samples[0][-3000:]
        if data.profiles:
            from .numeric import value_area
            va = value_area(dict(data.profiles))
            if va:
                data.profile_samples.append((now, tuple(k * data.tick for k in va)))
                data.profile_samples = data.profile_samples[-3000:]
        frame = make_frame(data, 0, bar, event.arrival_time, self.universe)
        for d in (1, -1):
            observations = ([{"time": previous.end.isoformat(), "price": d * opening}] if previous else [])
            observations += [{"time": t.event_time.isoformat(), "price": d * t.payload["price"]} for t in ticks]
            frame.values[d]["price_observations"] = observations
            if complete:
                frame.values[d]["price_path"] = [row["price"] for row in observations]
        return frame

    def _parents(self, frame):
        for d in (1, -1):
            f = frame.values[d]
            parents = [c for c in self.graph.active + self.graph.archive
                       if c.symbol == frame.symbol and c.venue == frame.venue and c.direction == d
                       and c.spec["setup_id"] != "SET-0019" and c.state in {"triggered", "invalidated"}
                       and not c.unresolved
                       and c.captured.get("risk") is not None and 0 < (frame.bar.end - c.last_stage).total_seconds() <= 1800]
            swept = next((p for p in reversed(parents) if f["directional_low"] < p.captured["risk"]), None)
            if parents:
                frame.capabilities.add("parent_detection")
            if swept:
                f.update(parent_sweep=True, parent_id=swept.detection_id)
            else:
                f["parent_sweep"] = False

    def coverage(self):
        by_id = {}
        for spec in self.graph.specs:
            stats = self.graph.coverage_stats.get(spec["setup_id"] + ":" + spec["variant_id"], {})
            by_id[spec["setup_id"] + ":" + spec["variant_id"]] = {
                "specification": "implemented_experimental", "real_market_validation": "not_run",
                "transition_count": stats.get("transition_count", 0), "triggered": stats.get("triggered", 0),
                "missing_observed": sorted(stats.get("missing_observed", set())),
                "requirements": spec["requirements"],
                "last_frame_missing_by_stream_scale": {"|".join(map(str, key)): sorted(set(spec["requirements"]) - frame.capabilities)
                    for key, frame in self.scale_frames.items() if frame.seconds in spec["scales"]
                    and (spec["variant_id"] == "source_open") == (frame.observation_kind == "open_price")},
            }
        return by_id

    def unfinished(self):
        """EOF is a data boundary, not an invented session close or invalidation."""
        return [{"detection_id": c.detection_id, "setup_id": c.spec["setup_id"], "variant_id": c.spec["variant_id"],
                 "symbol": c.symbol, "venue": c.venue, "state": c.state, "scale_seconds": c.seconds,
                 "last_evidence_time": c.last_stage.isoformat(), "reason": "input_ended_before_session_close"}
                for c in self.graph.active]

    def resource_usage(self):
        """Bounded-state counts for soak diagnostics, independent of Python RSS."""
        from dataclasses import asdict
        return {"symbols": len(self.universe), "seen": len(self.seen), "active": len(self.graph.active),
                "archive": len(self.graph.archive), "transitions": len(self.graph.transitions),
                "edges": len(self.graph.edges), "reentries": len(self.graph.reentries),
                "coverage_groups": len(self.graph.coverage_stats), "audit": len(self.audit),
                "stream_events": sum(len(d.trades) + len(d.quotes) + len(d.depths) for d in self.universe.values()),
                "bars": sum(len(bars) for d in self.universe.values() for bars in d.bars.values()),
                "news": sum(len(d.news) for d in self.universe.values()),
                "consensus": sum(len(d.consensus) for d in self.universe.values()),
                "profile_levels": sum(len(d.profiles) for d in self.universe.values()),
                "tpo_observations": sum(sum(len(prices) for prices in d.tpo.values()) for d in self.universe.values()),
                "source_references": sum(len(c.sources) for c in self.graph.active + self.graph.archive),
                "limits": asdict(self.runtime_policy) if self.runtime_policy else None,
                "evictions": dict(self.runtime_metrics)}
