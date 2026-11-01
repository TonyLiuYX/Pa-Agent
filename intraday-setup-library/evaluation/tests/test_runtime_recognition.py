"""Recognition runtime regression tests use actual input events and JSON state."""
from collections import defaultdict, deque
from dataclasses import replace
from datetime import timedelta
import gc
import hashlib
import json
from pathlib import Path
import unittest
import weakref

from recognition.config import RecognitionConfig
from recognition.engine import RecognitionEngine
from recognition.models import MarketEvent, timestamp
from recognition.policy import RuntimePolicy
from .support import OPEN, bar, event, session, reference


class StrictRuntimeTests(unittest.TestCase):
    def test_sequence_and_status_are_strict(self):
        for bad in (True, 1.5, "2", -1, None):
            with self.subTest(sequence=bad), self.assertRaises(ValueError):
                event("quote", OPEN, {"bid": 100, "ask": 100.01, "sequence": bad})
        for payload in ({"halted": 1}, {"gap": "false"}, {"stream": "unknown"}, {"clock": "false"}, {"connected": 1}, {"correction": "true"}, {"connection_state": []}, {"connection_state": " "}):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                event("status", OPEN, payload)
        self.assertEqual(event("status", OPEN, {"halted": False, "gap": True, "stream": "quote"}).kind, "status")

    def test_configuration_rejects_boolean_and_nonfinite(self):
        for name in ("location_noise_fraction", "candidate_slack", "event_window_seconds", "footprint_min_volume"):
            for bad in (True, float("nan"), float("inf")):
                with self.subTest(name=name, bad=bad), self.assertRaises(ValueError):
                    replace(RecognitionConfig(), **{name: bad}).validate()
        with self.assertRaises(ValueError):
            RecognitionConfig(swing_factors=(.3, True, 1.5)).validate()
        with self.assertRaises(ValueError):
            RecognitionConfig(footprint_min_levels=True).validate()

    def test_halt_requires_explicit_venue_resume(self):
        engine = RecognitionEngine(["SET-0005"])
        engine.feed(session())
        engine.feed(event("status", OPEN, {"halted": True, "vendor_status": "H"}))
        for offset,payload in enumerate(({"clock":True}, {"gap":True}, {"correction":True},
                                        {"connected":True,"halted":False},
                                        {"connected":False,"halted":False},
                                        {"connection_state":"connected","halted":False},
                                        {"vendor_status":"Q"}),1):
            engine.feed(event("status", OPEN+timedelta(seconds=offset), payload))
            self.assertTrue(engine.universe[("SYN","SYN")].halted)
        engine.feed(replace(session(op=OPEN+timedelta(days=1)),event_id="halted-next-day"))
        self.assertTrue(engine.universe[("SYN","SYN")].halted)
        engine.feed(event("status", OPEN+timedelta(days=1,seconds=1), {"halted":False,"vendor_status":"T"}))
        self.assertFalse(engine.universe[("SYN","SYN")].halted)

    def test_future_benchmark_does_not_hide_cutoff_history(self):
        engine = RecognitionEngine(["SET-0027"])
        engine.feed(session({"benchmarks": ["B"], "references": {"daily_direction": reference(1)}}, symbol="A"))
        engine.feed(replace(session(symbol="B"), event_id="session-B"))
        for minute in range(1, 8):
            engine.feed(replace(bar(minute, 100, 100.05, 99.85, 100 - minute * .01, symbol="B"), event_id=f"B-{minute}"))
        # Stock bars are received after the benchmark's seventh minute. Its
        # six-minute cutoff nevertheless has six aligned benchmark bars.
        for minute in range(1, 7):
            engine.feed(replace(bar(minute, 100, 100.05, 99.95, 100, symbol="A", arrival=OPEN + timedelta(minutes=7, seconds=minute)), event_id=f"A-{minute}"))
        frame = engine.scale_frames[("A", "SYN", 60, "bar")]
        self.assertIn("synced_market", frame.capabilities)
        self.assertIsNotNone(frame.values[1]["relative_strength"])
        self.assertEqual(frame.bar.end, (OPEN + timedelta(minutes=6)).astimezone(frame.bar.end.tzinfo))

    def test_json_checkpoint_continuation_matches_uninterrupted(self):
        engine = RecognitionEngine(["SET-0010"], runtime_policy=RuntimePolicy(max_seen=20))
        prefix = [session({"references": {"key_level": reference(100.)}})]
        prefix += [bar(i, 99.85, 99.89, 99.80, 99.85) for i in range(1, 6)]
        prefix += [bar(6, 99.85, 100.10, 99.83, 100.06)]
        for row in prefix:
            engine.feed(row)
        state = json.loads(json.dumps(engine.snapshot(), allow_nan=False))
        resumed = RecognitionEngine.restore(state)
        self.assertEqual(resumed.unfinished(), engine.unfinished())
        suffix = [bar(*c) for c in ((7, 100.01, 100.08, 99.99, 100.06), (8, 100.06, 100.16, 100.04, 100.12))]
        left, right = [], []
        for row in suffix:
            left += [r.to_dict() for r in engine.feed(row)]
            right += [r.to_dict() for r in resumed.feed(row)]
        self.assertEqual(left, right)
        self.assertEqual(engine.coverage(), resumed.coverage())
        self.assertEqual(engine.snapshot(), resumed.snapshot())

    def test_checkpoint_rejects_tamper_and_version_mismatch(self):
        engine = RecognitionEngine(["SET-0001"])
        engine.feed(session())
        for key, bad in (("checkpoint_version", 999), ("algorithm_sha256", "changed"), ("rule_version", "changed"), ("state_sha256", "changed")):
            state = engine.snapshot()
            state[key] = bad
            with self.subTest(key=key), self.assertRaises(ValueError):
                RecognitionEngine.restore(state)

    def test_bounded_runtime_reports_evictions_and_preserves_totals(self):
        policy = RuntimePolicy(max_seen=5, max_transitions=1, max_stream_events=2)
        engine = RecognitionEngine(["SET-0005"], runtime_policy=policy)
        engine.feed(session())
        for candle in ((1, 100, 101, 99, 100), (2, 100, 100.8, 99.2, 100), (3, 100, 100.5, 99.1, 99.5), (4, 99.5, 100.9, 99.2, 100.8), (5, 100.8, 101.5, 100.5, 101.4), (6, 101.4, 101.5, 99, 99.5)):
            engine.feed(bar(*candle))
        count = engine.coverage()["SET-0005:base"]["transition_count"]
        self.assertGreater(count, len(engine.graph.transitions))
        self.assertLessEqual(len(engine.seen), 5)
        self.assertLessEqual(len(engine.graph.transitions), 1)
        self.assertGreater(engine.runtime_metrics["seen_cache_evictions"], 0)
        for second in range(1, 4):
            engine.feed(event("quote", OPEN + timedelta(minutes=9, seconds=second), {"bid": 100, "ask": 100.01}))
        data = engine.universe[("SYN", "SYN")]
        self.assertEqual(len(data.quotes), 2)
        self.assertIn("runtime_cache_truncated_quotes", data.bad)
        self.assertEqual(engine.runtime_metrics["stream_cache_evictions"], 1)

    def test_bar_capacity_keeps_actual_open_and_downgrades_whole_session(self):
        engine = RecognitionEngine(["SET-0048"], runtime_policy=RuntimePolicy(max_bars=32))
        engine.feed(session())
        for minute in range(1, 36):
            opening = 100 + (minute - 1) * .1
            engine.feed(bar(minute, opening, opening + .15, opening - .02, opening + .1))
        data = engine.universe[("SYN", "SYN")]
        frame = engine.scale_frames[("SYN", "SYN", 60, "bar")]
        self.assertEqual(data.cash_open_bar_value, 100.)
        self.assertEqual(frame.values[1]["cash_open"], 100.)
        self.assertNotIn("exact_vwap", frame.capabilities)
        self.assertIn(60, data.truncated_scales)
        self.assertLessEqual(len(data.delta_samples[60]), 32)

    def test_active_capacity_stops_without_pruning_an_episode(self):
        from recognition.policy import CapacityExceeded
        engine = RecognitionEngine(["SET-0010"], runtime_policy=RuntimePolicy(max_active=1))
        engine.feed(session({"references": {"key_level": reference(100.), "prev_low": reference(99.9)}}))
        for minute in range(1, 6):
            engine.feed(bar(minute, 99.85, 99.89, 99.8, 99.85))
        with self.assertRaises(CapacityExceeded):
            engine.feed(bar(6, 99.85, 100.1, 99.83, 100.06))
        self.assertEqual(len(engine.graph.active), 1)
        self.assertEqual(engine.graph.pruned, [])

    def test_next_calendar_does_not_backdate_completion_receipt(self):
        engine = RecognitionEngine(["SET-0005"])
        engine.feed(session())
        for row in ((1,10,10.4,9.6,10.1),(2,10.1,10.2,9.8,9.95),(3,9.95,10.1,9.5,9.65),(4,9.65,10.3,9.6,10.25)):
            engine.feed(bar(*row))
        next_day = replace(session(op=OPEN+timedelta(days=1)),event_id="next-session")
        rows = engine.feed(next_day)
        self.assertTrue(rows)
        self.assertTrue(all(timestamp(r.identified_at) == next_day.arrival_time for r in rows))
        self.assertTrue(all("quality:incomplete_prior_session" in r.missing for r in rows))

    def test_complete_session_history_survives_json_and_calendar_reset(self):
        engine = RecognitionEngine(["SET-0005"])
        engine.feed(session(close=OPEN+timedelta(minutes=2)))
        engine.feed(bar(1,100,101,99,100.5))
        engine.feed(bar(2,100.5,102,100,101.5))
        next_day = replace(session(op=OPEN+timedelta(days=1)),event_id="next-session")
        engine.feed(next_day)
        resumed = RecognitionEngine.restore(json.loads(json.dumps(engine.snapshot())))
        data=resumed.universe[("SYN","SYN")]
        self.assertEqual(len(data.history),1)
        self.assertEqual(data.reference("prev_close"),101.5)
        self.assertEqual(data.reference("prev_high"),102.)
        self.assertFalse(data.bars[60])

    def test_many_sessions_bound_archives_and_clear_candidate_indexes(self):
        policy=RuntimePolicy(max_seen=10,max_archive=8,max_transitions=6,max_audit=5)
        engine=RecognitionEngine(["SET-0005"],include_variants=True,runtime_policy=policy)
        candles=((10,10.4,9.6,10.1),(10.1,10.2,9.8,9.95),(9.95,10.1,9.5,9.65),(9.65,10.3,9.6,10.25),(10.25,10.3,10.2,10.25))
        counts=[]
        for day in range(24):
            op=OPEN+timedelta(days=day)
            engine.feed(session(op=op,close=op+timedelta(minutes=5)))
            for minute,candle in enumerate(candles,1):
                engine.feed(bar(minute,*candle,op=op))
            usage=engine.resource_usage()
            counts.append(usage)
            self.assertEqual((usage['active'],usage['edges'],usage['reentries']),(0,0,0))
            self.assertLessEqual(usage['archive'],8)
            self.assertLessEqual(usage['transitions'],6)
            self.assertLessEqual(usage['seen'],10)
        # Filled caches plateau while cumulative coverage remains exact.
        self.assertEqual([r['archive'] for r in counts[-8:]],[8]*8)
        self.assertEqual(len(engine.graph.coverage_stats),2)
        self.assertEqual(engine.coverage()['SET-0005:base']['triggered'],24)
        resumed=RecognitionEngine.restore(json.loads(json.dumps(engine.snapshot())))
        self.assertEqual(resumed.resource_usage(),engine.resource_usage())
        self.assertEqual(resumed.coverage(),engine.coverage())

    def test_premarket_cache_truncation_removes_overnight_capability(self):
        engine=RecognitionEngine(["SET-0003"],runtime_policy=RuntimePolicy(max_bars=32))
        engine.feed(session({'premarket_complete':True}))
        for minute in range(-34,2):
            engine.feed(bar(minute,100,100.1,99.9,100))
        data=engine.universe[("SYN","SYN")]
        self.assertTrue(data.premarket_truncated)
        self.assertNotIn('premarket',engine.last_frames[("SYN","SYN")].capabilities)

    def test_cumulative_tpo_capacity_preserves_evidence_and_stops(self):
        from recognition.policy import CapacityExceeded
        engine=RecognitionEngine(["SET-0005"],runtime_policy=RuntimePolicy(max_session_records=1))
        engine.feed(session())
        engine.feed(event('trade',OPEN+timedelta(seconds=1),{'price':100.,'size':1.,'side':'buy'}))
        with self.assertRaises(CapacityExceeded):
            engine.feed(event('trade',OPEN+timedelta(seconds=1801),{'price':100.,'size':1.,'side':'buy'}))
        data=engine.universe[("SYN","SYN")]
        self.assertEqual(len(data.profiles),1)
        self.assertEqual(sum(len(prices) for prices in data.tpo.values()),2)

    def test_evidence_budget_applies_before_same_event_trigger_output(self):
        engine=RecognitionEngine(["SET-0049"],runtime_policy=RuntimePolicy(max_evidence_sources=3))
        engine.feed(session({'references':{'prev_close':reference(100.)}}))
        rows=[]
        for minute in range(1,361):
            rows=engine.feed(bar(minute,100.,100.1,99.9,100.))
        triggered=[row for row in rows if row.state=='triggered']
        self.assertTrue(triggered)
        for row in rows:
            self.assertLessEqual(len(row.anchor['source_ids']),3)
            self.assertTrue(all(len(condition['source_ids'])<=3 for condition in row.conditions))
            self.assertEqual(row.data_status,'partial')
            self.assertIn('evidence_source_examples_truncated',row.missing)
        self.assertTrue(all(len(condition['source_ids'])<=3 for candidate in engine.graph.active
                            for condition in candidate.conditions))
        self.assertGreater(engine.runtime_metrics['evidence_truncations'],0)
        # Canonical storage still preserves dictionary insertion order encoded
        # as item arrays and the bounded examples in subsequent state.
        restored=RecognitionEngine.restore(json.loads(json.dumps(engine.snapshot(),sort_keys=True)))
        self.assertEqual(restored.snapshot(),engine.snapshot())


class CheckpointCodecCompatibilityTests(unittest.TestCase):
    def test_frozen_v1_bytes_and_shared_encoding(self):
        from recognition.checkpoint import CHECKPOINT_VERSION, decode, digest, encode

        shared = {"source_ids": ["shared-evidence-0001", "shared-evidence-0001"], "counts": deque([1, 2])}
        candle = bar(1, 99.85, 99.89, 99.8, 99.85)
        original = {"second": shared, "first": shared,
                    "defaults": defaultdict(list, {"entry": ["shared-evidence-0001"]}),
                    "tuple": (OPEN, False, 4.5), "set": {"z", "a"}, "event": candle}
        encoded = encode(original)
        self.assertEqual(CHECKPOINT_VERSION, 1)
        # Captured using the frozen v1 codec before this optimization. This
        # oracle covers insertion order, type tags, dates, and unordered sets.
        self.assertEqual(digest(encoded), "05b21a2dc312a1dcc2abf4a61ea8840b30c5f0a35961ddf0f3458c74ba78d7f3")
        canonical = json.dumps(encoded, sort_keys=True, separators=(",", ":"), allow_nan=False)
        self.assertEqual(len(canonical.encode()), 1017)
        items = dict(encoded["items"])
        self.assertIs(items["second"], items["first"])
        decoded = decode(json.loads(canonical))
        self.assertEqual(decoded, original)
        self.assertEqual(decoded["event"].to_dict(), candle.to_dict())
        decoded["event"].validate()
        self.assertEqual(list(decoded), list(original))
        # The existing datetime decoder normalizes the fixture's offset to UTC.
        # Its separately frozen round-trip digest preserves that behavior too.
        self.assertEqual(digest(encode(decoded)), "f61078058c9632171364fd81e4559ce7a198cb623b1fe3978a661cb0fa8b213b")

    def test_frozen_raw_prefix_restores_and_identifies_same_suffix(self):
        from recognition.checkpoint import decode, digest, encode
        from recognition.provenance import algorithm_sha256
        from recognition.specs import RULE_VERSION, load_specs

        frozen_bytes = (Path(__file__).parent / "fixtures" / "checkpoint_v1.json").read_bytes().rstrip(b"\n")
        self.assertEqual(hashlib.sha256(frozen_bytes).hexdigest(),
                         "dfcaf1c42a70a2e36db05ae8e51ca32586c89c2ea720384704fc2b79f1881a14")
        frozen = json.loads(frozen_bytes)
        self.assertEqual(frozen["state_sha256"], "40e979694139716f53dd776b60b05d0f9a1e60ecd0f6f1036381309d9d680886")
        state_before = json.dumps(frozen["state"], sort_keys=True, separators=(",", ":"))
        restored_state = encode(decode(frozen["state"]))
        self.assertEqual(json.dumps(restored_state, sort_keys=True, separators=(",", ":")), state_before)
        self.assertEqual(digest(restored_state), frozen["state_sha256"])
        # Production must still reject code-version changes. Only this codec
        # compatibility experiment rebinds outer algorithm/rule identities; the
        # original state bytes and integrity hashes remain unchanged.
        with self.assertRaisesRegex(ValueError, "algorithm/rule version mismatch"):
            RecognitionEngine.restore(frozen)
        current_specs = load_specs(["SET-0010"])
        # This isolated fixture has no historical-calendar data. Its rule graph
        # must still be identical apart from the release's version metadata.
        self.assertEqual([{k:v for k,v in s.items() if k!="rule_version"} for s in frozen["specs"]],
                         [{k:v for k,v in s.items() if k!="rule_version"} for s in current_specs])
        frozen["algorithm_sha256"] = algorithm_sha256()
        frozen["rule_version"] = RULE_VERSION
        frozen["specs"] = current_specs
        frozen["specs_sha256"] = digest(current_specs)
        resumed = RecognitionEngine.restore(frozen)
        uninterrupted = RecognitionEngine(["SET-0010"])
        uninterrupted.feed(session({"references": {"key_level": reference(100.)}}))
        self.assertEqual(uninterrupted.snapshot(), frozen)
        suffix = [bar(i, 99.85, 99.89, 99.80, 99.85) for i in range(1, 6)]
        suffix += [bar(*candle) for candle in ((6, 99.85, 100.10, 99.83, 100.06),
                                             (7, 100.01, 100.08, 99.99, 100.06),
                                             (8, 100.06, 100.16, 100.04, 100.12))]
        left, right = [], []
        for candle in suffix:
            left.extend(row.to_dict() for row in uninterrupted.feed(candle))
            right.extend(row.to_dict() for row in resumed.feed(candle))
        self.assertTrue(any(row["setup_id"] == "SET-0010" and row["state"] == "triggered" for row in left))
        self.assertEqual(left, right)
        self.assertEqual(uninterrupted.coverage(), resumed.coverage())
        self.assertEqual(uninterrupted.snapshot(), resumed.snapshot())

    def test_decode_pools_evidence_strings_without_sharing_mutable_state(self):
        from recognition.checkpoint import decode, encode

        source_id = "source-evidence-" + "0123456789abcdef" * 8
        shared = {"source_ids": [source_id], "nested": {"sources": [source_id]}}
        encoded = encode({"anchor": shared, "condition": shared})
        first_wire = json.loads(json.dumps(encoded))
        first = decode(first_wire)
        second_wire = json.loads(json.dumps(encoded))
        second = decode(second_wire)
        source = first["anchor"]["source_ids"][0]
        self.assertIs(source, first["condition"]["source_ids"][0])
        self.assertIs(source, first["condition"]["nested"]["sources"][0])
        self.assertIsNot(source, second["anchor"]["source_ids"][0])
        self.assertIsNot(first["anchor"], first["condition"])
        self.assertIsNot(first["anchor"]["source_ids"], first["condition"]["source_ids"])
        self.assertIsNot(first["anchor"]["nested"], first["condition"]["nested"])
        self.assertIsNot(first["anchor"]["nested"]["sources"], first["condition"]["nested"]["sources"])
        first["anchor"]["source_ids"].append("new-evidence")
        first["anchor"]["nested"]["sources"].clear()
        self.assertEqual(first["condition"], shared)
        self.assertEqual(second["anchor"], shared)
        # The same encoded object can be visited twice directly too; the pool
        # must never turn its aliases into shared mutable restored containers.
        direct = decode(encoded)
        self.assertIsNot(direct["anchor"], direct["condition"])
        self.assertIs(direct["anchor"]["source_ids"][0], direct["condition"]["source_ids"][0])

    def test_encode_memo_expires_without_waiting_for_garbage_collection(self):
        from recognition.checkpoint import encode

        enabled = gc.isenabled()
        gc.disable()
        try:
            candle = bar(1, 99.85, 99.89, 99.8, 99.85)
            reference_to_input = weakref.ref(candle)
            encoded = encode({"left": candle, "right": candle})
            del candle
            self.assertIsNone(reference_to_input())
            shared = ["original"]
            first = encode([shared, shared])
            shared.append("changed")
            second = encode([shared, shared])
            self.assertEqual(first, [["original"], ["original"]])
            self.assertEqual(second, [["original", "changed"], ["original", "changed"]])
            self.assertIs(first[0], first[1])
            self.assertIs(second[0], second[1])
            self.assertIsNot(first[0], second[0])
            self.assertIsNotNone(encoded)
        finally:
            if enabled:
                gc.enable()

    def test_cycles_and_invalid_types_are_rejected(self):
        from recognition.checkpoint import decode, encode

        cyclic_list = []
        cyclic_list.append(cyclic_list)
        cyclic_dict = {}
        cyclic_dict["self"] = cyclic_dict
        cyclic_wire = {"$type": "dict", "items": []}
        cyclic_wire["items"].append(["self", cyclic_wire])
        for value in (cyclic_list, cyclic_dict):
            with self.subTest(encode=type(value).__name__), self.assertRaisesRegex(ValueError, "circular"):
                encode(value)
        for value in (cyclic_list, cyclic_wire):
            with self.subTest(decode=type(value).__name__), self.assertRaisesRegex(ValueError, "circular"):
                decode(value)
        for value in (object(), float("nan"), float("inf"), defaultdict(dict)):
            with self.subTest(encode=value), self.assertRaises(ValueError):
                encode(value)
        for value in ({}, {"$type": "UnknownClass", "fields": {}},
                      {"$type": "MarketEvent", "fields": {}}, float("nan"), float("inf")):
            with self.subTest(decode=value), self.assertRaises(ValueError):
                decode(value)
