"""Raw session budgets reject retained evidence before any engine mutation."""
from dataclasses import replace
from datetime import timedelta
import unittest
from unittest.mock import patch

from recognition.engine import RecognitionEngine
from recognition.models import MarketEvent
from recognition.policy import CapacityExceeded, RuntimePolicy
from .support import OPEN, bar, reference, session


def history_rows(count, op=OPEN + timedelta(days=1)):
    return [{"as_of": (op - timedelta(days=i) + timedelta(minutes=390)).isoformat(),
             "cash_open": 100., "source": "synthetic_capacity_fixture"}
            for i in range(count, 0, -1)]


class SessionCapacityTests(unittest.TestCase):
    def seeded_engine(self):
        engine = RecognitionEngine(["SET-0010"], runtime_policy=RuntimePolicy(max_session_records=32))
        engine.feed(session({"references": {"key_level": reference(100.)}}))
        for minute in range(1, 6):
            engine.feed(bar(minute, 99.85, 99.89, 99.80, 99.85))
        engine.feed(bar(6, 99.85, 100.10, 99.83, 100.06))
        self.assertTrue(engine.graph.active)
        return engine

    def next_session(self, extra):
        return replace(session(extra, op=OPEN + timedelta(days=1)), event_id="capacity-next-session")

    def assert_atomic_rejection(self, event, reason, exception=CapacityExceeded):
        engine = self.seeded_engine()
        before = engine.snapshot_json()
        original = engine.universe[("SYN", "SYN")]
        first = original.bars[60][0]
        # A rejected session must never reach the deep copy/signature phase.
        with patch.object(MarketEvent, "to_dict", side_effect=AssertionError("serialized before preflight")):
            with self.assertRaisesRegex(exception, reason):
                engine.feed(event)
        self.assertEqual(engine.snapshot_json(), before)
        self.assertIs(engine.universe[("SYN", "SYN")], original)
        self.assertIs(original.bars[60][0], first)
        self.assertNotIn(event.event_id, engine.seen)

    def test_history_calendar_references_and_metadata_use_one_complete_budget(self):
        row = history_rows(1)[0]
        calendar = [{"date": (OPEN - timedelta(days=i)).date().isoformat(),
                     "open_time": (OPEN - timedelta(days=i)).isoformat(),
                     "close_time": (OPEN - timedelta(days=i) + timedelta(minutes=390)).isoformat(),
                     "source": "synthetic_calendar"} for i in range(9, 0, -1)]
        cases = {"history_rows": {"history": history_rows(33)},
                 "history_closes": {"history": [{**row, "closes": {f"09:{i:02}": 100. for i in range(40)}}]},
                 "history_volumes": {"history": [{**row, "volumes": {f"09:{i:02}": 100. for i in range(40)}}]},
                 "calendar": {"history_calendar": calendar},
                 "references": {"references": {f"level{i}": reference(100.) for i in range(9)}},
                 "metadata": {"metadata": {"observations": list(range(40))}}}
        for name, extra in cases.items():
            with self.subTest(case=name):
                self.assert_atomic_rejection(self.next_session(extra), "session_payload_records_exceeded")

    def test_exact_32_record_boundary_is_accepted_without_trimming(self):
        raw = self.next_session({"metadata": {"observations": list(range(30))}})
        policy = RuntimePolicy(max_session_records=32)
        self.assertEqual(policy.preflight_session(raw.payload)["records"], 32)
        engine = self.seeded_engine()
        engine.feed(raw)
        self.assertEqual(engine.universe[("SYN", "SYN")].session["metadata"]["observations"], list(range(30)))
        self.assertIn(raw.event_id, engine.seen)

    def test_scalar_storage_counts_utf8_and_fixed_timestamp_headers(self):
        for text in ("a" * 9000, "中" * 3000):
            with self.subTest(text_kind=text[0]):
                self.assert_atomic_rejection(self.next_session({"metadata": text}),
                                             "session_payload_scalar_storage_exceeded")
        op = (OPEN + timedelta(days=1)).isoformat()
        huge_valid_time = op[:-6] + "." + "0" * 10000 + op[-6:]
        self.assert_atomic_rejection(self.next_session({"open_time": huge_valid_time}),
                                     "session_payload_scalar_storage_exceeded")

    def test_minimal_capacity_one_preserves_existing_session_and_tpo_contract(self):
        policy = RuntimePolicy(max_session_records=1)
        usage = policy.preflight_session(session().payload)
        self.assertEqual(usage["records"], 0)
        self.assertLessEqual(usage["scalar_bytes"], 256)
        engine = RecognitionEngine(["SET-0005"], runtime_policy=policy)
        engine.feed(session())
        self.assertEqual(engine.universe[("SYN", "SYN")].session["tick_size"], .01)

    def test_runtime_default_and_offline_keep_original_and_derived_history_contract(self):
        raw = self.next_session({"history": history_rows(31)})
        self.assertEqual(RuntimePolicy().preflight_session(raw.payload)["records"], 125)
        for policy in (None, RuntimePolicy()):
            with self.subTest(runtime_policy=policy is not None):
                engine = RecognitionEngine(["SET-0005"], runtime_policy=policy)
                engine.feed(raw)
                data = engine.universe[("SYN", "SYN")]
                self.assertEqual(len(data.history), 30)
                self.assertEqual(len(data.session["history"]), 31)
        offline = RecognitionEngine(["SET-0005"])
        offline.feed(self.next_session({"metadata": "a" * 13000000}))
        self.assertEqual(len(offline.universe[("SYN", "SYN")].session["metadata"]), 13000000)

    def test_cycles_invalid_values_and_deep_over_budget_inputs_are_atomic(self):
        cycle = []
        cycle.append(cycle)
        for value, reason in ((cycle, "cycle"), (float("nan"), "nonfinite"),
                              (float("inf"), "nonfinite"), (object(), "JSON-compatible"),
                              ({3: "invalid_key"}, "keys must be strings"),
                              ({None: "invalid_key"}, "keys must be strings")):
            with self.subTest(reason=reason):
                self.assert_atomic_rejection(self.next_session({"metadata": value}), reason, ValueError)
        nested = []
        for _ in range(2000):
            nested = [nested]
        self.assert_atomic_rejection(self.next_session({"metadata": nested}), "session_payload_records_exceeded")

    def test_shared_input_is_charged_at_each_occurrence(self):
        shared = [1, 2, 3]
        raw = session({"metadata": {"left": shared, "right": shared}})
        usage = RuntimePolicy(max_session_records=9).preflight_session(raw.payload)
        self.assertEqual(usage["records"], 9)
        with self.assertRaisesRegex(CapacityExceeded, "session_payload_records_exceeded"):
            RuntimePolicy(max_session_records=8).preflight_session(raw.payload)

    def test_default_demo_session_fits_without_altering_its_source_evidence(self):
        from connectors.sources import DemoProvider
        raw = next(DemoProvider(["SPY"], minutes=390).events())
        self.assertEqual(raw.kind, "session")
        usage = RuntimePolicy().preflight_session(raw.payload)
        self.assertLess(usage["records"], RuntimePolicy().max_session_records)
        engine = RecognitionEngine(["SET-0005"], runtime_policy=RuntimePolicy())
        engine.feed(raw)
        self.assertEqual(engine.universe[(raw.symbol, raw.venue)].session["history"], raw.payload["history"])
