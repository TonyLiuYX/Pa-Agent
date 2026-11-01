from datetime import timedelta
import unittest
from recognition.config import RecognitionConfig
from recognition.engine import RecognitionEngine
from evaluation.metrics import evaluate, validate_labels
from .support import OPEN, session, event, reference


def full_tape(extra=None):
    return {"coverage": {k: {"complete": True, "start_time": OPEN.isoformat()} for k in ("trades", "classified_trades", "depth")}, **(extra or {})}


class MicroEventTests(unittest.TestCase):
    def test_absorption_and_roll_can_complete_inside_one_minute(self):
        engine = RecognitionEngine(["SET-0003"], config=RecognitionConfig(event_window_seconds=1))
        engine.feed(session(full_tape({"references": {"key_level": reference(100)}})))
        counter = 0
        def send(seconds, price, size, side):
            nonlocal counter
            counter += 1
            return engine.feed(event("trade", OPEN + timedelta(seconds=seconds), {"price": price, "size": size, "side": side}, f"T-{counter}"))
        for t in (0., .3, .6):
            engine.feed(event("depth", OPEN + timedelta(seconds=t), {"mode": "snapshot", "bids": [[100, 1000]], "asks": [[100.01, 1000]]}, f"D-{t}"))
        rows = send(.6, 100, 10, "sell")
        rows += send(1., 100, 10, "sell")
        rows += send(1.2, 100, 100, "sell")
        rows += send(1.6, 100, 100, "sell")
        rows += send(2., 100.03, 100, "buy")
        rows += send(2.4, 100.05, 100, "buy")
        rows += send(2.6, 100.07, 100, "buy")
        rows += send(3., 100.09, 100, "buy")
        rows += send(3.6, 100.12, 100, "buy")
        triggered = [r for r in rows if r.state == "triggered"]
        self.assertTrue(triggered)
        self.assertTrue(all(r.observation_kind == "event_window" for r in triggered))
        self.assertLess((engine.last_frames[("SYN", "SYN")].bar.end - OPEN).total_seconds(), 60)

    def test_declared_coverage_is_required_for_tick_windows(self):
        engine = RecognitionEngine(["SET-0003"])
        engine.feed(session({"references": {"key_level": reference(100)}}))
        rows = []
        for i in range(10):
            rows += engine.feed(event("trade", OPEN + timedelta(seconds=i), {"price": 100, "size": 100, "side": "sell"}, f"T{i}"))
        self.assertFalse(any(r.state in {"formed", "triggered"} for r in rows))

    def test_source_open_uses_received_open_observation(self):
        vp = {"kind": "volume", "venue": "SYN", "poc": 105, "val": 103, "vah": 108, "low": 100, "high": 110, "lower_tail_fraction": .03}
        tp = {"kind": "tpo", "venue": "SYN", "val": 103, "vah": 108, "poc": 105}
        engine = RecognitionEngine(["SET-0020"], include_variants=True)
        engine.feed(session({"references": {k: reference(v) for k, v in {
            "prior_volume_profile": vp, "prior_tpo": tp, "prev_high": 110, "prev_low": 100, "prev_close": 105}.items()}}))
        rows = engine.feed(event("trade", OPEN, {"price": 100.01, "size": 10, "side": "buy"}))
        found = [r for r in rows if r.variant_id == "source_open" and r.state == "triggered"]
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].observation_kind, "open_price")
        self.assertEqual(found[0].anchor["captures"]["price_basis"], "trade")

    def test_news_window_honors_original_publication_and_local_arrival(self):
        from recognition.features import _news_features
        from recognition.data import SymbolData
        data = SymbolData("SYN", "SYN")
        data.update(session())
        old = event("news", OPEN - timedelta(minutes=3), {"event_key": "old", "source": "synthetic", "is_new": True}, arrival=OPEN)
        f = {"signal_risk": 9}
        _news_features(f, 1, [old], data, None, OPEN, None, None)
        self.assertFalse(f["news_fresh60"])
        self.assertFalse(f["news_fresh90"])


def label(case, start=0, end=3, positive=True, **extra):
    return dict(case_id=case, setup_id="SET-0005", variant_id="base", symbol="SYN", venue="SYN", direction=1,
        positive=positive, start=(OPEN + timedelta(minutes=start)).isoformat(), end=(OPEN + timedelta(minutes=end)).isoformat(),
        source_kind="synthetic", source="unit_fixture", split="test", group_id="same-day-event-family", **extra)


def prediction(pid, minute, **extra):
    return dict(detection_id=pid, setup_id="SET-0005", variant_id="base", symbol="SYN", venue="SYN", direction=1,
        state="triggered", data_status="complete", evidence_cutoff=(OPEN + timedelta(minutes=minute)).isoformat(),
        identified_at=(OPEN + timedelta(minutes=minute)).isoformat(), **extra)


class EventEvaluationTests(unittest.TestCase):
    def test_duplicate_predictions_do_not_inflate_recall(self):
        result = evaluate([prediction("A", 1), prediction("B", 2)], [label("one")])
        stats = result["groups"][0]["triggered"]
        self.assertEqual((stats["tp"], stats["fp"], stats["fn"]), (1, 1, 0))
        self.assertEqual(result["groups"][0]["acceptance"], "synthetic_logic_only")

    def test_overlap_matching_is_maximum_and_label_order_independent(self):
        rows = [label("broad", 0, 3), label("narrow", 0, 1)]
        predictions = [prediction("P1", 1), prediction("P2", 3)]
        a = evaluate(predictions, rows)["groups"][0]["triggered"]
        b = evaluate(predictions, rows[::-1])["groups"][0]["triggered"]
        self.assertEqual(a, b)
        self.assertEqual(a["tp"], 2)

    def test_unannotated_is_not_assumed_false_and_disputed_excluded(self):
        result = evaluate([prediction("P", 10)], [label("valid"), label("disputed", disputed=True)])
        stats = result["groups"][0]["triggered"]
        self.assertEqual((stats["fn"], stats["fp"], stats["ignored_unannotated_detections"]), (1, 0, 1))
        self.assertEqual(result["disputed_cases"], ["disputed"])

    def test_group_leakage_rejected_and_unknown_data_not_a_true_positive(self):
        second = label("two", 4, 5)
        second["split"] = "development"
        with self.assertRaises(ValueError):
            validate_labels([label("one"), second])
        p = prediction("P", 1)
        p["data_status"] = "partial"
        self.assertEqual(evaluate([p], [label("one")])["groups"][0]["triggered"]["tp"], 0)


if __name__ == "__main__":
    unittest.main()
