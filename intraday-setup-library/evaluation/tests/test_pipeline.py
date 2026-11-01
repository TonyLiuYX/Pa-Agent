from datetime import timedelta
import json
import tempfile
import unittest
from pathlib import Path

from recognition.engine import RecognitionEngine
from recognition.features import make_frame
from recognition.data import SymbolData
from recognition.models import MarketEvent
from evaluation.demo import create_demo
from .support import OPEN, event, session, bar, reference, history14


class PricePipelineTests(unittest.TestCase):
    def test_raw_hikkake_and_outside_bar_rejection(self):
        for outside, expected in ((False, True), (True, False)):
            engine = RecognitionEngine(["SET-0005"])
            engine.feed(session())
            engine.feed(bar(1, 10, 10.4, 9.6, 10.1))
            engine.feed(bar(2, 10.1, 10.2, 9.8, 9.95))
            engine.feed(bar(3, 9.95, 10.5 if outside else 10.1, 9.5, 9.65))
            rows = engine.feed(bar(4, 9.65, 10.3, 9.6, 10.25))
            self.assertEqual(any(r.state == "triggered" and r.direction == 1 for r in rows), expected)

    def test_hikkake_short_mirror(self):
        engine = RecognitionEngine(["SET-0005"])
        engine.feed(session())
        shapes = [(10, 10.4, 9.6, 10.1), (10.1, 10.2, 9.8, 9.95), (9.95, 10.1, 9.5, 9.65), (9.65, 10.3, 9.6, 10.25)]
        rows = []
        for i, (o, h, l, c) in enumerate(shapes, 1):
            rows += engine.feed(bar(i, 20 - o, 20 - l, 20 - h, 20 - c))
        self.assertTrue(any(r.state == "triggered" and r.direction == -1 for r in rows))

    def test_orb_cannot_use_unfinished_range_or_reverse_direction(self):
        engine = RecognitionEngine(["SET-0002"])
        engine.feed(session({"references": {"atr14": reference(2), "average_daily_volume14": reference(2000000)}}))
        rows = []
        for i in range(1, 5):
            rows += engine.feed(bar(i, 10, 10 + .1 * i, 9.9, 10 + .05 * i))
        self.assertFalse(rows)
        rows += engine.feed(bar(5, 10.2, 10.6, 10.1, 10.5))
        self.assertFalse(any(r.state == "triggered" for r in rows))
        rows += engine.feed(bar(6, 10.5, 10.7, 9, 10.6))
        self.assertTrue(any(r.state == "triggered" and r.direction == 1 for r in rows))
        self.assertFalse(any(r.state == "triggered" and r.direction == -1 for r in rows))

    def test_trend_bar_failure_one_bar_rule_from_raw_data(self):
        engine = RecognitionEngine(["SET-0017"])
        engine.feed(session())
        for i in range(1, 13):
            engine.feed(bar(i, 10 + .1 * (i - 1), 10 + .1 * i + .03, 10 + .1 * (i - 1) - .03, 10 + .1 * i))
        engine.feed(bar(13, 11.2, 11.23, 10.98, 11.0))
        engine.feed(bar(14, 11, 11.1, 10.96, 11.08))
        rows = engine.feed(bar(15, 11.08, 11.2, 11.05, 11.18))
        self.assertTrue(any(r.state == "triggered" and r.direction == 1 for r in rows))

    def test_slow_drift_is_not_strong_drive_first_micro_pullback(self):
        engine = RecognitionEngine(["SET-0009"])
        engine.feed(session({"history": history14("10:00")}))
        rows = []
        for i in range(1, 31):
            rows += engine.feed(bar(i, 10 + .002 * (i - 1), 10 + .002 * i + .05, 10 + .002 * (i - 1) - .05, 10 + .002 * i))
        self.assertFalse(any(r.state == "triggered" for r in rows))

    def test_noise_band_uses_mean_absolute_movement_and_fixed_clock(self):
        engine = RecognitionEngine(["SET-0048"])
        engine.feed(session({"history": history14("10:00", .01)}))
        rows = []
        for i in range(1, 31):
            rows += engine.feed(bar(i, 100 if i == 1 else 102, 103, 99 if i == 1 else 101.9, 102))
        triggered = [r for r in rows if r.state == "triggered"]
        self.assertEqual(len(triggered), 1)
        self.assertEqual(triggered[0].evidence_cutoff, (OPEN + timedelta(minutes=30)).astimezone(triggered_time_zone()).isoformat())
        frame = engine.last_frames[("SYN", "SYN")]
        self.assertAlmostEqual(frame.values[1]["noise_sigma"], .01)
        self.assertAlmostEqual(frame.values[1]["noise_upper"], 101.)

    def test_zero_first_return_is_short_and_half_day_is_skipped(self):
        for half, expected in ((False, True), (True, False)):
            engine = RecognitionEngine(["SET-0049"])
            close = OPEN + timedelta(minutes=210 if half else 390)
            engine.feed(session({"references": {"prev_close": reference(100)}}, close=close))
            rows = []
            for i in range(1, (210 if half else 390) + 1):
                rows += engine.feed(bar(i, 100, 100.1, 99.9, 100))
            triggered = [r for r in rows if r.state == "triggered"]
            self.assertEqual(bool(triggered), expected)
            if expected:
                self.assertEqual(triggered[0].direction, -1)
                self.assertTrue(any(r.state == "completed" for r in rows))

    def test_value_area_acceptance_requires_sixty_actual_internal_closes(self):
        tp = {"kind": "tpo", "venue": "SYN", "val": 100, "vah": 110, "poc": 105}
        engine = RecognitionEngine(["SET-0037"])
        engine.feed(session({"references": {"prior_tpo": reference(tp), "prev_close": reference(105),
                                             "prev_high": reference(111), "prev_low": reference(99)}}))
        rows = []
        for i in range(1, 60):
            rows += engine.feed(bar(i, 99 if i == 1 else 102, 103, 99 if i == 1 else 101, 102))
        self.assertFalse(any(r.state == "triggered" for r in rows))
        rows = engine.feed(bar(60, 102, 103, 101, 102))
        self.assertTrue(any(r.state == "triggered" for r in rows))

    def test_replay_prefix_and_same_input_are_deterministic(self):
        with tempfile.TemporaryDirectory() as d:
            create_demo(d)
            events = [MarketEvent.from_dict(json.loads(l)) for l in (Path(d) / "events.jsonl").read_text().splitlines()]
            def run(items):
                e = RecognitionEngine(["SET-0005", "SET-0048", "SET-0049"])
                return [r.to_dict() for x in items for r in e.feed(x)]
            first = run(events)
            self.assertEqual(first, run(events))
            before = run(events[:25])
            self.assertEqual(before, first[:len(before)])

    def test_missing_depth_does_not_become_orderflow_confirmation(self):
        engine = RecognitionEngine(["SET-0003", "SET-0040", "SET-0046"])
        engine.feed(session())
        rows = []
        for i in range(1, 10):
            rows += engine.feed(bar(i, 10, 10.1, 9.9, 10))
        self.assertFalse(any(r.state in {"formed", "triggered"} for r in rows))
        self.assertIn("depth", engine.coverage()["SET-0003:base"]["last_frame_missing_by_stream_scale"]["SYN|SYN|60|bar"])

    def test_bar_vwap_notional_required_no_typical_price_substitution(self):
        engine = RecognitionEngine(["SET-0023"])
        engine.feed(session())
        engine.feed(bar(1, 10, 10.2, 9.8, 10, notional=False))
        self.assertNotIn("exact_vwap", engine.last_frames[("SYN", "SYN")].capabilities)


def triggered_time_zone():
    from datetime import timezone
    return timezone.utc


class StructuredNewsTests(unittest.TestCase):
    def test_deterministic_macro_branches_and_later_information(self):
        from recognition.features import _news_features
        data = SymbolData("SYN", "SYN")
        data.update(session())
        consensus = event("consensus", OPEN - timedelta(minutes=10), {
            "source": "synthetic", "event_key": "release", "branches": [
                {"name": "up", "conditions": [["x", "gt", 2]], "surprise": True, "direction_by_symbol": {"SYN": 1, "SPY": 1}},
                {"name": "down", "conditions": [["x", "lt", 2]], "surprise": True, "direction_by_symbol": {"SYN": -1, "SPY": -1}}]})
        data.update(consensus)
        first = event("news", OPEN, {"source": "synthetic", "event_key": "release", "values": {"x": 3}, "stage": 1, "is_new": True})
        second = event("news", OPEN + timedelta(minutes=1), {"source": "synthetic", "event_key": "release", "values": {"x": 1}, "stage": 2, "is_new": True})
        data.update(first)
        b = data.update(bar(1, 10, 10.2, 9.8, 10.1))[0][1]
        f = {"signal_risk": 9}
        _news_features(f, 1, [first], data, b, b.end, None, None)
        self.assertTrue(f["macro_surprise"])
        data.update(second)
        f = {"signal_risk": -11}
        _news_features(f, -1, [first, second], data, b, second.arrival_time, None, None)
        self.assertTrue(f["reassessment"])

    def test_stale_news_is_not_relabelled_as_a_second_day_event(self):
        from recognition.features import _news_features
        data = SymbolData("SYN", "SYN")
        data.update(session({"history": history14()}))
        old = event("news", OPEN - timedelta(days=30), {"source": "synthetic", "event_key": "old", "is_new": True}, arrival=OPEN)
        b = data.update(bar(1, 10, 10.1, 9.9, 10))[0][1]
        f = {"signal_risk": 9}
        _news_features(f, 1, [old], data, b, b.end, 10, 10)
        self.assertFalse(f["second_day_news"])


if __name__ == "__main__":
    unittest.main()
