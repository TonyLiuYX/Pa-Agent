from datetime import timedelta
import unittest
from recognition.engine import RecognitionEngine
from recognition.features import make_frame
from .support import OPEN, event, session, reference, bar
from .test_micro_metrics import full_tape


class ContextRegressionTests(unittest.TestCase):
    def test_multiple_key_levels_keep_independent_episodes(self):
        engine = RecognitionEngine(["SET-0010"])
        engine.feed(session({"references": {"key_level": reference(100.), "prev_low": reference(99.90)}}))
        for i in range(1, 6):
            engine.feed(bar(i, 99.85, 99.89, 99.80, 99.85))
        rows = engine.feed(bar(6, 99.85, 100.10, 99.83, 100.06))
        first = [r for r in rows if r.scale_seconds == 60 and r.direction == 1]
        self.assertEqual({r.anchor["captures"]["level"] for r in first}, {99.9, 100.})
        self.assertEqual(len({r.detection_id for r in first}), 2)
        for candle in ((7, 100.01, 100.08, 99.99, 100.06), (8, 100.06, 100.16, 100.04, 100.12),
                       (9, 99.90, 99.96, 99.89, 99.95), (10, 99.95, 100.20, 99.94, 100.14)):
            rows += engine.feed(bar(*candle))
        detected = [r for r in rows if r.state == "triggered" and r.scale_seconds == 60]
        self.assertEqual({r.anchor["captures"]["level"] for r in detected}, {99.9, 100.})

    def test_delayed_closed_bar_cannot_use_later_cvd_profile_or_vwap(self):
        engine = RecognitionEngine(["SET-0044"])
        engine.feed(session(full_tape()))
        engine.feed(event("trade", OPEN + timedelta(seconds=30), {"price": 100, "size": 10, "side": "buy"}, "before"))
        engine.feed(event("trade", OPEN + timedelta(seconds=90), {"price": 110, "size": 1000, "side": "sell"}, "later"))
        late = bar(1, 100, 101, 99, 100, arrival=OPEN + timedelta(seconds=95))
        engine.feed(late)
        data = engine.universe[("SYN", "SYN")]
        sample_bar, sample_cvd = data.delta_samples[60][-1]
        self.assertEqual(sample_cvd, 10)
        self.assertEqual(data.profile_samples[-1][1], (100., 100., 100.))
        f = make_frame(data, 60, sample_bar, late.arrival_time, engine.universe)
        self.assertEqual(f.values[1]["vwap"], 100.)
        self.assertNotIn("later", f.sources)
        self.assertEqual(data.cvd, -990)  # replay state still contains the later trade

    def test_rejected_late_tick_does_not_generate_a_new_frame(self):
        engine = RecognitionEngine(["SET-0044"])
        engine.feed(session(full_tape()))
        engine.feed(event("trade", OPEN + timedelta(seconds=30), {"price": 100, "size": 10, "side": "buy"}, "T30"))
        engine.feed(bar(1, 100, 101, 99, 100))
        previous = engine.last_frames[("SYN", "SYN")]
        rows = engine.feed(event("trade", OPEN + timedelta(seconds=45), {"price": 90, "size": 1000, "side": "sell"}, "late", arrival=OPEN + timedelta(seconds=65)))
        self.assertEqual(rows, [])
        self.assertIs(engine.last_frames[("SYN", "SYN")], previous)
        self.assertIn("late_market_data", engine.universe[("SYN", "SYN")].bad)

    def test_eof_preserves_unfinished_detection_instead_of_inventing_close(self):
        engine = RecognitionEngine(["SET-0005"])
        engine.feed(session())
        for candle in ((1, 100, 101, 99, 100), (2, 100, 100.8, 99.2, 100), (3, 100, 100.5, 99.1, 99.5)):
            engine.feed(bar(*candle))
        active = engine.unfinished()
        self.assertTrue(active)
        self.assertTrue(all(r["reason"] == "input_ended_before_session_close" for r in active))
        self.assertFalse(any(r.state in {"completed", "expired"} for r in engine.graph.transitions))

    def test_prior_profile_from_other_venue_is_unavailable(self):
        tp = {"kind": "tpo", "venue": "OTHER", "val": 99, "vah": 101, "poc": 100}
        vp = {"kind": "volume", "venue": "OTHER", "val": 99, "vah": 101, "poc": 100, "low": 98, "high": 102,
              "lower_tail_fraction": .01, "upper_tail_fraction": .01}
        engine = RecognitionEngine(["SET-0020", "SET-0037"], include_variants=True)
        engine.feed(session({"references": {"prior_tpo": reference(tp), "prior_volume_profile": reference(vp)}}))
        rows = engine.feed(event("trade", OPEN, {"price": 98.01, "size": 10, "side": "buy"}))
        self.assertFalse(any(r.state == "triggered" for r in rows))
        engine.feed(bar(1, 98.01, 100.1, 98.01, 100.))
        f = engine.last_frames[("SYN", "SYN")]
        self.assertNotIn("prior_tpo", f.capabilities)
        self.assertNotIn("prior_volume_profile", f.capabilities)
