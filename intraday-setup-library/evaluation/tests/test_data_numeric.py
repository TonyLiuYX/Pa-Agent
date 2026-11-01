from datetime import timedelta
import unittest
from recognition.data import SymbolData
from recognition.engine import RecognitionEngine
from recognition.models import MarketEvent, timestamp
from recognition.numeric import SwingTracker, diagonal_zones, value_area
from .support import OPEN, event, session, bar, reference, history14


class InputAndNumericTests(unittest.TestCase):
    def test_rejects_naive_timestamp_nan_and_open_bar(self):
        with self.assertRaises(ValueError):
            timestamp("2026-09-30T09:30:00")
        row = bar(1, 10, 10.1, 9.9, 10).to_dict()
        for key, value in (("close", float("nan")), ("closed", False), ("volume", -1)):
            broken = dict(row, payload=dict(row["payload"], **{key: value}))
            with self.assertRaises(ValueError):
                MarketEvent.from_dict(broken)

    def test_arrival_order_duplicate_and_id_collision(self):
        engine = RecognitionEngine(["SET-0005"])
        engine.feed(session())
        b = bar(1, 10, 10.1, 9.9, 10)
        engine.feed(b)
        self.assertEqual(engine.feed(b), [])
        row = b.to_dict()
        row["payload"]["volume"] = 20000
        row["payload"]["notional"] = 200000
        with self.assertRaises(ValueError):
            engine.feed(MarketEvent.from_dict(row))
        with self.assertRaises(ValueError):
            engine.feed(bar(.5, 10, 10.1, 9.9, 10))

    def test_late_event_marks_gap_without_rewriting_prior_bars(self):
        engine = RecognitionEngine(["SET-0005"])
        engine.feed(session())
        engine.feed(bar(1, 10, 10.1, 9.9, 10))
        before = list(engine.universe[("SYN", "SYN")].bars[60])
        late = event("trade", OPEN + timedelta(seconds=30), {"price": 9.95, "size": 1, "side": "buy"}, arrival=OPEN + timedelta(minutes=2))
        engine.feed(late)
        self.assertEqual(before, engine.universe[("SYN", "SYN")].bars[60])
        self.assertIn("late_market_data", engine.universe[("SYN", "SYN")].bad)

    def test_gap_prevents_confirmation_and_aggregation_is_exact(self):
        data = SymbolData("SYN", "SYN")
        data.update(session())
        for i in (1, 2, 4, 5):
            data.update(bar(i, 10, 10.1, 9.9, 10))
        self.assertIn("bar_gap", data.bad)
        self.assertEqual(data.bars[300][0].quality, "partial")
        data = SymbolData("SYN", "SYN")
        data.update(session())
        for i in range(1, 6):
            data.update(bar(i, 10, 10 + i / 10, 9.9, 10, volume=i * 100))
        five = data.bars[300][0]
        self.assertEqual((five.high, five.volume, five.quality), (10.5, 1500, "complete"))

    def test_future_and_duplicate_historical_sessions_are_rejected(self):
        data = SymbolData("SYN", "SYN")
        h = history14()
        with self.assertRaises(ValueError):
            data.update(session({"history": h + [h[-1]]}))
        with self.assertRaises(ValueError):
            data.update(session({"references": {"prev_close": {"value": 10, "source": "fixture", "as_of": OPEN.isoformat()}}}))

    def test_pivots_never_rewrite_when_volatility_changes(self):
        tracker = SwingTracker(.7)
        data = SymbolData("SYN", "SYN")
        data.update(session())
        for i, price in enumerate((10, 11, 10, 12, 11), 1):
            b = data.update(bar(i, price, price + .1, price - .1, price))[0][1]
            tracker.push(b, .5)
        frozen = list(tracker.points)
        b = data.update(bar(6, 11, 15, 8, 14))[0][1]
        tracker.push(b, 20)
        self.assertEqual(tracker.points[:len(frozen)], frozen)
        self.assertTrue(all(p.extreme_time <= p.confirmed_at for p in frozen))

    def test_value_area_ties_and_diagonal_not_same_price_comparison(self):
        self.assertEqual(value_area({0: 2, 1: 10, 2: 2}, .7), (1, 1, 1))
        self.assertEqual(value_area({0: 2, 1: 10, 2: 2}, .9), (0, 2, 1))
        trades = []
        for k in (100, 101, 102):
            trades += [{"price": k, "size": 40, "side": "buy"}, {"price": k - 1, "size": 10, "side": "sell"}]
        self.assertTrue(any(z["direction"] == 1 and z["levels"] == 3 for z in diagonal_zones(trades, 1)))
        zero = [{"price": k, "size": 100, "side": "buy"} for k in (100, 101, 102)]
        self.assertEqual(diagonal_zones(zero, 1), [])

    def test_data_is_venue_specific(self):
        engine = RecognitionEngine(["SET-0005"])
        engine.feed(session())
        other = session().to_dict()
        other["venue"] = "OTHER"
        other["event_id"] = "other-session"
        engine.feed(MarketEvent.from_dict(other))
        self.assertIsNot(engine.universe[("SYN", "SYN")], engine.universe[("SYN", "OTHER")])


if __name__ == "__main__":
    unittest.main()
