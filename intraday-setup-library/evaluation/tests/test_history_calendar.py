"""Calendar-aware history must not replace a missing session with an older day."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from zoneinfo import ZoneInfo

from recognition.data import SymbolData
from recognition.engine import RecognitionEngine
from recognition.features import make_frame
from recognition.models import MarketEvent

NY = ZoneInfo("America/New_York")
OPEN = datetime(2026, 9, 30, 9, 30, tzinfo=NY)


def historical_input(count=15):
    days = []
    day = OPEN - timedelta(days=1)
    while len(days) < count:
        if day.weekday() < 5:
            days.append(day)
        day -= timedelta(days=1)
    calendar, history = [], []
    for index, day in enumerate(reversed(days)):
        close = day.replace(hour=16, minute=0)
        calendar.append({"date": day.date().isoformat(), "open_time": day.isoformat(),
                         "close_time": close.isoformat(), "source": "authored_calendar"})
        history.append({"date": day.date().isoformat(), "as_of": close.isoformat(), "source": "authored_history",
                        "complete": True, "cash_open": 100, "close": 100 + index / 100,
                        "high": 101, "low": 99, "volume": 100,
                        "closes": {"09:31": 100 + index / 100}, "volumes": {"09:31": 10}})
    return calendar, history


def session(history, calendar=None, *, at=OPEN, identifier="calendar-session", references=None):
    payload = {"open_time": at.isoformat(), "close_time": (at + timedelta(minutes=390)).isoformat(),
               "history": history, "references": references or {}}
    if calendar is not None:
        payload["history_calendar"] = calendar
    return MarketEvent.from_dict({"event_id": identifier, "symbol": "SPY", "venue": "IEX", "kind": "session",
                                  "event_time": at.isoformat(), "arrival_time": at.isoformat(), "payload": payload})


def first_frame(data):
    at = OPEN + timedelta(minutes=1)
    e = MarketEvent.from_dict({"event_id": "first-bar", "symbol": "SPY", "venue": "IEX", "kind": "bar",
        "event_time": at.isoformat(), "arrival_time": at.isoformat(),
        "payload": {"open": 100, "high": 101, "low": 99, "close": 100, "volume": 10, "seconds": 60}})
    data.update(e)
    return make_frame(data, 60, data.bars[60][-1], e.arrival_time, {(data.symbol, data.venue): data})


class CalendarHistoryTests(unittest.TestCase):
    def test_missing_previous_session_does_not_reuse_older_references_or_fallback(self):
        cal, history = historical_input()
        stale = {"value": 777, "as_of": history[-2]["as_of"], "source": "old_reference"}
        data = SymbolData("SPY", "IEX")
        data.update(session(history[:-1], cal, references={"prev_close": stale}))
        self.assertEqual(len(data.history), 14)  # Valid older evidence is retained.
        self.assertEqual(data.complete_history(), [])
        for name in ("prev_high", "prev_low", "prev_close", "atr14", "average_daily_volume14", "daily_direction"):
            self.assertIsNone(data.reference(name), name)
        frame = first_frame(data)
        self.assertFalse({"prior_session", "previous_close", "daily_atr14", "liquidity_history", "same_time14"} & frame.capabilities)

    def test_middle_missing_day_keeps_true_prior_but_blocks_long_window(self):
        cal, history = historical_input(16)
        history.pop(8)
        data = SymbolData("SPY", "IEX")
        data.update(session(history, cal))
        self.assertEqual(len(data.history), 15)
        self.assertEqual([h["date"] for h in data.complete_history()], [h["date"] for h in history[-7:]])
        self.assertEqual(data.reference("prev_close"), history[-1]["close"])
        self.assertIsNone(data.reference("atr14"))
        self.assertIsNone(data.reference("average_daily_volume14"))
        frame = first_frame(data)
        self.assertTrue({"prior_session", "previous_close", "volume_baseline"} <= frame.capabilities)
        self.assertFalse({"daily_atr14", "liquidity_history", "same_time14"} & frame.capabilities)

    def test_complete_calendar_history_preserves_valid_capabilities(self):
        cal, history = historical_input()
        data = SymbolData("SPY", "IEX")
        data.update(session(history, cal, references={"atr14": {"value": 777, "as_of": history[-1]["as_of"], "source": "unverified_override"}}))
        self.assertEqual(data.reference("atr14"), 2)
        self.assertEqual(data.reference("average_daily_volume14"), 100)
        self.assertEqual(data.reference("prev_close"), history[-1]["close"])
        self.assertEqual(data.reference("daily_direction"), 1)
        self.assertTrue({"prior_session", "previous_close", "daily_atr14", "liquidity_history", "same_time14"} <= first_frame(data).capabilities)

    def test_missing_oldest_day_preserves_the_complete_14_day_window(self):
        cal, history = historical_input()
        data = SymbolData("SPY", "IEX")
        data.update(session(history[1:], cal))
        self.assertEqual(len(data.complete_history(14)), 14)
        self.assertIsNone(data.reference("atr14"))  # True range needs 15 closes.
        self.assertEqual(data.reference("average_daily_volume14"), 100)
        frame = first_frame(data)
        self.assertTrue({"prior_session", "previous_close", "liquidity_history", "same_time14"} <= frame.capabilities)
        self.assertNotIn("daily_atr14", frame.capabilities)

    def test_unproven_complete_marker_cannot_upgrade_declared_calendar(self):
        cal, history = historical_input(1)
        for marker in (None, False):
            with self.subTest(marker=marker):
                candidate = deepcopy(history)
                if marker is None:
                    candidate[0].pop("complete")
                else:
                    candidate[0]["complete"] = marker
                data = SymbolData("SPY", "IEX")
                data.update(session(candidate, cal))
                self.assertIsNone(data.reference("prev_close"))
        with self.assertRaises(ValueError):
            SymbolData("SPY", "IEX").update(session([{**history[0], "complete": 1}], cal))

    def test_explicit_empty_history_clears_previous_state(self):
        _, history = historical_input(1)
        data = SymbolData("SPY", "IEX")
        data.update(session(history))
        data.update(session([], at=OPEN + timedelta(days=1), identifier="new-empty-history"))
        self.assertEqual(data.history, [])
        self.assertIsNone(data.reference("prev_close"))

    def test_omitted_history_retains_available_causal_history(self):
        _, history = historical_input(1)
        data = SymbolData("SPY", "IEX")
        data.update(session(history))
        e = session([], at=OPEN + timedelta(days=1), identifier="new-omitted-history")
        payload = dict(e.payload)
        payload.pop("history")
        data.update(MarketEvent(**{**e.__dict__, "payload": payload}))
        self.assertEqual(data.history, history)

    def test_legacy_without_calendar_keeps_explicit_reference_and_history_contract(self):
        _, history = historical_input()
        legacy = [{k: v for k, v in h.items() if k != "complete"} for h in history]
        data = SymbolData("SPY", "IEX")
        data.update(session(legacy, references={"prev_close": {"value": 123, "as_of": legacy[-1]["as_of"], "source": "legacy_known_reference"}}))
        self.assertEqual(data.reference("prev_close"), 123)
        self.assertEqual(data.reference("atr14"), 2)
        self.assertEqual(len(data.complete_history(14)), 14)

    def test_calendar_holiday_and_halfday_are_explicit_not_calendar_day_inferred(self):
        cal, history = historical_input(2)
        # The authoritative list can exclude a holiday; matching follows its
        # actual previous session rather than subtracting one day from cash open.
        cal.pop(); history.pop()
        half_close = datetime.fromisoformat(cal[0]["close_time"]).replace(hour=13)
        cal[0]["close_time"] = half_close.isoformat()
        history[0]["as_of"] = half_close.isoformat()
        data = SymbolData("SPY", "IEX")
        data.update(session(history, cal))
        self.assertEqual(data.reference("prev_close"), history[0]["close"])

    def test_declared_calendar_is_strict_and_bounded(self):
        cal, history = historical_input(2)
        cases = [True, {}, [cal[0]] * 31, [cal[0], cal[0]], list(reversed(cal)),
                 [{**cal[0], "source": ""}], [{**cal[0], "date": "2026-09-31"}],
                 [{**cal[0], "date": "2026-09-29"}],
                 [{**cal[0], "close_time": OPEN.isoformat()}]]
        for calendar in cases:
            with self.subTest(calendar=calendar), self.assertRaises(ValueError):
                SymbolData("SPY", "IEX").update(session(history, calendar))

    def test_restart_retains_calendar_guard_and_does_not_enable_stale_previous_session(self):
        cal, history = historical_input()
        engine = RecognitionEngine(include_variants=False)
        engine.feed(session(history[:-1], cal))
        restored = RecognitionEngine.restore(engine.snapshot())
        self.assertEqual(restored.universe[("SPY", "IEX")].history, history[:-1])
        self.assertIsNone(restored.universe[("SPY", "IEX")].reference("prev_close"))


if __name__ == "__main__":
    unittest.main()
