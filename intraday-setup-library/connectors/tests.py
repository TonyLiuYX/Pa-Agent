"""Read-only provider conversion, calendar and deterministic source tests."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
import unittest
from unittest.mock import patch

from recognition.models import MarketEvent
from . import AlpacaIEXProvider, DemoProvider, JsonlProvider
from .alpaca import AlpacaDataError, calendar_session, credential_presence, historical_summaries, DATA_URL, CALENDAR_URL

UTC = timezone.utc
NOW = datetime(2026, 9, 30, 14, tzinfo=UTC)


class ConnectorTests(unittest.TestCase):
    def test_demo_arrival_order_and_unique_ids(self):
        events = list(DemoProvider(minutes=15).events())
        self.assertEqual(len(events), 230)
        self.assertEqual(len({e.event_id for e in events}), len(events))
        self.assertEqual([e.arrival_time for e in events], sorted(e.arrival_time for e in events))
        self.assertTrue(all("notional" not in e.payload for e in events if e.kind == "bar"))
        self.assertEqual(DemoProvider().describe()["market_validation"], "not_run")

    def test_demo_multiday_skips_weekends_once(self):
        start = datetime(2026, 10, 2, 9, 30, tzinfo=timezone(timedelta(hours=-4)))
        events = [e for e in DemoProvider(symbols=["SPY"], minutes=1, start=start, sessions=4).events() if e.kind == "session"]
        self.assertEqual([e.payload["open_time"][:10] for e in events], ["2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07"])

    def test_stop_and_input_freeze(self):
        stop = Event()
        stop.set()
        self.assertEqual(list(DemoProvider().events(stop)), [])
        with TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            event = next(DemoProvider(symbols=["SPY"], minutes=1).events())
            path.write_text(json.dumps(event.to_dict()) + "\n")
            provider = JsonlProvider(path)
            self.assertEqual(list(provider.events())[0], event)
            path.write_text(path.read_text() + "\n")
            with self.assertRaisesRegex(ValueError, "changed"):
                list(provider.events())

    def test_credentials_presence_aliases_and_no_export(self):
        with patch.dict(os.environ, {"ALPACA_API_KEY_ID": "unit-key", "ALPACA_API_SECRET_KEY": "unit-secret"}, clear=True):
            self.assertTrue(credential_presence())
            self.assertNotIn("unit-key", json.dumps(AlpacaIEXProvider().describe()))
            self.assertNotIn("unit-secret", repr(AlpacaIEXProvider()))
        with patch.dict(os.environ, {"APCA_API_KEY_ID": "unit-key", "APCA_API_SECRET_KEY": "unit-secret"}, clear=True):
            self.assertTrue(AlpacaIEXProvider().describe()["configured"])
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(AlpacaDataError, "not_configured"):
                next(AlpacaIEXProvider().events())

    def test_symbol_limits(self):
        for symbols in (["SPY", "SPY"], ["spy"], [str(n) for n in range(31)]):
            with self.assertRaises(ValueError):
                AlpacaIEXProvider(symbols)

    def test_calendar_halfday_and_dst(self):
        summer = calendar_session({"date": "2026-07-02", "open": "09:30", "close": "13:00"})
        winter = calendar_session({"date": "2026-11-27", "open": "09:30", "close": "13:00"})
        self.assertEqual(summer["open_time"], "2026-07-02T13:30:00+00:00")
        self.assertEqual(winter["open_time"], "2026-11-27T14:30:00+00:00")
        self.assertEqual((datetime.fromisoformat(summer["close_time"]) - datetime.fromisoformat(summer["open_time"])).total_seconds(), 210 * 60)

    def test_bar_start_to_end_and_vendor_vwap_not_notional(self):
        provider = AlpacaIEXProvider(now=lambda: NOW)
        row = {"T": "b", "S": "SPY", "t": "2026-09-30T13:59:00Z", "o": 100, "h": 101, "l": 99, "c": 100, "v": 30, "vw": 100.4}
        event = provider.convert_message(row)
        self.assertEqual(event.event_time, NOW)
        self.assertNotIn("notional", event.payload)
        self.assertEqual(event.payload["vendor_vwap"], 100.4)
        self.assertEqual(provider.convert_message(row).event_id, event.event_id)

    def test_trade_unknown_and_correction_new_evidence(self):
        provider = AlpacaIEXProvider(now=lambda: NOW)
        trade = provider.convert_message({"T": "t", "S": "SPY", "t": "2026-09-30T13:59:59Z", "p": 100, "s": 3, "i": 4})
        self.assertEqual(trade.payload["side"], "unknown")
        correction = provider.convert_message({"T": "c", "S": "SPY", "t": "2026-09-30T13:59:59Z", "oi": 4, "ci": 5, "cp": 100.1})
        self.assertEqual(correction.kind, "status")
        self.assertTrue(correction.payload["correction"])
        self.assertTrue(correction.payload["gap"])
        self.assertNotEqual(correction.event_id, trade.event_id)

    def test_crossed_zero_quotes_become_explicit_gaps(self):
        provider = AlpacaIEXProvider(now=lambda: NOW)
        for bid, ask in ((101, 100), (0, 100)):
            row = {"T": "q", "S": "SPY", "t": "2026-09-30T13:59:59Z", "bp": bid, "ap": ask}
            event = provider.convert_message(row)
            self.assertEqual(event.kind, "status")
            self.assertTrue(event.payload["gap"])

    def test_history_no_current_or_missing_minute_calibration(self):
        cal = [calendar_session({"date": "2026-09-29", "open": "09:30", "close": "09:33"})]
        op = datetime.fromisoformat(cal[0]["open_time"])
        rows = [{"t": (op + timedelta(minutes=m)).isoformat(), "o": 100, "h": 101, "l": 99, "c": 100, "v": 10} for m in range(3)]
        summaries = historical_summaries(rows, cal, NOW)
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["volume"], 30)
        self.assertNotIn("vwap", summaries[0])
        self.assertEqual(historical_summaries(rows[:1] + rows[2:], cal, NOW), [])
        self.assertEqual(historical_summaries(rows, cal, op), [])

    def test_missing_previous_calendar_session_does_not_publish_older_previous_close(self):
        calendars = [{"date": day, "open": "09:30", "close": "09:32"}
                     for day in ("2026-09-28", "2026-09-29", "2026-09-30")]
        rows = [{"t": f"2026-09-28T13:3{minute}:00Z", "o": 100, "h": 101, "l": 99, "c": 101, "v": 10}
                for minute in (0, 1)]
        rows.append({"t": "2026-09-29T13:30:00Z", "o": 200, "h": 201, "l": 199, "c": 200, "v": 10})
        def get(url, params):
            return calendars if url == CALENDAR_URL else {"bars": {"SPY": rows}, "next_page_token": None}
        provider = AlpacaIEXProvider(["SPY"], http_get=get, history_sessions=2)
        output = provider.prepare_sessions(NOW)[0].payload
        self.assertEqual([h["date"] for h in output["history"]], ["2026-09-28"])
        self.assertNotIn("prev_close", output["references"])
        self.assertEqual(output["history_status"]["expected_previous_session"], "2026-09-29")
        self.assertEqual(output["history_status"]["missing_sessions"], ["2026-09-29"])
        self.assertEqual([c["date"] for c in output["history_calendar"]], ["2026-09-28", "2026-09-29"])
        self.assertTrue(all(c["source"] == "alpaca_trading_calendar" for c in output["history_calendar"]))

    def test_history_fetch_failure_keeps_only_calendar_matched_complete_causal_history(self):
        from recognition.data import SymbolData
        calendars = [{"date": day, "open": "09:30", "close": "09:32"}
                     for day in ("2026-09-28", "2026-09-29", "2026-09-30")]
        def get(url, params):
            if url == CALENDAR_URL:
                return calendars
            raise AlpacaDataError("rate_limited:429")
        provider = AlpacaIEXProvider(["SPY"], http_get=get, history_sessions=2)
        current = provider.prepare_sessions(NOW)[0]
        self.assertNotIn("history", current.payload)
        self.assertEqual(current.payload["history_status"]["error"], "rate_limited:429")
        self.assertEqual(current.payload["history_status"]["request_state"], "failed")
        fresh = SymbolData("SPY", "IEX")
        fresh.update(current)
        self.assertIsNone(fresh.reference("prev_close"))
        for day, expected in (("2026-09-29", 101), ("2026-09-28", None)):
            with self.subTest(recorded_day=day):
                data = SymbolData("SPY", "IEX")
                op = datetime.fromisoformat(day + "T13:30:00+00:00")
                data.update(MarketEvent.from_dict({"event_id": "local-session", "symbol": "SPY", "venue": "IEX",
                    "kind": "session", "event_time": op.isoformat(), "arrival_time": op.isoformat(),
                    "payload": {"open_time": op.isoformat(), "close_time": (op + timedelta(minutes=2)).isoformat()}}))
                for minute in (1, 2):
                    at = op + timedelta(minutes=minute)
                    data.update(MarketEvent.from_dict({"event_id": f"local-bar-{minute}", "symbol": "SPY", "venue": "IEX",
                        "kind": "bar", "event_time": at.isoformat(), "arrival_time": at.isoformat(),
                        "payload": {"open": 100, "high": 101, "low": 99, "close": 101, "volume": 10, "seconds": 60}}))
                data.update(current)
                self.assertEqual(len(data.history), 1)
                self.assertTrue(data.history[0]["complete"])
                self.assertEqual(data.history[0]["source"], "causal_replay:IEX")
                self.assertEqual(data.reference("prev_close"), expected)

    def test_zero_history_request_preserves_true_causal_prior_without_remote_request(self):
        from recognition.data import SymbolData
        calendars = [{"date": day, "open": "09:30", "close": "09:32"}
                     for day in ("2026-09-28", "2026-09-29", "2026-09-30")]
        calls = []
        def get(url, params):
            calls.append(url)
            self.assertEqual(url, CALENDAR_URL)
            return calendars
        provider = AlpacaIEXProvider(["SPY"], http_get=get, history_sessions=0)
        current = provider.prepare_sessions(NOW)[0]
        self.assertEqual(calls, [CALENDAR_URL])
        self.assertNotIn("history", current.payload)
        self.assertEqual(current.payload["history_status"]["request_state"], "disabled")
        self.assertIsNone(current.payload["history_status"]["error"])
        self.assertEqual(current.payload["history_status"]["requested_sessions"], 0)
        fresh = SymbolData("SPY", "IEX")
        fresh.update(current)
        self.assertIsNone(fresh.reference("prev_close"))
        for day, expected in (("2026-09-29", 101), ("2026-09-28", None)):
            with self.subTest(recorded_day=day):
                data = SymbolData("SPY", "IEX")
                op = datetime.fromisoformat(day + "T13:30:00+00:00")
                data.update(MarketEvent.from_dict({"event_id": "local-session", "symbol": "SPY", "venue": "IEX",
                    "kind": "session", "event_time": op.isoformat(), "arrival_time": op.isoformat(),
                    "payload": {"open_time": op.isoformat(), "close_time": (op + timedelta(minutes=2)).isoformat()}}))
                for minute in (1, 2):
                    at = op + timedelta(minutes=minute)
                    data.update(MarketEvent.from_dict({"event_id": f"local-bar-{minute}", "symbol": "SPY", "venue": "IEX",
                        "kind": "bar", "event_time": at.isoformat(), "arrival_time": at.isoformat(),
                        "payload": {"open": 100, "high": 101, "low": 99, "close": 101, "volume": 10, "seconds": 60}}))
                data.update(current)
                self.assertEqual(len(data.history), 1)
                self.assertTrue(data.history[0]["complete"])
                self.assertEqual(data.reference("prev_close"), expected)

    def test_rest_iex_raw_pagination_and_cycle_detection(self):
        calls = []
        def get(url, params):
            calls.append((url, dict(params)))
            if len(calls) == 1:
                return {"bars": {"SPY": [{"t": "2026-09-29T13:30:00Z"}]}, "next_page_token": "next"}
            return {"bars": {"SPY": []}, "next_page_token": None}
        provider = AlpacaIEXProvider(["SPY"], http_get=get)
        self.assertEqual(len(provider.history("2026-09-29T13:30:00Z", "2026-09-29T20:00:00Z")["SPY"]), 1)
        self.assertEqual(calls[0][1]["feed"], "iex")
        self.assertEqual(calls[0][1]["adjustment"], "raw")
        self.assertEqual(calls[1][1]["page_token"], "next")
        cycle = AlpacaIEXProvider(["SPY"], http_get=lambda *_: {"bars": {}, "next_page_token": "same"})
        with self.assertRaisesRegex(AlpacaDataError, "pagination_loop"):
            cycle.history("2026-09-29T13:30:00Z", "2026-09-29T20:00:00Z")

    def test_midday_calendar_arrival_does_not_claim_full_coverage(self):
        def get(url, params):
            self.assertEqual(url, CALENDAR_URL)
            return [{"date": "2026-09-30", "open": "09:30", "close": "16:00"}]
        provider = AlpacaIEXProvider(["SPY"], now=lambda: NOW, http_get=get, history_sessions=0)
        event = provider.prepare_sessions()[0]
        self.assertLess(event.event_time, event.arrival_time)
        self.assertFalse(event.payload["coverage"]["trades"]["complete"])
        self.assertNotIn("history", event.payload)
        self.assertEqual(event.payload["history_status"]["request_state"], "disabled")
        self.assertEqual(event.payload["source_scope"], "IEX_only")

    def test_vendor_identity_collision_and_explicit_update_identity(self):
        provider = AlpacaIEXProvider(now=lambda: NOW)
        bar = {"T": "b", "S": "SPY", "t": "2026-09-30T13:59:00Z", "o": 100, "h": 101, "l": 99, "c": 100, "v": 30}
        first = provider.convert_message(bar)
        changed = provider.convert_message(dict(bar, c=100.1))
        update = provider.convert_message(dict(bar, T="u", c=100.1))
        self.assertEqual(first.event_id, changed.event_id)
        self.assertNotEqual(first.event_id, update.event_id)
        self.assertEqual(update.kind, "status")
        trade = {"T": "t", "S": "SPY", "t": "2026-09-30T13:59:00Z", "p": 100, "s": 3, "i": 5}
        self.assertEqual(provider.convert_message(trade).event_id, provider.convert_message(dict(trade, p=101)).event_id)

    def test_socket_auth_subscribe_arrival_order_and_stop(self):
        stop = Event()
        frames = [[{"T": "success", "msg": "authenticated"}],
                  [{"T": "subscription", "bars": ["SPY"], "quotes": ["SPY"], "trades": ["SPY"]}],
                  [{"T": "q", "S": "SPY", "t": "2026-09-30T13:59:59Z", "bp": 99, "ap": 100},
                   {"T": "t", "S": "SPY", "t": "2026-09-30T13:59:58Z", "p": 100, "s": 1, "i": 8}]]
        sent = []
        class Socket:
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def send(self, message):
                sent.append(json.loads(message))
            def recv(self, timeout):
                frame = frames.pop(0)
                if not frames:
                    stop.set()
                return json.dumps(frame)
        def get(*_):
            return [{"date": "2026-09-30", "open": "09:30", "close": "16:00"}]
        with patch.dict(os.environ, {"ALPACA_API_KEY_ID": "unit-key", "ALPACA_API_SECRET_KEY": "unit-secret"}, clear=True):
            provider = AlpacaIEXProvider(["SPY"], now=lambda: NOW, http_get=get, socket_factory=lambda *_a, **_k: Socket(), history_sessions=0)
            events = list(provider.events(stop))
        self.assertEqual([e.kind for e in events], ["session", "status", "quote", "trade"])
        self.assertGreater(events[-2].event_time, events[-1].event_time)  # preserves received order, never sorts
        self.assertEqual(sent[0]["action"], "auth")
        self.assertEqual(sent[1]["quotes"], ["SPY"])

    def test_auth_error_is_terminal_and_sanitized(self):
        class Socket:
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def send(self, _):
                pass
            def recv(self, **_):
                return json.dumps([{"T": "error", "code": 402, "msg": "unit-secret"}])
        with patch.dict(os.environ, {"APCA_API_KEY_ID": "unit-key", "APCA_API_SECRET_KEY": "unit-secret"}, clear=True):
            provider = AlpacaIEXProvider(["SPY"], now=lambda: NOW, http_get=lambda *_: [{"date": "2026-09-30", "open": "09:30", "close": "16:00"}], socket_factory=lambda *_a, **_k: Socket(), history_sessions=0)
            iterator = provider.events()
            next(iterator)
            status = next(iterator)
            self.assertNotIn("unit-secret", json.dumps(status.to_dict()))
            with self.assertRaisesRegex(AlpacaDataError, "auth_or_subscription"):
                next(iterator)

    def test_mixed_frame_statuses_keep_receipt_clock_with_advancing_now(self):
        from recognition.engine import RecognitionEngine
        stop = Event()
        current = [NOW]
        def now():
            current[0] += timedelta(microseconds=100)
            return current[0]
        good_quote = {"T": "q", "S": "SPY", "t": "2026-09-30T13:59:59Z", "bp": 99, "ap": 100}
        frames = [[{"T": "success", "msg": "authenticated"}],
                  [{"T": "subscription", "bars": ["SPY"], "quotes": ["SPY"], "trades": ["SPY"]}, good_quote],
                  [{"T": "q", "S": "SPY", "bp": 99, "ap": 100}, {**good_quote, "bp": 98}],
                  [{**good_quote, "t": "2026-09-30T14:00:01Z"}, {**good_quote, "bp": 97}],
                  [{**good_quote, "bp": 0}, {**good_quote, "bp": 96}]]
        sent = []
        class Socket:
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def send(self, value):
                sent.append(json.loads(value))
            def recv(self, **_):
                frame = frames.pop(0)
                if not frames:
                    stop.set()
                return json.dumps(frame)
        with patch.dict(os.environ, {"ALPACA_API_KEY_ID": "unit-key", "ALPACA_API_SECRET_KEY": "unit-secret"}, clear=True):
            provider = AlpacaIEXProvider(["SPY"], now=now,
                http_get=lambda *_: [{"date": "2026-09-30", "open": "09:30", "close": "16:00"}],
                socket_factory=lambda *_a, **_k: Socket(), history_sessions=0)
            events = list(provider.events(stop))
        self.assertEqual([e.kind for e in events], ["session", "status", "quote", "status", "quote", "status", "quote", "status", "quote"])
        self.assertEqual([e.arrival_time for e in events], sorted(e.arrival_time for e in events))
        for index in (1, 3, 5, 7):
            self.assertEqual(events[index].arrival_time, events[index + 1].arrival_time)
        self.assertEqual(events[5].payload["connection_state"], "invalid_future_timestamp")
        self.assertEqual(sent[0]["action"], "auth")
        self.assertEqual(sent[1]["quotes"], ["SPY"])
        engine = RecognitionEngine()
        for event in events:
            engine.feed(event)  # mixed frames must remain acceptable to the real arrival guard

    def test_transient_calendar_failure_retries_then_stream_recovers(self):
        notices, waits, requests = [], [], []
        class NoWaitStop(Event):
            def wait(self, timeout=None):
                waits.append(timeout)
                return self.is_set()
        stop = NoWaitStop()
        def get(url, params):
            requests.append(url)
            if len(requests) == 1:
                raise AlpacaDataError("network_or_invalid_response")
            if len(requests) == 2:
                raise AlpacaDataError("http_error:503")
            return [{"date": "2026-09-30", "open": "09:30", "close": "16:00"}]
        frames = [[{"T": "success", "msg": "authenticated"}],
                  [{"T": "subscription", "bars": ["SPY"], "quotes": ["SPY"], "trades": ["SPY"]},
                   {"T": "q", "S": "SPY", "t": "2026-09-30T13:59:59Z", "bp": 99, "ap": 100}]]
        class Socket:
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def send(self, _):
                pass
            def recv(self, **_):
                frame = frames.pop(0)
                if not frames:
                    stop.set()
                return json.dumps(frame)
        with patch.dict(os.environ, {"ALPACA_API_KEY_ID": "unit-key", "ALPACA_API_SECRET_KEY": "unit-secret"}, clear=True):
            provider = AlpacaIEXProvider(["SPY"], now=lambda: NOW, http_get=get,
                socket_factory=lambda *_a, **_k: Socket(), history_sessions=0,
                status_callback=lambda state, **detail: notices.append({"state": state, **detail}))
            events = list(provider.events(stop))
        self.assertEqual(waits, [1, 2])
        self.assertEqual([e.kind for e in events], ["session", "status", "quote"])
        self.assertEqual([r["state"] for r in notices], ["DISCONNECTED", "DISCONNECTED", "INITIALIZED", "CONNECTED"])
        self.assertTrue(all(r["phase"] == "calendar_initialization" for r in notices[:3]))
        self.assertEqual([r["retry_seconds"] for r in notices[:2]], [1, 2])
        self.assertEqual(notices[-1]["at"], events[-1].arrival_time.isoformat())
        self.assertEqual(provider.describe()["initialization_status"]["state"], "ready")
        self.assertNotIn("unit-secret", json.dumps(notices))

    def test_terminal_calendar_error_never_fabricates_session_or_retries(self):
        for error in ("authentication_failed:401", "feed_not_entitled:403", "http_error:404", "invalid_calendar_response"):
            with self.subTest(error=error):
                stop = Event()
                notices = []
                def get(*_):
                    raise AlpacaDataError(error)
                with patch.dict(os.environ, {"ALPACA_API_KEY_ID": "unit-key", "ALPACA_API_SECRET_KEY": "unit-secret"}, clear=True):
                    provider = AlpacaIEXProvider(["SPY"], now=lambda: NOW, http_get=get,
                        socket_factory=lambda *_a, **_k: self.fail("terminal calendar failure must not open socket"),
                        status_callback=lambda state, **detail: notices.append({"state": state, **detail}))
                    with self.assertRaisesRegex(AlpacaDataError, error):
                        next(provider.events(stop))
                self.assertEqual(notices, [])  # owning runtime journals the single terminal outcome
                self.assertEqual(provider.describe()["initialization_status"]["state"], "failed")
                self.assertIsNone(provider.describe()["initialization_status"]["retry_seconds"])

    def test_rest_retry_classification(self):
        for kind in ("network_or_invalid_response", "rate_limited:429", "http_error:408", "http_error:500", "http_error:599"):
            self.assertTrue(AlpacaIEXProvider._retryable(AlpacaDataError(kind), "calendar_initialization"))
        for kind in ("http_error:400", "http_error:600", "invalid_calendar_response", "stream_auth_or_subscription_error:402"):
            self.assertFalse(AlpacaIEXProvider._retryable(AlpacaDataError(kind), "calendar_initialization"))
        self.assertFalse(AlpacaIEXProvider._retryable(ValueError("calendar malformed"), "calendar_initialization"))
        self.assertTrue(AlpacaIEXProvider._retryable(TimeoutError(), "calendar_initialization"))
        with self.assertRaises(ValueError):
            AlpacaIEXProvider(status_callback=False)

    def test_invalid_messages_and_nonfinite_pace(self):
        provider = AlpacaIEXProvider(now=lambda: NOW)
        row = provider.convert_message({"T": "q", "S": "SPY", "bp": 100, "ap": 101})
        self.assertEqual(row.kind, "status")
        self.assertEqual(row.payload["stream"], "quote")
        for value in (True, float("nan"), float("inf"), -1):
            with self.assertRaises(ValueError):
                DemoProvider(pace_seconds=value)

    def test_only_explicit_trading_resume_clears_halt(self):
        provider = AlpacaIEXProvider(now=lambda: NOW)
        def status(code):
            return provider.convert_message({"T": "s", "S": "SPY", "t": "2026-09-30T13:59:59Z", "sc": code})
        self.assertTrue(status("H").payload["halted"])
        self.assertFalse(status("T").payload["halted"])
        self.assertNotIn("halted", status("Q").payload)  # quote resume alone does not resume trading
        self.assertNotIn("halted", status("5").payload)  # price indication is not a trading resume


if __name__ == "__main__":
    unittest.main()
