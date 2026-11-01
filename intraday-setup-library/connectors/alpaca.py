"""Alpaca IEX read-only adapter. Secrets remain in environment/memory.

Only calendar and historical-bar GETs and the IEX market-data WebSocket exist
in this module. There is deliberately no broker order endpoint.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
import time
from threading import Event
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from recognition.models import MarketEvent, timestamp
from .sources import DEFAULT_SYMBOLS, symbols_checked

NY = ZoneInfo("America/New_York")
STREAM_URL = "wss://stream.data.alpaca.markets/v2/iex"
DATA_URL = "https://data.alpaca.markets/v2/stocks/bars"
CALENDAR_URL = "https://paper-api.alpaca.markets/v2/calendar"


class AlpacaDataError(RuntimeError):
    """Sanitized classification; never includes request headers or credentials."""


def credential_presence():
    return bool((os.environ.get("ALPACA_API_KEY_ID") or os.environ.get("APCA_API_KEY_ID"))
                and (os.environ.get("ALPACA_API_SECRET_KEY") or os.environ.get("APCA_API_SECRET_KEY")))


def calendar_session(row):
    day = date.fromisoformat(row["date"])
    def clock(value):
        if "T" in value:
            return timestamp(value)
        return datetime.fromisoformat(f"{day.isoformat()}T{value}").replace(tzinfo=NY).astimezone(timezone.utc)
    op, close = clock(row["open"]), clock(row["close"])
    if op >= close:
        raise ValueError("calendar session has invalid bounds")
    return {"date": day.isoformat(), "open_time": op.isoformat(), "close_time": close.isoformat()}


def historical_summaries(rows, calendar, cutoff):
    """RTH-only known, completed sessions. No artificial zero-minute coverage."""
    cutoff = timestamp(cutoff)
    output = []
    for session in calendar:
        op, close = timestamp(session["open_time"]), timestamp(session["close_time"])
        if close >= cutoff:
            continue
        bars = sorted((r for r in rows if op <= timestamp(r["t"]) < close), key=lambda r: timestamp(r["t"]))
        if not bars:
            continue
        times = [timestamp(r["t"]) for r in bars]
        # Missing opening/closing minutes do not supply cash-open/daily context.
        if times[0] != op or times[-1] + timedelta(minutes=1) != close or len(set(times)) != len(times):
            continue
        summary = {"date": session["date"], "as_of": close.isoformat(), "cash_open": bars[0]["o"],
                   "close": bars[-1]["c"], "high": max(r["h"] for r in bars), "low": min(r["l"] for r in bars),
                   "volume": sum(r["v"] for r in bars), "source": "alpaca_iex_rth_minute_aggregates",
                   "source_scope": "IEX_only", "closes": {}, "volumes": {},
                   "minute_coverage": len(times) / int((close - op).total_seconds() / 60),
                   "complete": len(times) == int((close - op).total_seconds() / 60)}
        for bar in bars:
            end = timestamp(bar["t"]) + timedelta(minutes=1)
            summary["closes"][end.astimezone(NY).strftime("%H:%M")] = bar["c"]
            summary["volumes"][end.astimezone(NY).strftime("%H:%M")] = bar["v"]
        # Incomplete daily totals are not valid ATR/liquidity calibration input.
        if summary["complete"]:
            output.append(summary)
    return output[-30:]


class AlpacaIEXProvider:
    def __init__(self, symbols=DEFAULT_SYMBOLS, *, now=None, http_get=None, socket_factory=None, history_sessions=15,
                 status_callback=None):
        self.symbols = symbols_checked(symbols)
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.http_get = http_get
        self.socket_factory = socket_factory
        self.history_sessions = history_sessions
        if type(history_sessions) is not int or not 0 <= history_sessions <= 30:
            raise ValueError("history_sessions must be integer 0..30")
        self._key = os.environ.get("ALPACA_API_KEY_ID") or os.environ.get("APCA_API_KEY_ID")
        self._secret = os.environ.get("ALPACA_API_SECRET_KEY") or os.environ.get("APCA_API_SECRET_KEY")
        self._session_events = {}
        self._calendars = []
        self._counter = 0
        self._connected = False
        self._status_callback = None
        self.set_status_callback(status_callback)
        self._initialization_status = {"state": "not_started", "phase": "calendar_initialization",
                                       "error_kind": None, "retry_seconds": None}

    def set_status_callback(self, callback):
        """Administrative health channel, including failures before a session exists.

        A runtime may bind its journalled source_status method. These notices never
        invent a market session or advance recognition's arrival watermark.
        """
        if callback is not None and not callable(callback):
            raise ValueError("status callback must be callable or None")
        self._status_callback = callback

    def _notice(self, state, **details):
        if self._status_callback is not None:
            self._status_callback(state, **details)

    @staticmethod
    def _error_kind(exc):
        if isinstance(exc, AlpacaDataError):
            return str(exc).split(";", 1)[0]
        if isinstance(exc, (URLError, TimeoutError)):
            return "network_or_invalid_response"
        return type(exc).__name__

    @staticmethod
    def _retryable(exc, phase):
        if not isinstance(exc, AlpacaDataError):
            return isinstance(exc, (URLError, TimeoutError, OSError)) if phase == "calendar_initialization" else True
        kind = str(exc).split(";", 1)[0]
        if kind.startswith(("credentials_not_configured", "authentication_failed", "feed_not_entitled",
                            "stream_auth_or_subscription_error", "incomplete_subscription")):
            return False
        if kind.startswith("http_error:"):
            try:
                code = int(kind.split(":", 1)[1])
            except ValueError:
                return False
            return code in {408, 425, 429} or 500 <= code <= 599
        if phase == "calendar_initialization":
            return kind.startswith(("network_or_invalid_response", "rate_limited"))
        return True

    def describe(self):
        return {"name": "alpaca_iex", "mode": "live", "symbols": list(self.symbols), "feed": "iex",
                "source_scope": "IEX_only_not_consolidated_US_market", "configured": bool(self._key and self._secret),
                "capabilities": ["bars", "quotes", "trades", "sync"],
                "conditional_capabilities": ["prior_session", "daily_atr14", "liquidity_history"],
                "unavailable": ["classified_trades", "depth", "news", "exact_bar_notional", "consolidated_market_volume"],
                "market_validation": "not_run", "arrival_order": "received_frame_order",
                "history_policy": "completed_RTH_sessions_with_all_minutes_only", "stream": STREAM_URL,
                "initialization_status": dict(self._initialization_status),
                "live_trading_status": "not_subscribed_not_validated",
                "reconnect_history": "no_same_session_refresh_or_automatic_backfill"}

    def _get(self, url, params):
        if self.http_get:
            return self.http_get(url, params)
        if not self._key or not self._secret:
            raise AlpacaDataError("credentials_not_configured")
        request = Request(url + "?" + urlencode(params), headers={"APCA-API-KEY-ID": self._key, "APCA-API-SECRET-KEY": self._secret}, method="GET")
        try:
            with urlopen(request, timeout=15) as response:
                return json.load(response)
        except HTTPError as exc:
            kind = {401: "authentication_failed", 403: "feed_not_entitled", 429: "rate_limited"}.get(exc.code, "http_error")
            raise AlpacaDataError(f"{kind}:{exc.code}") from None
        except (URLError, TimeoutError, ValueError):
            raise AlpacaDataError("network_or_invalid_response") from None

    def calendar(self, start, end):
        rows = self._get(CALENDAR_URL, {"start": str(start), "end": str(end)})
        if not isinstance(rows, list):
            raise AlpacaDataError("invalid_calendar_response")
        sessions = [calendar_session(r) for r in rows]
        sessions.sort(key=lambda r: r["open_time"])
        return sessions

    def history(self, start, end):
        result = {s: [] for s in self.symbols}
        token = None
        tokens = set()
        for _ in range(200):
            params = {"symbols": ",".join(self.symbols), "timeframe": "1Min", "start": timestamp(start).isoformat(),
                      "end": timestamp(end).isoformat(), "feed": "iex", "adjustment": "raw", "sort": "asc", "limit": 10000}
            if token:
                params["page_token"] = token
            row = self._get(DATA_URL, params)
            if not isinstance(row, dict) or not isinstance(row.get("bars"), dict):
                raise AlpacaDataError("invalid_history_response")
            for symbol in self.symbols:
                result[symbol].extend(row["bars"].get(symbol, []))
            token = row.get("next_page_token")
            if not token:
                return result
            if token in tokens:
                raise AlpacaDataError("history_pagination_loop")
            tokens.add(token)
        raise AlpacaDataError("history_pagination_limit")

    def prepare_sessions(self, observed_at=None):
        now = timestamp(observed_at or self.now())
        day = now.astimezone(NY).date()
        calendars = self.calendar(day - timedelta(days=70), day + timedelta(days=7))
        self._calendars = calendars
        current = next((r for r in calendars if r["date"] == day.isoformat()), None)
        if current is None:
            return []
        preceding = [r for r in calendars if timestamp(r["close_time"]) < timestamp(current["open_time"])]
        past = preceding[-self.history_sessions:] if self.history_sessions else []
        summaries = {s: [] for s in self.symbols}
        history_error = None
        if past:
            try:
                raw = self.history(past[0]["open_time"], past[-1]["close_time"])
                summaries = {s: historical_summaries(raw[s], past, current["open_time"]) for s in self.symbols}
            except AlpacaDataError as exc:
                history_error = str(exc)
        events = []
        for symbol in self.symbols:
            history = summaries[symbol]
            op = timestamp(current["open_time"])
            # Contract requires calendar event_time <= cash open; arrival records when
            # it became known. A mid-session start therefore never claims full tape.
            payload = {"open_time": current["open_time"], "close_time": current["close_time"], "tick_size": .01,
                       "asset_class": "ETF" if symbol in {"SPY", "QQQ"} else "equity", "history": history,
                       "history_calendar": [{**row, "source": "alpaca_trading_calendar"} for row in preceding[-30:]],
                       "source_scope": "IEX_only", "coverage": {"bars": {"complete": False, "start_time": now.isoformat()},
                       "trades": {"complete": False, "start_time": now.isoformat()}},
                       "history_status": {"complete_sessions": len(history), "requested_sessions": self.history_sessions,
                                          "error": history_error,
                                          "request_state": "disabled" if self.history_sessions == 0 else "failed" if history_error else "completed" if past else "no_prior_sessions",
                                          "expected_previous_session": preceding[-1]["date"] if preceding else None,
                                          "missing_sessions": [r["date"] for r in past if not any(h["date"] == r["date"] for h in history)]}, "references": {}}
            if history_error or self.history_sessions == 0:
                # An unrequested or failed fetch supplies no replacement history. Keep any
                # locally collected complete sessions; the explicit calendar still
                # prevents them from bridging an actually missing trading day.
                payload.pop("history")
            if history and preceding and history[-1]["date"] == preceding[-1]["date"]:
                payload["references"]["prev_close"] = {"value": history[-1]["close"], "as_of": history[-1]["as_of"], "source": "alpaca_iex_rth"}
            event = MarketEvent.from_dict({"event_id": f"ALPACA-IEX-{current['date']}-{symbol}-session",
                "symbol": symbol, "venue": "IEX", "kind": "session", "event_time": min(now, op).isoformat(),
                "arrival_time": now.isoformat(), "payload": payload})
            self._session_events[symbol] = event
            events.append(event)
        return events

    def _status(self, symbol, state, *, gap=False, halted=False, stream="all", error_kind=None, received_at=None):
        # A status derived from a received frame shares its reception clock. Taking
        # a new wall-clock sample here can move past later messages in that frame.
        now = timestamp(received_at if received_at is not None else self.now())
        self._counter += 1
        return MarketEvent.from_dict({"event_id": f"ALPACA-IEX-status-{state}-{now.isoformat()}-{self._counter}-{symbol}",
            "symbol": symbol, "venue": "IEX", "kind": "status", "event_time": now.isoformat(), "arrival_time": now.isoformat(),
            "quality": "partial" if gap else "complete", "payload": {"stream": stream, "gap": gap, "halted": halted,
            "connected": state == "connected", "connection_state": state, "source_scope": "IEX_only", "error_kind": error_kind}})

    def convert_message(self, row, received_at=None):
        if not isinstance(row, dict) or row.get("S") not in self.symbols:
            return None
        try:
            return self._convert_message(row, received_at)
        except (KeyError, TypeError, ValueError, AttributeError):
            stream = {"q": "quote", "b": "bar", "u": "bar", "t": "trade"}.get(row.get("T"), "all")
            return self._status(row["S"], "invalid_market_message", gap=True, stream=stream, received_at=received_at)

    def _convert_message(self, row, received_at=None):
        received = timestamp(received_at or self.now())
        kind, symbol = row.get("T"), row.get("S")
        if symbol not in self.symbols:
            return None
        if kind not in {"b", "q", "t", "u", "c", "x", "s"}:
            return None
        source_time = timestamp(row["t"])
        t = source_time + timedelta(minutes=1) if kind == "b" else source_time
        if received < t:
            return self._status(symbol, "invalid_future_timestamp", gap=True, stream={"q": "quote", "b": "bar", "t": "trade"}.get(kind, "all"), received_at=received)
        stable = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        digest = hashlib.sha256(stable.encode()).hexdigest()[:24]
        identity = f"ALPACA-IEX-{symbol}-{kind}-{digest}"
        if kind == "b":
            identity = f"ALPACA-IEX-{symbol}-b-{source_time.isoformat()}"
        elif kind == "t":
            if type(row.get("i")) is not int:
                raise ValueError("vendor trade id must be an integer")
            identity = f"ALPACA-IEX-{symbol}-t-{source_time.astimezone(NY).date().isoformat()}-{row.get('x', 'IEX')}-{row['i']}"
        p = {"source_scope": "IEX_only", "vendor_timestamp": row["t"]}
        quality = "complete"
        if kind == "b":
            p.update(open=row["o"], high=row["h"], low=row["l"], close=row["c"], volume=row["v"],
                     seconds=60, closed=True, vendor_vwap=row.get("vw"), vendor_trade_count=row.get("n"))
            event_kind = "bar"
        elif kind == "q":
            p.update(bid=row["bp"], ask=row["ap"], bid_size=row.get("bs"), ask_size=row.get("as"))
            event_kind = "quote"
        elif kind == "t":
            p.update(price=row["p"], size=row["s"], side="unknown", trade_id=row.get("i"), conditions=row.get("c", []))
            event_kind = "trade"
        elif kind == "s":
            p.update(gap=False, stream="all", vendor_status=row.get("sc"))
            if row.get("sc") in {"H", "2", "P"}:
                p["halted"] = True
            elif row.get("sc") in {"3", "T"}:
                p["halted"] = False
            event_kind = "status"
        else:
            # Corrections are new evidence and quality disruptions, never retroactive bars/trades.
            p.update(gap=True, halted=False, stream="trade" if kind in {"c", "x"} else "bar", correction=True,
                     correction_type=kind, original_trade_id=row.get("oi", row.get("i")), vendor_message=row)
            event_kind, quality = "status", "partial"
        try:
            return MarketEvent.from_dict({"event_id": identity, "symbol": symbol,
                "venue": "IEX", "kind": event_kind, "event_time": t.isoformat(), "arrival_time": received.isoformat(),
                "quality": quality, "payload": p})
        except (KeyError, TypeError, ValueError):
            return self._status(symbol, "invalid_market_message", gap=True, stream={"q": "quote", "b": "bar", "t": "trade"}.get(kind, "all"), received_at=received)

    def events(self, stop_event=None):
        stop = stop_event or Event()
        if not self._key or not self._secret:
            raise AlpacaDataError("credentials_not_configured; use demo or JSONL replay")
        factory = self.socket_factory
        if factory is None:
            try:
                from websockets.sync.client import connect
            except ImportError:
                raise AlpacaDataError("optional_dependency_missing: install connectors/requirements.txt") from None
            factory = connect
        last_day = None
        backoff = 1
        while not stop.is_set():
            phase = "calendar_initialization"
            try:
                now = timestamp(self.now())
                day = now.astimezone(NY).date().isoformat()
                if day != last_day:
                    self._initialization_status.update(state="initializing", error_kind=None, retry_seconds=None)
                    for event in self.prepare_sessions(now):
                        yield event
                    last_day = day
                    self._initialization_status.update(state="ready", error_kind=None, retry_seconds=None)
                    self._notice("INITIALIZED", phase=phase, at=now.isoformat())
                if not self._session_events or all(timestamp(e.payload["close_time"]) <= now for e in self._session_events.values()):
                    if stop.wait(30):
                        return
                    continue
                phase = "websocket"
                with factory(STREAM_URL, open_timeout=15, ping_interval=20, ping_timeout=20, max_queue=16) as socket:
                    socket.send(json.dumps({"action": "auth", "key": self._key, "secret": self._secret}))
                    authenticated, subscribed = False, False
                    auth_deadline = time.monotonic() + 15
                    while not stop.is_set():
                        if not subscribed and time.monotonic() > auth_deadline:
                            raise AlpacaDataError("authentication_or_subscription_timeout")
                        try:
                            frame = socket.recv(timeout=5)
                        except TimeoutError:
                            if timestamp(self.now()).astimezone(NY).date().isoformat() != day:
                                break
                            continue
                        received = timestamp(self.now())
                        messages = json.loads(frame)
                        if not isinstance(messages, list):
                            raise AlpacaDataError("invalid_stream_frame")
                        for row in messages:
                            if row.get("T") == "error":
                                code = row.get("code")
                                if code in {401, 402, 403, 405, 406, 407}:
                                    raise AlpacaDataError(f"stream_auth_or_subscription_error:{code}")
                                raise AlpacaDataError(f"stream_error:{code}")
                            if row.get("T") == "success" and row.get("msg") == "authenticated":
                                authenticated = True
                                socket.send(json.dumps({"action": "subscribe", "bars": list(self.symbols), "trades": list(self.symbols),
                                                        "quotes": list(self.symbols), "updatedBars": list(self.symbols)}))
                            elif row.get("T") == "subscription":
                                if not authenticated or any(set(row.get(channel, [])) != set(self.symbols) for channel in ("bars", "quotes", "trades")):
                                    raise AlpacaDataError("incomplete_subscription")
                                subscribed, self._connected, backoff = True, True, 1
                                self._notice("CONNECTED", phase=phase, at=received.isoformat())
                                for symbol in self.symbols:
                                    yield self._status(symbol, "connected", received_at=received)
                            elif subscribed:
                                event = self.convert_message(row, received)
                                if event is not None:
                                    # Live session initialized before processing even when starting midday.
                                    if event.symbol in self._session_events:
                                        yield event
                        if received.astimezone(NY).date().isoformat() != day:
                            break
            except Exception as exc:
                self._connected = False
                error_kind = self._error_kind(exc)
                retryable = self._retryable(exc, phase)
                if retryable:
                    self._notice("DISCONNECTED", phase=phase, code=error_kind,
                                 retry_seconds=backoff, retryable=True)
                if phase == "calendar_initialization":
                    self._initialization_status.update(state="retrying" if retryable else "failed",
                                                      error_kind=error_kind, retry_seconds=backoff if retryable else None)
                else:
                    for symbol in self.symbols:
                        yield self._status(symbol, "disconnected", gap=True, error_kind=error_kind)
                if not retryable:
                    raise
                # Exception text can contain headers; expose only class/category.
                if stop.wait(backoff):
                    return
                backoff = min(60, backoff * 2)
