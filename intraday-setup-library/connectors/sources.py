"""Deterministic demo and canonical JSONL sources, no SDK dependency."""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import math
from pathlib import Path
from threading import Event
from zoneinfo import ZoneInfo

from recognition.data import read_events
from recognition.models import MarketEvent

DEFAULT_SYMBOLS = ("SPY", "QQQ", "AAPL", "MSFT", "NVDA")


def symbols_checked(symbols):
    if isinstance(symbols, str):
        raise ValueError("symbols must be a ticker sequence")
    symbols = tuple(DEFAULT_SYMBOLS if symbols is None else symbols)
    if not 1 <= len(symbols) <= 30 or len(set(symbols)) != len(symbols):
        raise ValueError("select 1–30 distinct symbols")
    if any(not isinstance(s, str) or not s or len(s) > 20 or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-" for c in s) for s in symbols):
        raise ValueError("symbols require uppercase ticker characters")
    return symbols


def pause(stop_event, seconds):
    if seconds:
        return (stop_event or Event()).wait(seconds)
    return bool(stop_event and stop_event.is_set())


def _pace(value):
    if isinstance(value, bool):
        raise ValueError("pace must be a finite nonnegative number")
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise ValueError("pace must be a finite nonnegative number")
    return value


def _file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class JsonlProvider:
    def __init__(self, path, pace_seconds=0):
        self.path = Path(path)
        self.pace_seconds = _pace(pace_seconds)
        self.input_sha256 = _file_hash(self.path)

    def describe(self):
        return {"name": "jsonl", "mode": "replay", "source_scope": "declared_by_input",
                "input": str(self.path.resolve()), "input_sha256": self.input_sha256,
                "capabilities": [], "capability_note": "per-session declarations determine capabilities",
                "arrival_order": "preserved_input_order", "market_validation": "not_run"}

    def events(self, stop_event=None):
        for event in read_events(self.path):
            if pause(stop_event, self.pace_seconds):
                return
            yield event
        if _file_hash(self.path) != self.input_sha256:
            raise ValueError("JSONL input changed during replay")


class DemoProvider:
    """Five symbols with explicitly synthetic daily history, bars and future quotes."""
    def __init__(self, symbols=DEFAULT_SYMBOLS, minutes=390, pace_seconds=0, start=None, sessions=1):
        self.symbols = symbols_checked(symbols)
        if type(minutes) is not int or not 1 <= minutes <= 390 or type(sessions) is not int or sessions <= 0:
            raise ValueError("demo minutes=1..390 and sessions>0 required")
        self.minutes, self.sessions = minutes, sessions
        self.pace_seconds = _pace(pace_seconds)
        self.start = start or datetime(2026, 9, 30, 9, 30, tzinfo=ZoneInfo("America/New_York"))
        if self.start.tzinfo is None:
            raise ValueError("demo start requires timezone")

    def describe(self):
        return {"name": "demo", "mode": "demo", "symbols": list(self.symbols), "source_scope": "synthetic_not_market_data",
                "capabilities": ["bars", "quotes", "prior_session", "daily_atr14", "liquidity_history", "sync"],
                "unavailable": ["classified_trades", "depth", "news", "exact_bar_notional"],
                "arrival_order": "deterministic_synthetic", "market_validation": "not_run", "sessions": self.sessions}

    def events(self, stop_event=None):
        index = 0
        bases = {s: 100.0 + 20 * n for n, s in enumerate(self.symbols)}
        op = self.start
        for session_index in range(self.sessions):
            if session_index:
                op += timedelta(days=1)
            while op.weekday() >= 5:
                op += timedelta(days=1)
            close = op + timedelta(minutes=self.minutes)
            for symbol in self.symbols:
                base = bases[symbol]
                history = []
                day = op - timedelta(days=1)
                while len(history) < 15:
                    if day.weekday() < 5:
                        history.append({"date": day.date().isoformat(), "as_of": (day.replace(hour=16, minute=0)).isoformat(),
                            "cash_open": base, "close": base + .01, "high": base + 1, "low": base - 1,
                            "volume": 3_900_000, "source": "synthetic_demo_not_market_data",
                            "closes": {(day + timedelta(minutes=m)).strftime("%H:%M"): base + .01 for m in range(1, 391)},
                            "volumes": {(day + timedelta(minutes=m)).strftime("%H:%M"): 10000 for m in range(1, 391)}})
                    day -= timedelta(days=1)
                history.reverse()
                index += 1
                if pause(stop_event, self.pace_seconds):
                    return
                yield MarketEvent.from_dict({"event_id": f"DEMO-{session_index}-{symbol}-session", "symbol": symbol,
                    "venue": "SYNTHETIC", "kind": "session", "event_time": (op - timedelta(hours=1)).isoformat(),
                    "arrival_time": (op - timedelta(hours=1)).isoformat(), "payload": {"open_time": op.isoformat(),
                    "close_time": close.isoformat(), "asset_class": "ETF" if symbol in {"SPY", "QQQ"} else "equity",
                    "tick_size": .01, "history": history, "source_scope": "synthetic_not_market_data",
                    "references": {"prev_close": {"value": base + .01, "as_of": history[-1]["as_of"], "source": "synthetic_demo"}}}})
            previous = dict(bases)
            for minute in range(1, self.minutes + 1):
                t = op + timedelta(minutes=minute)
                quotes = []
                for symbol in self.symbols:
                    base, last = bases[symbol], previous[symbol]
                    # Authored compression, trap and trend examples; never market evidence.
                    if minute <= 8:
                        o, h, l, c = base, base + .1, base - .1, base
                    elif minute == 9:
                        o, h, l, c = base, base + .4, base - .4, base + .1
                    elif minute == 10:
                        o, h, l, c = base + .1, base + .2, base - .2, base - .05
                    elif minute == 11:
                        o, h, l, c = base - .05, base + .1, base - .5, base - .35
                    elif minute == 12:
                        o, h, l, c = base - .35, base + .3, base - .4, base + .25
                    else:
                        c = base + .25 + .006 * (minute - 12)
                        o, h, l = last, max(last, c) + .03, min(last, c) - .03
                    previous[symbol] = c
                    index += 1
                    if pause(stop_event, self.pace_seconds):
                        return
                    yield MarketEvent.from_dict({"event_id": f"DEMO-{session_index}-{symbol}-B-{minute}", "symbol": symbol,
                        "venue": "SYNTHETIC", "kind": "bar", "event_time": t.isoformat(), "arrival_time": t.isoformat(),
                        "payload": {"open": o, "high": h, "low": l, "close": c, "volume": 10000,
                                    "seconds": 60, "closed": True, "source_scope": "synthetic_not_market_data"}})
                    for step in (1, 2):
                        qt = t + timedelta(milliseconds=100 * step)
                        index += 1
                        quotes.append(MarketEvent.from_dict({"event_id": f"DEMO-{session_index}-{symbol}-Q-{minute}-{step}",
                            "symbol": symbol, "venue": "SYNTHETIC", "kind": "quote", "event_time": qt.isoformat(),
                            "arrival_time": qt.isoformat(), "payload": {"bid": round(c - .01, 6), "ask": round(c + .01, 6),
                            "source_scope": "synthetic_not_market_data"}}))
                for quote in sorted(quotes, key=lambda q: q.arrival_time):
                    if pause(stop_event, self.pace_seconds):
                        return
                    yield quote
