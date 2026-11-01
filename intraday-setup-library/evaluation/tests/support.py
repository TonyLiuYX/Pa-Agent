from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from recognition.models import MarketEvent

OPEN = datetime(2026, 9, 30, 9, 30, tzinfo=ZoneInfo("America/New_York"))


def event(kind, time, payload, event_id=None, symbol="SYN", venue="SYN", arrival=None, quality="complete"):
    return MarketEvent.from_dict(dict(event_id=event_id or kind + time.isoformat(), symbol=symbol, venue=venue,
        kind=kind, event_time=time.isoformat(), arrival_time=(arrival or time).isoformat(), payload=payload, quality=quality))


def session(extra=None, close=None, symbol="SYN", op=OPEN):
    return event("session", op - timedelta(hours=2), {"open_time": op.isoformat(),
        "close_time": (close or op + timedelta(minutes=390)).isoformat(), "tick_size": .01,
        "asset_class": "equity", **(extra or {})}, symbol=symbol)


def reference(value):
    return {"value": value, "as_of": (OPEN - timedelta(days=1)).isoformat(), "source": "synthetic_unit_fixture"}


def bar(minute, o, h, l, c, volume=10000, *, seconds=60, symbol="SYN", op=OPEN, notional=True, arrival=None):
    p = dict(open=o, high=h, low=l, close=c, volume=volume, seconds=seconds, closed=True)
    if notional:
        p["notional"] = c * volume
    return event("bar", op + timedelta(minutes=minute), p, symbol=symbol, arrival=arrival)


def history14(clock="10:00", amplitude=.01):
    rows = []
    for i in range(14, 0, -1):
        day = OPEN - timedelta(days=i)
        rows.append(dict(as_of=(day + timedelta(minutes=390)).isoformat(), source="synthetic_history",
                         cash_open=100., close=100., high=101., low=99., volume=3900000.,
                         closes={clock: 100 * (1 + amplitude)}, volumes={clock: 10000.}))
    return rows
