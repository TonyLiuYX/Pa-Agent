"""Synthetic, explicitly labelled data. This is NOT historical market evidence."""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
from pathlib import Path


def create_demo(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    ny = ZoneInfo("America/New_York")
    op = datetime(2026, 9, 30, 9, 30, tzinfo=ny)
    close = op + timedelta(minutes=390)
    history = []
    day = op - timedelta(days=1)
    while len(history) < 14:
        if day.weekday() < 5:
            history.append({"date": day.date().isoformat(), "as_of": (day + timedelta(minutes=390)).isoformat(),
                "cash_open": 10., "close": 10.01, "high": 10.1, "low": 9.9, "volume": 3900000,
                "source": "synthetic_demo_not_market_data",
                "closes": {(day + timedelta(minutes=m)).strftime("%H:%M"): 10.01 for m in range(1, 391)},
                "volumes": {(day + timedelta(minutes=m)).strftime("%H:%M"): 10000 for m in range(1, 391)}})
        day -= timedelta(days=1)
    history.reverse()
    def row(kind, time, payload, suffix):
        return dict(event_id="SYN-" + suffix, symbol="SYNTH", venue="SYNTHETIC", kind=kind,
                    event_time=time.isoformat(), arrival_time=time.isoformat(), quality="complete", payload=payload)
    session = row("session", op - timedelta(hours=1), {"open_time": op.isoformat(), "close_time": close.isoformat(),
        "asset_class": "equity", "tick_size": .01, "history": history,
        "references": {"prev_close": {"value": 10.01, "as_of": history[-1]["as_of"], "source": "synthetic_demo"}}}, "session")
    values = [(10., 10.1, 9.9, 10.)] * 8
    values += [(10., 10.4, 9.6, 10.1), (10.1, 10.2, 9.8, 9.95),
               (9.95, 10.1, 9.5, 9.65), (9.65, 10.3, 9.6, 10.25)]
    previous = 10.25
    for i in range(12, 390):
        price = 10.25 + .002 * (i - 12)
        values.append((previous, max(previous, price) + .03, min(previous, price) - .03, price))
        previous = price
    events = [session]
    for i, (o, h, l, c) in enumerate(values, 1):
        time = op + timedelta(minutes=i)
        events.append(row("bar", time, dict(open=o, high=h, low=l, close=c, volume=10000,
                        notional=c * 10000, seconds=60, closed=True), str(i)))
    labels = [dict(case_id="SYN-HIKKAKE-1", setup_id="SET-0005", variant_id="base", symbol="SYNTH", venue="SYNTHETIC",
        direction=1, positive=True, start=(op + timedelta(minutes=10)).isoformat(), end=(op + timedelta(minutes=14)).isoformat(),
        trigger_time=(op + timedelta(minutes=12)).isoformat(), group_id="SYNTHETIC-DAY-2026-09-30", split="test",
        source_kind="synthetic", source="hand-authored OHLC trap example", regime="trap"),
        dict(case_id="SYN-LAST-HALF-1", setup_id="SET-0049", variant_id="base", symbol="SYNTH", venue="SYNTHETIC",
        direction=1, positive=True, start=(op + timedelta(minutes=360)).isoformat(), end=(op + timedelta(minutes=361)).isoformat(),
        trigger_time=(op + timedelta(minutes=360)).isoformat(), group_id="SYNTHETIC-DAY-2026-09-30", split="test",
        source_kind="synthetic", source="fixed-clock synthetic day", regime="clock")]
    for name, rows in (("events.jsonl", events), ("labels.jsonl", labels)):
        path = directory / name
        if path.exists():
            raise FileExistsError(f"refusing to replace existing demo: {path}")
        path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")
    return {"events": len(events), "labels": len(labels), "data_kind": "synthetic", "directory": str(directory.resolve())}
