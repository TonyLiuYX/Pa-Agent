"""Portable CSV adapter. Vendor SDK integration is deliberately outside core."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from .data import read_events
from .models import MarketEvent


def convert_bars(csv_path, calendar_path, output):
    rows = list(read_events(calendar_path))
    if any(row.kind != "session" for row in rows):
        raise ValueError("calendar JSONL must contain only session events")
    with open(csv_path, encoding="utf-8-sig", newline="") as handle:
        for index, row in enumerate(csv.DictReader(handle), 1):
            payload = {k: float(row[k]) for k in ("open", "high", "low", "close", "volume")}
            payload.update(seconds=int(row.get("seconds") or 60), closed=True)
            if row.get("notional"):
                payload["notional"] = float(row["notional"])
            event = MarketEvent.from_dict(dict(event_id=row.get("event_id") or f"CSV-{index}",
                symbol=row["symbol"], venue=row["venue"], kind="bar", event_time=row["event_time"],
                arrival_time=row.get("arrival_time") or row["event_time"], payload=payload,
                quality=row.get("quality") or "complete"))
            rows.append(event)
    # Stable equal-arrival ordering: calendar before bars; otherwise retain source
    # order. Online adapters must supply the actually observed arrival time.
    rows.sort(key=lambda e: (e.arrival_time, e.kind != "session"))
    path = Path(output)
    if path.exists():
        raise FileExistsError("use a new adapter output file")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r.to_dict(), ensure_ascii=False, allow_nan=False) + "\n" for r in rows), encoding="utf-8")
    return {"events": len(rows), "arrival_time_note": "CSV without arrival_time uses event-time replay proxy; not live-latency validation"}
