"""Incremental, venue-specific event storage and exact closed-bar aggregation."""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import date, timedelta
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo
from .models import Bar, MarketEvent, timestamp, finite
from .numeric import SwingTracker, volatility, value_area
from statistics import median
from .config import RecognitionConfig

SCALES = (1, 5, 10, 60, 180, 300, 900)
NY = ZoneInfo("America/New_York")


@dataclass
class SymbolData:
    symbol: str
    venue: str
    session: dict = field(default_factory=dict)
    bars: dict = field(default_factory=lambda: defaultdict(list))
    buckets: dict = field(default_factory=dict)
    trades: deque = field(default_factory=deque)
    depths: deque = field(default_factory=deque)
    quotes: deque = field(default_factory=deque)
    news: list = field(default_factory=list)
    consensus: dict = field(default_factory=dict)
    history: list = field(default_factory=list)
    profiles: dict = field(default_factory=lambda: defaultdict(float))
    tpo: dict = field(default_factory=lambda: defaultdict(set))
    bad: set = field(default_factory=set)
    sequences: dict = field(default_factory=dict)
    watermark: object = None
    halted: bool = False
    swings: dict = field(default_factory=dict)
    delta_samples: dict = field(default_factory=lambda: defaultdict(list))
    cvd: float = 0
    cvd_valid: bool = True
    config: RecognitionConfig = field(default_factory=RecognitionConfig)
    pattern_memory: dict = field(default_factory=dict)
    profile_samples: list = field(default_factory=list)
    last_disruption: dict = field(default_factory=dict)
    trade_cash_notional: float = 0
    trade_cash_volume: float = 0
    vwap_samples: dict = field(default_factory=lambda: defaultdict(list))
    cash_open_value: float | None = None
    cash_open_bar_value: float | None = None
    truncated_scales: set = field(default_factory=set)
    premarket_truncated: bool = False

    @property
    def open_time(self):
        return timestamp(self.session["open_time"])

    @property
    def close_time(self):
        return timestamp(self.session["close_time"])

    @property
    def tick(self):
        return self.session.get("tick_size", .01)

    def complete_history(self, count=None):
        """Known complete suffix ending at the actual previous trading session.

        Legacy inputs without an explicit calendar retain their historical
        contract. Calendar-aware adapters cannot bridge a missing session with
        an older observation, even when the older data remains useful elsewhere.
        """
        if count is not None and (type(count) is not int or count <= 0):
            raise ValueError("history count must be a positive integer")
        if "history_calendar" not in self.session:
            return self.history[-count:] if count else list(self.history)
        rows = {timestamp(h["as_of"]): h for h in self.history if h.get("complete") is True}
        suffix = []
        for calendar in reversed(self.session["history_calendar"]):
            summary = rows.get(timestamp(calendar["close_time"]))
            if not summary or summary.get("date", timestamp(summary["as_of"]).astimezone(NY).date().isoformat()) != calendar["date"]:
                break
            suffix.append(summary)
            if count and len(suffix) == count:
                break
        return list(reversed(suffix))

    def reference(self, name, default=None):
        aliases = {"prev_high": "high", "prev_low": "low", "prev_close": "close",
                   "prior_volume_profile": "volume_profile", "prior_tpo": "tpo"}
        historical_names = {*aliases, "prev_close_location", "average_daily_volume14", "atr14", "historical_vwap", "daily_direction"}
        ref = self.session.get("references", {}).get(name)
        # With a declared calendar, historical references must be derived from
        # its complete sessions. An old explicit reference must not bypass a gap.
        if isinstance(ref, dict) and not ("history_calendar" in self.session and name in historical_names):
            return ref.get("value", default)
        history = self.complete_history()
        if history:
            last = history[-1]
            if name in aliases:
                return last.get(aliases[name], default)
            if name == "prev_close_location" and last.get("high", 0) > last.get("low", 0):
                return (last["close"] - last["low"]) / (last["high"] - last["low"])
            if name == "average_daily_volume14" and len(history) >= 14 and all("volume" in h for h in history[-14:]):
                return sum(h["volume"] for h in history[-14:]) / 14
            if name == "atr14" and len(history) >= 15 and all(all(k in h for k in ("high", "low", "close")) for h in history[-15:]):
                return sum(max(z["high"] - z["low"], abs(z["high"] - a["close"]), abs(z["low"] - a["close"]))
                           for a, z in zip(history[-15:-1], history[-14:])) / 14
            if name == "historical_vwap":
                eligible = [h for h in history[-20:] if h.get("vwap") is not None and h.get("volume") is not None]
                if eligible:
                    return max(eligible, key=lambda h: (h["volume"], h["as_of"]))["vwap"]
            if name == "daily_direction" and len(history) >= 5 and all("close" in h for h in history[-5:]):
                change = history[-1]["close"] - history[-5]["close"]
                return 1 if change > 0 else -1 if change < 0 else 0
        return default

    def save_history(self):
        cash = [b for b in self.bars[60] if self.open_time < b.end <= self.close_time]
        expected = int((self.close_time - self.open_time).total_seconds() / 60)
        bar_broken = any(k in self.last_disruption and self.last_disruption[k] >= self.open_time for k in ("bar", "all"))
        if len(cash) != expected or bar_broken or any(b.quality != "complete" for b in cash):
            return
        summary = {"date": self.open_time.date().isoformat(), "as_of": self.close_time.isoformat(), "complete": True,
                             "source": "causal_replay:" + self.venue,
                             "cash_open": cash[0].open, "close": cash[-1].close,
                             "high": max(b.high for b in cash), "low": min(b.low for b in cash),
                             "volume": sum(b.volume for b in cash),
                             "closes": {b.end.isoformat(): b.close for b in cash},
                             "volumes": {b.end.isoformat(): b.volume for b in cash}}
        if summary["volume"] > 0 and all(b.notional is not None for b in cash):
            summary["vwap"] = sum(b.notional for b in cash) / summary["volume"]
        coverage = self.session.get("coverage", {}).get("trades", {})
        trade_broken = any(k in self.last_disruption and self.last_disruption[k] >= self.open_time for k in ("trade", "all"))
        if self.profiles and coverage.get("complete") and timestamp(coverage["start_time"]) <= self.open_time and not trade_broken:
            va = value_area(dict(self.profiles))
            lo, hi, poc = [k * self.tick for k in va]
            total = sum(self.profiles.values())
            summary["volume_profile"] = {"kind": "volume", "venue": self.venue, "val": lo, "vah": hi, "poc": poc,
                "low": min(self.profiles) * self.tick, "high": max(self.profiles) * self.tick,
                "prominence": self.profiles[round(poc / self.tick)] / max(1, median(self.profiles.values())),
                "lower_tail_fraction": sum(v for k, v in self.profiles.items() if k * self.tick < lo) / total,
                "upper_tail_fraction": sum(v for k, v in self.profiles.items() if k * self.tick > hi) / total}
            weights = defaultdict(float)
            for prices in self.tpo.values():
                for k in prices:
                    weights[k] += 1
            tva = value_area(dict(weights))
            if tva:
                tl, th, tpc = [k * self.tick for k in tva]
                summary["tpo"] = {"kind": "tpo", "venue": self.venue, "val": tl, "vah": th, "poc": tpc}
        self.history.append(summary)
        self.history = self.history[-30:]

    def update(self, event: MarketEvent) -> list[tuple[int, Bar]]:
        p = event.payload
        if event.kind == "session":
            if self.session:
                self.save_history()
            if "history_calendar" in p:
                calendar = p["history_calendar"]
                if not isinstance(calendar, list) or len(calendar) > 30:
                    raise ValueError("history_calendar must be a list of at most 30 completed sessions")
                previous_close = None
                days = set()
                for row in calendar:
                    if not isinstance(row, dict) or not isinstance(row.get("source"), str) or not row["source"].strip():
                        raise ValueError("history_calendar requires explicit calendar provenance")
                    day = row.get("date")
                    if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
                        raise ValueError("history_calendar date must be an ISO date")
                    op, cl = timestamp(row["open_time"]), timestamp(row["close_time"])
                    if (op >= cl or cl >= timestamp(p["open_time"]) or day in days
                            or op.astimezone(NY).date().isoformat() != day or cl.astimezone(NY).date().isoformat() != day
                            or previous_close is not None and op <= previous_close):
                        raise ValueError("history_calendar must contain distinct ascending completed trading sessions")
                    days.add(day)
                    previous_close = cl
            for ref in p.get("references", {}).values():
                if not isinstance(ref, dict) or not ref.get("source") or not ref.get("as_of"):
                    raise ValueError("historical references need value, source and as_of")
                if timestamp(ref["as_of"]) >= timestamp(p["open_time"]):
                    raise ValueError("historical references must be known before cash open")
                if "value" not in ref:
                    raise ValueError("reference lacks value")
                if isinstance(ref["value"], (int, float)):
                    finite(ref["value"], "reference.value")
            days = set()
            last_history_time = None
            if "history" in p and not isinstance(p["history"], list):
                raise ValueError("history must be a list")
            for h in p.get("history", []):
                if not h.get("as_of") or timestamp(h["as_of"]) >= timestamp(p["open_time"]):
                    raise ValueError("history must precede current session")
                if not h.get("source"):
                    raise ValueError("external historical summary requires provenance")
                if "complete" in h and type(h["complete"]) is not bool:
                    raise ValueError("history.complete must be boolean")
                as_of = timestamp(h["as_of"])
                if as_of.date() in days or (last_history_time and as_of <= last_history_time):
                    raise ValueError("history must have distinct, ascending sessions")
                days.add(as_of.date())
                last_history_time = as_of
                finite(h["cash_open"], "history.cash_open", True)
                for clock, price in h.get("closes", {}).items():
                    finite(price, "history.close", True)
                    if ":" in clock and len(clock) != 5 and timestamp(clock) > as_of:
                        raise ValueError("historical point exceeds its as_of cutoff")
            self.session = dict(p)
            self.session["session_event_id"] = event.event_id
            if "history" in p:
                self.history = list(p["history"])[-30:]
            self.bars.clear()
            self.buckets.clear()
            self.trades.clear()
            self.depths.clear()
            self.quotes.clear()
            self.profiles.clear()
            self.tpo.clear()
            self.bad.clear()
            self.sequences.clear()
            self.watermark = None
            # A calendar boundary does not establish a venue trading resume.
            # Preserve an observed halt until an explicit venue status clears it.
            self.swings.clear()
            self.delta_samples.clear()
            self.cvd = 0
            self.cvd_valid = True
            self.pattern_memory.clear()
            self.profile_samples.clear()
            self.last_disruption.clear()
            self.trade_cash_notional = self.trade_cash_volume = 0
            self.vwap_samples.clear()
            self.cash_open_value = None
            self.cash_open_bar_value = None
            self.truncated_scales.clear()
            self.premarket_truncated = False
            return []
        if not self.session:
            raise ValueError("a session/calendar event must precede market data")
        if event.quality != "complete":
            self.bad.add("partial_input")
            self.last_disruption[event.kind] = event.arrival_time
            if event.kind == "trade":
                self.cvd_valid = False
        seq = p.get("sequence")
        if seq is not None:
            previous = self.sequences.get(event.kind)
            if previous is not None and seq != previous + 1:
                self.bad.add("sequence_gap_" + event.kind)
                self.last_disruption[event.kind] = event.arrival_time
                if event.kind == "trade":
                    self.cvd_valid = False
            self.sequences[event.kind] = seq
        if event.kind == "status":
            if p.get("halted") is True:
                self.halted = True
            elif (p.get("halted") is False and "connected" not in p
                  and "connection_state" not in p and not p.get("clock")
                  and not p.get("correction") and not p.get("gap")):
                self.halted = False
            if p.get("gap"):
                self.bad.add("adapter_gap")
                self.last_disruption[p.get("stream", "all")] = event.arrival_time
                if p.get("stream", "all") in {"trade", "all"}:
                    self.cvd_valid = False
            return []
        if event.kind == "consensus":
            if p["event_key"] in self.consensus:
                raise ValueError("consensus branches are immutable within a replay")
            self.consensus[p["event_key"]] = event
            return []
        if event.kind == "news":
            if not any(n.event_id == event.event_id for n in self.news):
                self.news.append(event)
            return []
        if self.watermark is not None and event.event_time < self.watermark:
            self.bad.add("late_market_data")
            self.last_disruption[event.kind] = event.arrival_time
            if event.kind == "trade":
                self.cvd_valid = False
            return []  # no retroactive insertion or rewriting prior decisions
        if event.kind == "trade":
            if self.trades and event.event_time < self.trades[-1].event_time:
                self.bad.add("out_of_order_trade")
                self.last_disruption["trade"] = event.arrival_time
                self.cvd_valid = False
                return []
            self.trades.append(event)
            if self.open_time <= event.event_time < self.close_time:
                if self.cash_open_value is None:
                    self.cash_open_value = p["price"]
                k = round(p["price"] / self.tick)
                self.profiles[k] += p["size"]
                self.trade_cash_notional += p["price"] * p["size"]
                self.trade_cash_volume += p["size"]
                if p["side"] == "unknown":
                    self.cvd_valid = False
                else:
                    self.cvd += p["size"] * (1 if p["side"] == "buy" else -1)
                bracket = int((event.event_time - self.open_time).total_seconds() // 1800)
                self.tpo[bracket].add(k)
            self._prune(event.event_time)
            return []
        if event.kind == "depth":
            if self.depths and event.event_time < self.depths[-1].event_time:
                self.last_disruption["depth"] = event.arrival_time
                self.bad.add("out_of_order_depth")
                return []
            self.depths.append(event)
            self._prune(event.event_time)
            return []
        if event.kind == "quote":
            if self.quotes and event.event_time < self.quotes[-1].event_time:
                self.last_disruption["quote"] = event.arrival_time
                self.bad.add("out_of_order_quote")
                return []
            self.quotes.append(event)
            self._prune(event.event_time)
            return []
        start = event.event_time - timedelta(seconds=p["seconds"])
        if start == self.open_time and self.cash_open_bar_value is None:
            self.cash_open_bar_value = p["open"]
        self.watermark = event.event_time
        base = Bar(start, event.event_time, p["open"], p["high"], p["low"], p["close"],
                   p["volume"], p.get("notional"), (event.event_id,), event.quality)
        closed = []
        for seconds in SCALES:
            if p["seconds"] > seconds or seconds % p["seconds"]:
                continue
            offset = int((start - self.open_time).total_seconds())
            if offset % p["seconds"]:
                self.bad.add("unaligned_bar")
                continue
            key = offset // seconds
            bucket_key = (seconds, key)
            bucket = self.buckets.setdefault(bucket_key, [])
            if bucket and bucket[-1].end != start:
                self.bad.add("bar_gap")
                self.last_disruption["bar"] = event.arrival_time
            bucket.append(base)
            end_offset = int((event.event_time - self.open_time).total_seconds())
            if end_offset % seconds:
                continue
            expected = self.open_time + timedelta(seconds=key * seconds)
            complete = bucket[0].start == expected and sum((b.end - b.start).total_seconds() for b in bucket) == seconds
            good = complete and all(b.quality == "complete" for b in bucket)
            aggregated = Bar(bucket[0].start, bucket[-1].end, bucket[0].open,
                             max(b.high for b in bucket), min(b.low for b in bucket), bucket[-1].close,
                             sum(b.volume for b in bucket),
                             sum(b.notional for b in bucket) if all(b.notional is not None for b in bucket) else None,
                             tuple(i for b in bucket for i in b.source_ids), "complete" if good else "partial")
            if not complete:
                self.bad.add("incomplete_aggregation")
            last = self.bars[seconds][-1] if self.bars[seconds] else None
            if last and aggregated.start != last.end and aggregated.start != self.open_time:
                self.bad.add("bar_gap")
                self.last_disruption["bar"] = event.arrival_time
            # Previous volatility only; existing pivots cannot be reclassified
            # when the next bar raises or lowers the noise scale.
            reversal_scale = volatility(self.bars[seconds], self.tick)
            for factor in self.config.swing_factors:
                tracker = self.swings.setdefault((seconds, factor), SwingTracker(factor))
                tracker.push(aggregated, reversal_scale)
            self.bars[seconds].append(aggregated)
            self.delta_samples[seconds].append((aggregated, self.cvd_at(aggregated.end)))
            self.delta_samples[seconds] = self.delta_samples[seconds][-3000:]
            if seconds == 60 and self.profiles:
                va = value_area(self.profile_at(aggregated.end))
                if va:
                    self.profile_samples.append((aggregated.end, tuple(k * self.tick for k in va)))
            if len(self.bars[seconds]) > 3000:
                self.note_bar_truncation(seconds, self.bars[seconds][:-3000])
                self.bars[seconds] = self.bars[seconds][-3000:]
            del self.buckets[bucket_key]
            closed.append((seconds, aggregated))
        return closed

    def note_bar_truncation(self, seconds, removed):
        self.truncated_scales.add(seconds)
        if seconds == 60 and any(b.end <= self.open_time for b in removed):
            self.premarket_truncated = True
        if seconds == 60:
            self.bad.add("cash_bar_cache_truncated")
            self.last_disruption["bar"] = removed[-1].end

    def cvd_at(self, cutoff):
        if not self.cvd_valid:
            return None
        future = []
        for e in reversed(self.trades):
            if e.event_time <= cutoff:
                break
            if self.open_time <= e.event_time < self.close_time:
                future.append(e)
        if future and self.trades[0].event_time > cutoff:
            return None
        return self.cvd - sum(e.payload["size"] * (1 if e.payload["side"] == "buy" else -1) for e in future)

    def profile_at(self, cutoff):
        profile = dict(self.profiles)
        for e in reversed(self.trades):
            if e.event_time <= cutoff:
                break
            if self.open_time <= e.event_time < self.close_time:
                key = round(e.payload["price"] / self.tick)
                profile[key] -= e.payload["size"]
        if self.trades and self.trades[0].event_time > cutoff:
            return {}
        return {k: v for k, v in profile.items() if v > 0}

    def _prune(self, now):
        for stream in (self.trades, self.depths, self.quotes):
            while stream and (now - stream[0].event_time).total_seconds() > 3600:
                stream.popleft()


def read_events(path):
    import json
    with open(path, encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                yield MarketEvent.from_dict(json.loads(line))
            except (ValueError, TypeError, KeyError) as exc:
                raise ValueError(f"{path}:{number}: {exc}") from exc
