"""Causal numerical features; unavailable observations remain None, not False."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from copy import copy
from datetime import timedelta
from statistics import median
from zoneinfo import ZoneInfo
import math

from .data import SymbolData
from .models import Bar, timestamp
from .numeric import clamp, diagonal_zones, efficiency, overlap, pivots, ramp, value_area, volatility

NY = ZoneInfo("America/New_York")


@dataclass
class Frame:
    symbol: str
    venue: str
    seconds: int
    bar: Bar
    arrived_at: object
    values: dict[int, dict]
    capabilities: set[str]
    quality: list[str]
    sources: list[str]
    market: str
    observation_kind: str = "bar"
    key_hypotheses: dict = field(default_factory=dict)


def event_window(stream, start, end, arrival):
    """Streams are monotonic; examine only the required suffix, not the full hour."""
    out = []
    for e in reversed(stream):
        if e.event_time < start:
            break
        if e.event_time <= end and e.arrival_time <= arrival:
            out.append(e)
    return list(reversed(out))


def _history_at(history, hhmm, key="closes"):
    out = []
    for h in history:
        points = h.get(key, {})
        point = points.get(hhmm)
        if point is None:
            point = next((v for t, v in points.items() if len(t) > 8 and timestamp(t).astimezone(NY).strftime("%H:%M:%S" if len(hhmm) == 8 else "%H:%M") == hhmm), None)
        if point is not None:
            out.append((h, point))
    return out


def _coverage(data, kind, now, lookback=None):
    row = data.session.get("coverage", {}).get(kind, {})
    stream = "trade" if kind in {"trades", "classified_trades"} else kind
    breaks = [data.last_disruption[k] for k in (stream, "all") if k in data.last_disruption and data.last_disruption[k] >= data.open_time]
    intact = not breaks if lookback is None else all(t < now - timedelta(seconds=lookback) for t in breaks)
    return bool(row.get("complete") is True and row.get("start_time")
                and timestamp(row["start_time"]) <= data.open_time and intact
                and (not row.get("end_time") or timestamp(row["end_time"]) >= now))


def _contiguous_complete(bars):
    return bool(bars and all(b.quality == "complete" for b in bars)
                and all(a.end == z.start for a, z in zip(bars, bars[1:])))


def _weak_opening_drop(bars, data, vol, tol):
    # Two shallow opening candles already establish an observable weak decline.
    # A two-close efficiency is always +/-1, so it cannot distinguish a shallow
    # early decline from an established bear trend by itself.
    opening = [x for x in bars[:-1] if x.start >= data.open_time]
    if bars[-1].close < bars[-1].open:
        opening.append(bars[-1])
    if len(opening) < 2 or opening[0].start != data.open_time or not _contiguous_complete(opening):
        return None
    sample = opening[-12:]
    loss = opening[0].open - sample[-1].close
    if len(opening) < 5:
        scale = max(vol, median(x.high - x.low for x in sample), data.tick)
        return tol < loss <= scale and all(x.open - x.close < .6 * scale for x in sample)
    return -.6 < efficiency(sample) < 0


def _tpo_edge_cluster(edges, direction):
    """At least three edges within one tick of a fixed center; no chained drift."""
    candidates = []
    for center in sorted(set(edges)):
        cluster = [edge for edge in edges if abs(edge - center) <= 1]
        candidates.append((len(cluster), center, cluster))
    if not candidates:
        return None, 0
    count, _, cluster = max(candidates, key=lambda row: (row[0], -row[1]))
    # Cross the whole band, rather than treating a close inside its tick noise
    # as acceptance beyond the ledge.
    return (max(cluster) if direction == 1 else min(cluster)), count


def make_frame(data: SymbolData, seconds: int, bar: Bar, arrival, universe: dict) -> Frame:
    # Freeze tick aggregates to THIS cutoff even when a closed bar arrives after
    # later ticks. Never use the then-current full-day VP/CVD as an earlier VP.
    future = []
    for e in reversed(data.trades):
        if e.event_time <= bar.end:
            break
        future.append(e)
    if future:
        view = copy(data)
        view.profiles = dict(data.profiles)
        for e in future:
            if data.open_time <= e.event_time < data.close_time:
                k = round(e.payload["price"] / data.tick)
                view.profiles[k] -= e.payload["size"]
                view.trade_cash_volume -= e.payload["size"]
                view.trade_cash_notional -= e.payload["price"] * e.payload["size"]
                if e.payload["side"] != "unknown":
                    view.cvd -= e.payload["size"] * (1 if e.payload["side"] == "buy" else -1)
        view.profiles = {k: v for k, v in view.profiles.items() if v > 0}
        if data.trades[0].event_time > bar.end:
            view.last_disruption = dict(data.last_disruption, trade=bar.end)
            view.cvd_valid = False
        data = view
    bars = [x for x in data.bars[seconds] if x.end <= bar.end]
    recent = bars[-20:]
    prior = bars[-21:-1]
    tick = data.tick
    vol = volatility(prior, tick)
    valid_quotes = event_window(data.quotes, bar.end - timedelta(seconds=30), bar.end, arrival)
    quote = valid_quotes[-1].payload if valid_quotes else None
    spread = quote["ask"] - quote["bid"] if quote else tick
    tolerance = max(tick, spread, data.config.location_noise_fraction * vol)
    quality = {"partial_bar"} if bar.quality != "complete" else set()
    cash_recent = [x for x in recent if x.start >= data.open_time]
    if any(x.quality != "complete" for x in cash_recent) or any(a.end != z.start for a, z in zip(cash_recent, cash_recent[1:])):
        quality.add("context_bar_gap")
    if any(k in data.last_disruption and data.last_disruption[k] >= max(recent[0].start, data.open_time) for k in ("bar", "all")):
        quality.add("context_data_disruption")
    if data.halted:
        quality.add("halted")
    quality = sorted(quality)
    if len(prior) < 5:
        quality.append("warmup")
    cash = [b for b in data.bars[60] if data.open_time < b.end <= min(bar.end, data.close_time)]
    pm = [b for b in data.bars[60] if b.end <= data.open_time]
    elapsed = (bar.end - data.open_time).total_seconds()
    full_day = (data.close_time - data.open_time).total_seconds() == 390 * 60 and data.open_time.astimezone(NY).strftime("%H:%M") == "09:30" and data.close_time.astimezone(NY).strftime("%H:%M") == "16:00"
    hhmm = bar.end.astimezone(NY).strftime("%H:%M")
    history = data.complete_history()
    prev_high = data.reference("prev_high")
    prev_low = data.reference("prev_low")
    prev_close = data.reference("prev_close")
    cash_open = data.cash_open_bar_value if data.cash_open_bar_value is not None else cash[0].open if cash else data.cash_open_value
    caps = {"bars"}
    if prev_high is not None and prev_low is not None and prev_close is not None:
        caps.add("prior_session")
    if prev_close is not None:
        caps.add("previous_close")
    if pm and data.session.get("premarket_complete") is True and not data.premarket_truncated:
        caps.add("premarket")
    if data.reference("atr14") is not None:
        caps.add("daily_atr14")
    if data.reference("average_daily_volume14") is not None:
        caps.add("liquidity_history")
    if data.reference("historical_vwap") is not None:
        caps.add("historical_vwap")
    if data.reference("float_shares") is not None:
        caps.add("equity_metadata")
    if data.reference("daily_direction") is not None:
        caps.add("daily_context")
    if seconds <= 10:
        caps.add("short_bars")
    cash_volume = sum(b.volume for b in cash)
    exact_vwap = bool(cash and cash[0].start == data.open_time and _contiguous_complete(cash)
                      and cash_volume > 0 and all(b.notional is not None for b in cash)
                      and not any(k in data.last_disruption and data.last_disruption[k] >= data.open_time for k in ("bar", "all")))
    vwap = sum(b.notional for b in cash) / cash_volume if exact_vwap else None
    if data.trade_cash_volume > 0 and _coverage(data, "trades", bar.end):
        vwap = data.trade_cash_notional / data.trade_cash_volume
        exact_vwap = True
    if exact_vwap:
        caps.add("exact_vwap")
    vp = data.reference("prior_volume_profile")
    tp = data.reference("prior_tpo")
    vp = vp if vp and vp.get("kind") == "volume" and vp.get("venue") == data.venue else None
    tp = tp if tp and tp.get("kind") == "tpo" and tp.get("venue") == data.venue else None
    if vp:
        caps.add("prior_volume_profile")
    if tp:
        caps.add("prior_tpo")
    flow_seconds = seconds if seconds else max(.001, (bar.end - bar.start).total_seconds())
    trades = event_window(data.trades, bar.start - timedelta(seconds=flow_seconds), bar.end, arrival)
    fresh_tape = trades and (bar.end - trades[-1].event_time).total_seconds() <= max(5, flow_seconds)
    local_tape = trades
    classified = bool(fresh_tape and all(t.payload["side"] != "unknown" for t in local_tape)
                      and _coverage(data, "classified_trades", bar.end, 2 * flow_seconds))
    if classified:
        caps.add("classified_trades")
    if fresh_tape and _coverage(data, "trades", bar.end):
        caps.update(("volume_profile", "tpo"))
    depths = event_window(data.depths, bar.end - timedelta(seconds=max(30, 2 * flow_seconds)), bar.end, arrival)
    if depths and (bar.end - depths[-1].event_time).total_seconds() <= max(5, flow_seconds) and _coverage(data, "depth", bar.end, 2 * flow_seconds):
        caps.add("depth")
    available_news = [n for n in data.news if n.arrival_time <= arrival and n.event_time <= bar.end and n.quality == "complete"]
    if available_news:
        caps.add("news")
    if data.consensus:
        caps.add("consensus")
    mins = [b for b in cash if b.end <= data.open_time + timedelta(minutes=5)]
    orb_ready = len(mins) == 5 and mins[-1].end == data.open_time + timedelta(minutes=5) and all(b.quality == "complete" for b in mins)
    ib_minutes = data.session.get("ib_minutes", 60)
    ib = [b for b in cash if b.end <= data.open_time + timedelta(minutes=ib_minutes)]
    ib_ready = len(ib) == ib_minutes and all(b.quality == "complete" for b in ib)
    if ib_ready:
        caps.add("ib")
    same_time = _history_at(history[-14:], hhmm)
    if len(same_time) == 14:
        caps.add("same_time14")
    micro_clock = bar.end.astimezone(NY).strftime("%H:%M:%S") if seconds < 60 else hhmm
    volume_history = _history_at(history[-14:], micro_clock, "volumes")
    minute_rvol = bar.volume / median(v for _, v in volume_history) if len(volume_history) >= 5 and median(v for _, v in volume_history) > 0 else None
    # Higher aggregation RVOL compares the matching historical constituent interval.
    if seconds > 60:
        historical_sums = []
        for h in history[-14:]:
            values = []
            for offset in range(0, seconds, 60):
                clock = (bar.end - timedelta(seconds=offset)).astimezone(NY).strftime("%H:%M")
                matched = _history_at([h], clock, "volumes")
                if matched:
                    values.append(matched[0][1])
            if len(values) == seconds // 60 and seconds >= 60:
                historical_sums.append(sum(values))
        minute_rvol = bar.volume / median(historical_sums) if len(historical_sums) >= 5 and median(historical_sums) > 0 else None
    if minute_rvol is not None:
        caps.add("volume_baseline")
    latest_news = available_news[-1] if available_news else None
    previous_bar = prior[-1] if prior else bar
    pv = [(e.payload | {"time": e.event_time}) for e in trades]
    current_tape = [t for t in pv if bar.start < t["time"] <= bar.end or (bar.start == data.open_time and t["time"] == bar.start)]
    previous_tape = [t for t in pv if bar.start - timedelta(seconds=flow_seconds) < t["time"] <= bar.start
                     and not (bar.start == data.open_time and t["time"] == bar.start)]
    current_size = sum(t["size"] for t in current_tape)
    previous_size = sum(t["size"] for t in previous_tape)
    local_va = value_area(dict(data.profiles)) if "volume_profile" in caps else None
    open10 = next((b.close for b in cash if b.end == data.open_time + timedelta(minutes=30)), None)
    if open10 is not None:
        caps.add("first_half_hour")
    benchmarks = data.session.get("benchmarks", [])
    cross_states = [universe.get((s, data.venue)) for s in benchmarks]
    aligned_cross = []
    for state in cross_states:
        # Arrival order may contain a future benchmark bar before this delayed
        # stock bar. Join the benchmark history at the decision cutoff instead
        # of rejecting usable earlier observations based on its latest bar.
        available = [b for b in state.bars[seconds] if b.end <= bar.end] if state else []
        disruptions = state.last_disruption.values() if state else ()
        if (len(available) >= 5 and 0 <= (bar.end - available[-1].end).total_seconds()
                <= max(seconds, data.config.event_window_seconds * 2)
                and not any(t <= bar.end for t in disruptions)
                and _contiguous_complete(available[-5:])):
            aligned_cross.append(state)
    if benchmarks and len(aligned_cross) == len(benchmarks):
        caps.add("synced_market")
    levels = [(name, data.reference(name)) for name in ("key_level", "prev_high", "prev_low", "historical_vwap")]
    levels = [(k, v) for k, v in levels if isinstance(v, (float, int))]
    if levels:
        caps.add("key_levels")
    results, hypotheses = {}, {}
    for d in (1, -1):
        f = {"directional_price": d * bar.close, "directional_extreme": d * (bar.high if d == 1 else bar.low),
             "directional_low": d * (bar.low if d == 1 else bar.high), "tolerance": tolerance,
             "extreme": d * (min(b.low for b in recent) if d == 1 else max(b.high for b in recent)),
             "signal_edge": d * (bar.high if d == 1 else bar.low),
             "signal_risk": d * (bar.low if d == 1 else bar.high),
             "signal": d * (bar.close - bar.open) > 0 and d * (bar.close - (bar.high + bar.low) / 2) > 0,
             "opening": 0 < elapsed <= 3600, "full_day": full_day,
             "half_hour": full_day and elapsed > 0 and bar.end.astimezone(NY).minute in (0, 30)
                          and bar.end.astimezone(NY).second == 0 and bar.end < data.close_time,
             "last_half_hour": full_day and hhmm == "15:30" and bar.end.astimezone(NY).second == 0,
             "london_0800": "08:00" <= bar.end.astimezone(ZoneInfo("Europe/London")).strftime("%H:%M") <= "08:05",
             "volume_increase": bar.volume / previous_bar.volume if previous_bar.volume > 0 else None,
             "trend": clamp(d * efficiency(prior[-12:])) if len(prior) >= 5 else None,
             "counter_impulse": ramp(-d * (bar.close - bar.open) / vol, .3, 1.5),
             "impulse": ramp(d * (bar.close - bar.open) / vol, .3, 1.5),
             "balance": clamp(1 - abs(efficiency(prior[-12:]))) * overlap(prior[-12:]) if len(prior) >= 8 else None,
             "rvol": minute_rvol, "captured_retest": None,
             "parent_sweep": None, "parent_id": None,
             "orb_ready": orb_ready, "orb_liquidity": None,
             "accepted": len(prior) >= 3 and all(d * (b.close - previous_bar.open) > -tolerance for b in recent[-3:]),
             "pullback": len(prior) >= 2 and d * (bar.close - previous_bar.close) < -tolerance,
             "resume": len(prior) >= 2 and (bar.high > max(b.high for b in prior[-2:]) + tick / 2 if d == 1
                                            else bar.low < min(b.low for b in prior[-2:]) - tick / 2)
                       and d * (bar.close - bar.open) > 0,
             "recent_directional_closes": [d * x.close for x in recent[-3:]],
             "range_break_edge": d * (max(b.high for b in prior[-3:]) if d == 1 else min(b.low for b in prior[-3:])) if prior else None,
             "counter_energy": ramp(-d * (bar.close - bar.open) / vol, 0, 2) * (minute_rvol if minute_rvol is not None else 1),
             "attack_energy": None, "flow_activity": None,
             "price_path": [d * t["price"] for t in current_tape] if "volume_profile" in caps else None,
             "tick_size": tick,
             "noise_scale": vol,
             "observation_scale_seconds": seconds,
             "cash_open": d * cash_open if cash_open is not None else None,
        }
        _price_features(f, d, bars, vol, tolerance, data)
        prior_edge = prev_high if d == 1 else prev_low
        prior_entry = prev_low if d == 1 else prev_high
        f.update(prior_edge=d * prior_edge if prior_edge is not None else None,
                 prior_entry=d * prior_entry if prior_entry is not None else None,
                 prior_break=bool(prior_edge is not None and d * bar.close > d * prior_edge + tolerance),
                 prior_opposite_break=bool(prior_entry is not None and f["directional_low"] < d * prior_entry - tolerance),
                 prior_reclaim=bool(prior_entry is not None and d * bar.close > d * prior_entry + tolerance))
        news_level = data.reference("prior_acceptance_level", prior_edge)
        f.update(second_day_edge=d * news_level if isinstance(news_level, (float, int)) else None,
                 second_day_break=d * bar.close > d * news_level + tolerance if isinstance(news_level, (float, int)) else None)
        if levels:
            contexts = []
            for name, key in levels:
                tests = [b for b in prior[-15:] if b.low - tolerance <= key <= b.high + tolerance]
                rejection = [abs(b.close - key) for b in tests]
                contexts.append(dict(key=d * key, key_name=name,
                    key_near=bar.low - tolerance <= key <= bar.high + tolerance,
                    key_break=d * previous_bar.close <= d * key + tolerance < d * bar.close,
                    pressure=len(tests) >= 3 and rejection[-1] < rejection[0]
                    and f["compression"] >= .4 and d * efficiency(prior[-8:]) > 0))
            hypotheses[d] = contexts
            f.update(min(contexts, key=lambda row: abs(row["key"] - d * bar.close)))
        else:
            f.update(key=None, key_near=None, key_break=None, pressure=None)
        if orb_ready:
            orb_dir = 1 if mins[-1].close > mins[0].open else -1 if mins[-1].close < mins[0].open else 0
            edge = max(b.high for b in mins) if d == 1 else min(b.low for b in mins)
            atr = data.reference("atr14")
            f.update(orb_direction=orb_dir == d, orb_edge=d * edge,
                     orb_stop=d * edge - .1 * atr if atr else None,
                     orb_liquidity=data.reference("average_daily_volume14") is not None
                     and data.reference("average_daily_volume14") >= 1_000_000 and mins[0].open >= 5
                     and atr is not None and atr > .5)
        else:
            f.update(orb_direction=False, orb_edge=None, orb_stop=None)
        if pm:
            pmh, pml = max(b.high for b in pm), min(b.low for b in pm)
            support = data.reference("pm_support", pml)
            f.update(pm_edge=d * (pmh if d == 1 else pml), pm_support=d * support,
                     pm_wash=d == 1 and 0 < elapsed <= 3600 and bar.low <= support + tolerance and bar.close > support,
                     near_pm_edge=abs(bar.close - (pmh if d == 1 else pml)) <= 2 * vol)
            on_dir = 1 if pm[-1].close > pm[0].open else -1 if pm[-1].close < pm[0].open else 0
            on_extreme = pmh if on_dir == 1 else pml
            known_open = cash and cash[0].start == data.open_time and _contiguous_complete(cash)
            known_overnight = data.session.get("premarket_complete") is True and not data.premarket_truncated and all(x.quality == "complete" for x in pm)
            if known_open and known_overnight:
                rejected = ((bar.high >= on_extreme - tolerance if on_dir == 1 else bar.low <= on_extreme + tolerance)
                            and d * (bar.close - on_extreme) > tolerance)
                improved = (max(x.high for x in cash) > on_extreme + tolerance if on_dir == 1
                            else min(x.low for x in cash) < on_extreme - tolerance)
                not_improved = not improved and d * (bar.close - cash[0].open) > tolerance
                f["overnight_failure"] = on_dir != 0 and d == -on_dir and (rejected or not_improved)
            else:
                f["overnight_failure"] = None
            f["overnight_risk"] = d * on_extreme if known_overnight and on_dir else None
        else:
            f.update(pm_edge=None, pm_support=None, pm_wash=None, near_pm_edge=None, overnight_failure=None, overnight_risk=None)
        if ib_ready:
            ibh, ibl = max(b.high for b in ib), min(b.low for b in ib)
            edge, opposite = (ibh, ibl) if d == 1 else (ibl, ibh)
            attacks = [x for x in cash if x.start >= data.open_time + timedelta(minutes=ib_minutes)
                       and (x.high > ibh or x.low < ibl)]
            first_side = None
            if attacks:
                first = attacks[0]
                if not (first.high > ibh and first.low < ibl):
                    first_side = 1 if first.high > ibh else -1
            f.update(ib_edge=d * edge, ib_break=d * previous_bar.close <= d * edge < d * bar.close,
                     ib_first_failure=first_side == -d and d * bar.close > d * opposite + tolerance)
        else:
            f.update(ib_edge=None, ib_break=None, ib_first_failure=None)
        _vwap_features(f, d, cash, vwap, cash_open, prev_close, same_time, data, bar, vol, tolerance, hhmm)
        _profile_features(f, d, vp, tp, local_va, data, bar, prior, tolerance, vol, arrival)
        _flow_features(f, d, classified, current_tape, previous_tape, depths if "depth" in caps else [], data, bar, vol, tolerance, seconds)
        _news_features(f, d, available_news, data, bar, arrival, prev_close, cash_open)
        _cross_features(f, d, aligned_cross, benchmarks, data, bars, seconds)
        results[d] = f
    if vwap is not None:
        data.vwap_samples[seconds].append((bar.end, vwap))
        data.vwap_samples[seconds] = data.vwap_samples[seconds][-3000:]
    sources = {data.session["session_event_id"], *bar.source_ids}
    sources.update(i for b in recent for i in b.source_ids)
    sources.update(t.event_id for t in trades[-100:])
    sources.update(e.event_id for e in depths[-8:])
    sources.update(n.event_id for n in available_news[-5:])
    sources.update(c.event_id for c in data.consensus.values())
    sources.update(i for b in bars[-31:] for i in b.source_ids)
    sources.update(i for s in aligned_cross for b in s.bars[seconds][-31:] if b.end <= bar.end for i in b.source_ids)
    sources.update(i for f in results.values() for i in f.get("second_day_news_event_ids", []))
    return Frame(data.symbol, data.venue, seconds, bar, arrival, results, caps, quality, sorted(sources),
                 data.session.get("asset_class", "equity"), "event_window" if seconds == 0 else "bar", hypotheses)


def _price_features(f, d, bars, vol, tol, data):
    b, prior = bars[-1], bars[:-1]
    r = prior[-12:]
    f["compression"] = clamp(1 - (max(x.high for x in bars[-3:]) - min(x.low for x in bars[-3:])) / (4 * vol))
    f["compression_risk"] = d * (min(x.low for x in bars[-3:]) if d == 1 else max(x.high for x in bars[-3:]))
    f["trend_origin"] = d * (min(x.low for x in prior[-20:]) if d == 1 else max(x.high for x in prior[-20:])) if prior else None
    f["tight_flag_edge"] = d * (max(x.high for x in prior[-3:]) if d == 1 else min(x.low for x in prior[-3:])) if prior else None
    ema = prior[0].close if prior else None
    if ema is not None:
        for x in prior[1:]:
            ema = (2 / 21) * x.close + (19 / 21) * ema
    f["ema_near"] = b.low - tol <= ema <= b.high + tol if len(prior) >= 20 else None
    if not prior:
        for key in ("hikkake_start", "double_inside", "close_double_inside", "triple_inside", "retrace_start", "trend_bar_failure", "first_micro", "three_push", "trend_break", "old_extreme_retest", "late_flag", "flag_failure", "anti_impulse", "failed_extension", "range_edge", "weak_open_drop", "low_volume_pullback", "volume_cliff", "volume_cliff_reference_volume", "volume_cliff_drive_ratio", "volume_cliff_rvol", "shallow_bounce", "weak_flush", "drive_recovery", "slow_drift"):
            f[key] = None
        return
    p = prior[-1]
    seconds = f.get("observation_scale_seconds", int((b.end - b.start).total_seconds()))
    drive = len(prior) >= 5 and d * efficiency(prior[-5:]) > .8 and d * (prior[-1].close - prior[-5].open) >= 2 * vol
    memory = data.pattern_memory.setdefault(("micro", seconds, d), {"drive": False, "active": False, "consumed": False})
    if drive and not memory["drive"]:
        memory.update(active=True, consumed=False)
    memory["drive"] = drive
    paused = (b.high - b.low < .8 * vol and abs(b.close - p.close) < .6 * vol
              and abs(b.close - b.open) < .35 * vol)
    micro = memory["active"] and not memory["consumed"] and paused
    if micro:
        memory["consumed"] = True
    counter_depth = p.high - b.low if d == 1 else b.high - p.low
    if counter_depth >= vol:
        memory["active"] = False
    f.update(pause_edge=d * (p.high if d == 1 else p.low),
             impulse_origin=d * p.open, drive_origin=d * (data.cash_open_bar_value if data.cash_open_bar_value is not None else data.cash_open_value if data.cash_open_value is not None else bars[0].open),
             first_micro=micro,
             failed_extension=(b.low < p.low and b.close > p.low + tol) if d == 1 else (b.high > p.high and b.close < p.high - tol),
             weak_open_drop=_weak_opening_drop(bars, data, vol, tol) if d == 1 else False,
             low_volume_pullback=None, volume_cliff=None,
             shallow_bounce=abs(b.close - p.close) < .5 * vol and d * efficiency(r) > .3,
             weak_flush=d * (b.close - p.close) < 0 and b.high - b.low < .8 * vol,
             drive_recovery=(d == 1 and len(bars) >= 5 and bars[0].close < bars[0].open
                and b.close > bars[0].open) if seconds not in data.truncated_scales else None,
             slow_drift=0 < d * (b.close - p.close) < .5 * vol and d * efficiency(r) > .3)
    if f["rvol"] is not None:
        # All bars in the pullback have their own same-clock baselines.
        pull = bars[-3:]
        ratios = []
        for x in pull:
            h = _history_at(data.complete_history(14), x.end.astimezone(NY).strftime("%H:%M"), "volumes")
            if len(h) >= 5 and median(v for _, v in h) > 0:
                ratios.append(x.volume / median(v for _, v in h))
        f["low_volume_pullback"] = len(ratios) == 3 and sum(ratios) / 3 < .75 and abs(b.close - pull[0].open) > .5 * vol
    # Compare to an earlier, completed participation window as well as the
    # same-clock historical baseline. Peak three-bar median in the prior twelve
    # bars is a robust initial-push proxy; the current bar never enters it.
    baseline = None
    if len(prior) >= 3 and _contiguous_complete(prior[-12:] + [b]):
        recent_prior = prior[-12:]
        windows = [recent_prior[i - 2:i + 1] for i in range(2, len(recent_prior))]
        baseline = max(median(x.volume for x in window) for window in windows)
    ratio = b.volume / baseline if baseline and baseline > 0 else None
    historical_cliff = f["rvol"] < .5 if f["rvol"] is not None else None
    relative_cliff = ratio <= .2 if ratio is not None else None
    f.update(volume_cliff_reference_volume=baseline, volume_cliff_drive_ratio=ratio,
             volume_cliff_rvol=historical_cliff,
             volume_cliff=any(x is True for x in (historical_cliff, relative_cliff)) if historical_cliff is not None or relative_cliff is not None else None)
    f["retrace_start"] = len(prior) >= 3 and all(-d * (x.close - x.open) > 0 for x in prior[-3:]) and d * (b.close - b.open) > 0
    f["hikkake_start"] = False
    f["double_inside"] = f["close_double_inside"] = f["triple_inside"] = False
    f["trend_bar_failure"] = False
    if len(prior) >= 2:
        mother, inner = prior[-2], prior[-1]
        inside = inner.high < mother.high and inner.low > mother.low
        f["hikkake_start"] = inside and ((b.high < inner.high and b.low < inner.low) if d == 1 else (b.high > inner.high and b.low > inner.low))
        f.update(hikkake_trigger=d * (inner.high if d == 1 else inner.low),
                 hikkake_risk=d * (b.low if d == 1 else b.high),
                 mother_edge=d * (mother.high if d == 1 else mother.low),
                 mother_risk=d * (mother.low if d == 1 else mother.high),
                 inner_edge=d * (b.high if d == 1 else b.low))
        f["double_inside"] = inside and b.high < inner.high and b.low > inner.low
        f["close_double_inside"] = mother.low < inner.close < mother.high and inner.low < b.close < inner.high
        if len(prior) >= 3:
            a = prior[-3]
            f["triple_inside"] = f["double_inside"] and mother.high < a.high and mother.low > a.low
        counter = -d * (p.close - p.open) > .5 * (p.high - p.low)
        tested = b.low < p.low if d == 1 else b.high > p.high
        no_follow = not (-d * (b.close - b.open) > .5 * (b.high - b.low))
        f["trend_bar_failure"] = counter and tested and no_follow and d * efficiency(prior[-12:-1]) > .55
    # Several frozen volatility scales are maintained by the engine's bar scale;
    # local reversal uses historical volatility, never a future completed leg.
    trackers = [data.swings.get((seconds, factor)) for factor in data.config.swing_factors]
    ps = [p for p in (trackers[1].points if trackers[1] else []) if p.confirmed_at < b.end]
    attack = [p for p in ps if p.direction == -d]
    f["three_push"] = False
    f["parabolic"] = False
    for tracker in trackers:
        attack = [p for p in (tracker.points if tracker else []) if p.direction == -d and p.confirmed_at <= b.end]
        if len(attack) >= 3 and all(-d * (y.price - x.price) >= -tol for x, y in zip(attack[-3:-1], attack[-2:])) and -d * efficiency(prior[-20:]) > .25:
            f["three_push"] = True
            f["parabolic"] |= -d * (attack[-1].price - attack[-2].price) > 1.2 * max(data.tick, -d * (attack[-2].price - attack[-3].price))
    old_extreme = min(x.low for x in r) if d == 1 else max(x.high for x in r)
    structure = next((x.price for x in reversed(ps) if x.direction == d), None)
    f.update(old_extreme=d * old_extreme,
             trend_break=structure is not None and -d * efficiency(prior[-20:]) > .4 and d * b.close > d * structure + tol,
             old_extreme_retest=abs((b.low if d == 1 else b.high) - old_extreme) <= 1.5 * vol,
             anti_impulse=-d * efficiency(prior[-12:]) > .5 and f["impulse"] >= .65,
             late_flag=len(prior) >= 15 and -d * efficiency(prior[-20:-4]) > .55 and overlap(prior[-4:]) > .6,
             flag_edge=d * (min(x.low for x in prior[-4:]) if d == 1 else max(x.high for x in prior[-4:])),
             flag_failure=(b.low < min(x.low for x in prior[-4:]) and b.close > min(x.low for x in prior[-4:]) + tol) if d == 1 else
                          (b.high > max(x.high for x in prior[-4:]) and b.close < max(x.high for x in prior[-4:]) - tol))
    if len(prior) >= 8:
        lo, hi = min(x.low for x in r), max(x.high for x in r)
        f.update(range_edge=(b.low <= lo + tol if d == 1 else b.high >= hi - tol),
                 range_boundary=d * (lo if d == 1 else hi))
    else:
        f.update(range_edge=None, range_boundary=None)


def _vwap_features(f, d, cash, vwap, op, prev_close, same_time, data, b, vol, tol, hhmm):
    scale = f.get("observation_scale_seconds", 60)
    snapshots = [v for time, v in data.vwap_samples[scale] if time < b.end]
    prior_vwap = snapshots[-1] if snapshots else None
    prior_bars = [x for x in data.bars[scale] if x.end < b.end]
    prior_price = prior_bars[-1].close if prior_bars else None
    f.update(vwap_reclaim=None, vwap_cross=None, flat_vwap=None, vwap_stretch=None, vwap_deviation_fail=None,
             noise_break=None, same_band=None, opposite_band=None, vwap_band=None, early_sign=None, joint_sign=None,
             historical_vwap_reject=None, vwap=d * vwap if vwap is not None else None)
    if vwap is not None:
        f.update(vwap_reclaim=prior_vwap is not None and prior_price is not None and d * prior_price <= d * prior_vwap and d * b.close > d * vwap,
                 vwap_cross=prior_vwap is not None and prior_price is not None and d * prior_price < d * prior_vwap <= d * b.close,
                 flat_vwap=prior_vwap is not None and abs(vwap - prior_vwap) < .1 * vol,
                 vwap_stretch=-d * (b.close - vwap) > 2 * vol,
                 vwap_deviation_fail=f.get("failed_extension") and -d * (b.close - vwap) > vol)
    if len(same_time) == 14 and op is not None and prev_close is not None:
        sigma = sum(abs(price / h["cash_open"] - 1) for h, price in same_time) / 14
        upper, lower = max(op, prev_close) * (1 + sigma), min(op, prev_close) * (1 - sigma)
        f.update(noise_break=f["half_hour"] and (b.close > upper if d == 1 else b.close < lower),
                 same_band=d * (upper if d == 1 else lower), opposite_band=d * (lower if d == 1 else upper),
                 vwap_band=(d * max(upper, vwap) if d == 1 else d * min(lower, vwap)) if vwap is not None else None,
                 noise_sigma=sigma, noise_upper=upper, noise_lower=lower)
    first = next((x.close for x in cash if x.end == data.open_time + timedelta(minutes=30)), None)
    p1500 = next((x.close for x in cash if x.end.astimezone(NY).strftime("%H:%M") == "15:00"), None)
    if first is not None and prev_close is not None:
        r1 = first / prev_close - 1
        sign = 1 if r1 > 0 else -1
        f.update(early_sign=sign == d, early_return=r1,
                 joint_sign=p1500 is not None and r1 * (b.close / p1500 - 1) > 0 and sign == d)
    hvwap = data.reference("historical_vwap")
    if hvwap is not None:
        f["historical_vwap_reject"] = b.low - tol <= hvwap <= b.high + tol and d * (b.close - hvwap) > 0


def _profile_features(f, d, vp, tp, local_va, data, b, prior, tol, vol, arrival=None):
    f.update(prominent_poc=None, poc=None, poc_break=None, poc_retest=None, prior_tail_open=None,
             prior_tail_risk=None, tail_reject=None, value_return=None, value_inside60=None,
             value_entry=None, value_risk=None, profile_stretch=None, micro_p=None, distribution_edge=None,
             ledge=None, ledge_break=None, ledge_failed=None, value_two_brackets=None,
             volume_value_return=None, volume_value_inside60=None, volume_value_entry=None, volume_value_risk=None)
    if vp and vp.get("kind") == "volume" and vp.get("venue") == data.venue:
        poc = vp["poc"]
        f.update(prominent_poc=vp.get("prominence", 0) >= 2, poc=d * poc,
                 poc_break=bool(prior and d * prior[-1].close <= d * poc < d * b.close),
                 poc_retest=b.low - tol <= poc <= b.high + tol and d * (b.close - poc) > 0)
        tail = vp.get("low") if d == 1 else vp.get("high")
        tail_vol = vp.get("lower_tail_fraction" if d == 1 else "upper_tail_fraction")
        cash = [x for x in data.bars[60] if data.open_time < x.end <= min(b.end, data.close_time)]
        if tail is not None and tail_vol is not None and cash:
            f.update(prior_tail_open=tail_vol < .1 and abs(cash[0].open - tail) < 2 * vol,
                     prior_tail_risk=d * tail, tail_reject=d * (b.close - tail) > tol)
        if cash and "val" in vp and "vah" in vp:
            lo, hi = vp["val"], vp["vah"]
            outside = cash[0].open < lo if d == 1 else cash[0].open > hi
            f.update(volume_value_return=outside and lo <= b.close <= hi,
                     volume_value_entry=d * (lo if d == 1 else hi), volume_value_risk=d * (lo if d == 1 else hi),
                     volume_value_inside60=outside and len(cash) >= 60 and all(lo <= x.close <= hi for x in cash[-60:]))
    if tp and tp.get("kind") == "tpo" and tp.get("venue") == data.venue:
        lo, hi = tp["val"], tp["vah"]
        cash = [x for x in data.bars[60] if data.open_time < x.end <= min(b.end, data.close_time)]
        if cash:
            outside = cash[0].open < lo if d == 1 else cash[0].open > hi
            f.update(value_return=outside and lo <= b.close <= hi,
                     value_entry=d * (lo if d == 1 else hi), value_risk=d * (lo if d == 1 else hi),
                     value_inside60=outside and len(cash) >= 60 and all(lo <= x.close <= hi for x in cash[-60:]))
            brackets = [prices for k, prices in sorted(data.tpo.items())
                        if data.open_time + timedelta(minutes=30 * (k + 1)) <= b.end]
            f["value_two_brackets"] = outside and len(brackets) >= 2 and all(any(lo <= k * data.tick <= hi for k in prices) for prices in brackets[-2:])
    if local_va:
        lo, hi, poc = [x * data.tick for x in local_va]
        before = [va for time, va in data.profile_samples if time <= b.start]
        old_poc = before[-1][2] if before else None
        f["prior_value_poc"] = d * old_poc if old_poc is not None else None
        f["profile_stretch"] = old_poc is not None and abs(poc - old_poc) < vol and -d * (b.close - old_poc) > 3 * vol and data.profiles.get(round(b.close / data.tick), 0) < .2 * data.profiles[round(poc / data.tick)]
        # Distribution is computed from a recent impulse footprint, not the final day.
        trades = event_window(data.trades, b.end - timedelta(seconds=300), b.end, arrival or b.end)
        profile = Counter()
        for t in trades:
            profile[round(t.payload["price"] / data.tick)] += t.payload["size"]
        va = value_area(dict(profile))
        if va and len(profile) >= 5:
            pl, ph, pp = [x * data.tick for x in va]
            width = max(data.tick, max(profile) * data.tick - min(profile) * data.tick)
            concentrated = (pp - min(profile) * data.tick) / width > .65 if d == 1 else (max(profile) * data.tick - pp) / width > .65
            f.update(micro_p=concentrated and d * (trades[-1].payload["price"] - trades[0].payload["price"]) > 2 * vol,
                     distribution_edge=d * (pl if d == 1 else ph))
    completed = [prices for bracket, prices in sorted(data.tpo.items()) if prices and data.open_time + timedelta(minutes=30 * (bracket + 1)) <= b.end]
    if len(completed) >= 3:
        # A ledge is frozen from completed brackets, never from today's final profile.
        edges = [max(p) if d == 1 else min(p) for p in completed if p]
        edge, count = _tpo_edge_cluster(edges, d)
        level = edge * data.tick
        failed_edges = [min(p) if d == 1 else max(p) for p in completed if p]
        fail_edge, fail_count = _tpo_edge_cluster(failed_edges, d)
        inside_old_value = tp and tp.get("val", math.inf) <= b.close <= tp.get("vah", -math.inf)
        f.update(ledge=d * level,
                 ledge_break=count >= 3 and prior and d * prior[-1].close <= d * level < d * b.close,
                 ledge_failed=bool(inside_old_value and fail_count >= 3 and d * b.close > d * fail_edge * data.tick + tol))


def _flow_features(f, d, classified, current, previous, depths, data, b, vol, tol, sample_scale=None):
    for name in ("absorption", "flow_turn", "flow_resume", "flow_activity", "attack_energy", "weak_attack", "exhaustion", "jump", "jump_edge", "jump_risk", "thin_hold", "stacked", "stacked150", "imbalance_edge", "imbalance_risk", "imbalance150_edge", "imbalance150_risk", "counter_stacked", "counter_zone_edge", "counter_zone_risk", "cvd_divergence", "supply_block", "supply", "sticky_offer", "passive_support", "passive_reload_count", "passive_reload_volume"):
        f[name] = None
    if not classified or not current or not previous:
        return
    delta = lambda rows: sum(t["size"] * (1 if t["side"] == "buy" else -1) for t in rows)
    size, old_size = sum(t["size"] for t in current), sum(t["size"] for t in previous)
    if old_size <= 0:
        return
    cd, pd = delta(current), delta(previous)
    net = d * cd / size
    movement = d * (current[-1]["price"] - current[0]["price"])
    f.update(flow_turn=d * pd < 0 < d * cd, flow_resume=net > .3 and movement > data.tick,
             flow_activity=size / old_size,
             attack_energy=size / old_size * max(0, movement) / vol,
             weak_attack=size / old_size < .75 and abs(movement) < .5 * vol,
             absorption=size / old_size >= 1.5 and abs(movement) < .25 * vol and net < -.3,
             exhaustion=-d * pd / old_size > .4 and size / old_size < .5 and abs(movement) < .5 * vol,
             supply_block=d == 1 and cd > 0 and size / old_size > 1.5 and movement < .25 * vol,
             supply=d * max(t["price"] for t in current),
             cvd_divergence=bool(f.get("failed_extension") and d * cd > d * pd and -d * movement > 0))
    for ratio, prefix, flag in ((3, "imbalance", "stacked"), (1.5, "imbalance150", "stacked150")):
        zones = diagonal_zones(current, data.tick, ratio, data.config.footprint_min_volume, data.config.footprint_min_levels)
        zone = next((z for z in zones if z["direction"] == d), None)
        f[flag] = zone is not None
        if zone:
            f[prefix + "_edge"] = d * (zone["low"] if d == 1 else zone["high"])
            f[prefix + "_risk"] = d * (zone["low"] if d == 1 else zone["high"])
        if ratio == 3:
            counter = next((z for z in zones if z["direction"] == -d), None)
            f["counter_stacked"] = counter is not None
            if counter:
                f.update(counter_zone_edge=d * (counter["high"] if d == 1 else counter["low"]),
                         counter_zone_risk=d * (counter["low"] if d == 1 else counter["high"]))
    # Same reset, identical two consecutive windows; not a single-bar Delta sign.
    scale = sample_scale if sample_scale is not None else int((b.end - b.start).total_seconds())
    samples = data.delta_samples[scale][:-1]
    samples = [(x, cvd) for x, cvd in samples[-60:] if cvd is not None]
    if samples and data.cvd_valid:
        anchor, anchor_cvd = min(samples, key=lambda x: x[0].low) if d == 1 else max(samples, key=lambda x: x[0].high)
        broken = b.low < anchor.low if d == 1 else b.high > anchor.high
        f["cvd_divergence"] = broken and d * data.cvd > d * anchor_cvd
    else:
        f["cvd_divergence"] = None
    # Decimal tick prices cannot always be subtracted exactly as binary floats.
    jumps = [(x, y) for x, y in zip(current, current[1:]) if d * (y["price"] - x["price"]) + data.tick * 1e-9 >= 2 * data.tick]
    f["jump"] = len(jumps) >= 2 and net > .5
    if jumps:
        x, y = jumps[0]
        f.update(jump_edge=d * y["price"], jump_risk=d * x["price"], thin_hold=net > .5 and all(d * t["price"] >= d * y["price"] - tol for t in current[-3:]))
    if len(depths) >= 3 and all(z.quality == "complete" for z in depths[-8:]):
        recent_depth = depths[-8:]
        support_side = "bids" if d == 1 else "asks"
        reloads = 0
        reload_volume = 0
        for a, z in zip(recent_depth, recent_depth[1:]):
            old, new = dict(a.payload[support_side]), dict(z.payload[support_side])
            for price in old.keys() & new.keys():
                opposing = [t for t in current if a.event_time < t["time"] <= z.event_time and abs(t["price"] - price) < data.tick / 2 and (t["side"] == "sell" if d == 1 else t["side"] == "buy")]
                # Executed volume consumed the old queue. A positive residual
                # after that consumption is the minimum observed replenishment,
                # even when the displayed queue is smaller in the next snapshot.
                replenished = new[price] - old[price] + sum(t["size"] for t in opposing)
                if opposing and replenished > 1e-9:
                    reloads += 1
                    reload_volume += replenished
        f.update(passive_reload_count=reloads, passive_reload_volume=reload_volume)
        f["passive_support"] = reloads >= 2 and movement >= -tol
        offers = [z.payload["asks"][0][0] for z in recent_depth if z.payload["asks"]]
        f["sticky_offer"] = d == -1 and len(offers) >= 3 and max(offers) - min(offers) <= tol and net > .3


def _branch(consensus, news):
    values = {}
    results = []
    for event in news:
        values.update(event.payload.get("values", {}))
        dimensions = consensus.payload.get("dimensions", {})
        if dimensions and all(k in values for k in dimensions):
            values["_net"] = sum(dim["weight"] * (values[k] - dim["consensus"]) / dim["scale"] for k, dim in dimensions.items())
        chosen = None
        for branch in consensus.payload.get("branches", []):
            tests = branch.get("conditions", [])
            if tests and all(k in values and isinstance(values[k], (int, float))
                             and ((values[k] > v) if op == "gt" else (values[k] < v) if op == "lt" else (values[k] == v))
                             for k, op, v in tests):
                chosen = branch
                break
        results.append((event, chosen))
    return results


def _news_features(f, d, news, data, b, arrival, prev_close, op):
    f.update(catalyst_gap=None, sai_catalyst_gap=None, second_day_news=None, second_day_news_event_ids=[], news_fresh60=False, news_fresh90=False,
             news_risk=f["signal_risk"], boulevard_context=None, macro_surprise=False,
             reassessment=False, expected_responses={})
    news = sorted((n for n in news if n.arrival_time <= arrival and n.quality == "complete"
                   and (b is None or n.event_time <= b.end)), key=lambda n: (n.arrival_time, n.event_time))
    if not news:
        return
    latest = news[-1]
    age = (arrival - latest.arrival_time).total_seconds()
    prior_history = data.complete_history(1)
    prior_date = timestamp(prior_history[-1]["as_of"]).astimezone(NY).date() if prior_history else None
    if prior_date is None and "history_calendar" not in data.session:
        ref = data.session.get("references", {}).get("prev_close")
        prior_date = timestamp(ref["as_of"]).astimezone(NY).date() if ref else None
    publication_age = (arrival - latest.event_time).total_seconds()
    f.update(news_fresh60=latest.payload.get("is_new") is True and 0 <= age <= 60 and publication_age <= 60,
             news_fresh90=latest.payload.get("is_new") is True and 0 <= age <= 90 and publication_age <= 90,
             catalyst_gap=prev_close is not None and op is not None and (op / prev_close - 1) >= .05
                          and latest.payload.get("is_new") is True,
             boulevard_context=data.reference("float_shares") is not None and data.reference("float_shares") <= 20_000_000
                                and prev_close is not None and op is not None and op / prev_close - 1 >= .05)
    f["sai_catalyst_gap"] = prev_close is not None and op is not None and op / prev_close - 1 >= .02 and latest.payload.get("is_new") is True
    if prior_date is not None:
        prior_news = [n for n in news if n.event_time.astimezone(NY).date() == prior_date
                      and n.payload.get("is_new") is True]
        effective = []
        relevant_ids = {n.event_id for n in prior_news}
        for n in prior_news:
            corrections = [z for z in news if z.payload["event_key"] == n.payload["event_key"]
                           and z.event_id != n.event_id and z.arrival_time >= n.arrival_time
                           and z.payload.get("invalidates_prior") is True]
            relevant_ids.update(z.event_id for z in corrections)
            if not corrections:
                effective.append(n)
        f.update(second_day_news=bool(effective), second_day_news_event_ids=sorted(relevant_ids))
    for key, consensus in data.consensus.items():
        matching = [n for n in news if n.payload["event_key"] == key and n.arrival_time > consensus.arrival_time and consensus.arrival_time < n.event_time]
        decisions = _branch(consensus, matching)
        if not decisions:
            continue
        event, branch = decisions[-1]
        if branch and branch.get("direction_by_symbol", {}).get(data.symbol) == d:
            f["macro_surprise"] = bool(branch.get("surprise") is True and (arrival - event.arrival_time).total_seconds() <= 900)
            f["expected_responses"] = branch.get("direction_by_symbol", {})
            f["news_arrived_at"] = event.arrival_time
            previous = [x for _, x in decisions[:-1] if x]
            f["reassessment"] = bool(previous and previous[-1]["name"] != branch["name"]
                                      and event.payload.get("stage", 1) > 1)


def _cross_features(f, d, aligned, names, data, bars, seconds):
    f.update(relative_strength=None, market_confirm=None, leader_break=None, cross_confirm=None,
             relative_strength_current=None, relative_strength_memory=None,
             daily_aligned=data.reference("daily_direction") == d if data.reference("daily_direction") is not None else None,
             weak_long_context=d == -1 and data.reference("prev_close_location", 0) > .7
             and f.get("overnight_failure") is True)
    if not aligned or len(aligned) != len(names):
        return
    cutoff = bars[-1].end
    benchmark = [x for x in aligned[0].bars[seconds] if x.end <= cutoff]
    own = [x for x in bars if x.end <= cutoff][-6:]
    if len(own) < 6 or len(benchmark) < 6:
        return
    market = benchmark[-6:]
    if (not _contiguous_complete(own) or not _contiguous_complete(market)
            or [x.end for x in own] != [x.end for x in market]):
        return
    own_return = own[-1].close / own[0].close - 1
    market_return = market[-1].close / market[0].close - 1
    dispersion = max(.0001, abs(market_return), abs(own_return))
    strength = ramp(d * (own_return - market_return) / dispersion, 0, 1)
    f.update(relative_strength=strength, relative_strength_current=strength,
             market_confirm=d * efficiency(market) > .55,
             weak_long_context=d == -1 and data.reference("prev_close_location", 0) > .7 and own_return < 0)
    # Reconstruct the earlier resistance to market pressure from synchronized
    # observations. This bounded memory is pure at the cutoff: repeated frames,
    # directions and scales cannot modify it or import a later benchmark bar.
    own_context = [x for x in bars if data.open_time <= x.start and x.end <= cutoff][-31:]
    market_by_end = {x.end: x for x in benchmark}
    pairs = [(x, market_by_end[x.end]) for x in own_context if x.end in market_by_end]
    remembered = []
    pressure_anchor_chosen = False
    tol = f.get("tolerance", data.tick)
    for i in range(5, len(pairs) - 1):
        window = pairs[i - 5:i + 1]
        stock_window, market_window = [x for x, _ in window], [x for _, x in window]
        subsequent = [x for x, _ in pairs[i:]]
        # Missing minutes cannot be bridged by an old support-memory proxy.
        if not (_contiguous_complete(stock_window + subsequent[1:])
                and _contiguous_complete(market_window + [x for _, x in pairs[i + 1:]])):
            pressure_anchor_chosen = False
            continue
        sr = stock_window[-1].close / stock_window[0].close - 1
        mr = market_window[-1].close / market_window[0].close - 1
        score = ramp(d * (sr - mr) / max(.0001, abs(sr), abs(mr)), 0, 1)
        pressure = d * mr < 0 and d * efficiency(market_window) < -.3
        if not pressure:
            pressure_anchor_chosen = False
            continue
        if score < .6 or pressure_anchor_chosen:
            continue
        # Freeze the first qualifying support during this pressure episode.
        # Later overlapping windows cannot lower that floor after it broke.
        pressure_anchor_chosen = True
        floor = d * (stock_window[0].low if d == 1 else stock_window[0].high)
        held = all(d * (x.low if d == 1 else x.high) >= floor - tol
                   for x in stock_window[1:] + subsequent[1:])
        if held:
            remembered.append(score)
    if remembered:
        f["relative_strength_memory"] = max(remembered)
        f["relative_strength"] = max(strength, f["relative_strength_memory"])
    expected = f.get("expected_responses", {})
    if expected and len(expected) >= 2:
        response = []
        for symbol, sign in expected.items():
            state = data if symbol == data.symbol else next((s for s in aligned if s.symbol == symbol), None)
            known = [x for x in state.bars[seconds] if x.end <= cutoff] if state else []
            before = [x for x in known if x.end <= f.get("news_arrived_at", cutoff)]
            if not state or not before:
                response.append(False)
            else:
                response.append(sign * (known[-1].close - before[-1].close) > volatility(known[:-1], state.tick) * .2)
        f["cross_confirm"] = all(response)
    # Leading relation is established from a frozen, earlier calibration, not
    # inferred from the present breakout or assigned by today's final winner.
    leadership = data.reference("leadership")
    if leadership and leadership.get("symbol") in names and leadership.get("correlation", 0) >= .6 and leadership.get("lag_bars", 0) >= 1:
        leader = next((s for s in aligned if s.symbol == leadership["symbol"]), None)
        if leader:
            lbs = [x for x in leader.bars[seconds] if leader.open_time <= x.start and x.end <= cutoff]
            ib_end = leader.open_time + timedelta(minutes=leader.session.get("ib_minutes", 60))
            lib = [x for x in leader.bars[60] if leader.open_time < x.end <= min(ib_end, cutoff)]
            level = leader.reference("key_level")
            if level is None and len(lib) == leader.session.get("ib_minutes", 60):
                level = max(x.high for x in lib) if d == 1 else min(x.low for x in lib)
                lbs = [x for x in lbs if x.end >= ib_end]
            if level is not None and len(lbs) >= 2 and _contiguous_complete(lbs[-6:]):
                lag = leadership["lag_bars"] * max(1, seconds)
                crossings = [i for i in range(1, len(lbs))
                             if lbs[i - 1].end == lbs[i].start
                             and d * (lbs[i - 1].close - level) <= 0 < d * (lbs[i].close - level)
                             and (cutoff - lbs[i].end).total_seconds() >= lag]
                f["leader_break"] = any(_contiguous_complete(lbs[i:])
                    and all(d * (x.close - level) > 0 for x in lbs[i:]) for i in crossings)
