"""Public, deterministic numerical definitions. No future pivots or fitted models."""
from __future__ import annotations

from collections import defaultdict
from statistics import median
from .models import Bar, Pivot


def clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def ramp(value: float, low: float, high: float) -> float:
    if high <= low:
        raise ValueError("ramp requires high > low")
    return clamp((value - low) / (high - low))


def volatility(bars: list[Bar], tick: float) -> float:
    sample = bars[-21:]
    ranges = [max(b.high - b.low, abs(b.high - a.close), abs(b.low - a.close))
              for a, b in zip(sample, sample[1:])]
    return max(tick, median(ranges) if ranges else tick)


def efficiency(bars: list[Bar]) -> float:
    changes = [b.close - a.close for a, b in zip(bars, bars[1:])]
    return sum(changes) / max(sum(abs(x) for x in changes), 1e-12)


def overlap(bars: list[Bar]) -> float:
    pairs = [(max(0, min(a.high, b.high) - max(a.low, b.low)), max(a.high, b.high) - min(a.low, b.low))
             for a, b in zip(bars, bars[1:])]
    return sum(x / max(y, 1e-12) for x, y in pairs) / max(len(pairs), 1)


def pivots(bars: list[Bar], reversal: float) -> list[Pivot]:
    """Close-based directional change. Extremes become usable at confirmation only."""
    if not bars:
        return []
    high = low = bars[0].close
    ht = lt = bars[0].end
    direction = 0
    out = []
    for b in bars[1:]:
        if direction >= 0:
            if b.close > high:
                high, ht = b.close, b.end
            if high - b.close >= reversal:
                out.append(Pivot(high, 1, ht, b.end))
                direction = -1
                low, lt = b.close, b.end
        if direction <= 0:
            if b.close < low:
                low, lt = b.close, b.end
            if b.close - low >= reversal:
                out.append(Pivot(low, -1, lt, b.end))
                direction = 1
                high, ht = b.close, b.end
    return out


class SwingTracker:
    """Incremental pivots, each with immutable extreme/confirmation timestamps."""
    def __init__(self, factor=.7):
        self.factor = factor
        self.high = self.low = None
        self.high_time = self.low_time = None
        self.direction = 0
        self.points = []

    def push(self, bar, scale):
        if self.high is None:
            self.high = self.low = bar.close
            self.high_time = self.low_time = bar.end
            return
        threshold = scale * self.factor
        if self.direction >= 0:
            if bar.close > self.high:
                self.high, self.high_time = bar.close, bar.end
            if self.high - bar.close >= threshold:
                self.points.append(Pivot(self.high, 1, self.high_time, bar.end))
                self.direction = -1
                self.low, self.low_time = bar.close, bar.end
                return
        if self.direction <= 0:
            if bar.close < self.low:
                self.low, self.low_time = bar.close, bar.end
            if bar.close - self.low >= threshold:
                self.points.append(Pivot(self.low, -1, self.low_time, bar.end))
                self.direction = 1
                self.high, self.high_time = bar.close, bar.end
        self.points = self.points[-1000:]


def value_area(weights: dict[int, float], fraction: float = .7) -> tuple[int, int, int] | None:
    """Expand contiguously from POC; equal sides added together, POC tie lowest."""
    if not weights or not 0 < fraction <= 1 or sum(weights.values()) <= 0:
        return None
    poc = min(weights, key=lambda k: (-weights[k], k))
    lo = hi = poc
    total, taken = sum(weights.values()), weights[poc]
    left, right = min(weights), max(weights)
    while taken < fraction * total and (lo > left or hi < right):
        lv = weights.get(lo - 1, 0) if lo > left else -1
        rv = weights.get(hi + 1, 0) if hi < right else -1
        if lv >= rv and lo > left:
            lo -= 1
            taken += weights.get(lo, 0)
        if rv >= lv and hi < right:
            hi += 1
            taken += weights.get(hi, 0)
    return lo, hi, poc


def diagonal_zones(trades: list[dict], tick: float, ratio: float = 3.0,
                   min_volume: float = 10, min_levels: int = 3) -> list[dict]:
    """Buy Ask(p)/Bid(p-tick), sell Bid(p)/Ask(p+tick). Zero is unknown."""
    buys, sells = defaultdict(float), defaultdict(float)
    for t in trades:
        price = round(t["price"] / tick)
        if t["side"] == "buy":
            buys[price] += t["size"]
        elif t["side"] == "sell":
            sells[price] += t["size"]
    out = []
    for direction in (1, -1):
        keys = set(buys) if direction == 1 else set(sells)
        selected = []
        for k in sorted(keys):
            num, den = (buys[k], sells[k - 1]) if direction == 1 else (sells[k], buys[k + 1])
            if den >= min_volume and num >= min_volume and num / den >= ratio:
                selected.append(k)
        groups = []
        for k in selected:
            if not groups or k != groups[-1][-1] + 1:
                groups.append([])
            groups[-1].append(k)
        for group in groups:
            if len(group) >= min_levels:
                out.append({"direction": direction, "low": group[0] * tick,
                            "high": group[-1] * tick, "levels": len(group)})
    return out
