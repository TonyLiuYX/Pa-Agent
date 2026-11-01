"""Small public contracts shared by streaming and replay adapters."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import math
from typing import Any


def timestamp(value: str | datetime) -> datetime:
    if not isinstance(value, (str, datetime)):
        raise ValueError("timestamp must be an ISO string or datetime")
    value = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if value.tzinfo is None:
        raise ValueError("timestamps must carry an explicit UTC offset")
    return value.astimezone(timezone.utc)


def finite(value: Any, name: str, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name}: finite number required")
    if positive and value <= 0:
        raise ValueError(f"{name}: positive number required")
    return float(value)


@dataclass(frozen=True)
class MarketEvent:
    event_id: str
    symbol: str
    venue: str
    kind: str
    event_time: datetime
    arrival_time: datetime
    payload: dict[str, Any]
    quality: str = "complete"

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "MarketEvent":
        event = cls(
            event_id=row["event_id"], symbol=row["symbol"], venue=row["venue"],
            kind=row["kind"], event_time=timestamp(row["event_time"]),
            arrival_time=timestamp(row["arrival_time"]), payload=dict(row["payload"]),
            quality=row.get("quality", "complete"),
        )
        event.validate()
        return event

    def validate(self) -> None:
        if not isinstance(self.event_time, datetime) or not isinstance(self.arrival_time, datetime) or self.event_time.tzinfo is None or self.arrival_time.tzinfo is None:
            raise ValueError("event timestamps must be timezone-aware")
        if not all(isinstance(v, str) and v for v in (self.event_id, self.symbol, self.venue)):
            raise ValueError("event_id, symbol and venue require nonempty strings")
        if self.kind not in {"session", "bar", "trade", "quote", "depth", "news", "consensus", "status"}:
            raise ValueError(f"unknown event kind: {self.kind}")
        if self.quality not in {"complete", "partial", "unavailable"}:
            raise ValueError("invalid data quality")
        if self.arrival_time < self.event_time:
            raise ValueError("arrival_time cannot precede event_time")
        p = self.payload
        if not isinstance(p, dict):
            raise ValueError("event payload must be an object")
        if "sequence" in p and (type(p["sequence"]) is not int or p["sequence"] < 0):
            raise ValueError("sequence must be a nonnegative integer")
        if self.kind == "bar":
            for key in ("open", "high", "low", "close"):
                finite(p[key], key, True)
            if not p["low"] <= min(p["open"], p["close"]) <= max(p["open"], p["close"]) <= p["high"]:
                raise ValueError("inconsistent OHLC")
            if finite(p["volume"], "volume") < 0:
                raise ValueError("negative volume")
            if finite(p["seconds"], "seconds", True) % 1:
                raise ValueError("bar duration must be integral seconds")
            if p.get("closed", True) is not True:
                raise ValueError("unfinished bars are not decision inputs")
            if "notional" in p:
                finite(p["notional"], "notional")
                if not p["low"] * p["volume"] <= p["notional"] <= p["high"] * p["volume"]:
                    raise ValueError("bar notional must be consistent with its price range")
        elif self.kind == "session":
            op, cl = timestamp(p["open_time"]), timestamp(p["close_time"])
            if op >= cl or self.event_time > op:
                raise ValueError("session must be supplied no later than its open")
            if finite(p.get("tick_size", .01), "tick_size", True) <= 0:
                raise ValueError("tick_size")
            # Coverage is an adapter declaration; it does not fill any missing data.
            if p.get("asset_class", "equity") not in {"equity", "ETF", "futures"}:
                raise ValueError("unsupported asset_class")
            if "premarket_complete" in p and type(p["premarket_complete"]) is not bool:
                raise ValueError("premarket_complete must be boolean")
            for name, coverage in p.get("coverage", {}).items():
                if not isinstance(coverage, dict) or type(coverage.get("complete")) is not bool:
                    raise ValueError("coverage.complete must be boolean")
                if coverage.get("start_time") is not None:
                    timestamp(coverage["start_time"])
                if coverage.get("end_time") is not None:
                    timestamp(coverage["end_time"])
        elif self.kind == "trade":
            finite(p["price"], "price", True)
            finite(p["size"], "size", True)
            if p.get("side") not in {"buy", "sell", "unknown"}:
                raise ValueError("trade side must be buy/sell/unknown")
        elif self.kind == "quote":
            if finite(p["bid"], "bid", True) > finite(p["ask"], "ask", True):
                raise ValueError("crossed quote")
        elif self.kind == "depth":
            if p.get("mode") != "snapshot":
                raise ValueError("adapter must reconstruct incremental depth into snapshots")
            for side in ("bids", "asks"):
                prices = []
                for price, size in p[side]:
                    finite(price, "depth.price", True)
                    if finite(size, "depth.size") < 0:
                        raise ValueError("negative depth")
                    prices.append(price)
                if len(prices) != len(set(prices)) or prices != sorted(prices, reverse=side == "bids"):
                    raise ValueError("depth prices must be unique, bids descending and asks ascending")
            if p["bids"] and p["asks"] and p["bids"][0][0] > p["asks"][0][0]:
                raise ValueError("crossed depth book")
        elif self.kind in {"news", "consensus"}:
            if not p.get("source") or not p.get("event_key"):
                raise ValueError("structured event requires source and event_key")
            if self.kind == "news":
                for key, value in p.get("values", {}).items():
                    finite(value, "news.values." + key)
            else:
                names = set()
                for branch in p.get("branches", []):
                    if not branch.get("name") or branch["name"] in names:
                        raise ValueError("consensus branches require unique names")
                    names.add(branch["name"])
                    for key, operation, value in branch.get("conditions", []):
                        if operation not in {"gt", "lt", "eq"}:
                            raise ValueError("branch operation must be gt/lt/eq")
                        finite(value, "branch.threshold")
                    if any(type(v) is not int or v not in {-1, 1} for v in branch.get("direction_by_symbol", {}).values()):
                        raise ValueError("branch directions must be +1/-1")
                for dim in p.get("dimensions", {}).values():
                    finite(dim["consensus"], "dimension.consensus")
                    finite(dim["scale"], "dimension.scale", True)
                    finite(dim["weight"], "dimension.weight")
        elif self.kind == "status":
            for name in ("halted", "gap", "connected", "clock", "correction"):
                if name in p and type(p[name]) is not bool:
                    raise ValueError(f"status.{name} must be boolean")
            if "stream" in p and p["stream"] not in {"all", "bar", "trade", "quote", "depth", "news", "consensus"}:
                raise ValueError("unknown status stream")
            if "connection_state" in p and (not isinstance(p["connection_state"], str) or not p["connection_state"].strip()):
                raise ValueError("status.connection_state must be a nonempty string")

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        for key in ("event_time", "arrival_time"):
            row[key] = row[key].isoformat()
        return row


@dataclass(frozen=True)
class Bar:
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    notional: float | None
    source_ids: tuple[str, ...]
    quality: str = "complete"


@dataclass(frozen=True)
class Pivot:
    price: float
    direction: int
    extreme_time: datetime
    confirmed_at: datetime


@dataclass
class DetectionEvent:
    detection_id: str
    setup_id: str
    variant_id: str
    symbol: str
    venue: str
    direction: int
    scale_seconds: int
    state: str
    identified_at: str
    evidence_cutoff: str
    anchor: dict[str, Any]
    conditions: list[dict[str, Any]]
    structure_score: float
    evidence_score: float
    data_status: str
    missing: list[str]
    source_version: str
    source_sha256: str
    rule_version: str
    market_status: str
    algorithm_sha256: str = ""
    configuration_sha256: str = ""
    observation_kind: str = "bar"
    related_ids: list[str] = field(default_factory=list)
    note: str = "editor_operationalization; real_market_validation_not_run; source_ids_are_examples_not_full_tape"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
