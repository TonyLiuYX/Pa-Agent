"""Exact-Decimal Alpaca trade registry and one-second reducer."""

from __future__ import annotations

import copy
import hashlib
import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

from pa_engine.contracts import ValidationIssue, ValidationResult, canonical_json_bytes, stable_id
from pa_engine.market import ALPACA_IEX_FEED, MarketFeedRef, validate_market_feed_ref

from .conditions import ConditionRule, UnsupportedConditionError, combine_rules
from .errors import DuplicateTradeError, MalformedPayloadError
from .parser import CancelErrorEvent, CorrectionEvent, TradeEvent, canonical_timestamp, parse_cancel_error, parse_correction, parse_message, parse_trade, timestamp_ns


SCHEMA_VERSION = "0.1.0"
ARTIFACT_KIND = "second_aggregate_event"
ADJUSTMENT = "raw"
ADJUSTMENT_POLICY = "unadjusted"
SOURCE_LABEL = "alpaca_iex"
_SECOND_NS = 1_000_000_000
_UTC_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$")


def _timestamp_from_ns(value: int) -> str:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    seconds, nanos = divmod(int(value), _SECOND_NS)
    instant = epoch + timedelta(seconds=seconds)
    if nanos:
        return instant.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}".rstrip("0") + "Z"
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def _canonical_decimal(value: Decimal | int | str, *, nonnegative: bool = False, positive: bool = False) -> str:
    if isinstance(value, float) or isinstance(value, bool):
        raise TypeError("Decimal values must not be binary floats")
    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    if not decimal.is_finite() or (nonnegative and decimal < 0) or (positive and decimal <= 0):
        raise ValueError("invalid Decimal value")
    if decimal == 0:
        return "0"
    text = format(decimal, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def canonical_decimal(value: Decimal | int | str, *, nonnegative: bool = False, positive: bool = False) -> str:
    """Public fixed-point Decimal serializer used by event payloads."""

    return _canonical_decimal(value, nonnegative=nonnegative, positive=positive)


def _parse_timestamp(value: Any) -> str:
    if not isinstance(value, str) or _UTC_RE.fullmatch(value) is None:
        raise ValueError("timestamp must be canonical UTC")
    # parser.timestamp_ns validates calendar ranges as well.
    timestamp_ns(value)
    fraction = (_UTC_RE.fullmatch(value).group(2) or "").ljust(9, "0")
    base = _UTC_RE.fullmatch(value).group(1)
    return f"{base}.{fraction.rstrip('0')}Z" if fraction.rstrip("0") else f"{base}Z"


def floor_second(timestamp: str) -> tuple[str, str]:
    """Return canonical ``[start,end)`` UTC second for an event timestamp."""

    nanoseconds = timestamp_ns(timestamp)
    start_ns = nanoseconds - nanoseconds % _SECOND_NS
    return _timestamp_from_ns(start_ns), _timestamp_from_ns(start_ns + _SECOND_NS)


def execution_watermark(start_at: str, end_at: str, first_receipt_at: str) -> str:
    """Compute the v0.1 execution freeze watermark."""

    start = _parse_timestamp(start_at)
    end = _parse_timestamp(end_at)
    receipt = _parse_timestamp(first_receipt_at)
    if timestamp_ns(end) - timestamp_ns(start) != _SECOND_NS:
        raise ValueError("execution watermark requires a one-second interval")
    return _timestamp_from_ns(max(timestamp_ns(receipt), timestamp_ns(end) + 3 * _SECOND_NS))


def _id_sort_key(value: int | str) -> tuple[int, Any]:
    # Provider IDs are normally integers; the prefix makes mixed int/string
    # identities deterministic without relying on Python's incomparable types.
    return (0, value) if isinstance(value, int) else (1, str(value))


@dataclass(frozen=True, slots=True)
class ConnectionGap:
    start_at: str
    end_at: str | None
    reason: str
    kind: str = "connection_gap"

    def to_dict(self) -> dict[str, Any]:
        return {"start_at": self.start_at, "end_at": self.end_at, "reason": self.reason, "kind": self.kind}


@dataclass(frozen=True, slots=True)
class VolumeOnlyAudit:
    """A trade which contributes volume but cannot establish OHLC."""

    symbol: str
    trade_id: int | str
    start_at: str
    end_at: str
    size: int
    conditions: tuple[str, ...]
    reason: str = "volume_only"
    source: str = SOURCE_LABEL

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "trade_id": self.trade_id,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "size": self.size,
            "conditions": list(self.conditions),
            "reason": self.reason,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class SecondAggregateEvent:
    """One immutable exact-Decimal second event and its revision identity."""

    symbol: str
    start_at: str
    end_at: str
    open: str
    high: str
    low: str
    close: str
    volume: str
    contributor_ids: tuple[str, ...] = ()
    contributor_hash: str | None = None
    execution_watermark: str | None = None
    status: str = "provisional"
    revision_sequence: int = 0
    quality: Mapping[str, Any] = field(default_factory=lambda: {"status": "ok", "codes": [], "connection_gap": False})
    market_feed: MarketFeedRef = ALPACA_IEX_FEED
    adjustment: str = ADJUSTMENT
    raw_adjustment: str = ADJUSTMENT
    parent_revision_id: str | None = None
    source: str = SOURCE_LABEL
    event_id: str = field(init=False)
    revision_id: str = field(init=False)
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        symbol = self.symbol
        if not isinstance(symbol, str) or not symbol or symbol != symbol.upper() or any(char.isspace() for char in symbol):
            raise ValueError("symbol must be an upper-case value")
        start = _parse_timestamp(self.start_at)
        end = _parse_timestamp(self.end_at)
        if timestamp_ns(end) - timestamp_ns(start) != _SECOND_NS:
            raise ValueError("second event interval must be exactly one second")
        if timestamp_ns(start) % _SECOND_NS:
            raise ValueError("second event start_at must be floored to a UTC second")
        if self.adjustment != ADJUSTMENT or self.raw_adjustment != ADJUSTMENT:
            raise ValueError("second events are raw/unadjusted only")
        if self.source != SOURCE_LABEL:
            raise ValueError("second events are restricted to alpaca_iex")
        if isinstance(self.revision_sequence, bool) or not isinstance(self.revision_sequence, int) or self.revision_sequence < 0:
            raise ValueError("revision_sequence must be non-negative")
        if self.parent_revision_id is not None and (not isinstance(self.parent_revision_id, str) or not self.parent_revision_id):
            raise ValueError("parent_revision_id must be a non-empty string")
        if self.execution_watermark is None:
            raise ValueError("execution_watermark is required")
        watermark = _parse_timestamp(self.execution_watermark)
        if timestamp_ns(watermark) < timestamp_ns(end) + 3 * _SECOND_NS:
            raise ValueError("execution_watermark must be at least end_at + 3 seconds")
        # Force canonical fixed-point text at construction, including fields
        # supplied by callers as Decimal objects.
        values = {
            "open": _canonical_decimal(self.open, positive=True),
            "high": _canonical_decimal(self.high, positive=True),
            "low": _canonical_decimal(self.low, positive=True),
            "close": _canonical_decimal(self.close, positive=True),
            "volume": _canonical_decimal(self.volume, nonnegative=True),
        }
        if Decimal(values["high"]) < max(Decimal(values["open"]), Decimal(values["close"])) or Decimal(values["low"]) > min(Decimal(values["open"]), Decimal(values["close"])):
            raise ValueError("OHLC invariant failed")
        if self.status not in {"provisional", "frozen", "reconciliation"}:
            raise ValueError("unknown second event status")
        feed = self.market_feed if isinstance(self.market_feed, MarketFeedRef) else MarketFeedRef.from_mapping(self.market_feed)
        if feed != ALPACA_IEX_FEED:
            raise ValueError("second events are restricted to ALPACA_IEX_FEED")
        quality = dict(self.quality)
        quality.setdefault("status", "ok")
        quality.setdefault("codes", [])
        quality.setdefault("connection_gap", False)
        if quality.get("status") not in {"ok", "partial"} or not isinstance(quality.get("codes"), list) or any(not isinstance(code, str) for code in quality["codes"]):
            raise ValueError("quality codes must be a list")
        if not isinstance(quality.get("connection_gap"), bool):
            raise ValueError("quality connection_gap must be boolean")
        contributors = tuple(self.contributor_ids)
        if any(not isinstance(item, str) for item in contributors):
            raise ValueError("contributor IDs must be strings")
        contributor_hash = self.contributor_hash
        expected_contributor_hash = hashlib.sha256(canonical_json_bytes(list(contributors))).hexdigest() if contributors else None
        if contributor_hash is None and expected_contributor_hash is not None:
            contributor_hash = expected_contributor_hash
        if contributor_hash is not None and contributor_hash != expected_contributor_hash:
            raise ValueError("contributor hash does not match contributor IDs")
        if not contributors or any(not item for item in contributors) or not isinstance(contributor_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", contributor_hash):
            raise ValueError("second event requires contributor IDs and a deterministic hash")
        if _contains_secret_like(quality):
            raise ValueError("quality metadata contains a secret-like field")
        digest_payload = {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "symbol": symbol,
            "start_at": start,
            "end_at": end,
            **values,
            "market_feed": feed.to_dict(),
            "adjustment": ADJUSTMENT,
            "raw_adjustment": ADJUSTMENT,
            "adjustment_policy": ADJUSTMENT_POLICY,
            "source": SOURCE_LABEL,
            "contributors": {"ids": list(contributors), "hash": contributor_hash},
            "execution_watermark": watermark,
            "status": self.status,
            "revision_sequence": self.revision_sequence,
            "parent_revision_id": self.parent_revision_id,
            "quality": quality,
        }
        digest = hashlib.sha256(canonical_json_bytes(digest_payload)).hexdigest()
        event_id = stable_id("sec_", {"symbol": symbol, "start_at": start, "end_at": end, "market_feed": feed.to_dict()})
        revision_id = stable_id("sec_rev_", {"event_id": event_id, "revision_sequence": self.revision_sequence, "content_hash": digest})
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "start_at", start)
        object.__setattr__(self, "end_at", end)
        for key, value in values.items():
            object.__setattr__(self, key, value)
        object.__setattr__(self, "contributor_ids", contributors)
        object.__setattr__(self, "contributor_hash", contributor_hash)
        object.__setattr__(self, "market_feed", feed)
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "execution_watermark", watermark)
        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "revision_id", revision_id)
        object.__setattr__(self, "content_hash", digest)

    @property
    def second_event_id(self) -> str:
        return self.event_id

    @property
    def second_aggregate_event_id(self) -> str:
        return self.event_id

    @property
    def event_revision_id(self) -> str:
        return self.revision_id

    @property
    def event_hash(self) -> str:
        return self.content_hash

    @property
    def reference_price(self) -> str:
        return self.close

    @property
    def revision(self) -> int:
        return self.revision_sequence

    @property
    def interval(self) -> tuple[str, str]:
        return self.start_at, self.end_at

    @property
    def feed(self) -> MarketFeedRef:
        return self.market_feed

    @property
    def contributors(self) -> tuple[str, ...]:
        return self.contributor_ids

    @property
    def ohlcv(self) -> dict[str, str]:
        return {"open": self.open, "high": self.high, "low": self.low, "close": self.close, "volume": self.volume}

    @property
    def quality_metadata(self) -> Mapping[str, Any]:
        return copy.deepcopy(dict(self.quality))

    @property
    def adjustment_basis(self) -> str:
        return self.adjustment

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "event_id": self.event_id,
            "event_hash": self.content_hash,
            "revision_id": self.revision_id,
            "content_hash": self.content_hash,
            "symbol": self.symbol,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "market_feed": self.market_feed.to_dict(),
            "extensions": {"market_feed": self.market_feed.to_dict(), "adjustment": ADJUSTMENT},
            "adjustment": self.adjustment,
            "raw_adjustment": self.raw_adjustment,
            "adjustment_policy": ADJUSTMENT_POLICY,
            "source": SOURCE_LABEL,
            "price": self.close,
            "reference_price": self.close,
            "contributors": {"ids": list(self.contributor_ids), "hash": self.contributor_hash},
            "execution_watermark": self.execution_watermark,
            "status": self.status,
            "revision_sequence": self.revision_sequence,
            "parent_revision_id": self.parent_revision_id,
            "revision_lineage": {"parent_revision_id": self.parent_revision_id, "revision_sequence": self.revision_sequence},
            "quality": copy.deepcopy(dict(self.quality)),
        }

    as_dict = to_dict

    def validate(self) -> ValidationResult:
        return validate_second_event(self.to_dict())


def _contains_secret_like(value: Any) -> bool:
    """Reject credential-shaped metadata from canonical event payloads."""

    secret_tokens = ("secret", "token", "password", "credential", "api_key", "apikey", "authorization")
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if isinstance(key, str) and any(token in key.lower() for token in secret_tokens):
                return True
            if _contains_secret_like(nested):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_secret_like(item) for item in value)
    return False


@dataclass(frozen=True, slots=True)
class SecondReconciliationTombstone:
    """Typed future-view revision for a frozen second with no remaining OHLC.

    The immutable execution event remains in ``frozen_events``.  This
    tombstone is deliberately a separate type so consumers cannot mistake a
    removed second for a valid zero/old OHLC aggregate.
    """

    symbol: str
    start_at: str
    end_at: str
    execution_event_id: str
    execution_revision_id: str
    execution_watermark: str
    revision_sequence: int
    parent_revision_id: str
    reason: str = "no_ohlc"
    removed_trade_ids: tuple[str, ...] = ()
    quality: Mapping[str, Any] = field(default_factory=lambda: {"status": "ok", "codes": [], "connection_gap": False})
    market_feed: MarketFeedRef = ALPACA_IEX_FEED
    adjustment: str = ADJUSTMENT
    raw_adjustment: str = ADJUSTMENT
    source: str = SOURCE_LABEL
    status: str = field(default="reconciliation", init=False)
    tombstone: bool = field(default=True, init=False)
    event_id: str = field(init=False)
    revision_id: str = field(init=False)
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        start = _parse_timestamp(self.start_at)
        end = _parse_timestamp(self.end_at)
        if timestamp_ns(end) - timestamp_ns(start) != _SECOND_NS or timestamp_ns(start) % _SECOND_NS:
            raise ValueError("reconciliation tombstone requires one UTC second")
        if not isinstance(self.execution_event_id, str) or not self.execution_event_id or not isinstance(self.execution_revision_id, str) or not self.execution_revision_id:
            raise ValueError("tombstone requires immutable execution lineage")
        if not isinstance(self.parent_revision_id, str) or not self.parent_revision_id:
            raise ValueError("tombstone parent revision is required")
        if self.reason not in {"trade_cancelled", "no_ohlc", "correction_no_ohlc"}:
            raise ValueError("unknown reconciliation reason")
        if isinstance(self.revision_sequence, bool) or not isinstance(self.revision_sequence, int) or self.revision_sequence < 0:
            raise ValueError("revision_sequence must be non-negative")
        if self.adjustment != ADJUSTMENT or self.raw_adjustment != ADJUSTMENT or self.source != SOURCE_LABEL or self.market_feed != ALPACA_IEX_FEED:
            raise ValueError("tombstone source/feed/adjustment mismatch")
        watermark = _parse_timestamp(self.execution_watermark)
        if timestamp_ns(watermark) < timestamp_ns(end) + 3 * _SECOND_NS:
            raise ValueError("tombstone watermark must be at least end_at + 3 seconds")
        quality = dict(self.quality)
        if _contains_secret_like(quality):
            raise ValueError("quality metadata contains a secret-like field")
        removed = tuple(self.removed_trade_ids)
        if any(not isinstance(item, str) or not item for item in removed):
            raise ValueError("removed trade IDs must be non-empty strings")
        digest_payload = {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "symbol": self.symbol,
            "start_at": start,
            "end_at": end,
            "status": "reconciliation",
            "tombstone": True,
            "reason": self.reason,
            "execution_event_id": self.execution_event_id,
            "execution_revision_id": self.execution_revision_id,
            "execution_watermark": watermark,
            "revision_sequence": self.revision_sequence,
            "parent_revision_id": self.parent_revision_id,
            "removed_trade_ids": list(removed),
            "market_feed": self.market_feed.to_dict(),
            "adjustment": ADJUSTMENT,
            "raw_adjustment": ADJUSTMENT,
            "source": SOURCE_LABEL,
            "quality": quality,
        }
        digest = hashlib.sha256(canonical_json_bytes(digest_payload)).hexdigest()
        event_id = stable_id("sec_", {"symbol": self.symbol, "start_at": start, "end_at": end, "market_feed": self.market_feed.to_dict()})
        revision_id = stable_id("sec_rev_", {"event_id": event_id, "revision_sequence": self.revision_sequence, "content_hash": digest})
        object.__setattr__(self, "symbol", self.symbol)
        object.__setattr__(self, "start_at", start)
        object.__setattr__(self, "end_at", end)
        object.__setattr__(self, "execution_watermark", watermark)
        object.__setattr__(self, "removed_trade_ids", removed)
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "revision_id", revision_id)
        object.__setattr__(self, "content_hash", digest)

    @property
    def event_hash(self) -> str:
        return self.content_hash

    @property
    def revision(self) -> int:
        return self.revision_sequence

    @property
    def feed(self) -> MarketFeedRef:
        return self.market_feed

    @property
    def contributors(self) -> tuple[str, ...]:
        return ()

    # Explicit null OHLC fields make accidental use in a future view obvious.
    open = high = low = close = volume = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "event_id": self.event_id,
            "event_hash": self.content_hash,
            "revision_id": self.revision_id,
            "content_hash": self.content_hash,
            "symbol": self.symbol,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None,
            "market_feed": self.market_feed.to_dict(),
            "extensions": {"market_feed": self.market_feed.to_dict(), "adjustment": ADJUSTMENT},
            "adjustment": ADJUSTMENT,
            "raw_adjustment": ADJUSTMENT,
            "adjustment_policy": ADJUSTMENT_POLICY,
            "source": SOURCE_LABEL,
            "contributors": {"ids": [], "hash": None},
            "execution_watermark": self.execution_watermark,
            "execution_event_id": self.execution_event_id,
            "execution_revision_id": self.execution_revision_id,
            "status": "reconciliation",
            "tombstone": True,
            "reason": self.reason,
            "removed_trade_ids": list(self.removed_trade_ids),
            "revision_sequence": self.revision_sequence,
            "parent_revision_id": self.parent_revision_id,
            "revision_lineage": {"parent_revision_id": self.parent_revision_id, "revision_sequence": self.revision_sequence},
            "quality": copy.deepcopy(dict(self.quality)),
        }

    as_dict = to_dict


def _second_validation(valid: bool, code: str = "ok") -> ValidationResult:
    issues = [] if valid else [ValidationIssue(code, "$", "second aggregate event is malformed")]
    return ValidationResult(valid, ARTIFACT_KIND, issues)


def validate_second_event(value: Any, *, expected_feed: MarketFeedRef = ALPACA_IEX_FEED) -> ValidationResult:
    """Validate a serialized second event, including feed/hash tampering."""

    if not isinstance(value, Mapping):
        return _second_validation(False, "malformed_second_event")
    if _contains_secret_like(value):
        return _second_validation(False, "secret_like_field")
    try:
        if value.get("adjustment_policy") != ADJUSTMENT_POLICY:
            return _second_validation(False, "adjustment_mismatch")
        if value.get("source") != SOURCE_LABEL:
            return _second_validation(False, "source_mismatch")
        feed_value = value.get("market_feed")
        result = validate_market_feed_ref(feed_value, expected=expected_feed)
        if not result.valid:
            return _second_validation(False, "market_feed_mismatch")
        contributors = value.get("contributors")
        if not isinstance(contributors, Mapping) or not isinstance(contributors.get("ids"), list):
            return _second_validation(False, "contributors_malformed")
        if not contributors["ids"] or any(not isinstance(item, str) or not item for item in contributors["ids"]):
            return _second_validation(False, "contributors_malformed")
        expected_contributor_hash = hashlib.sha256(canonical_json_bytes(contributors["ids"])).hexdigest()
        if contributors.get("hash") != expected_contributor_hash:
            return _second_validation(False, "contributors_hash_mismatch")
        revision_value = value.get("revision_sequence", 0)
        if isinstance(revision_value, bool) or not isinstance(revision_value, int):
            return _second_validation(False, "revision_malformed")
        event = SecondAggregateEvent(
            value["symbol"], value["start_at"], value["end_at"], value["open"], value["high"], value["low"], value["close"], value["volume"],
            tuple(contributors["ids"]), contributors.get("hash"), value.get("execution_watermark"), value.get("status", "provisional"), revision_value, value.get("quality") or {},
            MarketFeedRef.from_mapping(feed_value), value.get("adjustment", ADJUSTMENT), value.get("raw_adjustment", ADJUSTMENT),
            (value.get("revision_lineage") or {}).get("parent_revision_id") if isinstance(value.get("revision_lineage"), Mapping) else value.get("parent_revision_id"),
            value.get("source", SOURCE_LABEL),
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return _second_validation(False, "malformed_second_event")
    return _second_validation(event.to_dict() == dict(value), "identity_mismatch")


validate_second_aggregate_event = validate_second_event
validate_event = validate_second_event


def is_valid_second_event(value: Any, *, expected_feed: MarketFeedRef = ALPACA_IEX_FEED) -> bool:
    return bool(validate_second_event(value, expected_feed=expected_feed))


@dataclass(frozen=True, slots=True)
class AggregateUpdate:
    event: SecondAggregateEvent | SecondReconciliationTombstone | None
    accepted: bool
    duplicate: bool = False
    correction: bool = False
    cancel: bool = False
    frozen: bool = False
    reconciliation: bool = False
    audit: VolumeOnlyAudit | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.accepted and self.error is None


@dataclass(slots=True)
class _Contribution:
    event: TradeEvent
    ingest_sequence: int
    rule: ConditionRule
    start_at: str
    end_at: str


class LocalSecondAggregator:
    """One-symbol registry/reducer with provisional and frozen revisions."""

    def __init__(self, symbol: str, *, market_feed: MarketFeedRef = ALPACA_IEX_FEED, adjustment: str = ADJUSTMENT) -> None:
        if not isinstance(symbol, str) or not symbol or symbol != symbol.upper() or any(char.isspace() for char in symbol):
            raise ValueError("symbol must be upper-case")
        if not isinstance(market_feed, MarketFeedRef) or market_feed != ALPACA_IEX_FEED:
            raise ValueError("G020 supports only ALPACA_IEX_FEED")
        if adjustment != ADJUSTMENT:
            raise ValueError("G020 supports raw adjustment only")
        self.symbol = symbol
        self.market_feed = market_feed
        self.adjustment = adjustment
        self._registry: dict[int | str, _Contribution] = {}
        self._history: dict[int | str, TradeEvent] = {}
        self._tombstones: set[int | str] = set()
        self._bucket_first_receipt: dict[tuple[str, str], str] = {}
        self._buckets: dict[tuple[str, str], set[int | str]] = {}
        self._revision_counters: dict[tuple[str, str], int] = {}
        self._frozen_events: dict[tuple[str, str], SecondAggregateEvent] = {}
        self._reconciliation: dict[tuple[str, str], SecondAggregateEvent | SecondReconciliationTombstone] = {}
        self._reconciliation_history: dict[tuple[str, str], list[SecondAggregateEvent | SecondReconciliationTombstone]] = {}
        # Correction/cancel fingerprints make reconnect replay idempotent even
        # after the original trade has been removed from the active registry.
        self._processed_corrections: dict[str, tuple[str, str]] = {}
        self._processed_cancels: dict[str, tuple[str, str]] = {}
        self._audits: list[VolumeOnlyAudit] = []
        self._gaps: list[ConnectionGap] = []
        # This is only the final deterministic tie-break for contributor
        # ordering. It is deliberately not a publication revision counter.
        self._sequence = 0

    def reset_for_cutover(self) -> None:
        """Clear every mutable reducer observation for a new live seam.

        The reducer object is shared by the Alpaca client and FeedSession, so
        a recovery cutover must preserve this object's identity while removing
        all state from the previous live seam.  Keep the complete reset here,
        beside the state it owns, so callers do not need to know private
        implementation fields.
        """
        self._registry.clear()
        self._history.clear()
        self._tombstones.clear()
        self._bucket_first_receipt.clear()
        self._buckets.clear()
        self._revision_counters.clear()
        self._frozen_events.clear()
        self._reconciliation.clear()
        self._reconciliation_history.clear()
        self._processed_corrections.clear()
        self._processed_cancels.clear()
        self._audits.clear()
        self._gaps.clear()
        self._sequence = 0

    @property
    def registry(self) -> Mapping[int | str, TradeEvent]:
        return {trade_id: item.event for trade_id, item in self._registry.items()}

    def original_trade(self, trade_id: int | str) -> TradeEvent | None:
        """Return the stored original trade used for correction/cancel bucketing."""

        contribution = self._registry.get(trade_id)
        return contribution.event if contribution is not None else self._history.get(trade_id)

    lookup_trade = original_trade

    @property
    def trade_history(self) -> Mapping[int | str, TradeEvent]:
        return dict(self._history)

    @property
    def audits(self) -> tuple[VolumeOnlyAudit, ...]:
        return tuple(self._audits)

    @property
    def connection_gaps(self) -> tuple[ConnectionGap, ...]:
        return tuple(self._gaps)

    @property
    def frozen_events(self) -> Mapping[tuple[str, str], SecondAggregateEvent]:
        return dict(self._frozen_events)

    @property
    def published_events(self) -> tuple[SecondAggregateEvent, ...]:
        return self.events()

    @property
    def reconciliation_events(self) -> Mapping[tuple[str, str], SecondAggregateEvent | SecondReconciliationTombstone]:
        return dict(self._reconciliation)

    @property
    def reconciliations(self) -> tuple[SecondAggregateEvent | SecondReconciliationTombstone, ...]:
        return tuple(self._reconciliation.values())

    @property
    def reconciliation_history(self) -> Mapping[tuple[str, str], tuple[SecondAggregateEvent | SecondReconciliationTombstone, ...]]:
        """All immutable post-freeze revisions, grouped by second bucket.

        ``reconciliation_events`` remains the latest-by-bucket compatibility
        view; this history is the future replay source and never overwrites an
        earlier revision.
        """

        return {bucket: tuple(events) for bucket, events in self._reconciliation_history.items()}

    @property
    def tombstones(self) -> Mapping[tuple[str, str], SecondReconciliationTombstone]:
        return {bucket: event for bucket, event in self._reconciliation.items() if isinstance(event, SecondReconciliationTombstone)}

    def mark_connection_gap(self, start_at: str, end_at: str | None, *, reason: str = "disconnect") -> ConnectionGap:
        start = _parse_timestamp(start_at)
        end = None if end_at is None else _parse_timestamp(end_at)
        if end is not None and timestamp_ns(end) <= timestamp_ns(start):
            raise ValueError("connection gap requires start before end")
        if reason not in {"disconnect", "subscription", "subscription_mismatch", "entitlement", "timeout", "authentication", "malformed_payload", "server", "protocol"}:
            reason = "disconnect"
        gap = ConnectionGap(start, end, reason)
        self._gaps.append(gap)
        return gap

    def close_connection_gap(self, end_at: str) -> ConnectionGap | None:
        """Close the most recent open gap when a reconnect becomes ready."""

        end = _parse_timestamp(end_at)
        for index in range(len(self._gaps) - 1, -1, -1):
            gap = self._gaps[index]
            if gap.end_at is None:
                if timestamp_ns(end) <= timestamp_ns(gap.start_at):
                    return gap
                closed = ConnectionGap(gap.start_at, end, gap.reason, gap.kind)
                self._gaps[index] = closed
                return closed
        return None

    def _receipt(self, event: TradeEvent) -> str:
        # Offline replays may omit receipt time.  Event time remains a safe,
        # deterministic fallback and does not change bucket identity.
        return event.received_at or event.event_time

    def _rule(self, event: TradeEvent) -> ConditionRule:
        try:
            return combine_rules(event.conditions, event.tape)
        except UnsupportedConditionError:
            raise MalformedPayloadError() from None

    @staticmethod
    def _is_active_rule(rule: ConditionRule) -> bool:
        return rule.updates_open_close or rule.updates_high_low or rule.updates_volume

    def _record_bucket(self, event: TradeEvent, *, ingest_sequence: int) -> _Contribution:
        start, end = floor_second(event.event_time)
        rule = self._rule(event)
        contribution = _Contribution(event, ingest_sequence, rule, start, end)
        bucket = (start, end)
        if not self._is_active_rule(rule):
            # Keep the raw trade in registry/history for a later correction or
            # cancel, but it is not a contributor to OHLCV or its watermark.
            return contribution
        self._buckets.setdefault(bucket, set()).add(event.trade_id)
        receipt = self._receipt(event)
        previous = self._bucket_first_receipt.get(bucket)
        if previous is None or timestamp_ns(receipt) < timestamp_ns(previous):
            self._bucket_first_receipt[bucket] = receipt
        return contribution

    def _next_revision(self, bucket: tuple[str, str]) -> int:
        latest = self._reconciliation.get(bucket) or self._frozen_events.get(bucket)
        previous = self._revision_counters.get(bucket)
        if previous is None:
            previous = latest.revision_sequence if latest is not None else -1
        elif latest is not None:
            previous = max(previous, latest.revision_sequence)
        revision = previous + 1
        self._revision_counters[bucket] = revision
        return revision

    def _recompute_first_receipt(self, bucket: tuple[str, str]) -> None:
        receipts = [self._receipt(self._registry[trade_id].event) for trade_id in self._buckets.get(bucket, set()) if trade_id in self._registry]
        if receipts:
            self._bucket_first_receipt[bucket] = min(receipts, key=timestamp_ns)
        else:
            self._bucket_first_receipt.pop(bucket, None)

    @staticmethod
    def _with_receipt(event: TradeEvent | CorrectionEvent | CancelErrorEvent, received_at: Any) -> TradeEvent | CorrectionEvent | CancelErrorEvent:
        if received_at is None:
            return event
        if isinstance(received_at, datetime):
            if received_at.tzinfo is None:
                raise MalformedPayloadError()
            received_at = received_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        try:
            receipt = canonical_timestamp(received_at)
        except Exception:
            raise MalformedPayloadError() from None
        return replace(event, received_at=receipt)

    def _freeze_existing(self, bucket: tuple[str, str], candidate: SecondAggregateEvent | None) -> SecondAggregateEvent | None:
        if bucket in self._frozen_events:
            return self._frozen_events[bucket]
        if candidate is None:
            return None
        final = SecondAggregateEvent(
            candidate.symbol,
            candidate.start_at,
            candidate.end_at,
            candidate.open,
            candidate.high,
            candidate.low,
            candidate.close,
            candidate.volume,
            candidate.contributor_ids,
            candidate.contributor_hash,
            candidate.execution_watermark,
            "frozen",
            0,
            candidate.quality,
            candidate.market_feed,
            candidate.adjustment,
            candidate.raw_adjustment,
            None,
        )
        self._frozen_events[bucket] = final
        self._revision_counters[bucket] = 0
        return final

    def _prepare_revision(self, bucket: tuple[str, str], received_at: str | None) -> bool:
        """Freeze a provisional candidate before a boundary/late revision."""

        if bucket in self._frozen_events:
            return True
        if received_at is None:
            return False
        candidate = self.aggregate(*bucket)
        if candidate is None or candidate.execution_watermark is None:
            return False
        if timestamp_ns(received_at) >= timestamp_ns(candidate.execution_watermark):
            return self._freeze_existing(bucket, candidate) is not None
        return False

    def _tombstone_for(self, bucket: tuple[str, str], *, reason: str, removed_trade_ids: Iterable[int | str]) -> SecondReconciliationTombstone | None:
        frozen = self._frozen_events.get(bucket)
        if frozen is None:
            return None
        previous = self._reconciliation.get(bucket)
        parent = previous.revision_id if previous is not None else frozen.revision_id
        latest = previous or frozen
        revision = self._revision_counters.get(bucket)
        if revision is None or revision <= latest.revision_sequence:
            revision = latest.revision_sequence + 1
            self._revision_counters[bucket] = revision
        tombstone = SecondReconciliationTombstone(
            self.symbol,
            bucket[0],
            bucket[1],
            frozen.event_id,
            frozen.revision_id,
            frozen.execution_watermark or frozen.end_at,
            revision,
            parent,
            reason,
            tuple(sorted((str(item) for item in removed_trade_ids))),
            frozen.quality,
            self.market_feed,
            self.adjustment,
            self.adjustment,
            SOURCE_LABEL,
        )
        self._record_reconciliation(tombstone)
        return tombstone

    def _record_reconciliation(self, event: SecondAggregateEvent | SecondReconciliationTombstone) -> None:
        bucket = (event.start_at, event.end_at)
        history = self._reconciliation_history.setdefault(bucket, [])
        if not history or history[-1].revision_id != event.revision_id:
            history.append(event)
        self._reconciliation[bucket] = event
        self._revision_counters[bucket] = event.revision_sequence

    def _post_state(self, bucket: tuple[str, str], *, was_frozen: bool, reason: str, removed_trade_ids: Iterable[int | str] = ()) -> SecondAggregateEvent | SecondReconciliationTombstone | None:
        candidate = self.aggregate(*bucket)
        if candidate is None and was_frozen:
            return self._tombstone_for(bucket, reason=reason, removed_trade_ids=removed_trade_ids)
        return candidate

    def add_trade(self, event: TradeEvent | Mapping[str, Any], *, received_at: Any = None) -> AggregateUpdate:
        if isinstance(event, Mapping):
            event = parse_trade(event, received_at=received_at)
        if not isinstance(event, TradeEvent) or event.symbol != self.symbol:
            raise MalformedPayloadError()
        event = self._with_receipt(event, received_at)  # type: ignore[assignment]
        existing = self._registry.get(event.trade_id)
        if existing is not None:
            # Receipt time is transport telemetry, not provider trade
            # identity.  A reconnect replay can legitimately carry a new
            # local receipt while representing the exact same trade.
            if existing.event.to_dict() == event.to_dict():
                return AggregateUpdate(None, False, duplicate=True, error=None)
            raise DuplicateTradeError()
        if event.trade_id in self._tombstones:
            return AggregateUpdate(None, False, duplicate=True, error=None)
        rule = self._rule(event)
        if not self._is_active_rule(rule):
            self._sequence += 1
            contribution = self._record_bucket(event, ingest_sequence=self._sequence)
            self._registry[event.trade_id] = contribution
            self._history[event.trade_id] = event
            return AggregateUpdate(None, True)
        bucket = floor_second(event.event_time)
        was_frozen = self._prepare_revision(bucket, event.received_at)
        if was_frozen:
            self._next_revision(bucket)
        self._sequence += 1
        contribution = self._record_bucket(event, ingest_sequence=self._sequence)
        self._registry[event.trade_id] = contribution
        self._history[event.trade_id] = event
        audit = None
        if contribution.rule.updates_volume and not contribution.rule.updates_high_low:
            audit = VolumeOnlyAudit(self.symbol, event.trade_id, contribution.start_at, contribution.end_at, event.size, event.conditions)
            self._audits.append(audit)
        latest = self._post_state(bucket, was_frozen=was_frozen, reason="late_trade")
        return AggregateUpdate(latest, True, frozen=was_frozen, reconciliation=was_frozen, audit=audit)

    ingest_trade = add_trade
    ingest = add_trade

    def apply_correction(self, event: CorrectionEvent | Mapping[str, Any], *, received_at: Any = None) -> AggregateUpdate:
        if isinstance(event, Mapping):
            event = parse_correction(event, received_at=received_at)
        if not isinstance(event, CorrectionEvent) or event.symbol != self.symbol:
            raise MalformedPayloadError()
        event = self._with_receipt(event, received_at)  # type: ignore[assignment]
        fingerprint = hashlib.sha256(canonical_json_bytes(event.to_dict())).hexdigest()
        replay_bucket = self._processed_corrections.get(fingerprint)
        if replay_bucket is not None:
            return AggregateUpdate(self.latest_event(*replay_bucket), False, duplicate=True, correction=True, reconciliation=replay_bucket in self._frozen_events)
        original = self._registry.get(event.original_trade_id)
        if original is None:
            return AggregateUpdate(None, False, correction=True, error="unknown_trade")
        original_event = original.event
        # The original tuple is authoritative.  A forged correction must not
        # mutate the registry, bucket receipt, or revision counter.
        if (
            event.exchange != original_event.exchange
            or event.tape != original_event.tape
            or event.original_price != original_event.price
            or event.original_size != original_event.size
            or event.original_conditions != original_event.conditions
        ):
            return AggregateUpdate(None, False, correction=True, error="correction_mismatch")
        if event.corrected_trade_id != event.original_trade_id and (event.corrected_trade_id in self._registry or event.corrected_trade_id in self._history):
            return AggregateUpdate(None, False, correction=True, error="corrected_trade_id_conflict")
        old_bucket = (original.start_at, original.end_at)
        # Preserve the original event time and first receipt.  The correction's
        # delivery timestamp is never allowed to move the contribution.
        original_active = self._is_active_rule(original.rule)
        corrected = TradeEvent(
            self.symbol,
            event.corrected_trade_id,
            event.exchange,
            event.corrected_price,
            event.corrected_size,
            event.corrected_conditions,
            original.event.event_time,
            event.tape,
            original.event.received_at,
        )
        # Validate the replacement's tape/condition eligibility before any
        # freeze, revision counter, or registry mutation can occur.
        corrected_rule = self._rule(corrected)
        corrected_active = self._is_active_rule(corrected_rule)
        if not original_active and corrected_active:
            # A previously ignored raw trade becomes a contributor only when
            # the correction is delivered; its publication receipt is the
            # correction receipt, not the ignored trade's receipt.
            corrected = replace(corrected, received_at=event.received_at)
        if event.corrected_trade_id == event.original_trade_id and corrected.to_dict() == original_event.to_dict():
            self._processed_corrections[fingerprint] = old_bucket
            return AggregateUpdate(self.latest_event(*old_bucket), True, correction=True, reconciliation=old_bucket in self._frozen_events)
        semantic_change = original_active or corrected_active
        if not semantic_change:
            self._sequence += 1
            self._buckets.get(old_bucket, set()).discard(event.original_trade_id)
            self._registry.pop(event.original_trade_id, None)
            self._tombstones.add(event.original_trade_id)
            self._tombstones.discard(event.corrected_trade_id)
            contribution = self._record_bucket(corrected, ingest_sequence=self._sequence)
            self._registry[event.corrected_trade_id] = contribution
            self._history[event.corrected_trade_id] = corrected
            self._processed_corrections[fingerprint] = old_bucket
            return AggregateUpdate(None, True, correction=True)
        was_frozen = self._prepare_revision(old_bucket, event.received_at)
        new_bucket = floor_second(corrected.event_time)
        if was_frozen:
            self._next_revision(old_bucket)
        self._buckets.get(old_bucket, set()).discard(event.original_trade_id)
        if not self._buckets.get(old_bucket):
            self._buckets.pop(old_bucket, None)
        self._recompute_first_receipt(old_bucket)
        self._registry.pop(event.original_trade_id, None)
        self._tombstones.add(event.original_trade_id)
        self._tombstones.discard(event.corrected_trade_id)
        self._sequence += 1
        contribution = self._record_bucket(corrected, ingest_sequence=self._sequence)
        self._registry[event.corrected_trade_id] = contribution
        self._history[event.corrected_trade_id] = corrected
        audit = None
        if contribution.rule.updates_volume and not contribution.rule.updates_high_low:
            audit = VolumeOnlyAudit(self.symbol, corrected.trade_id, contribution.start_at, contribution.end_at, corrected.size, corrected.conditions)
            self._audits.append(audit)
        self._processed_corrections[fingerprint] = new_bucket
        latest = self._post_state(new_bucket, was_frozen=was_frozen, reason="correction_no_ohlc", removed_trade_ids=(event.original_trade_id,))
        if old_bucket != new_bucket:
            self._post_state(old_bucket, was_frozen=old_bucket in self._frozen_events, reason="correction_no_ohlc", removed_trade_ids=(event.original_trade_id,))
        return AggregateUpdate(latest, True, correction=True, frozen=was_frozen, reconciliation=was_frozen, audit=audit)

    ingest_correction = apply_correction

    def apply_cancel(self, event: CancelErrorEvent | Mapping[str, Any], *, received_at: Any = None) -> AggregateUpdate:
        if isinstance(event, Mapping):
            event = parse_cancel_error(event, received_at=received_at)
        if not isinstance(event, CancelErrorEvent) or event.symbol != self.symbol:
            raise MalformedPayloadError()
        event = self._with_receipt(event, received_at)  # type: ignore[assignment]
        fingerprint = hashlib.sha256(canonical_json_bytes(event.to_dict())).hexdigest()
        replay_bucket = self._processed_cancels.get(fingerprint)
        if replay_bucket is not None:
            return AggregateUpdate(self.latest_event(*replay_bucket), False, duplicate=True, cancel=True, reconciliation=replay_bucket in self._frozen_events)
        original = self._registry.get(event.trade_id)
        if original is None:
            return AggregateUpdate(None, False, cancel=True, error="unknown_trade")
        original_event = original.event
        if event.exchange != original_event.exchange or event.tape != original_event.tape or event.price != original_event.price or event.size != original_event.size:
            return AggregateUpdate(None, False, cancel=True, error="cancel_mismatch")
        bucket = (original.start_at, original.end_at)
        if not self._is_active_rule(original.rule):
            # A fully ineligible raw trade has no aggregate state to revise;
            # remove it from the active registry while leaving the event and
            # watermark untouched.
            self._registry.pop(event.trade_id, None)
            self._tombstones.add(event.trade_id)
            self._processed_cancels[fingerprint] = bucket
            return AggregateUpdate(None, True, cancel=True)
        was_frozen = self._prepare_revision(bucket, event.received_at)
        if was_frozen:
            self._next_revision(bucket)
        self._registry.pop(event.trade_id, None)
        self._tombstones.add(event.trade_id)
        self._buckets[bucket].discard(event.trade_id)
        if not self._buckets[bucket]:
            self._buckets.pop(bucket, None)
        self._recompute_first_receipt(bucket)
        self._processed_cancels[fingerprint] = bucket
        latest = self._post_state(bucket, was_frozen=was_frozen, reason="trade_cancelled", removed_trade_ids=(event.trade_id,))
        return AggregateUpdate(latest, True, cancel=True, frozen=was_frozen, reconciliation=was_frozen)

    ingest_cancel = apply_cancel

    def ingest_message(self, payload: Any, *, received_at: Any = None) -> tuple[AggregateUpdate, ...]:
        """Parse and reduce one Alpaca message array offline."""

        updates: list[AggregateUpdate] = []
        for event in parse_message(payload, received_at=received_at):
            if isinstance(event, TradeEvent):
                updates.append(self.add_trade(event))
            elif isinstance(event, CorrectionEvent):
                updates.append(self.apply_correction(event))
            elif isinstance(event, CancelErrorEvent):
                updates.append(self.apply_cancel(event))
        return tuple(updates)

    def _is_gap(self, start_at: str, end_at: str) -> bool:
        start_ns, end_ns = timestamp_ns(start_at), timestamp_ns(end_at)
        for gap in self._gaps:
            gap_end = timestamp_ns(gap.end_at) if gap.end_at else None
            if gap_end is None or (start_ns < gap_end and end_ns > timestamp_ns(gap.start_at)):
                return True
        return False

    def _bucket_contributions(self, start_at: str, end_at: str) -> list[_Contribution]:
        ids = self._buckets.get((start_at, end_at), set())
        return [self._registry[trade_id] for trade_id in ids if trade_id in self._registry]

    def _watermark(self, start_at: str, end_at: str) -> str:
        end_ns = timestamp_ns(end_at)
        first_receipt = self._bucket_first_receipt.get((start_at, end_at), end_at)
        watermark_ns = max(timestamp_ns(first_receipt), end_ns + 3 * _SECOND_NS)
        return _timestamp_from_ns(watermark_ns)

    def aggregate(self, start_at: str, end_at: str) -> SecondAggregateEvent | None:
        start = _parse_timestamp(start_at)
        end = _parse_timestamp(end_at)
        contributions = self._bucket_contributions(start, end)
        if not contributions:
            return None
        # Registry set order is irrelevant; tie-break is fully explicit.
        contributions.sort(key=lambda item: (item.event.event_time_ns, _id_sort_key(item.event.trade_id), item.ingest_sequence))
        price_contributions = [item for item in contributions if item.rule.updates_high_low]
        volume_contributions = [item for item in contributions if item.rule.updates_volume]
        if not price_contributions:
            return None
        opening = next((item for item in contributions if item.rule.updates_open_close), None)
        closing = next((item for item in reversed(contributions) if item.rule.updates_open_close), None)
        if opening is None or closing is None:
            return None
        high = max(item.event.price for item in price_contributions)
        low = min(item.event.price for item in price_contributions)
        volume = sum((item.event.size for item in volume_contributions), 0)
        contributor_ids = tuple(str(item.event.trade_id) for item in contributions)
        contributor_hash = hashlib.sha256(canonical_json_bytes(list(contributor_ids))).hexdigest()
        values = {
            "open": _canonical_decimal(opening.event.price, positive=True),
            "high": _canonical_decimal(high, positive=True),
            "low": _canonical_decimal(low, positive=True),
            "close": _canonical_decimal(closing.event.price, positive=True),
            "volume": _canonical_decimal(volume, nonnegative=True),
        }
        bucket = (start, end)
        published = self._reconciliation.get(bucket) or self._frozen_events.get(bucket)
        watermark = self._watermark(start, end)
        gap = self._is_gap(start, end)
        quality: dict[str, Any] = {"status": "partial" if gap else "ok", "codes": ["connection_gap"] if gap else [], "connection_gap": gap}
        if isinstance(published, SecondAggregateEvent) and (
            published.symbol == self.symbol
            and published.ohlcv == values
            and published.contributor_ids == contributor_ids
            and published.contributor_hash == contributor_hash
            and published.execution_watermark == watermark
            and dict(published.quality) == quality
            and published.market_feed == self.market_feed
            and published.adjustment == self.adjustment
            and published.raw_adjustment == self.adjustment
        ):
            # A read/replay of an already published aggregate is not a new
            # publication. This also keeps repeated post-freeze observation
            # from manufacturing a self-parented revision.
            return published
        if published is None:
            revision = 0
            status = "provisional"
            lineage_parent = None
        else:
            revision = self._revision_counters.get(bucket, published.revision_sequence)
            if revision <= published.revision_sequence:
                # Normal mutation paths allocate before calling aggregate().
                # Keep direct aggregate() observations coherent if a caller
                # sees a changed post-freeze registry.
                revision = published.revision_sequence + 1
                self._revision_counters[bucket] = revision
            status = "reconciliation"
            lineage_parent = published.revision_id
        event = SecondAggregateEvent(
            self.symbol,
            start,
            end,
            values["open"],
            values["high"],
            values["low"],
            values["close"],
            values["volume"],
            contributor_ids,
            contributor_hash,
            watermark,
            status,
            revision,
            quality,
            self.market_feed,
            self.adjustment,
            self.adjustment,
            lineage_parent,
        )
        if published is not None:
            self._record_reconciliation(event)
        return event

    aggregate_second = aggregate
    build_event = aggregate

    def provisional(self, start_at: str, end_at: str) -> SecondAggregateEvent | None:
        return self.aggregate(start_at, end_at)

    def freeze(self, *, now: str | datetime | None = None) -> tuple[SecondAggregateEvent, ...]:
        if now is None:
            now = datetime.now(timezone.utc)
        if isinstance(now, datetime):
            if now.tzinfo is None:
                raise ValueError("now requires UTC timezone")
            now = now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".") + "Z" if now.microsecond else now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        now_text = _parse_timestamp(now)
        frozen: list[SecondAggregateEvent] = []
        for bucket in sorted(self._buckets, key=lambda pair: timestamp_ns(pair[0])):
            event = self.aggregate(*bucket)
            if event is None or bucket in self._frozen_events:
                continue
            if timestamp_ns(now_text) < timestamp_ns(event.execution_watermark or event.end_at):
                continue
            final = SecondAggregateEvent(
                event.symbol, event.start_at, event.end_at, event.open, event.high, event.low, event.close, event.volume,
                event.contributor_ids, event.contributor_hash, event.execution_watermark, "frozen", 0,
                event.quality, event.market_feed, event.adjustment, event.raw_adjustment,
                None,
            )
            self._frozen_events[bucket] = final
            self._revision_counters[bucket] = 0
            frozen.append(final)
        return tuple(frozen)

    freeze_ready = freeze
    publish_ready = freeze
    flush = freeze

    def event(self, start_at: str, end_at: str) -> SecondAggregateEvent | None:
        bucket = (_parse_timestamp(start_at), _parse_timestamp(end_at))
        return self._frozen_events.get(bucket) or self._reconciliation.get(bucket) or self.aggregate(*bucket)

    def latest_event(self, start_at: str, end_at: str) -> SecondAggregateEvent | SecondReconciliationTombstone | None:
        """Return the latest future-view revision while preserving ``event`` as execution view."""

        bucket = (_parse_timestamp(start_at), _parse_timestamp(end_at))
        return self._reconciliation.get(bucket) or self._frozen_events.get(bucket) or self.aggregate(*bucket)

    reconcile = latest_event

    def events(self, *, include_reconciliation: bool = False) -> tuple[SecondAggregateEvent | SecondReconciliationTombstone, ...]:
        result = list(self._frozen_events.values())
        if include_reconciliation:
            for history in self._reconciliation_history.values():
                result.extend(history)
        return tuple(sorted(result, key=lambda event: (timestamp_ns(event.start_at), event.revision_sequence)))


TradeSecondAggregator = LocalSecondAggregator
SecondAggregator = LocalSecondAggregator
AggregateEvent = SecondAggregateEvent


__all__ = [
    "ADJUSTMENT",
    "ADJUSTMENT_POLICY",
    "ARTIFACT_KIND",
    "AggregateEvent",
    "AggregateUpdate",
    "ConnectionGap",
    "LocalSecondAggregator",
    "SCHEMA_VERSION",
    "SOURCE_LABEL",
    "SecondAggregateEvent",
    "SecondReconciliationTombstone",
    "SecondAggregator",
    "TradeSecondAggregator",
    "VolumeOnlyAudit",
    "canonical_decimal",
    "floor_second",
    "execution_watermark",
    "validate_second_event",
    "validate_second_aggregate_event",
    "validate_event",
    "is_valid_second_event",
]
