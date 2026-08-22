"""Half-open history/live candle construction for G021."""

from __future__ import annotations

import copy
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from pa_engine.contracts import ValidationIssue, ValidationResult, bar_id_for, canonical_json_bytes, finalize_artifact, validate_artifact
from pa_engine.market import ALPACA_IEX_FEED, MarketFeedRef, assess_market, build_observation_frame, validate_market_snapshot
from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent, SecondReconciliationTombstone, validate_second_event
from pa_engine.market_sources.alpaca.stream.parser import timestamp_ns

from .models import (
    Candle,
    InsufficientHistoryError,
    InvalidJoinError,
    MarketViewError,
    Readiness,
    SecondReference,
    SharedMarketView,
    _decimal,
)

UTC = timezone.utc
NY = ZoneInfo("America/New_York")
NS = 1_000_000_000
MINUTE_NS = 60 * NS
_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$")


def _canonical_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp requires timezone")
        value = value.astimezone(UTC)
        base = value.strftime("%Y-%m-%dT%H:%M:%S")
        fraction = f"{value.microsecond * 1000:09d}".rstrip("0")
        return base + ("." + fraction if fraction else "") + "Z"
    if not isinstance(value, str) or _UTC_RE.fullmatch(value) is None:
        raise ValueError("timestamp must be canonical UTC")
    # G020's parser performs calendar-range and nanosecond validation.
    ns_value = timestamp_ns(value)
    return _format_ns(ns_value)


def _format_ns(value: int) -> str:
    seconds, nanos = divmod(int(value), NS)
    stamp = datetime.fromtimestamp(seconds, tz=UTC)
    text = stamp.strftime("%Y-%m-%dT%H:%M:%S")
    fraction = f"{nanos:09d}".rstrip("0")
    return text + ("." + fraction if fraction else "") + "Z"


def _ns(value: str) -> int:
    return timestamp_ns(value)


def _local_from_ns(value: int) -> datetime:
    seconds, nanos = divmod(value, NS)
    # G021 intervals are one-second aligned; retaining microseconds here is
    # still exact for defensive callers that supply fractional boundaries.
    return (datetime.fromtimestamp(seconds, tz=UTC) + timedelta(microseconds=nanos // 1000)).astimezone(NY)


def _utc_ns(local: datetime) -> int:
    instant = local.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = instant - epoch
    return (delta.days * 86_400 + delta.seconds) * NS + delta.microseconds * 1000


def _regular_bounds(local_date) -> tuple[int, int]:
    start = datetime.combine(local_date, dt_time(9, 30), tzinfo=NY)
    end = datetime.combine(local_date, dt_time(16, 0), tzinfo=NY)
    return _utc_ns(start), _utc_ns(end)


def _session_second(start_ns: int, end_ns: int) -> bool:
    start = _local_from_ns(start_ns)
    end = _local_from_ns(end_ns)
    if start.date() != end.date() or start.weekday() >= 5:
        return False
    session_start, session_end = _regular_bounds(start.date())
    return start_ns >= session_start and end_ns <= session_end


def _timeframe_duration_ns(timeframe: Mapping[str, Any]) -> int | None:
    unit = timeframe.get("unit")
    multiplier = timeframe.get("multiplier")
    if isinstance(multiplier, bool) or not isinstance(multiplier, int) or multiplier < 1:
        return None
    return {"minute": MINUTE_NS, "hour": 60 * MINUTE_NS}.get(unit, 0) * multiplier or None


def _same_timeframe(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    return dict(a) == dict(b)


def _as_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    # G019's HistoryResult deliberately keeps the canonical snapshot behind
    # a named property; accepting it here avoids callers accidentally passing
    # the transport/result envelope into the market-view seam.
    snapshot = getattr(value, "snapshot", None)
    if isinstance(snapshot, Mapping):
        return snapshot
    snapshot = getattr(value, "market_snapshot", None)
    if isinstance(snapshot, Mapping):
        return snapshot
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        mapped = to_dict()
        if isinstance(mapped, Mapping):
            return mapped
    raise InvalidJoinError("malformed_input", f"{label} must be an object")


def _gap_parts(value: Any) -> tuple[str, str | None]:
    if isinstance(value, Mapping):
        start, end = value.get("start_at"), value.get("end_at")
    else:
        start, end = getattr(value, "start_at", None), getattr(value, "end_at", None)
    try:
        return _canonical_timestamp(start), None if end is None else _canonical_timestamp(end)
    except Exception as exc:
        raise InvalidJoinError("malformed_gap", "connection gap interval is malformed") from exc


def _interval_intersects(start: int, end: int, gap_start: int, gap_end: int | None) -> bool:
    return start < (gap_end if gap_end is not None else end + 1) and end > gap_start


def _event_from_mapping(value: Any) -> SecondAggregateEvent | SecondReconciliationTombstone:
    wrapped = getattr(value, "event", None)
    if wrapped is not None and not isinstance(value, (SecondAggregateEvent, SecondReconciliationTombstone)):
        value = wrapped
    if isinstance(value, (SecondAggregateEvent, SecondReconciliationTombstone)):
        event = value
    else:
        mapping = _as_mapping(value, "live event")
        if mapping.get("tombstone") is True:
            # Tombstones carry no OHLC and therefore can only become
            # reconciliation context.  They are never aggregated/current
            # price input, but retain their immutable lineage in the view.
            try:
                event = SecondReconciliationTombstone(
                    mapping["symbol"], mapping["start_at"], mapping["end_at"],
                    mapping["execution_event_id"], mapping["execution_revision_id"],
                    mapping["execution_watermark"], mapping["revision_sequence"],
                    mapping.get("parent_revision_id") or (mapping.get("revision_lineage") or {}).get("parent_revision_id"),
                    mapping.get("reason", "no_ohlc"), tuple(mapping.get("removed_trade_ids") or ()),
                    mapping.get("quality") or {}, MarketFeedRef.from_mapping(mapping["market_feed"]),
                    mapping.get("adjustment", "raw"), mapping.get("raw_adjustment", "raw"), mapping.get("source", "alpaca_iex"),
                )
            except Exception as exc:
                raise InvalidJoinError("invalid_reconciliation", "reconciliation tombstone failed G020 construction") from exc
            if event.market_feed != ALPACA_IEX_FEED or event.adjustment != "raw" or event.raw_adjustment != "raw" or event.source != "alpaca_iex":
                raise InvalidJoinError("second_feed_mismatch", "reconciliation tombstone feed/adjustment/source does not match IEX raw input")
            return event
        result = validate_second_event(mapping, expected_feed=ALPACA_IEX_FEED)
        if not result.valid:
            raise InvalidJoinError("invalid_second_event", "live event failed G020 validation", issues=tuple(result.issues))
        try:
            feed = MarketFeedRef.from_mapping(mapping["market_feed"])
            contributors = mapping.get("contributors") or {}
            event = SecondAggregateEvent(
                mapping["symbol"], mapping["start_at"], mapping["end_at"], mapping["open"], mapping["high"], mapping["low"], mapping["close"], mapping["volume"],
                tuple(contributors.get("ids", ())), contributors.get("hash"), mapping.get("execution_watermark"), mapping.get("status", "provisional"), mapping.get("revision_sequence", 0), mapping.get("quality") or {}, feed, mapping.get("adjustment", "raw"), mapping.get("raw_adjustment", "raw"), mapping.get("parent_revision_id"),
            )
        except Exception as exc:
            raise InvalidJoinError("invalid_second_event", "live event failed G020 construction") from exc
    if event.market_feed != ALPACA_IEX_FEED or event.adjustment != "raw" or event.raw_adjustment != "raw" or event.source != "alpaca_iex":
        raise InvalidJoinError("second_feed_mismatch", "live event feed/adjustment/source does not match IEX raw input")
    if isinstance(event, SecondAggregateEvent) and event.status not in {"frozen", "reconciliation"}:
        raise InvalidJoinError("second_not_frozen", "live event must have status=frozen or reconciliation")
    if isinstance(event, SecondAggregateEvent) and not validate_second_event(event.to_dict(), expected_feed=ALPACA_IEX_FEED).valid:
        raise InvalidJoinError("invalid_second_event", "live event identity validation failed")
    return event


def _history_validate(snapshot: Mapping[str, Any], symbol: str, timeframe: Mapping[str, Any]) -> tuple[dict[str, Any], tuple[ValidationIssue, ...]]:
    value = copy.deepcopy(dict(snapshot))
    result = validate_market_snapshot(value, expected_feed=ALPACA_IEX_FEED, require_feed=True, expected_adjustment_policy="unadjusted")
    issues = list(result.issues)
    if not result.valid:
        raise InvalidJoinError("invalid_history_snapshot", "history snapshot failed G015 validation", issues=tuple(issues))
    instrument = value.get("instrument")
    session = value.get("session")
    if not isinstance(instrument, Mapping) or instrument.get("symbol") != symbol or instrument.get("venue") != "IEX" or instrument.get("asset_class") != "us_equity" or instrument.get("currency") != "USD":
        raise InvalidJoinError("history_context_mismatch", "history instrument must be the selected IEX symbol")
    if not isinstance(session, Mapping) or dict(session) != {"kind": "regular", "timezone": "America/New_York"}:
        raise InvalidJoinError("history_session_mismatch", "history session must be regular America/New_York")
    if not _same_timeframe(value.get("timeframe") or {}, timeframe):
        raise InvalidJoinError("history_timeframe_mismatch", "history timeframe differs from selected timeframe")
    if value.get("adjustment_policy") != "unadjusted":
        raise InvalidJoinError("history_adjustment_mismatch", "history must be unadjusted")
    return value, tuple(issues)


def _history_bars(snapshot: Mapping[str, Any], cutover_ns: int) -> tuple[dict[str, Any], ...]:
    bars = snapshot.get("bars")
    if not isinstance(bars, list) or not bars:
        raise InsufficientHistoryError("insufficient_history", "history snapshot has no bars")
    selected: list[dict[str, Any]] = []
    for item in bars:
        if not isinstance(item, Mapping):
            raise InvalidJoinError("invalid_history_bar", "history bar is malformed")
        try:
            start, end = _ns(item["start_at"]), _ns(item["end_at"])
        except Exception as exc:
            raise InvalidJoinError("invalid_history_bar", "history bar timestamps are malformed") from exc
        timeframe = snapshot.get("timeframe") if isinstance(snapshot.get("timeframe"), Mapping) else {}
        # G019 has already applied the provider's regular-session filter, but
        # repeat the semantic boundary here so a re-hashed/forged G014
        # snapshot cannot smuggle pre/post-market bars into a shared view.
        if timeframe.get("unit") in {"minute", "hour"} and not _session_second(start, end):
            raise InvalidJoinError("history_session_mismatch", "history bar is outside regular America/New_York hours")
        if start < cutover_ns < end:
            raise InvalidJoinError("history_straddle", "history bar straddles the half-open cutover")
        if end <= cutover_ns:
            if item.get("is_closed") is not True:
                raise InvalidJoinError("history_not_closed", "history input contains a non-closed bar")
            selected.append(dict(item))
    selected.sort(key=lambda item: _ns(item["start_at"]))
    return tuple(selected)


def _bucket_for_event(event: SecondAggregateEvent, timeframe: Mapping[str, Any]) -> tuple[str, str] | None:
    start_ns, end_ns = _ns(event.start_at), _ns(event.end_at)
    if not _session_second(start_ns, end_ns):
        raise InvalidJoinError("session_mismatch", "live second is outside the regular New York session")
    unit, multiplier = timeframe["unit"], timeframe["multiplier"]
    local = _local_from_ns(start_ns)
    if unit in {"minute", "hour"}:
        session_start, session_end = _regular_bounds(local.date())
        duration = _timeframe_duration_ns(timeframe)
        assert duration is not None
        offset = start_ns - session_start
        bucket_start = session_start + (offset // duration) * duration
        bucket_end = bucket_start + duration
        if bucket_end > session_end:
            # The final odd-sized session fragment is not a supported selected
            # candle.  It remains an explicit partial/forming edge.
            return None
        return _format_ns(bucket_start), _format_ns(bucket_end)
    if unit == "day":
        day_start = datetime.combine(local.date(), dt_time.min, tzinfo=NY)
        day_end = day_start + timedelta(days=1)
        return _format_ns(_utc_ns(day_start)), _format_ns(_utc_ns(day_end))
    if unit == "week":
        monday = local.date() - timedelta(days=local.weekday())
        week_start = datetime.combine(monday, dt_time.min, tzinfo=NY)
        week_end = week_start + timedelta(days=7)
        return _format_ns(_utc_ns(week_start)), _format_ns(_utc_ns(week_end))
    raise InvalidJoinError("timeframe_unsupported", "selected timeframe is not supported")


def _aggregate_bucket(symbol: str, timeframe: Mapping[str, Any], session: Mapping[str, Any], bucket: tuple[str, str], events: Sequence[SecondAggregateEvent], *, closed: bool, quality: Mapping[str, Any] | None = None) -> Candle:
    ordered = sorted(events, key=lambda event: (_ns(event.start_at), event.event_id))
    if not ordered:
        raise ValueError("cannot aggregate an empty bucket")
    opening, closing = ordered[0], ordered[-1]
    high = max(Decimal(event.high) for event in ordered)
    low = min(Decimal(event.low) for event in ordered)
    volume = sum((Decimal(event.volume) for event in ordered), Decimal(0))
    def fmt(value: Decimal) -> str:
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    return Candle(
        symbol, timeframe, session, bucket[0], bucket[1], opening.open, fmt(high), fmt(low), closing.close, fmt(volume),
        tuple(event.event_id for event in ordered), closed, quality or {"status": "ok", "codes": [], "warnings": [], "details": None},
    )


def _bar_gaps(bars: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    missing: list[dict[str, str]] = []
    for previous, current in zip(bars, bars[1:]):
        end, start = _ns(previous["end_at"]), _ns(current["start_at"])
        if start > end:
            missing.append({"start_at": previous["end_at"], "end_at": current["start_at"]})
    return missing


def _safe_policy() -> dict[str, Any]:
    value = {
        "schema_version": "0.1.0",
        "artifact_kind": "analysis_policy",
        "minimum_closed_bars": 1,
        "max_staleness_seconds": 86_400,
        "allow_partial_data": False,
        "exclude_incomplete_last_bar": True,
        "required_capabilities": ["price_action"],
        "policy_version": "0.1.0",
        "extensions": {"market_feed": ALPACA_IEX_FEED.to_dict()},
    }
    return finalize_artifact(value, "analysis_policy")


@dataclass(frozen=True, slots=True)
class MarketViewBuild:
    """Optional-result wrapper useful to callers that do not want exceptions."""

    view: SharedMarketView | None
    error: MarketViewError | None = None

    @property
    def ok(self) -> bool:
        return self.view is not None and self.error is None

    @property
    def value(self) -> SharedMarketView | None:
        return self.view


class SharedMarketViewBuilder:
    """Build one deterministic selected-timeframe view from one source stream."""

    def __init__(self, symbol: str, timeframe: Mapping[str, Any] | str, *, session: Mapping[str, Any] | None = None, market_feed: MarketFeedRef = ALPACA_IEX_FEED, adjustment_policy: str = "unadjusted", minimum_closed_bars: int = 1, stale_after_seconds: int = 60) -> None:
        if not isinstance(symbol, str) or not symbol or symbol != symbol.upper() or any(char.isspace() for char in symbol):
            raise ValueError("symbol must be an upper-case value")
        if isinstance(timeframe, str):
            match = re.fullmatch(r"(\d+)\s*(min|minute|hour|hr|day|week)s?", timeframe, re.IGNORECASE) or re.fullmatch(r"(\d+)(Min|Hour|Day|Week)", timeframe)
            if match is None:
                raise ValueError("unsupported selected timeframe")
            raw_unit = match.group(2).lower()
            timeframe = {"unit": "minute" if raw_unit in {"min", "minute"} else "hour" if raw_unit in {"hour", "hr"} else raw_unit, "multiplier": int(match.group(1))}
        if not isinstance(timeframe, Mapping) or set(timeframe) != {"unit", "multiplier"}:
            raise ValueError("timeframe must contain unit and multiplier")
        unit, multiplier = timeframe.get("unit"), timeframe.get("multiplier")
        if unit not in {"minute", "hour", "day", "week"} or isinstance(multiplier, bool) or not isinstance(multiplier, int) or multiplier < 1:
            raise ValueError("unsupported selected timeframe")
        if unit == "minute" and multiplier > 59 or unit == "hour" and multiplier > 23 or unit in {"day", "week"} and multiplier != 1:
            raise ValueError("selected timeframe is outside G019 bounds")
        if market_feed != ALPACA_IEX_FEED or adjustment_policy != "unadjusted":
            raise ValueError("G021 accepts only Alpaca IEX raw input")
        if isinstance(minimum_closed_bars, bool) or not isinstance(minimum_closed_bars, int) or minimum_closed_bars < 1:
            raise ValueError("minimum_closed_bars must be positive")
        if isinstance(stale_after_seconds, bool) or not isinstance(stale_after_seconds, int) or stale_after_seconds < 0:
            raise ValueError("stale_after_seconds must be non-negative")
        self.symbol = symbol
        self.timeframe = dict(timeframe)
        self.session = dict(session or {"kind": "regular", "timezone": "America/New_York"})
        self.market_feed = market_feed
        self.adjustment_policy = adjustment_policy
        self.minimum_closed_bars = minimum_closed_bars
        self.stale_after_seconds = stale_after_seconds
        self._last_recovery_generation = 0
        self._last_build_fingerprint: str | None = None
        self._last_build_had_unresolved_gap = False

    def _validate_cutover(self, cutover: str) -> None:
        cutover_ns = _ns(cutover)
        local = _local_from_ns(cutover_ns)
        unit = self.timeframe.get("unit")
        # Calendar bars use the same local-midnight anchors as G019.  Their
        # UTC width changes across DST, so elapsed-duration modulo arithmetic
        # would reject a valid day/week handoff (or admit a 09:30 straddle).
        if unit in {"day", "week"}:
            if local.timetz().replace(tzinfo=None) != dt_time.min:
                raise InvalidJoinError("cutover_mismatch", "day/week cutover must be at local midnight")
            if local.weekday() >= 5:
                raise InvalidJoinError("cutover_session", "day/week cutover must begin on a regular-session weekday")
            if unit == "week" and local.weekday() != 0:
                raise InvalidJoinError("cutover_mismatch", "week cutover must be Monday local midnight")
            return
        duration = _timeframe_duration_ns(self.timeframe)
        if duration is not None:
            if local.weekday() >= 5:
                raise InvalidJoinError("cutover_session", "cutover must fall in a regular America/New_York session")
            session_start, session_end = _regular_bounds(local.date())
            if not (session_start <= cutover_ns <= session_end):
                raise InvalidJoinError("cutover_session", "cutover must fall in a regular America/New_York session")
            if (cutover_ns - session_start) % duration != 0:
                raise InvalidJoinError("cutover_mismatch", "cutover is not aligned to the selected timeframe")

    def build(self, history_snapshot: Mapping[str, Any], live_events: Iterable[Any] = (), *, cutover_at: str | datetime, now_at: str | datetime | None = None, connection_gaps: Iterable[Any] = (), recovery_generation: int = 0, recovered: bool = False, recovery_evidence: Mapping[str, Any] | None = None) -> SharedMarketView:
        try:
            cutover = _canonical_timestamp(cutover_at)
            cutover_alignment_error: InvalidJoinError | None = None
            try:
                self._validate_cutover(cutover)
            except InvalidJoinError as exc:
                cutover_alignment_error = exc
            now = _canonical_timestamp(now_at) if now_at is not None else None
            history, _ = _history_validate(_as_mapping(history_snapshot, "history snapshot"), self.symbol, self.timeframe)
            cutover_ns = _ns(cutover)
            history_bars = _history_bars(history, cutover_ns)
            raw_events = live_events.values() if isinstance(live_events, Mapping) else live_events
            parsed_events = [_event_from_mapping(value) for value in raw_events]
            for event in parsed_events:
                if event.symbol != self.symbol:
                    raise InvalidJoinError("symbol_mismatch", "live event symbol differs from selected symbol")
            # Revisions are lineage, not a second execution stream.  A replay
            # of the same immutable hash is idempotent; a changed hash must
            # advance sequence and point at the immediately previous revision.
            by_event_id: dict[str, list[Any]] = {}
            event_values: list[Any] = []
            for event in sorted(parsed_events, key=lambda item: (item.event_id, item.revision_sequence, item.event_hash)):
                if event.status == "reconciliation" and event.revision_sequence == 0:
                    raise InvalidJoinError("revision_lineage", "reconciliation event must advance a frozen revision")
                lineage = by_event_id.setdefault(event.event_id, [])
                if lineage and lineage[-1].event_hash == event.event_hash:
                    continue
                if lineage:
                    previous = lineage[-1]
                    if event.revision_sequence != previous.revision_sequence + 1 or event.parent_revision_id != previous.revision_id:
                        raise InvalidJoinError("revision_lineage", "second-event revisions are not a contiguous parent-linked lineage")
                elif event.revision_sequence != 0 and event.parent_revision_id is None:
                    raise InvalidJoinError("revision_lineage", "a non-zero second revision requires its parent revision")
                elif event.revision_sequence != 0 and event.status == "reconciliation":
                    raise InvalidJoinError("revision_lineage", "reconciliation input requires its frozen parent event authority")
                lineage.append(event)
                event_values.append(event)
            event_values.sort(key=lambda item: (_ns(item.start_at), item.event_id, item.revision_sequence))
            if now is None:
                now = max([cutover] + [event.execution_watermark or event.end_at for event in event_values], key=_ns)
            now_ns = _ns(now)
            parsed_gaps = tuple(_gap_parts(value) for value in connection_gaps)
            relevant_gaps: list[tuple[str, str | None]] = []
            for gap_start, gap_end in parsed_gaps:
                gs = _ns(gap_start)
                if gap_end is not None and _ns(gap_end) <= cutover_ns:
                    continue
                if gs > now_ns:
                    continue
                local_gap = _local_from_ns(gs)
                if local_gap.weekday() >= 5:
                    continue
                session_start, session_end = _regular_bounds(local_gap.date())
                if gap_end is not None and _ns(gap_end) <= session_start:
                    continue
                if gs >= session_end:
                    continue
                relevant_gaps.append((gap_start, gap_end))
            gaps = tuple(relevant_gaps)
            if isinstance(recovery_generation, bool) or not isinstance(recovery_generation, int) or recovery_generation < 0:
                raise InvalidJoinError("recovery_generation", "recovery generation must be a non-negative integer")
            if recovery_generation < self._last_recovery_generation:
                raise InvalidJoinError("recovery_generation", "recovery generation must be monotonic")

            # Apply the half-open cutover to every event revision.  Tombstones
            # remain context only; reconciliation events never become candles.
            buckets: dict[tuple[str, str], list[SecondAggregateEvent]] = {}
            edge_partial = False
            event_partial = False
            event_gap = False
            revisions: list[int] = []
            accepted_frozen: dict[tuple[str, str], SecondAggregateEvent] = {}
            source_events: list[Any] = []
            for event in event_values:
                start_ns, end_ns = _ns(event.start_at), _ns(event.end_at)
                if end_ns <= cutover_ns:
                    continue
                if start_ns < cutover_ns < end_ns:
                    raise InvalidJoinError("cutover_straddle", "live second straddles the half-open cutover")
                if start_ns < cutover_ns:
                    raise InvalidJoinError("cutover_overlap", "live second starts before the half-open cutover")
                if not _session_second(start_ns, end_ns):
                    raise InvalidJoinError("session_mismatch", "live second is outside the regular New York session")
                # An as-of view cannot observe a second before its execution
                # publication watermark.  Drop the event before it can affect
                # source evidence, buckets, forming metadata, quality or
                # semantic identity.  The raw event is still parsed/lineage
                # checked above, but is completely invisible to this view.
                if _ns(event.execution_watermark or event.end_at) > now_ns:
                    continue
                source_events.append(event)
                quality_meta = event.quality_metadata if hasattr(event, "quality_metadata") else dict(event.quality)
                event_partial = event_partial or quality_meta.get("status") == "partial"
                event_gap = event_gap or bool(quality_meta.get("connection_gap"))
                if isinstance(event, SecondReconciliationTombstone):
                    revisions.append(event.revision_sequence)
                    continue
                if event.status == "reconciliation":
                    revisions.append(event.revision_sequence)
                    continue
                # Keep only the latest frozen execution revision per exact
                # one-second interval; a reconciliation revision is handled
                # separately and never replaces this source event.
                current = accepted_frozen.get((event.start_at, event.end_at))
                if current is not None and event.revision_sequence <= current.revision_sequence:
                    continue
                accepted_frozen[(event.start_at, event.end_at)] = event
                revisions.append(event.revision_sequence)
            if cutover_alignment_error is not None:
                raise cutover_alignment_error
            for event in accepted_frozen.values():
                bucket = _bucket_for_event(event, self.timeframe)
                if bucket is None:
                    edge_partial = True
                    continue
                buckets.setdefault(bucket, []).append(event)

            closed_live: list[Candle] = []
            forming: Candle | None = None
            for bucket in sorted(buckets, key=lambda pair: _ns(pair[0])):
                events = buckets[bucket]
                bucket_end_ns = _ns(bucket[1])
                closure_ns = max(bucket_end_ns + 3 * NS, max(_ns(event.execution_watermark or event.end_at) for event in events))
                candle = _aggregate_bucket(self.symbol, self.timeframe, self.session, bucket, events, closed=now_ns >= closure_ns)
                if candle.is_closed:
                    closed_live.append(candle)
                else:
                    if forming is not None:
                        edge_partial = True
                    forming = candle

            history_starts = {_ns(bar["start_at"]) for bar in history_bars}
            if any(_ns(candle.start_at) in history_starts for candle in closed_live):
                raise InvalidJoinError("duplicate_boundary_candle", "history and live produced the same candle interval")
            all_closed_bars = [dict(bar) for bar in history_bars] + [candle.to_bar_dict() for candle in closed_live]
            all_closed_bars.sort(key=lambda item: _ns(item["start_at"]))
            if not all_closed_bars:
                raise InsufficientHistoryError("insufficient_history", "no closed candles are available")
            if any(item.get("is_closed") is not True for item in all_closed_bars):
                raise InvalidJoinError("forming_leak", "forming candle entered closed snapshot")

            source_coverage = history.get("coverage") if isinstance(history.get("coverage"), Mapping) else {}
            source_missing: list[dict[str, str]] = []
            actual_start, actual_end = _ns(all_closed_bars[0]["start_at"]), _ns(all_closed_bars[-1]["end_at"])
            raw_source_missing = source_coverage.get("missing_intervals", []) if isinstance(source_coverage.get("missing_intervals"), list) else []
            for value in raw_source_missing:
                if not isinstance(value, Mapping):
                    continue
                try:
                    ms, me = _ns(value["start_at"]), _ns(value["end_at"])
                except Exception:
                    continue
                if me <= actual_start or ms >= actual_end:
                    continue
                # A live closed bar can completely reseed an historical
                # partial interval after the cutover; do not carry the old
                # source_partial bit into the new view.
                if any(_ns(bar["start_at"]) <= ms and _ns(bar["end_at"]) >= me for bar in all_closed_bars):
                    continue
                source_missing.append({"start_at": _canonical_timestamp(value["start_at"]), "end_at": _canonical_timestamp(value["end_at"])})
            source_partial_unresolved = bool(source_missing)
            source_partial_flag = source_coverage.get("status") == "partial" and (source_partial_unresolved or not raw_source_missing)

            # A missing candle with no explicit connection evidence is a
            # no-trade sparse interval, not a feed outage.  G014 receives the
            # latest continuous closed suffix; the richer view records the
            # skipped intervals without fabricating zero-volume bars.  A
            # confirmed outage that falls inside a coarse selected candle is
            # excluded from closed evidence first; otherwise its missing
            # interval would overlap a retained bar and G015 would reject the
            # nested snapshot.
            confirmed_markers = list(gaps)
            confirmed_markers.extend((event.start_at, event.end_at) for event in source_events if bool((event.quality_metadata if hasattr(event, "quality_metadata") else dict(event.quality)).get("connection_gap")))
            confirmed_markers.extend((item["start_at"], item["end_at"]) for item in source_missing)

            def intersects_marker(item: Mapping[str, str]) -> bool:
                start, end = _ns(item["start_at"]), _ns(item["end_at"])
                return any(_interval_intersects(start, end, _ns(marker_start), _ns(marker_end) if marker_end is not None else None) for marker_start, marker_end in confirmed_markers)

            # Keep outage-crossing candles only in richer metadata, never in
            # the G014 closed-bar evidence.  The latest safe suffix is chosen
            # below, after computing the resulting interval gaps.
            safe_bars = [bar for bar in all_closed_bars if not intersects_marker(bar)]
            if not safe_bars:
                safe_bars = list(all_closed_bars)
            bar_gaps = _bar_gaps(safe_bars)

            def is_confirmed(item: Mapping[str, str]) -> bool:
                start, end = _ns(item["start_at"]), _ns(item["end_at"])
                return any(start < (_ns(marker_end) if marker_end is not None else end + 1) and end > _ns(marker_start) for marker_start, marker_end in confirmed_markers)

            confirmed_bar_gaps = [item for item in bar_gaps if is_confirmed(item)]
            sparse_gaps = [item for item in bar_gaps if not is_confirmed(item)]
            snapshot_bars = list(safe_bars)
            if confirmed_bar_gaps:
                # Prefer bars strictly after the latest confirmed outage.  If
                # no post-gap suffix exists, retain the safe prefix instead;
                # either choice gives G015 a continuous closed interval.
                suffix_start = max(_ns(item["end_at"]) for item in confirmed_bar_gaps)
                post_gap = [bar for bar in safe_bars if _ns(bar["start_at"]) >= suffix_start]
                snapshot_bars = post_gap or [bar for bar in safe_bars if _ns(bar["end_at"]) <= min(_ns(item["start_at"]) for item in confirmed_bar_gaps)] or [safe_bars[-1]]
            elif sparse_gaps and not source_missing:
                suffix_start = max(_ns(item["end_at"]) for item in sparse_gaps)
                snapshot_bars = [bar for bar in safe_bars if _ns(bar["start_at"]) >= suffix_start]
                if not snapshot_bars:
                    snapshot_bars = [safe_bars[-1]]

            # ``recovered=True`` is intentionally only an assertion.  It can
            # clear a confirmed gap when a new reseeded source fingerprint and
            # explicit interval evidence are supplied; a same-input boolean
            # or self-reported generation never grants execution permission.
            source_fingerprint = hashlib.sha256(canonical_json_bytes({
                "history_hash": history.get("content_hash"),
                "events": [[event.event_id, event.event_hash] for event in source_events],
                "gaps": [[start, end] for start, end in gaps],
            })).hexdigest()
            evidence_valid = False
            if recovered and isinstance(recovery_evidence, Mapping):
                evidence_generation = recovery_evidence.get("generation", recovery_evidence.get("recovery_generation"))
                evidence_source = recovery_evidence.get("source_fingerprint")
                # A reseed authority may publish the canonical history hash
                # instead of the internal combined fingerprint; accepting the
                # alias still requires the changed-input guard below.
                if evidence_source is None:
                    evidence_source = recovery_evidence.get("reseeded_snapshot_hash")
                resolved = recovery_evidence.get("resolved_intervals", [])
                def _resolved_pair(value: Any) -> tuple[str, str] | None:
                    if isinstance(value, Mapping):
                        start, end = value.get("start_at"), value.get("end_at")
                    elif isinstance(value, (list, tuple)) and len(value) == 2:
                        start, end = value
                    else:
                        return None
                    try:
                        return _canonical_timestamp(start), _canonical_timestamp(end)
                    except Exception:
                        return None
                resolved_pairs = [pair for item in resolved if (pair := _resolved_pair(item)) is not None] if isinstance(resolved, list) else []
                evidence_valid = (
                    recovery_evidence.get("reseeded") is True
                    and evidence_generation == recovery_generation
                    and isinstance(evidence_generation, int) and not isinstance(evidence_generation, bool) and evidence_generation > 0
                    and isinstance(resolved, list)
                    and all(any(_ns(gap_start) <= _ns(pair[0]) and _ns(gap_end) >= _ns(pair[1]) for gap_start, gap_end in gaps if gap_end is not None) for pair in resolved_pairs)
                    and all(any(_ns(gap_start) <= _ns(pair[0]) and _ns(gap_end) >= _ns(pair[1]) for pair in resolved_pairs) for gap_start, gap_end in gaps if gap_end is not None)
                    and (evidence_source == source_fingerprint or evidence_source == history.get("content_hash"))
                    and not gaps
                    and self._last_build_fingerprint != source_fingerprint
                )
            unresolved_connection = bool(gaps or event_gap or confirmed_bar_gaps)
            active_gap = bool(unresolved_connection and not evidence_valid)
            recovery_required = bool(active_gap)
            # A reconciliation event is a new immutable context, but not an
            # execution-frozen current-price source.  Gate execution until a
            # subsequent frozen/reseeded view arrives.
            frozen_sources = [event for event in source_events if isinstance(event, SecondAggregateEvent) and event.status == "frozen"]
            frozen_by_id = {event.event_id: event for event in frozen_sources}
            reconciliation_pending = any(
                getattr(event, "status", None) == "reconciliation"
                and (event.event_id not in frozen_by_id or event.revision_sequence >= frozen_by_id[event.event_id].revision_sequence)
                for event in source_events
            )

            # Build the G014 decision snapshot from the selected continuous
            # suffix.  Confirmed outages remain explicit partial coverage;
            # sparse no-trade holes are absent rather than zero-filled.
            missing: list[dict[str, str]] = []
            for item in confirmed_bar_gaps + source_missing:
                # Never emit an interval overlapping a retained bar.  A
                # direct connection gap that was removed by suffix selection
                # remains explicit in rich quality metadata instead.
                if any(_interval_intersects(_ns(bar["start_at"]), _ns(bar["end_at"]), _ns(item["start_at"]), _ns(item["end_at"])) for bar in snapshot_bars):
                    continue
                if _ns(item["end_at"]) > _ns(snapshot_bars[0]["start_at"]) and _ns(item["start_at"]) < _ns(snapshot_bars[-1]["end_at"]):
                    missing.append(item)
            missing_pairs = sorted({(item["start_at"], item["end_at"]) for item in missing}, key=lambda pair: _ns(pair[0]))
            missing = [{"start_at": start, "end_at": end} for start, end in missing_pairs]
            partial_markers = bool(active_gap or confirmed_bar_gaps or source_partial_unresolved or source_partial_flag or edge_partial or event_partial or reconciliation_pending)
            # A reconciliation/quality marker gates the richer executable
            # view, but does not invent a G014 missing interval.  Only actual
            # unresolved coverage evidence makes the nested snapshot partial.
            coverage_partial = bool(missing)
            snapshot_quality = {"status": "partial" if coverage_partial else "ok", "codes": ["partial_data"] if coverage_partial else [], "warnings": ["partial_data"] if coverage_partial else [], "details": None}
            instrument = {"asset_class": "us_equity", "symbol": self.symbol, "venue": "IEX", "currency": "USD"}
            expected = {"start_at": snapshot_bars[0]["start_at"], "end_at": snapshot_bars[-1]["end_at"]}
            snapshot_doc = {
                "schema_version": "0.1.0",
                "artifact_kind": "market_snapshot",
                "extensions": {"market_feed": ALPACA_IEX_FEED.to_dict(), "g021": {"cutover_at": cutover, "raw_basis": "raw"}},
                "input_mode": "ohlcv",
                "instrument": instrument,
                "timeframe": dict(self.timeframe),
                "session": dict(self.session),
                "adjustment_policy": "unadjusted",
                "analysis_as_of": now,
                "coverage": {"expected": expected, "actual": expected, "status": "partial" if coverage_partial else "complete", "missing_intervals": missing},
                "bars": snapshot_bars,
                "quality": snapshot_quality,
                "generated_at": now,
            }
            snapshot_doc = finalize_artifact(snapshot_doc, "market_snapshot")
            snapshot_result = validate_market_snapshot(snapshot_doc, expected_feed=ALPACA_IEX_FEED, require_feed=True, expected_adjustment_policy="unadjusted")
            if not snapshot_result.valid:
                raise InvalidJoinError("invalid_snapshot", "joined snapshot failed G015 validation", issues=tuple(snapshot_result.issues))

            eligible_events = [event for event in frozen_sources if _ns(event.execution_watermark or event.end_at) <= now_ns]
            latest_event = max(eligible_events, key=lambda event: (_ns(event.end_at), event.event_id, event.revision_sequence)) if eligible_events else None
            latest_ref = SecondReference(latest_event.event_id, latest_event.event_hash, latest_event.end_at, latest_event.close).to_dict() if latest_event is not None else None
            local_now = _local_from_ns(now_ns)
            session_start, session_end = _regular_bounds(local_now.date()) if local_now.weekday() < 5 else (None, None)
            session_ready = bool(session_start is not None and session_start <= now_ns < session_end)
            stale = bool(latest_event is not None and session_ready and now_ns - _ns(latest_event.end_at) > self.stale_after_seconds * NS)
            insufficient = len(snapshot_bars) < self.minimum_closed_bars
            frozen_second = latest_event is not None and not reconciliation_pending
            partial = bool(partial_markers)
            if reconciliation_pending:
                recovery_required = True
            readiness = Readiness(active_gap, stale, partial, insufficient, frozen_second, session_ready, recovery_required, recovery_generation)
            quality_codes: list[str] = []
            if partial:
                quality_codes.append("partial")
            if active_gap:
                quality_codes.append("connection_gap")
            if stale:
                quality_codes.append("stale")
            if insufficient:
                quality_codes.append("insufficient_history")
            # A reconciliation revision gates the execution-frozen state even
            # while the previous frozen event remains the historical price
            # reference.  Keep quality/readiness polarity explicit so the
            # constructor and serialized validator agree.
            if latest_event is None or reconciliation_pending:
                quality_codes.append("frozen_second_missing")
            if not session_ready:
                quality_codes.append("session_not_ready")
            if sparse_gaps:
                quality_codes.append("no_trade_sparse")
            if reconciliation_pending:
                quality_codes.append("reconciliation_pending")
            if recovery_required and "recovery_required" not in quality_codes:
                quality_codes.append("recovery_required")
            source_revisions = [{"event_id": event.event_id, "event_hash": event.event_hash, "revision": event.revision_sequence} for event in source_events]
            quality = {
                "status": "partial" if partial else "ok",
                "codes": quality_codes,
                "warnings": [],
                "details": ({
                    **({"no_trade_sparse_intervals": sparse_gaps} if sparse_gaps else {}),
                    **({"confirmed_gap_intervals": [{"start_at": start, "end_at": end} for start, end in confirmed_markers]} if active_gap or confirmed_bar_gaps else {}),
                } or None),
                "source_revisions": source_revisions,
                "source_events": [event.to_dict() for event in source_events],
                "reconciliation_revisions": [{"event_id": event.event_id, "event_hash": event.event_hash, "revision": event.revision_sequence} for event in source_events if getattr(event, "status", None) == "reconciliation"],
                "recovery_evidence": {"generation": recovery_generation, "source_fingerprint": source_fingerprint} if evidence_valid else None,
            }

            frame = None
            if not partial and not stale and not insufficient and not reconciliation_pending and frozen_second and session_ready and not recovery_required:
                policy = _safe_policy()
                assessment = assess_market(snapshot_doc, policy, expected_feed=ALPACA_IEX_FEED, require_feed=True, expected_adjustment_policy="unadjusted")
                if assessment.accepted and assessment.model_call_allowed:
                    try:
                        frame = build_observation_frame(snapshot_doc, assessment, lookbacks=(2,), generated_at=now)
                    except Exception:
                        frame = None
            revision = max(revisions) if revisions else 0
            view = SharedMarketView(self.symbol, self.timeframe, self.session, ALPACA_IEX_FEED, "unadjusted", cutover, snapshot_doc, frame, forming.to_dict() if forming is not None else None, latest_ref, quality, readiness, revision)
            self._last_recovery_generation = recovery_generation
            self._last_build_fingerprint = source_fingerprint
            self._last_build_had_unresolved_gap = bool(active_gap)
            return view
        except MarketViewError:
            raise
        except (TypeError, ValueError, KeyError, OverflowError, AttributeError) as exc:
            # Keep untrusted adapter data from leaking provider/credential
            # text through this boundary.
            raise InvalidJoinError("malformed_input", "market-view input is malformed") from exc

    def try_build(self, *args: Any, **kwargs: Any) -> MarketViewBuild:
        try:
            return MarketViewBuild(self.build(*args, **kwargs), None)
        except MarketViewError as exc:
            return MarketViewBuild(None, exc)

    build_view = build
    join = build


def build_shared_market_view(history_snapshot: Mapping[str, Any], live_events: Iterable[Any] = (), *, symbol: str | None = None, timeframe: Mapping[str, Any] | None = None, cutover_at: str | datetime, now_at: str | datetime | None = None, connection_gaps: Iterable[Any] = (), minimum_closed_bars: int = 1, stale_after_seconds: int = 60, recovery_generation: int = 0, recovered: bool = False, recovery_evidence: Mapping[str, Any] | None = None) -> SharedMarketView:
    """Functional convenience API for one immutable shared view."""

    history = _as_mapping(history_snapshot, "history snapshot")
    chosen_symbol = symbol or ((history.get("instrument") or {}).get("symbol") if isinstance(history.get("instrument"), Mapping) else None)
    chosen_timeframe = timeframe or history.get("timeframe")
    if not isinstance(chosen_symbol, str) or not isinstance(chosen_timeframe, Mapping):
        raise InvalidJoinError("missing_context", "symbol and timeframe are required")
    return SharedMarketViewBuilder(chosen_symbol, chosen_timeframe, minimum_closed_bars=minimum_closed_bars, stale_after_seconds=stale_after_seconds).build(history, live_events, cutover_at=cutover_at, now_at=now_at, connection_gaps=connection_gaps, recovery_generation=recovery_generation, recovered=recovered, recovery_evidence=recovery_evidence)


join_history_live = build_shared_market_view
build_market_view = build_shared_market_view
make_shared_market_view = build_shared_market_view
CandleBuilder = SharedMarketViewBuilder
SharedCandleEngine = SharedMarketViewBuilder
MarketViewBuilder = SharedMarketViewBuilder


__all__ = [
    "MarketViewBuild",
    "CandleBuilder",
    "MarketViewBuilder",
    "SharedCandleEngine",
    "SharedMarketViewBuilder",
    "build_market_view",
    "build_shared_market_view",
    "make_shared_market_view",
    "join_history_live",
]
