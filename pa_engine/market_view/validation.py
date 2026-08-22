"""Non-throwing identity and lineage validation for G021 values."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from pa_engine.contracts import ValidationIssue, ValidationResult, bar_id_for, canonical_json_bytes, validate_artifact
from pa_engine.market import ALPACA_IEX_FEED, MarketFeedRef, validate_market_snapshot
from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent, SecondReconciliationTombstone, validate_second_event
from pa_engine.market_sources.alpaca.stream.parser import timestamp_ns

from .models import ARTIFACT_KIND, SCHEMA_VERSION, Readiness, SharedMarketView, _contains_forbidden_domain, _thaw, view_hash_projection


@dataclass(frozen=True, slots=True)
class SharedMarketViewValidation:
    valid: bool
    issues: tuple[ValidationIssue, ...] = ()
    value: Mapping[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.valid

    @property
    def error(self) -> ValidationIssue | None:
        return self.issues[0] if self.issues else None

    def to_dict(self) -> dict[str, Any]:
        return {"valid": self.valid, "issues": [{"code": issue.code, "path": issue.path, "message": issue.message} for issue in self.issues]}


def _issue(code: str, path: str, message: str) -> ValidationIssue:
    return ValidationIssue(code, path, message)


def _mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, SharedMarketView):
        return value.to_dict()
    if isinstance(value, Mapping):
        return value
    return None


_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$")
_NY = ZoneInfo("America/New_York")
_NS = 1_000_000_000


def _canonical_timestamp(value: Any) -> str:
    if not isinstance(value, str) or _UTC_RE.fullmatch(value) is None:
        raise ValueError
    parsed = timestamp_ns(value)
    seconds, nanos = divmod(parsed, _NS)
    stamp = datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    fraction = f"{nanos:09d}".rstrip("0")
    return stamp + ("." + fraction if fraction else "") + "Z"


def _cutover_aligned(value: str, timeframe: Mapping[str, Any]) -> bool:
    parsed = timestamp_ns(value)
    seconds, nanos = divmod(parsed, _NS)
    local = (datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(microseconds=nanos // 1000)).astimezone(_NY)
    cutover = datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(microseconds=nanos // 1000)
    unit, multiplier = timeframe.get("unit"), timeframe.get("multiplier")
    if not isinstance(multiplier, int) or isinstance(multiplier, bool) or multiplier < 1:
        return False
    # G019 day/week bars are local-calendar intervals.  Their UTC boundaries
    # move with DST and must never be interpreted as 09:30 RTH elapsed bars.
    if unit in {"day", "week"}:
        if local.timetz().replace(tzinfo=None) != dt_time.min:
            return False
        if local.weekday() >= 5:
            return False
        return unit == "day" or local.weekday() == 0
    if local.weekday() >= 5:
        return False
    start = datetime.combine(local.date(), dt_time(9, 30), tzinfo=_NY).astimezone(timezone.utc)
    end = datetime.combine(local.date(), dt_time(16, 0), tzinfo=_NY).astimezone(timezone.utc)
    if not (start <= cutover <= end):
        return False
    duration = {"minute": 60, "hour": 3600}.get(unit)
    if duration is None:
        return False
    return (parsed - int(start.timestamp()) * _NS) % (duration * multiplier * _NS) == 0


def _forming_interval_aligned(start_text: str, end_text: str, timeframe: Mapping[str, Any]) -> bool:
    start_ns, end_ns = timestamp_ns(start_text), timestamp_ns(end_text)
    unit, multiplier = timeframe.get("unit"), timeframe.get("multiplier")
    if not isinstance(multiplier, int) or isinstance(multiplier, bool) or multiplier < 1:
        return False
    if unit in {"day", "week"}:
        seconds, nanos = divmod(start_ns, _NS)
        start_utc = datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(microseconds=nanos // 1000)
        local = start_utc.astimezone(_NY)
        if local.timetz().replace(tzinfo=None) != dt_time.min or local.weekday() >= 5 or (unit == "week" and local.weekday() != 0):
            return False
        target_date = local.date() + timedelta(days=1 if unit == "day" else 7)
        end_local = datetime.combine(target_date, dt_time.min, tzinfo=_NY)
        return end_ns == int(end_local.timestamp()) * _NS
    if unit not in {"minute", "hour"}:
        return end_ns > start_ns
    seconds, nanos = divmod(start_ns, _NS)
    start_utc = datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(microseconds=nanos // 1000)
    local = start_utc.astimezone(_NY)
    if local.weekday() >= 5:
        return False
    session_start = datetime.combine(local.date(), dt_time(9, 30), tzinfo=_NY).astimezone(timezone.utc)
    session_end = datetime.combine(local.date(), dt_time(16, 0), tzinfo=_NY).astimezone(timezone.utc)
    duration_ns = (60 if unit == "minute" else 3600) * multiplier * _NS
    return session_start <= start_utc < session_end and end_ns == start_ns + duration_ns and end_ns <= int(session_end.timestamp()) * _NS and (start_ns - int(session_start.timestamp()) * _NS) % duration_ns == 0


def _regular_second_interval(start_text: Any, end_text: Any) -> bool:
    """Return whether one G020 interval is wholly inside regular NY RTH."""

    start_ns, end_ns = timestamp_ns(start_text), timestamp_ns(end_text)
    if end_ns - start_ns != _NS:
        return False
    seconds, nanos = divmod(start_ns, _NS)
    local = (datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(microseconds=nanos // 1000)).astimezone(_NY)
    if local.weekday() >= 5:
        return False
    session_start = datetime.combine(local.date(), dt_time(9, 30), tzinfo=_NY).astimezone(timezone.utc)
    session_end = datetime.combine(local.date(), dt_time(16, 0), tzinfo=_NY).astimezone(timezone.utc)
    return start_ns >= int(session_start.timestamp()) * _NS and end_ns <= int(session_end.timestamp()) * _NS


def _decimal_text(value: Any, *, positive: bool = False, nonnegative: bool = False) -> str:
    from decimal import Decimal

    if not isinstance(value, str) or not re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value):
        raise ValueError
    parsed = Decimal(value)
    if not parsed.is_finite() or (positive and parsed <= 0) or (nonnegative and parsed < 0):
        raise ValueError
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _valid_source_event(value: Mapping[str, Any], expected_feed: MarketFeedRef) -> bool:
    """Validate either a G020 aggregate or its typed reconciliation tombstone."""

    if value.get("tombstone") is True:
        try:
            parent = value.get("parent_revision_id") or (value.get("revision_lineage") or {}).get("parent_revision_id")
            tombstone = SecondReconciliationTombstone(
                value["symbol"], value["start_at"], value["end_at"], value["execution_event_id"], value["execution_revision_id"],
                value["execution_watermark"], value["revision_sequence"], parent, value.get("reason", "no_ohlc"),
                tuple(value.get("removed_trade_ids") or ()), value.get("quality") or {}, MarketFeedRef.from_mapping(value["market_feed"]),
                value.get("adjustment", "raw"), value.get("raw_adjustment", "raw"), value.get("source", "alpaca_iex"),
            )
            return tombstone.market_feed == expected_feed and tombstone.to_dict() == dict(value)
        except Exception:
            return False
    return bool(validate_second_event(value, expected_feed=expected_feed).valid)


_PUBLIC_WIRE_KEYS = frozenset({
    "schema_version", "artifact_kind", "view_id", "view_hash",
    "shared_market_view_id", "shared_market_view_hash", "symbol", "timeframe",
    "session", "market_feed", "adjustment_policy", "raw_adjustment_basis",
    "cutover_at", "closed_snapshot_id", "selected_timeframe_closed_snapshot_id",
    "regular_session", "fresh", "complete", "non_gapped", "sufficient",
    "frozen_second", "closed_snapshot", "closed_snapshot_ref", "observation_frame",
    "observation_frame_ref", "forming_candle", "latest_second",
    "latest_frozen_second_ref", "quality", "readiness", "revision", "decision_view_ref",
})
_QUALITY_CODES = frozenset({
    "partial", "connection_gap", "stale", "insufficient_history", "frozen_second_missing",
    "session_not_ready", "no_trade_sparse", "reconciliation_pending", "recovery_required",
})


def _source_authority_issues(
    source_events: Any,
    source_refs: Any,
    *,
    symbol: Any,
    outer_feed: Mapping[str, Any],
    cutover: str,
    analysis_as_of: str,
    expected_feed: MarketFeedRef,
) -> tuple[list[ValidationIssue], list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """Validate every embedded G020 source event and derive latest authority.

    A compact latest ID/hash is not authority.  This routine checks all
    embedded events against the outer context, as-of watermark and contiguous
    revision lineage, then returns the legal frozen candidates used to derive
    the unique latest event.
    """

    issues: list[ValidationIssue] = []
    events: list[Mapping[str, Any]] = []
    valid_events: list[Mapping[str, Any]] = []
    refs: list[Mapping[str, Any]] = []
    if not isinstance(source_events, list):
        issues.append(_issue("invalid_source_event", "$.quality.source_events", "source_events must be an array"))
    else:
        for index, event in enumerate(source_events):
            path = f"$.quality.source_events[{index}]"
            if not isinstance(event, Mapping):
                issues.append(_issue("invalid_source_event", path, "source event evidence must be an object"))
                continue
            events.append(event)
            if not _valid_source_event(event, expected_feed):
                issues.append(_issue("invalid_source_event", path, "embedded source event failed G020 validation"))
                continue
            if event.get("status") not in {"frozen", "reconciliation"}:
                issues.append(_issue("invalid_source_event", path + ".status", "embedded source event must be frozen or reconciliation"))
                continue
            valid_events.append(event)
            if event.get("symbol") != symbol:
                issues.append(_issue("source_context_mismatch", path + ".symbol", "source event symbol differs from outer view"))
            if event.get("market_feed") != dict(outer_feed):
                issues.append(_issue("source_context_mismatch", path + ".market_feed", "source event feed differs from outer view"))
            if event.get("adjustment") != "raw" or event.get("raw_adjustment") != "raw":
                issues.append(_issue("source_context_mismatch", path + ".adjustment", "source event adjustment basis must be raw"))
            try:
                start = _canonical_timestamp(event.get("start_at"))
                end = _canonical_timestamp(event.get("end_at"))
                watermark = _canonical_timestamp(event.get("execution_watermark"))
                if timestamp_ns(start) < timestamp_ns(cutover):
                    issues.append(_issue("source_cutover_mismatch", path + ".start_at", "source event starts before the half-open cutover"))
                if timestamp_ns(watermark) > timestamp_ns(analysis_as_of):
                    issues.append(_issue("source_as_of_mismatch", path + ".execution_watermark", "source event watermark is later than nested analysis_as_of"))
                if not _regular_second_interval(start, end):
                    issues.append(_issue("source_session_mismatch", path, "source event is outside regular America/New_York session"))
            except Exception:
                issues.append(_issue("invalid_source_event", path, "source event timestamps are malformed"))

    if not isinstance(source_refs, list):
        issues.append(_issue("invalid_revision_lineage", "$.quality.source_revisions", "source_revisions must be an array"))
    else:
        for index, ref in enumerate(source_refs):
            path = f"$.quality.source_revisions[{index}]"
            if not isinstance(ref, Mapping) or set(ref) != {"event_id", "event_hash", "revision"} or not isinstance(ref.get("event_id"), str) or not re.fullmatch(r"[0-9a-z_]+", ref.get("event_id", "")) or not isinstance(ref.get("event_hash"), str) or not re.fullmatch(r"[0-9a-f]{64}", ref.get("event_hash", "")) or isinstance(ref.get("revision"), bool) or not isinstance(ref.get("revision"), int) or ref.get("revision") < 0:
                issues.append(_issue("invalid_revision_lineage", path, "source revision reference is malformed"))
                continue
            refs.append(ref)

    event_keys = [(event.get("event_id"), event.get("event_hash"), event.get("revision_sequence")) for event in events]
    ref_keys = [(ref.get("event_id"), ref.get("event_hash"), ref.get("revision")) for ref in refs]
    event_key_tokens = [tuple(repr(value) for value in item) for item in event_keys]
    ref_key_tokens = [tuple(repr(value) for value in item) for item in ref_keys]
    if len(set(event_key_tokens)) != len(event_key_tokens):
        issues.append(_issue("invalid_revision_lineage", "$.quality.source_events", "source events contain duplicate revisions"))
    if len(set(ref_key_tokens)) != len(ref_keys):
        issues.append(_issue("invalid_revision_lineage", "$.quality.source_revisions", "source revisions contain duplicates"))
    stable_event_keys = sorted(event_keys, key=lambda item: tuple("" if value is None else str(value) for value in item))
    stable_ref_keys = sorted(ref_keys, key=lambda item: tuple("" if value is None else str(value) for value in item))
    if stable_event_keys != stable_ref_keys:
        issues.append(_issue("invalid_revision_lineage", "$.quality", "source_revisions must resolve exactly one-to-one to source_events"))

    by_id: dict[str, list[Mapping[str, Any]]] = {}
    for event in events:
        if isinstance(event.get("event_id"), str):
            by_id.setdefault(event["event_id"], []).append(event)
    for event_id, lineage in by_id.items():
        ordered = sorted(lineage, key=lambda event: event.get("revision_sequence") if isinstance(event.get("revision_sequence"), int) and not isinstance(event.get("revision_sequence"), bool) else -1)
        for expected_revision, event in enumerate(ordered):
            if event.get("revision_sequence") != expected_revision:
                issues.append(_issue("invalid_revision_lineage", "$.quality.source_events", f"event {event_id} revision sequence is not contiguous"))
                break
            if expected_revision == 0:
                if event.get("parent_revision_id") not in {None, ""}:
                    issues.append(_issue("invalid_revision_lineage", "$.quality.source_events", f"event {event_id} initial revision has a parent"))
            else:
                previous = ordered[expected_revision - 1]
                if event.get("parent_revision_id") != previous.get("revision_id"):
                    issues.append(_issue("invalid_revision_lineage", "$.quality.source_events", f"event {event_id} revision parent is not the immediately previous revision"))

    frozen = [event for event in valid_events if event.get("status") == "frozen" and event.get("tombstone") is not True]
    return issues, events, frozen


def _quality_readiness_issues(document: Mapping[str, Any], snapshot: Mapping[str, Any] | None, source_events: list[Mapping[str, Any]]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    quality = document.get("quality")
    readiness_value = document.get("readiness")
    if not isinstance(quality, Mapping):
        return [_issue("malformed_quality", "$.quality", "quality metadata is required")]
    if not isinstance(readiness_value, Mapping):
        return [_issue("malformed_readiness", "$.readiness", "six readiness facts are required")]
    expected_readiness_keys = {
        "connection_gap", "stale", "partial", "insufficient_history", "insufficient",
        "frozen_second", "frozen", "session_ready", "regular_session", "fresh",
        "complete", "non_gapped", "sufficient", "recovery_required", "recovery_generation",
        "executable", "codes",
    }
    if set(readiness_value) != expected_readiness_keys:
        issues.append(_issue("malformed_readiness", "$.readiness", "readiness wire shape contains unknown or missing fields"))
    expected_quality_keys = {"status", "codes", "warnings", "details", "source_revisions", "source_events", "reconciliation_revisions", "recovery_evidence"}
    if set(quality) != expected_quality_keys:
        issues.append(_issue("malformed_quality", "$.quality", "quality metadata shape contains unknown or missing fields"))
    try:
        readiness = Readiness.from_mapping(readiness_value)
    except Exception:
        return [_issue("malformed_readiness", "$.readiness", "readiness facts are malformed")]
    status = quality.get("status")
    codes = quality.get("codes")
    malformed_codes = (
        not isinstance(codes, list)
        or any(not isinstance(code, str) or code not in _QUALITY_CODES for code in codes)
        or (all(isinstance(code, str) for code in codes) and len(codes) != len(set(codes)))
    )
    if not isinstance(status, str) or status not in {"ok", "partial"} or malformed_codes:
        issues.append(_issue("quality_mismatch", "$.quality", "quality status/codes are outside the frozen readiness vocabulary"))
        return issues
    warnings = quality.get("warnings")
    if not isinstance(warnings, list) or any(not isinstance(warning, str) for warning in warnings):
        issues.append(_issue("quality_mismatch", "$.quality.warnings", "quality warnings must be a string array"))
    code_set = set(codes)
    details = quality.get("details")
    if details is not None and (not isinstance(details, Mapping) or not set(details).issubset({"no_trade_sparse_intervals", "confirmed_gap_intervals"})):
        issues.append(_issue("quality_mismatch", "$.quality.details", "quality details shape is invalid"))
    sparse_intervals = details.get("no_trade_sparse_intervals") if isinstance(details, Mapping) else None
    if "no_trade_sparse" in code_set and (not isinstance(sparse_intervals, list) or not sparse_intervals):
        issues.append(_issue("quality_mismatch", "$.quality.details", "no_trade_sparse code requires explicit sparse interval metadata"))
    if isinstance(sparse_intervals, list) and sparse_intervals and "no_trade_sparse" not in code_set:
        issues.append(_issue("quality_mismatch", "$.quality.codes", "sparse interval metadata requires no_trade_sparse code"))
    confirmed_intervals = details.get("confirmed_gap_intervals") if isinstance(details, Mapping) else None
    if "connection_gap" in code_set and (not isinstance(confirmed_intervals, list) or not confirmed_intervals):
        issues.append(_issue("quality_mismatch", "$.quality.details", "connection_gap code requires confirmed gap metadata"))
    reconciliation_refs = quality.get("reconciliation_revisions")
    expected_reconciliation = {
        (repr(event.get("event_id")), repr(event.get("event_hash")), repr(event.get("revision_sequence")))
        for event in source_events
        if event.get("status") == "reconciliation"
    }
    if not isinstance(reconciliation_refs, list):
        issues.append(_issue("quality_mismatch", "$.quality.reconciliation_revisions", "reconciliation revision evidence must be an array"))
    else:
        actual_reconciliation: list[tuple[Any, Any, Any]] = []
        for ref in reconciliation_refs:
            if not isinstance(ref, Mapping) or set(ref) != {"event_id", "event_hash", "revision"}:
                issues.append(_issue("quality_mismatch", "$.quality.reconciliation_revisions", "reconciliation revision reference is malformed"))
                continue
            actual_reconciliation.append((repr(ref.get("event_id")), repr(ref.get("event_hash")), repr(ref.get("revision"))))
        if len(actual_reconciliation) != len(set(actual_reconciliation)) or set(actual_reconciliation) != expected_reconciliation:
            issues.append(_issue("quality_mismatch", "$.quality.reconciliation_revisions", "reconciliation revisions do not resolve exactly to source evidence"))
    adverse_map = {
        "connection_gap": readiness.connection_gap,
        "stale": readiness.stale,
        "insufficient_history": readiness.insufficient_history,
        "frozen_second_missing": not readiness.frozen_second,
        "session_not_ready": not readiness.session_ready,
        "partial": readiness.partial,
    }
    for code, adverse in adverse_map.items():
        if (code in code_set) != bool(adverse):
            issues.append(_issue("quality_mismatch", "$.quality.codes", f"quality code {code} disagrees with readiness facts"))
    if (status == "partial") != readiness.partial:
        issues.append(_issue("quality_mismatch", "$.quality.status", "partial quality must agree with readiness.partial"))
    if isinstance(snapshot, Mapping):
        coverage = snapshot.get("coverage")
        if isinstance(coverage, Mapping) and coverage.get("status") == "partial" and not readiness.partial:
            issues.append(_issue("quality_mismatch", "$.closed_snapshot.coverage", "partial nested coverage requires adverse readiness"))
    if "reconciliation_pending" in code_set and (not readiness.recovery_required or readiness.frozen_second or readiness.executable):
        issues.append(_issue("quality_mismatch", "$.quality.codes", "reconciliation_pending must gate execution and frozen-second readiness"))
    if readiness.recovery_required and not ({"recovery_required", "reconciliation_pending"} & code_set):
        issues.append(_issue("quality_mismatch", "$.quality.codes", "recovery_required readiness lacks a recovery code"))
    recovery_evidence = quality.get("recovery_evidence")
    if recovery_evidence is not None:
        if not isinstance(recovery_evidence, Mapping) or set(recovery_evidence) != {"generation", "source_fingerprint"} or recovery_evidence.get("generation") != readiness.recovery_generation or isinstance(recovery_evidence.get("generation"), bool) or not isinstance(recovery_evidence.get("generation"), int) or recovery_evidence.get("generation") <= 0 or not isinstance(recovery_evidence.get("source_fingerprint"), str) or not re.fullmatch(r"[0-9a-f]{64}", recovery_evidence.get("source_fingerprint", "")):
            issues.append(_issue("quality_mismatch", "$.quality.recovery_evidence", "recovery evidence is malformed or disagrees with readiness generation"))
    if not readiness.executable and document.get("observation_frame") is not None:
        issues.append(_issue("quality_mismatch", "$.observation_frame", "non-executable readiness cannot carry a decision observation frame"))
    if readiness.executable and (status != "ok" or code_set & {"partial", "connection_gap", "stale", "insufficient_history", "frozen_second_missing", "session_not_ready", "reconciliation_pending", "recovery_required"}):
        issues.append(_issue("quality_mismatch", "$.quality", "an executable view cannot carry adverse quality/readiness facts"))
    return issues


def validate_shared_market_view(value: Any, *, expected_feed: MarketFeedRef = ALPACA_IEX_FEED) -> SharedMarketViewValidation:
    """Validate a view, its G014 lineage and every trusted second reference."""

    document = _mapping(value)
    if document is None:
        return SharedMarketViewValidation(False, (_issue("malformed_view", "$", "shared market view must be an object"),))
    issues: list[ValidationIssue] = []
    if expected_feed != ALPACA_IEX_FEED:
        issues.append(_issue("market_feed_mismatch", "$.market_feed", "G021 validator accepts only ALPACA_IEX_FEED"))
    unknown_keys = set(document) - _PUBLIC_WIRE_KEYS
    missing_keys = _PUBLIC_WIRE_KEYS - set(document)
    if unknown_keys:
        issues.append(_issue("unknown_field", "$", "shared market view contains unknown top-level fields"))
    if missing_keys:
        issues.append(_issue("malformed_view", "$", "shared market view public wire shape is incomplete"))
    if document.get("schema_version") != SCHEMA_VERSION:
        issues.append(_issue("version_mismatch", "$.schema_version", "shared market view schema_version must be 0.1.0"))
    if document.get("artifact_kind") != ARTIFACT_KIND:
        issues.append(_issue("malformed_view", "$.artifact_kind", "artifact_kind must be shared_market_view"))
    feed = document.get("market_feed")
    if not isinstance(feed, Mapping) or dict(feed) != expected_feed.to_dict():
        issues.append(_issue("market_feed_mismatch", "$.market_feed", "shared market view feed does not match expected IEX feed"))
    snapshot = document.get("closed_snapshot")
    if not isinstance(snapshot, Mapping):
        issues.append(_issue("missing_snapshot", "$.closed_snapshot", "closed snapshot is required"))
    else:
        result = validate_market_snapshot(snapshot, expected_feed=expected_feed, require_feed=True, expected_adjustment_policy="unadjusted")
        issues.extend(result.issues)
    # Outer context is independently authoritative.  Recomputing a forged
    # view hash must not make it agree with a nested G014 snapshot by accident.
    if document.get("adjustment_policy") != "unadjusted":
        issues.append(_issue("adjustment_mismatch", "$.adjustment_policy", "shared view adjustment_policy must be unadjusted"))
    if document.get("raw_adjustment_basis") != "raw":
        issues.append(_issue("raw_adjustment_mismatch", "$.raw_adjustment_basis", "raw_adjustment_basis must be raw"))
    if isinstance(snapshot, Mapping):
        nested_symbol = (snapshot.get("instrument") or {}).get("symbol") if isinstance(snapshot.get("instrument"), Mapping) else None
        if document.get("symbol") != nested_symbol:
            issues.append(_issue("context_mismatch", "$.symbol", "outer symbol differs from nested snapshot instrument"))
        if document.get("timeframe") != snapshot.get("timeframe"):
            issues.append(_issue("context_mismatch", "$.timeframe", "outer timeframe differs from nested snapshot"))
        if document.get("session") != snapshot.get("session"):
            issues.append(_issue("context_mismatch", "$.session", "outer session differs from nested snapshot"))
    timeframe = document.get("timeframe")
    if not isinstance(timeframe, Mapping) or set(timeframe) != {"unit", "multiplier"}:
        issues.append(_issue("malformed_context", "$.timeframe", "timeframe must contain unit and multiplier"))
    cutover_value: str | None = None
    try:
        cutover = _canonical_timestamp(document.get("cutover_at"))
        cutover_value = cutover
        if document.get("cutover_at") != cutover:
            issues.append(_issue("cutover_mismatch", "$.cutover_at", "cutover_at must use canonical UTC spelling"))
        if not isinstance(timeframe, Mapping) or not _cutover_aligned(cutover, timeframe):
            issues.append(_issue("cutover_mismatch", "$.cutover_at", "cutover is not canonical and aligned to the selected regular-session timeframe"))
        nested_cutover = ((snapshot.get("extensions") or {}).get("g021") or {}).get("cutover_at") if isinstance(snapshot, Mapping) else None
        if nested_cutover != cutover:
            issues.append(_issue("cutover_mismatch", "$.cutover_at", "cutover differs from nested snapshot lineage"))
    except Exception:
        issues.append(_issue("cutover_mismatch", "$.cutover_at", "cutover_at must be a canonical aligned UTC timestamp"))
    analysis_as_of: str | None = None
    if isinstance(snapshot, Mapping):
        try:
            analysis_as_of = _canonical_timestamp(snapshot.get("analysis_as_of"))
            if snapshot.get("analysis_as_of") != analysis_as_of:
                issues.append(_issue("snapshot_as_of_mismatch", "$.closed_snapshot.analysis_as_of", "nested snapshot analysis_as_of must use canonical UTC spelling"))
        except Exception:
            issues.append(_issue("snapshot_as_of_mismatch", "$.closed_snapshot.analysis_as_of", "nested snapshot analysis_as_of must be canonical UTC"))
    frame = document.get("observation_frame")
    if frame is not None:
        if not isinstance(frame, Mapping) or not isinstance(snapshot, Mapping):
            issues.append(_issue("invalid_observation_frame", "$.observation_frame", "observation frame requires a snapshot"))
        else:
            result = validate_artifact(frame, expected_kind="market_observation_frame", related_artifacts={"market_snapshot": snapshot})
            issues.extend(result.issues)
    quality_value = document.get("quality")
    source_refs_global = quality_value.get("source_revisions") if isinstance(quality_value, Mapping) else None
    source_events_global = quality_value.get("source_events") if isinstance(quality_value, Mapping) else None
    authority_events: list[Mapping[str, Any]] = []
    frozen_candidates: list[Mapping[str, Any]] = []
    if cutover_value is not None and analysis_as_of is not None:
        authority_issues, authority_events, frozen_candidates = _source_authority_issues(
            source_events_global,
            source_refs_global,
            symbol=document.get("symbol"),
            outer_feed=feed if isinstance(feed, Mapping) else {},
            cutover=cutover_value,
            analysis_as_of=analysis_as_of,
            expected_feed=expected_feed,
        )
        issues.extend(authority_issues)
    if _contains_forbidden_domain(document):
        issues.append(_issue("forbidden_domain_field", "$", "shared market view contains a prohibited strategy/model/account field"))
    source_refs = (document.get("quality") or {}).get("source_revisions") if isinstance(document.get("quality"), Mapping) else None
    source_events_for_refs = (document.get("quality") or {}).get("source_events") if isinstance(document.get("quality"), Mapping) else None
    if source_refs is not None:
        if not isinstance(source_refs, list):
            issues.append(_issue("invalid_revision_lineage", "$.quality.source_revisions", "source revisions must be an array"))
        else:
            for index, ref in enumerate(source_refs):
                if not isinstance(ref, Mapping) or not isinstance(ref.get("event_id"), str) or not isinstance(ref.get("event_hash"), str) or not re.fullmatch(r"[0-9a-f]{64}", ref.get("event_hash", "")) or isinstance(ref.get("revision"), bool) or not isinstance(ref.get("revision"), int) or ref.get("revision") < 0:
                    issues.append(_issue("invalid_revision_lineage", f"$.quality.source_revisions[{index}]", "source revision reference is malformed"))
                elif isinstance(source_events_for_refs, list):
                    if not any(isinstance(event, Mapping) and event.get("event_id") == ref.get("event_id") and event.get("event_hash") == ref.get("event_hash") and event.get("revision_sequence") == ref.get("revision") for event in source_events_for_refs):
                        issues.append(_issue("invalid_revision_lineage", f"$.quality.source_revisions[{index}]", "source revision does not resolve to embedded G020 source evidence"))
    forming = document.get("forming_candle")
    if forming is not None:
        if not isinstance(forming, Mapping):
            issues.append(_issue("malformed_forming", "$.forming_candle", "forming candle must be an object or null"))
        else:
            required_forming = {"schema_version", "artifact_kind", "symbol", "timeframe", "session", "start_at", "end_at", "open", "high", "low", "close", "volume", "source_seconds", "is_closed", "bar_id", "quality"}
            if not required_forming.issubset(forming):
                issues.append(_issue("malformed_forming", "$.forming_candle", "forming candle shape is incomplete"))
            if set(forming) != required_forming:
                issues.append(_issue("malformed_forming", "$.forming_candle", "forming candle shape contains unknown fields"))
            if forming.get("schema_version") != SCHEMA_VERSION or forming.get("artifact_kind") != "candle":
                issues.append(_issue("malformed_forming", "$.forming_candle", "forming candle artifact identity is invalid"))
            if forming.get("is_closed") is not False:
                issues.append(_issue("forming_leak", "$.forming_candle.is_closed", "forming candle must explicitly remain open"))
            if forming.get("symbol") != document.get("symbol") or forming.get("timeframe") != timeframe or forming.get("session") != document.get("session"):
                issues.append(_issue("context_mismatch", "$.forming_candle", "forming candle context differs from outer view"))
            sources = forming.get("source_seconds")
            if not isinstance(sources, list) or not sources or any(not isinstance(item, str) or not re.fullmatch(r"sec_[0-9a-f]{64}", item) for item in sources):
                issues.append(_issue("malformed_forming", "$.forming_candle.source_seconds", "forming candle source lineage is required"))
            if not isinstance(forming.get("quality"), Mapping):
                issues.append(_issue("malformed_forming", "$.forming_candle.quality", "forming candle quality metadata must be an object"))
            elif set(forming["quality"]) != {"status", "codes", "warnings", "details"}:
                issues.append(_issue("malformed_forming", "$.forming_candle.quality", "forming candle quality metadata shape is invalid"))
            try:
                start = _canonical_timestamp(forming.get("start_at"))
                end = _canonical_timestamp(forming.get("end_at"))
                if forming.get("start_at") != start or forming.get("end_at") != end:
                    raise ValueError
                if not _forming_interval_aligned(start, end, timeframe):
                    raise ValueError
                for field in ("open", "high", "low", "close"):
                    _decimal_text(forming.get(field), positive=True)
                _decimal_text(forming.get("volume"), nonnegative=True)
                from decimal import Decimal

                high = Decimal(forming["high"])
                low = Decimal(forming["low"])
                opening = Decimal(forming["open"])
                closing = Decimal(forming["close"])
                if high < max(opening, closing) or low > min(opening, closing):
                    raise ValueError
                instrument = {"asset_class": "us_equity", "symbol": document.get("symbol"), "venue": "IEX", "currency": "USD"}
                if forming.get("bar_id") != bar_id_for(instrument, timeframe, document.get("session"), start, end):
                    raise ValueError
            except Exception:
                issues.append(_issue("malformed_forming", "$.forming_candle", "forming candle timestamps or exact OHLCV are malformed"))
    latest = document.get("latest_second")
    latest_alias = document.get("latest_frozen_second_ref")
    if latest_alias != latest:
        issues.append(_issue("lineage_mismatch", "$.latest_frozen_second_ref", "latest_frozen_second_ref must exactly match latest_second"))
    if latest is not None:
        if not isinstance(latest, Mapping):
            issues.append(_issue("invalid_second_reference", "$.latest_second", "latest second must be an object"))
        else:
            # The view carries a compact trusted reference rather than a
            # second event payload.  Validate the exact lineage fields here;
            # G020 has already validated the source event before publication.
            if set(latest) != {"event_id", "event_hash", "end_at", "close"}:
                issues.append(_issue("invalid_second_reference", "$.latest_second", "latest second reference has an unexpected shape"))
            if not isinstance(latest.get("event_id"), str) or not latest.get("event_id"):
                issues.append(_issue("invalid_second_reference", "$.latest_second.event_id", "latest second event ID is required"))
            if not isinstance(latest.get("event_hash"), str) or not re.fullmatch(r"[0-9a-f]{64}", latest.get("event_hash", "")):
                issues.append(_issue("invalid_second_reference", "$.latest_second.event_hash", "latest second event hash is invalid"))
            try:
                latest_end = _canonical_timestamp(latest.get("end_at"))
                if latest.get("end_at") != latest_end:
                    raise ValueError
            except Exception:
                issues.append(_issue("invalid_second_reference", "$.latest_second.end_at", "latest second end_at is required"))
            try:
                from decimal import Decimal

                close_text = latest.get("close")
                if not isinstance(close_text, str) or not re.fullmatch(r"(?:[1-9][0-9]*(?:\.[0-9]*[1-9])?|0\.[0-9]*[1-9])", close_text) or Decimal(close_text) <= 0:
                    raise ValueError
            except Exception:
                issues.append(_issue("invalid_second_reference", "$.latest_second.close", "latest second close must be positive exact Decimal"))
            source_refs = (document.get("quality") or {}).get("source_revisions") if isinstance(document.get("quality"), Mapping) else None
            if not isinstance(source_refs, list) or not any(isinstance(ref, Mapping) and ref.get("event_id") == latest.get("event_id") and ref.get("event_hash") == latest.get("event_hash") for ref in source_refs):
                issues.append(_issue("unresolvable_second", "$.latest_second", "latest second is not bound to a source revision"))
            # A compact ID/hash pair is not authority.  The view embeds the
            # complete G020 event evidence so a serialized mapping can be
            # checked independently and forged refs cannot be rehashed into
            # acceptance.
            source_events = (document.get("quality") or {}).get("source_events") if isinstance(document.get("quality"), Mapping) else None
            if not isinstance(source_events, list):
                issues.append(_issue("unresolvable_second", "$.quality.source_events", "embedded G020 source event evidence is required"))
            else:
                matches = []
                for index, event in enumerate(source_events):
                    if not isinstance(event, Mapping):
                        issues.append(_issue("invalid_source_event", f"$.quality.source_events[{index}]", "source event evidence must be an object"))
                        continue
                    if not _valid_source_event(event, expected_feed):
                        issues.append(_issue("invalid_source_event", f"$.quality.source_events[{index}]", "embedded source event failed G020 validation"))
                        continue
                    if event.get("event_id") == latest.get("event_id") and event.get("event_hash") == latest.get("event_hash"):
                        matches.append(event)
                if not matches:
                    issues.append(_issue("unresolvable_second", "$.latest_second", "latest second does not resolve to a validated frozen source event"))
                else:
                    source = matches[0]
                    if source.get("status") != "frozen" or source.get("end_at") != latest.get("end_at") or source.get("close") != latest.get("close") or source.get("market_feed") != expected_feed.to_dict() or source.get("adjustment") != "raw" or source.get("raw_adjustment") != "raw":
                        issues.append(_issue("lineage_mismatch", "$.latest_second", "latest second does not exactly match its frozen source event"))
        # Derive the latest from every legal embedded frozen event.  A
        # reconciliation revision may gate execution, but cannot make an
        # older arbitrary frozen price authoritative.
        if frozen_candidates:
            expected_latest = max(
                frozen_candidates,
                key=lambda event: (timestamp_ns(event["end_at"]), event.get("event_id", ""), event.get("revision_sequence", 0)),
            )
            if not isinstance(latest, Mapping) or any(
                latest.get(key) != expected_latest.get(source_key)
                for key, source_key in (("event_id", "event_id"), ("event_hash", "event_hash"), ("end_at", "end_at"), ("close", "close"))
            ):
                issues.append(_issue("latest_mismatch", "$.latest_second", "latest_second is not the deterministic maximum eligible frozen source event"))
        # Even without a current second, malformed source evidence must not
        # be silently accepted as a future authority.
    elif latest_alias is not None:
        issues.append(_issue("invalid_second_reference", "$.latest_frozen_second_ref", "latest frozen reference cannot exist without latest_second"))
    elif frozen_candidates:
        issues.append(_issue("latest_mismatch", "$.latest_second", "eligible frozen source evidence requires latest_second"))
    readiness = document.get("readiness")
    if not isinstance(readiness, Mapping):
        issues.append(_issue("malformed_readiness", "$.readiness", "six readiness facts are required"))
    else:
        try:
            parsed_readiness = Readiness.from_mapping(readiness)
            if "executable" in readiness and readiness.get("executable") != parsed_readiness.executable:
                issues.append(_issue("readiness_mismatch", "$.readiness.executable", "readiness executable flag does not match its facts"))
            if "insufficient" in readiness and readiness.get("insufficient") != parsed_readiness.insufficient_history:
                issues.append(_issue("readiness_mismatch", "$.readiness.insufficient", "readiness insufficient alias does not match"))
            if "frozen" in readiness and readiness.get("frozen") != parsed_readiness.frozen_second:
                issues.append(_issue("readiness_mismatch", "$.readiness.frozen", "readiness frozen alias does not match"))
            if "codes" in readiness and readiness.get("codes") != list(parsed_readiness.codes):
                issues.append(_issue("readiness_mismatch", "$.readiness.codes", "readiness codes do not match its facts"))
            for name in ("regular_session", "fresh", "complete", "non_gapped", "sufficient", "frozen_second"):
                if name in document and document.get(name) is not getattr(parsed_readiness, name):
                    issues.append(_issue("readiness_mismatch", f"$.{name}", "scheduler readiness alias does not match nested readiness"))
            if "closed_snapshot_id" in document and isinstance(snapshot, Mapping) and document.get("closed_snapshot_id") != snapshot.get("market_snapshot_id"):
                issues.append(_issue("lineage_mismatch", "$.closed_snapshot_id", "closed snapshot ID differs from nested snapshot"))
            if "selected_timeframe_closed_snapshot_id" in document and isinstance(snapshot, Mapping) and document.get("selected_timeframe_closed_snapshot_id") != snapshot.get("market_snapshot_id"):
                issues.append(_issue("lineage_mismatch", "$.selected_timeframe_closed_snapshot_id", "selected timeframe snapshot ID differs from nested snapshot"))
        except Exception:
            issues.append(_issue("malformed_readiness", "$.readiness", "readiness facts are malformed"))
    # Quality, nested coverage, reconciliation state and the six scheduler
    # facts form one fail-closed matrix; a rehashed serialized mapping cannot
    # turn a rich partial/gapped state into executable output.
    issues.extend(_quality_readiness_issues(document, snapshot if isinstance(snapshot, Mapping) else None, authority_events))
    try:
        projection = view_hash_projection(document)
        expected_hash = hashlib.sha256(canonical_json_bytes(projection)).hexdigest()
    except Exception:
        expected_hash = None
    view_hash = document.get("view_hash")
    view_id = document.get("view_id")
    if expected_hash is None or view_hash != expected_hash or view_id != "mview_" + expected_hash:
        issues.append(_issue("identity_mismatch", "$.view_hash", "shared view identity does not match its semantic projection"))
    if "view_id" in document and document.get("shared_market_view_id") != document.get("view_id"):
        issues.append(_issue("identity_mismatch", "$.shared_market_view_id", "shared market view ID alias differs from view_id"))
    if "view_hash" in document and document.get("shared_market_view_hash") != document.get("view_hash"):
        issues.append(_issue("identity_mismatch", "$.shared_market_view_hash", "shared market view hash alias differs from view_hash"))
    if isinstance(snapshot, Mapping):
        expected_snapshot_ref = {"market_snapshot_id": snapshot.get("market_snapshot_id"), "market_snapshot_hash": snapshot.get("content_hash")}
        if document.get("closed_snapshot_ref") != expected_snapshot_ref:
            issues.append(_issue("lineage_mismatch", "$.closed_snapshot_ref", "closed snapshot reference differs from nested snapshot"))
    if isinstance(frame, Mapping):
        expected_frame_ref = {"observation_frame_id": frame.get("observation_frame_id"), "observation_frame_hash": frame.get("content_hash")}
        if document.get("observation_frame_ref") != expected_frame_ref:
            issues.append(_issue("lineage_mismatch", "$.observation_frame_ref", "observation frame reference differs from nested frame"))
    elif document.get("observation_frame_ref") is not None:
        issues.append(_issue("lineage_mismatch", "$.observation_frame_ref", "observation frame reference must be null when frame is absent"))
    # Rich view identity is intentionally distinct from G018's smv_ pair
    # identity.  If a decision ref is present, it must match its actual pair.
    decision_ref = document.get("decision_view_ref")
    if frame is None:
        if decision_ref is not None:
            issues.append(_issue("decision_ref_mismatch", "$.decision_view_ref", "decision ref must be null when observation frame is absent"))
    elif not isinstance(snapshot, Mapping) or decision_ref is None:
        issues.append(_issue("decision_ref_mismatch", "$.decision_view_ref", "observation frame requires an exact G018 decision ref"))
    else:
        try:
            from pa_engine.strategy.decision import market_view_ref_for

            if dict(decision_ref) != market_view_ref_for(snapshot, frame):
                issues.append(_issue("decision_ref_mismatch", "$.decision_view_ref", "G018 decision ref differs from snapshot/frame pair"))
        except Exception:
            issues.append(_issue("decision_ref_mismatch", "$.decision_view_ref", "G018 decision ref is malformed"))
    return SharedMarketViewValidation(not issues, tuple(issues), document if not issues else None)


def _second_mapping_from_ref(value: Mapping[str, Any]) -> dict[str, Any]:
    """Reconstruct a strict event-shaped mapping for G020's identity check."""

    # A latest-second ref alone intentionally cannot be treated as a G020
    # event; the validator therefore only accepts full event mappings.  This
    # helper keeps error reporting typed when a caller supplies a short ref.
    return dict(value)


validate_view = validate_shared_market_view
verify_shared_market_view = validate_shared_market_view
validate_shared_view = validate_shared_market_view


__all__ = ["SharedMarketViewValidation", "validate_shared_market_view", "validate_shared_view", "validate_view", "verify_shared_market_view"]
