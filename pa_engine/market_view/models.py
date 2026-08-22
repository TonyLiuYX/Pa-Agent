"""Immutable values for the G021 shared market-view boundary.

The market-view package deliberately keeps its wire values boring: all
provider-specific data is reduced to canonical JSON primitives before it is
put in a view.  A view owns a content-derived identity and never exposes the
mutable dictionaries supplied by an adapter.
"""

from __future__ import annotations

import copy
import hashlib
import re
from dataclasses import dataclass, field
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Mapping

from pa_engine.contracts import ValidationIssue, ValidationResult, canonical_json_bytes
from pa_engine.market import ALPACA_IEX_FEED, MarketFeedRef, validate_market_snapshot

SCHEMA_VERSION = "0.1.0"
ARTIFACT_KIND = "shared_market_view"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class MarketViewError(ValueError):
    """Typed fail-closed error raised at the G021 integration boundary."""

    def __init__(self, code: str, message: str, *, path: str = "$", issues: tuple[ValidationIssue, ...] = ()) -> None:
        self.code = str(code)
        self.path = str(path)
        self.issues = tuple(issues)
        super().__init__(str(message))


class InvalidJoinError(MarketViewError):
    """History/live inputs cannot form one semantically safe view."""


class InsufficientHistoryError(MarketViewError):
    """No executable closed history is available."""


class MarketViewValidationError(MarketViewError):
    """A serialized shared view failed identity or lineage validation."""


def _freeze(value: Any) -> Any:
    """Recursively isolate a JSON value and make mappings immutable."""

    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    # Decimal/objects never cross the wire; copying here still prevents a
    # caller from retaining a mutable custom object by accident.
    return copy.deepcopy(value)


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw(item) for item in value]
    return copy.deepcopy(value)


def _contains_secret_like(value: Any) -> bool:
    tokens = ("secret", "token", "password", "credential", "api_key", "apikey", "authorization")
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if isinstance(key, str) and any(token in key.lower() for token in tokens):
                return True
            if _contains_secret_like(nested):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_secret_like(item) for item in value)
    return False


def _contains_forbidden_domain(value: Any) -> bool:
    tokens = ("strategy", "model", "account", "credential", "secret", "password", "api_key", "apikey", "authorization")
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if isinstance(key, str) and any(token in key.lower() for token in tokens):
                return True
            if _contains_forbidden_domain(nested):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_forbidden_domain(item) for item in value)
    return False


def _decimal(value: Any, *, nonnegative: bool = False, positive: bool = False) -> str:
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError("market-view decimals must be exact")
    parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    if not parsed.is_finite() or (nonnegative and parsed < 0) or (positive and parsed <= 0):
        raise ValueError("market-view decimal is invalid")
    text = format(parsed, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


@dataclass(frozen=True, slots=True)
class Readiness:
    """The six fail-closed market facts consumed by downstream runtime code."""

    connection_gap: bool = False
    stale: bool = False
    partial: bool = False
    insufficient_history: bool = False
    frozen_second: bool = False
    session_ready: bool = False
    recovery_required: bool = False
    recovery_generation: int = 0

    @property
    def executable(self) -> bool:
        return not any((self.connection_gap, self.stale, self.partial, self.insufficient_history)) and self.frozen_second and self.session_ready and not self.recovery_required

    # G023 consumes these six positive, fail-closed facts directly.  Keep the
    # original G021 diagnostics above as the canonical storage so old callers
    # and the downstream scheduler cannot accidentally disagree about
    # readiness polarity.
    @property
    def regular_session(self) -> bool:
        return self.session_ready

    @property
    def fresh(self) -> bool:
        return not self.stale

    @property
    def complete(self) -> bool:
        return not self.partial

    @property
    def non_gapped(self) -> bool:
        return not self.connection_gap

    @property
    def sufficient(self) -> bool:
        return not self.insufficient_history

    @property
    def ready(self) -> bool:
        return self.executable

    @property
    def ok(self) -> bool:
        return self.executable

    @property
    def insufficient(self) -> bool:
        return self.insufficient_history

    @property
    def frozen(self) -> bool:
        return self.frozen_second

    @property
    def stale_data(self) -> bool:
        return self.stale

    @property
    def codes(self) -> tuple[str, ...]:
        values: list[str] = []
        # ``codes`` are adverse conditions, never positive readiness facts.
        # The previous implementation inverted the first four booleans and
        # therefore labelled a healthy view as gapped/stale/partial.
        if self.connection_gap:
            values.append("connection_gap")
        if self.stale:
            values.append("stale")
        if self.partial:
            values.append("partial")
        if self.insufficient_history:
            values.append("insufficient_history")
        if not self.frozen_second:
            values.append("frozen_second_missing")
        if not self.session_ready:
            values.append("session_not_ready")
        if self.recovery_required:
            values.append("recovery_required")
        return tuple(values)

    def to_dict(self) -> dict[str, Any]:
        return {
            "connection_gap": self.connection_gap,
            "stale": self.stale,
            "partial": self.partial,
            "insufficient_history": self.insufficient_history,
            "insufficient": self.insufficient_history,
            "frozen_second": self.frozen_second,
            "frozen": self.frozen_second,
            "session_ready": self.session_ready,
            "regular_session": self.regular_session,
            "fresh": self.fresh,
            "complete": self.complete,
            "non_gapped": self.non_gapped,
            "sufficient": self.sufficient,
            "recovery_required": self.recovery_required,
            "recovery_generation": self.recovery_generation,
            "executable": self.executable,
            "codes": list(self.codes),
        }

    as_dict = to_dict

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "Readiness":
        if not isinstance(value, Mapping):
            raise TypeError("readiness must be an object")
        # Accept both the original adverse facts and the frozen G023 positive
        # aliases.  If both forms are present they must agree; silently
        # preferring one would make serialized tampering invisible.
        aliases = {
            "connection_gap": ("connection_gap", "non_gapped", True),
            "stale": ("stale", "fresh", True),
            "partial": ("partial", "complete", True),
            "insufficient_history": ("insufficient_history", "sufficient", True),
            "session_ready": ("session_ready", "regular_session", False),
        }
        parsed: dict[str, bool] = {}
        for adverse, (negative_key, positive_key, invert_positive) in aliases.items():
            has_negative = negative_key in value
            has_positive = positive_key in value
            negative = value.get(negative_key) if has_negative else None
            positive = value.get(positive_key) if has_positive else None
            if has_negative and type(negative) is not bool:
                raise ValueError("readiness facts must be boolean")
            if has_positive and type(positive) is not bool:
                raise ValueError("readiness aliases must be boolean")
            if has_negative and has_positive and ((negative == positive) if invert_positive else (negative != positive)):
                raise ValueError("readiness alias polarity mismatch")
            parsed[adverse] = bool(negative) if has_negative else (not bool(positive) if invert_positive else bool(positive)) if has_positive else False
        if "insufficient" in value:
            if type(value.get("insufficient")) is not bool:
                raise ValueError("readiness aliases must be boolean")
            if "insufficient_history" in value and value.get("insufficient_history") != value.get("insufficient"):
                raise ValueError("readiness insufficient aliases disagree")
            parsed["insufficient_history"] = bool(value.get("insufficient"))
        frozen = value.get("frozen_second", value.get("frozen", False))
        recovery = value.get("recovery_required", False)
        if type(frozen) is not bool or type(recovery) is not bool:
            raise ValueError("readiness facts must be boolean")
        values = {
            **parsed,
            "frozen_second": frozen,
            "recovery_required": recovery,
        }
        if any(type(item) is not bool for item in values.values()):
            raise ValueError("readiness facts must be boolean")
        generation = value.get("recovery_generation", 0)
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise ValueError("recovery_generation must be a non-negative integer")
        return cls(**values, recovery_generation=generation)


@dataclass(frozen=True, slots=True)
class SecondReference:
    """Trusted current-price lineage for an execution-frozen second."""

    event_id: str
    event_hash: str
    end_at: str
    close: str

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("second event ID is required")
        if not isinstance(self.event_hash, str) or not _SHA256_RE.fullmatch(self.event_hash):
            raise ValueError("second event hash is invalid")
        object.__setattr__(self, "close", _decimal(self.close, positive=True))

    @property
    def event_hash_value(self) -> str:
        return self.event_hash

    @property
    def reference_price(self) -> str:
        return self.close

    def to_dict(self) -> dict[str, str]:
        return {"event_id": self.event_id, "event_hash": self.event_hash, "end_at": self.end_at, "close": self.close}

    as_dict = to_dict

    def to_g018_ref(self) -> dict[str, str]:
        return {"schema_version": SCHEMA_VERSION, "event_id": self.event_id, "event_hash": self.event_hash, "reference_price": self.close}


@dataclass(frozen=True, slots=True)
class Candle:
    """Exact Decimal selected-timeframe candle.

    ``source_seconds`` is internal lineage metadata.  ``to_bar_dict`` emits
    only the exact G014 bar fields and therefore cannot accidentally turn a
    forming candle into closed evidence.
    """

    symbol: str
    timeframe: Mapping[str, Any]
    session: Mapping[str, Any]
    start_at: str
    end_at: str
    open: str
    high: str
    low: str
    close: str
    volume: str
    source_seconds: tuple[str, ...] = ()
    is_closed: bool = False
    quality: Mapping[str, Any] = field(default_factory=lambda: {"status": "ok", "codes": [], "warnings": [], "details": None})

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or not self.symbol or self.symbol != self.symbol.upper():
            raise ValueError("candle symbol must be upper-case")
        timeframe = dict(self.timeframe)
        session = dict(self.session)
        if set(timeframe) != {"unit", "multiplier"} or timeframe.get("unit") not in {"minute", "hour", "day", "week"} or not isinstance(timeframe.get("multiplier"), int) or isinstance(timeframe.get("multiplier"), bool) or timeframe["multiplier"] < 1:
            raise ValueError("candle timeframe is invalid")
        if session != {"kind": "regular", "timezone": "America/New_York"}:
            raise ValueError("candle session is fixed to regular America/New_York")
        values = {name: _decimal(getattr(self, name), nonnegative=name == "volume", positive=name != "volume") for name in ("open", "high", "low", "close", "volume")}
        if Decimal(values["high"]) < max(Decimal(values["open"]), Decimal(values["close"])) or Decimal(values["low"]) > min(Decimal(values["open"]), Decimal(values["close"])):
            raise ValueError("candle OHLC invariant failed")
        if not isinstance(self.is_closed, bool):
            raise ValueError("candle is_closed must be boolean")
        sources = tuple(str(item) for item in self.source_seconds)
        if any(not item for item in sources):
            raise ValueError("candle source lineage cannot be empty")
        object.__setattr__(self, "timeframe", _freeze(timeframe))
        object.__setattr__(self, "session", _freeze(session))
        for name, value in values.items():
            object.__setattr__(self, name, value)
        object.__setattr__(self, "source_seconds", sources)
        object.__setattr__(self, "quality", _freeze(dict(self.quality)))

    @property
    def bar_id(self) -> str:
        from pa_engine.contracts import bar_id_for

        return bar_id_for(_thaw(self._instrument), _thaw(self.timeframe), _thaw(self.session), self.start_at, self.end_at)

    @property
    def _instrument(self) -> Mapping[str, Any]:
        return {"asset_class": "us_equity", "symbol": self.symbol, "venue": "IEX", "currency": "USD"}

    def to_bar_dict(self) -> dict[str, Any]:
        return {
            "bar_id": self.bar_id,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "is_closed": self.is_closed,
        }

    def to_dict(self) -> dict[str, Any]:
        value = self.to_bar_dict()
        value.update({"schema_version": SCHEMA_VERSION, "artifact_kind": "candle", "symbol": self.symbol, "timeframe": _thaw(self.timeframe), "session": _thaw(self.session), "source_seconds": list(self.source_seconds), "quality": _thaw(self.quality)})
        return value

    as_dict = to_dict


@dataclass(frozen=True, slots=True)
class SharedMarketView:
    """Immutable richer market view shared by every strategy in a batch."""

    symbol: str
    timeframe: Mapping[str, Any]
    session: Mapping[str, Any]
    market_feed: MarketFeedRef
    adjustment_policy: str
    cutover_at: str
    closed_snapshot: Mapping[str, Any]
    observation_frame: Mapping[str, Any] | None
    forming_candle: Mapping[str, Any] | None
    latest_second: Mapping[str, Any] | None
    quality: Mapping[str, Any]
    readiness: Readiness
    revision: int = 0
    view_id: str = field(init=False)
    view_hash: str = field(init=False)
    decision_view_ref: Mapping[str, Any] | None = field(init=False)

    def __getattribute__(self, name: str) -> Any:
        # Return a deep copy for nested public values.  Internal storage stays
        # recursively frozen, while downstream G014/G018 validators receive
        # ordinary JSON-native dict/list values and cannot mutate the view.
        if name in {"closed_snapshot", "observation_frame", "forming_candle", "latest_second", "quality"}:
            raw = object.__getattribute__(self, name)
            return _thaw(raw) if raw is not None else None
        return object.__getattribute__(self, name)

    def __post_init__(self) -> None:
        if not isinstance(self.market_feed, MarketFeedRef) or self.market_feed != ALPACA_IEX_FEED:
            raise ValueError("shared market view feed is fixed to ALPACA_IEX_FEED")
        if self.adjustment_policy != "unadjusted":
            raise ValueError("shared market view adjustment policy is fixed to unadjusted")
        if isinstance(self.revision, bool) or not isinstance(self.revision, int) or self.revision < 0:
            raise ValueError("view revision must be non-negative")
        snapshot = _thaw(self.closed_snapshot)
        snapshot_result = validate_market_snapshot(snapshot, expected_feed=ALPACA_IEX_FEED, require_feed=True, expected_adjustment_policy="unadjusted")
        if not snapshot_result.valid:
            raise MarketViewValidationError("invalid_snapshot", "shared view closed snapshot failed G015 validation", issues=tuple(snapshot_result.issues))
        frame = _thaw(self.observation_frame) if self.observation_frame is not None else None
        if frame is not None:
            from pa_engine.contracts import validate_artifact

            frame_result = validate_artifact(frame, expected_kind="market_observation_frame", related_artifacts={"market_snapshot": snapshot})
            if not frame_result.valid:
                raise MarketViewValidationError("invalid_observation_frame", "shared view observation frame failed G014 validation", issues=tuple(frame_result.issues))
        timeframe = dict(self.timeframe)
        session = dict(self.session)
        if snapshot.get("instrument", {}).get("symbol") != self.symbol or snapshot.get("timeframe") != timeframe or snapshot.get("session") != session:
            raise InvalidJoinError("context_mismatch", "view context does not match its closed snapshot")
        readiness = self.readiness if isinstance(self.readiness, Readiness) else Readiness.from_mapping(self.readiness)
        forming = _thaw(self.forming_candle) if self.forming_candle is not None else None
        latest = _thaw(self.latest_second) if self.latest_second is not None else None
        quality = _thaw(self.quality)
        if _contains_forbidden_domain(snapshot) or _contains_forbidden_domain(frame) or _contains_forbidden_domain(forming) or _contains_forbidden_domain(latest) or _contains_forbidden_domain(quality):
            raise InvalidJoinError("forbidden_domain_field", "shared market view contains a prohibited strategy/model/account field")
        if forming is not None and forming.get("is_closed") is True:
            raise InvalidJoinError("forming_leak", "forming candle cannot be marked closed")
        # A compact ID/hash pair is not current-price authority.  The public
        # constructor must enforce the same trust boundary as the serialized
        # validator before it freezes values and derives the view identity.
        # Import lazily because validation imports this model module; at
        # construction time the package is fully initialized, so this does not
        # introduce an import cycle or recurse through SharedMarketView.
        source_events = quality.get("source_events") if isinstance(quality, Mapping) else None
        source_refs = quality.get("source_revisions") if isinstance(quality, Mapping) else None
        try:
            from .validation import _quality_readiness_issues, _source_authority_issues
            from pa_engine.market_sources.alpaca.stream.parser import timestamp_ns

            analysis_as_of = snapshot.get("analysis_as_of")
            if not isinstance(analysis_as_of, str):
                raise ValueError("closed snapshot analysis_as_of is required for source authority")
            authority_issues, authority_events, frozen_candidates = _source_authority_issues(
                source_events,
                source_refs,
                symbol=self.symbol,
                outer_feed=ALPACA_IEX_FEED.to_dict(),
                cutover=self.cutover_at,
                analysis_as_of=analysis_as_of,
                expected_feed=ALPACA_IEX_FEED,
            )
            if authority_issues:
                raise InvalidJoinError(
                    "invalid_source_authority",
                    "embedded G020 source evidence failed authority validation",
                    issues=tuple(authority_issues),
                )
            quality_issues = _quality_readiness_issues(
                {
                    "quality": quality,
                    "readiness": readiness.to_dict(),
                    "observation_frame": frame,
                },
                snapshot,
                authority_events,
            )
            if quality_issues:
                raise InvalidJoinError(
                    "quality_mismatch",
                    "shared market view quality and readiness disagree",
                    issues=tuple(quality_issues),
                )
            # Derive the sole legal current second from all embedded frozen
            # G020 events.  A caller cannot replace it with an arbitrary
            # positive Decimal and re-sign only the compact reference.
            if latest is not None:
                SecondReference(latest.get("event_id"), latest.get("event_hash"), latest.get("end_at"), latest.get("close"))
                if not frozen_candidates:
                    raise InvalidJoinError("unresolvable_second", "latest price reference has no eligible frozen source event")
                expected_latest = max(
                    frozen_candidates,
                    key=lambda event: (timestamp_ns(event["end_at"]), event.get("event_id", ""), event.get("revision_sequence", 0)),
                )
                if any(
                    latest.get(ref_key) != expected_latest.get(event_key)
                    for ref_key, event_key in (("event_id", "event_id"), ("event_hash", "event_hash"), ("end_at", "end_at"), ("close", "close"))
                ):
                    raise InvalidJoinError("latest_mismatch", "latest price reference is not the deterministic maximum frozen source event")
            elif frozen_candidates:
                raise InvalidJoinError("latest_mismatch", "eligible frozen source evidence requires latest_second")
            elif readiness.frozen_second:
                raise InvalidJoinError("latest_mismatch", "frozen-second readiness requires an eligible current source event")
        except InvalidJoinError:
            raise
        except Exception as exc:
            raise InvalidJoinError("invalid_source_authority", "shared market view source authority could not be verified") from exc
        object.__setattr__(self, "timeframe", _freeze(timeframe))
        object.__setattr__(self, "session", _freeze(session))
        object.__setattr__(self, "closed_snapshot", _freeze(snapshot))
        object.__setattr__(self, "observation_frame", _freeze(frame) if frame is not None else None)
        object.__setattr__(self, "forming_candle", _freeze(forming) if forming is not None else None)
        object.__setattr__(self, "latest_second", _freeze(latest) if latest is not None else None)
        object.__setattr__(self, "quality", _freeze(quality))
        object.__setattr__(self, "readiness", readiness)
        decision_ref: Mapping[str, Any] | None = None
        if frame is not None:
            try:
                from pa_engine.strategy.decision import market_view_ref_for

                decision_ref = market_view_ref_for(snapshot, frame)
            except Exception:
                decision_ref = None
        object.__setattr__(self, "decision_view_ref", _freeze(decision_ref) if decision_ref is not None else None)
        projection = {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "symbol": self.symbol,
            "timeframe": timeframe,
            "session": session,
            "market_feed": self.market_feed.to_dict(),
            "adjustment_policy": self.adjustment_policy,
            "cutover_at": self.cutover_at,
            "closed_snapshot_ref": {"market_snapshot_id": snapshot.get("market_snapshot_id"), "market_snapshot_hash": snapshot.get("content_hash")},
            "observation_frame_ref": {"observation_frame_id": frame.get("observation_frame_id"), "observation_frame_hash": frame.get("content_hash")} if frame is not None else None,
            "forming_candle": forming,
            "latest_second": latest,
            "quality": quality,
            "readiness": readiness.to_dict(),
            "revision": self.revision,
            "raw_adjustment_basis": "raw",
            "decision_view_ref": decision_ref,
        }
        digest = hashlib.sha256(canonical_json_bytes(projection)).hexdigest()
        object.__setattr__(self, "view_hash", digest)
        object.__setattr__(self, "view_id", "mview_" + digest)

    @property
    def content_hash(self) -> str:
        return self.view_hash

    @property
    def shared_market_view_id(self) -> str:
        """Compatibility alias for callers that do not need the G018 ref."""

        return self.view_id

    @property
    def shared_market_view_hash(self) -> str:
        return self.view_hash

    @property
    def executable(self) -> bool:
        return self.readiness.executable

    # Direct scheduler boundary aliases.  G023 does not need to know the
    # richer view internals; these six positive facts and the closed-snapshot
    # ID form the typed, fail-closed hand-off.
    @property
    def regular_session(self) -> bool:
        return self.readiness.regular_session

    @property
    def fresh(self) -> bool:
        return self.readiness.fresh

    @property
    def complete(self) -> bool:
        return self.readiness.complete

    @property
    def non_gapped(self) -> bool:
        return self.readiness.non_gapped

    @property
    def sufficient(self) -> bool:
        return self.readiness.sufficient

    @property
    def frozen_second(self) -> bool:
        return self.readiness.frozen_second

    @property
    def closed_snapshot_id(self) -> str | None:
        return self.closed_snapshot.get("market_snapshot_id")

    @property
    def market_snapshot_id(self) -> str | None:
        return self.closed_snapshot_id

    @property
    def readiness_facts(self) -> Mapping[str, Any]:
        return self.readiness.to_dict()

    @property
    def current_price(self) -> str | None:
        return self.latest_second.get("close") if self.latest_second is not None else None

    @property
    def current_price_ref(self) -> Mapping[str, Any] | None:
        return copy.deepcopy(_thaw(self.latest_second)) if self.latest_second is not None else None

    @property
    def latest_frozen_second_ref(self) -> Mapping[str, Any] | None:
        return self.current_price_ref

    @property
    def market_snapshot(self) -> Mapping[str, Any]:
        return self.closed_snapshot

    @property
    def snapshot(self) -> Mapping[str, Any]:
        return self.closed_snapshot

    @property
    def closed_snapshot_ref(self) -> Mapping[str, str]:
        snapshot = self.closed_snapshot
        return {"market_snapshot_id": snapshot.get("market_snapshot_id"), "market_snapshot_hash": snapshot.get("content_hash")}

    @property
    def observation(self) -> Mapping[str, Any] | None:
        return self.observation_frame

    @property
    def observation_frame_ref(self) -> Mapping[str, str] | None:
        frame = self.observation_frame
        return {"observation_frame_id": frame.get("observation_frame_id"), "observation_frame_hash": frame.get("content_hash")} if frame is not None else None

    def to_decision_ref(self) -> dict[str, Any] | None:
        return copy.deepcopy(_thaw(self.decision_view_ref)) if self.decision_view_ref is not None else None

    @property
    def shared_market_view_ref(self) -> Mapping[str, Any] | None:
        """G018-compatible pair ref (never the richer ``mview_`` identity)."""

        return self.to_decision_ref()

    @property
    def decision_ref(self) -> Mapping[str, Any] | None:
        return self.to_decision_ref()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "view_id": self.view_id,
            "view_hash": self.view_hash,
            "shared_market_view_id": self.view_id,
            "shared_market_view_hash": self.view_hash,
            "symbol": self.symbol,
            "timeframe": _thaw(self.timeframe),
            "session": _thaw(self.session),
            "market_feed": self.market_feed.to_dict(),
            "adjustment_policy": self.adjustment_policy,
            "raw_adjustment_basis": "raw",
            "cutover_at": self.cutover_at,
            "closed_snapshot_id": self.closed_snapshot_id,
            "selected_timeframe_closed_snapshot_id": self.closed_snapshot_id,
            "regular_session": self.regular_session,
            "fresh": self.fresh,
            "complete": self.complete,
            "non_gapped": self.non_gapped,
            "sufficient": self.sufficient,
            "frozen_second": self.frozen_second,
            "closed_snapshot": _thaw(self.closed_snapshot),
            "closed_snapshot_ref": {"market_snapshot_id": self.closed_snapshot.get("market_snapshot_id"), "market_snapshot_hash": self.closed_snapshot.get("content_hash")},
            "observation_frame": _thaw(self.observation_frame) if self.observation_frame is not None else None,
            "observation_frame_ref": {"observation_frame_id": self.observation_frame.get("observation_frame_id"), "observation_frame_hash": self.observation_frame.get("content_hash")} if self.observation_frame is not None else None,
            "forming_candle": _thaw(self.forming_candle) if self.forming_candle is not None else None,
            "latest_second": _thaw(self.latest_second) if self.latest_second is not None else None,
            "latest_frozen_second_ref": _thaw(self.latest_second) if self.latest_second is not None else None,
            "quality": _thaw(self.quality),
            "readiness": self.readiness.to_dict(),
            "revision": self.revision,
            "decision_view_ref": _thaw(self.decision_view_ref) if self.decision_view_ref is not None else None,
        }

    as_dict = to_dict

    def __deepcopy__(self, memo: dict[int, Any]) -> "SharedMarketView":
        # The object is already recursively isolated and immutable.
        return self


def view_hash_projection(value: SharedMarketView | Mapping[str, Any]) -> dict[str, Any]:
    """Return the stable semantic projection used by the view identity."""

    document = value.to_dict() if isinstance(value, SharedMarketView) else _thaw(value)
    snapshot = document.get("closed_snapshot") or {}
    frame = document.get("observation_frame")
    readiness = document.get("readiness") or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": ARTIFACT_KIND,
        "symbol": document.get("symbol"),
        "timeframe": document.get("timeframe"),
        "session": document.get("session"),
        "market_feed": document.get("market_feed"),
        "adjustment_policy": document.get("adjustment_policy"),
        "cutover_at": document.get("cutover_at"),
        "closed_snapshot_ref": {"market_snapshot_id": snapshot.get("market_snapshot_id"), "market_snapshot_hash": snapshot.get("content_hash")},
        "observation_frame_ref": {"observation_frame_id": frame.get("observation_frame_id"), "observation_frame_hash": frame.get("content_hash")} if isinstance(frame, Mapping) else None,
        "forming_candle": document.get("forming_candle"),
        "latest_second": document.get("latest_second"),
        "quality": document.get("quality"),
        "readiness": readiness,
        "revision": document.get("revision", 0),
        "raw_adjustment_basis": "raw",
        "decision_view_ref": document.get("decision_view_ref"),
    }


__all__ = [
    "ARTIFACT_KIND",
    "Candle",
    "InsufficientHistoryError",
    "InvalidJoinError",
    "MarketViewError",
    "MarketViewValidationError",
    "Readiness",
    "SCHEMA_VERSION",
    "SecondReference",
    "SharedMarketView",
    "view_hash_projection",
]
