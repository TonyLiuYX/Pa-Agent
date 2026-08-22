"""G028 feed session: one-symbol Alpaca IEX market-data session.

The feed session is the deterministic runtime composition seam for exactly
one selected symbol/timeframe.  It backfills raw Alpaca IEX history through
the G019 adapter, freezes locally aggregated one-second events through the
G020 aggregator, tracks connection gaps, and builds one immutable G021
shared market view per deterministic input batch.

Scope discipline
----------------
This module owns only the session composition.  It never modifies G019/G020
/G021 artifacts, never interpolates missing seconds, never fabricates bars,
and never validates recovery bars deeply (the G025/G026 reducers own that).
The feed is fixed to ``ALPACA_IEX_FEED`` raw/unadjusted input; any event,
aggregator or history snapshot that carries another feed identity fails
closed at the boundary where it is observed.

Cutover rule
------------
``cutover_at`` is the exclusive end instant of the latest closed history
bar, falling back to the history request's exclusive end when the snapshot
carries no bars.  G019's half-open filter guarantees bars never extend past
the request end, so the latest closed bar end is the unique instant that
satisfies the G021 builder's three hard requirements: no history bar
straddles the cutover, the cutover is aligned to the selected-timeframe grid
whenever the history bars are on-grid, and live seconds starting at the
cutover join cleanly (seconds ending at or before the cutover are dropped
by the builder, never treated as live input).

Recovery-evidence contract
--------------------------
``build_recovery_evidence`` returns the exact mapping the G021 builder
accepts to clear ``readiness.recovery_required`` on a reseeded view::

    {
        "reseeded": True,
        "generation": <int > 0>,              # must equal build_view(recovery_generation=...)
        "recovery_generation": <int > 0>,     # builder-accepted alias
        "resolved_intervals": [],             # must be an empty list (see below)
        "source_fingerprint": <history content_hash>,
    }

``SharedMarketViewBuilder.build`` (v0.1.0) only marks this evidence valid
when the current build passes NO connection gaps (``not gaps``) and the
resolved-interval coverage quantifiers are vacuous, so non-empty
``resolved_intervals`` actually invalidate the evidence.  The fingerprint
must equal the current history snapshot ``content_hash`` and the combined
source fingerprint must differ from the previous build (changed-input
guard).  Callers must therefore close the session gaps before the recovery
build and must never rebuild the identical recovery input twice: a
same-input rebuild makes the evidence inert and re-gates the view.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from threading import Event, RLock, Thread, current_thread
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from pa_engine.market import ALPACA_IEX_FEED, extract_market_feed
from pa_engine.market_sources.alpaca.history import DEFAULT_LIMIT, HistoryResult
from pa_engine.market_sources.alpaca.stream import LocalSecondAggregator, SecondAggregateEvent, timestamp_ns
from pa_engine.market_view import Readiness, SharedMarketView, SharedMarketViewBuilder
from pa_engine.strategy.runtime.clock import Clock, SystemClock, utc_instant

FEED_KEY = "alpaca:iex:single_exchange"
MARKET_DATA_USAGE = "internal_prototype"
_MAX_SKIPPED_REPLAYS = 1000
_NY_FEED = ZoneInfo("America/New_York")


def _session_day(instant: datetime) -> date:
    """Local regular-session day for the monotonic ingest guard.

    A new session day legitimately carries smaller UTC instants than the
    previous day's close; the guard must accept them.
    """
    if not isinstance(instant, datetime):
        instant = utc_instant(instant)
    return instant.astimezone(_NY_FEED).date()

_TIMEFRAME_UNITS = {"minute": "Min", "hour": "Hour", "day": "Day", "week": "Week"}
_TIMEFRAME_BOUNDS = {"minute": 59, "hour": 23, "day": 1, "week": 1}
_TIMEFRAME_WORDS_RE = re.compile(r"(\d+)\s*(min|minute|hour|hr|day|week)s?", re.IGNORECASE)
_TIMEFRAME_CANONICAL_RE = re.compile(r"(\d+)(Min|Hour|Day|Week)", re.IGNORECASE)
_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$")
_MISSING = object()
_STREAM_JOIN_TIMEOUT = 5.0
_STREAM_POLL_TIMEOUT = 0.25


class FeedSessionError(ValueError):
    """Typed failure at the feed-session boundary with a stable ``code``."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = str(code)
        self.message = message or code
        super().__init__(self.message)


def _normalize_timeframe(value: Mapping[str, Any] | str) -> dict[str, Any]:
    if isinstance(value, str):
        match = _TIMEFRAME_WORDS_RE.fullmatch(value) or _TIMEFRAME_CANONICAL_RE.fullmatch(value)
        if match is None:
            raise FeedSessionError("invalid_timeframe", "unsupported selected timeframe")
        raw_unit = match.group(2).lower()
        unit = "minute" if raw_unit in {"min", "minute"} else "hour" if raw_unit in {"hour", "hr"} else raw_unit
        value = {"unit": unit, "multiplier": int(match.group(1))}
    if not isinstance(value, Mapping) or set(value) != {"unit", "multiplier"}:
        raise FeedSessionError("invalid_timeframe", "timeframe must contain unit and multiplier")
    unit, multiplier = value.get("unit"), value.get("multiplier")
    if unit not in _TIMEFRAME_UNITS or isinstance(multiplier, bool) or not isinstance(multiplier, int) or multiplier < 1:
        raise FeedSessionError("invalid_timeframe", "unsupported selected timeframe")
    if multiplier > _TIMEFRAME_BOUNDS[unit]:
        raise FeedSessionError("invalid_timeframe", "selected timeframe is outside G019 bounds")
    return {"unit": unit, "multiplier": multiplier}


def _snapshot_of(result: Any) -> Mapping[str, Any] | None:
    if isinstance(result, Mapping):
        for key in ("market_snapshot", "snapshot"):
            value = result.get(key)
            if isinstance(value, Mapping):
                return value
        return None
    for name in ("market_snapshot", "snapshot"):
        value = getattr(result, name, None)
        if isinstance(value, Mapping):
            return value
    return None


def _canonical_instant(value: datetime | str | None, *, clock: Clock) -> str:
    if value is None:
        value = clock.now()
    instant = utc_instant(value)
    fraction = f"{instant.microsecond:06d}".rstrip("0")
    return instant.strftime("%Y-%m-%dT%H:%M:%S") + (f".{fraction}" if fraction else "") + "Z"


class FeedSession:
    """One-symbol IEX market-data session composing G019 history and G020 seconds.

    The session keeps a bounded frozen-second registry keyed by ``event_id``
    (the latest execution-frozen revision per exact one-second interval),
    exposes a chronological ``frozen_events`` tuple and the deterministic
    ``latest_second``, tracks unresolved connection gaps for view building,
    and stores the latest accepted shared market view as ``current_view``.
    """

    def __init__(
        self,
        *,
        symbol: str,
        timeframe: Mapping[str, Any] | str,
        history_adapter: Any = None,
        aggregator: Any = None,
        stream_client: Any = None,
        session: Mapping[str, Any] | None = None,
        market_feed: Any = ALPACA_IEX_FEED,
        adjustment_policy: str = "unadjusted",
        minimum_closed_bars: int = 3,
        stale_after_seconds: int = 60,
        clock: Any | None = None,
        max_retained_events: int = 5000,
        history_limit: int | None = None,
    ) -> None:
        if not isinstance(symbol, str) or not symbol or symbol != symbol.upper() or any(char.isspace() for char in symbol):
            raise FeedSessionError("invalid_symbol", "symbol must be an upper-case value")
        if market_feed != ALPACA_IEX_FEED:
            raise FeedSessionError("feed_scope_mismatch", "feed session accepts only ALPACA_IEX_FEED")
        if adjustment_policy != "unadjusted":
            raise FeedSessionError("invalid_adjustment_policy", "feed session accepts only unadjusted (raw) input")
        if session is None:
            session = {"kind": "regular", "timezone": "America/New_York"}
        if not isinstance(session, Mapping) or dict(session) != {"kind": "regular", "timezone": "America/New_York"}:
            raise FeedSessionError("invalid_session", "feed session is fixed to regular America/New_York")
        if isinstance(minimum_closed_bars, bool) or not isinstance(minimum_closed_bars, int) or minimum_closed_bars < 1:
            raise FeedSessionError("invalid_configuration", "minimum_closed_bars must be positive")
        if isinstance(stale_after_seconds, bool) or not isinstance(stale_after_seconds, int) or stale_after_seconds < 0:
            raise FeedSessionError("invalid_configuration", "stale_after_seconds must be non-negative")
        if isinstance(max_retained_events, bool) or not isinstance(max_retained_events, int) or max_retained_events < 1:
            raise FeedSessionError("invalid_configuration", "max_retained_events must be positive")
        if history_limit is not None and (isinstance(history_limit, bool) or not isinstance(history_limit, int) or history_limit < 1):
            raise FeedSessionError("invalid_configuration", "history_limit must be positive")
        self.symbol = symbol
        self.timeframe = _normalize_timeframe(timeframe)
        self.session = {"kind": "regular", "timezone": "America/New_York"}
        self.market_feed = market_feed
        self.adjustment_policy = adjustment_policy
        self.minimum_closed_bars = minimum_closed_bars
        self.stale_after_seconds = stale_after_seconds
        self.max_retained_events = max_retained_events
        self.history_limit = history_limit if history_limit is not None else DEFAULT_LIMIT
        self._clock = clock if clock is not None else SystemClock()
        self._history_adapter = history_adapter
        self._aggregator: Any = None
        self._stream_client: Any = None
        self._stream_control_lock = RLock()
        self._stream_data_lock = RLock()
        self._stream_stop_event: Event | None = None
        self._stream_recovery_event = Event()
        self._stream_thread: Thread | None = None
        self._stream_started = False
        self._stream_transport_ready = False
        self._stream_status: Any = None
        self._stream_gap_open = False
        self._stream_live_cutover_at: datetime | None = None
        self._stream_closed_client: Any = None
        self._stream_last_stop: dict[str, Any] | None = None
        self._frozen_registry: dict[str, SecondAggregateEvent] = {}
        self._skipped_replays: list[dict[str, Any]] = []
        self._stream_skipped_events: list[dict[str, Any]] = []
        self._last_ingest_end_ns: int | None = None
        self._last_ingest_session_day: date | None = None
        self._connection_gaps: list[dict[str, Any]] = []
        self._gap_history: list[dict[str, Any]] = []
        self.current_view: SharedMarketView | None = None
        self._last_history_result: HistoryResult | None = None
        self._last_history_snapshot: Mapping[str, Any] | None = None
        self._recovery_generation = 0
        self._view_builder: SharedMarketViewBuilder | None = None
        if aggregator is not None or stream_client is not None:
            self.attach(aggregator, stream_client=stream_client)

    @property
    def aggregator(self) -> Any:
        return self._aggregator

    @property
    def history_adapter(self) -> Any:
        return self._history_adapter

    @property
    def stream_client(self) -> Any:
        return self._stream_client

    @property
    def history_result(self) -> HistoryResult | None:
        """The latest accepted backfill result."""
        return self._last_history_result

    @property
    def frozen_events(self) -> tuple[SecondAggregateEvent, ...]:
        """Retained execution-frozen seconds in chronological order."""
        return tuple(sorted(self._frozen_registry.values(), key=lambda event: (timestamp_ns(event.end_at), event.event_id)))

    @property
    def latest_second(self) -> SecondAggregateEvent | None:
        """The retained frozen second with the maximum ``(end_at, event_id)``."""
        if not self._frozen_registry:
            return None
        return max(self._frozen_registry.values(), key=lambda event: (timestamp_ns(event.end_at), event.event_id))

    @property
    def connection_gaps(self) -> tuple[Mapping[str, Any], ...]:
        """Unresolved session connection gaps fed into view building."""
        return tuple(dict(gap) for gap in self._connection_gaps)

    @property
    def gap_history(self) -> tuple[Mapping[str, Any], ...]:
        """Every connection gap recorded by the session, including closed ones."""
        return tuple(dict(gap) for gap in self._gap_history)

    @property
    def skipped_replays(self) -> tuple[Mapping[str, Any], ...]:
        """Deterministic record of every skipped/duplicate ingest observation."""
        return tuple(dict(item) for item in self._skipped_replays)

    @property
    def skipped_stream_events(self) -> tuple[Mapping[str, Any], ...]:
        """Data frames dropped by the live recovery/cutover gate."""
        return tuple(dict(item) for item in self._stream_skipped_events)

    @property
    def live_cutover_at(self) -> str | None:
        """The exclusive end instant of the current live reducer seam."""
        if self._stream_live_cutover_at is None:
            return None
        return _canonical_instant(self._stream_live_cutover_at, clock=self._clock)

    def timeframe_text(self) -> str:
        """Return the exact G019 timeframe spelling (e.g. ``5Min``, ``1Hour``)."""
        return f"{self.timeframe['multiplier']}{_TIMEFRAME_UNITS[self.timeframe['unit']]}"

    def attach(self, aggregator: Any = None, *, stream_client: Any = None) -> None:
        """Attach one reducer and optional stream client without allowing a fork.

        An Alpaca client owns a reducer reference because it dispatches each
        received trade/correction/cancel directly into that reducer.  When a
        client is supplied without an explicit session aggregator, its reducer
        is adopted.  When both are supplied, identity must match exactly;
        silently replacing one would make live data disappear from the view.
        """
        active_stream = self._stream_started or (self._stream_thread is not None and self._stream_thread.is_alive())
        if aggregator is not None and self._aggregator is not None and aggregator is not self._aggregator and active_stream:
            raise FeedSessionError("stream_in_use", "cannot replace the aggregator while the stream consumer is running")
        if stream_client is not None and self._stream_client is not None and stream_client is not self._stream_client and active_stream:
            raise FeedSessionError("stream_in_use", "cannot replace the stream client while the consumer is running")
        if aggregator is not None:
            self._validate_aggregator(aggregator)
            if stream_client is None and self._stream_client is not None:
                existing_client_aggregator = getattr(self._stream_client, "aggregator", _MISSING)
                if existing_client_aggregator is not _MISSING and existing_client_aggregator is not None and existing_client_aggregator is not aggregator:
                    raise FeedSessionError(
                        "aggregator_mismatch",
                        "stream client and feed session must share the same aggregator instance",
                    )
        if stream_client is not None:
            client_aggregator = getattr(stream_client, "aggregator", _MISSING)
            if client_aggregator is not _MISSING and client_aggregator is not None:
                if aggregator is not None and client_aggregator is not aggregator:
                    raise FeedSessionError(
                        "aggregator_mismatch",
                        "stream client and feed session must share the same aggregator instance",
                    )
                if aggregator is None and self._aggregator is None:
                    self._validate_aggregator(client_aggregator)
                    self._aggregator = client_aggregator
                elif aggregator is None and self._aggregator is not None and client_aggregator is not self._aggregator:
                    raise FeedSessionError(
                        "aggregator_mismatch",
                        "stream client and feed session must share the same aggregator instance",
                    )
            elif client_aggregator is None and (aggregator is not None or self._aggregator is not None):
                target_aggregator = aggregator if aggregator is not None else self._aggregator
                try:
                    setattr(stream_client, "aggregator", target_aggregator)
                except Exception as exc:
                    raise FeedSessionError(
                        "aggregator_mismatch",
                        "stream client cannot be bound to the feed session aggregator",
                    ) from exc
            self._stream_client = stream_client
            self._stream_closed_client = None
            self._stream_last_stop = None
            if hasattr(stream_client, "dispatch_lock"):
                stream_client.dispatch_lock = self._stream_data_lock
            if hasattr(stream_client, "dispatch_filter"):
                stream_client.dispatch_filter = self._stream_dispatch_filter
        if aggregator is not None:
            self._aggregator = aggregator

        if self._stream_client is not None:
            client_aggregator = getattr(self._stream_client, "aggregator", _MISSING)
            if client_aggregator is not _MISSING and client_aggregator is not None and self._aggregator is not None:
                if client_aggregator is not self._aggregator:
                    raise FeedSessionError(
                        "aggregator_mismatch",
                        "stream client and feed session must share the same aggregator instance",
                    )

    def _validate_aggregator(self, aggregator: Any) -> None:
        feed = getattr(aggregator, "market_feed", ALPACA_IEX_FEED)
        if feed != ALPACA_IEX_FEED:
            raise FeedSessionError("feed_scope_mismatch", "aggregator feed must be ALPACA_IEX_FEED")
        symbol = getattr(aggregator, "symbol", None)
        if symbol != self.symbol:
            raise FeedSessionError("symbol_mismatch", "aggregator symbol differs from the session symbol")

    def _ensure_stream_binding(self, client: Any) -> None:
        client_aggregator = getattr(client, "aggregator", _MISSING)
        if client_aggregator is _MISSING:
            return
        if client_aggregator is None:
            if self._aggregator is not None:
                try:
                    setattr(client, "aggregator", self._aggregator)
                except Exception as exc:
                    raise FeedSessionError(
                        "aggregator_mismatch",
                        "stream client cannot be bound to the feed session aggregator",
                    ) from exc
            return
        self._validate_aggregator(client_aggregator)
        if self._aggregator is None:
            self._aggregator = client_aggregator
        elif client_aggregator is not self._aggregator:
            raise FeedSessionError(
                "aggregator_mismatch",
                "stream client and feed session must share the same aggregator instance",
            )

    def backfill(self, *, start_at: datetime | str, end_at: datetime | str | None = None) -> Any:
        """Fetch and validate the raw IEX history snapshot for the session."""
        if self._history_adapter is None:
            raise FeedSessionError("history_unavailable", "no history adapter is attached")
        fetch = getattr(self._history_adapter, "fetch_history", None)
        if not callable(fetch):
            raise FeedSessionError("history_unavailable", "history adapter does not implement fetch_history")
        result = fetch(
            symbol=self.symbol,
            start_at=start_at,
            end_at=end_at if end_at is not None else self._clock.now(),
            timeframe=self.timeframe_text(),
            limit=self.history_limit,
        )
        snapshot = _snapshot_of(result)
        if snapshot is None:
            raise FeedSessionError("history_unavailable", "history result carries no market snapshot")
        self._check_history_identity(snapshot)
        self._last_history_result = result
        self._last_history_snapshot = snapshot
        return result

    def _check_history_identity(self, snapshot: Mapping[str, Any]) -> None:
        feed, feed_result = extract_market_feed(snapshot, expected=ALPACA_IEX_FEED, require_feed=True)
        if not feed_result.valid or feed is None or feed != ALPACA_IEX_FEED:
            raise FeedSessionError("history_feed_mismatch", "history snapshot feed is not Alpaca IEX single-exchange")
        if snapshot.get("adjustment_policy") != "unadjusted":
            raise FeedSessionError("history_adjustment_mismatch", "history snapshot must be unadjusted")

    def cutover_at(self, history_result: Any) -> datetime:
        """Return the exclusive history end instant used as the half-open cutover.

        The cutover is the latest closed history bar's ``end_at``, falling
        back to the history request's exclusive end when the snapshot carries
        no bars.  G019's half-open filter guarantees that bars never extend
        past the request end, so this instant satisfies the G021 builder's
        half-open join: no history bar straddles the cutover, the cutover is
        aligned to the selected timeframe whenever the bars are on-grid, and
        live seconds with ``end_at <= cutover`` are dropped while seconds
        starting at the cutover join cleanly.
        """
        snapshot = _snapshot_of(history_result)
        bars = snapshot.get("bars") if snapshot is not None else None
        closed = [bar for bar in (bars or ()) if isinstance(bar, Mapping) and bar.get("is_closed") is True]
        end: str | None = None
        if closed:
            end = max(closed, key=lambda bar: timestamp_ns(bar["end_at"]))["end_at"]
        else:
            request = getattr(history_result, "request", None)
            if request is not None:
                end = getattr(request, "end_at", None)
            if end is None and isinstance(history_result, Mapping):
                request = history_result.get("request")
                if isinstance(request, Mapping):
                    end = request.get("end_at")
        if not isinstance(end, str) or _UTC_RE.fullmatch(end) is None:
            raise FeedSessionError("history_unavailable", "history result has no closed bars and no canonical request end")
        return utc_instant(end)

    def ingest(self, *, now: datetime | str | None = None) -> list[SecondAggregateEvent]:
        """Pull newly frozen seconds from the aggregator and retain them.

        Returns only NEW events (deduplicated by ``event_id`` with a
        monotonic non-decreasing ``end_at`` guard); replayed events and
        older-interval replays are skipped and recorded, never errors.
        """
        if self._aggregator is None:
            raise FeedSessionError("aggregator_unavailable", "no aggregator is attached")
        freeze = getattr(self._aggregator, "freeze", None)
        if not callable(freeze):
            raise FeedSessionError("aggregator_unavailable", "attached aggregator does not implement freeze")
        # The consumer thread mutates the same G020 reducer while the runtime
        # thread freezes it.  Serialize these two operations so a second can
        # never be observed half-reduced.
        with self._stream_data_lock:
            # A live stream gap isolates the reducer from the runtime.  In
            # particular, do not freeze pre-gap state into FeedSession while
            # G019 recovery is pending; release_stream_recovery reseeds the
            # same reducer before opening this gate.  Keep the whole freeze /
            # acceptance transaction under this lock so a cutover reset
            # cannot land between the reducer snapshot and registry update.
            if self._stream_client is not None and (self._connection_gaps or self._stream_gap_open):
                return []
            batch = freeze(now=now) if now is not None else freeze()
            accepted: list[SecondAggregateEvent] = []
            for item in batch or ():
                event = self._coerce_event(item)
                if event.status == "reconciliation":
                    self._record_skip(event, "reconciliation_event")
                    continue
                if event.symbol != self.symbol:
                    raise FeedSessionError("symbol_mismatch", "live event symbol differs from the session symbol")
                if event.market_feed != ALPACA_IEX_FEED or event.source != "alpaca_iex" or event.adjustment != "raw" or event.raw_adjustment != "raw":
                    raise FeedSessionError("feed_scope_mismatch", "live event feed/adjustment/source does not match IEX raw input")
                end_ns = timestamp_ns(event.end_at)
                if self._stream_live_cutover_at is not None and end_ns <= timestamp_ns(
                    _canonical_instant(self._stream_live_cutover_at, clock=self._clock)
                ):
                    self._record_skip(event, "before_live_cutover")
                    continue
                if event.event_id in self._frozen_registry:
                    self._record_skip(event, "duplicate_event_id")
                    continue
                if self._last_ingest_end_ns is not None and end_ns <= self._last_ingest_end_ns:
                    event_day = _session_day(event.end_at)
                    if self._last_ingest_session_day is None or event_day <= self._last_ingest_session_day:
                        self._record_skip(event, "out_of_order_interval")
                        continue
                if self._last_ingest_session_day is not None and _session_day(event.end_at) > self._last_ingest_session_day and self._frozen_registry:
                    # A new session day supersedes the previous day's retained
                    # seconds; keep the view a per-session construct.
                    self._frozen_registry.clear()
                self._frozen_registry[event.event_id] = event
                self._last_ingest_end_ns = end_ns
                self._last_ingest_session_day = _session_day(event.end_at)
                accepted.append(event)
            self._retain_bounded()
            return accepted

    @staticmethod
    def _coerce_event(value: Any) -> SecondAggregateEvent:
        if isinstance(value, SecondAggregateEvent):
            return value
        raise FeedSessionError("invalid_second_event", "live event must be a second aggregate event")

    def _record_skip(self, event: SecondAggregateEvent, reason: str) -> None:
        self._skipped_replays.append({"event_id": event.event_id, "end_at": event.end_at, "reason": reason})
        if len(self._skipped_replays) > _MAX_SKIPPED_REPLAYS:
            del self._skipped_replays[: len(self._skipped_replays) - _MAX_SKIPPED_REPLAYS]

    def _retain_bounded(self) -> None:
        while len(self._frozen_registry) > self.max_retained_events:
            oldest = min(self._frozen_registry.values(), key=lambda event: (timestamp_ns(event.end_at), event.event_id))
            self._frozen_registry.pop(oldest.event_id, None)

    def reset_seconds(self) -> int:
        """Drop the retained frozen seconds (post-recovery seam).

        Used when a new cutover/aggregator replaces the previous live stream
        after G019 recovery; returns the number of dropped events.
        """
        count = len(self._frozen_registry)
        self._frozen_registry.clear()
        self._skipped_replays.clear()
        self._last_ingest_end_ns = None
        self._last_ingest_session_day = None
        return count

    def _record_stream_skip(self, event: Any, reason: str) -> None:
        """Record a raw normalized event discarded before reducer dispatch."""
        item: dict[str, Any] = {
            "message_type": getattr(event, "message_type", None),
            "event_time": getattr(event, "event_time", None),
            "reason": reason,
        }
        if hasattr(event, "trade_id"):
            item["trade_id"] = getattr(event, "trade_id")
        if hasattr(event, "original_trade_id"):
            item["original_trade_id"] = getattr(event, "original_trade_id")
        self._stream_skipped_events.append(item)
        if len(self._stream_skipped_events) > _MAX_SKIPPED_REPLAYS:
            del self._stream_skipped_events[: len(self._stream_skipped_events) - _MAX_SKIPPED_REPLAYS]

    def _stream_dispatch_filter(self, event: Any) -> bool:
        """Apply the recovery gate while the client reducer lock is held.

        The client calls this after a complete frame has been received and
        immediately before each normalized data event is reduced.  A frame
        whose recv began before ``mark_connection_gap`` but returned after it
        therefore observes the gate and cannot mutate the reducer.
        """
        message_type = getattr(event, "message_type", None)
        if message_type not in {"t", "c", "x"}:
            # Control/status events still need to surface typed disconnects.
            return True
        if self._stream_gap_open or self._connection_gaps:
            self._record_stream_skip(event, "recovery_gate")
            return False
        if self._stream_client is not None and not self._stream_transport_ready:
            self._record_stream_skip(event, "transport_not_ready")
            return False
        cutover = self._stream_live_cutover_at
        if cutover is None:
            return True
        if message_type == "t":
            try:
                event_at = utc_instant(getattr(event, "event_time"))
            except Exception:
                self._record_stream_skip(event, "invalid_event_time")
                return False
            # Trade event_time is an instant within a G020 second; the
            # half-open live boundary is that second's exclusive end.
            event_end = event_at.replace(microsecond=0) + timedelta(seconds=1)
            if event_end <= cutover:
                self._record_stream_skip(event, "before_live_cutover")
                return False
            return True
        # Corrections/cancels have no independent bucket identity.  The
        # authoritative question is whether their target is an active trade
        # in the *current* post-cutover reducer.  A provider correction may
        # carry a delivery timestamp before the cutover while correcting a
        # live contribution; dropping it by that timestamp would leave the
        # stale trade in the next executable OHLCV.
        target = getattr(event, "original_trade_id", getattr(event, "trade_id", None))
        if not self._has_live_target(target):
            self._record_stream_skip(event, "target_not_in_live_cutover")
            return False
        return True

    def _has_live_target(self, target: Any) -> bool:
        """Return whether *target* is active in the authoritative reducer."""
        aggregator = self._aggregator
        if aggregator is None:
            return False
        has_live_trade = getattr(aggregator, "has_live_trade", None)
        if callable(has_live_trade):
            try:
                return bool(has_live_trade(target))
            except Exception:
                return False
        registry = getattr(aggregator, "registry", None)
        if isinstance(registry, Mapping):
            return target in registry
        # A lightweight test double without an active-registry seam is
        # conservative: it cannot prove that a correction/cancel belongs to
        # the new live reducer, so the event is skipped.
        return False

    def _reset_authoritative_reducer(self) -> None:
        """Clear the in-place LocalSecondAggregator state at a new seam.

        Keep the authoritative object identity intact and delegate the
        complete mutable-state reset to the G020 reducer.  Lightweight test
        doubles without this seam are explicitly treated as stateless
        compatibility reducers and are left untouched.
        """
        aggregator = self._aggregator
        if aggregator is None:
            return
        reset = getattr(aggregator, "reset_for_cutover", None)
        if callable(reset):
            reset()
            return
        if isinstance(aggregator, LocalSecondAggregator):
            raise FeedSessionError("reducer_reset_unavailable", "G020 reducer does not expose reset_for_cutover")

    def _reset_live_reducer_for_cutover(self, cutover_at: datetime | str) -> None:
        """Reseed the same client/session reducer before live release."""
        cutover = utc_instant(cutover_at)
        with self._stream_data_lock:
            self._reset_authoritative_reducer()
            self._frozen_registry.clear()
            self._skipped_replays.clear()
            self._last_ingest_end_ns = None
            self._last_ingest_session_day = None
            self._stream_live_cutover_at = cutover

    def build_view(
        self,
        *,
        history_result: Any = None,
        now_at: datetime | str | None = None,
        connection_gaps: Iterable[Any] | None = None,
        recovery_generation: int = 0,
        recovered: bool = False,
        recovery_evidence: Mapping[str, Any] | None = None,
        cutover_at: datetime | str | None = None,
    ) -> SharedMarketView:
        """Build and store one G021 shared market view from current session state.

        ``history_result`` defaults to the last accepted backfill result.
        ``connection_gaps`` defaults to the session's unresolved connection
        gaps; pass an explicit iterable to override.  A failed build raises
        ``FeedSessionError`` carrying the G021 builder's error code.

        One persistent ``SharedMarketViewBuilder`` instance backs every call,
        so the G021 changed-input guard and monotonic recovery-generation
        checks apply across the session's lifetime: a same-input rebuild makes
        the supplied recovery evidence inert, and the effective recovery
        generation is ``max(recovery_generation, session generation)`` so
        plain post-recovery builds never regress the generation.
        """
        if history_result is None:
            history_result = self._last_history_result
        if history_result is None:
            raise FeedSessionError("history_unavailable", "no history result is available; call backfill() first")
        snapshot = _snapshot_of(history_result)
        if snapshot is None:
            raise FeedSessionError("history_unavailable", "history result carries no market snapshot")
        if isinstance(recovery_generation, bool) or not isinstance(recovery_generation, int) or recovery_generation < 0:
            raise FeedSessionError("invalid_recovery_generation", "recovery generation must be a non-negative integer")
        effective_generation = recovery_generation if recovery_generation > self._recovery_generation else self._recovery_generation
        gaps = self.connection_gaps if connection_gaps is None else tuple(connection_gaps)
        effective_cutover = (
            utc_instant(cutover_at)
            if cutover_at is not None
            else self._stream_live_cutover_at or self.cutover_at(history_result)
        )
        builder = self._view_builder
        if builder is None:
            builder = SharedMarketViewBuilder(
                self.symbol,
                self.timeframe,
                session=self.session,
                market_feed=self.market_feed,
                adjustment_policy=self.adjustment_policy,
                minimum_closed_bars=self.minimum_closed_bars,
                stale_after_seconds=self.stale_after_seconds,
            )
            self._view_builder = builder
        attempt = builder.try_build(
            snapshot,
            live_events=self.frozen_events,
            cutover_at=effective_cutover,
            now_at=now_at,
            connection_gaps=gaps,
            recovery_generation=effective_generation,
            recovered=recovered,
            recovery_evidence=recovery_evidence,
        )
        if not attempt.ok:
            error = attempt.error
            code = getattr(error, "code", None) or "view_build_failed"
            message = str(error) if error is not None else "market view build failed"
            raise FeedSessionError(code, message)
        self.current_view = attempt.view
        self._recovery_generation = effective_generation
        return self.current_view

    def build_recovery_evidence(self, bars: Iterable[Mapping], *, generation: int) -> Mapping[str, Any]:
        """Construct the exact recovery evidence the G021 builder accepts.

        The returned mapping clears ``readiness.recovery_required`` only when
        the recovery view is built with no connection gaps, ``recovered=True``
        and ``recovery_generation == generation`` against the same history
        snapshot that produced this evidence.  ``bars`` contribute the
        diagnostic ``recovery_bar_intervals`` key; the builder-required
        ``resolved_intervals`` is deliberately empty because G021 v0.1.0
        marks non-empty interval evidence invalid when the gaps are resolved.
        """
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
            raise FeedSessionError("invalid_recovery_generation", "recovery generation must be a positive integer")
        intervals: list[dict[str, Any]] = []
        for bar in bars:
            if isinstance(bar, Mapping):
                start, end = bar.get("start_at"), bar.get("end_at")
            else:
                start, end = getattr(bar, "start_at", None), getattr(bar, "end_at", None)
            intervals.append({"start_at": start, "end_at": end})
        fingerprint = self._last_history_snapshot.get("content_hash") if self._last_history_snapshot is not None else None
        return {
            "reseeded": True,
            "generation": generation,
            "recovery_generation": generation,
            "resolved_intervals": [],
            "recovery_bar_intervals": [item for item in intervals if item["start_at"] is not None],
            "source_fingerprint": fingerprint,
        }

    def fetch_recovery(self, *, start_at: datetime | str, end_at: datetime | str | None = None) -> Any:
        """Fetch one-minute ``RecoveryBar 0.1.0`` history for the session."""
        if self._history_adapter is None:
            raise FeedSessionError("history_unavailable", "no history adapter is attached")
        fetch = getattr(self._history_adapter, "fetch_recovery", None)
        if not callable(fetch):
            raise FeedSessionError("history_unavailable", "history adapter does not implement fetch_recovery")
        result = fetch(
            symbol=self.symbol,
            start_at=start_at,
            end_at=end_at if end_at is not None else self._clock.now(),
        )
        self.recovery_bars(result)
        return result

    def recovery_bars(self, history_result: Any) -> tuple[Mapping[str, Any], ...]:
        """Return the recovery result's bars as ``RecoveryBar 0.1.0`` mappings.

        This is a shallow coercion boundary; deep recovery validation is
        deferred to the G025/G026 reducers by design.
        """
        raw = getattr(history_result, "recovery_bars", None)
        if raw is None and isinstance(history_result, Mapping):
            raw = history_result.get("recovery_bars")
        if raw is None:
            return ()
        converted: list[Mapping[str, Any]] = []
        for item in raw:
            if isinstance(item, Mapping):
                converted.append(dict(item))
            elif hasattr(item, "to_dict") and callable(getattr(item, "to_dict")):
                converted.append(dict(item.to_dict()))
            else:
                raise FeedSessionError("malformed_recovery_result", "recovery bars must be RecoveryBar 0.1.0 mappings")
        return tuple(converted)

    def recovery_cursor(self, history_result: Any) -> str | None:
        """Return the recovery request's resume cursor (its start instant)."""
        request = getattr(history_result, "request", None)
        if request is not None:
            start = getattr(request, "start_at", None)
            if isinstance(start, str) and start:
                return start
        if isinstance(history_result, Mapping):
            request = history_result.get("request")
            if isinstance(request, Mapping) and isinstance(request.get("start_at"), str):
                return request["start_at"]
        bars = self.recovery_bars(history_result)
        if bars:
            return bars[0].get("start_at")
        return None

    def source_request_hash(self, history_result: Any) -> str | None:
        """Return the source request hash carried by a history/recovery result."""
        if isinstance(history_result, Mapping):
            value = history_result.get("source_request_hash")
            if isinstance(value, str) and value:
                return value
            request = history_result.get("request")
            if isinstance(request, Mapping):
                value = request.get("source_request_hash")
                if isinstance(value, str) and value:
                    return value
        value = getattr(history_result, "source_request_hash", None)
        if isinstance(value, str) and value:
            return value
        request = getattr(history_result, "request", None)
        if request is not None:
            value = getattr(request, "source_request_hash", None)
            if isinstance(value, str) and value:
                return value
        return None

    def mark_connection_gap(
        self,
        start_at: datetime | str | None = None,
        end_at: datetime | str | None = None,
        *,
        reason: str = "disconnect",
    ) -> Mapping[str, Any]:
        """Record a connection gap on the session (and proxy to the aggregator).

        A gap stays in ``connection_gaps`` — and therefore blocks executable
        views — until ``close_connection_gap`` resolves it after recovery.
        """
        with self._stream_data_lock:
            start = _canonical_instant(start_at, clock=self._clock)
            end = None if end_at is None else _canonical_instant(end_at, clock=self._clock)
            if end is not None and timestamp_ns(end) <= timestamp_ns(start):
                raise FeedSessionError("invalid_gap", "connection gap requires start before end")
            gap = {"start_at": start, "end_at": end}
            self._connection_gaps.append(dict(gap))
            self._gap_history.append(dict(gap))
            if self._stream_client is not None and self._stream_started:
                # A runtime/restart gap is also a live-dispatch gate.  The
                # socket may remain transport-ready, but no post-gap frame
                # may reach G020 until recovery explicitly releases it.
                self._stream_gap_open = True
                self._stream_recovery_event.clear()
            if self._aggregator is not None:
                mark = getattr(self._aggregator, "mark_connection_gap", None)
                if callable(mark):
                    mark(start, end, reason=reason)
            return dict(gap)

    def close_connection_gap(self, start_at: datetime | str | None = None, end_at: datetime | str | None = None) -> Mapping[str, Any] | None:
        """Resolve the most recent unresolved session gap (and proxy to the aggregator).

        The resolved gap leaves ``connection_gaps`` (recovery views must be
        built without resolved gaps for the G021 evidence to clear
        ``recovery_required``) and is preserved in ``gap_history``.
        """
        with self._stream_data_lock:
            end = _canonical_instant(end_at, clock=self._clock)
            if self._aggregator is not None:
                close = getattr(self._aggregator, "close_connection_gap", None)
                if callable(close):
                    close(end)
            if not self._connection_gaps:
                return None
            gap = self._connection_gaps.pop()
            if gap["end_at"] is None:
                if timestamp_ns(end) <= timestamp_ns(gap["start_at"]):
                    raise FeedSessionError("invalid_gap", "connection gap close must follow its start")
                gap["end_at"] = end
            self._gap_history.append(dict(gap))
            return dict(gap)

    def release_stream_recovery(
        self,
        *,
        cutover_at: datetime | str | None = None,
        end_at: datetime | str | None = None,
    ) -> dict[str, Any]:
        """Close the unresolved gap and release live dispatch after recovery.

        Reauthentication/resubscription only makes the transport ready; it
        never calls this method.  The runtime calls it after validated recovery
        bars and ``commit_market_recovery`` have succeeded and the replacement
        view is built.  Until then the consumer waits on the recovery event,
        so queued post-disconnect frames cannot mutate the reducer.
        """
        with self._stream_control_lock:
            if self._stream_client is not None and not self._stream_transport_ready:
                return {
                    "ok": False,
                    "action": "release_recovery",
                    "stream": "blocked",
                    "error": "stream_not_ready",
                }
            if not self._stream_gap_open and not self._connection_gaps and cutover_at is None:
                self._stream_recovery_event.set()
                return {
                    "ok": True,
                    "action": "release_recovery",
                    "stream": "already_released",
                    "error": None,
                }
            if cutover_at is None:
                return {
                    "ok": False,
                    "action": "release_recovery",
                    "stream": "blocked",
                    "error": "cutover_required",
                }
            try:
                cutover = utc_instant(cutover_at)
                close_at = utc_instant(end_at) if end_at is not None else utc_instant(self._clock.now())
                if cutover > close_at:
                    return {
                        "ok": False,
                        "action": "release_recovery",
                        "stream": "blocked",
                        "error": "cutover_in_future",
                    }
                # This is deliberately inside the control lock and before
                # the gate is opened.  The client and FeedSession retain the
                # exact same reducer object, but all pre-cutover trades,
                # frozen revisions, tombstones and gap metadata must be gone
                # before the socket can consume its buffered frames.
                self._reset_live_reducer_for_cutover(cutover)
            except Exception:
                self._stream_status = {
                    "state": "error",
                    "outcome": {
                        "kind": "reducer_reset_failed",
                        "ok": False,
                        "retryable": False,
                        "detail": "live reducer could not be reseeded",
                    },
                }
                return {
                    "ok": False,
                    "action": "release_recovery",
                    "stream": "blocked",
                    "error": "reducer_reset_failed",
                }
            try:
                while self._connection_gaps:
                    self.close_connection_gap(end_at=close_at)
            except Exception:
                self._stream_status = {
                    "state": "error",
                    "outcome": {
                        "kind": "gap_close_failed",
                        "ok": False,
                        "retryable": False,
                        "detail": "stream recovery gap could not be closed",
                    },
                }
                return {
                    "ok": False,
                    "action": "release_recovery",
                    "stream": "blocked",
                    "error": "gap_close_failed",
                }
            with self._stream_data_lock:
                if self._stream_client is not None and not self._stream_transport_ready:
                    return {
                        "ok": False,
                        "action": "release_recovery",
                        "stream": "blocked",
                        "error": "stream_not_ready",
                    }
                self._stream_gap_open = bool(self._connection_gaps)
                if self._stream_gap_open:
                    return {
                        "ok": False,
                        "action": "release_recovery",
                        "stream": "blocked",
                        "error": "gap_close_incomplete",
                    }
                self._stream_recovery_event.set()
            return {
                "ok": True,
                "action": "release_recovery",
                "stream": "ready",
                "cutover_at": _canonical_instant(cutover, clock=self._clock),
                "error": None,
            }

    def start_stream(self) -> dict[str, Any]:
        """Connect and start one controlled background receive consumer.

        ``connect`` is deliberately performed synchronously so authentication
        and subscription failures reach the caller before the runtime claims
        that its stream is connected.  Only the Alpaca client/status surface is
        treated as authoritative; a legacy test double returning ``None`` is
        retained as an optimistic compatibility path.
        """
        return self._stream_control("start")

    def stop_stream(self) -> dict[str, Any]:
        """Close the attached stream client; a no-op status when absent."""
        return self._stream_control("stop")

    def _stream_control(self, action: str) -> dict[str, Any]:
        client = self._stream_client
        if client is None:
            return {"ok": True, "action": action, "stream": "not_attached", "error": None}

        if action == "start":
            with self._stream_control_lock:
                self._ensure_stream_binding(client)
                if (self._stream_thread is not None and self._stream_thread.is_alive()) or (
                    self._stream_thread is None and self._stream_started
                ):
                    return self._stream_result(
                        action,
                        ok=True,
                        stream="connected",
                        status=self._stream_status,
                        already_running=True,
                    )
                method = getattr(client, "connect", None)
                if not callable(method):
                    return self._stream_result(action, ok=False, stream="unavailable", error="method_missing")
                self._stream_closed_client = None
                self._stream_last_stop = None
                self._stream_transport_ready = False
                self._stream_recovery_event.clear()
                try:
                    status = method()
                except Exception as exc:
                    status = {
                        "state": "error",
                        "outcome": {
                            "kind": "connect_error",
                            "ok": False,
                            "retryable": False,
                            "detail": "stream connection failed",
                        },
                    }
                    self._record_stream_failure(status)
                    try:
                        client.close()
                    except Exception:
                        pass
                    return self._stream_result(
                        action,
                        ok=False,
                        stream="error",
                        status=status,
                        error=type(exc).__name__,
                    )
                self._stream_status = status
                if not self._status_ok(status):
                    self._stream_transport_ready = False
                    self._stream_recovery_event.clear()
                    self._record_stream_failure(status)
                    self._stream_started = False
                    try:
                        client.close()
                    except Exception:
                        pass
                    return self._stream_result(
                        action,
                        ok=False,
                        stream="error",
                        status=status,
                        error=self._status_kind(status),
                    )
                self._stream_stop_event = Event()
                self._stream_started = True
                self._stream_transport_ready = True
                # A restart/runtime gap can be recorded before a new stream
                # is started.  Derive the gate from unresolved session state
                # as well as the transport flag so start never opens a
                # pre-existing recovery seam accidentally.
                self._stream_gap_open = self._stream_gap_open or bool(self._connection_gaps)
                if self._stream_gap_open:
                    self._stream_recovery_event.clear()
                else:
                    self._stream_recovery_event.set()
                self._stream_closed_client = None
                self._stream_last_stop = None
                if self._supports_consumer(client):
                    thread = Thread(
                        target=self._consume_stream,
                        args=(client, self._stream_stop_event),
                        name=f"pa-feed-{self.symbol}",
                        daemon=False,
                    )
                    self._stream_thread = thread
                    thread.start()
                else:
                    self._stream_thread = None
                return self._stream_result(action, ok=True, stream="connected", status=status)

        with self._stream_control_lock:
            if self._stream_last_stop is not None and self._stream_closed_client is client:
                return dict(self._stream_last_stop)
            stop_event = self._stream_stop_event
            if stop_event is not None:
                stop_event.set()
            self._stream_recovery_event.set()
            method = getattr(client, "close", None)
            if not callable(method):
                result = self._stream_result(action, ok=False, stream="unavailable", error="method_missing")
                self._stream_last_stop = result
                self._stream_closed_client = client
                self._stream_started = False
                return result
            close_error: str | None = None
            try:
                method()
            except Exception as exc:
                close_error = type(exc).__name__
            thread = self._stream_thread
            if thread is not None and thread is not current_thread():
                thread.join(self._join_timeout(client))
            alive = thread is not None and thread.is_alive()
            if alive and close_error is None:
                close_error = "consumer_stop_timeout"
            self._stream_started = False
            self._stream_transport_ready = False
            if not alive:
                self._stream_thread = None
            result = self._stream_result(
                action,
                ok=close_error is None and not alive,
                stream="closed" if close_error is None and not alive else "error",
                status=self._stream_status,
                error=close_error,
            )
            self._stream_last_stop = result
            self._stream_closed_client = client
            return result

    @staticmethod
    def _supports_consumer(client: Any) -> bool:
        return (
            callable(getattr(client, "receive_frame_safe", None))
            or callable(getattr(client, "receive_frame", None))
            or callable(getattr(client, "receive_safe", None))
            or callable(getattr(client, "receive", None))
        )

    @staticmethod
    def _status_dict(status: Any) -> dict[str, Any] | None:
        if status is None:
            return None
        to_dict = getattr(status, "to_dict", None)
        if callable(to_dict):
            value = to_dict()
            return dict(value) if isinstance(value, Mapping) else None
        if isinstance(status, Mapping):
            return dict(status)
        return None

    @classmethod
    def _status_ok(cls, status: Any) -> bool:
        if status is None:
            return True
        payload = cls._status_dict(status)
        if payload is not None:
            state = payload.get("state")
            if state is None and "ok" in payload:
                # Keep older injected clients compatible; real Alpaca
                # ClientStatus always carries the strict state/outcome pair.
                return payload.get("ok") is not False
            outcome = payload.get("outcome")
            outcome_ok = outcome.get("ok") if isinstance(outcome, Mapping) else None
            return state == "ready" and outcome_ok is not False
        state = getattr(status, "state", None)
        outcome = getattr(status, "outcome", None)
        if state is not None:
            return state == "ready" and getattr(outcome, "ok", True) is not False
        return bool(status)

    @classmethod
    def _status_kind(cls, status: Any) -> str:
        payload = cls._status_dict(status)
        if payload is not None:
            outcome = payload.get("outcome")
            if isinstance(outcome, Mapping) and isinstance(outcome.get("kind"), str):
                return outcome["kind"]
            if isinstance(payload.get("error"), str):
                return payload["error"]
        outcome = getattr(status, "outcome", None)
        kind = getattr(outcome, "kind", None)
        return kind if isinstance(kind, str) else "stream_error"

    @classmethod
    def _status_retryable(cls, status: Any) -> bool:
        payload = cls._status_dict(status)
        if payload is not None:
            outcome = payload.get("outcome")
            if isinstance(outcome, Mapping):
                return bool(outcome.get("retryable"))
            return bool(payload.get("retryable"))
        outcome = getattr(status, "outcome", None)
        return bool(getattr(outcome, "retryable", False))

    def _stream_result(
        self,
        action: str,
        *,
        ok: bool,
        stream: str,
        status: Any = None,
        error: str | None = None,
        already_running: bool = False,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {"ok": ok, "action": action, "stream": stream, "error": error}
        payload = self._status_dict(status)
        if payload is not None:
            result["client_status"] = payload
        if already_running:
            result["already_running"] = True
        return result

    @staticmethod
    def _join_timeout(client: Any) -> float:
        config = getattr(client, "config", None)
        timeout = getattr(config, "timeout_seconds", _STREAM_JOIN_TIMEOUT)
        try:
            return max(0.1, min(float(timeout) + 1.0, _STREAM_JOIN_TIMEOUT))
        except (TypeError, ValueError):
            return _STREAM_JOIN_TIMEOUT

    def _stream_now(self) -> str:
        return _canonical_instant(None, clock=self._clock)

    def _record_stream_failure(self, status: Any) -> None:
        with self._stream_data_lock:
            self._stream_status = status
            self._stream_transport_ready = False
            self._stream_recovery_event.clear()
            if self._stream_gap_open or self._connection_gaps:
                if self._stream_client is not None:
                    self._stream_gap_open = True
                return
            try:
                self.mark_connection_gap(start_at=self._stream_now(), reason=self._status_kind(status))
                self._stream_gap_open = True
            except Exception:
                # The typed client status remains available even if a test double
                # has no G020 gap surface; never turn a stream failure into a
                # fabricated market event.
                pass

    def _wait_reconnect_delay(self, client: Any, stop_event: Event) -> bool:
        config = getattr(client, "config", None)
        try:
            delay = max(0.0, float(getattr(config, "reconnect_delay_seconds", 0.0)))
        except (TypeError, ValueError):
            delay = 0.0
        return stop_event.wait(delay)

    def _consume_stream(self, client: Any, stop_event: Event) -> None:
        reconnects_used = 0
        try:
            while not stop_event.is_set():
                if self._stream_gap_open and not self._stream_transport_ready:
                    # A failed reconnect must consume the remaining retry
                    # budget without entering recv() on an unready socket.
                    # Once a reconnect is ready, the separate recovery event
                    # below keeps live reducers paused until G028 releases it.
                    config = getattr(client, "config", None)
                    max_attempts = getattr(config, "reconnect_attempts", 0)
                    try:
                        max_attempts = max(0, int(max_attempts))
                    except (TypeError, ValueError):
                        max_attempts = 0
                    if reconnects_used >= max_attempts:
                        break
                    reconnects_used += 1
                    if self._wait_reconnect_delay(client, stop_event):
                        break
                    if hasattr(client, "reconnect_count"):
                        client.reconnect_count += 1
                    try:
                        status = client.connect()
                    except Exception:
                        status = {
                            "state": "error",
                            "outcome": {
                                "kind": "disconnect",
                                "ok": False,
                                "retryable": True,
                                "detail": "Alpaca stream disconnected",
                            },
                        }
                    self._stream_status = status
                    if self._status_ok(status):
                        self._stream_transport_ready = True
                        # Transport readiness is not G028 recovery.  Keep the
                        # reducer gate closed until the runtime commits bars
                        # and explicitly calls release_stream_recovery().
                        self._stream_recovery_event.clear()
                        reconnects_used = 0
                        continue
                    self._record_stream_failure(status)
                    if not self._status_retryable(status):
                        break
                    continue
                if self._stream_gap_open:
                    if stop_event.is_set():
                        break
                    if not self._stream_recovery_event.wait(_STREAM_POLL_TIMEOUT):
                        continue
                    if stop_event.is_set():
                        break
                try:
                    receive_frame_safe = getattr(client, "receive_frame_safe", None)
                    receive_safe = getattr(client, "receive_safe", None)
                    # The client owns the short dispatch critical section;
                    # this call must not hold the reducer lock while recv()
                    # blocks, otherwise a runtime step could never freeze a
                    # second until stop() closed the socket.
                    if callable(receive_frame_safe):
                        if getattr(client, "supports_receive_timeout", False):
                            result = receive_frame_safe(timeout=_STREAM_POLL_TIMEOUT)
                        else:
                            result = receive_frame_safe()
                    elif callable(receive_safe):
                        if getattr(client, "supports_receive_timeout", False):
                            result = receive_safe(timeout=_STREAM_POLL_TIMEOUT)
                        else:
                            result = receive_safe()
                    else:
                        result = client.receive()
                except Exception:
                    if stop_event.is_set():
                        break
                    # Unexpected consumer exceptions are terminal for this
                    # pump.  The public status is fixed/sanitized and no
                    # partial/false trade is injected.
                    self._record_stream_failure({
                        "state": "error",
                        "outcome": {
                            "kind": "consumer_error",
                            "ok": False,
                            "retryable": False,
                            "detail": "stream consumer failed",
                        },
                    })
                    break
                if stop_event.is_set():
                    break
                if not self._is_status_result(result):
                    # Some injected transports use a non-blocking receive
                    # surface and return ``None`` while no frame is ready.
                    # Yield only on that idle path so an actual frame batch
                    # is dispatched without an artificial delay.
                    if result is None:
                        stop_event.wait(_STREAM_POLL_TIMEOUT)
                    continue
                if self._status_kind(result) == "timeout" and getattr(client, "supports_receive_timeout", False):
                    continue
                self._record_stream_failure(result)
                if not self._status_retryable(result):
                    break
                # Leave the transport in the explicit not-ready state.  The
                # next iteration performs the next configured reconnect; no
                # second consumer is ever created.
        finally:
            if not stop_event.is_set():
                try:
                    client.close()
                except Exception:
                    pass
            with self._stream_data_lock:
                self._stream_started = False
                self._stream_transport_ready = False
                self._stream_recovery_event.clear()
                if self._stream_thread is current_thread():
                    self._stream_thread = None

    @staticmethod
    def _is_status_result(value: Any) -> bool:
        if isinstance(value, Mapping):
            return "state" in value and "outcome" in value
        return hasattr(value, "state") and hasattr(value, "outcome")

    def status(self) -> dict[str, Any]:
        """Return the deterministic session status/diagnostic mapping."""
        latest = self.latest_second
        if self.current_view is not None:
            readiness = self.current_view.readiness.to_dict()
        else:
            readiness = Readiness().to_dict()
        return {
            "symbol": self.symbol,
            "feed": FEED_KEY,
            "adjustment": "raw",
            "frozen_event_count": len(self._frozen_registry),
            "latest_second_end_at": latest.end_at if latest is not None else None,
            "has_view": self.current_view is not None,
            "view_id": self.current_view.view_id if self.current_view is not None else None,
            "readiness": readiness,
            "recovery_generation": self._recovery_generation,
            "market_data_usage": MARKET_DATA_USAGE,
            "stream": {
                "started": self._stream_started,
                "consumer_alive": bool(self._stream_thread is not None and self._stream_thread.is_alive()),
                "transport_ready": self._stream_transport_ready,
                "live_dispatch_allowed": self._stream_transport_ready and not self._stream_gap_open and not self._connection_gaps,
                "live_cutover_at": self.live_cutover_at,
                "skipped_event_count": len(self._stream_skipped_events),
                "status": self._status_dict(self._stream_status),
                "gap_open": self._stream_gap_open,
            },
        }


__all__ = ["FeedSession", "FeedSessionError"]
