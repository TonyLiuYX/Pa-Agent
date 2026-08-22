"""Deterministic fixture builders for the G028 feed-session tests.

The builders construct real G019/G020 values (``HistoryResult``,
``SecondAggregateEvent``, ``RecoveryBar``) so the feed session tests exercise
the same canonical hashes, identities and validation boundaries as the live
runtime.  Test doubles (``FakeHistoryAdapter``, ``FakeAggregator``,
``FakeStreamClient``) stay deliberately small and scripted.
"""

from __future__ import annotations

import hashlib
from typing import Any, Iterable, Mapping

from pa_engine.market import ALPACA_IEX_FEED, MarketFeedRef
from pa_engine.contracts import canonical_json_bytes
from pa_engine.market_sources.alpaca.history import (
    AdapterOutcome,
    AlpacaHistoryAdapter,
    HistoryRequest,
    HistoryResult,
    RecordedTransport,
    RecoveryBar,
    TransportResponse,
)
from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent, execution_watermark

SYMBOL = "AAPL"
SESSION = {"kind": "regular", "timezone": "America/New_York"}
ONE_MINUTE = {"unit": "minute", "multiplier": 1}


def history_snapshot(*, symbol: str = SYMBOL, timeframe: str = "1Min", start_at: str, end_at: str, bars: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    """Build a G015-valid history snapshot through the real G019 adapter."""
    payload = {"bars": {symbol: bars}}
    adapter = AlpacaHistoryAdapter(key_id="fixture", secret_key="fixture", transport=RecordedTransport([TransportResponse(200, payload)]))
    result = adapter.fetch_history(symbol=symbol, start_at=start_at, end_at=end_at, timeframe=timeframe)
    if not result.success or result.snapshot is None:
        raise AssertionError("synthetic history snapshot failed G019 conversion")
    return result.snapshot


def history_result(
    snapshot: Mapping[str, Any] | None,
    *,
    start_at: str,
    end_at: str,
    timeframe: Mapping[str, Any] | str = ONE_MINUTE,
    bars: Iterable[Mapping[str, Any]] = (),
    recovery_bars: Iterable[RecoveryBar] = (),
    outcome: AdapterOutcome | None = None,
) -> HistoryResult:
    """Wrap a snapshot into a canonical G019 ``HistoryResult``."""
    request = HistoryRequest(SYMBOL, start_at, end_at, timeframe, limit=1000)
    outcome = outcome or AdapterOutcome("success", "success", False, False, "history fetched", pages=1)
    return HistoryResult(request, outcome, bars=tuple(bars), snapshot=snapshot, recovery_bars=tuple(recovery_bars), coverage=request.coverage)


def frozen_second(
    *,
    symbol: str = SYMBOL,
    start_at: str,
    end_at: str,
    open: str = "10",
    high: str = "10",
    low: str = "10",
    close: str = "10",
    volume: str = "1",
    contributor_ids: tuple[str, ...] = ("1",),
    status: str = "frozen",
    quality: Mapping[str, Any] | None = None,
    watermark: str | None = None,
    revision_sequence: int = 0,
) -> SecondAggregateEvent:
    """Build a real execution-frozen ``SecondAggregateEvent``."""
    watermark = watermark or execution_watermark(start_at, end_at, end_at)
    contributor_hash = hashlib.sha256(canonical_json_bytes(list(contributor_ids))).hexdigest()
    return SecondAggregateEvent(
        symbol,
        start_at,
        end_at,
        open,
        high,
        low,
        close,
        volume,
        contributor_ids,
        contributor_hash,
        watermark,
        status,
        revision_sequence,
        dict(quality or {"status": "ok", "codes": [], "connection_gap": False}),
        ALPACA_IEX_FEED,
        "raw",
        "raw",
        None,
    )


def gap_flagged_second(*, symbol: str = SYMBOL, start_at: str, end_at: str, close: str = "10") -> SecondAggregateEvent:
    """A frozen second carrying an explicit connection-gap quality marker."""
    return frozen_second(
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        open=close,
        high=close,
        low=close,
        close=close,
        volume="1",
        contributor_ids=("gap",),
        quality={"status": "ok", "codes": ["connection_gap"], "connection_gap": True},
    )


def recovery_bar(
    *,
    start_at: str,
    end_at: str,
    open: str = "10",
    high: str = "10.5",
    low: str = "9.5",
    close: str = "10.25",
    volume: str = "100",
    complete: bool = True,
    source_request_hash: str | None = None,
    symbol: str = SYMBOL,
) -> RecoveryBar:
    """Build a real ``RecoveryBar 0.1.0`` minute value."""
    request_hash = source_request_hash or hashlib.sha256(b"g028-feed-recovery").hexdigest()
    return RecoveryBar(
        instrument={"asset_class": "us_equity", "symbol": symbol, "venue": "IEX", "currency": "USD"},
        timeframe=ONE_MINUTE,
        session=SESSION,
        start_at=start_at,
        end_at=end_at,
        open=open,
        high=high,
        low=low,
        close=close,
        volume=volume,
        complete=complete,
        coverage_state="complete" if complete else "partial",
        source_request_hash=request_hash,
        recovery_cursor=end_at,
        market_feed=ALPACA_IEX_FEED.to_dict(),
    )


class FakeHistoryAdapter:
    """Canned G019-style adapter returning prepared results."""

    def __init__(self, *, history_result: Any = None, recovery_result: Any = None) -> None:
        self.history_result = history_result
        self.recovery_result = recovery_result
        self.calls: list[Mapping[str, Any]] = []

    def fetch_history(self, *, symbol: str | None = None, start_at: Any = None, end_at: Any = None, timeframe: Any = None, limit: int | None = None, **kwargs: Any) -> Any:
        self.calls.append({"operation": "history", "symbol": symbol, "start_at": start_at, "end_at": end_at, "timeframe": timeframe, "limit": limit})
        return self.history_result

    def fetch_recovery(self, *, symbol: str | None = None, start_at: Any = None, end_at: Any = None, limit: int | None = None, **kwargs: Any) -> Any:
        self.calls.append({"operation": "recovery", "symbol": symbol, "start_at": start_at, "end_at": end_at, "limit": limit})
        return self.recovery_result


class FakeAggregator:
    """Scripted ``freeze()`` batches of real frozen second events."""

    def __init__(self, *, symbol: str = SYMBOL, market_feed: MarketFeedRef = ALPACA_IEX_FEED, batches: Iterable[Iterable[SecondAggregateEvent]] = ()) -> None:
        self.symbol = symbol
        self.market_feed = market_feed
        self._batches = [list(batch) for batch in batches]
        self.freeze_calls: list[Any] = []
        self.gaps: list[Mapping[str, Any]] = []

    def freeze(self, *, now: Any = None) -> tuple[SecondAggregateEvent, ...]:
        self.freeze_calls.append(now)
        return tuple(self._batches.pop(0) if self._batches else ())

    def mark_connection_gap(self, start_at: str, end_at: str | None, *, reason: str = "disconnect") -> None:
        self.gaps.append({"start_at": start_at, "end_at": end_at, "reason": reason})

    def close_connection_gap(self, end_at: str) -> None:
        for index in range(len(self.gaps) - 1, -1, -1):
            if self.gaps[index]["end_at"] is None:
                self.gaps[index]["end_at"] = end_at
                return


class FakeStreamClient:
    """Scripted stream client used by start/stop-stream status tests."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.connect_calls = 0
        self.close_calls = 0

    def connect(self) -> None:
        self.connect_calls += 1
        if self.fail_on == "connect":
            raise RuntimeError("connect failed")

    def close(self) -> None:
        self.close_calls += 1
        if self.fail_on == "close":
            raise RuntimeError("close failed")
