"""G028 regression tests for the real recorded Alpaca transport wiring."""

from __future__ import annotations

import importlib.util
import json
import time
from threading import Event, RLock, Thread, get_ident
from pathlib import Path
from typing import Any

import pytest

from pa_engine.market_sources.alpaca.stream import (
    AlpacaIEXClient,
    LocalSecondAggregator,
    RecordedWebSocketTransport,
    parse_cancel_error,
    parse_correction,
    parse_trade,
)
from pa_engine.runtime.cli import _alpaca_adapters_from_env
from pa_engine.runtime.config import RuntimeConfig
from pa_engine.runtime.engine import AutomaticPaperRuntime
from pa_engine.runtime.feed import FeedSession, FeedSessionError
from pa_engine.strategy.runtime import FakeClock
from pa_engine.strategies.knowledge import build_registry

ROOT = Path(__file__).resolve().parents[3]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "phase2" / "g028"
RECORDED_FRAMES = FIXTURE_ROOT / "recorded_iex_frames.json"


def _load(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"unable to load fixture module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ENGINE_BUILDERS = _load(FIXTURE_ROOT / "engine" / "builders.py", "g028_live_engine_builders")
RECORDED_SOCKET = _load(FIXTURE_ROOT / "recorded_transport.py", "g028_recorded_transport")


def _wait_for(predicate: Any, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("timed out waiting for recorded stream state")
        time.sleep(0.005)


def _recorded_payloads() -> list[str]:
    return [json.dumps(frame, separators=(",", ":")) for frame in json.loads(RECORDED_FRAMES.read_text())]


def _ready_status() -> dict[str, Any]:
    return {"state": "ready", "outcome": {"kind": "ready", "ok": True, "retryable": False}}


def test_recorded_trade_correction_cancel_reach_one_reducer_and_feed_ingest() -> None:
    """The actual receive path feeds the same reducer that FeedSession freezes."""
    socket = RECORDED_SOCKET.BlockingRecordedWebSocket(_recorded_payloads())
    aggregator = LocalSecondAggregator("AAPL")
    client = AlpacaIEXClient(
        "AAPL",
        key_id="recorded-key",
        secret_key="recorded-secret",
        transport=RecordedWebSocketTransport([socket]),
        aggregator=aggregator,
        receive_clock=lambda: "2026-03-09T13:34:00Z",
    )
    session = FeedSession(symbol="AAPL", timeframe="1Min", aggregator=aggregator, stream_client=client)

    started = session.start_stream()
    assert started["ok"] is True
    assert client.aggregator is aggregator is session.aggregator
    _wait_for(lambda: len(client.messages) == 4)

    message_types = [message.event.message_type for message in client.messages]
    assert message_types == ["t", "t", "c", "x"]
    assert client.messages[2].update is not None and client.messages[2].update.correction
    assert client.messages[3].update is not None and client.messages[3].update.cancel
    assert set(aggregator.registry) == {2}

    event = session.ingest(now="2026-03-09T13:34:04Z")[0]
    assert event.open == "10.25"
    assert event.high == "10.25"
    assert event.low == "10.25"
    assert event.close == "10.25"
    assert event.volume == "2"
    assert session.latest_second is event

    stopped = session.stop_stream()
    assert stopped["ok"] is True
    assert socket.closed is True
    assert session._stream_thread is None


def test_recorded_runtime_step_ingests_real_second_event(tmp_path: Path) -> None:
    """Recorded auth/subscription/trades traverse AutomaticPaperRuntime.step."""
    socket = RECORDED_SOCKET.BlockingRecordedWebSocket(_recorded_payloads())
    aggregator = LocalSecondAggregator("AAPL")
    client = AlpacaIEXClient(
        "AAPL",
        key_id="recorded-key",
        secret_key="recorded-secret",
        transport=RecordedWebSocketTransport([socket]),
        aggregator=aggregator,
        receive_clock=lambda: "2026-03-09T13:34:00Z",
    )
    registry = build_registry(
        str(ROOT / "knowledge"),
        registrations=[
            {"strategy_id": "pa:nanqiao-range-extreme-limit", "record_id": "nanqiao-range-extreme-limit"},
            {"strategy_id": "pa:nanqiao-always-in-ema20", "record_id": "nanqiao-always-in-ema20"},
        ],
    )
    config = RuntimeConfig.from_kwargs(
        symbol="AAPL",
        timeframe="1min",
        database_path=str(tmp_path / "recorded-runtime.db"),
    )
    engine = AutomaticPaperRuntime(
        config,
        registry=registry,
        history_adapter=ENGINE_BUILDERS.FakeHistoryAdapter(history_result=ENGINE_BUILDERS.history_result()),
        aggregator=aggregator,
        stream_client=client,
        db_path=str(tmp_path / "recorded-runtime.db"),
        run_id="g028-recorded-runtime",
        clock=FakeClock("2026-03-09T13:34:03Z"),
    )

    startup = engine.start()
    assert startup["errors"] == []
    _wait_for(lambda: len(client.messages) == 4)
    report = engine.step(now="2026-03-09T13:34:04Z")

    assert report["ingested"] == 1
    assert report["view_executable"] is True
    assert engine.feed is not None
    assert engine.feed.latest_second is not None
    assert engine.feed.latest_second.close == "10.25"
    assert engine.feed.aggregator is client.aggregator is aggregator

    stopped = engine.stop(reason="recorded-test")
    assert stopped["stream"]["ok"] is True
    assert socket.closed is True
    assert engine._stream_client._pending == []


def test_runtime_disconnect_recovery_commit_controls_live_cutover(tmp_path: Path) -> None:
    """A reconnect stays gated until runtime recovery commits and releases it."""

    ready = [
        '[{"T":"success","msg":"authenticated"}]',
        '[{"T":"subscription","trades":["AAPL"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
    ]
    first_trade = '[{"T":"t","S":"AAPL","i":7,"x":"V","p":10.25,"s":2,"c":["@"],"t":"2026-03-09T13:33:59.2Z","z":"C"}]'
    replayed_trade = first_trade
    old_gap_trade = '[{"T":"t","S":"AAPL","i":9,"x":"V","p":10.40,"s":1,"c":["@"],"t":"2026-03-09T13:34:01.2Z","z":"C"}]'
    old_correction = '[{"T":"c","S":"AAPL","x":"V","oi":7,"op":10.25,"os":2,"oc":["@"],"ci":70,"cp":10.00,"cs":2,"cc":["@"],"z":"C","t":"2026-03-09T13:35:05Z"}]'
    old_cancel = '[{"T":"x","S":"AAPL","i":7,"x":"V","p":10.25,"s":2,"a":"C","t":"2026-03-09T13:35:06Z","z":"C"}]'
    post_reconnect_trade = '[{"T":"t","S":"AAPL","i":8,"x":"V","p":10.50,"s":1,"c":["@"],"t":"2026-03-09T13:35:00.2Z","z":"C"}]'
    first_socket = RECORDED_SOCKET.BlockingRecordedWebSocket([*ready, first_trade])
    second_socket = RECORDED_SOCKET.BlockingRecordedWebSocket([*ready, replayed_trade, old_gap_trade, old_correction, old_cancel, post_reconnect_trade])
    aggregator = LocalSecondAggregator("AAPL")
    client = AlpacaIEXClient(
        "AAPL",
        key_id="recorded-key",
        secret_key="recorded-secret",
        transport=RecordedWebSocketTransport([first_socket, second_socket]),
        aggregator=aggregator,
        reconnect_attempts=1,
        receive_clock=lambda: "2026-03-09T13:34:00Z",
    )
    history_adapter = ENGINE_BUILDERS.FakeHistoryAdapter(history_result=ENGINE_BUILDERS.history_result())
    registry = build_registry(
        str(ROOT / "knowledge"),
        registrations=[
            {"strategy_id": "pa:nanqiao-range-extreme-limit", "record_id": "nanqiao-range-extreme-limit"},
            {"strategy_id": "pa:nanqiao-always-in-ema20", "record_id": "nanqiao-always-in-ema20"},
        ],
    )
    config = RuntimeConfig.from_kwargs(
        symbol="AAPL",
        timeframe="1min",
        database_path=str(tmp_path / "recorded-recovery-runtime.db"),
    )
    engine = AutomaticPaperRuntime(
        config,
        registry=registry,
        history_adapter=history_adapter,
        aggregator=aggregator,
        stream_client=client,
        db_path=str(tmp_path / "recorded-recovery-runtime.db"),
        run_id="g028-recorded-recovery",
        clock=FakeClock("2026-03-09T13:34:03Z"),
    )

    engine.start()
    try:
        _wait_for(lambda: len(client.messages) == 1)
        first_step = engine.step(now="2026-03-09T13:34:04Z")
        assert first_step["ingested"] == 1
        assert aggregator.registry.keys() == {7}

        first_socket.close()
        _wait_for(lambda: len(client.transport.urls) == 2)
        _wait_for(lambda: engine.feed is not None and engine.feed.status()["stream"]["transport_ready"] is True)
        stream = engine.feed.status()["stream"]
        assert stream["gap_open"] is True
        assert stream["live_dispatch_allowed"] is False
        assert len(client.messages) == 1
        assert aggregator.registry.keys() == {7}

        history_adapter.recovery_result = ENGINE_BUILDERS.recovery_result(
            start_at="2026-03-09T13:34:00Z",
            end_at="2026-03-09T13:35:00Z",
            bars=[ENGINE_BUILDERS.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z")],
        )
        commit = engine._sink.commit_market_recovery

        def fail_commit(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("commit blocked")

        engine._sink.commit_market_recovery = fail_commit
        failed = engine.step(now="2026-03-09T13:35:01Z")
        assert failed["recovery"]["status"] == "rejected"
        assert failed["recovery"]["code"] == "recovery_persist_failed"
        assert engine._recovery_pending is True
        assert engine.feed.connection_gaps
        assert len(client.messages) == 1
        assert 8 not in aggregator.registry

        engine._sink.commit_market_recovery = commit
        recovered = engine.step(now="2026-03-09T13:35:07Z")
        assert recovered["recovery"]["status"] == "recovered"
        assert engine._recovery_pending is False
        assert engine.feed.connection_gaps == ()
        _wait_for(lambda: len(client.messages) == 2)
        assert set(aggregator.registry) == {8}
        assert aggregator.connection_gaps == ()
        assert engine.feed.frozen_events == ()
        assert engine.feed.live_cutover_at == "2026-03-09T13:35:00Z"
        assert len(engine.feed.skipped_stream_events) >= 3
        assert engine.feed.status()["stream"]["live_dispatch_allowed"] is True

        post_reconnect_step = engine.step(now="2026-03-09T13:35:08Z")
        assert post_reconnect_step["ingested"] == 1
    finally:
        engine.stop(reason="recorded-recovery-test")
    assert first_socket.closed is True
    assert second_socket.closed is True


def test_authentication_failure_is_not_reported_as_connected() -> None:
    aggregator = LocalSecondAggregator("AAPL")
    client = AlpacaIEXClient("AAPL", aggregator=aggregator)
    session = FeedSession(symbol="AAPL", timeframe="1Min", aggregator=aggregator, stream_client=client)

    status = session.start_stream()

    assert status["ok"] is False
    assert status["stream"] == "error"
    assert status["error"] == "authentication"
    assert status["client_status"]["outcome"]["kind"] == "authentication"
    assert session.status()["stream"]["gap_open"] is True
    blocked = session.release_stream_recovery()
    assert blocked["ok"] is False
    assert blocked["error"] == "stream_not_ready"
    assert session.connection_gaps
    assert session._stream_thread is None


def test_cli_factory_passes_the_runtime_aggregator_to_alpaca_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY_ID", "key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret")
    aggregator = LocalSecondAggregator("AAPL")
    history, client = _alpaca_adapters_from_env("AAPL", ("key", "secret"), aggregator=aggregator)

    assert history is not None
    assert client.aggregator is aggregator


def test_mismatched_aggregators_fail_before_stream_start() -> None:
    session_aggregator = LocalSecondAggregator("AAPL")
    client_aggregator = LocalSecondAggregator("AAPL")
    client = AlpacaIEXClient("AAPL", aggregator=client_aggregator)

    with pytest.raises(FeedSessionError) as error:
        FeedSession(
            symbol="AAPL",
            timeframe="1Min",
            aggregator=session_aggregator,
            stream_client=client,
        )
    assert error.value.code == "aggregator_mismatch"


def test_start_stop_is_idempotent_and_closes_one_non_daemon_consumer() -> None:
    frames = [
        '[{"T":"success","msg":"authenticated"}]',
        '[{"T":"subscription","trades":["AAPL"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
    ]
    socket = RECORDED_SOCKET.BlockingRecordedWebSocket(frames)
    aggregator = LocalSecondAggregator("AAPL")
    client = AlpacaIEXClient(
        "AAPL",
        key_id="key",
        secret_key="secret",
        transport=RecordedWebSocketTransport([socket]),
        aggregator=aggregator,
    )
    session = FeedSession(symbol="AAPL", timeframe="1Min", aggregator=aggregator, stream_client=client)

    first = session.start_stream()
    thread = session._stream_thread
    second = session.start_stream()
    assert first["ok"] is True
    assert second["already_running"] is True
    assert len(client.transport.urls) == 1
    assert thread is not None and thread.daemon is False and thread.is_alive()

    stopped = session.stop_stream()
    repeated = session.stop_stream()
    assert stopped["ok"] is True
    assert repeated == stopped
    assert socket.closed is True
    assert not thread.is_alive()


def test_background_consumer_reconnects_once_and_deduplicates_replayed_trade() -> None:
    ready = [
        '[{"T":"success","msg":"authenticated"}]',
        '[{"T":"subscription","trades":["AAPL"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
    ]
    trade = '[{"T":"t","S":"AAPL","i":7,"x":"V","p":10.25,"s":2,"c":["@"],"t":"2026-03-09T13:33:59.2Z","z":"C"}]'
    new_trade = '[{"T":"t","S":"AAPL","i":8,"x":"V","p":10.50,"s":1,"c":["@"],"t":"2026-03-09T13:34:00.2Z","z":"C"}]'
    first_socket = RECORDED_SOCKET.BlockingRecordedWebSocket([*ready, trade])
    second_socket = RECORDED_SOCKET.BlockingRecordedWebSocket([*ready, trade, new_trade])
    aggregator = LocalSecondAggregator("AAPL")
    client = AlpacaIEXClient(
        "AAPL",
        key_id="key",
        secret_key="secret",
        transport=RecordedWebSocketTransport([first_socket, second_socket]),
        aggregator=aggregator,
        reconnect_attempts=1,
        receive_clock=lambda: "2026-03-09T13:34:00Z",
    )
    session = FeedSession(symbol="AAPL", timeframe="1Min", aggregator=aggregator, stream_client=client)

    session.start_stream()
    try:
        _wait_for(lambda: len(client.messages) == 1)
        first_socket.close()
        _wait_for(lambda: len(client.transport.urls) == 2)

        # Reauthentication/subscription only makes the new transport ready.
        # The replay and post-cutover trade stay queued until recovery closes
        # the unresolved gap.
        assert client.reconnect_count == 1
        assert len(client.messages) == 1
        assert aggregator.registry.keys() == {7}
        assert session.connection_gaps
        stream = session.status()["stream"]
        assert stream["gap_open"] is True
        assert stream["transport_ready"] is True
        assert stream["live_dispatch_allowed"] is False

        released = session.release_stream_recovery(cutover_at="2026-03-09T13:34:00Z")
        assert released["ok"] is True
        _wait_for(lambda: len(client.messages) == 2)
        assert aggregator.registry.keys() == {8}
        assert aggregator.connection_gaps == ()
        assert session.connection_gaps == ()
        stream = session.status()["stream"]
        assert stream["gap_open"] is False
        assert stream["live_dispatch_allowed"] is True
    finally:
        session.stop_stream()
    assert second_socket.closed is True


def test_cross_cutover_correction_cancel_targets_current_live_reducer() -> None:
    """A live target wins over correction delivery time at the seam."""

    ready = [
        '[{"T":"success","msg":"authenticated"}]',
        '[{"T":"subscription","trades":["AAPL"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
    ]
    frames = [
        *ready,
        '[{"T":"t","S":"AAPL","i":101,"x":"V","p":10.0,"s":1,"c":["@"],"t":"2026-03-09T13:35:02Z","z":"C"}]',
        '[{"T":"c","S":"AAPL","x":"V","oi":101,"op":10.0,"os":1,"oc":["@"],"ci":102,"cp":9.5,"cs":1,"cc":["@"],"t":"2026-03-09T13:34:59Z","z":"C"}]',
        '[{"T":"t","S":"AAPL","i":103,"x":"V","p":10.1,"s":1,"c":["@"],"t":"2026-03-09T13:35:03Z","z":"C"}]',
        '[{"T":"x","S":"AAPL","i":103,"x":"V","p":10.1,"s":1,"a":"C","t":"2026-03-09T13:34:58Z","z":"C"}]',
        '[{"T":"c","S":"AAPL","x":"V","oi":7,"op":10.0,"os":1,"oc":["@"],"ci":70,"cp":9.0,"cs":1,"cc":["@"],"t":"2026-03-09T13:35:09Z","z":"C"}]',
        '[{"T":"x","S":"AAPL","i":7,"x":"V","p":10.0,"s":1,"a":"C","t":"2026-03-09T13:35:09Z","z":"C"}]',
    ]
    socket = RECORDED_SOCKET.BlockingRecordedWebSocket(frames)
    aggregator = LocalSecondAggregator("AAPL")
    receive_at = ["2026-03-09T13:35:04Z"]
    client = AlpacaIEXClient(
        "AAPL",
        key_id="key",
        secret_key="secret",
        transport=RecordedWebSocketTransport([socket]),
        aggregator=aggregator,
        receive_clock=lambda: receive_at[0],
    )
    session = FeedSession(
        symbol="AAPL",
        timeframe="1Min",
        aggregator=aggregator,
        stream_client=client,
        clock=FakeClock("2026-03-09T13:35:10Z"),
    )

    session.mark_connection_gap(start_at="2026-03-09T13:35:00Z", reason="disconnect")
    assert session.start_stream()["ok"] is True
    try:
        released = session.release_stream_recovery(
            cutover_at="2026-03-09T13:35:00Z",
            end_at="2026-03-09T13:35:10Z",
        )
        assert released["ok"] is True
        _wait_for(lambda: len(client.messages) == 4)

        # The correction is delivered before the cutover but targets the
        # current live trade, while the cancel removes another live target.
        assert set(aggregator.registry) == {102}
        assert len(session.skipped_stream_events) == 2
        assert {item["reason"] for item in session.skipped_stream_events} == {"target_not_in_live_cutover"}

        frozen = session.ingest(now="2026-03-09T13:35:10Z")
        assert len(frozen) == 1
        event = frozen[0]
        assert (event.start_at, event.end_at) == ("2026-03-09T13:35:02Z", "2026-03-09T13:35:03Z")
        assert (event.open, event.high, event.low, event.close, event.volume) == ("9.5", "9.5", "9.5", "9.5", "1")
        assert ("2026-03-09T13:35:03Z", "2026-03-09T13:35:04Z") not in aggregator.frozen_events

        # A later correction still targets the active live registry, but the
        # already issued execution event remains immutable and the new state
        # is retained only as G020 reconciliation lineage.
        frozen_before = aggregator.frozen_events[(event.start_at, event.end_at)].to_dict()
        receive_at[0] = "2026-03-09T13:35:11Z"
        socket.push(
            '[{"T":"c","S":"AAPL","x":"V","oi":102,"op":9.5,"os":1,"oc":["@"],"ci":104,"cp":8.5,"cs":1,"cc":["@"],"t":"2026-03-09T13:34:59Z","z":"C"}]'
        )
        _wait_for(lambda: len(client.messages) == 5)
        assert set(aggregator.registry) == {104}
        assert aggregator.frozen_events[(event.start_at, event.end_at)].to_dict() == frozen_before
        assert aggregator.reconciliation_events[(event.start_at, event.end_at)]
        assert session.ingest(now="2026-03-09T13:35:12Z") == []
    finally:
        stopped = session.stop_stream()
    assert stopped["ok"] is True
    assert socket.closed is True


def test_reset_for_cutover_clears_full_reducer_state_preserving_identity() -> None:
    """G020 owns the complete in-place reset seam used by FeedSession."""

    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(
        parse_trade(
            {"T": "t", "S": "AAPL", "i": 1, "x": "V", "p": "10", "s": 1, "c": ["@"], "t": "2026-03-09T13:34:00.1Z", "z": "C"},
            received_at="2026-03-09T13:34:01Z",
        )
    )
    aggregator.add_trade(
        parse_trade(
            {"T": "t", "S": "AAPL", "i": 2, "x": "V", "p": "10.1", "s": 1, "c": ["@", "4", "I"], "t": "2026-03-09T13:34:01.1Z", "z": "C"},
            received_at="2026-03-09T13:34:02Z",
        )
    )
    aggregator.add_trade(
        parse_trade(
            {"T": "t", "S": "AAPL", "i": 3, "x": "V", "p": "12", "s": 1, "c": ["@"], "t": "2026-03-09T13:34:02.1Z", "z": "C"},
            received_at="2026-03-09T13:34:03Z",
        )
    )
    aggregator.apply_cancel(
        parse_cancel_error(
            {"T": "x", "S": "AAPL", "i": 1, "x": "V", "p": "10", "s": 1, "a": "C", "t": "2026-03-09T13:34:03Z", "z": "C"},
            received_at="2026-03-09T13:34:03Z",
        )
    )
    aggregator.freeze(now="2026-03-09T13:34:06Z")
    aggregator.apply_correction(
        parse_correction(
            {"T": "c", "S": "AAPL", "x": "V", "oi": 3, "op": "12", "os": 1, "oc": ["@"], "ci": 4, "cp": "13", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:34:07Z"},
            received_at="2026-03-09T13:34:07Z",
        )
    )
    aggregator.mark_connection_gap("2026-03-09T13:34:00Z", None, reason="disconnect")

    identity = id(aggregator)
    assert aggregator.registry
    assert aggregator.trade_history
    assert aggregator._tombstones
    assert aggregator.frozen_events
    assert aggregator.reconciliation_events
    assert aggregator.audits
    assert aggregator.connection_gaps
    assert aggregator._sequence > 0

    aggregator.reset_for_cutover()

    assert id(aggregator) == identity
    assert aggregator.registry == {}
    assert aggregator.trade_history == {}
    assert aggregator._tombstones == set()
    assert aggregator.frozen_events == {}
    assert aggregator.reconciliation_events == {}
    assert aggregator.reconciliation_history == {}
    assert aggregator.audits == ()
    assert aggregator.connection_gaps == ()
    assert aggregator._sequence == 0


def test_mark_gap_after_recv_started_drops_frame_that_returns_late() -> None:
    ready = [
        '[{"T":"success","msg":"authenticated"}]',
        '[{"T":"subscription","trades":["AAPL"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
    ]
    late_trade = '[{"T":"t","S":"AAPL","i":99,"x":"V","p":10.75,"s":1,"c":["@"],"t":"2026-03-09T13:34:00.2Z","z":"C"}]'
    socket = RECORDED_SOCKET.GateRaceWebSocket(ready, late_trade)
    aggregator = LocalSecondAggregator("AAPL")
    client = AlpacaIEXClient(
        "AAPL",
        key_id="key",
        secret_key="secret",
        transport=RecordedWebSocketTransport([socket]),
        aggregator=aggregator,
        receive_clock=lambda: "2026-03-09T13:34:00Z",
    )
    session = FeedSession(symbol="AAPL", timeframe="1Min", aggregator=aggregator, stream_client=client)

    session.start_stream()
    try:
        _wait_for(socket.recv_started.is_set)
        session.mark_connection_gap(start_at="2026-03-09T13:34:00Z", reason="disconnect")
        socket.release_frame.set()
        _wait_for(lambda: len(session.skipped_stream_events) == 1)
        assert aggregator.registry == {}
        assert client.messages == []
        assert session.connection_gaps
        assert session.status()["stream"]["live_dispatch_allowed"] is False
    finally:
        stopped = session.stop_stream()
    assert stopped["ok"] is True
    assert socket.closed is True


class _FrameProbeLock:
    """RLock probe that records a freeze attempt while a frame is reducing."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._owner: int | None = None
        self.freeze_attempted = Event()

    def __enter__(self) -> "_FrameProbeLock":
        owner = get_ident()
        if self._owner is not None and self._owner != owner:
            self.freeze_attempted.set()
        self._lock.acquire()
        self._owner = owner
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._owner = None
        self._lock.release()


def test_one_recorded_frame_reduces_atomically_before_freeze() -> None:
    """A freeze racing a two-trade JSON frame sees the complete OHLCV."""

    probe = _FrameProbeLock()

    class BarrierAggregator(LocalSecondAggregator):
        def __init__(self) -> None:
            super().__init__("AAPL")
            self.first_trade = Event()

        def add_trade(self, event: Any, *, received_at: Any = None) -> Any:
            update = super().add_trade(event, received_at=received_at)
            if getattr(event, "trade_id", None) == 1:
                self.first_trade.set()
                assert probe.freeze_attempted.wait(2), "freeze did not contend with the frame reducer"
            return update

    frame = (
        '[{"T":"t","S":"AAPL","i":1,"x":"V","p":10.00,"s":1,"c":["@"],'
        '"t":"2026-03-09T13:33:59.1Z","z":"C"},'
        '{"T":"t","S":"AAPL","i":2,"x":"V","p":12.00,"s":2,"c":["@"],'
        '"t":"2026-03-09T13:33:59.2Z","z":"C"}]'
    )
    socket = RECORDED_SOCKET.BlockingRecordedWebSocket(
        [
            '[{"T":"success","msg":"authenticated"}]',
            '[{"T":"subscription","trades":["AAPL"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
            frame,
        ]
    )
    aggregator = BarrierAggregator()
    client = AlpacaIEXClient(
        "AAPL",
        key_id="key",
        secret_key="secret",
        transport=RecordedWebSocketTransport([socket]),
        aggregator=aggregator,
        receive_clock=lambda: "2026-03-09T13:34:00Z",
    )
    session = FeedSession(symbol="AAPL", timeframe="1Min", aggregator=aggregator, stream_client=client)
    session._stream_data_lock = probe
    client.dispatch_lock = probe
    freeze_result: dict[str, Any] = {}

    def freeze() -> None:
        try:
            freeze_result["events"] = session.ingest(now="2026-03-09T13:34:04Z")
        except BaseException as exc:  # pragma: no cover - assertion below reports it
            freeze_result["error"] = exc

    session.start_stream()
    freeze_thread: Thread | None = None
    try:
        _wait_for(aggregator.first_trade.is_set)
        freeze_thread = Thread(target=freeze, name="g028-frame-freeze")
        freeze_thread.start()
        _wait_for(probe.freeze_attempted.is_set)
        _wait_for(lambda: len(client.messages) == 2)
        freeze_thread.join(2)
        assert freeze_thread.is_alive() is False
        assert "error" not in freeze_result
        events = freeze_result["events"]
        assert len(events) == 1
        event = events[0]
        assert event.open == "10"
        assert event.high == "12"
        assert event.low == "10"
        assert event.close == "12"
        assert event.volume == "3"
    finally:
        if freeze_thread is not None:
            freeze_thread.join(2)
        session.stop_stream()


def test_disconnect_and_consumer_exception_leave_no_false_events_or_live_thread() -> None:
    socket = RECORDED_SOCKET.BlockingRecordedWebSocket(
        [
            '[{"T":"success","msg":"authenticated"}]',
            '[{"T":"subscription","trades":["AAPL"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
        ]
    )
    aggregator = LocalSecondAggregator("AAPL")
    client = AlpacaIEXClient(
        "AAPL",
        key_id="key",
        secret_key="secret",
        transport=RecordedWebSocketTransport([socket]),
        aggregator=aggregator,
        reconnect_attempts=0,
    )
    session = FeedSession(symbol="AAPL", timeframe="1Min", aggregator=aggregator, stream_client=client)
    session.start_stream()
    socket.close()
    _wait_for(lambda: session._stream_thread is None)
    assert aggregator.registry == {}
    assert session.status()["stream"]["gap_open"] is True
    session.stop_stream()
    assert socket.closed is True

    exploding_aggregator = LocalSecondAggregator("AAPL")

    class ExplodingClient:
        def __init__(self) -> None:
            self.aggregator = exploding_aggregator
            self.closed = False

        def connect(self) -> dict[str, Any]:
            return _ready_status()

        def receive_safe(self) -> Any:
            raise RuntimeError("fixture consumer failure")

        def close(self) -> None:
            self.closed = True

    exploding = ExplodingClient()
    exploding_session = FeedSession(
        symbol="AAPL",
        timeframe="1Min",
        aggregator=exploding_aggregator,
        stream_client=exploding,
    )
    exploding_session.start_stream()
    _wait_for(lambda: exploding_session._stream_thread is None)
    assert exploding_session.status()["stream"]["status"]["outcome"]["kind"] == "consumer_error"
    assert exploding_aggregator.registry == {}
    assert exploding.closed is True
    exploding_session.stop_stream()
