"""Injectable Alpaca IEX WebSocket client.

The default transport uses the optional ``websockets`` package at runtime,
while tests inject a tiny synchronous socket implementing ``send``/``recv``.
No package import or network request is performed by pure parser/aggregator
imports.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol, Sequence

from .aggregate import AggregateUpdate, LocalSecondAggregator
from .errors import (
    AlpacaStreamError,
    AuthenticationError,
    DisconnectError,
    DuplicateTradeError,
    EntitlementError,
    MalformedPayloadError,
    ProtocolError,
    ServerError,
    StreamOutcome,
    StreamTimeoutError,
    SubscriptionError,
    SubscriptionMismatchError,
    outcome_for_error,
)
from .parser import StreamEvent, StatusEvent, parse_message


STREAM_URL = "wss://stream.data.alpaca.markets/v2/iex"
BASE_URL = STREAM_URL
FEED = "iex"
PROVIDER = "alpaca"
SYMBOL_RE = r"^[A-Z][A-Z0-9.\-]{0,11}$"
_CONTROL_CODES = frozenset({400, 401, 402, 404, 405, 406, 407, 409, 410, 500})


class WebSocketLike(Protocol):
    def send(self, payload: str) -> Any:
        ...

    def recv(self, timeout: float | None = None) -> Any:
        ...

    def close(self) -> Any:
        ...


class WebSocketTransport(Protocol):
    def connect(self, url: str, *, timeout: float) -> WebSocketLike:
        ...


class OptionalWebSocketTransport:
    """Real network transport loaded only when a client is actually run."""

    def connect(self, url: str, *, timeout: float) -> WebSocketLike:
        try:
            from websockets.sync.client import connect  # type: ignore[import-not-found]
        except (ImportError, ModuleNotFoundError):
            raise ProtocolError("websocket client dependency unavailable") from None
        try:
            return connect(url, open_timeout=timeout, close_timeout=timeout)
        except TimeoutError:
            raise StreamTimeoutError() from None
        except Exception:
            raise DisconnectError() from None


@dataclass(frozen=True, slots=True)
class ClientConfig:
    symbol: str
    key_id: str | None = None
    secret_key: str | None = None
    timeout_seconds: float = 10.0
    reconnect_attempts: int = 1
    reconnect_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        import re

        if not isinstance(self.symbol, str) or re.fullmatch(SYMBOL_RE, self.symbol) is None:
            raise ValueError("symbol must be an upper-case US-equity symbol")
        if self.key_id is not None and (not isinstance(self.key_id, str) or not self.key_id):
            raise ValueError("key_id must be non-empty when provided")
        if self.secret_key is not None and (not isinstance(self.secret_key, str) or not self.secret_key):
            raise ValueError("secret_key must be non-empty when provided")
        if isinstance(self.timeout_seconds, bool) or self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if isinstance(self.reconnect_attempts, bool) or not 0 <= self.reconnect_attempts <= 100:
            raise ValueError("reconnect_attempts is out of range")

    @property
    def credentials_present(self) -> bool:
        return bool(self.key_id and self.secret_key)


@dataclass(frozen=True, slots=True)
class ClientStatus:
    state: str
    outcome: StreamOutcome
    reconnect_count: int = 0
    authenticated: bool = False
    subscribed: bool = False
    corrections_enabled: bool = False
    cancel_errors_enabled: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "outcome": self.outcome.to_dict(),
            "reconnect_count": self.reconnect_count,
            "authenticated": self.authenticated,
            "subscribed": self.subscribed,
            "corrections_enabled": self.corrections_enabled,
            "cancel_errors_enabled": self.cancel_errors_enabled,
            "feed": "alpaca_iex",
        }


@dataclass(frozen=True, slots=True)
class ClientMessage:
    event: StreamEvent
    update: AggregateUpdate | None = None


@dataclass
class InMemoryWebSocket:
    """Small fixture socket useful for callers outside pytest."""

    incoming: list[Any] = field(default_factory=list)
    sent: list[str] = field(default_factory=list)
    closed: bool = False

    def send(self, payload: str) -> None:
        self.sent.append(payload)

    def recv(self, timeout: float | None = None) -> Any:
        if not self.incoming:
            if timeout is not None:
                raise TimeoutError()
            raise EOFError()
        return self.incoming.pop(0)

    def close(self) -> None:
        self.closed = True


class RecordedWebSocketTransport:
    """Deterministic queue of sockets for reconnect/lifecycle tests."""

    def __init__(self, sockets: Sequence[WebSocketLike] | Callable[[str], WebSocketLike]) -> None:
        self.sockets = list(sockets) if not callable(sockets) else []
        self.callback = sockets if callable(sockets) else None
        self.urls: list[str] = []

    def connect(self, url: str, *, timeout: float) -> WebSocketLike:
        self.urls.append(url)
        if self.callback is not None:
            return self.callback(url)
        if not self.sockets:
            raise DisconnectError()
        return self.sockets.pop(0)


def _now_text() -> str:
    now = datetime.now(timezone.utc)
    if now.microsecond:
        return now.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".") + "Z"
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_send(socket: WebSocketLike, payload: Mapping[str, Any]) -> None:
    try:
        socket.send(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    except Exception:
        raise DisconnectError() from None


def _control_error(status: StatusEvent) -> AlpacaStreamError:
    code = status.code
    if code in {401, 402}:
        return AuthenticationError(code=code)
    if code == 404:
        # Alpaca uses 404 when the auth response did not arrive before its
        # control deadline.  Preserve it as a typed timeout rather than an
        # entitlement failure.
        return StreamTimeoutError(code=code)
    if code in {405, 406}:
        return EntitlementError(code=code)
    if code == 407:
        return DisconnectError(code=code)
    if code == 409:
        return EntitlementError(code=code)
    if code == 410:
        return SubscriptionError(code=code)
    if code == 500:
        return ServerError(code=code)
    if code == 400:
        return ProtocolError(code=code)
    return ProtocolError(code=code)


class AlpacaIEXClient:
    """One-symbol authenticated IEX trades client with explicit reconnect."""

    def __init__(
        self,
        symbol: str,
        *,
        key_id: str | None = None,
        secret_key: str | None = None,
        transport: WebSocketTransport | None = None,
        timeout_seconds: float = 10.0,
        reconnect_attempts: int = 1,
        reconnect_delay_seconds: float = 0.0,
        aggregator: LocalSecondAggregator | None = None,
        clock: Callable[[], float] | None = None,
        receive_clock: Callable[[], Any] | None = None,
    ) -> None:
        self.config = ClientConfig(symbol, key_id, secret_key, timeout_seconds, reconnect_attempts, reconnect_delay_seconds)
        self.transport = transport or OptionalWebSocketTransport()
        self.aggregator = aggregator or LocalSecondAggregator(symbol)
        self.clock = clock or time.monotonic
        self.receive_clock = receive_clock or _now_text
        self.socket: WebSocketLike | None = None
        self.reconnect_count = 0
        self.authenticated = False
        self.subscribed = False
        self.corrections_enabled = False
        self.cancel_errors_enabled = False
        self.messages: list[ClientMessage] = []
        self._pending: list[StreamEvent] = []
        self.supports_receive_timeout = True
        # The runtime may install a reducer lock here.  It is held only while
        # dispatching a parsed event, never while waiting on socket.recv().
        self.dispatch_lock: Any = None
        # FeedSession installs a gate here.  It is called while dispatch_lock
        # is held, after recv() returns and immediately before each event is
        # reduced.  That second check closes the recv-before-gap race without
        # holding the reducer lock during a blocked socket read.
        self.dispatch_filter: Callable[[StreamEvent], bool] | None = None
        self.last_status = ClientStatus("idle", StreamOutcome("idle", True), 0)

    @classmethod
    def from_env(cls, symbol: str, **kwargs: Any) -> "AlpacaIEXClient":
        return cls(symbol, key_id=os.getenv("APCA_API_KEY_ID"), secret_key=os.getenv("APCA_API_SECRET_KEY"), **kwargs)

    @property
    def symbol(self) -> str:
        return self.config.symbol

    @property
    def credentials_present(self) -> bool:
        return self.config.credentials_present

    @property
    def status(self) -> ClientStatus:
        return self.last_status

    @property
    def outcome(self) -> StreamOutcome:
        return self.last_status.outcome

    def _set_status(self, state: str, outcome: StreamOutcome) -> ClientStatus:
        self.last_status = ClientStatus(state, outcome, self.reconnect_count, self.authenticated, self.subscribed, self.corrections_enabled, self.cancel_errors_enabled)
        return self.last_status

    def connect(self) -> ClientStatus:
        self.authenticated = False
        self.subscribed = False
        self.corrections_enabled = False
        self.cancel_errors_enabled = False
        self._pending.clear()
        if not self.credentials_present:
            return self._set_status("error", StreamOutcome("authentication", False, None, False, "Alpaca authentication failed"))
        try:
            if self.socket is not None:
                try:
                    self.socket.close()
                except Exception:
                    pass
            self.socket = self.transport.connect(STREAM_URL, timeout=self.config.timeout_seconds)
            _safe_send(self.socket, {"action": "auth", "key": self.config.key_id, "secret": self.config.secret_key})
            deadline = self.clock() + self.config.timeout_seconds
            while True:
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise StreamTimeoutError()
                event = self._receive(timeout=remaining)
                if isinstance(event, StatusEvent):
                    if event.message_type == "error":
                        raise _control_error(event)
                    if event.semantic == "subscription_ack":
                        raise ProtocolError()
                    # A transport-level ``connected`` success is not an auth
                    # acknowledgement.  Only the explicit authenticated
                    # semantic may advance this state.
                    if event.semantic == "authenticated":
                        self.authenticated = True
                        break
                    continue
                # Data before authentication is a protocol failure.
                raise ProtocolError()
            _safe_send(self.socket, {"action": "subscribe", "trades": [self.symbol]})
            deadline = self.clock() + self.config.timeout_seconds
            while True:
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise StreamTimeoutError()
                event = self._receive(timeout=remaining)
                if isinstance(event, StatusEvent):
                    if event.message_type == "error":
                        raise _control_error(event)
                    if event.semantic == "subscription_ack":
                        target = self.symbol
                        if not (event.trades == (target,) and event.corrections == (target,) and event.cancel_errors == (target,)):
                            raise SubscriptionMismatchError()
                        self.subscribed = True
                        self.corrections_enabled = target in event.corrections
                        self.cancel_errors_enabled = target in event.cancel_errors
                        return self._set_status("ready", StreamOutcome("ready", True))
                    continue
                # Data before a matching subscription acknowledgement is a
                # protocol violation.  It must not substitute for control
                # confirmation or be aggregated under an unknown lifecycle.
                raise ProtocolError()
        except AlpacaStreamError as error:
            self.authenticated = False if error.kind == "authentication" else self.authenticated
            return self._set_status("error", outcome_for_error(error))
        except TimeoutError:
            return self._set_status("error", outcome_for_error(StreamTimeoutError()))
        except (EOFError, StopIteration):
            return self._set_status("error", outcome_for_error(DisconnectError()))
        except Exception:
            return self._set_status("error", outcome_for_error(DisconnectError()))

    open = connect

    def _read_frame_events(self, *, timeout: float | None = None) -> tuple[StreamEvent, ...]:
        if self.socket is None:
            raise DisconnectError()
        if timeout is not None and timeout <= 0:
            raise StreamTimeoutError()
        try:
            if timeout is None:
                payload = self.socket.recv()
            else:
                payload = self.socket.recv(timeout=timeout)
        except TimeoutError:
            raise StreamTimeoutError() from None
        except (EOFError, StopIteration, OSError):
            raise DisconnectError() from None
        except Exception:
            raise DisconnectError() from None
        events = parse_message(payload, received_at=self.receive_clock())
        if not events:
            raise MalformedPayloadError()
        return tuple(events)

    def _receive(self, *, timeout: float | None = None) -> StreamEvent:
        if self._pending:
            return self._pending.pop(0)
        events = self._read_frame_events(timeout=timeout)
        first, *rest = events
        # The single-event API preserves its historical behavior.  The
        # frame API below consumes this remainder as one frame, so runtime
        # callers never split a JSON array across reducer critical sections.
        if rest:
            self._pending.extend(rest)
        return first

    def receive(self, *, timeout: float | None = None) -> ClientMessage:
        """Read and reduce one normalized stream event."""

        if getattr(self, "_pending", None):
            event = self._pending.pop(0)
        else:
            event = self._receive(timeout=timeout)
        update = self._dispatch(event)
        message = ClientMessage(event, update)
        self.messages.append(message)
        return message

    def receive_frame(self, *, timeout: float | None = None) -> tuple[ClientMessage, ...]:
        """Read and reduce one complete JSON-array frame atomically.

        Socket waiting happens before the reducer lock is acquired.  Once a
        frame is available, every normalized event in provider order is
        dispatched while the installed runtime lock is held once, preventing
        ``FeedSession.ingest``/``freeze`` from observing a half-applied frame.
        """

        if self._pending:
            events = tuple(self._pending)
            self._pending.clear()
        else:
            events = self._read_frame_events(timeout=timeout)
        return self._dispatch_frame(events)

    def _dispatch(self, event: StreamEvent) -> AggregateUpdate | None:
        lock = self.dispatch_lock
        if lock is not None:
            with lock:
                return self._dispatch_unlocked(event)
        return self._dispatch_unlocked(event)

    def _dispatch_frame(self, events: Sequence[StreamEvent]) -> tuple[ClientMessage, ...]:
        lock = self.dispatch_lock
        if lock is None:
            return self._dispatch_frame_unlocked(events)
        with lock:
            return self._dispatch_frame_unlocked(events)

    def _dispatch_frame_unlocked(self, events: Sequence[StreamEvent]) -> tuple[ClientMessage, ...]:
        messages: list[ClientMessage] = []
        for event in events:
            dispatch_filter = self.dispatch_filter
            if dispatch_filter is not None and not dispatch_filter(event):
                # A recovery/cutover gate may deliberately discard a queued
                # data event.  It is not a ClientMessage: no reducer update,
                # ingest observation or false successful dispatch is created.
                continue
            update = self._dispatch_unlocked(event)
            message = ClientMessage(event, update)
            self.messages.append(message)
            messages.append(message)
        return tuple(messages)

    def _dispatch_unlocked(self, event: StreamEvent) -> AggregateUpdate | None:
        if isinstance(event, StatusEvent):
            if event.message_type == "error":
                raise _control_error(event)
            return None
        try:
            from .parser import CancelErrorEvent, CorrectionEvent, TradeEvent

            if isinstance(event, TradeEvent):
                return self.aggregator.add_trade(event)
            if isinstance(event, CorrectionEvent):
                return self.aggregator.apply_correction(event)
            if isinstance(event, CancelErrorEvent):
                return self.aggregator.apply_cancel(event)
        except DuplicateTradeError:
            # A provider replay with the same trade identity is idempotent
            # for the stream.  Do not tear down an otherwise healthy socket.
            return AggregateUpdate(None, False, duplicate=True, error="duplicate_conflict")
        except AlpacaStreamError:
            raise
        except Exception:
            raise MalformedPayloadError() from None
        return None

    def receive_safe(self, *, timeout: float | None = None) -> ClientMessage | ClientStatus:
        try:
            return self.receive(timeout=timeout)
        except AlpacaStreamError as error:
            return self._set_status("error", outcome_for_error(error))
        except (EOFError, StopIteration):
            return self._set_status("error", outcome_for_error(DisconnectError()))

    def receive_frame_safe(self, *, timeout: float | None = None) -> tuple[ClientMessage, ...] | ClientStatus:
        try:
            return self.receive_frame(timeout=timeout)
        except AlpacaStreamError as error:
            return self._set_status("error", outcome_for_error(error))
        except (EOFError, StopIteration):
            return self._set_status("error", outcome_for_error(DisconnectError()))

    def process_message(self, payload: Any, *, received_at: Any = None) -> tuple[ClientMessage, ...]:
        """Process one recorded JSON-array frame without a socket."""

        events = parse_message(payload, received_at=received_at or _now_text())
        return self._dispatch_frame(events)

    handle_message = process_message

    def run(self, *, max_messages: int | None = None) -> ClientStatus:
        """Run until a typed failure or message budget, reconnecting once by policy."""

        reconnects_used = 0
        processed = 0
        while True:
            status = self.connect()
            if status.state != "ready":
                self.aggregator.mark_connection_gap(_now_text(), None, reason=status.outcome.kind)
                if reconnects_used >= self.config.reconnect_attempts or not status.outcome.retryable:
                    return status
                reconnects_used += 1
                self.reconnect_count += 1
                if self.config.reconnect_delay_seconds:
                    time.sleep(self.config.reconnect_delay_seconds)
                continue
            self.aggregator.close_connection_gap(_now_text())
            while max_messages is None or processed < max_messages:
                try:
                    result = self.receive_safe()
                    if isinstance(result, ClientStatus):
                        self.aggregator.mark_connection_gap(_now_text(), None, reason=result.outcome.kind)
                        if result.outcome.retryable and reconnects_used < self.config.reconnect_attempts:
                            reconnects_used += 1
                            self.reconnect_count += 1
                            break
                        return result
                    processed += 1
                except AlpacaStreamError as error:
                    self.aggregator.mark_connection_gap(_now_text(), None, reason=error.kind)
                    if error.retryable and reconnects_used < self.config.reconnect_attempts:
                        reconnects_used += 1
                        self.reconnect_count += 1
                        break
                    return self._set_status("error", outcome_for_error(error))
            else:
                return self._set_status("ready", StreamOutcome("message_budget", True))
            # Reconnect loop reauthenticates/resubscribes.  The aggregator's
            # registry remains intact, so already accepted trade IDs are not
            # duplicated when a server replays them.

    def close(self) -> None:
        if self.socket is not None:
            try:
                self.socket.close()
            except Exception:
                pass
        self.socket = None


AlpacaIEXStreamClient = AlpacaIEXClient
AlpacaTradeStream = AlpacaIEXClient
AlpacaTradeStreamAdapter = AlpacaIEXClient
AlpacaIEXTradeAdapter = AlpacaIEXClient
AlpacaIEXStreamAdapter = AlpacaIEXClient
StreamResult = ClientStatus


__all__ = [
    "AlpacaIEXClient",
    "AlpacaIEXStreamClient",
    "AlpacaTradeStream",
    "AlpacaTradeStreamAdapter",
    "AlpacaIEXTradeAdapter",
    "AlpacaIEXStreamAdapter",
    "ClientConfig",
    "ClientMessage",
    "ClientStatus",
    "FEED",
    "BASE_URL",
    "InMemoryWebSocket",
    "OptionalWebSocketTransport",
    "PROVIDER",
    "RecordedWebSocketTransport",
    "STREAM_URL",
    "StreamResult",
    "WebSocketLike",
    "WebSocketTransport",
]
