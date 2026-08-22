"""Recorded Alpaca IEX socket fixture with a close-aware blocking receive."""

from __future__ import annotations

from collections import deque
from threading import Condition, Event
from typing import Any, Iterable


class BlockingRecordedWebSocket:
    """Replay frames, then wait until the owner closes the socket.

    A finite ``InMemoryWebSocket`` reports EOF as soon as the fixture frames
    are consumed, which is useful for reconnect tests but would turn a
    successful end-to-end replay into an artificial data gap.  This socket
    keeps the recorded transport alive until ``FeedSession.stop_stream``
    closes it, so the test exercises the normal running state and deterministic
    stop path.
    """

    def __init__(self, frames: Iterable[Any]) -> None:
        self._incoming = deque(frames)
        self._condition = Condition()
        self.sent: list[str] = []
        self.closed = False

    def send(self, payload: str) -> None:
        with self._condition:
            self.sent.append(payload)

    def push(self, frame: Any) -> None:
        """Append a deterministic frame to the live side of the replay."""
        with self._condition:
            if self.closed:
                raise RuntimeError("recorded socket is closed")
            self._incoming.append(frame)
            self._condition.notify_all()

    def recv(self, timeout: float | None = None) -> Any:
        with self._condition:
            if self._incoming:
                return self._incoming.popleft()
            if self.closed:
                raise EOFError()
            self._condition.wait(timeout)
            if self._incoming:
                return self._incoming.popleft()
            if self.closed:
                raise EOFError()
            raise TimeoutError()

    def close(self) -> None:
        with self._condition:
            self.closed = True
            self._condition.notify_all()


class GateRaceWebSocket(BlockingRecordedWebSocket):
    """Hold one data frame after recv starts for gap-gate race tests."""

    def __init__(self, frames: Iterable[Any], delayed_frame: Any) -> None:
        super().__init__(frames)
        self.delayed_frame = delayed_frame
        self.recv_started = Event()
        self.release_frame = Event()
        self._delay_started = False
        self._delay_delivered = False

    def recv(self, timeout: float | None = None) -> Any:
        with self._condition:
            if self._incoming:
                return self._incoming.popleft()
            if self.closed:
                raise EOFError()
            if not self._delay_started:
                self._delay_started = True
                self.recv_started.set()
        if not self.release_frame.wait(timeout):
            raise TimeoutError()
        with self._condition:
            if self.closed:
                raise EOFError()
            if not self._delay_delivered:
                self._delay_delivered = True
                return self.delayed_frame
        return super().recv(timeout=timeout)

    def close(self) -> None:
        self.release_frame.set()
        super().close()
