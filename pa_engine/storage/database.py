"""Physical connection, bootstrap/verify and active-run lease for G027.

:func:`open_database` is the single entry point that creates or attaches
one v1 SQLite runtime database.  A missing path is bootstrapped with
:func:`pa_engine.storage.schema.create_schema` in one transaction; an existing
file (including a zero-length file) must pass ``PRAGMA integrity_check``,
:func:`~pa_engine.storage.schema.verify_schema` and
:func:`~pa_engine.storage.schema.verify_invariants` before any use.
Corrupt or incompatible state fails explicitly and is never reset or
recreated over existing content.

Exactly one live runtime may hold a database: the singleton ``lease`` row
records the active ``run_id`` with a heartbeat and TTL.  A second opener
with an unexpired lease (even the same ``run_id`` — a duplicate live
handle is a bug) gets :class:`LeaseConflictError`; a stale lease left by
a crashed run is atomically replaced exactly once.
"""

from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

from . import schema
from .errors import (
    InvalidStorageInputError,
    LeaseConflictError,
    StorageClosedError,
    StorageCorruptionError,
)
from .times import instant_from_text, instant_to_text

LEASE_DEFAULT_TTL_SECONDS = 30

RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{2,127}$")

_CONSTRUCTION_TOKEN = object()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class LeaseInfo:
    """Snapshot of the active-run lease held by this handle."""

    run_id: str
    mode: str
    acquired_at: str
    heartbeat_at: str
    ttl_seconds: int


class StorageDatabase:
    """Open storage handle.  Construct only via :func:`open_database`."""

    def __init__(
        self,
        *,
        _token: object,
        conn: sqlite3.Connection,
        path: str,
        lease: LeaseInfo,
        clock: Callable[[], datetime],
    ) -> None:
        if _token is not _CONSTRUCTION_TOKEN:
            raise InvalidStorageInputError(
                "StorageDatabase must be constructed via open_database"
            )
        self._conn = conn
        self._path = path
        self._lease = lease
        self._clock = clock
        self._closed = False
        self._in_transaction = False

    @property
    def conn(self) -> sqlite3.Connection:
        """The raw connection for the store layer."""

        self._ensure_open()
        return self._conn

    @property
    def path(self) -> str:
        return self._path

    @property
    def lease(self) -> LeaseInfo:
        return self._lease

    @property
    def closed(self) -> bool:
        return self._closed

    def _ensure_open(self) -> None:
        if self._closed:
            raise StorageClosedError("storage handle is closed", path=self._path)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """One ``BEGIN IMMEDIATE``/``COMMIT`` scope, ``ROLLBACK`` on error.

        At most one transaction may be open per handle; a nested call
        raises :class:`InvalidStorageInputError`.
        """

        self._ensure_open()
        if self._in_transaction:
            raise InvalidStorageInputError(
                "nested transactions are not allowed on one handle"
            )
        self._conn.execute("BEGIN IMMEDIATE")
        self._in_transaction = True
        try:
            yield self._conn
        except BaseException:
            self._conn.rollback()
            raise
        else:
            self._conn.commit()
        finally:
            self._in_transaction = False

    def heartbeat(self) -> None:
        """Refresh the lease heartbeat iff this handle still holds it."""

        self._ensure_open()
        if self._in_transaction:
            raise InvalidStorageInputError(
                "heartbeat is not allowed inside an open transaction"
            )
        now_text = instant_to_text(self._clock())
        with self.transaction() as conn:
            updated = conn.execute(
                "UPDATE lease SET heartbeat_at = ? WHERE id = 1 AND run_id = ? "
                "AND acquired_at = ? AND heartbeat_at = ?",
                (
                    now_text,
                    self._lease.run_id,
                    self._lease.acquired_at,
                    self._lease.heartbeat_at,
                ),
            )
            if updated.rowcount != 1:
                row = conn.execute("SELECT run_id FROM lease WHERE id = 1").fetchone()
                raise LeaseConflictError(
                    "active runtime lease vanished or changed hands",
                    holder=row[0] if row is not None else None,
                )
        self._lease = replace(self._lease, heartbeat_at=now_text)

    def close(self, release_lease: bool = True) -> None:
        """Close the handle, optionally releasing our lease.  Idempotent."""

        if self._closed:
            return
        self._closed = True
        try:
            if self._in_transaction:
                self._conn.rollback()
                self._in_transaction = False
            if release_lease:
                self._conn.execute(
                    "DELETE FROM lease WHERE id = 1 AND run_id = ? AND acquired_at = ?",
                    (self._lease.run_id, self._lease.acquired_at),
                )
                self._conn.commit()
        finally:
            self._conn.close()


def _acquire_lease(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    mode: str,
    ttl_seconds: int,
    clock: Callable[[], datetime],
) -> LeaseInfo:
    now_text = instant_to_text(clock())
    now = instant_from_text(now_text)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT run_id, heartbeat_at, ttl_seconds FROM lease WHERE id = 1"
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO lease (id, run_id, mode, acquired_at, heartbeat_at, "
                "ttl_seconds) VALUES (1, ?, ?, ?, ?, ?)",
                (run_id, mode, now_text, now_text, ttl_seconds),
            )
        else:
            holder, heartbeat_text, holder_ttl = row
            if type(holder_ttl) is not int or not 1 <= holder_ttl <= 86400:
                raise StorageCorruptionError("lease.ttl_seconds is outside the supported range")
            try:
                heartbeat = instant_from_text(heartbeat_text)
            except ValueError:
                raise StorageCorruptionError(
                    "lease.heartbeat_at is not canonical UTC text"
                ) from None
            if heartbeat + timedelta(seconds=holder_ttl) > now:
                raise LeaseConflictError(holder=holder)
            conn.execute(
                "UPDATE lease SET run_id = ?, mode = ?, acquired_at = ?, "
                "heartbeat_at = ?, ttl_seconds = ? WHERE id = 1",
                (run_id, mode, now_text, now_text, ttl_seconds),
            )
    except BaseException:
        conn.rollback()
        raise
    conn.commit()
    return LeaseInfo(
        run_id=run_id,
        mode=mode,
        acquired_at=now_text,
        heartbeat_at=now_text,
        ttl_seconds=ttl_seconds,
    )


def open_database(
    path: str,
    *,
    run_id: str,
    mode: str,
    lease_ttl_seconds: int = LEASE_DEFAULT_TTL_SECONDS,
    clock: Callable[[], datetime] | None = None,
) -> StorageDatabase:
    """Open (bootstrapping if the path is missing) the v1 runtime database at *path*.

    Existing content must pass integrity, version and invariant checks;
    any failure closes the connection and propagates the typed error
    without modifying the file.
    """

    if not isinstance(path, str) or not path:
        raise InvalidStorageInputError("path must be a non-empty string")
    if not isinstance(run_id, str) or not RUN_ID_RE.match(run_id):
        raise InvalidStorageInputError("run_id has an invalid shape")
    if not isinstance(mode, str) or mode not in schema.EVENT_MODES:
        raise InvalidStorageInputError("mode is not in the closed event vocabulary")
    if (
        not isinstance(lease_ttl_seconds, int)
        or isinstance(lease_ttl_seconds, bool)
        or not 1 <= lease_ttl_seconds <= 86400
    ):
        raise InvalidStorageInputError("lease_ttl_seconds must be an int in 1..86400")
    if clock is None:
        clock = _utc_now
    elif not callable(clock):
        raise InvalidStorageInputError("clock must be callable")

    exists = os.path.exists(path)
    if exists and os.path.getsize(path) == 0:
        raise StorageCorruptionError(
            "an existing zero-length file is not a valid runtime database", path=path
        )
    fresh = not exists
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA synchronous=FULL")
        except sqlite3.DatabaseError:
            if not fresh:
                raise StorageCorruptionError(
                    "sqlite could not read the existing file as a database", path=path
                ) from None
            raise
        if fresh:
            conn.execute("BEGIN IMMEDIATE")
            try:
                schema.create_schema(conn)
            except BaseException:
                conn.rollback()
                raise
            conn.commit()
        else:
            integrity = conn.execute("PRAGMA integrity_check").fetchall()
            if integrity != [("ok",)]:
                raise StorageCorruptionError("sqlite integrity_check failed", path=path)
            schema.verify_schema(conn)
            schema.verify_invariants(conn)
        lease = _acquire_lease(
            conn,
            run_id=run_id,
            mode=mode,
            ttl_seconds=lease_ttl_seconds,
            clock=clock,
        )
    except BaseException:
        conn.close()
        raise
    return StorageDatabase(
        _token=_CONSTRUCTION_TOKEN, conn=conn, path=path, lease=lease, clock=clock
    )


__all__ = [
    "LEASE_DEFAULT_TTL_SECONDS",
    "LeaseInfo",
    "StorageDatabase",
    "open_database",
]
