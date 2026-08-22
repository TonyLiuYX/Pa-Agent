"""Typed storage-layer errors for the G027 SQLite runtime persistence.

Every failure raised by :mod:`pa_engine.storage` carries a stable machine
``code`` so callers (and tests) can branch without string matching.  The
vocabulary is deliberately small and closed for ``g027-storage 0.1.0``:

- ``storage_closed`` — operation attempted on a closed storage handle.
- ``corruption`` — on-disk bytes failed integrity or invariant checks.
- ``incompatible_version`` — schema/application identity is not this version.
- ``lease_conflict`` — a live active-run lease is held by another run.
- ``envelope_validation`` — a runtime event envelope failed validation.
- ``secret_leak`` — credential/reasoning/broker-secret content was presented
  for durable storage.
- ``unique_conflict`` — an event/idempotency key already exists with
  different content.
- ``recovery_state`` — restart/recovery state is inconsistent or incomplete.
- ``invalid_input`` — caller supplied malformed state outside those classes.

No error message may contain credential material; messages are short and
refer to field paths only.
"""

from __future__ import annotations

from typing import Any


class StorageError(Exception):
    """Base class for all G027 storage failures."""

    code = "storage_error"

    def __init__(self, message: str | None = None, *, path: str | None = None) -> None:
        detail = message or self.code
        if path:
            detail = f"{detail} at {path}"
        super().__init__(detail)
        self.path = path

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "path": self.path}


class StorageClosedError(StorageError):
    """The storage handle has already been closed."""

    code = "storage_closed"


class StorageCorruptionError(StorageError):
    """On-disk state failed integrity/invariant checks; never auto-reset."""

    code = "corruption"


class StorageVersionError(StorageError):
    """The database schema/application identity is incompatible."""

    code = "incompatible_version"


class LeaseConflictError(StorageError):
    """A live active-run lease for the database is held by another run."""

    code = "lease_conflict"

    def __init__(self, message: str = "active runtime lease held by another run", *, holder: str | None = None) -> None:
        super().__init__(message)
        self.holder = holder


class EnvelopeValidationError(StorageError):
    """A RuntimeEventEnvelope failed structural, lineage or market-ref checks."""

    code = "envelope_validation"


class SecretLeakError(StorageError):
    """Credential, reasoning or broker-secret content was offered for storage."""

    code = "secret_leak"


class StorageConflictError(StorageError):
    """An idempotency/unique key exists with different content."""

    code = "unique_conflict"


class RecoveryStateError(StorageError):
    """Persisted restart/recovery state is inconsistent."""

    code = "recovery_state"


class InvalidStorageInputError(StorageError):
    """Caller supplied malformed materialized state."""

    code = "invalid_input"


__all__ = [
    "EnvelopeValidationError",
    "InvalidStorageInputError",
    "LeaseConflictError",
    "RecoveryStateError",
    "SecretLeakError",
    "StorageClosedError",
    "StorageConflictError",
    "StorageCorruptionError",
    "StorageError",
    "StorageVersionError",
]
