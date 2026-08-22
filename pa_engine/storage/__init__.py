"""G027 SQLite runtime persistence: single configured database, active-run lease,
versioned runtime event envelopes, repository layer and restart recovery.

This package owns the durable runtime state of the automatic paper-trading
engine: one account per strategy, strategy/timer snapshots, pending entries,
trades, the append-only runtime event stream and the market recovery cursor.
All public boundaries are typed and content-bound; failures are typed storage
errors and never reset existing state.
"""

from __future__ import annotations

from .database import (
    LEASE_DEFAULT_TTL_SECONDS,
    LeaseInfo,
    StorageDatabase,
    open_database,
)
from .envelope import (
    ENVELOPE_ARTIFACT_KIND,
    ENVELOPE_SCHEMA_VERSION,
    RuntimeEventEnvelope,
    validate_dict,
)
from .errors import (
    EnvelopeValidationError,
    InvalidStorageInputError,
    LeaseConflictError,
    RecoveryStateError,
    SecretLeakError,
    StorageClosedError,
    StorageConflictError,
    StorageCorruptionError,
    StorageError,
    StorageVersionError,
)
from .recovery import RETRY_DELAYS, DueWork, RestoreSnapshot, RuntimeRecovery
from .refs import (
    is_recovery_bar_ref,
    is_second_ref,
    make_recovery_bar_ref,
    make_second_ref,
    validate_market_ref,
)
from .store import (
    RuntimeStore,
    account_change_key,
    account_exhausted_key,
    close_key,
    decision_key,
    envelope_for_account_change,
    envelope_for_account_exhausted,
    envelope_for_close,
    envelope_for_decision,
    envelope_for_fill,
    envelope_for_fill_rejected,
    envelope_for_market_recovery,
    envelope_for_pending_entry,
    envelope_for_schedule,
    envelope_for_state_transition,
    fill_key,
    fill_rejected_key,
    pending_key,
    recovery_key,
    schedule_key,
    state_transition_key,
)
from .times import (
    decimal_from_text,
    decimal_to_text,
    instant_from_text,
    instant_to_text,
    is_canonical_instant,
    is_decimal_text,
)

__all__ = [
    "DueWork",
    "ENVELOPE_ARTIFACT_KIND",
    "ENVELOPE_SCHEMA_VERSION",
    "EnvelopeValidationError",
    "InvalidStorageInputError",
    "LEASE_DEFAULT_TTL_SECONDS",
    "LeaseConflictError",
    "LeaseInfo",
    "RETRY_DELAYS",
    "RecoveryStateError",
    "RestoreSnapshot",
    "RuntimeEventEnvelope",
    "RuntimeRecovery",
    "RuntimeStore",
    "SecretLeakError",
    "StorageClosedError",
    "StorageConflictError",
    "StorageCorruptionError",
    "StorageDatabase",
    "StorageError",
    "StorageVersionError",
    "account_change_key",
    "account_exhausted_key",
    "close_key",
    "decimal_from_text",
    "decimal_to_text",
    "decision_key",
    "envelope_for_account_change",
    "envelope_for_account_exhausted",
    "envelope_for_close",
    "envelope_for_decision",
    "envelope_for_fill",
    "envelope_for_fill_rejected",
    "envelope_for_market_recovery",
    "envelope_for_pending_entry",
    "envelope_for_schedule",
    "envelope_for_state_transition",
    "fill_key",
    "fill_rejected_key",
    "instant_from_text",
    "instant_to_text",
    "is_canonical_instant",
    "is_decimal_text",
    "is_recovery_bar_ref",
    "is_second_ref",
    "make_recovery_bar_ref",
    "make_second_ref",
    "open_database",
    "pending_key",
    "recovery_key",
    "schedule_key",
    "state_transition_key",
    "validate_dict",
    "validate_market_ref",
]
