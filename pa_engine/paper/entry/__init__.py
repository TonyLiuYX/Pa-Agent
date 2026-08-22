"""G025 price-trigger pending-entry monitor.

The public surface is intentionally small: create a pending state from a
validated G018 ``wait_for_price`` scheduler instruction, feed frozen G020
seconds (and optional G019 recovery bars/gaps), and consume typed immutable
transitions.  Wire G024/G023 through injected callbacks or pass their concrete
objects; this package itself performs no persistence, model calls, broker
operations or network I/O.
"""

from .boundary import (
    RESOLUTION_GRACE_SECONDS,
    canonical_timestamp,
    content_hash,
    event_mapping,
    exact_decimal,
    feed_from_instruction,
    make_pending_state,
    recovery_mapping,
    symbol_from_instruction,
    utc_instant,
    validate_wait_instruction,
)
from .models import (
    EntryEvent,
    EntryLineage,
    EntryOutcome,
    PendingEntryOutcome,
    PendingEntryState,
    PendingEntryTransition,
)
from .reducer import PendingEntryMonitor, PendingEntryReducer, create_monitor, create_pending_entry, monitor_pending_entry, reduce_pending, reduce_pending_entry
from .scheduler_bridge import PendingEntrySchedulerBridge, SchedulerBindingError

PendingEntry = PendingEntryState
PendingEntryResult = PendingEntryTransition
PendingEntryEvent = EntryEvent
TouchOutcome = PendingEntryTransition
EntryMonitor = PendingEntryMonitor
PriceTriggerMonitor = PendingEntryMonitor
PendingEntryController = PendingEntryMonitor
create_pending = create_pending_entry
reduce = reduce_pending_entry

__all__ = [
    "EntryEvent",
    "EntryLineage",
    "EntryMonitor",
    "EntryOutcome",
    "PendingEntry",
    "PendingEntryEvent",
    "PendingEntryMonitor",
    "PendingEntryController",
    "PendingEntryOutcome",
    "PendingEntryReducer",
    "PendingEntryResult",
    "PendingEntryState",
    "PendingEntryTransition",
    "PendingEntrySchedulerBridge",
    "SchedulerBindingError",
    "RESOLUTION_GRACE_SECONDS",
    "TouchOutcome",
    "PriceTriggerMonitor",
    "canonical_timestamp",
    "content_hash",
    "create_pending_entry",
    "create_pending",
    "create_monitor",
    "event_mapping",
    "exact_decimal",
    "feed_from_instruction",
    "make_pending_state",
    "recovery_mapping",
    "reduce_pending_entry",
    "reduce",
    "reduce_pending",
    "monitor_pending_entry",
    "symbol_from_instruction",
    "utc_instant",
    "validate_wait_instruction",
]
