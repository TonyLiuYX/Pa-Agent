"""Compatibility import surface for immutable G025 state values."""

from .models import EntryEvent, EntryLineage, EntryOutcome, PendingEntryOutcome, PendingEntryState, PendingEntryTransition

PendingEntry = PendingEntryState
PendingEntryResult = PendingEntryTransition

__all__ = [
    "EntryEvent",
    "EntryLineage",
    "EntryOutcome",
    "PendingEntryOutcome",
    "PendingEntryState",
    "PendingEntry",
    "PendingEntryResult",
    "PendingEntryTransition",
]
