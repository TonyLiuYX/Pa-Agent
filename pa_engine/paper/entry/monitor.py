"""Compatibility import surface for the G025 monitor."""

from .reducer import PendingEntryMonitor, PendingEntryReducer, create_monitor, create_pending_entry, monitor_pending_entry, reduce_pending_entry

EntryMonitor = PendingEntryMonitor
PriceTriggerMonitor = PendingEntryMonitor

__all__ = [
    "PendingEntryMonitor",
    "PendingEntryReducer",
    "EntryMonitor",
    "PriceTriggerMonitor",
    "create_monitor",
    "create_pending_entry",
    "monitor_pending_entry",
    "reduce_pending_entry",
]
