"""Compatibility facade for the G026 stop/target monitor."""

from .reducer import (
    ExitMonitor,
    ExitReducer,
    ExitSettlementReducer,
    LevelMonitor,
    OpenPositionMonitor,
    OpenTradeMonitor,
    PositionExitMonitor,
    StopTargetMonitor,
    StopTargetReducer,
    create_exit_monitor,
    create_monitor,
    make_exit_state,
    monitor_open_position,
    reduce,
    reduce_exit,
    settle_exit,
)

__all__ = [
    "ExitMonitor",
    "ExitReducer",
    "ExitSettlementReducer",
    "LevelMonitor",
    "OpenPositionMonitor",
    "OpenTradeMonitor",
    "PositionExitMonitor",
    "StopTargetMonitor",
    "StopTargetReducer",
    "create_exit_monitor",
    "create_monitor",
    "make_exit_state",
    "monitor_open_position",
    "reduce",
    "reduce_exit",
    "settle_exit",
]
