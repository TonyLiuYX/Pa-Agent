"""G026 deterministic stop/target monitoring and paper settlement.

The package consumes execution-frozen G020 seconds and complete G019 recovery
bars, calls the G024 close boundary exactly once per selected exit, and then
notifies the injected G023 scheduler.  It owns no model, persistence, broker,
clock or network implementation.
"""

from .boundary import (
    ALPACA_IEX_FEED,
    PATH_PRECISION,
    RESOLUTION_GRACE_SECONDS,
    canonical_timestamp,
    content_hash,
    event_mapping,
    exact_decimal,
    fill_interval,
    fill_timestamp,
    freeze,
    identity_hash,
    recovery_mapping,
    thaw,
    trade_mapping,
    utc_instant,
)
from .models import (
    ExitEvent,
    ExitLineage,
    ExitMonitorState,
    ExitOutcome,
    ExitResult,
    ExitState,
    ExitTransition,
    LevelExitState,
    OpenPositionState,
    PositionExitOutcome,
    PositionExitState,
    PositionExitTransition,
    SchedulerCloseNotification,
)
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
from .scheduler_bridge import CloseSchedulerBridge, ExitSchedulerBridge, PositionCloseSchedulerBridge, SchedulerBindingError

ExitPositionMonitor = ExitMonitor
PositionMonitor = ExitMonitor
StopLossTakeProfitMonitor = ExitMonitor
PaperExitMonitor = ExitMonitor
OpenTradeExitMonitor = ExitMonitor
ExitResultType = ExitTransition
PositionExitResult = ExitTransition
ExitStateSnapshot = ExitState

__all__ = [
    "ALPACA_IEX_FEED",
    "PATH_PRECISION",
    "RESOLUTION_GRACE_SECONDS",
    "ExitEvent",
    "ExitLineage",
    "ExitMonitorState",
    "ExitOutcome",
    "ExitMonitor",
    "ExitPositionMonitor",
    "ExitReducer",
    "ExitSettlementReducer",
    "ExitResult",
    "ExitResultType",
    "ExitSchedulerBridge",
    "ExitState",
    "ExitStateSnapshot",
    "ExitTransition",
    "LevelExitState",
    "OpenPositionState",
    "LevelMonitor",
    "OpenPositionMonitor",
    "OpenTradeMonitor",
    "OpenTradeExitMonitor",
    "PaperExitMonitor",
    "PositionCloseSchedulerBridge",
    "PositionExitMonitor",
    "PositionExitOutcome",
    "PositionExitResult",
    "PositionExitState",
    "PositionExitTransition",
    "PositionMonitor",
    "SchedulerBindingError",
    "SchedulerCloseNotification",
    "StopLossTakeProfitMonitor",
    "StopTargetMonitor",
    "StopTargetReducer",
    "CloseSchedulerBridge",
    "canonical_timestamp",
    "content_hash",
    "create_exit_monitor",
    "create_monitor",
    "event_mapping",
    "exact_decimal",
    "fill_interval",
    "fill_timestamp",
    "freeze",
    "identity_hash",
    "make_exit_state",
    "monitor_open_position",
    "recovery_mapping",
    "reduce",
    "reduce_exit",
    "settle_exit",
    "thaw",
    "trade_mapping",
    "utc_instant",
]
