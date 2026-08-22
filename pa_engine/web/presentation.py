"""Bounded, immutable presentation projection for the local web client.

This module is intentionally not a serialization shortcut around runtime
diagnostics.  It selects the small set of fields the browser needs and drops
provider payloads, raw trade/event envelopes, source trades and unbounded
history before the snapshot crosses the owner/HTTP boundary.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from pa_engine.runtime.config import RuntimeConfig
from pa_engine.storage.times import decimal_to_text, is_decimal_text


SCHEMA_VERSION = "g033.presentation.1.0.0"
MAX_CLOSED_BARS = 200
MAX_EVENTS = 100
MAX_DECISIONS = 50
MAX_TRADES = 200
MAX_ERRORS = 20
MAX_STRATEGIES = 100
_SECRET_PATTERN = re.compile(
    r"(?i)(?:api[_ -]?key|api[_ -]?token|client[_ -]?secret|password|credential|authorization|secret|token)\s*[:=]\s*[^\s,;]+"
)


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return copy.deepcopy(value)


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw(item) for item in value]
    return copy.deepcopy(value)


def _redact_tree(value: Any, secrets: tuple[str, ...]) -> Any:
    """Redact exact runtime-only values from every final scalar string."""

    if isinstance(value, str):
        return _safe_text(value, secrets=secrets)
    if isinstance(value, Mapping):
        return {str(key): _redact_tree(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_tree(item, secrets) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_tree(item, secrets) for item in value)
    return copy.deepcopy(value)


@dataclass(frozen=True, slots=True)
class PresentationSnapshot:
    """Immutable snapshot published by :class:`RuntimeSupervisor`."""

    _data: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "_data", _freeze(dict(self._data)))

    @property
    def lifecycle(self) -> str:
        return str(self._data.get("lifecycle", "idle"))

    def to_dict(self) -> dict[str, Any]:
        return _thaw(self._data)

    as_dict = to_dict

    def __getitem__(self, key: str) -> Any:
        return _thaw(self._data[key])


def _safe_text(value: Any, *, secrets: Iterable[str] = ()) -> str:
    text = str(value) if value is not None else ""
    for secret in secrets:
        if not isinstance(secret, str) or not secret:
            continue
        if len(secret) >= 4:
            text = text.replace(secret, "<redacted>")
        elif text == secret:
            text = "<redacted>"
    text = _SECRET_PATTERN.sub(lambda match: match.group(0).split("=", 1)[0].split(":", 1)[0] + "=<redacted>", text)
    return text[:400]


def _decimal_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and is_decimal_text(value):
        return value
    if isinstance(value, Decimal):
        try:
            return decimal_to_text(value)
        except (TypeError, ValueError):
            return None
    if isinstance(value, bool) or isinstance(value, float):
        return None
    try:
        parsed = Decimal(str(value))
        if not parsed.is_finite():
            return None
        return decimal_to_text(parsed)
    except (InvalidOperation, TypeError, ValueError):
        return None


def _instant(value: Any) -> str | None:
    return value if isinstance(value, str) and value.endswith("Z") else None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _view_value(view: Any, name: str, default: Any = None) -> Any:
    if view is None:
        return default
    try:
        if isinstance(view, Mapping):
            return view.get(name, default)
        return getattr(view, name, default)
    except Exception:
        return default


def _bar_projection(bar: Any) -> dict[str, Any] | None:
    item = _mapping(bar)
    required = ("start_at", "end_at", "open", "high", "low", "close", "volume")
    if any(not isinstance(item.get(key), str) for key in required):
        return None
    return {
        "bar_id": item.get("bar_id"),
        "start_at": item["start_at"],
        "end_at": item["end_at"],
        "open": item["open"],
        "high": item["high"],
        "low": item["low"],
        "close": item["close"],
        "volume": item["volume"],
        "is_closed": bool(item.get("is_closed", False)),
    }


def _project_candles(view: Any) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    snapshot = _mapping(_view_value(view, "closed_snapshot", {}))
    raw_bars = snapshot.get("bars")
    source_bars = raw_bars if isinstance(raw_bars, (list, tuple)) else ()
    bars = [_bar_projection(item) for item in source_bars]
    bars = [item for item in bars if item is not None and item["is_closed"]]
    bars.sort(key=lambda item: (item["start_at"], item["end_at"]))
    bars = bars[-MAX_CLOSED_BARS:]
    forming = _bar_projection(_view_value(view, "forming_candle"))
    if forming is not None:
        forming["is_closed"] = False
        forming["status"] = "forming"
    return bars, forming


def _stream_projection(feed_status: Mapping[str, Any]) -> dict[str, Any]:
    stream = _mapping(feed_status.get("stream"))
    status = _mapping(stream.get("status"))
    outcome = _mapping(status.get("outcome"))
    return {
        "state": _safe_text(status.get("state") or stream.get("state") or ("connected" if stream.get("started") else "idle")),
        "started": bool(stream.get("started")),
        "consumer_alive": bool(stream.get("consumer_alive")),
        "transport_ready": bool(stream.get("transport_ready")),
        "live_dispatch_allowed": bool(stream.get("live_dispatch_allowed")),
        "gap_open": bool(stream.get("gap_open")),
        "live_cutover_at": _instant(stream.get("live_cutover_at")),
        "outcome": {
            "kind": _safe_text(outcome.get("kind") or outcome.get("code") or "unknown"),
            "ok": bool(outcome.get("ok", True)),
            "retryable": bool(outcome.get("retryable", False)),
        },
    }


def _readiness_projection(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keep only the typed market-quality facts needed by the page."""

    bool_keys = (
        "connection_gap",
        "stale",
        "partial",
        "insufficient_history",
        "insufficient",
        "frozen_second",
        "frozen",
        "session_ready",
        "regular_session",
        "fresh",
        "complete",
        "non_gapped",
        "sufficient",
        "recovery_required",
    )
    result = {key: bool(value.get(key, False)) for key in bool_keys}
    result["recovery_generation"] = value.get("recovery_generation", 0) if isinstance(value.get("recovery_generation", 0), int) else 0
    calculated = not any(
        result[key]
        for key in ("connection_gap", "stale", "partial", "insufficient_history", "recovery_required")
    ) and result["frozen_second"] and result["session_ready"]
    result["executable"] = bool(value.get("executable", calculated))
    codes = value.get("codes", ())
    result["codes"] = [_safe_text(code)[:80] for code in codes if isinstance(code, str)][:12] if isinstance(codes, (list, tuple)) else []
    return result


def _strategy_name(record: Any, strategy_id: str) -> str:
    packet = _mapping(getattr(record, "packet", None))
    primary = _mapping(packet.get("primary_record"))
    name = primary.get("name")
    if isinstance(name, str) and name.strip():
        return name[:160]
    return strategy_id[3:] if strategy_id.startswith("pa:") else strategy_id


def _latest_reason(record: Any, state_row: Mapping[str, Any] | None) -> str | None:
    instruction = _mapping(getattr(record, "latest_instruction", None))
    if not instruction and state_row is not None:
        instruction = _mapping(state_row.get("latest_instruction"))
    reason = instruction.get("reason")
    if isinstance(reason, str) and reason.strip():
        return _safe_text(reason)[:240]
    blocked = getattr(record, "blocked_reason", None)
    if not blocked and state_row is not None:
        blocked = state_row.get("blocked_reason")
    if isinstance(blocked, str) and blocked.strip():
        return _safe_text(blocked)[:240]
    log = getattr(record, "transition_log", None)
    if isinstance(log, list) and log:
        reason = log[-1].get("reason") if isinstance(log[-1], Mapping) else None
        return _safe_text(reason)[:240] if reason else None
    return None


def _instruction_projection(record: Any, state_row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    instruction = _mapping(getattr(record, "latest_instruction", None))
    if not instruction and state_row is not None:
        instruction = _mapping(state_row.get("latest_instruction"))
    if not instruction:
        return None
    result: dict[str, Any] = {}
    for key in ("branch", "reason", "next_call_after_seconds", "entry_wait_seconds", "entry_price", "stop_price", "target_price", "accepted_at", "decision_accepted_at"):
        if key in instruction:
            value = instruction[key]
            if key in {"entry_price", "stop_price", "target_price"}:
                value = _decimal_text(value)
            elif key in {"reason"}:
                value = _safe_text(value)[:240]
            result[key] = value
    return result or None


def _account_rows(runtime: Any) -> dict[str, Mapping[str, Any]]:
    store = getattr(runtime, "_store", None)
    if store is not None:
        try:
            return {str(row["strategy_id"]): row for row in store.list_accounts() if isinstance(row, Mapping)}
        except Exception:
            pass
    book = getattr(runtime, "book", None)
    accounts = getattr(book, "accounts", {}) if book is not None else {}
    result: dict[str, Mapping[str, Any]] = {}
    if isinstance(accounts, Mapping):
        for sid, account in accounts.items():
            if isinstance(account, Mapping):
                result[str(sid)] = account
            else:
                try:
                    result[str(sid)] = account.to_dict()
                except Exception:
                    continue
    return result


def _state_rows(runtime: Any) -> dict[str, Mapping[str, Any]]:
    store = getattr(runtime, "_store", None)
    if store is not None:
        try:
            return {str(row["strategy_id"]): row for row in store.list_strategy_states() if isinstance(row, Mapping)}
        except Exception:
            pass
    return {}


def _pending_rows(runtime: Any) -> list[Mapping[str, Any]]:
    store = getattr(runtime, "_store", None)
    if store is not None:
        try:
            return [row for row in store.list_pending_entries() if isinstance(row, Mapping)]
        except Exception:
            return []
    return []


def _trade_rows(runtime: Any) -> list[Mapping[str, Any]]:
    store = getattr(runtime, "_store", None)
    if store is not None:
        try:
            return [row for row in store.list_trades() if isinstance(row, Mapping)]
        except Exception:
            return []
    return []


def _event_rows(runtime: Any) -> list[Mapping[str, Any]]:
    store = getattr(runtime, "_store", None)
    if store is not None:
        try:
            return [row for row in store.list_events() if isinstance(row, Mapping)]
        except Exception:
            return []
    return []


def _pending_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "pending_id": row.get("pending_id"),
        "strategy_id": row.get("strategy_id"),
        "status": row.get("status"),
        "symbol": row.get("symbol"),
        "direction": row.get("direction"),
        "entry_price": _decimal_text(row.get("entry_price")),
        "stop_price": _decimal_text(row.get("stop_price")),
        "target_price": _decimal_text(row.get("target_price")),
        "expires_at": _instant(row.get("expires_at")),
    }


def _trade_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    settlement = _mapping(row.get("settlement_json", row.get("settlement")))
    pnl = settlement.get("applied_pnl", settlement.get("net_pnl"))
    return {
        "trade_id": row.get("trade_id"),
        "strategy_id": row.get("strategy_id"),
        "status": row.get("status"),
        "symbol": row.get("symbol"),
        "direction": row.get("direction"),
        "quantity": _decimal_text(row.get("quantity")),
        "entry_price": _decimal_text(row.get("entry_price")),
        "stop_price": _decimal_text(row.get("stop_price")),
        "target_price": _decimal_text(row.get("target_price")),
        "opened_at": _instant(row.get("opened_at")),
        "closed_at": _instant(row.get("closed_at")),
        "exit_reason": _safe_text(row.get("exit_reason")) if row.get("exit_reason") is not None else None,
        "paper_pnl": _decimal_text(pnl),
        "gross_pnl": _decimal_text(settlement.get("gross_pnl")),
    }


def _decision_reason_by_strategy(state_rows: Mapping[str, Mapping[str, Any]], records: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for sid, row in state_rows.items():
        instruction = _mapping(row.get("latest_instruction"))
        if not instruction:
            instruction = _mapping(getattr(records.get(sid), "latest_instruction", None))
        reason = instruction.get("reason")
        if isinstance(reason, str) and reason.strip():
            result[sid] = _safe_text(reason)[:240]
    return result


def _event_projection(row: Mapping[str, Any], reasons: Mapping[str, str]) -> dict[str, Any] | None:
    event_type = row.get("event_type")
    if not isinstance(event_type, str):
        return None
    envelope = _mapping(row.get("envelope"))
    payload = _mapping(envelope.get("payload"))
    sid = row.get("strategy_id")
    item: dict[str, Any] = {
        "event_id": row.get("event_id"),
        "event_type": event_type,
        "occurred_at": _instant(row.get("occurred_at")),
        "strategy_id": sid,
    }
    if event_type == "strategy_state_changed":
        item.update({"summary": "strategy state changed", "state": payload.get("to"), "reason": _safe_text(payload.get("reason"))[:240]})
    elif event_type == "model_decision_accepted":
        item.update({"summary": "model decision accepted", "branch": payload.get("branch"), "reason": reasons.get(str(sid), "")})
    elif event_type == "schedule_set":
        item.update({"summary": "timer scheduled", "timer_kind": payload.get("timer_kind"), "due_at": _instant(payload.get("due_at"))})
    elif event_type in {"pending_entry_created", "pending_entry_expired", "pending_entry_cancelled"}:
        item.update({"summary": "pending entry " + event_type.removeprefix("pending_entry_"), "reason": _safe_text(payload.get("reason"))[:240] if payload.get("reason") else None})
    elif event_type == "paper_fill":
        item.update({"summary": "paper position opened", "direction": payload.get("direction"), "entry_price": _decimal_text(payload.get("entry_price")), "quantity": _decimal_text(payload.get("quantity"))})
    elif event_type == "paper_close":
        settlement = _mapping(payload.get("settlement"))
        item.update({"summary": "paper position closed", "exit_reason": _safe_text(payload.get("exit_reason"))[:120], "paper_pnl": _decimal_text(settlement.get("applied_pnl", settlement.get("net_pnl")))})
    elif event_type == "account_changed":
        item.update({"summary": "paper balance updated", "balance_after": _decimal_text(payload.get("balance_after"))})
    elif event_type == "account_exhausted":
        item.update({"summary": "paper account exhausted", "reason": _safe_text(payload.get("reason"))[:240]})
    elif event_type == "market_recovery":
        item.update({"summary": "market recovery recorded", "status": _safe_text(payload.get("status"))[:120]})
    elif event_type == "paper_fill_rejected":
        item.update({"summary": "paper fill rejected", "reason": _safe_text(payload.get("reason"))[:240]})
    else:
        item.update({"summary": "paper runtime event"})
    return item


_CONTROL_LIFECYCLES = frozenset({"starting", "stopping", "stopped"})
_MODEL_ERROR_TOKENS = frozenset(
    {
        "auth",
        "authentication",
        "credential",
        "forbidden",
        "provider",
        "rate",
        "too_many_requests",
        "transport",
        "network",
        "connection",
        "timeout",
        "unauthorized",
    }
)


def _is_model_error(value: Any) -> bool:
    text = str(value or "").lower()
    return any(token in text for token in _MODEL_ERROR_TOKENS)


def _status_has_model_error(status: Mapping[str, Any]) -> bool:
    for key in ("active_error", "current_error", "provider_error"):
        active = status.get(key)
        if isinstance(active, Mapping) and (_is_model_error(active.get("code")) or _is_model_error(active.get("message"))):
            return True
    errors = status.get("errors")
    if not isinstance(errors, (list, tuple)):
        return False
    typed_error = any(
        isinstance(error, Mapping)
        and (_is_model_error(error.get("code")) or _is_model_error(error.get("message")))
        for error in errors
    )
    strategies = status.get("strategies")
    if not isinstance(strategies, (list, tuple)):
        return False
    for strategy in strategies:
        if not isinstance(strategy, Mapping):
            continue
        state = str(strategy.get("state") or "").lower()
        reason = strategy.get("blocked_reason") or strategy.get("error")
        if state == "blocked" and _is_model_error(reason):
            return True
        if state == "retry_wait" and typed_error:
            return True
    return False


def _lifecycle_for(runtime: Any, requested: str, status: Mapping[str, Any], readiness: Mapping[str, Any], last_error: Mapping[str, Any] | None) -> str:
    """Project lifecycle from runtime facts, keeping only control states explicit.

    ``running`` is not a command acknowledgement.  Once a runtime exists its
    market readiness and typed status errors are authoritative, so a stale
    requested value cannot hide ``waiting_market`` or ``recovery``.
    """

    if requested in _CONTROL_LIFECYCLES:
        return requested
    if last_error is not None:
        return "model_error" if _is_model_error(last_error.get("code")) or _is_model_error(last_error.get("message")) else "blocked"
    if _status_has_model_error(status):
        return "model_error"
    if runtime is None:
        return requested or "idle"
    if bool(status.get("stopped")):
        return "stopped"
    if bool(status.get("recovery_pending")) or bool(readiness.get("recovery_required")):
        return "recovery"
    if not bool(status.get("started")):
        return "waiting_market"
    if not bool(readiness.get("executable", False)):
        return "waiting_market"
    return "running"


def build_presentation_snapshot(
    runtime: Any = None,
    *,
    lifecycle: str = "",
    public_config: Mapping[str, Any] | None = None,
    last_error: Mapping[str, Any] | None = None,
    secret_values: Iterable[str] = (),
) -> PresentationSnapshot:
    """Build one bounded snapshot without returning any mutable runtime object."""

    secrets = tuple(secret_values)
    status: Mapping[str, Any] = {}
    if runtime is not None:
        try:
            candidate = runtime.status()
            status = candidate if isinstance(candidate, Mapping) else {}
        except Exception as exc:
            status = {"errors": [{"code": "status_unavailable", "message": type(exc).__name__}]}
    config = getattr(runtime, "config", None) if runtime is not None else None
    if public_config is None and isinstance(config, RuntimeConfig):
        public_config = {
            "symbol": config.symbol,
            "timeframe": config.timeframe_mapping(),
            "provider": config.provider,
            "reasoning_mode": config.reasoning_mode,
            "openai_speed": config.openai_speed,
            "model": config.provider_model,
            "execution_policy": config.execution_policy,
        }
    public = dict(public_config or {})
    view = getattr(runtime, "_current_view", None) if runtime is not None else None
    if view is None and runtime is not None:
        feed = getattr(runtime, "feed", None)
        view = getattr(feed, "current_view", None) if feed is not None else None
    readiness = _mapping(_view_value(view, "readiness", {}))
    if hasattr(_view_value(view, "readiness", None), "to_dict"):
        try:
            readiness = _view_value(view, "readiness").to_dict()
        except Exception:
            readiness = {}
    readiness = _readiness_projection(readiness)
    closed_bars, forming = _project_candles(view)
    latest_second = _mapping(_view_value(view, "latest_second", {}))
    current_price = _view_value(view, "current_price", None) or latest_second.get("close")
    price_at = latest_second.get("end_at") if latest_second else None
    feed_status: Mapping[str, Any] = {}
    if runtime is not None and getattr(runtime, "feed", None) is not None:
        try:
            candidate = runtime.feed.status()
            feed_status = candidate if isinstance(candidate, Mapping) else {}
        except Exception as exc:
            feed_status = {"stream": {"status": {"state": "error", "outcome": {"kind": type(exc).__name__, "ok": False}}}}
    state_rows = _state_rows(runtime) if runtime is not None else {}
    account_rows = _account_rows(runtime) if runtime is not None else {}
    pending_rows = _pending_rows(runtime) if runtime is not None else []
    trade_rows = _trade_rows(runtime) if runtime is not None else []
    event_rows = _event_rows(runtime) if runtime is not None else []
    scheduler = getattr(runtime, "scheduler", None) if runtime is not None else None
    records = getattr(scheduler, "records", {}) if scheduler is not None else {}
    if not isinstance(records, Mapping):
        records = {}
    strategy_ids = sorted(set(state_rows) | set(account_rows) | set(records))
    strategies: list[dict[str, Any]] = []
    pending_by_sid: dict[str, dict[str, Any]] = {}
    for row in pending_rows:
        if row.get("status") == "pending" and isinstance(row.get("strategy_id"), str):
            pending_by_sid[str(row["strategy_id"])] = _pending_projection(row)
    open_by_sid: dict[str, dict[str, Any]] = {}
    closed_trades: list[dict[str, Any]] = []
    projected_trades: list[dict[str, Any]] = []
    for row in trade_rows:
        item = _trade_projection(row)
        projected_trades.append(item)
        if row.get("status") == "open" and isinstance(row.get("strategy_id"), str):
            open_by_sid[str(row["strategy_id"])] = item
        elif row.get("status") in {"settled", "exhausted"}:
            closed_trades.append(item)
    closed_trades.sort(key=lambda item: (item.get("closed_at") or "", item.get("trade_id") or ""))
    closed_trades = closed_trades[-MAX_TRADES:]
    for sid in strategy_ids:
        record = records.get(sid)
        state_row = state_rows.get(sid)
        account = account_rows.get(sid, {})
        state = getattr(record, "state", None)
        state = getattr(state, "value", state) or (state_row.get("state") if state_row else "analysis_due")
        latest_instruction = _instruction_projection(record, state_row)
        execution_policy = str(public.get("execution_policy") or getattr(config, "execution_policy", "paper"))
        timers: list[dict[str, Any]] = []
        for token in getattr(record, "timer_tokens", {}).values() if record is not None else ():
            if getattr(token, "active", False):
                timers.append({"kind": getattr(token, "kind", None), "due_at": _instant(getattr(token, "due_at", None).isoformat().replace("+00:00", "Z") if getattr(token, "due_at", None) is not None else None), "generation": getattr(token, "generation", None)})
        if not timers and state_row is not None:
            store = getattr(runtime, "_store", None)
            try:
                timers = [{"kind": timer.get("kind"), "due_at": _instant(timer.get("due_at")), "generation": timer.get("generation")} for timer in store.get_timers(sid) if timer.get("active")]
            except Exception:
                timers = []
        strategies.append(
            {
                "strategy_id": sid,
                "name": _strategy_name(record, sid),
                "state": str(state),
                "enabled": bool(getattr(record, "enabled", state_row.get("enabled", True) if state_row else True)),
                "exhausted": bool(getattr(record, "exhausted", state_row.get("exhausted", False) if state_row else False)),
                "balance": _decimal_text(account.get("balance")),
                "next_due_at": _instant(getattr(record, "next_due_at", None).isoformat().replace("+00:00", "Z") if getattr(record, "next_due_at", None) is not None else (state_row.get("next_due_at") if state_row else None)),
                "x_expiry_at": _instant(getattr(record, "expires_at", None).isoformat().replace("+00:00", "Z") if getattr(record, "expires_at", None) is not None else (state_row.get("expires_at") if state_row else None)),
                "last_reason": _latest_reason(record, state_row),
                "latest_decision": latest_instruction,
                "accepted_reason": _latest_reason(record, state_row),
                "gate_trace": [
                    {
                        "gate": "market_evidence",
                        "status": "passed" if readiness.get("executable") else "waiting",
                        "label": "market evidence executable" if readiness.get("executable") else "waiting for complete market evidence",
                    },
                    {
                        "gate": "setup_lineage",
                        "status": "passed",
                        "label": "source-linked setup packet bound",
                    },
                    {
                        "gate": "decision",
                        "status": "passed" if latest_instruction else "waiting",
                        "label": "validated decision accepted" if latest_instruction else "no accepted decision yet",
                    },
                    {
                        "gate": "execution",
                        "status": "bypassed" if execution_policy == "advisory" else "enabled",
                        "label": "advisory only — no simulated order" if execution_policy == "advisory" else "paper execution enabled",
                    },
                ],
                "timers": timers,
                "pending": pending_by_sid.get(sid),
                "open_trade": open_by_sid.get(sid),
            }
        )
    strategies = strategies[:MAX_STRATEGIES]
    reasons = _decision_reason_by_strategy(state_rows, records)
    events = [_event_projection(row, reasons) for row in event_rows]
    events = [item for item in events if item is not None][-MAX_EVENTS:]
    decisions = [item for item in events if item.get("event_type") == "model_decision_accepted"][-MAX_DECISIONS:]
    errors: list[dict[str, Any]] = []
    if last_error is not None:
        errors.append({"code": _safe_text(last_error.get("code")), "message": _safe_text(last_error.get("message"), secrets=secrets)})
    for raw in status.get("errors", []) if isinstance(status.get("errors"), list) else []:
        if isinstance(raw, Mapping):
            errors.append({"code": _safe_text(raw.get("code") or "runtime_error"), "message": _safe_text(raw.get("message") or "runtime error", secrets=secrets)})
    errors = errors[-MAX_ERRORS:]
    lifecycle_value = _lifecycle_for(runtime, lifecycle, status, readiness, last_error)
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "paper_only": True,
        "iex_only": True,
        "market_data_usage": "internal_prototype",
        "no_live_orders": True,
        "labels": {
            "paper_only": True,
            "paper_only_text": "PAPER TRADING ONLY — no live orders",
            "iex_only": True,
            "market_feed": "alpaca:iex:single_exchange",
            "market_feed_text": "Alpaca Basic IEX · single exchange",
            "market_data_usage": "internal_prototype",
            "no_live_orders": True,
        },
        "lifecycle": lifecycle_value,
        "run": {
            "run_id": status.get("run_id"),
            "symbol": public.get("symbol") or status.get("symbol"),
            "timeframe": public.get("timeframe") or status.get("timeframe"),
            "provider": public.get("provider"),
            "model": public.get("model"),
            "reasoning_mode": public.get("reasoning_mode"),
            "openai_speed": public.get("openai_speed"),
            "execution_policy": public.get("execution_policy") or status.get("execution_policy"),
            "selected_strategy_count": public.get("selected_strategy_count", len(strategies)),
            "provider_calls": status.get("provider_calls", 0),
            "provider_in_flight": status.get("provider_in_flight", 0),
        },
        "market": {
            "latest_price": _decimal_text(current_price),
            "latest_price_at": _instant(price_at),
            "readiness": dict(readiness),
            "stream": _stream_projection(feed_status),
            "recovery": {
                "pending": bool(status.get("recovery_pending")),
                "required": bool(readiness.get("recovery_required")),
                "generation": readiness.get("recovery_generation", 0),
            },
        },
        "bars": {
            "closed": closed_bars,
            "forming": forming,
            "latest_price": _decimal_text(current_price),
            "latest_price_at": _instant(price_at),
            "closed_count": len(closed_bars),
            "max_closed": MAX_CLOSED_BARS,
        },
        "strategies": strategies,
        "pending_entries": list(pending_by_sid.values())[:MAX_TRADES],
        "open_trades": [item for item in projected_trades if item.get("status") == "open"][:MAX_TRADES],
        "closed_trades": closed_trades,
        "recent_decisions": decisions,
        "events": events,
        "errors": errors,
        "limits": {"closed_bars": MAX_CLOSED_BARS, "events": MAX_EVENTS, "decisions": MAX_DECISIONS, "trades": MAX_TRADES},
    }
    return PresentationSnapshot(_redact_tree(snapshot, secrets))


__all__ = [
    "MAX_CLOSED_BARS",
    "MAX_DECISIONS",
    "MAX_ERRORS",
    "MAX_EVENTS",
    "MAX_STRATEGIES",
    "MAX_TRADES",
    "PresentationSnapshot",
    "SCHEMA_VERSION",
    "build_presentation_snapshot",
]
