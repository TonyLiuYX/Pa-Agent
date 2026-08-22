"""In-memory independent paper accounts and one-position transitions."""

from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .boundary import assert_model_payload_safe
from .errors import InvalidPaperInput
from .math import (
    INITIAL_BALANCE,
    ONE,
    PAPER_MATH_VERSION,
    ZERO,
    ZERO_FEES,
    ZERO_SLIPPAGE,
    calculate_settlement,
    calculate_trade,
    decimal_input,
    positive_decimal,
    stable_hash,
    validate_direction,
    validate_levels,
)
from .models import AccountSnapshot, PaperEvent, PaperTrade, PendingTrade, TransitionResult


def _require_strategy_id(strategy_id: Any) -> str:
    if type(strategy_id) is not str or not strategy_id.strip():
        raise InvalidPaperInput("strategy_id must be a non-empty string", code="invalid_strategy_id", path="$.strategy_id")
    return strategy_id


def _require_symbol(symbol: Any) -> str:
    if type(symbol) is not str or not symbol.strip():
        raise InvalidPaperInput("symbol must be a non-empty string", code="invalid_symbol", path="$.symbol")
    return symbol


def _require_optional_id(value: Any, field: str, code: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str or not value.strip():
        raise InvalidPaperInput(f"{field} must be a non-empty string", code=code, path=f"$.{field}")
    return value


def _key_or_auto(key: Any, request_hash: str) -> str:
    if key is None:
        return "idem_" + request_hash
    if type(key) is not str or not key.strip():
        raise InvalidPaperInput("idempotency_key must be a non-empty string", code="invalid_idempotency_key", path="$.idempotency_key")
    return key


def _identity_decimal(value: Any, field: str) -> Any:
    """Normalize exact decimal-like price inputs for semantic request hashes."""

    if value is None:
        return None
    try:
        return decimal_input(value, field)
    except Exception:
        # The actual operation still performs the typed validation.  Retaining
        # malformed input here lets an invalid request receive a stable key
        # rather than failing before its rejection result can be remembered.
        try:
            representation = repr(value)
        except Exception:
            representation = "<unrepresentable>"
        return {"__invalid_type__": type(value).__name__, "__invalid_repr__": representation}


def _price_reference(value: Any, field: str) -> tuple[Any, Mapping[str, Any] | None]:
    """Extract a price from a compact current-event reference mapping."""

    if isinstance(value, Mapping):
        price = value.get("reference_price", value.get("price"))
        if price is None:
            raise InvalidPaperInput(f"{field} reference is missing a price", code="missing_fill_reference", path=f"$.{field}")
        return price, copy.deepcopy(dict(value))
    return value, None


def _identity_value(value: Any, field: str = "value") -> Any:
    """Canonical request projection preserving nested event identity metadata."""

    if isinstance(value, Mapping):
        return {str(key): _identity_value(child, f"{field}.{key}") for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_identity_value(child, f"{field}[{index}]") for index, child in enumerate(value)]
    if value is None or type(value) in {bool, int, str}:
        return value
    return _identity_decimal(value, field)


@dataclass
class _ManagedAccount:
    strategy_id: str
    balance: Decimal = INITIAL_BALANCE
    open_trade: PaperTrade | None = None
    pending_trade: PendingTrade | None = None
    pending: bool = False
    exhausted: bool = False
    initialized_event: PaperEvent | None = None

    def snapshot(self) -> AccountSnapshot:
        return AccountSnapshot(
            strategy_id=self.strategy_id,
            balance=self.balance,
            initial_balance=INITIAL_BALANCE,
            exhausted=self.exhausted or self.balance == ZERO,
            open_trade=copy.deepcopy(self.open_trade),
            pending_trade=copy.deepcopy(self.pending_trade),
            pending=self.pending or self.pending_trade is not None,
            paper_math_version=PAPER_MATH_VERSION,
        )


class PaperAccount:
    """Small mutable account object useful when a caller owns one strategy.

    ``PaperAccountBook`` is normally more convenient for several strategies;
    this object exposes the same read-only snapshot shape without persistence.
    """

    __slots__ = ("_strategy_id", "_balance", "_open_trade", "_pending_trade", "_pending", "_delegate_book")

    def __init__(self, strategy_id: str) -> None:
        self._strategy_id = _require_strategy_id(strategy_id)
        # Public construction is intentionally not a hydration seam.  G027
        # owns restore from durable state; a fresh account has no balance
        # argument and always starts at the fixed v0.1 USD 1,000.00.
        self._balance = INITIAL_BALANCE
        self._open_trade: PaperTrade | None = None
        self._pending_trade: PendingTrade | None = None
        self._pending = False
        self._delegate_book: PaperAccountBook | None = None

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    @property
    def balance(self) -> Decimal:
        return self._balance

    @property
    def exhausted(self) -> bool:
        return self.balance == ZERO

    @property
    def available_balance(self) -> Decimal:
        return self.balance

    @property
    def account_id(self) -> str:
        return f"paper:{self.strategy_id}"

    def snapshot(self) -> AccountSnapshot:
        return AccountSnapshot(
            strategy_id=self.strategy_id,
            balance=self.balance,
            initial_balance=INITIAL_BALANCE,
            exhausted=self.exhausted,
            open_trade=copy.deepcopy(self._open_trade),
            pending_trade=copy.deepcopy(self._pending_trade),
            pending=self._pending,
        )

    @property
    def pending_trade(self) -> PendingTrade | None:
        return copy.deepcopy(self._pending_trade)

    @property
    def pending(self) -> bool:
        return self._pending

    def _book(self) -> "PaperAccountBook":
        if self._delegate_book is None:
            book = PaperAccountBook()
            book._accounts[self.strategy_id] = _ManagedAccount(
                self.strategy_id,
                balance=self._balance,
                open_trade=copy.deepcopy(self._open_trade),
                pending_trade=copy.deepcopy(self._pending_trade),
                pending=self._pending,
            )
            self._delegate_book = book
        return self._delegate_book

    def _sync(self) -> None:
        managed = self._book()._accounts[self.strategy_id]
        self._balance = managed.balance
        self._open_trade = copy.deepcopy(managed.open_trade)
        self._pending_trade = copy.deepcopy(managed.pending_trade)
        self._pending = managed.pending or managed.pending_trade is not None

    def open_trade(self, symbol: str, direction: str, entry_price: Any | None = None, stop_price: Any = None, target_price: Any = None, **kwargs: Any) -> TransitionResult:
        result = self._book().open_trade(self.strategy_id, symbol, direction, entry_price, stop_price, target_price, **kwargs)
        self._sync()
        return result

    @property
    def position(self) -> PaperTrade | None:
        return copy.deepcopy(self._open_trade)

    @property
    def has_open_position(self) -> bool:
        return self._open_trade is not None

    def close_trade(self, exit_price: Any = None, *positional: Any, **kwargs: Any) -> TransitionResult:
        result = self._book().close_trade(self.strategy_id, exit_price, *positional, **kwargs)
        self._sync()
        return result

    def create_pending_entry(self, symbol: str, direction: str, entry_price: Any, stop_price: Any, target_price: Any, **kwargs: Any) -> TransitionResult:
        result = self._book().create_pending_entry(self.strategy_id, symbol, direction, entry_price, stop_price, target_price, **kwargs)
        self._sync()
        return result

    def fill_pending_entry(self, pending_id: str | None = None, fill_reference: Any | None = None, **kwargs: Any) -> TransitionResult:
        result = self._book().fill_pending_entry(self.strategy_id, pending_id, fill_reference, **kwargs)
        self._sync()
        return result

    def expire_pending_entry(self, pending_id: str | None = None, **kwargs: Any) -> TransitionResult:
        result = self._book().expire_pending_entry(self.strategy_id, pending_id, **kwargs)
        self._sync()
        return result


class PaperAccountBook:
    """Independent strategy-scoped account manager.

    The book is intentionally in-memory.  G027 owns any durable store and can
    persist the immutable snapshots/events returned here.
    """

    def __init__(self, strategy_ids: Iterable[str] | None = None) -> None:
        self._accounts: dict[str, _ManagedAccount] = {}
        self._idempotency: dict[str, tuple[str, TransitionResult]] = {}
        self._events: list[PaperEvent] = []
        self._history: dict[str, list[PaperTrade]] = {}
        if strategy_ids is not None:
            for strategy_id in strategy_ids:
                self.ensure_account(strategy_id)

    @property
    def events(self) -> tuple[PaperEvent, ...]:
        return tuple(copy.deepcopy(self._events))

    @property
    def accounts(self) -> Mapping[str, AccountSnapshot]:
        return {key: value.snapshot() for key, value in self._accounts.items()}

    @property
    def idempotency(self) -> Mapping[str, str]:
        return {key: digest for key, (digest, _) in self._idempotency.items()}

    def _account(self, strategy_id: str) -> _ManagedAccount | None:
        return self._accounts.get(_require_strategy_id(strategy_id))

    def get_account(self, strategy_id: str) -> AccountSnapshot | None:
        account = self._account(strategy_id)
        return account.snapshot() if account is not None else None

    def ensure_account(self, strategy_id: str, *, idempotency_key: str | None = None) -> AccountSnapshot:
        """Create USD 1,000 once; never reset an existing account."""

        sid = _require_strategy_id(strategy_id)
        existing = self._accounts.get(sid)
        if existing is not None:
            return existing.snapshot()
        account = _ManagedAccount(sid)
        payload = {"paper_math_version": PAPER_MATH_VERSION, "operation": "initialize_account", "strategy_id": sid, "initial_balance": INITIAL_BALANCE}
        event = PaperEvent.make("account_initialized", sid, payload)
        account.initialized_event = event
        self._accounts[sid] = account
        self._events.append(event)
        return account.snapshot()

    def initialize_account(self, strategy_id: str, *, idempotency_key: str | None = None) -> TransitionResult:
        """Return a typed one-time initialization transition."""

        sid = _require_strategy_id(strategy_id)
        request_hash = stable_hash({"paper_math_version": PAPER_MATH_VERSION, "operation": "initialize_account", "strategy_id": sid})
        key = _key_or_auto(idempotency_key, request_hash)
        replay = self._replay_or_conflict(key, request_hash, sid, "initialize_account")
        if replay is not None:
            return replay
        existing = self._accounts.get(sid)
        if existing is not None:
            result = TransitionResult(
                status="no_op",
                strategy_id=sid,
                operation="initialize_account",
                changed=False,
                accepted=True,
                idempotency_key=key,
                request_hash=request_hash,
                account=existing.snapshot(),
                reason="account already initialized; balance was not reset",
                code="already_initialized",
            )
            return self._remember(key, request_hash, result)
        self.ensure_account(sid)
        account = self._accounts[sid]
        result = TransitionResult(
            status="accepted",
            strategy_id=sid,
            operation="initialize_account",
            changed=True,
            accepted=True,
            idempotency_key=key,
            request_hash=request_hash,
            event=account.initialized_event,
            account=account.snapshot(),
        )
        return self._remember(key, request_hash, result)

    def _replay_or_conflict(self, key: str, request_hash: str, strategy_id: str, operation: str) -> TransitionResult | None:
        prior = self._idempotency.get(key)
        if prior is None:
            return None
        prior_hash, prior_result = prior
        account = self.get_account(strategy_id)
        if prior_hash != request_hash:
            return TransitionResult(
                status="conflict",
                strategy_id=strategy_id,
                operation=operation,
                changed=False,
                conflict=True,
                idempotency_key=key,
                request_hash=request_hash,
                account=account,
                reason="idempotency key was already used for a different request",
                code="idempotency_conflict",
                original_status=prior_result.status,
            )
        return replace(
            copy.deepcopy(prior_result),
            status="replay",
            changed=False,
            accepted=prior_result.accepted,
            replayed=True,
            conflict=False,
            idempotency_key=key,
            request_hash=request_hash,
            account=prior_result.account or account,
            reason="idempotent replay; state was not changed",
            code="idempotent_replay",
            original_status=prior_result.status,
        )

    def _remember(self, key: str, request_hash: str, result: TransitionResult) -> TransitionResult:
        self._idempotency[key] = (request_hash, copy.deepcopy(result))
        return result

    def _rejection(
        self,
        *,
        strategy_id: str,
        operation: str,
        key: str,
        request_hash: str,
        reason: str,
        code: str,
        account: _ManagedAccount | None,
        fresh_price_rejected: bool = False,
        untradeable: bool = False,
        exhausted: bool = False,
        event_type: str | None = None,
        event_payload: Mapping[str, Any] | None = None,
    ) -> TransitionResult:
        event = None
        if event_type is not None:
            event = PaperEvent.make(event_type, strategy_id, event_payload or {"reason": reason, "code": code})
            self._events.append(event)
        result = TransitionResult(
            status="exhausted" if exhausted else "rejected",
            strategy_id=strategy_id,
            operation=operation,
            changed=False,
            accepted=False,
            idempotency_key=key,
            request_hash=request_hash,
            event=event,
            account=account.snapshot() if account is not None else None,
            reason=reason,
            code=code,
            fresh_price_rejected=fresh_price_rejected,
            untradeable=untradeable,
            exhausted=exhausted,
        )
        return self._remember(key, request_hash, result)

    def create_pending_entry(
        self,
        strategy_id: str,
        symbol: str,
        direction: str,
        entry_price: Any,
        stop_price: Any,
        target_price: Any,
        *,
        idempotency_key: str | None = None,
        pending_id: str | None = None,
    ) -> TransitionResult:
        """Register a conditional entry without reserving or changing funds."""

        sid = _require_strategy_id(strategy_id)
        ticker = _require_symbol(symbol)
        request = {
            "paper_math_version": PAPER_MATH_VERSION,
            "operation": "create_pending_entry",
            "strategy_id": sid,
            "symbol": ticker,
            "direction": direction,
            "entry_price": _identity_decimal(entry_price, "entry_price"),
            "stop_price": _identity_decimal(stop_price, "stop_price"),
            "target_price": _identity_decimal(target_price, "target_price"),
            "pending_id": _identity_value(pending_id, "pending_id"),
        }
        request_hash = stable_hash(request)
        key = _key_or_auto(idempotency_key, request_hash)
        replay = self._replay_or_conflict(key, request_hash, sid, "create_pending_entry")
        if replay is not None:
            return replay
        account = self._accounts.get(sid)
        try:
            supplied_pending_id = _require_optional_id(pending_id, "pending_id", "invalid_pending_id")
            side, entry, stop, target = validate_levels(direction, entry_price, stop_price, target_price)
            calculation = calculate_trade(side, account.balance if account is not None else INITIAL_BALANCE, entry, stop, target)
        except Exception as exc:
            return self._rejection(strategy_id=sid, operation="create_pending_entry", key=key, request_hash=request_hash, reason=getattr(exc, "message", "conditional entry is invalid"), code=getattr(exc, "code", "invalid_entry"), account=account)
        if account is None:
            self.ensure_account(sid)
            account = self._accounts[sid]
        if account.open_trade is not None or account.pending_trade is not None or account.pending:
            return self._rejection(strategy_id=sid, operation="create_pending_entry", key=key, request_hash=request_hash, reason="strategy already has a pending or open trade", code="position_exists", account=account)
        if account.balance <= ZERO or account.exhausted or not calculation["executable"]:
            account.exhausted = True
            return self._rejection(strategy_id=sid, operation="create_pending_entry", key=key, request_hash=request_hash, reason="available capital is exhausted or below one share quantum", code="untradeable_capital", account=account, exhausted=True, untradeable=True)
        projection = {
            "paper_math_version": PAPER_MATH_VERSION,
            "operation": "create_pending_entry",
            "strategy_id": sid,
            "symbol": ticker,
            "direction": side,
            "entry_price": entry,
            "stop_price": stop,
            "target_price": target,
            "planned_r": calculation["planned_r"],
        }
        digest = stable_hash(projection)
        actual_pending_id = supplied_pending_id or "ppd_" + digest
        pending = PendingTrade(sid, actual_pending_id, ticker, side, entry, stop, target, calculation["planned_r"], entry)
        account.pending_trade = pending
        account.pending = True
        event = PaperEvent.make("paper_entry_pending", sid, {"pending_trade": pending.to_dict()})
        self._events.append(event)
        result = TransitionResult(
            status="pending",
            strategy_id=sid,
            operation="create_pending_entry",
            changed=True,
            accepted=True,
            idempotency_key=key,
            request_hash=request_hash,
            event=event,
            account=account.snapshot(),
            pending_trade=copy.deepcopy(pending),
            pending_id=actual_pending_id,
            calculation=copy.deepcopy(calculation),
        )
        return self._remember(key, request_hash, result)

    def fill_pending_entry(
        self,
        strategy_id: str,
        pending_id: str | None = None,
        fill_reference: Any | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> TransitionResult:
        """Fill a previously validated pending entry at its exact requested level."""

        sid = _require_strategy_id(strategy_id)
        request_hash = stable_hash(
            {
                "operation": "fill_pending_entry",
                "strategy_id": sid,
                "pending_id": _identity_value(pending_id, "pending_id"),
                "fill_reference": _identity_value(fill_reference, "fill_reference"),
            }
        )
        key = _key_or_auto(idempotency_key, request_hash)
        replay = self._replay_or_conflict(key, request_hash, sid, "fill_pending_entry")
        if replay is not None:
            return replay
        account = self._accounts.get(sid)
        try:
            supplied_pending_id = _require_optional_id(pending_id, "pending_id", "invalid_pending_id")
        except Exception as exc:
            return self._rejection(
                strategy_id=sid,
                operation="fill_pending_entry",
                key=key,
                request_hash=request_hash,
                reason=getattr(exc, "message", "pending_id is invalid"),
                code=getattr(exc, "code", "invalid_pending_id"),
                account=account,
            )
        pending = account.pending_trade if account is not None else None
        if pending is None:
            return self._rejection(strategy_id=sid, operation="fill_pending_entry", key=key, request_hash=request_hash, reason="pending entry does not exist", code="no_pending_entry", account=account)
        if supplied_pending_id is not None and supplied_pending_id != pending.pending_id:
            return self._rejection(strategy_id=sid, operation="fill_pending_entry", key=key, request_hash=request_hash, reason="pending identity does not match", code="pending_mismatch", account=account)
        try:
            fill_value, _fill_metadata = _price_reference(fill_reference, "fill_reference") if fill_reference is not None else (pending.entry_price, None)
            actual = positive_decimal(fill_value, "fill_reference")
        except Exception as exc:
            return self._rejection(
                strategy_id=sid,
                operation="fill_pending_entry",
                key=key,
                request_hash=request_hash,
                reason=getattr(exc, "message", "fill reference is invalid"),
                code=getattr(exc, "code", "invalid_fill_reference"),
                account=account,
            )
        if actual != pending.entry_price:
            return self._rejection(strategy_id=sid, operation="fill_pending_entry", key=key, request_hash=request_hash, reason="conditional fills must use the requested exact entry level", code="fill_price_mismatch", account=account)
        account.pending_trade = None
        account.pending = False
        child_key = key + ":open"
        result = self.open_trade(sid, pending.symbol, pending.direction, actual, pending.stop_price, pending.target_price, idempotency_key=child_key, immediate=False, fill_reference=actual, requested_entry_price=pending.entry_price)
        if not result.accepted:
            account.pending_trade = pending
            account.pending = True
        return self._remember(key, request_hash, replace(result, idempotency_key=key, request_hash=request_hash, operation="fill_pending_entry", pending_trade=copy.deepcopy(pending), pending_id=pending.pending_id))

    def expire_pending_entry(self, strategy_id: str, pending_id: str | None = None, *, idempotency_key: str | None = None, reason: str | None = None) -> TransitionResult:
        sid = _require_strategy_id(strategy_id)
        request_hash = stable_hash({"operation": "expire_pending_entry", "strategy_id": sid, "pending_id": _identity_value(pending_id, "pending_id"), "reason": _identity_value(reason, "reason")})
        key = _key_or_auto(idempotency_key, request_hash)
        replay = self._replay_or_conflict(key, request_hash, sid, "expire_pending_entry")
        if replay is not None:
            return replay
        account = self._accounts.get(sid)
        try:
            supplied_pending_id = _require_optional_id(pending_id, "pending_id", "invalid_pending_id")
        except Exception as exc:
            return self._rejection(strategy_id=sid, operation="expire_pending_entry", key=key, request_hash=request_hash, reason=getattr(exc, "message", "pending_id is invalid"), code=getattr(exc, "code", "invalid_pending_id"), account=account)
        pending = account.pending_trade if account is not None else None
        if pending is None:
            return self._rejection(strategy_id=sid, operation="expire_pending_entry", key=key, request_hash=request_hash, reason="pending entry does not exist", code="no_pending_entry", account=account)
        if supplied_pending_id is not None and supplied_pending_id != pending.pending_id:
            return self._rejection(strategy_id=sid, operation="expire_pending_entry", key=key, request_hash=request_hash, reason="pending identity does not match", code="pending_mismatch", account=account)
        account.pending_trade = None
        account.pending = False
        event = PaperEvent.make("paper_entry_expired", sid, {"pending_trade": pending.to_dict(), "reason": _identity_value(reason, "reason")})
        self._events.append(event)
        result = TransitionResult(status="expired", strategy_id=sid, operation="expire_pending_entry", changed=True, accepted=True, idempotency_key=key, request_hash=request_hash, event=event, account=account.snapshot(), pending_trade=copy.deepcopy(pending), pending_id=pending.pending_id, reason=reason)
        return self._remember(key, request_hash, result)

    def open_trade(
        self,
        strategy_id: str,
        symbol: str,
        direction: str,
        entry_price: Any | None = None,
        stop_price: Any = None,
        target_price: Any = None,
        *,
        idempotency_key: str | None = None,
        immediate: bool = False,
        fill_reference: Any | None = None,
        requested_entry_price: Any | None = None,
        trade_id: str | None = None,
        model_payload: Any | None = None,
        fees: Any = ZERO,
        slippage: Any = ZERO,
    ) -> TransitionResult:
        """Open one exact-level whole-position trade.

        Immediate calls use ``fill_reference`` as the actual entry.  The
        directional ordering is checked against that post-response reference,
        so a moved current price rejects without changing the account.
        """

        sid = _require_strategy_id(strategy_id)
        ticker = _require_symbol(symbol)
        operation = "open_immediate" if immediate is True else "open_conditional"
        request = {
            "paper_math_version": PAPER_MATH_VERSION,
            "operation": operation,
            "strategy_id": sid,
            "symbol": ticker,
            "direction": direction,
            "immediate": _identity_value(immediate, "immediate"),
            "entry_price": _identity_decimal(entry_price, "entry_price"),
            "stop_price": _identity_decimal(stop_price, "stop_price"),
            "target_price": _identity_decimal(target_price, "target_price"),
            "fill_reference": _identity_value(fill_reference, "fill_reference"),
            "requested_entry_price": _identity_decimal(requested_entry_price, "requested_entry_price"),
            "trade_id": _identity_value(trade_id, "trade_id"),
            "fees": _identity_decimal(fees, "fees"),
            "slippage": _identity_decimal(slippage, "slippage"),
        }
        request_hash = stable_hash(request)
        key = _key_or_auto(idempotency_key, request_hash)
        replay = self._replay_or_conflict(key, request_hash, sid, operation)
        if replay is not None:
            return replay

        # Validate all untrusted values before creating an account.  Invalid
        # input therefore cannot mutate an unknown strategy's state.
        had_valid_fill_reference = False
        try:
            if type(immediate) is not bool:
                raise InvalidPaperInput("immediate must be exactly bool", code="invalid_immediate", path="$.immediate")
            supplied_trade_id = _require_optional_id(trade_id, "trade_id", "invalid_trade_id")
            if model_payload is not None:
                assert_model_payload_safe(model_payload)
            if decimal_input(fees, "fees") != ZERO or decimal_input(slippage, "slippage") != ZERO:
                raise InvalidPaperInput("fees and slippage are fixed at zero", code="nonzero_costs", path="$")
            side = validate_direction(direction)
            stop = positive_decimal(stop_price, "stop_price")
            target = positive_decimal(target_price, "target_price")
            if immediate:
                if fill_reference is None:
                    raise InvalidPaperInput("immediate entry requires an actual fill reference", code="missing_fill_reference", path="$.fill_reference")
                fill_value, _fill_metadata = _price_reference(fill_reference, "fill_reference")
                fill = positive_decimal(fill_value, "fill_reference")
                original_entry = requested_entry_price if requested_entry_price is not None else entry_price
                requested = positive_decimal(original_entry, "requested_entry_price") if original_entry is not None else None
                if requested is not None:
                    # Validate the model's original second-event plan first;
                    # only a later actual fill may produce a fresh-price
                    # drift rejection below.
                    validate_levels(side, requested, stop, target)
                had_valid_fill_reference = True
                # Revalidate against the actual fill, never the stale model
                # response.  This is the delayed-immediate drift guard.
                validate_levels(side, fill, stop, target)
                entry = fill
            else:
                if entry_price is None:
                    raise InvalidPaperInput("conditional entry requires entry_price", code="missing_entry_price", path="$.entry_price")
                requested = positive_decimal(entry_price, "entry_price")
                if requested_entry_price is not None:
                    supplied_requested = positive_decimal(requested_entry_price, "requested_entry_price")
                    if supplied_requested != requested:
                        raise InvalidPaperInput("requested_entry_price must equal entry_price", code="entry_price_mismatch", path="$.requested_entry_price")
                if fill_reference is not None:
                    fill_value, _fill_metadata = _price_reference(fill_reference, "fill_reference")
                    entry = positive_decimal(fill_value, "fill_reference")
                    if entry != requested:
                        raise InvalidPaperInput("conditional fill reference must equal the requested entry price", code="fill_price_mismatch", path="$.fill_reference")
                else:
                    entry = requested
                validate_levels(side, entry, stop, target)
        except Exception as exc:
            code = getattr(exc, "code", "invalid_entry")
            reason = getattr(exc, "message", "entry is invalid")
            fresh = bool(immediate is True and had_valid_fill_reference and code == "invalid_level_order")
            event_type = "paper_fill_rejected" if immediate is True else None
            payload = {"operation": operation, "reason": reason, "code": code, "fill_reference": _identity_value(fill_reference, "fill_reference")}
            return self._rejection(strategy_id=sid, operation=operation, key=key, request_hash=request_hash, reason=reason, code=code, account=self._accounts.get(sid), fresh_price_rejected=fresh, event_type=event_type, event_payload=payload)

        account = self._accounts.get(sid)
        if account is not None and (account.open_trade is not None or account.pending):
            return self._rejection(strategy_id=sid, operation=operation, key=key, request_hash=request_hash, reason="strategy already has a pending or open trade", code="position_exists", account=account)
        if account is not None and (account.balance <= ZERO or account.exhausted):
            account.exhausted = True
            return self._rejection(strategy_id=sid, operation=operation, key=key, request_hash=request_hash, reason="account balance is zero", code="account_exhausted", account=account, exhausted=True)

        try:
            calculation = calculate_trade(side, account.balance if account is not None else INITIAL_BALANCE, entry, stop, target)
        except Exception as exc:
            return self._rejection(
                strategy_id=sid,
                operation=operation,
                key=key,
                request_hash=request_hash,
                reason=getattr(exc, "message", "trade calculation failed"),
                code=getattr(exc, "code", "invalid_trade"),
                account=account,
            )
        if account is None:
            self.ensure_account(sid)
            account = self._accounts[sid]
        if not calculation["executable"]:
            account.exhausted = True
            return self._rejection(strategy_id=sid, operation=operation, key=key, request_hash=request_hash, reason="available capital cannot buy one share quantum", code="untradeable_capital", account=account, untradeable=True, exhausted=True)

        trade_projection = {
            "paper_math_version": PAPER_MATH_VERSION,
            "strategy_id": sid,
            "symbol": ticker,
            "direction": side,
            "entry_price": entry,
            "stop_price": stop,
            "target_price": target,
            "quantity": calculation["quantity"],
            "planned_r": calculation["planned_r"],
            "allocation_fraction": ONE,
            "immediate": immediate,
            "requested_entry_price": requested,
        }
        digest = stable_hash(trade_projection)
        actual_trade_id = supplied_trade_id or "ptr_" + digest
        trade = PaperTrade(
            strategy_id=sid,
            trade_id=actual_trade_id,
            symbol=ticker,
            direction=side,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            quantity=calculation["quantity"],
            planned_r=calculation["planned_r"],
            allocation_fraction=ONE,
            immediate=immediate,
            requested_entry_price=requested,
        )
        account.open_trade = trade
        event_payload = {
            "operation": operation,
            "trade": trade.to_dict(),
            "balance_before": account.balance,
            "allocation_fraction": ONE,
            "fill_reference": _identity_value(fill_reference, "fill_reference"),
            "fees": ZERO_FEES,
            "slippage": ZERO_SLIPPAGE,
        }
        event = PaperEvent.make("paper_trade_opened", sid, event_payload)
        self._events.append(event)
        result = TransitionResult(
            status="accepted",
            strategy_id=sid,
            operation=operation,
            changed=True,
            accepted=True,
            idempotency_key=key,
            request_hash=request_hash,
            event=event,
            account=account.snapshot(),
            trade=copy.deepcopy(trade),
            calculation=copy.deepcopy(calculation),
        )
        self._history.setdefault(sid, []).append(copy.deepcopy(trade))
        return self._remember(key, request_hash, result)

    def open_from_instruction(
        self,
        strategy_id: str,
        instruction: Mapping[str, Any],
        *,
        symbol: str,
        fill_reference: Any | None = None,
        idempotency_key: str | None = None,
        model_payload: Any | None = None,
    ) -> TransitionResult:
        """Consume a G018-like account-free instruction without importing G018."""
        sid = _require_strategy_id(strategy_id)
        operation = "open_from_instruction"
        request_projection = {
            "operation": operation,
            "strategy_id": sid,
            "instruction": _identity_value(instruction, "instruction"),
            "symbol": _identity_value(symbol, "symbol"),
            "fill_reference": _identity_value(fill_reference, "fill_reference"),
        }
        request_hash = stable_hash(request_projection)
        key = _key_or_auto(idempotency_key, request_hash)
        replay = self._replay_or_conflict(key, request_hash, sid, operation)
        if replay is not None:
            return replay
        try:
            if not isinstance(instruction, dict):
                raise InvalidPaperInput("instruction must be a JSON object", code="invalid_instruction", path="$.instruction")
            assert_model_payload_safe(instruction)
            if model_payload is not None:
                assert_model_payload_safe(model_payload)
            branch = instruction.get("branch")
            if branch not in {"no_entry", "enter_immediately", "wait_for_price"}:
                raise InvalidPaperInput("instruction branch is unsupported", code="invalid_branch", path="$.instruction.branch")
            bound_strategy = instruction.get("strategy_id")
            if type(bound_strategy) is not str or bound_strategy != sid:
                raise InvalidPaperInput("instruction strategy_id does not match the requested strategy", code="strategy_binding_mismatch", path="$.instruction.strategy_id")
        except Exception as exc:
            return self._rejection(
                strategy_id=sid,
                operation=operation,
                key=key,
                request_hash=request_hash,
                reason=getattr(exc, "message", "instruction is invalid"),
                code=getattr(exc, "code", "invalid_instruction"),
                account=self._accounts.get(sid),
            )
        if branch == "no_entry":
            return self._rejection(strategy_id=sid, operation=operation, key=key, request_hash=request_hash, reason="instruction does not contain an entry", code="no_entry", account=self._accounts.get(sid))
        direction = instruction.get("direction")
        stop = instruction.get("stop_price")
        target = instruction.get("target_price")
        if branch == "enter_immediately":
            second = instruction.get("second_event_ref") or {}
            original = second.get("reference_price")
            # Preserve the complete trusted G018 second-event identity in the
            # opening event when no later fill is supplied.  A fresh fill may
            # be a scalar or its own full reference mapping; the original
            # reference price remains the requested level for drift checks.
            actual = fill_reference if fill_reference is not None else second
            result = self.open_trade(
                strategy_id,
                symbol,
                direction,
                None,
                stop,
                target,
                immediate=True,
                fill_reference=actual,
                requested_entry_price=original if fill_reference is not None else None,
                idempotency_key=key,
                model_payload=model_payload or instruction,
            )
            return self._remember(key, request_hash, replace(result, operation=operation, idempotency_key=key, request_hash=request_hash))
        entry = instruction.get("entry_price")
        pending = self.create_pending_entry(strategy_id, symbol, direction, entry, stop, target, idempotency_key=key)
        if fill_reference is None or not pending.accepted:
            return self._remember(key, request_hash, replace(pending, operation=operation, idempotency_key=key, request_hash=request_hash))
        filled = self.fill_pending_entry(strategy_id, pending.pending_id, fill_reference, idempotency_key=key + ":fill")
        return self._remember(key, request_hash, replace(filled, operation=operation, idempotency_key=key, request_hash=request_hash))

    def close_trade(
        self,
        strategy_id: str,
        exit_price: Any = None,
        *positional: Any,
        idempotency_key: str | None = None,
        reason: str | None = None,
        position_id: str | None = None,
        trade_id: str | None = None,
        event_reference: Mapping[str, Any] | None = None,
        fees: Any = ZERO,
        slippage: Any = ZERO,
    ) -> TransitionResult:
        """Settle the current position at an exact supplied exit reference."""

        sid = _require_strategy_id(strategy_id)
        # Accept the adjacent monitor's ergonomic ``close(strategy, id,
        # exit_price)`` shape while retaining the concise ``close(strategy,
        # exit_price, position_id=...)`` form.
        positional_identity: str | None = None
        if positional:
            if len(positional) != 1:
                raise InvalidPaperInput("close accepts at most one positional exit reference", code="invalid_close_arguments", path="$")
            positional_identity = exit_price if isinstance(exit_price, str) else None
            exit_price = positional[0]
            if position_id is None and positional_identity is not None:
                position_id = positional_identity
        operation = "close"
        request = {
            "paper_math_version": PAPER_MATH_VERSION,
            "operation": operation,
            "strategy_id": sid,
            "exit_price": _identity_decimal(exit_price, "exit_price"),
            "reason": _identity_value(reason, "reason"),
            "position_id": _identity_value(position_id, "position_id"),
            "trade_id": _identity_value(trade_id, "trade_id"),
            "event_reference": _identity_value(event_reference, "event_reference"),
            "fees": _identity_decimal(fees, "fees"),
            "slippage": _identity_decimal(slippage, "slippage"),
        }
        request_hash = stable_hash(request)
        key = _key_or_auto(idempotency_key, request_hash)
        replay = self._replay_or_conflict(key, request_hash, sid, operation)
        if replay is not None:
            return replay
        account = self._accounts.get(sid)
        try:
            supplied_position_id = _require_optional_id(position_id, "position_id", "invalid_position_id")
            supplied_trade_id = _require_optional_id(trade_id, "trade_id", "invalid_trade_id")
            if supplied_position_id is not None and supplied_trade_id is not None and supplied_position_id != supplied_trade_id:
                raise InvalidPaperInput("position_id and trade_id must identify the same trade", code="position_mismatch", path="$")
            if decimal_input(fees, "fees") != ZERO or decimal_input(slippage, "slippage") != ZERO:
                raise InvalidPaperInput("fees and slippage are fixed at zero", code="nonzero_costs", path="$")
            exit_ = positive_decimal(exit_price, "exit_price")
        except Exception as exc:
            return self._rejection(strategy_id=sid, operation=operation, key=key, request_hash=request_hash, reason=getattr(exc, "message", "exit is invalid"), code=getattr(exc, "code", "invalid_exit"), account=account)
        if account is None or account.open_trade is None:
            return self._rejection(strategy_id=sid, operation=operation, key=key, request_hash=request_hash, reason="strategy has no open trade", code="no_open_position", account=account)
        trade = account.open_trade
        expected_id = supplied_position_id or supplied_trade_id
        if expected_id is not None and expected_id != trade.trade_id:
            return self._rejection(strategy_id=sid, operation=operation, key=key, request_hash=request_hash, reason="position identity does not match the open trade", code="position_mismatch", account=account)

        settlement = calculate_settlement(trade.direction, account.balance, trade.entry_price, exit_, trade.quantity)
        account.balance = settlement["closing_balance"]
        account.open_trade = None
        account.exhausted = settlement["exhausted"]
        status = "exhausted" if settlement["exhausted"] else "closed"
        event_payload = {
            "operation": operation,
            "trade": trade.to_dict(),
            "exit_price": exit_,
            "reason": _identity_value(reason, "reason"),
            "event_reference": _identity_value(event_reference, "event_reference"),
            "settlement": settlement,
            "fees": ZERO_FEES,
            "slippage": ZERO_SLIPPAGE,
        }
        event = PaperEvent.make("paper_trade_closed", sid, event_payload)
        self._events.append(event)
        result = TransitionResult(
            status=status,
            strategy_id=sid,
            operation=operation,
            changed=True,
            accepted=True,
            idempotency_key=key,
            request_hash=request_hash,
            event=event,
            account=account.snapshot(),
            closed_trade=copy.deepcopy(trade),
            settlement=copy.deepcopy(settlement),
            exhausted=settlement["exhausted"],
        )
        return self._remember(key, request_hash, result)

    def history(self, strategy_id: str) -> tuple[PaperTrade, ...]:
        sid = _require_strategy_id(strategy_id)
        return tuple(copy.deepcopy(self._history.get(sid, [])))

__all__ = ["PaperAccount", "PaperAccountBook"]
