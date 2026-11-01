"""Sequential, replayable quote simulation with integer-cent capital accounting.

The runtime owns durability. Each return contains only changed rows; a runtime
must commit those rows, this snapshot and its input progress in one transaction.
"""
from __future__ import annotations

import copy
from datetime import timedelta
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from zoneinfo import ZoneInfo

from recognition.models import MarketEvent, timestamp
from .policy import ExecutionPolicy, decimal, execution_profiles


def cents(value):
    return int((decimal(value) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def money(value):
    return format(Decimal(value) / 100, ".2f")


def price(value):
    return format(decimal(value).quantize(Decimal("0.000001")), "f")


class PaperSimulator:
    schema_version = "1.0.0"

    def __init__(self, policy=None):
        self.policy = policy or ExecutionPolicy()
        self.profiles = {(p["setup_id"], p["variant_id"]): p for p in execution_profiles(self.policy)}
        self.balance_cents = cents(self.policy.initial_cash)
        self.realized_cents = self.fees_cents = 0
        self.last_event_seq = 0
        self.counter = 0
        self.sessions = {}
        self.positions = {}
        self.orders = {}
        self.marks = {}
        self.halted = set()
        self.disconnected = set()
        self.consumed = set()
        self.day = None
        self.day_start_equity_cents = self.balance_cents
        self.daily_locked = False
        self.entry_paused = False
        self.last_clock_time = None
        self.recent_fills = []
        self.recent_account_events = []
        self._changes = None

    def _id(self, prefix):
        self.counter += 1
        return f"{prefix}-{self.counter:010d}"

    def _orders_by_id(self):
        # Serialized mappings carry no semantic insertion order.
        return [order for _, order in sorted(self.orders.items())]

    def _record(self, category, row):
        self._changes[category].append(copy.deepcopy(row))
        if category == "fills":
            self.recent_fills.append(copy.deepcopy(row))
            self.recent_fills = self.recent_fills[-self.policy.recent_rows_limit:]
        elif category == "account_events":
            self.recent_account_events.append(copy.deepcopy(row))
            self.recent_account_events = self.recent_account_events[-self.policy.recent_rows_limit:]

    def _ledger(self, kind, amount=0, **extra):
        self._record("account_events", {"account_event_id": self._id("ACC"), "event_seq": self.last_event_seq,
                     "kind": kind, "amount_cents": amount, "amount": money(amount),
                     "balance_delta_cents": amount if kind in {"position_closed", "fee"} else 0, **extra})

    def _decision(self, detection, reason, **extra):
        self._record("decisions", {"event_seq": self.last_event_seq, "detection_id": detection.get("detection_id"),
                     "symbol": detection.get("symbol"), "reason": reason, **extra})

    def _locked(self):
        return sum(p["entry_notional_cents"] for p in self.positions.values())

    def _reserved(self):
        return sum(o["budget_cents"] for o in self.orders.values() if o["purpose"] == "entry" and o["status"] == "pending")

    def _unrealized(self):
        total = 0
        for symbol, pos in self.positions.items():
            mark = self.marks.get(symbol)
            if mark and mark.get("venue") == pos["venue"]:
                px = decimal(mark["bid"] if pos["direction"] == 1 else mark["ask"])
                total += cents((px - decimal(pos["entry_price"])) * pos["quantity"] * pos["direction"])
        return total

    def account(self):
        unrealized = self._unrealized()
        locked, reserved = self._locked(), self._reserved()
        values = {"balance_cents": self.balance_cents, "equity_cents": self.balance_cents + unrealized,
                  "available_cents": self.balance_cents - locked - reserved, "locked_cents": locked,
                  "reserved_cents": reserved, "realized_pnl_cents": self.realized_cents,
                  "unrealized_pnl_cents": unrealized, "fees_cents": self.fees_cents}
        return {**values, **{k.removesuffix("_cents"): money(v) for k, v in values.items()},
                "currency": "USD", "execution_model": self.policy.model, "execution_version": self.policy.version,
                "execution_sha256": self.policy.fingerprint(), "day": self.day,
                "daily_entry_blocked": self.daily_locked, "position_count": len(self.positions),
                "entry_paused": self.entry_paused,
                "pending_entries": sum(o["purpose"] == "entry" and o["status"] == "pending" for o in self.orders.values()),
                "fee_per_fill": self.policy.fee_per_fill, "valuation_times": {k: v["event_time"] for k, v in self.marks.items()},
                "performance_scope": "experimental_overlay_not_source_strategy"}

    account_snapshot = account

    def _pending_entry(self, symbol):
        return next((o for o in self._orders_by_id() if o["symbol"] == symbol and o["purpose"] == "entry" and o["status"] == "pending"), None)

    def _execution_session(self, symbol, venue):
        session = self.sessions.get(symbol)
        return session if session and session.get("venue") == venue else None

    def _cancel(self, order, reason):
        if order["status"] != "pending":
            return
        order.update(status="cancelled", cancel_reason=reason, updated_seq=self.last_event_seq)
        self._record("orders", order)
        self._ledger("reservation_released", order["budget_cents"], order_id=order["order_id"], reason=reason)

    def _exit(self, pos, reason, event):
        existing = self.orders.get(pos.get("exit_order_id"))
        if existing and existing["status"] == "pending":
            if reason not in existing["reasons"]:
                existing["reasons"].append(reason)
                self._record("orders", existing)
            return
        order = {"order_id": self._id("ORD"), "symbol": pos["symbol"], "venue": pos["venue"], "purpose": "exit",
                 "status": "pending", "direction": pos["direction"], "quantity": pos["quantity"],
                 "detection_id": pos["detection_id"], "submitted_seq": self.last_event_seq,
                 "submitted_at": event.arrival_time.isoformat(), "cutoff": event.event_time.isoformat(),
                 "reasons": [reason], "budget_cents": 0, "profile": pos["profile"],
                 "execution_model": self.policy.model, "execution_version": self.policy.version}
        self.orders[order["order_id"]] = order
        pos["exit_order_id"] = order["order_id"]
        pos["status"] = "pending_exit"
        self._record("orders", order)

    def _risk_clock(self, event):
        now = max(event.arrival_time, timestamp(self.last_clock_time)) if self.last_clock_time else event.arrival_time
        self.last_clock_time = now.isoformat()
        for order in self._orders_by_id():
            if order["status"] == "pending" and order["purpose"] == "entry":
                close = (self._execution_session(order["symbol"], order["venue"]) or {}).get("close_time")
                if now > timestamp(order["expires_at"]):
                    self._cancel(order, "entry_timeout")
                elif close and now >= timestamp(close) - timedelta(seconds=self.policy.close_buffer_seconds):
                    self._cancel(order, "session_close_buffer")
        for _, pos in sorted(self.positions.items()):
            close = (self._execution_session(pos["symbol"], pos["venue"]) or {}).get("close_time")
            if close and now >= timestamp(close) - timedelta(seconds=self.policy.close_buffer_seconds):
                self._exit(pos, "session_close_buffer", event)
            if now >= timestamp(pos["filled_at"]) + timedelta(seconds=self.policy.max_hold_seconds):
                self._exit(pos, "max_hold", event)
        equity = self.balance_cents + self._unrealized()
        if self.day_start_equity_cents > 0 and equity <= self.day_start_equity_cents * (1 - decimal(self.policy.daily_loss_fraction)):
            if not self.daily_locked:
                self._ledger("daily_loss_lock", reason="daily_loss_limit")
            self.daily_locked = True
            for o in self._orders_by_id():
                if o["purpose"] == "entry" and o["status"] == "pending":
                    self._cancel(o, "daily_loss_limit")
            for _, pos in sorted(self.positions.items()):
                self._exit(pos, "daily_loss_limit", event)

    def _usable_quote(self, event):
        if not self._execution_session(event.symbol, event.venue):
            return False
        if event.kind != "quote" or event.quality != "complete" or event.symbol in self.halted or event.symbol in self.disconnected:
            return False
        if event.payload.get("analysis_only") or event.payload.get("correction"):
            return False
        if self.last_clock_time and event.arrival_time < timestamp(self.last_clock_time):
            return False
        prior = self.marks.get(event.symbol)
        if prior and event.event_time < timestamp(prior["event_time"]):
            return False
        if (event.arrival_time - event.event_time).total_seconds() > self.policy.quote_max_age_seconds:
            return False
        try:
            bid, ask = decimal(event.payload["bid"]), decimal(event.payload["ask"])
            return bid > 0 and ask >= bid
        except (KeyError, ValueError):
            return False

    def _fill_orders(self, event):
        if not self._usable_quote(event):
            return
        session = self._execution_session(event.symbol, event.venue)
        if not session or not timestamp(session["open_time"]) <= event.event_time < timestamp(session["close_time"]):
            return
        self.marks[event.symbol] = {"bid": price(event.payload["bid"]), "ask": price(event.payload["ask"]),
                                    "venue": event.venue, "event_time": event.event_time.isoformat(), "arrival_time": event.arrival_time.isoformat()}
        # A fresh adverse mark can hit daily limits before a waiting entry fills.
        self._risk_clock(event)
        tick = decimal(session.get("tick_size", self.policy.tick_size)) * self.policy.slippage_ticks
        fee = cents(self.policy.fee_per_fill)
        for order in self._orders_by_id():
            if order["status"] != "pending" or order["symbol"] != event.symbol or order["venue"] != event.venue:
                continue
            if self.last_event_seq <= order["submitted_seq"] or event.event_time <= timestamp(order["cutoff"]) or event.arrival_time <= timestamp(order["submitted_at"]):
                continue
            buy = (order["direction"] == 1) == (order["purpose"] == "entry")
            px = decimal(event.payload["ask"] if buy else event.payload["bid"]) + (tick if buy else -tick)
            if px <= 0:
                continue
            if order["purpose"] == "entry":
                # Budget includes the entry fee. Short proceeds are never credited.
                qty = int((Decimal(order["budget_cents"] - fee) / (px * 100)).to_integral_value(rounding=ROUND_FLOOR))
                if qty < 1:
                    self._cancel(order, "budget_below_one_share")
                    continue
                notional = cents(px * qty)
                pos = {"position_id": self._id("POS"), "symbol": event.symbol, "venue": event.venue,
                       "direction": order["direction"], "quantity": qty, "entry_price": price(px),
                       "entry_notional_cents": notional, "filled_at": event.arrival_time.isoformat(),
                       "entry_market_time": event.event_time.isoformat(), "detection_id": order["detection_id"],
                       "entry_order_id": order["order_id"], "exit_order_id": None, "status": "open",
                       "profile": order["profile"], "related_detection_ids": list(order["related_detection_ids"])}
                self.positions[event.symbol] = pos
                self.balance_cents -= fee
                self._ledger("reservation_released", order["budget_cents"], order_id=order["order_id"], reason="entry_filled")
                self._ledger("capital_locked", -notional, order_id=order["order_id"], position_id=pos["position_id"])
            else:
                pos = self.positions.get(event.symbol)
                if not pos or pos["venue"] != event.venue:
                    continue
                qty = pos["quantity"]
                notional = cents(px * qty)
                pnl = cents((px - decimal(pos["entry_price"])) * qty * pos["direction"])
                self.balance_cents += pnl - fee
                self.realized_cents += pnl
                self._ledger("position_closed", pnl, position_id=pos["position_id"], order_id=order["order_id"],
                             released_cents=pos["entry_notional_cents"], realized_pnl_cents=pnl)
                del self.positions[event.symbol]
            self.fees_cents += fee
            order.update(status="filled", quantity=qty, fill_price=price(px), filled_at=event.arrival_time.isoformat(), updated_seq=self.last_event_seq)
            self._record("orders", order)
            self._record("fills", {"fill_id": self._id("FILL"), "order_id": order["order_id"], "event_seq": self.last_event_seq,
                         "event_id": event.event_id, "symbol": event.symbol, "purpose": order["purpose"], "side": "buy" if buy else "sell",
                         "quantity": qty, "price": price(px), "notional_cents": notional, "fee_cents": fee,
                         "market_time": event.event_time.isoformat(), "received_at": event.arrival_time.isoformat(),
                         "execution_model": self.policy.model, "profile": order["profile"]})
            if fee:
                self._ledger("fee", -fee, order_id=order["order_id"])
        pos = self.positions.get(event.symbol)
        if pos and pos["venue"] == event.venue:
            liquidation = decimal(event.payload["bid"] if pos["direction"] == 1 else event.payload["ask"])
            change = (liquidation / decimal(pos["entry_price"]) - 1) * pos["direction"]
            if change <= -decimal(self.policy.stop_fraction):
                self._exit(pos, "stop_threshold", event)
            if change >= decimal(self.policy.target_fraction):
                self._exit(pos, "target_threshold", event)

    def _arbitrate(self, event, rows):
        # Handle final state once per detection: trigger then invalidate is never an entry.
        final = {}
        for row in rows:
            final[row["detection_id"]] = row
        for _, row in sorted(final.items()):
            if row["state"] not in {"invalidated", "expired", "completed"}:
                continue
            order = self._pending_entry(row["symbol"])
            if order and order["detection_id"] == row["detection_id"] and order["venue"] == row.get("venue"):
                self._cancel(order, "initiator_" + row["state"])
            pos = self.positions.get(row["symbol"])
            if pos and pos["detection_id"] == row["detection_id"] and pos["venue"] == row.get("venue"):
                self._exit(pos, "initiator_" + row["state"], event)
        groups = {}
        for _, row in sorted(final.items()):
            if row["state"] != "triggered" or row["detection_id"] in self.consumed:
                continue
            self.consumed.add(row["detection_id"])
            registered = self.sessions.get(row["symbol"])
            if registered and registered.get("venue") != row.get("venue"):
                self._decision(row, "execution_venue_mismatch", expected_venue=registered["venue"], observed_venue=row.get("venue"))
                continue
            if row.get("data_status") != "complete" or row.get("missing"):
                self._decision(row, "incomplete_detection")
                continue
            if type(row.get("direction")) is not int or row["direction"] not in {-1, 1}:
                self._decision(row, "invalid_direction")
                continue
            if type(row.get("scale_seconds")) is not int or row["scale_seconds"] < 0:
                self._decision(row, "invalid_scale")
                continue
            try:
                cutoff = timestamp(row["evidence_cutoff"])
                if cutoff > event.event_time:
                    raise ValueError("future evidence cutoff")
            except (ValueError, KeyError, TypeError, AttributeError):
                self._decision(row, "invalid_evidence_cutoff")
                continue
            profile = self.profiles.get((row.get("setup_id"), row.get("variant_id", "base")))
            if not profile:
                self._decision(row, "unknown_execution_profile")
                continue
            if row["direction"] not in profile["directions"]:
                self._decision(row, "unsupported_profile_direction")
                continue
            if row["scale_seconds"] not in profile["scales"]:
                self._decision(row, "unsupported_profile_scale")
                continue
            if any(row.get(k) != profile[k] for k in ("source_version", "source_sha256", "rule_version")):
                self._decision(row, "profile_version_mismatch")
                continue
            if row.get("algorithm_sha256") and row["algorithm_sha256"] != profile["algorithm_sha256"]:
                self._decision(row, "profile_algorithm_mismatch")
                continue
            groups.setdefault(row["symbol"], []).append(row)
        for symbol in sorted(groups):
            candidates = sorted(groups[symbol], key=lambda r: (r.get("variant_id", "base") != "base", r["setup_id"], r.get("variant_id", "base"), r.get("scale_seconds", 0), r["detection_id"]))
            owner = self.positions.get(symbol) or self._pending_entry(symbol)
            if owner:
                for row in candidates:
                    if owner["direction"] == row["direction"]:
                        if row["detection_id"] not in owner["related_detection_ids"]:
                            owner["related_detection_ids"].append(row["detection_id"])
                        self._decision(row, "associated_same_direction", owner_detection_id=owner["detection_id"])
                    else:
                        self._decision(row, "existing_position_no_reversal", owner_detection_id=owner["detection_id"])
                if owner.get("purpose") == "entry":
                    self._record("orders", owner)
                continue
            if len({r["direction"] for r in candidates}) > 1:
                for row in candidates:
                    self._decision(row, "same_batch_direction_conflict")
                continue
            row = candidates[0]
            session = self.sessions.get(symbol)
            blocked = None
            if event.payload.get("analysis_only"):
                blocked = "analysis_only_no_orders"
            elif self.last_clock_time and event.arrival_time < timestamp(self.last_clock_time):
                blocked = "stale_after_clock"
            elif not session:
                blocked = "missing_session"
            elif symbol in self.halted or symbol in self.disconnected:
                blocked = "data_disconnected_or_halted"
            elif not timestamp(session["open_time"]) <= event.arrival_time < timestamp(session["close_time"]) - timedelta(seconds=self.policy.close_buffer_seconds):
                blocked = "outside_entry_session"
            elif self.daily_locked:
                blocked = "daily_loss_limit"
            elif self.entry_paused:
                blocked = "run_paused"
            elif len(self.positions) + sum(o["status"] == "pending" and o["purpose"] == "entry" for o in self.orders.values()) >= self.policy.max_positions:
                blocked = "position_limit"
            if blocked:
                for candidate in candidates:
                    self._decision(candidate, blocked)
                continue
            account = self.account()
            budget = min(int((Decimal(account["equity_cents"]) * decimal(self.policy.allocation_fraction)).to_integral_value(rounding=ROUND_FLOOR)), account["available_cents"])
            if budget <= cents(self.policy.fee_per_fill):
                for candidate in candidates:
                    self._decision(candidate, "insufficient_available_capital")
                continue
            profile = copy.deepcopy(self.profiles[(row["setup_id"], row.get("variant_id", "base"))])
            # Freeze actual detection provenance in the order, including recognition config.
            profile["detection_algorithm_sha256"] = row.get("algorithm_sha256", "")
            profile["detection_configuration_sha256"] = row.get("configuration_sha256", "")
            order = {"order_id": self._id("ORD"), "symbol": symbol, "venue": row["venue"], "purpose": "entry", "status": "pending",
                     "direction": row["direction"], "detection_id": row["detection_id"], "submitted_seq": self.last_event_seq,
                     "submitted_at": event.arrival_time.isoformat(), "cutoff": row["evidence_cutoff"],
                     "expires_at": (event.arrival_time + timedelta(seconds=self.policy.entry_timeout_seconds)).isoformat(),
                     "budget_cents": budget, "profile": profile, "related_detection_ids": [r["detection_id"] for r in candidates[1:]],
                     "execution_model": self.policy.model, "execution_version": self.policy.version, "reasons": ["triggered"]}
            self.orders[order["order_id"]] = order
            self._record("orders", order)
            self._ledger("capital_reserved", -budget, order_id=order["order_id"])
            self._decision(row, "entry_submitted", order_id=order["order_id"])
            for other in candidates[1:]:
                self._decision(other, "associated_same_batch", owner_detection_id=row["detection_id"])

    def on_event(self, event, detections, event_seq):
        if type(event_seq) is not int or event_seq <= 0:
            raise ValueError("event_seq must be a positive integer")
        if event_seq < self.last_event_seq:
            raise ValueError("paper event sequence moved backwards")
        self._changes = {"orders": [], "fills": [], "account_events": [], "decisions": []}
        if event_seq == self.last_event_seq:
            return {**self._changes, "account": self.account(), "positions": copy.deepcopy([p for _, p in sorted(self.positions.items())])}
        self.last_event_seq = event_seq
        rows = [d.to_dict() if hasattr(d, "to_dict") else dict(d) for d in detections]
        if event.kind == "session":
            registered = self.sessions.get(event.symbol)
            if registered and registered["venue"] != event.venue:
                self._decision({"symbol": event.symbol}, "session_venue_mismatch", expected_venue=registered["venue"], observed_venue=event.venue)
            else:
                session_day = timestamp(event.payload["open_time"]).astimezone(ZoneInfo("America/New_York")).date().isoformat()
                if self.day != session_day:
                    self.day = session_day
                    self.day_start_equity_cents = self.balance_cents + self._unrealized()
                    self.daily_locked = False
                    self.consumed.clear()
                self.sessions[event.symbol] = {k: event.payload[k] for k in ("open_time", "close_time")}
                self.sessions[event.symbol].update(venue=event.venue, tick_size=event.payload.get("tick_size", self.policy.tick_size))
        elif event.kind == "status" and self._execution_session(event.symbol, event.venue):
            if event.payload.get("halted"):
                self.halted.add(event.symbol)
            elif (not event.payload.get("clock") and "connected" not in event.payload
                  and "connection_state" not in event.payload and not event.payload.get("correction")
                  and not event.payload.get("gap") and event.payload.get("halted") is False):
                self.halted.discard(event.symbol)
            if event.payload.get("gap") and event.payload.get("stream", "all") == "all":
                self.disconnected.add(event.symbol)
            elif event.payload.get("connected"):
                self.disconnected.discard(event.symbol)
        self._risk_clock(event)
        # Invalidations cancel entries before a quote can fill them.
        final_rows = {r["detection_id"]: r for r in rows}
        endings = [r for _, r in sorted(final_rows.items()) if r["state"] in {"invalidated", "expired", "completed"}]
        self._arbitrate(event, endings)
        fill_event = event
        if self.policy.model == "next_bar_proxy_experimental":
            fill_event = None
            if event.kind == "bar" and event.quality == "complete":
                fill_event = MarketEvent(event_id=event.event_id, symbol=event.symbol, venue=event.venue,
                    kind="quote", event_time=event.event_time, arrival_time=event.arrival_time,
                    quality=event.quality, payload={"bid": event.payload["open"], "ask": event.payload["open"],
                    "analysis_only": event.payload.get("analysis_only", False), "bar_proxy": True})
        if fill_event is not None:
            self._fill_orders(fill_event)
        self._risk_clock(event)
        self._arbitrate(event, rows)
        # Bound in-memory completed history; runtime keeps the committed complete history.
        terminal = [k for k, o in sorted(self.orders.items()) if o["status"] != "pending"]
        for key in terminal[:-self.policy.recent_rows_limit]:
            del self.orders[key]
        return {**self._changes, "account": self.account(), "positions": copy.deepcopy([p for _, p in sorted(self.positions.items())])}

    def snapshot(self):
        return {"schema_version": self.schema_version, "policy": self.policy.to_dict(), "policy_sha256": self.policy.fingerprint(),
                "profiles_sha256": self._profiles_hash(), "balance_cents": self.balance_cents, "realized_cents": self.realized_cents,
                "fees_cents": self.fees_cents, "last_event_seq": self.last_event_seq, "counter": self.counter,
                "sessions": copy.deepcopy(dict(sorted(self.sessions.items()))), "positions": copy.deepcopy(dict(sorted(self.positions.items()))), "orders": copy.deepcopy(dict(sorted(self.orders.items()))),
                "marks": copy.deepcopy(dict(sorted(self.marks.items()))), "halted": sorted(self.halted), "disconnected": sorted(self.disconnected), "consumed": sorted(self.consumed),
                "day": self.day, "day_start_equity_cents": self.day_start_equity_cents, "daily_locked": self.daily_locked,
                "entry_paused": self.entry_paused, "last_clock_time": self.last_clock_time,
                "recent_fills": copy.deepcopy(self.recent_fills), "recent_account_events": copy.deepcopy(self.recent_account_events)}

    def _profiles_hash(self):
        import hashlib
        import json
        return hashlib.sha256(json.dumps(list(self.profiles.values()), sort_keys=True, ensure_ascii=False).encode()).hexdigest()

    @classmethod
    def from_snapshot(cls, row, policy=None):
        if row.get("schema_version") != cls.schema_version:
            raise ValueError("paper snapshot schema mismatch")
        result = cls(policy or ExecutionPolicy(**row["policy"]))
        if row.get("policy_sha256") != result.policy.fingerprint() or row.get("profiles_sha256") != result._profiles_hash():
            raise ValueError("paper policy/rule/algorithm version mismatch; replay required")
        for key in ("balance_cents", "realized_cents", "fees_cents", "last_event_seq", "counter", "sessions", "positions", "orders", "marks", "day", "day_start_equity_cents", "daily_locked", "entry_paused", "last_clock_time", "recent_fills", "recent_account_events"):
            setattr(result, key, copy.deepcopy(row[key]))
        result.halted, result.consumed = set(row["halted"]), set(row["consumed"])
        result.disconnected = set(row["disconnected"])
        return result

    def control(self, action, event_seq, at=None):
        """Journalled pause/stop/resume; no fabricated liquidation quote."""
        from datetime import datetime, timezone
        if action not in {"pause", "stop", "resume", "start"}:
            raise ValueError("unknown paper control action")
        if type(event_seq) is not int or event_seq <= self.last_event_seq:
            raise ValueError("control sequence must advance")
        self.last_event_seq = event_seq
        self._changes = {"orders": [], "fills": [], "account_events": [], "decisions": []}
        self.entry_paused = action not in {"resume", "start"}
        if self.entry_paused:
            for order in self._orders_by_id():
                if order["purpose"] == "entry" and order["status"] == "pending":
                    self._cancel(order, "run_" + action)
        when = timestamp(at) if at else timestamp(self.last_clock_time) if self.last_clock_time else datetime.now(timezone.utc)
        self._ledger("run_control", action=action, at=when.isoformat())
        return {**self._changes, "account": self.account(), "positions": copy.deepcopy([p for _, p in sorted(self.positions.items())])}

    def source_status(self, state, event_seq, at=None, symbols=None):
        """Runtime-only connection status, kept out of recognition market clocks.

        The runtime must journal ``at`` so replay uses exactly the same cutoff.
        Disconnection blocks existing pending fills without inventing a quote.
        """
        from datetime import datetime, timezone
        if type(event_seq) is not int or event_seq <= self.last_event_seq:
            raise ValueError("source status sequence must advance")
        when = timestamp(at) if at else timestamp(self.last_clock_time) if self.last_clock_time else datetime.now(timezone.utc)
        known = set(self.sessions) if symbols is None else set(symbols)
        if state in {"DISCONNECTED", "SOURCE_ERROR", "AUTH_REQUIRED", "CAPACITY_BLOCKED"}:
            self.disconnected.update(known)
        elif state == "CONNECTED":
            self.disconnected.difference_update(known)
        else:
            raise ValueError("unsupported paper source status")
        event = MarketEvent(event_id=f"source-status-{event_seq}", symbol=next(iter(sorted(known)), "RUNTIME"), venue="RUNTIME",
                            kind="status", event_time=when, arrival_time=when, payload={"clock": True})
        result = self.on_event(event, [], event_seq)
        self._ledger("source_status", state=state, at=when.isoformat(), symbols=sorted(known))
        # on_event returned the same lists, so the added journal row is included.
        return result
