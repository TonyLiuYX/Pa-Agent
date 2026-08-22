# G025 pending-entry monitor

`PendingEntryMonitor` is the in-memory reducer for one strategy's validated
G018 `wait_for_price` instruction.  It creates exactly one G024 pending entry,
consumes only strict/frozen G020 `SecondAggregateEvent` values, and resolves
the X timer at `expires_at + 3s` so a final eligible second can arrive during
publication grace.  Eligibility is half-open and whole-interval:

```
start_at >= decision_accepted_at and end_at <= expires_at
```

Touches use `low <= requested_entry <= high`, including equality, and fill
through G024 at the exact requested Decimal level.  A fill transition exposes
`exit_eligible_after=end_at`; no same-second exit path is invented.

Confirmed gaps are never interpreted as a touch or no-touch.  At the deadline
the injected recovery callback is invoked at most once.  A strict complete
G019 `RecoveryBar 0.1.0` can fill when its whole minute lies in the window;
straddling/incomplete bars produce a typed `not_evaluated`/`partial_data`
expiry and stable recovery lineage.  Late revisions, duplicate seconds,
duplicate timers and late recovery are idempotent terminal no-ops.

Typical wiring:

```python
monitor = PendingEntryMonitor.from_instruction(
    instruction,
    symbol="AAPL",
    decision_accepted_at=accepted_at,
    submitted_at=submitted_at,
    generation=scheduler_generation,
    paper_account_book=book,
    scheduler=scheduler,
    recovery_callback=recover_once,
)
monitor.on_second(frozen_second)
monitor.on_expiry(clock.now())
```

When a live G023 `StrategyScheduler` is supplied, construction binds a
`PendingEntrySchedulerBridge`. The bridge verifies strategy/pending ID,
generation, state and exact `expires_at`, then claims and cancels G023's
ordinary `pending_expiry` timer. G025 therefore remains the only conditional
entry expiry reducer through `resolution_at`; fill/expiry callbacks are
preflighted and fail closed if the live scheduler record no longer matches.
The bridge uses G023's existing record/timer seam and does not alter scheduler
source or call a model while a pending entry is in its X/grace window.

G024 create/fill/expiry idempotency keys include the immutable pending episode
(`pending_id`, generation, decision-accepted instant and X expiry), so a later
G023 recall with the same instruction can reuse its compatible pending ID
without replaying the previous account transition.

The package has no SQLite, model/provider, broker, network, TP/SL or P&L
implementation.  G027/G028 should persist/route the returned immutable state
and transitions rather than reimplementing touch or timer logic.
