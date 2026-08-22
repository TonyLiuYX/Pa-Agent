# G026 exit monitor and settlement policy

`ExitMonitor` consumes one G024 `PaperTrade`, execution-frozen G020
`SecondAggregateEvent` values and, only while reconciling a confirmed gap,
complete G019 `RecoveryBar` values.  It has no model, persistence, broker or
network boundary.

For a long position, `low <= stop` is a stop and `high >= target` is a target.
For a short position, `high >= stop` is a stop and `low <= target` is a target.
All comparisons are inclusive and the selected fill is the configured exact
level (never the OHLC extreme).  If one second touches both levels, G026
selects stop first and records the internal `ambiguity_reason`
`dual_touch_stop_first`.  When a result needs a G014-compatible uncertainty
projection it uses only the existing `high_uncertainty` code.

The entry-fill interval is never evaluated as an exit interval.  Frozen
seconds are accepted in event-time watermark order; duplicates and revisions
older than the processed watermark are typed replays/rejections.  A confirmed
gap leaves the position open with `outcome_status=not_evaluated` and
`partial_data=true`.  No favorable exit is invented from a gap.  Monitoring
resumes only after a complete, identity-bound recovery bar is consumed.

G026 accepts both finite and open-ended confirmed gaps.  An open-ended gap is
`not_evaluated`/`partial_data` and remains active until a same-start finite
`close_gap` update; recovery cannot settle it while open.  Every finite
recovery batch is preflighted as one unit: if a G019 `HistoryRequest`/result
envelope is supplied, its source hash, symbol, feed, and half-open
`start_at`/`end_at` must be covered by the batch edges (a bar may conservatively
extend an edge, but every bar must intersect the request).  Sorted complete
bars must also begin at or before the earliest active gap, end at or after the
latest active gap, and form one contiguous union with no holes.  A
far-future, before-gap, malformed, late, incomplete, missing-head/middle/tail,
or partial-coverage candidate makes the whole batch `not_evaluated`/
`partial_data`; no bar in that batch is touched or can settle the trade.
The request cursor is an envelope cursor: each RecoveryBar keeps its own
`recovery_cursor == end_at`, and only the final bar may match an outer cursor.
Every gap start/end is first parsed as a strict canonical UTC instant; invalid
or timezone-less `ConnectionGap` values are typed `invalid_gap` rejections and
cannot append state or emit an event.  A terminal monitor ignores late
`close_gap` updates as immutable replays.
After successful recovery the active-gap worklist is cleared, so a later
unrelated gap is evaluated independently.

Recovery bars retain `recovery_bar_id`, content hash, source request/cursor,
and `path_precision=minute_bar_no_path`; a dual touch remains stop-first and
is marked high uncertainty because a minute bar has no intrabar path.

G024 settlement is the first state-changing boundary.  An attached
`paper_account_book` is authoritative: supplying a distinct settlement
override is rejected, and after `close_trade` G026 re-reads the same account
to verify the matching strategy, no open trade, and exact closing balance.  A
standalone settlement callback is not executable because its funds cannot be
verified.  The live account's full
G024 trade projection must match the monitor lineage.  Its idempotency key is
content-derived from strategy, trade, monitor episode, selected exit, reason
and source event.  Only after a changed G024 result (with a matching closed
trade, account-without-open-position, and settlement mapping) does G026 make
one typed `on_position_close` callback to G023.  A false or throwing callback
is recorded in `scheduler_callback` and cannot undo the accepted paper
settlement.  A fresh or independent monitor's real G024 idempotent replay is
terminal but callback-free.
Touching seconds and RecoveryBars are added to processed lineage only after
G024 returns an accepted changed result or an exact idempotent replay.  A
boundary exception, rejection, malformed result, authority mismatch, or
scheduler preflight failure leaves the trigger unconsumed so the same event
can retry with the same idempotency key.  The returned terminal transition is
refreshed after this lineage commit and has the exact same state as the
monitor.  An unknown outcome retains an immutable in-memory attempt (source,
selected level, reason, key and original commit context); every later second,
recovery delivery, timer, or gap operation retries that attempt first.  While
it remains unresolved, the newer input cannot advance watermarks, lineage, or
settle another level.  Fresh monitors still treat a G024 replay as
callback-free.

When a G023 scheduler is attached, its strategy, position ID, original
position generation, state and live callback are preflighted both at monitor
construction and immediately before G024.  A stale or missing binding rejects
before account mutation.  Without a scheduler, an explicit callable callback
is required before settlement; no callback is treated as a typed missing
boundary.  Disabled scheduler records retain their open-position generation
long enough for the close callback to clear the position; the scheduler then
chooses disabled/exhausted/no-recall behavior.
An injected `ExitSchedulerBridge` must be the frozen bridge type bound to the
same strategy, trade and generation (and, when both are supplied, the same
scheduler object); cross-position bridges are rejected before G024.

The compatibility `settle`/`close_trade`/`close_position` methods are typed but
non-executable (`direct_settlement_unsupported`): callers cannot select a
configured level without a validated frozen second or eligible recovery bar.
Confirmed gaps also emit an immutable deterministic internal `ExitEvent` with
the exact interval, reason, `not_evaluated` status and `partial_data` marker;
this event is not a G027 persistence envelope.
