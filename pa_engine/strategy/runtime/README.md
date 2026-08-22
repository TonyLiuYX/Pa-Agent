# G023 scheduler/runtime reference

`StrategyScheduler` is a deterministic event-driven state machine.  It owns
one `StrategyRecord` per executable `strategy_id` and exposes typed
`AnalysisJob` values through a bounded `WorkQueue`.  External workers call
`claim_jobs()` and return a finalized G018 scheduler instruction with
`complete_job(job_id, instruction, decision_accepted_at=...)`.  No thread,
sleep, Alpaca call, DeepSeek call or account calculation occurs here.

States are `analysis_due`, `analyzing`, `sleeping`, `waiting_entry`,
`open_position`, `retry_wait`, `market_wait`, `blocked`, `disabled` and
`exhausted`.  Every job and timer carries the strategy generation; disabling
or invalidating increments it, cancels local work, and makes late/duplicate
responses no-ops.  `market_wait` stores the intended state and resumes only on
a complete fresh regular-session view.  Waiting/open states never call the
model on market updates.

Valid G018 branches map as follows:

* `no_entry`: sleep until `decision_accepted_at + N`;
* `wait_for_price`: register one pending entry until `+ X` and wait for the
  G025 fill/expiry callback;
* `enter_immediately`: call the injected G024 open requester and enter
  `open_position` only after acceptance.  A fresh-price rejection gates the
  next call on a different complete shared-view ID.

Provider retry delays are 5/10/20/40/60 seconds, capped at 60.  Malformed or
grounding failures retry 5/10/20 seconds per profile/snapshot key and then
block.  Authentication, insufficient-balance and invalid-configuration
failures block until `configuration_changed()`.  Account/exhaustion and
transition logs are compact and contain no account, balance, quantity, P&L or
R fields.

The defaults are `queue_capacity=200` and `max_in_flight=40`; they are
independent limits.  `FakeClock.advance()` changes time only; call
`process_timers()` explicitly to process due wakeups.
