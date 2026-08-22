# pa_engine.storage — G027 SQLite runtime persistence

Operations and reference notes for the durable runtime layer. This is not a
design document; it is the manual for operating the database the runtime
creates.

## 1. Purpose and versioned scope

`pa_engine.storage` persists the automatic paper-trading runtime state in one
local SQLite database:

- one paper account row per `strategy_id` (initialized once at `1000.00`),
- one materialized strategy/timer snapshot per strategy,
- pending-entry and trade rows,
- the append-only runtime event stream, and
- the per-symbol market cursor with its recovery state.

The event stream is versioned: every event is one
`RuntimeEventEnvelope 0.1.0` with a closed vocabulary of 12 event types
(`strategy_state_changed`, `model_decision_accepted`, `schedule_set`,
`pending_entry_created`, `pending_entry_expired`, `pending_entry_cancelled`,
`paper_fill`, `paper_fill_rejected`, `paper_close`, `account_changed`,
`account_exhausted`, `market_recovery`). Every envelope runs in one of two
modes: `automatic` (live run) or `replay` (deterministic re-run). Identity is
content-bound: `event_hash` is SHA-256 of the canonical content projection and
`event_id` is `rev_` + that hash.

## 2. Database location convention

There is exactly one database file per runtime and it is explicitly configured
by the caller: `open_database(path, run_id=..., mode=...)` receives the path and
bootstraps a missing path with the v1 schema. An existing zero-length file is
treated as corruption, so the storage layer never
invents a default location; the caller owns the path (for example a
run-scoped file inside a state directory).

Every connection is opened with the same pragmas:

- `PRAGMA journal_mode=WAL` — concurrent reader/writer operation,
- `PRAGMA synchronous=FULL` — durable commits (no torn transactions),
- `PRAGMA foreign_keys=ON`,
- `PRAGMA busy_timeout=5000`.

## 3. Backup procedure

Take backups with the SQLite online backup API while the runtime still holds
its lease, for example from the `sqlite3` CLI:

```
sqlite3 run.db ".backup 'run.backup.db'"
```

`.backup` produces a consistent snapshot even while WAL frames are
un-checkpointed. Alternatively, close the database handle first (releasing the
lease) and then copy the file. When copying files directly you must copy the
WAL sidecar too: the database bytes in `run.db` can lag behind committed
transactions held in `run.db-wal`. Never copy only `run.db` while its WAL
contains un-checkpointed frames, or the copy silently misses the most recent
committed state.

## 4. Active-run lease

One live runtime owns the database. The singleton `lease` row records the
active `run_id`, `mode`, acquisition time, heartbeat and TTL.

- Default TTL is 30 seconds (`LEASE_DEFAULT_TTL_SECONDS`).
- A heartbeat refreshes the lease; heartbeating is not allowed inside an open
  transaction.
- A second opener while the current lease is unexpired is rejected with
  `LeaseConflictError` (a duplicate live handle is a bug, even for the same
  `run_id`).
- A stale lease (heartbeat older than its TTL) is atomically replaced exactly
  once by the next opener — that is how a crashed run is taken over.

## 5. Restart/recovery procedure

After a crash, restart like this:

1. `open_database(path, run_id=new_run_id, mode=mode, clock=...)` — acquires
   the lease (stale lease replacement happens here).
2. `RuntimeRecovery(store, db).startup_sequence(enabled_strategy_ids)` — this
   ensures one account row per enabled strategy and then runs one restore.
3. Read the returned `RestoreSnapshot`: accounts, strategy states, timers,
   pending entries, trades, cursors, `due_once` work, `retry_conversions`,
   `market_paused` strategies and the `live_blocked` gate.

The restore derives the minimal corrections and commits them with their linked
events in exactly one transaction:

- An `analyzing` strategy with an in-flight job converts to `retry_wait` with
  the bounded 5/10/20/40/60-second retry schedule (`RETRY_DELAYS`, capped at
  60); the conversion is listed in `retry_conversions`.
- An overdue `sleeping`/`retry_wait` timer fires `sleep_due`/`retry_due` once
  and the strategy moves to `analysis_due`.
- A `waiting_entry` strategy whose pending row expired yields
  `pending_expiry_due`; a strategy whose pending row is terminal or missing is
  reconciled to `analysis_due` (no fabricated pending events).
- An `open_position` strategy whose trade row is settled/missing is reconciled
  to `analysis_due`; a strategy in `analysis_due`/`sleeping`/`retry_wait` with
  an open trade row is restored to `open_position`.
- Before live processing resumes, complete the recovery: commit chronological
  minute `RecoveryBar` refs through `commit_recovery` (which clears the
  per-symbol live-block) and only then process live seconds. `live_blocked` in
  the snapshot tells the caller that live consumption must wait.

`due_once` is exact-once: a repeated restore sees the same state as finished
and produces no further events. Accounts are never reset by any restore;
balances and initial balances pass through untouched.

## 6. Secret exclusions

Durable storage rejects secret material instead of sanitizing it:

- credential-shaped fields (`api_key`, `token`, `password`, `credential`,
  authorization-like names and their suffix variants) are rejected by
  envelope validation,
- reasoning/thinking content (`reasoning`, `thinking`, `chain_of_thought`,
  `thoughts`, ...) is rejected,
- broker-order/real-order fields (`broker_order_id`, `real_order`, ...) are
  rejected,
- provider raw payload/response fields are rejected, including in materialized
  JSON columns rather than only in envelopes.

The check runs recursively over the whole envelope (payload, profile ref,
decision lineage, market ref). A field named `reason` is intentionally never
blocked. Nothing is silently scrubbed: a secret-bearing envelope fails
validation, and the error message refers to field paths only. The documented
field vocabularies (closed event types and payload key sets) are part of
`RuntimeEventEnvelope 0.1.0`.

## 7. Event stream notes

The event stream is append-only in one table with a unique `event_hash` and a
unique idempotency key. Corrections are expressed as new linked events, never
by rewriting a recorded one (G020 revisions/reconciliation follow the same
rule upstream). Re-delivery semantics are idempotency-based:

- appending an envelope that already exists byte-for-byte returns `replayed`
  and changes nothing,
- the same idempotency key with different content raises
  `StorageConflictError`,
- materialized-state commits (account change, trade, pending entry, schedule,
  recovery) are atomic with their linked event in one transaction, so a failed
  commit rolls back both the row and the event.

## 8. Corruption/version handling

Every open of an existing database verifies three pins before any use:
`PRAGMA integrity_check`, schema identity (`PRAGMA application_id`,
`PRAGMA user_version`, the `meta` row `schema_version`) and the closed
vocabulary/invariant probes over materialized rows. Failures are typed:

- `StorageCorruptionError` (code `corruption`) for failed integrity checks or
  violated invariants,
- `StorageVersionError` (code `incompatible_version`) for a foreign or future
  schema identity.

A failed open closes the connection without modifying the file. Storage
failures never auto-reset, auto-delete or recreate state; recovery from a
corrupt file is an operator decision (restore a backup), not a runtime
behavior.
