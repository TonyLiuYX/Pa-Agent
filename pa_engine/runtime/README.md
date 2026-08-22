# G028 Automatic Paper-Trading Runtime

> ## ⚠️ PAPER TRADING ONLY — NO LIVE ORDERS
>
> The G028 runtime is a **simulated** paper-trading loop. It never connects to a
> broker, never sends an order and cannot move real money. Every strategy starts
> with a fixed **USD 1,000.00 simulated account** with zero fees and zero
> slippage. Market data comes from **Alpaca Basic IEX** and is used only as an
> **internal prototype**: no public display or redistribution of the raw data is
> permitted in this phase.

## Overview

`pa_engine.runtime` composes the accepted G015–G027 components into the user's
complete process: one U.S. symbol and one timeframe, an Alpaca Basic IEX
history backfill, a local one-second IEX trade aggregator, one G021 shared
market view, and one independent selected-provider monitor per executable
strategy. Every strategy runs the full decision → pending entry → paper
position → exit/recall cycle against its own durable USD 1,000 account. Each
run selects exactly one analysis provider—DeepSeek V4 Flash or OpenAI
GPT-5.6 Luna—with no fallback, fan-out, review or dual-provider call.

The runtime is a scheduled, stateless-call state machine — not a permanent
process per strategy. State, accounts, pending entries, trades and events live
in one local SQLite database, so a restart resumes exactly where the previous
run stopped.

```
python -m pa_engine.runtime run --symbol AAPL --timeframe 1min --thinking enabled
python -m pa_engine.runtime run --symbol AAPL --provider openai --openai-speed standard
python -m pa_engine.runtime status --db runtime.db
python -m pa_engine.runtime smoke --symbol AAPL
```

## Prerequisites

- Python 3.10+ and the project dependencies (see the repository `pyproject.toml`).
- Environment credentials (read only from the environment, never stored in the
  database and never printed):
  - `DEEPSEEK_API_KEY` — DeepSeek V4 Flash API key (one key serves all strategies).
  - `OPENAI_API_KEY` — OpenAI GPT-5.6 Luna API key (used only when OpenAI is selected).
  - `ALPACA_API_KEY_ID` + `ALPACA_API_SECRET_KEY` — Alpaca Basic credentials for
    IEX history bars and the real-time IEX trade stream.
- One writable SQLite file per run (default `runtime.db`; a fresh file is
  bootstrapped automatically).

Without credentials the runtime still starts, but history backfill fails and
the engine stays in its pre-start state; calls for the selected provider are
classified as authentication failures and strategies stay blocked. The smoke
command reports missing credentials explicitly.

## Usage

### `run` — start the automatic paper runtime

```bash
DEEPSEEK_API_KEY=... ALPACA_API_KEY_ID=... ALPACA_API_SECRET_KEY=... \
  python -m pa_engine.runtime run --symbol AAPL --timeframe 1min
```

Behavior:

1. Validates the configuration (invalid values exit `2` with a JSON error).
2. Wires only the selected analysis adapter, the Alpaca history adapter and the
   IEX stream client from the environment; the unselected provider is neither
   constructed nor called. Missing credentials produce a startup warning on
   stderr and typed startup errors instead of a crash.
3. Prints the sanitized startup report (JSON) to stdout — never credentials.
4. Runs the decision loop; `--max-steps 0` starts and stops without stepping.
5. `Ctrl-C` stops gracefully (lease released, transitions persisted), then
   prints the stop report and the run summary; exit code `0`.
6. A typed runtime error prints `{"error": {"code", "message"}}` on stderr and
   exits `1`.

### `status` — read-only diagnostics of one database

```bash
python -m pa_engine.runtime status --db runtime.db
```

Opens the database read-mostly under a distinct `status-*` run id without
running recovery, prints a sanitized JSON report (database, lease, strategies
with timers, accounts, pending entries, trades, cursors, live-blocked state and
the event count) and exits `0`. A missing/corrupt database prints a JSON error
on stderr and exits `1`. When a live runtime holds the database lease, the
command prints `{"status": "live_lease_held", ...}` (never stealing the lease)
and exits `0`.

### `smoke` — secret-safe provider connectivity check

```bash
DEEPSEEK_API_KEY=... ALPACA_API_KEY_ID=... ALPACA_API_SECRET_KEY=... \
  python -m pa_engine.runtime smoke --symbol AAPL --provider deepseek [--include-stream]

OPENAI_API_KEY=... ALPACA_API_KEY_ID=... ALPACA_API_SECRET_KEY=... \
  python -m pa_engine.runtime smoke --symbol AAPL --provider openai --openai-speed standard [--include-stream]
```

No trading and no database access (the `--db` flag is accepted for parity only
and is never opened). With credentials present it attempts one small Alpaca IEX
history fetch and one ping for the selected analysis provider with a tiny token
budget, and prints only sanitized selected-provider evidence—never keys,
request payloads or raw responses. OpenAI smoke uses enabled reasoning and the
fixed maximum effort. `--include-stream` additionally connects to the IEX
WebSocket and consumes one message. Exit codes are documented in
`pa_engine/runtime/smoke.py`: `0` = at least one attempted provider succeeded,
`1` = every attempted provider failed, `2` = no credentials at all.

## Configuration

| Flag | Default | Meaning |
| --- | --- | --- |
| `--symbol` | *(required)* | Upper-case U.S. equity symbol (one per run). |
| `--timeframe` | `1min` | G014 timeframe: `1min`, `5min`, `15min`, `30min`, `1hour`, `2hour`, `4hour`, `day`, `week`. |
| `--provider` | `deepseek` | Exactly one analysis provider: `deepseek` or `openai`. |
| `--thinking` | `enabled` | DeepSeek reasoning mode: `enabled` or `disabled`. |
| `--openai-speed` | `standard` | OpenAI service tier: `standard` → `default`, `fast` → `fast`; no fixed latency multiplier is promised. DeepSeek rejects `fast`. |
| `--db` | `runtime.db` | SQLite runtime database path (one per run). |
| `--max-steps` | *(unbounded)* | Stop after N loop iterations; `0` = start then stop. |
| `--startup-history-seconds` | `604800` | History backfill window before the first view. |
| `--min-closed-bars` | `3` | Minimum closed bars required for an executable view. |
| `--stale-after-seconds` | `60` | Seconds after which a fresh second is considered stale. |
| `--queue-capacity` | `200` | Scheduler ready-queue capacity. |
| `--max-in-flight` | `40` | Maximum concurrent provider calls per sweep. |
| `--run-id` | *(random)* | Stable run id (default: generated). |
| `--knowledge-dir` | *(packaged)* | G016 knowledge snapshot directory override. |

Provider-derived fixed configuration: DeepSeek uses `deepseek-v4-flash` with
the enabled/disabled thinking switch; OpenAI uses `gpt-5.6-luna` with
`reasoning.effort=max`, strict structured output, `store=false` and stateless
Responses API calls. No model or reasoning-effort selector is exposed. Other
fixed settings are the regular `America/New_York` session, raw/unadjusted data,
`feed=alpaca_iex`, and `market_data_usage=internal_prototype`.

## Data-path notice

- Market data is **Alpaca Basic IEX only** — a single-exchange feed, not
  consolidated SIP data. History, seconds, views and paper events are all
  labeled IEX-only; an IEX/SIP mismatch is rejected.
- The data path is `internal_prototype`: Phase 2 consumes the feed **inside the
  local runtime only**. No public display, export or redistribution surface
  exists. Customer-facing market-data display requires a separate Phase 3
  licensing/terms decision.

## Paper-trading notice

- Each `strategy_id` owns one independent simulated account, initialized once
  at **USD 1,000.00**, with zero fees and zero slippage. Balances are durable,
  never shown to DeepSeek and never netted across strategies.
- Different strategies may hold the same symbol in the same or opposite
  directions; they are recorded separately.
- The runtime's normal exit does not liquidate simulated positions: open
  trades and pending entries persist and resume on the next run.

## Failure modes

- **`market_wait`** — startup history is insufficient, stale, partial or
  gapped; or a fresh frozen second has not arrived. No model call and no fill
  happens until the market view is executable again (`allow_partial_data=false`
  is fixed).
- **`recovery_required` / recovery pending** — a feed gap or restart blocked
  the runtime; G019 one-minute recovery bars are fetched and processed
  chronologically before live seconds resume. A recovery bar may trigger a
  pending level only inside the decision's X window; straddling bars expire
  as not-evaluated. Incomplete recovery keeps the runtime blocked and never
  assumes a trade.
- **`blocked` / `exhausted`** — configuration/credential/balance failures stop
  the strategy until the configuration changes; an account that loses its
  capital stops permanently and is never auto-recapitalized.
- **Provider failures** — timeout/rate-limit are retried with the bounded G023
  schedule (5/10/20/40/60 seconds); authentication, insufficient balance,
  invalid request and malformed responses are terminal and produce typed
  abstentions, never fills.

## Graceful stop / restart semantics

- `stop()` releases the database lease, persists pending transitions and stops
  the feed; a later `python -m pa_engine.runtime run` with the same database
  resumes balances, pending entries, open trades, timers and schedules.
- Restart recovery starts from the persisted second cursor: the runtime
  backfills from the cursor, marks the connection gap, applies G019 minute-bar
  recovery with the documented ambiguity rules, and only then establishes a
  new cutover and resumes live seconds.
- N/X wall-clock deadlines survive restarts: an overdue N becomes due once
  after market recovery; X is never extended beyond its exact deadline.
- One live runtime may hold a database at a time (singleton lease). A second
  concurrent `run` fails with a typed lease-conflict error; `status` reports
  `live_lease_held` without stealing the lease.
