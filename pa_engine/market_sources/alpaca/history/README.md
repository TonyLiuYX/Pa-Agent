# Alpaca IEX historical bars (G019)

`AlpacaHistoryAdapter` calls the official single-symbol endpoint
`/v2/stocks/{symbol}/bars` with fixed `feed=iex`, `adjustment=raw`, and
`sort=asc`.  Requests use an explicit `start`/`end` interval and the adapter
filters the vendor's documented inclusive `end` locally to `[start,end)`.
Only `us_equity`/`USD`, `regular`, `America/New_York` bars are retained;
extended-hours bars are omitted.  DST conversion uses `zoneinfo`.
Timeframes follow Alpaca's single-symbol grammar: `1..59Min`, `1..23Hour`,
`1Day`, and `1Week` (months and larger multipliers fail closed).  Minute/hour
bar ends use elapsed UTC durations; day/week aggregate ends use the next local
America/New_York calendar day/Monday boundary, so their UTC length changes at
DST without overlapping the following bar.

The adapter accepts an injected `RecordedTransport` for offline tests and
defaults to a standard-library `UrllibTransport`.  `fetch_history` and
`fetch_recovery` return a `HistoryResult` with a stable `AdapterOutcome`; use
`strict=True` or `raise_for_error()` when an exception boundary is preferred.
Authentication values are read at runtime only and are never serialized.

Recovery uses a bounded one-minute request (maximum seven days).  Each
`RecoveryBar` is a separate internal `0.1.0` value with exact Decimal text,
stable ID/hash, source-request hash, cursor, IEX feed identity, and
`path_precision=minute_bar_no_path`.  A partial/incomplete value validates as
an observation but `is_trade_trigger_eligible()` is always false.

For the credentialed smoke reserved for G030:

```bash
export ALPACA_API_KEY_ID='…'
export ALPACA_API_SECRET_KEY='…'
python -m pa_engine.market_sources.alpaca.history.smoke AAPL \
  --start 2026-08-11T13:30:00Z --end 2026-08-11T14:00:00Z --timeframe 1Min
```

The command prints only a compact status/count summary and performs no file
writes.  Missing credentials fail with a typed `missing_credentials` result.
