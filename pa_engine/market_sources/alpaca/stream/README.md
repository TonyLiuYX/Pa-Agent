# Alpaca IEX trade stream

This package is the offline-testable G020 path from one Alpaca Basic IEX stock
trade subscription to exact-Decimal one-second OHLCV.  It fixes the source to
`wss://stream.data.alpaca.markets/v2/iex`, keeps `feed=iex` and
`coverage=single_exchange` in every aggregate, and never claims consolidated
SIP coverage.

Pure parser/reducer imports do not require a WebSocket dependency.  The real
connection path uses the optional `websockets` package when the client is
actually run.  Tests inject `RecordedWebSocketTransport` or
`InMemoryWebSocket`.

The client reconnects at most once by default after a retryable disconnect or
timeout, then re-authenticates and resubscribes.  Its trade registry makes
replayed provider IDs idempotent; it never re-emits an already accepted trade
as a second contribution.

Each bucket's provisional candidate and first frozen execution event use
publication revision `0` with no parent, regardless of how many eligible
trades arrived before the watermark.  Only a post-freeze late trade,
correction, or cancel advances the bucket's retained reconciliation lineage;
each such revision points to the immediately previous published revision while
the frozen execution event remains unchanged.

For a sanitized live smoke, set `APCA_API_KEY_ID` and
`APCA_API_SECRET_KEY` in the process environment and run:

```text
python -m pa_engine.market_sources.alpaca.stream.smoke AAPL --messages 1
```

The command refuses to connect without both variables and prints only typed
status metadata.  Keys are never accepted as arguments, persisted, or logged.
