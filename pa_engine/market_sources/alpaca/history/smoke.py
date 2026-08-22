"""Secret-safe, opt-in Alpaca IEX historical-bars smoke command.

The command is intentionally small and does not write a key, response, or
snapshot to disk.  It is useful for G030 only; component acceptance remains
offline with :class:`RecordedTransport`.

Example::

    python -m pa_engine.market_sources.alpaca.history.smoke AAPL \
      --start 2026-08-11T13:30:00Z --end 2026-08-11T14:00:00Z \
      --timeframe 1Min

Credentials are read from ``ALPACA_API_KEY_ID`` and
``ALPACA_API_SECRET_KEY``.  Output contains only a sanitized status/count
summary and never the request URL, query, headers, token, or body.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from .adapter import AlpacaHistoryAdapter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="sanitized Alpaca IEX history smoke")
    parser.add_argument("symbol", help="upper-case US equity symbol")
    parser.add_argument("--start", required=True, dest="start_at", help="UTC RFC3339 start (inclusive)")
    parser.add_argument("--end", required=True, dest="end_at", help="UTC RFC3339 end (exclusive locally)")
    parser.add_argument("--timeframe", default="1Min", help="G014 timeframe, e.g. 1Min or 1Hour")
    parser.add_argument("--limit", type=int, default=1000)
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    adapter = AlpacaHistoryAdapter.from_env()
    result = adapter.fetch_history(symbol=args.symbol, start_at=args.start_at, end_at=args.end_at, timeframe=args.timeframe, limit=args.limit)
    summary = {
        "code": result.outcome.code,
        "classification": result.outcome.classification,
        "retryable": result.outcome.retryable,
        "authentication_terminal": result.outcome.authentication_terminal,
        "status_code": result.outcome.status_code,
        "bars": len(result.bars),
        "feed": "alpaca_iex",
        "boundary": "half_open",
    }
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0 if result.success else (2 if result.outcome.code == "missing_credentials" else 1)


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":  # pragma: no cover
    main()


__all__ = ["build_parser", "main", "run"]
