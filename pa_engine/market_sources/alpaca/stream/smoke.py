"""Secret-safe Alpaca IEX WebSocket smoke command.

Credentials are read only from ``APCA_API_KEY_ID`` and
``APCA_API_SECRET_KEY``.  The command never accepts keys as arguments, writes
them to disk, or prints them.  Without credentials it exits deterministically
with a sanitized typed result and does not attempt a network connection.
"""

from __future__ import annotations

import argparse
import json
from typing import Sequence

from .client import AlpacaIEXClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="sanitized Alpaca IEX trade-stream smoke")
    parser.add_argument("symbol", help="upper-case US equity symbol")
    parser.add_argument("--messages", type=int, default=1, help="maximum messages to consume")
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = AlpacaIEXClient.from_env(args.symbol)
    if not client.credentials_present:
        print(json.dumps({"state": "error", "kind": "authentication", "ok": False, "message": "Alpaca authentication failed", "feed": "alpaca_iex"}, sort_keys=True, separators=(",", ":")))
        return 2
    status = client.run(max_messages=max(0, args.messages))
    print(json.dumps(status.to_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if status.outcome.ok else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":  # pragma: no cover
    main()


__all__ = ["build_parser", "main", "run"]
