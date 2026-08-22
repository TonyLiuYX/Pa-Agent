"""G028/G031 automatic paper-trading runtime CLI: ``run``/``status``/``smoke``.

Every command output carries the frozen paper-only label and the
``internal_prototype`` market-data usage boundary.  Credential values are
never printed: reports are serialized through the G017 redaction helpers and
runtime secrets come only from the process environment.

Exit codes (documented in the README and in each subcommand):

- ``0`` — clean run/status or a smoke that executed without an infrastructure
  failure with at least one successful provider;
- ``1`` — typed runtime/storage error (or a smoke where every attempted
  provider failed);
- ``2`` — invalid configuration, or a smoke with no credentials at all.

The ``run`` command wires the G028 engine from the environment with injectable
adapters; ``status`` reads one SQLite database read-mostly under a distinct
``status-*`` run id and never runs recovery; ``smoke`` performs a tiny
secret-safe provider check without any trading or database access.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from typing import Any, Callable, Mapping, Sequence

from pa_engine.market_sources.alpaca.history.adapter import AlpacaHistoryAdapter
from pa_engine.market_sources.alpaca.stream.aggregate import LocalSecondAggregator
from pa_engine.market_sources.alpaca.stream.client import AlpacaIEXClient
from pa_engine.providers.base import redacted_json
from pa_engine.providers.deepseek.adapter import DeepSeekAdapter
from pa_engine.providers.openai.adapter import OpenAIAdapter
from pa_engine.providers.openai.config import OpenAIConfig
from pa_engine.runtime.config import RuntimeConfig, RuntimeConfigError
from pa_engine.runtime.engine import AutomaticPaperRuntime, RuntimeEngineError
from pa_engine.runtime.smoke import run_smoke, smoke_json
from pa_engine.storage import (
    LeaseConflictError,
    RuntimeStore,
    StorageError,
    open_database,
)
from pa_engine.strategies.knowledge import load_default_registry
from pa_engine.strategy.runtime.clock import SystemClock

DEEPSEEK_ENV = "DEEPSEEK_API_KEY"
OPENAI_ENV = "OPENAI_API_KEY"
ALPACA_KEY_ID_ENV = "ALPACA_API_KEY_ID"
ALPACA_SECRET_ENV = "ALPACA_API_SECRET_KEY"
CREDENTIAL_ENV = (DEEPSEEK_ENV, OPENAI_ENV, ALPACA_KEY_ID_ENV, ALPACA_SECRET_ENV)

STATUS_MODE = "automatic"
STATUS_RUN_ID_PREFIX = "status-"

# Frozen v1 surface from the G028 runtime configuration; computed once so the
# CLI label can never drift from ``RuntimeConfig.paper_only_label``.
PAPER_ONLY_LABEL = RuntimeConfig(symbol="AAPL").paper_only_label()
MARKET_DATA_USAGE = RuntimeConfig(symbol="AAPL").market_data_usage


def _env_secrets() -> tuple[str, ...]:
    return tuple(value for name in CREDENTIAL_ENV if (value := os.environ.get(name)))


def _print_error(code: str, message: str) -> None:
    sanitized = _sanitize_message(str(message))
    print(
        json.dumps(
            {"error": {"code": code, "message": sanitized}, "paper_only": True},
            sort_keys=True,
            ensure_ascii=False,
        ),
        file=sys.stderr,
    )


def _sanitize_message(message: str) -> str:
    for secret in _env_secrets():
        if secret:
            message = message.replace(secret, "***")
    return message


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pa_engine.runtime",
        description="G028 automatic paper-trading runtime (one symbol/timeframe).",
        epilog=(
            "PAPER TRADING ONLY — no live orders. "
            "Market data: Alpaca Basic IEX, internal_prototype "
            "(no public display/redistribution)."
        ),
    )
    parser.add_argument(
        "--knowledge-dir",
        default=None,
        help="directory containing the G016 knowledge snapshot (default: packaged registry)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_run_parser(subparsers)
    _add_status_parser(subparsers)
    _add_smoke_parser(subparsers)
    return parser


def _add_run_parser(subparsers: Any) -> None:
    run_parser = subparsers.add_parser(
        "run",
        help="start the automatic paper runtime for one symbol/timeframe",
        epilog=(
            "PAPER TRADING ONLY — no live orders. "
            "Market data: Alpaca Basic IEX, internal_prototype "
            "(no public display/redistribution)."
        ),
    )
    run_parser.add_argument("--symbol", required=True, help="upper-case US equity symbol")
    run_parser.add_argument("--timeframe", default="1min", help="G014 timeframe, e.g. 1min, 5min, 1hour, day")
    run_parser.add_argument("--provider", choices=("deepseek", "openai"), default="deepseek")
    run_parser.add_argument("--thinking", choices=("enabled", "disabled"), default="enabled")
    run_parser.add_argument("--openai-speed", choices=("standard", "fast"), default="standard", help="OpenAI service tier (only with --provider openai)")
    run_parser.add_argument("--db", default="runtime.db", help="SQLite runtime database path")
    run_parser.add_argument("--max-steps", type=int, default=None, help="stop after N loop iterations (0 = start then stop)")
    run_parser.add_argument("--startup-history-seconds", type=int, default=7 * 86400)
    run_parser.add_argument("--min-closed-bars", type=int, default=3)
    run_parser.add_argument("--stale-after-seconds", type=int, default=60)
    run_parser.add_argument("--queue-capacity", type=int, default=200)
    run_parser.add_argument("--max-in-flight", type=int, default=40)
    run_parser.add_argument("--run-id", default=None, help="stable run id (default: random)")


def _add_status_parser(subparsers: Any) -> None:
    status_parser = subparsers.add_parser(
        "status",
        help="read-only diagnostic of one runtime database",
        epilog=(
            "PAPER TRADING ONLY — no live orders. "
            "Market data: Alpaca Basic IEX, internal_prototype "
            "(no public display/redistribution)."
        ),
    )
    status_parser.add_argument("--db", required=True, help="existing SQLite runtime database path")


def _add_smoke_parser(subparsers: Any) -> None:
    smoke_parser = subparsers.add_parser(
        "smoke",
        help="secret-safe provider connectivity check (no trading, no database)",
        epilog=(
            "PAPER TRADING ONLY — no live orders. "
            "Market data: Alpaca Basic IEX, internal_prototype "
            "(no public display/redistribution)."
        ),
    )
    smoke_parser.add_argument("--symbol", required=True, help="upper-case US equity symbol")
    smoke_parser.add_argument("--timeframe", default="1min", help="G014 timeframe, e.g. 1min, 5min")
    smoke_parser.add_argument("--provider", choices=("deepseek", "openai"), default="deepseek")
    smoke_parser.add_argument("--thinking", choices=("enabled", "disabled"), default=None, help="reasoning mode; omitted uses disabled for DeepSeek and enabled for OpenAI")
    smoke_parser.add_argument("--openai-speed", choices=("standard", "fast"), default="standard", help="OpenAI service tier (only with --provider openai)")
    smoke_parser.add_argument("--db", default=None, help="accepted for parity; never opened")
    smoke_parser.add_argument("--include-stream", action="store_true", help="also attempt one IEX WebSocket receive")
    smoke_parser.add_argument("--timeout", type=float, default=60.0, help="provider timeout in seconds")


def _default_engine_factory(config: RuntimeConfig, **kwargs: Any) -> AutomaticPaperRuntime:
    return AutomaticPaperRuntime(config, **kwargs)


def main(
    argv: Sequence[str] | None = None,
    *,
    engine_factory: Callable[..., Any] | None = None,
    smoke_adapters: Mapping[str, Any] | None = None,
) -> int:
    """Run the CLI; returns the documented process exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        return _run_command(args, engine_factory=engine_factory)
    if args.command == "status":
        return _status_command(args)
    if args.command == "smoke":
        return _smoke_command(args, adapters=smoke_adapters)
    parser.print_help()
    return 0


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def _run_command(args: argparse.Namespace, *, engine_factory: Callable[..., Any] | None) -> int:
    try:
        config = RuntimeConfig.from_kwargs(
            symbol=args.symbol,
            timeframe=args.timeframe,
            reasoning_mode=args.thinking,
            database_path=args.db,
            provider=args.provider,
            openai_speed=args.openai_speed,
            minimum_closed_bars=args.min_closed_bars,
            stale_after_seconds=args.stale_after_seconds,
            queue_capacity=args.queue_capacity,
            max_in_flight=args.max_in_flight,
        )
    except RuntimeConfigError as exc:
        _print_error(exc.code, exc)
        return 2

    secrets = _env_secrets()
    adapter = _analysis_adapter_from_env(config, secrets)
    aggregator = LocalSecondAggregator(args.symbol)
    history_adapter, stream_client = _alpaca_adapters_from_env(args.symbol, secrets, aggregator=aggregator)

    registry = load_default_registry(args.knowledge_dir) if args.knowledge_dir else None
    factory = engine_factory if engine_factory is not None else _default_engine_factory
    engine = factory(
        config,
        registry=registry,
        adapter=adapter,
        history_adapter=history_adapter,
        aggregator=aggregator,
        stream_client=stream_client,
        db_path=config.database_path,
        run_id=args.run_id,
        startup_history_seconds=args.startup_history_seconds,
        max_output_tokens=4096,
        clock=SystemClock(),
    )

    try:
        startup = engine.start()
    except RuntimeEngineError as exc:
        _print_error(exc.code, exc)
        return 1
    except StorageError as exc:
        _print_error(exc.code, exc)
        return 1
    print(redacted_json(startup, secret_values=secrets))
    for entry in startup.get("errors", []):
        message = entry.get("message") if isinstance(entry, Mapping) else entry
        print(f"runtime startup error: {_sanitize_message(str(message))}", file=sys.stderr)

    if args.max_steps == 0:
        stop_report = engine.stop(reason="started_by_request")
        print(redacted_json(stop_report, secret_values=secrets))
        summary: dict[str, Any] = {"steps": 0, "final_status": engine.status()}
        print(redacted_json(summary, secret_values=secrets))
        return 0

    try:
        summary = engine.run(max_steps=args.max_steps)
    except KeyboardInterrupt:
        stop_report = engine.stop(reason="interrupted")
        print(redacted_json(stop_report, secret_values=secrets))
        print(redacted_json({"steps": None, "interrupted": True}, secret_values=secrets))
        return 0
    except RuntimeEngineError as exc:
        _print_error(exc.code, exc)
        try:
            engine.stop(reason="engine_error")
        except Exception:
            pass
        return 1
    except StorageError as exc:
        _print_error(exc.code, exc)
        try:
            engine.stop(reason="storage_error")
        except Exception:
            pass
        return 1
    stop_report = engine.stop(reason="cli_stop")
    print(redacted_json(stop_report, secret_values=secrets))
    print(redacted_json(summary, secret_values=secrets))
    return 0


def _analysis_adapter_from_env(config: RuntimeConfig, secrets: tuple[str, ...]) -> Any:
    """Construct only the immutable run-selected provider adapter."""

    del secrets  # redaction is handled by the caller; selection is config-bound.
    if config.provider == "deepseek":
        if os.environ.get(DEEPSEEK_ENV):
            return DeepSeekAdapter.from_env()
        print(
            "runtime warning: DeepSeek credentials are not set; model calls will be "
            "classified as authentication failures and strategies stay blocked",
            file=sys.stderr,
        )
        return DeepSeekAdapter()
    if os.environ.get(OPENAI_ENV):
        return OpenAIAdapter.from_env(config=OpenAIConfig(speed=config.openai_speed))
    print(
        "runtime warning: OpenAI credentials are not set; model calls will be "
        "classified as authentication failures and strategies stay blocked",
        file=sys.stderr,
    )
    return OpenAIAdapter(config=OpenAIConfig(speed=config.openai_speed))


def _alpaca_adapters_from_env(
    symbol: str,
    secrets: tuple[str, ...],
    *,
    aggregator: LocalSecondAggregator | None = None,
) -> tuple[Any, Any]:
    key_id = os.environ.get(ALPACA_KEY_ID_ENV)
    secret = os.environ.get(ALPACA_SECRET_ENV)
    if secrets and key_id and secret:
        return AlpacaHistoryAdapter.from_env(), AlpacaIEXClient(
            symbol,
            key_id=key_id,
            secret_key=secret,
            aggregator=aggregator,
        )
    print(
        "runtime warning: Alpaca credentials are not set; history backfill will "
        "fail and the runtime stays pre-start",
        file=sys.stderr,
    )
    return AlpacaHistoryAdapter(), AlpacaIEXClient(symbol, aggregator=aggregator)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def _status_command(args: argparse.Namespace) -> int:
    path = args.db
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        _print_error("database_missing", f"database not found or empty: {path}")
        return 1
    secrets = _env_secrets()
    run_id = f"{STATUS_RUN_ID_PREFIX}{uuid.uuid4().hex}"
    try:
        db = open_database(path, run_id=run_id, mode=STATUS_MODE)
    except LeaseConflictError as exc:
        lease = _lease_row_elsewhere(path, holder=exc.holder)
        report = {
            "status": "live_lease_held",
            "database": path,
            "lease": lease,
            "paper_only": PAPER_ONLY_LABEL,
            "market_data_usage": MARKET_DATA_USAGE,
        }
        print(redacted_json(report, secret_values=secrets))
        return 0
    except StorageError as exc:
        _print_error(exc.code, exc)
        return 1
    try:
        store = RuntimeStore(db)
        report = {
            "database": path,
            "mode": STATUS_MODE,
            "lease": {
                "held": True,
                "run_id": db.lease.run_id,
                "mode": db.lease.mode,
            },
            "strategies": _status_strategies(store),
            "accounts": _status_accounts(store),
            "pending_entries": _status_pending(store),
            "trades": _status_trades(store),
            "cursors": _status_cursors(store),
            "live_blocked": _status_live_blocked(store),
            "events_count": len(store.list_events()),
            "paper_only": PAPER_ONLY_LABEL,
            "market_data_usage": MARKET_DATA_USAGE,
        }
        print(redacted_json(report, secret_values=secrets))
        return 0
    except StorageError as exc:
        _print_error(exc.code, exc)
        return 1
    except Exception as exc:
        _print_error("database_error", f"database read failed: {_sanitize_message(str(exc))}")
        return 1
    finally:
        try:
            db.close(release_lease=True)
        except Exception:
            pass


def _lease_row_elsewhere(path: str, *, holder: str | None) -> dict[str, Any]:
    """Report the live-lease holder without taking the lease over.

    The raw row is intentionally not re-read with a direct SQLite connection
    (G027's import boundary keeps ``sqlite3`` inside the storage package);
    the holder identity from the typed conflict is sufficient for the local
    status diagnostic.
    """
    return {"held": True, "run_id": holder, "mode": None}


def _status_strategies(store: RuntimeStore) -> list[dict[str, Any]]:
    strategies: list[dict[str, Any]] = []
    for row in store.list_strategy_states():
        timers = [
            {
                "kind": timer["kind"],
                "generation": timer["generation"],
                "due_at": timer["due_at"],
                "active": bool(timer["active"]),
                "fired": bool(timer["fired"]),
            }
            for timer in store.get_timers(row["strategy_id"])
        ]
        strategies.append(
            {
                "strategy_id": row["strategy_id"],
                "state": row["state"],
                "enabled": bool(row["enabled"]),
                "exhausted": bool(row["exhausted"]),
                "generation": row["generation"],
                "pending_id": row["pending_id"],
                "open_position_id": row["open_position_id"],
                "blocked_reason": row["blocked_reason"],
                "timers": timers,
            }
        )
    return strategies


def _status_accounts(store: RuntimeStore) -> list[dict[str, Any]]:
    return [
        {
            "strategy_id": row["strategy_id"],
            "exhausted": bool(row["exhausted"]),
            "pending": bool(row["pending"]),
            "open": row["open_trade"] is not None,
        }
        for row in store.list_accounts()
    ]


def _status_pending(store: RuntimeStore) -> list[dict[str, Any]]:
    return [
        {
            "pending_id": row["pending_id"],
            "strategy_id": row["strategy_id"],
            "status": row["status"],
            "symbol": row["symbol"],
            "direction": row["direction"],
            "entry_price": row["entry_price"],
            "stop_price": row["stop_price"],
            "target_price": row["target_price"],
            "expires_at": row["expires_at"],
        }
        for row in store.list_pending_entries()
    ]


def _status_trades(store: RuntimeStore) -> list[dict[str, Any]]:
    return [
        {
            "trade_id": row["trade_id"],
            "strategy_id": row["strategy_id"],
            "status": row["status"],
            "symbol": row["symbol"],
            "direction": row["direction"],
            "entry_price": row["entry_price"],
            "stop_price": row["stop_price"],
            "target_price": row["target_price"],
            "opened_at": row["opened_at"],
            "closed_at": row["closed_at"],
            "exit_reason": row["exit_reason"],
        }
        for row in store.list_trades()
    ]


def _status_cursors(store: RuntimeStore) -> list[dict[str, Any]]:
    return [
        {
            "symbol": row["symbol"],
            "last_second_event_id": row["last_second_event_id"],
            "last_second_end_at": row["last_second_end_at"],
            "live_blocked": bool(row["live_blocked"]),
            "recovery_cursor": row["recovery_cursor"],
        }
        for row in store.list_cursors()
    ]


def _status_live_blocked(store: RuntimeStore) -> bool:
    return any(bool(row["live_blocked"]) for row in store.list_cursors())


# ---------------------------------------------------------------------------
# smoke
# ---------------------------------------------------------------------------


def _smoke_command(args: argparse.Namespace, *, adapters: Mapping[str, Any] | None) -> int:
    try:
        evidence = run_smoke(
            symbol=args.symbol,
            timeframe=args.timeframe,
            db_path=args.db,
            include_stream=args.include_stream,
            timeout=args.timeout,
            provider=args.provider,
            thinking=args.thinking,
            openai_speed=args.openai_speed,
            adapters=adapters,
        )
    except RuntimeConfigError as exc:
        _print_error(exc.code, exc)
        return 2
    print(smoke_json(evidence, secret_values=_env_secrets()))
    if not evidence.get("attempted"):
        return 2
    return 0 if evidence.get("ok") else 1


__all__ = ["main"]
