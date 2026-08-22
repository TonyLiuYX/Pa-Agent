"""Secret-safe selected-provider + Alpaca IEX smoke for the G028 runtime.

The smoke performs no trading and never opens a database.  Credentials are
read only from the process environment (or an injected ``environ`` mapping)
and are never printed, stored or echoed; the command output is one small
sanitized JSON evidence object serialized with the G017 redaction helpers
(:func:`pa_engine.providers.base.redacted_json`).

Exit codes (documented and consistent with the G022/G019/G020 smokes):

- ``0`` — the smoke executed without an infrastructure failure and at least
  one attempted provider succeeded (any failing provider is reported as typed
  per-provider evidence inside the JSON);
- ``1`` — credentials were present but every attempted provider failed with a
  typed provider error;
- ``2`` — no credentials were present at all and nothing was attempted.

Examples::

    python -m pa_engine.runtime smoke --symbol AAPL
    python -m pa_engine.runtime.smoke --symbol AAPL --include-stream

``run_smoke`` accepts an injectable ``adapters`` mapping (``history``, the
selected ``deepseek`` or ``openai`` key, and/or ``stream``) so tests can
exercise the command without a network or real credentials.  It never touches
the unselected provider.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from pa_engine.market_sources.alpaca.history.adapter import AlpacaHistoryAdapter
from pa_engine.market_sources.alpaca.stream.client import AlpacaIEXClient
from pa_engine.providers.base import redact_message
from pa_engine.providers.deepseek.adapter import DeepSeekAdapter
from pa_engine.providers.deepseek.config import DEEPSEEK_MODEL
from pa_engine.providers.deepseek.smoke import make_smoke_request
from pa_engine.providers.openai.adapter import OpenAIAdapter
from pa_engine.providers.openai.config import OPENAI_MODEL, OpenAIConfig
from pa_engine.providers.openai.smoke import make_smoke_request as make_openai_smoke_request
from pa_engine.runtime.config import RuntimeConfig, RuntimeConfigError

DEEPSEEK_ENV = "DEEPSEEK_API_KEY"
OPENAI_ENV = "OPENAI_API_KEY"
ALPACA_KEY_ID_ENV = "ALPACA_API_KEY_ID"
ALPACA_SECRET_ENV = "ALPACA_API_SECRET_KEY"
CREDENTIAL_ENV = (DEEPSEEK_ENV, OPENAI_ENV, ALPACA_KEY_ID_ENV, ALPACA_SECRET_ENV)

HISTORY_WINDOW_DAYS = 8
# Alpaca's limit is a page size, not a total-result cap.  A value of 2 turns
# the eight-day smoke window into more than a thousand sequential requests.
# One bounded 1,000-bar page keeps the smoke small while avoiding that false
# transport timeout; ordinary pagination still verifies multi-page handling.
HISTORY_BAR_LIMIT = 1_000
STREAM_TIMEOUT_SECONDS = 15.0


def _history_window(now: datetime) -> tuple[str, str]:
    start = now - timedelta(days=HISTORY_WINDOW_DAYS)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ"), now.strftime("%Y-%m-%dT%H:%M:%SZ")


def _provider_env(provider: str) -> str:
    if provider == "deepseek":
        return DEEPSEEK_ENV
    if provider == "openai":
        return OPENAI_ENV
    raise RuntimeConfigError("invalid_provider", "provider must be deepseek or openai")


def _scrub(value: Any, secret_values: tuple[str, ...]) -> Any:
    """Recursively scrub known secret values without renaming safe keys.

    The evidence object is hand-built from typed projections and never
    contains raw responses, but the G017 text redaction helper keeps a leaked
    credential value out of the output even if an injected adapter misbehaves.
    """
    if isinstance(value, Mapping):
        return {str(key): _scrub(item, secret_values) for key, item in value.items()}
    if isinstance(value, list):
        return [_scrub(item, secret_values) for item in value]
    if isinstance(value, tuple):
        return [_scrub(item, secret_values) for item in value]
    if isinstance(value, str):
        return redact_message(value, secret_values=secret_values)
    return value


def smoke_json(evidence: Mapping[str, Any], secret_values: Sequence[str] = ()) -> str:
    """Serialize one sanitized evidence mapping as deterministic JSON."""
    import json

    return json.dumps(
        _scrub(dict(evidence), tuple(secret_values)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _deepseek_evidence(outcome: Any) -> dict[str, Any]:
    """Project one adapter outcome into a tiny secret-free evidence dict."""
    response = getattr(outcome, "response", None)
    metadata = getattr(outcome, "call_metadata", None)
    response = response if isinstance(response, Mapping) else {}
    error = response.get("error")
    error_code = error.get("code") if isinstance(error, Mapping) else None
    classification = getattr(outcome, "classification", None)
    if error_code is None:
        error_code = classification
    usage = metadata.get("usage") if isinstance(metadata, Mapping) else None
    return {
        "ok": bool(getattr(outcome, "is_success", False)),
        "finish_reason": response.get("finish_reason"),
        "usage": usage,
        "error_code": error_code,
        "retryable": bool(getattr(outcome, "retryable", False)),
    }


def _openai_evidence(outcome: Any) -> dict[str, Any]:
    """Project one OpenAI outcome into the same tiny evidence shape."""

    response = getattr(outcome, "response", None)
    metadata = getattr(outcome, "call_metadata", None)
    response = response if isinstance(response, Mapping) else {}
    error = response.get("error")
    error_code = error.get("code") if isinstance(error, Mapping) else None
    classification = getattr(outcome, "classification", None)
    if error_code is None:
        error_code = classification
    usage = metadata.get("usage") if isinstance(metadata, Mapping) else None
    extensions = metadata.get("extensions") if isinstance(metadata, Mapping) else None
    openai = extensions.get("openai") if isinstance(extensions, Mapping) else None
    return {
        "ok": bool(getattr(outcome, "is_success", False)),
        "finish_reason": response.get("finish_reason"),
        "usage": usage,
        "effective_service_tier": openai.get("effective_service_tier") if isinstance(openai, Mapping) else None,
        "error_code": error_code,
        "retryable": bool(getattr(outcome, "retryable", False)),
    }


def _history_evidence(result: Any) -> dict[str, Any]:
    """Project one G019 history result into a sanitized evidence dict."""
    outcome = getattr(result, "outcome", None)
    snapshot = getattr(result, "market_snapshot", None)
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    return {
        "ok": bool(getattr(result, "success", False)),
        "bars": len(list(getattr(result, "bars", ()))),
        "snapshot_id": snapshot.get("market_snapshot_id"),
        "error_code": getattr(outcome, "code", None),
        "retryable": bool(getattr(outcome, "retryable", False)),
    }


def _stream_evidence(status: Any, client: Any) -> dict[str, Any]:
    """Project one stream status into a sanitized evidence dict."""
    outcome = getattr(status, "outcome", None)
    ok = bool(getattr(outcome, "ok", False))
    return {
        "ok": ok,
        "messages": len(list(getattr(client, "messages", ()))),
        "error_code": None if ok else getattr(outcome, "kind", None),
        "retryable": bool(getattr(outcome, "retryable", False)),
    }


def _resolve_adapter(adapters: Mapping[str, Any] | None, kind: str) -> Any:
    if adapters is None:
        return None
    return adapters.get(kind)


def run_smoke(
    *,
    symbol: str,
    timeframe: str = "1min",
    db_path: str | None = None,
    include_stream: bool = False,
    timeout: float = 60.0,
    provider: str = "deepseek",
    thinking: str | None = None,
    openai_speed: str = "standard",
    adapters: Mapping[str, Any] | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run the secret-safe smoke and return the sanitized evidence mapping.

    Credentials are resolved from *environ* (defaults to ``os.environ``) and
    are never part of the returned evidence.  *adapters* may inject fake
    ``history``/selected-provider/``stream`` objects for offline tests; when
    absent the selected real adapter is constructed from the environment.  The
    database path is accepted for CLI parity only and is never opened.
    """
    env = environ if environ is not None else os.environ
    # Keep the legacy DeepSeek smoke default while making an omitted OpenAI
    # choice use its only supported reasoning mode.  An explicit
    # ``thinking="disabled"`` still reaches RuntimeConfig and fails before
    # credential lookup or any adapter call.
    selected_thinking = thinking
    if selected_thinking is None:
        selected_thinking = "enabled" if provider == "openai" else "disabled"
    # Validate provider-specific reasoning and speed before credential lookup
    # or any adapter call.  OpenAI has no disabled-reasoning smoke path.
    config = RuntimeConfig(
        symbol=symbol,
        timeframe=timeframe,
        provider=provider,
        reasoning_mode=selected_thinking,
        openai_speed=openai_speed,
    )
    selected_env = _provider_env(config.provider)
    missing = [
        name
        for name in (selected_env, ALPACA_KEY_ID_ENV, ALPACA_SECRET_ENV)
        if not env.get(name)
    ]
    injected_selected = _resolve_adapter(adapters, config.provider) is not None
    injected_history = _resolve_adapter(adapters, "history") is not None
    injected_stream = _resolve_adapter(adapters, "stream") is not None
    alpaca_credentials = bool(env.get(ALPACA_KEY_ID_ENV) and env.get(ALPACA_SECRET_ENV))
    evidence: dict[str, Any] = {
        "ok": False,
        "attempted": False,
        "provider": config.provider,
        "feed": "alpaca:iex:single_exchange",
        "paper_only": True,
        "market_data_usage": "internal_prototype",
        "database": db_path,
        "alpaca": None,
        "deepseek": None,
        "openai": None,
        "stream": None,
    }
    if not (env.get(selected_env) or alpaca_credentials or injected_selected or injected_history or (include_stream and injected_stream)):
        evidence["missing_credentials"] = missing
        evidence["hint"] = (
            f"set {selected_env} and/or Alpaca credentials in the environment; "
            "credential values are never printed or stored"
        )
        return evidence
    evidence["attempted"] = True
    evidence["missing_credentials"] = missing

    # -- Alpaca IEX history (one small window, bounded bar limit) ----------
    history = _resolve_adapter(adapters, "history")
    if history is None:
        history = (
            AlpacaHistoryAdapter.from_env()
            if env.get(ALPACA_KEY_ID_ENV) and env.get(ALPACA_SECRET_ENV)
            else None
        )
    if history is None:
        evidence["alpaca"] = {"ok": False, "error_code": "missing_credentials", "retryable": False}
    else:
        try:
            timeframe_text = RuntimeConfig(symbol=symbol, timeframe=timeframe).timeframe_text()
        except Exception:
            timeframe_text = timeframe
        now = datetime.now(timezone.utc)
        start_at, end_at = _history_window(now)
        try:
            result = history.fetch_history(
                symbol=symbol,
                start_at=start_at,
                end_at=end_at,
                timeframe=timeframe_text,
                limit=HISTORY_BAR_LIMIT,
            )
            evidence["alpaca"] = _history_evidence(result)
        except Exception:
            evidence["alpaca"] = {"ok": False, "error_code": "transport", "retryable": True}

    # -- selected analysis-provider ping (one provider only) ---------------
    selected = _resolve_adapter(adapters, config.provider)
    if selected is None:
        if config.provider == "deepseek" and env.get(DEEPSEEK_ENV):
            selected = DeepSeekAdapter(api_key=env.get(DEEPSEEK_ENV))
        elif config.provider == "openai" and env.get(OPENAI_ENV):
            selected = OpenAIAdapter(api_key=env.get(OPENAI_ENV), config=OpenAIConfig(speed=config.openai_speed))
    if selected is None:
        evidence[config.provider] = {"ok": False, "error_code": "missing_credentials", "retryable": False}
    else:
        try:
            request = make_smoke_request(thinking=selected_thinking) if config.provider == "deepseek" else make_openai_smoke_request()
            # The provider smoke builders return canonical, hash-bound
            # normalized requests.  Mutating parameters here would invalidate
            # request_semantic_hash before the real adapter validates it.
            outcome = selected.complete(request)
            if config.provider == "deepseek":
                evidence["deepseek"] = {
                    "model": DEEPSEEK_MODEL,
                    "reasoning": selected_thinking,
                    **_deepseek_evidence(outcome),
                }
            else:
                evidence["openai"] = {
                    "model": OPENAI_MODEL,
                    "reasoning": "enabled",
                    "speed": config.openai_speed,
                    **_openai_evidence(outcome),
                }
        except Exception:
            evidence[config.provider] = {"ok": False, "error_code": "transport", "retryable": True}

    # -- Optional Alpaca IEX WebSocket connect + one receive ---------------
    if include_stream:
        stream = _resolve_adapter(adapters, "stream")
        if stream is None:
            stream = (
                AlpacaIEXClient(
                    symbol,
                    key_id=env.get(ALPACA_KEY_ID_ENV),
                    secret_key=env.get(ALPACA_SECRET_ENV),
                    timeout_seconds=min(timeout, STREAM_TIMEOUT_SECONDS),
                )
                if env.get(ALPACA_KEY_ID_ENV) and env.get(ALPACA_SECRET_ENV)
                else None
            )
        if stream is None:
            evidence["stream"] = {"ok": False, "error_code": "missing_credentials", "retryable": False}
        else:
            try:
                status = stream.run(max_messages=1)
                evidence["stream"] = _stream_evidence(status, stream)
            except Exception:
                evidence["stream"] = {"ok": False, "error_code": "transport", "retryable": True}
            finally:
                try:
                    stream.close()
                except Exception:
                    pass

    successes = [
        entry
        for entry in (evidence["alpaca"], evidence[config.provider], evidence["stream"])
        if isinstance(entry, Mapping) and entry.get("ok")
    ]
    evidence["ok"] = bool(successes)
    return evidence


def main(argv: Sequence[str] | None = None) -> int:
    """Delegate ``python -m pa_engine.runtime.smoke`` to the CLI smoke path."""
    from .cli import main as cli_main

    args = list(sys.argv[1:]) if argv is None else list(argv)
    return cli_main(["smoke", *args])


if __name__ == "__main__":  # pragma: no cover - exercised through subprocess
    raise SystemExit(main())


__all__ = ["CREDENTIAL_ENV", "main", "run_smoke", "smoke_json"]
