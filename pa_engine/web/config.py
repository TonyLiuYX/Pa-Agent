"""Validated configuration and request values for the Paper Desk web surface."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from pa_engine.runtime.config import RuntimeConfig, RuntimeConfigError


LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
SUPPORTED_TIMEFRAMES = (
    "1min",
    "5min",
    "15min",
    "30min",
    "1hour",
    "2hour",
    "4hour",
    "day",
    "week",
)
CURATED_SYMBOLS = ("NVDA", "TSLA", "AMD", "AAPL", "AMZN", "META")
EXECUTION_POLICIES = frozenset({"advisory", "paper"})
SUPPORTED_LOCALES = frozenset({"zh-CN", "en-US"})
PROVIDERS = frozenset({"deepseek", "openai"})
REASONING_MODES = frozenset({"enabled", "disabled"})
OPENAI_SPEEDS = frozenset({"standard", "fast"})
_SECRET_KEYS = frozenset(
    {
        "model_key",
        "api_key",
        "deepseek_api_key",
        "openai_api_key",
        "alpaca_key_id",
        "alpaca_secret_key",
        "alpaca_api_key_id",
        "alpaca_api_secret_key",
        "password",
        "authorization",
        "token",
        "secret",
    }
)
_START_FIELDS = frozenset({"symbol", "timeframe", "execution_policy"})


class WebConfigError(ValueError):
    """Typed, secret-safe validation failure at the web boundary."""

    def __init__(self, code: str, message: str) -> None:
        self.code = str(code)
        super().__init__(str(message))


def validate_loopback_host(host: str) -> str:
    """Return *host* when it is an explicit loopback bind target."""

    if not isinstance(host, str) or not host.strip():
        raise WebConfigError("invalid_host", "host must be an explicit loopback address")
    candidate = host.strip()
    if candidate.lower() == "localhost":
        return "127.0.0.1"
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError:
        raise WebConfigError("non_loopback_bind", "web server host must be 127.0.0.1 or ::1") from None
    if not address.is_loopback:
        raise WebConfigError("non_loopback_bind", "web server host must be a loopback address")
    return candidate


def _nonempty_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WebConfigError("missing_field", f"{field} is required")
    return value.strip()


def _looks_secret(field: Any) -> bool:
    normalized = str(field or "").strip().lower().replace("-", "_")
    return normalized in _SECRET_KEYS or any(token in normalized for token in ("key", "secret", "password", "credential", "authorization", "token"))


@dataclass(frozen=True, slots=True)
class StartRequest:
    """A deliberately secret-free runtime start command."""

    symbol: str
    timeframe: str
    execution_policy: str = "advisory"

    @classmethod
    def from_payload(cls, payload: Any) -> "StartRequest":
        if not isinstance(payload, Mapping):
            raise WebConfigError("invalid_json", "start body must be a JSON object")
        forbidden = sorted(str(key) for key in payload if _looks_secret(key))
        if forbidden:
            raise WebConfigError("start_secrets_forbidden", "credentials must be configured in Settings and are forbidden in start")
        if any(key in payload for key in ("database_path", "db", "database")):
            raise WebConfigError("database_path_forbidden", "database path is fixed by the server CLI")
        unknown = sorted(str(key) for key in payload if key not in _START_FIELDS)
        if unknown:
            raise WebConfigError("unknown_start_field", "start accepts only symbol, timeframe and execution_policy")
        symbol = _nonempty_text(payload.get("symbol"), "symbol").upper()
        if symbol not in CURATED_SYMBOLS:
            raise WebConfigError("invalid_symbol", "symbol must be one of the curated high-liquidity candidates")
        timeframe = _nonempty_text(payload.get("timeframe"), "timeframe").lower()
        if timeframe not in SUPPORTED_TIMEFRAMES:
            raise WebConfigError("invalid_timeframe", "timeframe is not supported by the runtime")
        execution_policy = _nonempty_text(payload.get("execution_policy", "advisory"), "execution_policy").lower()
        if execution_policy not in EXECUTION_POLICIES:
            raise WebConfigError("invalid_execution_policy", "execution_policy must be advisory or paper")
        try:
            RuntimeConfig(symbol=symbol, timeframe=timeframe, execution_policy=execution_policy)
        except RuntimeConfigError as exc:
            raise WebConfigError(exc.code, str(exc)) from None
        return cls(symbol=symbol, timeframe=timeframe, execution_policy=execution_policy)

    def public_config(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "execution_policy": self.execution_policy,
        }


@dataclass(frozen=True, slots=True)
class WebConfig:
    """Non-secret process configuration for the loopback service."""

    host: str = "127.0.0.1"
    port: int = 8765
    database_path: str = "runtime.db"
    workspace_path: str = ".pa-workspace"
    knowledge_dir: str | None = None
    startup_history_seconds: int = 7 * 86400
    step_interval_seconds: float = 1.0
    minimum_closed_bars: int = 3
    stale_after_seconds: int = 60
    queue_capacity: int = 200
    max_in_flight: int = 40

    def __post_init__(self) -> None:
        object.__setattr__(self, "host", validate_loopback_host(self.host))
        if isinstance(self.port, bool) or not isinstance(self.port, int) or not 0 <= self.port <= 65535:
            raise WebConfigError("invalid_port", "port must be an integer in 0..65535")
        for name in ("database_path", "workspace_path"):
            value = getattr(self, name)
            if not isinstance(value, (str, bytes)) and not hasattr(value, "__fspath__"):
                raise WebConfigError("invalid_local_path", f"{name} must be a local path")
            object.__setattr__(self, name, value.decode() if isinstance(value, bytes) else str(Path(value)))
        for name in ("startup_history_seconds", "minimum_closed_bars", "stale_after_seconds", "queue_capacity", "max_in_flight"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise WebConfigError("invalid_runtime_parameter", f"{name} must be positive")
        if isinstance(self.step_interval_seconds, bool) or not isinstance(self.step_interval_seconds, (int, float)) or self.step_interval_seconds < 1.0:
            raise WebConfigError("invalid_poll_interval", "step interval must be at least one second")


def secret_field_names() -> frozenset[str]:
    """Expose only field names for boundary tests; never values."""

    return _SECRET_KEYS


__all__ = [
    "CURATED_SYMBOLS",
    "EXECUTION_POLICIES",
    "LOOPBACK_HOSTS",
    "OPENAI_SPEEDS",
    "PROVIDERS",
    "REASONING_MODES",
    "SUPPORTED_LOCALES",
    "SUPPORTED_TIMEFRAMES",
    "StartRequest",
    "WebConfig",
    "WebConfigError",
    "secret_field_names",
    "validate_loopback_host",
]
