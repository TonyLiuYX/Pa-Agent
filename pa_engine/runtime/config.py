"""G028/G031 automatic paper-trading runtime configuration.

The runtime fixes the v1 configuration surface to the paper-only
``internal_prototype`` boundary: one U.S. symbol, one G014 timeframe, exactly
one selected analysis provider, a regular ``America/New_York`` session,
raw/unadjusted Alpaca Basic IEX data and ``automatic`` mode.  Anything outside
that frozen surface is rejected with a typed :class:`RuntimeConfigError`
before any market or model work starts.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping

from pa_engine.market.feed import ALPACA_IEX_FEED
from pa_engine.providers.deepseek.config import DEEPSEEK_ADAPTER_VERSION, DEEPSEEK_MODEL, DEEPSEEK_PROVIDER
from pa_engine.providers.openai.config import (
    OPENAI_ADAPTER_VERSION,
    OPENAI_MODEL,
    OPENAI_PROVIDER,
    OPENAI_SPEEDS,
)

SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.-]*$")
FEED_KEY = "alpaca:iex:single_exchange"
FIXED_SESSION = MappingProxyType({"kind": "regular", "timezone": "America/New_York"})
REASONING_MODES = frozenset({"enabled", "disabled"})
ANALYSIS_PROVIDERS = frozenset({DEEPSEEK_PROVIDER, OPENAI_PROVIDER})
PROVIDER_MODELS = {DEEPSEEK_PROVIDER: DEEPSEEK_MODEL, OPENAI_PROVIDER: OPENAI_MODEL}
PROVIDER_ADAPTERS = {DEEPSEEK_PROVIDER: DEEPSEEK_ADAPTER_VERSION, OPENAI_PROVIDER: OPENAI_ADAPTER_VERSION}
OPENAI_SPEED_CHOICES = frozenset(OPENAI_SPEEDS)
ALLOWED_MODE = "automatic"
EXECUTION_POLICIES = frozenset({"advisory", "paper"})
ALLOWED_ADJUSTMENT_POLICY = "unadjusted"
ALLOWED_MARKET_DATA_USAGE = "internal_prototype"
FIXED_MODEL = DEEPSEEK_MODEL

_STRING_TIMEFRAMES = {
    "1min": {"unit": "minute", "multiplier": 1},
    "5min": {"unit": "minute", "multiplier": 5},
    "15min": {"unit": "minute", "multiplier": 15},
    "30min": {"unit": "minute", "multiplier": 30},
    "1hour": {"unit": "hour", "multiplier": 1},
    "2hour": {"unit": "hour", "multiplier": 2},
    "4hour": {"unit": "hour", "multiplier": 4},
    "day": {"unit": "day", "multiplier": 1},
    "week": {"unit": "week", "multiplier": 1},
}
_TIME_UNITS = {"minute": "Min", "hour": "Hour", "day": "Day", "week": "Week"}
_TIMEFRAME_BOUNDS = {"minute": (1, 59), "hour": (1, 23), "day": (1, 1), "week": (1, 1)}

_POSITIVE_FIELDS = ("minimum_closed_bars", "stale_after_seconds", "queue_capacity", "max_in_flight")


class RuntimeConfigError(ValueError):
    """Invalid G028 runtime configuration carrying a stable error code."""

    def __init__(self, code: str, message: str) -> None:
        self.code = str(code)
        super().__init__(str(message))


@dataclass(frozen=True)
class RuntimeConfig:
    """Frozen runtime settings; construction always validates them."""

    symbol: str
    timeframe: Mapping[str, Any] | str = "1min"
    reasoning_mode: str = "enabled"
    database_path: str = "runtime.db"
    # Kept for programmatic compatibility, but validation always derives the
    # fixed model from ``provider``.  The public CLI has no model selector.
    model: str | None = None
    provider: str = DEEPSEEK_PROVIDER
    openai_speed: str = "standard"
    market_data_usage: str = "internal_prototype"
    session: Mapping[str, Any] = field(default_factory=lambda: {"kind": "regular", "timezone": "America/New_York"})
    adjustment_policy: str = "unadjusted"
    minimum_closed_bars: int = 3
    stale_after_seconds: int = 60
    queue_capacity: int = 200
    max_in_flight: int = 40
    mode: str = "automatic"
    execution_policy: str = "paper"
    no_live_orders: bool = True

    def __post_init__(self) -> None:
        self.validate()

    @property
    def market_feed(self):
        """The frozen G015 feed identity for this runtime."""
        return ALPACA_IEX_FEED

    @property
    def feed_key(self) -> str:
        """The storage-level feed key matching G027's ``FEED_KEY``."""
        return FEED_KEY

    def timeframe_mapping(self) -> dict[str, Any]:
        """Return the canonical ``{"unit", "multiplier"}`` G014 timeframe.

        Accepted spellings are the frozen strings ``1min``/``5min``/``15min``/
        ``30min``/``1hour``/``2hour``/``4hour``/``day``/``week`` (case
        insensitive) or a mapping with exactly ``unit``/``multiplier`` keys.
        Multipliers must match the G021/G019 builder bounds: minute 1..59,
        hour 1..23, day/week exactly 1.
        """
        raw = self.timeframe
        if isinstance(raw, str):
            mapped = _STRING_TIMEFRAMES.get(raw.strip().lower())
            if mapped is None:
                raise RuntimeConfigError("invalid_timeframe", f"unsupported timeframe string: {raw!r}")
            return dict(mapped)
        if isinstance(raw, Mapping):
            unit, multiplier = raw.get("unit"), raw.get("multiplier")
            if isinstance(unit, str) and isinstance(multiplier, int) and not isinstance(multiplier, bool) and unit in _TIMEFRAME_BOUNDS:
                low, high = _TIMEFRAME_BOUNDS[unit]
                if low <= multiplier <= high:
                    return {"unit": unit, "multiplier": multiplier}
        raise RuntimeConfigError("invalid_timeframe", "timeframe must be a supported string or a unit/multiplier mapping")

    def timeframe_text(self) -> str:
        """Return the G019/Alpaca timeframe string (for example ``5Min``)."""
        timeframe = self.timeframe_mapping()
        return f"{timeframe['multiplier']}{_TIME_UNITS[timeframe['unit']]}"

    def validate(self) -> "RuntimeConfig":
        """Validate every field and return ``self`` (raises on the first failure)."""
        symbol = self.symbol
        if not isinstance(symbol, str) or not symbol or SYMBOL_RE.fullmatch(symbol) is None:
            raise RuntimeConfigError("invalid_symbol", "symbol must match ^[A-Z][A-Z0-9.-]*$ and be upper-case")
        self.timeframe_mapping()
        if not isinstance(self.reasoning_mode, str) or self.reasoning_mode not in REASONING_MODES:
            raise RuntimeConfigError("invalid_reasoning_mode", "reasoning_mode must be enabled or disabled")
        if not isinstance(self.provider, str) or self.provider not in ANALYSIS_PROVIDERS:
            raise RuntimeConfigError("invalid_provider", "provider must be deepseek or openai")
        expected_model = PROVIDER_MODELS[self.provider]
        if self.model is not None and self.model != expected_model:
            raise RuntimeConfigError("invalid_model", f"model is fixed to {expected_model!r} for provider {self.provider!r}")
        if self.provider == OPENAI_PROVIDER and self.reasoning_mode != "enabled":
            raise RuntimeConfigError(
                "openai_reasoning_required",
                "OpenAI GPT-5.6 Luna requires reasoning_mode=enabled",
            )
        if not isinstance(self.openai_speed, str) or self.openai_speed not in OPENAI_SPEED_CHOICES:
            raise RuntimeConfigError("invalid_openai_speed", "openai_speed must be standard or fast")
        if self.provider == DEEPSEEK_PROVIDER and self.openai_speed != "standard":
            raise RuntimeConfigError(
                "invalid_openai_speed",
                "openai_speed=fast is only valid when provider=openai",
            )
        if self.mode != ALLOWED_MODE:
            raise RuntimeConfigError("invalid_mode", "mode must be automatic for the G028 paper runtime")
        if not isinstance(self.execution_policy, str) or self.execution_policy not in EXECUTION_POLICIES:
            raise RuntimeConfigError("invalid_execution_policy", "execution_policy must be advisory or paper")
        if not isinstance(self.session, Mapping) or dict(self.session) != dict(FIXED_SESSION):
            raise RuntimeConfigError("invalid_session", "session is fixed to regular America/New_York")
        if self.adjustment_policy != ALLOWED_ADJUSTMENT_POLICY:
            raise RuntimeConfigError("invalid_adjustment_policy", "adjustment_policy must be unadjusted")
        if self.market_data_usage != ALLOWED_MARKET_DATA_USAGE:
            raise RuntimeConfigError("invalid_market_data_usage", "market_data_usage must be internal_prototype")
        if self.no_live_orders is not True:
            raise RuntimeConfigError("live_orders_forbidden", "no_live_orders must always be True; live orders are not authorized")
        if not isinstance(self.database_path, (str, bytes)) and not hasattr(self.database_path, "__fspath__"):
            raise RuntimeConfigError("invalid_database_path", "database_path must be a string or path-like value")
        for name in _POSITIVE_FIELDS:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise RuntimeConfigError("non_positive_parameter", f"{name} must be a positive integer")
        timeframe = self.timeframe_mapping()
        object.__setattr__(self, "timeframe", dict(timeframe))
        object.__setattr__(self, "model", expected_model)
        return self

    @property
    def adapter(self) -> str:
        """Return the fixed normalized-request adapter identity."""

        return PROVIDER_ADAPTERS[self.provider]

    @property
    def provider_model(self) -> str:
        """Return the provider-derived fixed model identity."""

        return PROVIDER_MODELS[self.provider]

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-native settings projection (no feed/feed_key)."""
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe_mapping(),
            "reasoning_mode": self.reasoning_mode,
            "database_path": str(self.database_path),
            "model": self.model,
            "provider": self.provider,
            "openai_speed": self.openai_speed,
            "market_data_usage": self.market_data_usage,
            "session": dict(self.session),
            "adjustment_policy": self.adjustment_policy,
            "minimum_closed_bars": self.minimum_closed_bars,
            "stale_after_seconds": self.stale_after_seconds,
            "queue_capacity": self.queue_capacity,
            "max_in_flight": self.max_in_flight,
            "mode": self.mode,
            "execution_policy": self.execution_policy,
            "no_live_orders": self.no_live_orders,
        }

    as_dict = to_dict

    @classmethod
    def from_kwargs(cls, **kwargs: Any) -> "RuntimeConfig":
        """Apply defaults and construct a validated configuration."""
        return cls(**kwargs)

    def paper_only_label(self) -> str:
        """Human label that must appear in run/status output."""
        return (
            "PAPER TRADING ONLY — no live orders. "
            f"Market data: Alpaca Basic IEX, {ALLOWED_MARKET_DATA_USAGE} (no public display/redistribution)."
        )


__all__ = [
    "ANALYSIS_PROVIDERS",
    "EXECUTION_POLICIES",
    "FIXED_MODEL",
    "OPENAI_SPEED_CHOICES",
    "PROVIDER_ADAPTERS",
    "PROVIDER_MODELS",
    "RuntimeConfig",
    "RuntimeConfigError",
]
