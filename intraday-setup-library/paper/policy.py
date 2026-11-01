"""Versioned experimental execution overlay, separate from source strategies."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json


def decimal(value, name="value"):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be a finite decimal")
    return result


@dataclass(frozen=True)
class ExecutionPolicy:
    version: str = "1.0.0"
    model: str = "next_quote_experimental"
    initial_cash: str = "100000.00"
    allocation_fraction: str = "0.10"
    max_positions: int = 5
    tick_size: str = "0.01"
    slippage_ticks: int = 1
    fee_per_fill: str = "0.00"
    stop_fraction: str = "0.01"
    target_fraction: str = "0.02"
    max_hold_seconds: int = 3600
    entry_timeout_seconds: int = 60
    close_buffer_seconds: int = 300
    daily_loss_fraction: str = "0.02"
    quote_max_age_seconds: int = 5
    recent_rows_limit: int = 300

    def __post_init__(self):
        if self.version != "1.0.0" or self.model not in {"next_quote_experimental", "next_bar_proxy_experimental"}:
            raise ValueError("unsupported experimental execution policy")
        for name in ("initial_cash", "tick_size"):
            if decimal(getattr(self, name), name) <= 0:
                raise ValueError(f"{name} must be positive")
        if decimal(self.fee_per_fill, "fee_per_fill") < 0:
            raise ValueError("negative fee")
        for name in ("allocation_fraction", "stop_fraction", "target_fraction", "daily_loss_fraction"):
            if not 0 < decimal(getattr(self, name), name) <= 1:
                raise ValueError(f"{name} must be in (0,1]")
        for name in ("max_positions", "max_hold_seconds", "entry_timeout_seconds", "quote_max_age_seconds", "recent_rows_limit"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("slippage_ticks", "close_buffer_seconds"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a nonnegative integer")

    def to_dict(self):
        return asdict(self)

    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def execution_profiles(policy=None):
    """Generate one execution registration per executable rule graph (currently 84)."""
    from recognition.specs import load_specs
    from recognition.provenance import algorithm_sha256
    policy = policy or ExecutionPolicy()
    algo = algorithm_sha256()
    result = []
    for spec in load_specs(include_variants=True):
        rule_hash = hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        result.append({"setup_id": spec["setup_id"], "variant_id": spec["variant_id"],
                       "source_version": spec["source_version"], "source_sha256": spec["source_sha256"],
                       "rule_version": spec["rule_version"], "rule_sha256": rule_hash,
                       "algorithm_sha256": algo, "execution_version": policy.version,
                       "execution_sha256": policy.fingerprint(), "execution_model": policy.model,
                       "requirements": spec["requirements"], "scales": spec["scales"], "directions": spec["directions"],
                       "enabled": True, "market_validation": "not_run",
                       "performance_scope": "experimental_overlay_not_source_strategy"})
    return result
