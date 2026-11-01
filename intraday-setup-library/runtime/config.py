"""Secret-free runtime configuration and reproducible fingerprints."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SYMBOLS = ("SPY", "QQQ", "AAPL", "MSFT", "NVDA")


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class RuntimeConfig:
    mode: str = "demo"
    source_kind: str = "unknown"
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    include_variants: bool = True
    queue_size: int = 10000
    checkpoint_events: int = 10000
    checkpoint_seconds: float = 60
    port: int = 8765
    host: str = "127.0.0.1"
    pace_seconds: float = .02
    demo_minutes: int = 390
    demo_sessions: int = 1
    alpaca_history_sessions: int = 15
    recognition: dict = field(default_factory=dict)
    paper: dict = field(default_factory=dict)
    retention: dict = field(default_factory=dict)

    def validate(self):
        if self.mode not in {"demo", "replay", "live"}:
            raise ValueError("mode must be demo, replay or live; real execution is unsupported")
        if self.source_kind not in {"unknown","synthetic","real_market"}:
            raise ValueError("source_kind must be unknown, synthetic or real_market")
        if self.mode!="replay" and self.source_kind not in {"unknown",self.effective_source_kind}:
            raise ValueError("source_kind conflicts with actual runtime mode")
        if self.host != "127.0.0.1":
            raise ValueError("runtime binds only to 127.0.0.1")
        if not isinstance(self.symbols,(tuple,list)) or not self.symbols or len(set(self.symbols)) != len(self.symbols) or len(self.symbols) > 30:
            raise ValueError("watchlist requires 1–30 distinct symbols")
        if any(not isinstance(s, str) or not s or len(s) > 20 or not all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-' for c in s) for s in self.symbols):
            raise ValueError("invalid watchlist symbol")
        if type(self.include_variants) is not bool:
            raise ValueError("include_variants must be boolean")
        for name in ("queue_size", "checkpoint_events", "port", "demo_minutes", "demo_sessions"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(name + " must be a positive integer")
        if self.port > 65535 or self.queue_size > 1000000 or self.demo_minutes>390:
            raise ValueError("invalid port or queue size")
        if type(self.alpaca_history_sessions) is not int or not 0 <= self.alpaca_history_sessions <= 30:
            raise ValueError("alpaca_history_sessions must be an integer in 0..30")
        for name in ("checkpoint_seconds", "pace_seconds"):
            value = getattr(self, name)
            if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
                raise ValueError(name + " must be a finite nonnegative number")
        if self.checkpoint_seconds == 0:
            raise ValueError("checkpoint_seconds must be positive")
        if not all(isinstance(v,dict) for v in (self.recognition,self.paper,self.retention)):
            raise ValueError("recognition, paper and retention configuration must be objects")
        from recognition.config import RecognitionConfig
        RecognitionConfig(**self.recognition).validate()
        from paper import ExecutionPolicy
        ExecutionPolicy(**self.paper)
        from recognition.policy import RuntimePolicy
        RuntimePolicy(**self.retention).validate()
        return self

    @property
    def effective_source_kind(self):
        return "synthetic" if self.mode=="demo" else "real_market" if self.mode=="live" else self.source_kind

    def to_dict(self):
        return asdict(self)

    def fingerprint(self):
        # Presentation and pacing do not change recognition or execution.
        row = self.to_dict()
        for key in ("port", "host", "pace_seconds", "checkpoint_seconds", "checkpoint_events", "queue_size"):
            row.pop(key)
        return fingerprint(row)

    @classmethod
    def load(cls, path=None, **overrides):
        row = json.loads(Path(path).read_text()) if path else {}
        if not isinstance(row, dict):
            raise ValueError("configuration must be an object")
        def check_keys(value):
            if isinstance(value, dict):
                for k, v in value.items():
                    if any(s in str(k).lower() for s in ("secret", "password", "credential", "api_key", "token")):
                        raise ValueError("credentials belong in local environment, never configuration")
                    check_keys(v)
            elif isinstance(value, list):
                for v in value: check_keys(v)
        row.update({k: v for k, v in overrides.items() if v is not None})
        check_keys(row)
        if "symbols" in row: row["symbols"] = tuple(row["symbols"])
        return cls(**row).validate()


def validate_data_dir(path):
    resolved = Path(path).expanduser().resolve()
    if resolved == ROOT or resolved.is_relative_to(ROOT):
        raise ValueError("runtime data must be outside the library delivery directory")
    return resolved
