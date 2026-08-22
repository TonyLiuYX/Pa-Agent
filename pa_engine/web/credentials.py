"""Process-memory credential vault for the loopback Paper Desk service."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from .config import WebConfigError


@dataclass(frozen=True, slots=True, repr=False)
class CredentialSnapshot:
    """One runtime-owned credential copy with a deliberately opaque repr."""

    alpaca_key_id: str
    alpaca_secret_key: str
    model_api_key: str
    provider: str

    def __repr__(self) -> str:
        return f"CredentialSnapshot(provider={self.provider!r}, values=<runtime-only>)"

    def secret_values(self) -> tuple[str, ...]:
        return (self.alpaca_key_id, self.alpaca_secret_key, self.model_api_key)


class CredentialVault:
    """Small in-process BYOK store that is cleared when the service closes."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._values: dict[str, tuple[str, ...]] = {}

    def __repr__(self) -> str:
        return f"CredentialVault(configured={self.configured()!r}, values=<process-only>)"

    @staticmethod
    def _required(value: Any, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise WebConfigError("missing_credential", f"{field} is required")
        return value.strip()

    def set_alpaca(self, key_id: Any, secret_key: Any) -> None:
        values = (self._required(key_id, "alpaca_key_id"), self._required(secret_key, "alpaca_secret_key"))
        with self._lock:
            self._values["alpaca"] = values

    def set_model(self, provider: str, api_key: Any) -> None:
        if provider not in {"deepseek", "openai"}:
            raise WebConfigError("invalid_provider", "credential provider must be alpaca, deepseek or openai")
        value = self._required(api_key, "api_key")
        with self._lock:
            self._values[provider] = (value,)

    def clear(self, provider: str | None = None) -> None:
        with self._lock:
            if provider in (None, "all"):
                self._values.clear()
                return
            if provider not in {"alpaca", "deepseek", "openai"}:
                raise WebConfigError("invalid_provider", "credential provider must be alpaca, deepseek or openai")
            self._values.pop(provider, None)

    def configured(self) -> dict[str, bool]:
        with self._lock:
            return {name: name in self._values for name in ("alpaca", "deepseek", "openai")}

    def alpaca_values(self) -> tuple[str, str]:
        with self._lock:
            value = self._values.get("alpaca")
            if value is None:
                raise WebConfigError("credentials_missing", "configure Alpaca credentials in Settings before starting")
            return value[0], value[1]

    def snapshot_for_run(self, provider: str) -> CredentialSnapshot:
        with self._lock:
            alpaca = self._values.get("alpaca")
            model = self._values.get(provider)
            if alpaca is None:
                raise WebConfigError("credentials_missing", "configure Alpaca credentials in Settings before starting")
            if model is None:
                raise WebConfigError("credentials_missing", f"configure {provider} credentials in Settings before starting")
            return CredentialSnapshot(alpaca[0], alpaca[1], model[0], provider)


__all__ = ["CredentialSnapshot", "CredentialVault"]
