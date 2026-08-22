"""Runtime-only configuration for the DeepSeek V4 Flash adapter.

The configuration deliberately contains no credential field.  Credentials are
resolved at the adapter boundary and are never part of a normalized request,
response, call metadata object, or diagnostic representation.
"""

from __future__ import annotations

from dataclasses import dataclass


DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_PROVIDER = "deepseek"
DEEPSEEK_MODEL = "deepseek-v4-flash"
DEEPSEEK_ADAPTER_VERSION = "g022-deepseek-v4-flash-0.1.0"

# Conventional adapter constants used by the other provider packages.
ADAPTER_VERSION = DEEPSEEK_ADAPTER_VERSION
BASE_URL = DEEPSEEK_API_URL
MODEL = DEEPSEEK_MODEL
PROVIDER = DEEPSEEK_PROVIDER


@dataclass(frozen=True, slots=True)
class DeepSeekConfig:
    """Non-secret DeepSeek transport settings.

    ``reasoning_effort`` is intentionally absent.  DeepSeek's V4 Flash API
    currently defaults enabled thinking to high effort; G022 exposes only the
    provider-neutral enabled/disabled switch.
    """

    endpoint: str = DEEPSEEK_API_URL
    timeout_seconds: float = 60.0
    provider: str = DEEPSEEK_PROVIDER
    model: str = DEEPSEEK_MODEL

    def __post_init__(self) -> None:
        endpoint = self.endpoint.rstrip("/")
        # G022 intentionally has no configurable live host.  A caller may
        # inject a mock transport for tests, but a runtime adapter must always
        # send the key-bearing request to DeepSeek's documented endpoint.
        if endpoint != DEEPSEEK_API_URL:
            raise ValueError("DeepSeek endpoint is fixed to the official chat completions URL")
        if self.timeout_seconds <= 0:
            raise ValueError("DeepSeek timeout_seconds must be positive")
        if self.provider != DEEPSEEK_PROVIDER:
            raise ValueError("DeepSeek provider must be 'deepseek'")
        if self.model != DEEPSEEK_MODEL:
            raise ValueError("DeepSeek model must be 'deepseek-v4-flash'")
        object.__setattr__(self, "endpoint", endpoint)

    def __repr__(self) -> str:
        return (
            "DeepSeekConfig(endpoint=<configured>, "
            f"timeout_seconds={self.timeout_seconds!r}, provider={self.provider!r}, model={self.model!r})"
        )


# Friendly name for callers that distinguish static settings from the
# ephemeral credential handle resolved by RuntimeCredentialResolver.
DeepSeekRuntimeConfig = DeepSeekConfig


__all__ = [
    "DEEPSEEK_ADAPTER_VERSION",
    "DEEPSEEK_API_URL",
    "DEEPSEEK_MODEL",
    "DEEPSEEK_PROVIDER",
    "ADAPTER_VERSION",
    "BASE_URL",
    "MODEL",
    "PROVIDER",
    "DeepSeekConfig",
    "DeepSeekRuntimeConfig",
]
