"""Runtime-only configuration for the fixed OpenAI Responses adapter."""

from __future__ import annotations

from dataclasses import dataclass


OPENAI_API_URL = "https://api.openai.com/v1/responses"
OPENAI_PROVIDER = "openai"
OPENAI_MODEL = "gpt-5.6-luna"
OPENAI_REASONING_EFFORT = "max"
OPENAI_ADAPTER_VERSION = "g031-openai-gpt-5.6-luna-0.1.0"
OPENAI_SPEEDS = frozenset({"standard", "fast"})
OPENAI_SERVICE_TIERS = {"standard": "default", "fast": "fast"}

# Conventional aliases keep the provider package easy to use without making
# model or reasoning effort caller-selectable.
ADAPTER_VERSION = OPENAI_ADAPTER_VERSION
BASE_URL = OPENAI_API_URL
MODEL = OPENAI_MODEL
PROVIDER = OPENAI_PROVIDER


@dataclass(frozen=True, slots=True)
class OpenAIConfig:
    """Non-secret OpenAI transport settings.

    ``speed`` is the only runtime choice.  ``service_tier`` is accepted as a
    compatibility spelling for callers that work directly with the Responses
    API; it is always normalized to the two product choices.  The model and
    reasoning effort are fixed and cannot be changed to another value.
    """

    endpoint: str = OPENAI_API_URL
    timeout_seconds: float = 60.0
    provider: str = OPENAI_PROVIDER
    model: str = OPENAI_MODEL
    speed: str = "standard"
    service_tier: str | None = None
    reasoning_effort: str = OPENAI_REASONING_EFFORT

    def __post_init__(self) -> None:
        endpoint = self.endpoint.rstrip("/") if isinstance(self.endpoint, str) else self.endpoint
        if endpoint != OPENAI_API_URL:
            raise ValueError("OpenAI endpoint is fixed to the official Responses URL")
        if not isinstance(self.timeout_seconds, (int, float)) or isinstance(self.timeout_seconds, bool) or self.timeout_seconds <= 0:
            raise ValueError("OpenAI timeout_seconds must be positive")
        if self.provider != OPENAI_PROVIDER:
            raise ValueError("OpenAI provider must be 'openai'")
        if self.model != OPENAI_MODEL:
            raise ValueError("OpenAI model is fixed to 'gpt-5.6-luna'")
        if self.reasoning_effort != OPENAI_REASONING_EFFORT:
            raise ValueError("OpenAI reasoning effort is fixed to 'max'")

        speed = self.speed
        if not isinstance(speed, str):
            raise ValueError("OpenAI speed must be 'standard' or 'fast'")
        if self.service_tier is not None:
            if not isinstance(self.service_tier, str):
                raise ValueError("OpenAI service_tier must be 'default' or 'fast'")
            inverse = {value: key for key, value in OPENAI_SERVICE_TIERS.items()}
            if self.service_tier not in inverse:
                raise ValueError("OpenAI service_tier must be 'default' or 'fast'")
            service_speed = inverse[self.service_tier]
            if speed != "standard" and speed != service_speed:
                raise ValueError("OpenAI speed and service_tier disagree")
            speed = service_speed
        if speed not in OPENAI_SPEEDS:
            raise ValueError("OpenAI speed must be 'standard' or 'fast'")
        object.__setattr__(self, "endpoint", endpoint)
        object.__setattr__(self, "speed", speed)
        object.__setattr__(self, "service_tier", OPENAI_SERVICE_TIERS[speed])

    @property
    def requested_service_tier(self) -> str:
        """Return the exact service tier sent on the wire."""

        return str(self.service_tier)

    def __repr__(self) -> str:
        return (
            "OpenAIConfig(endpoint=<configured>, "
            f"timeout_seconds={self.timeout_seconds!r}, provider={self.provider!r}, "
            f"model={self.model!r}, speed={self.speed!r}, service_tier={self.service_tier!r}, "
            f"reasoning_effort={self.reasoning_effort!r})"
        )


OpenAIRuntimeConfig = OpenAIConfig
OpenAIAdapterConfig = OpenAIConfig


__all__ = [
    "OPENAI_ADAPTER_VERSION",
    "OPENAI_API_URL",
    "OPENAI_MODEL",
    "OPENAI_PROVIDER",
    "OPENAI_REASONING_EFFORT",
    "OPENAI_SERVICE_TIERS",
    "OPENAI_SPEEDS",
    "ADAPTER_VERSION",
    "BASE_URL",
    "MODEL",
    "PROVIDER",
    "OpenAIAdapterConfig",
    "OpenAIConfig",
    "OpenAIRuntimeConfig",
]
