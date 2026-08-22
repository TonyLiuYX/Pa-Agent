"""Loopback-only self-use web surface for the automatic paper runtime.

The web package is deliberately small: a standard-library HTTP server, one
owner-thread supervisor, an immutable presentation projection, and static
HTML/CSS/JavaScript.  It does not add a second trading/runtime boundary.
"""

from .config import (
    LOOPBACK_HOSTS,
    SUPPORTED_TIMEFRAMES,
    WebConfig,
    WebConfigError,
    validate_loopback_host,
)
from .presentation import PresentationSnapshot, build_presentation_snapshot
from .server import WebApplication, create_server, serve
from .supervisor import RuntimeSupervisor, StartRequest

__all__ = [
    "LOOPBACK_HOSTS",
    "SUPPORTED_TIMEFRAMES",
    "PresentationSnapshot",
    "RuntimeSupervisor",
    "StartRequest",
    "WebApplication",
    "WebConfig",
    "WebConfigError",
    "build_presentation_snapshot",
    "create_server",
    "serve",
    "validate_loopback_host",
]
