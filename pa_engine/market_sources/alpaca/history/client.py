"""Compatibility entry point for the G019 Alpaca historical-bars client."""

from .adapter import *  # noqa: F401,F403
from .adapter import __all__ as _adapter_all

__all__ = list(_adapter_all)
