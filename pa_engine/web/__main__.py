"""Entry point for ``python -m pa_engine.web``."""

from .server import main

if __name__ == "__main__":
    raise SystemExit(main())

__all__ = ["main"]
