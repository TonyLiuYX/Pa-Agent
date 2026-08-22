"""Entry point for ``python -m pa_engine.runtime``."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
