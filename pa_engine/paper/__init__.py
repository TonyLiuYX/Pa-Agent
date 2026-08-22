"""Independent paper-account math package.

The public implementation lives in :mod:`pa_engine.paper.core`.  This
package deliberately has no market, scheduling, persistence, provider, or
broker integration; callers supply exact Decimal prices and consume typed
transition results.
"""

from .core import *  # noqa: F401,F403
