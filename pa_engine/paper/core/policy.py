"""Named constants for the frozen v0.1 paper-math policy.

The simulation has no fee or slippage model: every transition carries exact
``Decimal("0.00")`` values for both fields.
"""

from decimal import Decimal

from .math import (
    INITIAL_BALANCE,
    MONEY_QUANTUM,
    PAPER_MATH_VERSION,
    R_QUANTUM,
    SHARE_QUANTUM,
    ZERO_FEES,
    ZERO_SLIPPAGE,
)

ALLOCATION_FRACTION = Decimal("1.0")
ROUNDING_QUANTITY = "ROUND_DOWN"
ROUNDING_MONEY = "ROUND_HALF_UP"
ROUNDING_R = "ROUND_HALF_UP"

__all__ = [
    "ALLOCATION_FRACTION",
    "INITIAL_BALANCE",
    "MONEY_QUANTUM",
    "PAPER_MATH_VERSION",
    "R_QUANTUM",
    "ROUNDING_MONEY",
    "ROUNDING_QUANTITY",
    "ROUNDING_R",
    "SHARE_QUANTUM",
    "ZERO_FEES",
    "ZERO_SLIPPAGE",
]
