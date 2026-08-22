"""Alpaca/SIP minute-bar trade-condition rules.

The official minute-bar table has three independently updated fields:
open/close (green, yellow, red), high/low (green or red), and volume (green or
red).  A tape-specific condition may occur more than once on a trade.  The
strictest rule for each field is selected independently, so a volume-only
trade remains auditable without inventing a price path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True, slots=True)
class ConditionRule:
    """One condition's minute-bar update behavior."""

    open_close: str  # green, yellow, red
    high_low: bool
    volume: bool

    @property
    def updates_open_close(self) -> bool:
        return self.open_close != "red"

    @property
    def updates_high_low(self) -> bool:
        return self.high_low

    @property
    def updates_volume(self) -> bool:
        return self.volume

    @property
    def high_low_rule(self) -> str:
        return "green" if self.high_low else "red"

    @property
    def volume_rule(self) -> str:
        return "green" if self.volume else "red"

    @property
    def open_close_rule(self) -> str:
        return self.open_close

    def to_dict(self) -> dict[str, object]:
        return {"open_close": self.open_close, "high_low": self.high_low_rule, "volume": self.volume_rule}


G = ConditionRule("green", True, True)
Y = ConditionRule("yellow", True, True)
R = ConditionRule("red", False, True)
N = ConditionRule("red", False, False)


# The table is represented with one row per code and tape.  The value for a
# code absent from a tape is intentionally not inferred from another tape.
# This keeps an accidental tape/condition mismatch from becoming an eligible
# trade.  All rows are the documented minute (M) rules, including the rows
# whose daily behavior differs.
_RULES: dict[str, dict[str, ConditionRule]] = {
    " ": {"A": G, "B": G},
    "@": {"C": G, "O": G},
    "A": {"C": G},
    "B": {"A": R, "B": R, "C": G},
    "C": {"A": R, "B": R, "C": R, "O": R},
    "D": {"C": G},
    "E": {"A": G, "B": G},
    "F": {"A": G, "B": G, "C": G},
    "G": {"C": R},  # bunched sold trade: minute bar
    "H": {"A": R, "B": R, "C": R},
    "I": {"A": R, "B": R, "C": R, "O": R},
    "K": {"A": G, "B": G, "C": G},
    "L": {"A": G, "B": G, "C": G},
    "M": {"A": N, "B": N, "C": N},
    "N": {"A": R, "B": R, "C": R, "O": R},
    "O": {"A": G, "B": G, "C": G},
    "P": {"A": R, "B": R, "C": R, "O": R},
    "Q": {"A": N, "B": N, "C": N},
    "R": {"A": R, "B": R, "C": R, "O": R},
    "T": {"A": G, "B": G, "C": G, "O": G},
    "U": {"A": R, "B": R, "C": R, "O": R},
    "V": {"A": R, "B": R, "C": R},
    "W": {"C": R, "O": R},
    "X": {"A": G, "B": G, "C": G},
    "Y": {"C": G},
    "Z": {"A": R, "B": R, "C": R},
    "4": {"A": R, "B": R, "C": R},
    "5": {"A": G, "B": G, "C": G},
    "6": {"A": G, "B": G, "C": G},
    "7": {"A": R, "B": R, "C": R},
    # Corrected Consolidated Close: red for open/close, high/low and volume.
    "9": {"A": N, "B": N, "C": N},
}


KNOWN_CONDITION_CODES = frozenset(_RULES)
TAPES = frozenset({"A", "B", "C", "O"})


class UnsupportedConditionError(ValueError):
    """A condition/tape combination not present in the official table."""


def condition_rule(code: str, tape: str) -> ConditionRule:
    if not isinstance(code, str) or len(code) != 1 or code not in _RULES or tape not in TAPES:
        raise UnsupportedConditionError("condition is not in the Alpaca minute-bar table")
    try:
        return _RULES[code][tape]
    except KeyError:
        raise UnsupportedConditionError("condition/tape combination is not in the Alpaca minute-bar table") from None


def combine_rules(conditions: Iterable[str], tape: str) -> ConditionRule:
    """Apply strictest-per-field behavior to all codes on one trade."""

    rules = [condition_rule(code, tape) for code in conditions]
    if not rules:
        raise UnsupportedConditionError("trade has no condition codes")
    # green < yellow < red for open/close; booleans are strictest when false.
    rank = {"green": 0, "yellow": 1, "red": 2}
    open_close = max((rule.open_close for rule in rules), key=rank.__getitem__)
    return ConditionRule(open_close, all(rule.high_low for rule in rules), all(rule.volume for rule in rules))


def minute_bar_rule(conditions: Iterable[str], tape: str) -> ConditionRule:
    """Public alias emphasizing that only minute rules are implemented."""

    return combine_rules(conditions, tape)


def condition_table() -> Mapping[str, Mapping[str, ConditionRule]]:
    """Return an immutable-ish copy suitable for audit tooling/tests."""

    return {code: dict(tapes) for code, tapes in _RULES.items()}


__all__ = [
    "ConditionRule",
    "KNOWN_CONDITION_CODES",
    "TAPES",
    "UnsupportedConditionError",
    "combine_rules",
    "condition_rule",
    "condition_table",
    "minute_bar_rule",
]
