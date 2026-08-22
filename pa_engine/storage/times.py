"""Canonical UTC instant and Decimal text forms for durable storage columns.

The forms mirror the upstream G020/G025/G026 canonical timestamp convention
(UTC, ``Z`` suffix, at most six fractional digits, trailing zeros trimmed)
and the plain fixed-point Decimal text used by G025/G026 projections
(``format(value, "f")`` — never exponent notation).  Storage columns carry
exactly these strings so hashes recomputed upstream remain stable.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

INSTANT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$")
DECIMAL_TEXT_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")


def instant_to_text(value: datetime) -> str:
    """Return the canonical UTC text for an aware :class:`datetime`."""

    if not isinstance(value, datetime):
        raise TypeError("instant must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("instant must be timezone-aware")
    utc = value.astimezone(timezone.utc)
    base = utc.strftime("%Y-%m-%dT%H:%M:%S")
    if utc.microsecond:
        fraction = f"{utc.microsecond:06d}".rstrip("0")
        return f"{base}.{fraction}Z"
    return f"{base}Z"


def is_canonical_instant(text: object) -> bool:
    """Return whether *text* is exactly one canonical UTC instant string."""

    if not isinstance(text, str) or not INSTANT_RE.match(text):
        return False
    try:
        instant_from_text(text)
    except (ValueError, TypeError):
        return False
    return True


def instant_from_text(text: str) -> datetime:
    """Parse one canonical UTC instant string back to an aware datetime."""

    if not isinstance(text, str) or not INSTANT_RE.match(text):
        raise ValueError("instant text must match canonical UTC form")
    parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    return parsed.astimezone(timezone.utc)


def decimal_to_text(value: Decimal) -> str:
    """Return plain fixed-point text for a finite :class:`Decimal`."""

    if not isinstance(value, Decimal):
        raise TypeError("decimal value must be a Decimal")
    if not value.is_finite():
        raise ValueError("decimal value must be finite")
    text = format(value, "f")
    if not DECIMAL_TEXT_RE.match(text):
        raise ValueError("decimal text is not in canonical fixed-point form")
    return text


def is_decimal_text(text: object) -> bool:
    """Return whether *text* is one canonical fixed-point decimal string."""

    if not isinstance(text, str) or not DECIMAL_TEXT_RE.match(text):
        return False
    try:
        Decimal(text)
    except InvalidOperation:
        return False
    return True


def decimal_from_text(text: str) -> Decimal:
    """Parse one canonical fixed-point decimal string."""

    if not is_decimal_text(text):
        raise ValueError("decimal text must match canonical fixed-point form")
    return Decimal(text)


__all__ = [
    "DECIMAL_TEXT_RE",
    "INSTANT_RE",
    "decimal_from_text",
    "decimal_to_text",
    "instant_from_text",
    "instant_to_text",
    "is_canonical_instant",
    "is_decimal_text",
]
