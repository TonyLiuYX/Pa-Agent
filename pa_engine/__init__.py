"""Provider-neutral Price Action engine contract package.

The public contract surface intentionally lives in :mod:`pa_engine.contracts`;
this module only exposes the package version and a convenient re-export.
"""

__version__ = "0.1.0"

from .contracts import (  # noqa: E402,F401
    ValidationIssue,
    ValidationResult,
    artifact_kinds,
    content_hash,
    finalize_artifact,
    validate_artifact,
)

__all__ = [
    "__version__",
    "ValidationIssue",
    "ValidationResult",
    "artifact_kinds",
    "content_hash",
    "finalize_artifact",
    "validate_artifact",
]
