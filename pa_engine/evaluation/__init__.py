"""Deterministic, offline evidence harness for the automatic paper runtime.

The public entry point is :func:`run_offline_evaluation`.  It only consumes
recorded fixtures and injected clocks/transports; it never creates a network
client or reads credentials.
"""

from .harness import EvaluationError, EvaluationReport, run_offline_evaluation, write_report

__all__ = ["EvaluationError", "EvaluationReport", "run_offline_evaluation", "write_report"]
