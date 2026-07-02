"""
Data classes for DQ run outputs.

``DQRunResult`` is the single object returned by ``DataQualityEngine.run()``.
It carries both the output DataFrames (for writers) and the summary metrics
(for logging and the metrics Delta table).

Design rationale
----------------
Returning a structured result object — rather than writing DataFrames inside
the engine — keeps the engine focused on validation logic and lets the caller
(notebook, test, DAG) decide what to write and where.  This also makes the
engine trivially unit-testable without any Delta dependency.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class RuleMetric:
    """Per-rule failure count collected during a DQ run."""

    rule: str
    column: str
    severity: str
    failed_rows: int
    pass_column: str


@dataclass
class DQRunResult:
    """
    Complete output of a single ``DataQualityEngine.run()`` call.

    Attributes
    ----------
    source_name:
        Source identifier from the YAML ``name:`` field.
    dq_run_id:
        UUID4 unique to this DQ run.  Correlates all violations and metrics
        written in the same invocation.
    batch_id:
        The ``_batch_id`` from Bronze metadata — links back to the ingestion run.
    passed_df:
        Records that passed every enabled DQ rule (DQ columns stripped).
        Written to the Silver Delta table.
    failed_df:
        Records that failed at least one rule (includes ``_dq_violations``
        array and ``_dq_run_id`` / ``_dq_timestamp`` metadata columns).
        Written to the Bronze failed-records table.
    violations_df:
        Exploded violations: one row per rule failure per record.
        Written to the audit violations table.
    rows_read:
        Total rows in the Bronze batch processed.
    rows_passed:
        Count of rows written to Silver.
    rows_failed:
        Count of rows written to the failed-records table.
    pass_rate:
        ``rows_passed / rows_read`` rounded to 4 decimal places.
    execution_time_seconds:
        Wall-clock time for the full DQ run (engine only, excludes writes).
    rule_metrics:
        Per-rule failure statistics.
    success:
        ``True`` unless the engine encountered an unexpected error.
    error_message:
        Non-empty only when ``success=False``.
    """

    source_name: str
    dq_run_id: str
    batch_id: str

    # Output DataFrames — None only on engine failure
    passed_df: DataFrame | None
    failed_df: DataFrame | None
    violations_df: DataFrame | None

    # Scalar metrics
    rows_read: int = 0
    rows_passed: int = 0
    rows_failed: int = 0
    pass_rate: float = 0.0
    execution_time_seconds: float = 0.0

    rule_metrics: list[RuleMetric] = field(default_factory=list)

    success: bool = True
    error_message: str = ""

    def summary_dict(self) -> dict[str, Any]:
        """Return a serialisable dict suitable for logging or display."""
        return {
            "source_name": self.source_name,
            "dq_run_id": self.dq_run_id,
            "batch_id": self.batch_id,
            "rows_read": self.rows_read,
            "rows_passed": self.rows_passed,
            "rows_failed": self.rows_failed,
            "pass_rate": self.pass_rate,
            "execution_time_seconds": self.execution_time_seconds,
            "success": self.success,
            "error_message": self.error_message,
        }
