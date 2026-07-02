"""
Transformation result data structures.

``TransformationMetric`` captures the outcome of a single transformation step.
``TransformationRunResult`` aggregates metrics for an entire engine run and
exposes the transformed DataFrame.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class TransformationMetric:
    """
    Outcome metrics for a single transformation step.

    Attributes
    ----------
    transformation_type:
        The registered type string (e.g. ``"rename_column"``).
    execution_order:
        1-based position in the transformation sequence.
    execution_time_seconds:
        Wall-clock time for this step.
    rows_before:
        Row count before the transformation.
    rows_after:
        Row count after the transformation.  Equal to ``rows_before`` for
        column-only transformations (those with ``modifies_row_count=False``).
    columns_before:
        Number of columns before the transformation.
    columns_after:
        Number of columns after the transformation.
    columns_added:
        Names of columns added by this step.
    columns_removed:
        Names of columns removed by this step.
    status:
        ``"SUCCESS"``, ``"FAILED"``, or ``"SKIPPED"``.
    error_message:
        Exception message when ``status == "FAILED"``.
    description:
        Human-readable description from ``BaseTransformation.describe()``.
    """

    transformation_type: str
    execution_order: int
    execution_time_seconds: float
    rows_before: int
    rows_after: int
    columns_before: int
    columns_after: int
    columns_added: list[str] = field(default_factory=list)
    columns_removed: list[str] = field(default_factory=list)
    status: str = "SUCCESS"       # SUCCESS | FAILED | SKIPPED
    error_message: str = ""
    description: str = ""

    @property
    def columns_added_str(self) -> str:
        """Comma-separated column names for audit table storage."""
        return ", ".join(self.columns_added)

    @property
    def columns_removed_str(self) -> str:
        """Comma-separated column names for audit table storage."""
        return ", ".join(self.columns_removed)

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable representation for display and audit."""
        return {
            "transformation_type": self.transformation_type,
            "execution_order": self.execution_order,
            "execution_time_seconds": round(self.execution_time_seconds, 4),
            "rows_before": self.rows_before,
            "rows_after": self.rows_after,
            "columns_before": self.columns_before,
            "columns_after": self.columns_after,
            "columns_added": self.columns_added_str,
            "columns_removed": self.columns_removed_str,
            "status": self.status,
            "error_message": self.error_message,
            "description": self.description,
        }


@dataclass
class TransformationRunResult:
    """
    Aggregated result of a ``TransformationEngine.run()`` call.

    Attributes
    ----------
    source_name:
        The source identifier this run was for.
    run_id:
        Pipeline run ID for correlation with the audit framework.
    input_df:
        The original DataFrame passed to the engine.
    output_df:
        The DataFrame after all transformations have been applied.
    metrics:
        One ``TransformationMetric`` per executed step.
    success:
        ``False`` when a ``fail_fast`` transformation failed and aborted the
        engine run.
    error_message:
        The exception message from the first failing step (when applicable).
    total_execution_time_seconds:
        Sum of all step execution times.
    """

    source_name: str
    run_id: str
    input_df: DataFrame
    output_df: DataFrame
    metrics: list[TransformationMetric]
    success: bool
    error_message: str = ""
    total_execution_time_seconds: float = 0.0

    @property
    def transformations_executed(self) -> int:
        """Count of steps that completed with SUCCESS status."""
        return sum(1 for m in self.metrics if m.status == "SUCCESS")

    @property
    def transformations_failed(self) -> int:
        """Count of steps that completed with FAILED status."""
        return sum(1 for m in self.metrics if m.status == "FAILED")

    @property
    def transformations_skipped(self) -> int:
        """Count of steps that were SKIPPED."""
        return sum(1 for m in self.metrics if m.status == "SKIPPED")
