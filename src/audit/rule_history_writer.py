"""
Rule execution history writer for the DataGuardian audit layer.

``RuleHistoryWriter`` appends one row per DQ rule per pipeline run to
``{catalog}.audit.rule_execution_history``.  This table enables per-rule trend
analysis — identifying which rules fail most often, on which columns, and how
violation rates change over time.

Schema
------
One row per rule per source per pipeline run.  The ``run_id`` links back to
``audit.pipeline_run_history`` for run-level context.

``pass_percentage`` is stored as a decimal (0.0–100.0) rather than a fraction
so that BI tools can display it directly without a calculated field.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.common.exceptions import WriterException
from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.common.pipeline_run import PipelineRun
    from src.quality.results import DQRunResult

_TABLE = "rule_execution_history"
_SCHEMA = "audit"


class RuleHistoryWriter:
    """
    Writes per-rule metrics to ``{catalog}.audit.rule_execution_history``.

    Parameters
    ----------
    catalog:
        Unity Catalog name (e.g. ``dg_dev``).
    logger:
        Optional pre-bound logger.
    """

    def __init__(
        self,
        catalog: str,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._catalog = catalog
        self._log = logger or get_logger("dataguardian.audit.rule_history")

    def write(
        self,
        run: PipelineRun,
        dq_result: DQRunResult,
        spark: SparkSession,
    ) -> None:
        """
        Append one row per rule in ``dq_result.rule_metrics`` to the history table.

        A no-op when ``dq_result.rule_metrics`` is empty (e.g. no rules were
        defined for the source).

        Parameters
        ----------
        run:
            Completed ``PipelineRun`` supplying the ``run_id`` and timestamps.
        dq_result:
            Completed ``DQRunResult`` supplying per-rule failure counts.
        spark:
            Active SparkSession.

        Raises
        ------
        WriterException
            If the Delta write fails.
        """
        if not dq_result.rule_metrics:
            return

        target = f"{self._catalog}.{_SCHEMA}.{_TABLE}"
        rows = [self._build_row(run, dq_result, m) for m in dq_result.rule_metrics]

        try:
            df = spark.createDataFrame(rows)
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target)
            )
            self._log.info(
                "Rule execution history written",
                target=target,
                run_id=run.run_id,
                source_name=run.source_name,
                rules_written=len(rows),
            )
        except Exception as exc:
            raise WriterException(
                f"Failed to write rule execution history to {target}: {exc}"
            ) from exc

    @staticmethod
    def _build_row(
        run: PipelineRun,
        dq_result: DQRunResult,
        metric: Any,
    ) -> dict[str, Any]:
        """Build one audit row from a ``RuleMetric``."""
        rows_checked = dq_result.rows_read
        violations = metric.failed_rows
        pass_pct = (
            round((1 - violations / rows_checked) * 100, 4)
            if rows_checked > 0
            else 100.0
        )
        return {
            "run_id": run.run_id,
            "pipeline_name": run.pipeline_name,
            "source_name": run.source_name,
            "environment": run.environment,
            "rule_name": metric.rule,
            "column_name": metric.column,
            "severity": metric.severity,
            "rows_checked": rows_checked,
            "violations": violations,
            "pass_percentage": pass_pct,
            "execution_time_seconds": dq_result.execution_time_seconds,
            "run_timestamp": run.start_time,
        }
