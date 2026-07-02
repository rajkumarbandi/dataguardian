"""
Pipeline run history writer for the DataGuardian audit layer.

``RunHistoryWriter`` appends one row to ``{catalog}.audit.pipeline_run_history``
for every completed or failed pipeline run.  This table is the primary source
of truth for pipeline health dashboards, SLA monitoring, and incident analysis.

Schema
------
One row per ``PipelineRun`` (i.e. one row per source per job execution).
The ``run_id`` column is globally unique and links this table to
``audit.rule_execution_history``, ``audit.dq_metrics``, and ``audit.dq_violations``.

Delta table design choices
--------------------------
* Append-only with ``mergeSchema=true`` so adding new columns never requires
  a manual table DDL change.
* No partition by default — the table is expected to be small (one row per
  pipeline run per source per day) and partition overhead would not help.
* Created automatically on first write via ``saveAsTable``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.common.exceptions import WriterException
from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.common.pipeline_run import PipelineRun

_TABLE = "pipeline_run_history"
_SCHEMA = "audit"


class RunHistoryWriter:
    """
    Writes ``PipelineRun`` records to ``{catalog}.audit.pipeline_run_history``.

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
        self._log = logger or get_logger("dataguardian.audit.run_history")

    def write(self, run: PipelineRun, spark: SparkSession) -> None:
        """
        Append a completed ``PipelineRun`` to ``audit.pipeline_run_history``.

        Parameters
        ----------
        run:
            Completed (status = SUCCESS or FAILED) ``PipelineRun``.
        spark:
            Active SparkSession.

        Raises
        ------
        WriterException
            If the Delta write fails after the engine's retry policy has been
            exhausted.  Callers (PipelineRunTracker) catch this and log a
            WARNING so audit failures never abort the pipeline.
        """
        target = f"{self._catalog}.{_SCHEMA}.{_TABLE}"
        row = self._build_row(run)

        try:
            df = spark.createDataFrame([row])
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target)
            )
            self._log.info(
                "Pipeline run history written",
                target=target,
                run_id=run.run_id,
                source_name=run.source_name,
                status=run.status,
            )
        except Exception as exc:
            raise WriterException(
                f"Failed to write pipeline run history to {target}: {exc}"
            ) from exc

    @staticmethod
    def _build_row(run: PipelineRun) -> dict[str, Any]:
        """Convert a PipelineRun to a Delta-compatible row dict."""
        return {
            "run_id": run.run_id,
            "pipeline_name": run.pipeline_name,
            "pipeline_version": run.pipeline_version,
            "source_name": run.source_name,
            "environment": run.environment,
            "notebook_name": run.notebook_name,
            "cluster_id": run.cluster_id,
            "status": run.status,
            "start_time": run.start_time,
            "end_time": run.end_time,
            "duration_seconds": run.duration_seconds,
            "rows_read": run.rows_read,
            "rows_passed": run.rows_passed,
            "rows_failed": run.rows_failed,
            "rules_executed": run.rules_executed,
            "records_written": run.records_written,
            "failed_records_written": run.failed_records_written,
            "error_message": run.error_message,
        }
