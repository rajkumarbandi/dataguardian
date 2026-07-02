"""
DQ metrics writer for the DataGuardian platform.

``MetricsWriter`` persists ``DQRunResult`` summary statistics to the
``audit.dq_metrics`` Delta table.  This table is the source of truth for
pipeline health dashboards and SLA monitoring.

Schema
------
Every row in ``audit.dq_metrics`` corresponds to one ``DataQualityEngine.run()``
invocation for one source.  The ``rule_metrics_json`` column stores per-rule
failure counts as a JSON string to avoid schema evolution when new rules are added.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from src.common.exceptions import ConfigurationError
from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.quality.results import DQRunResult

# Schema for the metrics Delta table (created on first write via mergeSchema)
_METRICS_TABLE = "dq_metrics"
_METRICS_SCHEMA = "audit"


class MetricsWriter:
    """
    Writes ``DQRunResult`` summary statistics to ``{catalog}.audit.dq_metrics``.

    Parameters
    ----------
    spark:
        Active ``SparkSession``.
    catalog:
        Unity Catalog name (e.g. ``dg_dev``).
    logger:
        Optional pre-bound logger.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._spark = spark
        self._catalog = catalog
        self._log = logger or get_logger("dataguardian.quality.metrics")

    def write(self, result: DQRunResult, environment: str) -> None:
        """
        Persist DQ run metrics to ``{catalog}.audit.dq_metrics``.

        Parameters
        ----------
        result:
            The completed ``DQRunResult`` from ``DataQualityEngine.run()``.
        environment:
            Environment identifier (dev / qa / prod) — stored as a column for
            cross-environment comparison in dashboards.
        """
        row = self._build_row(result, environment)
        df = self._spark.createDataFrame([row])

        target = f"{self._catalog}.{_METRICS_SCHEMA}.{_METRICS_TABLE}"
        try:
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target)
            )
            self._log.info(
                "DQ metrics written",
                target=target,
                dq_run_id=result.dq_run_id,
                source=result.source_name,
            )
        except Exception as exc:
            raise ConfigurationError(
                f"Failed to write DQ metrics to {target}: {exc}"
            ) from exc

    @staticmethod
    def _build_row(result: DQRunResult, environment: str) -> dict[str, Any]:
        rule_metrics_payload = [
            {
                "rule": m.rule,
                "column": m.column,
                "severity": m.severity,
                "failed_rows": m.failed_rows,
            }
            for m in result.rule_metrics
        ]
        return {
            "dq_run_id": result.dq_run_id,
            "source_name": result.source_name,
            "batch_id": result.batch_id,
            "environment": environment,
            "rows_read": result.rows_read,
            "rows_passed": result.rows_passed,
            "rows_failed": result.rows_failed,
            "pass_rate": result.pass_rate,
            "execution_time_seconds": result.execution_time_seconds,
            "rule_metrics_json": json.dumps(rule_metrics_payload),
            "success": result.success,
            "error_message": result.error_message,
        }
