"""Audit writer for transformation execution history."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.transformations.results import TransformationMetric

logger = logging.getLogger(__name__)

_TABLE = "transformation_history"
_SCHEMA = "audit"


class TransformationHistoryWriter:
    """Write per-transformation metrics to ``audit.transformation_history``.

    All Delta writes are soft-fail — a write error logs a WARNING and never
    propagates to the calling pipeline.
    """

    def __init__(self, catalog: str, enabled: bool = True) -> None:
        self._catalog = catalog
        self._enabled = enabled
        self._full_table = f"{catalog}.{_SCHEMA}.{_TABLE}"

    def write(
        self,
        run_id: str,
        source_name: str,
        metrics: list[TransformationMetric],
        spark: SparkSession,
    ) -> None:
        if not self._enabled or not metrics:
            return

        try:
            self._ensure_table(spark)
            rows = [self._to_row(run_id, source_name, m) for m in metrics]
            df = spark.createDataFrame(rows, schema=self._row_schema())
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(self._full_table)
            )
        except Exception as exc:
            logger.warning(
                "TransformationHistoryWriter: failed to write to %s — %s",
                self._full_table,
                exc,
            )

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _to_row(
        self,
        run_id: str,
        source_name: str,
        metric: TransformationMetric,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "source_name": source_name,
            "transformation_type": metric.transformation_type,
            "execution_order": metric.execution_order,
            "execution_time_seconds": float(metric.execution_time_seconds),
            "rows_before": int(metric.rows_before),
            "rows_after": int(metric.rows_after),
            "columns_before": int(metric.columns_before),
            "columns_after": int(metric.columns_after),
            "columns_added": metric.columns_added_str,
            "columns_removed": metric.columns_removed_str,
            "status": metric.status,
            "error_message": metric.error_message,
            "description": metric.description,
            "recorded_at": datetime.now(tz=timezone.utc),
        }

    def _row_schema(self) -> str:
        return (
            "run_id STRING, source_name STRING, transformation_type STRING, "
            "execution_order INT, execution_time_seconds DOUBLE, "
            "rows_before LONG, rows_after LONG, "
            "columns_before INT, columns_after INT, "
            "columns_added STRING, columns_removed STRING, "
            "status STRING, error_message STRING, description STRING, "
            "recorded_at TIMESTAMP"
        )

    def _ensure_table(self, spark: SparkSession) -> None:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._catalog}.{_SCHEMA}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._full_table} (
                run_id              STRING,
                source_name         STRING,
                transformation_type STRING,
                execution_order     INT,
                execution_time_seconds DOUBLE,
                rows_before         LONG,
                rows_after          LONG,
                columns_before      INT,
                columns_after       INT,
                columns_added       STRING,
                columns_removed     STRING,
                status              STRING,
                error_message       STRING,
                description         STRING,
                recorded_at         TIMESTAMP
            )
            USING DELTA
            COMMENT 'Per-step transformation execution metrics written by the TransformationEngine'
        """)
