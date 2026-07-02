"""
Schema history writer — persists schema validation events to the audit layer.

``SchemaHistoryWriter`` appends one row to ``{catalog}.audit.schema_history``
after every schema validation regardless of outcome.  This provides a complete
audit trail of when schemas drifted, what changed, and how the pipeline
responded.

Soft-fail design
----------------
Delta write failures are caught and logged as WARNING — consistent with all
other M4 audit writers.  A failing history writer never aborts the pipeline.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.schema.schema_drift_report import SchemaDriftReport


class SchemaHistoryWriter:
    """
    Writes schema validation events to ``{catalog}.audit.schema_history``.

    Parameters
    ----------
    catalog:
        Unity Catalog name (e.g. ``dg_dev``).
    logger:
        Optional pre-bound logger.
    """

    _TABLE = "schema_history"
    _SCHEMA = "audit"

    def __init__(
        self,
        catalog: str,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._catalog = catalog
        self._log = logger or get_logger("dataguardian.schema.history_writer")
        self._table_fqn = f"{catalog}.{self._SCHEMA}.{self._TABLE}"

    def write(
        self,
        run_id: str,
        source_name: str,
        schema_version: int,
        drift_report: SchemaDriftReport | None,
        spark: SparkSession,
        is_first_run: bool = False,
    ) -> None:
        """
        Append a schema history row to ``{catalog}.audit.schema_history``.

        Safe to call with ``drift_report=None`` (first-run or no-drift cases).
        Delta write errors are caught and logged as WARNING.

        Parameters
        ----------
        run_id:
            Pipeline run ID — correlates schema events with pipeline_run_history.
        source_name:
            Source identifier.
        schema_version:
            Active schema version after validation.
        drift_report:
            ``SchemaDriftReport`` from the comparator, or ``None``.
        spark:
            Active SparkSession for the Delta write.
        is_first_run:
            ``True`` when this is the first-run baseline registration.
        """
        drift_detected = drift_report.has_drift if drift_report else False
        breaking_changes = drift_report.has_breaking_changes if drift_report else False
        missing_count = len(drift_report.missing_columns) if drift_report else 0
        additional_count = len(drift_report.additional_columns) if drift_report else 0
        type_change_count = len(drift_report.type_changes) if drift_report else 0
        nullability_count = len(drift_report.nullability_changes) if drift_report else 0
        evolution_mode = drift_report.evolution_mode if drift_report else ""
        drift_details = drift_report.to_json() if drift_report else "{}"

        try:
            from pyspark.sql import Row

            row = Row(
                run_id=run_id,
                source_name=source_name,
                schema_version=schema_version,
                is_first_run=is_first_run,
                drift_detected=drift_detected,
                breaking_changes=breaking_changes,
                missing_columns=missing_count,
                additional_columns=additional_count,
                type_changes=type_change_count,
                nullability_changes=nullability_count,
                evolution_mode=evolution_mode,
                drift_details=drift_details,
                recorded_at=datetime.now(tz=timezone.utc),
            )
            df = spark.createDataFrame([row])
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(self._table_fqn)
            )
            self._log.info(
                "Schema history written",
                run_id=run_id,
                source_name=source_name,
                schema_version=schema_version,
                drift_detected=drift_detected,
                table=self._table_fqn,
            )
        except Exception as exc:
            self._log.warning(
                "audit.schema_history write failed — schema history record skipped",
                run_id=run_id,
                source_name=source_name,
                error=str(exc),
            )
