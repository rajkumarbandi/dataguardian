"""
Silver layer writer for the DataGuardian platform.

``SilverWriter`` writes validated records produced by ``DataQualityEngine``
to the Silver Delta table in Unity Catalog.

Silver Layer Contract
---------------------
* Only rows that passed **every** enabled DQ rule reach Silver.
* All DQ metadata columns (``_dq_*``) are stripped before writing.
* No business transformations at this layer — Silver records are structurally
  identical to their Bronze counterparts minus the failed rows and DQ columns.
* Partitioned by ``_load_date`` (inherited from Bronze ingestion metadata).
* Schema evolution is allowed via ``mergeSchema`` to accommodate source system
  schema changes without manual table DDL.

Milestone 4 additions
---------------------
The ``write()`` method accepts an optional ``run`` parameter.  When provided,
``ExecutionMetadataInjector`` automatically stamps every Silver row with
pipeline run metadata (``_run_id``, ``_pipeline_name``, ``_pipeline_version``,
``_source_name``, ``_pipeline_run_timestamp``) — no notebook code required.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pyspark.sql.functions as F

from src.common.exceptions import WriterException
from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from src.common.models import SourceConfig
    from src.common.pipeline_run import PipelineRun

# All DQ columns added by the engine match this prefix pattern
_DQ_COLUMN_PATTERN = re.compile(r"^_dq_")


class SilverWriter:
    """
    Writes DQ-validated records to the Silver Delta table.

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
        self._log = logger or get_logger("dataguardian.silver.writer")

    def write(
        self,
        passed_df: DataFrame,
        source_config: SourceConfig,
        dq_run_id: str,
        run: PipelineRun | None = None,
    ) -> str:
        """
        Write DQ-passed records to ``{catalog}.silver.{table}``.

        Strips all ``_dq_*`` columns before writing.  The ``_load_date``
        partition column added by Bronze ingestion is preserved so Silver
        inherits the same partition scheme.

        Parameters
        ----------
        passed_df:
            DataFrame of rows that passed all DQ rules.  DQ columns are
            already stripped by the engine before this call.
        source_config:
            Parsed source YAML — provides the target table name.
        dq_run_id:
            DQ engine run ID logged for traceability.
        run:
            Optional ``PipelineRun`` from ``PipelineRunTracker``.  When
            provided, pipeline-level metadata columns are injected automatically
            (``_run_id``, ``_pipeline_name``, ``_pipeline_version``,
            ``_source_name``, ``_pipeline_run_timestamp``).

        Returns
        -------
        str
            Fully qualified Silver table name written to.
        """
        # Drop any residual _dq_* columns (defensive — engine should have done this)
        dq_cols = [c for c in passed_df.columns if _DQ_COLUMN_PATTERN.match(c)]
        if dq_cols:
            passed_df = passed_df.drop(*dq_cols)

        # Add Silver-layer DQ provenance columns
        passed_df = passed_df.withColumn("_dq_run_id", F.lit(dq_run_id))
        passed_df = passed_df.withColumn("_silver_ingested_at", F.current_timestamp())

        # Inject pipeline run metadata when a run context is available (M4)
        if run is not None:
            from src.audit.metadata_injector import ExecutionMetadataInjector
            passed_df = ExecutionMetadataInjector().inject(passed_df, run)

        silver_table = f"{self._catalog}.silver.{source_config.target.table}"
        partition_col = source_config.target.partition_by

        try:
            writer = (
                passed_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
            )
            if partition_col in passed_df.columns:
                writer = writer.partitionBy(partition_col)

            writer.saveAsTable(silver_table)

            self._log.info(
                "Silver records written",
                target=silver_table,
                dq_run_id=dq_run_id,
                source=source_config.name,
                run_id=run.run_id if run else None,
            )
            return silver_table

        except Exception as exc:
            raise WriterException(
                f"Failed to write Silver records to {silver_table}: {exc}"
            ) from exc
