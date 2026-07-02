"""
Execution metadata injector for the DataGuardian platform.

``ExecutionMetadataInjector`` stamps every DataFrame written to Silver or
Failed-Records with a standard set of pipeline run metadata columns.  This
makes every output row traceable back to the exact pipeline run, version,
and source that produced it — without requiring the notebook to manage these
columns manually.

Injected columns
----------------
+---------------------------+----------+-------------------------------------------+
| Column                    | Type     | Value                                     |
+===========================+==========+===========================================+
| ``_run_id``               | string   | UUID4 of the ``PipelineRun``              |
+---------------------------+----------+-------------------------------------------+
| ``_pipeline_name``        | string   | From ``env_config.pipeline.pipeline_name``|
+---------------------------+----------+-------------------------------------------+
| ``_pipeline_version``     | string   | Semantic version of the pipeline          |
+---------------------------+----------+-------------------------------------------+
| ``_source_name``          | string   | Source YAML ``name:`` field               |
+---------------------------+----------+-------------------------------------------+
| ``_pipeline_run_timestamp``| timestamp| UTC start time of the pipeline run        |
+---------------------------+----------+-------------------------------------------+

All column names use the ``_`` prefix convention so they sort after business
columns in schema browsers and are visually distinct from source data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from src.common.pipeline_run import PipelineRun


class ExecutionMetadataInjector:
    """
    Adds standard pipeline run metadata columns to a DataFrame before writing.

    This class is stateless — a single instance can be reused across multiple
    DataFrames and pipeline runs.

    Usage
    -----
    ::

        injector = ExecutionMetadataInjector()
        silver_df = injector.inject(passed_df, run)
        silver_writer.write(silver_df, ...)
    """

    def inject(self, df: DataFrame, run: PipelineRun) -> DataFrame:
        """
        Return a new DataFrame with five pipeline run metadata columns added.

        Existing ``_run_id`` or ``_pipeline_*`` columns are overwritten to
        ensure consistency when the injector is called on a DataFrame that was
        previously annotated by the engine (e.g. the ``_dq_run_id`` column
        from the DQ engine is separate from ``_run_id``).

        Parameters
        ----------
        df:
            Input DataFrame (Silver passed records or Bronze failed records).
        run:
            The active or completed ``PipelineRun`` supplying the metadata.

        Returns
        -------
        DataFrame
            A new DataFrame with the five metadata columns appended as the
            last columns (or overwritten if they already exist).
        """
        return (
            df
            .withColumn("_run_id", F.lit(run.run_id))
            .withColumn("_pipeline_name", F.lit(run.pipeline_name))
            .withColumn("_pipeline_version", F.lit(run.pipeline_version))
            .withColumn("_source_name", F.lit(run.source_name))
            .withColumn(
                "_pipeline_run_timestamp",
                F.to_timestamp(F.lit(run.start_time.isoformat())),
            )
        )
