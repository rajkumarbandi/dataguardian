"""
DQ results writers for the DataGuardian platform.

``DQResultsWriter`` persists the two DQ output DataFrames produced by
``DataQualityEngine.run()``:

* **violations** → ``{catalog}.audit.dq_violations``
  One row per rule failure per record.  Used by the Stewardship App.

* **failed records** → ``{catalog}.bronze.{table}_failed``
  One row per Bronze record that failed at least one DQ rule.
  Includes the full ``_dq_violations`` array so data stewards can inspect
  all failures on a single row without joining to the violations table.

Writers are deliberately separate from the engine so that callers (notebooks,
integration tests) can choose which outputs to persist, and so the engine
remains testable without any Delta dependency.

Milestone 4 additions
---------------------
Both ``write_violations()`` and ``write_failed()`` accept an optional ``run``
parameter.  When provided, ``ExecutionMetadataInjector`` stamps every row with
pipeline-level metadata before writing — ensuring full traceability without
any notebook-side column management.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.common.exceptions import WriterException
from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from src.common.pipeline_run import PipelineRun
    from src.quality.results import DQRunResult


class DQResultsWriter:
    """
    Writes DQ output DataFrames produced by ``DataQualityEngine.run()``.

    Parameters
    ----------
    catalog:
        Unity Catalog name (e.g. ``dg_dev``).
    logger:
        Optional pre-bound logger.
    """

    _VIOLATIONS_TABLE = "dq_violations"
    _VIOLATIONS_SCHEMA = "audit"

    def __init__(
        self,
        catalog: str,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._catalog = catalog
        self._log = logger or get_logger("dataguardian.quality.writers")

    def write_all(
        self,
        result: DQRunResult,
        table_name: str,
        run: PipelineRun | None = None,
    ) -> None:
        """
        Convenience method: write violations and failed records if available.

        Parameters
        ----------
        result:
            Completed ``DQRunResult`` from the engine.
        table_name:
            Bronze target table name (e.g. ``erp_customers``).  Failed records
            are written to ``{catalog}.bronze.{table_name}_failed``.
        run:
            Optional ``PipelineRun`` — when provided, pipeline metadata is
            injected into failed-record rows automatically.
        """
        if result.violations_df is not None:
            self.write_violations(result.violations_df, result.dq_run_id)
        if result.failed_df is not None:
            self.write_failed(result.failed_df, table_name, result.dq_run_id, run=run)

    def write_violations(
        self,
        violations_df: DataFrame,
        dq_run_id: str,
    ) -> None:
        """
        Append exploded violations to ``{catalog}.audit.dq_violations``.

        Violations rows already carry ``source_name``, ``dq_run_id``, and
        ``batch_id`` from the engine — no additional metadata injection needed.

        Parameters
        ----------
        violations_df:
            Exploded violations DataFrame from ``DQRunResult.violations_df``.
        dq_run_id:
            Run ID for log correlation.
        """
        target = f"{self._catalog}.{self._VIOLATIONS_SCHEMA}.{self._VIOLATIONS_TABLE}"
        self._write_delta(violations_df, target, dq_run_id, "violations")

    def write_failed(
        self,
        failed_df: DataFrame,
        table_name: str,
        dq_run_id: str,
        run: PipelineRun | None = None,
    ) -> None:
        """
        Append failed Bronze records to ``{catalog}.bronze.{table_name}_failed``.

        The failed DataFrame retains all original Bronze columns plus the
        ``_dq_violations`` array, ``_dq_run_id``, and ``_dq_timestamp`` columns
        added by the engine.  When ``run`` is supplied, pipeline-level metadata
        columns are also injected before writing.

        Parameters
        ----------
        failed_df:
            Failed records DataFrame from ``DQRunResult.failed_df``.
        table_name:
            Target table base name (e.g. ``erp_customers``).  The writer
            appends ``_failed`` suffix automatically.
        dq_run_id:
            Run ID for log correlation.
        run:
            Optional ``PipelineRun`` — when provided, ``ExecutionMetadataInjector``
            stamps each row with ``_run_id``, ``_pipeline_name``,
            ``_pipeline_version``, ``_source_name``, ``_pipeline_run_timestamp``.
        """
        if run is not None:
            from src.audit.metadata_injector import ExecutionMetadataInjector
            failed_df = ExecutionMetadataInjector().inject(failed_df, run)

        target = f"{self._catalog}.bronze.{table_name}_failed"
        self._write_delta(failed_df, target, dq_run_id, "failed records")

    def _write_delta(
        self,
        df: DataFrame,
        target: str,
        dq_run_id: str,
        label: str,
    ) -> None:
        try:
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target)
            )
            self._log.info(
                f"DQ {label} written",
                target=target,
                dq_run_id=dq_run_id,
            )
        except Exception as exc:
            raise WriterException(
                f"Failed to write DQ {label} to {target}: {exc}"
            ) from exc
