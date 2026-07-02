"""
Pipeline run lifecycle management for the DataGuardian platform.

Every pipeline execution is represented by a ``PipelineRun`` — a mutable
tracking object created at the start and updated with metrics at completion.
``PipelineRunTracker`` manages the full lifecycle and writes completed runs
to the audit Delta tables.

The companion ``PipelineSummary`` is the immutable, notebook-friendly result
returned after every run — it contains all the information a data engineer
needs to assess the outcome without digging into raw audit tables.

Usage
-----
::

    tracker = PipelineRunTracker(spark=spark, env_config=env_config)
    run = tracker.start_run(source_name="customers")
    try:
        dq_result = engine.run(bronze_df, source_config, run.run_id)
        summary = tracker.complete_run(run, dq_result=dq_result)
    except Exception as exc:
        summary = tracker.fail_run(run, exc)
        raise
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.common.models import EnvironmentConfig
    from src.quality.results import DQRunResult


# ---------------------------------------------------------------------------
# PipelineRun — mutable tracking state
# ---------------------------------------------------------------------------


@dataclass
class PipelineRun:
    """
    Mutable tracking object for a single pipeline execution.

    Do not construct directly — use ``PipelineRunTracker.start_run()``.

    Attributes
    ----------
    run_id:
        UUID4 unique to this execution.  Correlates all audit records, Silver
        rows, failed-record rows, and violations written in the same run.
    pipeline_name:
        Human-readable name from ``env_config.pipeline.pipeline_name``.
    pipeline_version:
        Semantic version from ``env_config.pipeline.pipeline_version``.
    source_name:
        Source identifier (matches the YAML ``name:`` field).
    environment:
        Deployment target (dev | qa | prod | test).
    notebook_name:
        Databricks notebook path or "local" when running in tests.
    cluster_id:
        Databricks cluster ID — "unknown" when running locally.
    start_time:
        UTC timestamp recorded at run creation.
    """

    run_id: str
    pipeline_name: str
    pipeline_version: str
    source_name: str
    environment: str
    notebook_name: str
    cluster_id: str
    start_time: datetime

    # Fields updated during execution (mutable)
    end_time: datetime | None = None
    status: str = "RUNNING"          # RUNNING | SUCCESS | FAILED
    rows_read: int = 0
    rows_passed: int = 0
    rows_failed: int = 0
    rules_executed: int = 0
    records_written: int = 0         # rows promoted to Silver
    failed_records_written: int = 0
    error_message: str = ""

    # Schema management fields (Milestone 5)
    schema_version: int = 0
    schema_drift_detected: bool = False
    evolution_mode: str = "STRICT"

    # Transformation fields (Milestone 6)
    transformations_executed: int = 0
    transformation_failures: int = 0
    transformation_duration_seconds: float = 0.0

    # Contract validation fields (Milestone 7)
    contract_name: str = ""
    contract_version: str = ""
    contract_status: str = ""           # PASSED | FAILED | WARNING | SKIPPED
    contract_rules_passed: int = 0
    contract_rules_failed: int = 0
    contract_warnings: int = 0

    @property
    def duration_seconds(self) -> float:
        """Elapsed wall-clock time from start to end (0.0 while still running)."""
        if self.end_time is None:
            return 0.0
        return round((self.end_time - self.start_time).total_seconds(), 3)

    def to_dict(self) -> dict[str, Any]:
        """Serialisable representation for logging and display."""
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "source_name": self.source_name,
            "environment": self.environment,
            "notebook_name": self.notebook_name,
            "cluster_id": self.cluster_id,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "rows_read": self.rows_read,
            "rows_passed": self.rows_passed,
            "rows_failed": self.rows_failed,
            "rules_executed": self.rules_executed,
            "records_written": self.records_written,
            "failed_records_written": self.failed_records_written,
            "error_message": self.error_message,
            "schema_version": self.schema_version,
            "schema_drift_detected": self.schema_drift_detected,
            "evolution_mode": self.evolution_mode,
            "transformations_executed": self.transformations_executed,
            "transformation_failures": self.transformation_failures,
            "transformation_duration_seconds": self.transformation_duration_seconds,
            "contract_name": self.contract_name,
            "contract_version": self.contract_version,
            "contract_status": self.contract_status,
            "contract_rules_passed": self.contract_rules_passed,
            "contract_rules_failed": self.contract_rules_failed,
            "contract_warnings": self.contract_warnings,
        }


# ---------------------------------------------------------------------------
# PipelineSummary — immutable result returned to the notebook
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineSummary:
    """
    Immutable summary returned by ``PipelineRunTracker.complete_run()`` and
    ``PipelineRunTracker.fail_run()``.

    This is the structured object the notebook receives after each source run.
    Collect one per source, then aggregate for a job-level summary display.

    Attributes
    ----------
    success_rate:
        Fraction of rows that passed all DQ rules (0.0–1.0).
    silver_rows_written:
        Count of rows promoted to the Silver Delta table.
    failed_rows_written:
        Count of rows written to ``bronze.{table}_failed``.
    metrics_written:
        ``True`` when run metrics were persisted to ``audit.dq_metrics``.
    """

    run_id: str
    pipeline_name: str
    pipeline_version: str
    source_name: str
    environment: str
    status: str
    start_time: datetime
    end_time: datetime
    execution_time_seconds: float
    rows_read: int
    rows_passed: int
    rows_failed: int
    success_rate: float
    rules_executed: int
    silver_rows_written: int
    failed_rows_written: int
    metrics_written: bool
    error_message: str = ""

    # Schema management fields (Milestone 5)
    schema_version: int = 0
    schema_drift_detected: bool = False
    evolution_mode: str = "STRICT"

    # Transformation fields (Milestone 6)
    transformations_executed: int = 0
    transformation_failures: int = 0
    transformation_duration_seconds: float = 0.0

    # Contract validation fields (Milestone 7)
    contract_name: str = ""
    contract_version: str = ""
    contract_status: str = ""
    contract_rules_passed: int = 0
    contract_rules_failed: int = 0
    contract_warnings: int = 0

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_run(
        cls, run: PipelineRun, metrics_written: bool = False
    ) -> PipelineSummary:
        """Build an immutable summary from a completed ``PipelineRun``."""
        end = run.end_time or datetime.now(tz=timezone.utc)
        return cls(
            run_id=run.run_id,
            pipeline_name=run.pipeline_name,
            pipeline_version=run.pipeline_version,
            source_name=run.source_name,
            environment=run.environment,
            status=run.status,
            start_time=run.start_time,
            end_time=end,
            execution_time_seconds=run.duration_seconds,
            rows_read=run.rows_read,
            rows_passed=run.rows_passed,
            rows_failed=run.rows_failed,
            success_rate=(
                round(run.rows_passed / run.rows_read, 4) if run.rows_read > 0 else 0.0
            ),
            rules_executed=run.rules_executed,
            silver_rows_written=run.records_written,
            failed_rows_written=run.failed_records_written,
            metrics_written=metrics_written,
            error_message=run.error_message,
            schema_version=run.schema_version,
            schema_drift_detected=run.schema_drift_detected,
            evolution_mode=run.evolution_mode,
            transformations_executed=run.transformations_executed,
            transformation_failures=run.transformation_failures,
            transformation_duration_seconds=run.transformation_duration_seconds,
            contract_name=run.contract_name,
            contract_version=run.contract_version,
            contract_status=run.contract_status,
            contract_rules_passed=run.contract_rules_passed,
            contract_rules_failed=run.contract_rules_failed,
            contract_warnings=run.contract_warnings,
        )

    # ------------------------------------------------------------------
    # Display helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict suitable for DataFrame creation."""
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_version": self.pipeline_version,
            "source_name": self.source_name,
            "environment": self.environment,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "execution_time_seconds": self.execution_time_seconds,
            "rows_read": self.rows_read,
            "rows_passed": self.rows_passed,
            "rows_failed": self.rows_failed,
            "success_rate_pct": round(self.success_rate * 100, 2),
            "rules_executed": self.rules_executed,
            "silver_rows_written": self.silver_rows_written,
            "failed_rows_written": self.failed_rows_written,
            "metrics_written": self.metrics_written,
            "error_message": self.error_message,
            "schema_version": self.schema_version,
            "schema_drift_detected": self.schema_drift_detected,
            "evolution_mode": self.evolution_mode,
            "transformations_executed": self.transformations_executed,
            "transformation_failures": self.transformation_failures,
            "transformation_duration_seconds": self.transformation_duration_seconds,
            "contract_name": self.contract_name,
            "contract_version": self.contract_version,
            "contract_status": self.contract_status,
            "contract_rules_passed": self.contract_rules_passed,
            "contract_rules_failed": self.contract_rules_failed,
            "contract_warnings": self.contract_warnings,
        }

    def print_summary(self) -> None:
        """Print a human-readable summary block (designed for notebook cell output)."""
        status_line = f"[{self.status}]"
        bar = "=" * 70
        print(
            f"\n{bar}\n"
            f"  PIPELINE RUN SUMMARY  {status_line}\n"
            f"{bar}\n"
            f"  Run ID             : {self.run_id}\n"
            f"  Pipeline           : {self.pipeline_name} v{self.pipeline_version}\n"
            f"  Source             : {self.source_name}\n"
            f"  Environment        : {self.environment}\n"
            f"  Status             : {self.status}\n"
            f"  Execution time     : {self.execution_time_seconds:.1f}s\n"
            f"  ─────────────────────────────────────────────────────────────\n"
            f"  Rows read          : {self.rows_read:,}\n"
            f"  Rows passed        : {self.rows_passed:,}\n"
            f"  Rows failed        : {self.rows_failed:,}\n"
            f"  Success rate       : {self.success_rate:.1%}\n"
            f"  ─────────────────────────────────────────────────────────────\n"
            f"  Rules executed     : {self.rules_executed}\n"
            f"  Silver rows written: {self.silver_rows_written:,}\n"
            f"  Failed rows written: {self.failed_rows_written:,}\n"
            f"  Metrics written    : {self.metrics_written}\n"
            f"  ─────────────────────────────────────────────────────────────\n"
            f"  Schema version     : {self.schema_version}\n"
            f"  Schema drift       : {'Yes' if self.schema_drift_detected else 'No'}\n"
            f"  Evolution mode     : {self.evolution_mode}\n"
            f"  ─────────────────────────────────────────────────────────────\n"
            f"  Transforms executed: {self.transformations_executed}\n"
            f"  Transform failures : {self.transformation_failures}\n"
            f"  Transform duration : {self.transformation_duration_seconds:.3f}s\n"
            f"  ─────────────────────────────────────────────────────────────\n"
            f"  Contract name      : {self.contract_name or 'none'}\n"
            f"  Contract version   : {self.contract_version or 'n/a'}\n"
            f"  Contract status    : {self.contract_status or 'SKIPPED'}\n"
            f"  Contract passed    : {self.contract_rules_passed}\n"
            f"  Contract failed    : {self.contract_rules_failed}\n"
            f"  Contract warnings  : {self.contract_warnings}\n"
            + (f"  Error              : {self.error_message}\n" if self.error_message else "")
            + f"{bar}\n"
        )


# ---------------------------------------------------------------------------
# PipelineRunTracker — lifecycle management
# ---------------------------------------------------------------------------


class PipelineRunTracker:
    """
    Manages the lifecycle of ``PipelineRun`` objects and writes completed runs
    to the audit Delta tables (``audit.pipeline_run_history`` and
    ``audit.rule_execution_history``).

    Audit table writes are fire-and-forget with soft failure — a write error
    logs a WARNING but never aborts the pipeline.  This ensures the audit
    layer does not become a point of failure for the data pipeline itself.

    Parameters
    ----------
    spark:
        Active SparkSession.
    env_config:
        Full environment configuration — provides catalog, pipeline settings.
    notebook_name:
        Path or name of the executing notebook (pass ``__file__`` in tests).
    logger:
        Optional pre-bound logger; defaults to a module-level logger.
    """

    def __init__(
        self,
        spark: SparkSession,
        env_config: EnvironmentConfig,
        notebook_name: str = "",
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._spark = spark
        self._env_config = env_config
        self._catalog = env_config.unity_catalog.catalog
        self._pipeline_cfg = env_config.pipeline
        self._notebook_name = notebook_name
        self._log = logger or get_logger("dataguardian.pipeline.tracker")
        self._cluster_id = self._detect_cluster_id()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_run(self, source_name: str) -> PipelineRun:
        """
        Create and return a new ``PipelineRun`` in ``RUNNING`` status.

        Parameters
        ----------
        source_name:
            Source identifier for this run (e.g. ``"customers"``).
        """
        run = PipelineRun(
            run_id=str(uuid.uuid4()),
            pipeline_name=self._pipeline_cfg.pipeline_name,
            pipeline_version=self._pipeline_cfg.pipeline_version,
            source_name=source_name,
            environment=self._env_config.environment,
            notebook_name=self._notebook_name,
            cluster_id=self._cluster_id,
            start_time=datetime.now(tz=timezone.utc),
        )
        self._log.info(
            "Pipeline run started",
            run_id=run.run_id,
            pipeline_name=run.pipeline_name,
            pipeline_version=run.pipeline_version,
            source_name=source_name,
            environment=run.environment,
            notebook=self._notebook_name,
        )
        return run

    def complete_run(
        self,
        run: PipelineRun,
        dq_result: DQRunResult | None = None,
        metrics_written: bool = False,
        records_written: int = 0,
        failed_records_written: int = 0,
    ) -> PipelineSummary:
        """
        Mark ``run`` as ``SUCCESS``, extract DQ metrics, and write to audit tables.

        Parameters
        ----------
        run:
            The active ``PipelineRun`` to complete.
        dq_result:
            Optional result from ``DataQualityEngine.run()`` — when provided,
            all scalar metrics are extracted automatically.
        metrics_written:
            ``True`` when ``MetricsWriter.write()`` succeeded for this run.
        records_written:
            Silver row count (used only when ``dq_result`` is ``None``).
        failed_records_written:
            Failed-record row count (used only when ``dq_result`` is ``None``).
        """
        run.end_time = datetime.now(tz=timezone.utc)
        run.status = "SUCCESS"

        if dq_result is not None:
            run.rows_read = dq_result.rows_read
            run.rows_passed = dq_result.rows_passed
            run.rows_failed = dq_result.rows_failed
            run.rules_executed = len(dq_result.rule_metrics)
            run.records_written = dq_result.rows_passed
            run.failed_records_written = dq_result.rows_failed
        else:
            run.records_written = records_written
            run.failed_records_written = failed_records_written

        self._persist_run_history(run)
        if dq_result is not None:
            self._persist_rule_history(run, dq_result)

        summary = PipelineSummary.from_run(run, metrics_written=metrics_written)

        self._log.info(
            "Pipeline run completed successfully",
            run_id=run.run_id,
            source_name=run.source_name,
            status="SUCCESS",
            duration_seconds=run.duration_seconds,
            rows_read=run.rows_read,
            rows_passed=run.rows_passed,
            rows_failed=run.rows_failed,
            success_rate=f"{summary.success_rate:.1%}",
            silver_rows_written=run.records_written,
        )
        return summary

    def fail_run(self, run: PipelineRun, error: Exception) -> PipelineSummary:
        """
        Mark ``run`` as ``FAILED``, write to audit tables, and return summary.

        This method never raises — it absorbs the audit write so the caller
        can re-raise the original exception cleanly.

        Parameters
        ----------
        run:
            The active ``PipelineRun`` that failed.
        error:
            The exception that caused the failure.
        """
        run.end_time = datetime.now(tz=timezone.utc)
        run.status = "FAILED"
        run.error_message = str(error)

        self._persist_run_history(run)

        summary = PipelineSummary.from_run(run, metrics_written=False)

        self._log.error(
            "Pipeline run failed",
            run_id=run.run_id,
            source_name=run.source_name,
            status="FAILED",
            duration_seconds=run.duration_seconds,
            error=str(error),
        )
        return summary

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _persist_run_history(self, run: PipelineRun) -> None:
        """Write to audit.pipeline_run_history; soft-fail on error."""
        if not self._pipeline_cfg.audit_enabled:
            return
        try:
            from src.audit.run_history_writer import RunHistoryWriter
            RunHistoryWriter(catalog=self._catalog, logger=self._log).write(
                run, self._spark
            )
        except Exception as exc:
            self._log.warning(
                "audit.pipeline_run_history write failed — audit record skipped",
                run_id=run.run_id,
                error=str(exc),
            )

    def _persist_rule_history(
        self, run: PipelineRun, dq_result: DQRunResult
    ) -> None:
        """Write to audit.rule_execution_history; soft-fail on error."""
        if not self._pipeline_cfg.audit_enabled:
            return
        try:
            from src.audit.rule_history_writer import RuleHistoryWriter
            RuleHistoryWriter(catalog=self._catalog, logger=self._log).write(
                run, dq_result, self._spark
            )
        except Exception as exc:
            self._log.warning(
                "audit.rule_execution_history write failed — audit record skipped",
                run_id=run.run_id,
                error=str(exc),
            )

    def _detect_cluster_id(self) -> str:
        """Return the Databricks cluster ID, or 'unknown' in local mode."""
        try:
            return self._spark.conf.get(
                "spark.databricks.clusterUsageTags.clusterId", "unknown"
            )
        except Exception:
            return "unknown"
