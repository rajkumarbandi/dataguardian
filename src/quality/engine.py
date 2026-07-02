"""
DataQualityEngine — orchestrates YAML-driven DQ validation on a Bronze DataFrame.

Design principles
-----------------
* The engine does NOT write anything.  It returns a ``DQRunResult`` containing
  output DataFrames and scalar metrics.  The caller (notebook, DAG) decides
  what to persist and where.
* All rules are resolved from ``RuleRegistry`` at runtime — the engine never
  imports a concrete rule class directly.
* The Bronze DataFrame is processed in a single Spark job: one cache, one
  aggregation for rule metrics, two filter operations for pass/fail split.
* Importing this module auto-registers all built-in rules via the
  ``src.quality.rules`` package side-effect import.
"""

from __future__ import annotations

import time
import uuid
from functools import reduce
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

# Side-effect import: registers all 11 built-in rules with RuleRegistry
import src.quality.rules  # noqa: F401
from src.common.exceptions import DataGuardianError
from src.common.logger import DataGuardianLogger, get_logger
from src.quality.registry import RuleRegistry
from src.quality.results import DQRunResult, RuleMetric

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from src.common.models import DQRuleConfig, SourceConfig


class DataQualityEngine:
    """
    Applies YAML-declared DQ rules to a Bronze DataFrame and returns a
    ``DQRunResult`` containing passed records, failed records, exploded
    violations, and aggregate metrics.

    Parameters
    ----------
    spark:
        Active SparkSession — passed to rules that need it (FK, SQL).
    catalog:
        Unity Catalog name (e.g. ``dg_dev``) — stored in run metadata.
    logger:
        Optional pre-bound logger; a default one is created if omitted.

    Usage
    -----
    ::

        engine = DataQualityEngine(spark, catalog="dg_dev")
        result = engine.run(bronze_df, source_config, batch_id="20240101_abc")

        # result.passed_df  → write to Silver
        # result.failed_df  → write to bronze.<table>_failed
        # result.violations_df → write to audit.dq_violations
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._spark = spark
        self._catalog = catalog
        self._log = logger or get_logger("dataguardian.quality.engine")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        bronze_df: DataFrame,
        source_config: SourceConfig,
        batch_id: str,
    ) -> DQRunResult:
        """
        Run all enabled DQ rules defined in ``source_config.dq_rules`` against
        ``bronze_df`` and return a ``DQRunResult``.

        The method is structured in four phases:
        1. **Annotate** — add a ``_dq_{idx:03d}_pass`` boolean column per rule.
        2. **Aggregate** — compute ``_dq_all_pass`` and ``_dq_violations`` array,
           then cache the annotated DataFrame.
        3. **Split** — separate passed / failed records and build violations_df.
        4. **Metrics** — aggregate per-rule failure counts in one Spark action.

        Parameters
        ----------
        bronze_df:
            Raw Bronze DataFrame (all columns as ingested).
        source_config:
            Parsed source YAML configuration including the ``dq_rules`` list.
        batch_id:
            Bronze batch identifier — correlates DQ results back to ingestion.

        Returns
        -------
        DQRunResult
            Contains output DataFrames and scalar metrics.  ``success=False``
            and a non-empty ``error_message`` on unexpected engine errors.
        """
        dq_run_id = str(uuid.uuid4())
        start_time = time.time()

        self._log.info(
            "DQ engine starting",
            source=source_config.name,
            batch_id=batch_id,
            dq_run_id=dq_run_id,
            total_rules=len(source_config.dq_rules),
        )

        try:
            return self._run_internal(
                bronze_df, source_config, batch_id, dq_run_id, start_time
            )
        except Exception as exc:
            execution_time = round(time.time() - start_time, 3)
            self._log.error(
                "DQ engine failed",
                source=source_config.name,
                batch_id=batch_id,
                dq_run_id=dq_run_id,
                error=str(exc),
            )
            return DQRunResult(
                source_name=source_config.name,
                dq_run_id=dq_run_id,
                batch_id=batch_id,
                passed_df=None,
                failed_df=None,
                violations_df=None,
                execution_time_seconds=execution_time,
                success=False,
                error_message=str(exc),
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_internal(
        self,
        bronze_df: DataFrame,
        source_config: SourceConfig,
        batch_id: str,
        dq_run_id: str,
        start_time: float,
    ) -> DQRunResult:
        enabled_rules = [r for r in source_config.dq_rules if r.enabled]

        # No rules declared → all rows pass, no violations
        if not enabled_rules:
            rows_read = bronze_df.count()
            self._log.info(
                "No enabled DQ rules — all rows pass",
                source=source_config.name,
                rows_read=rows_read,
            )
            return DQRunResult(
                source_name=source_config.name,
                dq_run_id=dq_run_id,
                batch_id=batch_id,
                passed_df=bronze_df,
                failed_df=None,
                violations_df=None,
                rows_read=rows_read,
                rows_passed=rows_read,
                rows_failed=0,
                pass_rate=1.0,
                execution_time_seconds=round(time.time() - start_time, 3),
            )

        # Phase 1: Annotate ─────────────────────────────────────────────
        df, applied_rules = self._annotate(bronze_df, enabled_rules)

        # Phase 2: Aggregate ────────────────────────────────────────────
        df = self._build_aggregate_columns(df, applied_rules, dq_run_id)
        df = df.cache()
        rows_read = df.count()

        # Phase 3: Split ────────────────────────────────────────────────
        pass_cols = [pc for pc, _, _ in applied_rules]
        meta_cols = ["_dq_all_pass", "_dq_record_id"] + pass_cols

        passed_df = (
            df.filter(F.col("_dq_all_pass"))
              .drop(*meta_cols, "_dq_violations", "_dq_run_id", "_dq_timestamp")
        )

        failed_df = (
            df.filter(~F.col("_dq_all_pass"))
              .drop(*pass_cols, "_dq_all_pass")
        )

        violations_df = self._build_violations_df(
            failed_df, source_config.name, batch_id
        )

        # Phase 4: Metrics ──────────────────────────────────────────────
        rule_metrics = self._aggregate_rule_metrics(df, applied_rules)
        rows_passed = passed_df.count()
        rows_failed = rows_read - rows_passed

        df.unpersist()

        pass_rate = round(rows_passed / rows_read, 4) if rows_read > 0 else 1.0
        execution_time = round(time.time() - start_time, 3)

        self._log.info(
            "DQ engine complete",
            source=source_config.name,
            batch_id=batch_id,
            dq_run_id=dq_run_id,
            rows_read=rows_read,
            rows_passed=rows_passed,
            rows_failed=rows_failed,
            pass_rate=pass_rate,
            execution_time_seconds=execution_time,
        )

        return DQRunResult(
            source_name=source_config.name,
            dq_run_id=dq_run_id,
            batch_id=batch_id,
            passed_df=passed_df,
            failed_df=failed_df,
            violations_df=violations_df,
            rows_read=rows_read,
            rows_passed=rows_passed,
            rows_failed=rows_failed,
            pass_rate=pass_rate,
            execution_time_seconds=execution_time,
            rule_metrics=rule_metrics,
            success=True,
            error_message="",
        )

    def _annotate(
        self,
        df: DataFrame,
        enabled_rules: list[DQRuleConfig],
    ) -> tuple[DataFrame, list[tuple[str, Any, Any]]]:
        """Apply each rule and return (annotated_df, applied_rules list)."""
        # Stable per-row identifier needed to correlate violations later
        df = df.withColumn("_dq_record_id", F.monotonically_increasing_id())

        applied_rules: list[tuple[str, Any, Any]] = []
        for idx, rule_cfg in enumerate(enabled_rules):
            pass_column = f"_dq_{idx:03d}_pass"
            rule = RuleRegistry.get(rule_cfg.rule)
            df = rule.apply(
                df, rule_cfg.column, pass_column, rule_cfg.params, self._spark
            )
            # Coalesce to false so null results (e.g. from sql_expression) fail
            df = df.withColumn(
                pass_column, F.coalesce(F.col(pass_column), F.lit(False))
            )
            applied_rules.append((pass_column, rule_cfg, rule))

        return df, applied_rules

    def _build_aggregate_columns(
        self,
        df: DataFrame,
        applied_rules: list[tuple[str, Any, Any]],
        dq_run_id: str,
    ) -> DataFrame:
        """Add _dq_all_pass, _dq_violations array, and run metadata columns."""
        all_pass_cols = [F.col(pc) for pc, _, _ in applied_rules]
        _dq_all_pass_expr = reduce(lambda a, b: a & b, all_pass_cols)
        df = df.withColumn("_dq_all_pass", _dq_all_pass_expr)

        violation_structs = []
        for pass_col, rule_cfg, rule in applied_rules:
            error_msg = rule.error_message(rule_cfg.column, rule_cfg.params)
            violation_structs.append(
                F.when(
                    ~F.col(pass_col),
                    F.struct(
                        F.lit(rule_cfg.rule).alias("rule_name"),
                        F.lit(rule_cfg.column).alias("column_name"),
                        F.lit(rule_cfg.severity).alias("severity"),
                        F.lit(error_msg).alias("error_message"),
                        F.col(rule_cfg.column).cast("string").alias("failed_value"),
                    ),
                )
            )

        df = df.withColumn(
            "_dq_violations",
            F.filter(F.array(*violation_structs), lambda x: x.isNotNull()),
        )
        df = df.withColumn("_dq_run_id", F.lit(dq_run_id))
        df = df.withColumn("_dq_timestamp", F.current_timestamp())
        return df

    @staticmethod
    def _build_violations_df(
        failed_df: DataFrame,
        source_name: str,
        batch_id: str,
    ) -> DataFrame:
        """Explode _dq_violations into one row per rule failure per record."""
        return (
            failed_df
            .select(
                "_dq_record_id",
                "_dq_run_id",
                "_dq_timestamp",
                F.explode("_dq_violations").alias("_violation"),
            )
            .select(
                F.lit(source_name).alias("source_name"),
                F.col("_dq_run_id").alias("dq_run_id"),
                F.lit(batch_id).alias("batch_id"),
                F.col("_dq_record_id").alias("record_id"),
                F.col("_violation.rule_name").alias("rule_name"),
                F.col("_violation.column_name").alias("column_name"),
                F.col("_violation.severity").alias("severity"),
                F.col("_violation.error_message").alias("error_message"),
                F.col("_violation.failed_value").alias("failed_value"),
                F.col("_dq_timestamp").alias("ingestion_timestamp"),
            )
        )

    @staticmethod
    def _aggregate_rule_metrics(
        df: DataFrame,
        applied_rules: list[tuple[str, Any, Any]],
    ) -> list[RuleMetric]:
        """Count failures per rule in a single aggregation pass."""
        if not applied_rules:
            return []

        agg_exprs = [
            F.sum(F.when(~F.col(pc), 1).otherwise(0)).alias(pc)
            for pc, _, _ in applied_rules
        ]
        metrics_row = df.agg(*agg_exprs).collect()[0]

        return [
            RuleMetric(
                rule=rule_cfg.rule,
                column=rule_cfg.column,
                severity=rule_cfg.severity,
                failed_rows=int(metrics_row[pc] or 0),
                pass_column=pc,
            )
            for pc, rule_cfg, _ in applied_rules
        ]
