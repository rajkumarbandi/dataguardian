"""
Pipeline runner — Milestone 8.

Provides two public functions consumed by the silver_validation notebook:

    discover_sources(source_name_raw, context, config_root)
        Resolves the ``source_name`` widget value to an ordered list of source
        names to process.  Supports the literal ``"all"`` to auto-discover
        every YAML file under ``config/sources/``.

    run_pipeline(context, source_name, batch_id)
        Executes the full Bronze → Silver pipeline for a single source and
        returns a ``PipelineSummary``.  Never raises — all failures are
        captured in the summary so the notebook can report them and continue.

Pipeline stages
---------------
1.  Load source YAML config via ConfigLoader
2.  Read the requested Bronze batch (``latest`` or explicit batch_id)
3.  Schema validation + audit (M5)
4.  Transformation engine + audit (M6)
5.  Data quality engine with retry (M3)
6.  Contract validation + audit (M7)
7.  Write failed records + DQ metrics (M3)
8.  Write Silver records (M2)
9.  Complete the PipelineRun and return PipelineSummary

Design invariants
-----------------
- ``run_pipeline`` never raises.  Any exception is caught, logged via
  ``context.logger``, and surfaced through ``tracker.fail_run``.
- No Spark configuration changes occur here.  The context already holds a
  fully configured ``SparkSession``.
- No imports from Milestone 1–7 source files are modified by this module.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

import pyspark.sql.functions as F

from src.common.exceptions import PipelineExecutionException, ValidationException
from src.common.pipeline_run import PipelineRun, PipelineSummary

if TYPE_CHECKING:
    from src.bootstrap import PipelineContext


# ---------------------------------------------------------------------------
# Source discovery
# ---------------------------------------------------------------------------


def discover_sources(
    source_name_raw: str,
    context: PipelineContext,
    config_root: str | None = None,
) -> list[str]:
    """
    Resolve the ``source_name`` widget value to an ordered list of source names.

    Parameters
    ----------
    source_name_raw:
        Widget value — either a single source name (e.g. ``"customers"``) or
        the special value ``"all"`` to process every source YAML.
    context:
        Active ``PipelineContext`` (used for logging only in this function).
    config_root:
        Absolute path to the repository root.  When given, the resolver looks
        in ``{config_root}/sources/`` first.  Falls back to ``config/sources/``
        relative to the current working directory.

    Returns
    -------
    list[str]
        Ordered list of source names (without the ``.yml`` extension).

    Raises
    ------
    ValidationException
        When ``"all"`` is requested but no source files can be found.
    """
    name = source_name_raw.strip()
    if name.lower() != "all":
        return [name]

    candidates: list[Path] = []
    if config_root:
        candidates.append(Path(config_root) / "sources")
    candidates.append(Path("config") / "sources")

    for candidate in candidates:
        if candidate.is_dir():
            sources = sorted(
                f.stem
                for f in candidate.glob("*.yml")
                if not f.name.startswith("_") and not f.name.startswith("example")
            )
            if sources:
                context.logger.info(
                    "Source discovery complete",
                    config_dir=str(candidate),
                    sources=sources,
                )
                return sources

    raise ValidationException(
        "No source YAML files found. Ensure config/sources/*.yml files are present "
        "and the repository is correctly mounted."
    )


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def run_pipeline(
    context: PipelineContext,
    source_name: str,
    batch_id: str = "latest",
) -> PipelineSummary:
    """
    Execute the full Bronze → Silver pipeline for *source_name*.

    Parameters
    ----------
    context:
        Fully initialised ``PipelineContext`` from ``PipelineBootstrap.initialize()``.
    source_name:
        Source name matching a YAML file in ``config/sources/``.
    batch_id:
        Bronze batch to process.  ``"latest"`` selects the most recent
        ``_load_date`` partition automatically.

    Returns
    -------
    PipelineSummary
        Immutable summary of the completed (or failed) run.
        Never raises — failures are captured inside the summary.
    """
    logger = context.logger
    logger.info("Source validation starting", source=source_name, batch_id=batch_id)

    run: PipelineRun = context.tracker.start_run(source_name=source_name)
    metrics_written = False

    try:
        # 1. Source configuration --------------------------------------------
        source_config = context.loader.get_source(source_name)
        logger.info(
            "Source configuration loaded",
            source=source_name,
            run_id=run.run_id,
            connector=source_config.connector.type,
            dq_rules=len(source_config.dq_rules),
            transformations=len(source_config.transformations),
            has_contract=source_config.contract is not None,
        )

        # 2. Bronze batch read -----------------------------------------------
        bronze_df, effective_batch_id = _get_bronze_batch(context, source_config, batch_id)
        bronze_count = bronze_df.count()
        logger.info(
            "Bronze batch loaded",
            source=source_name,
            run_id=run.run_id,
            batch_id=effective_batch_id,
            rows=bronze_count,
        )

        # 3. Schema validation (M5) ------------------------------------------
        evolution_mode: str = (
            source_config.schema_evolution.evolution_mode
            or context.env_config.schema_registry.default_evolution_mode
        )
        run.evolution_mode = evolution_mode

        schema_result = context.schema_validator.validate(
            df=bronze_df,
            source_config=source_config,
            evolution_mode=evolution_mode,
            run_id=run.run_id,
        )
        run.schema_version = schema_result.schema_version
        run.schema_drift_detected = (
            schema_result.drift_report.has_drift
            if schema_result.drift_report is not None
            else False
        )

        logger.info(
            "Schema validation complete",
            source=source_name,
            run_id=run.run_id,
            schema_version=schema_result.schema_version,
            evolution_mode=evolution_mode,
            drift_detected=run.schema_drift_detected,
            can_proceed=schema_result.can_proceed,
        )

        if context.env_config.schema_registry.schema_audit_enabled:
            context.schema_history_writer.write(
                run_id=run.run_id,
                source_name=source_name,
                schema_version=schema_result.schema_version,
                drift_report=schema_result.drift_report,
                spark=context.spark,
                is_first_run=schema_result.is_first_run,
            )

        if not schema_result.can_proceed:
            raise PipelineExecutionException(
                f"Schema validation failed for '{source_name}': {schema_result.message}"
            )

        bronze_df = schema_result.resolved_df

        # 4. Transformation engine (M6) --------------------------------------
        transform_start = time.time()
        transformation_result = context.transformation_engine.run(
            df=bronze_df,
            source_config=source_config,
            run_id=run.run_id,
            input_row_count=bronze_count,
        )

        if not transformation_result.success:
            raise PipelineExecutionException(
                f"Transformation engine failed for '{source_name}': "
                f"{transformation_result.error_message}"
            )

        bronze_df = transformation_result.output_df
        run.transformations_executed = transformation_result.transformations_executed
        run.transformation_failures = transformation_result.transformations_failed
        run.transformation_duration_seconds = round(time.time() - transform_start, 3)

        logger.info(
            "Transformation engine complete",
            source=source_name,
            run_id=run.run_id,
            steps_executed=transformation_result.transformations_executed,
            steps_failed=transformation_result.transformations_failed,
            steps_skipped=transformation_result.transformations_skipped,
            duration_seconds=run.transformation_duration_seconds,
        )

        if transformation_result.metrics:
            context.transformation_history_writer.write(
                run_id=run.run_id,
                source_name=source_name,
                metrics=transformation_result.metrics,
                spark=context.spark,
            )

        # 5. Data quality engine (M3) ----------------------------------------
        dq_result = context.retry.execute(
            context.dq_engine.run,
            bronze_df=bronze_df,
            source_config=source_config,
            batch_id=effective_batch_id,
        )

        if not dq_result.success:
            raise PipelineExecutionException(
                f"DQ engine reported failure: {dq_result.error_message}"
            )

        logger.info(
            "DQ validation complete",
            source=source_name,
            run_id=run.run_id,
            rows_read=dq_result.rows_read,
            rows_passed=dq_result.rows_passed,
            rows_failed=dq_result.rows_failed,
            pass_rate=f"{dq_result.pass_rate:.1%}",
            rules_executed=len(dq_result.rule_metrics),
        )

        # 6. Contract validation (M7) ----------------------------------------
        if (
            context.env_config.contract_validation.contract_validation_enabled
            and source_config.contract is not None
        ):
            contract_result = context.contract_engine.validate(
                source_config=source_config,
                schema_result=schema_result,
                dq_result=dq_result,
                transformation_result=transformation_result,
                df=bronze_df,
                row_count=dq_result.rows_read,
                run_id=run.run_id,
                env_policy=context.env_config.contract_validation.default_contract_policy,
            )

            run.contract_name = contract_result.contract_name
            run.contract_version = contract_result.contract_version
            run.contract_status = contract_result.status
            run.contract_rules_passed = contract_result.rules_passed
            run.contract_rules_failed = contract_result.rules_failed
            run.contract_warnings = contract_result.warnings

            logger.info(
                "Contract validation complete",
                source=source_name,
                run_id=run.run_id,
                contract=contract_result.contract_name,
                status=contract_result.status,
                rules_passed=contract_result.rules_passed,
                rules_failed=contract_result.rules_failed,
                warnings=contract_result.warnings,
                can_proceed=contract_result.can_proceed,
            )

            if context.env_config.contract_validation.contract_audit_enabled:
                context.contract_history_writer.write(
                    run_id=run.run_id,
                    source_name=source_name,
                    contract_result=contract_result,
                    spark=context.spark,
                )

            if not contract_result.can_proceed:
                raise PipelineExecutionException(
                    f"Contract validation failed for '{source_name}': "
                    f"{contract_result.message}"
                )
        else:
            run.contract_status = "SKIPPED"

        # 7. Write DQ results ------------------------------------------------
        context.dq_writer.write_all(
            dq_result,
            table_name=source_config.target.table,
            run=run,
        )
        context.metrics_writer.write(result=dq_result, environment=context.env)
        metrics_written = True

        logger.info(
            "DQ outputs written",
            source=source_name,
            run_id=run.run_id,
            failed_records=dq_result.rows_failed,
        )

        # 8. Write Silver ----------------------------------------------------
        if dq_result.passed_df is not None and dq_result.rows_passed > 0:
            silver_table = context.silver_writer.write(
                passed_df=dq_result.passed_df,
                source_config=source_config,
                dq_run_id=dq_result.dq_run_id,
                run=run,
            )
            logger.info(
                "Silver records written",
                source=source_name,
                run_id=run.run_id,
                silver_table=silver_table,
                rows_written=dq_result.rows_passed,
            )
        else:
            logger.warning(
                "No records passed DQ — Silver table not written",
                source=source_name,
                run_id=run.run_id,
                rows_read=dq_result.rows_read,
                rows_failed=dq_result.rows_failed,
            )

        # 9. Complete run ----------------------------------------------------
        return context.tracker.complete_run(
            run=run,
            dq_result=dq_result,
            metrics_written=metrics_written,
        )

    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Source validation failed",
            source=source_name,
            run_id=run.run_id,
            error=str(exc),
        )
        return context.tracker.fail_run(run=run, error=exc)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_bronze_batch(
    context: PipelineContext,
    source_config: object,
    batch_id: str,
) -> tuple[object, str]:
    """
    Read the correct Bronze batch.

    Returns
    -------
    (DataFrame, effective_batch_id)
    """
    bronze_table = source_config.target.full_table_name  # type: ignore[union-attr]
    df = context.spark.table(bronze_table)

    if batch_id.strip().lower() == "latest":
        latest_date = df.agg(F.max("_load_date")).collect()[0][0]
        if latest_date is None:
            raise ValidationException(
                f"Bronze table '{bronze_table}' is empty. "
                "Run bronze ingestion before Silver validation."
            )
        batch_df = df.filter(F.col("_load_date") == latest_date)
        sample = batch_df.select("_batch_id").limit(1).collect()
        effective_batch_id: str = (
            sample[0]["_batch_id"] if sample else str(latest_date)
        )
        context.logger.info(
            "Latest Bronze batch selected",
            bronze_table=bronze_table,
            load_date=str(latest_date),
            batch_id=effective_batch_id,
        )
        return batch_df, effective_batch_id

    batch_df = df.filter(F.col("_batch_id") == batch_id)
    row_count = batch_df.count()
    if row_count == 0:
        raise ValidationException(
            f"No Bronze records found for batch_id='{batch_id}' in '{bronze_table}'."
        )
    context.logger.info(
        "Explicit Bronze batch selected",
        bronze_table=bronze_table,
        batch_id=batch_id,
        row_count=row_count,
    )
    return batch_df, batch_id
