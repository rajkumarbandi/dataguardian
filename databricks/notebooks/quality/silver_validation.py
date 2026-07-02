# Databricks notebook source
# DataGuardian — Silver Validation Notebook (Milestone 8)
#
# Pipeline: Bronze → Schema → Transformation → DQ → Contract → Silver
# Architecture: bootstrap() → run_pipeline() → print_summary()
#
# All initialisation logic lives in src/bootstrap.py.
# All pipeline orchestration lives in src/pipeline.py.
# This notebook is orchestration only — no business logic resides here.

# COMMAND ----------
# MAGIC %md
# MAGIC # DataGuardian: Silver Validation
# MAGIC
# MAGIC **Milestone 8** — CI/CD, Packaging & Deployment
# MAGIC
# MAGIC This notebook is the sole entry point for the Bronze → Silver validation
# MAGIC pipeline.  It calls three functions and nothing else:
# MAGIC
# MAGIC ```
# MAGIC PipelineBootstrap.initialize()   ← wires every component from YAML config
# MAGIC        ↓
# MAGIC run_pipeline()                   ← executes all 7 pipeline stages per source
# MAGIC        ↓
# MAGIC print_summary()                  ← structured output + audit table display
# MAGIC ```
# MAGIC
# MAGIC ### Widget parameters
# MAGIC | Widget | Example | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `env` | `dev` | Environment — drives catalog, ADLS root, and policy |
# MAGIC | `source_name` | `customers` or `all` | Single source or all sources |
# MAGIC | `batch_id` | `latest` | Bronze batch; `latest` uses the most recent `_load_date` |
# MAGIC | `secrets_scope` | `dataguardian-prod-scope` | Databricks secrets scope (blank = env-var fallback) |
# MAGIC
# MAGIC ### Audit tables written
# MAGIC | Table | Written by |
# MAGIC |-------|-----------|
# MAGIC | `audit.schema_registry` | SchemaRegistry (M5) |
# MAGIC | `audit.schema_history` | SchemaHistoryWriter (M5) |
# MAGIC | `audit.transformation_history` | TransformationHistoryWriter (M6) |
# MAGIC | `audit.dq_violations` | DQResultsWriter (M3) |
# MAGIC | `audit.dq_metrics` | MetricsWriter (M3) |
# MAGIC | `audit.contract_history` | ContractHistoryWriter (M7) |
# MAGIC | `audit.pipeline_run_history` | PipelineRunTracker (M4) |
# MAGIC | `audit.rule_execution_history` | PipelineRunTracker (M4) |

# COMMAND ----------
# MAGIC %md ## 1. Python path — resolve repo root

# COMMAND ----------

import sys

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore[name-defined]  # noqa: F821
    _nb_path = _ctx.notebookPath().get()
    _parts = _nb_path.rstrip("/").split("/")
    _anchor = next((i for i, p in enumerate(_parts) if p == "databricks"), None)
    _repo_root = "/".join(_parts[:_anchor]) if _anchor else "/Workspace/dataguardian"
    _notebook_name = _nb_path
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    print(f"Repo root: {_repo_root}")
except NameError:
    _repo_root = "."
    _notebook_name = "local"
    print("Running locally — using current working directory")

# COMMAND ----------
# MAGIC %md ## 2. Widgets

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment (dev | test | qa | prod)")  # type: ignore[name-defined]  # noqa: F821
dbutils.widgets.text("source_name", "all", "Source name or 'all'")  # type: ignore[name-defined]  # noqa: F821
dbutils.widgets.text("batch_id", "latest", "Bronze batch ID or 'latest'")  # type: ignore[name-defined]  # noqa: F821
dbutils.widgets.text("secrets_scope", "", "Databricks secrets scope (blank = env-var fallback)")  # type: ignore[name-defined]  # noqa: F821

env: str = dbutils.widgets.get("env")  # type: ignore[name-defined]  # noqa: F821
source_name_raw: str = dbutils.widgets.get("source_name")  # type: ignore[name-defined]  # noqa: F821
batch_id_param: str = dbutils.widgets.get("batch_id")  # type: ignore[name-defined]  # noqa: F821
secrets_scope_param: str = dbutils.widgets.get("secrets_scope")  # type: ignore[name-defined]  # noqa: F821

# COMMAND ----------
# MAGIC %md ## 3. Bootstrap — initialise all pipeline components

# COMMAND ----------

from src.bootstrap import PipelineBootstrap
from src.common.exceptions import PipelineExecutionException
from src.pipeline import discover_sources, run_pipeline
import pyspark.sql.functions as F

context = PipelineBootstrap.initialize(
    env=env,
    spark=spark,  # type: ignore[name-defined]  # noqa: F821 — provided by Databricks runtime
    dbutils=dbutils,  # type: ignore[name-defined]  # noqa: F821
    notebook_name=_notebook_name,
    secrets_scope=secrets_scope_param or None,
)

# COMMAND ----------
# MAGIC %md ## 4. Resolve sources and run pipeline

# COMMAND ----------

sources = discover_sources(
    source_name_raw,
    context,
    config_root=_repo_root + "/config",
)

context.logger.info(
    "Pipeline run starting",
    sources=sources,
    batch_id=batch_id_param,
    env=env,
    catalog=context.catalog,
)

summaries = [run_pipeline(context, source, batch_id_param) for source in sources]

# COMMAND ----------
# MAGIC %md ## 5. Per-source summaries

# COMMAND ----------

for summary in summaries:
    summary.print_summary()

# COMMAND ----------
# MAGIC %md ## 6. Job-level aggregate summary

# COMMAND ----------

total_read = sum(s.rows_read for s in summaries)
total_passed = sum(s.rows_passed for s in summaries)
total_failed = sum(s.rows_failed for s in summaries)
total_silver = sum(s.silver_rows_written for s in summaries)
succeeded = sum(1 for s in summaries if s.status == "SUCCESS")
failed_runs = len(summaries) - succeeded
overall_pass_rate = (total_passed / total_read * 100) if total_read > 0 else 0.0

context.logger.info(
    "Silver validation job complete",
    sources_processed=len(summaries),
    runs_succeeded=succeeded,
    runs_failed=failed_runs,
    total_rows_read=total_read,
    total_rows_passed=total_passed,
    total_rows_failed=total_failed,
    total_silver_written=total_silver,
    overall_pass_rate_pct=round(overall_pass_rate, 2),
)

summary_df = context.spark.createDataFrame([s.to_dict() for s in summaries])
display(summary_df.orderBy("source_name"))  # type: ignore[name-defined]  # noqa: F821

# COMMAND ----------
# MAGIC %md ## 7. Rule-level failure breakdown

# COMMAND ----------

run_ids = [s.run_id for s in summaries if s.status == "SUCCESS"]
if run_ids:
    try:
        rule_history_df = (
            context.spark.table(f"{context.catalog}.audit.rule_execution_history")
            .filter(F.col("run_id").isin(run_ids))
            .select(
                "source_name", "rule_name", "column_name", "severity",
                "rows_checked", "violations", "pass_percentage", "run_id",
            )
            .orderBy("source_name", "violations", ascending=[True, False])
        )
        display(rule_history_df)  # type: ignore[name-defined]  # noqa: F821
    except Exception as exc:
        context.logger.warning("Rule execution history not available", error=str(exc))

# COMMAND ----------
# MAGIC %md ## 8. Spot-check Silver tables

# COMMAND ----------

for summary in summaries:
    if summary.status == "SUCCESS" and summary.silver_rows_written > 0:
        silver_tbl = f"{context.catalog}.silver.erp_{summary.source_name}"
        try:
            display(  # type: ignore[name-defined]  # noqa: F821
                context.spark.table(silver_tbl)
                .orderBy(F.col("_silver_ingested_at").desc())
                .limit(5)
            )
        except Exception as exc:
            context.logger.warning(
                "Could not display Silver table", table=silver_tbl, error=str(exc)
            )

# COMMAND ----------
# MAGIC %md ## 9. Fail notebook on any pipeline run failure

# COMMAND ----------

if failed_runs > 0:
    failed_sources = [s.source_name for s in summaries if s.status == "FAILED"]
    context.logger.error(
        "Silver validation job failed",
        failed_sources=failed_sources,
        failed_count=failed_runs,
    )
    raise PipelineExecutionException(
        f"{failed_runs} source(s) failed Silver validation: {failed_sources}. "
        "Inspect audit.pipeline_run_history for details."
    )

context.logger.info(
    "Silver validation completed successfully",
    sources_processed=len(summaries),
    total_silver_written=total_silver,
    overall_pass_rate_pct=round(overall_pass_rate, 2),
)
