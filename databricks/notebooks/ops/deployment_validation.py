# Databricks notebook source
# DataGuardian — Deployment Validation Notebook (Milestone 8)
#
# Verifies that the target environment is ready before the first pipeline run.
# Run this after every new `databricks bundle deploy` to confirm the deployment.
#
# Usage:
#   databricks bundle run deployment_validation --target dev
#   databricks bundle run deployment_validation --target prod

# COMMAND ----------
# MAGIC %md
# MAGIC # DataGuardian: Deployment Validation
# MAGIC
# MAGIC **Purpose:** Verify the target Databricks environment is correctly
# MAGIC configured before running the DataGuardian pipeline for the first time
# MAGIC (or after a new deployment).
# MAGIC
# MAGIC ### Checks performed
# MAGIC | Check | Severity | Description |
# MAGIC |-------|----------|-------------|
# MAGIC | `catalog_exists` | error | Unity Catalog is accessible |
# MAGIC | `package_installed` | error | DataGuardian `src` package is importable |
# MAGIC | `env_config_catalog` | error | Catalog name is set in environment YAML |
# MAGIC | `schema_{name}` | warning | bronze/silver/audit schemas exist |
# MAGIC | `env_config_adls` | warning | ADLS root is configured |
# MAGIC | `audit_table_{name}` | info | Each audit Delta table exists (auto-created on first run) |
# MAGIC
# MAGIC ### Result
# MAGIC - **PASSED** — all error-severity checks passed; safe to run the pipeline
# MAGIC - **FAILED** — one or more error-severity checks failed; deployment needs attention

# COMMAND ----------
# MAGIC %md ## 1. Python path

# COMMAND ----------

import sys

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore[name-defined]  # noqa: F821
    _nb_path = _ctx.notebookPath().get()
    _parts = _nb_path.rstrip("/").split("/")
    _anchor = next((i for i, p in enumerate(_parts) if p == "databricks"), None)
    _repo_root = "/".join(_parts[:_anchor]) if _anchor else "/Workspace/dataguardian"
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    print(f"Repo root: {_repo_root}")
except NameError:
    _repo_root = "."
    print("Running locally — using current working directory")

# COMMAND ----------
# MAGIC %md ## 2. Widgets

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment (dev | test | qa | prod)")  # type: ignore[name-defined]  # noqa: F821
dbutils.widgets.text("secrets_scope", "", "Databricks secrets scope (blank = env-var fallback)")  # type: ignore[name-defined]  # noqa: F821
dbutils.widgets.text("fail_on_warnings", "false", "Fail notebook if warnings are found (true | false)")  # type: ignore[name-defined]  # noqa: F821

env: str = dbutils.widgets.get("env")  # type: ignore[name-defined]  # noqa: F821
secrets_scope_param: str = dbutils.widgets.get("secrets_scope")  # type: ignore[name-defined]  # noqa: F821
fail_on_warnings: bool = dbutils.widgets.get("fail_on_warnings").strip().lower() == "true"  # type: ignore[name-defined]  # noqa: F821

# COMMAND ----------
# MAGIC %md ## 3. Load configuration and initialise validator

# COMMAND ----------

from src.common.config_loader import ConfigLoader
from src.common.logger import get_logger
from src.deployment import DeploymentValidator

import os
os.environ["DATAGUARDIAN_ENV"] = env

loader = ConfigLoader(env=env)
env_config = loader.get_environment()
catalog = env_config.unity_catalog.catalog

logger = get_logger("dataguardian.ops.deployment_validation", env=env)
logger.info(
    "Deployment validation starting",
    env=env,
    catalog=catalog,
    spark_version=spark.version,  # type: ignore[name-defined]  # noqa: F821
)

# COMMAND ----------
# MAGIC %md ## 4. Run validation

# COMMAND ----------

validator = DeploymentValidator()
report = validator.validate(
    spark=spark,  # type: ignore[name-defined]  # noqa: F821
    catalog=catalog,
    env_config=env_config,
    environment=env,
)

# COMMAND ----------
# MAGIC %md ## 5. Print report

# COMMAND ----------

report.print_report()

# COMMAND ----------
# MAGIC %md ## 6. Display checks as a DataFrame

# COMMAND ----------

checks_df = spark.createDataFrame(  # type: ignore[name-defined]  # noqa: F821
    [c.to_dict() for c in report.checks]
).orderBy("severity", "passed", ascending=[True, True])
display(checks_df)  # type: ignore[name-defined]  # noqa: F821

# COMMAND ----------
# MAGIC %md ## 7. Structured summary log

# COMMAND ----------

logger.info(
    "Deployment validation complete",
    env=env,
    catalog=catalog,
    passed=report.passed,
    total_checks=len(report.checks),
    error_count=report.error_count,
    warning_count=report.warning_count,
    info_count=report.info_count,
)

# COMMAND ----------
# MAGIC %md ## 8. Fail on errors (or warnings if configured)

# COMMAND ----------

if not report.passed:
    error_checks = [c.name for c in report.checks if not c.passed and c.severity == "error"]
    logger.error(
        "Deployment validation FAILED — environment is not ready",
        failed_checks=error_checks,
        env=env,
    )
    raise RuntimeError(
        f"Deployment validation failed for environment '{env}'. "
        f"Failed error-severity checks: {error_checks}. "
        "Fix the issues listed above before running the pipeline."
    )

if fail_on_warnings and report.warning_count > 0:
    warning_checks = [c.name for c in report.checks if not c.passed and c.severity == "warning"]
    logger.warning(
        "Deployment validation has warnings (fail_on_warnings=true)",
        warning_checks=warning_checks,
        env=env,
    )
    raise RuntimeError(
        f"Deployment validation has {report.warning_count} warning(s) for environment '{env}'. "
        f"Warning checks: {warning_checks}."
    )

logger.info(
    "Deployment validation PASSED — environment is ready for DataGuardian",
    env=env,
    catalog=catalog,
)
print(f"\n✓ Environment '{env}' is ready. Run the Silver Validation job to start the pipeline.")
