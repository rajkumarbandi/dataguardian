# Databricks notebook source
# DataGuardian — Bronze Ingestion Notebook (Milestone 2)
# Fully YAML-driven: reads from ADLS Gen2, writes to Delta Bronze, displays statistics.

# COMMAND ----------
# MAGIC %md
# MAGIC # DataGuardian: Bronze Ingestion
# MAGIC
# MAGIC **Layer:** Bronze (raw landing zone)
# MAGIC **Purpose:** Read configured ERP sources from ADLS Gen2, attach metadata
# MAGIC columns, and write to Delta Bronze tables in Unity Catalog.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How it works
# MAGIC
# MAGIC ```
# MAGIC Widget (env, source_names)
# MAGIC        │
# MAGIC        ▼
# MAGIC ConfigLoader   ─── config/environments/{env}.yml
# MAGIC        │       ─── config/sources/{source}.yml
# MAGIC        ▼
# MAGIC SparkSessionManager   ──  applies shuffle partitions, AQE from env config
# MAGIC        │
# MAGIC        ▼
# MAGIC UnityCatalogClient   ──  USE CATALOG dg_{env}
# MAGIC        │             ──  CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC        ▼
# MAGIC IngestionEngine (per source)
# MAGIC        │   ├── CSVConnector.validate_connection()
# MAGIC        │   ├── CSVConnector.read()          ← ADLS CSV
# MAGIC        │   ├── _add_metadata()              ← _ingestion_timestamp, _batch_id, ...
# MAGIC        │   └── _write_bronze()              ← Delta append, partitioned by _load_date
# MAGIC        ▼
# MAGIC IngestionResult (per source) → summary table
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Widget parameters
# MAGIC | Widget | Example | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `env` | `dev` | Environment — drives catalog and ADLS root |
# MAGIC | `source_names` | `customers,products` or `all` | Comma-separated source names, or `all` |
# MAGIC
# MAGIC ### ADLS path resolution
# MAGIC All paths resolve from `{adls_root}` defined in `config/environments/{env}.yml`:
# MAGIC ```
# MAGIC dev  →  abfss://dataguardian-dev@dg-adls-dev.dfs.core.windows.net
# MAGIC qa   →  abfss://dataguardian-qa@dg-adls-qa.dfs.core.windows.net
# MAGIC prod →  abfss://dataguardian-prod@dg-adls-prod.dfs.core.windows.net
# MAGIC test →  sample_data   (local filesystem, mirrors ADLS structure)
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## 1. Setup — Python path

# COMMAND ----------

import sys
import os
import time
from pathlib import Path

# Resolve repo root dynamically so this notebook works in:
#   - Databricks Repos  (/Repos/<user>/dataguardian/databricks/notebooks/...)
#   - Databricks Asset Bundles  (/Workspace/<path>/databricks/notebooks/...)
#   - Local pytest / IDE (cwd = project root)
try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore[name-defined]  # noqa: F821
    _nb_path = _ctx.notebookPath().get()
    # Walk up the path until we find the 'databricks' segment — that's the repo root boundary
    _parts = _nb_path.rstrip("/").split("/")
    _anchor = next((i for i, p in enumerate(_parts) if p == "databricks"), None)
    _repo_root = "/".join(_parts[:_anchor]) if _anchor else "/Workspace/dataguardian"
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    print(f"Repo root: {_repo_root}")
except NameError:
    # Running locally — project root is already in sys.path via pytest pythonpath setting
    print("Running locally — using current working directory")

# COMMAND ----------
# MAGIC %md ## 2. Widgets

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment (dev | qa | prod | test)")  # type: ignore[name-defined]  # noqa: F821
dbutils.widgets.text("source_names", "all", "Sources — comma-separated names or 'all'")  # type: ignore[name-defined]  # noqa: F821

env: str = dbutils.widgets.get("env")  # type: ignore[name-defined]  # noqa: F821
source_names_raw: str = dbutils.widgets.get("source_names")  # type: ignore[name-defined]  # noqa: F821

print(f"Parameters → env={env!r}  source_names={source_names_raw!r}")

# COMMAND ----------
# MAGIC %md ## 3. Imports and bootstrap

# COMMAND ----------

from src.common.config_loader import ConfigLoader
from src.common.logger import get_logger
from src.common.spark_session import SparkSessionManager
from src.common.unity_catalog_client import UnityCatalogClient
from src.ingestion.ingestion_engine import IngestionEngine, IngestionResult

# Set env var so ConfigLoader picks it up without explicit argument
os.environ["DATAGUARDIAN_ENV"] = env

logger = get_logger("dataguardian.notebook.bronze_ingestion", env=env)
logger.info("Bronze ingestion notebook started", env=env, source_names=source_names_raw)

# COMMAND ----------
# MAGIC %md ## 4. Load environment configuration

# COMMAND ----------

loader = ConfigLoader(env=env)
env_config = loader.get_environment()

print(
    f"Environment config loaded\n"
    f"  environment : {env_config.environment}\n"
    f"  catalog     : {env_config.unity_catalog.catalog}\n"
    f"  adls_root   : {env_config.storage.adls_root}\n"
    f"  log_level   : {env_config.logging.level}\n"
    f"  shuffle_partitions : {env_config.spark.shuffle_partitions}"
)

# COMMAND ----------
# MAGIC %md ## 5. Spark session

# COMMAND ----------

manager = SparkSessionManager(env_config=env_config)
spark = manager.get_session()

print(f"SparkSession ready | version={spark.version}")

# COMMAND ----------
# MAGIC %md ## 6. Resolve source list

# COMMAND ----------

def _discover_sources(config_root: str | None = None) -> list[str]:
    """
    Return all source names available in config/sources/, excluding template files.
    Works regardless of whether the notebook runs from a Repo or a DAB workspace.
    """
    candidates = [
        Path(config_root) / "sources" if config_root else None,
        Path(_repo_root) / "config" / "sources" if "_repo_root" in dir() else None,  # type: ignore[has-type]
        Path("config") / "sources",
    ]
    for candidate in candidates:
        if candidate and candidate.is_dir():
            return sorted(
                f.stem
                for f in candidate.glob("*.yml")
                if not f.name.startswith("_") and not f.name.startswith("example")
            )
    return []


if source_names_raw.strip().lower() == "all":
    sources: list[str] = _discover_sources()
    if not sources:
        raise RuntimeError(
            "No sources found in config/sources/. "
            "Ensure the repository is correctly mounted and config/sources/ contains YAML files."
        )
    print(f"Processing ALL sources: {sources}")
else:
    sources = [s.strip() for s in source_names_raw.split(",") if s.strip()]
    print(f"Processing specified sources: {sources}")

# COMMAND ----------
# MAGIC %md ## 7. Unity Catalog client

# COMMAND ----------

uc_client = UnityCatalogClient(
    spark=spark,
    catalog=env_config.unity_catalog.catalog,
)
uc_client.use_catalog()

print(f"Active catalog → {env_config.unity_catalog.catalog}")

# COMMAND ----------
# MAGIC %md ## 8. Run ingestion — one source at a time

# COMMAND ----------

engine = IngestionEngine(spark=spark, uc_client=uc_client)
results: list[dict] = []

for source_name in sources:
    print(f"\n{'='*60}")
    print(f"  Ingesting: {source_name}")
    print(f"{'='*60}")

    run_start = time.time()

    try:
        source_config = loader.get_source(source_name)
        print(
            f"  connector  : {source_config.connector.type}\n"
            f"  location   : {source_config.connector.location}\n"
            f"  target     : {source_config.target.full_table_name}"
        )

        result: IngestionResult = engine.run(source_config)

    except Exception as exc:  # noqa: BLE001
        # Capture config-load or unexpected failures without stopping the loop
        result = IngestionResult(
            source_name=source_name,
            batch_id="N/A",
            target_table="N/A",
            success=False,
            error_message=str(exc),
        )

    duration = round(time.time() - run_start, 1)
    status_icon = "OK" if result.success else "FAIL"

    print(
        f"  [{status_icon}] rows={result.rows_written:,}  "
        f"batch={result.batch_id}  "
        f"duration={duration}s"
    )
    if not result.success:
        print(f"  ERROR: {result.error_message}")

    results.append(
        {
            "source": result.source_name,
            "target_table": result.target_table,
            "batch_id": result.batch_id,
            "rows_written": result.rows_written,
            "success": result.success,
            "duration_seconds": duration,
            "error": result.error_message if not result.success else "",
        }
    )

# COMMAND ----------
# MAGIC %md ## 9. Ingestion summary

# COMMAND ----------

summary_df = spark.createDataFrame(results)

total_rows = sum(r["rows_written"] for r in results)
succeeded = sum(1 for r in results if r["success"])
failed = len(results) - succeeded

print(
    f"\n{'='*60}\n"
    f"  INGESTION SUMMARY\n"
    f"{'='*60}\n"
    f"  Sources processed : {len(results)}\n"
    f"  Succeeded         : {succeeded}\n"
    f"  Failed            : {failed}\n"
    f"  Total rows written: {total_rows:,}\n"
    f"{'='*60}"
)

display(summary_df.orderBy("source"))  # type: ignore[name-defined]  # noqa: F821

# COMMAND ----------
# MAGIC %md ## 10. Validate — spot-check Bronze tables

# COMMAND ----------

for r in results:
    if r["success"] and r["target_table"] != "N/A":
        print(f"\n--- {r['target_table']} (last 5 rows) ---")
        display(  # type: ignore[name-defined]  # noqa: F821
            spark.table(r["target_table"])
            .orderBy("_ingestion_timestamp", ascending=False)
            .limit(5)
        )

# COMMAND ----------
# MAGIC %md ## 11. Fail notebook if any source failed

# COMMAND ----------

if failed > 0:
    failed_sources = [r["source"] for r in results if not r["success"]]
    raise RuntimeError(
        f"{failed} source(s) failed ingestion: {failed_sources}. "
        "See the per-source error messages above."
    )

logger.info(
    "Bronze ingestion notebook completed",
    sources_processed=len(results),
    total_rows=total_rows,
)
print("\nAll sources ingested successfully.")
