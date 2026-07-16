# Databricks notebook source
# MAGIC %md
# MAGIC # DataGuardian — Stewardship SLA Monitor
# MAGIC
# MAGIC Checks for SLA breaches in the stewardship layer and emits alerts.
# MAGIC Runs hourly as a Databricks Job task. Reads from the stewardship
# MAGIC Delta tables in Unity Catalog and flags records that have exceeded
# MAGIC the configured review SLA window.

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("catalog", "dg_dev", "Unity Catalog")

env = dbutils.widgets.get("env")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/dataguardian/src")

from src.config.loader import load_config

config = load_config(env=env)
print(f"[sla_monitor] env={env} catalog={catalog}")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

sla_hours = config.get("stewardship", {}).get("sla_hours", 48)
cutoff = F.current_timestamp() - F.expr(f"INTERVAL {sla_hours} HOURS")

pending = (
    spark.table(f"{catalog}.stewardship.stewardship_records")
    .filter(F.col("status") == "PENDING")
    .filter(F.col("created_at") < cutoff)
)

breach_count = pending.count()
print(f"[sla_monitor] SLA breaches (>{sla_hours}h pending): {breach_count}")

if breach_count > 0:
    pending.select("record_id", "source_name", "created_at").show(20, truncate=False)

dbutils.notebook.exit(f"sla_breaches={breach_count}")
