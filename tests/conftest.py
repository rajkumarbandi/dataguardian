"""Shared pytest fixtures for the DataGuardian test suite."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
import yaml

# ---------------------------------------------------------------------------
# Spark session (session-scoped — created once, shared across all tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
    """
    Local Delta-enabled SparkSession for unit and integration tests.

    Marked ``session``-scoped so PySpark starts once per pytest run, not once
    per test.  Tests that need a clean session should use the
    ``SparkSessionManager.reset()`` helper.
    """
    from delta import configure_spark_with_delta_pip  # type: ignore[import]
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("dataguardian-unit-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Reusable YAML fixtures written to tmp_path
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: dict[str, Any]) -> Path:
    path.write_text(yaml.dump(data), encoding="utf-8")
    return path


@pytest.fixture
def minimal_env_yaml(tmp_path: Path) -> Path:
    """Write a minimal dev environment YAML to ``tmp_path`` and return its path."""
    data = {
        "environment": "test",
        "unity_catalog": {
            "catalog": "dg_test",
            "schemas": {"bronze": "bronze", "silver": "silver"},
        },
        "storage": {
            "adls_account": "testaccount",
            "adls_container": "testcontainer",
            "adls_root": "abfss://testcontainer@testaccount.dfs.core.windows.net",
        },
    }
    env_dir = tmp_path / "environments"
    env_dir.mkdir(parents=True)
    return _write_yaml(env_dir / "test.yml", data)


@pytest.fixture
def minimal_source_yaml(tmp_path: Path) -> Path:
    """Write a minimal source YAML for the customers entity."""
    data = {
        "name": "customers",
        "system": "erp",
        "description": "Test customer source",
        "connector": {
            "type": "csv",
            "location": "tests/fixtures/customers.csv",
            "options": {"header": "true", "delimiter": ","},
        },
        "schema": [
            {"name": "customer_id", "type": "string"},
            {"name": "first_name", "type": "string"},
        ],
        "target": {
            "catalog": "dg_test",
            "schema": "bronze",
            "table": "erp_customers",
            "load_type": "append",
        },
        "metadata": {"owner": "test@example.com"},
    }
    sources_dir = tmp_path / "sources"
    sources_dir.mkdir(parents=True)
    return _write_yaml(sources_dir / "customers.yml", data)


@pytest.fixture
def customers_csv_path() -> Path:
    """Return the path to the fixture CSV."""
    return Path(__file__).parent / "fixtures" / "customers.csv"
