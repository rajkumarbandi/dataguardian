"""Unit tests for IngestionEngine."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.common.exceptions import ConfigurationError, ConnectorError
from src.common.models import ConnectorConfig, SourceConfig, TargetConfig
from src.ingestion.ingestion_engine import IngestionEngine, IngestionResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURE_CSV = Path(__file__).parent.parent / "fixtures" / "customers.csv"


def _make_source_config(location: str = str(FIXTURE_CSV)) -> SourceConfig:
    return SourceConfig(
        name="customers",
        system="erp",
        connector=ConnectorConfig(
            type="csv",
            location=location,
            options={"header": "true"},
        ),
        schema=[],
        target=TargetConfig(
            catalog="dg_test",
            schema="bronze",
            table="erp_customers",
            load_type="append",
            partition_by="_load_date",
        ),
    )


def _make_uc_client(schema_exists: bool = False) -> MagicMock:
    uc = MagicMock()
    uc.create_schema_if_not_exists = MagicMock()
    uc.table_exists = MagicMock(return_value=schema_exists)
    return uc


# ---------------------------------------------------------------------------
# Successful ingestion (integration-light: uses real Spark)
# ---------------------------------------------------------------------------


class TestRunSuccess:
    def test_returns_success_result(self, spark, tmp_path) -> None:
        source = _make_source_config()
        uc = _make_uc_client()
        engine = IngestionEngine(spark=spark, uc_client=uc)

        # Patch _write_bronze to avoid Delta write (no Unity Catalog locally)
        with patch.object(engine, "_write_bronze"):
            result = engine.run(source)

        assert result.success is True
        assert result.source_name == "customers"
        assert result.rows_written == 25

    def test_result_contains_batch_id(self, spark) -> None:
        source = _make_source_config()
        uc = _make_uc_client()
        engine = IngestionEngine(spark=spark, uc_client=uc)

        with patch.object(engine, "_write_bronze"):
            result = engine.run(source)

        assert result.batch_id != ""
        assert len(result.batch_id) == 36  # UUID4 format

    def test_schema_created_before_write(self, spark) -> None:
        source = _make_source_config()
        uc = _make_uc_client()
        engine = IngestionEngine(spark=spark, uc_client=uc)

        with patch.object(engine, "_write_bronze"):
            engine.run(source)

        uc.create_schema_if_not_exists.assert_called_once_with("bronze")


# ---------------------------------------------------------------------------
# Metadata column injection
# ---------------------------------------------------------------------------


class TestAddMetadata:
    def test_metadata_columns_added(self, spark) -> None:
        source = _make_source_config()
        raw_df = spark.createDataFrame(
            [("C001", "Alice")], ["customer_id", "first_name"]
        )
        engine = IngestionEngine(spark=spark, uc_client=MagicMock())
        enriched = engine._add_metadata(raw_df, source, batch_id="batch-abc")

        cols = set(enriched.columns)
        assert "_ingestion_timestamp" in cols
        assert "_source_system" in cols
        assert "_batch_id" in cols
        assert "_load_date" in cols

    def test_source_columns_preserved(self, spark) -> None:
        source = _make_source_config()
        raw_df = spark.createDataFrame(
            [("C001", "Alice")], ["customer_id", "first_name"]
        )
        engine = IngestionEngine(spark=spark, uc_client=MagicMock())
        enriched = engine._add_metadata(raw_df, source, batch_id="batch-abc")

        assert "customer_id" in enriched.columns
        assert "first_name" in enriched.columns

    def test_source_system_value(self, spark) -> None:
        source = _make_source_config()
        raw_df = spark.createDataFrame([("C001",)], ["customer_id"])
        engine = IngestionEngine(spark=spark, uc_client=MagicMock())
        enriched = engine._add_metadata(raw_df, source, batch_id="x")
        row = enriched.first()
        assert row["_source_system"] == "erp"

    def test_batch_id_value(self, spark) -> None:
        source = _make_source_config()
        raw_df = spark.createDataFrame([("C001",)], ["customer_id"])
        engine = IngestionEngine(spark=spark, uc_client=MagicMock())
        enriched = engine._add_metadata(raw_df, source, batch_id="my-batch-id")
        row = enriched.first()
        assert row["_batch_id"] == "my-batch-id"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestRunErrors:
    def test_connector_error_caught_as_failure(self, spark) -> None:
        source = _make_source_config(location="/nonexistent/path.csv")
        uc = _make_uc_client()
        engine = IngestionEngine(spark=spark, uc_client=uc)
        result = engine.run(source)

        assert result.success is False
        assert result.error_message != ""

    def test_unknown_connector_type_fails(self, spark) -> None:
        source = SourceConfig(
            name="bad",
            system="sys",
            connector=ConnectorConfig(type="csv", location="/x"),
            target=TargetConfig(
                catalog="dg_test", schema="bronze", table="bad_table"
            ),
        )
        # Patch registry to be empty so "csv" is not found
        uc = _make_uc_client()
        engine = IngestionEngine(spark=spark, uc_client=uc)

        with patch("src.ingestion.ingestion_engine._CONNECTOR_REGISTRY", {}):
            result = engine.run(source)

        assert result.success is False
        assert "No connector registered" in result.error_message

    def test_unity_catalog_error_caught(self, spark) -> None:
        source = _make_source_config()
        uc = _make_uc_client()
        uc.create_schema_if_not_exists.side_effect = ConfigurationError("UC unavailable")
        engine = IngestionEngine(spark=spark, uc_client=uc)

        with patch.object(engine, "_build_connector") as mock_build:
            mock_connector = MagicMock()
            mock_connector.validate_connection.return_value = True
            mock_connector.read.return_value = spark.createDataFrame(
                [("C001",)], ["customer_id"]
            )
            mock_build.return_value = mock_connector
            result = engine.run(source)

        assert result.success is False
        assert "UC unavailable" in result.error_message
