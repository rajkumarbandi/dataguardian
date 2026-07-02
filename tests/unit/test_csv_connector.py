"""Unit tests for CSVConnector."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.common.exceptions import ConnectorError
from src.common.models import ConnectorConfig, SourceConfig, TargetConfig
from src.ingestion.connectors.csv_connector import CSVConnector, _SPARK_TYPE_MAP


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURE_CSV = Path(__file__).parent.parent / "fixtures" / "customers.csv"


def _make_source_config(
    location: str = str(FIXTURE_CSV),
    schema: list | None = None,
    options: dict | None = None,
) -> SourceConfig:
    return SourceConfig(
        name="customers",
        system="erp",
        connector=ConnectorConfig(
            type="csv",
            location=location,
            options=options or {},
        ),
        schema=schema or [],
        target=TargetConfig(
            catalog="dg_test",
            schema="bronze",
            table="erp_customers",
        ),
    )


# ---------------------------------------------------------------------------
# connector_type
# ---------------------------------------------------------------------------


class TestConnectorType:
    def test_connector_type_is_csv(self, spark) -> None:
        cfg = _make_source_config()
        connector = CSVConnector(config=cfg, spark=spark)
        assert connector.connector_type == "csv"


# ---------------------------------------------------------------------------
# validate_connection
# ---------------------------------------------------------------------------


class TestValidateConnection:
    def test_returns_true_for_existing_local_file(self, spark) -> None:
        cfg = _make_source_config(location=str(FIXTURE_CSV))
        connector = CSVConnector(config=cfg, spark=spark)
        assert connector.validate_connection() is True

    def test_raises_for_missing_local_file(self, spark) -> None:
        cfg = _make_source_config(location="/nonexistent/path/data.csv")
        connector = CSVConnector(config=cfg, spark=spark)
        with pytest.raises(ConnectorError, match="does not exist"):
            connector.validate_connection()

    def test_adls_path_skips_check(self, spark) -> None:
        cfg = _make_source_config(
            location="abfss://container@account.dfs.core.windows.net/data.csv"
        )
        connector = CSVConnector(config=cfg, spark=spark)
        # Should return True with a warning, not raise
        result = connector.validate_connection()
        assert result is True


# ---------------------------------------------------------------------------
# read — schema inference (no schema declared in YAML)
# ---------------------------------------------------------------------------


class TestReadInferredSchema:
    def test_returns_dataframe(self, spark) -> None:
        cfg = _make_source_config(options={"header": "true"})
        connector = CSVConnector(config=cfg, spark=spark)
        df = connector.read()
        assert df is not None

    def test_all_source_columns_present(self, spark) -> None:
        cfg = _make_source_config(options={"header": "true"})
        connector = CSVConnector(config=cfg, spark=spark)
        df = connector.read()
        expected_cols = {
            "customer_id", "first_name", "last_name", "email",
            "phone", "country_code", "created_date", "is_active",
        }
        assert expected_cols.issubset(set(df.columns))

    def test_row_count_matches_fixture(self, spark) -> None:
        cfg = _make_source_config(options={"header": "true"})
        connector = CSVConnector(config=cfg, spark=spark)
        df = connector.read()
        assert df.count() == 25


# ---------------------------------------------------------------------------
# read — explicit schema (declared in YAML)
# ---------------------------------------------------------------------------


class TestReadExplicitSchema:
    def _schema(self) -> list:
        from src.common.models import ColumnDefinition
        return [
            ColumnDefinition(name="customer_id", type="string"),
            ColumnDefinition(name="first_name", type="string"),
            ColumnDefinition(name="last_name", type="string"),
            ColumnDefinition(name="email", type="string"),
            ColumnDefinition(name="phone", type="string"),
            ColumnDefinition(name="country_code", type="string"),
            ColumnDefinition(name="created_date", type="date"),
            ColumnDefinition(name="is_active", type="boolean"),
        ]

    def test_reads_with_explicit_schema(self, spark) -> None:
        cfg = _make_source_config(schema=self._schema(), options={"header": "true"})
        connector = CSVConnector(config=cfg, spark=spark)
        df = connector.read()
        assert df.count() == 25

    def test_declared_types_applied(self, spark) -> None:
        from pyspark.sql.types import BooleanType, DateType, StringType
        cfg = _make_source_config(schema=self._schema(), options={"header": "true"})
        connector = CSVConnector(config=cfg, spark=spark)
        df = connector.read()
        field_map = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(field_map["customer_id"], StringType)
        assert isinstance(field_map["created_date"], DateType)
        assert isinstance(field_map["is_active"], BooleanType)

    def test_all_fields_nullable(self, spark) -> None:
        cfg = _make_source_config(schema=self._schema(), options={"header": "true"})
        connector = CSVConnector(config=cfg, spark=spark)
        df = connector.read()
        for field in df.schema.fields:
            assert field.nullable, f"Field {field.name} should be nullable in Bronze"


# ---------------------------------------------------------------------------
# _build_spark_schema — edge cases
# ---------------------------------------------------------------------------


class TestBuildSparkSchema:
    def test_unknown_type_raises_connector_error(self, spark) -> None:
        from src.common.models import ColumnDefinition
        bad_schema = [ColumnDefinition(name="col", type="jsonb")]
        cfg = _make_source_config(schema=bad_schema, options={"header": "true"})
        connector = CSVConnector(config=cfg, spark=spark)
        with pytest.raises(ConnectorError, match="Unsupported column type"):
            connector.read()

    def test_all_supported_types_map_successfully(self, spark) -> None:
        from src.common.models import ColumnDefinition
        for type_str in _SPARK_TYPE_MAP:
            cols = [ColumnDefinition(name="col", type=type_str)]
            schema = CSVConnector._build_spark_schema(cols)
            assert len(schema.fields) == 1
