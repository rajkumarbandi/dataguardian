"""
Unit tests for the M6 Transformation Framework.

Coverage:
- All 18 built-in transformation classes (pure DataFrame logic, no Delta)
- TransformationRegistry — register, get, unknown type raises ConfigurationError
- TransformationEngine — no-op, success path, fail_fast, continue, skip error modes
- TransformationMetric and TransformationRunResult computed properties

All tests use a local SparkSession (no cluster required).
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Spark session — reused across the full module
# ---------------------------------------------------------------------------

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    _SPARK_AVAILABLE = True
except ImportError:
    _SPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _SPARK_AVAILABLE, reason="PySpark not available in this environment"
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("dg-test-transformations")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _customers_df(spark):
    """Small customer DataFrame with common data quality issues."""
    data = [
        ("C001", "  Alice  ", "  Smith  ", "  ALICE@EXAMPLE.COM  ", "gb", 500.0, None),
        ("C002", "Bob", "Jones", "bob@test.com", "US", None, "Enterprise"),
        ("C003", None, "Lee", "carol@co.uk", "au", 100.0, "SMB"),
    ]
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("annual_revenue", DoubleType(), True),
        StructField("customer_segment", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


def _orders_df(spark):
    """Small orders DataFrame."""
    data = [
        ("O001", "C001", "  pending  ", 100.0),
        ("O002", "C002", "CONFIRMED", 250.0),
        ("O003", "C003", "  shipped  ", 75.0),
    ]
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("total_amount", DoubleType(), True),
    ])
    return spark.createDataFrame(data, schema)


def _simple_df(spark, rows=None):
    if rows is None:
        rows = [("A", 1, 10.0), ("B", 2, 20.0), ("C", 3, 30.0)]
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("price", DoubleType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def _make_source_config(transformations: list[dict], on_error: str = "fail_fast"):
    """Build a minimal SourceConfig-like mock for the engine."""
    from src.common.models import (
        ConnectorConfig,
        SourceConfig,
        TargetConfig,
        TransformationConfig,
        TransformationPolicyConfig,
    )

    policy = TransformationPolicyConfig(on_error=on_error)
    steps = [
        TransformationConfig(
            type=t["type"],
            params=t.get("params", {}),
            enabled=t.get("enabled", True),
            on_error=t.get("on_error"),
            description=t.get("description", ""),
        )
        for t in transformations
    ]
    return SourceConfig(
        name="test_source",
        system="test",
        connector=ConnectorConfig(type="csv", location="/tmp/test"),
        target=TargetConfig(catalog="dg_test", schema="bronze", table="test"),
        transformation_policy=policy,
        transformations=steps,
    )


# ===========================================================================
# TransformationRegistry
# ===========================================================================

class TestTransformationRegistry:
    def test_all_18_registered(self):
        import src.transformations.transforms  # trigger registration
        from src.transformations.registry import TransformationRegistry

        types = TransformationRegistry.registered_types()
        expected = {
            "rename_column", "drop_columns", "select_columns", "cast_column",
            "trim_strings", "upper_case", "lower_case", "null_replacement",
            "add_constant_column", "add_timestamp_column", "derived_column",
            "date_format", "concatenate_columns", "split_column",
            "filter_rows", "sort_rows", "remove_duplicates", "column_mapping",
        }
        assert expected.issubset(set(types))

    def test_get_returns_instance(self):
        from src.transformations.registry import TransformationRegistry
        t = TransformationRegistry.get("trim_strings")
        assert t is not None
        assert hasattr(t, "apply")

    def test_get_unknown_raises_configuration_error(self):
        from src.common.exceptions import ConfigurationError
        from src.transformations.registry import TransformationRegistry

        with pytest.raises(ConfigurationError, match="Unknown transformation type"):
            TransformationRegistry.get("not_a_real_type")

    def test_is_registered(self):
        from src.transformations.registry import TransformationRegistry
        assert TransformationRegistry.is_registered("upper_case") is True
        assert TransformationRegistry.is_registered("quantum_leap") is False


# ===========================================================================
# TransformationMetric and TransformationRunResult
# ===========================================================================

class TestTransformationResults:
    def _metric(self, status="SUCCESS", columns_added=None, columns_removed=None):
        from src.transformations.results import TransformationMetric
        return TransformationMetric(
            transformation_type="trim_strings",
            execution_order=0,
            execution_time_seconds=0.1,
            rows_before=100,
            rows_after=100,
            columns_before=5,
            columns_after=5,
            columns_added=columns_added or [],
            columns_removed=columns_removed or [],
            status=status,
        )

    def test_columns_added_str_empty(self):
        m = self._metric()
        assert m.columns_added_str == ""

    def test_columns_added_str(self):
        m = self._metric(columns_added=["col_a", "col_b"])
        assert m.columns_added_str == "col_a,col_b"

    def test_columns_removed_str(self):
        m = self._metric(columns_removed=["old"])
        assert m.columns_removed_str == "old"

    def test_to_dict_keys(self):
        m = self._metric()
        d = m.to_dict()
        assert "transformation_type" in d
        assert "status" in d
        assert "rows_before" in d

    def test_run_result_counts(self):
        from src.transformations.results import TransformationMetric, TransformationRunResult

        metrics = [
            TransformationMetric("a", 0, 0.1, 10, 10, 3, 3, status="SUCCESS"),
            TransformationMetric("b", 1, 0.1, 10, 10, 3, 3, status="FAILED"),
            TransformationMetric("c", 2, 0.1, 10, 10, 3, 3, status="SKIPPED"),
        ]
        mock_df = MagicMock()
        result = TransformationRunResult(
            source_name="x",
            run_id="r1",
            input_df=mock_df,
            output_df=mock_df,
            metrics=metrics,
            success=True,
        )
        assert result.transformations_executed == 1
        assert result.transformations_failed == 1
        assert result.transformations_skipped == 1


# ===========================================================================
# Individual transformation classes
# ===========================================================================

class TestRenameColumn:
    def test_renames_existing_column(self, spark):
        from src.transformations.transforms.rename_column import RenameColumnTransformation
        df = _simple_df(spark)
        result = RenameColumnTransformation().apply(df, {"from": "name", "to": "label"})
        assert "label" in result.columns
        assert "name" not in result.columns

    def test_describe(self):
        from src.transformations.transforms.rename_column import RenameColumnTransformation
        desc = RenameColumnTransformation().describe({"from": "a", "to": "b"})
        assert "a" in desc and "b" in desc


class TestDropColumns:
    def test_drops_specified_columns(self, spark):
        from src.transformations.transforms.drop_columns import DropColumnsTransformation
        df = _simple_df(spark)
        result = DropColumnsTransformation().apply(df, {"columns": ["qty", "price"]})
        assert "qty" not in result.columns
        assert "price" not in result.columns
        assert "name" in result.columns

    def test_ignores_nonexistent_columns(self, spark):
        from src.transformations.transforms.drop_columns import DropColumnsTransformation
        df = _simple_df(spark)
        result = DropColumnsTransformation().apply(df, {"columns": ["does_not_exist"]})
        assert result.columns == df.columns

    def test_empty_list_is_noop(self, spark):
        from src.transformations.transforms.drop_columns import DropColumnsTransformation
        df = _simple_df(spark)
        result = DropColumnsTransformation().apply(df, {"columns": []})
        assert result.columns == df.columns


class TestSelectColumns:
    def test_keeps_only_listed_columns(self, spark):
        from src.transformations.transforms.select_columns import SelectColumnsTransformation
        df = _simple_df(spark)
        result = SelectColumnsTransformation().apply(df, {"columns": ["name", "qty"]})
        assert result.columns == ["name", "qty"]

    def test_skips_nonexistent_columns(self, spark):
        from src.transformations.transforms.select_columns import SelectColumnsTransformation
        df = _simple_df(spark)
        result = SelectColumnsTransformation().apply(df, {"columns": ["name", "missing"]})
        assert result.columns == ["name"]


class TestCastColumn:
    def test_cast_integer_to_long(self, spark):
        from pyspark.sql.types import LongType
        from src.transformations.transforms.cast_column import CastColumnTransformation
        df = _simple_df(spark)
        result = CastColumnTransformation().apply(df, {"column": "qty", "datatype": "long"})
        field = next(f for f in result.schema.fields if f.name == "qty")
        assert isinstance(field.dataType, LongType)

    def test_cast_double_to_string(self, spark):
        from pyspark.sql.types import StringType
        from src.transformations.transforms.cast_column import CastColumnTransformation
        df = _simple_df(spark)
        result = CastColumnTransformation().apply(df, {"column": "price", "datatype": "string"})
        field = next(f for f in result.schema.fields if f.name == "price")
        assert isinstance(field.dataType, StringType)


class TestTrimStrings:
    def test_trims_specified_columns(self, spark):
        from src.transformations.transforms.trim_strings import TrimStringsTransformation
        df = _customers_df(spark)
        result = TrimStringsTransformation().apply(df, {"columns": ["first_name", "last_name"]})
        rows = {r["customer_id"]: r for r in result.collect()}
        assert rows["C001"]["first_name"] == "Alice"
        assert rows["C001"]["last_name"] == "Smith"

    def test_trims_all_string_columns_when_empty_list(self, spark):
        from src.transformations.transforms.trim_strings import TrimStringsTransformation
        df = _customers_df(spark)
        result = TrimStringsTransformation().apply(df, {})
        rows = {r["customer_id"]: r for r in result.collect()}
        assert rows["C001"]["email"] == "ALICE@EXAMPLE.COM"

    def test_modifies_row_count_is_false(self):
        from src.transformations.transforms.trim_strings import TrimStringsTransformation
        assert TrimStringsTransformation.modifies_row_count is False


class TestUpperCase:
    def test_uppercases_specified_columns(self, spark):
        from src.transformations.transforms.upper_case import UpperCaseTransformation
        df = _customers_df(spark)
        result = UpperCaseTransformation().apply(df, {"columns": ["country_code"]})
        rows = {r["customer_id"]: r for r in result.collect()}
        assert rows["C001"]["country_code"] == "GB"
        assert rows["C003"]["country_code"] == "AU"

    def test_ignores_nonexistent_columns(self, spark):
        from src.transformations.transforms.upper_case import UpperCaseTransformation
        df = _simple_df(spark)
        result = UpperCaseTransformation().apply(df, {"columns": ["missing_col"]})
        assert result.count() == df.count()


class TestLowerCase:
    def test_lowercases_specified_columns(self, spark):
        from src.transformations.transforms.lower_case import LowerCaseTransformation
        df = _customers_df(spark)
        result = LowerCaseTransformation().apply(df, {"columns": ["email"]})
        rows = {r["customer_id"]: r for r in result.collect()}
        assert rows["C001"]["email"] == "  alice@example.com  "


class TestNullReplacement:
    def test_single_column_mode(self, spark):
        from src.transformations.transforms.null_replacement import NullReplacementTransformation
        df = _customers_df(spark)
        result = NullReplacementTransformation().apply(
            df, {"column": "customer_segment", "value": "Unknown"}
        )
        rows = {r["customer_id"]: r for r in result.collect()}
        assert rows["C001"]["customer_segment"] == "Unknown"
        assert rows["C002"]["customer_segment"] == "Enterprise"

    def test_replacements_dict_mode(self, spark):
        from src.transformations.transforms.null_replacement import NullReplacementTransformation
        df = _customers_df(spark)
        result = NullReplacementTransformation().apply(
            df, {"replacements": {"customer_segment": "Unknown", "annual_revenue": 0.0}}
        )
        rows = {r["customer_id"]: r for r in result.collect()}
        assert rows["C001"]["customer_segment"] == "Unknown"
        assert rows["C002"]["annual_revenue"] == 0.0

    def test_raises_on_missing_params(self, spark):
        from src.transformations.transforms.null_replacement import NullReplacementTransformation
        df = _simple_df(spark)
        with pytest.raises(ValueError, match="requires either"):
            NullReplacementTransformation().apply(df, {})


class TestAddConstantColumn:
    def test_adds_string_literal(self, spark):
        from src.transformations.transforms.add_constant_column import AddConstantColumnTransformation
        df = _simple_df(spark)
        result = AddConstantColumnTransformation().apply(
            df, {"column": "data_source", "value": "ERP", "datatype": "string"}
        )
        assert "data_source" in result.columns
        vals = [r["data_source"] for r in result.collect()]
        assert all(v == "ERP" for v in vals)

    def test_default_datatype_is_string(self, spark):
        from pyspark.sql.types import StringType
        from src.transformations.transforms.add_constant_column import AddConstantColumnTransformation
        df = _simple_df(spark)
        result = AddConstantColumnTransformation().apply(
            df, {"column": "tag", "value": "x"}
        )
        field = next(f for f in result.schema.fields if f.name == "tag")
        assert isinstance(field.dataType, StringType)


class TestAddTimestampColumn:
    def test_adds_timestamp_column(self, spark):
        from pyspark.sql.types import TimestampType
        from src.transformations.transforms.add_timestamp_column import AddTimestampColumnTransformation
        df = _simple_df(spark)
        result = AddTimestampColumnTransformation().apply(df, {})
        assert "_transformed_at" in result.columns
        field = next(f for f in result.schema.fields if f.name == "_transformed_at")
        assert isinstance(field.dataType, TimestampType)

    def test_custom_column_name(self, spark):
        from src.transformations.transforms.add_timestamp_column import AddTimestampColumnTransformation
        df = _simple_df(spark)
        result = AddTimestampColumnTransformation().apply(df, {"column": "processed_at"})
        assert "processed_at" in result.columns

    def test_default_column_is_transformed_at(self):
        from src.transformations.transforms.add_timestamp_column import AddTimestampColumnTransformation
        desc = AddTimestampColumnTransformation().describe({})
        assert "_transformed_at" in desc


class TestDerivedColumn:
    def test_creates_computed_column(self, spark):
        from src.transformations.transforms.derived_column import DerivedColumnTransformation
        df = _simple_df(spark)
        result = DerivedColumnTransformation().apply(
            df, {"column": "total", "expression": "qty * price"}
        )
        assert "total" in result.columns
        rows = {r["name"]: r for r in result.collect()}
        assert rows["A"]["total"] == pytest.approx(10.0)
        assert rows["B"]["total"] == pytest.approx(40.0)

    def test_overwrites_existing_column(self, spark):
        from src.transformations.transforms.derived_column import DerivedColumnTransformation
        df = _simple_df(spark)
        result = DerivedColumnTransformation().apply(
            df, {"column": "price", "expression": "price * 2"}
        )
        rows = {r["name"]: r for r in result.collect()}
        assert rows["A"]["price"] == pytest.approx(20.0)


class TestDateFormat:
    def test_reformats_date_column(self, spark):
        from pyspark.sql import functions as F
        from src.transformations.transforms.date_format import DateFormatTransformation
        data = [("2024-01-15",), ("2024-03-22",)]
        schema = StructType([StructField("created_date", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        df = df.withColumn("created_date", F.to_date("created_date", "yyyy-MM-dd"))
        result = DateFormatTransformation().apply(
            df, {"column": "created_date", "output_format": "yyyy/MM/dd"}
        )
        vals = [r["created_date"] for r in result.collect()]
        assert "2024/01/15" in vals

    def test_custom_output_column(self, spark):
        from pyspark.sql import functions as F
        from src.transformations.transforms.date_format import DateFormatTransformation
        data = [("2024-01-15",)]
        schema = StructType([StructField("raw_date", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        df = df.withColumn("raw_date", F.to_date("raw_date"))
        result = DateFormatTransformation().apply(
            df, {"column": "raw_date", "output_format": "dd-MM-yyyy", "output_column": "formatted"}
        )
        assert "formatted" in result.columns
        assert "raw_date" in result.columns


class TestConcatenateColumns:
    def test_concatenates_with_separator(self, spark):
        from src.transformations.transforms.concatenate_columns import ConcatenateColumnsTransformation
        df = _customers_df(spark)
        result = ConcatenateColumnsTransformation().apply(
            df, {"columns": ["first_name", "last_name"], "separator": " ", "output_column": "full_name"}
        )
        assert "full_name" in result.columns
        rows = {r["customer_id"]: r for r in result.collect()}
        assert "Bob" in rows["C002"]["full_name"]
        assert "Jones" in rows["C002"]["full_name"]

    def test_handles_null_as_empty_string(self, spark):
        from src.transformations.transforms.concatenate_columns import ConcatenateColumnsTransformation
        df = _customers_df(spark)
        result = ConcatenateColumnsTransformation().apply(
            df, {"columns": ["first_name", "last_name"], "separator": " ", "output_column": "full_name"}
        )
        rows = {r["customer_id"]: r for r in result.collect()}
        # C003 has null first_name — result should be " Lee" (empty concat with separator)
        assert "Lee" in rows["C003"]["full_name"]

    def test_empty_separator(self, spark):
        from src.transformations.transforms.concatenate_columns import ConcatenateColumnsTransformation
        df = _simple_df(spark)
        result = ConcatenateColumnsTransformation().apply(
            df, {"columns": ["name"], "separator": "", "output_column": "name_copy"}
        )
        assert "name_copy" in result.columns


class TestSplitColumn:
    def test_splits_into_array(self, spark):
        from pyspark.sql.types import ArrayType
        from src.transformations.transforms.split_column import SplitColumnTransformation
        data = [("a,b,c",)]
        schema = StructType([StructField("tags", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        result = SplitColumnTransformation().apply(
            df, {"column": "tags", "delimiter": ",", "output_column": "tags_array"}
        )
        assert "tags_array" in result.columns
        field = next(f for f in result.schema.fields if f.name == "tags_array")
        assert isinstance(field.dataType, ArrayType)

    def test_splits_into_named_columns(self, spark):
        from src.transformations.transforms.split_column import SplitColumnTransformation
        data = [("Alice Smith",), ("Bob Jones",)]
        schema = StructType([StructField("full_name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        result = SplitColumnTransformation().apply(
            df, {"column": "full_name", "delimiter": " ", "output_columns": ["fname", "lname"]}
        )
        assert "fname" in result.columns
        assert "lname" in result.columns
        rows = result.collect()
        assert rows[0]["fname"] == "Alice"
        assert rows[0]["lname"] == "Smith"


class TestFilterRows:
    def test_filters_rows(self, spark):
        from src.transformations.transforms.filter_rows import FilterRowsTransformation
        df = _simple_df(spark)
        result = FilterRowsTransformation().apply(df, {"condition": "qty > 1"})
        assert result.count() == 2

    def test_modifies_row_count_is_true(self):
        from src.transformations.transforms.filter_rows import FilterRowsTransformation
        assert FilterRowsTransformation.modifies_row_count is True

    def test_filter_returns_empty_when_no_match(self, spark):
        from src.transformations.transforms.filter_rows import FilterRowsTransformation
        df = _simple_df(spark)
        result = FilterRowsTransformation().apply(df, {"condition": "qty > 9999"})
        assert result.count() == 0


class TestSortRows:
    def test_sort_ascending(self, spark):
        from src.transformations.transforms.sort_rows import SortRowsTransformation
        df = _simple_df(spark, rows=[("C", 3, 30.0), ("A", 1, 10.0), ("B", 2, 20.0)])
        result = SortRowsTransformation().apply(df, {"columns": ["name"], "ascending": True})
        names = [r["name"] for r in result.collect()]
        assert names == ["A", "B", "C"]

    def test_sort_descending(self, spark):
        from src.transformations.transforms.sort_rows import SortRowsTransformation
        df = _simple_df(spark, rows=[("A", 1, 10.0), ("C", 3, 30.0), ("B", 2, 20.0)])
        result = SortRowsTransformation().apply(df, {"columns": ["qty"], "ascending": False})
        qtys = [r["qty"] for r in result.collect()]
        assert qtys == [3, 2, 1]

    def test_sort_mixed_directions(self, spark):
        from src.transformations.transforms.sort_rows import SortRowsTransformation
        data = [("A", 2, 10.0), ("A", 1, 10.0), ("B", 1, 10.0)]
        df = _simple_df(spark, rows=data)
        result = SortRowsTransformation().apply(
            df, {"columns": ["name", "qty"], "ascending": [True, False]}
        )
        rows = result.collect()
        assert rows[0]["name"] == "A" and rows[0]["qty"] == 2


class TestRemoveDuplicates:
    def test_removes_all_duplicates(self, spark):
        from src.transformations.transforms.remove_duplicates import RemoveDuplicatesTransformation
        data = [("A", 1, 10.0), ("A", 1, 10.0), ("B", 2, 20.0)]
        df = _simple_df(spark, rows=data)
        result = RemoveDuplicatesTransformation().apply(df, {})
        assert result.count() == 2

    def test_deduplicates_on_subset(self, spark):
        from src.transformations.transforms.remove_duplicates import RemoveDuplicatesTransformation
        data = [("A", 1, 10.0), ("A", 2, 20.0), ("B", 3, 30.0)]
        df = _simple_df(spark, rows=data)
        result = RemoveDuplicatesTransformation().apply(df, {"columns": ["name"]})
        assert result.count() == 2

    def test_modifies_row_count_is_true(self):
        from src.transformations.transforms.remove_duplicates import RemoveDuplicatesTransformation
        assert RemoveDuplicatesTransformation.modifies_row_count is True


class TestColumnMapping:
    def test_renames_multiple_columns(self, spark):
        from src.transformations.transforms.column_mapping import ColumnMappingTransformation
        df = _simple_df(spark)
        result = ColumnMappingTransformation().apply(
            df, {"mappings": {"name": "label", "qty": "quantity"}}
        )
        assert "label" in result.columns
        assert "quantity" in result.columns
        assert "name" not in result.columns
        assert "qty" not in result.columns

    def test_ignores_nonexistent_source_columns(self, spark):
        from src.transformations.transforms.column_mapping import ColumnMappingTransformation
        df = _simple_df(spark)
        result = ColumnMappingTransformation().apply(
            df, {"mappings": {"does_not_exist": "new_col"}}
        )
        assert result.columns == df.columns

    def test_empty_mappings_is_noop(self, spark):
        from src.transformations.transforms.column_mapping import ColumnMappingTransformation
        df = _simple_df(spark)
        result = ColumnMappingTransformation().apply(df, {"mappings": {}})
        assert result.columns == df.columns


# ===========================================================================
# TransformationEngine — integration-style tests
# ===========================================================================

class TestTransformationEngine:
    def test_noop_when_no_transformations(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([])
        df = _simple_df(spark)
        result = engine.run(df=df, source_config=source_config, run_id="r1")
        assert result.success is True
        assert result.metrics == []
        assert result.output_df.count() == df.count()

    def test_single_transform_success(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {"type": "upper_case", "params": {"columns": ["name"]}}
        ])
        df = _simple_df(spark, rows=[("alice", 1, 1.0)])
        result = engine.run(df=df, source_config=source_config, run_id="r2")
        assert result.success is True
        assert result.transformations_executed == 1
        out_rows = result.output_df.collect()
        assert out_rows[0]["name"] == "ALICE"

    def test_chained_transforms(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {"type": "trim_strings", "params": {"columns": ["name"]}},
            {"type": "upper_case", "params": {"columns": ["name"]}},
        ])
        df = _simple_df(spark, rows=[("  alice  ", 1, 1.0)])
        result = engine.run(df=df, source_config=source_config, run_id="r3")
        assert result.success is True
        assert result.transformations_executed == 2
        out = result.output_df.collect()[0]
        assert out["name"] == "ALICE"

    def test_disabled_step_is_skipped(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {"type": "upper_case", "params": {"columns": ["name"]}, "enabled": False},
        ])
        df = _simple_df(spark, rows=[("alice", 1, 1.0)])
        result = engine.run(df=df, source_config=source_config, run_id="r4")
        assert result.success is True
        assert result.metrics == []
        out = result.output_df.collect()[0]
        assert out["name"] == "alice"

    def test_fail_fast_on_error(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {"type": "derived_column", "params": {"column": "x", "expression": "invalid!!!!!"}},
        ], on_error="fail_fast")
        df = _simple_df(spark)
        result = engine.run(df=df, source_config=source_config, run_id="r5")
        assert result.success is False
        assert "FAILED" in result.error_message or result.transformations_failed >= 1

    def test_continue_on_error_keeps_previous_df(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {"type": "derived_column", "params": {"column": "x", "expression": "invalid!!!!!"}},
            {"type": "upper_case", "params": {"columns": ["name"]}},
        ], on_error="continue")
        df = _simple_df(spark, rows=[("alice", 1, 1.0)])
        result = engine.run(df=df, source_config=source_config, run_id="r6")
        assert result.success is True
        assert result.transformations_failed == 1
        out = result.output_df.collect()[0]
        assert out["name"] == "ALICE"

    def test_skip_on_error_keeps_previous_df(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {"type": "derived_column", "params": {"column": "x", "expression": "!!!!"}},
        ], on_error="skip")
        df = _simple_df(spark, rows=[("alice", 1, 1.0)])
        result = engine.run(df=df, source_config=source_config, run_id="r7")
        assert result.success is True

    def test_per_step_on_error_overrides_policy(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {
                "type": "derived_column",
                "params": {"column": "x", "expression": "!!!!"},
                "on_error": "continue",
            },
        ], on_error="fail_fast")
        df = _simple_df(spark)
        result = engine.run(df=df, source_config=source_config, run_id="r8")
        assert result.success is True

    def test_input_row_count_avoids_extra_count(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {"type": "upper_case", "params": {"columns": ["name"]}},
        ])
        df = _simple_df(spark)
        result = engine.run(
            df=df, source_config=source_config, run_id="r9", input_row_count=3
        )
        assert result.success is True
        assert result.metrics[0].rows_before == 3

    def test_row_modifying_transform_recounts(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {"type": "filter_rows", "params": {"condition": "qty > 1"}},
        ])
        df = _simple_df(spark)
        result = engine.run(
            df=df, source_config=source_config, run_id="r10", input_row_count=3
        )
        assert result.success is True
        assert result.metrics[0].rows_before == 3
        assert result.metrics[0].rows_after == 2

    def test_metrics_capture_column_changes(self, spark):
        from src.transformations.engine import TransformationEngine
        engine = TransformationEngine(spark=spark)
        source_config = _make_source_config([
            {"type": "add_constant_column", "params": {"column": "new_col", "value": "x"}},
        ])
        df = _simple_df(spark)
        result = engine.run(df=df, source_config=source_config, run_id="r11")
        assert result.success is True
        m = result.metrics[0]
        assert m.columns_after == m.columns_before + 1
        assert "new_col" in m.columns_added


# ===========================================================================
# Model validation — TransformationConfig and TransformationPolicyConfig
# ===========================================================================

class TestTransformationConfigModels:
    def test_valid_on_error_values(self):
        from src.common.models import TransformationPolicyConfig
        for v in ["fail_fast", "continue", "skip"]:
            cfg = TransformationPolicyConfig(on_error=v)
            assert cfg.on_error == v

    def test_invalid_on_error_raises(self):
        from pydantic import ValidationError
        from src.common.models import TransformationPolicyConfig
        with pytest.raises(ValidationError):
            TransformationPolicyConfig(on_error="explode")

    def test_transformation_config_defaults(self):
        from src.common.models import TransformationConfig
        cfg = TransformationConfig(type="trim_strings")
        assert cfg.enabled is True
        assert cfg.on_error is None
        assert cfg.params == {}

    def test_source_config_has_transformations_field(self):
        from src.common.models import (
            ConnectorConfig,
            SourceConfig,
            TargetConfig,
            TransformationConfig,
        )
        sc = SourceConfig(
            name="t",
            system="s",
            connector=ConnectorConfig(type="csv", location="/x"),
            target=TargetConfig(catalog="c", schema="s", table="t"),
            transformations=[TransformationConfig(type="upper_case", params={"columns": ["a"]})],
        )
        assert len(sc.transformations) == 1
        assert sc.transformations[0].type == "upper_case"

    def test_environment_config_has_transformation_field(self):
        from src.common.models import EnvironmentConfig, UnityCatalogConfig, TransformationEnvConfig
        ec = EnvironmentConfig(
            environment="test",
            unity_catalog=UnityCatalogConfig(catalog="dg_test"),
        )
        assert isinstance(ec.transformation, TransformationEnvConfig)
        assert ec.transformation.audit_enabled is True
