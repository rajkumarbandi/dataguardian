"""
Unit tests for DataQualityEngine.

Tests use an in-memory SparkSession (no Delta writes).  The engine itself
never writes — it returns DataFrames — so these tests run without any
catalog or storage dependency.

Test structure
--------------
Each test class covers a distinct engine behaviour.  The full engine pipeline
(annotate → split → metrics) is exercised end-to-end with small DataFrames.
"""

from __future__ import annotations

import pytest
from pyspark.sql import Row

import src.quality.rules  # noqa: F401  — registers built-in rules
from src.common.models import DQRuleConfig, SourceConfig, ConnectorConfig, TargetConfig
from src.quality.engine import DataQualityEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def source_config_factory():
    """Return a function that builds a minimal SourceConfig with given dq_rules."""
    def _make(dq_rules: list[dict]) -> SourceConfig:
        return SourceConfig(
            name="test_source",
            system="erp",
            description="Test source for engine unit tests",
            connector=ConnectorConfig(
                type="csv",
                location="sample_data/raw/erp/test",
            ),
            target=TargetConfig(
                catalog="dg_test",
                schema="bronze",
                table="erp_test",
                load_type="append",
                partition_by="_load_date",
            ),
            dq_rules=[DQRuleConfig(**r) for r in dq_rules],
        )
    return _make


@pytest.fixture()
def engine(spark):
    return DataQualityEngine(spark=spark, catalog="dg_test")


# ---------------------------------------------------------------------------
# No rules
# ---------------------------------------------------------------------------

class TestEngineNoRules:
    def test_all_rows_pass_when_no_rules_defined(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([
            Row(id="A", val=None),
            Row(id="B", val="hello"),
        ])
        config = source_config_factory([])
        result = engine.run(df, config, batch_id="batch_001")

        assert result.success is True
        assert result.rows_read == 2
        assert result.rows_passed == 2
        assert result.rows_failed == 0
        assert result.pass_rate == 1.0
        assert result.violations_df is None
        assert result.passed_df is not None

    def test_disabled_rules_are_skipped(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id=None)])
        config = source_config_factory([
            {"rule": "not_null", "column": "id", "enabled": False}
        ])
        result = engine.run(df, config, batch_id="batch_002")

        # Disabled rule → treated as no rules → all pass
        assert result.rows_passed == 1
        assert result.rows_failed == 0


# ---------------------------------------------------------------------------
# Pass / fail split
# ---------------------------------------------------------------------------

class TestEnginePassFailSplit:
    def test_correct_row_counts(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([
            Row(customer_id="C001"),  # passes
            Row(customer_id="C002"),  # passes
            Row(customer_id=None),    # fails not_null
        ])
        config = source_config_factory([
            {"rule": "not_null", "column": "customer_id"}
        ])
        result = engine.run(df, config, batch_id="b003")

        assert result.success is True
        assert result.rows_read == 3
        assert result.rows_passed == 2
        assert result.rows_failed == 1
        assert result.pass_rate == pytest.approx(2 / 3, abs=1e-3)

    def test_passed_df_has_no_dq_columns(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id="A", name="Alice")])
        config = source_config_factory([
            {"rule": "not_null", "column": "id"}
        ])
        result = engine.run(df, config, batch_id="b004")

        assert result.passed_df is not None
        dq_cols = [c for c in result.passed_df.columns if c.startswith("_dq_")]
        assert dq_cols == [], f"Unexpected DQ columns in passed_df: {dq_cols}"

    def test_failed_df_contains_violations_array(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([
            Row(id=None),
            Row(id="ok"),
        ])
        config = source_config_factory([
            {"rule": "not_null", "column": "id"}
        ])
        result = engine.run(df, config, batch_id="b005")

        assert result.failed_df is not None
        assert "_dq_violations" in result.failed_df.columns
        assert "_dq_run_id" in result.failed_df.columns
        assert "_dq_timestamp" in result.failed_df.columns

    def test_passed_rows_are_correct_records(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([
            Row(id="PASS"),
            Row(id=None),   # should end up in failed_df
        ])
        config = source_config_factory([
            {"rule": "not_null", "column": "id"}
        ])
        result = engine.run(df, config, batch_id="b006")

        assert result.passed_df is not None
        passed_ids = {r["id"] for r in result.passed_df.collect()}
        assert passed_ids == {"PASS"}


# ---------------------------------------------------------------------------
# Violations DataFrame
# ---------------------------------------------------------------------------

class TestEngineViolationsDF:
    def test_violations_df_structure(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id=None)])
        config = source_config_factory([
            {"rule": "not_null", "column": "id"}
        ])
        result = engine.run(df, config, batch_id="b007")

        assert result.violations_df is not None
        violations_cols = set(result.violations_df.columns)
        required = {
            "source_name", "dq_run_id", "batch_id", "record_id",
            "rule_name", "column_name", "severity",
            "error_message", "failed_value", "ingestion_timestamp",
        }
        assert required.issubset(violations_cols), (
            f"Missing columns: {required - violations_cols}"
        )

    def test_violations_df_one_row_per_rule_failure(self, spark, engine, source_config_factory):
        # Two rules, one row that fails both
        df = spark.createDataFrame([
            Row(id=None, price=-1.0),   # fails both not_null(id) and positive_number(price)
            Row(id="ok", price=10.0),   # passes both
        ])
        config = source_config_factory([
            {"rule": "not_null", "column": "id"},
            {"rule": "positive_number", "column": "price"},
        ])
        result = engine.run(df, config, batch_id="b008")

        assert result.violations_df is not None
        # The one failing row should produce 2 violation rows (one per rule)
        violation_count = result.violations_df.count()
        assert violation_count == 2

    def test_violations_source_name_matches_config(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id=None)])
        config = source_config_factory([{"rule": "not_null", "column": "id"}])
        result = engine.run(df, config, batch_id="b009")

        assert result.violations_df is not None
        source_names = {r["source_name"] for r in result.violations_df.collect()}
        assert source_names == {"test_source"}

    def test_violations_batch_id_matches_input(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id=None)])
        config = source_config_factory([{"rule": "not_null", "column": "id"}])
        result = engine.run(df, config, batch_id="my_special_batch")

        assert result.violations_df is not None
        batch_ids = {r["batch_id"] for r in result.violations_df.collect()}
        assert batch_ids == {"my_special_batch"}


# ---------------------------------------------------------------------------
# Rule metrics
# ---------------------------------------------------------------------------

class TestEngineRuleMetrics:
    def test_rule_metrics_count_failures_correctly(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([
            Row(id=None),   # fails
            Row(id=None),   # fails
            Row(id="ok"),   # passes
        ])
        config = source_config_factory([{"rule": "not_null", "column": "id"}])
        result = engine.run(df, config, batch_id="b010")

        assert len(result.rule_metrics) == 1
        metric = result.rule_metrics[0]
        assert metric.rule == "not_null"
        assert metric.column == "id"
        assert metric.failed_rows == 2

    def test_rule_metrics_per_rule_isolation(self, spark, engine, source_config_factory):
        # id fails not_null once; price fails positive_number twice
        df = spark.createDataFrame([
            Row(id=None,  price=-1.0),
            Row(id="ok",  price=-5.0),
            Row(id="ok2", price=10.0),
        ])
        config = source_config_factory([
            {"rule": "not_null", "column": "id"},
            {"rule": "positive_number", "column": "price"},
        ])
        result = engine.run(df, config, batch_id="b011")

        metrics_by_rule = {m.rule: m.failed_rows for m in result.rule_metrics}
        assert metrics_by_rule["not_null"] == 1
        assert metrics_by_rule["positive_number"] == 2

    def test_rule_severity_preserved_in_metrics(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id=None)])
        config = source_config_factory([
            {"rule": "not_null", "column": "id", "severity": "warning"}
        ])
        result = engine.run(df, config, batch_id="b012")

        assert result.rule_metrics[0].severity == "warning"


# ---------------------------------------------------------------------------
# Run metadata
# ---------------------------------------------------------------------------

class TestEngineRunMetadata:
    def test_result_has_unique_dq_run_id(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id="A")])
        config = source_config_factory([{"rule": "not_null", "column": "id"}])
        result1 = engine.run(df, config, batch_id="b001")
        result2 = engine.run(df, config, batch_id="b002")

        assert result1.dq_run_id != result2.dq_run_id

    def test_result_batch_id_matches_input(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id="A")])
        config = source_config_factory([{"rule": "not_null", "column": "id"}])
        result = engine.run(df, config, batch_id="test_batch_xyz")

        assert result.batch_id == "test_batch_xyz"

    def test_result_source_name_matches_config(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id="A")])
        config = source_config_factory([{"rule": "not_null", "column": "id"}])
        result = engine.run(df, config, batch_id="b")

        assert result.source_name == "test_source"

    def test_execution_time_is_positive(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id="A"), Row(id="B")])
        config = source_config_factory([{"rule": "not_null", "column": "id"}])
        result = engine.run(df, config, batch_id="b")

        assert result.execution_time_seconds > 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestEngineErrorHandling:
    def test_unknown_rule_type_returns_failure_result(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([Row(id="A")])
        # Manually build a config with an unregistered rule type
        config = source_config_factory([])
        config.dq_rules.append(
            DQRuleConfig(rule="nonexistent_rule", column="id")
        )
        result = engine.run(df, config, batch_id="b")

        assert result.success is False
        assert result.error_message != ""
        assert result.passed_df is None

    def test_multiple_rules_all_pass(self, spark, engine, source_config_factory):
        df = spark.createDataFrame([
            Row(id="A", email="a@example.com", price=9.99),
            Row(id="B", email="b@test.org", price=1.0),
        ])
        config = source_config_factory([
            {"rule": "not_null", "column": "id"},
            {"rule": "email", "column": "email"},
            {"rule": "positive_number", "column": "price"},
        ])
        result = engine.run(df, config, batch_id="ball_pass")

        assert result.success is True
        assert result.rows_passed == 2
        assert result.rows_failed == 0
        assert result.pass_rate == 1.0
        assert result.violations_df is None or result.violations_df.count() == 0
