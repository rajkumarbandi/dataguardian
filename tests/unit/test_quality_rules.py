"""
Unit tests for DataGuardian DQ rule implementations.

Each test class covers one rule type.  Tests are self-contained — they build
small in-memory DataFrames, apply the rule, and assert on the pass column.

Convention
----------
* Pass column: ``"pass"`` (short alias for clarity in test assertions)
* Null treatment is tested for every rule — most rules pass nulls through
  (defer to not_null for nullability enforcement).
* Params are supplied as plain dicts matching source YAML structure.
"""

from __future__ import annotations

import pytest
from pyspark.sql import Row

# Trigger built-in rule registration
import src.quality.rules  # noqa: F401
from src.quality.rules.allowed_values import AllowedValuesRule
from src.quality.rules.country_code_rule import CountryCodeRule
from src.quality.rules.email_rule import EmailRule
from src.quality.rules.foreign_key import ForeignKeyRule
from src.quality.rules.future_date import FutureDateRule
from src.quality.rules.not_null import NotNullRule
from src.quality.rules.positive_number import PositiveNumberRule
from src.quality.rules.primary_key import PrimaryKeyRule
from src.quality.rules.regex_rule import RegexRule
from src.quality.rules.sql_expression import SqlExpressionRule
from src.quality.rules.unique import UniqueRule

_PASS = "pass"


# ---------------------------------------------------------------------------
# NotNullRule
# ---------------------------------------------------------------------------

class TestNotNullRule:
    def test_passes_non_null(self, spark):
        df = spark.createDataFrame([Row(val="hello")])
        result = NotNullRule().apply(df, "val", _PASS, {})
        assert result.collect()[0][_PASS] is True

    def test_fails_null(self, spark):
        df = spark.createDataFrame([Row(val=None)])
        result = NotNullRule().apply(df, "val", _PASS, {})
        assert result.collect()[0][_PASS] is False

    def test_empty_string_passes(self, spark):
        # not_null only checks for NULL — empty string is not null
        df = spark.createDataFrame([Row(val="")])
        result = NotNullRule().apply(df, "val", _PASS, {})
        assert result.collect()[0][_PASS] is True

    def test_error_message_includes_column(self):
        msg = NotNullRule().error_message("customer_id", {})
        assert "customer_id" in msg


# ---------------------------------------------------------------------------
# UniqueRule
# ---------------------------------------------------------------------------

class TestUniqueRule:
    def test_passes_unique_values(self, spark):
        df = spark.createDataFrame([Row(val="A"), Row(val="B"), Row(val="C")])
        result = UniqueRule().apply(df, "val", _PASS, {})
        assert all(r[_PASS] for r in result.collect())

    def test_fails_duplicate_values(self, spark):
        df = spark.createDataFrame([Row(val="X"), Row(val="X"), Row(val="Y")])
        rows = UniqueRule().apply(df, "val", _PASS, {}).collect()
        duplicated = [r[_PASS] for r in rows if r["val"] == "X"]
        assert all(p is False for p in duplicated)
        unique_pass = [r[_PASS] for r in rows if r["val"] == "Y"]
        assert all(p is True for p in unique_pass)

    def test_two_nulls_are_duplicates(self, spark):
        df = spark.createDataFrame([Row(val=None), Row(val=None)])
        result = UniqueRule().apply(df, "val", _PASS, {})
        assert all(r[_PASS] is False for r in result.collect())


# ---------------------------------------------------------------------------
# RegexRule
# ---------------------------------------------------------------------------

class TestRegexRule:
    def test_passes_matching_pattern(self, spark):
        df = spark.createDataFrame([Row(val="AB123")])
        result = RegexRule().apply(df, "val", _PASS, {"pattern": r"^[A-Z]{2}\d{3}$"})
        assert result.collect()[0][_PASS] is True

    def test_fails_non_matching_pattern(self, spark):
        df = spark.createDataFrame([Row(val="ab123")])
        result = RegexRule().apply(df, "val", _PASS, {"pattern": r"^[A-Z]{2}\d{3}$"})
        assert result.collect()[0][_PASS] is False

    def test_null_passes(self, spark):
        df = spark.createDataFrame([Row(val=None)])
        result = RegexRule().apply(df, "val", _PASS, {"pattern": r"^[A-Z]+$"})
        assert result.collect()[0][_PASS] is True

    def test_default_pattern_matches_everything(self, spark):
        df = spark.createDataFrame([Row(val="anything 123 !@#")])
        result = RegexRule().apply(df, "val", _PASS, {})
        assert result.collect()[0][_PASS] is True


# ---------------------------------------------------------------------------
# EmailRule
# ---------------------------------------------------------------------------

class TestEmailRule:
    @pytest.mark.parametrize("address", [
        "user@example.com",
        "first.last+tag@sub.domain.co.uk",
        "user123@company.org",
    ])
    def test_passes_valid_emails(self, spark, address):
        df = spark.createDataFrame([Row(email=address)])
        result = EmailRule().apply(df, "email", _PASS, {})
        assert result.collect()[0][_PASS] is True, f"Expected {address!r} to pass"

    @pytest.mark.parametrize("address", [
        "notanemail",
        "missing@tld",
        "spaces in@email.com",
        "@nodomain.com",
        "no-at-sign.com",
    ])
    def test_fails_invalid_emails(self, spark, address):
        df = spark.createDataFrame([Row(email=address)])
        result = EmailRule().apply(df, "email", _PASS, {})
        assert result.collect()[0][_PASS] is False, f"Expected {address!r} to fail"

    def test_null_passes(self, spark):
        df = spark.createDataFrame([Row(email=None)])
        result = EmailRule().apply(df, "email", _PASS, {})
        assert result.collect()[0][_PASS] is True


# ---------------------------------------------------------------------------
# CountryCodeRule
# ---------------------------------------------------------------------------

class TestCountryCodeRule:
    @pytest.mark.parametrize("code", ["US", "GB", "DE", "FR", "AU", "JP", "CA"])
    def test_passes_valid_iso_codes(self, spark, code):
        df = spark.createDataFrame([Row(country=code)])
        result = CountryCodeRule().apply(df, "country", _PASS, {})
        assert result.collect()[0][_PASS] is True, f"Expected {code!r} to pass"

    @pytest.mark.parametrize("code", ["UK", "ZZ", "XX", "ENG", "USA"])
    def test_fails_invalid_codes(self, spark, code):
        df = spark.createDataFrame([Row(country=code)])
        result = CountryCodeRule().apply(df, "country", _PASS, {})
        assert result.collect()[0][_PASS] is False, f"Expected {code!r} to fail"

    def test_null_passes(self, spark):
        df = spark.createDataFrame([Row(country=None)])
        result = CountryCodeRule().apply(df, "country", _PASS, {})
        assert result.collect()[0][_PASS] is True

    def test_custom_allowed_codes(self, spark):
        df = spark.createDataFrame([Row(country="DE"), Row(country="FR"), Row(country="AU")])
        result = CountryCodeRule().apply(df, "country", _PASS, {"allowed_codes": ["US", "GB"]})
        rows = {r["country"]: r[_PASS] for r in result.collect()}
        assert rows["DE"] is False
        assert rows["FR"] is False
        assert rows["AU"] is False


# ---------------------------------------------------------------------------
# PositiveNumberRule
# ---------------------------------------------------------------------------

class TestPositiveNumberRule:
    def test_passes_positive_integer(self, spark):
        df = spark.createDataFrame([Row(n=5)])
        result = PositiveNumberRule().apply(df, "n", _PASS, {})
        assert result.collect()[0][_PASS] is True

    def test_passes_positive_float(self, spark):
        df = spark.createDataFrame([Row(n=0.01)])
        result = PositiveNumberRule().apply(df, "n", _PASS, {})
        assert result.collect()[0][_PASS] is True

    def test_fails_negative_number(self, spark):
        df = spark.createDataFrame([Row(n=-1)])
        result = PositiveNumberRule().apply(df, "n", _PASS, {})
        assert result.collect()[0][_PASS] is False

    def test_fails_zero_by_default(self, spark):
        df = spark.createDataFrame([Row(n=0)])
        result = PositiveNumberRule().apply(df, "n", _PASS, {})
        assert result.collect()[0][_PASS] is False

    def test_passes_zero_with_allow_zero(self, spark):
        df = spark.createDataFrame([Row(n=0)])
        result = PositiveNumberRule().apply(df, "n", _PASS, {"allow_zero": True})
        assert result.collect()[0][_PASS] is True

    def test_null_passes(self, spark):
        df = spark.createDataFrame([Row(n=None)])
        result = PositiveNumberRule().apply(df, "n", _PASS, {})
        assert result.collect()[0][_PASS] is True


# ---------------------------------------------------------------------------
# AllowedValuesRule
# ---------------------------------------------------------------------------

class TestAllowedValuesRule:
    def test_passes_value_in_list(self, spark):
        df = spark.createDataFrame([Row(status="ACTIVE")])
        result = AllowedValuesRule().apply(df, "status", _PASS, {"values": ["ACTIVE", "INACTIVE"]})
        assert result.collect()[0][_PASS] is True

    def test_fails_value_not_in_list(self, spark):
        df = spark.createDataFrame([Row(status="UNKNOWN")])
        result = AllowedValuesRule().apply(df, "status", _PASS, {"values": ["ACTIVE", "INACTIVE"]})
        assert result.collect()[0][_PASS] is False

    def test_null_passes(self, spark):
        df = spark.createDataFrame([Row(status=None)])
        result = AllowedValuesRule().apply(df, "status", _PASS, {"values": ["ACTIVE"]})
        assert result.collect()[0][_PASS] is True

    def test_case_sensitive_matching(self, spark):
        # "active" != "ACTIVE" — values list is matched case-sensitively
        df = spark.createDataFrame([Row(status="active")])
        result = AllowedValuesRule().apply(df, "status", _PASS, {"values": ["ACTIVE"]})
        assert result.collect()[0][_PASS] is False

    def test_numeric_values_cast_to_string(self, spark):
        df = spark.createDataFrame([Row(tier=1)])
        result = AllowedValuesRule().apply(df, "tier", _PASS, {"values": [1, 2, 3]})
        assert result.collect()[0][_PASS] is True


# ---------------------------------------------------------------------------
# FutureDateRule
# ---------------------------------------------------------------------------

class TestFutureDateRule:
    def test_passes_past_date(self, spark):
        df = spark.createDataFrame([Row(dt="2020-01-01")])
        result = FutureDateRule().apply(df, "dt", _PASS, {})
        assert result.collect()[0][_PASS] is True

    def test_fails_future_date(self, spark):
        df = spark.createDataFrame([Row(dt="2099-12-31")])
        result = FutureDateRule().apply(df, "dt", _PASS, {})
        assert result.collect()[0][_PASS] is False

    def test_null_passes(self, spark):
        df = spark.createDataFrame([Row(dt=None)])
        result = FutureDateRule().apply(df, "dt", _PASS, {})
        assert result.collect()[0][_PASS] is True


# ---------------------------------------------------------------------------
# PrimaryKeyRule
# ---------------------------------------------------------------------------

class TestPrimaryKeyRule:
    def test_passes_unique_non_null(self, spark):
        df = spark.createDataFrame([Row(pk="A"), Row(pk="B"), Row(pk="C")])
        result = PrimaryKeyRule().apply(df, "pk", _PASS, {})
        assert all(r[_PASS] for r in result.collect())

    def test_fails_duplicate_pk(self, spark):
        df = spark.createDataFrame([Row(pk="A"), Row(pk="A")])
        result = PrimaryKeyRule().apply(df, "pk", _PASS, {})
        assert all(r[_PASS] is False for r in result.collect())

    def test_fails_null_pk(self, spark):
        df = spark.createDataFrame([Row(pk=None), Row(pk="B")])
        result = PrimaryKeyRule().apply(df, "pk", _PASS, {})
        row_map = {(r["pk"] or "__NULL__"): r[_PASS] for r in result.collect()}
        assert row_map["__NULL__"] is False
        assert row_map["B"] is True

    def test_composite_key_all_combinations_unique(self, spark):
        df = spark.createDataFrame([
            Row(order_id="O1", product_id="P1"),
            Row(order_id="O1", product_id="P2"),
            Row(order_id="O2", product_id="P1"),
        ])
        result = PrimaryKeyRule().apply(
            df, "order_id", _PASS, {"columns": ["order_id", "product_id"]}
        )
        assert all(r[_PASS] for r in result.collect())

    def test_composite_key_duplicate_fails(self, spark):
        df = spark.createDataFrame([
            Row(order_id="O1", product_id="P1"),
            Row(order_id="O1", product_id="P1"),
        ])
        result = PrimaryKeyRule().apply(
            df, "order_id", _PASS, {"columns": ["order_id", "product_id"]}
        )
        assert all(r[_PASS] is False for r in result.collect())


# ---------------------------------------------------------------------------
# ForeignKeyRule
# ---------------------------------------------------------------------------

class TestForeignKeyRule:
    def test_soft_fail_when_no_spark_provided(self, spark):
        # Without spark, all rows pass (soft failure)
        df = spark.createDataFrame([Row(customer_id="C001"), Row(customer_id="CXXX")])
        result = ForeignKeyRule().apply(
            df, "customer_id", _PASS,
            {"reference_table": "dg_dev.bronze.erp_customers", "reference_column": "customer_id"},
            spark=None,
        )
        assert all(r[_PASS] is True for r in result.collect())

    def test_passes_when_reference_exists(self, spark):
        # Create a temp view to simulate a reference table
        ref_df = spark.createDataFrame([Row(customer_id="C001"), Row(customer_id="C002")])
        ref_df.createOrReplaceTempView("_test_ref_customers")

        df = spark.createDataFrame([Row(customer_id="C001"), Row(customer_id="C002")])
        result = ForeignKeyRule().apply(
            df, "customer_id", _PASS,
            {"reference_table": "_test_ref_customers", "reference_column": "customer_id"},
            spark=spark,
        )
        assert all(r[_PASS] is True for r in result.collect())

    def test_fails_when_key_missing_from_reference(self, spark):
        ref_df = spark.createDataFrame([Row(customer_id="C001")])
        ref_df.createOrReplaceTempView("_test_ref_customers_partial")

        df = spark.createDataFrame([Row(customer_id="C001"), Row(customer_id="CXXX")])
        rows = ForeignKeyRule().apply(
            df, "customer_id", _PASS,
            {"reference_table": "_test_ref_customers_partial", "reference_column": "customer_id"},
            spark=spark,
        ).collect()
        result_map = {r["customer_id"]: r[_PASS] for r in rows}
        assert result_map["C001"] is True
        assert result_map["CXXX"] is False

    def test_null_passes(self, spark):
        ref_df = spark.createDataFrame([Row(customer_id="C001")])
        ref_df.createOrReplaceTempView("_test_ref_fk_null")

        df = spark.createDataFrame([Row(customer_id=None)])
        result = ForeignKeyRule().apply(
            df, "customer_id", _PASS,
            {"reference_table": "_test_ref_fk_null", "reference_column": "customer_id"},
            spark=spark,
        )
        assert result.collect()[0][_PASS] is True


# ---------------------------------------------------------------------------
# SqlExpressionRule
# ---------------------------------------------------------------------------

class TestSqlExpressionRule:
    def test_passes_true_expression(self, spark):
        df = spark.createDataFrame([Row(n=50)])
        result = SqlExpressionRule().apply(
            df, "n", _PASS, {"expression": "n >= 0 AND n <= 100"}
        )
        assert result.collect()[0][_PASS] is True

    def test_fails_false_expression(self, spark):
        df = spark.createDataFrame([Row(n=150)])
        result = SqlExpressionRule().apply(
            df, "n", _PASS, {"expression": "n >= 0 AND n <= 100"}
        )
        assert result.collect()[0][_PASS] is False

    def test_cross_column_expression(self, spark):
        df = spark.createDataFrame([
            Row(start_date="2024-01-01", end_date="2024-12-31"),
            Row(start_date="2024-06-01", end_date="2024-01-01"),
        ])
        result = SqlExpressionRule().apply(
            df, "end_date", _PASS,
            {"expression": "end_date IS NULL OR end_date >= start_date"},
        )
        rows = result.collect()
        assert rows[0][_PASS] is True   # valid range
        assert rows[1][_PASS] is False  # end before start

    def test_null_handling_controlled_by_expression(self, spark):
        df = spark.createDataFrame([Row(n=None)])
        result = SqlExpressionRule().apply(
            df, "n", _PASS, {"expression": "n IS NULL OR n > 0"}
        )
        assert result.collect()[0][_PASS] is True

    def test_error_message_includes_description_when_present(self):
        msg = SqlExpressionRule().error_message(
            "discount_pct",
            {"expression": "discount_pct >= 0", "description": "Discount must be non-negative"},
        )
        assert "Discount must be non-negative" in msg


# ---------------------------------------------------------------------------
# RuleRegistry integration
# ---------------------------------------------------------------------------

class TestRuleRegistry:
    def test_all_11_rules_registered(self):
        from src.quality.registry import RuleRegistry
        registered = set(RuleRegistry.registered_types())
        expected = {
            "not_null", "unique", "regex", "email", "country_code",
            "positive_number", "allowed_values", "future_date",
            "primary_key", "foreign_key", "sql_expression",
        }
        assert expected.issubset(registered), (
            f"Missing rules: {expected - registered}"
        )

    def test_get_returns_fresh_instance_each_call(self):
        from src.quality.registry import RuleRegistry
        r1 = RuleRegistry.get("not_null")
        r2 = RuleRegistry.get("not_null")
        assert r1 is not r2

    def test_unknown_rule_raises_configuration_error(self):
        from src.common.exceptions import ConfigurationError
        from src.quality.registry import RuleRegistry
        with pytest.raises(ConfigurationError, match="Unknown DQ rule type"):
            RuleRegistry.get("definitely_not_a_real_rule")
