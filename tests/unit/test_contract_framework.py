"""
Unit tests for the M7 Data Contracts & Data Product Governance framework.

Coverage:
- ContractRuleResult and ContractValidationResult (model, properties, serialisation)
- ContractValidationEngine — all 7 rule categories
- Policy enforcement: FAIL_PIPELINE, WARNING_ONLY, IGNORE
- No-contract / disabled paths
- ContractConfig and ContractEnvConfig Pydantic validation
- ContractHistoryWriter (disabled mode — no Delta writes)
- PipelineRun and PipelineSummary contract field pass-through

All contract validator tests use a local SparkSession for schema/column checks
(no Spark actions are triggered — only metadata attributes are accessed).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    _SPARK_AVAILABLE = True
except ImportError:
    _SPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _SPARK_AVAILABLE, reason="PySpark not available"
)


# ===========================================================================
# Spark session
# ===========================================================================


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("dg-test-contracts")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ===========================================================================
# Fixtures — small DataFrames
# ===========================================================================


def _customers_df(spark):
    data = [
        ("C001", "Alice", "alice@example.com", "GB", True, 500.0, "Enterprise"),
        ("C002", "Bob", "bob@test.com", "US", False, None, "SMB"),
        ("C003", "Carol", "carol@co.uk", "AU", True, 100.0, "Startup"),
    ]
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("annual_revenue", DoubleType(), True),
        StructField("customer_segment", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


def _minimal_df(spark):
    """DataFrame with only id and name — used to test missing-column rules."""
    data = [("X001", "foo"), ("X002", "bar")]
    schema = StructType([
        StructField("item_id", StringType(), False),
        StructField("name", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


# ===========================================================================
# Helpers — build source configs and mock results
# ===========================================================================


def _make_source_config(contract_kwargs: dict | None = None, dq_rules: list | None = None):
    """Build a minimal SourceConfig with an optional ContractConfig."""
    from src.common.models import (
        ConnectorConfig,
        ContractConfig,
        ContractRowCountConfig,
        DQRuleConfig,
        SourceConfig,
        TargetConfig,
    )

    dq = [
        DQRuleConfig(rule=r["rule"], column=r["column"])
        for r in (dq_rules or [])
    ]

    contract = None
    if contract_kwargs is not None:
        row_count_raw = contract_kwargs.pop("row_count", {})
        row_count = ContractRowCountConfig(**row_count_raw) if row_count_raw else ContractRowCountConfig()
        contract = ContractConfig(**contract_kwargs, row_count=row_count)

    return SourceConfig(
        name="test_source",
        system="test",
        connector=ConnectorConfig(type="csv", location="/tmp/x"),
        target=TargetConfig(catalog="dg_test", schema="bronze", table="test"),
        dq_rules=dq,
        contract=contract,
    )


def _make_schema_result(version: int = 1):
    """Minimal mock for SchemaValidationResult."""
    mock = MagicMock()
    mock.schema_version = version
    return mock


def _make_dq_result(rows_read: int = 100, rules: list[dict] | None = None):
    """Minimal mock for DQRunResult."""
    mock = MagicMock()
    mock.rows_read = rows_read
    mock.rows_passed = rows_read
    mock.rows_failed = 0

    metric_mocks = []
    for r in (rules or []):
        m = MagicMock()
        m.rule_name = r["rule"]
        m.column_name = r["column"]
        m.violations = r.get("violations", 0)
        metric_mocks.append(m)
    mock.rule_metrics = metric_mocks
    return mock


def _make_transform_result():
    mock = MagicMock()
    mock.transformations_executed = 3
    mock.transformations_failed = 0
    return mock


def _run_validate(
    spark,
    contract_kwargs: dict,
    dq_rules: list | None = None,
    df=None,
    row_count: int = 3,
    schema_version: int = 1,
    dq_rule_mocks: list[dict] | None = None,
    env_policy: str = "FAIL_PIPELINE",
):
    from src.contracts.contract_validator import ContractValidationEngine

    source_config = _make_source_config(contract_kwargs, dq_rules)
    schema_result = _make_schema_result(schema_version)
    dq_result = _make_dq_result(rows_read=row_count, rules=dq_rule_mocks)
    transform_result = _make_transform_result()
    df = df or _customers_df(spark)

    engine = ContractValidationEngine()
    return engine.validate(
        source_config=source_config,
        schema_result=schema_result,
        dq_result=dq_result,
        transformation_result=transform_result,
        df=df,
        row_count=row_count,
        run_id="test-run",
        env_policy=env_policy,
    )


# ===========================================================================
# ContractRuleResult
# ===========================================================================


class TestContractRuleResult:
    def test_to_dict_keys(self):
        from src.contracts.contract_model import ContractRuleResult

        r = ContractRuleResult(
            rule_name="required_columns",
            passed=True,
            severity="error",
            category="structure",
            message="Column exists",
            column="customer_id",
        )
        d = r.to_dict()
        assert "rule_name" in d
        assert "passed" in d
        assert "severity" in d
        assert "category" in d
        assert "message" in d
        assert "column" in d

    def test_default_column_is_empty(self):
        from src.contracts.contract_model import ContractRuleResult

        r = ContractRuleResult("row_count", True, "error", "business", "ok")
        assert r.column == ""


# ===========================================================================
# ContractValidationResult
# ===========================================================================


class TestContractValidationResult:
    def _make_result(
        self,
        is_valid=True,
        can_proceed=True,
        policy="FAIL_PIPELINE",
        rules_passed=5,
        rules_failed=0,
        warnings=0,
        broken=None,
        all_rules=None,
    ):
        from src.contracts.contract_model import ContractValidationResult

        return ContractValidationResult(
            source_name="src",
            contract_name="test_contract",
            contract_version="1.0.0",
            validation_policy=policy,
            is_valid=is_valid,
            can_proceed=can_proceed,
            rules_passed=rules_passed,
            rules_failed=rules_failed,
            warnings=warnings,
            broken_rules=broken or [],
            all_rules=all_rules or [],
            message="ok",
        )

    def test_status_passed(self):
        r = self._make_result(all_rules=[MagicMock()])
        assert r.status == "PASSED"

    def test_status_failed(self):
        r = self._make_result(is_valid=False, can_proceed=False, policy="FAIL_PIPELINE", all_rules=[MagicMock()])
        assert r.status == "FAILED"

    def test_status_warning(self):
        r = self._make_result(is_valid=False, can_proceed=True, policy="WARNING_ONLY", all_rules=[MagicMock()])
        assert r.status == "WARNING"

    def test_status_skipped_when_no_rules(self):
        r = self._make_result(all_rules=[])
        assert r.status == "SKIPPED"

    def test_broken_rules_json(self):
        from src.contracts.contract_model import ContractRuleResult, ContractValidationResult
        import json

        rule = ContractRuleResult("required_columns", False, "error", "structure", "missing col", "email")
        r = ContractValidationResult(
            source_name="x", contract_name="c", contract_version="1.0.0",
            validation_policy="FAIL_PIPELINE", is_valid=False, can_proceed=False,
            rules_passed=0, rules_failed=1, warnings=0,
            broken_rules=[rule], all_rules=[rule], message="fail",
        )
        parsed = json.loads(r.broken_rules_json())
        assert len(parsed) == 1
        assert parsed[0]["rule_name"] == "required_columns"

    def test_to_dict_contains_required_keys(self):
        r = self._make_result()
        d = r.to_dict()
        for key in ("source_name", "contract_name", "contract_version",
                    "validation_policy", "status", "is_valid", "can_proceed",
                    "rules_passed", "rules_failed", "warnings", "message"):
            assert key in d


# ===========================================================================
# ContractValidationEngine — no contract path
# ===========================================================================


class TestNoContractPath:
    def test_none_contract_returns_skipped(self, spark):
        from src.contracts.contract_validator import ContractValidationEngine
        from src.common.models import ConnectorConfig, SourceConfig, TargetConfig

        sc = SourceConfig(
            name="no_contract_source",
            system="test",
            connector=ConnectorConfig(type="csv", location="/tmp/x"),
            target=TargetConfig(catalog="c", schema="s", table="t"),
            contract=None,
        )
        engine = ContractValidationEngine()
        result = engine.validate(
            source_config=sc,
            schema_result=_make_schema_result(),
            dq_result=_make_dq_result(),
            transformation_result=_make_transform_result(),
            df=_customers_df(spark),
            row_count=3,
        )
        assert result.is_valid is True
        assert result.can_proceed is True
        assert result.all_rules == []

    def test_ignore_policy_returns_skipped(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "x", "validation_policy": "IGNORE"},
        )
        assert result.is_valid is True
        assert result.can_proceed is True
        assert result.all_rules == []


# ===========================================================================
# ContractValidationEngine — required_columns
# ===========================================================================


class TestRequiredColumns:
    def test_all_required_columns_present(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "required_columns": ["customer_id", "email"]},
        )
        rules = [r for r in result.all_rules if r.rule_name == "required_columns"]
        assert all(r.passed for r in rules)
        assert result.is_valid is True

    def test_missing_required_column_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "required_columns": ["customer_id", "missing_col"]},
        )
        rules = [r for r in result.all_rules if r.rule_name == "required_columns"]
        failed = [r for r in rules if not r.passed]
        assert len(failed) == 1
        assert "missing_col" in failed[0].message
        assert result.is_valid is False

    def test_empty_required_columns_no_rules(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "required_columns": []},
        )
        rules = [r for r in result.all_rules if r.rule_name == "required_columns"]
        assert rules == []


# ===========================================================================
# ContractValidationEngine — allowed_datatypes
# ===========================================================================


class TestAllowedDatatypes:
    def test_correct_types_pass(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={
                "name": "c",
                "allowed_datatypes": {
                    "customer_id": "string",
                    "is_active": "boolean",
                    "annual_revenue": "double",
                },
            },
        )
        rules = [r for r in result.all_rules if r.rule_name == "allowed_datatypes"]
        assert all(r.passed for r in rules)

    def test_wrong_type_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={
                "name": "c",
                "allowed_datatypes": {"customer_id": "integer"},  # actually string
            },
        )
        rules = [r for r in result.all_rules if r.rule_name == "allowed_datatypes"]
        assert any(not r.passed for r in rules)
        assert result.is_valid is False

    def test_missing_column_for_type_check_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "allowed_datatypes": {"does_not_exist": "string"}},
        )
        rules = [r for r in result.all_rules if r.rule_name == "allowed_datatypes"]
        assert len(rules) == 1
        assert rules[0].passed is False

    def test_integer_alias_int_passes(self, spark):
        from src.contracts.contract_validator import _types_compatible
        assert _types_compatible("integer", "int") is True

    def test_long_alias_bigint_passes(self, spark):
        from src.contracts.contract_validator import _types_compatible
        assert _types_compatible("long", "bigint") is True

    def test_incompatible_types_fail(self, spark):
        from src.contracts.contract_validator import _types_compatible
        assert _types_compatible("string", "integer") is False


# ===========================================================================
# ContractValidationEngine — primary_keys
# ===========================================================================


class TestPrimaryKeys:
    def test_pk_with_both_dq_rules_passes(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "primary_keys": ["customer_id"]},
            dq_rules=[
                {"rule": "not_null", "column": "customer_id"},
                {"rule": "unique", "column": "customer_id"},
            ],
        )
        rules = [r for r in result.all_rules if r.rule_name == "primary_keys"]
        assert all(r.passed for r in rules)

    def test_pk_missing_not_null_rule_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "primary_keys": ["customer_id"]},
            dq_rules=[{"rule": "unique", "column": "customer_id"}],
        )
        rules = [r for r in result.all_rules if r.rule_name == "primary_keys"]
        assert any(not r.passed for r in rules)
        assert result.is_valid is False

    def test_pk_missing_unique_rule_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "primary_keys": ["customer_id"]},
            dq_rules=[{"rule": "not_null", "column": "customer_id"}],
        )
        rules = [r for r in result.all_rules if r.rule_name == "primary_keys"]
        assert any(not r.passed for r in rules)

    def test_pk_column_not_in_schema_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "primary_keys": ["ghost_id"]},
            dq_rules=[
                {"rule": "not_null", "column": "ghost_id"},
                {"rule": "unique", "column": "ghost_id"},
            ],
        )
        rules = [r for r in result.all_rules if r.rule_name == "primary_keys"]
        assert rules[0].passed is False
        assert "missing" in rules[0].message.lower()


# ===========================================================================
# ContractValidationEngine — non_nullable_columns
# ===========================================================================


class TestNonNullableColumns:
    def test_column_with_not_null_rule_and_zero_violations_passes(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "non_nullable_columns": ["customer_id"]},
            dq_rules=[{"rule": "not_null", "column": "customer_id"}],
            dq_rule_mocks=[{"rule": "not_null", "column": "customer_id", "violations": 0}],
        )
        rules = [r for r in result.all_rules if r.rule_name == "non_nullable_columns"]
        assert rules[0].passed is True

    def test_column_with_violations_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "non_nullable_columns": ["customer_id"]},
            dq_rules=[{"rule": "not_null", "column": "customer_id"}],
            dq_rule_mocks=[{"rule": "not_null", "column": "customer_id", "violations": 5}],
        )
        rules = [r for r in result.all_rules if r.rule_name == "non_nullable_columns"]
        assert rules[0].passed is False
        assert "5" in rules[0].message

    def test_column_missing_from_schema_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "non_nullable_columns": ["ghost_col"]},
        )
        rules = [r for r in result.all_rules if r.rule_name == "non_nullable_columns"]
        assert rules[0].passed is False


# ===========================================================================
# ContractValidationEngine — row_count
# ===========================================================================


class TestRowCount:
    def test_within_range_passes(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "row_count": {"min": 1, "max": 100}},
            row_count=50,
        )
        rules = [r for r in result.all_rules if r.rule_name == "row_count"]
        assert rules[0].passed is True

    def test_below_minimum_fails_with_error(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "row_count": {"min": 10}},
            row_count=3,
        )
        rules = [r for r in result.all_rules if r.rule_name == "row_count"]
        assert rules[0].passed is False
        assert rules[0].severity == "error"
        assert result.is_valid is False

    def test_above_maximum_warns(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "row_count": {"max": 2}},
            row_count=3,
        )
        rules = [r for r in result.all_rules if r.rule_name == "row_count"]
        assert rules[0].passed is False
        assert rules[0].severity == "warning"
        # Warnings don't invalidate — is_valid depends only on error rules
        assert result.is_valid is True
        assert result.warnings == 1

    def test_exact_minimum_passes(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "row_count": {"min": 3}},
            row_count=3,
        )
        rules = [r for r in result.all_rules if r.rule_name == "row_count"]
        assert rules[0].passed is True

    def test_no_row_count_config_produces_no_rule(self, spark):
        result = _run_validate(spark, contract_kwargs={"name": "c"})
        rules = [r for r in result.all_rules if r.rule_name == "row_count"]
        assert rules == []


# ===========================================================================
# ContractValidationEngine — required_dq_rules
# ===========================================================================


class TestRequiredDQRules:
    def test_required_rule_configured_passes(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "required_dq_rules": ["not_null", "unique"]},
            dq_rules=[
                {"rule": "not_null", "column": "customer_id"},
                {"rule": "unique", "column": "customer_id"},
            ],
        )
        rules = [r for r in result.all_rules if r.rule_name == "required_dq_rules"]
        assert all(r.passed for r in rules)

    def test_missing_required_rule_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "required_dq_rules": ["email"]},
            dq_rules=[{"rule": "not_null", "column": "customer_id"}],
        )
        rules = [r for r in result.all_rules if r.rule_name == "required_dq_rules"]
        assert rules[0].passed is False
        assert "email" in rules[0].message
        assert result.is_valid is False


# ===========================================================================
# ContractValidationEngine — schema_version_min
# ===========================================================================


class TestSchemaVersionMin:
    def test_schema_version_meets_minimum(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "schema_version_min": 1},
            schema_version=2,
        )
        rules = [r for r in result.all_rules if r.rule_name == "schema_version_min"]
        assert rules[0].passed is True

    def test_exact_minimum_version_passes(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "schema_version_min": 1},
            schema_version=1,
        )
        rules = [r for r in result.all_rules if r.rule_name == "schema_version_min"]
        assert rules[0].passed is True

    def test_schema_version_below_minimum_fails(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "schema_version_min": 3},
            schema_version=1,
        )
        rules = [r for r in result.all_rules if r.rule_name == "schema_version_min"]
        assert rules[0].passed is False
        assert result.is_valid is False

    def test_none_schema_result_treated_as_version_zero(self, spark):
        from src.contracts.contract_validator import ContractValidationEngine

        source_config = _make_source_config(
            {"name": "c", "schema_version_min": 1}
        )
        engine = ContractValidationEngine()
        result = engine.validate(
            source_config=source_config,
            schema_result=None,
            dq_result=_make_dq_result(),
            transformation_result=_make_transform_result(),
            df=_customers_df(spark),
            row_count=3,
        )
        rules = [r for r in result.all_rules if r.rule_name == "schema_version_min"]
        assert rules[0].passed is False


# ===========================================================================
# Policy enforcement
# ===========================================================================


class TestPolicyEnforcement:
    def test_fail_pipeline_sets_can_proceed_false(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={
                "name": "c",
                "required_columns": ["definitely_missing"],
            },
            env_policy="FAIL_PIPELINE",
        )
        assert result.is_valid is False
        assert result.can_proceed is False
        assert result.status == "FAILED"

    def test_warning_only_can_proceed_even_with_failures(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={
                "name": "c",
                "required_columns": ["definitely_missing"],
                "validation_policy": "WARNING_ONLY",
            },
        )
        assert result.is_valid is False
        assert result.can_proceed is True
        assert result.status == "WARNING"

    def test_source_policy_overrides_env_policy(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={
                "name": "c",
                "required_columns": ["definitely_missing"],
                "validation_policy": "WARNING_ONLY",  # source overrides env
            },
            env_policy="FAIL_PIPELINE",
        )
        assert result.can_proceed is True

    def test_all_rules_pass_is_valid(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={
                "name": "c",
                "required_columns": ["customer_id", "email"],
                "row_count": {"min": 1},
            },
            row_count=3,
        )
        assert result.is_valid is True
        assert result.can_proceed is True
        assert result.status == "PASSED"

    def test_warnings_do_not_affect_is_valid(self, spark):
        # row_count max exceeded → warning, not error
        result = _run_validate(
            spark,
            contract_kwargs={"name": "c", "row_count": {"max": 1}},
            row_count=3,
        )
        assert result.is_valid is True   # warnings don't fail the contract
        assert result.warnings == 1

    def test_broken_rules_list_contains_only_failures(self, spark):
        result = _run_validate(
            spark,
            contract_kwargs={
                "name": "c",
                "required_columns": ["customer_id", "ghost_col"],
            },
        )
        assert all(not r.passed for r in result.broken_rules)
        assert len(result.broken_rules) == 1


# ===========================================================================
# ContractConfig Pydantic model validation
# ===========================================================================


class TestContractConfigModel:
    def test_valid_contract_config(self):
        from src.common.models import ContractConfig

        cfg = ContractConfig(
            name="test_contract",
            version="2.0.0",
            owner="team@co.com",
            domain="sales",
            criticality="high",
            expected_refresh="daily",
            validation_policy="FAIL_PIPELINE",
            required_columns=["id"],
            primary_keys=["id"],
        )
        assert cfg.name == "test_contract"
        assert cfg.criticality == "high"
        assert cfg.validation_policy == "FAIL_PIPELINE"

    def test_criticality_case_insensitive(self):
        from src.common.models import ContractConfig

        cfg = ContractConfig(name="x", criticality="HIGH")
        assert cfg.criticality == "high"

    def test_invalid_criticality_raises(self):
        from pydantic import ValidationError
        from src.common.models import ContractConfig

        with pytest.raises(ValidationError):
            ContractConfig(name="x", criticality="ultra")

    def test_validation_policy_normalised_to_upper(self):
        from src.common.models import ContractConfig

        cfg = ContractConfig(name="x", validation_policy="warning_only")
        assert cfg.validation_policy == "WARNING_ONLY"

    def test_invalid_validation_policy_raises(self):
        from pydantic import ValidationError
        from src.common.models import ContractConfig

        with pytest.raises(ValidationError):
            ContractConfig(name="x", validation_policy="HALT")

    def test_none_validation_policy_is_allowed(self):
        from src.common.models import ContractConfig

        cfg = ContractConfig(name="x", validation_policy=None)
        assert cfg.validation_policy is None

    def test_row_count_config_optional_bounds(self):
        from src.common.models import ContractConfig, ContractRowCountConfig

        cfg = ContractConfig(name="x", row_count=ContractRowCountConfig(min=1))
        assert cfg.row_count.min == 1
        assert cfg.row_count.max is None

    def test_source_config_contract_field_defaults_to_none(self):
        from src.common.models import ConnectorConfig, SourceConfig, TargetConfig

        sc = SourceConfig(
            name="x",
            system="s",
            connector=ConnectorConfig(type="csv", location="/tmp"),
            target=TargetConfig(catalog="c", schema="s", table="t"),
        )
        assert sc.contract is None

    def test_source_config_accepts_contract(self):
        from src.common.models import (
            ConnectorConfig,
            ContractConfig,
            SourceConfig,
            TargetConfig,
        )

        sc = SourceConfig(
            name="x",
            system="s",
            connector=ConnectorConfig(type="csv", location="/tmp"),
            target=TargetConfig(catalog="c", schema="s", table="t"),
            contract=ContractConfig(name="my_contract"),
        )
        assert sc.contract is not None
        assert sc.contract.name == "my_contract"


# ===========================================================================
# ContractEnvConfig
# ===========================================================================


class TestContractEnvConfig:
    def test_defaults(self):
        from src.common.models import ContractEnvConfig

        cfg = ContractEnvConfig()
        assert cfg.contract_validation_enabled is True
        assert cfg.contract_audit_enabled is True
        assert cfg.default_contract_policy == "FAIL_PIPELINE"

    def test_valid_policies(self):
        from src.common.models import ContractEnvConfig

        for policy in ["FAIL_PIPELINE", "WARNING_ONLY", "IGNORE"]:
            cfg = ContractEnvConfig(default_contract_policy=policy)
            assert cfg.default_contract_policy == policy

    def test_invalid_policy_raises(self):
        from pydantic import ValidationError
        from src.common.models import ContractEnvConfig

        with pytest.raises(ValidationError):
            ContractEnvConfig(default_contract_policy="ABORT")

    def test_environment_config_has_contract_validation_field(self):
        from src.common.models import ContractEnvConfig, EnvironmentConfig, UnityCatalogConfig

        ec = EnvironmentConfig(
            environment="test",
            unity_catalog=UnityCatalogConfig(catalog="dg_test"),
        )
        assert isinstance(ec.contract_validation, ContractEnvConfig)
        assert ec.contract_validation.default_contract_policy == "FAIL_PIPELINE"


# ===========================================================================
# ContractHistoryWriter — disabled mode (no Delta)
# ===========================================================================


class TestContractHistoryWriterDisabled:
    def test_write_does_nothing_when_disabled(self, spark):
        from src.audit.contract_history_writer import ContractHistoryWriter
        from src.contracts.contract_model import ContractValidationResult

        writer = ContractHistoryWriter(catalog="dg_test", enabled=False)
        result = ContractValidationResult(
            source_name="s", contract_name="c", contract_version="1.0",
            validation_policy="FAIL_PIPELINE", is_valid=True, can_proceed=True,
            rules_passed=3, rules_failed=0, warnings=0,
            broken_rules=[], all_rules=[], message="ok",
        )
        # Should not raise even though there is no Delta catalog
        writer.write(run_id="r1", source_name="s", contract_result=result, spark=spark)

    def test_writer_is_enabled_by_default(self):
        from src.audit.contract_history_writer import ContractHistoryWriter

        w = ContractHistoryWriter(catalog="dg_test")
        assert w._enabled is True


# ===========================================================================
# PipelineRun / PipelineSummary — contract field pass-through
# ===========================================================================


class TestPipelineRunContractFields:
    def test_pipeline_run_default_contract_fields(self):
        from datetime import datetime, timezone
        from src.common.pipeline_run import PipelineRun

        run = PipelineRun(
            run_id="r1",
            pipeline_name="dg",
            pipeline_version="1.0",
            source_name="x",
            environment="test",
            notebook_name="nb",
            cluster_id="c1",
            start_time=datetime.now(tz=timezone.utc),
        )
        assert run.contract_name == ""
        assert run.contract_version == ""
        assert run.contract_status == ""
        assert run.contract_rules_passed == 0
        assert run.contract_rules_failed == 0
        assert run.contract_warnings == 0

    def test_pipeline_summary_from_run_carries_contract_fields(self):
        from datetime import datetime, timezone
        from src.common.pipeline_run import PipelineRun, PipelineSummary

        run = PipelineRun(
            run_id="r1",
            pipeline_name="dg",
            pipeline_version="1.0",
            source_name="x",
            environment="test",
            notebook_name="nb",
            cluster_id="c1",
            start_time=datetime.now(tz=timezone.utc),
        )
        run.end_time = datetime.now(tz=timezone.utc)
        run.status = "SUCCESS"
        run.contract_name = "my_contract"
        run.contract_version = "2.0.0"
        run.contract_status = "PASSED"
        run.contract_rules_passed = 7
        run.contract_rules_failed = 0
        run.contract_warnings = 1

        summary = PipelineSummary.from_run(run)
        assert summary.contract_name == "my_contract"
        assert summary.contract_version == "2.0.0"
        assert summary.contract_status == "PASSED"
        assert summary.contract_rules_passed == 7
        assert summary.contract_rules_failed == 0
        assert summary.contract_warnings == 1

    def test_pipeline_summary_to_dict_contains_contract_keys(self):
        from datetime import datetime, timezone
        from src.common.pipeline_run import PipelineRun, PipelineSummary

        run = PipelineRun(
            run_id="r1",
            pipeline_name="dg",
            pipeline_version="1.0",
            source_name="x",
            environment="test",
            notebook_name="nb",
            cluster_id="c1",
            start_time=datetime.now(tz=timezone.utc),
        )
        run.end_time = datetime.now(tz=timezone.utc)
        run.status = "SUCCESS"
        summary = PipelineSummary.from_run(run)
        d = summary.to_dict()
        for key in (
            "contract_name", "contract_version", "contract_status",
            "contract_rules_passed", "contract_rules_failed", "contract_warnings",
        ):
            assert key in d
