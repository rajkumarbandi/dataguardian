"""
Unit tests for DeploymentValidator (Milestone 8).

Spark is mocked — no cluster required.
The validator is tested with controlled success/failure responses from Spark SQL.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.deployment.validator import (
    SEVERITY_ERROR,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    DeploymentValidationReport,
    DeploymentValidator,
    ValidationCheck,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_env_config(catalog: str = "dg_test", adls_root: str = "abfss://...") -> MagicMock:
    cfg = MagicMock()
    cfg.unity_catalog.catalog = catalog
    cfg.storage.adls_root = adls_root
    return cfg


def _make_spark(sql_results: dict[str, object] | None = None, raise_on: list[str] | None = None) -> MagicMock:
    """
    Build a mock SparkSession whose sql() either returns rows or raises.

    sql_results: mapping of SQL-keyword → list of rows (each row is a MagicMock with [0])
    raise_on:    list of SQL keywords that trigger an Exception when matched
    """
    spark = MagicMock()
    sql_results = sql_results or {}
    raise_on = raise_on or []

    def _sql(query: str):
        for keyword in raise_on:
            if keyword.upper() in query.upper():
                raise Exception(f"Simulated SQL error for '{keyword}'")
        for keyword, rows in sql_results.items():
            if keyword.upper() in query.upper():
                result = MagicMock()
                result.collect.return_value = rows
                return result
        result = MagicMock()
        result.collect.return_value = []
        return result

    spark.sql.side_effect = _sql
    return spark


# ---------------------------------------------------------------------------
# ValidationCheck
# ---------------------------------------------------------------------------


class TestValidationCheck:
    def test_to_dict_keys(self):
        check = ValidationCheck(
            name="catalog_exists", passed=True, message="ok", severity=SEVERITY_ERROR
        )
        d = check.to_dict()
        assert set(d.keys()) == {"name", "passed", "message", "severity"}

    def test_to_dict_values(self):
        check = ValidationCheck(
            name="schema_audit", passed=False, message="missing", severity=SEVERITY_WARNING
        )
        d = check.to_dict()
        assert d["passed"] is False
        assert d["severity"] == SEVERITY_WARNING


# ---------------------------------------------------------------------------
# DeploymentValidationReport
# ---------------------------------------------------------------------------


class TestDeploymentValidationReport:
    def _report(self, checks: list[ValidationCheck]) -> DeploymentValidationReport:
        r = DeploymentValidationReport(environment="test", catalog="dg_test")
        r.checks = checks
        return r

    def test_passed_when_all_errors_pass(self):
        r = self._report([
            ValidationCheck("a", True, "ok", SEVERITY_ERROR),
            ValidationCheck("b", False, "warn", SEVERITY_WARNING),
        ])
        assert r.passed is True

    def test_failed_when_any_error_fails(self):
        r = self._report([
            ValidationCheck("a", False, "err", SEVERITY_ERROR),
        ])
        assert r.passed is False

    def test_error_count(self):
        r = self._report([
            ValidationCheck("a", False, "", SEVERITY_ERROR),
            ValidationCheck("b", False, "", SEVERITY_ERROR),
            ValidationCheck("c", True, "", SEVERITY_ERROR),
        ])
        assert r.error_count == 2

    def test_warning_count(self):
        r = self._report([
            ValidationCheck("a", False, "", SEVERITY_WARNING),
            ValidationCheck("b", True, "", SEVERITY_WARNING),
        ])
        assert r.warning_count == 1

    def test_info_count(self):
        r = self._report([
            ValidationCheck("a", False, "", SEVERITY_INFO),
        ])
        assert r.info_count == 1

    def test_to_dict_contains_required_fields(self):
        r = self._report([])
        d = r.to_dict()
        for key in ("environment", "catalog", "passed", "total_checks",
                    "error_count", "warning_count", "info_count", "checks"):
            assert key in d

    def test_print_report_does_not_raise(self, capsys):
        r = self._report([
            ValidationCheck("catalog_exists", True, "ok", SEVERITY_ERROR),
            ValidationCheck("schema_audit", False, "missing", SEVERITY_WARNING),
        ])
        r.print_report()
        captured = capsys.readouterr()
        assert "PASSED" in captured.out or "FAILED" in captured.out


# ---------------------------------------------------------------------------
# DeploymentValidator — catalog check
# ---------------------------------------------------------------------------


class TestCatalogCheck:
    def test_catalog_accessible_returns_passed(self):
        spark = _make_spark()  # sql() doesn't raise
        validator = DeploymentValidator()
        checks = validator._check_catalog(spark, "dg_test")
        assert len(checks) == 1
        assert checks[0].passed is True
        assert checks[0].severity == SEVERITY_ERROR

    def test_catalog_inaccessible_returns_failed(self):
        spark = _make_spark(raise_on=["USE CATALOG"])
        validator = DeploymentValidator()
        checks = validator._check_catalog(spark, "dg_missing")
        assert checks[0].passed is False
        assert checks[0].severity == SEVERITY_ERROR
        assert "dg_missing" in checks[0].message


# ---------------------------------------------------------------------------
# DeploymentValidator — schema checks
# ---------------------------------------------------------------------------


class TestSchemaChecks:
    def _row(self, name: str) -> MagicMock:
        row = MagicMock()
        row.__getitem__ = lambda self, idx: name  # row[0] returns name
        return row

    def test_existing_schema_passes(self):
        rows = [self._row("bronze"), self._row("silver"), self._row("audit")]
        spark = _make_spark(sql_results={"SHOW SCHEMAS": rows})
        validator = DeploymentValidator()
        checks = validator._check_schemas(spark, "dg_test")
        # All 3 required schemas are in the result
        assert all(c.passed for c in checks)

    def test_missing_schema_returns_warning(self):
        rows = [self._row("bronze")]  # silver and audit missing
        spark = _make_spark(sql_results={"SHOW SCHEMAS": rows})
        validator = DeploymentValidator()
        checks = validator._check_schemas(spark, "dg_test")
        failed = [c for c in checks if not c.passed]
        assert len(failed) == 2
        assert all(c.severity == SEVERITY_WARNING for c in failed)

    def test_sql_error_produces_warning_checks(self):
        spark = _make_spark(raise_on=["SHOW SCHEMAS"])
        validator = DeploymentValidator()
        checks = validator._check_schemas(spark, "dg_test")
        assert all(c.severity == SEVERITY_WARNING for c in checks)
        assert all(not c.passed for c in checks)


# ---------------------------------------------------------------------------
# DeploymentValidator — audit table checks
# ---------------------------------------------------------------------------


class TestAuditTableChecks:
    def test_existing_table_produces_info_passed(self):
        spark = _make_spark()  # DESCRIBE doesn't raise
        validator = DeploymentValidator()
        checks = validator._check_audit_tables(spark, "dg_test")
        assert all(c.passed for c in checks)
        assert all(c.severity == SEVERITY_INFO for c in checks)

    def test_missing_table_produces_info_failed(self):
        spark = _make_spark(raise_on=["DESCRIBE TABLE"])
        validator = DeploymentValidator()
        checks = validator._check_audit_tables(spark, "dg_test")
        assert all(not c.passed for c in checks)
        assert all(c.severity == SEVERITY_INFO for c in checks)

    def test_number_of_audit_checks(self):
        from src.deployment.validator import _REQUIRED_AUDIT_TABLES
        spark = _make_spark()
        validator = DeploymentValidator()
        checks = validator._check_audit_tables(spark, "dg_test")
        assert len(checks) == len(_REQUIRED_AUDIT_TABLES)


# ---------------------------------------------------------------------------
# DeploymentValidator — package install check
# ---------------------------------------------------------------------------


class TestPackageInstallCheck:
    def test_package_importable_passes(self):
        validator = DeploymentValidator()
        checks = validator._check_package_install()
        # src is on sys.path in tests — should pass
        assert checks[0].passed is True
        assert checks[0].severity == SEVERITY_ERROR

    def test_import_failure_returns_failed(self):
        validator = DeploymentValidator()
        with patch("builtins.__import__", side_effect=ImportError("no module")):
            checks = validator._check_package_install()
        assert checks[0].passed is False


# ---------------------------------------------------------------------------
# DeploymentValidator — env config checks
# ---------------------------------------------------------------------------


class TestEnvConfigChecks:
    def test_configured_catalog_and_adls_both_pass(self):
        env_config = _make_env_config(catalog="dg_prod", adls_root="abfss://container@account.dfs.core.windows.net")
        validator = DeploymentValidator()
        checks = validator._check_env_config(env_config)
        catalog_check = next(c for c in checks if c.name == "env_config_catalog")
        adls_check = next(c for c in checks if c.name == "env_config_adls")
        assert catalog_check.passed is True
        assert adls_check.passed is True

    def test_empty_catalog_fails_with_error(self):
        env_config = _make_env_config(catalog="")
        validator = DeploymentValidator()
        checks = validator._check_env_config(env_config)
        catalog_check = next(c for c in checks if c.name == "env_config_catalog")
        assert catalog_check.passed is False
        assert catalog_check.severity == SEVERITY_ERROR

    def test_empty_adls_root_warns(self):
        env_config = _make_env_config(adls_root="")
        validator = DeploymentValidator()
        checks = validator._check_env_config(env_config)
        adls_check = next(c for c in checks if c.name == "env_config_adls")
        assert adls_check.passed is False
        assert adls_check.severity == SEVERITY_WARNING


# ---------------------------------------------------------------------------
# DeploymentValidator — full validate()
# ---------------------------------------------------------------------------


class TestFullValidate:
    def test_validate_returns_report(self):
        spark = _make_spark()
        env_config = _make_env_config()
        validator = DeploymentValidator()
        report = validator.validate(
            spark=spark, catalog="dg_test", env_config=env_config, environment="test"
        )
        assert isinstance(report, DeploymentValidationReport)
        assert report.environment == "test"
        assert report.catalog == "dg_test"
        assert len(report.checks) > 0

    def test_report_has_all_check_categories(self):
        spark = _make_spark()
        env_config = _make_env_config()
        validator = DeploymentValidator()
        report = validator.validate(
            spark=spark, catalog="dg_test", env_config=env_config, environment="test"
        )
        names = {c.name for c in report.checks}
        assert "catalog_exists" in names
        assert "package_installed" in names
        assert "env_config_catalog" in names
        assert any(n.startswith("schema_") for n in names)
        assert any(n.startswith("audit_table_") for n in names)

    def test_catalog_failure_causes_report_failed(self):
        spark = _make_spark(raise_on=["USE CATALOG"])
        env_config = _make_env_config()
        validator = DeploymentValidator()
        report = validator.validate(
            spark=spark, catalog="bad_catalog", env_config=env_config, environment="test"
        )
        assert report.passed is False
        assert report.error_count >= 1
