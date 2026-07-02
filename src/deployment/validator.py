"""
DeploymentValidator — Milestone 8.

Verifies that the target Databricks environment is ready for DataGuardian
before the first pipeline run.  Returns a structured report; never raises
so the ops notebook can display partial results and still reach the summary.

Checks performed
----------------
error-severity (block deployment if failing):
    catalog_exists        — Unity Catalog is accessible
    package_installed     — DataGuardian package is importable
    env_config_catalog    — Catalog name is set in the environment YAML

warning-severity (log; do not block):
    schema_{name}         — bronze / silver / audit schemas exist
    env_config_adls       — ADLS root is configured

info-severity (informational only):
    audit_table_{name}    — each audit Delta table exists (auto-created on first run)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.common.models import EnvironmentConfig

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"

_REQUIRED_SCHEMAS: tuple[str, ...] = ("bronze", "silver", "audit")

_REQUIRED_AUDIT_TABLES: tuple[str, ...] = (
    "pipeline_run_history",
    "rule_execution_history",
    "dq_metrics",
    "schema_registry",
    "schema_history",
    "transformation_history",
    "contract_history",
    "dq_violations",
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ValidationCheck:
    """Result of a single deployment validation check."""

    name: str
    passed: bool
    message: str
    severity: str = SEVERITY_ERROR

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "passed": self.passed,
            "message": self.message,
            "severity": self.severity,
        }


@dataclass
class DeploymentValidationReport:
    """Aggregated result of all deployment checks for one environment."""

    environment: str
    catalog: str
    checks: list[ValidationCheck] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """True when no *error*-severity check has failed."""
        return all(c.passed for c in self.checks if c.severity == SEVERITY_ERROR)

    @property
    def error_count(self) -> int:
        return sum(1 for c in self.checks if not c.passed and c.severity == SEVERITY_ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for c in self.checks if not c.passed and c.severity == SEVERITY_WARNING)

    @property
    def info_count(self) -> int:
        return sum(1 for c in self.checks if not c.passed and c.severity == SEVERITY_INFO)

    def to_dict(self) -> dict[str, object]:
        return {
            "environment": self.environment,
            "catalog": self.catalog,
            "passed": self.passed,
            "total_checks": len(self.checks),
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "checks": [c.to_dict() for c in self.checks],
        }

    def print_report(self) -> None:
        status = "PASSED" if self.passed else "FAILED"
        print(f"\n{'=' * 65}")
        print(f"  DataGuardian Deployment Validation — {self.environment.upper()} [{status}]")
        print(f"  Catalog : {self.catalog}")
        print(
            f"  Checks  : {len(self.checks)} total | "
            f"{self.error_count} errors | "
            f"{self.warning_count} warnings | "
            f"{self.info_count} info"
        )
        print("=" * 65)

        _icons = {SEVERITY_ERROR: "✗", SEVERITY_WARNING: "!", SEVERITY_INFO: "i"}
        _pass_icon = "✓"

        for check in self.checks:
            icon = _pass_icon if check.passed else _icons.get(check.severity, "?")
            label = f"[{check.severity.upper():7s}]"
            print(f"  {icon} {label} {check.name}: {check.message}")

        print("=" * 65)


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


class DeploymentValidator:
    """
    Validates the target Databricks environment before DataGuardian deployment.

    Usage::

        validator = DeploymentValidator()
        report = validator.validate(spark, catalog, env_config, environment="prod")
        report.print_report()
        if not report.passed:
            raise RuntimeError("Deployment validation failed")
    """

    def validate(
        self,
        spark: SparkSession,
        catalog: str,
        env_config: EnvironmentConfig,
        environment: str,
    ) -> DeploymentValidationReport:
        """Run all validation checks and return a structured report."""
        report = DeploymentValidationReport(environment=environment, catalog=catalog)

        report.checks.extend(self._check_catalog(spark, catalog))
        report.checks.extend(self._check_schemas(spark, catalog))
        report.checks.extend(self._check_audit_tables(spark, catalog))
        report.checks.extend(self._check_package_install())
        report.checks.extend(self._check_env_config(env_config))

        return report

    # ------------------------------------------------------------------
    # Individual check methods
    # ------------------------------------------------------------------

    def _check_catalog(
        self, spark: SparkSession, catalog: str
    ) -> list[ValidationCheck]:
        try:
            spark.sql(f"USE CATALOG `{catalog}`")
            return [
                ValidationCheck(
                    name="catalog_exists",
                    passed=True,
                    message=f"Unity Catalog '{catalog}' is accessible",
                    severity=SEVERITY_ERROR,
                )
            ]
        except Exception as exc:  # noqa: BLE001
            return [
                ValidationCheck(
                    name="catalog_exists",
                    passed=False,
                    message=f"Unity Catalog '{catalog}' is not accessible: {exc}",
                    severity=SEVERITY_ERROR,
                )
            ]

    def _check_schemas(
        self, spark: SparkSession, catalog: str
    ) -> list[ValidationCheck]:
        checks: list[ValidationCheck] = []
        try:
            existing = {
                row[0] for row in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
            }
        except Exception as exc:  # noqa: BLE001
            for name in _REQUIRED_SCHEMAS:
                checks.append(
                    ValidationCheck(
                        name=f"schema_{name}",
                        passed=False,
                        message=f"Could not enumerate schemas in '{catalog}': {exc}",
                        severity=SEVERITY_WARNING,
                    )
                )
            return checks

        for name in _REQUIRED_SCHEMAS:
            exists = name in existing
            checks.append(
                ValidationCheck(
                    name=f"schema_{name}",
                    passed=exists,
                    message=(
                        f"Schema '{catalog}.{name}' exists"
                        if exists
                        else (
                            f"Schema '{catalog}.{name}' not found — "
                            "will be created automatically on first pipeline run"
                        )
                    ),
                    severity=SEVERITY_WARNING,
                )
            )
        return checks

    def _check_audit_tables(
        self, spark: SparkSession, catalog: str
    ) -> list[ValidationCheck]:
        checks: list[ValidationCheck] = []
        for table in _REQUIRED_AUDIT_TABLES:
            full_name = f"`{catalog}`.`audit`.`{table}`"
            try:
                spark.sql(f"DESCRIBE TABLE {full_name}")
                checks.append(
                    ValidationCheck(
                        name=f"audit_table_{table}",
                        passed=True,
                        message=f"Audit table '{catalog}.audit.{table}' exists",
                        severity=SEVERITY_INFO,
                    )
                )
            except Exception:  # noqa: BLE001
                checks.append(
                    ValidationCheck(
                        name=f"audit_table_{table}",
                        passed=False,
                        message=(
                            f"Audit table '{catalog}.audit.{table}' not found — "
                            "will be created on first pipeline run"
                        ),
                        severity=SEVERITY_INFO,
                    )
                )
        return checks

    def _check_package_install(self) -> list[ValidationCheck]:
        try:
            import src  # noqa: F401

            return [
                ValidationCheck(
                    name="package_installed",
                    passed=True,
                    message="DataGuardian package (src) is importable",
                    severity=SEVERITY_ERROR,
                )
            ]
        except ImportError as exc:
            return [
                ValidationCheck(
                    name="package_installed",
                    passed=False,
                    message=f"DataGuardian package import failed: {exc}",
                    severity=SEVERITY_ERROR,
                )
            ]

    def _check_env_config(
        self, env_config: EnvironmentConfig
    ) -> list[ValidationCheck]:
        checks: list[ValidationCheck] = []

        catalog_name = env_config.unity_catalog.catalog
        checks.append(
            ValidationCheck(
                name="env_config_catalog",
                passed=bool(catalog_name),
                message=(
                    f"Unity Catalog configured: '{catalog_name}'"
                    if catalog_name
                    else "unity_catalog.catalog is empty in the environment YAML"
                ),
                severity=SEVERITY_ERROR,
            )
        )

        adls_root = env_config.storage.adls_root
        checks.append(
            ValidationCheck(
                name="env_config_adls",
                passed=bool(adls_root),
                message=(
                    f"ADLS root configured: '{adls_root}'"
                    if adls_root
                    else "storage.adls_root is not set in the environment YAML"
                ),
                severity=SEVERITY_WARNING,
            )
        )

        return checks
