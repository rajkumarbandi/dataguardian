"""
Contract Validation Engine — evaluates a source's DataContract at runtime.

``ContractValidationEngine.validate()`` receives all pipeline stage outputs
(schema result, transformation result, DQ result, post-transformation DataFrame)
and evaluates every rule declared in ``source_config.contract``.

Design decisions
----------------
- **No new Spark actions.** The engine uses ``df.columns`` and ``df.schema``
  (metadata, no shuffle) for structural checks.  Row counts are passed in as
  ``row_count`` (already computed earlier in the pipeline) to avoid a second
  full scan.  Null checks use DQ rule execution results rather than
  ``df.filter(col.isNull()).count()``.
- **TYPE_CHECKING imports only** for PySpark and pipeline result types — the
  module is importable in environments where PySpark is not installed (useful
  for CI linting and unit tests that mock the types).
- **Policy-aware can_proceed.** The engine evaluates all rules regardless of
  policy.  ``can_proceed`` is derived from ``is_valid`` and ``validation_policy``
  so the notebook has a single flag to act on.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from src.contracts.contract_model import (
    CATEGORY_BUSINESS,
    CATEGORY_GOVERNANCE,
    CATEGORY_QUALITY,
    CATEGORY_STRUCTURE,
    SEVERITY_ERROR,
    SEVERITY_WARNING,
    STATUS_SKIPPED,
    ContractRuleResult,
    ContractValidationResult,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from src.common.models import SourceConfig
    from src.schema.schema_validator import SchemaValidationResult
    from src.transformations.results import TransformationRunResult

logger = logging.getLogger(__name__)

_POLICY_VALUES = frozenset({"FAIL_PIPELINE", "WARNING_ONLY", "IGNORE"})

# Spark typeName() aliases — maps known user-facing names to Spark internal names
_TYPE_ALIAS_GROUPS: list[frozenset[str]] = [
    frozenset({"integer", "int", "int32"}),
    frozenset({"long", "bigint", "int64"}),
    frozenset({"double", "float64"}),
    frozenset({"float", "real", "float32"}),
    frozenset({"string", "str", "varchar", "text"}),
    frozenset({"boolean", "bool"}),
    frozenset({"decimal", "numeric"}),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _types_compatible(actual: str, expected: str) -> bool:
    """Return True when ``actual`` and ``expected`` Spark type names are equivalent."""
    a, e = actual.lower().strip(), expected.lower().strip()
    if a == e:
        return True
    for group in _TYPE_ALIAS_GROUPS:
        if a in group and e in group:
            return True
    # decimal(p,s) → starts with "decimal"
    if a.startswith("decimal") and e.startswith("decimal"):
        return True
    return False


def _dq_violations(dq_result: Any, rule_name: str, column_name: str) -> int | None:
    """Return the violation count for a specific (rule, column) from a DQ result.

    Returns ``None`` when the rule was never executed or when ``dq_result`` is
    ``None``.  Uses ``getattr`` throughout to remain decoupled from the internal
    ``RuleMetric`` class.
    """
    if dq_result is None:
        return None
    for metric in getattr(dq_result, "rule_metrics", []):
        r = getattr(metric, "rule_name", None)
        c = getattr(metric, "column_name", None)
        if r == rule_name and c == column_name:
            return getattr(metric, "violations", None)
    return None


# ---------------------------------------------------------------------------
# ContractValidationEngine
# ---------------------------------------------------------------------------


class ContractValidationEngine:
    """
    Evaluates a ``DataContract`` against all outputs from the current pipeline run.

    All rule checks are ordered from structural (cheapest) to governance (data-
    level).  The engine never raises — it always returns a ``ContractValidationResult``
    with ``can_proceed=False`` when the validation should block the pipeline.

    Parameters
    ----------
    logger:
        Optional pre-bound logger; falls back to module-level logger.
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._log = logger or logging.getLogger("dataguardian.contract.engine")

    def validate(
        self,
        source_config: SourceConfig,
        schema_result: SchemaValidationResult | None,
        dq_result: Any,
        transformation_result: TransformationRunResult | None,
        df: DataFrame,
        row_count: int,
        run_id: str = "",
        env_policy: str = "FAIL_PIPELINE",
    ) -> ContractValidationResult:
        """
        Evaluate all contract rules and return a ``ContractValidationResult``.

        Parameters
        ----------
        source_config:
            Parsed source YAML — provides the ``contract:`` section.
        schema_result:
            Output from ``SchemaValidator.validate()`` — provides schema version.
        dq_result:
            Output from ``DataQualityEngine.run()`` — provides rule execution info.
        transformation_result:
            Output from ``TransformationEngine.run()`` — currently unused in rule
            evaluation but available for future governance rules.
        df:
            Post-transformation DataFrame.  Only metadata attributes
            (``df.columns``, ``df.schema``) are accessed — no new Spark actions.
        row_count:
            Pre-computed row count of ``df`` (from ``dq_result.rows_read`` or
            passed explicitly).  Avoids an extra ``df.count()`` call.
        run_id:
            Pipeline run ID for logging correlation.
        env_policy:
            Default policy from ``env_config.contract_validation.default_contract_policy``.
            Overridden by ``source_config.contract.validation_policy`` when set.
        """
        contract = source_config.contract if source_config is not None else None

        if contract is None:
            return self._skipped_result(source_name=source_config.name if source_config else "")

        # Resolve effective policy: source overrides env
        effective_policy = (contract.validation_policy or env_policy).upper()
        if effective_policy not in _POLICY_VALUES:
            self._log.warning(
                "Unknown contract validation_policy '%s' — defaulting to FAIL_PIPELINE",
                effective_policy,
            )
            effective_policy = "FAIL_PIPELINE"

        if effective_policy == "IGNORE":
            self._log.debug(
                "Contract validation policy is IGNORE — skipping all rule checks",
                source_name=source_config.name,
                contract_name=contract.name,
            )
            return self._skipped_result(
                source_name=source_config.name,
                contract_name=contract.name,
                contract_version=contract.version,
                policy=effective_policy,
            )

        self._log.info(
            "Contract validation starting",
            source_name=source_config.name,
            run_id=run_id,
            contract_name=contract.name,
            contract_version=contract.version,
            effective_policy=effective_policy,
        )

        all_rules = self._evaluate_all_rules(
            contract=contract,
            source_config=source_config,
            schema_result=schema_result,
            dq_result=dq_result,
            df=df,
            row_count=row_count,
        )

        broken_rules = [r for r in all_rules if not r.passed]
        error_failures = [r for r in broken_rules if r.severity == SEVERITY_ERROR]
        warning_failures = [r for r in broken_rules if r.severity == SEVERITY_WARNING]

        rules_passed = len([r for r in all_rules if r.passed])
        rules_failed = len(error_failures)
        warnings = len(warning_failures)

        is_valid = len(error_failures) == 0
        can_proceed = is_valid or effective_policy == "WARNING_ONLY"

        if is_valid:
            message = f"All {rules_passed} contract rule(s) passed."
        else:
            broken_names = ", ".join(
                f"'{r.rule_name}'" for r in error_failures[:5]
            )
            suffix = f" and {len(error_failures) - 5} more" if len(error_failures) > 5 else ""
            message = (
                f"{rules_failed} contract rule(s) failed: {broken_names}{suffix}. "
                f"Policy: {effective_policy}."
            )

        self._log.info(
            "Contract validation complete",
            source_name=source_config.name,
            run_id=run_id,
            contract_name=contract.name,
            contract_version=contract.version,
            is_valid=is_valid,
            can_proceed=can_proceed,
            rules_passed=rules_passed,
            rules_failed=rules_failed,
            warnings=warnings,
            effective_policy=effective_policy,
        )

        return ContractValidationResult(
            source_name=source_config.name,
            contract_name=contract.name,
            contract_version=contract.version,
            validation_policy=effective_policy,
            is_valid=is_valid,
            can_proceed=can_proceed,
            rules_passed=rules_passed,
            rules_failed=rules_failed,
            warnings=warnings,
            broken_rules=broken_rules,
            all_rules=all_rules,
            message=message,
        )

    # ------------------------------------------------------------------
    # Rule evaluation
    # ------------------------------------------------------------------

    def _evaluate_all_rules(
        self,
        contract: Any,
        source_config: SourceConfig,
        schema_result: Any,
        dq_result: Any,
        df: DataFrame,
        row_count: int,
    ) -> list[ContractRuleResult]:
        """Evaluate every active contract rule and return all results."""
        rules: list[ContractRuleResult] = []

        # ── Structure rules ─────────────────────────────────────────────
        rules.extend(self._check_required_columns(contract, df))
        rules.extend(self._check_allowed_datatypes(contract, df))
        rules.extend(self._check_primary_keys(contract, source_config, dq_result, df))
        rules.extend(self._check_non_nullable_columns(contract, source_config, dq_result, df))

        # ── Business rules ───────────────────────────────────────────────
        rules.extend(self._check_row_count(contract, row_count))

        # ── Quality rules ────────────────────────────────────────────────
        rules.extend(self._check_required_dq_rules(contract, source_config))

        # ── Governance rules ─────────────────────────────────────────────
        rules.extend(self._check_schema_version_min(contract, schema_result))

        return rules

    # ── Structure ──────────────────────────────────────────────────────

    def _check_required_columns(
        self, contract: Any, df: DataFrame
    ) -> list[ContractRuleResult]:
        results = []
        for col in getattr(contract, "required_columns", []):
            passed = col in df.columns
            results.append(ContractRuleResult(
                rule_name="required_columns",
                passed=passed,
                severity=SEVERITY_ERROR,
                category=CATEGORY_STRUCTURE,
                message=(
                    f"Required column '{col}' is present."
                    if passed else
                    f"Required column '{col}' is missing from the DataFrame."
                ),
                column=col,
            ))
        return results

    def _check_allowed_datatypes(
        self, contract: Any, df: DataFrame
    ) -> list[ContractRuleResult]:
        results = []
        type_map = {f.name: f.dataType.typeName() for f in df.schema.fields}
        for col, expected_type in getattr(contract, "allowed_datatypes", {}).items():
            if col not in df.columns:
                results.append(ContractRuleResult(
                    rule_name="allowed_datatypes",
                    passed=False,
                    severity=SEVERITY_ERROR,
                    category=CATEGORY_STRUCTURE,
                    message=f"Column '{col}' not found — cannot validate type.",
                    column=col,
                ))
                continue
            actual_type = type_map.get(col, "unknown")
            passed = _types_compatible(actual_type, expected_type)
            results.append(ContractRuleResult(
                rule_name="allowed_datatypes",
                passed=passed,
                severity=SEVERITY_ERROR,
                category=CATEGORY_STRUCTURE,
                message=(
                    f"Column '{col}': type is '{actual_type}' (expected '{expected_type}')."
                    if not passed else
                    f"Column '{col}': type '{actual_type}' matches expected '{expected_type}'."
                ),
                column=col,
            ))
        return results

    def _check_primary_keys(
        self,
        contract: Any,
        source_config: SourceConfig,
        dq_result: Any,
        df: DataFrame,
    ) -> list[ContractRuleResult]:
        results = []
        configured_dq = {(r.rule, r.column) for r in source_config.dq_rules}
        for pk in getattr(contract, "primary_keys", []):
            col_exists = pk in df.columns
            if not col_exists:
                results.append(ContractRuleResult(
                    rule_name="primary_keys",
                    passed=False,
                    severity=SEVERITY_ERROR,
                    category=CATEGORY_STRUCTURE,
                    message=f"Primary key column '{pk}' is missing from the schema.",
                    column=pk,
                ))
                continue
            has_not_null = ("not_null", pk) in configured_dq
            has_unique = ("unique", pk) in configured_dq
            passed = has_not_null and has_unique
            if passed:
                msg = f"Primary key '{pk}' is backed by not_null and unique DQ rules."
            elif not has_not_null and not has_unique:
                msg = f"Primary key '{pk}' lacks both not_null and unique DQ rules."
            elif not has_not_null:
                msg = f"Primary key '{pk}' has unique but no not_null DQ rule."
            else:
                msg = f"Primary key '{pk}' has not_null but no unique DQ rule."
            results.append(ContractRuleResult(
                rule_name="primary_keys",
                passed=passed,
                severity=SEVERITY_ERROR,
                category=CATEGORY_STRUCTURE,
                message=msg,
                column=pk,
            ))
        return results

    def _check_non_nullable_columns(
        self,
        contract: Any,
        source_config: SourceConfig,
        dq_result: Any,
        df: DataFrame,
    ) -> list[ContractRuleResult]:
        results = []
        configured_dq = {(r.rule, r.column) for r in source_config.dq_rules}
        for col in getattr(contract, "non_nullable_columns", []):
            if col not in df.columns:
                results.append(ContractRuleResult(
                    rule_name="non_nullable_columns",
                    passed=False,
                    severity=SEVERITY_ERROR,
                    category=CATEGORY_STRUCTURE,
                    message=f"Non-nullable column '{col}' is missing from the schema.",
                    column=col,
                ))
                continue

            has_not_null_rule = ("not_null", col) in configured_dq

            if has_not_null_rule:
                # Check DQ execution result for actual violations
                violations = _dq_violations(dq_result, "not_null", col)
                if violations is None:
                    # Rule configured but DQ result unavailable — pass with caveat
                    passed, msg = True, f"Column '{col}' has not_null DQ rule (violations not yet available)."
                elif violations == 0:
                    passed, msg = True, f"Column '{col}' has not_null DQ rule and zero violations."
                else:
                    passed, msg = False, f"Column '{col}' has {violations} null value(s) detected by not_null DQ rule."
            else:
                # Fall back to schema nullability flag (weak check — warns only)
                field = next((f for f in df.schema.fields if f.name == col), None)
                schema_not_null = field is not None and not field.nullable
                passed = schema_not_null
                if passed:
                    msg = f"Column '{col}' is declared non-nullable in schema (no DQ rule enforcing)."
                else:
                    msg = f"Column '{col}' is nullable in schema and has no not_null DQ rule."

            results.append(ContractRuleResult(
                rule_name="non_nullable_columns",
                passed=passed,
                severity=SEVERITY_ERROR if has_not_null_rule else SEVERITY_WARNING,
                category=CATEGORY_STRUCTURE,
                message=msg,
                column=col,
            ))
        return results

    # ── Business ────────────────────────────────────────────────────────

    def _check_row_count(
        self, contract: Any, row_count: int
    ) -> list[ContractRuleResult]:
        row_count_cfg = getattr(contract, "row_count", None)
        if row_count_cfg is None:
            return []

        min_rows = getattr(row_count_cfg, "min", None)
        max_rows = getattr(row_count_cfg, "max", None)

        if min_rows is None and max_rows is None:
            return []

        if min_rows is not None and row_count < min_rows:
            return [ContractRuleResult(
                rule_name="row_count",
                passed=False,
                severity=SEVERITY_ERROR,
                category=CATEGORY_BUSINESS,
                message=(
                    f"Row count {row_count:,} is below the contract minimum of {min_rows:,}."
                ),
            )]

        if max_rows is not None and row_count > max_rows:
            return [ContractRuleResult(
                rule_name="row_count",
                passed=False,
                severity=SEVERITY_WARNING,
                category=CATEGORY_BUSINESS,
                message=(
                    f"Row count {row_count:,} exceeds the contract maximum of {max_rows:,}."
                ),
            )]

        bounds = []
        if min_rows is not None:
            bounds.append(f"min={min_rows:,}")
        if max_rows is not None:
            bounds.append(f"max={max_rows:,}")

        return [ContractRuleResult(
            rule_name="row_count",
            passed=True,
            severity=SEVERITY_ERROR,
            category=CATEGORY_BUSINESS,
            message=f"Row count {row_count:,} is within expected range ({', '.join(bounds)}).",
        )]

    # ── Quality ─────────────────────────────────────────────────────────

    def _check_required_dq_rules(
        self, contract: Any, source_config: SourceConfig
    ) -> list[ContractRuleResult]:
        results = []
        configured_rule_types = {r.rule for r in source_config.dq_rules}
        for required_rule in getattr(contract, "required_dq_rules", []):
            passed = required_rule in configured_rule_types
            results.append(ContractRuleResult(
                rule_name="required_dq_rules",
                passed=passed,
                severity=SEVERITY_ERROR,
                category=CATEGORY_QUALITY,
                message=(
                    f"Required DQ rule '{required_rule}' is configured."
                    if passed else
                    f"Required DQ rule '{required_rule}' is NOT configured in dq_rules."
                ),
            ))
        return results

    # ── Governance ──────────────────────────────────────────────────────

    def _check_schema_version_min(
        self, contract: Any, schema_result: Any
    ) -> list[ContractRuleResult]:
        schema_version_min = getattr(contract, "schema_version_min", None)
        if schema_version_min is None:
            return []

        actual_version = getattr(schema_result, "schema_version", 0) if schema_result else 0
        passed = actual_version >= schema_version_min
        return [ContractRuleResult(
            rule_name="schema_version_min",
            passed=passed,
            severity=SEVERITY_ERROR,
            category=CATEGORY_GOVERNANCE,
            message=(
                f"Schema version {actual_version} meets the minimum of {schema_version_min}."
                if passed else
                f"Schema version {actual_version} is below the contract minimum of {schema_version_min}."
            ),
        )]

    # ------------------------------------------------------------------
    # No-contract sentinel result
    # ------------------------------------------------------------------

    @staticmethod
    def _skipped_result(
        source_name: str = "",
        contract_name: str = "none",
        contract_version: str = "",
        policy: str = "IGNORE",
    ) -> ContractValidationResult:
        return ContractValidationResult(
            source_name=source_name,
            contract_name=contract_name,
            contract_version=contract_version,
            validation_policy=policy,
            is_valid=True,
            can_proceed=True,
            rules_passed=0,
            rules_failed=0,
            warnings=0,
            broken_rules=[],
            all_rules=[],
            message="Contract validation skipped — no contract defined or policy is IGNORE.",
        )
