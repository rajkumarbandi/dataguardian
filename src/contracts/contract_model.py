"""
Data Contract domain model — result types and rule results for M7.

``ContractRuleResult`` represents the outcome of evaluating one contract rule.
``ContractValidationResult`` is the complete output of ``ContractValidationEngine.validate()``.
Neither class depends on PySpark or Spark sessions — they are pure Python dataclasses
safe to instantiate anywhere, including unit tests and driver-side code.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Contract rule categories
# ---------------------------------------------------------------------------

CATEGORY_STRUCTURE = "structure"
CATEGORY_QUALITY = "quality"
CATEGORY_BUSINESS = "business"
CATEGORY_GOVERNANCE = "governance"

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"

STATUS_PASSED = "PASSED"
STATUS_FAILED = "FAILED"
STATUS_WARNING = "WARNING"
STATUS_SKIPPED = "SKIPPED"


# ---------------------------------------------------------------------------
# ContractRuleResult
# ---------------------------------------------------------------------------


@dataclass
class ContractRuleResult:
    """
    Outcome of evaluating a single contract rule.

    Attributes
    ----------
    rule_name:
        Machine-readable rule identifier (e.g. ``"required_columns"``,
        ``"primary_keys"``, ``"row_count"``).
    passed:
        ``True`` when the rule evaluation succeeded.
    severity:
        ``"error"`` — failure blocks the pipeline (when policy is ``FAIL_PIPELINE``).
        ``"warning"`` — failure is recorded but never blocks the pipeline.
    category:
        Logical grouping for reporting:
        ``"structure"`` | ``"quality"`` | ``"business"`` | ``"governance"``.
    message:
        Human-readable explanation of the outcome.
    column:
        Relevant column name when the rule targets a specific field; empty otherwise.
    """

    rule_name: str
    passed: bool
    severity: str
    category: str
    message: str
    column: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "passed": self.passed,
            "severity": self.severity,
            "category": self.category,
            "message": self.message,
            "column": self.column,
        }


# ---------------------------------------------------------------------------
# ContractValidationResult
# ---------------------------------------------------------------------------


@dataclass
class ContractValidationResult:
    """
    Full result of a contract validation run.

    Produced by ``ContractValidationEngine.validate()`` and consumed by the
    notebook, ``ContractHistoryWriter``, and ``PipelineRunTracker``.

    Attributes
    ----------
    source_name:
        Source identifier (matches YAML ``name:``).
    contract_name:
        Human-readable contract name.
    contract_version:
        Semantic version of the evaluated contract.
    validation_policy:
        Active policy: ``FAIL_PIPELINE`` | ``WARNING_ONLY`` | ``IGNORE``.
    is_valid:
        ``True`` when all *error-severity* rules passed (warnings do not affect validity).
    can_proceed:
        Derived from ``is_valid`` and ``validation_policy``.  When ``False`` the
        notebook raises ``PipelineExecutionException``.
    rules_passed:
        Count of rules that passed (both severity levels).
    rules_failed:
        Count of rules that failed with severity ``"error"``.
    warnings:
        Count of rules that failed with severity ``"warning"``.
    broken_rules:
        All rule results with ``passed=False``.
    all_rules:
        Every evaluated rule result (for debugging and audit).
    message:
        One-line summary of the validation outcome.
    """

    source_name: str
    contract_name: str
    contract_version: str
    validation_policy: str
    is_valid: bool
    can_proceed: bool
    rules_passed: int
    rules_failed: int
    warnings: int
    broken_rules: list[ContractRuleResult]
    all_rules: list[ContractRuleResult]
    message: str = ""

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    @property
    def status(self) -> str:
        """High-level status string: PASSED | FAILED | WARNING | SKIPPED."""
        if not self.all_rules:
            return STATUS_SKIPPED
        if self.is_valid:
            return STATUS_PASSED
        if self.validation_policy in {"WARNING_ONLY", "IGNORE"}:
            return STATUS_WARNING
        return STATUS_FAILED

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def broken_rules_json(self) -> str:
        """JSON string of all broken rules — stored in the audit table."""
        return json.dumps([r.to_dict() for r in self.broken_rules])

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "contract_name": self.contract_name,
            "contract_version": self.contract_version,
            "validation_policy": self.validation_policy,
            "status": self.status,
            "is_valid": self.is_valid,
            "can_proceed": self.can_proceed,
            "rules_passed": self.rules_passed,
            "rules_failed": self.rules_failed,
            "warnings": self.warnings,
            "message": self.message,
            "broken_rules": [r.to_dict() for r in self.broken_rules],
        }
