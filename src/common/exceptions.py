"""DataGuardian domain exception hierarchy."""

from __future__ import annotations


class DataGuardianError(Exception):
    """Base exception for all DataGuardian errors."""


class ConfigurationError(DataGuardianError):
    """Raised when a configuration file is invalid or missing required fields."""


class SchemaConformanceError(DataGuardianError):
    """Raised when a DataFrame does not conform to the canonical schema contract."""


class ConnectorError(DataGuardianError):
    """Raised when a source connector cannot read data from its target."""


class InvalidStateTransitionError(DataGuardianError):
    """Raised when an invalid stewardship approval state transition is attempted."""


class QualityRuleError(DataGuardianError):
    """Raised when a DQ rule fails to evaluate due to a configuration or runtime error."""


class AIEnrichmentError(DataGuardianError):
    """Raised by AI services — caught internally and never propagated to the pipeline."""


# ---------------------------------------------------------------------------
# Milestone 4 — Auditing and observability exceptions
# ---------------------------------------------------------------------------


class PipelineExecutionException(DataGuardianError):
    """
    Raised when a pipeline run cannot complete due to an unrecoverable error.

    Wraps lower-level exceptions (connector errors, DQ engine failures, write
    failures) after all retry attempts have been exhausted.  Carries the
    original cause via ``__cause__`` so the full traceback is preserved.
    """


class RuleExecutionException(DataGuardianError):
    """
    Raised when a specific DQ rule fails to apply due to a runtime error.

    Distinct from ``QualityRuleError`` (configuration error) — this covers
    Spark execution errors (missing columns, cast failures, window function
    issues) encountered at rule application time.
    """


class WriterException(DataGuardianError):
    """
    Raised when a Delta write operation fails after all retries are exhausted.

    Covers writes to Silver, Bronze-failed, audit.dq_violations,
    audit.dq_metrics, audit.pipeline_run_history, and audit.rule_execution_history.
    """


class ValidationException(DataGuardianError):
    """
    Raised when input data or configuration fails a validation check.

    Examples:
    - Source DataFrame is empty when rows are expected
    - DQ threshold breach when ``fail_on_threshold_breach`` is enabled
    - Mandatory column missing from the Bronze DataFrame schema
    """


class TransformationException(DataGuardianError):
    """
    Raised when a transformation step fails and the error mode is ``fail_fast``.

    Wraps the original exception so the full stack trace is preserved.
    The ``TransformationEngine`` raises this after capturing metrics for the
    failed step.
    """


class ContractValidationException(DataGuardianError):
    """
    Raised when a data contract validation fails and the policy is ``FAIL_PIPELINE``.

    Carries the ``ContractValidationResult`` as ``result`` so the notebook can
    log broken rules before re-raising or surfacing to the caller.

    The ``ContractValidationEngine`` never raises this directly — the notebook
    is responsible for raising after inspecting ``ContractValidationResult.can_proceed``.
    """
