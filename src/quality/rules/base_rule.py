"""
Abstract base class for all DataGuardian Data Quality rules.

Every rule must subclass ``BaseRule`` and implement the three abstract members.
The ``DataQualityEngine`` discovers rules through ``RuleRegistry`` and calls
``apply()`` on each one in declaration order.

Extension point for AI
-----------------------
To add an AI-generated rule, subclass ``BaseRule``, implement the three methods,
and call ``RuleRegistry.register("my_ai_rule", MyAIRule)`` at import time.
The engine does not need to change.

Rule contract
-------------
* ``apply()`` receives the full DataFrame and must add exactly one boolean
  column named ``pass_column``.
* ``True``  = row passed this rule.
* ``False`` = row failed this rule.
* Null values in the checked column should be treated as PASSING this rule
  (null enforcement is the job of ``not_null``).  Each rule documents whether
  it deviates from this default.
* Rules must be **side-effect free** — no writes, no external calls, no state
  mutations between ``apply()`` calls.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class BaseRule(ABC):
    """
    Abstract interface for a single Data Quality rule.

    Subclassing
    -----------
    Implement ``rule_type``, ``apply()``, and ``error_message()``.
    Simple rules that need no SparkSession can ignore the ``spark``
    parameter in ``apply()``.

    ::

        class MyRule(BaseRule):
            @property
            def rule_type(self) -> str:
                return "my_rule"

            def apply(self, df, column, pass_column, params, spark=None):
                return df.withColumn(pass_column, ...)

            def error_message(self, column, params):
                return f"Column '{column}' failed my_rule"
    """

    @property
    @abstractmethod
    def rule_type(self) -> str:
        """
        Short identifier for this rule.

        Must match the key used to register it in ``RuleRegistry`` and the
        ``rule:`` field in source YAML.  Use snake_case.

        Examples: ``"not_null"``, ``"email"``, ``"allowed_values"``
        """

    @abstractmethod
    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        """
        Apply the rule and return the DataFrame with ``pass_column`` added.

        Parameters
        ----------
        df:
            The DataFrame being validated.  Never mutated in place.
        column:
            The column this rule validates.
        pass_column:
            The name of the boolean column to add (managed by the engine).
        params:
            Rule-specific parameters from the YAML ``params:`` block.
        spark:
            Active ``SparkSession`` — required only by rules that load
            reference data (e.g. ``foreign_key``).

        Returns
        -------
        DataFrame
            A new DataFrame identical to ``df`` plus the ``pass_column``
            boolean column (``True`` = pass, ``False`` = fail).
        """

    @abstractmethod
    def error_message(self, column: str, params: dict[str, Any]) -> str:
        """
        Return a human-readable explanation of what this rule checks.

        Used to populate the ``error_message`` field in the violations table
        so Stewardship App users understand why a record was rejected.

        Example: ``"Column 'email' must be a valid RFC 5322 email address."``
        """
