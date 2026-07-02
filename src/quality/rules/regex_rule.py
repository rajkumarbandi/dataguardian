"""Rule: regex — column value must match a regular expression pattern."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class RegexRule(BaseRule):
    """
    Fails any non-null row where ``column`` does not fully match ``params.pattern``.

    Uses PySpark's ``rlike()`` which applies Java regex semantics.  ``rlike``
    performs a partial match (anywhere in the string); anchor with ``^...$``
    for a full-string match.

    Required params
    ---------------
    ``pattern``:
        Java-compatible regular expression string.

    Null treatment
    --------------
    Null values PASS.  Use ``not_null`` to enforce non-nullability.

    Example YAML::

        - rule: regex
          column: postal_code
          params:
            pattern: "^[A-Z]{1,2}[0-9]{1,2}[A-Z]? [0-9][A-Z]{2}$"
    """

    @property
    def rule_type(self) -> str:
        return "regex"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        pattern: str = params.get("pattern", ".*")
        return df.withColumn(
            pass_column,
            F.col(column).isNull() | F.col(column).cast("string").rlike(pattern),
        )

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        pattern = params.get("pattern", ".*")
        return (
            f"Column '{column}' does not match the required pattern: {pattern!r}."
        )
