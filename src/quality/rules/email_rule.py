"""Rule: email — column value must be a valid email address format."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# RFC 5322 simplified pattern — practical for business data.
# Catches the most common structural violations (missing @, spaces, no TLD).
# Does not attempt full RFC 5322 compliance (that regex is ~6 KB).
_EMAIL_PATTERN = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"


class EmailRule(BaseRule):
    """
    Fails any non-null row where ``column`` is not a structurally valid email.

    The pattern validates:
    - Presence of exactly one ``@``
    - Non-empty local part (before ``@``)
    - Non-empty domain with at least one dot
    - TLD of at least two characters
    - No whitespace anywhere

    Null treatment
    --------------
    Null values PASS.  Use ``not_null`` to enforce non-nullability.

    Custom pattern
    --------------
    Override the built-in pattern via ``params.pattern``::

        - rule: email
          column: contact_email
          params:
            pattern: "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"
    """

    @property
    def rule_type(self) -> str:
        return "email"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        pattern = params.get("pattern", _EMAIL_PATTERN)
        return df.withColumn(
            pass_column,
            F.col(column).isNull() | F.col(column).cast("string").rlike(pattern),
        )

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        return (
            f"Column '{column}' must be a valid email address "
            "(e.g. user@example.com). Missing '@', spaces, or invalid domain."
        )
