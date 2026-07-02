"""Rule: sql_expression — column passes if a SQL expression evaluates to true."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql.functions import expr

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class SqlExpressionRule(BaseRule):
    """
    Passes a row when a Spark SQL boolean expression evaluates to ``true``.

    This is the escape hatch for any validation logic that cannot be expressed
    by the built-in rule types.  The expression has full access to all columns
    in the DataFrame and any Spark SQL built-in function.

    Required params
    ---------------
    ``expression``:
        A Spark SQL expression string that returns a boolean.
        Column references use backtick quoting for names with special characters.

    Null treatment
    --------------
    The expression controls null semantics.  If the expression returns null,
    PySpark treats it as ``false`` (fail) after the engine applies
    ``coalesce(pass_col, false)``.

    Examples::

        # Cross-column constraint
        - rule: sql_expression
          column: discount_pct
          params:
            expression: "discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100)"
          description: "Discount must be between 0 and 100 percent"

        # Date ordering
        - rule: sql_expression
          column: end_date
          params:
            expression: "end_date IS NULL OR end_date >= start_date"
          description: "End date must not precede start date"
    """

    @property
    def rule_type(self) -> str:
        return "sql_expression"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        expression: str = params.get("expression", "true")
        return df.withColumn(pass_column, expr(expression))

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        expression = params.get("expression", "")
        description = params.get("description", "")
        if description:
            return f"Column '{column}' failed SQL rule: {description}"
        return f"Column '{column}' failed SQL expression: {expression!r}"
