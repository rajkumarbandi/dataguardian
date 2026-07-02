"""Rule: allowed_values — column value must be one of a declared set."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class AllowedValuesRule(BaseRule):
    """
    Fails any non-null row where ``column`` value is not in ``params.values``.

    Required params
    ---------------
    ``values``:
        List of accepted values.  Type coercion is applied: values are cast to
        strings for comparison (since YAML delivers them as strings or numbers).

    Null treatment
    --------------
    Null values PASS this rule.  Use ``not_null`` to enforce non-nullability.

    Example YAML::

        - rule: allowed_values
          column: status
          params:
            values: [PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED]
    """

    @property
    def rule_type(self) -> str:
        return "allowed_values"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        values = [str(v) for v in params.get("values", [])]
        return df.withColumn(
            pass_column,
            F.col(column).isNull() | F.col(column).cast("string").isin(values),
        )

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        values = params.get("values", [])
        return (
            f"Column '{column}' contains an invalid value. "
            f"Allowed values: {values}."
        )
