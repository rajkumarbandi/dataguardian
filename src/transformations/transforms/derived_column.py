"""Transformation: derived_column — compute a column from a Spark SQL expression."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DerivedColumnTransformation(BaseTransformation):
    """
    Add or replace a column computed from a Spark SQL expression.

    The expression has full access to all existing columns and all Spark
    built-in functions.  Cross-column calculations, conditional logic, and
    window functions are all supported.

    YAML::

        - type: derived_column
          params:
            column: total_with_tax
            expression: "total_amount * 1.20"

        - type: derived_column
          params:
            column: customer_tier
            expression: >
              CASE
                WHEN annual_revenue >= 1000000 THEN 'Platinum'
                WHEN annual_revenue >= 100000  THEN 'Gold'
                ELSE 'Standard'
              END

        # Overwrite an existing column
        - type: derived_column
          params:
            column: email
            expression: "lower(trim(email))"
    """

    @property
    def transformation_type(self) -> str:
        return "derived_column"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        column: str = params["column"]
        expression: str = params["expression"]
        return df.withColumn(column, F.expr(expression))

    def describe(self, params: dict[str, Any]) -> str:
        return (
            f"derived_column: '{params.get('column')}' = "
            f"{params.get('expression', '')[:60]}"
        )
