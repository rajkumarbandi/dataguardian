"""Transformation: filter_rows — keep only rows matching a SQL condition."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FilterRowsTransformation(BaseTransformation):
    """
    Keep only rows that satisfy a Spark SQL boolean expression.

    This transformation modifies the row count — the engine counts the output
    rows to report accurate metrics.

    YAML::

        - type: filter_rows
          params:
            condition: "is_active = true"

        - type: filter_rows
          params:
            condition: "order_date >= '2024-01-01' AND total_amount > 0"
    """

    modifies_row_count = True

    @property
    def transformation_type(self) -> str:
        return "filter_rows"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        condition: str = params["condition"]
        return df.filter(condition)

    def describe(self, params: dict[str, Any]) -> str:
        cond = params.get("condition", "")
        return f"filter_rows: WHERE {cond[:80]}"
