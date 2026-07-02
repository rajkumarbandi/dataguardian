"""Transformation: rename_column — rename a single column."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class RenameColumnTransformation(BaseTransformation):
    """
    Rename a single column.

    YAML::

        - type: rename_column
          params:
            from: customerid
            to: customer_id
    """

    @property
    def transformation_type(self) -> str:
        return "rename_column"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        from_col = params["from"]
        to_col = params["to"]
        return df.withColumnRenamed(from_col, to_col)

    def describe(self, params: dict[str, Any]) -> str:
        return f"rename_column: '{params.get('from')}' → '{params.get('to')}'"
