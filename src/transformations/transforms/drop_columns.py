"""Transformation: drop_columns — remove one or more columns."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DropColumnsTransformation(BaseTransformation):
    """
    Drop one or more columns from the DataFrame.

    Columns that do not exist in the DataFrame are silently ignored — this
    makes the transformation safe to declare even when a source may or may
    not produce a given column.

    YAML::

        - type: drop_columns
          params:
            columns: [internal_id, legacy_field, _tmp_col]
    """

    @property
    def transformation_type(self) -> str:
        return "drop_columns"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        columns: list[str] = params.get("columns", [])
        to_drop = [c for c in columns if c in df.columns]
        return df.drop(*to_drop) if to_drop else df

    def describe(self, params: dict[str, Any]) -> str:
        cols = params.get("columns", [])
        return f"drop_columns: {cols}"
