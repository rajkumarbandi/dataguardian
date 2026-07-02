"""Transformation: concatenate_columns — join multiple columns into one string."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ConcatenateColumnsTransformation(BaseTransformation):
    """
    Concatenate two or more columns into a new string column.

    Null values in source columns are treated as empty strings.  The separator
    is inserted between each non-null value (``concat_ws`` semantics).

    YAML::

        - type: concatenate_columns
          params:
            columns: [first_name, last_name]
            separator: " "
            output_column: full_name

        - type: concatenate_columns
          params:
            columns: [city, state, country]
            separator: ", "
            output_column: address_display
    """

    @property
    def transformation_type(self) -> str:
        return "concatenate_columns"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        columns: list[str] = params["columns"]
        separator: str = params.get("separator", "")
        output_column: str = params["output_column"]

        col_exprs = [
            F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns
        ]
        return df.withColumn(output_column, F.concat_ws(separator, *col_exprs))

    def describe(self, params: dict[str, Any]) -> str:
        return (
            f"concatenate_columns: {params.get('columns')} → "
            f"'{params.get('output_column')}'"
        )
