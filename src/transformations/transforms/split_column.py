"""Transformation: split_column — split a string column by a delimiter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class SplitColumnTransformation(BaseTransformation):
    """
    Split a string column by a delimiter into multiple output columns or an array.

    When ``params.output_columns`` is provided, each element of the split
    array is assigned to a named column.  When omitted, the result is stored
    as an array column named ``{column}_parts`` (or ``params.output_column``).

    YAML::

        # Split into named columns
        - type: split_column
          params:
            column: full_name
            delimiter: " "
            output_columns: [first_name, last_name]

        # Split into an array column
        - type: split_column
          params:
            column: tags_str
            delimiter: ","
            output_column: tags_array
    """

    @property
    def transformation_type(self) -> str:
        return "split_column"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        column: str = params["column"]
        delimiter: str = params["delimiter"]
        output_columns: list[str] = params.get("output_columns", [])

        split_expr = F.split(F.col(column), delimiter)

        if output_columns:
            for i, out_col in enumerate(output_columns):
                df = df.withColumn(out_col, split_expr[i])
        else:
            output_column: str = params.get("output_column", f"{column}_parts")
            df = df.withColumn(output_column, split_expr)

        return df

    def describe(self, params: dict[str, Any]) -> str:
        out = params.get("output_columns") or params.get("output_column", f"{params.get('column')}_parts")
        return f"split_column: '{params.get('column')}' → {out}"
