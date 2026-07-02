"""Transformation: date_format — reformat a date or timestamp column."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class DateFormatTransformation(BaseTransformation):
    """
    Reformat a date or timestamp column to a specified output format string.

    When ``params.input_format`` is provided, the column is first parsed from
    that format (useful for string columns containing dates).  When omitted,
    Spark attempts automatic date/timestamp parsing.

    The result is always a string column.  Use ``cast_column`` afterwards if
    a Date or Timestamp type is needed.

    YAML::

        # Reformat an existing date column to YYYY/MM/DD
        - type: date_format
          params:
            column: created_date
            output_format: "yyyy/MM/dd"

        # Parse from a string and output in ISO format
        - type: date_format
          params:
            column: raw_date_str
            input_format: "dd/MM/yyyy"
            output_format: "yyyy-MM-dd"
            output_column: formatted_date
    """

    @property
    def transformation_type(self) -> str:
        return "date_format"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        column: str = params["column"]
        output_format: str = params["output_format"]
        output_column: str = params.get("output_column", column)
        input_format: str | None = params.get("input_format")

        if input_format:
            date_col = F.to_date(F.col(column), input_format)
        else:
            date_col = F.col(column)

        return df.withColumn(output_column, F.date_format(date_col, output_format))

    def describe(self, params: dict[str, Any]) -> str:
        out_col = params.get("output_column", params.get("column"))
        return (
            f"date_format: '{params.get('column')}' → '{out_col}' "
            f"({params.get('output_format')})"
        )
