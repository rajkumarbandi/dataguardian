"""Transformation: trim_strings — strip leading and trailing whitespace."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class TrimStringsTransformation(BaseTransformation):
    """
    Strip leading and trailing whitespace from string columns.

    When ``params.columns`` is empty or omitted, ALL string columns in the
    DataFrame are trimmed.

    YAML::

        # Trim specific columns
        - type: trim_strings
          params:
            columns: [first_name, last_name, email]

        # Trim all string columns
        - type: trim_strings
          params: {}
    """

    @property
    def transformation_type(self) -> str:
        return "trim_strings"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        from pyspark.sql.types import StringType

        columns: list[str] = params.get("columns", [])
        if not columns:
            columns = [
                f.name for f in df.schema.fields
                if isinstance(f.dataType, StringType)
            ]
        for col in columns:
            if col in df.columns:
                df = df.withColumn(col, F.trim(F.col(col)))
        return df

    def describe(self, params: dict[str, Any]) -> str:
        cols = params.get("columns", [])
        return f"trim_strings: {cols or 'all string columns'}"
