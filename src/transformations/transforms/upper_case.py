"""Transformation: upper_case — convert string columns to upper case."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class UpperCaseTransformation(BaseTransformation):
    """
    Convert specified string columns to upper case.

    YAML::

        - type: upper_case
          params:
            columns: [country_code, status]
    """

    @property
    def transformation_type(self) -> str:
        return "upper_case"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        columns: list[str] = params.get("columns", [])
        for col in columns:
            if col in df.columns:
                df = df.withColumn(col, F.upper(F.col(col)))
        return df

    def describe(self, params: dict[str, Any]) -> str:
        return f"upper_case: {params.get('columns', [])}"
