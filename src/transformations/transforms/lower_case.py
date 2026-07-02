"""Transformation: lower_case — convert string columns to lower case."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class LowerCaseTransformation(BaseTransformation):
    """
    Convert specified string columns to lower case.

    YAML::

        - type: lower_case
          params:
            columns: [email, username]
    """

    @property
    def transformation_type(self) -> str:
        return "lower_case"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        columns: list[str] = params.get("columns", [])
        for col in columns:
            if col in df.columns:
                df = df.withColumn(col, F.lower(F.col(col)))
        return df

    def describe(self, params: dict[str, Any]) -> str:
        return f"lower_case: {params.get('columns', [])}"
