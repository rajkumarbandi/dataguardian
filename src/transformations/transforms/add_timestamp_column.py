"""Transformation: add_timestamp_column — add a column with the current timestamp."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class AddTimestampColumnTransformation(BaseTransformation):
    """
    Add a new column containing ``current_timestamp()`` at execution time.

    Useful for recording when a transformation was applied — commonly used
    to stamp rows with ``_transformed_at`` for lineage tracking.

    YAML::

        - type: add_timestamp_column
          params:
            column: _transformed_at

        # Column name defaults to _transformed_at when omitted
        - type: add_timestamp_column
          params: {}
    """

    @property
    def transformation_type(self) -> str:
        return "add_timestamp_column"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        column: str = params.get("column", "_transformed_at")
        return df.withColumn(column, F.current_timestamp())

    def describe(self, params: dict[str, Any]) -> str:
        col = params.get("column", "_transformed_at")
        return f"add_timestamp_column: '{col}' = current_timestamp()"
