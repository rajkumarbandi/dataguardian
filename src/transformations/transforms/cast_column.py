"""Transformation: cast_column — cast a column to a new data type."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class CastColumnTransformation(BaseTransformation):
    """
    Cast a column to a specified Spark SQL data type.

    Uses PySpark's ``cast()`` with an inline SQL type string — supports any
    type Spark recognises: ``"integer"``, ``"double"``, ``"date"``,
    ``"timestamp"``, ``"decimal(18,2)"``, etc.

    Failed casts produce ``null`` values (standard Spark behaviour).

    YAML::

        - type: cast_column
          params:
            column: amount
            datatype: "decimal(18,2)"

        - type: cast_column
          params:
            column: created_at
            datatype: timestamp
    """

    @property
    def transformation_type(self) -> str:
        return "cast_column"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        column: str = params["column"]
        datatype: str = params["datatype"]
        return df.withColumn(column, F.col(column).cast(datatype))

    def describe(self, params: dict[str, Any]) -> str:
        return (
            f"cast_column: '{params.get('column')}' → {params.get('datatype')}"
        )
