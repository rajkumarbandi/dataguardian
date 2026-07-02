"""Transformation: add_constant_column — add a column with a fixed literal value."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class AddConstantColumnTransformation(BaseTransformation):
    """
    Add a new column containing a constant literal value.

    The value is cast to ``params.datatype`` (default ``"string"``).
    Supports any Spark SQL scalar type: ``"integer"``, ``"double"``,
    ``"boolean"``, ``"date"``, etc.

    YAML::

        - type: add_constant_column
          params:
            column: data_source
            value: ERP_SYSTEM
            datatype: string

        - type: add_constant_column
          params:
            column: is_migrated
            value: true
            datatype: boolean
    """

    @property
    def transformation_type(self) -> str:
        return "add_constant_column"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        column: str = params["column"]
        value: Any = params["value"]
        datatype: str = params.get("datatype", "string")
        return df.withColumn(column, F.lit(value).cast(datatype))

    def describe(self, params: dict[str, Any]) -> str:
        return (
            f"add_constant_column: '{params.get('column')}' = "
            f"{params.get('value')!r} ({params.get('datatype', 'string')})"
        )
