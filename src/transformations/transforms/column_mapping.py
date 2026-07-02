"""Transformation: column_mapping — rename multiple columns at once."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ColumnMappingTransformation(BaseTransformation):
    """
    Rename multiple columns in a single step.

    YAML::

        - type: column_mapping
          params:
            mappings:
              customerid: customer_id
              cust_name: customer_name
              amt: amount
    """

    @property
    def transformation_type(self) -> str:
        return "column_mapping"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        mappings: dict[str, str] = params.get("mappings", {})
        for old_name, new_name in mappings.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        return df

    def describe(self, params: dict[str, Any]) -> str:
        count = len(params.get("mappings", {}))
        return f"column_mapping: {count} column(s) renamed"
