"""Transformation: select_columns — keep only the specified columns."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class SelectColumnsTransformation(BaseTransformation):
    """
    Keep only the columns listed in ``params.columns``.

    Order is preserved as declared.  Use this to enforce a strict column
    contract on the DataFrame before writing to Silver.

    YAML::

        - type: select_columns
          params:
            columns: [customer_id, first_name, email, country_code]
    """

    @property
    def transformation_type(self) -> str:
        return "select_columns"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        columns: list[str] = params["columns"]
        existing = [c for c in columns if c in df.columns]
        return df.select(*existing)

    def describe(self, params: dict[str, Any]) -> str:
        cols = params.get("columns", [])
        return f"select_columns: keeping {len(cols)} column(s)"
