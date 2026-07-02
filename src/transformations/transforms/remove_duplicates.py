"""Transformation: remove_duplicates — drop duplicate rows."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class RemoveDuplicatesTransformation(BaseTransformation):
    """
    Remove duplicate rows, optionally considering only a subset of columns.

    When ``params.columns`` is empty or omitted, all columns are used for
    deduplication (``dropDuplicates()`` with no arguments).  When columns are
    specified, only those columns determine uniqueness (rows with the same
    values in those columns are deduplicated, keeping the first occurrence).

    This transformation modifies the row count.

    YAML::

        # Deduplicate on all columns
        - type: remove_duplicates
          params: {}

        # Deduplicate on specific key columns
        - type: remove_duplicates
          params:
            columns: [customer_id]

        - type: remove_duplicates
          params:
            columns: [order_id, product_id]
    """

    modifies_row_count = True

    @property
    def transformation_type(self) -> str:
        return "remove_duplicates"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        columns: list[str] = params.get("columns", [])
        if columns:
            return df.dropDuplicates(columns)
        return df.dropDuplicates()

    def describe(self, params: dict[str, Any]) -> str:
        cols = params.get("columns", [])
        return f"remove_duplicates: {'all columns' if not cols else cols}"
