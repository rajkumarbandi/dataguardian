"""Transformation: sort_rows — sort the DataFrame by one or more columns."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class SortRowsTransformation(BaseTransformation):
    """
    Sort the DataFrame by one or more columns.

    ``ascending`` can be a single boolean (applies to all columns) or a list
    of booleans (one per column).

    YAML::

        # Sort by a single column descending
        - type: sort_rows
          params:
            columns: [created_date]
            ascending: false

        # Multi-column sort with mixed directions
        - type: sort_rows
          params:
            columns: [customer_id, order_date]
            ascending: [true, false]
    """

    @property
    def transformation_type(self) -> str:
        return "sort_rows"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        columns: list[str] = params["columns"]
        ascending = params.get("ascending", True)

        if isinstance(ascending, list):
            sort_cols = [
                F.col(c).asc() if asc else F.col(c).desc()
                for c, asc in zip(columns, ascending)
            ]
            return df.orderBy(*sort_cols)

        return df.orderBy(columns, ascending=ascending)

    def describe(self, params: dict[str, Any]) -> str:
        asc = params.get("ascending", True)
        direction = "ASC" if asc is True else ("DESC" if asc is False else "mixed")
        return f"sort_rows: {params.get('columns')} {direction}"
