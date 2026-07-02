"""Transformation: null_replacement — replace null values with defaults."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.transformations.base_transformation import BaseTransformation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class NullReplacementTransformation(BaseTransformation):
    """
    Replace null values with specified defaults.

    Two modes:

    ``replacements`` dict (multi-column)
        Replaces nulls in multiple columns at once.  Values are passed to
        ``DataFrame.fillna()`` — all Spark-supported types are supported.

    ``column`` + ``value`` (single column)
        Uses ``coalesce()`` to replace null in one column.  Preserves the
        column type more reliably for complex types.

    YAML::

        # Multi-column mode
        - type: null_replacement
          params:
            replacements:
              customer_segment: Unknown
              annual_revenue: 0.0
              is_active: false

        # Single-column mode
        - type: null_replacement
          params:
            column: status
            value: PENDING
    """

    @property
    def transformation_type(self) -> str:
        return "null_replacement"

    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        if "replacements" in params:
            replacements: dict = params["replacements"]
            return df.fillna(replacements)
        if "column" in params and "value" in params:
            col = params["column"]
            val = params["value"]
            return df.withColumn(col, F.coalesce(F.col(col), F.lit(val)))
        raise ValueError(
            "null_replacement requires either 'replacements' dict "
            "or 'column' + 'value' parameters."
        )

    def describe(self, params: dict[str, Any]) -> str:
        if "replacements" in params:
            return f"null_replacement: {list(params['replacements'].keys())}"
        return f"null_replacement: '{params.get('column')}' → '{params.get('value')}'"
