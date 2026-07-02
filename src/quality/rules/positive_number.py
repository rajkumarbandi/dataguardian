"""Rule: positive_number — column value must be strictly greater than zero."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class PositiveNumberRule(BaseRule):
    """
    Fails any non-null row where ``column`` value is ≤ 0.

    Typical uses: unit_price, quantity, stock_quantity.

    Params (optional)
    -----------------
    ``allow_zero``:
        If ``true``, values ≥ 0 pass (default: ``false``, strictly positive).

    Null treatment
    --------------
    Null values PASS.  Use ``not_null`` to enforce non-nullability.
    """

    @property
    def rule_type(self) -> str:
        return "positive_number"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        allow_zero: bool = bool(params.get("allow_zero", False))
        threshold = 0 if allow_zero else 0
        if allow_zero:
            condition = F.col(column).isNull() | (F.col(column).cast("double") >= threshold)
        else:
            condition = F.col(column).isNull() | (F.col(column).cast("double") > threshold)
        return df.withColumn(pass_column, condition)

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        allow_zero = bool(params.get("allow_zero", False))
        bound = "≥ 0" if allow_zero else "> 0"
        return f"Column '{column}' must be {bound} (positive number). Negative or zero values are invalid."
