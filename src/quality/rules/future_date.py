"""Rule: future_date — date column value must not be in the future."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class FutureDateRule(BaseRule):
    """
    Fails any non-null row where a date or timestamp column is strictly after
    the current date (``CURRENT_DATE()``).

    Typical uses: ``created_date``, ``order_date``, ``birth_date``.

    Null treatment
    --------------
    Null values PASS.  Use ``not_null`` to enforce non-nullability.

    Note on clocks
    --------------
    ``CURRENT_DATE()`` is evaluated on the executor at the time the Spark plan
    runs — not on the driver at engine initialisation time.  For daily batch
    jobs this is equivalent.  If sub-day precision matters, use the
    ``sql_expression`` rule with an explicit timestamp comparison.
    """

    @property
    def rule_type(self) -> str:
        return "future_date"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        return df.withColumn(
            pass_column,
            F.col(column).isNull() | (F.col(column).cast("date") <= F.current_date()),
        )

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        return (
            f"Column '{column}' contains a future date. "
            "Dates must be on or before the current date."
        )
