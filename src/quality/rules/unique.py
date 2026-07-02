"""Rule: unique — column value must not appear more than once in the batch."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class UniqueRule(BaseRule):
    """
    Fails any row where ``column`` value appears more than once in the DataFrame.

    Uses a window function (``COUNT(*) OVER (PARTITION BY column)``) so no
    shuffle beyond what Spark already plans.

    Null treatment
    --------------
    Multiple null values ALL fail — ``PARTITION BY`` groups nulls together, so
    N nulls produce count = N ≠ 1.  A single null produces count = 1 = pass.
    Combined with ``not_null``, all nulls will be flagged regardless.

    Uniqueness scope
    ----------------
    Uniqueness is checked WITHIN the current batch only.  Records from
    previous batches already written to Bronze are not checked here.
    Cross-batch deduplication is the Silver layer's responsibility.
    """

    @property
    def rule_type(self) -> str:
        return "unique"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        _tmp = f"_uniq_count_{column}_tmp"
        w = Window.partitionBy(F.col(column))
        return (
            df.withColumn(_tmp, F.count("*").over(w))
            .withColumn(pass_column, F.col(_tmp) == 1)
            .drop(_tmp)
        )

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        return f"Column '{column}' must be unique within the batch — duplicate value detected."
