"""Rule: not_null — column value must not be null."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class NotNullRule(BaseRule):
    """
    Fails any row where ``column`` is null.

    This is the most fundamental rule — it should be declared first for
    columns that are required for downstream processing (primary keys,
    foreign keys, mandatory business fields).

    Null treatment
    --------------
    Deviates from the base-class default: a null value IS a failure here.
    ``True`` (pass) means the value is not null.
    """

    @property
    def rule_type(self) -> str:
        return "not_null"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        return df.withColumn(pass_column, F.col(column).isNotNull())

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        return f"Column '{column}' must not be null."
