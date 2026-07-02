"""Rule: primary_key — column (or column combination) must be non-null and unique."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class PrimaryKeyRule(BaseRule):
    """
    Fails any row where the primary key is null OR duplicated within the batch.

    Combines not-null and unique checks in a single rule for natural/surrogate
    primary keys.  For composite keys, list all component columns in
    ``params.columns``; the ``column`` field is used as the single-column
    fallback.

    Params (optional)
    -----------------
    ``columns``:
        List of column names forming the composite key.
        When omitted, ``column`` is used as a single-column key.

    Null treatment
    --------------
    Deviates from the base-class default: any null component column FAILS.

    Example YAML::

        # Single-column PK
        - rule: primary_key
          column: customer_id

        # Composite PK
        - rule: primary_key
          column: order_item_id
          params:
            columns: [order_id, product_id]
    """

    @property
    def rule_type(self) -> str:
        return "primary_key"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        key_columns: list[str] = params.get("columns", [column])

        # All key columns must be non-null
        not_null_cond = reduce(
            lambda a, b: a & b,
            [F.col(c).isNotNull() for c in key_columns],
        )

        # Combination must be unique within the batch
        _tmp = "_pk_count_tmp"
        w = Window.partitionBy(*[F.col(c) for c in key_columns])
        df = df.withColumn(_tmp, F.count("*").over(w))
        df = df.withColumn(pass_column, not_null_cond & (F.col(_tmp) == 1))
        return df.drop(_tmp)

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        key_columns = params.get("columns", [column])
        cols = ", ".join(f"'{c}'" for c in key_columns)
        return (
            f"Primary key violation on {cols}: "
            "value is null or appears more than once in the batch."
        )
