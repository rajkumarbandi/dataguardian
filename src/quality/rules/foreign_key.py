"""Rule: foreign_key — column value must exist in a reference Delta table."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from src.common.logger import get_logger
from src.quality.rules.base_rule import BaseRule

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_log = get_logger("dataguardian.quality.rules.foreign_key")


class ForeignKeyRule(BaseRule):
    """
    Fails any non-null row where ``column`` does not match any value in
    the reference column of a reference Delta table.

    Uses a broadcast LEFT JOIN for efficiency — assumes reference tables
    are small enough to fit in driver/executor memory (typical for
    dimension tables).

    Required params
    ---------------
    ``reference_table``:
        Fully qualified Delta table name (e.g. ``dg_dev.bronze.erp_customers``).
    ``reference_column``:
        Column in the reference table to join against.
        Defaults to the same name as ``column`` when omitted.

    Null treatment
    --------------
    Null values in ``column`` PASS — use ``not_null`` to enforce that FK
    columns are populated.

    Soft failure
    ------------
    If the reference table does not exist (e.g. first pipeline run, test
    environment), the rule logs a WARNING and marks all rows as passing rather
    than aborting the DQ run.  Set ``params.fail_on_missing_reference: true``
    to make a missing table an engine error instead.

    Example YAML::

        - rule: foreign_key
          column: customer_id
          params:
            reference_table: "{catalog}.bronze.erp_customers"
            reference_column: customer_id
    """

    @property
    def rule_type(self) -> str:
        return "foreign_key"

    def apply(
        self,
        df: DataFrame,
        column: str,
        pass_column: str,
        params: dict[str, Any],
        spark: SparkSession | None = None,
    ) -> DataFrame:
        ref_table: str = params.get("reference_table", "")
        ref_col: str = params.get("reference_column", column)
        fail_on_missing: bool = bool(params.get("fail_on_missing_reference", False))

        if not ref_table:
            raise ValueError(
                f"foreign_key rule on column '{column}' requires "
                "'params.reference_table' to be set."
            )

        if spark is None:
            _log.warning(
                "SparkSession not provided to foreign_key rule; marking all rows as passing",
                column=column,
                reference_table=ref_table,
            )
            return df.withColumn(pass_column, F.lit(True))

        try:
            ref_df = (
                spark.table(ref_table)
                .select(F.col(ref_col).alias("_fk_ref"))
                .distinct()
            )
        except Exception as exc:
            if fail_on_missing:
                raise RuntimeError(
                    f"foreign_key rule: reference table {ref_table!r} "
                    f"could not be loaded: {exc}"
                ) from exc
            _log.warning(
                "Reference table not found; foreign_key rule skipped (all rows pass)",
                reference_table=ref_table,
                column=column,
                error=str(exc),
            )
            return df.withColumn(pass_column, F.lit(True))

        _fk_alias = f"_fk_{column}_tmp"
        df = df.join(
            ref_df.withColumnRenamed("_fk_ref", _fk_alias).hint("broadcast"),
            on=F.col(column) == F.col(_fk_alias),
            how="left",
        )
        # Pass if column is null (not_null handles that separately)
        # OR if a matching row was found in the reference table
        df = df.withColumn(
            pass_column,
            F.col(column).isNull() | F.col(_fk_alias).isNotNull(),
        )
        return df.drop(_fk_alias)

    def error_message(self, column: str, params: dict[str, Any]) -> str:
        ref_table = params.get("reference_table", "<unknown>")
        ref_col = params.get("reference_column", column)
        return (
            f"Column '{column}' contains a value that does not exist in "
            f"'{ref_table}.{ref_col}'. Referential integrity violation."
        )
