"""Audit writer for contract validation history."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.contracts.contract_model import ContractValidationResult

logger = logging.getLogger(__name__)

_TABLE = "contract_history"
_SCHEMA = "audit"


class ContractHistoryWriter:
    """Write contract validation results to ``audit.contract_history``.

    All Delta writes are soft-fail — a write error logs a WARNING and never
    propagates to the calling pipeline.
    """

    def __init__(self, catalog: str, enabled: bool = True) -> None:
        self._catalog = catalog
        self._enabled = enabled
        self._full_table = f"{catalog}.{_SCHEMA}.{_TABLE}"

    def write(
        self,
        run_id: str,
        source_name: str,
        contract_result: ContractValidationResult,
        spark: SparkSession,
    ) -> None:
        if not self._enabled:
            return

        try:
            self._ensure_table(spark)
            row = self._to_row(run_id, source_name, contract_result)
            df = spark.createDataFrame([row], schema=self._row_schema())
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(self._full_table)
            )
        except Exception as exc:
            logger.warning(
                "ContractHistoryWriter: failed to write to %s — %s",
                self._full_table,
                exc,
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _to_row(
        self,
        run_id: str,
        source_name: str,
        result: ContractValidationResult,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "source_name": source_name,
            "contract_name": result.contract_name,
            "contract_version": result.contract_version,
            "validation_status": result.status,
            "validation_policy": result.validation_policy,
            "rules_passed": int(result.rules_passed),
            "rules_failed": int(result.rules_failed),
            "warnings": int(result.warnings),
            "broken_rules": result.broken_rules_json(),
            "message": result.message,
            "recorded_at": datetime.now(tz=timezone.utc),
        }

    def _row_schema(self) -> str:
        return (
            "run_id STRING, source_name STRING, contract_name STRING, "
            "contract_version STRING, validation_status STRING, "
            "validation_policy STRING, rules_passed INT, rules_failed INT, "
            "warnings INT, broken_rules STRING, message STRING, "
            "recorded_at TIMESTAMP"
        )

    def _ensure_table(self, spark: SparkSession) -> None:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._catalog}.{_SCHEMA}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._full_table} (
                run_id              STRING,
                source_name         STRING,
                contract_name       STRING,
                contract_version    STRING,
                validation_status   STRING,
                validation_policy   STRING,
                rules_passed        INT,
                rules_failed        INT,
                warnings            INT,
                broken_rules        STRING,
                message             STRING,
                recorded_at         TIMESTAMP
            )
            USING DELTA
            COMMENT 'Contract validation results written by ContractValidationEngine'
        """)
