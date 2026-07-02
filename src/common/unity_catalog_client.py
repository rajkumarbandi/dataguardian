"""
Unity Catalog client for the DataGuardian platform.

``UnityCatalogClient`` wraps all DDL operations so that pipeline code never
issues raw SQL strings directly.  Every identifier (catalog, schema, table) is
backtick-quoted before being embedded in SQL, preventing identifier injection
and supporting names that contain hyphens or dots.

Why a dedicated client instead of inline spark.sql() calls?
------------------------------------------------------------
* All catalog DDL is testable in isolation by injecting a mock SparkSession.
* The backtick-quoting logic lives in one place — no accidental omissions.
* ``table_exists()`` uses ``SHOW TABLES`` rather than catching an exception,
  which is more robust and avoids confusing error logs in normal operation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.common.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class UnityCatalogClient:
    """
    Performs catalog / schema / table management via Unity Catalog DDL.

    Parameters
    ----------
    spark:
        An active ``SparkSession`` configured with Unity Catalog support.
    catalog:
        The Unity Catalog catalog to operate against (e.g. ``dg_dev``).

    Example
    -------
    ::

        client = UnityCatalogClient(spark=spark, catalog="dg_dev")
        client.use_catalog()
        client.create_schema_if_not_exists("bronze")
        exists = client.table_exists("bronze", "erp_customers")
    """

    def __init__(self, spark: SparkSession, catalog: str) -> None:
        if not catalog:
            raise ConfigurationError(
                "catalog must be a non-empty string. "
                "Check the unity_catalog.catalog value in your environment config."
            )
        self._spark = spark
        self._catalog = catalog

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def use_catalog(self) -> None:
        """Set the active catalog for subsequent SQL statements."""
        self._execute(f"USE CATALOG {self._quote(self._catalog)}")

    def use_schema(self, schema: str) -> None:
        """Set the active schema within the current catalog."""
        self._execute(f"USE SCHEMA {self._quote(schema)}")

    def create_schema_if_not_exists(self, schema: str) -> None:
        """
        Create ``schema`` in the active catalog if it does not already exist.

        Idempotent — safe to call on every pipeline run.
        """
        fq_schema = f"{self._quote(self._catalog)}.{self._quote(schema)}"
        self._execute(f"CREATE SCHEMA IF NOT EXISTS {fq_schema}")

    def table_exists(self, schema: str, table: str) -> bool:
        """
        Return ``True`` if ``catalog.schema.table`` exists in Unity Catalog.

        Uses ``SHOW TABLES`` rather than catching a ``AnalysisException``
        so the check is silent and predictable.
        """
        rows = self._spark.sql(
            f"SHOW TABLES IN {self._quote(self._catalog)}.{self._quote(schema)} "
            f"LIKE {self._quote(table)}"
        ).collect()
        return any(r.tableName == table for r in rows)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _execute(self, sql: str) -> None:
        """Execute a DDL statement, surfacing errors as ``ConfigurationError``."""
        try:
            self._spark.sql(sql)
        except Exception as exc:
            raise ConfigurationError(
                f"Unity Catalog DDL failed.\n"
                f"Statement: {sql}\n"
                f"Error: {exc}"
            ) from exc

    @staticmethod
    def _quote(identifier: str) -> str:
        """Wrap ``identifier`` in backticks, escaping any embedded backticks."""
        escaped = identifier.replace("`", "``")
        return f"`{escaped}`"
