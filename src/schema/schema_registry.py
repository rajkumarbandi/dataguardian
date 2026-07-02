"""
Schema registry — version-tracked storage of entity schema contracts.

``SchemaRegistry`` maintains the canonical expected schema for each DataGuardian
source.  Schema versions are persisted to the Delta table
``{catalog}.audit.schema_registry`` for full lineage.  When the table does not
yet exist (first deployment), the registry transparently initialises from the
source YAML column definitions or from the first observed incoming schema.

Version numbering
-----------------
Version 1 is the initial registration — either from the source YAML ``schema:``
section or from the first observed incoming DataFrame when no YAML definition is
present.  ``AUTO_EVOLVE`` mode increments the version on each schema change.

Soft-fail design
----------------
All Delta reads/writes are wrapped in try/except.  A registry failure (wrong
permissions, table not yet created, catalog unavailable) logs a WARNING and
falls back to YAML-derived behaviour — it never aborts the pipeline.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

    from src.common.models import SourceConfig


@dataclass
class SchemaVersion:
    """A single versioned schema entry in the registry."""

    source_name: str
    version: int
    schema_json: str          # PySpark StructType JSON string
    column_count: int
    registered_by: str        # run_id that registered this version
    registered_at: datetime
    evolution_mode: str
    change_summary: str = ""

    def to_struct(self) -> StructType:
        """Deserialise the stored JSON back to a PySpark StructType."""
        from pyspark.sql.types import StructType
        return StructType.fromJson(json.loads(self.schema_json))


class SchemaRegistry:
    """
    Versioned schema storage backed by ``{catalog}.audit.schema_registry``.

    Parameters
    ----------
    spark:
        Active SparkSession.
    catalog:
        Unity Catalog name (e.g. ``dg_dev``).
    enabled:
        When ``False`` the registry is a no-op: ``get_registered_schema``
        always returns ``None`` and ``register_schema`` skips the Delta write.
        Set via ``env_config.schema_registry.schema_registry_enabled``.
    logger:
        Optional pre-bound logger.
    """

    _TABLE = "schema_registry"
    _SCHEMA = "audit"

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        enabled: bool = True,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._spark = spark
        self._catalog = catalog
        self._enabled = enabled
        self._log = logger or get_logger("dataguardian.schema.registry")
        self._table_fqn = f"{catalog}.{self._SCHEMA}.{self._TABLE}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_registered_schema(self, source_name: str) -> SchemaVersion | None:
        """
        Return the most recent schema version for ``source_name``, or ``None``
        when no version has been registered yet.

        Soft-fail: a Delta read error returns ``None`` with a WARNING.

        Parameters
        ----------
        source_name:
            Source identifier (matches YAML ``name:``).
        """
        if not self._enabled:
            return None
        try:
            df = (
                self._spark.table(self._table_fqn)
                .filter(f"source_name = '{source_name}'")
                .orderBy("schema_version", ascending=False)
                .limit(1)
            )
            rows = df.collect()
            if not rows:
                return None
            row = rows[0]
            return SchemaVersion(
                source_name=row["source_name"],
                version=row["schema_version"],
                schema_json=row["schema_json"],
                column_count=row["column_count"],
                registered_by=row["registered_by"],
                registered_at=row["registered_at"],
                evolution_mode=row["evolution_mode"],
                change_summary=row["change_summary"] if "change_summary" in row else "",
            )
        except Exception as exc:
            self._log.warning(
                "Schema registry read failed — treating as first run",
                source_name=source_name,
                error=str(exc),
            )
            return None

    def get_next_version(self, source_name: str) -> int:
        """Return the next version number for ``source_name`` (current max + 1, or 1)."""
        current = self.get_registered_schema(source_name)
        return (current.version + 1) if current else 1

    def register_schema(
        self,
        source_name: str,
        schema: StructType,
        run_id: str = "",
        evolution_mode: str = "STRICT",
        change_summary: str = "",
    ) -> SchemaVersion:
        """
        Register a new schema version.

        The Delta write is soft-fail: on failure the method logs a WARNING and
        returns the in-memory ``SchemaVersion`` (pipeline continues regardless).

        Parameters
        ----------
        source_name:
            Source identifier to register against.
        schema:
            PySpark StructType to persist.
        run_id:
            Pipeline run ID recorded as ``registered_by``.
        evolution_mode:
            Evolution mode that triggered this registration.
        change_summary:
            Human-readable description of what changed (empty on first run).
        """
        version = self.get_next_version(source_name)
        schema_json = json.dumps(schema.jsonValue())
        now = datetime.now(tz=timezone.utc)

        sv = SchemaVersion(
            source_name=source_name,
            version=version,
            schema_json=schema_json,
            column_count=len(schema.fields),
            registered_by=run_id,
            registered_at=now,
            evolution_mode=evolution_mode,
            change_summary=change_summary,
        )

        if not self._enabled:
            self._log.info(
                "Schema registry disabled — version tracked in memory only",
                source_name=source_name,
                version=version,
            )
            return sv

        self._write_version(sv)
        return sv

    def build_struct_from_yaml(self, source_config: SourceConfig) -> StructType | None:
        """
        Convert source YAML column definitions to a PySpark StructType.

        Returns ``None`` when the YAML has no ``schema:`` section.
        """
        if not source_config.schema:
            return None

        from pyspark.sql.types import (
            BooleanType, DateType, DoubleType, FloatType,
            IntegerType, LongType, ShortType, StringType,
            StructField, StructType, TimestampType,
        )

        _TYPE_MAP: dict[str, Any] = {
            "string": StringType(),
            "str": StringType(),
            "integer": IntegerType(),
            "int": IntegerType(),
            "long": LongType(),
            "bigint": LongType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "bool": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "short": ShortType(),
            "smallint": ShortType(),
        }

        fields = []
        for col_def in source_config.schema:
            spark_type = _TYPE_MAP.get(col_def.type.lower(), StringType())
            fields.append(StructField(col_def.name, spark_type, col_def.nullable))
        return StructType(fields)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _write_version(self, sv: SchemaVersion) -> None:
        """Append a schema version row to the Delta registry table."""
        from pyspark.sql import Row

        row = Row(
            source_name=sv.source_name,
            schema_version=sv.version,
            schema_json=sv.schema_json,
            column_count=sv.column_count,
            registered_by=sv.registered_by,
            registered_at=sv.registered_at,
            evolution_mode=sv.evolution_mode,
            change_summary=sv.change_summary,
        )
        try:
            df = self._spark.createDataFrame([row])
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(self._table_fqn)
            )
            self._log.info(
                "Schema version registered",
                source_name=sv.source_name,
                version=sv.version,
                column_count=sv.column_count,
                table=self._table_fqn,
            )
        except Exception as exc:
            self._log.warning(
                "Schema registry write failed — version tracked in memory only",
                source_name=sv.source_name,
                version=sv.version,
                error=str(exc),
            )
