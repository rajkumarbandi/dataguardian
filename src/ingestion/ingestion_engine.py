"""
Bronze ingestion engine for the DataGuardian platform.

``IngestionEngine`` is the single orchestration point for all Bronze loads.
Given a ``SourceConfig``, it:

1. Selects the correct connector from the registry.
2. Validates the source connection.
3. Reads the raw ``DataFrame``.
4. Injects four standard metadata columns.
5. Ensures the target schema exists in Unity Catalog.
6. Writes the enriched DataFrame to a Delta Bronze table.

Connector registry pattern
--------------------------
``_CONNECTOR_REGISTRY`` maps connector type strings to their classes.
Adding a new connector (e.g. ``JDBCConnector``) requires only one line here —
the engine itself never needs to change.

Dependency injection
--------------------
``SparkSession``, ``ConfigLoader``, ``UnityCatalogClient``, and the logger
are all injected at construction time.  The engine never creates these itself.
This makes the engine trivially unit-testable via mock injection.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyspark.sql.functions as F

from src.common.exceptions import ConfigurationError, ConnectorError
from src.common.logger import DataGuardianLogger, get_pipeline_logger
from src.ingestion.connectors.csv_connector import CSVConnector

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from src.common.models import SourceConfig
    from src.common.unity_catalog_client import UnityCatalogClient
    from src.ingestion.base_connector import BaseConnector


# ---------------------------------------------------------------------------
# Connector registry
# ---------------------------------------------------------------------------

# Maps the ``connector.type`` YAML value to the concrete connector class.
# New connectors are registered here — the engine itself never changes.
_CONNECTOR_REGISTRY: dict[str, type[BaseConnector]] = {
    "csv": CSVConnector,
}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class IngestionResult:
    """
    Captures the outcome of a single Bronze ingestion run.

    All fields are populated by ``IngestionEngine.run()`` before it returns,
    whether the run succeeded or failed.
    """

    source_name: str
    batch_id: str
    target_table: str
    rows_written: int = 0
    success: bool = False
    error_message: str = ""
    metadata: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# IngestionEngine
# ---------------------------------------------------------------------------


class IngestionEngine:
    """
    Orchestrates the end-to-end Bronze ingestion for a single source.

    Parameters
    ----------
    spark:
        Active ``SparkSession`` — injected, never created internally.
    uc_client:
        ``UnityCatalogClient`` configured for the target catalog.
    logger:
        Optional pre-bound logger.  If omitted, one is created using the
        source name and a generated ``batch_id`` as context.

    Example
    -------
    ::

        engine = IngestionEngine(spark=spark, uc_client=uc_client)
        result = engine.run(source_config)
        print(f"Wrote {result.rows_written} rows to {result.target_table}")
    """

    def __init__(
        self,
        spark: SparkSession,
        uc_client: UnityCatalogClient,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._spark = spark
        self._uc = uc_client
        self._logger = logger

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, source_config: SourceConfig) -> IngestionResult:
        """
        Execute a full Bronze ingestion cycle for ``source_config``.

        Steps
        -----
        1. Build a ``batch_id`` (UUID4) to correlate all log records.
        2. Select and validate the connector.
        3. Read the raw DataFrame.
        4. Add the four standard metadata columns.
        5. Ensure the target Delta schema exists.
        6. Write to Delta in append (or overwrite) mode.

        Returns
        -------
        IngestionResult
            Populated result object.  ``success=True`` on completion;
            ``success=False`` with ``error_message`` set on any failure.
        """
        batch_id = str(uuid.uuid4())
        target = source_config.target
        result = IngestionResult(
            source_name=source_config.name,
            batch_id=batch_id,
            target_table=target.full_table_name,
        )

        log = self._logger or get_pipeline_logger(
            source_system=source_config.system,
            entity=source_config.name,
            batch_id=batch_id,
        )

        log.info(
            "Bronze ingestion started",
            source=source_config.name,
            target=target.full_table_name,
            load_type=target.load_type,
        )

        try:
            connector = self._build_connector(source_config, log)
            self._validate_connector(connector, log)

            raw_df = connector.read()
            enriched_df = self._add_metadata(raw_df, source_config, batch_id)

            self._uc.create_schema_if_not_exists(target.schema)
            self._write_bronze(enriched_df, source_config)

            result.rows_written = enriched_df.count()
            result.success = True
            log.info(
                "Bronze ingestion completed",
                rows_written=result.rows_written,
                target=target.full_table_name,
            )

        except ConnectorError as exc:
            result.error_message = str(exc)
            log.error("Connector error during ingestion", error=str(exc))

        except ConfigurationError as exc:
            result.error_message = str(exc)
            log.error("Configuration error during ingestion", error=str(exc))

        except Exception as exc:
            result.error_message = f"Unexpected error: {exc}"
            log.exception("Unexpected error during ingestion", error=str(exc))

        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_connector(
        self,
        source_config: SourceConfig,
        log: DataGuardianLogger,
    ) -> BaseConnector:
        connector_type = source_config.connector.type
        connector_class = _CONNECTOR_REGISTRY.get(connector_type)
        if connector_class is None:
            registered = sorted(_CONNECTOR_REGISTRY)
            raise ConfigurationError(
                f"No connector registered for type {connector_type!r}. "
                f"Registered types: {registered}. "
                "Add the connector to _CONNECTOR_REGISTRY in ingestion_engine.py."
            )
        return connector_class(
            config=source_config,
            spark=self._spark,
            logger=log,
        )

    @staticmethod
    def _validate_connector(connector: BaseConnector, log: DataGuardianLogger) -> None:
        log.debug("Validating source connectivity", connector=connector.connector_type)
        connector.validate_connection()

    @staticmethod
    def _add_metadata(
        df: DataFrame,
        source_config: SourceConfig,
        batch_id: str,
    ) -> DataFrame:
        """
        Inject the four standard Bronze metadata columns.

        All metadata column names begin with ``_`` to distinguish them from
        source columns and to prevent naming collisions.

        Columns added
        -------------
        ``_ingestion_timestamp``  — exact moment the row was written (UTC)
        ``_source_system``        — system identifier from the source YAML
        ``_batch_id``             — UUID4 unique to this ingestion run
        ``_load_date``            — date portion of the ingestion timestamp
                                   (used as the Delta partition column)
        """
        return (
            df.withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_system", F.lit(source_config.system))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_load_date", F.current_date())
        )

    def _write_bronze(
        self,
        df: DataFrame,
        source_config: SourceConfig,
    ) -> None:
        """Write ``df`` to the Bronze Delta table defined in ``source_config``."""
        target = source_config.target
        partition_col = target.partition_by

        writer = (
            df.write.format("delta")
            .mode(target.load_type)
            .option("mergeSchema", "true")
        )

        if partition_col and partition_col in df.columns:
            writer = writer.partitionBy(partition_col)

        writer.saveAsTable(target.full_table_name)
