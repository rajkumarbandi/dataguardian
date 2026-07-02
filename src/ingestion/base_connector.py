"""
Abstract base class for all DataGuardian source connectors.

Every connector (CSV, JDBC, REST API, SFTP, Kafka …) must subclass
``BaseConnector`` and implement the three abstract members.  The ingestion
engine discovers connectors through the registry in ``ingestion_engine.py``
rather than importing concrete classes directly, so adding a new connector
never requires changing the engine.

Design contract
---------------
* Connectors are **read-only** — they must never modify, rename, or drop
  columns in the source data.
* All column modifications (metadata injection, type casting, renaming) happen
  in the ingestion engine, not in connectors.
* Errors must be raised as ``ConnectorError``, not bare ``Exception`` or
  ``RuntimeError``, so the engine can distinguish connector failures from
  infrastructure failures.
* The SparkSession is injected at construction time — connectors never call
  ``SparkSession.builder`` themselves.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from src.common.exceptions import ConnectorError
from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from src.common.models import SourceConfig


class BaseConnector(ABC):
    """
    Abstract interface for a DataGuardian source connector.

    Parameters
    ----------
    config:
        The validated ``SourceConfig`` for this source, parsed from YAML.
    spark:
        An active ``SparkSession`` to use for all Spark operations.
    logger:
        Optional pre-bound ``DataGuardianLogger``.  When omitted, a default
        logger is created using the connector type and source name as context.

    Subclassing
    -----------
    Implement ``connector_type``, ``validate_connection()``, and ``read()``.
    Call ``super().__init__()`` to set up ``self.config``, ``self.spark``,
    and ``self.log``.

    ::

        class MyConnector(BaseConnector):
            @property
            def connector_type(self) -> str:
                return "mytype"

            def validate_connection(self) -> bool:
                ...

            def read(self) -> DataFrame:
                ...
    """

    def __init__(
        self,
        config: SourceConfig,
        spark: SparkSession,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self.config = config
        self.spark = spark
        self.log: DataGuardianLogger = (
            logger.bind(
                connector=self.connector_type,
                source=config.name,
            )
            if logger
            else get_logger(
                name=f"dataguardian.connector.{self.connector_type}",
                source=config.name,
                connector=self.connector_type,
            )
        )

    # ------------------------------------------------------------------
    # Abstract interface — subclasses must implement all three
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def connector_type(self) -> str:
        """
        Short identifier for this connector type.

        Must match the ``type`` field in the source YAML connector block
        and the key used to register the class in the connector registry.

        Examples: ``"csv"``, ``"jdbc"``, ``"api"``, ``"sftp"``, ``"kafka"``
        """

    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Verify that the source is reachable before reading begins.

        Return ``True`` if the connection is healthy.  Raise
        ``ConnectorError`` with a descriptive message on failure rather than
        returning ``False`` — ``False`` is reserved for cases where
        reachability cannot be determined (e.g. the source is a stream).

        This method is called by the ingestion engine before ``read()``.
        A ``ConnectorError`` here aborts the run without attempting a read.
        """

    @abstractmethod
    def read(self) -> DataFrame:
        """
        Read data from the source and return a raw, unmodified ``DataFrame``.

        Guarantees
        ----------
        * All source columns are preserved exactly as received.
        * No metadata columns are added here — that is the engine's job.
        * Schema enforcement (casting to declared types) is done here when
          an explicit schema is configured; otherwise, infer the schema.

        Raises
        ------
        ConnectorError
            On any failure to read from the source.
        """

    # ------------------------------------------------------------------
    # Shared helpers available to all subclasses
    # ------------------------------------------------------------------

    def _raise(self, message: str, cause: Exception | None = None) -> None:
        """
        Log and raise a ``ConnectorError``.

        Centralising this call ensures that every connector failure is logged
        at ERROR level before the exception propagates to the engine.
        """
        self.log.error(message, connector=self.connector_type, source=self.config.name)
        raise ConnectorError(message) from cause
