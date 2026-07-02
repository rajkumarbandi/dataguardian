"""
CSV source connector for the DataGuardian platform.

Reads delimited files from a local path or an ADLS Gen2 ``abfss://`` URI.
All options (delimiter, header, encoding, quote character) are driven by the
source YAML — no connector-level defaults are hardcoded.

Schema handling
---------------
* When the source YAML declares an explicit ``schema`` list, the connector
  builds a ``StructType`` from it and passes it to the Spark reader.  This
  prevents schema inference from silently changing between runs and avoids
  an extra Spark job for schema discovery.
* All columns are read as **nullable** regardless of the YAML declaration.
  Nullability is enforced downstream in the Data Quality layer, not during
  Bronze ingestion (raw data is stored as-is).
* When no schema is declared, the reader falls back to ``inferSchema=true``
  so ad-hoc sources work without a schema contract.

Path validation
---------------
On ADLS (``abfss://`` paths), path existence cannot be checked from the driver
without a full Spark read — ``validate_connection()`` skips the check and
returns ``True`` with a warning.  For local paths it checks existence directly.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.exceptions import ConnectorError
from src.ingestion.base_connector import BaseConnector

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from src.common.logger import DataGuardianLogger
    from src.common.models import ColumnDefinition, SourceConfig


# Maps YAML type strings to PySpark types.
# All Bronze columns are nullable — nullability is enforced in DQ, not here.
_SPARK_TYPE_MAP: dict[str, object] = {
    "string": StringType(),
    "str": StringType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "decimal": DecimalType(38, 10),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


class CSVConnector(BaseConnector):
    """
    Reads a CSV/TSV file into a Spark ``DataFrame``.

    Connector options (set in ``connector.options`` within the source YAML)
    ----------
    ``header``
        ``"true"`` / ``"false"`` — whether the first row is a header.
        Default: ``"true"``.
    ``delimiter``
        Column separator character.  Default: ``","``.
    ``encoding``
        File encoding.  Default: ``"UTF-8"``.
    ``quote``
        Quote character.  Default: ``'"'``.
    ``multiLine``
        ``"true"`` to support multi-line quoted fields.  Default: ``"false"``.
    ``nullValue``
        String to interpret as ``null``.  Default: ``""``.
    ``dateFormat``
        Java ``SimpleDateFormat`` pattern for date columns.
    ``timestampFormat``
        Java ``SimpleDateFormat`` pattern for timestamp columns.
    """

    @property
    def connector_type(self) -> str:
        return "csv"

    def validate_connection(self) -> bool:
        """
        Check that the source path is accessible.

        For ADLS paths, returns ``True`` with a warning (driver cannot list
        ADLS without a Spark operation).  For local paths, verifies existence.
        """
        location = self.config.connector.location
        if location.startswith("abfss://") or location.startswith("wasbs://"):
            self.log.warning(
                "ADLS path connectivity cannot be verified from the driver; "
                "skipping validate_connection",
                path=location,
            )
            return True

        # Expand environment variables in local paths (e.g. ${ADLS_ROOT})
        resolved = os.path.expandvars(location)
        if not os.path.exists(resolved):
            self._raise(
                f"CSV source path does not exist: {resolved!r}. "
                "Check the 'connector.location' field in your source YAML."
            )
        return True

    def read(self) -> DataFrame:
        """
        Read the CSV file and return a raw ``DataFrame``.

        Applies the declared schema when present; falls back to schema
        inference otherwise.
        """
        location = self.config.connector.location
        options = self.config.connector.options

        # Merge caller options with safe defaults — caller wins on conflict
        reader_options: dict[str, str] = {
            "header": "true",
            "delimiter": ",",
            "encoding": "UTF-8",
            "quote": '"',
            "multiLine": "false",
            "nullValue": "",
            **options,
        }

        self.log.info("Reading CSV source", path=location, options=reader_options)

        reader = self.spark.read.format("csv").options(**reader_options)

        if self.config.schema:
            spark_schema = self._build_spark_schema(self.config.schema)
            reader = reader.schema(spark_schema)
        else:
            reader = reader.option("inferSchema", "true")

        try:
            df = reader.load(location)
        except Exception as exc:
            self._raise(
                f"Failed to read CSV from {location!r}: {exc}",
                cause=exc,
            )

        self.log.info(
            "CSV read completed",
            path=location,
            num_columns=len(df.columns),
        )
        return df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_spark_schema(columns: list[ColumnDefinition]) -> StructType:
        """
        Convert a list of ``ColumnDefinition`` objects into a ``StructType``.

        All fields are declared nullable regardless of the YAML setting —
        Bronze is a raw landing zone; nullability is enforced by Data Quality.
        """
        fields: list[StructField] = []
        for col in columns:
            spark_type = _SPARK_TYPE_MAP.get(col.type.lower())
            if spark_type is None:
                raise ConnectorError(
                    f"Unsupported column type {col.type!r} for column {col.name!r}. "
                    f"Supported types: {sorted(_SPARK_TYPE_MAP)}"
                )
            fields.append(StructField(col.name, spark_type, nullable=True))  # type: ignore[arg-type]
        return StructType(fields)
