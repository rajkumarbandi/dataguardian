"""ADLS Gen2 connector — reads Parquet, CSV, JSON, Delta, and Avro from Azure Data Lake."""

from __future__ import annotations

# TODO (Milestone 2): Implement ADLSConnector(BaseConnector)
#
# Supported formats: parquet | csv | json | delta | avro
# Connection: ADLS Gen2 via ABFSS URI, authenticated via Databricks secret scope
#
# Key behaviors:
# - Incremental mode: filter by watermark_column > last_processed_value
# - Full refresh mode: read entire source location
# - Handle missing files gracefully (empty DataFrame + warning, not error)
# - Schema inference for unstructured sources (CSV/JSON)
# - Schema enforcement for structured sources (Parquet/Delta)
