"""JDBC connector — reads from SQL Server, PostgreSQL, MySQL via Spark JDBC."""

from __future__ import annotations

# TODO (Milestone 2): Implement JDBCConnector(BaseConnector)
#
# Supported databases: SQL Server, PostgreSQL, MySQL (via JDBC driver)
# Authentication: username + password from Databricks secret scope
#
# Key behaviors:
# - Incremental mode: WHERE watermark_column > last_processed_value
# - Partition pushdown: use numPartitions + partitionColumn for parallel reads
# - Connection validation: SELECT 1 to test connectivity
# - Never expose JDBC password in logs or exception messages
