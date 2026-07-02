"""REST API connector — reads paginated JSON responses and converts to Spark DataFrame."""

from __future__ import annotations

# TODO (Milestone 2): Implement APIConnector(BaseConnector)
#
# Supports: REST APIs returning JSON arrays or objects with pagination
# Authentication: Bearer token, API key via Databricks secret scope
#
# Key behaviors:
# - Configurable pagination: cursor-based or page/offset-based
# - Rate limit handling: respect Retry-After headers
# - Incremental: filter by since_timestamp query parameter if supported
# - Converts nested JSON to flattened Spark DataFrame
# - Max pages limit to prevent runaway reads
