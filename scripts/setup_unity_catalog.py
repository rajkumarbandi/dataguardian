#!/usr/bin/env python3
"""Bootstrap Unity Catalog structure for a new DataGuardian environment.

Creates catalogs, schemas, and sets initial table properties.
Must be run with a Databricks-authenticated Spark session.

Usage:
    # Run as a Databricks notebook or with databricks-connect:
    python scripts/setup_unity_catalog.py --env dev --catalog dg_dev
"""

from __future__ import annotations

# TODO (Milestone 1): Implement when UnityCatalogClient is built
#
# This script will:
# 1. Create the catalog if it doesn't exist: CREATE CATALOG IF NOT EXISTS dg_{env}
# 2. Create schemas: bronze, silver, stewardship, gold, audit
# 3. Set catalog and schema comments for discoverability
# 4. Grant appropriate permissions to service principals and user groups
# 5. Apply initial table properties (delta.appendOnly on audit schema)
# 6. Log all actions taken

import sys


def main() -> None:
    print("Unity Catalog setup script — implementation pending Milestone 1")
    print("See docs/runbooks/deployment.md for manual setup instructions.")
    sys.exit(0)


if __name__ == "__main__":
    main()
