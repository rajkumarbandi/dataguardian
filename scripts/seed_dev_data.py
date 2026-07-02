#!/usr/bin/env python3
"""Seed the DEV environment with sample data for end-to-end pipeline testing.

Writes sample customer records to the Bronze ADLS landing zone so the
full ingestion → stewardship → gold pipeline can be exercised in DEV.

Usage:
    python scripts/seed_dev_data.py --env dev
"""

from __future__ import annotations

# TODO (Milestone 2): Implement after Bronze ingestion is built
#
# Generates synthetic (non-PII) customer records with intentional quality issues:
# - Some records with null required fields (triggers completeness rule failure)
# - Some records with duplicate customer IDs (triggers uniqueness rule failure)
# - Some records with invalid email formats (triggers pattern rule failure)
# - Some records with future dates of birth (triggers range rule failure)
# - Some records with invalid country codes (triggers referential rule failure)
# - The majority of records with no issues (pass all DQ rules → go straight to Gold)
#
# The intentional failures are documented so developers know which stewardship
# records to expect when running the pipeline in DEV.

import sys


def main() -> None:
    print("Dev data seeder — implementation pending Milestone 2")
    sys.exit(0)


if __name__ == "__main__":
    main()
