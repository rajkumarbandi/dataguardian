"""Gold publisher — merges APPROVED stewardship records into the Gold Delta layer."""

from __future__ import annotations

# TODO (Milestone 7): Implement GoldPublisher
#
# Responsibilities:
# - Read records with _approval_status = 'APPROVED' and _promoted = false
# - For each entity, merge (upsert) records into {catalog}.gold.{entity}
#   using the business key defined in the schema contract
# - Write promotion metadata to Gold records:
#   _approved_by, _approval_timestamp, _stewardship_id
# - Mark processed stewardship records as _promoted = true (MERGE on stewardship_id)
# - Apply OPTIMIZE to Gold table after large promotion batches
# - Operation is idempotent: re-running will not create duplicates
#
# The MERGE pattern (Delta Lake):
#   MERGE INTO gold.customers AS target
#   USING approved_records AS source
#   ON target.customer_id = source.customer_id
#   WHEN MATCHED THEN UPDATE SET *
#   WHEN NOT MATCHED THEN INSERT *
