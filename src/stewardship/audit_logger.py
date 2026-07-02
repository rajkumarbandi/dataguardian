"""Audit logger — append-only writer for stewardship approval events."""

from __future__ import annotations

# TODO (Milestone 5): Implement AuditLogger
#
# Writes to: dg_{env}.audit.approval_events
# Mode: append-only — records are NEVER updated or deleted
#
# Each audit event captures:
# - event_id (UUID)
# - stewardship_id, entity, batch_id
# - from_status, to_status
# - actor (email), actor_role (STEWARD / SENIOR_OWNER / SYSTEM)
# - comment (nullable)
# - event_timestamp (UTC)
# - record_count (for batch actions)
#
# The audit log is the compliance record. It must be immutable.
# In the Databricks environment, this is enforced by:
# - Delta append-only table mode (ALTER TABLE ... SET TBLPROPERTIES ('delta.appendOnly' = 'true'))
# - Unity Catalog GRANT: no UPDATE/DELETE privileges on the audit schema for any role
