"""Approval engine — implements the stewardship state machine with ACID Delta writes."""

from __future__ import annotations

# TODO (Milestone 5): Implement ApprovalEngine
#
# Implements the state machine:
#   PENDING → IN_REVIEW → APPROVED | REJECTED | ESCALATED
#   ESCALATED → APPROVED | REJECTED (by Senior Data Owner)
#   PENDING → EXPIRED (by SLA monitor)
#
# All transitions:
# - Are validated against the allowed transition table
# - Raise InvalidStateTransitionError for invalid transitions
# - Write the new status to the stewardship Delta table (MERGE on stewardship_id)
# - Emit an audit event to audit.approval_events (append-only)
# - Are atomic — both the stewardship update and audit write succeed or both fail
#
# Required fields for state transitions:
# - REJECTED: comment is mandatory
# - ESCALATED: comment and escalation_target are mandatory
# - APPROVED, IN_REVIEW, EXPIRED: comment is optional
