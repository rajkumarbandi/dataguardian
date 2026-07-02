# Stewardship Workflow Design

## Overview

The stewardship workflow is the business validation layer of DataGuardian. It provides a governed, auditable process for business users to review data quality-flagged records and decide whether to approve them for Gold promotion.

---

## Actors

| Actor | Role |
|---|---|
| **Data Steward** | Business domain expert responsible for validating data accuracy |
| **Senior Data Owner** | Escalation point for disputed or complex records |
| **Data Engineer** | Monitors pipeline health; resolves technical DQ issues |
| **Platform Admin** | Manages steward assignments and SLA configuration |

---

## Approval State Machine

```
                  ┌─────────────────────────────────────┐
                  │                                      │
                  ▼                                      │
              PENDING ──────────────────────► EXPIRED (SLA breach)
                  │
                  │ Steward opens record
                  ▼
            IN_REVIEW
           /          \
          /            \
         ▼              ▼
     APPROVED        REJECTED ─── (requires comment)
                         │
                    ESCALATED ─── (requires comment + escalation target)
                         │
                    [Senior Data Owner reviews]
                        / \
                       /   \
                      ▼     ▼
                  APPROVED REJECTED
```

**Valid transitions:**

| From | To | Actor | Condition |
|---|---|---|---|
| PENDING | IN_REVIEW | Data Steward | Steward opens the record |
| IN_REVIEW | APPROVED | Data Steward | Steward approves |
| IN_REVIEW | REJECTED | Data Steward | Steward rejects; comment required |
| IN_REVIEW | ESCALATED | Data Steward | Steward escalates; comment + target required |
| ESCALATED | APPROVED | Senior Data Owner | Owner approves |
| ESCALATED | REJECTED | Senior Data Owner | Owner rejects; comment required |
| PENDING | EXPIRED | System (SLA monitor) | SLA deadline passed |

**Invalid transitions** — any transition not listed above — raise an `InvalidStateTransitionError` and are not applied.

---

## SLA Configuration

Per-entity SLA configured in the source YAML:

```yaml
stewardship:
  approvers:
    - "data.steward.finance@company.com"
    - "data.steward.ops@company.com"
  escalation_contact: "data.owner.finance@company.com"
  sla_hours: 48          # hours before PENDING → EXPIRED
  notify_on_pending: true
  notify_on_expiry: true
```

---

## Streamlit Application Pages

### Dashboard
- Count of records by status (PENDING, IN_REVIEW, EXPIRED)
- SLA status indicators — records approaching deadline highlighted in amber
- Records past deadline highlighted in red
- Recent approval activity feed
- DQ trend chart for the current user's assigned entities

### Data Review
- Paginated table of PENDING and IN_REVIEW records
- Per-record view showing all column values, DQ score, DQ dimension breakdown, failed rule list, and AI profiling summary
- Approve / Reject / Escalate action buttons
- Batch action: select multiple records and approve/reject the batch
- Comment field (free text, required for REJECT and ESCALATE)
- Record history — all prior state transitions for the record

### Schema Mapping Review
- AI-suggested column mappings awaiting confirmation
- Source column name, sample values, suggested canonical column, confidence score, AI explanation
- Confirm / Override / Reject mapping actions
- Confirmed mappings are written back to the YAML config (PR or direct commit based on permission)

### Audit Trail
- Filterable history of all approval events
- Filters: entity, status, steward, date range
- Export to CSV for compliance reporting
- Summary metrics: approval rate, rejection rate, average time-to-review

---

## Notification Design

Notifications are sent at the following events:

| Event | Recipients | Channel |
|---|---|---|
| Records enter PENDING | Assigned stewards | Email (configurable) |
| Record SLA approaching (75% elapsed) | Assigned stewards | Email |
| Record EXPIRED | Assigned stewards + escalation contact | Email + alert |
| Record ESCALATED | Escalation contact | Email |

Notification configuration is in `config/environments/{env}.yml`. In DEV, notifications are disabled by default.

---

## Audit Trail Schema

`dg_{env}.audit.approval_events` — append-only, immutable:

| Column | Type | Description |
|---|---|---|
| `event_id` | string | UUID for this event |
| `stewardship_id` | string | FK to stewardship record |
| `entity` | string | Entity name |
| `from_status` | string | Previous approval status |
| `to_status` | string | New approval status |
| `actor` | string | Email of the actor |
| `actor_role` | string | STEWARD / SENIOR_OWNER / SYSTEM |
| `comment` | string | Reviewer comment (null if not provided) |
| `event_timestamp` | timestamp | UTC timestamp of state change |
| `batch_id` | string | Source batch identifier |
| `record_count` | integer | Number of records affected (for batch actions) |
