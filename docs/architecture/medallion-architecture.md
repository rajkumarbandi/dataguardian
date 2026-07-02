# Medallion Architecture with Stewardship Layer

## Standard Medallion vs DataGuardian Extension

The industry-standard Medallion Architecture defines three layers:

| Layer | Purpose |
|---|---|
| Bronze | Raw ingestion, nothing dropped |
| Silver | Cleansed and standardized |
| Gold | Curated, analytics-ready |

DataGuardian extends this with a **Stewardship Layer** between Silver and Gold:

| Layer | Purpose | DataGuardian Addition |
|---|---|---|
| Bronze | Raw ingestion | Connector framework, schema preservation |
| Silver | Cleansed, standardized | AI schema mapping, DQ scoring |
| **Stewardship** | **Business validation holding zone** | **Approval state machine, Streamlit UI** |
| Gold | Trusted, analytics-ready | Lineage-tracked, merge-safe promotion |

---

## Why a Physical Stewardship Layer?

Several alternative patterns were considered before settling on a physical Delta table:

**Alternative A: Flag column in Silver** — Add an `approval_status` column to the Silver table. Rejected because Silver is a technical layer; mixing business workflow state into it violates separation of concerns. It also makes Silver queries slower and more complex.

**Alternative B: Application-only state in a relational database** — Store approval state in Azure SQL or Cosmos DB. Rejected because it creates a dependency on a separate service, complicates lineage tracking, and removes the benefit of Delta Lake ACID guarantees for state transitions.

**Alternative C: Physical Stewardship Delta table (chosen)** — A dedicated Delta table with approval state columns. Benefits: ACID transactions, time-travel for audit, Unity Catalog lineage, native Spark access from notebooks and the Streamlit app, and zero additional service dependencies.

---

## Bronze Layer Design

**Location:** `dg_{env}.bronze.{source}_{entity}`

**Characteristics:**
- Schema is inferred or declared, never enforced at landing
- All source columns preserved, including unexpected ones
- Partitioned by `ingestion_date` and optionally `source_system`
- Metadata columns added: `_ingestion_timestamp`, `_source_system`, `_source_file`, `_batch_id`
- No records deleted — append-only by default

**Retention:** 90 days (configurable per source in YAML)

---

## Silver Layer Design

**Location:** `dg_{env}.silver.{entity}`

**Characteristics:**
- Canonical schema applied (mapped from source columns via schema registry)
- Null handling, type coercion, and standardization applied
- Deduplication applied using business key defined in schema contract
- Data quality score column added: `_dq_score` (0.0 – 1.0)
- Quality dimension scores: `_dq_completeness`, `_dq_uniqueness`, `_dq_validity`
- Records below `dq_threshold` flagged: `_requires_stewardship = true`

**Merge Strategy:** Upsert on business key to handle incremental loads.

---

## Stewardship Layer Design

**Location:** `dg_{env}.stewardship.{entity}_pending`

**Approval State Machine:**

```
PENDING
   │
   ├──► IN_REVIEW  (steward opens record in UI)
   │        │
   │        ├──► APPROVED   (steward approves)
   │        ├──► REJECTED   (steward rejects with comment)
   │        └──► ESCALATED  (steward escalates to senior owner)
   │
   └──► EXPIRED   (SLA breach — automatic, triggers alert)
```

**Key Columns:**

| Column | Type | Description |
|---|---|---|
| `_stewardship_id` | STRING | Unique ID for this stewardship record |
| `_approval_status` | STRING | PENDING / IN_REVIEW / APPROVED / REJECTED / ESCALATED / EXPIRED |
| `_assigned_steward` | STRING | Email of the assigned business steward |
| `_reviewed_by` | STRING | Email of the reviewer |
| `_review_timestamp` | TIMESTAMP | When status was last changed |
| `_comment` | STRING | Reviewer comment (required for REJECTED/ESCALATED) |
| `_sla_deadline` | TIMESTAMP | When the record expires if not reviewed |
| `_ai_profile_summary` | STRING | AI-generated data quality narrative |
| `_source_batch_id` | STRING | Link back to the originating Bronze batch |

---

## Gold Layer Design

**Location:** `dg_{env}.gold.{entity}`

**Characteristics:**
- Only `APPROVED` records from the Stewardship layer
- Merge (upsert) on business key — idempotent promotion
- Lineage column: `_approved_by`, `_approval_timestamp`, `_stewardship_id`
- Optimized with `OPTIMIZE` and `ZORDER` for analytics query patterns
- Table Properties: owner, data classification, SLA documented as Delta table properties

---

## Audit Layer

**Location:** `dg_{env}.audit.approval_events`

Every state transition in the Stewardship layer emits an audit event record — immutable, append-only. This provides a complete, tamper-evident history of who approved or rejected what, and when.
