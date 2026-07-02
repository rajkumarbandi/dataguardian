# ADR-0001: Extend Medallion Architecture with a Physical Stewardship Layer

**Status:** Accepted
**Date:** 2026-06-29
**Author:** Platform Architecture Team

---

## Context

DataGuardian's core value proposition is governed business validation of curated data before it reaches the Gold layer. The standard Medallion Architecture (Bronze → Silver → Gold) does not natively model a business validation step. Several approaches were considered for where and how to represent approval state.

---

## Decision

We will add a dedicated **Stewardship Layer** as a physical Delta table between Silver and Gold. Records requiring business validation are written to `dg_{env}.stewardship.{entity}_pending` and remain there until a business steward approves or rejects them via the Streamlit application. Only `APPROVED` records are promoted to Gold.

---

## Alternatives Considered

### Option A: Approval flag column in Silver table
Add an `approval_status` column to the Silver table. Silver records remain as-is; the flag drives promotion logic.

**Rejected because:**
- Silver is a technical layer. Mixing business workflow state violates separation of concerns.
- Silver tables may be large; adding a state column that changes over time inflates data volume and complicates incremental processing.
- Reading only unapproved records from Silver requires filtering a potentially large table on every stewardship job run.

### Option B: External relational database for approval state
Store approval state in Azure SQL Database or Cosmos DB. The pipeline queries this database to determine which records to promote.

**Rejected because:**
- Introduces a new service dependency. If the database is unavailable, the pipeline is blocked.
- Unity Catalog cannot provide lineage across a non-Databricks data store.
- Increases operational overhead (another service to secure, monitor, and maintain).
- Breaks the "data lives in Delta Lake" principle.

### Option C: Physical Stewardship Delta table (chosen)
A dedicated Delta table with approval state columns. The Streamlit application reads and writes this table directly.

**Accepted because:**
- ACID transactions guarantee that state changes are atomic and consistent.
- Delta Lake time-travel provides a full history of every record's state changes for audit.
- Unity Catalog tracks lineage from Silver → Stewardship → Gold automatically.
- No additional service dependencies.
- The Streamlit application accesses the table via standard Spark DataFrame operations.
- Familiar operational model — same monitoring, backup, and access control as other Delta tables.

---

## Consequences

**Positive:**
- Clear physical separation between technical data quality (Silver) and business validation (Stewardship)
- Full ACID audit trail via Delta Lake versioning
- Unity Catalog lineage across all four layers
- Stewardship layer is independently queryable for reporting and SLA monitoring

**Negative:**
- Adds a storage layer — Gold data exists in both Stewardship (approved) and Gold (promoted). Small additional cost.
- Gold promotion requires an additional job step to read from Stewardship and merge into Gold.

**Neutral:**
- Requires a merge (upsert) pattern for Gold promotion to handle incremental loads safely. This is standard practice with Delta Lake.
