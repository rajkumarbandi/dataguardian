# Data Flow

## End-to-End Pipeline Flow

This document traces the journey of a record from source system to Gold layer, including all decision points and side effects.

---

## Stage 1: Ingestion (Bronze Landing)

**Trigger:** Databricks Workflow job scheduled per source system (configurable cron in YAML)

**Steps:**
1. `ConfigLoader` reads the source YAML (`config/sources/{source}.yml`)
2. `IngestionEngine` instantiates the appropriate connector (`adls`, `jdbc`, or `api`)
3. Connector reads data from the source location
4. Metadata columns are appended: `_ingestion_timestamp`, `_source_system`, `_source_file`, `_batch_id`
5. Data is written to `dg_{env}.bronze.{source}_{entity}` partitioned by `ingestion_date`
6. Batch metadata is recorded in `dg_{env}.audit.ingestion_batches`

**Outputs:** Bronze Delta table, ingestion audit record

---

## Stage 2: Standardization (Silver Transformation)

**Trigger:** Downstream job in the Databricks Workflow, triggered after Bronze success

**Steps:**
1. `SchemaRegistry` loads the canonical schema for the entity (`config/schemas/{entity}.yml`)
2. `SchemaMapper` applies column renames and type coercions from the source mapping
3. For unmapped columns: AI suggestion invoked (if enabled) or column flagged as `_unmapped`
4. `DeduplicationEngine` applies exact-match deduplication on the declared business key
5. If duplicate detection AI is enabled: fuzzy pairs are identified and written to `dg_{env}.stewardship.duplicate_candidates` for review
6. Cleansed records are merged (upserted) into `dg_{env}.silver.{entity}`

**Outputs:** Silver Delta table (upserted), optional duplicate candidates table

---

## Stage 3: Quality Evaluation

**Trigger:** Runs as a step within the Silver transformation job

**Steps:**
1. `QualityRuleEngine` loads the rule suite from `config/quality/{entity}_rules.yml`
2. Each rule is evaluated against the DataFrame — producing a per-record pass/fail result
3. Per-record DQ score is computed as weighted average of rule results
4. Records below `dq_threshold` (configurable) receive `_requires_stewardship = true`
5. A batch-level DQ report is written to `dg_{env}.audit.dq_reports`
6. Records requiring stewardship are written to `dg_{env}.stewardship.{entity}_pending` with status `PENDING`

**Outputs:** DQ-annotated Silver records, stewardship pending records, DQ report

---

## Stage 4: AI Profiling (Optional)

**Trigger:** Asynchronous enrichment job, runs after stewardship records are created

**Steps:**
1. For each batch of pending stewardship records:
   - DQ statistics are computed (null rates, distinct counts, range outliers)
   - Statistics are formatted into an AI prompt
   - Azure OpenAI GPT-4o returns a plain-language profiling summary
2. Summary is written to the `_ai_profile_summary` column in the stewardship table
3. Notification is sent to the assigned steward(s)

**Outputs:** AI profiling summaries in Stewardship table, steward notification

---

## Stage 5: Business Validation (Stewardship UI)

**Actor:** Business steward via Streamlit application

**Steps:**
1. Steward logs into the Streamlit app (authenticated via Databricks SSO)
2. App reads `PENDING` records for datasets assigned to this steward
3. Steward reviews records, DQ scores, AI profiling summaries
4. For each record (or batch), steward selects: `APPROVE`, `REJECT`, or `ESCALATE`
5. Status is updated in the Stewardship Delta table — ACID transaction
6. State transition event is written to `dg_{env}.audit.approval_events`
7. SLA monitor checks for `PENDING` records past their deadline — transitions to `EXPIRED` and alerts

**Outputs:** Updated stewardship records, audit events

---

## Stage 6: Gold Promotion

**Trigger:** Scheduled job or event-triggered after stewardship review window

**Steps:**
1. `GoldPublisher` reads all records with `_approval_status = 'APPROVED'` that have not yet been promoted
2. Records are merged (upserted) into `dg_{env}.gold.{entity}` on business key
3. Promotion metadata written: `_approved_by`, `_approval_timestamp`, `_stewardship_id`
4. Stewardship records updated to `_promoted = true`
5. Unity Catalog lineage is automatically tracked via the Spark job

**Outputs:** Gold Delta table (upserted), updated stewardship records

---

## Error Handling and Retry

| Stage | Failure Type | Behavior |
|---|---|---|
| Ingestion | Source unavailable | Job retries 3x with exponential backoff; alert on final failure |
| Ingestion | Schema drift detected | Records flagged; DQ rule `schema_conformance` fails; routed to stewardship |
| Silver | Unmapped columns | Flagged; AI suggestion attempted; surfaced in stewardship UI |
| DQ Evaluation | Rule exception | Rule marked as `ERROR`; contributes 0 to score; alert raised |
| AI Enrichment | API unavailable | Step skipped; `_ai_profile_summary = NULL`; pipeline continues |
| Gold Promotion | Merge conflict | Job fails; alert raised; manual investigation required |
