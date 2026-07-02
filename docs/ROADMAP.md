# DataGuardian — Development Roadmap

This roadmap defines the milestones for building DataGuardian incrementally. Each milestone is independently releasable and builds on the previous one. Business value is delivered from Milestone 3 onward.

---

## Milestone 0 — Repository Scaffold ✅

**Goal:** Establish enterprise-grade project structure, documentation, and CI/CD skeleton before any application code is written.

**Deliverables:**
- [x] Repository structure with all folders
- [x] Architecture documentation and ADRs
- [x] YAML configuration templates
- [x] Databricks Asset Bundle skeleton
- [x] GitHub Actions CI/CD pipeline stubs
- [x] Development standards (CONTRIBUTING, pyproject.toml, Makefile)
- [x] Professional README

**Duration:** 1 sprint

---

## Milestone 1 — Core Infrastructure

**Goal:** Build the foundational utilities all pipeline stages depend on.

**Deliverables:**
- [ ] `ConfigLoader` — YAML loading with schema validation, environment variable substitution
- [ ] `SparkSessionManager` — Singleton Spark session with Unity Catalog configuration
- [ ] `UnityCatalogClient` — Table creation, schema management, lineage helpers
- [ ] `StructuredLogger` — JSON-structured logging with correlation ID propagation
- [ ] `BaseException` hierarchy for domain errors
- [ ] Unit tests for all utilities
- [ ] `scripts/setup_unity_catalog.py` — Bootstrap catalogs and schemas for a new environment

**Duration:** 1 sprint

---

## Milestone 2 — Ingestion Layer (Bronze)

**Goal:** Ingest data from source systems into the Bronze layer via a plugin-based connector framework.

**Deliverables:**
- [ ] `BaseConnector` abstract class with interface contract
- [ ] `ADLSConnector` — Read Parquet/CSV/JSON from Azure Data Lake Gen2
- [ ] `JDBCConnector` — Read from SQL Server, PostgreSQL, MySQL via JDBC
- [ ] `APIConnector` — Read from REST API sources with pagination support
- [ ] `IngestionEngine` — Orchestrates connector selection, metadata enrichment, Bronze write
- [ ] Schema drift detection — Alert on unexpected new/removed columns
- [ ] Config templates for all connector types
- [ ] Unit tests with mock data
- [ ] Databricks notebook: `bronze_ingestion.py`
- [ ] Example source config: `config/sources/example_erp.yml`

**Duration:** 2 sprints

---

## Milestone 3 — Schema Standardization (Silver)

**Goal:** Map source schemas to canonical entity schemas and produce cleansed Silver records.

**Deliverables:**
- [ ] `SchemaRegistry` — Load and cache canonical schema definitions from YAML
- [ ] `SchemaMapper` — Apply column mappings, type coercions, null handling
- [ ] `DeduplicationEngine` — Exact-key deduplication with configurable business key
- [ ] Silver merge (upsert) logic using Delta Lake MERGE
- [ ] DQ score columns appended to Silver records
- [ ] Unit tests with PySpark test fixtures (using `chispa`)
- [ ] Databricks notebook: `silver_transform.py`
- [ ] Example schema contract: `config/schemas/example_customer.yml`

**Duration:** 2 sprints

---

## Milestone 4 — Data Quality Rule Engine

**Goal:** Build a pluggable, configuration-driven rule engine that evaluates data quality at the record and dataset level.

**Deliverables:**
- [ ] `BaseRule` abstract class
- [ ] `CompletenessRule` — Null/empty value check per column
- [ ] `UniquenessRule` — Duplicate detection on specified column set
- [ ] `RangeRule` — Numeric/date value within expected range
- [ ] `PatternRule` — Regex pattern conformance (e.g., email, phone, postcode)
- [ ] `ReferentialRule` — Value exists in a reference dataset
- [ ] `RuleEngine` — Loads rule suite from YAML, evaluates rules, computes DQ score
- [ ] `QualityReporter` — Writes batch DQ report to audit schema
- [ ] Unit tests for all rules including edge cases (nulls, empty DataFrames, boundary values)
- [ ] Databricks notebook: `dq_checks.py`
- [ ] Example rule suite: `config/quality/example_customer_rules.yml`

**Duration:** 2 sprints

---

## Milestone 5 — Stewardship Layer and Approval State Machine

**Goal:** Build the stewardship Delta table, approval state machine, and audit trail.

**Deliverables:**
- [ ] `StewardshipWriter` — Write DQ-failed records to stewardship table with `PENDING` status
- [ ] `ApprovalEngine` — State machine transitions with ACID Delta writes
- [ ] `AuditLogger` — Append-only audit event writer
- [ ] SLA monitor — Detect `PENDING` records past deadline, transition to `EXPIRED`, raise alert
- [ ] Unit tests for all state transitions including invalid transitions
- [ ] Integration test for full Silver → Stewardship → Gold flow

**Duration:** 2 sprints

---

## Milestone 6 — Streamlit Stewardship Application

**Goal:** Build the business user interface for data validation, deployed as a Databricks App.

**Deliverables:**
- [ ] App authentication via Databricks SSO
- [ ] Dashboard page — pending review count, SLA status, recent activity
- [ ] Data review page — paginated record grid with DQ scores, AI summaries, approve/reject/escalate controls
- [ ] Schema mapping review page — review AI-suggested column mappings
- [ ] Audit trail page — approval history with filters and export
- [ ] Batch approval support — approve/reject entire batches
- [ ] Responsive layout, accessible color scheme
- [ ] Databricks Apps deployment configuration

**Duration:** 3 sprints

---

## Milestone 7 — Gold Publishing with Lineage

**Goal:** Promote approved records to the Gold layer with full lineage tracking.

**Deliverables:**
- [ ] `GoldPublisher` — Merge approved records into Gold Delta table
- [ ] Idempotent promotion logic — re-runnable without duplicates
- [ ] `OPTIMIZE` and `ZORDER` hints for analytics query patterns
- [ ] Delta table properties for ownership, classification, SLA
- [ ] Unity Catalog lineage validation tests
- [ ] Databricks notebook: `gold_publish.py`

**Duration:** 1 sprint

---

## Milestone 8 — AI Enrichment Services

**Goal:** Add optional AI-powered enrichment features as decorators on the core pipeline.

**Deliverables:**
- [ ] `AISchemaMappingService` — GPT-4o schema mapping suggestions for unmapped columns
- [ ] `DuplicateDetectionService` — Embedding-based fuzzy duplicate detection
- [ ] `ProfilingSummaryService` — GPT-4o narrative summary for stewardship UI
- [ ] `NLSQLService` — Natural language to SQL for business query interface
- [ ] `CommentSummaryService` — Audit report comment summarization
- [ ] PII field exclusion from all AI prompts
- [ ] AI availability health check and circuit breaker
- [ ] All services disabled in unit test environments
- [ ] Integration tests with Azure OpenAI (QA environment only)

**Duration:** 3 sprints

---

## Milestone 9 — CI/CD Pipeline and Asset Bundle Completion

**Goal:** Complete the GitHub Actions CI/CD pipeline and full Databricks Asset Bundle configuration.

**Deliverables:**
- [ ] GitHub Actions `ci.yml` — lint, type-check, unit tests on every PR
- [ ] GitHub Actions `deploy-dev.yml` — auto-deploy to DEV on merge to `main`
- [ ] GitHub Actions `deploy-qa.yml` — manual-approval deploy to QA on release tag
- [ ] GitHub Actions `deploy-prod.yml` — manual-approval deploy to PROD after QA sign-off
- [ ] Complete DAB job definitions for all pipeline stages
- [ ] DAB cluster configurations for all environments
- [ ] Secret scope references in all DAB configs (no hardcoded credentials)
- [ ] Bundle validation in CI
- [ ] `scripts/validate_config.py` — validate all YAML configs in CI

**Duration:** 2 sprints

---

## Milestone 10 — Observability, Monitoring, and Alerting

**Goal:** Add production-grade observability so the platform can be operated reliably.

**Deliverables:**
- [ ] Structured JSON logging throughout all pipeline stages with correlation IDs
- [ ] Pipeline run metrics written to `dg_{env}.audit.pipeline_metrics`
- [ ] Databricks Workflow failure alerts via email/Teams/PagerDuty
- [ ] SLA breach alerts for stewardship records
- [ ] DQ score trend dashboard (Databricks SQL or Streamlit)
- [ ] Data freshness monitoring — alert if Gold table not updated within SLA window
- [ ] Runbook for each alert type in `docs/runbooks/`

**Duration:** 2 sprints

---

## Post-Roadmap: Future Considerations

These items are deliberately out of scope for the initial roadmap but worth tracking:

- **Multi-tenant support** — Multiple business units with isolated stewardship workflows
- **Data Product catalog** — Expose Gold tables as subscribable data products
- **dbt integration** — Allow dbt models to consume Gold layer with quality guarantees
- **Real-time ingestion** — Structured Streaming for near-real-time Bronze landing
- **Cost governance** — Per-source, per-job cost attribution and budget alerting
