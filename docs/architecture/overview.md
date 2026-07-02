# Architecture Overview

## Introduction

DataGuardian is an Enterprise AI-Powered Data Stewardship Platform built on Azure Databricks. It solves a fundamental problem in enterprise data engineering: data is technically processed, but business users do not trust it until they can validate it themselves.

DataGuardian provides a structured, auditable workflow for business validation — replacing ad hoc Excel reviews with a governed approval process that gates data promotion to the Gold layer.

---

## Core Design Principles

| Principle | Application |
|---|---|
| **Configuration over Code** | Sources, schemas, quality rules, and workflows are declared in YAML. No SQL control tables. |
| **Separation of Concerns** | Each layer (ingest, standardize, quality, stewardship, publish) is independently deployable and testable. |
| **AI as Optional Enrichment** | AI features are decorators on the base pipeline. The platform functions without them. |
| **Single Source of Truth** | Unity Catalog governs all data assets. Access, lineage, and discovery are centralized. |
| **Environment Parity** | DEV, QA, and PROD run identical code. Only DAB target config differs. |

---

## System Components

### 1. Ingestion Engine
Plugin-based connector framework. Each source system is declared in a YAML config referencing a connector type (`adls`, `jdbc`, `api`). The engine loads the config, instantiates the connector, and lands raw data in the Bronze layer with minimal transformation — preserving source fidelity.

### 2. Schema Registry & Mapper
Maintains canonical schema definitions per entity (e.g., `customer`, `product`, `order`). Maps source-specific column names to canonical names using YAML-declared mappings, optionally enriched by AI-suggested mappings for new or ambiguous sources.

### 3. Data Quality Rule Engine
A pluggable rule engine that evaluates a suite of rules per dataset. Rules are declared in YAML and implemented as Python classes. The engine produces a per-record quality score and a per-dataset summary report. Records below threshold are flagged for stewardship review.

### 4. Stewardship Layer
The core innovation of DataGuardian. A physical Delta table persists records in a formal approval state machine: `PENDING → IN_REVIEW → APPROVED | REJECTED | ESCALATED`. Business users interact with this layer through the Streamlit application. All state transitions are recorded with user identity, timestamp, and optional comments.

### 5. Streamlit Stewardship Application
A Databricks Apps-hosted Streamlit application. Business users see pending records, DQ scores, AI profiling summaries, and can approve, reject, or escalate records individually or in batches. Comments are captured and stored with the audit trail.

### 6. Gold Publisher
Reads only `APPROVED` records from the Stewardship layer and writes them to the Gold Delta table using a merge (upsert) strategy to handle incremental updates safely.

### 7. AI Enrichment Services
Optional services invoked as post-processing steps:
- Schema mapping suggestions (Azure OpenAI)
- Duplicate detection (embedding similarity)
- Data profiling narrative (Azure OpenAI)
- Natural language to SQL (Azure OpenAI)
- Comment summarization for audit reports (Azure OpenAI)

### 8. CI/CD Pipeline
GitHub Actions workflows orchestrate linting, testing, and deployment. Databricks Asset Bundles manage environment-specific resource provisioning. The same artifact is promoted from DEV → QA → PROD without rebuilding.

---

## Data Flow Summary

```
Source System
     │
     │  Connector (ADLS / JDBC / API)
     ▼
Bronze Delta Table          ← raw, partitioned by ingestion_date
     │
     │  Schema mapping, deduplication, standardization
     ▼
Silver Delta Table          ← cleansed, canonical schema, DQ-scored
     │
     │  AI profiling (optional), rule evaluation, flagging
     ▼
Stewardship Delta Table     ← approval state machine
     │                         ↑ Streamlit App reads/writes
     │  APPROVED records only
     ▼
Gold Delta Table            ← trusted, analytics-ready, lineage-tracked
```

---

## Unity Catalog Naming Convention

```
Catalog:  dg_{env}          (e.g., dg_dev, dg_qa, dg_prod)
Schema:   bronze / silver / stewardship / gold / audit
Table:    {source}_{entity} (bronze) | {entity} (silver, gold)
```

**Examples:**
```
dg_dev.bronze.erp_customers
dg_dev.silver.customers
dg_dev.stewardship.customers_pending
dg_dev.gold.customers
dg_dev.audit.approval_events
```

This convention makes environment promotion trivial: the catalog name is injected by the DAB target configuration. No code changes are required.

---

## Related Documents

- [Medallion Architecture Design](medallion-architecture.md)
- [AI Integration Design](ai-integration.md)
- [Security Model](security-model.md)
- [Data Flow Diagram](data-flow.md)
- [ADR-0001: Medallion with Stewardship Layer](../adr/0001-medallion-with-stewardship.md)
