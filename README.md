# DataGuardian

> **Building Trust in Enterprise Data**

[![CI](https://github.com/rajkumarbandi/dataguardian/actions/workflows/ci.yml/badge.svg)](https://github.com/rajkumarbandi/dataguardian/actions/workflows/ci.yml)
[![Deploy DEV](https://github.com/rajkumarbandi/dataguardian/actions/workflows/deploy-dev.yml/badge.svg)](https://github.com/rajkumarbandi/dataguardian/actions/workflows/deploy-dev.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://img.shields.io/badge/mypy-checked-blue)](https://mypy-lang.org/)
[![Azure Databricks](https://img.shields.io/badge/platform-Azure%20Databricks-FF3621.svg)](https://azure.microsoft.com/en-us/products/databricks)
[![Delta Lake](https://img.shields.io/badge/storage-Delta%20Lake-00ADD8.svg)](https://delta.io/)

---

## Table of Contents

- [The Problem](#the-problem)
- [What DataGuardian Does](#what-dataguardian-does)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Configuration Philosophy](#configuration-philosophy)
- [AI Capabilities](#ai-capabilities)
- [Security](#security)
- [CI/CD Pipeline](#cicd-pipeline)
- [Development Roadmap](#development-roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## The Problem

Modern enterprises receive data from dozens of heterogeneous business systems — ERP, CRM, billing, logistics, HR — each with different schemas, naming conventions, data types, and quality issues.

Data engineers build ingestion pipelines and curate this data through a Medallion Architecture. But the last mile of trust is broken:

**Business users still validate curated data manually — through Excel exports, email threads, and recurring meetings — before they trust it enough to use it.**

This creates:
- Delayed decision-making as validation cycles stretch across days or weeks
- Undocumented approval history with no audit trail
- Inconsistent standards as individual stewards apply their own judgement
- Data published to Gold before it is genuinely trusted
- No mechanism to reject records, escalate disputes, or enforce SLAs

---

## What DataGuardian Does

DataGuardian is an **Enterprise AI-Powered Data Stewardship Platform** built on Azure Databricks.

It introduces a governed, auditable business validation layer between the Silver and Gold tiers of the Medallion Architecture. Business stewards review flagged records through a purpose-built Streamlit application, apply approve / reject / escalate decisions with comments, and only trusted records are promoted to the Gold layer for analytics and reporting.

The result: a single platform that handles ingestion, standardisation, quality assurance, business validation, and trusted data publication — with a complete audit trail and full Unity Catalog lineage.

---

## Key Features

**Ingestion**
- Plugin-based connector framework supporting ADLS Gen2, JDBC (SQL Server, PostgreSQL, MySQL), and REST APIs
- Schema drift detection on every ingestion run
- Incremental and full-refresh ingestion modes configurable per source
- Automatic metadata enrichment: batch ID, ingestion timestamp, source system

**Schema Standardisation**
- YAML-declared schema contracts per entity (canonical column names, types, nullability)
- Configurable column alias mapping from source to canonical names
- AI-assisted suggestions for unmapped columns (optional, human-confirmed)
- Type coercion with per-record error flags, not silent data loss

**Data Quality**
- Pluggable rule engine: completeness, uniqueness, range, pattern, referential integrity
- Per-record DQ score (0.0 – 1.0) with weighted rule contributions
- Records below the DQ threshold automatically routed to the Stewardship layer
- Batch-level DQ report written to the audit schema after every run

**Stewardship Workflow**
- Physical Delta table holding records pending business validation
- Formal approval state machine: `PENDING → IN_REVIEW → APPROVED | REJECTED | ESCALATED`
- SLA monitoring with automatic expiry and alerts on deadline breach
- Batch approval and rejection — stewards process sets of records, not individual rows
- Full audit trail: every state transition recorded with actor, timestamp, and comment

**Streamlit Application**
- Business-user-facing validation interface hosted as a Databricks App
- Dashboard with pending counts, SLA status, and recent activity
- Record review grid with DQ scores, AI profiling summaries, and action controls
- Audit trail page with filters and CSV export for compliance reporting
- Natural language query interface for the Gold layer

**Gold Publishing**
- Only `APPROVED` records promoted from Stewardship to Gold
- Delta MERGE (upsert) on business key — idempotent, safe for re-runs
- Promotion metadata written to every Gold record: approved by, timestamp, stewardship ID
- Unity Catalog lineage automatically tracked across all layers

**AI Enrichment** (optional, never on the critical path)
- Schema mapping suggestions via Azure OpenAI GPT-4o
- Embedding-based duplicate detection for fuzzy matches
- Plain-language DQ profiling summaries for business stewards
- Natural language to SQL for Gold data exploration
- Audit comment summarisation for executive reporting

**Operations**
- Entirely configuration-driven: YAML for sources, schemas, quality rules, and workflows
- Three-environment model (DEV / QA / PROD) via Databricks Asset Bundle targets
- GitHub Actions CI/CD with manual approval gates for QA and PROD
- Structured JSON logging with correlation ID propagation

---

## Architecture

DataGuardian extends the standard Medallion Architecture with a dedicated **Stewardship Layer** — a physical Delta table that serves as the holding zone for business validation.

```
Source Systems (ERP, CRM, JDBC, REST API, ADLS files)
        │
        │  Plugin-based connectors
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│                           BRONZE LAYER                                 │
│   Raw ingestion — schema preserved, nothing dropped, append-only      │
│   Metadata added: _batch_id, _ingestion_timestamp, _source_system     │
│   Table pattern: dg_{env}.bronze.{source}_{entity}                    │
└─────────────────────────────┬─────────────────────────────────────────┘
                               │  Schema mapping · type coercion · dedup
                               ▼
┌───────────────────────────────────────────────────────────────────────┐
│                           SILVER LAYER                                 │
│   Canonical schema applied · DQ rules evaluated · scores computed     │
│   Records below threshold flagged: _requires_stewardship = true       │
│   Table pattern: dg_{env}.silver.{entity}                             │
└─────────────────────────────┬─────────────────────────────────────────┘
                               │  DQ-flagged records routed here
                               ▼
┌───────────────────────────────────────────────────────────────────────┐
│                       STEWARDSHIP LAYER           ◄── Streamlit App   │
│   Approval state machine persisted in Delta                           │
│   PENDING → IN_REVIEW → APPROVED | REJECTED | ESCALATED              │
│   SLA monitoring · AI profiling summaries · batch actions             │
│   Table pattern: dg_{env}.stewardship.{entity}_pending                │
└─────────────────────────────┬─────────────────────────────────────────┘
                               │  APPROVED records only
                               ▼
┌───────────────────────────────────────────────────────────────────────┐
│                           GOLD LAYER                                   │
│   Trusted, business-validated, analytics-ready                        │
│   Delta MERGE on business key · lineage-tracked · SLA-documented      │
│   Table pattern: dg_{env}.gold.{entity}                               │
└───────────────────────────────────────────────────────────────────────┘
        │
        └──► Analytics · Reporting · Machine Learning
```

**Unity Catalog Naming Convention**

| Layer | Pattern | Example |
|---|---|---|
| Bronze | `dg_{env}.bronze.{source}_{entity}` | `dg_prod.bronze.erp_customers` |
| Silver | `dg_{env}.silver.{entity}` | `dg_prod.silver.customers` |
| Stewardship | `dg_{env}.stewardship.{entity}_pending` | `dg_prod.stewardship.customers_pending` |
| Gold | `dg_{env}.gold.{entity}` | `dg_prod.gold.customers` |
| Audit | `dg_{env}.audit.{event_type}` | `dg_prod.audit.approval_events` |

Environment promotion is a single CLI flag. No code changes between environments.

> Full design documentation: [Architecture Overview](docs/architecture/overview.md) | [Medallion Architecture](docs/architecture/medallion-architecture.md) | [Data Flow](docs/architecture/data-flow.md)

---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Compute | Azure Databricks | Unified analytics platform |
| Governance | Unity Catalog | Data access, lineage, discovery |
| Storage Format | Delta Lake | ACID transactions, time-travel, MERGE |
| Processing | PySpark | Distributed data transformation |
| Storage | Azure Data Lake Gen2 | Scalable object storage |
| Secrets | Azure Key Vault | Credential management |
| Orchestration | Databricks Workflows | Pipeline scheduling and dependency management |
| Deployment | Databricks Asset Bundles | Infrastructure-as-code for Databricks resources |
| CI/CD | GitHub Actions | Automated testing and deployment pipelines |
| Application | Streamlit (Databricks Apps) | Business stewardship UI |
| AI Services | Azure OpenAI / OpenAI API | Schema mapping, profiling, NL-to-SQL |
| Configuration | YAML (PyYAML + Pydantic) | Validated, version-controlled pipeline config |
| Language | Python 3.11+ | Application and pipeline logic |
| Linting | Ruff | Fast Python linter and formatter |
| Type Checking | mypy (strict mode) | Static type safety |
| Testing | pytest + chispa | Unit and PySpark DataFrame testing |

---

## Repository Structure

```
dataguardian/
│
├── .github/
│   ├── workflows/
│   │   ├── ci.yml                  # Lint · type-check · unit tests on every PR
│   │   ├── deploy-dev.yml          # Auto-deploy to DEV on merge to main
│   │   ├── deploy-qa.yml           # Manual-gate deploy to QA on release tag
│   │   └── deploy-prod.yml         # Senior-approval deploy to PROD
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.md
│   │   └── feature_request.md
│   └── pull_request_template.md
│
├── config/
│   ├── environments/
│   │   ├── dev.yml                 # DEV environment settings
│   │   ├── qa.yml                  # QA environment settings
│   │   └── prod.yml                # PROD environment settings
│   ├── sources/
│   │   ├── _source_template.yml    # Copy this to onboard a new source
│   │   └── example_erp.yml         # Example ERP customer source
│   ├── schemas/
│   │   ├── _schema_template.yml    # Copy this to define a new entity contract
│   │   └── example_customer.yml    # Example customer entity schema
│   ├── quality/
│   │   ├── _rules_template.yml     # Copy this to define DQ rules for an entity
│   │   └── example_customer_rules.yml
│   └── workflows/
│       └── ingest_workflow.yml
│
├── databricks/
│   ├── bundle/
│   │   ├── databricks.yml          # DAB root config with variable definitions
│   │   ├── resources/
│   │   │   ├── jobs/               # Job definitions per pipeline stage
│   │   │   └── clusters/           # Shared cluster configuration
│   │   └── targets/                # Per-environment target overrides
│   └── notebooks/
│       ├── ingestion/              # Bronze landing notebook
│       ├── transformation/         # Silver transformation notebook
│       ├── quality/                # DQ evaluation notebook
│       └── publishing/             # Gold promotion notebook
│
├── docs/
│   ├── adr/                        # Architecture Decision Records
│   │   ├── ADR_TEMPLATE.md         # Template for new ADRs
│   │   ├── 0001-medallion-with-stewardship.md
│   │   ├── 0002-yaml-config-over-sql-control-tables.md
│   │   ├── 0003-databricks-asset-bundles.md
│   │   └── 0004-ai-as-optional-enrichment.md
│   ├── architecture/               # System-level design documents
│   ├── design/                     # Component-level design documents
│   ├── runbooks/                   # Operational procedures
│   └── standards/
│       └── coding-standards.md     # Engineering standards for this repository
│
├── scripts/
│   ├── validate_config.py          # Validate all YAML configs (runs in CI)
│   ├── setup_unity_catalog.py      # Bootstrap catalogs and schemas
│   └── seed_dev_data.py            # Generate synthetic test data for DEV
│
├── src/
│   ├── ai/                         # AI enrichment services (optional decorators)
│   ├── app/                        # Streamlit stewardship application
│   │   ├── pages/
│   │   └── components/
│   ├── common/                     # Shared utilities used across all modules
│   │   ├── config_loader.py        # YAML config loading and validation
│   │   ├── exceptions.py           # Domain exception hierarchy
│   │   ├── logger.py               # Structured JSON logger
│   │   ├── spark_session.py        # Spark session manager
│   │   └── unity_catalog_client.py # Unity Catalog table management
│   ├── ingestion/
│   │   ├── base_connector.py       # Abstract connector interface
│   │   ├── ingestion_engine.py     # Connector orchestration and Bronze write
│   │   └── connectors/             # ADLS · JDBC · API connector implementations
│   ├── publishing/
│   │   └── gold_publisher.py       # Gold MERGE and lineage tracking
│   ├── quality/
│   │   ├── rule_engine.py          # Rule loading, evaluation, and DQ scoring
│   │   └── rules/                  # Completeness · Uniqueness · Range · Pattern · Referential
│   ├── schema/
│   │   ├── schema_mapper.py        # Source → canonical column mapping
│   │   ├── schema_registry.py      # Schema contract loading and caching
│   │   └── schema_validator.py     # Post-mapping conformance validation
│   └── stewardship/
│       ├── approval_engine.py      # State machine transitions with ACID writes
│       └── audit_logger.py         # Append-only audit event writer
│
└── tests/
    ├── conftest.py                 # Shared Spark session and fixtures
    ├── fixtures/                   # Sample configs and data for tests
    ├── unit/                       # Fast, isolated unit tests (no Spark required)
    └── integration/                # Pipeline-level tests (requires Spark + Delta)
```

---

## Getting Started

### Prerequisites

| Requirement | Minimum Version |
|---|---|
| Python | 3.11 |
| Databricks CLI | 0.200 |
| Azure Databricks workspace | Unity Catalog enabled |
| Azure Data Lake Gen2 | — |
| Azure Key Vault | — |

### 1. Clone and Install

```bash
git clone https://github.com/rajkumarbandi/dataguardian.git
cd dataguardian

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate          # Linux / macOS
# .venv\Scripts\activate           # Windows

# Install with all development dependencies
pip install -e ".[dev]"
```

### 2. Configure Your Environment

```bash
# Copy the environment template
cp .env.example .env

# Edit .env with your workspace-specific values (never commit this file)
# See .env.example for all required variables
```

### 3. Validate Configuration

```bash
# Validate all YAML config files for the DEV environment
python scripts/validate_config.py --env dev

# Run the full pre-commit check suite
make check-all
```

### 4. Databricks Setup

```bash
# Authenticate with your Databricks workspace
databricks auth login --host https://<workspace>.azuredatabricks.net

# Validate the Asset Bundle for DEV
databricks bundle validate --target dev

# Deploy to DEV
databricks bundle deploy --target dev
```

> Full step-by-step instructions: [Deployment Runbook](docs/runbooks/deployment.md)
> New source onboarding: [Onboarding Runbook](docs/runbooks/onboarding-new-source.md)

### 5. Run the Tests

```bash
# Unit tests only (no Spark required)
make test-unit

# Full test suite with coverage report
make test
```

---

## Configuration Philosophy

DataGuardian is **entirely configuration-driven**. There are no SQL control tables. Environment-specific values are never hardcoded.

Every source system, schema mapping, quality rule suite, and workflow is declared in YAML and version-controlled alongside the application code. Configuration changes go through pull request review, exactly like code changes.

**Source definition** (`config/sources/erp_customers.yml`):
```yaml
source:
  name: erp_customers
  entity: customer
  connector: adls
  format: parquet
  connection:
    location: "{adls_root}/raw/erp/customers/"
  column_mappings:
    CustID: customer_id
    FullName: full_name
    EmailAddr: email_address
  stewardship:
    approvers: ["finance.steward@company.com"]
    sla_hours: 48
```

**Schema contract** (`config/schemas/example_customer.yml`):
```yaml
entity: customer
business_key: [customer_id]
columns:
  - name: customer_id
    type: string
    nullable: false
    pii: false
  - name: email_address
    type: string
    nullable: true
    pii: true
    validation:
      pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"
```

**Quality rule suite** (`config/quality/example_customer_rules.yml`):
```yaml
entity: customer
dq_threshold: 0.75
rules:
  - rule: completeness
    id: required_fields_present
    weight: 0.35
    columns: [customer_id, full_name, country_code]
  - rule: uniqueness
    id: unique_customer_id
    weight: 0.25
    columns: [customer_id]
  - rule: pattern
    id: valid_email_format
    weight: 0.40
    column: email_address
    pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"
```

The same codebase deploys to DEV, QA, and PROD — only the Databricks Asset Bundle target changes.

---

## AI Capabilities

AI is used only where it delivers measurable engineering value. Every AI feature is an **optional enrichment layer** — the platform runs fully without AI services. If Azure OpenAI is unavailable, pipelines continue; AI columns remain null.

| Feature | Trigger | Value |
|---|---|---|
| Schema Mapping Suggestions | Unmapped source column detected | Reduces manual mapping effort for new sources |
| Duplicate Detection | Bronze → Silver transition | Catches fuzzy duplicates that exact-key dedup misses |
| Data Profiling Summary | Record enters Stewardship layer | Plain-language DQ narrative for non-technical stewards |
| Natural Language to SQL | Business user submits a question | Enables Gold data exploration without SQL knowledge |
| Comment Summarisation | Audit report generated | Converts raw comments into actionable management themes |

**PII Protection:** Columns flagged `pii: true` in the schema contract are never included in AI prompts. Statistics (null rates, counts) are sent — not values.

---

## Security

- **No credentials in code or YAML.** All secrets stored in Azure Key Vault, referenced via Databricks secret scopes.
- **Unity Catalog governs all access.** Table-level, column-level masking, and row-level security applied centrally — not in application code.
- **PII columns masked** for non-privileged roles at the Unity Catalog layer.
- **Audit trail is append-only.** The `audit.approval_events` table has `delta.appendOnly = true` and no UPDATE/DELETE privileges granted to any role.
- **PROD deployments require manual approval** via a GitHub Actions environment gate — no automated pushes to production.
- **Network isolation** via VNet injection, Private Endpoints for ADLS and Key Vault.

See [Security Model](docs/architecture/security-model.md) for full detail.

---

## CI/CD Pipeline

```
feature/* branch
      │
      │  push / pull request
      ▼
  ┌────────────────────────────────┐
  │  CI (on every PR to main)      │
  │  ├── ruff lint                 │
  │  ├── mypy type-check           │
  │  ├── pytest unit tests         │
  │  ├── validate_config.py        │
  │  └── databricks bundle validate│
  └───────────────┬────────────────┘
                  │  merge to main
                  ▼
  ┌────────────────────────────────┐
  │  Deploy to DEV (automatic)     │
  │  databricks bundle deploy      │
  │  --target dev                  │
  └───────────────┬────────────────┘
                  │  release tag + manual approval
                  ▼
  ┌────────────────────────────────┐
  │  Deploy to QA                  │
  │  Same artifact · --target qa   │
  └───────────────┬────────────────┘
                  │  QA sign-off + senior approval
                  ▼
  ┌────────────────────────────────┐
  │  Deploy to PROD                │
  │  Same artifact · --target prod │
  └────────────────────────────────┘
```

Build once. Promote everywhere. No rebuilds between environments.

---

## Development Roadmap

| Milestone | Scope | Status |
|---|---|---|
| **M0** | Repository scaffold, architecture docs, engineering standards | **Complete** |
| **M1** | Core infrastructure: ConfigLoader, SparkSession, UnityCatalogClient, Logger | Planned |
| **M2** | Ingestion layer: ADLS, JDBC, API connectors · Bronze landing · schema drift detection | Planned |
| **M3** | Schema standardisation: registry, mapper, validator · Silver transformation | Planned |
| **M4** | Data quality rule engine: completeness, uniqueness, range, pattern, referential | Planned |
| **M5** | Stewardship layer: state machine, audit trail, SLA monitor | Planned |
| **M6** | Streamlit stewardship application: dashboard, review, mapping, audit pages | Planned |
| **M7** | Gold publishing: MERGE, lineage tracking, OPTIMIZE | Planned |
| **M8** | AI enrichment services: schema mapping, duplicate detection, profiling, NL-SQL | Planned |
| **M9** | CI/CD completion: full GitHub Actions pipelines, DAB resource definitions | Planned |
| **M10** | Observability: structured logging, DQ trend dashboards, alerting | Planned |

See [ROADMAP.md](docs/ROADMAP.md) for detailed deliverables per milestone.

---

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting pull requests.

```bash
# Quick start
git checkout -b feat/your-feature-name
# make changes
make check-all          # lint + type-check + tests
git commit -m "feat(scope): description"
# open pull request
```

All contributions require:
- A pull request with the provided template completed
- At least one reviewer approval
- CI passing (lint, type-check, unit tests)
- CHANGELOG.md updated under `[Unreleased]`

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

*DataGuardian — Building Trust in Enterprise Data*
