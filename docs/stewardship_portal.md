# DataGuardian — Business Data Stewardship Portal

**Milestone 9 — Databricks Apps (Streamlit)**

---

## Table of Contents

1. [Purpose](#1-purpose)
2. [Architecture](#2-architecture)
3. [Application Pages](#3-application-pages)
4. [Data Layer](#4-data-layer)
5. [Steward Actions](#5-steward-actions)
6. [Delta Tables](#6-delta-tables)
7. [Configuration](#7-configuration)
8. [Local Development](#8-local-development)
9. [Databricks Apps Deployment](#9-databricks-apps-deployment)
10. [DAB Integration](#10-dab-integration)

---

## 1. Purpose

The DataGuardian Stewardship Portal is an **operational enterprise application** used by Business Data Stewards to validate curated (Silver) data before it is promoted to the Gold layer.

This is **not** an analytics dashboard. It is an action-oriented interface where stewards:

- Inspect records flagged by the DQ pipeline
- Review specific rule violations and raw field values
- Approve, reject, or request corrections
- Assign records to specific stewards
- Track all decisions with an immutable audit trail
- Monitor pipeline health and DQ score trends

The portal sits at the boundary between the automated DQ pipeline (Milestones 1–8) and the curated Gold layer. No record moves to Gold without a steward approval.

---

## 2. Architecture

```
Databricks Apps (Streamlit)
│
├── app.py                   ← Entry point (app.yaml references this)
├── pages/                   ← 7 Streamlit pages (multi-page app)
│   ├── 01_dashboard.py
│   ├── 02_pending.py
│   ├── 03_review.py
│   ├── 04_queue.py
│   ├── 05_history.py
│   ├── 06_metrics.py
│   └── 07_admin.py
│
├── ui/                      ← Reusable UI components
│   ├── styles.py            ← Global CSS + color palette
│   ├── components.py        ← KPI cards, tables, action panel, comments
│   └── charts.py            ← Plotly chart builders
│
├── services/                ← Business logic (pages → services → repos)
│   ├── dashboard_service.py
│   ├── stewardship_service.py
│   ├── pipeline_service.py
│   └── admin_service.py
│
├── repository/              ← All Delta reads/writes
│   ├── stewardship_repo.py
│   ├── pipeline_repo.py
│   ├── comments_repo.py
│   └── audit_repo.py
│
├── data/                    ← Data provider abstraction
│   ├── provider.py          ← DataProvider (abstract) + SparkDataProvider + SampleDataProvider
│   ├── sample.py            ← 200+ record deterministic sample data generator
│   └── schema.py            ← Delta table DDL
│
├── models/                  ← Domain models (dataclasses + enums)
│   ├── stewardship.py       ← RecordStatus, ActionType, StewardshipRecord, etc.
│   └── pipeline.py          ← PipelineRun, PipelineMetrics
│
├── config/                  ← Application settings
│   └── settings.py          ← AppSettings (from env vars)
│
├── requirements.txt         ← App-specific Python dependencies
└── app.yaml                 ← Databricks Apps configuration
```

### Layering rules

- **Pages** call **services** only. No direct repository or provider access from pages.
- **Services** call **repositories** only.
- **Repositories** call **DataProvider** only.
- **DataProvider** is the only component that reads from or writes to Delta tables.
- The **SampleDataProvider** substitutes the real Delta provider in demo/local mode.

---

## 3. Application Pages

### 1. Dashboard (`01_dashboard.py`)

**Purpose:** Operational overview for team leads and senior stewards.

**Content:**
- 5 KPI metrics: Pending, Approved, Rejected, Correction Requested, Avg DQ Score
- Record status donut chart + Records by source bar chart
- Top DQ violations breakdown + Approval activity trend (7 days)
- 7-day stats: approval rate, records resolved, active stewards
- Recent pipeline runs table

### 2. Pending Validations (`02_pending.py`)

**Purpose:** Queue of all records awaiting steward action.

**Content:**
- Filter controls: Source, Assigned Steward
- Summary: total pending, unassigned count, avg DQ score
- Paginated records table (25 per page) with selection — clicking a row navigates to Record Review
- Table columns: Source, Batch, Status, DQ Score, Violations, Assigned To, Created

### 3. Record Review (`03_review.py`)

**Purpose:** The primary action surface — the most important page in the application.

**Content:**
- Record header: ID, source, batch, status badge, DQ score
- Metadata panel: table, assigned_to, timestamps, reviewed_by
- DQ violations list: rule name, column, severity badge, message, expected/actual values
- Four tabs:
  - **Raw Record**: full field-value grid of the failed record
  - **Take Action**: Approve / Reject / Request Correction / Assign tabs with required comment fields
  - **Comments**: threaded discussion with reply support
  - **History**: action timeline (newest first)

**Action guard:** Actions are only available when the record status is PENDING.

### 4. My Approval Queue (`04_queue.py`)

**Purpose:** Personal queue for each steward — records assigned to them.

**Content:**
- Steward selector (switchable in demo mode)
- Pending action section: expandable cards with DQ score and "Review →" button
- Correction requested section: records awaiting upstream fix
- Resolved records: collapsible table of approved/rejected records

### 5. Audit History (`05_history.py`)

**Purpose:** Immutable log of all steward operations for compliance and accountability.

**Content:**
- Filters: time window (7/14/30/60/90 days), operation type, steward
- Activity timeline bar chart (daily, stacked by operation)
- Operation breakdown with progress bars
- Paginated audit log table
- Entry detail viewer (JSON inspector)

**Design principle:** No delete or update operations are available. The audit log is append-only by design.

### 6. Pipeline Metrics (`06_metrics.py`)

**Purpose:** Data engineering view — DQ health and pipeline execution statistics.

**Content:**
- Overall health KPIs: total runs, rows processed, avg DQ score, success rate
- Source health summary table
- DQ score trend line chart (with 80% target threshold line)
- Row volume stacked bar chart (Silver passed vs Failed)
- Pipeline run log table with filtering by source

### 7. Administration (`07_admin.py`)

**Purpose:** Read-only system health and configuration overview for administrators.

**Content:**
- System info: version, environment, catalog, data mode, Python version
- Record status distribution with progress bars
- Records by source with progress bars
- Pipeline health by source table
- Steward activity bar chart (30 days)
- Recent audit log (last 100 entries)

**Design principle:** This page is read-only. No modifications can be made.

---

## 4. Data Layer

### DataProvider interface

All data access goes through the `DataProvider` abstract class (`src/app/data/provider.py`):

```python
class DataProvider(ABC):
    def get_stewardship_records(status, source_name, assigned_to, limit) -> pd.DataFrame
    def get_stewardship_record(record_id) -> dict | None
    def get_actions(record_id) -> pd.DataFrame
    def get_comments(record_id) -> pd.DataFrame
    def get_audit_log(entity_type, performed_by, operation, days, limit) -> pd.DataFrame
    def get_pipeline_runs(source_name, limit) -> pd.DataFrame
    def save_action(action: dict) -> None
    def save_comment(comment: dict) -> None
    def update_record_status(record_id, new_status, previous_status, performed_by, comment) -> None
    def get_stewards() -> list[str]
    def get_sources() -> list[str]
```

### SampleDataProvider (demo mode)

Activated when `DG_DEMO_MODE=true` or `DG_WAREHOUSE_HTTP_PATH` is not set.

Generates 200+ records deterministically (seed=42) covering:
- 4 sources: customers (62), orders (78), products (38), order_items (22)
- 4 statuses: PENDING (38%), APPROVED (35%), REJECTED (14%), CORRECTION_REQUESTED (13%)
- 6 batches over the last 30 days
- 10 violation types: not_null, unique, email, positive_number, future_date, allowed_values, min_length, referential_integrity, sql_expression
- 4 stewards: Sarah Mitchell, James Chen, Emma Davis, Oliver Brown
- Threaded comments, action history, and audit log entries

### SparkDataProvider (production mode)

Activated when `DG_WAREHOUSE_HTTP_PATH` is set.

Uses `databricks-sql-connector` with credentials auto-injected by Databricks Apps:
- `DATABRICKS_HOST` — workspace URL (auto-injected)
- `DATABRICKS_TOKEN` — OAuth token (auto-injected)
- `DG_WAREHOUSE_HTTP_PATH` — SQL Warehouse HTTP path (from app.yaml configVariable)

### Factory

```python
provider = get_data_provider()  # @st.cache_resource — created once per app process
```

---

## 5. Steward Actions

All steward actions require a non-empty comment/justification:

| Action | Required Comment | Result |
|--------|-----------------|--------|
| **Approve** | Justification for approval | Status → APPROVED. Record promoted to Gold. |
| **Reject** | Rejection reason | Status → REJECTED. Record excluded from Gold. |
| **Request Correction** | Instructions for source team | Status → CORRECTION_REQUESTED. Returned to source. |
| **Assign** | Auto-generated | No status change. Record assigned to named steward. |
| **Comment** | Comment text | No status change. Appended to thread. |

Every action writes to:
1. `stewardship.stewardship_records` — status updated
2. `stewardship.stewardship_actions` — action appended
3. `stewardship.audit_log` — immutable audit entry appended

---

## 6. Delta Tables

All four stewardship tables are created by `src/app/data/schema.py`.

### `stewardship.stewardship_records`

Primary table — one row per DQ-failed record pending review.

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | STRING | Unique record identifier (UUID) |
| `run_id` | STRING | Pipeline run that produced this failure |
| `source_name` | STRING | Source entity name (e.g. customers) |
| `batch_id` | STRING | Batch identifier from bronze ingestion |
| `table_name` | STRING | Target silver table |
| `dq_score` | DOUBLE | Data quality score at time of failure (0–1) |
| `status` | STRING | PENDING \| APPROVED \| REJECTED \| CORRECTION_REQUESTED |
| `assigned_to` | STRING | Steward assigned to this record |
| `violation_count` | INT | Number of DQ rule violations |
| `failed_rules` | STRING | JSON array of FailedRule objects |
| `raw_record` | STRING | JSON of the raw failed record |
| `ingested_at` | TIMESTAMP | When the record was loaded into bronze |
| `created_at` | TIMESTAMP | When the stewardship record was created |
| `reviewed_at` | TIMESTAMP | When the last steward action was taken |
| `reviewed_by` | STRING | Steward who last acted |
| `updated_at` | TIMESTAMP | Last update timestamp |

Change Data Feed is enabled for downstream streaming consumption.

### `stewardship.stewardship_actions`

Append-only. One row per steward action.

| Column | Type | Description |
|--------|------|-------------|
| `action_id` | STRING | UUID |
| `record_id` | STRING | FK → stewardship_records |
| `action_type` | STRING | APPROVE \| REJECT \| REQUEST_CORRECTION \| COMMENT \| ASSIGN \| REASSIGN |
| `performed_by` | STRING | Steward name |
| `comment` | STRING | Justification or note |
| `assigned_to` | STRING | Target steward (ASSIGN/REASSIGN only) |
| `previous_status` | STRING | Status before action |
| `new_status` | STRING | Status after action |
| `action_timestamp` | TIMESTAMP | When the action was taken |
| `metadata` | STRING | JSON (dq_score, source details) |

### `stewardship.comments`

Threaded discussion. One row per comment.

| Column | Type | Description |
|--------|------|-------------|
| `comment_id` | STRING | UUID |
| `record_id` | STRING | FK → stewardship_records |
| `parent_comment_id` | STRING | FK → comments.comment_id (NULL for top-level) |
| `author` | STRING | Comment author |
| `message` | STRING | Comment text |
| `status` | STRING | ACTIVE \| DELETED |
| `created_at` | TIMESTAMP | When posted |

### `stewardship.audit_log`

Immutable audit trail. `delta.appendOnly = true` enforced.

| Column | Type | Description |
|--------|------|-------------|
| `audit_id` | STRING | UUID |
| `entity_type` | STRING | Entity type (stewardship_record) |
| `entity_id` | STRING | ID of the entity acted on |
| `operation` | STRING | Operation name |
| `performed_by` | STRING | User who performed the operation |
| `details` | STRING | JSON details |
| `audit_timestamp` | TIMESTAMP | When the operation occurred |

---

## 7. Configuration

All configuration is resolved from environment variables at startup:

| Variable | Default | Description |
|----------|---------|-------------|
| `DG_CATALOG` | `dg_prod` | Unity Catalog name |
| `DG_ENV` | `prod` | Environment name |
| `DG_WAREHOUSE_HTTP_PATH` | `` | SQL Warehouse HTTP path |
| `DATABRICKS_HOST` | `` | Workspace URL (auto-injected by Apps) |
| `DATABRICKS_TOKEN` | `` | OAuth token (auto-injected by Apps) |
| `DG_DEMO_MODE` | `true` | Force demo mode (sample data) |

When `DG_WAREHOUSE_HTTP_PATH` is empty, the app automatically falls back to demo mode (`SampleDataProvider`).

---

## 8. Local Development

### Prerequisites

```bash
pip install streamlit>=1.36 plotly>=5.20 pandas>=2.1
```

### Run locally

```bash
# From the repository root
cd src/app
streamlit run app.py
```

The app starts in **demo mode** automatically (no credentials needed).

### Change the active steward

Use the **Active Steward** dropdown in the sidebar to switch between stewards and see their personal queues.

### Run with real data (local Databricks Connect)

```bash
export DATABRICKS_HOST=https://adb-<id>.azuredatabricks.net
export DATABRICKS_TOKEN=<pat>
export DG_WAREHOUSE_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
export DG_CATALOG=dg_dev
export DG_ENV=dev
export DG_DEMO_MODE=false

cd src/app
streamlit run app.py
```

---

## 9. Databricks Apps Deployment

### Step 1: Initialize the stewardship schema

Run this once per environment (replace `dg_prod` with your catalog):

```python
from src.app.data.schema import initialize_schema
initialize_schema(spark, catalog="dg_prod")
```

Or use the provided notebook at `databricks/notebooks/ops/init_stewardship_schema.py` (create if needed).

### Step 2: Deploy the app

```bash
cd databricks/bundle

# Validate
databricks bundle validate --target prod

# Deploy (creates/updates the Databricks App)
databricks bundle deploy --target prod
```

The app source code is synced from `src/app/` to `/Workspace/dataguardian/src/app/` in the workspace.

### Step 3: Configure the app

Set the following configuration variables in the Databricks App settings (or in `databricks.yml` variables):

| Variable | Description |
|----------|-------------|
| `sql_warehouse_id` | SQL Warehouse ID to use for queries |
| `sql_warehouse_http_path` | HTTP path of the SQL Warehouse |
| `data_stewards_group` | AAD group with CAN_VIEW permission |
| `data_engineering_group` | AAD group with CAN_MANAGE permission |

### Step 4: Start the app

```bash
databricks apps start dataguardian-stewardship-prod
```

Or start from the Databricks UI: Apps → dataguardian-stewardship-prod → Start.

### Step 5: Grant access

The app inherits Databricks authentication. All workspace users can access the app via SSO. Permissions are controlled by the `data_stewards_group` and `data_engineering_group` variables.

---

## 10. DAB Integration

The app is declared in `databricks/bundle/resources/apps/stewardship_portal.yml`:

```yaml
resources:
  apps:
    stewardship_portal:
      name: "dataguardian-stewardship-${bundle.target}"
      source_code_path: ../../src/app
      config:
        - name: catalog
          value: "${var.catalog}"
        - name: sql_warehouse_http_path
          value: "${var.sql_warehouse_http_path}"
      resources:
        - name: stewardship-sql-warehouse
          sql_warehouse:
            id: "${var.sql_warehouse_id}"
            permission: CAN_USE
```

The include directive in `databricks.yml` includes this file:

```yaml
include:
  - resources/jobs/*.yml
  - resources/clusters/*.yml
  - resources/apps/*.yml
```

The app is deployed alongside the pipeline jobs with `databricks bundle deploy`, ensuring that both the pipeline and the portal are always in sync.

---

*DataGuardian — Milestone 9: Business Data Stewardship Portal*
