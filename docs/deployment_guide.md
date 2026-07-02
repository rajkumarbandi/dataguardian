# DataGuardian — Deployment Guide

**Milestone 8 — CI/CD, Packaging & Deployment**

---

## Table of Contents

1. [Project Structure](#1-project-structure)
2. [Installation](#2-installation)
3. [Local Development](#3-local-development)
4. [Databricks Asset Bundle (DAB) Deployment](#4-databricks-asset-bundle-dab-deployment)
5. [Environment Promotion](#5-environment-promotion)
6. [Secrets Management](#6-secrets-management)
7. [Configuration Management](#7-configuration-management)
8. [Deployment Validation](#8-deployment-validation)
9. [Rollback Strategy](#9-rollback-strategy)
10. [CI/CD Pipeline](#10-cicd-pipeline)

---

## 1. Project Structure

```
dataguardian/
│
├── databricks/
│   ├── bundle/                         # Databricks Asset Bundle root
│   │   ├── databricks.yml              # Bundle config: targets, variables, sync
│   │   └── resources/
│   │       ├── jobs/
│   │       │   ├── silver_validation_job.yml
│   │       │   ├── deployment_validation_job.yml
│   │       │   ├── bronze_ingestion_job.yml
│   │       │   └── stewardship_job.yml
│   │       └── clusters/
│   │           └── shared_cluster.yml
│   └── notebooks/
│       ├── quality/
│       │   └── silver_validation.py    # Simplified (M8): bootstrap → run → summary
│       ├── ingestion/
│       │   └── bronze_ingestion.py
│       └── ops/
│           └── deployment_validation.py  # NEW (M8)
│
├── src/                                # Python package (importable as `src`)
│   ├── __init__.py
│   ├── bootstrap.py                    # NEW (M8): PipelineBootstrap + PipelineContext
│   ├── pipeline.py                     # NEW (M8): run_pipeline() + discover_sources()
│   ├── common/
│   │   ├── config_loader.py
│   │   ├── exceptions.py
│   │   ├── logger.py
│   │   ├── models.py
│   │   ├── pipeline_run.py
│   │   ├── retry.py
│   │   ├── secrets.py                  # NEW (M8): SecretsManager
│   │   ├── spark_session.py
│   │   └── unity_catalog_client.py
│   ├── audit/
│   ├── contracts/
│   ├── deployment/                     # NEW (M8): DeploymentValidator
│   │   ├── __init__.py
│   │   └── validator.py
│   ├── quality/
│   ├── schema/
│   ├── silver/
│   └── transformations/
│
├── config/
│   ├── environments/                   # dev.yml, test.yml, qa.yml, prod.yml
│   └── sources/                        # customers.yml, orders.yml, ...
│
├── tests/
│   └── unit/
│
├── pyproject.toml                      # Package metadata, tool config, deps
├── requirements.txt                    # Runtime deps (pip-compiled from pyproject.toml)
├── requirements-dev.txt                # Dev + test deps
└── docs/
    └── deployment_guide.md             # This file
```

---

## 2. Installation

### Local installation (pip)

The `src` package is the DataGuardian Python package.  Install it with:

```bash
# Install runtime dependencies only
pip install .

# Install with development + test toolchain
pip install -e ".[dev,spark]"

# Or from requirements files
pip install -r requirements-dev.txt
```

`pip install .` installs the `src` package so `from src.bootstrap import PipelineBootstrap`
works in any Python environment.

### On Databricks (DAB file sync)

On Databricks, the `src/` directory is synced to the workspace via DAB and
added to `sys.path` by the notebook's path-detection header.  No wheel
installation is required.  Runtime dependencies (pydantic, pyyaml, structlog)
are installed on the cluster via the `library:` section in `shared_cluster.yml`.

---

## 3. Local Development

### Prerequisites

- Python 3.11+
- Java 11+ (for local PySpark)
- Databricks CLI v0.220+ (for bundle commands)

### Setup

```bash
# Clone the repository
git clone https://github.com/your-org/dataguardian.git
cd dataguardian

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

# Install the full development stack
pip install -e ".[dev,spark]"

# Configure pre-commit hooks
pre-commit install

# Set environment variables for local secrets fallback (see §6)
export DG_DATAGUARDIAN_DEV_STORAGE_ACCOUNT_KEY=...
```

### Running tests locally

```bash
# Unit tests only (fast, no Spark required for non-integration tests)
pytest tests/unit/ -m "not integration" -v

# All unit tests (includes Spark-based tests — requires pyspark installed)
pytest tests/unit/ -v

# With coverage report
pytest tests/unit/ --cov=src --cov-report=term-missing

# Run a specific test file
pytest tests/unit/test_m8_secrets.py -v
```

### Linting and type checking

```bash
ruff check src/ tests/          # lint
ruff format src/ tests/         # format
mypy src/                       # type check
bandit -r src/                  # security scan
```

---

## 4. Databricks Asset Bundle (DAB) Deployment

### Prerequisites

```bash
# Install Databricks CLI
pip install databricks-cli  # or: brew install databricks

# Authenticate
databricks configure --token
# OR: set env vars
export DATABRICKS_HOST=https://adb-<workspace-id>.azuredatabricks.net
export DATABRICKS_TOKEN=<personal-access-token>
```

### Bundle location

The bundle root is `databricks/bundle/`.  All `databricks bundle` commands
must be run from this directory:

```bash
cd databricks/bundle
```

### Deploy to an environment

```bash
# Validate the bundle (dry run)
databricks bundle validate --target dev

# Deploy to dev (default target)
databricks bundle deploy

# Deploy to a specific target
databricks bundle deploy --target test
databricks bundle deploy --target qa
databricks bundle deploy --target prod
```

DAB will:
1. Sync all files listed in `sync.include` to `/Workspace/dataguardian/`
2. Create or update jobs defined in `resources/jobs/`
3. Create or update the shared cluster defined in `resources/clusters/`

### Trigger a job run

```bash
# Run Silver Validation for all sources
databricks bundle run silver_validation --target dev

# Run for a single source
databricks bundle run silver_validation --target dev \
  --notebook-params '{"source_name": "customers"}'

# Run deployment validation (ops check)
databricks bundle run deployment_validation --target dev
```

### What gets synced

The `sync.include` section of `databricks.yml` controls what is uploaded:

| Path | Purpose |
|------|---------|
| `src/**` | DataGuardian Python package |
| `config/**` | Environment and source YAML configs |
| `databricks/notebooks/**` | Notebooks |
| `pyproject.toml` | Package metadata |
| `requirements.txt` | Runtime dependency list |

Tests, dist/, .git/, and __pycache__ are excluded.

---

## 5. Environment Promotion

DataGuardian follows a **build-once, promote-everywhere** pattern.
Notebooks and job definitions are identical across all four environments.
Only the variables in `databricks.yml` differ.

### Promotion flow

```
Feature Branch
      │
      ▼ (PR merge)
  dev  ──────────────── CI: pytest + ruff + mypy + bandit
      │
      ▼ (manual trigger after CI passes)
  test ──────────────── Integration tests: end-to-end pipeline on dg_test catalog
      │
      ▼ (QA sign-off)
  qa   ──────────────── Performance + data quality acceptance tests
      │
      ▼ (release approval)
  prod ──────────────── Production deployment
```

### Per-environment differences

| Variable | dev | test | qa | prod |
|----------|-----|------|----|------|
| `catalog` | dg_dev | dg_test | dg_qa | dg_prod |
| `adls_root` | `...-dev...` | `...-test...` | `...-qa...` | `...-prod...` |
| `secret_scope` | `*-dev-scope` | `*-test-scope` | `*-qa-scope` | `*-prod-scope` |
| `log_level` | DEBUG | INFO | INFO | WARNING |
| `num_workers` | 2 | 2 | 4 | 8 |
| `schedule_cron` | 06:00 UTC | 04:00 UTC | 03:00 UTC | 02:00 UTC |
| DAB mode | development | development | staging | production |

### Promotion commands

```bash
cd databricks/bundle

# Promote dev → test
databricks bundle deploy --target test

# Promote test → qa
databricks bundle deploy --target qa

# Promote qa → prod (requires protected branch + approvals in CI)
databricks bundle deploy --target prod
```

---

## 6. Secrets Management

DataGuardian never stores credentials in code or YAML files.
All secrets are retrieved at runtime via `SecretsManager`.

### Architecture

```
Databricks Secret Scope
    dataguardian-{env}-scope
          │
          ▼
    SecretsManager.get("key")
          │
          ▼ (fallback for local dev)
    Environment Variable
    DG_{SCOPE}_{KEY}
```

### Adding a secret

```bash
# Add a secret to the Databricks scope
databricks secrets put-secret dataguardian-prod-scope storage-account-key

# Verify
databricks secrets list-secrets dataguardian-prod-scope
```

### Standard secret keys

| Key | Description |
|-----|-------------|
| `storage-account-name` | ADLS Gen2 storage account name |
| `storage-account-key` | ADLS Gen2 storage account key |
| `sp-tenant-id` | Azure Service Principal tenant ID |
| `sp-client-id` | Azure Service Principal client ID |
| `sp-client-secret` | Azure Service Principal client secret |

### Local development fallback

When `dbutils` is not available (local dev, CI), `SecretsManager` reads
environment variables with this naming convention:

```
DG_{SCOPE_UPPER}_{KEY_UPPER}
```

Example: scope=`dataguardian-dev`, key=`storage-account-key`
→ `DG_DATAGUARDIAN_DEV_STORAGE_ACCOUNT_KEY`

Set these in your shell or in a `.env` file (excluded from git via `.gitignore`):

```bash
# .env (never commit this file)
export DG_DATAGUARDIAN_DEV_STORAGE_ACCOUNT_KEY=your_key_here
export DG_DATAGUARDIAN_DEV_SP_CLIENT_SECRET=your_secret_here
```

### Using SecretsManager in a notebook

```python
context = PipelineBootstrap.initialize(
    env=env,
    spark=spark,
    dbutils=dbutils,
    secrets_scope="dataguardian-prod-scope",  # passed from DAB via widget
)

# Access a secret anywhere in the pipeline
key = context.secrets.get("storage-account-key")
creds = context.secrets.get_storage_credentials()
sp = context.secrets.get_service_principal()
```

---

## 7. Configuration Management

DataGuardian is entirely configuration-driven.  No environment-specific
values exist in Python code.

### Environment YAML (`config/environments/{env}.yml`)

Controls pipeline behaviour per environment:

```yaml
environment: prod
unity_catalog:
  catalog: dg_prod
storage:
  adls_root: "abfss://dataguardian-prod@..."
pipeline:
  pipeline_name: dataguardian
  pipeline_version: "0.8.0"
  audit_enabled: true
  retry_policy:
    max_attempts: 3
schema_registry:
  schema_registry_enabled: true
  schema_audit_enabled: true
  default_evolution_mode: STRICT
transformation:
  audit_enabled: true
contract_validation:
  contract_validation_enabled: true
  contract_audit_enabled: true
  default_contract_policy: FAIL_PIPELINE
```

### Source YAML (`config/sources/{source}.yml`)

Controls behaviour for a single data source:

```yaml
name: customers
dq_rules:
  - rule: not_null
    column: customer_id
    severity: error
transformations:
  - type: trim_strings
    params:
      columns: [email]
contract:
  name: customers_data_contract
  criticality: critical
  validation_policy: FAIL_PIPELINE
  required_columns: [customer_id, email]
```

### Token substitution

YAML values support `{env}`, `{catalog}`, `{adls_root}`, `{today}` tokens
resolved at runtime by `ConfigLoader`.

---

## 8. Deployment Validation

Run the deployment validation job after every new `databricks bundle deploy`
to confirm the environment is ready before the pipeline runs.

```bash
databricks bundle run deployment_validation --target prod
```

Or trigger via the notebook directly:
`databricks/notebooks/ops/deployment_validation.py`

### Checks performed

| Check | Severity | What it verifies |
|-------|----------|-----------------|
| `catalog_exists` | **error** | Unity Catalog is accessible |
| `package_installed` | **error** | `src` package is importable |
| `env_config_catalog` | **error** | Catalog name is set in environment YAML |
| `schema_bronze` | warning | bronze schema exists |
| `schema_silver` | warning | silver schema exists |
| `schema_audit` | warning | audit schema exists |
| `env_config_adls` | warning | ADLS root is configured |
| `audit_table_{name}` | info | Each audit table exists (×8) |

**Error checks block the pipeline.**  Warning and info checks are advisory —
they will be resolved automatically on the first pipeline run.

---

## 9. Rollback Strategy

### Notebook / code rollback

Notebooks and `src/` are synced from git.  To roll back:

```bash
git revert <commit> --no-edit
git push origin main

# Redeploy the reverted version
cd databricks/bundle
databricks bundle deploy --target prod
```

### Job rollback

Jobs are declarative (defined in YAML).  Redeploy the previous version:

```bash
git checkout <previous-tag>
cd databricks/bundle
databricks bundle deploy --target prod
```

### Data rollback

DataGuardian uses Delta Lake time travel for data rollback:

```sql
-- Restore Silver table to before a bad pipeline run
RESTORE TABLE dg_prod.silver.erp_customers TO TIMESTAMP AS OF '2026-07-01 00:00:00';

-- Or restore to a specific version
RESTORE TABLE dg_prod.silver.erp_customers TO VERSION AS OF 42;
```

Identify the run to roll back from `audit.pipeline_run_history`:

```sql
SELECT run_id, source_name, start_time, status, silver_rows_written
FROM dg_prod.audit.pipeline_run_history
ORDER BY start_time DESC
LIMIT 20;
```

### Config rollback

Environment and source YAML files are versioned in git.  Revert and redeploy.

---

## 10. CI/CD Pipeline

DataGuardian ships with GitHub Actions workflows (`.github/workflows/`).

### Recommended pipeline stages

```yaml
# .github/workflows/ci.yml (outline)
stages:
  - name: Lint & Type Check
    run: |
      ruff check src/ tests/
      mypy src/
      bandit -r src/

  - name: Unit Tests
    run: pytest tests/unit/ --cov=src --cov-fail-under=80

  - name: Bundle Validate (dev)
    run: |
      cd databricks/bundle
      databricks bundle validate --target dev

  - name: Deploy to Test
    if: branch == 'main'
    run: |
      cd databricks/bundle
      databricks bundle deploy --target test
      databricks bundle run deployment_validation --target test
      databricks bundle run silver_validation --target test

  - name: Deploy to QA
    if: tag matches 'v*'
    run: |
      cd databricks/bundle
      databricks bundle deploy --target qa
      databricks bundle run deployment_validation --target qa

  - name: Deploy to Prod
    if: approved release
    run: |
      cd databricks/bundle
      databricks bundle deploy --target prod
      databricks bundle run deployment_validation --target prod
```

### Branch strategy

| Branch | Deploys to |
|--------|-----------|
| `feature/*` | Manual deploy to dev |
| `main` | Auto-deploy to test |
| `release/*` | Auto-deploy to qa |
| Tagged release | Manual-approved deploy to prod |
