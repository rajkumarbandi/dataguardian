# Deployment Runbook

## Overview

DataGuardian uses Databricks Asset Bundles (DAB) managed by GitHub Actions for all deployments. This runbook covers manual and automated deployment procedures.

---

## Prerequisites

- Databricks CLI v0.200+ installed and authenticated
- Access to the target Databricks workspace
- GitHub repository access with environment secrets configured

---

## Automated Deployment (Normal Path)

### DEV Deployment
Triggered automatically on every merge to `main`:

1. GitHub Actions `ci.yml` runs: lint → type-check → unit tests
2. If CI passes, `deploy-dev.yml` triggers automatically
3. DAB deploys to DEV: `databricks bundle deploy --target dev`
4. Deployment status visible in GitHub Actions and Databricks workspace

### QA Deployment
Triggered on release tag creation with manual approval:

1. Create a release tag: `git tag v0.2.0 && git push origin v0.2.0`
2. GitHub Actions `deploy-qa.yml` runs and waits for manual approval
3. An authorized reviewer approves in the GitHub Actions environment gate
4. DAB deploys to QA: `databricks bundle deploy --target qa`

### PROD Deployment
Requires QA sign-off and manual approval gate:

1. QA validation must be complete (sign-off in the GitHub release notes)
2. GitHub Actions `deploy-prod.yml` runs and waits for manual approval
3. A senior team member approves in the GitHub Actions environment gate
4. DAB deploys to PROD: `databricks bundle deploy --target prod`

---

## Manual Deployment

If the automated pipeline is unavailable or a hotfix is required:

```bash
# Authenticate
databricks auth login --host https://<workspace>.azuredatabricks.net

# Validate the bundle first
databricks bundle validate --target <env>

# Deploy
databricks bundle deploy --target <env>

# Verify deployment
databricks bundle run ingestion_job --target <env>
```

---

## First-Time Environment Setup

For setting up a new environment from scratch:

```bash
# 1. Bootstrap Unity Catalog structure
python scripts/setup_unity_catalog.py --env <env>

# 2. Configure Key Vault secret scope in Databricks workspace
# (See Azure Key Vault documentation for secret scope setup)

# 3. Validate YAML configuration
python scripts/validate_config.py --env <env>

# 4. Validate the DAB bundle
databricks bundle validate --target <env>

# 5. Deploy
databricks bundle deploy --target <env>
```

---

## Rollback Procedure

### If deployment fails during DAB deploy:
The previous bundle state is still active. The new bundle version was not applied. No rollback needed.

### If a job fails after deployment:
1. Identify the failing job in the Databricks Workflows UI
2. Check structured logs in the Databricks cluster driver logs
3. If the issue is a config change: `git revert` the offending commit, merge, and let CI/CD redeploy
4. If the issue is a data issue: investigate the stewardship layer for stuck records

### Emergency PROD rollback:
```bash
# Revert to previous bundle by deploying the prior tag
git checkout v<previous-tag>
databricks bundle deploy --target prod
```

---

## Environment-Specific Configuration

| Setting | DEV | QA | PROD |
|---|---|---|---|
| Unity Catalog | `dg_dev` | `dg_qa` | `dg_prod` |
| Cluster size | Single node | Small cluster | Medium cluster |
| AI enrichment | Disabled | Enabled | Enabled |
| Notifications | Disabled | Test recipients | Production recipients |
| SLA hours | 1 | 24 | 48 |
| Log level | DEBUG | INFO | INFO |

---

## Verification Steps After Deployment

1. Validate DAB deployment: `databricks bundle validate --target <env>`
2. Trigger a test ingestion run with sample data
3. Verify Bronze table created in Unity Catalog
4. Verify Silver table populated and DQ scores applied
5. Verify stewardship records created for any DQ-failed records
6. Log into Streamlit app and verify records appear for review
7. Approve a test record and verify Gold table populated

---

## Escalation

If deployment fails and cannot be resolved within 30 minutes:
1. Revert to the last known good state using the rollback procedure above
2. Open a GitHub issue with the full error log
3. Notify the platform team via the configured alert channel
