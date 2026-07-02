# ADR-0003: Databricks Asset Bundles for Deployment and Environment Management

**Status:** Accepted
**Date:** 2026-06-29
**Author:** Platform Architecture Team

---

## Context

The platform must be deployed consistently across DEV, QA, and PROD environments. Job definitions, cluster configurations, permissions, and environment-specific values must be managed in a repeatable, auditable way. Several Databricks deployment mechanisms were evaluated.

---

## Decision

We will use **Databricks Asset Bundles (DAB)** as the primary deployment mechanism. All Databricks resources (jobs, clusters, permissions) are declared as YAML in the `databricks/bundle/` directory and deployed via the Databricks CLI. Environment-specific configuration is declared in separate target files (`targets/dev.yml`, `targets/qa.yml`, `targets/prod.yml`).

---

## Alternatives Considered

### Option A: Databricks Repos + Manual Job Configuration
Store notebooks in Databricks Repos (Git-backed). Configure jobs, clusters, and permissions manually via the Databricks UI or REST API scripts.

**Rejected because:**
- Manual UI configuration is not reproducible. There is no single source of truth for job definitions — changes made in the UI are invisible to git history.
- REST API scripts for resource management are fragile and require custom maintenance.
- No built-in environment target concept — environment differences must be managed by hand.
- Does not scale: as the number of jobs grows, manual management becomes unmanageable.

### Option B: Terraform for Databricks Resources
Use the Databricks Terraform provider to manage all resources declaratively.

**Not chosen as primary (may be used for workspace-level infrastructure):**
- Terraform is excellent for workspace-level infrastructure (networks, Unity Catalog catalogs, storage mounts) — it will be used for that purpose.
- For job-level resources that change frequently (notebook paths, task dependencies, schedule changes), DAB provides a tighter development loop — `databricks bundle deploy` is faster than `terraform apply` for iterative development.
- DAB is purpose-built for Databricks and has first-class support for notebook-relative paths, library installs, and Python wheel deployment.

### Option C: Databricks Asset Bundles (chosen)
Declare all Databricks resources (jobs, clusters, permissions) as YAML in a `databricks.yml` bundle file. Use DAB targets for environment management.

**Accepted because:**
- Purpose-built for Databricks — native support for notebook paths, wheel uploads, job task dependencies.
- Environment targets (`dev`, `qa`, `prod`) allow the same bundle to be deployed to any environment by changing one CLI flag: `databricks bundle deploy --target prod`.
- Variable substitution handles environment-specific values (catalog names, cluster sizes) declaratively without code changes.
- Integrates natively with GitHub Actions — the Databricks CLI is available as a GitHub Action.
- Bundle validation (`databricks bundle validate`) can run in CI to catch configuration errors before deployment.
- The entire deployment is code-reviewed via git pull request.

---

## Bundle Structure

```
databricks/bundle/
├── databricks.yml              # Root bundle config, includes all resources
├── resources/
│   ├── jobs/
│   │   ├── ingestion_job.yml
│   │   ├── quality_job.yml
│   │   └── stewardship_job.yml
│   └── clusters/
│       └── shared_cluster.yml
└── targets/
    ├── dev.yml
    ├── qa.yml
    └── prod.yml
```

---

## Environment Promotion Model

**Build once, promote everywhere.** The GitHub Actions pipeline:

1. Tests and validates the bundle in DEV
2. Promotes the identical artifact to QA by deploying with `--target qa`
3. Promotes to PROD after manual approval gate with `--target prod`

No code changes between environments. Only the target configuration differs.

---

## Consequences

**Positive:**
- All Databricks resources are version-controlled and code-reviewed
- Consistent, repeatable deployments across all environments
- Environment promotion requires only a target flag change
- CI/CD integration is first-class
- DAB is the Databricks-recommended deployment mechanism going forward

**Negative:**
- DAB is a relatively new tool (GA'd 2023); some edge cases may require workarounds
- Teams unfamiliar with DAB require onboarding
- Databricks CLI must be installed and authenticated in CI runners

**Related:**
- Workspace-level infrastructure (Unity Catalog catalogs, VNet, storage mounts) will be managed by Terraform separately from DAB
