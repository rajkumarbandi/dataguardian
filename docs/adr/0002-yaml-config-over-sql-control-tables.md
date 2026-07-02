# ADR-0002: YAML Configuration Files over SQL Control Tables

**Status:** Accepted
**Date:** 2026-06-29
**Author:** Platform Architecture Team

---

## Context

The platform must support multiple source systems, each with different connection details, schema mappings, quality rules, and workflow schedules. This metadata must be managed somewhere. The two dominant patterns in enterprise data engineering are SQL control tables and file-based configuration.

---

## Decision

We will manage all pipeline metadata — source system definitions, schema mappings, quality rule suites, and environment settings — in **YAML files** version-controlled alongside the application code. We will not use SQL control tables.

---

## Alternatives Considered

### Option A: SQL Control Tables
Store pipeline metadata in a relational database (e.g., Azure SQL or Delta tables used as control tables). A `sources` table, `schema_mappings` table, `quality_rules` table, etc. Pipelines read their configuration from these tables at runtime.

**Rejected because:**
- Configuration is decoupled from code. A schema mapping change is made in the database without a corresponding code review, making it invisible in the git history and impossible to roll back cleanly.
- Requires a database to be available before the pipeline can start — an additional dependency.
- No native diff/review tooling for SQL table changes. Comparing "what changed" between environments requires writing custom queries.
- Onboarding a new source system requires a database INSERT, which is not part of the standard pull request workflow.
- Environment promotion of configuration requires data migrations between databases — fragile and error-prone.

### Option B: YAML Configuration Files (chosen)
All metadata is declared in YAML files in the `config/` directory, version-controlled in GitHub alongside the application code.

**Accepted because:**
- Configuration changes go through the same pull request, review, and CI process as code changes. Every change is reviewed, tested, and traceable in git history.
- Rolling back a bad configuration change is `git revert` — no database migrations.
- Environment promotion is handled by the Databricks Asset Bundle target system — the same YAML files are deployed to DEV, QA, and PROD; only environment-specific values (catalog name, cluster config, secret scope) differ.
- Adding a new source system is a pull request — observable, reviewable, auditable.
- YAML files can be validated in CI with a schema linter, catching errors before deployment.
- No runtime database dependency for configuration loading.

---

## Implementation Notes

- The `ConfigLoader` module is the single entry point for all configuration. It handles YAML parsing, environment variable substitution, and schema validation.
- Environment-specific values (catalog names, cluster sizes, ADLS paths) are declared in `config/environments/{env}.yml` and injected by the DAB target at deployment time — never hardcoded.
- Secret references are never stored in YAML. YAML files contain only the secret *key name* (e.g., `secret_key: adls-connection-string`). The actual value is resolved at runtime from the Databricks secret scope.

---

## Consequences

**Positive:**
- Full git history for all configuration changes
- Pull request workflow enforces review for any metadata change
- No additional service dependency for configuration loading
- Consistent environment promotion model
- CI can validate YAML schemas before deployment

**Negative:**
- Config changes require a code deployment to take effect — not suitable for frequent runtime changes. (This is intentional: configuration stability is a feature, not a bug.)
- Large YAML files can become unwieldy for complex rule suites. Mitigated by keeping each entity's rules in a separate file and using YAML anchors for reuse.

**Out of scope:**
- Runtime-mutable configuration (e.g., toggling a feature without a deployment) is handled by Databricks feature flags or environment variables, not YAML config files.
