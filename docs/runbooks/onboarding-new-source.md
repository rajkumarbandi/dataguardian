# Runbook: Onboarding a New Source System

## Overview

This runbook walks through the steps to onboard a new data source into DataGuardian. The process is entirely configuration-driven — no application code changes are required for standard source types.

**Estimated time:** 2–4 hours for a standard source

---

## Step 1: Understand the Source

Before writing any configuration, gather the following information:

- [ ] Source system name and type (ADLS, JDBC, REST API)
- [ ] Connection details (host, path, database name — not credentials)
- [ ] Entity being ingested (e.g., customers, products, orders)
- [ ] Source schema — all column names and data types
- [ ] Business key — which columns uniquely identify a record
- [ ] Ingestion frequency (hourly, daily, weekly)
- [ ] Data volume and expected row count
- [ ] Data owner and steward contact(s)
- [ ] SLA for stewardship review

---

## Step 2: Configure the Source

Create a new source config file: `config/sources/{source_name}.yml`

Use the template at `config/sources/_source_template.yml` as a starting point.

**Minimum required fields:**

```yaml
source:
  name: <source_name>           # unique identifier, snake_case
  entity: <entity_name>         # canonical entity (must match a schema contract)
  connector: adls               # adls | jdbc | api
  format: parquet               # parquet | csv | json | delta
  location: "{adls_root}/raw/{source_name}/"

  stewardship:
    approvers: ["steward@company.com"]
    sla_hours: 48
```

---

## Step 3: Define the Schema Contract

If a canonical schema contract does not yet exist for this entity, create one:
`config/schemas/{entity_name}.yml`

Use the template at `config/schemas/_schema_template.yml`.

If the schema contract already exists, add the source column aliases under each column's `aliases` list so the mapper can match them without AI assistance.

---

## Step 4: Define Quality Rules

Create a quality rule suite for this entity if one does not exist:
`config/quality/{entity_name}_rules.yml`

Use the template at `config/quality/_rules_template.yml`.

Start with:
- Completeness rules on the business key and mandatory columns
- Uniqueness rule on the business key
- Pattern rules for any structured fields (email, phone, date)

Agree with the data owner on the DQ threshold before deployment.

---

## Step 5: Register the Secret

Add any required connection credentials to Azure Key Vault and ensure the secret is accessible from the Databricks secret scope:

```bash
# Verify the secret is accessible from Databricks
databricks secrets get-secret --scope dataguardian-scope --key <secret-name>
```

Reference the secret name (not value) in the source config:

```yaml
connection:
  secret_scope: dataguardian-scope
  secret_key: <source-name>-connection-string
```

---

## Step 6: Validate the Configuration

Run the config validator to catch YAML syntax errors, missing required fields, and schema inconsistencies:

```bash
python scripts/validate_config.py --env dev --source <source_name>
```

---

## Step 7: Test in DEV

1. Create a feature branch: `git checkout -b feat/onboard-{source_name}`
2. Add the new config files
3. Run `make validate-config`
4. Push the branch and open a pull request
5. After CI passes and review is approved, merge to `main`
6. DEV deployment triggers automatically
7. Run the ingestion job manually in DEV to validate end-to-end:
   ```bash
   databricks bundle run ingestion_job --target dev
   ```
8. Verify Bronze and Silver tables are created
9. Verify DQ scores are computed
10. Log into the Streamlit app and validate stewardship records appear

---

## Step 8: Promote to QA and PROD

Follow the standard promotion process in the [Deployment Runbook](deployment.md).

---

## Troubleshooting

**Source not found / connection error:**
- Verify the secret is present in the Key Vault and accessible from the secret scope
- Verify the ADLS path or JDBC host is reachable from the Databricks cluster

**Schema mismatch / unmapped columns:**
- Check the Streamlit schema mapping review page for AI suggestions
- Add confirmed mappings to the source config YAML and redeploy

**All records failing DQ:**
- Review the DQ report in `dg_dev.audit.dq_reports`
- Check if the DQ threshold is appropriate for initial onboarding
- Work with the data owner to understand expected data quality for this source

**No records in stewardship:**
- Verify `dq_threshold` is set and some records fall below it
- If all records pass DQ, they go directly to Gold — this is correct behavior
