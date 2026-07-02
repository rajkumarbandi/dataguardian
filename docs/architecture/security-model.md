# Security Model

## Overview

DataGuardian adopts a defense-in-depth security posture across credential management, data access control, network isolation, and audit logging.

---

## Credential Management

**Principle:** No credentials in code or configuration files. Ever.

All secrets are stored in **Azure Key Vault** and accessed via **Databricks Secret Scopes** backed by the Key Vault instance.

Reference pattern in DAB configuration:
```yaml
# Never the actual value — always a reference
connection_string: "{{secrets/dataguardian-scope/adls-connection-string}}"
```

Reference pattern in Python:
```python
# Never os.environ['MY_KEY'] from a .env file
secret = dbutils.secrets.get(scope="dataguardian-scope", key="adls-connection-string")
```

**Secrets inventory:**

| Secret Name | Purpose | Scope |
|---|---|---|
| `adls-connection-string` | ADLS Gen2 access | Per environment |
| `azure-openai-api-key` | AI service access | Shared |
| `jdbc-{source}-password` | Source DB credentials | Per source |
| `sp-client-id` | Service Principal identity | Per environment |
| `sp-client-secret` | Service Principal credential | Per environment |

---

## Data Access Control (Unity Catalog)

Access to data assets is governed entirely through **Unity Catalog** — not filesystem ACLs or notebook-level grants.

**Access model:**

| Role | Bronze | Silver | Stewardship | Gold | Audit |
|---|---|---|---|---|---|
| Data Engineer | READ/WRITE | READ/WRITE | READ | READ | READ |
| Pipeline Service Principal | READ/WRITE | READ/WRITE | READ/WRITE | READ/WRITE | WRITE |
| Business Steward | — | READ | READ/WRITE | READ | READ |
| Data Analyst | — | — | — | READ | — |
| Admin | ALL | ALL | ALL | ALL | ALL |

Row-level security (RLS) and column-level masking for PII columns are applied at the Unity Catalog level — not in application code.

---

## PII Handling

Columns marked `pii: true` in the schema contract receive special treatment:

1. **Column-level masking** applied in Unity Catalog for non-privileged roles
2. **Excluded from AI prompts** — PII fields are stripped before any data is sent to external AI APIs
3. **Encrypted at rest** — enforced by Azure Data Lake Gen2 with customer-managed keys
4. **Audit logged** — access to PII columns is captured in Unity Catalog audit logs

---

## Network Security

- Databricks workspace deployed with **VNet injection** — no public cluster nodes
- ADLS Gen2 access via **Private Endpoints** only
- Azure Key Vault accessible only from the Databricks VNet
- GitHub Actions runners use a **self-hosted runner** on the private network for production deployments (or Azure-hosted runners with IP allowlisting for dev/qa)

---

## CI/CD Security

- GitHub repository uses **branch protection** on `main` — no direct pushes, required reviews and CI
- Databricks Service Principal credentials for CI are stored in **GitHub Actions Secrets** — never in the repository
- Deployment to PROD requires a **manual approval gate** in the GitHub Actions workflow
- `DATABRICKS_TOKEN` is scoped to the minimum required permissions per environment

---

## Audit and Compliance

- All stewardship approval events are written to `dg_{env}.audit.approval_events` — immutable, append-only
- Unity Catalog captures all data access events in the Azure Monitor-integrated audit log
- AI API calls are logged with request ID, model, token count, and latency
- Pipeline run metadata is stored in Delta for lineage tracing

---

## Related Documents

- [ADR-0003: Databricks Asset Bundles](../adr/0003-databricks-asset-bundles.md)
- [Deployment Runbook](../runbooks/deployment.md)
