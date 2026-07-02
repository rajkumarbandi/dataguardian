# DataGuardian — Engineering Coding Standards

This document defines the coding standards, conventions, and quality expectations for all code in this repository. These standards apply to all contributors without exception.

**Tooling enforces most of these rules automatically.** Running `make check-all` before pushing catches the majority of violations before code review.

---

## Table of Contents

- [Python Standards](#python-standards)
- [PySpark Standards](#pyspark-standards)
- [Naming Conventions](#naming-conventions)
- [Type Annotations](#type-annotations)
- [Error Handling](#error-handling)
- [Documentation](#documentation)
- [Testing Standards](#testing-standards)
- [Configuration Standards](#configuration-standards)
- [Security Standards](#security-standards)
- [Git and Version Control](#git-and-version-control)
- [Forbidden Patterns](#forbidden-patterns)

---

## Python Standards

### Language Version

Python 3.11+ is the minimum version. Use modern language features actively:
- `match` / `case` for state machine logic (approval state transitions)
- `TypeAlias` for complex type definitions
- `dataclasses` or `pydantic.BaseModel` for structured config objects — not plain dicts
- `pathlib.Path` — never `os.path`
- `|` union type syntax — never `Optional[X]` or `Union[X, Y]`

### Formatting

All code is formatted by `ruff format`. The configuration is in `pyproject.toml`. Do not configure your editor to use a different formatter.

- Line length: **100 characters**
- Quote style: **double quotes**
- Trailing commas: always in multi-line structures

### Linting

`ruff check` with the configured rule set is mandatory. Zero warnings are acceptable in CI. Never use `# noqa` to silence a warning without a comment explaining why.

```python
result = some_function()  # noqa: SIM117 — nested context managers required here due to X
```

### Imports

Order enforced by ruff/isort:
1. Standard library
2. Third-party packages
3. Internal packages (`src.*`)

Use absolute imports within `src/`. Never use relative imports (`from ..common import X`).

```python
# Correct
from src.common.exceptions import ConfigurationError
from src.common.logger import get_logger

# Wrong
from ..common.exceptions import ConfigurationError
```

### Module Structure

Every Python module follows this layout, in order:

```python
"""One-line module docstring describing its responsibility."""

from __future__ import annotations

# Standard library imports
import sys
from pathlib import Path

# Third-party imports
import yaml
from pydantic import BaseModel

# Internal imports
from src.common.exceptions import ConfigurationError
from src.common.logger import get_logger

# Module-level constants (UPPER_SNAKE_CASE)
DEFAULT_TIMEOUT = 30

# Type aliases
ColumnMapping = dict[str, str]

# Classes, then functions
```

---

## PySpark Standards

### DataFrame Operations

- **Never iterate over rows.** All transformations must use DataFrame API operations or SQL expressions. Row-by-row processing negates every benefit of distributed computing.
- **Never collect large DataFrames.** `.collect()` is only permitted in tests and for small metadata payloads (< 1000 rows).
- **Prefer `F.` prefix** for all Spark functions to avoid name collisions with Python builtins.

```python
# Correct
from pyspark.sql import functions as F

df = df.withColumn("_dq_score", F.lit(0.0))
df = df.filter(F.col("country_code").isNotNull())

# Wrong
df = df.withColumn("_dq_score", lit(0.0))  # ambiguous import
```

### Schema Handling

- **Always define schemas explicitly** for structured sources. Do not rely on schema inference in production code — it is a source of subtle bugs and performance penalties.
- Declare schemas using `pyspark.sql.types.StructType`, not string DDL in production code.

```python
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

BRONZE_METADATA_SCHEMA = StructType([
    StructField("_batch_id", StringType(), nullable=False),
    StructField("_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("_source_system", StringType(), nullable=False),
])
```

### Partitioning and Performance

- Always partition Bronze tables by `ingestion_date`.
- Set `spark.sql.shuffle.partitions` appropriately per environment (configured in `config/environments/{env}.yml`). Do not hardcode shuffle partition counts in application code.
- Use `MERGE` (Delta `DeltaTable.forName(...).merge(...)`) for all upsert operations — never `overwrite` on tables that accumulate data across runs.

### Null Handling

Do not assume columns are non-null even if the schema declares them non-nullable. Source systems lie. Apply explicit null checks and fail loudly in DQ rules rather than silently propagating nulls.

```python
# Correct — explicit null coalescing with an audit flag
df = df.withColumn(
    "email_address",
    F.when(F.col("email_address").isNull(), F.lit(None)).otherwise(
        F.lower(F.trim(F.col("email_address")))
    )
)

# Wrong — assumes email_address is never null
df = df.withColumn("email_address", F.lower(F.trim(F.col("email_address"))))
```

---

## Naming Conventions

### Python

| Element | Convention | Example |
|---|---|---|
| Module | `snake_case` | `schema_mapper.py` |
| Class | `PascalCase` | `SchemaMapper` |
| Function / method | `snake_case` | `map_columns()` |
| Variable / parameter | `snake_case` | `source_config` |
| Constant | `UPPER_SNAKE_CASE` | `DEFAULT_DQ_THRESHOLD` |
| Private attribute | `_snake_case` | `_cached_schema` |
| Type alias | `PascalCase` | `ColumnMapping` |
| Protocol | `PascalCase` | `AIService` |

### DataFrame Columns

Internal pipeline columns — appended by DataGuardian, not present in source data — are always prefixed with `_`:

| Prefix | Purpose | Examples |
|---|---|---|
| `_` | Internal pipeline metadata | `_batch_id`, `_ingestion_timestamp`, `_source_system` |
| `_dq_` | Data quality scores and flags | `_dq_score`, `_dq_completeness`, `_requires_stewardship` |
| `_rule_` | Per-rule pass/fail | `_rule_complete_required_fields_pass` |
| `_unmapped_` | Source columns without canonical mapping | `_unmapped_legacy_ref` |
| `_type_error_` | Type coercion failure flag | `_type_error_date_of_birth` |

Source columns must never be renamed or dropped in Bronze. All transformations happen in Silver.

### Unity Catalog Objects

```
Catalog:   dg_{env}
Schema:    bronze | silver | stewardship | gold | audit
Table:     {source}_{entity}   (bronze)
           {entity}             (silver, gold)
           {entity}_pending     (stewardship)
           {event_type}         (audit)
```

### Configuration Keys

All YAML keys use `snake_case`. Boolean values use `true` / `false` (YAML built-in). String values that contain path-like references use `{placeholder}` notation for runtime substitution.

### File Names

- Python modules: `snake_case.py`
- YAML configuration files: `snake_case.yml`
- Documentation: `kebab-case.md`
- Test files: `test_{module_name}.py`

---

## Type Annotations

Type annotations are **mandatory** on all public functions, methods, and class attributes. `mypy --strict` is enforced in CI.

### Rules

- Use `from __future__ import annotations` in every module for deferred evaluation.
- Use `X | None` — never `Optional[X]`.
- Use `X | Y` — never `Union[X, Y]`.
- Use built-in generic types (`list[str]`, `dict[str, int]`) — never `List[str]` from `typing`.
- Use `TYPE_CHECKING` to import types that are only needed for annotations.

```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def map_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Apply column name mapping from source to canonical names."""
    ...
```

### Return Types

Never use `Any` as a return type or annotation. If you cannot express the type, use a TypeVar or a Protocol. `Any` in annotations is a signal that the design needs revisiting.

### Pydantic for Config Objects

All configuration structures parsed from YAML must be validated using Pydantic models — never raw `dict[str, Any]`.

```python
from pydantic import BaseModel, EmailStr, field_validator


class StewardshipConfig(BaseModel):
    approvers: list[str]
    escalation_contact: str
    sla_hours: int

    @field_validator("sla_hours")
    @classmethod
    def sla_must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("sla_hours must be a positive integer")
        return v
```

---

## Error Handling

### Use the Domain Exception Hierarchy

All exceptions raised by DataGuardian code must be subclasses of `DataGuardianError` defined in `src/common/exceptions.py`. Never raise generic `Exception`, `ValueError`, or `RuntimeError` from application code.

```python
# Correct
from src.common.exceptions import ConfigurationError

raise ConfigurationError(f"Required key 'entity' missing in source config: {source_name!r}")

# Wrong
raise ValueError(f"Missing entity")
```

### Exception Specificity

Match the exception type to the failure domain:

| Exception | When to raise |
|---|---|
| `ConfigurationError` | Invalid or missing YAML configuration |
| `SchemaConformanceError` | DataFrame does not conform to canonical schema |
| `ConnectorError` | Source connector cannot read data |
| `InvalidStateTransitionError` | Illegal stewardship state transition |
| `QualityRuleError` | DQ rule fails to evaluate (config or runtime error) |
| `AIEnrichmentError` | AI service failure — caught internally, never propagated |

### AI Services: Never Propagate Exceptions

AI service methods must catch all exceptions internally, log them as warnings, and return `None`. An AI failure must never cause a pipeline failure.

```python
def enrich(self, input: EnrichmentInput) -> EnrichmentResult | None:
    try:
        response = self._client.chat.completions.create(...)
        return self._parse_response(response)
    except Exception:
        self._logger.warning("AI enrichment failed", exc_info=True)
        return None
```

### Context in Error Messages

Error messages must include enough context to diagnose the problem without needing to reproduce it:

```python
# Correct — tells you exactly where and what
raise ConfigurationError(
    f"Rule weights in 'config/quality/{entity}_rules.yml' sum to {total:.3f}; "
    f"they must sum to exactly 1.0."
)

# Wrong — useless in a distributed log
raise ConfigurationError("Invalid weights")
```

---

## Documentation

### Docstrings

Public classes and functions require a one-line docstring. Longer explanations may follow, separated by a blank line. Do not document what the code obviously does — document *why*, or *what constraint* applies.

```python
class GoldPublisher:
    """Promotes APPROVED stewardship records to the Gold Delta layer via MERGE."""

    def publish(self, entity: str) -> int:
        """
        Merge all APPROVED, unpromoted records for the entity into Gold.

        Returns the number of records promoted. Idempotent — safe to re-run.
        Records are marked _promoted=true in the stewardship table after promotion
        to prevent double-counting on re-runs.
        """
```

Do not write docstrings that re-state the function signature. These add noise without value:

```python
# Wrong
def get_schema(self, entity: str) -> SchemaContract:
    """
    Get the schema for an entity.

    Args:
        entity: The entity name.

    Returns:
        The schema contract.
    """
```

### Inline Comments

Write inline comments only when the *why* is non-obvious: a hidden constraint, a workaround for a known external issue, or a subtle invariant. If the code is clear, no comment is needed.

```python
# Correct — explains a non-obvious constraint
# Delta MERGE requires the source DataFrame to be cached when the target is large;
# without this, the query plan re-evaluates the source for each WHEN clause.
source_df.cache()

# Wrong — restates what the code already says
# Iterate over columns
for col in columns:
```

---

## Testing Standards

### Test File Organisation

Every module in `src/` has a corresponding test file in `tests/unit/`:

```
src/quality/rule_engine.py  →  tests/unit/test_rule_engine.py
src/schema/schema_mapper.py →  tests/unit/test_schema_mapper.py
```

Integration tests live in `tests/integration/` and require a Spark session. They are marked `@pytest.mark.integration` and are not run in standard CI.

### Test Naming

Test function names follow the pattern `test_{scenario}_{expected_outcome}`:

```python
def test_completeness_rule_fails_when_required_column_is_null() -> None: ...
def test_completeness_rule_passes_empty_dataframe_without_error() -> None: ...
def test_approval_engine_raises_on_invalid_state_transition() -> None: ...
```

### Test Coverage Requirements

- Minimum **80% line coverage** for all new code (enforced by `pytest-cov` in CI)
- **100% branch coverage** for state machine logic (approval transitions)
- **100% branch coverage** for rule weight validation

### Required Edge Cases

Every DQ rule implementation must test:
- Empty DataFrame (zero rows)
- DataFrame with all nulls in the checked column
- DataFrame with all values passing
- DataFrame with all values failing
- Boundary values (min, max, min±1, max±1 for range rules)

### Test Independence

Tests must not depend on each other. Shared state between tests is a bug, not a feature. Use `pytest` fixtures for setup and teardown. Never rely on test execution order.

### No External Dependencies in Unit Tests

Unit tests must run without:
- A Spark session (use mocks or in-memory test doubles)
- Network access
- File system access outside `tests/fixtures/`
- Databricks workspace access

If your unit test needs Spark, it is an integration test and belongs in `tests/integration/`.

---

## Configuration Standards

### The ConfigLoader Is the Only Configuration Entry Point

Never read YAML files directly in pipeline code. Always use `ConfigLoader`.

```python
# Correct
config = ConfigLoader(env="dev")
source_config = config.get_source("erp_customers")

# Wrong — bypasses validation and substitution
import yaml
with open("config/sources/erp_customers.yml") as f:
    source_config = yaml.safe_load(f)
```

### No Hardcoded Values in Pipeline Code

Environment names, catalog names, schema names, ADLS paths, DQ thresholds — none of these belong in Python code. They are configuration.

```python
# Correct — resolved from config at runtime
catalog = env_config.unity_catalog.catalog
bronze_table = f"{catalog}.bronze.{source}_{entity}"

# Wrong
bronze_table = "dg_dev.bronze.erp_customers"
```

### YAML Template Discipline

Every new YAML key must be documented in the corresponding `_template.yml` file with an inline comment explaining its purpose and allowed values.

---

## Security Standards

### Secrets

- **Zero tolerance for committed secrets.** Pre-commit hooks block any file containing strings that match secret patterns.
- All secrets are stored in Azure Key Vault and accessed via Databricks secret scopes.
- Python code accesses secrets via `dbutils.secrets.get(scope, key)` — never via environment variables in production.
- Never log secret values. Log the key name only.

```python
# Correct
logger.info("Resolving connection secret", scope=scope, key=secret_key)
value = dbutils.secrets.get(scope=scope, key=secret_key)

# Wrong
logger.info("Connection string resolved", value=connection_string)
```

### PII

- Columns marked `pii: true` in the schema contract are never included in AI prompts, log messages, or error message text.
- PII columns are masked at the Unity Catalog layer — not in application code.
- Test fixtures must never contain real PII. Use synthetic data only.

### Input Validation

Validate all external input (YAML config files, API responses, user input to the Streamlit app) at the boundary using Pydantic. Do not pass unvalidated data into pipeline logic.

### SQL Injection

Never use string concatenation to build Spark SQL queries. Use parameterised expressions via the DataFrame API or `spark.sql()` with variable bindings.

```python
# Correct
df = df.filter(F.col("entity") == entity_name)

# Wrong — SQL injection risk
df = spark.sql(f"SELECT * FROM table WHERE entity = '{entity_name}'")
```

---

## Git and Version Control

### Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short imperative description>

[optional body — explain WHY, not WHAT]

[optional footer — breaking changes, issue references]
```

**Types:** `feat` | `fix` | `refactor` | `test` | `docs` | `chore` | `perf`

**Scopes:** `ingestion` | `schema` | `quality` | `stewardship` | `publishing` | `ai` | `app` | `common` | `config` | `ci` | `databricks`

**Examples:**
```
feat(quality): add referential integrity rule type
fix(ingestion): handle empty ADLS directory without raising ConnectorError
refactor(stewardship): extract SLA monitor into standalone class
docs(adr): add ADR-005 for Unity Catalog naming convention
chore(ci): pin Databricks CLI to v0.220 to fix bundle validate regression
```

### Branch Naming

```
feat/add-jdbc-connector
fix/null-handling-in-range-rule
docs/update-deployment-runbook
chore/upgrade-ruff-to-0.5
```

### What Not to Commit

- `.env` files
- `__pycache__/` directories
- `.mypy_cache/`, `.ruff_cache/`, `.pytest_cache/`
- `htmlcov/` coverage reports
- Databricks notebook checkpoint files (`.ipynb_checkpoints/`)
- Any file containing actual credentials, connection strings, or API keys

---

## Forbidden Patterns

These patterns are prohibited in all production code:

| Forbidden | Reason | Alternative |
|---|---|---|
| `df.toPandas()` on large DataFrames | OOM risk on large datasets | Use Spark DataFrame operations |
| `df.collect()` without row limit | OOM risk | Use `.limit(n).collect()` or aggregate first |
| `import *` | Pollutes namespace, hides dependencies | Explicit imports only |
| `print()` in `src/` | Not captured in structured logs | Use `get_logger()` |
| `os.environ.get("MY_SECRET")` in production | Bypasses Key Vault | `dbutils.secrets.get()` |
| `except Exception: pass` | Silently swallows errors | Log the exception, return `None` for AI services |
| Hardcoded catalog/schema/table names | Breaks environment promotion | Resolve from config |
| `time.sleep()` in pipeline code | Hides performance problems | Fix the underlying issue |
| `global` variables | Thread-safety and testability problems | Use dependency injection |
| Mutable default arguments | Classic Python bug | Use `None` and assign in the function body |

```python
# Mutable default argument — forbidden
def process(columns: list[str] = []) -> None: ...

# Correct
def process(columns: list[str] | None = None) -> None:
    if columns is None:
        columns = []
```
