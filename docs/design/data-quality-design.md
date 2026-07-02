# Data Quality Rule Engine Design

## Overview

The data quality rule engine evaluates a configurable suite of rules against a dataset and produces per-record quality scores. Rules are declared in YAML and implemented as Python classes following a common interface.

---

## Design Goals

- Rules are configured, not coded — adding a rule to a dataset requires only a YAML change
- Rules are independently testable
- New rule types are added by implementing one Python class
- Rule evaluation is vectorized using PySpark DataFrame operations — no row-by-row loops
- The engine is deterministic — same input always produces the same output

---

## Rule Interface

All rule classes must implement `BaseRule`:

```python
# Conceptual interface — implementation in src/quality/rules/base_rule.py
class BaseRule(ABC):
    def __init__(self, config: RuleConfig) -> None: ...

    @abstractmethod
    def evaluate(self, df: DataFrame) -> DataFrame:
        """
        Return the input DataFrame with two additional columns appended:
        - _rule_{rule_id}_pass: boolean (True = passed)
        - _rule_{rule_id}_detail: string (None if passed, description if failed)
        """
        ...

    @property
    @abstractmethod
    def rule_id(self) -> str: ...

    @property
    @abstractmethod
    def weight(self) -> float:
        """Contribution to overall DQ score. All weights in a suite must sum to 1.0."""
        ...
```

---

## Built-in Rule Types

### CompletenessRule
Checks that specified columns are not null or empty.

```yaml
- rule: completeness
  id: complete_required_fields
  weight: 0.35
  columns: [customer_id, full_name, email_address]
  fail_on_empty_string: true
```

### UniquenessRule
Checks that the combination of specified columns is unique within the dataset.

```yaml
- rule: uniqueness
  id: unique_customer_id
  weight: 0.25
  columns: [customer_id]
```

### RangeRule
Checks that numeric or date values fall within an expected range.

```yaml
- rule: range
  id: valid_age_range
  weight: 0.15
  column: date_of_birth
  min_value: "1900-01-01"
  max_value: "{today}"   # resolved at runtime
```

### PatternRule
Checks that string values conform to a regular expression.

```yaml
- rule: pattern
  id: valid_email_format
  weight: 0.15
  column: email_address
  pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"
  nullable: true  # null values pass the rule; use completeness rule for null checks
```

### ReferentialRule
Checks that values exist in a reference dataset (lookup table).

```yaml
- rule: referential
  id: valid_country_code
  weight: 0.10
  column: country_code
  reference_table: "{catalog}.gold.ref_countries"
  reference_column: country_code
```

---

## DQ Score Computation

The overall DQ score for a record is computed as the weighted sum of individual rule pass/fail outcomes:

```
dq_score = Σ (rule.weight × rule_pass_indicator)
```

Where `rule_pass_indicator` is `1.0` if the rule passes and `0.0` if it fails.

All weights in a rule suite must sum to `1.0`. The engine validates this at load time and raises a `ConfigurationError` if the constraint is violated.

---

## Quality Suite YAML Structure

Defined in `config/quality/{entity}_rules.yml`:

```yaml
entity: customer
version: "1.0"
dq_threshold: 0.75   # records below this score route to stewardship
rules:
  - rule: completeness
    id: complete_required_fields
    weight: 0.35
    columns: [customer_id, full_name, email_address]

  - rule: uniqueness
    id: unique_customer_id
    weight: 0.25
    columns: [customer_id]

  - rule: pattern
    id: valid_email_format
    weight: 0.20
    column: email_address
    pattern: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"
    nullable: true

  - rule: range
    id: valid_dob_range
    weight: 0.10
    column: date_of_birth
    min_value: "1900-01-01"
    max_value: "{today}"

  - rule: referential
    id: valid_country_code
    weight: 0.10
    column: country_code
    reference_table: "{catalog}.gold.ref_countries"
    reference_column: country_code
```

---

## Output Columns Added to DataFrame

After rule evaluation, the following columns are appended to every record:

| Column | Type | Description |
|---|---|---|
| `_dq_score` | double | Weighted overall score (0.0 – 1.0) |
| `_dq_completeness` | double | Completeness dimension score |
| `_dq_uniqueness` | double | Uniqueness dimension score |
| `_dq_validity` | double | Validity (pattern + range + referential) dimension score |
| `_dq_failed_rules` | array\<string\> | IDs of rules that failed |
| `_requires_stewardship` | boolean | True if `_dq_score < dq_threshold` |
| `_dq_evaluated_at` | timestamp | Timestamp of rule evaluation |

---

## DQ Report

After evaluation, a batch-level report is written to `dg_{env}.audit.dq_reports`:

| Column | Description |
|---|---|
| `batch_id` | Ingestion batch identifier |
| `entity` | Entity name |
| `evaluation_timestamp` | When the report was generated |
| `total_records` | Total records evaluated |
| `passed_records` | Records meeting the DQ threshold |
| `failed_records` | Records below the DQ threshold |
| `pass_rate` | `passed / total` |
| `avg_dq_score` | Average score across all records |
| `rule_results` | JSON — per-rule pass rate and failure count |
