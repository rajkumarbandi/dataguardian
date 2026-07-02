# AI Integration Design

## Philosophy

AI in DataGuardian is applied only where it delivers measurable engineering value. Every AI feature is:

1. **Optional** — the platform operates fully without AI services
2. **Auditable** — AI suggestions are logged; humans make the final decision
3. **Gracefully degradable** — if Azure OpenAI is unavailable, the pipeline continues without the enrichment
4. **Not on the critical path** — AI failures never block data promotion

---

## AI Features

### 1. Schema Mapping Suggestions

**When:** During Silver transformation, when a new source column cannot be matched to the canonical schema by name or configured alias.

**How:** The source column name, sample values, and data type are sent to Azure OpenAI GPT-4o. The model returns a ranked list of candidate canonical column mappings with confidence scores. An engineer or data steward reviews the suggestions and confirms the mapping — which is then written back to the source YAML config so it is hardcoded for all future runs.

**Value:** Dramatically reduces manual effort for onboarding new source systems with non-standard naming conventions.

**Failure mode:** If the API is unavailable, the column is flagged as `_unmapped` and surfaced in the stewardship UI for manual resolution.

---

### 2. Duplicate Detection Suggestions

**When:** During Bronze → Silver deduplication, for fuzzy/semantic matches that exact-key deduplication cannot resolve.

**How:** Record text fields are converted to embeddings using Azure OpenAI text-embedding-3-large. Cosine similarity identifies candidate duplicate pairs above a configurable threshold. Pairs are surfaced in the stewardship UI for human confirmation.

**Value:** Catches duplicates arising from data entry variations (e.g., "ACME Corp" vs "Acme Corporation") that rule-based deduplication misses.

**Failure mode:** If embeddings are unavailable, exact-key deduplication still runs. Fuzzy pairs are not surfaced.

---

### 3. Data Profiling Summary

**When:** When a record or batch enters the Stewardship layer.

**How:** Column-level DQ statistics (null rates, distinct counts, range outliers, pattern anomalies) are summarized in a structured prompt to Azure OpenAI GPT-4o. The model returns a human-readable narrative summary for the business steward. This summary appears in the Streamlit stewardship UI alongside the record data.

**Value:** Business users are not data engineers. A plain-language summary ("This batch has 12% null phone numbers and 3 records with dates in the future") is far more actionable than raw statistics.

**Failure mode:** If the API is unavailable, raw statistics are shown in the UI without the narrative.

---

### 4. Natural Language to SQL

**When:** A business user types a question into the Streamlit query interface.

**How:** The question, Unity Catalog table schemas (via the schema registry), and a system prompt constraining scope to the current user's accessible tables are sent to Azure OpenAI GPT-4o. The returned SQL is validated (syntax check, table/column existence check) before execution.

**Value:** Enables business users to query Gold data without writing SQL.

**Failure mode:** If the API is unavailable, the query interface falls back to a manual SQL editor.

---

### 5. Comment Summarization

**When:** An audit report or executive summary is generated.

**How:** A batch of stewardship comments from a review period are sent to Azure OpenAI GPT-4o with a prompt to produce an executive summary of key themes, common rejection reasons, and recommendations.

**Value:** Converts raw comment logs into actionable management reporting.

**Failure mode:** Raw comments are included in the report without summarization.

---

## Implementation Pattern

AI services are implemented as **decorator classes** that wrap the base pipeline steps:

```python
# Conceptual pattern — not yet implemented
class SilverTransformationStep:
    def run(self, df: DataFrame) -> DataFrame:
        ...

class AISchemaMappingDecorator:
    def __init__(self, base: SilverTransformationStep, ai_client: AIClient) -> None:
        self._base = base
        self._ai_client = ai_client

    def run(self, df: DataFrame) -> DataFrame:
        result = self._base.run(df)
        # Enrich: attempt AI mapping for unmapped columns
        # Gracefully skip if ai_client is None or unavailable
        return result
```

This ensures the base step is testable without any AI dependency.

---

## Security Considerations

- Azure OpenAI API keys are stored in Azure Key Vault, referenced via Databricks secret scopes
- PII data is **never sent** to external AI APIs — columns marked `pii: true` in the schema contract are excluded from AI prompts
- All AI API calls are logged with request ID, model version, and latency for auditability
- AI-generated SQL goes through a validation layer before execution — no raw model output is executed directly
