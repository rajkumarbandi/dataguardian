# DataGuardian — AI Intelligence Layer

**Milestone 10 — AI-Powered Enterprise Data Stewardship**

---

## Table of Contents

1. [Purpose](#1-purpose)
2. [Architecture Overview](#2-architecture-overview)
3. [Provider Architecture](#3-provider-architecture)
4. [Prompt Architecture](#4-prompt-architecture)
5. [Feature Modules](#5-feature-modules)
6. [Token Tracking and Cost Estimation](#6-token-tracking-and-cost-estimation)
7. [Caching](#7-caching)
8. [Security](#8-security)
9. [Configuration](#9-configuration)
10. [Demo Mode](#10-demo-mode)
11. [UI Integration](#11-ui-integration)
12. [Testing](#12-testing)
13. [Extending the AI Layer](#13-extending-the-ai-layer)

---

## 1. Purpose

The DataGuardian AI Intelligence Layer adds AI-powered decision support to the Business Data Stewardship Portal. Every AI feature is designed to solve a specific business problem:

| Feature | Business Problem Solved |
|---------|------------------------|
| **DQ Assistant** | Stewards cannot diagnose root cause of DQ failures — AI explains impact in plain English |
| **Explanation Engine** | Stewards need a quick brief on why a record was flagged — AI summarises in 300 words |
| **Comment Summarizer** | Long discussion threads are time-consuming to read — AI distils them to a decision brief |
| **Root Cause Analyzer** | Individual failures hide systemic patterns — AI analyses the full batch |
| **Schema Mapper** | Manual field mapping is slow and error-prone — AI suggests mappings with confidence scores |
| **Duplicate Detector** | Entity resolution requires domain knowledge — AI handles abbreviations, rebrands, subsidiaries |
| **Natural Language SQL** | Non-technical stewards cannot query their own data — AI writes SELECT queries for them |
| **Profiling Assistant** | Column-level stats are technical — AI converts them to an executive summary |

---

## 2. Architecture Overview

```
Streamlit UI (pages/)
       │
       ▼
AIComponents (src/ai/components.py)     ← assembled once per session
       │
       ├── DQAssistant
       ├── ExplanationEngine
       ├── CommentSummarizer
       ├── RootCauseAnalyzer
       ├── SchemaMapper
       ├── DuplicateDetector
       ├── NaturalLanguageSQL
       └── ProfilingAssistant
               │
               ▼
       PromptManager (config/prompts/*.yml)
               │
               ▼
       LLMProvider (ABC)
         ├── MockProvider        — deterministic, no API key
         ├── OpenAIProvider      — gpt-4o, gpt-4o-mini
         ├── AzureOpenAIProvider — Azure-hosted OpenAI
         └── AnthropicProvider   — Claude models
               │
               ▼
       PromptCache (hash-based TTL)
       TokenCounter (cost tracking)
```

### Layering rules

- **UI pages never call providers directly.** All AI calls go through feature modules.
- **Feature modules never hardcode prompts.** All prompt text lives in YAML files.
- **Feature modules never instantiate their own dependencies.** All wiring happens in `AIComponents`.
- **All features work in demo mode** using `MockProvider` without any API key.

---

## 3. Provider Architecture

`src/ai/provider.py` defines the `LLMProvider` ABC and four concrete implementations.

### LLMProvider interface

```python
class LLMProvider(ABC):
    def complete(self, messages: list[LLMMessage], max_tokens: int, temperature: float) -> LLMResponse
    def provider_name(self) -> str    # "mock" | "openai" | "azure_openai" | "anthropic"
    def model_name(self) -> str       # "mock-gpt-4" | "gpt-4o" | etc.
```

### Switching providers

Set `DG_AI_PROVIDER` and ensure the corresponding API credentials are in Databricks Secrets or environment variables:

| Provider | Env Var | Secrets Key |
|----------|---------|-------------|
| `openai` | `OPENAI_API_KEY` | `openai_api_key` |
| `azure_openai` | `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT` | `azure_openai_api_key`, `azure_openai_endpoint` |
| `anthropic` | `ANTHROPIC_API_KEY` | `anthropic_api_key` |

No code changes are required to switch providers — only configuration changes.

### MockProvider

`MockProvider` uses a hash-based selection to return deterministic, realistic responses from a pre-written pool. The same question always produces the same response. Different questions rotate through the pool. No API key required — ideal for demos and testing.

---

## 4. Prompt Architecture

All prompts live in `config/prompts/` as YAML files. Feature modules never contain raw prompt strings.

### Prompt file structure

```yaml
system: |
  You are a data quality expert for DataGuardian...
  [System instructions that never change]

user: |
  Analyse the following:
  Source: {{ source_name }}
  Violation: {{ rule_name }}
  [User input with {{ variable }} placeholders]
```

### Variable substitution

`PromptManager.render(feature, variables)` replaces `{{ variable }}` placeholders. Unreplaced variables raise `PromptRenderError` — this prevents incomplete prompts from reaching the LLM.

### Available prompts

| File | Feature |
|------|---------|
| `dq_explanation.yml` | DQAssistant |
| `schema_mapping.yml` | SchemaMapper |
| `root_cause.yml` | RootCauseAnalyzer |
| `duplicate_detection.yml` | DuplicateDetector |
| `comment_summary.yml` | CommentSummarizer |
| `nl_sql.yml` | NaturalLanguageSQL |
| `data_profiling.yml` | ProfilingAssistant |
| `record_explanation.yml` | ExplanationEngine |

---

## 5. Feature Modules

### DQAssistant (`src/ai/dq_assistant.py`)

Explains a single DQ rule failure in plain English with a business impact assessment.

```python
explanation = ai.dq_assistant.explain_failure(record_dict, failed_rule_dict)
# explanation.explanation  — full markdown explanation
# explanation.risk_level   — "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
# explanation.cached       — True if served from cache
```

### ExplanationEngine (`src/ai/explanation_engine.py`)

Generates a 300-word plain-English brief explaining why a record was flagged.

```python
explanation = ai.explanation_engine.explain_record(record_dict)
# explanation.explanation        — full markdown brief
# explanation.recommended_action — "Approve" | "Reject" | "Request Correction"
```

### CommentSummarizer (`src/ai/comment_summarizer.py`)

Distils a stewardship discussion thread into a concise decision brief.

```python
summary = ai.comment_summarizer.summarize(record_dict, comments_df)
# summary.summary      — executive brief in markdown
# summary.participants — list of participants
```

### RootCauseAnalyzer (`src/ai/root_cause_analyzer.py`)

Analyses a batch of records to identify systemic DQ failure patterns.

```python
report = ai.root_cause_analyzer.analyze_batch(records_df, source_name="customers", batch_id="B001")
# report.report         — structured markdown report
# report.top_violations — [{"rule_name": ..., "count": ..., "pct": ...}]
# report.failure_rate   — float (percentage)
```

### SchemaMapper (`src/ai/schema_mapper.py`)

Suggests source-to-target field mappings with confidence scores.

```python
result = ai.schema_mapper.suggest_mappings(
    source_fields=["CustomerName", "DOB"],
    target_fields=["customer_name", "birth_date"],
    source_system="Salesforce",
    domain="CRM",
)
# result.mappings              — list[FieldMapping]
# result.unmapped_fields       — list[str]
# result.high_confidence_count — int
```

### DuplicateDetector (`src/ai/duplicate_detector.py`)

Identifies likely duplicate entities using semantic analysis.

```python
result = ai.duplicate_detector.detect(
    candidates=[DuplicateCandidate(
        record_ids=["r1", "r2"],
        field_values={"name": ["IBM Corporation", "International Business Machines"]},
    )],
    entity_type="vendor",
    domain="Procurement",
)
# result.analysis — structured markdown with merge/review/skip recommendations
```

### NaturalLanguageSQL (`src/ai/natural_language_sql.py`)

Converts plain-English questions to SELECT queries with automatic security validation.

```python
result = ai.natural_language_sql.query(
    "Show me all pending records with a DQ score below 0.7",
    tables={"stewardship_records": df},   # for demo execution
)
# result.sql         — generated SELECT SQL
# result.data        — pd.DataFrame (demo mode) or None
# result.success     — True if SQL generated and executed without error
```

**Security guarantees:**
- Only SELECT statements are accepted
- DDL/DML keywords are blocked (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, MERGE)
- SQL comments are blocked (-- and /* */)
- Generated SQL is shown to the user before execution
- LIMIT clause enforced (max 1000 rows)

### ProfilingAssistant (`src/ai/profiling_assistant.py`)

Generates an AI data quality executive summary from batch statistics.

```python
profile = ai.profiling_assistant.profile(records_df, source_name="customers", batch_id="B001")
# profile.summary        — executive brief with markdown
# profile.column_issues  — list[ColumnIssue] with risk levels
# profile.failure_rate   — float
```

---

## 6. Token Tracking and Cost Estimation

`TokenCounter` accumulates token usage across all AI calls in a session.

```python
stats = ai.token_counter.stats()
# {
#   "total_tokens": 12450,
#   "total_calls": 23,
#   "estimated_cost_usd": 0.0312,
#   "by_feature": {
#     "dq_assistant": {"prompt_tokens": 450, "completion_tokens": 890, "call_count": 5, ...},
#     ...
#   }
# }
```

Cost per 1,000 tokens is configured in `config/ai.yml` under `pricing`.
In demo mode (MockProvider) the cost is always $0.00.

---

## 7. Caching

`PromptCache` uses SHA-256 hashes of the serialised message list as keys. Cache is in-memory, session-scoped, and resets when the Streamlit app restarts.

| Setting | Default | Description |
|---------|---------|-------------|
| `cache.enabled` | `true` | Enable/disable caching |
| `cache.ttl_seconds` | `3600` | Time-to-live per entry (1 hour) |
| Max size | `512` | Oldest entry evicted on overflow |

Cache statistics are available on the **Usage Stats** tab in the AI Assistant page.

---

## 8. Security

### Credential management

API keys and endpoints are never stored in source code or configuration files. They are resolved through:

1. **Databricks Secrets** (production) — `dbutils.secrets.get(scope, key)`
2. **Environment variables** (fallback) — `OPENAI_API_KEY`, `AZURE_OPENAI_API_KEY`, etc.

The `SecretsManager` from M8 (`src/common/secrets.py`) handles this transparently.

### NL SQL security

All generated SQL is validated before execution:
- Must start with `SELECT`
- Forbidden keywords block execution: INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, MERGE
- SQL comments blocked: `--` and `/* */`
- SQL is displayed to the user for review before execution

### PII in prompts

The prompts are designed to avoid sending actual PII to the LLM:
- **DQAssistant**: Sends field names, expected values, rule names — not bulk record data
- **RootCauseAnalyzer**: Sends aggregated statistics and anonymised failure snippets
- **ProfilingAssistant**: Sends column statistics (null rates, counts) — not field values

For higher PII sensitivity requirements, implement a `PIIRedactor` at the feature module level before calling `PromptManager.render()`.

---

## 9. Configuration

### `config/ai.yml`

```yaml
provider:
  name: mock              # mock | openai | azure_openai | anthropic
  model: mock-gpt-4

temperature: 0.1
max_tokens: 2048

cache:
  enabled: true
  ttl_seconds: 3600

pricing:
  prompt_per_1k: 0.0025
  completion_per_1k: 0.0100

features:
  dq_assistant: true
  schema_mapper: true
  # ... (all features enabled by default)
```

### Environment variable overrides

| Variable | Default | Description |
|----------|---------|-------------|
| `DG_AI_PROVIDER` | `mock` | Provider name override |
| `DG_AI_MODEL` | `mock-gpt-4` | Model name override |
| `OPENAI_API_KEY` | — | OpenAI API key |
| `AZURE_OPENAI_API_KEY` | — | Azure OpenAI key |
| `AZURE_OPENAI_ENDPOINT` | — | Azure OpenAI endpoint URL |
| `ANTHROPIC_API_KEY` | — | Anthropic API key |

---

## 10. Demo Mode

When no API key is configured or `provider.name: mock` is set in `config/ai.yml`, the MockProvider activates automatically. Every AI feature produces realistic, pre-written responses:

- **Schema mapping**: Returns field mappings with confidence scores and reasoning
- **DQ explanations**: Returns business impact assessments with risk levels
- **Root cause reports**: Returns structured analysis with failure pattern breakdown
- **Duplicate detection**: Returns entity resolution analysis with merge recommendations
- **Comment summaries**: Returns executive briefs with blockers and next steps
- **NL SQL**: Returns valid SELECT queries matching common questions
- **Data profiling**: Returns executive summaries with column health tables
- **Record explanations**: Returns plain-English briefs with recommended actions

**Response determinism**: Same input → same response (hash-based selection). Different inputs → different responses (rotates through the pool).

---

## 11. UI Integration

### AI Assistant page (`pages/08_ai_assistant.py`)

New navigation entry under **AI Intelligence**. Four tabs:

| Tab | Feature |
|-----|---------|
| 💬 Ask Data | NaturalLanguageSQL — question → SQL → results |
| 🔀 Schema Mapping | SchemaMapper — source fields → target mappings |
| 📊 Batch Analysis | RootCauseAnalyzer, ProfilingAssistant, DuplicateDetector |
| 📈 Usage Stats | TokenCounter + PromptCache statistics |

### Record Review page (`pages/03_review.py`)

A new **🤖 AI Insights** tab is added to the existing 4-tab layout. Contains:

1. **Plain-English Record Explanation** — one-click ExplanationEngine summary
2. **Violation Business Impact** — expandable per-violation DQAssistant explanations
3. **Discussion Summary** — one-click CommentSummarizer brief

---

## 12. Testing

All 6 test files run without an API key (MockProvider):

```bash
pytest tests/unit/test_m10_provider.py       # LLMProvider ABC + MockProvider
pytest tests/unit/test_m10_prompt_manager.py # PromptManager + production prompts
pytest tests/unit/test_m10_cache.py          # PromptCache TTL + stats
pytest tests/unit/test_m10_dq_assistant.py   # DQAssistant + token counter
pytest tests/unit/test_m10_schema_mapper.py  # SchemaMapper + _parse_mappings
pytest tests/unit/test_m10_nl_sql.py         # NaturalLanguageSQL + security validation
```

---

## 13. Extending the AI Layer

### Adding a new AI feature

1. Add a prompt YAML file to `config/prompts/<feature_name>.yml`
2. Create `src/ai/<feature_name>.py` with a result dataclass and a service class
3. Add the feature instance to `AIComponents` in `src/ai/components.py`
4. Add it to `src/ai/__init__.py` exports
5. Add the feature flag to `config/ai.yml` under `features`
6. Add unit tests to `tests/unit/test_m10_<feature_name>.py`

### Switching to a real LLM (Azure OpenAI)

1. Create a Databricks Secret scope: `dataguardian-ai`
2. Add secrets: `azure_openai_api_key`, `azure_openai_endpoint`
3. Set in `config/ai.yml`:
   ```yaml
   provider:
     name: azure_openai
     deployment_name: your-gpt4o-deployment
   ```
4. Or set `DG_AI_PROVIDER=azure_openai` as an environment variable in `app.yaml`

### Future: RAG and Vector Search

The prompt-based architecture is designed to accept retrieved context as a template variable. To add RAG:
1. Add a `context` variable to the relevant prompt YAML
2. Implement a `VectorStore` service that retrieves relevant chunks
3. Pass retrieved text as `variables["context"]` in the feature module
4. No changes to the provider or cache layers required

---

*DataGuardian — Milestone 10: AI Intelligence Layer*
