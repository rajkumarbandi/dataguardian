# ADR-0004: AI Features Implemented as Optional Enrichment Decorators

**Status:** Accepted
**Date:** 2026-06-29
**Author:** Platform Architecture Team

---

## Context

DataGuardian includes several AI-powered features (schema mapping suggestions, duplicate detection, data profiling narratives, natural language SQL). A key architectural decision is where AI fits in the pipeline — as a core dependency or as an optional enhancement.

---

## Decision

All AI features will be implemented as **optional enrichment steps** — decorators or post-processing stages that wrap core pipeline steps. The core pipeline must function completely without any AI service dependency. AI unavailability (API errors, quota limits, network issues) must never cause pipeline failures or block data promotion.

---

## Alternatives Considered

### Option A: AI as Core Pipeline Step
AI features are integrated directly into core pipeline stages. For example, the Silver transformation job calls Azure OpenAI to suggest schema mappings before proceeding.

**Rejected because:**
- Creates a hard dependency on an external API in the critical path. An Azure OpenAI outage (which has a different SLA than Databricks) blocks the entire data pipeline.
- Significantly increases pipeline latency — waiting on API calls for every batch.
- Makes testing harder: unit tests require mocking or live API access.
- Couples business-critical data processing to an AI service that may change pricing, rate limits, or model behavior at any time.

### Option B: Separate AI Enrichment Jobs (chosen approach)
AI features run as separate, independent jobs in the Databricks Workflow. They are triggered asynchronously after core pipeline stages complete. If they fail, core pipeline stages are unaffected.

**Accepted because:**
- Core pipeline (ingest → standardize → quality → stewardship → promote) runs independently of all AI services.
- AI enrichment failure results in missing optional fields (e.g., `_ai_profile_summary = NULL`) — not pipeline failure.
- AI jobs can be retried independently without re-running the full pipeline.
- AI job concurrency can be tuned separately from core pipeline concurrency.
- Testing core pipeline logic does not require any AI mock setup.

### Option C: AI as Synchronous Decorator
AI features are wrapped around core step classes using the decorator pattern. The decorator checks if AI is available, calls it if so, and falls through if not.

**Also acceptable** for synchronous enrichment needs (e.g., column-level schema mapping where a suggestion is needed inline). Will be used for the schema mapper in the Silver transformation step.

---

## Implementation Contract

Every AI service class must implement the following contract:

```python
class AIService(Protocol):
    def is_available(self) -> bool:
        """Return True if the AI backend is reachable and configured."""
        ...

    def enrich(self, input: EnrichmentInput) -> EnrichmentResult | None:
        """
        Return enrichment result, or None if unavailable.
        Must NEVER raise an exception that propagates to the caller.
        """
        ...
```

The `enrich()` method is responsible for catching all API errors, logging them with structured context, and returning `None` to signal graceful degradation.

---

## Configuration

AI enrichment is toggled per environment in `config/environments/{env}.yml`:

```yaml
ai:
  enabled: true
  provider: azure_openai
  schema_mapping: true
  duplicate_detection: false   # disabled in dev to save costs
  profiling_summary: true
  nl_sql: true
```

This allows AI to be fully disabled in a CI/test environment without code changes.

---

## Consequences

**Positive:**
- Core pipeline SLA is independent of AI service SLA
- AI services can be upgraded, swapped, or disabled without touching core pipeline code
- Dramatically simplifies unit testing — no AI mocking required for core logic tests
- Cost control: AI can be selectively disabled per feature per environment

**Negative:**
- Some AI features (e.g., schema mapping suggestions) are most valuable when inline. The decorator pattern adds some architectural complexity for these cases.
- Stewardship UI users see `NULL` profiling summaries when AI is unavailable — must handle gracefully in the UI.

**Enforcement:**
- CI lint rule will flag any import of AI service modules from within core pipeline modules (`ingestion`, `quality`, `publishing`, `stewardship`)
- AI service availability is checked and logged at job startup
