"""
LLM Provider abstraction for DataGuardian AI features.

Architecture:
    LLMProvider (ABC)
        ├── OpenAIProvider      — openai library, direct endpoint
        ├── AzureOpenAIProvider — openai library, Azure endpoint
        ├── AnthropicProvider   — anthropic library
        └── MockProvider        — deterministic, no API key required

The factory function get_provider() resolves the correct implementation
from AIConfig without any hardcoded provider logic in feature modules.
"""

from __future__ import annotations

import hashlib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from src.ai.config import AIConfig


@dataclass
class LLMMessage:
    role: str   # "system" | "user" | "assistant"
    content: str


@dataclass
class LLMResponse:
    content: str
    prompt_tokens: int
    completion_tokens: int
    model: str
    provider: str
    latency_ms: float


class LLMProvider(ABC):
    """Abstract LLM provider — all feature modules depend only on this interface."""

    @abstractmethod
    def complete(
        self,
        messages: list[LLMMessage],
        max_tokens: int = 1024,
        temperature: float = 0.1,
    ) -> LLMResponse: ...

    @property
    @abstractmethod
    def provider_name(self) -> str: ...

    @property
    @abstractmethod
    def model_name(self) -> str: ...


# ── OpenAI Provider ───────────────────────────────────────────────────────────

class OpenAIProvider(LLMProvider):
    """Standard OpenAI API (gpt-4o, gpt-4o-mini, etc.)."""

    def __init__(self, api_key: str, model: str = "gpt-4o") -> None:
        from openai import OpenAI  # type: ignore[import-untyped]
        self._client = OpenAI(api_key=api_key)
        self._model = model

    def complete(
        self,
        messages: list[LLMMessage],
        max_tokens: int = 1024,
        temperature: float = 0.1,
    ) -> LLMResponse:
        start = time.monotonic()
        msgs = [{"role": m.role, "content": m.content} for m in messages]
        response = self._client.chat.completions.create(
            model=self._model,
            messages=msgs,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        latency = (time.monotonic() - start) * 1000
        choice = response.choices[0]
        return LLMResponse(
            content=choice.message.content or "",
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            model=self._model,
            provider=self.provider_name,
            latency_ms=latency,
        )

    @property
    def provider_name(self) -> str:
        return "openai"

    @property
    def model_name(self) -> str:
        return self._model


# ── Azure OpenAI Provider ─────────────────────────────────────────────────────

class AzureOpenAIProvider(LLMProvider):
    """Azure-hosted OpenAI deployment."""

    def __init__(
        self,
        api_key: str,
        endpoint: str,
        deployment_name: str,
        api_version: str = "2024-02-01",
    ) -> None:
        from openai import AzureOpenAI  # type: ignore[import-untyped]
        self._client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=api_version,
        )
        self._deployment = deployment_name

    def complete(
        self,
        messages: list[LLMMessage],
        max_tokens: int = 1024,
        temperature: float = 0.1,
    ) -> LLMResponse:
        start = time.monotonic()
        msgs = [{"role": m.role, "content": m.content} for m in messages]
        response = self._client.chat.completions.create(
            model=self._deployment,
            messages=msgs,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        latency = (time.monotonic() - start) * 1000
        choice = response.choices[0]
        return LLMResponse(
            content=choice.message.content or "",
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            model=self._deployment,
            provider=self.provider_name,
            latency_ms=latency,
        )

    @property
    def provider_name(self) -> str:
        return "azure_openai"

    @property
    def model_name(self) -> str:
        return self._deployment


# ── Anthropic Provider ────────────────────────────────────────────────────────

class AnthropicProvider(LLMProvider):
    """Anthropic Claude API."""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-6") -> None:
        import anthropic  # type: ignore[import-untyped]
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model

    def complete(
        self,
        messages: list[LLMMessage],
        max_tokens: int = 1024,
        temperature: float = 0.1,
    ) -> LLMResponse:
        start = time.monotonic()
        system_parts = [m.content for m in messages if m.role == "system"]
        user_msgs = [{"role": m.role, "content": m.content} for m in messages if m.role != "system"]
        system = "\n\n".join(system_parts)

        kwargs: dict[str, Any] = {
            "model": self._model,
            "max_tokens": max_tokens,
            "messages": user_msgs,
        }
        if system:
            kwargs["system"] = system

        response = self._client.messages.create(**kwargs)
        latency = (time.monotonic() - start) * 1000
        content = response.content[0].text if response.content else ""
        return LLMResponse(
            content=content,
            prompt_tokens=response.usage.input_tokens,
            completion_tokens=response.usage.output_tokens,
            model=self._model,
            provider=self.provider_name,
            latency_ms=latency,
        )

    @property
    def provider_name(self) -> str:
        return "anthropic"

    @property
    def model_name(self) -> str:
        return self._model


# ── Mock Provider ─────────────────────────────────────────────────────────────

class MockProvider(LLMProvider):
    """
    Deterministic mock provider — no API key required.

    Response selection is hash-based: same input → same response.
    Different inputs → different responses (rotates through the pool).
    Simulates realistic latency (100ms).
    """

    _RESPONSES: dict[str, list[str]] = {
        "schema_mapping": [
            """## Schema Mapping Recommendations

**CustomerName** → `customer_name` ✅ Confidence: 96%
*Reason*: Direct semantic equivalence — 'Customer' maps to the entity, 'Name' maps to the attribute. Standard CamelCase to snake_case transformation.

**Cust_Name** → `customer_name` ✅ Confidence: 93%
*Reason*: 'Cust' is a well-established abbreviation for 'Customer' in ERP systems (SAP, Oracle). Combined with 'Name', this is a high-confidence match.

**Email_Addr** → `email` ✅ Confidence: 97%
*Reason*: 'Addr' is a standard abbreviation for 'address'. The 'Email' context makes this unambiguous. Single-field canonical form preferred.

**DOB** → `birth_date` ✅ Confidence: 91%
*Reason*: 'DOB' is the standard acronym for 'Date of Birth', directly mapping to the target `birth_date` field.

**cust_tier** → `customer_segment` ⚠️ Confidence: 74%
*Reason*: 'Tier' and 'Segment' are used interchangeably in CRM systems. Moderate confidence — **recommend human review** to confirm business terminology alignment.

**RevAmt** → `annual_revenue` ⚠️ Confidence: 82%
*Reason*: 'Rev' maps to 'Revenue', 'Amt' maps to 'Amount'. The annual period assumption must be confirmed against source documentation.

**⛔ Unmapped**: `legacy_flag` — No matching field in target schema. May represent a deprecated attribute from a prior system migration. Recommend investigation with the source system owner before creating a new target field.""",

            """## Schema Mapping Recommendations

**CUSTOMER_FULL_NM** → `customer_name` ✅ Confidence: 95%
*Reason*: 'FULL_NM' is Oracle-style naming for 'Full Name'. The 'CUSTOMER' prefix confirms entity context. High confidence.

**ACCT_STATUS_CD** → `account_status` ✅ Confidence: 89%
*Reason*: '_CD' suffix indicates a code/categorical field. 'ACCT_STATUS' maps cleanly to 'account_status' in the target schema.

**CRTE_DT** → `created_at` ✅ Confidence: 92%
*Reason*: '_DT' is a standard Oracle date suffix. 'CRTE' maps to 'Create'. This is a common pattern in Oracle/SAP source systems.

**PREF_LANG_CD** → `preferred_language` ⚠️ Confidence: 78%
*Reason*: 'PREF' is 'Preferred', 'LANG' is 'Language', '_CD' is a code. The target field stores the ISO language code, which aligns with a '_CD' source pattern.

**⛔ Unmapped**: `SRC_SYS_ID` — Source system identifier. Recommend mapping to `_source_system` (metadata field) or dropping if not needed downstream.""",
        ],

        "dq_explanation": [
            """## Business Impact Assessment

The email address in this customer record is **incomplete** — it is missing the domain name after the '@' symbol. In business terms:

### What This Means for Your Operations

1. **Customer Communications Will Fail**: Automated order confirmations, shipping updates, and account alerts cannot be delivered. This customer will not receive any email-based service communications.

2. **Marketing Revenue at Risk**: This customer will be suppressed from all email campaigns. At an average email campaign conversion rate of 2% and average order value, each campaign cycle represents a measurable revenue gap for this customer.

3. **Account Recovery Blocked**: If the customer needs a password reset or account verification, the standard self-service flow will not work. This increases support center call volume.

4. **CRM Data Integrity**: If promoted to Gold without correction, this invalid email will propagate to your CRM, marketing automation, and analytics systems. Retroactive correction at that point is significantly more expensive.

### Root Cause Assessment

This pattern (email ending with '@' or containing '@@') typically results from **web form validation bypass** — the user submitted the form before completing the email field, and the server-side API endpoint lacks equivalent validation.

### Recommended Action

1. Mark this record as **CORRECTION_REQUESTED**
2. Contact the customer through an alternative channel (phone number if available)
3. Submit a bug report to the source system team to add server-side email validation

**Risk Level**: 🔴 HIGH — Customer data is unusable for communication without a valid email.""",

            """## Business Impact Assessment

A missing `customer_id` is a **critical data integrity issue** that affects core operational systems.

### What This Means for Your Operations

1. **Order Attribution Impossible**: Without a valid customer_id, this transaction cannot be linked to any customer account. Customer lifetime value, order history, and loyalty calculations will all be incomplete.

2. **Silent Data Loss Downstream**: ERP, CRM, and analytics systems use customer_id as the primary join key. A NULL value will cause silent failures in downstream ETL processes — records may simply disappear from reports without any error.

3. **Regulatory Exposure**: For regulated industries, an unattributed transaction may fail audit requirements. This record would appear as an "orphaned transaction" in compliance reports.

4. **Deduplication Impossible**: Without a primary key, there is no way to detect if this record has already been processed. The same transaction may be counted multiple times.

### Root Cause Assessment

NULL customer_id values typically indicate one of three root causes: (1) **Guest checkout flow** where no account was created, (2) **API timeout** during customer lookup where the field was not populated on failure, or (3) **Data migration gap** from a legacy system where the mapping table was incomplete.

### Recommended Action

1. Check the source system's guest checkout handling — does it generate an `anonymous_id`?
2. If this is a known guest pattern, request a business rule decision: create placeholder IDs or drop guest records from Silver
3. Escalate to data engineering for a root cause investigation of the API timeout scenario

**Risk Level**: 🔴 CRITICAL — Primary key violation that blocks all downstream processing.""",

            """## Business Impact Assessment

The `birth_date` field contains a future date (`2030-01-15`), which is not a valid date of birth for any living customer.

### What This Means for Your Operations

1. **Age-Gated Products Miscategorized**: If your systems use birth_date to determine eligibility for age-gated products or services, this customer will be incorrectly flagged as underage (they appear to be -4 years old).

2. **Regulatory Age Verification**: In regulated sectors (financial services, alcohol, gambling), incorrect age data creates compliance exposure. An adult customer could be blocked from services they are entitled to.

3. **Analytics Distortion**: Any cohort analysis by age group or generation will be distorted. A customer born in '2030' will skew age distribution reports.

### Root Cause Assessment

This is almost certainly a **data entry error** — likely a transposition of '2003' → '2030' on a numeric keypad. This pattern (birth year being 27 years in the future) is a well-known data entry artifact.

### Recommended Action

1. Correct the birth_date to `2003-01-15` if the customer is approximately 23 years old
2. Verify against a supporting document (ID scan, account application) if available
3. Request correction from the customer if no supporting document exists

**Risk Level**: 🟡 MEDIUM — Data entry error with straightforward correction path.""",
        ],

        "root_cause": [
            """## Root Cause Analysis Report

### Batch Overview
**Source**: customers | **Total Failures**: {violation_count} records

---

### Failure Pattern Analysis

| Rank | Issue | Estimated Share |
|------|-------|----------------|
| 1 | Invalid email format | ~38% |
| 2 | NULL primary key | ~26% |
| 3 | Future/invalid dates | ~19% |
| 4 | Invalid reference values | ~17% |

---

### Primary Root Cause: Server-Side Validation Gap

The dominant pattern in this batch is email addresses that end with '@' or '@@'. This is the signature of a **web form submitted before the email domain was entered**, combined with **missing server-side validation** on the API endpoint. The front-end validation catches this in the browser, but direct API submissions bypass it.

**Evidence**: All malformed emails share the same session timestamp range (14:30–15:45 UTC), suggesting a specific client version or browser configuration caused form premature submission.

### Secondary Root Cause: Guest Checkout Null ID Pattern

The NULL customer_id records are all traceable to the `/api/v2/orders/guest` endpoint. Guest orders do not create customer accounts, but the pipeline schema requires a customer_id. This is a **known architectural gap** that requires a business decision: generate anonymous IDs for guests, or create a separate guest order entity.

### Contributing Factor: Manual Data Entry Errors

The future birth_date entries (2030 instead of 2003) are consistent with numeric keypad transposition errors during phone-assisted data entry. This is a human process issue, not a system bug.

---

### Business Impact

- **Revenue at Risk**: Records in CORRECTION_REQUESTED status are excluded from Gold until resolved
- **Data Freshness**: Each day of delay increases staleness in downstream Gold layer reports
- **Customer Experience**: ~38% of affected customers cannot receive email communications

---

### Recommended Remediation

1. **Immediate (24h)**: Flag the guest checkout API endpoint for urgent review by data engineering
2. **Short-term (1 week)**: Add server-side email format validation to all API ingest endpoints
3. **Long-term**: Implement real-time DQ scoring at ingestion to surface failures before they reach the stewardship queue""",
        ],

        "duplicate": [
            """## Duplicate Detection Analysis

### Record Pair Comparison

**Candidate A**: "IBM Corporation"
**Candidate B**: "International Business Machines"

---

**Similarity Score**: 94% — 🔴 Likely Duplicate

**Evidence Supporting Duplicate**:
- "IBM" is the officially registered NYSE ticker symbol and trade name of "International Business Machines Corporation"
- Both records share the same EIN pattern and registered US headquarters state (New York)
- "IBM Corporation" is the standard vendor management shortform used in procurement systems globally

**Risk of False Positive**: Low — this is one of the most well-known corporate name abbreviations worldwide

**Recommendation**: **Merge records**. Keep the record with more complete attributes as primary. Store "International Business Machines" as an alternate name (`aka_names` array field).

---

### Additional Patterns Detected

**"Microsoft Corp" / "Microsoft Corporation" / "MSFT"** → 99% duplicate — Merge immediately (suffix and ticker variation only)

**"J.P. Morgan" / "JPMorgan Chase & Co."** → 87% probable duplicate — Confirm with vendor management: these may represent separate legal entities for different business relationships (investment banking vs. retail banking)

**"Accenture plc" / "Accenture"** → 96% duplicate — "plc" is a UK legal suffix with no semantic distinction in a vendor context""",

            """## Duplicate Detection Analysis

### Semantic Similarity Analysis

**Input records analyzed**: 5 potential duplicate clusters identified

---

**Cluster 1: "3M Company" / "Minnesota Mining and Manufacturing" / "3M"**
Confidence: 97% | **Recommendation**: Merge — same entity, three name variants

**Cluster 2: "General Electric" / "GE Capital" / "GE Aviation"**
Confidence: 62% | **Recommendation**: Do NOT merge — these are legally distinct subsidiaries. GE Capital and GE Aviation have separate EINs and independent financial reporting. Link as related entities in a parent-child hierarchy.

**Cluster 3: "Alphabet Inc." / "Google LLC" / "Google"**
Confidence: 78% | **Recommendation**: Review — Alphabet is the holding company; Google is a subsidiary. Depending on the business context (billing entity vs. service provider), these may need to remain separate.

**Cluster 4: "SAP AG" / "SAP SE" / "SAP America"**
Confidence: 71% | **Recommendation**: Review with legal — SAP transitioned from AG to SE (European Company) in 2014. Historical contracts may reference the old entity. "SAP America" is the US subsidiary.

**Cluster 5: "BHP" / "BHP Billiton" / "BHP Group"**
Confidence: 89% | **Recommendation**: Merge — BHP dropped the "Billiton" name in 2017 and rebranded as "BHP Group". All three names refer to the same legal entity.""",
        ],

        "comment_summary": [
            """## Discussion Summary

**Thread**: {thread_count} messages | **Stewards**: Sarah Mitchell, James Chen, Oliver Brown

---

### Key Issues Identified

1. **Technical Root Cause Confirmed**: The email format failure is a systemic issue from the source API (not an isolated user error). James Chen cross-referenced the CRM and found the same pattern in 18 other records in this batch.

2. **Engineering Ticket Raised**: Oliver Brown confirmed that ticket **DG-4821** has been submitted to the data engineering team. The API-level validation fix is scheduled for the next sprint (ETA: 2026-07-15).

3. **Business Decision Pending**: The team discussed whether to approve with a documented exception or reject pending the fix. **No consensus was reached** — the decision requires input from the data owner.

---

### Current Blockers

- **Waiting on**: Maria Santos (Commercial Operations) for a business priority decision
- **Deadline**: Data freshness SLA requires action within 48 hours
- **Risk if delayed**: Gold layer customers report will be 27 records short for the monthly dashboard

---

### Recommended Next Step

Escalate to data owner (**Maria Santos**) for a business decision. The technical path is clear — the business needs to decide: approve with exception and document in audit trail, or hold until the API fix is deployed.""",

            """## Discussion Summary

**Thread**: {thread_count} messages | **Participants**: Emma Davis, Sarah Mitchell

---

### Core Issue

The team confirmed this is a **known guest checkout pattern** — customer_id is NULL because the customer did not create an account. The source system does not generate an anonymous ID for guests.

### Agreement Reached

Both stewards agree that this record should be **rejected** because:
1. It cannot be linked to any customer account for attribution
2. Promoting NULL primary keys to Gold would corrupt downstream CRM joins
3. A proper fix (anonymous ID generation) must be implemented at the source

### Action Items from Thread

- Emma Davis will document this as a **known exception pattern** in the data quality runbook
- Sarah Mitchell will reject this and the 11 similar guest records in this batch
- The engineering team has been notified to implement anonymous ID generation in the next sprint

### Open Question

Should guest orders be tracked in a **separate Gold table** (e.g., `gold.anonymous_orders`) rather than merged with attributed orders? The thread flagged this as a design question for the data architect.""",
        ],

        "nl_sql": [
            """```sql
SELECT
    r.record_id,
    r.source_name,
    r.batch_id,
    r.status,
    r.dq_score,
    r.violation_count,
    r.assigned_to,
    r.created_at
FROM stewardship.stewardship_records r
WHERE r.status = 'PENDING'
ORDER BY r.dq_score ASC, r.created_at ASC
LIMIT 100;
```

This query retrieves all pending stewardship records ordered by lowest DQ score first (worst quality at the top), then by creation date to prioritize older records.""",

            """```sql
SELECT
    a.performed_by,
    a.action_type,
    COUNT(*) AS action_count,
    MIN(a.action_timestamp) AS first_action,
    MAX(a.action_timestamp) AS last_action
FROM stewardship.stewardship_actions a
WHERE a.action_timestamp >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY a.performed_by, a.action_type
ORDER BY a.performed_by, a.action_count DESC;
```

This query shows the activity breakdown by steward and action type over the past 7 days, useful for team performance reporting.""",

            """```sql
SELECT
    r.source_name,
    r.batch_id,
    COUNT(*) AS total_records,
    SUM(CASE WHEN r.status = 'APPROVED' THEN 1 ELSE 0 END) AS approved,
    SUM(CASE WHEN r.status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected,
    SUM(CASE WHEN r.status = 'PENDING' THEN 1 ELSE 0 END) AS pending,
    ROUND(AVG(r.dq_score), 4) AS avg_dq_score
FROM stewardship.stewardship_records r
GROUP BY r.source_name, r.batch_id
ORDER BY r.batch_id DESC, r.source_name;
```

This query provides a batch-level summary grouped by source and batch, showing the distribution of statuses and average DQ score per batch.""",
        ],

        "data_profiling": [
            """## Data Quality Executive Summary

**Source**: {source_name} | **Batch**: {batch_id}

---

### Overall Health: ⚠️ Needs Attention

**DQ Score**: 72.3% (Target: 85%+)

The {source_name} dataset shows meaningful data quality issues concentrated in 3 columns. The majority of records (72.7%) pass all quality checks and are ready for promotion to Gold. The 27.3% failure rate is above the acceptable threshold of 15% and requires steward intervention before this batch can be closed.

---

### Column Health Assessment

| Column | Issue | Risk |
|--------|-------|------|
| email | 21.7% invalid format | 🔴 High |
| customer_id | 1.4% NULL values | 🔴 High |
| customer_segment | 8.8% invalid codes | 🟡 Medium |
| birth_date | 1.1% future dates | 🟡 Medium |
| annual_revenue | 2.1% negative values | 🟢 Low |

---

### Business Risks

1. **Customer Communication Gap**: 21.7% of customers have invalid emails — they cannot receive any automated communications (confirmations, alerts, marketing).

2. **Attribution Loss**: 1.4% NULL customer_id means those transactions cannot be attributed to any account — affecting revenue reporting, CLV calculations, and audit trails.

3. **Segmentation Errors**: 8.8% of records have invalid segment codes, which will distort segment-based reports and campaign targeting.

---

### Key Observations

- Email failures have **increased 12%** vs the previous batch — suggests a regression in source system validation
- {source_name} has the highest failure rate across all 4 sources (27.3% vs. 18.3% average)
- 94% of all failures are concentrated in just 3 columns — **targeted fixes will have maximum impact**

---

### Recommended Actions

1. Engage the {source_name} source system owner to investigate email validation regression
2. Review guest checkout flow for customer_id NULL handling
3. Update the allowed values list for customer_segment — the current list is outdated""",
        ],

        "record_explanation": [
            """## Plain-English Record Explanation

**Source**: {source_name} | **DQ Score**: {dq_score}

---

### What Happened

This {source_name} record was **automatically flagged** by the DataGuardian quality pipeline before it could be added to the curated database. The system found **{violation_count} data quality issue(s)** that need your review.

---

### Issues Found

**1. {rule_name} violation on `{column_name}`**

In plain terms: {explanation}

*Business impact*: Without resolving this issue, this record cannot be used in reports, customer communications, or downstream business processes.

---

### Overall Quality Score: {dq_score_pct}%

A score below 80% indicates the record has significant quality problems. This record scored {dq_score_pct}% — it has data that does not meet the minimum quality standards for the Gold layer.

---

### What Should You Do?

- **If the data is clearly wrong** (typo, missing field): Select *Request Correction* and explain what needs to be fixed at the source
- **If you can confirm the data is correct** despite the rule warning: Select *Approve* with a justification explaining the exception
- **If this record should not exist** (test data, duplicate): Select *Reject* with a reason

---

### Risk if Not Actioned

Every day this record sits in the PENDING queue, the Gold layer is missing this data point. For time-sensitive metrics (daily sales, customer counts), this creates a data gap that affects business decisions.""",
        ],

        "default": [
            "I've analyzed the data and identified the key patterns. The records show a mix of structural and content-level issues that warrant steward review. The most impactful action would be to address the highest-frequency violations first.",
        ],
    }

    def __init__(self, model: str = "mock-gpt-4") -> None:
        self._model = model

    def complete(
        self,
        messages: list[LLMMessage],
        max_tokens: int = 1024,
        temperature: float = 0.1,
    ) -> LLMResponse:
        time.sleep(0.08)  # Simulate realistic latency

        # Feature detection from system message.
        # Keywords must match phrases that actually appear in config/prompts/*.yml system messages.
        _FEATURE_KEYWORDS: dict[str, list[str]] = {
            "dq_explanation": ["data quality violation", "data quality analyst", "business impact assessment"],
            "schema_mapping": ["schema mapping", "source schema", "mapping recommendation"],
            "root_cause": ["root cause", "systemic pattern"],
            "duplicate": ["entity resolution", "duplicate detection", "duplicate"],
            "comment_summary": ["summarises stewardship", "discussion thread", "executive brief"],
            "nl_sql": ["sql expert", "select statement", "sql query", "natural language"],
            "data_profiling": ["data profiling", "profiling report"],
            "record_explanation": ["explain why a specific data record", "data steward"],
        }
        system = next(
            (m.content.lower() for m in messages if m.role == "system"), ""
        )
        # Also check user message for additional context
        user_combined = " ".join(m.content.lower() for m in messages if m.role == "user")
        all_text = system + " " + user_combined
        feature = "default"
        for resp_key, keywords in _FEATURE_KEYWORDS.items():
            if any(kw in all_text for kw in keywords):
                # Map keyword group back to response pool key
                pool_key = resp_key if resp_key in self._RESPONSES else (
                    "duplicate" if "duplicate" in resp_key else "default"
                )
                feature = pool_key
                break

        # Deterministic response selection — same input → same response
        user_content = " ".join(m.content for m in messages if m.role == "user")
        responses = self._RESPONSES.get(feature, self._RESPONSES["default"])
        idx = int(hashlib.md5(user_content.encode()).hexdigest(), 16) % len(responses)
        content = responses[idx]

        # Simulate token counts
        prompt_tokens = max(10, len(user_content.split()) * 2)
        completion_tokens = max(10, len(content.split()) * 2)

        return LLMResponse(
            content=content,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            model=self._model,
            provider=self.provider_name,
            latency_ms=80.0,
        )

    @property
    def provider_name(self) -> str:
        return "mock"

    @property
    def model_name(self) -> str:
        return self._model


# ── Factory ───────────────────────────────────────────────────────────────────

def get_provider(ai_config: AIConfig, secrets_manager: Any = None) -> LLMProvider:
    """Instantiate the LLM provider specified in ai_config."""
    name = ai_config.provider

    if name == "mock":
        return MockProvider(model=ai_config.model)

    # Resolve credentials
    api_key = _resolve_api_key(name, ai_config, secrets_manager)

    if name == "openai":
        return OpenAIProvider(api_key=api_key, model=ai_config.model)

    if name == "azure_openai":
        endpoint = _resolve_azure_endpoint(ai_config, secrets_manager)
        return AzureOpenAIProvider(
            api_key=api_key,
            endpoint=endpoint,
            deployment_name=ai_config.azure_openai_deployment,
            api_version=ai_config.azure_openai_api_version,
        )

    if name == "anthropic":
        return AnthropicProvider(api_key=api_key, model=ai_config.model)

    from src.common.exceptions import ConfigurationError
    raise ConfigurationError(f"Unknown AI provider: '{name}'. Valid options: mock, openai, azure_openai, anthropic")


def _resolve_api_key(provider: str, config: AIConfig, secrets_manager: Any) -> str:
    import os
    key_map = {
        "openai": ("openai_api_key_secret", "OPENAI_API_KEY"),
        "azure_openai": ("azure_openai_api_key_secret", "AZURE_OPENAI_API_KEY"),
        "anthropic": ("anthropic_api_key_secret", "ANTHROPIC_API_KEY"),
    }
    secret_key, env_var = key_map.get(provider, ("", ""))
    if secrets_manager and secret_key:
        secret_name = getattr(config, secret_key, secret_key)
        return secrets_manager.get_optional(secret_name, "")
    return os.environ.get(env_var, "")


def _resolve_azure_endpoint(config: AIConfig, secrets_manager: Any) -> str:
    import os
    if secrets_manager:
        return secrets_manager.get_optional(config.azure_openai_endpoint_secret, "")
    return os.environ.get("AZURE_OPENAI_ENDPOINT", "")
