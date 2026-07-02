"""
Unit tests for M10 DQAssistant.

Tests explanation generation, risk level extraction, caching integration,
and token counter integration using the MockProvider.
"""

import pytest

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.dq_assistant import DQAssistant, _extract_risk_level
from src.ai.prompt_manager import PromptManager
from src.ai.provider import MockProvider
from src.ai.token_counter import TokenCounter


@pytest.fixture
def assistant():
    config = AIConfig(provider="mock")
    provider = MockProvider()
    pm = PromptManager("config/prompts")
    cache = PromptCache(ttl_seconds=3600)
    counter = TokenCounter()
    return DQAssistant(provider, pm, config, cache, counter), counter, cache


def make_record(**kwargs):
    defaults = {
        "record_id": "rec-001",
        "source_name": "customers",
        "table_name": "silver.customers",
        "dq_score": 0.72,
        "violation_count": 1,
        "raw_record": {"customer_id": "C001", "email": "user@", "name": "Alice"},
    }
    return {**defaults, **kwargs}


def make_rule(**kwargs):
    defaults = {
        "rule_name": "email_format",
        "column": "email",
        "severity": "HIGH",
        "message": "Email must contain a valid domain",
        "expected_value": "valid email",
        "actual_value": "user@",
    }
    return {**defaults, **kwargs}


# ── explain_failure ───────────────────────────────────────────────────────────

class TestDQAssistantExplainFailure:
    def test_returns_dq_explanation(self, assistant):
        dqa, _, _ = assistant
        result = dqa.explain_failure(make_record(), make_rule())
        assert result.record_id == "rec-001"
        assert result.rule_name == "email_format"
        assert result.column_name == "email"
        assert result.explanation
        assert result.risk_level in ("LOW", "MEDIUM", "HIGH", "CRITICAL", "UNKNOWN")

    def test_same_input_uses_cache(self, assistant):
        dqa, counter, cache = assistant
        record = make_record()
        rule = make_rule()
        # First call — cache miss
        r1 = dqa.explain_failure(record, rule)
        tokens_after_first = counter.stats()["total_tokens"]
        assert not r1.cached
        # Second call — cache hit
        r2 = dqa.explain_failure(record, rule)
        tokens_after_second = counter.stats()["total_tokens"]
        assert r2.cached
        # Token counter should NOT increase on cache hit
        assert tokens_after_second == tokens_after_first

    def test_token_counter_incremented_on_miss(self, assistant):
        dqa, counter, _ = assistant
        assert counter.stats()["total_calls"] == 0
        dqa.explain_failure(make_record(), make_rule())
        assert counter.stats()["total_calls"] == 1

    def test_raw_record_as_json_string(self, assistant):
        import json
        dqa, _, _ = assistant
        record = make_record()
        record["raw_record"] = json.dumps({"email": "user@", "id": "C001"})
        result = dqa.explain_failure(record, make_rule())
        assert result.explanation

    def test_source_name_in_explanation_context(self, assistant):
        dqa, _, _ = assistant
        result = dqa.explain_failure(make_record(source_name="orders"), make_rule())
        # The explanation should be meaningful (non-empty, reasonable length)
        assert len(result.explanation) > 100

    def test_prompt_tokens_greater_than_zero(self, assistant):
        dqa, _, _ = assistant
        result = dqa.explain_failure(make_record(), make_rule())
        assert result.prompt_tokens > 0

    def test_completion_tokens_greater_than_zero(self, assistant):
        dqa, _, _ = assistant
        result = dqa.explain_failure(make_record(), make_rule())
        assert result.completion_tokens > 0


# ── _extract_risk_level ───────────────────────────────────────────────────────

class TestExtractRiskLevel:
    @pytest.mark.parametrize("text,expected", [
        ("Risk Level: 🔴 CRITICAL — this is serious", "CRITICAL"),
        ("Risk level: HIGH — moderate issue", "HIGH"),
        ("this is a MEDIUM risk", "MEDIUM"),
        ("LOW risk — minor issue", "LOW"),
        ("No risk mentioned here", "UNKNOWN"),
        ("CRITICAL impact detected", "CRITICAL"),
        ("critical", "CRITICAL"),       # lowercase
    ])
    def test_extracts_correct_level(self, text, expected):
        assert _extract_risk_level(text) == expected

    def test_prefers_highest_severity_when_multiple(self):
        # "CRITICAL" appears before "HIGH" so it should be returned
        result = _extract_risk_level("CRITICAL issue, also HIGH risk")
        assert result == "CRITICAL"
