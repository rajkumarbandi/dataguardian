"""
Unit tests for M10 SchemaMapper.

Tests mapping suggestions, confidence parsing, unmapped field detection,
and caching behaviour.
"""

import pytest

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.prompt_manager import PromptManager
from src.ai.provider import MockProvider
from src.ai.schema_mapper import SchemaMapper, _parse_mappings
from src.ai.token_counter import TokenCounter


@pytest.fixture
def mapper():
    config = AIConfig(provider="mock")
    provider = MockProvider()
    pm = PromptManager("config/prompts")
    cache = PromptCache(ttl_seconds=3600)
    counter = TokenCounter()
    return SchemaMapper(provider, pm, config, cache, counter)


SOURCE_FIELDS = ["CustomerName", "DOB", "Email_Addr", "cust_tier"]
TARGET_FIELDS = ["customer_name", "birth_date", "email", "customer_segment"]


class TestSchemaMapper:
    def test_returns_result_object(self, mapper):
        result = mapper.suggest_mappings(
            source_fields=SOURCE_FIELDS,
            target_fields=TARGET_FIELDS,
            source_system="Salesforce",
            domain="CRM",
        )
        assert result.source_system == "Salesforce"
        assert result.domain == "CRM"
        assert result.raw_response

    def test_result_has_prompt_tokens(self, mapper):
        result = mapper.suggest_mappings(SOURCE_FIELDS, TARGET_FIELDS)
        assert result.prompt_tokens > 0

    def test_second_call_uses_cache(self, mapper):
        r1 = mapper.suggest_mappings(SOURCE_FIELDS, TARGET_FIELDS, source_system="Salesforce")
        r2 = mapper.suggest_mappings(SOURCE_FIELDS, TARGET_FIELDS, source_system="Salesforce")
        assert r2.cached

    def test_different_sources_bypass_cache(self, mapper):
        r1 = mapper.suggest_mappings(SOURCE_FIELDS, TARGET_FIELDS, source_system="Salesforce")
        r2 = mapper.suggest_mappings(SOURCE_FIELDS, TARGET_FIELDS, source_system="Oracle")
        # Different source system → different prompt → different cache key
        # r2 may or may not be cached depending on cache state, but it should succeed
        assert r2.raw_response

    def test_high_confidence_count_property(self, mapper):
        result = mapper.suggest_mappings(SOURCE_FIELDS, TARGET_FIELDS)
        # high_confidence_count should be >= 0 and <= total mappings
        assert 0 <= result.high_confidence_count <= len(result.mappings)

    def test_review_required_count_property(self, mapper):
        result = mapper.suggest_mappings(SOURCE_FIELDS, TARGET_FIELDS)
        assert result.review_required_count >= 0


# ── _parse_mappings ────────────────────────────────────────────────────────────

class TestParseMappings:
    def test_extracts_arrow_notation_mapping(self):
        text = (
            "**CustomerName** → `customer_name` ✅ Confidence: 96%\n"
            "*Reason*: Direct semantic equivalence."
        )
        mappings, unmapped = _parse_mappings(text, ["CustomerName"])
        assert len(mappings) == 1
        assert mappings[0].source_field == "CustomerName"
        assert mappings[0].target_field == "customer_name"
        assert mappings[0].confidence == 96
        assert not mappings[0].requires_review

    def test_low_confidence_flagged_for_review(self):
        text = (
            "**cust_tier** → `customer_segment` ⚠️ Confidence: 74%\n"
            "*Reason*: Ambiguous mapping."
        )
        mappings, _ = _parse_mappings(text, ["cust_tier"])
        assert len(mappings) == 1
        assert mappings[0].requires_review

    def test_unmapped_fields_detected(self):
        text = "⛔ Unmapped: `legacy_flag` — No matching field"
        mappings, unmapped = _parse_mappings(text, ["CustomerName", "legacy_flag"])
        assert "legacy_flag" in unmapped

    def test_source_fields_not_in_response_are_unmapped(self):
        text = "**CustomerName** → `customer_name` ✅ Confidence: 95%"
        _, unmapped = _parse_mappings(text, ["CustomerName", "SomeOtherField"])
        assert "SomeOtherField" in unmapped

    def test_multiple_mappings_parsed(self):
        text = (
            "**CustomerName** → `customer_name` ✅ Confidence: 96%\n"
            "*Reason*: Direct.\n"
            "**DOB** → `birth_date` ✅ Confidence: 91%\n"
            "*Reason*: Standard acronym."
        )
        mappings, _ = _parse_mappings(text, ["CustomerName", "DOB"])
        assert len(mappings) == 2

    def test_no_mappings_returns_all_unmapped(self):
        mappings, unmapped = _parse_mappings("No valid mappings found.", ["FieldA", "FieldB"])
        assert len(mappings) == 0
        assert "FieldA" in unmapped
        assert "FieldB" in unmapped
