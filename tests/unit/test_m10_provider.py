"""
Unit tests for M10 LLM Provider abstraction.

Tests the MockProvider thoroughly (determinism, response selection, token simulation)
and the provider factory function with configuration.
"""

import pytest

from src.ai.config import AIConfig
from src.ai.provider import (
    LLMMessage,
    MockProvider,
    get_provider,
)


# ── MockProvider ──────────────────────────────────────────────────────────────

class TestMockProvider:
    def setup_method(self):
        self.provider = MockProvider()

    def test_complete_returns_llm_response(self):
        messages = [LLMMessage(role="user", content="Hello")]
        response = self.provider.complete(messages)
        assert response.content
        assert response.provider == "mock"
        assert response.model == "mock-gpt-4"
        assert response.prompt_tokens > 0
        assert response.completion_tokens > 0
        assert response.latency_ms > 0

    def test_same_input_produces_same_response(self):
        messages = [LLMMessage(role="user", content="Find schema mapping suggestions")]
        r1 = self.provider.complete(messages)
        r2 = self.provider.complete(messages)
        assert r1.content == r2.content

    def test_different_inputs_can_produce_different_responses(self):
        # Two very different inputs should potentially select different pooled responses
        m1 = [LLMMessage(role="user", content="alpha beta gamma delta")]
        m2 = [LLMMessage(role="user", content="omega psi xi eta")]
        r1 = self.provider.complete(m1)
        r2 = self.provider.complete(m2)
        # Both must be non-empty; content may differ (hash-based selection)
        assert r1.content
        assert r2.content

    def test_feature_detection_via_system_message(self):
        messages = [
            LLMMessage(role="system", content="You are helping with schema mapping tasks"),
            LLMMessage(role="user", content="Map CustomerName to target schema"),
        ]
        response = self.provider.complete(messages)
        # The schema_mapping pool should be selected — response will contain mapping content
        assert "→" in response.content or "Confidence" in response.content or "Recommendation" in response.content

    def test_dq_explanation_feature_detection(self):
        messages = [
            LLMMessage(role="system", content="You are helping with dq explanation tasks"),
            LLMMessage(role="user", content="Explain the email validation failure"),
        ]
        response = self.provider.complete(messages)
        assert response.content

    def test_provider_name(self):
        assert self.provider.provider_name == "mock"

    def test_model_name(self):
        assert self.provider.model_name == "mock-gpt-4"

    def test_custom_model_name(self):
        p = MockProvider(model="my-custom-model")
        assert p.model_name == "my-custom-model"

    def test_token_counts_are_positive(self):
        messages = [LLMMessage(role="user", content="short question")]
        response = self.provider.complete(messages)
        assert response.prompt_tokens >= 1
        assert response.completion_tokens >= 1

    def test_latency_simulated(self):
        messages = [LLMMessage(role="user", content="test")]
        response = self.provider.complete(messages)
        assert response.latency_ms >= 50  # Mock simulates ~80ms


# ── Factory ───────────────────────────────────────────────────────────────────

class TestGetProvider:
    def test_mock_provider_returned_for_mock_config(self):
        config = AIConfig(provider="mock", model="mock-gpt-4")
        provider = get_provider(config)
        assert isinstance(provider, MockProvider)

    def test_unknown_provider_raises(self):
        from src.common.exceptions import ConfigurationError
        config = AIConfig(provider="unknown_llm", model="some-model")
        with pytest.raises(ConfigurationError, match="Unknown AI provider"):
            get_provider(config)

    def test_mock_provider_is_default(self):
        config = AIConfig()
        provider = get_provider(config)
        assert isinstance(provider, MockProvider)
