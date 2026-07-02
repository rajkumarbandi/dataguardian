"""
DataGuardian AI Intelligence Layer.

Provides AI-powered enrichment features as optional decorators on top of
the core data stewardship pipeline. All features degrade gracefully to
mock/no-op behaviour when no API key is configured.

Quick start:
    from src.ai import get_ai_components
    ai = get_ai_components()
    explanation = ai.dq_assistant.explain_failure(record, failed_rule)
"""

from src.ai.components import AIComponents, build_ai_components, get_ai_components
from src.ai.config import AIConfig, load_ai_config
from src.ai.provider import LLMProvider, LLMResponse, MockProvider, get_provider
from src.ai.token_counter import TokenCounter, TokenUsage

__all__ = [
    "AIComponents",
    "AIConfig",
    "LLMProvider",
    "LLMResponse",
    "MockProvider",
    "TokenCounter",
    "TokenUsage",
    "build_ai_components",
    "get_ai_components",
    "load_ai_config",
    "get_provider",
]
