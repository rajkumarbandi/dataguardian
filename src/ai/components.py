"""
AIComponents — assembled set of all AI feature instances.

Created once per Streamlit session via get_ai_components().
All feature modules reference their dependencies through this container
rather than constructing providers and caches individually.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.ai.cache import PromptCache
from src.ai.comment_summarizer import CommentSummarizer
from src.ai.config import AIConfig, load_ai_config
from src.ai.dq_assistant import DQAssistant
from src.ai.duplicate_detector import DuplicateDetector
from src.ai.explanation_engine import ExplanationEngine
from src.ai.natural_language_sql import NaturalLanguageSQL
from src.ai.profiling_assistant import ProfilingAssistant
from src.ai.prompt_manager import PromptManager
from src.ai.provider import LLMProvider, get_provider
from src.ai.root_cause_analyzer import RootCauseAnalyzer
from src.ai.schema_mapper import SchemaMapper
from src.ai.token_counter import TokenCounter


@dataclass
class AIComponents:
    config: AIConfig
    provider: LLMProvider
    prompt_manager: PromptManager
    cache: PromptCache
    token_counter: TokenCounter
    # Feature modules
    dq_assistant: DQAssistant
    schema_mapper: SchemaMapper
    root_cause_analyzer: RootCauseAnalyzer
    duplicate_detector: DuplicateDetector
    comment_summarizer: CommentSummarizer
    natural_language_sql: NaturalLanguageSQL
    profiling_assistant: ProfilingAssistant
    explanation_engine: ExplanationEngine

    @property
    def is_mock(self) -> bool:
        return self.config.is_mock()

    @property
    def provider_label(self) -> str:
        return f"{self.provider.provider_name} / {self.provider.model_name}"


def build_ai_components(
    config_path: str | Path | None = None,
    secrets_manager: Any = None,
) -> AIComponents:
    """
    Assemble all AI feature instances from configuration.

    This is the only place that wires together providers, caches, and feature modules.
    Feature modules never instantiate their own dependencies.
    """
    config = load_ai_config(config_path)
    provider = get_provider(config, secrets_manager)
    prompt_manager = PromptManager(prompts_dir=config.prompts_dir)
    cache = PromptCache(
        ttl_seconds=config.cache_ttl_seconds,
        max_size=512,
    ) if config.cache_enabled else _NoOpCache()
    token_counter = TokenCounter(
        prompt_cost_per_1k=config.pricing.prompt_per_1k,
        completion_cost_per_1k=config.pricing.completion_per_1k,
    )

    shared = (provider, prompt_manager, config, cache, token_counter)

    return AIComponents(
        config=config,
        provider=provider,
        prompt_manager=prompt_manager,
        cache=cache,
        token_counter=token_counter,
        dq_assistant=DQAssistant(*shared),
        schema_mapper=SchemaMapper(*shared),
        root_cause_analyzer=RootCauseAnalyzer(*shared),
        duplicate_detector=DuplicateDetector(*shared),
        comment_summarizer=CommentSummarizer(*shared),
        natural_language_sql=NaturalLanguageSQL(*shared),
        profiling_assistant=ProfilingAssistant(*shared),
        explanation_engine=ExplanationEngine(*shared),
    )


def get_ai_components(
    config_path: str | Path | None = None,
    secrets_manager: Any = None,
) -> AIComponents:
    """
    Return the session-scoped AIComponents instance.

    When called inside Streamlit, the result is cached via @st.cache_resource.
    Outside Streamlit (tests, CLI), returns a fresh instance.
    """
    try:
        import streamlit as st
        return _get_cached(config_path, secrets_manager)
    except Exception:
        return build_ai_components(config_path, secrets_manager)


def _get_cached(
    config_path: str | Path | None,
    secrets_manager: Any,
) -> AIComponents:
    import streamlit as st

    @st.cache_resource
    def _inner() -> AIComponents:
        return build_ai_components(config_path, secrets_manager)

    return _inner()


# ── No-op cache for when caching is disabled ──────────────────────────────────

class _NoOpCache(PromptCache):
    """Cache that never stores or retrieves — used when cache_enabled=False."""

    def __init__(self) -> None:
        super().__init__(ttl_seconds=0, max_size=0)

    def get(self, messages):
        return None

    def put(self, messages, response):
        pass
