"""
AIConfig — strongly-typed configuration model for the AI Intelligence Layer.

Loaded once at startup from config/ai.yml and environment variables.
All feature modules receive an AIConfig instance — never raw dicts.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class RetryConfig:
    max_attempts: int = 3
    backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0


@dataclass
class ProviderPricing:
    """USD cost per 1 000 tokens."""
    prompt_per_1k: float = 0.0
    completion_per_1k: float = 0.0


@dataclass
class AIConfig:
    # Provider selection
    provider: str = "mock"                          # mock | openai | azure_openai | anthropic
    model: str = "mock-gpt-4"

    # Azure OpenAI extras
    azure_openai_deployment: str = "gpt-4o"
    azure_openai_api_version: str = "2024-02-01"
    azure_openai_endpoint_secret: str = "azure_openai_endpoint"
    azure_openai_api_key_secret: str = "azure_openai_api_key"

    # OpenAI extras
    openai_api_key_secret: str = "openai_api_key"

    # Anthropic extras
    anthropic_api_key_secret: str = "anthropic_api_key"

    # Inference defaults
    temperature: float = 0.1
    max_tokens: int = 2048

    # Cache
    cache_enabled: bool = True
    cache_ttl_seconds: int = 3600

    # Retry
    retry: RetryConfig = field(default_factory=RetryConfig)

    # Pricing (USD/1k tokens) — used for cost tracking in demo mode
    pricing: ProviderPricing = field(default_factory=ProviderPricing)

    # Feature flags
    features: dict[str, bool] = field(default_factory=lambda: {
        "dq_assistant": True,
        "schema_mapper": True,
        "root_cause_analyzer": True,
        "duplicate_detector": True,
        "comment_summarizer": True,
        "natural_language_sql": True,
        "profiling_assistant": True,
        "explanation_engine": True,
    })

    # Prompt directory
    prompts_dir: str = "config/prompts"

    def is_feature_enabled(self, feature: str) -> bool:
        return self.features.get(feature, True)

    def is_mock(self) -> bool:
        return self.provider == "mock"


# ── Loader ────────────────────────────────────────────────────────────────────

def load_ai_config(config_path: str | Path | None = None) -> AIConfig:
    """
    Load AIConfig from config/ai.yml with environment variable overrides.

    Env vars take precedence over file values.
    Falls back to mock provider defaults when no config file is found.
    """
    resolved = _resolve_config_path(config_path)
    raw: dict[str, Any] = {}
    if resolved and resolved.exists():
        with resolved.open() as fh:
            raw = yaml.safe_load(fh) or {}

    provider_raw = raw.get("provider", {}) if isinstance(raw.get("provider"), dict) else {}
    features_raw = raw.get("features", {})
    retry_raw = raw.get("retry", {})
    pricing_raw = raw.get("pricing", {})

    # Provider name — env var wins
    provider = os.environ.get(
        "DG_AI_PROVIDER",
        provider_raw.get("name", raw.get("provider", "mock") if not isinstance(raw.get("provider"), dict) else "mock"),
    )
    model = os.environ.get("DG_AI_MODEL", provider_raw.get("model", "mock-gpt-4"))

    retry = RetryConfig(
        max_attempts=int(retry_raw.get("max_attempts", 3)),
        backoff_seconds=float(retry_raw.get("backoff_seconds", 1.0)),
        max_backoff_seconds=float(retry_raw.get("max_backoff_seconds", 30.0)),
    )
    pricing = ProviderPricing(
        prompt_per_1k=float(pricing_raw.get("prompt_per_1k", 0.0)),
        completion_per_1k=float(pricing_raw.get("completion_per_1k", 0.0)),
    )

    default_features = {
        "dq_assistant": True,
        "schema_mapper": True,
        "root_cause_analyzer": True,
        "duplicate_detector": True,
        "comment_summarizer": True,
        "natural_language_sql": True,
        "profiling_assistant": True,
        "explanation_engine": True,
    }
    features = {**default_features, **features_raw}

    return AIConfig(
        provider=provider,
        model=model,
        azure_openai_deployment=provider_raw.get("deployment_name", "gpt-4o"),
        azure_openai_api_version=provider_raw.get("api_version", "2024-02-01"),
        azure_openai_endpoint_secret=provider_raw.get("endpoint_secret", "azure_openai_endpoint"),
        azure_openai_api_key_secret=provider_raw.get("api_key_secret", "azure_openai_api_key"),
        openai_api_key_secret=provider_raw.get("api_key_secret", "openai_api_key"),
        anthropic_api_key_secret=provider_raw.get("api_key_secret", "anthropic_api_key"),
        temperature=float(raw.get("temperature", 0.1)),
        max_tokens=int(raw.get("max_tokens", 2048)),
        cache_enabled=bool(raw.get("cache", {}).get("enabled", True)),
        cache_ttl_seconds=int(raw.get("cache", {}).get("ttl_seconds", 3600)),
        retry=retry,
        pricing=pricing,
        features=features,
        prompts_dir=raw.get("prompts_dir", "config/prompts"),
    )


def _resolve_config_path(config_path: str | Path | None) -> Path | None:
    if config_path:
        return Path(config_path)
    candidates = [
        Path("config/ai.yml"),
        Path(__file__).parent.parent.parent / "config" / "ai.yml",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None
