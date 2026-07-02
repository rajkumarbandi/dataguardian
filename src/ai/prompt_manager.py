"""
Prompt Manager — loads prompt templates from YAML files and renders them.

Prompts are stored in config/prompts/<feature>.yml.
The PromptManager is the single source of truth for all prompt text.
Feature modules never contain raw prompt strings.

YAML structure for each prompt file:
    system: |
        You are a data quality expert...
    user: |
        Analyse the following record:
        Column: {{ column_name }}
        Value: {{ value }}
        Rule: {{ rule_name }}

Template rendering uses simple {{ variable }} substitution (no Jinja2 dependency).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from src.ai.provider import LLMMessage


class PromptRenderError(ValueError):
    """Raised when a required template variable is missing."""


class PromptNotFoundError(KeyError):
    """Raised when the requested feature prompt file does not exist."""


class PromptManager:
    """
    Loads and renders prompt templates from the prompts directory.

    Caches loaded YAML files in memory — the files are read once at first use.
    """

    def __init__(self, prompts_dir: str | Path = "config/prompts") -> None:
        self._dir = Path(prompts_dir)
        self._cache: dict[str, dict[str, str]] = {}

    def render(self, feature: str, variables: dict[str, Any] | None = None) -> list[LLMMessage]:
        """
        Load the prompt for `feature` and render it with `variables`.

        Returns a list of LLMMessage objects ready for LLMProvider.complete().
        Raises PromptNotFoundError if the feature file doesn't exist.
        Raises PromptRenderError if a required variable is missing.
        """
        template = self._load(feature)
        vars_: dict[str, Any] = variables or {}

        messages: list[LLMMessage] = []
        if "system" in template:
            messages.append(LLMMessage(
                role="system",
                content=_render_template(template["system"], vars_, feature),
            ))
        if "user" in template:
            messages.append(LLMMessage(
                role="user",
                content=_render_template(template["user"], vars_, feature),
            ))
        if not messages:
            raise PromptRenderError(f"Prompt '{feature}' has neither 'system' nor 'user' key.")
        return messages

    def get_raw(self, feature: str) -> dict[str, str]:
        """Return the raw (un-rendered) template dict for introspection."""
        return dict(self._load(feature))

    def list_features(self) -> list[str]:
        """Return all available feature names (file stems in prompts_dir)."""
        if not self._dir.exists():
            return []
        return sorted(p.stem for p in self._dir.glob("*.yml"))

    # ── Internal ───────────────────────────────────────────────────────────────

    def _load(self, feature: str) -> dict[str, str]:
        if feature in self._cache:
            return self._cache[feature]

        path = self._dir / f"{feature}.yml"
        if not path.exists():
            # Try resolving relative to project root
            alt = Path(__file__).parent.parent.parent / "config" / "prompts" / f"{feature}.yml"
            if alt.exists():
                path = alt
            else:
                raise PromptNotFoundError(
                    f"Prompt file not found: '{path}'. "
                    f"Available features: {self.list_features()}"
                )

        with path.open(encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}

        self._cache[feature] = {k: str(v) for k, v in raw.items()}
        return self._cache[feature]


# ── Template rendering ─────────────────────────────────────────────────────────

_PLACEHOLDER = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def _render_template(template: str, variables: dict[str, Any], feature: str) -> str:
    missing: list[str] = []

    def replacer(match: re.Match) -> str:  # type: ignore[type-arg]
        key = match.group(1)
        if key not in variables:
            missing.append(key)
            return match.group(0)
        return str(variables[key])

    result = _PLACEHOLDER.sub(replacer, template)
    if missing:
        raise PromptRenderError(
            f"Prompt '{feature}' requires variables that were not provided: {missing}. "
            f"Provided: {list(variables.keys())}"
        )
    return result
