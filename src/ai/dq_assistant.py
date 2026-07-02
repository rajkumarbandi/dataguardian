"""
DQ Assistant — AI-powered data quality violation explainer.

Translates technical DQ rule failures into plain-English business impact assessments
that a non-technical data steward can read and act on immediately.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.prompt_manager import PromptManager
from src.ai.provider import LLMProvider
from src.ai.token_counter import TokenCounter


@dataclass
class DQExplanation:
    record_id: str
    rule_name: str
    column_name: str
    explanation: str
    risk_level: str          # LOW | MEDIUM | HIGH | CRITICAL
    prompt_tokens: int
    completion_tokens: int
    cached: bool


class DQAssistant:
    """
    Explains DQ violations in plain English for business stewards.

    Usage:
        assistant = DQAssistant(provider, prompt_manager, config, cache, counter)
        explanation = assistant.explain_failure(record, failed_rule)
    """

    def __init__(
        self,
        provider: LLMProvider,
        prompt_manager: PromptManager,
        config: AIConfig,
        cache: PromptCache,
        token_counter: TokenCounter,
    ) -> None:
        self._provider = provider
        self._pm = prompt_manager
        self._config = config
        self._cache = cache
        self._counter = token_counter

    def explain_failure(self, record: dict[str, Any], failed_rule: dict[str, Any]) -> DQExplanation:
        """
        Generate a plain-English explanation for a single DQ rule failure.

        Args:
            record: The stewardship record dict (from StewardshipRecord or DataProvider)
            failed_rule: One entry from failed_rules (rule_name, column, severity, etc.)
        """
        raw_record = record.get("raw_record", {})
        if isinstance(raw_record, str):
            raw_record = json.loads(raw_record)

        # Summarise key fields for the prompt (top 8 fields, no PII labels)
        raw_summary = "\n".join(
            f"  {k}: {v}" for k, v in list(raw_record.items())[:8]
        )

        variables = {
            "source_name": record.get("source_name", "unknown"),
            "table_name": record.get("table_name", "unknown"),
            "dq_score": f"{float(record.get('dq_score', 0.0)):.1%}",
            "rule_name": failed_rule.get("rule_name", "unknown"),
            "column_name": failed_rule.get("column", "unknown"),
            "severity": failed_rule.get("severity", "WARNING"),
            "expected_value": str(failed_rule.get("expected_value", "N/A")),
            "actual_value": str(failed_rule.get("actual_value", "N/A")),
            "rule_message": failed_rule.get("message", ""),
            "raw_record_summary": raw_summary or "No field details available",
        }

        messages = self._pm.render("dq_explanation", variables)
        cached = False

        cached_response = self._cache.get(messages)
        if cached_response is not None:
            response = cached_response
            cached = True
        else:
            response = self._provider.complete(
                messages,
                max_tokens=self._config.max_tokens,
                temperature=self._config.temperature,
            )
            self._cache.put(messages, response)

        if not cached:
            self._counter.record_from_response("dq_assistant", response)

        risk_level = _extract_risk_level(response.content)

        return DQExplanation(
            record_id=str(record.get("record_id", "")),
            rule_name=str(failed_rule.get("rule_name", "")),
            column_name=str(failed_rule.get("column", "")),
            explanation=response.content,
            risk_level=risk_level,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            cached=cached,
        )


def _extract_risk_level(text: str) -> str:
    """Extract the risk level from the LLM response text."""
    upper = text.upper()
    for level in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        if level in upper:
            return level
    return "UNKNOWN"
