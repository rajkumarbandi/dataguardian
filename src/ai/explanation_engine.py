"""
Explanation Engine — explains a stewardship record to a data steward in plain English.

Translates the combination of DQ failures, raw field values, and pipeline context
into a brief that a business user can understand without any technical knowledge.
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
class RecordExplanation:
    record_id: str
    source_name: str
    explanation: str
    recommended_action: str
    prompt_tokens: int
    completion_tokens: int
    cached: bool


class ExplanationEngine:
    """
    Explains a stewardship record to a non-technical data steward.

    Usage:
        engine = ExplanationEngine(provider, prompt_manager, config, cache, counter)
        explanation = engine.explain_record(record_dict)
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

    def explain_record(self, record: dict[str, Any]) -> RecordExplanation:
        """
        Generate a plain-English explanation for a stewardship record.

        Args:
            record: The full stewardship record dict
        """
        failed_rules = record.get("failed_rules", [])
        if isinstance(failed_rules, str):
            failed_rules = json.loads(failed_rules)

        raw_record = record.get("raw_record", {})
        if isinstance(raw_record, str):
            raw_record = json.loads(raw_record)

        violations_list = _format_violations(failed_rules)
        key_fields = _format_key_fields(raw_record)

        dq_score = float(record.get("dq_score", 0.0))
        violation_count = int(record.get("violation_count", len(failed_rules)))

        variables = {
            "record_id": str(record.get("record_id", "N/A")),
            "source_name": str(record.get("source_name", "unknown")),
            "dq_score_pct": f"{dq_score * 100:.1f}",
            "status": str(record.get("status", "PENDING")),
            "violation_count": str(violation_count),
            "violations_list": violations_list,
            "key_fields": key_fields,
        }

        messages = self._pm.render("record_explanation", variables)
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
            self._counter.record_from_response("explanation_engine", response)

        recommended_action = _extract_recommended_action(response.content)

        return RecordExplanation(
            record_id=str(record.get("record_id", "")),
            source_name=str(record.get("source_name", "unknown")),
            explanation=response.content,
            recommended_action=recommended_action,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            cached=cached,
        )


def _format_violations(failed_rules: list[dict]) -> str:
    if not failed_rules:
        return "  No violations recorded"
    lines: list[str] = []
    for i, rule in enumerate(failed_rules, 1):
        if not isinstance(rule, dict):
            continue
        lines.append(
            f"  {i}. **{rule.get('rule_name', 'unknown')}** on column `{rule.get('column', 'N/A')}`\n"
            f"     Severity: {rule.get('severity', 'WARNING')}\n"
            f"     Expected: {rule.get('expected_value', 'N/A')} | Actual: {rule.get('actual_value', 'N/A')}\n"
            f"     Message: {rule.get('message', '')}"
        )
    return "\n".join(lines)


def _format_key_fields(raw_record: dict) -> str:
    if not raw_record:
        return "  No field data available"
    # Show first 10 fields
    lines = [f"  {k}: {v}" for k, v in list(raw_record.items())[:10]]
    return "\n".join(lines)


def _extract_recommended_action(text: str) -> str:
    """Extract the recommended next action from the LLM response."""
    import re
    # Look for patterns like "Select *Request Correction*" or "choose Approve"
    patterns = [
        r"Select\s+\*?(Request Correction|Approve|Reject)\*?",
        r"choose\s+(Request Correction|Approve|Reject)",
        r"recommend.*?(Request Correction|Approve|Reject)",
        r"action.*?:\s*(Request Correction|Approve|Reject)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return "Review record and take appropriate action"
