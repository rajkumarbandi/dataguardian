"""
Duplicate Detector — AI-powered entity resolution and duplicate detection.

Identifies whether multiple records refer to the same real-world entity
using semantic similarity, naming conventions, and domain knowledge.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.prompt_manager import PromptManager
from src.ai.provider import LLMProvider
from src.ai.token_counter import TokenCounter


@dataclass
class DuplicateCandidate:
    record_ids: list[str]
    field_values: dict[str, list[str]]     # {field_name: [value_a, value_b, ...]}


@dataclass
class DuplicateResult:
    entity_type: str
    domain: str
    analysis: str
    candidates_analysed: int
    prompt_tokens: int
    completion_tokens: int
    cached: bool


class DuplicateDetector:
    """
    Detects potential duplicate records using LLM-based semantic analysis.

    Usage:
        detector = DuplicateDetector(provider, prompt_manager, config, cache, counter)
        result = detector.detect(
            candidates=[
                DuplicateCandidate(
                    record_ids=["r1", "r2"],
                    field_values={"name": ["IBM Corporation", "International Business Machines"]},
                )
            ],
            entity_type="vendor",
            domain="Procurement",
            matching_attributes=["name", "tax_id"],
        )
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

    def detect(
        self,
        candidates: list[DuplicateCandidate],
        entity_type: str = "record",
        domain: str = "General",
        matching_attributes: list[str] | None = None,
    ) -> DuplicateResult:
        """
        Analyse a list of candidate duplicate groups.

        Args:
            candidates: List of DuplicateCandidate objects (pairs or clusters)
            entity_type: The type of entity (customer, vendor, product, etc.)
            domain: Business domain for context
            matching_attributes: Fields used for matching
        """
        if not candidates:
            return DuplicateResult(
                entity_type=entity_type,
                domain=domain,
                analysis="No candidate records provided.",
                candidates_analysed=0,
                prompt_tokens=0,
                completion_tokens=0,
                cached=False,
            )

        candidate_text = _format_candidates(candidates)
        attrs_text = ", ".join(matching_attributes or ["name"])

        variables = {
            "entity_type": entity_type,
            "domain": domain,
            "candidate_records": candidate_text,
            "matching_attributes": attrs_text,
        }

        messages = self._pm.render("duplicate_detection", variables)
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
            self._counter.record_from_response("duplicate_detector", response)

        return DuplicateResult(
            entity_type=entity_type,
            domain=domain,
            analysis=response.content,
            candidates_analysed=len(candidates),
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            cached=cached,
        )


def _format_candidates(candidates: list[DuplicateCandidate]) -> str:
    lines: list[str] = []
    for i, cand in enumerate(candidates, 1):
        lines.append(f"**Candidate Group {i}** (IDs: {', '.join(cand.record_ids[:4])}):")
        for field_name, values in cand.field_values.items():
            lines.append(f"  {field_name}: {' | '.join(str(v) for v in values)}")
        lines.append("")
    return "\n".join(lines)
