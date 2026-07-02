"""
Schema Mapper — AI-powered source-to-target field mapping suggestions.

Converts source schema field names to target schema field names using semantic
analysis, common naming conventions, and domain knowledge. Returns confidence
scores and explanations for each mapping.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.prompt_manager import PromptManager
from src.ai.provider import LLMProvider
from src.ai.token_counter import TokenCounter


@dataclass
class FieldMapping:
    source_field: str
    target_field: str | None    # None = unmapped
    confidence: int             # 0–100
    reasoning: str
    requires_review: bool       # True when confidence < 80


@dataclass
class SchemaMappingResult:
    source_system: str
    domain: str
    mappings: list[FieldMapping]
    unmapped_fields: list[str]
    raw_response: str
    prompt_tokens: int
    completion_tokens: int
    cached: bool

    @property
    def high_confidence_count(self) -> int:
        return sum(1 for m in self.mappings if m.target_field and m.confidence >= 80)

    @property
    def review_required_count(self) -> int:
        return sum(1 for m in self.mappings if m.requires_review)


class SchemaMapper:
    """
    Suggests field-level mappings from a source schema to a target schema.

    Usage:
        mapper = SchemaMapper(provider, prompt_manager, config, cache, counter)
        result = mapper.suggest_mappings(
            source_fields=["CustomerName", "DOB", "Email_Addr"],
            target_fields=["customer_name", "birth_date", "email"],
            source_system="Salesforce",
            domain="CRM",
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

    def suggest_mappings(
        self,
        source_fields: list[str],
        target_fields: list[str],
        source_system: str = "Unknown",
        domain: str = "General",
        notes: str = "",
    ) -> SchemaMappingResult:
        variables = {
            "source_fields": "\n".join(f"  - {f}" for f in source_fields),
            "target_fields": "\n".join(f"  - {f}" for f in target_fields),
            "source_system": source_system,
            "domain": domain,
            "notes": notes or "None",
        }

        messages = self._pm.render("schema_mapping", variables)
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
            self._counter.record_from_response("schema_mapper", response)

        mappings, unmapped = _parse_mappings(response.content, source_fields)

        return SchemaMappingResult(
            source_system=source_system,
            domain=domain,
            mappings=mappings,
            unmapped_fields=unmapped,
            raw_response=response.content,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            cached=cached,
        )


def _parse_mappings(text: str, source_fields: list[str]) -> tuple[list[FieldMapping], list[str]]:
    """
    Extract structured mappings from the LLM response text.

    The LLM produces markdown-formatted output. This parser extracts the
    arrow-notation mappings (**Source** → `target`) and confidence percentages.
    Falls back to creating placeholder mappings when parsing is uncertain.
    """
    import re

    mappings: list[FieldMapping] = []
    unmapped: list[str] = []
    mapped_sources: set[str] = set()

    # Pattern: **FieldName** → `target_field` ... Confidence: N%
    arrow_pattern = re.compile(
        r"\*\*(.+?)\*\*\s*→\s*`(.+?)`.*?Confidence:\s*(\d+)%",
        re.IGNORECASE,
    )
    unmapped_pattern = re.compile(r"⛔\s*Unmapped.*?`(.+?)`", re.IGNORECASE)

    for match in arrow_pattern.finditer(text):
        src = match.group(1).strip()
        tgt = match.group(2).strip()
        conf = int(match.group(3))

        # Find the surrounding text for reasoning (next ~200 chars)
        start = match.end()
        snippet = text[start : start + 300].strip()
        reasoning_match = re.search(r"\*Reason\*:\s*(.+?)(?=\n|\Z)", snippet, re.DOTALL)
        reasoning = reasoning_match.group(1).strip() if reasoning_match else "See full response for details."

        mappings.append(FieldMapping(
            source_field=src,
            target_field=tgt,
            confidence=conf,
            reasoning=reasoning,
            requires_review=conf < 80,
        ))
        mapped_sources.add(src.lower())

    for match in unmapped_pattern.finditer(text):
        unmapped.append(match.group(1).strip())

    # Any source fields not found in the response are implicitly unmapped
    for sf in source_fields:
        if sf.lower() not in mapped_sources and sf not in unmapped:
            unmapped.append(sf)

    return mappings, unmapped
