"""
Comment Summarizer — AI-powered stewardship discussion thread summarisation.

Condenses long discussion threads into a concise executive brief so senior
stewards and data owners can make decisions without reading every message.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.prompt_manager import PromptManager
from src.ai.provider import LLMProvider
from src.ai.token_counter import TokenCounter


@dataclass
class CommentSummary:
    record_id: str
    thread_count: int
    participants: list[str]
    summary: str
    prompt_tokens: int
    completion_tokens: int
    cached: bool


class CommentSummarizer:
    """
    Summarises stewardship discussion threads into concise executive briefs.

    Usage:
        summarizer = CommentSummarizer(provider, prompt_manager, config, cache, counter)
        summary = summarizer.summarize(record=record_dict, comments_df=df)
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

    def summarize(
        self,
        record: dict,
        comments_df: pd.DataFrame,
    ) -> CommentSummary:
        """
        Summarise a discussion thread for a stewardship record.

        Args:
            record: The stewardship record dict
            comments_df: DataFrame of comments from CommentsRepo (columns: author, message, created_at)
        """
        if comments_df.empty:
            return CommentSummary(
                record_id=str(record.get("record_id", "")),
                thread_count=0,
                participants=[],
                summary="No discussion thread available for this record.",
                prompt_tokens=0,
                completion_tokens=0,
                cached=False,
            )

        thread_count = len(comments_df)
        participants = sorted(comments_df["author"].dropna().unique().tolist())
        thread_content = _format_thread(comments_df)

        # Build violation summary
        failed_rules = record.get("failed_rules", [])
        if isinstance(failed_rules, str):
            import json
            failed_rules = json.loads(failed_rules)
        violation_summary = ", ".join(
            r.get("rule_name", "unknown") for r in (failed_rules[:3] if failed_rules else [])
        ) or "No violations listed"

        variables = {
            "record_id": str(record.get("record_id", "N/A")),
            "source_name": str(record.get("source_name", "unknown")),
            "violation_summary": violation_summary,
            "participants": ", ".join(participants) if participants else "Unknown",
            "thread_count": str(thread_count),
            "thread_content": thread_content,
        }

        messages = self._pm.render("comment_summary", variables)
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
            self._counter.record_from_response("comment_summarizer", response)

        return CommentSummary(
            record_id=str(record.get("record_id", "")),
            thread_count=thread_count,
            participants=participants,
            summary=response.content,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            cached=cached,
        )


def _format_thread(df: pd.DataFrame) -> str:
    """Format comment DataFrame as a readable thread string."""
    lines: list[str] = []
    for _, row in df.iterrows():
        ts = row.get("created_at", "")
        author = row.get("author", "Unknown")
        message = row.get("message", "")
        parent = row.get("parent_comment_id")
        prefix = "  ↳ " if parent else ""
        lines.append(f"{prefix}[{ts}] {author}: {message}")
    return "\n".join(lines)
