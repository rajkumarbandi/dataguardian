"""
Profiling Assistant — AI-powered data quality executive summary for a batch.

Takes computed profile statistics (column nulls, type errors, value distributions)
and generates a plain-English executive brief suitable for business stakeholders.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.prompt_manager import PromptManager
from src.ai.provider import LLMProvider
from src.ai.token_counter import TokenCounter


@dataclass
class ColumnIssue:
    column_name: str
    issue_type: str
    affected_count: int
    total_count: int
    pct: float
    risk: str   # LOW | MEDIUM | HIGH | CRITICAL


@dataclass
class DataProfile:
    source_name: str
    batch_id: str
    total_records: int
    passed_records: int
    failed_records: int
    dq_score: float
    column_issues: list[ColumnIssue]
    summary: str
    prompt_tokens: int
    completion_tokens: int
    cached: bool

    @property
    def failure_rate(self) -> float:
        return self.failed_records / self.total_records * 100 if self.total_records > 0 else 0.0


class ProfilingAssistant:
    """
    Generates an AI-powered executive summary from a batch of stewardship records.

    Usage:
        assistant = ProfilingAssistant(provider, prompt_manager, config, cache, counter)
        profile = assistant.profile(records_df, source_name="customers", batch_id="B001")
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

    def profile(
        self,
        records_df: pd.DataFrame,
        source_name: str,
        batch_id: str,
        historical_df: pd.DataFrame | None = None,
    ) -> DataProfile:
        """
        Generate an AI data quality executive summary.

        Args:
            records_df: Stewardship records for the source/batch
            source_name: The source entity name
            batch_id: Batch identifier
            historical_df: Optional prior batch for trend comparison
        """
        if records_df.empty:
            return DataProfile(
                source_name=source_name,
                batch_id=batch_id,
                total_records=0,
                passed_records=0,
                failed_records=0,
                dq_score=0.0,
                column_issues=[],
                summary="No records found for profiling.",
                prompt_tokens=0,
                completion_tokens=0,
                cached=False,
            )

        total = len(records_df)
        failed = int(records_df["violation_count"].gt(0).sum()) if "violation_count" in records_df.columns else 0
        passed = total - failed
        avg_score = float(records_df["dq_score"].mean()) if "dq_score" in records_df.columns else 0.0

        column_issues = _compute_column_issues(records_df)
        violation_breakdown = _compute_violation_breakdown(records_df)
        historical_comparison = _compare_historical(records_df, historical_df)

        variables = {
            "source_name": source_name,
            "batch_id": batch_id,
            "total_records": str(total),
            "passed_records": str(passed),
            "failed_records": str(failed),
            "dq_score": f"{avg_score * 100:.1f}",
            "column_issues": _format_column_issues(column_issues),
            "violation_breakdown": violation_breakdown,
            "historical_comparison": historical_comparison,
        }

        messages = self._pm.render("data_profiling", variables)
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
            self._counter.record_from_response("profiling_assistant", response)

        return DataProfile(
            source_name=source_name,
            batch_id=batch_id,
            total_records=total,
            passed_records=passed,
            failed_records=failed,
            dq_score=avg_score,
            column_issues=column_issues,
            summary=response.content,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            cached=cached,
        )


def _compute_column_issues(df: pd.DataFrame) -> list[ColumnIssue]:
    """Extract per-column issue counts from failed_rules JSON."""
    import json
    from collections import defaultdict

    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "rule": "unknown"})
    total = len(df)

    for _, row in df.iterrows():
        rules = row.get("failed_rules", [])
        if isinstance(rules, str):
            try:
                rules = json.loads(rules)
            except Exception:
                rules = []
        seen_cols: set[str] = set()
        for rule in (rules or []):
            if not isinstance(rule, dict):
                continue
            col = rule.get("column", "unknown")
            if col not in seen_cols:
                counts[col]["count"] += 1
                counts[col]["rule"] = rule.get("rule_name", "unknown")
                seen_cols.add(col)

    issues: list[ColumnIssue] = []
    for col, data in sorted(counts.items(), key=lambda x: x[1]["count"], reverse=True)[:8]:
        pct = data["count"] / total * 100 if total > 0 else 0.0
        risk = "CRITICAL" if pct > 20 else "HIGH" if pct > 10 else "MEDIUM" if pct > 5 else "LOW"
        issues.append(ColumnIssue(
            column_name=col,
            issue_type=data["rule"],
            affected_count=data["count"],
            total_count=total,
            pct=round(pct, 1),
            risk=risk,
        ))
    return issues


def _format_column_issues(issues: list[ColumnIssue]) -> str:
    if not issues:
        return "  No column-level issues detected"
    lines = [
        f"  - {i.column_name}: {i.issue_type} ({i.pct}% of records) — Risk: {i.risk}"
        for i in issues
    ]
    return "\n".join(lines)


def _compute_violation_breakdown(df: pd.DataFrame) -> str:
    import json
    from collections import Counter
    counts: Counter[str] = Counter()
    for _, row in df.iterrows():
        rules = row.get("failed_rules", [])
        if isinstance(rules, str):
            try:
                rules = json.loads(rules)
            except Exception:
                rules = []
        for rule in (rules or []):
            if isinstance(rule, dict):
                counts[rule.get("rule_name", "unknown")] += 1

    if not counts:
        return "  No violations recorded"
    total = sum(counts.values())
    lines = [
        f"  {i + 1}. {name}: {cnt} ({cnt / total * 100:.1f}%)"
        for i, (name, cnt) in enumerate(counts.most_common(8))
    ]
    return "\n".join(lines)


def _compare_historical(current_df: pd.DataFrame, historical_df: pd.DataFrame | None) -> str:
    if historical_df is None or historical_df.empty:
        return "No historical data available for comparison."
    curr_score = float(current_df["dq_score"].mean()) if "dq_score" in current_df.columns else 0.0
    prev_score = float(historical_df["dq_score"].mean()) if "dq_score" in historical_df.columns else 0.0
    delta = curr_score - prev_score
    direction = "improved" if delta > 0 else "degraded" if delta < 0 else "unchanged"
    return (
        f"DQ score has {direction} by {abs(delta):.1%} vs the previous batch "
        f"(current: {curr_score:.1%}, previous: {prev_score:.1%})."
    )
