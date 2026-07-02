"""
Root Cause Analyzer — AI-powered batch DQ failure root cause analysis.

Identifies systemic patterns across a batch of failed records and produces
a prioritised remediation report. Works on groups of records, not individuals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.prompt_manager import PromptManager
from src.ai.provider import LLMProvider
from src.ai.token_counter import TokenCounter


@dataclass
class RootCauseReport:
    source_name: str
    batch_id: str
    total_records: int
    failed_records: int
    failure_rate: float
    report: str
    top_violations: list[dict[str, Any]]
    prompt_tokens: int
    completion_tokens: int
    cached: bool


class RootCauseAnalyzer:
    """
    Analyses a batch of stewardship records to identify root causes of DQ failures.

    Usage:
        analyzer = RootCauseAnalyzer(provider, prompt_manager, config, cache, counter)
        report = analyzer.analyze_batch(records_df, source_name="customers", batch_id="B001")
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

    def analyze_batch(
        self,
        records_df: pd.DataFrame,
        source_name: str,
        batch_id: str,
        previous_failure_rate: float | None = None,
    ) -> RootCauseReport:
        """
        Analyse a DataFrame of stewardship records for systemic DQ patterns.

        Args:
            records_df: Filtered subset of stewardship_records for the batch
            source_name: The source entity name
            batch_id: The batch identifier
            previous_failure_rate: Optional historical comparison baseline
        """
        total = len(records_df)
        if total == 0:
            return RootCauseReport(
                source_name=source_name,
                batch_id=batch_id,
                total_records=0,
                failed_records=0,
                failure_rate=0.0,
                report="No records in batch — analysis not possible.",
                top_violations=[],
                prompt_tokens=0,
                completion_tokens=0,
                cached=False,
            )

        failed = int(records_df["violation_count"].gt(0).sum()) if "violation_count" in records_df.columns else total
        failure_rate = round(failed / total * 100, 1) if total > 0 else 0.0

        top_violations, violation_summary = _summarize_violations(records_df)
        sample_failures = _sample_failures(records_df)
        prev_rate = f"{previous_failure_rate:.1f}" if previous_failure_rate is not None else "N/A"
        trend = _compute_trend(failure_rate, previous_failure_rate)

        variables = {
            "source_name": source_name,
            "batch_id": batch_id,
            "total_records": str(total),
            "failed_records": str(failed),
            "failure_rate": str(failure_rate),
            "violation_summary": violation_summary,
            "sample_failures": sample_failures,
            "previous_failure_rate": prev_rate,
            "trend": trend,
        }

        messages = self._pm.render("root_cause", variables)
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
            self._counter.record_from_response("root_cause_analyzer", response)

        return RootCauseReport(
            source_name=source_name,
            batch_id=batch_id,
            total_records=total,
            failed_records=failed,
            failure_rate=failure_rate,
            report=response.content,
            top_violations=top_violations,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            cached=cached,
        )


def _summarize_violations(df: pd.DataFrame) -> tuple[list[dict[str, Any]], str]:
    """Aggregate violation types across all records."""
    import json

    counts: dict[str, int] = {}
    for _, row in df.iterrows():
        rules = row.get("failed_rules", [])
        if isinstance(rules, str):
            try:
                rules = json.loads(rules)
            except Exception:
                rules = []
        for rule in (rules or []):
            name = rule.get("rule_name", "unknown") if isinstance(rule, dict) else "unknown"
            counts[name] = counts.get(name, 0) + 1

    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    total_violations = sum(counts.values()) or 1
    top_violations = [
        {"rule_name": k, "count": v, "pct": round(v / total_violations * 100, 1)}
        for k, v in sorted_counts[:10]
    ]
    summary_lines = [
        f"  {i + 1}. {item['rule_name']}: {item['count']} occurrences ({item['pct']}%)"
        for i, item in enumerate(top_violations)
    ]
    return top_violations, "\n".join(summary_lines) or "  No violations recorded"


def _sample_failures(df: pd.DataFrame) -> str:
    """Produce a compact summary of the first 10 failed records."""
    sample = df.head(10)
    lines: list[str] = []
    for _, row in sample.iterrows():
        lines.append(
            f"  - record_id={row.get('record_id', 'N/A')[:8]}... "
            f"dq_score={row.get('dq_score', 0):.2f} "
            f"violations={row.get('violation_count', 0)}"
        )
    return "\n".join(lines) or "  No sample data"


def _compute_trend(current: float, previous: float | None) -> str:
    if previous is None:
        return "No historical data available"
    delta = current - previous
    if abs(delta) < 1.0:
        return f"Stable (Δ{delta:+.1f}%)"
    if delta > 0:
        return f"Degrading (+{delta:.1f}% vs previous batch)"
    return f"Improving ({delta:.1f}% vs previous batch)"
