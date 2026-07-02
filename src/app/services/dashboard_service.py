"""Dashboard aggregation service."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.app.data.provider import DataProvider
from src.app.repository.audit_repo import AuditRepository
from src.app.repository.pipeline_repo import PipelineRepository
from src.app.repository.stewardship_repo import StewardshipRepository


@dataclass
class DashboardSummary:
    total_pending: int
    total_approved: int
    total_rejected: int
    total_correction: int
    avg_dq_score: float
    records_by_source: dict[str, int]
    rule_violation_counts: dict[str, int]
    recent_actions: pd.DataFrame
    pipeline_run_summary: pd.DataFrame
    approval_rate_7d: float
    records_resolved_7d: int
    active_stewards: list[str] = field(default_factory=list)


class DashboardService:
    def __init__(self, provider: DataProvider) -> None:
        self._sr = StewardshipRepository(provider)
        self._pr = PipelineRepository(provider)
        self._ar = AuditRepository(provider)

    def get_summary(self) -> DashboardSummary:
        counts = self._sr.count_by_status()
        by_source = self._sr.count_by_source()
        recent_actions = self._ar.get_log(days=7, limit=100)
        runs = self._pr.list_runs(limit=30)

        # Aggregate DQ score from pipeline runs
        success_runs = runs[runs["status"] == "SUCCESS"] if not runs.empty else pd.DataFrame()
        avg_dq = float(success_runs["dq_score"].mean()) if not success_runs.empty else 0.0

        # Violation rule breakdown from all records
        rule_counts = self._compute_rule_breakdown()

        # 7-day stats
        approved_7d = len(recent_actions[recent_actions["operation"] == "APPROVE"]) if not recent_actions.empty else 0
        rejected_7d = len(recent_actions[recent_actions["operation"] == "REJECT"]) if not recent_actions.empty else 0
        resolved_7d = approved_7d + rejected_7d
        approval_rate = (approved_7d / resolved_7d) if resolved_7d > 0 else 0.0

        # Active stewards
        active = []
        if not recent_actions.empty:
            active = recent_actions["performed_by"].dropna().unique().tolist()[:5]

        # Pipeline run summary (last 5 runs per source)
        run_summary = runs.head(20) if not runs.empty else pd.DataFrame()

        return DashboardSummary(
            total_pending=counts.get("PENDING", 0),
            total_approved=counts.get("APPROVED", 0),
            total_rejected=counts.get("REJECTED", 0),
            total_correction=counts.get("CORRECTION_REQUESTED", 0),
            avg_dq_score=avg_dq,
            records_by_source=by_source,
            rule_violation_counts=rule_counts,
            recent_actions=recent_actions,
            pipeline_run_summary=run_summary,
            approval_rate_7d=approval_rate,
            records_resolved_7d=resolved_7d,
            active_stewards=active,
        )

    def _compute_rule_breakdown(self) -> dict[str, int]:
        import json
        df = self._sr.list_records(status="PENDING", limit=1000)
        if df.empty:
            return {}
        counts: dict[str, int] = {}
        for rules_json in df["failed_rules"].dropna():
            try:
                rules = json.loads(rules_json) if isinstance(rules_json, str) else rules_json
                for rule in rules:
                    name = rule.get("rule_name", "unknown")
                    counts[name] = counts.get(name, 0) + 1
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))
