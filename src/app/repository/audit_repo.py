"""Immutable audit log repository — append-only, no deletes or updates."""

from __future__ import annotations

import pandas as pd

from src.app.data.provider import DataProvider


class AuditRepository:
    def __init__(self, provider: DataProvider) -> None:
        self._p = provider

    def get_log(
        self,
        entity_type: str | None = None,
        performed_by: str | None = None,
        operation: str | None = None,
        days: int = 30,
        limit: int = 500,
    ) -> pd.DataFrame:
        return self._p.get_audit_log(
            entity_type=entity_type,
            performed_by=performed_by,
            operation=operation,
            days=days,
            limit=limit,
        )

    def get_actions_for_record(self, record_id: str) -> pd.DataFrame:
        """Return all stewardship actions for a specific record."""
        return self._p.get_actions(record_id)

    def get_recent_operations(self, days: int = 7) -> dict[str, int]:
        """Return operation counts for the past N days."""
        df = self.get_log(days=days, limit=1000)
        if df.empty:
            return {}
        return df.groupby("operation").size().to_dict()

    def get_activity_by_steward(self, days: int = 30) -> pd.DataFrame:
        """Return action counts grouped by steward."""
        df = self.get_log(days=days, limit=2000)
        if df.empty:
            return pd.DataFrame(columns=["performed_by", "count"])
        return (
            df.groupby("performed_by")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
