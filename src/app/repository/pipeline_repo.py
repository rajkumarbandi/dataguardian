"""Pipeline run repository."""

from __future__ import annotations

import pandas as pd

from src.app.data.provider import DataProvider
from src.app.models.pipeline import PipelineMetrics, PipelineRun


class PipelineRepository:
    def __init__(self, provider: DataProvider) -> None:
        self._p = provider

    def list_runs(self, source_name: str | None = None, limit: int = 50) -> pd.DataFrame:
        return self._p.get_pipeline_runs(source_name=source_name, limit=limit)

    def get_metrics_by_source(self) -> list[PipelineMetrics]:
        df = self._p.get_pipeline_runs(limit=500)
        if df.empty:
            return []
        metrics: list[PipelineMetrics] = []
        for source, group in df.groupby("source_name"):
            success = group[group["status"] == "SUCCESS"]
            metrics.append(PipelineMetrics(
                source_name=str(source),
                total_runs=len(group),
                success_rate=len(success) / len(group) if len(group) > 0 else 0.0,
                avg_dq_score=float(success["dq_score"].mean()) if not success.empty else 0.0,
                avg_duration_seconds=float(group["duration_seconds"].dropna().mean()) if not group.empty else 0.0,
                total_rows_processed=int(group["bronze_rows_read"].sum()),
                total_failed_rows=int(group["failed_rows"].sum()),
                last_run_at=pd.to_datetime(group["start_time"].max()).to_pydatetime() if not group.empty else None,
            ))
        return sorted(metrics, key=lambda m: m.source_name)

    def get_dq_trend(self, source_name: str | None = None, limit: int = 20) -> pd.DataFrame:
        """Return DQ score over time for charting."""
        df = self._p.get_pipeline_runs(source_name=source_name, limit=limit)
        if df.empty:
            return pd.DataFrame(columns=["start_time", "source_name", "dq_score"])
        return df[df["status"] == "SUCCESS"][["start_time", "source_name", "dq_score"]].dropna()

    def get_volume_trend(self, source_name: str | None = None, limit: int = 20) -> pd.DataFrame:
        """Return row volume over time for charting."""
        df = self._p.get_pipeline_runs(source_name=source_name, limit=limit)
        if df.empty:
            return pd.DataFrame(columns=["start_time", "source_name", "bronze_rows_read", "silver_rows_written", "failed_rows"])
        cols = ["start_time", "source_name", "bronze_rows_read", "silver_rows_written", "failed_rows"]
        return df[cols].sort_values("start_time")
