"""Pipeline metrics and trend analysis service."""

from __future__ import annotations

import pandas as pd

from src.app.data.provider import DataProvider
from src.app.models.pipeline import PipelineMetrics
from src.app.repository.pipeline_repo import PipelineRepository


class PipelineService:
    def __init__(self, provider: DataProvider) -> None:
        self._repo = PipelineRepository(provider)

    def get_all_metrics(self) -> list[PipelineMetrics]:
        return self._repo.get_metrics_by_source()

    def get_runs(self, source_name: str | None = None, limit: int = 50) -> pd.DataFrame:
        return self._repo.list_runs(source_name=source_name, limit=limit)

    def get_dq_trend(self, source_name: str | None = None, limit: int = 20) -> pd.DataFrame:
        return self._repo.get_dq_trend(source_name=source_name, limit=limit)

    def get_volume_trend(self, source_name: str | None = None, limit: int = 20) -> pd.DataFrame:
        return self._repo.get_volume_trend(source_name=source_name, limit=limit)

    def get_overall_health(self) -> dict[str, object]:
        """Aggregate health metrics across all sources."""
        metrics = self.get_all_metrics()
        if not metrics:
            return {"status": "no_data"}
        total_runs = sum(m.total_runs for m in metrics)
        total_rows = sum(m.total_rows_processed for m in metrics)
        total_failed = sum(m.total_failed_rows for m in metrics)
        avg_success_rate = sum(m.success_rate for m in metrics) / len(metrics)
        avg_dq = sum(m.avg_dq_score for m in metrics) / len(metrics)
        return {
            "total_runs": total_runs,
            "total_rows_processed": total_rows,
            "total_failed_rows": total_failed,
            "overall_success_rate": round(avg_success_rate, 4),
            "avg_dq_score": round(avg_dq, 4),
            "source_count": len(metrics),
        }
