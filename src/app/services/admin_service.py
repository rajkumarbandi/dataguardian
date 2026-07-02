"""Administration read-only service — system health and configuration overview."""

from __future__ import annotations

import sys

import pandas as pd

from src.app.config.settings import AppSettings
from src.app.data.provider import DataProvider
from src.app.repository.audit_repo import AuditRepository
from src.app.repository.pipeline_repo import PipelineRepository
from src.app.repository.stewardship_repo import StewardshipRepository


class AdminService:
    def __init__(self, provider: DataProvider, settings: AppSettings) -> None:
        self._sr = StewardshipRepository(provider)
        self._pr = PipelineRepository(provider)
        self._ar = AuditRepository(provider)
        self._settings = settings

    def get_system_info(self) -> dict[str, str]:
        return {
            "App Version": self._settings.app_version,
            "Environment": self._settings.environment.upper(),
            "Catalog": self._settings.catalog,
            "Data Mode": "Demo (Sample Data)" if self._settings.demo_mode else "Live (Delta Lake)",
            "Python Version": sys.version.split()[0],
            "Cache TTL": f"{self._settings.cache_ttl_seconds}s",
        }

    def get_steward_activity(self, days: int = 30) -> pd.DataFrame:
        return self._ar.get_activity_by_steward(days=days)

    def get_record_status_summary(self) -> dict[str, int]:
        return self._sr.count_by_status()

    def get_source_summary(self) -> dict[str, int]:
        return self._sr.count_by_source()

    def get_pipeline_health(self) -> list:
        return self._pr.get_metrics_by_source()

    def get_recent_audit_log(self, limit: int = 100) -> pd.DataFrame:
        return self._ar.get_log(days=90, limit=limit)
