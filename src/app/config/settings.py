"""Application configuration resolved from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class AppSettings:
    catalog: str
    environment: str
    warehouse_http_path: str
    server_hostname: str
    demo_mode: bool
    app_title: str = "DataGuardian — Stewardship Portal"
    app_version: str = "0.9.0"
    page_size: int = 25
    cache_ttl_seconds: int = 30


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return application settings, resolved once per process."""
    host = os.environ.get("DATABRICKS_HOST", "")
    demo = os.environ.get("DG_DEMO_MODE", "true").lower() in ("true", "1", "yes")
    # When no warehouse HTTP path is configured, fall back to demo mode
    http_path = os.environ.get("DG_WAREHOUSE_HTTP_PATH", "")
    if not http_path:
        demo = True
    return AppSettings(
        catalog=os.environ.get("DG_CATALOG", "dg_prod"),
        environment=os.environ.get("DG_ENV", "prod"),
        warehouse_http_path=http_path,
        server_hostname=host.replace("https://", "").rstrip("/"),
        demo_mode=demo,
    )
