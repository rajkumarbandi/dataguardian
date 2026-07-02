"""Pipeline execution and metrics models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class PipelineRun:
    run_id: str
    source_name: str
    batch_id: str
    status: str
    start_time: datetime
    end_time: datetime | None
    duration_seconds: float | None
    bronze_rows_read: int
    silver_rows_written: int
    failed_rows: int
    dq_score: float | None
    schema_violations: int
    contract_violations: int
    error_message: str | None = None


@dataclass
class PipelineMetrics:
    source_name: str
    total_runs: int
    success_rate: float
    avg_dq_score: float
    avg_duration_seconds: float
    total_rows_processed: int
    total_failed_rows: int
    last_run_at: datetime | None
