"""Threaded comments repository."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.app.data.provider import DataProvider


class CommentsRepository:
    def __init__(self, provider: DataProvider) -> None:
        self._p = provider

    def get_thread(self, record_id: str) -> pd.DataFrame:
        """Return all comments for a record, sorted chronologically."""
        return self._p.get_comments(record_id)

    def add_comment(self, record_id: str, author: str, message: str, parent_comment_id: str | None = None) -> None:
        self._p.save_comment({
            "record_id": record_id,
            "parent_comment_id": parent_comment_id,
            "author": author,
            "message": message,
            "status": "ACTIVE",
            "created_at": datetime.utcnow(),
        })

    def get_recent_activity(self, limit: int = 20) -> pd.DataFrame:
        """Return recent comments across all records for the dashboard activity feed."""
        df = self._p.get_audit_log(limit=limit)
        return df
