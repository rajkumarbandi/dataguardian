"""Stewardship action orchestration service."""

from __future__ import annotations

import pandas as pd

from src.app.data.provider import DataProvider
from src.app.models.stewardship import StewardshipRecord
from src.app.repository.comments_repo import CommentsRepository
from src.app.repository.stewardship_repo import StewardshipRepository


class StewardshipService:
    """Coordinates all steward actions and enforces business rules."""

    def __init__(self, provider: DataProvider) -> None:
        self._repo = StewardshipRepository(provider)
        self._comments = CommentsRepository(provider)

    # ── Queries ────────────────────────────────────────────────────────────────

    def get_record(self, record_id: str) -> StewardshipRecord | None:
        return self._repo.get_record(record_id)

    def list_pending(
        self,
        source_name: str | None = None,
        assigned_to: str | None = None,
    ) -> pd.DataFrame:
        return self._repo.list_records(
            status="PENDING",
            source_name=source_name,
            assigned_to=assigned_to,
        )

    def list_my_queue(self, steward_name: str) -> pd.DataFrame:
        """Return records assigned to the given steward that still need action."""
        return self._repo.list_records(assigned_to=steward_name, limit=200)

    def get_record_comments(self, record_id: str) -> pd.DataFrame:
        return self._comments.get_thread(record_id)

    def get_sources(self) -> list[str]:
        return self._repo.get_sources()

    def get_stewards(self) -> list[str]:
        return self._repo.get_stewards()

    # ── Actions ────────────────────────────────────────────────────────────────

    def approve(self, record_id: str, performed_by: str, justification: str) -> None:
        if not justification.strip():
            raise ValueError("A justification comment is required for approval.")
        self._repo.approve(record_id, performed_by, justification)

    def reject(self, record_id: str, performed_by: str, reason: str) -> None:
        if not reason.strip():
            raise ValueError("A reason is required for rejection.")
        self._repo.reject(record_id, performed_by, reason)

    def request_correction(self, record_id: str, performed_by: str, instructions: str) -> None:
        if not instructions.strip():
            raise ValueError("Correction instructions are required.")
        self._repo.request_correction(record_id, performed_by, instructions)

    def assign(self, record_id: str, assigned_to: str, performed_by: str) -> None:
        self._repo.assign(record_id, assigned_to, performed_by)

    def add_comment(
        self,
        record_id: str,
        author: str,
        message: str,
        parent_comment_id: str | None = None,
    ) -> None:
        if not message.strip():
            raise ValueError("Comment message cannot be empty.")
        self._comments.add_comment(record_id, author, message, parent_comment_id)
