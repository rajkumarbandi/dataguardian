"""Stewardship records repository — all record reads/writes go through here."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd

from src.app.data.provider import DataProvider
from src.app.models.stewardship import FailedRule, RecordStatus, SeverityLevel, StewardshipRecord


class StewardshipRepository:
    def __init__(self, provider: DataProvider) -> None:
        self._p = provider

    # ── Reads ──────────────────────────────────────────────────────────────────

    def list_records(
        self,
        status: str | None = None,
        source_name: str | None = None,
        assigned_to: str | None = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        return self._p.get_stewardship_records(
            status=status,
            source_name=source_name,
            assigned_to=assigned_to,
            limit=limit,
        )

    def get_record(self, record_id: str) -> StewardshipRecord | None:
        raw = self._p.get_stewardship_record(record_id)
        if raw is None:
            return None
        return self._hydrate(raw)

    def get_pending(self, assigned_to: str | None = None) -> pd.DataFrame:
        return self.list_records(status="PENDING", assigned_to=assigned_to)

    def count_by_status(self) -> dict[str, int]:
        df = self._p.get_stewardship_records(limit=5000)
        if df.empty:
            return {s.value: 0 for s in RecordStatus}
        return df.groupby("status").size().to_dict()

    def count_by_source(self) -> dict[str, int]:
        df = self._p.get_stewardship_records(limit=5000)
        if df.empty:
            return {}
        return df.groupby("source_name").size().to_dict()

    def get_sources(self) -> list[str]:
        return self._p.get_sources()

    def get_stewards(self) -> list[str]:
        return self._p.get_stewards()

    # ── Writes ─────────────────────────────────────────────────────────────────

    def approve(self, record_id: str, performed_by: str, comment: str = "") -> None:
        rec = self._p.get_stewardship_record(record_id)
        prev = rec["status"] if rec else "PENDING"
        self._p.update_record_status(record_id, "APPROVED", prev, performed_by, comment)
        self._p.save_action({
            "record_id": record_id,
            "action_type": "APPROVE",
            "performed_by": performed_by,
            "comment": comment,
            "assigned_to": performed_by,
            "previous_status": prev,
            "new_status": "APPROVED",
            "action_timestamp": datetime.utcnow(),
        })

    def reject(self, record_id: str, performed_by: str, comment: str = "") -> None:
        rec = self._p.get_stewardship_record(record_id)
        prev = rec["status"] if rec else "PENDING"
        self._p.update_record_status(record_id, "REJECTED", prev, performed_by, comment)
        self._p.save_action({
            "record_id": record_id,
            "action_type": "REJECT",
            "performed_by": performed_by,
            "comment": comment,
            "assigned_to": performed_by,
            "previous_status": prev,
            "new_status": "REJECTED",
            "action_timestamp": datetime.utcnow(),
        })

    def request_correction(self, record_id: str, performed_by: str, comment: str = "") -> None:
        rec = self._p.get_stewardship_record(record_id)
        prev = rec["status"] if rec else "PENDING"
        self._p.update_record_status(record_id, "CORRECTION_REQUESTED", prev, performed_by, comment)
        self._p.save_action({
            "record_id": record_id,
            "action_type": "REQUEST_CORRECTION",
            "performed_by": performed_by,
            "comment": comment,
            "assigned_to": performed_by,
            "previous_status": prev,
            "new_status": "CORRECTION_REQUESTED",
            "action_timestamp": datetime.utcnow(),
        })

    def assign(self, record_id: str, assigned_to: str, performed_by: str) -> None:
        self._p.save_action({
            "record_id": record_id,
            "action_type": "ASSIGN",
            "performed_by": performed_by,
            "comment": f"Assigned to {assigned_to}",
            "assigned_to": assigned_to,
            "previous_status": None,
            "new_status": None,
            "action_timestamp": datetime.utcnow(),
        })

    # ── Hydration ──────────────────────────────────────────────────────────────

    @staticmethod
    def _hydrate(raw: dict[str, Any]) -> StewardshipRecord:
        rules_raw = raw.get("failed_rules") or []
        if isinstance(rules_raw, str):
            rules_raw = json.loads(rules_raw)
        rules = [
            FailedRule(
                rule_name=r.get("rule_name", ""),
                column_name=r.get("column_name", ""),
                severity=SeverityLevel(r.get("severity", "error")),
                message=r.get("message", ""),
                expected=r.get("expected", ""),
                actual=r.get("actual", ""),
            )
            for r in rules_raw
        ]
        raw_record = raw.get("raw_record") or {}
        if isinstance(raw_record, str):
            raw_record = json.loads(raw_record)

        def _dt(val: Any) -> datetime | None:
            if val is None:
                return None
            if isinstance(val, datetime):
                return val
            return pd.to_datetime(val).to_pydatetime()

        return StewardshipRecord(
            record_id=raw["record_id"],
            run_id=raw["run_id"],
            source_name=raw["source_name"],
            batch_id=raw["batch_id"],
            table_name=raw["table_name"],
            dq_score=float(raw.get("dq_score") or 0.0),
            status=RecordStatus(raw["status"]),
            assigned_to=raw.get("assigned_to") or "",
            violation_count=int(raw.get("violation_count") or 0),
            failed_rules=rules,
            raw_record=raw_record,
            ingested_at=_dt(raw.get("ingested_at")) or datetime.utcnow(),
            created_at=_dt(raw.get("created_at")) or datetime.utcnow(),
            reviewed_at=_dt(raw.get("reviewed_at")),
            reviewed_by=raw.get("reviewed_by"),
            updated_at=_dt(raw.get("updated_at")),
        )
