"""Core stewardship domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class RecordStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    CORRECTION_REQUESTED = "CORRECTION_REQUESTED"


class ActionType(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    REQUEST_CORRECTION = "REQUEST_CORRECTION"
    COMMENT = "COMMENT"
    ASSIGN = "ASSIGN"
    REASSIGN = "REASSIGN"


class SeverityLevel(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class FailedRule:
    rule_name: str
    column_name: str
    severity: SeverityLevel
    message: str
    expected: str = ""
    actual: str = ""


@dataclass
class StewardshipRecord:
    record_id: str
    run_id: str
    source_name: str
    batch_id: str
    table_name: str
    dq_score: float
    status: RecordStatus
    assigned_to: str
    violation_count: int
    failed_rules: list[FailedRule]
    raw_record: dict[str, Any]
    ingested_at: datetime
    created_at: datetime
    reviewed_at: datetime | None = None
    reviewed_by: str | None = None
    updated_at: datetime | None = None


@dataclass
class StewardshipAction:
    action_id: str
    record_id: str
    action_type: ActionType
    performed_by: str
    comment: str
    assigned_to: str
    previous_status: RecordStatus | None
    new_status: RecordStatus | None
    action_timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Comment:
    comment_id: str
    record_id: str
    parent_comment_id: str | None
    author: str
    message: str
    status: str
    created_at: datetime
