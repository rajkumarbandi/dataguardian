"""
Schema drift detection data structures.

``ColumnDrift`` captures a single detected difference between the registered
schema and the incoming DataFrame schema.  ``SchemaDriftReport`` aggregates all
detected differences for a given source and exposes convenience properties for
breaking-change detection and serialisation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnDrift:
    """A single detected schema difference for one column."""

    column_name: str
    drift_type: str           # MISSING | ADDED | TYPE_CHANGE | NULLABILITY_CHANGE
    expected_type: str | None = None
    actual_type: str | None = None
    expected_nullable: bool | None = None
    actual_nullable: bool | None = None
    is_breaking: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "column_name": self.column_name,
            "drift_type": self.drift_type,
            "expected_type": self.expected_type,
            "actual_type": self.actual_type,
            "expected_nullable": self.expected_nullable,
            "actual_nullable": self.actual_nullable,
            "is_breaking": self.is_breaking,
        }


@dataclass
class SchemaDriftReport:
    """
    Aggregated schema drift results for a single source validation.

    Attributes
    ----------
    source_name:
        The source identifier (matches YAML ``name:``).
    schema_version:
        Version number of the registered schema that was compared.
    evolution_mode:
        The evolution mode in effect when the comparison was performed.
    missing_columns:
        Columns in the registered schema absent from the incoming data.
        These are always breaking — the downstream pipeline expects them.
    additional_columns:
        Columns in the incoming data not in the registered schema.
        Non-breaking by default; allowed or auto-registered depending on mode.
    type_changes:
        Columns where the incoming type differs from the registered type.
        Breaking unless ``allow_type_promotion=True`` and the change is a
        widening promotion (e.g. int → long).
    nullability_changes:
        Columns where nullable status differs.  Breaking unless
        ``allow_nullable_changes=True``.
    """

    source_name: str
    schema_version: int
    evolution_mode: str
    missing_columns: list[ColumnDrift] = field(default_factory=list)
    additional_columns: list[ColumnDrift] = field(default_factory=list)
    type_changes: list[ColumnDrift] = field(default_factory=list)
    nullability_changes: list[ColumnDrift] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        """``True`` when any category of drift is present."""
        return bool(
            self.missing_columns
            or self.additional_columns
            or self.type_changes
            or self.nullability_changes
        )

    @property
    def all_changes(self) -> list[ColumnDrift]:
        """Flat list of all detected changes regardless of category."""
        return (
            self.missing_columns
            + self.additional_columns
            + self.type_changes
            + self.nullability_changes
        )

    @property
    def breaking_changes(self) -> list[ColumnDrift]:
        """Subset of changes flagged as breaking."""
        return [c for c in self.all_changes if c.is_breaking]

    @property
    def non_breaking_changes(self) -> list[ColumnDrift]:
        """Subset of changes that are safe to proceed with."""
        return [c for c in self.all_changes if not c.is_breaking]

    @property
    def has_breaking_changes(self) -> bool:
        """``True`` when at least one breaking change is detected."""
        return bool(self.breaking_changes)

    def summary_message(self) -> str:
        """One-line human-readable drift summary for logging."""
        parts = []
        if self.missing_columns:
            parts.append(f"{len(self.missing_columns)} missing")
        if self.additional_columns:
            parts.append(f"{len(self.additional_columns)} additional")
        if self.type_changes:
            parts.append(f"{len(self.type_changes)} type changes")
        if self.nullability_changes:
            parts.append(f"{len(self.nullability_changes)} nullability changes")
        return ", ".join(parts) if parts else "no drift"

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable representation for audit table storage."""
        return {
            "source_name": self.source_name,
            "schema_version": self.schema_version,
            "evolution_mode": self.evolution_mode,
            "has_drift": self.has_drift,
            "has_breaking_changes": self.has_breaking_changes,
            "missing_columns": [c.to_dict() for c in self.missing_columns],
            "additional_columns": [c.to_dict() for c in self.additional_columns],
            "type_changes": [c.to_dict() for c in self.type_changes],
            "nullability_changes": [c.to_dict() for c in self.nullability_changes],
        }

    def to_json(self) -> str:
        """JSON string of ``to_dict()`` for Delta table storage."""
        return json.dumps(self.to_dict(), default=str)
