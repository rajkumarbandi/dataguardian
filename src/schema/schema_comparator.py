"""
Schema comparator — field-by-field structural comparison of two PySpark schemas.

``SchemaComparator`` is a pure function object: it has no state and produces
a ``SchemaDriftReport`` describing every detected difference between a
registered (expected) schema and an incoming DataFrame schema.

Design
------
- Columns starting with ``_`` in the **incoming** schema are treated as
  DataGuardian system metadata (e.g. ``_ingestion_timestamp``, ``_batch_id``)
  and excluded from comparison.  This prevents false-positive "additional
  column" drift on every Bronze batch.
- Breaking vs non-breaking classification is performed here using caller-
  provided policy flags (``allow_nullable_changes``, ``allow_type_promotion``).
- Type promotion detection covers the most common widening casts supported
  by Spark's implicit coercion rules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.schema.schema_drift_report import ColumnDrift, SchemaDriftReport

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

# Widening type promotions that are safe for downstream consumers.
# Keyed as (from_type_name, to_type_name) using PySpark typeName().
_PROMOTABLE_PAIRS: frozenset[tuple[str, str]] = frozenset(
    {
        ("short", "integer"),
        ("short", "long"),
        ("integer", "long"),
        ("float", "double"),
        ("integer", "double"),
        ("float", "decimal"),
        ("integer", "decimal"),
        ("long", "decimal"),
    }
)


def _type_name(data_type: object) -> str:
    """Return the PySpark typeName() for a DataType instance."""
    return str(getattr(data_type, "typeName", lambda: str(data_type))())


def _is_promotable(from_type: object, to_type: object) -> bool:
    """``True`` when the type change is a safe widening promotion."""
    return (_type_name(from_type), _type_name(to_type)) in _PROMOTABLE_PAIRS


class SchemaComparator:
    """
    Compares a registered schema against an incoming DataFrame schema.

    Instantiate once and reuse across multiple comparisons — the object is
    stateless.

    Parameters
    ----------
    system_column_prefix:
        Columns in the incoming schema whose name starts with this prefix are
        silently excluded from comparison.  Defaults to ``"_"`` — DataGuardian
        metadata columns such as ``_batch_id`` and ``_ingestion_timestamp``.
    """

    def __init__(self, system_column_prefix: str = "_") -> None:
        self._prefix = system_column_prefix

    def compare(
        self,
        registered: StructType,
        incoming: StructType,
        source_name: str,
        schema_version: int,
        evolution_mode: str = "STRICT",
        allow_nullable_changes: bool = False,
        allow_type_promotion: bool = False,
    ) -> SchemaDriftReport:
        """
        Produce a ``SchemaDriftReport`` describing every structural difference.

        Parameters
        ----------
        registered:
            The canonical schema stored in the registry.
        incoming:
            The schema observed on the Bronze DataFrame.
        source_name:
            Source identifier — stored in the report for traceability.
        schema_version:
            Registry version that ``registered`` was read from.
        evolution_mode:
            Active evolution mode — recorded in the report for context.
        allow_nullable_changes:
            When ``True``, nullability differences are classified as
            non-breaking.  Defaults to ``False``.
        allow_type_promotion:
            When ``True``, widening type promotions (e.g. int → long) are
            classified as non-breaking.  Defaults to ``False``.

        Returns
        -------
        SchemaDriftReport
        """
        registered_by_name = {f.name: f for f in registered.fields}
        incoming_by_name = {
            f.name: f
            for f in incoming.fields
            if not f.name.startswith(self._prefix)
        }

        missing: list[ColumnDrift] = []
        additional: list[ColumnDrift] = []
        type_changes: list[ColumnDrift] = []
        nullability_changes: list[ColumnDrift] = []

        # Columns in registered but absent from incoming — always breaking
        for col_name, reg_field in registered_by_name.items():
            if col_name not in incoming_by_name:
                missing.append(
                    ColumnDrift(
                        column_name=col_name,
                        drift_type="MISSING",
                        expected_type=_type_name(reg_field.dataType),
                        actual_type=None,
                        expected_nullable=reg_field.nullable,
                        actual_nullable=None,
                        is_breaking=True,
                    )
                )

        # Columns in incoming but absent from registered
        for col_name, inc_field in incoming_by_name.items():
            if col_name not in registered_by_name:
                additional.append(
                    ColumnDrift(
                        column_name=col_name,
                        drift_type="ADDED",
                        expected_type=None,
                        actual_type=_type_name(inc_field.dataType),
                        expected_nullable=None,
                        actual_nullable=inc_field.nullable,
                        is_breaking=False,
                    )
                )

        # Columns present in both — check type and nullability differences
        for col_name in registered_by_name:
            if col_name not in incoming_by_name:
                continue
            reg_field = registered_by_name[col_name]
            inc_field = incoming_by_name[col_name]

            if reg_field.dataType != inc_field.dataType:
                if allow_type_promotion and _is_promotable(
                    reg_field.dataType, inc_field.dataType
                ):
                    breaking = False
                else:
                    breaking = True
                type_changes.append(
                    ColumnDrift(
                        column_name=col_name,
                        drift_type="TYPE_CHANGE",
                        expected_type=_type_name(reg_field.dataType),
                        actual_type=_type_name(inc_field.dataType),
                        expected_nullable=None,
                        actual_nullable=None,
                        is_breaking=breaking,
                    )
                )

            if reg_field.nullable != inc_field.nullable:
                nullability_changes.append(
                    ColumnDrift(
                        column_name=col_name,
                        drift_type="NULLABILITY_CHANGE",
                        expected_type=None,
                        actual_type=None,
                        expected_nullable=reg_field.nullable,
                        actual_nullable=inc_field.nullable,
                        is_breaking=not allow_nullable_changes,
                    )
                )

        return SchemaDriftReport(
            source_name=source_name,
            schema_version=schema_version,
            evolution_mode=evolution_mode,
            missing_columns=missing,
            additional_columns=additional,
            type_changes=type_changes,
            nullability_changes=nullability_changes,
        )
