"""
Schema evolution manager — applies evolution-mode policy decisions to an
incoming DataFrame and optionally registers a new schema version.

``SchemaEvolutionManager`` is invoked by ``SchemaValidator`` after
``SchemaComparator`` has produced a drift report and the caller has determined
that the drift is safe to evolve (no missing columns, no incompatible type
changes).  It registers a new schema version in the registry and returns the
(unchanged) DataFrame alongside the new active version number.

Evolution modes handled here
----------------------------
Only ``AUTO_EVOLVE`` triggers schema registration.  ``STRICT`` and
``ALLOW_NEW_COLUMNS`` decisions are made upstream in ``SchemaValidator`` —
this class is only called when the policy verdict is "evolve and continue".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from src.common.models import SourceConfig
    from src.schema.schema_drift_report import SchemaDriftReport
    from src.schema.schema_registry import SchemaRegistry


class SchemaEvolutionManager:
    """
    Applies ``AUTO_EVOLVE`` policy by registering a new schema version.

    Parameters
    ----------
    registry:
        Active ``SchemaRegistry`` for the current environment.
    logger:
        Optional pre-bound logger.
    """

    def __init__(
        self,
        registry: SchemaRegistry,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._registry = registry
        self._log = logger or get_logger("dataguardian.schema.evolution")

    def apply(
        self,
        df: DataFrame,
        drift_report: SchemaDriftReport,
        source_config: SourceConfig,
        run_id: str = "",
    ) -> tuple[DataFrame, int]:
        """
        Register a new schema version based on the incoming DataFrame schema.

        The DataFrame is returned unchanged — evolution does not reshape data.
        Only called when ``AUTO_EVOLVE`` is the active mode and all drift is
        non-breaking (no missing columns, no incompatible type changes).

        Parameters
        ----------
        df:
            Incoming Bronze DataFrame whose schema will be registered.
        drift_report:
            Drift report from ``SchemaComparator.compare()``.
        source_config:
            Parsed source YAML — provides name for registry lookup.
        run_id:
            Pipeline run ID recorded in the registry entry.

        Returns
        -------
        tuple[DataFrame, int]
            ``(df, active_version)`` — the unchanged DataFrame and the new
            schema version number.
        """
        if drift_report.missing_columns:
            # Guard against being called with blocking drift — return unchanged
            self._log.warning(
                "SchemaEvolutionManager.apply() called with missing columns — skipped",
                source_name=source_config.name,
                missing=[c.column_name for c in drift_report.missing_columns],
            )
            return df, drift_report.schema_version

        # Build a human-readable change summary for the registry entry
        change_parts = []
        if drift_report.additional_columns:
            names = ", ".join(c.column_name for c in drift_report.additional_columns)
            change_parts.append(
                f"{len(drift_report.additional_columns)} new column(s): {names}"
            )
        if drift_report.type_changes:
            change_parts.append(
                f"{len(drift_report.type_changes)} type promotion(s)"
            )
        if drift_report.nullability_changes:
            change_parts.append(
                f"{len(drift_report.nullability_changes)} nullability change(s)"
            )
        change_summary = "; ".join(change_parts) or "schema updated"

        new_sv = self._registry.register_schema(
            source_name=source_config.name,
            schema=df.schema,
            run_id=run_id,
            evolution_mode=drift_report.evolution_mode,
            change_summary=change_summary,
        )

        self._log.info(
            "AUTO_EVOLVE: new schema version registered",
            source_name=source_config.name,
            new_version=new_sv.version,
            previous_version=drift_report.schema_version,
            change_summary=change_summary,
        )
        return df, new_sv.version
