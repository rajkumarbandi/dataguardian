"""
Schema validator — orchestrates schema registration, comparison, and evolution.

``SchemaValidator`` is the primary M5 entry point for the notebook.  It
integrates ``SchemaRegistry``, ``SchemaComparator``, and
``SchemaEvolutionManager`` into a single call per source that runs before the
DQ engine.

Flow
----
1. Retrieve the registered schema from ``SchemaRegistry``.
2. If no registration exists (first run), register the YAML schema (or the
   incoming schema when no YAML schema is defined) as version 1 and return
   ``can_proceed=True``.
3. Compare the registered schema against the incoming schema via
   ``SchemaComparator``.
4. Based on the active evolution mode, determine whether the pipeline can
   continue and whether to register a new schema version.
5. Return a ``SchemaValidationResult`` with the drift report, active version,
   and the resolved DataFrame.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.common.logger import DataGuardianLogger, get_logger
from src.schema.schema_comparator import SchemaComparator
from src.schema.schema_evolution_manager import SchemaEvolutionManager

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from src.common.models import SourceConfig
    from src.schema.schema_drift_report import SchemaDriftReport
    from src.schema.schema_registry import SchemaRegistry


@dataclass
class SchemaValidationResult:
    """
    Result of a single schema validation call.

    Attributes
    ----------
    source_name:
        Source identifier.
    schema_version:
        The schema version active after validation (may be a newly registered
        version when ``AUTO_EVOLVE`` triggered registration).
    evolution_mode:
        The evolution mode that was applied.
    is_first_run:
        ``True`` when no prior schema was registered — baseline initialisation.
    is_valid:
        ``True`` when the incoming schema exactly matches the registered schema.
    can_proceed:
        ``True`` when the pipeline is allowed to continue.  Always ``True`` on
        first run; depends on evolution mode and drift severity otherwise.
    drift_report:
        Populated after comparison.  ``None`` on first run.
    resolved_df:
        The DataFrame to use in subsequent steps.  For M5 always the same
        object as the incoming DataFrame — a future milestone may reshape it.
    message:
        Human-readable explanation of the outcome.
    """

    source_name: str
    schema_version: int
    evolution_mode: str
    is_first_run: bool
    is_valid: bool
    can_proceed: bool
    drift_report: SchemaDriftReport | None
    resolved_df: DataFrame
    message: str


class SchemaValidator:
    """
    Orchestrates schema registration, comparison, and evolution policy.

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
        self._comparator = SchemaComparator()
        self._evolution_mgr = SchemaEvolutionManager(registry=registry, logger=logger)
        self._log = logger or get_logger("dataguardian.schema.validator")

    def validate(
        self,
        df: DataFrame,
        source_config: SourceConfig,
        evolution_mode: str,
        run_id: str = "",
    ) -> SchemaValidationResult:
        """
        Validate the incoming DataFrame schema against the registered baseline.

        Parameters
        ----------
        df:
            Incoming Bronze DataFrame.
        source_config:
            Parsed source YAML.
        evolution_mode:
            Active evolution mode — resolved from env config with a possible
            per-source override from ``source_config.schema_evolution``.
        run_id:
            Pipeline run ID for registry and audit traceability.

        Returns
        -------
        SchemaValidationResult
        """
        source_name = source_config.name
        allow_nullable = source_config.schema_evolution.allow_nullable_changes
        allow_promotion = source_config.schema_evolution.allow_type_promotion

        # Retrieve the registered schema (None on first run)
        registered_sv = self._registry.get_registered_schema(source_name)

        if registered_sv is None:
            return self._handle_first_run(
                df=df,
                source_config=source_config,
                evolution_mode=evolution_mode,
                run_id=run_id,
            )

        # Compare registered schema against the incoming schema
        registered_struct = registered_sv.to_struct()
        drift = self._comparator.compare(
            registered=registered_struct,
            incoming=df.schema,
            source_name=source_name,
            schema_version=registered_sv.version,
            evolution_mode=evolution_mode,
            allow_nullable_changes=allow_nullable,
            allow_type_promotion=allow_promotion,
        )

        if not drift.has_drift:
            self._log.info(
                "Schema validation passed — no drift detected",
                source_name=source_name,
                schema_version=registered_sv.version,
                evolution_mode=evolution_mode,
            )
            return SchemaValidationResult(
                source_name=source_name,
                schema_version=registered_sv.version,
                evolution_mode=evolution_mode,
                is_first_run=False,
                is_valid=True,
                can_proceed=True,
                drift_report=drift,
                resolved_df=df,
                message=(
                    f"Schema matches version {registered_sv.version} — no drift detected."
                ),
            )

        # Drift detected — log and apply evolution policy
        self._log.warning(
            "Schema drift detected",
            source_name=source_name,
            schema_version=registered_sv.version,
            evolution_mode=evolution_mode,
            drift_summary=drift.summary_message(),
            breaking_count=len(drift.breaking_changes),
            non_breaking_count=len(drift.non_breaking_changes),
        )
        return self._apply_evolution_policy(
            df=df,
            drift=drift,
            source_config=source_config,
            run_id=run_id,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _handle_first_run(
        self,
        df: DataFrame,
        source_config: SourceConfig,
        evolution_mode: str,
        run_id: str,
    ) -> SchemaValidationResult:
        """Register the baseline schema on first run and return can_proceed=True."""
        yaml_struct = self._registry.build_struct_from_yaml(source_config)
        baseline_schema = yaml_struct if yaml_struct is not None else df.schema
        change_summary = (
            "Initial registration from YAML schema definition"
            if yaml_struct is not None
            else "Initial registration from observed incoming schema"
        )

        registered_sv = self._registry.register_schema(
            source_name=source_config.name,
            schema=baseline_schema,
            run_id=run_id,
            evolution_mode=evolution_mode,
            change_summary=change_summary,
        )

        self._log.info(
            "Schema first run — baseline registered",
            source_name=source_config.name,
            version=registered_sv.version,
            column_count=registered_sv.column_count,
            from_yaml=yaml_struct is not None,
        )
        return SchemaValidationResult(
            source_name=source_config.name,
            schema_version=registered_sv.version,
            evolution_mode=evolution_mode,
            is_first_run=True,
            is_valid=True,
            can_proceed=True,
            drift_report=None,
            resolved_df=df,
            message=(
                f"First run — schema v{registered_sv.version} registered "
                f"({'YAML definition' if yaml_struct is not None else 'inferred from data'})."
            ),
        )

    def _apply_evolution_policy(
        self,
        df: DataFrame,
        drift: SchemaDriftReport,
        source_config: SourceConfig,
        run_id: str,
    ) -> SchemaValidationResult:
        """Return a result based on the active evolution mode and drift severity."""
        evolution_mode = drift.evolution_mode
        source_name = source_config.name

        # ── STRICT ────────────────────────────────────────────────────────────
        if evolution_mode == "STRICT":
            return SchemaValidationResult(
                source_name=source_name,
                schema_version=drift.schema_version,
                evolution_mode=evolution_mode,
                is_first_run=False,
                is_valid=False,
                can_proceed=False,
                drift_report=drift,
                resolved_df=df,
                message=(
                    f"STRICT mode: schema drift detected ({drift.summary_message()}). "
                    "Update the registered schema version or change the evolution mode."
                ),
            )

        # ── ALLOW_NEW_COLUMNS ──────────────────────────────────────────────────
        if evolution_mode == "ALLOW_NEW_COLUMNS":
            if drift.has_breaking_changes:
                breaking_cols = [c.column_name for c in drift.breaking_changes]
                return SchemaValidationResult(
                    source_name=source_name,
                    schema_version=drift.schema_version,
                    evolution_mode=evolution_mode,
                    is_first_run=False,
                    is_valid=False,
                    can_proceed=False,
                    drift_report=drift,
                    resolved_df=df,
                    message=(
                        f"ALLOW_NEW_COLUMNS mode: breaking changes detected "
                        f"({drift.summary_message()}). Columns: {breaking_cols}."
                    ),
                )
            return SchemaValidationResult(
                source_name=source_name,
                schema_version=drift.schema_version,
                evolution_mode=evolution_mode,
                is_first_run=False,
                is_valid=True,
                can_proceed=True,
                drift_report=drift,
                resolved_df=df,
                message=(
                    f"ALLOW_NEW_COLUMNS mode: {len(drift.additional_columns)} "
                    "additional column(s) detected and permitted."
                ),
            )

        # ── AUTO_EVOLVE ────────────────────────────────────────────────────────
        if drift.missing_columns:
            missing_cols = [c.column_name for c in drift.missing_columns]
            return SchemaValidationResult(
                source_name=source_name,
                schema_version=drift.schema_version,
                evolution_mode=evolution_mode,
                is_first_run=False,
                is_valid=False,
                can_proceed=False,
                drift_report=drift,
                resolved_df=df,
                message=(
                    f"AUTO_EVOLVE mode: missing columns cannot be auto-resolved: "
                    f"{missing_cols}. Manual schema intervention required."
                ),
            )

        incompatible_type_changes = [c for c in drift.type_changes if c.is_breaking]
        if incompatible_type_changes:
            bad_cols = [c.column_name for c in incompatible_type_changes]
            return SchemaValidationResult(
                source_name=source_name,
                schema_version=drift.schema_version,
                evolution_mode=evolution_mode,
                is_first_run=False,
                is_valid=False,
                can_proceed=False,
                drift_report=drift,
                resolved_df=df,
                message=(
                    f"AUTO_EVOLVE mode: incompatible type changes for {bad_cols}. "
                    "Cannot auto-evolve — manual intervention required."
                ),
            )

        # Safe non-breaking drift — delegate to SchemaEvolutionManager
        resolved_df, new_version = self._evolution_mgr.apply(
            df=df,
            drift_report=drift,
            source_config=source_config,
            run_id=run_id,
        )
        return SchemaValidationResult(
            source_name=source_name,
            schema_version=new_version,
            evolution_mode=evolution_mode,
            is_first_run=False,
            is_valid=True,
            can_proceed=True,
            drift_report=drift,
            resolved_df=resolved_df,
            message=(
                f"AUTO_EVOLVE mode: schema evolved to version {new_version} "
                f"({drift.summary_message()})."
            ),
        )
