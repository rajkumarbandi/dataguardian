"""
DataGuardian schema management package — Milestone 5.

Provides schema registry, drift detection, validation, and evolution
capabilities for the Silver validation pipeline.

Main entry point for notebooks::

    from src.schema import SchemaRegistry, SchemaValidator, SchemaHistoryWriter

    registry = SchemaRegistry(
        spark=spark,
        catalog=catalog,
        enabled=env_config.schema_registry.schema_registry_enabled,
    )
    validator = SchemaValidator(registry=registry)
    result = validator.validate(
        df=bronze_df,
        source_config=source_config,
        evolution_mode="ALLOW_NEW_COLUMNS",
        run_id=run.run_id,
    )
    if not result.can_proceed:
        raise PipelineExecutionException(result.message)
    bronze_df = result.resolved_df
"""

from src.schema.schema_comparator import SchemaComparator
from src.schema.schema_drift_report import ColumnDrift, SchemaDriftReport
from src.schema.schema_evolution_manager import SchemaEvolutionManager
from src.schema.schema_history_writer import SchemaHistoryWriter
from src.schema.schema_registry import SchemaRegistry, SchemaVersion
from src.schema.schema_validator import SchemaValidationResult, SchemaValidator

__all__ = [
    "ColumnDrift",
    "SchemaComparator",
    "SchemaDriftReport",
    "SchemaEvolutionManager",
    "SchemaHistoryWriter",
    "SchemaRegistry",
    "SchemaValidationResult",
    "SchemaValidator",
    "SchemaVersion",
]
