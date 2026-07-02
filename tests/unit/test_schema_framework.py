"""
Unit tests for the M5 Schema Management Framework.

Covers:
- SchemaDriftReport — computed properties and serialisation
- SchemaComparator — all four drift types, type promotion, system column skipping
- SchemaRegistry — disabled mode (no-op) and YAML-to-struct conversion
- SchemaValidator — first run, no drift, STRICT / ALLOW_NEW_COLUMNS / AUTO_EVOLVE
- SchemaEvolutionManager — AUTO_EVOLVE path with disabled registry

Tests run against local PySpark and a disabled SchemaRegistry (no Delta writes).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.schema.schema_comparator import SchemaComparator, _is_promotable, _type_name
from src.schema.schema_drift_report import ColumnDrift, SchemaDriftReport
from src.schema.schema_evolution_manager import SchemaEvolutionManager
from src.schema.schema_registry import SchemaRegistry, SchemaVersion
from src.schema.schema_validator import SchemaValidator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_struct(*fields: tuple) -> StructType:
    """Helper to create a StructType from (name, type, nullable) tuples."""
    return StructType([StructField(n, t, nullable) for n, t, nullable in fields])


def _make_registered_sv(schema: StructType, version: int = 1) -> SchemaVersion:
    """Return a SchemaVersion wrapping a given StructType."""
    return SchemaVersion(
        source_name="test_source",
        version=version,
        schema_json=json.dumps(schema.jsonValue()),
        column_count=len(schema.fields),
        registered_by="test-run-id",
        registered_at=datetime.now(tz=timezone.utc),
        evolution_mode="STRICT",
        change_summary="",
    )


def _disabled_registry(spark) -> SchemaRegistry:
    """Return a SchemaRegistry configured as disabled (no Delta writes)."""
    return SchemaRegistry(spark=spark, catalog="dg_test", enabled=False)


def _source_config_stub(schema_fields=None, evolution_mode=None):
    """Return a minimal SourceConfig-like stub for validator tests."""
    stub = MagicMock()
    stub.name = "test_source"
    stub.schema = []  # no YAML schema by default

    evo = MagicMock()
    evo.evolution_mode = evolution_mode
    evo.allow_nullable_changes = False
    evo.allow_type_promotion = False
    stub.schema_evolution = evo
    return stub


# ---------------------------------------------------------------------------
# SchemaDriftReport tests
# ---------------------------------------------------------------------------


class TestSchemaDriftReport:
    def _empty_report(self) -> SchemaDriftReport:
        return SchemaDriftReport(
            source_name="test",
            schema_version=1,
            evolution_mode="STRICT",
        )

    def test_no_drift_when_all_empty(self):
        report = self._empty_report()
        assert not report.has_drift
        assert not report.has_breaking_changes
        assert report.all_changes == []
        assert report.breaking_changes == []
        assert report.non_breaking_changes == []

    def test_has_drift_on_missing_column(self):
        report = self._empty_report()
        report.missing_columns = [
            ColumnDrift("col_a", "MISSING", "string", None, True, None, True)
        ]
        assert report.has_drift
        assert report.has_breaking_changes
        assert len(report.breaking_changes) == 1

    def test_has_drift_on_additional_column(self):
        report = self._empty_report()
        report.additional_columns = [
            ColumnDrift("new_col", "ADDED", None, "string", None, True, False)
        ]
        assert report.has_drift
        assert not report.has_breaking_changes
        assert len(report.non_breaking_changes) == 1

    def test_summary_message_no_drift(self):
        assert self._empty_report().summary_message() == "no drift"

    def test_summary_message_with_multiple_drift_types(self):
        report = self._empty_report()
        report.missing_columns = [ColumnDrift("a", "MISSING", is_breaking=True)]
        report.additional_columns = [ColumnDrift("b", "ADDED", is_breaking=False)]
        report.type_changes = [ColumnDrift("c", "TYPE_CHANGE", is_breaking=True)]
        msg = report.summary_message()
        assert "1 missing" in msg
        assert "1 additional" in msg
        assert "1 type changes" in msg

    def test_to_dict_structure(self):
        report = self._empty_report()
        d = report.to_dict()
        assert "source_name" in d
        assert "has_drift" in d
        assert "has_breaking_changes" in d
        assert d["missing_columns"] == []

    def test_to_json_is_valid_json(self):
        report = self._empty_report()
        j = report.to_json()
        parsed = json.loads(j)
        assert parsed["source_name"] == "test"


# ---------------------------------------------------------------------------
# SchemaComparator tests
# ---------------------------------------------------------------------------


class TestSchemaComparator:
    def setup_method(self):
        self.comp = SchemaComparator()

    def _compare(self, registered, incoming, **kwargs):
        return self.comp.compare(
            registered=registered,
            incoming=incoming,
            source_name="src",
            schema_version=1,
            **kwargs,
        )

    def test_no_drift_identical_schemas(self):
        schema = _make_struct(("id", StringType(), False), ("name", StringType(), True))
        drift = self._compare(schema, schema)
        assert not drift.has_drift

    def test_detects_missing_column(self):
        registered = _make_struct(("id", StringType(), False), ("name", StringType(), True))
        incoming = _make_struct(("id", StringType(), False))
        drift = self._compare(registered, incoming)
        assert len(drift.missing_columns) == 1
        assert drift.missing_columns[0].column_name == "name"
        assert drift.missing_columns[0].is_breaking

    def test_detects_additional_column(self):
        registered = _make_struct(("id", StringType(), False))
        incoming = _make_struct(("id", StringType(), False), ("extra", StringType(), True))
        drift = self._compare(registered, incoming)
        assert len(drift.additional_columns) == 1
        assert drift.additional_columns[0].column_name == "extra"
        assert not drift.additional_columns[0].is_breaking

    def test_detects_type_change_breaking(self):
        registered = _make_struct(("amount", StringType(), True))
        incoming = _make_struct(("amount", IntegerType(), True))
        drift = self._compare(registered, incoming)
        assert len(drift.type_changes) == 1
        assert drift.type_changes[0].is_breaking

    def test_detects_type_promotion_non_breaking_when_allowed(self):
        registered = _make_struct(("qty", IntegerType(), True))
        incoming = _make_struct(("qty", LongType(), True))
        drift = self._compare(registered, incoming, allow_type_promotion=True)
        assert len(drift.type_changes) == 1
        assert not drift.type_changes[0].is_breaking

    def test_type_promotion_is_breaking_when_not_allowed(self):
        registered = _make_struct(("qty", IntegerType(), True))
        incoming = _make_struct(("qty", LongType(), True))
        drift = self._compare(registered, incoming, allow_type_promotion=False)
        assert len(drift.type_changes) == 1
        assert drift.type_changes[0].is_breaking

    def test_detects_nullability_change_breaking(self):
        registered = _make_struct(("id", StringType(), False))
        incoming = _make_struct(("id", StringType(), True))
        drift = self._compare(registered, incoming)
        assert len(drift.nullability_changes) == 1
        assert drift.nullability_changes[0].is_breaking

    def test_nullability_change_non_breaking_when_allowed(self):
        registered = _make_struct(("id", StringType(), False))
        incoming = _make_struct(("id", StringType(), True))
        drift = self._compare(registered, incoming, allow_nullable_changes=True)
        assert len(drift.nullability_changes) == 1
        assert not drift.nullability_changes[0].is_breaking

    def test_system_columns_excluded_from_comparison(self):
        """Columns starting with '_' in incoming are ignored — no false positives."""
        registered = _make_struct(("id", StringType(), False))
        incoming = _make_struct(
            ("id", StringType(), False),
            ("_batch_id", StringType(), True),
            ("_ingestion_timestamp", StringType(), True),
        )
        drift = self._compare(registered, incoming)
        assert not drift.has_drift

    def test_expected_and_actual_types_recorded(self):
        registered = _make_struct(("col", StringType(), True))
        incoming = _make_struct(("col", IntegerType(), True))
        drift = self._compare(registered, incoming)
        change = drift.type_changes[0]
        assert change.expected_type == "string"
        assert change.actual_type == "integer"


class TestTypeHelpers:
    def test_is_promotable_int_to_long(self):
        assert _is_promotable(IntegerType(), LongType())

    def test_is_promotable_float_to_double(self):
        assert _is_promotable(DoubleType(), DoubleType()) is False  # same type — not in set
        # float → double is in the set
        from pyspark.sql.types import FloatType
        assert _is_promotable(FloatType(), DoubleType())

    def test_not_promotable_string_to_int(self):
        assert not _is_promotable(StringType(), IntegerType())

    def test_type_name_string(self):
        assert _type_name(StringType()) == "string"

    def test_type_name_integer(self):
        assert _type_name(IntegerType()) == "integer"

    def test_type_name_long(self):
        assert _type_name(LongType()) == "long"


# ---------------------------------------------------------------------------
# SchemaRegistry tests (disabled mode — no Delta writes)
# ---------------------------------------------------------------------------


class TestSchemaRegistryDisabled:
    def test_get_registered_schema_returns_none_when_disabled(self, spark):
        registry = _disabled_registry(spark)
        assert registry.get_registered_schema("any_source") is None

    def test_register_schema_returns_version_in_memory(self, spark):
        registry = _disabled_registry(spark)
        schema = _make_struct(("id", StringType(), False))
        sv = registry.register_schema(
            source_name="test",
            schema=schema,
            run_id="run-001",
        )
        assert sv.version == 1
        assert sv.source_name == "test"
        assert sv.column_count == 1

    def test_get_next_version_is_1_when_no_existing(self, spark):
        registry = _disabled_registry(spark)
        assert registry.get_next_version("no_source") == 1

    def test_build_struct_from_yaml_returns_none_when_no_schema(self, spark):
        registry = _disabled_registry(spark)
        cfg = MagicMock()
        cfg.schema = []
        assert registry.build_struct_from_yaml(cfg) is None

    def test_build_struct_from_yaml_converts_columns(self, spark):
        from src.common.models import ColumnDefinition

        registry = _disabled_registry(spark)
        cfg = MagicMock()
        cfg.schema = [
            ColumnDefinition(name="id", type="string", nullable=False),
            ColumnDefinition(name="amount", type="double", nullable=True),
            ColumnDefinition(name="qty", type="integer", nullable=True),
        ]
        struct = registry.build_struct_from_yaml(cfg)
        assert struct is not None
        assert len(struct.fields) == 3
        assert struct["id"].dataType == StringType()
        assert struct["amount"].dataType == DoubleType()
        assert struct["qty"].dataType == IntegerType()
        assert struct["id"].nullable is False
        assert struct["amount"].nullable is True

    def test_schema_version_round_trips_through_json(self, spark):
        registry = _disabled_registry(spark)
        original = _make_struct(("id", StringType(), False), ("ts", DateType(), True))
        sv = registry.register_schema("src", original, run_id="r1")
        recovered = sv.to_struct()
        assert len(recovered.fields) == len(original.fields)
        assert recovered["id"].dataType == StringType()
        assert recovered["ts"].dataType == DateType()


# ---------------------------------------------------------------------------
# SchemaValidator tests
# ---------------------------------------------------------------------------


class TestSchemaValidatorFirstRun:
    """First-run path: no registered schema → baseline registration."""

    def _validator(self, spark) -> SchemaValidator:
        registry = _disabled_registry(spark)
        return SchemaValidator(registry=registry)

    def test_first_run_can_proceed(self, spark):
        validator = self._validator(spark)
        df = spark.createDataFrame([], _make_struct(("id", StringType(), False)))
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "STRICT", run_id="r1")
        assert result.is_first_run
        assert result.can_proceed
        assert result.is_valid
        assert result.drift_report is None
        assert result.schema_version == 1

    def test_first_run_uses_yaml_schema_when_available(self, spark):
        from src.common.models import ColumnDefinition

        registry = _disabled_registry(spark)
        validator = SchemaValidator(registry=registry)

        cfg = _source_config_stub()
        cfg.schema = [ColumnDefinition(name="id", type="string", nullable=False)]

        df = spark.createDataFrame([], _make_struct(("id", StringType(), False)))
        result = validator.validate(df, cfg, "STRICT", run_id="r1")
        assert result.is_first_run
        assert "YAML" in result.message


class TestSchemaValidatorNoDrift:
    def _validator_with_registered(self, spark, registered_schema) -> SchemaValidator:
        registry = _disabled_registry(spark)
        # Inject a pre-registered schema by monkey-patching get_registered_schema
        sv = _make_registered_sv(registered_schema)
        registry.get_registered_schema = lambda _: sv
        return SchemaValidator(registry=registry)

    def test_valid_schema_passes(self, spark):
        schema = _make_struct(("id", StringType(), False), ("name", StringType(), True))
        validator = self._validator_with_registered(spark, schema)
        df = spark.createDataFrame([], schema)
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "STRICT")
        assert result.is_valid
        assert result.can_proceed
        assert not result.is_first_run
        assert result.drift_report is not None
        assert not result.drift_report.has_drift


class TestSchemaValidatorStrict:
    def _validator_with_registered(self, spark, registered_schema) -> SchemaValidator:
        registry = _disabled_registry(spark)
        sv = _make_registered_sv(registered_schema)
        registry.get_registered_schema = lambda _: sv
        return SchemaValidator(registry=registry)

    def test_strict_blocks_on_missing_column(self, spark):
        registered = _make_struct(("id", StringType(), False), ("name", StringType(), True))
        incoming = _make_struct(("id", StringType(), False))
        validator = self._validator_with_registered(spark, registered)
        df = spark.createDataFrame([], incoming)
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "STRICT")
        assert not result.can_proceed
        assert not result.is_valid
        assert "STRICT" in result.message

    def test_strict_blocks_on_additional_column(self, spark):
        registered = _make_struct(("id", StringType(), False))
        incoming = _make_struct(("id", StringType(), False), ("extra", StringType(), True))
        validator = self._validator_with_registered(spark, registered)
        df = spark.createDataFrame([], incoming)
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "STRICT")
        # STRICT treats ANY drift as blocking
        assert not result.can_proceed


class TestSchemaValidatorAllowNewColumns:
    def _validator_with_registered(self, spark, registered_schema) -> SchemaValidator:
        registry = _disabled_registry(spark)
        sv = _make_registered_sv(registered_schema)
        registry.get_registered_schema = lambda _: sv
        return SchemaValidator(registry=registry)

    def test_allow_new_columns_passes_additional_columns(self, spark):
        registered = _make_struct(("id", StringType(), False))
        incoming = _make_struct(("id", StringType(), False), ("extra", StringType(), True))
        validator = self._validator_with_registered(spark, registered)
        df = spark.createDataFrame([], incoming)
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "ALLOW_NEW_COLUMNS")
        assert result.can_proceed
        assert result.is_valid
        assert result.drift_report.has_drift

    def test_allow_new_columns_blocks_missing_columns(self, spark):
        registered = _make_struct(("id", StringType(), False), ("required", StringType(), False))
        incoming = _make_struct(("id", StringType(), False))
        validator = self._validator_with_registered(spark, registered)
        df = spark.createDataFrame([], incoming)
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "ALLOW_NEW_COLUMNS")
        assert not result.can_proceed

    def test_allow_new_columns_blocks_incompatible_type_change(self, spark):
        registered = _make_struct(("id", StringType(), False))
        incoming = _make_struct(("id", IntegerType(), False))
        validator = self._validator_with_registered(spark, registered)
        df = spark.createDataFrame([], incoming)
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "ALLOW_NEW_COLUMNS")
        assert not result.can_proceed


class TestSchemaValidatorAutoEvolve:
    def _validator_with_registered(self, spark, registered_schema) -> SchemaValidator:
        registry = _disabled_registry(spark)
        sv = _make_registered_sv(registered_schema)
        registry.get_registered_schema = lambda _: sv
        return SchemaValidator(registry=registry)

    def test_auto_evolve_passes_additional_columns(self, spark):
        registered = _make_struct(("id", StringType(), False))
        incoming = _make_struct(("id", StringType(), False), ("new_col", StringType(), True))
        validator = self._validator_with_registered(spark, registered)
        df = spark.createDataFrame([], incoming)
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "AUTO_EVOLVE")
        assert result.can_proceed
        assert result.is_valid

    def test_auto_evolve_passes_type_promotion_when_allowed(self, spark):
        registered = _make_struct(("qty", IntegerType(), True))
        incoming = _make_struct(("qty", LongType(), True))
        validator = self._validator_with_registered(spark, registered)
        df = spark.createDataFrame([], incoming)

        cfg = _source_config_stub()
        cfg.schema_evolution.allow_type_promotion = True

        result = validator.validate(df, cfg, "AUTO_EVOLVE")
        assert result.can_proceed

    def test_auto_evolve_blocks_missing_columns(self, spark):
        registered = _make_struct(("id", StringType(), False), ("required", StringType(), False))
        incoming = _make_struct(("id", StringType(), False))
        validator = self._validator_with_registered(spark, registered)
        df = spark.createDataFrame([], incoming)
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "AUTO_EVOLVE")
        assert not result.can_proceed
        assert "missing columns" in result.message.lower()

    def test_auto_evolve_blocks_incompatible_type_change(self, spark):
        registered = _make_struct(("id", StringType(), False))
        incoming = _make_struct(("id", IntegerType(), False))
        validator = self._validator_with_registered(spark, registered)
        df = spark.createDataFrame([], incoming)
        cfg = _source_config_stub()
        result = validator.validate(df, cfg, "AUTO_EVOLVE")
        assert not result.can_proceed


# ---------------------------------------------------------------------------
# SchemaEvolutionManager tests
# ---------------------------------------------------------------------------


class TestSchemaEvolutionManager:
    def test_registers_new_version_on_additional_column(self, spark):
        from src.schema.schema_drift_report import SchemaDriftReport

        registry = _disabled_registry(spark)
        # Simulate an existing version 1 so the next registration becomes version 2
        existing_sv = _make_registered_sv(_make_struct(("id", StringType(), False)), version=1)
        registry.get_registered_schema = lambda _: existing_sv

        manager = SchemaEvolutionManager(registry=registry)

        incoming_schema = _make_struct(("id", StringType(), False), ("new", StringType(), True))
        df = spark.createDataFrame([], incoming_schema)

        drift = SchemaDriftReport(
            source_name="test",
            schema_version=1,
            evolution_mode="AUTO_EVOLVE",
            additional_columns=[
                ColumnDrift("new", "ADDED", actual_type="string", is_breaking=False)
            ],
        )

        cfg = _source_config_stub()
        _, new_version = manager.apply(df=df, drift_report=drift, source_config=cfg)
        assert new_version == 2  # v1 was current → auto-evolve registers v2

    def test_skips_registration_on_missing_columns(self, spark):
        from src.schema.schema_drift_report import SchemaDriftReport

        registry = _disabled_registry(spark)
        manager = SchemaEvolutionManager(registry=registry)

        schema = _make_struct(("id", StringType(), False))
        df = spark.createDataFrame([], schema)

        drift = SchemaDriftReport(
            source_name="test",
            schema_version=1,
            evolution_mode="AUTO_EVOLVE",
            missing_columns=[
                ColumnDrift("required", "MISSING", expected_type="string", is_breaking=True)
            ],
        )

        cfg = _source_config_stub()
        _, version = manager.apply(df=df, drift_report=drift, source_config=cfg)
        # Missing columns → manager aborts, returns original version
        assert version == 1
