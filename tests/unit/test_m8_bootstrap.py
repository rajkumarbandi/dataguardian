"""
Unit tests for PipelineBootstrap, PipelineContext, and discover_sources() (Milestone 8).

No real Spark, no real config files.  Every external dependency is mocked so
the tests run in milliseconds without a Databricks cluster.
"""

from __future__ import annotations

import dataclasses
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.bootstrap import PipelineBootstrap, PipelineContext
from src.pipeline import discover_sources


# ---------------------------------------------------------------------------
# Helpers — mock factories
# ---------------------------------------------------------------------------


def _mock_env_config() -> MagicMock:
    cfg = MagicMock()
    cfg.unity_catalog.catalog = "dg_test"
    cfg.storage.adls_root = "abfss://test"
    cfg.pipeline.pipeline_name = "dataguardian"
    cfg.pipeline.pipeline_version = "0.8.0"
    cfg.pipeline.audit_enabled = True
    cfg.pipeline.retry_policy.max_attempts = 3
    cfg.schema_registry.schema_registry_enabled = True
    cfg.schema_registry.schema_audit_enabled = True
    cfg.schema_registry.default_evolution_mode = "STRICT"
    cfg.transformation.audit_enabled = True
    cfg.contract_validation.contract_validation_enabled = True
    cfg.contract_validation.contract_audit_enabled = True
    cfg.contract_validation.default_contract_policy = "FAIL_PIPELINE"
    cfg.logging.level = "INFO"
    return cfg


def _mock_spark() -> MagicMock:
    spark = MagicMock()
    spark.version = "3.5.0"
    return spark


def _all_src_mocks() -> dict[str, MagicMock]:
    """Return a flat dict of patch targets → MagicMock for every src.* import in bootstrap."""
    return {
        "src.bootstrap.ConfigLoader": MagicMock(),
        "src.bootstrap.get_logger": MagicMock(return_value=MagicMock()),
        "src.bootstrap.SecretsManager": MagicMock(),
        "src.bootstrap.SparkSessionManager": MagicMock(),
        "src.bootstrap.UnityCatalogClient": MagicMock(),
        "src.bootstrap.SchemaRegistry": MagicMock(),
        "src.bootstrap.SchemaValidator": MagicMock(),
        "src.bootstrap.SchemaHistoryWriter": MagicMock(),
        "src.bootstrap.TransformationEngine": MagicMock(),
        "src.bootstrap.TransformationHistoryWriter": MagicMock(),
        "src.bootstrap.DataQualityEngine": MagicMock(),
        "src.bootstrap.DQResultsWriter": MagicMock(),
        "src.bootstrap.MetricsWriter": MagicMock(),
        "src.bootstrap.ContractValidationEngine": MagicMock(),
        "src.bootstrap.ContractHistoryWriter": MagicMock(),
        "src.bootstrap.SilverWriter": MagicMock(),
        "src.bootstrap.PipelineRunTracker": MagicMock(),
        "src.bootstrap.RetryHelper": MagicMock(),
    }


# ---------------------------------------------------------------------------
# PipelineContext — dataclass structure
# ---------------------------------------------------------------------------


class TestPipelineContextStructure:
    def test_is_dataclass(self):
        assert dataclasses.is_dataclass(PipelineContext)

    def test_required_fields_present(self):
        field_names = {f.name for f in dataclasses.fields(PipelineContext)}
        required = {
            "env", "catalog", "notebook_name",
            "loader", "env_config",
            "spark", "logger", "uc_client", "secrets",
            "schema_registry", "schema_validator", "schema_history_writer",
            "transformation_engine", "transformation_history_writer",
            "dq_engine", "dq_writer", "metrics_writer",
            "contract_engine", "contract_history_writer",
            "silver_writer",
            "tracker", "retry",
        }
        assert required.issubset(field_names)

    def test_context_instantiation_with_mocks(self):
        ctx = PipelineContext(
            env="test",
            catalog="dg_test",
            notebook_name="nb",
            loader=MagicMock(),
            env_config=MagicMock(),
            spark=MagicMock(),
            logger=MagicMock(),
            uc_client=MagicMock(),
            secrets=None,
            schema_registry=MagicMock(),
            schema_validator=MagicMock(),
            schema_history_writer=MagicMock(),
            transformation_engine=MagicMock(),
            transformation_history_writer=MagicMock(),
            dq_engine=MagicMock(),
            dq_writer=MagicMock(),
            metrics_writer=MagicMock(),
            contract_engine=MagicMock(),
            contract_history_writer=MagicMock(),
            silver_writer=MagicMock(),
            tracker=MagicMock(),
            retry=MagicMock(),
        )
        assert ctx.env == "test"
        assert ctx.catalog == "dg_test"
        assert ctx.secrets is None


# ---------------------------------------------------------------------------
# PipelineBootstrap.initialize()
# ---------------------------------------------------------------------------


class TestPipelineBootstrapInitialize:
    def _run_bootstrap(
        self,
        mocks: dict[str, Any],
        env: str = "test",
        secrets_scope: str | None = None,
    ) -> PipelineContext:
        """Apply all patches and call PipelineBootstrap.initialize()."""
        env_config = _mock_env_config()
        mocks["src.bootstrap.ConfigLoader"].return_value.get_environment.return_value = env_config

        with patch.multiple("src.bootstrap", **{
            k.replace("src.bootstrap.", ""): v for k, v in mocks.items()
        }):
            return PipelineBootstrap.initialize(
                env=env,
                spark=_mock_spark(),
                dbutils=MagicMock(),
                notebook_name="test_nb",
                secrets_scope=secrets_scope,
            )

    def test_returns_pipeline_context(self):
        mocks = _all_src_mocks()
        ctx = self._run_bootstrap(mocks)
        assert isinstance(ctx, PipelineContext)

    def test_env_and_catalog_set_correctly(self):
        mocks = _all_src_mocks()
        ctx = self._run_bootstrap(mocks, env="test")
        assert ctx.env == "test"
        assert ctx.catalog == "dg_test"

    def test_notebook_name_stored(self):
        mocks = _all_src_mocks()
        ctx = self._run_bootstrap(mocks)
        assert ctx.notebook_name == "test_nb"

    def test_secrets_none_when_no_scope(self):
        mocks = _all_src_mocks()
        ctx = self._run_bootstrap(mocks, secrets_scope=None)
        # SecretsManager should NOT be instantiated
        mocks["src.bootstrap.SecretsManager"].assert_not_called()

    def test_secrets_created_when_scope_provided(self):
        mocks = _all_src_mocks()
        ctx = self._run_bootstrap(mocks, secrets_scope="dataguardian-test-scope")
        mocks["src.bootstrap.SecretsManager"].assert_called_once()
        call_kwargs = mocks["src.bootstrap.SecretsManager"].call_args.kwargs
        assert call_kwargs["scope"] == "dataguardian-test-scope"

    def test_uc_client_use_catalog_called(self):
        mocks = _all_src_mocks()
        self._run_bootstrap(mocks)
        uc_instance = mocks["src.bootstrap.UnityCatalogClient"].return_value
        uc_instance.use_catalog.assert_called_once()

    def test_audit_schema_created(self):
        mocks = _all_src_mocks()
        self._run_bootstrap(mocks)
        uc_instance = mocks["src.bootstrap.UnityCatalogClient"].return_value
        schema_calls = [c.args[0] for c in uc_instance.create_schema_if_not_exists.call_args_list]
        assert "audit" in schema_calls
        assert "silver" in schema_calls

    def test_schema_registry_created_with_enabled_flag(self):
        mocks = _all_src_mocks()
        self._run_bootstrap(mocks)
        mocks["src.bootstrap.SchemaRegistry"].assert_called_once()
        kwargs = mocks["src.bootstrap.SchemaRegistry"].call_args.kwargs
        assert kwargs["enabled"] is True  # from _mock_env_config

    def test_transformation_history_writer_created_with_audit_flag(self):
        mocks = _all_src_mocks()
        self._run_bootstrap(mocks)
        mocks["src.bootstrap.TransformationHistoryWriter"].assert_called_once()
        kwargs = mocks["src.bootstrap.TransformationHistoryWriter"].call_args.kwargs
        assert kwargs["enabled"] is True

    def test_contract_history_writer_created_with_audit_flag(self):
        mocks = _all_src_mocks()
        self._run_bootstrap(mocks)
        mocks["src.bootstrap.ContractHistoryWriter"].assert_called_once()
        kwargs = mocks["src.bootstrap.ContractHistoryWriter"].call_args.kwargs
        assert kwargs["enabled"] is True

    def test_config_loader_called_with_env(self):
        mocks = _all_src_mocks()
        self._run_bootstrap(mocks, env="qa")
        mocks["src.bootstrap.ConfigLoader"].assert_called_once_with(env="qa")

    def test_all_engines_present_in_context(self):
        mocks = _all_src_mocks()
        ctx = self._run_bootstrap(mocks)
        # Verify every engine field is not None
        assert ctx.dq_engine is not None
        assert ctx.transformation_engine is not None
        assert ctx.contract_engine is not None
        assert ctx.schema_registry is not None
        assert ctx.schema_validator is not None

    def test_all_writers_present_in_context(self):
        mocks = _all_src_mocks()
        ctx = self._run_bootstrap(mocks)
        assert ctx.dq_writer is not None
        assert ctx.metrics_writer is not None
        assert ctx.silver_writer is not None
        assert ctx.schema_history_writer is not None
        assert ctx.transformation_history_writer is not None
        assert ctx.contract_history_writer is not None

    def test_tracker_and_retry_present(self):
        mocks = _all_src_mocks()
        ctx = self._run_bootstrap(mocks)
        assert ctx.tracker is not None
        assert ctx.retry is not None


# ---------------------------------------------------------------------------
# discover_sources()
# ---------------------------------------------------------------------------


class TestDiscoverSources:
    def _context(self) -> MagicMock:
        ctx = MagicMock(spec=PipelineContext)
        ctx.logger = MagicMock()
        return ctx

    def test_single_source_returned_as_list(self, tmp_path):
        ctx = self._context()
        result = discover_sources("customers", ctx)
        assert result == ["customers"]

    def test_all_discovers_yaml_files(self, tmp_path):
        sources_dir = tmp_path / "sources"
        sources_dir.mkdir()
        (sources_dir / "customers.yml").write_text("")
        (sources_dir / "orders.yml").write_text("")
        (sources_dir / "products.yml").write_text("")

        ctx = self._context()
        result = discover_sources("all", ctx, config_root=str(tmp_path))
        assert result == ["customers", "orders", "products"]

    def test_all_sorted_alphabetically(self, tmp_path):
        sources_dir = tmp_path / "sources"
        sources_dir.mkdir()
        for name in ["zebra", "alpha", "middle"]:
            (sources_dir / f"{name}.yml").write_text("")

        ctx = self._context()
        result = discover_sources("all", ctx, config_root=str(tmp_path))
        assert result == ["alpha", "middle", "zebra"]

    def test_underscore_prefixed_files_excluded(self, tmp_path):
        sources_dir = tmp_path / "sources"
        sources_dir.mkdir()
        (sources_dir / "customers.yml").write_text("")
        (sources_dir / "_template.yml").write_text("")

        ctx = self._context()
        result = discover_sources("all", ctx, config_root=str(tmp_path))
        assert "_template" not in result
        assert "customers" in result

    def test_example_prefixed_files_excluded(self, tmp_path):
        sources_dir = tmp_path / "sources"
        sources_dir.mkdir()
        (sources_dir / "orders.yml").write_text("")
        (sources_dir / "example_source.yml").write_text("")

        ctx = self._context()
        result = discover_sources("all", ctx, config_root=str(tmp_path))
        assert "example_source" not in result

    def test_all_with_no_sources_raises(self, tmp_path):
        from src.common.exceptions import ValidationException

        ctx = self._context()
        with pytest.raises(ValidationException, match="No source YAML files"):
            discover_sources("all", ctx, config_root=str(tmp_path))

    def test_whitespace_trimmed_from_source_name(self):
        ctx = self._context()
        result = discover_sources("  customers  ", ctx)
        assert result == ["customers"]
