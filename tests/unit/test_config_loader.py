"""Unit tests for ConfigLoader."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.config_loader import ConfigLoader
from src.common.exceptions import ConfigurationError
from src.common.models import EnvironmentConfig, SourceConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data), encoding="utf-8")


def _env_data(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "environment": "test",
        "unity_catalog": {"catalog": "dg_test"},
    }
    base.update(overrides)
    return base


def _source_data(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "name": "customers",
        "system": "erp",
        "connector": {
            "type": "csv",
            "location": "tests/fixtures/customers.csv",
        },
        "target": {
            "catalog": "dg_test",
            "schema": "bronze",
            "table": "erp_customers",
        },
    }
    base.update(overrides)
    return base


def _make_loader(tmp_path: Path, env: str = "test") -> ConfigLoader:
    monkeypatch_env = {"DATAGUARDIAN_CONFIG_DIR": str(tmp_path)}
    for k, v in monkeypatch_env.items():
        os.environ[k] = v
    return ConfigLoader(env=env)


# ---------------------------------------------------------------------------
# EnvironmentConfig loading
# ---------------------------------------------------------------------------


class TestGetEnvironment:
    def test_loads_valid_yaml(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "test.yml", _env_data())
        loader = _make_loader(tmp_path)
        cfg = loader.get_environment()
        assert isinstance(cfg, EnvironmentConfig)
        assert cfg.environment == "test"
        assert cfg.unity_catalog.catalog == "dg_test"

    def test_result_is_cached(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "test.yml", _env_data())
        loader = _make_loader(tmp_path)
        cfg1 = loader.get_environment()
        cfg2 = loader.get_environment()
        assert cfg1 is cfg2

    def test_raises_when_file_missing(self, tmp_path: Path) -> None:
        loader = _make_loader(tmp_path)
        with pytest.raises(ConfigurationError, match="not found"):
            loader.get_environment()

    def test_raises_on_invalid_environment_value(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "bad.yml", _env_data(environment="bad"))
        loader = _make_loader(tmp_path, env="bad")
        with pytest.raises(ConfigurationError):
            loader.get_environment()

    def test_raises_when_required_field_missing(self, tmp_path: Path) -> None:
        # unity_catalog is required — omit it
        _write(tmp_path / "environments" / "test.yml", {"environment": "test"})
        loader = _make_loader(tmp_path)
        with pytest.raises(ConfigurationError, match="validation failed"):
            loader.get_environment()

    def test_raises_on_malformed_yaml(self, tmp_path: Path) -> None:
        bad_yaml = "environment: test\nunity_catalog: [\n  - broken"
        path = tmp_path / "environments" / "test.yml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(bad_yaml, encoding="utf-8")
        loader = _make_loader(tmp_path)
        with pytest.raises(ConfigurationError, match="YAML parsing failed"):
            loader.get_environment()

    def test_defaults_populated_for_optional_sections(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "test.yml", _env_data())
        loader = _make_loader(tmp_path)
        cfg = loader.get_environment()
        assert cfg.logging.level == "INFO"
        assert cfg.ai.enabled is False
        assert cfg.spark.shuffle_partitions == 200


# ---------------------------------------------------------------------------
# SourceConfig loading
# ---------------------------------------------------------------------------


class TestGetSource:
    def test_loads_valid_source(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "test.yml", _env_data())
        _write(tmp_path / "sources" / "customers.yml", _source_data())
        loader = _make_loader(tmp_path)
        cfg = loader.get_source("customers")
        assert isinstance(cfg, SourceConfig)
        assert cfg.name == "customers"
        assert cfg.connector.type == "csv"

    def test_raises_when_source_file_missing(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "test.yml", _env_data())
        loader = _make_loader(tmp_path)
        with pytest.raises(ConfigurationError, match="not found"):
            loader.get_source("nonexistent")

    def test_raises_on_unsupported_connector_type(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "test.yml", _env_data())
        _write(
            tmp_path / "sources" / "customers.yml",
            _source_data(connector={"type": "ftp", "location": "/data"}),
        )
        loader = _make_loader(tmp_path)
        with pytest.raises(ConfigurationError):
            loader.get_source("customers")

    def test_full_table_name_property(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "test.yml", _env_data())
        _write(tmp_path / "sources" / "customers.yml", _source_data())
        loader = _make_loader(tmp_path)
        cfg = loader.get_source("customers")
        assert cfg.target.full_table_name == "dg_test.bronze.erp_customers"


# ---------------------------------------------------------------------------
# Token substitution
# ---------------------------------------------------------------------------


class TestTokenSubstitution:
    def test_env_token_replaced(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "test.yml", _env_data())
        _write(
            tmp_path / "sources" / "customers.yml",
            _source_data(
                target={
                    "catalog": "dg_{env}",
                    "schema": "bronze",
                    "table": "erp_customers",
                }
            ),
        )
        loader = _make_loader(tmp_path)
        cfg = loader.get_source("customers")
        assert cfg.target.catalog == "dg_test"

    def test_unknown_token_left_in_place(self, tmp_path: Path) -> None:
        _write(tmp_path / "environments" / "test.yml", _env_data())
        _write(
            tmp_path / "sources" / "customers.yml",
            _source_data(description="Source for {unknown_token} system"),
        )
        loader = _make_loader(tmp_path)
        cfg = loader.get_source("customers")
        assert "{unknown_token}" in cfg.description
