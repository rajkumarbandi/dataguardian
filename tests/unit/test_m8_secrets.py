"""
Unit tests for SecretsManager (Milestone 8).

All tests are isolated — no Databricks runtime, no network calls.
dbutils is replaced with a MagicMock; env-var fallback is tested via os.environ.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from src.common.exceptions import ConfigurationError
from src.common.secrets import SecretsManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dbutils(values: dict[str, str] | None = None) -> MagicMock:
    """Build a mock dbutils whose secrets.get() resolves from *values*."""
    mock = MagicMock()
    resolved = values or {}

    def _get(scope: str, key: str) -> str:  # noqa: ARG001
        if key in resolved:
            return resolved[key]
        raise Exception(f"Secret '{key}' not found")

    mock.secrets.get.side_effect = _get
    return mock


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestSecretsManagerConstruction:
    def test_scope_stored(self):
        sm = SecretsManager(scope="my-scope")
        assert sm.scope == "my-scope"

    def test_empty_scope_raises(self):
        with pytest.raises(ConfigurationError):
            SecretsManager(scope="")

    def test_blank_scope_raises(self):
        with pytest.raises(ConfigurationError):
            SecretsManager(scope="   ".strip())


# ---------------------------------------------------------------------------
# dbutils path
# ---------------------------------------------------------------------------


class TestSecretsManagerDbutils:
    def test_get_returns_value_from_dbutils(self):
        sm = SecretsManager(
            scope="test-scope",
            dbutils=_make_dbutils({"storage-account-key": "abc123"}),
        )
        assert sm.get("storage-account-key") == "abc123"

    def test_get_optional_returns_value_from_dbutils(self):
        sm = SecretsManager(
            scope="test-scope",
            dbutils=_make_dbutils({"api-key": "xyz"}),
        )
        assert sm.get_optional("api-key", "fallback") == "xyz"

    def test_get_raises_when_dbutils_missing_and_no_fallback(self):
        sm = SecretsManager(
            scope="test-scope",
            dbutils=_make_dbutils({}),
            allow_env_fallback=False,
        )
        with pytest.raises(ConfigurationError, match="storage-account-key"):
            sm.get("storage-account-key")

    def test_get_optional_returns_default_when_dbutils_missing(self):
        sm = SecretsManager(
            scope="test-scope",
            dbutils=_make_dbutils({}),
            allow_env_fallback=False,
        )
        assert sm.get_optional("missing-key", "default_val") == "default_val"

    def test_dbutils_called_with_correct_scope_and_key(self):
        dbutils = _make_dbutils({"my-key": "val"})
        sm = SecretsManager(scope="my-scope", dbutils=dbutils)
        sm.get("my-key")
        dbutils.secrets.get.assert_called_once_with(scope="my-scope", key="my-key")


# ---------------------------------------------------------------------------
# Environment-variable fallback
# ---------------------------------------------------------------------------


class TestSecretsManagerEnvFallback:
    def test_get_reads_env_var_when_dbutils_absent(self, monkeypatch):
        monkeypatch.setenv("DG_TEST_SCOPE_MY_KEY", "env_value")
        sm = SecretsManager(scope="test-scope")
        assert sm.get("my-key") == "env_value"

    def test_get_env_var_with_hyphens_normalised(self, monkeypatch):
        monkeypatch.setenv("DG_DATAGUARDIAN_DEV_STORAGE_ACCOUNT_KEY", "secret123")
        sm = SecretsManager(scope="dataguardian-dev")
        assert sm.get("storage-account-key") == "secret123"

    def test_get_raises_when_env_var_not_set(self):
        sm = SecretsManager(scope="test-scope")
        key = "definitely-not-set-key-xyz"
        env_key = f"DG_TEST_SCOPE_{key.upper().replace('-', '_')}"
        os.environ.pop(env_key, None)
        with pytest.raises(ConfigurationError, match=key):
            sm.get(key)

    def test_get_optional_returns_default_when_env_var_not_set(self):
        sm = SecretsManager(scope="test-scope")
        result = sm.get_optional("nonexistent-key-9999", "the-default")
        assert result == "the-default"

    def test_allow_env_fallback_false_skips_env_var(self, monkeypatch):
        monkeypatch.setenv("DG_TEST_SCOPE_MY_KEY", "env_value")
        sm = SecretsManager(scope="test-scope", allow_env_fallback=False)
        with pytest.raises(ConfigurationError):
            sm.get("my-key")

    def test_dbutils_takes_precedence_over_env_var(self, monkeypatch):
        monkeypatch.setenv("DG_TEST_SCOPE_MY_KEY", "env_value")
        sm = SecretsManager(
            scope="test-scope",
            dbutils=_make_dbutils({"my-key": "dbutils_value"}),
        )
        assert sm.get("my-key") == "dbutils_value"


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


class TestSecretsManagerCaching:
    def test_second_get_uses_cache(self):
        dbutils = _make_dbutils({"key": "val"})
        sm = SecretsManager(scope="s", dbutils=dbutils)
        sm.get("key")
        sm.get("key")
        assert dbutils.secrets.get.call_count == 1

    def test_get_optional_cached_after_first_resolve(self):
        dbutils = _make_dbutils({"key": "val"})
        sm = SecretsManager(scope="s", dbutils=dbutils)
        sm.get_optional("key", "default")
        sm.get_optional("key", "default")
        assert dbutils.secrets.get.call_count == 1

    def test_cache_not_populated_for_missing_key(self):
        sm = SecretsManager(scope="s", allow_env_fallback=False)
        sm.get_optional("missing", "default")
        sm.get_optional("missing", "default")
        # No error; default returned each time — nothing in cache for missing key
        assert "missing" not in sm._cache


# ---------------------------------------------------------------------------
# Structured credential helpers
# ---------------------------------------------------------------------------


class TestSecretsManagerHelpers:
    def test_get_storage_credentials(self):
        sm = SecretsManager(
            scope="s",
            dbutils=_make_dbutils({
                "storage-account-name": "mystorage",
                "storage-account-key": "mykey",
            }),
        )
        creds = sm.get_storage_credentials()
        assert creds["storage_account_name"] == "mystorage"
        assert creds["storage_account_key"] == "mykey"

    def test_get_service_principal(self):
        sm = SecretsManager(
            scope="s",
            dbutils=_make_dbutils({
                "sp-tenant-id": "tid",
                "sp-client-id": "cid",
                "sp-client-secret": "csecret",
            }),
        )
        creds = sm.get_service_principal()
        assert creds["tenant_id"] == "tid"
        assert creds["client_id"] == "cid"
        assert creds["client_secret"] == "csecret"

    def test_get_db_credentials_with_default_port(self):
        sm = SecretsManager(
            scope="s",
            dbutils=_make_dbutils({
                "pg-host": "db.example.com",
                "pg-database": "mydb",
                "pg-username": "user",
                "pg-password": "pass",
            }),
        )
        creds = sm.get_db_credentials("pg")
        assert creds["host"] == "db.example.com"
        assert creds["port"] == "5432"
        assert creds["database"] == "mydb"

    def test_get_db_credentials_custom_port(self):
        sm = SecretsManager(
            scope="s",
            dbutils=_make_dbutils({
                "pg-host": "h",
                "pg-port": "5433",
                "pg-database": "d",
                "pg-username": "u",
                "pg-password": "p",
            }),
        )
        creds = sm.get_db_credentials("pg")
        assert creds["port"] == "5433"

    def test_get_api_key(self):
        sm = SecretsManager(
            scope="s",
            dbutils=_make_dbutils({"openai-api-key": "sk-123"}),
        )
        assert sm.get_api_key("openai") == "sk-123"

    def test_storage_credentials_raises_when_key_missing(self):
        sm = SecretsManager(scope="s", dbutils=_make_dbutils({}), allow_env_fallback=False)
        with pytest.raises(ConfigurationError):
            sm.get_storage_credentials()


# ---------------------------------------------------------------------------
# Env-var name generation
# ---------------------------------------------------------------------------


class TestEnvVarNaming:
    def test_hyphens_converted_to_underscores(self):
        sm = SecretsManager(scope="my-scope")
        assert sm._env_var_name("my-key") == "DG_MY_SCOPE_MY_KEY"

    def test_dots_converted_to_underscores(self):
        sm = SecretsManager(scope="my.scope")
        assert sm._env_var_name("my.key") == "DG_MY_SCOPE_MY_KEY"

    def test_uppercased(self):
        sm = SecretsManager(scope="prod")
        assert sm._env_var_name("storage-account-key") == "DG_PROD_STORAGE_ACCOUNT_KEY"
