"""
SecretsManager — Milestone 8.

Retrieves secrets from Databricks Secret Scopes with an environment-variable
fallback for local development.  No secret value is ever stored in source code
or YAML; the caller always asks by key name.

Usage in a notebook (Databricks runtime):
    from src.common.secrets import SecretsManager

    secrets = SecretsManager(scope="dataguardian-prod", dbutils=dbutils)
    storage_key = secrets.get("storage-account-key")

Usage in a local test (env-var fallback):
    # export DG_DATAGUARDIAN_DEV_STORAGE_ACCOUNT_KEY=mykey
    secrets = SecretsManager(scope="dataguardian-dev")
    storage_key = secrets.get("storage-account-key")   # reads env var

Secret naming convention
------------------------
Env-var fallback key format:
    DG_{SCOPE}_{KEY}
    (scope and key uppercased; hyphens and dots replaced with underscores)

Example: scope="dataguardian-dev", key="storage-account-key"
    → DG_DATAGUARDIAN_DEV_STORAGE_ACCOUNT_KEY
"""

from __future__ import annotations

import os
from typing import Any

from src.common.exceptions import ConfigurationError


class SecretsManager:
    """
    Retrieve secrets from Databricks Secret Scopes.

    Falls back to environment variables when ``dbutils`` is absent (local dev/CI).
    Caches retrieved values for the lifetime of the instance so that repeated
    lookups for the same key do not incur additional API round-trips.
    """

    def __init__(
        self,
        scope: str,
        dbutils: Any = None,
        allow_env_fallback: bool = True,
    ) -> None:
        if not scope:
            raise ConfigurationError("SecretsManager requires a non-empty scope name.")
        self._scope = scope
        self._dbutils = dbutils
        self._allow_env_fallback = allow_env_fallback
        self._cache: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def scope(self) -> str:
        """The Databricks secret scope this manager is bound to."""
        return self._scope

    def get(self, key: str) -> str:
        """
        Retrieve a required secret.

        Raises:
            ConfigurationError: if the secret is not found in either
                the Databricks scope or the environment-variable fallback.
        """
        value = self._resolve(key)
        if value is None:
            raise ConfigurationError(
                f"Secret '{key}' not found in scope '{self._scope}'. "
                "Ensure the secret exists in Databricks Secrets or set the "
                f"environment variable '{self._env_var_name(key)}'."
            )
        return value

    def get_optional(self, key: str, default: str = "") -> str:
        """
        Retrieve a secret, returning *default* if not found.

        Does not raise even when the secret is absent.
        """
        value = self._resolve(key)
        return value if value is not None else default

    # ------------------------------------------------------------------
    # Structured credential helpers
    # ------------------------------------------------------------------

    def get_storage_credentials(self) -> dict[str, str]:
        """
        Return ADLS Gen2 storage credentials.

        Expected secrets in scope:
            ``storage-account-name``
            ``storage-account-key``
        """
        return {
            "storage_account_name": self.get("storage-account-name"),
            "storage_account_key": self.get("storage-account-key"),
        }

    def get_service_principal(self) -> dict[str, str]:
        """
        Return Azure Service Principal credentials.

        Expected secrets in scope:
            ``sp-tenant-id``
            ``sp-client-id``
            ``sp-client-secret``
        """
        return {
            "tenant_id": self.get("sp-tenant-id"),
            "client_id": self.get("sp-client-id"),
            "client_secret": self.get("sp-client-secret"),
        }

    def get_db_credentials(self, prefix: str, default_port: str = "5432") -> dict[str, str]:
        """
        Return database credentials for the given *prefix*.

        Expected secrets:
            ``{prefix}-host``
            ``{prefix}-port``   (optional, defaults to *default_port*)
            ``{prefix}-database``
            ``{prefix}-username``
            ``{prefix}-password``
        """
        return {
            "host": self.get(f"{prefix}-host"),
            "port": self.get_optional(f"{prefix}-port", default_port),
            "database": self.get(f"{prefix}-database"),
            "username": self.get(f"{prefix}-username"),
            "password": self.get(f"{prefix}-password"),
        }

    def get_api_key(self, service_name: str) -> str:
        """
        Return the API key for *service_name*.

        Expected secret: ``{service_name}-api-key``
        """
        return self.get(f"{service_name}-api-key")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _resolve(self, key: str) -> str | None:
        """Try cache → dbutils → env var.  Returns None if all fail."""
        if key in self._cache:
            return self._cache[key]

        value = self._get_from_dbutils(key)
        if value is None and self._allow_env_fallback:
            value = self._get_from_env(key)

        if value is not None:
            self._cache[key] = value
        return value

    def _get_from_dbutils(self, key: str) -> str | None:
        if self._dbutils is None:
            return None
        try:
            return self._dbutils.secrets.get(scope=self._scope, key=key)
        except Exception:  # noqa: BLE001
            return None

    def _get_from_env(self, key: str) -> str | None:
        return os.environ.get(self._env_var_name(key))

    def _env_var_name(self, key: str) -> str:
        """Convert scope + key to an environment variable name."""
        normalised_scope = self._scope.upper().replace("-", "_").replace(".", "_")
        normalised_key = key.upper().replace("-", "_").replace(".", "_")
        return f"DG_{normalised_scope}_{normalised_key}"
