"""
Configuration loader for the DataGuardian platform.

``ConfigLoader`` is the single entry point for reading YAML files from
``config/``.  It:

1. Resolves the correct file paths for environments, sources, schemas, and
   quality suites.
2. Performs token substitution on YAML *values* (``{env}``, ``{catalog}``,
   ``{today}``, ``{adls_root}``) before parsing.
3. Validates the parsed dict against the appropriate Pydantic model, raising
   ``ConfigurationError`` with a clear message on failure.

Token substitution keeps YAML files DRY — the Bronze table target, for
example, can be written as ``{catalog}.bronze.{entity}`` rather than repeating
the full catalog name in every source file.
"""

from __future__ import annotations

import os
import re
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from src.common.exceptions import ConfigurationError
from src.common.models import EnvironmentConfig, SourceConfig


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Tokens that can appear inside YAML string values
_TOKEN_PATTERN = re.compile(r"\{(\w+)\}")

# Default config root — overridable via DATAGUARDIAN_CONFIG_DIR env var
_DEFAULT_CONFIG_ROOT = Path(__file__).parent.parent.parent / "config"


def _config_root() -> Path:
    custom = os.getenv("DATAGUARDIAN_CONFIG_DIR")
    return Path(custom) if custom else _DEFAULT_CONFIG_ROOT


# ---------------------------------------------------------------------------
# ConfigLoader
# ---------------------------------------------------------------------------


class ConfigLoader:
    """
    Loads and validates YAML configuration files for a target environment.

    Parameters
    ----------
    env:
        The deployment environment: ``dev``, ``qa``, or ``prod``.  Defaults to
        the ``DATAGUARDIAN_ENV`` environment variable, falling back to ``dev``.

    Example
    -------
    ::

        loader = ConfigLoader(env="dev")
        env_cfg = loader.get_environment()
        src_cfg = loader.get_source("customers")
    """

    def __init__(self, env: str | None = None) -> None:
        self._env: str = env or os.getenv("DATAGUARDIAN_ENV", "dev")
        self._config_root: Path = _config_root()
        # Lazy-loaded so we only read the environment file once
        self._env_config: EnvironmentConfig | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_environment(self) -> EnvironmentConfig:
        """
        Load and return the environment configuration.

        The result is cached — subsequent calls return the same object.
        """
        if self._env_config is None:
            self._env_config = self._load_environment()
        return self._env_config

    def get_source(self, source_name: str) -> SourceConfig:
        """
        Load the source configuration for ``source_name``.

        Parameters
        ----------
        source_name:
            The stem of the YAML file inside ``config/sources/``
            (e.g. ``"customers"`` for ``customers.yml``).
        """
        path = self._config_root / "sources" / f"{source_name}.yml"
        env_cfg = self.get_environment()
        substitutions = self._build_substitutions(env_cfg)
        raw = self._load_yaml(path, substitutions)
        return self._parse(raw, SourceConfig, path)

    # ------------------------------------------------------------------
    # Private — loading & validation
    # ------------------------------------------------------------------

    def _load_environment(self) -> EnvironmentConfig:
        path = self._config_root / "environments" / f"{self._env}.yml"
        # Minimal substitutions available before env config is loaded
        substitutions = {"env": self._env}
        raw = self._load_yaml(path, substitutions)
        return self._parse(raw, EnvironmentConfig, path)

    def _load_yaml(self, path: Path, substitutions: dict[str, str]) -> dict[str, Any]:
        if not path.exists():
            raise ConfigurationError(
                f"Configuration file not found: {path}. "
                f"Ensure 'config/' is present relative to the project root."
            )
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigurationError(
                f"Unable to read configuration file {path}: {exc}"
            ) from exc

        resolved = self._resolve_placeholders(text, substitutions)

        try:
            data = yaml.safe_load(resolved)
        except yaml.YAMLError as exc:
            raise ConfigurationError(
                f"YAML parsing failed for {path}: {exc}"
            ) from exc

        if not isinstance(data, dict):
            raise ConfigurationError(
                f"Expected a YAML mapping at the top level of {path}, "
                f"got {type(data).__name__}"
            )
        return data  # type: ignore[return-value]

    def _resolve_placeholders(self, text: str, substitutions: dict[str, str]) -> str:
        """Replace ``{token}`` occurrences with their resolved values."""

        def _replace(match: re.Match[str]) -> str:
            token = match.group(1)
            if token in substitutions:
                return substitutions[token]
            # Leave unknown tokens in place — Pydantic validation will catch
            # structural issues; unknown tokens are often intentional literals
            return match.group(0)

        return _TOKEN_PATTERN.sub(_replace, text)

    def _build_substitutions(self, env_cfg: EnvironmentConfig) -> dict[str, str]:
        """Build the full token → value map for a loaded environment config."""
        return {
            "env": self._env,
            "catalog": env_cfg.unity_catalog.catalog,
            "adls_root": env_cfg.storage.adls_root,
            "today": date.today().isoformat(),
        }

    @staticmethod
    def _parse(raw: dict[str, Any], model: type, path: Path) -> Any:
        """Validate ``raw`` against ``model``, raising ``ConfigurationError`` on failure."""
        try:
            return model(**raw)
        except Exception as exc:
            raise ConfigurationError(
                f"Configuration validation failed for {path}.\n"
                f"Model: {model.__name__}\n"
                f"Error: {exc}"
            ) from exc
