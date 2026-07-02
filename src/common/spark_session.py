"""
Spark session manager for the DataGuardian platform.

``SparkSessionManager`` enforces a singleton SparkSession per Python process.
On Databricks, the session already exists and is simply returned.
Outside Databricks (local tests), a new session is built with Delta Lake
support and the configuration supplied by ``EnvironmentConfig``.

Why a manager class instead of a bare function?
------------------------------------------------
The class allows ``reset()`` (test-only) to tear down the session between
test cases without affecting production code paths.  It also centralises all
Spark configuration so no pipeline component ever calls
``SparkSession.builder`` directly.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from src.common.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.common.models import EnvironmentConfig


# ---------------------------------------------------------------------------
# Runtime detection
# ---------------------------------------------------------------------------


def _is_databricks_runtime() -> bool:
    """
    Return ``True`` when running inside a Databricks cluster.

    Databricks sets ``DATABRICKS_RUNTIME_VERSION`` on all cluster nodes.
    This variable is absent in local pytest runs.
    """
    return bool(os.getenv("DATABRICKS_RUNTIME_VERSION"))


# ---------------------------------------------------------------------------
# SparkSessionManager
# ---------------------------------------------------------------------------


class SparkSessionManager:
    """
    Creates or retrieves the singleton ``SparkSession`` for this process.

    Parameters
    ----------
    env_config:
        The validated environment configuration produced by ``ConfigLoader``.
        Used to apply Spark tunables and set the active Unity Catalog catalog.

    Example
    -------
    ::

        manager = SparkSessionManager(env_config=loader.get_environment())
        spark = manager.get_session()
    """

    _instance: SparkSession | None = None

    def __init__(self, env_config: EnvironmentConfig) -> None:
        self._env_config = env_config

    def get_session(self) -> SparkSession:
        """
        Return the active ``SparkSession``.

        On Databricks: retrieves the pre-existing session via
        ``SparkSession.getActiveSession()``.
        Locally: builds a new Delta-enabled session if none exists.
        """
        if SparkSessionManager._instance is not None:
            return SparkSessionManager._instance

        if _is_databricks_runtime():
            session = self._get_databricks_session()
        else:
            session = self._create_local_session()

        self._apply_config(session)
        SparkSessionManager._instance = session
        return session

    def _get_databricks_session(self) -> SparkSession:
        try:
            from pyspark.sql import SparkSession  # noqa: PLC0415
        except ImportError as exc:
            raise ConfigurationError(
                "pyspark is not installed. "
                "Install it with: pip install pyspark"
            ) from exc

        session = SparkSession.getActiveSession()
        if session is None:
            raise ConfigurationError(
                "No active SparkSession found on this Databricks cluster. "
                "Ensure the notebook is attached to a running cluster."
            )
        return session

    def _create_local_session(self) -> SparkSession:
        try:
            from delta import configure_spark_with_delta_pip  # noqa: PLC0415
            from pyspark.sql import SparkSession  # noqa: PLC0415
        except ImportError as exc:
            raise ConfigurationError(
                "pyspark and delta-spark are required for local sessions. "
                "Install them with: pip install pyspark delta-spark"
            ) from exc

        catalog = self._env_config.unity_catalog.catalog
        builder = (
            SparkSession.builder.appName(f"dataguardian-{catalog}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    def _apply_config(self, session: SparkSession) -> None:
        """Push environment-specific tunables into the active session."""
        spark_cfg = self._env_config.spark
        conf_pairs = [
            (
                "spark.sql.shuffle.partitions",
                str(spark_cfg.shuffle_partitions),
            ),
            (
                "spark.sql.adaptive.enabled",
                str(spark_cfg.adaptive_query_execution).lower(),
            ),
            (
                "spark.sql.autoBroadcastJoinThreshold",
                str(spark_cfg.broadcast_threshold_mb * 1024 * 1024),
            ),
        ]
        for key, value in conf_pairs:
            session.conf.set(key, value)

    @classmethod
    def reset(cls) -> None:
        """
        Destroy the singleton and stop the underlying session.

        **For use in tests only.**  Allows each test to start with a clean
        session without interference from other tests.
        """
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
