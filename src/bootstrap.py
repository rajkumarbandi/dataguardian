"""
PipelineBootstrap — Milestone 8.

Single entry point that initialises every DataGuardian component and returns
a ``PipelineContext`` dataclass.  Notebooks call exactly one function::

    context = PipelineBootstrap.initialize(
        env=env,
        spark=spark,
        dbutils=dbutils,
        notebook_name=_notebook_name,
        secrets_scope=secrets_scope_param or None,
    )

All engines, writers, registries, and audit components are wired up here.
The notebook (and ``src.pipeline.run_pipeline``) only consumes the context.

Architecture
------------
PipelineContext (frozen dataclass-style, mutable fields are set before return)
  ├── env / catalog / notebook_name        — identity
  ├── loader / env_config                  — configuration layer
  ├── spark / logger / secrets / uc_client — infrastructure
  ├── schema_registry / schema_validator   — M5 schema management
  ├── transformation_engine                — M6 transformation engine
  ├── dq_engine                            — M3 data quality engine
  ├── contract_engine                      — M7 contract validation
  ├── *_writer (×6)                        — audit and data writers
  └── tracker / retry                      — orchestration helpers
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from src.audit.contract_history_writer import ContractHistoryWriter
from src.audit.transformation_history_writer import TransformationHistoryWriter
from src.common.config_loader import ConfigLoader
from src.common.logger import DataGuardianLogger, get_logger
from src.common.models import EnvironmentConfig
from src.common.pipeline_run import PipelineRunTracker
from src.common.retry import RetryHelper
from src.common.secrets import SecretsManager
from src.common.spark_session import SparkSessionManager
from src.common.unity_catalog_client import UnityCatalogClient
from src.contracts import ContractValidationEngine
from src.quality.engine import DataQualityEngine
from src.quality.metrics import MetricsWriter
from src.quality.writers import DQResultsWriter
from src.schema import SchemaHistoryWriter, SchemaRegistry, SchemaValidator
from src.silver.silver_writer import SilverWriter
from src.transformations import TransformationEngine


# ---------------------------------------------------------------------------
# Pipeline context — holds every initialised component
# ---------------------------------------------------------------------------


@dataclass
class PipelineContext:
    """
    All initialised DataGuardian components for one pipeline execution session.

    Produced by ``PipelineBootstrap.initialize()`` and consumed by
    ``src.pipeline.run_pipeline()``.  Every field is populated before the
    dataclass is returned; no lazy initialisation occurs after construction.
    """

    # Identity
    env: str
    catalog: str
    notebook_name: str

    # Configuration
    loader: ConfigLoader
    env_config: EnvironmentConfig

    # Infrastructure
    spark: SparkSession
    logger: DataGuardianLogger
    uc_client: UnityCatalogClient
    secrets: SecretsManager | None

    # Schema management (M5)
    schema_registry: SchemaRegistry
    schema_validator: SchemaValidator
    schema_history_writer: SchemaHistoryWriter

    # Transformation engine (M6)
    transformation_engine: TransformationEngine
    transformation_history_writer: TransformationHistoryWriter

    # Data quality engine (M3)
    dq_engine: DataQualityEngine
    dq_writer: DQResultsWriter
    metrics_writer: MetricsWriter

    # Contract validation (M7)
    contract_engine: ContractValidationEngine
    contract_history_writer: ContractHistoryWriter

    # Silver writer (M2)
    silver_writer: SilverWriter

    # Orchestration
    tracker: PipelineRunTracker
    retry: RetryHelper


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


class PipelineBootstrap:
    """
    Factory for ``PipelineContext``.

    All initialisation that previously lived in the notebook (config loading,
    Spark session, engine wiring, Unity Catalog setup, logger init) is
    centralised here.  The notebook becomes three lines:
    ``bootstrap → run_pipeline → print_summary``.
    """

    @staticmethod
    def initialize(
        env: str,
        spark: SparkSession | None = None,
        dbutils: Any = None,
        notebook_name: str = "unknown",
        secrets_scope: str | None = None,
    ) -> PipelineContext:
        """
        Initialise all pipeline components for *env*.

        Parameters
        ----------
        env:
            Environment name — ``dev``, ``test``, ``qa``, or ``prod``.
            Must match a file in ``config/environments/``.
        spark:
            Active ``SparkSession``.  When ``None`` (local dev / tests),
            ``SparkSessionManager`` creates a local session automatically.
        dbutils:
            Databricks ``dbutils`` object (injected by the runtime).
            ``None`` in local dev — secrets fall back to environment variables.
        notebook_name:
            Notebook path recorded in audit tables.
        secrets_scope:
            Databricks secret scope name.  When provided, a ``SecretsManager``
            is created and available as ``context.secrets``.

        Returns
        -------
        PipelineContext
            Fully wired context ready for ``run_pipeline()``.
        """
        os.environ["DATAGUARDIAN_ENV"] = env

        # 1. Configuration ---------------------------------------------------
        loader = ConfigLoader(env=env)
        env_config = loader.get_environment()
        catalog = env_config.unity_catalog.catalog

        # 2. Logger (before anything else so failures are captured) ----------
        logger: DataGuardianLogger = get_logger(
            "dataguardian.bootstrap", env=env
        )
        logger.info(
            "Pipeline bootstrap starting",
            env=env,
            catalog=catalog,
            notebook_name=notebook_name,
            secrets_scope=secrets_scope or "none",
        )

        # 3. Secrets (optional) ----------------------------------------------
        secrets: SecretsManager | None = None
        if secrets_scope:
            secrets = SecretsManager(
                scope=secrets_scope,
                dbutils=dbutils,
                allow_env_fallback=True,
            )
            logger.info("SecretsManager initialised", scope=secrets_scope)

        # 4. Spark session ---------------------------------------------------
        if spark is None:
            manager = SparkSessionManager(env_config=env_config)
            spark = manager.get_session()
        logger.info("SparkSession ready", spark_version=spark.version)

        # 5. Unity Catalog — ensure schemas exist ----------------------------
        uc_client = UnityCatalogClient(spark=spark, catalog=catalog)
        uc_client.use_catalog()
        uc_client.create_schema_if_not_exists("audit")
        uc_client.create_schema_if_not_exists("silver")
        logger.info(
            "Unity Catalog schemas verified",
            catalog=catalog,
            schemas=["audit", "silver"],
        )

        # 6. Schema management (M5) ------------------------------------------
        schema_registry = SchemaRegistry(
            spark=spark,
            catalog=catalog,
            enabled=env_config.schema_registry.schema_registry_enabled,
        )
        schema_validator = SchemaValidator(registry=schema_registry)
        schema_history_writer = SchemaHistoryWriter(catalog=catalog)

        # 7. Transformation engine (M6) --------------------------------------
        transformation_engine = TransformationEngine(spark=spark)
        transformation_history_writer = TransformationHistoryWriter(
            catalog=catalog,
            enabled=env_config.transformation.audit_enabled,
        )

        # 8. Data quality engine (M3) ----------------------------------------
        dq_engine = DataQualityEngine(spark=spark, catalog=catalog)
        dq_writer = DQResultsWriter(catalog=catalog)
        metrics_writer = MetricsWriter(spark=spark, catalog=catalog)

        # 9. Contract validation (M7) ----------------------------------------
        contract_engine = ContractValidationEngine()
        contract_history_writer = ContractHistoryWriter(
            catalog=catalog,
            enabled=env_config.contract_validation.contract_audit_enabled,
        )

        # 10. Silver writer (M2) --------------------------------------------
        silver_writer = SilverWriter(catalog=catalog)

        # 11. Orchestration --------------------------------------------------
        tracker = PipelineRunTracker(
            spark=spark,
            env_config=env_config,
            notebook_name=notebook_name,
        )
        retry = RetryHelper(policy=env_config.pipeline.retry_policy)

        logger.info(
            "Pipeline bootstrap complete",
            catalog=catalog,
            pipeline_name=env_config.pipeline.pipeline_name,
            pipeline_version=env_config.pipeline.pipeline_version,
            schema_registry_enabled=env_config.schema_registry.schema_registry_enabled,
            transformation_audit_enabled=env_config.transformation.audit_enabled,
            contract_validation_enabled=env_config.contract_validation.contract_validation_enabled,
            retry_max_attempts=env_config.pipeline.retry_policy.max_attempts,
        )

        return PipelineContext(
            env=env,
            catalog=catalog,
            notebook_name=notebook_name,
            loader=loader,
            env_config=env_config,
            spark=spark,
            logger=logger,
            uc_client=uc_client,
            secrets=secrets,
            schema_registry=schema_registry,
            schema_validator=schema_validator,
            schema_history_writer=schema_history_writer,
            transformation_engine=transformation_engine,
            transformation_history_writer=transformation_history_writer,
            dq_engine=dq_engine,
            dq_writer=dq_writer,
            metrics_writer=metrics_writer,
            contract_engine=contract_engine,
            contract_history_writer=contract_history_writer,
            silver_writer=silver_writer,
            tracker=tracker,
            retry=retry,
        )
