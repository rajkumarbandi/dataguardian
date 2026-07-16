"""
Pydantic data models for all DataGuardian configuration structures.

Every configuration file parsed from YAML flows through one of these models.
Pydantic v2 provides field validation, clear error messages, and IDE-friendly
typed attributes — replacing raw ``dict[str, Any]`` throughout the codebase.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Source configuration models
# ---------------------------------------------------------------------------


class ColumnDefinition(BaseModel):
    """Defines a single column in an explicit source schema declaration."""

    name: str
    type: str
    nullable: bool = True
    description: str = ""


class ConnectorConfig(BaseModel):
    """Connection parameters for a source connector."""

    type: str
    location: str
    options: dict[str, str] = Field(default_factory=dict)

    @field_validator("type")
    @classmethod
    def type_must_be_registered(cls, v: str) -> str:
        supported = {"csv", "adls", "jdbc", "api", "sftp", "kafka"}
        if v not in supported:
            raise ValueError(
                f"Connector type {v!r} is not supported. "
                f"Supported types: {sorted(supported)}"
            )
        return v


class TargetConfig(BaseModel):
    """Defines the Bronze Delta table target for an ingestion source."""

    catalog: str
    schema: str
    table: str
    load_type: str = "append"
    partition_by: str = "load_date"

    @field_validator("load_type")
    @classmethod
    def validate_load_type(cls, v: str) -> str:
        allowed = {"append", "overwrite"}
        if v not in allowed:
            raise ValueError(
                f"load_type must be one of {allowed}, got {v!r}"
            )
        return v

    @property
    def full_table_name(self) -> str:
        """Fully qualified table name: catalog.schema.table."""
        return f"{self.catalog}.{self.schema}.{self.table}"


class SourceMetadata(BaseModel):
    """Descriptive metadata attached to a source configuration."""

    owner: str = ""
    data_classification: str = "internal"
    tags: dict[str, str] = Field(default_factory=dict)


class SchemaEvolutionConfig(BaseModel):
    """
    Per-source schema evolution configuration.

    When ``evolution_mode`` is set it overrides the environment-level default.
    Leave ``None`` to inherit ``EnvironmentConfig.schema_registry.default_evolution_mode``.

    YAML::

        schema_evolution:
          evolution_mode: ALLOW_NEW_COLUMNS   # overrides env default
          allow_nullable_changes: false
          allow_type_promotion: false
    """

    evolution_mode: str | None = None
    allow_nullable_changes: bool = False
    allow_type_promotion: bool = False

    @field_validator("evolution_mode")
    @classmethod
    def validate_mode(cls, v: str | None) -> str | None:
        if v is None:
            return v
        allowed = {"STRICT", "ALLOW_NEW_COLUMNS", "AUTO_EVOLVE"}
        if v not in allowed:
            raise ValueError(f"evolution_mode must be one of {allowed}, got {v!r}")
        return v


class TransformationPolicyConfig(BaseModel):
    """
    Pipeline-level error handling policy for the transformation engine.

    The ``on_error`` value applies to all transformation steps unless a step
    provides its own ``on_error`` override.

    YAML::

        transformation_policy:
          on_error: fail_fast   # fail_fast | continue | skip
    """

    on_error: str = "fail_fast"

    @field_validator("on_error")
    @classmethod
    def validate_on_error(cls, v: str) -> str:
        allowed = {"fail_fast", "continue", "skip"}
        if v not in allowed:
            raise ValueError(f"on_error must be one of {allowed}, got {v!r}")
        return v


class TransformationConfig(BaseModel):
    """
    Configuration for a single transformation step within a source YAML.

    Steps are applied in declaration order by the ``TransformationEngine``.
    The ``type`` field is looked up in ``TransformationRegistry`` to find the
    implementation class.

    Example YAML::

        transformations:
          - type: trim_strings
            params:
              columns: [first_name, last_name, email]
          - type: upper_case
            params:
              columns: [country_code]
          - type: null_replacement
            params:
              replacements:
                customer_segment: Unknown
    """

    type: str
    params: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    on_error: str | None = None
    description: str = ""

    @field_validator("on_error")
    @classmethod
    def validate_on_error(cls, v: str | None) -> str | None:
        if v is None:
            return v
        allowed = {"fail_fast", "continue", "skip"}
        if v not in allowed:
            raise ValueError(f"on_error must be one of {allowed}, got {v!r}")
        return v


class TransformationEnvConfig(BaseModel):
    """
    Environment-level configuration for the transformation subsystem.

    YAML::

        transformation:
          audit_enabled: true
    """

    audit_enabled: bool = True


class ContractRowCountConfig(BaseModel):
    """
    Expected row count bounds for a data contract.

    Both bounds are optional.  Only the configured bounds are evaluated:

    YAML::

        row_count:
          min: 1          # at least one row must be present
          max: 10000000   # warn when exceeding ten million rows
    """

    min: int | None = None
    max: int | None = None


class ContractConfig(BaseModel):
    """
    Formal data contract for a source entity.

    Declared in ``config/sources/{source}.yml`` under the ``contract:`` key.
    The ``ContractValidationEngine`` evaluates these rules after the DQ engine.

    YAML::

        contract:
          name: customers_data_contract
          version: "1.0.0"
          owner: data-engineering@company.com
          domain: customer
          description: "Formal contract for the customer master entity"
          criticality: critical         # critical | high | medium | low
          expected_refresh: daily       # hourly | daily | weekly | monthly
          validation_policy: FAIL_PIPELINE   # FAIL_PIPELINE | WARNING_ONLY | IGNORE
          required_columns:
            - customer_id
            - email
          primary_keys:
            - customer_id
          non_nullable_columns:
            - customer_id
          allowed_datatypes:
            customer_id: string
            is_active: boolean
          row_count:
            min: 1
            max: 10000000
          required_dq_rules:
            - not_null
            - unique
            - email
          schema_version_min: 1
    """

    name: str
    version: str = "1.0.0"
    owner: str = ""
    domain: str = ""
    description: str = ""
    criticality: str = "medium"
    expected_refresh: str = "daily"
    validation_policy: str | None = None

    required_columns: list[str] = Field(default_factory=list)
    primary_keys: list[str] = Field(default_factory=list)
    non_nullable_columns: list[str] = Field(default_factory=list)
    allowed_datatypes: dict[str, str] = Field(default_factory=dict)
    row_count: ContractRowCountConfig = Field(default_factory=ContractRowCountConfig)
    required_dq_rules: list[str] = Field(default_factory=list)
    schema_version_min: int | None = None

    @field_validator("criticality")
    @classmethod
    def validate_criticality(cls, v: str) -> str:
        allowed = {"critical", "high", "medium", "low"}
        if v.lower() not in allowed:
            raise ValueError(f"criticality must be one of {allowed}, got {v!r}")
        return v.lower()

    @field_validator("validation_policy")
    @classmethod
    def validate_policy(cls, v: str | None) -> str | None:
        if v is None:
            return v
        allowed = {"FAIL_PIPELINE", "WARNING_ONLY", "IGNORE"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(f"validation_policy must be one of {allowed}, got {v!r}")
        return upper


class DQRuleConfig(BaseModel):
    """
    Configuration for a single Data Quality rule declaration within a source YAML.

    Each rule is applied by the ``DataQualityEngine`` in sequence.  The ``rule``
    field is looked up in the ``RuleRegistry`` to find the implementation class.

    Example YAML::

        dq_rules:
          - rule: not_null
            column: customer_id
            severity: error
          - rule: allowed_values
            column: status
            severity: error
            params:
              values: [PENDING, CONFIRMED, DELIVERED]
    """

    rule: str
    column: str
    params: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    severity: str = "error"

    @field_validator("severity")
    @classmethod
    def validate_severity(cls, v: str) -> str:
        allowed = {"error", "warning"}
        if v not in allowed:
            raise ValueError(f"severity must be one of {allowed}, got {v!r}")
        return v


class SourceConfig(BaseModel):
    """
    Full configuration for a single ingestion source.

    Parsed from ``config/sources/{source_name}.yml``.
    """

    name: str
    system: str
    description: str = ""
    connector: ConnectorConfig
    schema: list[ColumnDefinition] = Field(default_factory=list)
    target: TargetConfig
    metadata: SourceMetadata = Field(default_factory=SourceMetadata)
    dq_rules: list[DQRuleConfig] = Field(default_factory=list)
    schema_evolution: SchemaEvolutionConfig = Field(default_factory=SchemaEvolutionConfig)
    transformation_policy: TransformationPolicyConfig = Field(default_factory=TransformationPolicyConfig)
    transformations: list[TransformationConfig] = Field(default_factory=list)
    contract: ContractConfig | None = None


# ---------------------------------------------------------------------------
# Environment configuration models
# ---------------------------------------------------------------------------


class UnityCatalogConfig(BaseModel):
    """Unity Catalog identifiers for a given environment."""

    catalog: str
    schemas: dict[str, str] = Field(default_factory=dict)


class StorageConfig(BaseModel):
    """Azure Data Lake Gen2 storage configuration."""

    adls_account: str = ""
    adls_container: str = ""
    adls_root: str = ""


class IngestionPipelineConfig(BaseModel):
    """Global ingestion pipeline tuning parameters."""

    default_partition_column: str = "ingestion_date"
    batch_size_rows: int = 100_000
    retry_attempts: int = 3
    retry_delay_seconds: int = 30


class AIConfig(BaseModel):
    """AI enrichment feature flags."""

    enabled: bool = False
    provider: str = "azure_openai"


class LoggingConfig(BaseModel):
    """Logging configuration for a given environment."""

    level: str = "INFO"
    format: str = "json"

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(
                f"Log level must be one of {allowed}, got {v!r}"
            )
        return upper


class SparkConfig(BaseModel):
    """Spark session configuration overrides per environment."""

    shuffle_partitions: int = 200
    adaptive_query_execution: bool = True
    broadcast_threshold_mb: int = 10


# ---------------------------------------------------------------------------
# Milestone 4 — Pipeline and retry configuration models
# ---------------------------------------------------------------------------


class RetryPolicyConfig(BaseModel):
    """
    Exponential backoff retry policy for transient failures.

    Delay formula:
        ``min(initial_delay × backoff_multiplier^(attempt-1), max_delay)``

    Example (defaults): delays are 1s, 2s, 4s before the 4th and final attempt.

    YAML::

        retry_policy:
          max_attempts: 3
          initial_delay_seconds: 1.0
          backoff_multiplier: 2.0
          max_delay_seconds: 60.0
    """

    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 60.0

    @field_validator("max_attempts")
    @classmethod
    def validate_max_attempts(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"max_attempts must be >= 1, got {v}")
        return v

    @field_validator("backoff_multiplier")
    @classmethod
    def validate_multiplier(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError(f"backoff_multiplier must be >= 1.0, got {v}")
        return v


class PipelineConfig(BaseModel):
    """
    Pipeline-level configuration for auditing, versioning, and retry behaviour.

    All fields have production-safe defaults so existing environment YAMLs that
    do not yet include a ``pipeline:`` section continue to work unchanged.

    YAML::

        pipeline:
          pipeline_name: dataguardian-erp
          pipeline_version: "1.0.0"
          audit_enabled: true
          retry_policy:
            max_attempts: 3
            initial_delay_seconds: 1.0
            backoff_multiplier: 2.0
            max_delay_seconds: 60.0
    """

    pipeline_name: str = "dataguardian"
    pipeline_version: str = "1.0.0"
    audit_enabled: bool = True
    retry_policy: RetryPolicyConfig = Field(default_factory=RetryPolicyConfig)


class SchemaRegistryConfig(BaseModel):
    """
    Schema registry configuration for a given environment.

    Controls whether the Delta-backed schema registry is active and what the
    default evolution mode is for all sources in this environment.  Per-source
    overrides are declared in the source YAML ``schema_evolution:`` section.

    YAML::

        schema_registry:
          schema_registry_enabled: true
          schema_audit_enabled: true
          default_evolution_mode: STRICT
    """

    schema_registry_enabled: bool = True
    schema_audit_enabled: bool = True
    default_evolution_mode: str = "STRICT"

    @field_validator("default_evolution_mode")
    @classmethod
    def validate_evolution_mode(cls, v: str) -> str:
        allowed = {"STRICT", "ALLOW_NEW_COLUMNS", "AUTO_EVOLVE"}
        if v not in allowed:
            raise ValueError(f"default_evolution_mode must be one of {allowed}, got {v!r}")
        return v


class ContractEnvConfig(BaseModel):
    """
    Environment-level configuration for the contract validation subsystem.

    YAML::

        contract_validation:
          contract_validation_enabled: true
          contract_audit_enabled: true
          default_contract_policy: FAIL_PIPELINE
    """

    contract_validation_enabled: bool = True
    contract_audit_enabled: bool = True
    default_contract_policy: str = "FAIL_PIPELINE"

    @field_validator("default_contract_policy")
    @classmethod
    def validate_policy(cls, v: str) -> str:
        allowed = {"FAIL_PIPELINE", "WARNING_ONLY", "IGNORE"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(f"default_contract_policy must be one of {allowed}, got {v!r}")
        return upper


class EnvironmentConfig(BaseModel):
    """
    Top-level environment configuration.

    Parsed from ``config/environments/{env}.yml``.
    """

    environment: str
    unity_catalog: UnityCatalogConfig
    storage: StorageConfig = Field(default_factory=StorageConfig)
    ingestion: IngestionPipelineConfig = Field(default_factory=IngestionPipelineConfig)
    ai: AIConfig = Field(default_factory=AIConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    spark: SparkConfig = Field(default_factory=SparkConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    schema_registry: SchemaRegistryConfig = Field(default_factory=SchemaRegistryConfig)
    transformation: TransformationEnvConfig = Field(default_factory=TransformationEnvConfig)
    contract_validation: ContractEnvConfig = Field(default_factory=ContractEnvConfig)

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("environment must not be empty")
        return v.strip()
