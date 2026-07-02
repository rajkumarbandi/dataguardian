"""
Delta Lake DDL for DataGuardian stewardship tables.

Run once per environment to initialize the stewardship schema.
Tables are created with IF NOT EXISTS guards — safe to re-run.
"""

from __future__ import annotations

_SCHEMA_DDL = """CREATE SCHEMA IF NOT EXISTS {catalog}.stewardship
    COMMENT 'DataGuardian business data stewardship operational tables'"""

_TABLES_DDL: list[str] = [
    """CREATE TABLE IF NOT EXISTS {catalog}.stewardship.stewardship_records (
        record_id        STRING NOT NULL COMMENT 'Unique record identifier (UUID)',
        run_id           STRING NOT NULL COMMENT 'Pipeline run that produced this failure',
        source_name      STRING NOT NULL COMMENT 'Source entity name (e.g. customers)',
        batch_id         STRING NOT NULL COMMENT 'Batch identifier from bronze ingestion',
        table_name       STRING NOT NULL COMMENT 'Target silver table (catalog.schema.table)',
        dq_score         DOUBLE         COMMENT 'Data quality score at time of failure (0-1)',
        status           STRING NOT NULL COMMENT 'PENDING | APPROVED | REJECTED | CORRECTION_REQUESTED',
        assigned_to      STRING         COMMENT 'Steward assigned to this record',
        violation_count  INT    NOT NULL COMMENT 'Number of DQ rule violations',
        failed_rules     STRING         COMMENT 'JSON array of FailedRule objects',
        raw_record       STRING         COMMENT 'JSON object of the raw failed record',
        ingested_at      TIMESTAMP NOT NULL COMMENT 'When the record was loaded into bronze',
        created_at       TIMESTAMP NOT NULL COMMENT 'When the stewardship record was created',
        reviewed_at      TIMESTAMP      COMMENT 'When the last steward action was taken',
        reviewed_by      STRING         COMMENT 'Steward who last acted on this record',
        updated_at       TIMESTAMP      COMMENT 'Last update timestamp'
    )
    USING DELTA
    COMMENT 'Failed records pending business data stewardship review'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",

    """CREATE TABLE IF NOT EXISTS {catalog}.stewardship.stewardship_actions (
        action_id        STRING    NOT NULL COMMENT 'Unique action identifier (UUID)',
        record_id        STRING    NOT NULL COMMENT 'FK to stewardship_records.record_id',
        action_type      STRING    NOT NULL COMMENT 'APPROVE | REJECT | REQUEST_CORRECTION | COMMENT | ASSIGN | REASSIGN',
        performed_by     STRING    NOT NULL COMMENT 'Steward who performed the action',
        comment          STRING             COMMENT 'Free-text justification',
        assigned_to      STRING             COMMENT 'Steward assigned (for ASSIGN/REASSIGN actions)',
        previous_status  STRING             COMMENT 'Record status before this action',
        new_status       STRING             COMMENT 'Record status after this action',
        action_timestamp TIMESTAMP NOT NULL COMMENT 'When this action was performed',
        metadata         STRING             COMMENT 'JSON metadata (dq_score, source details)'
    )
    USING DELTA
    COMMENT 'All steward actions — append-only, do not delete or update rows'""",

    """CREATE TABLE IF NOT EXISTS {catalog}.stewardship.comments (
        comment_id        STRING    NOT NULL COMMENT 'Unique comment identifier (UUID)',
        record_id         STRING    NOT NULL COMMENT 'FK to stewardship_records.record_id',
        parent_comment_id STRING             COMMENT 'FK to comments.comment_id for threaded replies',
        author            STRING    NOT NULL COMMENT 'Comment author (steward name)',
        message           STRING    NOT NULL COMMENT 'Comment text',
        status            STRING    NOT NULL COMMENT 'ACTIVE | DELETED',
        created_at        TIMESTAMP NOT NULL COMMENT 'When the comment was posted'
    )
    USING DELTA
    COMMENT 'Threaded discussion comments on stewardship records'""",

    """CREATE TABLE IF NOT EXISTS {catalog}.stewardship.audit_log (
        audit_id        STRING    NOT NULL COMMENT 'Unique audit entry identifier (UUID)',
        entity_type     STRING    NOT NULL COMMENT 'Entity type (stewardship_record)',
        entity_id       STRING    NOT NULL COMMENT 'ID of the entity that was acted on',
        operation       STRING    NOT NULL COMMENT 'Operation name (APPROVE, REJECT, etc.)',
        performed_by    STRING    NOT NULL COMMENT 'User who performed the operation',
        details         STRING             COMMENT 'JSON details of the operation',
        audit_timestamp TIMESTAMP NOT NULL COMMENT 'When the operation occurred'
    )
    USING DELTA
    COMMENT 'Immutable audit trail — never update or delete rows'
    TBLPROPERTIES ('delta.appendOnly' = 'true')""",
]


def initialize_schema(spark: object, catalog: str) -> None:
    """Create stewardship schema and all tables if they do not exist."""
    spark.sql(_SCHEMA_DDL.format(catalog=catalog))  # type: ignore[union-attr]
    for ddl in _TABLES_DDL:
        spark.sql(ddl.format(catalog=catalog))  # type: ignore[union-attr]
