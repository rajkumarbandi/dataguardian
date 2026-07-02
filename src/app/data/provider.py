"""
DataProvider abstraction for the DataGuardian Stewardship Portal.

SparkDataProvider  — reads/writes via Databricks SQL Warehouse (production)
SampleDataProvider — in-memory pandas DataFrames (demo / local dev)

The factory function get_data_provider() selects the correct implementation
based on the application settings (DG_DEMO_MODE env var or missing warehouse path).
"""

from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from src.app.config.settings import get_settings


class DataProvider(ABC):
    """Abstract data access layer — all pages talk only through this interface."""

    @abstractmethod
    def get_stewardship_records(
        self,
        status: str | None = None,
        source_name: str | None = None,
        assigned_to: str | None = None,
        limit: int = 500,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def get_stewardship_record(self, record_id: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def get_actions(self, record_id: str) -> pd.DataFrame: ...

    @abstractmethod
    def get_comments(self, record_id: str) -> pd.DataFrame: ...

    @abstractmethod
    def get_audit_log(
        self,
        entity_type: str | None = None,
        performed_by: str | None = None,
        operation: str | None = None,
        days: int = 30,
        limit: int = 500,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def get_pipeline_runs(
        self,
        source_name: str | None = None,
        limit: int = 50,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def save_action(self, action: dict[str, Any]) -> None: ...

    @abstractmethod
    def save_comment(self, comment: dict[str, Any]) -> None: ...

    @abstractmethod
    def update_record_status(
        self,
        record_id: str,
        new_status: str,
        previous_status: str,
        performed_by: str,
        comment: str = "",
    ) -> None: ...

    @abstractmethod
    def get_stewards(self) -> list[str]: ...

    @abstractmethod
    def get_sources(self) -> list[str]: ...


# ── Sample Data Provider ──────────────────────────────────────────────────────

class SampleDataProvider(DataProvider):
    """In-memory DataProvider backed by the sample data generator."""

    def __init__(self) -> None:
        from src.app.data.sample import generate_sample_data
        self._tables = generate_sample_data()

    def _records(self) -> pd.DataFrame:
        return self._tables["stewardship_records"]

    def _actions(self) -> pd.DataFrame:
        return self._tables["stewardship_actions"]

    def _comments(self) -> pd.DataFrame:
        return self._tables["comments"]

    def _audit(self) -> pd.DataFrame:
        return self._tables["audit_log"]

    def _runs(self) -> pd.DataFrame:
        return self._tables["pipeline_runs"]

    def get_stewardship_records(
        self,
        status: str | None = None,
        source_name: str | None = None,
        assigned_to: str | None = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        df = self._records().copy()
        if status:
            df = df[df["status"] == status]
        if source_name:
            df = df[df["source_name"] == source_name]
        if assigned_to is not None:
            df = df[df["assigned_to"] == assigned_to]
        return df.sort_values("created_at", ascending=False).head(limit).reset_index(drop=True)

    def get_stewardship_record(self, record_id: str) -> dict[str, Any] | None:
        df = self._records()
        rows = df[df["record_id"] == record_id]
        if rows.empty:
            return None
        row = rows.iloc[0].to_dict()
        # Parse JSON fields
        if isinstance(row.get("failed_rules"), str):
            row["failed_rules"] = json.loads(row["failed_rules"])
        if isinstance(row.get("raw_record"), str):
            row["raw_record"] = json.loads(row["raw_record"])
        return row

    def get_actions(self, record_id: str) -> pd.DataFrame:
        df = self._actions()
        return df[df["record_id"] == record_id].sort_values("action_timestamp").reset_index(drop=True)

    def get_comments(self, record_id: str) -> pd.DataFrame:
        df = self._comments()
        return df[df["record_id"] == record_id].sort_values("created_at").reset_index(drop=True)

    def get_audit_log(
        self,
        entity_type: str | None = None,
        performed_by: str | None = None,
        operation: str | None = None,
        days: int = 30,
        limit: int = 500,
    ) -> pd.DataFrame:
        df = self._audit().copy()
        cutoff = datetime(2026, 6, 26) - timedelta(days=days)
        if not df.empty:
            df["audit_timestamp"] = pd.to_datetime(df["audit_timestamp"])
            df = df[df["audit_timestamp"] >= cutoff]
        if entity_type:
            df = df[df["entity_type"] == entity_type]
        if performed_by:
            df = df[df["performed_by"] == performed_by]
        if operation:
            df = df[df["operation"] == operation]
        return df.sort_values("audit_timestamp", ascending=False).head(limit).reset_index(drop=True)

    def get_pipeline_runs(
        self,
        source_name: str | None = None,
        limit: int = 50,
    ) -> pd.DataFrame:
        df = self._runs().copy()
        if source_name:
            df = df[df["source_name"] == source_name]
        return df.sort_values("start_time", ascending=False).head(limit).reset_index(drop=True)

    def save_action(self, action: dict[str, Any]) -> None:
        row = {**action, "action_id": str(uuid.uuid4()), "metadata": json.dumps(action.get("metadata", {}))}
        new_row = pd.DataFrame([row])
        self._tables["stewardship_actions"] = pd.concat(
            [self._tables["stewardship_actions"], new_row], ignore_index=True
        )
        # Also append to audit log
        audit_row = pd.DataFrame([{
            "audit_id": str(uuid.uuid4()),
            "entity_type": "stewardship_record",
            "entity_id": action["record_id"],
            "operation": action["action_type"],
            "performed_by": action["performed_by"],
            "details": json.dumps({"comment": action.get("comment", ""), "new_status": action.get("new_status")}),
            "audit_timestamp": action.get("action_timestamp", datetime.utcnow()),
        }])
        self._tables["audit_log"] = pd.concat(
            [self._tables["audit_log"], audit_row], ignore_index=True
        )

    def save_comment(self, comment: dict[str, Any]) -> None:
        row = {**comment, "comment_id": str(uuid.uuid4())}
        new_row = pd.DataFrame([row])
        self._tables["comments"] = pd.concat(
            [self._tables["comments"], new_row], ignore_index=True
        )

    def update_record_status(
        self,
        record_id: str,
        new_status: str,
        previous_status: str,
        performed_by: str,
        comment: str = "",
    ) -> None:
        idx = self._tables["stewardship_records"].index[
            self._tables["stewardship_records"]["record_id"] == record_id
        ]
        if not idx.empty:
            self._tables["stewardship_records"].loc[idx[0], "status"] = new_status
            self._tables["stewardship_records"].loc[idx[0], "reviewed_by"] = performed_by
            self._tables["stewardship_records"].loc[idx[0], "reviewed_at"] = datetime.utcnow()
            self._tables["stewardship_records"].loc[idx[0], "updated_at"] = datetime.utcnow()

    def get_stewards(self) -> list[str]:
        return ["Sarah Mitchell", "James Chen", "Emma Davis", "Oliver Brown"]

    def get_sources(self) -> list[str]:
        return ["customers", "orders", "products", "order_items"]


# ── Spark Data Provider ───────────────────────────────────────────────────────

class SparkDataProvider(DataProvider):
    """
    DataProvider backed by a Databricks SQL Warehouse.

    Uses databricks-sql-connector with credentials from environment variables
    (DATABRICKS_HOST and DATABRICKS_TOKEN) auto-injected by Databricks Apps.
    """

    def __init__(self, catalog: str, http_path: str, server_hostname: str, access_token: str) -> None:
        from databricks import sql  # type: ignore[import-untyped]
        self._catalog = catalog
        self._conn = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema="stewardship",
        )

    def _query(self, sql_text: str) -> pd.DataFrame:
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql_text)
            result = cursor.fetchall_arrow()
            return result.to_pandas()
        finally:
            cursor.close()

    def _execute(self, sql_text: str) -> None:
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql_text)
        finally:
            cursor.close()

    def get_stewardship_records(
        self,
        status: str | None = None,
        source_name: str | None = None,
        assigned_to: str | None = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        where_clauses = []
        if status:
            where_clauses.append(f"status = '{status}'")
        if source_name:
            where_clauses.append(f"source_name = '{source_name}'")
        if assigned_to is not None:
            where_clauses.append(f"assigned_to = '{assigned_to}'")
        where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        return self._query(
            f"SELECT * FROM {self._catalog}.stewardship.stewardship_records "
            f"{where} ORDER BY created_at DESC LIMIT {limit}"
        )

    def get_stewardship_record(self, record_id: str) -> dict[str, Any] | None:
        df = self._query(
            f"SELECT * FROM {self._catalog}.stewardship.stewardship_records "
            f"WHERE record_id = '{record_id}' LIMIT 1"
        )
        if df.empty:
            return None
        row = df.iloc[0].to_dict()
        for field in ("failed_rules", "raw_record"):
            if isinstance(row.get(field), str):
                row[field] = json.loads(row[field])
        return row

    def get_actions(self, record_id: str) -> pd.DataFrame:
        return self._query(
            f"SELECT * FROM {self._catalog}.stewardship.stewardship_actions "
            f"WHERE record_id = '{record_id}' ORDER BY action_timestamp ASC"
        )

    def get_comments(self, record_id: str) -> pd.DataFrame:
        return self._query(
            f"SELECT * FROM {self._catalog}.stewardship.comments "
            f"WHERE record_id = '{record_id}' ORDER BY created_at ASC"
        )

    def get_audit_log(
        self,
        entity_type: str | None = None,
        performed_by: str | None = None,
        operation: str | None = None,
        days: int = 30,
        limit: int = 500,
    ) -> pd.DataFrame:
        clauses = [f"audit_timestamp >= current_timestamp() - INTERVAL {days} DAYS"]
        if entity_type:
            clauses.append(f"entity_type = '{entity_type}'")
        if performed_by:
            clauses.append(f"performed_by = '{performed_by}'")
        if operation:
            clauses.append(f"operation = '{operation}'")
        where = "WHERE " + " AND ".join(clauses)
        return self._query(
            f"SELECT * FROM {self._catalog}.stewardship.audit_log "
            f"{where} ORDER BY audit_timestamp DESC LIMIT {limit}"
        )

    def get_pipeline_runs(self, source_name: str | None = None, limit: int = 50) -> pd.DataFrame:
        where = f"WHERE source_name = '{source_name}'" if source_name else ""
        return self._query(
            f"SELECT * FROM {self._catalog}.audit.pipeline_run_history "
            f"{where} ORDER BY start_time DESC LIMIT {limit}"
        )

    def save_action(self, action: dict[str, Any]) -> None:
        action_id = str(uuid.uuid4())
        ts = action.get("action_timestamp", datetime.utcnow()).isoformat()
        metadata = json.dumps(action.get("metadata", {})).replace("'", "''")
        comment = (action.get("comment") or "").replace("'", "''")
        self._execute(
            f"INSERT INTO {self._catalog}.stewardship.stewardship_actions VALUES ("
            f"'{action_id}', '{action['record_id']}', '{action['action_type']}', "
            f"'{action['performed_by']}', '{comment}', '{action.get('assigned_to', '')}', "
            f"'{action.get('previous_status', '')}', '{action.get('new_status', '')}', "
            f"TIMESTAMP '{ts}', '{metadata}')"
        )

    def save_comment(self, comment: dict[str, Any]) -> None:
        comment_id = str(uuid.uuid4())
        ts = datetime.utcnow().isoformat()
        parent = f"'{comment.get('parent_comment_id')}'" if comment.get("parent_comment_id") else "NULL"
        msg = (comment.get("message") or "").replace("'", "''")
        self._execute(
            f"INSERT INTO {self._catalog}.stewardship.comments VALUES ("
            f"'{comment_id}', '{comment['record_id']}', {parent}, "
            f"'{comment['author']}', '{msg}', 'ACTIVE', TIMESTAMP '{ts}')"
        )

    def update_record_status(
        self,
        record_id: str,
        new_status: str,
        previous_status: str,
        performed_by: str,
        comment: str = "",
    ) -> None:
        ts = datetime.utcnow().isoformat()
        self._execute(
            f"UPDATE {self._catalog}.stewardship.stewardship_records "
            f"SET status = '{new_status}', reviewed_by = '{performed_by}', "
            f"reviewed_at = TIMESTAMP '{ts}', updated_at = TIMESTAMP '{ts}' "
            f"WHERE record_id = '{record_id}'"
        )

    def get_stewards(self) -> list[str]:
        df = self._query(
            f"SELECT DISTINCT reviewed_by FROM {self._catalog}.stewardship.stewardship_records "
            f"WHERE reviewed_by IS NOT NULL AND reviewed_by != '' ORDER BY reviewed_by"
        )
        return df["reviewed_by"].tolist() if not df.empty else []

    def get_sources(self) -> list[str]:
        df = self._query(
            f"SELECT DISTINCT source_name FROM {self._catalog}.stewardship.stewardship_records "
            f"ORDER BY source_name"
        )
        return df["source_name"].tolist() if not df.empty else []


# ── Factory ───────────────────────────────────────────────────────────────────

def get_data_provider() -> DataProvider:
    """
    Return the appropriate DataProvider for the current environment.

    When called inside a Streamlit app the result is cached via
    @st.cache_resource so the provider is created only once per process.
    Outside Streamlit (tests, CLI) the function returns a fresh instance.
    """
    try:
        import streamlit as st
        return _get_data_provider_cached()
    except Exception:
        return _build_provider()


def _get_data_provider_cached() -> DataProvider:
    import streamlit as st

    @st.cache_resource
    def _inner() -> DataProvider:
        return _build_provider()

    return _inner()


def _build_provider() -> DataProvider:
    import os
    settings = get_settings()
    if settings.demo_mode:
        return SampleDataProvider()
    token = os.environ.get("DATABRICKS_TOKEN", "")
    return SparkDataProvider(
        catalog=settings.catalog,
        http_path=settings.warehouse_http_path,
        server_hostname=settings.server_hostname,
        access_token=token,
    )
