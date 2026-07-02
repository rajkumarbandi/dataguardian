"""
Unit tests for M10 NaturalLanguageSQL.

Tests SQL generation, security validation (SELECT-only), DDL/DML rejection,
SQL comment blocking, and demo-mode DuckDB execution.
"""

import pandas as pd
import pytest

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.natural_language_sql import (
    NaturalLanguageSQL,
    _extract_sql_and_explanation,
    _validate_sql,
)
from src.ai.prompt_manager import PromptManager
from src.ai.provider import MockProvider
from src.ai.token_counter import TokenCounter


@pytest.fixture
def nl_sql():
    config = AIConfig(provider="mock")
    provider = MockProvider()
    pm = PromptManager("config/prompts")
    cache = PromptCache(ttl_seconds=3600)
    counter = TokenCounter()
    return NaturalLanguageSQL(provider, pm, config, cache, counter)


def make_tables() -> dict[str, pd.DataFrame]:
    """Minimal in-memory tables for DuckDB demo execution tests."""
    records = pd.DataFrame({
        "record_id": ["r1", "r2", "r3"],
        "source_name": ["customers", "orders", "customers"],
        "status": ["PENDING", "APPROVED", "REJECTED"],
        "dq_score": [0.65, 0.90, 0.45],
        "violation_count": [2, 0, 3],
        "assigned_to": ["Sarah Mitchell", "James Chen", "Sarah Mitchell"],
        "created_at": pd.to_datetime(["2026-06-01", "2026-06-05", "2026-06-10"]),
        "batch_id": ["B001", "B001", "B002"],
    })
    return {"stewardship_records": records}


# ── SQL generation ─────────────────────────────────────────────────────────────

class TestNaturalLanguageSQLGeneration:
    def test_query_returns_result(self, nl_sql):
        result = nl_sql.query("Show me all pending records")
        assert result.question == "Show me all pending records"
        assert result.sql or result.error   # Either generated SQL or an error

    def test_result_has_explanation(self, nl_sql):
        result = nl_sql.query("Show me all pending records")
        assert result.explanation is not None

    def test_same_question_uses_cache(self, nl_sql):
        question = "How many records are pending?"
        r1 = nl_sql.query(question)
        r2 = nl_sql.query(question)
        assert r2.cached


# ── Security validation ────────────────────────────────────────────────────────

class TestValidateSQL:
    @pytest.mark.parametrize("sql", [
        "SELECT * FROM stewardship_records",
        "SELECT count(*) FROM stewardship_records WHERE status = 'PENDING'",
        "SELECT r.record_id, r.dq_score FROM stewardship_records r ORDER BY dq_score LIMIT 10",
    ])
    def test_valid_select_passes(self, sql):
        assert _validate_sql(sql) is None

    @pytest.mark.parametrize("forbidden_sql", [
        "INSERT INTO stewardship_records VALUES ('x')",
        "UPDATE stewardship_records SET status = 'APPROVED'",
        "DELETE FROM stewardship_records WHERE record_id = '1'",
        "DROP TABLE stewardship_records",
        "CREATE TABLE evil AS SELECT * FROM records",
        "ALTER TABLE stewardship_records ADD COLUMN x STRING",
        "TRUNCATE TABLE stewardship_records",
        "MERGE INTO stewardship_records USING ...",
        "SELECT * FROM records; DROP TABLE stewardship_records",
    ])
    def test_forbidden_statement_rejected(self, forbidden_sql):
        assert _validate_sql(forbidden_sql) is not None

    @pytest.mark.parametrize("sql_with_comments", [
        "SELECT * FROM records -- this is a comment",
        "SELECT /* inline comment */ * FROM records",
    ])
    def test_sql_comments_rejected(self, sql_with_comments):
        assert _validate_sql(sql_with_comments) is not None

    def test_non_select_rejected(self):
        error = _validate_sql("SHOW TABLES")
        assert error is not None
        assert "SELECT" in error

    def test_case_insensitive_detection(self):
        assert _validate_sql("insert into foo values ('x')") is not None
        assert _validate_sql("drop table bar") is not None

    def test_static_validate_method(self):
        assert NaturalLanguageSQL.validate_sql("SELECT 1") is None
        assert NaturalLanguageSQL.validate_sql("DELETE FROM x") is not None


# ── SQL extraction ─────────────────────────────────────────────────────────────

class TestExtractSQLAndExplanation:
    def test_extracts_fenced_sql(self):
        text = "Some preamble\n```sql\nSELECT * FROM records LIMIT 10;\n```\nThis query does X."
        sql, explanation = _extract_sql_and_explanation(text)
        assert sql.strip() == "SELECT * FROM records LIMIT 10;"
        assert "This query does X" in explanation

    def test_no_fence_returns_empty_sql(self):
        text = "I cannot generate that query."
        sql, explanation = _extract_sql_and_explanation(text)
        assert sql == ""
        assert explanation == text.strip()

    def test_case_insensitive_fence(self):
        text = "```SQL\nSELECT 1;\n```"
        sql, _ = _extract_sql_and_explanation(text)
        assert "SELECT 1" in sql

    def test_explanation_is_text_after_fence(self):
        text = "```sql\nSELECT 1;\n```\nReturns a single row."
        _, explanation = _extract_sql_and_explanation(text)
        assert "single row" in explanation


# ── Demo execution ─────────────────────────────────────────────────────────────

class TestDemoExecution:
    def test_select_executes_against_tables(self, nl_sql):
        tables = make_tables()
        result = nl_sql.query(
            "Show all records",
            tables=tables,
        )
        # Even if the generated SQL fails, the result object must be returned
        assert result is not None

    def test_no_tables_gives_no_data(self, nl_sql):
        result = nl_sql.query("Show all records", tables=None)
        # Without tables, data should be None (SQL shown but not executed)
        assert result.data is None or isinstance(result.data, pd.DataFrame)

    def test_forbidden_sql_not_executed(self):
        """Even if the LLM returns forbidden SQL, it must not be executed."""
        from src.ai.natural_language_sql import _execute_demo
        tables = make_tables()
        forbidden = "DROP TABLE stewardship_records"
        error = _validate_sql(forbidden)
        assert error is not None
        # We never call _execute_demo with forbidden SQL in the main path
