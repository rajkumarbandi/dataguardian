"""
Natural Language SQL — converts plain-English questions to SELECT queries.

Security: Only SELECT statements are permitted. DDL/DML and SQL comments are
rejected before query execution. The generated SQL is shown to the user for
transparency before any execution occurs.

In demo mode: DuckDB executes the query in-memory against pandas DataFrames.
In production: SQL Warehouse is used via a separate AIDataConnector.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.ai.cache import PromptCache
from src.ai.config import AIConfig
from src.ai.prompt_manager import PromptManager
from src.ai.provider import LLMProvider
from src.ai.token_counter import TokenCounter


@dataclass
class NLSQLResult:
    question: str
    sql: str
    explanation: str
    data: pd.DataFrame | None
    row_count: int
    error: str | None
    prompt_tokens: int
    completion_tokens: int
    cached: bool

    @property
    def success(self) -> bool:
        return self.error is None and self.data is not None


# Patterns that are never allowed in generated SQL
_FORBIDDEN_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|REPLACE|UPSERT"
    r"|EXEC|EXECUTE|CALL|GRANT|REVOKE|LOCK|UNLOCK|SET)\b"
    r"|/\*.*?\*/|--",
    re.IGNORECASE | re.DOTALL,
)

_SQL_FENCE = re.compile(r"```sql\s*([\s\S]+?)```", re.IGNORECASE)


class NaturalLanguageSQL:
    """
    Converts natural language questions to SQL SELECT queries.

    Usage:
        nl_sql = NaturalLanguageSQL(provider, prompt_manager, config, cache, counter)
        result = nl_sql.query("Show me all pending records with a DQ score below 0.7")
        if result.success:
            st.dataframe(result.data)
    """

    def __init__(
        self,
        provider: LLMProvider,
        prompt_manager: PromptManager,
        config: AIConfig,
        cache: PromptCache,
        token_counter: TokenCounter,
    ) -> None:
        self._provider = provider
        self._pm = prompt_manager
        self._config = config
        self._cache = cache
        self._counter = token_counter

    def query(
        self,
        question: str,
        tables: dict[str, pd.DataFrame] | None = None,
        schema_context: str = "",
    ) -> NLSQLResult:
        """
        Convert `question` to SQL and optionally execute it.

        Args:
            question: The natural language question
            tables: Dict of {table_name: DataFrame} for demo mode execution.
                    If None, SQL is generated but not executed.
            schema_context: Additional schema context for the prompt.
        """
        variables = {
            "question": question,
            "schema_context": schema_context or "Use the DataGuardian stewardship schema.",
        }

        messages = self._pm.render("nl_sql", variables)
        cached = False

        cached_response = self._cache.get(messages)
        if cached_response is not None:
            response = cached_response
            cached = True
        else:
            response = self._provider.complete(
                messages,
                max_tokens=self._config.max_tokens,
                temperature=0.0,    # Always deterministic for SQL
            )
            self._cache.put(messages, response)

        if not cached:
            self._counter.record_from_response("natural_language_sql", response)

        sql, explanation = _extract_sql_and_explanation(response.content)
        if not sql:
            return NLSQLResult(
                question=question,
                sql="",
                explanation=response.content,
                data=None,
                row_count=0,
                error="Could not extract a SQL query from the AI response.",
                prompt_tokens=response.prompt_tokens,
                completion_tokens=response.completion_tokens,
                cached=cached,
            )

        # Security validation — must pass before any execution
        error = _validate_sql(sql)
        if error:
            return NLSQLResult(
                question=question,
                sql=sql,
                explanation=explanation,
                data=None,
                row_count=0,
                error=error,
                prompt_tokens=response.prompt_tokens,
                completion_tokens=response.completion_tokens,
                cached=cached,
            )

        # Execute in demo mode using DuckDB
        data: pd.DataFrame | None = None
        exec_error: str | None = None
        if tables:
            data, exec_error = _execute_demo(sql, tables)

        return NLSQLResult(
            question=question,
            sql=sql,
            explanation=explanation,
            data=data,
            row_count=len(data) if data is not None else 0,
            error=exec_error,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            cached=cached,
        )

    @staticmethod
    def validate_sql(sql: str) -> str | None:
        """Public validation helper — returns error string or None if valid."""
        return _validate_sql(sql)


def _extract_sql_and_explanation(text: str) -> tuple[str, str]:
    """Extract SQL from a fenced code block and the explanation that follows."""
    match = _SQL_FENCE.search(text)
    if not match:
        return "", text.strip()
    sql = match.group(1).strip()
    # Everything after the closing ``` is the explanation
    explanation = text[match.end():].strip()
    return sql, explanation


def _validate_sql(sql: str) -> str | None:
    """
    Validate that the SQL is a safe SELECT statement.

    Returns an error message if invalid, None if safe.
    """
    stripped = sql.strip()
    if not stripped.upper().startswith("SELECT"):
        return "Only SELECT queries are permitted. The generated query does not start with SELECT."
    if _FORBIDDEN_PATTERNS.search(stripped):
        return (
            "The generated SQL contains forbidden keywords or comments. "
            "Only read-only SELECT queries are permitted."
        )
    return None


def _execute_demo(sql: str, tables: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame | None, str | None]:
    """Execute the SQL against in-memory DataFrames using DuckDB."""
    try:
        import duckdb  # type: ignore[import-untyped]
    except ImportError:
        return None, "DuckDB is not installed. Install it with: pip install duckdb"

    try:
        con = duckdb.connect(database=":memory:")
        for table_name, df in tables.items():
            # Map common table name aliases to their DataFrames
            clean_name = table_name.replace("stewardship.", "").replace(".", "_")
            con.register(clean_name, df)
            con.register(table_name, df)

        # Rewrite qualified table names for DuckDB
        safe_sql = _rewrite_for_duckdb(sql)
        result = con.execute(safe_sql).df()
        con.close()
        return result, None
    except Exception as exc:
        return None, f"Query execution error: {exc}"


def _rewrite_for_duckdb(sql: str) -> str:
    """Strip schema prefixes that DuckDB doesn't understand."""
    # stewardship.stewardship_records → stewardship_records
    return re.sub(r"\b\w+\.(\w+)\b", r"\1", sql)
