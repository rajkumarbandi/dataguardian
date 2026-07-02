"""Unit tests for the DataGuardian structured logger."""

from __future__ import annotations

import json
import logging

import pytest

from src.common.logger import (
    DataGuardianLogger,
    JsonFormatter,
    get_logger,
    get_pipeline_logger,
)


# ---------------------------------------------------------------------------
# JsonFormatter
# ---------------------------------------------------------------------------


class TestJsonFormatter:
    def _make_record(self, message: str, level: int = logging.INFO) -> logging.LogRecord:
        record = logging.LogRecord(
            name="test",
            level=level,
            pathname="test.py",
            lineno=1,
            msg=message,
            args=(),
            exc_info=None,
        )
        return record

    def test_output_is_valid_json(self) -> None:
        formatter = JsonFormatter()
        record = self._make_record("hello")
        output = formatter.format(record)
        parsed = json.loads(output)
        assert isinstance(parsed, dict)

    def test_standard_fields_present(self) -> None:
        formatter = JsonFormatter()
        record = self._make_record("test message")
        parsed = json.loads(formatter.format(record))
        assert "timestamp" in parsed
        assert "level" in parsed
        assert "message" in parsed
        assert parsed["message"] == "test message"

    def test_context_fields_embedded(self) -> None:
        formatter = JsonFormatter(context={"batch_id": "abc-123", "entity": "customer"})
        record = self._make_record("ingestion started")
        parsed = json.loads(formatter.format(record))
        assert parsed["batch_id"] == "abc-123"
        assert parsed["entity"] == "customer"

    def test_level_name_correct(self) -> None:
        formatter = JsonFormatter()
        record = self._make_record("warning!", level=logging.WARNING)
        parsed = json.loads(formatter.format(record))
        assert parsed["level"] == "WARNING"


# ---------------------------------------------------------------------------
# DataGuardianLogger
# ---------------------------------------------------------------------------


class TestDataGuardianLogger:
    def test_bind_returns_new_instance(self) -> None:
        original = get_logger("test.bind", level="INFO", component="engine")
        bound = original.bind(batch_id="xyz")
        assert bound is not original

    def test_bind_merges_context(self) -> None:
        original = get_logger("test.merge", component="engine")
        bound = original.bind(batch_id="xyz")
        assert bound._context["component"] == "engine"
        assert bound._context["batch_id"] == "xyz"

    def test_bind_does_not_mutate_parent(self) -> None:
        original = get_logger("test.immutable", component="engine")
        original.bind(batch_id="xyz")
        assert "batch_id" not in original._context

    def test_info_does_not_raise(self) -> None:
        logger = get_logger("test.info")
        logger.info("no exception expected")

    def test_warning_does_not_raise(self) -> None:
        logger = get_logger("test.warning")
        logger.warning("this is a warning")

    def test_error_does_not_raise(self) -> None:
        logger = get_logger("test.error")
        logger.error("this is an error")

    def test_exception_captures_traceback(self) -> None:
        logger = get_logger("test.exception")
        try:
            raise ValueError("deliberate")
        except ValueError:
            # Should not raise — captures and logs the traceback
            logger.exception("caught an error")

    def test_get_pipeline_logger_binds_standard_fields(self) -> None:
        pl = get_pipeline_logger(
            source_system="erp",
            entity="customers",
            batch_id="batch-001",
        )
        assert pl._context["source_system"] == "erp"
        assert pl._context["entity"] == "customers"
        assert pl._context["batch_id"] == "batch-001"


# ---------------------------------------------------------------------------
# File handler
# ---------------------------------------------------------------------------


class TestFileHandler:
    def test_log_file_created(self, tmp_path) -> None:
        log_path = str(tmp_path / "logs" / "test.log")
        logger = get_logger("test.file", log_file=log_path)
        logger.info("written to file")
        import os
        assert os.path.exists(log_path)

    def test_log_file_contains_json(self, tmp_path) -> None:
        log_path = str(tmp_path / "logs" / "json.log")
        logger = get_logger("test.json.file", log_file=log_path)
        logger.info("json line")
        with open(log_path, encoding="utf-8") as f:
            line = f.readline().strip()
        parsed = json.loads(line)
        assert parsed["message"] == "json line"
