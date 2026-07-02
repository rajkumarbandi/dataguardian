"""
Unit tests for M10 PromptManager.

Tests YAML loading, template rendering, variable substitution,
and error cases (missing file, missing variable).
"""

import textwrap
from pathlib import Path

import pytest

from src.ai.prompt_manager import (
    PromptManager,
    PromptNotFoundError,
    PromptRenderError,
    _render_template,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_prompts_dir(tmp_path: Path, files: dict[str, str]) -> Path:
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir()
    for name, content in files.items():
        (prompts_dir / name).write_text(content, encoding="utf-8")
    return prompts_dir


# ── PromptManager.render ──────────────────────────────────────────────────────

class TestPromptManagerRender:
    def test_renders_system_and_user_messages(self, tmp_path):
        prompts_dir = make_prompts_dir(tmp_path, {
            "test_feature.yml": textwrap.dedent("""\
                system: You are a helpful assistant for {{ domain }}.
                user: Analyse {{ item }}.
            """),
        })
        pm = PromptManager(prompts_dir)
        messages = pm.render("test_feature", {"domain": "finance", "item": "customer record"})
        assert len(messages) == 2
        assert messages[0].role == "system"
        assert "finance" in messages[0].content
        assert messages[1].role == "user"
        assert "customer record" in messages[1].content

    def test_renders_user_only(self, tmp_path):
        prompts_dir = make_prompts_dir(tmp_path, {
            "user_only.yml": "user: Answer {{ question }}.",
        })
        pm = PromptManager(prompts_dir)
        messages = pm.render("user_only", {"question": "What is DQ?"})
        assert len(messages) == 1
        assert messages[0].role == "user"
        assert "What is DQ?" in messages[0].content

    def test_missing_variable_raises_render_error(self, tmp_path):
        prompts_dir = make_prompts_dir(tmp_path, {
            "needs_var.yml": "user: Value is {{ required_var }}.",
        })
        pm = PromptManager(prompts_dir)
        with pytest.raises(PromptRenderError, match="required_var"):
            pm.render("needs_var", {})

    def test_extra_variables_are_ignored(self, tmp_path):
        prompts_dir = make_prompts_dir(tmp_path, {
            "simple.yml": "user: Hello {{ name }}.",
        })
        pm = PromptManager(prompts_dir)
        messages = pm.render("simple", {"name": "World", "extra": "ignored"})
        assert "Hello World" in messages[0].content

    def test_missing_feature_file_raises(self, tmp_path):
        pm = PromptManager(tmp_path / "nonexistent_prompts_dir")
        with pytest.raises(PromptNotFoundError):
            pm.render("no_such_feature", {})

    def test_no_system_no_user_raises(self, tmp_path):
        prompts_dir = make_prompts_dir(tmp_path, {
            "empty.yml": "other_key: something",
        })
        pm = PromptManager(prompts_dir)
        with pytest.raises(PromptRenderError, match="neither"):
            pm.render("empty", {})


# ── PromptManager.list_features ───────────────────────────────────────────────

class TestListFeatures:
    def test_lists_yml_stems(self, tmp_path):
        prompts_dir = make_prompts_dir(tmp_path, {
            "feature_a.yml": "user: a",
            "feature_b.yml": "user: b",
        })
        pm = PromptManager(prompts_dir)
        features = pm.list_features()
        assert "feature_a" in features
        assert "feature_b" in features

    def test_empty_dir_returns_empty_list(self, tmp_path):
        pm = PromptManager(tmp_path)
        assert pm.list_features() == []


# ── Template rendering ─────────────────────────────────────────────────────────

class TestRenderTemplate:
    def test_simple_substitution(self):
        result = _render_template("Hello {{ name }}!", {"name": "Alice"}, "test")
        assert result == "Hello Alice!"

    def test_whitespace_tolerance(self):
        result = _render_template("{{ x }}", {"x": "42"}, "test")
        assert result == "42"

    def test_missing_variable_raises(self):
        with pytest.raises(PromptRenderError, match="missing_var"):
            _render_template("{{ missing_var }}", {}, "test")

    def test_non_string_values_converted(self):
        result = _render_template("Score: {{ score }}", {"score": 0.876}, "test")
        assert "0.876" in result

    def test_multiple_occurrences(self):
        result = _render_template("{{ x }} and {{ x }}", {"x": "hello"}, "test")
        assert result == "hello and hello"


# ── Caching ───────────────────────────────────────────────────────────────────

class TestPromptManagerCache:
    def test_file_read_once(self, tmp_path):
        prompts_dir = make_prompts_dir(tmp_path, {
            "cached.yml": "user: Cached {{ val }}",
        })
        pm = PromptManager(prompts_dir)
        # First render — loads and caches the file
        pm.render("cached", {"val": "a"})
        assert "cached" in pm._cache
        cache_size_after_first = len(pm._cache)
        # Second render — hits cache, no file re-read
        pm.render("cached", {"val": "b"})
        # Cache size must not grow (same feature key)
        assert len(pm._cache) == cache_size_after_first


# ── Production prompt files ────────────────────────────────────────────────────

class TestProductionPrompts:
    """Validate all 8 production prompt files can be loaded and rendered."""

    FEATURE_VARS = {
        "dq_explanation": {
            "source_name": "customers", "table_name": "silver.customers",
            "dq_score": "0.72", "rule_name": "email_format", "column_name": "email",
            "severity": "HIGH", "expected_value": "valid email", "actual_value": "user@",
            "rule_message": "Email must contain a valid domain", "raw_record_summary": "id: 123",
        },
        "schema_mapping": {
            "source_fields": "- CustomerName\n- DOB",
            "target_fields": "- customer_name\n- birth_date",
            "source_system": "Salesforce", "domain": "CRM", "notes": "None",
        },
        "root_cause": {
            "source_name": "customers", "batch_id": "B001",
            "total_records": "100", "failed_records": "20", "failure_rate": "20.0",
            "violation_summary": "  1. email: 15 (75%)",
            "sample_failures": "  - record_id=abc123... dq_score=0.60 violations=2",
            "previous_failure_rate": "18.0", "trend": "Degrading (+2.0%)",
        },
        "duplicate_detection": {
            "entity_type": "vendor", "domain": "Procurement",
            "candidate_records": "**Group 1**: IBM | International Business Machines",
            "matching_attributes": "name, tax_id",
        },
        "comment_summary": {
            "record_id": "r-001", "source_name": "customers",
            "violation_summary": "email_format", "participants": "Sarah, James",
            "thread_count": "5", "thread_content": "[2026] Sarah: this is broken",
        },
        "nl_sql": {
            "question": "Show all pending records",
            "schema_context": "stewardship_records table",
        },
        "data_profiling": {
            "source_name": "customers", "batch_id": "B001",
            "total_records": "200", "passed_records": "160", "failed_records": "40",
            "dq_score": "72.0", "column_issues": "  - email: invalid (20%)",
            "violation_breakdown": "  1. email_format: 30 (75%)",
            "historical_comparison": "DQ score improved by 2% vs previous batch",
        },
        "record_explanation": {
            "record_id": "r-001", "source_name": "customers",
            "dq_score_pct": "72.0", "status": "PENDING",
            "violation_count": "2",
            "violations_list": "  1. **email_format** on `email`",
            "key_fields": "  id: 123\n  name: Alice",
        },
    }

    @pytest.mark.parametrize("feature,variables", FEATURE_VARS.items())
    def test_prompt_renders_without_error(self, feature, variables):
        """Each production prompt must render given its required variables."""
        pm = PromptManager("config/prompts")
        messages = pm.render(feature, variables)
        assert messages
        for msg in messages:
            assert msg.content.strip()
            assert "{{" not in msg.content, f"Unreplaced placeholder in {feature}"
