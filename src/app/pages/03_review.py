"""
DataGuardian Stewardship Portal — Record Review page.

The primary action surface: loads a single record, shows its violations,
raw data, and provides the approve/reject/correct/assign/comment interface.
"""

import streamlit as st

from src.app.data.provider import get_data_provider
from src.app.repository.audit_repo import AuditRepository
from src.app.services.stewardship_service import StewardshipService
from src.app.ui.components import (
    action_panel,
    action_timeline,
    comment_thread,
    empty_state,
    raw_record_viewer,
    record_detail_card,
)
from src.app.ui.styles import inject_global_css, page_header

def _render_ai_insights(record, provider) -> None:
    """AI Insights tab — record explanation and per-violation DQ analysis."""
    from src.ai.components import get_ai_components
    from src.app.ui.styles import section_title

    ai = get_ai_components()

    if not ai.config.is_feature_enabled("explanation_engine"):
        st.info("AI Insights are not enabled. Set `features.explanation_engine: true` in config/ai.yml.")
        return

    badge = "DEMO — Mock AI" if ai.is_mock else ai.provider_label
    st.caption(f"AI Provider: **{badge}**")

    # ── Record explanation ────────────────────────────────────────────────────
    section_title("Plain-English Record Explanation")
    if st.button("Explain This Record", key="ai_explain_record", type="primary"):
        with st.spinner("Generating explanation..."):
            record_dict = {
                "record_id": record.record_id,
                "source_name": record.source_name,
                "dq_score": record.dq_score,
                "status": record.status.value,
                "violation_count": record.violation_count,
                "failed_rules": [
                    {
                        "rule_name": r.rule_name,
                        "column": r.column,
                        "severity": r.severity.value if hasattr(r.severity, "value") else str(r.severity),
                        "message": r.message,
                        "expected_value": getattr(r, "expected_value", "N/A"),
                        "actual_value": getattr(r, "actual_value", "N/A"),
                    }
                    for r in record.failed_rules
                ],
                "raw_record": record.raw_record,
            }
            explanation = ai.explanation_engine.explain_record(record_dict)
        st.markdown(explanation.explanation)
        st.caption(
            f"{'Cached' if explanation.cached else f'{explanation.prompt_tokens}p/{explanation.completion_tokens}c tokens'}"
        )

    st.divider()

    # ── Per-violation DQ explanations ─────────────────────────────────────────
    if record.failed_rules and ai.config.is_feature_enabled("dq_assistant"):
        section_title("Violation Business Impact")
        st.markdown("Click a violation to get a plain-English business impact assessment.")
        for i, rule in enumerate(record.failed_rules):
            rule_dict = {
                "rule_name": rule.rule_name,
                "column": rule.column,
                "severity": rule.severity.value if hasattr(rule.severity, "value") else str(rule.severity),
                "message": rule.message,
                "expected_value": getattr(rule, "expected_value", "N/A"),
                "actual_value": getattr(rule, "actual_value", "N/A"),
            }
            with st.expander(f"{rule.rule_name} on `{rule.column}`", expanded=False):
                if st.button("Get AI Explanation", key=f"ai_rule_{i}_{rule.rule_name}"):
                    with st.spinner("Analysing violation..."):
                        record_dict_for_rule = {
                            "record_id": record.record_id,
                            "source_name": record.source_name,
                            "table_name": record.table_name,
                            "dq_score": record.dq_score,
                            "raw_record": record.raw_record,
                        }
                        dq_exp = ai.dq_assistant.explain_failure(record_dict_for_rule, rule_dict)
                    badge_cls = {
                        "CRITICAL": "🔴",
                        "HIGH": "🟠",
                        "MEDIUM": "🟡",
                        "LOW": "🟢",
                    }.get(dq_exp.risk_level, "⚪")
                    st.markdown(f"**Risk Level**: {badge_cls} {dq_exp.risk_level}")
                    st.markdown(dq_exp.explanation)

    st.divider()

    # ── Comment summary ────────────────────────────────────────────────────────
    if ai.config.is_feature_enabled("comment_summarizer"):
        section_title("Discussion Summary")
        comments_df = provider.get_comments(record.record_id)
        if comments_df.empty:
            st.info("No comments yet — add a comment in the Comments tab first.")
        else:
            if st.button("Summarise Discussion", key="ai_summarise_comments"):
                with st.spinner("Summarising..."):
                    record_dict_for_summary = {
                        "record_id": record.record_id,
                        "source_name": record.source_name,
                        "failed_rules": [{"rule_name": r.rule_name} for r in record.failed_rules],
                    }
                    summary = ai.comment_summarizer.summarize(record_dict_for_summary, comments_df)
                st.markdown(summary.summary)
                st.caption(f"Thread: {summary.thread_count} messages | Participants: {', '.join(summary.participants)}")


inject_global_css()
page_header("Record Review", "Inspect DQ violations and take a steward action", "🔍")

provider = get_data_provider()
service = StewardshipService(provider)
audit_repo = AuditRepository(provider)

current_user = st.session_state.get("current_user", {"name": "Sarah Mitchell", "role": "data_steward"})

# ── Record selector ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Record Selection")
    manual_id = st.text_input(
        "Enter Record ID",
        value=st.session_state.get("selected_record_id", ""),
        placeholder="Paste a record ID…",
        key="review_manual_id",
    )
    if manual_id.strip():
        st.session_state["selected_record_id"] = manual_id.strip()

    if st.button("Clear Selection", key="review_clear"):
        st.session_state.pop("selected_record_id", None)
        st.rerun()

    st.divider()
    st.caption("Select a record from **Pending Validations** or paste a Record ID above.")

record_id = st.session_state.get("selected_record_id")

if not record_id:
    empty_state(
        "No Record Selected",
        "Go to Pending Validations and click a row, or enter a Record ID in the sidebar.",
        "🔍",
    )
    st.stop()

# ── Load record ───────────────────────────────────────────────────────────────
with st.spinner("Loading record…"):
    record = service.get_record(record_id)

if record is None:
    st.error(f"Record `{record_id}` not found. It may have been deleted or the ID is incorrect.")
    st.stop()

# ── Detail card ───────────────────────────────────────────────────────────────
record_detail_card(record)

# ── Tabs: Raw Data | Actions | Comments | History | AI Insights ───────────────
tab_raw, tab_action, tab_comments, tab_history, tab_ai = st.tabs([
    "📄 Raw Record", "⚡ Take Action", "💬 Comments", "📜 History", "🤖 AI Insights"
])

with tab_raw:
    raw_record_viewer(record.raw_record)

with tab_action:
    stewards = service.get_stewards()
    result = action_panel(record, current_user, stewards)
    if result:
        action = result["action"]
        comment = result.get("comment", "")
        try:
            if action == "approve":
                service.approve(record_id, current_user["name"], comment)
                st.success("Record approved and queued for promotion to Gold.")
            elif action == "reject":
                service.reject(record_id, current_user["name"], comment)
                st.warning("Record rejected and excluded from Gold layer.")
            elif action == "request_correction":
                service.request_correction(record_id, current_user["name"], comment)
                st.info("Correction requested — source team has been notified.")
            elif action == "assign":
                service.assign(record_id, result.get("assigned_to", ""), current_user["name"])
                st.success(f"Record assigned to {result.get('assigned_to')}.")
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))

with tab_comments:
    comments_df = service.get_record_comments(record_id)
    new_comment = comment_thread(comments_df, record_id, current_user)
    if new_comment:
        try:
            service.add_comment(
                record_id=record_id,
                author=current_user["name"],
                message=new_comment["message"],
                parent_comment_id=new_comment.get("parent_comment_id"),
            )
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))

with tab_history:
    actions_df = audit_repo.get_actions_for_record(record_id)
    action_timeline(actions_df)

with tab_ai:
    _render_ai_insights(record, provider)
