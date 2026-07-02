"""
Reusable UI components for the DataGuardian Stewardship Portal.

Each component renders a self-contained Streamlit widget or HTML block.
No business logic here — pages call services, then pass results to components.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.models.stewardship import FailedRule, StewardshipAction, StewardshipRecord
from src.app.ui.styles import C, page_header, score_class, section_title, severity_badge, status_badge


# ── KPI Cards ─────────────────────────────────────────────────────────────────

def kpi_row(
    pending: int,
    approved: int,
    rejected: int,
    correction: int,
    avg_dq_score: float,
) -> None:
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Pending Review", f"{pending:,}", help="Records awaiting steward action")
    c2.metric("Approved", f"{approved:,}", help="Records approved and promoted to Gold")
    c3.metric("Rejected", f"{rejected:,}", help="Records rejected and excluded from Gold")
    c4.metric("Correction Requested", f"{correction:,}", help="Records sent back to source for correction")
    c5.metric("Avg DQ Score", f"{avg_dq_score:.1%}", help="Average data quality score across recent pipeline runs")


# ── Status badge ──────────────────────────────────────────────────────────────

def render_status_badge(status: str) -> None:
    st.markdown(status_badge(status), unsafe_allow_html=True)


# ── Pending records table ─────────────────────────────────────────────────────

def pending_records_table(df: pd.DataFrame, page_size: int = 25) -> str | None:
    """
    Render a paginated table of stewardship records.
    Returns the record_id selected by the user, or None.
    """
    if df.empty:
        st.info("No records match the current filters.")
        return None

    # Pagination
    total = len(df)
    if "table_page" not in st.session_state:
        st.session_state["table_page"] = 0
    page = st.session_state["table_page"]
    max_page = max(0, (total - 1) // page_size)
    page = min(page, max_page)
    st.session_state["table_page"] = page

    start = page * page_size
    end = start + page_size
    page_df = df.iloc[start:end].copy()

    # Format columns for display
    display = pd.DataFrame({
        "Source": page_df["source_name"],
        "Batch": page_df["batch_id"].str.replace("batch_", "", regex=False),
        "Status": page_df["status"].apply(lambda s: s.replace("_", " ")),
        "DQ Score": page_df["dq_score"].apply(lambda x: f"{x:.1%}"),
        "Violations": page_df["violation_count"],
        "Assigned To": page_df["assigned_to"].fillna("—"),
        "Created": pd.to_datetime(page_df["created_at"]).dt.strftime("%Y-%m-%d %H:%M"),
    })
    display.index = page_df["record_id"].values  # type: ignore[assignment]

    # Column config
    col_cfg = {
        "Source": st.column_config.TextColumn("Source", width="small"),
        "Batch": st.column_config.TextColumn("Batch", width="small"),
        "Status": st.column_config.TextColumn("Status", width="medium"),
        "DQ Score": st.column_config.TextColumn("DQ Score", width="small"),
        "Violations": st.column_config.NumberColumn("Violations", width="small"),
        "Assigned To": st.column_config.TextColumn("Assigned To", width="medium"),
        "Created": st.column_config.TextColumn("Created", width="medium"),
    }

    selected = st.dataframe(
        display,
        column_config=col_cfg,
        use_container_width=True,
        hide_index=False,
        selection_mode="single-row",
        on_select="rerun",
        key="records_table",
    )

    # Pagination controls
    col_prev, col_info, col_next = st.columns([1, 4, 1])
    with col_prev:
        if st.button("← Prev", disabled=page == 0, key="tbl_prev"):
            st.session_state["table_page"] = page - 1
            st.rerun()
    with col_info:
        st.caption(f"Page {page + 1} of {max_page + 1} — {total:,} records total")
    with col_next:
        if st.button("Next →", disabled=page >= max_page, key="tbl_next"):
            st.session_state["table_page"] = page + 1
            st.rerun()

    # Return selected record_id
    rows = selected.get("selection", {}).get("rows", [])
    if rows:
        row_idx = rows[0]
        return str(display.index[row_idx])
    return None


# ── Record detail card ────────────────────────────────────────────────────────

def record_detail_card(record: StewardshipRecord) -> None:
    """Render the full detail view of a single stewardship record."""
    # Header row
    col_id, col_status = st.columns([3, 1])
    with col_id:
        st.markdown(f"**Record ID:** `{record.record_id}`")
        st.caption(f"Source: **{record.source_name}** | Batch: `{record.batch_id}` | Run: `{record.run_id}`")
    with col_status:
        render_status_badge(record.status.value)
        score_cls = score_class(record.dq_score)
        st.markdown(
            f'<span class="{score_cls}" style="font-size:1.4rem">{record.dq_score:.1%}</span> DQ Score',
            unsafe_allow_html=True,
        )

    st.divider()

    col_meta, col_rules = st.columns([1, 1])

    with col_meta:
        section_title("Record Metadata")
        fields = [
            ("Table", record.table_name),
            ("Assigned To", record.assigned_to or "Unassigned"),
            ("Violations", str(record.violation_count)),
            ("Ingested At", record.ingested_at.strftime("%Y-%m-%d %H:%M") if record.ingested_at else "—"),
            ("Created At", record.created_at.strftime("%Y-%m-%d %H:%M") if record.created_at else "—"),
            ("Reviewed By", record.reviewed_by or "—"),
            ("Reviewed At", record.reviewed_at.strftime("%Y-%m-%d %H:%M") if record.reviewed_at else "—"),
        ]
        for label, value in fields:
            st.markdown(
                f'<div class="dg-field-row"><span class="dg-field-label">{label}</span>'
                f'<span class="dg-field-value">{value}</span></div>',
                unsafe_allow_html=True,
            )

    with col_rules:
        section_title("DQ Violations")
        if not record.failed_rules:
            st.caption("No violations recorded.")
        else:
            failed_rules_cards(record.failed_rules)


def failed_rules_cards(rules: list[FailedRule]) -> None:
    for rule in rules:
        with st.container():
            sev_html = severity_badge(rule.severity.value)
            st.markdown(
                f"{sev_html} &nbsp;<strong>{rule.rule_name}</strong> on <code>{rule.column_name}</code>",
                unsafe_allow_html=True,
            )
            st.caption(rule.message)
            if rule.expected or rule.actual:
                c1, c2 = st.columns(2)
                c1.caption(f"Expected: `{rule.expected}`")
                c2.caption(f"Actual: `{rule.actual}`")
            st.markdown("---")


def raw_record_viewer(raw: dict) -> None:
    """Render the raw record as a readable key-value table."""
    section_title("Raw Record")
    if not raw:
        st.caption("No raw record data available.")
        return
    rows = [(k, str(v) if v is not None else "NULL") for k, v in raw.items() if not k.startswith("_")]
    system_rows = [(k, str(v)) for k, v in raw.items() if k.startswith("_")]
    df = pd.DataFrame(rows, columns=["Field", "Value"])
    st.dataframe(df, use_container_width=True, hide_index=True)
    if system_rows:
        with st.expander("System Fields"):
            sys_df = pd.DataFrame(system_rows, columns=["Field", "Value"])
            st.dataframe(sys_df, use_container_width=True, hide_index=True)


# ── Action panel ──────────────────────────────────────────────────────────────

def action_panel(
    record: StewardshipRecord,
    current_user: dict,
    stewards: list[str],
) -> dict | None:
    """
    Render the steward action panel. Returns a dict describing the action taken,
    or None if no action was submitted this render cycle.
    """
    if record.status.value != "PENDING":
        st.info(
            f"This record has already been **{record.status.value.replace('_', ' ')}** "
            f"and cannot be actioned again unless it is re-opened."
        )
        return None

    section_title("Steward Actions")

    tab_approve, tab_reject, tab_correct, tab_assign = st.tabs([
        "✅ Approve", "❌ Reject", "🔄 Request Correction", "👤 Assign"
    ])

    with tab_approve:
        justification = st.text_area(
            "Justification *",
            placeholder="Describe why this record meets quality standards...",
            key=f"approve_comment_{record.record_id}",
            height=100,
        )
        if st.button("Approve Record", type="primary", key=f"btn_approve_{record.record_id}"):
            if not justification.strip():
                st.error("A justification comment is required.")
            else:
                return {"action": "approve", "comment": justification}

    with tab_reject:
        reason = st.text_area(
            "Rejection Reason *",
            placeholder="Describe why this record cannot be promoted...",
            key=f"reject_comment_{record.record_id}",
            height=100,
        )
        if st.button("Reject Record", type="primary", key=f"btn_reject_{record.record_id}"):
            if not reason.strip():
                st.error("A rejection reason is required.")
            else:
                return {"action": "reject", "comment": reason}

    with tab_correct:
        instructions = st.text_area(
            "Correction Instructions *",
            placeholder="Describe what must be corrected in the source system...",
            key=f"correct_comment_{record.record_id}",
            height=100,
        )
        if st.button("Request Correction", type="primary", key=f"btn_correct_{record.record_id}"):
            if not instructions.strip():
                st.error("Correction instructions are required.")
            else:
                return {"action": "request_correction", "comment": instructions}

    with tab_assign:
        assignee = st.selectbox(
            "Assign to Steward",
            options=stewards,
            key=f"assign_to_{record.record_id}",
        )
        if st.button("Assign Record", key=f"btn_assign_{record.record_id}"):
            return {"action": "assign", "assigned_to": assignee, "comment": f"Assigned to {assignee}"}

    return None


# ── Threaded comments ─────────────────────────────────────────────────────────

def comment_thread(
    comments_df: pd.DataFrame,
    record_id: str,
    current_user: dict,
) -> dict | None:
    """
    Render threaded comments. Returns a new comment dict if one was submitted, else None.
    """
    section_title("Discussion")

    # Render existing comments
    if comments_df.empty:
        st.caption("No comments yet. Be the first to add one.")
    else:
        top_level = comments_df[comments_df["parent_comment_id"].isna()]
        replies = comments_df[comments_df["parent_comment_id"].notna()]

        for _, comment in top_level.iterrows():
            ts = pd.to_datetime(comment["created_at"]).strftime("%b %d, %Y %H:%M")
            st.markdown(
                f'<div class="dg-comment-thread">'
                f'<strong>{comment["author"]}</strong> &nbsp;<span style="color:{C["text_muted"]};font-size:0.8rem">{ts}</span>'
                f'<p style="margin:0.25rem 0 0.5rem 0">{comment["message"]}</p>'
                f'</div>',
                unsafe_allow_html=True,
            )
            # Replies to this comment
            for _, reply in replies[replies["parent_comment_id"] == comment["comment_id"]].iterrows():
                r_ts = pd.to_datetime(reply["created_at"]).strftime("%b %d, %Y %H:%M")
                st.markdown(
                    f'<div class="dg-comment-reply">'
                    f'↳ <strong>{reply["author"]}</strong> &nbsp;<span style="color:{C["text_muted"]};font-size:0.8rem">{r_ts}</span>'
                    f'<p style="margin:0.25rem 0 0">{reply["message"]}</p>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # New comment form
    st.markdown("")
    with st.form(key=f"comment_form_{record_id}", clear_on_submit=True):
        message = st.text_area(
            "Add a comment",
            placeholder="Share context, ask a question, or provide additional information...",
            height=80,
            label_visibility="collapsed",
        )
        submitted = st.form_submit_button("Post Comment")
        if submitted:
            if not message.strip():
                st.error("Comment cannot be empty.")
            else:
                return {
                    "record_id": record_id,
                    "author": current_user.get("name", "Unknown"),
                    "message": message.strip(),
                    "parent_comment_id": None,
                }
    return None


# ── Action history timeline ───────────────────────────────────────────────────

def action_timeline(actions_df: pd.DataFrame) -> None:
    """Render an action history timeline for a record."""
    section_title("Action History")
    if actions_df.empty:
        st.caption("No actions recorded yet.")
        return
    icon_map = {
        "APPROVE": "✅",
        "REJECT": "❌",
        "REQUEST_CORRECTION": "🔄",
        "COMMENT": "💬",
        "ASSIGN": "👤",
        "REASSIGN": "🔀",
    }
    for _, action in actions_df.sort_values("action_timestamp", ascending=False).iterrows():
        ts = pd.to_datetime(action["action_timestamp"]).strftime("%b %d, %Y %H:%M")
        icon = icon_map.get(str(action["action_type"]), "•")
        comment = str(action.get("comment") or "")
        st.markdown(
            f"**{icon} {action['action_type'].replace('_', ' ')}** by **{action['performed_by']}** &nbsp;"
            f'<span style="color:{C["text_muted"]};font-size:0.8rem">{ts}</span>',
            unsafe_allow_html=True,
        )
        if comment:
            st.caption(f'"{comment}"')
        st.divider()


# ── Empty state ───────────────────────────────────────────────────────────────

def empty_state(title: str, body: str, icon: str = "📭") -> None:
    st.markdown(
        f'<div style="text-align:center;padding:3rem 1rem;color:{C["text_muted"]}">'
        f'<div style="font-size:3rem">{icon}</div>'
        f'<h3 style="margin:0.5rem 0">{title}</h3>'
        f'<p style="margin:0">{body}</p>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ── Filter sidebar helpers ────────────────────────────────────────────────────

def source_filter(sources: list[str], key: str = "filter_source") -> str | None:
    options = ["All Sources"] + sorted(sources)
    choice = st.selectbox("Source", options, key=key)
    return None if choice == "All Sources" else choice


def status_filter(key: str = "filter_status") -> str | None:
    options = ["All Statuses", "PENDING", "APPROVED", "REJECTED", "CORRECTION_REQUESTED"]
    choice = st.selectbox("Status", options, key=key)
    return None if choice == "All Statuses" else choice


def steward_filter(stewards: list[str], key: str = "filter_steward") -> str | None:
    options = ["All Stewards"] + sorted(stewards)
    choice = st.selectbox("Assigned To", options, key=key)
    return None if choice == "All Stewards" else choice
