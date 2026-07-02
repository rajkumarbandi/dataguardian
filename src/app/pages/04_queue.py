"""
DataGuardian Stewardship Portal — My Approval Queue page.

Shows all records assigned to the currently logged-in steward,
across all statuses, with quick-action buttons to navigate to review.
"""

import pandas as pd
import streamlit as st

from src.app.data.provider import get_data_provider
from src.app.services.stewardship_service import StewardshipService
from src.app.ui.components import empty_state, source_filter, status_filter
from src.app.ui.styles import inject_global_css, page_header, section_title, status_badge

inject_global_css()
page_header("My Approval Queue", "Records assigned to you across all statuses", "✅")

provider = get_data_provider()
service = StewardshipService(provider)

current_user = st.session_state.get("current_user", {"name": "Sarah Mitchell", "role": "data_steward"})
sources = service.get_sources()

# ── Sidebar: switch active steward for demo ───────────────────────────────────
with st.sidebar:
    st.markdown("### Queue Settings")
    all_stewards = service.get_stewards()
    steward_name = st.selectbox(
        "Viewing queue for",
        options=all_stewards,
        index=all_stewards.index(current_user["name"]) if current_user["name"] in all_stewards else 0,
        key="queue_steward",
    )
    st.divider()
    filter_source = source_filter(sources, key="queue_source")
    filter_status = status_filter(key="queue_status")

# ── Load queue ────────────────────────────────────────────────────────────────
df = service.list_my_queue(steward_name)

if filter_source:
    df = df[df["source_name"] == filter_source]
if filter_status:
    df = df[df["status"] == filter_status]

# ── Summary metrics ───────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total in Queue", f"{len(df):,}")
if not df.empty:
    c2.metric("Pending Action", f"{len(df[df['status'] == 'PENDING']):,}")
    c3.metric("In Correction", f"{len(df[df['status'] == 'CORRECTION_REQUESTED']):,}")
    c4.metric("Resolved", f"{len(df[df['status'].isin(['APPROVED', 'REJECTED'])]):,}")

st.markdown("")

if df.empty:
    empty_state(
        "Queue Empty",
        f"There are no records assigned to {steward_name} matching the current filters.",
        "✨",
    )
else:
    # ── Pending section ───────────────────────────────────────────────────────
    pending = df[df["status"] == "PENDING"]
    if not pending.empty:
        section_title(f"Pending Action ({len(pending)})")
        for _, row in pending.iterrows():
            with st.container():
                c_meta, c_score, c_btn = st.columns([4, 1, 1])
                with c_meta:
                    st.markdown(
                        f"**{row['source_name']}** &nbsp;`{str(row['record_id'])[:12]}…`  \n"
                        f"Batch: `{row['batch_id']}` · Violations: **{row['violation_count']}**"
                    )
                    created = pd.to_datetime(row["created_at"]).strftime("%b %d, %Y")
                    st.caption(f"Created: {created}")
                with c_score:
                    dq = float(row["dq_score"])
                    color = "#2E7D32" if dq >= 0.85 else "#E65100" if dq >= 0.65 else "#B71C1C"
                    st.markdown(
                        f'<div style="text-align:center;padding-top:0.5rem;font-size:1.2rem;font-weight:700;color:{color}">'
                        f'{dq:.1%}</div>',
                        unsafe_allow_html=True,
                    )
                with c_btn:
                    if st.button("Review →", key=f"queue_review_{row['record_id']}"):
                        st.session_state["selected_record_id"] = str(row["record_id"])
                        st.switch_page("pages/03_review.py")
                st.divider()

    # ── Correction requested section ──────────────────────────────────────────
    correction = df[df["status"] == "CORRECTION_REQUESTED"]
    if not correction.empty:
        section_title(f"Awaiting Correction ({len(correction)})")
        for _, row in correction.iterrows():
            c_meta, c_badge = st.columns([5, 1])
            with c_meta:
                st.markdown(f"**{row['source_name']}** `{str(row['record_id'])[:12]}…`")
                reviewed = pd.to_datetime(row["reviewed_at"]).strftime("%b %d, %Y") if row.get("reviewed_at") else "—"
                st.caption(f"Sent for correction: {reviewed} by {row.get('reviewed_by', '—')}")
            with c_badge:
                st.markdown(status_badge("CORRECTION_REQUESTED"), unsafe_allow_html=True)
            st.divider()

    # ── Resolved section ──────────────────────────────────────────────────────
    resolved = df[df["status"].isin(["APPROVED", "REJECTED"])]
    if not resolved.empty:
        with st.expander(f"Resolved Records ({len(resolved)})", expanded=False):
            display = resolved[["source_name", "batch_id", "status", "dq_score", "reviewed_at", "reviewed_by"]].copy()
            display["dq_score"] = display["dq_score"].apply(lambda x: f"{x:.1%}")
            display["reviewed_at"] = pd.to_datetime(display["reviewed_at"]).dt.strftime("%Y-%m-%d")
            display.columns = ["Source", "Batch", "Status", "DQ Score", "Resolved At", "Resolved By"]
            st.dataframe(display, use_container_width=True, hide_index=True)
