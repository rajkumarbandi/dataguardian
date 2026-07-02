"""
DataGuardian Stewardship Portal — Pending Validations page.

Shows all records awaiting steward review with filter controls.
Selecting a record navigates to the Record Review page.
"""

import streamlit as st

from src.app.data.provider import get_data_provider
from src.app.services.stewardship_service import StewardshipService
from src.app.ui.components import (
    empty_state,
    pending_records_table,
    source_filter,
    steward_filter,
)
from src.app.ui.styles import inject_global_css, page_header

inject_global_css()
page_header("Pending Validations", "Records flagged by the DQ pipeline awaiting steward review", "📋")

provider = get_data_provider()
service = StewardshipService(provider)

sources = service.get_sources()
stewards = service.get_stewards()

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    selected_source = source_filter(sources, key="pv_source")
    selected_steward = steward_filter(stewards, key="pv_steward")
    st.divider()
    st.caption("Showing **PENDING** records only. Use Audit History to view resolved records.")
    if st.button("Clear Filters", key="pv_clear"):
        for key in ["pv_source", "pv_steward", "table_page"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────
df = service.list_pending(
    source_name=selected_source,
    assigned_to=selected_steward,
)

# ── Summary row ───────────────────────────────────────────────────────────────
c1, c2, c3 = st.columns(3)
c1.metric("Pending Records", f"{len(df):,}")
if not df.empty:
    unassigned = len(df[df["assigned_to"].fillna("") == ""])
    c2.metric("Unassigned", f"{unassigned:,}")
    avg_score = df["dq_score"].mean()
    c3.metric("Avg DQ Score", f"{avg_score:.1%}")

st.markdown("")

# ── Records table ─────────────────────────────────────────────────────────────
if df.empty:
    empty_state(
        "All Clear",
        "There are no pending records matching the current filters.",
        "🎉",
    )
else:
    selected_id = pending_records_table(df, page_size=25)

    if selected_id:
        st.session_state["selected_record_id"] = selected_id
        st.info(f"Record selected: `{selected_id[:8]}…` — navigate to **Record Review** to take action.")
