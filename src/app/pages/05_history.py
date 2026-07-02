"""
DataGuardian Stewardship Portal — Audit History page.

Immutable audit trail of all steward operations with filtering
by steward, operation type, and date range.
"""

import pandas as pd
import streamlit as st

from src.app.data.provider import get_data_provider
from src.app.repository.audit_repo import AuditRepository
from src.app.ui.styles import inject_global_css, page_header, section_title

inject_global_css()
page_header("Audit History", "Immutable log of all steward actions — append-only, never modified", "📜")

provider = get_data_provider()
repo = AuditRepository(provider)

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    days = st.selectbox("Time Window", [7, 14, 30, 60, 90], index=2, key="hist_days")

    operation_options = ["All Operations", "APPROVE", "REJECT", "REQUEST_CORRECTION", "ASSIGN", "REASSIGN"]
    selected_op = st.selectbox("Operation", operation_options, key="hist_op")
    operation = None if selected_op == "All Operations" else selected_op

    # Build steward list from loaded data (lazy)
    st.divider()
    st.caption(f"Showing last **{days}** days of audit activity.")
    if st.button("Reset Filters", key="hist_reset"):
        for k in ["hist_days", "hist_op", "hist_steward"]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading audit log…"):
    df = repo.get_log(operation=operation, days=int(days), limit=500)

if df.empty:
    st.info("No audit entries found for the selected filters.")
    st.stop()

# Fix timestamp type
df["audit_timestamp"] = pd.to_datetime(df["audit_timestamp"])

# ── Steward filter (post-load) ────────────────────────────────────────────────
stewards = sorted(df["performed_by"].dropna().unique().tolist())
with st.sidebar:
    steward_options = ["All Stewards"] + stewards
    selected_steward = st.selectbox("Steward", steward_options, key="hist_steward")
    if selected_steward != "All Stewards":
        df = df[df["performed_by"] == selected_steward]

# ── Summary row ───────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Entries", f"{len(df):,}")
op_counts = df["operation"].value_counts()
c2.metric("Approvals", f"{op_counts.get('APPROVE', 0):,}")
c3.metric("Rejections", f"{op_counts.get('REJECT', 0):,}")
c4.metric("Corrections", f"{op_counts.get('REQUEST_CORRECTION', 0):,}")

st.markdown("")

# ── Activity by steward ───────────────────────────────────────────────────────
col_chart, col_ops = st.columns([2, 1])

with col_chart:
    section_title("Activity Timeline")
    df_daily = df.copy()
    df_daily["date"] = df_daily["audit_timestamp"].dt.date
    pivot = df_daily.groupby(["date", "operation"]).size().unstack(fill_value=0).reset_index()
    import plotly.graph_objects as go
    from src.app.ui.styles import C
    fig = go.Figure()
    op_colors = {
        "APPROVE": C["success_light"],
        "REJECT": C["danger_light"],
        "REQUEST_CORRECTION": C["correction_light"],
        "ASSIGN": C["brand_light"],
        "REASSIGN": C["brand"],
    }
    for op in [c for c in pivot.columns if c != "date"]:
        fig.add_trace(go.Bar(
            name=op.replace("_", " "),
            x=pivot["date"],
            y=pivot[op],
            marker_color=op_colors.get(op, C["brand"]),
        ))
    fig.update_layout(
        barmode="stack",
        margin={"l": 16, "r": 16, "t": 32, "b": 16},
        paper_bgcolor="white",
        plot_bgcolor="white",
        font={"family": "Inter, sans-serif"},
        legend={"bgcolor": "white"},
        yaxis={"title": "Actions"},
    )
    st.plotly_chart(fig, use_container_width=True)

with col_ops:
    section_title("By Operation")
    for op, count in op_counts.items():
        pct = count / len(df) * 100
        st.markdown(f"**{op.replace('_', ' ')}**")
        st.progress(int(pct), text=f"{count:,} ({pct:.0f}%)")

# ── Audit log table ───────────────────────────────────────────────────────────
section_title("Audit Log Entries")

display = df[["audit_timestamp", "operation", "performed_by", "entity_id", "entity_type"]].copy()
display["audit_timestamp"] = display["audit_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
display["entity_id"] = display["entity_id"].str[:12] + "…"
display.columns = ["Timestamp", "Operation", "Performed By", "Entity ID", "Entity Type"]

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Timestamp": st.column_config.TextColumn("Timestamp", width="medium"),
        "Operation": st.column_config.TextColumn("Operation", width="medium"),
        "Performed By": st.column_config.TextColumn("Performed By", width="medium"),
        "Entity ID": st.column_config.TextColumn("Entity ID", width="small"),
        "Entity Type": st.column_config.TextColumn("Entity Type", width="small"),
    },
)

# ── Detail expander ───────────────────────────────────────────────────────────
with st.expander("View Entry Details"):
    entry_idx = st.number_input("Row index (0-based)", min_value=0, max_value=max(len(df) - 1, 0), value=0, step=1)
    if entry_idx < len(df):
        entry = df.iloc[int(entry_idx)]
        import json
        details_raw = entry.get("details") or "{}"
        try:
            details = json.loads(details_raw) if isinstance(details_raw, str) else details_raw
        except (json.JSONDecodeError, TypeError):
            details = {"raw": str(details_raw)}
        st.json({
            "audit_id": entry.get("audit_id", "—"),
            "entity_id": entry.get("entity_id", "—"),
            "operation": entry.get("operation", "—"),
            "performed_by": entry.get("performed_by", "—"),
            "timestamp": str(entry.get("audit_timestamp", "—")),
            "details": details,
        })
