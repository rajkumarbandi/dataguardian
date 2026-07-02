"""
DataGuardian Stewardship Portal — Dashboard page.

Provides an operational overview: KPIs, record distribution,
top DQ violations, approval activity, and recent pipeline runs.
"""

import streamlit as st

from src.app.data.provider import get_data_provider
from src.app.services.dashboard_service import DashboardService
from src.app.ui.charts import (
    approval_trend,
    records_by_source_bar,
    status_donut,
    violation_breakdown_bar,
)
from src.app.ui.components import kpi_row
from src.app.ui.styles import inject_global_css, page_header, section_title

inject_global_css()
page_header("Dashboard", "Operational overview of data stewardship activity", "📊")

provider = get_data_provider()
service = DashboardService(provider)

with st.spinner("Loading dashboard…"):
    summary = service.get_summary()

# ── KPI row ───────────────────────────────────────────────────────────────────
kpi_row(
    pending=summary.total_pending,
    approved=summary.total_approved,
    rejected=summary.total_rejected,
    correction=summary.total_correction,
    avg_dq_score=summary.avg_dq_score,
)

st.markdown("")

# ── Row 2: Status donut + Source bar ─────────────────────────────────────────
col_left, col_right = st.columns([1, 1])

with col_left:
    fig = status_donut({
        "PENDING": summary.total_pending,
        "APPROVED": summary.total_approved,
        "REJECTED": summary.total_rejected,
        "CORRECTION_REQUESTED": summary.total_correction,
    })
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    fig2 = records_by_source_bar(summary.records_by_source)
    st.plotly_chart(fig2, use_container_width=True)

# ── Row 3: Violation breakdown + Approval trend ───────────────────────────────
col_viol, col_trend = st.columns([1, 1])

with col_viol:
    fig3 = violation_breakdown_bar(summary.rule_violation_counts)
    st.plotly_chart(fig3, use_container_width=True)

with col_trend:
    fig4 = approval_trend(summary.recent_actions, title="Approval Activity (last 7 days)")
    st.plotly_chart(fig4, use_container_width=True)

# ── Row 4: Snapshot stats + Active stewards ───────────────────────────────────
col_rate, col_resolved, col_stewards = st.columns([1, 1, 2])

with col_rate:
    st.metric(
        "7-Day Approval Rate",
        f"{summary.approval_rate_7d:.0%}",
        help="Approvals as a proportion of all resolved records in the past 7 days",
    )

with col_resolved:
    st.metric(
        "Records Resolved (7d)",
        f"{summary.records_resolved_7d:,}",
        help="Approved + Rejected records in the past 7 days",
    )

with col_stewards:
    section_title("Active Stewards (7 days)")
    if summary.active_stewards:
        st.markdown(" &nbsp;·&nbsp; ".join(f"**{s}**" for s in summary.active_stewards))
    else:
        st.caption("No steward activity in the last 7 days.")

# ── Row 5: Recent pipeline runs ───────────────────────────────────────────────
section_title("Recent Pipeline Runs")

if not summary.pipeline_run_summary.empty:
    import pandas as pd
    display = summary.pipeline_run_summary[[
        "source_name", "batch_id", "status", "bronze_rows_read",
        "silver_rows_written", "failed_rows", "dq_score", "start_time",
    ]].copy()
    display["dq_score"] = display["dq_score"].apply(
        lambda x: f"{x:.1%}" if x is not None and str(x) != "nan" else "—"
    )
    display["start_time"] = pd.to_datetime(display["start_time"]).dt.strftime("%Y-%m-%d %H:%M")
    display.columns = ["Source", "Batch", "Status", "Bronze Rows", "Silver Rows", "Failed", "DQ Score", "Started"]
    st.dataframe(display, use_container_width=True, hide_index=True)
else:
    st.caption("No pipeline run data available.")
