"""
DataGuardian Stewardship Portal — Administration page.

Read-only system administration: environment configuration,
pipeline health, steward activity, and recent audit log.
"""

import pandas as pd
import streamlit as st

from src.app.config.settings import get_settings
from src.app.data.provider import get_data_provider
from src.app.services.admin_service import AdminService
from src.app.ui.charts import steward_activity_bar
from src.app.ui.styles import inject_global_css, page_header, section_title

inject_global_css()
page_header("Administration", "System health, configuration, and steward activity (read-only)", "⚙️")

provider = get_data_provider()
settings = get_settings()
service = AdminService(provider, settings)

st.info("This page is **read-only**. No modifications can be made from this interface.", icon="ℹ️")

# ── System information ────────────────────────────────────────────────────────
section_title("System Information")
sys_info = service.get_system_info()
col1, col2 = st.columns(2)
items = list(sys_info.items())
half = len(items) // 2
with col1:
    for k, v in items[:half]:
        st.markdown(
            f'<div style="display:flex;justify-content:space-between;padding:0.4rem 0;'
            f'border-bottom:1px solid #E2E8F0;font-size:0.875rem">'
            f'<span style="color:#6B7280;font-weight:500">{k}</span>'
            f'<span style="font-weight:600">{v}</span></div>',
            unsafe_allow_html=True,
        )
with col2:
    for k, v in items[half:]:
        st.markdown(
            f'<div style="display:flex;justify-content:space-between;padding:0.4rem 0;'
            f'border-bottom:1px solid #E2E8F0;font-size:0.875rem">'
            f'<span style="color:#6B7280;font-weight:500">{k}</span>'
            f'<span style="font-weight:600">{v}</span></div>',
            unsafe_allow_html=True,
        )

st.markdown("")

# ── Record status overview ────────────────────────────────────────────────────
col_status, col_source = st.columns(2)

with col_status:
    section_title("Record Status Distribution")
    status_counts = service.get_record_status_summary()
    if status_counts:
        total = sum(status_counts.values())
        for status, count in sorted(status_counts.items()):
            pct = count / total * 100 if total > 0 else 0
            st.markdown(f"**{status.replace('_', ' ')}** — {count:,}")
            st.progress(int(pct), text=f"{pct:.0f}%")
    else:
        st.caption("No records found.")

with col_source:
    section_title("Records by Source")
    source_counts = service.get_source_summary()
    if source_counts:
        total_src = sum(source_counts.values())
        for src, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
            pct = count / total_src * 100 if total_src > 0 else 0
            st.markdown(f"**{src}** — {count:,}")
            st.progress(int(pct), text=f"{pct:.0f}%")
    else:
        st.caption("No records found.")

st.markdown("")

# ── Pipeline health ───────────────────────────────────────────────────────────
section_title("Pipeline Health by Source")
pipeline_metrics = service.get_pipeline_health()
if pipeline_metrics:
    rows = [
        {
            "Source": m.source_name,
            "Total Runs": m.total_runs,
            "Success Rate": f"{m.success_rate:.1%}",
            "Avg DQ Score": f"{m.avg_dq_score:.1%}",
            "Avg Duration": f"{m.avg_duration_seconds:.0f}s",
            "Total Rows": f"{m.total_rows_processed:,}",
            "Failed Rows": f"{m.total_failed_rows:,}",
        }
        for m in pipeline_metrics
    ]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.markdown("")

# ── Steward activity chart ────────────────────────────────────────────────────
section_title("Steward Activity (30 days)")
activity_df = service.get_steward_activity(days=30)
if not activity_df.empty:
    fig = steward_activity_bar(activity_df)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.caption("No steward activity in the past 30 days.")

# ── Recent audit entries ──────────────────────────────────────────────────────
section_title("Recent Audit Log (last 100 entries)")
audit_df = service.get_recent_audit_log(limit=100)
if not audit_df.empty:
    display = audit_df[["audit_timestamp", "operation", "performed_by", "entity_id"]].copy()
    display["audit_timestamp"] = pd.to_datetime(display["audit_timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    display["entity_id"] = display["entity_id"].str[:16] + "…"
    display.columns = ["Timestamp", "Operation", "Performed By", "Entity ID"]
    st.dataframe(display, use_container_width=True, hide_index=True)
else:
    st.caption("No audit entries found.")
