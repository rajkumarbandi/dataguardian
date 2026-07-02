"""
DataGuardian Stewardship Portal — Pipeline Metrics page.

DQ score trends, row volume, success rates, and run history
across all data sources.
"""

import pandas as pd
import streamlit as st

from src.app.data.provider import get_data_provider
from src.app.services.pipeline_service import PipelineService
from src.app.ui.charts import dq_score_trend, volume_bar
from src.app.ui.styles import inject_global_css, page_header, section_title

inject_global_css()
page_header("Pipeline Metrics", "Data quality trends and pipeline execution health", "📈")

provider = get_data_provider()
service = PipelineService(provider)

sources = ["All Sources"] + ["customers", "orders", "products", "order_items"]

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    source_choice = st.selectbox("Source", sources, key="metrics_source")
    selected_source = None if source_choice == "All Sources" else source_choice
    run_limit = st.slider("Max Runs to Display", 10, 50, 20, step=5, key="metrics_limit")

# ── Overall health metrics ────────────────────────────────────────────────────
health = service.get_overall_health()
if health.get("status") != "no_data":
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Pipeline Runs", f"{health['total_runs']:,}")
    c2.metric("Total Rows Processed", f"{health['total_rows_processed']:,}")
    c3.metric("Avg DQ Score", f"{health['avg_dq_score']:.1%}")
    c4.metric("Overall Success Rate", f"{health['overall_success_rate']:.1%}")
    st.markdown("")

# ── Source-level metrics table ────────────────────────────────────────────────
section_title("Source Health Summary")
all_metrics = service.get_all_metrics()
if all_metrics:
    rows = []
    for m in all_metrics:
        rows.append({
            "Source": m.source_name,
            "Total Runs": m.total_runs,
            "Success Rate": f"{m.success_rate:.1%}",
            "Avg DQ Score": f"{m.avg_dq_score:.1%}",
            "Avg Duration": f"{m.avg_duration_seconds:.0f}s",
            "Rows Processed": f"{m.total_rows_processed:,}",
            "Failed Rows": f"{m.total_failed_rows:,}",
            "Last Run": m.last_run_at.strftime("%Y-%m-%d %H:%M") if m.last_run_at else "—",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.markdown("")

# ── Charts ────────────────────────────────────────────────────────────────────
col_dq, col_vol = st.columns([1, 1])

with col_dq:
    trend_df = service.get_dq_trend(source_name=selected_source, limit=run_limit)
    title = f"DQ Score Trend — {selected_source}" if selected_source else "DQ Score Trend — All Sources"
    fig_dq = dq_score_trend(trend_df, title=title)
    st.plotly_chart(fig_dq, use_container_width=True)

with col_vol:
    vol_df = service.get_volume_trend(source_name=selected_source, limit=run_limit)
    title_vol = f"Row Volume — {selected_source}" if selected_source else "Row Volume — All Sources"
    fig_vol = volume_bar(vol_df, source_name=selected_source, title=title_vol)
    st.plotly_chart(fig_vol, use_container_width=True)

# ── Run history table ─────────────────────────────────────────────────────────
section_title("Pipeline Run Log")
runs_df = service.get_runs(source_name=selected_source, limit=run_limit)

if not runs_df.empty:
    display = runs_df[[
        "run_id", "source_name", "batch_id", "status",
        "bronze_rows_read", "silver_rows_written", "failed_rows",
        "dq_score", "duration_seconds", "start_time",
    ]].copy()
    display["dq_score"] = display["dq_score"].apply(
        lambda x: f"{float(x):.1%}" if x is not None and str(x) != "nan" else "—"
    )
    display["duration_seconds"] = display["duration_seconds"].apply(
        lambda x: f"{float(x):.0f}s" if x is not None and str(x) != "nan" else "—"
    )
    display["start_time"] = pd.to_datetime(display["start_time"]).dt.strftime("%Y-%m-%d %H:%M")
    display.columns = [
        "Run ID", "Source", "Batch", "Status",
        "Bronze Rows", "Silver Rows", "Failed", "DQ Score", "Duration", "Started",
    ]
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Status": st.column_config.TextColumn("Status", width="small"),
            "DQ Score": st.column_config.TextColumn("DQ Score", width="small"),
            "Duration": st.column_config.TextColumn("Duration", width="small"),
        },
    )
else:
    st.info("No pipeline runs found for the selected source.")
