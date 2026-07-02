"""
Plotly chart builders for the DataGuardian Stewardship Portal.

Each builder accepts a pandas DataFrame and returns a plotly Figure.
Pages call these builders and render with st.plotly_chart().
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.app.ui.styles import C

# ── Common layout defaults ────────────────────────────────────────────────────

_LAYOUT = {
    "plot_bgcolor": "white",
    "paper_bgcolor": "white",
    "font": {"family": "Inter, -apple-system, sans-serif", "size": 12, "color": C["text"]},
    "margin": {"l": 16, "r": 16, "t": 40, "b": 16},
    "legend": {"bgcolor": "white", "bordercolor": C["border"], "borderwidth": 1},
    "colorway": [C["brand"], C["success_light"], C["warning_light"], C["danger_light"], C["correction_light"]],
}

_STATUS_COLOR_MAP = {
    "PENDING": C["warning_light"],
    "APPROVED": C["success_light"],
    "REJECTED": C["danger_light"],
    "CORRECTION_REQUESTED": C["correction_light"],
}


def _apply_layout(fig: go.Figure) -> go.Figure:
    fig.update_layout(**_LAYOUT)
    return fig


# ── Status distribution donut ─────────────────────────────────────────────────

def status_donut(counts: dict[str, int], title: str = "Records by Status") -> go.Figure:
    """Donut chart showing record count by status."""
    if not counts:
        return go.Figure()
    labels = list(counts.keys())
    values = list(counts.values())
    colors = [_STATUS_COLOR_MAP.get(l, C["brand"]) for l in labels]
    labels_display = [l.replace("_", " ") for l in labels]

    fig = go.Figure(go.Pie(
        labels=labels_display,
        values=values,
        hole=0.55,
        marker={"colors": colors, "line": {"color": "white", "width": 2}},
        textinfo="label+percent",
        textposition="outside",
        hovertemplate="%{label}: %{value:,} records<extra></extra>",
    ))
    total = sum(values)
    fig.add_annotation(
        text=f"<b>{total:,}</b><br>Total",
        x=0.5, y=0.5, showarrow=False,
        font={"size": 16, "color": C["text"]},
        align="center",
    )
    fig.update_layout(**_LAYOUT, title={"text": title, "font": {"size": 14}}, showlegend=True)
    return fig


# ── Records by source bar ─────────────────────────────────────────────────────

def records_by_source_bar(by_source: dict[str, int], title: str = "Records by Source") -> go.Figure:
    if not by_source:
        return go.Figure()
    df = pd.DataFrame(
        sorted(by_source.items(), key=lambda x: x[1], reverse=True),
        columns=["source", "count"],
    )
    fig = px.bar(
        df, x="source", y="count",
        text="count",
        color="count",
        color_continuous_scale=["#DBEAFE", C["brand"]],
        labels={"source": "Source", "count": "Records"},
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(**_LAYOUT, title={"text": title, "font": {"size": 14}})
    fig.update_coloraxes(showscale=False)
    return fig


# ── DQ violation breakdown bar ────────────────────────────────────────────────

def violation_breakdown_bar(rule_counts: dict[str, int], title: str = "Top DQ Violations") -> go.Figure:
    if not rule_counts:
        return go.Figure()
    df = pd.DataFrame(
        list(rule_counts.items())[:10],
        columns=["rule", "count"],
    ).sort_values("count")
    fig = px.bar(
        df, x="count", y="rule",
        orientation="h",
        text="count",
        color="count",
        color_continuous_scale=["#FEE2E2", C["danger"]],
        labels={"rule": "Rule", "count": "Violations"},
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(**_LAYOUT, title={"text": title, "font": {"size": 14}})
    fig.update_coloraxes(showscale=False)
    return fig


# ── DQ score trend line ───────────────────────────────────────────────────────

def dq_score_trend(df: pd.DataFrame, title: str = "DQ Score Trend") -> go.Figure:
    if df.empty:
        return go.Figure()
    df = df.copy()
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["dq_score_pct"] = df["dq_score"] * 100

    sources = df["source_name"].unique()
    fig = go.Figure()
    palette = [C["brand"], C["success_light"], C["warning_light"], C["correction_light"]]
    for i, source in enumerate(sources):
        src_df = df[df["source_name"] == source].sort_values("start_time")
        color = palette[i % len(palette)]
        fig.add_trace(go.Scatter(
            x=src_df["start_time"],
            y=src_df["dq_score_pct"],
            name=source,
            mode="lines+markers",
            line={"color": color, "width": 2},
            marker={"size": 6, "color": color},
            hovertemplate="%{x|%Y-%m-%d}<br>DQ Score: %{y:.1f}%<extra>" + source + "</extra>",
        ))

    fig.add_hline(
        y=80, line_dash="dash", line_color=C["success"],
        annotation_text="Target (80%)", annotation_position="bottom right",
    )
    fig.update_layout(
        **_LAYOUT,
        title={"text": title, "font": {"size": 14}},
        yaxis={"ticksuffix": "%", "range": [0, 105]},
        xaxis={"title": ""},
    )
    return fig


# ── Row volume grouped bar ────────────────────────────────────────────────────

def volume_bar(df: pd.DataFrame, source_name: str | None = None, title: str = "Row Volume by Run") -> go.Figure:
    if df.empty:
        return go.Figure()
    df = df.copy()
    df["start_time"] = pd.to_datetime(df["start_time"])
    if source_name:
        df = df[df["source_name"] == source_name]
    df = df.sort_values("start_time").tail(15)
    df["run_label"] = df["start_time"].dt.strftime("%m-%d")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Silver (Passed)",
        x=df["run_label"], y=df["silver_rows_written"],
        marker_color=C["success_light"],
        hovertemplate="Passed: %{y:,}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Failed (DQ)",
        x=df["run_label"], y=df["failed_rows"],
        marker_color=C["danger_light"],
        hovertemplate="Failed: %{y:,}<extra></extra>",
    ))
    fig.update_layout(
        **_LAYOUT,
        title={"text": title, "font": {"size": 14}},
        barmode="stack",
        xaxis={"title": "Run Date"},
        yaxis={"title": "Rows"},
    )
    return fig


# ── Approval trend area ───────────────────────────────────────────────────────

def approval_trend(audit_df: pd.DataFrame, title: str = "Approval Activity (7 days)") -> go.Figure:
    if audit_df.empty:
        return go.Figure()
    df = audit_df.copy()
    df["audit_timestamp"] = pd.to_datetime(df["audit_timestamp"])
    df["date"] = df["audit_timestamp"].dt.date
    df = df[df["operation"].isin(["APPROVE", "REJECT", "REQUEST_CORRECTION"])]
    if df.empty:
        return go.Figure()

    pivot = df.groupby(["date", "operation"]).size().unstack(fill_value=0).reset_index()
    fig = go.Figure()
    op_color = {"APPROVE": C["success_light"], "REJECT": C["danger_light"], "REQUEST_CORRECTION": C["correction_light"]}
    for op in ["APPROVE", "REJECT", "REQUEST_CORRECTION"]:
        if op in pivot.columns:
            fig.add_trace(go.Bar(
                name=op.replace("_", " "),
                x=pivot["date"],
                y=pivot[op],
                marker_color=op_color.get(op, C["brand"]),
            ))
    fig.update_layout(
        **_LAYOUT,
        title={"text": title, "font": {"size": 14}},
        barmode="group",
        xaxis={"title": ""},
        yaxis={"title": "Actions"},
    )
    return fig


# ── Steward activity bar ──────────────────────────────────────────────────────

def steward_activity_bar(df: pd.DataFrame, title: str = "Actions by Steward (30 days)") -> go.Figure:
    if df.empty:
        return go.Figure()
    df = df.sort_values("count")
    fig = px.bar(
        df, x="count", y="performed_by",
        orientation="h",
        text="count",
        color="count",
        color_continuous_scale=["#DBEAFE", C["brand"]],
        labels={"performed_by": "Steward", "count": "Actions"},
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(**_LAYOUT, title={"text": title, "font": {"size": 14}})
    fig.update_coloraxes(showscale=False)
    return fig
