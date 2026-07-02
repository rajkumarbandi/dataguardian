"""Global CSS injection for the DataGuardian stewardship portal."""

from __future__ import annotations

import streamlit as st

# ── Palette ─────────────────────────────────────────────────────────────────
C = {
    "navy": "#0F1B2D",
    "navy_mid": "#1A2C45",
    "navy_border": "#1E3A5F",
    "brand": "#1565C0",
    "brand_light": "#42A5F5",
    "success": "#2E7D32",
    "success_bg": "#E8F5E9",
    "success_light": "#4CAF50",
    "warning": "#E65100",
    "warning_bg": "#FFF3E0",
    "warning_light": "#FF9800",
    "danger": "#B71C1C",
    "danger_bg": "#FFEBEE",
    "danger_light": "#E53935",
    "correction": "#6A1B9A",
    "correction_bg": "#F3E5F5",
    "correction_light": "#9C27B0",
    "bg": "#F0F4F8",
    "card": "#FFFFFF",
    "text": "#1A202C",
    "text_muted": "#6B7280",
    "border": "#E2E8F0",
    "hover": "#F7FAFC",
}

STATUS_COLORS: dict[str, dict[str, str]] = {
    "PENDING": {"bg": C["warning_bg"], "fg": C["warning"], "dot": C["warning_light"]},
    "APPROVED": {"bg": C["success_bg"], "fg": C["success"], "dot": C["success_light"]},
    "REJECTED": {"bg": C["danger_bg"], "fg": C["danger"], "dot": C["danger_light"]},
    "CORRECTION_REQUESTED": {"bg": C["correction_bg"], "fg": C["correction"], "dot": C["correction_light"]},
}

SEVERITY_COLORS: dict[str, str] = {
    "error": C["danger"],
    "warning": C["warning"],
    "info": C["brand"],
}


_CSS = f"""
<style>
/* ── Global ────────────────────────────────────────────────────────────── */
html, body, [class*="css"] {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}}
.stApp {{
    background-color: {C['bg']};
}}

/* ── Sidebar ────────────────────────────────────────────────────────────── */
section[data-testid="stSidebar"] {{
    background: linear-gradient(180deg, {C['navy']} 0%, {C['navy_mid']} 100%) !important;
    border-right: 1px solid {C['navy_border']};
}}
section[data-testid="stSidebar"] * {{
    color: #CBD5E1 !important;
}}
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {{
    color: #F1F5F9 !important;
    font-weight: 700;
}}
section[data-testid="stSidebar"] .stRadio label,
section[data-testid="stSidebar"] .stSelectbox label {{
    color: #94A3B8 !important;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}}
section[data-testid="stSidebar"] hr {{
    border-color: {C['navy_border']} !important;
    margin: 0.75rem 0;
}}
section[data-testid="stSidebar"] .stButton > button {{
    background: transparent;
    border: 1px solid {C['navy_border']};
    color: #94A3B8 !important;
    width: 100%;
    text-align: left;
}}

/* ── Metric cards ────────────────────────────────────────────────────────── */
div[data-testid="metric-container"] {{
    background: {C['card']};
    border: 1px solid {C['border']};
    border-radius: 10px;
    padding: 1rem 1.25rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}}
div[data-testid="metric-container"] [data-testid="stMetricLabel"] {{
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: {C['text_muted']};
}}
div[data-testid="metric-container"] [data-testid="stMetricValue"] {{
    font-size: 1.75rem;
    font-weight: 700;
    color: {C['text']};
}}

/* ── Buttons ──────────────────────────────────────────────────────────── */
.stButton > button {{
    border-radius: 6px;
    font-weight: 500;
    font-size: 0.875rem;
    transition: all 0.15s ease;
    border: 1px solid {C['border']};
}}
.stButton > button:hover {{
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.12);
}}

/* ── DataFrames / tables ─────────────────────────────────────────────── */
.stDataFrame {{
    border: 1px solid {C['border']};
    border-radius: 8px;
    overflow: hidden;
}}

/* ── Expanders ──────────────────────────────────────────────────────── */
details summary {{
    font-weight: 600;
    color: {C['text']};
}}

/* ── Forms ───────────────────────────────────────────────────────────── */
.stTextArea textarea, .stTextInput input, .stSelectbox select {{
    border-radius: 6px;
    border-color: {C['border']};
}}

/* ── Dividers ──────────────────────────────────────────────────────── */
hr {{
    border-color: {C['border']};
    margin: 1rem 0;
}}

/* ── Custom component classes ──────────────────────────────────────── */
.dg-card {{
    background: {C['card']};
    border: 1px solid {C['border']};
    border-radius: 10px;
    padding: 1.25rem 1.5rem;
    margin-bottom: 1rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}}
.dg-card-accent-pending {{
    border-left: 4px solid {C['warning_light']};
}}
.dg-card-accent-approved {{
    border-left: 4px solid {C['success_light']};
}}
.dg-card-accent-rejected {{
    border-left: 4px solid {C['danger_light']};
}}
.dg-card-accent-correction {{
    border-left: 4px solid {C['correction_light']};
}}

.dg-badge {{
    display: inline-block;
    padding: 0.2rem 0.65rem;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
}}
.dg-badge-pending   {{ background:{C['warning_bg']};    color:{C['warning']};    }}
.dg-badge-approved  {{ background:{C['success_bg']};    color:{C['success']};    }}
.dg-badge-rejected  {{ background:{C['danger_bg']};     color:{C['danger']};     }}
.dg-badge-correction {{ background:{C['correction_bg']}; color:{C['correction']}; }}
.dg-badge-error     {{ background:{C['danger_bg']};     color:{C['danger']};     }}
.dg-badge-warning   {{ background:{C['warning_bg']};    color:{C['warning']};    }}
.dg-badge-info      {{ background:#EFF6FF;              color:{C['brand']};      }}

.dg-page-header {{
    padding: 0.5rem 0 1.5rem 0;
    border-bottom: 1px solid {C['border']};
    margin-bottom: 1.5rem;
}}
.dg-page-header h1 {{
    margin: 0 0 0.25rem 0;
    font-size: 1.6rem;
    font-weight: 700;
    color: {C['text']};
}}
.dg-page-header p {{
    margin: 0;
    color: {C['text_muted']};
    font-size: 0.9rem;
}}

.dg-section-title {{
    font-size: 0.8rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: {C['text_muted']};
    margin: 1.25rem 0 0.5rem 0;
    padding-bottom: 0.35rem;
    border-bottom: 1px solid {C['border']};
}}

.dg-field-row {{
    display: flex;
    justify-content: space-between;
    padding: 0.4rem 0;
    border-bottom: 1px solid {C['border']};
    font-size: 0.875rem;
}}
.dg-field-label {{
    color: {C['text_muted']};
    font-weight: 500;
    min-width: 160px;
}}
.dg-field-value {{
    color: {C['text']};
    font-weight: 400;
    text-align: right;
}}

.dg-score-high   {{ color: {C['success']};  font-weight: 700; }}
.dg-score-medium {{ color: {C['warning']};  font-weight: 700; }}
.dg-score-low    {{ color: {C['danger']};   font-weight: 700; }}

.dg-comment-thread {{
    border-left: 3px solid {C['border']};
    padding-left: 1rem;
    margin: 0.5rem 0;
}}
.dg-comment-reply {{
    border-left: 3px solid {C['brand_light']};
    padding-left: 1rem;
    margin: 0.5rem 0 0.5rem 1.5rem;
    background: #F8FAFF;
    border-radius: 0 6px 6px 0;
    padding-top: 0.5rem;
    padding-bottom: 0.5rem;
}}

.dg-action-btn-approve  {{ background:{C['success']};    color:white; border:none; }}
.dg-action-btn-reject   {{ background:{C['danger']};     color:white; border:none; }}
.dg-action-btn-correct  {{ background:{C['correction']}; color:white; border:none; }}
.dg-action-btn-comment  {{ background:{C['brand']};      color:white; border:none; }}
</style>
"""


def inject_global_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def status_badge(status: str) -> str:
    key = status.lower().replace("_requested", "").replace("correction", "correction")
    if "correction" in status.lower():
        key = "correction"
    return f'<span class="dg-badge dg-badge-{key}">{status.replace("_", " ")}</span>'


def severity_badge(severity: str) -> str:
    return f'<span class="dg-badge dg-badge-{severity.lower()}">{severity.upper()}</span>'


def score_class(score: float) -> str:
    if score >= 0.85:
        return "dg-score-high"
    if score >= 0.65:
        return "dg-score-medium"
    return "dg-score-low"


def page_header(title: str, subtitle: str = "", icon: str = "") -> None:
    icon_html = f"{icon}&nbsp;" if icon else ""
    subtitle_html = f"<p>{subtitle}</p>" if subtitle else ""
    st.markdown(
        f'<div class="dg-page-header"><h1>{icon_html}{title}</h1>{subtitle_html}</div>',
        unsafe_allow_html=True,
    )


def section_title(text: str) -> None:
    st.markdown(f'<div class="dg-section-title">{text}</div>', unsafe_allow_html=True)
