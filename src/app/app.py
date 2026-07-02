"""
DataGuardian — Business Data Stewardship Portal

Entry point for the Databricks Apps deployment.
This file must be named app.py and is referenced in app.yaml.
"""

import streamlit as st

from src.app.config.settings import get_settings
from src.app.ui.styles import C, inject_global_css

# ── Page config (must be the first Streamlit command) ────────────────────────
st.set_page_config(
    page_title="DataGuardian — Stewardship Portal",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"About": "DataGuardian Business Data Stewardship Portal v0.9.0"},
)

inject_global_css()

# ── Session state bootstrap ───────────────────────────────────────────────────
_DEFAULTS: dict = {
    "current_user": {
        "name": "Sarah Mitchell",
        "role": "data_steward",
        "email": "s.mitchell@company.com",
    },
    "selected_record_id": None,
    "table_page": 0,
}

for _key, _val in _DEFAULTS.items():
    if _key not in st.session_state:
        st.session_state[_key] = _val

# ── Navigation ────────────────────────────────────────────────────────────────
_pages = {
    "Overview": [
        st.Page("pages/01_dashboard.py", title="Dashboard", icon="📊", default=True),
    ],
    "Stewardship": [
        st.Page("pages/02_pending.py", title="Pending Validations", icon="📋"),
        st.Page("pages/03_review.py", title="Record Review", icon="🔍"),
        st.Page("pages/04_queue.py", title="My Approval Queue", icon="✅"),
    ],
    "Analytics": [
        st.Page("pages/05_history.py", title="Audit History", icon="📜"),
        st.Page("pages/06_metrics.py", title="Pipeline Metrics", icon="📈"),
    ],
    "AI Intelligence": [
        st.Page("pages/08_ai_assistant.py", title="AI Assistant", icon="🤖"),
    ],
    "System": [
        st.Page("pages/07_admin.py", title="Administration", icon="⚙️"),
    ],
}

pg = st.navigation(_pages)

# ── Sidebar brand + user identity ────────────────────────────────────────────
with st.sidebar:
    settings = get_settings()
    env_label = settings.environment.upper()
    env_color = {
        "PROD": C["danger"],
        "QA": C["warning"],
        "TEST": C["brand"],
        "DEV": C["success"],
    }.get(env_label, C["brand"])

    st.markdown(
        f"""
        <div style="padding:0.75rem 0 1rem 0">
            <div style="font-size:1.3rem;font-weight:800;color:#F1F5F9;letter-spacing:-0.5px">
                🛡️ DataGuardian
            </div>
            <div style="font-size:0.7rem;margin-top:0.15rem">
                <span style="background:{env_color};color:white;padding:0.1rem 0.5rem;
                border-radius:4px;font-weight:700;letter-spacing:0.05em">{env_label}</span>
                &nbsp;<span style="color:#64748B">{settings.catalog}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    # ── User switcher (demo only) ─────────────────────────────────────────────
    _stewards = ["Sarah Mitchell", "James Chen", "Emma Davis", "Oliver Brown"]
    current = st.session_state["current_user"]["name"]
    new_user = st.selectbox(
        "Active Steward",
        _stewards,
        index=_stewards.index(current) if current in _stewards else 0,
        key="sidebar_user",
        label_visibility="visible",
    )
    if new_user != current:
        st.session_state["current_user"]["name"] = new_user
        st.rerun()

    st.divider()

    # ── Demo mode badge ───────────────────────────────────────────────────────
    if settings.demo_mode:
        st.markdown(
            '<div style="background:#1E3A5F;border-radius:6px;padding:0.5rem 0.75rem;'
            'font-size:0.75rem;color:#94A3B8;text-align:center">'
            '🧪 Demo Mode — Sample Data</div>',
            unsafe_allow_html=True,
        )

pg.run()
