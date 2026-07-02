"""
AI Assistant — DataGuardian AI Intelligence Layer UI.

Four tabs:
  1. Ask Data (Natural Language SQL)
  2. Schema Mapping
  3. Batch Analysis (Root Cause + Profiling)
  4. Usage Stats (token tracker)
"""

from __future__ import annotations

import streamlit as st

from src.ai.components import get_ai_components
from src.ai.duplicate_detector import DuplicateCandidate
from src.app.data.provider import get_data_provider
from src.app.ui.styles import inject_global_css, page_header, section_title


def main() -> None:
    inject_global_css()
    page_header("AI Assistant", "AI-powered data intelligence for enterprise stewardship", "🤖")

    ai = get_ai_components()
    provider = get_data_provider()

    # Provider badge
    badge_color = "#4CAF50" if ai.is_mock else "#2196F3"
    st.markdown(
        f'<span style="background:{badge_color};color:white;padding:2px 8px;border-radius:4px;font-size:12px;">'
        f"{'DEMO — Mock AI' if ai.is_mock else ai.provider_label}"
        f"</span>",
        unsafe_allow_html=True,
    )
    if ai.is_mock:
        st.info(
            "Running in **Demo Mode** — responses use the built-in Mock AI provider (no API key required). "
            "Set `DG_AI_PROVIDER=azure_openai` or `openai` and configure credentials to use a real LLM.",
            icon="ℹ️",
        )

    st.divider()

    tab_sql, tab_schema, tab_batch, tab_stats = st.tabs([
        "💬 Ask Data",
        "🔀 Schema Mapping",
        "📊 Batch Analysis",
        "📈 Usage Stats",
    ])

    # ── Tab 1: Natural Language SQL ───────────────────────────────────────────
    with tab_sql:
        _render_nl_sql(ai, provider)

    # ── Tab 2: Schema Mapping ─────────────────────────────────────────────────
    with tab_schema:
        _render_schema_mapping(ai)

    # ── Tab 3: Batch Analysis ─────────────────────────────────────────────────
    with tab_batch:
        _render_batch_analysis(ai, provider)

    # ── Tab 4: Usage Stats ────────────────────────────────────────────────────
    with tab_stats:
        _render_usage_stats(ai)


# ── NL SQL ─────────────────────────────────────────────────────────────────────

def _render_nl_sql(ai, provider) -> None:
    section_title("Ask Data in Plain English")
    st.markdown(
        "Ask questions about your stewardship data in plain English. "
        "The AI converts your question to a SQL SELECT query and executes it."
    )

    examples = [
        "Show me all pending records with a DQ score below 0.7",
        "Which steward has approved the most records this week?",
        "How many records are pending per source?",
        "Show the 10 most recent APPROVED actions with their justifications",
        "Which batch has the highest failure rate?",
    ]
    st.caption("**Example questions:**")
    example_cols = st.columns(len(examples))
    selected_example = None
    for col, example in zip(example_cols, examples):
        if col.button(example[:35] + "…", key=f"ex_{hash(example)}", use_container_width=True):
            selected_example = example

    question = st.text_input(
        "Your question:",
        value=selected_example or st.session_state.get("nl_sql_question", ""),
        placeholder="e.g. Show me all pending records assigned to Sarah Mitchell",
        key="nl_sql_question",
    )

    col_run, col_clear = st.columns([1, 5])
    run = col_run.button("Ask AI", type="primary", disabled=not question.strip())

    if run and question.strip():
        with st.spinner("Generating SQL..."):
            tables = _get_demo_tables(provider)
            result = ai.natural_language_sql.query(
                question=question,
                tables=tables,
            )

        if result.error:
            st.error(f"Error: {result.error}")
        else:
            st.success(f"Query generated {'(cached)' if result.cached else ''}")
            with st.expander("Generated SQL", expanded=True):
                st.code(result.sql, language="sql")
            if result.explanation:
                st.caption(result.explanation)
            if result.data is not None and not result.data.empty:
                st.markdown(f"**Results** — {result.row_count} rows")
                st.dataframe(result.data, use_container_width=True)
            elif result.data is not None:
                st.info("Query returned no results.")


def _get_demo_tables(provider) -> dict:
    """Build table dict for DuckDB demo execution."""
    try:
        from src.app.data.provider import SampleDataProvider
        if isinstance(provider, SampleDataProvider):
            tables = provider._tables
        else:
            return {}
        return {
            "stewardship_records": tables.get("stewardship_records"),
            "stewardship_actions": tables.get("stewardship_actions"),
            "comments": tables.get("comments"),
            "audit_log": tables.get("audit_log"),
            "pipeline_runs": tables.get("pipeline_runs"),
        }
    except Exception:
        return {}


# ── Schema Mapping ─────────────────────────────────────────────────────────────

def _render_schema_mapping(ai) -> None:
    section_title("AI Schema Mapping")
    st.markdown(
        "Paste source schema field names and target field names — the AI suggests "
        "semantic mappings with confidence scores."
    )

    col_src, col_tgt = st.columns(2)
    with col_src:
        source_text = st.text_area(
            "Source fields (one per line):",
            value="CustomerName\nCust_Name\nEmail_Addr\nDOB\ncust_tier\nRevAmt\nlegacy_flag",
            height=200,
        )
        source_system = st.text_input("Source system:", value="Salesforce CRM")
        domain = st.text_input("Domain:", value="Customer")

    with col_tgt:
        target_text = st.text_area(
            "Target fields (one per line):",
            value="customer_name\nemail\nbirth_date\ncustomer_segment\nannual_revenue\ncreated_at",
            height=200,
        )
        notes = st.text_area("Additional context:", placeholder="Optional notes for the AI", height=90)

    if st.button("Map Fields", type="primary"):
        source_fields = [f.strip() for f in source_text.splitlines() if f.strip()]
        target_fields = [f.strip() for f in target_text.splitlines() if f.strip()]

        if not source_fields or not target_fields:
            st.warning("Please enter at least one source field and one target field.")
            return

        with st.spinner("Analysing schema..."):
            result = ai.schema_mapper.suggest_mappings(
                source_fields=source_fields,
                target_fields=target_fields,
                source_system=source_system,
                domain=domain,
                notes=notes,
            )

        st.success(
            f"Mapping complete — {result.high_confidence_count}/{len(result.mappings)} high-confidence, "
            f"{result.review_required_count} require review "
            f"({'cached' if result.cached else f'{result.prompt_tokens}p/{result.completion_tokens}c tokens'})"
        )
        st.markdown(result.raw_response)
        if result.unmapped_fields:
            st.warning(f"Unmapped source fields: {', '.join(result.unmapped_fields)}")


# ── Batch Analysis ─────────────────────────────────────────────────────────────

def _render_batch_analysis(ai, provider) -> None:
    section_title("Batch Root Cause Analysis")
    st.markdown(
        "Select a source and batch to generate an AI-powered root cause analysis "
        "and data quality executive summary."
    )

    sources = provider.get_sources()
    col_src, col_batch, col_type = st.columns(3)
    source = col_src.selectbox("Source:", sources)
    analysis_type = col_type.selectbox(
        "Analysis type:",
        ["Root Cause Report", "Executive Profiling Summary", "Duplicate Detection"],
    )

    # Load records for selected source
    records_df = provider.get_stewardship_records(source_name=source, limit=200)
    batches = sorted(records_df["batch_id"].unique().tolist(), reverse=True) if not records_df.empty else []
    batch = col_batch.selectbox("Batch:", batches if batches else ["All"])

    if batch and batch != "All":
        filtered_df = records_df[records_df["batch_id"] == batch]
    else:
        filtered_df = records_df

    st.caption(f"**{len(filtered_df)}** records selected for analysis")

    if st.button("Run Analysis", type="primary", disabled=filtered_df.empty):
        with st.spinner("Running AI analysis..."):
            if analysis_type == "Root Cause Report":
                report = ai.root_cause_analyzer.analyze_batch(
                    records_df=filtered_df,
                    source_name=source,
                    batch_id=batch or "All",
                )
                st.markdown(report.report)
                if report.top_violations:
                    st.markdown("#### Top Violations")
                    import pandas as pd
                    st.dataframe(pd.DataFrame(report.top_violations), use_container_width=True)

            elif analysis_type == "Executive Profiling Summary":
                profile = ai.profiling_assistant.profile(
                    records_df=filtered_df,
                    source_name=source,
                    batch_id=batch or "All",
                )
                st.markdown(profile.summary)

            elif analysis_type == "Duplicate Detection":
                _render_duplicate_detection(ai, filtered_df)


def _render_duplicate_detection(ai, records_df) -> None:
    """Build duplicate candidates from the records and run detection."""
    import pandas as pd
    import json

    # Extract distinct source_name values as simple dedup candidates
    candidates = []
    if "source_name" in records_df.columns:
        unique_sources = records_df["source_name"].unique().tolist()
        if len(unique_sources) >= 2:
            candidates.append(DuplicateCandidate(
                record_ids=records_df["record_id"].head(4).tolist(),
                field_values={"source_name": unique_sources[:4]},
            ))

    if not candidates:
        st.info("Not enough distinct values for duplicate detection in this batch.")
        return

    result = ai.duplicate_detector.detect(
        candidates=candidates,
        entity_type="records",
        domain="Data Stewardship",
    )
    st.markdown(result.analysis)


# ── Usage Stats ───────────────────────────────────────────────────────────────

def _render_usage_stats(ai) -> None:
    section_title("AI Token Usage & Cost")
    st.markdown("Tracks AI API usage for this session. Resets when the app restarts.")

    stats = ai.token_counter.stats()
    cache_stats = ai.cache.stats()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Tokens", f"{stats['total_tokens']:,}")
    col2.metric("API Calls", stats["total_calls"])
    col3.metric("Est. Cost (USD)", f"${stats['estimated_cost_usd']:.4f}")
    col4.metric("Cache Hit Rate", f"{cache_stats['hit_rate']:.0%}")

    st.divider()

    col_a, col_b = st.columns(2)
    with col_a:
        section_title("Usage by Feature")
        by_feature = stats.get("by_feature", {})
        if by_feature:
            import pandas as pd
            rows = [
                {
                    "Feature": k,
                    "Calls": v.get("call_count", 0),
                    "Tokens": v.get("prompt_tokens", 0) + v.get("completion_tokens", 0),
                    "Cost USD": f"${v.get('estimated_cost_usd', 0.0):.4f}",
                }
                for k, v in by_feature.items()
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        else:
            st.info("No AI calls made yet in this session.")

    with col_b:
        section_title("Cache Statistics")
        st.metric("Cache Size", f"{cache_stats['size']}/{cache_stats['max_size']}")
        st.metric("Cache Hits", cache_stats["hits"])
        st.metric("Cache Misses", cache_stats["misses"])
        st.caption(f"TTL: {cache_stats['ttl_seconds']}s")

        if st.button("Clear Cache"):
            ai.cache.clear()
            st.success("Cache cleared.")
            st.rerun()


main()
