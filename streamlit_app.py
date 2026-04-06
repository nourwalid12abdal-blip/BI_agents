# streamlit_app.py
import streamlit as st
import requests
import plotly.graph_objects as go
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

API_URL = "http://localhost:8000/ask"

st.set_page_config(
    page_title="BI Agent",
    page_icon="📊",
    layout="wide",
)


# ── Session state defaults ─────────────────────────────────────────────────
for key, default in {
    "history": [],
    "question_input": "",
    "schema_summary": None,
    "schema_loaded": False,
    "crawl_error": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📊 BI Agent")
    st.divider()

    # ── Database config ────────────────────────────────────────────────
    st.subheader("🗄 Database connections")

    sql_url = st.text_input(
        "SQL connection URL",
        value="sqlite:///./bi_agent.db",
        placeholder="sqlite:///./bi_agent.db  or  postgresql://...",
        help="SQLAlchemy connection string",
    )

    # MongoDB connection
    mongo_uri = st.text_input(
        "MongoDB URI",
        value="",
        placeholder="mongodb+srv://<username>:<password>@cluster.mongodb.net/",
        type="password",
    )

    mongo_db_name = st.text_input(
        "MongoDB Database Name",
        value="bi_agent_dev",
        placeholder="bi_agent_dev",
    )

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        crawl_btn = st.button(
            "🔍 Crawl schema",
            use_container_width=True,
            type="primary",
            help="Connect to both databases and build the schema graph",
        )
    with col2:
        seed_btn = st.button(
            "🌱 Seed test data",
            use_container_width=True,
            help="Insert sample rows into SQL and MongoDB for testing",
        )

    # ── Crawl ──────────────────────────────────────────────────────────
    if crawl_btn:
        with st.spinner("Crawling schema..."):
            try:
                import os

                os.environ["SQL_DB_URL"] = sql_url
                os.environ["MONGO_URI"] = mongo_uri
                os.environ["MONGO_DB_NAME"] = mongo_db_name

                from src.schema.crawler import crawl
                from src.schema.schema_store import save, get_schema_summary_for_llm

                graph = crawl(
                    sql_url=sql_url,
                    mongo_uri=mongo_uri,
                    mongo_db=mongo_db_name,
                )
                save(graph)

                st.session_state["schema_summary"] = get_schema_summary_for_llm(graph)
                st.session_state["schema_graph"] = graph
                st.session_state["schema_loaded"] = True
                st.session_state["crawl_error"] = None
                st.success("Schema crawled and saved!")

            except Exception as e:
                st.session_state["crawl_error"] = str(e)
                st.session_state["schema_loaded"] = False
                st.error(f"Crawl failed: {e}")

    # ── Seed ───────────────────────────────────────────────────────────
    if seed_btn:
        with st.spinner("Seeding test data..."):
            try:
                import os

                os.environ["SQL_DB_URL"] = sql_url
                os.environ["MONGO_URI"] = mongo_uri
                os.environ["MONGO_DB_NAME"] = mongo_db_name

                from scripts.seed_test_data import seed_sql

                seed_sql()
                st.success("Test data seeded!")
            except Exception as e:
                st.error(f"Seed failed: {e}")

    # ── Schema summary ─────────────────────────────────────────────────
    if st.session_state["schema_loaded"]:
        st.divider()
        st.subheader("📋 Schema summary")

        graph = st.session_state.get("schema_graph")
        if graph:
            m1, m2 = st.columns(2)
            m1.metric("SQL tables", len(graph.sql))
            m2.metric("Mongo collections", len(graph.mongo))

            m3, m4 = st.columns(2)
            total_cols = sum(len(t.columns) for t in graph.sql.values())
            m3.metric("SQL columns", total_cols)
            m4.metric("Cross-source links", len(graph.cross_source_relations))

        with st.expander("View full schema", expanded=False):
            st.code(st.session_state["schema_summary"], language="text")

        # SQL tables detail
        if graph and graph.sql:
            with st.expander("SQL tables", expanded=False):
                for table, schema in graph.sql.items():
                    st.markdown(f"**{table}** — {schema.row_count} rows")
                    col_data = [
                        {
                            "column": c.name,
                            "type": c.type,
                            "pk": "✓" if c.name in schema.primary_keys else "",
                        }
                        for c in schema.columns
                    ]
                    st.dataframe(col_data, hide_index=True, use_container_width=True)

        # Mongo collections detail
        if graph and graph.mongo:
            with st.expander("MongoDB collections", expanded=False):
                for col_name, schema in graph.mongo.items():
                    st.markdown(f"**{col_name}** — {schema.doc_count} documents")
                    field_data = [
                        {"field": k, "type": v} for k, v in schema.fields.items()
                    ]
                    st.dataframe(field_data, hide_index=True, use_container_width=True)

        # Cross-source relations
        if graph and graph.cross_source_relations:
            with st.expander("Cross-source relations", expanded=False):
                for rel in graph.cross_source_relations:
                    st.markdown(
                        f"🔗 `mongo.{rel.mongo_collection}.{rel.mongo_field}` "
                        f"→ `sql.{rel.sql_table}.{rel.sql_column}` "
                        f"[{rel.confidence}]"
                    )

    elif st.session_state["crawl_error"]:
        st.error(st.session_state["crawl_error"])
    else:
        st.info("Configure connections above and click **Crawl schema** to begin.")

    st.divider()
    needs_chart = st.toggle("Generate chart", value=True)

    st.divider()
    if st.button("🗑 Clear history", use_container_width=True):
        st.session_state["history"] = []
        st.rerun()


# ── Chart renderer ─────────────────────────────────────────────────────────
def render_chart(chart_spec: dict, item_idx: int):
    if not chart_spec or not chart_spec.get("data"):
        return

    fig = go.Figure()
    for trace_dict in chart_spec.get("data", []):
        trace_type = trace_dict.get("type", "scatter")
        trace_clean = {k: v for k, v in trace_dict.items() if k != "type"}

        type_map = {
            "bar": go.Bar,
            "scatter": go.Scatter,
            "pie": go.Pie,
            "funnel": go.Funnel,
            "heatmap": go.Heatmap,
            "indicator": go.Indicator,
            "table": go.Table,
        }
        TraceClass = type_map.get(trace_type, go.Scatter)
        fig.add_trace(TraceClass(**trace_clean))

    fig.update_layout(**chart_spec.get("layout", {}))
    st.plotly_chart(fig, use_container_width=True, key=f"chart_{item_idx}")

    caption = chart_spec.get("caption")
    if caption:
        st.caption(f"💡 {caption}")

    drilldowns = chart_spec.get("drilldowns", [])
    if drilldowns:
        st.markdown("**Explore further:**")
        cols = st.columns(min(len(drilldowns), 3))
        for i, d in enumerate(drilldowns):
            icon = "⚠️" if d.get("is_anomaly") else "🔍"
            with cols[i % 3]:
                if st.button(
                    f"{icon} {d.get('label', '')}",
                    key=f"drill_{item_idx}_{i}",
                    use_container_width=True,
                    help=d.get("question", ""),
                ):
                    st.session_state["question_input"] = d.get("question", "")
                    st.rerun()


# ── Main ───────────────────────────────────────────────────────────────────
st.title("Ask your data")

if not st.session_state["schema_loaded"]:
    st.warning("Connect and crawl your databases in the sidebar first.")

col_input, col_btn = st.columns([5, 1])
with col_input:
    question = st.text_input(
        "Question",
        value=st.session_state.get("question_input", ""),
        placeholder="e.g. How many orders did each customer place?",
        label_visibility="collapsed",
        key="main_input",
        disabled=not st.session_state["schema_loaded"],
    )
with col_btn:
    ask_clicked = st.button(
        "Ask",
        type="primary",
        use_container_width=True,
        disabled=not st.session_state["schema_loaded"],
    )

if ask_clicked and question.strip():
    st.session_state["question_input"] = ""
    with st.spinner("Thinking..."):
        try:
            resp = requests.post(
                API_URL,
                json={"question": question, "needs_chart": needs_chart},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            st.session_state["history"].append(
                {
                    "question": question,
                    "response": data.get("response", ""),
                    "chart_spec": data.get("chart_spec"),
                    "intent": data.get("intent", ""),
                    "source": data.get("source", ""),
                    "error": data.get("error"),
                }
            )
        except requests.exceptions.ConnectionError:
            st.error(
                "Cannot connect to API. Make sure FastAPI is running on port 8000."
            )
        except Exception as e:
            st.error(f"Error: {e}")
    st.rerun()

# ── History ────────────────────────────────────────────────────────────────
for i, item in enumerate(reversed(st.session_state["history"])):
    idx = len(st.session_state["history"]) - i
    with st.container(border=True):
        hcol1, hcol2 = st.columns([6, 2])
        with hcol1:
            st.markdown(f"**Q{idx}: {item['question']}**")
        with hcol2:
            badges = []
            if item.get("intent"):
                badges.append(f"`{item['intent']}`")
            if item.get("source"):
                badges.append(f"`{item['source']}`")
            st.markdown("  ".join(badges))

        if item.get("error"):
            st.error(item["error"])
            continue

        st.markdown(item["response"])

        if item.get("chart_spec"):
            render_chart(item["chart_spec"], idx)
        elif needs_chart:
            st.caption("_No chart generated for this result._")

if not st.session_state["history"]:
    st.info(
        "Ask a question above to get started."
        if st.session_state["schema_loaded"]
        else ""
    )
