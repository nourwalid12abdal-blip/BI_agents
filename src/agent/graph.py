# src/agent/graph.py

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from langgraph.graph import StateGraph, END
from src.agent.state import AgentState
from src.agent.nodes.understand_node import understand_node
from src.agent.nodes.sql_planner_node import sql_planner_node
from src.agent.nodes.mongo_planner_node import mongo_planner_node
from src.agent.nodes.executor_node import executor_node
from src.agent.nodes.format_node import format_node
import logging

logger = logging.getLogger(__name__)
# at the top of your script
import logging

logging.basicConfig(level=logging.INFO)

# in your node
logger = logging.getLogger(__name__)
logger.info("Hello, logging works now!")

# ── Optional charter node ──────────────────────────────────────────────────
# from src.agent.nodes.charter_node import charter_node

# def charter_node(state: AgentState) -> AgentState:
#     """Passthrough stub — replace with your real charter_node when ready."""
#     return {**state, "chart_spec": None}

from src.agent.nodes.charter_node import charter_node


# ── Retry nodes (one per planner) ─────────────────────────────────────────
# Having two separate nodes makes the graph topology explicit —
# LangGraph draws both retry arrows correctly instead of collapsing them.


def retry_sql(state: AgentState) -> AgentState:
    """Bumps retry_count and copies the error before going back to sql_planner."""
    logger.info(f"[retry_sql] retry {state.get('retry_count', 0) + 1}")
    return {
        **state,
        "retry_count": state.get("retry_count", 0) + 1,
        "error_feedback": state.get("execution_error"),
    }


def retry_mongo(state: AgentState) -> AgentState:
    """Bumps retry_count and copies the error before going back to mongo_planner."""
    logger.info(f"[retry_mongo] retry {state.get('retry_count', 0) + 1}")
    return {
        **state,
        "retry_count": state.get("retry_count", 0) + 1,
        "error_feedback": state.get("execution_error"),
    }


# ── Routing functions ──────────────────────────────────────────────────────


def route_planner(state: AgentState) -> str:
    """
    After understand_node — pick the right planner.
    source="sql"   → sql_planner
    source="mongo" → mongo_planner

    If MongoDB is not configured, always route to sql_planner.
    """
    from config.settings import settings

    source = state.get("source", "sql")

    # If MongoDB is not configured, force SQL
    if source == "mongo" and not settings.mongo_uri:
        logger.warning("[router] MongoDB not configured, routing to sql_planner")
        return "sql_planner"

    logger.info(f"[router] route_planner: source={source}")
    return "mongo_planner" if source == "mongo" else "sql_planner"


def route_after_exec(state: AgentState) -> str:
    """
    After executor_node — four possible outcomes:
      success + needs_chart        → charter
      success + no chart           → format
      failure + retries remaining  → retry_sql  OR  retry_mongo
      failure + no retries left    → format  (final_error set by format_node)
    """
    success = state.get("success", False)
    retry_count = state.get("retry_count", 0)
    max_retries = state.get("max_retries", 2)
    query_type = state.get("query_type", "sql")

    if success:
        route = "charter" if state.get("needs_chart") else "format"
        logger.info(f"[router] route_after_exec: success → {route}")
        return route

    if retry_count < max_retries:
        route = "retry_sql" if query_type == "sql" else "retry_mongo"
        logger.info(
            f"[router] route_after_exec: failed "
            f"({retry_count + 1}/{max_retries}) → {route}"
        )
        return route

    logger.warning("[router] route_after_exec: max retries exhausted → format")
    return "format"


# ── Graph builder ──────────────────────────────────────────────────────────


def build_graph() -> StateGraph:
    """
    Nodes
    ─────
      understand    → classify intent, source, entities, needs_chart
      sql_planner   → write SQL SELECT
      mongo_planner → write Mongo aggregation pipeline
      executor      → run the query against the real DB
      retry_sql     → bump counter + copy error, then back to sql_planner
      retry_mongo   → bump counter + copy error, then back to mongo_planner
      charter       → optionally produce a chart spec
      format        → write the plain-English answer

    Edges
    ─────
      START → understand
      understand  → sql_planner | mongo_planner        (route_planner)
      sql_planner   → executor
      mongo_planner → executor
      executor    → charter | format | retry_sql | retry_mongo  (route_after_exec)
      retry_sql   → sql_planner
      retry_mongo → mongo_planner
      charter     → format
      format      → END
    """
    graph = StateGraph(AgentState)

    # ── Nodes ──────────────────────────────────────────────────────────
    graph.add_node("understand", understand_node)
    graph.add_node("sql_planner", sql_planner_node)
    graph.add_node("mongo_planner", mongo_planner_node)
    graph.add_node("executor", executor_node)
    graph.add_node("retry_sql", retry_sql)
    graph.add_node("retry_mongo", retry_mongo)
    graph.add_node("charter", charter_node)
    graph.add_node("format", format_node)

    # ── Entry ──────────────────────────────────────────────────────────
    graph.set_entry_point("understand")

    # ── understand → planner ───────────────────────────────────────────
    graph.add_conditional_edges(
        "understand",
        route_planner,
        {
            "sql_planner": "sql_planner",
            "mongo_planner": "mongo_planner",
        },
    )

    # ── planners → executor ────────────────────────────────────────────
    graph.add_edge("sql_planner", "executor")
    graph.add_edge("mongo_planner", "executor")

    # ── executor → charter | format | retry_sql | retry_mongo ──────────
    graph.add_conditional_edges(
        "executor",
        route_after_exec,
        {
            "charter": "charter",
            "format": "format",
            "retry_sql": "retry_sql",
            "retry_mongo": "retry_mongo",
        },
    )

    # ── retry loops back to the correct planner ────────────────────────
    graph.add_edge("retry_sql", "sql_planner")
    graph.add_edge("retry_mongo", "mongo_planner")

    # ── charter → format → END ─────────────────────────────────────────
    graph.add_edge("charter", "format")
    graph.add_edge("format", END)

    return graph.compile()


# ── Default initial state ──────────────────────────────────────────────────


def initial_state(question: str) -> AgentState:
    return {
        "question": question,
        "intent": "",
        "source": "sql",
        "entities": [],
        "needs_chart": True,
        "intent_reasoning": "",
        "query_type": "sql",
        "query": "",
        "collection": None,
        "query_reasoning": "",
        "success": False,
        "data": [],
        "row_count": 0,
        "execution_error": None,
        "retry_count": 0,
        "max_retries": 2,
        "error_feedback": None,
        "response": None,
        "final_error": None,
        "chart_spec": None,
        "requested_chart_type": None,
    }


# ── Convenience runner ─────────────────────────────────────────────────────


def run(question: str) -> AgentState:
    """
    from src.agent.graph import run
    result = run("How many orders did Alice place?")
    print(result["response"])
    """
    app = build_graph()
    state = initial_state(question)
    return app.invoke(state)


# ── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    import webbrowser

    question = " Show total revenue per product"
    print(f"\nQuestion: {question}\n")

    result = run(question)  # your agent call

    print(f"Response:  {result}")
    if result.get("chart_spec"):
        print(f"Chart:     {json.dumps(result['chart_spec'], indent=2)}")
    if result.get("final_error"):
        print(f"Error:     {result['final_error']}")

    print("response:  ", result["response"])
    chart_type = (
        result["chart_spec"]["chart_type"] if result.get("chart_spec") else None
    )
    caption = result["chart_spec"].get("caption") if result.get("chart_spec") else None
    print("chart_type:", chart_type)
    print("caption:   ", caption)

    # Only build HTML if chart_spec exists
    if result.get("chart_spec"):
        spec = result["chart_spec"]
        html = f"""<!DOCTYPE html>
<html>
<head>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
</head>
<body style="padding:20px;font-family:sans-serif">
  <p>{result["response"]}</p>
  <p style="color:#888;font-size:13px">{spec.get("caption", "")}</p>
  <div id="c" style="height:450px"></div>
  <script>
    var chart = document.getElementById('c');
    Plotly.newPlot('c', {json.dumps(spec["data"])}, {json.dumps(spec["layout"])}, {json.dumps(spec["config"])});
    chart.on('plotly_click', function(d){{
      var q = d.points[0].customdata;
      if(q) alert('Drill-down: ' + q[0]);
    }});
  </script>
</body>
</html>"""

        path = "/home/nour/Downloads/bi_chart.html"
        with open(path, "w") as f:
            f.write(html)
        webbrowser.open("file://" + path)
        print("Chart opened in browser")
    else:
        print("No chart_spec found — HTML not generated.")
