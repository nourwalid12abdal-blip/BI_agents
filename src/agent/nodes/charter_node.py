# src/agent/nodes/charter_node.py

from src.agent.state import AgentState
from src.agent.charter.layer1_normalizer    import normalize
from src.agent.charter.layer2_classifier    import classify
from src.agent.charter.layer3_selector      import select
from src.agent.charter.layer4_intelligence  import analyze
from src.agent.charter.layer5_spec_builder  import build
from src.agent.charter.layer6_drilldown     import generate
import logging

logger = logging.getLogger(__name__)

# ── Layer 7 stub ───────────────────────────────────────────────────────────
# Replace this with:
#   from src.agent.charter.layer7_caption import write
# once layer7_caption.py is implemented.

def _write_caption(rows, profile, selection, intelligence, intent, question) -> str:
    """
    Layer 7 stub — generates a plain caption from templates.
    Replace with LLM call when layer7_caption.py is ready.
    """
    chart_type = selection.get("chart_type", "table")
    x_col      = selection.get("x_column", "")
    y_cols     = selection.get("y_columns", [])
    y_col      = y_cols[0] if y_cols else ""
    trend      = intelligence.get("trend", {}).get("direction", "")
    anomalies  = intelligence.get("anomalies", [])

    # KPI — just state the number
    if chart_type == "kpi" and rows:
        value = rows[0].get(y_col, "")
        return f"The total {y_col} is {value}."

    # Trend caption
    if trend and trend not in ("not_applicable", "insufficient_data", "flat"):
        arrow = "↗" if "growing" in trend else "↘"
        caption = f"{arrow} {y_col} is {trend.replace('_', ' ')} over {x_col}."
    else:
        caption = f"{y_col} broken down by {x_col}."

    # Append anomaly note
    if anomalies:
        labels = ", ".join(a["label"] for a in anomalies[:2])
        caption += f" Notable outlier(s): {labels}."

    return caption


# ── Node ───────────────────────────────────────────────────────────────────

def charter_node(state: AgentState) -> AgentState:
    """
    LangGraph node — runs the full 7-layer charter pipeline.

    Reads from state:
        state.data          — query results from executor_node
        state.row_count     — number of rows
        state.question      — original user question
        state.intent        — classified intent from understand_node
        state.needs_chart   — True if understand_node flagged a chart

    Writes to state:
        state.chart_spec    — full Plotly spec dict ready for the frontend:
            {
                data:         [...],   # Plotly traces
                layout:       {...},   # Plotly layout
                config:       {...},   # Plotly config
                chart_type:   str,
                fallback_used: bool,
                caption:      str,     # one-sentence insight (Layer 7)
                drilldowns:   [...],   # click-to-explore questions (Layer 6)
            }
    """
    question  = state.get("question", "")
    intent    = state.get("intent", "aggregation")
    rows      = state.get("data", [])
    row_count = state.get("row_count", 0)

    logger.info(
        f"[charter_node] Starting pipeline — "
        f"rows={row_count}  intent={intent}  question='{question[:60]}'"
    )

    # ── Guard: nothing to chart ────────────────────────────────────────
    if not rows or row_count == 0:
        logger.warning("[charter_node] No data — skipping chart")
        return {**state, "chart_spec": None}

    try:
        # ── Layer 1: normalize ─────────────────────────────────────────
        clean_rows = normalize(rows)
        logger.info(f"[charter_node] L1 done — {len(clean_rows)} clean rows")

        # ── Layer 2: classify ──────────────────────────────────────────
        profile = classify(clean_rows)
        logger.info(f"[charter_node] L2 done — {len(profile)-1} columns profiled")

        # ── Layer 3: select chart ──────────────────────────────────────
        selection = select(
            profile=profile,
            question=question,
            intent=intent,
            row_count=len(clean_rows),
            requested_chart=state.get("requested_chart_type"),  # ← add this  # ← add this
        )
        logger.info(
            f"[charter_node] L3 done — "
            f"chart={selection['chart_type']}  confidence={selection['confidence']}"
        )

        # ── Layer 4: intelligence ──────────────────────────────────────
        intelligence = analyze(clean_rows, profile, selection)
        logger.info(
            f"[charter_node] L4 done — "
            f"anomalies={len(intelligence['anomalies'])}  "
            f"trend={intelligence['trend']['direction']}"
        )

        # ── Layer 5: build spec ────────────────────────────────────────
        spec = build(clean_rows, profile, selection, intelligence)
        logger.info(
            f"[charter_node] L5 done — "
            f"chart={spec['chart_type']}  fallback={spec['fallback_used']}"
        )

        # ── Layer 6: drill-downs ───────────────────────────────────────
        l6_result = generate(
            rows=clean_rows,
            profile=profile,
            selection=selection,
            intelligence=intelligence,
            spec=spec,
            intent=intent,
        )
        spec      = l6_result["spec"]
        drilldowns = l6_result["drilldowns"]
        logger.info(f"[charter_node] L6 done — {len(drilldowns)} drill-down questions")

        # ── Layer 7: caption ───────────────────────────────────────────
        caption = _write_caption(
            rows=clean_rows,
            profile=profile,
            selection=selection,
            intelligence=intelligence,
            intent=intent,
            question=question,
        )
        logger.info(f"[charter_node] L7 done — caption: '{caption}'")

        # ── Assemble final chart_spec ──────────────────────────────────
        chart_spec = {
            **spec,
            "caption":    caption,
            "drilldowns": drilldowns,
        }

        logger.info("[charter_node] Pipeline complete")
        return {**state, "chart_spec": chart_spec}

    except Exception as e:
        logger.error(f"[charter_node] Pipeline failed: {e}", exc_info=True)
        return {**state, "chart_spec": None}