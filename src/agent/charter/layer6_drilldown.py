# src/agent/charter/layer6_drilldown.py

import logging

logger = logging.getLogger(__name__)


# ── Template patterns ──────────────────────────────────────────────────────
# Fast path — no LLM needed for standard drill-downs.
# {x_col} and {label} are substituted at runtime.

INTENT_TEMPLATES = {
    "aggregation": "Show me the details behind {x_col} = {label}",
    "filter":      "Show all records where {x_col} is {label}",
    "comparison":  "Compare {label} against the other {x_col} values",
    "trend":       "What caused the change at {label}?",
    "lookup":      "Show full details for {label}",
    "dashboard":   "Break down the numbers for {label}",
}

ANOMALY_TEMPLATE = (
    "Why is {label} unusually {direction} for {x_col}? "
    "The value is {value} vs an average of {mean}."
)

FALLBACK_TEMPLATE = "Tell me more about {label}"


# ── Anomaly index builder ──────────────────────────────────────────────────

def _build_anomaly_index(intelligence: dict, x_column: str) -> dict:
    """
    Builds a fast lookup: label → anomaly dict.
    Used to check if a data point is anomalous in O(1).
    """
    index = {}
    for a in intelligence.get("anomalies", []):
        if a.get("column") and a.get("label"):
            index[str(a["label"])] = a
    return index


# ── Question generator ─────────────────────────────────────────────────────

def _make_question(
    label:        str,
    value,
    x_col:        str,
    intent:       str,
    anomaly:      dict | None,
) -> str:
    """
    Generates a drill-down question for one data point.

    Anomalous points get a special question explaining the anomaly.
    Normal points get an intent-aware template question.
    """
    if anomaly:
        return ANOMALY_TEMPLATE.format(
            label=label,
            direction=anomaly.get("direction", "different"),
            x_col=x_col,
            value=value,
            mean=anomaly.get("mean", "?"),
        )

    template = INTENT_TEMPLATES.get(intent, FALLBACK_TEMPLATE)
    return template.format(x_col=x_col, label=label, value=value)


# ── Customdata injector ────────────────────────────────────────────────────

def _inject_into_spec(spec: dict, drilldowns: list[dict]) -> dict:
    """
    Attaches drill-down questions to the Plotly spec as customdata.

    Each trace point gets:
        customdata: [question_string]
        hovertemplate: updated to show "Click to explore"

    The frontend reads customdata[0] on click and calls sendPrompt().
    Only works for scatter/bar/line traces — skips pie, table, indicator.
    """
    SUPPORTED_TYPES = {"scatter", "bar"}

    questions = [d["question"] for d in drilldowns]

    for trace in spec.get("data", []):
        if trace.get("type") not in SUPPORTED_TYPES:
            continue

        trace["customdata"] = [[q] for q in questions]

        # Append click hint to existing hovertemplate
        existing = trace.get("hovertemplate", "%{x}: %{y}<extra></extra>")
        if "<extra></extra>" in existing:
            trace["hovertemplate"] = existing.replace(
                "<extra></extra>",
                "<br><i style='color:#888'>Click to explore</i><extra></extra>",
            )

    return spec


# ── Public API ─────────────────────────────────────────────────────────────

def generate(
    rows:         list[dict],
    profile:      dict,
    selection:    dict,
    intelligence: dict,
    spec:         dict,
    intent:       str = "aggregation",
) -> dict:
    """
    Layer 6 — Generate drill-down questions for every data point
    and inject them into the Plotly spec as customdata.

    Args:
        rows:         Normalized flat rows from Layer 1
        profile:      Column profile from Layer 2
        selection:    Chart selection from Layer 3
        intelligence: Analysis from Layer 4
        spec:         Plotly spec from Layer 5 (mutated in place)
        intent:       Intent from understand_node

    Returns:
        {
            "drilldowns": [
                {
                    "label":      str,    # x axis value
                    "value":      any,    # y axis value
                    "question":   str,    # drill-down question
                    "is_anomaly": bool,
                }
            ],
            "spec": dict    # Plotly spec with customdata injected
        }
    """
    x_col  = selection.get("x_column", "")
    y_cols = selection.get("y_columns", [])
    y_col  = y_cols[0] if y_cols else None

    # KPI and table have no clickable points
    chart_type = selection.get("chart_type", "table")
    if chart_type in ("kpi", "table", "indicator"):
        logger.info(f"[L6/drilldown] Skipping — chart type '{chart_type}' has no drill-down points")
        return {"drilldowns": [], "spec": spec}

    logger.info(
        f"[L6/drilldown] Generating drill-downs — "
        f"{len(rows)} rows  x={x_col}  intent={intent}"
    )

    anomaly_index = _build_anomaly_index(intelligence, x_col)
    drilldowns    = []

    for row in rows:
        label  = str(row.get(x_col, ""))
        value  = row.get(y_col) if y_col else None
        anomaly = anomaly_index.get(label)

        question = _make_question(
            label=label,
            value=value,
            x_col=x_col,
            intent=intent,
            anomaly=anomaly,
        )

        drilldowns.append({
            "label":      label,
            "value":      value,
            "question":   question,
            "is_anomaly": anomaly is not None,
        })

        logger.info(
            f"[L6/drilldown]  {label} → '{question}'"
            + (" [ANOMALY]" if anomaly else "")
        )

    # Inject into spec
    spec = _inject_into_spec(spec, drilldowns)

    return {
        "drilldowns": drilldowns,
        "spec":       spec,
    }