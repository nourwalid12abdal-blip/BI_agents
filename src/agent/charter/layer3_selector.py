# # src/agent/charter/layer3_selector.py

# from langchain_core.prompts import ChatPromptTemplate
# from langchain_core.output_parsers import JsonOutputParser
# from src.agent.llm import get_llm
# import json
# import logging

# logger = logging.getLogger(__name__)

# LOW_CONFIDENCE_THRESHOLD  = 0.4   # below this → fall back to table
# MID_CONFIDENCE_THRESHOLD  = 0.6   # below this → use fallback chart type


# # ── Prompt ─────────────────────────────────────────────────────────────────

# SELECTOR_PROMPT = ChatPromptTemplate.from_messages([
#     (
#         "system",
#         """You are a senior data visualization expert deciding the best chart type for a BI dashboard.

# You will receive a column profile, the user's question, and the intent. 
# Your job is to pick the chart type that best communicates the answer.

# Available chart types and when to use each:
# - kpi        → 1 row with 1-4 numeric values. Single important metrics. Never use for multiple rows.
# - bar        → 1 categorical column (≤15 unique values) + 1 numeric. Best for comparing groups.
# - grouped_bar → 1 categorical + 2 or more numeric columns. Side-by-side comparison.
# - stacked_bar → 1 categorical + multiple numerics that sum to a meaningful whole.
# - line       → 1 temporal column + 1 numeric. Trends over time. Needs at least 3 time points.
# - multiline  → 1 temporal column + 2 or more numeric columns. Multiple trends over time.
# - area       → same as line but emphasizes volume/cumulative effect.
# - pie        → 1 categorical (≤6 unique values) + 1 numeric that represents parts of a whole.
# - donut      → same as pie but with a hole. Use when the total value matters.
# - scatter    → exactly 2 numeric columns, no category. Shows correlation.
# - bubble     → 2 numeric columns + 1 more numeric for size. 3-dimensional correlation.
# - heatmap    → 2 categorical columns + 1 numeric. Shows intensity across two dimensions.
# - funnel     → stages with decreasing values. Conversion or drop-off analysis.
# - table      → many columns, high cardinality, or no clear visual pattern. Always a safe fallback.

# Decision rules — follow these strictly:
# - If only 1 row exists → kpi (never chart a single row)
# - If temporal column exists → line, multiline, or area (time always wins for x axis)
# - If categorical cardinality > 15 → table (too many categories for a chart)
# - If all columns are categorical → table
# - If values represent parts of a whole (ratios, percentages) → pie or donut
# - If 2+ numeric + 0 categorical → scatter or bubble
# - If 2 categorical + 1 numeric → heatmap
# - When unsure → table (it is always correct)

# Confidence rules:
# - 0.9-1.0 → perfect fit, clear choice
# - 0.7-0.9 → good fit, minor ambiguity
# - 0.5-0.7 → reasonable fit, another chart could also work
# - below 0.5 → uncertain, use fallback

# very strict rule keep the desried chart in consideration all the time 
# Return ONLY this JSON object — no explanation outside it:
# {{
#   "chart_type":    "bar",
#   "x_column":      "name",
#   "y_columns":     ["total_orders"],
#   "color_column":  null,
#   "title":         "Total orders per customer",
#   "confidence":    0.92,
#   "fallback_type": "table",
#   "reasoning":     "3 customers with order counts — categorical comparison, bar is the clearest choice"
# }}""",
#     ),
#     (
#         "human",
#         """Column profile:
# {profile_text}

# Question: {question}
# Intent: {intent}
# Total rows: {row_count}
# Requested chart type:    {{requested_chart}} 

# Pick the best chart type.""",
#     ),
# ])


# # ── Profile text builder ───────────────────────────────────────────────────

# def _build_profile_text(profile: dict) -> str:
#     """
#     Converts the Layer 2 profile dict into a compact readable text
#     the LLM can reason about without being overwhelmed by raw JSON.
#     """
#     lines = []

#     for col, info in profile.items():
#         if col == "_suggestions":
#             continue

#         col_type    = info.get("type", "unknown")
#         cardinality = info.get("cardinality", "?")
#         null_rate   = info.get("null_rate", 0)
#         samples     = info.get("sample_values", [])
#         val_range   = info.get("value_range")
#         mono        = info.get("monotonicity")

#         parts = [f"{col} ({col_type})"]
#         parts.append(f"{cardinality} unique values")

#         if samples:
#             sample_str = ", ".join(str(s) for s in samples[:4])
#             parts.append(f"samples: {sample_str}")

#         if val_range:
#             parts.append(f"range: {val_range[0]} to {val_range[1]}")

#         if mono and mono != "mixed":
#             parts.append(f"trend: {mono}")

#         if null_rate > 0.1:
#             parts.append(f"{int(null_rate * 100)}% null")

#         lines.append("  " + " — ".join(parts))

#     suggestions = profile.get("_suggestions", {})
#     if suggestions:
#         lines.append("")
#         lines.append(f"Suggested x axis: {suggestions.get('x_column', 'unknown')}")
#         lines.append(f"Suggested y axis: {suggestions.get('y_column', 'unknown')}")

#     return "\n".join(lines)


# # ── Result validator ───────────────────────────────────────────────────────

# def _validate_result(result: dict, profile: dict, row_count: int,requested_chart: str = None) -> dict:
#     """
#     Applies hard rules on top of the LLM decision.
#     The LLM is smart but these rules are absolute.
#     """
#     if requested_chart:
#         result["chart_type"] = requested_chart
#         result["confidence"] = 0.9
#         result["reasoning"] += f" — forced by user request to {requested_chart}"




#     chart_type = result.get("chart_type", "table")

#     # Hard rule 1: single row → always KPI
#     if row_count == 1:
#         result["chart_type"]  = "kpi"
#         result["confidence"]  = 1.0
#         result["reasoning"]   = "Single row result — KPI card is always correct here"

#     # Hard rule 2: pie/donut needs ≤ 6 categories
#     # if chart_type in ("pie", "donut"):
#     #     x_col = result.get("x_column", "")
#     #     col_info = profile.get(x_col, {})
#     #     if col_info.get("cardinality", 999) > 2:
#     #         result["chart_type"]  = result.get("fallback_type", "bar")
#     #         result["confidence"]  = 0.55
#     #         result["reasoning"]  += " — switched from pie: too many categories"

#     # Hard rule 3: low confidence → use fallback
#     if result.get("confidence", 1.0) < MID_CONFIDENCE_THRESHOLD:
#         result["chart_type"] = result.get("fallback_type", "table")
#         result["reasoning"]  += f" — confidence too low, using fallback"

#     # Hard rule 4: y_columns must be a list
#     if isinstance(result.get("y_columns"), str):
#         result["y_columns"] = [result["y_columns"]]

#     # Hard rule 5: ensure all required keys exist
#     result.setdefault("chart_type",    "table")
#     result.setdefault("x_column",      "")
#     result.setdefault("y_columns",     [])
#     result.setdefault("color_column",  None)
#     result.setdefault("title",         "Results")
#     result.setdefault("confidence",    0.5)
#     result.setdefault("fallback_type", "table")
#     result.setdefault("reasoning",     "")

#     # ── heatmap structure ─────────────────────────────
#     if result.get("chart_type") == "heatmap":
#      categorical_cols = [
#         c for c, p in profile.items()
#         if c != "_suggestions" and p.get("type") == "categorical"
#      ]
#      numeric_cols = [
#         c for c, p in profile.items()
#         if c != "_suggestions" and p.get("type") == "numeric"
#      ]

#     if len(categorical_cols) >= 2 and len(numeric_cols) >= 1:
#         result["x_column"] = categorical_cols[0]
#         result["y_column"] = categorical_cols[1]   
#         result["z_column"] = numeric_cols[0]       

#         result["y_columns"] = [result["y_column"]]

#     return result


# # ── Public API ─────────────────────────────────────────────────────────────

# def select(
#     profile:   dict,
#     question:  str,
#     intent:    str,
#     row_count: int,
# ) -> dict:
#     """
#     Layer 3 — Uses the LLM to select the best chart type.

#     Args:
#         profile:   Column profile from layer2_classifier.classify()
#         question:  The original user question
#         intent:    Intent from the understand node
#         row_count: Number of rows in the data

#     Returns:
#         {
#             "chart_type":    "bar" | "line" | "kpi" | "pie" | etc.
#             "x_column":      "column_name",
#             "y_columns":     ["col1", "col2"],
#             "color_column":  "col_name" | null,
#             "title":         "Chart title",
#             "confidence":    0.92,
#             "fallback_type": "table",
#             "reasoning":     "explanation"
#         }
#     """
#     logger.info(f"[L3/selector] Selecting chart for intent='{intent}' rows={row_count}")

#     # Hard rule: single row → KPI immediately, no LLM call needed
#     if row_count == 1:
#         suggestions = profile.get("_suggestions", {})
#         numeric_cols = [
#             c for c, p in profile.items()
#             if c != "_suggestions" and p.get("type") == "numeric"
#         ]
#         logger.info("[L3/selector] Single row → KPI (no LLM call)")
#         return {
#             "chart_type":    "kpi",
#             "x_column":      suggestions.get("x_column", ""),
#             "y_columns":     numeric_cols,
#             "color_column":  None,
#             "title":         question,
#             "confidence":    1.0,
#             "fallback_type": "table",
#             "reasoning":     "Single row result — KPI card is always correct",
#         }

#     # Build profile text for LLM
#     profile_text = _build_profile_text(profile)
#     logger.debug(f"[L3/selector] Profile text:\n{profile_text}")

#     chain = SELECTOR_PROMPT | get_llm(temperature=0.0) | JsonOutputParser()

#     try:
#         result = chain.invoke({
#             "profile_text": profile_text,
#             "question":     question,
#             "intent":       intent,
#             "row_count":    row_count,
#         })

#         # Apply hard validation rules
#         result = _validate_result(result, profile, row_count)

#         logger.info(
#             f"[L3/selector] Selected: {result['chart_type']} "
#             f"(confidence={result['confidence']}) — {result['reasoning']}"
#         )

#         return result

#     except Exception as e:
#         logger.error(f"[L3/selector] LLM call failed: {e} — defaulting to table")
#         return {
#             "chart_type":    "table",
#             "x_column":      "",
#             "y_columns":     [],
#             "color_column":  None,
#             "title":         question,
#             "confidence":    0.0,
#             "fallback_type": "table",
#             "reasoning":     f"Selector failed: {e}",
#         }



# src/agent/charter/layer3_selector.py

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from src.agent.llm import get_llm
import logging

logger = logging.getLogger(__name__)

LOW_CONFIDENCE_THRESHOLD = 0.4
MID_CONFIDENCE_THRESHOLD = 0.6

VALID_CHART_TYPES = {
    "kpi", "bar", "grouped_bar", "stacked_bar",
    "line", "multiline", "area",
    "pie", "donut",
    "scatter", "bubble",
    "heatmap", "funnel", "table",
}


# ── Prompt ─────────────────────────────────────────────────────────────────

SELECTOR_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        """You are a senior data visualization expert deciding the best chart type for a BI dashboard.

Available chart types and when to use each:
- kpi         → exactly 1 row, 1-4 numeric values. Single aggregated metric.
- bar         → 1 categorical (≤15 unique) + 1 numeric. Compare groups.
- grouped_bar → 1 categorical + 2+ numeric. Side-by-side comparison.
- stacked_bar → 1 categorical + multiple numerics that sum to a whole.
- line        → 1 temporal + 1 numeric. Trend over time (≥3 points).
- multiline   → 1 temporal + 2+ numeric. Multiple trends over time.
- area        → same as line but emphasizes volume or cumulative value.
- pie         → 1 categorical (≤6 unique) + 1 numeric. Parts of a whole.
- donut       → same as pie but with a hole. Use when total matters too.
- scatter     → 2 numeric, 0 categorical. Correlation between two values.
- bubble      → 2 numeric + 1 numeric for size. 3-dimensional correlation.
- heatmap     → 2 categorical + 1 numeric. Intensity across two dimensions.
- funnel      → ordered stages with decreasing values. Conversion analysis.
- table       → many columns, high cardinality, or no clear pattern. Always safe.

Hard decision rules — never break these:
- 1 row only              → kpi
"temporal column exists + intent is trend → prefer line/area
 temporal column exists + intent is filter/lookup → prefer table (user wants records, not a trend)"- categorical cardinality > 15 → table
- all columns categorical → table
- parts of a whole        → pie or donut
- 2+ numeric, 0 categorical → scatter or bubble
- 2 categorical + 1 numeric → heatmap
- unsure                  → table

Confidence scale:
- 0.9–1.0  perfect fit
- 0.7–0.9  good fit
- 0.5–0.7  reasonable, another type could also work
- < 0.5    uncertain → use fallback_type

Return ONLY this JSON — no explanation outside it:
{{
  "chart_type":    "bar",
  "x_column":      "name",
  "y_columns":     ["total_orders"],
  "color_column":  null,
  "title":         "Total orders per customer",
  "confidence":    0.92,
  "fallback_type": "table",
  "reasoning":     "3 customers with order counts — bar is clearest"
}}""",
    ),
    (
        "human",
        """Column profile:
{profile_text}

Question:       {question}
Intent:         {intent}
Total rows:     {row_count}
Requested type: {requested_chart}

If a specific chart type was requested and it is feasible for this data, use it.
Otherwise pick the best type automatically.""",
    ),
])


# ── Profile text builder ───────────────────────────────────────────────────

def _build_profile_text(profile: dict) -> str:
    lines = []
    for col, info in profile.items():
        if col == "_suggestions":
            continue
        parts = [f"{col} ({info.get('type', '?')})"]
        parts.append(f"{info.get('cardinality', '?')} unique values")

        samples = info.get("sample_values", [])
        if samples:
            parts.append(f"samples: {', '.join(str(s) for s in samples[:4])}")

        val_range = info.get("value_range")
        if val_range:
            parts.append(f"range: {val_range[0]} – {val_range[1]}")

        mono = info.get("monotonicity")
        if mono and mono != "mixed":
            parts.append(f"trend: {mono}")

        if info.get("null_rate", 0) > 0.1:
            parts.append(f"{int(info['null_rate'] * 100)}% null")

        lines.append("  " + " — ".join(parts))

    suggestions = profile.get("_suggestions", {})
    if suggestions:
        lines.append("")
        lines.append(f"Suggested x: {suggestions.get('x_column', '?')}")
        lines.append(f"Suggested y: {suggestions.get('y_column', '?')}")

    return "\n".join(lines)


# ── Validator ──────────────────────────────────────────────────────────────

def _validate(
    result:          dict,
    profile:         dict,
    row_count:       int,
    requested_chart: str | None,
) -> dict:
    """
    Applies hard rules on top of the LLM decision.

    Priority order:
      1. Single row          → always KPI
      2. User requested type → use it if valid and feasible
      3. Low confidence      → use fallback_type
      4. Ensure all keys exist
      5. Fix heatmap column mapping
    """
    

    # ── Rule 1: single row → KPI always ───────────────────────────────
    if row_count == 1:
        numeric_cols = [
            c for c, p in profile.items()
            if c != "_suggestions" and p.get("type") == "numeric"
        ]
        result["chart_type"] = "kpi"
        result["confidence"] = 1.0
        result["y_columns"]  = numeric_cols
        result["reasoning"]  = "Single row — KPI is always correct"
        return _ensure_keys(result)

    # ── Rule 2: honour user request if feasible ────────────────────────
    if requested_chart and requested_chart in VALID_CHART_TYPES:
        feasible, reason = _is_feasible(requested_chart, profile, row_count)
        if feasible:
            result["chart_type"] = requested_chart
            result["confidence"] = 0.95
            result["reasoning"]  = f"User requested '{requested_chart}' — feasible: {reason}"
            logger.info(f"[L3] Honouring user request: {requested_chart}")
        else:
            logger.warning(
                f"[L3] User requested '{requested_chart}' but not feasible: {reason} "
                f"— keeping LLM choice '{result.get('chart_type')}'"
            )
            result["reasoning"] += f" | Note: '{requested_chart}' not feasible ({reason})"

    # ── Rule 3: low confidence → use fallback ─────────────────────────
    if result.get("confidence", 1.0) < MID_CONFIDENCE_THRESHOLD:
        result["chart_type"] = result.get("fallback_type", "table")
        result["reasoning"] += " — confidence too low, using fallback"

    # ── Rule 4: ensure y_columns is a list ────────────────────────────
    if isinstance(result.get("y_columns"), str):
        result["y_columns"] = [result["y_columns"]]

    # ── Rule 5: fix heatmap column mapping ────────────────────────────
    if result.get("chart_type") == "heatmap":
        result = _fix_heatmap(result, profile)

    return _ensure_keys(result)


def _is_feasible(chart_type: str, profile: dict, row_count: int) -> tuple[bool, str]:
    """
    Checks whether a requested chart type is compatible with the data.
    Returns (feasible: bool, reason: str).
    """
    cat_cols = [
        c for c, p in profile.items()
        if c != "_suggestions" and p.get("type") == "categorical"
    ]
    num_cols = [
        c for c, p in profile.items()
        if c != "_suggestions" and p.get("type") == "numeric"
    ]
    tmp_cols = [
        c for c, p in profile.items()
        if c != "_suggestions" and p.get("type") == "temporal"
    ]

    checks = {
        "kpi":         (row_count == 1,                       "needs exactly 1 row"),
        "bar":         (len(cat_cols) >= 1 and len(num_cols) >= 1, "needs 1 categorical + 1 numeric"),
        "grouped_bar": (len(cat_cols) >= 1 and len(num_cols) >= 2, "needs 1 categorical + 2+ numeric"),
        "stacked_bar": (len(cat_cols) >= 1 and len(num_cols) >= 2, "needs 1 categorical + 2+ numeric"),
        "line":        (len(tmp_cols) >= 1 and len(num_cols) >= 1, "needs 1 temporal + 1 numeric"),
        "multiline":   (len(tmp_cols) >= 1 and len(num_cols) >= 2, "needs 1 temporal + 2+ numeric"),
        "area":        (len(tmp_cols) >= 1 and len(num_cols) >= 1, "needs 1 temporal + 1 numeric"),
        "pie":         (len(cat_cols) >= 1 and len(num_cols) >= 1, "needs 1 categorical + 1 numeric"),
        "donut":       (len(cat_cols) >= 1 and len(num_cols) >= 1, "needs 1 categorical + 1 numeric"),
        "scatter":     (len(num_cols) >= 2,                    "needs 2+ numeric columns"),
        "bubble":      (len(num_cols) >= 3,                    "needs 3+ numeric columns"),
        "heatmap":     (len(cat_cols) >= 2 and len(num_cols) >= 1, "needs 2 categorical + 1 numeric"),
        "funnel":      (len(cat_cols) >= 1 and len(num_cols) >= 1, "needs 1 categorical + 1 numeric"),
        "table":       (True,                                  "always feasible"),
    }

    ok, reason = checks.get(chart_type, (False, "unknown chart type"))
    return ok, reason


def _fix_heatmap(result: dict, profile: dict) -> dict:
    """Ensures heatmap has x_column (cat), y_column (cat), z via y_columns (numeric)."""
    cat_cols = [
        c for c, p in profile.items()
        if c != "_suggestions" and p.get("type") == "categorical"
    ]
    num_cols = [
        c for c, p in profile.items()
        if c != "_suggestions" and p.get("type") == "numeric"
    ]
    if len(cat_cols) >= 2 and num_cols:
        result["x_column"] = cat_cols[0]
        result["y_columns"] = [cat_cols[1]]
        result["z_column"]  = num_cols[0]
    return result


def _ensure_keys(result: dict) -> dict:
    """Fills in any missing keys with safe defaults."""
    result.setdefault("chart_type",    "table")
    result.setdefault("x_column",      "")
    result.setdefault("y_columns",     [])
    result.setdefault("color_column",  None)
    result.setdefault("title",         "Results")
    result.setdefault("confidence",    0.5)
    result.setdefault("fallback_type", "table")
    result.setdefault("reasoning",     "")
    return result


# ── Public API ─────────────────────────────────────────────────────────────

def select(
    profile:         dict,
    question:        str,
    intent:          str,
    row_count:       int,
    requested_chart: str | None = None,
) -> dict:
    """
    Layer 3 — Select the best chart type for the data.

    Args:
        profile:         Column profile from layer2_classifier.classify()
        question:        Original user question
        intent:          Intent from understand_node
        row_count:       Number of rows
        requested_chart: Optional chart type the user explicitly asked for

    Returns:
        {
            chart_type, x_column, y_columns, color_column,
            title, confidence, fallback_type, reasoning
        }
    """
    logger.info(
        f"[L3/selector] intent='{intent}'  rows={row_count}  "
        f"requested='{requested_chart or 'auto'}'"
    )

    # Fast path — single row is always KPI
    if row_count == 1:
        numeric_cols = [
            c for c, p in profile.items()
            if c != "_suggestions" and p.get("type") == "numeric"
        ]
        return _ensure_keys({
            "chart_type":    "kpi",
            "x_column":      profile.get("_suggestions", {}).get("x_column", ""),
            "y_columns":     numeric_cols,
            "color_column":  None,
            "title":         question,
            "confidence":    1.0,
            "fallback_type": "table",
            "reasoning":     "Single row — KPI always correct",
        })

    profile_text = _build_profile_text(profile)
    chain = SELECTOR_PROMPT | get_llm(temperature=0.0) | JsonOutputParser()

    try:
        result = chain.invoke({
            "profile_text":    profile_text,
            "question":        question,
            "intent":          intent,
            "row_count":       row_count,
            "requested_chart": requested_chart or "none — pick automatically",
        })

        result = _validate(result, profile, row_count, requested_chart)

        logger.info(
            f"[L3/selector] → {result['chart_type']}  "
            f"confidence={result['confidence']}  "
            f"reasoning={result['reasoning']}"
        )
        return result

    except Exception as e:
        logger.error(f"[L3/selector] LLM failed: {e} — defaulting to table")
        return _ensure_keys({
            "chart_type":    "table",
            "title":         question,
            "confidence":    0.0,
            "fallback_type": "table",
            "reasoning":     f"Selector error: {e}",
        })