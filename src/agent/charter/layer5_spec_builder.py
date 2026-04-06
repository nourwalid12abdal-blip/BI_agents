# src/agent/charter/layer5_spec_builder.py

import logging

logger = logging.getLogger(__name__)

# ── Color palette ──────────────────────────────────────────────────────────
# Matches the project's design system ramps

PALETTE = [
    "#534AB7",  # purple-600
    "#1D9E75",  # teal-400
    "#D85A30",  # coral-400
    "#378ADD",  # blue-400
    "#BA7517",  # amber-400
    "#639922",  # green-400
    "#D4537E",  # pink-400
    "#888780",  # gray-400
    "#E24B4A",  # red-400
]

ANOMALY_COLOR = "#E24B4A"  # red-400
GRID_COLOR = "rgba(136,135,128,0.15)"
FONT_FAMILY = "Inter, system-ui, sans-serif"


# ── Base layout / config ───────────────────────────────────────────────────


def _base_layout(title: str, annotations: list | None = None) -> dict:
    """Shared Plotly layout properties — every builder merges overrides on top."""
    return {
        "title": {
            "text": title,
            "x": 0.02,
            "xanchor": "left",
            "font": {"size": 15, "family": FONT_FAMILY, "color": "#2C2C2A"},
        },
        "font": {"family": FONT_FAMILY, "size": 12, "color": "#5F5E5A"},
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "margin": {"t": 52, "r": 20, "b": 48, "l": 56},
        "showlegend": False,
        "annotations": annotations or [],
        "hoverlabel": {
            "bgcolor": "#FFFFFF",
            "bordercolor": "#D3D1C7",
            "font": {"family": FONT_FAMILY, "size": 12},
        },
    }


def _base_config() -> dict:
    """Plotly config — removes toolbar clutter, keeps essential buttons."""
    return {
        "displaylogo": False,
        "modeBarButtonsToRemove": [
            "zoom2d",
            "pan2d",
            "select2d",
            "lasso2d",
            "zoomIn2d",
            "zoomOut2d",
            "autoScale2d",
        ],
        "toImageButtonOptions": {
            "format": "png",
            "height": 400,
            "width": 700,
            "scale": 2,
        },
        "responsive": True,
    }


def _axis_style(title: str = "", show_grid: bool = True) -> dict:
    """Shared axis styling."""
    return {
        "title": {"text": title, "font": {"size": 11}},
        "gridcolor": GRID_COLOR,
        "showgrid": show_grid,
        "zeroline": False,
        "linecolor": GRID_COLOR,
        "tickfont": {"size": 11},
        "automargin": True,
    }


# ── Chart builders ─────────────────────────────────────────────────────────


def _build_bar(rows, profile, selection, intel) -> dict:
    """
    Bar chart — one bar per x category, height = y value.
    Anomalous bars are highlighted in red using anomaly labels from Layer 4.
    """
    x_col = selection["x_column"]
    y_col = (selection["y_columns"] or [None])[0]

    if not y_col:
        return _build_table(rows, profile, selection, intel)

    x_vals = [str(row.get(x_col, "")) for row in rows]
    y_vals = [row.get(y_col) for row in rows]

    anomaly_labels = {a["label"] for a in intel.get("anomalies", [])}
    colors = [ANOMALY_COLOR if x in anomaly_labels else PALETTE[0] for x in x_vals]

    trace = {
        "type": "bar",
        "x": x_vals,
        "y": y_vals,
        "marker": {"color": colors, "opacity": 0.88},
        "hovertemplate": f"<b>%{{x}}</b><br>{y_col}: %{{y:,.2f}}<extra></extra>",
    }

    layout = _base_layout(
        title=selection.get("title", f"{y_col} by {x_col}"),
        annotations=intel.get("annotations", []),
    )
    layout["xaxis"] = _axis_style(x_col, show_grid=False)
    layout["yaxis"] = _axis_style(y_col)
    layout["bargap"] = 0.35

    return {"data": [trace], "layout": layout}


def _build_line(rows, profile, selection, intel) -> dict:
    """
    Line chart — x is temporal or ordered, one line per y column.
    Works for both 'line' and 'multiline' chart types.
    Trend direction from Layer 4 is shown as a subtitle annotation.
    """
    x_col = selection["x_column"]
    y_cols = selection["y_columns"]
    title = selection.get("title", f"{', '.join(y_cols)} over {x_col}")

    x_vals = [str(row.get(x_col, "")) for row in rows]
    traces = []

    for i, y_col in enumerate(y_cols):
        color = PALETTE[i % len(PALETTE)]
        traces.append(
            {
                "type": "scatter",
                "mode": "lines+markers",
                "name": y_col,
                "x": x_vals,
                "y": [row.get(y_col) for row in rows],
                "line": {"color": color, "width": 2},
                "marker": {"color": color, "size": 5},
                "hovertemplate": f"<b>%{{x}}</b><br>{y_col}: %{{y:,.2f}}<extra></extra>",
            }
        )

    annotations = list(intel.get("annotations", []))
    trend = intel.get("trend", {})
    direction = trend.get("direction", "")
    if direction not in (None, "", "not_applicable", "insufficient_data"):
        arrow = (
            "↗"
            if "growing" in direction
            else ("↘" if "declining" in direction else "→")
        )
        annotations.append(
            {
                "xref": "paper",
                "yref": "paper",
                "x": 0.02,
                "y": 1.06,
                "text": f"{arrow} {direction.replace('_', ' ')} trend",
                "showarrow": False,
                "font": {"size": 11, "color": "#888780"},
            }
        )

    layout = _base_layout(title=title, annotations=annotations)
    layout["xaxis"] = _axis_style(x_col, show_grid=False)
    layout["yaxis"] = _axis_style("")
    layout["showlegend"] = len(y_cols) > 1
    layout["legend"] = {"orientation": "h", "y": -0.18}

    return {"data": traces, "layout": layout}


def _build_area(rows, profile, selection, intel) -> dict:
    """
    Area chart — like line but with fill to zero.
    Good for showing volume or cumulative value over time.
    """
    spec = _build_line(rows, profile, selection, intel)
    for trace in spec["data"]:
        trace["fill"] = "tozeroy"
        # Add 12% alpha to the line color for the fill
        base_color = trace["line"]["color"]
        trace["fillcolor"] = base_color + "1F"
    return spec


def _build_pie(rows, profile, selection, intel) -> dict:
    """
    Donut chart — labels from x column, values from y column.
    Donut style (hole=0.42) reads cleaner than a full pie.
    """
    x_col = selection["x_column"]
    y_col = (selection["y_columns"] or [None])[0]

    if not y_col:
        return _build_table(rows, profile, selection, intel)

    labels = [str(row.get(x_col, "")) for row in rows]  # x_col should be "name"
    values = [row.get(y_col, 0) for row in rows]

    labels = [str(row.get(x_col, "")) for row in rows]
    values = [row.get(y_col, 0) for row in rows]

    trace = {
        "type": "pie",
        "labels": labels,
        "values": values,
        "hole": 0.42,
        "marker": {"colors": PALETTE[: len(labels)]},
        "textinfo": "label+percent",
        "textfont": {"size": 11},
        "hovertemplate": "<b>%{label}</b><br>%{value:,.2f} (%{percent})<extra></extra>",
    }

    layout = _base_layout(
        title=selection.get("title", f"{y_col} by {x_col}"),
        annotations=intel.get("annotations", []),
    )
    layout["showlegend"] = True
    layout["legend"] = {"orientation": "h", "y": -0.1}

    return {"data": [trace], "layout": layout}


def _build_scatter(rows, profile, selection, intel) -> dict:
    """
    Scatter plot — x and y are both numeric.
    Uses selection['color_by'] (added in Layer 3) for categorical color encoding.
    Correlation strength from Layer 4 is shown as a subtitle.
    """
    x_col = selection["x_column"]
    y_col = (selection["y_columns"] or [None])[0]
    # color_by = selection.get("color_by")   # optional — may be None
    color_by = selection.get("color_column")

    if not y_col:
        return _build_table(rows, profile, selection, intel)

    labels = [str(row.get(color_by or x_col, "")) for row in rows]

    if color_by and profile.get(color_by, {}).get("type") == "categorical":
        unique_cats = list(dict.fromkeys(labels))
        color_map = {c: PALETTE[i % len(PALETTE)] for i, c in enumerate(unique_cats)}
        colors = [color_map[l] for l in labels]
    else:
        colors = PALETTE[0]

    trace = {
        "type": "scatter",
        "mode": "markers",
        "x": [row.get(x_col) for row in rows],
        "y": [row.get(y_col) for row in rows],
        "text": labels,
        "marker": {"color": colors, "size": 8, "opacity": 0.75},
        "hovertemplate": f"<b>%{{text}}</b><br>{x_col}: %{{x:,.2f}}<br>{y_col}: %{{y:,.2f}}<extra></extra>",
    }

    annotations = list(intel.get("annotations", []))
    for corr in intel.get("correlations", []):
        if {corr["col_a"], corr["col_b"]} == {x_col, y_col}:
            annotations.append(
                {
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.02,
                    "y": 1.06,
                    "text": f"r = {corr['r']}  ({corr['strength']} {corr['direction']} correlation)",
                    "showarrow": False,
                    "font": {"size": 11, "color": "#888780"},
                }
            )

    layout = _base_layout(
        title=selection.get("title", f"{y_col} vs {x_col}"),
        annotations=annotations,
    )
    layout["xaxis"] = _axis_style(x_col)
    layout["yaxis"] = _axis_style(y_col)

    return {"data": [trace], "layout": layout}


def _build_heatmap(rows, profile, selection, intel) -> dict:
    """
    Heatmap — two categorical columns define the grid, one numeric fills the cells.
    Falls back to table if the data doesn't have the right shape.
    """
    cols = list(rows[0].keys()) if rows else []
    cat_cols = [c for c in cols if profile.get(c, {}).get("type") == "categorical"]
    num_cols = [c for c in cols if profile.get(c, {}).get("type") == "numeric"]

    if len(cat_cols) < 2 or not num_cols:
        logger.warning(
            "[L5] Heatmap requires 2 categoricals + 1 numeric — falling back to table"
        )
        return _build_table(rows, profile, selection, intel)

    row_col = cat_cols[0]
    col_col = cat_cols[1]
    val_col = num_cols[0]

    row_labels = sorted({str(r.get(row_col, "")) for r in rows})
    col_labels = sorted({str(r.get(col_col, "")) for r in rows})
    row_idx = {v: i for i, v in enumerate(row_labels)}
    col_idx = {v: i for i, v in enumerate(col_labels)}

    z = [[None] * len(col_labels) for _ in range(len(row_labels))]
    for row in rows:
        r = str(row.get(row_col, ""))
        c = str(row.get(col_col, ""))
        v = row.get(val_col)
        if r in row_idx and c in col_idx and v is not None:
            z[row_idx[r]][col_idx[c]] = v

    trace = {
        "type": "heatmap",
        "x": col_labels,
        "y": row_labels,
        "z": z,
        "colorscale": [[0, "#E1F5EE"], [0.5, "#1D9E75"], [1, "#04342C"]],
        "hovertemplate": f"{row_col}: %{{y}}<br>{col_col}: %{{x}}<br>{val_col}: %{{z:,.2f}}<extra></extra>",
    }

    layout = _base_layout(
        title=selection.get("title", f"{val_col} by {row_col} and {col_col}"),
        annotations=intel.get("annotations", []),
    )
    layout["xaxis"] = _axis_style(col_col, show_grid=False)
    layout["yaxis"] = _axis_style(row_col, show_grid=False)

    return {"data": [trace], "layout": layout}


def _build_funnel(rows, profile, selection, intel) -> dict:
    """
    Funnel chart — ordered stages (x) dropping to values (y).
    Rows are assumed to be already in stage order.
    """
    x_col = selection["x_column"]
    y_col = (selection["y_columns"] or [None])[0]

    if not y_col:
        return _build_table(rows, profile, selection, intel)

    trace = {
        "type": "funnel",
        "y": [str(row.get(x_col, "")) for row in rows],
        "x": [row.get(y_col, 0) for row in rows],
        "textposition": "inside",
        "textinfo": "value+percent initial",
        "marker": {"color": PALETTE[: len(rows)]},
        "hovertemplate": "<b>%{y}</b><br>%{x:,.2f}<extra></extra>",
    }

    layout = _base_layout(
        title=selection.get("title", f"{y_col} funnel"),
        annotations=intel.get("annotations", []),
    )

    return {"data": [trace], "layout": layout}


def _build_kpi(rows, profile, selection, intel) -> dict:
    """
    KPI indicator — single big number.
    Used when the query returns one aggregated row (count, sum, avg).
    """
    y_col = (selection["y_columns"] or [None])[0]
    value = None

    if y_col and rows:
        value = rows[0].get(y_col)

    if value is None and rows:
        for col, meta in profile.items():
            if meta.get("type") == "numeric" and rows[0].get(col) is not None:
                value = rows[0][col]
                y_col = col
                break

    trace = {
        "type": "indicator",
        "mode": "number",
        "value": value or 0,
        "title": {"text": y_col or "value", "font": {"size": 14}},
        "number": {"font": {"size": 52, "color": PALETTE[0]}},
    }

    layout = _base_layout(
        title=selection.get("title", ""),
        annotations=intel.get("annotations", []),
    )
    layout["margin"] = {"t": 60, "r": 20, "b": 20, "l": 20}

    return {"data": [trace], "layout": layout}


def _build_table(rows, profile, selection, intel) -> dict:
    """
    Fallback table — shows all columns and all rows.
    Used when no other chart type is appropriate,
    or when a builder fails and falls back.
    """
    if not rows:
        return {"data": [], "layout": _base_layout("No data")}

    cols = list(rows[0].keys())
    cells = [[row.get(c, "") for row in rows] for c in cols]

    trace = {
        "type": "table",
        "header": {
            "values": [f"<b>{c}</b>" for c in cols],
            "fill": {"color": "#EEEDFE"},  # purple-50
            "font": {"color": "#3C3489", "size": 12},  # purple-800
            "align": "left",
            "line": {"color": "#D3D1C7", "width": 0.5},
        },
        "cells": {
            "values": cells,
            "fill": {"color": ["#FFFFFF", "#F1EFE8"] * (len(cols) // 2 + 1)},
            "font": {"color": "#444441", "size": 11},
            "align": "left",
            "line": {"color": "#D3D1C7", "width": 0.5},
        },
    }

    layout = _base_layout(
        title=selection.get("title", "Data table"),
        annotations=intel.get("annotations", []),
    )
    layout["margin"] = {"t": 52, "r": 10, "b": 10, "l": 10}

    return {"data": [trace], "layout": layout}


# ── Router ─────────────────────────────────────────────────────────────────

_BUILDERS = {
    "bar": _build_bar,
    "line": _build_line,
    "multiline": _build_line,  # same builder handles multiple y columns
    "area": _build_area,
    "pie": _build_pie,
    "scatter": _build_scatter,
    "heatmap": _build_heatmap,
    "funnel": _build_funnel,
    "kpi": _build_kpi,
    "table": _build_table,
}


# ── Public API ─────────────────────────────────────────────────────────────


def build(
    rows: list[dict],
    profile: dict,
    selection: dict,
    intelligence: dict,
) -> dict:
    """
    Layer 5 — Build a Plotly-compatible chart spec.

    Args:
        rows:         Normalized flat rows from Layer 1
        profile:      Column profile from Layer 2:
                        {col → {type, cardinality, min, max, null_rate, sample_values}}
        selection:    Chart selection from Layer 3:
                        {
                            chart_type:    str,
                            x_column:      str,
                            y_columns:     list[str],
                            title:         str,
                            color_by:      str | None,   # for scatter
                            confidence:    float,
                            fallback_type: str,
                            reasoning:     str,
                        }
        intelligence: Analysis from Layer 4:
                        {
                            anomalies:    [...],
                            trend:        {...},
                            correlations: [...],
                            annotations:  [...],        # Plotly annotation objects
                        }

    Returns:
        {
            data:          [...],   # Plotly traces
            layout:        {...},   # Plotly layout
            config:        {...},   # Plotly config
            chart_type:    str,     # which builder ran (may differ if fallback used)
            fallback_used: bool,
        }
    """
    chart_type = selection.get("chart_type", "table")
    fallback_type = selection.get("fallback_type", "table")
    fallback_used = False

    logger.info(
        f"[L5/spec_builder] Building '{chart_type}' — "
        f"{len(rows)} rows  x={selection.get('x_column')}  y={selection.get('y_columns')}"
    )

    # ── Try primary builder ────────────────────────────────────────────
    builder = _BUILDERS.get(chart_type)
    spec = None

    if builder:
        try:
            spec = builder(rows, profile, selection, intelligence)
        except Exception as e:
            logger.error(f"[L5/spec_builder] Builder '{chart_type}' failed: {e}")
            spec = None

    # ── Fallback if primary returned nothing or errored ────────────────
    if not spec or not spec.get("data"):
        logger.warning(
            f"[L5/spec_builder] Falling back '{chart_type}' → '{fallback_type}'"
        )
        fallback_builder = _BUILDERS.get(fallback_type, _build_table)
        fallback_used = True
        try:
            spec = fallback_builder(rows, profile, selection, intelligence)
        except Exception as e:
            logger.error(
                f"[L5/spec_builder] Fallback '{fallback_type}' also failed: {e}"
            )
            spec = {"data": [], "layout": _base_layout("Could not render chart")}

    spec["config"] = _base_config()
    spec["chart_type"] = fallback_type if fallback_used else chart_type
    spec["fallback_used"] = fallback_used

    logger.info(
        f"[L5/spec_builder] Done — chart_type={spec['chart_type']}  "
        f"traces={len(spec.get('data', []))}  fallback={fallback_used}"
    )

    return spec
