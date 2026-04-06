# src/agent/charter/layer4_intelligence.py

import math
import logging

logger = logging.getLogger(__name__)

# ── Thresholds ─────────────────────────────────────────────────────────────

ANOMALY_Z_SCORE = 1.8  # standard deviations to flag as anomaly
TREND_STRONG_PCT = 0.10  # >10% change per step = strong trend
TREND_WEAK_PCT = 0.02  # >2%  change per step = weak trend
CORRELATION_STRONG = 0.8  # |r| > 0.8 = strong correlation
CORRELATION_MODERATE = 0.5  # |r| > 0.5 = moderate correlation
MIN_POINTS_FOR_TREND = 3  # need at least 3 points for trend analysis
MIN_POINTS_FOR_CORR = 4  # need at least 4 points for correlation


# ── Statistics helpers ─────────────────────────────────────────────────────


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: list[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    """
    Pearson correlation coefficient between two numeric lists.
    Returns None if calculation is not possible.
    """
    n = len(xs)
    if n < MIN_POINTS_FOR_CORR or len(ys) != n:
        return None

    mean_x, mean_y = _mean(xs), _mean(ys)
    std_x = _std(xs, mean_x)
    std_y = _std(ys, mean_y)

    if std_x == 0 or std_y == 0:
        return None

    covariance = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n)) / n

    return round(covariance / (std_x * std_y), 4)


# ── Anomaly detection ──────────────────────────────────────────────────────


def detect_anomalies(
    rows: list[dict],
    profile: dict,
    x_column: str,
    y_columns: list[str],
) -> list[dict]:
    """
    Flags data points where a numeric value is more than
    ANOMALY_Z_SCORE standard deviations from the column mean.

    Args:
        rows:      Normalized data rows
        profile:   Column profile from Layer 2
        x_column:  The column used as labels (categorical or temporal)
        y_columns: The numeric columns to check

    Returns:
        List of anomaly dicts:
        [
            {
                "column":  "total_orders",
                "label":   "Alice",
                "value":   10,
                "mean":    1.5,
                "z_score": 3.2,
                "direction": "high"  | "low"
            }
        ]
    """

    if not y_columns:
        return []
    anomalies = []

    for col in y_columns:
        if profile.get(col, {}).get("type") != "numeric":
            continue

        values = [
            row[col]
            for row in rows
            if row.get(col) is not None and isinstance(row.get(col), (int, float))
        ]

        if len(values) < 3:
            continue

        col_mean = _mean(values)
        col_std = _std(values, col_mean)

        if col_std == 0:
            continue

        for row in rows:
            val = row.get(col)
            if val is None or not isinstance(val, (int, float)):
                continue

            z = (val - col_mean) / col_std

            if abs(z) >= ANOMALY_Z_SCORE:
                label = str(row.get(x_column, "unknown"))
                anomalies.append(
                    {
                        "column": col,
                        "label": label,
                        "value": val,
                        "mean": round(col_mean, 4),
                        "z_score": round(z, 4),
                        "direction": "high" if z > 0 else "low",
                    }
                )

    if anomalies:
        logger.info(f"[L4/intelligence] Anomalies detected: {len(anomalies)}")
        for a in anomalies:
            logger.info(
                f"  {a['label']}.{a['column']} = {a['value']} "
                f"(z={a['z_score']}, direction={a['direction']})"
            )
    else:
        logger.info("[L4/intelligence] No anomalies detected")

    return anomalies


# ── Trend detection ────────────────────────────────────────────────────────


def detect_trend(
    rows: list[dict],
    profile: dict,
    x_column: str,
    y_columns: list[str],
) -> dict:
    """
    Detects the trend direction for time-series or ordered numeric data.
    Uses the slope of a simple linear regression across the y values.

    Args:
        rows:      Normalized data rows (assumed to be in order)
        profile:   Column profile from Layer 2
        x_column:  The time or sequence column
        y_columns: Numeric columns to analyze

    Returns:
        {
            "direction":   "strongly_growing" | "growing" | "flat" |
                           "declining" | "strongly_declining" | "mixed" | "insufficient_data",
            "per_column":  {"col_name": "growing", ...},
            "slope":       0.42,
            "pct_change":  0.34,
            "reversal":    True | False
        }
    """
    if not y_columns or len(rows) < MIN_POINTS_FOR_TREND:
        return {
            "direction": "insufficient_data",
            "per_column": {},
            "slope": None,
            "pct_change": None,
            "reversal": False,
        }

    per_column = {}
    all_slopes = []
    for col in y_columns:
        if profile.get(col, {}).get("type") != "numeric":
            continue

        values = [
            row.get(col)
            for row in rows
            if row.get(col) is not None and isinstance(row.get(col), (int, float))
        ]

        if len(values) < MIN_POINTS_FOR_TREND:
            continue

        # Simple linear regression slope
        n = len(values)
        xs = list(range(n))
        mean_x = _mean(xs)
        mean_y = _mean(values)

        numerator = sum((xs[i] - mean_x) * (values[i] - mean_y) for i in range(n))
        denominator = sum((xs[i] - mean_x) ** 2 for i in range(n))
        slope = numerator / denominator if denominator != 0 else 0.0

        # Express slope as % of mean per step
        pct_per_step = (slope / mean_y) if mean_y != 0 else 0.0
        all_slopes.append(pct_per_step)

        # Classify this column's trend
        if abs(pct_per_step) < TREND_WEAK_PCT:
            direction = "flat"
        elif pct_per_step >= TREND_STRONG_PCT:
            direction = "strongly_growing"
        elif pct_per_step > TREND_WEAK_PCT:
            direction = "growing"
        elif pct_per_step <= -TREND_STRONG_PCT:
            direction = "strongly_declining"
        else:
            direction = "declining"

        per_column[col] = direction

        # Detect recent reversal (last step goes opposite to overall slope)
        if len(values) >= 4:
            recent_change = values[-1] - values[-2]
            overall_positive = slope > 0
            recent_positive = recent_change > 0
            per_column[col + "_reversal"] = overall_positive != recent_positive

    if not all_slopes:
        return {
            "direction": "insufficient_data",
            "per_column": per_column,
            "slope": None,
            "pct_change": None,
            "reversal": False,
        }

    # Overall direction = average slope across all y columns
    avg_slope = _mean(all_slopes)

    if abs(avg_slope) < TREND_WEAK_PCT:
        overall = "flat"
    elif avg_slope >= TREND_STRONG_PCT:
        overall = "strongly_growing"
    elif avg_slope > TREND_WEAK_PCT:
        overall = "growing"
    elif avg_slope <= -TREND_STRONG_PCT:
        overall = "strongly_declining"
    else:
        overall = "declining"

    # Check for reversal across any column
    any_reversal = any(v for k, v in per_column.items() if k.endswith("_reversal"))

    result = {
        "direction": overall,
        "per_column": {
            k: v for k, v in per_column.items() if not k.endswith("_reversal")
        },
        "slope": round(avg_slope, 6),
        "pct_change": round(avg_slope * 100, 2),
        "reversal": any_reversal,
    }

    logger.info(
        f"[L4/intelligence] Trend: {overall} "
        f"(slope={result['slope']}, reversal={any_reversal})"
    )

    return result


# ── Correlation detection ──────────────────────────────────────────────────


def detect_correlations(
    rows: list[dict],
    profile: dict,
    y_columns: list[str],
) -> list[dict]:
    """
    Calculates Pearson correlation between every pair of numeric columns.
    Only reports correlations above CORRELATION_MODERATE threshold.

    Args:
        rows:      Normalized data rows
        profile:   Column profile from Layer 2
        y_columns: Numeric columns to compare

    Returns:
        List of correlation dicts:
        [
            {
                "col_a":    "price",
                "col_b":    "quantity",
                "r":        -0.87,
                "strength": "strong",
                "direction": "negative"
            }
        ]
    """
    if not y_columns:
        return []
    numeric_cols = [c for c in y_columns if profile.get(c, {}).get("type") == "numeric"]

    if len(numeric_cols) < 2:
        return []

    correlations = []

    # Check every pair
    for i in range(len(numeric_cols)):
        for j in range(i + 1, len(numeric_cols)):
            col_a = numeric_cols[i]
            col_b = numeric_cols[j]

            xs = [
                row[col_a]
                for row in rows
                if row.get(col_a) is not None
                and row.get(col_b) is not None
                and isinstance(row.get(col_a), (int, float))
            ]
            ys = [
                row[col_b]
                for row in rows
                if row.get(col_a) is not None
                and row.get(col_b) is not None
                and isinstance(row.get(col_b), (int, float))
            ]

            r = _pearson(xs, ys)
            if r is None:
                continue

            abs_r = abs(r)
            if abs_r < CORRELATION_MODERATE:
                continue

            strength = "strong" if abs_r >= CORRELATION_STRONG else "moderate"
            direction = "positive" if r > 0 else "negative"

            correlations.append(
                {
                    "col_a": col_a,
                    "col_b": col_b,
                    "r": r,
                    "strength": strength,
                    "direction": direction,
                }
            )

            logger.info(
                f"[L4/intelligence] Correlation: {col_a} ↔ {col_b} "
                f"r={r} ({strength} {direction})"
            )

    return correlations


# ── Plotly annotation builder ──────────────────────────────────────────────


def build_annotations(anomalies: list[dict]) -> list[dict]:
    """
    Converts anomaly dicts into Plotly annotation objects
    that can be dropped directly into the layout.annotations list.

    Each anomaly becomes a red arrow pointing at the anomalous point.
    """
    annotations = []

    for a in anomalies:
        direction_text = (
            "unusually high" if a["direction"] == "high" else "unusually low"
        )

        annotations.append(
            {
                "x": a["label"],
                "y": a["value"],
                "text": f"{direction_text} (z={a['z_score']:.1f})",
                "showarrow": True,
                "arrowhead": 2,
                "arrowcolor": "#E24B4A",
                "font": {
                    "color": "#E24B4A",
                    "size": 11,
                },
                "bgcolor": "rgba(252,235,235,0.85)",
                "bordercolor": "#E24B4A",
                "borderwidth": 1,
                "ax": 0,
                "ay": -36,
            }
        )

    return annotations


# ── Public API ─────────────────────────────────────────────────────────────


def analyze(
    rows: list[dict],
    profile: dict,
    selection: dict,
) -> dict:
    """
    Layer 4 — Run all three intelligence analyses on the data.

    Args:
        rows:      Normalized data rows from Layer 1
        profile:   Column profile from Layer 2
        selection: Chart selection from Layer 3

    Returns:
        {
            "anomalies":     [...],
            "trend":         {...},
            "correlations":  [...],
            "annotations":   [...]   # Plotly annotation objects
        }
    """
    x_column = selection.get("x_column", "")
    y_columns = selection.get("y_columns", [])
    chart_type = selection.get("chart_type", "table")

    logger.info(
        f"[L4/intelligence] Analyzing {len(rows)} rows — "
        f"chart={chart_type} x={x_column} y={y_columns}"
    )
    # print(f"profifel{profile}")
    # ── Anomaly detection ──────────────────────────────────────────────
    anomalies = detect_anomalies(rows, profile, x_column, y_columns)

    # ── Trend detection — only for time series ─────────────────────────
    is_time_series = (
        chart_type in ("line", "multiline", "area")
        or profile.get(x_column, {}).get("type") == "temporal"
    )
    trend = (
        detect_trend(rows, profile, x_column, y_columns)
        if is_time_series
        else {
            "direction": "not_applicable",
            "per_column": {},
            "slope": None,
            "pct_change": None,
            "reversal": False,
        }
    )

    # ── Correlation — only when 2+ numeric columns ─────────────────────
    correlations = detect_correlations(rows, profile, y_columns)

    # ── Build Plotly annotations from anomalies ─────────────────────────
    annotations = build_annotations(anomalies)

    return {
        "anomalies": anomalies,
        "trend": trend,
        "correlations": correlations,
        "annotations": annotations,
    }
