# src/agent/nodes/charter/layer2_classifier.py

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────

TYPE_THRESHOLD  = 0.7   # 70% of non-null values must match to assign a type
MAX_SAMPLES     = 5     # number of sample values to store per column


# ── Date detector ──────────────────────────────────────────────────────────

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
]

def _is_date_string(value: str) -> bool:
    """Returns True if the string matches any known date format."""
    for fmt in DATE_FORMATS:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


# ── Type detectors ─────────────────────────────────────────────────────────

def _detect_type(values: list) -> str:
    """
    Scans actual values and returns the dominant type.

    Priority: temporal > numeric > categorical
    A type wins if >= 70% of non-null values match it.
    """
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "categorical"

    total = len(non_null)

    # Check temporal
    date_hits = sum(
        1 for v in non_null
        if isinstance(v, str) and _is_date_string(v)
    )
    if date_hits / total >= TYPE_THRESHOLD:
        return "temporal"

    # Check numeric
    numeric_hits = sum(
        1 for v in non_null
        if isinstance(v, (int, float)) and not isinstance(v, bool)
    )
    if numeric_hits / total >= TYPE_THRESHOLD:
        return "numeric"

    return "categorical"


# ── Measurements ───────────────────────────────────────────────────────────

def _cardinality(values: list) -> int:
    """Number of unique non-null values."""
    return len(set(str(v) for v in values if v is not None))


def _null_rate(values: list) -> float:
    """Fraction of values that are None. 0.0 = no nulls, 1.0 = all null."""
    if not values:
        return 0.0
    return round(sum(1 for v in values if v is None) / len(values), 3)


def _value_range(values: list) -> list | None:
    """[min, max] for numeric columns. None for everything else."""
    nums = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if not nums:
        return None
    return [min(nums), max(nums)]


def _monotonicity(values: list) -> str | None:
    """
    Compares consecutive numeric values to detect direction.

    Returns:
        "increasing" — every step goes up or stays flat
        "decreasing" — every step goes down or stays flat
        "flat"       — all values identical
        "mixed"      — no consistent direction
        None         — not enough numeric values to evaluate
    """
    nums = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if len(nums) < 2:
        return None

    diffs = [nums[i + 1] - nums[i] for i in range(len(nums) - 1)]

    if all(d == 0 for d in diffs):
        return "flat"
    if all(d >= 0 for d in diffs):
        return "increasing"
    if all(d <= 0 for d in diffs):
        return "decreasing"
    return "mixed"


def _sample_values(values: list, n: int = MAX_SAMPLES) -> list:
    """
    Returns up to n unique non-null sample values.
    Deduplicates so the LLM sees variety, not repeats.
    """
    seen = set()
    samples = []
    for v in values:
        if v is None:
            continue
        key = str(v)
        if key not in seen:
            seen.add(key)
            samples.append(v)
        if len(samples) >= n:
            break
    return samples


# ── X / Y suggestion ──────────────────────────────────────────────────────

def _suggest_axes(profile: dict) -> dict:
    """
    Suggests the best x and y columns based on the profile.

    X rules (priority order):
        1. Temporal column   — time is almost always the best x axis
        2. Lowest cardinality categorical — fewest unique values = cleaner chart

    Y rules:
        1. Numeric column with the largest value range — most meaningful to plot
        2. Fall back to first numeric column found
    """
    temporal    = [c for c, p in profile.items() if p["type"] == "temporal"]
    categorical = [c for c, p in profile.items() if p["type"] == "categorical"]
    numeric     = [c for c, p in profile.items() if p["type"] == "numeric"]

    # Best x
    if temporal:
        x = temporal[0]
    elif categorical:
        x = min(categorical, key=lambda c: profile[c]["cardinality"])
    elif numeric:
        x = numeric[0]
    else:
        x = list(profile.keys())[0] if profile else ""

    # Best y — numeric with largest range
    if numeric:
        def range_size(col):
            r = profile[col]["value_range"]
            return (r[1] - r[0]) if r else 0
        y = max(numeric, key=range_size)
    elif categorical:
        y = categorical[0]
    else:
        cols = list(profile.keys())
        y = cols[1] if len(cols) > 1 else cols[0] if cols else ""

    return {"x_column": x, "y_column": y}


# ── Public API ─────────────────────────────────────────────────────────────

def classify(rows: list[dict]) -> dict:
    """
    Layer 2 — Profile every column in the normalized data.

    Args:
        rows: Clean flat rows from layer1_normalizer.normalize()

    Returns:
        A dict with one entry per column plus a _suggestions key:

        {
            "column_name": {
                "type":          "temporal" | "categorical" | "numeric",
                "cardinality":   int,
                "null_rate":     float,
                "value_range":   [min, max] | None,
                "monotonicity":  "increasing" | "decreasing" | "flat" | "mixed" | None,
                "sample_values": [val1, val2, ...]
            },
            "_suggestions": {
                "x_column": "best_x_column_name",
                "y_column": "best_y_column_name"
            }
        }
    """
    if not rows:
        logger.warning("[L2/classifier] Received empty rows — returning empty profile")
        return {}

    # Collect all values per column
    columns: dict[str, list] = {}
    for row in rows:
        for k, v in row.items():
            columns.setdefault(k, []).append(v)

    # Build profile per column
    profile = {}
    for col, values in columns.items():
        col_type = _detect_type(values)

        profile[col] = {
            "type":          col_type,
            "cardinality":   _cardinality(values),
            "null_rate":     _null_rate(values),
            "value_range":   _value_range(values) if col_type == "numeric" else None,
            "monotonicity":  _monotonicity(values) if col_type == "numeric" else None,
            "sample_values": _sample_values(values),
        }

    # Add axis suggestions
    profile["_suggestions"] = _suggest_axes(profile)

    logger.info(
        f"[L2/classifier] Classified {len(profile) - 1} columns — "
        f"types: { {k: v['type'] for k, v in profile.items() if k != '_suggestions'} }"
    )
    logger.info(f"[L2/classifier] Suggestions: {profile['_suggestions']}")

    return profile