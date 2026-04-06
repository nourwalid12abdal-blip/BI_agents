# src/agent/nodes/charter/layer1_normalizer.py

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Date formats to try ────────────────────────────────────────────────────

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
]


# ── Helpers ────────────────────────────────────────────────────────────────

def _parse_date(value: str) -> datetime | None:
    """
    Tries every known date format against the string.
    Returns a datetime if any format matches, else None.
    """
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _flatten_dict(d: dict, parent_key: str = "", sep: str = "_") -> dict:
    """
    Recursively flattens nested dicts into a single level.

    Examples:
        {"specs": {"ram_gb": 16}}       → {"specs_ram_gb": 16}
        {"user": {"id": 1, "name": "A"}} → {"user_id": 1, "user_name": "A"}
    """
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(_flatten_dict(v, new_key, sep))
        else:
            items[new_key] = v
    return items


def _clean_value(value):
    """
    Cleans a single value:
    - Single-item primitive arrays → unwrap to the scalar
    - Date strings                 → ISO format string
    - Everything else              → returned as-is
    """
    # Unwrap single-item primitive arrays
    if isinstance(value, list):
        if len(value) == 1 and not isinstance(value[0], (dict, list)):
            return value[0]
        return value  # multi-item or nested arrays stay as-is

    # Cast numeric strings to int or float
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass

    # Parse date strings and return as ISO
    if isinstance(value, str):
        parsed = _parse_date(value)
        if parsed:
            return parsed.isoformat()

    return value


# ── Public API ─────────────────────────────────────────────────────────────

def normalize(data: list[dict]) -> list[dict]:
    """
    Layer 1 — Normalize raw executor output into clean flat rows.

    Steps applied to every row:
        1. Flatten nested dicts          (specs.ram_gb → specs_ram_gb)
        2. Unwrap single-item arrays     ([42] → 42)
        3. Parse date strings            ("2024-01-10" → "2024-01-10T00:00:00")
        4. Preserve nulls as None        (no imputation — Layer 2 measures null rate)

    Args:
        data: Raw list of dicts from the executor node.

    Returns:
        Clean list of flat dicts — same length as input, always.
    """
    if not data:
        logger.warning("[L1/normalizer] Received empty data — returning []")
        return []

    normalized = []

    for row in data:
        # Step 1: flatten nested dicts
        flat = _flatten_dict(row)

        # Steps 2 + 3: clean each value
        cleaned = {k: _clean_value(v) for k, v in flat.items()}

        normalized.append(cleaned)

    logger.info(
        f"[L1/normalizer] Normalized {len(normalized)} rows — "
        f"columns: {list(normalized[0].keys()) if normalized else []}"
    )

    return normalized