# src/agent/nodes/merge_node.py

from src.agent.state import AgentState
from src.schema.schema_store import load
import logging

logger = logging.getLogger(__name__)


# ── Join key finder ────────────────────────────────────────────────────────

def _find_join_key(
    sql_rows:   list[dict],
    mongo_rows: list[dict],
    graph,
) -> tuple[str, str] | None:
    """
    Finds the best column to join SQL and Mongo results on.

    Strategy (priority order):
      1. Cross-source relations from schema graph (highest confidence)
      2. Column name that exists in both result sets (fallback)

    Returns (sql_col, mongo_col) or None if nothing found.
    """
    if not sql_rows or not mongo_rows:
        return None

    sql_cols   = set(sql_rows[0].keys())
    mongo_cols = set(mongo_rows[0].keys())

    # Priority 1 — use schema cross-source relations
    for rel in graph.cross_source_relations:
        if rel.sql_column in sql_cols and rel.mongo_field in mongo_cols:
            logger.info(
                f"[merge_node] Join key from schema: "
                f"sql.{rel.sql_column} ↔ mongo.{rel.mongo_field} [{rel.confidence}]"
            )
            return (rel.sql_column, rel.mongo_field)

    # Priority 2 — same column name in both sets
    common = sql_cols & mongo_cols
    if common:
        key = sorted(common)[0]
        logger.warning(
            f"[merge_node] No schema relation found — "
            f"falling back to shared column name: '{key}'"
        )
        return (key, key)

    logger.error(
        f"[merge_node] No join key found. "
        f"SQL cols: {sql_cols}  Mongo cols: {mongo_cols}"
    )
    return None


# ── Merger ─────────────────────────────────────────────────────────────────

def _merge(
    sql_rows:   list[dict],
    mongo_rows: list[dict],
    sql_key:    str,
    mongo_key:  str,
) -> list[dict]:
    """
    Left-joins SQL rows with Mongo rows on the given key pair.

    - Every SQL row appears in the output (left join).
    - Mongo fields are merged in when the key matches.
    - If a SQL row has no matching Mongo row, Mongo fields are None.
    - Conflicting column names get a _mongo suffix to avoid overwriting.

    Example:
        SQL:   [{"id": 1, "name": "Alice", "orders": 2}]
        Mongo: [{"customer_id": 1, "event": "checkout"}]
        Key:   sql.id ↔ mongo.customer_id
        Result:[{"id": 1, "name": "Alice", "orders": 2, "event": "checkout"}]
    """
    # Build Mongo lookup index keyed by the join value
    mongo_index: dict = {}
    for row in mongo_rows:
        key_val = row.get(mongo_key)
        if key_val is not None:
            # Multiple Mongo rows can match one SQL row — keep a list
            mongo_index.setdefault(key_val, []).append(row)

    merged = []

    for sql_row in sql_rows:
        key_val      = sql_row.get(sql_key)
        mongo_matches = mongo_index.get(key_val, [None])

        for mongo_row in mongo_matches:
            combined = dict(sql_row)  # start with all SQL fields

            if mongo_row:
                for k, v in mongo_row.items():
                    if k == mongo_key:
                        continue  # skip the join key from Mongo side
                    # Avoid overwriting SQL columns
                    out_key = k if k not in combined else f"{k}_mongo"
                    combined[out_key] = v

            merged.append(combined)

    logger.info(
        f"[merge_node] Merged {len(sql_rows)} SQL rows × "
        f"{len(mongo_rows)} Mongo rows → {len(merged)} combined rows"
    )
    return merged


# ── Node ───────────────────────────────────────────────────────────────────

def merge_node(state: AgentState) -> AgentState:
    """
    LangGraph node — joins SQL and Mongo results into a single dataset.

    Only runs when source="both". Reads sql_data and mongo_data from state,
    finds the join key using the schema graph, merges the rows, and writes
    the combined result into state.data so the rest of the pipeline
    (charter, format) sees one unified dataset.

    Reads:
        state.sql_data    — rows from SQL executor
        state.mongo_data  — rows from Mongo executor

    Writes:
        state.data        — merged rows
        state.row_count   — number of merged rows
        state.success     — True if merge produced rows, False if empty
        state.execution_error — set if merge failed
    """
    sql_rows   = state.get("sql_data",   [])
    mongo_rows = state.get("mongo_data", [])

    logger.info(
        f"[merge_node] Merging — "
        f"sql={len(sql_rows)} rows  mongo={len(mongo_rows)} rows"
    )

    # ── Guard: nothing to merge ────────────────────────────────────────
    if not sql_rows and not mongo_rows:
        logger.warning("[merge_node] Both sql_data and mongo_data are empty")
        return {
            **state,
            "data":            [],
            "row_count":       0,
            "success":         False,
            "execution_error": "Both SQL and Mongo queries returned no data.",
        }

    # If only one side has data, just use it directly
    if not sql_rows:
        logger.warning("[merge_node] SQL data empty — using Mongo data only")
        return {
            **state,
            "data":            mongo_rows,
            "row_count":       len(mongo_rows),
            "success":         True,
            "execution_error": None,
        }

    if not mongo_rows:
        logger.warning("[merge_node] Mongo data empty — using SQL data only")
        return {
            **state,
            "data":            sql_rows,
            "row_count":       len(sql_rows),
            "success":         True,
            "execution_error": None,
        }

    # ── Load schema graph for join key detection ───────────────────────
    try:
        graph = load()
    except FileNotFoundError:
        logger.error("[merge_node] Schema graph not found — cannot detect join key")
        return {
            **state,
            "data":            sql_rows,   # fall back to SQL only
            "row_count":       len(sql_rows),
            "success":         True,
            "execution_error": "Schema graph missing — merged SQL only.",
        }

    # ── Find join key ──────────────────────────────────────────────────
    join_key = _find_join_key(sql_rows, mongo_rows, graph)

    if not join_key:
        # No join key — concatenate side by side as best effort
        logger.warning("[merge_node] No join key — concatenating SQL + Mongo rows")
        combined = sql_rows + mongo_rows
        return {
            **state,
            "data":            combined,
            "row_count":       len(combined),
            "success":         True,
            "execution_error": None,
        }

    sql_key, mongo_key = join_key

    # ── Merge ──────────────────────────────────────────────────────────
    try:
        merged = _merge(sql_rows, mongo_rows, sql_key, mongo_key)

        if not merged:
            logger.warning("[merge_node] Merge produced 0 rows — no matching keys")
            return {
                **state,
                "data":            [],
                "row_count":       0,
                "success":         False,
                "execution_error": (
                    f"No matching rows found when joining on "
                    f"sql.{sql_key} ↔ mongo.{mongo_key}."
                ),
            }

        return {
            **state,
            "data":            merged,
            "row_count":       len(merged),
            "success":         True,
            "execution_error": None,
        }

    except Exception as e:
        logger.error(f"[merge_node] Merge failed: {e}")
        return {
            **state,
            "data":            sql_rows,   # fall back to SQL only
            "row_count":       len(sql_rows),
            "success":         True,
            "execution_error": f"Merge error: {e} — using SQL data only.",
        }