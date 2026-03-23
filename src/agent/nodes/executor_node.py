# src/agent/nodes/executor_node.py

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from pymongo import MongoClient
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError
from bson import ObjectId
from decimal import Decimal
import datetime
import logging

from src.agent.state import AgentState
from config.settings import settings

logger = logging.getLogger(__name__)

# Words that cannot start a permitted query
FORBIDDEN_SQL_STARTS = (
    "insert", "update", "delete", "drop",
    "alter", "truncate", "create", "replace",
)


# ── Entry point ────────────────────────────────────────────────────────────

def executor_node(state: AgentState) -> AgentState:
    """
    LangGraph node — runs the query plan against the real database.
    No LLM. No retries. Pure execution and error reporting.

    Reads:
        state.query_type   — "sql" or "mongo"
        state.query        — SQL string or Mongo pipeline list
        state.collection   — Mongo collection name (None for SQL)

    Writes:
        state.success          — True if query ran without error
        state.data             — list of clean dicts (rows or documents)
        state.row_count        — number of items in data
        state.execution_error  — error message string if failed, else None
    """
    query_type = state.get("query_type", "sql")

    logger.info(
        f"[executor_node] Running {query_type} query "
        f"(retry={state.get('retry_count', 0)})"
    )

    if query_type == "sql":
        return _run_sql(state)
    elif query_type == "mongo":
        return _run_mongo(state)
    else:
        return _failure(state, f"Unknown query type: '{query_type}'")


# ── SQL runner ─────────────────────────────────────────────────────────────

def _run_sql(state: AgentState) -> AgentState:
    query = state.get("query", "")

    # ── Safety check — block write operations before touching the DB ───
    first_word = query.strip().lower().split()[0] if query.strip() else ""
    if first_word in FORBIDDEN_SQL_STARTS:
        return _failure(
            state,
            f"Blocked: '{first_word.upper()}' is a write operation. "
            "Only SELECT queries are permitted."
        )

    if not query.strip():
        return _failure(state, "Empty SQL query received from planner.")

    logger.info(f"[executor_node] SQL:\n{query}")

    try:
        engine = create_engine(settings.sql_db_url)

        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows   = [dict(row._mapping) for row in result]

        engine.dispose()

        logger.info(f"[executor_node] SQL returned {len(rows)} rows")
        return _success(state, rows)

    except SQLAlchemyError as e:
        # First line of the error is the useful part
        error_msg = str(e).split("\n")[0].strip()
        logger.error(f"[executor_node] SQL error: {error_msg}")
        return _failure(state, error_msg)

    except Exception as e:
        logger.error(f"[executor_node] Unexpected SQL error: {e}")
        return _failure(state, str(e))


# ── Mongo runner ───────────────────────────────────────────────────────────

def _run_mongo(state: AgentState) -> AgentState:
    collection_name = state.get("collection")
    pipeline        = state.get("query", [])

    if not collection_name:
        return _failure(state, "No MongoDB collection specified in query plan.")

    if not isinstance(pipeline, list):
        return _failure(state, f"Mongo pipeline must be a list, got: {type(pipeline)}")

    logger.info(
        f"[executor_node] Mongo collection='{collection_name}' "
        f"pipeline={pipeline}"
    )

    try:
        client = MongoClient(
            settings.mongo_uri,
            serverSelectionTimeoutMS=5000,  # 5 second connection timeout
        )
        db  = client[settings.mongo_db_name]
        col = db[collection_name]

        raw  = list(col.aggregate(pipeline, maxTimeMS=30000))  # 30 second query timeout
        docs = [_serialize(doc) for doc in raw]

        client.close()

        logger.info(f"[executor_node] Mongo returned {len(docs)} documents")
        return _success(state, docs)

    except OperationFailure as e:
        error_msg = str(e).split("\n")[0].strip()
        logger.error(f"[executor_node] Mongo operation error: {error_msg}")
        return _failure(state, error_msg)

    except ServerSelectionTimeoutError:
        error_msg = "Could not connect to MongoDB — server selection timed out."
        logger.error(f"[executor_node] {error_msg}")
        return _failure(state, error_msg)

    except Exception as e:
        logger.error(f"[executor_node] Unexpected Mongo error: {e}")
        return _failure(state, str(e))


# ── Serializer ─────────────────────────────────────────────────────────────

def _serialize(value):
    """
    Recursively converts any non-JSON-serializable MongoDB type to a
    plain Python type so the state stays clean throughout the graph.

    Handles:
        ObjectId      → str
        datetime      → ISO format string
        Decimal128    → str
        dict          → recursively serialized dict
        list          → recursively serialized list
        everything else → returned as-is (int, float, str, bool, None)
    """
    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, datetime.datetime):
        return value.isoformat()

    if isinstance(value, datetime.date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}

    if isinstance(value, list):
        return [_serialize(item) for item in value]

    return value


# ── State helpers ──────────────────────────────────────────────────────────

def _success(state: AgentState, data: list) -> AgentState:
    """Writes a successful execution result into state."""
    return {
        **state,
        "success":         True,
        "data":            data,
        "row_count":       len(data),
        "execution_error": None,
    }


def _failure(state: AgentState, error_msg: str) -> AgentState:
    """Writes a failed execution result into state."""
    return {
        **state,
        "success":         False,
        "data":            [],
        "row_count":       0,
        "execution_error": error_msg,
    }