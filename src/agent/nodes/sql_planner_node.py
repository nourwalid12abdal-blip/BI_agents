# src/agent/nodes/sql_planner_node.py

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from src.agent.state import AgentState
from src.agent.llm import get_llm
from src.schema.schema_store import load
import re
import logging

logger = logging.getLogger(__name__)


# ── Prompt ─────────────────────────────────────────────────────────────────

SQL_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        """You are an expert SQL query writer for a BI system.

Database schema — use EXACT table and column names as written below:
{schema}

Rules:
- Write ONLY a SELECT query — never INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE
- Use the exact table and column names from the schema above — do not invent names
- Add LIMIT 1000 unless the query uses COUNT, SUM, AVG, MIN, MAX, or GROUP BY
- Use table aliases when joining multiple tables
- Return ONLY the raw SQL — no explanation, no markdown, no code fences

{error_section}""",
    ),
    (
        "human",
        "{question}"
    ),
])

# ── Node function ──────────────────────────────────────────────────────────

def sql_planner_node(state: AgentState) -> AgentState:
    """
    LangGraph node — generates a SQL SELECT query for the question.

    Reads:
        state.question
        state.entities       — which tables to focus on
        state.error_feedback — error from previous attempt (if retry)

    Writes:
        state.query_type     — always "sql"
        state.query          — the generated SELECT statement
        state.collection     — always None (SQL has no collection)
        state.query_reasoning
    """
    logger.info(
        f"[sql_planner_node] Planning SQL "
        f"(retry={state.get('retry_count', 0)}) "
        f"entities={state.get('entities')}"
    )

    graph       = load()
    print(graph)
    schema_text = _build_schema_text(graph.sql, state.get("entities", []))
    error_section = _build_error_section(state.get("error_feedback"))

    chain = SQL_PROMPT | get_llm(temperature=0.0) | StrOutputParser()

    try:
        raw   = chain.invoke({
            "schema":        schema_text,
            "question":      state["question"],
            "error_section": error_section,
        })

        query = _clean_sql(raw)

        logger.info(f"[sql_planner_node] Generated query:\n{query}")

        return {
            **state,
            "query_type":      "sql",
            "query":           query,
            "collection":      None,
            "query_reasoning": f"SQL query on tables: {state.get('entities')}",
        }

    except Exception as e:
        logger.error(f"[sql_planner_node] Failed: {e}")

        # Safe fallback — SELECT everything from the first entity
        first_table = state.get("entities", [""])[0]
        fallback    = f"SELECT * FROM {first_table} LIMIT 10"

        return {
            **state,
            "query_type":      "sql",
            "query":           fallback,
            "collection":      None,
            "query_reasoning": f"Fallback query due to error: {e}",
        }


# ── Schema builder ─────────────────────────────────────────────────────────

def _build_schema_text(sql_schema: dict, entities: list) -> str:
    """
    Builds a DDL-style schema string for ONLY the relevant tables.
    Keeps the prompt focused — the model should not see tables it
    does not need and risk hallucinating columns from them.

    Falls back to full schema if no entities match.
    """
    tables_to_use = (
        {e: sql_schema[e] for e in entities if e in sql_schema}
        or sql_schema
    )

    lines = []

    for table, schema in tables_to_use.items():
        col_lines = []
        for c in schema.columns:
            parts = [c.name, c.type]
            if c.name in schema.primary_keys:
                parts.append("PRIMARY KEY")
            if not c.nullable:
                parts.append("NOT NULL")
            col_lines.append("  " + " ".join(parts))

        fk_lines = [
            f"  -- {fk.column} references {fk.ref_table}.{fk.ref_column}"
            for fk in schema.foreign_keys
        ]

        lines.append(f"CREATE TABLE {table} (")
        lines.extend(col_lines)
        if fk_lines:
            lines.extend(fk_lines)
        lines.append(f");  -- {schema.row_count} rows")
        lines.append("")

    return "\n".join(lines)


# ── Error section builder ──────────────────────────────────────────────────

def _build_error_section(error_feedback: str | None) -> str:
    """
    Returns an error section to inject into the prompt on retry.
    Empty string on the first attempt so the prompt stays clean.
    """
    if not error_feedback:
        return ""
    return (
        f"\nPREVIOUS ATTEMPT FAILED with this error:\n"
        f"  {error_feedback}\n"
        f"Fix the query to avoid this error. "
        f"Double-check table names, column names, and syntax."
    )


# ── SQL cleaner ────────────────────────────────────────────────────────────

FORBIDDEN = ("insert", "update", "delete", "drop", "alter", "truncate", "create")

def _clean_sql(raw: str) -> str:
    """
    Three steps:
    1. Strip markdown fences
    2. Extract the first SELECT statement
    3. Add LIMIT 1000 if the query is not an aggregation
    """
    text = raw.strip()

    # ── Step 1: strip markdown fences ─────────────────────────────────
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            part = part.strip()
            if part.lower().startswith("sql"):
                part = part[3:].strip()
            if part.upper().startswith("SELECT"):
                text = part
                break

    text = text.strip()

    # ── Step 2: extract first SELECT block ────────────────────────────
    match = re.search(r'(SELECT\s.+?)(?:;|$)', text, re.DOTALL | re.IGNORECASE)
    if match:
        text = match.group(1).strip()

    # ── Step 3: block write operations ────────────────────────────────
    first_word = text.strip().lower().split()[0] if text.strip() else ""
    if first_word in FORBIDDEN:
        raise ValueError(
            f"Blocked: '{first_word.upper()}' is a write operation. "
            "Only SELECT queries are permitted."
        )

    # ── Step 4: add LIMIT if needed ───────────────────────────────────
    return _ensure_limit(text)


def _ensure_limit(sql: str) -> str:
    """
    Adds LIMIT 1000 to queries that:
    - Are not aggregations (no COUNT, SUM, AVG, MIN, MAX, GROUP BY)
    - Do not already have a LIMIT clause
    """
    upper     = sql.upper()
    has_agg   = any(k in upper for k in (
        "COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "GROUP BY"
    ))
    has_limit = "LIMIT" in upper

    if not has_agg and not has_limit:
        return sql.rstrip(";") + "\nLIMIT 1000"

    return sql