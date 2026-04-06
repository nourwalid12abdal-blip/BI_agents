# # # src/agent/nodes/format_node.py

# # from langchain_core.prompts import ChatPromptTemplate
# # from langchain_core.output_parsers import StrOutputParser
# # from src.agent.state import AgentState
# # from src.agent.llm import get_llm
# # import json
# # import logging

# # logger = logging.getLogger(__name__)


# # # ── Prompt ─────────────────────────────────────────────────────────────────

# # # FORMAT_PROMPT = ChatPromptTemplate.from_messages([
# # #     (
# # #         "system",
# # #         """You are a helpful data analyst presenting query results to a business user.

# # # Your job is to write a clear, direct answer to the user's question based on the data returned.

# # # Rules:
# # # - Answer in 1 to 3 sentences maximum
# # # - Always mention specific numbers from the data
# # # - Be direct — start with the answer, not with "Based on the data..."
# # # - Do not repeat the question back to the user
# # # - Do not mention SQL, MongoDB, queries, or technical terms
# # # - Do not use markdown formatting
# # # - If the data shows something unexpected or notable, mention it""",
# # #     ),
# # #     (
# # #         "human",
# # #         """Question: {question}

# # # Data returned ({row_count} rows):
# # # {data_preview}

# # # Write a direct answer.""",
# # #     ),
# # # ])

# # FORMAT_PROMPT = ChatPromptTemplate.from_messages([
# #     (
# #         "system",
# #         """You are a data analyst presenting query results to a business user.

# # CRITICAL RULES:
# # - You MUST use ONLY the numbers from the data provided below — never invent or assume numbers
# # - Answer in 1 to 3 sentences maximum
# # - Be direct — start with the answer
# # - Do not mention SQL, MongoDB, queries, or technical terms
# # - Do not use markdown formatting""",
# #     ),
# #     (
# #         "human",
# #         """Question: {question}

# # Data returned ({row_count} rows):
# # {data_preview}

# # Write a direct answer using ONLY the numbers above.""",
# #     ),
# # ])


# # # ── Node function ──────────────────────────────────────────────────────────

# # def format_node(state: AgentState) -> AgentState:
# #     """
# #     LangGraph node — turns raw query results into a plain English answer.

# #     Reads:
# #         state.success
# #         state.data
# #         state.row_count
# #         state.question
# #         state.query
# #         state.execution_error
# #         state.final_error

# #     Writes:
# #         state.response
# #     """
# #     logger.info(f"[format_node] Formatting response — success={state.get('success')}")

# #     # ── Path 1: all retries exhausted — report failure ─────────────────
# #     if state.get("final_error"):
# #         return {
# #             **state,
# #             "response": (
# #                 f"I was unable to answer your question after multiple attempts. "
# #                 f"The last error was: {state['final_error']}. "
# #                 "Please try rephrasing your question or check that the data exists."
# #             ),
# #         }

# #     # ── Path 2: execution failed on final retry ────────────────────────
# #     if not state.get("success"):
# #         error = state.get("execution_error", "unknown error")
# #         return {
# #             **state,
# #             "response": (
# #                 f"I ran into an error while fetching your data: {error}. "
# #                 "Please try rephrasing your question."
# #             ),
# #         }

# #     # ── Path 3: query succeeded but returned no rows ───────────────────
# #     if state.get("row_count", 0) == 0:
# #         return {
# #             **state,
# #             "response": (
# #                 "The query ran successfully but returned no results. "
# #                 "There may be no data matching your question, or the "
# #                 "filters you described do not match any records."
# #             ),
# #         }

# #     # ── Path 4: success with data — call the LLM ──────────────────────
# #     data      = state.get("data", [])
# #     row_count = state.get("row_count", 0)

# #     # Send at most 5 rows to the LLM — enough context, low token cost
# #     preview_rows = data[:5]
# #     data_preview = json.dumps(preview_rows, indent=2, default=str)

# #     if row_count > 5:
# #         data_preview += f"\n\n... and {row_count - 5} more rows not shown."

# #     chain = FORMAT_PROMPT | get_llm(temperature=0.0) | StrOutputParser()
# #     #print(FORMAT_PROMPT)
# #     try:
# #         response = chain.invoke({
# #             "question":     state["question"],
# #             "row_count":    row_count,
# #             "data_preview": data_preview,
# #         })

# #         logger.info(f"[format_node] Response: {response[:200]}")
# #         print(response)
# #         return {**state, "response": response.strip()}

# #     except Exception as e:
# #         logger.error(f"[format_node] LLM formatting failed: {e} — using fallback")

# #         # Fallback — describe the result without LLM
# #         first = data[0] if data else {}
# #         return {
# #             **state,
# #             "response": (
# #                 f"Your query returned {row_count} result(s). "
# #                 f"First result: {json.dumps(first, default=str)}"
# #             ),
# #         }


# from langchain_core.prompts import ChatPromptTemplate
# from langchain_core.output_parsers import StrOutputParser
# from src.agent.state import AgentState
# from src.agent.llm import get_llm
# import json
# import logging

# logger = logging.getLogger(__name__)


# # ── Improved Prompt ────────────────────────────────────────────────────────

# FORMAT_PROMPT = ChatPromptTemplate.from_messages([
#     (
#         "system",
#         """You are a data analyst presenting query results.

# STRICT RULES:
# - ALWAYS include entity names with their values
# - NEVER output numbers without labels
# - NEVER summarize away important columns
# - Use clear mapping: name → value
# - Max 3 sentences
# - No technical words""",
#     ),
#     (
#         "human",
#         """Question: {question}

# Data:
# {data_preview}

# Write a clear answer with names and values.""",
#     ),
# ])


# # ── Helper: Detect aggregation pattern ─────────────────────────────────────

# def _is_simple_aggregation(rows: list[dict]) -> bool:
#     """
#     Detect cases like:
#     [{'name': 'Bob', 'total_quantity': 184}, ...]
#     """
#     if not rows:
#         return False

#     keys = rows[0].keys()

#     # one categorical + one numeric
#     if len(keys) == 2:
#         return True

#     return False


# # ── Helper: Deterministic formatter (VERY IMPORTANT) ──────────────────────

# def _format_aggregation(rows: list[dict]) -> str:
#     """
#     Converts:
#     [{'name': 'Bob', 'total_quantity': 184}, ...]

#     →
#     1. Bob → 184
#     2. Lena → 112
#     """
#     if not rows:
#         return "No data found."

#     keys = list(rows[0].keys())

#     name_key = None
#     value_key = None

#     for k in keys:
#         if isinstance(rows[0][k], str):
#             name_key = k
#         else:
#             value_key = k

#     if not name_key or not value_key:
#         return None

#     lines = []
#     for i, row in enumerate(rows):
#         lines.append(f"{i+1}. {row[name_key]} → {row[value_key]}")

#     return "\n".join(lines)


# # ── Main Node ─────────────────────────────────────────────────────────────

# def format_node(state: AgentState) -> AgentState:

#     logger.info(f"[format_node] Formatting response — success={state.get('success')}")

#     # ── Errors ────────────────────────────────────────────────────────
#     if state.get("final_error"):
#         return {
#             **state,
#             "response": (
#                 f"I was unable to answer your question after multiple attempts. "
#                 f"Error: {state['final_error']}"
#             ),
#         }

#     if not state.get("success"):
#         return {
#             **state,
#             "response": (
#                 f"I ran into an error: {state.get('execution_error')}"
#             ),
#         }

#     if state.get("row_count", 0) == 0:
#         return {
#             **state,
#             "response": "No data found matching your request."
#         }

#     data = state.get("data", [])
#     row_count = state.get("row_count", 0)

#     # ── 🚀 STEP 1: Try deterministic aggregation formatting ────────────
#     if _is_simple_aggregation(data):
#         formatted = _format_aggregation(data)

#         if formatted:
#             logger.info("[format_node] Using deterministic aggregation formatter")
#             return {**state, "response": formatted}

#     # ── STEP 2: LLM fallback ───────────────────────────────────────────
#     preview_rows = data[:10]
#     data_preview = json.dumps(preview_rows, indent=2, default=str)

#     if row_count > 10:
#         data_preview += f"\n... and {row_count - 5} more rows"

#     chain = FORMAT_PROMPT | get_llm(temperature=0.0) | StrOutputParser()

#     try:
#         response = chain.invoke({
#             "question": state["question"],
#             "data_preview": data_preview,
#         })

#         logger.info(f"[format_node] LLM response: {response[:200]}")
#         return {**state, "response": response.strip()}

#     except Exception as e:
#         logger.error(f"[format_node] LLM failed: {e}")

#         # ── STEP 3: Safe fallback ─────────────────────────────────────
#         first = data[0] if data else {}
#         return {
#             **state,
#             "response": (
#                 f"{row_count} results found. First row: {json.dumps(first)}"
#             ),
#         }


# src/agent/nodes/format_node.py

# src/agent/nodes/format_node.py

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from src.agent.state import AgentState
from src.agent.llm import get_llm
import json
import logging

logger = logging.getLogger(__name__)


# ── Prompt ─────────────────────────────────────────────────────────────────

FORMAT_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are a business intelligence analyst writing a concise executive summary.

STRICT RULES:
- Use ONLY facts visible in the data below — never invent dates, numbers, or details not shown
- No speculation, no "it's worth noting", no "this could indicate", no "interesting"
- State what the data shows, nothing more
- If data has numbers — state the key metric clearly (total, highest, lowest)
- If data is a list of names/records — state who they are and how many
- Maximum 2 sentences
- Professional tone — factual, direct, confident
- No markdown, no bullet points, no technical terms""",
        ),
        (
            "human",
            """Question: {question}

{stats_section}

Data ({row_count} records):
{data_preview}

Write a 2-sentence executive summary using only what is in the data above.""",
        ),
    ]
)


# ── Column name cleaner ────────────────────────────────────────────────────

UNIT_SUFFIXES = {
    "_sec": "(seconds)",
    "_ms": "(ms)",
    "_gb": "(GB)",
    "_mb": "(MB)",
    "_pct": "(%)",
    "_usd": "($)",
    "_rate": "rate",
    "_count": "count",
}

JUNK_COLUMNS = {"_id", "id", "__v", "objectid"}


def _is_junk_value(values: list) -> bool:
    """Returns True if all non-null values look like hex ObjectIds or internal keys."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return True
    hex_count = sum(
        1
        for v in non_null
        if isinstance(v, str)
        and len(v) == 24
        and all(c in "0123456789abcdef" for c in v.lower())
    )
    return hex_count / len(non_null) > 0.8


def _clean_column_name(col: str) -> str:
    """
    Converts raw column names to readable labels.
    duration_sec → Duration (seconds)
    customer_id  → Customer
    total_amount → Total Amount
    """
    name = col

    # Strip unit suffixes and replace with readable version
    for suffix, label in UNIT_SUFFIXES.items():
        if name.endswith(suffix):
            name = name[: -len(suffix)] + " " + label
            break

    # Strip trailing _id
    if name.endswith("_id"):
        name = name[:-3]

    # snake_case → Title Case
    name = name.replace("_", " ").strip().title()

    return name


def _clean_data(rows: list[dict]) -> tuple[list[dict], dict[str, str]]:
    """
    Removes junk columns and renames all columns to human-readable labels.

    Returns:
        clean_rows  — list of dicts with renamed keys
        name_map    — {original_col: clean_label} for reference
    """
    if not rows:
        return [], {}

    # Collect all values per column
    col_values: dict[str, list] = {}
    for row in rows:
        for k, v in row.items():
            col_values.setdefault(k, []).append(v)

    # Decide which columns to keep
    keep = []
    for col, values in col_values.items():
        if col.lower() in JUNK_COLUMNS:
            continue
        if _is_junk_value(values):
            continue
        keep.append(col)

    # Build name map
    name_map = {col: _clean_column_name(col) for col in keep}

    # Rename rows
    clean_rows = [{name_map[k]: v for k, v in row.items() if k in keep} for row in rows]

    return clean_rows, name_map


# ── Stats pre-computation ──────────────────────────────────────────────────


def _compute_stats(rows: list[dict]) -> dict:
    """
    Pre-computes min, max, average, and total for every numeric column.
    Injected into the prompt so the LLM uses real numbers.
    """
    if not rows:
        return {}

    stats = {}
    col_values: dict[str, list] = {}

    for row in rows:
        for k, v in row.items():
            col_values.setdefault(k, []).append(v)

    ID_PATTERNS = {"_id", "id", "pk", "key", "code", "number"}

    for col, values in col_values.items():
        col_lower = col.lower()
        if any(col_lower.endswith(p) or col_lower == p for p in ID_PATTERNS):
            continue

        nums = [v for v in values if isinstance(v, (int, float))]
        if not nums:
            continue
        stats[col] = {
            "min": min(nums),
            "max": max(nums),
            "average": round(sum(nums) / len(nums), 2),
            "total": round(sum(nums), 2),
        }

    return stats


def _build_stats_section(stats: dict) -> str:
    """Formats pre-computed stats into a readable section for the prompt.
    Returns empty string when no numeric columns exist — keeps prompt clean."""
    if not stats:
        return ""

    lines = ["Pre-computed statistics (use these exact numbers):"]
    for col, s in stats.items():
        lines.append(
            f"  {col}: min={s['min']}, max={s['max']}, "
            f"avg={s['average']}, total={s['total']}"
        )
    return "\n".join(lines)


# ── Node ───────────────────────────────────────────────────────────────────


def format_node(state: AgentState) -> AgentState:
    """
    LangGraph node — turns raw query results into a human-style insight.

    Always uses the LLM. Pre-computes stats and cleans column names
    before sending to the LLM so the output is grounded in real numbers.
    """
    logger.info(
        f"[format_node] Processing: "
        f"success={state.get('success')}, rows={state.get('row_count', 0)}"
    )

    # ── Error paths ────────────────────────────────────────────────────
    if state.get("final_error"):
        return {
            **state,
            "response": (
                "I wasn't able to answer this after multiple attempts. "
                f"Last error: {state['final_error']}. "
                "Try rephrasing your question."
            ),
        }

    if not state.get("success"):
        return {
            **state,
            "response": (
                "Something went wrong retrieving the data. "
                f"Error: {state.get('execution_error', 'unknown')}."
            ),
        }

    if state.get("row_count", 0) == 0:
        return {
            **state,
            "response": "No records matched your question.",
        }

    # ── Clean data + compute stats ─────────────────────────────────────
    raw_data = state.get("data", [])
    row_count = state.get("row_count", 0)

    clean_rows, _ = _clean_data(raw_data)
    stats = _compute_stats(clean_rows)
    stats_section = _build_stats_section(stats)

    # Send up to 10 rows — enough for the LLM to see the full picture
    preview = clean_rows[:10]
    data_preview = json.dumps(preview, indent=2, default=str)

    if row_count > 10:
        data_preview += f"\n[{row_count - 10} more records not shown]"

    # ── LLM call ───────────────────────────────────────────────────────
    chain = FORMAT_PROMPT | get_llm(temperature=0.1) | StrOutputParser()

    try:
        response = chain.invoke(
            {
                "question": state["question"],
                "row_count": row_count,
                "stats_section": stats_section,
                "data_preview": data_preview,
            }
        )

        logger.info(f"[format_node] Response: {response[:200]}")
        return {**state, "response": response.strip()}

    except Exception as e:
        logger.error(f"[format_node] LLM failed: {e}")

        # Graceful fallback — still human-readable
        first = clean_rows[0] if clean_rows else {}
        return {
            **state,
            "response": (
                f"{row_count} records found. "
                f"First result: {json.dumps(first, default=str)}"
            ),
        }
