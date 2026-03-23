# src/agent/nodes/format_node.py

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from src.agent.state import AgentState
from src.agent.llm import get_llm
import json
import logging

logger = logging.getLogger(__name__)


# ── Prompt ─────────────────────────────────────────────────────────────────

# FORMAT_PROMPT = ChatPromptTemplate.from_messages([
#     (
#         "system",
#         """You are a helpful data analyst presenting query results to a business user.

# Your job is to write a clear, direct answer to the user's question based on the data returned.

# Rules:
# - Answer in 1 to 3 sentences maximum
# - Always mention specific numbers from the data
# - Be direct — start with the answer, not with "Based on the data..."
# - Do not repeat the question back to the user
# - Do not mention SQL, MongoDB, queries, or technical terms
# - Do not use markdown formatting
# - If the data shows something unexpected or notable, mention it""",
#     ),
#     (
#         "human",
#         """Question: {question}

# Data returned ({row_count} rows):
# {data_preview}

# Write a direct answer.""",
#     ),
# ])

FORMAT_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        """You are a data analyst presenting query results to a business user.

CRITICAL RULES:
- You MUST use ONLY the numbers from the data provided below — never invent or assume numbers
- Answer in 1 to 3 sentences maximum
- Be direct — start with the answer
- Do not mention SQL, MongoDB, queries, or technical terms
- Do not use markdown formatting""",
    ),
    (
        "human",
        """Question: {question}

Data returned ({row_count} rows):
{data_preview}

Write a direct answer using ONLY the numbers above.""",
    ),
])




# ── Node function ──────────────────────────────────────────────────────────

def format_node(state: AgentState) -> AgentState:
    """
    LangGraph node — turns raw query results into a plain English answer.

    Reads:
        state.success
        state.data
        state.row_count
        state.question
        state.query
        state.execution_error
        state.final_error

    Writes:
        state.response
    """
    logger.info(f"[format_node] Formatting response — success={state.get('success')}")

    # ── Path 1: all retries exhausted — report failure ─────────────────
    if state.get("final_error"):
        return {
            **state,
            "response": (
                f"I was unable to answer your question after multiple attempts. "
                f"The last error was: {state['final_error']}. "
                "Please try rephrasing your question or check that the data exists."
            ),
        }

    # ── Path 2: execution failed on final retry ────────────────────────
    if not state.get("success"):
        error = state.get("execution_error", "unknown error")
        return {
            **state,
            "response": (
                f"I ran into an error while fetching your data: {error}. "
                "Please try rephrasing your question."
            ),
        }

    # ── Path 3: query succeeded but returned no rows ───────────────────
    if state.get("row_count", 0) == 0:
        return {
            **state,
            "response": (
                "The query ran successfully but returned no results. "
                "There may be no data matching your question, or the "
                "filters you described do not match any records."
            ),
        }

    # ── Path 4: success with data — call the LLM ──────────────────────
    data      = state.get("data", [])
    row_count = state.get("row_count", 0)

    # Send at most 5 rows to the LLM — enough context, low token cost
    preview_rows = data[:5]
    data_preview = json.dumps(preview_rows, indent=2, default=str)

    if row_count > 5:
        data_preview += f"\n\n... and {row_count - 5} more rows not shown."

    chain = FORMAT_PROMPT | get_llm(temperature=0.0) | StrOutputParser()
    #print(FORMAT_PROMPT)
    try:
        response = chain.invoke({
            "question":     state["question"],
            "row_count":    row_count,
            "data_preview": data_preview,
        })

        logger.info(f"[format_node] Response: {response[:200]}")
        print(response)
        return {**state, "response": response.strip()}

    except Exception as e:
        logger.error(f"[format_node] LLM formatting failed: {e} — using fallback")

        # Fallback — describe the result without LLM
        first = data[0] if data else {}
        return {
            **state,
            "response": (
                f"Your query returned {row_count} result(s). "
                f"First result: {json.dumps(first, default=str)}"
            ),
        }