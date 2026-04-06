# src/agent/state.py

from typing import Optional, Any
from typing_extensions import TypedDict


class AgentState(TypedDict):
    """
    The single shared object that flows through every node in the graph.

    No node passes data directly to another node.
    Every node reads from this state and writes back to it.

    Fields are grouped by which node fills them in.
    """

    # ── Input ──────────────────────────────────────────────────────────────
    # Set once at the start. Never modified after that.

    question: str
    # The raw user question exactly as typed.
    # Example: "how many orders did customer Alice place last month?"

    # ── Understand node output ─────────────────────────────────────────────
    # Filled by the understand node after reading the question + schema graph.

    intent: str
    # What kind of question is being asked.
    # One of: "aggregation" | "filter" | "trend" | "comparison" | "lookup" | "dashboard"
    #
    # aggregation  — count, sum, average, min, max
    # filter       — show rows matching a condition
    # trend        — change over time
    # comparison   — compare two things
    # lookup       — find a specific record
    # dashboard    — multiple metrics at once

    source: str
    # Which database has the data needed to answer.
    # One of: "sql" | "mongo" | "both"
    #
    # "both" means the question needs data from SQL and Mongo combined.
    # When source="both" the SQL planner runs first, Mongo planner second.

    entities: list[str]
    # The specific table or collection names that are relevant to this question.
    # Taken directly from schema_graph.json — only names that actually exist.
    # Example: ["cust", "ord"]  or  ["user_events"]

    intent_reasoning: str
    # The LLM's explanation of why it chose this intent and source.
    # Not shown to the user — used for debugging and logging only.

    # ── Planner node output ────────────────────────────────────────────────
    # Filled by either the SQL planner or the Mongo planner node.
    # On retry, the planner overwrites these fields with a corrected version.

    query_type: str
    # Which kind of query was generated.
    # One of: "sql" | "mongo"

    query: Any
    # The actual query to run.
    # If query_type="sql"   → a string containing a SELECT statement
    # If query_type="mongo" → a list of dicts (aggregation pipeline stages)
    # Example SQL:   "SELECT COUNT(*) as total FROM cust"
    # Example Mongo: [{"$match": {"event": "checkout"}}, {"$count": "total"}]

    collection: Optional[str]
    # Only used when query_type="mongo".
    # The exact MongoDB collection name to run the pipeline against.
    # None when query_type="sql".

    query_reasoning: str
    # The planner's explanation of why it wrote this query.
    # Used for debugging. Not shown to the user.

    # ── Executor node output ───────────────────────────────────────────────
    # Filled by the executor node after running the query.

    success: bool
    # True if the query ran without error and returned results.
    # False if the database returned an error or the query was blocked.

    data: list[dict]
    # The rows (SQL) or documents (Mongo) returned by the query.
    # Each item is a plain dict — no ObjectId, no special types.
    # Empty list if success=False.

    row_count: int
    # How many rows or documents are in data.
    # 0 if success=False.

    execution_error: Optional[str]
    # The exact error message from the database if success=False.
    # None if success=True.
    # Example: "no such column: customer_name"
    # This is what gets fed back to the planner on retry.

    # ── Retry control ──────────────────────────────────────────────────────
    # Managed by the router between executor and planner.

    retry_count: int
    # How many times we have retried after a failed execution.
    # Starts at 0. Incremented by 1 each time the executor fails.

    max_retries: int
    # The hard limit on retries. Set to 2 at the start and never changed.
    # When retry_count reaches max_retries, the graph goes to format
    # instead of back to the planner.

    error_feedback: Optional[str]
    # The execution_error copied here so the planner can read it on retry.
    # None on the first attempt.
    # Example: "no such table: customers — did you mean: cust?"
    # The planner prompt includes this so the LLM knows what went wrong.

    # ── Format node output ─────────────────────────────────────────────────
    # Filled by the format node. This is what gets returned to the user.

    response: Optional[str]
    # The plain English answer to the user's question.
    # Written by the LLM in the format node using the data as context.
    # Example: "Alice placed 3 orders last month totalling $2,400."

    final_error: Optional[str]
    # Set only if all retries were exhausted and the agent gave up.
    # None if the agent succeeded.
    # Example: "Could not generate a valid query after 2 attempts."

    needs_chart: bool  # ← set by understand node
    chart_spec: Optional[dict]  # ← set by charter node (None if no chart)
    requested_chart_type: str  # could come from query parsing
