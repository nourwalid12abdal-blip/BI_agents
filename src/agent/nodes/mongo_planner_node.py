# # src/agent/nodes/mongo_planner_node.py

# from langchain_core.prompts import ChatPromptTemplate
# from langchain_core.output_parsers import StrOutputParser
# from src.agent.state import AgentState
# from src.agent.llm import get_llm
# from src.schema.schema_store import load
# import json
# import re
# import logging

# logger = logging.getLogger(__name__)


# # ── Prompt ─────────────────────────────────────────────────────────────────

# MONGO_PROMPT = ChatPromptTemplate.from_messages([
#     (
#         "system",
#         """You are an expert MongoDB aggregation pipeline writer for a BI system.

# Collection schema — use EXACT field names as written below:
# {schema}

# Rules:
# - Return ONLY a valid JSON array representing the aggregation pipeline
# - Use exact field names from the schema — do not invent field names
# - Add a $limit stage of 1000 unless the pipeline uses $group or $count
# - Never use $out or $merge — these write to the database
# - Keep the pipeline as simple as possible — avoid unnecessary stages
# - Return ONLY the raw JSON array — no explanation, no markdown, no code fences

# {error_section}""",
#     ),
#     (
#         "human",
#         "{question}"
#     ),
# ])


# # ── Node function ──────────────────────────────────────────────────────────

# def mongo_planner_node(state: AgentState) -> AgentState:
#     """
#     LangGraph node — generates a MongoDB aggregation pipeline.

#     Reads:
#         state.question
#         state.entities       — which collections to focus on
#         state.error_feedback — error from previous attempt (if retry)

#     Writes:
#         state.query_type     — always "mongo"
#         state.query          — the aggregation pipeline as a list of dicts
#         state.collection     — the target collection name
#         state.query_reasoning
#     """
#     logger.info(
#         f"[mongo_planner_node] Planning Mongo pipeline "
#         f"(retry={state.get('retry_count', 0)}) "
#         f"entities={state.get('entities')}"
#     )
#     print("hi im mongo")


#     graph      = load()
#     entities   = state.get("entities", [])

#     # Pick the most relevant collection from entities
#     collection = next((e for e in entities if e in graph.mongo), None)

#     # Fall back to first Mongo collection if nothing matched
#     if not collection and graph.mongo:
#         collection = list(graph.mongo.keys())[0]
#         logger.warning(
#             f"[mongo_planner_node] No entity matched a Mongo collection — "
#             f"defaulting to '{collection}'"
#         )

#     schema_text   = _build_schema_text(graph.mongo, collection)
#     error_section = _build_error_section(state.get("error_feedback"))

#     chain = MONGO_PROMPT | get_llm(temperature=0.0) | StrOutputParser()

#     try:
#         raw      = chain.invoke({
#             "schema":        schema_text,
#             "question":      state["question"],
#             "error_section": error_section,
#         })

#         pipeline = _extract_pipeline(raw)
#         pipeline = _validate_pipeline(pipeline)

#         logger.info(
#             f"[mongo_planner_node] Pipeline for '{collection}':\n"
#             f"{json.dumps(pipeline, indent=2)}"
#         )
#         print(({
#             **state,
#             "query_type":      "mongo",
#             "query":           pipeline,
#             "collection":      collection,
#             "query_reasoning": f"Mongo pipeline on collection: {collection}",
#         }))
#         return {
#             **state,
#             "query_type":      "mongo",
#             "query":           pipeline,
#             "collection":      collection,
#             "query_reasoning": f"Mongo pipeline on collection: {collection}",
#         }

#     except Exception as e:
#         logger.error(f"[mongo_planner_node] Failed: {e}")

#         # Safe fallback — return all documents with a limit
#         fallback = [{"$limit": 100}]

#         return {
#             **state,
#             "query_type":      "mongo",
#             "query":           fallback,
#             "collection":      collection,
#             "query_reasoning": f"Fallback pipeline due to error: {e}",
#         }


# # ── Schema builder ─────────────────────────────────────────────────────────

# def _build_schema_text(mongo_schema: dict, collection: str) -> str:
#     """
#     Builds a readable schema description for one collection.
#     Includes fields, types, embedded documents, and array fields
#     so the model knows the exact shape of the data.
#     """
#     schema = mongo_schema.get(collection)

#     if not schema:
#         return f"Collection: {collection}\nNo schema available."

#     lines = [f"Collection: {collection}", "", "Fields:"]

#     for field, ftype in schema.fields.items():
#         lines.append(f"  {field}: {ftype}")

#     if schema.embedded_docs:
#         lines.append("")
#         lines.append("Embedded documents (access with dot notation):")
#         for doc in schema.embedded_docs:
#             lines.append(f"  {doc}")

#     if schema.array_fields:
#         lines.append("")
#         lines.append("Array fields (use $unwind to expand):")
#         for arr in schema.array_fields:
#             lines.append(f"  {arr}")

#     lines.append("")
#     lines.append(f"Total documents: ~{schema.doc_count}")

#     return "\n".join(lines)


# # ── Error section builder ──────────────────────────────────────────────────

# def _build_error_section(error_feedback: str | None) -> str:
#     """
#     Returns an error section to inject into the prompt on retry.
#     Empty string on the first attempt so the prompt stays clean.
#     """
#     if not error_feedback:
#         return ""
#     return (
#         f"\nPREVIOUS ATTEMPT FAILED with this error:\n"
#         f"  {error_feedback}\n"
#         f"Fix the pipeline to avoid this error. "
#         f"Double-check field names, stage syntax, and operator usage."
#     )


# # ── Pipeline extractor ─────────────────────────────────────────────────────

# def _extract_pipeline(raw: str) -> list:
#     """
#     Robustly extracts a JSON array from whatever the model returned.

#     Handles all known patterns:
#       [...]                   — clean ideal output
#       ```json\\n[...]\\n```   — markdown fenced
#       {{"pipeline": [...]}}   — wrapped in a named key
#       some text [...] more    — array buried in text
#     """
#     text = raw.strip()

#     # ── 1. Strip markdown fences ───────────────────────────────────────
#     if "```" in text:
#         parts = text.split("```")
#         for part in parts:
#             part = part.strip()
#             if part.lower().startswith("json"):
#                 part = part[4:].strip()
#             if part.startswith("[") or part.startswith("{"):
#                 text = part
#                 break
#     text = text.strip()

#     # ── 2. Clean array — ideal case ────────────────────────────────────
#     if text.startswith("["):
#         try:
#             result = json.loads(text)
#             if isinstance(result, list):
#                 return result
#         except json.JSONDecodeError:
#             pass

#     # ── 3. Wrapped in a dict — {"pipeline": [...]} ─────────────────────
#     if text.startswith("{"):
#         try:
#             parsed = json.loads(text)
#             if isinstance(parsed, dict):
#                 for key in ("pipeline", "aggregation", "stages", "result"):
#                     if key in parsed and isinstance(parsed[key], list):
#                         return parsed[key]
#                 # Grab first list value regardless of key name
#                 for v in parsed.values():
#                     if isinstance(v, list):
#                         return v
#         except json.JSONDecodeError:
#             pass

#     # ── 4. Regex — find any [...] block in the string ──────────────────
#     match = re.search(r'\[.*\]', text, re.DOTALL)
#     if match:
#         try:
#             result = json.loads(match.group())
#             if isinstance(result, list):
#                 logger.warning("[mongo_planner_node] Used regex fallback to extract pipeline")
#                 return result
#         except json.JSONDecodeError:
#             pass

#     # ── 5. Nothing worked — return safe fallback ───────────────────────
#     logger.error(f"[mongo_planner_node] Could not extract pipeline from:\n{raw[:300]}")
#     return [{"$limit": 100}]


# # ── Pipeline validator ─────────────────────────────────────────────────────

# FORBIDDEN_STAGES = {"$out", "$merge"}

# def _validate_pipeline(pipeline: list) -> list:
#     """
#     Two safety checks:
#     1. Block any write stages ($out, $merge)
#     2. Add $limit if no $group or $count stage exists
#     """
#     # Block write stages
#     for stage in pipeline:
#         for key in stage.keys():
#             if key in FORBIDDEN_STAGES:
#                 raise ValueError(
#                     f"Blocked: '{key}' is a write stage and is not permitted."
#                 )

#     # Add $limit if the pipeline does not aggregate
#     has_group = any("$group" in stage or "$count" in stage for stage in pipeline)
#     has_limit = any("$limit" in stage for stage in pipeline)

#     if not has_group and not has_limit:
#         pipeline.append({"$limit": 1000})

#     return pipeline



# src/agent/nodes/mongo_planner_node.py

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from src.agent.state import AgentState
from src.agent.llm import get_llm
from src.schema.schema_store import load
import json
import re
import logging

logger = logging.getLogger(__name__)


# ── Intent rules injected into the prompt ─────────────────────────────────

INTENT_RULES = {
    "filter": """
INTENT: filter
- Use $match to return individual documents matching the condition
- NEVER use $group — the user wants raw documents, not aggregated totals
- Add $limit 1000 after $match
- Example: [{"$match": {"duration_sec": {"$lt": 300}}}, {"$limit": 1000}]
""",
    "aggregation": """
INTENT: aggregation
- Use $group with $sum, $count, $avg, $min, or $max
- Always give meaningful names to computed fields (e.g. "total_orders", not "count")
- Example: [{"$group": {"_id": "$event", "count": {"$sum": 1}}}]
""",
    "trend": """
INTENT: trend
- Group by a date or time field to show change over time
- Always $sort by the date field ascending
- Example: [{"$group": {"_id": "$created_at", "total": {"$sum": "$amount"}}}, {"$sort": {"_id": 1}}]
""",
    "comparison": """
INTENT: comparison
- Group by the categorical field you are comparing
- Compute a metric per group (count, sum, avg)
- $sort descending so the largest value comes first
- Example: [{"$group": {"_id": "$event", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
""",
    "lookup": """
INTENT: lookup
- Use $match to find the specific document(s)
- Add $limit 10 — lookups return a small known set
- NEVER use $group
- Example: [{"$match": {"customer_id": 1}}, {"$limit": 10}]
""",
    "dashboard": """
INTENT: dashboard
- Return a summary per category or time period
- Use $group + $sort
- Keep it simple — one metric per group
""",
}

DEFAULT_INTENT_RULE = """
INTENT: unknown
- Use $match if the question filters by a condition
- Use $group if the question asks for a count, sum, or average
- Add $limit 1000 if no $group stage exists
"""


# ── Prompt ─────────────────────────────────────────────────────────────────

MONGO_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        """You are an expert MongoDB aggregation pipeline writer for a BI system.

Collection schema — use EXACT field names as written below:
{schema}

{intent_rules}

General rules:
- Return ONLY a valid JSON array representing the aggregation pipeline
- Use EXACT field names from the schema — do not invent field names
- Never use $out or $merge — these write to the database
- Keep the pipeline as simple as possible — avoid unnecessary stages
- Return ONLY the raw JSON array — no explanation, no markdown, no code fences

{error_section}""",
    ),
    (
        "human",
        "{question}"
    ),
])


# ── Node function ──────────────────────────────────────────────────────────

def mongo_planner_node(state: AgentState) -> AgentState:
    """
    LangGraph node — generates a MongoDB aggregation pipeline.

    Reads:
        state.question
        state.intent         — filter | aggregation | trend | comparison | lookup | dashboard
        state.entities       — which collections to focus on
        state.error_feedback — error from previous attempt (if retry)

    Writes:
        state.query_type     — always "mongo"
        state.query          — the aggregation pipeline as a list of dicts
        state.collection     — the target collection name
        state.query_reasoning
    """
    intent = state.get("intent", "unknown")

    logger.info(
        f"[mongo_planner_node] Planning Mongo pipeline "
        f"(retry={state.get('retry_count', 0)}) "
        f"intent={intent} "
        f"entities={state.get('entities')}"
    )

    graph    = load()
    entities = state.get("entities", [])

    # Pick the most relevant collection from entities
    collection = next((e for e in entities if e in graph.mongo), None)

    # Fall back to first Mongo collection if nothing matched
    if not collection and graph.mongo:
        collection = list(graph.mongo.keys())[0]
        logger.warning(
            f"[mongo_planner_node] No entity matched a Mongo collection — "
            f"defaulting to '{collection}'"
        )

    schema_text   = _build_schema_text(graph.mongo, collection)
    intent_rules  = INTENT_RULES.get(intent, DEFAULT_INTENT_RULE)
    error_section = _build_error_section(state.get("error_feedback"))

    chain = MONGO_PROMPT | get_llm(temperature=0.0) | StrOutputParser()

    try:
        raw = chain.invoke({
            "schema":        schema_text,
            "intent_rules":  intent_rules,
            "question":      state["question"],
            "error_section": error_section,
        })

        pipeline = _extract_pipeline(raw)
        pipeline = _validate_pipeline(pipeline, intent)

        logger.info(
            f"[mongo_planner_node] Pipeline for '{collection}':\n"
            f"{json.dumps(pipeline, indent=2)}"
        )

        return {
            **state,
            "query_type":      "mongo",
            "query":           pipeline,
            "collection":      collection,
            "query_reasoning": f"Mongo pipeline on collection: {collection} | intent: {intent}",
        }

    except Exception as e:
        logger.error(f"[mongo_planner_node] Failed: {e}")

        fallback = _safe_fallback(intent)

        return {
            **state,
            "query_type":      "mongo",
            "query":           fallback,
            "collection":      collection,
            "query_reasoning": f"Fallback pipeline due to error: {e}",
        }


# ── Schema builder ─────────────────────────────────────────────────────────

def _build_schema_text(mongo_schema: dict, collection: str) -> str:
    schema = mongo_schema.get(collection)

    if not schema:
        return f"Collection: {collection}\nNo schema available."

    lines = [f"Collection: {collection}", "", "Fields:"]

    for field, ftype in schema.fields.items():
        lines.append(f"  {field}: {ftype}")

    if schema.embedded_docs:
        lines.append("")
        lines.append("Embedded documents (access with dot notation):")
        for doc in schema.embedded_docs:
            lines.append(f"  {doc}")

    if schema.array_fields:
        lines.append("")
        lines.append("Array fields (use $unwind to expand):")
        for arr in schema.array_fields:
            lines.append(f"  {arr}")

    lines.append("")
    lines.append(f"Total documents: ~{schema.doc_count}")

    return "\n".join(lines)


# ── Error section builder ──────────────────────────────────────────────────

def _build_error_section(error_feedback: str | None) -> str:
    if not error_feedback:
        return ""
    return (
        f"\nPREVIOUS ATTEMPT FAILED with this error:\n"
        f"  {error_feedback}\n"
        f"Fix the pipeline to avoid this error. "
        f"Double-check field names, stage syntax, and operator usage."
    )


# ── Pipeline extractor ─────────────────────────────────────────────────────

def _extract_pipeline(raw: str) -> list:
    text = raw.strip()

    # Strip markdown fences
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            part = part.strip()
            if part.lower().startswith("json"):
                part = part[4:].strip()
            if part.startswith("[") or part.startswith("{"):
                text = part
                break
    text = text.strip()

    # Clean array
    if text.startswith("["):
        try:
            result = json.loads(text)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    # Wrapped in dict
    if text.startswith("{"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                for key in ("pipeline", "aggregation", "stages", "result"):
                    if key in parsed and isinstance(parsed[key], list):
                        return parsed[key]
                for v in parsed.values():
                    if isinstance(v, list):
                        return v
        except json.JSONDecodeError:
            pass

    # Regex fallback
    match = re.search(r'\[.*\]', text, re.DOTALL)
    if match:
        try:
            result = json.loads(match.group())
            if isinstance(result, list):
                logger.warning("[mongo_planner_node] Used regex fallback to extract pipeline")
                return result
        except json.JSONDecodeError:
            pass

    logger.error(f"[mongo_planner_node] Could not extract pipeline from:\n{raw[:300]}")
    return [{"$limit": 100}]


# ── Pipeline validator ─────────────────────────────────────────────────────

FORBIDDEN_STAGES = {"$out", "$merge"}


def _validate_pipeline(pipeline: list, intent: str) -> list:
    """
    Three checks:
    1. Block write stages ($out, $merge)
    2. For filter/lookup intent — warn and strip $group if present
    3. Add $limit if no $group or $count stage exists
    """
    # Block write stages
    for stage in pipeline:
        for key in stage.keys():
            if key in FORBIDDEN_STAGES:
                raise ValueError(
                    f"Blocked: '{key}' is a write stage and is not permitted."
                )

    # For filter/lookup — strip $group stages (they contradict the intent)
    if intent in ("filter", "lookup"):
        has_group = any("$group" in stage for stage in pipeline)
        if has_group:
            logger.warning(
                f"[mongo_planner_node] intent={intent} but pipeline has $group — stripping it"
            )
            pipeline = [stage for stage in pipeline if "$group" not in stage]

    # Add $limit if no aggregation stage exists
    has_group = any("$group" in stage or "$count" in stage for stage in pipeline)
    has_limit = any("$limit" in stage for stage in pipeline)

    if not has_group and not has_limit:
        pipeline.append({"$limit": 1000})

    return pipeline


# ── Safe fallback per intent ───────────────────────────────────────────────

def _safe_fallback(intent: str) -> list:
    """Returns a safe fallback pipeline that matches the intent."""
    if intent in ("filter", "lookup"):
        return [{"$limit": 100}]
    if intent == "aggregation":
        return [{"$group": {"_id": None, "count": {"$sum": 1}}}]
    return [{"$limit": 100}]