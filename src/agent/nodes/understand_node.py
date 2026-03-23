# src/agent/nodes/understand_node.py

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from src.agent.state import AgentState
from src.agent.llm import get_llm
from src.schema.schema_store import load, get_schema_summary_for_llm
import json
import logging

logger = logging.getLogger(__name__)


# ── Prompt ─────────────────────────────────────────────────────────────────

UNDERSTAND_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        """You are a data analyst for a BI system connected to two databases.

Available SQL tables:
{sql_tables}

Available MongoDB collections:
{mongo_collections}

Schema details:
{schema}

Your job is to deeply understand what the user is asking and classify it.

Intent — choose the one that best describes the core of the question:
- aggregation  → the answer is a computed number or ranked list (count, sum, average, top N, most, least)
- filter       → the answer is a subset of records matching a condition
- trend        → the answer shows how something changes across a continuous dimension (time, sequence)
- comparison   → the answer places two or more groups side by side to highlight differences
- lookup       → the answer is one specific record or a small known set
- dashboard    → the answer requires multiple independent metrics combined

Source — where does the data live:
- "sql"   → all required data exists in SQL tables
- "mongo" → all required data exists in MongoDB collections
- "both"  → the question genuinely cannot be answered from one database alone —
            it requires joining or combining data across SQL and MongoDB.
            This is rare. Only use it when there is no other way.

needs_chart — will a chart communicate the answer better than a sentence:
- A chart adds value when the answer has multiple data points that form a
  pattern, trend, or comparison worth seeing visually
- A chart does NOT add value when the answer is a single number, a simple
  list, or a specific record lookup

Entities — which tables or collections are the minimum needed:
- Include only what is strictly necessary to answer the question
- Do not include a table just because it exists
- SQL tables and MongoDB collections must not be mixed unless source is "both"

Return ONLY this JSON object — no explanation outside it:
{{
  "intent":           "aggregation",
  "source":           "sql",
  "entities":         ["cust", "ord"],
  "needs_chart":      false,
  "intent_reasoning": "brief explanation of your reasoning"
}}

CRITICAL: entities must contain ONLY names from the lists above.""",
    ),
    (
        "human",
        "Question: {question}"
    ),
])


# ── Validation ─────────────────────────────────────────────────────────────

def _validate_and_correct(result: dict, sql_tables: list, mongo_cols: list) -> dict:
    """
    Fixes two specific mistakes the model makes consistently:

    1. Including product_catalog (Mongo) in SQL questions about products
       when prod (SQL) already has that data.

    2. Marking source=both when all Mongo entities are just mirrors
       of SQL tables and SQL already covers the question.
    """
    entities      = result.get("entities", [])
    source        = result.get("source", "sql")

    # Collections that mirror SQL tables — rarely needed
    # when the SQL equivalent is already in entities
    mongo_mirrors = {"product_catalog"}

    sql_entities   = [e for e in entities if e in sql_tables]
    mongo_entities = [e for e in entities if e in mongo_cols]

    # source=both but all Mongo entities are mirrors → correct to sql
    if source == "both":
        genuine_mongo = [e for e in mongo_entities if e not in mongo_mirrors]
        if not genuine_mongo:
            result["source"]   = "sql"
            result["entities"] = sql_entities

    # source=sql but mirror collections snuck in → remove them
    if source == "sql":
        result["entities"] = [e for e in entities if e not in mongo_cols]

    return result


# ── Node function ──────────────────────────────────────────────────────────

def understand_node(state: AgentState) -> AgentState:
    """
    LangGraph node — first node every question hits.

    Reads:
        state.question

    Writes:
        state.intent
        state.source
        state.entities
        state.needs_chart
        state.intent_reasoning
    """
    logger.info(f"[understand_node] Question: '{state['question']}'")

    graph       = load()
    schema_text = get_schema_summary_for_llm(graph)
    sql_tables  = list(graph.sql.keys())
    mongo_cols  = list(graph.mongo.keys())

    chain = UNDERSTAND_PROMPT | get_llm(temperature=0.0) | JsonOutputParser()

    try:
        result = chain.invoke({
            "sql_tables":        json.dumps(sql_tables),
            "mongo_collections": json.dumps(mongo_cols),
            "schema":            schema_text,
            "question":          state["question"],
        })

        # Fix known model mistakes before writing to state
        result = _validate_and_correct(result, sql_tables, mongo_cols)

        # Drop any entity name the LLM invented
        valid_names = set(sql_tables + mongo_cols)
        entities    = [e for e in result.get("entities", []) if e in valid_names]

        if not entities:
            entities = sql_tables[:1]
            logger.warning(f"[understand_node] No valid entities — falling back to: {entities}")

        intent      = result.get("intent", "filter")
        source      = result.get("source", "sql")
        needs_chart = result.get("needs_chart", False)
        reasoning   = result.get("intent_reasoning", "")

        logger.info(
            f"[understand_node] intent={intent} source={source} "
            f"entities={entities} needs_chart={needs_chart}"
        )
        print(({**state,
            "intent":           intent,
            "source":           source,
            "entities":         entities,
            "needs_chart":      needs_chart,
            "intent_reasoning": reasoning,
         }))
        return {
            **state,
            "intent":           intent,
            "source":           source,
            "entities":         entities,
            "needs_chart":      needs_chart,
            "intent_reasoning": reasoning,
        }

    except Exception as e:
        logger.error(f"[understand_node] Failed: {e} — using safe fallback")
        return {
            **state,
            "intent":           "filter",
            "source":           "sql",
            "entities":         sql_tables[:1],
            "needs_chart":      False,
            "intent_reasoning": f"Fallback due to error: {e}",
        }