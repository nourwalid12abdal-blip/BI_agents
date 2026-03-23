# # src/schema/relation_detector.py
# from src.schema.models import (
#     SQLTableSchema, MongoCollectionSchema, CrossSourceRelation
# )
# import logging

# logger = logging.getLogger(__name__)


# def detect_cross_source_relations(
#     sql_schema: dict[str, SQLTableSchema],
#     mongo_schema: dict[str, MongoCollectionSchema],
# ) -> list[CrossSourceRelation]:
#     """
#     Compares SQL table names/columns against Mongo reference hints
#     to find likely cross-database joins.

#     Strategy:
#     1. For each Mongo reference hint (e.g. customer_id → customers),
#        check if a SQL table named 'customers' exists.
#     2. If it does, check if that SQL table has a column that matches
#        (e.g. 'id' or 'customer_id').
#     3. Record the link with a confidence score.
#     """
#     relations = []
#     sql_table_names = {name.lower(): name for name in sql_schema.keys()}

#     for col_name, col_schema in mongo_schema.items():
#         for hint in col_schema.reference_hints:
#             # Does the hinted SQL table actually exist?
#             likely_ref_lower = hint.likely_ref.lower()
#             matched_sql_table = sql_table_names.get(likely_ref_lower)

#             if not matched_sql_table:
#                 # Try singular form too (customer → customers didn't match,
#                 # but maybe the table is called 'customer')
#                 singular = likely_ref_lower.rstrip("s")
#                 matched_sql_table = sql_table_names.get(singular)

#             if matched_sql_table:
#                 sql_table = sql_schema[matched_sql_table]

#                 # Find the best matching SQL column
#                 sql_col = _find_matching_column(hint.field, sql_table)

#                 relations.append(CrossSourceRelation(
#                     mongo_collection=col_name,
#                     mongo_field=hint.field,
#                     sql_table=matched_sql_table,
#                     sql_column=sql_col,
#                     confidence=hint.confidence,
#                 ))
#                 logger.info(
#                     f"Cross-source relation detected: "
#                     f"mongo.{col_name}.{hint.field} → sql.{matched_sql_table}.{sql_col} "
#                     f"[{hint.confidence}]"
#                 )

#     logger.info(f"Detected {len(relations)} cross-source relations")
#     return relations


# def _find_matching_column(mongo_field: str, sql_table: SQLTableSchema) -> str:
#     """
#     Given a Mongo field like 'customer_id', find the best matching
#     column in the SQL table. Tries 'id' first, then exact match,
#     then falls back to the first primary key.
#     """
#     col_names = [c.name.lower() for c in sql_table.columns]

#     # Exact match first
#     if mongo_field.lower() in col_names:
#         return mongo_field

#     # 'id' is the most common PK name
#     if "id" in col_names:
#         return "id"

#     # Fall back to first primary key
#     if sql_table.primary_keys:
#         return sql_table.primary_keys[0]

#     # Last resort — first column
#     return sql_table.columns[0].name if sql_table.columns else "id"






















# src/schema/relation_detector.py  (improved)
# src/schema/relation_detector.py
# src/schema/relation_detector.py

from src.schema.models import (
    SQLTableSchema, MongoCollectionSchema, CrossSourceRelation
)
from config.settings import settings
from openai import OpenAI
from dotenv import load_dotenv
import json
import re
import logging

load_dotenv()
logger = logging.getLogger(__name__)


# ── Entry point ────────────────────────────────────────────────────────────

def detect_cross_source_relations(
    sql_schema: dict[str, SQLTableSchema],
    mongo_schema: dict[str, MongoCollectionSchema],
) -> list[CrossSourceRelation]:
    """
    Uses an LLM (via HuggingFace router) to detect cross-source relations
    between SQL tables and MongoDB collections.

    Steps:
      1. Build a compact schema summary for the LLM
      2. Ask the LLM to find all likely cross-source relations
      3. Extract the JSON array from the raw response
      4. Validate every suggestion against the real schema — drop hallucinations
    """

    schema_context = _build_schema_context(sql_schema, mongo_schema)
    logger.info("Schema context built — sending to LLM for relation detection")

    raw_list  = _ask_llm(schema_context, sql_schema, mongo_schema)
    relations = _validate_relations(raw_list, sql_schema, mongo_schema)

    logger.info(f"Relation detection complete — {len(relations)} confirmed")
    for r in relations:
        logger.info(
            f"  mongo.{r.mongo_collection}.{r.mongo_field} "
            f"→ sql.{r.sql_table}.{r.sql_column} [{r.confidence}]"
        )

    return relations


# ── Step 1: Build schema context ──────────────────────────────────────────

def _build_schema_context(
    sql_schema: dict[str, SQLTableSchema],
    mongo_schema: dict[str, MongoCollectionSchema],
) -> str:
    """
    Builds a compact, readable summary of both schemas for the LLM.
    Includes exact table/collection names so the model cannot invent new ones.
    """
    lines = []

    # SQL section
    lines.append("=== SQL TABLES ===")
    for table, schema in sql_schema.items():
        cols = []
        for c in schema.columns:
            tag = " [PK]" if c.name in schema.primary_keys else ""
            cols.append(f"{c.name} ({c.type}){tag}")

        fks = [
            f"{fk.column} → {fk.ref_table}.{fk.ref_column}"
            for fk in schema.foreign_keys
        ]

        lines.append(f"\nTable: {table}")
        lines.append(f"  Columns: {', '.join(cols)}")
        if fks:
            lines.append(f"  Foreign keys: {', '.join(fks)}")

    # MongoDB section
    lines.append("\n=== MONGODB COLLECTIONS ===")
    for col, schema in mongo_schema.items():
        fields = [f"{k} ({v})" for k, v in schema.fields.items()]

        lines.append(f"\nCollection: {col}")
        lines.append(f"  Fields: {', '.join(fields)}")
        if schema.embedded_docs:
            lines.append(f"  Embedded documents: {', '.join(schema.embedded_docs)}")
        if schema.array_fields:
            lines.append(f"  Array fields: {', '.join(schema.array_fields)}")

    return "\n".join(lines)


# ── Step 2: Ask the LLM ───────────────────────────────────────────────────

def _build_system_prompt(
    sql_schema: dict[str, SQLTableSchema],
    mongo_schema: dict[str, MongoCollectionSchema],
) -> str:
    """
    Injects the exact table and collection names directly into the system
    prompt so the model is forced to use them — no abbreviations, no guesses.
    """
    exact_sql_tables = list(sql_schema.keys())
    exact_mongo_cols = list(mongo_schema.keys())

    return f"""You are a database architect finding relationships between a SQL database and a MongoDB database.

CRITICAL: You MUST use ONLY these exact names — do not abbreviate, shorten, or invent names.

Allowed SQL table names (copy exactly):
{json.dumps(exact_sql_tables)}

Allowed MongoDB collection names (copy exactly):
{json.dumps(exact_mongo_cols)}

Your task: find every MongoDB field that references or relates to a SQL table.

Look for:
- ID references (customer_id, product_id, order_id, user_id)
- Semantic matches (author_name ↔ users.name, created_by ↔ employees.id)
- Shared business entities that appear in both databases
- Naming mismatches (usr_id ↔ users, ord_ref ↔ orders)
- Embedded document fields that mirror a SQL table structure

Return ONLY a raw JSON array — no explanation, no markdown, no code fences.
Your response must start with [ and end with ]

Example format:
[
  {{
    "mongo_collection": "user_events",
    "mongo_field": "customer_id",
    "sql_table": "cust",
    "sql_column": "id",
    "confidence": "high",
    "reasoning": "customer_id in MongoDB directly references the id column of the cust table"
  }}
]

Confidence levels:
- "high"   — near certain (explicit _id pattern, direct FK equivalent)
- "medium" — likely (semantic name match, shared business entity)
- "low"    — possible (indirect or weak signal)

If no relations found, return: []"""


def _ask_llm(
    schema_context: str,
    sql_schema: dict[str, SQLTableSchema],
    mongo_schema: dict[str, MongoCollectionSchema],
) -> list[dict]:
    """
    Calls the HuggingFace-hosted model via the OpenAI-compatible router.
    Returns a list of raw relation dicts (not yet validated).
    """
    try:
        client = OpenAI(
            base_url="https://router.huggingface.co/v1",
            api_key=settings.HF_TOKEN,
        )

        system_prompt = _build_system_prompt(sql_schema, mongo_schema)

        response = client.chat.completions.create(
            model=settings.HF_MODEL,
            temperature=0,
            # NOTE: response_format is intentionally NOT set to json_object
            # because json_object mode forces a dict wrapper and breaks plain arrays
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": (
                        f"Find all cross-source relations in these schemas:\n\n"
                        f"{schema_context}\n\n"
                        "Remember: return ONLY a JSON array starting with [ and ending with ]"
                    ),
                },
            ],
        )

        raw = response.choices[0].message.content.strip()
        logger.info(f"LLM raw response (first 400 chars):\n{raw[:400]}")

        return _extract_json_array(raw)

    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return []


# ── Step 3: Extract JSON array from raw response ──────────────────────────

def _extract_json_array(raw: str) -> list[dict]:
    """
    Robustly extracts a JSON array from whatever the model returned.

    Handles all known model response patterns:
      [...]                   — clean ideal output
      ```json\\n[...]\\n```   — markdown fenced
      {"relations": [...]}    — wrapped in a named object key
      {"[{...}]": []}         — array accidentally used as a dict key
      some text [...] more    — array buried in surrounding text
    """

    # ── 1. Strip markdown fences ──────────────────────────────────────────
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("[") or part.startswith("{"):
                raw = part
                break
    raw = raw.strip()

    # ── 2. Clean array — ideal case ───────────────────────────────────────
    if raw.startswith("["):
        try:
            result = json.loads(raw)
            if isinstance(result, list):
                logger.debug("JSON array parsed cleanly")
                return result
        except json.JSONDecodeError:
            pass

    # ── 3. Wrapped in a dict ──────────────────────────────────────────────
    if raw.startswith("{"):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):

                # Common wrap: {"relations": [...]} or {"results": [...]}
                for key in ("relations", "relationships", "results", "data", "output"):
                    if key in parsed and isinstance(parsed[key], list):
                        logger.debug(f"Unwrapped array from dict key: '{key}'")
                        return parsed[key]

                # The bug case: array is the KEY → {"[{...}]": []}
                for key in parsed.keys():
                    if key.strip().startswith("["):
                        try:
                            extracted = json.loads(key.strip())
                            if isinstance(extracted, list):
                                logger.warning(
                                    "Fixed: LLM used JSON array as dict key — extracted successfully"
                                )
                                return extracted
                        except json.JSONDecodeError:
                            pass

                # Last dict fallback: grab the first list value regardless of key name
                for v in parsed.values():
                    if isinstance(v, list):
                        logger.warning("Grabbed first list value from dict response")
                        return v

        except json.JSONDecodeError:
            pass

    # ── 4. Regex: find any [...] block anywhere in the string ─────────────
    match = re.search(r'\[.*\]', raw, re.DOTALL)
    if match:
        try:
            extracted = json.loads(match.group())
            if isinstance(extracted, list):
                logger.warning("Used regex fallback to extract JSON array")
                return extracted
        except json.JSONDecodeError:
            pass

    # ── 5. Nothing worked ─────────────────────────────────────────────────
    logger.error(f"Could not extract a JSON array from LLM response:\n{raw[:400]}")
    return []


# ── Step 4: Validate against real schema ──────────────────────────────────

def _validate_relations(
    raw: list[dict],
    sql_schema: dict[str, SQLTableSchema],
    mongo_schema: dict[str, MongoCollectionSchema],
) -> list[CrossSourceRelation]:
    """
    Validates every LLM suggestion against the actual schema.

    Checks (in order):
      1. Mongo collection name exists
      2. SQL table name exists
      3. Mongo field exists in that collection
      4. SQL column exists in that table (falls back to 'id' if not found)

    Anything that fails validation is dropped with a warning log.
    """
    relations  = []
    sql_lower  = {k.lower(): k for k in sql_schema.keys()}
    mongo_lower = {k.lower(): k for k in mongo_schema.keys()}
    seen       = set()   # deduplicate identical relations

    for item in raw:
        try:
            mc         = item.get("mongo_collection", "").lower().strip()
            mf         = item.get("mongo_field", "").strip()
            st         = item.get("sql_table", "").lower().strip()
            sc         = item.get("sql_column", "").strip()
            confidence = item.get("confidence", "medium")
            reasoning  = item.get("reasoning", "")

            # ── Check 1: Mongo collection ──────────────────────────────
            if mc not in mongo_lower:
                logger.warning(f"Skipping — unknown Mongo collection: '{mc}'")
                continue

            # ── Check 2: SQL table ─────────────────────────────────────
            if st not in sql_lower:
                logger.warning(f"Skipping — unknown SQL table: '{st}'")
                continue

            real_mc = mongo_lower[mc]
            real_st = sql_lower[st]

            # ── Check 3: Mongo field ───────────────────────────────────
            if mf not in mongo_schema[real_mc].fields:
                logger.warning(f"Skipping — unknown Mongo field '{mf}' in '{real_mc}'")
                continue

            # ── Check 4: SQL column ────────────────────────────────────
            sql_col_names = [c.name for c in sql_schema[real_st].columns]
            if sc not in sql_col_names:
                if "id" in sql_col_names:
                    logger.warning(
                        f"Column '{sc}' not in '{real_st}' — falling back to 'id'"
                    )
                    sc = "id"
                else:
                    logger.warning(
                        f"Skipping — unknown SQL column '{sc}' in '{real_st}'"
                    )
                    continue

            # ── Deduplicate ────────────────────────────────────────────
            key = (real_mc, mf, real_st, sc)
            if key in seen:
                logger.debug(f"Skipping duplicate relation: {key}")
                continue
            seen.add(key)

            # ── Build the validated relation ───────────────────────────
            relations.append(CrossSourceRelation(
                mongo_collection=real_mc,
                mongo_field=mf,
                sql_table=real_st,
                sql_column=sc,
                confidence=confidence,
                reasoning=reasoning,
            ))

        except Exception as e:
            logger.warning(f"Could not parse relation item {item}: {e}")
            continue

    return relations