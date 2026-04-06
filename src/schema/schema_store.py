# src/schema/schema_store.py
from src.schema.models import SchemaGraph
from config.settings import settings
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)


def save(graph: SchemaGraph, path: Path = None) -> Path:
    """Serializes the SchemaGraph to a JSON file."""
    path = path or settings.schema_graph_path
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(graph.model_dump(), f, indent=2, default=str)

    size_kb = path.stat().st_size / 1024
    logger.info(f"Schema graph saved to {path} ({size_kb:.1f} KB)")
    return path


def load(path: Path = None) -> SchemaGraph:
    """Loads a previously saved SchemaGraph from JSON."""
    path = path or settings.schema_graph_path
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(
            f"Schema graph not found at {path}. "
            "Run 'python scripts/crawl_schema.py' first."
        )

    with open(path) as f:
        data = json.load(f)

    graph = SchemaGraph(**data)
    logger.info(f"Schema graph loaded — {len(graph.sql)} SQL tables, {len(graph.mongo)} Mongo collections")
    return graph


# def get_schema_summary_for_llm(graph: SchemaGraph) -> str:
#     """
#     Produces a compact text summary of the schema the LLM
#     receives as context in every prompt. Kept short on purpose.
#     """
#     lines = ["=== DATABASE SCHEMA ===\n"]

#     lines.append("-- SQL TABLES --")
#     for table, schema in graph.sql.items():
#         col_str = ", ".join(
#             f"{c.name} ({c.type})" + (" PK" if c.name in schema.primary_keys else "")
#             for c in schema.columns
#         )
#         lines.append(f"  {table}: {col_str}")
#         for fk in schema.foreign_keys:
#             lines.append(f"    FK: {fk.column} → {fk.ref_table}.{fk.ref_column}")

#     lines.append("\n-- MONGODB COLLECTIONS --")
#     for col, schema in graph.mongo.items():
#         field_str = ", ".join(f"{k} ({v})" for k, v in schema.fields.items())
#         lines.append(f"  {col}: {field_str}")
#         if schema.embedded_docs:
#             lines.append(f"    Embedded: {', '.join(schema.embedded_docs)}")

#     if graph.cross_source_relations:
#         lines.append("\n-- CROSS-SOURCE LINKS --")
#         for rel in graph.cross_source_relations:
#             lines.append(
#                 f"  mongo.{rel.mongo_collection}.{rel.mongo_field} "
#                 f"→ sql.{rel.sql_table}.{rel.sql_column} [{rel.confidence}]"
#             )

#     return "\n".join(lines)


# src/schema/schema_store_summary.py

from datetime import datetime
import json

def get_schema_summary_for_llm(graph):
    """
    Produces a detailed schema summary for LLM:
    - Includes up to 3 sample rows per table/collection
    - Shows column types and hints (categorical/numeric/datetime)
    - Shows PK/FK for SQL, embedded docs for Mongo
    - Lists categories for categorical columns
    - Keeps cross-source links
    """
    def format_sample(vals):
        if not vals:
            return ""
        # Limit to 3 samples
        samples = vals[:3]
        # Wrap strings in quotes
        formatted = [f'"{v}"' if isinstance(v, str) else str(v) for v in samples]
        return f" sample: {', '.join(formatted)}"

    lines = ["=== DATABASE SCHEMA ===\n"]

    # ── SQL TABLES ──────────────────────────────────────────────
    lines.append("-- SQL TABLES --")
    for table, schema in graph.sql.items():
        for col in schema.columns:
            col_type = col.type
            # infer hint
            if col_type.lower() in ("integer", "int", "real", "float", "numeric", "double"):
                hint = "[numeric]"
            elif "date" in col_type.lower() or "time" in col_type.lower():
                hint = "[datetime]"
            else:
                hint = "[categorical]"
            # sample values
            sample_vals = getattr(col, "samples", None)
            sample_str = format_sample(sample_vals)
            pk_flag = " PK" if col.name in schema.primary_keys else ""
            lines.append(f"  {table}.{col.name} ({col_type}){pk_flag} {hint}{sample_str}")
        # foreign keys
        for fk in schema.foreign_keys:
            lines.append(f"    FK: {fk.column} → {fk.ref_table}.{fk.ref_column}")

    # ── MONGODB COLLECTIONS ─────────────────────────────────────
    lines.append("\n-- MONGODB COLLECTIONS --")
    for col_name, schema in graph.mongo.items():
        for field, ftype in schema.fields.items():
            hint = "[numeric]" if ftype in ("int", "float") else "[datetime]" if "date" in ftype else "[categorical]"
            # sample values
            sample_vals = getattr(schema, "samples", {}).get(field, None)
            sample_str = format_sample(sample_vals)
            lines.append(f"  {col_name}.{field} ({ftype}) {hint}{sample_str}")
        # embedded docs
        if schema.embedded_docs:
            for emb in schema.embedded_docs:
                emb_fields = getattr(schema, "embedded_samples", {}).get(emb, {})
                if emb_fields:
                    for f, vals in emb_fields.items():
                        emb_sample = format_sample(vals)
                        lines.append(f"    Embedded {emb}.{f} {emb_sample}")
                else:
                    lines.append(f"    Embedded: {emb}")

    # ── CROSS-SOURCE LINKS ─────────────────────────────────────
    if getattr(graph, "cross_source_relations", None):
        lines.append("\n-- CROSS-SOURCE LINKS --")
        for rel in graph.cross_source_relations:
            lines.append(
                f"  mongo.{rel.mongo_collection}.{rel.mongo_field} → sql.{rel.sql_table}.{rel.sql_column} [{rel.confidence}]"
            )

    return "\n".join(lines)