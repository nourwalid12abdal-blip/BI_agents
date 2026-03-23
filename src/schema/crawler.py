# src/schema/crawler.py
from src.connectors.sql_connector import SQLConnector
from src.connectors.mongo_connector import MongoConnector
from src.schema.models import (
    SchemaGraph, SQLTableSchema, MongoCollectionSchema,
    ColumnInfo, ForeignKey, ReferenceHint
)
from src.schema.relation_detector import detect_cross_source_relations
from config.settings import settings
import logging

logger = logging.getLogger(__name__)


def crawl(
    sql_url: str = None,
    mongo_uri: str = None,
    mongo_db: str = None,
    sample_limit: int = None,
) -> SchemaGraph:
    """
    Connects to both databases, crawls their schemas,
    detects cross-source relations, and returns a SchemaGraph.

    All arguments fall back to settings if not provided,
    so calling crawl() with no args just works.
    """
    sql_url     = sql_url     or settings.sql_db_url
    mongo_uri   = mongo_uri   or settings.mongo_uri
    mongo_db    = mongo_db    or settings.mongo_db_name
    sample_limit = sample_limit or settings.schema_sample_limit

    # ── 1. SQL ─────────────────────────────────────────────────────────────
    logger.info("Crawling SQL schema...")
    sql_connector = SQLConnector(sql_url)

    if not sql_connector.test_connection():
        raise ConnectionError(f"Cannot connect to SQL database: {sql_url}")

    raw_sql = sql_connector.get_schema()
    sql_schema = _parse_sql_schema(raw_sql)
    logger.info(f"SQL: found {len(sql_schema)} tables")

    # ── 2. MongoDB ─────────────────────────────────────────────────────────
    logger.info("Crawling MongoDB schema...")
    mongo_connector = MongoConnector(mongo_uri, mongo_db, sample_limit)

    if not mongo_connector.test_connection():
        raise ConnectionError(f"Cannot connect to MongoDB: {mongo_uri}")

    raw_mongo = mongo_connector.get_schema()
    mongo_schema = _parse_mongo_schema(raw_mongo)
    logger.info(f"MongoDB: found {len(mongo_schema)} collections")

    # ── 3. Detect cross-source relations ───────────────────────────────────
    logger.info("Detecting cross-source relations...")
    relations = detect_cross_source_relations(sql_schema, mongo_schema)

    # ── 4. Build summary ───────────────────────────────────────────────────
    summary = _build_summary(sql_schema, mongo_schema, relations)

    # ── 5. Close connections ───────────────────────────────────────────────
    sql_connector.close()
    mongo_connector.close()

    graph = SchemaGraph(
        sql=sql_schema,
        mongo=mongo_schema,
        cross_source_relations=relations,
        summary=summary,
    )

    logger.info("Schema crawl complete")
    _print_summary(summary)
    return graph


# ── Parsers ────────────────────────────────────────────────────────────────

def _parse_sql_schema(raw: dict) -> dict[str, SQLTableSchema]:
    parsed = {}
    for table_name, table_data in raw.items():
        columns = [
            ColumnInfo(
                name=col["name"],
                type=col["type"],
                nullable=col.get("nullable", True),
                default=col.get("default"),
            )
            for col in table_data.get("columns", [])
        ]
        foreign_keys = [
            ForeignKey(
                column=fk["column"],
                ref_table=fk["ref_table"],
                ref_column=fk["ref_column"],
            )
            for fk in table_data.get("foreign_keys", [])
            if fk.get("column") and fk.get("ref_table") and fk.get("ref_column")
        ]
        parsed[table_name] = SQLTableSchema(
            columns=columns,
            primary_keys=table_data.get("primary_keys", []),
            foreign_keys=foreign_keys,
            row_count=table_data.get("row_count", -1),
        )
    return parsed


def _parse_mongo_schema(raw: dict) -> dict[str, MongoCollectionSchema]:
    parsed = {}
    for col_name, col_data in raw.items():
        hints = [
            ReferenceHint(
                field=h["field"],
                likely_ref=h["likely_ref"],
                confidence=h["confidence"],
            )
            for h in col_data.get("reference_hints", [])
        ]
        parsed[col_name] = MongoCollectionSchema(
            fields=col_data.get("fields", {}),
            embedded_docs=col_data.get("embedded_docs", []),
            array_fields=col_data.get("array_fields", []),
            reference_hints=hints,
            sample_count=col_data.get("sample_count", 0),
            doc_count=col_data.get("doc_count", 0),
        )
    return parsed


# ── Summary ────────────────────────────────────────────────────────────────

def _build_summary(sql_schema, mongo_schema, relations) -> dict:
    total_sql_cols = sum(len(t.columns) for t in sql_schema.values())
    total_sql_fks  = sum(len(t.foreign_keys) for t in sql_schema.values())
    total_mongo_fields = sum(len(c.fields) for c in mongo_schema.values())

    return {
        "sql_tables":           len(sql_schema),
        "sql_total_columns":    total_sql_cols,
        "sql_foreign_keys":     total_sql_fks,
        "mongo_collections":    len(mongo_schema),
        "mongo_total_fields":   total_mongo_fields,
        "cross_source_links":   len(relations),
    }


def _print_summary(summary: dict):
    print("\n── Schema Crawl Summary ─────────────────────────")
    print(f"  SQL tables:          {summary['sql_tables']}")
    print(f"  SQL columns:         {summary['sql_total_columns']}")
    print(f"  SQL foreign keys:    {summary['sql_foreign_keys']}")
    print(f"  Mongo collections:   {summary['mongo_collections']}")
    print(f"  Mongo fields:        {summary['mongo_total_fields']}")
    print(f"  Cross-source links:  {summary['cross_source_links']}")
    print("─────────────────────────────────────────────────\n")