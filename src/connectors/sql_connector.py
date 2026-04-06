# src/connectors/sql_connector.py
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from src.connectors.base_connector import BaseConnector
import logging

logger = logging.getLogger(__name__)


class SQLConnector(BaseConnector):

    def __init__(self, url: str):
        self.url = url
        self.engine = create_engine(url)

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            logger.error(f"SQL connection failed: {e}")
            return False

    def get_schema(self) -> dict:
        """
        Returns a dict like:
        {
          "customers": {
            "columns":      [{"name": "id", "type": "INTEGER", "nullable": False}, ...],
            "primary_keys": ["id"],
            "foreign_keys": [{"column": "customer_id", "ref_table": "customers", "ref_column": "id"}],
            "row_count":    3
          },
          ...
        }
        """
        inspector = inspect(self.engine)
        schema = {}

        for table_name in inspector.get_table_names():
            try:
                columns = []
                for col in inspector.get_columns(table_name):
                    columns.append({
                        "name":     col["name"],
                        "type":     str(col["type"]),
                        "nullable": col.get("nullable", True),
                        "default":  str(col.get("default", "")) or None,
                    })

                pk_info = inspector.get_pk_constraint(table_name)
                primary_keys = pk_info.get("constrained_columns", [])

                foreign_keys = []
                for fk in inspector.get_foreign_keys(table_name):
                    foreign_keys.append({
                        "column":     fk["constrained_columns"][0] if fk["constrained_columns"] else None,
                        "ref_table":  fk["referred_table"],
                        "ref_column": fk["referred_columns"][0] if fk["referred_columns"] else None,
                    })

                indexes = []
                for idx in inspector.get_indexes(table_name):
                    indexes.append({
                        "name":    idx["name"],
                        "columns": idx["column_names"],
                        "unique":  idx.get("unique", False),
                    })

                row_count = self._get_row_count(table_name)

                schema[table_name] = {
                    "columns":      columns,
                    "primary_keys": primary_keys,
                    "foreign_keys": foreign_keys,
                    "indexes":      indexes,
                    "row_count":    row_count,
                }

            except Exception as e:
                logger.warning(f"Could not inspect table '{table_name}': {e}")
                continue

        logger.info(f"SQL schema crawled — {len(schema)} tables found")
        return schema

    def run_query(self, query: str, **kwargs) -> list[dict]:
        """
        Executes a read-only SQL query.
        Raises ValueError if the query looks like a write operation.
        """
        self._assert_read_only(query)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row._mapping) for row in result]
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def get_sample_rows(self, table_name: str, limit: int = 3) -> list[dict]:
        """Returns a few sample rows from a table — useful for giving the LLM context."""
        return self.run_query(f"SELECT * FROM {table_name} LIMIT {limit}")

    def close(self):
        self.engine.dispose()
        logger.info("SQL connection closed")

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_row_count(self, table_name: str) -> int:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar()
        except Exception:
            return -1

    def _assert_read_only(self, query: str):
        """Block any query that could mutate data."""
        forbidden = ["insert", "update", "delete", "drop", "alter", "truncate", "create"]
        q = query.strip().lower()
        for word in forbidden:
            if q.startswith(word):
                raise ValueError(f"Write operation '{word.upper()}' is not allowed. Only SELECT queries are permitted.")