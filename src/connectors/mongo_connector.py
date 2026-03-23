# src/connectors/mongo_connector.py
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from src.connectors.base_connector import BaseConnector
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)


class MongoConnector(BaseConnector):

    def __init__(self, uri: str, db_name: str, sample_limit: int = 50):
        self.uri = uri
        self.db_name = db_name
        self.sample_limit = sample_limit
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[db_name]

    def test_connection(self) -> bool:
        try:
            self.client.admin.command("ping")
            return True
        except ConnectionFailure as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False

    def get_schema(self) -> dict:
        """
        Returns a dict like:
        {
          "user_events": {
            "fields":            {"customer_id": "int", "event": "str", "page": "str"},
            "embedded_docs":     ["specs"],
            "array_fields":      ["tags"],
            "reference_hints":   [{"field": "customer_id", "likely_ref": "customers"}],
            "sample_count":      6,
            "doc_count":         6
          },
          ...
        }
        """
        schema = {}

        for col_name in self.db.list_collection_names():
            try:
                samples = list(self.db[col_name].find().limit(self.sample_limit))
                if not samples:
                    schema[col_name] = {
                        "fields": {}, "embedded_docs": [],
                        "array_fields": [], "reference_hints": [],
                        "sample_count": 0, "doc_count": 0
                    }
                    continue

                fields = self._infer_fields(samples)
                embedded_docs = self._detect_embedded_docs(samples)
                array_fields = self._detect_array_fields(samples)
                reference_hints = self._detect_reference_hints(fields, col_name)
                doc_count = self.db[col_name].estimated_document_count()

                schema[col_name] = {
                    "fields":          fields,
                    "embedded_docs":   embedded_docs,
                    "array_fields":    array_fields,
                    "reference_hints": reference_hints,
                    "sample_count":    len(samples),
                    "doc_count":       doc_count,
                }

            except Exception as e:
                logger.warning(f"Could not inspect collection '{col_name}': {e}")
                continue

        logger.info(f"MongoDB schema crawled — {len(schema)} collections found")
        return schema

    def run_query(self, collection: str, pipeline: list, **kwargs) -> list[dict]:
        """
        Runs a MongoDB aggregation pipeline on a collection.
        Returns results as a list of plain dicts (ObjectId converted to str).
        """
        try:
            results = list(self.db[collection].aggregate(pipeline))
            return [self._serialize(doc) for doc in results]
        except OperationFailure as e:
            logger.error(f"MongoDB aggregation failed on '{collection}': {e}")
            raise

    def get_sample_docs(self, collection: str, limit: int = 3) -> list[dict]:
        """Returns a few sample documents — useful for LLM context."""
        docs = list(self.db[collection].find().limit(limit))
        return [self._serialize(doc) for doc in docs]

    def close(self):
        self.client.close()
        logger.info("MongoDB connection closed")

    # ── Field inference ───────────────────────────────────────────────────────

    def _infer_fields(self, samples: list[dict]) -> dict[str, str]:
        """
        Scans all sampled documents and builds a field → type map.
        When a field has mixed types across docs, records the most common one.
        Skips _id and nested sub-document fields (those go into embedded_docs).
        """
        field_types: dict[str, dict[str, int]] = {}

        for doc in samples:
            for key, value in doc.items():
                if key == "_id":
                    continue
                type_name = self._python_type_to_name(value)
                if key not in field_types:
                    field_types[key] = {}
                field_types[key][type_name] = field_types[key].get(type_name, 0) + 1

        # Resolve to the most common type per field
        resolved = {}
        for field, counts in field_types.items():
            resolved[field] = max(counts, key=counts.get)

        return resolved

    def _detect_embedded_docs(self, samples: list[dict]) -> list[str]:
        """
        Returns field names whose value is a nested dict in at least one sample doc.
        These represent embedded sub-documents (e.g. "specs": {"ram_gb": 16}).
        """
        embedded = set()
        for doc in samples:
            for key, value in doc.items():
                if key == "_id":
                    continue
                if isinstance(value, dict):
                    embedded.add(key)
        return sorted(embedded)

    def _detect_array_fields(self, samples: list[dict]) -> list[str]:
        """
        Returns field names whose value is a list in at least one sample doc.
        E.g. "tags": ["electronics", "mobile"]
        """
        arrays = set()
        for doc in samples:
            for key, value in doc.items():
                if key == "_id":
                    continue
                if isinstance(value, list):
                    arrays.add(key)
        return sorted(arrays)

    def _detect_reference_hints(self, fields: dict[str, str], collection_name: str) -> list[dict]:
        """
        Looks for fields named *_id or *_ref that are likely foreign references
        to other collections. E.g. "customer_id" probably references "customers".
        Returns hints — not guaranteed, just inferred from naming conventions.
        """
        hints = []
        for field in fields:
            if field == "_id":
                continue
            if field.endswith("_id") and field != "_id":
                likely_ref = field[:-3] + "s"  # customer_id → customers
                hints.append({
                    "field":      field,
                    "likely_ref": likely_ref,
                    "confidence": "high" if field.endswith("_id") else "medium",
                })
            elif field.endswith("_ref"):
                likely_ref = field[:-4] + "s"
                hints.append({
                    "field":      field,
                    "likely_ref": likely_ref,
                    "confidence": "medium",
                })
        return hints

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _python_type_to_name(self, value) -> str:
        """Maps a Python value to a readable type name for the schema."""
        if isinstance(value, bool):     return "bool"
        if isinstance(value, int):      return "int"
        if isinstance(value, float):    return "float"
        if isinstance(value, str):      return "str"
        if isinstance(value, dict):     return "embedded_doc"
        if isinstance(value, list):     return "array"
        if isinstance(value, ObjectId): return "ObjectId"
        return type(value).__name__

    def _serialize(self, doc: dict) -> dict:
        """Converts ObjectId and other non-JSON-serializable types to strings."""
        result = {}
        for k, v in doc.items():
            if isinstance(v, ObjectId):
                result[k] = str(v)
            elif isinstance(v, dict):
                result[k] = self._serialize(v)
            elif isinstance(v, list):
                result[k] = [str(i) if isinstance(i, ObjectId) else i for i in v]
            else:
                result[k] = v
        return result