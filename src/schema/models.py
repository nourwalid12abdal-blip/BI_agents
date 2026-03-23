# src/schema/models.py
from pydantic import BaseModel
from typing import Optional


class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool = True
    default: Optional[str] = None


class ForeignKey(BaseModel):
    column: str
    ref_table: str
    ref_column: str


class SQLTableSchema(BaseModel):
    columns: list[ColumnInfo]
    primary_keys: list[str]
    foreign_keys: list[ForeignKey]
    row_count: int = -1


class ReferenceHint(BaseModel):
    field: str
    likely_ref: str
    confidence: str  # "high" | "medium"


class MongoCollectionSchema(BaseModel):
    fields: dict[str, str]          # field_name → type_name
    embedded_docs: list[str]        # fields that are sub-documents
    array_fields: list[str]         # fields that are arrays
    reference_hints: list[ReferenceHint]
    sample_count: int
    doc_count: int


class CrossSourceRelation(BaseModel):
    """A detected link between a Mongo field and a SQL table."""
    mongo_collection: str
    mongo_field: str
    sql_table: str
    sql_column: str
    confidence: str                 # "high" | "medium" | "inferred"


class SchemaGraph(BaseModel):
    sql: dict[str, SQLTableSchema]
    mongo: dict[str, MongoCollectionSchema]
    cross_source_relations: list[CrossSourceRelation] = []
    summary: dict = {}