# 🤖 BI Agents

![Python](https://img.shields.io/badge/python-3.13+-blue?logo=python&style=flat)
![Python](https://img.shields.io/badge/Python-3.13-534AB7?logo=python)
[![License: MIT](https://img.shields.io/badge/License-MIT-green?style=flat)](LICENSE)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.55+-FE4354?logo=streamlit&style=flat)](https://streamlit.io)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.135+-009779?logo=fastapi&style=flat)](https://fastapi.tiangolo.com)
[![LangChain](https://img.shields.io/badge/LangChain-1.2+-blue?logo=langchain&style=flat)](https://www.langchain.com)
[![Plotly](https://img.shields.io/badge/Plotly-6.6+-3F4F75?logo=plotly&style=flat)](https://plotly.com)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0+-d71a1b?logo=sqlalchemy&style=flat)
[![MongoDB](https://img.shields.io/badge/MongoDB-4.16+-47A248?logo=mongodb&style=flat)
[![pytest](https://img.shields.io/badge/pytest-9.0+-0A7EBC?logo=pytest&style=flat)
[![uv](https://img.shields.io/badge/uv-0.5+-4B7BEC?logo=astral&style=flat)](https://astral.sh/uv)
[![Code Style: ruff](https://img.shields.io/badge/code%20style-ruff-cyan?style=flat)](https://docs.astral.sh/ruff)

---

A sophisticated AI-powered Business Intelligence agent that transforms natural language queries into actionable data insights. The system translates user questions into SQL or MongoDB queries, executes them against real databases, and presents results with auto-generated, interactive Plotly visualizations.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [The Charter System](#the-charter-system)
4. [Graph Nodes](#graph-nodes)
5. [Connectors](#connectors)
6. [Schema System](#schema-system)
7. [State Management](#state-management)
8. [API & UI](#api--ui)
9. [Getting Started](#getting-started)
10. [Project Structure](#project-structure)
11. [Tech Stack](#tech-stack)

---

## Overview

BI Agents bridges the gap between natural language and database querying. Users can ask questions like *"What were our top 5 selling products last month?"* or *"Show me the trend of user signups over the past 6 months"* and receive:

- **Natural language responses** explaining the results
- **Interactive visualizations** with drill-down capabilities
- **Anomaly detection** highlighting unusual data points
- **Trend analysis** showing growth/decline patterns

The system automatically:
- Classifies user intent (aggregation, filter, trend, comparison, lookup, dashboard)
- Selects the appropriate data source (SQL, MongoDB, or both)
- Generates optimized database queries
- Executes queries safely (read-only, with limits)
- Transforms raw data into intelligent, interactive charts

---

## Architecture

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER INPUT                                     │
│                    (Natural Language Question)                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          UNDERSTAND NODE                                     │
│         (Classify intent, source, entities using LLM)                       │
│              Intent: aggregation|filter|trend|comparison|                   │
│                      lookup|dashboard                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
              ┌─────────────────────┴─────────────────────┐
              ▼                                           ▼
┌─────────────────────────────┐             ┌─────────────────────────────┐
│      SQL PLANNER NODE      │             │    MONGO PLANNER NODE       │
│   (Generate SELECT query)  │             │ (Generate aggregation      │
│   with safety checks)      │             │  pipeline)                 │
└─────────────────────────────┘             └─────────────────────────────┘
              │                                           │
              └─────────────────────┬─────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EXECUTOR NODE                                     │
│            (Run query via SQLAlchemy / PyMongo)                            │
│                   Serialize ObjectId, DateTime, Decimal                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
              ┌─────────────────────┴─────────────────────┐
              ▼                                           ▼
┌─────────────────────────────┐             ┌─────────────────────────────┐
│      CHARTER NODE           │             │      FORMAT NODE            │
│  (7-layer pipeline for      │             │  (Generate natural language │
│   chart generation)         │             │   response from data)      │
│                            │             │                             │
│  1. Normalizer             │             │  - Cleans data              │
│  2. Classifier             │             │  - Computes stats           │
│  3. Selector               │             │  - Generates answer         │
│  4. Intelligence           │             │                             │
│  5. Spec Builder           │             │                             │
│  6. Drilldown              │             │                             │
│  7. Caption                │             │                             │
└─────────────────────────────┘             └─────────────────────────────┘
              │                               │
              └───────────────┬───────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FINAL RESPONSE                                      │
│         (Plain English + Interactive Plotly Chart + Drilldowns)            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Graph Topology

```
START → understand
        │
        ├─→ sql_planner ──→ executor ──→ format → END
        │                              │
        │                              ├─→ retry_sql → sql_planner
        │                              └─→ charter → format
        │
        └─→ mongo_planner ──→ executor ──→ format → END
                                   │
                                   ├─→ retry_mongo → mongo_planner
                                   └─→ charter → format
```

The system includes **retry logic** (max 2 retries) that passes error feedback back to planners to fix query issues.

---

## The Charter System

The Charter is the core differentiator of BI Agents—a 7-layer pipeline that transforms raw database results into intelligent, interactive visualizations.

### Layer 1: Normalizer

**File:** `src/agent/charter/layer1_normalizer.py`

**Purpose:** Clean and flatten raw database output into consistent row dictionaries.

**Transformations:**
1. **Flatten nested dicts**: `{"specs": {"ram_gb": 16}}` → `{"specs_ram_gb": 16}`
2. **Unwrap single-item arrays**: `[42]` → `42`
3. **Parse date strings**: "2024-01-15" → ISO format datetime
4. **Cast numeric strings**: "123.45" → float

**Key Functions:**
```python
def normalize(data: list[dict]) -> list[dict]
def _flatten_dict(d: dict, parent_key: str, sep: str) -> dict
def _clean_value(value) -> any
def _parse_date(value: str) -> datetime
```

---

### Layer 2: Classifier

**File:** `src/agent/charter/layer2_classifier.py`

**Purpose:** Profile each column to understand data types, characteristics, and suggest optimal visualization axes.

**Classification Logic:**
- **Type detection** (70% threshold): temporal → numeric → categorical
- **Cardinality**: Count unique non-null values
- **Null rate**: Fraction of null values
- **Value range**: [min, max] for numeric columns
- **Monotonicity**: increasing | decreasing | flat | mixed

**Output Profile:**
```python
{
    "column_name": {
        "type": "temporal" | "numeric" | "categorical",
        "cardinality": int,
        "null_rate": float,
        "value_range": [min, max] | None,
        "monotonicity": "increasing" | "decreasing" | "flat" | "mixed" | None,
        "sample_values": [val1, val2, ...]
    },
    "_suggestions": {"x_column": "date", "y_column": "revenue"}
}
```

---

### Layer 3: Selector

**File:** `src/agent/charter/layer3_selector.py`

**Purpose:** Intelligently select the best chart type using LLM.

**Supported Chart Types:**
| Category | Chart Types |
|----------|-------------|
| Simple | `kpi`, `table` |
| Categorical | `bar`, `grouped_bar`, `stacked_bar`, `pie`, `donut` |
| Temporal | `line`, `multiline`, `area` |
| Correlational | `scatter`, `bubble`, `heatmap` |
| Funnel | `funnel` |

**Selection Logic:**
1. Single row → always KPI
2. User requested type → use if feasible
3. LLM confidence < 0.6 → fallback to bar chart
4. Hard validation rules for each chart type

**Key Functions:**
```python
def select(profile: dict, question: str, intent: str, 
           row_count: int, requested_chart: str) -> dict
def _is_feasible(chart_type: str, profile: dict, row_count: int) -> bool
def _validate(result: dict, profile: dict, ...) -> dict
```

---

### Layer 4: Intelligence

**File:** `src/agent/charter/layer4_intelligence.py`

**Purpose:** Analyze data for anomalies, trends, and correlations.

**Anomaly Detection:**
- Uses Z-score method (threshold: 1.8 standard deviations)
- Identifies direction (above/below average)
- Generates annotation labels for visualization

**Trend Detection:**
- Linear regression slope analysis
- Categorizes: strongly_growing → growing → flat → declining → strongly_declining
- Calculates percentage change and reversal detection

**Correlation Analysis:**
- Pearson correlation coefficient
- Thresholds: 0.5 (moderate), 0.8 (strong)
- Bidirectional correlation matrix

**Output:**
```python
{
    "anomalies": [
        {"column": "revenue", "label": "December", "value": 150000, 
         "mean": 80000, "z_score": 2.5, "direction": "above_average"}
    ],
    "trend": {
        "direction": "growing",
        "per_column": {...},
        "slope": 0.15,
        "pct_change": 23.5,
        "reversal": False
    },
    "correlations": [
        {"col_a": "ad_spend", "col_b": "revenue", 
         "r": 0.85, "strength": "strong", "direction": "positive"}
    ],
    "annotations": [...]  # Plotly annotation objects
}
```

---

### Layer 5: Spec Builder

**File:** `src/agent/charter/layer5_spec_builder.py`

**Purpose:** Build Plotly-compatible chart specifications.

**Supported Chart Builders:**
| Chart Type | Builder Function | Features |
|------------|------------------|----------|
| Bar | `_build_bar()` | Anomaly highlighting, hover |
| Line/Multiline | `_build_line()` | Trend annotation, markers |
| Area | `_build_area()` | Gradient fill |
| Pie/Donut | `_build_pie()` | Hole=0.42, percentage labels |
| Scatter | `_build_scatter()` | Correlation annotation |
| Heatmap | `_build_heatmap()` | 2 categorical + 1 numeric |
| Funnel | `_build_funnel()` | Conversion stages |
| KPI | `_build_kpi()` | Big number, delta indicator |
| Table | `_build_table()` | Styled data table |

**Color Palette:**
```python
COLORS = [
    "#534AB7",  # Primary purple
    "#1D9E75",  # Green
    "#D85A30",  # Orange
    "#F7B538",  # Yellow
    "#4A90D9",  # Blue
    "#E05D5D",  # Red
    "#8E44AD",  # Purple
    "#1ABC9C",  # Teal
]
```

---

### Layer 6: Drilldown

**File:** `src/agent/charter/layer6_drilldown.py`

**Purpose:** Generate clickable drill-down questions for each data point.

**Question Templates:**
| Intent | Template |
|--------|----------|
| aggregation | "Show me the details behind {x_col} = {label}" |
| filter | "Show all records where {x_col} is {label}" |
| comparison | "Compare {label} against the other {x_col} values" |
| trend | "What caused the change at {label}?" |
| lookup | "Show full details for {label}" |
| dashboard | "Break down the numbers for {label}" |

**Anomaly Template:**
```
"Why is {label} unusually {direction} for {x_col}? 
The value is {value} vs an average of {mean}."
```

**Implementation:**
- O(1) anomaly lookup using dictionary index
- Injects customdata into Plotly traces
- Enables clickable interactions in Streamlit

---

### Layer 7: Caption

**File:** `src/agent/nodes/charter_node.py`

**Purpose:** Generate a one-sentence insight caption combining trend and anomaly information.

**Example:**
```
"Revenue grew by 23.5% with a spike in December, 
which was 2.5 standard deviations above the average."
```

---

## Graph Nodes

### Understand Node

**File:** `src/agent/nodes/understand_node.py`

**Function:** `understand_node(state: AgentState) -> AgentState`

- **Reads:** `state.question`
- **Writes:** `intent`, `source`, `entities`, `needs_chart`, `intent_reasoning`
- **Uses LLM** to classify intent and select data source
- **Entity validation:** Drops hallucinated table/collection names

**Intent Types:**
- `aggregation`: SUM, AVG, COUNT, etc.
- `filter`: WHERE clauses
- `trend`: Time-series analysis
- `comparison`: A vs B analysis
- `lookup`: Single record fetch
- `dashboard`: Multi-metric summary

---

### SQL Planner Node

**File:** `src/agent/nodes/sql_planner_node.py`

**Function:** `sql_planner_node(state: AgentState) -> AgentState`

- **Reads:** `question`, `entities`, `error_feedback`, `retry_count`
- **Writes:** `query_type: "sql"`, `query` (SQL string), `query_reasoning`
- **Safety checks:**
  - Blocks INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE
  - Validates all required entities in query
- **Auto-LIMIT:** Adds LIMIT 1000 for non-aggregation queries

---

### Mongo Planner Node

**File:** `src/agent/nodes/mongo_planner_node.py`

**Function:** `mongo_planner_node(state: AgentState) -> AgentState`

- **Reads:** `question`, `intent`, `entities`, `error_feedback`
- **Writes:** `query_type: "mongo"`, `query` (pipeline list), `collection`
- **Intent-specific rules:** Uses `INTENT_RULES` dict for each intent type
- **Safety checks:** Blocks `$out`, `$merge` write stages
- **Auto-LIMIT:** Adds `$limit 1000` for non-group queries

---

### Executor Node

**File:** `src/agent/nodes/executor_node.py`

**Function:** `executor_node(state: AgentState) -> AgentState`

- **Reads:** `query_type`, `query`, `collection`
- **Writes:** `success`, `data`, `row_count`, `execution_error`
- **SQL:** Uses SQLAlchemy `text()` for raw SQL
- **MongoDB:** Uses PyMongo `aggregate()` with 30s timeout
- **Serialization:**
  - `ObjectId` → string
  - `datetime` → ISO format
  - `Decimal` → string

---

### Charter Node

**File:** `src/agent/nodes/charter_node.py`

**Function:** `charter_node(state: AgentState) -> AgentState`

- **Reads:** `data`, `row_count`, `question`, `intent`, `needs_chart`
- **Writes:** `chart_spec` (full Plotly spec with caption and drilldowns)
- **Runs full 7-layer charter pipeline**

---

### Format Node

**File:** `src/agent/nodes/format_node.py`

**Function:** `format_node(state: AgentState) -> AgentState`

- **Reads:** `success`, `data`, `row_count`, `question`, `execution_error`
- **Writes:** `response` (plain English answer)
- **Data cleaning:** Removes junk columns (`_id`, `__v`), renames to readable labels
- **Pre-computes stats:** min, max, average, total for numeric columns
- **Output rule:** Max 2 sentences, factual, no speculation

---

### Merge Node

**File:** `src/agent/nodes/merge_node.py`

**Function:** `merge_node(state: AgentState) -> AgentState`

- **Purpose:** Handle queries requiring both SQL and MongoDB data
- **Reads:** `sql_data`, `mongo_data`
- **Writes:** `data`, `row_count`
- **Join logic:** Uses `cross_source_relations` from schema graph
- **Merge strategy:** Left join, SQL fields first, Mongo fields merged with `_mongo` suffix

---

## Connectors

### Base Connector

**File:** `src/connectors/base_connector.py`

Abstract interface:
```python
class BaseConnector(ABC):
    @abstractmethod
    def get_schema(self) -> dict: ...
    
    @abstractmethod
    def run_query(self, query, **kwargs) -> list[dict]: ...
    
    @abstractmethod
    def test_connection(self) -> bool: ...
    
    @abstractmethod
    def close(self): ...
```

---

### SQL Connector

**File:** `src/connectors/sql_connector.py`

**Class:** `SQLConnector(BaseConnector)`

**Initialization:**
```python
SQLConnector(url: str)  # SQLAlchemy connection string
```

**Methods:**
| Method | Description |
|--------|-------------|
| `test_connection()` | Runs `SELECT 1` |
| `get_schema()` | Returns tables with columns, PKs, FKs, indexes, row counts |
| `run_query(query)` | Executes SELECT, returns dicts |
| `get_sample_rows(table, limit)` | Sample rows for LLM context |
| `_assert_read_only(query)` | Blocks write operations |

---

### MongoDB Connector

**File:** `src/connectors/mongo_connector.py`

**Class:** `MongoConnector(BaseConnector)`

**Initialization:**
```python
MongoConnector(uri, db_name, sample_limit=50)
```

**Methods:**
| Method | Description |
|--------|-------------|
| `test_connection()` | Runs `ping` command |
| `get_schema()` | Returns collections with fields, embedded docs, array fields, reference hints, doc counts |
| `run_query(collection, pipeline)` | Runs aggregation pipeline |
| `get_sample_docs(collection, limit)` | Sample docs for LLM |

**Inference Functions:**
- `_infer_fields(samples)` → Maps field → most common type
- `_detect_embedded_docs(samples)` → Finds nested dict fields
- `_detect_array_fields(samples)` → Finds list fields
- `_detect_reference_hints(fields, collection_name)` → Infers FKs from `*_id` naming

---

## Schema System

### Models

**File:** `src/schema/models.py`

```python
# SQL Schema
ColumnInfo(name, type, nullable, default)
ForeignKey(column, ref_table, ref_column)
SQLTableSchema(columns, primary_keys, foreign_keys, row_count)

# MongoDB Schema  
ReferenceHint(field, likely_ref, confidence)
MongoCollectionSchema(fields, embedded_docs, array_fields, 
                      reference_hints, sample_count, doc_count)

# Cross-Source Relations
CrossSourceRelation(mongo_collection, mongo_field, 
                    sql_table, sql_column, confidence)

# Full Schema Graph
SchemaGraph(sql: dict, mongo: dict, cross_source_relations, summary)
```

---

### Crawler

**File:** `src/schema/crawler.py`

**Function:** `crawl(sql_url, mongo_uri, mongo_db, sample_limit) -> SchemaGraph`

**Process:**
1. Connect to SQL database via SQLAlchemy
2. Connect to MongoDB via PyMongo
3. Extract SQL schema (tables, columns, PKs, FKs, indexes)
4. Extract MongoDB schema (collections, fields, types)
5. Detect cross-source relations using LLM
6. Return `SchemaGraph`

---

### Relation Detector

**File:** `src/schema/relation_detector.py`

**Function:** `detect_cross_source_relations(sql_schema, mongo_schema) -> list[CrossSourceRelation]`

**Process:**
1. Build schema context with sample values and type hints
2. Ask LLM to identify Mongo fields that reference SQL tables
3. Validate every suggestion against actual schema (drops hallucinations)
4. Return validated `CrossSourceRelation` objects

**Validation checks:**
- Mongo collection exists
- SQL table exists
- Mongo field exists in collection
- SQL column exists in table (falls back to 'id')

---

### Schema Store

**File:** `src/schema/schema_store.py`

**Functions:**
| Function | Description |
|----------|-------------|
| `save(graph: SchemaGraph, path)` | Serializes to JSON |
| `load(path) -> SchemaGraph` | Loads from JSON |
| `get_schema_summary_for_llm(graph)` | Creates detailed text summary for LLM prompts |

---

## State Management

### AgentState (TypedDict)

**File:** `src/agent/state.py`

**Input Fields (set once):**
| Field | Type | Description |
|-------|------|-------------|
| `question` | `str` | Raw user question |

**Understand Node Output:**
| Field | Type | Description |
|-------|------|-------------|
| `intent` | `str` | aggregation\|filter\|trend\|comparison\|lookup\|dashboard |
| `source` | `str` | sql\|mongo\|both |
| `entities` | `list[str]` | Required table/collection names |
| `needs_chart` | `bool` | Whether to generate visualization |
| `intent_reasoning` | `str` | LLM's reasoning (debugging) |

**Planner Node Output:**
| Field | Type | Description |
|-------|------|-------------|
| `query_type` | `str` | sql\|mongo |
| `query` | `Any` | SQL string or MongoDB pipeline |
| `collection` | `Optional[str]` | MongoDB collection (None for SQL) |
| `query_reasoning` | `str` | Debug info |

**Executor Node Output:**
| Field | Type | Description |
|-------|------|-------------|
| `success` | `bool` | Query succeeded |
| `data` | `list[dict]` | Results |
| `row_count` | `int` | Number of rows |
| `execution_error` | `Optional[str]` | Error message if failed |

**Retry Control:**
| Field | Type | Description |
|-------|------|-------------|
| `retry_count` | `int` | Current retry number |
| `max_retries` | `int` | Limit (default 2) |
| `error_feedback` | `Optional[str]` | Error for retry |

**Format/Chart Output:**
| Field | Type | Description |
|-------|------|-------------|
| `response` | `Optional[str]` | Plain English answer |
| `chart_spec` | `Optional[dict]` | Plotly spec with caption and drilldowns |
| `final_error` | `Optional[str]` | Error if all retries exhausted |

---

## API & UI

### FastAPI Endpoints

**File:** `src/api.py`

#### GET `/health`
```json
{"status": "ok"}
```

#### POST `/ask`
**Request:**
```python
{
    "question": str,
    "needs_chart": bool = True
}
```

**Response:**
```python
{
    "question": str,
    "response": str,
    "chart_spec": dict | None,
    "intent": str,
    "source": str,
    "success": bool,
    "error": str | None
}
```

---

### Streamlit UI

**File:** `streamlit_app.py`

#### Sidebar Components:
- **SQL connection URL** input
- **MongoDB URI** and **database name** inputs
- **"Crawl schema"** button - triggers schema crawl
- **"Seed test data"** button - populates test data
- **Schema summary** display (SQL tables, Mongo collections, column counts)
- **Full schema expander** - View complete schema details
- **SQL tables detail** expander
- **MongoDB collections detail** expander
- **Cross-source relations** expander
- **"Generate chart"** toggle
- **"Clear history"** button

#### Main Area Components:
- **Question text input**
- **"Ask"** button
- **Response display** with intent/source badges
- **Chart rendering** via `render_chart()`:
  - Uses Plotly graph_objects
  - Maps chart_type to trace class
  - Renders drill-down buttons if available
- **History display** (reversed order, numbered)

---

## Getting Started

### Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) (recommended) or `pip`

### Installation

```bash
# Clone the repository
git clone <repo-url>
cd BI_agents

# Install dependencies using uv
uv sync

# Or with pip
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the root directory:

```env
# API Keys (at least one required)
OPENAI_API_KEY=your_openai_key_here
GROQ_API_KEY=your_groq_key_here
HF_TOKEN=your_huggingface_token_here

# LLM Models (optional, defaults shown)
groq_model=llama-3.3-70b-versatile
openai_model=gpt-4o
HF_MODEL=Qwen/Qwen2.5-72B-Instruct

# Database Connections
sql_db_url=postgresql://user:pass@localhost:5432/mydb
mongo_uri=mongodb://localhost:27017
mongo_db_name=my_database

# Schema Settings
schema_graph_path=schema_graph.json
schema_sample_limit=50
```

### Running the Application

#### Streamlit UI
```bash
streamlit run streamlit_app.py
```

#### FastAPI Server
```bash
uvicorn src.api:app --reload
```

### Seeding Test Data

```bash
# Seed SQL test data
python scripts/seed_test_data.py

# Seed complex data
python scripts/seed_complex_data.py
```

### Running Tests

```bash
pytest tests/
```

---

## Project Structure

```
BI_agents/
├── src/
│   ├── agent/
│   │   ├── charter/           # Charter pipeline layers
│   │   │   ├── layer1_normalizer.py
│   │   │   ├── layer2_classifier.py
│   │   │   ├── layer3_selector.py
│   │   │   ├── layer4_intelligence.py
│   │   │   ├── layer5_spec_builder.py
│   │   │   └── layer6_drilldown.py
│   │   ├── nodes/            # LangGraph nodes
│   │   │   ├── understand_node.py
│   │   │   ├── sql_planner_node.py
│   │   │   ├── mongo_planner_node.py
│   │   │   ├── executor_node.py
│   │   │   ├── charter_node.py
│   │   │   ├── format_node.py
│   │   │   └── merge_node.py
│   │   ├── graph.py           # LangGraph definition
│   │   ├── state.py          # AgentState TypedDict
│   │   └── llm.py             # LLM configuration
│   ├── connectors/            # Database connectors
│   │   ├── base_connector.py
│   │   ├── sql_connector.py
│   │   └── mongo_connector.py
│   ├── schema/                # Schema system
│   │   ├── crawler.py
│   │   ├── models.py
│   │   ├── relation_detector.py
│   │   └── schema_store.py
│   └── api.py                 # FastAPI endpoints
├── scripts/                   # Utility scripts
│   ├── crawl_schema.py
│   ├── seed_test_data.py
│   ├── seed_complex_data.py
│   └── test_all_cases.py
├── config/
│   └── settings.py            # Pydantic settings
├── tests/
│   └── test_agent.py
├── streamlit_app.py            # Streamlit UI
├── pyproject.toml             # Project config
└── requirements.txt           # Dependencies
```

---

## Tech Stack

| Category | Technology |
|----------|-------------|
| **Language** | Python 3.13 |
| **Frontend** | Streamlit |
| **Backend** | FastAPI / Uvicorn |
| **AI Orchestration** | LangChain |
| **Graph State** | LangGraph |
| **LLMs** | OpenAI, Groq, Ollama, HuggingFace |
| **SQL** | SQLAlchemy |
| **NoSQL** | PyMongo |
| **Visualization** | Plotly |
| **Validation** | Pydantic |
| **Testing** | pytest |
| **Package Manager** | uv |

---

## License

Distributed under the MIT License.
