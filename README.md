# NL2SQL Context-Aware Engine

A deterministic, context-driven Natural Language to SQL translation engine. Unlike traditional rule-based systems, this engine derives its understanding entirely from your database **metadata** (descriptions, data types, and sample values), allowing it to resolve filters and aggregations without any hardcoded column or table mappings.

## 🚀 Running the API

Start the FastAPI server:
```bash
uv run uvicorn api.main:app --host 0.0.0.0 --port 8000
```
*(Note: The first request may take a few seconds as the spaCy NLP model loads.)*

## 📡 API Usage

### `POST /translate`

Accepts a natural language query and the database schema metadata.

**Request Structure:**
```json
{
  "query": "list male employees hired after 2020",
  "metadata": {
    "tables": {
      "employees": {
        "description": "Employee records and salary data",
        "columns": {
          "employee_id": { "data_type": "integer", "is_primary_key": true },
          "first_name": { "data_type": "text", "description": "First name" },
          "gender": { "data_type": "text", "distinct_values": ["Male", "Female"] },
          "hire_date": { "data_type": "date", "description": "Joining date" },
          "salary": { "data_type": "numeric" }
        }
      }
    }
  }
}
```

**Response (`is_matching: true`):**
```json
{
  "is_matching": true,
  "sql_query": "SELECT * FROM employees WHERE gender = 'Male' AND EXTRACT(YEAR FROM hire_date) > 2020;"
}
```

---

## 🔥 Key Features

### 1. Zero Hardcoding
The engine has no built-in knowledge of your tables. It uses **Fuzzy Semantic Matching** against column descriptions and human-readable labels provided in the metadata.

### 2. Auto-Filter Resolution
By providing `distinct_values` or `sample_values`, the engine can automatically map user words to specific column values.
*   *Input:* "employees from Chennai"
*   *Detection:* Matches "Chennai" in `city` column metadata → `WHERE city = 'Chennai'`

### 3. Smart Date/Time Logic
Supports temporal comparisons like `after`, `since`, `before`, and `prior to`.
*   *Input:* "hired after 2020"
*   *Logic:* Detects `2020` as a year and `hire_date` as a date type → `EXTRACT(YEAR FROM hire_date) > 2020`

### 4. Ranking & Aggregations
Intelligently handles "Top N" queries and business aggregations:
*   *Input:* "Top 5 employees by salary"
*   *Result:* Automatically includes label columns (names/titles) for context → `SELECT first_name, last_name, salary FROM employees ORDER BY salary DESC LIMIT 5;`

### 5. Hallucination Prevention
If a query is completely unrelated to the schema (e.g., "What is the capital of France?"), the engine scores the relevance and returns `is_matching: false` rather than generating invalid queries.

---

## 🛠 Project Structure

- `nlp/parser.py`: Pure linguistic analysis (parts-of-speech, named entities).
- `nlp/entity_extractor.py`: Context-driven resolution of tables, columns, and values.
- `query_builder/sql_generator.py`: Generates standards-compliant SQL using SQLAlchemy/sqlglot patterns.
- `schema/inspector.py`: Tool to automatically generate the rich `metadata.json` from an existing DB.

## 🧪 Testing
Run the comprehensive test suite to verify 17+ different query patterns:
```bash
uv run python test_fastapi.py
```
