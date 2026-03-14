# NL2SQL FastAPI Endpoint

The project has been configured with a dynamic FastAPI endpoint that accepts both the Natural Language query and the Database Schema metadata as arguments for translation.

## 🚀 Running the API

Start the API server by running:
```bash
uv run uvicorn api.main:app --host 0.0.0.0 --port 8000
```
(Be aware: the first API request might take 1-2 seconds longer as spaCy initially loads in the background).

## 📡 API Endpoint Details

- **URL:** `POST /translate`
- **Content-Type:** `application/json`

### Option 1: Valid Matching Query
Send a natural language query with the JSON database describe mapping (the Tables & Columns format that models Postgres schemas).

**Input Form:**
```json
{
  "query": "Show the top 3 customers who spent the most money",
  "metadata": {
    "tables": {
      "customers": {
        "columns": ["customer_id", "first_name", "last_name", "email", "city"],
        "primary_key": "customer_id"
      },
      "orders": {
        "columns": ["order_id", "customer_id", "order_date", "total_amount"],
        "primary_key": "order_id",
        "foreign_keys": {
          "customer_id": "customers.customer_id"
        }
      }
    }
  }
}
```

**Output Form (`200 OK`):**
Because the schema successfully relates to the question, the system matches it and binds the proper SQL.
```json
{
  "is_matching": true,
  "sql_query": "SELECT SUM(t1.total_amount), t2.first_name, t2.last_name, t2.email, t2.city FROM orders AS t1 JOIN customers AS t2 ON t1.customer_id = t2.customer_id GROUP BY t2.first_name, t2.last_name, t2.email, t2.city ORDER BY SUM(t1.total_amount) DESC LIMIT 3;"
}
```

### Option 2: Unrelated (Non-Matching) Query
If the natural language query asks about something outside the provided schema database (e.g. asking about `"capital of France"` when the Postgres schema holds `"customers"`), it will be detected as invalid.

**Input Form:**
```json
{
  "query": "What is the capital of France?",
  "metadata": {
    "tables": {
      "customers": {
        ...
      }
    }
  }
}
```

**Output Form (`200 OK`):**
```json
{
  "is_matching": false,
  "sql_query": null
}
```

## ⚙️ How it Works Intemally
1. The Postgres Database schema passed into `request.metadata` is dynamically pushed into the `EntityExtractor` and `SQLGenerator` memory classes.
2. The endpoint checks how many exact Tables, Columns or Filters match in the context.
3. If no matching Tables or context filters identify themselves, the API safely falls back indicating the question implies objects outside the Postgres context (`is_matching: false`).
4. Provided it has semantic references, the query gets bundled into `SQLGenerator(request.metadata).generate()` and safely returns the valid SQL syntax.
