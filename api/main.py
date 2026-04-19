# Developer Bershay

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any

from nlp.parser import NLParser
from nlp.entity_extractor import EntityExtractor, IntentClassifier
from query_builder.sql_generator import SQLGenerator

app = FastAPI(title="NL2SQL API")

# Stateless singletons
parser = NLParser()
classifier = IntentClassifier()


class QueryRequest(BaseModel):
    query: str
    metadata: Dict[str, Any]


class QueryResponse(BaseModel):
    is_matching: bool
    sql_query: Optional[str] = None


@app.get("/")
def read_root():
    return {"message": "Welcome to NL2SQL API"}


import os
import sys

# Structured secret path for the hidden AI core engine
engine_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.engine/lib")
sys.path.append(engine_path)

from core import InternalGeminiService

# Instantiate the secret engine once
ai_service = InternalGeminiService()


@app.post("/translate", response_model=QueryResponse)
def translate_query(request: QueryRequest):
    try:
        # The AI core will now handle the natural language understanding and SQL generation 
        # using the provided schema metadata and query.
        sql = ai_service.generate_sql(request.query, request.metadata)

        if not sql:
            return QueryResponse(is_matching=False, sql_query=None)

        return QueryResponse(is_matching=True, sql_query=sql)

    except Exception:
        # Strictly returning 500 Internal Server Error without any detail
        # to hide API keys, token limits, or other internal processing logic.
        raise HTTPException(status_code=500, detail="Internal Server Error")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
