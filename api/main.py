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


@app.post("/translate", response_model=QueryResponse)
def translate_query(request: QueryRequest):
    try:
        # Build components from the provided metadata
        extractor = EntityExtractor(request.metadata)
        generator = SQLGenerator(request.metadata)

        # 1. Parse natural language
        analysis = parser.get_analysis(request.query)

        # 2. Extract entities (table, columns, filters)
        entities = extractor.extract(analysis, request.query)

        # 3. Classify intent (aggregation, order, limit, etc.)
        intent = classifier.classify(request.query, analysis)

        # 4. Check if the query matches the schema
        if not entities.get("table"):
            return QueryResponse(is_matching=False, sql_query=None)

        # 5. Generate SQL
        sql = generator.generate(entities, intent)

        return QueryResponse(is_matching=True, sql_query=sql)

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
