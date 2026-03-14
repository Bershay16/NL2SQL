from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any

from nlp.parser import NLParser
from nlp.entity_extractor import EntityExtractor, IntentClassifier
from query_builder.sql_generator import SQLGenerator

app = FastAPI(title="NL2SQL API")

# We can keep one parser and classifier instance since they are heavy/stateless
parser = NLParser()
classifier = IntentClassifier()

class QueryRequest(BaseModel):
    query: str
    metadata: Dict[str, Any]  # The 'describe table' / schema metadata object passed statically 

class QueryResponse(BaseModel):
    is_matching: bool
    sql_query: Optional[str]

@app.get("/")
def read_root():
    return {"message": "Welcome to NL2SQL API"}

@app.post("/translate", response_model=QueryResponse)
def translate_query(request: QueryRequest):
    try:
        # Initialize the schema-dependent components dynamically using the received metadata
        extractor = EntityExtractor(request.metadata)
        generator = SQLGenerator(request.metadata)
        
        # Step 1: Parse natural language text
        analysis = parser.get_analysis(request.query)
        
        # Step 2: Extract entities (tables, columns, filters) using the provided database metadata
        entities = extractor.extract(analysis, request.query)
        
        # Step 3: Classify the user intent (aggregation, order, limit, having, etc.)
        intent = classifier.classify(request.query, analysis)
        
        # Step 4: Verify if there is a match with the database
        # If no relevant tables were found from the query, it does not match the provided schema.
        if not entities.get("tables"):
            return QueryResponse(is_matching=False, sql_query=None)
            
        # Step 5: Generate the SQL Query
        sql = generator.generate(entities, intent, extractor.graph)
        
        return QueryResponse(is_matching=True, sql_query=sql)
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
