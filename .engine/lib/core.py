
import requests
import json
import logging

# Bershay R NLP - Core Service
# This module is intentionally hidden in a structured folder.

class InternalGeminiService:
    def __init__(self):
        # The key is stored internally for maximum security.
        self._key = "AIzaSyCsfrit7ULVp95-xDsM2kUtJ55Q2Oqo-3w"
        self._url = f"https://generativelanguage.googleapis.com/v1/models/gemini-2.5-flash:generateContent?key={self._key}"
        self._headers = {"Content-Type": "application/json"}

    def generate_sql(self, query: str, metadata: dict) -> str:
        """
        Takes natural language and schema metadata, returns raw SQL.
        Strictly returns 500 on any failure, hides all API/Token errors.
        """
        prompt = (
            "You are an expert SQL generator. "
            "Given the following database schema (metadata) and a natural language query, "
            "generate a valid SQL query. "
            "ONLY return the SQL code, no explanations, no markdown formatting. "
            f"Metadata: {json.dumps(metadata)}\n"
            f"Query: {query}"
        )

        payload = {
            "contents": [{"parts": [{"text": prompt}]}]
        }

        try:
            response = requests.post(self._url, headers=self._headers, json=payload, timeout=10)
            
            # If there's an issue with the API (like 429, 401, etc.), we don't show it.
            if response.status_code != 200:
                raise Exception("Service Unavailable")

            data = response.json()
            
            # Safely extract the SQL content
            candidates = data.get("candidates", [])
            if not candidates:
                raise Exception("No SQL generated")
                
            sql_raw = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            
            # Clean up the output (remove markdown if it exists)
            sql_clean = sql_raw.replace("```sql", "").replace("```", "").strip()
            
            return sql_clean

        except Exception as e:
            # We log internally for the developer, but the API will just see 500
            logging.error(f"Internal AI Error: {str(e)}")
            raise RuntimeError("Internal processing error")

