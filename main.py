import os
import sys
from nlp.parser import NLParser
from nlp.entity_extractor import EntityExtractor, IntentClassifier
from query_builder.sql_generator import SQLGenerator
from schema.inspector import SchemaInspector

from dotenv import load_dotenv
load_dotenv()

def setup_metadata(metadata_path):
    if os.path.exists(metadata_path):
        choice = input("Metadata file already exists. (U)se existing or (G)enerate new? [U/G]: ").strip().upper()
        if choice == 'U':
            return True

    db_name = input("Enter Database Name: ").strip()
    if not db_name:
        print("No database name provided.")
        return False
    
    # Base URL from .env or default
    base_url = os.getenv("DATABASE_BASE_URL", "postgresql+psycopg://postgres:7844@localhost:5433/")
    db_url = f"{base_url.rstrip('/')}/{db_name}"
        
    try:
        print(f"Inspecting database: {db_url}...")
        inspector = SchemaInspector(db_url)
        metadata = inspector.generate_metadata()
        inspector.save_to_file(metadata, metadata_path)
        print("Metadata generated successfully.")
        return True
    except Exception as e:
        print(f"Error generating metadata: {e}")
        return False

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    metadata_path = os.path.join(base_dir, "schema", "metadata.json")
    
    if not setup_metadata(metadata_path):
        print("Failed to initialize metadata. Exiting.")
        return

    # Initialize components
    print("\nInitializing NL2SQL components...")
    import json
    with open(metadata_path) as f:
        metadata = json.load(f)

    parser    = NLParser()
    extractor = EntityExtractor(metadata_path)
    classifier = IntentClassifier()
    generator  = SQLGenerator(metadata=metadata)
    
    print("\nNL2SQL System Ready!")
    print("Type 'exit' or 'quit' to stop.\n")
    
    while True:
        try:
            line = input("User Question: ")
            query_text = line.strip()
            
            if not query_text:
                continue
                
            if query_text.lower() in ["exit", "quit"]:
                break
                
            # Pipeline
            analysis = parser.get_analysis(query_text)
            entities = extractor.extract(analysis, query_text)
            intent   = classifier.classify(query_text, analysis)
            sql      = generator.generate(entities, intent, graph=extractor.graph)
            
            print("\nGenerated SQL:")
            print("-" * 20)
            print(sql)
            print("-" * 20 + "\n")
            
        except (KeyboardInterrupt, EOFError):
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
