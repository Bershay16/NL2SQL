import os
import json
from nlp.parser import NLParser
from nlp.entity_extractor import EntityExtractor, IntentClassifier
from nlp.intelligence import DatabaseLinguist
from query_builder.sql_generator import SQLGenerator
from schema.inspector import SchemaInspector

from dotenv import load_dotenv
load_dotenv()


def setup_metadata(metadata_path):
    if os.path.exists(metadata_path):
        choice = input(
            "Metadata file already exists. (U)se existing or (G)enerate new? [U/G]: "
        ).strip().upper()
        if choice == "U":
            with open(metadata_path, 'r') as f:
                md = json.load(f)
                return md.get("db_url", os.getenv("DATABASE_BASE_URL", "sqlite:///sample.db"))
        else:
            # Explicitly clear old data as requested
            print(f"Clearing old metadata from {metadata_path}...")
            if os.path.exists(metadata_path):
                os.remove(metadata_path)

    db_name = input("Enter Database Name: ").strip()
    if not db_name:
        print("No database name provided.")
        return None

    base_url = os.getenv(
        "DATABASE_BASE_URL",
        "sqlite:///sample.db", # Fallback for local testing
    )
    # Check for postgres-style URL vs SQLite
    if base_url.startswith("sqlite"):
        db_url = base_url
    else:
        db_url = f"{base_url.rstrip('/')}/{db_name}"

    try:
        print(f"Inspecting database: {db_url}...")
        inspector = SchemaInspector(db_url)
        metadata = inspector.generate_metadata()
        inspector.save_to_file(metadata, metadata_path)
        print("Metadata generated successfully.")
        return db_url
    except Exception as e:
        print(f"Error generating metadata: {e}")
        return None


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    metadata_path = os.path.join(base_dir, "schema", "metadata.json")

    db_url = setup_metadata(metadata_path)
    if not db_url:
        print("Failed to initialize database connection. Exiting.")
        return

    # Load metadata
    with open(metadata_path) as f:
        metadata = json.load(f)

    # Initialize components
    print("\nInitializing NL2SQL components...")
    parser     = NLParser()
    linguist   = DatabaseLinguist(db_url)
    extractor  = EntityExtractor(metadata, linguist=linguist)
    classifier = IntentClassifier()
    generator  = SQLGenerator(metadata=metadata)

    # Display database reflection
    print(linguist.get_reflection_report())

    print("\nNL2SQL System Ready!")
    print("Type 'exit' or 'quit' to stop.\n")

    while True:
        try:
            query_text = input("User Question: ").strip()

            if not query_text:
                continue
            if query_text.lower() in ("exit", "quit"):
                break

            # Pipeline
            analysis = parser.get_analysis(query_text)
            entities = extractor.extract(analysis, query_text)
            intent   = classifier.classify(query_text, analysis, entities=entities)
            sql      = generator.generate(entities, intent)

            print("\nGenerated SQL:")
            print("-" * 40)
            print(sql)
            print("-" * 40 + "\n")

        except (KeyboardInterrupt, EOFError):
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
