import json
from sqlalchemy import create_engine, inspect

class SchemaInspector:
    def __init__(self, db_url: str):
        """
        db_url can be:
        - sqlite:///path/to/db.sqlite
        - postgresql://user:pass@localhost/dbname
        """
        self.engine = create_engine(db_url)
        self.inspector = inspect(self.engine)

    def generate_metadata(self) -> dict:
        metadata = {"tables": {}}
        
        table_names = self.inspector.get_table_names()
        
        for table_name in table_names:
            columns = self.inspector.get_columns(table_name)
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            fk_constraints = self.inspector.get_foreign_keys(table_name)
            
            table_info = {
                "columns": [col['name'] for col in columns],
                "primary_key": pk_constraint.get('constrained_columns', [])[0] if pk_constraint.get('constrained_columns') else None,
                "foreign_keys": {}
            }
            
            for fk in fk_constraints:
                referred_table = fk['referred_table']
                for local_col, referred_col in zip(fk['constrained_columns'], fk['referred_columns']):
                    table_info["foreign_keys"][local_col] = f"{referred_table}.{referred_col}"
            
            metadata["tables"][table_name] = table_info
            
        return metadata

    def save_to_file(self, metadata: dict, output_path: str):
        with open(output_path, 'w') as f:
            json.dump(metadata, f, indent=2)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python schema_inspector.py <db_url> [output_path]")
        sys.exit(1)
    
    db_url = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "schema/metadata.json"
    
    inspector = SchemaInspector(db_url)
    metadata = inspector.generate_metadata()
    inspector.save_to_file(metadata, output_path)
    print(f"Successfully generated metadata for {db_url} -> {output_path}")
