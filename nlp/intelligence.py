"""
Linguistic Intelligence Layer.
Provides the NLP components with direct access to the database schema
and content to resolve ambiguity at runtime.
"""

from sqlalchemy import text, inspect
from schema.inspector import SchemaInspector

class DatabaseLinguist:
    def __init__(self, db_url: str):
        self.inspector = SchemaInspector(db_url)
        self.engine = self.inspector.engine
        self._schema_cache = None

    def get_context_summary(self, table_name: str = None) -> str:
        """
        Generates a text summary of the table schema and samples.
        This string is intended to be the 'input' or 'context' for the NLP logic.
        """
        metadata = self.inspector.generate_metadata()
        tables = metadata.get("tables", {})
        
        if table_name and table_name in tables:
            target_tables = {table_name: tables[table_name]}
        else:
            target_tables = tables

        summary = []
        for name, info in target_tables.items():
            summary.append(f"Table: {name}")
            summary.append(f"Description: {info.get('description', '')}")
            summary.append("Columns:")
            for col, meta in info.get("columns", {}).items():
                pk_str = " (PK)" if meta.get("is_primary_key") else ""
                samples = ", ".join(map(str, meta.get("sample_values", [])))
                summary.append(f" - {col} [{meta['data_type']}]{pk_str}: Samples: {samples}")
            summary.append("")
        
        return "\n".join(summary)

    def get_reflection_report(self) -> str:
        """
        Produce a user-friendly summary of the database 'discovery' phase.
        """
        metadata = self.inspector.generate_metadata()
        tables = metadata.get("tables", {})
        
        report = ["\n--- Database Reflection ---"]
        for name, info in tables.items():
            report.append(f"Table '{name}':")
            for col, meta in info.get("columns", {}).items():
                samples = meta.get("sample_values", [])
                distinct = meta.get("distinct_values", [])
                # Convert to strings for consistent sorting across mixed types
                all_vals = sorted(list(set(map(str, samples + distinct))))[:5]
                
                val_str = f" (e.g., {', '.join(map(str, all_vals))})" if all_vals else ""
                report.append(f"  • {col} [{meta['data_type']}]{val_str}")
        report.append("---------------------------\n")
        return "\n".join(report)

    def search_value_dynamically(self, value: str, table_name: str) -> list[str]:
        """
        Actually query the DB to see if a value exists in any column
        if it wasn't found in the pre-cached metadata.
        """
        found_in_cols = []
        # get columns for table
        insp = inspect(self.engine)
        cols = [c['name'] for c in insp.get_columns(table_name)]
        
        with self.engine.connect() as conn:
            for col in cols:
                try:
                    # Case-insensitive search for the value
                    res = conn.execute(text(f'SELECT 1 FROM "{table_name}" WHERE CAST("{col}" AS TEXT) ILIKE :val LIMIT 1'), {"val": f"%{value}%"})
                    if res.fetchone():
                        found_in_cols.append(col)
                except:
                    continue
        return found_in_cols
