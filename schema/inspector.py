import json
from sqlalchemy import create_engine, inspect, text


class SchemaInspector:
    """
    Introspects a database and generates rich metadata including:
    - Column descriptions (inferred from name)
    - Data types (mapped to generic: text, numeric, date, timestamp, integer, boolean)
    - Sample values (first N rows)
    - Distinct values (for low-cardinality text columns)
    """

    # Map SQL types to generic categories
    TYPE_MAP = {
        "integer": "integer", "int": "integer", "bigint": "integer",
        "smallint": "integer", "serial": "integer", "bigserial": "integer",
        "numeric": "numeric", "decimal": "numeric", "real": "numeric",
        "double": "numeric", "float": "numeric", "money": "numeric",
        "text": "text", "varchar": "text", "character varying": "text",
        "char": "text", "character": "text", "citext": "text", "name": "text",
        "date": "date",
        "timestamp": "timestamp", "timestamptz": "timestamp",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamp",
        "boolean": "boolean", "bool": "boolean",
    }

    # Max distinct values to store (only for categorical columns)
    MAX_DISTINCT = 50
    # Max sample rows to fetch
    MAX_SAMPLES = 5

    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.inspector = inspect(self.engine)

    def _normalize_type(self, raw_type: str) -> str:
        """Convert a raw SQL type string to a generic category."""
        raw = str(raw_type).lower().split("(")[0].strip()
        return self.TYPE_MAP.get(raw, "text")

    def _infer_description(self, col_name: str, table_name: str) -> str:
        """Generate a basic human-readable description from the column name."""
        readable = col_name.replace("_", " ").title()
        return f"{readable} of {table_name}"

    def generate_metadata(self) -> dict:
        metadata = {"tables": {}}
        table_names = self.inspector.get_table_names()

        for table_name in table_names:
            raw_columns = self.inspector.get_columns(table_name)
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            pk_cols = set(pk_constraint.get("constrained_columns", []))
            fk_constraints = self.inspector.get_foreign_keys(table_name)

            # Build foreign keys map
            foreign_keys = {}
            for fk in fk_constraints:
                referred_table = fk["referred_table"]
                for local_col, referred_col in zip(
                    fk["constrained_columns"], fk["referred_columns"]
                ):
                    foreign_keys[local_col] = f"{referred_table}.{referred_col}"

            # Fetch sample rows and distinct values from the actual database
            sample_data = {}
            distinct_data = {}
            try:
                with self.engine.connect() as conn:
                    col_names = [c["name"] for c in raw_columns]
                    cols_sql = ", ".join(f'"{c}"' for c in col_names)

                    # Sample values
                    result = conn.execute(
                        text(f'SELECT {cols_sql} FROM "{table_name}" LIMIT {self.MAX_SAMPLES}')
                    )
                    rows = result.fetchall()
                    for i, col in enumerate(col_names):
                        vals = [row[i] for row in rows if row[i] is not None]
                        sample_data[col] = [self._serialize(v) for v in vals]

                    # Distinct values for low-cardinality text columns
                    for col_info in raw_columns:
                        col_name = col_info["name"]
                        generic_type = self._normalize_type(str(col_info["type"]))
                        if generic_type == "text" and col_name not in pk_cols:
                            count_result = conn.execute(
                                text(f'SELECT COUNT(DISTINCT "{col_name}") FROM "{table_name}"')
                            )
                            count = count_result.scalar()
                            if count and count <= self.MAX_DISTINCT:
                                dist_result = conn.execute(
                                    text(
                                        f'SELECT DISTINCT "{col_name}" FROM "{table_name}" '
                                        f'WHERE "{col_name}" IS NOT NULL '
                                        f"ORDER BY \"{col_name}\" LIMIT {self.MAX_DISTINCT}"
                                    )
                                )
                                distinct_data[col_name] = [
                                    str(r[0]) for r in dist_result.fetchall()
                                ]
            except Exception:
                pass  # Gracefully degrade if DB is unavailable

            # Build column metadata
            columns_meta = {}
            for col_info in raw_columns:
                col_name = col_info["name"]
                generic_type = self._normalize_type(str(col_info["type"]))
                entry = {
                    "data_type": generic_type,
                    "description": self._infer_description(col_name, table_name),
                }
                if col_name in pk_cols:
                    entry["is_primary_key"] = True
                if col_name in sample_data and sample_data[col_name]:
                    entry["sample_values"] = sample_data[col_name]
                if col_name in distinct_data:
                    entry["distinct_values"] = distinct_data[col_name]

                columns_meta[col_name] = entry

            table_entry = {
                "description": f"Table: {table_name}",
                "columns": columns_meta,
            }
            if foreign_keys:
                table_entry["foreign_keys"] = foreign_keys

            metadata["tables"][table_name] = table_entry

        return metadata

    def save_to_file(self, metadata: dict, output_path: str):
        with open(output_path, "w") as f:
            json.dump(metadata, f, indent=2)

    @staticmethod
    def _serialize(val):
        """Convert a value to a JSON-safe type."""
        if isinstance(val, (int, float, bool, str)):
            return val
        return str(val)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python inspector.py <db_url> [output_path]")
        sys.exit(1)

    db_url = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "schema/metadata.json"

    inspector = SchemaInspector(db_url)
    metadata = inspector.generate_metadata()
    inspector.save_to_file(metadata, output_path)
    print(f"Successfully generated metadata for {db_url} -> {output_path}")
