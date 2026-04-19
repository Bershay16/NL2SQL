"""
Single-table SQL Generator.

Takes entities + intent dicts and produces a valid SQL string.
No JOIN logic — operates on exactly one table.
Uses metadata data_types to intelligently pick aggregation targets
and GROUP BY columns.
"""

import re
from sqlglot import parse_one


class SQLGenerator:
    """
    Schema-aware, single-table SQL generator.
    """

    def __init__(self, metadata: dict | None = None):
        self._metadata = metadata or {}
        self._tables = self._metadata.get("tables", {})

        # Pre-compute column type info per table
        self._col_types: dict[str, dict[str, str]] = {}   # table → {col: data_type}
        self._numeric_cols: dict[str, list[str]] = {}      # table → [numeric cols]
        self._text_cols: dict[str, list[str]] = {}          # table → [text cols]
        self._date_cols: dict[str, list[str]] = {}          # table → [date cols]
        self._label_cols: dict[str, list[str]] = {}         # table → [name-like cols]
        self._pk_col: dict[str, str | None] = {}            # table → pk

        for tbl, info in self._tables.items():
            cols_raw = info.get("columns", {})
            # Handle old list format
            if isinstance(cols_raw, list):
                cols_raw = {c: {"data_type": "text"} for c in cols_raw}

            types = {}
            numeric, text, date, label = [], [], [], []
            pk = None

            for col, meta in cols_raw.items():
                dt = meta.get("data_type", "text")
                types[col] = dt

                if meta.get("is_primary_key"):
                    pk = col

                if dt in ("numeric", "integer", "float", "real", "money"):
                    if not meta.get("is_primary_key"):
                        numeric.append(col)
                elif dt in ("date", "timestamp", "timestamptz"):
                    date.append(col)
                else:
                    text.append(col)
                    # "name"-like columns are good for GROUP BY display
                    name_hints = ("name", "title", "label", "category",
                                  "description", "department", "type")
                    if any(h in col.lower() for h in name_hints):
                        label.append(col)

            self._col_types[tbl] = types
            self._numeric_cols[tbl] = numeric
            self._text_cols[tbl] = text
            self._date_cols[tbl] = date
            self._label_cols[tbl] = label
            self._pk_col[tbl] = pk

    # ------------------------------------------------------------------ #
    def generate(self, entities: dict, intent: dict, **_kwargs) -> str:
        table   = entities.get("table")
        columns = list(entities.get("columns", []))
        filters = list(entities.get("filters", []))

        agg      = (intent.get("aggregation") or "").upper()
        order    = intent.get("order_by")           # "ASC" | "DESC" | None
        limit    = intent.get("limit")              # int | None
        having   = intent.get("having")             # {"op": ">", "value": "5"} | None
        temporal = intent.get("temporal")            # "year" | "month" | None
        group_hint = intent.get("group_by_hint", False)

        if not table:
            return "-- ERROR: no table could be resolved from the query"

        col_types = self._col_types.get(table, {})
        pk = self._pk_col.get(table)

        # ---------------------------------------------------------------- #
        # 1. SELECT
        # ---------------------------------------------------------------- #
        select_parts: list[str] = []
        group_cols: list[str] = []
        agg_expr: str | None = None

        select_all = entities.get("select_all", False)
        
        if temporal:
            # "per year" / "per month"
            date_col = self._pick_date_col(table)
            select_parts.append(f"EXTRACT({temporal.upper()} FROM {date_col})")
            select_parts.append("COUNT(*)")
            group_cols.append(select_parts[0])
            agg_expr = "COUNT(*)"

        elif agg:
            agg_target = self._pick_agg_target(agg, columns, table)
            agg_expr = f"{agg}({agg_target})"
            select_parts.append(agg_expr)

            # Add grouping columns
            group_cols = self._pick_group_cols(
                agg, columns, filters, table, group_hint,
            )
            select_parts.extend(group_cols)

        elif columns and not select_all:
            # Explicit columns resolve
            for c in columns:
                col = c["column"]
                if col not in select_parts:
                    select_parts.append(col)
            
            # PROACTIVE: If we only have numeric/technical columns and it's a ranking 
            # or sorted query, add identifying labels (first_name, etc.)
            if (order or limit) and select_parts != ["*"]:
                # Check if we have any identifying columns
                has_label = any(s in self._label_cols.get(table, []) for s in select_parts)
                has_pk = pk in select_parts
                if not (has_label or has_pk):
                    # Only add label if we don't already have one and it's not a generic *
                    labels = self._label_cols.get(table, [])
                    if labels and labels[0] not in select_parts:
                        select_parts.insert(0, labels[0])
                    elif pk and pk not in select_parts:
                        select_parts.insert(0, pk)

        else:
            select_parts.append("*")

        # ---------------------------------------------------------------- #
        # 2. FROM
        # ---------------------------------------------------------------- #
        from_clause = table

        # ---------------------------------------------------------------- #
        # 3. WHERE
        # ---------------------------------------------------------------- #
        where_conds: list[str] = []
        for f in filters:
            col = f["column"]
            op  = f["operator"]
            val = f["value"]
            col_type = col_types.get(col, "text")

            # Normalize dates: "January 1, 2022" -> "2022-01-01"
            if col_type in ("date", "timestamp"):
                val = self._normalize_date_value(str(val))

            # Handle year filters for date/timestamp columns:
            # "hired > 2020" -> EXTRACT(YEAR FROM hire_date) > 2020
            is_year_val = (
                str(val).isdigit() 
                and len(str(val)) == 4 
                and 1900 <= int(val) <= 2100
            )
            if col_type in ("date", "timestamp") and is_year_val:
                where_conds.append(f"EXTRACT(YEAR FROM {col}) {op} {val}")
                continue

            # Quote non-numeric values
            if op not in ("IS NULL", "IS NOT NULL"):
                if not str(val).lstrip("-").replace(".", "").replace(",", "").isdigit():
                    val = f"'{val}'"
                where_conds.append(f"{col} {op} {val}")
            else:
                where_conds.append(f"{col} {op}")

        # ---------------------------------------------------------------- #
        # 4. GROUP BY
        # ---------------------------------------------------------------- #
        # (group_cols already built above)

        # ---------------------------------------------------------------- #
        # 5. HAVING
        # ---------------------------------------------------------------- #
        having_clause: str | None = None
        if having and agg_expr:
            having_clause = f"{agg_expr} {having['op']} {having['value']}"

        # ---------------------------------------------------------------- #
        # 6. ORDER BY
        # ---------------------------------------------------------------- #
        order_clause: str | None = None
        if order:
            if agg_expr:
                order_clause = f"{agg_expr} {order}"
            elif columns:
                # Prefer numeric columns for ordering
                order_col = self._pick_order_col(columns, table)
                order_clause = f"{order_col} {order}"
            else:
                # Fallback: first numeric column or PK
                nc = self._numeric_cols.get(table, [])
                order_col = nc[0] if nc else (pk or select_parts[0])
                order_clause = f"{order_col} {order}"

        # ---------------------------------------------------------------- #
        # 7. Assemble
        # ---------------------------------------------------------------- #
        distinct_str = "DISTINCT " if intent.get("distinct") else ""
        parts = [f"SELECT {distinct_str}{', '.join(select_parts)}"]
        parts.append(f"FROM {from_clause}")

        if where_conds:
            parts.append(f"WHERE {' AND '.join(where_conds)}")
        if group_cols:
            parts.append(f"GROUP BY {', '.join(group_cols)}")
        if having_clause:
            parts.append(f"HAVING {having_clause}")
        if order_clause:
            parts.append(f"ORDER BY {order_clause}")
        if limit:
            parts.append(f"LIMIT {limit}")

        raw = " ".join(parts)

        # Validate / pretty-print via sqlglot
        try:
            return parse_one(raw).sql(pretty=False) + ";"
        except Exception:
            return raw + ";"

    # ------------------------------------------------------------------ #
    #  Private helpers
    # ------------------------------------------------------------------ #
    def _pick_agg_target(
        self, agg: str, columns: list[dict], table: str,
    ) -> str:
        """Choose the right column to aggregate."""
        numeric = self._numeric_cols.get(table, [])

        if agg == "COUNT":
            pk = self._pk_col.get(table)
            return pk if pk else "*"

        # SUM / AVG / MAX / MIN → need a numeric column
        # 1. From explicitly matched columns
        for c in columns:
            col = c["column"]
            dt = self._col_types.get(table, {}).get(col, "text")
            if dt in ("numeric", "integer", "float", "real", "money"):
                return col

        # 2. First numeric column in schema
        if numeric:
            return numeric[0]

        # 3. Absolute fallback
        return "*"

    def _normalize_date_value(self, val: str) -> str:
        """Convert natural dates like 'January 1, 2022' to '2022-01-01'."""
        month_map = {
            "january": "01", "jan": "01",
            "february": "02", "feb": "02",
            "march": "03", "mar": "03",
            "april": "04", "apr": "04",
            "may": "05",
            "june": "06", "jun": "06",
            "july": "07", "jul": "07",
            "august": "08", "aug": "08",
            "september": "09", "sep": "09",
            "october": "10", "oct": "10",
            "november": "11", "nov": "11",
            "december": "12", "dec": "12"
        }
        
        # Regex for: Month Name (optional comma/space) Day (optional comma/space) Year
        # Handles "January 1, 2022" or "jan 1 2022"
        match = re.search(r"([a-z]{3,})\s+(\d{1,2}),?\s+(\d{4})", val.lower())
        if match:
            m_name, day, year = match.groups()
            m_num = None
            # Check full names and abbreviations
            for name, num in month_map.items():
                if m_name.startswith(name):
                    m_num = num
                    break
            if m_num:
                return f"{year}-{m_num}-{day.zfill(2)}"
        
        return val

    def _pick_group_cols(
        self, agg: str, columns: list[dict], filters: list[dict],
        table: str, group_hint: bool,
    ) -> list[str]:
        """Determine GROUP BY columns for an aggregation query."""
        filter_col_set = {f["column"] for f in filters}
        numeric_set = set(self._numeric_cols.get(table, []))

        # Explicitly mentioned non-numeric, non-filter columns → GROUP BY
        explicit: list[str] = []
        for c in columns:
            col = c["column"]
            if col not in numeric_set and col not in filter_col_set:
                pk = self._pk_col.get(table)
                if col != pk:
                    explicit.append(col)

        if explicit:
            return explicit

        # If "per department", "each city" → use label/text columns from nouns
        if group_hint:
            labels = self._label_cols.get(table, [])
            if labels:
                return [labels[0]]

        return []

    def _pick_order_col(self, columns: list[dict], table: str) -> str:
        """Pick the best column for ORDER BY (prefer numeric)."""
        for c in columns:
            col = c["column"]
            if col == "*":
                continue
            dt = self._col_types.get(table, {}).get(col, "text")
            if dt in ("numeric", "integer", "float", "real", "money"):
                return col
        
        # If we only have *, pick first numeric or PK
        if columns and columns[0]["column"] == "*":
            nc = self._numeric_cols.get(table, [])
            if nc: return nc[0]
            pk = self._pk_col.get(table)
            if pk: return pk

        return columns[0]["column"] if columns else "*"

    def _pick_date_col(self, table: str) -> str:
        """Pick the best date/timestamp column."""
        dc = self._date_cols.get(table, [])
        return dc[0] if dc else "created_at"
