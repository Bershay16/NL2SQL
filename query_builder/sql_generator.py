import networkx as nx
from sqlglot import parse_one

class SQLGenerator:
    """
    Schema-agnostic SQL generator.
    Accepts entities + intent dicts and produces a valid single-line SQL string.
    """
    AGG_FUNCS = {"count", "sum", "avg", "max", "min"}

    def __init__(self, metadata=None):
        # Schema metadata used for fallback column discovery
        self._metadata = metadata or {}
        self._table_columns = {
            t: set(info["columns"])
            for t, info in self._metadata.get("tables", {}).items()
        }
        self._date_cols = {}   # table → best date column
        self._label_cols = {}  # table → [name-like columns]
        date_hints  = ["date", "time", "created", "modified", "timestamp", "_at"]
        # Only include human-readable identifier columns in GROUP BY label set
        # city/email/region are excluded to avoid over-grouping
        label_hints = ["_name", "title", "label", "category", "description"]
        for tbl, cols in self._table_columns.items():
            for col in cols:
                cl = col.lower()
                if any(h in cl for h in date_hints) and tbl not in self._date_cols:
                    self._date_cols[tbl] = col
            self._label_cols[tbl] = [
                col for col in cols
                if any(h in col.lower() for h in label_hints)
            ]

    def generate(self, entities, intent, graph=None):
        tables     = list(entities.get("tables", []))
        columns    = list(entities.get("columns", []))
        filters    = list(entities.get("filters", []))
        comp_agg   = entities.get("composite_agg")

        agg_func   = (intent.get("aggregation") or "").upper()
        order_dir  = intent.get("order_by")
        limit_val  = intent.get("limit")
        having     = intent.get("having")
        temporal   = intent.get("temporal")
        left_join  = intent.get("left_join", False)

        # ------------------------------------------------------------------ #
        # 1. Infer tables from columns when none detected
        # ------------------------------------------------------------------ #
        if not tables and columns:
            for col in columns:
                for t in col.get("tables", []):
                    if t not in tables:
                        tables.append(t)

        if not tables:
            return "SELECT * FROM unknown_table;"

        # ------------------------------------------------------------------ #
        # 2. Expand tables via shortest-path JOIN resolution
        # ------------------------------------------------------------------ #
        aliases = {}
        join_clauses = []

        def get_alias(table):
            if table not in aliases:
                aliases[table] = f"t{len(aliases)+1}"
            return aliases[table]

        primary = tables[0]
        get_alias(primary)

        if len(tables) > 1 and graph:
            covered = {primary}
            # Expand to cover all requested tables via shortest path
            remaining = list(tables[1:])
            iterations = 0
            while remaining and iterations < 10:
                iterations += 1
                target = remaining.pop(0)
                if target in covered:
                    continue
                try:
                    path = nx.shortest_path(graph, primary, target)
                except Exception:
                    # try any covered node as bridge
                    path = None
                    for cv in covered:
                        try:
                            path = nx.shortest_path(graph, cv, target)
                            break
                        except Exception:
                            continue
                    if path is None:
                        path = [primary, target]

                for i in range(len(path) - 1):
                    u, v = path[i], path[i+1]
                    if v in covered:
                        continue
                    covered.add(v)
                    get_alias(v)
                    edge = (graph.get_edge_data(u, v) or {})
                    on_cond = edge.get("on", f"{u}.id = {v}.{u}_id")
                    on_alias = on_cond
                    for tbl, al in aliases.items():
                        on_alias = on_alias.replace(f"{tbl}.", f"{al}.")

                    join_type = "LEFT JOIN" if left_join else "JOIN"
                    join_clauses.append(
                        f"{join_type} {v} {aliases[v]} ON {on_alias}"
                    )

        # Make sure every table has an alias
        for t in tables:
            get_alias(t)

        # Resolve alias for a column (check which table it belongs to)
        def col_alias(col_name, col_tables):
            for t in tables:
                if t in (col_tables or []):
                    return f"{aliases[t]}.{col_name}"
            return f"{aliases[primary]}.{col_name}"

        # ------------------------------------------------------------------ #
        # 3. Build SELECT list
        # ------------------------------------------------------------------ #
        select_parts = []
        NUMERIC_HINTS = ["amount", "total", "price", "revenue", "cost", "salary", "balance",
                         "score", "rate", "value", "fee", "tax", "discount"]
        NAME_HINTS    = ["name", "title", "label", "description", "city", "region",
                         "category", "email"]

        def get_label_cols_for_table(tbl):
            """Name-like columns from a single table (for GROUP BY)."""
            return [
                f"{aliases[tbl]}.{col}"
                for col in self._label_cols.get(tbl, [])
            ]

        def auto_numeric_col(tbl):
            """Find a numeric-sounding column in a table from schema."""
            for col in self._table_columns.get(tbl, []):
                if any(h in col.lower() for h in NUMERIC_HINTS):
                    return col
            return None

        def auto_pk_col(tbl):
            """Find the primary key column of a table."""
            info = self._metadata.get("tables", {}).get(tbl, {})
            return info.get("primary_key")

        if agg_func:
            agg_target_ref = None
            # ---- Composite: SUM(qty * price) ----
            if comp_agg == "quantity_x_price":
                qty_alias   = col_alias("quantity", self.columns_for("quantity", columns))
                price_alias = col_alias("price",    self.columns_for("price",    columns))
                select_parts.append(f"SUM({qty_alias} * {price_alias})")

            # ---- COUNT: prefer PK of primary table ----
            elif agg_func == "COUNT":
                if columns:
                    target_col = self._pick_agg_column(columns, agg_func)
                    agg_target_ref = col_alias(target_col['column'], target_col['tables'])
                    select_parts.append(f"COUNT({agg_target_ref})")
                else:
                    pk = auto_pk_col(primary)
                    if pk:
                        select_parts.append(f"COUNT({aliases[primary]}.{pk})")
                    else:
                        select_parts.append("COUNT(*)")

            # ---- SUM/AVG/MAX/MIN: find numeric col from schema if not explicitly given ----
            else:
                if columns:
                    target_col = self._pick_agg_column(columns, agg_func)
                    agg_target_ref = col_alias(target_col['column'], target_col['tables'])
                    select_parts.append(f"{agg_func}({agg_target_ref})")
                else:
                    # Auto-search all joined tables for a numeric column
                    num_col, num_tbl = None, None
                    for tbl in tables:
                        nc = auto_numeric_col(tbl)
                        if nc:
                            num_col, num_tbl = nc, tbl
                            break
                    if num_col and num_tbl:
                        select_parts.append(f"{agg_func}({aliases[num_tbl]}.{num_col})")
                    else:
                        select_parts.append(f"{agg_func}(*)")

            # ---- First add explicitly matched non-ID columns ----
            explicit_group_cols = 0
            for col in columns:
                ref = col_alias(col["column"], col["tables"])
                if ref not in select_parts and ref != agg_target_ref:
                    select_parts.append(ref)
                    explicit_group_cols += 1

            # ---- If no explicit columns were added, use label columns from "grouping" table ----
            if explicit_group_cols == 0:
                group_tbl = tables[-1] if len(tables) > 1 else tables[0]
                for lc in get_label_cols_for_table(group_tbl):
                    if lc not in select_parts:
                        select_parts.append(lc)



        elif temporal:
            date_col = self._find_date_col_for_tables(tables)
            select_parts.append(f"EXTRACT({temporal.upper()} FROM {date_col})")
            select_parts.append("COUNT(*)")

        elif columns:
            seen = set()
            for col in columns:
                ref = col_alias(col["column"], col["tables"])
                if ref not in seen:
                    select_parts.append(ref)
                    seen.add(ref)
        else:
            select_parts.append(f"{aliases[primary]}.*")

        # ------------------------------------------------------------------ #
        # 4. FROM + JOIN
        # ------------------------------------------------------------------ #
        parts = [f"SELECT {', '.join(select_parts)}",
                 f"FROM {primary} {aliases[primary]}"]
        parts.extend(join_clauses)

        # ------------------------------------------------------------------ #
        # 5. WHERE (with LEFT JOIN NULL check for "never ordered" queries)
        # WHERE + LEFT JOIN IS NULL
        where_conds = []
        for f in filters:
            col_ref = col_alias(f["column"], [])
            val = f["value"]
            if not str(val).lstrip("-").replace(".", "").isnumeric():
                val = f"'{val}'"
            where_conds.append(f"{col_ref} {f['operator']} {val}")

        if left_join and len(tables) > 1:
            # The last joined table's primary key IS NULL ("never ordered")
            last_t = tables[-1]
            last_info = self._metadata.get("tables", {}).get(last_t, {})
            pk = last_info.get("primary_key")
            null_col = f"{aliases[last_t]}.{pk}" if pk else f"{aliases[last_t]}.*"
            where_conds.append(f"{null_col} IS NULL")

        if where_conds:
            parts.append(f"WHERE {' AND '.join(where_conds)}")

        # ------------------------------------------------------------------ #
        # 6. GROUP BY — all non-aggregated items in SELECT
        # ------------------------------------------------------------------ #
        group_by_cols = []
        if agg_func or temporal:
            for item in select_parts:
                if not any(fn in item.upper() for fn in ["SUM(", "COUNT(", "AVG(", "MAX(", "MIN(", "EXTRACT("]):
                    group_by_cols.append(item)
            if group_by_cols:
                parts.append(f"GROUP BY {', '.join(group_by_cols)}")
            elif temporal:
                parts.append(f"GROUP BY EXTRACT({temporal.upper()} FROM {self._find_date_col_for_tables(tables)})")
            elif having and agg_func:
                # HAVING requires GROUP BY — use label cols from the non-primary (grouping) table
                group_tbl = tables[-1] if len(tables) > 1 else tables[0]
                fallback_labels = [
                    f"{aliases[group_tbl]}.{col}"
                    for col in self._label_cols.get(group_tbl, [])
                ]
                if fallback_labels:
                    parts.append(f"GROUP BY {', '.join(fallback_labels)}")
                else:
                    # Last resort: group by primary key
                    pk = self._metadata.get("tables", {}).get(group_tbl, {}).get("primary_key")
                    if pk:
                        parts.append(f"GROUP BY {aliases[group_tbl]}.{pk}")

        # 7. HAVING
        if having and agg_func:
            having_expr = select_parts[0]
            parts.append(f"HAVING {having_expr} {having['op']} {having['value']}")


        # ------------------------------------------------------------------ #
        # 8. ORDER BY (always after GROUP/HAVING)
        # --------------------------------------------------------------        # ORDER BY
        if order_dir:
            if agg_func and select_parts:
                # Order by the aggregated expression (first item)
                order_col = select_parts[0]
            elif columns:
                oc = self._pick_order_column(columns)
                order_col = col_alias(oc["column"], oc["tables"])
            else:
                # Fallback to a numeric column, else primary key
                nc, nt = None, None
                for tbl in tables:
                    c = auto_numeric_col(tbl)
                    if c:
                        nc, nt = c, tbl
                        break
                if nc:
                    order_col = f"{aliases[nt]}.{nc}"
                else:
                    pk = auto_pk_col(primary)
                    order_col = f"{aliases[primary]}.{pk}" if pk else f"{aliases[primary]}.id"

            parts.append(f"ORDER BY {order_col} {order_dir}")

        # LIMIT
        if limit_val:
            parts.append(f"LIMIT {limit_val}")

        query_str = " ".join(parts)
        try:
            return parse_one(query_str).sql(pretty=False) + ";"
        except Exception as e:
            return query_str + ";  -- parse error: " + str(e)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def columns_for(self, name, columns):
        for c in columns:
            if c["column"] == name:
                return c["tables"]
        return []

    def _pick_agg_column(self, columns, agg_func):
        numeric_hints = ["amount", "price", "total", "revenue", "salary", "cost",
                         "quantity", "count", "balance", "score", "rate", "value"]
        if agg_func in ("SUM", "AVG", "MAX", "MIN"):
            for col in columns:
                if any(h in col["column"].lower() for h in numeric_hints):
                    return col
        return columns[0]

    def _pick_order_column(self, columns):
        numeric_hints = ["amount", "price", "total", "revenue", "date", "quantity",
                         "count", "score", "rate", "value"]
        for col in columns:
            if any(h in col["column"].lower() for h in numeric_hints):
                return col
        return columns[0]

    def _find_date_col_for_tables(self, tables):
        """Find the best date column from the schema for the given tables."""
        date_hints = ["date", "time", "created", "modified", "timestamp", "_at"]
        for tbl in tables:
            for col in self._table_columns.get(tbl, []):
                if any(h in col.lower() for h in date_hints):
                    al = self._get_alias_for(tbl)
                    return f"{al}.{col}" if al else col
        return "created_at"

    def _get_alias_for(self, tbl):
        """Return the alias for a table if it was registered during generate()."""
        # Aliases are local to generate(); this is a best-effort hint.
        return None

    def _find_date_column(self, columns, tables):
        """Legacy: find date col from matched columns."""
        date_hints = ["date", "time", "created", "modified", "timestamp", "at"]
        for col in columns:
            if any(h in col["column"].lower() for h in date_hints):
                return col["column"]
        return self._find_date_col_for_tables(tables)
