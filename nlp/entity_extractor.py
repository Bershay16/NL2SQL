import re
import networkx as nx
from rapidfuzz import process, fuzz
import json

class IntentClassifier:

    # Map NL patterns → aggregation function
    # Priority: first match wins — order matters!
    AGG_PATTERNS = [
        # COUNT
        ("count", [
            "total number of", "number of", "how many", "count of",
            "count the", "total count"
        ]),
        # SUM
        ("sum", [
            "total revenue", "total sales", "total amount", "total spent",
            "total quantity", "total products sold", "sum of", "combined"
        ]),
        # AVG
        ("avg", ["average", "avg", "mean", "average number"]),
        # MAX
        ("max", ["highest", "maximum", "max ", "most expensive", "largest"]),
        # MIN
        ("min", ["lowest", "minimum", "min ", "cheapest", "smallest"]),
    ]

    ORDER_PATTERNS = [
        ("DESC", ["highest", "most", "largest", "top", "desc", "descending"]),
        ("ASC",  ["lowest", "least", "smallest", "cheapest", "asc", "ascending", "alphabetically"]),
    ]

    def classify(self, text, analysis=None):
        t = text.lower()

        intent = {
            "aggregation": None,
            "order_by":    None,
            "limit":       None,
            "having":      None,
            "temporal":    None,
            "left_join":   False,
        }

        # ---- LIMIT -------------------------------------------------------
        m = re.search(r"\btop\s+(\d+)\b|\blimit\s+(\d+)\b|\bfirst\s+(\d+)\b", t)
        if m:
            intent["limit"] = int(m.group(1) or m.group(2) or m.group(3))

        # ---- TEMPORAL ----------------------------------------------------
        if analysis:
            intent["temporal"] = analysis.get("temporal")

        # ---- HAVING hint -------------------------------------------------
        if analysis and analysis.get("having_hint"):
            intent["having"] = analysis["having_hint"]

        # ---- LEFT JOIN hint ----------------------------------------------
        if any(p in t for p in ["never", "no order", "not ordered", "without order", "have no order",
                                  "products that have never", "never been ordered"]):
            intent["left_join"] = True

        # ---- AGGREGATION (explicit keyword match, priority order) --------
        SUM_VERBS     = ["spent", "spend", "earned", "generated", "sold for"]
        COUNT_VERBS   = ["handled", "placed", "processed", "submitted"]
        COUNT_PHRASES = ["total number of", "number of", "how many", "count of", "count the", "total count"]
        SUM_PHRASES   = ["total revenue", "total sales", "total amount", "total spent",
                         "total quantity", "total products sold", "sum of", "combined"]
        AVG_PHRASES   = ["average", "avg", "mean"]
        # "highest price" → aggregation. "order with the highest" → ORDER BY
        # Rule: MAX only if directly followed by a metric noun (not "the X" subject)
        MAX_DIRECT    = re.search(r"\b(highest|maximum|largest|most expensive)\s+(?:total\s+)?(price|amount|revenue|salary|score|value|cost)\b", t)
        MIN_DIRECT    = re.search(r"\b(lowest|minimum|cheapest|smallest)\s+(?:total\s+)?(price|amount|revenue|salary|score|value|cost)\b", t)

        if any(p in t for p in COUNT_PHRASES):
            intent["aggregation"] = "count"
        elif MAX_DIRECT and "most" not in t:
            intent["aggregation"] = "max"
        elif MIN_DIRECT:
            intent["aggregation"] = "min"
        elif any(p in t for p in SUM_PHRASES) or any(v in t for v in SUM_VERBS):
            intent["aggregation"] = "sum"
        elif any(p in t for p in AVG_PHRASES):
            intent["aggregation"] = "avg"

        # Special case: 'order with highest' means ORDER BY ... LIMIT 1, not aggregation
        if any(w in t for w in ["order with", "customer with", "product with", "rep with"]):
            intent["aggregation"] = None
            intent["limit"] = 1
            if any(w in t for w in ["highest", "most", "largest", "maximum"]):
                intent["order_by"] = "DESC"
            elif any(w in t for w in ["lowest", "cheapest", "smallest", "minimum"]):
                intent["order_by"] = "ASC"

        # ---- INFER aggregation from HAVING (e.g. "placed more than 5 orders") ------
        # If HAVING is detected but no explicit aggregation found, infer COUNT
        if intent["having"] and not intent["aggregation"]:
            # Only infer if there's an action verb suggesting aggregation
            if any(v in t for v in COUNT_VERBS + SUM_VERBS + ["with more", "with less", "with fewer"]):
                val = int(intent["having"]["value"])
                if val > 1000:  # large values more likely to be SUM
                    intent["aggregation"] = "sum"
                else:
                    intent["aggregation"] = "count"
            else:
                # E.g. "price greater than 1000" -> treat as a regular filter, not HAVING
                intent["having"] = None


        # ---- INFER aggregation from "most [noun]" + LIMIT or ORDER BY ----
        # "most orders" → COUNT. "most revenue/sales/money" → SUM
        if not intent["aggregation"]:
            most_match = re.search(r"\bmost\s+(\w+)", t)
            if most_match:
                noun = most_match.group(1)
                sum_nouns  = ["money", "revenue", "sales", "amount", "value", "cost"]
                count_nouns = ["orders", "order", "items", "products", "customers", "purchases"]
                if noun in sum_nouns:
                    intent["aggregation"] = "sum"
                elif noun in count_nouns:
                    intent["aggregation"] = "count"

        # ---- ORDER BY (always set if LIMIT is present) -------------------
        if not intent["order_by"]:
            ORDER_DESC = ["highest", "most", "largest", "top", "desc", "descending",
                          "best", "most expensive"]
            ORDER_ASC  = ["lowest", "least", "smallest", "cheapest", "asc", "ascending",
                          "alphabetically"]
            if any(p in t for p in ORDER_DESC):
                intent["order_by"] = "DESC"
            elif any(p in t for p in ORDER_ASC):
                intent["order_by"] = "ASC"
            elif intent["limit"]:
                # If LIMIT is present, always ORDER BY DESC (ranking query)
                intent["order_by"] = "DESC"

        return intent




class EntityExtractor:
    def __init__(self, metadata):
        if isinstance(metadata, str):
            with open(metadata, 'r') as f:
                self.metadata = json.load(f)
        else:
            self.metadata = metadata


        self.tables = list(self.metadata['tables'].keys())
        # col_name → [table, ...]
        self.columns = {}
        # table → {col: type_hint}  (we don't have types so we track names)
        self.table_columns = {}
        self.graph = nx.DiGraph()

        for table, info in self.metadata['tables'].items():
            self.graph.add_node(table)
            self.table_columns[table] = set(info['columns'])
            for col in info['columns']:
                self.columns.setdefault(col, [])
                if table not in self.columns[col]:
                    self.columns[col].append(table)

            for local_col, target in info.get('foreign_keys', {}).items():
                target_table, target_col = target.split('.')
                # bidirectional edge with join condition
                cond = f"{table}.{local_col} = {target_table}.{target_col}"
                self.graph.add_edge(table, target_table, on=cond, local=local_col, remote=target_col)
                self.graph.add_edge(target_table, table, on=cond, local=target_col, remote=local_col)

    # ------------------------------------------------------------------
    def _fuzzy_table(self, word):
        """Return best-matching table name or None."""
        result = process.extractOne(word, self.tables, scorer=fuzz.WRatio)
        if result and result[1] >= 82:
            return result[0]
        return None

    def _fuzzy_column(self, word):
        """Return best-matching (column_name, [tables]) or (None, None)."""
        result = process.extractOne(word, list(self.columns.keys()), scorer=fuzz.WRatio)
        if result and result[1] >= 82:
            return result[0], self.columns[result[0]]
        return None, None

    def get_join_path(self, src, dst):
        try:
            return nx.shortest_path(self.graph, src, dst)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    # ------------------------------------------------------------------
    def extract(self, analysis, text):
        """
        Returns:
          {
            tables: [ordered list, primary first],
            columns: [{column, tables, agg_hint}],  # agg_hint: 'price*quantity' etc.
            filters: [{column, operator, value, table}],
          }
        """
        t = text.lower()
        nouns  = analysis["nouns"]
        values = analysis["values"]

        matched_tables  = []
        matched_columns = []  # list of {column, tables}

        # ---------- 1. Match tables & columns from nouns ----------
        # Build set of pk/fk columns to suppress from SELECT (only used for joins)
        pk_fk_cols = set()
        for tbl_name, info in self.metadata["tables"].items():
            pk = info.get("primary_key")
            if pk:
                pk_fk_cols.add(pk)
            for fk_col in info.get("foreign_keys", {}).keys():
                pk_fk_cols.add(fk_col)
        pk_fk_cols.update(col for col in self.columns if col.endswith("_id"))

        for word in nouns:
            tbl = self._fuzzy_table(word)
            if tbl and tbl not in matched_tables:
                matched_tables.append(tbl)

            col, col_tables = self._fuzzy_column(word)
            # Suppress ID/FK columns from SELECT unless user actually typed them
            if col and not any(c["column"] == col for c in matched_columns):
                if col not in pk_fk_cols or col.lower() in t:
                    matched_columns.append({"column": col, "tables": col_tables})

        # ---------- 2. Infer additional tables from column ownership ----------
        for col_info in matched_columns:
            for tbl in col_info["tables"]:
                if tbl not in matched_tables:
                    matched_tables.append(tbl)

        # ---------- 3. Detect domain-level keywords → explicit table+column hints ----------
        # These are language-level patterns, not database-specific names.
        # "revenue" → SUM(quantity * price), need a table with both quantity & price cols
        # "quantity sold" → SUM(quantity)
        # "items per order" → subquery over order+item tables
        # We annotate rather than hardcode table names.
        composite_agg = None
        if re.search(r"\brevenue\b|\btotal revenue\b|\btotal sales\b", t):
            # find the table that has both quantity and price columns (generic)
            for tbl, cols in self.table_columns.items():
                if "quantity" in cols and "price" in cols:
                    if tbl not in matched_tables:
                        matched_tables.append(tbl)
                    composite_agg = "quantity_x_price"
                    break

        if re.search(r"\bquantity sold\b|\bquantities sold\b|\btotal quantity\b|\btotal products sold\b", t):
            for tbl, cols in self.table_columns.items():
                if "quantity" in cols:
                    if tbl not in matched_tables:
                        matched_tables.append(tbl)
            # ensure the quantity column is in matched_columns
            if "quantity" in self.columns and not any(c["column"] == "quantity" for c in matched_columns):
                matched_columns.append({"column": "quantity", "tables": self.columns["quantity"]})

        # ---------- 4. Filter extraction ----------
        filters = []
        skip_words = {
            "id", "order", "list", "show", "all", "each", "per", "the", "a",
            "an", "by", "of", "in", "on", "at", "to", "and", "or", "not",
            "never", "is", "are", "was", "were"
        }
        for val in values:
            val_text = val["text"]
            if val_text.lower() in skip_words:
                continue
            # Only treat as filter value if it's a proper noun/GPE or a number in a comparison context
            is_location = val["type"] in ["GPE", "PROPN"]
            is_number   = val["type"] in ["CARDINAL", "NUM"]
            if not (is_location or is_number):
                continue

            # Find nearest matched column
            best_col, best_dist = None, 9999
            for col_info in matched_columns:
                col_pos = t.find(col_info["column"])
                val_pos = t.find(val_text.lower())
                if col_pos == -1: continue
                dist = abs(col_pos - val_pos)
                if dist < best_dist:
                    best_dist = dist
                    best_col = col_info["column"]

            if best_col:
                op = "="
                if "greater" in analysis["comparisons"]: op = ">"
                if "less" in analysis["comparisons"]: op = "<"
                # If operator is > or < and val_text isn't numeric, we probably extracted wrong word
                if op in [">", "<"] and not val_text.replace(".", "").replace(",", "").isdigit():
                    continue
                if not any(f["column"] == best_col and f["value"] == val_text for f in filters):
                    filters.append({"column": best_col, "operator": op, "value": val_text})

        # ---------- 5. Deduplicate tables + ordering ----------
        seen = []
        for t2 in matched_tables:
            if t2 not in seen:
                seen.append(t2)
        matched_tables = seen

        # ---------- 6. Infer tables from aggregation verbs (schema-agnostic) ----------
        # "orders placed by each customer" → If only customers matched, also need orders
        # "spent the most money" → need the table that has total_amount
        # Strategy: find tables by looking for numeric columns related to the aggregation

        def _table_with_col_hint(hints):
            """Find the first table (not yet in matched_tables) that has a col matching hints."""
            for tbl, cols in self.table_columns.items():
                if any(any(h in c.lower() for h in hints) for c in cols):
                    return tbl
            return None

        # Check if NL mentions a verb that implies a second table
        spending_verbs = ["spent", "spend", "earned", "generated", "revenue", "sales"]
        counting_verbs = ["placed", "made", "handled", "processed"]

        if any(v in t for v in spending_verbs):
            # Need table with amount/total column
            amount_tbl = _table_with_col_hint(["amount", "total", "revenue", "price", "cost"])
            if amount_tbl and amount_tbl not in matched_tables:
                matched_tables.append(amount_tbl)

        if any(v in t for v in counting_verbs):
            detail_suffixes = ("_items", "_details", "_lines", "_entries", "items", "details")
            added_one = False
            for matched_t in list(matched_tables):
                if added_one:
                    break
                for tbl, info in self.metadata["tables"].items():
                    if tbl in matched_tables:
                        continue
                    if any(tbl.endswith(s) for s in detail_suffixes):
                        continue
                    for fk_val in info.get("foreign_keys", {}).values():
                        if fk_val.startswith(matched_t + "."):
                            matched_tables.append(tbl)
                            added_one = True
                            break
                    if added_one:
                        break

        # ---------- 7. Re-order: aggregation source table should be primary ----------
        # For "orders per customer": primary = orders (what we COUNT), not customers
        # For "revenue per product": primary = order_items (where quantity*price lives)
        # For "avg order value per customer": primary = orders (WHERE amount is)
        if len(matched_tables) > 1:
            is_left_join = any(w in t for w in ["never", "no order", "not ordered", "without order", "have no order"])
            if not is_left_join:
                # Find which table has the numeric column most relevant to the aggregation
                numeric_hints = ["amount", "total", "price", "quantity", "revenue", "cost", "salary"]
                for i, tbl in enumerate(matched_tables):
                    if i == 0:
                        continue  # already primary, skip
                    tbl_cols = self.table_columns.get(tbl, set())
                    if any(any(h in c.lower() for h in numeric_hints) for c in tbl_cols):
                        # Swap to primary
                        matched_tables.pop(i)
                        matched_tables.insert(0, tbl)
                        break

        # ---------- 8. Generic "show/list all X" → clear specific columns ----------
        is_generic_select = any(w in t for w in [
            "show all", "list all", "show every", "list every",
            "list all", "show all"
        ])
        if is_generic_select and matched_tables and not filters:
            matched_columns = []

        return {
            "tables": matched_tables,
            "columns": matched_columns,
            "filters": filters,
            "composite_agg": composite_agg,
        }
