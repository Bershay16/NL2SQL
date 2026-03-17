"""
Context-driven Entity Extractor + Intent Classifier for single-table NL→SQL.

KEY DESIGN PRINCIPLES:
  • ZERO hard-coded column names or table names.
  • All understanding comes from the *metadata* passed at init:
      – column descriptions & data_type  →  decide numeric vs text
      – sample_values & distinct_values  →  resolve filter values
      – column names                     →  fuzzy-match user words
  • Single-table only: picks the best-matching table, no JOIN logic.
"""

import re
import json
from rapidfuzz import process, fuzz


# ===================================================================== #
#  INTENT CLASSIFIER
# ===================================================================== #
class IntentClassifier:
    """
    Detects SQL intent (aggregation, ordering, limit, having, temporal,
    grouping) from the raw NL text + parser analysis.
    """

    # Aggregation concept → trigger phrases  (order = priority)
    _AGG_PATTERNS: list[tuple[str, list[str]]] = [
        ("count", [
            "how many", "number of", "count of", "count the",
            "total number", "total count",
        ]),
        ("sum", [
            "total revenue", "total sales", "total amount", "total spent",
            "total cost", "total quantity", "sum of", "combined",
        ]),
        ("avg", ["average", "avg ", "mean "]),
        ("max", ["highest", "maximum", "max ", "most expensive", "largest",
                 "greatest", "top salary", "top amount"]),
        ("min", ["lowest", "minimum", "min ", "cheapest", "smallest"]),
    ]

    def classify(self, text: str, analysis: dict | None = None) -> dict:
        t = text.lower()

        intent: dict = {
            "original_text": text,
            "aggregation":   None,
            "order_by":      None,
            "limit":         None,
            "having":        None,
            "temporal":      None,
            "group_by_hint": False,
        }

        # ---- LIMIT -------------------------------------------------------
        m = re.search(
            r"\btop\s+(\d+)\b|\blimit\s+(\d+)\b|\bfirst\s+(\d+)\b"
            r"|\bbottom\s+(\d+)\b|\blast\s+(\d+)\b",
            t,
        )
        if m:
            intent["limit"] = int(next(g for g in m.groups() if g))

        # ---- TEMPORAL ----------------------------------------------------
        if analysis:
            intent["temporal"] = analysis.get("temporal")

        # ---- GROUP BY hint -----------------------------------------------
        if re.search(r"\b(per|each|every|by each|for each|group by|grouped)\b", t):
            intent["group_by_hint"] = True

        # ---- "X with the highest Y" → ORDER BY + LIMIT 1, NOT agg -------
        entity_superlative = re.search(
            r"\b\w+\s+with\s+(the\s+)?"
            r"(highest|lowest|most|least|maximum|minimum)\b", t,
        )
        if entity_superlative:
            intent["limit"] = intent["limit"] or 1
            sup = entity_superlative.group(2)
            intent["order_by"] = (
                "ASC" if sup in ("lowest", "least", "minimum") else "DESC"
            )
        else:
            # Explicit aggregation keywords
            for func, phrases in self._AGG_PATTERNS:
                if any(p in t for p in phrases):
                    intent["aggregation"] = func
                    break

            # ---- HAVING --------------------------------------------------
            # Only set HAVING when there's also a GROUP BY hint,
            # otherwise it's a simple WHERE filter.
            if analysis and analysis.get("having_hint") and intent["group_by_hint"]:
                intent["having"] = analysis["having_hint"]
                if not intent["aggregation"]:
                    intent["aggregation"] = "count"

            # Infer from "most X" pattern
            if not intent["aggregation"]:
                most_m = re.search(r"\bmost\s+(\w+)", t)
                if most_m:
                    intent["aggregation"] = "count"
                    intent["order_by"] = "DESC"

        # ---- ORDER BY (if not already set) --------------------------------
        if not intent["order_by"]:
            # Only match contextual order words, not "highest/lowest" when
            # they are used for aggregation (those are already handled)
            if intent["aggregation"] in ("max", "min"):
                pass  # don't add ORDER BY for pure MAX()/MIN()
            else:
                desc_words = ("highest", "most", "largest", "top", "descending",
                              "desc", "best", "greatest")
                asc_words  = ("lowest", "least", "smallest", "cheapest",
                              "ascending", "asc", "alphabetically", "oldest")
                if any(w in t for w in desc_words):
                    intent["order_by"] = "DESC"
                elif any(w in t for w in asc_words):
                    intent["order_by"] = "ASC"
                elif intent["limit"]:
                    intent["order_by"] = "DESC"

        return intent


# ===================================================================== #
#  ENTITY EXTRACTOR  (context-driven, single-table)
# ===================================================================== #
class EntityExtractor:
    """
    Resolves which *table*, *columns*, and *filter values* a natural-
    language query refers to — driven entirely by the metadata dict.
    """

    FUZZY_THRESHOLD = 78
    COLUMN_FUZZY_THRESHOLD = 85  # Higher threshold for column matching to avoid false positives

    def __init__(self, metadata):
        if isinstance(metadata, str):
            with open(metadata, "r") as f:
                self.metadata = json.load(f)
        else:
            self.metadata = metadata

        self.tables: dict[str, dict] = self.metadata.get("tables", {})

        # Pre-index: column_name → {data_type, description, sample, distinct, table}
        self._col_index: dict[str, dict] = {}
        # value_lower → (column, table)  quick lookup from distinct/sample values
        self._value_index: dict[str, tuple[str, str]] = {}
        # All column names
        self._all_col_names: list[str] = []

        for tbl_name, tbl_info in self.tables.items():
            cols = tbl_info.get("columns", {})
            if isinstance(cols, list):
                cols = {c: {"data_type": "text", "description": c} for c in cols}
            for col_name, col_meta in cols.items():
                self._col_index[col_name] = {
                    "table": tbl_name,
                    "data_type": col_meta.get("data_type", "text"),
                    "description": col_meta.get("description", col_name),
                    "sample_values": col_meta.get("sample_values", []),
                    "distinct_values": col_meta.get("distinct_values", []),
                    "is_primary_key": col_meta.get("is_primary_key", False),
                }
                self._all_col_names.append(col_name)

                # Index distinct values
                for val in col_meta.get("distinct_values", []):
                    val_lower = str(val).lower()
                    self._value_index[val_lower] = (col_name, tbl_name)

                # Index sample values (lower priority, don't overwrite distinct)
                for val in col_meta.get("sample_values", []):
                    if val is not None:
                        val_lower = str(val).lower()
                        if val_lower not in self._value_index:
                            self._value_index[val_lower] = (col_name, tbl_name)

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #
    def extract(self, analysis: dict, text: str) -> dict:
        t = text.lower()
        nouns = analysis.get("nouns", [])
        values = analysis.get("values", [])

        # ---- 1. Resolve the single table ---------------------------------
        table = self._resolve_table(t, nouns, values)
        if not table:
            return {"table": None, "tables": [], "columns": [], "filters": []}

        table_cols = self._cols_for_table(table)

        # ---- 2. Resolve filter values from distinct/sample values --------
        filters = self._extract_filters_from_values(values, table, t)
        # Also scan raw text for value matches (handles words spaCy misses)
        filters += self._extract_filters_from_text_scan(t, table)
        filters += self._extract_comparison_filters(t, table)
        filters += self._extract_like_filters(t, table)
        filters = self._dedupe_filters(filters)

        # ---- 3. Resolve SELECT columns -----------------------------------
        columns = self._resolve_columns(t, nouns, table, table_cols, filters)

        # ---- 4. Handle "show all" / "list all" → SELECT * ----------------
        if self._is_generic_show(t, table):
            filter_col_names = {f["column"] for f in filters}
            columns = [c for c in columns if c["column"] not in filter_col_names]

        return {
            "table":   table,
            "tables":  [table],
            "columns": columns,
            "filters": filters,
        }

    # ------------------------------------------------------------------ #
    #  Table resolution
    # ------------------------------------------------------------------ #
    def _resolve_table(self, text: str, nouns: list[str], values: list[dict]) -> str | None:
        """Pick the single best-matching table, or None if query is unrelated."""
        table_names = list(self.tables.keys())
        if not table_names:
            return None

        # Score each table — must actually find evidence the query relates to it
        best_table, best_score = None, 0

        for tbl in table_names:
            score = 0

            # Exact table name in text (strongest signal)
            if re.search(r"\b" + re.escape(tbl) + r"\b", text):
                score += 100

            # Singular/plural fuzzy match of table name against nouns
            for noun in nouns:
                r = process.extractOne(noun, [tbl], scorer=fuzz.WRatio)
                if r and r[1] >= 82:
                    score += 50

            # Check if any word in text matches a distinct/sample value in this table
            for val_lower, (col, val_tbl) in self._value_index.items():
                if val_tbl == tbl and re.search(r"\b" + re.escape(val_lower) + r"\b", text):
                    score += 30

            # Check if any noun matches a column name in this table
            tbl_col_names = list(self._cols_for_table(tbl).keys())
            for noun in nouns:
                if len(noun) < 3:  # skip very short words
                    continue
                r = process.extractOne(noun, tbl_col_names, scorer=fuzz.WRatio)
                if r and r[1] >= 88:
                    score += 20

            if score > best_score:
                best_score = score
                best_table = tbl

        # Require a minimum relevance score to avoid matching unrelated queries
        if best_score >= 30:
            return best_table

        # If only one table, be lenient but still require SOME signal
        if len(table_names) == 1 and best_score >= 15:
            return table_names[0]

        return None

    # ------------------------------------------------------------------ #
    #  Column resolution
    # ------------------------------------------------------------------ #
    def _resolve_columns(
        self, text: str, nouns: list[str],
        table: str, table_cols: dict[str, dict],
        filters: list[dict],
    ) -> list[dict]:
        """Identify which columns the user wants in SELECT."""
        matched: list[dict] = []
        filter_cols = {f["column"] for f in filters}
        col_names = list(table_cols.keys())

        # Common English words that should NEVER fuzzy-match to a column
        noise_words = {
            "show", "list", "find", "get", "display", "all", "the", "a",
            "an", "from", "in", "on", "at", "to", "by", "with", "of",
            "and", "or", "not", "is", "are", "was", "were", "who", "whom",
            "which", "that", "these", "those", "this", "each", "per",
            "every", "total", "number", "count", "many", "how", "what",
            "where", "than", "more", "less", "between", "ordered",
            "ascending", "descending", "top", "bottom", "first", "last",
            "there", "their", "they", "employee", "employees", "customer",
            "customers", "record", "records", "data",
            "it", "he", "she", "we", "us", "me", "my", "his", "her",
            "its", "capital", "do", "does", "did", "have", "has", "had",
        }
        # Also add table names as noise (don't match "employees" to a column)
        for tbl in self.tables:
            noise_words.add(tbl.lower())

        # Direct substring match: "first_name" or "first name" in text
        for col in col_names:
            pattern = re.escape(col).replace(r"\_", r"[\s\_]")
            if re.search(r"\b" + pattern + r"\b", text):
                if col not in filter_cols and not table_cols[col].get("is_primary_key"):
                    if not any(c["column"] == col for c in matched):
                        matched.append({"column": col, "table": table})

        # Fuzzy match nouns → column names (strict threshold)
        for noun in nouns:
            if noun in noise_words or len(noun) < 3:
                continue
            # Skip nouns that are known distinct values (they are filters, not SELECT cols)
            if noun in self._value_index:
                continue
            result = process.extractOne(noun, col_names, scorer=fuzz.WRatio)
            if result and result[1] >= self.COLUMN_FUZZY_THRESHOLD:
                col = result[0]
                if (col not in filter_cols
                        and not table_cols[col].get("is_primary_key")
                        and not any(c["column"] == col for c in matched)):
                    matched.append({"column": col, "table": table})

        return matched

    # ------------------------------------------------------------------ #
    #  Filter extraction
    # ------------------------------------------------------------------ #
    def _extract_filters_from_values(
        self, values: list[dict], table: str, text: str,
    ) -> list[dict]:
        """Match spaCy-detected values against distinct_values / sample_values."""
        filters: list[dict] = []
        table_cols = self._cols_for_table(table)

        skip = {
            "all", "show", "list", "find", "get", "display", "the", "a",
            "an", "each", "per", "every", "by", "of", "in", "on", "to",
            "and", "or", "not", "is", "are", "was", "were", "who", "whom",
            "which", "that", "top", "bottom", "first", "last", "most",
            "least", "total", "average", "count", "sum", "max", "min",
            "number", "many", "few", "more", "less", "than", "above",
            "below", "over", "under", "between", "employee", "employees",
        }

        for val_item in values:
            val_text = val_item["text"]
            val_lower = val_text.lower()

            if val_lower in skip:
                continue
            if val_text.replace(",", "").replace(".", "").lstrip("-").isdigit():
                continue

            # 1. Exact match in value index
            if val_lower in self._value_index:
                col, tbl = self._value_index[val_lower]
                if tbl == table and col in table_cols:
                    proper_val = self._find_proper_case(col, val_text)
                    filters.append({
                        "column": col, "operator": "=",
                        "value": proper_val, "table": table,
                    })
                    continue

            # 2. Fuzzy match against all distinct values for this table
            best_col, best_val, best_score = None, None, 0
            for col_name, col_meta in table_cols.items():
                for dv in col_meta.get("distinct_values", []):
                    score = fuzz.WRatio(val_lower, str(dv).lower())
                    if score > best_score and score >= 85:
                        best_col, best_val, best_score = col_name, dv, score
            if best_col:
                filters.append({
                    "column": best_col, "operator": "=",
                    "value": str(best_val), "table": table,
                })

        return filters

    def _extract_filters_from_text_scan(self, text: str, table: str) -> list[dict]:
        """
        Scan raw text for words that match distinct_values but were missed
        by spaCy NER (e.g., 'male', 'female', 'IT', 'active').
        """
        filters: list[dict] = []
        table_cols = self._cols_for_table(table)

        for col_name, col_meta in table_cols.items():
            for dv in col_meta.get("distinct_values", []):
                dv_str = str(dv)
                dv_lower = dv_str.lower()
                # Must be at least 2 chars to avoid matching single letters
                if len(dv_lower) < 2:
                    continue
                # Check if this distinct value appears as a whole word in text
                if re.search(r"\b" + re.escape(dv_lower) + r"\b", text):
                    if not any(
                        f["column"] == col_name and f["value"] == dv_str
                        for f in filters
                    ):
                        filters.append({
                            "column": col_name, "operator": "=",
                            "value": dv_str, "table": table,
                        })

        return filters

    def _extract_comparison_filters(self, text: str, table: str) -> list[dict]:
        """Extract numeric comparison filters: 'salary > 50000', 'salary above 50000'."""
        filters: list[dict] = []
        table_cols = self._cols_for_table(table)

        patterns = [
            (r"(\w+)\s+(?:is\s+)?(?:greater than|more than|above|over|higher than|exceeds?|after|since|later than|>)\s+(\d[\d,\.]*)", ">"),
            (r"(\w+)\s+(?:is\s+)?(?:less than|fewer than|below|under|lower than|before|prior to|earlier than|<)\s+(\d[\d,\.]*)", "<"),
            (r"(\w+)\s+(?:is\s+)?(?:at least|>=)\s+(\d[\d,\.]*)", ">="),
            (r"(\w+)\s+(?:is\s+)?(?:at most|<=)\s+(\d[\d,\.]*)", "<="),
            (r"(\w+)\s*>\s*(\d[\d,\.]*)", ">"),
            (r"(\w+)\s*<\s*(\d[\d,\.]*)", "<"),
            (r"(\w+)\s*>=\s*(\d[\d,\.]*)", ">="),
            (r"(\w+)\s*<=\s*(\d[\d,\.]*)", "<="),
            (r"(\w+)\s*=\s*(\d[\d,\.]*)", "="),
            (r"(\w+)\s+between\s+(\d[\d,\.]*)\s+and\s+(\d[\d,\.]*)", "BETWEEN"),
        ]

        for pat, op in patterns:
            for m in re.finditer(pat, text):
                col_hint = m.group(1).strip()
                col = self._resolve_col_hint(col_hint, table_cols)
                if not col:
                    continue

                if op == "BETWEEN":
                    v1 = m.group(2).replace(",", "")
                    v2 = m.group(3).replace(",", "")
                    filters.append({"column": col, "operator": ">=", "value": v1, "table": table})
                    filters.append({"column": col, "operator": "<=", "value": v2, "table": table})
                else:
                    val = m.group(2).replace(",", "").rstrip(".")
                    filters.append({"column": col, "operator": op, "value": val, "table": table})

        return filters

    def _extract_like_filters(self, text: str, table: str) -> list[dict]:
        """Extract LIKE filters: 'name starts with A', 'email contains gmail'."""
        filters: list[dict] = []
        table_cols = self._cols_for_table(table)

        like_patterns = [
            (r"(\w+(?:\s+\w+)?)\s+(?:starts? with|beginning with)\s+['\"]?(\w+)['\"]?",
             lambda v: f"{v}%"),
            (r"(\w+(?:\s+\w+)?)\s+(?:ends? with|ending with)\s+['\"]?([\w@\.]+)['\"]?",
             lambda v: f"%{v}"),
            (r"(\w+(?:\s+\w+)?)\s+(?:contains?|containing)\s+['\"]?(\w+)['\"]?",
             lambda v: f"%{v}%"),
        ]
        for pat, fmt in like_patterns:
            for m in re.finditer(pat, text):
                col_hint = m.group(1).strip()
                val = m.group(2).strip()
                col = self._resolve_col_hint(col_hint, table_cols)
                if col:
                    filters.append({
                        "column": col, "operator": "LIKE",
                        "value": fmt(val), "table": table,
                    })
        return filters

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #
    def _cols_for_table(self, table: str) -> dict[str, dict]:
        tbl_info = self.tables.get(table, {})
        cols = tbl_info.get("columns", {})
        if isinstance(cols, list):
            return {c: {"data_type": "text"} for c in cols}
        return cols

    def _resolve_col_hint(self, hint: str, table_cols: dict) -> str | None:
        hint_lower = hint.lower().replace(" ", "_")

        # Exact
        if hint_lower in table_cols:
            return hint_lower

        # Partial containment
        for col in table_cols:
            if hint_lower in col or col in hint_lower:
                return col

        # Fuzzy
        result = process.extractOne(
            hint_lower, list(table_cols.keys()), scorer=fuzz.WRatio,
        )
        if result and result[1] >= 70:
            return result[0]

        return None

    def _find_proper_case(self, col: str, user_val: str) -> str:
        meta = self._col_index.get(col, {})
        for pool in (meta.get("distinct_values", []), meta.get("sample_values", [])):
            for v in pool:
                if str(v).lower() == user_val.lower():
                    return str(v)
        return user_val

    @staticmethod
    def _dedupe_filters(filters: list[dict]) -> list[dict]:
        seen: set[tuple] = set()
        out: list[dict] = []
        for f in filters:
            key = (f["column"], f["operator"], str(f["value"]))
            if key not in seen:
                seen.add(key)
                out.append(f)
        return out

    @staticmethod
    def _is_generic_show(text: str, table: str) -> bool:
        generic = ("show all", "list all", "display all", "show every",
                   "list every", "get all", "fetch all", "select all")
        if any(g in text for g in generic):
            return True
        if text.startswith(f"list {table}") or text.startswith(f"show {table}"):
            return True
        return False
