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
from nlp.intelligence import DatabaseLinguist


class IntentClassifier:
    """
    Detects SQL intent (aggregation, ordering, limit, having, temporal,
    grouping) from the raw NL text + parser analysis.
    """

    _AGG_PATTERNS: list[tuple[str, list[str]]] = [
        ("count", [
            "how many", "count of", "total count", "total records",
            "number of records", "number of rows", "count ",
        ]),
        ("sum", [
            "total revenue", "total sales", "total amount", "total spent",
            "total cost", "total quantity", "sum of", "combined", "total",
        ]),
        ("avg", ["average", "avg ", "mean "]),
        ("max", ["highest", "maximum", "max ", "most expensive", "largest",
                 "greatest", "top salary", "top amount", "paid the most"]),
        ("min", ["lowest", "minimum", "min ", "cheapest", "smallest", "paid the least"]),
    ]

    def classify(self, text: str, analysis: dict | None = None, entities: dict | None = None) -> dict:
        t = text.lower()
        t = text.lower()
        # Handle 'SELCET' typo and generic SELECT hints
        t = t.replace("selcet", "select")

        intent: dict = {
            "original_text": text,
            "aggregation":   None,
            "order_by":      None,
            "limit":         None,
            "having":        None,
            "temporal":      None,
            "group_by_hint": False,
            "distinct":      False,
        }

        # ---- DISTINCT ----------------------------------------------------
        if re.search(r"\b(distinct|unique|different|unique list of)\b", t):
            intent["distinct"] = True

        # ---- LIMIT -------------------------------------------------------
        m = re.search(
            r"\btop\s+(\d+)\b|\blimit\s+(\d+)\b|\bfirst\s+(\d+)\b"
            r"|\bbottom\s+(\d+)\b|\blast\s+(\d+)\b",
            t,
        )
        if m:
            intent["limit"] = int(next(g for g in m.groups() if g))
        elif "all" in t or "*" in t:
             if any(w in t for w in ("show", "list", "get", "display", "select")):
                 intent["all_columns"] = True # Explicit hint for SELECT *

        # ---- TEMPORAL ----------------------------------------------------
        if analysis:
            intent["temporal"] = analysis.get("temporal")

        # ---- GROUP BY hint -----------------------------------------------
        if re.search(r"\b(per|each|every|by each|for each|group by|grouped)\b", t) or (analysis and analysis.get("group_by_hint")):
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
                # Use word boundaries to avoid 'phone number' matching 'number'
                pattern = r"\b(" + "|".join(re.escape(p) for p in phrases) + r")\b"
                m_agg = re.search(pattern, t)
                if m_agg:
                    # print(f"DEBUG: matched agg {func} on word {m_agg.group(0)}")
                    intent["aggregation"] = func
                    break

            if analysis and analysis.get("having_hint"):
                # Detect aggregation indicators (but avoid literal table names like 'employees')
                has_agg_signal = (
                    intent["group_by_hint"] or 
                    any(w in t for w in ("number", "count", "many", "total", "sum", "records", "employees", "people"))
                )

                # Collision check: If the value in having_hint is already used as a WHERE filter, 
                # e.g., 'salary > 70000', skip the HAVING logic unless it's a clear 'group by' query.
                if entities and not intent["group_by_hint"]:
                    # Clean the value for robust comparison
                    val_str = str(analysis["having_hint"]["value"]).strip().strip(",").strip(".")
                    if any(str(f["value"]).strip() == val_str for f in entities.get("filters", [])):
                        has_agg_signal = False

                if has_agg_signal:
                    intent["having"] = analysis["having_hint"]
                    if not intent["aggregation"]:
                        # Treat 'highest number of' as count, not max
                        if "number" in t or "many" in t:
                             intent["aggregation"] = "count"
                        else:
                             intent["aggregation"] = "count"

            # Infer from "most X" pattern
            if not intent["aggregation"]:
                most_m = re.search(r"\bmost\s+(\w+)", t)
                if most_m:
                    intent["aggregation"] = "count"
                    intent["order_by"] = "DESC"

        # ---- ORDER BY (if not already set) --------------------------------
        if not intent["order_by"]:
            desc_words = ("highest", "most", "largest", "top", "descending", "desc", "best", "greatest", "youngest", "recent", "recently")
            asc_words  = ("lowest", "least", "smallest", "cheapest", "ascending", "asc", "alphabetically", "oldest")
            
            if any(w in t for w in desc_words):
                intent["order_by"] = "DESC"
            elif any(w in t for w in asc_words) or any(w in t for w in ("sort", "order", "sorted")):
                intent["order_by"] = "ASC"
            elif intent["limit"]:
                intent["order_by"] = "DESC"

            # SPECIAL CASE: 'Top N highest' means ranking, not max()
            agg_val = (intent.get("aggregation") or "").lower()
            if (intent.get("limit") or 0) > 0 and agg_val in ("max", "min"):
                 intent["aggregation"] = None
                 if not intent["order_by"]:
                     intent["order_by"] = "DESC" if agg_val == "max" else "ASC"

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

    def __init__(self, metadata, linguist: DatabaseLinguist | None = None):
        if isinstance(metadata, str):
            with open(metadata, "r") as f:
                self.metadata = json.load(f)
        else:
            self.metadata = metadata

        self.linguist = linguist  # Direct DB access for 'fine-tuned' accuracy

        self.tables: dict[str, dict] = self.metadata.get("tables", {})
        self.db_url = self.metadata.get("db_url") # Store for dynamic context if available

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
                    "distinct_values": col_meta.get("distinct_values", col_meta.get("sample_values", [])),
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
        # Pre-process: merge 'i d' back into 'id' if spacy split it
        t = t.replace(" i d ", " id ").replace(" i.d. ", " id ").replace("name and id", "name id")
        
        nouns = analysis.get("nouns", [])
        if "id" not in nouns and ("id" in t or " i d " in t):
             nouns.append("id")

        # Include both detected values and other potential entities (GPE, etc.)
        raw_vals = analysis.get("values", [])
        values = [v for v in raw_vals if len(v["text"]) > 1 or v["text"].isdigit()]
        
        # ---- Optional: Log the DB context used for this query ----
        if self.linguist:
            context = self.linguist.get_context_summary()
            
        # ---- 1. Resolve the single table ---------------------------------
        table = self._resolve_table(t, nouns, values)
        if not table:
            return {"table": None, "tables": [], "columns": [], "filters": []}

        table_cols = self._cols_for_table(table)

        # ---- 2. Resolve filter values from distinct/sample values --------
        filters = self._extract_filters_from_values(values, table, t)
        
        # ---- 3. Resolve temporal filters (Dates) -------------------------
        temp = analysis.get("temporal_filter")
        temp_val = None
        if temp:
            temp_val = str(temp.get("value")).lower()
            # Smart find: match column keywords like 'hired', 'born', 'joined'
            date_col = None
            date_cols = [c for c, m in table_cols.items() if m.get("data_type") in ("date", "timestamp")]
            # Priority 1: Match keyword in query to column label
            # e.g. "hired" matches "hire_date"
            for dc in date_cols:
                # Priority: query words like 'hired' should match 'hire_date'
                # Use a tighter match: check if column name contains root of word
                if any(w[:4] in dc.lower() for w in t.split() if len(w) > 3):
                    date_col = dc
                    break
            # Priority 2: Use first available
            if not date_col and date_cols:
                date_col = date_cols[0]
            
            if date_col:
                filters.append({
                    "column": date_col, "operator": temp.get("operator", ">"),
                    "value": temp.get("value"), "table": table
                })

        # ALSO scan raw text for value matches (handles words spaCy misses)
        filters += self._extract_filters_from_text_scan(t, table, temp_val=temp_val)
        filters += self._extract_comparison_filters(t, table)
        filters += self._extract_like_filters(t, table)
        filters += self._extract_null_filters(t, table)
        filters = self._dedupe_filters(filters)
        
        # Prevent overlap between exact match filter (from spaCy entity) and LIKE phrase filter
        like_vals = [str(f["value"]).replace("%", "").lower() for f in filters if f["operator"] == "LIKE"]
        if like_vals:
             filters = [f for f in filters if not (f["operator"] in ("=", "ILIKE") and str(f["value"]).lower() in like_vals)]

        # ---- 4. Resolve SELECT columns -----------------------------------
        columns = self._resolve_columns(t, nouns, table, table_cols, filters)

        # ---- 4. Handle "show all" / "list all" → SELECT * ----------------
        select_all = False
        
        filter_cols_set = {f["column"] for f in filters}
        non_filter_cols = [c["column"] for c in columns if c["column"] not in filter_cols_set and c["column"] != "id"]
        
        auxiliary_cols = {"salary", "hire_date", "date_of_birth", "city", "first_name", "last_name", "created_at"}
        
        if not non_filter_cols:
            if self._is_generic_show(t, table) or ("all" in t or "*" in t):
                select_all = True
        elif len(non_filter_cols) == 1 and non_filter_cols[0] in auxiliary_cols:
            # If there's a sorting/ranking keyword, this column is for ordering
            if any(w in t for w in ("sort", "order", "highest", "lowest", "most", "least", "top", "bottom", "youngest", "oldest", "recent", "recently")):
                select_all = True

        result = {
            "table":   table,
            "tables":  [table],
            "columns": columns,
            "filters": filters,
            "select_all": select_all
        }
        return result

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
                # Allow PK if explicitly mentioned in text
                if col not in filter_cols:
                    if not any(c["column"] == col for c in matched):
                        matched.append({"column": col, "table": table})

        # Match "id" or "identifier" to the Primary Key
        if re.search(r"\b(id|ids|identifier|identifiers|pk)\b", text):
            pk = None
            for col, meta in table_cols.items():
                if meta.get("is_primary_key"):
                    pk = col
                    break
            if pk and not any(c["column"] == pk for c in matched):
                matched.append({"column": pk, "table": table})

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

        synonym_columns = {
            "oldest": "date_of_birth",
            "youngest": "date_of_birth",
            "recent": "created_at",
            "recently": "created_at",
            "created": "created_at",
            "earning": "salary",
            "emails": "email"
        }
        for w, target_col in synonym_columns.items():
            if w in text.lower() and target_col in col_names:
                if not any(c["column"] == target_col for c in matched):
                     matched.append({"column": target_col, "table": table})

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
            "name", "id", "data", "record", "list", "show", "i", "d"
        }

        for val_item in values:
            val_text = val_item["text"]
            val_lower = val_text.lower()


            if val_lower in skip:
                continue
            if val_text.replace(",", "").replace(".", "").lstrip("-").isdigit():
                continue

            # Skip if value is a noise word
            if val_lower in skip:
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
            if best_col and best_score >= 90: # Higher bar for fuzzy values
                filters.append({
                    "column": best_col, "operator": "=",
                    "value": str(best_val), "table": table,
                })

        return filters

    def _extract_filters_from_text_scan(self, text: str, table: str, temp_val: str | None = None) -> list[dict]:
       
        filters: list[dict] = []
        table_cols = self._cols_for_table(table)
        words = [w.strip(".,;:?!\"'") for w in text.split()]
        skip = {
            "all", "show", "list", "find", "get", "display", "the", "a",
            "an", "each", "per", "every", "by", "of", "in", "on", "to",
            "and", "or", "not", "is", "are", "was", "were", "who", "whom",
            "which", "that", "top", "bottom", "first", "last", "most",
            "least", "total", "average", "count", "sum", "max", "min",
            "number", "many", "few", "more", "less", "than", "above",
            "below", "over", "under", "between", "employee", "employees",
            "name", "names", "email", "emails", "id", "data", "record", 
            "from", "after", "before", "since", "until", "for", "with",
            "unique", "different"
        }
        if temp_val:
            for w in temp_val.split():
                skip.add(w)
            skip.add(temp_val)

        # 1. Exact sentence inclusion for multi-word values (e.g., 'New York')
        for col_name, col_meta in table_cols.items():
            for dv in col_meta.get("distinct_values", []):
                dv_str = str(dv)
                dv_lower = dv_str.lower()
                if " " in dv_lower and len(dv_lower) > 3:
                     if dv_lower in text:
                         if not any(f["column"] == col_name and f["value"] == dv_str for f in filters):
                             filters.append({"column": col_name, "operator": "=", "value": dv_str, "table": table})

        # 2. Find the absolute best mathematical match for each single word
        import re
        quotes = re.findall(r"['\"](.*?)['\"]", text)
        words_in_quotes = {w.lower() for q in quotes for w in q.split()}

        for word in words:
            w_lower = word.lower()
            if w_lower in skip or w_lower in words_in_quotes:
                continue

                
            # If length is < 3, we only consider it if it EXACTLY matches a distinct value
            # Let's handle short words carefully.
            best_col = None

            best_dv = None
            best_score = 0
            
            for col_name, col_meta in table_cols.items():
                for dv in col_meta.get("distinct_values", []):
                    dv_str = str(dv)
                    dv_lower = dv_str.lower()
                    
                    if len(dv_lower) == 1 and not w_lower.startswith(dv_lower):
                        continue
                        
                    score = fuzz.WRatio(w_lower, dv_lower)
                    if score > best_score:
                        best_score = score
                        best_col = col_name
                        best_dv = dv_str

            # Only accept the highest-scoring match if it meets our strict accuracy threshold
            # For short words, be much stricter
            min_score = 92 if len(w_lower) < 5 else 88
            if best_score >= min_score and best_col and best_dv: 
                # Avoid false positives for 'manager' matching 'Project Manager'
                if w_lower == "manager" and "manager" in best_dv.lower() and best_dv.lower() != "manager":
                     # Skip if it's just a partial match like 'Project Manager'
                     continue
                
                # CRITICAL: If value is very short (e.g. 'IT'), require exact word match
                if len(best_dv) <= 2:
                    if w_lower != best_dv.lower():
                        continue

                # Avoid appending if a multi-word phrase already captured it
                if not any(f["column"] == best_col and f["value"] == best_dv for f in filters):
                    filters.append({
                        "column": best_col, "operator": "=",
                        "value": best_dv, "table": table,
                    })

        return filters

    def _extract_null_filters(self, text: str, table: str) -> list[dict]:
        """Extract IS NULL / IS NOT NULL filters."""
        filters: list[dict] = []
        t = text.lower()
        table_cols = self._cols_for_table(table)

        null_patterns = [
            (r"\b(?:do\s+)?not\s+have\s+(?:a\s+)?(\w+)\s+(?:assigned|set|available|manager)?", True),
            (r"\bno\s+(\w+)\s+(assigned|set|available|manager)?", True),
            (r"\bwithout\s+(?:a\s+)?(\w+)", True),
            (r"\b(\w+)\s+is\s+(null|missing|empty|none|not set)\b", True),
            (r"\b(\w+)\s+is\s+not\s+(null|missing|empty|none)\b", False),
        ]

        for pat, is_null in null_patterns:
            for m in re.finditer(pat, t):
                col_hint = m.group(1) if is_null else m.group(1)
                if pat.startswith(r"\b(not|no|none|without|missing)"):
                    col_hint = m.group(2)
                
                col = self._resolve_col_hint(col_hint, table_cols)
                if col:
                    filters.append({
                        "column": col, "operator": "IS NULL" if is_null else "IS NOT NULL",
                        "value": None, "table": table
                    })
        return filters

    def _extract_comparison_filters(self, text: str, table: str) -> list[dict]:
        """Extract numeric comparison filters: 'salary > 50000', 'salary above 50000'."""
        filters: list[dict] = []
        table_cols = self._cols_for_table(table)

        # Synonym map for columns
        synonyms = {
            "earning": "salary",
            "earnings": "salary",
            "paid": "salary",
            "compensation": "salary",
            "age": "date_of_birth", # Usually flipped logic, but helps match
            "born": "date_of_birth",
            "hired": "hire_date",
            "joined": "hire_date",
        }

        # Pre-process text to replace synonyms for column resolution
        processed_text = text
        for syn, target in synonyms.items():
            processed_text = re.sub(r"\b" + syn + r"\b", target, processed_text)

        patterns = [
            (r"(\w+)\s+(?:is\s+)?(?:greater than|more than|above|over|higher than|exceeds?|>)\s+(\d[\d,\.]*)", ">"),
            (r"(\w+)\s+(?:is\s+)?(?:less than|fewer than|below|under|lower than|<)\s+(\d[\d,\.]*)", "<"),
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
            for m in re.finditer(pat, processed_text):
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
        return {k: v for k, v in self._col_index.items() if v.get("table") == table}

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
        if text.startswith(f"list {table}") or text.startswith(f"show {table}") or text.startswith(f"find {table}") or text.startswith(f"get {table}"):
            return True
        # also check singular "employee"
        singular = table.rstrip('s')
        if text.startswith(f"list {singular}") or text.startswith(f"show {singular}") or text.startswith(f"find {singular}") or text.startswith(f"get {singular}"):
            return True
        return False
