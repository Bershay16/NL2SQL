"""
NL Parser — uses spaCy to extract structured linguistic analysis from
natural-language text.  The output is a plain dict consumed downstream
by the EntityExtractor and IntentClassifier.

No database-specific knowledge lives here.
"""

import re
import spacy


class NLParser:
    def __init__(self, model_name: str = "en_core_web_sm"):
        try:
            self.nlp = spacy.load(model_name)
        except OSError:
            self.nlp = spacy.load("en_core_web_sm")

    # ------------------------------------------------------------------ #
    def parse(self, text: str):
        return self.nlp(text)

    # ------------------------------------------------------------------ #
    def get_analysis(self, text: str) -> dict:
        doc = self.parse(text)
        text_lower = text.lower()

        analysis = {
            "tokens": [],
            "nouns": [],          # lemmatised nouns + compound nouns
            "values": [],         # named entities & bare values
            "comparisons": [],    # "greater" | "less" | "equal"
            "temporal": None,     # "year" | "month" | None
            "having_hint": None,  # {"op": ">", "value": "5"}
        }

        # ---- tokens + nouns ------------------------------------------------
        for token in doc:
            analysis["tokens"].append({
                "text": token.text,
                "lemma": token.lemma_.lower(),
                "pos": token.pos_,
                "dep": token.dep_,
                "head": token.head.text,
                "ent_type": token.ent_type_,
            })

            is_noun    = token.pos_ in ("NOUN", "PROPN")
            is_keyword = token.dep_ in ("attr", "dobj", "nsubj", "pobj")

            if is_noun or is_keyword:
                lemma = token.lemma_.lower()
                if lemma not in analysis["nouns"]:
                    analysis["nouns"].append(lemma)

                # compound nouns: "first name", "hire date"
                if token.dep_ == "compound":
                    compound = f"{lemma} {token.head.lemma_.lower()}"
                    if compound not in analysis["nouns"]:
                        analysis["nouns"].append(compound)

                # adjective + noun: "total amount", "last name"
                for child in token.children:
                    if child.pos_ == "ADJ":
                        adj_noun = f"{child.lemma_.lower()} {lemma}"
                        if adj_noun not in analysis["nouns"]:
                            analysis["nouns"].append(adj_noun)

        # ---- values (entities + bare tokens) -------------------------------
        captured_spans: set[tuple[int, int]] = set()
        for ent in doc.ents:
            if ent.label_ in ("GPE", "LOC", "DATE", "CARDINAL", "ORDINAL",
                              "PERSON", "ORG", "MONEY"):
                analysis["values"].append({
                    "text": ent.text,
                    "type": ent.label_,
                    "position": ent.start_char,
                })
                captured_spans.add((ent.start_char, ent.end_char))

        for token in doc:
            already = any(s <= token.idx < e for s, e in captured_spans)
            if already:
                continue
            if (token.pos_ in ("NUM", "PROPN")
                    or token.ent_type_ in ("GPE", "CARDINAL")
                    or token.like_num):
                analysis["values"].append({
                    "text": token.text,
                    "type": token.ent_type_ or token.pos_,
                    "position": token.idx,
                })

        # ---- comparisons ---------------------------------------------------
        gt_words = (">", "greater", "more than", "above", "after",
                    "higher", "over", "exceeds", "exceed")
        lt_words = ("<", "less", "fewer", "below", "before",
                    "lower", "under", "at most")
        eq_words = ("=", "equal", "is", "in", "from", "named",
                    "called", "who live", "lives in", "located")

        if any(w in text_lower for w in gt_words):
            analysis["comparisons"].append("greater")
        if any(w in text_lower for w in lt_words):
            analysis["comparisons"].append("less")
        if any(w in text_lower for w in eq_words):
            analysis["comparisons"].append("equal")

        # ---- HAVING hint ---------------------------------------------------
        having_re = (
            r"(more than|fewer than|less than|greater than|"
            r"over|above|under|below|at least|exceeding)\s+(\d[\d,]*)"
        )
        m = re.search(having_re, text_lower)
        if m:
            op_word = m.group(1)
            val = m.group(2).replace(",", "")
            if any(w in op_word for w in ("more", "greater", "over",
                                          "above", "least", "exceeding")):
                op = ">"
            else:
                op = "<"
            analysis["having_hint"] = {"op": op, "value": val}

        # ---- temporal ------------------------------------------------------
        if re.search(r"\b(each|per|every|by)\s+year\b", text_lower):
            analysis["temporal"] = "year"
        elif re.search(r"\b(each|per|every|by)\s+month\b", text_lower):
            analysis["temporal"] = "month"

        return analysis
