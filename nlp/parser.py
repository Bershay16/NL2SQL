import re
import spacy

class NLParser:
    def __init__(self, model_name="en_core_web_sm"):
        try:
            self.nlp = spacy.load(model_name)
        except OSError:
            self.nlp = spacy.load("en_core_web_sm")

    def parse(self, text):
        return self.nlp(text)

    def get_analysis(self, text):
        doc = self.parse(text)
        analysis = {
            "tokens": [],
            "nouns": [],
            "values": [],
            "comparisons": [],
            "temporal": None,     # year / month extraction
            "having_hint": None,  # numeric having condition
        }

        for token in doc:
            analysis["tokens"].append({
                "text": token.text,
                "lemma": token.lemma_.lower(),
                "pos": token.pos_,
                "dep": token.dep_,
                "head": token.head.text,
                "ent_type": token.ent_type_
            })

            if token.pos_ in ["NOUN", "PROPN"]:
                analysis["nouns"].append(token.lemma_.lower())

            # Collect values: numbers, proper nouns (city names etc), dates
            if token.pos_ in ["PROPN", "NUM"] or token.ent_type_ in ["GPE", "DATE", "CARDINAL"]:
                analysis["values"].append({
                    "text": token.text,
                    "type": token.ent_type_ or token.pos_,
                    "position": token.idx
                })

        text_lower = text.lower()

        # Comparison operators
        if any(w in text_lower for w in [">", "greater", "more than", "above", "after", "higher"]): 
            analysis["comparisons"].append("greater")
        if any(w in text_lower for w in ["<", "less", "fewer", "below", "before", "lower"]):  
            analysis["comparisons"].append("less")
        if any(w in text_lower for w in ["=", "equal", "is", "in", "live", "from", "named", "called", "who live"]): 
            analysis["comparisons"].append("equal")

        # HAVING hints: "more than N orders", "more than N000 in total"
        having_match = re.search(r"(more than|fewer than|less than|greater than|over|above|under|below)\s+(\d[\d,]*)", text_lower)
        if having_match:
            op_word = having_match.group(1)
            val = having_match.group(2).replace(",", "")
            op = ">" if any(w in op_word for w in ["more", "greater", "over", "above"]) else "<"
            analysis["having_hint"] = {"op": op, "value": val}

        # Temporal extraction (year/month from order_date, etc.)
        if "each year" in text_lower or "per year" in text_lower or "every year" in text_lower:
            analysis["temporal"] = "year"
        elif "each month" in text_lower or "per month" in text_lower or "every month" in text_lower:
            analysis["temporal"] = "month"

        return analysis
