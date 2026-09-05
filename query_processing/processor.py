from dataclasses import dataclass, field

from search_index.tokenizer import Tokenizer
from query_processing.parser import QueryParser


@dataclass
class ProcessedQuery:
    original: str
    terms: list = field(default_factory=list)
    phrases: list = field(default_factory=list)
    operators: list = field(default_factory=list)
    filters: dict = field(default_factory=dict)
    tokens: list = field(default_factory=list)

    def to_dict(self):
        return {
            "original": self.original,
            "terms": list(self.terms),
            "phrases": list(self.phrases),
            "operators": list(self.operators),
            "filters": dict(self.filters),
            "tokens": [
                {
                    "value": token.value,
                    "token_type": token.token_type,
                    "position": token.position,
                }
                for token in self.tokens
            ],
        }


class QueryProcessor:
    def __init__(self, parser=None, tokenizer=None):
        self.parser = parser if parser is not None else QueryParser()
        self.tokenizer = tokenizer if tokenizer is not None else Tokenizer()

    def _normalize_term(self, value):
        normalized = self.tokenizer.normalize_text(value)
        tokens = self.tokenizer.tokenize(normalized)

        if not tokens:
            return None

        return tokens[0]

    def _extract_phrase_terms(self, value):
        phrase = value[1:-1]

        return self.tokenizer.tokenize(phrase)

    def process(self, query):
        if not isinstance(query, str):
            raise TypeError("query must be a string")

        tokens = self.parser.parse(query)

        processed = ProcessedQuery(
            original=query,
            tokens=tokens,
        )

        seen_terms = set()
        seen_phrases = set()

        for token in tokens:

            if token.token_type == "TERM":
                normalized = self._normalize_term(token.value)

                if normalized and normalized not in seen_terms:
                    processed.terms.append(normalized)
                    seen_terms.add(normalized)

            elif token.token_type == "PHRASE":
                phrase_terms = self._extract_phrase_terms(token.value)

                if phrase_terms:
                    phrase = tuple(phrase_terms)

                    if phrase not in seen_phrases:
                        processed.phrases.append(
                            list(phrase_terms)
                        )
                        seen_phrases.add(phrase)

            elif token.token_type in ("AND", "OR", "NOT"):
                processed.operators.append(
                    token.token_type
                )

        return processed

    def process_dict(self, query):
        return self.process(query).to_dict()
