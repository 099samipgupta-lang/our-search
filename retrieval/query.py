from search_index.tokenizer import Tokenizer


class QueryParser:

    def __init__(self, tokenizer=None):

        self.tokenizer = (
            tokenizer
            if tokenizer is not None
            else Tokenizer()
        )

    def parse(self, query):

        if not isinstance(query, str):
            raise TypeError(
                "query must be a string"
            )

        return self.tokenizer.tokenize(
            query
        )

    def parse_unique(self, query):

        terms = self.parse(query)

        seen = set()
        result = []

        for term in terms:

            if term not in seen:

                seen.add(term)
                result.append(term)

        return result
