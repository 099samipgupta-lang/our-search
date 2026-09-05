class QueryPlanner:
    def __init__(self, index_source):
        self.index = index_source

    def estimate_document_frequency(self, term):
        method = getattr(
            self.index,
            "estimate_document_frequency",
            None
        )

        if method is not None:
            return method(term)

        postings = self.index.get_postings(term)

        return len(postings)

    def order_terms(self, terms):
        terms = list(terms)

        estimates = []

        for position, term in enumerate(terms):
            df = self.estimate_document_frequency(
                term
            )

            estimates.append(
                (
                    df,
                    position,
                    term
                )
            )

        estimates.sort(
            key=lambda item: (
                item[0],
                item[1]
            )
        )

        return [
            item[2]
            for item in estimates
        ]
