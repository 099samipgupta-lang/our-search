class PostingListStats:

    def __init__(self):
        self.stats = {}

    def build(self, postings):
        if not isinstance(postings, dict):
            raise TypeError(
                "postings must be a dictionary"
            )

        document_frequency = len(postings)

        total_positions = 0

        for positions in postings.values():
            total_positions += len(positions)

        average_positions = (
            total_positions / document_frequency
            if document_frequency
            else 0.0
        )

        return {
            "document_frequency":
                document_frequency,

            "total_positions":
                total_positions,

            "average_positions":
                average_positions,
        }

    def record(self, term, postings):
        self.stats[term] = self.build(postings)

    def get(self, term):
        return self.stats.get(term)

    def clear(self):
        self.stats.clear()

    def vocabulary_size(self):
        return len(self.stats)

    def get_state(self):
        return {
            term: dict(values)
            for term, values in self.stats.items()
        }

    def load_state(self, state):
        self.stats = {
            term: dict(values)
            for term, values in (state or {}).items()
        }
