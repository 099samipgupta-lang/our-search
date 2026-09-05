from collections import defaultdict

from search_index.tokenizer import Tokenizer


class InvertedIndex:

    def __init__(
        self,
        tokenizer=None
    ):

        self.tokenizer = (
            tokenizer
            if tokenizer is not None
            else Tokenizer()
        )

        self.index = defaultdict(dict)

        self.documents = {}

    def add_document(
        self,
        document_id,
        text
    ):

        if document_id in self.documents:

            self.remove_document(
                document_id
            )

        positions = (
            self.tokenizer
            .tokenize_with_positions(text)
        )

        term_positions = defaultdict(list)

        for item in positions:

            term_positions[
                item["term"]
            ].append(
                item["position"]
            )

        for term, term_positions_list in (
            term_positions.items()
        ):

            self.index[
                term
            ][document_id] = (
                term_positions_list
            )

        self.documents[
            document_id
        ] = {
            "length":
                len(positions),

            "terms":
                len(term_positions)
        }

    def remove_document(
        self,
        document_id
    ):

        if document_id not in self.documents:
            return False

        for term in list(
            self.index.keys()
        ):

            postings = self.index[
                term
            ]

            if document_id in postings:

                del postings[
                    document_id
                ]

            if not postings:

                del self.index[
                    term
                ]

        del self.documents[
            document_id
        ]

        return True

    def get_postings(
        self,
        term
    ):

        normalized = (
            self.tokenizer
            .normalize_text(term)
        )

        return dict(
            self.index.get(
                normalized,
                {}
            )
        )

    def document_frequency(
        self,
        term
    ):

        return len(
            self.get_postings(
                term
            )
        )

    def term_frequency(
        self,
        term,
        document_id
    ):

        postings = self.get_postings(
            term
        )

        positions = postings.get(
            document_id,
            []
        )

        return len(
            positions
        )

    def contains(
        self,
        term
    ):

        return (
            self.document_frequency(
                term
            ) > 0
        )

    def vocabulary_size(self):

        return len(
            self.index
        )

    def document_count(self):

        return len(
            self.documents
        )

    def get_document_info(
        self,
        document_id
    ):

        return self.documents.get(
            document_id
        )

    def get_state(self):

        return {
            "index":
                {
                    term: dict(postings)
                    for term, postings
                    in self.index.items()
                },

            "documents":
                dict(self.documents)
        }

    def load_state(
        self,
        state
    ):

        if not state:
            return

        self.index = defaultdict(
            dict,
            {
                term: dict(postings)
                for term, postings
                in state.get(
                    "index",
                    {}
                ).items()
            }
        )

        self.documents = dict(
            state.get(
                "documents",
                {}
            )
        )
