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
        text=None,
        title=None,
        url=None,
        domain=None,
        canonical_url=None,
        headings=None,
        navigation=None,
        footer=None,
        sidebar=None,
        anchor_text=None,
    ):

        if document_id in self.documents:

            self.remove_document(
                document_id
            )

        # Field-aware indexing.
        # Preserve the original combined-text behavior when only `text`
        # is supplied, while allowing title/URL/body-specific postings.
        if (
            title is not None
            or url is not None
            or domain is not None
            or canonical_url is not None
        ):
            fields = [
                ("title", title or ""),
                ("url", url or ""),
                ("domain", domain or ""),
                ("body", text or ""),
                ("canonical_url", canonical_url or ""),
                ("headings", headings or ""),
                ("navigation", navigation or ""),
                ("footer", footer or ""),
                ("sidebar", sidebar or ""),
                ("anchor_text", anchor_text or ""),
            ]

            positions = []
            field_positions = {}

            global_position = 0

            for field_name, field_text in fields:
                field_tokens = (
                    self.tokenizer
                    .tokenize_with_positions(field_text)
                )

                field_positions[field_name] = {}

                for item in field_tokens:
                    term = item["term"]
                    position = global_position + item["position"]

                    field_positions[field_name].setdefault(
                        term,
                        []
                    ).append(position)

                    positions.append({
                        "term": term,
                        "position": position,
                        "field": field_name,
                    })

                global_position += len(field_tokens)

            term_positions = defaultdict(list)

            for item in positions:
                term_positions[
                    item["term"]
                ].append(
                    item["position"]
                )
        else:
            positions = (
                self.tokenizer
                .tokenize_with_positions(text or "")
            )

            term_positions = defaultdict(list)

            for item in positions:
                term_positions[
                    item["term"]
                ].append(
                    item["position"]
                )
            field_positions = {}

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
                len(term_positions),

            "fields":
                field_positions,
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
