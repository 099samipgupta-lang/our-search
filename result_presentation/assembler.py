from result_presentation.snippet import SnippetGenerator


class ResultAssembler:
    def __init__(
        self,
        document_store,
        snippet_generator=None,
    ):
        self.document_store = document_store

        self.snippet_generator = (
            snippet_generator
            if snippet_generator is not None
            else SnippetGenerator()
        )

    def assemble(
        self,
        ranked_results,
        query_terms,
        total_candidates=None,
    ):
        results = []

        for rank, ranked in enumerate(
            ranked_results,
            start=1,
        ):
            document_id = ranked["document_id"]

            document = self.document_store.get(
                document_id
            )

            if document is None:
                continue

            text = document.get("text", "")

            snippet = self.snippet_generator.generate(
                text,
                query_terms,
            )

            highlighted = (
                self.snippet_generator.highlight(
                    snippet,
                    query_terms,
                )
            )

            results.append(
                {
                    "rank": rank,
                    "document_id": document_id,
                    "title": document.get(
                        "title",
                        "",
                    ),
                    "url": document.get(
                        "url",
                        "",
                    ),
                    "snippet": snippet,
                    "highlighted_snippet": highlighted,
                    "score": float(
                        ranked.get("score", 0.0)
                    ),
                    "matched_terms": list(
                        ranked.get(
                            "matched_terms",
                            [],
                        )
                    ),
                    "document_length": int(
                        ranked.get(
                            "document_length",
                            0,
                        )
                    ),
                }
            )

        if total_candidates is None:
            total_candidates = len(
                ranked_results
            )

        return {
            "total_candidates": int(
                total_candidates
            ),
            "results": results,
        }
