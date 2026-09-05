from ranking.scorer import RankingScorer


class RankingEngine:

    def __init__(
        self,
        index_source,
        scorer=None
    ):

        self.index = index_source

        self.scorer = (
            scorer
            if scorer is not None
            else RankingScorer()
        )

    def _document_count(self):

        if hasattr(
            self.index,
            "document_count"
        ):

            return self.index.document_count()

        if hasattr(
            self.index,
            "total_documents"
        ):

            return self.index.total_documents()

        return 0

    def _document_info(
        self,
        document_id
    ):

        return self.index.get_document_info(
            document_id
        )

    def _average_document_length(self):

        document_count = (
            self._document_count()
        )

        if document_count <= 0:
            return 1.0

        total_length = 0.0

        for metadata in getattr(
            self.index,
            "segments",
            []
        ):

            segment = self.index.load_segment(
                metadata["segment_id"]
            )

            for info in segment.data.get(
                "documents",
                {}
            ).values():

                total_length += float(
                    info.get(
                        "length",
                        0
                    )
                )

        if total_length <= 0:
            return 1.0

        return (
            total_length
            /
            document_count
        )

    def score_document(
        self,
        document_id,
        query_terms,
        retrieval_result
    ):

        document_info = (
            self._document_info(
                document_id
            )
        )

        if document_info is None:
            return None

        document_count = (
            self._document_count()
        )

        average_length = (
            self._average_document_length()
        )

        document_length = int(
            document_info.get(
                "length",
                0
            )
        )

        matched_terms = retrieval_result.get(
            "matched_terms",
            []
        )

        positions_by_term = (
            retrieval_result.get(
                "positions",
                {}
            )
        )

        score = 0.0

        for term in query_terms:

            positions = positions_by_term.get(
                term,
                []
            )

            term_frequency = len(
                positions
            )

            document_frequency = (
                len(
                    self.index.get_postings(
                        term
                    )
                )
            )

            score += self.scorer.term_score(
                term_frequency,
                document_frequency,
                document_count,
                document_length,
                average_length
            )

        score += (
            self.scorer.coverage_bonus(
                matched_terms,
                query_terms
            )
        )

        all_positions = []

        for positions in (
            positions_by_term.values()
        ):

            all_positions.extend(
                positions
            )

        score += (
            self.scorer.proximity_bonus(
                all_positions
            )
        )

        return {
            "document_id":
                document_id,

            "score":
                score,

            "matched_terms":
                list(matched_terms),

            "document_length":
                document_length
        }

    def rank(
        self,
        query_terms,
        candidates
    ):

        results = []

        for document_id, retrieval_result in (
            candidates.items()
        ):

            scored = self.score_document(
                document_id,
                query_terms,
                retrieval_result
            )

            if scored is not None:
                results.append(
                    scored
                )

        results.sort(
            key=lambda item: (
                -item["score"],
                item["document_id"]
            )
        )

        return results
