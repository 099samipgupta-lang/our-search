import math


class RankingScorer:

    def __init__(
        self,
        k1=1.2,
        b=0.75
    ):

        self.k1 = float(k1)
        self.b = float(b)

    def idf(
        self,
        document_count,
        document_frequency
    ):

        if document_count <= 0:
            return 0.0

        if document_frequency <= 0:
            return 0.0

        return math.log(
            1.0
            +
            (
                (document_count - document_frequency + 0.5)
                /
                (document_frequency + 0.5)
            )
        )

    def term_score(
        self,
        term_frequency,
        document_frequency,
        document_count,
        document_length,
        average_document_length
    ):

        if term_frequency <= 0:
            return 0.0

        if document_count <= 0:
            return 0.0

        if average_document_length <= 0:
            average_document_length = 1.0

        idf_value = self.idf(
            document_count,
            document_frequency
        )

        length_ratio = (
            document_length
            /
            average_document_length
        )

        denominator = (
            term_frequency
            +
            self.k1
            *
            (
                1.0
                -
                self.b
                +
                self.b
                *
                length_ratio
            )
        )

        if denominator <= 0:
            return 0.0

        return (
            idf_value
            *
            (
                (
                    term_frequency
                    *
                    (self.k1 + 1.0)
                )
                /
                denominator
            )
        )

    def coverage_bonus(
        self,
        matched_terms,
        query_terms
    ):

        if not query_terms:
            return 0.0

        unique_query_terms = set(
            query_terms
        )

        matched = set(
            matched_terms
        )

        coverage = (
            len(
                matched
                &
                unique_query_terms
            )
            /
            len(unique_query_terms)
        )

        return coverage

    def proximity_bonus(
        self,
        positions
    ):

        if len(positions) < 2:
            return 0.0

        ordered = sorted(
            positions
        )

        gaps = []

        for index in range(
            1,
            len(ordered)
        ):

            gaps.append(
                ordered[index]
                -
                ordered[index - 1]
            )

        if not gaps:
            return 0.0

        average_gap = (
            sum(gaps)
            /
            len(gaps)
        )

        return 1.0 / (
            1.0
            +
            average_gap
        )
