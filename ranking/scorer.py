import math


class RankingScorer:
    """
    OUR SEARCH Stage 7 relevance scorer.

    Core scoring is BM25 with additional deterministic relevance
    signals for query coverage and positional proximity.
    """

    def __init__(self, k1=1.2, b=0.75):
        self.k1 = float(k1)
        self.b = float(b)

        if self.k1 < 0:
            raise ValueError("k1 must be non-negative")

        if not 0.0 <= self.b <= 1.0:
            raise ValueError("b must be between 0 and 1")

    def idf(self, document_count, document_frequency):
        document_count = int(document_count)
        document_frequency = int(document_frequency)

        if document_count <= 0:
            return 0.0

        if document_frequency <= 0:
            return 0.0

        if document_frequency > document_count:
            document_frequency = document_count

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
        average_document_length,
    ):
        term_frequency = int(term_frequency)
        document_frequency = int(document_frequency)
        document_count = int(document_count)
        document_length = max(0, int(document_length))
        average_document_length = float(average_document_length)

        if term_frequency <= 0:
            return 0.0

        if document_count <= 0:
            return 0.0

        if average_document_length <= 0:
            average_document_length = 1.0

        idf_value = self.idf(
            document_count,
            document_frequency,
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
                self.b * length_ratio
            )
        )

        if denominator <= 0.0:
            return 0.0

        return (
            idf_value
            *
            (
                term_frequency
                *
                (self.k1 + 1.0)
                /
                denominator
            )
        )

    def coverage_bonus(self, matched_terms, query_terms):
        unique_query_terms = set(query_terms)

        if not unique_query_terms:
            return 0.0

        matched = set(matched_terms)

        coverage = (
            len(matched & unique_query_terms)
            /
            len(unique_query_terms)
        )

        return coverage

    def proximity_bonus(self, positions):
        """
        Backward-compatible positional bonus.

        Used when a caller supplies a single combined position list.
        """
        if len(positions) < 2:
            return 0.0

        ordered = sorted(int(position) for position in positions)

        gaps = []

        for index in range(1, len(ordered)):
            gaps.append(
                ordered[index] - ordered[index - 1]
            )

        if not gaps:
            return 0.0

        average_gap = (
            sum(gaps)
            /
            len(gaps)
        )

        return 1.0 / (1.0 + average_gap)

    def cross_term_proximity_bonus(
        self,
        positions_by_term,
        query_terms,
    ):
        """
        Rewards documents where different query terms occur close together.

        The score is based on the smallest positional window containing
        every matched query term. Documents missing query terms receive
        no proximity bonus.
        """
        unique_terms = list(dict.fromkeys(query_terms))

        if len(unique_terms) < 2:
            return 0.0

        occurrences = []

        for term in unique_terms:
            positions = positions_by_term.get(term, [])

            if not positions:
                return 0.0

            for position in positions:
                occurrences.append(
                    (int(position), term)
                )

        if len(occurrences) < len(unique_terms):
            return 0.0

        occurrences.sort(key=lambda item: item[0])

        required = set(unique_terms)
        counts = {}
        covered = 0
        left = 0
        best_span = None

        for right in range(len(occurrences)):
            position, term = occurrences[right]

            previous = counts.get(term, 0)
            counts[term] = previous + 1

            if previous == 0:
                covered += 1

            while covered == len(required) and left <= right:
                left_position, left_term = occurrences[left]

                span = position - left_position

                if best_span is None or span < best_span:
                    best_span = span

                counts[left_term] -= 1

                if counts[left_term] == 0:
                    covered -= 1

                left += 1

        if best_span is None:
            return 0.0

        return 1.0 / (1.0 + float(best_span))

    def title_score(
        self,
        query_terms,
        title,
    ):
        """
        Small title relevance signal.

        This is deliberately bounded so title matching improves ranking
        without overpowering textual relevance.
        """
        if not isinstance(title, str) or not title.strip():
            return 0.0

        normalized_title = title.casefold()

        unique_terms = list(dict.fromkeys(query_terms))

        if not unique_terms:
            return 0.0

        matched = sum(
            1
            for term in unique_terms
            if str(term).casefold() in normalized_title
        )

        if matched == 0:
            return 0.0

        coverage = matched / len(unique_terms)

        return 3.0 * coverage

    def exact_title_bonus(
        self,
        query,
        title,
    ):
        if not isinstance(query, str):
            return 0.0

        if not isinstance(title, str):
            return 0.0

        query_normalized = " ".join(
            query.casefold().split()
        )

        title_normalized = " ".join(
            title.casefold().split()
        )

        if not query_normalized or not title_normalized:
            return 0.0

        if query_normalized == title_normalized:
            return 5.0

        if query_normalized in title_normalized:
            return 1.5

        return 0.0

    def url_score(
        self,
        query_terms,
        url,
    ):
        if not isinstance(url, str) or not url:
            return 0.0

        normalized_url = url.casefold()

        unique_terms = list(dict.fromkeys(query_terms))

        if not unique_terms:
            return 0.0

        matched = sum(
            1
            for term in unique_terms
            if str(term).casefold() in normalized_url
        )

        if matched == 0:
            return 0.0

        return 1.5 * (
            matched / len(unique_terms)
        )
