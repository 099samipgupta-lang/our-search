import re
from urllib.parse import urlparse


class RelationshipEngine:
    def __init__(self):
        pass

    @staticmethod
    def _words(value):
        if not isinstance(value, str):
            return set()

        return {
            word
            for word in re.findall(
                r"[a-z0-9]+",
                value.casefold(),
            )
            if len(word) > 2
        }

    @classmethod
    def _document_terms(cls, document):
        values = [
            document.get("title", ""),
            document.get("snippet", ""),
            " ".join(
                document.get("matched_terms", [])
                if isinstance(document.get("matched_terms", []), list)
                else []
            ),
        ]

        terms = set()

        for value in values:
            terms.update(cls._words(value))

        return terms

    @staticmethod
    def _domain(document):
        url = document.get("url", "")

        if not isinstance(url, str):
            return ""

        try:
            return urlparse(url).netloc.casefold()
        except ValueError:
            return ""

    def score(self, source, candidate):
        source_terms = self._document_terms(source)
        candidate_terms = self._document_terms(candidate)

        if not source_terms or not candidate_terms:
            return 0.0

        intersection = source_terms & candidate_terms
        union = source_terms | candidate_terms

        lexical = (
            len(intersection) / len(union)
            if union
            else 0.0
        )

        source_domain = self._domain(source)
        candidate_domain = self._domain(candidate)

        domain_signal = (
            0.10
            if source_domain
            and candidate_domain
            and source_domain == candidate_domain
            else 0.0
        )

        matched_a = set(
            source.get("matched_terms", [])
            if isinstance(source.get("matched_terms", []), list)
            else []
        )

        matched_b = set(
            candidate.get("matched_terms", [])
            if isinstance(candidate.get("matched_terms", []), list)
            else []
        )

        matched_overlap = (
            len(matched_a & matched_b)
            / max(len(matched_a | matched_b), 1)
        )

        score = (
            lexical * 0.60
            + matched_overlap * 0.30
            + domain_signal
        )

        return max(
            0.0,
            min(1.0, score),
        )

    def related_results(
        self,
        source,
        documents,
        limit=10,
    ):
        candidates = []

        for document in documents:
            if not isinstance(document, dict):
                continue

            if (
                document.get("document_id")
                == source.get("document_id")
            ):
                continue

            score = self.score(
                source,
                document,
            )

            if score <= 0.0:
                continue

            item = dict(document)
            item["relationship_score"] = round(
                score,
                6,
            )

            candidates.append(item)

        candidates.sort(
            key=lambda item: (
                item["relationship_score"],
                -int(item.get("rank", 10**9)),
            ),
            reverse=True,
        )

        return candidates[:max(0, int(limit))]
