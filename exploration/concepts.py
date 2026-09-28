import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Concept:
    name: str
    source_count: int
    relevance: float


class ConceptSystem:
    """
    Builds a structured concept map from information already
    retrieved by OUR SEARCH.

    This is a computational knowledge-organization machine,
    not a replacement for the search engine.
    """

    STOP_WORDS = {
        "the",
        "and",
        "for",
        "with",
        "that",
        "this",
        "from",
        "what",
        "when",
        "where",
        "which",
        "into",
        "about",
        "have",
        "has",
        "are",
        "was",
        "were",
        "how",
        "why",
    }

    def _words(self, value):
        if not isinstance(value, str):
            return []

        return [
            word
            for word in re.findall(
                r"[a-z0-9]+",
                value.casefold(),
            )
            if len(word) > 2
            and word not in self.STOP_WORDS
        ]

    def build(
        self,
        query,
        sources,
        limit=12,
    ):
        query_words = set(
            self._words(query)
        )

        counts = {}

        for source in sources:
            if not isinstance(source, dict):
                continue

            text = " ".join(
                [
                    str(source.get("title", "")),
                    str(source.get("snippet", "")),
                ]
            )

            words = set(
                self._words(text)
            )

            for word in words:
                counts[word] = (
                    counts.get(word, 0) + 1
                )

        concepts = []

        total_sources = max(
            len(sources),
            1,
        )

        for word, count in counts.items():
            relevance = count / total_sources

            if word in query_words:
                relevance += 0.5

            concepts.append(
                Concept(
                    name=word,
                    source_count=count,
                    relevance=round(
                        relevance,
                        6,
                    ),
                )
            )

        concepts.sort(
            key=lambda item: (
                item.relevance,
                item.source_count,
                item.name,
            ),
            reverse=True,
        )

        return concepts[:max(0, int(limit))]


__all__ = [
    "Concept",
    "ConceptSystem",
]
