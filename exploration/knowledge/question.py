from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class KnowledgeQuestion:
    original: str
    normalized: str
    intent: str
    entities: List[str]
    terms: List[str]


class KnowledgeQuestionUnderstandingEngine:
    VERSION = "knowledge-question.v1"

    QUESTION_WORDS = {
        "what",
        "who",
        "where",
        "when",
        "why",
        "how",
        "which",
        "whose",
        "is",
        "are",
        "does",
        "do",
        "can",
    }

    STOP_WORDS = {
        "a",
        "an",
        "the",
        "is",
        "are",
        "was",
        "were",
        "what",
        "who",
        "where",
        "when",
        "why",
        "how",
        "which",
        "does",
        "do",
        "can",
        "of",
        "in",
        "on",
        "to",
        "for",
        "and",
        "or",
        "with",
        "about",
    }

    def _normalize(self, text: str) -> str:
        return " ".join(
            text.strip()
            .lower()
            .replace("?", " ")
            .replace("!", " ")
            .replace(",", " ")
            .split()
        )

    def _intent(self, normalized: str) -> str:
        words = normalized.split()

        if not words:
            return "unknown"

        first = words[0]

        if first == "what":
            return "definition"

        if first == "who":
            return "person"

        if first == "where":
            return "location"

        if first == "when":
            return "time"

        if first == "why":
            return "cause"

        if first == "how":
            if len(words) > 1 and words[1] in {
                "does",
                "do",
                "can",
                "is",
                "are",
            }:
                return "explanation"
            return "method"

        if first == "which":
            return "selection"

        return "general"

    def _terms(self, normalized: str) -> List[str]:
        return [
            word
            for word in normalized.split()
            if word not in self.STOP_WORDS
            and len(word) > 1
        ]

    def _entities(self, text: str) -> List[str]:
        raw_words = text.strip().split()

        entities = []
        current = []

        for word in raw_words:
            clean = word.strip(
                ".,!?;:()[]{}\"'"
            )

            if not clean:
                continue

            if clean[0].isupper():
                current.append(clean)
            else:
                if current:
                    entities.append(
                        " ".join(current)
                    )
                    current = []

        if current:
            entities.append(
                " ".join(current)
            )

        unique = []
        seen = set()

        for entity in entities:
            key = entity.lower()

            if key not in seen:
                seen.add(key)
                unique.append(entity)

        return unique

    def understand(
        self,
        question: str,
    ) -> KnowledgeQuestion:
        original = question.strip()
        normalized = self._normalize(original)

        return KnowledgeQuestion(
            original=original,
            normalized=normalized,
            intent=self._intent(normalized),
            entities=self._entities(original),
            terms=self._terms(normalized),
        )
