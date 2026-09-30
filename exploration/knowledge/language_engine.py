from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class LanguageAnalysis:
    text: str
    normalized: str
    tokens: List[str]
    sentences: List[str]
    question_type: str
    keywords: List[str]


class LanguageCapabilityEngine:
    VERSION = "language-capability.v1"

    _stop_words = {
        "a",
        "an",
        "the",
        "is",
        "are",
        "was",
        "were",
        "am",
        "be",
        "been",
        "being",
        "of",
        "to",
        "in",
        "on",
        "at",
        "for",
        "from",
        "with",
        "by",
        "and",
        "or",
        "but",
        "if",
        "then",
        "as",
        "it",
        "this",
        "that",
        "these",
        "those",
        "do",
        "does",
        "did",
        "can",
        "could",
        "would",
        "should",
        "will",
        "what",
        "who",
        "where",
        "when",
        "why",
        "how",
        "which",
    }

    def normalize(
        self,
        text: str,
    ) -> str:
        return " ".join(
            text.strip().lower().split()
        )

    def tokenize(
        self,
        text: str,
    ) -> List[str]:
        return re.findall(
            r"\b[\w'-]+\b",
            text,
            flags=re.UNICODE,
        )

    def split_sentences(
        self,
        text: str,
    ) -> List[str]:
        return [
            sentence.strip()
            for sentence in re.split(
                r"(?<=[.!?])\s+",
                text.strip(),
            )
            if sentence.strip()
        ]

    def question_type(
        self,
        text: str,
    ) -> str:
        normalized = self.normalize(text)

        if not normalized:
            return "unknown"

        if normalized.startswith("what "):
            return "what"

        if normalized.startswith("who "):
            return "who"

        if normalized.startswith("where "):
            return "where"

        if normalized.startswith("when "):
            return "when"

        if normalized.startswith("why "):
            return "why"

        if normalized.startswith("how "):
            return "how"

        if normalized.startswith("which "):
            return "which"

        if text.strip().endswith("?"):
            return "question"

        return "statement"

    def keywords(
        self,
        tokens: List[str],
    ) -> List[str]:
        result = []
        seen = set()

        for token in tokens:
            key = token.lower()

            if (
                len(key) <= 1
                or key in self._stop_words
            ):
                continue

            if key in seen:
                continue

            seen.add(key)
            result.append(token)

        return result

    def analyze(
        self,
        text: str,
    ) -> LanguageAnalysis:
        normalized = self.normalize(text)
        tokens = self.tokenize(text)

        return LanguageAnalysis(
            text=text,
            normalized=normalized,
            tokens=tokens,
            sentences=self.split_sentences(text),
            question_type=self.question_type(text),
            keywords=self.keywords(tokens),
        )

    def can_handle(
        self,
        query: str,
    ) -> bool:
        return bool(
            self.tokenize(query.strip())
        )
