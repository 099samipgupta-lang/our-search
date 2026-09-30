from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class AttentionItem:
    kind: str
    value: Any
    score: float
    reason: str = ""


class KnowledgeAttentionEngine:
    VERSION = "knowledge-attention.v1"

    def __init__(self):
        self.weights = {
            "concept": 1.0,
            "fact": 1.0,
            "relation": 0.9,
            "context": 0.8,
            "semantic": 0.9,
        }

    def _terms(self, text: str) -> List[str]:
        return [
            word.lower()
            for word in text.split()
            if word.strip()
        ]

    def _overlap(
        self,
        query_terms: List[str],
        value: Any,
    ) -> float:
        text = str(value).lower()

        if not query_terms:
            return 0.0

        matches = sum(
            1
            for term in query_terms
            if term in text
        )

        return matches / len(query_terms)

    def score(
        self,
        query: str,
        kind: str,
        value: Any,
    ) -> float:
        query_terms = self._terms(query)

        relevance = self._overlap(
            query_terms,
            value,
        )

        base_weight = self.weights.get(
            kind,
            0.5,
        )

        score = relevance * base_weight

        return min(
            1.0,
            max(0.0, score),
        )

    def attend(
        self,
        query: str,
        meaning: Dict[str, Any],
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        candidates: List[AttentionItem] = []

        for concept in meaning.get(
            "concepts",
            [],
        ):
            candidates.append(
                AttentionItem(
                    kind="concept",
                    value=concept,
                    score=self.score(
                        query,
                        "concept",
                        concept,
                    ),
                    reason="query relevance",
                )
            )

        for fact in meaning.get(
            "facts",
            [],
        ):
            candidates.append(
                AttentionItem(
                    kind="fact",
                    value=fact,
                    score=self.score(
                        query,
                        "fact",
                        fact,
                    ),
                    reason="fact relevance",
                )
            )

        for relation in meaning.get(
            "relations",
            [],
        ):
            candidates.append(
                AttentionItem(
                    kind="relation",
                    value=relation,
                    score=self.score(
                        query,
                        "relation",
                        relation,
                    ),
                    reason="relationship relevance",
                )
            )

        context = meaning.get(
            "context",
            {},
        )

        for key, value in context.items():
            candidates.append(
                AttentionItem(
                    kind="context",
                    value={
                        "key": key,
                        "value": value,
                    },
                    score=self.score(
                        query,
                        "context",
                        value,
                    ),
                    reason="context relevance",
                )
            )

        semantics = meaning.get(
            "semantics",
            {},
        )

        for key, value in semantics.items():
            candidates.append(
                AttentionItem(
                    kind="semantic",
                    value={
                        "key": key,
                        "value": value,
                    },
                    score=self.score(
                        query,
                        "semantic",
                        value,
                    ),
                    reason="semantic relevance",
                )
            )

        candidates.sort(
            key=lambda item: item.score,
            reverse=True,
        )

        return [
            {
                "kind": item.kind,
                "value": item.value,
                "score": item.score,
                "reason": item.reason,
            }
            for item in candidates[
                :max(0, int(limit))
            ]
        ]

    def focus(
        self,
        query: str,
        meaning: Dict[str, Any],
    ) -> Dict[str, Any]:
        attended = self.attend(
            query=query,
            meaning=meaning,
        )

        return {
            "query": query,
            "items": attended,
            "count": len(attended),
            "focused": bool(attended),
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "weight_count": len(
                self.weights
            ),
        }


__all__ = [
    "AttentionItem",
    "KnowledgeAttentionEngine",
]
