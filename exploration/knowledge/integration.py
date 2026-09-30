from __future__ import annotations

from typing import Any, Dict, Optional

from exploration.knowledge.brain import KnowledgeBrain


class KnowledgeBrainIntegration:
    VERSION = "knowledge-integration.v1"

    def __init__(
        self,
        knowledge_brain: Optional[KnowledgeBrain] = None,
    ):
        self.knowledge_brain = (
            knowledge_brain
            if knowledge_brain is not None
            else KnowledgeBrain()
        )

    def answer(
        self,
        query: str,
        limit: int = 8,
    ) -> Dict[str, Any]:
        result = self.knowledge_brain.think(
            query=query,
            limit=limit,
        )

        answer = result.get("answer", {})

        return {
            "version": self.VERSION,
            "query": query,
            "answer": answer.get("answer", ""),
            "confidence": answer.get(
                "confidence",
                0.0,
            ),
            "grounded": answer.get(
                "grounded",
                False,
            ),
            "intent": answer.get(
                "intent",
                "unknown",
            ),
            "question": result.get(
                "question",
                {},
            ),
            "knowledge": result.get(
                "knowledge",
                [],
            ),
            "reasoning": result.get(
                "reasoning",
                [],
            ),
            "memory": result.get(
                "memory",
                {},
            ),
        }

    def learn_fact(
        self,
        subject: str,
        predicate: str,
        value: str,
        category: str = "",
        source: str = "internal",
        confidence: float = 1.0,
    ) -> None:
        self.knowledge_brain.remember_fact(
            subject=subject,
            predicate=predicate,
            value=value,
            category=category,
            source=source,
            confidence=confidence,
        )

    def learn_concept(
        self,
        name: str,
        definition: str,
        category: str = "",
        source: str = "internal",
        confidence: float = 1.0,
    ) -> None:
        self.knowledge_brain.remember_concept(
            name=name,
            definition=definition,
            category=category,
            source=source,
            confidence=confidence,
        )

    def stats(self) -> Dict[str, Any]:
        return self.knowledge_brain.stats()
