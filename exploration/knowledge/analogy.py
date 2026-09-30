from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgeAnalogy:
    analogy_id: str
    source_concept: str
    target_concept: str
    similarity: float
    shared_relations: List[str] = field(
        default_factory=list
    )
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeAnalogyEngine:
    VERSION = "knowledge-analogy.v1"

    def __init__(self):
        self.analogies: Dict[
            str, KnowledgeAnalogy
        ] = {}

    def add(
        self,
        analogy_id: str,
        source_concept: str,
        target_concept: str,
        similarity: float = 0.5,
        shared_relations: Optional[
            List[str]
        ] = None,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeAnalogy:
        analogy = KnowledgeAnalogy(
            analogy_id=analogy_id,
            source_concept=source_concept.strip(),
            target_concept=target_concept.strip(),
            similarity=max(
                0.0,
                min(1.0, float(similarity)),
            ),
            shared_relations=list(
                shared_relations or []
            ),
            metadata=dict(metadata or {}),
        )

        self.analogies[analogy_id] = analogy
        return analogy

    def get(
        self,
        analogy_id: str,
    ) -> Optional[KnowledgeAnalogy]:
        return self.analogies.get(
            analogy_id
        )

    def related_to(
        self,
        concept: str,
    ) -> List[KnowledgeAnalogy]:
        target = concept.lower().strip()

        return [
            analogy
            for analogy in self.analogies.values()
            if (
                analogy.source_concept.lower()
                == target
                or analogy.target_concept.lower()
                == target
            )
        ]

    def strongest(
        self,
        concept: str,
        limit: int = 5,
    ) -> List[KnowledgeAnalogy]:
        matches = self.related_to(concept)

        matches.sort(
            key=lambda item: item.similarity,
            reverse=True,
        )

        return matches[:max(0, int(limit))]

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "analogy_count": len(
                self.analogies
            ),
        }


__all__ = [
    "KnowledgeAnalogy",
    "KnowledgeAnalogyEngine",
]
