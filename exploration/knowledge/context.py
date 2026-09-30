from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgeContext:
    context_id: str
    topic: str
    entities: List[str] = field(
        default_factory=list
    )
    concepts: List[str] = field(
        default_factory=list
    )
    assumptions: List[str] = field(
        default_factory=list
    )
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeContextEngine:
    VERSION = "knowledge-context.v1"

    def __init__(self):
        self.contexts: Dict[
            str, KnowledgeContext
        ] = {}

    def create(
        self,
        context_id: str,
        topic: str = "",
        entities: Optional[
            List[str]
        ] = None,
        concepts: Optional[
            List[str]
        ] = None,
        assumptions: Optional[
            List[str]
        ] = None,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeContext:
        context = KnowledgeContext(
            context_id=context_id,
            topic=topic.strip(),
            entities=list(
                entities or []
            ),
            concepts=list(
                concepts or []
            ),
            assumptions=list(
                assumptions or []
            ),
            metadata=dict(metadata or {}),
        )

        self.contexts[context_id] = context
        return context

    def get(
        self,
        context_id: str,
    ) -> Optional[KnowledgeContext]:
        return self.contexts.get(
            context_id
        )

    def add_entity(
        self,
        context_id: str,
        entity: str,
    ) -> Optional[KnowledgeContext]:
        context = self.contexts.get(
            context_id
        )

        if context is None:
            return None

        if entity not in context.entities:
            context.entities.append(entity)

        return context

    def add_concept(
        self,
        context_id: str,
        concept: str,
    ) -> Optional[KnowledgeContext]:
        context = self.contexts.get(
            context_id
        )

        if context is None:
            return None

        if concept not in context.concepts:
            context.concepts.append(concept)

        return context

    def add_assumption(
        self,
        context_id: str,
        assumption: str,
    ) -> Optional[KnowledgeContext]:
        context = self.contexts.get(
            context_id
        )

        if context is None:
            return None

        if assumption not in context.assumptions:
            context.assumptions.append(
                assumption
            )

        return context

    def relevance(
        self,
        context_id: str,
        terms: List[str],
    ) -> float:
        context = self.contexts.get(
            context_id
        )

        if context is None:
            return 0.0

        context_terms = {
            item.lower().strip()
            for item in (
                [context.topic]
                + context.entities
                + context.concepts
            )
            if item.strip()
        }

        query_terms = {
            term.lower().strip()
            for term in terms
            if term.strip()
        }

        if not query_terms:
            return 0.0

        overlap = (
            context_terms & query_terms
        )

        return len(overlap) / len(
            query_terms
        )

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "context_count": len(
                self.contexts
            ),
        }


__all__ = [
    "KnowledgeContext",
    "KnowledgeContextEngine",
]
