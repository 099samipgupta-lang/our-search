from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class SemanticRepresentation:
    concept: str
    attributes: Set[str] = field(
        default_factory=set
    )
    relations: Set[str] = field(
        default_factory=set
    )
    aliases: Set[str] = field(
        default_factory=set
    )
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeSemanticEngine:
    VERSION = "knowledge-semantic.v1"

    def __init__(self):
        self.representations: Dict[
            str, SemanticRepresentation
        ] = {}

    def add(
        self,
        concept: str,
        attributes: Optional[
            List[str]
        ] = None,
        relations: Optional[
            List[str]
        ] = None,
        aliases: Optional[
            List[str]
        ] = None,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> SemanticRepresentation:
        key = concept.lower().strip()

        representation = SemanticRepresentation(
            concept=concept.strip(),
            attributes=set(
                attributes or []
            ),
            relations=set(
                relations or []
            ),
            aliases=set(
                aliases or []
            ),
            metadata=dict(metadata or {}),
        )

        self.representations[key] = (
            representation
        )

        return representation

    def get(
        self,
        concept: str,
    ) -> Optional[SemanticRepresentation]:
        return self.representations.get(
            concept.lower().strip()
        )

    def similarity(
        self,
        first: str,
        second: str,
    ) -> float:
        left = self.get(first)
        right = self.get(second)

        if left is None or right is None:
            return 0.0

        left_terms = (
            left.attributes
            | left.relations
            | left.aliases
        )

        right_terms = (
            right.attributes
            | right.relations
            | right.aliases
        )

        if not left_terms and not right_terms:
            return (
                1.0
                if first.lower().strip()
                == second.lower().strip()
                else 0.0
            )

        union = left_terms | right_terms

        if not union:
            return 0.0

        intersection = (
            left_terms & right_terms
        )

        return len(intersection) / len(
            union
        )

    def related(
        self,
        concept: str,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        results = []

        for key, representation in (
            self.representations.items()
        ):
            if key == concept.lower().strip():
                continue

            score = self.similarity(
                concept,
                representation.concept,
            )

            if score > 0:
                results.append(
                    {
                        "concept": (
                            representation.concept
                        ),
                        "similarity": score,
                    }
                )

        results.sort(
            key=lambda item: item[
                "similarity"
            ],
            reverse=True,
        )

        return results[
            :max(0, int(limit))
        ]

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "representation_count": len(
                self.representations
            ),
        }


__all__ = [
    "SemanticRepresentation",
    "KnowledgeSemanticEngine",
]
