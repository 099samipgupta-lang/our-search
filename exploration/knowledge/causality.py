from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class CausalRelation:
    relation_id: str
    cause: str
    effect: str
    strength: float
    evidence_ids: List[str] = field(
        default_factory=list
    )
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeCausalityEngine:
    VERSION = "knowledge-causality.v1"

    def __init__(self):
        self.relations: Dict[
            str, CausalRelation
        ] = {}

    def add(
        self,
        relation_id: str,
        cause: str,
        effect: str,
        strength: float = 0.5,
        evidence_ids: Optional[
            List[str]
        ] = None,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> CausalRelation:
        relation = CausalRelation(
            relation_id=relation_id,
            cause=cause.strip(),
            effect=effect.strip(),
            strength=max(
                0.0,
                min(1.0, float(strength)),
            ),
            evidence_ids=list(
                evidence_ids or []
            ),
            metadata=dict(metadata or {}),
        )

        self.relations[relation_id] = relation
        return relation

    def get(
        self,
        relation_id: str,
    ) -> Optional[CausalRelation]:
        return self.relations.get(
            relation_id
        )

    def causes(
        self,
        effect: str,
    ) -> List[CausalRelation]:
        target = effect.lower().strip()

        return [
            relation
            for relation in self.relations.values()
            if relation.effect.lower() == target
        ]

    def effects(
        self,
        cause: str,
    ) -> List[CausalRelation]:
        target = cause.lower().strip()

        return [
            relation
            for relation in self.relations.values()
            if relation.cause.lower() == target
        ]

    def chain(
        self,
        start: str,
        max_depth: int = 3,
    ) -> List[List[str]]:
        paths: List[List[str]] = []

        def walk(
            current: str,
            path: List[str],
            depth: int,
        ):
            if depth >= max_depth:
                paths.append(path)
                return

            next_relations = self.effects(
                current
            )

            if not next_relations:
                paths.append(path)
                return

            for relation in next_relations:
                if relation.effect in path:
                    continue

                walk(
                    relation.effect,
                    path + [relation.effect],
                    depth + 1,
                )

        walk(
            start,
            [start],
            0,
        )

        return paths

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "relation_count": len(
                self.relations
            ),
        }


__all__ = [
    "CausalRelation",
    "KnowledgeCausalityEngine",
]
