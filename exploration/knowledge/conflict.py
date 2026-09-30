from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgeConflict:
    subject: str
    predicate: str
    first_value: str
    second_value: str
    first_confidence: float
    second_confidence: float


@dataclass
class ConflictResolution:
    status: str
    selected_value: Optional[str]
    confidence: float
    reason: str
    candidates: List[Dict[str, Any]]


class KnowledgeConflictResolutionEngine:
    VERSION = "knowledge-conflict.v1"

    def detect(
        self,
        facts: List[Dict[str, Any]],
    ) -> List[KnowledgeConflict]:
        groups: Dict[tuple[str, str], List[Dict[str, Any]]] = {}

        for fact in facts:
            subject = str(
                fact.get("subject", "")
            ).strip()

            predicate = str(
                fact.get("predicate", "")
            ).strip()

            if not subject or not predicate:
                continue

            key = (
                subject.lower(),
                predicate.lower(),
            )

            groups.setdefault(key, []).append(fact)

        conflicts: List[KnowledgeConflict] = []

        for group in groups.values():
            values = {}

            for fact in group:
                value = str(
                    fact.get("value", "")
                ).strip()

                if not value:
                    continue

                values.setdefault(
                    value.lower(),
                    [],
                ).append(fact)

            if len(values) <= 1:
                continue

            first_facts = next(
                iter(values.values())
            )
            second_group = list(values.values())[1]
            second_facts = second_group

            first = first_facts[0]
            second = second_facts[0]

            conflicts.append(
                KnowledgeConflict(
                    subject=str(
                        first.get("subject", "")
                    ),
                    predicate=str(
                        first.get("predicate", "")
                    ),
                    first_value=str(
                        first.get("value", "")
                    ),
                    second_value=str(
                        second.get("value", "")
                    ),
                    first_confidence=float(
                        first.get(
                            "confidence",
                            0.0,
                        )
                    ),
                    second_confidence=float(
                        second.get(
                            "confidence",
                            0.0,
                        )
                    ),
                )
            )

        return conflicts

    def resolve(
        self,
        conflict: KnowledgeConflict,
    ) -> ConflictResolution:
        first = conflict.first_confidence
        second = conflict.second_confidence

        candidates = [
            {
                "value": conflict.first_value,
                "confidence": first,
            },
            {
                "value": conflict.second_value,
                "confidence": second,
            },
        ]

        difference = abs(first - second)

        if difference < 0.05:
            return ConflictResolution(
                status="unresolved",
                selected_value=None,
                confidence=max(first, second),
                reason="Confidence values are too close to safely select one.",
                candidates=candidates,
            )

        if first > second:
            selected = conflict.first_value
            selected_confidence = first
        else:
            selected = conflict.second_value
            selected_confidence = second

        return ConflictResolution(
            status="resolved",
            selected_value=selected,
            confidence=selected_confidence,
            reason="Selected the candidate with higher confidence.",
            candidates=candidates,
        )

    def resolve_all(
        self,
        facts: List[Dict[str, Any]],
    ) -> List[ConflictResolution]:
        return [
            self.resolve(conflict)
            for conflict in self.detect(facts)
        ]

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
        }


__all__ = [
    "KnowledgeConflict",
    "ConflictResolution",
    "KnowledgeConflictResolutionEngine",
]
