from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgeEvidence:
    evidence_id: str
    knowledge_id: str
    source_id: str
    evidence_type: str
    content: Any
    strength: float
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeEvidenceEngine:
    VERSION = "knowledge-evidence.v1"

    def __init__(self):
        self.evidence: Dict[
            str, KnowledgeEvidence
        ] = {}

    def add(
        self,
        evidence_id: str,
        knowledge_id: str,
        source_id: str,
        content: Any,
        evidence_type: str = "supporting",
        strength: float = 0.5,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeEvidence:
        strength = max(
            0.0,
            min(1.0, float(strength)),
        )

        item = KnowledgeEvidence(
            evidence_id=evidence_id,
            knowledge_id=knowledge_id,
            source_id=source_id,
            evidence_type=evidence_type,
            content=content,
            strength=strength,
            metadata=dict(metadata or {}),
        )

        self.evidence[evidence_id] = item
        return item

    def get(
        self,
        evidence_id: str,
    ) -> Optional[KnowledgeEvidence]:
        return self.evidence.get(evidence_id)

    def for_knowledge(
        self,
        knowledge_id: str,
    ) -> List[KnowledgeEvidence]:
        return [
            item
            for item in self.evidence.values()
            if item.knowledge_id == knowledge_id
        ]

    def supporting(
        self,
        knowledge_id: str,
    ) -> List[KnowledgeEvidence]:
        return [
            item
            for item in self.for_knowledge(
                knowledge_id
            )
            if item.evidence_type == "supporting"
        ]

    def contradicting(
        self,
        knowledge_id: str,
    ) -> List[KnowledgeEvidence]:
        return [
            item
            for item in self.for_knowledge(
                knowledge_id
            )
            if item.evidence_type == "contradicting"
        ]

    def aggregate_strength(
        self,
        knowledge_id: str,
    ) -> Dict[str, float]:
        supporting = sum(
            item.strength
            for item in self.supporting(
                knowledge_id
            )
        )

        contradicting = sum(
            item.strength
            for item in self.contradicting(
                knowledge_id
            )
        )

        return {
            "supporting": min(
                1.0,
                supporting,
            ),
            "contradicting": min(
                1.0,
                contradicting,
            ),
            "net": max(
                -1.0,
                min(
                    1.0,
                    supporting - contradicting,
                ),
            ),
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "evidence_count": len(
                self.evidence
            ),
        }


__all__ = [
    "KnowledgeEvidence",
    "KnowledgeEvidenceEngine",
]
