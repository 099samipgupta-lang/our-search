from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgeUncertainty:
    knowledge_id: str
    probability: float
    status: str
    reasons: List[str] = field(
        default_factory=list
    )
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeUncertaintyEngine:
    VERSION = "knowledge-uncertainty.v1"

    STATUSES = {
        "certain",
        "high_confidence",
        "uncertain",
        "ambiguous",
        "unknown",
    }

    def assess(
        self,
        knowledge_id: str,
        probability: float,
        reasons: Optional[
            List[str]
        ] = None,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeUncertainty:
        probability = max(
            0.0,
            min(1.0, float(probability)),
        )

        if probability >= 0.95:
            status = "certain"
        elif probability >= 0.80:
            status = "high_confidence"
        elif probability >= 0.50:
            status = "uncertain"
        elif probability > 0.0:
            status = "ambiguous"
        else:
            status = "unknown"

        return KnowledgeUncertainty(
            knowledge_id=knowledge_id,
            probability=probability,
            status=status,
            reasons=list(reasons or []),
            metadata=dict(metadata or {}),
        )

    def compare(
        self,
        first: KnowledgeUncertainty,
        second: KnowledgeUncertainty,
    ) -> Dict[str, Any]:
        difference = (
            first.probability
            - second.probability
        )

        return {
            "first_probability": (
                first.probability
            ),
            "second_probability": (
                second.probability
            ),
            "difference": difference,
            "closer": (
                abs(difference) < 0.10
            ),
        }

    def combine(
        self,
        assessments: List[
            KnowledgeUncertainty
        ],
    ) -> Optional[
        KnowledgeUncertainty
    ]:
        if not assessments:
            return None

        probability = sum(
            item.probability
            for item in assessments
        ) / len(assessments)

        reasons: List[str] = []

        for item in assessments:
            reasons.extend(item.reasons)

        return self.assess(
            knowledge_id=(
                assessments[0].knowledge_id
            ),
            probability=probability,
            reasons=reasons,
        )

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "status_count": len(
                self.STATUSES
            ),
        }


__all__ = [
    "KnowledgeUncertainty",
    "KnowledgeUncertaintyEngine",
]
