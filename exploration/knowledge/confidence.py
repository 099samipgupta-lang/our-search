from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ConfidenceAssessment:
    knowledge_id: str
    score: float
    level: str
    reasons: list[str]


class KnowledgeConfidenceEngine:
    VERSION = "knowledge-confidence.v1"

    SOURCE_WEIGHTS = {
        "internal": 0.95,
        "trusted": 0.90,
        "learned": 0.75,
        "external": 0.70,
        "inferred": 0.55,
        "unknown": 0.35,
    }

    def assess(
        self,
        knowledge_id: str,
        source_type: str = "unknown",
        validation_score: float = 1.0,
        provenance_available: bool = False,
        inferred: bool = False,
    ) -> ConfidenceAssessment:
        source_score = self.SOURCE_WEIGHTS.get(
            source_type,
            self.SOURCE_WEIGHTS["unknown"],
        )

        validation_score = max(
            0.0,
            min(1.0, float(validation_score)),
        )

        score = (
            source_score * 0.50
            + validation_score * 0.35
            + (0.15 if provenance_available else 0.0)
        )

        if inferred:
            score *= 0.85

        score = max(0.0, min(1.0, score))

        if score >= 0.90:
            level = "very_high"
        elif score >= 0.75:
            level = "high"
        elif score >= 0.60:
            level = "moderate"
        elif score >= 0.40:
            level = "low"
        else:
            level = "very_low"

        reasons = [
            f"source_type={source_type}",
            f"validation_score={validation_score:.3f}",
            f"provenance_available={provenance_available}",
            f"inferred={inferred}",
        ]

        return ConfidenceAssessment(
            knowledge_id=knowledge_id,
            score=score,
            level=level,
            reasons=reasons,
        )

    def compare(
        self,
        first: ConfidenceAssessment,
        second: ConfidenceAssessment,
    ) -> Dict[str, Any]:
        if first.score > second.score:
            relation = "first_higher"
        elif second.score > first.score:
            relation = "second_higher"
        else:
            relation = "equal"

        return {
            "relation": relation,
            "first": first.score,
            "second": second.score,
            "difference": abs(first.score - second.score),
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "source_types": len(self.SOURCE_WEIGHTS),
        }


__all__ = [
    "ConfidenceAssessment",
    "KnowledgeConfidenceEngine",
]
