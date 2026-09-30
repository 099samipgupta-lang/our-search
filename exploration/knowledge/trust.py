from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from exploration.knowledge.confidence import (
    ConfidenceAssessment,
    KnowledgeConfidenceEngine,
)
from exploration.knowledge.provenance import (
    KnowledgeProvenanceEngine,
)
from exploration.knowledge.verification import (
    KnowledgeVerificationCoordinator,
)


@dataclass
class KnowledgeTrustAssessment:
    knowledge_id: str
    trusted: bool
    confidence: ConfidenceAssessment
    verification: Dict[str, Any]
    provenance: Dict[str, Any]


class KnowledgeTrustEngine:
    VERSION = "knowledge-trust.v1"

    def __init__(
        self,
        verification: Optional[
            KnowledgeVerificationCoordinator
        ] = None,
        provenance: Optional[
            KnowledgeProvenanceEngine
        ] = None,
        confidence: Optional[
            KnowledgeConfidenceEngine
        ] = None,
    ):
        self.verification = (
            verification
            if verification is not None
            else KnowledgeVerificationCoordinator()
        )
        self.provenance = (
            provenance
            if provenance is not None
            else KnowledgeProvenanceEngine()
        )
        self.confidence = (
            confidence
            if confidence is not None
            else KnowledgeConfidenceEngine()
        )

    def assess_fact(
        self,
        knowledge_id: str,
        fact: Dict[str, Any],
        source_type: str = "unknown",
        validation_score: float = 1.0,
        inferred: bool = False,
    ) -> KnowledgeTrustAssessment:
        verification_result = self.verification.verify_fact(
            fact
        )

        provenance_record = self.provenance.get(
            knowledge_id
        )

        confidence = self.confidence.assess(
            knowledge_id=knowledge_id,
            source_type=source_type,
            validation_score=validation_score,
            provenance_available=(
                provenance_record is not None
            ),
            inferred=inferred,
        )

        verification_data = {
            "status": getattr(
                verification_result,
                "status",
                "unknown",
            ),
            "valid": getattr(
                verification_result,
                "valid",
                False,
            ),
        }

        provenance_data = {
            "available": provenance_record is not None,
            "source_id": (
                provenance_record.source_id
                if provenance_record
                else None
            ),
            "source_type": (
                provenance_record.source_type
                if provenance_record
                else None
            ),
        }

        trusted = (
            verification_data["valid"]
            and confidence.score >= 0.60
        )

        return KnowledgeTrustAssessment(
            knowledge_id=knowledge_id,
            trusted=trusted,
            confidence=confidence,
            verification=verification_data,
            provenance=provenance_data,
        )

    def should_use(
        self,
        assessment: KnowledgeTrustAssessment,
        minimum_confidence: float = 0.60,
    ) -> bool:
        return (
            assessment.trusted
            and assessment.confidence.score
            >= minimum_confidence
        )

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "verification": self.verification.stats(),
            "provenance": self.provenance.stats(),
            "confidence": self.confidence.stats(),
        }


__all__ = [
    "KnowledgeTrustAssessment",
    "KnowledgeTrustEngine",
]
