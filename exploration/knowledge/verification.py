from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from exploration.knowledge.validation import (
    KnowledgeValidationEngine,
)


@dataclass
class KnowledgeVerification:
    accepted: bool
    confidence: float
    status: str
    reasons: List[str]


class KnowledgeVerificationCoordinator:
    VERSION = "knowledge-verification.v1"

    def __init__(
        self,
        validator: KnowledgeValidationEngine | None = None,
    ):
        self.validator = (
            validator
            if validator is not None
            else KnowledgeValidationEngine()
        )

    def verify_fact(
        self,
        fact: Dict[str, Any],
    ) -> KnowledgeVerification:
        validation = self.validator.validate_fact(
            fact
        )

        confidence = float(
            fact.get("confidence", 0.0)
        )

        confidence = max(
            0.0,
            min(1.0, confidence),
        )

        reasons = [
            finding.message
            for finding in validation.findings
        ]

        if not validation.valid:
            return KnowledgeVerification(
                accepted=False,
                confidence=0.0,
                status="rejected",
                reasons=reasons,
            )

        if confidence >= 0.9:
            status = "high_confidence"
        elif confidence >= 0.6:
            status = "moderate_confidence"
        else:
            status = "low_confidence"

        return KnowledgeVerification(
            accepted=True,
            confidence=confidence,
            status=status,
            reasons=reasons,
        )

    def verify_batch(
        self,
        facts: Iterable[Dict[str, Any]],
    ) -> Dict[str, Any]:
        results = []
        accepted = 0
        rejected = 0

        for fact in facts:
            verification = self.verify_fact(
                fact
            )

            if verification.accepted:
                accepted += 1
            else:
                rejected += 1

            results.append(
                {
                    "fact": fact,
                    "verification": {
                        "accepted": (
                            verification.accepted
                        ),
                        "confidence": (
                            verification.confidence
                        ),
                        "status": (
                            verification.status
                        ),
                        "reasons": (
                            verification.reasons
                        ),
                    },
                }
            )

        return {
            "version": self.VERSION,
            "accepted": accepted,
            "rejected": rejected,
            "results": results,
        }

    def classify_source(
        self,
        source: str,
    ) -> str:
        value = source.strip().lower()

        if value == "internal":
            return "internal"

        if value in {
            "learned",
            "learning",
        }:
            return "learned"

        if value in {
            "inferred",
            "reasoning",
        }:
            return "inferred"

        if value in {
            "web",
            "website",
            "external",
        }:
            return "external"

        return "unknown"

    def stats(self) -> Dict[str, str]:
        return {
            "version": self.VERSION,
            "role": (
                "knowledge verification "
                "and confidence classification"
            ),
        }
