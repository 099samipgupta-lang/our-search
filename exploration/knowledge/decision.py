from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgeDecisionOption:
    option_id: str
    description: str
    score: float
    confidence: float
    evidence_ids: List[str] = field(
        default_factory=list
    )
    constraints: List[str] = field(
        default_factory=list
    )
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class KnowledgeDecision:
    decision_id: str
    question: str
    selected_option: Optional[str]
    confidence: float
    options: List[KnowledgeDecisionOption]


class KnowledgeDecisionEngine:
    VERSION = "knowledge-decision.v1"

    def __init__(self):
        self.decisions: Dict[
            str, KnowledgeDecision
        ] = {}

    def add_option(
        self,
        decision_id: str,
        option_id: str,
        description: str,
        score: float = 0.0,
        confidence: float = 0.0,
        evidence_ids: Optional[
            List[str]
        ] = None,
        constraints: Optional[
            List[str]
        ] = None,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeDecisionOption:
        decision = self.decisions.get(
            decision_id
        )

        if decision is None:
            decision = KnowledgeDecision(
                decision_id=decision_id,
                question="",
                selected_option=None,
                confidence=0.0,
                options=[],
            )
            self.decisions[decision_id] = decision

        option = KnowledgeDecisionOption(
            option_id=option_id,
            description=description.strip(),
            score=float(score),
            confidence=max(
                0.0,
                min(1.0, float(confidence)),
            ),
            evidence_ids=list(
                evidence_ids or []
            ),
            constraints=list(
                constraints or []
            ),
            metadata=dict(metadata or {}),
        )

        decision.options.append(option)
        return option

    def create(
        self,
        decision_id: str,
        question: str,
        options: Optional[
            List[Dict[str, Any]]
        ] = None,
    ) -> KnowledgeDecision:
        decision = KnowledgeDecision(
            decision_id=decision_id,
            question=question.strip(),
            selected_option=None,
            confidence=0.0,
            options=[],
        )

        self.decisions[decision_id] = decision

        for item in options or []:
            self.add_option(
                decision_id=decision_id,
                option_id=str(
                    item.get("option_id", "")
                ),
                description=str(
                    item.get(
                        "description",
                        "",
                    )
                ),
                score=float(
                    item.get("score", 0.0)
                ),
                confidence=float(
                    item.get(
                        "confidence",
                        0.0,
                    )
                ),
                evidence_ids=list(
                    item.get(
                        "evidence_ids",
                        [],
                    )
                ),
                constraints=list(
                    item.get(
                        "constraints",
                        [],
                    )
                ),
                metadata=dict(
                    item.get(
                        "metadata",
                        {},
                    )
                ),
            )

        return decision

    def evaluate(
        self,
        decision_id: str,
    ) -> Optional[KnowledgeDecision]:
        decision = self.decisions.get(
            decision_id
        )

        if decision is None:
            return None

        valid_options = [
            option
            for option in decision.options
            if not option.constraints
        ]

        if not valid_options:
            decision.selected_option = None
            decision.confidence = 0.0
            return decision

        selected = max(
            valid_options,
            key=lambda option: (
                option.score,
                option.confidence,
            ),
        )

        decision.selected_option = (
            selected.option_id
        )
        decision.confidence = (
            selected.confidence
        )

        return decision

    def get(
        self,
        decision_id: str,
    ) -> Optional[KnowledgeDecision]:
        return self.decisions.get(
            decision_id
        )

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "decision_count": len(
                self.decisions
            ),
        }


__all__ = [
    "KnowledgeDecisionOption",
    "KnowledgeDecision",
    "KnowledgeDecisionEngine",
]
