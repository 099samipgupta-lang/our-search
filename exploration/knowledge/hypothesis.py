from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgeHypothesis:
    hypothesis_id: str
    statement: str
    confidence: float
    status: str
    supporting_ids: List[str] = field(
        default_factory=list
    )
    contradicting_ids: List[str] = field(
        default_factory=list
    )
    created_at: str = ""
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeHypothesisEngine:
    VERSION = "knowledge-hypothesis.v1"

    def __init__(self):
        self.hypotheses: Dict[
            str, KnowledgeHypothesis
        ] = {}

    def _now(self) -> str:
        return datetime.now(
            timezone.utc
        ).isoformat()

    def create(
        self,
        hypothesis_id: str,
        statement: str,
        confidence: float = 0.0,
        supporting_ids: Optional[
            List[str]
        ] = None,
        contradicting_ids: Optional[
            List[str]
        ] = None,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeHypothesis:
        confidence = max(
            0.0,
            min(1.0, float(confidence)),
        )

        hypothesis = KnowledgeHypothesis(
            hypothesis_id=hypothesis_id,
            statement=statement.strip(),
            confidence=confidence,
            status="open",
            supporting_ids=list(
                supporting_ids or []
            ),
            contradicting_ids=list(
                contradicting_ids or []
            ),
            created_at=self._now(),
            metadata=dict(metadata or {}),
        )

        self.hypotheses[
            hypothesis_id
        ] = hypothesis

        return hypothesis

    def support(
        self,
        hypothesis_id: str,
        knowledge_id: str,
        confidence_delta: float = 0.05,
    ) -> Optional[KnowledgeHypothesis]:
        hypothesis = self.hypotheses.get(
            hypothesis_id
        )

        if hypothesis is None:
            return None

        if knowledge_id not in hypothesis.supporting_ids:
            hypothesis.supporting_ids.append(
                knowledge_id
            )

        hypothesis.confidence = max(
            0.0,
            min(
                1.0,
                hypothesis.confidence
                + float(confidence_delta),
            ),
        )

        return hypothesis

    def contradict(
        self,
        hypothesis_id: str,
        knowledge_id: str,
        confidence_delta: float = 0.05,
    ) -> Optional[KnowledgeHypothesis]:
        hypothesis = self.hypotheses.get(
            hypothesis_id
        )

        if hypothesis is None:
            return None

        if (
            knowledge_id
            not in hypothesis.contradicting_ids
        ):
            hypothesis.contradicting_ids.append(
                knowledge_id
            )

        hypothesis.confidence = max(
            0.0,
            min(
                1.0,
                hypothesis.confidence
                - float(confidence_delta),
            ),
        )

        return hypothesis

    def resolve(
        self,
        hypothesis_id: str,
        accepted: bool,
    ) -> Optional[KnowledgeHypothesis]:
        hypothesis = self.hypotheses.get(
            hypothesis_id
        )

        if hypothesis is None:
            return None

        hypothesis.status = (
            "accepted"
            if accepted
            else "rejected"
        )

        return hypothesis

    def get(
        self,
        hypothesis_id: str,
    ) -> Optional[KnowledgeHypothesis]:
        return self.hypotheses.get(
            hypothesis_id
        )

    def open_hypotheses(
        self,
    ) -> List[KnowledgeHypothesis]:
        return [
            hypothesis
            for hypothesis
            in self.hypotheses.values()
            if hypothesis.status == "open"
        ]

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "hypothesis_count": len(
                self.hypotheses
            ),
            "open_count": len(
                self.open_hypotheses()
            ),
        }


__all__ = [
    "KnowledgeHypothesis",
    "KnowledgeHypothesisEngine",
]
