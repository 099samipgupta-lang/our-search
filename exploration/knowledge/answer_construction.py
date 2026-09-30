from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ConstructedAnswer:
    query: str
    answer: str
    key_points: List[str] = field(default_factory=list)
    evidence: List[Dict[str, Any]] = field(default_factory=list)
    reasoning: List[Dict[str, Any]] = field(default_factory=list)
    confidence: float = 0.0
    grounded: bool = False


class KnowledgeAnswerConstructionEngine:
    VERSION = "knowledge-answer-construction.v1"

    def _text(self, value: Any) -> str:
        if isinstance(value, dict):
            return " ".join(
                str(v)
                for v in value.values()
                if v is not None
            ).strip()
        return str(value).strip()

    def _sentence_from_fact(
        self,
        fact: Dict[str, Any],
    ) -> str:
        subject = str(
            fact.get("subject", "")
        ).strip()
        predicate = str(
            fact.get("predicate", "")
        ).strip()
        value = self._text(
            fact.get(
                "value",
                fact.get("object", ""),
            )
        )

        if not subject or not predicate or not value:
            return ""

        return (
            f"{subject} {predicate} {value}."
        )

    def _sentence_from_relation(
        self,
        relation: Dict[str, Any],
    ) -> str:
        subject = str(
            relation.get("subject", "")
        ).strip()
        predicate = str(
            relation.get("predicate", "")
        ).strip()
        object_value = self._text(
            relation.get(
                "object",
                relation.get("value", ""),
            )
        )

        if not subject or not predicate or not object_value:
            return ""

        return (
            f"{subject} {predicate} "
            f"{object_value}."
        )

    def _deduplicate(
        self,
        values: List[str],
    ) -> List[str]:
        seen = set()
        result = []

        for value in values:
            value = value.strip()

            if not value:
                continue

            key = value.lower()

            if key in seen:
                continue

            seen.add(key)
            result.append(value)

        return result

    def construct(
        self,
        query: str,
        processed: Dict[str, Any],
        reasoning: Dict[str, Any],
    ) -> ConstructedAnswer:
        points: List[str] = []
        evidence: List[Dict[str, Any]] = []

        for fact in processed.get(
            "facts",
            [],
        ):
            if not isinstance(fact, dict):
                continue

            sentence = self._sentence_from_fact(
                fact
            )

            if sentence:
                points.append(sentence)
                evidence.append(
                    {
                        "type": "fact",
                        "content": fact,
                    }
                )

        for relation in processed.get(
            "relations",
            [],
        ):
            if not isinstance(relation, dict):
                continue

            sentence = self._sentence_from_relation(
                relation
            )

            if sentence:
                points.append(sentence)
                evidence.append(
                    {
                        "type": "relation",
                        "content": relation,
                    }
                )

        for conclusion in reasoning.get(
            "conclusions",
            [],
        ):
            if not isinstance(
                conclusion,
                dict,
            ):
                continue

            content = conclusion.get(
                "content"
            )

            text = self._text(content)

            if text:
                points.append(text)
                evidence.append(
                    {
                        "type": conclusion.get(
                            "type",
                            "reasoning",
                        ),
                        "content": content,
                    }
                )

        points = self._deduplicate(points)

        if points:
            answer = " ".join(points)
        else:
            answer = (
                "I do not have enough internal "
                "knowledge to answer this yet."
            )

        confidence = float(
            reasoning.get(
                "confidence",
                0.0,
            )
        )

        if evidence:
            confidence = max(
                confidence,
                min(
                    1.0,
                    len(evidence) / 5.0,
                ),
            )

        return ConstructedAnswer(
            query=query,
            answer=answer,
            key_points=points,
            evidence=evidence,
            reasoning=list(
                reasoning.get(
                    "conclusions",
                    [],
                )
            ),
            confidence=round(
                confidence,
                4,
            ),
            grounded=bool(evidence),
        )

    def to_dict(
        self,
        result: ConstructedAnswer,
    ) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "query": result.query,
            "answer": result.answer,
            "key_points": list(
                result.key_points
            ),
            "evidence": list(
                result.evidence
            ),
            "reasoning": list(
                result.reasoning
            ),
            "confidence": result.confidence,
            "grounded": result.grounded,
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
        }


__all__ = [
    "ConstructedAnswer",
    "KnowledgeAnswerConstructionEngine",
]
