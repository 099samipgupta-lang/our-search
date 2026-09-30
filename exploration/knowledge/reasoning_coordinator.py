from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from exploration.knowledge.reasoning import (
    KnowledgeReasoningEngine,
)
from exploration.knowledge.causality import (
    KnowledgeCausalityEngine,
)
from exploration.knowledge.logic_engine import (
    LogicCapabilityEngine,
)


@dataclass
class ReasoningResult:
    query: str
    inferences: List[Dict[str, Any]] = field(
        default_factory=list
    )
    causal: List[Dict[str, Any]] = field(
        default_factory=list
    )
    logical: List[Dict[str, Any]] = field(
        default_factory=list
    )
    conclusions: List[Dict[str, Any]] = field(
        default_factory=list
    )
    confidence: float = 0.0
    ready: bool = False


class KnowledgeReasoningCoordinator:
    VERSION = "knowledge-reasoning-coordinator.v1"

    def __init__(
        self,
        reasoning: Optional[
            KnowledgeReasoningEngine
        ] = None,
        causality: Optional[
            KnowledgeCausalityEngine
        ] = None,
        logic: Optional[
            LogicCapabilityEngine
        ] = None,
    ):
        self.reasoning = (
            reasoning
            if reasoning is not None
            else KnowledgeReasoningEngine()
        )

        self.causality = (
            causality
            if causality is not None
            else KnowledgeCausalityEngine()
        )

        self.logic = (
            logic
            if logic is not None
            else LogicCapabilityEngine()
        )

    def reason(
        self,
        query: str,
        processed: Dict[str, Any],
    ) -> ReasoningResult:
        facts = list(
            processed.get(
                "facts",
                [],
            )
        )

        relations = list(
            processed.get(
                "relations",
                [],
            )
        )

        derived = list(
            processed.get(
                "derived",
                [],
            )
        )

        inferences = self._infer(
            facts,
            relations,
        )

        causal = self._causal(
            relations
        )

        logical = self._logical(
            query,
            processed,
        )

        conclusions = self._conclusions(
            inferences,
            causal,
            logical,
            derived,
        )

        confidence = self._confidence(
            inferences,
            causal,
            logical,
            conclusions,
        )

        return ReasoningResult(
            query=query,
            inferences=inferences,
            causal=causal,
            logical=logical,
            conclusions=conclusions,
            confidence=confidence,
            ready=True,
        )

    def _infer(
        self,
        facts: List[Dict[str, Any]],
        relations: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        items = []

        try:
            result = self.reasoning.infer(
                facts=facts,
                relations=relations,
            )

            if isinstance(result, list):
                items.extend(result)

            elif isinstance(result, dict):
                items.append(result)

        except (TypeError, AttributeError):
            pass

        return items

    def _causal(
        self,
        relations: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        results = []

        for relation in relations:
            predicate = str(
                relation.get(
                    "predicate",
                    "",
                )
            ).lower()

            if predicate not in {
                "causes",
                "caused by",
                "leads to",
                "results in",
                "produces",
            }:
                continue

            subject = str(
                relation.get(
                    "subject",
                    "",
                )
            )

            object_value = str(
                relation.get(
                    "object",
                    relation.get(
                        "value",
                        "",
                    ),
                )
            )

            if not subject or not object_value:
                continue

            results.append(
                {
                    "subject": subject,
                    "predicate": predicate,
                    "object": object_value,
                }
            )

        return results

    def _logical(
        self,
        query: str,
        processed: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        results = []

        text = query.strip()

        if not text:
            return results

        for relation in processed.get(
            "relations",
            [],
        ):
            predicate = str(
                relation.get(
                    "predicate",
                    "",
                )
            )

            subject = str(
                relation.get(
                    "subject",
                    "",
                )
            )

            object_value = str(
                relation.get(
                    "object",
                    relation.get(
                        "value",
                        "",
                    ),
                )
            )

            if (
                not subject
                or not predicate
                or not object_value
            ):
                continue

            results.append(
                {
                    "type": "relation",
                    "subject": subject,
                    "predicate": predicate,
                    "object": object_value,
                }
            )

        return results

    def _conclusions(
        self,
        inferences: List[Dict[str, Any]],
        causal: List[Dict[str, Any]],
        logical: List[Dict[str, Any]],
        derived: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        conclusions = []

        for item in inferences:
            conclusions.append(
                {
                    "type": "inference",
                    "content": item,
                }
            )

        for item in causal:
            conclusions.append(
                {
                    "type": "causal",
                    "content": item,
                }
            )

        for item in logical:
            conclusions.append(
                {
                    "type": "logical",
                    "content": item,
                }
            )

        for item in derived:
            conclusions.append(
                {
                    "type": "derived",
                    "content": item,
                }
            )

        return self._deduplicate(
            conclusions
        )

    def _deduplicate(
        self,
        items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        seen = set()
        result = []

        for item in items:
            key = repr(item)

            if key in seen:
                continue

            seen.add(key)
            result.append(item)

        return result

    def _confidence(
        self,
        inferences: List[Dict[str, Any]],
        causal: List[Dict[str, Any]],
        logical: List[Dict[str, Any]],
        conclusions: List[Dict[str, Any]],
    ) -> float:
        evidence_count = (
            len(inferences)
            + len(causal)
            + len(logical)
        )

        if evidence_count == 0:
            return 0.0

        conclusion_factor = min(
            1.0,
            len(conclusions) / 5.0,
        )

        evidence_factor = min(
            1.0,
            evidence_count / 5.0,
        )

        return round(
            (
                evidence_factor * 0.6
                + conclusion_factor * 0.4
            ),
            4,
        )

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "reasoning_components": 3,
        }


__all__ = [
    "ReasoningResult",
    "KnowledgeReasoningCoordinator",
]
