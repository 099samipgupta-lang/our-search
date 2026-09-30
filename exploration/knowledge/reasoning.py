from dataclasses import dataclass
from typing import Any

from exploration.knowledge.graph import KnowledgeGraph


@dataclass(frozen=True)
class ReasoningInference:
    conclusion: str
    evidence: tuple[str, ...]
    confidence: float
    rule: str


class KnowledgeReasoningEngine:
    """
    Symbolic reasoning layer over the OUR SEARCH knowledge graph.

    It derives conclusions from connected knowledge rather than
    simply returning stored facts.
    """

    VERSION = "knowledge-reasoning.v1"

    def __init__(
        self,
        graph: KnowledgeGraph | None = None,
    ):
        self.graph = (
            graph
            if graph is not None
            else KnowledgeGraph()
        )

        self.rules = [
            self._transitive_relation,
            self._type_relation,
            self._two_hop_relation,
        ]

    @staticmethod
    def _normalize(value):
        return " ".join(
            str(value or "").casefold().split()
        )

    def _relation_text(self, relation):
        return (
            f"{relation.subject} "
            f"{relation.predicate} "
            f"{relation.object}"
        )

    def _transitive_relation(self, subject):
        """
        If A relates to B and B relates to C using the same
        predicate, derive A relates to C.
        """
        relations = self.graph.related_to(
            subject,
            direction="out",
        )

        inferences = []

        for first in relations:
            second_relations = self.graph.related_to(
                first.object,
                direction="out",
            )

            for second in second_relations:
                if (
                    self._normalize(
                        first.predicate
                    )
                    != self._normalize(
                        second.predicate
                    )
                ):
                    continue

                if self._normalize(
                    second.object
                ) == self._normalize(subject):
                    continue

                inferences.append(
                    ReasoningInference(
                        conclusion=(
                            f"{subject} "
                            f"{first.predicate} "
                            f"{second.object}"
                        ),
                        evidence=(
                            self._relation_text(first),
                            self._relation_text(second),
                        ),
                        confidence=min(
                            first.confidence,
                            second.confidence,
                        ) * 0.85,
                        rule="transitive_relation",
                    )
                )

        return inferences

    def _type_relation(self, subject):
        """
        Follow an entity's type and inspect knowledge attached
        to that type.
        """
        relations = self.graph.related_to(
            subject,
            direction="out",
        )

        inferences = []

        for relation in relations:
            if self._normalize(
                relation.predicate
            ) != "type":
                continue

            type_relations = self.graph.related_to(
                relation.object,
                direction="out",
            )

            for type_relation in type_relations:
                if self._normalize(
                    type_relation.predicate
                ) == "type":
                    continue

                inferences.append(
                    ReasoningInference(
                        conclusion=(
                            f"{subject} "
                            f"{type_relation.predicate} "
                            f"{type_relation.object}"
                        ),
                        evidence=(
                            self._relation_text(relation),
                            self._relation_text(
                                type_relation
                            ),
                        ),
                        confidence=min(
                            relation.confidence,
                            type_relation.confidence,
                        ) * 0.80,
                        rule="type_inheritance",
                    )
                )

        return inferences

    def _two_hop_relation(self, subject):
        """
        Follow two different relationships to discover a
        connected conclusion.
        """
        relations = self.graph.related_to(
            subject,
            direction="out",
        )

        inferences = []

        for first in relations:
            second_relations = self.graph.related_to(
                first.object,
                direction="out",
            )

            for second in second_relations:
                if self._normalize(
                    second.object
                ) == self._normalize(subject):
                    continue

                conclusion = (
                    f"{subject} is connected through "
                    f"{first.object} to "
                    f"{second.object}"
                )

                inferences.append(
                    ReasoningInference(
                        conclusion=conclusion,
                        evidence=(
                            self._relation_text(first),
                            self._relation_text(second),
                        ),
                        confidence=min(
                            first.confidence,
                            second.confidence,
                        ) * 0.70,
                        rule="two_hop_connection",
                    )
                )

        return inferences

    def infer(self, subject, limit=20):
        subject = " ".join(
            str(subject or "").split()
        ).strip()

        if not subject:
            return []

        candidates = []

        for rule in self.rules:
            candidates.extend(
                rule(subject)
            )

        unique = {}

        for inference in candidates:
            key = self._normalize(
                inference.conclusion
            )

            previous = unique.get(key)

            if (
                previous is None
                or inference.confidence
                > previous.confidence
            ):
                unique[key] = inference

        results = list(
            unique.values()
        )

        results.sort(
            key=lambda item: (
                item.confidence,
                len(item.evidence),
            ),
            reverse=True,
        )

        return results[:max(1, int(limit))]

    def explain_inference(
        self,
        inference: ReasoningInference,
    ) -> dict[str, Any]:
        return {
            "conclusion": inference.conclusion,
            "evidence": list(
                inference.evidence
            ),
            "confidence": inference.confidence,
            "rule": inference.rule,
        }


__all__ = [
    "ReasoningInference",
    "KnowledgeReasoningEngine",
]
