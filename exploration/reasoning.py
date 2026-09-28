from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ReasoningStep:
    operation: str
    input_data: dict[str, Any]
    conclusion: str


class ReasoningEngine:
    """
    Structured reasoning machine for OUR SEARCH.

    It operates on information supplied by the other brain
    machines and records how conclusions were formed.
    """

    def analyze(
        self,
        query: str,
        understanding: dict[str, Any],
        knowledge: dict[str, Any],
        concepts: list[dict[str, Any]],
        relationships: list[dict[str, Any]],
    ) -> dict[str, Any]:

        steps = []

        source_count = int(
            knowledge.get("source_count", 0)
        )

        steps.append(
            ReasoningStep(
                operation="source_analysis",
                input_data={
                    "source_count": source_count,
                },
                conclusion=(
                    "No searchable source was available."
                    if source_count == 0
                    else (
                        f"{source_count} searchable "
                        "source(s) are available."
                    )
                ),
            )
        )

        query_words = understanding.get(
            "words",
            [],
        )

        steps.append(
            ReasoningStep(
                operation="query_analysis",
                input_data={
                    "word_count": len(query_words),
                },
                conclusion=(
                    "The query contains searchable terms."
                    if query_words
                    else "The query contains no searchable terms."
                ),
            )
        )

        if concepts:
            top_concepts = [
                item.get("name", "")
                for item in concepts[:5]
                if isinstance(item, dict)
            ]

            steps.append(
                ReasoningStep(
                    operation="concept_analysis",
                    input_data={
                        "concept_count": len(concepts),
                    },
                    conclusion=(
                        "The retrieved information contains "
                        "identifiable concepts: "
                        + ", ".join(top_concepts)
                    ),
                )
            )

        steps.append(
            ReasoningStep(
                operation="relationship_analysis",
                input_data={
                    "relationship_count": len(
                        relationships
                    ),
                },
                conclusion=(
                    "Related information was identified."
                    if relationships
                    else "No related information was identified."
                ),
            )
        )

        conclusions = [
            step.conclusion
            for step in steps
        ]

        return {
            "query": query,
            "step_count": len(steps),
            "steps": [
                {
                    "operation": step.operation,
                    "input": step.input_data,
                    "conclusion": step.conclusion,
                }
                for step in steps
            ],
            "conclusions": conclusions,
        }


__all__ = [
    "ReasoningStep",
    "ReasoningEngine",
]
