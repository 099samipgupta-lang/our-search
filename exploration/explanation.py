from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExplanationSection:
    title: str
    content: str
    sources: tuple[str, ...]


class ExplanationEngine:
    """
    Explanation construction machine for OUR SEARCH.

    It organizes verified information into a structured
    explanation that the experience layer can render.
    """

    def build(
        self,
        query: str,
        understanding: dict[str, Any],
        knowledge: dict[str, Any],
        concepts: list[dict[str, Any]],
        reasoning: dict[str, Any],
        verification: dict[str, Any],
    ) -> dict[str, Any]:

        sources = knowledge.get(
            "sources",
            [],
        )

        source_urls = tuple(
            str(source.get("url", ""))
            for source in sources
            if isinstance(source, dict)
            and source.get("url")
        )

        sections = []

        sections.append(
            ExplanationSection(
                title="Question",
                content=(
                    str(
                        understanding.get(
                            "normalized_query",
                            query,
                        )
                    )
                ),
                sources=(),
            )
        )

        if concepts:
            concept_names = [
                str(item.get("name", ""))
                for item in concepts[:8]
                if isinstance(item, dict)
            ]

            sections.append(
                ExplanationSection(
                    title="Key Concepts",
                    content=", ".join(
                        name
                        for name in concept_names
                        if name
                    ),
                    sources=source_urls,
                )
            )

        structured = reasoning.get(
            "structured",
            {},
        )

        conclusions = structured.get(
            "conclusions",
            [],
        )

        if conclusions:
            sections.append(
                ExplanationSection(
                    title="Reasoning",
                    content=" ".join(
                        str(item)
                        for item in conclusions
                    ),
                    sources=source_urls,
                )
            )

        structured_verification = verification.get(
            "structured",
            {},
        )

        supported_count = structured_verification.get(
            "supported_source_count",
            0,
        )

        sections.append(
            ExplanationSection(
                title="Evidence",
                content=(
                    f"{supported_count} source(s) passed "
                    "the available structural checks."
                ),
                sources=source_urls,
            )
        )

        return {
            "query": query,
            "section_count": len(sections),
            "sections": [
                {
                    "title": section.title,
                    "content": section.content,
                    "sources": list(section.sources),
                }
                for section in sections
            ],
            "source_count": len(sources),
        }


__all__ = [
    "ExplanationSection",
    "ExplanationEngine",
]
