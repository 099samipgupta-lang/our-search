from dataclasses import dataclass
from typing import Iterable

from exploration.knowledge.internal import (
    InternalKnowledgeSystem,
)


@dataclass(frozen=True)
class LearnedKnowledge:
    concepts_added: int
    facts_added: int
    relationships_added: int
    source: str


class KnowledgeLearningEngine:
    """
    Converts structured knowledge material into the OUR SEARCH
    internal knowledge substrate.

    This is the first learning/acquisition layer of the brain.
    """

    VERSION = "knowledge-learning.v1"

    def __init__(
        self,
        knowledge: InternalKnowledgeSystem | None = None,
    ):
        self.knowledge = (
            knowledge
            if knowledge is not None
            else InternalKnowledgeSystem()
        )

    @staticmethod
    def _clean(value) -> str:
        return " ".join(
            str(value or "").split()
        ).strip()

    def learn_concept(
        self,
        name: str,
        definition: str,
        aliases: Iterable[str] = (),
        category: str = "learned",
        related: Iterable[str] = (),
    ):
        name = self._clean(name)
        definition = self._clean(definition)

        if not name or not definition:
            return False

        self.knowledge.add_concept(
            name=name,
            definition=definition,
            aliases=tuple(
                self._clean(alias)
                for alias in aliases
                if self._clean(alias)
            ),
            category=category,
            related=tuple(
                self._clean(item)
                for item in related
                if self._clean(item)
            ),
        )

        return True

    def learn_fact(
        self,
        subject: str,
        predicate: str,
        value: str,
        category: str = "learned",
        confidence: float = 1.0,
        source: str = "learning",
        tags: Iterable[str] = (),
    ):
        subject = self._clean(subject)
        predicate = self._clean(predicate)
        value = self._clean(value)

        if not subject or not predicate or not value:
            return False

        self.knowledge.add_fact(
            subject=subject,
            predicate=predicate,
            value=value,
            category=category,
            confidence=confidence,
            source=source,
            tags=tuple(
                self._clean(tag)
                for tag in tags
                if self._clean(tag)
            ),
        )

        return True

    def learn_batch(
        self,
        concepts=None,
        facts=None,
        source: str = "batch",
    ) -> LearnedKnowledge:
        concepts = concepts or []
        facts = facts or []

        concepts_added = 0
        facts_added = 0
        relationships_added = 0

        for concept in concepts:
            if not isinstance(concept, dict):
                continue

            if self.learn_concept(
                name=concept.get("name", ""),
                definition=concept.get("definition", ""),
                aliases=concept.get("aliases", ()),
                category=concept.get(
                    "category",
                    "learned",
                ),
                related=concept.get("related", ()),
            ):
                concepts_added += 1

                if concept.get("related"):
                    relationships_added += len(
                        tuple(concept.get("related", ()))
                    )

        for fact in facts:
            if not isinstance(fact, dict):
                continue

            if self.learn_fact(
                subject=fact.get("subject", ""),
                predicate=fact.get("predicate", ""),
                value=fact.get("value", ""),
                category=fact.get(
                    "category",
                    "learned",
                ),
                confidence=fact.get(
                    "confidence",
                    1.0,
                ),
                source=fact.get(
                    "source",
                    source,
                ),
                tags=fact.get("tags", ()),
            ):
                facts_added += 1

        return LearnedKnowledge(
            concepts_added=concepts_added,
            facts_added=facts_added,
            relationships_added=relationships_added,
            source=source,
        )

    def learn_text(
        self,
        text: str,
        source: str = "text",
    ) -> LearnedKnowledge:
        """
        Conservative text-learning entry point.

        Raw text is not automatically treated as truth.
        Future learning modules will perform extraction,
        semantic analysis, conflict detection, and validation
        before committing knowledge.
        """

        text = self._clean(text)

        if not text:
            return LearnedKnowledge(
                concepts_added=0,
                facts_added=0,
                relationships_added=0,
                source=source,
            )

        return LearnedKnowledge(
            concepts_added=0,
            facts_added=0,
            relationships_added=0,
            source=source,
        )

    def acquire(
        self,
        material,
        source: str = "external",
    ) -> LearnedKnowledge:
        """
        General acquisition interface.

        Structured knowledge can be learned immediately.
        Raw material is deliberately routed through the future
        extraction/validation pipeline rather than blindly stored.
        """

        if isinstance(material, dict):
            return self.learn_batch(
                concepts=material.get("concepts", []),
                facts=material.get("facts", []),
                source=source,
            )

        if isinstance(material, (list, tuple)):
            return self.learn_batch(
                concepts=[
                    item
                    for item in material
                    if isinstance(item, dict)
                    and "definition" in item
                ],
                facts=[
                    item
                    for item in material
                    if isinstance(item, dict)
                    and "subject" in item
                    and "predicate" in item
                    and "value" in item
                ],
                source=source,
            )

        return self.learn_text(
            str(material or ""),
            source=source,
        )


__all__ = [
    "LearnedKnowledge",
    "KnowledgeLearningEngine",
]
