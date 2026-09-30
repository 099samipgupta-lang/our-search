from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class MeaningComponent:
    kind: str
    name: str
    value: Any = None
    confidence: float = 1.0
    source: str = ""


@dataclass
class UnifiedMeaning:
    subject: str
    concepts: Set[str] = field(default_factory=set)
    facts: List[Dict[str, Any]] = field(default_factory=list)
    relations: List[Dict[str, Any]] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)
    semantics: Dict[str, Any] = field(default_factory=dict)
    components: List[MeaningComponent] = field(
        default_factory=list
    )


class KnowledgeCompositionEngine:
    VERSION = "knowledge-composition.v1"

    def __init__(self):
        self.meanings: Dict[str, UnifiedMeaning] = {}

    def _key(self, subject: str) -> str:
        return subject.lower().strip()

    def create(
        self,
        subject: str,
        concepts: Optional[List[str]] = None,
        facts: Optional[List[Dict[str, Any]]] = None,
        relations: Optional[List[Dict[str, Any]]] = None,
        context: Optional[Dict[str, Any]] = None,
        semantics: Optional[Dict[str, Any]] = None,
    ) -> UnifiedMeaning:
        meaning = UnifiedMeaning(
            subject=subject.strip(),
            concepts=set(concepts or []),
            facts=list(facts or []),
            relations=list(relations or []),
            context=dict(context or {}),
            semantics=dict(semantics or {}),
        )

        for concept in meaning.concepts:
            meaning.components.append(
                MeaningComponent(
                    kind="concept",
                    name=concept,
                )
            )

        for fact in meaning.facts:
            meaning.components.append(
                MeaningComponent(
                    kind="fact",
                    name=str(
                        fact.get("predicate", "")
                    ),
                    value=fact.get("value"),
                    confidence=float(
                        fact.get("confidence", 1.0)
                    ),
                    source=str(
                        fact.get("source", "")
                    ),
                )
            )

        for relation in meaning.relations:
            meaning.components.append(
                MeaningComponent(
                    kind="relation",
                    name=str(
                        relation.get("predicate", "")
                    ),
                    value=relation,
                    confidence=float(
                        relation.get("confidence", 1.0)
                    ),
                    source=str(
                        relation.get("source", "")
                    ),
                )
            )

        self.meanings[self._key(subject)] = meaning
        return meaning

    def add_concept(
        self,
        subject: str,
        concept: str,
    ) -> UnifiedMeaning:
        meaning = self.get(subject)

        if meaning is None:
            meaning = self.create(subject)

        meaning.concepts.add(concept)

        meaning.components.append(
            MeaningComponent(
                kind="concept",
                name=concept,
            )
        )

        return meaning

    def add_fact(
        self,
        subject: str,
        fact: Dict[str, Any],
    ) -> UnifiedMeaning:
        meaning = self.get(subject)

        if meaning is None:
            meaning = self.create(subject)

        meaning.facts.append(dict(fact))

        meaning.components.append(
            MeaningComponent(
                kind="fact",
                name=str(
                    fact.get("predicate", "")
                ),
                value=fact.get("value"),
                confidence=float(
                    fact.get("confidence", 1.0)
                ),
                source=str(
                    fact.get("source", "")
                ),
            )
        )

        return meaning

    def add_relation(
        self,
        subject: str,
        relation: Dict[str, Any],
    ) -> UnifiedMeaning:
        meaning = self.get(subject)

        if meaning is None:
            meaning = self.create(subject)

        meaning.relations.append(
            dict(relation)
        )

        meaning.components.append(
            MeaningComponent(
                kind="relation",
                name=str(
                    relation.get("predicate", "")
                ),
                value=dict(relation),
                confidence=float(
                    relation.get(
                        "confidence", 1.0
                    )
                ),
                source=str(
                    relation.get("source", "")
                ),
            )
        )

        return meaning

    def add_context(
        self,
        subject: str,
        context: Dict[str, Any],
    ) -> UnifiedMeaning:
        meaning = self.get(subject)

        if meaning is None:
            meaning = self.create(subject)

        meaning.context.update(context)
        return meaning

    def add_semantics(
        self,
        subject: str,
        semantics: Dict[str, Any],
    ) -> UnifiedMeaning:
        meaning = self.get(subject)

        if meaning is None:
            meaning = self.create(subject)

        meaning.semantics.update(semantics)
        return meaning

    def get(
        self,
        subject: str,
    ) -> Optional[UnifiedMeaning]:
        return self.meanings.get(
            self._key(subject)
        )

    def compose(
        self,
        subject: str,
        parts: Optional[
            Dict[str, Any]
        ] = None,
    ) -> UnifiedMeaning:
        parts = parts or {}

        meaning = self.get(subject)

        if meaning is None:
            meaning = self.create(subject)

        for concept in parts.get(
            "concepts", []
        ):
            self.add_concept(
                subject,
                str(concept),
            )

        for fact in parts.get(
            "facts", []
        ):
            if isinstance(fact, dict):
                self.add_fact(
                    subject,
                    fact,
                )

        for relation in parts.get(
            "relations", []
        ):
            if isinstance(relation, dict):
                self.add_relation(
                    subject,
                    relation,
                )

        context = parts.get(
            "context"
        )

        if isinstance(context, dict):
            self.add_context(
                subject,
                context,
            )

        semantics = parts.get(
            "semantics"
        )

        if isinstance(semantics, dict):
            self.add_semantics(
                subject,
                semantics,
            )

        return meaning

    def summary(
        self,
        subject: str,
    ) -> Dict[str, Any]:
        meaning = self.get(subject)

        if meaning is None:
            return {}

        return {
            "subject": meaning.subject,
            "concepts": sorted(
                meaning.concepts
            ),
            "facts": list(
                meaning.facts
            ),
            "relations": list(
                meaning.relations
            ),
            "context": dict(
                meaning.context
            ),
            "semantics": dict(
                meaning.semantics
            ),
            "component_count": len(
                meaning.components
            ),
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "meaning_count": len(
                self.meanings
            ),
            "component_count": sum(
                len(
                    meaning.components
                )
                for meaning in self.meanings.values()
            ),
        }


__all__ = [
    "MeaningComponent",
    "UnifiedMeaning",
    "KnowledgeCompositionEngine",
]
