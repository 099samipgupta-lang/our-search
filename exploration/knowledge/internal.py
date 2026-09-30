from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class KnowledgeFact:
    subject: str
    predicate: str
    value: str
    category: str = "fact"
    confidence: float = 1.0
    source: str = "internal"
    tags: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class KnowledgeConcept:
    name: str
    definition: str
    aliases: tuple[str, ...] = field(default_factory=tuple)
    category: str = "concept"
    related: tuple[str, ...] = field(default_factory=tuple)


class InternalKnowledgeSystem:
    """
    Internal knowledge substrate for the OUR SEARCH Brain.

    This system is independent of web retrieval. It stores concepts,
    facts, relationships, definitions, and rules that the brain can
    use directly when answering questions.
    """

    VERSION = "internal-knowledge.v1"

    def __init__(self):
        self.facts: list[KnowledgeFact] = []
        self.concepts: dict[str, KnowledgeConcept] = {}
        self.rules: list[dict[str, Any]] = []

        self._fact_index: dict[str, list[KnowledgeFact]] = {}
        self._alias_index: dict[str, str] = {}

        self._load_foundational_knowledge()

    @staticmethod
    def _normalize(value: str) -> str:
        return " ".join(
            str(value or "").casefold().split()
        )

    def add_fact(
        self,
        subject: str,
        predicate: str,
        value: str,
        category: str = "fact",
        confidence: float = 1.0,
        source: str = "internal",
        tags=(),
    ):
        fact = KnowledgeFact(
            subject=str(subject),
            predicate=str(predicate),
            value=str(value),
            category=str(category),
            confidence=float(confidence),
            source=str(source),
            tags=tuple(str(tag) for tag in tags),
        )

        self.facts.append(fact)

        subject_key = self._normalize(subject)

        self._fact_index.setdefault(
            subject_key,
            [],
        ).append(fact)

        return fact

    def add_concept(
        self,
        name: str,
        definition: str,
        aliases=(),
        category: str = "concept",
        related=(),
    ):
        concept = KnowledgeConcept(
            name=str(name),
            definition=str(definition),
            aliases=tuple(str(alias) for alias in aliases),
            category=str(category),
            related=tuple(str(item) for item in related),
        )

        key = self._normalize(name)

        self.concepts[key] = concept

        for alias in concept.aliases:
            self._alias_index[
                self._normalize(alias)
            ] = key

        return concept

    def add_rule(self, name: str, condition, conclusion):
        self.rules.append(
            {
                "name": str(name),
                "condition": condition,
                "conclusion": conclusion,
            }
        )

    def find_concept(self, query: str):
        key = self._normalize(query)

        if key in self.concepts:
            return self.concepts[key]

        alias_key = self._alias_index.get(key)

        if alias_key:
            return self.concepts.get(alias_key)

        matches = []

        for concept_key, concept in self.concepts.items():
            if (
                key in concept_key
                or concept_key in key
            ):
                matches.append(concept)

        if len(matches) == 1:
            return matches[0]

        return None

    def find_facts(
        self,
        subject: str | None = None,
        predicate: str | None = None,
    ):
        if subject is not None:
            facts = self._fact_index.get(
                self._normalize(subject),
                [],
            )
        else:
            facts = list(self.facts)

        if predicate is None:
            return list(facts)

        predicate_key = self._normalize(predicate)

        return [
            fact
            for fact in facts
            if self._normalize(fact.predicate)
            == predicate_key
        ]

    def search(self, query: str, limit: int = 10):
        """
        Search only internal knowledge.

        No website, crawler, external API, or search engine is
        consulted here.
        """
        words = {
            word.strip(".,!?;:()[]{}\"'")
            for word in self._normalize(query).split()
            if len(word.strip(".,!?;:()[]{}\"'")) > 1
        }

        matches = []

        for concept in self.concepts.values():
            searchable = " ".join(
                [
                    concept.name,
                    concept.definition,
                    *concept.aliases,
                    *concept.related,
                ]
            ).casefold()

            score = sum(
                1
                for word in words
                if word in searchable.split()
            )

            if score:
                matches.append(
                    (
                        score,
                        "concept",
                        concept,
                    )
                )

        for fact in self.facts:
            searchable = " ".join(
                [
                    fact.subject,
                    fact.predicate,
                    fact.value,
                    *fact.tags,
                ]
            ).casefold()

            score = sum(
                1
                for word in words
                if word in searchable.split()
            )

            if score:
                matches.append(
                    (
                        score,
                        "fact",
                        fact,
                    )
                )

        matches.sort(
            key=lambda item: (
                item[0],
                getattr(
                    item[2],
                    "confidence",
                    1.0,
                ),
            ),
            reverse=True,
        )

        return [
            {
                "score": score,
                "type": kind,
                "knowledge": item,
            }
            for score, kind, item in matches[:max(1, int(limit))]
        ]

    def _load_foundational_knowledge(self):
        self.add_concept(
            "computer",
            "A programmable electronic system that processes "
            "data according to instructions.",
            aliases=("computing machine",),
            category="technology",
        )

        self.add_concept(
            "computer science",
            "The study of computation, algorithms, information, "
            "software, and computational systems.",
            aliases=("cs",),
            category="academic field",
        )

        self.add_concept(
            "internet",
            "A global network of interconnected computer networks "
            "that communicate using standardized protocols.",
            category="technology",
        )

        self.add_concept(
            "website",
            "A collection of related web pages and resources "
            "available through the World Wide Web.",
            category="technology",
        )

        self.add_concept(
            "search engine",
            "A system that discovers, organizes, retrieves, and "
            "presents information in response to user queries.",
            aliases=("search system",),
            category="technology",
        )

        self.add_concept(
            "university",
            "An institution of higher education and research.",
            aliases=("higher education institution",),
            category="education",
        )

        self.add_concept(
            "planet",
            "A large astronomical body that orbits a star or "
            "stellar remnant and is massive enough to be "
            "approximately spherical.",
            category="astronomy",
        )

        self.add_concept(
            "star",
            "A massive astronomical object whose own gravity "
            "confines hot plasma and whose energy is produced "
            "primarily by nuclear processes.",
            category="astronomy",
        )

        self.add_concept(
            "black hole",
            "A region of spacetime with gravity so strong that "
            "nothing, including light, can escape once it passes "
            "the event horizon.",
            aliases=("blackhole",),
            category="astronomy",
        )

        self.add_concept(
            "algorithm",
            "A finite, ordered procedure for solving a problem "
            "or performing a computation.",
            category="computer science",
        )

        self.add_concept(
            "database",
            "An organized collection of data designed to be "
            "stored, accessed, and managed systematically.",
            category="computer science",
        )

        self.add_fact(
            "Earth",
            "type",
            "planet",
            category="astronomy",
        )

        self.add_fact(
            "Sun",
            "type",
            "star",
            category="astronomy",
        )

        self.add_fact(
            "Earth",
            "orbits",
            "Sun",
            category="astronomy",
        )

        self.add_fact(
            "OUR SEARCH",
            "type",
            "search engine",
            category="system",
        )

        self.add_fact(
            "OUR SEARCH",
            "uses",
            "crawler, index, storage, retrieval, reasoning, "
            "and conversational systems",
            category="system",
        )
