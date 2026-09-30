from dataclasses import dataclass
import re


@dataclass(frozen=True)
class ExtractedFact:
    subject: str
    predicate: str
    value: str
    confidence: float


@dataclass(frozen=True)
class ExtractedConcept:
    name: str
    definition: str
    confidence: float


@dataclass(frozen=True)
class ExtractionResult:
    concepts: list[ExtractedConcept]
    facts: list[ExtractedFact]
    relationships: list[tuple[str, str, str]]


class KnowledgeExtractionEngine:
    """
    Converts readable text into preliminary structured knowledge.

    This is intentionally conservative. Extraction is not verification.
    Later verification layers decide what is safe to commit to memory.
    """

    VERSION = "knowledge-extraction.v1"

    _DEFINITION_PATTERNS = (
        re.compile(
            r"^\s*(.+?)\s+is\s+(?:a|an|the)\s+(.+?)[.!?]\s*$",
            re.IGNORECASE,
        ),
        re.compile(
            r"^\s*(.+?)\s+are\s+(.+?)[.!?]\s*$",
            re.IGNORECASE,
        ),
    )

    _RELATION_PATTERNS = (
        re.compile(
            r"^\s*(.+?)\s+(?:is|was)\s+located\s+in\s+(.+?)[.!?]\s*$",
            re.IGNORECASE,
        ),
        re.compile(
            r"^\s*(.+?)\s+is\s+part\s+of\s+(.+?)[.!?]\s*$",
            re.IGNORECASE,
        ),
        re.compile(
            r"^\s*(.+?)\s+uses\s+(.+?)[.!?]\s*$",
            re.IGNORECASE,
        ),
        re.compile(
            r"^\s*(.+?)\s+contains\s+(.+?)[.!?]\s*$",
            re.IGNORECASE,
        ),
        re.compile(
            r"^\s*(.+?)\s+created\s+(.+?)[.!?]\s*$",
            re.IGNORECASE,
        ),
    )

    @staticmethod
    def _clean(value):
        return " ".join(
            str(value or "").split()
        ).strip(" \t\r\n.,;:")

    @staticmethod
    def _valid_phrase(value):
        value = str(value or "").strip()

        if not value:
            return False

        if len(value) > 300:
            return False

        if len(value.split()) > 40:
            return False

        return True

    def _sentences(self, text):
        text = self._clean(text)

        if not text:
            return []

        return [
            self._clean(sentence)
            for sentence in re.split(
                r"(?<=[.!?])\s+",
                text,
            )
            if self._clean(sentence)
        ]

    def _extract_definition(self, sentence):
        for pattern in self._DEFINITION_PATTERNS:
            match = pattern.match(sentence)

            if not match:
                continue

            subject = self._clean(match.group(1))
            definition = self._clean(match.group(2))

            if (
                self._valid_phrase(subject)
                and self._valid_phrase(definition)
            ):
                return ExtractedConcept(
                    name=subject,
                    definition=definition,
                    confidence=0.72,
                )

        return None

    def _extract_relationship(self, sentence):
        for pattern in self._RELATION_PATTERNS:
            match = pattern.match(sentence)

            if not match:
                continue

            subject = self._clean(match.group(1))
            value = self._clean(match.group(2))

            if not subject or not value:
                continue

            predicate = ""

            if "located" in pattern.pattern:
                predicate = "located in"
            elif "part of" in pattern.pattern:
                predicate = "part of"
            elif "uses" in pattern.pattern:
                predicate = "uses"
            elif "contains" in pattern.pattern:
                predicate = "contains"
            elif "created" in pattern.pattern:
                predicate = "created"

            return (
                subject,
                predicate,
                value,
            )

        return None

    def extract(self, text):
        concepts = []
        facts = []
        relationships = []

        seen_concepts = set()
        seen_facts = set()
        seen_relationships = set()

        for sentence in self._sentences(text):
            concept = self._extract_definition(
                sentence
            )

            if concept:
                key = (
                    concept.name.casefold(),
                    concept.definition.casefold(),
                )

                if key not in seen_concepts:
                    seen_concepts.add(key)
                    concepts.append(concept)

                    facts.append(
                        ExtractedFact(
                            subject=concept.name,
                            predicate="definition",
                            value=concept.definition,
                            confidence=concept.confidence,
                        )
                    )

            relationship = self._extract_relationship(
                sentence
            )

            if relationship:
                subject, predicate, value = relationship

                key = (
                    subject.casefold(),
                    predicate.casefold(),
                    value.casefold(),
                )

                if key not in seen_relationships:
                    seen_relationships.add(key)
                    relationships.append(
                        relationship
                    )

                    facts.append(
                        ExtractedFact(
                            subject=subject,
                            predicate=predicate,
                            value=value,
                            confidence=0.65,
                        )
                    )

        return ExtractionResult(
            concepts=concepts,
            facts=facts,
            relationships=relationships,
        )


__all__ = [
    "ExtractedFact",
    "ExtractedConcept",
    "ExtractionResult",
    "KnowledgeExtractionEngine",
]
