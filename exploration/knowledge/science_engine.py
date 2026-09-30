from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class ScienceConcept:
    name: str
    field: str
    definition: str
    relationships: List[str]


class ScienceCapabilityEngine:
    VERSION = "science-capability.v1"

    def __init__(self):
        self._concepts: Dict[str, ScienceConcept] = {}
        self._load_foundations()

    def _add(
        self,
        name: str,
        field: str,
        definition: str,
        relationships: List[str],
    ) -> None:
        self._concepts[
            name.lower()
        ] = ScienceConcept(
            name=name,
            field=field,
            definition=definition,
            relationships=list(relationships),
        )

    def _load_foundations(self) -> None:
        self._add(
            "gravity",
            "physics",
            "A physical interaction associated with mass and energy that causes attraction between objects.",
            [
                "mass",
                "motion",
                "orbits",
            ],
        )

        self._add(
            "atom",
            "physics",
            "A basic unit of ordinary matter consisting of a nucleus and surrounding electrons.",
            [
                "matter",
                "electron",
                "proton",
                "neutron",
            ],
        )

        self._add(
            "cell",
            "biology",
            "The basic structural and functional unit of living organisms.",
            [
                "biology",
                "organism",
                "DNA",
            ],
        )

        self._add(
            "DNA",
            "biology",
            "A molecule that stores hereditary genetic information in living organisms and many viruses.",
            [
                "genes",
                "heredity",
                "cell",
            ],
        )

        self._add(
            "photosynthesis",
            "biology",
            "A biological process in which photosynthetic organisms convert light energy into stored chemical energy.",
            [
                "plants",
                "light",
                "carbon dioxide",
                "oxygen",
            ],
        )

        self._add(
            "ecosystem",
            "ecology",
            "A system consisting of living organisms and their physical environment interacting with one another.",
            [
                "organisms",
                "environment",
                "ecology",
            ],
        )

        self._add(
            "planet",
            "astronomy",
            "A large astronomical body that orbits a star or, in some cases, another astronomical system.",
            [
                "orbit",
                "star",
                "solar system",
            ],
        )

        self._add(
            "star",
            "astronomy",
            "A luminous astronomical object whose energy is generated primarily by processes in its interior.",
            [
                "gravity",
                "fusion",
                "galaxy",
            ],
        )

        self._add(
            "black hole",
            "astronomy",
            "A region of spacetime associated with gravity so strong that objects crossing its event horizon cannot escape back out.",
            [
                "gravity",
                "event horizon",
                "spacetime",
            ],
        )

    def get(
        self,
        name: str,
    ) -> ScienceConcept | None:
        return self._concepts.get(
            name.strip().lower()
        )

    def search(
        self,
        query: str,
        limit: int = 10,
    ) -> List[ScienceConcept]:
        terms = {
            term.lower()
            for term in re.findall(
                r"\b[\w'-]+\b",
                query,
                flags=re.UNICODE,
            )
        }

        matches = []

        for concept in self._concepts.values():
            searchable = " ".join(
                [
                    concept.name,
                    concept.field,
                    concept.definition,
                    *concept.relationships,
                ]
            ).lower()

            score = sum(
                1
                for term in terms
                if term in searchable
            )

            if score:
                matches.append(
                    (score, concept)
                )

        matches.sort(
            key=lambda item: item[0],
            reverse=True,
        )

        return [
            concept
            for _, concept in matches[:limit]
        ]

    def explain(
        self,
        name: str,
    ) -> Dict[str, object]:
        concept = self.get(name)

        if concept is None:
            return {
                "found": False,
                "name": name,
            }

        return {
            "found": True,
            "name": concept.name,
            "field": concept.field,
            "definition": concept.definition,
            "relationships": concept.relationships,
        }

    def can_handle(
        self,
        query: str,
    ) -> bool:
        text = query.lower()

        science_terms = {
            "science",
            "physics",
            "biology",
            "chemistry",
            "astronomy",
            "gravity",
            "atom",
            "cell",
            "dna",
            "planet",
            "star",
            "black hole",
            "photosynthesis",
            "ecosystem",
        }

        return any(
            term in text
            for term in science_terms
        )

    def stats(self) -> Dict[str, int]:
        return {
            "version": self.VERSION,
            "concepts": len(
                self._concepts
            ),
        }
