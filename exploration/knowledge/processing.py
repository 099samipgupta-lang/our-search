from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ProcessingResult:
    query: str
    inputs: List[Dict[str, Any]] = field(
        default_factory=list
    )
    concepts: List[str] = field(
        default_factory=list
    )
    facts: List[Dict[str, Any]] = field(
        default_factory=list
    )
    relations: List[Dict[str, Any]] = field(
        default_factory=list
    )
    derived: List[Dict[str, Any]] = field(
        default_factory=list
    )
    state: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeProcessingEngine:
    VERSION = "knowledge-processing.v1"

    def _unique_strings(
        self,
        values: List[str],
    ) -> List[str]:
        seen = set()
        result = []

        for value in values:
            value = str(value).strip()

            if not value:
                continue

            key = value.lower()

            if key in seen:
                continue

            seen.add(key)
            result.append(value)

        return result

    def _classify(
        self,
        item: Dict[str, Any],
    ) -> str:
        kind = str(
            item.get(
                "kind",
                "unknown",
            )
        ).lower()

        if kind in {
            "concept",
            "fact",
            "relation",
            "context",
            "semantic",
        }:
            return kind

        return "unknown"

    def _extract_subject(
        self,
        value: Any,
    ) -> str:
        if isinstance(value, dict):
            return str(
                value.get(
                    "subject",
                    "",
                )
            ).strip()

        return ""

    def _extract_predicate(
        self,
        value: Any,
    ) -> str:
        if isinstance(value, dict):
            return str(
                value.get(
                    "predicate",
                    "",
                )
            ).strip()

        return ""

    def _extract_object(
        self,
        value: Any,
    ) -> Any:
        if isinstance(value, dict):
            if "value" in value:
                return value["value"]

            if "object" in value:
                return value["object"]

        return value

    def process(
        self,
        query: str,
        working_memory: List[
            Dict[str, Any]
        ],
    ) -> ProcessingResult:
        concepts = []
        facts = []
        relations = []
        derived = []
        state = {
            "input_count": len(
                working_memory
            ),
            "query": query,
        }

        for item in working_memory:
            kind = self._classify(item)
            value = item.get("value")

            if kind == "concept":
                concepts.append(
                    str(value)
                )

            elif kind == "fact":
                facts.append(
                    self._normalize_fact(
                        value
                    )
                )

            elif kind == "relation":
                relations.append(
                    self._normalize_relation(
                        value
                    )
                )

            elif kind in {
                "context",
                "semantic",
            }:
                derived.append(
                    {
                        "kind": kind,
                        "value": value,
                        "source": item.get(
                            "source",
                            "",
                        ),
                    }
                )

        concepts = self._unique_strings(
            concepts
        )

        facts = self._deduplicate(
            facts
        )

        relations = self._deduplicate(
            relations
        )

        derived.extend(
            self._derive_connections(
                concepts,
                facts,
                relations,
            )
        )

        state.update(
            {
                "concept_count": len(
                    concepts
                ),
                "fact_count": len(
                    facts
                ),
                "relation_count": len(
                    relations
                ),
                "derived_count": len(
                    derived
                ),
            }
        )

        return ProcessingResult(
            query=query,
            inputs=list(
                working_memory
            ),
            concepts=concepts,
            facts=facts,
            relations=relations,
            derived=derived,
            state=state,
        )

    def _normalize_fact(
        self,
        value: Any,
    ) -> Dict[str, Any]:
        if not isinstance(
            value,
            dict,
        ):
            return {
                "value": value
            }

        return {
            "subject": self._extract_subject(
                value
            ),
            "predicate": self._extract_predicate(
                value
            ),
            "value": self._extract_object(
                value
            ),
            "confidence": float(
                value.get(
                    "confidence",
                    1.0,
                )
            ),
            "source": str(
                value.get(
                    "source",
                    "",
                )
            ),
        }

    def _normalize_relation(
        self,
        value: Any,
    ) -> Dict[str, Any]:
        if not isinstance(
            value,
            dict,
        ):
            return {
                "value": value
            }

        return {
            "subject": self._extract_subject(
                value
            ),
            "predicate": self._extract_predicate(
                value
            ),
            "object": self._extract_object(
                value
            ),
            "confidence": float(
                value.get(
                    "confidence",
                    1.0,
                )
            ),
            "source": str(
                value.get(
                    "source",
                    "",
                )
            ),
        }

    def _deduplicate(
        self,
        values: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        seen = set()
        result = []

        for value in values:
            key = repr(
                sorted(
                    value.items()
                )
            )

            if key in seen:
                continue

            seen.add(key)
            result.append(value)

        return result

    def _derive_connections(
        self,
        concepts: List[str],
        facts: List[Dict[str, Any]],
        relations: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        derived = []

        known_subjects = set()

        for fact in facts:
            subject = str(
                fact.get(
                    "subject",
                    "",
                )
            ).strip()

            if subject:
                known_subjects.add(
                    subject.lower()
                )

        for relation in relations:
            subject = str(
                relation.get(
                    "subject",
                    "",
                )
            ).strip()

            object_value = str(
                relation.get(
                    "object",
                    "",
                )
            ).strip()

            predicate = str(
                relation.get(
                    "predicate",
                    "",
                )
            ).strip()

            if (
                subject
                and object_value
                and predicate
            ):
                derived.append(
                    {
                        "kind": "connection",
                        "subject": subject,
                        "predicate": predicate,
                        "object": object_value,
                        "basis": "working-memory relation",
                    }
                )

        for concept in concepts:
            if (
                concept.lower()
                in known_subjects
            ):
                derived.append(
                    {
                        "kind": "concept-fact-link",
                        "concept": concept,
                        "basis": "shared subject",
                    }
                )

        return derived

    def combine(
        self,
        first: ProcessingResult,
        second: ProcessingResult,
    ) -> ProcessingResult:
        return ProcessingResult(
            query=first.query
            or second.query,
            inputs=(
                first.inputs
                + second.inputs
            ),
            concepts=self._unique_strings(
                first.concepts
                + second.concepts
            ),
            facts=self._deduplicate(
                first.facts
                + second.facts
            ),
            relations=self._deduplicate(
                first.relations
                + second.relations
            ),
            derived=self._deduplicate(
                first.derived
                + second.derived
            ),
            state={
                "combined": True,
                "first_state": dict(
                    first.state
                ),
                "second_state": dict(
                    second.state
                ),
            },
        )

    def prepare_for_reasoning(
        self,
        result: ProcessingResult,
    ) -> Dict[str, Any]:
        return {
            "query": result.query,
            "concepts": list(
                result.concepts
            ),
            "facts": list(
                result.facts
            ),
            "relations": list(
                result.relations
            ),
            "derived": list(
                result.derived
            ),
            "ready": True,
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "operation_count": 5,
        }


__all__ = [
    "ProcessingResult",
    "KnowledgeProcessingEngine",
]
