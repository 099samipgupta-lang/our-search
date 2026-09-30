from __future__ import annotations

from typing import Any, Dict, Iterable, List

from exploration.knowledge.memory import KnowledgeMemory
from exploration.knowledge.validation import KnowledgeValidationEngine


class KnowledgeConsolidationEngine:
    VERSION = "knowledge-consolidation.v1"

    def __init__(
        self,
        memory: KnowledgeMemory | None = None,
        validator: KnowledgeValidationEngine | None = None,
    ):
        self.memory = memory if memory is not None else KnowledgeMemory()
        self.validator = (
            validator
            if validator is not None
            else KnowledgeValidationEngine()
        )

    def consolidate_facts(
        self,
        facts: Iterable[Dict[str, Any]],
        source: str = "consolidated",
    ) -> Dict[str, Any]:
        accepted: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []

        for fact in facts:
            subject = str(fact.get("subject", "")).strip()
            predicate = str(fact.get("predicate", "")).strip()
            value = str(fact.get("value", "")).strip()
            confidence = float(fact.get("confidence", 1.0))

            candidate = {
                "subject": subject,
                "predicate": predicate,
                "value": value,
                "confidence": confidence,
            }

            validation = self.validator.validate_fact(candidate)

            if not validation.valid:
                rejected.append(
                    {
                        "fact": candidate,
                        "reasons": [
                            finding.message
                            for finding in validation.findings
                        ],
                    }
                )
                continue

            self.memory.remember_fact(
                subject=subject,
                predicate=predicate,
                value=value,
                category=str(fact.get("category", "")),
                source=str(fact.get("source", source)),
                confidence=confidence,
            )

            accepted.append(candidate)

        self.memory.save()

        return {
            "version": self.VERSION,
            "accepted": accepted,
            "rejected": rejected,
            "memory": self.memory.stats(),
        }

    def consolidate_concepts(
        self,
        concepts: Iterable[Dict[str, Any]],
        source: str = "consolidated",
    ) -> Dict[str, Any]:
        accepted = []
        rejected = []

        for concept in concepts:
            name = str(concept.get("name", "")).strip()
            definition = str(concept.get("definition", "")).strip()

            if not name or not definition:
                rejected.append(dict(concept))
                continue

            self.memory.remember_concept(
                name=name,
                definition=definition,
                category=str(concept.get("category", "")),
                source=str(concept.get("source", source)),
                confidence=float(concept.get("confidence", 1.0)),
            )

            accepted.append(
                {
                    "name": name,
                    "definition": definition,
                }
            )

        self.memory.save()

        return {
            "version": self.VERSION,
            "accepted": accepted,
            "rejected": rejected,
            "memory": self.memory.stats(),
        }

    def consolidate_relations(
        self,
        relations: Iterable[Dict[str, Any]],
        source: str = "consolidated",
    ) -> Dict[str, Any]:
        accepted = []
        rejected = []

        for relation in relations:
            subject = str(relation.get("subject", "")).strip()
            predicate = str(
                relation.get(
                    "relation",
                    relation.get("predicate", ""),
                )
            ).strip()
            target = str(
                relation.get(
                    "target",
                    relation.get("value", ""),
                )
            ).strip()

            if not subject or not predicate or not target:
                rejected.append(dict(relation))
                continue

            self.memory.remember_relation(
                subject=subject,
                relation=predicate,
                target=target,
                source=str(relation.get("source", source)),
                confidence=float(
                    relation.get("confidence", 1.0)
                ),
            )

            accepted.append(
                {
                    "subject": subject,
                    "relation": predicate,
                    "target": target,
                }
            )

        self.memory.save()

        return {
            "version": self.VERSION,
            "accepted": accepted,
            "rejected": rejected,
            "memory": self.memory.stats(),
        }

    def consolidate(
        self,
        facts: Iterable[Dict[str, Any]] = (),
        concepts: Iterable[Dict[str, Any]] = (),
        relations: Iterable[Dict[str, Any]] = (),
        source: str = "consolidated",
    ) -> Dict[str, Any]:
        fact_result = self.consolidate_facts(
            facts,
            source=source,
        )

        concept_result = self.consolidate_concepts(
            concepts,
            source=source,
        )

        relation_result = self.consolidate_relations(
            relations,
            source=source,
        )

        return {
            "version": self.VERSION,
            "facts": fact_result,
            "concepts": concept_result,
            "relations": relation_result,
            "memory": self.memory.stats(),
        }
