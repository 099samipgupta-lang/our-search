from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from exploration.knowledge.consolidation import (
    KnowledgeConsolidationEngine,
)
from exploration.knowledge.extraction import (
    KnowledgeExtractionEngine,
)
from exploration.knowledge.learning import (
    KnowledgeLearningEngine,
)


class KnowledgeLearningCoordinator:
    VERSION = "knowledge-learning-coordinator.v1"

    def __init__(
        self,
        learning: Optional[KnowledgeLearningEngine] = None,
        extraction: Optional[KnowledgeExtractionEngine] = None,
        consolidation: Optional[
            KnowledgeConsolidationEngine
        ] = None,
    ):
        self.learning = (
            learning
            if learning is not None
            else KnowledgeLearningEngine()
        )

        self.extraction = (
            extraction
            if extraction is not None
            else KnowledgeExtractionEngine()
        )

        self.consolidation = (
            consolidation
            if consolidation is not None
            else KnowledgeConsolidationEngine()
        )

    def learn_structured(
        self,
        facts: Iterable[Dict[str, Any]] = (),
        concepts: Iterable[Dict[str, Any]] = (),
        source: str = "learning",
    ) -> Dict[str, Any]:
        result = self.consolidation.consolidate(
            facts=facts,
            concepts=concepts,
            source=source,
        )

        return {
            "version": self.VERSION,
            "mode": "structured",
            "result": result,
        }

    def learn_text(
        self,
        text: str,
        source: str = "text",
    ) -> Dict[str, Any]:
        extracted = self.extraction.extract(
            text
        )

        facts = []
        concepts = []
        relations = []

        for item in extracted.facts:
            facts.append(
                {
                    "subject": item.subject,
                    "predicate": item.predicate,
                    "value": item.value,
                    "confidence": getattr(
                        item,
                        "confidence",
                        0.7,
                    ),
                    "source": source,
                }
            )

        for item in extracted.concepts:
            concepts.append(
                {
                    "name": item.name,
                    "definition": item.definition,
                    "confidence": getattr(
                        item,
                        "confidence",
                        0.7,
                    ),
                    "source": source,
                }
            )

        for item in extracted.relationships:
            relations.append(
                {
                    "subject": item.subject,
                    "relation": item.predicate,
                    "target": item.value,
                    "confidence": getattr(
                        item,
                        "confidence",
                        0.7,
                    ),
                    "source": source,
                }
            )

        result = self.consolidation.consolidate(
            facts=facts,
            concepts=concepts,
            relations=relations,
            source=source,
        )

        return {
            "version": self.VERSION,
            "mode": "text",
            "extracted": {
                "facts": facts,
                "concepts": concepts,
                "relations": relations,
            },
            "result": result,
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "memory": self.consolidation.memory.stats(),
        }
