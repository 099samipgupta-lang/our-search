from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict

from exploration.knowledge.graph import KnowledgeGraph
from exploration.knowledge.memory import KnowledgeMemory
from exploration.knowledge.question import (
    KnowledgeQuestionUnderstandingEngine,
)
from exploration.knowledge.retrieval import (
    KnowledgeRetrievalEngine,
)
from exploration.knowledge.synthesis import (
    KnowledgeAnswerSynthesisEngine,
)
from exploration.knowledge.reasoning import (
    KnowledgeReasoningEngine,
)


class KnowledgeBrain:
    VERSION = "knowledge-brain.v1"

    def __init__(self):
        self.memory = KnowledgeMemory()
        self.graph = KnowledgeGraph()

        self.question = (
            KnowledgeQuestionUnderstandingEngine()
        )

        self.retrieval = KnowledgeRetrievalEngine(
            memory=self.memory
        )

        self.reasoning = KnowledgeReasoningEngine(
            graph=self.graph
        )

        self.synthesis = KnowledgeAnswerSynthesisEngine(
            question_engine=self.question,
            retrieval_engine=self.retrieval,
        )

    def think(
        self,
        query: str,
        limit: int = 8,
    ) -> Dict[str, Any]:
        understood = self.question.understand(query)

        matches = self.retrieval.retrieve(
            query,
            limit=limit,
        )

        reasoning_results = []

        for match in matches:
            value = match.record.value

            if match.record.kind == "fact":
                inference = self.reasoning.infer(
                    subject=str(
                        value.get("subject", "")
                    ),
                    predicate=str(
                        value.get("predicate", "")
                    ),
                    value=str(
                        value.get("value", "")
                    ),
                )

                reasoning_results.extend(
                    inference
                )

        answer = self.synthesis.synthesize(
            query,
            limit=limit,
        )

        return {
            "version": self.VERSION,
            "question": asdict(understood),
            "knowledge": [
                {
                    "kind": match.record.kind,
                    "key": match.record.key,
                    "value": match.record.value,
                    "confidence": match.record.confidence,
                    "source": match.record.source,
                    "score": match.score,
                    "matched_terms": match.matched_terms,
                }
                for match in matches
            ],
            "reasoning": [
                asdict(item)
                if hasattr(item, "__dataclass_fields__")
                else item
                for item in reasoning_results
            ],
            "answer": asdict(answer),
            "memory": self.memory.stats(),
        }

    def remember_fact(
        self,
        subject: str,
        predicate: str,
        value: str,
        category: str = "",
        source: str = "internal",
        confidence: float = 1.0,
    ) -> None:
        self.memory.remember_fact(
            subject=subject,
            predicate=predicate,
            value=value,
            category=category,
            source=source,
            confidence=confidence,
        )

        self.graph.connect_facts(
            [
                {
                    "subject": subject,
                    "predicate": predicate,
                    "value": value,
                }
            ]
        )

        self.memory.save()

    def remember_concept(
        self,
        name: str,
        definition: str,
        category: str = "",
        source: str = "internal",
        confidence: float = 1.0,
    ) -> None:
        self.memory.remember_concept(
            name=name,
            definition=definition,
            category=category,
            source=source,
            confidence=confidence,
        )

        self.graph.add_node(
            name,
            node_type="concept",
            metadata={
                "definition": definition,
                "category": category,
            },
        )

        self.memory.save()

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "memory": self.memory.stats(),
            "graph": self.graph.snapshot(),
        }
