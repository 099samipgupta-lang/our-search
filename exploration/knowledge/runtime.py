from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from exploration.knowledge.brain import KnowledgeBrain
from exploration.knowledge.learning_coordinator import (
    KnowledgeLearningCoordinator,
)
from exploration.knowledge.capability_integration import (
    KnowledgeCapabilityIntegration,
)
from exploration.knowledge.source_manager import (
    KnowledgeSourceManager,
)
from exploration.knowledge.provenance import (
    KnowledgeProvenanceEngine,
)
from exploration.knowledge.confidence import (
    KnowledgeConfidenceEngine,
)
from exploration.knowledge.trust import (
    KnowledgeTrustEngine,
)
from exploration.knowledge.conflict import (
    KnowledgeConflictResolutionEngine,
)
from exploration.knowledge.temporal_integration import (
    KnowledgeTemporalIntegration,
)
from exploration.knowledge.evolution import (
    KnowledgeEvolutionEngine,
)
from exploration.knowledge.observation import (
    KnowledgeObservationEngine,
)
from exploration.knowledge.hypothesis import (
    KnowledgeHypothesisEngine,
)
from exploration.knowledge.evidence import (
    KnowledgeEvidenceEngine,
)
from exploration.knowledge.causality import (
    KnowledgeCausalityEngine,
)
from exploration.knowledge.analogy import (
    KnowledgeAnalogyEngine,
)
from exploration.knowledge.planning import (
    KnowledgePlanningEngine,
)
from exploration.knowledge.decision import (
    KnowledgeDecisionEngine,
)
from exploration.knowledge.uncertainty import (
    KnowledgeUncertaintyEngine,
)
from exploration.knowledge.context import (
    KnowledgeContextEngine,
)
from exploration.knowledge.semantic import (
    KnowledgeSemanticEngine,
)
from exploration.knowledge.composition import (
    KnowledgeCompositionEngine,
)
from exploration.knowledge.attention import (
    KnowledgeAttentionEngine,
)
from exploration.knowledge.activation import (
    KnowledgeActivationEngine,
)
from exploration.knowledge.working_memory import (
    KnowledgeWorkingMemoryEngine,
)
from exploration.knowledge.processing import (
    KnowledgeProcessingEngine,
)
from exploration.knowledge.reasoning_coordinator import (
    KnowledgeReasoningCoordinator,
)
from exploration.knowledge.answer_construction import (
    KnowledgeAnswerConstructionEngine,
)


@dataclass
class KnowledgeRuntimeResult:
    query: str
    knowledge: Dict[str, Any] = field(
        default_factory=dict
    )
    attention: Dict[str, Any] = field(
        default_factory=dict
    )
    activation: list = field(
        default_factory=list
    )
    working_memory: list = field(
        default_factory=list
    )
    processing: Dict[str, Any] = field(
        default_factory=dict
    )
    reasoning: Dict[str, Any] = field(
        default_factory=dict
    )
    answer: Dict[str, Any] = field(
        default_factory=dict
    )
    capabilities: Dict[str, Any] = field(
        default_factory=dict
    )
    ready: bool = False


class KnowledgeRuntimeEngine:
    VERSION = "knowledge-runtime.v2"

    def __init__(
        self,
        working_memory_capacity: int = 64,
    ):
        # Core knowledge brain.
        self.knowledge_brain = (
            KnowledgeBrain()
        )

        # Learning and acquisition.
        self.learning = (
            KnowledgeLearningCoordinator()
        )
        self.sources = (
            KnowledgeSourceManager()
        )

        # Trust, validation lineage and evolution.
        self.provenance = (
            KnowledgeProvenanceEngine()
        )
        self.confidence = (
            KnowledgeConfidenceEngine()
        )
        self.trust = (
            KnowledgeTrustEngine()
        )
        self.conflict = (
            KnowledgeConflictResolutionEngine()
        )
        self.temporal = (
            KnowledgeTemporalIntegration()
        )
        self.evolution = (
            KnowledgeEvolutionEngine()
        )

        # Observation and evidence.
        self.observation = (
            KnowledgeObservationEngine()
        )
        self.hypothesis = (
            KnowledgeHypothesisEngine()
        )
        self.evidence = (
            KnowledgeEvidenceEngine()
        )

        # Higher-order knowledge.
        self.causality = (
            KnowledgeCausalityEngine()
        )
        self.analogy = (
            KnowledgeAnalogyEngine()
        )
        self.planning = (
            KnowledgePlanningEngine()
        )
        self.decision = (
            KnowledgeDecisionEngine()
        )
        self.uncertainty = (
            KnowledgeUncertaintyEngine()
        )
        self.context = (
            KnowledgeContextEngine()
        )

        # Semantic understanding.
        self.semantic = (
            KnowledgeSemanticEngine()
        )
        self.composition = (
            KnowledgeCompositionEngine()
        )
        self.attention = (
            KnowledgeAttentionEngine()
        )
        self.activation = (
            KnowledgeActivationEngine()
        )
        self.working_memory = (
            KnowledgeWorkingMemoryEngine(
                capacity=working_memory_capacity
            )
        )
        self.processing = (
            KnowledgeProcessingEngine()
        )
        self.reasoning = (
            KnowledgeReasoningCoordinator()
        )
        self.answer = (
            KnowledgeAnswerConstructionEngine()
        )

        # Capability architecture.
        self.capabilities = (
            KnowledgeCapabilityIntegration()
        )

    def learn_text(
        self,
        text: str,
        source: str = "internal",
    ) -> Dict[str, Any]:
        result = self.learning.learn_text(
            text=text,
            source=source,
        )

        return self._to_dict(result)

    def learn_fact(
        self,
        subject: str,
        predicate: str,
        value: Any,
        source: str = "internal",
        confidence: float = 1.0,
    ) -> Any:
        return self.learning.learn_structured(
            facts=[
                {
                    "subject": subject,
                    "predicate": predicate,
                    "value": value,
                    "source": source,
                    "confidence": confidence,
                }
            ]
        )

    def think(
        self,
        query: str,
        meaning: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeRuntimeResult:
        # The unified internal meaning is supplied
        # directly when available. Otherwise the
        # internal KnowledgeBrain supplies it.
        if meaning is None:
            brain_result = self.knowledge_brain.think(
                query=query
            )
            meaning = self._to_dict(
                brain_result
            )

        # Semantic composition is part of the
        # complete runtime, not a detached subsystem.
        subject = str(
            meaning.get(
                "subject",
                query,
            )
        )

        self.composition.compose(
            subject=subject,
            parts=meaning,
        )

        composed = (
            self.composition.summary(
                subject
            )
        )

        # Attention selects what matters now.
        attention = self.attention.focus(
            query=query,
            meaning=composed,
        )

        # Activation turns selected knowledge
        # into active processing material.
        activated = self.activation.activate(
            attended=attention.get(
                "items",
                [],
            ),
            query=query,
        )

        # Working memory holds the current
        # active state.
        self.working_memory.clear()
        self.working_memory.remember_many(
            activated
        )

        working_memory = (
            self.working_memory.recall(
                query=query,
                limit=self.working_memory.capacity,
            )
        )

        # Processing prepares structured knowledge
        # for the reasoning layer.
        processed_result = (
            self.processing.process(
                query=query,
                working_memory=working_memory,
            )
        )

        processed = {
            "query": processed_result.query,
            "concepts": processed_result.concepts,
            "facts": processed_result.facts,
            "relations": processed_result.relations,
            "derived": processed_result.derived,
            "state": processed_result.state,
        }

        # Reasoning coordinates inference,
        # causality and logical processing.
        reasoning_result = (
            self.reasoning.reason(
                query=query,
                processed=processed,
            )
        )

        reasoning = {
            "query": reasoning_result.query,
            "inferences": (
                reasoning_result.inferences
            ),
            "causal": reasoning_result.causal,
            "logical": reasoning_result.logical,
            "conclusions": (
                reasoning_result.conclusions
            ),
            "confidence": (
                reasoning_result.confidence
            ),
            "ready": reasoning_result.ready,
        }

        # Final internal answer construction.
        answer_result = (
            self.answer.construct(
                query=query,
                processed=processed,
                reasoning=reasoning,
            )
        )

        answer = self.answer.to_dict(
            answer_result
        )

        return KnowledgeRuntimeResult(
            query=query,
            knowledge=composed,
            attention=attention,
            activation=activated,
            working_memory=working_memory,
            processing=processed,
            reasoning=reasoning,
            answer=answer,
            capabilities=(
                self.capabilities.stats()
            ),
            ready=True,
        )

    def run(
        self,
        query: str,
        meaning: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeRuntimeResult:
        return self.think(
            query=query,
            meaning=meaning,
        )

    def run_dict(
        self,
        query: str,
        meaning: Optional[
            Dict[str, Any]
        ] = None,
    ) -> Dict[str, Any]:
        result = self.think(
            query=query,
            meaning=meaning,
        )

        return {
            "version": self.VERSION,
            "query": result.query,
            "knowledge": result.knowledge,
            "attention": result.attention,
            "activation": result.activation,
            "working_memory": (
                result.working_memory
            ),
            "processing": result.processing,
            "reasoning": result.reasoning,
            "answer": result.answer,
            "capabilities": result.capabilities,
            "ready": result.ready,
        }

    def clear_working_memory(self) -> None:
        self.working_memory.clear()

    def _to_dict(
        self,
        value: Any,
    ) -> Dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)

        if hasattr(value, "__dict__"):
            return dict(
                value.__dict__
            )

        return {
            "value": value
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "complete_knowledge_system": True,
            "components": [
                "knowledge_brain",
                "learning",
                "sources",
                "provenance",
                "confidence",
                "trust",
                "conflict",
                "temporal",
                "evolution",
                "observation",
                "hypothesis",
                "evidence",
                "causality",
                "analogy",
                "planning",
                "decision",
                "uncertainty",
                "context",
                "semantic",
                "composition",
                "attention",
                "activation",
                "working_memory",
                "processing",
                "reasoning",
                "answer",
                "capabilities",
            ],
            "component_count": 27,
            "working_memory": (
                self.working_memory.stats()
            ),
        }


__all__ = [
    "KnowledgeRuntimeResult",
    "KnowledgeRuntimeEngine",
]
