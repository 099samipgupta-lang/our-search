from __future__ import annotations

from typing import Any, Dict, Optional

from exploration.knowledge.capability_registry import (
    KnowledgeCapabilityRegistry,
)
from exploration.knowledge.brain import KnowledgeBrain


class KnowledgeCapabilityIntegration:
    VERSION = "knowledge-capability-integration.v1"

    def __init__(
        self,
        brain: Optional[KnowledgeBrain] = None,
        registry: Optional[KnowledgeCapabilityRegistry] = None,
    ):
        self.brain = (
            brain
            if brain is not None
            else KnowledgeBrain()
        )

        self.registry = (
            registry
            if registry is not None
            else KnowledgeCapabilityRegistry()
        )

    def think(
        self,
        query: str,
        limit: int = 8,
    ) -> Dict[str, Any]:
        knowledge_result = self.brain.think(
            query=query,
            limit=limit,
        )

        capability_result = (
            self.registry.route(query)
        )

        return {
            "version": self.VERSION,
            "query": query,
            "knowledge": knowledge_result,
            "capabilities": capability_result,
        }

    def capabilities(
        self,
        query: str,
    ) -> Dict[str, Any]:
        return {
            "query": query,
            "plan": self.registry.orchestrator.plan(
                query=query,
            ),
            "stats": self.registry.stats(),
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "brain": self.brain.stats(),
            "capabilities": self.registry.stats(),
        }
