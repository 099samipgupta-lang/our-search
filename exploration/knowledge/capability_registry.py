from __future__ import annotations

from typing import Any

from exploration.knowledge.capabilities import (
    KnowledgeCapabilityEngine,
)
from exploration.knowledge.language_engine import (
    LanguageCapabilityEngine,
)
from exploration.knowledge.logic_engine import (
    LogicCapabilityEngine,
)
from exploration.knowledge.math_engine import (
    MathCapabilityEngine,
)
from exploration.knowledge.science_engine import (
    ScienceCapabilityEngine,
)
from exploration.knowledge.orchestrator import (
    KnowledgeCapabilityOrchestrator,
)


class KnowledgeCapabilityRegistry:
    VERSION = "knowledge-capability-registry.v1"

    def __init__(self):
        self.capabilities = (
            KnowledgeCapabilityEngine()
        )

        self.language = (
            LanguageCapabilityEngine()
        )

        self.logic = (
            LogicCapabilityEngine()
        )

        self.math = (
            MathCapabilityEngine()
        )

        self.science = (
            ScienceCapabilityEngine()
        )

        self.orchestrator = (
            KnowledgeCapabilityOrchestrator()
        )

        self._register_defaults()

    def _register_defaults(self) -> None:
        self.capabilities.register(
            name="language",
            description=(
                "Language normalization, "
                "tokenization and analysis."
            ),
            handler=self.language.analyze,
            priority=10,
        )

        self.capabilities.register(
            name="logic",
            description=(
                "Logical statements and "
                "deductive reasoning."
            ),
            handler=self.logic.chain,
            priority=20,
        )

        self.capabilities.register(
            name="mathematics",
            description=(
                "Mathematical calculation "
                "and expression evaluation."
            ),
            handler=self.math.calculate,
            priority=30,
        )

        self.capabilities.register(
            name="science",
            description=(
                "Scientific concepts and "
                "relationships."
            ),
            handler=self.science.search,
            priority=20,
        )

        self.orchestrator.register(
            name="language",
            capability=self.language,
            priority=10,
        )

        self.orchestrator.register(
            name="logic",
            capability=self.logic,
            priority=20,
        )

        self.orchestrator.register(
            name="mathematics",
            capability=self.math,
            priority=30,
        )

        self.orchestrator.register(
            name="science",
            capability=self.science,
            priority=20,
        )

    def route(
        self,
        query: str,
    ) -> Any:
        return self.orchestrator.execute(
            query
        )

    def list_capabilities(self):
        return self.capabilities.list_capabilities()

    def stats(self):
        return {
            "version": self.VERSION,
            "capability_engine": (
                self.capabilities.stats()
            ),
            "orchestrator": (
                self.orchestrator.stats()
            ),
        }
