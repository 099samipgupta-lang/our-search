from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass
class KnowledgeCapability:
    name: str
    description: str
    handler: Callable[..., Any]
    priority: int = 0


class KnowledgeCapabilityEngine:
    VERSION = "knowledge-capabilities.v1"

    def __init__(self):
        self._capabilities: Dict[str, KnowledgeCapability] = {}

    def register(
        self,
        name: str,
        description: str,
        handler: Callable[..., Any],
        priority: int = 0,
    ) -> None:
        key = name.strip().lower()

        if not key:
            raise ValueError(
                "Capability name cannot be empty."
            )

        self._capabilities[key] = KnowledgeCapability(
            name=name.strip(),
            description=description.strip(),
            handler=handler,
            priority=int(priority),
        )

    def unregister(self, name: str) -> bool:
        key = name.strip().lower()
        return self._capabilities.pop(
            key,
            None,
        ) is not None

    def get(
        self,
        name: str,
    ) -> Optional[KnowledgeCapability]:
        return self._capabilities.get(
            name.strip().lower()
        )

    def list_capabilities(self) -> List[Dict[str, Any]]:
        capabilities = sorted(
            self._capabilities.values(),
            key=lambda item: (
                -item.priority,
                item.name.lower(),
            ),
        )

        return [
            {
                "name": capability.name,
                "description": capability.description,
                "priority": capability.priority,
            }
            for capability in capabilities
        ]

    def execute(
        self,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        capability = self.get(name)

        if capability is None:
            raise KeyError(
                f"Unknown knowledge capability: {name}"
            )

        return capability.handler(
            *args,
            **kwargs,
        )

    def match(
        self,
        query: str,
    ) -> List[KnowledgeCapability]:
        text = query.lower()

        matches = []

        for capability in self._capabilities.values():
            name_terms = set(
                capability.name.lower().split()
            )

            description_terms = set(
                capability.description.lower().split()
            )

            query_terms = set(text.split())

            overlap = len(
                query_terms
                & (name_terms | description_terms)
            )

            if overlap:
                matches.append(
                    (
                        overlap,
                        capability.priority,
                        capability,
                    )
                )

        matches.sort(
            key=lambda item: (
                -item[0],
                -item[1],
                item[2].name.lower(),
            )
        )

        return [
            capability
            for _, _, capability in matches
        ]

    def stats(self) -> Dict[str, int]:
        return {
            "version": self.VERSION,
            "capabilities": len(
                self._capabilities
            ),
        }
