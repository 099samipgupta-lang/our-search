from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass
class CapabilityRoute:
    name: str
    score: float
    reason: str


class KnowledgeCapabilityRouter:
    VERSION = "knowledge-router.v1"

    def __init__(self):
        self._capabilities: Dict[
            str,
            Any,
        ] = {}

    def register(
        self,
        name: str,
        capability: Any,
        priority: int = 0,
    ) -> None:
        self._capabilities[
            name.strip().lower()
        ] = {
            "name": name.strip(),
            "capability": capability,
            "priority": int(priority),
        }

    def unregister(
        self,
        name: str,
    ) -> bool:
        return (
            self._capabilities.pop(
                name.strip().lower(),
                None,
            )
            is not None
        )

    def _can_handle(
        self,
        capability: Any,
        query: str,
    ) -> bool:
        method = getattr(
            capability,
            "can_handle",
            None,
        )

        if not callable(method):
            return False

        try:
            return bool(
                method(query)
            )
        except Exception:
            return False

    def route(
        self,
        query: str,
        limit: int = 3,
    ) -> List[CapabilityRoute]:
        routes: List[
            CapabilityRoute
        ] = []

        normalized = query.strip().lower()

        for entry in self._capabilities.values():
            capability = entry["capability"]

            if not self._can_handle(
                capability,
                normalized,
            ):
                continue

            score = float(
                entry["priority"]
            )

            routes.append(
                CapabilityRoute(
                    name=entry["name"],
                    score=score,
                    reason=(
                        "Capability accepted "
                        "the query."
                    ),
                )
            )

        routes.sort(
            key=lambda route: (
                -route.score,
                route.name.lower(),
            )
        )

        return routes[
            : max(0, int(limit))
        ]

    def select(
        self,
        query: str,
    ) -> Optional[str]:
        routes = self.route(
            query,
            limit=1,
        )

        if not routes:
            return None

        return routes[0].name

    def execute(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        selected = self.select(query)

        if selected is None:
            return None

        capability = self._capabilities[
            selected.lower()
        ]["capability"]

        handler = getattr(
            capability,
            "calculate",
            None,
        )

        if callable(handler):
            return handler(
                query,
                *args,
                **kwargs,
            )

        handler = getattr(
            capability,
            "analyze",
            None,
        )

        if callable(handler):
            return handler(
                query,
                *args,
                **kwargs,
            )

        handler = getattr(
            capability,
            "search",
            None,
        )

        if callable(handler):
            return handler(
                query,
                *args,
                **kwargs,
            )

        return None

    def stats(self) -> Dict[str, int]:
        return {
            "version": self.VERSION,
            "registered_capabilities": len(
                self._capabilities
            ),
        }
