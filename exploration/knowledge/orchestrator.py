from __future__ import annotations

from typing import Any, Dict, List

from exploration.knowledge.router import (
    KnowledgeCapabilityRouter,
)


class KnowledgeCapabilityOrchestrator:
    VERSION = "knowledge-orchestrator.v1"

    def __init__(
        self,
        router: KnowledgeCapabilityRouter | None = None,
    ):
        self.router = (
            router
            if router is not None
            else KnowledgeCapabilityRouter()
        )

    def register(
        self,
        name: str,
        capability: Any,
        priority: int = 0,
    ) -> None:
        self.router.register(
            name=name,
            capability=capability,
            priority=priority,
        )

    def plan(
        self,
        query: str,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        routes = self.router.route(
            query=query,
            limit=limit,
        )

        return [
            {
                "capability": route.name,
                "score": route.score,
                "reason": route.reason,
            }
            for route in routes
        ]

    def execute(
        self,
        query: str,
    ) -> Dict[str, Any]:
        plan = self.plan(
            query=query,
            limit=5,
        )

        results = []

        for item in plan:
            name = item["capability"]

            entry = self.router._capabilities.get(
                name.lower()
            )

            if entry is None:
                continue

            capability = entry["capability"]
            result = None

            for method_name in (
                "calculate",
                "analyze",
                "search",
                "explain",
            ):
                method = getattr(
                    capability,
                    method_name,
                    None,
                )

                if not callable(method):
                    continue

                try:
                    result = method(query)
                except Exception as exc:
                    result = {
                        "success": False,
                        "error": str(exc),
                    }

                break

            results.append(
                {
                    "capability": name,
                    "result": result,
                }
            )

        return {
            "version": self.VERSION,
            "query": query,
            "plan": plan,
            "results": results,
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "router": self.router.stats(),
        }
