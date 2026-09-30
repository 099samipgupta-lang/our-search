from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class WorkingMemoryItem:
    key: str
    kind: str
    value: Any
    activation: float = 0.0
    relevance: float = 0.0
    source: str = ""
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeWorkingMemoryEngine:
    VERSION = "knowledge-working-memory.v1"

    def __init__(
        self,
        capacity: int = 64,
    ):
        self.capacity = max(
            1,
            int(capacity),
        )
        self.items: Dict[
            str,
            WorkingMemoryItem,
        ] = {}

    def _key(
        self,
        item: Dict[str, Any],
    ) -> str:
        if item.get("key"):
            return str(
                item["key"]
            )

        kind = str(
            item.get("kind", "unknown")
        )

        value = str(
            item.get("value", "")
        )

        return (
            f"{kind}:"
            f"{value.lower().strip()}"
        )

    def _score(
        self,
        item: WorkingMemoryItem,
    ) -> float:
        return (
            float(item.activation)
            * 0.6
            + float(item.relevance)
            * 0.4
        )

    def remember(
        self,
        item: Dict[str, Any],
    ) -> WorkingMemoryItem:
        key = self._key(item)

        memory_item = WorkingMemoryItem(
            key=key,
            kind=str(
                item.get(
                    "kind",
                    "unknown",
                )
            ),
            value=item.get("value"),
            activation=float(
                item.get(
                    "energy",
                    item.get(
                        "activation",
                        0.0,
                    ),
                )
            ),
            relevance=float(
                item.get(
                    "score",
                    item.get(
                        "relevance",
                        0.0,
                    ),
                )
            ),
            source=str(
                item.get(
                    "source",
                    "",
                )
            ),
            metadata=dict(
                item.get(
                    "metadata",
                    {},
                )
            ),
        )

        self.items[key] = memory_item
        self._trim()

        return memory_item

    def remember_many(
        self,
        items: List[Dict[str, Any]],
    ) -> List[WorkingMemoryItem]:
        remembered = []

        for item in items:
            remembered.append(
                self.remember(item)
            )

        return remembered

    def recall(
        self,
        query: str = "",
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        query_terms = {
            term.lower()
            for term in query.split()
            if term.strip()
        }

        scored = []

        for item in self.items.values():
            text = str(
                item.value
            ).lower()

            overlap = sum(
                1
                for term in query_terms
                if term in text
            )

            query_score = (
                overlap
                / max(
                    1,
                    len(query_terms),
                )
            )

            score = (
                self._score(item)
                * 0.7
                + query_score
                * 0.3
            )

            scored.append(
                (
                    score,
                    item,
                )
            )

        scored.sort(
            key=lambda pair: pair[0],
            reverse=True,
        )

        return [
            {
                "key": item.key,
                "kind": item.kind,
                "value": item.value,
                "activation": item.activation,
                "relevance": item.relevance,
                "score": score,
                "source": item.source,
                "metadata": dict(
                    item.metadata
                ),
            }
            for score, item in scored[
                :max(0, int(limit))
            ]
        ]

    def strengthen(
        self,
        key: str,
        amount: float = 0.1,
    ) -> bool:
        item = self.items.get(key)

        if item is None:
            return False

        item.activation = min(
            1.0,
            item.activation
            + max(0.0, amount),
        )

        return True

    def weaken(
        self,
        key: str,
        amount: float = 0.1,
    ) -> bool:
        item = self.items.get(key)

        if item is None:
            return False

        item.activation = max(
            0.0,
            item.activation
            - max(0.0, amount),
        )

        return True

    def forget(
        self,
        key: str,
    ) -> bool:
        if key not in self.items:
            return False

        del self.items[key]
        return True

    def clear(self) -> None:
        self.items.clear()

    def _trim(self) -> None:
        if len(self.items) <= self.capacity:
            return

        ordered = sorted(
            self.items.values(),
            key=self._score,
            reverse=True,
        )

        keep = ordered[
            :self.capacity
        ]

        self.items = {
            item.key: item
            for item in keep
        }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "capacity": self.capacity,
            "size": len(
                self.items
            ),
            "items": [
                {
                    "key": item.key,
                    "kind": item.kind,
                    "value": item.value,
                    "activation": item.activation,
                    "relevance": item.relevance,
                    "source": item.source,
                    "metadata": dict(
                        item.metadata
                    ),
                }
                for item in self.items.values()
            ],
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "capacity": self.capacity,
            "item_count": len(
                self.items
            ),
        }


__all__ = [
    "WorkingMemoryItem",
    "KnowledgeWorkingMemoryEngine",
]
