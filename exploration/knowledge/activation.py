from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Set


@dataclass
class Activation:
    kind: str
    key: str
    value: Any
    energy: float
    source: str = ""
    links: Set[str] = field(
        default_factory=set
    )


class KnowledgeActivationEngine:
    VERSION = "knowledge-activation.v1"

    def __init__(self):
        self.activations: Dict[str, Activation] = {}

    def _key(
        self,
        kind: str,
        value: Any,
    ) -> str:
        return (
            f"{kind}:"
            f"{str(value).lower().strip()}"
        )

    def _activate(
        self,
        kind: str,
        value: Any,
        energy: float,
        source: str = "",
    ) -> Activation:
        key = self._key(
            kind,
            value,
        )

        activation = self.activations.get(key)

        if activation is None:
            activation = Activation(
                kind=kind,
                key=key,
                value=value,
                energy=max(
                    0.0,
                    min(1.0, energy),
                ),
                source=source,
            )
            self.activations[key] = activation
        else:
            activation.energy = max(
                activation.energy,
                min(1.0, energy),
            )

        return activation

    def activate(
        self,
        attended: List[Dict[str, Any]],
        query: str = "",
    ) -> List[Dict[str, Any]]:
        activated: List[Activation] = []

        for item in attended:
            kind = str(
                item.get("kind", "unknown")
            )
            value = item.get("value")
            score = float(
                item.get("score", 0.0)
            )
            reason = str(
                item.get("reason", "")
            )

            activation = self._activate(
                kind=kind,
                value=value,
                energy=score,
                source=reason,
            )

            activated.append(activation)

        activated.sort(
            key=lambda item: item.energy,
            reverse=True,
        )

        return [
            {
                "kind": item.kind,
                "key": item.key,
                "value": item.value,
                "energy": item.energy,
                "source": item.source,
                "links": sorted(item.links),
            }
            for item in activated
        ]

    def spread(
        self,
        activations: List[Dict[str, Any]],
        links: Dict[str, List[str]] | None = None,
        decay: float = 0.75,
    ) -> List[Dict[str, Any]]:
        links = links or {}
        results = list(activations)

        for item in activations:
            key = str(
                item.get("key", "")
            )
            energy = float(
                item.get("energy", 0.0)
            )

            for linked_key in links.get(
                key,
                [],
            ):
                linked_energy = energy * decay

                existing = next(
                    (
                        result
                        for result in results
                        if result.get("key")
                        == linked_key
                    ),
                    None,
                )

                if existing is None:
                    results.append(
                        {
                            "kind": "linked",
                            "key": linked_key,
                            "value": linked_key,
                            "energy": linked_energy,
                            "source": "activation spread",
                            "links": [],
                        }
                    )
                else:
                    existing["energy"] = max(
                        float(
                            existing.get(
                                "energy",
                                0.0,
                            )
                        ),
                        linked_energy,
                    )

        results.sort(
            key=lambda item: float(
                item.get("energy", 0.0)
            ),
            reverse=True,
        )

        return results

    def strongest(
        self,
        activations: List[Dict[str, Any]],
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        ordered = sorted(
            activations,
            key=lambda item: float(
                item.get("energy", 0.0)
            ),
            reverse=True,
        )

        return ordered[
            :max(0, int(limit))
        ]

    def clear(self) -> None:
        self.activations.clear()

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "activation_count": len(
                self.activations
            ),
        }


__all__ = [
    "Activation",
    "KnowledgeActivationEngine",
]
