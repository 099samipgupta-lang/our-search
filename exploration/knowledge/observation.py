from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgeObservation:
    observation_id: str
    source_id: str
    content: Any
    observed_at: str
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


class KnowledgeObservationEngine:
    VERSION = "knowledge-observation.v1"

    def __init__(self):
        self.observations: Dict[
            str, KnowledgeObservation
        ] = {}

    def _now(self) -> str:
        return datetime.now(
            timezone.utc
        ).isoformat()

    def observe(
        self,
        observation_id: str,
        source_id: str,
        content: Any,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeObservation:
        observation = KnowledgeObservation(
            observation_id=observation_id,
            source_id=source_id,
            content=content,
            observed_at=self._now(),
            metadata=dict(metadata or {}),
        )

        self.observations[
            observation_id
        ] = observation

        return observation

    def get(
        self,
        observation_id: str,
    ) -> Optional[KnowledgeObservation]:
        return self.observations.get(
            observation_id
        )

    def by_source(
        self,
        source_id: str,
    ) -> List[KnowledgeObservation]:
        return [
            observation
            for observation
            in self.observations.values()
            if observation.source_id == source_id
        ]

    def search(
        self,
        term: str,
    ) -> List[KnowledgeObservation]:
        normalized = str(term).lower().strip()

        if not normalized:
            return []

        matches = []

        for observation in self.observations.values():
            content = str(
                observation.content
            ).lower()

            if normalized in content:
                matches.append(observation)

        return matches

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "observation_count": len(
                self.observations
            ),
        }


__all__ = [
    "KnowledgeObservation",
    "KnowledgeObservationEngine",
]
