from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class KnowledgeVersion:
    knowledge_id: str
    version: int
    value: Any
    status: str
    created_at: str
    metadata: Dict[str, Any]


class KnowledgeEvolutionEngine:
    VERSION = "knowledge-evolution.v1"

    def __init__(self):
        self.history: Dict[
            str, List[KnowledgeVersion]
        ] = {}

    def _now(self) -> str:
        return datetime.now(
            timezone.utc
        ).isoformat()

    def add_version(
        self,
        knowledge_id: str,
        value: Any,
        status: str = "active",
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeVersion:
        versions = self.history.setdefault(
            knowledge_id,
            [],
        )

        version = KnowledgeVersion(
            knowledge_id=knowledge_id,
            version=len(versions) + 1,
            value=value,
            status=status,
            created_at=self._now(),
            metadata=dict(metadata or {}),
        )

        versions.append(version)
        return version

    def current(
        self,
        knowledge_id: str,
    ) -> Optional[KnowledgeVersion]:
        versions = self.history.get(
            knowledge_id,
            [],
        )

        for version in reversed(versions):
            if version.status == "active":
                return version

        return None

    def history_for(
        self,
        knowledge_id: str,
    ) -> List[KnowledgeVersion]:
        return list(
            self.history.get(
                knowledge_id,
                [],
            )
        )

    def supersede(
        self,
        knowledge_id: str,
        new_value: Any,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeVersion:
        versions = self.history.get(
            knowledge_id,
            [],
        )

        if versions:
            versions[-1].status = "superseded"

        return self.add_version(
            knowledge_id=knowledge_id,
            value=new_value,
            status="active",
            metadata=metadata,
        )

    def restore(
        self,
        knowledge_id: str,
        version_number: int,
    ) -> Optional[KnowledgeVersion]:
        versions = self.history.get(
            knowledge_id,
            [],
        )

        if not (
            1 <= version_number <= len(versions)
        ):
            return None

        for version in versions:
            version.status = "superseded"

        selected = versions[
            version_number - 1
        ]
        selected.status = "active"

        return selected

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "knowledge_items": len(
                self.history
            ),
            "total_versions": sum(
                len(versions)
                for versions in self.history.values()
            ),
        }


__all__ = [
    "KnowledgeVersion",
    "KnowledgeEvolutionEngine",
]
