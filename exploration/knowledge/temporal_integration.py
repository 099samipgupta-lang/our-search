from __future__ import annotations

from typing import Any, Dict, List, Optional

from exploration.knowledge.temporal import (
    KnowledgeTemporalEngine,
)


class KnowledgeTemporalIntegration:
    VERSION = "knowledge-temporal-integration.v1"

    def __init__(
        self,
        temporal: Optional[
            KnowledgeTemporalEngine
        ] = None,
    ):
        self.temporal = (
            temporal
            if temporal is not None
            else KnowledgeTemporalEngine()
        )

    def register(
        self,
        knowledge_id: str,
        valid_from: Optional[str] = None,
        valid_until: Optional[str] = None,
        observed_at: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        record = self.temporal.record(
            knowledge_id=knowledge_id,
            valid_from=valid_from,
            valid_until=valid_until,
            observed_at=observed_at,
            metadata=metadata,
        )

        return {
            "knowledge_id": record.knowledge_id,
            "valid_from": record.valid_from,
            "valid_until": record.valid_until,
            "observed_at": record.observed_at,
            "active": self.temporal.is_active(
                record.knowledge_id
            ),
        }

    def is_current(
        self,
        knowledge_id: str,
        timestamp: Optional[str] = None,
    ) -> bool:
        return self.temporal.is_active(
            knowledge_id,
            timestamp=timestamp,
        )

    def filter_current(
        self,
        knowledge_ids: List[str],
        timestamp: Optional[str] = None,
    ) -> List[str]:
        return [
            knowledge_id
            for knowledge_id in knowledge_ids
            if self.temporal.is_active(
                knowledge_id,
                timestamp=timestamp,
            )
        ]

    def supersedes(
        self,
        newer_id: str,
        older_id: str,
    ) -> bool:
        return self.temporal.supersedes(
            newer_id,
            older_id,
        )

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "temporal": self.temporal.stats(),
        }


__all__ = [
    "KnowledgeTemporalIntegration",
]
