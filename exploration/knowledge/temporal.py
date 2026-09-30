from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class TemporalKnowledge:
    knowledge_id: str
    valid_from: Optional[str]
    valid_until: Optional[str]
    observed_at: str
    metadata: Dict[str, Any]


class KnowledgeTemporalEngine:
    VERSION = "knowledge-temporal.v1"

    def __init__(self):
        self.records: Dict[str, TemporalKnowledge] = {}

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def record(
        self,
        knowledge_id: str,
        valid_from: Optional[str] = None,
        valid_until: Optional[str] = None,
        observed_at: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TemporalKnowledge:
        record = TemporalKnowledge(
            knowledge_id=knowledge_id,
            valid_from=valid_from,
            valid_until=valid_until,
            observed_at=observed_at or self._now(),
            metadata=dict(metadata or {}),
        )

        self.records[knowledge_id] = record
        return record

    def get(
        self,
        knowledge_id: str,
    ) -> Optional[TemporalKnowledge]:
        return self.records.get(knowledge_id)

    def is_active(
        self,
        knowledge_id: str,
        timestamp: Optional[str] = None,
    ) -> bool:
        record = self.records.get(knowledge_id)

        if record is None:
            return False

        current = timestamp or self._now()

        if (
            record.valid_from is not None
            and current < record.valid_from
        ):
            return False

        if (
            record.valid_until is not None
            and current > record.valid_until
        ):
            return False

        return True

    def supersedes(
        self,
        newer_id: str,
        older_id: str,
    ) -> bool:
        newer = self.records.get(newer_id)
        older = self.records.get(older_id)

        if newer is None or older is None:
            return False

        if (
            newer.valid_from is None
            or older.valid_from is None
        ):
            return False

        return newer.valid_from > older.valid_from

    def active_records(self) -> List[TemporalKnowledge]:
        return [
            record
            for record in self.records.values()
            if self.is_active(record.knowledge_id)
        ]

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "record_count": len(self.records),
            "active_count": len(
                self.active_records()
            ),
        }


__all__ = [
    "TemporalKnowledge",
    "KnowledgeTemporalEngine",
]
