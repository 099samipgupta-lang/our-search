from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class ProvenanceRecord:
    knowledge_id: str
    source_id: str
    source_type: str
    operation: str
    parent_ids: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = ""


class KnowledgeProvenanceEngine:
    VERSION = "knowledge-provenance.v1"

    def __init__(self):
        self.records: Dict[str, ProvenanceRecord] = {}

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def record(
        self,
        knowledge_id: str,
        source_id: str,
        source_type: str,
        operation: str,
        parent_ids: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ProvenanceRecord:
        provenance = ProvenanceRecord(
            knowledge_id=knowledge_id,
            source_id=source_id,
            source_type=source_type,
            operation=operation,
            parent_ids=list(parent_ids or []),
            metadata=dict(metadata or {}),
            created_at=self._timestamp(),
        )

        self.records[knowledge_id] = provenance
        return provenance

    def get(
        self,
        knowledge_id: str,
    ) -> Optional[ProvenanceRecord]:
        return self.records.get(knowledge_id)

    def lineage(
        self,
        knowledge_id: str,
    ) -> List[ProvenanceRecord]:
        result: List[ProvenanceRecord] = []
        visited = set()

        def visit(current_id: str):
            if current_id in visited:
                return

            visited.add(current_id)

            record = self.records.get(current_id)
            if record is None:
                return

            result.append(record)

            for parent_id in record.parent_ids:
                visit(parent_id)

        visit(knowledge_id)
        return result

    def source_records(
        self,
        source_id: str,
    ) -> List[ProvenanceRecord]:
        return [
            record
            for record in self.records.values()
            if record.source_id == source_id
        ]

    def derived_from(
        self,
        parent_id: str,
    ) -> List[ProvenanceRecord]:
        return [
            record
            for record in self.records.values()
            if parent_id in record.parent_ids
        ]

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "record_count": len(self.records),
        }


__all__ = [
    "ProvenanceRecord",
    "KnowledgeProvenanceEngine",
]
