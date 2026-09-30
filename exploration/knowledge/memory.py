from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class MemoryRecord:
    kind: str
    key: str
    value: Dict[str, Any]
    confidence: float = 1.0
    source: str = "internal"
    version: int = 1


class KnowledgeMemory:
    VERSION = "knowledge-memory.v1"

    def __init__(self, path: str = "exploration/knowledge/data/memory.json"):
        self.path = Path(path)
        self.records: Dict[str, MemoryRecord] = {}
        self.load()

    def _key(self, kind: str, key: str) -> str:
        return f"{kind}:{key.strip().lower()}"

    def remember(
        self,
        kind: str,
        key: str,
        value: Dict[str, Any],
        confidence: float = 1.0,
        source: str = "internal",
    ) -> MemoryRecord:
        confidence = max(0.0, min(1.0, float(confidence)))

        record = MemoryRecord(
            kind=kind,
            key=key.strip(),
            value=dict(value),
            confidence=confidence,
            source=source,
        )

        self.records[self._key(kind, key)] = record
        return record

    def remember_concept(
        self,
        name: str,
        definition: str,
        category: str = "",
        source: str = "internal",
        confidence: float = 1.0,
    ) -> MemoryRecord:
        return self.remember(
            "concept",
            name,
            {
                "name": name,
                "definition": definition,
                "category": category,
            },
            confidence,
            source,
        )

    def remember_fact(
        self,
        subject: str,
        predicate: str,
        value: str,
        category: str = "",
        source: str = "internal",
        confidence: float = 1.0,
    ) -> MemoryRecord:
        key = f"{subject}|{predicate}|{value}"

        return self.remember(
            "fact",
            key,
            {
                "subject": subject,
                "predicate": predicate,
                "value": value,
                "category": category,
            },
            confidence,
            source,
        )

    def remember_relation(
        self,
        subject: str,
        relation: str,
        target: str,
        source: str = "internal",
        confidence: float = 1.0,
    ) -> MemoryRecord:
        key = f"{subject}|{relation}|{target}"

        return self.remember(
            "relation",
            key,
            {
                "subject": subject,
                "relation": relation,
                "target": target,
            },
            confidence,
            source,
        )

    def recall(self, query: str, limit: int = 20) -> List[MemoryRecord]:
        terms = {
            token.lower()
            for token in query.split()
            if token.strip()
        }

        scored = []

        for record in self.records.values():
            text = json.dumps(
                {
                    "kind": record.kind,
                    "key": record.key,
                    "value": record.value,
                },
                ensure_ascii=False,
            ).lower()

            score = sum(1 for term in terms if term in text)

            if score:
                scored.append(
                    (
                        score,
                        record.confidence,
                        record,
                    )
                )

        scored.sort(
            key=lambda item: (item[0], item[1]),
            reverse=True,
        )

        return [
            record
            for _, _, record in scored[: max(0, int(limit))]
        ]

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "version": self.VERSION,
            "records": [
                asdict(record)
                for record in self.records.values()
            ],
        }

        temporary = self.path.with_suffix(".tmp")

        temporary.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        temporary.replace(self.path)

    def load(self) -> None:
        if not self.path.exists():
            return

        try:
            payload = json.loads(
                self.path.read_text(encoding="utf-8")
            )
        except (OSError, ValueError):
            return

        for item in payload.get("records", []):
            try:
                record = MemoryRecord(**item)
            except (TypeError, ValueError):
                continue

            self.records[self._key(record.kind, record.key)] = record

    def stats(self) -> Dict[str, int]:
        result: Dict[str, int] = {}

        for record in self.records.values():
            result[record.kind] = result.get(record.kind, 0) + 1

        result["total"] = len(self.records)
        return result
