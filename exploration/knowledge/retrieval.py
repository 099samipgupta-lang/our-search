from __future__ import annotations

from dataclasses import dataclass
from typing import List

from exploration.knowledge.memory import KnowledgeMemory, MemoryRecord


@dataclass
class KnowledgeMatch:
    record: MemoryRecord
    score: float
    matched_terms: List[str]


class KnowledgeRetrievalEngine:
    VERSION = "knowledge-retrieval.v1"

    def __init__(self, memory: KnowledgeMemory | None = None):
        self.memory = memory if memory is not None else KnowledgeMemory()

    def _terms(self, text: str) -> List[str]:
        return [
            term.lower()
            for term in text.split()
            if term.strip()
        ]

    def retrieve(
        self,
        query: str,
        limit: int = 10,
    ) -> List[KnowledgeMatch]:
        terms = self._terms(query)

        if not terms:
            return []

        matches: List[KnowledgeMatch] = []

        for record in self.memory.records.values():
            searchable = " ".join(
                [
                    record.kind,
                    record.key,
                    *[
                        str(value)
                        for value in record.value.values()
                    ],
                ]
            ).lower()

            matched = [
                term
                for term in terms
                if term in searchable
            ]

            if not matched:
                continue

            coverage = len(set(matched)) / len(set(terms))
            confidence = max(
                0.0,
                min(1.0, record.confidence),
            )

            score = (
                coverage * 0.7
                + confidence * 0.3
            )

            matches.append(
                KnowledgeMatch(
                    record=record,
                    score=score,
                    matched_terms=sorted(set(matched)),
                )
            )

        matches.sort(
            key=lambda item: (
                item.score,
                item.record.confidence,
            ),
            reverse=True,
        )

        return matches[: max(0, int(limit))]

    def recall_text(
        self,
        query: str,
        limit: int = 10,
    ) -> List[str]:
        matches = self.retrieve(
            query=query,
            limit=limit,
        )

        output = []

        for match in matches:
            record = match.record

            if record.kind == "concept":
                name = record.value.get(
                    "name",
                    record.key,
                )
                definition = record.value.get(
                    "definition",
                    "",
                )
                output.append(
                    f"{name}: {definition}"
                )

            elif record.kind == "fact":
                subject = record.value.get(
                    "subject",
                    "",
                )
                predicate = record.value.get(
                    "predicate",
                    "",
                )
                value = record.value.get(
                    "value",
                    "",
                )
                output.append(
                    f"{subject} {predicate} {value}"
                )

            elif record.kind == "relation":
                subject = record.value.get(
                    "subject",
                    "",
                )
                relation = record.value.get(
                    "relation",
                    "",
                )
                target = record.value.get(
                    "target",
                    "",
                )
                output.append(
                    f"{subject} {relation} {target}"
                )

        return output
