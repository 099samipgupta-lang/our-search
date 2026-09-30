from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from exploration.knowledge.learning_coordinator import (
    KnowledgeLearningCoordinator,
)


@dataclass
class KnowledgeSource:
    source_id: str
    source_type: str
    name: str
    metadata: Dict[str, Any]


class KnowledgeSourceManager:
    VERSION = "knowledge-source-manager.v1"

    def __init__(
        self,
        learning: Optional[
            KnowledgeLearningCoordinator
        ] = None,
    ):
        self.learning = (
            learning
            if learning is not None
            else KnowledgeLearningCoordinator()
        )
        self.sources: Dict[
            str, KnowledgeSource
        ] = {}

    def register_source(
        self,
        source_id: str,
        source_type: str,
        name: str,
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> KnowledgeSource:
        source = KnowledgeSource(
            source_id=source_id,
            source_type=source_type,
            name=name,
            metadata=metadata or {},
        )

        self.sources[source_id] = source
        return source

    def get_source(
        self,
        source_id: str,
    ) -> Optional[KnowledgeSource]:
        return self.sources.get(source_id)

    def list_sources(self) -> List[KnowledgeSource]:
        return list(self.sources.values())

    def ingest_text(
        self,
        text: str,
        source_id: str = "unknown",
        source_type: str = "text",
        metadata: Optional[
            Dict[str, Any]
        ] = None,
    ) -> Dict[str, Any]:
        if source_id not in self.sources:
            self.register_source(
                source_id=source_id,
                source_type=source_type,
                name=source_id,
                metadata=metadata,
            )

        result = self.learning.learn_text(
            text=text,
            source=source_id,
        )

        return {
            "version": self.VERSION,
            "source_id": source_id,
            "source_type": source_type,
            "result": result,
        }

    def ingest_structured(
        self,
        facts: Optional[
            Iterable[Dict[str, Any]]
        ] = None,
        concepts: Optional[
            Iterable[Dict[str, Any]]
        ] = None,
        relations: Optional[
            Iterable[Dict[str, Any]]
        ] = None,
        source_id: str = "unknown",
        source_type: str = "structured",
    ) -> Dict[str, Any]:
        if source_id not in self.sources:
            self.register_source(
                source_id=source_id,
                source_type=source_type,
                name=source_id,
            )

        result = self.learning.learn_structured(
            facts=list(facts or []),
            concepts=list(concepts or []),
            relations=list(relations or []),
            source=source_id,
        )

        return {
            "version": self.VERSION,
            "source_id": source_id,
            "source_type": source_type,
            "result": result,
        }

    def ingest_web_text(
        self,
        text: str,
        url: str,
        title: str = "",
    ) -> Dict[str, Any]:
        source_id = f"web:{url}"

        return self.ingest_text(
            text=text,
            source_id=source_id,
            source_type="web",
            metadata={
                "url": url,
                "title": title,
            },
        )

    def stats(self) -> Dict[str, Any]:
        return {
            "version": self.VERSION,
            "source_count": len(self.sources),
            "sources": [
                {
                    "source_id": source.source_id,
                    "source_type": source.source_type,
                    "name": source.name,
                }
                for source in self.sources.values()
            ],
        }
