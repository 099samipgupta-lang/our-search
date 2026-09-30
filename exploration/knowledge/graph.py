from dataclasses import dataclass, field
from typing import Iterable


@dataclass(frozen=True)
class KnowledgeRelation:
    subject: str
    predicate: str
    object: str
    confidence: float = 1.0
    source: str = "internal"


@dataclass
class KnowledgeNode:
    name: str
    node_type: str = "entity"
    attributes: dict = field(default_factory=dict)


class KnowledgeGraph:
    """
    Connected knowledge representation for the OUR SEARCH Brain.

    Facts become relationships between entities instead of remaining
    isolated pieces of text.
    """

    VERSION = "knowledge-graph.v1"

    def __init__(self):
        self.nodes: dict[str, KnowledgeNode] = {}
        self.relations: list[KnowledgeRelation] = []

        self.outgoing: dict[
            str,
            list[KnowledgeRelation],
        ] = {}

        self.incoming: dict[
            str,
            list[KnowledgeRelation],
        ] = {}

    @staticmethod
    def _normalize(value):
        return " ".join(
            str(value or "").casefold().split()
        )

    @staticmethod
    def _display(value):
        return " ".join(
            str(value or "").split()
        ).strip()

    def add_node(
        self,
        name,
        node_type="entity",
        attributes=None,
    ):
        name = self._display(name)

        if not name:
            return None

        key = self._normalize(name)

        if key not in self.nodes:
            self.nodes[key] = KnowledgeNode(
                name=name,
                node_type=node_type,
                attributes=dict(
                    attributes or {}
                ),
            )
        else:
            if attributes:
                self.nodes[key].attributes.update(
                    attributes
                )

        return self.nodes[key]

    def add_relation(
        self,
        subject,
        predicate,
        object,
        confidence=1.0,
        source="internal",
    ):
        subject = self._display(subject)
        predicate = self._display(predicate)
        object = self._display(object)

        if not subject or not predicate or not object:
            return None

        self.add_node(subject)
        self.add_node(object)

        relation = KnowledgeRelation(
            subject=subject,
            predicate=predicate,
            object=object,
            confidence=float(confidence),
            source=str(source),
        )

        relation_key = (
            self._normalize(subject),
            self._normalize(predicate),
            self._normalize(object),
        )

        for existing in self.relations:
            existing_key = (
                self._normalize(existing.subject),
                self._normalize(existing.predicate),
                self._normalize(existing.object),
            )

            if existing_key == relation_key:
                return existing

        self.relations.append(relation)

        subject_key = self._normalize(subject)
        object_key = self._normalize(object)

        self.outgoing.setdefault(
            subject_key,
            [],
        ).append(relation)

        self.incoming.setdefault(
            object_key,
            [],
        ).append(relation)

        return relation

    def connect_facts(
        self,
        facts: Iterable,
        source="internal",
    ):
        added = 0

        for fact in facts:
            if isinstance(fact, dict):
                subject = fact.get(
                    "subject",
                    "",
                )
                predicate = fact.get(
                    "predicate",
                    "",
                )
                object_value = fact.get(
                    "value",
                    "",
                )
                confidence = fact.get(
                    "confidence",
                    1.0,
                )
            else:
                subject = getattr(
                    fact,
                    "subject",
                    "",
                )
                predicate = getattr(
                    fact,
                    "predicate",
                    "",
                )
                object_value = getattr(
                    fact,
                    "value",
                    "",
                )
                confidence = getattr(
                    fact,
                    "confidence",
                    1.0,
                )

            before = len(self.relations)

            self.add_relation(
                subject=subject,
                predicate=predicate,
                object=object_value,
                confidence=confidence,
                source=source,
            )

            if len(self.relations) > before:
                added += 1

        return added

    def get_entity(self, name):
        return self.nodes.get(
            self._normalize(name)
        )

    def related_to(self, name, direction="both"):
        key = self._normalize(name)

        results = []

        if direction in {"out", "both"}:
            results.extend(
                self.outgoing.get(
                    key,
                    [],
                )
            )

        if direction in {"in", "both"}:
            results.extend(
                self.incoming.get(
                    key,
                    [],
                )
            )

        return results

    def neighbors(self, name):
        relations = self.related_to(
            name,
            direction="both",
        )

        neighbors = []

        key = self._normalize(name)

        for relation in relations:
            if self._normalize(
                relation.subject
            ) == key:
                neighbors.append(
                    relation.object
                )
            else:
                neighbors.append(
                    relation.subject
                )

        return list(
            dict.fromkeys(neighbors)
        )

    def find_path(
        self,
        start,
        target,
        max_depth=4,
    ):
        start_key = self._normalize(start)
        target_key = self._normalize(target)

        if start_key == target_key:
            return [start]

        queue = [
            (
                start_key,
                [start],
            )
        ]

        visited = {
            start_key,
        }

        while queue:
            current, path = queue.pop(0)

            if len(path) > max_depth:
                continue

            for relation in self.outgoing.get(
                current,
                [],
            ):
                next_key = self._normalize(
                    relation.object
                )

                if next_key in visited:
                    continue

                next_path = path + [
                    relation.object
                ]

                if next_key == target_key:
                    return next_path

                visited.add(next_key)

                queue.append(
                    (
                        next_key,
                        next_path,
                    )
                )

        return []

    def snapshot(self):
        return {
            "version": self.VERSION,
            "node_count": len(self.nodes),
            "relation_count": len(
                self.relations
            ),
            "nodes": [
                {
                    "name": node.name,
                    "type": node.node_type,
                    "attributes": dict(
                        node.attributes
                    ),
                }
                for node in self.nodes.values()
            ],
            "relations": [
                {
                    "subject": relation.subject,
                    "predicate": relation.predicate,
                    "object": relation.object,
                    "confidence": relation.confidence,
                    "source": relation.source,
                }
                for relation in self.relations
            ],
        }


__all__ = [
    "KnowledgeRelation",
    "KnowledgeNode",
    "KnowledgeGraph",
]
