"""
Stage 5.4.1 — Distributed Worker Architecture & Ownership.

Defines deterministic ownership of crawl partitions by worker nodes.

Ownership is calculated using rendezvous hashing (highest-random-weight
selection) so adding/removing nodes causes substantially less partition
movement than simple modulo assignment.

This module does NOT perform failure takeover. Failure recovery belongs to
Stage 5.6.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class WorkerNode:
    """Logical distributed worker node."""

    node_id: str
    worker_count: int

    def __post_init__(self):
        node_id = str(self.node_id).strip()

        if not node_id:
            raise ValueError("node_id must not be empty")

        worker_count = int(self.worker_count)

        if worker_count <= 0:
            raise ValueError(
                "worker_count must be greater than zero"
            )

        object.__setattr__(self, "node_id", node_id)
        object.__setattr__(self, "worker_count", worker_count)


class WorkerOwnership:
    """
    Deterministically assigns crawl partitions to worker nodes.

    The assignment depends only on:
        cluster_id
        partition_id
        configured node IDs

    It does not depend on process IDs, Python hash(), dictionary ordering,
    process startup order, or machine-local state.
    """

    SCHEMA_VERSION = 1

    def __init__(
        self,
        partition_count: int,
        nodes: Iterable[WorkerNode],
        cluster_id: str = "our-search",
    ):
        partition_count = int(partition_count)

        if partition_count <= 0:
            raise ValueError(
                "partition_count must be greater than zero"
            )

        cluster_id = str(cluster_id).strip()

        if not cluster_id:
            raise ValueError(
                "cluster_id must not be empty"
            )

        normalized_nodes = tuple(nodes)

        if not normalized_nodes:
            raise ValueError(
                "at least one worker node is required"
            )

        node_ids = [node.node_id for node in normalized_nodes]

        if len(set(node_ids)) != len(node_ids):
            raise ValueError(
                "worker node IDs must be unique"
            )

        self.partition_count = partition_count
        self.nodes = tuple(
            sorted(
                normalized_nodes,
                key=lambda node: node.node_id,
            )
        )
        self.cluster_id = cluster_id

    @staticmethod
    def _score(
        cluster_id: str,
        partition_id: int,
        node_id: str,
    ) -> int:
        payload = (
            f"{cluster_id}|partition:{partition_id}|node:{node_id}"
        ).encode("utf-8")

        digest = hashlib.sha256(payload).digest()

        return int.from_bytes(
            digest,
            byteorder="big",
            signed=False,
        )

    def validate_partition_id(
        self,
        partition_id: int,
    ) -> int:
        partition_id = int(partition_id)

        if not 0 <= partition_id < self.partition_count:
            raise ValueError(
                f"invalid partition id {partition_id}; "
                f"expected 0..{self.partition_count - 1}"
            )

        return partition_id

    def owner_for_partition(
        self,
        partition_id: int,
    ) -> str:
        partition_id = self.validate_partition_id(
            partition_id
        )

        return max(
            self.nodes,
            key=lambda node: (
                self._score(
                    self.cluster_id,
                    partition_id,
                    node.node_id,
                ),
                node.node_id,
            ),
        ).node_id

    def owner_node_for_partition(
        self,
        partition_id: int,
    ) -> WorkerNode:
        owner_id = self.owner_for_partition(
            partition_id
        )

        return next(
            node
            for node in self.nodes
            if node.node_id == owner_id
        )

    def owns_partition(
        self,
        node_id: str,
        partition_id: int,
    ) -> bool:
        node_id = str(node_id).strip()

        if node_id not in self.node_ids():
            raise ValueError(
                f"unknown worker node: {node_id!r}"
            )

        return (
            self.owner_for_partition(partition_id)
            == node_id
        )

    def partitions_for_node(
        self,
        node_id: str,
    ) -> list[int]:
        node_id = str(node_id).strip()

        if node_id not in self.node_ids():
            raise ValueError(
                f"unknown worker node: {node_id!r}"
            )

        return [
            partition_id
            for partition_id in range(
                self.partition_count
            )
            if self.owner_for_partition(
                partition_id
            ) == node_id
        ]

    def ownership_map(self) -> dict[int, str]:
        return {
            partition_id: self.owner_for_partition(
                partition_id
            )
            for partition_id in range(
                self.partition_count
            )
        }

    def node_ids(self) -> list[str]:
        return [
            node.node_id
            for node in self.nodes
        ]

    def validate_complete_ownership(self) -> bool:
        ownership = self.ownership_map()

        if len(ownership) != self.partition_count:
            return False

        if any(
            owner not in self.node_ids()
            for owner in ownership.values()
        ):
            return False

        for partition_id in range(
            self.partition_count
        ):
            owners = [
                node.node_id
                for node in self.nodes
                if self.owns_partition(
                    node.node_id,
                    partition_id,
                )
            ]

            if len(owners) != 1:
                return False

        return True

    def configuration(self) -> dict:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "cluster_id": self.cluster_id,
            "partition_count": self.partition_count,
            "nodes": [
                {
                    "node_id": node.node_id,
                    "worker_count": node.worker_count,
                }
                for node in self.nodes
            ],
        }

    def save_configuration(
        self,
        path: str,
    ) -> None:
        target = Path(path)

        if target.parent:
            target.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

        target.write_text(
            json.dumps(
                self.configuration(),
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    @classmethod
    def load_configuration(
        cls,
        path: str,
    ) -> "WorkerOwnership":
        target = Path(path)

        data = json.loads(
            target.read_text(
                encoding="utf-8"
            )
        )

        if int(
            data.get("schema_version", 0)
        ) != cls.SCHEMA_VERSION:
            raise ValueError(
                "unsupported WorkerOwnership schema version"
            )

        nodes = [
            WorkerNode(
                node_id=item["node_id"],
                worker_count=item["worker_count"],
            )
            for item in data["nodes"]
        ]

        return cls(
            partition_count=data["partition_count"],
            nodes=nodes,
            cluster_id=data["cluster_id"],
        )
