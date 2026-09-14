from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Optional


@dataclass(frozen=True)
class WorkerNodeRegistration:
    node_id: str
    worker_count: int
    registered_at: float
    last_heartbeat: float
    active: bool = True

    def __post_init__(self):
        if not self.node_id or not self.node_id.strip():
            raise ValueError("node_id must be non-empty")
        if int(self.worker_count) <= 0:
            raise ValueError("worker_count must be greater than zero")
        if float(self.registered_at) < 0:
            raise ValueError("registered_at must be non-negative")
        if float(self.last_heartbeat) < 0:
            raise ValueError("last_heartbeat must be non-negative")

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "WorkerNodeRegistration":
        return cls(
            node_id=str(data["node_id"]),
            worker_count=int(data["worker_count"]),
            registered_at=float(data["registered_at"]),
            last_heartbeat=float(data["last_heartbeat"]),
            active=bool(data.get("active", True)),
        )


class WorkerRegistry:
    SCHEMA_VERSION = 1

    def __init__(self, cluster_id: str = "our-search"):
        if not cluster_id or not cluster_id.strip():
            raise ValueError("cluster_id must be non-empty")

        self.cluster_id = cluster_id
        self._nodes: Dict[str, WorkerNodeRegistration] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _validate_node_id(node_id: str) -> str:
        node_id = str(node_id).strip()
        if not node_id:
            raise ValueError("node_id must be non-empty")
        return node_id

    @staticmethod
    def _validate_worker_count(worker_count: int) -> int:
        worker_count = int(worker_count)
        if worker_count <= 0:
            raise ValueError("worker_count must be greater than zero")
        return worker_count

    def register(
        self,
        node_id: str,
        worker_count: int,
        now: Optional[float] = None,
    ) -> WorkerNodeRegistration:
        node_id = self._validate_node_id(node_id)
        worker_count = self._validate_worker_count(worker_count)
        timestamp = time.time() if now is None else float(now)

        if timestamp < 0:
            raise ValueError("now must be non-negative")

        with self._lock:
            if node_id in self._nodes and self._nodes[node_id].active:
                raise ValueError(f"node already registered: {node_id}")

            registration = WorkerNodeRegistration(
                node_id=node_id,
                worker_count=worker_count,
                registered_at=timestamp,
                last_heartbeat=timestamp,
                active=True,
            )
            self._nodes[node_id] = registration
            return registration

    def unregister(self, node_id: str) -> WorkerNodeRegistration:
        node_id = self._validate_node_id(node_id)

        with self._lock:
            registration = self._nodes.get(node_id)
            if registration is None:
                raise KeyError(f"unknown node: {node_id}")

            updated = WorkerNodeRegistration(
                node_id=registration.node_id,
                worker_count=registration.worker_count,
                registered_at=registration.registered_at,
                last_heartbeat=registration.last_heartbeat,
                active=False,
            )
            self._nodes[node_id] = updated
            return updated

    def heartbeat(
        self,
        node_id: str,
        now: Optional[float] = None,
    ) -> WorkerNodeRegistration:
        node_id = self._validate_node_id(node_id)
        timestamp = time.time() if now is None else float(now)

        if timestamp < 0:
            raise ValueError("now must be non-negative")

        with self._lock:
            registration = self._nodes.get(node_id)
            if registration is None:
                raise KeyError(f"unknown node: {node_id}")
            if not registration.active:
                raise RuntimeError(f"node is inactive: {node_id}")

            updated = WorkerNodeRegistration(
                node_id=registration.node_id,
                worker_count=registration.worker_count,
                registered_at=registration.registered_at,
                last_heartbeat=timestamp,
                active=True,
            )
            self._nodes[node_id] = updated
            return updated

    def update_worker_count(
        self,
        node_id: str,
        worker_count: int,
    ) -> WorkerNodeRegistration:
        node_id = self._validate_node_id(node_id)
        worker_count = self._validate_worker_count(worker_count)

        with self._lock:
            registration = self._nodes.get(node_id)
            if registration is None:
                raise KeyError(f"unknown node: {node_id}")

            updated = WorkerNodeRegistration(
                node_id=registration.node_id,
                worker_count=worker_count,
                registered_at=registration.registered_at,
                last_heartbeat=registration.last_heartbeat,
                active=registration.active,
            )
            self._nodes[node_id] = updated
            return updated

    def get(self, node_id: str) -> Optional[WorkerNodeRegistration]:
        node_id = self._validate_node_id(node_id)
        with self._lock:
            return self._nodes.get(node_id)

    def active_nodes(self) -> tuple[WorkerNodeRegistration, ...]:
        with self._lock:
            return tuple(
                self._nodes[node_id]
                for node_id in sorted(self._nodes)
                if self._nodes[node_id].active
            )

    def all_nodes(self) -> tuple[WorkerNodeRegistration, ...]:
        with self._lock:
            return tuple(
                self._nodes[node_id]
                for node_id in sorted(self._nodes)
            )

    def node_ids(self, active_only: bool = True) -> tuple[str, ...]:
        nodes = self.active_nodes() if active_only else self.all_nodes()
        return tuple(node.node_id for node in nodes)

    def total_worker_capacity(self, active_only: bool = True) -> int:
        nodes = self.active_nodes() if active_only else self.all_nodes()
        return sum(node.worker_count for node in nodes)

    def count(self, active_only: bool = True) -> int:
        return len(self.active_nodes() if active_only else self.all_nodes())

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "schema_version": self.SCHEMA_VERSION,
                "cluster_id": self.cluster_id,
                "nodes": [
                    node.to_dict()
                    for node in self.all_nodes()
                ],
            }

    def validate(self) -> bool:
        with self._lock:
            if not self.cluster_id.strip():
                raise ValueError("invalid cluster_id")

            seen = set()
            for node in self.all_nodes():
                if node.node_id in seen:
                    raise ValueError(f"duplicate node_id: {node.node_id}")
                seen.add(node.node_id)

                self._validate_node_id(node.node_id)
                self._validate_worker_count(node.worker_count)

            return True

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = self.snapshot()
        temporary = path.with_name(path.name + ".tmp")

        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")

        temporary.replace(path)

    @classmethod
    def load(cls, path: str | Path) -> "WorkerRegistry":
        path = Path(path)

        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        if int(payload.get("schema_version", -1)) != cls.SCHEMA_VERSION:
            raise ValueError("unsupported WorkerRegistry schema version")

        registry = cls(cluster_id=str(payload["cluster_id"]))

        for data in payload.get("nodes", []):
            node = WorkerNodeRegistration.from_dict(data)

            if node.node_id in registry._nodes:
                raise ValueError(f"duplicate node_id: {node.node_id}")

            registry._nodes[node.node_id] = node

        registry.validate()
        return registry
