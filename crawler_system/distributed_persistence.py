from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


class DistributedPersistenceError(Exception):
    """Raised when distributed persistence invariants are violated."""


class DistributedPersistenceManager:
    """
    Persistence integrity layer for partitioned crawler state.

    Partition SQLite databases remain the authoritative URL state.
    This manager provides deterministic manifests and restart validation.
    """

    SCHEMA_VERSION = 1

    def __init__(self, partition_registry):
        self.registry = partition_registry

    @staticmethod
    def _file_digest(path: str) -> str:
        digest = hashlib.sha256()

        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)

        return digest.hexdigest()

    def manifest(self) -> dict[str, Any]:
        router = self.registry.router

        partitions = []

        for partition_id in range(router.partition_count):
            path = router.partition_path(partition_id)

            entry = {
                "partition_id": partition_id,
                "database_path": os.path.abspath(path),
                "exists": os.path.exists(path),
            }

            if entry["exists"]:
                entry["sha256"] = self._file_digest(path)
                entry["size"] = os.path.getsize(path)
            else:
                entry["sha256"] = None
                entry["size"] = 0

            partitions.append(entry)

        return {
            "schema_version": self.SCHEMA_VERSION,
            "partition_count": router.partition_count,
            "partitions": partitions,
        }

    def save_manifest(self, path: str | Path) -> dict[str, Any]:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = self.manifest()
        temporary = path.with_name(path.name + ".tmp")

        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        temporary.replace(path)
        return payload

    def load_manifest(self, path: str | Path) -> dict[str, Any]:
        path = Path(path)

        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        if int(payload.get("schema_version", -1)) != self.SCHEMA_VERSION:
            raise DistributedPersistenceError(
                "unsupported persistence manifest schema"
            )

        if int(payload.get("partition_count", -1)) != (
            self.registry.router.partition_count
        ):
            raise DistributedPersistenceError(
                "persistence manifest partition count mismatch"
            )

        return payload

    def validate_manifest(self, manifest: dict[str, Any]) -> bool:
        if int(manifest.get("schema_version", -1)) != self.SCHEMA_VERSION:
            raise DistributedPersistenceError(
                "invalid persistence manifest schema"
            )

        expected_count = self.registry.router.partition_count
        partitions = manifest.get("partitions", [])

        if len(partitions) != expected_count:
            raise DistributedPersistenceError(
                "persistence manifest does not cover every partition"
            )

        seen = set()

        for entry in partitions:
            partition_id = int(entry["partition_id"])

            if partition_id in seen:
                raise DistributedPersistenceError(
                    f"duplicate partition in manifest: {partition_id}"
                )

            seen.add(partition_id)

            if not 0 <= partition_id < expected_count:
                raise DistributedPersistenceError(
                    f"invalid partition in manifest: {partition_id}"
                )

            path = entry["database_path"]
            exists = os.path.exists(path)

            if bool(entry["exists"]) != exists:
                raise DistributedPersistenceError(
                    f"partition existence changed: {partition_id}"
                )

            if exists:
                if self._file_digest(path) != entry["sha256"]:
                    raise DistributedPersistenceError(
                        f"partition checksum changed: {partition_id}"
                    )

        if seen != set(range(expected_count)):
            raise DistributedPersistenceError(
                "manifest has incomplete partition coverage"
            )

        return True

    def validate_live_partitions(self) -> bool:
        router = self.registry.router

        for partition_id in range(router.partition_count):
            store = self.registry.store_for_partition(partition_id)

            if store is None:
                raise DistributedPersistenceError(
                    f"partition store unavailable: {partition_id}"
                )

            store.count()

        return True

    def validate(self) -> bool:
        self.validate_live_partitions()
        return True
