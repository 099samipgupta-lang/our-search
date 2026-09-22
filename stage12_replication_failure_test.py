import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.backend import IndexStorageBackend
from index_storage.replication import ReplicatedIndexStorage


OBJECT_COUNT = 100
OBJECT_SIZE = 1024
REPLICA_COUNT = 3


class FailingBackend(IndexStorageBackend):
    def __init__(self, backend):
        self.backend = backend
        self.failed = False

    def put(self, key, data):
        if self.failed:
            raise OSError("simulated replica failure")
        return self.backend.put(key, data)

    def get(self, key):
        if self.failed:
            raise OSError("simulated replica failure")
        return self.backend.get(key)

    def exists(self, key):
        if self.failed:
            raise OSError("simulated replica failure")
        return self.backend.exists(key)

    def delete(self, key):
        if self.failed:
            raise OSError("simulated replica failure")
        return self.backend.delete(key)

    def list_keys(self, prefix=""):
        if self.failed:
            raise OSError("simulated replica failure")
        return self.backend.list_keys(prefix)


def main():
    root = tempfile.mkdtemp(prefix="oursearch_stage12_replica_failure_")

    try:
        physical = [
            LocalIndexStorage(f"{root}/replica_{index}")
            for index in range(REPLICA_COUNT)
        ]

        failing = FailingBackend(physical[2])

        storage = ReplicatedIndexStorage(
            [physical[0], physical[1], failing],
            write_quorum=2,
        )

        payload = b"x" * OBJECT_SIZE

        for index in range(OBJECT_COUNT):
            storage.put(
                f"documents/{index}",
                payload,
            )

        failing.failed = True

        for index in range(OBJECT_COUNT):
            assert storage.get(f"documents/{index}") == payload

        health = storage.health()

        assert health["healthy"] == 2

        failing.failed = False

        repaired = storage.repair_replica(
            target_index=2,
            source_index=0,
        )

        assert repaired == OBJECT_COUNT

        for index in range(OBJECT_COUNT):
            assert physical[2].get(
                f"documents/{index}"
            ) == payload

        final_health = storage.health()

        assert final_health["healthy"] == 3

        print(f"OBJECTS: {OBJECT_COUNT}")
        print(f"REPLICAS: {REPLICA_COUNT}")
        print("FAILURE_SIMULATED: replica_2")
        print("QUORUM_DURING_FAILURE: 2")
        print(f"RECOVERED_OBJECTS: {repaired}")
        print(f"FINAL_HEALTHY_REPLICAS: {final_health['healthy']}")
        print()
        print("MILESTONE 12 REPLICATION FAILURE TEST: PASS")

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
