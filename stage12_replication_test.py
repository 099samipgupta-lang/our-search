import shutil
import tempfile
import time

from index_storage.local import LocalIndexStorage
from index_storage.replication import ReplicatedIndexStorage


OBJECT_COUNT = 100
OBJECT_SIZE = 1024
REPLICA_COUNT = 3


def main():
    root = tempfile.mkdtemp(prefix="oursearch_stage12_replication_")

    try:
        backends = [
            LocalIndexStorage(f"{root}/replica_{index}")
            for index in range(REPLICA_COUNT)
        ]

        storage = ReplicatedIndexStorage(
            backends,
            write_quorum=REPLICA_COUNT,
        )

        payload = b"x" * OBJECT_SIZE

        start = time.perf_counter()

        for index in range(OBJECT_COUNT):
            storage.put(
                f"documents/{index}",
                payload,
            )

        write_seconds = time.perf_counter() - start

        start = time.perf_counter()

        for index in range(OBJECT_COUNT):
            assert storage.get(f"documents/{index}") == payload

        read_seconds = time.perf_counter() - start

        for backend in backends:
            assert len(backend.list_keys("documents/")) == OBJECT_COUNT

        health = storage.health()

        assert health["healthy"] == REPLICA_COUNT

        print(f"OBJECTS: {OBJECT_COUNT}")
        print(f"REPLICAS: {REPLICA_COUNT}")
        print(f"OBJECT_SIZE_BYTES: {OBJECT_SIZE}")
        print(f"TOTAL_DATA_BYTES: {OBJECT_COUNT * OBJECT_SIZE}")
        print(f"WRITE_SECONDS: {write_seconds:.6f}")
        print(f"READ_SECONDS: {read_seconds:.6f}")
        print(
            f"WRITE_OPS_PER_SECOND: "
            f"{OBJECT_COUNT / write_seconds:.2f}"
        )
        print(
            f"READ_OPS_PER_SECOND: "
            f"{OBJECT_COUNT / read_seconds:.2f}"
        )
        print(f"HEALTHY_REPLICAS: {health['healthy']}")
        print()
        print("MILESTONE 12 REPLICATION TEST: PASS")

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
