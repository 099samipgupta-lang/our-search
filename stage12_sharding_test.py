import shutil
import tempfile
import time
from collections import Counter

from index_storage.local import LocalIndexStorage
from index_storage.sharding import ShardedIndexStorage


OBJECT_COUNT = 100
OBJECT_SIZE = 1024
SHARD_COUNT = 4


def main():
    root = tempfile.mkdtemp(prefix="oursearch_stage12_sharding_")

    try:
        backends = [
            LocalIndexStorage(f"{root}/shard_{index}")
            for index in range(SHARD_COUNT)
        ]

        storage = ShardedIndexStorage(backends)
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

        distribution = Counter(
            storage.shard_for_key(f"documents/{index}")
            for index in range(OBJECT_COUNT)
        )

        keys = storage.list_keys("documents/")

        assert len(keys) == OBJECT_COUNT
        assert sum(distribution.values()) == OBJECT_COUNT

        health = storage.health()

        assert all(
            state["healthy"]
            for state in health["states"]
        )

        print(f"OBJECTS: {OBJECT_COUNT}")
        print(f"SHARDS: {SHARD_COUNT}")
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
        print(f"KEYS_VISIBLE: {len(keys)}")
        print(f"SHARD_DISTRIBUTION: {dict(sorted(distribution.items()))}")
        print()
        print("MILESTONE 12 SHARDING TEST: PASS")

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
