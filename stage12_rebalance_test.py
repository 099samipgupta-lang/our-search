import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.sharding import ShardedIndexStorage


OBJECT_COUNT = 500
OBJECT_SIZE = 1024
OLD_SHARDS = 4
NEW_SHARDS = 6


def main():
    root = tempfile.mkdtemp(prefix="oursearch_stage12_rebalance_")

    try:
        old_backends = [
            LocalIndexStorage(f"{root}/old_{index}")
            for index in range(OLD_SHARDS)
        ]

        storage = ShardedIndexStorage(old_backends)
        payload = b"x" * OBJECT_SIZE

        for index in range(OBJECT_COUNT):
            storage.put(
                f"documents/{index}",
                payload,
            )

        before_keys = storage.list_keys("documents/")
        assert len(before_keys) == OBJECT_COUNT

        new_backends = [
            LocalIndexStorage(f"{root}/new_{index}")
            for index in range(NEW_SHARDS)
        ]

        moved = storage.rebalance(new_backends)

        after_keys = storage.list_keys("documents/")

        assert moved == OBJECT_COUNT
        assert len(after_keys) == OBJECT_COUNT
        assert after_keys == before_keys

        for index in range(OBJECT_COUNT):
            assert storage.get(
                f"documents/{index}"
            ) == payload

        health = storage.health()

        assert health["shards"] == NEW_SHARDS
        assert all(
            state["healthy"]
            for state in health["states"]
        )

        print(f"OBJECTS: {OBJECT_COUNT}")
        print(f"OLD_SHARDS: {OLD_SHARDS}")
        print(f"NEW_SHARDS: {NEW_SHARDS}")
        print(f"OBJECTS_MOVED: {moved}")
        print(f"OBJECTS_AFTER_REBALANCE: {len(after_keys)}")
        print(f"VERIFIED_OBJECTS: {OBJECT_COUNT}")
        print()
        print("MILESTONE 12 REBALANCE TEST: PASS")

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
