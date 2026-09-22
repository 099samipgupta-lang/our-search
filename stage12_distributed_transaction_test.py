import shutil
import tempfile
import time

from index_storage.local import LocalIndexStorage
from index_storage.sharding import ShardedIndexStorage
from index_storage.distributed_transactions import (
    DistributedTransactionalIndexStorage,
)


TRANSACTION_COUNT = 100
SHARD_COUNT = 4
OBJECT_SIZE = 1024


def main():
    root = tempfile.mkdtemp(
        prefix="oursearch_stage12_transactions_"
    )

    try:
        backends = [
            LocalIndexStorage(f"{root}/shard_{index}")
            for index in range(SHARD_COUNT)
        ]

        sharded = ShardedIndexStorage(backends)

        storage = DistributedTransactionalIndexStorage(
            sharded,
            wal_path=f"{root}/distributed_transactions.wal",
        )

        payload = b"x" * OBJECT_SIZE

        start = time.perf_counter()

        for index in range(TRANSACTION_COUNT):
            transaction = storage.begin_transaction()

            transaction.put(
                f"documents/{index}/a",
                payload,
            )

            transaction.put(
                f"documents/{index}/b",
                payload,
            )

            transaction.commit()

        write_seconds = time.perf_counter() - start

        expected_objects = TRANSACTION_COUNT * 2

        keys = storage.list_keys("documents/")

        assert len(keys) == expected_objects

        for index in range(TRANSACTION_COUNT):
            assert storage.get(
                f"documents/{index}/a"
            ) == payload

            assert storage.get(
                f"documents/{index}/b"
            ) == payload

        print(f"TRANSACTIONS: {TRANSACTION_COUNT}")
        print(f"SHARDS: {SHARD_COUNT}")
        print(f"OBJECTS_COMMITTED: {expected_objects}")
        print(f"OBJECT_SIZE_BYTES: {OBJECT_SIZE}")
        print(f"WRITE_SECONDS: {write_seconds:.6f}")
        print(
            f"TRANSACTIONS_PER_SECOND: "
            f"{TRANSACTION_COUNT / write_seconds:.2f}"
        )
        print(f"VERIFIED_OBJECTS: {len(keys)}")
        print()
        print("MILESTONE 12 DISTRIBUTED TRANSACTION TEST: PASS")

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
