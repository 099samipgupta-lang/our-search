import os
import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.sharding import ShardedIndexStorage
from index_storage.distributed_transactions import (
    DistributedTransactionalIndexStorage,
)


def main():
    root = tempfile.mkdtemp(prefix="oursearch_stage11_distributed_failure_")

    try:
        shard_a = LocalIndexStorage(os.path.join(root, "shard_a"))
        shard_b = LocalIndexStorage(os.path.join(root, "shard_b"))

        sharded = ShardedIndexStorage([shard_a, shard_b])

        wal_path = os.path.join(root, "distributed.wal")

        storage = DistributedTransactionalIndexStorage(
            sharded,
            wal_path=wal_path,
        )

        transaction = storage.begin_transaction()
        transaction.put("documents/a", b"alpha")
        transaction.put("documents/b", b"beta")

        # Simulate the distributed coordinator reaching its durable
        # commit point before participant application.
        storage.wal.append(
            {
                "type": "commit",
                "transaction_id": transaction.transaction_id,
                "operations": [
                    {
                        "operation": operation,
                        "key": key,
                        "data": data.hex(),
                    }
                    for operation, key, data in transaction._operations
                ],
            }
        )

        # Simulated coordinator crash:
        del storage

        recovered = DistributedTransactionalIndexStorage(
            sharded,
            wal_path=wal_path,
        )

        assert recovered.get("documents/a") == b"alpha"
        assert recovered.get("documents/b") == b"beta"

        print("PASS: distributed committed transaction recovered after crash")

        assert recovered.wal.recover() == []

        print("PASS: distributed WAL cleared after recovery")
        print("PASS: distributed transaction failure recovery test")


    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
