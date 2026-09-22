import os
import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.sharding import ShardedIndexStorage
from index_storage.distributed_transactions import (
    DistributedTransactionalIndexStorage,
)


def main():
    root = tempfile.mkdtemp(prefix="oursearch_stage11_distributed_")

    try:
        shard_a = LocalIndexStorage(os.path.join(root, "shard_a"))
        shard_b = LocalIndexStorage(os.path.join(root, "shard_b"))

        sharded = ShardedIndexStorage([shard_a, shard_b])

        storage = DistributedTransactionalIndexStorage(
            sharded,
            wal_path=os.path.join(root, "distributed.wal"),
        )

        transaction = storage.begin_transaction()

        transaction.put("documents/a", b"alpha")
        transaction.put("documents/b", b"beta")

        assert transaction.get("documents/a") == b"alpha"
        assert transaction.get("documents/b") == b"beta"

        transaction.commit()

        assert storage.get("documents/a") == b"alpha"
        assert storage.get("documents/b") == b"beta"

        print("PASS: distributed transaction committed across shards")

        transaction = storage.begin_transaction()
        transaction.put("documents/c", b"gamma")
        transaction.delete("documents/a")
        transaction.rollback()

        assert storage.get("documents/c") is None
        assert storage.get("documents/a") == b"alpha"

        print("PASS: distributed rollback preserved existing state")

        print("PASS: distributed transaction test")


    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
