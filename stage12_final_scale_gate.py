import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.replication import ReplicatedIndexStorage
from index_storage.sharding import ShardedIndexStorage
from index_storage.distributed_transactions import (
    DistributedTransactionalIndexStorage,
)


OBJECT_SIZE = 1024
OBJECT_COUNT = 100
SHARD_COUNT = 4
REPLICA_COUNT = 3


def make_backends(root, prefix, count):
    return [
        LocalIndexStorage(
            f"{root}/{prefix}_{index}"
        )
        for index in range(count)
    ]


def main():
    root = tempfile.mkdtemp(
        prefix="oursearch_stage12_final_gate_"
    )

    try:
        payload = b"x" * OBJECT_SIZE

        # 1. Basic capacity + restart persistence
        local_root = f"{root}/local"

        storage = LocalIndexStorage(local_root)

        for index in range(OBJECT_COUNT):
            storage.put(
                f"capacity/{index}",
                payload,
            )

        assert len(storage.list_keys("capacity/")) == OBJECT_COUNT

        del storage

        storage = LocalIndexStorage(local_root)

        for index in range(OBJECT_COUNT):
            assert storage.get(
                f"capacity/{index}"
            ) == payload

        assert len(storage.list_keys("capacity/")) == OBJECT_COUNT

        # 2. Replication
        replica_backends = make_backends(
            root,
            "replica",
            REPLICA_COUNT,
        )

        replicated = ReplicatedIndexStorage(
            replica_backends,
            write_quorum=REPLICA_COUNT,
        )

        for index in range(OBJECT_COUNT):
            replicated.put(
                f"replicated/{index}",
                payload,
            )

        assert len(
            replicated.list_keys("replicated/")
        ) == OBJECT_COUNT

        assert all(
            len(replica.list_keys("replicated/"))
            == OBJECT_COUNT
            for replica in replica_backends
        )

        # 3. Sharding
        shard_backends = make_backends(
            root,
            "shard",
            SHARD_COUNT,
        )

        sharded = ShardedIndexStorage(
            shard_backends
        )

        for index in range(OBJECT_COUNT):
            sharded.put(
                f"sharded/{index}",
                payload,
            )

        assert len(
            sharded.list_keys("sharded/")
        ) == OBJECT_COUNT

        # 4. Distributed transactions
        transaction_storage = (
            DistributedTransactionalIndexStorage(
                sharded,
                wal_path=(
                    f"{root}/"
                    "distributed_transactions.wal"
                ),
            )
        )

        transaction_count = OBJECT_COUNT // 2

        for index in range(transaction_count):
            transaction = (
                transaction_storage.begin_transaction()
            )

            transaction.put(
                f"transactions/{index}/a",
                payload,
            )

            transaction.put(
                f"transactions/{index}/b",
                payload,
            )

            transaction.commit()

        expected_transaction_objects = (
            transaction_count * 2
        )

        assert len(
            transaction_storage.list_keys(
                "transactions/"
            )
        ) == expected_transaction_objects

        for index in range(transaction_count):
            assert transaction_storage.get(
                f"transactions/{index}/a"
            ) == payload

            assert transaction_storage.get(
                f"transactions/{index}/b"
            ) == payload

        # 5. Large-object/chunk storage
        large_root = f"{root}/large"
        large_storage = LocalIndexStorage(large_root)

        large_payload = b"L" * (5 * 1024 * 1024)

        large_storage.put(
            "large/object",
            large_payload,
        )

        assert (
            large_storage.get("large/object")
            == large_payload
        )

        # 6. Final namespace verification
        assert len(
            storage.list_keys("capacity/")
        ) == OBJECT_COUNT

        print("BASIC_CAPACITY: PASS")
        print("RESTART_PERSISTENCE: PASS")
        print("REPLICATION: PASS")
        print("SHARDING: PASS")
        print("DISTRIBUTED_TRANSACTIONS: PASS")
        print("LARGE_OBJECT_STORAGE: PASS")
        print(
            f"CAPACITY_OBJECTS: {OBJECT_COUNT}"
        )
        print(
            f"REPLICAS: {REPLICA_COUNT}"
        )
        print(
            f"SHARDS: {SHARD_COUNT}"
        )
        print(
            f"TRANSACTION_OBJECTS: "
            f"{expected_transaction_objects}"
        )
        print(
            "LARGE_OBJECT_SIZE_BYTES: "
            f"{len(large_payload)}"
        )
        print()
        print(
            "MILESTONE 12 FINAL SCALE GATE: PASS"
        )

    finally:
        shutil.rmtree(
            root,
            ignore_errors=True,
        )


if __name__ == "__main__":
    main()
