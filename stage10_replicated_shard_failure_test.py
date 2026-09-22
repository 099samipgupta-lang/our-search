import tempfile
import os
import shutil

from index_storage.local import LocalIndexStorage
from index_storage.replication import ReplicatedIndexStorage
from index_storage.sharding import ShardedIndexStorage


def main():
    root = tempfile.mkdtemp(prefix="stage10_replicated_shard_")

    try:
        shard_a_1 = LocalIndexStorage(os.path.join(root, "a1"))
        shard_a_2 = LocalIndexStorage(os.path.join(root, "a2"))

        shard_b_1 = LocalIndexStorage(os.path.join(root, "b1"))
        shard_b_2 = LocalIndexStorage(os.path.join(root, "b2"))

        replicated_a = ReplicatedIndexStorage(
            [shard_a_1, shard_a_2],
            write_quorum=2,
        )

        replicated_b = ReplicatedIndexStorage(
            [shard_b_1, shard_b_2],
            write_quorum=2,
        )

        storage = ShardedIndexStorage(
            [replicated_a, replicated_b]
        )

        keys = [
            "documents/shard-recovery-a",
            "documents/shard-recovery-b",
            "documents/shard-recovery-c",
        ]

        for key in keys:
            storage.put(
                key,
                f"value:{key}".encode(),
            )

        for key in keys:
            assert storage.get(key) == f"value:{key}".encode()

        print("PASS: data stored through replicated shards")

        # Simulate loss of one physical replica from shard A.
        shutil.rmtree(
            os.path.join(root, "a1")
        )

        replacement_a_1 = LocalIndexStorage(
            os.path.join(root, "a1")
        )

        recovered_replicated_a = ReplicatedIndexStorage(
            [replacement_a_1, shard_a_2],
            write_quorum=1,
        )

        recovered_storage = ShardedIndexStorage(
            [recovered_replicated_a, replicated_b]
        )

        for key in keys:
            assert (
                recovered_storage.get(key)
                == f"value:{key}".encode()
            )

        print("PASS: shard data survived physical replica loss")
        print("PASS: shard recovered through surviving replica")
        print("PASS: recovered shard served all objects")

        print()
        print("=" * 48)
        print("MILESTONE 10 REPLICATED SHARD FAILURE TEST PASSED")
        print("=" * 48)

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
