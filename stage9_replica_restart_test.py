import os
import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.replication import ReplicatedIndexStorage


def main():
    root = tempfile.mkdtemp(prefix="stage9_restart_")

    try:
        replica_a_path = os.path.join(root, "replica_a")
        replica_b_path = os.path.join(root, "replica_b")

        replica_a = LocalIndexStorage(replica_a_path)
        replica_b = LocalIndexStorage(replica_b_path)

        storage = ReplicatedIndexStorage(
            [replica_a, replica_b],
            write_quorum=2,
        )

        key = "documents/restart-test"
        value = b"OUR SEARCH durable replication"

        storage.put(key, value)

        assert replica_a.get(key) == value
        assert replica_b.get(key) == value

        print("PASS: data written to both replicas")

        # Simulate complete process shutdown.
        del storage
        del replica_a
        del replica_b

        # Reopen both persistent replicas.
        replica_a = LocalIndexStorage(replica_a_path)
        replica_b = LocalIndexStorage(replica_b_path)

        storage = ReplicatedIndexStorage(
            [replica_a, replica_b],
            write_quorum=2,
        )

        assert replica_a.get(key) == value
        assert replica_b.get(key) == value
        assert storage.get(key) == value

        print("PASS: replica A recovered data after restart")
        print("PASS: replica B recovered data after restart")
        print("PASS: replicated backend recovered data after restart")

        print()
        print("=" * 48)
        print("MILESTONE 9 REPLICA RESTART TEST PASSED")
        print("=" * 48)

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
