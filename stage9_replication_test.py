import os
import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.replication import ReplicatedIndexStorage


def main():
    root = tempfile.mkdtemp(prefix="stage9_replication_")

    try:
        primary_path = os.path.join(root, "primary")
        replica_path = os.path.join(root, "replica")

        primary = LocalIndexStorage(primary_path)
        replica = LocalIndexStorage(replica_path)

        storage = ReplicatedIndexStorage(
            [primary, replica],
            write_quorum=2,
        )

        storage.put(
            "documents/test",
            b"OUR SEARCH replication test",
        )

        assert primary.get(
            "documents/test"
        ) == b"OUR SEARCH replication test"

        assert replica.get(
            "documents/test"
        ) == b"OUR SEARCH replication test"

        assert storage.get(
            "documents/test"
        ) == b"OUR SEARCH replication test"

        # Simulate primary failure by removing the primary data.
        shutil.rmtree(primary_path)

        primary = LocalIndexStorage(primary_path)

        # The replica still contains the data.
        assert replica.get(
            "documents/test"
        ) == b"OUR SEARCH replication test"

        # Rebuild the replicated backend with the failed primary first.
        storage = ReplicatedIndexStorage(
            [primary, replica],
            write_quorum=1,
        )

        assert storage.get(
            "documents/test"
        ) == b"OUR SEARCH replication test"

        assert storage.exists(
            "documents/test"
        )

        print("PASS: replicated write reached both replicas")
        print("PASS: replica retained data after primary loss")
        print("PASS: read failover recovered data")
        print("PASS: replicated storage failure test")

        print()
        print("=" * 48)
        print("MILESTONE 9 REPLICATION TEST PASSED")
        print("=" * 48)

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
