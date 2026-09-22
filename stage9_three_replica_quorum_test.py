from index_storage.backend import IndexStorageBackend
from index_storage.replication import ReplicatedIndexStorage


class MemoryStorage(IndexStorageBackend):
    def __init__(self):
        self.data = {}

    def put(self, key, data):
        self.data[key] = data

    def get(self, key):
        return self.data.get(key)

    def exists(self, key):
        return key in self.data

    def delete(self, key):
        self.data.pop(key, None)

    def list_keys(self, prefix=""):
        return sorted(
            key for key in self.data
            if key.startswith(prefix)
        )


class FailingStorage(IndexStorageBackend):
    def put(self, key, data):
        raise RuntimeError("simulated replica failure")

    def get(self, key):
        raise RuntimeError("simulated replica failure")

    def exists(self, key):
        raise RuntimeError("simulated replica failure")

    def delete(self, key):
        raise RuntimeError("simulated replica failure")

    def list_keys(self, prefix=""):
        raise RuntimeError("simulated replica failure")


def main():
    replica_a = MemoryStorage()
    replica_b = MemoryStorage()
    replica_c = FailingStorage()

    storage = ReplicatedIndexStorage(
        [replica_a, replica_b, replica_c],
        write_quorum=2,
    )

    storage.put(
        "documents/three-replica",
        b"OUR SEARCH three replica quorum",
    )

    assert replica_a.get(
        "documents/three-replica"
    ) == b"OUR SEARCH three replica quorum"

    assert replica_b.get(
        "documents/three-replica"
    ) == b"OUR SEARCH three replica quorum"

    assert storage.get(
        "documents/three-replica"
    ) == b"OUR SEARCH three replica quorum"

    print("PASS: 2 of 3 replicas reached quorum")
    print("PASS: failed replica did not block the write")
    print("PASS: healthy replicas retained the data")
    print("PASS: read returned replicated data")

    print()
    print("=" * 48)
    print("MILESTONE 9 THREE-REPLICA QUORUM TEST PASSED")
    print("=" * 48)


if __name__ == "__main__":
    main()
