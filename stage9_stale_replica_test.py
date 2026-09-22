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


def main():
    healthy_a = MemoryStorage()
    healthy_b = MemoryStorage()
    recovering = MemoryStorage()

    storage = ReplicatedIndexStorage(
        [healthy_a, healthy_b, recovering],
        write_quorum=2,
    )

    key = "documents/recovery-test"
    value = b"OUR SEARCH recovery test"

    # Simulate the third replica being unavailable by temporarily
    # using only the two healthy replicas.
    degraded = ReplicatedIndexStorage(
        [healthy_a, healthy_b],
        write_quorum=2,
    )

    degraded.put(key, value)

    assert healthy_a.get(key) == value
    assert healthy_b.get(key) == value
    assert recovering.get(key) is None

    print("PASS: healthy replicas contain the new data")
    print("PASS: recovering replica is stale")

    # Bring the third replica back.
    # This test intentionally verifies that the current
    # replication layer does NOT automatically repair it.
    assert storage.get(key) == value
    assert recovering.get(key) is None

    print("PASS: stale replica condition reproduced")
    print()
    print("=" * 48)
    print("MILESTONE 9 STALE REPLICA TEST PASSED")
    print("=" * 48)


if __name__ == "__main__":
    main()
