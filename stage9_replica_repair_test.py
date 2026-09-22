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
    replica_a = MemoryStorage()
    replica_b = MemoryStorage()
    recovering = MemoryStorage()

    healthy_storage = ReplicatedIndexStorage(
        [replica_a, replica_b],
        write_quorum=2,
    )

    key = "documents/repair-test"
    value = b"OUR SEARCH replica repair"

    healthy_storage.put(key, value)

    assert replica_a.get(key) == value
    assert replica_b.get(key) == value
    assert recovering.get(key) is None

    print("PASS: recovering replica starts stale")

    storage = ReplicatedIndexStorage(
        [replica_a, replica_b, recovering],
        write_quorum=2,
    )

    repaired = storage.repair_replica(
        target_index=2,
        source_index=0,
    )

    assert repaired == 1
    assert recovering.get(key) == value

    print("PASS: stale replica was repaired")
    print("PASS: repaired replica contains source data")
    print("PASS: repair returned correct key count")

    print()
    print("=" * 48)
    print("MILESTONE 9 REPLICA REPAIR TEST PASSED")
    print("=" * 48)


if __name__ == "__main__":
    main()
