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
    source = MemoryStorage()
    target = MemoryStorage()

    source.put(
        "documents/keep",
        b"keep this",
    )

    target.put(
        "documents/keep",
        b"old value",
    )

    target.put(
        "documents/stale",
        b"this must be removed",
    )

    storage = ReplicatedIndexStorage(
        [source, target],
        write_quorum=1,
    )

    repaired = storage.repair_replica(
        target_index=1,
        source_index=0,
    )

    assert repaired == 1
    assert target.get("documents/keep") == b"keep this"
    assert not target.exists("documents/stale")

    print("PASS: existing key was synchronized")
    print("PASS: stale extra key was removed")
    print("PASS: repair reconciled target with source")

    print()
    print("=" * 48)
    print("MILESTONE 9 REPLICA RECONCILIATION TEST PASSED")
    print("=" * 48)


if __name__ == "__main__":
    main()
