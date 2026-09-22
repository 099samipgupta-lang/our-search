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

    source.put("documents/a", b"alpha")
    source.put("documents/b", b"bravo")
    source.put("documents/c", b"charlie")

    target.put("documents/a", b"old-alpha")
    target.put("documents/stale1", b"stale-one")
    target.put("documents/stale2", b"stale-two")

    storage = ReplicatedIndexStorage(
        [source, target],
        write_quorum=1,
    )

    repaired = storage.repair_replica(
        target_index=1,
        source_index=0,
    )

    assert repaired == 3

    assert target.get("documents/a") == b"alpha"
    assert target.get("documents/b") == b"bravo"
    assert target.get("documents/c") == b"charlie"

    assert not target.exists("documents/stale1")
    assert not target.exists("documents/stale2")

    assert target.list_keys("") == [
        "documents/a",
        "documents/b",
        "documents/c",
    ]

    print("PASS: multiple objects synchronized")
    print("PASS: existing object was updated")
    print("PASS: missing objects were restored")
    print("PASS: stale objects were removed")
    print("PASS: target namespace exactly matches source")

    print()
    print("=" * 48)
    print("MILESTONE 9 MULTI-OBJECT REPAIR TEST PASSED")
    print("=" * 48)


if __name__ == "__main__":
    main()
