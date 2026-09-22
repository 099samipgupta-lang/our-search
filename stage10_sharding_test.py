from index_storage.backend import IndexStorageBackend
from index_storage.sharding import ShardedIndexStorage


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
    shards = [
        MemoryStorage(),
        MemoryStorage(),
        MemoryStorage(),
        MemoryStorage(),
    ]

    storage = ShardedIndexStorage(shards)

    keys = [
        "documents/a",
        "documents/b",
        "documents/c",
        "documents/d",
        "documents/e",
        "documents/f",
        "documents/g",
        "documents/h",
        "documents/i",
        "documents/j",
    ]

    for key in keys:
        storage.put(key, key.encode())

    # Every object must be retrievable through the shard router.
    for key in keys:
        assert storage.get(key) == key.encode()
        assert storage.exists(key)

    # Verify deterministic routing.
    for key in keys:
        first = storage.shard_for_key(key)
        second = storage.shard_for_key(key)
        assert first == second
        assert 0 <= first < 4

    # Verify the data is physically distributed among shards.
    populated_shards = sum(
        bool(shard.list_keys(""))
        for shard in shards
    )

    assert populated_shards >= 2

    # Verify global listing.
    assert storage.list_keys("") == sorted(keys)

    # Verify deletion through the shard router.
    storage.delete("documents/e")

    assert not storage.exists("documents/e")
    assert storage.get("documents/e") is None

    print("PASS: keys routed to deterministic shards")
    print("PASS: all sharded reads succeeded")
    print("PASS: data distributed across multiple shards")
    print("PASS: global key listing succeeded")
    print("PASS: sharded deletion succeeded")

    print()
    print("=" * 48)
    print("MILESTONE 10 SHARDING TEST PASSED")
    print("=" * 48)


if __name__ == "__main__":
    main()
