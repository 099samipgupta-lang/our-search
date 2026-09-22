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


class FailingStorage(IndexStorageBackend):
    def put(self, key, data):
        raise RuntimeError("simulated shard failure")

    def get(self, key):
        raise RuntimeError("simulated shard failure")

    def exists(self, key):
        raise RuntimeError("simulated shard failure")

    def delete(self, key):
        raise RuntimeError("simulated shard failure")

    def list_keys(self, prefix=""):
        raise RuntimeError("simulated shard failure")


def main():
    healthy_a = MemoryStorage()
    healthy_b = MemoryStorage()
    failed = FailingStorage()

    storage = ShardedIndexStorage(
        [healthy_a, healthy_b, failed]
    )

    # Find a key routed to the failed shard.
    failed_key = None

    for i in range(1000):
        key = f"documents/failure-{i}"

        if storage.shard_for_key(key) == 2:
            failed_key = key
            break

    assert failed_key is not None

    try:
        storage.put(
            failed_key,
            b"failure recovery test",
        )
    except RuntimeError:
        print("PASS: failed shard rejected the write")
    else:
        raise AssertionError(
            "write unexpectedly succeeded on failed shard"
        )

    try:
        storage.get(failed_key)
    except RuntimeError:
        print("PASS: failed shard was detected during read")
    else:
        raise AssertionError(
            "failed shard unexpectedly returned data"
        )

    print("PASS: shard failure condition reproduced")
    print()
    print("=" * 48)
    print("MILESTONE 10 SHARD FAILURE TEST PASSED")
    print("=" * 48)


if __name__ == "__main__":
    main()
