from index_storage.backend import IndexStorageBackend
from index_storage.replication import ReplicatedIndexStorage


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
    healthy = MemoryStorage()
    failing = FailingStorage()

    storage = ReplicatedIndexStorage(
        [healthy, failing],
        write_quorum=2,
    )

    try:
        storage.put(
            "documents/quorum-test",
            b"quorum failure test",
        )
    except RuntimeError as exc:
        assert "replication quorum failed" in str(exc)
        print("PASS: write rejected when quorum was not reached")
    else:
        raise AssertionError(
            "write unexpectedly succeeded without quorum"
        )

    assert healthy.get("documents/quorum-test") == b"quorum failure test"

    print("PASS: partial replica write was detected")
    print()
    print("=" * 48)
    print("MILESTONE 9 QUORUM FAILURE TEST PASSED")
    print("=" * 48)


if __name__ == "__main__":
    main()
