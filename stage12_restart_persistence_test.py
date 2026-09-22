import shutil
import tempfile

from index_storage.local import LocalIndexStorage


OBJECT_COUNT = 500
OBJECT_SIZE = 1024


def main():
    root = tempfile.mkdtemp(prefix="oursearch_stage12_restart_")

    try:
        payload = b"x" * OBJECT_SIZE

        storage = LocalIndexStorage(root)

        for index in range(OBJECT_COUNT):
            storage.put(
                f"documents/{index}",
                payload,
            )

        assert len(storage.list_keys("documents/")) == OBJECT_COUNT

        del storage

        recovered = LocalIndexStorage(root)

        keys = recovered.list_keys("documents/")

        assert len(keys) == OBJECT_COUNT

        for index in range(OBJECT_COUNT):
            assert recovered.get(f"documents/{index}") == payload

        print(f"OBJECTS_WRITTEN: {OBJECT_COUNT}")
        print(f"OBJECTS_RECOVERED: {len(keys)}")
        print(f"OBJECT_SIZE_BYTES: {OBJECT_SIZE}")
        print(f"TOTAL_DATA_BYTES: {OBJECT_COUNT * OBJECT_SIZE}")
        print()
        print("MILESTONE 12 RESTART PERSISTENCE TEST: PASS")

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
