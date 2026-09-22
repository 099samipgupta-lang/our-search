import shutil
import tempfile
import time

from index_storage.local import LocalIndexStorage
from index_storage.large_objects import LargeObjectStore


OBJECT_COUNT = 3
OBJECT_SIZE = 5 * 1024 * 1024


def main():
    root = tempfile.mkdtemp(prefix="oursearch_stage12_large_")

    try:
        storage = LocalIndexStorage(root)
        large_store = LargeObjectStore(storage)

        payload = b"x" * OBJECT_SIZE

        start = time.perf_counter()

        for index in range(OBJECT_COUNT):
            large_store.put(
                f"large/{index}",
                payload,
            )

        write_seconds = time.perf_counter() - start

        start = time.perf_counter()

        for index in range(OBJECT_COUNT):
            data = large_store.get(f"large/{index}")
            assert data == payload

        read_seconds = time.perf_counter() - start

        print(f"OBJECTS: {OBJECT_COUNT}")
        print(f"OBJECT_SIZE_BYTES: {OBJECT_SIZE}")
        print(f"TOTAL_DATA_BYTES: {OBJECT_COUNT * OBJECT_SIZE}")
        print(f"WRITE_SECONDS: {write_seconds:.6f}")
        print(f"READ_SECONDS: {read_seconds:.6f}")
        print(
            f"WRITE_OBJECTS_PER_SECOND: "
            f"{OBJECT_COUNT / write_seconds:.2f}"
        )
        print(
            f"READ_OBJECTS_PER_SECOND: "
            f"{OBJECT_COUNT / read_seconds:.2f}"
        )
        print(f"VERIFIED_OBJECTS: {OBJECT_COUNT}")
        print()
        print("MILESTONE 12 LARGE OBJECT TEST: PASS")

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
