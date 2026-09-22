import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.scale_testing import CapacityBenchmark


OBJECT_COUNT = 100
OBJECT_SIZE = 1024


def main():
    root = tempfile.mkdtemp(prefix="oursearch_stage12_")

    try:
        storage = LocalIndexStorage(root)
        benchmark = CapacityBenchmark(storage)
        payload = b"x" * OBJECT_SIZE

        write_result = benchmark.write_objects(
            "documents",
            payload,
            OBJECT_COUNT,
        )

        read_result = benchmark.read_objects(
            "documents",
            OBJECT_COUNT,
        )

        keys = storage.list_keys("documents/")

        assert len(keys) == OBJECT_COUNT

        for index in range(OBJECT_COUNT):
            assert storage.get(f"documents/{index}") == payload

        print(f"OBJECTS: {OBJECT_COUNT}")
        print(f"OBJECT_SIZE_BYTES: {OBJECT_SIZE}")
        print(f"TOTAL_DATA_BYTES: {OBJECT_COUNT * OBJECT_SIZE}")
        print(f"WRITE_SECONDS: {write_result.elapsed_seconds:.6f}")
        print(f"WRITE_OPS_PER_SECOND: {write_result.operations_per_second:.2f}")
        print(f"READ_SECONDS: {read_result.elapsed_seconds:.6f}")
        print(f"READ_OPS_PER_SECOND: {read_result.operations_per_second:.2f}")
        print(f"VERIFIED_OBJECTS: {len(keys)}")
        print()
        print("MILESTONE 12 CAPACITY TEST: PASS")

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
