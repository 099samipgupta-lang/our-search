import gc
import os
import shutil
import tempfile
import time
import tracemalloc

from index_storage.local import LocalIndexStorage


OBJECT_COUNT = 500
OBJECT_SIZE = 1024
ROUNDS = 5


def main():
    root = tempfile.mkdtemp(
        prefix="oursearch_stage12_sustained_"
    )

    try:
        storage = LocalIndexStorage(root)
        payload = b"x" * OBJECT_SIZE

        gc.collect()
        tracemalloc.start()

        start = time.perf_counter()

        for round_index in range(ROUNDS):
            prefix = f"round_{round_index}"

            for index in range(OBJECT_COUNT):
                storage.put(
                    f"{prefix}/{index}",
                    payload,
                )

            for index in range(OBJECT_COUNT):
                assert storage.get(
                    f"{prefix}/{index}"
                ) == payload

        elapsed = time.perf_counter() - start

        current_memory, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        expected_objects = OBJECT_COUNT * ROUNDS
        keys = storage.list_keys()

        assert len(keys) == expected_objects

        print(f"ROUNDS: {ROUNDS}")
        print(f"OBJECTS_PER_ROUND: {OBJECT_COUNT}")
        print(f"TOTAL_OBJECTS: {expected_objects}")
        print(f"OBJECT_SIZE_BYTES: {OBJECT_SIZE}")
        print(f"TOTAL_DATA_BYTES: {expected_objects * OBJECT_SIZE}")
        print(f"ELAPSED_SECONDS: {elapsed:.6f}")
        print(
            f"TOTAL_OPERATIONS: "
            f"{expected_objects * 2}"
        )
        print(
            f"OPERATIONS_PER_SECOND: "
            f"{(expected_objects * 2) / elapsed:.2f}"
        )
        print(
            f"CURRENT_PYTHON_MEMORY_BYTES: "
            f"{current_memory}"
        )
        print(
            f"PEAK_PYTHON_MEMORY_BYTES: "
            f"{peak_memory}"
        )
        print(f"VERIFIED_OBJECTS: {len(keys)}")
        print()
        print("MILESTONE 12 SUSTAINED WORKLOAD TEST: PASS")

    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
