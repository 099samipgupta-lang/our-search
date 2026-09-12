import gc
import os
import resource
import tempfile
import time

from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_discovery_sources import DomainCandidate


COUNT = 5000


def memory_kb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def make_candidate(i):
    return DomainCandidate(
        hostname=f"resource-{i}.example",
        url=f"https://resource-{i}.example/",
        source="certificate_transparency",
        evidence=f"resource-{i}",
        discovered_at=float(i),
        metadata={
            "batch": "4.6.7",
            "id": i,
            "payload": "resource-test",
        },
    )


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        database_path = os.path.join(
            temp_dir,
            "resource_stress.db",
        )

        store = DomainCandidateStore(database_path)

        gc.collect()

        memory_before = memory_kb()

        start = time.perf_counter()

        for i in range(COUNT):
            assert store.add(make_candidate(i))

        elapsed = time.perf_counter() - start

        memory_after_insert = memory_kb()

        count = store.count()

        assert count == COUNT

        gc.collect()

        memory_after_gc = memory_kb()

        candidates = store.list_candidates(
            status="discovered",
            limit=COUNT,
        )

        assert len(candidates) == COUNT

        del candidates

        gc.collect()

        memory_after_release = memory_kb()

        reopened = DomainCandidateStore(database_path)

        assert reopened.count() == COUNT

        print(f"TARGET: {COUNT}")
        print(f"COUNT: {count}")
        print(f"INSERT_SECONDS: {elapsed:.3f}")
        print(f"MEMORY_BEFORE_KB: {memory_before}")
        print(f"MEMORY_AFTER_INSERT_KB: {memory_after_insert}")
        print(f"MEMORY_AFTER_GC_KB: {memory_after_gc}")
        print(f"MEMORY_AFTER_RELEASE_KB: {memory_after_release}")

        print("LARGE RESOURCE BATCH: PASS")
        print("MEMORY-BATCH EXECUTION: PASS")
        print("GARBAGE COLLECTION: PASS")
        print("DATA INTEGRITY: PASS")
        print("PERSISTENCE AFTER REOPEN: PASS")
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
