import os
import tempfile
import time

from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_discovery_sources import DomainCandidate


COUNT = 2000


def make_candidate(i):
    return DomainCandidate(
        hostname=f"scale-test-{i}.example",
        url=f"https://scale-test-{i}.example/",
        source="certificate_transparency",
        evidence=f"scale-test-{i}",
        discovered_at=float(i),
        metadata={"batch": "4.6.1", "id": i},
    )


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        database_path = os.path.join(temp_dir, "candidate_volume.db")

        store = DomainCandidateStore(database_path)

        start = time.perf_counter()

        inserted = 0

        for i in range(COUNT):
            if store.add(make_candidate(i)):
                inserted += 1

        elapsed = time.perf_counter() - start

        count = store.count()
        candidates = store.list_candidates(
            status="discovered",
            limit=COUNT,
        )

        print(f"INSERTED: {inserted}")
        print(f"COUNT: {count}")
        print(f"LISTED: {len(candidates)}")
        print(f"ELAPSED_SECONDS: {elapsed:.3f}")

        assert inserted == COUNT
        assert count == COUNT
        assert len(candidates) == COUNT

        priorities = [float(candidate["priority"]) for candidate in candidates]
        assert priorities == sorted(priorities, reverse=True)

        print("VOLUME INSERTION: PASS")
        print("VOLUME COUNT: PASS")
        print("VOLUME LIST: PASS")
        print("PRIORITY ORDER: PASS")

        reopened = DomainCandidateStore(database_path)

        reopened_count = reopened.count()

        print(f"REOPENED_COUNT: {reopened_count}")

        assert reopened_count == COUNT

        reopened_candidates = reopened.list_candidates(
            status="discovered",
            limit=COUNT,
        )

        assert len(reopened_candidates) == COUNT

        print("PERSISTENCE AFTER REOPEN: PASS")
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
