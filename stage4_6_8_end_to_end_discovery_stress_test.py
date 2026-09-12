import os
import tempfile

from crawler_system.domain_candidate_store import DomainCandidateStore
from crawler_system.domain_candidate_activation import DomainCandidateActivator
from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.expansion_store import ExpansionCandidateStore
from crawler_system.expansion_queue import ExpansionQueue


COUNT = 1000


class FakeCrawler:
    def __init__(self):
        self.added_urls = []

    def add_url(self, url, **kwargs):
        self.added_urls.append((url, kwargs))
        return True


def make_candidate(i):
    return DomainCandidate(
        hostname=f"e2e-{i}.example",
        url=f"https://e2e-{i}.example/",
        source="certificate_transparency",
        evidence=f"e2e-{i}",
        discovered_at=float(i),
        metadata={"batch": "4.6.8", "id": i},
    )


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        domain_db = os.path.join(temp_dir, "domains.db")
        expansion_db = os.path.join(temp_dir, "expansion.db")

        domain_store = DomainCandidateStore(domain_db)
        expansion_store = ExpansionCandidateStore(expansion_db)
        queue = ExpansionQueue(expansion_db)

        for i in range(COUNT):
            assert domain_store.add(make_candidate(i))

        assert domain_store.count() == COUNT

        activated = 0

        for i in range(COUNT):
            hostname = f"e2e-{i}.example"

            activation = DomainCandidateActivator(
                domain_store=domain_store,
                expansion_store=expansion_store,
                expansion_queue=queue,
            )

            if activation.activate(hostname):
                activated += 1

        queued = queue.count(status="queued")

        print(f"DISCOVERED: {COUNT}")
        print(f"ACTIVATED: {activated}")
        print(f"QUEUED: {queued}")

        assert activated == COUNT
        assert queued == COUNT

        crawler = FakeCrawler()

        processed = 0

        while True:
            candidate = queue.claim_next()

            if candidate is None:
                break

            added = crawler.add_url(
                candidate["first_url"],
                source="expansion",
                depth=0,
                seed=False,
                source_url=candidate["first_url"],
            )

            assert added is True

            assert queue.mark_complete(
                candidate["hostname"]
            )

            processed += 1

        complete = queue.count(status="complete")

        print(f"PROCESSED: {processed}")
        print(f"COMPLETE: {complete}")
        print(f"CRAWLER_ADD_URL_CALLS: {len(crawler.added_urls)}")

        assert processed == COUNT
        assert complete == COUNT
        assert len(crawler.added_urls) == COUNT

        for url, kwargs in crawler.added_urls:
            assert kwargs["source"] == "expansion"
            assert kwargs["depth"] == 0
            assert kwargs["seed"] is False
            assert kwargs["source_url"] == url

        reopened_domains = DomainCandidateStore(domain_db)
        reopened_expansion = ExpansionCandidateStore(expansion_db)
        reopened_queue = ExpansionQueue(expansion_db)

        assert reopened_domains.count() == COUNT
        assert reopened_expansion.count(status="discovered") == COUNT
        assert reopened_queue.count(status="complete") == COUNT
        assert reopened_queue.count(status="queued") == 0
        assert reopened_queue.count(status="processing") == 0

        print("MASS DISCOVERY: PASS")
        print("MASS ACTIVATION: PASS")
        print("MASS QUEUEING: PASS")
        print("END-TO-END PROCESSING: PASS")
        print("CRAWLER INTEGRATION: PASS")
        print("SOURCE PROPAGATION: PASS")
        print("NO LOST CANDIDATES: PASS")
        print("RESTART PERSISTENCE: PASS")
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
