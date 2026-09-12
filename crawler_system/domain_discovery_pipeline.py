from crawler_system.domain_candidate_pipeline import (
    DomainCandidatePipeline,
)
from crawler_system.domain_candidate_store import (
    DomainCandidateStore,
)
from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
    DomainDiscoverySourceRegistry,
)


class DomainDiscoveryPipeline:
    """
    Production integration layer for independent domain discovery.

    Flow:

        Discovery Registry
              ↓
        Candidate Pipeline
              ↓
        Durable Candidate Store

    This component discovers domain candidates only.
    It does not crawl or activate domains.
    """

    def __init__(
        self,
        registry: DomainDiscoverySourceRegistry,
        store: DomainCandidateStore,
        candidate_pipeline=None,
    ):
        if not isinstance(
            registry,
            DomainDiscoverySourceRegistry,
        ):
            raise TypeError(
                "registry must be DomainDiscoverySourceRegistry"
            )

        if not isinstance(
            store,
            DomainCandidateStore,
        ):
            raise TypeError(
                "store must be DomainCandidateStore"
            )

        self.registry = registry
        self.store = store

        if candidate_pipeline is None:
            candidate_pipeline = DomainCandidatePipeline()

        if not isinstance(
            candidate_pipeline,
            DomainCandidatePipeline,
        ):
            raise TypeError(
                "candidate_pipeline must be DomainCandidatePipeline"
            )

        self.candidate_pipeline = candidate_pipeline

    def discover(
        self,
        context: DomainDiscoveryContext | None = None,
    ):
        """
        Execute registered discovery sources and return the
        normalized, validated, deduplicated candidates that
        were accepted by the pipeline.
        """

        candidates = self.registry.discover(context)

        accepted = self.candidate_pipeline.process_many(
            candidates
        )

        return accepted

    def discover_and_store(
        self,
        context: DomainDiscoveryContext | None = None,
    ):
        """
        Execute discovery, process candidates through the
        candidate pipeline, and persist accepted candidates.

        Returns a result dictionary containing:
            discovered
            accepted
            stored
            duplicates
        """

        candidates = self.registry.discover(context)

        accepted = self.candidate_pipeline.process_many(
            candidates
        )

        stored = 0
        duplicates = 0

        for candidate in accepted:
            if self.store.add(candidate):
                stored += 1
            else:
                duplicates += 1

        return {
            "discovered": len(candidates),
            "accepted": len(accepted),
            "stored": stored,
            "duplicates": duplicates,
        }

    def process_candidates(
        self,
        candidates,
    ):
        """
        Process externally supplied candidates without executing
        discovery sources.
        """

        if candidates is None:
            return set()

        return self.candidate_pipeline.process_many(
            candidates
        )

    def process_and_store(
        self,
        candidates,
    ):
        """
        Process externally supplied candidates and persist the
        accepted candidates.
        """

        accepted = self.process_candidates(candidates)

        stored = 0
        duplicates = 0

        for candidate in accepted:
            if self.store.add(candidate):
                stored += 1
            else:
                duplicates += 1

        return {
            "input": len(candidates) if candidates is not None else 0,
            "accepted": len(accepted),
            "stored": stored,
            "duplicates": duplicates,
        }

    def stored_candidates(
        self,
        status="discovered",
        limit=100,
    ):
        return self.store.list_candidates(
            status=status,
            limit=limit,
        )

    def stored_count(self, status=None):
        return self.store.count(status=status)
