import json

from crawler_system.discovery_priority_recalculator import (
    DiscoveryPriorityRecalculator,
)


class DiscoveryPriorityUpdater:
    """
    Updates an existing domain-discovery candidate and immediately
    recalculates its durable priority.
    """

    def __init__(self, store, priority_engine=None):
        self.store = store
        self.recalculator = DiscoveryPriorityRecalculator(
            priority_engine=priority_engine
        )

    def update_candidate(self, candidate):
        if candidate is None:
            return False

        hostname = getattr(candidate, "hostname", None)

        if not isinstance(hostname, str) or not hostname.strip():
            return False

        existing = self.store.get(hostname)

        if existing is None:
            if not self.store.add(candidate):
                return False
        else:
            metadata = getattr(candidate, "metadata", None)

            try:
                metadata_json = (
                    json.dumps(
                        metadata,
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    if metadata is not None
                    else None
                )
            except (TypeError, ValueError):
                return False

            discovered_at = candidate.discovered_at

            if discovered_at is None:
                discovered_at = existing["discovered_at"]

            with self.store._lock:
                connection = self.store._connect()

                try:
                    cursor = connection.execute(
                        """
                        UPDATE domain_candidates
                        SET url = ?,
                            source = ?,
                            evidence = ?,
                            discovered_at = ?,
                            metadata_json = ?
                        WHERE hostname = ?
                        """,
                        (
                            candidate.url,
                            candidate.source,
                            candidate.evidence,
                            float(discovered_at),
                            metadata_json,
                            self.store._normalize_hostname(hostname),
                        ),
                    )

                    connection.commit()

                    if cursor.rowcount != 1:
                        return False

                finally:
                    connection.close()

        return self.recalculator.recalculate_store_candidate(
            self.store,
            hostname,
        )

    def update_hostname(self, hostname):
        return self.recalculator.recalculate_store_candidate(
            self.store,
            hostname,
        )
