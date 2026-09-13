import time
from threading import Lock


class GlobalExpansionFeedbackController:
    """
    Connects durable domain-discovery outcomes to the global expansion
    feedback system.

    Responsibilities:
    - attribute outcomes to the original discovery source
    - observe activation and expansion outcomes
    - record each outcome idempotently
    - expose source effectiveness for future adaptation
    - isolate feedback failures from crawling/discovery

    This component does NOT replace DomainDiscoverySourceRegistry's
    health/adaptive controller.
    """

    def __init__(
        self,
        domain_store,
        expansion_queue,
        expansion_store,
        feedback,
    ):
        if domain_store is None:
            raise TypeError("domain_store is required")

        if expansion_queue is None:
            raise TypeError("expansion_queue is required")

        if expansion_store is None:
            raise TypeError("expansion_store is required")

        if feedback is None:
            raise TypeError("feedback is required")

        self.domain_store = domain_store
        self.expansion_queue = expansion_queue
        self.expansion_store = expansion_store
        self.feedback = feedback

        self._lock = Lock()

        self.stats = {
            "cycles": 0,
            "domains_observed": 0,
            "activation_success": 0,
            "activation_failed": 0,
            "expansion_success": 0,
            "expansion_failed": 0,
            "feedback_recorded": 0,
            "feedback_duplicates": 0,
            "feedback_errors": 0,
            "last_cycle_at": None,
        }

    @staticmethod
    def _source(record):
        if not isinstance(record, dict):
            return None

        source = record.get("source")

        if not isinstance(source, str):
            return None

        source = source.strip()

        return source or None

    @staticmethod
    def _hostname(record):
        if not isinstance(record, dict):
            return None

        hostname = record.get("hostname")

        if not isinstance(hostname, str):
            return None

        hostname = hostname.strip().lower().rstrip(".")

        return hostname or None

    def _record(self, hostname, source, outcome):
        try:
            result = self.feedback.record_outcome(
                hostname=hostname,
                source=source,
                outcome=outcome,
            )

            if result.get("recorded"):
                with self._lock:
                    self.stats["feedback_recorded"] += 1

            elif result.get("duplicate"):
                with self._lock:
                    self.stats["feedback_duplicates"] += 1

            return result

        except Exception:
            with self._lock:
                self.stats["feedback_errors"] += 1

            return {
                "recorded": False,
                "duplicate": False,
                "error": True,
            }

    def observe_domain(
        self,
        record,
    ):
        hostname = self._hostname(record)
        source = self._source(record)

        if hostname is None or source is None:
            return {
                "observed": False,
                "hostname": hostname,
                "source": source,
            }

        status = record.get("status")

        result = {
            "observed": True,
            "hostname": hostname,
            "source": source,
            "status": status,
            "activation": None,
            "expansion": None,
        }

        with self._lock:
            self.stats["domains_observed"] += 1

        if status == "activated":
            activation = self._record(
                hostname,
                source,
                "activation_success",
            )

            result["activation"] = activation

            if activation.get("recorded"):
                with self._lock:
                    self.stats["activation_success"] += 1

        elif status == "activation_failed":
            activation = self._record(
                hostname,
                source,
                "activation_failed",
            )

            result["activation"] = activation

            if activation.get("recorded"):
                with self._lock:
                    self.stats["activation_failed"] += 1

        return result

    def observe_expansion(
        self,
        hostname,
        source,
    ):
        hostname = self._hostname(
            {"hostname": hostname}
        )

        if hostname is None:
            return None

        try:
            candidate = self.expansion_store.get(hostname)
        except Exception:
            candidate = None

        if candidate is None:
            return None

        status = candidate.get("status")

        if status == "complete":
            result = self._record(
                hostname,
                source,
                "expansion_success",
            )

            if result.get("recorded"):
                with self._lock:
                    self.stats["expansion_success"] += 1

            return result

        if status == "failed":
            result = self._record(
                hostname,
                source,
                "expansion_failed",
            )

            if result.get("recorded"):
                with self._lock:
                    self.stats["expansion_failed"] += 1

            return result

        return {
            "recorded": False,
            "duplicate": False,
            "pending": True,
            "status": status,
        }

    def process(
        self,
        limit=100,
    ):
        try:
            limit = max(1, int(limit))
        except Exception:
            limit = 100

        records = []

        statuses = (
            "discovered",
            "approved",
            "activated",
            "activation_failed",
        )

        remaining = limit

        for status in statuses:
            if remaining <= 0:
                break

            try:
                batch = self.domain_store.list_candidates(
                    status=status,
                    limit=remaining,
                )
            except Exception:
                continue

            records.extend(batch)
            remaining = limit - len(records)

        results = []

        for record in records:
            try:
                observed = self.observe_domain(record)

                if not observed.get("observed"):
                    continue

                source = observed.get("source")
                hostname = observed.get("hostname")

                expansion = self.observe_expansion(
                    hostname,
                    source,
                )

                observed["expansion"] = expansion

                results.append(observed)

            except Exception:
                with self._lock:
                    self.stats["feedback_errors"] += 1

        with self._lock:
            self.stats["cycles"] += 1
            self.stats["last_cycle_at"] = time.time()

        return {
            "processed": len(results),
            "results": results,
        }

    def source_effectiveness(self):
        try:
            return self.feedback.source_stats()
        except Exception:
            with self._lock:
                self.stats["feedback_errors"] += 1

            return []

    def adaptation_multiplier(self, source):
        try:
            score = self.feedback.get_score(source)
        except Exception:
            with self._lock:
                self.stats["feedback_errors"] += 1
            return 1.0

        return max(0.1, min(2.0, float(score)))

    def adapt_candidate_priority(self, record):
        """
        Adapt a discovered candidate's base priority using durable
        source-effectiveness feedback.

        Candidates without prior feedback remain unchanged.
        """
        if not isinstance(record, dict):
            return 50.0

        try:
            base_priority = float(record.get("priority", 50.0))
        except (TypeError, ValueError):
            base_priority = 50.0

        base_priority = max(0.0, min(100.0, base_priority))

        source = self._source(record)
        if source is None:
            return base_priority

        multiplier = self.adaptation_multiplier(source)

        try:
            adapted = base_priority * multiplier
        except Exception:
            adapted = base_priority

        return max(0.0, min(100.0, float(adapted)))

    def status(self):
        with self._lock:
            stats = dict(self.stats)

        return {
            "stats": stats,
            "sources": self.source_effectiveness(),
        }
