from crawler_system.domain_candidate_store import (
    DomainCandidateStore,
)
from crawler_system.expansion_store import (
    ExpansionCandidateStore,
)
from crawler_system.expansion_queue import (
    ExpansionQueue,
)


class DomainCandidateActivator:
    """
    Controlled activation bridge between the independent
    domain-discovery pipeline and the existing expansion pipeline.

    Flow:

        DomainCandidateStore
                ↓
        ExpansionCandidateStore
                ↓
        ExpansionQueue
                ↓
        Existing crawler

    This component does not crawl domains.
    """

    DEFAULT_ELIGIBLE_STATUSES = {
        "discovered",
        "approved",
    }

    def __init__(
        self,
        domain_store,
        expansion_store,
        expansion_queue,
        eligible_statuses=None,
    ):
        if not isinstance(
            domain_store,
            DomainCandidateStore,
        ):
            raise TypeError(
                "domain_store must be DomainCandidateStore"
            )

        if not isinstance(
            expansion_store,
            ExpansionCandidateStore,
        ):
            raise TypeError(
                "expansion_store must be ExpansionCandidateStore"
            )

        if not isinstance(
            expansion_queue,
            ExpansionQueue,
        ):
            raise TypeError(
                "expansion_queue must be ExpansionQueue"
            )

        self.domain_store = domain_store
        self.expansion_store = expansion_store
        self.expansion_queue = expansion_queue

        if eligible_statuses is None:
            eligible_statuses = (
                self.DEFAULT_ELIGIBLE_STATUSES
            )

        self.eligible_statuses = {
            status.strip().lower()
            for status in eligible_statuses
            if isinstance(status, str)
            and status.strip()
        }

    @staticmethod
    def _normalize_hostname(hostname):
        if not isinstance(hostname, str):
            return None

        hostname = (
            hostname
            .strip()
            .lower()
            .rstrip(".")
        )

        if not hostname:
            return None

        return hostname

    @staticmethod
    def _candidate_from_record(record):
        if not isinstance(record, dict):
            return None

        hostname = record.get("hostname")
        first_url = record.get("url")

        if not isinstance(hostname, str):
            return None

        if not isinstance(first_url, str):
            return None

        hostname = (
            hostname
            .strip()
            .lower()
            .rstrip(".")
        )

        first_url = first_url.strip()

        if not hostname or not first_url:
            return None

        return {
            "hostname": hostname,
            "first_url": first_url,
            "source_hostname": None,
            "discovered_at": float(
                record.get("discovered_at", 0.0)
            ),
        }

    def _queue_existing_candidate(
        self,
        expansion_candidate,
        priority,
    ):
        if not isinstance(
            expansion_candidate,
            dict,
        ):
            return False

        candidate = {
            "hostname": expansion_candidate.get(
                "hostname"
            ),
            "first_url": expansion_candidate.get(
                "first_url"
            ),
            "source_hostname": expansion_candidate.get(
                "source_hostname"
            ),
            "discovered_at": expansion_candidate.get(
                "discovered_at",
                0.0,
            ),
        }

        return self.expansion_queue.enqueue(
            candidate,
            priority=priority,
        )

    def activate(
        self,
        hostname,
        priority=None,
    ):
        """
        Activate one independently discovered domain.

        Returns a structured result describing what happened.
        """

        hostname = self._normalize_hostname(hostname)

        if hostname is None:
            return {
                "success": False,
                "status": "invalid_hostname",
                "hostname": None,
            }

        record = self.domain_store.get(hostname)

        if record is None:
            return {
                "success": False,
                "status": "not_found",
                "hostname": hostname,
            }

        current_status = record.get("status")

        if isinstance(current_status, str):
            current_status = current_status.strip().lower()
        else:
            current_status = ""

        if current_status == "activated":
            return {
                "success": True,
                "status": "already_activated",
                "hostname": hostname,
            }

        if current_status not in self.eligible_statuses:
            return {
                "success": False,
                "status": "not_eligible",
                "hostname": hostname,
                "candidate_status": current_status,
            }

        candidate = self._candidate_from_record(record)

        if candidate is None:
            return {
                "success": False,
                "status": "invalid_candidate",
                "hostname": hostname,
            }

        if priority is None:
            priority = record.get(
                "priority",
                50.0,
            )

        try:
            priority = float(priority)
        except (TypeError, ValueError):
            return {
                "success": False,
                "status": "invalid_priority",
                "hostname": hostname,
            }

        if priority < 0.0 or priority > 100.0:
            return {
                "success": False,
                "status": "invalid_priority",
                "hostname": hostname,
            }

        try:
            expansion_added = (
                self.expansion_store.add(
                    candidate["first_url"]
                )
            )

            expansion_record = (
                self.expansion_store.get(
                    hostname
                )
            )

            if expansion_record is None:
                self.domain_store.mark_status(
                    hostname,
                    "activation_failed",
                )

                return {
                    "success": False,
                    "status": "expansion_store_failed",
                    "hostname": hostname,
                }

            queued = self._queue_existing_candidate(
                expansion_record,
                priority,
            )

            if queued:
                self.domain_store.mark_status(
                    hostname,
                    "activated",
                )

                return {
                    "success": True,
                    "status": "activated",
                    "hostname": hostname,
                    "expansion_added": expansion_added,
                    "queued": True,
                    "priority": priority,
                }

            queue_status = self.expansion_queue.next()

            if (
                isinstance(queue_status, dict)
                and queue_status.get("hostname")
                == hostname
            ):
                self.domain_store.mark_status(
                    hostname,
                    "activated",
                )

                return {
                    "success": True,
                    "status": "already_queued",
                    "hostname": hostname,
                    "expansion_added": expansion_added,
                    "queued": False,
                    "priority": priority,
                }

            queue_count = self.expansion_queue.count()

            if queue_count >= 0:
                existing_queue = None

                with self.expansion_queue._lock:
                    connection = (
                        self.expansion_queue._connect()
                    )

                    try:
                        row = connection.execute(
                            """
                            SELECT
                                hostname,
                                status
                            FROM expansion_queue
                            WHERE hostname = ?
                            """,
                            (hostname,),
                        ).fetchone()

                        if row is not None:
                            existing_queue = dict(row)

                    finally:
                        connection.close()

                if existing_queue is not None:
                    self.domain_store.mark_status(
                        hostname,
                        "activated",
                    )

                    return {
                        "success": True,
                        "status": "already_in_pipeline",
                        "hostname": hostname,
                        "expansion_added": expansion_added,
                        "queued": False,
                        "queue_status": existing_queue.get(
                            "status"
                        ),
                        "priority": priority,
                    }

            self.domain_store.mark_status(
                hostname,
                "activation_failed",
            )

            return {
                "success": False,
                "status": "queue_failed",
                "hostname": hostname,
                "expansion_added": expansion_added,
                "queued": False,
                "priority": priority,
            }

        except Exception:
            self.domain_store.mark_status(
                hostname,
                "activation_failed",
            )

            raise

    def activate_many(
        self,
        hostnames,
        priority=None,
    ):
        """
        Activate multiple candidates independently.

        Returns summary counts and individual results.
        """

        if hostnames is None:
            return {
                "input": 0,
                "activated": 0,
                "already_active": 0,
                "failed": 0,
                "results": [],
            }

        results = []

        for hostname in hostnames:
            result = self.activate(
                hostname,
                priority=priority,
            )

            results.append(result)

        activated = sum(
            1
            for result in results
            if result.get("success")
            and result.get("status")
            in {
                "activated",
                "already_activated",
                "already_queued",
                "already_in_pipeline",
            }
        )

        already_active = sum(
            1
            for result in results
            if result.get("status")
            == "already_activated"
        )

        failed = len(results) - activated

        return {
            "input": len(results),
            "activated": activated,
            "already_active": already_active,
            "failed": failed,
            "results": results,
        }
