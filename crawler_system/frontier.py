from urllib.parse import urlparse
import heapq
import time
import os

from crawler_system.frontier_storage import FrontierStorage
from crawler_system.document_identity import DocumentIdentity


class CrawlFrontier:
    """
    Crawl frontier.

    When a URLStateStore is supplied, SQLite becomes the source of truth
    for URL state, priority, leases, retry timing, and host scheduling.

    The legacy JSON/in-memory frontier remains available when no state_store
    is supplied, preserving backwards compatibility.
    """

    def __init__(
        self,
        default_delay=2,
        max_retries=3,
        storage_path="crawler_storage/frontier/state.json",
        state_store=None,
    ):
        self.domains = {}
        self.url_entries = {}
        self.leased_entries = {}
        self.heap = []
        self.sequence = 0

        self.default_delay = float(default_delay)
        self.max_retries = int(max_retries)

        self.state_store = state_store

        # SQLite-backed mode
        if self.state_store is not None:
            self.storage = None
            self.owner = (
                f"frontier-{os.getpid()}-{id(self)}"
            )
            return

        # Legacy JSON-backed mode
        self.storage = FrontierStorage(storage_path)

        state = self.storage.load()

        if state:
            self.load_state(state)

    # ------------------------------------------------------------------
    # Common helpers
    # ------------------------------------------------------------------

    def _domain(self, url):
        parsed = urlparse(url)
        return parsed.netloc.lower()

    # ------------------------------------------------------------------
    # SQLite-backed frontier
    # ------------------------------------------------------------------

    def _sqlite_add(self, url, priority=50, available_at=None):
        normalized_url = url

        if available_at is None:
            available_at = time.time()

        existing = self.state_store.get(normalized_url)

        if existing is None:
            host = self._domain(normalized_url)
            document_id = DocumentIdentity.from_normalized_url(
                normalized_url
            )

            # Initialize the host BEFORE inserting the URL.
            # add_discovered() creates the host with its database default
            # only when the host does not already exist. Initializing it
            # first therefore ensures this frontier's configured delay
            # becomes the authoritative initial host policy.
            self.state_store.ensure_host(
                host,
                crawl_delay=self.default_delay,
            )

            inserted = self.state_store.add_discovered(
                normalized_url,
                document_id,
                host,
                priority=priority,
            )

            if not inserted:
                return False

        existing = self.state_store.get(normalized_url)

        if existing is None:
            return False

        # Do not put already-active URLs into the queue again.
        if existing["state"] in ("queued", "leased"):
            return False

        # A crawled URL is not re-added automatically.
        # Recrawling will be handled by the scheduler later.
        if existing["state"] == "crawled":
            return False

        return self.state_store.mark_queued(
            normalized_url,
            priority=priority,
            next_crawl_at=available_at,
        )

    def _sqlite_get_next(self):
        result = self.state_store.claim_next(
            owner=self.owner,
            now=time.time(),
        )

        if result is None:
            return None

        self.leased_entries[result["url"]] = result

        return result["url"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, url, priority=50, available_at=None):
        """
        Add a URL to the frontier.

        Returns True when the URL becomes queued.
        """

        if self.state_store is not None:
            return self._sqlite_add(
                url,
                priority=priority,
                available_at=available_at,
            )

        # --------------------------------------------------------------
        # Legacy implementation
        # --------------------------------------------------------------

        domain = self._domain(url)

        if not domain:
            return False

        if url in self.url_entries:
            existing = self.url_entries[url]

            if existing["state"] in ("queued", "leased"):
                return False

        if available_at is None:
            available_at = time.time()

        entry = {
            "url": url,
            "domain": domain,
            "priority": float(priority),
            "state": "queued",
            "attempts": 0,
            "available_at": float(available_at),
            "added_at": time.time(),
            "last_crawled_at": 0,
            "last_error": None,
        }

        self.url_entries[url] = entry

        self.sequence += 1

        heapq.heappush(
            self.heap,
            (
                -float(priority),
                float(available_at),
                self.sequence,
                url,
            ),
        )

        if domain not in self.domains:
            self.domains[domain] = {
                "crawl_delay": self.default_delay,
                "last_crawl_time": 0,
                "next_allowed_time": 0,
                "failures": 0,
            }

        self._save()

        return True

    def size(self):
        if self.state_store is not None:
            counts = self.state_store.counts()

            return (
                counts.get("queued", 0)
                + counts.get("retry", 0)
                + counts.get("leased", 0)
            )

        return sum(
            1
            for entry in self.url_entries.values()
            if entry["state"] in ("queued", "retry")
        )

    def queued_size(self):
        if self.state_store is not None:
            counts = self.state_store.counts()

            return (
                counts.get("queued", 0)
                + counts.get("retry", 0)
            )

        return sum(
            1
            for entry in self.url_entries.values()
            if entry["state"] in ("queued", "retry")
        )

    def leased_size(self):
        if self.state_store is not None:
            return self.state_store.counts().get(
                "leased",
                0,
            )

        return len(self.leased_entries)

    def get_next(self):
        if self.state_store is not None:
            return self._sqlite_get_next()

        # --------------------------------------------------------------
        # Legacy implementation
        # --------------------------------------------------------------

        now = time.time()

        skipped = []

        while self.heap:
            priority, available_at, sequence, url = heapq.heappop(
                self.heap
            )

            entry = self.url_entries.get(url)

            if entry is None:
                continue

            if entry["state"] not in ("queued", "retry"):
                continue

            if available_at > now:
                skipped.append(
                    (
                        priority,
                        available_at,
                        sequence,
                        url,
                    )
                )
                continue

            domain = entry["domain"]

            if not self._domain_ready(domain, now):
                skipped.append(
                    (
                        priority,
                        available_at,
                        sequence,
                        url,
                    )
                )
                continue

            entry["state"] = "leased"
            self.leased_entries[url] = entry

            self._reserve_domain(domain, now)

            for item in skipped:
                heapq.heappush(self.heap, item)

            self._save()

            return url

        for item in skipped:
            heapq.heappush(self.heap, item)

        return None

    def complete(self, url):
        if self.state_store is not None:
            result = self.state_store.mark_crawled(url)

            self.leased_entries.pop(url, None)

            return result

        # Legacy
        entry = self.url_entries.get(url)

        if entry is None:
            return False

        entry["state"] = "crawled"
        entry["last_crawled_at"] = time.time()

        self.leased_entries.pop(url, None)

        self._save()

        return True

    def release(
        self,
        url,
        retry_at=None,
        priority=None,
        available_at=None
    ):
        """
        Release a leased URL back to the frontier.

        ``retry_at`` is the legacy timestamp parameter.
        ``available_at`` is accepted for compatibility with
        WorkerCoordinator.
        """

        if available_at is not None:
            retry_at = available_at

        if self.state_store is not None:

            if retry_at is None:
                retry_at = time.time()

            result = self.state_store.release_lease(
                url,
                retry_at=retry_at,
                increment_attempts=False,
            )

            self.leased_entries.pop(
                url,
                None
            )

            return result

        # Legacy
        entry = self.url_entries.get(url)

        if entry is None:
            return False

        if retry_at is None:
            retry_at = time.time()

        if priority is not None:
            entry["priority"] = float(priority)

        entry["state"] = "retry"
        entry["available_at"] = float(retry_at)

        self.leased_entries.pop(
            url,
            None
        )

        self.sequence += 1

        heapq.heappush(
            self.heap,
            (
                -entry["priority"],
                entry["available_at"],
                self.sequence,
                url,
            ),
        )

        self._save()

        return True

    def recover_leases(self):
        if self.state_store is not None:
            recovered = self.state_store.recover_expired_leases(
                lease_timeout=0
            )

            self.leased_entries.clear()

            return recovered

        # Legacy
        recovered = 0

        for url, entry in list(
            self.leased_entries.items()
        ):
            entry["state"] = "retry"
            entry["available_at"] = time.time()

            self.sequence += 1

            heapq.heappush(
                self.heap,
                (
                    -entry["priority"],
                    entry["available_at"],
                    self.sequence,
                    url,
                ),
            )

            recovered += 1

        self.leased_entries.clear()

        self._save()

        return recovered

    def mark_crawled(self, url):
        if self.state_store is not None:
            result = self.state_store.mark_crawled(url)

            self.leased_entries.pop(url, None)

            return result

        entry = self.url_entries.get(url)

        if entry is None:
            return False

        entry["state"] = "crawled"
        entry["last_crawled_at"] = time.time()

        self.leased_entries.pop(url, None)

        self._save()

        return True

    def mark_failed(
        self,
        url,
        error=None,
        retry_delay=30,
    ):
        if self.state_store is not None:
            entry = self.state_store.get(url)

            if entry is None:
                return False

            attempts = int(
                entry.get("attempts", 0)
            )

            self.leased_entries.pop(url, None)

            if attempts >= self.max_retries:
                return self.state_store.mark_failed(
                    url,
                    error=error,
                    retry_at=None,
                )

            retry_at = (
                time.time()
                + float(retry_delay)
            )

            return self.state_store.mark_failed(
                url,
                error=error,
                retry_at=retry_at,
            )

        # Legacy
        entry = self.url_entries.get(url)

        if entry is None:
            return False

        entry["attempts"] += 1
        entry["last_error"] = error

        self.leased_entries.pop(url, None)

        if entry["attempts"] >= self.max_retries:
            entry["state"] = "failed"

            self._save()

            return False

        entry["state"] = "retry"

        entry["available_at"] = (
            time.time()
            + float(retry_delay)
        )

        self.sequence += 1

        heapq.heappush(
            self.heap,
            (
                -entry["priority"],
                entry["available_at"],
                self.sequence,
                url,
            ),
        )

        self._save()

        return True

    def reprioritize(self, url, priority):
        if self.state_store is not None:
            return self.state_store.update_priority(
                url,
                priority,
            )

        entry = self.url_entries.get(url)

        if entry is None:
            return False

        entry["priority"] = float(priority)

        if entry["state"] in ("queued", "retry"):
            self.sequence += 1

            heapq.heappush(
                self.heap,
                (
                    -entry["priority"],
                    entry["available_at"],
                    self.sequence,
                    url,
                ),
            )

        self._save()

        return True

    # ------------------------------------------------------------------
    # Legacy domain scheduling
    # ------------------------------------------------------------------

    def _domain_ready(self, domain, now):
        info = self.domains.get(domain)

        if info is None:
            return True

        return (
            now >= info["next_allowed_time"]
        )

    def _reserve_domain(self, domain, now):
        info = self.domains.setdefault(
            domain,
            {
                "crawl_delay": self.default_delay,
                "last_crawl_time": 0,
                "next_allowed_time": 0,
                "failures": 0,
            },
        )

        info["last_crawl_time"] = now

        info["next_allowed_time"] = (
            now
            + info["crawl_delay"]
        )

    # ------------------------------------------------------------------
    # Legacy persistence
    # ------------------------------------------------------------------

    def _save(self):
        if self.state_store is not None:
            return

        self.storage.save(
            self.get_state()
        )

    def get_state(self):
        return {
            "domains": self.domains,
            "url_entries": self.url_entries,
            "leased_entries": self.leased_entries,
            "sequence": self.sequence,
            "default_delay": self.default_delay,
            "max_retries": self.max_retries,
        }

    def load_state(self, state):
        self.domains = state.get(
            "domains",
            {},
        )

        self.url_entries = state.get(
            "url_entries",
            {},
        )

        self.leased_entries = state.get(
            "leased_entries",
            {},
        )

        self.sequence = state.get(
            "sequence",
            0,
        )

        self.default_delay = float(
            state.get(
                "default_delay",
                self.default_delay,
            )
        )

        self.max_retries = int(
            state.get(
                "max_retries",
                self.max_retries,
            )
        )

        self.heap = []

        for url, entry in self.url_entries.items():
            if entry["state"] in (
                "queued",
                "retry",
            ):
                self.sequence += 1

                heapq.heappush(
                    self.heap,
                    (
                        -float(
                            entry["priority"]
                        ),
                        float(
                            entry.get(
                                "available_at",
                                0,
                            )
                        ),
                        self.sequence,
                        url,
                    ),
                )

    def clear(self):
        if self.state_store is not None:
            result = self.state_store.clear()

            self.leased_entries.clear()

            return result

        self.domains.clear()
        self.url_entries.clear()
        self.leased_entries.clear()
        self.heap.clear()
        self.sequence = 0

        self._save()
