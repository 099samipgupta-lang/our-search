from urllib.parse import urlparse
import heapq
import time

from crawler_system.frontier_storage import FrontierStorage


class CrawlFrontier:

    def __init__(
        self,
        default_delay=2,
        max_retries=3,
        storage_path="crawler_storage/frontier/state.json"
    ):
        self.domains = {}
        self.url_entries = {}
        self.leased_entries = {}

        self.heap = []
        self.sequence = 0

        self.default_delay = float(default_delay)
        self.max_retries = int(max_retries)

        self.storage = FrontierStorage(storage_path)

        state = self.storage.load()

        if state:
            self.load_state(state)

    def _domain(self, url):
        parsed = urlparse(url)
        return parsed.netloc.lower()

    def _ensure_domain(self, domain):

        if domain not in self.domains:
            self.domains[domain] = {
                "last_crawl_time": 0,
                "next_allowed_time": 0,
                "crawl_delay": self.default_delay,
                "failures": {}
            }

        else:
            self.domains[domain].setdefault(
                "next_allowed_time",
                0
            )

    def _save(self):
        self.storage.save(
            self.get_state()
        )

    def add(
        self,
        url,
        priority=50,
        available_at=None
    ):
        if (
            url in self.url_entries
            or url in self.leased_entries
        ):
            return False

        domain = self._domain(url)

        if not domain:
            return False

        self._ensure_domain(domain)

        if available_at is None:
            available_at = time.time()

        entry = {
            "url": url,
            "priority": float(priority),
            "available_at": float(available_at),
            "sequence": self.sequence
        }

        self.sequence += 1

        self.url_entries[url] = entry

        heapq.heappush(
            self.heap,
            (
                -float(priority),
                float(available_at),
                entry["sequence"],
                url
            )
        )

        self._save()

        return True

    def size(self):
        return (
            len(self.url_entries)
            + len(self.leased_entries)
        )

    def queued_size(self):
        return len(self.url_entries)

    def leased_size(self):
        return len(self.leased_entries)

    def _domain_ready(self, domain):

        self._ensure_domain(domain)

        data = self.domains[domain]

        now = time.time()

        last_crawl_ready = (
            data["last_crawl_time"]
            + data["crawl_delay"]
        )

        reserved_ready = data.get(
            "next_allowed_time",
            0
        )

        return now >= max(
            last_crawl_ready,
            reserved_ready
        )

    def _reserve_domain(self, domain):

        self._ensure_domain(domain)

        data = self.domains[domain]

        now = time.time()

        current_ready = data.get(
            "next_allowed_time",
            0
        )

        data["next_allowed_time"] = max(
            current_ready,
            now
        ) + data["crawl_delay"]

    def get_next(self):

        if not self.heap:
            return None

        skipped = []

        selected = None

        while self.heap:

            (
                negative_priority,
                available_at,
                sequence,
                url
            ) = heapq.heappop(
                self.heap
            )

            entry = self.url_entries.get(url)

            if entry is None:
                continue

            if entry["sequence"] != sequence:
                continue

            now = time.time()

            if now < available_at:

                skipped.append(
                    (
                        negative_priority,
                        available_at,
                        sequence,
                        url
                    )
                )

                continue

            domain = self._domain(url)

            if not self._domain_ready(domain):

                skipped.append(
                    (
                        negative_priority,
                        available_at,
                        sequence,
                        url
                    )
                )

                continue

            selected = url

            del self.url_entries[url]

            self.leased_entries[url] = {
                **entry,
                "leased_at": time.time()
            }

            self._reserve_domain(domain)

            break

        for item in skipped:
            heapq.heappush(
                self.heap,
                item
            )

        if selected is not None:
            self._save()

        return selected

    def complete(self, url):

        if url in self.leased_entries:
            del self.leased_entries[url]

            self.mark_crawled(url)

            self._save()

            return True

        return False

    def release(
        self,
        url,
        priority=None,
        available_at=None
    ):
        entry = self.leased_entries.pop(
            url,
            None
        )

        if entry is None:
            return False

        if priority is None:
            priority = entry["priority"]

        if available_at is None:
            available_at = time.time()

        new_entry = {
            "url": url,
            "priority": float(priority),
            "available_at": float(available_at),
            "sequence": self.sequence
        }

        self.sequence += 1

        self.url_entries[url] = new_entry

        heapq.heappush(
            self.heap,
            (
                -float(new_entry["priority"]),
                new_entry["available_at"],
                new_entry["sequence"],
                url
            )
        )

        self._save()

        return True

    def recover_leases(self):

        recovered = 0

        for url, entry in list(
            self.leased_entries.items()
        ):
            self.leased_entries.pop(
                url,
                None
            )

            new_entry = {
                "url": url,
                "priority": float(
                    entry.get("priority", 50)
                ),
                "available_at": time.time(),
                "sequence": self.sequence
            }

            self.sequence += 1

            self.url_entries[url] = new_entry

            heapq.heappush(
                self.heap,
                (
                    -new_entry["priority"],
                    new_entry["available_at"],
                    new_entry["sequence"],
                    url
                )
            )

            recovered += 1

        if recovered:
            self._save()

        return recovered

    def mark_crawled(self, url):

        domain = self._domain(url)

        self._ensure_domain(domain)

        self.domains[
            domain
        ]["last_crawl_time"] = time.time()

        self._save()

    def mark_failed(self, url):

        domain = self._domain(url)

        self._ensure_domain(domain)

        failures = self.domains[
            domain
        ]["failures"]

        failures[url] = (
            failures.get(url, 0) + 1
        )

        count = failures[url]

        if count <= self.max_retries:

            delay = min(
                60 * (2 ** (count - 1)),
                3600
            )

            self.release(
                url,
                priority=max(
                    1,
                    50 - count * 10
                ),
                available_at=time.time() + delay
            )

            return True

        self.leased_entries.pop(
            url,
            None
        )

        self._save()

        return False

    def reprioritize(
        self,
        url,
        priority
    ):
        if url not in self.url_entries:
            return False

        entry = self.url_entries[url]

        entry["priority"] = float(
            priority
        )

        entry["sequence"] = self.sequence

        self.sequence += 1

        heapq.heappush(
            self.heap,
            (
                -float(priority),
                entry["available_at"],
                entry["sequence"],
                url
            )
        )

        self._save()

        return True

    def get_state(self):

        return {
            "version": 3,
            "domains": self.domains,
            "url_entries": self.url_entries,
            "leased_entries": self.leased_entries,
            "sequence": self.sequence
        }

    def load_state(self, state):

        if not state:
            return

        self.domains = dict(
            state.get(
                "domains",
                {}
            )
        )

        for domain, data in self.domains.items():

            data.setdefault(
                "next_allowed_time",
                0
            )

            data.setdefault(
                "last_crawl_time",
                0
            )

            data.setdefault(
                "crawl_delay",
                self.default_delay
            )

            data.setdefault(
                "failures",
                {}
            )

        self.url_entries = dict(
            state.get(
                "url_entries",
                {}
            )
        )

        self.leased_entries = dict(
            state.get(
                "leased_entries",
                {}
            )
        )

        self.sequence = int(
            state.get(
                "sequence",
                0
            )
        )

        self.heap = []

        for url, entry in (
            self.url_entries.items()
        ):

            heapq.heappush(
                self.heap,
                (
                    -float(
                        entry.get(
                            "priority",
                            50
                        )
                    ),
                    float(
                        entry.get(
                            "available_at",
                            time.time()
                        )
                    ),
                    int(
                        entry.get(
                            "sequence",
                            0
                        )
                    ),
                    url
                )
            )

    def clear(self):

        self.domains = {}
        self.url_entries = {}
        self.leased_entries = {}
        self.heap = []
        self.sequence = 0

        self._save()
