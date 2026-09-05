from urllib.parse import urlparse
import heapq
import time


class CrawlFrontier:

    def __init__(
        self,
        default_delay=2,
        max_retries=3
    ):

        self.domains = {}

        self.url_entries = {}

        self.heap = []

        self.sequence = 0

        self.default_delay = default_delay

        self.max_retries = max_retries

    def _domain(self, url):

        parsed = urlparse(url)

        return parsed.netloc.lower()

    def _ensure_domain(self, domain):

        if domain not in self.domains:

            self.domains[domain] = {
                "last_crawl_time": 0,
                "crawl_delay":
                    self.default_delay,
                "failures": {}
            }

    def add(
        self,
        url,
        priority=50,
        available_at=None
    ):

        if url in self.url_entries:
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
            "available_at": available_at,
            "sequence": self.sequence
        }

        self.sequence += 1

        self.url_entries[url] = entry

        heapq.heappush(
            self.heap,
            (
                -float(priority),
                available_at,
                entry["sequence"],
                url
            )
        )

        return True

    def size(self):

        return len(self.url_entries)

    def _domain_ready(self, domain):

        data = self.domains[domain]

        return (
            time.time()
            - data["last_crawl_time"]
            >= data["crawl_delay"]
        )

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

            break

        for item in skipped:

            heapq.heappush(
                self.heap,
                item
            )

        return selected

    def mark_crawled(self, url):

        domain = self._domain(url)

        if domain in self.domains:

            self.domains[
                domain
            ]["last_crawl_time"] = time.time()

    def mark_failed(
        self,
        url
    ):

        domain = self._domain(url)

        if domain not in self.domains:
            return False

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

            self.add(
                url,
                priority=max(
                    1,
                    50 - count * 10
                ),
                available_at=(
                    time.time() + delay
                )
            )

            return True

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

        return True

    def get_state(self):

        return {
            "domains":
                self.domains,

            "url_entries":
                self.url_entries,

            "sequence":
                self.sequence
        }

    def load_state(
        self,
        state
    ):

        if not state:
            return

        self.domains = dict(
            state.get(
                "domains",
                {}
            )
        )

        self.url_entries = dict(
            state.get(
                "url_entries",
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
                        entry["priority"]
                    ),
                    float(
                        entry["available_at"]
                    ),
                    int(
                        entry["sequence"]
                    ),
                    url
                )
            )
