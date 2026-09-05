from urllib.parse import urlparse
import time
from url_dedup import URLDeduplicator

class CrawlFrontier:
    def __init__(self):
        self.domains = {}
        self.deduplicator = URLDeduplicator()
        self.max_retries = 3
        self.domain_order = []
        self.next_domain_index = 0

    def add(self, url):
        domain = urlparse(url).netloc

        if domain not in self.domains:
            self.domains[domain] = {
                "urls": [],
                "last_crawl_time": 0,
                "crawl_delay": 2,
                "failures": 0
            }

            self.domain_order.append(domain)

        if self.deduplicator.is_new(url):
            self.domains[domain]["urls"].append(url)

    def size(self):
        total = 0

        for domain in self.domains:
            total += len(self.domains[domain]["urls"])

        return total

    def get_state(self):
        return {
            "domains": self.domains,
            "domain_order": self.domain_order,
            "next_domain_index": self.next_domain_index
        }

    def get_all_urls(self):
        urls = []

        for domain in self.domains:
            urls.extend(self.domains[domain]["urls"])

        return urls

    def get_next(self):
        if not self.domain_order:
            return None

        checked = 0

        while checked < len(self.domain_order):
            domain = self.domain_order[self.next_domain_index]

            self.next_domain_index = (
                self.next_domain_index + 1
            ) % len(self.domain_order)

            checked += 1

            if self.domains[domain]["urls"] and time.time() - self.domains[domain]["last_crawl_time"] >= self.domains[domain]["crawl_delay"]:
                return self.domains[domain]["urls"].pop(0)

        return None

    def mark_crawled(self, url):
        domain = urlparse(url).netloc

        if domain in self.domains:
            self.domains[domain]["last_crawl_time"] = time.time()

    def mark_failed(self, url):
        domain = urlparse(url).netloc

        if domain in self.domains:
            self.domains[domain]["failures"] += 1

            if self.domains[domain]["failures"] <= self.max_retries:
                self.domains[domain]["urls"].append(url)
                return True

            return False

        return False
