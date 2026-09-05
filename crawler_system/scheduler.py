import time


class CrawlScheduler:

    def __init__(self, default_delay=2):
        self.default_delay = default_delay
        self.last_crawl = {}

    def is_ready(self, domain):
        now = time.time()

        last_time = self.last_crawl.get(
            domain,
            0
        )

        return (
            now - last_time
            >= self.default_delay
        )

    def mark_crawled(self, domain):
        self.last_crawl[domain] = time.time()
