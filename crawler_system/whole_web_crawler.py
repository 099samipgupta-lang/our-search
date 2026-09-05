import time
from urllib.parse import urlparse

from crawler_system.coordinator import WorkerCoordinator
from crawler_system.discovery import WebDiscovery
from crawler_system.priority import CrawlPriority
from crawler_system.url_normalizer import URLNormalizer
from crawler_system.dedup import URLDeduplicator
from crawler_system.content_dedup import ContentDeduplicator
from crawler_system.change_tracker import ChangeTracker
from crawler_system.sitemap import SitemapDiscovery
from crawler_system.storage import CrawlStorage
from crawler_system.frontier import CrawlFrontier


class WholeWebCrawler:

    def __init__(
        self,
        worker_count=4,
        frontier_delay=2,
        task_timeout=60,
        max_attempts=3,
        storage_root="crawler_storage"
    ):

        self.normalizer = URLNormalizer()

        self.url_dedup = URLDeduplicator(
            self.normalizer
        )

        self.content_dedup = (
            ContentDeduplicator()
        )

        self.change_tracker = (
            ChangeTracker()
        )

        self.priority = CrawlPriority()

        self.discovery = WebDiscovery()

        self.sitemap = SitemapDiscovery()

        self.frontier = CrawlFrontier(
            default_delay=frontier_delay,
            max_retries=max_attempts
        )

        self.storage = CrawlStorage(
            root=storage_root
        )

        self.coordinator = WorkerCoordinator(
            self.frontier,
            worker_count=worker_count,
            document_id_start=1,
            task_timeout=task_timeout,
            max_attempts=max_attempts
        )

        self.seeds = set()

        self.running = False

        self.stats = {
            "discovered": 0,
            "accepted_urls": 0,
            "duplicates": 0,
            "pages_completed": 0,
            "pages_failed": 0,
            "new_pages": 0,
            "changed_pages": 0,
            "unchanged_pages": 0,
            "gone_pages": 0,
            "unique_contents": 0,
            "exact_duplicates": 0,
            "possible_duplicates": 0,
            "sitemap_urls": 0,
            "sitemap_added": 0,
            "storage_errors": 0
        }

    def add_seed(self, url):

        normalized = self.normalizer.normalize(
            url
        )

        if normalized is None:
            return False

        self.seeds.add(
            normalized
        )

        return self._add_url(
            normalized,
            source="seed",
            seed=True
        )

    def _add_url(
        self,
        url,
        source="discovery",
        depth=0,
        seed=False
    ):

        normalized = self.normalizer.normalize(
            url
        )

        if normalized is None:
            return False

        self.stats["discovered"] += 1

        if not self.url_dedup.is_new(
            normalized
        ):

            self.stats["duplicates"] += 1

            return False

        priority = self.priority.score(
            source=source,
            depth=depth,
            seed=seed
        )

        added = self.frontier.add(
            normalized,
            priority=priority
        )

        if added:

            self.stats[
                "accepted_urls"
            ] += 1

        return added

    def add_seeds(self, urls):

        added = 0

        for url in urls:

            if self.add_seed(url):
                added += 1

        return added

    def _process_result(self, result):

        if result is None:
            return

        response = result.response

        url = response.get(
            "requested_url",
            result.task.url
        )

        status = response.get(
            "status",
            0
        )

        body = response.get(
            "body",
            b""
        )

        content_type = response.get(
            "content_type",
            ""
        )

        if 200 <= status < 300:

            self.stats[
                "pages_completed"
            ] += 1

            change = self.change_tracker.check(
                url,
                body=body,
                status=status
            )

            state = change["state"]

            if state == "NEW":

                self.stats[
                    "new_pages"
                ] += 1

            elif state == "CHANGED":

                self.stats[
                    "changed_pages"
                ] += 1

            elif state == "UNCHANGED":

                self.stats[
                    "unchanged_pages"
                ] += 1

            dedup = self.content_dedup.inspect(
                body,
                content_type
            )

            exact_duplicate = (
                dedup["exact_duplicate_of"]
            )

            possible_duplicate = (
                dedup["possible_duplicate_of"]
            )

            if exact_duplicate:

                self.stats[
                    "exact_duplicates"
                ] += 1

            else:

                self.stats[
                    "unique_contents"
                ] += 1

                if possible_duplicate:

                    self.stats[
                        "possible_duplicates"
                    ] += 1

                try:

                    self.storage.save_page(
                        result.task.document_id,
                        body,
                        {
                            "url": url,
                            "final_url":
                                response.get(
                                    "final_url"
                                ),
                            "status": status,
                            "content_type":
                                content_type,
                            "change_state":
                                state,
                            "raw_hash":
                                dedup["raw_hash"],
                            "text_hash":
                                dedup["text_hash"],
                            "redirect_chain":
                                response.get(
                                    "redirect_chain",
                                    []
                                )
                        }
                    )

                    self.content_dedup.register(
                        result.task.document_id,
                        body,
                        content_type
                    )

                except Exception:

                    self.stats[
                        "storage_errors"
                    ] += 1

            self.change_tracker.register(
                url,
                body,
                status=status,
                etag=response.get(
                    "headers",
                    {}
                ).get(
                    "ETag"
                ),
                last_modified=response.get(
                    "headers",
                    {}
                ).get(
                    "Last-Modified"
                ),
                final_url=response.get(
                    "final_url"
                )
            )

            integration = getattr(
                self,
                "index_integration",
                None
            )

            if integration is not None:
                integration.process_success(
                    result,
                    state=state,
                    content_type=content_type,
                    exact_duplicate=bool(
                        exact_duplicate
                    )
                )

            if "html" in content_type.lower():

                discovered = self.discovery.discover(
                    response.get(
                        "final_url",
                        url
                    ),
                    body
                )

                for discovered_url in discovered:

                    self._add_url(
                        discovered_url,
                        source="link"
                    )

        elif status in (404, 410):

            self.stats[
                "gone_pages"
            ] += 1

            self.change_tracker.check(
                url,
                body=None,
                status=status
            )

            self.change_tracker.mark_gone(
                url
            )

            integration = getattr(
                self,
                "index_integration",
                None
            )

            if integration is not None:
                integration.process_deleted(
                    result.task.document_id
                )

        else:

            self.stats[
                "pages_failed"
            ] += 1

    def _discover_sitemaps(self):

        domains = set()

        for seed in self.seeds:

            parsed = urlparse(seed)

            if parsed.netloc:

                domains.add(
                    parsed.netloc.lower()
                )

        for domain in domains:

            try:

                urls = self.sitemap.discover(
                    "https://" + domain
                )

            except Exception:

                continue

            self.stats[
                "sitemap_urls"
            ] += len(urls)

            for url in urls:

                if self._add_url(
                    url,
                    source="sitemap"
                ):

                    self.stats[
                        "sitemap_added"
                    ] += 1

    def run(
        self,
        max_cycles=None
    ):

        if self.running:
            return self.status()

        self.running = True

        self.coordinator.start()

        self._discover_sitemaps()

        cycles = 0

        while self.running:

            cycles += 1

            if (
                max_cycles is not None
                and cycles > max_cycles
            ):

                break

            self.coordinator.monitor()

            self.coordinator.dispatch()

            results = self.coordinator.collect(
                timeout=0.5
            )

            for result in results:

                self._process_result(
                    result
                )

            if (
                self.frontier.size() == 0
                and
                not self.coordinator.in_flight
            ):

                break

        return self.status()

    def continuous_loop(
        self,
        interval=5
    ):

        if self.running:
            return

        self.running = True

        self.coordinator.start()

        self._discover_sitemaps()

        while self.running:

            self.coordinator.monitor()

            self.coordinator.dispatch()

            results = self.coordinator.collect(
                timeout=0.5
            )

            for result in results:

                self._process_result(
                    result
                )

            time.sleep(
                max(
                    0.01,
                    float(interval)
                )
            )

    def stop(self):

        self.running = False

        self.coordinator.stop()

        integration = getattr(
            self,
            "index_integration",
            None
        )

        if integration is not None:
            integration.close()

        self.storage.close()

    def status(self):

        return {
            "running":
                self.running,

            "frontier":
                self.frontier.size(),

            "workers":
                self.coordinator.pool.active_workers(),

            "in_flight":
                len(
                    self.coordinator.in_flight
                ),

            "coordinator":
                self.coordinator.status(),

            "stats":
                dict(self.stats),

            "storage_pages":
                self.storage.count_pages()
        }
