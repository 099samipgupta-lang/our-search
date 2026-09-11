import time
from urllib.parse import urlparse

from crawler_system.coordinator import WorkerCoordinator
from crawler_system.discovery import WebDiscovery
from crawler_system.discovery_control import DiscoveryControl
from crawler_system.priority import CrawlPriority
from crawler_system.url_normalizer import URLNormalizer
from crawler_system.dedup import URLDeduplicator
from crawler_system.content_dedup import ContentDeduplicator
from crawler_system.change_tracker import ChangeTracker
from crawler_system.sitemap import SitemapDiscovery
from crawler_system.storage import CrawlStorage
from crawler_system.frontier import CrawlFrontier
from crawler_system.state_storage import CrawlerStateStorage
from crawler_system.url_state import URLStateStore

from indexing_pipeline.crawler_bridge import CrawlerIndexBridge
from indexing_pipeline.remote_bridge import RemoteCrawlerIndexBridge
from indexing_pipeline.automatic import AutomaticCrawlerIndexer


class WholeWebCrawler:

    def __init__(
        self,
        worker_count=4,
        frontier_delay=2,
        task_timeout=60,
        max_attempts=3,
        storage_root="crawler_storage",
        index_url=None
    ):

        self.normalizer = URLNormalizer()

        # ---------------------------------------------------------
        # Durable SQLite URL state
        # ---------------------------------------------------------

        url_state_path = (
            f"{storage_root.rstrip('/')}/url_state.db"
        )

        self.url_state = URLStateStore(
            database_path=url_state_path
        )

        self.url_dedup = URLDeduplicator(
            normalizer=self.normalizer,
            state_store=self.url_state
        )

        self.content_dedup = ContentDeduplicator()

        self.change_tracker = ChangeTracker()

        self.priority = CrawlPriority()

        self.discovery = WebDiscovery()

        # ---------------------------------------------------------
        # Discovery Control
        #
        # Raw discovered URLs pass through this layer before
        # entering the durable frontier.
        # ---------------------------------------------------------

        self.discovery_control = DiscoveryControl(
            normalizer=self.normalizer
        )

        self.sitemap = SitemapDiscovery()

        self.storage = CrawlStorage(
            root=storage_root
        )

        # ---------------------------------------------------------
        # Existing legacy crawler state
        #
        # Kept for content/change compatibility while SQLite
        # remains the durable URL-state foundation.
        # ---------------------------------------------------------

        self.state_storage = CrawlerStateStorage(
            storage_root
        )

        crawler_state = self.state_storage.load()

        self.url_dedup.load_state(
            crawler_state.get(
                "url_dedup",
                []
            )
        )

        self.content_dedup.load_state(
            crawler_state.get(
                "content_dedup",
                {}
            )
        )

        self.change_tracker.load_state(
            crawler_state.get(
                "change_tracker",
                {}
            )
        )

        # ---------------------------------------------------------
        # Frontier
        # ---------------------------------------------------------

        frontier_storage_path = (
            f"{storage_root.rstrip('/')}/frontier/state.json"
        )

        self.frontier = CrawlFrontier(
            default_delay=frontier_delay,
            max_retries=max_attempts,
            storage_path=frontier_storage_path,
            state_store=self.url_state
        )

        # ---------------------------------------------------------
        # Worker coordinator
        # ---------------------------------------------------------

        self.coordinator = WorkerCoordinator(
            self.frontier,
            worker_count=worker_count,
            task_timeout=task_timeout,
            max_attempts=max_attempts
        )

        # ---------------------------------------------------------
        # Index integration
        # ---------------------------------------------------------

        if index_url is None:
            index_bridge = CrawlerIndexBridge()
        else:
            index_bridge = RemoteCrawlerIndexBridge(
                index_url
            )

        self.index_integration = AutomaticCrawlerIndexer(
            index_bridge
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
            "storage_errors": 0,
            "discovery_rejected": 0
        }

    # =============================================================
    # DUE URL REACTIVATION
    # =============================================================

    def reactivate_due_urls(self, limit=100):
        """
        Reactivate durable crawled URLs whose recrawl time has arrived.

        Reactivation happens directly through SQLite because these
        URLs are already known. They are not new discoveries and
        therefore must not pass through normal frontier.add().
        """

        due_urls = self.url_state.list_due_urls(
            now=time.time(),
            limit=limit
        )

        reactivated = 0

        for row in due_urls:

            if self.url_state.reactivate_due(
                row["url"],
                now=time.time()
            ):
                reactivated += 1

        return reactivated

    # =============================================================
    # URL / DISCOVERY
    # =============================================================

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

        self.stats["discovered"] += 1

        # ---------------------------------------------------------
        # Discovery Control
        #
        # Raw URLs must pass the control layer before entering the
        # durable frontier.
        # ---------------------------------------------------------

        decision = self.discovery_control.evaluate(
            url,
            source=source,
            depth=depth,
            seed=seed
        )

        if not decision.accepted:

            self.stats[
                "discovery_rejected"
            ] += 1

            return False

        normalized = decision.url

        # ---------------------------------------------------------
        # SQLite-backed URL deduplication
        # ---------------------------------------------------------

        if not self.url_dedup.is_new(
            normalized
        ):

            self.stats["duplicates"] += 1

            return False

        # ---------------------------------------------------------
        # Existing centralized crawl priority
        #
        # CrawlPriority remains responsible for the base priority.
        # Discovery intelligence then adjusts that priority so
        # crawl-space URLs are explored later without completely
        # discarding legitimate URLs.
        # ---------------------------------------------------------

        priority = self.priority.score(
            source=source,
            depth=depth,
            seed=seed
        )

        # ---------------------------------------------------------
        # Discovery intelligence priority adjustment
        #
        # Seeds keep their full priority.
        #
        # For discovered URLs:
        #
        # crawl_now   -> normal intelligence-weighted priority
        # crawl_later -> additional reduction
        # low_priority -> stronger reduction
        #
        # The minimum multiplier prevents legitimate URLs from
        # becoming effectively impossible to crawl.
        # ---------------------------------------------------------

        if not seed:

            crawl_score = float(
                getattr(
                    decision,
                    "crawl_score",
                    100.0
                )
            )

            intelligence_multiplier = max(
                0.25,
                min(
                    crawl_score / 100.0,
                    1.0
                )
            )

            priority *= intelligence_multiplier

            crawl_action = getattr(
                decision,
                "crawl_action",
                "crawl_now"
            )

            if crawl_action == "crawl_later":

                priority *= 0.80

            elif crawl_action == "low_priority":

                priority *= 0.50

        priority = max(
            1.0,
            float(priority)
        )

        # ---------------------------------------------------------
        # Add to durable frontier
        # ---------------------------------------------------------

        added = self.frontier.add(
            normalized,
            priority=priority
        )

        if added:

            # Move SQLite state from discovered -> queued.
            self.url_state.mark_queued(
                normalized
            )

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

    # =============================================================
    # ADAPTIVE RECrawl SCHEDULING
    # =============================================================

    def _next_crawl_at(
        self,
        status,
        change_state
    ):
        """
        Calculate the next crawl time.

        The crawler does not treat a URL as permanently finished.
        """

        now = time.time()

        if 200 <= status < 300:

            if change_state == "NEW":
                interval = 24 * 60 * 60

            elif change_state == "CHANGED":
                interval = 6 * 60 * 60

            elif change_state == "UNCHANGED":
                interval = 7 * 24 * 60 * 60

            elif change_state == "GONE":
                interval = 30 * 24 * 60 * 60

            else:
                interval = 24 * 60 * 60

            return now + interval

        if status == 304:

            return now + (
                7 * 24 * 60 * 60
            )

        if status == 410:

            return now + (
                30 * 24 * 60 * 60
            )

        if status == 404:

            return now + (
                7 * 24 * 60 * 60
            )

        if status in (
            408,
            425,
            429,
            500,
            502,
            503,
            504
        ):

            return now + (
                60 * 60
            )

        return now + (
            24 * 60 * 60
        )

    # =============================================================
    # RESULT PROCESSING
    # =============================================================

    def _process_result(self, result):

        if result is None:
            return

        response = result.response

        url = result.task.url

        status = response.get(
            "status",
            0
        )

        content_type = response.get(
            "content_type",
            ""
        )

        body = response.get(
            "body",
            b""
        )

        previous_etag = getattr(
            result.task,
            "etag",
            None
        )

        previous_last_modified = getattr(
            result.task,
            "last_modified",
            None
        )

        response_etag = response.get(
            "etag"
        )

        response_last_modified = response.get(
            "last_modified"
        )

        etag = (
            response_etag
            if response_etag is not None
            else previous_etag
        )

        last_modified = (
            response_last_modified
            if response_last_modified is not None
            else previous_last_modified
        )

        successful = (
            200 <= status < 300
            or status == 304
        )

        if not successful:

            self.stats[
                "pages_failed"
            ] += 1

        state_info = self.change_tracker.check(
            url,
            body,
            status=status
        )

        change_state = state_info.get(
            "state",
            "UNKNOWN"
        )

        # ---------------------------------------------------------
        # 304 Not Modified
        # ---------------------------------------------------------

        if status == 304:

            next_crawl_at = self._next_crawl_at(
                status,
                change_state
            )

            self.url_state.mark_crawled(
                url,
                status=status,
                next_crawl_at=next_crawl_at,
                etag=etag,
                last_modified=last_modified
            )

            self.stats[
                "unchanged_pages"
            ] += 1

            self.stats[
                "pages_completed"
            ] += 1

            return

        # ---------------------------------------------------------
        # Successful body processing
        # ---------------------------------------------------------

        if (
            200 <= status < 300
            and body
        ):

            self.change_tracker.register(
                url,
                body,
                status=status,
                etag=etag,
                last_modified=last_modified,
                final_url=response.get(
                    "final_url"
                )
            )

        # ---------------------------------------------------------
        # Content deduplication
        # ---------------------------------------------------------

        content_info = None

        exact_duplicate = False

        if (
            200 <= status < 300
            and body
        ):

            content_info = (
                self.content_dedup.inspect(
                    body,
                    content_type
                )
            )

            exact_duplicate = (
                content_info[
                    "exact_duplicate_of"
                ]
                is not None
            )

            if exact_duplicate:

                self.stats[
                    "exact_duplicates"
                ] += 1

            if (
                content_info[
                    "possible_duplicate_of"
                ] is not None
            ):

                self.stats[
                    "possible_duplicates"
                ] += 1

            self.content_dedup.register(
                result.task.document_id,
                body,
                content_type
            )

        # ---------------------------------------------------------
        # Change statistics
        # ---------------------------------------------------------

        if change_state == "NEW":

            self.stats[
                "new_pages"
            ] += 1

        elif change_state == "CHANGED":

            self.stats[
                "changed_pages"
            ] += 1

        elif change_state == "UNCHANGED":

            self.stats[
                "unchanged_pages"
            ] += 1

        # ---------------------------------------------------------
        # Index integration
        # ---------------------------------------------------------

        integration = getattr(
            self,
            "index_integration",
            None
        )

        if (
            integration is not None
            and 200 <= status < 300
            and body
        ):

            integration.process_success(
                result,
                state=change_state,
                content_type=content_type,
                exact_duplicate=bool(
                    exact_duplicate
                )
            )

        # ---------------------------------------------------------
        # Successful crawl -> durable SQLite state
        # ---------------------------------------------------------

        if successful:

            next_crawl_at = self._next_crawl_at(
                status,
                change_state
            )

            self.url_state.mark_crawled(
                url,
                status=status,
                next_crawl_at=next_crawl_at,
                etag=etag,
                last_modified=last_modified
            )

        # ---------------------------------------------------------
        # HTML discovery
        # ---------------------------------------------------------

        if (
            200 <= status < 300
            and body
            and "html" in content_type.lower()
        ):

            discovered = (
                self.discovery.discover(
                    response.get(
                        "final_url",
                        url
                    ),
                    body
                )
            )

            for discovered_url in discovered:

                self._add_url(
                    discovered_url,
                    source="link"
                )

        # ---------------------------------------------------------
        # Deleted pages
        # ---------------------------------------------------------

        elif status in (
            404,
            410
        ):

            self.stats[
                "gone_pages"
            ] += 1

            if integration is not None:

                integration.process_deleted(
                    result.task.document_id
                )

            self.url_state.mark_crawled(
                url,
                status=status,
                next_crawl_at=self._next_crawl_at(
                    status,
                    "GONE"
                ),
                etag=etag,
                last_modified=last_modified
            )

        # ---------------------------------------------------------
        # Completed successful crawl
        # ---------------------------------------------------------

        if successful:

            self.stats[
                "pages_completed"
            ] += 1

    # =============================================================
    # SITEMAPS
    # =============================================================

    def _discover_sitemaps(self):

        domains = set()

        for seed in self.seeds:

            parsed = urlparse(
                seed
            )

            if parsed.netloc:

                domains.add(
                    parsed.netloc
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

    # =============================================================
    # MAIN RUN
    # =============================================================

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

            self.reactivate_due_urls(
                limit=100
            )

            self.coordinator.dispatch()

            results = (
                self.coordinator.collect(
                    timeout=0.5
                )
            )

            for result in results:

                self._process_result(
                    result
                )

            if (
                self.frontier.size() == 0
                and not self.coordinator.in_flight
            ):

                break

        integration = getattr(
            self,
            "index_integration",
            None
        )

        if integration is not None:

            integration.flush()

        self.state_storage.save(
            self.url_dedup,
            self.content_dedup,
            self.change_tracker
        )

        self.coordinator.stop()

        self.running = False

        return self.status()

    # =============================================================
    # CONTINUOUS MODE
    # =============================================================

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

            self.reactivate_due_urls(
                limit=100
            )

            self.coordinator.dispatch()

            results = (
                self.coordinator.collect(
                    timeout=0.5
                )
            )

            for result in results:

                self._process_result(
                    result
                )

            time.sleep(
                max(
                    0.1,
                    float(interval)
                )
            )

    # =============================================================
    # STOP
    # =============================================================

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

        self.url_state.close()

    # =============================================================
    # STATUS
    # =============================================================

    def status(self):

        integration = getattr(
            self,
            "index_integration",
            None
        )

        return {
            "running": self.running,

            "frontier": self.frontier.size(),

            "workers": (
                self.coordinator.worker_count
                if hasattr(
                    self.coordinator,
                    "worker_count"
                )
                else 0
            ),

            "in_flight": len(
                self.coordinator.in_flight
            ),

            "coordinator": (
                self.coordinator.status()
            ),

            "stats": dict(
                self.stats
            ),

            "url_state": (
                self.url_state.counts()
            ),

            "indexing": (
                integration.status()
                if integration is not None
                else {}
            )
        }
