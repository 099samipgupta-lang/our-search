import time
from urllib.parse import urlparse

from crawler_system.frontier import CrawlFrontier
from crawler_system.fetcher import Fetcher
from crawler_system.policy import CrawlPolicy
from crawler_system.storage import PageStorage
from crawler_system.dedup import URLDeduplicator
from crawler_system.scheduler import CrawlScheduler
from crawler_system.worker import CrawlWorker
from crawler_system.state import CrawlState
from crawler_system.discovery import WebDiscovery
from crawler_system.sitemap import SitemapDiscovery
from crawler_system.content_dedup import ContentDeduplicator
from crawler_system.change_tracker import ChangeTracker
from crawler_system.priority import CrawlPriority


class CrawlerCore:

    def __init__(
        self,
        frontier=None,
        fetcher=None,
        policy=None,
        storage=None,
        dedup=None,
        scheduler=None,
        worker=None,
        state=None,
        discovery=None,
        sitemap_discovery=None,
        content_dedup=None,
        change_tracker=None
    ):

        self.frontier = (
            frontier
            if frontier is not None
            else CrawlFrontier()
        )

        self.fetcher = (
            fetcher
            if fetcher is not None
            else Fetcher()
        )

        self.policy = (
            policy
            if policy is not None
            else CrawlPolicy()
        )

        self.storage = (
            storage
            if storage is not None
            else PageStorage()
        )

        self.dedup = (
            dedup
            if dedup is not None
            else URLDeduplicator()
        )

        self.scheduler = (
            scheduler
            if scheduler is not None
            else CrawlScheduler()
        )

        self.worker = (
            worker
            if worker is not None
            else CrawlWorker(
                self.fetcher,
                self.policy
            )
        )

        self.state = (
            state
            if state is not None
            else CrawlState()
        )

        self.discovery = (
            discovery
            if discovery is not None
            else WebDiscovery()
        )

        self.sitemap_discovery = (
            sitemap_discovery
            if sitemap_discovery is not None
            else SitemapDiscovery()
        )

        self.content_dedup = (
            content_dedup
            if content_dedup is not None
            else ContentDeduplicator()
        )

        self.priority = CrawlPriority()

        self.change_tracker = (
            change_tracker
            if change_tracker is not None
            else ChangeTracker()
        )

        self.sitemap_domains = set()

        self.running = False

        self.pages_crawled = 0
        self.unique_contents = 0
        self.exact_duplicates = 0
        self.possible_duplicates = 0

        self.new_pages = 0
        self.unchanged_pages = 0
        self.changed_pages = 0
        self.gone_pages = 0

        self._restore_state()

    def _restore_state(self):

        saved = self.state.load()

        if not saved:
            return

        self.pages_crawled = int(
            saved.get(
                "pages_crawled",
                0
            )
        )

        self.unique_contents = int(
            saved.get(
                "unique_contents",
                0
            )
        )

        self.exact_duplicates = int(
            saved.get(
                "exact_duplicates",
                0
            )
        )

        self.possible_duplicates = int(
            saved.get(
                "possible_duplicates",
                0
            )
        )

        self.new_pages = int(
            saved.get(
                "new_pages",
                0
            )
        )

        self.unchanged_pages = int(
            saved.get(
                "unchanged_pages",
                0
            )
        )

        self.changed_pages = int(
            saved.get(
                "changed_pages",
                0
            )
        )

        self.gone_pages = int(
            saved.get(
                "gone_pages",
                0
            )
        )

        frontier_state = saved.get(
            "frontier"
        )

        if frontier_state:
            self.frontier.load_state(
                frontier_state
            )

        sitemap_domains = saved.get(
            "sitemap_domains",
            []
        )

        self.sitemap_domains = set(
            sitemap_domains
        )

        content_state = saved.get(
            "content_dedup"
        )

        if content_state:
            self.content_dedup.load_state(
                content_state
            )

        change_state = saved.get(
            "change_tracker"
        )

        if change_state:
            self.change_tracker.load_state(
                change_state
            )

        print(
            "Crawler state restored."
        )

        print(
            "Restored pages:",
            self.pages_crawled
        )

        print(
            "Restored frontier:",
            self.frontier.size()
        )

        print(
            "Restored content identities:",
            len(
                self.content_dedup.raw_hashes
            )
        )

        print(
            "Restored tracked pages:",
            len(
                self.change_tracker.pages
            )
        )

    def add_url(self, url):

        normalized_url = (
            self.dedup.normalize(url)
        )

        if normalized_url is None:
            return False

        if not self.policy.is_valid_url(
            normalized_url
        ):
            return False

        if not self.dedup.is_new(
            normalized_url
        ):
            return False

        priority = self.priority.score(
            source="discovery"
        )

        self.frontier.add(
            normalized_url,
            priority=priority
        )

        return True

    def discover_sitemaps(self, url):

        parsed = urlparse(url)

        domain = parsed.netloc.lower()

        if domain in self.sitemap_domains:
            return 0

        self.sitemap_domains.add(
            domain
        )

        print(
            "Discovering sitemaps:",
            domain
        )

        sitemap_urls = (
            self.sitemap_discovery.discover(
                url
            )
        )

        new_urls = 0

        for sitemap_url in sitemap_urls:

            if self.add_url(sitemap_url):
                new_urls += 1

        print(
            "Sitemap URLs found:",
            len(sitemap_urls)
        )

        print(
            "New sitemap URLs added:",
            new_urls
        )

        return new_urls

    def save_state(self):

        self.state.save(
            {
                "pages_crawled":
                    self.pages_crawled,

                "unique_contents":
                    self.unique_contents,

                "exact_duplicates":
                    self.exact_duplicates,

                "possible_duplicates":
                    self.possible_duplicates,

                "new_pages":
                    self.new_pages,

                "unchanged_pages":
                    self.unchanged_pages,

                "changed_pages":
                    self.changed_pages,

                "gone_pages":
                    self.gone_pages,

                "frontier":
                    self.frontier.get_state(),

                "url_dedup":
                    self.dedup.get_state(),

                "sitemap_domains":
                    sorted(
                        self.sitemap_domains
                    ),

                "content_dedup":
                    self.content_dedup.get_state(),

                "change_tracker":
                    self.change_tracker.get_state()
            }
        )

    def crawl_one(self):

        url = self.frontier.get_next()

        if url is None:
            return False

        parsed = urlparse(url)

        domain = parsed.netloc

        if not self.scheduler.is_ready(
            domain
        ):

            self.frontier.add(
                url,
                priority=25
            )

            time.sleep(0.1)

            return True

        print(
            "Crawling:",
            url
        )

        previous = (
            self.change_tracker.pages.get(
                url
            )
        )

        document_id = (
            "DOC-"
            + str(
                self.pages_crawled + 1
            ).zfill(6)
        )

        try:

            result = self.worker.crawl(
                url,
                document_id,
                previous
            )

            status = result.get(
                "status",
                0
            )

            status_type = result.get(
                "status_type",
                "unknown"
            )

            print(
                "HTTP:",
                status,
                status_type
            )

            self.scheduler.mark_crawled(
                domain
            )

            self.frontier.mark_crawled(
                url
            )

            final_url = (
                self.dedup.normalize(
                    result.get(
                        "final_url",
                        url
                    )
                )
            )

            headers = result.get(
                "headers",
                {}
            )

            # 404 / 410
            if status in (404, 410):

                change = (
                    self.change_tracker.check(
                        url,
                        status=status
                    )
                )

                self.gone_pages += 1

                print(
                    "Page state:",
                    change["state"]
                )

                self.change_tracker.mark_gone(
                    url
                )

                if status == 404:

                    print(
                        "Page not found:",
                        url
                    )

                else:

                    print(
                        "Page gone:",
                        url
                    )

            # HTTP 304
            elif status == 304:

                change = (
                    self.change_tracker.check(
                        url,
                        status=304
                    )
                )

                self.pages_crawled += 1
                self.unchanged_pages += 1

                print(
                    "Page state:",
                    change["state"]
                )

            # Successful fetch
            elif 200 <= status < 300:

                self.pages_crawled += 1

                body = result.get(
                    "body",
                    b""
                )

                change = (
                    self.change_tracker.check(
                        url,
                        body=body,
                        status=status
                    )
                )

                page_state = change["state"]

                print(
                    "Page state:",
                    page_state
                )

                if page_state == "NEW":

                    self.new_pages += 1

                elif page_state == "UNCHANGED":

                    self.unchanged_pages += 1

                elif page_state == "CHANGED":

                    self.changed_pages += 1

                # Content deduplication
                fingerprint = (
                    self.content_dedup.inspect(
                        body,
                        result.get(
                            "content_type",
                            ""
                        )
                    )
                )

                exact_duplicate_of = (
                    fingerprint[
                        "exact_duplicate_of"
                    ]
                )

                possible_duplicate_of = (
                    fingerprint[
                        "possible_duplicate_of"
                    ]
                )

                print(
                    "Content hash:",
                    fingerprint[
                        "raw_hash"
                    ][:16]
                    + "..."
                )

                if exact_duplicate_of:

                    self.exact_duplicates += 1

                    print(
                        "Content status:",
                        "EXACT DUPLICATE"
                    )

                    print(
                        "Duplicate of:",
                        exact_duplicate_of
                    )

                else:

                    self.storage.save(
                        document_id,
                        body
                    )

                    self.content_dedup.register(
                        document_id,
                        body,
                        result.get(
                            "content_type",
                            ""
                        )
                    )

                    self.unique_contents += 1

                    print(
                        "Content status:",
                        "UNIQUE"
                    )

                    if possible_duplicate_of:

                        self.possible_duplicates += 1

                        print(
                            "Possible text duplicate of:",
                            possible_duplicate_of
                        )

                # Register new version
                self.change_tracker.register(
                    url,
                    body,
                    status=status,
                    etag=headers.get(
                        "ETag"
                    ),
                    last_modified=headers.get(
                        "Last-Modified"
                    ),
                    final_url=final_url
                )

                print(
                    "Final URL:",
                    final_url
                )

                discovered_links = (
                    self.discovery.discover(
                        final_url,
                        body
                    )
                )

                new_links = 0

                for link in discovered_links:

                    if self.add_url(link):
                        new_links += 1

                print(
                    "Discovered links:",
                    len(discovered_links)
                )

                print(
                    "New URLs added:",
                    new_links
                )

                self.discover_sitemaps(
                    final_url
                )

            elif status == 429:

                print(
                    "Rate limited:",
                    url
                )

                self.frontier.mark_failed(
                    url
                )

            elif 500 <= status < 600:

                print(
                    "Server error:",
                    status
                )

                self.frontier.mark_failed(
                    url
                )

            elif status >= 400:

                print(
                    "Client error:",
                    status
                )

            elif status_type in (
                "network_error",
                "fetch_error"
            ):

                print(
                    "Network/fetch failure"
                )

                self.frontier.mark_failed(
                    url
                )

            elif status_type == (
                "too_many_redirects"
            ):

                print(
                    "Too many redirects"
                )

                self.frontier.mark_failed(
                    url
                )

            elif status_type == (
                "redirect_loop"
            ):

                print(
                    "Redirect loop"
                )

                self.frontier.mark_failed(
                    url
                )

            self.save_state()

            return True

        except Exception as error:

            print(
                "Crawl error:",
                error
            )

            self.frontier.mark_failed(
                url
            )

            self.save_state()

            return True

    def start(self, max_pages=None):

        self.running = True

        while self.running:

            if (
                max_pages is not None
                and self.pages_crawled >= max_pages
            ):
                break

            if self.frontier.size() == 0:
                break

            progressed = self.crawl_one()

            if not progressed:
                break

        self.running = False

        print(
            "Pages crawled:",
            self.pages_crawled
        )

        print(
            "New pages:",
            self.new_pages
        )

        print(
            "Unchanged pages:",
            self.unchanged_pages
        )

        print(
            "Changed pages:",
            self.changed_pages
        )

        print(
            "Gone pages:",
            self.gone_pages
        )

        print(
            "Unique contents:",
            self.unique_contents
        )

        print(
            "Exact duplicates:",
            self.exact_duplicates
        )

        print(
            "Possible text duplicates:",
            self.possible_duplicates
        )

        print(
            "Frontier remaining:",
            self.frontier.size()
        )

    def stop(self):

        self.running = False
