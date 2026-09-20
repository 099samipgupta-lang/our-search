from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse


class FeedEndpointExtractor(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.feeds = set()

    @staticmethod
    def _valid_url(url):
        if not isinstance(url, str):
            return False

        parsed = urlparse(url)

        return (
            parsed.scheme in ("http", "https")
            and bool(parsed.netloc)
        )

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "link":
            return

        attributes = {
            str(name).lower(): value
            for name, value in attrs
        }

        rel = attributes.get("rel")
        href = attributes.get("href")
        feed_type = attributes.get("type")

        if not href:
            return

        if isinstance(rel, str):
            rel_values = {
                value.strip().lower()
                for value in rel.split()
            }
        else:
            rel_values = set()

        feed_types = {
            "application/rss+xml",
            "application/atom+xml",
            "application/feed+xml",
        }

        is_feed = (
            "alternate" in rel_values
            and isinstance(feed_type, str)
            and feed_type.lower().strip() in feed_types
        )

        if not is_feed:
            return

        absolute_url = urljoin(self.base_url, href)

        if self._valid_url(absolute_url):
            self.feeds.add(absolute_url)


class FeedEndpointDiscovery:
    def discover(self, page_url, body):
        if not body:
            return set()

        try:
            html = body.decode("utf-8", errors="ignore")

            extractor = FeedEndpointExtractor(page_url)
            extractor.feed(html)

            return extractor.feeds

        except Exception:
            return set()
