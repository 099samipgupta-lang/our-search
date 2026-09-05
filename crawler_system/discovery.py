from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urldefrag


class LinkExtractor(HTMLParser):

    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.links = set()

    def handle_starttag(self, tag, attrs):

        if tag.lower() != "a":
            return

        href = None

        for name, value in attrs:

            if name.lower() == "href" and value:
                href = value
                break

        if not href:
            return

        absolute_url = urljoin(
            self.base_url,
            href
        )

        absolute_url, _ = urldefrag(
            absolute_url
        )

        parsed = urlparse(
            absolute_url
        )

        if parsed.scheme not in (
            "http",
            "https"
        ):
            return

        if not parsed.netloc:
            return

        self.links.add(
            absolute_url
        )


class WebDiscovery:

    def discover(self, url, body):

        try:

            html = body.decode(
                "utf-8",
                errors="ignore"
            )

            extractor = LinkExtractor(
                url
            )

            extractor.feed(html)

            return extractor.links

        except Exception:

            return set()
