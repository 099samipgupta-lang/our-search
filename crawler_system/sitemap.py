import gzip
from urllib.request import Request, urlopen
from urllib.parse import urlparse, urljoin
from xml.etree import ElementTree as ET


class SitemapDiscovery:

    def __init__(
        self,
        user_agent="OurSearchBot/1.0",
        timeout=10,
        max_sitemaps=100,
        max_urls=50000,
        max_bytes=20 * 1024 * 1024
    ):
        self.user_agent = user_agent
        self.timeout = timeout
        self.max_sitemaps = max_sitemaps
        self.max_urls = max_urls
        self.max_bytes = max_bytes

    def _fetch(self, url):

        request = Request(
            url,
            headers={
                "User-Agent": self.user_agent
            }
        )

        with urlopen(
            request,
            timeout=self.timeout
        ) as response:

            body = response.read(
                self.max_bytes + 1
            )

            if len(body) > self.max_bytes:
                raise ValueError(
                    "Sitemap exceeds size limit"
                )

            content_type = (
                response.headers.get(
                    "Content-Type",
                    ""
                ).lower()
            )

            return body, content_type

    def _robots_sitemaps(self, page_url):

        parsed = urlparse(page_url)

        robots_url = (
            parsed.scheme
            + "://"
            + parsed.netloc
            + "/robots.txt"
        )

        try:

            body, _ = self._fetch(
                robots_url
            )

        except Exception:

            return []

        text = body.decode(
            "utf-8",
            errors="ignore"
        )

        sitemaps = []

        for line in text.splitlines():

            line = line.strip()

            if not line:
                continue

            if ":" not in line:
                continue

            name, value = line.split(
                ":",
                1
            )

            if name.strip().lower() != "sitemap":
                continue

            value = value.strip()

            if not value:
                continue

            sitemap_url = urljoin(
                robots_url,
                value
            )

            parsed_sitemap = urlparse(
                sitemap_url
            )

            if parsed_sitemap.scheme not in (
                "http",
                "https"
            ):
                continue

            if not parsed_sitemap.netloc:
                continue

            sitemaps.append(
                sitemap_url
            )

        return sitemaps

    def _parse_sitemap(self, body, base_url):

        try:

            root = ET.fromstring(
                body
            )

        except Exception:

            return [], []

        root_name = (
            root.tag.rsplit(
                "}",
                1
            )[-1].lower()
        )

        child_sitemaps = []
        urls = []

        if root_name == "sitemapindex":

            for element in root.iter():

                name = (
                    element.tag.rsplit(
                        "}",
                        1
                    )[-1].lower()
                )

                if name != "loc":
                    continue

                if not element.text:
                    continue

                value = element.text.strip()

                if not value:
                    continue

                child_url = urljoin(
                    base_url,
                    value
                )

                parsed = urlparse(
                    child_url
                )

                if parsed.scheme in (
                    "http",
                    "https"
                ) and parsed.netloc:

                    child_sitemaps.append(
                        child_url
                    )

        elif root_name == "urlset":

            for element in root.iter():

                name = (
                    element.tag.rsplit(
                        "}",
                        1
                    )[-1].lower()
                )

                if name != "loc":
                    continue

                if not element.text:
                    continue

                value = element.text.strip()

                if not value:
                    continue

                page_url = urljoin(
                    base_url,
                    value
                )

                parsed = urlparse(
                    page_url
                )

                if parsed.scheme in (
                    "http",
                    "https"
                ) and parsed.netloc:

                    urls.append(
                        page_url
                    )

        return child_sitemaps, urls

    def _decode_sitemap(
        self,
        body,
        url,
        content_type
    ):

        is_gzip = (
            url.lower().endswith(".gz")
            or "gzip" in content_type
        )

        if is_gzip:

            try:
                body = gzip.decompress(
                    body
                )
            except Exception:
                return None

        return body

    def discover(self, page_url):

        sitemap_queue = []

        discovered_sitemaps = set()

        discovered_urls = set()

        robots_sitemaps = (
            self._robots_sitemaps(
                page_url
            )
        )

        for sitemap_url in robots_sitemaps:
            sitemap_queue.append(
                sitemap_url
            )

        if not sitemap_queue:

            parsed = urlparse(
                page_url
            )

            fallback = (
                parsed.scheme
                + "://"
                + parsed.netloc
                + "/sitemap.xml"
            )

            sitemap_queue.append(
                fallback
            )

        while (
            sitemap_queue
            and len(discovered_sitemaps)
            < self.max_sitemaps
            and len(discovered_urls)
            < self.max_urls
        ):

            sitemap_url = (
                sitemap_queue.pop(0)
            )

            if sitemap_url in discovered_sitemaps:
                continue

            discovered_sitemaps.add(
                sitemap_url
            )

            try:

                body, content_type = (
                    self._fetch(
                        sitemap_url
                    )
                )

                body = self._decode_sitemap(
                    body,
                    sitemap_url,
                    content_type
                )

                if body is None:
                    continue

                child_sitemaps, urls = (
                    self._parse_sitemap(
                        body,
                        sitemap_url
                    )
                )

            except Exception:

                continue

            for child in child_sitemaps:

                if child not in discovered_sitemaps:
                    sitemap_queue.append(
                        child
                    )

            for url in urls:

                if len(discovered_urls) >= self.max_urls:
                    break

                discovered_urls.add(
                    url
                )

        return discovered_urls
