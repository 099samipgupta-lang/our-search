from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET


class FeedDiscovery:

    def __init__(self, max_urls=50000):
        self.max_urls = max(1, int(max_urls))

    @staticmethod
    def _valid_url(url):
        if not isinstance(url, str):
            return False

        parsed = urlparse(url)

        return (
            parsed.scheme in ("http", "https")
            and bool(parsed.netloc)
        )

    @staticmethod
    def _local_name(tag):
        if not isinstance(tag, str):
            return ""

        return tag.rsplit("}", 1)[-1].lower()

    def discover(self, feed_url, body=None):

        if not body:
            return set()

        try:
            root = ET.fromstring(body)
        except Exception:
            return set()

        discovered = set()

        # ---------------------------------------------------------
        # RSS
        #
        # Common structure:
        # <item>
        #     <link>https://example.com/article</link>
        # </item>
        # ---------------------------------------------------------

        for element in root.iter():

            if self._local_name(element.tag) != "link":
                continue

            value = None

            # Atom commonly stores the URL in href.
            href = element.attrib.get("href")

            if href:
                value = href.strip()

            # RSS commonly stores the URL as element text.
            elif element.text:
                value = element.text.strip()

            if not value:
                continue

            absolute_url = urljoin(
                feed_url,
                value
            )

            if not self._valid_url(absolute_url):
                continue

            discovered.add(absolute_url)

            if len(discovered) >= self.max_urls:
                break

        return discovered
