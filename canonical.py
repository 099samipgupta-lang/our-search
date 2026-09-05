from html.parser import HTMLParser
from urllib.parse import urljoin


class CanonicalExtractor(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.canonical_url = None

    def handle_starttag(self, tag, attrs):
        if tag != "link":
            return

        rel = ""
        href = ""

        for name, value in attrs:
            if name == "rel" and value:
                rel = value.lower()

            if name == "href" and value:
                href = value

        if rel == "canonical" and href:
            self.canonical_url = urljoin(
                self.base_url,
                href
            )
