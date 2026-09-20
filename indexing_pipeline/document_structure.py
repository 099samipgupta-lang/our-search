from html.parser import HTMLParser


class DocumentStructureParser(HTMLParser):
    """
    Extract structured document regions from HTML.

    The first version deliberately keeps extraction deterministic and
    lightweight. It separates identity/content signals from common
    boilerplate regions without changing the existing search index yet.
    """

    BOILERPLATE_TAGS = {
        "nav",
        "footer",
        "aside",
    }

    CONTENT_TAGS = {
        "article",
        "main",
    }

    HEADING_TAGS = {
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
    }

    SKIP_TAGS = {
        "script",
        "style",
        "noscript",
        "template",
        "svg",
    }

    def __init__(self):
        super().__init__()

        self.title_parts = []
        self.headings = []
        self.main_content = []
        self.navigation_text = []
        self.footer_text = []
        self.sidebar_text = []
        self.anchor_text = []

        self._stack = []
        self._skip_depth = 0
        self._current_heading = None
        self._current_anchor = None

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        self._stack.append(tag)

        if tag in self.SKIP_TAGS:
            self._skip_depth += 1
            return

        if tag == "title":
            self._current_heading = None

        if tag in self.HEADING_TAGS:
            self._current_heading = []

        if tag == "a":
            self._current_anchor = []

    def handle_endtag(self, tag):
        tag = tag.lower()

        if self._skip_depth:
            if tag in self.SKIP_TAGS:
                self._skip_depth -= 1
            if self._stack:
                self._stack.pop()
            return

        if tag == "title":
            pass

        if tag in self.HEADING_TAGS and self._current_heading is not None:
            value = self._clean(" ".join(self._current_heading))
            if value:
                self.headings.append(value)
            self._current_heading = None

        if tag == "a" and self._current_anchor is not None:
            value = self._clean(" ".join(self._current_anchor))
            if value:
                self.anchor_text.append(value)
            self._current_anchor = None

        if self._stack:
            self._stack.pop()

    def handle_data(self, data):
        if self._skip_depth:
            return

        value = self._clean(data)

        if not value:
            return

        current = self._stack[-1] if self._stack else ""

        if current == "title":
            self.title_parts.append(value)

        if self._current_heading is not None:
            self._current_heading.append(value)

        if self._current_anchor is not None:
            self._current_anchor.append(value)

        region = self._region()

        if region == "navigation":
            self.navigation_text.append(value)
        elif region == "footer":
            self.footer_text.append(value)
        elif region == "sidebar":
            self.sidebar_text.append(value)
        elif region == "main":
            self.main_content.append(value)

    def _region(self):
        stack = set(self._stack)

        if "nav" in stack:
            return "navigation"

        if "footer" in stack:
            return "footer"

        if "aside" in stack:
            return "sidebar"

        if "article" in stack or "main" in stack:
            return "main"

        return None

    @staticmethod
    def _clean(value):
        return " ".join(
            value.split()
        )

    @staticmethod
    def _join(values):
        return " ".join(
            value
            for value in values
            if isinstance(value, str) and value
        )

    def result(self):
        return {
            "title": self._join(self.title_parts),
            "headings": self._join(self.headings),
            "main_content": self._join(self.main_content),
            "navigation_text": self._join(self.navigation_text),
            "footer_text": self._join(self.footer_text),
            "sidebar_text": self._join(self.sidebar_text),
            "anchor_text": self._join(self.anchor_text),
        }


def analyze_document_structure(html):
    """
    Parse HTML and return a deterministic structured representation.
    """
    parser = DocumentStructureParser()
    parser.feed(html or "")
    parser.close()
    return parser.result()
