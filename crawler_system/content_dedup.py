import hashlib
import re
from html.parser import HTMLParser


class VisibleTextExtractor(HTMLParser):

    def __init__(self):
        super().__init__()
        self.parts = []
        self.ignore_depth = 0

    def handle_starttag(self, tag, attrs):

        tag = tag.lower()

        if tag in ("script", "style", "noscript", "template"):
            self.ignore_depth += 1

    def handle_endtag(self, tag):

        tag = tag.lower()

        if tag in ("script", "style", "noscript", "template"):
            if self.ignore_depth > 0:
                self.ignore_depth -= 1

    def handle_data(self, data):

        if self.ignore_depth > 0:
            return

        text = data.strip()

        if text:
            self.parts.append(text)

    def get_text(self):
        return " ".join(self.parts)


class ContentFingerprint:

    @staticmethod
    def raw_hash(body):
        return hashlib.sha256(body).hexdigest()

    @staticmethod
    def visible_text_hash(body):

        try:
            html = body.decode(
                "utf-8",
                errors="ignore"
            )

            parser = VisibleTextExtractor()
            parser.feed(html)

            text = parser.get_text()

            text = re.sub(
                r"\s+",
                " ",
                text
            ).strip().lower()

            return hashlib.sha256(
                text.encode("utf-8")
            ).hexdigest()

        except Exception:
            return None


class ContentDeduplicator:

    def __init__(self):
        self.raw_hashes = {}
        self.text_hashes = {}

    def inspect(
        self,
        body,
        content_type=""
    ):

        raw_hash = ContentFingerprint.raw_hash(
            body
        )

        exact_duplicate_of = (
            self.raw_hashes.get(
                raw_hash
            )
        )

        text_hash = None
        possible_duplicate_of = None

        if "html" in content_type.lower():

            text_hash = (
                ContentFingerprint.visible_text_hash(
                    body
                )
            )

            if text_hash:
                possible_duplicate_of = (
                    self.text_hashes.get(
                        text_hash
                    )
                )

        return {
            "raw_hash": raw_hash,
            "text_hash": text_hash,
            "exact_duplicate_of":
                exact_duplicate_of,
            "possible_duplicate_of":
                possible_duplicate_of
        }

    def register(
        self,
        document_id,
        body,
        content_type=""
    ):

        result = self.inspect(
            body,
            content_type
        )

        raw_hash = result["raw_hash"]

        if raw_hash not in self.raw_hashes:
            self.raw_hashes[raw_hash] = (
                document_id
            )

        text_hash = result["text_hash"]

        if (
            text_hash
            and text_hash not in self.text_hashes
        ):
            self.text_hashes[text_hash] = (
                document_id
            )

        return result

    def get_state(self):

        return {
            "raw_hashes":
                self.raw_hashes,
            "text_hashes":
                self.text_hashes
        }

    def load_state(self, state):

        if not state:
            return

        self.raw_hashes = dict(
            state.get(
                "raw_hashes",
                {}
            )
        )

        self.text_hashes = dict(
            state.get(
                "text_hashes",
                {}
            )
        )
