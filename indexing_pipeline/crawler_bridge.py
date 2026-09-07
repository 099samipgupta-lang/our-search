from html.parser import HTMLParser
from urllib.parse import urljoin



class PageExtractor(HTMLParser):

    def __init__(self, base_url=""):
        super().__init__()

        self.base_url = base_url

        self.title_parts = []
        self.text_parts = []

        self.in_title = False
        self.skip_depth = 0

        self.skip_tags = {
            "script",
            "style",
            "noscript",
            "template",
            "svg",
        }

        self.canonical_url = ""

    def handle_starttag(self, tag, attrs):

        tag = tag.lower()

        attributes = dict(attrs)

        if tag == "title":
            self.in_title = True

        if tag in self.skip_tags:
            self.skip_depth += 1

        if tag == "link":

            rel = attributes.get("rel", "")
            href = attributes.get("href", "")

            if (
                href
                and "canonical" in rel.lower()
            ):
                self.canonical_url = urljoin(
                    self.base_url,
                    href
                )

    def handle_endtag(self, tag):

        tag = tag.lower()

        if tag == "title":
            self.in_title = False

        if tag in self.skip_tags:
            if self.skip_depth > 0:
                self.skip_depth -= 1

    def handle_data(self, data):

        if self.skip_depth > 0:
            return

        text = data.strip()

        if not text:
            return

        if self.in_title:
            self.title_parts.append(text)
        else:
            self.text_parts.append(text)

    def result(self):

        title = " ".join(
            self.title_parts
        ).strip()

        text = " ".join(
            self.text_parts
        ).strip()

        return {
            "title": title,
            "text": text,
            "canonical_url": self.canonical_url,
        }


class CrawlerIndexBridge:

    def __init__(
        self,
        pipeline=None,
    ):

        if pipeline is None:
            from indexing_pipeline.pipeline import CrawlIndexPipeline
            pipeline = CrawlIndexPipeline()

        self.pipeline = pipeline

        self.processed = 0
        self.indexed = 0
        self.skipped = 0
        self.failed = 0

    def extract(
        self,
        url,
        body,
    ):

        if not isinstance(body, bytes):
            raise TypeError(
                "body must be bytes"
            )

        try:

            html = body.decode(
                "utf-8",
                errors="ignore"
            )

        except Exception:

            html = body.decode(
                errors="ignore"
            )

        parser = PageExtractor(
            base_url=url
        )

        parser.feed(html)
        parser.close()

        return parser.result()

    def process_page(
        self,
        document_id,
        url,
        body,
    ):

        self.processed += 1

        try:

            extracted = self.extract(
                url,
                body
            )

            text = extracted["text"]

            if not text:
                self.skipped += 1

                return {
                    "action": "skipped",
                    "reason": "no_visible_text",
                    "document_id": document_id,
                }

            result = self.pipeline.add_document(
                document_id=document_id,
                url=url,
                title=extracted["title"],
                text=text,
                canonical_url=(
                    extracted["canonical_url"]
                ),
            )

            action = result.get(
                "action"
            )

            if action == "skipped":
                self.skipped += 1
            else:
                self.indexed += 1

            return result

        except Exception as error:

            self.failed += 1

            return {
                "action": "failed",
                "document_id": document_id,
                "error": str(error),
            }

    def remove_document(self, document_id):

        return self.pipeline.remove_document(document_id)

    def flush(self):

        return self.pipeline.flush()

    def status(self):

        status = self.pipeline.status()

        status["bridge"] = {
            "processed": self.processed,
            "indexed": self.indexed,
            "skipped": self.skipped,
            "failed": self.failed,
        }

        return status

    def close(self):

        self.pipeline.close()
