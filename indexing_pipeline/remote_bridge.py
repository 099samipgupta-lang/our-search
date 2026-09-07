import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class RemoteCrawlerIndexBridge:

    def __init__(
        self,
        index_url,
        timeout=30,
    ):

        if not isinstance(index_url, str):
            raise TypeError(
                "index_url must be a string"
            )

        index_url = index_url.strip()

        if not index_url:
            raise ValueError(
                "index_url must not be empty"
            )

        self.index_url = index_url

        if index_url.endswith("/index"):
            self.delete_url = (
                index_url[:-len("/index")]
                + "/delete"
            )
        else:
            self.delete_url = (
                index_url.rstrip("/")
                + "/delete"
            )

        self.timeout = max(
            1,
            int(timeout)
        )

        self.processed = 0
        self.indexed = 0
        self.skipped = 0
        self.failed = 0

    def _post_json(self, url, payload):

        body = json.dumps(
            payload,
            ensure_ascii=False
        ).encode("utf-8")

        request = Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type":
                    "application/json; charset=utf-8",
                "Accept":
                    "application/json",
            },
        )

        try:

            with urlopen(
                request,
                timeout=self.timeout
            ) as response:

                response_body = (
                    response.read()
                    .decode(
                        "utf-8",
                        errors="replace"
                    )
                )

                return json.loads(
                    response_body
                )

        except HTTPError as error:

            error_body = ""

            try:
                error_body = (
                    error.read()
                    .decode(
                        "utf-8",
                        errors="replace"
                    )
                )
            except Exception:
                pass

            raise RuntimeError(
                "index API returned HTTP "
                f"{error.code}: {error_body}"
            )

        except URLError as error:

            raise RuntimeError(
                "index API connection failed: "
                f"{error.reason}"
            )

    def process_page(
        self,
        document_id,
        url,
        body,
    ):

        self.processed += 1

        if not isinstance(body, bytes):
            self.failed += 1

            return {
                "action": "failed",
                "document_id": document_id,
                "error": "body must be bytes",
            }

        try:

            from indexing_pipeline.crawler_bridge import (
                PageExtractor
            )

            html = body.decode(
                "utf-8",
                errors="ignore"
            )

            parser = PageExtractor(
                base_url=url
            )

            parser.feed(html)
            parser.close()

            extracted = parser.result()

            text = extracted["text"]

            if not text:

                self.skipped += 1

                return {
                    "action": "skipped",
                    "reason": "no_visible_text",
                    "document_id": document_id,
                }

            payload = {
                "document_id": document_id,
                "url": url,
                "title": extracted["title"],
                "text": text,
                "canonical_url":
                    extracted["canonical_url"],
            }

            result = self._post_json(
                self.index_url,
                payload
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

    def remove_document(
        self,
        document_id,
    ):

        payload = {
            "document_id": document_id,
        }

        result = self._post_json(
            self.delete_url,
            payload
        )

        if result.get("action") == "deleted":
            return True

        return False

    def flush(self):

        return None

    def close(self):

        return None

    def status(self):

        return {
            "bridge": {
                "type":
                    "remote",
                "index_url":
                    self.index_url,
                "processed":
                    self.processed,
                "indexed":
                    self.indexed,
                "skipped":
                    self.skipped,
                "failed":
                    self.failed,
            }
        }
