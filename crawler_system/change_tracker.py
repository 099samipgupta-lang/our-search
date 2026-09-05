import hashlib
import time


class ChangeTracker:

    def __init__(self):
        self.pages = {}

    @staticmethod
    def fingerprint(body):
        return hashlib.sha256(body).hexdigest()

    def check(
        self,
        url,
        body=None,
        status=200
    ):

        previous = self.pages.get(url)

        # Page disappeared
        if status in (404, 410):

            if previous is None:
                return {
                    "state": "GONE",
                    "previous": None,
                    "current_hash": None
                }

            return {
                "state": "GONE",
                "previous": previous,
                "current_hash": None
            }

        # HTTP 304 means server says unchanged
        if status == 304:

            if previous is None:
                return {
                    "state": "UNKNOWN",
                    "previous": None,
                    "current_hash": None
                }

            return {
                "state": "UNCHANGED",
                "previous": previous,
                "current_hash":
                    previous["content_hash"]
            }

        if body is None:
            return {
                "state": "UNKNOWN",
                "previous": previous,
                "current_hash": None
            }

        current_hash = self.fingerprint(body)

        # First time seeing this URL
        if previous is None:

            return {
                "state": "NEW",
                "previous": None,
                "current_hash": current_hash
            }

        # Content identical
        if (
            previous["content_hash"]
            == current_hash
        ):

            return {
                "state": "UNCHANGED",
                "previous": previous,
                "current_hash": current_hash
            }

        # Content changed
        return {
            "state": "CHANGED",
            "previous": previous,
            "current_hash": current_hash
        }

    def register(
        self,
        url,
        body,
        status=200,
        etag=None,
        last_modified=None,
        final_url=None
    ):

        content_hash = self.fingerprint(
            body
        )

        self.pages[url] = {
            "content_hash":
                content_hash,

            "etag":
                etag,

            "last_modified":
                last_modified,

            "final_url":
                final_url or url,

            "last_crawled":
                time.time()
        }

        return self.pages[url]

    def mark_gone(self, url):

        if url in self.pages:

            self.pages[url][
                "gone"
            ] = True

            self.pages[url][
                "last_checked"
            ] = time.time()

    def get_state(self):

        return self.pages

    def load_state(self, state):

        if not state:
            return

        self.pages = dict(state)
