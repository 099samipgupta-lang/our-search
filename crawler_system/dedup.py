from urllib.parse import urlparse

from crawler_system.url_normalizer import URLNormalizer
from crawler_system.document_identity import DocumentIdentity


class URLDeduplicator:

    def __init__(
        self,
        normalizer=None,
        state_store=None
    ):

        self.normalizer = (
            normalizer
            if normalizer is not None
            else URLNormalizer()
        )

        self.state_store = state_store
        self.seen = set()

    def normalize(self, url):

        return self.normalizer.normalize(
            url
        )

    def is_new(self, url):

        normalized = self.normalize(
            url
        )

        if normalized is None:
            return False

        if normalized in self.seen:
            return False

        if self.state_store is not None:

            existing = self.state_store.get(
                normalized
            )

            if existing is not None:
                self.seen.add(
                    normalized
                )

                return False

            parsed = urlparse(
                normalized
            )

            host = parsed.netloc.lower()

            document_id = (
                DocumentIdentity.from_normalized_url(
                    normalized
                )
            )

            inserted = self.state_store.add_discovered(
                url=normalized,
                document_id=document_id,
                host=host
            )

            if not inserted:
                self.seen.add(
                    normalized
                )

                return False

        self.seen.add(
            normalized
        )

        return True

    def get_state(self):

        return sorted(
            self.seen
        )

    def load_state(self, state):

        if not state:
            return

        self.seen = set(
            state
        )
