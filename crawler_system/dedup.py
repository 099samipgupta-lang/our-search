from crawler_system.url_normalizer import URLNormalizer


class URLDeduplicator:

    def __init__(
        self,
        normalizer=None
    ):

        self.normalizer = (
            normalizer
            if normalizer is not None
            else URLNormalizer()
        )

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

        self.seen = set(state)
