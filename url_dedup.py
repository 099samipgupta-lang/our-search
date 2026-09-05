class URLDeduplicator:
    def __init__(self):
        self.seen = set()

    def is_new(self, url):
        if url in self.seen:
            return False

        self.seen.add(url)
        return True
