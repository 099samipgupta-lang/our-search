class AutomaticCrawlerIndexer:
    def __init__(self, bridge, flush_every=25):
        self.bridge = bridge
        self.flush_every = max(1, int(flush_every))

        self.pending = 0

        self.stats = {
            "new_indexed": 0,
            "updated": 0,
            "unchanged": 0,
            "deleted": 0,
            "non_html": 0,
            "failed": 0,
            "flushes": 0,
        }

    def process_success(
        self,
        result,
        state,
        content_type,
        exact_duplicate=False,
    ):
        if "html" not in (content_type or "").lower():
            self.stats["non_html"] += 1
            return

        if state == "UNCHANGED":
            self.stats["unchanged"] += 1
            return

        try:
            task = result.task
            response = result.response

            url = response.get("final_url") or task.url
            body = response.get("body", b"")

            outcome = self.bridge.process_page(
                task.document_id,
                url,
                body,
            )

            action = outcome.get("action")

            if action == "indexed":
                self.stats["new_indexed"] += 1
                self.pending += 1

            elif action == "updated":
                self.stats["updated"] += 1
                self.pending += 1

            elif action == "skipped":
                self.stats["unchanged"] += 1

            if self.pending >= self.flush_every:
                self.flush()

        except Exception:
            self.stats["failed"] += 1

    def process_deleted(self, document_id):
        try:
            removed = self.bridge.pipeline.remove_document(document_id)

            if removed:
                self.stats["deleted"] += 1

            return removed

        except Exception:
            self.stats["failed"] += 1
            return False

    def flush(self):
        try:
            segment_id = self.bridge.flush()

            if segment_id is not None:
                self.stats["flushes"] += 1

            self.pending = 0
            return segment_id

        except Exception:
            self.stats["failed"] += 1
            return None

    def close(self):
        self.flush()
        self.bridge.close()

    def status(self):
        return {
            "pending": self.pending,
            "stats": dict(self.stats),
            "bridge": self.bridge.status(),
        }


def attach_to_crawler(crawler, integration):
    crawler.index_integration = integration
    return crawler
