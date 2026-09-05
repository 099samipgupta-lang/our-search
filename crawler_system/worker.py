from crawler_system.result import CrawlResult


class CrawlWorker:

    def __init__(
        self,
        fetcher,
        policy,
        worker_id=0
    ):

        self.fetcher = fetcher
        self.policy = policy
        self.worker_id = worker_id

        self.tasks_processed = 0
        self.tasks_failed = 0

    def process(
        self,
        task,
        previous=None
    ):

        self.tasks_processed += 1

        if not self.policy.allow(
            task.url
        ):

            self.tasks_failed += 1

            response = {
                "url": task.url,
                "requested_url": task.url,
                "status": 0,
                "status_type":
                    "robots_denied",
                "content_type": "",
                "headers": {},
                "body": b"",
                "redirect_chain": [],
                "final_url": task.url,
                "storage_path": None,
                "worker_id":
                    self.worker_id
            }

            return CrawlResult(
                task,
                response
            )

        etag = None
        last_modified = None

        if previous:

            etag = previous.get(
                "etag"
            )

            last_modified = previous.get(
                "last_modified"
            )

        response = self.fetcher.fetch(
            task.url,
            etag=etag,
            last_modified=last_modified
        )

        response["storage_path"] = None

        response["worker_id"] = (
            self.worker_id
        )

        if response.get(
            "status",
            0
        ) >= 400:

            self.tasks_failed += 1

        return CrawlResult(
            task,
            response
        )
