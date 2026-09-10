from dataclasses import dataclass
from typing import Optional


@dataclass
class CrawlTask:

    url: str
    document_id: str
    priority: float = 50.0
    attempt: int = 0

    # HTTP conditional-request validators.
    # These are copied from durable URL state by the coordinator
    # before the task enters the multiprocessing pool.
    etag: Optional[str] = None
    last_modified: Optional[str] = None

    def to_dict(self):

        return {
            "url": self.url,
            "document_id": self.document_id,
            "priority": self.priority,
            "attempt": self.attempt,
            "etag": self.etag,
            "last_modified": self.last_modified
        }

    @classmethod
    def from_dict(cls, data):

        return cls(
            url=data["url"],
            document_id=data["document_id"],
            priority=float(
                data.get("priority", 50.0)
            ),
            attempt=int(
                data.get("attempt", 0)
            ),
            etag=data.get("etag"),
            last_modified=data.get("last_modified")
        )

    def retry(self):

        return CrawlTask(
            url=self.url,
            document_id=self.document_id,
            priority=self.priority,
            attempt=self.attempt + 1,
            etag=self.etag,
            last_modified=self.last_modified
        )
