from dataclasses import dataclass


@dataclass
class CrawlTask:

    url: str
    document_id: str
    priority: float = 50.0
    attempt: int = 0

    def to_dict(self):

        return {
            "url": self.url,
            "document_id": self.document_id,
            "priority": self.priority,
            "attempt": self.attempt
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
            )
        )

    def retry(self):

        return CrawlTask(
            url=self.url,
            document_id=self.document_id,
            priority=self.priority,
            attempt=self.attempt + 1
        )
