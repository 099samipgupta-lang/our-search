from dataclasses import dataclass

from crawler_system.task import CrawlTask


@dataclass
class CrawlResult:

    task: CrawlTask
    response: dict

    def success(self):

        return (
            200
            <= self.response.get(
                "status",
                0
            )
            < 300
        )

    def to_dict(self):

        return {
            "task":
                self.task.to_dict(),

            "response":
                self.response
        }
