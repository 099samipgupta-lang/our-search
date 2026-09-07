import os

from crawler_system.storage.backend import StorageBackend
from crawler_system.storage.filesystem import FileSystemStorage
from crawler_system.storage.metadata import MetadataStore
from crawler_system.storage.manager import CrawlStorage


class PageStorage:

    def __init__(self, directory="crawler_data"):
        self.directory = directory
        os.makedirs(
            self.directory,
            exist_ok=True
        )

    def save(self, document_id, body):
        path = os.path.join(
            self.directory,
            document_id + ".html"
        )

        with open(path, "wb") as file:
            file.write(body)

        return path


__all__ = [
    "StorageBackend",
    "FileSystemStorage",
    "MetadataStore",
    "CrawlStorage",
    "PageStorage",
]
