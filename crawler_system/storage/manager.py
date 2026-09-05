import hashlib
import os
import time

from crawler_system.storage.filesystem import FileSystemStorage
from crawler_system.storage.metadata import MetadataStore


class CrawlStorage:

    def __init__(
        self,
        root="crawler_storage"
    ):

        self.root = os.path.abspath(root)

        self.pages = FileSystemStorage(
            os.path.join(
                self.root,
                "pages"
            )
        )

        self.metadata = MetadataStore(
            os.path.join(
                self.root,
                "metadata.json"
            )
        )

    @staticmethod
    def _page_key(document_id):

        return (
            str(document_id)
            + ".html"
        )

    def save_page(
        self,
        document_id,
        body,
        metadata=None
    ):

        if not isinstance(body, bytes):
            raise TypeError(
                "body must be bytes"
            )

        key = self._page_key(
            document_id
        )

        self.pages.put(
            key,
            body
        )

        record = {
            "document_id":
                document_id,

            "storage_key":
                key,

            "size":
                len(body),

            "content_hash":
                hashlib.sha256(
                    body
                ).hexdigest(),

            "stored_at":
                time.time()
        }

        if metadata:
            record.update(
                metadata
            )

        self.metadata.put(
            document_id,
            record
        )

        self.metadata.save()

        return record

    def get_page(
        self,
        document_id
    ):

        key = self._page_key(
            document_id
        )

        return self.pages.get(
            key
        )

    def get_metadata(
        self,
        document_id
    ):

        return self.metadata.get(
            document_id
        )

    def page_exists(
        self,
        document_id
    ):

        return self.pages.exists(
            self._page_key(
                document_id
            )
        )

    def delete_page(
        self,
        document_id
    ):

        deleted = self.pages.delete(
            self._page_key(
                document_id
            )
        )

        self.metadata.delete(
            document_id
        )

        self.metadata.save()

        return deleted

    def list_pages(self):

        keys = self.pages.list_keys()

        return [
            key[:-5]
            for key in keys
            if key.endswith(".html")
        ]

    def count_pages(self):

        return len(
            self.list_pages()
        )

    def close(self):

        self.metadata.save()
