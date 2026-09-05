import json
import os
import tempfile
import time


class VersionState:

    FORMAT_VERSION = 1

    def __init__(self, path="version_state.json"):
        self.path = os.path.abspath(path)
        self.documents = {}

        self.load()

    def _atomic_save(self, data):

        directory = os.path.dirname(self.path)

        os.makedirs(
            directory,
            exist_ok=True
        )

        fd, temporary_path = tempfile.mkstemp(
            prefix=".version_state_",
            suffix=".tmp",
            dir=directory
        )

        try:

            with os.fdopen(
                fd,
                "w",
                encoding="utf-8"
            ) as file:

                json.dump(
                    data,
                    file,
                    ensure_ascii=False,
                    indent=2
                )

                file.flush()
                os.fsync(file.fileno())

            os.replace(
                temporary_path,
                self.path
            )

        except Exception:

            try:
                os.unlink(temporary_path)
            except OSError:
                pass

            raise

    def save(self):

        data = {
            "format_version":
                self.FORMAT_VERSION,

            "documents":
                self.documents
        }

        self._atomic_save(data)

    def load(self):

        if not os.path.exists(self.path):
            return False

        with open(
            self.path,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        if data.get("format_version") != self.FORMAT_VERSION:
            raise ValueError(
                "Unsupported version state format"
            )

        self.documents = dict(
            data.get("documents", {})
        )

        return True

    def activate(
        self,
        document_id,
        content_hash="",
    ):

        previous = self.documents.get(
            document_id
        )

        version = 1

        if previous:
            version = int(
                previous.get("version", 0)
            ) + 1

        record = {
            "version": version,
            "state": "active",
            "content_hash": content_hash,
            "updated_at": time.time()
        }

        self.documents[
            document_id
        ] = record

        return dict(record)

    def tombstone(
        self,
        document_id,
    ):

        previous = self.documents.get(
            document_id
        )

        version = 1

        if previous:
            version = int(
                previous.get("version", 0)
            ) + 1

        record = {
            "version": version,
            "state": "deleted",
            "content_hash": "",
            "updated_at": time.time()
        }

        self.documents[
            document_id
        ] = record

        return dict(record)

    def is_active(
        self,
        document_id
    ):

        record = self.documents.get(
            document_id
        )

        return bool(
            record
            and record.get("state") == "active"
        )

    def get(
        self,
        document_id
    ):

        record = self.documents.get(
            document_id
        )

        if record is None:
            return None

        return dict(record)

    def active_document_ids(self):

        return {
            document_id
            for document_id, record
            in self.documents.items()
            if record.get("state") == "active"
        }

    def document_count(self):

        return len(
            self.active_document_ids()
        )

    def deleted_count(self):

        return sum(
            1
            for record in self.documents.values()
            if record.get("state") == "deleted"
        )
