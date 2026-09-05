import json
import os
import tempfile


class MetadataStore:

    def __init__(
        self,
        path="crawler_storage/metadata.json"
    ):

        self.path = os.path.abspath(path)

        os.makedirs(
            os.path.dirname(self.path),
            exist_ok=True
        )

        self.records = {}

        self.load()

    def put(self, document_id, metadata):

        self.records[
            document_id
        ] = dict(metadata)

    def get(self, document_id):

        record = self.records.get(
            document_id
        )

        if record is None:
            return None

        return dict(record)

    def delete(self, document_id):

        if document_id not in self.records:
            return False

        del self.records[
            document_id
        ]

        return True

    def count(self):

        return len(
            self.records
        )

    def save(self):

        directory = os.path.dirname(
            self.path
        )

        fd, temporary_path = tempfile.mkstemp(
            prefix=".metadata_",
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
                    self.records,
                    file,
                    ensure_ascii=False,
                    indent=2
                )

                file.flush()

                os.fsync(
                    file.fileno()
                )

            os.replace(
                temporary_path,
                self.path
            )

        except Exception:

            try:
                os.unlink(
                    temporary_path
                )
            except OSError:
                pass

            raise

    def load(self):

        if not os.path.exists(
            self.path
        ):

            return

        with open(
            self.path,
            "r",
            encoding="utf-8"
        ) as file:

            self.records = json.load(
                file
            )
