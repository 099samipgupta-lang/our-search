import json
import os


class DocumentStore:
    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.documents = {}

    def add_document(
        self,
        document_id,
        title="",
        url="",
        text="",
        description="",
    ):
        if not document_id:
            raise ValueError("document_id is required")

        self.documents[document_id] = {
            "document_id": document_id,
            "title": title or "",
            "url": url or "",
            "text": text or "",
            "description": description or "",
        }

    def get(self, document_id):
        return self.documents.get(document_id)

    def save(self):
        directory = os.path.dirname(self.path)
        os.makedirs(directory, exist_ok=True)

        temporary_path = self.path + ".tmp"

        with open(
            temporary_path,
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                self.documents,
                file,
                ensure_ascii=False,
                indent=2,
            )
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary_path, self.path)

    def to_bytes(self):

        return json.dumps(
            self.documents,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")

    def load_bytes(
        self,
        payload
    ):

        if not isinstance(
            payload,
            bytes
        ):
            raise TypeError(
                "payload must be bytes"
            )

        self.documents = json.loads(
            payload.decode("utf-8")
        )

        if not isinstance(
            self.documents,
            dict
        ):
            raise ValueError(
                "document store data must be an object"
            )

    def save_to_storage(
        self,
        storage,
        key
    ):

        storage.put(
            key,
            self.to_bytes()
        )

    def load_from_storage(
        self,
        storage,
        key
    ):

        payload = storage.get(
            key
        )

        if payload is None:
            return False

        self.load_bytes(
            payload
        )

        return True

    def load(self):
        if not os.path.exists(self.path):
            return False

        with open(
            self.path,
            "r",
            encoding="utf-8",
        ) as file:
            self.documents = json.load(file)

        return True

    def count(self):
        return len(self.documents)
