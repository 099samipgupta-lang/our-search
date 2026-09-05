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
