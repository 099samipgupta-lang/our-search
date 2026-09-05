import os


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
