import os
import tempfile

from crawler_system.storage.backend import StorageBackend


class FileSystemStorage(StorageBackend):

    def __init__(self, root="crawler_storage"):

        self.root = os.path.abspath(root)

        os.makedirs(
            self.root,
            exist_ok=True
        )

    def _path(self, key):

        key = str(key).replace("\\", "/")

        if key.startswith("/"):
            raise ValueError("Invalid storage key")

        parts = key.split("/")

        if any(
            part in ("", ".", "..")
            for part in parts
        ):
            raise ValueError("Invalid storage key")

        return os.path.join(
            self.root,
            *parts
        )

    def put(self, key, data):

        path = self._path(key)

        directory = os.path.dirname(path)

        os.makedirs(
            directory,
            exist_ok=True
        )

        fd, temporary_path = tempfile.mkstemp(
            prefix=".storage_",
            dir=directory
        )

        try:

            with os.fdopen(fd, "wb") as file:

                file.write(data)

                file.flush()

                os.fsync(
                    file.fileno()
                )

            os.replace(
                temporary_path,
                path
            )

        except Exception:

            try:
                os.unlink(
                    temporary_path
                )
            except OSError:
                pass

            raise

        return key

    def get(self, key):

        path = self._path(key)

        with open(
            path,
            "rb"
        ) as file:

            return file.read()

    def exists(self, key):

        return os.path.isfile(
            self._path(key)
        )

    def delete(self, key):

        path = self._path(key)

        if not os.path.exists(path):
            return False

        os.remove(path)

        return True

    def list_keys(self, prefix=""):

        base = self._path(
            prefix
        ) if prefix else self.root

        if not os.path.exists(base):
            return []

        if os.path.isfile(base):

            relative = os.path.relpath(
                base,
                self.root
            )

            return [
                relative.replace("\\", "/")
            ]

        results = []

        for directory, _, files in os.walk(base):

            for filename in files:

                full_path = os.path.join(
                    directory,
                    filename
                )

                relative = os.path.relpath(
                    full_path,
                    self.root
                )

                results.append(
                    relative.replace("\\", "/")
                )

        return sorted(results)
