import os


from index_storage.backend import IndexStorageBackend


class LocalIndexStorage(IndexStorageBackend):

    def __init__(self, root="index_storage_data"):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)

    def _path(self, key):
        if not isinstance(key, str):
            raise TypeError("key must be a string")

        key = key.strip()

        if not key:
            raise ValueError("key must not be empty")

        if os.path.isabs(key):
            raise ValueError("absolute keys are not allowed")

        path = os.path.abspath(
            os.path.join(self.root, key)
        )

        if os.path.commonpath(
            [self.root, path]
        ) != self.root:
            raise ValueError("key escapes storage root")

        return path

    def put(self, key, data):
        path = self._path(key)

        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)

        temporary_path = path + ".tmp"

        with open(
            temporary_path,
            "wb"
        ) as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())

        os.replace(
            temporary_path,
            path
        )

    def get(self, key):
        path = self._path(key)

        if not os.path.exists(path):
            return None

        with open(
            path,
            "rb"
        ) as file:
            return file.read()

    def exists(self, key):
        return os.path.exists(
            self._path(key)
        )

    def delete(self, key):
        path = self._path(key)

        if not os.path.exists(path):
            return False

        os.remove(path)

        return True

    def list_keys(self, prefix=""):
        prefix = prefix or ""

        base = self._path(prefix) if prefix else self.root

        if not os.path.exists(base):
            return []

        keys = []

        for root, directories, files in os.walk(base):
            directories.sort()
            files.sort()

            for filename in files:
                if filename.endswith(".tmp"):
                    continue

                full_path = os.path.join(
                    root,
                    filename
                )

                relative = os.path.relpath(
                    full_path,
                    self.root
                )

                keys.append(
                    relative.replace(
                        os.sep,
                        "/"
                    )
                )

        return sorted(keys)
