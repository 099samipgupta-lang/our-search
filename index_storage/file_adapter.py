import os


class StorageFileAdapter:

    def __init__(self, storage, root=""):
        self.storage = storage
        self.root = root.strip("/")

    def _key(self, path):
        path = os.path.normpath(path)

        if os.path.isabs(path):
            raise ValueError("absolute paths are not allowed")

        if path == ".":
            relative = ""
        else:
            relative = path

        if self.root:
            if relative:
                return f"{self.root}/{relative}"
            return self.root

        return relative

    def write_bytes(self, path, data):
        self.storage.put(
            self._key(path),
            data
        )

    def read_bytes(self, path):
        return self.storage.get(
            self._key(path)
        )

    def exists(self, path):
        return self.storage.exists(
            self._key(path)
        )

    def delete(self, path):
        return self.storage.delete(
            self._key(path)
        )

    def list_files(self, prefix=""):
        key_prefix = self._key(prefix)
        return self.storage.list_keys(
            key_prefix
        )
