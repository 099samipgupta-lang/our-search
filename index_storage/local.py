import os

from index_storage.backend import IndexStorageBackend
from index_storage.integrity import StorageIntegrity
from index_storage.wal import WriteAheadLog


class LocalIndexStorage(IndexStorageBackend):

    def __init__(self, root="index_storage_data"):
        self.root = os.path.abspath(root)

        os.makedirs(self.root, exist_ok=True)

        self.wal_path = os.path.join(
            self.root,
            "storage.wal",
        )

        self.integrity_root = os.path.join(
            self.root,
            ".integrity",
        )

        os.makedirs(
            self.integrity_root,
            exist_ok=True,
        )

        self.wal = WriteAheadLog(self.wal_path)

        self._recover()

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

    def _integrity_path(self, key):
        path = self._path(key)

        relative = os.path.relpath(
            path,
            self.root,
        )

        metadata_path = os.path.join(
            self.integrity_root,
            relative + ".sha256",
        )

        return metadata_path

    def _write_integrity(self, key, data):
        metadata_path = self._integrity_path(key)

        directory = os.path.dirname(metadata_path)
        os.makedirs(directory, exist_ok=True)

        checksum = StorageIntegrity.checksum(data)

        temporary_path = metadata_path + ".tmp"

        with open(
            temporary_path,
            "w",
            encoding="utf-8",
        ) as file:
            file.write(checksum)
            file.flush()
            os.fsync(file.fileno())

        os.replace(
            temporary_path,
            metadata_path,
        )

    def _remove_integrity(self, key):
        metadata_path = self._integrity_path(key)

        if os.path.exists(metadata_path):
            os.remove(metadata_path)

    def _verify_integrity(self, key, data):
        metadata_path = self._integrity_path(key)

        if not os.path.exists(metadata_path):
            raise ValueError(
                f"missing integrity metadata: {key}"
            )

        with open(
            metadata_path,
            "r",
            encoding="utf-8",
        ) as file:
            expected_checksum = file.read().strip()

        if not StorageIntegrity.verify(
            data,
            expected_checksum,
        ):
            raise ValueError(
                f"storage integrity check failed: {key}"
            )

    def _recover(self):
        records = self.wal.recover()

        for record in records:
            operation = record["operation"]
            key = record["key"]
            data = record["data"]

            path = self._path(key)

            if operation == "put":
                directory = os.path.dirname(path)
                os.makedirs(directory, exist_ok=True)

                temporary_path = path + ".recovery.tmp"

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

                self._write_integrity(
                    key,
                    data,
                )

            elif operation == "delete":
                if os.path.exists(path):
                    os.remove(path)

                self._remove_integrity(
                    key,
                )

        if records:
            self.wal.clear()

    def put(self, key, data):
        path = self._path(key)

        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)

        self.wal.append(
            "put",
            key,
            data,
        )

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

        self._write_integrity(
            key,
            data,
        )

        self.wal.clear()

    def get(self, key):
        path = self._path(key)

        if not os.path.exists(path):
            return None

        with open(
            path,
            "rb"
        ) as file:
            data = file.read()

        self._verify_integrity(
            key,
            data,
        )

        return data

    def exists(self, key):
        return os.path.exists(
            self._path(key)
        )

    def delete(self, key):
        path = self._path(key)

        if not os.path.exists(path):
            return False

        self.wal.append(
            "delete",
            key,
        )

        os.remove(path)

        self._remove_integrity(
            key,
        )

        self.wal.clear()

        return True

    def list_keys(self, prefix=""):
        prefix = prefix or ""

        base = self._path(prefix) if prefix else self.root

        if not os.path.exists(base):
            return []

        keys = []

        for root, directories, files in os.walk(base):
            directories[:] = [
                directory
                for directory in directories
                if directory != ".integrity"
            ]

            directories.sort()
            files.sort()

            for filename in files:
                if filename in {
                    "storage.wal",
                }:
                    continue

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
