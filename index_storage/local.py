import os
import threading

from index_storage.backend import IndexStorageBackend
from index_storage.catalog import StorageCatalog
from index_storage.config import validate_storage_config
from index_storage.health import StorageHealthMonitor
from index_storage.integrity import StorageIntegrity
from index_storage.large_objects import LargeObjectStore
from index_storage.metadata import StorageMetadata
from index_storage.metrics import StorageMetrics
from index_storage.wal import WriteAheadLog

from functools import wraps


def _count_storage_errors(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception:
            self.metrics.record_error()
            raise

    return wrapped




class _DirectStorageAdapter:

    def __init__(self, storage):
        self.storage = storage

    def put(self, key, data):
        self.storage._ensure_open()
        return self.storage._put_direct(key, data)

    def get(self, key):
        self.storage._ensure_open()
        return self.storage._get_direct(key)

    def exists(self, key):
        self.storage._ensure_open()
        return self.storage._exists_direct(key)

    def delete(self, key):
        self.storage._ensure_open()
        return self.storage._delete_direct(key)

    def list_keys(self, prefix=""):
        self._ensure_open()
        return self.storage._list_keys_direct(prefix)


class LocalIndexStorage(IndexStorageBackend):

    def __init__(
        self,
        root="index_storage_data",
        max_object_size=None,
        max_key_length=1024,
    ):
        validate_storage_config(
            root,
            max_object_size=max_object_size,
            max_key_length=max_key_length,
        )
        self.root = os.path.abspath(root)
        self.max_object_size = max_object_size
        self.max_key_length = max_key_length
        self._lock = threading.RLock()

        os.makedirs(self.root, exist_ok=True)

        self.wal_path = os.path.join(
            self.root,
            "storage.wal",
        )

        self.integrity_root = os.path.join(
            self.root,
            ".integrity",
        )

        self.metadata_root = os.path.join(
            self.root,
            ".metadata",
        )

        os.makedirs(
            self.integrity_root,
            exist_ok=True,
        )

        os.makedirs(
            self.metadata_root,
            exist_ok=True,
        )

        self.wal = WriteAheadLog(self.wal_path)

        self.catalog = StorageCatalog(
            self.root
        )
        self.metrics = StorageMetrics()
        self.health_monitor = StorageHealthMonitor(self)

        self._recover()

        self._large_objects = LargeObjectStore(
            _DirectStorageAdapter(self)
        )

        self._closed = False

    def close(self):
        with self._lock:
            if self._closed:
                return

            self.catalog.compact()
            self._closed = True

    def _ensure_open(self):
        if self._closed:
            raise RuntimeError("storage is closed")

    def _path(self, key):
        if not isinstance(key, str):
            raise TypeError("key must be a string")

        key = key.strip()

        if not key:
            raise ValueError("key must not be empty")

        if len(key) > self.max_key_length:
            raise ValueError(
                f"key exceeds maximum length: {self.max_key_length}"
            )

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

    def _metadata_path(self, key):
        path = self._path(key)

        relative = os.path.relpath(
            path,
            self.root,
        )

        return os.path.join(
            self.metadata_root,
            relative + ".json",
        )

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

    def _read_metadata(self, key):
        metadata_path = self._metadata_path(key)

        if not os.path.exists(metadata_path):
            return None

        with open(
            metadata_path,
            "rb",
        ) as file:
            data = file.read()

        return StorageMetadata.from_bytes(data)

    def _next_version(self, key):
        metadata = self._read_metadata(key)

        if metadata is None:
            return 1

        return metadata.version + 1

    @staticmethod
    def _metadata_from_dict(record):
        return StorageMetadata(
            version=record["version"],
            size=record["size"],
            checksum=record["checksum"],
            created_at=record["created_at"],
            updated_at=record["updated_at"],
        )

    def _write_metadata(self, key, metadata):
        metadata_path = self._metadata_path(key)

        directory = os.path.dirname(metadata_path)
        os.makedirs(directory, exist_ok=True)

        temporary_path = metadata_path + ".tmp"

        with open(
            temporary_path,
            "wb",
        ) as file:
            file.write(metadata.to_bytes())
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

                metadata_record = record.get("metadata")

                if metadata_record is None:
                    raise ValueError(
                        f"missing WAL metadata: {key}"
                    )

                metadata = self._metadata_from_dict(
                    metadata_record,
                )

                self._write_metadata(
                    key,
                    metadata,
                )

                if (
                    ".chunks/" not in key
                    and not key.endswith(".manifest")
                ):
                    self.catalog.put(
                        key,
                        metadata.to_dict(),
                    )

            elif operation == "delete":
                if os.path.exists(path):
                    os.remove(path)

                self._remove_integrity(
                    key,
                )

                metadata_path = self._metadata_path(key)

                if os.path.exists(metadata_path):
                    os.remove(metadata_path)

                if (
                    ".chunks/" not in key
                    and not key.endswith(".manifest")
                ):
                    self.catalog.delete(
                        key
                    )

        if records:
            self.wal.clear()

    def _put_direct(self, key, data):
        path = self._path(key)

        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        if (
            self.max_object_size is not None
            and len(data) > self.max_object_size
        ):
            raise ValueError(
                f"object exceeds maximum size: "
                f"{self.max_object_size}"
            )

        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)

        version = self._next_version(key)

        checksum = StorageIntegrity.checksum(data)

        metadata = StorageMetadata.create(
            version=version,
            data=data,
            checksum=checksum,
        )

        self.wal.append(
            "put",
            key,
            data,
            metadata=metadata.to_dict(),
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

        self._write_metadata(
            key,
            metadata,
        )

        self.wal.clear()

    def _get_direct(self, key):
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

    def _exists_direct(self, key):
        return os.path.exists(
            self._path(key)
        )

    def _delete_direct(self, key):
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

        metadata_path = self._metadata_path(key)

        if os.path.exists(metadata_path):
            os.remove(metadata_path)

        self.catalog.delete(
            key
        )

        self.wal.clear()

        return True

    def _list_keys_direct(self, prefix=""):
        prefix = prefix or ""

        base = self._path(prefix) if prefix else self.root

        if not os.path.exists(base):
            return []

        keys = []

        for root, directories, files in os.walk(base):
            directories[:] = [
                directory
                for directory in directories
                if directory not in {
                    ".integrity",
                    ".metadata",
                }
            ]

            directories.sort()
            files.sort()

            for filename in files:
                if filename in {
                    "storage.wal",
                    StorageCatalog.FILENAME,
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

    @_count_storage_errors
    def put(self, key, data):
        self._ensure_open()
        with self._lock:
            if not isinstance(data, bytes):
                raise TypeError("data must be bytes")

            if (
                self.max_object_size is not None
                and len(data) > self.max_object_size
            ):
                raise ValueError(
                    f"object exceeds maximum size: "
                    f"{self.max_object_size}"
                )

            if self._large_objects.policy.is_large(data):
                manifest = self._large_objects.put(
                    key,
                    data,
                )

                self.catalog.put(
                    key,
                    manifest.to_dict(),
                )

                self.metrics.record_put()
                return

            self._put_direct(key, data)

            metadata = self._read_metadata(key)

            if metadata is None:
                raise ValueError(
                    f"missing metadata after put: {key}"
                )

            self.catalog.put(
                key,
                metadata.to_dict(),
            )

            self.metrics.record_put()

    @_count_storage_errors
    def get(self, key):
        self._ensure_open()
        with self._lock:
            manifest_key = (
                self._large_objects._manifest_key(key)
            )

            if self._exists_direct(manifest_key):
                data = self._large_objects.get(key)
                self.metrics.record_get()
                return data

            data = self._get_direct(key)
            self.metrics.record_get()
            return data

    @_count_storage_errors
    def exists(self, key):
        self._ensure_open()
        with self._lock:
            manifest_key = (
                self._large_objects._manifest_key(key)
            )

            if self._exists_direct(manifest_key):
                self.metrics.record_exists()
                return True

            result = self._exists_direct(key)
            self.metrics.record_exists()
            return result

    @_count_storage_errors
    def delete(self, key):
        self._ensure_open()
        with self._lock:
            manifest_key = (
                self._large_objects._manifest_key(key)
            )

            if self._exists_direct(manifest_key):
                deleted = self._large_objects.delete(key)

                if deleted:
                    self.catalog.delete(key)
                    self.metrics.record_delete()

                return deleted

            deleted = self._delete_direct(key)

            if deleted:
                self.metrics.record_delete()

            return deleted

    @_count_storage_errors
    def list_keys(self, prefix=""):
        self._ensure_open()
        with self._lock:
            return self.catalog.list_keys(
                prefix
            )
