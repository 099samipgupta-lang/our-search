from index_storage.chunk_manifest import ChunkManifest
from index_storage.chunks import ChunkStore
from index_storage.integrity import StorageIntegrity
from index_storage.object_policy import LargeObjectPolicy


class LargeObjectStore:

    MANIFEST_SUFFIX = ".manifest"

    def __init__(
        self,
        storage,
        chunk_size=ChunkStore.DEFAULT_CHUNK_SIZE,
        threshold=4 * 1024 * 1024,
    ):
        self.storage = storage

        self.policy = LargeObjectPolicy(
            threshold=threshold,
        )

        self.chunks = ChunkStore(
            storage,
            chunk_size=chunk_size,
        )

    def _manifest_key(self, key):
        return f"{key}{self.MANIFEST_SUFFIX}"

    def put(self, key, data):
        if not isinstance(data, bytes):
            raise TypeError(
                "data must be bytes"
            )

        if not self.policy.is_large(data):
            raise ValueError(
                "data does not meet large-object threshold"
            )

        checksum = StorageIntegrity.checksum(
            data
        )

        chunk_count = self.chunks.chunk_count(
            data
        )

        try:
            self.chunks.write_chunks(
                key,
                data,
            )

            manifest = ChunkManifest(
                key=key,
                size=len(data),
                chunk_size=self.chunks.chunk_size,
                chunk_count=chunk_count,
                checksum=checksum,
            )

            self.storage.put(
                self._manifest_key(key),
                manifest.to_bytes(),
            )

            return manifest

        except Exception:
            if chunk_count:
                self.chunks.delete_chunks(
                    key,
                    chunk_count,
                )

            raise

    def put_file(self, key, file_path, expected_size=None):
        """Publish a large file using bounded-memory chunks."""
        import hashlib
        import os

        file_size = os.path.getsize(file_path)
        if expected_size is not None and file_size != expected_size:
            raise ValueError("staged file size mismatch")
        if file_size < self.policy.threshold:
            raise ValueError("file does not meet large-object threshold")

        chunk_size = self.chunks.chunk_size
        chunk_count = (file_size + chunk_size - 1) // chunk_size
        digest = hashlib.sha256()
        written = 0

        try:
            with open(file_path, "rb") as source:
                for index in range(chunk_count):
                    chunk = source.read(chunk_size)
                    if not chunk:
                        raise ValueError("unexpected end of staged file")
                    digest.update(chunk)
                    self.storage.put(
                        self.chunks._chunk_key(key, index),
                        chunk,
                    )
                    written += 1

                if source.read(1):
                    raise ValueError("staged file grew during publication")

            from index_storage.chunk_manifest import ChunkManifest
            manifest = ChunkManifest(
                key=key,
                size=file_size,
                chunk_size=chunk_size,
                chunk_count=chunk_count,
                checksum=digest.hexdigest(),
            )
            self.storage.put(
                self._manifest_key(key),
                manifest.to_bytes(),
            )
            return manifest

        except Exception:
            if written:
                self.chunks.delete_chunks(key, written)
            raise

    def put_staged_chunks(
        self, key, chunk_paths, expected_size, expected_checksum
    ):
        """Publish staged chunk files with bounded memory and checksum verification."""
        import hashlib
        import os

        if not isinstance(expected_size, int) or expected_size <= 0:
            raise ValueError("expected_size must be positive")
        if expected_size < self.policy.threshold:
            raise ValueError("file does not meet large-object threshold")
        if not isinstance(expected_checksum, str):
            raise TypeError("expected_checksum must be a string")
        if self.storage.exists(self._manifest_key(key)):
            raise FileExistsError("object already exists")

        paths = list(chunk_paths)
        chunk_size = self.chunks.chunk_size
        expected_count = (expected_size + chunk_size - 1) // chunk_size
        if len(paths) != expected_count:
            raise ValueError("staged chunk count mismatch")

        digest = hashlib.sha256()
        actual_size = 0
        written = 0

        try:
            for index, path in enumerate(paths):
                expected_length = min(
                    chunk_size, expected_size - index * chunk_size
                )
                if os.path.getsize(path) != expected_length:
                    raise ValueError(f"staged chunk {index} has wrong size")

                with open(path, "rb") as source:
                    chunk = source.read(expected_length)
                    if len(chunk) != expected_length or source.read(1):
                        raise ValueError(f"could not read staged chunk {index}")

                digest.update(chunk)
                actual_size += len(chunk)
                self.storage.put(
                    self.chunks._chunk_key(key, index), chunk
                )
                written += 1

            if actual_size != expected_size:
                raise ValueError("staged upload size mismatch")
            if digest.hexdigest() != expected_checksum:
                raise ValueError("staged upload checksum mismatch")

            manifest = ChunkManifest(
                key=key,
                size=actual_size,
                chunk_size=chunk_size,
                chunk_count=expected_count,
                checksum=digest.hexdigest(),
            )
            self.storage.put(self._manifest_key(key), manifest.to_bytes())
            return manifest

        except Exception:
            if written:
                self.chunks.delete_chunks(key, written)
            raise

    def get(self, key):
        manifest_data = self.storage.get(
            self._manifest_key(key)
        )

        if manifest_data is None:
            return None

        manifest = ChunkManifest.from_bytes(
            manifest_data
        )

        data = self.chunks.read(
            key,
            manifest.chunk_count,
        )

        if len(data) != manifest.size:
            raise ValueError(
                f"large object size mismatch: {key}"
            )

        if not StorageIntegrity.verify(
            data,
            manifest.checksum,
        ):
            raise ValueError(
                f"large object checksum mismatch: {key}"
            )

        return data

    def delete(self, key):
        manifest_data = self.storage.get(
            self._manifest_key(key)
        )

        if manifest_data is None:
            return False

        manifest = ChunkManifest.from_bytes(
            manifest_data
        )

        self.chunks.delete_chunks(
            key,
            manifest.chunk_count,
        )

        self.storage.delete(
            self._manifest_key(key)
        )

        return True
