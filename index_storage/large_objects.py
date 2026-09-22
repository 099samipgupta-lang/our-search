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
