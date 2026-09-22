import hashlib
import threading

from index_storage.backend import IndexStorageBackend


class ShardedIndexStorage(IndexStorageBackend):
    """
    Routes keys across multiple storage backends using deterministic hashing.
    """

    def __init__(self, backends):
        if not isinstance(backends, (list, tuple)):
            raise TypeError("backends must be a list or tuple")

        if not backends:
            raise ValueError("at least one backend is required")

        if any(
            not isinstance(backend, IndexStorageBackend)
            for backend in backends
        ):
            raise TypeError(
                "all backends must implement IndexStorageBackend"
            )

        self.backends = list(backends)
        self.shard_count = len(self.backends)
        self._lock = threading.RLock()

    def _shard_index(self, key):
        digest = hashlib.sha256(
            key.encode("utf-8")
        ).digest()

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
        )

        return value % self.shard_count

    def _backend_for_key(self, key):
        return self.backends[self._shard_index(key)]

    def put(self, key, data):
        with self._lock:
            self._backend_for_key(key).put(key, data)

    def get(self, key):
        with self._lock:
            return self._backend_for_key(key).get(key)

    def exists(self, key):
        with self._lock:
            return self._backend_for_key(key).exists(key)

    def delete(self, key):
        with self._lock:
            return self._backend_for_key(key).delete(key)

    def list_keys(self, prefix=""):
        with self._lock:
            keys = set()

            for backend in self.backends:
                keys.update(backend.list_keys(prefix))

            return sorted(keys)

    def shard_for_key(self, key):
        with self._lock:
            return self._shard_index(key)

    def health(self):
        with self._lock:
            states = []

            for index, backend in enumerate(self.backends):
                try:
                    backend.list_keys("")
                    states.append(
                        {
                            "shard": index,
                            "healthy": True,
                        }
                    )
                except Exception as exc:
                    states.append(
                        {
                            "shard": index,
                            "healthy": False,
                            "error": str(exc),
                        }
                    )

            return {
                "shards": self.shard_count,
                "states": states,
            }

    def rebalance(self, new_backends):
        if not isinstance(new_backends, (list, tuple)):
            raise TypeError("new_backends must be a list or tuple")

        if not new_backends:
            raise ValueError("at least one backend is required")

        if any(
            not isinstance(backend, IndexStorageBackend)
            for backend in new_backends
        ):
            raise TypeError(
                "all backends must implement IndexStorageBackend"
            )

        with self._lock:
            old_backends = self.backends
            old_shard_count = self.shard_count

            all_keys = set()

            for backend in old_backends:
                all_keys.update(backend.list_keys(""))

            self.backends = list(new_backends)
            self.shard_count = len(new_backends)

            try:
                for key in sorted(all_keys):
                    source = old_backends[
                        self._shard_index_with_count(
                            key,
                            old_shard_count,
                        )
                    ]

                    data = source.get(key)

                    if data is None:
                        continue

                    target = self._backend_for_key(key)

                    target.put(key, data)

                    if target is not source:
                        source.delete(key)

            except Exception:
                self.backends = old_backends
                self.shard_count = old_shard_count
                raise

            return len(all_keys)

    @staticmethod
    def _shard_index_with_count(key, shard_count):
        digest = hashlib.sha256(
            key.encode("utf-8")
        ).digest()

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
        )

        return value % shard_count
