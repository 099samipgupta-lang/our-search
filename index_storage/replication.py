import threading

from index_storage.backend import IndexStorageBackend


class ReplicatedIndexStorage(IndexStorageBackend):
    """
    Replicated storage backend.

    The first backend is the primary. Additional backends are replicas.

    Writes are sent to every replica. A write succeeds only when the
    configured write quorum is reached.

    Reads try the primary first and automatically fail over to replicas.
    """

    def __init__(
        self,
        backends,
        write_quorum=None,
    ):
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
        self.replica_count = len(self.backends)

        if write_quorum is None:
            write_quorum = self.replica_count

        write_quorum = int(write_quorum)

        if write_quorum < 1:
            raise ValueError("write_quorum must be at least 1")

        if write_quorum > self.replica_count:
            raise ValueError(
                "write_quorum cannot exceed replica count"
            )

        self.write_quorum = write_quorum
        self._lock = threading.RLock()

    def _replicate(self, operation, key, data=None):
        successes = 0
        errors = []

        for backend in self.backends:
            try:
                if operation == "put":
                    backend.put(key, data)
                else:
                    backend.delete(key)

                successes += 1

            except Exception as exc:
                errors.append(exc)

        if successes < self.write_quorum:
            if errors:
                raise RuntimeError(
                    f"replication quorum failed: "
                    f"{successes}/{self.write_quorum} "
                    f"successful"
                ) from errors[0]

            raise RuntimeError(
                f"replication quorum failed: "
                f"{successes}/{self.write_quorum} successful"
            )

        return successes

    def put(self, key, data):
        with self._lock:
            self._replicate("put", key, data)

    def get(self, key):
        with self._lock:
            errors = []

            for backend in self.backends:
                try:
                    data = backend.get(key)

                    if data is not None:
                        return data

                except Exception as exc:
                    errors.append(exc)

            if errors:
                raise RuntimeError(
                    f"all replicated reads failed for key: {key}"
                ) from errors[0]

            return None

    def exists(self, key):
        with self._lock:
            for backend in self.backends:
                try:
                    if backend.exists(key):
                        return True
                except Exception:
                    continue

            return False

    def delete(self, key):
        with self._lock:
            return bool(
                self._replicate("delete", key)
            )

    def list_keys(self, prefix=""):
        with self._lock:
            keys = set()

            for backend in self.backends:
                try:
                    keys.update(
                        backend.list_keys(prefix)
                    )
                except Exception:
                    continue

            return sorted(keys)

    def health(self):
        with self._lock:
            healthy = 0
            states = []

            for index, backend in enumerate(self.backends):
                try:
                    backend.list_keys("")
                    healthy += 1
                    states.append(
                        {
                            "replica": index,
                            "healthy": True,
                        }
                    )
                except Exception as exc:
                    states.append(
                        {
                            "replica": index,
                            "healthy": False,
                            "error": str(exc),
                        }
                    )

            return {
                "replicas": self.replica_count,
                "healthy": healthy,
                "write_quorum": self.write_quorum,
                "states": states,
            }

    def repair_replica(self, target_index, source_index=0):
        with self._lock:
            if target_index == source_index:
                raise ValueError(
                    "target replica cannot equal source replica"
                )

            if not 0 <= target_index < self.replica_count:
                raise IndexError("invalid target replica index")

            if not 0 <= source_index < self.replica_count:
                raise IndexError("invalid source replica index")

            source = self.backends[source_index]
            target = self.backends[target_index]

            keys = source.list_keys("")

            for key in keys:
                data = source.get(key)

                if data is not None:
                    target.put(key, data)

            target_keys = set(target.list_keys(""))

            for key in target_keys:
                if key not in keys:
                    target.delete(key)

            return len(keys)
