import os


class StorageCompactor:

    INTERNAL_DIRECTORIES = {
        ".integrity",
        ".metadata",
    }

    TEMP_SUFFIXES = (
        ".tmp",
        ".recovery.tmp",
    )

    def __init__(self, storage):
        self.storage = storage

    def _remove_stale_temp_files(self):
        removed = 0

        for root, directories, files in os.walk(self.storage.root):
            directories[:] = [
                directory
                for directory in directories
                if directory not in self.INTERNAL_DIRECTORIES
            ]

            for filename in files:
                if not filename.endswith(self.TEMP_SUFFIXES):
                    continue

                path = os.path.join(root, filename)

                if os.path.isfile(path):
                    os.remove(path)
                    removed += 1

        return removed

    def _remove_orphan_integrity(self):
        removed = 0

        if not os.path.exists(self.storage.integrity_root):
            return removed

        for root, directories, files in os.walk(
            self.storage.integrity_root
        ):
            for filename in files:
                if not filename.endswith(".sha256"):
                    continue

                full_path = os.path.join(
                    root,
                    filename,
                )

                relative = os.path.relpath(
                    full_path,
                    self.storage.integrity_root,
                )

                key = relative[:-len(".sha256")]

                object_path = self.storage._path(key)

                if not os.path.exists(object_path):
                    os.remove(full_path)
                    removed += 1

        return removed

    def _remove_orphan_metadata(self):
        removed = 0

        if not os.path.exists(self.storage.metadata_root):
            return removed

        for root, directories, files in os.walk(
            self.storage.metadata_root
        ):
            for filename in files:
                if not filename.endswith(".json"):
                    continue

                full_path = os.path.join(
                    root,
                    filename,
                )

                relative = os.path.relpath(
                    full_path,
                    self.storage.metadata_root,
                )

                key = relative[:-len(".json")]

                object_path = self.storage._path(key)

                if not os.path.exists(object_path):
                    os.remove(full_path)
                    removed += 1

        return removed

    def _remove_empty_directories(self):
        removed = 0

        for root, directories, files in os.walk(
            self.storage.root,
            topdown=False,
        ):
            for directory in directories:
                if directory in self.INTERNAL_DIRECTORIES:
                    continue

                path = os.path.join(
                    root,
                    directory,
                )

                try:
                    os.rmdir(path)
                except OSError:
                    continue

                removed += 1

        return removed

    def _ensure_wal_clean(self):
        records = self.storage.wal.recover()

        if records:
            raise RuntimeError(
                "cannot compact storage with "
                "pending WAL records"
            )

    def compact(self):
        """
        Compact the storage directory.

        Returns a report describing cleanup performed.
        """

        with self.storage._lock:
            self._ensure_wal_clean()

            removed_temp_files = (
                self._remove_stale_temp_files()
            )

            removed_orphan_integrity = (
                self._remove_orphan_integrity()
            )

            removed_orphan_metadata = (
                self._remove_orphan_metadata()
            )

            removed_empty_directories = (
                self._remove_empty_directories()
            )

            return {
                "removed_temp_files": removed_temp_files,
                "removed_orphan_integrity": (
                    removed_orphan_integrity
                ),
                "removed_orphan_metadata": (
                    removed_orphan_metadata
                ),
                "removed_empty_directories": (
                    removed_empty_directories
                ),
            }
