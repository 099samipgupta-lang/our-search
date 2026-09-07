from search_index.persistence.segment import IndexSegment
from search_index.persistence.posting_stats import PersistentPostingStats
from result_presentation.document_store import DocumentStore
from indexing_pipeline.version_state import VersionState


class IndexStorageRepository:

    def __init__(
        self,
        storage,
        root="index"
    ):
        self.storage = storage
        self.root = root.strip("/")

    def _key(self, name):
        if not name:
            raise ValueError(
                "storage name is required"
            )

        if self.root:
            return f"{self.root}/{name}"

        return name

    # --------------------------------------------------
    # Manifest
    # --------------------------------------------------

    def save_manifest(
        self,
        manifest
    ):
        if not isinstance(manifest, bytes):
            raise TypeError(
                "manifest must be bytes"
            )

        key = self._key(
            "manifest.json"
        )

        self.storage.put(
            key,
            manifest
        )

        return key

    def load_manifest(self):
        return self.storage.get(
            self._key(
                "manifest.json"
            )
        )

    def manifest_exists(self):
        return self.storage.exists(
            self._key(
                "manifest.json"
            )
        )

    # --------------------------------------------------
    # Segments
    # --------------------------------------------------

    def save_segment(
        self,
        segment
    ):
        key = self._key(
            f"segments/{segment.data['segment_id']}.segment"
        )

        segment.save_to_storage(
            self.storage,
            key
        )

        return key

    def load_segment(
        self,
        segment_id
    ):
        segment = IndexSegment(
            "unused.segment"
        )

        segment.load_from_storage(
            self.storage,
            self._key(
                f"segments/{segment_id}.segment"
            )
        )

        return segment

    def delete_segment(
        self,
        segment_id
    ):
        return self.storage.delete(
            self._key(
                f"segments/{segment_id}.segment"
            )
        )

    def list_segments(self):
        prefix = self._key(
            "segments"
        )

        keys = self.storage.list_keys(
            prefix
        )

        return [
            key
            for key in keys
            if key.endswith(".segment")
        ]

    # --------------------------------------------------
    # Posting statistics
    # --------------------------------------------------

    def save_posting_stats(
        self,
        posting_stats
    ):
        key = self._key(
            "posting_stats.json"
        )

        posting_stats.save_to_storage(
            self.storage,
            key
        )

        return key

    def load_posting_stats(self):
        posting_stats = PersistentPostingStats(
            "unused.json"
        )

        posting_stats.load_from_storage(
            self.storage,
            self._key(
                "posting_stats.json"
            )
        )

        return posting_stats

    # --------------------------------------------------
    # Documents
    # --------------------------------------------------

    def save_documents(
        self,
        document_store
    ):
        key = self._key(
            "documents.json"
        )

        document_store.save_to_storage(
            self.storage,
            key
        )

        return key

    def load_documents(self):
        document_store = DocumentStore(
            "unused.json"
        )

        document_store.load_from_storage(
            self.storage,
            self._key(
                "documents.json"
            )
        )

        return document_store

    # --------------------------------------------------
    # Version state
    # --------------------------------------------------

    def save_version_state(
        self,
        version_state
    ):
        key = self._key(
            "version_state.json"
        )

        version_state.save_to_storage(
            self.storage,
            key
        )

        return key

    def load_version_state(self):
        version_state = VersionState(
            "unused.json"
        )

        version_state.load_from_storage(
            self.storage,
            self._key(
                "version_state.json"
            )
        )

        return version_state
