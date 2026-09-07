import hashlib
import os

from search_index.persistence.segment_manager import SegmentManager
from result_presentation.document_store import DocumentStore

from indexing_pipeline.indexer import DocumentIndexer
from indexing_pipeline.version_state import VersionState
from indexing_pipeline.versioned_index import VersionedIndex
from indexing_pipeline.live_index import LiveVersionedIndex
from search_index.persistence.maintenance import IndexMaintenance
from search_index.persistence.auto_maintenance import AutomaticIndexMaintenance
from index_storage.repository import IndexStorageRepository


class CrawlIndexPipeline:
    def __init__(
        self,
        root="indexing_pipeline_data",
        storage=None,
        storage_repository=None,
    ):
        self.root = os.path.abspath(root)

        self.storage = storage

        self.storage_repository = (
            storage_repository
            if storage_repository is not None
            else (
                IndexStorageRepository(storage)
                if storage is not None
                else None
            )
        )

        self.index_root = os.path.join(
            self.root,
            "index",
        )

        self.documents_path = os.path.join(
            self.root,
            "documents.json",
        )

        self.version_state_path = os.path.join(
            self.root,
            "version_state.json",
        )

        self.segment_manager = SegmentManager(
            self.index_root,
            storage=self.storage,
            storage_repository=self.storage_repository,
        )

        if self.storage_repository is not None:

            self.document_store = (
                self.storage_repository.load_documents()
            )

            self.version_state = (
                self.storage_repository.load_version_state()
            )

        else:

            self.document_store = DocumentStore(
                self.documents_path
            )

            self.document_store.load()

            self.version_state = VersionState(
                self.version_state_path
            )

        self.indexer = DocumentIndexer()

        self.search_index = LiveVersionedIndex(
            self.segment_manager,
            self.indexer.index,
            self.version_state,
        )

        self.maintenance = IndexMaintenance(
            self.segment_manager
        )

        self.auto_maintenance = AutomaticIndexMaintenance(
            self.maintenance,
            max_segments=10,
        )

        self.stats = {
            "processed": 0,
            "indexed": 0,
            "updated": 0,
            "skipped": 0,
            "removed": 0,
        }

    @staticmethod
    def _hash_text(text):
        return hashlib.sha256(
            text.encode("utf-8")
        ).hexdigest()

    def add_document(
        self,
        document_id,
        url,
        title,
        text,
        canonical_url="",
    ):
        content_hash = self._hash_text(text)

        current = self.version_state.get(
            document_id
        )

        if (
            current
            and current.get("state") == "active"
            and current.get("content_hash") == content_hash
        ):
            existing = self.indexer.get_document(
                document_id
            )

            self.stats["skipped"] += 1

            return {
                "action": "skipped",
                "document": (
                    existing.to_dict()
                    if hasattr(existing, "to_dict")
                    else existing
                ),
            }

        is_update = (
            current is not None
            and current.get("state") == "active"
        )

        if is_update:
            self.indexer.remove_document(
                document_id
            )
            self.stats["updated"] += 1
        else:
            self.stats["indexed"] += 1

        document = self.indexer.index_document(
            document_id=document_id,
            url=url,
            title=title,
            text=text,
            canonical_url=canonical_url,
        )

        self.version_state.activate(
            document_id,
            content_hash,
        )

        self.document_store.add_document(
            document_id=document_id,
            title=title,
            url=url,
            text=text,
        )

        self.stats["processed"] += 1

        return {
            "action": (
                "updated"
                if is_update
                else "indexed"
            ),
            "document": document.to_dict(),
            "version": self.version_state.get(
                document_id
            ),
        }

    def remove_document(self, document_id):
        current = self.version_state.get(
            document_id
        )

        if (
            current is None
            or current.get("state") == "deleted"
        ):
            return False

        self.indexer.remove_document(
            document_id
        )

        self.version_state.tombstone(
            document_id
        )

        self.document_store.documents.pop(
            document_id,
            None,
        )

        self.stats["removed"] += 1

        return True

    def flush(self):

        if self.storage_repository is not None:

            self.storage_repository.save_documents(
                self.document_store
            )

        else:

            self.document_store.save()

        segment_id = self.indexer.build_segment(
            self.segment_manager
        )

        if self.storage_repository is not None:

            self.storage_repository.save_version_state(
                self.version_state
            )

        else:

            self.version_state.save()

        if segment_id is not None:
            self.auto_maintenance.check()

        return segment_id

    def index_document(self, document_id, url, title, text, canonical_url=""):
        result = self.add_document(document_id=document_id, url=url, title=title, text=text, canonical_url=canonical_url)
        segment_id = None
        if result.get("action") != "skipped":
            segment_id = self.flush()
        result["segment_id"] = segment_id
        return result

    def status(self):
        return {
            "stats": dict(self.stats),
            "indexed_documents":
                self.version_state.document_count(),
            "deleted_documents":
                self.version_state.deleted_count(),
            "stored_documents":
                self.document_store.count(),
            "segments":
                self.segment_manager.segment_count(),
        }

    def close(self):

        if self.storage_repository is not None:

            self.storage_repository.save_documents(
                self.document_store
            )

            self.storage_repository.save_version_state(
                self.version_state
            )

        else:

            self.document_store.save()
            self.version_state.save()

        self.segment_manager.close()

