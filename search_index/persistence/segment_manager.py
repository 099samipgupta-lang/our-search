import json
import os
import tempfile
import threading
import time

from search_index.persistence.segment import IndexSegment
from search_index.persistence.posting_stats import PersistentPostingStats


class SegmentManager:

    MANIFEST_VERSION = 1

    def __init__(
        self,
        root="search_index_data",
        storage=None,
        storage_repository=None
    ):

        self.root = os.path.abspath(root)

        self.storage = storage
        self.storage_repository = storage_repository

        self.segments_dir = os.path.join(
            self.root,
            "segments"
        )

        self.manifest_path = os.path.join(
            self.root,
            "manifest.json"
        )

        os.makedirs(
            self.segments_dir,
            exist_ok=True
        )

        self.segments = []

        self.next_segment_number = 1

        self.posting_stats = PersistentPostingStats(
            os.path.join(
                self.root,
                "posting_stats.json"
            )
        )

        self.posting_stats.load()

        # --------------------------------------------------
        # In-memory segment cache
        #
        # Segments are immutable after they are written.
        # Keeping loaded segments in RAM prevents every search
        # term from downloading the same segment repeatedly
        # from persistent storage.
        # --------------------------------------------------

        self._segment_cache = {}

        self._segment_cache_lock = threading.RLock()

        self.load_manifest()

    def _segment_path(
        self,
        segment_id
    ):

        return os.path.join(
            self.segments_dir,
            f"{segment_id}.segment"
        )

    def _new_segment_id(self):

        segment_id = (
            f"segment-"
            f"{self.next_segment_number:08d}"
        )

        self.next_segment_number += 1

        return segment_id

    # --------------------------------------------------
    # Segment cache
    # --------------------------------------------------

    def _get_cached_segment(
        self,
        segment_id
    ):

        with self._segment_cache_lock:
            return self._segment_cache.get(
                segment_id
            )

    def _cache_segment(
        self,
        segment_id,
        segment
    ):

        with self._segment_cache_lock:
            self._segment_cache[
                segment_id
            ] = segment

        return segment

    def _remove_cached_segment(
        self,
        segment_id
    ):

        with self._segment_cache_lock:
            self._segment_cache.pop(
                segment_id,
                None
            )

    def _clear_segment_cache(self):

        with self._segment_cache_lock:
            self._segment_cache.clear()

    def cache_size(self):

        with self._segment_cache_lock:
            return len(
                self._segment_cache
            )

    # --------------------------------------------------
    # Segment persistence
    # --------------------------------------------------

    def flush_index(
        self,
        index
    ):

        segment_id = (
            self._new_segment_id()
        )

        path = self._segment_path(
            segment_id
        )

        segment = IndexSegment(
            path
        )

        segment.build(
            segment_id,
            index.get_state()
        )

        if self.storage_repository is not None:
            self.storage_repository.save_segment(
                segment
            )
        else:
            segment.save()

        for term, postings in segment.data.get(
            "terms",
            {}
        ).items():

            self.posting_stats.record(
                segment_id,
                term,
                len(postings)
            )

        if self.storage_repository is not None:
            self.storage_repository.save_posting_stats(
                self.posting_stats
            )
        else:
            self.posting_stats.save()

        self.segments.append(
            {
                "segment_id":
                    segment_id,

                "created_at":
                    time.time(),

                "documents":
                    segment.document_count(),

                "vocabulary":
                    segment.vocabulary_size()
            }
        )

        # The segment already exists in memory, so cache it
        # immediately. Future searches do not need to reload it.
        self._cache_segment(
            segment_id,
            segment
        )

        self.save_manifest()

        return segment_id

    def load_segment(
        self,
        segment_id
    ):

        cached = self._get_cached_segment(
            segment_id
        )

        if cached is not None:
            return cached

        path = self._segment_path(
            segment_id
        )

        segment = IndexSegment(
            path
        )

        if self.storage_repository is not None:
            segment = self.storage_repository.load_segment(
                segment_id
            )
        else:
            segment.load()

        return self._cache_segment(
            segment_id,
            segment
        )

    # --------------------------------------------------
    # Segment metadata
    # --------------------------------------------------

    def list_segments(self):

        return [
            dict(segment)
            for segment in self.segments
        ]

    def segment_count(self):

        return len(
            self.segments
        )

    def total_documents(self):

        total = 0

        for metadata in self.segments:

            total += int(
                metadata.get(
                    "documents",
                    0
                )
            )

        return total

    def total_vocabulary_entries(self):

        total = 0

        for metadata in self.segments:

            total += int(
                metadata.get(
                    "vocabulary",
                    0
                )
            )

        return total

    # --------------------------------------------------
    # Retrieval
    # --------------------------------------------------

    def estimate_document_frequency(
        self,
        term
    ):

        document_ids = set()

        for metadata in self.segments:

            segment_id = metadata["segment_id"]

            value = self.posting_stats.get(
                segment_id,
                term
            )

            if value is None:
                continue

            segment = self.load_segment(
                segment_id
            )

            postings = segment.get_postings(
                term
            )

            document_ids.update(
                postings.keys()
            )

        return len(
            document_ids
        )

    def get_postings(
        self,
        term
    ):

        merged = {}

        for metadata in self.segments:

            segment = self.load_segment(
                metadata["segment_id"]
            )

            postings = segment.get_postings(
                term
            )

            for document_id, positions in (
                postings.items()
            ):

                merged[
                    document_id
                ] = positions

        return merged

    def get_document_info(
        self,
        document_id
    ):

        for metadata in reversed(
            self.segments
        ):

            segment = self.load_segment(
                metadata["segment_id"]
            )

            info = (
                segment.get_document_info(
                    document_id
                )
            )

            if info is not None:
                return info

        return None

    # --------------------------------------------------
    # Segment merging
    # --------------------------------------------------

    def merge_segments(
        self,
        segment_ids=None
    ):

        if segment_ids is None:

            segment_ids = [
                item["segment_id"]
                for item in self.segments
            ]

        segment_ids = list(
            segment_ids
        )

        if len(segment_ids) < 2:
            return None

        selected = [
            item
            for item in self.segments
            if item["segment_id"] in segment_ids
        ]

        if len(selected) < 2:
            raise ValueError(
                "At least two valid segments are required"
            )

        merged_terms = {}
        merged_documents = {}

        for metadata in selected:

            segment = self.load_segment(
                metadata["segment_id"]
            )

            for term, postings in (
                segment.data[
                    "terms"
                ].items()
            ):

                destination = merged_terms.setdefault(
                    term,
                    {}
                )

                for document_id, positions in (
                    postings.items()
                ):

                    destination[
                        document_id
                    ] = positions

            for document_id, info in (
                segment.data[
                    "documents"
                ].items()
            ):

                merged_documents[
                    document_id
                ] = info

        new_id = self._new_segment_id()

        new_path = self._segment_path(
            new_id
        )

        merged_segment = IndexSegment(
            new_path
        )

        merged_segment.build(
            new_id,
            {
                "index":
                    merged_terms,

                "documents":
                    merged_documents
            }
        )

        if self.storage_repository is not None:
            self.storage_repository.save_segment(
                merged_segment
            )
        else:
            merged_segment.save()

        for term, postings in merged_segment.data.get(
            "terms",
            {}
        ).items():

            self.posting_stats.record(
                new_id,
                term,
                len(postings)
            )

        for old_id in segment_ids:

            self.posting_stats.remove_segment(
                old_id
            )

        if self.storage_repository is not None:
            self.storage_repository.save_posting_stats(
                self.posting_stats
            )
        else:
            self.posting_stats.save()

        selected_ids = set(
            segment_ids
        )

        self.segments = [
            item
            for item in self.segments
            if item["segment_id"] not in selected_ids
        ]

        self.segments.append(
            {
                "segment_id":
                    new_id,

                "created_at":
                    time.time(),

                "documents":
                    merged_segment.document_count(),

                "vocabulary":
                    merged_segment.vocabulary_size()
            }
        )

        # Cache the newly-created merged segment.
        self._cache_segment(
            new_id,
            merged_segment
        )

        # Old segments are no longer part of the active manifest.
        for old_id in selected_ids:
            self._remove_cached_segment(
                old_id
            )

        self.save_manifest()

        for old_id in selected_ids:

            if self.storage_repository is not None:

                self.storage_repository.delete_segment(
                    old_id
                )

            else:

                old_path = self._segment_path(
                    old_id
                )

                if os.path.exists(
                    old_path
                ):

                    os.remove(
                        old_path
                    )

        return new_id

    # --------------------------------------------------
    # Manifest
    # --------------------------------------------------

    def save_manifest(self):

        manifest = {
            "format_version":
                self.MANIFEST_VERSION,

            "next_segment_number":
                self.next_segment_number,

            "segments":
                self.segments
        }

        manifest_bytes = json.dumps(
            manifest,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")

        if self.storage_repository is not None:

            self.storage_repository.save_manifest(
                manifest_bytes
            )

            return

        directory = os.path.dirname(
            self.manifest_path
        )

        fd, temporary_path = tempfile.mkstemp(
            prefix=".manifest_",
            suffix=".tmp",
            dir=directory
        )

        try:

            with os.fdopen(
                fd,
                "w",
                encoding="utf-8"
            ) as file:

                json.dump(
                    manifest,
                    file,
                    ensure_ascii=False,
                    indent=2
                )

                file.flush()

                os.fsync(
                    file.fileno()
                )

            os.replace(
                temporary_path,
                self.manifest_path
            )

        except Exception:

            try:
                os.unlink(
                    temporary_path
                )
            except OSError:
                pass

            raise

    def load_manifest(self):

        if self.storage_repository is not None:

            payload = (
                self.storage_repository.load_manifest()
            )

            if payload is None:
                return

            manifest = json.loads(
                payload.decode("utf-8")
            )

        else:

            if not os.path.exists(
                self.manifest_path
            ):
                return

            with open(
                self.manifest_path,
                "r",
                encoding="utf-8"
            ) as file:

                manifest = json.load(
                    file
                )

        version = manifest.get(
            "format_version"
        )

        if version != self.MANIFEST_VERSION:

            raise ValueError(
                f"Unsupported manifest format: {version}"
            )

        self.next_segment_number = int(
            manifest.get(
                "next_segment_number",
                1
            )
        )

        self.segments = list(
            manifest.get(
                "segments",
                []
            )
        )

        # The manifest is authoritative. Any previous cache must
        # be discarded when the manifest is reloaded.
        self._clear_segment_cache()

    def close(self):

        # Segment metadata is persisted whenever segments are
        # created, merged, or otherwise changed.
        #
        # Cached segments are intentionally kept only in process
        # memory and disappear when this process exits.
        return None
