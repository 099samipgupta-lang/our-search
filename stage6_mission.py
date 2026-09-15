import hashlib
import json
import os
import shutil
import time
from pathlib import Path

from indexing_pipeline import CrawlIndexPipeline


STAGE6_ROOT = "stage6_production_data"
SHARD_COUNT = 8


class DistributedIndex:
    """
    Stage 6 distributed durable index.

    Documents are deterministically assigned to one shard using:
        SHA-256(document_id) % shard_count

    Each shard is an independent existing CrawlIndexPipeline.
    """

    FORMAT_VERSION = 1

    def __init__(
        self,
        root=STAGE6_ROOT,
        shard_count=SHARD_COUNT,
    ):
        if int(shard_count) <= 0:
            raise ValueError("shard_count must be positive")

        self.root = os.path.abspath(root)
        self.shard_count = int(shard_count)

        os.makedirs(self.root, exist_ok=True)

        self.shards = {}

        for shard_id in range(self.shard_count):
            shard_root = os.path.join(
                self.root,
                f"shard-{shard_id:04d}",
            )

            self.shards[shard_id] = CrawlIndexPipeline(
                shard_root
            )

    def shard_for_document(self, document_id):
        if not isinstance(document_id, str):
            raise TypeError("document_id must be a string")

        digest = hashlib.sha256(
            document_id.encode("utf-8")
        ).digest()

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return value % self.shard_count

    def pipeline_for_document(self, document_id):
        return self.shards[
            self.shard_for_document(document_id)
        ]

    def add_document(
        self,
        document_id,
        url,
        title,
        text,
        canonical_url="",
        metadata=None,
    ):
        pipeline = self.pipeline_for_document(
            document_id
        )

        return pipeline.add_document(
            document_id=document_id,
            url=url,
            title=title,
            text=text,
            canonical_url=canonical_url,
            metadata=metadata,
        )

    def remove_document(self, document_id):
        pipeline = self.pipeline_for_document(
            document_id
        )

        return pipeline.remove_document(
            document_id
        )

    def flush(self):
        segment_ids = {}

        for shard_id, pipeline in self.shards.items():
            segment_id = pipeline.flush()

            if segment_id is not None:
                segment_ids[shard_id] = segment_id

        return segment_ids

    def get_postings(self, term):
        merged = {}

        for pipeline in self.shards.values():
            postings = pipeline.search_index.get_postings(
                term
            )

            for document_id, positions in postings.items():
                merged[document_id] = positions

        return merged

    def get_document_info(self, document_id):
        pipeline = self.pipeline_for_document(
            document_id
        )

        return pipeline.search_index.get_document_info(
            document_id
        )

    def document_count(self):
        return sum(
            pipeline.search_index.document_count()
            for pipeline in self.shards.values()
        )

    def deleted_count(self):
        return sum(
            pipeline.version_state.deleted_count()
            for pipeline in self.shards.values()
        )

    def segment_count(self):
        return sum(
            pipeline.segment_manager.segment_count()
            for pipeline in self.shards.values()
        )

    def vocabulary_size(self):
        return sum(
            pipeline.search_index.total_vocabulary_entries()
            for pipeline in self.shards.values()
        )

    def shard_distribution(self):
        return {
            shard_id: pipeline.search_index.document_count()
            for shard_id, pipeline in self.shards.items()
        }

    def status(self):
        return {
            "format_version": self.FORMAT_VERSION,
            "root": self.root,
            "shard_count": self.shard_count,
            "document_count": self.document_count(),
            "deleted_count": self.deleted_count(),
            "segment_count": self.segment_count(),
            "vocabulary_entries": self.vocabulary_size(),
            "shards": {
                str(shard_id): pipeline.status()
                for shard_id, pipeline in self.shards.items()
            },
        }

    def validate_distribution(self):
        distribution = self.shard_distribution()

        if len(distribution) != self.shard_count:
            raise AssertionError(
                "not all shards are represented"
            )

        total = sum(distribution.values())

        if total != self.document_count():
            raise AssertionError(
                "shard document counts do not equal global count"
            )

        return distribution

    def save_manifest(self):
        manifest = {
            "format_version": self.FORMAT_VERSION,
            "shard_count": self.shard_count,
            "shards": [
                {
                    "shard_id": shard_id,
                    "path": f"shard-{shard_id:04d}",
                    "documents": pipeline.search_index.document_count(),
                    "segments": pipeline.segment_manager.segment_count(),
                }
                for shard_id, pipeline in self.shards.items()
            ],
        }

        path = os.path.join(
            self.root,
            "distributed_manifest.json",
        )

        temporary = path + ".tmp"

        with open(
            temporary,
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                manifest,
                file,
                ensure_ascii=False,
                indent=2,
            )
            file.flush()
            os.fsync(file.fileno())

        os.replace(
            temporary,
            path,
        )

        return path

    def close(self):
        for pipeline in self.shards.values():
            pipeline.close()


def create_documents(count):
    for number in range(count):
        document_id = f"DOC-{number:08d}"

        yield {
            "document_id": document_id,
            "url": f"https://example.com/page/{number}",
            "title": f"Search Technology Page {number}",
            "text": (
                f"Search technology information document {number}. "
                "Our search engine indexes information across the web. "
                "Distributed indexing creates a durable searchable "
                "representation of public web documents."
            ),
        }


def clean_root():
    if os.path.exists(STAGE6_ROOT):
        shutil.rmtree(STAGE6_ROOT)


def gate_shard_architecture():
    print("\n=== 6.1 DISTRIBUTED SHARD ARCHITECTURE ===")

    index = DistributedIndex(
        root=STAGE6_ROOT,
        shard_count=SHARD_COUNT,
    )

    seen = {}

    for number in range(1000):
        document_id = f"DOC-{number:08d}"
        shard = index.shard_for_document(
            document_id
        )
        seen.setdefault(shard, 0)
        seen[shard] += 1

    print("Shard distribution:", seen)

    assert len(seen) == SHARD_COUNT
    assert sum(seen.values()) == 1000

    index.close()

    print("PASS — deterministic document sharding")


def gate_ingestion_and_persistence():
    print("\n=== 6.2 INDEX INGESTION + DURABILITY ===")

    index = DistributedIndex(
        root=STAGE6_ROOT,
        shard_count=SHARD_COUNT,
    )

    count = 5000

    for document in create_documents(count):
        result = index.add_document(**document)

        assert result["action"] == "indexed"

    segment_ids = index.flush()
    index.save_manifest()

    print("Indexed:", index.document_count())
    print("Segments:", index.segment_count())
    print("Flush result:", segment_ids)

    assert index.document_count() == count
    assert len(segment_ids) == SHARD_COUNT
    assert index.segment_count() == SHARD_COUNT

    distribution = index.validate_distribution()

    print("Distribution:", distribution)

    index.close()


def gate_recovery_and_search():
    print("\n=== 6.3 RESTART RECOVERY + GLOBAL INDEX ===")

    recovered = DistributedIndex(
        root=STAGE6_ROOT,
        shard_count=SHARD_COUNT,
    )

    print("Recovered documents:", recovered.document_count())
    print("Recovered segments:", recovered.segment_count())

    assert recovered.document_count() == 5000
    assert recovered.segment_count() == SHARD_COUNT

    postings = recovered.get_postings(
        "technology"
    )

    print(
        "Global technology postings:",
        len(postings),
    )

    assert len(postings) == 5000

    information = recovered.get_postings(
        "information"
    )

    print(
        "Global information postings:",
        len(information),
    )

    assert len(information) == 5000

    shard_id = recovered.shard_for_document("DOC-00002500")
    document = recovered.shards[shard_id].document_store.get(
        "DOC-00002500"
    )

    assert document is not None
    assert document["url"] == (
        "https://example.com/page/2500"
    )

    recovered.close()

    print("PASS — distributed index survives restart")


def gate_updates_and_deletes():
    print("\n=== 6.4 UPDATE + DELETE CONSISTENCY ===")

    index = DistributedIndex(
        root=STAGE6_ROOT,
        shard_count=SHARD_COUNT,
    )

    updated = index.add_document(
        document_id="DOC-00002500",
        url="https://example.com/page/2500",
        title="Updated Search Technology",
        text=(
            "Updated document contains "
            "distributed retrieval information."
        ),
    )

    assert updated["action"] == "updated"
    assert updated["version"]["version"] == 2

    removed = index.remove_document(
        "DOC-00003000"
    )

    assert removed is True

    index.flush()

    updated_postings = index.get_postings(
        "distributed"
    )

    assert "DOC-00002500" in updated_postings

    deleted_postings = index.get_postings(
        "technology"
    )

    assert "DOC-00003000" not in deleted_postings

    assert index.document_count() == 4999

    index.close()

    recovered = DistributedIndex(
        root=STAGE6_ROOT,
        shard_count=SHARD_COUNT,
    )

    assert recovered.document_count() == 4999

    assert (
        recovered.get_document_info(
            "DOC-00003000"
        )
        is None
    )

    assert (
        "DOC-00003000"
        not in recovered.get_postings("technology")
    )

    assert (
        "DOC-00002500"
        in recovered.get_postings("distributed")
    )

    recovered.close()

    print("PASS — update/delete state survives restart")


def gate_stress():
    print("\n=== 6.5 DISTRIBUTED INDEX STRESS ===")

    clean_root()

    index = DistributedIndex(
        root=STAGE6_ROOT,
        shard_count=SHARD_COUNT,
    )

    count = 20000

    started = time.time()

    for document in create_documents(count):
        result = index.add_document(**document)

        if result["action"] != "indexed":
            raise AssertionError(
                f"unexpected action: {result['action']}"
            )

    index.flush()
    index.save_manifest()

    elapsed = time.time() - started

    throughput = (
        count / elapsed
        if elapsed > 0
        else 0
    )

    print("Stress documents:", count)
    print("Elapsed:", round(elapsed, 3), "s")
    print("Throughput:", round(throughput, 2), "docs/s")
    print("Segments:", index.segment_count())
    print("Distribution:", index.shard_distribution())

    assert index.document_count() == count
    assert index.segment_count() == SHARD_COUNT

    for shard_id, value in index.shard_distribution().items():
        assert value > 0, (
            f"shard {shard_id} received no documents"
        )

    index.close()

    recovered = DistributedIndex(
        root=STAGE6_ROOT,
        shard_count=SHARD_COUNT,
    )

    assert recovered.document_count() == count

    postings = recovered.get_postings(
        "information"
    )

    assert len(postings) == count

    recovered.close()

    print("PASS — 20,000-document distributed restart stress")


def final_gate():
    print("\n=== STAGE 6 FINAL GATE ===")

    required_paths = [
        "search_index/inverted_index.py",
        "search_index/persistence/segment.py",
        "search_index/persistence/segment_manager.py",
        "indexing_pipeline/indexer.py",
        "indexing_pipeline/pipeline.py",
        "indexing_pipeline/version_state.py",
        "index_storage/repository.py",
        "stage6_mission.py",
    ]

    for path in required_paths:
        assert os.path.exists(path), (
            f"missing required production file: {path}"
        )

    index = DistributedIndex(
        root=STAGE6_ROOT,
        shard_count=SHARD_COUNT,
    )

    assert index.document_count() == 20000
    assert index.segment_count() == SHARD_COUNT

    distribution = index.validate_distribution()

    assert len(distribution) == SHARD_COUNT
    assert all(
        value > 0
        for value in distribution.values()
    )

    technology = index.get_postings(
        "technology"
    )

    information = index.get_postings(
        "information"
    )

    assert len(technology) == 20000
    assert len(information) == 20000

    deleted_test_id = "DOC-00003000"

    # The stress dataset was freshly created after the update/delete
    # gate, so this document must exist in the fresh final dataset.
    assert (
        index.get_document_info(
            deleted_test_id
        )
        is not None
    )

    index.close()

    print("PASS — production architecture present")
    print("PASS — 8/8 shards active")
    print("PASS — 20,000 documents recovered")
    print("PASS — global postings verified")
    print("PASS — distributed consistency verified")
    print()
    print("RESULT: PASS")
    print("STAGE 6: 100% COMPLETE")
    print("READY FOR STAGE 7")


def main():
    print("=" * 70)
    print("OUR SEARCH — STAGE 6 INTEGRATED MISSION")
    print("=" * 70)

    clean_root()

    gate_shard_architecture()
    gate_ingestion_and_persistence()
    gate_recovery_and_search()
    gate_updates_and_deletes()
    gate_stress()
    final_gate()

    print("=" * 70)
    print("STAGE 6 MISSION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
