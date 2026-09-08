from indexing_pipeline.document import IndexedDocument
from indexing_pipeline.indexer import DocumentIndexer
from indexing_pipeline.crawler_bridge import CrawlerIndexBridge
from indexing_pipeline.version_state import VersionState
from indexing_pipeline.versioned_index import VersionedIndex


__all__ = [
    "IndexedDocument",
    "DocumentIndexer",
    "CrawlerIndexBridge",
    "VersionState",
    "VersionedIndex",
    "CrawlIndexPipeline",
]


def __getattr__(name):
    if name == "CrawlIndexPipeline":
        from indexing_pipeline.pipeline import CrawlIndexPipeline
        return CrawlIndexPipeline

    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )
