from search_index.inverted_index import InvertedIndex
from indexing_pipeline.document import IndexedDocument


class DocumentIndexer:
    def __init__(self):
        self.index = InvertedIndex()

    def index_document(
        self,
        document_id,
        url,
        title,
        text,
        canonical_url="",
    ):
        self.index.add_document(
            document_id,
            text,
        )

        return IndexedDocument(
            document_id=document_id,
            url=url,
            title=title,
            text=text,
            content_hash="",
            canonical_url=canonical_url,
            status="active",
        )

    def remove_document(self, document_id):
        return self.index.remove_document(document_id)

    def get_document(self, document_id):
        return self.index.get_document_info(document_id)

    def document_count(self):
        return self.index.document_count()

    def has_pending_documents(self):
        return self.document_count() > 0

    def reset(self):
        self.index = InvertedIndex()

    def build_segment(self, segment_manager):
        if not self.has_pending_documents():
            return None

        segment_id = segment_manager.flush_index(self.index)

        self.reset()

        return segment_id
