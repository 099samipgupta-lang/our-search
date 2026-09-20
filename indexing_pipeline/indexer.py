from urllib.parse import urlparse

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
        structure=None,
    ):
        searchable_text = " ".join(
            part
            for part in (url, title, text, canonical_url)
            if isinstance(part, str) and part
        )

        structure = (
            structure
            if isinstance(structure, dict)
            else {}
        )

        parsed_url = urlparse(
            url or ""
        )

        domain = (
            parsed_url.hostname
            or ""
        ).casefold()

        self.index.add_document(
            document_id,
            text=structure.get(
                "main_content",
                text,
            ),
            title=structure.get(
                "title",
                title,
            ),
            url=url,
            domain=domain,
            canonical_url=canonical_url,
            headings=structure.get(
                "headings",
                "",
            ),
            navigation=structure.get(
                "navigation_text",
                "",
            ),
            footer=structure.get(
                "footer_text",
                "",
            ),
            sidebar=structure.get(
                "sidebar_text",
                "",
            ),
            anchor_text=structure.get(
                "anchor_text",
                "",
            ),
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
