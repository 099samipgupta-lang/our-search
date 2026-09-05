class VersionedIndex:

    def __init__(
        self,
        segment_manager,
        version_state
    ):

        self.segment_manager = segment_manager
        self.version_state = version_state

    def get_postings(self, term):

        postings = self.segment_manager.get_postings(
            term
        )

        active_ids = (
            self.version_state.active_document_ids()
        )

        return {
            document_id: positions
            for document_id, positions
            in postings.items()
            if document_id in active_ids
        }

    def get_document_info(
        self,
        document_id
    ):

        if not self.version_state.is_active(
            document_id
        ):
            return None

        return self.segment_manager.get_document_info(
            document_id
        )

    def total_documents(self):

        return self.version_state.document_count()

    def document_count(self):

        return self.version_state.document_count()

    def total_vocabulary_entries(self):

        return self.segment_manager.total_vocabulary_entries()

    def segment_count(self):

        return self.segment_manager.segment_count()

    def list_segments(self):

        return self.segment_manager.list_segments()
