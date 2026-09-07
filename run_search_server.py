import os

from search_index.inverted_index import InvertedIndex
from search_index.persistence.segment_manager import SegmentManager
from indexing_pipeline.version_state import VersionState
from indexing_pipeline.live_index import LiveVersionedIndex
from result_presentation.document_store import DocumentStore
from search_service.service import SearchService
from search_service.server import SearchHTTPServer


INDEX_ROOT = "indexing_pipeline_data/index"
VERSION_STATE_PATH = "indexing_pipeline_data/version_state.json"
DOCUMENTS_PATH = "indexing_pipeline_data/documents.json"


segment_manager = SegmentManager(INDEX_ROOT)

version_state = VersionState(VERSION_STATE_PATH)
version_state.load()

pending_index = InvertedIndex()

search_index = LiveVersionedIndex(
    segment_manager,
    pending_index,
    version_state,
)

document_store = DocumentStore(DOCUMENTS_PATH)
document_store.load()

service = SearchService(
    search_index,
    document_store,
)


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8080"))

server = SearchHTTPServer(
    service,
    host=HOST,
    port=PORT,
)

print("OUR SEARCH SEARCH SERVER")
print(f"Running on {HOST}:{PORT}")
print("Press Ctrl+C to stop.")

try:
    server.serve_forever()

except KeyboardInterrupt:
    print("\nStopping server...")

finally:
    server.server_close()
    segment_manager.close()
