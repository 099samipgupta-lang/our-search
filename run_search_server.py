import os

from index_storage.remote import RemoteIndexStorage
from index_storage.repository import IndexStorageRepository
from indexing_pipeline.pipeline import CrawlIndexPipeline
from search_service.service import SearchService
from search_service.server import SearchHTTPServer


INDEX_ROOT = "indexing_pipeline_data"

STORAGE_URL = os.environ.get(
    "OUR_SEARCH_STORAGE_URL"
)

if not STORAGE_URL:
    raise RuntimeError(
        "OUR_SEARCH_STORAGE_URL is required"
    )

STORAGE_API_KEY = os.environ.get(
    "OUR_SEARCH_STORAGE_API_KEY"
)

if not STORAGE_API_KEY:
    raise RuntimeError(
        "OUR_SEARCH_STORAGE_API_KEY is required"
    )

storage = RemoteIndexStorage(
    STORAGE_URL,
    api_key=STORAGE_API_KEY,
)

storage_repository = IndexStorageRepository(
    storage
)

indexing_pipeline = CrawlIndexPipeline(
    root=INDEX_ROOT,
    storage=storage,
    storage_repository=storage_repository,
)

search_index = indexing_pipeline.search_index
document_store = indexing_pipeline.document_store

service = SearchService(
    search_index,
    document_store,
    indexing_pipeline=indexing_pipeline,
)

HOST = "0.0.0.0"

PORT = int(
    os.environ.get(
        "PORT",
        "8080",
    )
)

server = SearchHTTPServer(
    service,
    host=HOST,
    port=PORT,
)

print("OUR SEARCH SEARCH SERVER")
print(f"Running on {HOST}:{PORT}")
print("Remote storage:", STORAGE_URL)
print("Remote storage authentication: enabled")
print("Press Ctrl+C to stop.")

try:
    server.serve_forever()

except KeyboardInterrupt:
    print("\nStopping server...")

finally:
    server.server_close()
    indexing_pipeline.close()
