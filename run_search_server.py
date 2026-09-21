import os

from index_storage.repository import IndexStorageRepository
from index_storage.config import create_index_storage
from indexing_pipeline.pipeline import CrawlIndexPipeline
from search_service.service import SearchService
from search_service.server import SearchHTTPServer


INDEX_ROOT = "indexing_pipeline_data"

storage = create_index_storage()

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
        "8082",
    )
)

server = SearchHTTPServer(
    service,
    host=HOST,
    port=PORT,
)

print("OUR SEARCH SEARCH SERVER")
print(f"Running on {HOST}:{PORT}")
print("Storage backend: configured by OUR_SEARCH_STORAGE_MODE")

try:
    server.serve_forever()

except KeyboardInterrupt:
    print("\nStopping server...")

finally:
    server.server_close()
    indexing_pipeline.close()
