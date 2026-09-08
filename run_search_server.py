import os

from index_storage.repository import IndexStorageRepository
from index_storage.supabase import SupabaseIndexStorage
from indexing_pipeline.pipeline import CrawlIndexPipeline
from search_service.service import SearchService
from search_service.server import SearchHTTPServer


INDEX_ROOT = "indexing_pipeline_data"

SUPABASE_URL = os.environ.get(
    "SUPABASE_URL"
)

SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY"
)

SUPABASE_BUCKET = os.environ.get(
    "SUPABASE_BUCKET",
    "Videos",
)

if not SUPABASE_URL:
    raise RuntimeError(
        "SUPABASE_URL is required"
    )

if not SUPABASE_KEY:
    raise RuntimeError(
        "SUPABASE_KEY is required"
    )

storage = SupabaseIndexStorage(
    project_url=SUPABASE_URL,
    api_key=SUPABASE_KEY,
    bucket=SUPABASE_BUCKET,
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
print("Storage: Supabase")
print(f"Bucket: {SUPABASE_BUCKET}")

try:
    server.serve_forever()

except KeyboardInterrupt:
    print("\nStopping server...")

finally:
    server.server_close()
    indexing_pipeline.close()
