import os

from indexing_pipeline.pipeline import CrawlIndexPipeline
from search_service.service import SearchService
from search_service.server import SearchHTTPServer


INDEX_ROOT = "indexing_pipeline_data"


indexing_pipeline = CrawlIndexPipeline(
    root=INDEX_ROOT
)

search_index = indexing_pipeline.search_index
document_store = indexing_pipeline.document_store

service = SearchService(
    search_index,
    document_store,
    indexing_pipeline=indexing_pipeline,
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
    indexing_pipeline.close()
