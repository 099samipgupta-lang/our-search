import json

from search_engine import SearchEngine
from result_presentation import DocumentStore, ResultAssembler
from indexing_pipeline.pipeline import CrawlIndexPipeline


class SearchService:
    def __init__(self, index_source, document_store, indexing_pipeline=None):
        self.engine = SearchEngine(index_source)
        self.document_store = document_store
        self.assembler = ResultAssembler(document_store)
        self.indexing_pipeline = indexing_pipeline

    def search(self, query, mode="OR", top_k=10):
        response = self.engine.search(
            query=query,
            mode=mode,
            top_k=top_k,
        )

        processed_terms = []

        for result in response.results:
            processed_terms.extend(
                result.matched_terms
            )

        processed_terms = list(
            dict.fromkeys(processed_terms)
        )

        assembled = self.assembler.assemble(
            ranked_results=[
                result.to_dict()
                for result in response.results
            ],
            query_terms=processed_terms,
            total_candidates=response.total_candidates,
        )

        return {
            "query": response.query,
            "mode": response.mode,
            "total_candidates": assembled[
                "total_candidates"
            ],
            "results": assembled["results"],
        }

    def index_document(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("request body must be an object")

        document_id = payload.get("document_id")
        url = payload.get("url")
        title = payload.get("title", "")
        text = payload.get("text")
        canonical_url = payload.get("canonical_url", "")

        if not isinstance(document_id, str) or not document_id:
            raise ValueError("document_id must be a non-empty string")
        if not isinstance(url, str) or not url:
            raise ValueError("url must be a non-empty string")
        if not isinstance(title, str):
            raise ValueError("title must be a string")
        if not isinstance(text, str) or not text:
            raise ValueError("text must be a non-empty string")
        if not isinstance(canonical_url, str):
            raise ValueError("canonical_url must be a string")

        if self.indexing_pipeline is None:
            raise RuntimeError("indexing pipeline is not configured")

        return self.indexing_pipeline.index_document(
                document_id=document_id,
                url=url,
                title=title,
                text=text,
            canonical_url=canonical_url,
        )

    def delete_document(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("request body must be an object")

        document_id = payload.get("document_id")

        if not isinstance(document_id, str):
            raise ValueError("document_id must be a string")

        document_id = document_id.strip()

        if not document_id:
            raise ValueError("document_id must not be empty")

        if self.indexing_pipeline is None:
            raise ValueError("indexing pipeline is not configured")

        removed = self.indexing_pipeline.remove_document(
            document_id
        )

        segment_id = None

        if removed:
            segment_id = self.indexing_pipeline.flush()

        return {
            "action": (
                "deleted"
                if removed
                else "not_found"
            ),
            "document_id": document_id,
            "segment_id": segment_id,
        }

    def handle_request(self, payload):
        if not isinstance(payload, dict):
            raise ValueError(
                "request body must be an object"
            )

        query = payload.get("query")

        if not isinstance(query, str):
            raise ValueError(
                "query must be a string"
            )

        mode = payload.get("mode", "OR")
        top_k = payload.get("top_k", 10)

        if not isinstance(mode, str):
            raise ValueError(
                "mode must be a string"
            )

        if not isinstance(top_k, int) or isinstance(
            top_k, bool
        ):
            raise ValueError(
                "top_k must be an integer"
            )

        if top_k < 1:
            raise ValueError(
                "top_k must be at least 1"
            )

        if top_k > 100:
            raise ValueError(
                "top_k must not exceed 100"
            )

        return self.search(
            query=query,
            mode=mode,
            top_k=top_k,
        )

    def health(self):
        return {
            "status": "ok",
            "service": "search_service",
        }
