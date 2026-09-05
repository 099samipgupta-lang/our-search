import json

from search_engine import SearchEngine
from result_presentation import DocumentStore, ResultAssembler


class SearchService:
    def __init__(self, index_source, document_store):
        self.engine = SearchEngine(index_source)
        self.document_store = document_store
        self.assembler = ResultAssembler(document_store)

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
