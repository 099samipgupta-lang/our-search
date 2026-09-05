from retrieval.query import QueryParser
from retrieval.engine import RetrievalEngine
from ranking.engine import RankingEngine
from search_engine.result import SearchResult, SearchResponse


class SearchEngine:
    def __init__(
        self,
        index_source,
        query_parser=None,
        retrieval_engine=None,
        ranking_engine=None,
    ):
        self.index = index_source

        self.query_parser = (
            query_parser
            if query_parser is not None
            else QueryParser()
        )

        self.retrieval = (
            retrieval_engine
            if retrieval_engine is not None
            else RetrievalEngine(
                self.index,
                query_parser=self.query_parser,
            )
        )

        self.ranking = (
            ranking_engine
            if ranking_engine is not None
            else RankingEngine(self.index)
        )

    def search(self, query, mode="OR", top_k=10):
        if not isinstance(query, str):
            raise TypeError("query must be a string")

        if not isinstance(top_k, int):
            raise TypeError("top_k must be an integer")

        if top_k < 1:
            raise ValueError("top_k must be at least 1")

        mode = mode.upper()

        if mode not in ("OR", "AND"):
            raise ValueError("mode must be OR or AND")

        terms = self.query_parser.parse_unique(query)

        if not terms:
            return SearchResponse(
                query=query,
                mode=mode,
                total_candidates=0,
                results=[],
            )

        candidates = self.retrieval.search(query, mode)

        ranked = self.ranking.rank(
            terms,
            candidates,
        )

        ranked = ranked[:top_k]

        results = [
            SearchResult(
                document_id=item["document_id"],
                score=float(item["score"]),
                matched_terms=list(item["matched_terms"]),
                document_length=int(item["document_length"]),
            )
            for item in ranked
        ]

        return SearchResponse(
            query=query,
            mode=mode,
            total_candidates=len(candidates),
            results=results,
        )

    def search_dict(self, query, mode="OR", top_k=10):
        return self.search(
            query=query,
            mode=mode,
            top_k=top_k,
        ).to_dict()
