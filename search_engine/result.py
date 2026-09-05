from dataclasses import dataclass, asdict


@dataclass
class SearchResult:
    document_id: str
    score: float
    matched_terms: list
    document_length: int

    def to_dict(self):
        return asdict(self)


@dataclass
class SearchResponse:
    query: str
    mode: str
    total_candidates: int
    results: list

    def to_dict(self):
        return {
            "query": self.query,
            "mode": self.mode,
            "total_candidates": self.total_candidates,
            "results": [
                result.to_dict()
                if isinstance(result, SearchResult)
                else result
                for result in self.results
            ],
        }
