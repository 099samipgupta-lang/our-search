from dataclasses import dataclass


@dataclass(frozen=True)
class ExplorationQuery:
    raw_query: str
    normalized_query: str
    words: tuple[str, ...]
    topic: str


def understand_query(query: str) -> ExplorationQuery:
    raw_query = str(query or "").strip()
    normalized = " ".join(raw_query.split())

    words = tuple(
        word
        for word in normalized.split(" ")
        if word
    )

    return ExplorationQuery(
        raw_query=raw_query,
        normalized_query=normalized,
        words=words,
        topic=normalized,
    )


__all__ = [
    "ExplorationQuery",
    "understand_query",
]
