from dataclasses import dataclass


@dataclass(frozen=True)
class KnowledgeResult:
    topic: str
    core_idea: str
    visual_description: str
    related_topics: tuple[str, ...]


class KnowledgeMachine:
    """
    Interface for OUR SEARCH's own knowledge system.

    The machine is intentionally independent from the website,
    crawler, index, and search-result renderer.
    """

    def explain(self, topic: str) -> KnowledgeResult:
        raise NotImplementedError(
            "A concrete OUR SEARCH knowledge machine must implement explain()."
        )


__all__ = [
    "KnowledgeResult",
    "KnowledgeMachine",
]
