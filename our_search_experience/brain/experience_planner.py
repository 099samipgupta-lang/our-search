from dataclasses import dataclass

from .query_understanding import ExplorationQuery


@dataclass(frozen=True)
class ExplorationPlan:
    topic: str
    include_visual: bool
    include_watch: bool
    include_listen: bool
    include_read: bool
    include_core_idea: bool
    include_related_topics: bool


def create_exploration_plan(
    query: ExplorationQuery,
) -> ExplorationPlan:

    has_topic = bool(
        query.normalized_query
    )

    return ExplorationPlan(
        topic=query.topic,
        include_visual=has_topic,
        include_watch=has_topic,
        include_listen=has_topic,
        include_read=has_topic,
        include_core_idea=has_topic,
        include_related_topics=has_topic,
    )


__all__ = [
    "ExplorationPlan",
    "create_exploration_plan",
]
