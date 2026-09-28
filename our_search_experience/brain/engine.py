from .content_builder import build_exploration_content
from .experience_planner import create_exploration_plan
from .query_understanding import understand_query


def build_experience(query: str):
    understood = understand_query(query)

    if not understood.normalized_query:
        return None

    plan = create_exploration_plan(understood)

    return build_exploration_content(plan)


__all__ = [
    "build_experience",
]
