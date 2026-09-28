from .experience_content import (
    ExplorationContent,
    ExplorationTopic,
)
from .experience_planner import ExplorationPlan


def build_exploration_content(
    plan: ExplorationPlan,
) -> ExplorationContent:

    topic = plan.topic

    related_topics = (
        ExplorationTopic(
            title="Key concepts",
            description="Important concepts connected to this topic.",
        ),
        ExplorationTopic(
            title="How it works",
            description="Explore the mechanisms and ideas behind this topic.",
        ),
        ExplorationTopic(
            title="Deeper exploration",
            description="Continue exploring connected information.",
        ),
    )

    return ExplorationContent(
        title=topic,
        visual_title="Visual explanation",
        visual_description=(
            "A visual explanation for this topic will appear here."
        ),
        core_idea=(
            "A verified explanation of the core idea will appear here."
        ),
        related_topics=related_topics,
    )


__all__ = [
    "build_exploration_content",
]
