from dataclasses import dataclass, field


@dataclass(frozen=True)
class ExplorationTopic:
    title: str
    description: str = ""


@dataclass(frozen=True)
class ExplorationContent:
    title: str
    visual_title: str
    visual_description: str
    core_idea: str
    related_topics: tuple[ExplorationTopic, ...] = field(
        default_factory=tuple
    )


__all__ = [
    "ExplorationTopic",
    "ExplorationContent",
]
