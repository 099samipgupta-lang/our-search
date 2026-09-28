from dataclasses import dataclass, field
from typing import Any


@dataclass
class ConversationState:
    """
    Runtime conversation state for OUR SEARCH.

    This stores structured conversation context rather than
    replacing the underlying search/index systems.
    """

    turns: list[dict[str, Any]] = field(default_factory=list)
    current_topic: str = ""
    previous_query: str = ""

    def remember(
        self,
        query: str,
        response: dict[str, Any],
    ):
        self.turns.append(
            {
                "query": query,
                "response": response,
            }
        )

        self.previous_query = query
        self.current_topic = query

    def recent(self, limit=10):
        return self.turns[-max(0, int(limit)):]


class ConversationMemory:
    """
    Memory machine for the OUR SEARCH brain.

    It provides conversational continuity without changing
    the existing crawler, index, storage, or search engine.
    """

    def __init__(self):
        self.state = ConversationState()

    def get_context(self):
        return {
            "current_topic": self.state.current_topic,
            "previous_query": self.state.previous_query,
            "recent_turns": self.state.recent(),
        }

    def remember(self, query, response):
        self.state.remember(
            query=query,
            response=response,
        )


class AnswerBuilder:
    """
    Converts the brain's structured internal results into
    an answer-ready representation.
    """

    def build(
        self,
        query: str,
        explanation: dict[str, Any],
        verification: dict[str, Any],
        memory: dict[str, Any],
    ) -> dict[str, Any]:
        sources = explanation.get(
            "core_sources",
            [],
        )

        return {
            "query": query,
            "available": bool(
                verification.get("verified", False)
            ),
            "source_count": int(
                verification.get("source_count", 0)
            ),
            "core_sources": sources,
            "conversation_context": memory,
        }


class ConversationSystem:
    """
    Coordinates memory and answer construction.

    This is one machine inside the larger OUR SEARCH brain.
    """

    def __init__(
        self,
        memory=None,
        answer_builder=None,
    ):
        self.memory = (
            memory
            if memory is not None
            else ConversationMemory()
        )

        self.answer_builder = (
            answer_builder
            if answer_builder is not None
            else AnswerBuilder()
        )

    def build_answer(
        self,
        query: str,
        explanation: dict[str, Any],
        verification: dict[str, Any],
    ):
        context = self.memory.get_context()

        answer = self.answer_builder.build(
            query=query,
            explanation=explanation,
            verification=verification,
            memory=context,
        )

        self.memory.remember(
            query=query,
            response=answer,
        )

        return answer


__all__ = [
    "ConversationState",
    "ConversationMemory",
    "AnswerBuilder",
    "ConversationSystem",
]
