from dataclasses import dataclass
from typing import Any

from exploration.relationships import RelationshipEngine
from exploration.conversation import ConversationSystem
from exploration.concepts import ConceptSystem
from exploration.reasoning import ReasoningEngine
from exploration.verification import VerificationEngine
from exploration.explanation import ExplanationEngine


@dataclass(frozen=True)
class BrainContext:
    query: str
    results: list[dict[str, Any]]


@dataclass(frozen=True)
class BrainResult:
    query: str
    understanding: dict[str, Any]
    knowledge: dict[str, Any]
    relationships: list[dict[str, Any]]
    reasoning: dict[str, Any]
    verification: dict[str, Any]
    explanation: dict[str, Any]
    conversation: dict[str, Any]
    concepts: list[dict[str, Any]]


class LanguageSystem:
    def understand(self, query: str) -> dict[str, Any]:
        normalized = " ".join(str(query or "").split())

        words = [
            word
            for word in normalized.casefold().split()
            if word
        ]

        return {
            "raw_query": str(query or ""),
            "normalized_query": normalized,
            "words": words,
            "word_count": len(words),
        }


class KnowledgeSystem:
    def collect(
        self,
        context: BrainContext,
    ) -> dict[str, Any]:
        sources = []

        for result in context.results:
            if not isinstance(result, dict):
                continue

            sources.append(
                {
                    "document_id": result.get("document_id"),
                    "title": result.get("title", ""),
                    "url": result.get("url", ""),
                    "snippet": result.get("snippet", ""),
                    "matched_terms": result.get(
                        "matched_terms",
                        [],
                    ),
                }
            )

        return {
            "source_count": len(sources),
            "sources": sources,
        }


class RetrievalSystem:
    def retrieve(
        self,
        context: BrainContext,
    ) -> list[dict[str, Any]]:
        return [
            dict(result)
            for result in context.results
            if isinstance(result, dict)
        ]


class ReasoningSystem:
    def reason(
        self,
        context: BrainContext,
        knowledge: dict[str, Any],
        relationships: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "has_sources": bool(
                knowledge["source_count"]
            ),
            "source_count": knowledge["source_count"],
            "relationship_count": len(relationships),
            "query_has_content": bool(
                str(context.query).strip()
            ),
        }


class VerificationSystem:
    def verify(
        self,
        knowledge: dict[str, Any],
        reasoning: dict[str, Any],
    ) -> dict[str, Any]:
        source_count = int(
            knowledge.get("source_count", 0)
        )

        return {
            "verified": source_count > 0,
            "source_count": source_count,
            "has_supporting_sources": source_count > 0,
        }


class ExplanationSystem:
    def explain(
        self,
        context: BrainContext,
        knowledge: dict[str, Any],
        reasoning: dict[str, Any],
        verification: dict[str, Any],
    ) -> dict[str, Any]:
        sources = knowledge.get("sources", [])

        return {
            "query": context.query,
            "available": verification["verified"],
            "source_count": verification["source_count"],
            "core_sources": [
                {
                    "title": source.get("title", ""),
                    "snippet": source.get("snippet", ""),
                    "url": source.get("url", ""),
                }
                for source in sources[:5]
            ],
        }


class OurSearchBrain:
    """
    Central computational brain of OUR SEARCH.

    SearchEngine remains the retrieval engine.
    This brain coordinates the systems that operate
    on retrieved information.
    """

    VERSION = "brain.v3"

    def __init__(
        self,
        language=None,
        knowledge=None,
        retrieval=None,
        relationships=None,
        reasoning=None,
        verification=None,
        explanation=None,
        conversation=None,
        concepts=None,
        reasoning_engine=None,
        verification_engine=None,
        explanation_engine=None,
    ):
        self.language = (
            language
            if language is not None
            else LanguageSystem()
        )

        self.knowledge = (
            knowledge
            if knowledge is not None
            else KnowledgeSystem()
        )

        self.retrieval = (
            retrieval
            if retrieval is not None
            else RetrievalSystem()
        )

        self.relationships = (
            relationships
            if relationships is not None
            else RelationshipEngine()
        )

        self.reasoning = (
            reasoning
            if reasoning is not None
            else ReasoningSystem()
        )

        self.verification = (
            verification
            if verification is not None
            else VerificationSystem()
        )

        self.explanation = (
            explanation
            if explanation is not None
            else ExplanationSystem()
        )

        self.conversation = (
            conversation
            if conversation is not None
            else ConversationSystem()
        )

        self.concepts = (
            concepts
            if concepts is not None
            else ConceptSystem()
        )

        self.reasoning_engine = (
            reasoning_engine
            if reasoning_engine is not None
            else ReasoningEngine()
        )

        self.verification_engine = (
            verification_engine
            if verification_engine is not None
            else VerificationEngine()
        )

        self.explanation_engine = (
            explanation_engine
            if explanation_engine is not None
            else ExplanationEngine()
        )

    def think(
        self,
        query: str,
        results: list[dict[str, Any]],
    ) -> BrainResult:
        context = BrainContext(
            query=query,
            results=[
                dict(result)
                for result in results
                if isinstance(result, dict)
            ],
        )

        understanding = self.language.understand(
            context.query
        )

        retrieved = self.retrieval.retrieve(context)

        knowledge = self.knowledge.collect(
            BrainContext(
                query=context.query,
                results=retrieved,
            )
        )

        related = []

        if retrieved:
            related = self.relationships.related_results(
                retrieved[0],
                retrieved,
                limit=10,
            )

        concept_items = self.concepts.build(
            query=context.query,
            sources=knowledge.get("sources", []),
            limit=12,
        )

        concept_data = [
            {
                "name": item.name,
                "source_count": item.source_count,
                "relevance": item.relevance,
            }
            for item in concept_items
        ]

        reasoning = self.reasoning.reason(
            context=context,
            knowledge=knowledge,
            relationships=related,
        )

        structured_reasoning = self.reasoning_engine.analyze(
            query=context.query,
            understanding=understanding,
            knowledge=knowledge,
            concepts=concept_data,
            relationships=related,
        )

        reasoning = {
            **reasoning,
            "structured": structured_reasoning,
        }

        verification = self.verification.verify(
            knowledge=knowledge,
            reasoning=reasoning,
        )

        verification_findings = (
            self.verification_engine.verify_sources(
                knowledge.get("sources", [])
            )
        )

        verification = {
            **verification,
            "structured": verification_findings,
        }

        explanation = self.explanation.explain(
            context=context,
            knowledge=knowledge,
            reasoning=reasoning,
            verification=verification,
        )

        structured_explanation = (
            self.explanation_engine.build(
                query=context.query,
                understanding=understanding,
                knowledge=knowledge,
                concepts=concept_data,
                reasoning=reasoning,
                verification=verification,
            )
        )

        explanation = {
            **explanation,
            "structured": structured_explanation,
        }

        conversation = self.conversation.build_answer(
            query=context.query,
            explanation=explanation,
            verification=verification,
        )

        return BrainResult(
            query=context.query,
            understanding=understanding,
            knowledge=knowledge,
            relationships=related,
            reasoning=reasoning,
            verification=verification,
            explanation=explanation,
            conversation=conversation,
            concepts=concept_data,
        )


__all__ = [
    "BrainContext",
    "BrainResult",
    "LanguageSystem",
    "KnowledgeSystem",
    "RetrievalSystem",
    "RelationshipEngine",
    "ReasoningSystem",
    "VerificationSystem",
    "ExplanationSystem",
    "ConversationSystem",
    "ConceptSystem",
    "ReasoningEngine",
    "VerificationEngine",
    "ExplanationEngine",
    "OurSearchBrain",
]
