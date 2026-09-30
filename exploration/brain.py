from dataclasses import dataclass
from typing import Any

from exploration.relationships import RelationshipEngine
from exploration.conversation import ConversationSystem
from exploration.knowledge.runtime import KnowledgeRuntimeEngine
from exploration.language_core.core import LanguageBrain
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
    knowledge_runtime: dict[str, Any]
    internal_answer: str


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


class AnswerGenerationSystem:
    """
    Deterministic grounded answer construction layer.

    This system turns retrieved search evidence into an actual
    user-facing answer. It does not invent facts and does not
    replace the search engine, crawler, index, or storage system.
    """

    QUESTION_WORDS = {
        "what", "who", "where", "when", "why", "how",
        "which", "can", "does", "do", "is", "are",
        "was", "were", "will", "should", "could",
    }

    def _clean(self, value):
        if value is None:
            return ""

        text = " ".join(str(value).split())
        return text.strip()

    def _question_type(self, query):
        words = self._clean(query).casefold().split()

        if not words:
            return "general"

        first = words[0]

        if first in {"what", "who", "where", "when", "why", "how"}:
            return first

        if first in {"can", "could", "should", "will"}:
            return "decision"

        if first in {"is", "are", "was", "were", "does", "do"}:
            return "yes_no"

        return "general"

    def _source_text(self, source):
        title = self._clean(source.get("title", ""))
        snippet = self._clean(source.get("snippet", ""))

        if title and snippet:
            return f"{title}: {snippet}"

        return title or snippet

    def _extract_evidence(self, query, sources, limit=5):
        query_words = {
            word.casefold().strip(".,!?;:()[]{}\"'")
            for word in self._clean(query).split()
            if len(word) > 2
        }

        candidates = []

        for source in sources:
            if not isinstance(source, dict):
                continue

            text = self._source_text(source)

            if not text:
                continue

            lower = text.casefold()

            overlap = sum(
                1
                for word in query_words
                if word in lower
            )

            candidates.append(
                (
                    overlap,
                    source.get("title", ""),
                    source.get("url", ""),
                    source.get("snippet", ""),
                )
            )

        candidates.sort(
            key=lambda item: (
                item[0],
                bool(item[2]),
                bool(item[1]),
            ),
            reverse=True,
        )

        return [
            {
                "title": self._clean(item[1]),
                "url": self._clean(item[2]),
                "snippet": self._clean(item[3]),
                "relevance": item[0],
            }
            for item in candidates[:max(1, int(limit))]
        ]

    def _build_from_evidence(
        self,
        query,
        question_type,
        evidence,
    ):
        if not evidence:
            return (
                "I couldn't find enough supporting information "
                "in the current search results to answer that yet."
            )

        first = evidence[0]

        title = first.get("title", "")
        snippet = first.get("snippet", "")

        if not snippet:
            snippet = title

        if not snippet:
            return (
                "I found search sources for this question, "
                "but they do not contain enough readable information "
                "to construct a reliable answer."
            )

        prefix = {
            "what": "The search results indicate that",
            "who": "The available sources identify",
            "where": "The available sources indicate that",
            "when": "The available sources indicate that",
            "why": "The available sources explain that",
            "how": "The available sources describe how",
            "yes_no": "Based on the available sources,",
            "decision": "Based on the available sources,",
            "general": "Based on the available search sources,",
        }.get(question_type, "Based on the available search sources,")

        sentence = snippet.rstrip(" .")

        if question_type in {"who", "what", "where", "when", "why", "how"}:
            answer = f"{prefix} {sentence}."

        else:
            answer = f"{prefix} {sentence}."

        if len(evidence) > 1:
            supporting = []

            for source in evidence[1:3]:
                extra = source.get("snippet", "")
                if extra:
                    supporting.append(
                        self._clean(extra).rstrip(" .")
                    )

            if supporting:
                answer += " Additional search sources provide related information: "
                answer += "; ".join(supporting) + "."

        return answer

    def generate(
        self,
        query,
        knowledge,
        reasoning,
        verification,
    ):
        normalized_query = self._clean(query)
        sources = knowledge.get("sources", [])

        question_type = self._question_type(
            normalized_query
        )

        evidence = self._extract_evidence(
            normalized_query,
            sources,
            limit=5,
        )

        answer = self._build_from_evidence(
            normalized_query,
            question_type,
            evidence,
        )

        return {
            "answer": answer,
            "question_type": question_type,
            "evidence": evidence,
            "source_count": len(sources),
            "grounded": bool(evidence),
            "reasoning_available": bool(
                reasoning.get("structured")
                or reasoning.get("has_sources")
            ),
            "verified_support": bool(
                verification.get("verified")
            ),
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

        self.answer_generation = AnswerGenerationSystem()
        # Learned language model subsystem.
        self.language_brain = LanguageBrain()
        # Complete internal knowledge architecture.
        # This coordinates the entire exploration/knowledge system.
        self.knowledge_runtime = KnowledgeRuntimeEngine()


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

        greeting_words = {
            "hi",
            "hello",
            "hey",
            "hiya",
            "yo",
            "howdy",
        }

        normalized_words = set(
            understanding.get("words", [])
        )

        is_greeting = bool(
            normalized_words
            and normalized_words.issubset(greeting_words)
        )

        understanding = {
            **understanding,
            "intent": "greeting" if is_greeting else "search",
            "is_greeting": is_greeting,
        }

        if is_greeting:
            retrieved = []
        else:
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

        # Run the complete internal knowledge architecture.
        knowledge_runtime_result = self.knowledge_runtime.think(
            query=context.query
        )

        # Prefer the internal knowledge system when it produces a grounded
        # answer. Existing web/search evidence remains available as fallback.
        internal_answer = knowledge_runtime_result.answer

        if is_greeting:
            generated_answer = {
                "answer": "Hi! How are you doing?",
                "question_type": "greeting",
                "evidence": [],
                "source_count": 0,
                "grounded": True,
                "reasoning_available": False,
                "verified_support": False,
            }
        else:
            generated_answer = self.answer_generation.generate(
                query=context.query,
                knowledge=knowledge,
                reasoning=reasoning,
                verification=verification,
            )

        conversation = self.conversation.build_answer(
            query=context.query,
            explanation=explanation,
            verification=verification,
        )

        runtime_answer = knowledge_runtime_result.answer

        if (
            isinstance(runtime_answer, dict)
            and runtime_answer.get("grounded")
            and str(runtime_answer.get("answer", "")).strip()
        ):
            visible_answer = runtime_answer["answer"]
            visible_evidence = runtime_answer.get(
                "evidence",
                generated_answer["evidence"],
            )
            visible_grounded = runtime_answer.get(
                "grounded",
                True,
            )
        else:
            visible_answer = generated_answer["answer"]
            visible_evidence = generated_answer["evidence"]
            visible_grounded = generated_answer["grounded"]

        # Use the learned language model to express the grounded
        # answer naturally, while keeping the existing brain systems
        # responsible for knowledge and grounding.
        language_prompt = (
            "You are the language generation subsystem of OUR SEARCH. "
            "Rewrite the supplied answer naturally and clearly. "
            "Do not add facts that are not present in the supplied answer. "
            "If the supplied answer says there is not enough knowledge, "
            "preserve that limitation.\n\n"
            f"User question: {context.query}\n"
            f"Grounded answer: {visible_answer}\n"
            "Natural answer:"
        )

        language_response = self.language_brain.runtime.generate(
            language_prompt,
            max_tokens=128,
        )

        if str(language_response).strip():
            visible_answer = language_response.strip()

        conversation = {
            **conversation,
            "answer": visible_answer,
            "question_type": generated_answer["question_type"],
            "evidence": visible_evidence,
            "grounded": visible_grounded,
            "language_model": {
                "version": self.language_brain.VERSION,
                "runtime": self.language_brain.runtime.VERSION,
                "learned_model": True,
            },
        }

        return BrainResult(
            query=context.query,
            understanding=understanding,
            knowledge=knowledge,
            relationships=related,
            reasoning=reasoning,
            verification=verification,
            explanation=explanation,
            conversation=conversation,
            knowledge_runtime=knowledge_runtime_result,
            internal_answer=internal_answer,
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
    "AnswerGenerationSystem",
    "OurSearchBrain",
]
