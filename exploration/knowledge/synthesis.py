from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from exploration.knowledge.question import (
    KnowledgeQuestion,
    KnowledgeQuestionUnderstandingEngine,
)
from exploration.knowledge.retrieval import (
    KnowledgeMatch,
    KnowledgeRetrievalEngine,
)


@dataclass
class KnowledgeAnswer:
    answer: str
    confidence: float
    supporting_knowledge: List[Dict[str, Any]]
    intent: str
    grounded: bool


class KnowledgeAnswerSynthesisEngine:
    VERSION = "knowledge-synthesis.v1"

    def __init__(
        self,
        question_engine: (
            KnowledgeQuestionUnderstandingEngine | None
        ) = None,
        retrieval_engine: (
            KnowledgeRetrievalEngine | None
        ) = None,
    ):
        self.question_engine = (
            question_engine
            if question_engine is not None
            else KnowledgeQuestionUnderstandingEngine()
        )

        self.retrieval_engine = (
            retrieval_engine
            if retrieval_engine is not None
            else KnowledgeRetrievalEngine()
        )

    def _fact_sentence(
        self,
        match: KnowledgeMatch,
    ) -> str:
        record = match.record
        value = record.value

        if record.kind == "fact":
            subject = value.get("subject", "")
            predicate = value.get("predicate", "")
            target = value.get("value", "")

            return (
                f"{subject} {predicate} {target}."
            )

        if record.kind == "concept":
            name = value.get(
                "name",
                record.key,
            )
            definition = value.get(
                "definition",
                "",
            )

            return (
                f"{name} is {definition}."
            )

        if record.kind == "relation":
            subject = value.get(
                "subject",
                "",
            )
            relation = value.get(
                "relation",
                "",
            )
            target = value.get(
                "target",
                "",
            )

            return (
                f"{subject} {relation} {target}."
            )

        return ""

    def _deduplicate(
        self,
        sentences: List[str],
    ) -> List[str]:
        output = []
        seen = set()

        for sentence in sentences:
            clean = sentence.strip()

            if not clean:
                continue

            key = clean.lower()

            if key in seen:
                continue

            seen.add(key)
            output.append(clean)

        return output

    def synthesize(
        self,
        question: str,
        limit: int = 8,
    ) -> KnowledgeAnswer:
        understood: KnowledgeQuestion = (
            self.question_engine.understand(question)
        )

        matches = self.retrieval_engine.retrieve(
            question,
            limit=limit,
        )

        sentences = self._deduplicate(
            [
                self._fact_sentence(match)
                for match in matches
            ]
        )

        supporting = []

        for match in matches:
            supporting.append(
                {
                    "kind": match.record.kind,
                    "key": match.record.key,
                    "value": match.record.value,
                    "confidence": match.record.confidence,
                    "source": match.record.source,
                    "score": match.score,
                    "matched_terms": match.matched_terms,
                }
            )

        if not sentences:
            return KnowledgeAnswer(
                answer=(
                    "I do not have enough internal "
                    "knowledge to answer that yet."
                ),
                confidence=0.0,
                supporting_knowledge=[],
                intent=understood.intent,
                grounded=False,
            )

        top_scores = [
            match.score
            for match in matches
        ]

        confidence = sum(top_scores) / len(top_scores)

        if understood.intent == "definition":
            answer = " ".join(sentences[:3])
        else:
            answer = " ".join(sentences[:5])

        return KnowledgeAnswer(
            answer=answer,
            confidence=max(
                0.0,
                min(1.0, confidence),
            ),
            supporting_knowledge=supporting,
            intent=understood.intent,
            grounded=True,
        )
