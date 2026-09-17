"""
OUR SEARCH
Phase 11.3 — Query Type / Intent Analysis

Version:
    query-type-intent-analysis.v1

Purpose:
    Analyze the canonical representation produced by Phase 11.2 and
    determine the structural query type and likely retrieval intents.

Scale target:
    Billions -> potentially trillions of publicly accessible Web resources.

Design target:
    Google-scale general Web-search capability without using Google
    technology or infrastructure.

This layer is a query-analysis/control-plane component.

It does NOT:
    - retrieve documents
    - rank documents
    - access Google's systems
    - generate semantic embeddings
    - replace the ranking system
    - replace retrieval planning
    - modify the index
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import re
import time
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple


ARCHITECTURE_VERSION = "query-type-intent-analysis.v1"
SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.time()


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{_hash(f'{prefix}:{time.time_ns()}')[:24]}"


# ---------------------------------------------------------------------------
# States
# ---------------------------------------------------------------------------

class IntentAnalysisState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    SIGNAL_EXTRACTION = "signal_extraction"
    TYPE_ANALYSIS = "type_analysis"
    INTENT_ANALYSIS = "intent_analysis"
    CONFIDENCE_CALCULATION = "confidence_calculation"
    COMPLETED = "completed"
    REJECTED = "rejected"


# ---------------------------------------------------------------------------
# Query structural types
# ---------------------------------------------------------------------------

class QueryType(str, Enum):
    GENERAL = "general"
    NAVIGATIONAL = "navigational"
    INFORMATIONAL = "informational"
    TRANSACTIONAL = "transactional"
    LOCAL = "local"
    ENTITY = "entity"
    MEDIA = "media"
    PROCEDURAL = "procedural"
    COMPARISON = "comparison"
    FACTUAL = "factual"
    TIME_SENSITIVE = "time_sensitive"


class QueryIntent(str, Enum):
    FIND_WEBSITE = "find_website"
    FIND_PAGE = "find_page"
    LEARN_TOPIC = "learn_topic"
    ANSWER_FACT = "answer_fact"
    RESEARCH_TOPIC = "research_topic"
    COMPARE_OPTIONS = "compare_options"
    COMPLETE_TASK = "complete_task"
    FIND_LOCAL_RESOURCE = "find_local_resource"
    FIND_PERSON = "find_person"
    FIND_ORGANIZATION = "find_organization"
    FIND_PRODUCT = "find_product"
    FIND_SERVICE = "find_service"
    FIND_VIDEO = "find_video"
    FIND_IMAGE = "find_image"
    FIND_NEWS = "find_news"
    FIND_DOCUMENT = "find_document"
    FIND_DEFINITION = "find_definition"
    FIND_INSTRUCTIONS = "find_instructions"
    FIND_PRICE = "find_price"
    FIND_LOCATION = "find_location"
    FIND_EVENT = "find_event"
    EXPLORE_TOPIC = "explore_topic"
    UNKNOWN = "unknown"


class IntentSignalType(str, Enum):
    LEXICAL = "lexical"
    STRUCTURAL = "structural"
    FIELD = "field"
    PHRASE = "phrase"
    URL = "url"
    NUMBER = "number"
    TEMPORAL = "temporal"
    LOCAL = "local"
    ACTION = "action"
    ENTITY = "entity"
    MEDIA = "media"
    COMPARISON = "comparison"


class ConfidenceLevel(str, Enum):
    VERY_LOW = "very_low"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


class IntentAnalysisEventType(str, Enum):
    ANALYSIS_RECEIVED = "analysis_received"
    ANALYSIS_STARTED = "analysis_started"
    SIGNALS_EXTRACTED = "signals_extracted"
    QUERY_TYPE_DETECTED = "query_type_detected"
    INTENT_DETECTED = "intent_detected"
    CONFIDENCE_CALCULATED = "confidence_calculated"
    ALTERNATIVE_INTENTS_CREATED = "alternative_intents_created"
    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_REJECTED = "analysis_rejected"
    CHECKPOINT_CREATED = "checkpoint_created"


# ---------------------------------------------------------------------------
# Input contract
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntentAnalysisRequest:
    canonical_query: Any
    request_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryIntentSignal:
    signal_id: str
    signal_type: IntentSignalType
    value: str
    weight: float
    evidence: str


@dataclass(frozen=True)
class QuerySignalProfile:
    query_id: str
    term_count: int
    phrase_count: int
    has_url: bool
    has_email: bool
    has_numbers: bool
    has_field_constraints: bool
    has_operators: bool

    lexical_signals: Tuple[QueryIntentSignal, ...]
    structural_signals: Tuple[QueryIntentSignal, ...]
    field_signals: Tuple[QueryIntentSignal, ...]
    temporal_signals: Tuple[QueryIntentSignal, ...]
    local_signals: Tuple[QueryIntentSignal, ...]
    action_signals: Tuple[QueryIntentSignal, ...]
    media_signals: Tuple[QueryIntentSignal, ...]
    comparison_signals: Tuple[QueryIntentSignal, ...]
    entity_signals: Tuple[QueryIntentSignal, ...]


# ---------------------------------------------------------------------------
# Intent candidates
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntentCandidate:
    intent: QueryIntent
    query_type: QueryType
    confidence_score: float
    confidence_level: ConfidenceLevel
    supporting_signals: Tuple[str, ...]


@dataclass(frozen=True)
class QueryIntentDecision:
    primary_type: QueryType
    primary_intent: QueryIntent
    confidence_score: float
    confidence_level: ConfidenceLevel
    alternatives: Tuple[IntentCandidate, ...]


# ---------------------------------------------------------------------------
# Analysis identity
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntentAnalysisIdentity:
    analysis_id: str
    source_query_id: str
    canonical_query_hash: str
    analysis_hash: str
    created_at: float
    architecture_version: str = ARCHITECTURE_VERSION


# ---------------------------------------------------------------------------
# Final analysis representation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryIntentAnalysis:
    identity: IntentAnalysisIdentity

    query_id: str
    canonical_query_hash: str

    query_type: QueryType
    primary_intent: QueryIntent

    confidence_score: float
    confidence_level: ConfidenceLevel

    alternatives: Tuple[IntentCandidate, ...]

    signals: QuerySignalProfile

    retrieval_characteristics: Tuple[str, ...]
    downstream_requirements: Tuple[str, ...]

    version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class IntentAnalysisResult:
    state: IntentAnalysisState
    analysis: Optional[QueryIntentAnalysis]
    errors: Tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntentAnalysisCheckpoint:
    checkpoint_id: str
    epoch: int
    created_at: float
    processed_queries: int
    architecture_version: str = ARCHITECTURE_VERSION


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntentAnalysisEvent:
    event_id: str
    event_type: IntentAnalysisEventType
    query_id: Optional[str]
    epoch: int
    created_at: float
    payload: Mapping[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Backend contract
# ---------------------------------------------------------------------------

class QueryIntentAnalysisBackend(Protocol):

    def record_event(
        self,
        event: IntentAnalysisEvent,
    ) -> None:
        ...

    def save_checkpoint(
        self,
        checkpoint: IntentAnalysisCheckpoint,
    ) -> None:
        ...

    def get_epoch(self) -> int:
        ...

    def advance_epoch(self) -> int:
        ...


# ---------------------------------------------------------------------------
# Reference backend
# ---------------------------------------------------------------------------

class InMemoryQueryIntentAnalysisMetadata:

    def __init__(self) -> None:
        self.events: List[IntentAnalysisEvent] = []
        self.checkpoints: List[IntentAnalysisCheckpoint] = []
        self._epoch = 0

    def record_event(
        self,
        event: IntentAnalysisEvent,
    ) -> None:
        self.events.append(event)

    def save_checkpoint(
        self,
        checkpoint: IntentAnalysisCheckpoint,
    ) -> None:
        self.checkpoints.append(checkpoint)

    def get_epoch(self) -> int:
        return self._epoch

    def advance_epoch(self) -> int:
        self._epoch += 1
        return self._epoch


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QueryIntentAnalysisPolicy:

    minimum_confidence: float = 0.0
    maximum_alternatives: int = 5

    checkpoint_interval_queries: int = 1

    # Lexical intent vocabulary.
    navigation_terms: Tuple[str, ...] = (
        "website",
        "official",
        "homepage",
        "login",
        "portal",
    )

    informational_terms: Tuple[str, ...] = (
        "what",
        "why",
        "how",
        "meaning",
        "definition",
        "explain",
        "information",
        "about",
    )

    research_terms: Tuple[str, ...] = (
        "research",
        "study",
        "papers",
        "literature",
        "analysis",
        "report",
        "history",
    )

    transactional_terms: Tuple[str, ...] = (
        "buy",
        "purchase",
        "order",
        "shop",
        "price",
        "cost",
        "deal",
    )

    local_terms: Tuple[str, ...] = (
        "near",
        "nearby",
        "local",
        "restaurant",
        "hospital",
        "hotel",
        "school",
        "college",
        "store",
        "shop",
        "clinic",
    )

    comparison_terms: Tuple[str, ...] = (
        "vs",
        "versus",
        "compare",
        "comparison",
        "difference",
        "better",
        "between",
    )

    media_terms: Tuple[str, ...] = (
        "video",
        "videos",
        "image",
        "images",
        "photo",
        "photos",
        "movie",
        "music",
    )

    procedural_terms: Tuple[str, ...] = (
        "how",
        "steps",
        "guide",
        "tutorial",
        "instructions",
        "install",
        "setup",
        "fix",
        "repair",
    )

    temporal_terms: Tuple[str, ...] = (
        "today",
        "yesterday",
        "tomorrow",
        "latest",
        "recent",
        "current",
        "now",
        "2024",
        "2025",
        "2026",
    )

    entity_terms: Tuple[str, ...] = (
        "who",
        "person",
        "company",
        "organization",
        "university",
        "president",
        "ceo",
    )


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------

class QueryTypeIntentAnalysis:
    """
    Phase 11.3 query type and intent analysis.

    The implementation is deliberately deterministic and explainable.

    It creates an intent hypothesis that later retrieval/ranking systems
    can consume. It does not decide which document should win.

    The lexical rules here are architecture-level signals, not a claim
    that this finite vocabulary is sufficient for the entire Web.
    """

    def __init__(
        self,
        backend: Optional[QueryIntentAnalysisBackend] = None,
        policy: Optional[QueryIntentAnalysisPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryQueryIntentAnalysisMetadata()
        self.policy = policy or QueryIntentAnalysisPolicy()

        self.processed_queries = 0

    # ------------------------------------------------------------------
    # Epoch
    # ------------------------------------------------------------------

    @property
    def epoch(self) -> int:
        return self.backend.get_epoch()

    def advance_epoch(self) -> int:
        return self.backend.advance_epoch()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(
        self,
        request: IntentAnalysisRequest | Any,
    ) -> IntentAnalysisResult:

        if not isinstance(
            request,
            IntentAnalysisRequest,
        ):
            request = IntentAnalysisRequest(
                canonical_query=request,
            )

        query_id = self._query_id(
            request.canonical_query
        )

        self._emit(
            IntentAnalysisEventType.ANALYSIS_RECEIVED,
            query_id,
            {},
        )

        errors = self._validate(
            request.canonical_query
        )

        if errors:
            self._emit(
                IntentAnalysisEventType.ANALYSIS_REJECTED,
                query_id,
                {"errors": errors},
            )

            return IntentAnalysisResult(
                state=IntentAnalysisState.REJECTED,
                analysis=None,
                errors=tuple(errors),
            )

        self._emit(
            IntentAnalysisEventType.ANALYSIS_STARTED,
            query_id,
            {},
        )

        signals = self._extract_signals(
            request.canonical_query
        )

        self._emit(
            IntentAnalysisEventType.SIGNALS_EXTRACTED,
            query_id,
            {
                "lexical": len(signals.lexical_signals),
                "structural": len(signals.structural_signals),
                "field": len(signals.field_signals),
                "temporal": len(signals.temporal_signals),
                "local": len(signals.local_signals),
                "action": len(signals.action_signals),
                "media": len(signals.media_signals),
                "comparison": len(signals.comparison_signals),
                "entity": len(signals.entity_signals),
            },
        )

        decision = self._decide(
            request.canonical_query,
            signals,
        )

        self._emit(
            IntentAnalysisEventType.QUERY_TYPE_DETECTED,
            query_id,
            {
                "query_type": decision.primary_type.value,
                "confidence": decision.confidence_score,
            },
        )

        self._emit(
            IntentAnalysisEventType.INTENT_DETECTED,
            query_id,
            {
                "intent": decision.primary_intent.value,
                "confidence": decision.confidence_score,
            },
        )

        self._emit(
            IntentAnalysisEventType.CONFIDENCE_CALCULATED,
            query_id,
            {
                "score": decision.confidence_score,
                "level": decision.confidence_level.value,
            },
        )

        if decision.alternatives:
            self._emit(
                IntentAnalysisEventType.ALTERNATIVE_INTENTS_CREATED,
                query_id,
                {
                    "count": len(decision.alternatives),
                },
            )

        retrieval_characteristics = (
            self._retrieval_characteristics(
                decision,
                signals,
            )
        )

        downstream_requirements = (
            self._downstream_requirements(
                decision,
                signals,
            )
        )

        canonical_hash = self._canonical_hash(
            request.canonical_query
        )

        analysis_hash = self._analysis_hash(
            canonical_hash,
            decision,
            signals,
        )

        identity = IntentAnalysisIdentity(
            analysis_id=_new_id(
                "query-intent-analysis"
            ),
            source_query_id=query_id,
            canonical_query_hash=canonical_hash,
            analysis_hash=analysis_hash,
            created_at=_now(),
        )

        analysis = QueryIntentAnalysis(
            identity=identity,
            query_id=query_id,
            canonical_query_hash=canonical_hash,
            query_type=decision.primary_type,
            primary_intent=decision.primary_intent,
            confidence_score=decision.confidence_score,
            confidence_level=decision.confidence_level,
            alternatives=decision.alternatives,
            signals=signals,
            retrieval_characteristics=tuple(
                retrieval_characteristics
            ),
            downstream_requirements=tuple(
                downstream_requirements
            ),
        )

        self.processed_queries += 1

        self._emit(
            IntentAnalysisEventType.ANALYSIS_COMPLETED,
            query_id,
            {
                "query_type": analysis.query_type.value,
                "primary_intent": analysis.primary_intent.value,
                "confidence": analysis.confidence_score,
            },
        )

        self._maybe_checkpoint()

        return IntentAnalysisResult(
            state=IntentAnalysisState.COMPLETED,
            analysis=analysis,
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(
        self,
        canonical_query: Any,
    ) -> List[str]:

        errors: List[str] = []

        if canonical_query is None:
            errors.append(
                "canonical_query_is_null"
            )
            return errors

        required = (
            "identity",
            "normalized_query",
            "tokens",
        )

        for attribute in required:
            if not hasattr(
                canonical_query,
                attribute,
            ):
                errors.append(
                    f"canonical_query_missing_{attribute}"
                )

        return errors

    # ------------------------------------------------------------------
    # Query identity helpers
    # ------------------------------------------------------------------

    def _query_id(
        self,
        canonical_query: Any,
    ) -> str:

        identity = getattr(
            canonical_query,
            "identity",
            None,
        )

        if identity is not None:
            value = getattr(
                identity,
                "query_id",
                None,
            )

            if value:
                return str(value)

        return _new_id("query")

    def _canonical_hash(
        self,
        canonical_query: Any,
    ) -> str:

        identity = getattr(
            canonical_query,
            "identity",
            None,
        )

        if identity is not None:
            value = getattr(
                identity,
                "canonical_hash",
                None,
            )

            if value:
                return str(value)

        return _hash(
            str(
                getattr(
                    canonical_query,
                    "normalized_query",
                    "",
                )
            )
        )

    # ------------------------------------------------------------------
    # Signal extraction
    # ------------------------------------------------------------------

    def _extract_signals(
        self,
        canonical_query: Any,
    ) -> QuerySignalProfile:

        tokens = tuple(
            getattr(
                canonical_query,
                "tokens",
                (),
            )
        )

        phrases = tuple(
            getattr(
                canonical_query,
                "phrases",
                (),
            )
        )

        constraints = tuple(
            getattr(
                canonical_query,
                "field_constraints",
                (),
            )
        )

        operators = tuple(
            getattr(
                canonical_query,
                "operators",
                (),
            )
        )

        terms = [
            str(
                getattr(
                    token,
                    "normalized_text",
                    "",
                )
            ).casefold()
            for token in tokens
        ]

        lexical: List[QueryIntentSignal] = []
        structural: List[QueryIntentSignal] = []
        field_signals: List[QueryIntentSignal] = []
        temporal: List[QueryIntentSignal] = []
        local: List[QueryIntentSignal] = []
        action: List[QueryIntentSignal] = []
        media: List[QueryIntentSignal] = []
        comparison: List[QueryIntentSignal] = []
        entity: List[QueryIntentSignal] = []

        def add(
            target: List[QueryIntentSignal],
            signal_type: IntentSignalType,
            value: str,
            weight: float,
            evidence: str,
        ) -> None:
            target.append(
                QueryIntentSignal(
                    signal_id=_new_id(
                        "intent-signal"
                    ),
                    signal_type=signal_type,
                    value=value,
                    weight=weight,
                    evidence=evidence,
                )
            )

        for term in terms:

            if term in self.policy.navigation_terms:
                add(
                    lexical,
                    IntentSignalType.LEXICAL,
                    term,
                    1.0,
                    "navigation vocabulary",
                )

            if term in self.policy.informational_terms:
                add(
                    lexical,
                    IntentSignalType.LEXICAL,
                    term,
                    0.8,
                    "informational vocabulary",
                )

            if term in self.policy.research_terms:
                add(
                    lexical,
                    IntentSignalType.LEXICAL,
                    term,
                    0.9,
                    "research vocabulary",
                )

            if term in self.policy.transactional_terms:
                add(
                    action,
                    IntentSignalType.ACTION,
                    term,
                    1.0,
                    "transaction vocabulary",
                )

            if term in self.policy.local_terms:
                add(
                    local,
                    IntentSignalType.LOCAL,
                    term,
                    0.9,
                    "local vocabulary",
                )

            if term in self.policy.comparison_terms:
                add(
                    comparison,
                    IntentSignalType.COMPARISON,
                    term,
                    1.0,
                    "comparison vocabulary",
                )

            if term in self.policy.media_terms:
                add(
                    media,
                    IntentSignalType.MEDIA,
                    term,
                    1.0,
                    "media vocabulary",
                )

            if term in self.policy.procedural_terms:
                add(
                    action,
                    IntentSignalType.ACTION,
                    term,
                    0.9,
                    "procedural vocabulary",
                )

            if term in self.policy.temporal_terms:
                add(
                    temporal,
                    IntentSignalType.TEMPORAL,
                    term,
                    1.0,
                    "temporal vocabulary",
                )

            if term in self.policy.entity_terms:
                add(
                    entity,
                    IntentSignalType.ENTITY,
                    term,
                    0.8,
                    "entity vocabulary",
                )

        # URLs are strong navigational signals.
        for token in tokens:
            kind = str(
                getattr(
                    token,
                    "kind",
                    "",
                )
            )

            if kind.endswith("URL"):
                add(
                    structural,
                    IntentSignalType.URL,
                    "url",
                    1.5,
                    "explicit URL token",
                )

            if kind.endswith("EMAIL"):
                add(
                    structural,
                    IntentSignalType.STRUCTURAL,
                    "email",
                    0.5,
                    "email token",
                )

            if kind.endswith("NUMBER"):
                add(
                    structural,
                    IntentSignalType.NUMBER,
                    "number",
                    0.2,
                    "numeric token",
                )

        if phrases:
            add(
                structural,
                IntentSignalType.PHRASE,
                "quoted_phrase",
                0.7,
                "explicit quoted phrase",
            )

        if constraints:
            for constraint in constraints:
                field_name = str(
                    getattr(
                        constraint,
                        "field",
                        "any",
                    )
                )

                value = str(
                    getattr(
                        constraint,
                        "normalized_value",
                        "",
                    )
                )

                add(
                    field_signals,
                    IntentSignalType.FIELD,
                    field_name,
                    1.0,
                    value,
                )

        if operators:
            add(
                structural,
                IntentSignalType.STRUCTURAL,
                "boolean_operator",
                0.4,
                "explicit query operator",
            )

        return QuerySignalProfile(
            query_id=self._query_id(
                canonical_query
            ),
            term_count=len(terms),
            phrase_count=len(phrases),
            has_url=any(
                "url" in str(
                    getattr(
                        token,
                        "kind",
                        "",
                    )
                ).casefold()
                for token in tokens
            ),
            has_email=any(
                "email" in str(
                    getattr(
                        token,
                        "kind",
                        "",
                    )
                ).casefold()
                for token in tokens
            ),
            has_numbers=any(
                "number" in str(
                    getattr(
                        token,
                        "kind",
                        "",
                    )
                ).casefold()
                for token in tokens
            ),
            has_field_constraints=bool(
                constraints
            ),
            has_operators=bool(
                operators
            ),
            lexical_signals=tuple(lexical),
            structural_signals=tuple(structural),
            field_signals=tuple(field_signals),
            temporal_signals=tuple(temporal),
            local_signals=tuple(local),
            action_signals=tuple(action),
            media_signals=tuple(media),
            comparison_signals=tuple(comparison),
            entity_signals=tuple(entity),
        )

    # ------------------------------------------------------------------
    # Decision engine
    # ------------------------------------------------------------------

    def _decide(
        self,
        canonical_query: Any,
        signals: QuerySignalProfile,
    ) -> QueryIntentDecision:

        scores: Dict[QueryIntent, float] = {
            intent: 0.0
            for intent in QueryIntent
        }

        evidence: Dict[
            QueryIntent,
            List[str],
        ] = {
            intent: []
            for intent in QueryIntent
        }

        def boost(
            intent: QueryIntent,
            amount: float,
            reason: str,
        ) -> None:
            scores[intent] += amount
            evidence[intent].append(reason)

        # URL / navigation.
        if signals.has_url:
            boost(
                QueryIntent.FIND_WEBSITE,
                3.0,
                "explicit URL",
            )

        for signal in signals.lexical_signals:
            if signal.value in self.policy.navigation_terms:
                boost(
                    QueryIntent.FIND_WEBSITE,
                    signal.weight,
                    signal.evidence,
                )

        # Information.
        for signal in signals.lexical_signals:
            if signal.value in self.policy.informational_terms:
                boost(
                    QueryIntent.LEARN_TOPIC,
                    signal.weight,
                    signal.evidence,
                )

                if signal.value == "definition":
                    boost(
                        QueryIntent.FIND_DEFINITION,
                        1.4,
                        "definition keyword",
                    )

                if signal.value == "what":
                    boost(
                        QueryIntent.ANSWER_FACT,
                        0.8,
                        "question vocabulary",
                    )

        # Research.
        for signal in signals.lexical_signals:
            if signal.value in self.policy.research_terms:
                boost(
                    QueryIntent.RESEARCH_TOPIC,
                    signal.weight,
                    signal.evidence,
                )

        # Transaction.
        for signal in signals.action_signals:
            if signal.value in self.policy.transactional_terms:
                boost(
                    QueryIntent.COMPLETE_TASK,
                    signal.weight,
                    signal.evidence,
                )

                if signal.value in {
                    "price",
                    "cost",
                    "deal",
                }:
                    boost(
                        QueryIntent.FIND_PRICE,
                        1.3,
                        "price vocabulary",
                    )

                if signal.value in {
                    "buy",
                    "purchase",
                    "order",
                    "shop",
                }:
                    boost(
                        QueryIntent.FIND_PRODUCT,
                        1.0,
                        "purchase vocabulary",
                    )

        # Local.
        if signals.local_signals:
            boost(
                QueryIntent.FIND_LOCAL_RESOURCE,
                2.0,
                "local vocabulary",
            )

            for signal in signals.local_signals:
                if signal.value in {
                    "restaurant",
                    "hotel",
                    "hospital",
                    "school",
                    "college",
                    "clinic",
                    "store",
                    "shop",
                }:
                    boost(
                        QueryIntent.FIND_SERVICE,
                        0.7,
                        signal.evidence,
                    )

        # Comparison.
        if signals.comparison_signals:
            boost(
                QueryIntent.COMPARE_OPTIONS,
                2.5,
                "comparison vocabulary",
            )

        # Media.
        for signal in signals.media_signals:
            boost(
                QueryIntent.FIND_VIDEO
                if signal.value in {
                    "video",
                    "videos",
                    "movie",
                }
                else QueryIntent.FIND_IMAGE,
                1.4,
                signal.evidence,
            )

        # Procedures.
        for signal in signals.action_signals:
            if signal.value in self.policy.procedural_terms:
                boost(
                    QueryIntent.FIND_INSTRUCTIONS,
                    1.2,
                    signal.evidence,
                )

        # Entity.
        for signal in signals.entity_signals:
            if signal.value in {
                "who",
                "person",
                "president",
                "ceo",
            }:
                boost(
                    QueryIntent.FIND_PERSON,
                    1.3,
                    signal.evidence,
                )

            if signal.value in {
                "company",
                "organization",
                "university",
            }:
                boost(
                    QueryIntent.FIND_ORGANIZATION,
                    1.3,
                    signal.evidence,
                )

        # Temporal.
        if signals.temporal_signals:
            boost(
                QueryIntent.FIND_NEWS,
                0.8,
                "temporal signal",
            )

        # Explicit URL is dominant.
        if signals.has_url:
            primary_intent = QueryIntent.FIND_WEBSITE

        else:
            ranked = sorted(
                scores.items(),
                key=lambda item: (
                    item[1],
                    item[0].value,
                ),
                reverse=True,
            )

            primary_intent = ranked[0][0]

            if scores[primary_intent] <= 0:
                primary_intent = QueryIntent.EXPLORE_TOPIC
                scores[
                    primary_intent
                ] = 0.1

        primary_type = self._type_for_intent(
            primary_intent,
            signals,
        )

        confidence = self._confidence(
            scores,
            primary_intent,
            signals,
        )

        alternatives: List[IntentCandidate] = []

        for intent, score in sorted(
            scores.items(),
            key=lambda item: item[1],
            reverse=True,
        ):
            if intent == primary_intent:
                continue

            if score <= 0:
                continue

            alternative_confidence = min(
                1.0,
                score / (
                    scores[primary_intent] + 1e-9
                ),
            )

            alternatives.append(
                IntentCandidate(
                    intent=intent,
                    query_type=self._type_for_intent(
                        intent,
                        signals,
                    ),
                    confidence_score=round(
                        alternative_confidence,
                        6,
                    ),
                    confidence_level=self._confidence_level(
                        alternative_confidence
                    ),
                    supporting_signals=tuple(
                        evidence[intent]
                    ),
                )
            )

            if len(alternatives) >= (
                self.policy.maximum_alternatives
            ):
                break

        return QueryIntentDecision(
            primary_type=primary_type,
            primary_intent=primary_intent,
            confidence_score=round(
                confidence,
                6,
            ),
            confidence_level=self._confidence_level(
                confidence
            ),
            alternatives=tuple(alternatives),
        )

    # ------------------------------------------------------------------
    # Type mapping
    # ------------------------------------------------------------------

    def _type_for_intent(
        self,
        intent: QueryIntent,
        signals: QuerySignalProfile,
    ) -> QueryType:

        if intent == QueryIntent.FIND_WEBSITE:
            return QueryType.NAVIGATIONAL

        if intent in {
            QueryIntent.FIND_LOCAL_RESOURCE,
            QueryIntent.FIND_SERVICE,
            QueryIntent.FIND_LOCATION,
        }:
            return QueryType.LOCAL

        if intent in {
            QueryIntent.FIND_PRODUCT,
            QueryIntent.FIND_PRICE,
            QueryIntent.COMPLETE_TASK,
        }:
            return QueryType.TRANSACTIONAL

        if intent in {
            QueryIntent.FIND_VIDEO,
            QueryIntent.FIND_IMAGE,
            QueryIntent.FIND_NEWS,
        }:
            return QueryType.MEDIA

        if intent in {
            QueryIntent.COMPARE_OPTIONS,
        }:
            return QueryType.COMPARISON

        if intent in {
            QueryIntent.FIND_PERSON,
            QueryIntent.FIND_ORGANIZATION,
        }:
            return QueryType.ENTITY

        if intent in {
            QueryIntent.FIND_INSTRUCTIONS,
        }:
            return QueryType.PROCEDURAL

        if intent in {
            QueryIntent.RESEARCH_TOPIC,
            QueryIntent.RESEARCH_TOPIC,
        }:
            return QueryType.INFORMATIONAL

        if intent in {
            QueryIntent.ANSWER_FACT,
            QueryIntent.FIND_DEFINITION,
        }:
            return QueryType.FACTUAL

        if signals.temporal_signals:
            return QueryType.TIME_SENSITIVE

        if intent in {
            QueryIntent.LEARN_TOPIC,
            QueryIntent.EXPLORE_TOPIC,
        }:
            return QueryType.INFORMATIONAL

        return QueryType.GENERAL

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------

    def _confidence(
        self,
        scores: Mapping[QueryIntent, float],
        primary: QueryIntent,
        signals: QuerySignalProfile,
    ) -> float:

        score = scores.get(
            primary,
            0.0,
        )

        total_positive = sum(
            max(value, 0.0)
            for value in scores.values()
        )

        if total_positive <= 0:
            return 0.1

        concentration = (
            score / total_positive
        )

        evidence_count = sum(
            len(values)
            for values in [
                signals.lexical_signals,
                signals.structural_signals,
                signals.field_signals,
                signals.temporal_signals,
                signals.local_signals,
                signals.action_signals,
                signals.media_signals,
                signals.comparison_signals,
                signals.entity_signals,
            ]
        )

        evidence_factor = min(
            1.0,
            0.35
            + (
                evidence_count * 0.08
            ),
        )

        confidence = (
            concentration * 0.65
            + evidence_factor * 0.35
        )

        return max(
            0.0,
            min(1.0, confidence),
        )

    def _confidence_level(
        self,
        score: float,
    ) -> ConfidenceLevel:

        if score >= 0.9:
            return ConfidenceLevel.VERY_HIGH

        if score >= 0.75:
            return ConfidenceLevel.HIGH

        if score >= 0.5:
            return ConfidenceLevel.MEDIUM

        if score >= 0.25:
            return ConfidenceLevel.LOW

        return ConfidenceLevel.VERY_LOW

    # ------------------------------------------------------------------
    # Retrieval characteristics
    # ------------------------------------------------------------------

    def _retrieval_characteristics(
        self,
        decision: QueryIntentDecision,
        signals: QuerySignalProfile,
    ) -> List[str]:

        result: List[str] = []

        if decision.primary_type == QueryType.NAVIGATIONAL:
            result.extend([
                "strong_exact_match_candidates",
                "official_site_candidates",
                "url_and_domain_signals",
            ])

        if decision.primary_type == QueryType.INFORMATIONAL:
            result.extend([
                "broad_content_retrieval",
                "topic_relevance",
                "multiple_source_candidates",
            ])

        if decision.primary_type == QueryType.FACTUAL:
            result.extend([
                "high_precision_candidate_retrieval",
                "entity_and_fact_sources",
            ])

        if decision.primary_type == QueryType.TRANSACTIONAL:
            result.extend([
                "product_and_service_candidates",
                "price_signals",
                "commercial_intent_signals",
            ])

        if decision.primary_type == QueryType.LOCAL:
            result.extend([
                "location_sensitive_retrieval",
                "local_entity_candidates",
                "geographic_signals",
            ])

        if decision.primary_type == QueryType.MEDIA:
            result.extend([
                "media_document_candidates",
                "media_field_signals",
            ])

        if decision.primary_type == QueryType.COMPARISON:
            result.extend([
                "multi_entity_retrieval",
                "comparison_candidate_groups",
            ])

        if decision.primary_type == QueryType.PROCEDURAL:
            result.extend([
                "instruction_content",
                "step_based_documents",
            ])

        if decision.primary_type == QueryType.ENTITY:
            result.extend([
                "entity_centric_retrieval",
                "entity_identity_resolution",
            ])

        if signals.temporal_signals:
            result.append(
                "freshness_sensitive_retrieval"
            )

        if signals.has_field_constraints:
            result.append(
                "field_restricted_retrieval"
            )

        if signals.has_operators:
            result.append(
                "boolean_constraint_processing"
            )

        if signals.phrase_count:
            result.append(
                "phrase_sensitive_matching"
            )

        return result

    # ------------------------------------------------------------------
    # Downstream requirements
    # ------------------------------------------------------------------

    def _downstream_requirements(
        self,
        decision: QueryIntentDecision,
        signals: QuerySignalProfile,
    ) -> List[str]:

        requirements = [
            "phase_11_4_term_and_field_expansion",
            "phase_11_5_distributed_retrieval_planning",
        ]

        if decision.primary_type == QueryType.LOCAL:
            requirements.append(
                "location_aware_retrieval"
            )

        if decision.primary_type == QueryType.ENTITY:
            requirements.append(
                "entity_resolution"
            )

        if decision.primary_type == QueryType.NAVIGATIONAL:
            requirements.append(
                "domain_and_url_candidate_routing"
            )

        if signals.temporal_signals:
            requirements.append(
                "freshness_aware_candidate_selection"
            )

        if signals.comparison_signals:
            requirements.append(
                "multi_entity_candidate_fanout"
            )

        return requirements

    # ------------------------------------------------------------------
    # Stable analysis hash
    # ------------------------------------------------------------------

    def _analysis_hash(
        self,
        canonical_hash: str,
        decision: QueryIntentDecision,
        signals: QuerySignalProfile,
    ) -> str:

        payload = {
            "canonical_hash": canonical_hash,
            "primary_type": decision.primary_type.value,
            "primary_intent": decision.primary_intent.value,
            "confidence": decision.confidence_score,
            "alternatives": [
                (
                    candidate.intent.value,
                    candidate.query_type.value,
                )
                for candidate in decision.alternatives
            ],
            "signal_counts": {
                "lexical": len(
                    signals.lexical_signals
                ),
                "structural": len(
                    signals.structural_signals
                ),
                "field": len(
                    signals.field_signals
                ),
                "temporal": len(
                    signals.temporal_signals
                ),
                "local": len(
                    signals.local_signals
                ),
                "action": len(
                    signals.action_signals
                ),
                "media": len(
                    signals.media_signals
                ),
                "comparison": len(
                    signals.comparison_signals
                ),
                "entity": len(
                    signals.entity_signals
                ),
            },
        }

        return _hash(
            repr(payload)
        )

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------

    def _maybe_checkpoint(self) -> None:

        interval = (
            self.policy.checkpoint_interval_queries
        )

        if interval <= 0:
            return

        if (
            self.processed_queries % interval
            != 0
        ):
            return

        checkpoint = IntentAnalysisCheckpoint(
            checkpoint_id=_new_id(
                "intent-analysis-checkpoint"
            ),
            epoch=self.epoch,
            created_at=_now(),
            processed_queries=self.processed_queries,
        )

        self.backend.save_checkpoint(
            checkpoint
        )

        self._emit(
            IntentAnalysisEventType.CHECKPOINT_CREATED,
            None,
            {
                "checkpoint_id":
                    checkpoint.checkpoint_id,
                "processed_queries":
                    self.processed_queries,
            },
        )

    # ------------------------------------------------------------------
    # Event emission
    # ------------------------------------------------------------------

    def _emit(
        self,
        event_type: IntentAnalysisEventType,
        query_id: Optional[str],
        payload: Mapping[str, Any],
    ) -> None:

        event = IntentAnalysisEvent(
            event_id=_new_id(
                "intent-analysis-event"
            ),
            event_type=event_type,
            query_id=query_id,
            epoch=self.epoch,
            created_at=_now(),
            payload=dict(payload),
        )

        self.backend.record_event(event)

    # ------------------------------------------------------------------
    # Architecture contract
    # ------------------------------------------------------------------

    def architecture(self) -> Dict[str, Any]:

        return {
            "name":
                "Query Type and Intent Analysis",

            "phase":
                "11.3",

            "version":
                ARCHITECTURE_VERSION,

            "scale_target":
                SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "input":
                "Phase 11.2 canonical query",

            "pipeline": [
                "canonical_query_validation",
                "query_signal_extraction",
                "structural_signal_analysis",
                "lexical_intent_signal_analysis",
                "field_signal_analysis",
                "temporal_signal_analysis",
                "local_signal_analysis",
                "action_signal_analysis",
                "media_signal_analysis",
                "comparison_signal_analysis",
                "entity_signal_analysis",
                "query_type_detection",
                "primary_intent_detection",
                "alternative_intent_generation",
                "confidence_calculation",
                "retrieval_characteristic_generation",
                "downstream_requirement_generation",
                "analysis_fingerprint_generation",
                "checkpointing",
            ],

            "query_types": [
                value.value
                for value in QueryType
            ],

            "intents": [
                value.value
                for value in QueryIntent
            ],

            "outputs": [
                "query_type",
                "primary_intent",
                "confidence_score",
                "confidence_level",
                "alternative_intents",
                "query_signal_profile",
                "retrieval_characteristics",
                "downstream_requirements",
                "analysis_hash",
            ],

            "distributed_design": {
                "deterministic_signal_processing": True,
                "horizontal_query_processing": True,
                "single_global_intent_worker_required": False,
                "single_global_intent_database_required": False,
                "single_global_query_file_required": False,
                "fixed_global_query_capacity": False,
                "epoch_aware": True,
                "checkpointable": True,
                "replaceable_backend": True,
            },

            "semantic_boundary": {
                "does_not_retrieve_documents": True,
                "does_not_rank_documents": True,
                "does_not_generate_embeddings": True,
                "does_not_perform_final_semantic_expansion": True,
                "does_not_modify_index": True,
                "expansion_phase": "11.4",
                "retrieval_planning_phase": "11.5",
            },

            "scale_properties": {
                "billions_of_resources_supported_by_architecture":
                    True,
                "trillions_of_resources_supported_by_architecture":
                    True,
                "global_horizontal_scaling":
                    True,
                "partition_independent":
                    True,
                "region_independent":
                    True,
                "backend_replaceability":
                    True,
            },

            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],
        }


# ---------------------------------------------------------------------------
# Stable aliases
# ---------------------------------------------------------------------------

QueryIntentAnalysisArchitecture = QueryTypeIntentAnalysis
GlobalQueryIntentAnalysis = QueryTypeIntentAnalysis
Phase11_3QueryTypeIntentAnalysis = QueryTypeIntentAnalysis


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "IntentAnalysisState",
    "QueryType",
    "QueryIntent",
    "IntentSignalType",
    "ConfidenceLevel",
    "IntentAnalysisEventType",

    "IntentAnalysisRequest",
    "QueryIntentSignal",
    "QuerySignalProfile",
    "IntentCandidate",
    "QueryIntentDecision",
    "IntentAnalysisIdentity",
    "QueryIntentAnalysis",
    "IntentAnalysisResult",
    "IntentAnalysisCheckpoint",
    "IntentAnalysisEvent",

    "QueryIntentAnalysisBackend",
    "InMemoryQueryIntentAnalysisMetadata",
    "QueryIntentAnalysisPolicy",

    "QueryTypeIntentAnalysis",
    "QueryIntentAnalysisArchitecture",
    "GlobalQueryIntentAnalysis",
    "Phase11_3QueryTypeIntentAnalysis",
]
