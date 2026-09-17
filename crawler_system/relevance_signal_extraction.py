from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ============================================================
# OUR SEARCH
# Phase 12.2 — Relevance Signal Extraction
# ============================================================

ARCHITECTURE_VERSION = "relevance-signal-extraction.v1"
SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _hash(value: Any) -> str:
    return sha256(
        repr(value).encode("utf-8", errors="replace")
    ).hexdigest()


# ============================================================
# STATES
# ============================================================

class RelevanceExtractionState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    QUERY_SIGNAL_ANALYSIS = "query_signal_analysis"
    DOCUMENT_SIGNAL_ANALYSIS = "document_signal_analysis"
    MATCH_ANALYSIS = "match_analysis"
    CONTEXT_ANALYSIS = "context_analysis"
    SIGNAL_AGGREGATION = "signal_aggregation"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class RelevanceSignalFamily(str, Enum):
    EXACT_TERM = "exact_term"
    TERM_COVERAGE = "term_coverage"
    PHRASE_MATCH = "phrase_match"
    FIELD_MATCH = "field_match"
    TITLE_MATCH = "title_match"
    URL_MATCH = "url_match"
    ANCHOR_MATCH = "anchor_match"
    CONTENT_MATCH = "content_match"
    PROXIMITY = "proximity"
    QUERY_COVERAGE = "query_coverage"
    INTENT_MATCH = "intent_match"
    SEMANTIC_CONTEXT = "semantic_context"


class RelevanceSignalStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    EXACT = "exact"


class RelevanceArtifactState(str, Enum):
    RECEIVED = "received"
    VALID = "valid"
    PARTIAL = "partial"
    COMPLETE = "complete"
    INVALID = "invalid"


class RelevanceEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    EXTRACTION_STARTED = "extraction_started"
    QUERY_SIGNALS_EXTRACTED = "query_signals_extracted"
    DOCUMENT_SIGNALS_EXTRACTED = "document_signals_extracted"
    MATCH_SIGNALS_EXTRACTED = "match_signals_extracted"
    CONTEXT_SIGNALS_EXTRACTED = "context_signals_extracted"
    SIGNALS_AGGREGATED = "signals_aggregated"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    EXTRACTION_COMPLETED = "extraction_completed"
    EXTRACTION_REJECTED = "extraction_rejected"
    EXTRACTION_FAILED = "extraction_failed"


# ============================================================
# IDENTITIES
# ============================================================

@dataclass(frozen=True)
class RelevanceExtractionIdentity:
    extraction_id: str
    ranking_id: str
    retrieval_id: str
    query_id: str
    candidate_set_id: str
    architecture_version: str = ARCHITECTURE_VERSION
    created_at: datetime = field(default_factory=_now)


@dataclass(frozen=True)
class RelevanceLineage:
    extraction_id: str
    ranking_id: str
    retrieval_id: str
    query_id: str
    canonical_query_hash: str
    retrieval_candidate_hash: str
    extraction_input_hash: str


# ============================================================
# QUERY SIGNALS
# ============================================================

@dataclass(frozen=True)
class QueryRelevanceProfile:
    original_query: str

    normalized_query: str

    terms: Tuple[str, ...] = ()
    phrases: Tuple[str, ...] = ()

    field_constraints: Mapping[str, Tuple[str, ...]] = field(
        default_factory=dict
    )

    intent: Optional[str] = None

    expansion_terms: Tuple[str, ...] = ()

    query_hash: str = ""


# ============================================================
# DOCUMENT SIGNALS
# ============================================================

@dataclass(frozen=True)
class DocumentRelevanceProfile:
    resource_id: str

    title_terms: Tuple[str, ...] = ()
    body_terms: Tuple[str, ...] = ()
    url_terms: Tuple[str, ...] = ()
    anchor_terms: Tuple[str, ...] = ()

    fields: Mapping[str, Tuple[str, ...]] = field(
        default_factory=dict
    )

    phrases: Tuple[str, ...] = ()

    document_length: int = 0

    document_hash: str = ""


# ============================================================
# RELEVANCE SIGNAL
# ============================================================

@dataclass(frozen=True)
class RelevanceSignal:
    signal_id: str

    resource_id: str

    family: RelevanceSignalFamily
    strength: RelevanceSignalStrength

    value: float
    confidence: float = 1.0

    query_terms: Tuple[str, ...] = ()
    matched_terms: Tuple[str, ...] = ()

    source: Optional[str] = None

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ============================================================
# SIGNAL PROFILE
# ============================================================

@dataclass
class CandidateRelevanceProfile:

    resource_id: str

    signals: Tuple[RelevanceSignal, ...]

    aggregate_score: float = 0.0

    signal_hash: str = ""

    partial: bool = False

    def __post_init__(self) -> None:

        if self.aggregate_score == 0.0:
            self.aggregate_score = sum(
                signal.value
                * signal.confidence
                for signal in self.signals
            )

        if not self.signal_hash:

            self.signal_hash = _hash(
                tuple(
                    (
                        signal.family.value,
                        signal.strength.value,
                        signal.value,
                        signal.confidence,
                        signal.matched_terms,
                    )
                    for signal in self.signals
                )
            )


# ============================================================
# EXTRACTION RESULT
# ============================================================

@dataclass
class RelevanceExtractionResult:

    extraction_id: str

    state: RelevanceExtractionState

    query_profile: QueryRelevanceProfile

    candidate_profiles: Tuple[
        CandidateRelevanceProfile,
        ...
    ]

    candidate_count: int

    partial: bool

    extraction_hash: str

    created_at: datetime = field(default_factory=_now)


# ============================================================
# CHECKPOINT
# ============================================================

@dataclass(frozen=True)
class RelevanceExtractionCheckpoint:

    checkpoint_id: str

    extraction_id: str

    state: RelevanceExtractionState

    processed_candidates: int

    partial: bool

    extraction_hash: str

    created_at: datetime = field(default_factory=_now)


# ============================================================
# EVENTS
# ============================================================

@dataclass(frozen=True)
class RelevanceExtractionEvent:

    event_id: str

    extraction_id: str

    event_type: RelevanceEventType

    message: str

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )

    created_at: datetime = field(default_factory=_now)


# ============================================================
# BACKEND
# ============================================================

class RelevanceSignalExtractionBackend(Protocol):

    def persist_event(
        self,
        event: RelevanceExtractionEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: RelevanceExtractionCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: RelevanceExtractionResult,
    ) -> None:
        ...


class InMemoryRelevanceSignalMetadata:

    def __init__(self) -> None:

        self._events: List[
            RelevanceExtractionEvent
        ] = []

        self._checkpoints: List[
            RelevanceExtractionCheckpoint
        ] = []

        self._results: List[
            RelevanceExtractionResult
        ] = []

    def persist_event(
        self,
        event: RelevanceExtractionEvent,
    ) -> None:

        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: RelevanceExtractionCheckpoint,
    ) -> None:

        self._checkpoints.append(checkpoint)

    def persist_result(
        self,
        result: RelevanceExtractionResult,
    ) -> None:

        self._results.append(result)

    def events(
        self,
    ) -> Tuple[RelevanceExtractionEvent, ...]:

        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[
        RelevanceExtractionCheckpoint,
        ...
    ]:

        return tuple(self._checkpoints)

    def results(
        self,
    ) -> Tuple[
        RelevanceExtractionResult,
        ...
    ]:

        return tuple(self._results)


# ============================================================
# POLICY
# ============================================================

@dataclass(frozen=True)
class RelevanceSignalExtractionPolicy:

    # Operational per-execution safety controls.
    max_candidates_per_execution: int = 100000
    max_signals_per_candidate: int = 256
    max_query_terms: int = 512
    max_query_phrases: int = 256

    allow_partial_execution: bool = True

    require_candidate_identity: bool = True
    require_query: bool = True

    exact_term_weight: float = 4.0
    term_coverage_weight: float = 2.0
    phrase_weight: float = 4.0
    field_weight: float = 3.0
    title_weight: float = 3.0
    url_weight: float = 1.5
    anchor_weight: float = 1.5
    content_weight: float = 1.0
    proximity_weight: float = 2.0
    query_coverage_weight: float = 3.0
    intent_weight: float = 2.0
    semantic_context_weight: float = 1.0


# ============================================================
# ARCHITECTURE
# ============================================================

class RelevanceSignalExtractionArchitecture:

    """
    OUR SEARCH Phase 12.2.

    Extracts explicit relevance signals between the query and
    retrieved documents.

    This stage produces ranking evidence.

    It does NOT:
        - determine final ranking
        - determine authority
        - determine document quality
        - determine spam status
        - determine freshness
        - crawl the Web
        - mutate the index
        - replace Phase 11 retrieval
        - depend on Google technology

    Later Phase 12 stages consume these signals.
    """

    def __init__(
        self,
        backend: Optional[
            RelevanceSignalExtractionBackend
        ] = None,
        policy: Optional[
            RelevanceSignalExtractionPolicy
        ] = None,
    ) -> None:

        self.backend = (
            backend
            if backend is not None
            else InMemoryRelevanceSignalMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else RelevanceSignalExtractionPolicy()
        )

        self._events: List[
            RelevanceExtractionEvent
        ] = []

        self._checkpoints: List[
            RelevanceExtractionCheckpoint
        ] = []

    # --------------------------------------------------------
    # Events
    # --------------------------------------------------------

    def _emit(
        self,
        extraction_id: str,
        event_type: RelevanceEventType,
        message: str,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> RelevanceExtractionEvent:

        event = RelevanceExtractionEvent(
            event_id=_new_id(
                "relevance_event"
            ),
            extraction_id=extraction_id,
            event_type=event_type,
            message=message,
            metadata=dict(metadata or {}),
        )

        self._events.append(event)
        self.backend.persist_event(event)

        return event

    # --------------------------------------------------------
    # Tokenization
    # --------------------------------------------------------

    @staticmethod
    def _tokens(value: str) -> Tuple[str, ...]:

        return tuple(
            token
            for token in (
                value.lower()
                .replace(",", " ")
                .replace(".", " ")
                .replace(":", " ")
                .replace(";", " ")
                .replace("/", " ")
                .replace("-", " ")
                .split()
            )
            if token
        )

    @staticmethod
    def _phrases(
        value: str,
    ) -> Tuple[str, ...]:

        tokens = RelevanceSignalExtractionArchitecture._tokens(
            value
        )

        phrases = set()

        for size in (2, 3, 4):

            for index in range(
                max(0, len(tokens) - size + 1)
            ):

                phrases.add(
                    " ".join(
                        tokens[
                            index:
                            index + size
                        ]
                    )
                )

        return tuple(sorted(phrases))

    # --------------------------------------------------------
    # Query profile
    # --------------------------------------------------------

    def _build_query_profile(
        self,
        *,
        original_query: str,
        normalized_query: Optional[str] = None,
        query_terms: Optional[Sequence[str]] = None,
        query_phrases: Optional[Sequence[str]] = None,
        field_constraints: Optional[
            Mapping[str, Sequence[str]]
        ] = None,
        intent: Optional[str] = None,
        expansion_terms: Optional[
            Sequence[str]
        ] = None,
    ) -> QueryRelevanceProfile:

        normalized = (
            normalized_query
            if normalized_query
            else original_query.strip().lower()
        )

        terms = tuple(
            dict.fromkeys(
                query_terms
                if query_terms is not None
                else self._tokens(normalized)
            )
        )[
            :self.policy.max_query_terms
        ]

        phrases = tuple(
            dict.fromkeys(
                query_phrases
                if query_phrases is not None
                else self._phrases(normalized)
            )
        )[
            :self.policy.max_query_phrases
        ]

        constraints = {
            str(key): tuple(
                str(item).lower()
                for item in values
            )
            for key, values in (
                field_constraints or {}
            ).items()
        }

        expansions = tuple(
            dict.fromkeys(
                str(item).lower()
                for item in (
                    expansion_terms or ()
                )
            )
        )

        return QueryRelevanceProfile(
            original_query=original_query,
            normalized_query=normalized,
            terms=terms,
            phrases=phrases,
            field_constraints=constraints,
            intent=intent,
            expansion_terms=expansions,
            query_hash=_hash(
                (
                    original_query,
                    normalized,
                    terms,
                    phrases,
                    constraints,
                    intent,
                    expansions,
                )
            ),
        )

    # --------------------------------------------------------
    # Document profile
    # --------------------------------------------------------

    def _build_document_profile(
        self,
        candidate: Mapping[str, Any],
    ) -> DocumentRelevanceProfile:

        resource_id = str(
            candidate.get(
                "resource_id",
                candidate.get(
                    "document_id",
                    candidate.get(
                        "canonical_url",
                        "",
                    ),
                ),
            )
        )

        title = str(
            candidate.get(
                "title",
                candidate.get(
                    "document_title",
                    "",
                ),
            )
        )

        body = str(
            candidate.get(
                "body",
                candidate.get(
                    "content",
                    candidate.get(
                        "text",
                        "",
                    ),
                ),
            )
        )

        url = str(
            candidate.get(
                "canonical_url",
                candidate.get(
                    "url",
                    "",
                ),
            )
        )

        anchor = str(
            candidate.get(
                "anchor_text",
                "",
            )
        )

        phrases = tuple(
            str(item).lower()
            for item in candidate.get(
                "phrases",
                (),
            )
        )

        fields = {
            str(key): tuple(
                str(item).lower()
                for item in (
                    values
                    if isinstance(
                        values,
                        Sequence,
                    )
                    and not isinstance(
                        values,
                        str,
                    )
                    else (values,)
                )
            )
            for key, values in candidate.get(
                "fields",
                {},
            ).items()
        }

        return DocumentRelevanceProfile(
            resource_id=resource_id,

            title_terms=self._tokens(title),
            body_terms=self._tokens(body),
            url_terms=self._tokens(url),
            anchor_terms=self._tokens(anchor),

            fields=fields,

            phrases=phrases,

            document_length=len(
                self._tokens(body)
            ),

            document_hash=_hash(
                (
                    resource_id,
                    title,
                    body,
                    url,
                    anchor,
                    fields,
                    phrases,
                )
            ),
        )

    # --------------------------------------------------------
    # Signal helpers
    # --------------------------------------------------------

    @staticmethod
    def _strength(
        value: float,
    ) -> RelevanceSignalStrength:

        if value <= 0:
            return RelevanceSignalStrength.NONE

        if value >= 1.0:
            return RelevanceSignalStrength.EXACT

        if value >= 0.75:
            return RelevanceSignalStrength.STRONG

        if value >= 0.40:
            return RelevanceSignalStrength.MODERATE

        return RelevanceSignalStrength.WEAK

    def _signal(
        self,
        *,
        resource_id: str,
        family: RelevanceSignalFamily,
        value: float,
        confidence: float = 1.0,
        query_terms: Sequence[str] = (),
        matched_terms: Sequence[str] = (),
        source: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> RelevanceSignal:

        bounded_value = max(
            0.0,
            min(1.0, float(value)),
        )

        bounded_confidence = max(
            0.0,
            min(1.0, float(confidence)),
        )

        return RelevanceSignal(
            signal_id=_new_id(
                "relevance_signal"
            ),

            resource_id=resource_id,

            family=family,

            strength=self._strength(
                bounded_value
            ),

            value=bounded_value,

            confidence=bounded_confidence,

            query_terms=tuple(
                query_terms
            ),

            matched_terms=tuple(
                matched_terms
            ),

            source=source,

            metadata=dict(
                metadata or {}
            ),
        )

    # --------------------------------------------------------
    # Candidate extraction
    # --------------------------------------------------------

    def _extract_candidate_signals(
        self,
        query: QueryRelevanceProfile,
        document: DocumentRelevanceProfile,
    ) -> CandidateRelevanceProfile:

        query_terms = set(
            query.terms
        )

        title = set(
            document.title_terms
        )

        body = set(
            document.body_terms
        )

        url = set(
            document.url_terms
        )

        anchor = set(
            document.anchor_terms
        )

        matched_body = tuple(
            sorted(
                query_terms & body
            )
        )

        matched_title = tuple(
            sorted(
                query_terms & title
            )
        )

        matched_url = tuple(
            sorted(
                query_terms & url
            )
        )

        matched_anchor = tuple(
            sorted(
                query_terms & anchor
            )
        )

        signals: List[
            RelevanceSignal
        ] = []

        # Exact term evidence.
        exact_value = (
            1.0
            if matched_body
            else 0.0
        )

        if matched_body:

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.EXACT_TERM
                    ),
                    value=exact_value,
                    query_terms=query.terms,
                    matched_terms=matched_body,
                    source="document_body",
                )
            )

        # Term coverage.
        coverage = (
            len(matched_body)
            / len(query_terms)
            if query_terms
            else 0.0
        )

        if coverage > 0:

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.TERM_COVERAGE
                    ),
                    value=coverage,
                    query_terms=query.terms,
                    matched_terms=matched_body,
                    source="document_body",
                )
            )

        # Title match.
        if matched_title:

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.TITLE_MATCH
                    ),
                    value=min(
                        1.0,
                        len(matched_title)
                        / max(
                            1,
                            len(query_terms),
                        ),
                    ),
                    query_terms=query.terms,
                    matched_terms=matched_title,
                    source="document_title",
                )
            )

        # URL match.
        if matched_url:

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.URL_MATCH
                    ),
                    value=min(
                        1.0,
                        len(matched_url)
                        / max(
                            1,
                            len(query_terms),
                        ),
                    ),
                    query_terms=query.terms,
                    matched_terms=matched_url,
                    source="document_url",
                )
            )

        # Anchor match.
        if matched_anchor:

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.ANCHOR_MATCH
                    ),
                    value=min(
                        1.0,
                        len(matched_anchor)
                        / max(
                            1,
                            len(query_terms),
                        ),
                    ),
                    query_terms=query.terms,
                    matched_terms=matched_anchor,
                    source="anchor_text",
                )
            )

        # Content match.
        if matched_body:

            density = (
                len(matched_body)
                / max(
                    1,
                    document.document_length,
                )
            )

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.CONTENT_MATCH
                    ),
                    value=min(
                        1.0,
                        density * 100.0,
                    ),
                    query_terms=query.terms,
                    matched_terms=matched_body,
                    source="document_body",
                )
            )

        # Query coverage.
        all_document_terms = (
            title
            | body
            | url
            | anchor
        )

        coverage_all = (
            len(
                query_terms
                & all_document_terms
            )
            / len(query_terms)
            if query_terms
            else 0.0
        )

        if coverage_all > 0:

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.QUERY_COVERAGE
                    ),
                    value=coverage_all,
                    query_terms=query.terms,
                    matched_terms=tuple(
                        sorted(
                            query_terms
                            & all_document_terms
                        )
                    ),
                    source="document",
                )
            )

        # Phrase match.
        document_phrases = set(
            document.phrases
        )

        matched_phrases = tuple(
            sorted(
                set(query.phrases)
                & document_phrases
            )
        )

        if matched_phrases:

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.PHRASE_MATCH
                    ),
                    value=min(
                        1.0,
                        len(matched_phrases)
                        / max(
                            1,
                            len(query.phrases),
                        ),
                    ),
                    query_terms=query.terms,
                    matched_terms=matched_phrases,
                    source="document_phrases",
                )
            )

        # Field matches.
        field_matches: List[str] = []

        for field_name, values in (
            document.fields.items()
        ):

            value_set = set(values)

            if query_terms & value_set:
                field_matches.append(
                    field_name
                )

        if field_matches:

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.FIELD_MATCH
                    ),
                    value=min(
                        1.0,
                        len(field_matches)
                        / max(
                            1,
                            len(
                                document.fields
                            ),
                        ),
                    ),
                    query_terms=query.terms,
                    matched_terms=tuple(
                        sorted(field_matches)
                    ),
                    source="document_fields",
                )
            )

        # Intent compatibility.
        if query.intent:

            intent_value = 0.5

            if (
                query.intent
                in (
                    "find_website",
                    "find_page",
                    "find_organization",
                )
                and (
                    matched_title
                    or matched_url
                )
            ):
                intent_value = 1.0

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        RelevanceSignalFamily.INTENT_MATCH
                    ),
                    value=intent_value,
                    source="query_intent",
                    metadata={
                        "intent": query.intent,
                    },
                )
            )

        # Proximity placeholder derived only from explicit
        # adjacent query-term occurrence in the token stream.
        if len(query.terms) >= 2:

            body_sequence = (
                document.body_terms
            )

            proximity_hits = 0

            for index in range(
                len(body_sequence) - 1
            ):

                pair = {
                    body_sequence[index],
                    body_sequence[index + 1],
                }

                if pair.issubset(
                    query_terms
                ):
                    proximity_hits += 1

            proximity = min(
                1.0,
                proximity_hits
                / max(
                    1,
                    len(query.terms) - 1,
                ),
            )

            if proximity > 0:

                signals.append(
                    self._signal(
                        resource_id=document.resource_id,
                        family=(
                            RelevanceSignalFamily.PROXIMITY
                        ),
                        value=proximity,
                        query_terms=query.terms,
                        source="document_body",
                    )
                )

        signals = signals[
            :self.policy.max_signals_per_candidate
        ]

        weighted_score = 0.0

        weights = {
            RelevanceSignalFamily.EXACT_TERM:
                self.policy.exact_term_weight,

            RelevanceSignalFamily.TERM_COVERAGE:
                self.policy.term_coverage_weight,

            RelevanceSignalFamily.PHRASE_MATCH:
                self.policy.phrase_weight,

            RelevanceSignalFamily.FIELD_MATCH:
                self.policy.field_weight,

            RelevanceSignalFamily.TITLE_MATCH:
                self.policy.title_weight,

            RelevanceSignalFamily.URL_MATCH:
                self.policy.url_weight,

            RelevanceSignalFamily.ANCHOR_MATCH:
                self.policy.anchor_weight,

            RelevanceSignalFamily.CONTENT_MATCH:
                self.policy.content_weight,

            RelevanceSignalFamily.PROXIMITY:
                self.policy.proximity_weight,

            RelevanceSignalFamily.QUERY_COVERAGE:
                self.policy.query_coverage_weight,

            RelevanceSignalFamily.INTENT_MATCH:
                self.policy.intent_weight,

            RelevanceSignalFamily.SEMANTIC_CONTEXT:
                self.policy.semantic_context_weight,
        }

        for signal in signals:

            weighted_score += (
                signal.value
                * signal.confidence
                * weights.get(
                    signal.family,
                    1.0,
                )
            )

        return CandidateRelevanceProfile(
            resource_id=document.resource_id,
            signals=tuple(signals),
            aggregate_score=weighted_score,
        )

    # --------------------------------------------------------
    # Main extraction
    # --------------------------------------------------------

    def extract(
        self,
        *,
        ranking_id: str,
        retrieval_id: str,
        query_id: str,
        candidate_set_id: str,
        canonical_query_hash: str,
        retrieval_candidate_hash: str,
        original_query: str,
        normalized_query: Optional[str] = None,
        query_terms: Optional[Sequence[str]] = None,
        query_phrases: Optional[Sequence[str]] = None,
        field_constraints: Optional[
            Mapping[str, Sequence[str]]
        ] = None,
        intent: Optional[str] = None,
        expansion_terms: Optional[
            Sequence[str]
        ] = None,
        candidates: Sequence[Mapping[str, Any]] = (),
    ) -> Dict[str, Any]:

        extraction_id = _new_id(
            "relevance_extraction"
        )

        identity = RelevanceExtractionIdentity(
            extraction_id=extraction_id,
            ranking_id=ranking_id,
            retrieval_id=retrieval_id,
            query_id=query_id,
            candidate_set_id=candidate_set_id,
        )

        self._emit(
            extraction_id,
            RelevanceEventType.REQUEST_RECEIVED,
            "Relevance signal extraction request received",
        )

        if (
            self.policy.require_query
            and not original_query.strip()
        ):

            self._emit(
                extraction_id,
                RelevanceEventType.EXTRACTION_REJECTED,
                "Relevance extraction rejected empty query",
            )

            return {
                "extraction_id": extraction_id,
                "state": (
                    RelevanceExtractionState.REJECTED.value
                ),
                "identity": identity,
                "result": None,
                "lineage": None,
                "checkpoint": None,
            }

        query_profile = (
            self._build_query_profile(
                original_query=original_query,
                normalized_query=normalized_query,
                query_terms=query_terms,
                query_phrases=query_phrases,
                field_constraints=field_constraints,
                intent=intent,
                expansion_terms=expansion_terms,
            )
        )

        self._emit(
            extraction_id,
            RelevanceEventType.QUERY_SIGNALS_EXTRACTED,
            "Query relevance profile prepared",
            metadata={
                "term_count": len(
                    query_profile.terms
                ),
                "phrase_count": len(
                    query_profile.phrases
                ),
            },
        )

        limited_candidates = list(
            candidates[
                :self.policy.max_candidates_per_execution
            ]
        )

        partial = (
            len(candidates)
            > self.policy.max_candidates_per_execution
        )

        profiles: List[
            CandidateRelevanceProfile
        ] = []

        for raw_candidate in limited_candidates:

            try:

                document = (
                    self._build_document_profile(
                        raw_candidate
                    )
                )

            except Exception:
                continue

            if (
                self.policy.require_candidate_identity
                and not document.resource_id
            ):
                continue

            profile = (
                self._extract_candidate_signals(
                    query_profile,
                    document,
                )
            )

            profiles.append(profile)

        if partial:

            self._emit(
                extraction_id,
                RelevanceEventType.PARTIAL_INPUT_DETECTED,
                "Candidate input exceeded per-execution control budget",
                metadata={
                    "input_count": len(candidates),
                    "processed_count": len(
                        profiles
                    ),
                },
            )

        self._emit(
            extraction_id,
            RelevanceEventType.DOCUMENT_SIGNALS_EXTRACTED,
            "Document relevance signals extracted",
            metadata={
                "candidate_count": len(
                    profiles
                ),
            },
        )

        self._emit(
            extraction_id,
            RelevanceEventType.MATCH_SIGNALS_EXTRACTED,
            "Query-document match signals extracted",
        )

        self._emit(
            extraction_id,
            RelevanceEventType.SIGNALS_AGGREGATED,
            "Relevance signals aggregated",
        )

        extraction_hash = _hash(
            (
                query_profile.query_hash,
                tuple(
                    profile.signal_hash
                    for profile in profiles
                ),
            )
        )

        result = RelevanceExtractionResult(
            extraction_id=extraction_id,
            state=(
                RelevanceExtractionState.PARTIAL
                if partial
                else RelevanceExtractionState.COMPLETED
            ),
            query_profile=query_profile,
            candidate_profiles=tuple(profiles),
            candidate_count=len(profiles),
            partial=partial,
            extraction_hash=extraction_hash,
        )

        self.backend.persist_result(
            result
        )

        lineage = RelevanceLineage(
            extraction_id=extraction_id,
            ranking_id=ranking_id,
            retrieval_id=retrieval_id,
            query_id=query_id,
            canonical_query_hash=(
                canonical_query_hash
            ),
            retrieval_candidate_hash=(
                retrieval_candidate_hash
            ),
            extraction_input_hash=_hash(
                tuple(
                    profile.resource_id
                    for profile in profiles
                )
            ),
        )

        checkpoint = RelevanceExtractionCheckpoint(
            checkpoint_id=_new_id(
                "relevance_checkpoint"
            ),
            extraction_id=extraction_id,
            state=result.state,
            processed_candidates=len(
                profiles
            ),
            partial=partial,
            extraction_hash=extraction_hash,
        )

        self._checkpoints.append(
            checkpoint
        )

        self.backend.persist_checkpoint(
            checkpoint
        )

        self._emit(
            extraction_id,
            RelevanceEventType.CHECKPOINT_CREATED,
            "Relevance extraction checkpoint created",
            metadata={
                "checkpoint_id": (
                    checkpoint.checkpoint_id
                ),
            },
        )

        self._emit(
            extraction_id,
            RelevanceEventType.EXTRACTION_COMPLETED,
            "Relevance signal extraction completed",
            metadata={
                "candidate_count": (
                    len(profiles)
                ),
                "partial": partial,
            },
        )

        return {
            "extraction_id": extraction_id,
            "state": result.state.value,
            "identity": identity,
            "lineage": lineage,
            "result": result,
            "checkpoint": checkpoint,
        }

    # --------------------------------------------------------
    # Introspection
    # --------------------------------------------------------

    def events(
        self,
    ) -> Tuple[RelevanceExtractionEvent, ...]:

        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[
        RelevanceExtractionCheckpoint,
        ...
    ]:

        return tuple(self._checkpoints)

    # --------------------------------------------------------
    # Architecture declaration
    # --------------------------------------------------------

    @staticmethod
    def architecture() -> Dict[str, Any]:

        return {
            "name": (
                "OUR SEARCH Relevance Signal Extraction"
            ),

            "version": ARCHITECTURE_VERSION,

            "phase": 12,
            "stage": "12.2",

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "input": (
                "Phase 11 final retrieval candidate set"
            ),

            "purpose": [
                "extract explicit query-document relevance evidence",
                "preserve matched-term evidence",
                "preserve field evidence",
                "preserve phrase evidence",
                "preserve title evidence",
                "preserve URL evidence",
                "preserve anchor evidence",
                "preserve query coverage",
                "preserve proximity evidence",
                "produce reusable ranking signals",
            ],

            "signal_families": [
                family.value
                for family
                in RelevanceSignalFamily
            ],

            "properties": [
                "deterministic",
                "distributed",
                "partitionable",
                "provenance preserving",
                "lineage preserving",
                "checkpointable",
                "backend replaceable",
                "partial-input aware",
                "horizontally scalable",
            ],

            "global_limits": {
                "fixed_global_document_limit": False,
                "fixed_global_candidate_limit": False,
                "fixed_global_partition_limit": False,
                "fixed_global_worker_limit": False,
                "fixed_global_web_resource_limit": False,
            },

            "operational_limits_are": [
                "per-execution candidate controls",
                "per-candidate signal controls",
                "query expansion safety controls",
                "not global Web-scale limits",
            ],

            "does_not_determine": [
                "final ranking",
                "authority",
                "document quality",
                "spam classification",
                "freshness ranking",
                "trust ranking",
                "final result ordering",
            ],

            "next_stage": (
                "12.3 Document Quality & Authority Signals"
            ),

            "dependencies": [
                "Phase 11 final retrieval architecture",
                "Phase 12.1 global ranking architecture",
            ],

            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],

            "google_dependency": False,
        }


# ============================================================
# PUBLIC ALIASES
# ============================================================

RelevanceSignalExtraction = (
    RelevanceSignalExtractionArchitecture
)

GlobalRelevanceSignalExtraction = (
    RelevanceSignalExtractionArchitecture
)

Phase12_2RelevanceSignalExtraction = (
    RelevanceSignalExtractionArchitecture
)


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "RelevanceExtractionState",
    "RelevanceSignalFamily",
    "RelevanceSignalStrength",
    "RelevanceArtifactState",
    "RelevanceEventType",

    "RelevanceExtractionIdentity",
    "RelevanceLineage",

    "QueryRelevanceProfile",
    "DocumentRelevanceProfile",

    "RelevanceSignal",
    "CandidateRelevanceProfile",

    "RelevanceExtractionResult",
    "RelevanceExtractionCheckpoint",
    "RelevanceExtractionEvent",

    "RelevanceSignalExtractionBackend",
    "InMemoryRelevanceSignalMetadata",

    "RelevanceSignalExtractionPolicy",
    "RelevanceSignalExtractionArchitecture",

    "RelevanceSignalExtraction",
    "GlobalRelevanceSignalExtraction",
    "Phase12_2RelevanceSignalExtraction",
]
