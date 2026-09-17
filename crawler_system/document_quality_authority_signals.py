from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ============================================================
# OUR SEARCH
# Phase 12.3 — Document Quality & Authority Signals
# ============================================================

ARCHITECTURE_VERSION = "document-quality-authority-signals.v1"
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
# ENUMS
# ============================================================

class QualityAuthorityState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    DOCUMENT_ANALYSIS = "document_analysis"
    QUALITY_ANALYSIS = "quality_analysis"
    AUTHORITY_ANALYSIS = "authority_analysis"
    TRUST_ANALYSIS = "trust_analysis"
    SIGNAL_AGGREGATION = "signal_aggregation"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class QualitySignalFamily(str, Enum):
    CONTENT_COMPLETENESS = "content_completeness"
    CONTENT_DEPTH = "content_depth"
    CONTENT_STRUCTURE = "content_structure"
    CONTENT_CLARITY = "content_clarity"
    TITLE_QUALITY = "title_quality"
    FIELD_COMPLETENESS = "field_completeness"
    SOURCE_REPUTATION = "source_reputation"
    DOMAIN_AUTHORITY = "domain_authority"
    LINK_AUTHORITY = "link_authority"
    CITATION_EVIDENCE = "citation_evidence"
    SOURCE_CONSISTENCY = "source_consistency"
    IDENTITY_STABILITY = "identity_stability"
    CANONICAL_CONSISTENCY = "canonical_consistency"
    DOCUMENT_STABILITY = "document_stability"
    TRUST_INDICATOR = "trust_indicator"


class QualitySignalStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class AuthorityEvidenceType(str, Enum):
    DOMAIN = "domain"
    LINKS = "links"
    REFERENCES = "references"
    CITATIONS = "citations"
    SOURCE_HISTORY = "source_history"
    IDENTITY = "identity"


class QualityEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    DOCUMENT_ANALYSIS_STARTED = "document_analysis_started"
    QUALITY_SIGNALS_EXTRACTED = "quality_signals_extracted"
    AUTHORITY_SIGNALS_EXTRACTED = "authority_signals_extracted"
    TRUST_SIGNALS_EXTRACTED = "trust_signals_extracted"
    SIGNALS_AGGREGATED = "signals_aggregated"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    EXTRACTION_COMPLETED = "extraction_completed"
    EXTRACTION_REJECTED = "extraction_rejected"
    EXTRACTION_FAILED = "extraction_failed"


# ============================================================
# IDENTITY
# ============================================================

@dataclass(frozen=True)
class QualityAuthorityIdentity:
    analysis_id: str
    ranking_id: str
    retrieval_id: str
    query_id: str
    candidate_set_id: str
    relevance_extraction_id: str
    architecture_version: str = ARCHITECTURE_VERSION
    created_at: datetime = field(default_factory=_now)


@dataclass(frozen=True)
class QualityAuthorityLineage:
    analysis_id: str
    ranking_id: str
    retrieval_id: str
    query_id: str
    relevance_extraction_id: str
    canonical_query_hash: str
    retrieval_candidate_hash: str
    relevance_signal_hash: str
    input_hash: str


# ============================================================
# DOCUMENT QUALITY INPUT
# ============================================================

@dataclass(frozen=True)
class DocumentQualityInput:
    resource_id: str

    title: str = ""
    content: str = ""
    canonical_url: str = ""

    content_length: int = 0

    fields: Mapping[str, Any] = field(
        default_factory=dict
    )

    headings: Tuple[str, ...] = ()

    links_in: int = 0
    links_out: int = 0

    citations: int = 0
    references: int = 0

    source_reputation: float = 0.0
    domain_authority: float = 0.0
    historical_stability: float = 0.0

    identity_stability: float = 0.0
    canonical_consistency: float = 0.0

    trust_indicators: Tuple[str, ...] = ()

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ============================================================
# SIGNAL
# ============================================================

@dataclass(frozen=True)
class QualityAuthoritySignal:
    signal_id: str

    resource_id: str

    family: QualitySignalFamily
    strength: QualitySignalStrength

    value: float
    confidence: float

    evidence_type: AuthorityEvidenceType

    source: Optional[str] = None

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ============================================================
# DOCUMENT PROFILE
# ============================================================

@dataclass
class DocumentQualityAuthorityProfile:

    resource_id: str

    quality_signals: Tuple[
        QualityAuthoritySignal,
        ...

    ]

    authority_signals: Tuple[
        QualityAuthoritySignal,
        ...
    ]

    trust_signals: Tuple[
        QualityAuthoritySignal,
        ...
    ]

    quality_score: float = 0.0
    authority_score: float = 0.0
    trust_score: float = 0.0

    aggregate_score: float = 0.0

    signal_hash: str = ""

    partial: bool = False

    def __post_init__(self) -> None:

        if self.quality_score == 0.0:
            self.quality_score = self._score(
                self.quality_signals
            )

        if self.authority_score == 0.0:
            self.authority_score = self._score(
                self.authority_signals
            )

        if self.trust_score == 0.0:
            self.trust_score = self._score(
                self.trust_signals
            )

        if self.aggregate_score == 0.0:
            self.aggregate_score = (
                self.quality_score
                + self.authority_score
                + self.trust_score
            ) / 3.0

        if not self.signal_hash:

            self.signal_hash = _hash(
                (
                    self.resource_id,
                    tuple(
                        (
                            s.family.value,
                            s.value,
                            s.confidence,
                        )
                        for s in (
                            self.quality_signals
                            + self.authority_signals
                            + self.trust_signals
                        )
                    ),
                )
            )

    @staticmethod
    def _score(
        signals: Sequence[
            QualityAuthoritySignal
        ],
    ) -> float:

        if not signals:
            return 0.0

        total = sum(
            signal.value
            * signal.confidence
            for signal in signals
        )

        return min(
            1.0,
            total / len(signals),
        )


# ============================================================
# RESULT
# ============================================================

@dataclass
class QualityAuthorityResult:

    analysis_id: str

    state: QualityAuthorityState

    profiles: Tuple[
        DocumentQualityAuthorityProfile,
        ...
    ]

    candidate_count: int

    partial: bool

    analysis_hash: str

    created_at: datetime = field(default_factory=_now)


# ============================================================
# CHECKPOINT
# ============================================================

@dataclass(frozen=True)
class QualityAuthorityCheckpoint:

    checkpoint_id: str

    analysis_id: str

    state: QualityAuthorityState

    processed_candidates: int

    partial: bool

    analysis_hash: str

    created_at: datetime = field(default_factory=_now)


# ============================================================
# EVENT
# ============================================================

@dataclass(frozen=True)
class QualityAuthorityEvent:

    event_id: str

    analysis_id: str

    event_type: QualityEventType

    message: str

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )

    created_at: datetime = field(default_factory=_now)


# ============================================================
# BACKEND
# ============================================================

class QualityAuthorityBackend(Protocol):

    def persist_event(
        self,
        event: QualityAuthorityEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: QualityAuthorityCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: QualityAuthorityResult,
    ) -> None:
        ...


class InMemoryQualityAuthorityMetadata:

    def __init__(self) -> None:

        self._events: List[
            QualityAuthorityEvent
        ] = []

        self._checkpoints: List[
            QualityAuthorityCheckpoint
        ] = []

        self._results: List[
            QualityAuthorityResult
        ] = []

    def persist_event(
        self,
        event: QualityAuthorityEvent,
    ) -> None:

        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: QualityAuthorityCheckpoint,
    ) -> None:

        self._checkpoints.append(checkpoint)

    def persist_result(
        self,
        result: QualityAuthorityResult,
    ) -> None:

        self._results.append(result)

    def events(
        self,
    ) -> Tuple[QualityAuthorityEvent, ...]:

        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[QualityAuthorityCheckpoint, ...]:

        return tuple(self._checkpoints)

    def results(
        self,
    ) -> Tuple[QualityAuthorityResult, ...]:

        return tuple(self._results)


# ============================================================
# POLICY
# ============================================================

@dataclass(frozen=True)
class QualityAuthorityPolicy:

    max_candidates_per_execution: int = 100000
    max_signals_per_candidate: int = 256

    allow_partial_execution: bool = True

    minimum_confidence: float = 0.10

    quality_content_weight: float = 1.0
    quality_depth_weight: float = 1.0
    quality_structure_weight: float = 1.0
    quality_clarity_weight: float = 1.0
    quality_title_weight: float = 1.0
    quality_field_weight: float = 1.0

    authority_source_weight: float = 1.0
    authority_domain_weight: float = 1.0
    authority_links_weight: float = 1.0
    authority_citation_weight: float = 1.0
    authority_consistency_weight: float = 1.0

    trust_identity_weight: float = 1.0
    trust_canonical_weight: float = 1.0
    trust_stability_weight: float = 1.0
    trust_indicator_weight: float = 1.0


# ============================================================
# ARCHITECTURE
# ============================================================

class DocumentQualityAuthoritySignalsArchitecture:

    """
    OUR SEARCH Phase 12.3.

    Produces document quality, authority, and trust evidence
    for downstream ranking.

    This stage does NOT perform final ranking.

    It does NOT:
        - reorder final search results
        - decide the final winner
        - crawl the Web
        - mutate the index
        - perform spam classification
        - perform freshness ranking
        - depend on Google infrastructure
    """

    def __init__(
        self,
        backend: Optional[
            QualityAuthorityBackend
        ] = None,
        policy: Optional[
            QualityAuthorityPolicy
        ] = None,
    ) -> None:

        self.backend = (
            backend
            if backend is not None
            else InMemoryQualityAuthorityMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else QualityAuthorityPolicy()
        )

        self._events: List[
            QualityAuthorityEvent
        ] = []

        self._checkpoints: List[
            QualityAuthorityCheckpoint
        ] = []

    # --------------------------------------------------------
    # Events
    # --------------------------------------------------------

    def _emit(
        self,
        analysis_id: str,
        event_type: QualityEventType,
        message: str,
        metadata: Optional[
            Mapping[str, Any]
        ] = None,
    ) -> None:

        event = QualityAuthorityEvent(
            event_id=_new_id(
                "quality_event"
            ),
            analysis_id=analysis_id,
            event_type=event_type,
            message=message,
            metadata=dict(
                metadata or {}
            ),
        )

        self._events.append(event)
        self.backend.persist_event(event)

    # --------------------------------------------------------
    # Signal
    # --------------------------------------------------------

    @staticmethod
    def _strength(
        value: float,
    ) -> QualitySignalStrength:

        if value <= 0:
            return QualitySignalStrength.NONE

        if value >= 0.90:
            return QualitySignalStrength.VERY_STRONG

        if value >= 0.70:
            return QualitySignalStrength.STRONG

        if value >= 0.40:
            return QualitySignalStrength.MODERATE

        return QualitySignalStrength.WEAK

    def _signal(
        self,
        *,
        resource_id: str,
        family: QualitySignalFamily,
        value: float,
        confidence: float = 1.0,
        evidence_type: AuthorityEvidenceType,
        source: Optional[str] = None,
        metadata: Optional[
            Mapping[str, Any]
        ] = None,
    ) -> QualityAuthoritySignal:

        value = max(
            0.0,
            min(1.0, float(value)),
        )

        confidence = max(
            0.0,
            min(1.0, float(confidence)),
        )

        return QualityAuthoritySignal(
            signal_id=_new_id(
                "quality_signal"
            ),
            resource_id=resource_id,
            family=family,
            strength=self._strength(value),
            value=value,
            confidence=confidence,
            evidence_type=evidence_type,
            source=source,
            metadata=dict(
                metadata or {}
            ),
        )

    # --------------------------------------------------------
    # Quality
    # --------------------------------------------------------

    def _quality_signals(
        self,
        document: DocumentQualityInput,
    ) -> Tuple[
        QualityAuthoritySignal,
        ...
    ]:

        signals: List[
            QualityAuthoritySignal
        ] = []

        content_length = (
            document.content_length
            if document.content_length > 0
            else len(document.content)
        )

        # Content completeness.
        completeness = min(
            1.0,
            content_length / 2000.0,
        )

        signals.append(
            self._signal(
                resource_id=document.resource_id,
                family=(
                    QualitySignalFamily
                    .CONTENT_COMPLETENESS
                ),
                value=completeness,
                evidence_type=(
                    AuthorityEvidenceType.IDENTITY
                ),
                source="content",
            )
        )

        # Content depth.
        depth = min(
            1.0,
            content_length / 5000.0,
        )

        signals.append(
            self._signal(
                resource_id=document.resource_id,
                family=(
                    QualitySignalFamily
                    .CONTENT_DEPTH
                ),
                value=depth,
                evidence_type=(
                    AuthorityEvidenceType.IDENTITY
                ),
                source="content",
            )
        )

        # Structure.
        structure = min(
            1.0,
            len(document.headings) / 8.0,
        )

        signals.append(
            self._signal(
                resource_id=document.resource_id,
                family=(
                    QualitySignalFamily
                    .CONTENT_STRUCTURE
                ),
                value=structure,
                evidence_type=(
                    AuthorityEvidenceType.IDENTITY
                ),
                source="headings",
            )
        )

        # Clarity proxy.
        word_count = max(
            1,
            len(
                document.content.split()
            ),
        )

        average_token_length = (
            sum(
                len(token)
                for token in
                document.content.split()
            )
            / word_count
        )

        clarity = (
            1.0
            if 2.5 <= average_token_length <= 9.0
            else 0.5
        )

        signals.append(
            self._signal(
                resource_id=document.resource_id,
                family=(
                    QualitySignalFamily
                    .CONTENT_CLARITY
                ),
                value=clarity,
                evidence_type=(
                    AuthorityEvidenceType.IDENTITY
                ),
                source="content",
            )
        )

        # Title quality.
        title_length = len(
            document.title.strip()
        )

        title_quality = (
            1.0
            if 10 <= title_length <= 120
            else (
                0.5
                if title_length > 0
                else 0.0
            )
        )

        signals.append(
            self._signal(
                resource_id=document.resource_id,
                family=(
                    QualitySignalFamily.TITLE_QUALITY
                ),
                value=title_quality,
                evidence_type=(
                    AuthorityEvidenceType.IDENTITY
                ),
                source="title",
            )
        )

        # Field completeness.
        field_count = len(
            document.fields
        )

        nonempty_fields = sum(
            1
            for value in document.fields.values()
            if value not in (
                None,
                "",
                (),
                [],
                {},
            )
        )

        field_quality = (
            nonempty_fields
            / field_count
            if field_count
            else 0.0
        )

        signals.append(
            self._signal(
                resource_id=document.resource_id,
                family=(
                    QualitySignalFamily
                    .FIELD_COMPLETENESS
                ),
                value=field_quality,
                evidence_type=(
                    AuthorityEvidenceType.IDENTITY
                ),
                source="metadata",
            )
        )

        return tuple(
            signals[
                :self.policy.max_signals_per_candidate
            ]
        )

    # --------------------------------------------------------
    # Authority
    # --------------------------------------------------------

    def _authority_signals(
        self,
        document: DocumentQualityInput,
    ) -> Tuple[
        QualityAuthoritySignal,
        ...
    ]:

        signals: List[
            QualityAuthoritySignal
        ] = []

        source_reputation = max(
            0.0,
            min(
                1.0,
                document.source_reputation,
            ),
        )

        domain_authority = max(
            0.0,
            min(
                1.0,
                document.domain_authority,
            ),
        )

        link_authority = min(
            1.0,
            (
                (
                    document.links_in
                    / 100.0
                )
                ** 0.5
            )
            if document.links_in > 0
            else 0.0,
        )

        citation_authority = min(
            1.0,
            (
                (
                    document.citations
                    + document.references
                )
                / 50.0
            ),
        )

        source_consistency = max(
            0.0,
            min(
                1.0,
                document.historical_stability,
            ),
        )

        signals.extend(
            [
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        QualitySignalFamily
                        .SOURCE_REPUTATION
                    ),
                    value=source_reputation,
                    evidence_type=(
                        AuthorityEvidenceType.SOURCE_HISTORY
                    ),
                    source="source_history",
                ),

                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        QualitySignalFamily
                        .DOMAIN_AUTHORITY
                    ),
                    value=domain_authority,
                    evidence_type=(
                        AuthorityEvidenceType.DOMAIN
                    ),
                    source="domain",
                ),

                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        QualitySignalFamily
                        .LINK_AUTHORITY
                    ),
                    value=link_authority,
                    evidence_type=(
                        AuthorityEvidenceType.LINKS
                    ),
                    source="incoming_links",
                ),

                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        QualitySignalFamily
                        .CITATION_EVIDENCE
                    ),
                    value=citation_authority,
                    evidence_type=(
                        AuthorityEvidenceType.CITATIONS
                    ),
                    source="citations",
                ),

                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        QualitySignalFamily
                        .SOURCE_CONSISTENCY
                    ),
                    value=source_consistency,
                    evidence_type=(
                        AuthorityEvidenceType.SOURCE_HISTORY
                    ),
                    source="source_history",
                ),
            ]
        )

        return tuple(
            signals[
                :self.policy.max_signals_per_candidate
            ]
        )

    # --------------------------------------------------------
    # Trust
    # --------------------------------------------------------

    def _trust_signals(
        self,
        document: DocumentQualityInput,
    ) -> Tuple[
        QualityAuthoritySignal,
        ...
    ]:

        signals: List[
            QualityAuthoritySignal
        ] = []

        identity = max(
            0.0,
            min(
                1.0,
                document.identity_stability,
            ),
        )

        canonical = max(
            0.0,
            min(
                1.0,
                document.canonical_consistency,
            ),
        )

        stability = max(
            0.0,
            min(
                1.0,
                document.historical_stability,
            ),
        )

        indicator_score = min(
            1.0,
            len(
                document.trust_indicators
            )
            / 5.0,
        )

        signals.extend(
            [
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        QualitySignalFamily
                        .IDENTITY_STABILITY
                    ),
                    value=identity,
                    evidence_type=(
                        AuthorityEvidenceType.IDENTITY
                    ),
                    source="identity",
                ),

                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        QualitySignalFamily
                        .CANONICAL_CONSISTENCY
                    ),
                    value=canonical,
                    evidence_type=(
                        AuthorityEvidenceType.IDENTITY
                    ),
                    source="canonical_identity",
                ),

                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        QualitySignalFamily
                        .DOCUMENT_STABILITY
                    ),
                    value=stability,
                    evidence_type=(
                        AuthorityEvidenceType.SOURCE_HISTORY
                    ),
                    source="history",
                ),

                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        QualitySignalFamily
                        .TRUST_INDICATOR
                    ),
                    value=indicator_score,
                    evidence_type=(
                        AuthorityEvidenceType.IDENTITY
                    ),
                    source="trust_indicators",
                ),
            ]
        )

        return tuple(
            signals[
                :self.policy.max_signals_per_candidate
            ]
        )

    # --------------------------------------------------------
    # Candidate analysis
    # --------------------------------------------------------

    def _analyze_document(
        self,
        document: DocumentQualityInput,
    ) -> DocumentQualityAuthorityProfile:

        quality = self._quality_signals(
            document
        )

        authority = self._authority_signals(
            document
        )

        trust = self._trust_signals(
            document
        )

        return DocumentQualityAuthorityProfile(
            resource_id=document.resource_id,
            quality_signals=quality,
            authority_signals=authority,
            trust_signals=trust,
        )

    # --------------------------------------------------------
    # Main API
    # --------------------------------------------------------

    def analyze(
        self,
        *,
        ranking_id: str,
        retrieval_id: str,
        query_id: str,
        candidate_set_id: str,
        relevance_extraction_id: str,
        canonical_query_hash: str,
        retrieval_candidate_hash: str,
        relevance_signal_hash: str,
        candidates: Sequence[
            Mapping[str, Any]
        ] = (),
    ) -> Dict[str, Any]:

        analysis_id = _new_id(
            "quality_authority"
        )

        identity = QualityAuthorityIdentity(
            analysis_id=analysis_id,
            ranking_id=ranking_id,
            retrieval_id=retrieval_id,
            query_id=query_id,
            candidate_set_id=candidate_set_id,
            relevance_extraction_id=(
                relevance_extraction_id
            ),
        )

        self._emit(
            analysis_id,
            QualityEventType.REQUEST_RECEIVED,
            "Document quality and authority analysis request received",
        )

        if not candidates:

            self._emit(
                analysis_id,
                QualityEventType.EXTRACTION_REJECTED,
                "No candidate documents supplied",
            )

            return {
                "analysis_id": analysis_id,
                "state": (
                    QualityAuthorityState.REJECTED.value
                ),
                "identity": identity,
                "result": None,
                "lineage": None,
                "checkpoint": None,
            }

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
            DocumentQualityAuthorityProfile
        ] = []

        self._emit(
            analysis_id,
            QualityEventType.DOCUMENT_ANALYSIS_STARTED,
            "Document quality and authority analysis started",
        )

        for candidate in limited_candidates:

            try:

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

                if not resource_id:
                    continue

                fields = candidate.get(
                    "fields",
                    {},
                )

                if not isinstance(
                    fields,
                    Mapping,
                ):
                    fields = {}

                document = DocumentQualityInput(
                    resource_id=resource_id,

                    title=str(
                        candidate.get(
                            "title",
                            candidate.get(
                                "document_title",
                                "",
                            ),
                        )
                    ),

                    content=str(
                        candidate.get(
                            "content",
                            candidate.get(
                                "body",
                                candidate.get(
                                    "text",
                                    "",
                                ),
                            ),
                        )
                    ),

                    canonical_url=str(
                        candidate.get(
                            "canonical_url",
                            candidate.get(
                                "url",
                                "",
                            ),
                        )
                    ),

                    content_length=int(
                        candidate.get(
                            "content_length",
                            0,
                        )
                        or 0
                    ),

                    fields=dict(fields),

                    headings=tuple(
                        str(item)
                        for item in candidate.get(
                            "headings",
                            (),
                        )
                    ),

                    links_in=int(
                        candidate.get(
                            "links_in",
                            0,
                        )
                        or 0
                    ),

                    links_out=int(
                        candidate.get(
                            "links_out",
                            0,
                        )
                        or 0
                    ),

                    citations=int(
                        candidate.get(
                            "citations",
                            0,
                        )
                        or 0
                    ),

                    references=int(
                        candidate.get(
                            "references",
                            0,
                        )
                        or 0
                    ),

                    source_reputation=float(
                        candidate.get(
                            "source_reputation",
                            0.0,
                        )
                        or 0.0
                    ),

                    domain_authority=float(
                        candidate.get(
                            "domain_authority",
                            0.0,
                        )
                        or 0.0
                    ),

                    historical_stability=float(
                        candidate.get(
                            "historical_stability",
                            0.0,
                        )
                        or 0.0
                    ),

                    identity_stability=float(
                        candidate.get(
                            "identity_stability",
                            0.0,
                        )
                        or 0.0
                    ),

                    canonical_consistency=float(
                        candidate.get(
                            "canonical_consistency",
                            0.0,
                        )
                        or 0.0
                    ),

                    trust_indicators=tuple(
                        str(item)
                        for item in candidate.get(
                            "trust_indicators",
                            (),
                        )
                    ),

                    metadata=dict(
                        candidate.get(
                            "metadata",
                            {},
                        )
                        if isinstance(
                            candidate.get(
                                "metadata",
                                {},
                            ),
                            Mapping,
                        )
                        else {}
                    ),
                )

                profiles.append(
                    self._analyze_document(
                        document
                    )
                )

            except Exception:
                continue

        if partial:

            self._emit(
                analysis_id,
                QualityEventType.PARTIAL_INPUT_DETECTED,
                "Candidate input exceeded execution budget",
                metadata={
                    "input_count": len(candidates),
                    "processed_count": len(
                        profiles
                    ),
                },
            )

        self._emit(
            analysis_id,
            QualityEventType.QUALITY_SIGNALS_EXTRACTED,
            "Document quality signals extracted",
            metadata={
                "candidate_count": len(
                    profiles
                ),
            },
        )

        self._emit(
            analysis_id,
            QualityEventType.AUTHORITY_SIGNALS_EXTRACTED,
            "Document authority signals extracted",
        )

        self._emit(
            analysis_id,
            QualityEventType.TRUST_SIGNALS_EXTRACTED,
            "Document trust signals extracted",
        )

        self._emit(
            analysis_id,
            QualityEventType.SIGNALS_AGGREGATED,
            "Quality, authority, and trust signals aggregated",
        )

        analysis_hash = _hash(
            (
                tuple(
                    profile.signal_hash
                    for profile in profiles
                ),
                canonical_query_hash,
                retrieval_candidate_hash,
                relevance_signal_hash,
            )
        )

        state = (
            QualityAuthorityState.PARTIAL
            if partial
            else QualityAuthorityState.COMPLETED
        )

        result = QualityAuthorityResult(
            analysis_id=analysis_id,
            state=state,
            profiles=tuple(profiles),
            candidate_count=len(profiles),
            partial=partial,
            analysis_hash=analysis_hash,
        )

        self.backend.persist_result(
            result
        )

        lineage = QualityAuthorityLineage(
            analysis_id=analysis_id,
            ranking_id=ranking_id,
            retrieval_id=retrieval_id,
            query_id=query_id,
            relevance_extraction_id=(
                relevance_extraction_id
            ),
            canonical_query_hash=(
                canonical_query_hash
            ),
            retrieval_candidate_hash=(
                retrieval_candidate_hash
            ),
            relevance_signal_hash=(
                relevance_signal_hash
            ),
            input_hash=_hash(
                tuple(
                    profile.resource_id
                    for profile in profiles
                )
            ),
        )

        checkpoint = QualityAuthorityCheckpoint(
            checkpoint_id=_new_id(
                "quality_checkpoint"
            ),
            analysis_id=analysis_id,
            state=state,
            processed_candidates=len(
                profiles
            ),
            partial=partial,
            analysis_hash=analysis_hash,
        )

        self._checkpoints.append(
            checkpoint
        )

        self.backend.persist_checkpoint(
            checkpoint
        )

        self._emit(
            analysis_id,
            QualityEventType.CHECKPOINT_CREATED,
            "Quality and authority checkpoint created",
            metadata={
                "checkpoint_id": (
                    checkpoint.checkpoint_id
                ),
            },
        )

        self._emit(
            analysis_id,
            QualityEventType.EXTRACTION_COMPLETED,
            "Document quality and authority analysis completed",
            metadata={
                "candidate_count": len(
                    profiles
                ),
                "partial": partial,
            },
        )

        return {
            "analysis_id": analysis_id,
            "state": state.value,
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
    ) -> Tuple[QualityAuthorityEvent, ...]:

        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[QualityAuthorityCheckpoint, ...]:

        return tuple(self._checkpoints)

    # --------------------------------------------------------
    # Architecture declaration
    # --------------------------------------------------------

    @staticmethod
    def architecture() -> Dict[str, Any]:

        return {
            "name": (
                "OUR SEARCH Document Quality and Authority Signals"
            ),

            "version": ARCHITECTURE_VERSION,

            "phase": 12,
            "stage": "12.3",

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "input": [
                "Phase 11 final retrieval candidates",
                "Phase 12.1 ranking architecture",
                "Phase 12.2 relevance signals",
            ],

            "outputs": [
                "document quality signals",
                "document authority signals",
                "source reputation signals",
                "domain authority signals",
                "link authority evidence",
                "citation evidence",
                "identity stability signals",
                "canonical consistency signals",
                "document stability signals",
                "trust indicators",
            ],

            "signal_families": [
                family.value
                for family in QualitySignalFamily
            ],

            "properties": [
                "distributed",
                "partitionable",
                "deterministic",
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
                "not global Web-scale limits",
            ],

            "does_not_determine": [
                "final ranking",
                "final result ordering",
                "spam classification",
                "freshness ranking",
                "crawl scheduling",
            ],

            "next_stage": (
                "12.4 Freshness and Temporal Relevance Signals"
            ),

            "google_dependency": False,
        }


# ============================================================
# PUBLIC ALIASES
# ============================================================

DocumentQualityAuthoritySignals = (
    DocumentQualityAuthoritySignalsArchitecture
)

GlobalDocumentQualityAuthoritySignals = (
    DocumentQualityAuthoritySignalsArchitecture
)

Phase12_3DocumentQualityAuthoritySignals = (
    DocumentQualityAuthoritySignalsArchitecture
)


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "QualityAuthorityState",
    "QualitySignalFamily",
    "QualitySignalStrength",
    "AuthorityEvidenceType",
    "QualityEventType",

    "QualityAuthorityIdentity",
    "QualityAuthorityLineage",

    "DocumentQualityInput",
    "QualityAuthoritySignal",
    "DocumentQualityAuthorityProfile",

    "QualityAuthorityResult",
    "QualityAuthorityCheckpoint",
    "QualityAuthorityEvent",

    "QualityAuthorityBackend",
    "InMemoryQualityAuthorityMetadata",

    "QualityAuthorityPolicy",
    "DocumentQualityAuthoritySignalsArchitecture",

    "DocumentQualityAuthoritySignals",
    "GlobalDocumentQualityAuthoritySignals",
    "Phase12_3DocumentQualityAuthoritySignals",
]
