from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from math import exp
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ============================================================
# OUR SEARCH
# Phase 12.4 — Freshness & Temporal Relevance Signals
# ============================================================

ARCHITECTURE_VERSION = "freshness-temporal-relevance-signals.v1"

SCALE_TARGET = (
    "billions_to_trillions_of_public_web_resources"
)

GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _hash(value: Any) -> str:
    return sha256(
        repr(value).encode(
            "utf-8",
            errors="replace",
        )
    ).hexdigest()


def _clamp(
    value: float,
    minimum: float = 0.0,
    maximum: float = 1.0,
) -> float:
    return max(
        minimum,
        min(maximum, float(value)),
    )


def _days_between(
    newer: datetime,
    older: datetime,
) -> float:
    seconds = (
        newer - older
    ).total_seconds()

    return max(
        0.0,
        seconds / 86400.0,
    )


# ============================================================
# ENUMS
# ============================================================

class FreshnessState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    TEMPORAL_ANALYSIS = "temporal_analysis"
    FRESHNESS_ANALYSIS = "freshness_analysis"
    CHANGE_ANALYSIS = "change_analysis"
    QUERY_TIME_ANALYSIS = "query_time_analysis"
    SIGNAL_AGGREGATION = "signal_aggregation"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class TemporalSignalFamily(str, Enum):
    DOCUMENT_AGE = "document_age"
    DOCUMENT_RECENCY = "document_recency"
    CONTENT_CHANGE_RATE = "content_change_rate"
    RECENT_CHANGE = "recent_change"
    UPDATE_REGULARITY = "update_regularity"
    SOURCE_FRESHNESS = "source_freshness"
    QUERY_TIME_ALIGNMENT = "query_time_alignment"
    EVENT_TIME_ALIGNMENT = "event_time_alignment"
    TEMPORAL_VALIDITY = "temporal_validity"
    LAST_CRAWL_RECENCY = "last_crawl_recency"


class TemporalEvidenceType(str, Enum):
    PUBLISHED_TIME = "published_time"
    MODIFIED_TIME = "modified_time"
    CRAWL_TIME = "crawl_time"
    CHANGE_HISTORY = "change_history"
    SOURCE_HISTORY = "source_history"
    QUERY_TIME = "query_time"
    EVENT_TIME = "event_time"


class TemporalSignalStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class TemporalEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    TEMPORAL_ANALYSIS_STARTED = "temporal_analysis_started"
    FRESHNESS_SIGNALS_EXTRACTED = "freshness_signals_extracted"
    CHANGE_SIGNALS_EXTRACTED = "change_signals_extracted"
    QUERY_TIME_SIGNALS_EXTRACTED = "query_time_signals_extracted"
    SIGNALS_AGGREGATED = "signals_aggregated"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    ANALYSIS_COMPLETED = "analysis_completed"
    ANALYSIS_REJECTED = "analysis_rejected"
    ANALYSIS_FAILED = "analysis_failed"


# ============================================================
# IDENTITIES
# ============================================================

@dataclass(frozen=True)
class FreshnessTemporalIdentity:
    analysis_id: str
    ranking_id: str
    retrieval_id: str
    query_id: str
    candidate_set_id: str
    quality_authority_analysis_id: str
    architecture_version: str = ARCHITECTURE_VERSION
    created_at: datetime = field(default_factory=_now)


@dataclass(frozen=True)
class FreshnessTemporalLineage:
    analysis_id: str
    ranking_id: str
    retrieval_id: str
    query_id: str
    quality_authority_analysis_id: str

    canonical_query_hash: str
    retrieval_candidate_hash: str
    quality_authority_hash: str

    input_hash: str
    temporal_signal_hash: str


# ============================================================
# DOCUMENT TEMPORAL INPUT
# ============================================================

@dataclass(frozen=True)
class DocumentTemporalInput:
    resource_id: str

    published_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    crawled_at: Optional[datetime] = None

    previous_modified_at: Optional[datetime] = None

    change_count: int = 0
    historical_observation_count: int = 0

    source_freshness: float = 0.0

    temporal_valid_from: Optional[datetime] = None
    temporal_valid_until: Optional[datetime] = None

    event_time: Optional[datetime] = None

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ============================================================
# QUERY TEMPORAL CONTEXT
# ============================================================

@dataclass(frozen=True)
class QueryTemporalContext:
    query_time: datetime

    is_time_sensitive: bool = False

    requested_start: Optional[datetime] = None
    requested_end: Optional[datetime] = None

    temporal_terms: Tuple[str, ...] = ()

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ============================================================
# SIGNAL
# ============================================================

@dataclass(frozen=True)
class FreshnessTemporalSignal:
    signal_id: str

    resource_id: str

    family: TemporalSignalFamily
    strength: TemporalSignalStrength

    value: float
    confidence: float

    evidence_type: TemporalEvidenceType

    source: Optional[str] = None

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ============================================================
# PROFILE
# ============================================================

@dataclass
class FreshnessTemporalProfile:

    resource_id: str

    freshness_signals: Tuple[
        FreshnessTemporalSignal,
        ...
    ]

    change_signals: Tuple[
        FreshnessTemporalSignal,
        ...
    ]

    query_temporal_signals: Tuple[
        FreshnessTemporalSignal,
        ...
    ]

    freshness_score: float = 0.0
    change_score: float = 0.0
    temporal_alignment_score: float = 0.0
    aggregate_score: float = 0.0

    signal_hash: str = ""

    partial: bool = False

    def __post_init__(self) -> None:

        self.freshness_score = self._score(
            self.freshness_signals
        )

        self.change_score = self._score(
            self.change_signals
        )

        self.temporal_alignment_score = self._score(
            self.query_temporal_signals
        )

        self.aggregate_score = (
            self.freshness_score
            + self.change_score
            + self.temporal_alignment_score
        ) / 3.0

        if not self.signal_hash:
            self.signal_hash = _hash(
                (
                    self.resource_id,
                    tuple(
                        (
                            signal.family.value,
                            signal.value,
                            signal.confidence,
                        )
                        for signal in (
                            self.freshness_signals
                            + self.change_signals
                            + self.query_temporal_signals
                        )
                    ),
                )
            )

    @staticmethod
    def _score(
        signals: Sequence[
            FreshnessTemporalSignal
        ],
    ) -> float:

        if not signals:
            return 0.0

        weighted = sum(
            signal.value
            * signal.confidence
            for signal in signals
        )

        confidence = sum(
            signal.confidence
            for signal in signals
        )

        if confidence <= 0:
            return 0.0

        return _clamp(
            weighted / confidence
        )


# ============================================================
# RESULT
# ============================================================

@dataclass
class FreshnessTemporalResult:

    analysis_id: str
    state: FreshnessState

    profiles: Tuple[
        FreshnessTemporalProfile,
        ...
    ]

    candidate_count: int
    partial: bool

    analysis_hash: str

    created_at: datetime = field(
        default_factory=_now
    )


# ============================================================
# CHECKPOINT
# ============================================================

@dataclass(frozen=True)
class FreshnessTemporalCheckpoint:

    checkpoint_id: str
    analysis_id: str

    state: FreshnessState

    processed_candidates: int

    partial: bool

    analysis_hash: str

    created_at: datetime = field(
        default_factory=_now
    )


# ============================================================
# EVENT
# ============================================================

@dataclass(frozen=True)
class FreshnessTemporalEvent:

    event_id: str
    analysis_id: str

    event_type: TemporalEventType
    message: str

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )

    created_at: datetime = field(
        default_factory=_now
    )


# ============================================================
# BACKEND
# ============================================================

class FreshnessTemporalBackend(Protocol):

    def persist_event(
        self,
        event: FreshnessTemporalEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: FreshnessTemporalCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: FreshnessTemporalResult,
    ) -> None:
        ...


class InMemoryFreshnessTemporalMetadata:

    def __init__(self) -> None:

        self._events: List[
            FreshnessTemporalEvent
        ] = []

        self._checkpoints: List[
            FreshnessTemporalCheckpoint
        ] = []

        self._results: List[
            FreshnessTemporalResult
        ] = []

    def persist_event(
        self,
        event: FreshnessTemporalEvent,
    ) -> None:

        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: FreshnessTemporalCheckpoint,
    ) -> None:

        self._checkpoints.append(checkpoint)

    def persist_result(
        self,
        result: FreshnessTemporalResult,
    ) -> None:

        self._results.append(result)

    def events(
        self,
    ) -> Tuple[FreshnessTemporalEvent, ...]:

        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[FreshnessTemporalCheckpoint, ...]:

        return tuple(self._checkpoints)

    def results(
        self,
    ) -> Tuple[FreshnessTemporalResult, ...]:

        return tuple(self._results)


# ============================================================
# POLICY
# ============================================================

@dataclass(frozen=True)
class FreshnessTemporalPolicy:

    max_candidates_per_execution: int = 100000
    max_signals_per_candidate: int = 256

    allow_partial_execution: bool = True

    minimum_confidence: float = 0.10

    # Recency half-lives.
    document_recency_half_life_days: float = 30.0
    recent_change_half_life_days: float = 14.0
    crawl_recency_half_life_days: float = 7.0

    # Change-rate normalization.
    change_rate_observation_window_days: float = 365.0

    # Signal weights.
    document_recency_weight: float = 1.0
    recent_change_weight: float = 1.0
    change_rate_weight: float = 1.0
    update_regularity_weight: float = 1.0
    source_freshness_weight: float = 1.0
    crawl_recency_weight: float = 0.5

    query_time_alignment_weight: float = 1.0
    event_time_alignment_weight: float = 1.0
    temporal_validity_weight: float = 1.0


# ============================================================
# ARCHITECTURE
# ============================================================

class FreshnessTemporalRelevanceSignalsArchitecture:

    """
    OUR SEARCH Phase 12.4.

    Produces temporal and freshness evidence for the
    downstream ranking system.

    This stage does NOT perform final ranking.

    It does NOT:
        - determine final result ordering
        - crawl the Web
        - schedule recrawls
        - mutate the index
        - classify spam
        - replace the freshness/recrawl infrastructure
        - depend on Google technology
    """

    def __init__(
        self,
        backend: Optional[
            FreshnessTemporalBackend
        ] = None,
        policy: Optional[
            FreshnessTemporalPolicy
        ] = None,
    ) -> None:

        self.backend = (
            backend
            if backend is not None
            else InMemoryFreshnessTemporalMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else FreshnessTemporalPolicy()
        )

        self._events: List[
            FreshnessTemporalEvent
        ] = []

        self._checkpoints: List[
            FreshnessTemporalCheckpoint
        ] = []

    # --------------------------------------------------------
    # Event emission
    # --------------------------------------------------------

    def _emit(
        self,
        analysis_id: str,
        event_type: TemporalEventType,
        message: str,
        metadata: Optional[
            Mapping[str, Any]
        ] = None,
    ) -> None:

        event = FreshnessTemporalEvent(
            event_id=_new_id(
                "temporal_event"
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
    # Strength
    # --------------------------------------------------------

    @staticmethod
    def _strength(
        value: float,
    ) -> TemporalSignalStrength:

        if value <= 0.0:
            return TemporalSignalStrength.NONE

        if value >= 0.90:
            return TemporalSignalStrength.VERY_STRONG

        if value >= 0.70:
            return TemporalSignalStrength.STRONG

        if value >= 0.40:
            return TemporalSignalStrength.MODERATE

        return TemporalSignalStrength.WEAK

    # --------------------------------------------------------
    # Signal
    # --------------------------------------------------------

    def _signal(
        self,
        *,
        resource_id: str,
        family: TemporalSignalFamily,
        value: float,
        confidence: float,
        evidence_type: TemporalEvidenceType,
        source: Optional[str] = None,
        metadata: Optional[
            Mapping[str, Any]
        ] = None,
    ) -> FreshnessTemporalSignal:

        value = _clamp(value)
        confidence = _clamp(confidence)

        return FreshnessTemporalSignal(
            signal_id=_new_id(
                "temporal_signal"
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
    # Exponential recency
    # --------------------------------------------------------

    @staticmethod
    def _recency(
        age_days: float,
        half_life_days: float,
    ) -> float:

        if half_life_days <= 0:
            return 0.0

        return _clamp(
            exp(
                -(
                    age_days
                    / half_life_days
                )
                * 0.6931471805599453
            )
        )

    # --------------------------------------------------------
    # Quality of timestamp
    # --------------------------------------------------------

    @staticmethod
    def _timestamp_confidence(
        timestamp: Optional[datetime],
    ) -> float:

        if timestamp is None:
            return 0.0

        return 1.0

    # --------------------------------------------------------
    # Freshness signals
    # --------------------------------------------------------

    def _freshness_signals(
        self,
        document: DocumentTemporalInput,
        now: datetime,
    ) -> Tuple[
        FreshnessTemporalSignal,
        ...
    ]:

        signals: List[
            FreshnessTemporalSignal
        ] = []

        reference_time = (
            document.modified_at
            or document.published_at
        )

        if reference_time is not None:

            age = _days_between(
                now,
                reference_time,
            )

            recency = self._recency(
                age,
                self.policy.document_recency_half_life_days,
            )

            family = (
                TemporalSignalFamily
                .DOCUMENT_RECENCY
            )

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=family,
                    value=recency,
                    confidence=1.0,
                    evidence_type=(
                        TemporalEvidenceType.MODIFIED_TIME
                        if document.modified_at
                        else TemporalEvidenceType.PUBLISHED_TIME
                    ),
                    source="document_timestamp",
                    metadata={
                        "age_days": age,
                    },
                )
            )

            age_signal = _clamp(
                age
                / (
                    self.policy
                    .document_recency_half_life_days
                    * 4.0
                )
            )

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        TemporalSignalFamily
                        .DOCUMENT_AGE
                    ),
                    value=1.0 - age_signal,
                    confidence=1.0,
                    evidence_type=(
                        TemporalEvidenceType.MODIFIED_TIME
                        if document.modified_at
                        else TemporalEvidenceType.PUBLISHED_TIME
                    ),
                    source="document_age",
                    metadata={
                        "age_days": age,
                    },
                )
            )

        if document.crawled_at is not None:

            crawl_age = _days_between(
                now,
                document.crawled_at,
            )

            crawl_recency = self._recency(
                crawl_age,
                self.policy.crawl_recency_half_life_days,
            )

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        TemporalSignalFamily
                        .LAST_CRAWL_RECENCY
                    ),
                    value=crawl_recency,
                    confidence=1.0,
                    evidence_type=(
                        TemporalEvidenceType.CRAWL_TIME
                    ),
                    source="crawl_timestamp",
                    metadata={
                        "crawl_age_days": crawl_age,
                    },
                )
            )

        signals.append(
            self._signal(
                resource_id=document.resource_id,
                family=(
                    TemporalSignalFamily
                    .SOURCE_FRESHNESS
                ),
                value=_clamp(
                    document.source_freshness
                ),
                confidence=(
                    1.0
                    if document.source_freshness > 0
                    else 0.0
                ),
                evidence_type=(
                    TemporalEvidenceType.SOURCE_HISTORY
                ),
                source="source_history",
            )
        )

        return tuple(
            signals[
                :self.policy.max_signals_per_candidate
            ]
        )

    # --------------------------------------------------------
    # Change signals
    # --------------------------------------------------------

    def _change_signals(
        self,
        document: DocumentTemporalInput,
        now: datetime,
    ) -> Tuple[
        FreshnessTemporalSignal,
        ...
    ]:

        signals: List[
            FreshnessTemporalSignal
        ] = []

        observation_days = max(
            1.0,
            self.policy
            .change_rate_observation_window_days,
        )

        change_rate = _clamp(
            (
                document.change_count
                / observation_days
            )
            * 30.0
        )

        signals.append(
            self._signal(
                resource_id=document.resource_id,
                family=(
                    TemporalSignalFamily
                    .CONTENT_CHANGE_RATE
                ),
                value=change_rate,
                confidence=(
                    1.0
                    if document.historical_observation_count > 0
                    else 0.0
                ),
                evidence_type=(
                    TemporalEvidenceType.CHANGE_HISTORY
                ),
                source="change_history",
                metadata={
                    "change_count": (
                        document.change_count
                    ),
                    "observation_count": (
                        document.historical_observation_count
                    ),
                },
            )
        )

        if (
            document.modified_at is not None
            and document.previous_modified_at
            is not None
        ):

            interval = _days_between(
                document.modified_at,
                document.previous_modified_at,
            )

            recent_change = self._recency(
                _days_between(
                    now,
                    document.modified_at,
                ),
                self.policy.recent_change_half_life_days,
            )

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        TemporalSignalFamily
                        .RECENT_CHANGE
                    ),
                    value=recent_change,
                    confidence=1.0,
                    evidence_type=(
                        TemporalEvidenceType.MODIFIED_TIME
                    ),
                    source="modification_history",
                    metadata={
                        "previous_interval_days":
                            interval,
                    },
                )
            )

            regularity = (
                1.0
                if interval > 0
                else 0.0
            )

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        TemporalSignalFamily
                        .UPDATE_REGULARITY
                    ),
                    value=regularity,
                    confidence=1.0,
                    evidence_type=(
                        TemporalEvidenceType.CHANGE_HISTORY
                    ),
                    source="modification_history",
                )
            )

        return tuple(
            signals[
                :self.policy.max_signals_per_candidate
            ]
        )

    # --------------------------------------------------------
    # Query temporal signals
    # --------------------------------------------------------

    def _query_temporal_signals(
        self,
        document: DocumentTemporalInput,
        query: QueryTemporalContext,
    ) -> Tuple[
        FreshnessTemporalSignal,
        ...
    ]:

        signals: List[
            FreshnessTemporalSignal
        ] = []

        query_time = query.query_time

        reference_time = (
            document.modified_at
            or document.published_at
            or document.crawled_at
        )

        if reference_time is not None:

            age = _days_between(
                query_time,
                reference_time,
            )

            alignment = self._recency(
                age,
                self.policy.document_recency_half_life_days,
            )

            if query.is_time_sensitive:

                signals.append(
                    self._signal(
                        resource_id=document.resource_id,
                        family=(
                            TemporalSignalFamily
                            .QUERY_TIME_ALIGNMENT
                        ),
                        value=alignment,
                        confidence=1.0,
                        evidence_type=(
                            TemporalEvidenceType.QUERY_TIME
                        ),
                        source="query_time",
                        metadata={
                            "age_days": age,
                        },
                    )
                )
            else:

                signals.append(
                    self._signal(
                        resource_id=document.resource_id,
                        family=(
                            TemporalSignalFamily
                            .QUERY_TIME_ALIGNMENT
                        ),
                        value=0.5,
                        confidence=0.25,
                        evidence_type=(
                            TemporalEvidenceType.QUERY_TIME
                        ),
                        source="query_time",
                        metadata={
                            "time_sensitive": False,
                        },
                    )
                )

        if document.event_time is not None:

            event_distance = abs(
                (
                    query_time
                    - document.event_time
                ).total_seconds()
            ) / 86400.0

            event_alignment = self._recency(
                event_distance,
                self.policy.document_recency_half_life_days,
            )

            signals.append(
                self._signal(
                    resource_id=document.resource_id,
                    family=(
                        TemporalSignalFamily
                        .EVENT_TIME_ALIGNMENT
                    ),
                    value=event_alignment,
                    confidence=1.0,
                    evidence_type=(
                        TemporalEvidenceType.EVENT_TIME
                    ),
                    source="event_time",
                    metadata={
                        "distance_days":
                            event_distance,
                    },
                )
            )

        validity = 1.0

        if (
            document.temporal_valid_from is not None
            and query_time
            < document.temporal_valid_from
        ):
            validity = 0.0

        if (
            document.temporal_valid_until is not None
            and query_time
            > document.temporal_valid_until
        ):
            validity = 0.0

        signals.append(
            self._signal(
                resource_id=document.resource_id,
                family=(
                    TemporalSignalFamily
                    .TEMPORAL_VALIDITY
                ),
                value=validity,
                confidence=1.0,
                evidence_type=(
                    TemporalEvidenceType.QUERY_TIME
                ),
                source="temporal_validity",
            )
        )

        return tuple(
            signals[
                :self.policy.max_signals_per_candidate
            ]
        )

    # --------------------------------------------------------
    # Document analysis
    # --------------------------------------------------------

    def _analyze_document(
        self,
        document: DocumentTemporalInput,
        query: QueryTemporalContext,
    ) -> FreshnessTemporalProfile:

        now = query.query_time

        freshness = self._freshness_signals(
            document,
            now,
        )

        changes = self._change_signals(
            document,
            now,
        )

        temporal = self._query_temporal_signals(
            document,
            query,
        )

        return FreshnessTemporalProfile(
            resource_id=document.resource_id,
            freshness_signals=freshness,
            change_signals=changes,
            query_temporal_signals=temporal,
        )

    # --------------------------------------------------------
    # Input conversion
    # --------------------------------------------------------

    @staticmethod
    def _parse_datetime(
        value: Any,
    ) -> Optional[datetime]:

        if isinstance(value, datetime):

            if value.tzinfo is None:
                return value.replace(
                    tzinfo=timezone.utc
                )

            return value.astimezone(
                timezone.utc
            )

        if not value:
            return None

        if isinstance(value, str):

            try:

                parsed = datetime.fromisoformat(
                    value.replace(
                        "Z",
                        "+00:00",
                    )
                )

                if parsed.tzinfo is None:
                    parsed = parsed.replace(
                        tzinfo=timezone.utc
                    )

                return parsed.astimezone(
                    timezone.utc
                )

            except ValueError:
                return None

        return None

    def _document_from_candidate(
        self,
        candidate: Mapping[str, Any],
    ) -> Optional[DocumentTemporalInput]:

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
            return None

        return DocumentTemporalInput(
            resource_id=resource_id,

            published_at=self._parse_datetime(
                candidate.get(
                    "published_at"
                )
            ),

            modified_at=self._parse_datetime(
                candidate.get(
                    "modified_at"
                )
            ),

            crawled_at=self._parse_datetime(
                candidate.get(
                    "crawled_at"
                )
            ),

            previous_modified_at=self._parse_datetime(
                candidate.get(
                    "previous_modified_at"
                )
            ),

            change_count=int(
                candidate.get(
                    "change_count",
                    0,
                )
                or 0
            ),

            historical_observation_count=int(
                candidate.get(
                    "historical_observation_count",
                    0,
                )
                or 0
            ),

            source_freshness=float(
                candidate.get(
                    "source_freshness",
                    0.0,
                )
                or 0.0
            ),

            temporal_valid_from=self._parse_datetime(
                candidate.get(
                    "temporal_valid_from"
                )
            ),

            temporal_valid_until=self._parse_datetime(
                candidate.get(
                    "temporal_valid_until"
                )
            ),

            event_time=self._parse_datetime(
                candidate.get(
                    "event_time"
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
        quality_authority_analysis_id: str,
        canonical_query_hash: str,
        retrieval_candidate_hash: str,
        quality_authority_hash: str,
        query_time: Optional[datetime] = None,
        is_time_sensitive: bool = False,
        temporal_terms: Sequence[str] = (),
        candidates: Sequence[
            Mapping[str, Any]
        ] = (),
    ) -> Dict[str, Any]:

        analysis_id = _new_id(
            "freshness_temporal"
        )

        identity = FreshnessTemporalIdentity(
            analysis_id=analysis_id,
            ranking_id=ranking_id,
            retrieval_id=retrieval_id,
            query_id=query_id,
            candidate_set_id=candidate_set_id,
            quality_authority_analysis_id=(
                quality_authority_analysis_id
            ),
        )

        self._emit(
            analysis_id,
            TemporalEventType.REQUEST_RECEIVED,
            "Freshness and temporal relevance analysis request received",
        )

        if not candidates:

            self._emit(
                analysis_id,
                TemporalEventType.ANALYSIS_REJECTED,
                "No candidate documents supplied",
            )

            return {
                "analysis_id": analysis_id,
                "state": FreshnessState.REJECTED.value,
                "identity": identity,
                "result": None,
                "lineage": None,
                "checkpoint": None,
            }

        if query_time is None:
            query_time = _now()

        query_time = self._parse_datetime(
            query_time
        ) or _now()

        query_context = QueryTemporalContext(
            query_time=query_time,
            is_time_sensitive=is_time_sensitive,
            temporal_terms=tuple(
                str(term)
                for term in temporal_terms
            ),
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

        self._emit(
            analysis_id,
            TemporalEventType.TEMPORAL_ANALYSIS_STARTED,
            "Temporal relevance analysis started",
            metadata={
                "candidate_count": len(
                    limited_candidates
                ),
                "time_sensitive": (
                    is_time_sensitive
                ),
            },
        )

        profiles: List[
            FreshnessTemporalProfile
        ] = []

        for candidate in limited_candidates:

            document = (
                self._document_from_candidate(
                    candidate
                )
            )

            if document is None:
                continue

            try:

                profiles.append(
                    self._analyze_document(
                        document,
                        query_context,
                    )
                )

            except Exception:
                continue

        if partial:

            self._emit(
                analysis_id,
                TemporalEventType.PARTIAL_INPUT_DETECTED,
                "Candidate input exceeded execution budget",
                metadata={
                    "input_count": len(
                        candidates
                    ),
                    "processed_count": len(
                        profiles
                    ),
                },
            )

        self._emit(
            analysis_id,
            TemporalEventType.FRESHNESS_SIGNALS_EXTRACTED,
            "Freshness signals extracted",
            metadata={
                "profile_count": len(
                    profiles
                ),
            },
        )

        self._emit(
            analysis_id,
            TemporalEventType.CHANGE_SIGNALS_EXTRACTED,
            "Content change signals extracted",
        )

        self._emit(
            analysis_id,
            TemporalEventType.QUERY_TIME_SIGNALS_EXTRACTED,
            "Query-time temporal signals extracted",
        )

        self._emit(
            analysis_id,
            TemporalEventType.SIGNALS_AGGREGATED,
            "Freshness and temporal signals aggregated",
        )

        analysis_hash = _hash(
            (
                tuple(
                    profile.signal_hash
                    for profile in profiles
                ),
                canonical_query_hash,
                retrieval_candidate_hash,
                quality_authority_hash,
                query_context.query_time.isoformat(),
                query_context.is_time_sensitive,
            )
        )

        state = (
            FreshnessState.PARTIAL
            if partial
            else FreshnessState.COMPLETED
        )

        result = FreshnessTemporalResult(
            analysis_id=analysis_id,
            state=state,
            profiles=tuple(profiles),
            candidate_count=len(
                profiles
            ),
            partial=partial,
            analysis_hash=analysis_hash,
        )

        self.backend.persist_result(
            result
        )

        lineage = FreshnessTemporalLineage(
            analysis_id=analysis_id,
            ranking_id=ranking_id,
            retrieval_id=retrieval_id,
            query_id=query_id,
            quality_authority_analysis_id=(
                quality_authority_analysis_id
            ),
            canonical_query_hash=(
                canonical_query_hash
            ),
            retrieval_candidate_hash=(
                retrieval_candidate_hash
            ),
            quality_authority_hash=(
                quality_authority_hash
            ),
            input_hash=_hash(
                tuple(
                    profile.resource_id
                    for profile in profiles
                )
            ),
            temporal_signal_hash=analysis_hash,
        )

        checkpoint = FreshnessTemporalCheckpoint(
            checkpoint_id=_new_id(
                "temporal_checkpoint"
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
            TemporalEventType.CHECKPOINT_CREATED,
            "Freshness temporal checkpoint created",
            metadata={
                "checkpoint_id":
                    checkpoint.checkpoint_id,
            },
        )

        self._emit(
            analysis_id,
            TemporalEventType.ANALYSIS_COMPLETED,
            "Freshness and temporal relevance analysis completed",
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
    ) -> Tuple[FreshnessTemporalEvent, ...]:

        return tuple(self._events)

    def checkpoints(
        self,
    ) -> Tuple[FreshnessTemporalCheckpoint, ...]:

        return tuple(self._checkpoints)

    # --------------------------------------------------------
    # Architecture declaration
    # --------------------------------------------------------

    @staticmethod
    def architecture() -> Dict[str, Any]:

        return {
            "name": (
                "OUR SEARCH Freshness and Temporal Relevance Signals"
            ),

            "version": ARCHITECTURE_VERSION,

            "phase": 12,
            "stage": "12.4",

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "input": [
                "Phase 11 final retrieval candidates",
                "Phase 12.1 ranking architecture",
                "Phase 12.2 relevance signals",
                "Phase 12.3 document quality and authority signals",
            ],

            "outputs": [
                "document recency",
                "document age",
                "recent change",
                "content change rate",
                "update regularity",
                "source freshness",
                "last crawl recency",
                "query-time alignment",
                "event-time alignment",
                "temporal validity",
            ],

            "properties": [
                "distributed",
                "partitionable",
                "deterministic",
                "time-aware",
                "query-aware",
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
                "recrawl scheduling",
                "crawl priority",
                "spam classification",
            ],

            "next_stage": (
                "12.5 Link and Graph Authority Signals"
            ),

            "google_dependency": False,
        }


# ============================================================
# PUBLIC ALIASES
# ============================================================

FreshnessTemporalRelevanceSignals = (
    FreshnessTemporalRelevanceSignalsArchitecture
)

GlobalFreshnessTemporalRelevanceSignals = (
    FreshnessTemporalRelevanceSignalsArchitecture
)

Phase12_4FreshnessTemporalRelevanceSignals = (
    FreshnessTemporalRelevanceSignalsArchitecture
)


__all__ = [
    "ARCHITECTURE_VERSION",
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",

    "FreshnessState",
    "TemporalSignalFamily",
    "TemporalEvidenceType",
    "TemporalSignalStrength",
    "TemporalEventType",

    "FreshnessTemporalIdentity",
    "FreshnessTemporalLineage",

    "DocumentTemporalInput",
    "QueryTemporalContext",
    "FreshnessTemporalSignal",
    "FreshnessTemporalProfile",

    "FreshnessTemporalResult",
    "FreshnessTemporalCheckpoint",
    "FreshnessTemporalEvent",

    "FreshnessTemporalBackend",
    "InMemoryFreshnessTemporalMetadata",

    "FreshnessTemporalPolicy",

    "FreshnessTemporalRelevanceSignalsArchitecture",

    "FreshnessTemporalRelevanceSignals",
    "GlobalFreshnessTemporalRelevanceSignals",
    "Phase12_4FreshnessTemporalRelevanceSignals",
]
