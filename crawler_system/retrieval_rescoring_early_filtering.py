"""
OUR SEARCH — Phase 11.8
Retrieval Rescoring / Early Filtering

Architecture version:
    retrieval-rescoring-early-filtering.v1

Purpose:
    Perform lightweight retrieval-stage rescoring and early candidate
    filtering on the unified candidate set produced by Phase 11.7.

Scale target:
    Billions → trillions of publicly accessible Web resources.

This stage is NOT the final ranking system.

Pipeline:

    11.7 UNIFIED CANDIDATE SET
                ↓
        RETRIEVAL EVIDENCE ANALYSIS
                ↓
          MATCH SIGNAL EXTRACTION
                ↓
       TERM / PHRASE / FIELD SIGNALS
                ↓
       EARLY QUALITY / VALIDITY FILTERS
                ↓
          RETRIEVAL RESCORE
                ↓
          CANDIDATE PRUNING
                ↓
      SURVIVING RETRIEVAL CANDIDATES
                ↓
       11.9 FINAL RETRIEVAL ARCHITECTURE
                ↓
              PHASE 12

Important separation:

    Phase 11.8 determines whether a candidate is useful enough to
    continue through the retrieval pipeline.

    Phase 12 performs large-scale relevance ranking and final ordering.

11.8 does NOT:
    - perform final ranking
    - determine final result order
    - crawl the Web
    - mutate the index
    - generate embeddings
    - depend on Google Search/API/index/crawler/infrastructure

Architecture principles:

    - distributed
    - partition-aware
    - evidence-aware
    - field-aware
    - phrase-aware
    - quality-gate aware
    - deterministic
    - horizontally scalable
    - checkpointable
    - backend replaceable
    - no fixed global Web-size ceiling

There is intentionally no fixed global:
    - document count
    - candidate count
    - partition count
    - worker count
    - index size
    - Web-resource count

Operational limits are per-request execution controls, not global
capacity ceilings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


ARCHITECTURE_VERSION = "retrieval-rescoring-early-filtering.v1"
SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _hash(value: Any) -> str:
    return sha256(
        repr(value).encode("utf-8")
    ).hexdigest()


# ----------------------------------------------------------------------
# States
# ----------------------------------------------------------------------


class RescoringState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    SIGNAL_EXTRACTION = "signal_extraction"
    QUALITY_ANALYSIS = "quality_analysis"
    RESCORING = "rescoring"
    EARLY_FILTERING = "early_filtering"
    PRUNING = "pruning"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class CandidateDecisionState(str, Enum):
    ACCEPT = "accept"
    FILTER = "filter"
    DEFER = "defer"
    PARTIAL = "partial"


class FilterReason(str, Enum):
    NO_MATCH_EVIDENCE = "no_match_evidence"
    INVALID_IDENTITY = "invalid_identity"
    EMPTY_CANDIDATE = "empty_candidate"
    WEAK_MATCH = "weak_match"
    LOW_EVIDENCE = "low_evidence"
    QUALITY_GATE = "quality_gate"
    DUPLICATE_SIGNAL = "duplicate_signal"
    POLICY_FILTER = "policy_filter"
    RESOURCE_STATE = "resource_state"
    BUDGET_LIMIT = "budget_limit"


class SignalType(str, Enum):
    TERM_MATCH = "term_match"
    FIELD_MATCH = "field_match"
    PHRASE_MATCH = "phrase_match"
    CROSS_PARTITION_MATCH = "cross_partition_match"
    CROSS_REPLICA_MATCH = "cross_replica_match"
    EVIDENCE_DENSITY = "evidence_density"
    DOCUMENT_IDENTITY = "document_identity"
    CANONICAL_URL = "canonical_url"
    CONTENT_IDENTITY = "content_identity"


class SignalStrength(str, Enum):
    NONE = "none"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    EXACT = "exact"


class QualityGateState(str, Enum):
    NOT_CHECKED = "not_checked"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class RescoringEventType(str, Enum):
    RESCORING_RECEIVED = "rescoring_received"
    RESCORING_STARTED = "rescoring_started"
    SIGNALS_EXTRACTED = "signals_extracted"
    QUALITY_GATES_EVALUATED = "quality_gates_evaluated"
    CANDIDATES_RESCORED = "candidates_rescored"
    CANDIDATES_FILTERED = "candidates_filtered"
    CANDIDATES_PRUNED = "candidates_pruned"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    RESCORING_COMPLETED = "rescoring_completed"
    RESCORING_REJECTED = "rescoring_rejected"
    RESCORING_FAILED = "rescoring_failed"
    CHECKPOINT_CREATED = "checkpoint_created"


# ----------------------------------------------------------------------
# Identity
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RescoringIdentity:
    rescoring_id: str
    fusion_id: str
    retrieval_id: str
    query_id: str
    canonical_query_hash: str
    created_at: str
    architecture_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class CandidateIdentity:
    identity_type: str
    identity_value: str
    normalized_value: str
    identity_hash: str


# ----------------------------------------------------------------------
# Signals
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RetrievalSignal:
    signal_id: str
    candidate_identity: CandidateIdentity
    signal_type: SignalType
    strength: SignalStrength
    value: float
    source: str
    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


@dataclass
class CandidateSignalProfile:
    candidate_identity: CandidateIdentity
    signals: List[RetrievalSignal] = field(
        default_factory=list
    )

    term_matches: int = 0
    field_matches: int = 0
    phrase_matches: int = 0
    partition_count: int = 0
    replica_count: int = 0
    evidence_count: int = 0

    term_signal: float = 0.0
    field_signal: float = 0.0
    phrase_signal: float = 0.0
    partition_signal: float = 0.0
    replica_signal: float = 0.0
    evidence_density: float = 0.0


# ----------------------------------------------------------------------
# Quality
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class CandidateQualityAssessment:
    candidate_identity: CandidateIdentity
    identity_valid: bool
    has_match_evidence: bool
    quality_gate: QualityGateState
    filter_reason: Optional[FilterReason]
    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ----------------------------------------------------------------------
# Candidate input
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RetrievalCandidate:
    candidate_key: str
    identity: CandidateIdentity

    document_id: Optional[str]
    canonical_url: Optional[str]
    content_identity: Optional[str]

    partitions: Tuple[str, ...] = ()
    replicas: Tuple[str, ...] = ()
    source_reads: Tuple[str, ...] = ()

    matched_terms: Tuple[str, ...] = ()
    matched_fields: Tuple[str, ...] = ()
    matched_phrases: Tuple[str, ...] = ()

    evidence_count: int = 0
    duplicate_observation_count: int = 0

    retrieval_features: Mapping[str, Any] = field(
        default_factory=dict
    )


# ----------------------------------------------------------------------
# Scored candidate
# ----------------------------------------------------------------------


@dataclass
class RescoredCandidate:
    candidate_key: str
    identity: CandidateIdentity

    document_id: Optional[str]
    canonical_url: Optional[str]
    content_identity: Optional[str]

    raw_retrieval_score: float
    normalized_retrieval_score: float

    signals: CandidateSignalProfile
    quality: CandidateQualityAssessment

    decision: CandidateDecisionState
    filter_reason: Optional[FilterReason]

    partitions: Tuple[str, ...]
    replicas: Tuple[str, ...]
    source_reads: Tuple[str, ...]

    matched_terms: Tuple[str, ...]
    matched_fields: Tuple[str, ...]
    matched_phrases: Tuple[str, ...]

    metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ----------------------------------------------------------------------
# Budgets
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RetrievalRescoringBudget:
    max_input_candidates: int = 100000
    max_output_candidates: int = 100000

    max_signals_per_candidate: int = 128

    minimum_candidate_score: float = 0.01
    minimum_evidence_count: int = 1

    max_parallel_groups: int = 64


# ----------------------------------------------------------------------
# Decision
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RetrievalRescoringDecision:
    rescoring_id: str
    state: RescoringState

    input_candidates: int
    accepted_candidates: int
    filtered_candidates: int
    deferred_candidates: int
    output_candidates: int

    budget_applied: bool
    partial_input: bool

    decision_reason: str


# ----------------------------------------------------------------------
# Result
# ----------------------------------------------------------------------


@dataclass
class RetrievalRescoringResult:
    rescoring_id: str
    identity: RescoringIdentity
    state: RescoringState

    candidates: Dict[str, RescoredCandidate] = field(
        default_factory=dict
    )

    signal_profiles: Dict[str, CandidateSignalProfile] = field(
        default_factory=dict
    )

    quality_assessments: Dict[
        str,
        CandidateQualityAssessment,
    ] = field(default_factory=dict)

    decision: Optional[RetrievalRescoringDecision] = None

    input_candidates: int = 0
    filtered_candidates: int = 0
    accepted_candidates: int = 0

    partial_input: bool = False

    rescoring_hash: Optional[str] = None

    created_at: str = field(
        default_factory=_now
    )

    completed_at: Optional[str] = None


# ----------------------------------------------------------------------
# Checkpoint
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RetrievalRescoringCheckpoint:
    checkpoint_id: str
    rescoring_id: str
    state: RescoringState

    input_candidates: int
    output_candidates: int
    filtered_candidates: int

    created_at: str = field(
        default_factory=_now
    )


# ----------------------------------------------------------------------
# Events
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RetrievalRescoringEvent:
    event_id: str
    rescoring_id: str
    event_type: RescoringEventType
    state: RescoringState
    timestamp: str

    payload: Mapping[str, Any] = field(
        default_factory=dict
    )


# ----------------------------------------------------------------------
# Request
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RetrievalRescoringRequest:
    fusion_id: str
    retrieval_id: str
    query_id: str
    canonical_query_hash: str

    candidate_set: Sequence[Mapping[str, Any]]

    max_candidates: Optional[int] = None

    request_metadata: Mapping[str, Any] = field(
        default_factory=dict
    )


# ----------------------------------------------------------------------
# Backend
# ----------------------------------------------------------------------


class RetrievalRescoringBackend(Protocol):
    """
    Replaceable retrieval-stage backend.

    Production implementations can connect this interface to:

        - distributed candidate stores
        - document identity services
        - URL canonicalization systems
        - index metadata
        - quality metadata
        - retrieval evidence stores
        - partition metadata services

    The architecture does not depend on a specific database,
    filesystem, vendor, or distributed execution framework.
    """

    def normalize_url(
        self,
        url: str,
    ) -> str:
        ...

    def normalize_content_identity(
        self,
        value: str,
    ) -> str:
        ...

    def validate_document_identity(
        self,
        document_id: Optional[str],
    ) -> bool:
        ...


class InMemoryRetrievalRescoringBackend:
    """
    Reference backend.

    This backend performs deterministic normalization and identity
    validation only.
    """

    def normalize_url(
        self,
        url: str,
    ) -> str:
        value = str(url).strip().lower()

        if value.endswith("/"):
            value = value[:-1]

        return value

    def normalize_content_identity(
        self,
        value: str,
    ) -> str:
        return str(value).strip().lower()

    def validate_document_identity(
        self,
        document_id: Optional[str],
    ) -> bool:
        return bool(
            document_id
            and str(document_id).strip()
        )


# ----------------------------------------------------------------------
# Policy
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class RetrievalRescoringPolicy:
    budget: RetrievalRescoringBudget = field(
        default_factory=RetrievalRescoringBudget
    )

    term_weight: float = 1.0
    field_weight: float = 1.5
    phrase_weight: float = 2.0

    partition_weight: float = 0.25
    replica_weight: float = 0.10
    evidence_weight: float = 0.50

    cross_partition_bonus: float = 0.25
    cross_replica_bonus: float = 0.10

    require_match_evidence: bool = True
    require_valid_identity: bool = True

    allow_deferred_candidates: bool = True


# ----------------------------------------------------------------------
# Architecture
# ----------------------------------------------------------------------


class RetrievalRescoringEarlyFilteringArchitecture:
    """
    Phase 11.8.

    Performs retrieval-stage rescoring and early filtering.

    It is deliberately lighter than Phase 12 ranking.

    Responsibilities:

        1. Accept Phase 11.7 unified candidates.
        2. Extract retrieval evidence.
        3. Build deterministic retrieval signals.
        4. Evaluate early quality/validity gates.
        5. Calculate a retrieval-stage score.
        6. Filter clearly unusable candidates.
        7. Preserve surviving candidate provenance.
        8. Produce input for Phase 11.9.

    Not responsible for:

        - final ranking
        - final relevance ordering
        - authority ranking
        - PageRank-like global scoring
        - sophisticated quality ranking
        - personalization
        - final result composition
    """

    def __init__(
        self,
        backend: Optional[RetrievalRescoringBackend] = None,
        policy: Optional[RetrievalRescoringPolicy] = None,
    ) -> None:
        self.backend = (
            backend
            or InMemoryRetrievalRescoringBackend()
        )

        self.policy = (
            policy
            or RetrievalRescoringPolicy()
        )

        self._events: Dict[
            str,
            List[RetrievalRescoringEvent],
        ] = {}

        self._checkpoints: Dict[
            str,
            List[RetrievalRescoringCheckpoint],
        ] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def rescore(
        self,
        request: RetrievalRescoringRequest,
    ) -> RetrievalRescoringResult:
        identity = RescoringIdentity(
            rescoring_id=_new_id(
                "rescoring"
            ),
            fusion_id=request.fusion_id,
            retrieval_id=request.retrieval_id,
            query_id=request.query_id,
            canonical_query_hash=(
                request.canonical_query_hash
            ),
            created_at=_now(),
        )

        result = RetrievalRescoringResult(
            rescoring_id=identity.rescoring_id,
            identity=identity,
            state=RescoringState.RECEIVED,
        )

        self._emit(
            result,
            RescoringEventType.RESCORING_RECEIVED,
            {
                "fusion_id": request.fusion_id,
                "retrieval_id": request.retrieval_id,
                "query_id": request.query_id,
                "candidate_count": len(
                    request.candidate_set
                ),
            },
        )

        try:
            result.state = RescoringState.VALIDATING

            self._validate_request(
                request
            )

            result.state = (
                RescoringState.SIGNAL_EXTRACTION
            )

            candidates = self._materialize_candidates(
                request.candidate_set
            )

            if len(candidates) > (
                self.policy.budget.max_input_candidates
            ):
                candidates = candidates[
                    : self.policy.budget.max_input_candidates
                ]

                result.partial_input = True

            result.input_candidates = len(
                candidates
            )

            signal_profiles: Dict[
                str,
                CandidateSignalProfile,
            ] = {}

            for candidate in candidates:
                profile = (
                    self._extract_signals(
                        candidate
                    )
                )

                signal_profiles[
                    candidate.candidate_key
                ] = profile

            result.signal_profiles = (
                signal_profiles
            )

            self._emit(
                result,
                RescoringEventType.SIGNALS_EXTRACTED,
                {
                    "candidate_count": len(
                        candidates
                    ),
                    "signal_profiles": len(
                        signal_profiles
                    ),
                },
            )

            result.state = (
                RescoringState.QUALITY_ANALYSIS
            )

            assessments: Dict[
                str,
                CandidateQualityAssessment,
            ] = {}

            for candidate in candidates:
                assessment = (
                    self._evaluate_quality(
                        candidate,
                        signal_profiles[
                            candidate.candidate_key
                        ],
                    )
                )

                assessments[
                    candidate.candidate_key
                ] = assessment

            result.quality_assessments = (
                assessments
            )

            self._emit(
                result,
                RescoringEventType.QUALITY_GATES_EVALUATED,
                {
                    "assessment_count": len(
                        assessments
                    ),
                },
            )

            result.state = (
                RescoringState.RESCORING
            )

            rescored: Dict[
                str,
                RescoredCandidate,
            ] = {}

            for candidate in candidates:
                profile = signal_profiles[
                    candidate.candidate_key
                ]

                assessment = assessments[
                    candidate.candidate_key
                ]

                rescored_candidate = (
                    self._rescore_candidate(
                        candidate,
                        profile,
                        assessment,
                    )
                )

                rescored[
                    candidate.candidate_key
                ] = rescored_candidate

            result.candidates = rescored

            self._emit(
                result,
                RescoringEventType.CANDIDATES_RESCORED,
                {
                    "candidate_count": len(
                        rescored
                    ),
                },
            )

            result.state = (
                RescoringState.EARLY_FILTERING
            )

            self._apply_early_filtering(
                result
            )

            self._emit(
                result,
                RescoringEventType.CANDIDATES_FILTERED,
                {
                    "filtered_candidates": (
                        result.filtered_candidates
                    ),
                    "accepted_candidates": (
                        result.accepted_candidates
                    ),
                },
            )

            result.state = (
                RescoringState.PRUNING
            )

            self._prune(
                result,
                request.max_candidates,
            )

            self._emit(
                result,
                RescoringEventType.CANDIDATES_PRUNED,
                {
                    "output_candidates": len(
                        result.candidates
                    ),
                },
            )

            if result.partial_input:
                result.state = (
                    RescoringState.PARTIAL
                )

                self._emit(
                    result,
                    RescoringEventType.PARTIAL_INPUT_DETECTED,
                    {
                        "input_candidates": (
                            result.input_candidates
                        ),
                    },
                )
            else:
                result.state = (
                    RescoringState.COMPLETED
                )

            result.rescoring_hash = _hash(
                {
                    "identity": result.identity,
                    "candidate_ids": tuple(
                        sorted(
                            result.candidates.keys()
                        )
                    ),
                    "accepted": (
                        result.accepted_candidates
                    ),
                    "filtered": (
                        result.filtered_candidates
                    ),
                    "state": result.state.value,
                }
            )

            result.decision = (
                self._build_decision(
                    result
                )
            )

            result.completed_at = _now()

            self._checkpoint(
                result
            )

            self._emit(
                result,
                RescoringEventType.RESCORING_COMPLETED,
                {
                    "candidate_count": len(
                        result.candidates
                    ),
                    "rescoring_hash": (
                        result.rescoring_hash
                    ),
                },
            )

            return result

        except Exception as exc:
            result.state = (
                RescoringState.FAILED
            )

            self._emit(
                result,
                RescoringEventType.RESCORING_FAILED,
                {
                    "error": str(exc)
                },
            )

            self._checkpoint(
                result
            )

            raise

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_request(
        request: RetrievalRescoringRequest,
    ) -> None:
        if not request.fusion_id:
            raise ValueError(
                "fusion_id is required"
            )

        if not request.retrieval_id:
            raise ValueError(
                "retrieval_id is required"
            )

        if not request.query_id:
            raise ValueError(
                "query_id is required"
            )

        if not request.canonical_query_hash:
            raise ValueError(
                "canonical_query_hash is required"
            )

        if not isinstance(
            request.candidate_set,
            Sequence,
        ):
            raise TypeError(
                "candidate_set must be a sequence"
            )

    # ------------------------------------------------------------------
    # Candidate materialization
    # ------------------------------------------------------------------

    def _materialize_candidates(
        self,
        raw_candidates: Sequence[
            Mapping[str, Any]
        ],
    ) -> List[RetrievalCandidate]:
        candidates: List[
            RetrievalCandidate
        ] = []

        for index, raw in enumerate(
            raw_candidates
        ):
            candidate_key = str(
                raw.get(
                    "candidate_key",
                    raw.get(
                        "document_id",
                        f"candidate-{index}",
                    ),
                )
            )

            document_id = raw.get(
                "document_id"
            )

            canonical_url = raw.get(
                "canonical_url"
            )

            content_identity = raw.get(
                "content_identity"
            )

            if document_id:
                identity_type = (
                    "document_id"
                )
                identity_value = str(
                    document_id
                )
            elif canonical_url:
                identity_type = (
                    "canonical_url"
                )
                identity_value = str(
                    canonical_url
                )
            elif content_identity:
                identity_type = (
                    "content_identity"
                )
                identity_value = str(
                    content_identity
                )
            else:
                identity_type = (
                    "resource_id"
                )
                identity_value = candidate_key

            normalized_value = (
                identity_value.strip().lower()
            )

            if canonical_url:
                canonical_url = (
                    self.backend.normalize_url(
                        str(canonical_url)
                    )
                )

                normalized_value = (
                    canonical_url
                )

            if content_identity:
                content_identity = (
                    self.backend
                    .normalize_content_identity(
                        str(content_identity)
                    )
                )

            identity = CandidateIdentity(
                identity_type=identity_type,
                identity_value=identity_value,
                normalized_value=normalized_value,
                identity_hash=_hash(
                    normalized_value
                ),
            )

            candidates.append(
                RetrievalCandidate(
                    candidate_key=candidate_key,
                    identity=identity,
                    document_id=(
                        str(document_id)
                        if document_id
                        else None
                    ),
                    canonical_url=canonical_url,
                    content_identity=content_identity,
                    partitions=tuple(
                        str(value)
                        for value in raw.get(
                            "partitions",
                            (),
                        )
                    ),
                    replicas=tuple(
                        str(value)
                        for value in raw.get(
                            "replicas",
                            (),
                        )
                    ),
                    source_reads=tuple(
                        str(value)
                        for value in raw.get(
                            "source_reads",
                            (),
                        )
                    ),
                    matched_terms=tuple(
                        str(value)
                        for value in raw.get(
                            "matched_terms",
                            (),
                        )
                    ),
                    matched_fields=tuple(
                        str(value)
                        for value in raw.get(
                            "matched_fields",
                            (),
                        )
                    ),
                    matched_phrases=tuple(
                        str(value)
                        for value in raw.get(
                            "matched_phrases",
                            (),
                        )
                    ),
                    evidence_count=int(
                        raw.get(
                            "evidence_count",
                            0,
                        )
                    ),
                    duplicate_observation_count=int(
                        raw.get(
                            "duplicate_observation_count",
                            0,
                        )
                    ),
                    retrieval_features=dict(
                        raw.get(
                            "retrieval_features",
                            {},
                        )
                    ),
                )
            )

        return candidates

    # ------------------------------------------------------------------
    # Signal extraction
    # ------------------------------------------------------------------

    def _extract_signals(
        self,
        candidate: RetrievalCandidate,
    ) -> CandidateSignalProfile:
        profile = CandidateSignalProfile(
            candidate_identity=candidate.identity
        )

        profile.term_matches = len(
            candidate.matched_terms
        )

        profile.field_matches = len(
            candidate.matched_fields
        )

        profile.phrase_matches = len(
            candidate.matched_phrases
        )

        profile.partition_count = len(
            candidate.partitions
        )

        profile.replica_count = len(
            candidate.replicas
        )

        profile.evidence_count = (
            candidate.evidence_count
        )

        profile.term_signal = min(
            1.0,
            float(profile.term_matches)
            / max(
                1,
                profile.term_matches,
            ),
        )

        profile.field_signal = min(
            1.0,
            float(profile.field_matches)
            / max(
                1,
                profile.field_matches,
            ),
        )

        profile.phrase_signal = min(
            1.0,
            float(profile.phrase_matches)
            / max(
                1,
                profile.phrase_matches,
            ),
        )

        profile.partition_signal = min(
            1.0,
            float(profile.partition_count)
            / 4.0,
        )

        profile.replica_signal = min(
            1.0,
            float(profile.replica_count)
            / 3.0,
        )

        profile.evidence_density = min(
            1.0,
            float(profile.evidence_count)
            / max(
                1,
                profile.term_matches
                + profile.field_matches
                + profile.phrase_matches,
            ),
        )

        self._append_signal(
            profile,
            candidate,
            SignalType.TERM_MATCH,
            profile.term_signal,
            SignalStrength.STRONG
            if profile.term_matches
            else SignalStrength.NONE,
            "matched_terms",
        )

        self._append_signal(
            profile,
            candidate,
            SignalType.FIELD_MATCH,
            profile.field_signal,
            SignalStrength.STRONG
            if profile.field_matches
            else SignalStrength.NONE,
            "matched_fields",
        )

        self._append_signal(
            profile,
            candidate,
            SignalType.PHRASE_MATCH,
            profile.phrase_signal,
            SignalStrength.EXACT
            if profile.phrase_matches
            else SignalStrength.NONE,
            "matched_phrases",
        )

        self._append_signal(
            profile,
            candidate,
            SignalType.CROSS_PARTITION_MATCH,
            profile.partition_signal,
            SignalStrength.MODERATE
            if profile.partition_count > 1
            else SignalStrength.NONE,
            "partitions",
        )

        self._append_signal(
            profile,
            candidate,
            SignalType.CROSS_REPLICA_MATCH,
            profile.replica_signal,
            SignalStrength.WEAK
            if profile.replica_count > 1
            else SignalStrength.NONE,
            "replicas",
        )

        self._append_signal(
            profile,
            candidate,
            SignalType.EVIDENCE_DENSITY,
            profile.evidence_density,
            SignalStrength.STRONG
            if profile.evidence_density >= 0.75
            else (
                SignalStrength.MODERATE
                if profile.evidence_density >= 0.40
                else SignalStrength.WEAK
            ),
            "evidence_count",
        )

        return profile

    def _append_signal(
        self,
        profile: CandidateSignalProfile,
        candidate: RetrievalCandidate,
        signal_type: SignalType,
        value: float,
        strength: SignalStrength,
        source: str,
    ) -> None:
        if len(profile.signals) >= (
            self.policy.budget.max_signals_per_candidate
        ):
            return

        profile.signals.append(
            RetrievalSignal(
                signal_id=_new_id(
                    "retrieval-signal"
                ),
                candidate_identity=(
                    candidate.identity
                ),
                signal_type=signal_type,
                strength=strength,
                value=value,
                source=source,
            )
        )

    # ------------------------------------------------------------------
    # Quality gates
    # ------------------------------------------------------------------

    def _evaluate_quality(
        self,
        candidate: RetrievalCandidate,
        profile: CandidateSignalProfile,
    ) -> CandidateQualityAssessment:
        identity_valid = (
            self.backend.validate_document_identity(
                candidate.document_id
            )
            or bool(
                candidate.canonical_url
            )
            or bool(
                candidate.content_identity
            )
        )

        has_match_evidence = (
            profile.term_matches > 0
            or profile.field_matches > 0
            or profile.phrase_matches > 0
        )

        if (
            self.policy.require_valid_identity
            and not identity_valid
        ):
            return CandidateQualityAssessment(
                candidate_identity=candidate.identity,
                identity_valid=False,
                has_match_evidence=has_match_evidence,
                quality_gate=QualityGateState.FAILED,
                filter_reason=(
                    FilterReason.INVALID_IDENTITY
                ),
            )

        if (
            self.policy.require_match_evidence
            and not has_match_evidence
        ):
            return CandidateQualityAssessment(
                candidate_identity=candidate.identity,
                identity_valid=identity_valid,
                has_match_evidence=False,
                quality_gate=QualityGateState.FAILED,
                filter_reason=(
                    FilterReason.NO_MATCH_EVIDENCE
                ),
            )

        if (
            profile.evidence_count
            < self.policy.budget.minimum_evidence_count
        ):
            return CandidateQualityAssessment(
                candidate_identity=candidate.identity,
                identity_valid=identity_valid,
                has_match_evidence=has_match_evidence,
                quality_gate=QualityGateState.DEFERRED,
                filter_reason=(
                    FilterReason.LOW_EVIDENCE
                ),
            )

        return CandidateQualityAssessment(
            candidate_identity=candidate.identity,
            identity_valid=identity_valid,
            has_match_evidence=has_match_evidence,
            quality_gate=QualityGateState.PASSED,
            filter_reason=None,
        )

    # ------------------------------------------------------------------
    # Rescoring
    # ------------------------------------------------------------------

    def _rescore_candidate(
        self,
        candidate: RetrievalCandidate,
        profile: CandidateSignalProfile,
        quality: CandidateQualityAssessment,
    ) -> RescoredCandidate:
        raw_score = (
            self.policy.term_weight
            * profile.term_signal
            + self.policy.field_weight
            * profile.field_signal
            + self.policy.phrase_weight
            * profile.phrase_signal
            + self.policy.partition_weight
            * profile.partition_signal
            + self.policy.replica_weight
            * profile.replica_signal
            + self.policy.evidence_weight
            * profile.evidence_density
        )

        if profile.partition_count > 1:
            raw_score += (
                self.policy.cross_partition_bonus
            )

        if profile.replica_count > 1:
            raw_score += (
                self.policy.cross_replica_bonus
            )

        normalization_base = (
            self.policy.term_weight
            + self.policy.field_weight
            + self.policy.phrase_weight
            + self.policy.partition_weight
            + self.policy.replica_weight
            + self.policy.evidence_weight
            + self.policy.cross_partition_bonus
            + self.policy.cross_replica_bonus
        )

        normalized_score = (
            raw_score / normalization_base
            if normalization_base > 0
            else 0.0
        )

        if quality.quality_gate == (
            QualityGateState.FAILED
        ):
            decision = CandidateDecisionState.FILTER
            filter_reason = (
                quality.filter_reason
                or FilterReason.QUALITY_GATE
            )

        elif quality.quality_gate == (
            QualityGateState.DEFERRED
        ):
            if self.policy.allow_deferred_candidates:
                decision = (
                    CandidateDecisionState.DEFER
                )
                filter_reason = (
                    quality.filter_reason
                )
            else:
                decision = (
                    CandidateDecisionState.FILTER
                )
                filter_reason = (
                    quality.filter_reason
                )

        elif normalized_score < (
            self.policy.budget.minimum_candidate_score
        ):
            decision = CandidateDecisionState.FILTER
            filter_reason = (
                FilterReason.WEAK_MATCH
            )

        else:
            decision = CandidateDecisionState.ACCEPT
            filter_reason = None

        return RescoredCandidate(
            candidate_key=candidate.candidate_key,
            identity=candidate.identity,
            document_id=candidate.document_id,
            canonical_url=candidate.canonical_url,
            content_identity=candidate.content_identity,
            raw_retrieval_score=raw_score,
            normalized_retrieval_score=normalized_score,
            signals=profile,
            quality=quality,
            decision=decision,
            filter_reason=filter_reason,
            partitions=candidate.partitions,
            replicas=candidate.replicas,
            source_reads=candidate.source_reads,
            matched_terms=candidate.matched_terms,
            matched_fields=candidate.matched_fields,
            matched_phrases=candidate.matched_phrases,
            metadata={
                "architecture_stage": "11.8",
                "ranking_stage": "retrieval_only",
            },
        )

    # ------------------------------------------------------------------
    # Early filtering
    # ------------------------------------------------------------------

    def _apply_early_filtering(
        self,
        result: RetrievalRescoringResult,
    ) -> None:
        filtered = 0
        accepted = 0

        for key, candidate in list(
            result.candidates.items()
        ):
            if candidate.decision == (
                CandidateDecisionState.FILTER
            ):
                filtered += 1

                del result.candidates[key]

            elif candidate.decision in {
                CandidateDecisionState.ACCEPT,
                CandidateDecisionState.DEFER,
                CandidateDecisionState.PARTIAL,
            }:
                accepted += 1

        result.filtered_candidates = filtered
        result.accepted_candidates = accepted

    # ------------------------------------------------------------------
    # Candidate pruning
    # ------------------------------------------------------------------

    def _prune(
        self,
        result: RetrievalRescoringResult,
        requested_limit: Optional[int],
    ) -> None:
        limit = (
            requested_limit
            or self.policy.budget.max_output_candidates
        )

        if limit <= 0:
            result.candidates = {}
            return

        ordered = sorted(
            result.candidates.items(),
            key=lambda item: (
                item[1].normalized_retrieval_score,
                item[1].signals.evidence_density,
                item[1].signals.phrase_signal,
                item[1].signals.field_signal,
                item[1].signals.term_signal,
            ),
            reverse=True,
        )

        result.candidates = dict(
            ordered[:limit]
        )

    # ------------------------------------------------------------------
    # Decision
    # ------------------------------------------------------------------

    def _build_decision(
        self,
        result: RetrievalRescoringResult,
    ) -> RetrievalRescoringDecision:
        deferred = sum(
            1
            for candidate in result.candidates.values()
            if candidate.decision
            == CandidateDecisionState.DEFER
        )

        budget_applied = (
            result.input_candidates
            >= self.policy.budget.max_input_candidates
            or len(result.candidates)
            >= self.policy.budget.max_output_candidates
        )

        return RetrievalRescoringDecision(
            rescoring_id=result.rescoring_id,
            state=result.state,
            input_candidates=result.input_candidates,
            accepted_candidates=result.accepted_candidates,
            filtered_candidates=result.filtered_candidates,
            deferred_candidates=deferred,
            output_candidates=len(
                result.candidates
            ),
            budget_applied=budget_applied,
            partial_input=result.partial_input,
            decision_reason=(
                "retrieval rescoring completed"
                if result.state
                == RescoringState.COMPLETED
                else (
                    "retrieval rescoring completed "
                    "with partial input"
                )
            ),
        )

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------

    def _checkpoint(
        self,
        result: RetrievalRescoringResult,
    ) -> None:
        checkpoint = (
            RetrievalRescoringCheckpoint(
                checkpoint_id=_new_id(
                    "rescoring-checkpoint"
                ),
                rescoring_id=result.rescoring_id,
                state=result.state,
                input_candidates=(
                    result.input_candidates
                ),
                output_candidates=len(
                    result.candidates
                ),
                filtered_candidates=(
                    result.filtered_candidates
                ),
            )
        )

        self._checkpoints.setdefault(
            result.rescoring_id,
            [],
        ).append(
            checkpoint
        )

        self._emit(
            result,
            RescoringEventType.CHECKPOINT_CREATED,
            {
                "checkpoint_id": (
                    checkpoint.checkpoint_id
                )
            },
        )

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def _emit(
        self,
        result: RetrievalRescoringResult,
        event_type: RescoringEventType,
        payload: Mapping[str, Any],
    ) -> None:
        event = RetrievalRescoringEvent(
            event_id=_new_id(
                "rescoring-event"
            ),
            rescoring_id=result.rescoring_id,
            event_type=event_type,
            state=result.state,
            timestamp=_now(),
            payload=dict(payload),
        )

        self._events.setdefault(
            result.rescoring_id,
            [],
        ).append(event)

    def events(
        self,
        rescoring_id: Optional[str] = None,
    ) -> Tuple[
        RetrievalRescoringEvent,
        ...,
    ]:
        if rescoring_id is not None:
            return tuple(
                self._events.get(
                    rescoring_id,
                    (),
                )
            )

        values: List[
            RetrievalRescoringEvent
        ] = []

        for events in self._events.values():
            values.extend(events)

        return tuple(values)

    def checkpoints(
        self,
        rescoring_id: Optional[str] = None,
    ) -> Tuple[
        RetrievalRescoringCheckpoint,
        ...,
    ]:
        if rescoring_id is not None:
            return tuple(
                self._checkpoints.get(
                    rescoring_id,
                    (),
                )
            )

        values: List[
            RetrievalRescoringCheckpoint
        ] = []

        for checkpoints in (
            self._checkpoints.values()
        ):
            values.extend(checkpoints)

        return tuple(values)

    # ------------------------------------------------------------------
    # Architecture declaration
    # ------------------------------------------------------------------

    @staticmethod
    def architecture() -> Mapping[str, Any]:
        return {
            "architecture_version": (
                ARCHITECTURE_VERSION
            ),
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),
            "stage": "11.8",
            "name": (
                "Retrieval Rescoring / Early Filtering"
            ),
            "input": (
                "Phase 11.7 unified candidate set"
            ),
            "output": (
                "filtered and retrieval-rescored "
                "candidate set"
            ),
            "next_stage": (
                "11.9 Final Retrieval Architecture"
            ),
            "pipeline": [
                "11.7 unified candidate set",
                "retrieval evidence analysis",
                "match signal extraction",
                "term signals",
                "field signals",
                "phrase signals",
                "partition signals",
                "replica signals",
                "evidence density",
                "identity validation",
                "quality gates",
                "retrieval-stage rescoring",
                "early filtering",
                "candidate pruning",
                "11.9 final retrieval architecture",
            ],
            "signal_layers": [
                "term match",
                "field match",
                "phrase match",
                "cross-partition evidence",
                "cross-replica evidence",
                "evidence density",
                "document identity",
                "canonical URL",
                "content identity",
            ],
            "quality_gates": [
                "identity validity",
                "match evidence presence",
                "minimum evidence",
                "weak-match filtering",
            ],
            "distributed_properties": [
                "partition-aware",
                "replica-aware",
                "evidence-aware",
                "deterministic",
                "horizontally scalable",
                "checkpointable",
                "backend-replaceable",
                "partial-input aware",
            ],
            "no_fixed_global_limits": [
                "documents",
                "candidates",
                "partitions",
                "replicas",
                "workers",
                "index size",
                "public Web resources",
            ],
            "operational_budgets_are": [
                "per-request",
                "per-execution",
                "safety controls",
            ],
            "not_final_ranking": True,
            "phase_12_responsibility": [
                "large-scale relevance ranking",
                "ranking feature extraction",
                "authority and quality signals",
                "global ranking signals",
                "final result ordering",
            ],
            "phase_10_dependencies": [
                "distributed inverted-index segment fabric",
                "partition-aware index routing",
                "replication and durability",
                "index recovery/checkpointing",
                "storage capacity and tiering",
            ],
            "phase_11_dependencies": [
                "11.2 query normalization",
                "11.3 intent analysis",
                "11.4 term and field expansion",
                "11.5 distributed retrieval planning",
                "11.6 multi-partition candidate retrieval",
                "11.7 candidate fusion and deduplication",
            ],
            "protected_components": [
                "website_server.py",
                "search_service/server.py",
            ],
            "dependency_policy": {
                "google_search_api": False,
                "google_index": False,
                "google_crawler": False,
                "google_infrastructure": False,
                "google_search_technology": False,
            },
        }


# ----------------------------------------------------------------------
# Compatibility aliases
# ----------------------------------------------------------------------


RetrievalRescoringEarlyFiltering = (
    RetrievalRescoringEarlyFilteringArchitecture
)

GlobalRetrievalRescoring = (
    RetrievalRescoringEarlyFilteringArchitecture
)

Phase11_8RetrievalRescoringEarlyFiltering = (
    RetrievalRescoringEarlyFilteringArchitecture
)
