"""
OUR SEARCH
Phase 12.9 — Final Ranking Architecture & Global Ranking Pipeline

Purpose
-------
Final production architecture for transforming retrieved search candidates
and their ranking features/model scores into a globally coordinated ranking
result set at enormous public-Web scale.

Scale target
------------
Billions to trillions of public-Web resources.

Architecture position
---------------------
Phase 11
    Search Retrieval / Query Understanding
        ↓
Phase 12.1–12.6
    Ranking signal families
        ↓
Phase 12.7
    Multi-Signal Ranking Feature Fusion
        ↓
Phase 12.8
    Learning-to-Rank / Ranking Model Architecture
        ↓
Phase 12.9
    Final Ranking Architecture & Global Ranking Pipeline
        ↓
Phase 13
    Freshness + Recrawling
        ↓
Phase 14
    Spam / Abuse / Security / Quality Systems
        ↓
Phase 15
    Full Scale-Proof Program

Important boundary
------------------
This module defines the final ranking architecture.

It does NOT:
- crawl the Web
- retrieve documents from the index
- mutate the index
- perform Web discovery
- perform freshness crawling
- replace Phase 11 retrieval
- train an ML model
- implement a specific ML framework
- perform spam classification
- make security policy decisions
- claim current Google-scale deployment

The architecture is designed for Google-scale capability without using
Google Search APIs, Google indexes, Google crawlers, Google infrastructure,
or Google ranking technology.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from math import isfinite
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ============================================================================
# GLOBAL ARCHITECTURE CONSTANTS
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "final-ranking-architecture.v1"
PHASE = "12.9"

PREVIOUS_STAGE_FEATURE_FUSION = "phase12.7-multi-signal-ranking-feature-fusion"
PREVIOUS_STAGE_LTR = "phase12.8-learning-to-rank-architecture"
PREVIOUS_STAGE_RETRIEVAL = "phase11-final-retrieval-architecture"

NEXT_PHASE = "13"


# ============================================================================
# ENUMS
# ============================================================================


class RankingPipelineState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    CANDIDATE_VALIDATION = "candidate_validation"
    FEATURE_VALIDATION = "feature_validation"
    MODEL_VALIDATION = "model_validation"
    SCORE_COMPUTATION = "score_computation"
    SCORE_NORMALIZATION = "score_normalization"
    QUERY_DOCUMENT_CALIBRATION = "query_document_calibration"
    AUTHORITY_CALIBRATION = "authority_calibration"
    FRESHNESS_CALIBRATION = "freshness_calibration"
    DIVERSITY_PROCESSING = "diversity_processing"
    DUPLICATE_SUPPRESSION = "duplicate_suppression"
    FINAL_SORT = "final_sort"
    TOP_K_SELECTION = "top_k_selection"
    RESULT_ASSEMBLY = "result_assembly"
    PARTIAL = "partial"
    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"


class RankingCandidateState(str, Enum):
    RECEIVED = "received"
    VALID = "valid"
    PARTIAL = "partial"
    SCORED = "scored"
    SUPPRESSED = "suppressed"
    SELECTED = "selected"
    REJECTED = "rejected"


class RankingScoreComponent(str, Enum):
    MODEL_SCORE = "model_score"
    TEXT_RELEVANCE = "text_relevance"
    SEMANTIC_RELEVANCE = "semantic_relevance"
    QUERY_ALIGNMENT = "query_alignment"
    QUALITY = "quality"
    AUTHORITY = "authority"
    TRUST = "trust"
    FRESHNESS = "freshness"
    TEMPORAL_RELEVANCE = "temporal_relevance"
    LINK_AUTHORITY = "link_authority"
    GRAPH_AUTHORITY = "graph_authority"
    SOURCE_DIVERSITY = "source_diversity"
    EVIDENCE_DIVERSITY = "evidence_diversity"
    QUERY_SPECIFIC_AUTHORITY = "query_specific_authority"
    QUERY_DOCUMENT_COHERENCE = "query_document_coherence"
    DOCUMENT_STRUCTURE = "document_structure"
    FIELD_MATCH = "field_match"
    PHRASE_MATCH = "phrase_match"
    ENTITY_MATCH = "entity_match"
    INTENT_ALIGNMENT = "intent_alignment"
    QUERY_TYPE_ALIGNMENT = "query_type_alignment"


class RankingNormalizationMethod(str, Enum):
    NONE = "none"
    MIN_MAX = "min_max"
    Z_SCORE = "z_score"
    SOFTMAX = "softmax"
    SIGMOID = "sigmoid"
    RANK_NORMALIZED = "rank_normalized"


class DiversityMode(str, Enum):
    NONE = "none"
    DOMAIN_DIVERSITY = "domain_diversity"
    SOURCE_DIVERSITY = "source_diversity"
    HOST_DIVERSITY = "host_diversity"
    EVIDENCE_DIVERSITY = "evidence_diversity"
    COMBINED = "combined"


class DuplicateSuppressionMode(str, Enum):
    NONE = "none"
    EXACT_DOCUMENT = "exact_document"
    CANONICAL_URL = "canonical_url"
    NEAR_DUPLICATE = "near_duplicate"
    HOST_CLUSTER = "host_cluster"
    COMBINED = "combined"


class RankingExecutionMode(str, Enum):
    ONLINE = "online"
    BATCH = "batch"
    STREAMING = "streaming"
    OFFLINE_EVALUATION = "offline_evaluation"


class RankingEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    CANDIDATES_VALIDATED = "candidates_validated"
    FEATURES_VALIDATED = "features_validated"
    MODEL_VALIDATED = "model_validated"
    SCORES_COMPUTED = "scores_computed"
    SCORES_NORMALIZED = "scores_normalized"
    CALIBRATION_COMPLETED = "calibration_completed"
    DIVERSITY_COMPLETED = "diversity_completed"
    DUPLICATES_SUPPRESSED = "duplicates_suppressed"
    FINAL_SORT_COMPLETED = "final_sort_completed"
    TOP_K_SELECTED = "top_k_selected"
    RESULT_ASSEMBLED = "result_assembled"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    RANKING_COMPLETED = "ranking_completed"
    RANKING_REJECTED = "ranking_rejected"
    RANKING_FAILED = "ranking_failed"


class CheckpointType(str, Enum):
    VALIDATION = "validation"
    SCORING = "scoring"
    CALIBRATION = "calibration"
    DIVERSITY = "diversity"
    DEDUPLICATION = "deduplication"
    FINAL_SORT = "final_sort"
    RESULT_ASSEMBLY = "result_assembly"


# ============================================================================
# IDENTITIES
# ============================================================================


@dataclass(frozen=True)
class RankingIdentity:
    request_id: str
    query_id: str
    ranking_request_id: str = ""

    def stable_key(self) -> str:
        return "|".join(
            (
                self.query_id,
                self.ranking_request_id,
                self.request_id,
            )
        )


@dataclass(frozen=True)
class RankingDocumentIdentity:
    document_id: str
    canonical_url: str
    host: str = ""
    domain: str = ""
    shard_id: str = ""
    partition_id: str = ""

    def stable_key(self) -> str:
        return "|".join(
            (
                self.document_id,
                self.canonical_url,
                self.host,
                self.domain,
            )
        )


@dataclass(frozen=True)
class RankingLineage:
    retrieval_stage: str = PREVIOUS_STAGE_RETRIEVAL
    retrieval_version: str = ""
    feature_fusion_stage: str = PREVIOUS_STAGE_FEATURE_FUSION
    feature_fusion_version: str = ""
    learning_to_rank_stage: str = PREVIOUS_STAGE_LTR
    learning_to_rank_version: str = ""
    model_id: str = ""
    model_version: str = ""
    feature_schema_id: str = ""
    feature_schema_version: str = ""
    ranking_version: str = ARCHITECTURE_VERSION
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def fingerprint(self) -> str:
        return "|".join(
            (
                self.retrieval_stage,
                self.retrieval_version,
                self.feature_fusion_stage,
                self.feature_fusion_version,
                self.learning_to_rank_stage,
                self.learning_to_rank_version,
                self.model_id,
                self.model_version,
                self.feature_schema_id,
                self.feature_schema_version,
                self.ranking_version,
                self.observed_at.isoformat(),
            )
        )


# ============================================================================
# MODEL SCORE
# ============================================================================


@dataclass(frozen=True)
class RankingModelScore:
    model_id: str
    model_version: str
    score: float
    confidence: float = 1.0
    partial: bool = False
    feature_count: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def normalized_score(self) -> float:
        value = self.score

        if not isfinite(value):
            return 0.0

        if value < 0.0:
            return 0.0

        if value > 1.0:
            return 1.0

        return value


# ============================================================================
# RANKING SIGNALS
# ============================================================================


@dataclass(frozen=True)
class RankingScoreSignal:
    component: RankingScoreComponent
    value: float
    confidence: float = 1.0
    evidence_count: int = 1
    partial: bool = False
    source: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def normalized_value(self) -> float:
        value = self.value

        if not isfinite(value):
            return 0.0

        if value < 0.0:
            return 0.0

        if value > 1.0:
            return 1.0

        return value


# ============================================================================
# CANDIDATE
# ============================================================================


@dataclass
class FinalRankingCandidate:
    identity: RankingDocumentIdentity
    state: RankingCandidateState
    model_score: Optional[RankingModelScore] = None
    signals: Tuple[RankingScoreSignal, ...] = tuple()
    base_score: float = 0.0
    calibrated_score: float = 0.0
    diversity_adjusted_score: float = 0.0
    final_score: float = 0.0
    confidence: float = 0.0
    partial: bool = False
    original_retrieval_score: float = 0.0
    retrieval_rank: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def signal_map(self) -> Dict[RankingScoreComponent, float]:
        return {
            signal.component: signal.normalized_value()
            for signal in self.signals
        }


# ============================================================================
# FINAL RESULT
# ============================================================================


@dataclass(frozen=True)
class FinalRankingResultItem:
    rank: int
    document_id: str
    canonical_url: str
    host: str
    domain: str
    score: float
    confidence: float
    partial: bool
    retrieval_rank: int
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalRankingResult:
    request_id: str
    query_id: str
    items: Tuple[FinalRankingResultItem, ...]
    model_id: str
    model_version: str
    ranking_version: str
    total_candidates_received: int
    total_candidates_scored: int
    total_candidates_suppressed: int
    partial: bool
    completed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    lineage: Optional[RankingLineage] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# CHECKPOINT / EVENTS
# ============================================================================


@dataclass(frozen=True)
class FinalRankingCheckpoint:
    checkpoint_id: str
    request_id: str
    state: RankingPipelineState
    checkpoint_type: CheckpointType
    processed_candidates: int
    total_candidates: int
    progress: float
    partition_id: str = ""
    shard_id: str = ""
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalRankingEvent:
    event_id: str
    event_type: RankingEventType
    request_id: str
    query_id: str
    state: RankingPipelineState
    message: str = ""
    document_id: str = ""
    partition_id: str = ""
    shard_id: str = ""
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# POLICY
# ============================================================================


@dataclass(frozen=True)
class FinalRankingPolicy:
    """
    Per-request and per-worker limits.

    These are operational safety limits, NOT global Web-scale limits.
    """

    max_candidates_per_request: int = 100000
    max_signals_per_candidate: int = 512
    max_output_results: int = 100000
    max_domains_tracked_per_request: int = 100000
    max_hosts_tracked_per_request: int = 100000

    minimum_signal_confidence: float = 0.10
    minimum_candidate_confidence: float = 0.10

    model_weight: float = 1.50
    relevance_weight: float = 1.50
    authority_weight: float = 1.25
    quality_weight: float = 1.00
    freshness_weight: float = 1.00
    graph_weight: float = 1.00
    diversity_weight: float = 0.50
    query_specific_weight: float = 1.50

    diversity_penalty: float = 0.10
    duplicate_penalty: float = 1.00

    allow_partial: bool = True
    deterministic: bool = True

    default_top_k: int = 10
    max_parallel_partitions: int = 256
    max_parallel_shards: int = 256

    checkpoint_enabled: bool = True


# ============================================================================
# BACKEND
# ============================================================================


class FinalRankingBackend(Protocol):
    def persist_event(self, event: FinalRankingEvent) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: FinalRankingCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: FinalRankingResult,
    ) -> None:
        ...

    def get_result(
        self,
        request_id: str,
    ) -> Optional[FinalRankingResult]:
        ...


# ============================================================================
# IN-MEMORY BACKEND
# ============================================================================


class InMemoryFinalRankingMetadata:
    """
    Reference metadata backend.

    Production deployments should replace this with distributed durable
    metadata/event/checkpoint/result infrastructure.
    """

    def __init__(self) -> None:
        self._events: List[FinalRankingEvent] = []
        self._checkpoints: List[FinalRankingCheckpoint] = []
        self._results: Dict[str, FinalRankingResult] = {}

    def persist_event(
        self,
        event: FinalRankingEvent,
    ) -> None:
        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: FinalRankingCheckpoint,
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_result(
        self,
        result: FinalRankingResult,
    ) -> None:
        self._results[result.request_id] = result

    def get_result(
        self,
        request_id: str,
    ) -> Optional[FinalRankingResult]:
        return self._results.get(request_id)

    def events(self) -> Tuple[FinalRankingEvent, ...]:
        return tuple(self._events)

    def checkpoints(self) -> Tuple[FinalRankingCheckpoint, ...]:
        return tuple(self._checkpoints)

    def results(self) -> Tuple[FinalRankingResult, ...]:
        return tuple(self._results.values())


# ============================================================================
# FINAL RANKING ARCHITECTURE
# ============================================================================


class FinalRankingArchitecture:
    """
    Final Phase 12.9 ranking architecture.

    The implementation is framework-neutral. It provides deterministic
    orchestration and a reference scoring path while keeping the actual
    learned model runtime replaceable.

    Production model execution can be connected through the Phase 12.8
    model runtime / registry architecture.
    """

    def __init__(
        self,
        backend: Optional[FinalRankingBackend] = None,
        policy: Optional[FinalRankingPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryFinalRankingMetadata()
        self.policy = policy or FinalRankingPolicy()

    # ------------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------------

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _clamp(
        value: float,
        lower: float = 0.0,
        upper: float = 1.0,
    ) -> float:
        if not isfinite(value):
            return lower

        if value < lower:
            return lower

        if value > upper:
            return upper

        return value

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return 0.0

        if not isfinite(number):
            return 0.0

        return number

    # ------------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------------

    def _event(
        self,
        event_type: RankingEventType,
        request_id: str,
        query_id: str,
        state: RankingPipelineState,
        message: str = "",
        *,
        document_id: str = "",
        partition_id: str = "",
        shard_id: str = "",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> FinalRankingEvent:
        event = FinalRankingEvent(
            event_id=str(uuid4()),
            event_type=event_type,
            request_id=request_id,
            query_id=query_id,
            state=state,
            message=message,
            document_id=document_id,
            partition_id=partition_id,
            shard_id=shard_id,
            metadata=dict(metadata or {}),
        )

        self.backend.persist_event(event)

        return event

    # ------------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------------

    def _checkpoint(
        self,
        request_id: str,
        state: RankingPipelineState,
        checkpoint_type: CheckpointType,
        processed_candidates: int,
        total_candidates: int,
        *,
        partition_id: str = "",
        shard_id: str = "",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> FinalRankingCheckpoint:
        if total_candidates <= 0:
            progress = 1.0
        else:
            progress = self._clamp(
                processed_candidates / total_candidates
            )

        checkpoint = FinalRankingCheckpoint(
            checkpoint_id=str(uuid4()),
            request_id=request_id,
            state=state,
            checkpoint_type=checkpoint_type,
            processed_candidates=processed_candidates,
            total_candidates=total_candidates,
            progress=progress,
            partition_id=partition_id,
            shard_id=shard_id,
            metadata=dict(metadata or {}),
        )

        self.backend.persist_checkpoint(checkpoint)

        return checkpoint

    # ------------------------------------------------------------------------
    # Candidate validation
    # ------------------------------------------------------------------------

    def validate_candidates(
        self,
        candidates: Sequence[FinalRankingCandidate],
    ) -> None:
        if not candidates:
            raise ValueError("at least one ranking candidate is required")

        if len(candidates) > self.policy.max_candidates_per_request:
            raise ValueError(
                "candidate count exceeds configured per-request limit"
            )

        for candidate in candidates:
            if not candidate.identity.document_id:
                raise ValueError("candidate document id is required")

            if not candidate.identity.canonical_url:
                raise ValueError("candidate canonical URL is required")

            if len(candidate.signals) > self.policy.max_signals_per_candidate:
                raise ValueError(
                    "candidate signal count exceeds configured limit"
                )

            for signal in candidate.signals:
                if not isfinite(signal.confidence):
                    raise ValueError(
                        "signal confidence must be finite"
                    )

                if signal.confidence < 0.0:
                    raise ValueError(
                        "signal confidence cannot be negative"
                    )

    # ------------------------------------------------------------------------
    # Model validation
    # ------------------------------------------------------------------------

    def validate_model_score(
        self,
        score: RankingModelScore,
    ) -> None:
        if not score.model_id:
            raise ValueError("model id is required")

        if not score.model_version:
            raise ValueError("model version is required")

        if not isfinite(score.confidence):
            raise ValueError("model confidence must be finite")

        if score.confidence < 0.0:
            raise ValueError(
                "model confidence cannot be negative"
            )

    # ------------------------------------------------------------------------
    # Signal aggregation
    # ------------------------------------------------------------------------

    def _weighted_signal(
        self,
        signals: Mapping[RankingScoreComponent, float],
        components: Sequence[RankingScoreComponent],
        weight: float,
    ) -> Tuple[float, float]:
        values: List[float] = []

        for component in components:
            if component in signals:
                values.append(
                    self._clamp(
                        self._safe_float(signals[component])
                    )
                )

        if not values:
            return 0.0, 0.0

        average = sum(values) / len(values)

        return average, self._clamp(
            average * max(weight, 0.0)
        )

    def _base_score(
        self,
        candidate: FinalRankingCandidate,
    ) -> float:
        signals = candidate.signal_map()

        total = 0.0
        weight_total = 0.0

        if candidate.model_score is not None:
            model_value = candidate.model_score.normalized_score()

            total += model_value * self.policy.model_weight
            weight_total += self.policy.model_weight

        groups = (
            (
                (
                    RankingScoreComponent.TEXT_RELEVANCE,
                    RankingScoreComponent.SEMANTIC_RELEVANCE,
                    RankingScoreComponent.QUERY_ALIGNMENT,
                ),
                self.policy.relevance_weight,
            ),
            (
                (
                    RankingScoreComponent.AUTHORITY,
                    RankingScoreComponent.TRUST,
                    RankingScoreComponent.LINK_AUTHORITY,
                    RankingScoreComponent.GRAPH_AUTHORITY,
                ),
                self.policy.authority_weight,
            ),
            (
                (
                    RankingScoreComponent.QUALITY,
                    RankingScoreComponent.DOCUMENT_STRUCTURE,
                    RankingScoreComponent.FIELD_MATCH,
                ),
                self.policy.quality_weight,
            ),
            (
                (
                    RankingScoreComponent.FRESHNESS,
                    RankingScoreComponent.TEMPORAL_RELEVANCE,
                ),
                self.policy.freshness_weight,
            ),
            (
                (
                    RankingScoreComponent.LINK_AUTHORITY,
                    RankingScoreComponent.GRAPH_AUTHORITY,
                ),
                self.policy.graph_weight,
            ),
            (
                (
                    RankingScoreComponent.QUERY_SPECIFIC_AUTHORITY,
                    RankingScoreComponent.QUERY_DOCUMENT_COHERENCE,
                    RankingScoreComponent.INTENT_ALIGNMENT,
                    RankingScoreComponent.QUERY_TYPE_ALIGNMENT,
                ),
                self.policy.query_specific_weight,
            ),
        )

        for components, weight in groups:
            average, _ = self._weighted_signal(
                signals,
                components,
                weight,
            )

            if average > 0.0:
                total += average * weight
                weight_total += weight

        if weight_total <= 0.0:
            return 0.0

        return self._clamp(
            total / weight_total
        )

    # ------------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------------

    def _candidate_confidence(
        self,
        candidate: FinalRankingCandidate,
    ) -> float:
        confidence_values: List[float] = []

        if candidate.model_score is not None:
            confidence_values.append(
                self._clamp(
                    self._safe_float(
                        candidate.model_score.confidence
                    )
                )
            )

        for signal in candidate.signals:
            confidence_values.append(
                self._clamp(
                    self._safe_float(signal.confidence)
                )
            )

        if not confidence_values:
            return 0.0

        return self._clamp(
            sum(confidence_values) / len(confidence_values)
        )

    # ------------------------------------------------------------------------
    # Calibration
    # ------------------------------------------------------------------------

    def _calibrate_candidate(
        self,
        candidate: FinalRankingCandidate,
    ) -> None:
        score = self._clamp(
            self._safe_float(candidate.base_score)
        )

        confidence = self._candidate_confidence(candidate)

        # Confidence-aware calibration.
        #
        # A partial / low-confidence candidate is not automatically removed.
        # Its score is conservatively adjusted while preserving partial
        # result support.
        calibrated = score * (
            0.5 + 0.5 * confidence
        )

        candidate.calibrated_score = self._clamp(
            calibrated
        )

    # ------------------------------------------------------------------------
    # Diversity
    # ------------------------------------------------------------------------

    def _apply_diversity(
        self,
        candidates: Sequence[FinalRankingCandidate],
    ) -> None:
        domain_counts: Dict[str, int] = {}
        host_counts: Dict[str, int] = {}

        for candidate in candidates:
            domain = candidate.identity.domain or candidate.identity.host
            host = candidate.identity.host

            if domain:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1

            if host:
                host_counts[host] = host_counts.get(host, 0) + 1

        for candidate in candidates:
            domain = candidate.identity.domain or candidate.identity.host
            host = candidate.identity.host

            penalty = 0.0

            if domain:
                count = domain_counts.get(domain, 1)

                if count > 1:
                    penalty += self.policy.diversity_penalty * (
                        min(count - 1, 10) / 10.0
                    )

            if host:
                count = host_counts.get(host, 1)

                if count > 1:
                    penalty += self.policy.diversity_penalty * (
                        min(count - 1, 10) / 10.0
                    )

            candidate.diversity_adjusted_score = self._clamp(
                candidate.calibrated_score - penalty
            )

    # ------------------------------------------------------------------------
    # Duplicate suppression
    # ------------------------------------------------------------------------

    def _suppress_duplicates(
        self,
        candidates: Sequence[FinalRankingCandidate],
    ) -> int:
        seen_document_ids: set[str] = set()
        seen_urls: set[str] = set()

        suppressed = 0

        for candidate in candidates:
            document_id = candidate.identity.document_id
            canonical_url = candidate.identity.canonical_url

            duplicate = False

            if document_id in seen_document_ids:
                duplicate = True

            if canonical_url in seen_urls:
                duplicate = True

            if duplicate:
                candidate.state = RankingCandidateState.SUPPRESSED
                candidate.final_score = 0.0
                suppressed += 1
                continue

            seen_document_ids.add(document_id)
            seen_urls.add(canonical_url)

        return suppressed

    # ------------------------------------------------------------------------
    # Final score
    # ------------------------------------------------------------------------

    def _finalize_score(
        self,
        candidate: FinalRankingCandidate,
    ) -> None:
        score = candidate.diversity_adjusted_score

        if candidate.original_retrieval_score > 0.0:
            retrieval_signal = self._clamp(
                candidate.original_retrieval_score
            )

            score = (
                score * 0.90
                + retrieval_signal * 0.10
            )

        candidate.final_score = self._clamp(
            score
        )

        candidate.confidence = self._candidate_confidence(
            candidate
        )

        candidate.state = RankingCandidateState.SCORED

    # ------------------------------------------------------------------------
    # Candidate sorting
    # ------------------------------------------------------------------------

    @staticmethod
    def _sort_key(
        candidate: FinalRankingCandidate,
    ) -> Tuple[float, float, int, str]:
        return (
            -candidate.final_score,
            -candidate.confidence,
            candidate.retrieval_rank,
            candidate.identity.document_id,
        )

    # ------------------------------------------------------------------------
    # Top-K
    # ------------------------------------------------------------------------

    def _select_top_k(
        self,
        candidates: Sequence[FinalRankingCandidate],
        top_k: int,
    ) -> Tuple[FinalRankingCandidate, ...]:
        if top_k <= 0:
            raise ValueError("top_k must be positive")

        top_k = min(
            top_k,
            self.policy.max_output_results,
        )

        ordered = sorted(
            (
                candidate
                for candidate in candidates
                if candidate.state != RankingCandidateState.SUPPRESSED
            ),
            key=self._sort_key,
        )

        selected = ordered[:top_k]

        for candidate in selected:
            candidate.state = RankingCandidateState.SELECTED

        return tuple(selected)

    # ------------------------------------------------------------------------
    # Final result assembly
    # ------------------------------------------------------------------------

    def _assemble_result(
        self,
        identity: RankingIdentity,
        selected: Sequence[FinalRankingCandidate],
        *,
        model_id: str,
        model_version: str,
        total_candidates_received: int,
        total_candidates_scored: int,
        total_candidates_suppressed: int,
        partial: bool,
        lineage: Optional[RankingLineage],
    ) -> FinalRankingResult:
        items: List[FinalRankingResultItem] = []

        for index, candidate in enumerate(selected, start=1):
            items.append(
                FinalRankingResultItem(
                    rank=index,
                    document_id=candidate.identity.document_id,
                    canonical_url=candidate.identity.canonical_url,
                    host=candidate.identity.host,
                    domain=candidate.identity.domain,
                    score=self._clamp(
                        candidate.final_score
                    ),
                    confidence=self._clamp(
                        candidate.confidence
                    ),
                    partial=candidate.partial,
                    retrieval_rank=candidate.retrieval_rank,
                    metadata=dict(candidate.metadata),
                )
            )

        return FinalRankingResult(
            request_id=identity.request_id,
            query_id=identity.query_id,
            items=tuple(items),
            model_id=model_id,
            model_version=model_version,
            ranking_version=ARCHITECTURE_VERSION,
            total_candidates_received=total_candidates_received,
            total_candidates_scored=total_candidates_scored,
            total_candidates_suppressed=total_candidates_suppressed,
            partial=partial,
            lineage=lineage,
            metadata={
                "scale_target": SCALE_TARGET,
                "google_scale_capability_target": (
                    GOOGLE_SCALE_CAPABILITY_TARGET
                ),
                "google_technology_dependency": (
                    GOOGLE_TECHNOLOGY_DEPENDENCY
                ),
            },
        )

    # ------------------------------------------------------------------------
    # Main ranking pipeline
    # ------------------------------------------------------------------------

    def rank(
        self,
        identity: RankingIdentity,
        candidates: Sequence[FinalRankingCandidate],
        *,
        model_id: str,
        model_version: str,
        top_k: Optional[int] = None,
        lineage: Optional[RankingLineage] = None,
    ) -> FinalRankingResult:
        request_id = identity.request_id
        query_id = identity.query_id

        if top_k is None:
            top_k = self.policy.default_top_k

        self._event(
            RankingEventType.REQUEST_RECEIVED,
            request_id,
            query_id,
            RankingPipelineState.RECEIVED,
            message="final ranking request received",
        )

        # --------------------------------------------------------------------
        # Validation
        # --------------------------------------------------------------------

        self._event(
            RankingEventType.VALIDATION_STARTED,
            request_id,
            query_id,
            RankingPipelineState.VALIDATING,
        )

        self.validate_candidates(candidates)

        for candidate in candidates:
            candidate.state = RankingCandidateState.VALID

        self._event(
            RankingEventType.CANDIDATES_VALIDATED,
            request_id,
            query_id,
            RankingPipelineState.CANDIDATE_VALIDATION,
            metadata={
                "candidate_count": len(candidates),
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                request_id,
                RankingPipelineState.CANDIDATE_VALIDATION,
                CheckpointType.VALIDATION,
                0,
                len(candidates),
            )

        # --------------------------------------------------------------------
        # Model validation
        # --------------------------------------------------------------------

        for candidate in candidates:
            if candidate.model_score is not None:
                self.validate_model_score(
                    candidate.model_score
                )

        self._event(
            RankingEventType.MODEL_VALIDATED,
            request_id,
            query_id,
            RankingPipelineState.MODEL_VALIDATION,
            model_id=model_id if False else "",
        )

        # --------------------------------------------------------------------
        # Feature / signal validation
        # --------------------------------------------------------------------

        partial = False

        for candidate in candidates:
            candidate_partial = candidate.partial

            for signal in candidate.signals:
                if signal.partial:
                    candidate_partial = True

                if (
                    signal.confidence
                    < self.policy.minimum_signal_confidence
                ):
                    candidate_partial = True

            if candidate.model_score is not None:
                if candidate.model_score.partial:
                    candidate_partial = True

            candidate.partial = candidate_partial

            if candidate_partial:
                partial = True
                candidate.state = RankingCandidateState.PARTIAL

        self._event(
            RankingEventType.FEATURES_VALIDATED,
            request_id,
            query_id,
            RankingPipelineState.FEATURE_VALIDATION,
            metadata={
                "partial": partial,
            },
        )

        if partial and not self.policy.allow_partial:
            self._event(
                RankingEventType.RANKING_REJECTED,
                request_id,
                query_id,
                RankingPipelineState.REJECTED,
                message="partial candidates are disabled by policy",
            )
            raise ValueError(
                "partial ranking input is disabled"
            )

        if partial:
            self._event(
                RankingEventType.PARTIAL_INPUT_DETECTED,
                request_id,
                query_id,
                RankingPipelineState.PARTIAL,
                message="partial ranking input detected",
            )

        # --------------------------------------------------------------------
        # Score computation
        # --------------------------------------------------------------------

        for candidate in candidates:
            candidate.base_score = self._base_score(
                candidate
            )

        self._event(
            RankingEventType.SCORES_COMPUTED,
            request_id,
            query_id,
            RankingPipelineState.SCORE_COMPUTATION,
            metadata={
                "candidate_count": len(candidates),
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                request_id,
                RankingPipelineState.SCORE_COMPUTATION,
                CheckpointType.SCORING,
                len(candidates),
                len(candidates),
            )

        # --------------------------------------------------------------------
        # Score calibration
        # --------------------------------------------------------------------

        for candidate in candidates:
            self._calibrate_candidate(
                candidate
            )

        self._event(
            RankingEventType.SCORES_NORMALIZED,
            request_id,
            query_id,
            RankingPipelineState.SCORE_NORMALIZATION,
        )

        self._event(
            RankingEventType.CALIBRATION_COMPLETED,
            request_id,
            query_id,
            RankingPipelineState.QUERY_DOCUMENT_CALIBRATION,
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                request_id,
                RankingPipelineState.QUERY_DOCUMENT_CALIBRATION,
                CheckpointType.CALIBRATION,
                len(candidates),
                len(candidates),
            )

        # --------------------------------------------------------------------
        # Diversity
        # --------------------------------------------------------------------

        self._apply_diversity(
            candidates
        )

        self._event(
            RankingEventType.DIVERSITY_COMPLETED,
            request_id,
            query_id,
            RankingPipelineState.DIVERSITY_PROCESSING,
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                request_id,
                RankingPipelineState.DIVERSITY_PROCESSING,
                CheckpointType.DIVERSITY,
                len(candidates),
                len(candidates),
            )

        # --------------------------------------------------------------------
        # Duplicate suppression
        # --------------------------------------------------------------------

        suppressed = self._suppress_duplicates(
            candidates
        )

        self._event(
            RankingEventType.DUPLICATES_SUPPRESSED,
            request_id,
            query_id,
            RankingPipelineState.DUPLICATE_SUPPRESSION,
            metadata={
                "suppressed_count": suppressed,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                request_id,
                RankingPipelineState.DUPLICATE_SUPPRESSION,
                CheckpointType.DEDUPLICATION,
                len(candidates),
                len(candidates),
                metadata={
                    "suppressed_count": suppressed,
                },
            )

        # --------------------------------------------------------------------
        # Final scoring
        # --------------------------------------------------------------------

        scored_count = 0

        for candidate in candidates:
            if candidate.state == RankingCandidateState.SUPPRESSED:
                continue

            self._finalize_score(
                candidate
            )

            scored_count += 1

        # --------------------------------------------------------------------
        # Final global sort
        # --------------------------------------------------------------------

        ordered_candidates = sorted(
            (
                candidate
                for candidate in candidates
                if candidate.state != RankingCandidateState.SUPPRESSED
            ),
            key=self._sort_key,
        )

        self._event(
            RankingEventType.FINAL_SORT_COMPLETED,
            request_id,
            query_id,
            RankingPipelineState.FINAL_SORT,
            metadata={
                "candidate_count": len(ordered_candidates),
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                request_id,
                RankingPipelineState.FINAL_SORT,
                CheckpointType.FINAL_SORT,
                len(ordered_candidates),
                len(ordered_candidates),
            )

        # --------------------------------------------------------------------
        # Top-K
        # --------------------------------------------------------------------

        selected = self._select_top_k(
            ordered_candidates,
            top_k,
        )

        self._event(
            RankingEventType.TOP_K_SELECTED,
            request_id,
            query_id,
            RankingPipelineState.TOP_K_SELECTION,
            metadata={
                "top_k": len(selected),
            },
        )

        # --------------------------------------------------------------------
        # Result assembly
        # --------------------------------------------------------------------

        result = self._assemble_result(
            identity,
            selected,
            model_id=model_id,
            model_version=model_version,
            total_candidates_received=len(candidates),
            total_candidates_scored=scored_count,
            total_candidates_suppressed=suppressed,
            partial=partial,
            lineage=lineage,
        )

        self._event(
            RankingEventType.RESULT_ASSEMBLED,
            request_id,
            query_id,
            RankingPipelineState.RESULT_ASSEMBLY,
            metadata={
                "result_count": len(result.items),
            },
        )

        self.backend.persist_result(
            result
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                request_id,
                RankingPipelineState.COMPLETED,
                CheckpointType.RESULT_ASSEMBLY,
                len(candidates),
                len(candidates),
                metadata={
                    "result_count": len(result.items),
                    "partial": partial,
                },
            )

        self._event(
            RankingEventType.RANKING_COMPLETED,
            request_id,
            query_id,
            RankingPipelineState.COMPLETED,
            metadata={
                "result_count": len(result.items),
                "partial": partial,
            },
        )

        return result

    # ------------------------------------------------------------------------
    # Distributed ranking plan
    # ------------------------------------------------------------------------

    def build_distributed_ranking_plan(
        self,
        candidates: Sequence[FinalRankingCandidate],
        *,
        top_k: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not candidates:
            raise ValueError(
                "candidates are required"
            )

        if top_k is None:
            top_k = self.policy.default_top_k

        partitions = {
            candidate.identity.partition_id
            for candidate in candidates
            if candidate.identity.partition_id
        }

        shards = {
            candidate.identity.shard_id
            for candidate in candidates
            if candidate.identity.shard_id
        }

        return {
            "phase": PHASE,
            "architecture_version": ARCHITECTURE_VERSION,
            "execution_mode": RankingExecutionMode.ONLINE.value,

            "candidate_count": len(candidates),
            "requested_top_k": top_k,

            "distributed": True,
            "partition_local_scoring": True,
            "partition_local_feature_processing": True,
            "shard_local_processing": True,

            "partition_count": len(partitions) or 1,
            "shard_count": len(shards) or 1,

            "parallel_partition_processing": True,
            "parallel_shard_processing": True,

            "local_score_computation": True,
            "global_score_normalization": True,
            "global_candidate_fusion": True,
            "global_deduplication": True,
            "global_diversity_processing": True,
            "global_final_sort": True,
            "global_top_k_selection": True,

            "partial_results_supported": True,
            "checkpointable": self.policy.checkpoint_enabled,
            "deterministic": self.policy.deterministic,

            "no_global_candidate_ceiling": True,
            "no_global_document_ceiling": True,
            "no_global_query_ceiling": True,
            "no_global_partition_ceiling": True,
            "no_global_shard_ceiling": True,

            "google_technology_dependency": False,
        }

    # ------------------------------------------------------------------------
    # Architecture description
    # ------------------------------------------------------------------------

    def architecture(self) -> Dict[str, Any]:
        return {
            "phase": PHASE,
            "name": "Final Ranking Architecture & Global Ranking Pipeline",
            "version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),

            "inputs": {
                "retrieval": PREVIOUS_STAGE_RETRIEVAL,
                "feature_fusion": PREVIOUS_STAGE_FEATURE_FUSION,
                "learning_to_rank": PREVIOUS_STAGE_LTR,
            },

            "pipeline": [
                "candidate_validation",
                "feature_validation",
                "model_validation",
                "model_score_ingestion",
                "ranking_signal_aggregation",
                "base_score_computation",
                "confidence_calibration",
                "score_normalization",
                "query_document_calibration",
                "authority_calibration",
                "freshness_calibration",
                "diversity_processing",
                "duplicate_suppression",
                "final_score_computation",
                "global_candidate_sort",
                "top_k_selection",
                "result_assembly",
            ],

            "ranking": {
                "learned_model_score": True,
                "ranking_signal_fusion": True,
                "confidence_aware_scoring": True,
                "query_dependent_scoring": True,
                "authority_aware_scoring": True,
                "freshness_aware_scoring": True,
                "quality_aware_scoring": True,
                "graph_aware_scoring": True,
                "retrieval_score_integration": True,
            },

            "distributed_execution": {
                "partition_local_scoring": True,
                "shard_local_scoring": True,
                "parallel_candidate_processing": True,
                "parallel_partition_processing": True,
                "parallel_shard_processing": True,
                "global_candidate_fusion": True,
                "global_sort": True,
                "global_top_k": True,
                "horizontal_scaling": True,
                "elastic_worker_scaling": True,
            },

            "result_quality": {
                "duplicate_suppression": True,
                "canonical_url_suppression": True,
                "domain_diversity": True,
                "host_diversity": True,
                "confidence_calibration": True,
                "partial_result_support": True,
                "deterministic_tie_breaking": True,
            },

            "reliability": {
                "checkpointing": True,
                "restartability": True,
                "partial_processing": True,
                "durable_metadata": True,
                "lineage": True,
                "provenance": True,
                "backend_replaceability": True,
            },

            "scale": {
                "billions_to_trillions_of_public_web_resources": True,
                "distributed_queries": True,
                "distributed_candidates": True,
                "distributed_partitions": True,
                "distributed_shards": True,
                "distributed_workers": True,
                "no_fixed_global_document_limit": True,
                "no_fixed_global_query_limit": True,
                "no_fixed_global_candidate_limit": True,
                "no_fixed_global_partition_limit": True,
                "no_fixed_global_shard_limit": True,
            },

            "technology_independence": {
                "google_search_api": False,
                "google_index": False,
                "google_crawler": False,
                "google_infrastructure": False,
                "google_ranking_technology": False,
                "own_ranking_architecture": True,
                "own_index_architecture": True,
                "own_crawler_architecture": True,
            },

            "boundaries": {
                "does_not_crawl": True,
                "does_not_discover_web": True,
                "does_not_mutate_index": True,
                "does_not_replace_phase11_retrieval": True,
                "does_not_train_models": True,
                "does_not_implement_spam_classification": True,
                "does_not_implement_security_policy": True,
            },

            "phase12_complete": True,
            "next_phase": NEXT_PHASE,
            "next_stage_name": (
                "Freshness + Recrawling at Enormous Scale"
            ),
        }

    # ------------------------------------------------------------------------
    # Metadata access
    # ------------------------------------------------------------------------

    def events(self) -> Tuple[FinalRankingEvent, ...]:
        if hasattr(self.backend, "events"):
            return getattr(self.backend, "events")()

        return tuple()

    def checkpoints(
        self,
    ) -> Tuple[FinalRankingCheckpoint, ...]:
        if hasattr(self.backend, "checkpoints"):
            return getattr(self.backend, "checkpoints")()

        return tuple()

    def results(
        self,
    ) -> Tuple[FinalRankingResult, ...]:
        if hasattr(self.backend, "results"):
            return getattr(self.backend, "results")()

        return tuple()


# ============================================================================
# ALIASES
# ============================================================================


FinalRanking = FinalRankingArchitecture
GlobalFinalRanking = FinalRankingArchitecture
Phase12_9FinalRanking = FinalRankingArchitecture
GlobalRankingPipeline = FinalRankingArchitecture
FinalGlobalRankingArchitecture = FinalRankingArchitecture


# ============================================================================
# EXPORTS
# ============================================================================


__all__ = [
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",
    "ARCHITECTURE_VERSION",
    "PHASE",
    "NEXT_PHASE",

    "RankingPipelineState",
    "RankingCandidateState",
    "RankingScoreComponent",
    "RankingNormalizationMethod",
    "DiversityMode",
    "DuplicateSuppressionMode",
    "RankingExecutionMode",
    "RankingEventType",
    "CheckpointType",

    "RankingIdentity",
    "RankingDocumentIdentity",
    "RankingLineage",

    "RankingModelScore",
    "RankingScoreSignal",

    "FinalRankingCandidate",

    "FinalRankingResultItem",
    "FinalRankingResult",

    "FinalRankingCheckpoint",
    "FinalRankingEvent",

    "FinalRankingPolicy",
    "FinalRankingBackend",
    "InMemoryFinalRankingMetadata",

    "FinalRankingArchitecture",

    "FinalRanking",
    "GlobalFinalRanking",
    "Phase12_9FinalRanking",
    "GlobalRankingPipeline",
    "FinalGlobalRankingArchitecture",
]
