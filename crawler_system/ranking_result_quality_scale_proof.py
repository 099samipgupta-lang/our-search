"""
OUR SEARCH
Phase 15.5 — Ranking & Result-Quality Scale Proof

Purpose
-------
Defines the scale-proof architecture for validating ranking and result-quality
systems under enormous search workloads.

Target:
    billions -> trillions of publicly accessible Web resources

Scope:
    retrieved candidates
        -> relevance evaluation
        -> ranking feature availability
        -> authority signals
        -> freshness signals
        -> quality signals
        -> query-dependent ranking
        -> score generation
        -> candidate ordering
        -> result diversity
        -> duplicate suppression
        -> ranking stability
        -> quality evaluation
        -> distributed consistency
        -> latency/resource constraints
        -> failure recovery

This module is a validation/control-plane architecture.

It does NOT:
    - crawl the Web
    - perform HTTP requests
    - access Google Search
    - use Google's index
    - use Google's ranking system
    - mutate a production index
    - execute production ranking workers
    - train a production ML model
    - expose private user data
    - perform offensive security activity
    - depend on Google infrastructure

The architecture consumes externally supplied ranking and result-quality
measurements and evaluates whether the ranking subsystem satisfies a
configured scale-proof policy.

Important:
-----------
Passing this stage means that the supplied ranking/result-quality evidence
satisfies the configured validation policy.

It does NOT mean that OUR SEARCH has already achieved production Google-scale
ranking quality across the live public Web.

Actual production quality requires continuous operation of the complete
crawler, index, retrieval, ranking, freshness, quality, security, and serving
systems against the public Web.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol, Sequence, Tuple


# ============================================================================
# ARCHITECTURE CONSTANTS
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"

GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "ranking-result-quality-scale-proof.v1"

PHASE = "15.5"
PREVIOUS_STAGE = "15.4"
NEXT_STAGE = "15.6"

PHASE_NAME = "Full Scale-Proof Program"
STAGE_NAME = "Ranking & Result-Quality Scale Proof"
NEXT_STAGE_NAME = "Freshness & Recrawling Scale Proof"

MAX_RESOURCE_COUNT = 10**18
MAX_QUERY_COUNT = 10**18
MAX_CANDIDATE_COUNT = 10**18
MAX_WORKER_COUNT = 10**12
MAX_SHARD_COUNT = 10**12
MAX_RESULT_COUNT = 10**18

DEFAULT_MIN_RESOURCE_COUNT = 1_000_000
DEFAULT_MIN_QUERY_COUNT = 100_000
DEFAULT_MIN_CANDIDATE_COUNT = 1_000

DEFAULT_MIN_RELEVANCE = 0.90
DEFAULT_MIN_AUTHORITY = 0.80
DEFAULT_MIN_FRESHNESS = 0.80
DEFAULT_MIN_CONTENT_QUALITY = 0.85
DEFAULT_MIN_QUERY_DEPENDENT_QUALITY = 0.85
DEFAULT_MIN_RANKING_CORRECTNESS = 0.95
DEFAULT_MIN_ORDERING_STABILITY = 0.95
DEFAULT_MIN_RESULT_DIVERSITY = 0.80
DEFAULT_MIN_DISTRIBUTED_CONSISTENCY = 0.95
DEFAULT_MIN_FAILURE_RECOVERY = 0.95
DEFAULT_MIN_RESOURCE_EFFICIENCY = 0.80

DEFAULT_MAX_DUPLICATE_RATE = 0.03
DEFAULT_MAX_RANKING_ERROR_RATE = 0.02
DEFAULT_MAX_RESULT_LOSS_RATE = 0.01
DEFAULT_MAX_QUALITY_REGRESSION_RATE = 0.05
DEFAULT_MAX_TIMEOUT_RATE = 0.02

DEFAULT_MAX_P50_LATENCY_MS = 250.0
DEFAULT_MAX_P95_LATENCY_MS = 750.0
DEFAULT_MAX_P99_LATENCY_MS = 1500.0

DEFAULT_MIN_TOP_K_COVERAGE = 0.95
DEFAULT_MIN_NDCG = 0.80
DEFAULT_MIN_MRR = 0.80
DEFAULT_MIN_PRECISION_AT_K = 0.80


# ============================================================================
# ENUMERATIONS
# ============================================================================


class RankingProofState(str, Enum):
    CREATED = "created"
    COLLECTING = "collecting"
    NORMALIZED = "normalized"
    EVALUATED = "evaluated"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class RankingTestDimension(str, Enum):
    RESOURCE_SCALE = "resource_scale"
    QUERY_VOLUME = "query_volume"
    CANDIDATE_VOLUME = "candidate_volume"

    RELEVANCE = "relevance"
    AUTHORITY = "authority"
    FRESHNESS = "freshness"
    CONTENT_QUALITY = "content_quality"
    QUERY_DEPENDENT_QUALITY = "query_dependent_quality"

    RANKING_CORRECTNESS = "ranking_correctness"
    ORDERING_STABILITY = "ordering_stability"
    RESULT_DIVERSITY = "result_diversity"
    DISTRIBUTED_CONSISTENCY = "distributed_consistency"

    TOP_K_COVERAGE = "top_k_coverage"
    NDCG = "ndcg"
    MRR = "mrr"
    PRECISION_AT_K = "precision_at_k"

    DUPLICATE_PREVENTION = "duplicate_prevention"
    RANKING_ERROR_HANDLING = "ranking_error_handling"
    RESULT_LOSS_PREVENTION = "result_loss_prevention"
    QUALITY_REGRESSION_CONTROL = "quality_regression_control"

    P50_LATENCY = "p50_latency"
    P95_LATENCY = "p95_latency"
    P99_LATENCY = "p99_latency"

    FAILURE_RECOVERY = "failure_recovery"
    RESOURCE_EFFICIENCY = "resource_efficiency"
    OVERLOAD_RESILIENCE = "overload_resilience"


class EvidenceStrength(str, Enum):
    OBSERVED = "observed"
    VERIFIED = "verified"
    DERIVED = "derived"
    SIMULATED = "simulated"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class EvidenceKind(str, Enum):
    RESOURCE = "resource"
    QUERY = "query"
    CANDIDATE = "candidate"
    RELEVANCE = "relevance"
    AUTHORITY = "authority"
    FRESHNESS = "freshness"
    QUALITY = "quality"
    QUERY_DEPENDENT = "query_dependent"
    CORRECTNESS = "correctness"
    STABILITY = "stability"
    DIVERSITY = "diversity"
    CONSISTENCY = "consistency"
    EVALUATION = "evaluation"
    DUPLICATE = "duplicate"
    ERROR = "error"
    LOSS = "loss"
    REGRESSION = "regression"
    LATENCY = "latency"
    RECOVERY = "recovery"
    RESOURCE_USAGE = "resource_usage"
    OVERLOAD = "overload"


class RankingScaleBand(str, Enum):
    SMALL = "small"
    LARGE = "large"
    MASSIVE = "massive"
    BILLIONS = "billions"
    TRILLIONS = "trillions"
    UNKNOWN = "unknown"


class ProofDecision(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    DEFER = "defer"


class ProofReason(str, Enum):
    ALL_REQUIRED_DIMENSIONS_PASSED = "all_required_dimensions_passed"
    MISSING_EVIDENCE = "missing_evidence"

    INSUFFICIENT_RESOURCE_SCALE = "insufficient_resource_scale"
    INSUFFICIENT_QUERY_VOLUME = "insufficient_query_volume"
    INSUFFICIENT_CANDIDATE_VOLUME = "insufficient_candidate_volume"

    RELEVANCE_TOO_LOW = "relevance_too_low"
    AUTHORITY_TOO_LOW = "authority_too_low"
    FRESHNESS_TOO_LOW = "freshness_too_low"
    CONTENT_QUALITY_TOO_LOW = "content_quality_too_low"
    QUERY_DEPENDENT_QUALITY_TOO_LOW = "query_dependent_quality_too_low"

    RANKING_CORRECTNESS_TOO_LOW = "ranking_correctness_too_low"
    ORDERING_INSTABILITY = "ordering_instability"
    RESULT_DIVERSITY_TOO_LOW = "result_diversity_too_low"
    DISTRIBUTED_CONSISTENCY_TOO_LOW = "distributed_consistency_too_low"

    TOP_K_COVERAGE_TOO_LOW = "top_k_coverage_too_low"
    NDCG_TOO_LOW = "ndcg_too_low"
    MRR_TOO_LOW = "mrr_too_low"
    PRECISION_AT_K_TOO_LOW = "precision_at_k_too_low"

    DUPLICATE_RATE_TOO_HIGH = "duplicate_rate_too_high"
    RANKING_ERROR_RATE_TOO_HIGH = "ranking_error_rate_too_high"
    RESULT_LOSS_RATE_TOO_HIGH = "result_loss_rate_too_high"
    QUALITY_REGRESSION_TOO_HIGH = "quality_regression_too_high"

    P50_LATENCY_TOO_HIGH = "p50_latency_too_high"
    P95_LATENCY_TOO_HIGH = "p95_latency_too_high"
    P99_LATENCY_TOO_HIGH = "p99_latency_too_high"

    FAILURE_RECOVERY_TOO_LOW = "failure_recovery_too_low"
    RESOURCE_EFFICIENCY_TOO_LOW = "resource_efficiency_too_low"
    OVERLOAD_RESILIENCE_TOO_LOW = "overload_resilience_too_low"

    INVALID_MEASUREMENT = "invalid_measurement"
    PARTIAL_EVIDENCE = "partial_evidence"
    NON_PUBLIC_SCOPE = "non_public_scope"


class CheckpointType(str, Enum):
    PROOF_CREATED = "proof_created"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    DIMENSION_VALIDATED = "dimension_validated"
    RANKING_EVALUATED = "ranking_evaluated"
    PROOF_FINALIZED = "proof_finalized"


class EventType(str, Enum):
    PROOF_CREATED = "proof_created"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    EVIDENCE_REJECTED = "evidence_rejected"
    DIMENSION_EVALUATED = "dimension_evaluated"
    RANKING_EVALUATED = "ranking_evaluated"
    PROOF_PASSED = "proof_passed"
    PROOF_FAILED = "proof_failed"
    PROOF_DEFERRED = "proof_deferred"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass(frozen=True)
class RankingProofIdentity:
    proof_id: str
    evaluation_id: str
    created_at: str
    schema_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class RankingProofLineage:
    source_systems: Tuple[str, ...] = ()
    source_phases: Tuple[str, ...] = ()
    source_stages: Tuple[str, ...] = ()
    parent_evaluation_ids: Tuple[str, ...] = ()
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class RankingQualityMeasurement:
    dimension: RankingTestDimension

    resource_count: int
    query_count: int
    candidate_count: int

    relevance_score: float
    authority_score: float
    freshness_score: float
    content_quality_score: float
    query_dependent_quality_score: float

    ranking_correctness: float
    ordering_stability: float
    result_diversity: float
    distributed_consistency: float

    top_k_coverage: float
    ndcg: float
    mrr: float
    precision_at_k: float

    duplicate_rate: float
    ranking_error_rate: float
    result_loss_rate: float
    quality_regression_rate: float

    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float

    failure_recovery_rate: float
    resource_efficiency: float
    overload_resilience: float

    worker_count: int = 0
    shard_count: int = 0
    result_count: int = 0
    measurement_window_seconds: float = 0.0

    evidence_strength: EvidenceStrength = EvidenceStrength.OBSERVED
    evidence_kind: EvidenceKind = EvidenceKind.EVALUATION

    source: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RankingProofInput:
    identity: RankingProofIdentity
    lineage: RankingProofLineage

    measurements: Tuple[RankingQualityMeasurement, ...]

    target_resource_count: int
    target_query_count: int
    target_candidate_count: int

    public_web_scope: bool = True
    partial_evidence_allowed: bool = True

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RankingProofPolicy:
    minimum_resource_count: int = DEFAULT_MIN_RESOURCE_COUNT
    minimum_query_count: int = DEFAULT_MIN_QUERY_COUNT
    minimum_candidate_count: int = DEFAULT_MIN_CANDIDATE_COUNT

    minimum_relevance_score: float = DEFAULT_MIN_RELEVANCE
    minimum_authority_score: float = DEFAULT_MIN_AUTHORITY
    minimum_freshness_score: float = DEFAULT_MIN_FRESHNESS
    minimum_content_quality_score: float = (
        DEFAULT_MIN_CONTENT_QUALITY
    )
    minimum_query_dependent_quality_score: float = (
        DEFAULT_MIN_QUERY_DEPENDENT_QUALITY
    )

    minimum_ranking_correctness: float = (
        DEFAULT_MIN_RANKING_CORRECTNESS
    )
    minimum_ordering_stability: float = (
        DEFAULT_MIN_ORDERING_STABILITY
    )
    minimum_result_diversity: float = (
        DEFAULT_MIN_RESULT_DIVERSITY
    )
    minimum_distributed_consistency: float = (
        DEFAULT_MIN_DISTRIBUTED_CONSISTENCY
    )

    minimum_top_k_coverage: float = DEFAULT_MIN_TOP_K_COVERAGE
    minimum_ndcg: float = DEFAULT_MIN_NDCG
    minimum_mrr: float = DEFAULT_MIN_MRR
    minimum_precision_at_k: float = DEFAULT_MIN_PRECISION_AT_K

    maximum_duplicate_rate: float = DEFAULT_MAX_DUPLICATE_RATE
    maximum_ranking_error_rate: float = (
        DEFAULT_MAX_RANKING_ERROR_RATE
    )
    maximum_result_loss_rate: float = (
        DEFAULT_MAX_RESULT_LOSS_RATE
    )
    maximum_quality_regression_rate: float = (
        DEFAULT_MAX_QUALITY_REGRESSION_RATE
    )

    maximum_p50_latency_ms: float = DEFAULT_MAX_P50_LATENCY_MS
    maximum_p95_latency_ms: float = DEFAULT_MAX_P95_LATENCY_MS
    maximum_p99_latency_ms: float = DEFAULT_MAX_P99_LATENCY_MS

    minimum_failure_recovery_rate: float = (
        DEFAULT_MIN_FAILURE_RECOVERY
    )
    minimum_resource_efficiency: float = (
        DEFAULT_MIN_RESOURCE_EFFICIENCY
    )
    minimum_overload_resilience: float = (
        DEFAULT_MIN_ORDERING_STABILITY
    )

    require_all_dimensions: bool = True
    allow_partial_evidence: bool = True
    require_public_web_scope: bool = True
    require_deterministic_evaluation: bool = True


@dataclass(frozen=True)
class RankingFinding:
    dimension: Optional[RankingTestDimension]
    reason: ProofReason
    message: str

    measured_value: Optional[float] = None
    required_value: Optional[float] = None

    evidence_strength: EvidenceStrength = EvidenceStrength.UNKNOWN
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class RankingDimensionResult:
    dimension: RankingTestDimension
    passed: bool

    resource_count: int
    query_count: int
    candidate_count: int

    relevance_score: float
    authority_score: float
    freshness_score: float
    content_quality_score: float
    query_dependent_quality_score: float

    ranking_correctness: float
    ordering_stability: float
    result_diversity: float
    distributed_consistency: float

    top_k_coverage: float
    ndcg: float
    mrr: float
    precision_at_k: float

    duplicate_rate: float
    ranking_error_rate: float
    result_loss_rate: float
    quality_regression_rate: float

    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float

    failure_recovery_rate: float
    resource_efficiency: float
    overload_resilience: float

    findings: Tuple[RankingFinding, ...] = ()


@dataclass(frozen=True)
class GlobalRankingResult:
    passed: bool

    dimensions_evaluated: int
    expected_dimensions: int

    aggregate_relevance: float
    aggregate_authority: float
    aggregate_freshness: float
    aggregate_content_quality: float
    aggregate_query_dependent_quality: float

    aggregate_ranking_correctness: float
    aggregate_ordering_stability: float
    aggregate_result_diversity: float
    aggregate_distributed_consistency: float

    aggregate_top_k_coverage: float
    aggregate_ndcg: float
    aggregate_mrr: float
    aggregate_precision_at_k: float

    aggregate_duplicate_rate: float
    aggregate_ranking_error_rate: float
    aggregate_result_loss_rate: float
    aggregate_quality_regression_rate: float

    aggregate_p50_latency_ms: float
    aggregate_p95_latency_ms: float
    aggregate_p99_latency_ms: float

    aggregate_failure_recovery: float
    aggregate_resource_efficiency: float
    aggregate_overload_resilience: float

    findings: Tuple[RankingFinding, ...] = ()


@dataclass(frozen=True)
class RankingProofCheckpoint:
    checkpoint_id: str
    checkpoint_type: CheckpointType

    timestamp: str

    proof_id: str
    evaluation_id: str

    payload_digest: str
    sequence_number: int

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RankingProofEvent:
    event_id: str
    event_type: EventType

    timestamp: str

    proof_id: str
    evaluation_id: str

    payload_digest: str

    dimension: Optional[RankingTestDimension] = None

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RankingProofResult:
    identity: RankingProofIdentity
    lineage: RankingProofLineage

    state: RankingProofState
    decision: ProofDecision

    target_resource_count: int
    target_query_count: int
    target_candidate_count: int

    evaluated_resource_count: int
    evaluated_query_count: int
    evaluated_candidate_count: int

    scale_band: RankingScaleBand

    dimension_results: Tuple[RankingDimensionResult, ...]
    global_result: GlobalRankingResult

    findings: Tuple[RankingFinding, ...]

    overall_score: float

    partial_evidence: bool
    deterministic: bool

    checkpoints: Tuple[RankingProofCheckpoint, ...] = ()
    events: Tuple[RankingProofEvent, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class RankingProofBackend(Protocol):
    def save_result(
        self,
        result: RankingProofResult,
    ) -> None:
        ...

    def load_result(
        self,
        evaluation_id: str,
    ) -> Optional[RankingProofResult]:
        ...

    def save_checkpoint(
        self,
        checkpoint: RankingProofCheckpoint,
    ) -> None:
        ...

    def save_event(
        self,
        event: RankingProofEvent,
    ) -> None:
        ...


class InMemoryRankingProofBackend:
    """
    Deterministic reference backend.

    Production deployments may replace this with a distributed durable
    persistence system.
    """

    def __init__(self) -> None:
        self._results: Dict[str, RankingProofResult] = {}
        self._checkpoints: Dict[str, RankingProofCheckpoint] = {}
        self._events: Dict[str, RankingProofEvent] = {}

    def save_result(
        self,
        result: RankingProofResult,
    ) -> None:
        self._results[result.identity.evaluation_id] = result

    def load_result(
        self,
        evaluation_id: str,
    ) -> Optional[RankingProofResult]:
        return self._results.get(evaluation_id)

    def save_checkpoint(
        self,
        checkpoint: RankingProofCheckpoint,
    ) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(
        self,
        event: RankingProofEvent,
    ) -> None:
        self._events[event.event_id] = event


# ============================================================================
# ARCHITECTURE
# ============================================================================


class RankingResultQualityScaleProof:
    """
    Phase 15.5 ranking/result-quality scale-proof control plane.

    It validates externally supplied ranking and result-quality measurements.

    It does not execute production ranking workloads.
    """

    REQUIRED_DIMENSIONS: Tuple[RankingTestDimension, ...] = (
        RankingTestDimension.RESOURCE_SCALE,
        RankingTestDimension.QUERY_VOLUME,
        RankingTestDimension.CANDIDATE_VOLUME,

        RankingTestDimension.RELEVANCE,
        RankingTestDimension.AUTHORITY,
        RankingTestDimension.FRESHNESS,
        RankingTestDimension.CONTENT_QUALITY,
        RankingTestDimension.QUERY_DEPENDENT_QUALITY,

        RankingTestDimension.RANKING_CORRECTNESS,
        RankingTestDimension.ORDERING_STABILITY,
        RankingTestDimension.RESULT_DIVERSITY,
        RankingTestDimension.DISTRIBUTED_CONSISTENCY,

        RankingTestDimension.TOP_K_COVERAGE,
        RankingTestDimension.NDCG,
        RankingTestDimension.MRR,
        RankingTestDimension.PRECISION_AT_K,

        RankingTestDimension.DUPLICATE_PREVENTION,
        RankingTestDimension.RANKING_ERROR_HANDLING,
        RankingTestDimension.RESULT_LOSS_PREVENTION,
        RankingTestDimension.QUALITY_REGRESSION_CONTROL,

        RankingTestDimension.P50_LATENCY,
        RankingTestDimension.P95_LATENCY,
        RankingTestDimension.P99_LATENCY,

        RankingTestDimension.FAILURE_RECOVERY,
        RankingTestDimension.RESOURCE_EFFICIENCY,
        RankingTestDimension.OVERLOAD_RESILIENCE,
    )

    def __init__(
        self,
        backend: Optional[RankingProofBackend] = None,
        policy: Optional[RankingProofPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryRankingProofBackend()
        self.policy = policy or RankingProofPolicy()

    # ------------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------------

    def evaluate(
        self,
        proof_input: RankingProofInput,
    ) -> RankingProofResult:
        self._validate_input(proof_input)

        checkpoints = []
        events = []

        self._emit_event(
            events,
            proof_input,
            EventType.PROOF_CREATED,
        )

        self._checkpoint(
            checkpoints,
            proof_input,
            CheckpointType.PROOF_CREATED,
            1,
        )

        normalized = self._normalize_input(
            proof_input
        )

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.INPUT_NORMALIZED,
            2,
        )

        dimension_results = []

        for sequence, measurement in enumerate(
            normalized.measurements,
            start=1,
        ):
            result = self._evaluate_dimension(
                measurement
            )

            dimension_results.append(result)

            self._emit_event(
                events,
                normalized,
                EventType.DIMENSION_EVALUATED,
                dimension=measurement.dimension,
            )

            self._checkpoint(
                checkpoints,
                normalized,
                CheckpointType.DIMENSION_VALIDATED,
                sequence_number=2 + sequence,
                payload=result,
            )

        global_result = self._evaluate_global(
            normalized,
            dimension_results,
        )

        self._emit_event(
            events,
            normalized,
            EventType.RANKING_EVALUATED,
        )

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.RANKING_EVALUATED,
            sequence_number=3 + len(dimension_results),
            payload=global_result,
        )

        findings = self._collect_findings(
            normalized,
            dimension_results,
            global_result,
        )

        decision = self._make_decision(
            normalized,
            dimension_results,
            global_result,
            findings,
        )

        state = self._state_from_decision(
            decision
        )

        evaluated_resource_count = (
            self._evaluated_resource_count(
                dimension_results
            )
        )

        evaluated_query_count = (
            self._evaluated_query_count(
                dimension_results
            )
        )

        evaluated_candidate_count = (
            self._evaluated_candidate_count(
                dimension_results
            )
        )

        scale_reference = max(
            evaluated_resource_count,
            normalized.target_resource_count,
        )

        scale_band = self._scale_band(
            scale_reference
        )

        overall_score = self._overall_score(
            dimension_results,
            global_result,
            findings,
        )

        partial_evidence = any(
            measurement.evidence_strength
            in (
                EvidenceStrength.PARTIAL,
                EvidenceStrength.SIMULATED,
            )
            for measurement in normalized.measurements
        )

        result = RankingProofResult(
            identity=normalized.identity,
            lineage=normalized.lineage,

            state=state,
            decision=decision,

            target_resource_count=(
                normalized.target_resource_count
            ),
            target_query_count=(
                normalized.target_query_count
            ),
            target_candidate_count=(
                normalized.target_candidate_count
            ),

            evaluated_resource_count=(
                evaluated_resource_count
            ),
            evaluated_query_count=(
                evaluated_query_count
            ),
            evaluated_candidate_count=(
                evaluated_candidate_count
            ),

            scale_band=scale_band,

            dimension_results=tuple(
                dimension_results
            ),

            global_result=global_result,

            findings=tuple(findings),

            overall_score=overall_score,

            partial_evidence=partial_evidence,
            deterministic=(
                self.policy.require_deterministic_evaluation
            ),

            checkpoints=tuple(checkpoints),
            events=tuple(events),

            metadata={
                "architecture_version": (
                    ARCHITECTURE_VERSION
                ),
                "phase": PHASE,
                "scale_target": SCALE_TARGET,
                "google_scale_capability_target": (
                    GOOGLE_SCALE_CAPABILITY_TARGET
                ),
                "google_technology_dependency": (
                    GOOGLE_TECHNOLOGY_DEPENDENCY
                ),
            },
        )

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.PROOF_FINALIZED,
            sequence_number=4 + len(dimension_results),
            payload=result,
        )

        self.backend.save_result(result)

        self._emit_event(
            events,
            normalized,
            {
                ProofDecision.PASS: EventType.PROOF_PASSED,
                ProofDecision.FAIL: EventType.PROOF_FAILED,
                ProofDecision.DEFER: EventType.PROOF_DEFERRED,
            }[decision],
        )

        return result

    def evaluate_many(
        self,
        inputs: Iterable[RankingProofInput],
    ) -> Tuple[RankingProofResult, ...]:
        return tuple(
            self.evaluate(item)
            for item in inputs
        )

    def get_result(
        self,
        evaluation_id: str,
    ) -> Optional[RankingProofResult]:
        return self.backend.load_result(
            evaluation_id
        )

    # ------------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------------

    def _validate_input(
        self,
        proof_input: RankingProofInput,
    ) -> None:
        if not proof_input.identity.proof_id.strip():
            raise ValueError(
                "proof_id must not be empty"
            )

        if not proof_input.identity.evaluation_id.strip():
            raise ValueError(
                "evaluation_id must not be empty"
            )

        if not 0 <= proof_input.target_resource_count <= MAX_RESOURCE_COUNT:
            raise ValueError(
                "invalid target_resource_count"
            )

        if not 0 <= proof_input.target_query_count <= MAX_QUERY_COUNT:
            raise ValueError(
                "invalid target_query_count"
            )

        if not 0 <= proof_input.target_candidate_count <= MAX_CANDIDATE_COUNT:
            raise ValueError(
                "invalid target_candidate_count"
            )

        if (
            self.policy.require_public_web_scope
            and not proof_input.public_web_scope
        ):
            raise ValueError(
                "public Web scope is required"
            )

        seen = set()

        for measurement in proof_input.measurements:
            if measurement.dimension in seen:
                raise ValueError(
                    "duplicate measurement dimension: "
                    f"{measurement.dimension.value}"
                )

            seen.add(
                measurement.dimension
            )

            self._validate_measurement(
                measurement
            )

    def _validate_measurement(
        self,
        measurement: RankingQualityMeasurement,
    ) -> None:
        integer_fields = (
            (
                "resource_count",
                measurement.resource_count,
            ),
            (
                "query_count",
                measurement.query_count,
            ),
            (
                "candidate_count",
                measurement.candidate_count,
            ),
            (
                "worker_count",
                measurement.worker_count,
            ),
            (
                "shard_count",
                measurement.shard_count,
            ),
            (
                "result_count",
                measurement.result_count,
            ),
        )

        for name, value in integer_fields:
            if isinstance(value, bool):
                raise ValueError(
                    f"{name} must be an integer"
                )

            if value < 0:
                raise ValueError(
                    f"{name} must be non-negative"
                )

        bounded_counts = (
            (
                "resource_count",
                measurement.resource_count,
                MAX_RESOURCE_COUNT,
            ),
            (
                "query_count",
                measurement.query_count,
                MAX_QUERY_COUNT,
            ),
            (
                "candidate_count",
                measurement.candidate_count,
                MAX_CANDIDATE_COUNT,
            ),
            (
                "worker_count",
                measurement.worker_count,
                MAX_WORKER_COUNT,
            ),
            (
                "shard_count",
                measurement.shard_count,
                MAX_SHARD_COUNT,
            ),
            (
                "result_count",
                measurement.result_count,
                MAX_RESULT_COUNT,
            ),
        )

        for name, value, maximum in bounded_counts:
            if value > maximum:
                raise ValueError(
                    f"{name} exceeds supported bound"
                )

        numeric_fields = (
            (
                "relevance_score",
                measurement.relevance_score,
            ),
            (
                "authority_score",
                measurement.authority_score,
            ),
            (
                "freshness_score",
                measurement.freshness_score,
            ),
            (
                "content_quality_score",
                measurement.content_quality_score,
            ),
            (
                "query_dependent_quality_score",
                measurement.query_dependent_quality_score,
            ),
            (
                "ranking_correctness",
                measurement.ranking_correctness,
            ),
            (
                "ordering_stability",
                measurement.ordering_stability,
            ),
            (
                "result_diversity",
                measurement.result_diversity,
            ),
            (
                "distributed_consistency",
                measurement.distributed_consistency,
            ),
            (
                "top_k_coverage",
                measurement.top_k_coverage,
            ),
            (
                "ndcg",
                measurement.ndcg,
            ),
            (
                "mrr",
                measurement.mrr,
            ),
            (
                "precision_at_k",
                measurement.precision_at_k,
            ),
            (
                "duplicate_rate",
                measurement.duplicate_rate,
            ),
            (
                "ranking_error_rate",
                measurement.ranking_error_rate,
            ),
            (
                "result_loss_rate",
                measurement.result_loss_rate,
            ),
            (
                "quality_regression_rate",
                measurement.quality_regression_rate,
            ),
            (
                "p50_latency_ms",
                measurement.p50_latency_ms,
            ),
            (
                "p95_latency_ms",
                measurement.p95_latency_ms,
            ),
            (
                "p99_latency_ms",
                measurement.p99_latency_ms,
            ),
            (
                "failure_recovery_rate",
                measurement.failure_recovery_rate,
            ),
            (
                "resource_efficiency",
                measurement.resource_efficiency,
            ),
            (
                "overload_resilience",
                measurement.overload_resilience,
            ),
            (
                "measurement_window_seconds",
                measurement.measurement_window_seconds,
            ),
        )

        for name, value in numeric_fields:
            if isinstance(value, bool):
                raise ValueError(
                    f"{name} must be numeric"
                )

            if not math.isfinite(
                float(value)
            ):
                raise ValueError(
                    f"{name} must be finite"
                )

        non_negative_fields = (
            (
                "p50_latency_ms",
                measurement.p50_latency_ms,
            ),
            (
                "p95_latency_ms",
                measurement.p95_latency_ms,
            ),
            (
                "p99_latency_ms",
                measurement.p99_latency_ms,
            ),
            (
                "measurement_window_seconds",
                measurement.measurement_window_seconds,
            ),
        )

        for name, value in non_negative_fields:
            if value < 0:
                raise ValueError(
                    f"{name} must be non-negative"
                )

        bounded_fields = (
            (
                "relevance_score",
                measurement.relevance_score,
            ),
            (
                "authority_score",
                measurement.authority_score,
            ),
            (
                "freshness_score",
                measurement.freshness_score,
            ),
            (
                "content_quality_score",
                measurement.content_quality_score,
            ),
            (
                "query_dependent_quality_score",
                measurement.query_dependent_quality_score,
            ),
            (
                "ranking_correctness",
                measurement.ranking_correctness,
            ),
            (
                "ordering_stability",
                measurement.ordering_stability,
            ),
            (
                "result_diversity",
                measurement.result_diversity,
            ),
            (
                "distributed_consistency",
                measurement.distributed_consistency,
            ),
            (
                "top_k_coverage",
                measurement.top_k_coverage,
            ),
            (
                "ndcg",
                measurement.ndcg,
            ),
            (
                "mrr",
                measurement.mrr,
            ),
            (
                "precision_at_k",
                measurement.precision_at_k,
            ),
            (
                "duplicate_rate",
                measurement.duplicate_rate,
            ),
            (
                "ranking_error_rate",
                measurement.ranking_error_rate,
            ),
            (
                "result_loss_rate",
                measurement.result_loss_rate,
            ),
            (
                "quality_regression_rate",
                measurement.quality_regression_rate,
            ),
            (
                "failure_recovery_rate",
                measurement.failure_recovery_rate,
            ),
            (
                "resource_efficiency",
                measurement.resource_efficiency,
            ),
            (
                "overload_resilience",
                measurement.overload_resilience,
            ),
        )

        for name, value in bounded_fields:
            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"{name} must be between 0 and 1"
                )

        if (
            measurement.p95_latency_ms
            < measurement.p50_latency_ms
        ):
            raise ValueError(
                "p95_latency_ms must be >= p50_latency_ms"
            )

        if (
            measurement.p99_latency_ms
            < measurement.p95_latency_ms
        ):
            raise ValueError(
                "p99_latency_ms must be >= p95_latency_ms"
            )

    # ------------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------------

    def _normalize_input(
        self,
        proof_input: RankingProofInput,
    ) -> RankingProofInput:
        ordered = tuple(
            sorted(
                proof_input.measurements,
                key=lambda item: item.dimension.value,
            )
        )

        return RankingProofInput(
            identity=proof_input.identity,
            lineage=proof_input.lineage,

            measurements=ordered,

            target_resource_count=(
                proof_input.target_resource_count
            ),
            target_query_count=(
                proof_input.target_query_count
            ),
            target_candidate_count=(
                proof_input.target_candidate_count
            ),

            public_web_scope=(
                proof_input.public_web_scope
            ),
            partial_evidence_allowed=(
                proof_input.partial_evidence_allowed
            ),

            metadata=dict(
                proof_input.metadata
            ),
        )

    # ------------------------------------------------------------------------
    # Dimension evaluation
    # ------------------------------------------------------------------------

    def _evaluate_dimension(
        self,
        measurement: RankingQualityMeasurement,
    ) -> RankingDimensionResult:
        findings = []

        if (
            measurement.resource_count
            < self.policy.minimum_resource_count
        ):
            findings.append(
                RankingFinding(
                    dimension=measurement.dimension,
                    reason=(
                        ProofReason.INSUFFICIENT_RESOURCE_SCALE
                    ),
                    message=(
                        "Resource scale is below the configured "
                        "ranking-proof floor."
                    ),
                    measured_value=float(
                        measurement.resource_count
                    ),
                    required_value=float(
                        self.policy.minimum_resource_count
                    ),
                    evidence_strength=(
                        measurement.evidence_strength
                    ),
                )
            )

        if (
            measurement.query_count
            < self.policy.minimum_query_count
        ):
            findings.append(
                RankingFinding(
                    dimension=measurement.dimension,
                    reason=(
                        ProofReason.INSUFFICIENT_QUERY_VOLUME
                    ),
                    message=(
                        "Query volume is below the configured "
                        "ranking-proof floor."
                    ),
                    measured_value=float(
                        measurement.query_count
                    ),
                    required_value=float(
                        self.policy.minimum_query_count
                    ),
                    evidence_strength=(
                        measurement.evidence_strength
                    ),
                )
            )

        if (
            measurement.candidate_count
            < self.policy.minimum_candidate_count
        ):
            findings.append(
                RankingFinding(
                    dimension=measurement.dimension,
                    reason=(
                        ProofReason.INSUFFICIENT_CANDIDATE_VOLUME
                    ),
                    message=(
                        "Candidate volume is below the configured "
                        "ranking-proof floor."
                    ),
                    measured_value=float(
                        measurement.candidate_count
                    ),
                    required_value=float(
                        self.policy.minimum_candidate_count
                    ),
                    evidence_strength=(
                        measurement.evidence_strength
                    ),
                )
            )

        self._minimum(
            findings,
            measurement,
            "relevance_score",
            measurement.relevance_score,
            self.policy.minimum_relevance_score,
            ProofReason.RELEVANCE_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "authority_score",
            measurement.authority_score,
            self.policy.minimum_authority_score,
            ProofReason.AUTHORITY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "freshness_score",
            measurement.freshness_score,
            self.policy.minimum_freshness_score,
            ProofReason.FRESHNESS_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "content_quality_score",
            measurement.content_quality_score,
            self.policy.minimum_content_quality_score,
            ProofReason.CONTENT_QUALITY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "query_dependent_quality_score",
            measurement.query_dependent_quality_score,
            self.policy.minimum_query_dependent_quality_score,
            ProofReason.QUERY_DEPENDENT_QUALITY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "ranking_correctness",
            measurement.ranking_correctness,
            self.policy.minimum_ranking_correctness,
            ProofReason.RANKING_CORRECTNESS_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "ordering_stability",
            measurement.ordering_stability,
            self.policy.minimum_ordering_stability,
            ProofReason.ORDERING_INSTABILITY,
        )

        self._minimum(
            findings,
            measurement,
            "result_diversity",
            measurement.result_diversity,
            self.policy.minimum_result_diversity,
            ProofReason.RESULT_DIVERSITY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "distributed_consistency",
            measurement.distributed_consistency,
            self.policy.minimum_distributed_consistency,
            ProofReason.DISTRIBUTED_CONSISTENCY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "top_k_coverage",
            measurement.top_k_coverage,
            self.policy.minimum_top_k_coverage,
            ProofReason.TOP_K_COVERAGE_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "ndcg",
            measurement.ndcg,
            self.policy.minimum_ndcg,
            ProofReason.NDCG_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "mrr",
            measurement.mrr,
            self.policy.minimum_mrr,
            ProofReason.MRR_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "precision_at_k",
            measurement.precision_at_k,
            self.policy.minimum_precision_at_k,
            ProofReason.PRECISION_AT_K_TOO_LOW,
        )

        self._maximum(
            findings,
            measurement,
            "duplicate_rate",
            measurement.duplicate_rate,
            self.policy.maximum_duplicate_rate,
            ProofReason.DUPLICATE_RATE_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "ranking_error_rate",
            measurement.ranking_error_rate,
            self.policy.maximum_ranking_error_rate,
            ProofReason.RANKING_ERROR_RATE_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "result_loss_rate",
            measurement.result_loss_rate,
            self.policy.maximum_result_loss_rate,
            ProofReason.RESULT_LOSS_RATE_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "quality_regression_rate",
            measurement.quality_regression_rate,
            self.policy.maximum_quality_regression_rate,
            ProofReason.QUALITY_REGRESSION_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "p50_latency_ms",
            measurement.p50_latency_ms,
            self.policy.maximum_p50_latency_ms,
            ProofReason.P50_LATENCY_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "p95_latency_ms",
            measurement.p95_latency_ms,
            self.policy.maximum_p95_latency_ms,
            ProofReason.P95_LATENCY_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "p99_latency_ms",
            measurement.p99_latency_ms,
            self.policy.maximum_p99_latency_ms,
            ProofReason.P99_LATENCY_TOO_HIGH,
        )

        self._minimum(
            findings,
            measurement,
            "failure_recovery_rate",
            measurement.failure_recovery_rate,
            self.policy.minimum_failure_recovery_rate,
            ProofReason.FAILURE_RECOVERY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "resource_efficiency",
            measurement.resource_efficiency,
            self.policy.minimum_resource_efficiency,
            ProofReason.RESOURCE_EFFICIENCY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "overload_resilience",
            measurement.overload_resilience,
            self.policy.minimum_overload_resilience,
            ProofReason.OVERLOAD_RESILIENCE_TOO_LOW,
        )

        if (
            measurement.evidence_strength
            == EvidenceStrength.UNKNOWN
        ):
            findings.append(
                RankingFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.MISSING_EVIDENCE,
                    message="Evidence strength is unknown.",
                    evidence_strength=(
                        measurement.evidence_strength
                    ),
                )
            )

        if (
            measurement.evidence_strength
            == EvidenceStrength.PARTIAL
            and not self.policy.allow_partial_evidence
        ):
            findings.append(
                RankingFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.PARTIAL_EVIDENCE,
                    message=(
                        "Partial evidence is not allowed."
                    ),
                    evidence_strength=(
                        measurement.evidence_strength
                    ),
                )
            )

        return RankingDimensionResult(
            dimension=measurement.dimension,
            passed=len(findings) == 0,

            resource_count=measurement.resource_count,
            query_count=measurement.query_count,
            candidate_count=measurement.candidate_count,

            relevance_score=measurement.relevance_score,
            authority_score=measurement.authority_score,
            freshness_score=measurement.freshness_score,
            content_quality_score=(
                measurement.content_quality_score
            ),
            query_dependent_quality_score=(
                measurement.query_dependent_quality_score
            ),

            ranking_correctness=(
                measurement.ranking_correctness
            ),
            ordering_stability=(
                measurement.ordering_stability
            ),
            result_diversity=(
                measurement.result_diversity
            ),
            distributed_consistency=(
                measurement.distributed_consistency
            ),

            top_k_coverage=measurement.top_k_coverage,
            ndcg=measurement.ndcg,
            mrr=measurement.mrr,
            precision_at_k=measurement.precision_at_k,

            duplicate_rate=measurement.duplicate_rate,
            ranking_error_rate=(
                measurement.ranking_error_rate
            ),
            result_loss_rate=(
                measurement.result_loss_rate
            ),
            quality_regression_rate=(
                measurement.quality_regression_rate
            ),

            p50_latency_ms=measurement.p50_latency_ms,
            p95_latency_ms=measurement.p95_latency_ms,
            p99_latency_ms=measurement.p99_latency_ms,

            failure_recovery_rate=(
                measurement.failure_recovery_rate
            ),
            resource_efficiency=(
                measurement.resource_efficiency
            ),
            overload_resilience=(
                measurement.overload_resilience
            ),

            findings=tuple(findings),
        )

    def _minimum(
        self,
        findings: list[RankingFinding],
        measurement: RankingQualityMeasurement,
        name: str,
        measured: float,
        required: float,
        reason: ProofReason,
    ) -> None:
        if measured < required:
            findings.append(
                RankingFinding(
                    dimension=measurement.dimension,
                    reason=reason,
                    message=(
                        f"{name} is below the configured minimum."
                    ),
                    measured_value=measured,
                    required_value=required,
                    evidence_strength=(
                        measurement.evidence_strength
                    ),
                )
            )

    def _maximum(
        self,
        findings: list[RankingFinding],
        measurement: RankingQualityMeasurement,
        name: str,
        measured: float,
        maximum: float,
        reason: ProofReason,
    ) -> None:
        if measured > maximum:
            findings.append(
                RankingFinding(
                    dimension=measurement.dimension,
                    reason=reason,
                    message=(
                        f"{name} exceeds the configured maximum."
                    ),
                    measured_value=measured,
                    required_value=maximum,
                    evidence_strength=(
                        measurement.evidence_strength
                    ),
                )
            )

    # ------------------------------------------------------------------------
    # Global evaluation
    # ------------------------------------------------------------------------

    def _evaluate_global(
        self,
        proof_input: RankingProofInput,
        dimension_results: Sequence[RankingDimensionResult],
    ) -> GlobalRankingResult:
        findings = []

        present = {
            result.dimension
            for result in dimension_results
        }

        missing = (
            set(self.REQUIRED_DIMENSIONS)
            - present
        )

        if missing and self.policy.require_all_dimensions:
            for dimension in sorted(
                missing,
                key=lambda item: item.value,
            ):
                findings.append(
                    RankingFinding(
                        dimension=dimension,
                        reason=ProofReason.MISSING_EVIDENCE,
                        message=(
                            f"Required dimension "
                            f"'{dimension.value}' has no measurement."
                        ),
                    )
                )

        if not dimension_results:
            findings.append(
                RankingFinding(
                    dimension=None,
                    reason=ProofReason.MISSING_EVIDENCE,
                    message=(
                        "No ranking/result-quality measurements "
                        "were supplied."
                    ),
                )
            )

            return GlobalRankingResult(
                passed=False,

                dimensions_evaluated=0,
                expected_dimensions=(
                    len(self.REQUIRED_DIMENSIONS)
                ),

                aggregate_relevance=0.0,
                aggregate_authority=0.0,
                aggregate_freshness=0.0,
                aggregate_content_quality=0.0,
                aggregate_query_dependent_quality=0.0,

                aggregate_ranking_correctness=0.0,
                aggregate_ordering_stability=0.0,
                aggregate_result_diversity=0.0,
                aggregate_distributed_consistency=0.0,

                aggregate_top_k_coverage=0.0,
                aggregate_ndcg=0.0,
                aggregate_mrr=0.0,
                aggregate_precision_at_k=0.0,

                aggregate_duplicate_rate=1.0,
                aggregate_ranking_error_rate=1.0,
                aggregate_result_loss_rate=1.0,
                aggregate_quality_regression_rate=1.0,

                aggregate_p50_latency_ms=0.0,
                aggregate_p95_latency_ms=0.0,
                aggregate_p99_latency_ms=0.0,

                aggregate_failure_recovery=0.0,
                aggregate_resource_efficiency=0.0,
                aggregate_overload_resilience=0.0,

                findings=tuple(findings),
            )

        aggregate = {
            "relevance": self._mean(
                result.relevance_score
                for result in dimension_results
            ),

            "authority": self._mean(
                result.authority_score
                for result in dimension_results
            ),

            "freshness": self._mean(
                result.freshness_score
                for result in dimension_results
            ),

            "content_quality": self._mean(
                result.content_quality_score
                for result in dimension_results
            ),

            "query_dependent_quality": self._mean(
                result.query_dependent_quality_score
                for result in dimension_results
            ),

            "ranking_correctness": self._mean(
                result.ranking_correctness
                for result in dimension_results
            ),

            "ordering_stability": self._mean(
                result.ordering_stability
                for result in dimension_results
            ),

            "result_diversity": self._mean(
                result.result_diversity
                for result in dimension_results
            ),

            "distributed_consistency": self._mean(
                result.distributed_consistency
                for result in dimension_results
            ),

            "top_k_coverage": self._mean(
                result.top_k_coverage
                for result in dimension_results
            ),

            "ndcg": self._mean(
                result.ndcg
                for result in dimension_results
            ),

            "mrr": self._mean(
                result.mrr
                for result in dimension_results
            ),

            "precision_at_k": self._mean(
                result.precision_at_k
                for result in dimension_results
            ),

            "duplicate_rate": self._mean(
                result.duplicate_rate
                for result in dimension_results
            ),

            "ranking_error_rate": self._mean(
                result.ranking_error_rate
                for result in dimension_results
            ),

            "result_loss_rate": self._mean(
                result.result_loss_rate
                for result in dimension_results
            ),

            "quality_regression_rate": self._mean(
                result.quality_regression_rate
                for result in dimension_results
            ),

            "p50": self._mean(
                result.p50_latency_ms
                for result in dimension_results
            ),

            "p95": self._mean(
                result.p95_latency_ms
                for result in dimension_results
            ),

            "p99": self._mean(
                result.p99_latency_ms
                for result in dimension_results
            ),

            "failure_recovery": self._mean(
                result.failure_recovery_rate
                for result in dimension_results
            ),

            "resource_efficiency": self._mean(
                result.resource_efficiency
                for result in dimension_results
            ),

            "overload_resilience": self._mean(
                result.overload_resilience
                for result in dimension_results
            ),
        }

        return GlobalRankingResult(
            passed=len(findings) == 0,

            dimensions_evaluated=len(
                dimension_results
            ),

            expected_dimensions=len(
                self.REQUIRED_DIMENSIONS
            ),

            aggregate_relevance=aggregate[
                "relevance"
            ],

            aggregate_authority=aggregate[
                "authority"
            ],

            aggregate_freshness=aggregate[
                "freshness"
            ],

            aggregate_content_quality=aggregate[
                "content_quality"
            ],

            aggregate_query_dependent_quality=aggregate[
                "query_dependent_quality"
            ],

            aggregate_ranking_correctness=aggregate[
                "ranking_correctness"
            ],

            aggregate_ordering_stability=aggregate[
                "ordering_stability"
            ],

            aggregate_result_diversity=aggregate[
                "result_diversity"
            ],

            aggregate_distributed_consistency=aggregate[
                "distributed_consistency"
            ],

            aggregate_top_k_coverage=aggregate[
                "top_k_coverage"
            ],

            aggregate_ndcg=aggregate[
                "ndcg"
            ],

            aggregate_mrr=aggregate[
                "mrr"
            ],

            aggregate_precision_at_k=aggregate[
                "precision_at_k"
            ],

            aggregate_duplicate_rate=aggregate[
                "duplicate_rate"
            ],

            aggregate_ranking_error_rate=aggregate[
                "ranking_error_rate"
            ],

            aggregate_result_loss_rate=aggregate[
                "result_loss_rate"
            ],

            aggregate_quality_regression_rate=aggregate[
                "quality_regression_rate"
            ],

            aggregate_p50_latency_ms=aggregate[
                "p50"
            ],

            aggregate_p95_latency_ms=aggregate[
                "p95"
            ],

            aggregate_p99_latency_ms=aggregate[
                "p99"
            ],

            aggregate_failure_recovery=aggregate[
                "failure_recovery"
            ],

            aggregate_resource_efficiency=aggregate[
                "resource_efficiency"
            ],

            aggregate_overload_resilience=aggregate[
                "overload_resilience"
            ],

            findings=tuple(findings),
        )

    # ------------------------------------------------------------------------
    # Findings
    # ------------------------------------------------------------------------

    def _collect_findings(
        self,
        proof_input: RankingProofInput,
        dimension_results: Sequence[RankingDimensionResult],
        global_result: GlobalRankingResult,
    ) -> list[RankingFinding]:
        findings = []

        for result in dimension_results:
            findings.extend(
                result.findings
            )

        findings.extend(
            global_result.findings
        )

        if (
            self.policy.require_public_web_scope
            and not proof_input.public_web_scope
        ):
            findings.append(
                RankingFinding(
                    dimension=None,
                    reason=ProofReason.NON_PUBLIC_SCOPE,
                    message=(
                        "The ranking evidence is not explicitly "
                        "scoped to the public Web."
                    ),
                )
            )

        return findings

    # ------------------------------------------------------------------------
    # Decision
    # ------------------------------------------------------------------------

    def _make_decision(
        self,
        proof_input: RankingProofInput,
        dimension_results: Sequence[RankingDimensionResult],
        global_result: GlobalRankingResult,
        findings: Sequence[RankingFinding],
    ) -> ProofDecision:
        if not dimension_results:
            return ProofDecision.DEFER

        partial = any(
            measurement.evidence_strength
            in (
                EvidenceStrength.PARTIAL,
                EvidenceStrength.SIMULATED,
            )
            for measurement in proof_input.measurements
        )

        if (
            partial
            and not self.policy.allow_partial_evidence
        ):
            return ProofDecision.DEFER

        if findings:
            return ProofDecision.FAIL

        if not global_result.passed:
            return ProofDecision.FAIL

        if self.policy.require_all_dimensions:
            present = {
                result.dimension
                for result in dimension_results
            }

            if not set(
                self.REQUIRED_DIMENSIONS
            ).issubset(present):
                return ProofDecision.DEFER

        return ProofDecision.PASS

    @staticmethod
    def _state_from_decision(
        decision: ProofDecision,
    ) -> RankingProofState:
        if decision == ProofDecision.PASS:
            return RankingProofState.PASSED

        if decision == ProofDecision.FAIL:
            return RankingProofState.FAILED

        return RankingProofState.DEFERRED

    # ------------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------------

    def _overall_score(
        self,
        dimension_results: Sequence[RankingDimensionResult],
        global_result: GlobalRankingResult,
        findings: Sequence[RankingFinding],
    ) -> float:
        if not dimension_results:
            return 0.0

        dimension_scores = [
            self._dimension_score(result)
            for result in dimension_results
        ]

        base = self._mean(
            dimension_scores
        )

        relevance_quality = self._mean(
            (
                global_result.aggregate_relevance,
                global_result.aggregate_authority,
                global_result.aggregate_freshness,
                global_result.aggregate_content_quality,
                global_result.aggregate_query_dependent_quality,
            )
        )

        ranking_quality = self._mean(
            (
                global_result.aggregate_ranking_correctness,
                global_result.aggregate_ordering_stability,
                global_result.aggregate_result_diversity,
                global_result.aggregate_distributed_consistency,
                global_result.aggregate_top_k_coverage,
                global_result.aggregate_ndcg,
                global_result.aggregate_mrr,
                global_result.aggregate_precision_at_k,
            )
        )

        reliability_quality = self._mean(
            (
                max(
                    0.0,
                    1.0
                    - global_result.aggregate_duplicate_rate,
                ),
                max(
                    0.0,
                    1.0
                    - global_result.aggregate_ranking_error_rate,
                ),
                max(
                    0.0,
                    1.0
                    - global_result.aggregate_result_loss_rate,
                ),
                max(
                    0.0,
                    1.0
                    - global_result.aggregate_quality_regression_rate,
                ),
                global_result.aggregate_failure_recovery,
                global_result.aggregate_resource_efficiency,
                global_result.aggregate_overload_resilience,
                self._latency_quality(
                    global_result
                ),
            )
        )

        score = (
            (base * 0.25)
            + (relevance_quality * 0.25)
            + (ranking_quality * 0.35)
            + (reliability_quality * 0.15)
        )

        if findings:
            penalty = min(
                0.50,
                len(findings) / 100.0,
            )

            score *= max(
                0.0,
                1.0 - penalty,
            )

        return max(
            0.0,
            min(1.0, score),
        )

    def _dimension_score(
        self,
        result: RankingDimensionResult,
    ) -> float:
        latency_quality = (
            self._single_latency_quality(
                result
            )
        )

        values = (
            result.relevance_score,
            result.authority_score,
            result.freshness_score,
            result.content_quality_score,
            result.query_dependent_quality_score,

            result.ranking_correctness,
            result.ordering_stability,
            result.result_diversity,
            result.distributed_consistency,

            result.top_k_coverage,
            result.ndcg,
            result.mrr,
            result.precision_at_k,

            max(
                0.0,
                1.0 - result.duplicate_rate,
            ),

            max(
                0.0,
                1.0 - result.ranking_error_rate,
            ),

            max(
                0.0,
                1.0 - result.result_loss_rate,
            ),

            max(
                0.0,
                1.0 - result.quality_regression_rate,
            ),

            latency_quality,

            result.failure_recovery_rate,
            result.resource_efficiency,
            result.overload_resilience,
        )

        return self._mean(
            values
        )

    def _latency_quality(
        self,
        result: GlobalRankingResult,
    ) -> float:
        return self._mean(
            (
                self._bounded_latency_quality(
                    result.aggregate_p50_latency_ms,
                    self.policy.maximum_p50_latency_ms,
                ),
                self._bounded_latency_quality(
                    result.aggregate_p95_latency_ms,
                    self.policy.maximum_p95_latency_ms,
                ),
                self._bounded_latency_quality(
                    result.aggregate_p99_latency_ms,
                    self.policy.maximum_p99_latency_ms,
                ),
            )
        )

    def _single_latency_quality(
        self,
        result: RankingDimensionResult,
    ) -> float:
        return self._mean(
            (
                self._bounded_latency_quality(
                    result.p50_latency_ms,
                    self.policy.maximum_p50_latency_ms,
                ),
                self._bounded_latency_quality(
                    result.p95_latency_ms,
                    self.policy.maximum_p95_latency_ms,
                ),
                self._bounded_latency_quality(
                    result.p99_latency_ms,
                    self.policy.maximum_p99_latency_ms,
                ),
            )
        )

    @staticmethod
    def _bounded_latency_quality(
        measured: float,
        maximum: float,
    ) -> float:
        if maximum <= 0:
            return 0.0

        if measured <= maximum:
            return 1.0

        ratio = measured / maximum

        return max(
            0.0,
            min(
                1.0,
                1.0 / ratio,
            ),
        )

    # ------------------------------------------------------------------------
    # Aggregation helpers
    # ------------------------------------------------------------------------

    @staticmethod
    def _mean(
        values: Iterable[float],
    ) -> float:
        values_tuple = tuple(
            values
        )

        if not values_tuple:
            return 0.0

        return sum(
            values_tuple
        ) / len(values_tuple)

    # ------------------------------------------------------------------------
    # Evaluated volume
    # ------------------------------------------------------------------------

    @staticmethod
    def _evaluated_resource_count(
        results: Sequence[RankingDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.resource_count
            for result in results
        )

    @staticmethod
    def _evaluated_query_count(
        results: Sequence[RankingDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.query_count
            for result in results
        )

    @staticmethod
    def _evaluated_candidate_count(
        results: Sequence[RankingDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.candidate_count
            for result in results
        )

    # ------------------------------------------------------------------------
    # Scale classification
    # ------------------------------------------------------------------------

    @staticmethod
    def _scale_band(
        resource_count: int,
    ) -> RankingScaleBand:
        if resource_count >= 10**12:
            return RankingScaleBand.TRILLIONS

        if resource_count >= 10**9:
            return RankingScaleBand.BILLIONS

        if resource_count >= 10**8:
            return RankingScaleBand.MASSIVE

        if resource_count >= 10**6:
            return RankingScaleBand.LARGE

        if resource_count > 0:
            return RankingScaleBand.SMALL

        return RankingScaleBand.UNKNOWN

    # ------------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------------

    def _checkpoint(
        self,
        checkpoints: list[RankingProofCheckpoint],
        proof_input: RankingProofInput,
        checkpoint_type: CheckpointType,
        sequence_number: int,
        payload: Any = None,
    ) -> None:
        checkpoint_id = self._stable_id(
            "checkpoint",
            proof_input.identity.evaluation_id,
            checkpoint_type.value,
            str(sequence_number),
        )

        checkpoint = RankingProofCheckpoint(
            checkpoint_id=checkpoint_id,
            checkpoint_type=checkpoint_type,

            timestamp=self._now(),

            proof_id=proof_input.identity.proof_id,
            evaluation_id=proof_input.identity.evaluation_id,

            payload_digest=self._digest(
                payload
                if payload is not None
                else proof_input
            ),

            sequence_number=sequence_number,
        )

        checkpoints.append(
            checkpoint
        )

        self.backend.save_checkpoint(
            checkpoint
        )

    # ------------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------------

    def _emit_event(
        self,
        events: list[RankingProofEvent],
        proof_input: RankingProofInput,
        event_type: EventType,
        dimension: Optional[RankingTestDimension] = None,
    ) -> None:
        event_id = self._stable_id(
            "event",
            proof_input.identity.evaluation_id,
            event_type.value,
            dimension.value
            if dimension
            else "",
        )

        event = RankingProofEvent(
            event_id=event_id,
            event_type=event_type,

            timestamp=self._now(),

            proof_id=proof_input.identity.proof_id,
            evaluation_id=proof_input.identity.evaluation_id,

            payload_digest=self._digest(
                {
                    "event_type": event_type.value,
                    "dimension": (
                        dimension.value
                        if dimension
                        else None
                    ),
                }
            ),

            dimension=dimension,
        )

        events.append(
            event
        )

        self.backend.save_event(
            event
        )

    # ------------------------------------------------------------------------
    # Canonical hashing
    # ------------------------------------------------------------------------

    @staticmethod
    def _digest(
        value: Any,
    ) -> str:
        normalized = (
            RankingResultQualityScaleProof._canonicalize(
                value
            )
        )

        payload = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")

        return hashlib.sha256(
            payload
        ).hexdigest()

    @staticmethod
    def _canonicalize(
        value: Any,
    ) -> Any:
        if value is None:
            return None

        if isinstance(value, Enum):
            return value.value

        if hasattr(
            value,
            "__dataclass_fields__",
        ):
            return {
                key:
                    RankingResultQualityScaleProof._canonicalize(
                        getattr(value, key)
                    )
                for key in value.__dataclass_fields__
            }

        if isinstance(
            value,
            Mapping,
        ):
            return {
                str(key):
                    RankingResultQualityScaleProof._canonicalize(
                        item
                    )
                for key, item in sorted(
                    value.items(),
                    key=lambda pair: str(
                        pair[0]
                    ),
                )
            }

        if isinstance(
            value,
            (list, tuple),
        ):
            return [
                RankingResultQualityScaleProof._canonicalize(
                    item
                )
                for item in value
            ]

        if isinstance(
            value,
            (set, frozenset),
        ):
            items = [
                RankingResultQualityScaleProof._canonicalize(
                    item
                )
                for item in value
            ]

            return sorted(
                items,
                key=lambda item: json.dumps(
                    item,
                    sort_keys=True,
                    default=str,
                ),
            )

        if isinstance(
            value,
            float,
        ):
            if not math.isfinite(
                value
            ):
                raise ValueError(
                    "non-finite float cannot be canonicalized"
                )

            return value

        return value

    @staticmethod
    def _stable_id(
        *parts: str,
    ) -> str:
        return hashlib.sha256(
            "|".join(parts).encode("utf-8")
        ).hexdigest()[:32]

    @staticmethod
    def _now() -> str:
        return datetime.now(
            timezone.utc
        ).isoformat()


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================

GlobalRankingResultQualityScaleProof = (
    RankingResultQualityScaleProof
)

Phase15_5RankingResultQualityScaleProof = (
    RankingResultQualityScaleProof
)

RankingScaleProof = RankingResultQualityScaleProof

ResultQualityScaleProof = RankingResultQualityScaleProof

MassiveRankingScaleProof = RankingResultQualityScaleProof


# ============================================================================
# PUBLIC EXPORTS
# ============================================================================

__all__ = [
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",
    "ARCHITECTURE_VERSION",

    "PHASE",
    "PREVIOUS_STAGE",
    "NEXT_STAGE",

    "PHASE_NAME",
    "STAGE_NAME",
    "NEXT_STAGE_NAME",

    "RankingProofState",
    "RankingTestDimension",
    "EvidenceStrength",
    "EvidenceKind",
    "RankingScaleBand",
    "ProofDecision",
    "ProofReason",
    "CheckpointType",
    "EventType",

    "RankingProofIdentity",
    "RankingProofLineage",
    "RankingQualityMeasurement",
    "RankingProofInput",
    "RankingProofPolicy",
    "RankingFinding",
    "RankingDimensionResult",
    "GlobalRankingResult",
    "RankingProofCheckpoint",
    "RankingProofEvent",
    "RankingProofResult",

    "RankingProofBackend",
    "InMemoryRankingProofBackend",

    "RankingResultQualityScaleProof",
    "GlobalRankingResultQualityScaleProof",
    "Phase15_5RankingResultQualityScaleProof",
    "RankingScaleProof",
    "ResultQualityScaleProof",
    "MassiveRankingScaleProof",
]
