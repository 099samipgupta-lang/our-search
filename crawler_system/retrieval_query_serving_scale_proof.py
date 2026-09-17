"""
OUR SEARCH
Phase 15.4 — Retrieval & Query-Serving Scale Proof

Purpose
-------
Defines the scale-proof architecture for validating the retrieval and
query-serving subsystem under enormous search workloads.

Target:
    billions -> trillions of publicly accessible Web resources

Scope:
    query intake
        -> query normalization
        -> query understanding
        -> retrieval
        -> candidate generation
        -> shard fan-out
        -> distributed query execution
        -> result merging
        -> timeout handling
        -> cache behavior
        -> concurrency
        -> throughput
        -> latency
        -> availability
        -> correctness
        -> duplicate prevention
        -> overload protection
        -> failure recovery

This module is a validation/control-plane architecture.

It does NOT:
    - perform live Web crawling
    - perform HTTP requests
    - access Google Search
    - use Google's index
    - use Google's ranking technology
    - mutate a production search index
    - execute production query workers
    - expose user data
    - perform offensive scanning
    - depend on Google infrastructure

The architecture consumes externally supplied measurements and evidence and
evaluates whether the retrieval/query-serving subsystem satisfies a configured
scale-proof policy.

Important:
-----------
Passing this stage means the supplied retrieval/query-serving evidence
satisfies the configured validation policy.

It does NOT mean that OUR SEARCH has already served Google-scale production
traffic. Actual global search capability requires sustained operation of the
complete deployed crawler, index, retrieval, ranking, freshness, quality,
security, and serving infrastructure.
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

ARCHITECTURE_VERSION = "retrieval-query-serving-scale-proof.v1"

PHASE = "15.4"
PREVIOUS_STAGE = "15.3"
NEXT_STAGE = "15.5"

PHASE_NAME = "Full Scale-Proof Program"
STAGE_NAME = "Retrieval & Query-Serving Scale Proof"
NEXT_STAGE_NAME = "Ranking & Result-Quality Scale Proof"

MAX_RESOURCE_COUNT = 10**18
MAX_QUERY_COUNT = 10**18
MAX_WORKER_COUNT = 10**12
MAX_SHARD_COUNT = 10**12
MAX_CACHE_SIZE = 10**18

DEFAULT_MIN_RESOURCE_COUNT = 1_000_000
DEFAULT_MIN_QUERY_COUNT = 100_000

DEFAULT_MIN_QUERY_THROUGHPUT = 1_000.0

DEFAULT_MAX_P50_LATENCY_MS = 250.0
DEFAULT_MAX_P95_LATENCY_MS = 750.0
DEFAULT_MAX_P99_LATENCY_MS = 1_500.0
DEFAULT_MAX_TIMEOUT_RATE = 0.02
DEFAULT_MAX_ERROR_RATE = 0.02
DEFAULT_MAX_DUPLICATE_RATE = 0.02
DEFAULT_MAX_LOSS_RATE = 0.01

DEFAULT_MIN_AVAILABILITY = 0.995
DEFAULT_MIN_CORRECTNESS = 0.95
DEFAULT_MIN_RETRIEVAL_COMPLETION = 0.95
DEFAULT_MIN_SHARD_SUCCESS = 0.98
DEFAULT_MIN_CACHE_EFFECTIVENESS = 0.50
DEFAULT_MIN_CONCURRENCY_EFFICIENCY = 0.90
DEFAULT_MIN_RESOURCE_EFFICIENCY = 0.80
DEFAULT_MIN_RESULT_MERGE_SUCCESS = 0.99

DEFAULT_MAX_OVERLOAD_RATE = 0.02
DEFAULT_MAX_PARTIAL_RESPONSE_RATE = 0.02

DEFAULT_MIN_FAILURE_RECOVERY = 0.95
DEFAULT_MIN_BACKPRESSURE_STABILITY = 0.95


# ============================================================================
# ENUMERATIONS
# ============================================================================


class RetrievalProofState(str, Enum):
    CREATED = "created"
    COLLECTING = "collecting"
    NORMALIZED = "normalized"
    EVALUATED = "evaluated"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class RetrievalTestDimension(str, Enum):
    RESOURCE_SCALE = "resource_scale"
    QUERY_VOLUME = "query_volume"
    QUERY_THROUGHPUT = "query_throughput"
    P50_LATENCY = "p50_latency"
    P95_LATENCY = "p95_latency"
    P99_LATENCY = "p99_latency"
    CONCURRENCY = "concurrency"
    SHARD_FANOUT = "shard_fanout"
    SHARD_SUCCESS = "shard_success"
    RETRIEVAL_COMPLETION = "retrieval_completion"
    RESULT_MERGE = "result_merge"
    CORRECTNESS = "correctness"
    AVAILABILITY = "availability"
    CACHE_EFFECTIVENESS = "cache_effectiveness"
    RESOURCE_EFFICIENCY = "resource_efficiency"
    TIMEOUT_HANDLING = "timeout_handling"
    ERROR_HANDLING = "error_handling"
    DUPLICATE_PREVENTION = "duplicate_prevention"
    LOSS_PREVENTION = "loss_prevention"
    OVERLOAD_PROTECTION = "overload_protection"
    BACKPRESSURE = "backpressure"
    FAILURE_RECOVERY = "failure_recovery"
    PARTIAL_RESPONSE_CONTROL = "partial_response_control"


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
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    CONCURRENCY = "concurrency"
    SHARD = "shard"
    COMPLETION = "completion"
    MERGE = "merge"
    CORRECTNESS = "correctness"
    AVAILABILITY = "availability"
    CACHE = "cache"
    RESOURCE_USAGE = "resource_usage"
    TIMEOUT = "timeout"
    ERROR = "error"
    DUPLICATE = "duplicate"
    LOSS = "loss"
    OVERLOAD = "overload"
    BACKPRESSURE = "backpressure"
    RECOVERY = "recovery"
    PARTIAL_RESPONSE = "partial_response"


class QueryServingScaleBand(str, Enum):
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
    QUERY_THROUGHPUT_TOO_LOW = "query_throughput_too_low"
    P50_LATENCY_TOO_HIGH = "p50_latency_too_high"
    P95_LATENCY_TOO_HIGH = "p95_latency_too_high"
    P99_LATENCY_TOO_HIGH = "p99_latency_too_high"
    CONCURRENCY_TOO_LOW = "concurrency_too_low"
    SHARD_SUCCESS_TOO_LOW = "shard_success_too_low"
    RETRIEVAL_COMPLETION_TOO_LOW = "retrieval_completion_too_low"
    RESULT_MERGE_FAILURE = "result_merge_failure"
    CORRECTNESS_TOO_LOW = "correctness_too_low"
    AVAILABILITY_TOO_LOW = "availability_too_low"
    CACHE_EFFECTIVENESS_TOO_LOW = "cache_effectiveness_too_low"
    RESOURCE_EFFICIENCY_TOO_LOW = "resource_efficiency_too_low"
    TIMEOUT_RATE_TOO_HIGH = "timeout_rate_too_high"
    ERROR_RATE_TOO_HIGH = "error_rate_too_high"
    DUPLICATE_RATE_TOO_HIGH = "duplicate_rate_too_high"
    LOSS_RATE_TOO_HIGH = "loss_rate_too_high"
    OVERLOAD_RATE_TOO_HIGH = "overload_rate_too_high"
    BACKPRESSURE_FAILURE = "backpressure_failure"
    FAILURE_RECOVERY_TOO_LOW = "failure_recovery_too_low"
    PARTIAL_RESPONSE_RATE_TOO_HIGH = "partial_response_rate_too_high"
    INVALID_MEASUREMENT = "invalid_measurement"
    PARTIAL_EVIDENCE = "partial_evidence"
    NON_PUBLIC_SCOPE = "non_public_scope"


class CheckpointType(str, Enum):
    PROOF_CREATED = "proof_created"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    DIMENSION_VALIDATED = "dimension_validated"
    RETRIEVAL_EVALUATED = "retrieval_evaluated"
    PROOF_FINALIZED = "proof_finalized"


class EventType(str, Enum):
    PROOF_CREATED = "proof_created"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    EVIDENCE_REJECTED = "evidence_rejected"
    DIMENSION_EVALUATED = "dimension_evaluated"
    RETRIEVAL_EVALUATED = "retrieval_evaluated"
    PROOF_PASSED = "proof_passed"
    PROOF_FAILED = "proof_failed"
    PROOF_DEFERRED = "proof_deferred"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass(frozen=True)
class RetrievalProofIdentity:
    proof_id: str
    evaluation_id: str
    created_at: str
    schema_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class RetrievalProofLineage:
    source_systems: Tuple[str, ...] = ()
    source_phases: Tuple[str, ...] = ()
    source_stages: Tuple[str, ...] = ()
    parent_evaluation_ids: Tuple[str, ...] = ()
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class QueryServingMeasurement:
    dimension: RetrievalTestDimension

    resource_count: int
    query_count: int

    query_throughput_per_second: float

    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float

    concurrency_efficiency: float

    shard_count: int
    shard_success_rate: float

    retrieval_completion_rate: float
    result_merge_success_rate: float

    correctness_rate: float
    availability_rate: float

    cache_effectiveness: float
    resource_efficiency: float

    timeout_rate: float
    error_rate: float

    duplicate_rate: float
    loss_rate: float

    overload_rate: float
    backpressure_stability: float

    failure_recovery_rate: float
    partial_response_rate: float

    worker_count: int = 0
    measurement_window_seconds: float = 0.0

    evidence_strength: EvidenceStrength = EvidenceStrength.OBSERVED
    evidence_kind: EvidenceKind = EvidenceKind.QUERY

    source: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RetrievalProofInput:
    identity: RetrievalProofIdentity
    lineage: RetrievalProofLineage

    measurements: Tuple[QueryServingMeasurement, ...]

    target_resource_count: int
    target_query_count: int

    public_web_scope: bool = True
    partial_evidence_allowed: bool = True

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RetrievalProofPolicy:
    minimum_resource_count: int = DEFAULT_MIN_RESOURCE_COUNT
    minimum_query_count: int = DEFAULT_MIN_QUERY_COUNT

    minimum_query_throughput: float = DEFAULT_MIN_QUERY_THROUGHPUT

    maximum_p50_latency_ms: float = DEFAULT_MAX_P50_LATENCY_MS
    maximum_p95_latency_ms: float = DEFAULT_MAX_P95_LATENCY_MS
    maximum_p99_latency_ms: float = DEFAULT_MAX_P99_LATENCY_MS

    minimum_concurrency_efficiency: float = (
        DEFAULT_MIN_CONCURRENCY_EFFICIENCY
    )

    minimum_shard_success_rate: float = DEFAULT_MIN_SHARD_SUCCESS
    minimum_retrieval_completion_rate: float = (
        DEFAULT_MIN_RETRIEVAL_COMPLETION
    )

    minimum_result_merge_success_rate: float = (
        DEFAULT_MIN_RESULT_MERGE_SUCCESS
    )

    minimum_correctness_rate: float = DEFAULT_MIN_CORRECTNESS
    minimum_availability_rate: float = DEFAULT_MIN_AVAILABILITY

    minimum_cache_effectiveness: float = DEFAULT_MIN_CACHE_EFFECTIVENESS
    minimum_resource_efficiency: float = DEFAULT_MIN_RESOURCE_EFFICIENCY

    maximum_timeout_rate: float = DEFAULT_MAX_TIMEOUT_RATE
    maximum_error_rate: float = DEFAULT_MAX_ERROR_RATE
    maximum_duplicate_rate: float = DEFAULT_MAX_DUPLICATE_RATE
    maximum_loss_rate: float = DEFAULT_MAX_LOSS_RATE
    maximum_overload_rate: float = DEFAULT_MAX_OVERLOAD_RATE
    maximum_partial_response_rate: float = (
        DEFAULT_MAX_PARTIAL_RESPONSE_RATE
    )

    minimum_backpressure_stability: float = (
        DEFAULT_MIN_BACKPRESSURE_STABILITY
    )

    minimum_failure_recovery_rate: float = (
        DEFAULT_MIN_FAILURE_RECOVERY
    )

    require_all_dimensions: bool = True
    allow_partial_evidence: bool = True
    require_public_web_scope: bool = True
    require_deterministic_evaluation: bool = True


@dataclass(frozen=True)
class RetrievalFinding:
    dimension: Optional[RetrievalTestDimension]
    reason: ProofReason
    message: str

    measured_value: Optional[float] = None
    required_value: Optional[float] = None

    evidence_strength: EvidenceStrength = EvidenceStrength.UNKNOWN
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class RetrievalDimensionResult:
    dimension: RetrievalTestDimension
    passed: bool

    resource_count: int
    query_count: int

    query_throughput_per_second: float

    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float

    concurrency_efficiency: float

    shard_count: int
    shard_success_rate: float

    retrieval_completion_rate: float
    result_merge_success_rate: float

    correctness_rate: float
    availability_rate: float

    cache_effectiveness: float
    resource_efficiency: float

    timeout_rate: float
    error_rate: float

    duplicate_rate: float
    loss_rate: float

    overload_rate: float
    backpressure_stability: float

    failure_recovery_rate: float
    partial_response_rate: float

    findings: Tuple[RetrievalFinding, ...] = ()


@dataclass(frozen=True)
class GlobalRetrievalResult:
    passed: bool

    dimensions_evaluated: int
    expected_dimensions: int

    aggregate_query_throughput: float

    aggregate_p50_latency_ms: float
    aggregate_p95_latency_ms: float
    aggregate_p99_latency_ms: float

    aggregate_concurrency: float
    aggregate_shard_success: float
    aggregate_completion: float
    aggregate_merge_success: float

    aggregate_correctness: float
    aggregate_availability: float
    aggregate_cache_effectiveness: float
    aggregate_resource_efficiency: float

    aggregate_timeout_rate: float
    aggregate_error_rate: float
    aggregate_duplicate_rate: float
    aggregate_loss_rate: float
    aggregate_overload_rate: float

    aggregate_backpressure_stability: float
    aggregate_failure_recovery: float
    aggregate_partial_response_rate: float

    findings: Tuple[RetrievalFinding, ...] = ()


@dataclass(frozen=True)
class RetrievalProofCheckpoint:
    checkpoint_id: str
    checkpoint_type: CheckpointType

    timestamp: str

    proof_id: str
    evaluation_id: str

    payload_digest: str
    sequence_number: int

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RetrievalProofEvent:
    event_id: str
    event_type: EventType

    timestamp: str

    proof_id: str
    evaluation_id: str

    payload_digest: str

    dimension: Optional[RetrievalTestDimension] = None

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RetrievalProofResult:
    identity: RetrievalProofIdentity
    lineage: RetrievalProofLineage

    state: RetrievalProofState
    decision: ProofDecision

    target_resource_count: int
    target_query_count: int

    evaluated_resource_count: int
    evaluated_query_count: int

    scale_band: QueryServingScaleBand

    dimension_results: Tuple[RetrievalDimensionResult, ...]
    global_result: GlobalRetrievalResult

    findings: Tuple[RetrievalFinding, ...]

    overall_score: float

    partial_evidence: bool
    deterministic: bool

    checkpoints: Tuple[RetrievalProofCheckpoint, ...] = ()
    events: Tuple[RetrievalProofEvent, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class RetrievalProofBackend(Protocol):
    def save_result(self, result: RetrievalProofResult) -> None:
        ...

    def load_result(
        self,
        evaluation_id: str,
    ) -> Optional[RetrievalProofResult]:
        ...

    def save_checkpoint(
        self,
        checkpoint: RetrievalProofCheckpoint,
    ) -> None:
        ...

    def save_event(
        self,
        event: RetrievalProofEvent,
    ) -> None:
        ...


class InMemoryRetrievalProofBackend:
    """
    Deterministic reference backend.

    Production deployments may replace this with a distributed durable
    persistence layer.
    """

    def __init__(self) -> None:
        self._results: Dict[str, RetrievalProofResult] = {}
        self._checkpoints: Dict[str, RetrievalProofCheckpoint] = {}
        self._events: Dict[str, RetrievalProofEvent] = {}

    def save_result(
        self,
        result: RetrievalProofResult,
    ) -> None:
        self._results[result.identity.evaluation_id] = result

    def load_result(
        self,
        evaluation_id: str,
    ) -> Optional[RetrievalProofResult]:
        return self._results.get(evaluation_id)

    def save_checkpoint(
        self,
        checkpoint: RetrievalProofCheckpoint,
    ) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(
        self,
        event: RetrievalProofEvent,
    ) -> None:
        self._events[event.event_id] = event


# ============================================================================
# ARCHITECTURE
# ============================================================================


class RetrievalQueryServingScaleProof:
    """
    Phase 15.4 retrieval and query-serving scale-proof control plane.

    It validates externally supplied query-serving stress measurements.

    It does not execute production search workloads.
    """

    REQUIRED_DIMENSIONS: Tuple[RetrievalTestDimension, ...] = (
        RetrievalTestDimension.RESOURCE_SCALE,
        RetrievalTestDimension.QUERY_VOLUME,
        RetrievalTestDimension.QUERY_THROUGHPUT,
        RetrievalTestDimension.P50_LATENCY,
        RetrievalTestDimension.P95_LATENCY,
        RetrievalTestDimension.P99_LATENCY,
        RetrievalTestDimension.CONCURRENCY,
        RetrievalTestDimension.SHARD_FANOUT,
        RetrievalTestDimension.SHARD_SUCCESS,
        RetrievalTestDimension.RETRIEVAL_COMPLETION,
        RetrievalTestDimension.RESULT_MERGE,
        RetrievalTestDimension.CORRECTNESS,
        RetrievalTestDimension.AVAILABILITY,
        RetrievalTestDimension.CACHE_EFFECTIVENESS,
        RetrievalTestDimension.RESOURCE_EFFICIENCY,
        RetrievalTestDimension.TIMEOUT_HANDLING,
        RetrievalTestDimension.ERROR_HANDLING,
        RetrievalTestDimension.DUPLICATE_PREVENTION,
        RetrievalTestDimension.LOSS_PREVENTION,
        RetrievalTestDimension.OVERLOAD_PROTECTION,
        RetrievalTestDimension.BACKPRESSURE,
        RetrievalTestDimension.FAILURE_RECOVERY,
        RetrievalTestDimension.PARTIAL_RESPONSE_CONTROL,
    )

    def __init__(
        self,
        backend: Optional[RetrievalProofBackend] = None,
        policy: Optional[RetrievalProofPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryRetrievalProofBackend()
        self.policy = policy or RetrievalProofPolicy()

    # ------------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------------

    def evaluate(
        self,
        proof_input: RetrievalProofInput,
    ) -> RetrievalProofResult:
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

        normalized = self._normalize_input(proof_input)

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
            result = self._evaluate_dimension(measurement)

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
            EventType.RETRIEVAL_EVALUATED,
        )

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.RETRIEVAL_EVALUATED,
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

        state = self._state_from_decision(decision)

        evaluated_resource_count = self._evaluated_resource_count(
            dimension_results
        )

        evaluated_query_count = self._evaluated_query_count(
            dimension_results
        )

        scale_reference = max(
            evaluated_resource_count,
            normalized.target_resource_count,
        )

        scale_band = self._scale_band(scale_reference)

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

        result = RetrievalProofResult(
            identity=normalized.identity,
            lineage=normalized.lineage,

            state=state,
            decision=decision,

            target_resource_count=normalized.target_resource_count,
            target_query_count=normalized.target_query_count,

            evaluated_resource_count=evaluated_resource_count,
            evaluated_query_count=evaluated_query_count,

            scale_band=scale_band,

            dimension_results=tuple(dimension_results),
            global_result=global_result,

            findings=tuple(findings),

            overall_score=overall_score,

            partial_evidence=partial_evidence,
            deterministic=self.policy.require_deterministic_evaluation,

            checkpoints=tuple(checkpoints),
            events=tuple(events),

            metadata={
                "architecture_version": ARCHITECTURE_VERSION,
                "phase": PHASE,
                "scale_target": SCALE_TARGET,
                "google_scale_capability_target":
                    GOOGLE_SCALE_CAPABILITY_TARGET,
                "google_technology_dependency":
                    GOOGLE_TECHNOLOGY_DEPENDENCY,
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
        inputs: Iterable[RetrievalProofInput],
    ) -> Tuple[RetrievalProofResult, ...]:
        return tuple(
            self.evaluate(item)
            for item in inputs
        )

    def get_result(
        self,
        evaluation_id: str,
    ) -> Optional[RetrievalProofResult]:
        return self.backend.load_result(evaluation_id)

    # ------------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------------

    def _validate_input(
        self,
        proof_input: RetrievalProofInput,
    ) -> None:
        if not proof_input.identity.proof_id.strip():
            raise ValueError("proof_id must not be empty")

        if not proof_input.identity.evaluation_id.strip():
            raise ValueError("evaluation_id must not be empty")

        if not 0 <= proof_input.target_resource_count <= MAX_RESOURCE_COUNT:
            raise ValueError(
                "invalid target_resource_count"
            )

        if not 0 <= proof_input.target_query_count <= MAX_QUERY_COUNT:
            raise ValueError(
                "invalid target_query_count"
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

            seen.add(measurement.dimension)

            self._validate_measurement(measurement)

    def _validate_measurement(
        self,
        measurement: QueryServingMeasurement,
    ) -> None:
        integer_fields = (
            ("resource_count", measurement.resource_count),
            ("query_count", measurement.query_count),
            ("shard_count", measurement.shard_count),
            ("worker_count", measurement.worker_count),
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

        if measurement.resource_count > MAX_RESOURCE_COUNT:
            raise ValueError(
                "resource_count exceeds supported bound"
            )

        if measurement.query_count > MAX_QUERY_COUNT:
            raise ValueError(
                "query_count exceeds supported bound"
            )

        if measurement.shard_count > MAX_SHARD_COUNT:
            raise ValueError(
                "shard_count exceeds supported bound"
            )

        if measurement.worker_count > MAX_WORKER_COUNT:
            raise ValueError(
                "worker_count exceeds supported bound"
            )

        numeric_fields = (
            (
                "query_throughput_per_second",
                measurement.query_throughput_per_second,
            ),
            ("p50_latency_ms", measurement.p50_latency_ms),
            ("p95_latency_ms", measurement.p95_latency_ms),
            ("p99_latency_ms", measurement.p99_latency_ms),
            (
                "concurrency_efficiency",
                measurement.concurrency_efficiency,
            ),
            (
                "shard_success_rate",
                measurement.shard_success_rate,
            ),
            (
                "retrieval_completion_rate",
                measurement.retrieval_completion_rate,
            ),
            (
                "result_merge_success_rate",
                measurement.result_merge_success_rate,
            ),
            (
                "correctness_rate",
                measurement.correctness_rate,
            ),
            (
                "availability_rate",
                measurement.availability_rate,
            ),
            (
                "cache_effectiveness",
                measurement.cache_effectiveness,
            ),
            (
                "resource_efficiency",
                measurement.resource_efficiency,
            ),
            ("timeout_rate", measurement.timeout_rate),
            ("error_rate", measurement.error_rate),
            ("duplicate_rate", measurement.duplicate_rate),
            ("loss_rate", measurement.loss_rate),
            ("overload_rate", measurement.overload_rate),
            (
                "backpressure_stability",
                measurement.backpressure_stability,
            ),
            (
                "failure_recovery_rate",
                measurement.failure_recovery_rate,
            ),
            (
                "partial_response_rate",
                measurement.partial_response_rate,
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

            if not math.isfinite(float(value)):
                raise ValueError(
                    f"{name} must be finite"
                )

        non_negative_fields = (
            (
                "query_throughput_per_second",
                measurement.query_throughput_per_second,
            ),
            ("p50_latency_ms", measurement.p50_latency_ms),
            ("p95_latency_ms", measurement.p95_latency_ms),
            ("p99_latency_ms", measurement.p99_latency_ms),
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
                "concurrency_efficiency",
                measurement.concurrency_efficiency,
            ),
            (
                "shard_success_rate",
                measurement.shard_success_rate,
            ),
            (
                "retrieval_completion_rate",
                measurement.retrieval_completion_rate,
            ),
            (
                "result_merge_success_rate",
                measurement.result_merge_success_rate,
            ),
            (
                "correctness_rate",
                measurement.correctness_rate,
            ),
            (
                "availability_rate",
                measurement.availability_rate,
            ),
            (
                "cache_effectiveness",
                measurement.cache_effectiveness,
            ),
            (
                "resource_efficiency",
                measurement.resource_efficiency,
            ),
            ("timeout_rate", measurement.timeout_rate),
            ("error_rate", measurement.error_rate),
            ("duplicate_rate", measurement.duplicate_rate),
            ("loss_rate", measurement.loss_rate),
            ("overload_rate", measurement.overload_rate),
            (
                "backpressure_stability",
                measurement.backpressure_stability,
            ),
            (
                "failure_recovery_rate",
                measurement.failure_recovery_rate,
            ),
            (
                "partial_response_rate",
                measurement.partial_response_rate,
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
        proof_input: RetrievalProofInput,
    ) -> RetrievalProofInput:
        ordered = tuple(
            sorted(
                proof_input.measurements,
                key=lambda item: item.dimension.value,
            )
        )

        return RetrievalProofInput(
            identity=proof_input.identity,
            lineage=proof_input.lineage,

            measurements=ordered,

            target_resource_count=proof_input.target_resource_count,
            target_query_count=proof_input.target_query_count,

            public_web_scope=proof_input.public_web_scope,
            partial_evidence_allowed=proof_input.partial_evidence_allowed,

            metadata=dict(proof_input.metadata),
        )

    # ------------------------------------------------------------------------
    # Dimension evaluation
    # ------------------------------------------------------------------------

    def _evaluate_dimension(
        self,
        measurement: QueryServingMeasurement,
    ) -> RetrievalDimensionResult:
        findings = []

        if (
            measurement.resource_count
            < self.policy.minimum_resource_count
        ):
            findings.append(
                RetrievalFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.INSUFFICIENT_RESOURCE_SCALE,
                    message=(
                        "Indexed resource scale is below the "
                        "configured retrieval-proof floor."
                    ),
                    measured_value=float(
                        measurement.resource_count
                    ),
                    required_value=float(
                        self.policy.minimum_resource_count
                    ),
                    evidence_strength=measurement.evidence_strength,
                )
            )

        if (
            measurement.query_count
            < self.policy.minimum_query_count
        ):
            findings.append(
                RetrievalFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.INSUFFICIENT_QUERY_VOLUME,
                    message=(
                        "Query volume is below the configured "
                        "retrieval-proof floor."
                    ),
                    measured_value=float(
                        measurement.query_count
                    ),
                    required_value=float(
                        self.policy.minimum_query_count
                    ),
                    evidence_strength=measurement.evidence_strength,
                )
            )

        self._minimum(
            findings,
            measurement,
            "query_throughput_per_second",
            measurement.query_throughput_per_second,
            self.policy.minimum_query_throughput,
            ProofReason.QUERY_THROUGHPUT_TOO_LOW,
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
            "concurrency_efficiency",
            measurement.concurrency_efficiency,
            self.policy.minimum_concurrency_efficiency,
            ProofReason.CONCURRENCY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "shard_success_rate",
            measurement.shard_success_rate,
            self.policy.minimum_shard_success_rate,
            ProofReason.SHARD_SUCCESS_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "retrieval_completion_rate",
            measurement.retrieval_completion_rate,
            self.policy.minimum_retrieval_completion_rate,
            ProofReason.RETRIEVAL_COMPLETION_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "result_merge_success_rate",
            measurement.result_merge_success_rate,
            self.policy.minimum_result_merge_success_rate,
            ProofReason.RESULT_MERGE_FAILURE,
        )

        self._minimum(
            findings,
            measurement,
            "correctness_rate",
            measurement.correctness_rate,
            self.policy.minimum_correctness_rate,
            ProofReason.CORRECTNESS_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "availability_rate",
            measurement.availability_rate,
            self.policy.minimum_availability_rate,
            ProofReason.AVAILABILITY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "cache_effectiveness",
            measurement.cache_effectiveness,
            self.policy.minimum_cache_effectiveness,
            ProofReason.CACHE_EFFECTIVENESS_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "resource_efficiency",
            measurement.resource_efficiency,
            self.policy.minimum_resource_efficiency,
            ProofReason.RESOURCE_EFFICIENCY_TOO_LOW,
        )

        self._maximum(
            findings,
            measurement,
            "timeout_rate",
            measurement.timeout_rate,
            self.policy.maximum_timeout_rate,
            ProofReason.TIMEOUT_RATE_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "error_rate",
            measurement.error_rate,
            self.policy.maximum_error_rate,
            ProofReason.ERROR_RATE_TOO_HIGH,
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
            "loss_rate",
            measurement.loss_rate,
            self.policy.maximum_loss_rate,
            ProofReason.LOSS_RATE_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "overload_rate",
            measurement.overload_rate,
            self.policy.maximum_overload_rate,
            ProofReason.OVERLOAD_RATE_TOO_HIGH,
        )

        self._minimum(
            findings,
            measurement,
            "backpressure_stability",
            measurement.backpressure_stability,
            self.policy.minimum_backpressure_stability,
            ProofReason.BACKPRESSURE_FAILURE,
        )

        self._minimum(
            findings,
            measurement,
            "failure_recovery_rate",
            measurement.failure_recovery_rate,
            self.policy.minimum_failure_recovery_rate,
            ProofReason.FAILURE_RECOVERY_TOO_LOW,
        )

        self._maximum(
            findings,
            measurement,
            "partial_response_rate",
            measurement.partial_response_rate,
            self.policy.maximum_partial_response_rate,
            ProofReason.PARTIAL_RESPONSE_RATE_TOO_HIGH,
        )

        if (
            measurement.evidence_strength
            == EvidenceStrength.UNKNOWN
        ):
            findings.append(
                RetrievalFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.MISSING_EVIDENCE,
                    message="Evidence strength is unknown.",
                    evidence_strength=measurement.evidence_strength,
                )
            )

        if (
            measurement.evidence_strength
            == EvidenceStrength.PARTIAL
            and not self.policy.allow_partial_evidence
        ):
            findings.append(
                RetrievalFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.PARTIAL_EVIDENCE,
                    message="Partial evidence is not allowed.",
                    evidence_strength=measurement.evidence_strength,
                )
            )

        return RetrievalDimensionResult(
            dimension=measurement.dimension,
            passed=len(findings) == 0,

            resource_count=measurement.resource_count,
            query_count=measurement.query_count,

            query_throughput_per_second=(
                measurement.query_throughput_per_second
            ),

            p50_latency_ms=measurement.p50_latency_ms,
            p95_latency_ms=measurement.p95_latency_ms,
            p99_latency_ms=measurement.p99_latency_ms,

            concurrency_efficiency=measurement.concurrency_efficiency,

            shard_count=measurement.shard_count,
            shard_success_rate=measurement.shard_success_rate,

            retrieval_completion_rate=(
                measurement.retrieval_completion_rate
            ),

            result_merge_success_rate=(
                measurement.result_merge_success_rate
            ),

            correctness_rate=measurement.correctness_rate,
            availability_rate=measurement.availability_rate,

            cache_effectiveness=measurement.cache_effectiveness,
            resource_efficiency=measurement.resource_efficiency,

            timeout_rate=measurement.timeout_rate,
            error_rate=measurement.error_rate,

            duplicate_rate=measurement.duplicate_rate,
            loss_rate=measurement.loss_rate,

            overload_rate=measurement.overload_rate,
            backpressure_stability=(
                measurement.backpressure_stability
            ),

            failure_recovery_rate=(
                measurement.failure_recovery_rate
            ),

            partial_response_rate=(
                measurement.partial_response_rate
            ),

            findings=tuple(findings),
        )

    def _minimum(
        self,
        findings: list[RetrievalFinding],
        measurement: QueryServingMeasurement,
        name: str,
        measured: float,
        required: float,
        reason: ProofReason,
    ) -> None:
        if measured < required:
            findings.append(
                RetrievalFinding(
                    dimension=measurement.dimension,
                    reason=reason,
                    message=(
                        f"{name} is below the configured minimum."
                    ),
                    measured_value=measured,
                    required_value=required,
                    evidence_strength=measurement.evidence_strength,
                )
            )

    def _maximum(
        self,
        findings: list[RetrievalFinding],
        measurement: QueryServingMeasurement,
        name: str,
        measured: float,
        maximum: float,
        reason: ProofReason,
    ) -> None:
        if measured > maximum:
            findings.append(
                RetrievalFinding(
                    dimension=measurement.dimension,
                    reason=reason,
                    message=(
                        f"{name} exceeds the configured maximum."
                    ),
                    measured_value=measured,
                    required_value=maximum,
                    evidence_strength=measurement.evidence_strength,
                )
            )

    # ------------------------------------------------------------------------
    # Global evaluation
    # ------------------------------------------------------------------------

    def _evaluate_global(
        self,
        proof_input: RetrievalProofInput,
        dimension_results: Sequence[RetrievalDimensionResult],
    ) -> GlobalRetrievalResult:
        findings = []

        present = {
            result.dimension
            for result in dimension_results
        }

        missing = (
            set(self.REQUIRED_DIMENSIONS) - present
        )

        if missing and self.policy.require_all_dimensions:
            for dimension in sorted(
                missing,
                key=lambda item: item.value,
            ):
                findings.append(
                    RetrievalFinding(
                        dimension=dimension,
                        reason=ProofReason.MISSING_EVIDENCE,
                        message=(
                            f"Required dimension '{dimension.value}' "
                            "has no measurement."
                        ),
                    )
                )

        if not dimension_results:
            findings.append(
                RetrievalFinding(
                    dimension=None,
                    reason=ProofReason.MISSING_EVIDENCE,
                    message=(
                        "No retrieval/query-serving measurements "
                        "were supplied."
                    ),
                )
            )

            return GlobalRetrievalResult(
                passed=False,

                dimensions_evaluated=0,
                expected_dimensions=len(self.REQUIRED_DIMENSIONS),

                aggregate_query_throughput=0.0,

                aggregate_p50_latency_ms=0.0,
                aggregate_p95_latency_ms=0.0,
                aggregate_p99_latency_ms=0.0,

                aggregate_concurrency=0.0,
                aggregate_shard_success=0.0,
                aggregate_completion=0.0,
                aggregate_merge_success=0.0,

                aggregate_correctness=0.0,
                aggregate_availability=0.0,
                aggregate_cache_effectiveness=0.0,
                aggregate_resource_efficiency=0.0,

                aggregate_timeout_rate=1.0,
                aggregate_error_rate=1.0,
                aggregate_duplicate_rate=1.0,
                aggregate_loss_rate=1.0,
                aggregate_overload_rate=1.0,

                aggregate_backpressure_stability=0.0,
                aggregate_failure_recovery=0.0,
                aggregate_partial_response_rate=1.0,

                findings=tuple(findings),
            )

        aggregate = {
            "query_throughput": self._mean(
                result.query_throughput_per_second
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

            "concurrency": self._mean(
                result.concurrency_efficiency
                for result in dimension_results
            ),

            "shard_success": self._mean(
                result.shard_success_rate
                for result in dimension_results
            ),

            "completion": self._mean(
                result.retrieval_completion_rate
                for result in dimension_results
            ),

            "merge_success": self._mean(
                result.result_merge_success_rate
                for result in dimension_results
            ),

            "correctness": self._mean(
                result.correctness_rate
                for result in dimension_results
            ),

            "availability": self._mean(
                result.availability_rate
                for result in dimension_results
            ),

            "cache_effectiveness": self._mean(
                result.cache_effectiveness
                for result in dimension_results
            ),

            "resource_efficiency": self._mean(
                result.resource_efficiency
                for result in dimension_results
            ),

            "timeout_rate": self._mean(
                result.timeout_rate
                for result in dimension_results
            ),

            "error_rate": self._mean(
                result.error_rate
                for result in dimension_results
            ),

            "duplicate_rate": self._mean(
                result.duplicate_rate
                for result in dimension_results
            ),

            "loss_rate": self._mean(
                result.loss_rate
                for result in dimension_results
            ),

            "overload_rate": self._mean(
                result.overload_rate
                for result in dimension_results
            ),

            "backpressure": self._mean(
                result.backpressure_stability
                for result in dimension_results
            ),

            "recovery": self._mean(
                result.failure_recovery_rate
                for result in dimension_results
            ),

            "partial_response": self._mean(
                result.partial_response_rate
                for result in dimension_results
            ),
        }

        return GlobalRetrievalResult(
            passed=len(findings) == 0,

            dimensions_evaluated=len(dimension_results),
            expected_dimensions=len(self.REQUIRED_DIMENSIONS),

            aggregate_query_throughput=(
                aggregate["query_throughput"]
            ),

            aggregate_p50_latency_ms=aggregate["p50"],
            aggregate_p95_latency_ms=aggregate["p95"],
            aggregate_p99_latency_ms=aggregate["p99"],

            aggregate_concurrency=aggregate["concurrency"],
            aggregate_shard_success=aggregate["shard_success"],
            aggregate_completion=aggregate["completion"],
            aggregate_merge_success=aggregate["merge_success"],

            aggregate_correctness=aggregate["correctness"],
            aggregate_availability=aggregate["availability"],
            aggregate_cache_effectiveness=(
                aggregate["cache_effectiveness"]
            ),
            aggregate_resource_efficiency=(
                aggregate["resource_efficiency"]
            ),

            aggregate_timeout_rate=aggregate["timeout_rate"],
            aggregate_error_rate=aggregate["error_rate"],
            aggregate_duplicate_rate=aggregate["duplicate_rate"],
            aggregate_loss_rate=aggregate["loss_rate"],
            aggregate_overload_rate=aggregate["overload_rate"],

            aggregate_backpressure_stability=(
                aggregate["backpressure"]
            ),

            aggregate_failure_recovery=aggregate["recovery"],

            aggregate_partial_response_rate=(
                aggregate["partial_response"]
            ),

            findings=tuple(findings),
        )

    # ------------------------------------------------------------------------
    # Findings
    # ------------------------------------------------------------------------

    def _collect_findings(
        self,
        proof_input: RetrievalProofInput,
        dimension_results: Sequence[RetrievalDimensionResult],
        global_result: GlobalRetrievalResult,
    ) -> list[RetrievalFinding]:
        findings = []

        for result in dimension_results:
            findings.extend(result.findings)

        findings.extend(global_result.findings)

        if (
            self.policy.require_public_web_scope
            and not proof_input.public_web_scope
        ):
            findings.append(
                RetrievalFinding(
                    dimension=None,
                    reason=ProofReason.NON_PUBLIC_SCOPE,
                    message=(
                        "The retrieval evidence is not explicitly "
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
        proof_input: RetrievalProofInput,
        dimension_results: Sequence[RetrievalDimensionResult],
        global_result: GlobalRetrievalResult,
        findings: Sequence[RetrievalFinding],
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

            if not set(self.REQUIRED_DIMENSIONS).issubset(
                present
            ):
                return ProofDecision.DEFER

        return ProofDecision.PASS

    @staticmethod
    def _state_from_decision(
        decision: ProofDecision,
    ) -> RetrievalProofState:
        if decision == ProofDecision.PASS:
            return RetrievalProofState.PASSED

        if decision == ProofDecision.FAIL:
            return RetrievalProofState.FAILED

        return RetrievalProofState.DEFERRED

    # ------------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------------

    def _overall_score(
        self,
        dimension_results: Sequence[RetrievalDimensionResult],
        global_result: GlobalRetrievalResult,
        findings: Sequence[RetrievalFinding],
    ) -> float:
        if not dimension_results:
            return 0.0

        dimension_scores = [
            self._dimension_score(result)
            for result in dimension_results
        ]

        base = self._mean(dimension_scores)

        latency_quality = self._latency_quality(
            global_result
        )

        reliability_quality = self._mean(
            (
                global_result.aggregate_correctness,
                global_result.aggregate_availability,
                global_result.aggregate_completion,
                global_result.aggregate_merge_success,
                global_result.aggregate_shard_success,
                global_result.aggregate_concurrency,
                global_result.aggregate_cache_effectiveness,
                global_result.aggregate_resource_efficiency,
                max(
                    0.0,
                    1.0 - global_result.aggregate_timeout_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_error_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_duplicate_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_loss_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_overload_rate,
                ),
                global_result.aggregate_backpressure_stability,
                global_result.aggregate_failure_recovery,
                max(
                    0.0,
                    1.0
                    - global_result.aggregate_partial_response_rate,
                ),
            )
        )

        score = (
            (base * 0.50)
            + (latency_quality * 0.20)
            + (reliability_quality * 0.30)
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
        result: RetrievalDimensionResult,
    ) -> float:
        latency_quality = self._single_latency_quality(
            result
        )

        values = (
            result.concurrency_efficiency,
            result.shard_success_rate,
            result.retrieval_completion_rate,
            result.result_merge_success_rate,
            result.correctness_rate,
            result.availability_rate,
            result.cache_effectiveness,
            result.resource_efficiency,

            max(
                0.0,
                1.0 - result.timeout_rate,
            ),

            max(
                0.0,
                1.0 - result.error_rate,
            ),

            max(
                0.0,
                1.0 - result.duplicate_rate,
            ),

            max(
                0.0,
                1.0 - result.loss_rate,
            ),

            max(
                0.0,
                1.0 - result.overload_rate,
            ),

            result.backpressure_stability,
            result.failure_recovery_rate,

            max(
                0.0,
                1.0 - result.partial_response_rate,
            ),

            latency_quality,
        )

        return self._mean(values)

    def _latency_quality(
        self,
        result: GlobalRetrievalResult,
    ) -> float:
        p50 = self._bounded_latency_quality(
            result.aggregate_p50_latency_ms,
            self.policy.maximum_p50_latency_ms,
        )

        p95 = self._bounded_latency_quality(
            result.aggregate_p95_latency_ms,
            self.policy.maximum_p95_latency_ms,
        )

        p99 = self._bounded_latency_quality(
            result.aggregate_p99_latency_ms,
            self.policy.maximum_p99_latency_ms,
        )

        return self._mean(
            (
                p50,
                p95,
                p99,
            )
        )

    def _single_latency_quality(
        self,
        result: RetrievalDimensionResult,
    ) -> float:
        p50 = self._bounded_latency_quality(
            result.p50_latency_ms,
            self.policy.maximum_p50_latency_ms,
        )

        p95 = self._bounded_latency_quality(
            result.p95_latency_ms,
            self.policy.maximum_p95_latency_ms,
        )

        p99 = self._bounded_latency_quality(
            result.p99_latency_ms,
            self.policy.maximum_p99_latency_ms,
        )

        return self._mean(
            (
                p50,
                p95,
                p99,
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
        values_tuple = tuple(values)

        if not values_tuple:
            return 0.0

        return sum(values_tuple) / len(values_tuple)

    # ------------------------------------------------------------------------
    # Evaluated volume
    # ------------------------------------------------------------------------

    @staticmethod
    def _evaluated_resource_count(
        results: Sequence[RetrievalDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.resource_count
            for result in results
        )

    @staticmethod
    def _evaluated_query_count(
        results: Sequence[RetrievalDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.query_count
            for result in results
        )

    # ------------------------------------------------------------------------
    # Scale classification
    # ------------------------------------------------------------------------

    @staticmethod
    def _scale_band(
        resource_count: int,
    ) -> QueryServingScaleBand:
        if resource_count >= 10**12:
            return QueryServingScaleBand.TRILLIONS

        if resource_count >= 10**9:
            return QueryServingScaleBand.BILLIONS

        if resource_count >= 10**8:
            return QueryServingScaleBand.MASSIVE

        if resource_count >= 10**6:
            return QueryServingScaleBand.LARGE

        if resource_count > 0:
            return QueryServingScaleBand.SMALL

        return QueryServingScaleBand.UNKNOWN

    # ------------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------------

    def _checkpoint(
        self,
        checkpoints: list[RetrievalProofCheckpoint],
        proof_input: RetrievalProofInput,
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

        checkpoint = RetrievalProofCheckpoint(
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

        checkpoints.append(checkpoint)

        self.backend.save_checkpoint(checkpoint)

    # ------------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------------

    def _emit_event(
        self,
        events: list[RetrievalProofEvent],
        proof_input: RetrievalProofInput,
        event_type: EventType,
        dimension: Optional[RetrievalTestDimension] = None,
    ) -> None:
        event_id = self._stable_id(
            "event",
            proof_input.identity.evaluation_id,
            event_type.value,
            dimension.value if dimension else "",
        )

        event = RetrievalProofEvent(
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

        events.append(event)

        self.backend.save_event(event)

    # ------------------------------------------------------------------------
    # Canonical hashing
    # ------------------------------------------------------------------------

    @staticmethod
    def _digest(value: Any) -> str:
        normalized = (
            RetrievalQueryServingScaleProof._canonicalize(
                value
            )
        )

        payload = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")

        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _canonicalize(value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, Enum):
            return value.value

        if hasattr(value, "__dataclass_fields__"):
            return {
                key: RetrievalQueryServingScaleProof._canonicalize(
                    getattr(value, key)
                )
                for key in value.__dataclass_fields__
            }

        if isinstance(value, Mapping):
            return {
                str(key): RetrievalQueryServingScaleProof._canonicalize(
                    item
                )
                for key, item in sorted(
                    value.items(),
                    key=lambda pair: str(pair[0]),
                )
            }

        if isinstance(value, (list, tuple)):
            return [
                RetrievalQueryServingScaleProof._canonicalize(
                    item
                )
                for item in value
            ]

        if isinstance(value, (set, frozenset)):
            items = [
                RetrievalQueryServingScaleProof._canonicalize(
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

        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(
                    "non-finite float cannot be canonicalized"
                )

            return value

        return value

    @staticmethod
    def _stable_id(*parts: str) -> str:
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

GlobalRetrievalQueryServingScaleProof = (
    RetrievalQueryServingScaleProof
)

Phase15_4RetrievalQueryServingScaleProof = (
    RetrievalQueryServingScaleProof
)

RetrievalScaleProof = RetrievalQueryServingScaleProof

QueryServingScaleProof = RetrievalQueryServingScaleProof

MassiveRetrievalScaleProof = RetrievalQueryServingScaleProof


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

    "RetrievalProofState",
    "RetrievalTestDimension",
    "EvidenceStrength",
    "EvidenceKind",
    "QueryServingScaleBand",
    "ProofDecision",
    "ProofReason",
    "CheckpointType",
    "EventType",

    "RetrievalProofIdentity",
    "RetrievalProofLineage",
    "QueryServingMeasurement",
    "RetrievalProofInput",
    "RetrievalProofPolicy",
    "RetrievalFinding",
    "RetrievalDimensionResult",
    "GlobalRetrievalResult",
    "RetrievalProofCheckpoint",
    "RetrievalProofEvent",
    "RetrievalProofResult",

    "RetrievalProofBackend",
    "InMemoryRetrievalProofBackend",

    "RetrievalQueryServingScaleProof",
    "GlobalRetrievalQueryServingScaleProof",
    "Phase15_4RetrievalQueryServingScaleProof",
    "RetrievalScaleProof",
    "QueryServingScaleProof",
    "MassiveRetrievalScaleProof",
]
