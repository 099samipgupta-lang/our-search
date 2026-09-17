"""
OUR SEARCH
Phase 15.6 — Freshness & Recrawling Scale Proof

Purpose
-------
Scale-proof architecture for validating the freshness and recrawling
subsystem under enormous public-Web workloads.

Target
------
Billions to trillions of publicly accessible Web resources.

This module is a deterministic control-plane / proof layer.

It does NOT:
- crawl the Web
- perform HTTP requests
- fetch URLs
- assign production workers
- mutate the production index
- execute recrawl jobs
- access Google Search
- use Google's index, crawler, ranking, or infrastructure
- execute malware or offensive security operations

Instead, it consumes externally supplied measurements/evidence from
freshness and recrawling infrastructure and determines whether the
supplied evidence satisfies the configured scale-proof policy.

The proof covers:
- resource scale
- freshness coverage
- change detection
- recrawl scheduling
- adaptive recrawl frequency
- queue scale
- distributed orchestration
- resource allocation
- global coordination
- failure recovery
- recrawl completion
- freshness SLA compliance
- duplicate prevention
- lost-work prevention
- scheduling correctness
- priority correctness
- overload resilience
- latency
- resource efficiency
- regional consistency
- partition stability
- recovery correctness
- freshness regression control

A PASS means the supplied evidence satisfies this proof policy.
It does NOT mean that OUR SEARCH has already crawled the entire
real-world Web or achieved Google's live coverage/quality.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
import uuid
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


# ============================================================================
# GLOBAL ARCHITECTURE CONSTANTS
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "freshness-recrawling-scale-proof.v1"

PHASE = "15.6"
PREVIOUS_STAGE = "15.5"
NEXT_STAGE = "15.7"

PHASE_NAME = "Full Scale-Proof Program"
STAGE_NAME = "Freshness & Recrawling Scale Proof"
NEXT_STAGE_NAME = "Spam / Abuse / Security Scale Proof"


# ============================================================================
# HARD SAFETY / SCALE BOUNDS
# ============================================================================

MAX_RESOURCE_COUNT = 10**18
MAX_CHANGED_RESOURCE_COUNT = 10**18
MAX_RECrawl_COUNT = 10**18
MAX_QUEUE_COUNT = 10**18
MAX_WORKER_COUNT = 10**12
MAX_SHARD_COUNT = 10**12
MAX_REGION_COUNT = 10**6
MAX_PARTITION_COUNT = 10**12
MAX_MEASUREMENT_WINDOW_SECONDS = 10**9


# ============================================================================
# DEFAULT POLICY THRESHOLDS
# ============================================================================

DEFAULT_MIN_RESOURCE_COUNT = 1_000_000
DEFAULT_MIN_CHANGED_RESOURCE_COUNT = 100_000
DEFAULT_MIN_RECRAWL_COUNT = 100_000
DEFAULT_MIN_QUEUE_COUNT = 100_000

DEFAULT_MIN_FRESHNESS_COVERAGE = 0.95
DEFAULT_MIN_CHANGE_DETECTION_ACCURACY = 0.95
DEFAULT_MIN_RECRAWL_SCHEDULING_CORRECTNESS = 0.95
DEFAULT_MIN_ADAPTIVE_FREQUENCY_QUALITY = 0.90
DEFAULT_MIN_RECRAWL_COMPLETION_RATE = 0.99
DEFAULT_MIN_FRESHNESS_SLA_COMPLIANCE = 0.95

DEFAULT_MIN_DISTRIBUTED_ORCHESTRATION = 0.95
DEFAULT_MIN_RESOURCE_ALLOCATION = 0.90
DEFAULT_MIN_GLOBAL_COORDINATION = 0.95
DEFAULT_MIN_FAILURE_RECOVERY = 0.95
DEFAULT_MIN_REGIONAL_CONSISTENCY = 0.95
DEFAULT_MIN_PARTITION_STABILITY = 0.95

DEFAULT_MIN_PRIORITY_CORRECTNESS = 0.95
DEFAULT_MIN_SCHEDULE_STABILITY = 0.95
DEFAULT_MIN_RESOURCE_EFFICIENCY = 0.80
DEFAULT_MIN_OVERLOAD_RESILIENCE = 0.90

DEFAULT_MAX_DUPLICATE_RATE = 0.01
DEFAULT_MAX_LOST_WORK_RATE = 0.005
DEFAULT_MAX_SCHEDULING_ERROR_RATE = 0.02
DEFAULT_MAX_FRESHNESS_REGRESSION_RATE = 0.05
DEFAULT_MAX_TIMEOUT_RATE = 0.02

DEFAULT_MAX_P50_LATENCY_MS = 250.0
DEFAULT_MAX_P95_LATENCY_MS = 750.0
DEFAULT_MAX_P99_LATENCY_MS = 1500.0

DEFAULT_MIN_QUEUE_DRAIN_RATE = 0.95
DEFAULT_MIN_RECOVERY_COMPLETION_RATE = 0.95


# ============================================================================
# ENUMERATIONS
# ============================================================================


class FreshnessProofState(str, Enum):
    CREATED = "created"
    COLLECTING = "collecting"
    NORMALIZED = "normalized"
    EVALUATED = "evaluated"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class FreshnessTestDimension(str, Enum):
    RESOURCE_SCALE = "resource_scale"
    CHANGED_RESOURCE_SCALE = "changed_resource_scale"
    RECRAWL_VOLUME = "recrawl_volume"
    QUEUE_SCALE = "queue_scale"

    FRESHNESS_COVERAGE = "freshness_coverage"
    CHANGE_DETECTION = "change_detection"
    RECRAWL_SCHEDULING = "recrawl_scheduling"
    ADAPTIVE_FREQUENCY = "adaptive_frequency"
    RECRAWL_COMPLETION = "recrawl_completion"
    FRESHNESS_SLA = "freshness_sla"

    DISTRIBUTED_ORCHESTRATION = "distributed_orchestration"
    RESOURCE_ALLOCATION = "resource_allocation"
    GLOBAL_COORDINATION = "global_coordination"
    FAILURE_RECOVERY = "failure_recovery"

    REGIONAL_CONSISTENCY = "regional_consistency"
    PARTITION_STABILITY = "partition_stability"

    PRIORITY_CORRECTNESS = "priority_correctness"
    SCHEDULE_STABILITY = "schedule_stability"

    DUPLICATE_PREVENTION = "duplicate_prevention"
    LOST_WORK_PREVENTION = "lost_work_prevention"
    SCHEDULING_ERROR_CONTROL = "scheduling_error_control"
    FRESHNESS_REGRESSION_CONTROL = "freshness_regression_control"

    P50_LATENCY = "p50_latency"
    P95_LATENCY = "p95_latency"
    P99_LATENCY = "p99_latency"

    QUEUE_DRAIN = "queue_drain"
    RECOVERY_COMPLETION = "recovery_completion"

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
    CHANGE = "change"
    RECRAWL = "recrawl"
    QUEUE = "queue"
    FRESHNESS = "freshness"
    SCHEDULING = "scheduling"
    ADAPTIVE = "adaptive"
    ORCHESTRATION = "orchestration"
    ALLOCATION = "allocation"
    COORDINATION = "coordination"
    RECOVERY = "recovery"
    REGIONAL = "regional"
    PARTITION = "partition"
    PRIORITY = "priority"
    DUPLICATE = "duplicate"
    LOSS = "loss"
    REGRESSION = "regression"
    LATENCY = "latency"
    RESOURCE_USAGE = "resource_usage"
    OVERLOAD = "overload"


class FreshnessScaleBand(str, Enum):
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
    PASS_POLICY = "pass_policy"

    NO_DIMENSIONS = "no_dimensions"
    PARTIAL_EVIDENCE_NOT_ALLOWED = "partial_evidence_not_allowed"
    SIMULATED_EVIDENCE_NOT_ALLOWED = "simulated_evidence_not_allowed"
    UNKNOWN_EVIDENCE = "unknown_evidence"

    INSUFFICIENT_RESOURCE_VOLUME = "insufficient_resource_volume"
    INSUFFICIENT_CHANGED_RESOURCE_VOLUME = "insufficient_changed_resource_volume"
    INSUFFICIENT_RECRAWL_VOLUME = "insufficient_recrawl_volume"
    INSUFFICIENT_QUEUE_VOLUME = "insufficient_queue_volume"

    LOW_FRESHNESS_COVERAGE = "low_freshness_coverage"
    LOW_CHANGE_DETECTION = "low_change_detection"
    LOW_SCHEDULING_CORRECTNESS = "low_scheduling_correctness"
    LOW_ADAPTIVE_FREQUENCY_QUALITY = "low_adaptive_frequency_quality"
    LOW_RECRAWL_COMPLETION = "low_recrawl_completion"
    LOW_FRESHNESS_SLA = "low_freshness_sla"

    LOW_DISTRIBUTED_ORCHESTRATION = "low_distributed_orchestration"
    LOW_RESOURCE_ALLOCATION = "low_resource_allocation"
    LOW_GLOBAL_COORDINATION = "low_global_coordination"
    LOW_FAILURE_RECOVERY = "low_failure_recovery"

    LOW_REGIONAL_CONSISTENCY = "low_regional_consistency"
    LOW_PARTITION_STABILITY = "low_partition_stability"

    LOW_PRIORITY_CORRECTNESS = "low_priority_correctness"
    LOW_SCHEDULE_STABILITY = "low_schedule_stability"

    HIGH_DUPLICATE_RATE = "high_duplicate_rate"
    HIGH_LOST_WORK_RATE = "high_lost_work_rate"
    HIGH_SCHEDULING_ERROR_RATE = "high_scheduling_error_rate"
    HIGH_FRESHNESS_REGRESSION_RATE = "high_freshness_regression_rate"

    HIGH_TIMEOUT_RATE = "high_timeout_rate"

    HIGH_P50_LATENCY = "high_p50_latency"
    HIGH_P95_LATENCY = "high_p95_latency"
    HIGH_P99_LATENCY = "high_p99_latency"

    LOW_QUEUE_DRAIN = "low_queue_drain"
    LOW_RECOVERY_COMPLETION = "low_recovery_completion"

    LOW_RESOURCE_EFFICIENCY = "low_resource_efficiency"
    LOW_OVERLOAD_RESILIENCE = "low_overload_resilience"

    INVALID_INPUT = "invalid_input"
    INVALID_METRIC = "invalid_metric"
    DUPLICATE_DIMENSION = "duplicate_dimension"
    PUBLIC_SCOPE_REQUIRED = "public_scope_required"


class CheckpointType(str, Enum):
    PROOF_CREATED = "proof_created"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    DIMENSION_VALIDATED = "dimension_validated"
    FRESHNESS_EVALUATED = "freshness_evaluated"
    PROOF_FINALIZED = "proof_finalized"


class EventType(str, Enum):
    PROOF_CREATED = "proof_created"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    EVIDENCE_REJECTED = "evidence_rejected"
    DIMENSION_EVALUATED = "dimension_evaluated"
    FRESHNESS_EVALUATED = "freshness_evaluated"
    PROOF_PASSED = "proof_passed"
    PROOF_FAILED = "proof_failed"
    PROOF_DEFERRED = "proof_deferred"


# ============================================================================
# DATACLASSES
# ============================================================================


@dataclass(frozen=True)
class FreshnessProofIdentity:
    proof_id: str
    architecture_version: str
    phase: str
    created_at: str
    target: str = SCALE_TARGET


@dataclass(frozen=True)
class FreshnessProofLineage:
    previous_stage: str
    next_stage: str
    phase_name: str
    source_components: Tuple[str, ...]
    parent_proof_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class FreshnessRecrawlingMeasurement:
    """
    Externally supplied measurement/evidence.

    Every measurement is associated with one proof dimension.
    """

    dimension: FreshnessTestDimension

    resource_count: int = 0
    changed_resource_count: int = 0
    recrawl_count: int = 0
    queue_count: int = 0

    freshness_coverage: float = 0.0
    change_detection_accuracy: float = 0.0
    recrawl_scheduling_correctness: float = 0.0
    adaptive_frequency_quality: float = 0.0
    recrawl_completion_rate: float = 0.0
    freshness_sla_compliance: float = 0.0

    distributed_orchestration: float = 0.0
    resource_allocation: float = 0.0
    global_coordination: float = 0.0
    failure_recovery_rate: float = 0.0

    regional_consistency: float = 0.0
    partition_stability: float = 0.0

    priority_correctness: float = 0.0
    schedule_stability: float = 0.0

    duplicate_rate: float = 0.0
    lost_work_rate: float = 0.0
    scheduling_error_rate: float = 0.0
    freshness_regression_rate: float = 0.0
    timeout_rate: float = 0.0

    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0

    queue_drain_rate: float = 0.0
    recovery_completion_rate: float = 0.0

    resource_efficiency: float = 0.0
    overload_resilience: float = 0.0

    worker_count: int = 0
    shard_count: int = 0
    region_count: int = 0
    partition_count: int = 0

    measurement_window_seconds: float = 0.0

    evidence_strength: EvidenceStrength = EvidenceStrength.UNKNOWN
    evidence_kind: EvidenceKind = EvidenceKind.FRESHNESS

    source: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FreshnessProofInput:
    measurements: Tuple[FreshnessRecrawlingMeasurement, ...]

    public_web_scope: bool = True

    evidence_strength: EvidenceStrength = EvidenceStrength.OBSERVED

    allow_partial_evidence: bool = False
    allow_simulated_evidence: bool = False
    require_all_dimensions: bool = True

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FreshnessProofPolicy:
    min_resource_count: int = DEFAULT_MIN_RESOURCE_COUNT
    min_changed_resource_count: int = DEFAULT_MIN_CHANGED_RESOURCE_COUNT
    min_recrawl_count: int = DEFAULT_MIN_RECRAWL_COUNT
    min_queue_count: int = DEFAULT_MIN_QUEUE_COUNT

    min_freshness_coverage: float = DEFAULT_MIN_FRESHNESS_COVERAGE
    min_change_detection_accuracy: float = DEFAULT_MIN_CHANGE_DETECTION_ACCURACY
    min_recrawl_scheduling_correctness: float = DEFAULT_MIN_RECRAWL_SCHEDULING_CORRECTNESS
    min_adaptive_frequency_quality: float = DEFAULT_MIN_ADAPTIVE_FREQUENCY_QUALITY
    min_recrawl_completion_rate: float = DEFAULT_MIN_RECRAWL_COMPLETION_RATE
    min_freshness_sla_compliance: float = DEFAULT_MIN_FRESHNESS_SLA_COMPLIANCE

    min_distributed_orchestration: float = DEFAULT_MIN_DISTRIBUTED_ORCHESTRATION
    min_resource_allocation: float = DEFAULT_MIN_RESOURCE_ALLOCATION
    min_global_coordination: float = DEFAULT_MIN_GLOBAL_COORDINATION
    min_failure_recovery_rate: float = DEFAULT_MIN_FAILURE_RECOVERY

    min_regional_consistency: float = DEFAULT_MIN_REGIONAL_CONSISTENCY
    min_partition_stability: float = DEFAULT_MIN_PARTITION_STABILITY

    min_priority_correctness: float = DEFAULT_MIN_PRIORITY_CORRECTNESS
    min_schedule_stability: float = DEFAULT_MIN_SCHEDULE_STABILITY

    max_duplicate_rate: float = DEFAULT_MAX_DUPLICATE_RATE
    max_lost_work_rate: float = DEFAULT_MAX_LOST_WORK_RATE
    max_scheduling_error_rate: float = DEFAULT_MAX_SCHEDULING_ERROR_RATE
    max_freshness_regression_rate: float = DEFAULT_MAX_FRESHNESS_REGRESSION_RATE
    max_timeout_rate: float = DEFAULT_MAX_TIMEOUT_RATE

    max_p50_latency_ms: float = DEFAULT_MAX_P50_LATENCY_MS
    max_p95_latency_ms: float = DEFAULT_MAX_P95_LATENCY_MS
    max_p99_latency_ms: float = DEFAULT_MAX_P99_LATENCY_MS

    min_queue_drain_rate: float = DEFAULT_MIN_QUEUE_DRAIN_RATE
    min_recovery_completion_rate: float = DEFAULT_MIN_RECOVERY_COMPLETION_RATE

    min_resource_efficiency: float = DEFAULT_MIN_RESOURCE_EFFICIENCY
    min_overload_resilience: float = DEFAULT_MIN_OVERLOAD_RESILIENCE

    require_public_web_scope: bool = True


@dataclass(frozen=True)
class FreshnessFinding:
    dimension: FreshnessTestDimension
    reason: ProofReason
    message: str
    severity: str = "error"
    measurement_index: Optional[int] = None
    observed_value: Optional[float] = None
    required_value: Optional[float] = None


@dataclass(frozen=True)
class FreshnessDimensionResult:
    dimension: FreshnessTestDimension
    passed: bool
    score: float
    findings: Tuple[FreshnessFinding, ...] = ()
    measurement_count: int = 0


@dataclass(frozen=True)
class GlobalFreshnessResult:
    decision: ProofDecision
    score: float
    scale_band: FreshnessScaleBand
    evaluated_resource_count: int
    evaluated_changed_resource_count: int
    evaluated_recrawl_count: int
    evaluated_queue_count: int
    dimensions_evaluated: int
    dimensions_required: int
    passed_dimensions: int
    failed_dimensions: int
    deferred_dimensions: int


@dataclass(frozen=True)
class FreshnessProofCheckpoint:
    checkpoint_id: str
    proof_id: str
    checkpoint_type: CheckpointType
    state: FreshnessProofState
    created_at: str
    payload_digest: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FreshnessProofEvent:
    event_id: str
    proof_id: str
    event_type: EventType
    created_at: str
    payload_digest: str
    dimension: Optional[FreshnessTestDimension] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FreshnessProofResult:
    identity: FreshnessProofIdentity
    lineage: FreshnessProofLineage

    state: FreshnessProofState
    decision: ProofDecision
    reason: ProofReason

    overall_score: float
    scale_band: FreshnessScaleBand

    evaluated_resource_count: int
    evaluated_changed_resource_count: int
    evaluated_recrawl_count: int
    evaluated_queue_count: int

    dimensions: Tuple[FreshnessDimensionResult, ...]
    findings: Tuple[FreshnessFinding, ...]
    global_result: GlobalFreshnessResult

    checkpoints: Tuple[FreshnessProofCheckpoint, ...]
    events: Tuple[FreshnessProofEvent, ...]

    created_at: str
    finalized_at: str

    digest: str


# ============================================================================
# BACKEND PROTOCOL
# ============================================================================


class FreshnessProofBackend(Protocol):
    def save_result(self, result: FreshnessProofResult) -> None:
        ...

    def load_result(self, proof_id: str) -> Optional[FreshnessProofResult]:
        ...

    def save_checkpoint(self, checkpoint: FreshnessProofCheckpoint) -> None:
        ...

    def save_event(self, event: FreshnessProofEvent) -> None:
        ...


class InMemoryFreshnessProofBackend:
    """
    Reference backend for deterministic testing.

    Production deployments can replace this with durable distributed
    storage without changing the proof logic.
    """

    def __init__(self) -> None:
        self.results: Dict[str, FreshnessProofResult] = {}
        self.checkpoints: Dict[str, FreshnessProofCheckpoint] = {}
        self.events: Dict[str, FreshnessProofEvent] = {}

    def save_result(self, result: FreshnessProofResult) -> None:
        self.results[result.identity.proof_id] = result

    def load_result(self, proof_id: str) -> Optional[FreshnessProofResult]:
        return self.results.get(proof_id)

    def save_checkpoint(self, checkpoint: FreshnessProofCheckpoint) -> None:
        self.checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(self, event: FreshnessProofEvent) -> None:
        self.events[event.event_id] = event


# ============================================================================
# MAIN SCALE-PROOF ENGINE
# ============================================================================


class FreshnessRecrawlingScaleProof:
    """
    Deterministic scale-proof engine for Phase 15.6.
    """

    REQUIRED_DIMENSIONS: Tuple[FreshnessTestDimension, ...] = (
        FreshnessTestDimension.RESOURCE_SCALE,
        FreshnessTestDimension.CHANGED_RESOURCE_SCALE,
        FreshnessTestDimension.RECRAWL_VOLUME,
        FreshnessTestDimension.QUEUE_SCALE,
        FreshnessTestDimension.FRESHNESS_COVERAGE,
        FreshnessTestDimension.CHANGE_DETECTION,
        FreshnessTestDimension.RECRAWL_SCHEDULING,
        FreshnessTestDimension.ADAPTIVE_FREQUENCY,
        FreshnessTestDimension.RECRAWL_COMPLETION,
        FreshnessTestDimension.FRESHNESS_SLA,
        FreshnessTestDimension.DISTRIBUTED_ORCHESTRATION,
        FreshnessTestDimension.RESOURCE_ALLOCATION,
        FreshnessTestDimension.GLOBAL_COORDINATION,
        FreshnessTestDimension.FAILURE_RECOVERY,
        FreshnessTestDimension.REGIONAL_CONSISTENCY,
        FreshnessTestDimension.PARTITION_STABILITY,
        FreshnessTestDimension.PRIORITY_CORRECTNESS,
        FreshnessTestDimension.SCHEDULE_STABILITY,
        FreshnessTestDimension.DUPLICATE_PREVENTION,
        FreshnessTestDimension.LOST_WORK_PREVENTION,
        FreshnessTestDimension.SCHEDULING_ERROR_CONTROL,
        FreshnessTestDimension.FRESHNESS_REGRESSION_CONTROL,
        FreshnessTestDimension.P50_LATENCY,
        FreshnessTestDimension.P95_LATENCY,
        FreshnessTestDimension.P99_LATENCY,
        FreshnessTestDimension.QUEUE_DRAIN,
        FreshnessTestDimension.RECOVERY_COMPLETION,
        FreshnessTestDimension.RESOURCE_EFFICIENCY,
        FreshnessTestDimension.OVERLOAD_RESILIENCE,
    )

    def __init__(
        self,
        proof_input: FreshnessProofInput,
        *,
        policy: Optional[FreshnessProofPolicy] = None,
        backend: Optional[FreshnessProofBackend] = None,
        proof_id: Optional[str] = None,
        parent_proof_ids: Sequence[str] = (),
    ) -> None:
        self.input = proof_input
        self.policy = policy or FreshnessProofPolicy()
        self.backend = backend or InMemoryFreshnessProofBackend()

        now = self._now()

        self.identity = FreshnessProofIdentity(
            proof_id=proof_id or f"15.6-{uuid.uuid4().hex}",
            architecture_version=ARCHITECTURE_VERSION,
            phase=PHASE,
            created_at=now,
            target=SCALE_TARGET,
        )

        self.lineage = FreshnessProofLineage(
            previous_stage=PREVIOUS_STAGE,
            next_stage=NEXT_STAGE,
            phase_name=PHASE_NAME,
            source_components=(
                "freshness_prioritization",
                "change_detection",
                "recrawl_scheduling",
                "adaptive_recrawl_frequency",
                "freshness_queues",
                "distributed_recrawl_orchestration",
                "freshness_resource_allocation",
                "global_recrawl_coordination",
                "failure_recovery",
            ),
            parent_proof_ids=tuple(parent_proof_ids),
        )

        self.state = FreshnessProofState.CREATED

        self._dimensions: List[FreshnessDimensionResult] = []
        self._findings: List[FreshnessFinding] = []
        self._checkpoints: List[FreshnessProofCheckpoint] = []
        self._events: List[FreshnessProofEvent] = []

        self._result: Optional[FreshnessProofResult] = None

        self._checkpoint(
            CheckpointType.PROOF_CREATED,
            {
                "phase": PHASE,
                "architecture_version": ARCHITECTURE_VERSION,
                "target": SCALE_TARGET,
            },
        )

        self._emit_event(
            EventType.PROOF_CREATED,
            {
                "phase": PHASE,
                "target": SCALE_TARGET,
            },
        )

    # ----------------------------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------------------------

    def evaluate(self) -> FreshnessProofResult:
        if self._result is not None:
            return self._result

        self.state = FreshnessProofState.COLLECTING

        try:
            self._validate_input()

            self._checkpoint(
                CheckpointType.INPUT_ACCEPTED,
                {
                    "measurement_count": len(self.input.measurements),
                },
            )

            normalized = self._normalize_input()

            self._checkpoint(
                CheckpointType.INPUT_NORMALIZED,
                {
                    "measurement_count": len(normalized.measurements),
                },
            )

            if not normalized.measurements:
                return self._finalize_deferred(
                    ProofReason.NO_DIMENSIONS,
                    "No freshness or recrawling measurements were supplied.",
                )

            self.state = FreshnessProofState.NORMALIZED

            self._dimensions = []

            for dimension in self.REQUIRED_DIMENSIONS:
                result = self._evaluate_dimension(
                    dimension,
                    normalized.measurements,
                )
                self._dimensions.append(result)

                self._checkpoint(
                    CheckpointType.DIMENSION_VALIDATED,
                    {
                        "dimension": dimension.value,
                        "passed": result.passed,
                        "score": result.score,
                    },
                )

                self._emit_event(
                    EventType.DIMENSION_EVALUATED,
                    {
                        "passed": result.passed,
                        "score": result.score,
                    },
                    dimension=dimension,
                )

            self.state = FreshnessProofState.EVALUATED

            global_result = self._evaluate_global(normalized.measurements)
            findings = self._collect_findings()

            decision, reason = self._make_decision(
                normalized,
                global_result,
                findings,
            )

            self._checkpoint(
                CheckpointType.FRESHNESS_EVALUATED,
                {
                    "decision": decision.value,
                    "reason": reason.value,
                    "score": global_result.score,
                },
            )

            return self._finalize(
                normalized,
                global_result,
                findings,
                decision,
                reason,
            )

        except ValueError as exc:
            finding = FreshnessFinding(
                dimension=FreshnessTestDimension.RESOURCE_SCALE,
                reason=ProofReason.INVALID_INPUT,
                message=str(exc),
            )

            self._findings.append(finding)

            return self._finalize(
                self.input,
                self._evaluate_global(self.input.measurements),
                tuple(self._findings),
                ProofDecision.FAIL,
                ProofReason.INVALID_INPUT,
            )

    def evaluate_many(
        self,
        inputs: Iterable[FreshnessProofInput],
    ) -> Tuple[FreshnessProofResult, ...]:
        results: List[FreshnessProofResult] = []

        for proof_input in inputs:
            proof = FreshnessRecrawlingScaleProof(
                proof_input,
                policy=self.policy,
                backend=self.backend,
            )
            results.append(proof.evaluate())

        return tuple(results)

    def get_result(self) -> Optional[FreshnessProofResult]:
        return self._result

    # ----------------------------------------------------------------------
    # VALIDATION
    # ----------------------------------------------------------------------

    def _validate_input(self) -> None:
        if self.policy.require_public_web_scope:
            if not self.input.public_web_scope:
                raise ValueError(
                    ProofReason.PUBLIC_SCOPE_REQUIRED.value
                )

        if not self.input.measurements:
            return

        seen: set[FreshnessTestDimension] = set()

        for measurement in self.input.measurements:
            if measurement.dimension in seen:
                raise ValueError(
                    f"{ProofReason.DUPLICATE_DIMENSION.value}: "
                    f"{measurement.dimension.value}"
                )

            seen.add(measurement.dimension)

            self._validate_measurement(measurement)

    def _validate_measurement(
        self,
        measurement: FreshnessRecrawlingMeasurement,
    ) -> None:
        bounded_counts = {
            "resource_count": measurement.resource_count,
            "changed_resource_count": measurement.changed_resource_count,
            "recrawl_count": measurement.recrawl_count,
            "queue_count": measurement.queue_count,
            "worker_count": measurement.worker_count,
            "shard_count": measurement.shard_count,
            "region_count": measurement.region_count,
            "partition_count": measurement.partition_count,
        }

        for name, value in bounded_counts.items():
            if value < 0:
                raise ValueError(
                    f"{ProofReason.INVALID_METRIC.value}: "
                    f"{name} cannot be negative"
                )

        if measurement.resource_count > MAX_RESOURCE_COUNT:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "resource_count exceeds maximum"
            )

        if measurement.changed_resource_count > MAX_CHANGED_RESOURCE_COUNT:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "changed_resource_count exceeds maximum"
            )

        if measurement.recrawl_count > MAX_RECrawl_COUNT:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "recrawl_count exceeds maximum"
            )

        if measurement.queue_count > MAX_QUEUE_COUNT:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "queue_count exceeds maximum"
            )

        if measurement.worker_count > MAX_WORKER_COUNT:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "worker_count exceeds maximum"
            )

        if measurement.shard_count > MAX_SHARD_COUNT:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "shard_count exceeds maximum"
            )

        if measurement.region_count > MAX_REGION_COUNT:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "region_count exceeds maximum"
            )

        if measurement.partition_count > MAX_PARTITION_COUNT:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "partition_count exceeds maximum"
            )

        if measurement.measurement_window_seconds < 0:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "measurement_window_seconds cannot be negative"
            )

        if (
            measurement.measurement_window_seconds
            > MAX_MEASUREMENT_WINDOW_SECONDS
        ):
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "measurement_window_seconds exceeds maximum"
            )

        bounded_scores = {
            "freshness_coverage": measurement.freshness_coverage,
            "change_detection_accuracy": measurement.change_detection_accuracy,
            "recrawl_scheduling_correctness":
                measurement.recrawl_scheduling_correctness,
            "adaptive_frequency_quality":
                measurement.adaptive_frequency_quality,
            "recrawl_completion_rate":
                measurement.recrawl_completion_rate,
            "freshness_sla_compliance":
                measurement.freshness_sla_compliance,
            "distributed_orchestration":
                measurement.distributed_orchestration,
            "resource_allocation":
                measurement.resource_allocation,
            "global_coordination":
                measurement.global_coordination,
            "failure_recovery_rate":
                measurement.failure_recovery_rate,
            "regional_consistency":
                measurement.regional_consistency,
            "partition_stability":
                measurement.partition_stability,
            "priority_correctness":
                measurement.priority_correctness,
            "schedule_stability":
                measurement.schedule_stability,
            "duplicate_rate":
                measurement.duplicate_rate,
            "lost_work_rate":
                measurement.lost_work_rate,
            "scheduling_error_rate":
                measurement.scheduling_error_rate,
            "freshness_regression_rate":
                measurement.freshness_regression_rate,
            "timeout_rate":
                measurement.timeout_rate,
            "queue_drain_rate":
                measurement.queue_drain_rate,
            "recovery_completion_rate":
                measurement.recovery_completion_rate,
            "resource_efficiency":
                measurement.resource_efficiency,
            "overload_resilience":
                measurement.overload_resilience,
        }

        for name, value in bounded_scores.items():
            if not math.isfinite(value):
                raise ValueError(
                    f"{ProofReason.INVALID_METRIC.value}: "
                    f"{name} must be finite"
                )

            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"{ProofReason.INVALID_METRIC.value}: "
                    f"{name} must be between 0 and 1"
                )

        latency_values = (
            measurement.p50_latency_ms,
            measurement.p95_latency_ms,
            measurement.p99_latency_ms,
        )

        for latency in latency_values:
            if not math.isfinite(latency):
                raise ValueError(
                    f"{ProofReason.INVALID_METRIC.value}: "
                    "latency must be finite"
                )

            if latency < 0:
                raise ValueError(
                    f"{ProofReason.INVALID_METRIC.value}: "
                    "latency cannot be negative"
                )

        if measurement.p50_latency_ms > measurement.p95_latency_ms:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "p50 latency cannot exceed p95 latency"
            )

        if measurement.p95_latency_ms > measurement.p99_latency_ms:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "p95 latency cannot exceed p99 latency"
            )

    # ----------------------------------------------------------------------
    # NORMALIZATION
    # ----------------------------------------------------------------------

    def _normalize_input(self) -> FreshnessProofInput:
        measurements = tuple(
            sorted(
                self.input.measurements,
                key=lambda item: item.dimension.value,
            )
        )

        return FreshnessProofInput(
            measurements=measurements,
            public_web_scope=self.input.public_web_scope,
            evidence_strength=self.input.evidence_strength,
            allow_partial_evidence=self.input.allow_partial_evidence,
            allow_simulated_evidence=self.input.allow_simulated_evidence,
            require_all_dimensions=self.input.require_all_dimensions,
            metadata=dict(self.input.metadata),
        )

    # ----------------------------------------------------------------------
    # DIMENSION EVALUATION
    # ----------------------------------------------------------------------

    def _evaluate_dimension(
        self,
        dimension: FreshnessTestDimension,
        measurements: Sequence[FreshnessRecrawlingMeasurement],
    ) -> FreshnessDimensionResult:
        matching = [
            item
            for item in measurements
            if item.dimension == dimension
        ]

        if not matching:
            return FreshnessDimensionResult(
                dimension=dimension,
                passed=False,
                score=0.0,
                findings=(),
                measurement_count=0,
            )

        findings: List[FreshnessFinding] = []

        for index, measurement in enumerate(matching):
            findings.extend(
                self._evaluate_measurement_for_dimension(
                    dimension,
                    measurement,
                    index,
                )
            )

        score = self._dimension_score(
            dimension,
            matching,
        )

        return FreshnessDimensionResult(
            dimension=dimension,
            passed=not findings,
            score=score,
            findings=tuple(findings),
            measurement_count=len(matching),
        )

    def _evaluate_measurement_for_dimension(
        self,
        dimension: FreshnessTestDimension,
        measurement: FreshnessRecrawlingMeasurement,
        index: int,
    ) -> Tuple[FreshnessFinding, ...]:
        findings: List[FreshnessFinding] = []
        policy = self.policy

        def add(
            reason: ProofReason,
            message: str,
            observed: Optional[float] = None,
            required: Optional[float] = None,
        ) -> None:
            findings.append(
                FreshnessFinding(
                    dimension=dimension,
                    reason=reason,
                    message=message,
                    measurement_index=index,
                    observed_value=observed,
                    required_value=required,
                )
            )

        if dimension == FreshnessTestDimension.RESOURCE_SCALE:
            if measurement.resource_count < policy.min_resource_count:
                add(
                    ProofReason.INSUFFICIENT_RESOURCE_VOLUME,
                    "Resource volume is below the configured scale-proof minimum.",
                    measurement.resource_count,
                    policy.min_resource_count,
                )

        elif dimension == FreshnessTestDimension.CHANGED_RESOURCE_SCALE:
            if (
                measurement.changed_resource_count
                < policy.min_changed_resource_count
            ):
                add(
                    ProofReason.INSUFFICIENT_CHANGED_RESOURCE_VOLUME,
                    "Changed-resource volume is below the configured proof minimum.",
                    measurement.changed_resource_count,
                    policy.min_changed_resource_count,
                )

        elif dimension == FreshnessTestDimension.RECRAWL_VOLUME:
            if measurement.recrawl_count < policy.min_recrawl_count:
                add(
                    ProofReason.INSUFFICIENT_RECRAWL_VOLUME,
                    "Recrawl volume is below the configured proof minimum.",
                    measurement.recrawl_count,
                    policy.min_recrawl_count,
                )

        elif dimension == FreshnessTestDimension.QUEUE_SCALE:
            if measurement.queue_count < policy.min_queue_count:
                add(
                    ProofReason.INSUFFICIENT_QUEUE_VOLUME,
                    "Freshness/recrawl queue volume is below the configured proof minimum.",
                    measurement.queue_count,
                    policy.min_queue_count,
                )

        elif dimension == FreshnessTestDimension.FRESHNESS_COVERAGE:
            if measurement.freshness_coverage < policy.min_freshness_coverage:
                add(
                    ProofReason.LOW_FRESHNESS_COVERAGE,
                    "Freshness coverage is below the required threshold.",
                    measurement.freshness_coverage,
                    policy.min_freshness_coverage,
                )

        elif dimension == FreshnessTestDimension.CHANGE_DETECTION:
            if (
                measurement.change_detection_accuracy
                < policy.min_change_detection_accuracy
            ):
                add(
                    ProofReason.LOW_CHANGE_DETECTION,
                    "Change-detection accuracy is below the required threshold.",
                    measurement.change_detection_accuracy,
                    policy.min_change_detection_accuracy,
                )

        elif dimension == FreshnessTestDimension.RECRAWL_SCHEDULING:
            if (
                measurement.recrawl_scheduling_correctness
                < policy.min_recrawl_scheduling_correctness
            ):
                add(
                    ProofReason.LOW_SCHEDULING_CORRECTNESS,
                    "Recrawl scheduling correctness is below the required threshold.",
                    measurement.recrawl_scheduling_correctness,
                    policy.min_recrawl_scheduling_correctness,
                )

        elif dimension == FreshnessTestDimension.ADAPTIVE_FREQUENCY:
            if (
                measurement.adaptive_frequency_quality
                < policy.min_adaptive_frequency_quality
            ):
                add(
                    ProofReason.LOW_ADAPTIVE_FREQUENCY_QUALITY,
                    "Adaptive recrawl-frequency quality is below the required threshold.",
                    measurement.adaptive_frequency_quality,
                    policy.min_adaptive_frequency_quality,
                )

        elif dimension == FreshnessTestDimension.RECRAWL_COMPLETION:
            if (
                measurement.recrawl_completion_rate
                < policy.min_recrawl_completion_rate
            ):
                add(
                    ProofReason.LOW_RECRAWL_COMPLETION,
                    "Recrawl completion rate is below the required threshold.",
                    measurement.recrawl_completion_rate,
                    policy.min_recrawl_completion_rate,
                )

        elif dimension == FreshnessTestDimension.FRESHNESS_SLA:
            if (
                measurement.freshness_sla_compliance
                < policy.min_freshness_sla_compliance
            ):
                add(
                    ProofReason.LOW_FRESHNESS_SLA,
                    "Freshness SLA compliance is below the required threshold.",
                    measurement.freshness_sla_compliance,
                    policy.min_freshness_sla_compliance,
                )

        elif dimension == FreshnessTestDimension.DISTRIBUTED_ORCHESTRATION:
            if (
                measurement.distributed_orchestration
                < policy.min_distributed_orchestration
            ):
                add(
                    ProofReason.LOW_DISTRIBUTED_ORCHESTRATION,
                    "Distributed recrawl orchestration is below the required threshold.",
                    measurement.distributed_orchestration,
                    policy.min_distributed_orchestration,
                )

        elif dimension == FreshnessTestDimension.RESOURCE_ALLOCATION:
            if measurement.resource_allocation < policy.min_resource_allocation:
                add(
                    ProofReason.LOW_RESOURCE_ALLOCATION,
                    "Freshness resource allocation quality is below the required threshold.",
                    measurement.resource_allocation,
                    policy.min_resource_allocation,
                )

        elif dimension == FreshnessTestDimension.GLOBAL_COORDINATION:
            if measurement.global_coordination < policy.min_global_coordination:
                add(
                    ProofReason.LOW_GLOBAL_COORDINATION,
                    "Global recrawl coordination is below the required threshold.",
                    measurement.global_coordination,
                    policy.min_global_coordination,
                )

        elif dimension == FreshnessTestDimension.FAILURE_RECOVERY:
            if (
                measurement.failure_recovery_rate
                < policy.min_failure_recovery_rate
            ):
                add(
                    ProofReason.LOW_FAILURE_RECOVERY,
                    "Failure recovery rate is below the required threshold.",
                    measurement.failure_recovery_rate,
                    policy.min_failure_recovery_rate,
                )

        elif dimension == FreshnessTestDimension.REGIONAL_CONSISTENCY:
            if measurement.regional_consistency < policy.min_regional_consistency:
                add(
                    ProofReason.LOW_REGIONAL_CONSISTENCY,
                    "Regional freshness consistency is below the required threshold.",
                    measurement.regional_consistency,
                    policy.min_regional_consistency,
                )

        elif dimension == FreshnessTestDimension.PARTITION_STABILITY:
            if measurement.partition_stability < policy.min_partition_stability:
                add(
                    ProofReason.LOW_PARTITION_STABILITY,
                    "Partition stability is below the required threshold.",
                    measurement.partition_stability,
                    policy.min_partition_stability,
                )

        elif dimension == FreshnessTestDimension.PRIORITY_CORRECTNESS:
            if measurement.priority_correctness < policy.min_priority_correctness:
                add(
                    ProofReason.LOW_PRIORITY_CORRECTNESS,
                    "Recrawl priority correctness is below the required threshold.",
                    measurement.priority_correctness,
                    policy.min_priority_correctness,
                )

        elif dimension == FreshnessTestDimension.SCHEDULE_STABILITY:
            if measurement.schedule_stability < policy.min_schedule_stability:
                add(
                    ProofReason.LOW_SCHEDULE_STABILITY,
                    "Recrawl schedule stability is below the required threshold.",
                    measurement.schedule_stability,
                    policy.min_schedule_stability,
                )

        elif dimension == FreshnessTestDimension.DUPLICATE_PREVENTION:
            if measurement.duplicate_rate > policy.max_duplicate_rate:
                add(
                    ProofReason.HIGH_DUPLICATE_RATE,
                    "Duplicate recrawl rate exceeds the allowed maximum.",
                    measurement.duplicate_rate,
                    policy.max_duplicate_rate,
                )

        elif dimension == FreshnessTestDimension.LOST_WORK_PREVENTION:
            if measurement.lost_work_rate > policy.max_lost_work_rate:
                add(
                    ProofReason.HIGH_LOST_WORK_RATE,
                    "Lost recrawl work rate exceeds the allowed maximum.",
                    measurement.lost_work_rate,
                    policy.max_lost_work_rate,
                )

        elif dimension == FreshnessTestDimension.SCHEDULING_ERROR_CONTROL:
            if (
                measurement.scheduling_error_rate
                > policy.max_scheduling_error_rate
            ):
                add(
                    ProofReason.HIGH_SCHEDULING_ERROR_RATE,
                    "Scheduling error rate exceeds the allowed maximum.",
                    measurement.scheduling_error_rate,
                    policy.max_scheduling_error_rate,
                )

        elif dimension == FreshnessTestDimension.FRESHNESS_REGRESSION_CONTROL:
            if (
                measurement.freshness_regression_rate
                > policy.max_freshness_regression_rate
            ):
                add(
                    ProofReason.HIGH_FRESHNESS_REGRESSION_RATE,
                    "Freshness regression rate exceeds the allowed maximum.",
                    measurement.freshness_regression_rate,
                    policy.max_freshness_regression_rate,
                )

        elif dimension in (
            FreshnessTestDimension.P50_LATENCY,
            FreshnessTestDimension.P95_LATENCY,
            FreshnessTestDimension.P99_LATENCY,
        ):
            findings.extend(
                self._evaluate_latency_dimension(
                    dimension,
                    measurement,
                    index,
                )
            )

        elif dimension == FreshnessTestDimension.QUEUE_DRAIN:
            if measurement.queue_drain_rate < policy.min_queue_drain_rate:
                add(
                    ProofReason.LOW_QUEUE_DRAIN,
                    "Queue drain rate is below the required threshold.",
                    measurement.queue_drain_rate,
                    policy.min_queue_drain_rate,
                )

        elif dimension == FreshnessTestDimension.RECOVERY_COMPLETION:
            if (
                measurement.recovery_completion_rate
                < policy.min_recovery_completion_rate
            ):
                add(
                    ProofReason.LOW_RECOVERY_COMPLETION,
                    "Recovery completion rate is below the required threshold.",
                    measurement.recovery_completion_rate,
                    policy.min_recovery_completion_rate,
                )

        elif dimension == FreshnessTestDimension.RESOURCE_EFFICIENCY:
            if measurement.resource_efficiency < policy.min_resource_efficiency:
                add(
                    ProofReason.LOW_RESOURCE_EFFICIENCY,
                    "Resource efficiency is below the required threshold.",
                    measurement.resource_efficiency,
                    policy.min_resource_efficiency,
                )

        elif dimension == FreshnessTestDimension.OVERLOAD_RESILIENCE:
            if measurement.overload_resilience < policy.min_overload_resilience:
                add(
                    ProofReason.LOW_OVERLOAD_RESILIENCE,
                    "Overload resilience is below the required threshold.",
                    measurement.overload_resilience,
                    policy.min_overload_resilience,
                )

        if measurement.timeout_rate > policy.max_timeout_rate:
            add(
                ProofReason.HIGH_TIMEOUT_RATE,
                "Timeout rate exceeds the allowed maximum.",
                measurement.timeout_rate,
                policy.max_timeout_rate,
            )

        return tuple(findings)

    def _evaluate_latency_dimension(
        self,
        dimension: FreshnessTestDimension,
        measurement: FreshnessRecrawlingMeasurement,
        index: int,
    ) -> Tuple[FreshnessFinding, ...]:
        if dimension == FreshnessTestDimension.P50_LATENCY:
            observed = measurement.p50_latency_ms
            maximum = self.policy.max_p50_latency_ms
            reason = ProofReason.HIGH_P50_LATENCY

        elif dimension == FreshnessTestDimension.P95_LATENCY:
            observed = measurement.p95_latency_ms
            maximum = self.policy.max_p95_latency_ms
            reason = ProofReason.HIGH_P95_LATENCY

        else:
            observed = measurement.p99_latency_ms
            maximum = self.policy.max_p99_latency_ms
            reason = ProofReason.HIGH_P99_LATENCY

        if observed > maximum:
            return (
                FreshnessFinding(
                    dimension=dimension,
                    reason=reason,
                    message=(
                        f"{dimension.value} latency exceeds the configured "
                        "maximum."
                    ),
                    measurement_index=index,
                    observed_value=observed,
                    required_value=maximum,
                ),
            )

        return ()

    # ----------------------------------------------------------------------
    # SCORE FUNCTIONS
    # ----------------------------------------------------------------------

    def _dimension_score(
        self,
        dimension: FreshnessTestDimension,
        measurements: Sequence[FreshnessRecrawlingMeasurement],
    ) -> float:
        if not measurements:
            return 0.0

        values: List[float] = []

        for measurement in measurements:
            values.append(
                self._single_dimension_score(
                    dimension,
                    measurement,
                )
            )

        return self._mean(values)

    def _single_dimension_score(
        self,
        dimension: FreshnessTestDimension,
        measurement: FreshnessRecrawlingMeasurement,
    ) -> float:
        if dimension == FreshnessTestDimension.RESOURCE_SCALE:
            return self._volume_quality(
                measurement.resource_count,
                self.policy.min_resource_count,
            )

        if dimension == FreshnessTestDimension.CHANGED_RESOURCE_SCALE:
            return self._volume_quality(
                measurement.changed_resource_count,
                self.policy.min_changed_resource_count,
            )

        if dimension == FreshnessTestDimension.RECRAWL_VOLUME:
            return self._volume_quality(
                measurement.recrawl_count,
                self.policy.min_recrawl_count,
            )

        if dimension == FreshnessTestDimension.QUEUE_SCALE:
            return self._volume_quality(
                measurement.queue_count,
                self.policy.min_queue_count,
            )

        if dimension == FreshnessTestDimension.FRESHNESS_COVERAGE:
            return self._threshold_quality(
                measurement.freshness_coverage,
                self.policy.min_freshness_coverage,
            )

        if dimension == FreshnessTestDimension.CHANGE_DETECTION:
            return self._threshold_quality(
                measurement.change_detection_accuracy,
                self.policy.min_change_detection_accuracy,
            )

        if dimension == FreshnessTestDimension.RECRAWL_SCHEDULING:
            return self._threshold_quality(
                measurement.recrawl_scheduling_correctness,
                self.policy.min_recrawl_scheduling_correctness,
            )

        if dimension == FreshnessTestDimension.ADAPTIVE_FREQUENCY:
            return self._threshold_quality(
                measurement.adaptive_frequency_quality,
                self.policy.min_adaptive_frequency_quality,
            )

        if dimension == FreshnessTestDimension.RECRAWL_COMPLETION:
            return self._threshold_quality(
                measurement.recrawl_completion_rate,
                self.policy.min_recrawl_completion_rate,
            )

        if dimension == FreshnessTestDimension.FRESHNESS_SLA:
            return self._threshold_quality(
                measurement.freshness_sla_compliance,
                self.policy.min_freshness_sla_compliance,
            )

        if dimension == FreshnessTestDimension.DISTRIBUTED_ORCHESTRATION:
            return self._threshold_quality(
                measurement.distributed_orchestration,
                self.policy.min_distributed_orchestration,
            )

        if dimension == FreshnessTestDimension.RESOURCE_ALLOCATION:
            return self._threshold_quality(
                measurement.resource_allocation,
                self.policy.min_resource_allocation,
            )

        if dimension == FreshnessTestDimension.GLOBAL_COORDINATION:
            return self._threshold_quality(
                measurement.global_coordination,
                self.policy.min_global_coordination,
            )

        if dimension == FreshnessTestDimension.FAILURE_RECOVERY:
            return self._threshold_quality(
                measurement.failure_recovery_rate,
                self.policy.min_failure_recovery_rate,
            )

        if dimension == FreshnessTestDimension.REGIONAL_CONSISTENCY:
            return self._threshold_quality(
                measurement.regional_consistency,
                self.policy.min_regional_consistency,
            )

        if dimension == FreshnessTestDimension.PARTITION_STABILITY:
            return self._threshold_quality(
                measurement.partition_stability,
                self.policy.min_partition_stability,
            )

        if dimension == FreshnessTestDimension.PRIORITY_CORRECTNESS:
            return self._threshold_quality(
                measurement.priority_correctness,
                self.policy.min_priority_correctness,
            )

        if dimension == FreshnessTestDimension.SCHEDULE_STABILITY:
            return self._threshold_quality(
                measurement.schedule_stability,
                self.policy.min_schedule_stability,
            )

        if dimension == FreshnessTestDimension.DUPLICATE_PREVENTION:
            return self._inverse_rate_quality(
                measurement.duplicate_rate,
                self.policy.max_duplicate_rate,
            )

        if dimension == FreshnessTestDimension.LOST_WORK_PREVENTION:
            return self._inverse_rate_quality(
                measurement.lost_work_rate,
                self.policy.max_lost_work_rate,
            )

        if dimension == FreshnessTestDimension.SCHEDULING_ERROR_CONTROL:
            return self._inverse_rate_quality(
                measurement.scheduling_error_rate,
                self.policy.max_scheduling_error_rate,
            )

        if dimension == FreshnessTestDimension.FRESHNESS_REGRESSION_CONTROL:
            return self._inverse_rate_quality(
                measurement.freshness_regression_rate,
                self.policy.max_freshness_regression_rate,
            )

        if dimension == FreshnessTestDimension.P50_LATENCY:
            return self._bounded_latency_quality(
                measurement.p50_latency_ms,
                self.policy.max_p50_latency_ms,
            )

        if dimension == FreshnessTestDimension.P95_LATENCY:
            return self._bounded_latency_quality(
                measurement.p95_latency_ms,
                self.policy.max_p95_latency_ms,
            )

        if dimension == FreshnessTestDimension.P99_LATENCY:
            return self._bounded_latency_quality(
                measurement.p99_latency_ms,
                self.policy.max_p99_latency_ms,
            )

        if dimension == FreshnessTestDimension.QUEUE_DRAIN:
            return self._threshold_quality(
                measurement.queue_drain_rate,
                self.policy.min_queue_drain_rate,
            )

        if dimension == FreshnessTestDimension.RECOVERY_COMPLETION:
            return self._threshold_quality(
                measurement.recovery_completion_rate,
                self.policy.min_recovery_completion_rate,
            )

        if dimension == FreshnessTestDimension.RESOURCE_EFFICIENCY:
            return self._threshold_quality(
                measurement.resource_efficiency,
                self.policy.min_resource_efficiency,
            )

        if dimension == FreshnessTestDimension.OVERLOAD_RESILIENCE:
            return self._threshold_quality(
                measurement.overload_resilience,
                self.policy.min_overload_resilience,
            )

        return 0.0

    def _volume_quality(
        self,
        observed: int,
        minimum: int,
    ) -> float:
        if minimum <= 0:
            return 1.0

        if observed <= 0:
            return 0.0

        ratio = observed / minimum

        if ratio >= 1000:
            return 1.0

        if ratio >= 100:
            return 0.99

        if ratio >= 10:
            return 0.95

        if ratio >= 1:
            return 0.90

        return min(0.89, ratio * 0.90)

    def _threshold_quality(
        self,
        observed: float,
        minimum: float,
    ) -> float:
        if observed >= minimum:
            if minimum >= 1.0:
                return 1.0

            extra = observed - minimum
            return min(
                1.0,
                0.90 + (extra / max(1.0 - minimum, 1e-9)) * 0.10,
            )

        if minimum <= 0:
            return 1.0

        return max(0.0, observed / minimum * 0.90)

    def _inverse_rate_quality(
        self,
        observed: float,
        maximum: float,
    ) -> float:
        if observed <= maximum:
            if maximum <= 0:
                return 1.0

            return min(
                1.0,
                0.90 + ((maximum - observed) / maximum) * 0.10,
            )

        if observed >= 1.0:
            return 0.0

        return max(
            0.0,
            0.90 * (1.0 - observed),
        )

    def _bounded_latency_quality(
        self,
        observed: float,
        maximum: float,
    ) -> float:
        if observed <= 0:
            return 1.0

        if maximum <= 0:
            return 0.0

        if observed <= maximum:
            return min(
                1.0,
                0.90 + ((maximum - observed) / maximum) * 0.10,
            )

        ratio = maximum / observed
        return max(0.0, min(0.89, ratio * 0.89))

    # ----------------------------------------------------------------------
    # GLOBAL EVALUATION
    # ----------------------------------------------------------------------

    def _evaluate_global(
        self,
        measurements: Sequence[FreshnessRecrawlingMeasurement],
    ) -> GlobalFreshnessResult:
        resource_count = self._evaluated_resource_count(measurements)
        changed_count = self._evaluated_changed_resource_count(measurements)
        recrawl_count = self._evaluated_recrawl_count(measurements)
        queue_count = self._evaluated_queue_count(measurements)

        dimensions_evaluated = len(
            [item for item in self._dimensions if item.measurement_count > 0]
        )

        dimensions_required = len(self.REQUIRED_DIMENSIONS)

        passed_dimensions = len(
            [item for item in self._dimensions if item.passed]
        )

        failed_dimensions = len(
            [
                item
                for item in self._dimensions
                if item.measurement_count > 0 and not item.passed
            ]
        )

        deferred_dimensions = dimensions_required - dimensions_evaluated

        decision = ProofDecision.PASS

        if failed_dimensions > 0:
            decision = ProofDecision.FAIL
        elif deferred_dimensions > 0:
            decision = ProofDecision.DEFER

        return GlobalFreshnessResult(
            decision=decision,
            score=self._overall_score(),
            scale_band=self._scale_band(resource_count),
            evaluated_resource_count=resource_count,
            evaluated_changed_resource_count=changed_count,
            evaluated_recrawl_count=recrawl_count,
            evaluated_queue_count=queue_count,
            dimensions_evaluated=dimensions_evaluated,
            dimensions_required=dimensions_required,
            passed_dimensions=passed_dimensions,
            failed_dimensions=failed_dimensions,
            deferred_dimensions=deferred_dimensions,
        )

    def _collect_findings(self) -> Tuple[FreshnessFinding, ...]:
        findings: List[FreshnessFinding] = []

        for dimension in self._dimensions:
            findings.extend(dimension.findings)

        return tuple(findings)

    def _make_decision(
        self,
        proof_input: FreshnessProofInput,
        global_result: GlobalFreshnessResult,
        findings: Sequence[FreshnessFinding],
    ) -> Tuple[ProofDecision, ProofReason]:
        if not proof_input.public_web_scope and self.policy.require_public_web_scope:
            return (
                ProofDecision.FAIL,
                ProofReason.PUBLIC_SCOPE_REQUIRED,
            )

        if not proof_input.measurements:
            return (
                ProofDecision.DEFER,
                ProofReason.NO_DIMENSIONS,
            )

        if (
            proof_input.evidence_strength
            in (
                EvidenceStrength.PARTIAL,
                EvidenceStrength.UNKNOWN,
            )
            and not proof_input.allow_partial_evidence
        ):
            return (
                ProofDecision.DEFER,
                ProofReason.PARTIAL_EVIDENCE_NOT_ALLOWED,
            )

        if (
            any(
                item.evidence_strength == EvidenceStrength.SIMULATED
                for item in proof_input.measurements
            )
            and not proof_input.allow_simulated_evidence
        ):
            return (
                ProofDecision.DEFER,
                ProofReason.SIMULATED_EVIDENCE_NOT_ALLOWED,
            )

        if (
            proof_input.require_all_dimensions
            and global_result.deferred_dimensions > 0
        ):
            return (
                ProofDecision.DEFER,
                ProofReason.NO_DIMENSIONS,
            )

        if findings:
            return (
                ProofDecision.FAIL,
                findings[0].reason,
            )

        if global_result.decision == ProofDecision.FAIL:
            return (
                ProofDecision.FAIL,
                ProofReason.INVALID_INPUT,
            )

        if global_result.decision == ProofDecision.DEFER:
            return (
                ProofDecision.DEFER,
                ProofReason.NO_DIMENSIONS,
            )

        return (
            ProofDecision.PASS,
            ProofReason.PASS_POLICY,
        )

    def _state_from_decision(
        self,
        decision: ProofDecision,
    ) -> FreshnessProofState:
        if decision == ProofDecision.PASS:
            return FreshnessProofState.PASSED

        if decision == ProofDecision.FAIL:
            return FreshnessProofState.FAILED

        return FreshnessProofState.DEFERRED

    # ----------------------------------------------------------------------
    # AGGREGATION
    # ----------------------------------------------------------------------

    def _overall_score(self) -> float:
        if not self._dimensions:
            return 0.0

        scores = [
            item.score
            for item in self._dimensions
            if item.measurement_count > 0
        ]

        if not scores:
            return 0.0

        return self._mean(scores)

    def _mean(self, values: Sequence[float]) -> float:
        if not values:
            return 0.0

        return sum(values) / len(values)

    def _minimum(
        self,
        measurements: Sequence[FreshnessRecrawlingMeasurement],
        attribute: str,
    ) -> float:
        values = [
            float(getattr(item, attribute))
            for item in measurements
        ]

        return min(values) if values else 0.0

    def _maximum(
        self,
        measurements: Sequence[FreshnessRecrawlingMeasurement],
        attribute: str,
    ) -> float:
        values = [
            float(getattr(item, attribute))
            for item in measurements
        ]

        return max(values) if values else 0.0

    def _evaluated_resource_count(
        self,
        measurements: Sequence[FreshnessRecrawlingMeasurement],
    ) -> int:
        return max(
            (item.resource_count for item in measurements),
            default=0,
        )

    def _evaluated_changed_resource_count(
        self,
        measurements: Sequence[FreshnessRecrawlingMeasurement],
    ) -> int:
        return max(
            (item.changed_resource_count for item in measurements),
            default=0,
        )

    def _evaluated_recrawl_count(
        self,
        measurements: Sequence[FreshnessRecrawlingMeasurement],
    ) -> int:
        return max(
            (item.recrawl_count for item in measurements),
            default=0,
        )

    def _evaluated_queue_count(
        self,
        measurements: Sequence[FreshnessRecrawlingMeasurement],
    ) -> int:
        return max(
            (item.queue_count for item in measurements),
            default=0,
        )

    # ----------------------------------------------------------------------
    # SCALE BAND
    # ----------------------------------------------------------------------

    def _scale_band(
        self,
        resource_count: int,
    ) -> FreshnessScaleBand:
        if resource_count >= 10**12:
            return FreshnessScaleBand.TRILLIONS

        if resource_count >= 10**9:
            return FreshnessScaleBand.BILLIONS

        if resource_count >= 10**8:
            return FreshnessScaleBand.MASSIVE

        if resource_count >= 10**6:
            return FreshnessScaleBand.LARGE

        if resource_count > 0:
            return FreshnessScaleBand.SMALL

        return FreshnessScaleBand.UNKNOWN

    # ----------------------------------------------------------------------
    # FINALIZATION
    # ----------------------------------------------------------------------

    def _finalize_deferred(
        self,
        reason: ProofReason,
        message: str,
    ) -> FreshnessProofResult:
        finding = FreshnessFinding(
            dimension=FreshnessTestDimension.RESOURCE_SCALE,
            reason=reason,
            message=message,
            severity="deferred",
        )

        self._findings.append(finding)

        return self._finalize(
            self.input,
            self._evaluate_global(self.input.measurements),
            tuple(self._findings),
            ProofDecision.DEFER,
            reason,
        )

    def _finalize(
        self,
        proof_input: FreshnessProofInput,
        global_result: GlobalFreshnessResult,
        findings: Sequence[FreshnessFinding],
        decision: ProofDecision,
        reason: ProofReason,
    ) -> FreshnessProofResult:
        self.state = self._state_from_decision(decision)

        finalized_at = self._now()

        digest_payload = {
            "identity": self.identity,
            "lineage": self.lineage,
            "state": self.state,
            "decision": decision,
            "reason": reason,
            "overall_score": global_result.score,
            "scale_band": global_result.scale_band,
            "evaluated_resource_count":
                global_result.evaluated_resource_count,
            "evaluated_changed_resource_count":
                global_result.evaluated_changed_resource_count,
            "evaluated_recrawl_count":
                global_result.evaluated_recrawl_count,
            "evaluated_queue_count":
                global_result.evaluated_queue_count,
            "dimensions": self._dimensions,
            "findings": findings,
        }

        digest = self._digest(digest_payload)

        result = FreshnessProofResult(
            identity=self.identity,
            lineage=self.lineage,
            state=self.state,
            decision=decision,
            reason=reason,
            overall_score=global_result.score,
            scale_band=global_result.scale_band,
            evaluated_resource_count=(
                global_result.evaluated_resource_count
            ),
            evaluated_changed_resource_count=(
                global_result.evaluated_changed_resource_count
            ),
            evaluated_recrawl_count=(
                global_result.evaluated_recrawl_count
            ),
            evaluated_queue_count=(
                global_result.evaluated_queue_count
            ),
            dimensions=tuple(self._dimensions),
            findings=tuple(findings),
            global_result=global_result,
            checkpoints=tuple(self._checkpoints),
            events=tuple(self._events),
            created_at=self.identity.created_at,
            finalized_at=finalized_at,
            digest=digest,
        )

        self._result = result

        self._checkpoint(
            CheckpointType.PROOF_FINALIZED,
            {
                "decision": decision.value,
                "reason": reason.value,
                "overall_score": global_result.score,
                "digest": digest,
            },
        )

        if decision == ProofDecision.PASS:
            self._emit_event(
                EventType.PROOF_PASSED,
                {
                    "score": global_result.score,
                    "digest": digest,
                },
            )

        elif decision == ProofDecision.FAIL:
            self._emit_event(
                EventType.PROOF_FAILED,
                {
                    "reason": reason.value,
                    "digest": digest,
                },
            )

        else:
            self._emit_event(
                EventType.PROOF_DEFERRED,
                {
                    "reason": reason.value,
                    "digest": digest,
                },
            )

        self.backend.save_result(result)

        return result

    # ----------------------------------------------------------------------
    # CHECKPOINTS / EVENTS
    # ----------------------------------------------------------------------

    def _checkpoint(
        self,
        checkpoint_type: CheckpointType,
        payload: Mapping[str, Any],
    ) -> None:
        checkpoint = FreshnessProofCheckpoint(
            checkpoint_id=f"cp-{uuid.uuid4().hex}",
            proof_id=self.identity.proof_id,
            checkpoint_type=checkpoint_type,
            state=self.state,
            created_at=self._now(),
            payload_digest=self._digest(payload),
            metadata=dict(payload),
        )

        self._checkpoints.append(checkpoint)

        self.backend.save_checkpoint(checkpoint)

    def _emit_event(
        self,
        event_type: EventType,
        payload: Mapping[str, Any],
        *,
        dimension: Optional[FreshnessTestDimension] = None,
    ) -> None:
        event = FreshnessProofEvent(
            event_id=f"evt-{uuid.uuid4().hex}",
            proof_id=self.identity.proof_id,
            event_type=event_type,
            created_at=self._now(),
            payload_digest=self._digest(payload),
            dimension=dimension,
            metadata=dict(payload),
        )

        self._events.append(event)

        self.backend.save_event(event)

    # ----------------------------------------------------------------------
    # DETERMINISTIC SERIALIZATION / DIGEST
    # ----------------------------------------------------------------------

    def _digest(self, value: Any) -> str:
        canonical = self._canonicalize(value)

        encoded = json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")

        return hashlib.sha256(encoded).hexdigest()

    def _canonicalize(self, value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value

        if isinstance(value, Mapping):
            return {
                str(key): self._canonicalize(item)
                for key, item in sorted(
                    value.items(),
                    key=lambda pair: str(pair[0]),
                )
            }

        if isinstance(value, (tuple, list)):
            return [
                self._canonicalize(item)
                for item in value
            ]

        if isinstance(value, set):
            return sorted(
                self._canonicalize(item)
                for item in value
            )

        if hasattr(value, "__dataclass_fields__"):
            return {
                field_name: self._canonicalize(
                    getattr(value, field_name)
                )
                for field_name in value.__dataclass_fields__
            }

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("Non-finite value cannot be canonicalized.")
            return value

        return value

    # ----------------------------------------------------------------------
    # TIME
    # ----------------------------------------------------------------------

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================

GlobalFreshnessRecrawlingScaleProof = FreshnessRecrawlingScaleProof
Phase15_6FreshnessRecrawlingScaleProof = FreshnessRecrawlingScaleProof
FreshnessScaleProof = FreshnessRecrawlingScaleProof
RecrawlingScaleProof = FreshnessRecrawlingScaleProof
MassiveFreshnessScaleProof = FreshnessRecrawlingScaleProof


# ============================================================================
# PUBLIC EXPORTS
# ============================================================================

__all__ = [
    # Constants
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

    # Enums
    "FreshnessProofState",
    "FreshnessTestDimension",
    "EvidenceStrength",
    "EvidenceKind",
    "FreshnessScaleBand",
    "ProofDecision",
    "ProofReason",
    "CheckpointType",
    "EventType",

    # Dataclasses
    "FreshnessProofIdentity",
    "FreshnessProofLineage",
    "FreshnessRecrawlingMeasurement",
    "FreshnessProofInput",
    "FreshnessProofPolicy",
    "FreshnessFinding",
    "FreshnessDimensionResult",
    "GlobalFreshnessResult",
    "FreshnessProofCheckpoint",
    "FreshnessProofEvent",
    "FreshnessProofResult",

    # Backend
    "FreshnessProofBackend",
    "InMemoryFreshnessProofBackend",

    # Main proof engine
    "FreshnessRecrawlingScaleProof",

    # Aliases
    "GlobalFreshnessRecrawlingScaleProof",
    "Phase15_6FreshnessRecrawlingScaleProof",
    "FreshnessScaleProof",
    "RecrawlingScaleProof",
    "MassiveFreshnessScaleProof",
]
