"""
OUR SEARCH
Phase 15.2 — Massive Crawler & Discovery Stress Proof

Purpose
-------
Defines the scale-proof architecture for validating the crawler and Web
discovery pipeline under enormous workloads.

Target:
    billions -> trillions of publicly accessible Web resources

Scope:
    discovery
        -> candidate generation
        -> normalization
        -> deduplication
        -> queue admission
        -> distributed processing
        -> completion tracking
        -> restart/recovery
        -> throughput validation
        -> resource-pressure validation

This module is a validation/control-plane architecture.

It does NOT:
    - perform HTTP requests
    - crawl arbitrary external systems
    - fetch Web pages
    - assign real production workers
    - execute crawler jobs
    - mutate a production index
    - attack or scan external systems
    - execute malware
    - enforce external-resource changes
    - depend on Google APIs, indexes, crawlers, infrastructure, or ranking

The architecture consumes externally supplied measurements and stress-test
evidence and evaluates whether the discovery/crawler subsystem satisfies
configured scale-proof requirements.

Important:
-----------
Passing this stage means the supplied crawler/discovery evidence satisfies
the configured validation policy.

It does NOT mean that OUR SEARCH has already crawled billions or trillions
of real public-Web resources. Actual Web coverage requires sustained
production operation against the public Web.
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

ARCHITECTURE_VERSION = "massive-crawler-discovery-stress-proof.v1"

PHASE = "15.2"
PREVIOUS_STAGE = "15.1"
NEXT_STAGE = "15.3"

PHASE_NAME = "Full Scale-Proof Program"
STAGE_NAME = "Massive Crawler & Discovery Stress Proof"
NEXT_STAGE_NAME = "Massive Index & Storage Stress Proof"

MAX_RESOURCE_COUNT = 10**18
MAX_WORKER_COUNT = 10**12
MAX_QUEUE_DEPTH = 10**18
MAX_THROUGHPUT = 10**18

DEFAULT_MIN_RESOURCE_COUNT = 1_000_000
DEFAULT_MIN_CANDIDATE_THROUGHPUT = 1_000
DEFAULT_MIN_QUEUE_COMPLETION = 0.95
DEFAULT_MIN_DEDUPLICATION = 0.95
DEFAULT_MIN_DURABILITY = 0.95
DEFAULT_MIN_RESTART_RECOVERY = 0.95
DEFAULT_MIN_CONCURRENCY = 0.95
DEFAULT_MIN_RESOURCE_EFFICIENCY = 0.80
DEFAULT_MAX_LOSS_RATE = 0.01
DEFAULT_MAX_DUPLICATE_RATE = 0.05
DEFAULT_MAX_ERROR_RATE = 0.05


# ============================================================================
# ENUMERATIONS
# ============================================================================


class StressProofState(str, Enum):
    CREATED = "created"
    COLLECTING = "collecting"
    NORMALIZED = "normalized"
    EVALUATED = "evaluated"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class StressTestDimension(str, Enum):
    CANDIDATE_VOLUME = "candidate_volume"
    CANDIDATE_THROUGHPUT = "candidate_throughput"
    CONCURRENCY = "concurrency"
    QUEUE_PRESSURE = "queue_pressure"
    DEDUPLICATION = "deduplication"
    COMPLETION = "completion"
    DURABILITY = "durability"
    RESTART_RECOVERY = "restart_recovery"
    RESOURCE_EFFICIENCY = "resource_efficiency"
    ERROR_HANDLING = "error_handling"
    LOSS_PREVENTION = "loss_prevention"
    BACKPRESSURE = "backpressure"
    PARTITION_STABILITY = "partition_stability"


class EvidenceStrength(str, Enum):
    OBSERVED = "observed"
    VERIFIED = "verified"
    DERIVED = "derived"
    SIMULATED = "simulated"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class EvidenceKind(str, Enum):
    VOLUME = "volume"
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    CONCURRENCY = "concurrency"
    QUEUE = "queue"
    DEDUPLICATION = "deduplication"
    COMPLETION = "completion"
    DURABILITY = "durability"
    RECOVERY = "recovery"
    RESOURCE_USAGE = "resource_usage"
    ERROR = "error"
    LOSS = "loss"
    BACKPRESSURE = "backpressure"
    PARTITION = "partition"


class ScaleBand(str, Enum):
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
    INSUFFICIENT_VOLUME = "insufficient_volume"
    THROUGHPUT_TOO_LOW = "throughput_too_low"
    CONCURRENCY_TOO_LOW = "concurrency_too_low"
    QUEUE_COMPLETION_TOO_LOW = "queue_completion_too_low"
    DEDUPLICATION_TOO_LOW = "deduplication_too_low"
    DURABILITY_TOO_LOW = "durability_too_low"
    RESTART_RECOVERY_TOO_LOW = "restart_recovery_too_low"
    RESOURCE_EFFICIENCY_TOO_LOW = "resource_efficiency_too_low"
    ERROR_RATE_TOO_HIGH = "error_rate_too_high"
    LOSS_RATE_TOO_HIGH = "loss_rate_too_high"
    DUPLICATE_RATE_TOO_HIGH = "duplicate_rate_too_high"
    BACKPRESSURE_FAILURE = "backpressure_failure"
    PARTITION_INSTABILITY = "partition_instability"
    INVALID_MEASUREMENT = "invalid_measurement"
    PARTIAL_EVIDENCE = "partial_evidence"


class CheckpointType(str, Enum):
    PROOF_CREATED = "proof_created"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    DIMENSION_VALIDATED = "dimension_validated"
    STRESS_EVALUATED = "stress_evaluated"
    PROOF_FINALIZED = "proof_finalized"


class EventType(str, Enum):
    PROOF_CREATED = "proof_created"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    EVIDENCE_REJECTED = "evidence_rejected"
    DIMENSION_EVALUATED = "dimension_evaluated"
    STRESS_EVALUATED = "stress_evaluated"
    PROOF_PASSED = "proof_passed"
    PROOF_FAILED = "proof_failed"
    PROOF_DEFERRED = "proof_deferred"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass(frozen=True)
class StressProofIdentity:
    proof_id: str
    evaluation_id: str
    created_at: str
    schema_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class StressProofLineage:
    source_systems: Tuple[str, ...] = ()
    source_phases: Tuple[str, ...] = ()
    source_stages: Tuple[str, ...] = ()
    parent_evaluation_ids: Tuple[str, ...] = ()
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class CrawlerStressMeasurement:
    dimension: StressTestDimension

    resource_count: int
    candidate_count: int

    throughput_per_second: float
    peak_queue_depth: int
    queue_completion_rate: float

    deduplication_rate: float
    completion_rate: float
    durability_rate: float
    restart_recovery_rate: float
    concurrency_efficiency: float
    resource_efficiency: float

    error_rate: float
    loss_rate: float
    duplicate_rate: float

    backpressure_stable: bool
    partition_stable: bool

    worker_count: int = 0
    measurement_window_seconds: float = 0.0

    evidence_strength: EvidenceStrength = EvidenceStrength.OBSERVED
    source: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StressProofInput:
    identity: StressProofIdentity
    lineage: StressProofLineage

    measurements: Tuple[CrawlerStressMeasurement, ...]

    target_resource_count: int
    target_candidate_count: int

    public_web_scope: bool = True
    partial_evidence_allowed: bool = True

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StressProofPolicy:
    minimum_resource_count: int = DEFAULT_MIN_RESOURCE_COUNT
    minimum_candidate_count: int = DEFAULT_MIN_RESOURCE_COUNT

    minimum_candidate_throughput: float = DEFAULT_MIN_CANDIDATE_THROUGHPUT

    minimum_queue_completion_rate: float = DEFAULT_MIN_QUEUE_COMPLETION
    minimum_deduplication_rate: float = DEFAULT_MIN_DEDUPLICATION
    minimum_completion_rate: float = DEFAULT_MIN_QUEUE_COMPLETION
    minimum_durability_rate: float = DEFAULT_MIN_DURABILITY
    minimum_restart_recovery_rate: float = DEFAULT_MIN_RESTART_RECOVERY
    minimum_concurrency_efficiency: float = DEFAULT_MIN_CONCURRENCY
    minimum_resource_efficiency: float = DEFAULT_MIN_RESOURCE_EFFICIENCY

    maximum_error_rate: float = DEFAULT_MAX_ERROR_RATE
    maximum_loss_rate: float = DEFAULT_MAX_LOSS_RATE
    maximum_duplicate_rate: float = DEFAULT_MAX_DUPLICATE_RATE

    require_backpressure_stability: bool = True
    require_partition_stability: bool = True
    require_all_dimensions: bool = True

    allow_partial_evidence: bool = True
    require_public_web_scope: bool = True
    require_deterministic_evaluation: bool = True


@dataclass(frozen=True)
class StressFinding:
    dimension: Optional[StressTestDimension]
    reason: ProofReason
    message: str

    measured_value: Optional[float] = None
    required_value: Optional[float] = None

    evidence_strength: EvidenceStrength = EvidenceStrength.UNKNOWN
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class DimensionProofResult:
    dimension: StressTestDimension
    passed: bool

    resource_count: int
    candidate_count: int

    throughput_per_second: float
    peak_queue_depth: int
    queue_completion_rate: float

    deduplication_rate: float
    completion_rate: float
    durability_rate: float
    restart_recovery_rate: float
    concurrency_efficiency: float
    resource_efficiency: float

    error_rate: float
    loss_rate: float
    duplicate_rate: float

    backpressure_stable: bool
    partition_stable: bool

    findings: Tuple[StressFinding, ...] = ()


@dataclass(frozen=True)
class GlobalStressResult:
    passed: bool

    dimensions_evaluated: int
    expected_dimensions: int

    aggregate_throughput: float
    aggregate_completion: float
    aggregate_deduplication: float
    aggregate_durability: float
    aggregate_recovery: float
    aggregate_concurrency: float
    aggregate_resource_efficiency: float

    aggregate_error_rate: float
    aggregate_loss_rate: float
    aggregate_duplicate_rate: float

    findings: Tuple[StressFinding, ...] = ()


@dataclass(frozen=True)
class StressProofCheckpoint:
    checkpoint_id: str
    checkpoint_type: CheckpointType

    timestamp: str

    proof_id: str
    evaluation_id: str

    payload_digest: str
    sequence_number: int

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StressProofEvent:
    event_id: str
    event_type: EventType

    timestamp: str

    proof_id: str
    evaluation_id: str

    payload_digest: str

    dimension: Optional[StressTestDimension] = None

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StressProofResult:
    identity: StressProofIdentity
    lineage: StressProofLineage

    state: StressProofState
    decision: ProofDecision

    target_resource_count: int
    target_candidate_count: int

    evaluated_resource_count: int
    evaluated_candidate_count: int

    scale_band: ScaleBand

    dimension_results: Tuple[DimensionProofResult, ...]
    global_result: GlobalStressResult

    findings: Tuple[StressFinding, ...]

    overall_score: float

    partial_evidence: bool
    deterministic: bool

    checkpoints: Tuple[StressProofCheckpoint, ...] = ()
    events: Tuple[StressProofEvent, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class StressProofBackend(Protocol):
    def save_result(self, result: StressProofResult) -> None:
        ...

    def load_result(self, evaluation_id: str) -> Optional[StressProofResult]:
        ...

    def save_checkpoint(self, checkpoint: StressProofCheckpoint) -> None:
        ...

    def save_event(self, event: StressProofEvent) -> None:
        ...


class InMemoryStressProofBackend:
    """
    Deterministic reference backend.

    Production systems may replace this with a distributed durable backend.
    """

    def __init__(self) -> None:
        self._results: Dict[str, StressProofResult] = {}
        self._checkpoints: Dict[str, StressProofCheckpoint] = {}
        self._events: Dict[str, StressProofEvent] = {}

    def save_result(self, result: StressProofResult) -> None:
        self._results[result.identity.evaluation_id] = result

    def load_result(
        self,
        evaluation_id: str,
    ) -> Optional[StressProofResult]:
        return self._results.get(evaluation_id)

    def save_checkpoint(
        self,
        checkpoint: StressProofCheckpoint,
    ) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(
        self,
        event: StressProofEvent,
    ) -> None:
        self._events[event.event_id] = event


# ============================================================================
# ARCHITECTURE
# ============================================================================


class MassiveCrawlerDiscoveryStressProof:
    """
    Phase 15.2 crawler/discovery stress-proof control plane.

    It validates externally supplied stress-test measurements.

    It does not execute crawler workloads.
    """

    REQUIRED_DIMENSIONS: Tuple[StressTestDimension, ...] = (
        StressTestDimension.CANDIDATE_VOLUME,
        StressTestDimension.CANDIDATE_THROUGHPUT,
        StressTestDimension.CONCURRENCY,
        StressTestDimension.QUEUE_PRESSURE,
        StressTestDimension.DEDUPLICATION,
        StressTestDimension.COMPLETION,
        StressTestDimension.DURABILITY,
        StressTestDimension.RESTART_RECOVERY,
        StressTestDimension.RESOURCE_EFFICIENCY,
        StressTestDimension.ERROR_HANDLING,
        StressTestDimension.LOSS_PREVENTION,
        StressTestDimension.BACKPRESSURE,
        StressTestDimension.PARTITION_STABILITY,
    )

    def __init__(
        self,
        backend: Optional[StressProofBackend] = None,
        policy: Optional[StressProofPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryStressProofBackend()
        self.policy = policy or StressProofPolicy()

    # ------------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------------

    def evaluate(
        self,
        proof_input: StressProofInput,
    ) -> StressProofResult:
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
            EventType.STRESS_EVALUATED,
        )

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.STRESS_EVALUATED,
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

        evaluated_candidate_count = self._evaluated_candidate_count(
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

        result = StressProofResult(
            identity=normalized.identity,
            lineage=normalized.lineage,

            state=state,
            decision=decision,

            target_resource_count=normalized.target_resource_count,
            target_candidate_count=normalized.target_candidate_count,

            evaluated_resource_count=evaluated_resource_count,
            evaluated_candidate_count=evaluated_candidate_count,

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
        inputs: Iterable[StressProofInput],
    ) -> Tuple[StressProofResult, ...]:
        return tuple(
            self.evaluate(item)
            for item in inputs
        )

    def get_result(
        self,
        evaluation_id: str,
    ) -> Optional[StressProofResult]:
        return self.backend.load_result(evaluation_id)

    # ------------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------------

    def _validate_input(
        self,
        proof_input: StressProofInput,
    ) -> None:
        if not proof_input.identity.proof_id.strip():
            raise ValueError("proof_id must not be empty")

        if not proof_input.identity.evaluation_id.strip():
            raise ValueError("evaluation_id must not be empty")

        if not 0 <= proof_input.target_resource_count <= MAX_RESOURCE_COUNT:
            raise ValueError("invalid target_resource_count")

        if not 0 <= proof_input.target_candidate_count <= MAX_RESOURCE_COUNT:
            raise ValueError("invalid target_candidate_count")

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
        measurement: CrawlerStressMeasurement,
    ) -> None:
        integer_fields = (
            ("resource_count", measurement.resource_count),
            ("candidate_count", measurement.candidate_count),
            ("peak_queue_depth", measurement.peak_queue_depth),
            ("worker_count", measurement.worker_count),
        )

        for name, value in integer_fields:
            if isinstance(value, bool):
                raise ValueError(f"{name} must be an integer")

            if value < 0:
                raise ValueError(
                    f"{name} must be non-negative"
                )

        if measurement.resource_count > MAX_RESOURCE_COUNT:
            raise ValueError("resource_count exceeds supported bound")

        if measurement.candidate_count > MAX_RESOURCE_COUNT:
            raise ValueError("candidate_count exceeds supported bound")

        if measurement.peak_queue_depth > MAX_QUEUE_DEPTH:
            raise ValueError("peak_queue_depth exceeds supported bound")

        if measurement.worker_count > MAX_WORKER_COUNT:
            raise ValueError("worker_count exceeds supported bound")

        numeric_fields = (
            ("throughput_per_second", measurement.throughput_per_second),
            ("queue_completion_rate", measurement.queue_completion_rate),
            ("deduplication_rate", measurement.deduplication_rate),
            ("completion_rate", measurement.completion_rate),
            ("durability_rate", measurement.durability_rate),
            ("restart_recovery_rate", measurement.restart_recovery_rate),
            ("concurrency_efficiency", measurement.concurrency_efficiency),
            ("resource_efficiency", measurement.resource_efficiency),
            ("error_rate", measurement.error_rate),
            ("loss_rate", measurement.loss_rate),
            ("duplicate_rate", measurement.duplicate_rate),
            (
                "measurement_window_seconds",
                measurement.measurement_window_seconds,
            ),
        )

        for name, value in numeric_fields:
            if isinstance(value, bool):
                raise ValueError(f"{name} must be numeric")

            if not math.isfinite(float(value)):
                raise ValueError(f"{name} must be finite")

        if measurement.throughput_per_second < 0:
            raise ValueError(
                "throughput_per_second must be non-negative"
            )

        if measurement.measurement_window_seconds < 0:
            raise ValueError(
                "measurement_window_seconds must be non-negative"
            )

        bounded = (
            ("queue_completion_rate", measurement.queue_completion_rate),
            ("deduplication_rate", measurement.deduplication_rate),
            ("completion_rate", measurement.completion_rate),
            ("durability_rate", measurement.durability_rate),
            ("restart_recovery_rate", measurement.restart_recovery_rate),
            ("concurrency_efficiency", measurement.concurrency_efficiency),
            ("resource_efficiency", measurement.resource_efficiency),
            ("error_rate", measurement.error_rate),
            ("loss_rate", measurement.loss_rate),
            ("duplicate_rate", measurement.duplicate_rate),
        )

        for name, value in bounded:
            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"{name} must be between 0 and 1"
                )

    # ------------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------------

    def _normalize_input(
        self,
        proof_input: StressProofInput,
    ) -> StressProofInput:
        ordered = tuple(
            sorted(
                proof_input.measurements,
                key=lambda item: item.dimension.value,
            )
        )

        return StressProofInput(
            identity=proof_input.identity,
            lineage=proof_input.lineage,

            measurements=ordered,

            target_resource_count=proof_input.target_resource_count,
            target_candidate_count=proof_input.target_candidate_count,

            public_web_scope=proof_input.public_web_scope,
            partial_evidence_allowed=proof_input.partial_evidence_allowed,

            metadata=dict(proof_input.metadata),
        )

    # ------------------------------------------------------------------------
    # Dimension evaluation
    # ------------------------------------------------------------------------

    def _evaluate_dimension(
        self,
        measurement: CrawlerStressMeasurement,
    ) -> DimensionProofResult:
        findings = []

        if measurement.resource_count < self.policy.minimum_resource_count:
            findings.append(
                StressFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.INSUFFICIENT_VOLUME,
                    message=(
                        "Resource volume is below the configured "
                        "stress-proof floor."
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

        if measurement.candidate_count < self.policy.minimum_candidate_count:
            findings.append(
                StressFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.INSUFFICIENT_VOLUME,
                    message=(
                        "Candidate volume is below the configured "
                        "stress-proof floor."
                    ),
                    measured_value=float(
                        measurement.candidate_count
                    ),
                    required_value=float(
                        self.policy.minimum_candidate_count
                    ),
                    evidence_strength=measurement.evidence_strength,
                )
            )

        if (
            measurement.throughput_per_second
            < self.policy.minimum_candidate_throughput
        ):
            findings.append(
                StressFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.THROUGHPUT_TOO_LOW,
                    message=(
                        "Candidate throughput is below the configured "
                        "minimum."
                    ),
                    measured_value=measurement.throughput_per_second,
                    required_value=self.policy.minimum_candidate_throughput,
                    evidence_strength=measurement.evidence_strength,
                )
            )

        self._minimum(
            findings,
            measurement,
            "queue_completion_rate",
            measurement.queue_completion_rate,
            self.policy.minimum_queue_completion_rate,
            ProofReason.QUEUE_COMPLETION_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "deduplication_rate",
            measurement.deduplication_rate,
            self.policy.minimum_deduplication_rate,
            ProofReason.DEDUPLICATION_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "completion_rate",
            measurement.completion_rate,
            self.policy.minimum_completion_rate,
            ProofReason.QUEUE_COMPLETION_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "durability_rate",
            measurement.durability_rate,
            self.policy.minimum_durability_rate,
            ProofReason.DURABILITY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "restart_recovery_rate",
            measurement.restart_recovery_rate,
            self.policy.minimum_restart_recovery_rate,
            ProofReason.RESTART_RECOVERY_TOO_LOW,
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
            "resource_efficiency",
            measurement.resource_efficiency,
            self.policy.minimum_resource_efficiency,
            ProofReason.RESOURCE_EFFICIENCY_TOO_LOW,
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
            "loss_rate",
            measurement.loss_rate,
            self.policy.maximum_loss_rate,
            ProofReason.LOSS_RATE_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "duplicate_rate",
            measurement.duplicate_rate,
            self.policy.maximum_duplicate_rate,
            ProofReason.DUPLICATE_RATE_TOO_HIGH,
        )

        if (
            self.policy.require_backpressure_stability
            and not measurement.backpressure_stable
        ):
            findings.append(
                StressFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.BACKPRESSURE_FAILURE,
                    message=(
                        "Backpressure stability was not demonstrated."
                    ),
                    evidence_strength=measurement.evidence_strength,
                )
            )

        if (
            self.policy.require_partition_stability
            and not measurement.partition_stable
        ):
            findings.append(
                StressFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.PARTITION_INSTABILITY,
                    message=(
                        "Partition stability was not demonstrated."
                    ),
                    evidence_strength=measurement.evidence_strength,
                )
            )

        if (
            measurement.evidence_strength == EvidenceStrength.UNKNOWN
        ):
            findings.append(
                StressFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.MISSING_EVIDENCE,
                    message="Evidence strength is unknown.",
                    evidence_strength=measurement.evidence_strength,
                )
            )

        if (
            measurement.evidence_strength == EvidenceStrength.PARTIAL
            and not self.policy.allow_partial_evidence
        ):
            findings.append(
                StressFinding(
                    dimension=measurement.dimension,
                    reason=ProofReason.PARTIAL_EVIDENCE,
                    message="Partial evidence is not allowed.",
                    evidence_strength=measurement.evidence_strength,
                )
            )

        return DimensionProofResult(
            dimension=measurement.dimension,
            passed=len(findings) == 0,

            resource_count=measurement.resource_count,
            candidate_count=measurement.candidate_count,

            throughput_per_second=measurement.throughput_per_second,
            peak_queue_depth=measurement.peak_queue_depth,
            queue_completion_rate=measurement.queue_completion_rate,

            deduplication_rate=measurement.deduplication_rate,
            completion_rate=measurement.completion_rate,
            durability_rate=measurement.durability_rate,
            restart_recovery_rate=measurement.restart_recovery_rate,
            concurrency_efficiency=measurement.concurrency_efficiency,
            resource_efficiency=measurement.resource_efficiency,

            error_rate=measurement.error_rate,
            loss_rate=measurement.loss_rate,
            duplicate_rate=measurement.duplicate_rate,

            backpressure_stable=measurement.backpressure_stable,
            partition_stable=measurement.partition_stable,

            findings=tuple(findings),
        )

    def _minimum(
        self,
        findings: list[StressFinding],
        measurement: CrawlerStressMeasurement,
        name: str,
        measured: float,
        required: float,
        reason: ProofReason,
    ) -> None:
        if measured < required:
            findings.append(
                StressFinding(
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
        findings: list[StressFinding],
        measurement: CrawlerStressMeasurement,
        name: str,
        measured: float,
        maximum: float,
        reason: ProofReason,
    ) -> None:
        if measured > maximum:
            findings.append(
                StressFinding(
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
        proof_input: StressProofInput,
        dimension_results: Sequence[DimensionProofResult],
    ) -> GlobalStressResult:
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
                    StressFinding(
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
                StressFinding(
                    dimension=None,
                    reason=ProofReason.MISSING_EVIDENCE,
                    message="No stress measurements were supplied.",
                )
            )

            return GlobalStressResult(
                passed=False,
                dimensions_evaluated=0,
                expected_dimensions=len(self.REQUIRED_DIMENSIONS),

                aggregate_throughput=0.0,
                aggregate_completion=0.0,
                aggregate_deduplication=0.0,
                aggregate_durability=0.0,
                aggregate_recovery=0.0,
                aggregate_concurrency=0.0,
                aggregate_resource_efficiency=0.0,

                aggregate_error_rate=1.0,
                aggregate_loss_rate=1.0,
                aggregate_duplicate_rate=1.0,

                findings=tuple(findings),
            )

        aggregate = {
            "throughput": self._mean(
                result.throughput_per_second
                for result in dimension_results
            ),
            "completion": self._mean(
                result.completion_rate
                for result in dimension_results
            ),
            "deduplication": self._mean(
                result.deduplication_rate
                for result in dimension_results
            ),
            "durability": self._mean(
                result.durability_rate
                for result in dimension_results
            ),
            "recovery": self._mean(
                result.restart_recovery_rate
                for result in dimension_results
            ),
            "concurrency": self._mean(
                result.concurrency_efficiency
                for result in dimension_results
            ),
            "resource_efficiency": self._mean(
                result.resource_efficiency
                for result in dimension_results
            ),
            "error_rate": self._mean(
                result.error_rate
                for result in dimension_results
            ),
            "loss_rate": self._mean(
                result.loss_rate
                for result in dimension_results
            ),
            "duplicate_rate": self._mean(
                result.duplicate_rate
                for result in dimension_results
            ),
        }

        return GlobalStressResult(
            passed=len(findings) == 0,

            dimensions_evaluated=len(dimension_results),
            expected_dimensions=len(self.REQUIRED_DIMENSIONS),

            aggregate_throughput=aggregate["throughput"],
            aggregate_completion=aggregate["completion"],
            aggregate_deduplication=aggregate["deduplication"],
            aggregate_durability=aggregate["durability"],
            aggregate_recovery=aggregate["recovery"],
            aggregate_concurrency=aggregate["concurrency"],
            aggregate_resource_efficiency=aggregate["resource_efficiency"],

            aggregate_error_rate=aggregate["error_rate"],
            aggregate_loss_rate=aggregate["loss_rate"],
            aggregate_duplicate_rate=aggregate["duplicate_rate"],

            findings=tuple(findings),
        )

    # ------------------------------------------------------------------------
    # Findings and decision
    # ------------------------------------------------------------------------

    def _collect_findings(
        self,
        proof_input: StressProofInput,
        dimension_results: Sequence[DimensionProofResult],
        global_result: GlobalStressResult,
    ) -> list[StressFinding]:
        findings = []

        for result in dimension_results:
            findings.extend(result.findings)

        findings.extend(global_result.findings)

        if (
            self.policy.require_public_web_scope
            and not proof_input.public_web_scope
        ):
            findings.append(
                StressFinding(
                    dimension=None,
                    reason=ProofReason.INVALID_MEASUREMENT,
                    message=(
                        "The evidence is not explicitly scoped to "
                        "the public Web."
                    ),
                )
            )

        return findings

    def _make_decision(
        self,
        proof_input: StressProofInput,
        dimension_results: Sequence[DimensionProofResult],
        global_result: GlobalStressResult,
        findings: Sequence[StressFinding],
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

        if partial and not self.policy.allow_partial_evidence:
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

            if not set(self.REQUIRED_DIMENSIONS).issubset(present):
                return ProofDecision.DEFER

        return ProofDecision.PASS

    @staticmethod
    def _state_from_decision(
        decision: ProofDecision,
    ) -> StressProofState:
        if decision == ProofDecision.PASS:
            return StressProofState.PASSED

        if decision == ProofDecision.FAIL:
            return StressProofState.FAILED

        return StressProofState.DEFERRED

    # ------------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------------

    def _overall_score(
        self,
        dimension_results: Sequence[DimensionProofResult],
        global_result: GlobalStressResult,
        findings: Sequence[StressFinding],
    ) -> float:
        if not dimension_results:
            return 0.0

        dimension_scores = [
            self._dimension_score(result)
            for result in dimension_results
        ]

        base = self._mean(dimension_scores)

        global_quality = self._mean(
            (
                global_result.aggregate_completion,
                global_result.aggregate_deduplication,
                global_result.aggregate_durability,
                global_result.aggregate_recovery,
                global_result.aggregate_concurrency,
                global_result.aggregate_resource_efficiency,
                max(
                    0.0,
                    1.0 - global_result.aggregate_error_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_loss_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_duplicate_rate,
                ),
            )
        )

        score = (base * 0.75) + (global_quality * 0.25)

        if findings:
            penalty = min(
                0.5,
                len(findings) / 100.0,
            )
            score *= max(0.0, 1.0 - penalty)

        return max(0.0, min(1.0, score))

    def _dimension_score(
        self,
        result: DimensionProofResult,
    ) -> float:
        values = (
            result.queue_completion_rate,
            result.deduplication_rate,
            result.completion_rate,
            result.durability_rate,
            result.restart_recovery_rate,
            result.concurrency_efficiency,
            result.resource_efficiency,
            max(0.0, 1.0 - result.error_rate),
            max(0.0, 1.0 - result.loss_rate),
            max(0.0, 1.0 - result.duplicate_rate),
            1.0 if result.backpressure_stable else 0.0,
            1.0 if result.partition_stable else 0.0,
        )

        return self._mean(values)

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
        results: Sequence[DimensionProofResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.resource_count
            for result in results
        )

    @staticmethod
    def _evaluated_candidate_count(
        results: Sequence[DimensionProofResult],
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
    ) -> ScaleBand:
        if resource_count >= 10**12:
            return ScaleBand.TRILLIONS

        if resource_count >= 10**9:
            return ScaleBand.BILLIONS

        if resource_count >= 10**8:
            return ScaleBand.MASSIVE

        if resource_count >= 10**6:
            return ScaleBand.LARGE

        if resource_count > 0:
            return ScaleBand.SMALL

        return ScaleBand.UNKNOWN

    # ------------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------------

    def _checkpoint(
        self,
        checkpoints: list[StressProofCheckpoint],
        proof_input: StressProofInput,
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

        checkpoint = StressProofCheckpoint(
            checkpoint_id=checkpoint_id,
            checkpoint_type=checkpoint_type,
            timestamp=self._now(),

            proof_id=proof_input.identity.proof_id,
            evaluation_id=proof_input.identity.evaluation_id,

            payload_digest=self._digest(
                payload if payload is not None else proof_input
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
        events: list[StressProofEvent],
        proof_input: StressProofInput,
        event_type: EventType,
        dimension: Optional[StressTestDimension] = None,
    ) -> None:
        event_id = self._stable_id(
            "event",
            proof_input.identity.evaluation_id,
            event_type.value,
            dimension.value if dimension else "",
        )

        event = StressProofEvent(
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
        normalized = MassiveCrawlerDiscoveryStressProof._canonicalize(
            value
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
                key: MassiveCrawlerDiscoveryStressProof._canonicalize(
                    getattr(value, key)
                )
                for key in value.__dataclass_fields__
            }

        if isinstance(value, Mapping):
            return {
                str(key): MassiveCrawlerDiscoveryStressProof._canonicalize(
                    item
                )
                for key, item in sorted(
                    value.items(),
                    key=lambda pair: str(pair[0]),
                )
            }

        if isinstance(value, (list, tuple)):
            return [
                MassiveCrawlerDiscoveryStressProof._canonicalize(
                    item
                )
                for item in value
            ]

        if isinstance(value, (set, frozenset)):
            items = [
                MassiveCrawlerDiscoveryStressProof._canonicalize(
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
        return datetime.now(timezone.utc).isoformat()


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================

GlobalCrawlerDiscoveryStressProof = MassiveCrawlerDiscoveryStressProof

Phase15_2MassiveCrawlerDiscoveryStressProof = (
    MassiveCrawlerDiscoveryStressProof
)

CrawlerDiscoveryScaleProof = MassiveCrawlerDiscoveryStressProof

MassiveDiscoveryStressProof = MassiveCrawlerDiscoveryStressProof


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

    "StressProofState",
    "StressTestDimension",
    "EvidenceStrength",
    "EvidenceKind",
    "ScaleBand",
    "ProofDecision",
    "ProofReason",
    "CheckpointType",
    "EventType",

    "StressProofIdentity",
    "StressProofLineage",
    "CrawlerStressMeasurement",
    "StressProofInput",
    "StressProofPolicy",
    "StressFinding",
    "DimensionProofResult",
    "GlobalStressResult",
    "StressProofCheckpoint",
    "StressProofEvent",
    "StressProofResult",

    "StressProofBackend",
    "InMemoryStressProofBackend",

    "MassiveCrawlerDiscoveryStressProof",
    "GlobalCrawlerDiscoveryStressProof",
    "Phase15_2MassiveCrawlerDiscoveryStressProof",
    "CrawlerDiscoveryScaleProof",
    "MassiveDiscoveryStressProof",
]
