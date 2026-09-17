"""
OUR SEARCH
Phase 15.8 — Distributed Failure, Recovery & Resilience Proof

Purpose
-------
Provides an auditable, deterministic, evidence-driven scale-proof system for
validating distributed failure handling, recovery, resilience, durability,
consistency, and service continuity across the OUR SEARCH architecture.

Target
------
billions -> trillions of public-Web resources

This module is a CONTROL-PLANE proof system.

It does NOT:
- execute production workloads
- perform destructive failure injection
- attack infrastructure
- exploit vulnerabilities
- execute malware
- crawl the public Web
- mutate the production index
- depend on Google Search, Google infrastructure, or Google ranking
- require proprietary external infrastructure

Instead, it consumes externally supplied measurements/evidence produced by
authorized infrastructure, reliability, disaster-recovery, chaos-testing,
load-testing, and production-observability systems.

The proof system evaluates:
- distributed worker failures
- shard failures
- regional failures
- storage failures
- queue failures
- coordinator failures
- network partition evidence
- service continuity
- recovery success
- recovery time
- data durability
- work-loss prevention
- duplicate-work prevention
- state consistency
- checkpoint recovery
- queue recovery
- index recovery
- crawler recovery
- retrieval-service recovery
- ranking-service recovery
- freshness-service recovery
- security/quality-service recovery
- overload resilience
- graceful degradation
- failover correctness
- replica availability
- quorum health
- dependency recovery
- restart persistence
- disaster recovery readiness
- recovery automation
- evidence freshness and provenance

The result is deterministic and auditable.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


# ============================================================================
# GLOBAL ARCHITECTURE CONSTANTS
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "distributed-failure-recovery-resilience-proof.v1"

PHASE = "15.8"
PREVIOUS_STAGE = "15.7"
NEXT_STAGE = "15.9"

PHASE_NAME = "Full Scale-Proof Program"
STAGE_NAME = "Distributed Failure, Recovery & Resilience Proof"
NEXT_STAGE_NAME = "Final Global Scale-Proof Gate"

MAX_RESOURCE_COUNT = 10**18
MAX_WORKER_COUNT = 10**12
MAX_SHARD_COUNT = 10**12
MAX_REGION_COUNT = 10**6
MAX_EVENT_COUNT = 10**18
MAX_QUEUE_DEPTH = 10**18
MAX_REPLICA_COUNT = 10**12
MAX_DEPENDENCY_COUNT = 10**12


# ============================================================================
# DEFAULT POLICY
# ============================================================================

DEFAULT_MIN_RESOURCE_COUNT = 1_000_000
DEFAULT_MIN_FAILURE_EVENT_COUNT = 1_000
DEFAULT_MIN_RECOVERY_EVENT_COUNT = 1_000

DEFAULT_MIN_SERVICE_CONTINUITY = 0.99
DEFAULT_MIN_RECOVERY_SUCCESS = 0.98
DEFAULT_MIN_DATA_DURABILITY = 0.999
DEFAULT_MIN_STATE_CONSISTENCY = 0.99
DEFAULT_MIN_FAILOVER_CORRECTNESS = 0.98
DEFAULT_MIN_CHECKPOINT_RECOVERY = 0.99
DEFAULT_MIN_QUEUE_RECOVERY = 0.99
DEFAULT_MIN_INDEX_RECOVERY = 0.99
DEFAULT_MIN_CRAWLER_RECOVERY = 0.98
DEFAULT_MIN_RETRIEVAL_RECOVERY = 0.99
DEFAULT_MIN_RANKING_RECOVERY = 0.99
DEFAULT_MIN_FRESHNESS_RECOVERY = 0.98
DEFAULT_MIN_SECURITY_QUALITY_RECOVERY = 0.99
DEFAULT_MIN_OVERLOAD_RESILIENCE = 0.95
DEFAULT_MIN_GRACEFUL_DEGRADATION = 0.95
DEFAULT_MIN_REPLICA_AVAILABILITY = 0.99
DEFAULT_MIN_QUORUM_HEALTH = 0.99
DEFAULT_MIN_DEPENDENCY_RECOVERY = 0.98
DEFAULT_MIN_AUTOMATED_RECOVERY = 0.95
DEFAULT_MIN_DISASTER_RECOVERY_READINESS = 0.98

DEFAULT_MAX_WORK_LOSS_RATE = 0.01
DEFAULT_MAX_DATA_LOSS_RATE = 0.001
DEFAULT_MAX_DUPLICATE_WORK_RATE = 0.03
DEFAULT_MAX_STATE_DIVERGENCE_RATE = 0.01
DEFAULT_MAX_RECOVERY_FAILURE_RATE = 0.02
DEFAULT_MAX_TIMEOUT_RATE = 0.02
DEFAULT_MAX_ERROR_RATE = 0.02
DEFAULT_MAX_SERVICE_INTERRUPT_RATE = 0.01

DEFAULT_MAX_P50_RECOVERY_MS = 500.0
DEFAULT_MAX_P95_RECOVERY_MS = 5000.0
DEFAULT_MAX_P99_RECOVERY_MS = 15000.0

DEFAULT_MIN_EVIDENCE_FRESHNESS = 0.90


# ============================================================================
# ENUMS
# ============================================================================


class ResilienceProofState(str, Enum):
    CREATED = "created"
    COLLECTING = "collecting"
    NORMALIZED = "normalized"
    EVALUATED = "evaluated"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class ResilienceTestDimension(str, Enum):
    RESOURCE_SCALE = "resource_scale"
    FAILURE_VOLUME = "failure_volume"
    WORKER_FAILURE = "worker_failure"
    SHARD_FAILURE = "shard_failure"
    REGIONAL_FAILURE = "regional_failure"
    STORAGE_FAILURE = "storage_failure"
    QUEUE_FAILURE = "queue_failure"
    COORDINATOR_FAILURE = "coordinator_failure"
    NETWORK_PARTITION = "network_partition"
    SERVICE_CONTINUITY = "service_continuity"
    RECOVERY_SUCCESS = "recovery_success"
    RECOVERY_LATENCY = "recovery_latency"
    DATA_DURABILITY = "data_durability"
    WORK_LOSS_PREVENTION = "work_loss_prevention"
    DUPLICATE_WORK_PREVENTION = "duplicate_work_prevention"
    STATE_CONSISTENCY = "state_consistency"
    CHECKPOINT_RECOVERY = "checkpoint_recovery"
    QUEUE_RECOVERY = "queue_recovery"
    INDEX_RECOVERY = "index_recovery"
    CRAWLER_RECOVERY = "crawler_recovery"
    RETRIEVAL_RECOVERY = "retrieval_recovery"
    RANKING_RECOVERY = "ranking_recovery"
    FRESHNESS_RECOVERY = "freshness_recovery"
    SECURITY_QUALITY_RECOVERY = "security_quality_recovery"
    FAILOVER_CORRECTNESS = "failover_correctness"
    OVERLOAD_RESILIENCE = "overload_resilience"
    GRACEFUL_DEGRADATION = "graceful_degradation"
    REPLICA_AVAILABILITY = "replica_availability"
    QUORUM_HEALTH = "quorum_health"
    DEPENDENCY_RECOVERY = "dependency_recovery"
    RESTART_PERSISTENCE = "restart_persistence"
    AUTOMATED_RECOVERY = "automated_recovery"
    DISASTER_RECOVERY = "disaster_recovery"
    RESOURCE_EFFICIENCY = "resource_efficiency"
    ERROR_HANDLING = "error_handling"
    TIMEOUT_HANDLING = "timeout_handling"
    EVIDENCE_FRESHNESS = "evidence_freshness"


class EvidenceStrength(str, Enum):
    OBSERVED = "observed"
    VERIFIED = "verified"
    DERIVED = "derived"
    SIMULATED = "simulated"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class EvidenceKind(str, Enum):
    SCALE = "scale"
    FAILURE = "failure"
    RECOVERY = "recovery"
    LATENCY = "latency"
    DURABILITY = "durability"
    CONSISTENCY = "consistency"
    FAILOVER = "failover"
    OVERLOAD = "overload"
    DEGRADATION = "degradation"
    REPLICA = "replica"
    QUORUM = "quorum"
    DEPENDENCY = "dependency"
    DISASTER_RECOVERY = "disaster_recovery"
    RESOURCE_USAGE = "resource_usage"
    ERROR = "error"
    TIMEOUT = "timeout"
    FRESHNESS = "freshness"


class ResilienceScaleBand(str, Enum):
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
    NO_DIMENSIONS = "no_dimensions"
    INSUFFICIENT_VOLUME = "insufficient_volume"
    MISSING_REQUIRED_DIMENSIONS = "missing_required_dimensions"
    PARTIAL_EVIDENCE_DISALLOWED = "partial_evidence_disallowed"
    SIMULATED_EVIDENCE_DISALLOWED = "simulated_evidence_disallowed"
    LOW_CONTINUITY = "low_service_continuity"
    LOW_RECOVERY_SUCCESS = "low_recovery_success"
    HIGH_RECOVERY_LATENCY = "high_recovery_latency"
    LOW_DURABILITY = "low_data_durability"
    HIGH_WORK_LOSS = "high_work_loss"
    HIGH_DUPLICATE_WORK = "high_duplicate_work"
    HIGH_STATE_DIVERGENCE = "high_state_divergence"
    LOW_FAILOVER_CORRECTNESS = "low_failover_correctness"
    LOW_RECOVERY_READINESS = "low_recovery_readiness"
    LOW_OVERLOAD_RESILIENCE = "low_overload_resilience"
    LOW_GRACEFUL_DEGRADATION = "low_graceful_degradation"
    LOW_REPLICA_AVAILABILITY = "low_replica_availability"
    LOW_QUORUM_HEALTH = "low_quorum_health"
    LOW_DEPENDENCY_RECOVERY = "low_dependency_recovery"
    LOW_AUTOMATED_RECOVERY = "low_automated_recovery"
    LOW_DISASTER_RECOVERY = "low_disaster_recovery"
    HIGH_ERROR_RATE = "high_error_rate"
    HIGH_TIMEOUT_RATE = "high_timeout_rate"
    LOW_EVIDENCE_FRESHNESS = "low_evidence_freshness"
    INVALID_INPUT = "invalid_input"
    INVALID_MEASUREMENT = "invalid_measurement"
    PUBLIC_WEB_SCOPE_REQUIRED = "public_web_scope_required"
    GLOBAL_RESULT_FAILED = "global_result_failed"


class CheckpointType(str, Enum):
    PROOF_CREATED = "proof_created"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    DIMENSION_VALIDATED = "dimension_validated"
    RESILIENCE_EVALUATED = "resilience_evaluated"
    RECOVERY_EVALUATED = "recovery_evaluated"
    PROOF_FINALIZED = "proof_finalized"


class EventType(str, Enum):
    PROOF_CREATED = "proof_created"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    EVIDENCE_REJECTED = "evidence_rejected"
    DIMENSION_EVALUATED = "dimension_evaluated"
    RECOVERY_EVALUATED = "recovery_evaluated"
    PROOF_PASSED = "proof_passed"
    PROOF_FAILED = "proof_failed"
    PROOF_DEFERRED = "proof_deferred"


# ============================================================================
# DATACLASSES
# ============================================================================


@dataclass(frozen=True)
class ResilienceProofIdentity:
    proof_id: str
    architecture_version: str
    phase: str
    created_at: str
    target_scope: str


@dataclass(frozen=True)
class ResilienceProofLineage:
    previous_stage: str
    next_stage: str
    parent_proof_id: Optional[str] = None
    source_system: str = "our_search"
    lineage_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class ResilienceMeasurement:
    """
    External evidence describing one distributed resilience measurement.

    A single measurement may represent one or more dimensions. The
    `dimension` field identifies the dimension being evaluated.
    """

    dimension: ResilienceTestDimension

    resource_count: int = 0
    worker_count: int = 0
    shard_count: int = 0
    region_count: int = 0

    failure_event_count: int = 0
    recovery_event_count: int = 0

    service_continuity: Optional[float] = None
    recovery_success: Optional[float] = None

    p50_recovery_ms: Optional[float] = None
    p95_recovery_ms: Optional[float] = None
    p99_recovery_ms: Optional[float] = None

    data_durability: Optional[float] = None
    work_loss_rate: Optional[float] = None
    duplicate_work_rate: Optional[float] = None
    state_divergence_rate: Optional[float] = None

    checkpoint_recovery: Optional[float] = None
    queue_recovery: Optional[float] = None
    index_recovery: Optional[float] = None
    crawler_recovery: Optional[float] = None
    retrieval_recovery: Optional[float] = None
    ranking_recovery: Optional[float] = None
    freshness_recovery: Optional[float] = None
    security_quality_recovery: Optional[float] = None

    failover_correctness: Optional[float] = None
    overload_resilience: Optional[float] = None
    graceful_degradation: Optional[float] = None

    replica_availability: Optional[float] = None
    quorum_health: Optional[float] = None
    dependency_recovery: Optional[float] = None

    restart_persistence: Optional[float] = None
    automated_recovery: Optional[float] = None
    disaster_recovery_readiness: Optional[float] = None

    resource_efficiency: Optional[float] = None
    error_rate: Optional[float] = None
    timeout_rate: Optional[float] = None
    evidence_freshness: Optional[float] = None

    measurement_window_seconds: float = 0.0

    evidence_strength: EvidenceStrength = EvidenceStrength.UNKNOWN
    evidence_kind: EvidenceKind = EvidenceKind.RECOVERY

    source: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResilienceProofInput:
    proof_id: str
    measurements: List[ResilienceMeasurement]

    target_resource_count: int = DEFAULT_MIN_RESOURCE_COUNT
    target_scope: str = "public_web"

    required_dimensions: List[ResilienceTestDimension] = field(
        default_factory=lambda: list(ResilienceTestDimension)
    )

    allow_partial_evidence: bool = False
    allow_simulated_evidence: bool = False
    require_all_dimensions: bool = True

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResilienceProofPolicy:
    min_resource_count: int = DEFAULT_MIN_RESOURCE_COUNT
    min_failure_event_count: int = DEFAULT_MIN_FAILURE_EVENT_COUNT
    min_recovery_event_count: int = DEFAULT_MIN_RECOVERY_EVENT_COUNT

    min_service_continuity: float = DEFAULT_MIN_SERVICE_CONTINUITY
    min_recovery_success: float = DEFAULT_MIN_RECOVERY_SUCCESS
    min_data_durability: float = DEFAULT_MIN_DATA_DURABILITY
    min_failover_correctness: float = DEFAULT_MIN_FAILOVER_CORRECTNESS

    min_checkpoint_recovery: float = DEFAULT_MIN_CHECKPOINT_RECOVERY
    min_queue_recovery: float = DEFAULT_MIN_QUEUE_RECOVERY
    min_index_recovery: float = DEFAULT_MIN_INDEX_RECOVERY
    min_crawler_recovery: float = DEFAULT_MIN_CRAWLER_RECOVERY
    min_retrieval_recovery: float = DEFAULT_MIN_RETRIEVAL_RECOVERY
    min_ranking_recovery: float = DEFAULT_MIN_RANKING_RECOVERY
    min_freshness_recovery: float = DEFAULT_MIN_FRESHNESS_RECOVERY
    min_security_quality_recovery: float = DEFAULT_MIN_SECURITY_QUALITY_RECOVERY

    min_overload_resilience: float = DEFAULT_MIN_OVERLOAD_RESILIENCE
    min_graceful_degradation: float = DEFAULT_MIN_GRACEFUL_DEGRADATION
    min_replica_availability: float = DEFAULT_MIN_REPLICA_AVAILABILITY
    min_quorum_health: float = DEFAULT_MIN_QUORUM_HEALTH
    min_dependency_recovery: float = DEFAULT_MIN_DEPENDENCY_RECOVERY
    min_automated_recovery: float = DEFAULT_MIN_AUTOMATED_RECOVERY
    min_disaster_recovery_readiness: float = (
        DEFAULT_MIN_DISASTER_RECOVERY_READINESS
    )

    max_work_loss_rate: float = DEFAULT_MAX_WORK_LOSS_RATE
    max_data_loss_rate: float = DEFAULT_MAX_DATA_LOSS_RATE
    max_duplicate_work_rate: float = DEFAULT_MAX_DUPLICATE_WORK_RATE
    max_state_divergence_rate: float = DEFAULT_MAX_STATE_DIVERGENCE_RATE
    max_recovery_failure_rate: float = DEFAULT_MAX_RECOVERY_FAILURE_RATE
    max_timeout_rate: float = DEFAULT_MAX_TIMEOUT_RATE
    max_error_rate: float = DEFAULT_MAX_ERROR_RATE
    max_service_interrupt_rate: float = DEFAULT_MAX_SERVICE_INTERRUPT_RATE

    max_p50_recovery_ms: float = DEFAULT_MAX_P50_RECOVERY_MS
    max_p95_recovery_ms: float = DEFAULT_MAX_P95_RECOVERY_MS
    max_p99_recovery_ms: float = DEFAULT_MAX_P99_RECOVERY_MS

    min_evidence_freshness: float = DEFAULT_MIN_EVIDENCE_FRESHNESS

    require_public_web_scope: bool = True


@dataclass
class ResilienceFinding:
    dimension: ResilienceTestDimension
    reason: ProofReason
    message: str
    severity: str = "error"
    observed_value: Optional[float] = None
    required_value: Optional[float] = None
    source: Optional[str] = None


@dataclass
class ResilienceDimensionResult:
    dimension: ResilienceTestDimension
    passed: bool
    score: float
    finding_count: int
    findings: List[ResilienceFinding] = field(default_factory=list)


@dataclass
class GlobalResilienceResult:
    decision: ProofDecision
    passed: bool
    overall_score: float
    scale_band: ResilienceScaleBand
    evaluated_dimensions: int
    passed_dimensions: int
    failed_dimensions: int
    deferred_dimensions: int
    findings: List[ResilienceFinding] = field(default_factory=list)


@dataclass
class ResilienceProofCheckpoint:
    checkpoint_id: str
    proof_id: str
    checkpoint_type: CheckpointType
    state: ResilienceProofState
    timestamp: str
    digest: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResilienceProofEvent:
    event_id: str
    proof_id: str
    event_type: EventType
    timestamp: str
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResilienceProofResult:
    identity: ResilienceProofIdentity
    lineage: ResilienceProofLineage
    state: ResilienceProofState
    decision: ProofDecision

    overall_score: float
    scale_band: ResilienceScaleBand

    dimensions: List[ResilienceDimensionResult]
    global_result: GlobalResilienceResult

    evaluated_resource_count: int
    evaluated_failure_event_count: int
    evaluated_recovery_event_count: int

    findings: List[ResilienceFinding]

    created_at: str
    completed_at: Optional[str]

    digest: str
    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class ResilienceProofBackend(Protocol):
    def save_result(self, result: ResilienceProofResult) -> None:
        ...

    def load_result(self, proof_id: str) -> Optional[ResilienceProofResult]:
        ...

    def save_checkpoint(self, checkpoint: ResilienceProofCheckpoint) -> None:
        ...

    def save_event(self, event: ResilienceProofEvent) -> None:
        ...


class InMemoryResilienceProofBackend:
    def __init__(self) -> None:
        self.results: Dict[str, ResilienceProofResult] = {}
        self.checkpoints: Dict[str, ResilienceProofCheckpoint] = {}
        self.events: Dict[str, ResilienceProofEvent] = {}

    def save_result(self, result: ResilienceProofResult) -> None:
        self.results[result.identity.proof_id] = result

    def load_result(self, proof_id: str) -> Optional[ResilienceProofResult]:
        return self.results.get(proof_id)

    def save_checkpoint(self, checkpoint: ResilienceProofCheckpoint) -> None:
        self.checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(self, event: ResilienceProofEvent) -> None:
        self.events[event.event_id] = event


# ============================================================================
# MAIN PROOF ENGINE
# ============================================================================


class DistributedFailureRecoveryResilienceProof:
    """
    Deterministic proof engine for distributed resilience.

    It evaluates supplied evidence only. It does not generate destructive
    workloads or manipulate production infrastructure.
    """

    def __init__(
        self,
        proof_input: ResilienceProofInput,
        policy: Optional[ResilienceProofPolicy] = None,
        backend: Optional[ResilienceProofBackend] = None,
        lineage: Optional[ResilienceProofLineage] = None,
    ) -> None:
        self.input = proof_input
        self.policy = policy or ResilienceProofPolicy()
        self.backend = backend or InMemoryResilienceProofBackend()

        now = self._now()

        self.identity = ResilienceProofIdentity(
            proof_id=proof_input.proof_id,
            architecture_version=ARCHITECTURE_VERSION,
            phase=PHASE,
            created_at=now,
            target_scope=proof_input.target_scope,
        )

        self.lineage = lineage or ResilienceProofLineage(
            previous_stage=PREVIOUS_STAGE,
            next_stage=NEXT_STAGE,
        )

        self.state = ResilienceProofState.CREATED
        self._result: Optional[ResilienceProofResult] = None

        self._checkpoint(
            CheckpointType.PROOF_CREATED,
            ResilienceProofState.CREATED,
        )

        self._emit_event(
            EventType.PROOF_CREATED,
            {
                "proof_id": self.identity.proof_id,
                "phase": PHASE,
                "architecture_version": ARCHITECTURE_VERSION,
            },
        )

    # ---------------------------------------------------------------------
    # PUBLIC API
    # ---------------------------------------------------------------------

    def evaluate(self) -> ResilienceProofResult:
        self.state = ResilienceProofState.COLLECTING

        self._checkpoint(
            CheckpointType.INPUT_ACCEPTED,
            ResilienceProofState.COLLECTING,
        )

        self._validate_input()

        self._checkpoint(
            CheckpointType.INPUT_NORMALIZED,
            ResilienceProofState.NORMALIZED,
        )

        normalized = self._normalize_input()

        dimension_results: List[ResilienceDimensionResult] = []

        for measurement in normalized.measurements:
            result = self._evaluate_dimension(measurement)
            dimension_results.append(result)

            self._emit_event(
                EventType.DIMENSION_EVALUATED,
                {
                    "dimension": measurement.dimension.value,
                    "passed": result.passed,
                    "score": result.score,
                    "finding_count": result.finding_count,
                },
            )

        self._checkpoint(
            CheckpointType.RESILIENCE_EVALUATED,
            ResilienceProofState.EVALUATED,
        )

        global_result = self._evaluate_global(
            normalized,
            dimension_results,
        )

        self._emit_event(
            EventType.RECOVERY_EVALUATED,
            {
                "decision": global_result.decision.value,
                "overall_score": global_result.overall_score,
                "scale_band": global_result.scale_band.value,
            },
        )

        decision = global_result.decision
        self.state = self._state_from_decision(decision)

        created_at = self.identity.created_at
        completed_at = self._now()

        result_without_digest = {
            "identity": asdict(self.identity),
            "lineage": asdict(self.lineage),
            "state": self.state.value,
            "decision": decision.value,
            "overall_score": global_result.overall_score,
            "scale_band": global_result.scale_band.value,
            "dimensions": [
                self._dimension_to_dict(item)
                for item in dimension_results
            ],
            "global_result": self._global_to_dict(global_result),
            "evaluated_resource_count": self._evaluated_resource_count(
                normalized
            ),
            "evaluated_failure_event_count": self._evaluated_failure_count(
                normalized
            ),
            "evaluated_recovery_event_count": self._evaluated_recovery_count(
                normalized
            ),
            "findings": [
                self._finding_to_dict(item)
                for item in global_result.findings
            ],
            "created_at": created_at,
            "completed_at": completed_at,
            "metadata": normalized.metadata,
        }

        digest = self._digest(result_without_digest)

        result = ResilienceProofResult(
            identity=self.identity,
            lineage=self.lineage,
            state=self.state,
            decision=decision,
            overall_score=global_result.overall_score,
            scale_band=global_result.scale_band,
            dimensions=dimension_results,
            global_result=global_result,
            evaluated_resource_count=self._evaluated_resource_count(
                normalized
            ),
            evaluated_failure_event_count=self._evaluated_failure_count(
                normalized
            ),
            evaluated_recovery_event_count=self._evaluated_recovery_count(
                normalized
            ),
            findings=global_result.findings,
            created_at=created_at,
            completed_at=completed_at,
            digest=digest,
            metadata=normalized.metadata,
        )

        self._result = result
        self.backend.save_result(result)

        self._checkpoint(
            CheckpointType.PROOF_FINALIZED,
            self.state,
            metadata={
                "decision": decision.value,
                "digest": digest,
            },
        )

        if decision == ProofDecision.PASS:
            self._emit_event(
                EventType.PROOF_PASSED,
                {
                    "digest": digest,
                    "overall_score": global_result.overall_score,
                },
            )
        elif decision == ProofDecision.FAIL:
            self._emit_event(
                EventType.PROOF_FAILED,
                {
                    "digest": digest,
                    "finding_count": len(global_result.findings),
                },
            )
        else:
            self._emit_event(
                EventType.PROOF_DEFERRED,
                {
                    "digest": digest,
                    "finding_count": len(global_result.findings),
                },
            )

        return result

    def evaluate_many(
        self,
        inputs: Sequence[ResilienceProofInput],
    ) -> List[ResilienceProofResult]:
        results: List[ResilienceProofResult] = []

        for proof_input in inputs:
            proof = DistributedFailureRecoveryResilienceProof(
                proof_input=proof_input,
                policy=self.policy,
                backend=self.backend,
                lineage=self.lineage,
            )
            results.append(proof.evaluate())

        return results

    def get_result(self) -> Optional[ResilienceProofResult]:
        if self._result is not None:
            return self._result

        return self.backend.load_result(self.identity.proof_id)

    # ---------------------------------------------------------------------
    # INPUT VALIDATION
    # ---------------------------------------------------------------------

    def _validate_input(self) -> None:
        if not self.input.proof_id.strip():
            raise ValueError("proof_id must not be empty")

        if not self.input.target_scope.strip():
            raise ValueError("target_scope must not be empty")

        if (
            self.policy.require_public_web_scope
            and self.input.target_scope.lower() != "public_web"
        ):
            raise ValueError(
                "public-web scope is required for this proof"
            )

        if not self.input.measurements:
            return

        seen: set[ResilienceTestDimension] = set()

        for measurement in self.input.measurements:
            if measurement.dimension in seen:
                raise ValueError(
                    f"duplicate dimension: {measurement.dimension.value}"
                )

            seen.add(measurement.dimension)

            self._validate_measurement(measurement)

    def _validate_measurement(
        self,
        measurement: ResilienceMeasurement,
    ) -> None:
        integer_fields = {
            "resource_count": measurement.resource_count,
            "worker_count": measurement.worker_count,
            "shard_count": measurement.shard_count,
            "region_count": measurement.region_count,
            "failure_event_count": measurement.failure_event_count,
            "recovery_event_count": measurement.recovery_event_count,
        }

        for name, value in integer_fields.items():
            if value < 0:
                raise ValueError(f"{name} must be non-negative")

        if measurement.resource_count > MAX_RESOURCE_COUNT:
            raise ValueError("resource_count exceeds maximum")

        if measurement.worker_count > MAX_WORKER_COUNT:
            raise ValueError("worker_count exceeds maximum")

        if measurement.shard_count > MAX_SHARD_COUNT:
            raise ValueError("shard_count exceeds maximum")

        if measurement.region_count > MAX_REGION_COUNT:
            raise ValueError("region_count exceeds maximum")

        if measurement.failure_event_count > MAX_EVENT_COUNT:
            raise ValueError("failure_event_count exceeds maximum")

        if measurement.recovery_event_count > MAX_EVENT_COUNT:
            raise ValueError("recovery_event_count exceeds maximum")

        if measurement.measurement_window_seconds < 0:
            raise ValueError("measurement_window_seconds must be non-negative")

        bounded_fields = {
            "service_continuity": measurement.service_continuity,
            "recovery_success": measurement.recovery_success,
            "data_durability": measurement.data_durability,
            "checkpoint_recovery": measurement.checkpoint_recovery,
            "queue_recovery": measurement.queue_recovery,
            "index_recovery": measurement.index_recovery,
            "crawler_recovery": measurement.crawler_recovery,
            "retrieval_recovery": measurement.retrieval_recovery,
            "ranking_recovery": measurement.ranking_recovery,
            "freshness_recovery": measurement.freshness_recovery,
            "security_quality_recovery": (
                measurement.security_quality_recovery
            ),
            "failover_correctness": measurement.failover_correctness,
            "overload_resilience": measurement.overload_resilience,
            "graceful_degradation": measurement.graceful_degradation,
            "replica_availability": measurement.replica_availability,
            "quorum_health": measurement.quorum_health,
            "dependency_recovery": measurement.dependency_recovery,
            "restart_persistence": measurement.restart_persistence,
            "automated_recovery": measurement.automated_recovery,
            "disaster_recovery_readiness": (
                measurement.disaster_recovery_readiness
            ),
            "resource_efficiency": measurement.resource_efficiency,
            "evidence_freshness": measurement.evidence_freshness,
        }

        for name, value in bounded_fields.items():
            if value is not None:
                self._validate_unit_interval(name, value)

        rate_fields = {
            "work_loss_rate": measurement.work_loss_rate,
            "duplicate_work_rate": measurement.duplicate_work_rate,
            "state_divergence_rate": measurement.state_divergence_rate,
            "error_rate": measurement.error_rate,
            "timeout_rate": measurement.timeout_rate,
        }

        for name, value in rate_fields.items():
            if value is not None:
                self._validate_unit_interval(name, value)

        latency_fields = {
            "p50_recovery_ms": measurement.p50_recovery_ms,
            "p95_recovery_ms": measurement.p95_recovery_ms,
            "p99_recovery_ms": measurement.p99_recovery_ms,
        }

        for name, value in latency_fields.items():
            if value is not None:
                if not math.isfinite(value) or value < 0:
                    raise ValueError(
                        f"{name} must be finite and non-negative"
                    )

        latencies = [
            value
            for value in latency_fields.values()
            if value is not None
        ]

        if latencies != sorted(latencies):
            raise ValueError(
                "recovery latency values must satisfy "
                "p50 <= p95 <= p99"
            )

    @staticmethod
    def _validate_unit_interval(
        name: str,
        value: float,
    ) -> None:
        if not math.isfinite(value):
            raise ValueError(f"{name} must be finite")

        if value < 0.0 or value > 1.0:
            raise ValueError(
                f"{name} must be between 0 and 1"
            )

    # ---------------------------------------------------------------------
    # NORMALIZATION
    # ---------------------------------------------------------------------

    def _normalize_input(self) -> ResilienceProofInput:
        measurements = list(self.input.measurements)

        normalized_measurements: List[ResilienceMeasurement] = []

        for measurement in measurements:
            copied = ResilienceMeasurement(
                **asdict(measurement)
            )
            normalized_measurements.append(copied)

        self.state = ResilienceProofState.NORMALIZED

        return ResilienceProofInput(
            proof_id=self.input.proof_id,
            measurements=normalized_measurements,
            target_resource_count=self.input.target_resource_count,
            target_scope=self.input.target_scope,
            required_dimensions=list(self.input.required_dimensions),
            allow_partial_evidence=self.input.allow_partial_evidence,
            allow_simulated_evidence=self.input.allow_simulated_evidence,
            require_all_dimensions=self.input.require_all_dimensions,
            metadata=dict(self.input.metadata),
        )

    # ---------------------------------------------------------------------
    # DIMENSION EVALUATION
    # ---------------------------------------------------------------------

    def _evaluate_dimension(
        self,
        measurement: ResilienceMeasurement,
    ) -> ResilienceDimensionResult:
        findings: List[ResilienceFinding] = []
        scores: List[float] = []

        dimension = measurement.dimension

        if dimension == ResilienceTestDimension.RESOURCE_SCALE:
            self._evaluate_minimum_volume(
                dimension,
                measurement.resource_count,
                self.policy.min_resource_count,
                "resource count",
                findings,
            )
            scores.append(
                self._volume_score(
                    measurement.resource_count,
                    self.policy.min_resource_count,
                )
            )

        elif dimension == ResilienceTestDimension.FAILURE_VOLUME:
            self._evaluate_minimum_volume(
                dimension,
                measurement.failure_event_count,
                self.policy.min_failure_event_count,
                "failure-event count",
                findings,
            )
            scores.append(
                self._volume_score(
                    measurement.failure_event_count,
                    self.policy.min_failure_event_count,
                )
            )

        elif dimension in {
            ResilienceTestDimension.WORKER_FAILURE,
            ResilienceTestDimension.SHARD_FAILURE,
            ResilienceTestDimension.REGIONAL_FAILURE,
            ResilienceTestDimension.STORAGE_FAILURE,
            ResilienceTestDimension.QUEUE_FAILURE,
            ResilienceTestDimension.COORDINATOR_FAILURE,
            ResilienceTestDimension.NETWORK_PARTITION,
        }:
            self._evaluate_recovery_basics(
                measurement,
                findings,
            )
            scores.extend(
                self._recovery_basic_scores(measurement)
            )

        elif dimension == ResilienceTestDimension.SERVICE_CONTINUITY:
            self._require_min(
                dimension,
                measurement.service_continuity,
                self.policy.min_service_continuity,
                "service continuity",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.service_continuity,
                    self.policy.min_service_continuity,
                )
            )

        elif dimension == ResilienceTestDimension.RECOVERY_SUCCESS:
            self._require_min(
                dimension,
                measurement.recovery_success,
                self.policy.min_recovery_success,
                "recovery success",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.recovery_success,
                    self.policy.min_recovery_success,
                )
            )

        elif dimension == ResilienceTestDimension.RECOVERY_LATENCY:
            self._evaluate_latency(
                dimension,
                measurement,
                findings,
            )
            scores.append(
                self._latency_quality(measurement)
            )

        elif dimension == ResilienceTestDimension.DATA_DURABILITY:
            self._require_min(
                dimension,
                measurement.data_durability,
                self.policy.min_data_durability,
                "data durability",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.data_durability,
                    self.policy.min_data_durability,
                )
            )

        elif dimension == ResilienceTestDimension.WORK_LOSS_PREVENTION:
            self._require_max(
                dimension,
                measurement.work_loss_rate,
                self.policy.max_work_loss_rate,
                "work loss rate",
                findings,
            )
            scores.append(
                self._score_max(
                    measurement.work_loss_rate,
                    self.policy.max_work_loss_rate,
                )
            )

        elif dimension == ResilienceTestDimension.DUPLICATE_WORK_PREVENTION:
            self._require_max(
                dimension,
                measurement.duplicate_work_rate,
                self.policy.max_duplicate_work_rate,
                "duplicate work rate",
                findings,
            )
            scores.append(
                self._score_max(
                    measurement.duplicate_work_rate,
                    self.policy.max_duplicate_work_rate,
                )
            )

        elif dimension == ResilienceTestDimension.STATE_CONSISTENCY:
            self._require_max(
                dimension,
                measurement.state_divergence_rate,
                self.policy.max_state_divergence_rate,
                "state divergence rate",
                findings,
            )
            scores.append(
                self._score_max(
                    measurement.state_divergence_rate,
                    self.policy.max_state_divergence_rate,
                )
            )

        elif dimension == ResilienceTestDimension.CHECKPOINT_RECOVERY:
            self._require_min(
                dimension,
                measurement.checkpoint_recovery,
                self.policy.min_checkpoint_recovery,
                "checkpoint recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.checkpoint_recovery,
                    self.policy.min_checkpoint_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.QUEUE_RECOVERY:
            self._require_min(
                dimension,
                measurement.queue_recovery,
                self.policy.min_queue_recovery,
                "queue recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.queue_recovery,
                    self.policy.min_queue_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.INDEX_RECOVERY:
            self._require_min(
                dimension,
                measurement.index_recovery,
                self.policy.min_index_recovery,
                "index recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.index_recovery,
                    self.policy.min_index_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.CRAWLER_RECOVERY:
            self._require_min(
                dimension,
                measurement.crawler_recovery,
                self.policy.min_crawler_recovery,
                "crawler recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.crawler_recovery,
                    self.policy.min_crawler_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.RETRIEVAL_RECOVERY:
            self._require_min(
                dimension,
                measurement.retrieval_recovery,
                self.policy.min_retrieval_recovery,
                "retrieval recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.retrieval_recovery,
                    self.policy.min_retrieval_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.RANKING_RECOVERY:
            self._require_min(
                dimension,
                measurement.ranking_recovery,
                self.policy.min_ranking_recovery,
                "ranking recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.ranking_recovery,
                    self.policy.min_ranking_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.FRESHNESS_RECOVERY:
            self._require_min(
                dimension,
                measurement.freshness_recovery,
                self.policy.min_freshness_recovery,
                "freshness recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.freshness_recovery,
                    self.policy.min_freshness_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.SECURITY_QUALITY_RECOVERY:
            self._require_min(
                dimension,
                measurement.security_quality_recovery,
                self.policy.min_security_quality_recovery,
                "security/quality recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.security_quality_recovery,
                    self.policy.min_security_quality_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.FAILOVER_CORRECTNESS:
            self._require_min(
                dimension,
                measurement.failover_correctness,
                self.policy.min_failover_correctness,
                "failover correctness",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.failover_correctness,
                    self.policy.min_failover_correctness,
                )
            )

        elif dimension == ResilienceTestDimension.OVERLOAD_RESILIENCE:
            self._require_min(
                dimension,
                measurement.overload_resilience,
                self.policy.min_overload_resilience,
                "overload resilience",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.overload_resilience,
                    self.policy.min_overload_resilience,
                )
            )

        elif dimension == ResilienceTestDimension.GRACEFUL_DEGRADATION:
            self._require_min(
                dimension,
                measurement.graceful_degradation,
                self.policy.min_graceful_degradation,
                "graceful degradation",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.graceful_degradation,
                    self.policy.min_graceful_degradation,
                )
            )

        elif dimension == ResilienceTestDimension.REPLICA_AVAILABILITY:
            self._require_min(
                dimension,
                measurement.replica_availability,
                self.policy.min_replica_availability,
                "replica availability",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.replica_availability,
                    self.policy.min_replica_availability,
                )
            )

        elif dimension == ResilienceTestDimension.QUORUM_HEALTH:
            self._require_min(
                dimension,
                measurement.quorum_health,
                self.policy.min_quorum_health,
                "quorum health",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.quorum_health,
                    self.policy.min_quorum_health,
                )
            )

        elif dimension == ResilienceTestDimension.DEPENDENCY_RECOVERY:
            self._require_min(
                dimension,
                measurement.dependency_recovery,
                self.policy.min_dependency_recovery,
                "dependency recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.dependency_recovery,
                    self.policy.min_dependency_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.RESTART_PERSISTENCE:
            self._require_min(
                dimension,
                measurement.restart_persistence,
                self.policy.min_recovery_success,
                "restart persistence",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.restart_persistence,
                    self.policy.min_recovery_success,
                )
            )

        elif dimension == ResilienceTestDimension.AUTOMATED_RECOVERY:
            self._require_min(
                dimension,
                measurement.automated_recovery,
                self.policy.min_automated_recovery,
                "automated recovery",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.automated_recovery,
                    self.policy.min_automated_recovery,
                )
            )

        elif dimension == ResilienceTestDimension.DISASTER_RECOVERY:
            self._require_min(
                dimension,
                measurement.disaster_recovery_readiness,
                self.policy.min_disaster_recovery_readiness,
                "disaster recovery readiness",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.disaster_recovery_readiness,
                    self.policy.min_disaster_recovery_readiness,
                )
            )

        elif dimension == ResilienceTestDimension.RESOURCE_EFFICIENCY:
            self._require_min(
                dimension,
                measurement.resource_efficiency,
                0.80,
                "resource efficiency",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.resource_efficiency,
                    0.80,
                )
            )

        elif dimension == ResilienceTestDimension.ERROR_HANDLING:
            self._require_max(
                dimension,
                measurement.error_rate,
                self.policy.max_error_rate,
                "error rate",
                findings,
            )
            scores.append(
                self._score_max(
                    measurement.error_rate,
                    self.policy.max_error_rate,
                )
            )

        elif dimension == ResilienceTestDimension.TIMEOUT_HANDLING:
            self._require_max(
                dimension,
                measurement.timeout_rate,
                self.policy.max_timeout_rate,
                "timeout rate",
                findings,
            )
            scores.append(
                self._score_max(
                    measurement.timeout_rate,
                    self.policy.max_timeout_rate,
                )
            )

        elif dimension == ResilienceTestDimension.EVIDENCE_FRESHNESS:
            self._require_min(
                dimension,
                measurement.evidence_freshness,
                self.policy.min_evidence_freshness,
                "evidence freshness",
                findings,
            )
            scores.append(
                self._score_min(
                    measurement.evidence_freshness,
                    self.policy.min_evidence_freshness,
                )
            )

        else:
            findings.append(
                ResilienceFinding(
                    dimension=dimension,
                    reason=ProofReason.INVALID_MEASUREMENT,
                    message="Unsupported resilience dimension",
                )
            )

        score = self._mean(scores) if scores else 0.0
        passed = len(findings) == 0

        return ResilienceDimensionResult(
            dimension=dimension,
            passed=passed,
            score=score,
            finding_count=len(findings),
            findings=findings,
        )

    # ---------------------------------------------------------------------
    # RECOVERY BASICS
    # ---------------------------------------------------------------------

    def _evaluate_recovery_basics(
        self,
        measurement: ResilienceMeasurement,
        findings: List[ResilienceFinding],
    ) -> None:
        self._evaluate_minimum_volume(
            measurement.dimension,
            measurement.failure_event_count,
            self.policy.min_failure_event_count,
            "failure-event count",
            findings,
        )

        self._evaluate_minimum_volume(
            measurement.dimension,
            measurement.recovery_event_count,
            self.policy.min_recovery_event_count,
            "recovery-event count",
            findings,
        )

        self._require_min(
            measurement.dimension,
            measurement.recovery_success,
            self.policy.min_recovery_success,
            "recovery success",
            findings,
        )

    def _recovery_basic_scores(
        self,
        measurement: ResilienceMeasurement,
    ) -> List[float]:
        return [
            self._volume_score(
                measurement.failure_event_count,
                self.policy.min_failure_event_count,
            ),
            self._volume_score(
                measurement.recovery_event_count,
                self.policy.min_recovery_event_count,
            ),
            self._score_min(
                measurement.recovery_success,
                self.policy.min_recovery_success,
            ),
        ]

    # ---------------------------------------------------------------------
    # LATENCY
    # ---------------------------------------------------------------------

    def _evaluate_latency(
        self,
        dimension: ResilienceTestDimension,
        measurement: ResilienceMeasurement,
        findings: List[ResilienceFinding],
    ) -> None:
        latency_values = {
            "p50": (
                measurement.p50_recovery_ms,
                self.policy.max_p50_recovery_ms,
            ),
            "p95": (
                measurement.p95_recovery_ms,
                self.policy.max_p95_recovery_ms,
            ),
            "p99": (
                measurement.p99_recovery_ms,
                self.policy.max_p99_recovery_ms,
            ),
        }

        for label, pair in latency_values.items():
            observed, maximum = pair

            if observed is None:
                findings.append(
                    ResilienceFinding(
                        dimension=dimension,
                        reason=ProofReason.HIGH_RECOVERY_LATENCY,
                        message=f"{label} recovery latency is missing",
                        required_value=maximum,
                    )
                )
                continue

            if observed > maximum:
                findings.append(
                    ResilienceFinding(
                        dimension=dimension,
                        reason=ProofReason.HIGH_RECOVERY_LATENCY,
                        message=(
                            f"{label} recovery latency exceeds maximum"
                        ),
                        observed_value=observed,
                        required_value=maximum,
                    )
                )

    def _latency_quality(
        self,
        measurement: ResilienceMeasurement,
    ) -> float:
        values = [
            self._bounded_latency_quality(
                measurement.p50_recovery_ms,
                self.policy.max_p50_recovery_ms,
            ),
            self._bounded_latency_quality(
                measurement.p95_recovery_ms,
                self.policy.max_p95_recovery_ms,
            ),
            self._bounded_latency_quality(
                measurement.p99_recovery_ms,
                self.policy.max_p99_recovery_ms,
            ),
        ]

        return self._mean(values)

    @staticmethod
    def _bounded_latency_quality(
        observed: Optional[float],
        maximum: float,
    ) -> float:
        if observed is None:
            return 0.0

        if observed <= 0:
            return 1.0

        if observed <= maximum:
            return 1.0

        return max(
            0.0,
            maximum / observed,
        )

    # ---------------------------------------------------------------------
    # GLOBAL EVALUATION
    # ---------------------------------------------------------------------

    def _evaluate_global(
        self,
        normalized: ResilienceProofInput,
        dimensions: Sequence[ResilienceDimensionResult],
    ) -> GlobalResilienceResult:
        findings = self._collect_findings(dimensions)

        evaluated_resource_count = self._evaluated_resource_count(
            normalized
        )

        if not dimensions:
            return GlobalResilienceResult(
                decision=ProofDecision.DEFER,
                passed=False,
                overall_score=0.0,
                scale_band=self._scale_band(
                    evaluated_resource_count
                ),
                evaluated_dimensions=0,
                passed_dimensions=0,
                failed_dimensions=0,
                deferred_dimensions=0,
                findings=[
                    ResilienceFinding(
                        dimension=ResilienceTestDimension.RESOURCE_SCALE,
                        reason=ProofReason.NO_DIMENSIONS,
                        message="No resilience dimensions were supplied",
                    )
                ],
            )

        if (
            not normalized.allow_partial_evidence
            and any(
                measurement.evidence_strength
                in {
                    EvidenceStrength.PARTIAL,
                    EvidenceStrength.UNKNOWN,
                }
                for measurement in normalized.measurements
            )
        ):
            findings.append(
                ResilienceFinding(
                    dimension=ResilienceTestDimension.RESOURCE_SCALE,
                    reason=ProofReason.PARTIAL_EVIDENCE_DISALLOWED,
                    message=(
                        "Partial or unknown evidence is not allowed "
                        "by the current proof policy"
                    ),
                )
            )

        if (
            not normalized.allow_simulated_evidence
            and any(
                measurement.evidence_strength
                == EvidenceStrength.SIMULATED
                for measurement in normalized.measurements
            )
        ):
            findings.append(
                ResilienceFinding(
                    dimension=ResilienceTestDimension.RESOURCE_SCALE,
                    reason=ProofReason.SIMULATED_EVIDENCE_DISALLOWED,
                    message=(
                        "Simulated evidence is not allowed "
                        "by the current proof policy"
                    ),
                )
            )

        if evaluated_resource_count < self.policy.min_resource_count:
            findings.append(
                ResilienceFinding(
                    dimension=ResilienceTestDimension.RESOURCE_SCALE,
                    reason=ProofReason.INSUFFICIENT_VOLUME,
                    message=(
                        "Evidence volume is below the minimum "
                        "resource-scale proof threshold"
                    ),
                    observed_value=float(evaluated_resource_count),
                    required_value=float(
                        self.policy.min_resource_count
                    ),
                )
            )

        if normalized.require_all_dimensions:
            supplied = {
                measurement.dimension
                for measurement in normalized.measurements
            }

            missing = [
                dimension
                for dimension in normalized.required_dimensions
                if dimension not in supplied
            ]

            if missing:
                findings.append(
                    ResilienceFinding(
                        dimension=ResilienceTestDimension.RESOURCE_SCALE,
                        reason=ProofReason.MISSING_REQUIRED_DIMENSIONS,
                        message=(
                            "Required resilience dimensions are missing: "
                            + ", ".join(
                                item.value
                                for item in missing
                            )
                        ),
                    )
                )

        passed_dimensions = sum(
            1
            for item in dimensions
            if item.passed
        )

        failed_dimensions = sum(
            1
            for item in dimensions
            if not item.passed
        )

        overall_score = self._overall_score(dimensions)

        if findings:
            decision = ProofDecision.FAIL
        elif failed_dimensions > 0:
            decision = ProofDecision.FAIL
        else:
            decision = ProofDecision.PASS

        return GlobalResilienceResult(
            decision=decision,
            passed=decision == ProofDecision.PASS,
            overall_score=overall_score,
            scale_band=self._scale_band(
                evaluated_resource_count
            ),
            evaluated_dimensions=len(dimensions),
            passed_dimensions=passed_dimensions,
            failed_dimensions=failed_dimensions,
            deferred_dimensions=0,
            findings=findings,
        )

    def _collect_findings(
        self,
        dimensions: Sequence[ResilienceDimensionResult],
    ) -> List[ResilienceFinding]:
        findings: List[ResilienceFinding] = []

        for dimension in dimensions:
            findings.extend(dimension.findings)

        return findings

    # ---------------------------------------------------------------------
    # SCORING
    # ---------------------------------------------------------------------

    @staticmethod
    def _score_min(
        observed: Optional[float],
        minimum: float,
    ) -> float:
        if observed is None:
            return 0.0

        if minimum <= 0:
            return 1.0

        return min(
            1.0,
            max(
                0.0,
                observed / minimum,
            ),
        )

    @staticmethod
    def _score_max(
        observed: Optional[float],
        maximum: float,
    ) -> float:
        if observed is None:
            return 0.0

        if observed <= maximum:
            return 1.0

        if observed <= 1.0:
            return max(
                0.0,
                1.0 - (
                    (observed - maximum)
                    / max(1e-12, 1.0 - maximum)
                ),
            )

        return 0.0

    @staticmethod
    def _volume_score(
        observed: int,
        minimum: int,
    ) -> float:
        if minimum <= 0:
            return 1.0

        if observed >= minimum:
            return 1.0

        if observed <= 0:
            return 0.0

        return min(
            1.0,
            observed / minimum,
        )

    @staticmethod
    def _overall_score(
        dimensions: Sequence[ResilienceDimensionResult],
    ) -> float:
        if not dimensions:
            return 0.0

        return sum(
            item.score
            for item in dimensions
        ) / len(dimensions)

    @staticmethod
    def _mean(values: Iterable[float]) -> float:
        values = list(values)

        if not values:
            return 0.0

        return sum(values) / len(values)

    # ---------------------------------------------------------------------
    # REQUIREMENT HELPERS
    # ---------------------------------------------------------------------

    def _require_min(
        self,
        dimension: ResilienceTestDimension,
        observed: Optional[float],
        minimum: float,
        label: str,
        findings: List[ResilienceFinding],
    ) -> None:
        if observed is None:
            findings.append(
                ResilienceFinding(
                    dimension=dimension,
                    reason=ProofReason.INVALID_MEASUREMENT,
                    message=f"{label} is missing",
                    required_value=minimum,
                )
            )
            return

        if observed < minimum:
            reason = ProofReason.LOW_RECOVERY_SUCCESS

            if "continuity" in label:
                reason = ProofReason.LOW_CONTINUITY
            elif "failover" in label:
                reason = ProofReason.LOW_FAILOVER_CORRECTNESS
            elif "overload" in label:
                reason = ProofReason.LOW_OVERLOAD_RESILIENCE
            elif "degradation" in label:
                reason = ProofReason.LOW_GRACEFUL_DEGRADATION
            elif "replica" in label:
                reason = ProofReason.LOW_REPLICA_AVAILABILITY
            elif "quorum" in label:
                reason = ProofReason.LOW_QUORUM_HEALTH
            elif "dependency" in label:
                reason = ProofReason.LOW_DEPENDENCY_RECOVERY
            elif "automated" in label:
                reason = ProofReason.LOW_AUTOMATED_RECOVERY
            elif "disaster" in label:
                reason = ProofReason.LOW_DISASTER_RECOVERY
            elif "durability" in label:
                reason = ProofReason.LOW_DURABILITY

            findings.append(
                ResilienceFinding(
                    dimension=dimension,
                    reason=reason,
                    message=f"{label} is below minimum",
                    observed_value=observed,
                    required_value=minimum,
                )
            )

    def _require_max(
        self,
        dimension: ResilienceTestDimension,
        observed: Optional[float],
        maximum: float,
        label: str,
        findings: List[ResilienceFinding],
    ) -> None:
        if observed is None:
            findings.append(
                ResilienceFinding(
                    dimension=dimension,
                    reason=ProofReason.INVALID_MEASUREMENT,
                    message=f"{label} is missing",
                    required_value=maximum,
                )
            )
            return

        if observed > maximum:
            reason = ProofReason.HIGH_ERROR_RATE

            if "work loss" in label:
                reason = ProofReason.HIGH_WORK_LOSS
            elif "duplicate" in label:
                reason = ProofReason.HIGH_DUPLICATE_WORK
            elif "divergence" in label:
                reason = ProofReason.HIGH_STATE_DIVERGENCE
            elif "timeout" in label:
                reason = ProofReason.HIGH_TIMEOUT_RATE

            findings.append(
                ResilienceFinding(
                    dimension=dimension,
                    reason=reason,
                    message=f"{label} exceeds maximum",
                    observed_value=observed,
                    required_value=maximum,
                )
            )

    def _evaluate_minimum_volume(
        self,
        dimension: ResilienceTestDimension,
        observed: int,
        minimum: int,
        label: str,
        findings: List[ResilienceFinding],
    ) -> None:
        if observed < minimum:
            findings.append(
                ResilienceFinding(
                    dimension=dimension,
                    reason=ProofReason.INSUFFICIENT_VOLUME,
                    message=f"{label} is below minimum",
                    observed_value=float(observed),
                    required_value=float(minimum),
                )
            )

    # ---------------------------------------------------------------------
    # METRIC AGGREGATION
    # ---------------------------------------------------------------------

    @staticmethod
    def _evaluated_resource_count(
        proof_input: ResilienceProofInput,
    ) -> int:
        return max(
            (
                measurement.resource_count
                for measurement in proof_input.measurements
            ),
            default=0,
        )

    @staticmethod
    def _evaluated_failure_count(
        proof_input: ResilienceProofInput,
    ) -> int:
        return sum(
            measurement.failure_event_count
            for measurement in proof_input.measurements
        )

    @staticmethod
    def _evaluated_recovery_count(
        proof_input: ResilienceProofInput,
    ) -> int:
        return sum(
            measurement.recovery_event_count
            for measurement in proof_input.measurements
        )

    @staticmethod
    def _scale_band(
        resource_count: int,
    ) -> ResilienceScaleBand:
        if resource_count >= 10**12:
            return ResilienceScaleBand.TRILLIONS

        if resource_count >= 10**9:
            return ResilienceScaleBand.BILLIONS

        if resource_count >= 10**8:
            return ResilienceScaleBand.MASSIVE

        if resource_count >= 10**6:
            return ResilienceScaleBand.LARGE

        if resource_count > 0:
            return ResilienceScaleBand.SMALL

        return ResilienceScaleBand.UNKNOWN

    # ---------------------------------------------------------------------
    # STATE
    # ---------------------------------------------------------------------

    @staticmethod
    def _state_from_decision(
        decision: ProofDecision,
    ) -> ResilienceProofState:
        if decision == ProofDecision.PASS:
            return ResilienceProofState.PASSED

        if decision == ProofDecision.FAIL:
            return ResilienceProofState.FAILED

        return ResilienceProofState.DEFERRED

    # ---------------------------------------------------------------------
    # CHECKPOINTS
    # ---------------------------------------------------------------------

    def _checkpoint(
        self,
        checkpoint_type: CheckpointType,
        state: ResilienceProofState,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        checkpoint_payload = {
            "proof_id": self.identity.proof_id,
            "checkpoint_type": checkpoint_type.value,
            "state": state.value,
            "timestamp": self._now(),
            "metadata": metadata or {},
        }

        checkpoint = ResilienceProofCheckpoint(
            checkpoint_id=self._stable_id(
                checkpoint_type.value,
                checkpoint_payload,
            ),
            proof_id=self.identity.proof_id,
            checkpoint_type=checkpoint_type,
            state=state,
            timestamp=checkpoint_payload["timestamp"],
            digest=self._digest(checkpoint_payload),
            metadata=metadata or {},
        )

        self.backend.save_checkpoint(checkpoint)

    # ---------------------------------------------------------------------
    # EVENTS
    # ---------------------------------------------------------------------

    def _emit_event(
        self,
        event_type: EventType,
        payload: Dict[str, Any],
    ) -> None:
        timestamp = self._now()

        event_payload = {
            "proof_id": self.identity.proof_id,
            "event_type": event_type.value,
            "timestamp": timestamp,
            "payload": payload,
        }

        event = ResilienceProofEvent(
            event_id=self._stable_id(
                event_type.value,
                event_payload,
            ),
            proof_id=self.identity.proof_id,
            event_type=event_type,
            timestamp=timestamp,
            payload=payload,
        )

        self.backend.save_event(event)

    # ---------------------------------------------------------------------
    # SERIALIZATION
    # ---------------------------------------------------------------------

    @staticmethod
    def _finding_to_dict(
        finding: ResilienceFinding,
    ) -> Dict[str, Any]:
        return {
            "dimension": finding.dimension.value,
            "reason": finding.reason.value,
            "message": finding.message,
            "severity": finding.severity,
            "observed_value": finding.observed_value,
            "required_value": finding.required_value,
            "source": finding.source,
        }

    @staticmethod
    def _dimension_to_dict(
        result: ResilienceDimensionResult,
    ) -> Dict[str, Any]:
        return {
            "dimension": result.dimension.value,
            "passed": result.passed,
            "score": result.score,
            "finding_count": result.finding_count,
            "findings": [
                DistributedFailureRecoveryResilienceProof
                ._finding_to_dict(item)
                for item in result.findings
            ],
        }

    @staticmethod
    def _global_to_dict(
        result: GlobalResilienceResult,
    ) -> Dict[str, Any]:
        return {
            "decision": result.decision.value,
            "passed": result.passed,
            "overall_score": result.overall_score,
            "scale_band": result.scale_band.value,
            "evaluated_dimensions": result.evaluated_dimensions,
            "passed_dimensions": result.passed_dimensions,
            "failed_dimensions": result.failed_dimensions,
            "deferred_dimensions": result.deferred_dimensions,
            "findings": [
                DistributedFailureRecoveryResilienceProof
                ._finding_to_dict(item)
                for item in result.findings
            ],
        }

    # ---------------------------------------------------------------------
    # DIGEST / DETERMINISM
    # ---------------------------------------------------------------------

    @classmethod
    def _digest(
        cls,
        value: Any,
    ) -> str:
        canonical = cls._canonicalize(value)

        return hashlib.sha256(
            canonical.encode("utf-8")
        ).hexdigest()

    @classmethod
    def _canonicalize(
        cls,
        value: Any,
    ) -> str:
        if isinstance(value, Enum):
            value = value.value

        elif hasattr(value, "__dataclass_fields__"):
            value = asdict(value)

        elif isinstance(value, Mapping):
            value = {
                str(key): cls._canonicalize(item)
                for key, item in sorted(
                    value.items(),
                    key=lambda item: str(item[0]),
                )
            }

        elif isinstance(value, (list, tuple)):
            value = [
                cls._canonicalize(item)
                for item in value
            ]

        elif isinstance(value, set):
            value = sorted(
                cls._canonicalize(item)
                for item in value
            )

        elif isinstance(value, float):
            if math.isnan(value):
                value = "NaN"
            elif math.isinf(value):
                value = "Infinity" if value > 0 else "-Infinity"

        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )

    @classmethod
    def _stable_id(
        cls,
        namespace: str,
        payload: Any,
    ) -> str:
        digest = cls._digest(
            {
                "namespace": namespace,
                "payload": payload,
            }
        )

        return f"{namespace}-{digest[:32]}"

    @staticmethod
    def _now() -> str:
        return datetime.now(
            timezone.utc
        ).isoformat()


# ============================================================================
# ALIASES
# ============================================================================

GlobalDistributedFailureRecoveryResilienceProof = (
    DistributedFailureRecoveryResilienceProof
)

Phase15_8DistributedFailureRecoveryResilienceProof = (
    DistributedFailureRecoveryResilienceProof
)

DistributedResilienceScaleProof = (
    DistributedFailureRecoveryResilienceProof
)

FailureRecoveryScaleProof = (
    DistributedFailureRecoveryResilienceProof
)

GlobalResilienceScaleProof = (
    DistributedFailureRecoveryResilienceProof
)

MassiveFailureRecoveryProof = (
    DistributedFailureRecoveryResilienceProof
)


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
    "ResilienceProofState",
    "ResilienceTestDimension",
    "EvidenceStrength",
    "EvidenceKind",
    "ResilienceScaleBand",
    "ProofDecision",
    "ProofReason",
    "CheckpointType",
    "EventType",
    "ResilienceProofIdentity",
    "ResilienceProofLineage",
    "ResilienceMeasurement",
    "ResilienceProofInput",
    "ResilienceProofPolicy",
    "ResilienceFinding",
    "ResilienceDimensionResult",
    "GlobalResilienceResult",
    "ResilienceProofCheckpoint",
    "ResilienceProofEvent",
    "ResilienceProofResult",
    "ResilienceProofBackend",
    "InMemoryResilienceProofBackend",
    "DistributedFailureRecoveryResilienceProof",
    "GlobalDistributedFailureRecoveryResilienceProof",
    "Phase15_8DistributedFailureRecoveryResilienceProof",
    "DistributedResilienceScaleProof",
    "FailureRecoveryScaleProof",
    "GlobalResilienceScaleProof",
    "MassiveFailureRecoveryProof",
]


# ============================================================================
# MODULE SELF-CHECK
# ============================================================================


def _self_check() -> bool:
    """
    Lightweight deterministic construction check.

    This does not execute a production workload.
    """

    proof_input = ResilienceProofInput(
        proof_id="phase15_8_self_check",
        target_scope="public_web",
        measurements=[
            ResilienceMeasurement(
                dimension=ResilienceTestDimension.RESOURCE_SCALE,
                resource_count=1_000_000,
                evidence_strength=EvidenceStrength.VERIFIED,
                evidence_kind=EvidenceKind.SCALE,
                source="self_check",
            )
        ],
        required_dimensions=[
            ResilienceTestDimension.RESOURCE_SCALE,
        ],
        require_all_dimensions=True,
        allow_partial_evidence=True,
        allow_simulated_evidence=True,
    )

    proof = DistributedFailureRecoveryResilienceProof(
        proof_input=proof_input,
    )

    result = proof.evaluate()

    return (
        result.identity.proof_id
        == "phase15_8_self_check"
        and result.digest != ""
        and result.overall_score >= 0.0
        and result.overall_score <= 1.0
    )


if __name__ == "__main__":
    if not _self_check():
        raise SystemExit(
            "Phase 15.8 self-check failed"
        )

    print(
        "Phase 15.8 distributed failure, recovery and resilience "
        "proof self-check: PASS"
    )
