"""
OUR SEARCH
Phase 15.7 — Spam / Abuse / Security Scale Proof

Purpose
-------
Scale-proof architecture for validating the spam, abuse, security,
and quality-control subsystem under enormous public-Web workloads.

Target
------
Billions to trillions of publicly accessible Web resources.

This module is a deterministic control-plane / proof layer.

It does NOT:
- crawl the Web
- perform HTTP requests
- execute malware
- execute payloads
- perform exploit attempts
- perform offensive security scanning
- attack external systems
- mutate the production index
- directly block or delete resources
- assign production workers
- access Google Search
- use Google's index, crawler, ranking, or infrastructure

Instead, it consumes externally supplied measurements and evidence
from the Phase 14 spam, abuse, security, and quality architecture.

The proof validates:
- spam detection scale
- abuse detection scale
- security-signal scale
- malware/phishing detection evidence
- content manipulation detection
- link-spam detection
- trust/safe-browsing signals
- resource protection
- distributed abuse detection
- quarantine/recovery control-plane readiness
- false-positive control
- false-negative control
- duplicate detection
- evidence coverage
- distributed consistency
- partition stability
- enforcement-decision correctness
- recovery correctness
- overload resilience
- latency
- resource efficiency
- auditability
- provenance completeness
- security-system availability

A PASS means that the supplied evidence satisfies this configured
scale-proof policy.

It does NOT mean that OUR SEARCH has already scanned the entire
real-world Web or that live global security quality has been achieved.
"""


from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
import uuid
from typing import (
    Any,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)


# ============================================================================
# GLOBAL ARCHITECTURE CONSTANTS
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "spam-abuse-security-scale-proof.v1"

PHASE = "15.7"
PREVIOUS_STAGE = "15.6"
NEXT_STAGE = "15.8"

PHASE_NAME = "Full Scale-Proof Program"
STAGE_NAME = "Spam / Abuse / Security Scale Proof"
NEXT_STAGE_NAME = "Distributed Failure, Recovery & Resilience Proof"


# ============================================================================
# HARD SCALE BOUNDS
# ============================================================================

MAX_RESOURCE_COUNT = 10**18
MAX_SUSPICIOUS_RESOURCE_COUNT = 10**18
MAX_SECURITY_EVENT_COUNT = 10**18
MAX_QUEUE_COUNT = 10**18
MAX_WORKER_COUNT = 10**12
MAX_SHARD_COUNT = 10**12
MAX_REGION_COUNT = 10**6
MAX_PARTITION_COUNT = 10**12
MAX_EVIDENCE_COUNT = 10**18
MAX_MEASUREMENT_WINDOW_SECONDS = 10**9


# ============================================================================
# DEFAULT POLICY THRESHOLDS
# ============================================================================

DEFAULT_MIN_RESOURCE_COUNT = 1_000_000
DEFAULT_MIN_SUSPICIOUS_RESOURCE_COUNT = 100_000
DEFAULT_MIN_SECURITY_EVENT_COUNT = 100_000
DEFAULT_MIN_QUEUE_COUNT = 100_000

DEFAULT_MIN_SPAM_DETECTION_ACCURACY = 0.95
DEFAULT_MIN_ABUSE_DETECTION_ACCURACY = 0.95
DEFAULT_MIN_SECURITY_SIGNAL_COVERAGE = 0.95
DEFAULT_MIN_MALWARE_DETECTION_COVERAGE = 0.95
DEFAULT_MIN_PHISHING_DETECTION_COVERAGE = 0.95
DEFAULT_MIN_CONTENT_MANIPULATION_DETECTION = 0.90
DEFAULT_MIN_LINK_SPAM_DETECTION = 0.90
DEFAULT_MIN_TRUST_SIGNAL_COVERAGE = 0.95

DEFAULT_MIN_DISTRIBUTED_ABUSE_DETECTION = 0.95
DEFAULT_MIN_RESOURCE_PROTECTION = 0.95
DEFAULT_MIN_QUARANTINE_CONTROL_CORRECTNESS = 0.95
DEFAULT_MIN_RECOVERY_CONTROL_CORRECTNESS = 0.95

DEFAULT_MIN_FALSE_POSITIVE_CONTROL = 0.95
DEFAULT_MIN_FALSE_NEGATIVE_CONTROL = 0.90
DEFAULT_MIN_DUPLICATE_DETECTION = 0.95
DEFAULT_MIN_EVIDENCE_COVERAGE = 0.95
DEFAULT_MIN_DISTRIBUTED_CONSISTENCY = 0.95
DEFAULT_MIN_PARTITION_STABILITY = 0.95
DEFAULT_MIN_ENFORCEMENT_CORRECTNESS = 0.95
DEFAULT_MIN_RECOVERY_CORRECTNESS = 0.95

DEFAULT_MIN_QUEUE_DRAIN_RATE = 0.95
DEFAULT_MIN_RESOURCE_EFFICIENCY = 0.80
DEFAULT_MIN_OVERLOAD_RESILIENCE = 0.90
DEFAULT_MIN_AUDITABILITY = 0.95
DEFAULT_MIN_PROVENANCE_COMPLETENESS = 0.95
DEFAULT_MIN_SYSTEM_AVAILABILITY = 0.99

DEFAULT_MAX_DUPLICATE_RATE = 0.01
DEFAULT_MAX_FALSE_POSITIVE_RATE = 0.03
DEFAULT_MAX_FALSE_NEGATIVE_RATE = 0.05
DEFAULT_MAX_SECURITY_EVENT_LOSS_RATE = 0.005
DEFAULT_MAX_EVIDENCE_LOSS_RATE = 0.005
DEFAULT_MAX_ENFORCEMENT_ERROR_RATE = 0.02
DEFAULT_MAX_RECOVERY_ERROR_RATE = 0.02
DEFAULT_MAX_QUALITY_REGRESSION_RATE = 0.05
DEFAULT_MAX_TIMEOUT_RATE = 0.02

DEFAULT_MAX_P50_LATENCY_MS = 250.0
DEFAULT_MAX_P95_LATENCY_MS = 750.0
DEFAULT_MAX_P99_LATENCY_MS = 1500.0


# ============================================================================
# ENUMERATIONS
# ============================================================================


class SecurityProofState(str, Enum):
    CREATED = "created"
    COLLECTING = "collecting"
    NORMALIZED = "normalized"
    EVALUATED = "evaluated"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class SecurityTestDimension(str, Enum):
    RESOURCE_SCALE = "resource_scale"
    SUSPICIOUS_RESOURCE_SCALE = "suspicious_resource_scale"
    SECURITY_EVENT_SCALE = "security_event_scale"
    QUEUE_SCALE = "queue_scale"

    SPAM_DETECTION = "spam_detection"
    ABUSE_DETECTION = "abuse_detection"
    SECURITY_SIGNAL_COVERAGE = "security_signal_coverage"
    MALWARE_DETECTION = "malware_detection"
    PHISHING_DETECTION = "phishing_detection"
    CONTENT_MANIPULATION_DETECTION = "content_manipulation_detection"
    LINK_SPAM_DETECTION = "link_spam_detection"
    TRUST_SIGNAL_COVERAGE = "trust_signal_coverage"

    DISTRIBUTED_ABUSE_DETECTION = "distributed_abuse_detection"
    RESOURCE_PROTECTION = "resource_protection"
    QUARANTINE_CONTROL = "quarantine_control"
    RECOVERY_CONTROL = "recovery_control"

    FALSE_POSITIVE_CONTROL = "false_positive_control"
    FALSE_NEGATIVE_CONTROL = "false_negative_control"
    DUPLICATE_DETECTION = "duplicate_detection"
    EVIDENCE_COVERAGE = "evidence_coverage"
    DISTRIBUTED_CONSISTENCY = "distributed_consistency"
    PARTITION_STABILITY = "partition_stability"

    ENFORCEMENT_CORRECTNESS = "enforcement_correctness"
    RECOVERY_CORRECTNESS = "recovery_correctness"

    QUEUE_DRAIN = "queue_drain"
    RESOURCE_EFFICIENCY = "resource_efficiency"
    OVERLOAD_RESILIENCE = "overload_resilience"

    AUDITABILITY = "auditability"
    PROVENANCE_COMPLETENESS = "provenance_completeness"
    SYSTEM_AVAILABILITY = "system_availability"

    P50_LATENCY = "p50_latency"
    P95_LATENCY = "p95_latency"
    P99_LATENCY = "p99_latency"


class EvidenceStrength(str, Enum):
    OBSERVED = "observed"
    VERIFIED = "verified"
    DERIVED = "derived"
    SIMULATED = "simulated"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class EvidenceKind(str, Enum):
    RESOURCE = "resource"
    SPAM = "spam"
    ABUSE = "abuse"
    SECURITY = "security"
    MALWARE = "malware"
    PHISHING = "phishing"
    CONTENT_MANIPULATION = "content_manipulation"
    LINK_SPAM = "link_spam"
    TRUST = "trust"
    PROTECTION = "protection"
    QUARANTINE = "quarantine"
    RECOVERY = "recovery"
    DISTRIBUTED = "distributed"
    PARTITION = "partition"
    FALSE_POSITIVE = "false_positive"
    FALSE_NEGATIVE = "false_negative"
    DUPLICATE = "duplicate"
    EVIDENCE = "evidence"
    ENFORCEMENT = "enforcement"
    QUEUE = "queue"
    RESOURCE_USAGE = "resource_usage"
    OVERLOAD = "overload"
    AUDIT = "audit"
    PROVENANCE = "provenance"
    AVAILABILITY = "availability"
    LATENCY = "latency"


class SecurityScaleBand(str, Enum):
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
    INSUFFICIENT_SUSPICIOUS_RESOURCE_VOLUME = (
        "insufficient_suspicious_resource_volume"
    )
    INSUFFICIENT_SECURITY_EVENT_VOLUME = (
        "insufficient_security_event_volume"
    )
    INSUFFICIENT_QUEUE_VOLUME = "insufficient_queue_volume"

    LOW_SPAM_DETECTION = "low_spam_detection"
    LOW_ABUSE_DETECTION = "low_abuse_detection"
    LOW_SECURITY_SIGNAL_COVERAGE = "low_security_signal_coverage"
    LOW_MALWARE_DETECTION = "low_malware_detection"
    LOW_PHISHING_DETECTION = "low_phishing_detection"
    LOW_CONTENT_MANIPULATION_DETECTION = (
        "low_content_manipulation_detection"
    )
    LOW_LINK_SPAM_DETECTION = "low_link_spam_detection"
    LOW_TRUST_SIGNAL_COVERAGE = "low_trust_signal_coverage"

    LOW_DISTRIBUTED_ABUSE_DETECTION = (
        "low_distributed_abuse_detection"
    )
    LOW_RESOURCE_PROTECTION = "low_resource_protection"
    LOW_QUARANTINE_CONTROL = "low_quarantine_control"
    LOW_RECOVERY_CONTROL = "low_recovery_control"

    LOW_FALSE_POSITIVE_CONTROL = "low_false_positive_control"
    LOW_FALSE_NEGATIVE_CONTROL = "low_false_negative_control"
    LOW_DUPLICATE_DETECTION = "low_duplicate_detection"
    LOW_EVIDENCE_COVERAGE = "low_evidence_coverage"
    LOW_DISTRIBUTED_CONSISTENCY = "low_distributed_consistency"
    LOW_PARTITION_STABILITY = "low_partition_stability"

    LOW_ENFORCEMENT_CORRECTNESS = "low_enforcement_correctness"
    LOW_RECOVERY_CORRECTNESS = "low_recovery_correctness"

    LOW_QUEUE_DRAIN = "low_queue_drain"
    LOW_RESOURCE_EFFICIENCY = "low_resource_efficiency"
    LOW_OVERLOAD_RESILIENCE = "low_overload_resilience"

    LOW_AUDITABILITY = "low_auditability"
    LOW_PROVENANCE_COMPLETENESS = "low_provenance_completeness"
    LOW_SYSTEM_AVAILABILITY = "low_system_availability"

    HIGH_DUPLICATE_RATE = "high_duplicate_rate"
    HIGH_FALSE_POSITIVE_RATE = "high_false_positive_rate"
    HIGH_FALSE_NEGATIVE_RATE = "high_false_negative_rate"
    HIGH_SECURITY_EVENT_LOSS_RATE = "high_security_event_loss_rate"
    HIGH_EVIDENCE_LOSS_RATE = "high_evidence_loss_rate"
    HIGH_ENFORCEMENT_ERROR_RATE = "high_enforcement_error_rate"
    HIGH_RECOVERY_ERROR_RATE = "high_recovery_error_rate"
    HIGH_QUALITY_REGRESSION_RATE = "high_quality_regression_rate"
    HIGH_TIMEOUT_RATE = "high_timeout_rate"

    HIGH_P50_LATENCY = "high_p50_latency"
    HIGH_P95_LATENCY = "high_p95_latency"
    HIGH_P99_LATENCY = "high_p99_latency"

    INVALID_INPUT = "invalid_input"
    INVALID_METRIC = "invalid_metric"
    DUPLICATE_DIMENSION = "duplicate_dimension"
    PUBLIC_SCOPE_REQUIRED = "public_scope_required"


class CheckpointType(str, Enum):
    PROOF_CREATED = "proof_created"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    DIMENSION_VALIDATED = "dimension_validated"
    SECURITY_EVALUATED = "security_evaluated"
    PROOF_FINALIZED = "proof_finalized"


class EventType(str, Enum):
    PROOF_CREATED = "proof_created"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    EVIDENCE_REJECTED = "evidence_rejected"
    DIMENSION_EVALUATED = "dimension_evaluated"
    SECURITY_EVALUATED = "security_evaluated"
    PROOF_PASSED = "proof_passed"
    PROOF_FAILED = "proof_failed"
    PROOF_DEFERRED = "proof_deferred"


# ============================================================================
# DATACLASSES
# ============================================================================


@dataclass(frozen=True)
class SecurityProofIdentity:
    proof_id: str
    architecture_version: str
    phase: str
    created_at: str
    target: str = SCALE_TARGET


@dataclass(frozen=True)
class SecurityProofLineage:
    previous_stage: str
    next_stage: str
    phase_name: str
    source_components: Tuple[str, ...]
    parent_proof_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class SpamAbuseSecurityMeasurement:
    """
    Externally supplied scale-proof measurement.

    The measurement contains only evidence/metrics.
    It does not execute any security operation.
    """

    dimension: SecurityTestDimension

    resource_count: int = 0
    suspicious_resource_count: int = 0
    security_event_count: int = 0
    queue_count: int = 0

    spam_detection_accuracy: float = 0.0
    abuse_detection_accuracy: float = 0.0
    security_signal_coverage: float = 0.0
    malware_detection_coverage: float = 0.0
    phishing_detection_coverage: float = 0.0
    content_manipulation_detection: float = 0.0
    link_spam_detection: float = 0.0
    trust_signal_coverage: float = 0.0

    distributed_abuse_detection: float = 0.0
    resource_protection: float = 0.0
    quarantine_control_correctness: float = 0.0
    recovery_control_correctness: float = 0.0

    false_positive_control: float = 0.0
    false_negative_control: float = 0.0
    duplicate_detection: float = 0.0
    evidence_coverage: float = 0.0
    distributed_consistency: float = 0.0
    partition_stability: float = 0.0

    enforcement_correctness: float = 0.0
    recovery_correctness: float = 0.0

    queue_drain_rate: float = 0.0
    resource_efficiency: float = 0.0
    overload_resilience: float = 0.0

    auditability: float = 0.0
    provenance_completeness: float = 0.0
    system_availability: float = 0.0

    duplicate_rate: float = 0.0
    false_positive_rate: float = 0.0
    false_negative_rate: float = 0.0
    security_event_loss_rate: float = 0.0
    evidence_loss_rate: float = 0.0
    enforcement_error_rate: float = 0.0
    recovery_error_rate: float = 0.0
    quality_regression_rate: float = 0.0
    timeout_rate: float = 0.0

    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0

    worker_count: int = 0
    shard_count: int = 0
    region_count: int = 0
    partition_count: int = 0
    evidence_count: int = 0

    measurement_window_seconds: float = 0.0

    evidence_strength: EvidenceStrength = EvidenceStrength.UNKNOWN
    evidence_kind: EvidenceKind = EvidenceKind.SECURITY

    source: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SecurityProofInput:
    measurements: Tuple[SpamAbuseSecurityMeasurement, ...]

    public_web_scope: bool = True

    evidence_strength: EvidenceStrength = EvidenceStrength.OBSERVED

    allow_partial_evidence: bool = False
    allow_simulated_evidence: bool = False
    require_all_dimensions: bool = True

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SecurityProofPolicy:
    min_resource_count: int = DEFAULT_MIN_RESOURCE_COUNT
    min_suspicious_resource_count: int = (
        DEFAULT_MIN_SUSPICIOUS_RESOURCE_COUNT
    )
    min_security_event_count: int = DEFAULT_MIN_SECURITY_EVENT_COUNT
    min_queue_count: int = DEFAULT_MIN_QUEUE_COUNT

    min_spam_detection_accuracy: float = (
        DEFAULT_MIN_SPAM_DETECTION_ACCURACY
    )
    min_abuse_detection_accuracy: float = (
        DEFAULT_MIN_ABUSE_DETECTION_ACCURACY
    )
    min_security_signal_coverage: float = (
        DEFAULT_MIN_SECURITY_SIGNAL_COVERAGE
    )
    min_malware_detection_coverage: float = (
        DEFAULT_MIN_MALWARE_DETECTION_COVERAGE
    )
    min_phishing_detection_coverage: float = (
        DEFAULT_MIN_PHISHING_DETECTION_COVERAGE
    )
    min_content_manipulation_detection: float = (
        DEFAULT_MIN_CONTENT_MANIPULATION_DETECTION
    )
    min_link_spam_detection: float = (
        DEFAULT_MIN_LINK_SPAM_DETECTION
    )
    min_trust_signal_coverage: float = (
        DEFAULT_MIN_TRUST_SIGNAL_COVERAGE
    )

    min_distributed_abuse_detection: float = (
        DEFAULT_MIN_DISTRIBUTED_ABUSE_DETECTION
    )
    min_resource_protection: float = (
        DEFAULT_MIN_RESOURCE_PROTECTION
    )
    min_quarantine_control_correctness: float = (
        DEFAULT_MIN_QUARANTINE_CONTROL_CORRECTNESS
    )
    min_recovery_control_correctness: float = (
        DEFAULT_MIN_RECOVERY_CONTROL_CORRECTNESS
    )

    min_false_positive_control: float = (
        DEFAULT_MIN_FALSE_POSITIVE_CONTROL
    )
    min_false_negative_control: float = (
        DEFAULT_MIN_FALSE_NEGATIVE_CONTROL
    )
    min_duplicate_detection: float = DEFAULT_MIN_DUPLICATE_DETECTION
    min_evidence_coverage: float = DEFAULT_MIN_EVIDENCE_COVERAGE
    min_distributed_consistency: float = (
        DEFAULT_MIN_DISTRIBUTED_CONSISTENCY
    )
    min_partition_stability: float = DEFAULT_MIN_PARTITION_STABILITY

    min_enforcement_correctness: float = (
        DEFAULT_MIN_ENFORCEMENT_CORRECTNESS
    )
    min_recovery_correctness: float = (
        DEFAULT_MIN_RECOVERY_CORRECTNESS
    )

    min_queue_drain_rate: float = DEFAULT_MIN_QUEUE_DRAIN_RATE
    min_resource_efficiency: float = (
        DEFAULT_MIN_RESOURCE_EFFICIENCY
    )
    min_overload_resilience: float = (
        DEFAULT_MIN_OVERLOAD_RESILIENCE
    )

    min_auditability: float = DEFAULT_MIN_AUDITABILITY
    min_provenance_completeness: float = (
        DEFAULT_MIN_PROVENANCE_COMPLETENESS
    )
    min_system_availability: float = (
        DEFAULT_MIN_SYSTEM_AVAILABILITY
    )

    max_duplicate_rate: float = DEFAULT_MAX_DUPLICATE_RATE
    max_false_positive_rate: float = DEFAULT_MAX_FALSE_POSITIVE_RATE
    max_false_negative_rate: float = DEFAULT_MAX_FALSE_NEGATIVE_RATE
    max_security_event_loss_rate: float = (
        DEFAULT_MAX_SECURITY_EVENT_LOSS_RATE
    )
    max_evidence_loss_rate: float = DEFAULT_MAX_EVIDENCE_LOSS_RATE
    max_enforcement_error_rate: float = (
        DEFAULT_MAX_ENFORCEMENT_ERROR_RATE
    )
    max_recovery_error_rate: float = (
        DEFAULT_MAX_RECOVERY_ERROR_RATE
    )
    max_quality_regression_rate: float = (
        DEFAULT_MAX_QUALITY_REGRESSION_RATE
    )
    max_timeout_rate: float = DEFAULT_MAX_TIMEOUT_RATE

    max_p50_latency_ms: float = DEFAULT_MAX_P50_LATENCY_MS
    max_p95_latency_ms: float = DEFAULT_MAX_P95_LATENCY_MS
    max_p99_latency_ms: float = DEFAULT_MAX_P99_LATENCY_MS

    require_public_web_scope: bool = True


@dataclass(frozen=True)
class SecurityFinding:
    dimension: SecurityTestDimension
    reason: ProofReason
    message: str
    severity: str = "error"
    measurement_index: Optional[int] = None
    observed_value: Optional[float] = None
    required_value: Optional[float] = None


@dataclass(frozen=True)
class SecurityDimensionResult:
    dimension: SecurityTestDimension
    passed: bool
    score: float
    findings: Tuple[SecurityFinding, ...] = ()
    measurement_count: int = 0


@dataclass(frozen=True)
class GlobalSecurityResult:
    decision: ProofDecision
    score: float
    scale_band: SecurityScaleBand

    evaluated_resource_count: int
    evaluated_suspicious_resource_count: int
    evaluated_security_event_count: int
    evaluated_queue_count: int

    dimensions_evaluated: int
    dimensions_required: int
    passed_dimensions: int
    failed_dimensions: int
    deferred_dimensions: int


@dataclass(frozen=True)
class SecurityProofCheckpoint:
    checkpoint_id: str
    proof_id: str
    checkpoint_type: CheckpointType
    state: SecurityProofState
    created_at: str
    payload_digest: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SecurityProofEvent:
    event_id: str
    proof_id: str
    event_type: EventType
    created_at: str
    payload_digest: str
    dimension: Optional[SecurityTestDimension] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SecurityProofResult:
    identity: SecurityProofIdentity
    lineage: SecurityProofLineage

    state: SecurityProofState
    decision: ProofDecision
    reason: ProofReason

    overall_score: float
    scale_band: SecurityScaleBand

    evaluated_resource_count: int
    evaluated_suspicious_resource_count: int
    evaluated_security_event_count: int
    evaluated_queue_count: int

    dimensions: Tuple[SecurityDimensionResult, ...]
    findings: Tuple[SecurityFinding, ...]
    global_result: GlobalSecurityResult

    checkpoints: Tuple[SecurityProofCheckpoint, ...]
    events: Tuple[SecurityProofEvent, ...]

    created_at: str
    finalized_at: str

    digest: str


# ============================================================================
# BACKEND PROTOCOL
# ============================================================================


class SecurityProofBackend(Protocol):
    def save_result(self, result: SecurityProofResult) -> None:
        ...

    def load_result(self, proof_id: str) -> Optional[SecurityProofResult]:
        ...

    def save_checkpoint(self, checkpoint: SecurityProofCheckpoint) -> None:
        ...

    def save_event(self, event: SecurityProofEvent) -> None:
        ...


class InMemorySecurityProofBackend:
    """
    Reference backend for deterministic testing.

    Production deployments can replace this with durable distributed
    storage without changing the proof engine.
    """

    def __init__(self) -> None:
        self.results: Dict[str, SecurityProofResult] = {}
        self.checkpoints: Dict[str, SecurityProofCheckpoint] = {}
        self.events: Dict[str, SecurityProofEvent] = {}

    def save_result(self, result: SecurityProofResult) -> None:
        self.results[result.identity.proof_id] = result

    def load_result(
        self,
        proof_id: str,
    ) -> Optional[SecurityProofResult]:
        return self.results.get(proof_id)

    def save_checkpoint(
        self,
        checkpoint: SecurityProofCheckpoint,
    ) -> None:
        self.checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(
        self,
        event: SecurityProofEvent,
    ) -> None:
        self.events[event.event_id] = event


# ============================================================================
# MAIN SCALE-PROOF ENGINE
# ============================================================================


class SpamAbuseSecurityScaleProof:
    """
    Deterministic scale-proof engine for Phase 15.7.
    """

    REQUIRED_DIMENSIONS: Tuple[SecurityTestDimension, ...] = (
        SecurityTestDimension.RESOURCE_SCALE,
        SecurityTestDimension.SUSPICIOUS_RESOURCE_SCALE,
        SecurityTestDimension.SECURITY_EVENT_SCALE,
        SecurityTestDimension.QUEUE_SCALE,

        SecurityTestDimension.SPAM_DETECTION,
        SecurityTestDimension.ABUSE_DETECTION,
        SecurityTestDimension.SECURITY_SIGNAL_COVERAGE,
        SecurityTestDimension.MALWARE_DETECTION,
        SecurityTestDimension.PHISHING_DETECTION,
        SecurityTestDimension.CONTENT_MANIPULATION_DETECTION,
        SecurityTestDimension.LINK_SPAM_DETECTION,
        SecurityTestDimension.TRUST_SIGNAL_COVERAGE,

        SecurityTestDimension.DISTRIBUTED_ABUSE_DETECTION,
        SecurityTestDimension.RESOURCE_PROTECTION,
        SecurityTestDimension.QUARANTINE_CONTROL,
        SecurityTestDimension.RECOVERY_CONTROL,

        SecurityTestDimension.FALSE_POSITIVE_CONTROL,
        SecurityTestDimension.FALSE_NEGATIVE_CONTROL,
        SecurityTestDimension.DUPLICATE_DETECTION,
        SecurityTestDimension.EVIDENCE_COVERAGE,
        SecurityTestDimension.DISTRIBUTED_CONSISTENCY,
        SecurityTestDimension.PARTITION_STABILITY,

        SecurityTestDimension.ENFORCEMENT_CORRECTNESS,
        SecurityTestDimension.RECOVERY_CORRECTNESS,

        SecurityTestDimension.QUEUE_DRAIN,
        SecurityTestDimension.RESOURCE_EFFICIENCY,
        SecurityTestDimension.OVERLOAD_RESILIENCE,

        SecurityTestDimension.AUDITABILITY,
        SecurityTestDimension.PROVENANCE_COMPLETENESS,
        SecurityTestDimension.SYSTEM_AVAILABILITY,

        SecurityTestDimension.P50_LATENCY,
        SecurityTestDimension.P95_LATENCY,
        SecurityTestDimension.P99_LATENCY,
    )

    def __init__(
        self,
        proof_input: SecurityProofInput,
        *,
        policy: Optional[SecurityProofPolicy] = None,
        backend: Optional[SecurityProofBackend] = None,
        proof_id: Optional[str] = None,
        parent_proof_ids: Sequence[str] = (),
    ) -> None:
        self.input = proof_input
        self.policy = policy or SecurityProofPolicy()
        self.backend = backend or InMemorySecurityProofBackend()

        now = self._now()

        self.identity = SecurityProofIdentity(
            proof_id=proof_id or f"15.7-{uuid.uuid4().hex}",
            architecture_version=ARCHITECTURE_VERSION,
            phase=PHASE,
            created_at=now,
            target=SCALE_TARGET,
        )

        self.lineage = SecurityProofLineage(
            previous_stage=PREVIOUS_STAGE,
            next_stage=NEXT_STAGE,
            phase_name=PHASE_NAME,
            source_components=(
                "spam_detection_signal_architecture",
                "content_quality_manipulation_detection",
                "link_spam_graph_abuse_detection",
                "malware_phishing_harmful_resource_detection",
                "security_trust_safe_browsing_signals",
                "crawl_index_abuse_resource_protection",
                "distributed_abuse_detection_enforcement",
                "global_quality_control_quarantine_recovery",
                "final_spam_abuse_security_quality_architecture",
            ),
            parent_proof_ids=tuple(parent_proof_ids),
        )

        self.state = SecurityProofState.CREATED

        self._dimensions: list[SecurityDimensionResult] = []
        self._findings: list[SecurityFinding] = []
        self._checkpoints: list[SecurityProofCheckpoint] = []
        self._events: list[SecurityProofEvent] = []

        self._result: Optional[SecurityProofResult] = None

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

    def evaluate(self) -> SecurityProofResult:
        if self._result is not None:
            return self._result

        self.state = SecurityProofState.COLLECTING

        try:
            self._validate_input()

            self._checkpoint(
                CheckpointType.INPUT_ACCEPTED,
                {
                    "measurement_count": len(
                        self.input.measurements
                    ),
                },
            )

            normalized = self._normalize_input()

            self._checkpoint(
                CheckpointType.INPUT_NORMALIZED,
                {
                    "measurement_count": len(
                        normalized.measurements
                    ),
                },
            )

            if not normalized.measurements:
                return self._finalize_deferred(
                    ProofReason.NO_DIMENSIONS,
                    "No spam, abuse, security, or quality measurements "
                    "were supplied.",
                )

            self.state = SecurityProofState.NORMALIZED

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

            self.state = SecurityProofState.EVALUATED

            global_result = self._evaluate_global(
                normalized.measurements
            )

            findings = self._collect_findings()

            decision, reason = self._make_decision(
                normalized,
                global_result,
                findings,
            )

            self._checkpoint(
                CheckpointType.SECURITY_EVALUATED,
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
            finding = SecurityFinding(
                dimension=SecurityTestDimension.RESOURCE_SCALE,
                reason=ProofReason.INVALID_INPUT,
                message=str(exc),
            )

            self._findings.append(finding)

            return self._finalize(
                self.input,
                self._evaluate_global(
                    self.input.measurements
                ),
                tuple(self._findings),
                ProofDecision.FAIL,
                ProofReason.INVALID_INPUT,
            )

    def evaluate_many(
        self,
        inputs: Iterable[SecurityProofInput],
    ) -> Tuple[SecurityProofResult, ...]:
        results = []

        for proof_input in inputs:
            proof = SpamAbuseSecurityScaleProof(
                proof_input,
                policy=self.policy,
                backend=self.backend,
            )

            results.append(proof.evaluate())

        return tuple(results)

    def get_result(self) -> Optional[SecurityProofResult]:
        return self._result

    # ----------------------------------------------------------------------
    # VALIDATION
    # ----------------------------------------------------------------------

    def _validate_input(self) -> None:
        if (
            self.policy.require_public_web_scope
            and not self.input.public_web_scope
        ):
            raise ValueError(
                ProofReason.PUBLIC_SCOPE_REQUIRED.value
            )

        if not self.input.measurements:
            return

        seen: set[SecurityTestDimension] = set()

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
        measurement: SpamAbuseSecurityMeasurement,
    ) -> None:
        bounded_counts = {
            "resource_count": measurement.resource_count,
            "suspicious_resource_count":
                measurement.suspicious_resource_count,
            "security_event_count":
                measurement.security_event_count,
            "queue_count": measurement.queue_count,
            "worker_count": measurement.worker_count,
            "shard_count": measurement.shard_count,
            "region_count": measurement.region_count,
            "partition_count": measurement.partition_count,
            "evidence_count": measurement.evidence_count,
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

        if (
            measurement.suspicious_resource_count
            > MAX_SUSPICIOUS_RESOURCE_COUNT
        ):
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "suspicious_resource_count exceeds maximum"
            )

        if (
            measurement.security_event_count
            > MAX_SECURITY_EVENT_COUNT
        ):
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "security_event_count exceeds maximum"
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

        if measurement.evidence_count > MAX_EVIDENCE_COUNT:
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "evidence_count exceeds maximum"
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
            "spam_detection_accuracy":
                measurement.spam_detection_accuracy,
            "abuse_detection_accuracy":
                measurement.abuse_detection_accuracy,
            "security_signal_coverage":
                measurement.security_signal_coverage,
            "malware_detection_coverage":
                measurement.malware_detection_coverage,
            "phishing_detection_coverage":
                measurement.phishing_detection_coverage,
            "content_manipulation_detection":
                measurement.content_manipulation_detection,
            "link_spam_detection":
                measurement.link_spam_detection,
            "trust_signal_coverage":
                measurement.trust_signal_coverage,
            "distributed_abuse_detection":
                measurement.distributed_abuse_detection,
            "resource_protection":
                measurement.resource_protection,
            "quarantine_control_correctness":
                measurement.quarantine_control_correctness,
            "recovery_control_correctness":
                measurement.recovery_control_correctness,
            "false_positive_control":
                measurement.false_positive_control,
            "false_negative_control":
                measurement.false_negative_control,
            "duplicate_detection":
                measurement.duplicate_detection,
            "evidence_coverage":
                measurement.evidence_coverage,
            "distributed_consistency":
                measurement.distributed_consistency,
            "partition_stability":
                measurement.partition_stability,
            "enforcement_correctness":
                measurement.enforcement_correctness,
            "recovery_correctness":
                measurement.recovery_correctness,
            "queue_drain_rate":
                measurement.queue_drain_rate,
            "resource_efficiency":
                measurement.resource_efficiency,
            "overload_resilience":
                measurement.overload_resilience,
            "auditability":
                measurement.auditability,
            "provenance_completeness":
                measurement.provenance_completeness,
            "system_availability":
                measurement.system_availability,
            "duplicate_rate":
                measurement.duplicate_rate,
            "false_positive_rate":
                measurement.false_positive_rate,
            "false_negative_rate":
                measurement.false_negative_rate,
            "security_event_loss_rate":
                measurement.security_event_loss_rate,
            "evidence_loss_rate":
                measurement.evidence_loss_rate,
            "enforcement_error_rate":
                measurement.enforcement_error_rate,
            "recovery_error_rate":
                measurement.recovery_error_rate,
            "quality_regression_rate":
                measurement.quality_regression_rate,
            "timeout_rate":
                measurement.timeout_rate,
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

        if (
            measurement.p50_latency_ms
            > measurement.p95_latency_ms
        ):
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "p50 latency cannot exceed p95 latency"
            )

        if (
            measurement.p95_latency_ms
            > measurement.p99_latency_ms
        ):
            raise ValueError(
                f"{ProofReason.INVALID_METRIC.value}: "
                "p95 latency cannot exceed p99 latency"
            )

    # ----------------------------------------------------------------------
    # NORMALIZATION
    # ----------------------------------------------------------------------

    def _normalize_input(self) -> SecurityProofInput:
        measurements = tuple(
            sorted(
                self.input.measurements,
                key=lambda item: item.dimension.value,
            )
        )

        return SecurityProofInput(
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
        dimension: SecurityTestDimension,
        measurements: Sequence[SpamAbuseSecurityMeasurement],
    ) -> SecurityDimensionResult:
        matching = [
            item
            for item in measurements
            if item.dimension == dimension
        ]

        if not matching:
            return SecurityDimensionResult(
                dimension=dimension,
                passed=False,
                score=0.0,
                findings=(),
                measurement_count=0,
            )

        findings = []

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

        return SecurityDimensionResult(
            dimension=dimension,
            passed=not findings,
            score=score,
            findings=tuple(findings),
            measurement_count=len(matching),
        )

    def _evaluate_measurement_for_dimension(
        self,
        dimension: SecurityTestDimension,
        measurement: SpamAbuseSecurityMeasurement,
        index: int,
    ) -> Tuple[SecurityFinding, ...]:
        findings = []
        policy = self.policy

        def add(
            reason: ProofReason,
            message: str,
            observed: Optional[float] = None,
            required: Optional[float] = None,
        ) -> None:
            findings.append(
                SecurityFinding(
                    dimension=dimension,
                    reason=reason,
                    message=message,
                    measurement_index=index,
                    observed_value=observed,
                    required_value=required,
                )
            )

        # --------------------------------------------------------------
        # VOLUME
        # --------------------------------------------------------------

        if dimension == SecurityTestDimension.RESOURCE_SCALE:
            if (
                measurement.resource_count
                < policy.min_resource_count
            ):
                add(
                    ProofReason.INSUFFICIENT_RESOURCE_VOLUME,
                    "Resource volume is below the configured "
                    "scale-proof minimum.",
                    measurement.resource_count,
                    policy.min_resource_count,
                )

        elif (
            dimension
            == SecurityTestDimension.SUSPICIOUS_RESOURCE_SCALE
        ):
            if (
                measurement.suspicious_resource_count
                < policy.min_suspicious_resource_count
            ):
                add(
                    ProofReason.INSUFFICIENT_SUSPICIOUS_RESOURCE_VOLUME,
                    "Suspicious-resource volume is below the "
                    "configured proof minimum.",
                    measurement.suspicious_resource_count,
                    policy.min_suspicious_resource_count,
                )

        elif (
            dimension
            == SecurityTestDimension.SECURITY_EVENT_SCALE
        ):
            if (
                measurement.security_event_count
                < policy.min_security_event_count
            ):
                add(
                    ProofReason.INSUFFICIENT_SECURITY_EVENT_VOLUME,
                    "Security-event volume is below the "
                    "configured proof minimum.",
                    measurement.security_event_count,
                    policy.min_security_event_count,
                )

        elif dimension == SecurityTestDimension.QUEUE_SCALE:
            if measurement.queue_count < policy.min_queue_count:
                add(
                    ProofReason.INSUFFICIENT_QUEUE_VOLUME,
                    "Security/abuse queue volume is below the "
                    "configured proof minimum.",
                    measurement.queue_count,
                    policy.min_queue_count,
                )

        # --------------------------------------------------------------
        # DETECTION
        # --------------------------------------------------------------

        elif dimension == SecurityTestDimension.SPAM_DETECTION:
            if (
                measurement.spam_detection_accuracy
                < policy.min_spam_detection_accuracy
            ):
                add(
                    ProofReason.LOW_SPAM_DETECTION,
                    "Spam detection accuracy is below the "
                    "required threshold.",
                    measurement.spam_detection_accuracy,
                    policy.min_spam_detection_accuracy,
                )

        elif dimension == SecurityTestDimension.ABUSE_DETECTION:
            if (
                measurement.abuse_detection_accuracy
                < policy.min_abuse_detection_accuracy
            ):
                add(
                    ProofReason.LOW_ABUSE_DETECTION,
                    "Abuse detection accuracy is below the "
                    "required threshold.",
                    measurement.abuse_detection_accuracy,
                    policy.min_abuse_detection_accuracy,
                )

        elif (
            dimension
            == SecurityTestDimension.SECURITY_SIGNAL_COVERAGE
        ):
            if (
                measurement.security_signal_coverage
                < policy.min_security_signal_coverage
            ):
                add(
                    ProofReason.LOW_SECURITY_SIGNAL_COVERAGE,
                    "Security-signal coverage is below the "
                    "required threshold.",
                    measurement.security_signal_coverage,
                    policy.min_security_signal_coverage,
                )

        elif dimension == SecurityTestDimension.MALWARE_DETECTION:
            if (
                measurement.malware_detection_coverage
                < policy.min_malware_detection_coverage
            ):
                add(
                    ProofReason.LOW_MALWARE_DETECTION,
                    "Malware detection coverage is below the "
                    "required threshold.",
                    measurement.malware_detection_coverage,
                    policy.min_malware_detection_coverage,
                )

        elif dimension == SecurityTestDimension.PHISHING_DETECTION:
            if (
                measurement.phishing_detection_coverage
                < policy.min_phishing_detection_coverage
            ):
                add(
                    ProofReason.LOW_PHISHING_DETECTION,
                    "Phishing detection coverage is below the "
                    "required threshold.",
                    measurement.phishing_detection_coverage,
                    policy.min_phishing_detection_coverage,
                )

        elif (
            dimension
            == SecurityTestDimension.CONTENT_MANIPULATION_DETECTION
        ):
            if (
                measurement.content_manipulation_detection
                < policy.min_content_manipulation_detection
            ):
                add(
                    ProofReason.LOW_CONTENT_MANIPULATION_DETECTION,
                    "Content-manipulation detection is below "
                    "the required threshold.",
                    measurement.content_manipulation_detection,
                    policy.min_content_manipulation_detection,
                )

        elif dimension == SecurityTestDimension.LINK_SPAM_DETECTION:
            if (
                measurement.link_spam_detection
                < policy.min_link_spam_detection
            ):
                add(
                    ProofReason.LOW_LINK_SPAM_DETECTION,
                    "Link-spam detection is below the "
                    "required threshold.",
                    measurement.link_spam_detection,
                    policy.min_link_spam_detection,
                )

        elif dimension == SecurityTestDimension.TRUST_SIGNAL_COVERAGE:
            if (
                measurement.trust_signal_coverage
                < policy.min_trust_signal_coverage
            ):
                add(
                    ProofReason.LOW_TRUST_SIGNAL_COVERAGE,
                    "Trust/safe-browsing signal coverage is "
                    "below the required threshold.",
                    measurement.trust_signal_coverage,
                    policy.min_trust_signal_coverage,
                )

        # --------------------------------------------------------------
        # CONTROL PLANES
        # --------------------------------------------------------------

        elif (
            dimension
            == SecurityTestDimension.DISTRIBUTED_ABUSE_DETECTION
        ):
            if (
                measurement.distributed_abuse_detection
                < policy.min_distributed_abuse_detection
            ):
                add(
                    ProofReason.LOW_DISTRIBUTED_ABUSE_DETECTION,
                    "Distributed abuse detection is below "
                    "the required threshold.",
                    measurement.distributed_abuse_detection,
                    policy.min_distributed_abuse_detection,
                )

        elif dimension == SecurityTestDimension.RESOURCE_PROTECTION:
            if (
                measurement.resource_protection
                < policy.min_resource_protection
            ):
                add(
                    ProofReason.LOW_RESOURCE_PROTECTION,
                    "Resource protection is below the "
                    "required threshold.",
                    measurement.resource_protection,
                    policy.min_resource_protection,
                )

        elif dimension == SecurityTestDimension.QUARANTINE_CONTROL:
            if (
                measurement.quarantine_control_correctness
                < policy.min_quarantine_control_correctness
            ):
                add(
                    ProofReason.LOW_QUARANTINE_CONTROL,
                    "Quarantine control correctness is below "
                    "the required threshold.",
                    measurement.quarantine_control_correctness,
                    policy.min_quarantine_control_correctness,
                )

        elif dimension == SecurityTestDimension.RECOVERY_CONTROL:
            if (
                measurement.recovery_control_correctness
                < policy.min_recovery_control_correctness
            ):
                add(
                    ProofReason.LOW_RECOVERY_CONTROL,
                    "Recovery control correctness is below "
                    "the required threshold.",
                    measurement.recovery_control_correctness,
                    policy.min_recovery_control_correctness,
                )

        # --------------------------------------------------------------
        # QUALITY / ERROR CONTROL
        # --------------------------------------------------------------

        elif dimension == SecurityTestDimension.FALSE_POSITIVE_CONTROL:
            if (
                measurement.false_positive_control
                < policy.min_false_positive_control
            ):
                add(
                    ProofReason.LOW_FALSE_POSITIVE_CONTROL,
                    "False-positive control quality is below "
                    "the required threshold.",
                    measurement.false_positive_control,
                    policy.min_false_positive_control,
                )

            if (
                measurement.false_positive_rate
                > policy.max_false_positive_rate
            ):
                add(
                    ProofReason.HIGH_FALSE_POSITIVE_RATE,
                    "False-positive rate exceeds the allowed maximum.",
                    measurement.false_positive_rate,
                    policy.max_false_positive_rate,
                )

        elif dimension == SecurityTestDimension.FALSE_NEGATIVE_CONTROL:
            if (
                measurement.false_negative_control
                < policy.min_false_negative_control
            ):
                add(
                    ProofReason.LOW_FALSE_NEGATIVE_CONTROL,
                    "False-negative control quality is below "
                    "the required threshold.",
                    measurement.false_negative_control,
                    policy.min_false_negative_control,
                )

            if (
                measurement.false_negative_rate
                > policy.max_false_negative_rate
            ):
                add(
                    ProofReason.HIGH_FALSE_NEGATIVE_RATE,
                    "False-negative rate exceeds the allowed maximum.",
                    measurement.false_negative_rate,
                    policy.max_false_negative_rate,
                )

        elif dimension == SecurityTestDimension.DUPLICATE_DETECTION:
            if (
                measurement.duplicate_detection
                < policy.min_duplicate_detection
            ):
                add(
                    ProofReason.LOW_DUPLICATE_DETECTION,
                    "Duplicate detection quality is below "
                    "the required threshold.",
                    measurement.duplicate_detection,
                    policy.min_duplicate_detection,
                )

            if (
                measurement.duplicate_rate
                > policy.max_duplicate_rate
            ):
                add(
                    ProofReason.HIGH_DUPLICATE_RATE,
                    "Duplicate rate exceeds the allowed maximum.",
                    measurement.duplicate_rate,
                    policy.max_duplicate_rate,
                )

        elif dimension == SecurityTestDimension.EVIDENCE_COVERAGE:
            if (
                measurement.evidence_coverage
                < policy.min_evidence_coverage
            ):
                add(
                    ProofReason.LOW_EVIDENCE_COVERAGE,
                    "Evidence coverage is below the required threshold.",
                    measurement.evidence_coverage,
                    policy.min_evidence_coverage,
                )

            if (
                measurement.evidence_loss_rate
                > policy.max_evidence_loss_rate
            ):
                add(
                    ProofReason.HIGH_EVIDENCE_LOSS_RATE,
                    "Evidence loss rate exceeds the allowed maximum.",
                    measurement.evidence_loss_rate,
                    policy.max_evidence_loss_rate,
                )

        elif (
            dimension
            == SecurityTestDimension.DISTRIBUTED_CONSISTENCY
        ):
            if (
                measurement.distributed_consistency
                < policy.min_distributed_consistency
            ):
                add(
                    ProofReason.LOW_DISTRIBUTED_CONSISTENCY,
                    "Distributed security-state consistency is "
                    "below the required threshold.",
                    measurement.distributed_consistency,
                    policy.min_distributed_consistency,
                )

        elif dimension == SecurityTestDimension.PARTITION_STABILITY:
            if (
                measurement.partition_stability
                < policy.min_partition_stability
            ):
                add(
                    ProofReason.LOW_PARTITION_STABILITY,
                    "Security partition stability is below "
                    "the required threshold.",
                    measurement.partition_stability,
                    policy.min_partition_stability,
                )

        elif (
            dimension
            == SecurityTestDimension.ENFORCEMENT_CORRECTNESS
        ):
            if (
                measurement.enforcement_correctness
                < policy.min_enforcement_correctness
            ):
                add(
                    ProofReason.LOW_ENFORCEMENT_CORRECTNESS,
                    "Enforcement-decision correctness is below "
                    "the required threshold.",
                    measurement.enforcement_correctness,
                    policy.min_enforcement_correctness,
                )

            if (
                measurement.enforcement_error_rate
                > policy.max_enforcement_error_rate
            ):
                add(
                    ProofReason.HIGH_ENFORCEMENT_ERROR_RATE,
                    "Enforcement error rate exceeds the allowed maximum.",
                    measurement.enforcement_error_rate,
                    policy.max_enforcement_error_rate,
                )

        elif (
            dimension
            == SecurityTestDimension.RECOVERY_CORRECTNESS
        ):
            if (
                measurement.recovery_correctness
                < policy.min_recovery_correctness
            ):
                add(
                    ProofReason.LOW_RECOVERY_CORRECTNESS,
                    "Recovery correctness is below the "
                    "required threshold.",
                    measurement.recovery_correctness,
                    policy.min_recovery_correctness,
                )

            if (
                measurement.recovery_error_rate
                > policy.max_recovery_error_rate
            ):
                add(
                    ProofReason.HIGH_RECOVERY_ERROR_RATE,
                    "Recovery error rate exceeds the allowed maximum.",
                    measurement.recovery_error_rate,
                    policy.max_recovery_error_rate,
                )

        # --------------------------------------------------------------
        # OPERATIONS
        # --------------------------------------------------------------

        elif dimension == SecurityTestDimension.QUEUE_DRAIN:
            if (
                measurement.queue_drain_rate
                < policy.min_queue_drain_rate
            ):
                add(
                    ProofReason.LOW_QUEUE_DRAIN,
                    "Security queue drain rate is below the "
                    "required threshold.",
                    measurement.queue_drain_rate,
                    policy.min_queue_drain_rate,
                )

        elif dimension == SecurityTestDimension.RESOURCE_EFFICIENCY:
            if (
                measurement.resource_efficiency
                < policy.min_resource_efficiency
            ):
                add(
                    ProofReason.LOW_RESOURCE_EFFICIENCY,
                    "Security-system resource efficiency is below "
                    "the required threshold.",
                    measurement.resource_efficiency,
                    policy.min_resource_efficiency,
                )

        elif dimension == SecurityTestDimension.OVERLOAD_RESILIENCE:
            if (
                measurement.overload_resilience
                < policy.min_overload_resilience
            ):
                add(
                    ProofReason.LOW_OVERLOAD_RESILIENCE,
                    "Overload resilience is below the "
                    "required threshold.",
                    measurement.overload_resilience,
                    policy.min_overload_resilience,
                )

        elif dimension == SecurityTestDimension.AUDITABILITY:
            if measurement.auditability < policy.min_auditability:
                add(
                    ProofReason.LOW_AUDITABILITY,
                    "Auditability is below the required threshold.",
                    measurement.auditability,
                    policy.min_auditability,
                )

        elif (
            dimension
            == SecurityTestDimension.PROVENANCE_COMPLETENESS
        ):
            if (
                measurement.provenance_completeness
                < policy.min_provenance_completeness
            ):
                add(
                    ProofReason.LOW_PROVENANCE_COMPLETENESS,
                    "Evidence provenance completeness is below "
                    "the required threshold.",
                    measurement.provenance_completeness,
                    policy.min_provenance_completeness,
                )

        elif dimension == SecurityTestDimension.SYSTEM_AVAILABILITY:
            if (
                measurement.system_availability
                < policy.min_system_availability
            ):
                add(
                    ProofReason.LOW_SYSTEM_AVAILABILITY,
                    "Security-system availability is below "
                    "the required threshold.",
                    measurement.system_availability,
                    policy.min_system_availability,
                )

        # --------------------------------------------------------------
        # LATENCY
        # --------------------------------------------------------------

        elif dimension in (
            SecurityTestDimension.P50_LATENCY,
            SecurityTestDimension.P95_LATENCY,
            SecurityTestDimension.P99_LATENCY,
        ):
            findings.extend(
                self._evaluate_latency_dimension(
                    dimension,
                    measurement,
                    index,
                )
            )

        # --------------------------------------------------------------
        # GLOBAL ERROR CONTROLS
        # --------------------------------------------------------------

        if (
            measurement.security_event_loss_rate
            > policy.max_security_event_loss_rate
        ):
            add(
                ProofReason.HIGH_SECURITY_EVENT_LOSS_RATE,
                "Security-event loss rate exceeds the allowed maximum.",
                measurement.security_event_loss_rate,
                policy.max_security_event_loss_rate,
            )

        if (
            measurement.quality_regression_rate
            > policy.max_quality_regression_rate
        ):
            add(
                ProofReason.HIGH_QUALITY_REGRESSION_RATE,
                "Security/quality regression rate exceeds "
                "the allowed maximum.",
                measurement.quality_regression_rate,
                policy.max_quality_regression_rate,
            )

        if measurement.timeout_rate > policy.max_timeout_rate:
            add(
                ProofReason.HIGH_TIMEOUT_RATE,
                "Security-system timeout rate exceeds "
                "the allowed maximum.",
                measurement.timeout_rate,
                policy.max_timeout_rate,
            )

        return tuple(findings)

    def _evaluate_latency_dimension(
        self,
        dimension: SecurityTestDimension,
        measurement: SpamAbuseSecurityMeasurement,
        index: int,
    ) -> Tuple[SecurityFinding, ...]:
        if dimension == SecurityTestDimension.P50_LATENCY:
            observed = measurement.p50_latency_ms
            maximum = self.policy.max_p50_latency_ms
            reason = ProofReason.HIGH_P50_LATENCY

        elif dimension == SecurityTestDimension.P95_LATENCY:
            observed = measurement.p95_latency_ms
            maximum = self.policy.max_p95_latency_ms
            reason = ProofReason.HIGH_P95_LATENCY

        else:
            observed = measurement.p99_latency_ms
            maximum = self.policy.max_p99_latency_ms
            reason = ProofReason.HIGH_P99_LATENCY

        if observed > maximum:
            return (
                SecurityFinding(
                    dimension=dimension,
                    reason=reason,
                    message=(
                        f"{dimension.value} latency exceeds "
                        "the configured maximum."
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
        dimension: SecurityTestDimension,
        measurements: Sequence[SpamAbuseSecurityMeasurement],
    ) -> float:
        if not measurements:
            return 0.0

        values = [
            self._single_dimension_score(
                dimension,
                measurement,
            )
            for measurement in measurements
        ]

        return self._mean(values)

    def _single_dimension_score(
        self,
        dimension: SecurityTestDimension,
        measurement: SpamAbuseSecurityMeasurement,
    ) -> float:
        volume_dimensions = {
            SecurityTestDimension.RESOURCE_SCALE: (
                measurement.resource_count,
                self.policy.min_resource_count,
            ),
            SecurityTestDimension.SUSPICIOUS_RESOURCE_SCALE: (
                measurement.suspicious_resource_count,
                self.policy.min_suspicious_resource_count,
            ),
            SecurityTestDimension.SECURITY_EVENT_SCALE: (
                measurement.security_event_count,
                self.policy.min_security_event_count,
            ),
            SecurityTestDimension.QUEUE_SCALE: (
                measurement.queue_count,
                self.policy.min_queue_count,
            ),
        }

        if dimension in volume_dimensions:
            observed, minimum = volume_dimensions[dimension]
            return self._volume_quality(observed, minimum)

        threshold_dimensions = {
            SecurityTestDimension.SPAM_DETECTION: (
                measurement.spam_detection_accuracy,
                self.policy.min_spam_detection_accuracy,
            ),
            SecurityTestDimension.ABUSE_DETECTION: (
                measurement.abuse_detection_accuracy,
                self.policy.min_abuse_detection_accuracy,
            ),
            SecurityTestDimension.SECURITY_SIGNAL_COVERAGE: (
                measurement.security_signal_coverage,
                self.policy.min_security_signal_coverage,
            ),
            SecurityTestDimension.MALWARE_DETECTION: (
                measurement.malware_detection_coverage,
                self.policy.min_malware_detection_coverage,
            ),
            SecurityTestDimension.PHISHING_DETECTION: (
                measurement.phishing_detection_coverage,
                self.policy.min_phishing_detection_coverage,
            ),
            SecurityTestDimension.CONTENT_MANIPULATION_DETECTION: (
                measurement.content_manipulation_detection,
                self.policy.min_content_manipulation_detection,
            ),
            SecurityTestDimension.LINK_SPAM_DETECTION: (
                measurement.link_spam_detection,
                self.policy.min_link_spam_detection,
            ),
            SecurityTestDimension.TRUST_SIGNAL_COVERAGE: (
                measurement.trust_signal_coverage,
                self.policy.min_trust_signal_coverage,
            ),
            SecurityTestDimension.DISTRIBUTED_ABUSE_DETECTION: (
                measurement.distributed_abuse_detection,
                self.policy.min_distributed_abuse_detection,
            ),
            SecurityTestDimension.RESOURCE_PROTECTION: (
                measurement.resource_protection,
                self.policy.min_resource_protection,
            ),
            SecurityTestDimension.QUARANTINE_CONTROL: (
                measurement.quarantine_control_correctness,
                self.policy.min_quarantine_control_correctness,
            ),
            SecurityTestDimension.RECOVERY_CONTROL: (
                measurement.recovery_control_correctness,
                self.policy.min_recovery_control_correctness,
            ),
            SecurityTestDimension.FALSE_POSITIVE_CONTROL: (
                measurement.false_positive_control,
                self.policy.min_false_positive_control,
            ),
            SecurityTestDimension.FALSE_NEGATIVE_CONTROL: (
                measurement.false_negative_control,
                self.policy.min_false_negative_control,
            ),
            SecurityTestDimension.DUPLICATE_DETECTION: (
                measurement.duplicate_detection,
                self.policy.min_duplicate_detection,
            ),
            SecurityTestDimension.EVIDENCE_COVERAGE: (
                measurement.evidence_coverage,
                self.policy.min_evidence_coverage,
            ),
            SecurityTestDimension.DISTRIBUTED_CONSISTENCY: (
                measurement.distributed_consistency,
                self.policy.min_distributed_consistency,
            ),
            SecurityTestDimension.PARTITION_STABILITY: (
                measurement.partition_stability,
                self.policy.min_partition_stability,
            ),
            SecurityTestDimension.ENFORCEMENT_CORRECTNESS: (
                measurement.enforcement_correctness,
                self.policy.min_enforcement_correctness,
            ),
            SecurityTestDimension.RECOVERY_CORRECTNESS: (
                measurement.recovery_correctness,
                self.policy.min_recovery_correctness,
            ),
            SecurityTestDimension.QUEUE_DRAIN: (
                measurement.queue_drain_rate,
                self.policy.min_queue_drain_rate,
            ),
            SecurityTestDimension.RESOURCE_EFFICIENCY: (
                measurement.resource_efficiency,
                self.policy.min_resource_efficiency,
            ),
            SecurityTestDimension.OVERLOAD_RESILIENCE: (
                measurement.overload_resilience,
                self.policy.min_overload_resilience,
            ),
            SecurityTestDimension.AUDITABILITY: (
                measurement.auditability,
                self.policy.min_auditability,
            ),
            SecurityTestDimension.PROVENANCE_COMPLETENESS: (
                measurement.provenance_completeness,
                self.policy.min_provenance_completeness,
            ),
            SecurityTestDimension.SYSTEM_AVAILABILITY: (
                measurement.system_availability,
                self.policy.min_system_availability,
            ),
        }

        if dimension in threshold_dimensions:
            observed, minimum = threshold_dimensions[dimension]
            return self._threshold_quality(observed, minimum)

        inverse_dimensions = {
            SecurityTestDimension.FALSE_POSITIVE_CONTROL: (
                measurement.false_positive_rate,
                self.policy.max_false_positive_rate,
            ),
            SecurityTestDimension.FALSE_NEGATIVE_CONTROL: (
                measurement.false_negative_rate,
                self.policy.max_false_negative_rate,
            ),
        }

        if dimension in inverse_dimensions:
            observed, maximum = inverse_dimensions[dimension]
            return self._inverse_rate_quality(observed, maximum)

        if dimension == SecurityTestDimension.DUPLICATE_DETECTION:
            return self._inverse_rate_quality(
                measurement.duplicate_rate,
                self.policy.max_duplicate_rate,
            )

        if dimension == SecurityTestDimension.EVIDENCE_COVERAGE:
            return self._inverse_rate_quality(
                measurement.evidence_loss_rate,
                self.policy.max_evidence_loss_rate,
            )

        if dimension == SecurityTestDimension.ENFORCEMENT_CORRECTNESS:
            return self._inverse_rate_quality(
                measurement.enforcement_error_rate,
                self.policy.max_enforcement_error_rate,
            )

        if dimension == SecurityTestDimension.RECOVERY_CORRECTNESS:
            return self._inverse_rate_quality(
                measurement.recovery_error_rate,
                self.policy.max_recovery_error_rate,
            )

        if dimension == SecurityTestDimension.P50_LATENCY:
            return self._bounded_latency_quality(
                measurement.p50_latency_ms,
                self.policy.max_p50_latency_ms,
            )

        if dimension == SecurityTestDimension.P95_LATENCY:
            return self._bounded_latency_quality(
                measurement.p95_latency_ms,
                self.policy.max_p95_latency_ms,
            )

        if dimension == SecurityTestDimension.P99_LATENCY:
            return self._bounded_latency_quality(
                measurement.p99_latency_ms,
                self.policy.max_p99_latency_ms,
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
                0.90
                + (
                    extra
                    / max(1.0 - minimum, 1e-9)
                )
                * 0.10,
            )

        if minimum <= 0:
            return 1.0

        return max(
            0.0,
            observed / minimum * 0.90,
        )

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
                0.90
                + ((maximum - observed) / maximum) * 0.10,
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
                0.90
                + ((maximum - observed) / maximum) * 0.10,
            )

        ratio = maximum / observed

        return max(
            0.0,
            min(0.89, ratio * 0.89),
        )

    # ----------------------------------------------------------------------
    # GLOBAL EVALUATION
    # ----------------------------------------------------------------------

    def _evaluate_global(
        self,
        measurements: Sequence[SpamAbuseSecurityMeasurement],
    ) -> GlobalSecurityResult:
        resource_count = self._evaluated_resource_count(
            measurements
        )

        suspicious_count = (
            self._evaluated_suspicious_resource_count(
                measurements
            )
        )

        security_event_count = (
            self._evaluated_security_event_count(
                measurements
            )
        )

        queue_count = self._evaluated_queue_count(
            measurements
        )

        dimensions_evaluated = len(
            [
                item
                for item in self._dimensions
                if item.measurement_count > 0
            ]
        )

        dimensions_required = len(
            self.REQUIRED_DIMENSIONS
        )

        passed_dimensions = len(
            [
                item
                for item in self._dimensions
                if item.passed
            ]
        )

        failed_dimensions = len(
            [
                item
                for item in self._dimensions
                if (
                    item.measurement_count > 0
                    and not item.passed
                )
            ]
        )

        deferred_dimensions = (
            dimensions_required
            - dimensions_evaluated
        )

        decision = ProofDecision.PASS

        if failed_dimensions > 0:
            decision = ProofDecision.FAIL

        elif deferred_dimensions > 0:
            decision = ProofDecision.DEFER

        return GlobalSecurityResult(
            decision=decision,
            score=self._overall_score(),
            scale_band=self._scale_band(
                resource_count
            ),
            evaluated_resource_count=resource_count,
            evaluated_suspicious_resource_count=suspicious_count,
            evaluated_security_event_count=security_event_count,
            evaluated_queue_count=queue_count,
            dimensions_evaluated=dimensions_evaluated,
            dimensions_required=dimensions_required,
            passed_dimensions=passed_dimensions,
            failed_dimensions=failed_dimensions,
            deferred_dimensions=deferred_dimensions,
        )

    def _collect_findings(
        self,
    ) -> Tuple[SecurityFinding, ...]:
        findings = []

        for dimension in self._dimensions:
            findings.extend(dimension.findings)

        return tuple(findings)

    def _make_decision(
        self,
        proof_input: SecurityProofInput,
        global_result: GlobalSecurityResult,
        findings: Sequence[SecurityFinding],
    ) -> Tuple[ProofDecision, ProofReason]:
        if (
            not proof_input.public_web_scope
            and self.policy.require_public_web_scope
        ):
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
                item.evidence_strength
                == EvidenceStrength.SIMULATED
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
    ) -> SecurityProofState:
        if decision == ProofDecision.PASS:
            return SecurityProofState.PASSED

        if decision == ProofDecision.FAIL:
            return SecurityProofState.FAILED

        return SecurityProofState.DEFERRED

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

    def _mean(
        self,
        values: Sequence[float],
    ) -> float:
        if not values:
            return 0.0

        return sum(values) / len(values)

    def _minimum(
        self,
        measurements: Sequence[SpamAbuseSecurityMeasurement],
        attribute: str,
    ) -> float:
        values = [
            float(getattr(item, attribute))
            for item in measurements
        ]

        return min(values) if values else 0.0

    def _maximum(
        self,
        measurements: Sequence[SpamAbuseSecurityMeasurement],
        attribute: str,
    ) -> float:
        values = [
            float(getattr(item, attribute))
            for item in measurements
        ]

        return max(values) if values else 0.0

    def _evaluated_resource_count(
        self,
        measurements: Sequence[SpamAbuseSecurityMeasurement],
    ) -> int:
        return max(
            (
                item.resource_count
                for item in measurements
            ),
            default=0,
        )

    def _evaluated_suspicious_resource_count(
        self,
        measurements: Sequence[SpamAbuseSecurityMeasurement],
    ) -> int:
        return max(
            (
                item.suspicious_resource_count
                for item in measurements
            ),
            default=0,
        )

    def _evaluated_security_event_count(
        self,
        measurements: Sequence[SpamAbuseSecurityMeasurement],
    ) -> int:
        return max(
            (
                item.security_event_count
                for item in measurements
            ),
            default=0,
        )

    def _evaluated_queue_count(
        self,
        measurements: Sequence[SpamAbuseSecurityMeasurement],
    ) -> int:
        return max(
            (
                item.queue_count
                for item in measurements
            ),
            default=0,
        )

    # ----------------------------------------------------------------------
    # SCALE BAND
    # ----------------------------------------------------------------------

    def _scale_band(
        self,
        resource_count: int,
    ) -> SecurityScaleBand:
        if resource_count >= 10**12:
            return SecurityScaleBand.TRILLIONS

        if resource_count >= 10**9:
            return SecurityScaleBand.BILLIONS

        if resource_count >= 10**8:
            return SecurityScaleBand.MASSIVE

        if resource_count >= 10**6:
            return SecurityScaleBand.LARGE

        if resource_count > 0:
            return SecurityScaleBand.SMALL

        return SecurityScaleBand.UNKNOWN

    # ----------------------------------------------------------------------
    # FINALIZATION
    # ----------------------------------------------------------------------

    def _finalize_deferred(
        self,
        reason: ProofReason,
        message: str,
    ) -> SecurityProofResult:
        finding = SecurityFinding(
            dimension=SecurityTestDimension.RESOURCE_SCALE,
            reason=reason,
            message=message,
            severity="deferred",
        )

        self._findings.append(finding)

        return self._finalize(
            self.input,
            self._evaluate_global(
                self.input.measurements
            ),
            tuple(self._findings),
            ProofDecision.DEFER,
            reason,
        )

    def _finalize(
        self,
        proof_input: SecurityProofInput,
        global_result: GlobalSecurityResult,
        findings: Sequence[SecurityFinding],
        decision: ProofDecision,
        reason: ProofReason,
    ) -> SecurityProofResult:
        self.state = self._state_from_decision(
            decision
        )

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
            "evaluated_suspicious_resource_count":
                global_result.evaluated_suspicious_resource_count,
            "evaluated_security_event_count":
                global_result.evaluated_security_event_count,
            "evaluated_queue_count":
                global_result.evaluated_queue_count,
            "dimensions": self._dimensions,
            "findings": findings,
        }

        digest = self._digest(
            digest_payload
        )

        result = SecurityProofResult(
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
            evaluated_suspicious_resource_count=(
                global_result.evaluated_suspicious_resource_count
            ),
            evaluated_security_event_count=(
                global_result.evaluated_security_event_count
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
        checkpoint = SecurityProofCheckpoint(
            checkpoint_id=f"cp-{uuid.uuid4().hex}",
            proof_id=self.identity.proof_id,
            checkpoint_type=checkpoint_type,
            state=self.state,
            created_at=self._now(),
            payload_digest=self._digest(payload),
            metadata=dict(payload),
        )

        self._checkpoints.append(checkpoint)

        self.backend.save_checkpoint(
            checkpoint
        )

    def _emit_event(
        self,
        event_type: EventType,
        payload: Mapping[str, Any],
        *,
        dimension: Optional[SecurityTestDimension] = None,
    ) -> None:
        event = SecurityProofEvent(
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

    def _digest(
        self,
        value: Any,
    ) -> str:
        canonical = self._canonicalize(value)

        encoded = json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")

        return hashlib.sha256(
            encoded
        ).hexdigest()

    def _canonicalize(
        self,
        value: Any,
    ) -> Any:
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

        if hasattr(
            value,
            "__dataclass_fields__",
        ):
            return {
                field_name: self._canonicalize(
                    getattr(value, field_name)
                )
                for field_name
                in value.__dataclass_fields__
            }

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(
                    "Non-finite value cannot be canonicalized."
                )

            return value

        return value

    # ----------------------------------------------------------------------
    # TIME
    # ----------------------------------------------------------------------

    def _now(self) -> str:
        return datetime.now(
            timezone.utc
        ).isoformat()


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================

GlobalSpamAbuseSecurityScaleProof = (
    SpamAbuseSecurityScaleProof
)

Phase15_7SpamAbuseSecurityScaleProof = (
    SpamAbuseSecurityScaleProof
)

SecurityScaleProof = SpamAbuseSecurityScaleProof

SpamAbuseScaleProof = SpamAbuseSecurityScaleProof

MassiveSecurityScaleProof = SpamAbuseSecurityScaleProof


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
    "SecurityProofState",
    "SecurityTestDimension",
    "EvidenceStrength",
    "EvidenceKind",
    "SecurityScaleBand",
    "ProofDecision",
    "ProofReason",
    "CheckpointType",
    "EventType",

    # Dataclasses
    "SecurityProofIdentity",
    "SecurityProofLineage",
    "SpamAbuseSecurityMeasurement",
    "SecurityProofInput",
    "SecurityProofPolicy",
    "SecurityFinding",
    "SecurityDimensionResult",
    "GlobalSecurityResult",
    "SecurityProofCheckpoint",
    "SecurityProofEvent",
    "SecurityProofResult",

    # Backend
    "SecurityProofBackend",
    "InMemorySecurityProofBackend",

    # Main proof engine
    "SpamAbuseSecurityScaleProof",

    # Aliases
    "GlobalSpamAbuseSecurityScaleProof",
    "Phase15_7SpamAbuseSecurityScaleProof",
    "SecurityScaleProof",
    "SpamAbuseScaleProof",
    "MassiveSecurityScaleProof",
]
