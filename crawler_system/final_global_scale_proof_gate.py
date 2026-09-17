"""
OUR SEARCH
Phase 15.9 — Final Global Scale-Proof Gate

Purpose
-------
Final, deterministic, auditable gate for the complete OUR SEARCH
full-scale proof program.

This stage consolidates evidence from:

    15.1 Global End-to-End Scale Gate
    15.2 Massive Crawler & Discovery Stress Proof
    15.3 Massive Index & Storage Stress Proof
    15.4 Retrieval & Query-Serving Scale Proof
    15.5 Ranking & Result-Quality Scale Proof
    15.6 Freshness & Recrawling Scale Proof
    15.7 Spam / Abuse / Security Scale Proof
    15.8 Distributed Failure, Recovery & Resilience Proof

Target
------
billions -> trillions of public-Web resources

Important
---------
This is a CONTROL-PLANE proof gate.

It does NOT:
- crawl the Web
- execute production crawler workers
- execute malware
- perform attacks
- perform destructive failure injection
- mutate production indexes
- access Google Search
- access Google's index
- use Google infrastructure
- use Google ranking technology
- claim that measured evidence automatically equals live Web coverage

Instead, it evaluates externally supplied, authorized, auditable evidence.

A PASS means:

    The supplied evidence satisfies the configured final global
    architecture proof policy.

It does NOT mean:

    OUR SEARCH has already indexed every public Web resource.

Actual live-world capability requires operating the architecture continuously
against the public Web at enormous scale.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence


# ============================================================================
# GLOBAL ARCHITECTURE CONSTANTS
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "final-global-scale-proof-gate.v1"

PHASE = "15.9"
PREVIOUS_STAGE = "15.8"
NEXT_STAGE = "complete"

PHASE_NAME = "Full Scale-Proof Program"
STAGE_NAME = "Final Global Scale-Proof Gate"
NEXT_STAGE_NAME = "Phase 15 Complete"

MAX_RESOURCE_COUNT = 10**18
MAX_QUERY_COUNT = 10**18
MAX_CANDIDATE_COUNT = 10**18
MAX_WORKER_COUNT = 10**12
MAX_SHARD_COUNT = 10**12
MAX_REGION_COUNT = 10**6
MAX_EVENT_COUNT = 10**18
MAX_RESULT_COUNT = 10**18


# ============================================================================
# DEFAULT FINAL POLICY
# ============================================================================

DEFAULT_MIN_RESOURCE_COUNT = 1_000_000_000
DEFAULT_MIN_QUERY_COUNT = 1_000_000
DEFAULT_MIN_CANDIDATE_COUNT = 1_000_000

DEFAULT_MIN_CRAWL_THROUGHPUT = 10_000.0
DEFAULT_MIN_INDEX_THROUGHPUT = 10_000.0
DEFAULT_MIN_QUERY_THROUGHPUT = 10_000.0

DEFAULT_MIN_COVERAGE = 0.95
DEFAULT_MIN_RELEVANCE = 0.90
DEFAULT_MIN_RESULT_QUALITY = 0.85
DEFAULT_MIN_RANKING_CORRECTNESS = 0.95

DEFAULT_MIN_FRESHNESS = 0.90
DEFAULT_MIN_RECRAWL_SUCCESS = 0.95

DEFAULT_MIN_SPAM_DETECTION = 0.95
DEFAULT_MIN_SECURITY_DETECTION = 0.95
DEFAULT_MIN_ABUSE_ENFORCEMENT = 0.95

DEFAULT_MIN_SERVICE_CONTINUITY = 0.99
DEFAULT_MIN_RECOVERY_SUCCESS = 0.98
DEFAULT_MIN_DATA_DURABILITY = 0.999
DEFAULT_MIN_DISTRIBUTED_CONSISTENCY = 0.99

DEFAULT_MIN_OVERLOAD_RESILIENCE = 0.95
DEFAULT_MIN_FAILURE_RECOVERY = 0.98
DEFAULT_MIN_GLOBAL_RESILIENCE = 0.98

DEFAULT_MAX_DATA_LOSS_RATE = 0.001
DEFAULT_MAX_RESULT_LOSS_RATE = 0.01
DEFAULT_MAX_DUPLICATE_RATE = 0.03
DEFAULT_MAX_ERROR_RATE = 0.02
DEFAULT_MAX_TIMEOUT_RATE = 0.02
DEFAULT_MAX_SPAM_FALSE_POSITIVE_RATE = 0.02
DEFAULT_MAX_SECURITY_FALSE_POSITIVE_RATE = 0.02

DEFAULT_MAX_P50_LATENCY_MS = 250.0
DEFAULT_MAX_P95_LATENCY_MS = 750.0
DEFAULT_MAX_P99_LATENCY_MS = 1500.0

DEFAULT_MIN_EVIDENCE_FRESHNESS = 0.90


# ============================================================================
# ENUMS
# ============================================================================


class FinalProofState(str, Enum):
    CREATED = "created"
    COLLECTING = "collecting"
    NORMALIZED = "normalized"
    VALIDATED = "validated"
    EVALUATED = "evaluated"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class FinalProofDimension(str, Enum):
    GLOBAL_RESOURCE_SCALE = "global_resource_scale"
    GLOBAL_QUERY_SCALE = "global_query_scale"
    GLOBAL_CANDIDATE_SCALE = "global_candidate_scale"

    CRAWLER_SCALE = "crawler_scale"
    DISCOVERY_SCALE = "discovery_scale"
    CRAWL_THROUGHPUT = "crawl_throughput"

    INDEX_SCALE = "index_scale"
    STORAGE_SCALE = "storage_scale"
    INDEX_THROUGHPUT = "index_throughput"

    RETRIEVAL_SCALE = "retrieval_scale"
    QUERY_SERVING_SCALE = "query_serving_scale"
    QUERY_THROUGHPUT = "query_throughput"

    RANKING_SCALE = "ranking_scale"
    RANKING_CORRECTNESS = "ranking_correctness"
    RESULT_QUALITY = "result_quality"
    RESULT_RELEVANCE = "result_relevance"

    FRESHNESS_SCALE = "freshness_scale"
    RECRAWLING_SCALE = "recrawling_scale"
    FRESHNESS_QUALITY = "freshness_quality"

    SPAM_SCALE = "spam_scale"
    ABUSE_SCALE = "abuse_scale"
    SECURITY_SCALE = "security_scale"
    QUALITY_CONTROL = "quality_control"

    DISTRIBUTED_SCALE = "distributed_scale"
    SERVICE_CONTINUITY = "service_continuity"
    FAILURE_RECOVERY = "failure_recovery"
    DATA_DURABILITY = "data_durability"
    DISTRIBUTED_CONSISTENCY = "distributed_consistency"

    OVERLOAD_RESILIENCE = "overload_resilience"
    GLOBAL_RESILIENCE = "global_resilience"

    DATA_LOSS_PREVENTION = "data_loss_prevention"
    RESULT_LOSS_PREVENTION = "result_loss_prevention"
    DUPLICATE_PREVENTION = "duplicate_prevention"
    ERROR_HANDLING = "error_handling"
    TIMEOUT_HANDLING = "timeout_handling"

    P50_LATENCY = "p50_latency"
    P95_LATENCY = "p95_latency"
    P99_LATENCY = "p99_latency"

    EVIDENCE_FRESHNESS = "evidence_freshness"
    EVIDENCE_COMPLETENESS = "evidence_completeness"
    EVIDENCE_PROVENANCE = "evidence_provenance"

    ARCHITECTURE_INDEPENDENCE = "architecture_independence"
    PUBLIC_WEB_SCOPE = "public_web_scope"


class EvidenceStrength(str, Enum):
    OBSERVED = "observed"
    VERIFIED = "verified"
    DERIVED = "derived"
    SIMULATED = "simulated"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class EvidenceKind(str, Enum):
    SCALE = "scale"
    CRAWL = "crawl"
    DISCOVERY = "discovery"
    INDEX = "index"
    STORAGE = "storage"
    RETRIEVAL = "retrieval"
    QUERY = "query"
    RANKING = "ranking"
    QUALITY = "quality"
    FRESHNESS = "freshness"
    RECRAWL = "recrawl"
    SPAM = "spam"
    ABUSE = "abuse"
    SECURITY = "security"
    DISTRIBUTED = "distributed"
    FAILURE = "failure"
    RECOVERY = "recovery"
    RESILIENCE = "resilience"
    LATENCY = "latency"
    DATA = "data"
    PROVENANCE = "provenance"


class FinalScaleBand(str, Enum):
    SMALL = "small"
    LARGE = "large"
    MASSIVE = "massive"
    BILLIONS = "billions"
    TRILLIONS = "trillions"
    UNKNOWN = "unknown"


class FinalDecision(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    DEFER = "defer"


class FinalReason(str, Enum):
    NO_EVIDENCE = "no_evidence"
    INSUFFICIENT_RESOURCE_SCALE = "insufficient_resource_scale"
    INSUFFICIENT_QUERY_SCALE = "insufficient_query_scale"
    INSUFFICIENT_CANDIDATE_SCALE = "insufficient_candidate_scale"

    CRAWLER_FAILURE = "crawler_failure"
    DISCOVERY_FAILURE = "discovery_failure"
    INDEX_FAILURE = "index_failure"
    STORAGE_FAILURE = "storage_failure"
    RETRIEVAL_FAILURE = "retrieval_failure"
    RANKING_FAILURE = "ranking_failure"
    FRESHNESS_FAILURE = "freshness_failure"
    SPAM_FAILURE = "spam_failure"
    ABUSE_FAILURE = "abuse_failure"
    SECURITY_FAILURE = "security_failure"
    DISTRIBUTED_FAILURE = "distributed_failure"
    RESILIENCE_FAILURE = "resilience_failure"

    LOW_COVERAGE = "low_coverage"
    LOW_RELEVANCE = "low_relevance"
    LOW_RESULT_QUALITY = "low_result_quality"
    LOW_RANKING_CORRECTNESS = "low_ranking_correctness"

    LOW_FRESHNESS = "low_freshness"
    LOW_RECRAWL_SUCCESS = "low_recrawl_success"

    LOW_SERVICE_CONTINUITY = "low_service_continuity"
    LOW_RECOVERY_SUCCESS = "low_recovery_success"
    LOW_DURABILITY = "low_durability"
    LOW_DISTRIBUTED_CONSISTENCY = "low_distributed_consistency"
    LOW_OVERLOAD_RESILIENCE = "low_overload_resilience"

    HIGH_DATA_LOSS = "high_data_loss"
    HIGH_RESULT_LOSS = "high_result_loss"
    HIGH_DUPLICATE_RATE = "high_duplicate_rate"
    HIGH_ERROR_RATE = "high_error_rate"
    HIGH_TIMEOUT_RATE = "high_timeout_rate"

    HIGH_SPAM_FALSE_POSITIVE = "high_spam_false_positive"
    HIGH_SECURITY_FALSE_POSITIVE = "high_security_false_positive"

    HIGH_LATENCY = "high_latency"

    INCOMPLETE_EVIDENCE = "incomplete_evidence"
    STALE_EVIDENCE = "stale_evidence"
    INVALID_PROVENANCE = "invalid_provenance"

    GOOGLE_DEPENDENCY = "google_dependency"
    NON_PUBLIC_SCOPE = "non_public_scope"

    PARTIAL_EVIDENCE_DISALLOWED = "partial_evidence_disallowed"
    SIMULATED_EVIDENCE_DISALLOWED = "simulated_evidence_disallowed"

    MISSING_REQUIRED_DIMENSIONS = "missing_required_dimensions"
    GLOBAL_RESULT_FAILED = "global_result_failed"
    INVALID_INPUT = "invalid_input"


class CheckpointType(str, Enum):
    PROOF_CREATED = "proof_created"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    EVIDENCE_VALIDATED = "evidence_validated"
    DIMENSIONS_EVALUATED = "dimensions_evaluated"
    GLOBAL_GATE_EVALUATED = "global_gate_evaluated"
    PROOF_FINALIZED = "proof_finalized"


class EventType(str, Enum):
    PROOF_CREATED = "proof_created"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    EVIDENCE_REJECTED = "evidence_rejected"
    DIMENSION_EVALUATED = "dimension_evaluated"
    GLOBAL_GATE_EVALUATED = "global_gate_evaluated"
    PROOF_PASSED = "proof_passed"
    PROOF_FAILED = "proof_failed"
    PROOF_DEFERRED = "proof_deferred"


# ============================================================================
# DATACLASSES
# ============================================================================


@dataclass(frozen=True)
class FinalProofIdentity:
    proof_id: str
    architecture_version: str
    phase: str
    created_at: str
    target_scope: str


@dataclass(frozen=True)
class FinalProofLineage:
    previous_stage: str
    next_stage: str
    source_system: str = "our_search"
    parent_proof_id: Optional[str] = None
    lineage_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class FinalScaleEvidence:
    """
    Evidence from one Phase 15 stage.

    All metrics are externally supplied.

    A stage can submit multiple metrics in a single evidence record.
    """

    dimension: FinalProofDimension
    source_stage: str

    resource_count: int = 0
    query_count: int = 0
    candidate_count: int = 0

    worker_count: int = 0
    shard_count: int = 0
    region_count: int = 0

    crawl_throughput: Optional[float] = None
    index_throughput: Optional[float] = None
    query_throughput: Optional[float] = None

    coverage: Optional[float] = None
    relevance: Optional[float] = None
    result_quality: Optional[float] = None
    ranking_correctness: Optional[float] = None

    freshness: Optional[float] = None
    recrawl_success: Optional[float] = None

    spam_detection: Optional[float] = None
    abuse_enforcement: Optional[float] = None
    security_detection: Optional[float] = None
    quality_control: Optional[float] = None

    service_continuity: Optional[float] = None
    recovery_success: Optional[float] = None
    data_durability: Optional[float] = None
    distributed_consistency: Optional[float] = None

    overload_resilience: Optional[float] = None
    global_resilience: Optional[float] = None

    data_loss_rate: Optional[float] = None
    result_loss_rate: Optional[float] = None
    duplicate_rate: Optional[float] = None
    error_rate: Optional[float] = None
    timeout_rate: Optional[float] = None

    spam_false_positive_rate: Optional[float] = None
    security_false_positive_rate: Optional[float] = None

    p50_latency_ms: Optional[float] = None
    p95_latency_ms: Optional[float] = None
    p99_latency_ms: Optional[float] = None

    evidence_freshness: Optional[float] = None
    evidence_completeness: Optional[float] = None
    evidence_provenance: Optional[float] = None

    public_web_scope_verified: bool = True
    google_dependency_detected: bool = False

    evidence_strength: EvidenceStrength = EvidenceStrength.UNKNOWN
    evidence_kind: EvidenceKind = EvidenceKind.SCALE

    source: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FinalProofInput:
    proof_id: str
    evidence: List[FinalScaleEvidence]

    target_scope: str = "public_web"

    required_stages: List[str] = field(
        default_factory=lambda: [
            "15.1",
            "15.2",
            "15.3",
            "15.4",
            "15.5",
            "15.6",
            "15.7",
            "15.8",
        ]
    )

    required_dimensions: List[FinalProofDimension] = field(
        default_factory=lambda: list(FinalProofDimension)
    )

    allow_partial_evidence: bool = False
    allow_simulated_evidence: bool = False
    require_all_stages: bool = True
    require_all_dimensions: bool = True

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalProofPolicy:
    min_resource_count: int = DEFAULT_MIN_RESOURCE_COUNT
    min_query_count: int = DEFAULT_MIN_QUERY_COUNT
    min_candidate_count: int = DEFAULT_MIN_CANDIDATE_COUNT

    min_crawl_throughput: float = DEFAULT_MIN_CRAWL_THROUGHPUT
    min_index_throughput: float = DEFAULT_MIN_INDEX_THROUGHPUT
    min_query_throughput: float = DEFAULT_MIN_QUERY_THROUGHPUT

    min_coverage: float = DEFAULT_MIN_COVERAGE
    min_relevance: float = DEFAULT_MIN_RELEVANCE
    min_result_quality: float = DEFAULT_MIN_RESULT_QUALITY
    min_ranking_correctness: float = DEFAULT_MIN_RANKING_CORRECTNESS

    min_freshness: float = DEFAULT_MIN_FRESHNESS
    min_recrawl_success: float = DEFAULT_MIN_RECRAWL_SUCCESS

    min_spam_detection: float = DEFAULT_MIN_SPAM_DETECTION
    min_security_detection: float = DEFAULT_MIN_SECURITY_DETECTION
    min_abuse_enforcement: float = DEFAULT_MIN_ABUSE_ENFORCEMENT

    min_service_continuity: float = DEFAULT_MIN_SERVICE_CONTINUITY
    min_recovery_success: float = DEFAULT_MIN_RECOVERY_SUCCESS
    min_data_durability: float = DEFAULT_MIN_DATA_DURABILITY
    min_distributed_consistency: float = (
        DEFAULT_MIN_DISTRIBUTED_CONSISTENCY
    )

    min_overload_resilience: float = DEFAULT_MIN_OVERLOAD_RESILIENCE
    min_global_resilience: float = DEFAULT_MIN_GLOBAL_RESILIENCE

    max_data_loss_rate: float = DEFAULT_MAX_DATA_LOSS_RATE
    max_result_loss_rate: float = DEFAULT_MAX_RESULT_LOSS_RATE
    max_duplicate_rate: float = DEFAULT_MAX_DUPLICATE_RATE
    max_error_rate: float = DEFAULT_MAX_ERROR_RATE
    max_timeout_rate: float = DEFAULT_MAX_TIMEOUT_RATE

    max_spam_false_positive_rate: float = (
        DEFAULT_MAX_SPAM_FALSE_POSITIVE_RATE
    )
    max_security_false_positive_rate: float = (
        DEFAULT_MAX_SECURITY_FALSE_POSITIVE_RATE
    )

    max_p50_latency_ms: float = DEFAULT_MAX_P50_LATENCY_MS
    max_p95_latency_ms: float = DEFAULT_MAX_P95_LATENCY_MS
    max_p99_latency_ms: float = DEFAULT_MAX_P99_LATENCY_MS

    min_evidence_freshness: float = DEFAULT_MIN_EVIDENCE_FRESHNESS
    min_evidence_completeness: float = 1.0
    min_evidence_provenance: float = 0.95

    require_public_web_scope: bool = True
    require_google_independence: bool = True


@dataclass
class FinalProofFinding:
    dimension: FinalProofDimension
    reason: FinalReason
    message: str
    severity: str = "error"

    observed_value: Optional[float] = None
    required_value: Optional[float] = None

    source_stage: Optional[str] = None
    source: Optional[str] = None


@dataclass
class FinalDimensionResult:
    dimension: FinalProofDimension
    passed: bool
    score: float
    finding_count: int

    findings: List[FinalProofFinding] = field(default_factory=list)


@dataclass
class FinalGlobalResult:
    decision: FinalDecision
    passed: bool

    overall_score: float
    scale_band: FinalScaleBand

    evaluated_stages: int
    required_stages: int

    evaluated_dimensions: int
    passed_dimensions: int
    failed_dimensions: int
    deferred_dimensions: int

    findings: List[FinalProofFinding] = field(default_factory=list)


@dataclass
class FinalProofCheckpoint:
    checkpoint_id: str
    proof_id: str

    checkpoint_type: CheckpointType
    state: FinalProofState

    timestamp: str
    digest: str

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FinalProofEvent:
    event_id: str
    proof_id: str

    event_type: EventType
    timestamp: str

    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FinalProofResult:
    identity: FinalProofIdentity
    lineage: FinalProofLineage

    state: FinalProofState
    decision: FinalDecision

    overall_score: float
    scale_band: FinalScaleBand

    dimensions: List[FinalDimensionResult]
    global_result: FinalGlobalResult

    evaluated_resource_count: int
    evaluated_query_count: int
    evaluated_candidate_count: int

    evaluated_stages: List[str]

    findings: List[FinalProofFinding]

    created_at: str
    completed_at: Optional[str]

    digest: str

    phase_15_complete: bool

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class FinalProofBackend(Protocol):
    def save_result(self, result: FinalProofResult) -> None:
        ...

    def load_result(
        self,
        proof_id: str,
    ) -> Optional[FinalProofResult]:
        ...

    def save_checkpoint(
        self,
        checkpoint: FinalProofCheckpoint,
    ) -> None:
        ...

    def save_event(
        self,
        event: FinalProofEvent,
    ) -> None:
        ...


class InMemoryFinalProofBackend:
    def __init__(self) -> None:
        self.results: Dict[str, FinalProofResult] = {}
        self.checkpoints: Dict[str, FinalProofCheckpoint] = {}
        self.events: Dict[str, FinalProofEvent] = {}

    def save_result(
        self,
        result: FinalProofResult,
    ) -> None:
        self.results[result.identity.proof_id] = result

    def load_result(
        self,
        proof_id: str,
    ) -> Optional[FinalProofResult]:
        return self.results.get(proof_id)

    def save_checkpoint(
        self,
        checkpoint: FinalProofCheckpoint,
    ) -> None:
        self.checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(
        self,
        event: FinalProofEvent,
    ) -> None:
        self.events[event.event_id] = event


# ============================================================================
# FINAL GLOBAL SCALE-PROOF ENGINE
# ============================================================================


class FinalGlobalScaleProofGate:
    """
    Final gate for Phase 15.

    The gate consumes evidence from stages 15.1 through 15.8 and produces
    a deterministic final proof decision.

    It is intentionally separated from live production execution.
    """

    def __init__(
        self,
        proof_input: FinalProofInput,
        policy: Optional[FinalProofPolicy] = None,
        backend: Optional[FinalProofBackend] = None,
        lineage: Optional[FinalProofLineage] = None,
    ) -> None:
        self.input = proof_input
        self.policy = policy or FinalProofPolicy()
        self.backend = backend or InMemoryFinalProofBackend()

        now = self._now()

        self.identity = FinalProofIdentity(
            proof_id=proof_input.proof_id,
            architecture_version=ARCHITECTURE_VERSION,
            phase=PHASE,
            created_at=now,
            target_scope=proof_input.target_scope,
        )

        self.lineage = lineage or FinalProofLineage(
            previous_stage=PREVIOUS_STAGE,
            next_stage=NEXT_STAGE,
        )

        self.state = FinalProofState.CREATED
        self._result: Optional[FinalProofResult] = None

        self._checkpoint(
            CheckpointType.PROOF_CREATED,
            FinalProofState.CREATED,
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

    def evaluate(self) -> FinalProofResult:
        self.state = FinalProofState.COLLECTING

        self._checkpoint(
            CheckpointType.INPUT_ACCEPTED,
            FinalProofState.COLLECTING,
        )

        self._validate_input()

        self._checkpoint(
            CheckpointType.EVIDENCE_VALIDATED,
            FinalProofState.VALIDATED,
        )

        normalized = self._normalize_input()

        self._checkpoint(
            CheckpointType.INPUT_NORMALIZED,
            FinalProofState.NORMALIZED,
        )

        dimensions: List[FinalDimensionResult] = []

        for evidence in normalized.evidence:
            dimension_result = self._evaluate_dimension(evidence)
            dimensions.append(dimension_result)

            self._emit_event(
                EventType.DIMENSION_EVALUATED,
                {
                    "dimension": evidence.dimension.value,
                    "source_stage": evidence.source_stage,
                    "passed": dimension_result.passed,
                    "score": dimension_result.score,
                    "finding_count": (
                        dimension_result.finding_count
                    ),
                },
            )

        self._checkpoint(
            CheckpointType.DIMENSIONS_EVALUATED,
            FinalProofState.EVALUATED,
        )

        global_result = self._evaluate_global(
            normalized,
            dimensions,
        )

        self._emit_event(
            EventType.GLOBAL_GATE_EVALUATED,
            {
                "decision": global_result.decision.value,
                "overall_score": global_result.overall_score,
                "scale_band": global_result.scale_band.value,
                "evaluated_stages": (
                    global_result.evaluated_stages
                ),
            },
        )

        decision = global_result.decision

        self.state = self._state_from_decision(
            decision
        )

        created_at = self.identity.created_at
        completed_at = self._now()

        evaluated_resource_count = (
            self._evaluated_resource_count(normalized)
        )

        evaluated_query_count = (
            self._evaluated_query_count(normalized)
        )

        evaluated_candidate_count = (
            self._evaluated_candidate_count(normalized)
        )

        evaluated_stages = sorted(
            {
                evidence.source_stage
                for evidence in normalized.evidence
            }
        )

        result_payload = {
            "identity": asdict(self.identity),
            "lineage": asdict(self.lineage),
            "state": self.state.value,
            "decision": decision.value,
            "overall_score": global_result.overall_score,
            "scale_band": global_result.scale_band.value,
            "dimensions": [
                self._dimension_to_dict(item)
                for item in dimensions
            ],
            "global_result": self._global_to_dict(
                global_result
            ),
            "evaluated_resource_count": (
                evaluated_resource_count
            ),
            "evaluated_query_count": (
                evaluated_query_count
            ),
            "evaluated_candidate_count": (
                evaluated_candidate_count
            ),
            "evaluated_stages": evaluated_stages,
            "findings": [
                self._finding_to_dict(item)
                for item in global_result.findings
            ],
            "created_at": created_at,
            "completed_at": completed_at,
            "phase_15_complete": (
                decision == FinalDecision.PASS
            ),
            "metadata": normalized.metadata,
        }

        digest = self._digest(result_payload)

        result = FinalProofResult(
            identity=self.identity,
            lineage=self.lineage,
            state=self.state,
            decision=decision,
            overall_score=global_result.overall_score,
            scale_band=global_result.scale_band,
            dimensions=dimensions,
            global_result=global_result,
            evaluated_resource_count=evaluated_resource_count,
            evaluated_query_count=evaluated_query_count,
            evaluated_candidate_count=evaluated_candidate_count,
            evaluated_stages=evaluated_stages,
            findings=global_result.findings,
            created_at=created_at,
            completed_at=completed_at,
            digest=digest,
            phase_15_complete=(
                decision == FinalDecision.PASS
            ),
            metadata=normalized.metadata,
        )

        self._result = result

        self.backend.save_result(result)

        self._checkpoint(
            CheckpointType.GLOBAL_GATE_EVALUATED,
            self.state,
            metadata={
                "decision": decision.value,
                "overall_score": global_result.overall_score,
            },
        )

        self._checkpoint(
            CheckpointType.PROOF_FINALIZED,
            self.state,
            metadata={
                "decision": decision.value,
                "digest": digest,
                "phase_15_complete": (
                    result.phase_15_complete
                ),
            },
        )

        if decision == FinalDecision.PASS:
            self._emit_event(
                EventType.PROOF_PASSED,
                {
                    "digest": digest,
                    "overall_score": (
                        global_result.overall_score
                    ),
                    "phase_15_complete": True,
                },
            )

        elif decision == FinalDecision.FAIL:
            self._emit_event(
                EventType.PROOF_FAILED,
                {
                    "digest": digest,
                    "finding_count": (
                        len(global_result.findings)
                    ),
                },
            )

        else:
            self._emit_event(
                EventType.PROOF_DEFERRED,
                {
                    "digest": digest,
                    "finding_count": (
                        len(global_result.findings)
                    ),
                },
            )

        return result

    def evaluate_many(
        self,
        inputs: Sequence[FinalProofInput],
    ) -> List[FinalProofResult]:
        results: List[FinalProofResult] = []

        for proof_input in inputs:
            gate = FinalGlobalScaleProofGate(
                proof_input=proof_input,
                policy=self.policy,
                backend=self.backend,
                lineage=self.lineage,
            )

            results.append(
                gate.evaluate()
            )

        return results

    def get_result(self) -> Optional[FinalProofResult]:
        if self._result is not None:
            return self._result

        return self.backend.load_result(
            self.identity.proof_id
        )

    # ---------------------------------------------------------------------
    # INPUT VALIDATION
    # ---------------------------------------------------------------------

    def _validate_input(self) -> None:
        if not self.input.proof_id.strip():
            raise ValueError(
                "proof_id must not be empty"
            )

        if not self.input.target_scope.strip():
            raise ValueError(
                "target_scope must not be empty"
            )

        if (
            self.policy.require_public_web_scope
            and self.input.target_scope.lower()
            != "public_web"
        ):
            raise ValueError(
                "public-web scope is required"
            )

        if not self.input.evidence:
            return

        seen_dimensions: set[FinalProofDimension] = set()

        for evidence in self.input.evidence:
            if (
                evidence.dimension
                in seen_dimensions
            ):
                raise ValueError(
                    "duplicate final proof dimension: "
                    + evidence.dimension.value
                )

            seen_dimensions.add(
                evidence.dimension
            )

            self._validate_evidence(
                evidence
            )

    def _validate_evidence(
        self,
        evidence: FinalScaleEvidence,
    ) -> None:
        if not evidence.source_stage.strip():
            raise ValueError(
                "source_stage must not be empty"
            )

        integer_fields = {
            "resource_count": (
                evidence.resource_count
            ),
            "query_count": (
                evidence.query_count
            ),
            "candidate_count": (
                evidence.candidate_count
            ),
            "worker_count": (
                evidence.worker_count
            ),
            "shard_count": (
                evidence.shard_count
            ),
            "region_count": (
                evidence.region_count
            ),
        }

        for name, value in integer_fields.items():
            if value < 0:
                raise ValueError(
                    f"{name} must be non-negative"
                )

        if evidence.resource_count > MAX_RESOURCE_COUNT:
            raise ValueError(
                "resource_count exceeds maximum"
            )

        if evidence.query_count > MAX_QUERY_COUNT:
            raise ValueError(
                "query_count exceeds maximum"
            )

        if evidence.candidate_count > MAX_CANDIDATE_COUNT:
            raise ValueError(
                "candidate_count exceeds maximum"
            )

        if evidence.worker_count > MAX_WORKER_COUNT:
            raise ValueError(
                "worker_count exceeds maximum"
            )

        if evidence.shard_count > MAX_SHARD_COUNT:
            raise ValueError(
                "shard_count exceeds maximum"
            )

        if evidence.region_count > MAX_REGION_COUNT:
            raise ValueError(
                "region_count exceeds maximum"
            )

        bounded_fields = {
            "coverage": evidence.coverage,
            "relevance": evidence.relevance,
            "result_quality": (
                evidence.result_quality
            ),
            "ranking_correctness": (
                evidence.ranking_correctness
            ),
            "freshness": evidence.freshness,
            "recrawl_success": (
                evidence.recrawl_success
            ),
            "spam_detection": (
                evidence.spam_detection
            ),
            "abuse_enforcement": (
                evidence.abuse_enforcement
            ),
            "security_detection": (
                evidence.security_detection
            ),
            "quality_control": (
                evidence.quality_control
            ),
            "service_continuity": (
                evidence.service_continuity
            ),
            "recovery_success": (
                evidence.recovery_success
            ),
            "data_durability": (
                evidence.data_durability
            ),
            "distributed_consistency": (
                evidence.distributed_consistency
            ),
            "overload_resilience": (
                evidence.overload_resilience
            ),
            "global_resilience": (
                evidence.global_resilience
            ),
            "data_loss_rate": (
                evidence.data_loss_rate
            ),
            "result_loss_rate": (
                evidence.result_loss_rate
            ),
            "duplicate_rate": (
                evidence.duplicate_rate
            ),
            "error_rate": (
                evidence.error_rate
            ),
            "timeout_rate": (
                evidence.timeout_rate
            ),
            "spam_false_positive_rate": (
                evidence.spam_false_positive_rate
            ),
            "security_false_positive_rate": (
                evidence.security_false_positive_rate
            ),
            "evidence_freshness": (
                evidence.evidence_freshness
            ),
            "evidence_completeness": (
                evidence.evidence_completeness
            ),
            "evidence_provenance": (
                evidence.evidence_provenance
            ),
        }

        for name, value in bounded_fields.items():
            if value is not None:
                self._validate_unit_interval(
                    name,
                    value,
                )

        throughput_fields = {
            "crawl_throughput": (
                evidence.crawl_throughput
            ),
            "index_throughput": (
                evidence.index_throughput
            ),
            "query_throughput": (
                evidence.query_throughput
            ),
        }

        for name, value in throughput_fields.items():
            if value is not None:
                if (
                    not math.isfinite(value)
                    or value < 0
                ):
                    raise ValueError(
                        f"{name} must be finite "
                        "and non-negative"
                    )

        latency_fields = {
            "p50_latency_ms": (
                evidence.p50_latency_ms
            ),
            "p95_latency_ms": (
                evidence.p95_latency_ms
            ),
            "p99_latency_ms": (
                evidence.p99_latency_ms
            ),
        }

        for name, value in latency_fields.items():
            if value is not None:
                if (
                    not math.isfinite(value)
                    or value < 0
                ):
                    raise ValueError(
                        f"{name} must be finite "
                        "and non-negative"
                    )

        latencies = [
            value
            for value in latency_fields.values()
            if value is not None
        ]

        if latencies != sorted(latencies):
            raise ValueError(
                "latency values must satisfy "
                "p50 <= p95 <= p99"
            )

    @staticmethod
    def _validate_unit_interval(
        name: str,
        value: float,
    ) -> None:
        if not math.isfinite(value):
            raise ValueError(
                f"{name} must be finite"
            )

        if value < 0.0 or value > 1.0:
            raise ValueError(
                f"{name} must be between 0 and 1"
            )

    # ---------------------------------------------------------------------
    # NORMALIZATION
    # ---------------------------------------------------------------------

    def _normalize_input(
        self,
    ) -> FinalProofInput:
        normalized_evidence: List[
            FinalScaleEvidence
        ] = []

        for evidence in self.input.evidence:
            normalized_evidence.append(
                FinalScaleEvidence(
                    **asdict(evidence)
                )
            )

        return FinalProofInput(
            proof_id=self.input.proof_id,
            evidence=normalized_evidence,
            target_scope=self.input.target_scope,
            required_stages=list(
                self.input.required_stages
            ),
            required_dimensions=list(
                self.input.required_dimensions
            ),
            allow_partial_evidence=(
                self.input.allow_partial_evidence
            ),
            allow_simulated_evidence=(
                self.input.allow_simulated_evidence
            ),
            require_all_stages=(
                self.input.require_all_stages
            ),
            require_all_dimensions=(
                self.input.require_all_dimensions
            ),
            metadata=dict(
                self.input.metadata
            ),
        )

    # ---------------------------------------------------------------------
    # DIMENSION EVALUATION
    # ---------------------------------------------------------------------

    def _evaluate_dimension(
        self,
        evidence: FinalScaleEvidence,
    ) -> FinalDimensionResult:
        findings: List[FinalProofFinding] = []
        scores: List[float] = []

        dimension = evidence.dimension

        # ---------------------------------------------------------------
        # GLOBAL SCALE
        # ---------------------------------------------------------------

        if dimension == FinalProofDimension.GLOBAL_RESOURCE_SCALE:
            self._require_min_integer(
                dimension,
                evidence.resource_count,
                self.policy.min_resource_count,
                FinalReason.INSUFFICIENT_RESOURCE_SCALE,
                "resource count",
                findings,
                evidence,
            )

            scores.append(
                self._volume_score(
                    evidence.resource_count,
                    self.policy.min_resource_count,
                )
            )

        elif dimension == FinalProofDimension.GLOBAL_QUERY_SCALE:
            self._require_min_integer(
                dimension,
                evidence.query_count,
                self.policy.min_query_count,
                FinalReason.INSUFFICIENT_QUERY_SCALE,
                "query count",
                findings,
                evidence,
            )

            scores.append(
                self._volume_score(
                    evidence.query_count,
                    self.policy.min_query_count,
                )
            )

        elif dimension == FinalProofDimension.GLOBAL_CANDIDATE_SCALE:
            self._require_min_integer(
                dimension,
                evidence.candidate_count,
                self.policy.min_candidate_count,
                FinalReason.INSUFFICIENT_CANDIDATE_SCALE,
                "candidate count",
                findings,
                evidence,
            )

            scores.append(
                self._volume_score(
                    evidence.candidate_count,
                    self.policy.min_candidate_count,
                )
            )

        # ---------------------------------------------------------------
        # CRAWLER / DISCOVERY
        # ---------------------------------------------------------------

        elif dimension in {
            FinalProofDimension.CRAWLER_SCALE,
            FinalProofDimension.DISCOVERY_SCALE,
        }:
            self._require_min_integer(
                dimension,
                evidence.resource_count,
                self.policy.min_resource_count,
                FinalReason.CRAWLER_FAILURE,
                "crawler/discovery resource scale",
                findings,
                evidence,
            )

            scores.append(
                self._volume_score(
                    evidence.resource_count,
                    self.policy.min_resource_count,
                )
            )

        elif dimension == FinalProofDimension.CRAWL_THROUGHPUT:
            self._require_min_float(
                dimension,
                evidence.crawl_throughput,
                self.policy.min_crawl_throughput,
                FinalReason.CRAWLER_FAILURE,
                "crawl throughput",
                findings,
                evidence,
            )

            scores.append(
                self._throughput_score(
                    evidence.crawl_throughput,
                    self.policy.min_crawl_throughput,
                )
            )

        # ---------------------------------------------------------------
        # INDEX / STORAGE
        # ---------------------------------------------------------------

        elif dimension in {
            FinalProofDimension.INDEX_SCALE,
            FinalProofDimension.STORAGE_SCALE,
        }:
            self._require_min_integer(
                dimension,
                evidence.resource_count,
                self.policy.min_resource_count,
                FinalReason.INDEX_FAILURE,
                "index/storage resource scale",
                findings,
                evidence,
            )

            scores.append(
                self._volume_score(
                    evidence.resource_count,
                    self.policy.min_resource_count,
                )
            )

        elif dimension == FinalProofDimension.INDEX_THROUGHPUT:
            self._require_min_float(
                dimension,
                evidence.index_throughput,
                self.policy.min_index_throughput,
                FinalReason.INDEX_FAILURE,
                "index throughput",
                findings,
                evidence,
            )

            scores.append(
                self._throughput_score(
                    evidence.index_throughput,
                    self.policy.min_index_throughput,
                )
            )

        # ---------------------------------------------------------------
        # RETRIEVAL / QUERY
        # ---------------------------------------------------------------

        elif dimension in {
            FinalProofDimension.RETRIEVAL_SCALE,
            FinalProofDimension.QUERY_SERVING_SCALE,
        }:
            self._require_min_integer(
                dimension,
                evidence.query_count,
                self.policy.min_query_count,
                FinalReason.RETRIEVAL_FAILURE,
                "retrieval/query scale",
                findings,
                evidence,
            )

            scores.append(
                self._volume_score(
                    evidence.query_count,
                    self.policy.min_query_count,
                )
            )

        elif dimension == FinalProofDimension.QUERY_THROUGHPUT:
            self._require_min_float(
                dimension,
                evidence.query_throughput,
                self.policy.min_query_throughput,
                FinalReason.RETRIEVAL_FAILURE,
                "query throughput",
                findings,
                evidence,
            )

            scores.append(
                self._throughput_score(
                    evidence.query_throughput,
                    self.policy.min_query_throughput,
                )
            )

        # ---------------------------------------------------------------
        # RANKING / QUALITY
        # ---------------------------------------------------------------

        elif dimension == FinalProofDimension.RANKING_SCALE:
            self._require_min_integer(
                dimension,
                evidence.candidate_count,
                self.policy.min_candidate_count,
                FinalReason.RANKING_FAILURE,
                "ranking candidate scale",
                findings,
                evidence,
            )

            scores.append(
                self._volume_score(
                    evidence.candidate_count,
                    self.policy.min_candidate_count,
                )
            )

        elif dimension == FinalProofDimension.RANKING_CORRECTNESS:
            self._require_min(
                dimension,
                evidence.ranking_correctness,
                self.policy.min_ranking_correctness,
                FinalReason.LOW_RANKING_CORRECTNESS,
                "ranking correctness",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.ranking_correctness,
                    self.policy.min_ranking_correctness,
                )
            )

        elif dimension == FinalProofDimension.RESULT_QUALITY:
            self._require_min(
                dimension,
                evidence.result_quality,
                self.policy.min_result_quality,
                FinalReason.LOW_RESULT_QUALITY,
                "result quality",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.result_quality,
                    self.policy.min_result_quality,
                )
            )

        elif dimension == FinalProofDimension.RESULT_RELEVANCE:
            self._require_min(
                dimension,
                evidence.relevance,
                self.policy.min_relevance,
                FinalReason.LOW_RELEVANCE,
                "result relevance",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.relevance,
                    self.policy.min_relevance,
                )
            )

        # ---------------------------------------------------------------
        # FRESHNESS
        # ---------------------------------------------------------------

        elif dimension in {
            FinalProofDimension.FRESHNESS_SCALE,
            FinalProofDimension.FRESHNESS_QUALITY,
        }:
            self._require_min(
                dimension,
                evidence.freshness,
                self.policy.min_freshness,
                FinalReason.LOW_FRESHNESS,
                "freshness",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.freshness,
                    self.policy.min_freshness,
                )
            )

        elif dimension == FinalProofDimension.RECRAWLING_SCALE:
            self._require_min(
                dimension,
                evidence.recrawl_success,
                self.policy.min_recrawl_success,
                FinalReason.LOW_RECRAWL_SUCCESS,
                "recrawl success",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.recrawl_success,
                    self.policy.min_recrawl_success,
                )
            )

        # ---------------------------------------------------------------
        # SPAM / ABUSE / SECURITY
        # ---------------------------------------------------------------

        elif dimension == FinalProofDimension.SPAM_SCALE:
            self._require_min(
                dimension,
                evidence.spam_detection,
                self.policy.min_spam_detection,
                FinalReason.SPAM_FAILURE,
                "spam detection",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.spam_detection,
                    self.policy.min_spam_detection,
                )
            )

        elif dimension == FinalProofDimension.ABUSE_SCALE:
            self._require_min(
                dimension,
                evidence.abuse_enforcement,
                self.policy.min_abuse_enforcement,
                FinalReason.ABUSE_FAILURE,
                "abuse enforcement",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.abuse_enforcement,
                    self.policy.min_abuse_enforcement,
                )
            )

        elif dimension == FinalProofDimension.SECURITY_SCALE:
            self._require_min(
                dimension,
                evidence.security_detection,
                self.policy.min_security_detection,
                FinalReason.SECURITY_FAILURE,
                "security detection",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.security_detection,
                    self.policy.min_security_detection,
                )
            )

        elif dimension == FinalProofDimension.QUALITY_CONTROL:
            self._require_min(
                dimension,
                evidence.quality_control,
                0.90,
                FinalReason.SECURITY_FAILURE,
                "quality control",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.quality_control,
                    0.90,
                )
            )

        # ---------------------------------------------------------------
        # DISTRIBUTED / RESILIENCE
        # ---------------------------------------------------------------

        elif dimension == FinalProofDimension.DISTRIBUTED_SCALE:
            self._require_min_integer(
                dimension,
                evidence.worker_count,
                1,
                FinalReason.DISTRIBUTED_FAILURE,
                "worker scale",
                findings,
                evidence,
            )

            self._require_min_integer(
                dimension,
                evidence.shard_count,
                1,
                FinalReason.DISTRIBUTED_FAILURE,
                "shard scale",
                findings,
                evidence,
            )

            scores.extend(
                [
                    self._volume_score(
                        evidence.worker_count,
                        1,
                    ),
                    self._volume_score(
                        evidence.shard_count,
                        1,
                    ),
                ]
            )

        elif dimension == FinalProofDimension.SERVICE_CONTINUITY:
            self._require_min(
                dimension,
                evidence.service_continuity,
                self.policy.min_service_continuity,
                FinalReason.LOW_SERVICE_CONTINUITY,
                "service continuity",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.service_continuity,
                    self.policy.min_service_continuity,
                )
            )

        elif dimension == FinalProofDimension.FAILURE_RECOVERY:
            self._require_min(
                dimension,
                evidence.recovery_success,
                self.policy.min_recovery_success,
                FinalReason.LOW_RECOVERY_SUCCESS,
                "failure recovery",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.recovery_success,
                    self.policy.min_recovery_success,
                )
            )

        elif dimension == FinalProofDimension.DATA_DURABILITY:
            self._require_min(
                dimension,
                evidence.data_durability,
                self.policy.min_data_durability,
                FinalReason.LOW_DURABILITY,
                "data durability",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.data_durability,
                    self.policy.min_data_durability,
                )
            )

        elif dimension == FinalProofDimension.DISTRIBUTED_CONSISTENCY:
            self._require_min(
                dimension,
                evidence.distributed_consistency,
                self.policy.min_distributed_consistency,
                FinalReason.LOW_DISTRIBUTED_CONSISTENCY,
                "distributed consistency",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.distributed_consistency,
                    self.policy.min_distributed_consistency,
                )
            )

        elif dimension == FinalProofDimension.OVERLOAD_RESILIENCE:
            self._require_min(
                dimension,
                evidence.overload_resilience,
                self.policy.min_overload_resilience,
                FinalReason.LOW_OVERLOAD_RESILIENCE,
                "overload resilience",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.overload_resilience,
                    self.policy.min_overload_resilience,
                )
            )

        elif dimension == FinalProofDimension.GLOBAL_RESILIENCE:
            self._require_min(
                dimension,
                evidence.global_resilience,
                self.policy.min_global_resilience,
                FinalReason.RESILIENCE_FAILURE,
                "global resilience",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.global_resilience,
                    self.policy.min_global_resilience,
                )
            )

        # ---------------------------------------------------------------
        # LOSS / ERROR CONTROL
        # ---------------------------------------------------------------

        elif dimension == FinalProofDimension.DATA_LOSS_PREVENTION:
            self._require_max(
                dimension,
                evidence.data_loss_rate,
                self.policy.max_data_loss_rate,
                FinalReason.HIGH_DATA_LOSS,
                "data loss rate",
                findings,
                evidence,
            )

            scores.append(
                self._score_max(
                    evidence.data_loss_rate,
                    self.policy.max_data_loss_rate,
                )
            )

        elif dimension == FinalProofDimension.RESULT_LOSS_PREVENTION:
            self._require_max(
                dimension,
                evidence.result_loss_rate,
                self.policy.max_result_loss_rate,
                FinalReason.HIGH_RESULT_LOSS,
                "result loss rate",
                findings,
                evidence,
            )

            scores.append(
                self._score_max(
                    evidence.result_loss_rate,
                    self.policy.max_result_loss_rate,
                )
            )

        elif dimension == FinalProofDimension.DUPLICATE_PREVENTION:
            self._require_max(
                dimension,
                evidence.duplicate_rate,
                self.policy.max_duplicate_rate,
                FinalReason.HIGH_DUPLICATE_RATE,
                "duplicate rate",
                findings,
                evidence,
            )

            scores.append(
                self._score_max(
                    evidence.duplicate_rate,
                    self.policy.max_duplicate_rate,
                )
            )

        elif dimension == FinalProofDimension.ERROR_HANDLING:
            self._require_max(
                dimension,
                evidence.error_rate,
                self.policy.max_error_rate,
                FinalReason.HIGH_ERROR_RATE,
                "error rate",
                findings,
                evidence,
            )

            scores.append(
                self._score_max(
                    evidence.error_rate,
                    self.policy.max_error_rate,
                )
            )

        elif dimension == FinalProofDimension.TIMEOUT_HANDLING:
            self._require_max(
                dimension,
                evidence.timeout_rate,
                self.policy.max_timeout_rate,
                FinalReason.HIGH_TIMEOUT_RATE,
                "timeout rate",
                findings,
                evidence,
            )

            scores.append(
                self._score_max(
                    evidence.timeout_rate,
                    self.policy.max_timeout_rate,
                )
            )

        # ---------------------------------------------------------------
        # FALSE POSITIVE CONTROL
        # ---------------------------------------------------------------

        elif dimension == FinalProofDimension.SPAM_SCALE:
            if evidence.spam_false_positive_rate is not None:
                self._require_max(
                    dimension,
                    evidence.spam_false_positive_rate,
                    self.policy.max_spam_false_positive_rate,
                    FinalReason.HIGH_SPAM_FALSE_POSITIVE,
                    "spam false-positive rate",
                    findings,
                    evidence,
                )

                scores.append(
                    self._score_max(
                        evidence.spam_false_positive_rate,
                        self.policy.max_spam_false_positive_rate,
                    )
                )

        # ---------------------------------------------------------------
        # LATENCY
        # ---------------------------------------------------------------

        elif dimension in {
            FinalProofDimension.P50_LATENCY,
            FinalProofDimension.P95_LATENCY,
            FinalProofDimension.P99_LATENCY,
        }:
            self._evaluate_latency_dimension(
                dimension,
                evidence,
                findings,
            )

            scores.append(
                self._latency_quality_for_dimension(
                    dimension,
                    evidence,
                )
            )

        # ---------------------------------------------------------------
        # EVIDENCE QUALITY
        # ---------------------------------------------------------------

        elif dimension == FinalProofDimension.EVIDENCE_FRESHNESS:
            self._require_min(
                dimension,
                evidence.evidence_freshness,
                self.policy.min_evidence_freshness,
                FinalReason.STALE_EVIDENCE,
                "evidence freshness",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.evidence_freshness,
                    self.policy.min_evidence_freshness,
                )
            )

        elif dimension == FinalProofDimension.EVIDENCE_COMPLETENESS:
            self._require_min(
                dimension,
                evidence.evidence_completeness,
                self.policy.min_evidence_completeness,
                FinalReason.INCOMPLETE_EVIDENCE,
                "evidence completeness",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.evidence_completeness,
                    self.policy.min_evidence_completeness,
                )
            )

        elif dimension == FinalProofDimension.EVIDENCE_PROVENANCE:
            self._require_min(
                dimension,
                evidence.evidence_provenance,
                self.policy.min_evidence_provenance,
                FinalReason.INVALID_PROVENANCE,
                "evidence provenance",
                findings,
                evidence,
            )

            scores.append(
                self._score_min(
                    evidence.evidence_provenance,
                    self.policy.min_evidence_provenance,
                )
            )

        # ---------------------------------------------------------------
        # ARCHITECTURE INDEPENDENCE
        # ---------------------------------------------------------------

        elif dimension == FinalProofDimension.ARCHITECTURE_INDEPENDENCE:
            if (
                self.policy.require_google_independence
                and evidence.google_dependency_detected
            ):
                findings.append(
                    FinalProofFinding(
                        dimension=dimension,
                        reason=FinalReason.GOOGLE_DEPENDENCY,
                        message=(
                            "Google technology dependency "
                            "was detected in supplied evidence"
                        ),
                        source_stage=evidence.source_stage,
                        source=evidence.source,
                    )
                )

            scores.append(
                0.0
                if evidence.google_dependency_detected
                else 1.0
            )

        elif dimension == FinalProofDimension.PUBLIC_WEB_SCOPE:
            if (
                self.policy.require_public_web_scope
                and not evidence.public_web_scope_verified
            ):
                findings.append(
                    FinalProofFinding(
                        dimension=dimension,
                        reason=FinalReason.NON_PUBLIC_SCOPE,
                        message=(
                            "Public-Web scope was not verified"
                        ),
                        source_stage=evidence.source_stage,
                        source=evidence.source,
                    )
                )

                scores.append(0.0)
            else:
                scores.append(1.0)

        else:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=FinalReason.INVALID_INPUT,
                    message=(
                        "Unsupported final proof dimension"
                    ),
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )

        return FinalDimensionResult(
            dimension=dimension,
            passed=len(findings) == 0,
            score=self._mean(scores),
            finding_count=len(findings),
            findings=findings,
        )

    # ---------------------------------------------------------------------
    # GLOBAL EVALUATION
    # ---------------------------------------------------------------------

    def _evaluate_global(
        self,
        normalized: FinalProofInput,
        dimensions: Sequence[FinalDimensionResult],
    ) -> FinalGlobalResult:
        findings = self._collect_findings(
            dimensions
        )

        evaluated_stages = sorted(
            {
                evidence.source_stage
                for evidence in normalized.evidence
            }
        )

        evaluated_resource_count = (
            self._evaluated_resource_count(
                normalized
            )
        )

        if not normalized.evidence:
            findings.append(
                FinalProofFinding(
                    dimension=(
                        FinalProofDimension
                        .GLOBAL_RESOURCE_SCALE
                    ),
                    reason=FinalReason.NO_EVIDENCE,
                    message=(
                        "No final scale-proof evidence "
                        "was supplied"
                    ),
                )
            )

        if (
            not normalized.allow_partial_evidence
            and any(
                evidence.evidence_strength
                in {
                    EvidenceStrength.PARTIAL,
                    EvidenceStrength.UNKNOWN,
                }
                for evidence in normalized.evidence
            )
        ):
            findings.append(
                FinalProofFinding(
                    dimension=(
                        FinalProofDimension
                        .EVIDENCE_COMPLETENESS
                    ),
                    reason=(
                        FinalReason
                        .PARTIAL_EVIDENCE_DISALLOWED
                    ),
                    message=(
                        "Partial or unknown evidence is "
                        "not allowed by final policy"
                    ),
                )
            )

        if (
            not normalized.allow_simulated_evidence
            and any(
                evidence.evidence_strength
                == EvidenceStrength.SIMULATED
                for evidence in normalized.evidence
            )
        ):
            findings.append(
                FinalProofFinding(
                    dimension=(
                        FinalProofDimension
                        .EVIDENCE_COMPLETENESS
                    ),
                    reason=(
                        FinalReason
                        .SIMULATED_EVIDENCE_DISALLOWED
                    ),
                    message=(
                        "Simulated evidence is not allowed "
                        "by final policy"
                    ),
                )
            )

        if normalized.require_all_stages:
            missing_stages = [
                stage
                for stage in normalized.required_stages
                if stage not in evaluated_stages
            ]

            if missing_stages:
                findings.append(
                    FinalProofFinding(
                        dimension=(
                            FinalProofDimension
                            .EVIDENCE_COMPLETENESS
                        ),
                        reason=(
                            FinalReason
                            .INCOMPLETE_EVIDENCE
                        ),
                        message=(
                            "Required Phase 15 stages are missing: "
                            + ", ".join(missing_stages)
                        ),
                    )
                )

        if normalized.require_all_dimensions:
            supplied_dimensions = {
                evidence.dimension
                for evidence in normalized.evidence
            }

            missing_dimensions = [
                dimension
                for dimension in normalized.required_dimensions
                if dimension not in supplied_dimensions
            ]

            if missing_dimensions:
                findings.append(
                    FinalProofFinding(
                        dimension=(
                            FinalProofDimension
                            .EVIDENCE_COMPLETENESS
                        ),
                        reason=(
                            FinalReason
                            .MISSING_REQUIRED_DIMENSIONS
                        ),
                        message=(
                            "Required final proof dimensions "
                            "are missing: "
                            + ", ".join(
                                dimension.value
                                for dimension in missing_dimensions
                            )
                        ),
                    )
                )

        if (
            evaluated_resource_count
            < self.policy.min_resource_count
        ):
            findings.append(
                FinalProofFinding(
                    dimension=(
                        FinalProofDimension
                        .GLOBAL_RESOURCE_SCALE
                    ),
                    reason=(
                        FinalReason
                        .INSUFFICIENT_RESOURCE_SCALE
                    ),
                    message=(
                        "Final resource-scale evidence is "
                        "below the configured threshold"
                    ),
                    observed_value=float(
                        evaluated_resource_count
                    ),
                    required_value=float(
                        self.policy.min_resource_count
                    ),
                )
            )

        failed_dimensions = sum(
            1
            for result in dimensions
            if not result.passed
        )

        passed_dimensions = sum(
            1
            for result in dimensions
            if result.passed
        )

        overall_score = self._overall_score(
            dimensions
        )

        if findings or failed_dimensions > 0:
            decision = FinalDecision.FAIL
        elif not dimensions:
            decision = FinalDecision.DEFER
        else:
            decision = FinalDecision.PASS

        return FinalGlobalResult(
            decision=decision,
            passed=decision == FinalDecision.PASS,
            overall_score=overall_score,
            scale_band=self._scale_band(
                evaluated_resource_count
            ),
            evaluated_stages=len(evaluated_stages),
            required_stages=len(
                normalized.required_stages
            ),
            evaluated_dimensions=len(dimensions),
            passed_dimensions=passed_dimensions,
            failed_dimensions=failed_dimensions,
            deferred_dimensions=0,
            findings=findings,
        )

    # ---------------------------------------------------------------------
    # REQUIRED VALUE HELPERS
    # ---------------------------------------------------------------------

    def _require_min(
        self,
        dimension: FinalProofDimension,
        observed: Optional[float],
        minimum: float,
        reason: FinalReason,
        label: str,
        findings: List[FinalProofFinding],
        evidence: FinalScaleEvidence,
    ) -> None:
        if observed is None:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=FinalReason.INVALID_INPUT,
                    message=f"{label} is missing",
                    required_value=minimum,
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )
            return

        if observed < minimum:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=reason,
                    message=f"{label} is below minimum",
                    observed_value=observed,
                    required_value=minimum,
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )

    def _require_min_integer(
        self,
        dimension: FinalProofDimension,
        observed: int,
        minimum: int,
        reason: FinalReason,
        label: str,
        findings: List[FinalProofFinding],
        evidence: FinalScaleEvidence,
    ) -> None:
        if observed < minimum:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=reason,
                    message=f"{label} is below minimum",
                    observed_value=float(observed),
                    required_value=float(minimum),
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )

    def _require_min_float(
        self,
        dimension: FinalProofDimension,
        observed: Optional[float],
        minimum: float,
        reason: FinalReason,
        label: str,
        findings: List[FinalProofFinding],
        evidence: FinalScaleEvidence,
    ) -> None:
        if observed is None:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=FinalReason.INVALID_INPUT,
                    message=f"{label} is missing",
                    required_value=minimum,
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )
            return

        if observed < minimum:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=reason,
                    message=f"{label} is below minimum",
                    observed_value=observed,
                    required_value=minimum,
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )

    def _require_max(
        self,
        dimension: FinalProofDimension,
        observed: Optional[float],
        maximum: float,
        reason: FinalReason,
        label: str,
        findings: List[FinalProofFinding],
        evidence: FinalScaleEvidence,
    ) -> None:
        if observed is None:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=FinalReason.INVALID_INPUT,
                    message=f"{label} is missing",
                    required_value=maximum,
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )
            return

        if observed > maximum:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=reason,
                    message=f"{label} exceeds maximum",
                    observed_value=observed,
                    required_value=maximum,
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )

    # ---------------------------------------------------------------------
    # LATENCY
    # ---------------------------------------------------------------------

    def _evaluate_latency_dimension(
        self,
        dimension: FinalProofDimension,
        evidence: FinalScaleEvidence,
        findings: List[FinalProofFinding],
    ) -> None:
        if dimension == FinalProofDimension.P50_LATENCY:
            observed = evidence.p50_latency_ms
            maximum = self.policy.max_p50_latency_ms
            label = "p50 latency"

        elif dimension == FinalProofDimension.P95_LATENCY:
            observed = evidence.p95_latency_ms
            maximum = self.policy.max_p95_latency_ms
            label = "p95 latency"

        else:
            observed = evidence.p99_latency_ms
            maximum = self.policy.max_p99_latency_ms
            label = "p99 latency"

        if observed is None:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=FinalReason.HIGH_LATENCY,
                    message=f"{label} is missing",
                    required_value=maximum,
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )
            return

        if observed > maximum:
            findings.append(
                FinalProofFinding(
                    dimension=dimension,
                    reason=FinalReason.HIGH_LATENCY,
                    message=(
                        f"{label} exceeds configured maximum"
                    ),
                    observed_value=observed,
                    required_value=maximum,
                    source_stage=evidence.source_stage,
                    source=evidence.source,
                )
            )

    def _latency_quality_for_dimension(
        self,
        dimension: FinalProofDimension,
        evidence: FinalScaleEvidence,
    ) -> float:
        if dimension == FinalProofDimension.P50_LATENCY:
            return self._bounded_latency_quality(
                evidence.p50_latency_ms,
                self.policy.max_p50_latency_ms,
            )

        if dimension == FinalProofDimension.P95_LATENCY:
            return self._bounded_latency_quality(
                evidence.p95_latency_ms,
                self.policy.max_p95_latency_ms,
            )

        return self._bounded_latency_quality(
            evidence.p99_latency_ms,
            self.policy.max_p99_latency_ms,
        )

    @staticmethod
    def _bounded_latency_quality(
        observed: Optional[float],
        maximum: float,
    ) -> float:
        if observed is None:
            return 0.0

        if observed <= maximum:
            return 1.0

        return max(
            0.0,
            maximum / observed,
        )

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

        if observed >= 1.0:
            return 0.0

        return max(
            0.0,
            1.0
            - (
                (observed - maximum)
                / max(
                    1e-12,
                    1.0 - maximum,
                )
            ),
        )

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

        return observed / minimum

    @staticmethod
    def _throughput_score(
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
    def _overall_score(
        dimensions: Sequence[FinalDimensionResult],
    ) -> float:
        if not dimensions:
            return 0.0

        return sum(
            result.score
            for result in dimensions
        ) / len(dimensions)

    @staticmethod
    def _mean(
        values: Iterable[float],
    ) -> float:
        values = list(values)

        if not values:
            return 0.0

        return sum(values) / len(values)

    # ---------------------------------------------------------------------
    # FINDINGS
    # ---------------------------------------------------------------------

    @staticmethod
    def _collect_findings(
        dimensions: Sequence[FinalDimensionResult],
    ) -> List[FinalProofFinding]:
        findings: List[FinalProofFinding] = []

        for result in dimensions:
            findings.extend(
                result.findings
            )

        return findings

    # ---------------------------------------------------------------------
    # SCALE
    # ---------------------------------------------------------------------

    @staticmethod
    def _scale_band(
        resource_count: int,
    ) -> FinalScaleBand:
        if resource_count >= 10**12:
            return FinalScaleBand.TRILLIONS

        if resource_count >= 10**9:
            return FinalScaleBand.BILLIONS

        if resource_count >= 10**8:
            return FinalScaleBand.MASSIVE

        if resource_count >= 10**6:
            return FinalScaleBand.LARGE

        if resource_count > 0:
            return FinalScaleBand.SMALL

        return FinalScaleBand.UNKNOWN

    # ---------------------------------------------------------------------
    # METRIC AGGREGATION
    # ---------------------------------------------------------------------

    @staticmethod
    def _evaluated_resource_count(
        proof_input: FinalProofInput,
    ) -> int:
        return max(
            (
                evidence.resource_count
                for evidence in proof_input.evidence
            ),
            default=0,
        )

    @staticmethod
    def _evaluated_query_count(
        proof_input: FinalProofInput,
    ) -> int:
        return max(
            (
                evidence.query_count
                for evidence in proof_input.evidence
            ),
            default=0,
        )

    @staticmethod
    def _evaluated_candidate_count(
        proof_input: FinalProofInput,
    ) -> int:
        return max(
            (
                evidence.candidate_count
                for evidence in proof_input.evidence
            ),
            default=0,
        )

    # ---------------------------------------------------------------------
    # STATE
    # ---------------------------------------------------------------------

    @staticmethod
    def _state_from_decision(
        decision: FinalDecision,
    ) -> FinalProofState:
        if decision == FinalDecision.PASS:
            return FinalProofState.PASSED

        if decision == FinalDecision.FAIL:
            return FinalProofState.FAILED

        return FinalProofState.DEFERRED

    # ---------------------------------------------------------------------
    # CHECKPOINTS
    # ---------------------------------------------------------------------

    def _checkpoint(
        self,
        checkpoint_type: CheckpointType,
        state: FinalProofState,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        timestamp = self._now()

        payload = {
            "proof_id": self.identity.proof_id,
            "checkpoint_type": (
                checkpoint_type.value
            ),
            "state": state.value,
            "timestamp": timestamp,
            "metadata": metadata or {},
        }

        checkpoint = FinalProofCheckpoint(
            checkpoint_id=self._stable_id(
                checkpoint_type.value,
                payload,
            ),
            proof_id=self.identity.proof_id,
            checkpoint_type=checkpoint_type,
            state=state,
            timestamp=timestamp,
            digest=self._digest(payload),
            metadata=metadata or {},
        )

        self.backend.save_checkpoint(
            checkpoint
        )

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

        event = FinalProofEvent(
            event_id=self._stable_id(
                event_type.value,
                event_payload,
            ),
            proof_id=self.identity.proof_id,
            event_type=event_type,
            timestamp=timestamp,
            payload=payload,
        )

        self.backend.save_event(
            event
        )

    # ---------------------------------------------------------------------
    # SERIALIZATION
    # ---------------------------------------------------------------------

    @staticmethod
    def _finding_to_dict(
        finding: FinalProofFinding,
    ) -> Dict[str, Any]:
        return {
            "dimension": (
                finding.dimension.value
            ),
            "reason": finding.reason.value,
            "message": finding.message,
            "severity": finding.severity,
            "observed_value": (
                finding.observed_value
            ),
            "required_value": (
                finding.required_value
            ),
            "source_stage": (
                finding.source_stage
            ),
            "source": finding.source,
        }

    @staticmethod
    def _dimension_to_dict(
        result: FinalDimensionResult,
    ) -> Dict[str, Any]:
        return {
            "dimension": (
                result.dimension.value
            ),
            "passed": result.passed,
            "score": result.score,
            "finding_count": (
                result.finding_count
            ),
            "findings": [
                FinalGlobalScaleProofGate
                ._finding_to_dict(item)
                for item in result.findings
            ],
        }

    @staticmethod
    def _global_to_dict(
        result: FinalGlobalResult,
    ) -> Dict[str, Any]:
        return {
            "decision": (
                result.decision.value
            ),
            "passed": result.passed,
            "overall_score": (
                result.overall_score
            ),
            "scale_band": (
                result.scale_band.value
            ),
            "evaluated_stages": (
                result.evaluated_stages
            ),
            "required_stages": (
                result.required_stages
            ),
            "evaluated_dimensions": (
                result.evaluated_dimensions
            ),
            "passed_dimensions": (
                result.passed_dimensions
            ),
            "failed_dimensions": (
                result.failed_dimensions
            ),
            "deferred_dimensions": (
                result.deferred_dimensions
            ),
            "findings": [
                FinalGlobalScaleProofGate
                ._finding_to_dict(item)
                for item in result.findings
            ],
        }

    # ---------------------------------------------------------------------
    # DETERMINISTIC DIGEST
    # ---------------------------------------------------------------------

    @classmethod
    def _digest(
        cls,
        value: Any,
    ) -> str:
        canonical = cls._canonicalize(
            value
        )

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

        elif hasattr(
            value,
            "__dataclass_fields__",
        ):
            value = asdict(value)

        elif isinstance(
            value,
            Mapping,
        ):
            value = {
                str(key): cls._canonicalize(item)
                for key, item in sorted(
                    value.items(),
                    key=lambda item: str(
                        item[0]
                    ),
                )
            }

        elif isinstance(
            value,
            (list, tuple),
        ):
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
                value = (
                    "Infinity"
                    if value > 0
                    else "-Infinity"
                )

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

        return (
            f"{namespace}-{digest[:32]}"
        )

    @staticmethod
    def _now() -> str:
        return datetime.now(
            timezone.utc
        ).isoformat()


# ============================================================================
# PUBLIC ALIASES
# ============================================================================


GlobalFinalScaleProofGate = (
    FinalGlobalScaleProofGate
)

Phase15_9FinalGlobalScaleProofGate = (
    FinalGlobalScaleProofGate
)

FinalGlobalScaleProof = (
    FinalGlobalScaleProofGate
)

GlobalScaleProofGate = (
    FinalGlobalScaleProofGate
)

Phase15FinalScaleProofGate = (
    FinalGlobalScaleProofGate
)

UltimateGlobalScaleProofGate = (
    FinalGlobalScaleProofGate
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
    "FinalProofState",
    "FinalProofDimension",
    "EvidenceStrength",
    "EvidenceKind",
    "FinalScaleBand",
    "FinalDecision",
    "FinalReason",
    "CheckpointType",
    "EventType",
    "FinalProofIdentity",
    "FinalProofLineage",
    "FinalScaleEvidence",
    "FinalProofInput",
    "FinalProofPolicy",
    "FinalProofFinding",
    "FinalDimensionResult",
    "FinalGlobalResult",
    "FinalProofCheckpoint",
    "FinalProofEvent",
    "FinalProofResult",
    "FinalProofBackend",
    "InMemoryFinalProofBackend",
    "FinalGlobalScaleProofGate",
    "GlobalFinalScaleProofGate",
    "Phase15_9FinalGlobalScaleProofGate",
    "FinalGlobalScaleProof",
    "GlobalScaleProofGate",
    "Phase15FinalScaleProofGate",
    "UltimateGlobalScaleProofGate",
]


# ============================================================================
# MODULE SELF-CHECK
# ============================================================================


def _self_check() -> bool:
    """
    Lightweight construction and evaluation check.

    This intentionally does not execute a production workload.
    """

    evidence = [
        FinalScaleEvidence(
            dimension=FinalProofDimension.ARCHITECTURE_INDEPENDENCE,
            source_stage="15.1",
            resource_count=1_000_000_000,
            evidence_strength=EvidenceStrength.VERIFIED,
            evidence_kind=EvidenceKind.PROVENANCE,
            source="self_check",
            public_web_scope_verified=True,
            google_dependency_detected=False,
        )
    ]

    proof_input = FinalProofInput(
        proof_id="phase15_9_self_check",
        evidence=evidence,
        target_scope="public_web",
        required_stages=["15.1"],
        required_dimensions=[
            FinalProofDimension.ARCHITECTURE_INDEPENDENCE
        ],
        require_all_stages=True,
        require_all_dimensions=True,
        allow_partial_evidence=True,
        allow_simulated_evidence=True,
    )

    policy = FinalProofPolicy(
        min_resource_count=1,
        require_public_web_scope=True,
        require_google_independence=True,
    )

    gate = FinalGlobalScaleProofGate(
        proof_input=proof_input,
        policy=policy,
    )

    result = gate.evaluate()

    return (
        result.identity.proof_id
        == "phase15_9_self_check"
        and result.digest != ""
        and result.overall_score >= 0.0
        and result.overall_score <= 1.0
        and result.phase_15_complete is True
        and result.decision == FinalDecision.PASS
    )


if __name__ == "__main__":
    if not _self_check():
        raise SystemExit(
            "Phase 15.9 final global scale-proof "
            "gate self-check failed"
        )

    print(
        "Phase 15.9 final global scale-proof "
        "gate self-check: PASS"
    )
