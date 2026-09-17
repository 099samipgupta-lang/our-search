"""
OUR SEARCH
Phase 15.1 — Global End-to-End Scale Gate

Purpose
-------
Defines the global control-plane architecture for proving that the complete
OUR SEARCH search-engine pipeline can operate coherently at enormous scale.

Target:
    billions -> trillions of publicly accessible Web resources

Pipeline covered:
    discovery
        -> crawling
        -> storage/indexing
        -> retrieval/query serving
        -> ranking
        -> freshness/recrawling
        -> spam/abuse/security/quality
        -> global end-to-end scale validation

This module is an architecture and validation-gate layer.

It does NOT:
    - crawl the Web
    - perform HTTP requests
    - assign crawler workers
    - mutate a production index
    - execute ranking requests against production infrastructure
    - execute malware
    - attack or scan external systems
    - enforce bans or blocks
    - delete resources
    - replace existing Phase 1-14 systems
    - depend on Google infrastructure, APIs, indexes, crawlers, or ranking
      technology

The gate consumes externally supplied stage measurements and produces a
deterministic, auditable scale-proof result.

Important:
-----------
Passing this gate means the supplied evidence satisfies the configured
architecture-level scale criteria.

It does NOT claim that the real public Web has already been crawled at
billions/trillions scale. Real-world coverage is established only through
actual production operation and measured Web coverage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


# ============================================================================
# ARCHITECTURE CONSTANTS
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "global-end-to-end-scale-gate.v1"

PHASE = "15.1"
PREVIOUS_PHASE = "14.9"
NEXT_STAGE = "15.2"

PHASE_NAME = "Full Scale-Proof Program"
STAGE_NAME = "Global End-to-End Scale Gate"
NEXT_STAGE_NAME = "Massive Crawler & Discovery Stress Proof"

MAX_RESOURCE_COUNT = 10**18
MAX_LATENCY_MS = 10**12
MAX_ERROR_RATE = 1.0
MAX_PERCENTAGE = 100.0

DEFAULT_MIN_RESOURCE_SCALE = 1_000_000
DEFAULT_MIN_PIPELINE_COHERENCE = 0.95
DEFAULT_MAX_ERROR_RATE = 0.05
DEFAULT_MIN_COMPLETENESS = 0.90
DEFAULT_MIN_DURABILITY = 0.95
DEFAULT_MIN_RECOVERY = 0.95
DEFAULT_MIN_PROVENANCE = 0.95
DEFAULT_MIN_OBSERVABILITY = 0.95
DEFAULT_MIN_IDEMPOTENCY = 0.95


# ============================================================================
# ENUMERATIONS
# ============================================================================


class ScaleGateState(str, Enum):
    CREATED = "created"
    COLLECTING = "collecting"
    NORMALIZED = "normalized"
    EVALUATED = "evaluated"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class PipelineStage(str, Enum):
    DISCOVERY = "discovery"
    CRAWLING = "crawling"
    STORAGE_INDEX = "storage_index"
    RETRIEVAL = "retrieval"
    RANKING = "ranking"
    FRESHNESS = "freshness"
    SECURITY_QUALITY = "security_quality"


class EvidenceKind(str, Enum):
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    ERROR_RATE = "error_rate"
    COMPLETENESS = "completeness"
    DURABILITY = "durability"
    RECOVERY = "recovery"
    PROVENANCE = "provenance"
    OBSERVABILITY = "observability"
    IDEMPOTENCY = "idempotency"
    CONSISTENCY = "consistency"
    CAPACITY = "capacity"
    RESOURCE_USAGE = "resource_usage"


class EvidenceStrength(str, Enum):
    OBSERVED = "observed"
    VERIFIED = "verified"
    DERIVED = "derived"
    SIMULATED = "simulated"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class ScaleBand(str, Enum):
    SMALL = "small"
    LARGE = "large"
    MASSIVE = "massive"
    BILLIONS = "billions"
    TRILLIONS = "trillions"
    UNKNOWN = "unknown"


class GateDecision(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    DEFER = "defer"


class GateReason(str, Enum):
    ALL_REQUIRED_GATES_PASSED = "all_required_gates_passed"
    MISSING_EVIDENCE = "missing_evidence"
    INSUFFICIENT_SCALE = "insufficient_scale"
    PIPELINE_INCOHERENCE = "pipeline_incoherence"
    ERROR_RATE_TOO_HIGH = "error_rate_too_high"
    COMPLETENESS_TOO_LOW = "completeness_too_low"
    DURABILITY_TOO_LOW = "durability_too_low"
    RECOVERY_TOO_LOW = "recovery_too_low"
    PROVENANCE_TOO_LOW = "provenance_too_low"
    OBSERVABILITY_TOO_LOW = "observability_too_low"
    IDEMPOTENCY_TOO_LOW = "idempotency_too_low"
    CONSISTENCY_TOO_LOW = "consistency_too_low"
    INVALID_MEASUREMENT = "invalid_measurement"
    PARTIAL_EVIDENCE = "partial_evidence"


class CheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    STAGE_VALIDATED = "stage_validated"
    CROSS_STAGE_VALIDATED = "cross_stage_validated"
    GATE_EVALUATED = "gate_evaluated"
    RESULT_PERSISTED = "result_persisted"


class EventType(str, Enum):
    GATE_CREATED = "gate_created"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    EVIDENCE_REJECTED = "evidence_rejected"
    STAGE_EVALUATED = "stage_evaluated"
    CROSS_STAGE_EVALUATED = "cross_stage_evaluated"
    GATE_PASSED = "gate_passed"
    GATE_FAILED = "gate_failed"
    GATE_DEFERRED = "gate_deferred"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass(frozen=True)
class ScaleGateIdentity:
    gate_id: str
    evaluation_id: str
    created_at: str
    schema_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class ScaleGateLineage:
    source_systems: Tuple[str, ...] = ()
    source_phases: Tuple[str, ...] = ()
    source_stages: Tuple[str, ...] = ()
    parent_evaluation_ids: Tuple[str, ...] = ()
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class StageMeasurement:
    stage: PipelineStage

    resource_count: int
    throughput_per_second: float
    latency_ms: float
    error_rate: float

    completeness: float
    durability: float
    recovery: float
    provenance: float
    observability: float
    idempotency: float
    consistency: float

    evidence_strength: EvidenceStrength = EvidenceStrength.OBSERVED
    source: str = ""
    measurement_window_seconds: float = 0.0

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScaleGateInput:
    identity: ScaleGateIdentity
    lineage: ScaleGateLineage
    measurements: Tuple[StageMeasurement, ...]

    target_resource_count: int

    declared_public_web_scope: bool = True
    partial_evidence_allowed: bool = True

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScaleGatePolicy:
    minimum_resource_count: int = DEFAULT_MIN_RESOURCE_SCALE

    minimum_pipeline_coherence: float = DEFAULT_MIN_PIPELINE_COHERENCE
    maximum_error_rate: float = DEFAULT_MAX_ERROR_RATE

    minimum_completeness: float = DEFAULT_MIN_COMPLETENESS
    minimum_durability: float = DEFAULT_MIN_DURABILITY
    minimum_recovery: float = DEFAULT_MIN_RECOVERY
    minimum_provenance: float = DEFAULT_MIN_PROVENANCE
    minimum_observability: float = DEFAULT_MIN_OBSERVABILITY
    minimum_idempotency: float = DEFAULT_MIN_IDEMPOTENCY

    minimum_consistency: float = DEFAULT_MIN_DURABILITY

    require_all_pipeline_stages: bool = True
    allow_partial_evidence: bool = True

    require_public_web_scope: bool = True
    require_deterministic_evaluation: bool = True


@dataclass(frozen=True)
class GateFinding:
    stage: Optional[PipelineStage]
    reason: GateReason
    message: str

    measured_value: Optional[float] = None
    required_value: Optional[float] = None

    evidence_strength: EvidenceStrength = EvidenceStrength.UNKNOWN
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class StageGateResult:
    stage: PipelineStage

    passed: bool

    resource_count: int
    throughput_per_second: float
    latency_ms: float
    error_rate: float

    completeness: float
    durability: float
    recovery: float
    provenance: float
    observability: float
    idempotency: float
    consistency: float

    findings: Tuple[GateFinding, ...] = ()


@dataclass(frozen=True)
class CrossStageResult:
    passed: bool
    coherence_score: float

    stage_count: int
    expected_stage_count: int

    findings: Tuple[GateFinding, ...] = ()


@dataclass(frozen=True)
class ScaleGateCheckpoint:
    checkpoint_id: str
    checkpoint_type: CheckpointType

    timestamp: str

    gate_id: str
    evaluation_id: str

    payload_digest: str
    sequence_number: int

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScaleGateEvent:
    event_id: str
    event_type: EventType

    timestamp: str

    gate_id: str
    evaluation_id: str

    payload_digest: str

    stage: Optional[PipelineStage] = None

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScaleGateResult:
    identity: ScaleGateIdentity
    lineage: ScaleGateLineage

    state: ScaleGateState
    decision: GateDecision

    target_resource_count: int
    evaluated_resource_count: int

    scale_band: ScaleBand

    stage_results: Tuple[StageGateResult, ...]
    cross_stage_result: CrossStageResult

    findings: Tuple[GateFinding, ...]

    completeness_score: float
    durability_score: float
    recovery_score: float
    provenance_score: float
    observability_score: float
    idempotency_score: float
    consistency_score: float

    overall_score: float

    partial_evidence: bool
    deterministic: bool

    checkpoints: Tuple[ScaleGateCheckpoint, ...] = ()
    events: Tuple[ScaleGateEvent, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class ScaleGateBackend(Protocol):
    def save_result(self, result: ScaleGateResult) -> None:
        ...

    def load_result(self, evaluation_id: str) -> Optional[ScaleGateResult]:
        ...

    def save_checkpoint(self, checkpoint: ScaleGateCheckpoint) -> None:
        ...

    def save_event(self, event: ScaleGateEvent) -> None:
        ...


class InMemoryScaleGateBackend:
    """
    Deterministic reference backend.

    Production deployments can replace this backend with a distributed
    durable implementation without changing the scale-gate decision model.
    """

    def __init__(self) -> None:
        self._results: Dict[str, ScaleGateResult] = {}
        self._checkpoints: Dict[str, ScaleGateCheckpoint] = {}
        self._events: Dict[str, ScaleGateEvent] = {}

    def save_result(self, result: ScaleGateResult) -> None:
        self._results[result.identity.evaluation_id] = result

    def load_result(self, evaluation_id: str) -> Optional[ScaleGateResult]:
        return self._results.get(evaluation_id)

    def save_checkpoint(self, checkpoint: ScaleGateCheckpoint) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(self, event: ScaleGateEvent) -> None:
        self._events[event.event_id] = event


# ============================================================================
# ARCHITECTURE
# ============================================================================


class GlobalEndToEndScaleGate:
    """
    Phase 15.1 global end-to-end scale-proof control plane.

    This architecture validates supplied measurements from the complete
    OUR SEARCH pipeline.

    It intentionally does not execute the systems being measured.
    """

    REQUIRED_STAGES: Tuple[PipelineStage, ...] = (
        PipelineStage.DISCOVERY,
        PipelineStage.CRAWLING,
        PipelineStage.STORAGE_INDEX,
        PipelineStage.RETRIEVAL,
        PipelineStage.RANKING,
        PipelineStage.FRESHNESS,
        PipelineStage.SECURITY_QUALITY,
    )

    def __init__(
        self,
        backend: Optional[ScaleGateBackend] = None,
        policy: Optional[ScaleGatePolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryScaleGateBackend()
        self.policy = policy or ScaleGatePolicy()

    # ------------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------------

    def evaluate(self, gate_input: ScaleGateInput) -> ScaleGateResult:
        self._validate_input(gate_input)

        checkpoints: List[ScaleGateCheckpoint] = []
        events: List[ScaleGateEvent] = []

        self._emit_event(
            events,
            gate_input,
            EventType.GATE_CREATED,
        )

        self._checkpoint(
            checkpoints,
            gate_input,
            CheckpointType.INPUT_ACCEPTED,
            sequence_number=1,
        )

        normalized = self._normalize_input(gate_input)

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.INPUT_NORMALIZED,
            sequence_number=2,
        )

        stage_results: List[StageGateResult] = []

        for index, measurement in enumerate(normalized.measurements, start=1):
            result = self._evaluate_stage(measurement)

            stage_results.append(result)

            self._emit_event(
                events,
                normalized,
                EventType.STAGE_EVALUATED,
                stage=measurement.stage,
            )

            self._checkpoint(
                checkpoints,
                normalized,
                CheckpointType.STAGE_VALIDATED,
                sequence_number=2 + index,
                payload=result,
            )

        cross_stage = self._evaluate_cross_stage(stage_results)

        self._emit_event(
            events,
            normalized,
            EventType.CROSS_STAGE_EVALUATED,
        )

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.CROSS_STAGE_VALIDATED,
            sequence_number=2 + len(stage_results) + 1,
            payload=cross_stage,
        )

        findings = self._collect_global_findings(
            normalized,
            stage_results,
            cross_stage,
        )

        decision = self._make_decision(
            normalized,
            stage_results,
            cross_stage,
            findings,
        )

        state = self._state_from_decision(decision)

        evaluated_resource_count = self._evaluated_resource_count(stage_results)
        scale_band = self._scale_band(
            max(
                evaluated_resource_count,
                normalized.target_resource_count,
            )
        )

        scores = self._aggregate_scores(stage_results)

        overall_score = self._overall_score(
            stage_results=stage_results,
            cross_stage=cross_stage,
            findings=findings,
        )

        partial_evidence = any(
            measurement.evidence_strength == EvidenceStrength.PARTIAL
            or measurement.evidence_strength == EvidenceStrength.SIMULATED
            for measurement in normalized.measurements
        )

        deterministic = self.policy.require_deterministic_evaluation

        result = ScaleGateResult(
            identity=normalized.identity,
            lineage=normalized.lineage,

            state=state,
            decision=decision,

            target_resource_count=normalized.target_resource_count,
            evaluated_resource_count=evaluated_resource_count,

            scale_band=scale_band,

            stage_results=tuple(stage_results),
            cross_stage_result=cross_stage,

            findings=tuple(findings),

            completeness_score=scores["completeness"],
            durability_score=scores["durability"],
            recovery_score=scores["recovery"],
            provenance_score=scores["provenance"],
            observability_score=scores["observability"],
            idempotency_score=scores["idempotency"],
            consistency_score=scores["consistency"],

            overall_score=overall_score,

            partial_evidence=partial_evidence,
            deterministic=deterministic,

            checkpoints=tuple(checkpoints),
            events=tuple(events),

            metadata={
                "architecture_version": ARCHITECTURE_VERSION,
                "phase": PHASE,
                "scale_target": SCALE_TARGET,
                "google_scale_capability_target": GOOGLE_SCALE_CAPABILITY_TARGET,
                "google_technology_dependency": GOOGLE_TECHNOLOGY_DEPENDENCY,
            },
        )

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.GATE_EVALUATED,
            sequence_number=2 + len(stage_results) + 2,
            payload=result,
        )

        self.backend.save_result(result)

        self._emit_event(
            events,
            normalized,
            {
                GateDecision.PASS: EventType.GATE_PASSED,
                GateDecision.FAIL: EventType.GATE_FAILED,
                GateDecision.DEFER: EventType.GATE_DEFERRED,
            }[decision],
        )

        return result

    def evaluate_many(
        self,
        inputs: Iterable[ScaleGateInput],
    ) -> Tuple[ScaleGateResult, ...]:
        results: List[ScaleGateResult] = []

        for item in inputs:
            results.append(self.evaluate(item))

        return tuple(results)

    def get_result(
        self,
        evaluation_id: str,
    ) -> Optional[ScaleGateResult]:
        return self.backend.load_result(evaluation_id)

    # ------------------------------------------------------------------------
    # Input normalization
    # ------------------------------------------------------------------------

    def _validate_input(self, gate_input: ScaleGateInput) -> None:
        if gate_input.identity.gate_id.strip() == "":
            raise ValueError("gate_id must not be empty")

        if gate_input.identity.evaluation_id.strip() == "":
            raise ValueError("evaluation_id must not be empty")

        if gate_input.target_resource_count < 0:
            raise ValueError("target_resource_count must be non-negative")

        if gate_input.target_resource_count > MAX_RESOURCE_COUNT:
            raise ValueError("target_resource_count exceeds supported bound")

        if self.policy.require_public_web_scope:
            if not gate_input.declared_public_web_scope:
                raise ValueError(
                    "public Web scope is required by the Phase 15.1 policy"
                )

        seen: set[PipelineStage] = set()

        for measurement in gate_input.measurements:
            if measurement.stage in seen:
                raise ValueError(
                    f"duplicate measurement for stage: {measurement.stage.value}"
                )

            seen.add(measurement.stage)

            self._validate_measurement(measurement)

    def _validate_measurement(
        self,
        measurement: StageMeasurement,
    ) -> None:
        numeric_fields = (
            ("resource_count", measurement.resource_count),
            ("throughput_per_second", measurement.throughput_per_second),
            ("latency_ms", measurement.latency_ms),
            ("error_rate", measurement.error_rate),
            ("completeness", measurement.completeness),
            ("durability", measurement.durability),
            ("recovery", measurement.recovery),
            ("provenance", measurement.provenance),
            ("observability", measurement.observability),
            ("idempotency", measurement.idempotency),
            ("consistency", measurement.consistency),
        )

        for name, value in numeric_fields:
            if isinstance(value, bool):
                raise ValueError(f"{name} must be numeric")

            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError(f"{name} must be finite")

        if measurement.resource_count < 0:
            raise ValueError("resource_count must be non-negative")

        if measurement.throughput_per_second < 0:
            raise ValueError("throughput_per_second must be non-negative")

        if measurement.latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")

        if not 0.0 <= measurement.error_rate <= MAX_ERROR_RATE:
            raise ValueError("error_rate must be between 0 and 1")

        bounded = (
            ("completeness", measurement.completeness),
            ("durability", measurement.durability),
            ("recovery", measurement.recovery),
            ("provenance", measurement.provenance),
            ("observability", measurement.observability),
            ("idempotency", measurement.idempotency),
            ("consistency", measurement.consistency),
        )

        for name, value in bounded:
            if not 0.0 <= value <= 1.0:
                raise ValueError(
                    f"{name} must be between 0 and 1"
                )

    def _normalize_input(
        self,
        gate_input: ScaleGateInput,
    ) -> ScaleGateInput:
        ordered = tuple(
            sorted(
                gate_input.measurements,
                key=lambda item: item.stage.value,
            )
        )

        return ScaleGateInput(
            identity=gate_input.identity,
            lineage=gate_input.lineage,
            measurements=ordered,
            target_resource_count=gate_input.target_resource_count,
            declared_public_web_scope=gate_input.declared_public_web_scope,
            partial_evidence_allowed=gate_input.partial_evidence_allowed,
            metadata=dict(gate_input.metadata),
        )

    # ------------------------------------------------------------------------
    # Stage evaluation
    # ------------------------------------------------------------------------

    def _evaluate_stage(
        self,
        measurement: StageMeasurement,
    ) -> StageGateResult:
        findings: List[GateFinding] = []

        minimum_scale = self.policy.minimum_resource_count

        if measurement.resource_count < minimum_scale:
            findings.append(
                GateFinding(
                    stage=measurement.stage,
                    reason=GateReason.INSUFFICIENT_SCALE,
                    message=(
                        "Measured resource count is below the configured "
                        "Phase 15.1 scale-validation floor."
                    ),
                    measured_value=float(measurement.resource_count),
                    required_value=float(minimum_scale),
                    evidence_strength=measurement.evidence_strength,
                )
            )

        if measurement.error_rate > self.policy.maximum_error_rate:
            findings.append(
                GateFinding(
                    stage=measurement.stage,
                    reason=GateReason.ERROR_RATE_TOO_HIGH,
                    message=(
                        "Measured error rate exceeds the configured "
                        "maximum."
                    ),
                    measured_value=measurement.error_rate,
                    required_value=self.policy.maximum_error_rate,
                    evidence_strength=measurement.evidence_strength,
                )
            )

        self._append_threshold_finding(
            findings,
            measurement,
            "completeness",
            measurement.completeness,
            self.policy.minimum_completeness,
            GateReason.COMPLETENESS_TOO_LOW,
        )

        self._append_threshold_finding(
            findings,
            measurement,
            "durability",
            measurement.durability,
            self.policy.minimum_durability,
            GateReason.DURABILITY_TOO_LOW,
        )

        self._append_threshold_finding(
            findings,
            measurement,
            "recovery",
            measurement.recovery,
            self.policy.minimum_recovery,
            GateReason.RECOVERY_TOO_LOW,
        )

        self._append_threshold_finding(
            findings,
            measurement,
            "provenance",
            measurement.provenance,
            self.policy.minimum_provenance,
            GateReason.PROVENANCE_TOO_LOW,
        )

        self._append_threshold_finding(
            findings,
            measurement,
            "observability",
            measurement.observability,
            self.policy.minimum_observability,
            GateReason.OBSERVABILITY_TOO_LOW,
        )

        self._append_threshold_finding(
            findings,
            measurement,
            "idempotency",
            measurement.idempotency,
            self.policy.minimum_idempotency,
            GateReason.IDEMPOTENCY_TOO_LOW,
        )

        self._append_threshold_finding(
            findings,
            measurement,
            "consistency",
            measurement.consistency,
            self.policy.minimum_consistency,
            GateReason.CONSISTENCY_TOO_LOW,
        )

        if (
            measurement.evidence_strength == EvidenceStrength.PARTIAL
            and not self.policy.allow_partial_evidence
        ):
            findings.append(
                GateFinding(
                    stage=measurement.stage,
                    reason=GateReason.PARTIAL_EVIDENCE,
                    message="Partial evidence is not permitted by policy.",
                    evidence_strength=measurement.evidence_strength,
                )
            )

        if (
            measurement.evidence_strength == EvidenceStrength.UNKNOWN
        ):
            findings.append(
                GateFinding(
                    stage=measurement.stage,
                    reason=GateReason.MISSING_EVIDENCE,
                    message="Evidence strength is unknown.",
                    evidence_strength=measurement.evidence_strength,
                )
            )

        passed = len(findings) == 0

        return StageGateResult(
            stage=measurement.stage,
            passed=passed,

            resource_count=measurement.resource_count,
            throughput_per_second=measurement.throughput_per_second,
            latency_ms=measurement.latency_ms,
            error_rate=measurement.error_rate,

            completeness=measurement.completeness,
            durability=measurement.durability,
            recovery=measurement.recovery,
            provenance=measurement.provenance,
            observability=measurement.observability,
            idempotency=measurement.idempotency,
            consistency=measurement.consistency,

            findings=tuple(findings),
        )

    def _append_threshold_finding(
        self,
        findings: List[GateFinding],
        measurement: StageMeasurement,
        field_name: str,
        measured: float,
        required: float,
        reason: GateReason,
    ) -> None:
        if measured < required:
            findings.append(
                GateFinding(
                    stage=measurement.stage,
                    reason=reason,
                    message=(
                        f"{field_name} is below the configured "
                        "minimum threshold."
                    ),
                    measured_value=measured,
                    required_value=required,
                    evidence_strength=measurement.evidence_strength,
                )
            )

    # ------------------------------------------------------------------------
    # Cross-stage evaluation
    # ------------------------------------------------------------------------

    def _evaluate_cross_stage(
        self,
        stage_results: Sequence[StageGateResult],
    ) -> CrossStageResult:
        findings: List[GateFinding] = []

        present = {item.stage for item in stage_results}
        expected = set(self.REQUIRED_STAGES)

        missing = expected - present

        if missing and self.policy.require_all_pipeline_stages:
            for stage in sorted(missing, key=lambda item: item.value):
                findings.append(
                    GateFinding(
                        stage=stage,
                        reason=GateReason.MISSING_EVIDENCE,
                        message=(
                            f"Required pipeline stage '{stage.value}' "
                            "has no supplied measurement."
                        ),
                    )
                )

        if not stage_results:
            return CrossStageResult(
                passed=False,
                coherence_score=0.0,
                stage_count=0,
                expected_stage_count=len(self.REQUIRED_STAGES),
                findings=tuple(
                    findings
                    + [
                        GateFinding(
                            stage=None,
                            reason=GateReason.PIPELINE_INCOHERENCE,
                            message="No pipeline stage evidence supplied.",
                        )
                    ]
                ),
            )

        stage_scores: List[float] = []

        for result in stage_results:
            stage_scores.append(
                self._stage_coherence_score(result)
            )

        coherence_score = sum(stage_scores) / len(stage_scores)

        if coherence_score < self.policy.minimum_pipeline_coherence:
            findings.append(
                GateFinding(
                    stage=None,
                    reason=GateReason.PIPELINE_INCOHERENCE,
                    message=(
                        "Cross-stage pipeline coherence is below the "
                        "configured minimum."
                    ),
                    measured_value=coherence_score,
                    required_value=self.policy.minimum_pipeline_coherence,
                )
            )

        return CrossStageResult(
            passed=len(findings) == 0,
            coherence_score=coherence_score,
            stage_count=len(stage_results),
            expected_stage_count=len(self.REQUIRED_STAGES),
            findings=tuple(findings),
        )

    def _stage_coherence_score(
        self,
        result: StageGateResult,
    ) -> float:
        components = (
            result.completeness,
            result.durability,
            result.recovery,
            result.provenance,
            result.observability,
            result.idempotency,
            result.consistency,
            max(0.0, 1.0 - result.error_rate),
        )

        return sum(components) / len(components)

    # ------------------------------------------------------------------------
    # Global findings and decision
    # ------------------------------------------------------------------------

    def _collect_global_findings(
        self,
        gate_input: ScaleGateInput,
        stage_results: Sequence[StageGateResult],
        cross_stage: CrossStageResult,
    ) -> List[GateFinding]:
        findings: List[GateFinding] = []

        for result in stage_results:
            findings.extend(result.findings)

        findings.extend(cross_stage.findings)

        if self.policy.require_public_web_scope:
            if not gate_input.declared_public_web_scope:
                findings.append(
                    GateFinding(
                        stage=None,
                        reason=GateReason.INVALID_MEASUREMENT,
                        message=(
                            "The supplied evidence does not explicitly "
                            "declare public-Web scope."
                        ),
                    )
                )

        if not gate_input.measurements:
            findings.append(
                GateFinding(
                    stage=None,
                    reason=GateReason.MISSING_EVIDENCE,
                    message="No measurements were supplied.",
                )
            )

        return findings

    def _make_decision(
        self,
        gate_input: ScaleGateInput,
        stage_results: Sequence[StageGateResult],
        cross_stage: CrossStageResult,
        findings: Sequence[GateFinding],
    ) -> GateDecision:
        if not stage_results:
            return GateDecision.DEFER

        partial = any(
            measurement.evidence_strength in (
                EvidenceStrength.PARTIAL,
                EvidenceStrength.SIMULATED,
            )
            for measurement in gate_input.measurements
        )

        if partial and not self.policy.allow_partial_evidence:
            return GateDecision.DEFER

        if findings:
            return GateDecision.FAIL

        if not cross_stage.passed:
            return GateDecision.FAIL

        if self.policy.require_all_pipeline_stages:
            present = {item.stage for item in stage_results}

            if not set(self.REQUIRED_STAGES).issubset(present):
                return GateDecision.DEFER

        return GateDecision.PASS

    def _state_from_decision(
        self,
        decision: GateDecision,
    ) -> ScaleGateState:
        if decision == GateDecision.PASS:
            return ScaleGateState.PASSED

        if decision == GateDecision.FAIL:
            return ScaleGateState.FAILED

        return ScaleGateState.DEFERRED

    # ------------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------------

    def _aggregate_scores(
        self,
        stage_results: Sequence[StageGateResult],
    ) -> Dict[str, float]:
        if not stage_results:
            return {
                "completeness": 0.0,
                "durability": 0.0,
                "recovery": 0.0,
                "provenance": 0.0,
                "observability": 0.0,
                "idempotency": 0.0,
                "consistency": 0.0,
            }

        return {
            "completeness": self._mean(
                result.completeness for result in stage_results
            ),
            "durability": self._mean(
                result.durability for result in stage_results
            ),
            "recovery": self._mean(
                result.recovery for result in stage_results
            ),
            "provenance": self._mean(
                result.provenance for result in stage_results
            ),
            "observability": self._mean(
                result.observability for result in stage_results
            ),
            "idempotency": self._mean(
                result.idempotency for result in stage_results
            ),
            "consistency": self._mean(
                result.consistency for result in stage_results
            ),
        }

    def _overall_score(
        self,
        stage_results: Sequence[StageGateResult],
        cross_stage: CrossStageResult,
        findings: Sequence[GateFinding],
    ) -> float:
        if not stage_results:
            return 0.0

        stage_scores = [
            self._stage_quality_score(result)
            for result in stage_results
        ]

        base = self._mean(stage_scores)

        coherence = cross_stage.coherence_score

        score = (base * 0.8) + (coherence * 0.2)

        if findings:
            score *= max(
                0.0,
                1.0 - min(0.5, len(findings) / 100.0),
            )

        return max(0.0, min(1.0, score))

    def _stage_quality_score(
        self,
        result: StageGateResult,
    ) -> float:
        values = (
            result.completeness,
            result.durability,
            result.recovery,
            result.provenance,
            result.observability,
            result.idempotency,
            result.consistency,
            max(0.0, 1.0 - result.error_rate),
        )

        return self._mean(values)

    @staticmethod
    def _mean(values: Iterable[float]) -> float:
        values_tuple = tuple(values)

        if not values_tuple:
            return 0.0

        return sum(values_tuple) / len(values_tuple)

    # ------------------------------------------------------------------------
    # Scale classification
    # ------------------------------------------------------------------------

    def _scale_band(
        self,
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

    @staticmethod
    def _evaluated_resource_count(
        stage_results: Sequence[StageGateResult],
    ) -> int:
        if not stage_results:
            return 0

        return min(
            result.resource_count
            for result in stage_results
        )

    # ------------------------------------------------------------------------
    # Checkpoints/events
    # ------------------------------------------------------------------------

    def _checkpoint(
        self,
        checkpoints: List[ScaleGateCheckpoint],
        gate_input: ScaleGateInput,
        checkpoint_type: CheckpointType,
        sequence_number: int,
        payload: Any = None,
    ) -> None:
        checkpoint_id = self._stable_id(
            "checkpoint",
            gate_input.identity.evaluation_id,
            checkpoint_type.value,
            str(sequence_number),
        )

        payload_digest = self._digest(
            payload if payload is not None else gate_input
        )

        checkpoint = ScaleGateCheckpoint(
            checkpoint_id=checkpoint_id,
            checkpoint_type=checkpoint_type,
            timestamp=self._now(),

            gate_id=gate_input.identity.gate_id,
            evaluation_id=gate_input.identity.evaluation_id,

            payload_digest=payload_digest,
            sequence_number=sequence_number,
        )

        checkpoints.append(checkpoint)
        self.backend.save_checkpoint(checkpoint)

    def _emit_event(
        self,
        events: List[ScaleGateEvent],
        gate_input: ScaleGateInput,
        event_type: EventType,
        stage: Optional[PipelineStage] = None,
    ) -> None:
        event_id = self._stable_id(
            "event",
            gate_input.identity.evaluation_id,
            event_type.value,
            stage.value if stage else "",
        )

        event = ScaleGateEvent(
            event_id=event_id,
            event_type=event_type,
            timestamp=self._now(),

            gate_id=gate_input.identity.gate_id,
            evaluation_id=gate_input.identity.evaluation_id,

            payload_digest=self._digest(
                {
                    "event_type": event_type.value,
                    "stage": stage.value if stage else None,
                }
            ),

            stage=stage,
        )

        events.append(event)
        self.backend.save_event(event)

    # ------------------------------------------------------------------------
    # Deterministic hashing
    # ------------------------------------------------------------------------

    @staticmethod
    def _digest(value: Any) -> str:
        normalized = GlobalEndToEndScaleGate._canonicalize(value)

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
                key: GlobalEndToEndScaleGate._canonicalize(
                    getattr(value, key)
                )
                for key in value.__dataclass_fields__
            }

        if isinstance(value, Mapping):
            return {
                str(key): GlobalEndToEndScaleGate._canonicalize(
                    item
                )
                for key, item in sorted(
                    value.items(),
                    key=lambda pair: str(pair[0]),
                )
            }

        if isinstance(value, (list, tuple, set, frozenset)):
            items = [
                GlobalEndToEndScaleGate._canonicalize(item)
                for item in value
            ]

            if isinstance(value, (set, frozenset)):
                return sorted(
                    items,
                    key=lambda item: json.dumps(
                        item,
                        sort_keys=True,
                        default=str,
                    ),
                )

            return items

        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("non-finite float cannot be canonicalized")

            return value

        return value

    @staticmethod
    def _stable_id(*parts: str) -> str:
        raw = "|".join(parts).encode("utf-8")

        return hashlib.sha256(raw).hexdigest()[:32]

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================

GlobalEndToEndScaleProof = GlobalEndToEndScaleGate

Phase15_1GlobalEndToEndScaleGate = GlobalEndToEndScaleGate

FullScaleProofGlobalEndToEndGate = GlobalEndToEndScaleGate

GlobalScaleGate = GlobalEndToEndScaleGate


# ============================================================================
# PUBLIC EXPORTS
# ============================================================================

__all__ = [
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",
    "ARCHITECTURE_VERSION",
    "PHASE",
    "PREVIOUS_PHASE",
    "NEXT_STAGE",
    "PHASE_NAME",
    "STAGE_NAME",
    "NEXT_STAGE_NAME",

    "ScaleGateState",
    "PipelineStage",
    "EvidenceKind",
    "EvidenceStrength",
    "ScaleBand",
    "GateDecision",
    "GateReason",
    "CheckpointType",
    "EventType",

    "ScaleGateIdentity",
    "ScaleGateLineage",
    "StageMeasurement",
    "ScaleGateInput",
    "ScaleGatePolicy",
    "GateFinding",
    "StageGateResult",
    "CrossStageResult",
    "ScaleGateCheckpoint",
    "ScaleGateEvent",
    "ScaleGateResult",

    "ScaleGateBackend",
    "InMemoryScaleGateBackend",

    "GlobalEndToEndScaleGate",
    "GlobalEndToEndScaleProof",
    "Phase15_1GlobalEndToEndScaleGate",
    "FullScaleProofGlobalEndToEndGate",
    "GlobalScaleGate",
]
