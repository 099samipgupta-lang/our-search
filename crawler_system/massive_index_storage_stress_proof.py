"""
OUR SEARCH
Phase 15.3 — Massive Index & Storage Stress Proof

Purpose
-------
Scale-proof architecture for validating the massive index and storage
subsystem of OUR SEARCH.

Target:
    billions -> trillions of publicly accessible Web resources

Pipeline scope:
    crawled documents
        -> storage admission
        -> document persistence
        -> index persistence
        -> shard distribution
        -> replication
        -> durability
        -> consistency
        -> capacity pressure
        -> recovery
        -> read/write validation
        -> massive-scale proof

This module is a validation/control-plane architecture.

It does NOT:
    - crawl the Web
    - perform HTTP requests
    - fetch external resources
    - execute production indexing
    - mutate a production index
    - assign storage workers
    - delete production data
    - attack external systems
    - execute malware
    - depend on Google infrastructure, APIs, indexes, crawlers, or ranking

The architecture consumes externally supplied measurements and produces a
deterministic, auditable stress-proof result.

Important:
-----------
A passing result means the supplied evidence satisfies the configured
Phase 15.3 validation policy.

It does NOT claim that OUR SEARCH has already stored or indexed billions
or trillions of real Web resources. Actual Web-scale capacity is established
by deploying and operating the storage/index infrastructure at that scale.
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

ARCHITECTURE_VERSION = "massive-index-storage-stress-proof.v1"

PHASE = "15.3"
PREVIOUS_STAGE = "15.2"
NEXT_STAGE = "15.4"

PHASE_NAME = "Full Scale-Proof Program"
STAGE_NAME = "Massive Index & Storage Stress Proof"
NEXT_STAGE_NAME = "Retrieval & Query-Serving Scale Proof"

MAX_RESOURCE_COUNT = 10**18
MAX_STORAGE_BYTES = 10**30
MAX_SHARD_COUNT = 10**15
MAX_REPLICA_COUNT = 10**6

DEFAULT_MIN_RESOURCE_COUNT = 1_000_000
DEFAULT_MIN_DOCUMENT_WRITE_THROUGHPUT = 1_000
DEFAULT_MIN_INDEX_WRITE_THROUGHPUT = 1_000
DEFAULT_MIN_READ_THROUGHPUT = 1_000

DEFAULT_MIN_DURABILITY = 0.99
DEFAULT_MIN_REPLICATION = 0.95
DEFAULT_MIN_CONSISTENCY = 0.95
DEFAULT_MIN_RECOVERY = 0.95
DEFAULT_MIN_SHARD_STABILITY = 0.95
DEFAULT_MIN_CAPACITY_UTILIZATION = 0.50
DEFAULT_MIN_IDEMPOTENCY = 0.95
DEFAULT_MIN_PROVENANCE = 0.95
DEFAULT_MIN_OBSERVABILITY = 0.95

DEFAULT_MAX_WRITE_ERROR_RATE = 0.05
DEFAULT_MAX_READ_ERROR_RATE = 0.05
DEFAULT_MAX_DATA_LOSS_RATE = 0.01
DEFAULT_MAX_CORRUPTION_RATE = 0.01
DEFAULT_MAX_DUPLICATE_RATE = 0.05


# ============================================================================
# ENUMERATIONS
# ============================================================================


class StorageProofState(str, Enum):
    CREATED = "created"
    COLLECTING = "collecting"
    NORMALIZED = "normalized"
    EVALUATED = "evaluated"
    PASSED = "passed"
    FAILED = "failed"
    DEFERRED = "deferred"


class StorageTestDimension(str, Enum):
    RESOURCE_VOLUME = "resource_volume"
    STORAGE_CAPACITY = "storage_capacity"
    DOCUMENT_WRITE_THROUGHPUT = "document_write_throughput"
    INDEX_WRITE_THROUGHPUT = "index_write_throughput"
    READ_THROUGHPUT = "read_throughput"
    REPLICATION = "replication"
    DURABILITY = "durability"
    CONSISTENCY = "consistency"
    SHARD_STABILITY = "shard_stability"
    RECOVERY = "recovery"
    IDEMPOTENCY = "idempotency"
    PROVENANCE = "provenance"
    OBSERVABILITY = "observability"
    ERROR_HANDLING = "error_handling"
    DATA_LOSS_PREVENTION = "data_loss_prevention"
    CORRUPTION_PREVENTION = "corruption_prevention"
    CAPACITY_PRESSURE = "capacity_pressure"


class EvidenceStrength(str, Enum):
    OBSERVED = "observed"
    VERIFIED = "verified"
    DERIVED = "derived"
    SIMULATED = "simulated"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class EvidenceKind(str, Enum):
    RESOURCE_VOLUME = "resource_volume"
    STORAGE_CAPACITY = "storage_capacity"
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    REPLICATION = "replication"
    DURABILITY = "durability"
    CONSISTENCY = "consistency"
    SHARD = "shard"
    RECOVERY = "recovery"
    IDEMPOTENCY = "idempotency"
    PROVENANCE = "provenance"
    OBSERVABILITY = "observability"
    ERROR = "error"
    DATA_LOSS = "data_loss"
    CORRUPTION = "corruption"


class ScaleBand(str, Enum):
    SMALL = "small"
    LARGE = "large"
    MASSIVE = "massive"
    BILLIONS = "billions"
    TRILLIONS = "trillions"
    UNKNOWN = "unknown"


class StorageProofDecision(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    DEFER = "defer"


class StorageProofReason(str, Enum):
    ALL_REQUIRED_DIMENSIONS_PASSED = "all_required_dimensions_passed"
    MISSING_EVIDENCE = "missing_evidence"
    INSUFFICIENT_RESOURCE_VOLUME = "insufficient_resource_volume"
    INSUFFICIENT_STORAGE_CAPACITY = "insufficient_storage_capacity"
    DOCUMENT_WRITE_THROUGHPUT_TOO_LOW = "document_write_throughput_too_low"
    INDEX_WRITE_THROUGHPUT_TOO_LOW = "index_write_throughput_too_low"
    READ_THROUGHPUT_TOO_LOW = "read_throughput_too_low"
    REPLICATION_TOO_LOW = "replication_too_low"
    DURABILITY_TOO_LOW = "durability_too_low"
    CONSISTENCY_TOO_LOW = "consistency_too_low"
    SHARD_STABILITY_TOO_LOW = "shard_stability_too_low"
    RECOVERY_TOO_LOW = "recovery_too_low"
    IDEMPOTENCY_TOO_LOW = "idempotency_too_low"
    PROVENANCE_TOO_LOW = "provenance_too_low"
    OBSERVABILITY_TOO_LOW = "observability_too_low"
    WRITE_ERROR_RATE_TOO_HIGH = "write_error_rate_too_high"
    READ_ERROR_RATE_TOO_HIGH = "read_error_rate_too_high"
    DATA_LOSS_TOO_HIGH = "data_loss_too_high"
    CORRUPTION_TOO_HIGH = "corruption_too_high"
    DUPLICATE_RATE_TOO_HIGH = "duplicate_rate_too_high"
    CAPACITY_PRESSURE_FAILURE = "capacity_pressure_failure"
    INVALID_MEASUREMENT = "invalid_measurement"
    PARTIAL_EVIDENCE = "partial_evidence"


class CheckpointType(str, Enum):
    PROOF_CREATED = "proof_created"
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    DIMENSION_VALIDATED = "dimension_validated"
    STORAGE_EVALUATED = "storage_evaluated"
    PROOF_FINALIZED = "proof_finalized"


class EventType(str, Enum):
    PROOF_CREATED = "proof_created"
    EVIDENCE_ACCEPTED = "evidence_accepted"
    EVIDENCE_REJECTED = "evidence_rejected"
    DIMENSION_EVALUATED = "dimension_evaluated"
    STORAGE_EVALUATED = "storage_evaluated"
    PROOF_PASSED = "proof_passed"
    PROOF_FAILED = "proof_failed"
    PROOF_DEFERRED = "proof_deferred"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass(frozen=True)
class StorageProofIdentity:
    proof_id: str
    evaluation_id: str
    created_at: str
    schema_version: str = ARCHITECTURE_VERSION


@dataclass(frozen=True)
class StorageProofLineage:
    source_systems: Tuple[str, ...] = ()
    source_phases: Tuple[str, ...] = ()
    source_stages: Tuple[str, ...] = ()
    parent_evaluation_ids: Tuple[str, ...] = ()
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class IndexStorageStressMeasurement:
    dimension: StorageTestDimension

    resource_count: int
    document_count: int
    index_entry_count: int

    storage_capacity_bytes: int
    storage_used_bytes: int

    shard_count: int
    replica_count: int

    document_write_throughput_per_second: float
    index_write_throughput_per_second: float
    read_throughput_per_second: float

    durability_rate: float
    replication_rate: float
    consistency_rate: float
    shard_stability_rate: float
    recovery_rate: float
    idempotency_rate: float
    provenance_rate: float
    observability_rate: float

    write_error_rate: float
    read_error_rate: float
    data_loss_rate: float
    corruption_rate: float
    duplicate_rate: float

    capacity_pressure_stable: bool

    measurement_window_seconds: float = 0.0
    evidence_strength: EvidenceStrength = EvidenceStrength.OBSERVED
    source: str = ""

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StorageProofInput:
    identity: StorageProofIdentity
    lineage: StorageProofLineage

    measurements: Tuple[IndexStorageStressMeasurement, ...]

    target_resource_count: int
    target_storage_capacity_bytes: int

    public_web_scope: bool = True
    partial_evidence_allowed: bool = True

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StorageProofPolicy:
    minimum_resource_count: int = DEFAULT_MIN_RESOURCE_COUNT

    minimum_storage_capacity_bytes: int = 1

    minimum_document_write_throughput: float = (
        DEFAULT_MIN_DOCUMENT_WRITE_THROUGHPUT
    )
    minimum_index_write_throughput: float = (
        DEFAULT_MIN_INDEX_WRITE_THROUGHPUT
    )
    minimum_read_throughput: float = DEFAULT_MIN_READ_THROUGHPUT

    minimum_durability_rate: float = DEFAULT_MIN_DURABILITY
    minimum_replication_rate: float = DEFAULT_MIN_REPLICATION
    minimum_consistency_rate: float = DEFAULT_MIN_CONSISTENCY
    minimum_shard_stability_rate: float = DEFAULT_MIN_SHARD_STABILITY
    minimum_recovery_rate: float = DEFAULT_MIN_RECOVERY
    minimum_idempotency_rate: float = DEFAULT_MIN_IDEMPOTENCY
    minimum_provenance_rate: float = DEFAULT_MIN_PROVENANCE
    minimum_observability_rate: float = DEFAULT_MIN_OBSERVABILITY

    maximum_write_error_rate: float = DEFAULT_MAX_WRITE_ERROR_RATE
    maximum_read_error_rate: float = DEFAULT_MAX_READ_ERROR_RATE
    maximum_data_loss_rate: float = DEFAULT_MAX_DATA_LOSS_RATE
    maximum_corruption_rate: float = DEFAULT_MAX_CORRUPTION_RATE
    maximum_duplicate_rate: float = DEFAULT_MAX_DUPLICATE_RATE

    minimum_capacity_utilization: float = DEFAULT_MIN_CAPACITY_UTILIZATION

    require_capacity_pressure_stability: bool = True
    require_all_dimensions: bool = True

    allow_partial_evidence: bool = True
    require_public_web_scope: bool = True
    require_deterministic_evaluation: bool = True


@dataclass(frozen=True)
class StorageFinding:
    dimension: Optional[StorageTestDimension]

    reason: StorageProofReason
    message: str

    measured_value: Optional[float] = None
    required_value: Optional[float] = None

    evidence_strength: EvidenceStrength = EvidenceStrength.UNKNOWN
    evidence_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class StorageDimensionResult:
    dimension: StorageTestDimension
    passed: bool

    resource_count: int
    document_count: int
    index_entry_count: int

    storage_capacity_bytes: int
    storage_used_bytes: int

    shard_count: int
    replica_count: int

    document_write_throughput_per_second: float
    index_write_throughput_per_second: float
    read_throughput_per_second: float

    durability_rate: float
    replication_rate: float
    consistency_rate: float
    shard_stability_rate: float
    recovery_rate: float
    idempotency_rate: float
    provenance_rate: float
    observability_rate: float

    write_error_rate: float
    read_error_rate: float
    data_loss_rate: float
    corruption_rate: float
    duplicate_rate: float

    capacity_pressure_stable: bool

    findings: Tuple[StorageFinding, ...] = ()


@dataclass(frozen=True)
class GlobalStorageResult:
    passed: bool

    dimensions_evaluated: int
    expected_dimensions: int

    aggregate_document_write_throughput: float
    aggregate_index_write_throughput: float
    aggregate_read_throughput: float

    aggregate_durability: float
    aggregate_replication: float
    aggregate_consistency: float
    aggregate_shard_stability: float
    aggregate_recovery: float
    aggregate_idempotency: float
    aggregate_provenance: float
    aggregate_observability: float

    aggregate_write_error_rate: float
    aggregate_read_error_rate: float
    aggregate_data_loss_rate: float
    aggregate_corruption_rate: float
    aggregate_duplicate_rate: float

    findings: Tuple[StorageFinding, ...] = ()


@dataclass(frozen=True)
class StorageProofCheckpoint:
    checkpoint_id: str
    checkpoint_type: CheckpointType

    timestamp: str

    proof_id: str
    evaluation_id: str

    payload_digest: str
    sequence_number: int

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StorageProofEvent:
    event_id: str
    event_type: EventType

    timestamp: str

    proof_id: str
    evaluation_id: str

    payload_digest: str

    dimension: Optional[StorageTestDimension] = None

    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StorageProofResult:
    identity: StorageProofIdentity
    lineage: StorageProofLineage

    state: StorageProofState
    decision: StorageProofDecision

    target_resource_count: int
    target_storage_capacity_bytes: int

    evaluated_resource_count: int
    evaluated_document_count: int
    evaluated_index_entry_count: int

    evaluated_storage_capacity_bytes: int
    evaluated_storage_used_bytes: int

    scale_band: ScaleBand

    dimension_results: Tuple[StorageDimensionResult, ...]
    global_result: GlobalStorageResult

    findings: Tuple[StorageFinding, ...]

    overall_score: float

    partial_evidence: bool
    deterministic: bool

    checkpoints: Tuple[StorageProofCheckpoint, ...] = ()
    events: Tuple[StorageProofEvent, ...] = ()

    metadata: Mapping[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class StorageProofBackend(Protocol):
    def save_result(self, result: StorageProofResult) -> None:
        ...

    def load_result(
        self,
        evaluation_id: str,
    ) -> Optional[StorageProofResult]:
        ...

    def save_checkpoint(
        self,
        checkpoint: StorageProofCheckpoint,
    ) -> None:
        ...

    def save_event(
        self,
        event: StorageProofEvent,
    ) -> None:
        ...


class InMemoryStorageProofBackend:
    """
    Deterministic reference backend.

    Production deployments can replace this with a distributed durable
    implementation without changing the proof model.
    """

    def __init__(self) -> None:
        self._results: Dict[str, StorageProofResult] = {}
        self._checkpoints: Dict[str, StorageProofCheckpoint] = {}
        self._events: Dict[str, StorageProofEvent] = {}

    def save_result(
        self,
        result: StorageProofResult,
    ) -> None:
        self._results[result.identity.evaluation_id] = result

    def load_result(
        self,
        evaluation_id: str,
    ) -> Optional[StorageProofResult]:
        return self._results.get(evaluation_id)

    def save_checkpoint(
        self,
        checkpoint: StorageProofCheckpoint,
    ) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def save_event(
        self,
        event: StorageProofEvent,
    ) -> None:
        self._events[event.event_id] = event


# ============================================================================
# ARCHITECTURE
# ============================================================================


class MassiveIndexStorageStressProof:
    """
    Phase 15.3 massive index/storage stress-proof control plane.

    This class evaluates externally supplied measurements from the index and
    storage infrastructure.

    It intentionally does not execute storage or indexing workloads.
    """

    REQUIRED_DIMENSIONS: Tuple[StorageTestDimension, ...] = (
        StorageTestDimension.RESOURCE_VOLUME,
        StorageTestDimension.STORAGE_CAPACITY,
        StorageTestDimension.DOCUMENT_WRITE_THROUGHPUT,
        StorageTestDimension.INDEX_WRITE_THROUGHPUT,
        StorageTestDimension.READ_THROUGHPUT,
        StorageTestDimension.REPLICATION,
        StorageTestDimension.DURABILITY,
        StorageTestDimension.CONSISTENCY,
        StorageTestDimension.SHARD_STABILITY,
        StorageTestDimension.RECOVERY,
        StorageTestDimension.IDEMPOTENCY,
        StorageTestDimension.PROVENANCE,
        StorageTestDimension.OBSERVABILITY,
        StorageTestDimension.ERROR_HANDLING,
        StorageTestDimension.DATA_LOSS_PREVENTION,
        StorageTestDimension.CORRUPTION_PREVENTION,
        StorageTestDimension.CAPACITY_PRESSURE,
    )

    def __init__(
        self,
        backend: Optional[StorageProofBackend] = None,
        policy: Optional[StorageProofPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryStorageProofBackend()
        self.policy = policy or StorageProofPolicy()

    # ------------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------------

    def evaluate(
        self,
        proof_input: StorageProofInput,
    ) -> StorageProofResult:
        self._validate_input(proof_input)

        checkpoints: list[StorageProofCheckpoint] = []
        events: list[StorageProofEvent] = []

        self._emit_event(
            events,
            proof_input,
            EventType.PROOF_CREATED,
        )

        self._checkpoint(
            checkpoints,
            proof_input,
            CheckpointType.PROOF_CREATED,
            sequence_number=1,
        )

        normalized = self._normalize_input(proof_input)

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.INPUT_NORMALIZED,
            sequence_number=2,
        )

        dimension_results: list[StorageDimensionResult] = []

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
            EventType.STORAGE_EVALUATED,
        )

        self._checkpoint(
            checkpoints,
            normalized,
            CheckpointType.STORAGE_EVALUATED,
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

        evaluated_resource_count = (
            self._evaluated_resource_count(dimension_results)
        )

        evaluated_document_count = (
            self._evaluated_document_count(dimension_results)
        )

        evaluated_index_entry_count = (
            self._evaluated_index_entry_count(dimension_results)
        )

        evaluated_capacity = (
            self._evaluated_storage_capacity(dimension_results)
        )

        evaluated_used = (
            self._evaluated_storage_used(dimension_results)
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

        result = StorageProofResult(
            identity=normalized.identity,
            lineage=normalized.lineage,

            state=state,
            decision=decision,

            target_resource_count=normalized.target_resource_count,
            target_storage_capacity_bytes=(
                normalized.target_storage_capacity_bytes
            ),

            evaluated_resource_count=evaluated_resource_count,
            evaluated_document_count=evaluated_document_count,
            evaluated_index_entry_count=evaluated_index_entry_count,

            evaluated_storage_capacity_bytes=evaluated_capacity,
            evaluated_storage_used_bytes=evaluated_used,

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
                StorageProofDecision.PASS: EventType.PROOF_PASSED,
                StorageProofDecision.FAIL: EventType.PROOF_FAILED,
                StorageProofDecision.DEFER: EventType.PROOF_DEFERRED,
            }[decision],
        )

        return result

    def evaluate_many(
        self,
        inputs: Iterable[StorageProofInput],
    ) -> Tuple[StorageProofResult, ...]:
        return tuple(
            self.evaluate(item)
            for item in inputs
        )

    def get_result(
        self,
        evaluation_id: str,
    ) -> Optional[StorageProofResult]:
        return self.backend.load_result(evaluation_id)

    # ------------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------------

    def _validate_input(
        self,
        proof_input: StorageProofInput,
    ) -> None:
        if not proof_input.identity.proof_id.strip():
            raise ValueError("proof_id must not be empty")

        if not proof_input.identity.evaluation_id.strip():
            raise ValueError("evaluation_id must not be empty")

        if not (
            0
            <= proof_input.target_resource_count
            <= MAX_RESOURCE_COUNT
        ):
            raise ValueError(
                "target_resource_count is outside supported bounds"
            )

        if not (
            0
            <= proof_input.target_storage_capacity_bytes
            <= MAX_STORAGE_BYTES
        ):
            raise ValueError(
                "target_storage_capacity_bytes is outside supported bounds"
            )

        if (
            self.policy.require_public_web_scope
            and not proof_input.public_web_scope
        ):
            raise ValueError(
                "public Web scope is required"
            )

        seen: set[StorageTestDimension] = set()

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
        measurement: IndexStorageStressMeasurement,
    ) -> None:
        integer_fields = (
            ("resource_count", measurement.resource_count),
            ("document_count", measurement.document_count),
            ("index_entry_count", measurement.index_entry_count),
            ("storage_capacity_bytes", measurement.storage_capacity_bytes),
            ("storage_used_bytes", measurement.storage_used_bytes),
            ("shard_count", measurement.shard_count),
            ("replica_count", measurement.replica_count),
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

        if measurement.document_count > MAX_RESOURCE_COUNT:
            raise ValueError(
                "document_count exceeds supported bound"
            )

        if measurement.index_entry_count > MAX_RESOURCE_COUNT:
            raise ValueError(
                "index_entry_count exceeds supported bound"
            )

        if measurement.storage_capacity_bytes > MAX_STORAGE_BYTES:
            raise ValueError(
                "storage_capacity_bytes exceeds supported bound"
            )

        if measurement.storage_used_bytes > MAX_STORAGE_BYTES:
            raise ValueError(
                "storage_used_bytes exceeds supported bound"
            )

        if measurement.shard_count > MAX_SHARD_COUNT:
            raise ValueError(
                "shard_count exceeds supported bound"
            )

        if measurement.replica_count > MAX_REPLICA_COUNT:
            raise ValueError(
                "replica_count exceeds supported bound"
            )

        if (
            measurement.storage_used_bytes
            > measurement.storage_capacity_bytes
        ):
            raise ValueError(
                "storage_used_bytes cannot exceed storage_capacity_bytes"
            )

        numeric_fields = (
            (
                "document_write_throughput_per_second",
                measurement.document_write_throughput_per_second,
            ),
            (
                "index_write_throughput_per_second",
                measurement.index_write_throughput_per_second,
            ),
            (
                "read_throughput_per_second",
                measurement.read_throughput_per_second,
            ),
            ("durability_rate", measurement.durability_rate),
            ("replication_rate", measurement.replication_rate),
            ("consistency_rate", measurement.consistency_rate),
            ("shard_stability_rate", measurement.shard_stability_rate),
            ("recovery_rate", measurement.recovery_rate),
            ("idempotency_rate", measurement.idempotency_rate),
            ("provenance_rate", measurement.provenance_rate),
            ("observability_rate", measurement.observability_rate),
            ("write_error_rate", measurement.write_error_rate),
            ("read_error_rate", measurement.read_error_rate),
            ("data_loss_rate", measurement.data_loss_rate),
            ("corruption_rate", measurement.corruption_rate),
            ("duplicate_rate", measurement.duplicate_rate),
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

        non_negative = (
            (
                "document_write_throughput_per_second",
                measurement.document_write_throughput_per_second,
            ),
            (
                "index_write_throughput_per_second",
                measurement.index_write_throughput_per_second,
            ),
            (
                "read_throughput_per_second",
                measurement.read_throughput_per_second,
            ),
            (
                "measurement_window_seconds",
                measurement.measurement_window_seconds,
            ),
        )

        for name, value in non_negative:
            if value < 0:
                raise ValueError(
                    f"{name} must be non-negative"
                )

        bounded = (
            ("durability_rate", measurement.durability_rate),
            ("replication_rate", measurement.replication_rate),
            ("consistency_rate", measurement.consistency_rate),
            ("shard_stability_rate", measurement.shard_stability_rate),
            ("recovery_rate", measurement.recovery_rate),
            ("idempotency_rate", measurement.idempotency_rate),
            ("provenance_rate", measurement.provenance_rate),
            ("observability_rate", measurement.observability_rate),
            ("write_error_rate", measurement.write_error_rate),
            ("read_error_rate", measurement.read_error_rate),
            ("data_loss_rate", measurement.data_loss_rate),
            ("corruption_rate", measurement.corruption_rate),
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
        proof_input: StorageProofInput,
    ) -> StorageProofInput:
        ordered = tuple(
            sorted(
                proof_input.measurements,
                key=lambda item: item.dimension.value,
            )
        )

        return StorageProofInput(
            identity=proof_input.identity,
            lineage=proof_input.lineage,
            measurements=ordered,

            target_resource_count=proof_input.target_resource_count,
            target_storage_capacity_bytes=(
                proof_input.target_storage_capacity_bytes
            ),

            public_web_scope=proof_input.public_web_scope,
            partial_evidence_allowed=(
                proof_input.partial_evidence_allowed
            ),

            metadata=dict(proof_input.metadata),
        )

    # ------------------------------------------------------------------------
    # Dimension evaluation
    # ------------------------------------------------------------------------

    def _evaluate_dimension(
        self,
        measurement: IndexStorageStressMeasurement,
    ) -> StorageDimensionResult:
        findings: list[StorageFinding] = []

        self._minimum(
            findings,
            measurement,
            "resource_count",
            float(measurement.resource_count),
            float(self.policy.minimum_resource_count),
            StorageProofReason.INSUFFICIENT_RESOURCE_VOLUME,
        )

        self._minimum(
            findings,
            measurement,
            "storage_capacity_bytes",
            float(measurement.storage_capacity_bytes),
            float(self.policy.minimum_storage_capacity_bytes),
            StorageProofReason.INSUFFICIENT_STORAGE_CAPACITY,
        )

        self._minimum(
            findings,
            measurement,
            "document_write_throughput_per_second",
            measurement.document_write_throughput_per_second,
            self.policy.minimum_document_write_throughput,
            StorageProofReason.DOCUMENT_WRITE_THROUGHPUT_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "index_write_throughput_per_second",
            measurement.index_write_throughput_per_second,
            self.policy.minimum_index_write_throughput,
            StorageProofReason.INDEX_WRITE_THROUGHPUT_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "read_throughput_per_second",
            measurement.read_throughput_per_second,
            self.policy.minimum_read_throughput,
            StorageProofReason.READ_THROUGHPUT_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "replication_rate",
            measurement.replication_rate,
            self.policy.minimum_replication_rate,
            StorageProofReason.REPLICATION_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "durability_rate",
            measurement.durability_rate,
            self.policy.minimum_durability_rate,
            StorageProofReason.DURABILITY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "consistency_rate",
            measurement.consistency_rate,
            self.policy.minimum_consistency_rate,
            StorageProofReason.CONSISTENCY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "shard_stability_rate",
            measurement.shard_stability_rate,
            self.policy.minimum_shard_stability_rate,
            StorageProofReason.SHARD_STABILITY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "recovery_rate",
            measurement.recovery_rate,
            self.policy.minimum_recovery_rate,
            StorageProofReason.RECOVERY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "idempotency_rate",
            measurement.idempotency_rate,
            self.policy.minimum_idempotency_rate,
            StorageProofReason.IDEMPOTENCY_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "provenance_rate",
            measurement.provenance_rate,
            self.policy.minimum_provenance_rate,
            StorageProofReason.PROVENANCE_TOO_LOW,
        )

        self._minimum(
            findings,
            measurement,
            "observability_rate",
            measurement.observability_rate,
            self.policy.minimum_observability_rate,
            StorageProofReason.OBSERVABILITY_TOO_LOW,
        )

        self._maximum(
            findings,
            measurement,
            "write_error_rate",
            measurement.write_error_rate,
            self.policy.maximum_write_error_rate,
            StorageProofReason.WRITE_ERROR_RATE_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "read_error_rate",
            measurement.read_error_rate,
            self.policy.maximum_read_error_rate,
            StorageProofReason.READ_ERROR_RATE_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "data_loss_rate",
            measurement.data_loss_rate,
            self.policy.maximum_data_loss_rate,
            StorageProofReason.DATA_LOSS_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "corruption_rate",
            measurement.corruption_rate,
            self.policy.maximum_corruption_rate,
            StorageProofReason.CORRUPTION_TOO_HIGH,
        )

        self._maximum(
            findings,
            measurement,
            "duplicate_rate",
            measurement.duplicate_rate,
            self.policy.maximum_duplicate_rate,
            StorageProofReason.DUPLICATE_RATE_TOO_HIGH,
        )

        if (
            self.policy.require_capacity_pressure_stability
            and not measurement.capacity_pressure_stable
        ):
            findings.append(
                StorageFinding(
                    dimension=measurement.dimension,
                    reason=StorageProofReason.CAPACITY_PRESSURE_FAILURE,
                    message=(
                        "Capacity-pressure stability was not demonstrated."
                    ),
                    evidence_strength=measurement.evidence_strength,
                )
            )

        if (
            measurement.evidence_strength
            == EvidenceStrength.UNKNOWN
        ):
            findings.append(
                StorageFinding(
                    dimension=measurement.dimension,
                    reason=StorageProofReason.MISSING_EVIDENCE,
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
                StorageFinding(
                    dimension=measurement.dimension,
                    reason=StorageProofReason.PARTIAL_EVIDENCE,
                    message="Partial evidence is not allowed.",
                    evidence_strength=measurement.evidence_strength,
                )
            )

        return StorageDimensionResult(
            dimension=measurement.dimension,
            passed=len(findings) == 0,

            resource_count=measurement.resource_count,
            document_count=measurement.document_count,
            index_entry_count=measurement.index_entry_count,

            storage_capacity_bytes=(
                measurement.storage_capacity_bytes
            ),
            storage_used_bytes=measurement.storage_used_bytes,

            shard_count=measurement.shard_count,
            replica_count=measurement.replica_count,

            document_write_throughput_per_second=(
                measurement.document_write_throughput_per_second
            ),
            index_write_throughput_per_second=(
                measurement.index_write_throughput_per_second
            ),
            read_throughput_per_second=(
                measurement.read_throughput_per_second
            ),

            durability_rate=measurement.durability_rate,
            replication_rate=measurement.replication_rate,
            consistency_rate=measurement.consistency_rate,
            shard_stability_rate=measurement.shard_stability_rate,
            recovery_rate=measurement.recovery_rate,
            idempotency_rate=measurement.idempotency_rate,
            provenance_rate=measurement.provenance_rate,
            observability_rate=measurement.observability_rate,

            write_error_rate=measurement.write_error_rate,
            read_error_rate=measurement.read_error_rate,
            data_loss_rate=measurement.data_loss_rate,
            corruption_rate=measurement.corruption_rate,
            duplicate_rate=measurement.duplicate_rate,

            capacity_pressure_stable=(
                measurement.capacity_pressure_stable
            ),

            findings=tuple(findings),
        )

    def _minimum(
        self,
        findings: list[StorageFinding],
        measurement: IndexStorageStressMeasurement,
        name: str,
        measured: float,
        required: float,
        reason: StorageProofReason,
    ) -> None:
        if measured < required:
            findings.append(
                StorageFinding(
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
        findings: list[StorageFinding],
        measurement: IndexStorageStressMeasurement,
        name: str,
        measured: float,
        maximum: float,
        reason: StorageProofReason,
    ) -> None:
        if measured > maximum:
            findings.append(
                StorageFinding(
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
        proof_input: StorageProofInput,
        dimension_results: Sequence[StorageDimensionResult],
    ) -> GlobalStorageResult:
        findings: list[StorageFinding] = []

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
                    StorageFinding(
                        dimension=dimension,
                        reason=StorageProofReason.MISSING_EVIDENCE,
                        message=(
                            f"Required dimension '{dimension.value}' "
                            "has no measurement."
                        ),
                    )
                )

        if not dimension_results:
            findings.append(
                StorageFinding(
                    dimension=None,
                    reason=StorageProofReason.MISSING_EVIDENCE,
                    message="No storage measurements were supplied.",
                )
            )

            return GlobalStorageResult(
                passed=False,

                dimensions_evaluated=0,
                expected_dimensions=len(self.REQUIRED_DIMENSIONS),

                aggregate_document_write_throughput=0.0,
                aggregate_index_write_throughput=0.0,
                aggregate_read_throughput=0.0,

                aggregate_durability=0.0,
                aggregate_replication=0.0,
                aggregate_consistency=0.0,
                aggregate_shard_stability=0.0,
                aggregate_recovery=0.0,
                aggregate_idempotency=0.0,
                aggregate_provenance=0.0,
                aggregate_observability=0.0,

                aggregate_write_error_rate=1.0,
                aggregate_read_error_rate=1.0,
                aggregate_data_loss_rate=1.0,
                aggregate_corruption_rate=1.0,
                aggregate_duplicate_rate=1.0,

                findings=tuple(findings),
            )

        return GlobalStorageResult(
            passed=len(findings) == 0,

            dimensions_evaluated=len(dimension_results),
            expected_dimensions=len(self.REQUIRED_DIMENSIONS),

            aggregate_document_write_throughput=self._mean(
                result.document_write_throughput_per_second
                for result in dimension_results
            ),
            aggregate_index_write_throughput=self._mean(
                result.index_write_throughput_per_second
                for result in dimension_results
            ),
            aggregate_read_throughput=self._mean(
                result.read_throughput_per_second
                for result in dimension_results
            ),

            aggregate_durability=self._mean(
                result.durability_rate
                for result in dimension_results
            ),
            aggregate_replication=self._mean(
                result.replication_rate
                for result in dimension_results
            ),
            aggregate_consistency=self._mean(
                result.consistency_rate
                for result in dimension_results
            ),
            aggregate_shard_stability=self._mean(
                result.shard_stability_rate
                for result in dimension_results
            ),
            aggregate_recovery=self._mean(
                result.recovery_rate
                for result in dimension_results
            ),
            aggregate_idempotency=self._mean(
                result.idempotency_rate
                for result in dimension_results
            ),
            aggregate_provenance=self._mean(
                result.provenance_rate
                for result in dimension_results
            ),
            aggregate_observability=self._mean(
                result.observability_rate
                for result in dimension_results
            ),

            aggregate_write_error_rate=self._mean(
                result.write_error_rate
                for result in dimension_results
            ),
            aggregate_read_error_rate=self._mean(
                result.read_error_rate
                for result in dimension_results
            ),
            aggregate_data_loss_rate=self._mean(
                result.data_loss_rate
                for result in dimension_results
            ),
            aggregate_corruption_rate=self._mean(
                result.corruption_rate
                for result in dimension_results
            ),
            aggregate_duplicate_rate=self._mean(
                result.duplicate_rate
                for result in dimension_results
            ),

            findings=tuple(findings),
        )

    # ------------------------------------------------------------------------
    # Findings and decision
    # ------------------------------------------------------------------------

    def _collect_findings(
        self,
        proof_input: StorageProofInput,
        dimension_results: Sequence[StorageDimensionResult],
        global_result: GlobalStorageResult,
    ) -> list[StorageFinding]:
        findings: list[StorageFinding] = []

        for result in dimension_results:
            findings.extend(result.findings)

        findings.extend(global_result.findings)

        if (
            self.policy.require_public_web_scope
            and not proof_input.public_web_scope
        ):
            findings.append(
                StorageFinding(
                    dimension=None,
                    reason=StorageProofReason.INVALID_MEASUREMENT,
                    message=(
                        "Evidence is not explicitly scoped to "
                        "the public Web."
                    ),
                )
            )

        return findings

    def _make_decision(
        self,
        proof_input: StorageProofInput,
        dimension_results: Sequence[StorageDimensionResult],
        global_result: GlobalStorageResult,
        findings: Sequence[StorageFinding],
    ) -> StorageProofDecision:
        if not dimension_results:
            return StorageProofDecision.DEFER

        partial = any(
            measurement.evidence_strength
            in (
                EvidenceStrength.PARTIAL,
                EvidenceStrength.SIMULATED,
            )
            for measurement in proof_input.measurements
        )

        if partial and not self.policy.allow_partial_evidence:
            return StorageProofDecision.DEFER

        if findings:
            return StorageProofDecision.FAIL

        if not global_result.passed:
            return StorageProofDecision.FAIL

        if self.policy.require_all_dimensions:
            present = {
                result.dimension
                for result in dimension_results
            }

            if not set(self.REQUIRED_DIMENSIONS).issubset(present):
                return StorageProofDecision.DEFER

        return StorageProofDecision.PASS

    @staticmethod
    def _state_from_decision(
        decision: StorageProofDecision,
    ) -> StorageProofState:
        if decision == StorageProofDecision.PASS:
            return StorageProofState.PASSED

        if decision == StorageProofDecision.FAIL:
            return StorageProofState.FAILED

        return StorageProofState.DEFERRED

    # ------------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------------

    def _overall_score(
        self,
        dimension_results: Sequence[StorageDimensionResult],
        global_result: GlobalStorageResult,
        findings: Sequence[StorageFinding],
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
                global_result.aggregate_durability,
                global_result.aggregate_replication,
                global_result.aggregate_consistency,
                global_result.aggregate_shard_stability,
                global_result.aggregate_recovery,
                global_result.aggregate_idempotency,
                global_result.aggregate_provenance,
                global_result.aggregate_observability,
                max(
                    0.0,
                    1.0 - global_result.aggregate_write_error_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_read_error_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_data_loss_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_corruption_rate,
                ),
                max(
                    0.0,
                    1.0 - global_result.aggregate_duplicate_rate,
                ),
            )
        )

        score = (base * 0.75) + (global_quality * 0.25)

        if findings:
            score *= max(
                0.0,
                1.0 - min(0.5, len(findings) / 100.0),
            )

        return max(
            0.0,
            min(1.0, score),
        )

    def _dimension_score(
        self,
        result: StorageDimensionResult,
    ) -> float:
        values = (
            result.durability_rate,
            result.replication_rate,
            result.consistency_rate,
            result.shard_stability_rate,
            result.recovery_rate,
            result.idempotency_rate,
            result.provenance_rate,
            result.observability_rate,
            max(
                0.0,
                1.0 - result.write_error_rate,
            ),
            max(
                0.0,
                1.0 - result.read_error_rate,
            ),
            max(
                0.0,
                1.0 - result.data_loss_rate,
            ),
            max(
                0.0,
                1.0 - result.corruption_rate,
            ),
            max(
                0.0,
                1.0 - result.duplicate_rate,
            ),
            1.0 if result.capacity_pressure_stable else 0.0,
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
    # Evaluated measurements
    # ------------------------------------------------------------------------

    @staticmethod
    def _evaluated_resource_count(
        results: Sequence[StorageDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.resource_count
            for result in results
        )

    @staticmethod
    def _evaluated_document_count(
        results: Sequence[StorageDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.document_count
            for result in results
        )

    @staticmethod
    def _evaluated_index_entry_count(
        results: Sequence[StorageDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.index_entry_count
            for result in results
        )

    @staticmethod
    def _evaluated_storage_capacity(
        results: Sequence[StorageDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.storage_capacity_bytes
            for result in results
        )

    @staticmethod
    def _evaluated_storage_used(
        results: Sequence[StorageDimensionResult],
    ) -> int:
        if not results:
            return 0

        return min(
            result.storage_used_bytes
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
        checkpoints: list[StorageProofCheckpoint],
        proof_input: StorageProofInput,
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

        checkpoint = StorageProofCheckpoint(
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
        events: list[StorageProofEvent],
        proof_input: StorageProofInput,
        event_type: EventType,
        dimension: Optional[StorageTestDimension] = None,
    ) -> None:
        event_id = self._stable_id(
            "event",
            proof_input.identity.evaluation_id,
            event_type.value,
            dimension.value if dimension else "",
        )

        event = StorageProofEvent(
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
                        if dimension is not None
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
        normalized = MassiveIndexStorageStressProof._canonicalize(
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
                key: MassiveIndexStorageStressProof._canonicalize(
                    getattr(value, key)
                )
                for key in value.__dataclass_fields__
            }

        if isinstance(value, Mapping):
            return {
                str(key): MassiveIndexStorageStressProof._canonicalize(
                    item
                )
                for key, item in sorted(
                    value.items(),
                    key=lambda pair: str(pair[0]),
                )
            }

        if isinstance(value, (list, tuple)):
            return [
                MassiveIndexStorageStressProof._canonicalize(
                    item
                )
                for item in value
            ]

        if isinstance(value, (set, frozenset)):
            items = [
                MassiveIndexStorageStressProof._canonicalize(
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

GlobalIndexStorageStressProof = MassiveIndexStorageStressProof

Phase15_3MassiveIndexStorageStressProof = (
    MassiveIndexStorageStressProof
)

IndexStorageScaleProof = MassiveIndexStorageStressProof

MassiveStorageStressProof = MassiveIndexStorageStressProof


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

    "StorageProofState",
    "StorageTestDimension",
    "EvidenceStrength",
    "EvidenceKind",
    "ScaleBand",
    "StorageProofDecision",
    "StorageProofReason",
    "CheckpointType",
    "EventType",

    "StorageProofIdentity",
    "StorageProofLineage",
    "IndexStorageStressMeasurement",
    "StorageProofInput",
    "StorageProofPolicy",
    "StorageFinding",
    "StorageDimensionResult",
    "GlobalStorageResult",
    "StorageProofCheckpoint",
    "StorageProofEvent",
    "StorageProofResult",

    "StorageProofBackend",
    "InMemoryStorageProofBackend",

    "MassiveIndexStorageStressProof",
    "GlobalIndexStorageStressProof",
    "Phase15_3MassiveIndexStorageStressProof",
    "IndexStorageScaleProof",
    "MassiveStorageStressProof",
]
