"""
OUR SEARCH
Phase 13.8 — Global Recrawl Coordination / Failure Recovery

Purpose
-------
Coordinate recrawl scheduling across the global freshness queue and provide
durable failure-recovery semantics for enormous distributed Web crawling.

This stage consumes recrawl queue entries produced by earlier freshness
stages and coordinates their global execution state across partitions,
regions, hosts, domains, and queue shards.

This module is an architecture and coordination layer.

It does NOT:
- perform HTTP requests
- fetch Web resources
- parse Web pages
- execute crawling
- assign individual crawler workers
- mutate the search index
- perform final ranking
- classify spam
- replace the crawler transport layer
- depend on Google Search, Google's index, Google's crawler, or Google's
  infrastructure

Scale target
------------
Designed directly for billions to trillions of publicly accessible Web
resources.

The architecture avoids imposing a fixed global ceiling on:
- resources
- URLs
- documents
- hosts
- domains
- partitions
- regions
- queue shards
- workers
- recrawl operations
- failures
- retries
- recovery operations

Per-request and per-batch safety limits exist only to bound individual
processing units.

Architecture boundary
---------------------
13.1  Crawl-Freshness Prioritization
13.2  Change Detection / Content Change Signals
13.3  Recrawl Scheduling
13.4  Adaptive Recrawl Frequency
13.5  URL/Document Freshness Queues
13.6  Distributed Recrawl Orchestration
13.7  Freshness-Aware Crawl Resource Allocation
13.8  Global Recrawl Coordination / Failure Recovery
13.9  Final Freshness + Recrawling Architecture
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import math
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)


# ============================================================================
# ARCHITECTURE METADATA
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "global-recrawl-coordination-failure-recovery.v1"
PHASE = "13.8"
PREVIOUS_STAGE = "13.7"
NEXT_STAGE = "13.9"


# ============================================================================
# ENUMERATIONS
# ============================================================================


class GlobalRecrawlCoordinationState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    PARTITION_COORDINATION = "partition_coordination"
    FAILURE_ANALYSIS = "failure_analysis"
    RECOVERY_PLANNING = "recovery_planning"
    RETRY_PLANNING = "retry_planning"
    REBALANCING = "rebalancing"
    COORDINATING = "coordinating"
    CHECKPOINTING = "checkpointing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class RecrawlOperationState(str, Enum):
    PENDING = "pending"
    READY = "ready"
    DISPATCHABLE = "dispatchable"
    IN_PROGRESS = "in_progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRY_WAIT = "retry_wait"
    RECOVERY_PENDING = "recovery_pending"
    RECOVERED = "recovered"
    DEFERRED = "deferred"
    CANCELLED = "cancelled"


class RecrawlFailureClass(str, Enum):
    NONE = "none"
    TRANSIENT = "transient"
    NETWORK = "network"
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    HOST_UNAVAILABLE = "host_unavailable"
    PARTITION_UNAVAILABLE = "partition_unavailable"
    REGION_UNAVAILABLE = "region_unavailable"
    QUEUE_UNAVAILABLE = "queue_unavailable"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    COORDINATION_FAILURE = "coordination_failure"
    CHECKPOINT_FAILURE = "checkpoint_failure"
    PERSISTENCE_FAILURE = "persistence_failure"
    UNKNOWN = "unknown"


class RecrawlFailureSeverity(str, Enum):
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"


class RecoveryDecision(str, Enum):
    NO_RECOVERY_REQUIRED = "no_recovery_required"
    RETRY = "retry"
    REQUEUE = "requeue"
    DEFER = "defer"
    RELOCATE = "relocate"
    REBUILD_COORDINATION = "rebuild_coordination"
    RESTORE_FROM_CHECKPOINT = "restore_from_checkpoint"
    QUARANTINE = "quarantine"
    CANCEL = "cancel"


class RetryPolicy(str, Enum):
    NONE = "none"
    IMMEDIATE = "immediate"
    FIXED_BACKOFF = "fixed_backoff"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    EXPONENTIAL_JITTER = "exponential_jitter"


class CoordinationDecision(str, Enum):
    ACCEPT = "accept"
    UPDATE = "update"
    COORDINATE = "coordinate"
    REBALANCE = "rebalance"
    RECOVER = "recover"
    RETRY = "retry"
    DEFER = "defer"
    REJECT = "reject"


class CoordinationScope(str, Enum):
    RESOURCE = "resource"
    HOST = "host"
    DOMAIN = "domain"
    PARTITION = "partition"
    SHARD = "shard"
    REGION = "region"
    GLOBAL = "global"


class CoordinationEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    INPUT_NORMALIZED = "input_normalized"
    PARTITION_COORDINATION_STARTED = "partition_coordination_started"
    FAILURE_ANALYSIS_STARTED = "failure_analysis_started"
    FAILURE_CLASSIFIED = "failure_classified"
    RECOVERY_PLANNING_STARTED = "recovery_planning_started"
    RETRY_PLAN_CREATED = "retry_plan_created"
    RECOVERY_PLAN_CREATED = "recovery_plan_created"
    REBALANCING_STARTED = "rebalancing_started"
    REBALANCING_COMPLETED = "rebalancing_completed"
    COORDINATION_UPDATED = "coordination_updated"
    CHECKPOINT_CREATED = "checkpoint_created"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    COORDINATION_COMPLETED = "coordination_completed"
    COORDINATION_DEFERRED = "coordination_deferred"
    COORDINATION_REJECTED = "coordination_rejected"
    COORDINATION_FAILED = "coordination_failed"


class CoordinationCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    PARTITIONS_COORDINATED = "partitions_coordinated"
    FAILURES_ANALYZED = "failures_analyzed"
    RECOVERY_PLANNED = "recovery_planned"
    RETRIES_PLANNED = "retries_planned"
    REBALANCING_COMPLETED = "rebalancing_completed"
    COORDINATION_PERSISTED = "coordination_persisted"
    COMPLETED = "completed"


# ============================================================================
# DATACLASSES — IDENTITY / LINEAGE
# ============================================================================


@dataclass
class GlobalRecrawlCoordinationIdentity:
    resource_id: str
    operation_id: str = ""
    coordination_id: str = ""
    coordination_version: str = ARCHITECTURE_VERSION
    partition_id: str = ""
    shard_id: str = ""
    host_id: str = ""
    domain_id: str = ""
    region_id: str = ""

    def key(self) -> str:
        return ":".join(
            [
                self.resource_id,
                self.operation_id,
                self.coordination_version,
            ]
        )


@dataclass
class GlobalRecrawlCoordinationLineage:
    resource_id: str

    previous_stage: str = PREVIOUS_STAGE
    current_stage: str = PHASE

    source_queue_entry_id: str = ""
    source_schedule_id: str = ""
    source_frequency_version: str = ""

    source_signal_version: str = ""
    source_history_version: str = ""

    parent_coordination_ids: List[str] = field(default_factory=list)
    parent_operation_ids: List[str] = field(default_factory=list)

    lineage_metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# INPUT MODELS
# ============================================================================


@dataclass
class RecrawlOperationInput:
    resource_id: str

    operation_id: str = ""

    resource_type: str = "url"

    queue_entry_id: str = ""
    schedule_id: str = ""
    frequency_version: str = ""

    partition_id: str = ""
    shard_id: str = ""
    host_id: str = ""
    domain_id: str = ""
    region_id: str = ""

    state: RecrawlOperationState = RecrawlOperationState.PENDING

    priority_score: float = 0.0

    scheduled_at: Optional[str] = None
    deadline_at: Optional[str] = None

    attempt_count: int = 0
    previous_failure_count: int = 0

    last_failure_class: RecrawlFailureClass = RecrawlFailureClass.NONE
    last_failure_severity: RecrawlFailureSeverity = RecrawlFailureSeverity.NONE

    last_failure_timestamp: Optional[str] = None

    freshness_score: float = 0.0
    urgency_score: float = 0.0
    change_rate: float = 0.0
    volatility: float = 0.0

    confidence: float = 1.0
    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecrawlFailureRecord:
    resource_id: str
    operation_id: str = ""

    failure_id: str = ""

    failure_class: RecrawlFailureClass = RecrawlFailureClass.UNKNOWN
    severity: RecrawlFailureSeverity = RecrawlFailureSeverity.MODERATE

    occurred_at: Optional[str] = None

    attempt_number: int = 0

    retryable: bool = True

    partition_id: str = ""
    shard_id: str = ""
    host_id: str = ""
    domain_id: str = ""
    region_id: str = ""

    error_code: str = ""
    error_message: str = ""

    transient: bool = True

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PartitionHealth:
    partition_id: str

    region_id: str = ""

    available: bool = True

    health_score: float = 1.0

    queue_health: float = 1.0
    persistence_health: float = 1.0
    coordination_health: float = 1.0

    active_operations: int = 0
    failed_operations: int = 0
    recovery_operations: int = 0

    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RegionHealth:
    region_id: str

    available: bool = True

    health_score: float = 1.0

    partition_health: float = 1.0
    queue_health: float = 1.0
    persistence_health: float = 1.0
    coordination_health: float = 1.0

    active_operations: int = 0
    failed_operations: int = 0

    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CoordinationInput:
    operations: List[RecrawlOperationInput] = field(default_factory=list)

    failures: List[RecrawlFailureRecord] = field(default_factory=list)

    partition_health: List[PartitionHealth] = field(default_factory=list)

    region_health: List[RegionHealth] = field(default_factory=list)

    coordination_scope: CoordinationScope = CoordinationScope.GLOBAL

    confidence: float = 1.0
    partial: bool = False

    source_version: str = ""

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# POLICY
# ============================================================================


@dataclass
class GlobalRecrawlCoordinationPolicy:
    max_operations_per_batch: int = 100000
    max_failures_per_batch: int = 100000

    minimum_confidence: float = 0.10

    allow_partial: bool = True

    deterministic: bool = True

    checkpoint_enabled: bool = True

    max_retry_attempts: int = 8

    retry_base_delay_seconds: float = 30.0

    retry_max_delay_seconds: float = 86400.0

    retry_jitter_fraction: float = 0.10

    transient_failure_weight: float = 1.00
    network_failure_weight: float = 0.90
    timeout_failure_weight: float = 1.00
    rate_limit_failure_weight: float = 1.20
    host_failure_weight: float = 1.25
    partition_failure_weight: float = 1.50
    region_failure_weight: float = 1.75
    queue_failure_weight: float = 1.50
    resource_exhaustion_weight: float = 1.25
    coordination_failure_weight: float = 1.50
    persistence_failure_weight: float = 1.75

    critical_failure_threshold: float = 0.90
    high_failure_threshold: float = 0.70
    moderate_failure_threshold: float = 0.40

    minimum_partition_health: float = 0.40
    minimum_region_health: float = 0.40

    recovery_priority_boost: float = 0.20
    missed_deadline_boost: float = 0.20

    relocation_health_threshold: float = 0.35

    partial_result_penalty: float = 0.05

    checkpoint_retention_hint: int = 100

    max_recovery_operations_per_resource: int = 16


# ============================================================================
# COORDINATION RESULT MODELS
# ============================================================================


@dataclass
class RecoveryPlan:
    resource_id: str

    operation_id: str = ""

    decision: RecoveryDecision = RecoveryDecision.NO_RECOVERY_REQUIRED

    failure_class: RecrawlFailureClass = RecrawlFailureClass.NONE

    retry_policy: RetryPolicy = RetryPolicy.NONE

    retry_count: int = 0

    next_retry_at: Optional[str] = None

    recovery_priority: float = 0.0

    target_partition_id: str = ""
    target_region_id: str = ""

    relocate: bool = False

    restore_checkpoint: bool = False

    quarantine: bool = False

    reasons: List[str] = field(default_factory=list)

    confidence: float = 1.0

    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CoordinatedOperation:
    resource_id: str

    operation_id: str = ""

    coordination_id: str = ""

    state: RecrawlOperationState = RecrawlOperationState.READY

    decision: CoordinationDecision = CoordinationDecision.ACCEPT

    coordination_scope: CoordinationScope = CoordinationScope.GLOBAL

    priority_score: float = 0.0

    recovery_priority: float = 0.0

    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""

    target_partition_id: str = ""
    target_region_id: str = ""

    retry_count: int = 0

    next_retry_at: Optional[str] = None

    recovery_plan: Optional[RecoveryPlan] = None

    failure_class: RecrawlFailureClass = RecrawlFailureClass.NONE

    reasons: List[str] = field(default_factory=list)

    confidence: float = 1.0

    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GlobalRecrawlCoordinationResult:
    coordination_id: str

    state: GlobalRecrawlCoordinationState

    decision: CoordinationDecision

    operations: List[CoordinatedOperation] = field(default_factory=list)

    recovery_plans: List[RecoveryPlan] = field(default_factory=list)

    accepted_count: int = 0
    retry_count: int = 0
    recovery_count: int = 0
    rebalanced_count: int = 0
    deferred_count: int = 0
    rejected_count: int = 0

    partial: bool = False

    confidence: float = 1.0

    created_at: str = ""
    completed_at: Optional[str] = None

    lineage: Optional[GlobalRecrawlCoordinationLineage] = None

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# CHECKPOINT / EVENT MODELS
# ============================================================================


@dataclass
class GlobalRecrawlCoordinationCheckpoint:
    checkpoint_id: str

    coordination_id: str

    checkpoint_type: CoordinationCheckpointType

    created_at: str

    state: GlobalRecrawlCoordinationState

    operation_count: int = 0

    failure_count: int = 0

    recovery_count: int = 0

    partition_count: int = 0

    region_count: int = 0

    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GlobalRecrawlCoordinationEvent:
    event_id: str

    coordination_id: str

    event_type: CoordinationEventType

    created_at: str

    resource_id: str = ""

    operation_id: str = ""

    partition_id: str = ""

    region_id: str = ""

    failure_class: RecrawlFailureClass = RecrawlFailureClass.NONE

    recovery_decision: RecoveryDecision = (
        RecoveryDecision.NO_RECOVERY_REQUIRED
    )

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND
# ============================================================================


class GlobalRecrawlCoordinationBackend(Protocol):
    def persist_event(
        self,
        event: GlobalRecrawlCoordinationEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: GlobalRecrawlCoordinationCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: GlobalRecrawlCoordinationResult,
    ) -> None:
        ...

    def get_result(
        self,
        coordination_id: str,
    ) -> Optional[GlobalRecrawlCoordinationResult]:
        ...

    def persist_operation(
        self,
        operation: CoordinatedOperation,
    ) -> None:
        ...

    def get_operation(
        self,
        operation_id: str,
    ) -> Optional[CoordinatedOperation]:
        ...


class InMemoryGlobalRecrawlCoordinationMetadata:
    """
    Reference metadata backend.

    This backend intentionally stores data in memory only.

    Production deployment should replace this backend with a durable,
    distributed metadata and coordination implementation.
    """

    def __init__(self) -> None:
        self._events: List[GlobalRecrawlCoordinationEvent] = []
        self._checkpoints: List[GlobalRecrawlCoordinationCheckpoint] = []
        self._results: Dict[str, GlobalRecrawlCoordinationResult] = {}
        self._operations: Dict[str, CoordinatedOperation] = {}

    def persist_event(
        self,
        event: GlobalRecrawlCoordinationEvent,
    ) -> None:
        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: GlobalRecrawlCoordinationCheckpoint,
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_result(
        self,
        result: GlobalRecrawlCoordinationResult,
    ) -> None:
        self._results[result.coordination_id] = result

    def get_result(
        self,
        coordination_id: str,
    ) -> Optional[GlobalRecrawlCoordinationResult]:
        return self._results.get(coordination_id)

    def persist_operation(
        self,
        operation: CoordinatedOperation,
    ) -> None:
        key = operation.operation_id or operation.resource_id
        self._operations[key] = operation

    def get_operation(
        self,
        operation_id: str,
    ) -> Optional[CoordinatedOperation]:
        return self._operations.get(operation_id)

    def events(self) -> List[GlobalRecrawlCoordinationEvent]:
        return list(self._events)

    def checkpoints(
        self,
    ) -> List[GlobalRecrawlCoordinationCheckpoint]:
        return list(self._checkpoints)

    def results(
        self,
    ) -> List[GlobalRecrawlCoordinationResult]:
        return list(self._results.values())

    def operations(
        self,
    ) -> List[CoordinatedOperation]:
        return list(self._operations.values())


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class GlobalRecrawlCoordinationFailureRecoveryArchitecture:
    """
    Phase 13.8 global recrawl coordination and failure recovery architecture.

    Responsibilities
    ----------------
    - coordinate recrawl operations globally
    - analyze distributed failures
    - classify retryable failures
    - generate deterministic retry schedules
    - create recovery plans
    - detect unhealthy partitions and regions
    - support operation relocation
    - support checkpoint restoration
    - preserve coordination lineage
    - persist restartable coordination state
    - support partial results
    - support deterministic distributed processing

    Explicit non-responsibilities
    ------------------------------
    - HTTP fetching
    - crawler transport
    - parser execution
    - crawler worker assignment
    - final crawl execution
    - index mutation
    - ranking
    - spam classification
    """

    def __init__(
        self,
        backend: Optional[GlobalRecrawlCoordinationBackend] = None,
        policy: Optional[GlobalRecrawlCoordinationPolicy] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryGlobalRecrawlCoordinationMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else GlobalRecrawlCoordinationPolicy()
        )

    # ------------------------------------------------------------------
    # BASIC UTILITIES
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _clamp(
        value: float,
        minimum: float = 0.0,
        maximum: float = 1.0,
    ) -> float:
        if not math.isfinite(value):
            return minimum

        return max(minimum, min(maximum, value))

    @staticmethod
    def _safe_float(
        value: Any,
        default: float = 0.0,
    ) -> float:
        try:
            number = float(value)

            if not math.isfinite(number):
                return default

            return number

        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_int(
        value: Any,
        default: int = 0,
    ) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_timestamp(
        value: Optional[str],
    ) -> Optional[datetime]:
        if not value:
            return None

        try:
            parsed = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            return parsed.astimezone(timezone.utc)

        except (TypeError, ValueError):
            return None

    @staticmethod
    def _deterministic_unit(
        *parts: str,
    ) -> float:
        material = "|".join(str(part) for part in parts)

        digest = hashlib.sha256(
            material.encode("utf-8")
        ).digest()

        integer = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return integer / float(2**64 - 1)

    def _event(
        self,
        coordination_id: str,
        event_type: CoordinationEventType,
        *,
        resource_id: str = "",
        operation_id: str = "",
        partition_id: str = "",
        region_id: str = "",
        failure_class: RecrawlFailureClass = RecrawlFailureClass.NONE,
        recovery_decision: RecoveryDecision = (
            RecoveryDecision.NO_RECOVERY_REQUIRED
        ),
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> GlobalRecrawlCoordinationEvent:

        event_id = hashlib.sha256(
            (
                f"{coordination_id}|"
                f"{event_type.value}|"
                f"{resource_id}|"
                f"{operation_id}|"
                f"{partition_id}|"
                f"{region_id}|"
                f"{self._now().isoformat()}"
            ).encode("utf-8")
        ).hexdigest()

        event = GlobalRecrawlCoordinationEvent(
            event_id=event_id,
            coordination_id=coordination_id,
            event_type=event_type,
            created_at=self._now().isoformat(),
            resource_id=resource_id,
            operation_id=operation_id,
            partition_id=partition_id,
            region_id=region_id,
            failure_class=failure_class,
            recovery_decision=recovery_decision,
            metadata=dict(metadata or {}),
        )

        self.backend.persist_event(event)

        return event

    def _checkpoint(
        self,
        coordination_id: str,
        checkpoint_type: CoordinationCheckpointType,
        state: GlobalRecrawlCoordinationState,
        *,
        operation_count: int = 0,
        failure_count: int = 0,
        recovery_count: int = 0,
        partition_count: int = 0,
        region_count: int = 0,
        partial: bool = False,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> GlobalRecrawlCoordinationCheckpoint:

        checkpoint_id = hashlib.sha256(
            (
                f"{coordination_id}|"
                f"{checkpoint_type.value}|"
                f"{self._now().isoformat()}"
            ).encode("utf-8")
        ).hexdigest()

        checkpoint = GlobalRecrawlCoordinationCheckpoint(
            checkpoint_id=checkpoint_id,
            coordination_id=coordination_id,
            checkpoint_type=checkpoint_type,
            created_at=self._now().isoformat(),
            state=state,
            operation_count=operation_count,
            failure_count=failure_count,
            recovery_count=recovery_count,
            partition_count=partition_count,
            region_count=region_count,
            partial=partial,
            metadata=dict(metadata or {}),
        )

        if self.policy.checkpoint_enabled:
            self.backend.persist_checkpoint(checkpoint)

        self._event(
            coordination_id,
            CoordinationEventType.CHECKPOINT_CREATED,
            metadata={
                "checkpoint_id": checkpoint_id,
                "checkpoint_type": checkpoint_type.value,
            },
        )

        return checkpoint

    # ------------------------------------------------------------------
    # INPUT NORMALIZATION
    # ------------------------------------------------------------------

    def _normalize_operation(
        self,
        operation: RecrawlOperationInput,
    ) -> RecrawlOperationInput:

        operation.priority_score = self._clamp(
            self._safe_float(operation.priority_score)
        )

        operation.freshness_score = self._clamp(
            self._safe_float(operation.freshness_score)
        )

        operation.urgency_score = self._clamp(
            self._safe_float(operation.urgency_score)
        )

        operation.change_rate = self._clamp(
            self._safe_float(operation.change_rate)
        )

        operation.volatility = self._clamp(
            self._safe_float(operation.volatility)
        )

        operation.confidence = self._clamp(
            self._safe_float(
                operation.confidence,
                1.0,
            )
        )

        operation.attempt_count = max(
            0,
            self._safe_int(operation.attempt_count),
        )

        operation.previous_failure_count = max(
            0,
            self._safe_int(
                operation.previous_failure_count
            ),
        )

        return operation

    def _normalize_input(
        self,
        coordination_input: CoordinationInput,
    ) -> CoordinationInput:

        normalized_operations = [
            self._normalize_operation(operation)
            for operation in coordination_input.operations[
                : self.policy.max_operations_per_batch
            ]
        ]

        failures = list(
            coordination_input.failures[
                : self.policy.max_failures_per_batch
            ]
        )

        confidence = self._clamp(
            self._safe_float(
                coordination_input.confidence,
                1.0,
            )
        )

        partial = (
            coordination_input.partial
            or len(normalized_operations)
            < len(coordination_input.operations)
            or len(failures)
            < len(coordination_input.failures)
        )

        return CoordinationInput(
            operations=normalized_operations,
            failures=failures,
            partition_health=list(
                coordination_input.partition_health
            ),
            region_health=list(
                coordination_input.region_health
            ),
            coordination_scope=coordination_input.coordination_scope,
            confidence=confidence,
            partial=partial,
            source_version=coordination_input.source_version,
            metadata=dict(coordination_input.metadata),
        )

    # ------------------------------------------------------------------
    # FAILURE ANALYSIS
    # ------------------------------------------------------------------

    def _failure_weight(
        self,
        failure_class: RecrawlFailureClass,
    ) -> float:

        weights = {
            RecrawlFailureClass.TRANSIENT:
                self.policy.transient_failure_weight,

            RecrawlFailureClass.NETWORK:
                self.policy.network_failure_weight,

            RecrawlFailureClass.TIMEOUT:
                self.policy.timeout_failure_weight,

            RecrawlFailureClass.RATE_LIMIT:
                self.policy.rate_limit_failure_weight,

            RecrawlFailureClass.HOST_UNAVAILABLE:
                self.policy.host_failure_weight,

            RecrawlFailureClass.PARTITION_UNAVAILABLE:
                self.policy.partition_failure_weight,

            RecrawlFailureClass.REGION_UNAVAILABLE:
                self.policy.region_failure_weight,

            RecrawlFailureClass.QUEUE_UNAVAILABLE:
                self.policy.queue_failure_weight,

            RecrawlFailureClass.RESOURCE_EXHAUSTION:
                self.policy.resource_exhaustion_weight,

            RecrawlFailureClass.COORDINATION_FAILURE:
                self.policy.coordination_failure_weight,

            RecrawlFailureClass.PERSISTENCE_FAILURE:
                self.policy.persistence_failure_weight,
        }

        return weights.get(
            failure_class,
            self.policy.transient_failure_weight,
        )

    def _classify_failure(
        self,
        failure: RecrawlFailureRecord,
    ) -> Tuple[
        RecrawlFailureClass,
        RecrawlFailureSeverity,
        bool,
    ]:

        failure_class = failure.failure_class

        if failure_class == RecrawlFailureClass.NONE:
            failure_class = RecrawlFailureClass.UNKNOWN

        severity = failure.severity

        if severity == RecrawlFailureSeverity.NONE:
            severity = RecrawlFailureSeverity.MODERATE

        retryable = bool(failure.retryable)

        if failure_class in {
            RecrawlFailureClass.PERSISTENCE_FAILURE,
            RecrawlFailureClass.REGION_UNAVAILABLE,
            RecrawlFailureClass.PARTITION_UNAVAILABLE,
        }:
            retryable = True

        if severity == RecrawlFailureSeverity.CRITICAL:
            retryable = True

        return failure_class, severity, retryable

    def _failure_score(
        self,
        failure: RecrawlFailureRecord,
    ) -> float:

        weight = self._failure_weight(
            failure.failure_class
        )

        severity_multiplier = {
            RecrawlFailureSeverity.NONE: 0.0,
            RecrawlFailureSeverity.LOW: 0.25,
            RecrawlFailureSeverity.MODERATE: 0.50,
            RecrawlFailureSeverity.HIGH: 0.75,
            RecrawlFailureSeverity.CRITICAL: 1.00,
        }.get(
            failure.severity,
            0.50,
        )

        return self._clamp(
            weight * severity_multiplier / 1.75
        )

    def _failures_for_operation(
        self,
        operation: RecrawlOperationInput,
        failures: Sequence[RecrawlFailureRecord],
    ) -> List[RecrawlFailureRecord]:

        matches: List[RecrawlFailureRecord] = []

        for failure in failures:
            if failure.resource_id != operation.resource_id:
                continue

            if (
                failure.operation_id
                and operation.operation_id
                and failure.operation_id
                != operation.operation_id
            ):
                continue

            matches.append(failure)

        return matches

    # ------------------------------------------------------------------
    # HEALTH ANALYSIS
    # ------------------------------------------------------------------

    def _partition_health_map(
        self,
        health: Sequence[PartitionHealth],
    ) -> Dict[str, PartitionHealth]:

        return {
            item.partition_id: item
            for item in health
            if item.partition_id
        }

    def _region_health_map(
        self,
        health: Sequence[RegionHealth],
    ) -> Dict[str, RegionHealth]:

        return {
            item.region_id: item
            for item in health
            if item.region_id
        }

    def _partition_is_healthy(
        self,
        partition_id: str,
        health_map: Mapping[str, PartitionHealth],
    ) -> bool:

        if not partition_id:
            return True

        health = health_map.get(partition_id)

        if health is None:
            return True

        return (
            health.available
            and health.health_score
            >= self.policy.minimum_partition_health
        )

    def _region_is_healthy(
        self,
        region_id: str,
        health_map: Mapping[str, RegionHealth],
    ) -> bool:

        if not region_id:
            return True

        health = health_map.get(region_id)

        if health is None:
            return True

        return (
            health.available
            and health.health_score
            >= self.policy.minimum_region_health
        )

    def _find_recovery_partition(
        self,
        operation: RecrawlOperationInput,
        partition_health: Sequence[PartitionHealth],
        region_health: Sequence[RegionHealth],
    ) -> Tuple[str, str]:

        partition_map = self._partition_health_map(
            partition_health
        )

        region_map = self._region_health_map(
            region_health
        )

        candidates: List[
            Tuple[float, str, str]
        ] = []

        for partition in partition_health:

            if not partition.available:
                continue

            if (
                partition.health_score
                < self.policy.relocation_health_threshold
            ):
                continue

            if operation.region_id:
                if partition.region_id == operation.region_id:
                    region_bonus = 0.10
                else:
                    region_bonus = 0.0
            else:
                region_bonus = 0.0

            region_ok = self._region_is_healthy(
                partition.region_id,
                region_map,
            )

            if not region_ok:
                continue

            load_penalty = self._clamp(
                partition.active_operations / 100000.0
            )

            score = (
                self._clamp(partition.health_score)
                + region_bonus
                - 0.25 * load_penalty
            )

            candidates.append(
                (
                    score,
                    partition.partition_id,
                    partition.region_id,
                )
            )

        if not candidates:
            return "", ""

        candidates.sort(
            key=lambda item: (
                -item[0],
                item[1],
                item[2],
            )
        )

        return candidates[0][1], candidates[0][2]

    # ------------------------------------------------------------------
    # RETRY TIMING
    # ------------------------------------------------------------------

    def _retry_delay(
        self,
        operation: RecrawlOperationInput,
        failure: Optional[RecrawlFailureRecord],
    ) -> float:

        attempt = max(
            0,
            operation.attempt_count,
        )

        base = self.policy.retry_base_delay_seconds

        if failure is not None:
            if (
                failure.failure_class
                == RecrawlFailureClass.RATE_LIMIT
            ):
                base *= 2.0

            elif (
                failure.failure_class
                == RecrawlFailureClass.HOST_UNAVAILABLE
            ):
                base *= 2.5

            elif (
                failure.failure_class
                == RecrawlFailureClass.REGION_UNAVAILABLE
            ):
                base *= 4.0

            elif (
                failure.failure_class
                == RecrawlFailureClass.PERSISTENCE_FAILURE
            ):
                base *= 3.0

        delay = base * (2.0 ** min(attempt, 16))

        delay = min(
            delay,
            self.policy.retry_max_delay_seconds,
        )

        if self.policy.deterministic:
            jitter = (
                self._deterministic_unit(
                    operation.resource_id,
                    operation.operation_id,
                    str(attempt),
                    ARCHITECTURE_VERSION,
                )
                * 2.0
                - 1.0
            )

            delay *= (
                1.0
                + jitter
                * self.policy.retry_jitter_fraction
            )

        return max(
            self.policy.retry_base_delay_seconds,
            min(
                delay,
                self.policy.retry_max_delay_seconds,
            ),
        )

    # ------------------------------------------------------------------
    # RECOVERY PLAN
    # ------------------------------------------------------------------

    def _build_recovery_plan(
        self,
        operation: RecrawlOperationInput,
        failures: Sequence[RecrawlFailureRecord],
        partition_health: Sequence[PartitionHealth],
        region_health: Sequence[RegionHealth],
    ) -> RecoveryPlan:

        latest_failure: Optional[
            RecrawlFailureRecord
        ] = None

        if failures:
            latest_failure = max(
                failures,
                key=lambda item: (
                    item.attempt_number,
                    item.occurred_at or "",
                ),
            )

        if latest_failure is None:
            if operation.state == RecrawlOperationState.SUCCEEDED:
                return RecoveryPlan(
                    resource_id=operation.resource_id,
                    operation_id=operation.operation_id,
                    decision=(
                        RecoveryDecision.NO_RECOVERY_REQUIRED
                    ),
                    confidence=operation.confidence,
                    partial=operation.partial,
                )

            if operation.state in {
                RecrawlOperationState.PENDING,
                RecrawlOperationState.READY,
                RecrawlOperationState.DISPATCHABLE,
            }:
                return RecoveryPlan(
                    resource_id=operation.resource_id,
                    operation_id=operation.operation_id,
                    decision=RecoveryDecision.NO_RECOVERY_REQUIRED,
                    retry_policy=RetryPolicy.NONE,
                    confidence=operation.confidence,
                    partial=operation.partial,
                )

            latest_failure = RecrawlFailureRecord(
                resource_id=operation.resource_id,
                operation_id=operation.operation_id,
                failure_class=operation.last_failure_class,
                severity=operation.last_failure_severity,
                attempt_number=operation.attempt_count,
                retryable=True,
            )

        failure_class, severity, retryable = (
            self._classify_failure(latest_failure)
        )

        attempt = max(
            operation.attempt_count,
            latest_failure.attempt_number,
        )

        reasons: List[str] = []

        failure_score = self._failure_score(
            latest_failure
        )

        recovery_priority = self._clamp(
            operation.priority_score
            + self.policy.recovery_priority_boost
            + failure_score * 0.50
        )

        if operation.deadline_at:
            deadline = self._parse_timestamp(
                operation.deadline_at
            )

            if deadline is not None and self._now() > deadline:
                recovery_priority = self._clamp(
                    recovery_priority
                    + self.policy.missed_deadline_boost
                )

                reasons.append(
                    "missed_deadline"
                )

        if failure_class == RecrawlFailureClass.TRANSIENT:
            reasons.append("transient_failure")

        elif failure_class == RecrawlFailureClass.NETWORK:
            reasons.append("network_failure")

        elif failure_class == RecrawlFailureClass.TIMEOUT:
            reasons.append("timeout_failure")

        elif failure_class == RecrawlFailureClass.RATE_LIMIT:
            reasons.append("rate_limit_failure")

        elif failure_class == RecrawlFailureClass.HOST_UNAVAILABLE:
            reasons.append("host_unavailable")

        elif failure_class == RecrawlFailureClass.PARTITION_UNAVAILABLE:
            reasons.append("partition_unavailable")

        elif failure_class == RecrawlFailureClass.REGION_UNAVAILABLE:
            reasons.append("region_unavailable")

        elif failure_class == RecrawlFailureClass.QUEUE_UNAVAILABLE:
            reasons.append("queue_unavailable")

        elif failure_class == RecrawlFailureClass.RESOURCE_EXHAUSTION:
            reasons.append("resource_exhaustion")

        elif failure_class == RecrawlFailureClass.COORDINATION_FAILURE:
            reasons.append("coordination_failure")

        elif failure_class == RecrawlFailureClass.PERSISTENCE_FAILURE:
            reasons.append("persistence_failure")

        if severity == RecrawlFailureSeverity.CRITICAL:
            reasons.append("critical_failure")

        elif severity == RecrawlFailureSeverity.HIGH:
            reasons.append("high_failure")

        target_partition = ""
        target_region = ""

        relocate = False
        restore_checkpoint = False
        quarantine = False

        decision = RecoveryDecision.NO_RECOVERY_REQUIRED
        retry_policy = RetryPolicy.NONE
        next_retry_at: Optional[str] = None

        if (
            failure_class
            in {
                RecrawlFailureClass.PARTITION_UNAVAILABLE,
                RecrawlFailureClass.REGION_UNAVAILABLE,
                RecrawlFailureClass.QUEUE_UNAVAILABLE,
            }
        ):
            (
                target_partition,
                target_region,
            ) = self._find_recovery_partition(
                operation,
                partition_health,
                region_health,
            )

            if target_partition:
                relocate = True
                decision = RecoveryDecision.RELOCATE
                reasons.append(
                    "healthy_recovery_partition_found"
                )

            else:
                decision = RecoveryDecision.DEFER
                reasons.append(
                    "no_healthy_recovery_partition"
                )

        elif failure_class in {
            RecrawlFailureClass.PERSISTENCE_FAILURE,
            RecrawlFailureClass.CHECKPOINT_FAILURE,
        }:
            restore_checkpoint = True
            decision = (
                RecoveryDecision.RESTORE_FROM_CHECKPOINT
            )
            reasons.append(
                "durable_state_recovery_required"
            )

        elif failure_class == RecrawlFailureClass.COORDINATION_FAILURE:
            restore_checkpoint = True
            decision = (
                RecoveryDecision.REBUILD_COORDINATION
            )
            reasons.append(
                "coordination_state_rebuild_required"
            )

        elif (
            failure_class == RecrawlFailureClass.RESOURCE_EXHAUSTION
        ):
            decision = RecoveryDecision.DEFER
            reasons.append(
                "resource_pressure_requires_defer"
            )

        elif retryable and attempt < self.policy.max_retry_attempts:
            retry_policy = RetryPolicy.EXPONENTIAL_JITTER

            delay = self._retry_delay(
                operation,
                latest_failure,
            )

            next_retry_at = (
                self._now()
                + timedelta(seconds=delay)
            ).isoformat()

            decision = RecoveryDecision.RETRY

            reasons.append(
                "retryable_failure"
            )

        elif attempt >= self.policy.max_retry_attempts:
            decision = RecoveryDecision.QUARANTINE
            quarantine = True

            reasons.append(
                "maximum_retry_attempts_reached"
            )

        else:
            decision = RecoveryDecision.DEFER

            reasons.append(
                "failure_not_retryable"
            )

        confidence = self._clamp(
            (
                operation.confidence
                * 0.70
                + (1.0 - failure_score) * 0.30
            )
        )

        return RecoveryPlan(
            resource_id=operation.resource_id,
            operation_id=operation.operation_id,
            decision=decision,
            failure_class=failure_class,
            retry_policy=retry_policy,
            retry_count=attempt,
            next_retry_at=next_retry_at,
            recovery_priority=recovery_priority,
            target_partition_id=target_partition,
            target_region_id=target_region,
            relocate=relocate,
            restore_checkpoint=restore_checkpoint,
            quarantine=quarantine,
            reasons=reasons,
            confidence=confidence,
            partial=operation.partial,
            metadata={
                "failure_score": failure_score,
                "failure_severity": severity.value,
                "retryable": retryable,
            },
        )

    # ------------------------------------------------------------------
    # GLOBAL OPERATION COORDINATION
    # ------------------------------------------------------------------

    def _coordinate_operation(
        self,
        coordination_id: str,
        operation: RecrawlOperationInput,
        failures: Sequence[RecrawlFailureRecord],
        partition_health: Sequence[PartitionHealth],
        region_health: Sequence[RegionHealth],
        scope: CoordinationScope,
    ) -> CoordinatedOperation:

        operation = self._normalize_operation(
            operation
        )

        recovery_plan = self._build_recovery_plan(
            operation,
            failures,
            partition_health,
            region_health,
        )

        decision = CoordinationDecision.ACCEPT
        state = RecrawlOperationState.READY

        target_partition = operation.partition_id
        target_region = operation.region_id

        reasons: List[str] = []

        if recovery_plan.decision == RecoveryDecision.RETRY:
            decision = CoordinationDecision.RETRY
            state = RecrawlOperationState.RETRY_WAIT

            reasons.extend(
                recovery_plan.reasons
            )

        elif recovery_plan.decision == RecoveryDecision.RELOCATE:
            decision = CoordinationDecision.REBALANCE
            state = RecrawlOperationState.RECOVERY_PENDING

            target_partition = (
                recovery_plan.target_partition_id
                or operation.partition_id
            )

            target_region = (
                recovery_plan.target_region_id
                or operation.region_id
            )

            reasons.extend(
                recovery_plan.reasons
            )

        elif (
            recovery_plan.decision
            == RecoveryDecision.RESTORE_FROM_CHECKPOINT
        ):
            decision = CoordinationDecision.RECOVER
            state = RecrawlOperationState.RECOVERY_PENDING

            reasons.extend(
                recovery_plan.reasons
            )

        elif (
            recovery_plan.decision
            == RecoveryDecision.REBUILD_COORDINATION
        ):
            decision = CoordinationDecision.RECOVER
            state = RecrawlOperationState.RECOVERY_PENDING

            reasons.extend(
                recovery_plan.reasons
            )

        elif recovery_plan.decision == RecoveryDecision.DEFER:
            decision = CoordinationDecision.DEFER
            state = RecrawlOperationState.DEFERRED

            reasons.extend(
                recovery_plan.reasons
            )

        elif recovery_plan.decision == RecoveryDecision.QUARANTINE:
            decision = CoordinationDecision.DEFER
            state = RecrawlOperationState.DEFERRED

            reasons.extend(
                recovery_plan.reasons
            )

        elif recovery_plan.decision == RecoveryDecision.CANCEL:
            decision = CoordinationDecision.REJECT
            state = RecrawlOperationState.CANCELLED

            reasons.extend(
                recovery_plan.reasons
            )

        else:
            if not operation.partition_id:
                reasons.append(
                    "partition_not_preassigned"
                )

            if not operation.region_id:
                reasons.append(
                    "region_not_preassigned"
                )

            if operation.partial:
                reasons.append(
                    "partial_operation_input"
                )

        priority = self._clamp(
            operation.priority_score
            + recovery_plan.recovery_priority * 0.35
        )

        if operation.urgency_score > 0.80:
            priority = self._clamp(
                priority + 0.10
            )

        coordinated = CoordinatedOperation(
            resource_id=operation.resource_id,
            operation_id=operation.operation_id,
            coordination_id=coordination_id,
            state=state,
            decision=decision,
            coordination_scope=scope,
            priority_score=priority,
            recovery_priority=recovery_plan.recovery_priority,
            partition_id=operation.partition_id,
            shard_id=operation.shard_id,
            region_id=operation.region_id,
            target_partition_id=target_partition,
            target_region_id=target_region,
            retry_count=recovery_plan.retry_count,
            next_retry_at=recovery_plan.next_retry_at,
            recovery_plan=recovery_plan,
            failure_class=recovery_plan.failure_class,
            reasons=reasons,
            confidence=self._clamp(
                min(
                    operation.confidence,
                    recovery_plan.confidence,
                )
            ),
            partial=operation.partial,
            metadata=dict(operation.metadata),
        )

        self.backend.persist_operation(
            coordinated
        )

        return coordinated

    # ------------------------------------------------------------------
    # DUPLICATE COORDINATION
    # ------------------------------------------------------------------

    def _deduplicate_operations(
        self,
        operations: Sequence[RecrawlOperationInput],
    ) -> List[RecrawlOperationInput]:

        seen: Dict[str, RecrawlOperationInput] = {}

        for operation in operations:

            key = (
                operation.operation_id
                or operation.resource_id
            )

            existing = seen.get(key)

            if existing is None:
                seen[key] = operation
                continue

            existing.priority_score = max(
                existing.priority_score,
                operation.priority_score,
            )

            existing.urgency_score = max(
                existing.urgency_score,
                operation.urgency_score,
            )

            existing.confidence = max(
                existing.confidence,
                operation.confidence,
            )

            existing.partial = (
                existing.partial
                or operation.partial
            )

            if operation.attempt_count > existing.attempt_count:
                existing.attempt_count = (
                    operation.attempt_count
                )

            if (
                operation.last_failure_class
                != RecrawlFailureClass.NONE
            ):
                existing.last_failure_class = (
                    operation.last_failure_class
                )

            if (
                operation.last_failure_severity
                != RecrawlFailureSeverity.NONE
            ):
                existing.last_failure_severity = (
                    operation.last_failure_severity
                )

        return list(seen.values())

    # ------------------------------------------------------------------
    # GLOBAL COORDINATION
    # ------------------------------------------------------------------

    def coordinate(
        self,
        coordination_input: CoordinationInput,
        *,
        coordination_id: Optional[str] = None,
        lineage: Optional[
            GlobalRecrawlCoordinationLineage
        ] = None,
    ) -> GlobalRecrawlCoordinationResult:

        if coordination_id is None:
            coordination_id = hashlib.sha256(
                (
                    f"{self._now().isoformat()}|"
                    f"{len(coordination_input.operations)}|"
                    f"{coordination_input.source_version}"
                ).encode("utf-8")
            ).hexdigest()

        self._event(
            coordination_id,
            CoordinationEventType.REQUEST_RECEIVED,
        )

        self._event(
            coordination_id,
            CoordinationEventType.VALIDATION_STARTED,
        )

        normalized = self._normalize_input(
            coordination_input
        )

        if normalized.partial:
            self._event(
                coordination_id,
                CoordinationEventType.PARTIAL_INPUT_DETECTED,
            )

            if not self.policy.allow_partial:
                self._event(
                    coordination_id,
                    CoordinationEventType.COORDINATION_REJECTED,
                )

                return GlobalRecrawlCoordinationResult(
                    coordination_id=coordination_id,
                    state=GlobalRecrawlCoordinationState.REJECTED,
                    decision=CoordinationDecision.REJECT,
                    partial=True,
                    confidence=normalized.confidence,
                    created_at=self._now().isoformat(),
                    lineage=lineage,
                )

        self._event(
            coordination_id,
            CoordinationEventType.INPUT_NORMALIZED,
        )

        self._checkpoint(
            coordination_id,
            CoordinationCheckpointType.INPUT_NORMALIZED,
            GlobalRecrawlCoordinationState.NORMALIZING,
            operation_count=len(
                normalized.operations
            ),
            failure_count=len(
                normalized.failures
            ),
            partition_count=len(
                normalized.partition_health
            ),
            region_count=len(
                normalized.region_health
            ),
            partial=normalized.partial,
        )

        self._event(
            coordination_id,
            CoordinationEventType.PARTITION_COORDINATION_STARTED,
        )

        deduplicated_operations = (
            self._deduplicate_operations(
                normalized.operations
            )
        )

        coordinated_operations: List[
            CoordinatedOperation
        ] = []

        recovery_plans: List[RecoveryPlan] = []

        for operation in deduplicated_operations:

            failures = self._failures_for_operation(
                operation,
                normalized.failures,
            )

            if failures:
                self._event(
                    coordination_id,
                    CoordinationEventType.FAILURE_CLASSIFIED,
                    resource_id=operation.resource_id,
                    operation_id=operation.operation_id,
                    failure_class=failures[-1].failure_class,
                )

            coordinated = self._coordinate_operation(
                coordination_id,
                operation,
                failures,
                normalized.partition_health,
                normalized.region_health,
                normalized.coordination_scope,
            )

            coordinated_operations.append(
                coordinated
            )

            if coordinated.recovery_plan is not None:
                recovery_plans.append(
                    coordinated.recovery_plan
                )

        self._checkpoint(
            coordination_id,
            CoordinationCheckpointType.PARTITIONS_COORDINATED,
            GlobalRecrawlCoordinationState.PARTITION_COORDINATION,
            operation_count=len(
                coordinated_operations
            ),
            failure_count=len(
                normalized.failures
            ),
            recovery_count=len(
                recovery_plans
            ),
            partition_count=len(
                normalized.partition_health
            ),
            region_count=len(
                normalized.region_health
            ),
            partial=normalized.partial,
        )

        retry_count = sum(
            1
            for operation in coordinated_operations
            if operation.decision
            == CoordinationDecision.RETRY
        )

        recovery_count = sum(
            1
            for operation in coordinated_operations
            if operation.decision
            == CoordinationDecision.RECOVER
        )

        rebalanced_count = sum(
            1
            for operation in coordinated_operations
            if operation.decision
            == CoordinationDecision.REBALANCE
        )

        deferred_count = sum(
            1
            for operation in coordinated_operations
            if operation.decision
            == CoordinationDecision.DEFER
        )

        rejected_count = sum(
            1
            for operation in coordinated_operations
            if operation.decision
            == CoordinationDecision.REJECT
        )

        accepted_count = sum(
            1
            for operation in coordinated_operations
            if operation.decision
            == CoordinationDecision.ACCEPT
        )

        if recovery_count:
            self._event(
                coordination_id,
                CoordinationEventType.RECOVERY_PLAN_CREATED,
                metadata={
                    "recovery_count": recovery_count
                },
            )

        if retry_count:
            self._event(
                coordination_id,
                CoordinationEventType.RETRY_PLAN_CREATED,
                metadata={
                    "retry_count": retry_count
                },
            )

        if rebalanced_count:
            self._event(
                coordination_id,
                CoordinationEventType.REBALANCING_COMPLETED,
                metadata={
                    "rebalanced_count": rebalanced_count
                },
            )

        if any(
            operation.decision
            in {
                CoordinationDecision.RECOVER,
                CoordinationDecision.REBALANCE,
                CoordinationDecision.RETRY,
            }
            for operation in coordinated_operations
        ):
            final_decision = CoordinationDecision.COORDINATE
        elif deferred_count:
            final_decision = CoordinationDecision.DEFER
        elif rejected_count == len(
            coordinated_operations
        ) and coordinated_operations:
            final_decision = CoordinationDecision.REJECT
        else:
            final_decision = CoordinationDecision.ACCEPT

        if not coordinated_operations:
            final_state = (
                GlobalRecrawlCoordinationState.DEFERRED
            )
            final_decision = CoordinationDecision.DEFER
        elif rejected_count == len(
            coordinated_operations
        ):
            final_state = (
                GlobalRecrawlCoordinationState.REJECTED
            )
        elif (
            retry_count
            or recovery_count
            or rebalanced_count
            or deferred_count
        ):
            final_state = (
                GlobalRecrawlCoordinationState.PARTIAL
            )
        else:
            final_state = (
                GlobalRecrawlCoordinationState.COMPLETED
            )

        confidence_values = [
            operation.confidence
            for operation in coordinated_operations
        ]

        if confidence_values:
            confidence = sum(
                confidence_values
            ) / len(confidence_values)
        else:
            confidence = normalized.confidence

        confidence = self._clamp(
            confidence
        )

        if normalized.partial:
            confidence = self._clamp(
                confidence
                - self.policy.partial_result_penalty
            )

        created_at = self._now().isoformat()

        result = GlobalRecrawlCoordinationResult(
            coordination_id=coordination_id,
            state=final_state,
            decision=final_decision,
            operations=coordinated_operations,
            recovery_plans=recovery_plans,
            accepted_count=accepted_count,
            retry_count=retry_count,
            recovery_count=recovery_count,
            rebalanced_count=rebalanced_count,
            deferred_count=deferred_count,
            rejected_count=rejected_count,
            partial=normalized.partial
            or final_state
            == GlobalRecrawlCoordinationState.PARTIAL,
            confidence=confidence,
            created_at=created_at,
            completed_at=(
                self._now().isoformat()
                if final_state
                == GlobalRecrawlCoordinationState.COMPLETED
                else None
            ),
            lineage=lineage,
            metadata={
                "input_operation_count": len(
                    normalized.operations
                ),
                "deduplicated_operation_count": len(
                    deduplicated_operations
                ),
                "input_failure_count": len(
                    normalized.failures
                ),
                "coordination_scope": (
                    normalized.coordination_scope.value
                ),
                "source_version": (
                    normalized.source_version
                ),
            },
        )

        self._checkpoint(
            coordination_id,
            CoordinationCheckpointType.FAILURES_ANALYZED,
            final_state,
            operation_count=len(
                coordinated_operations
            ),
            failure_count=len(
                normalized.failures
            ),
            recovery_count=len(
                recovery_plans
            ),
            partition_count=len(
                normalized.partition_health
            ),
            region_count=len(
                normalized.region_health
            ),
            partial=result.partial,
        )

        self._checkpoint(
            coordination_id,
            CoordinationCheckpointType.RECOVERY_PLANNED,
            final_state,
            operation_count=len(
                coordinated_operations
            ),
            failure_count=len(
                normalized.failures
            ),
            recovery_count=len(
                recovery_plans
            ),
            partition_count=len(
                normalized.partition_health
            ),
            region_count=len(
                normalized.region_health
            ),
            partial=result.partial,
        )

        self.backend.persist_result(
            result
        )

        self._event(
            coordination_id,
            CoordinationEventType.COORDINATION_COMPLETED,
            metadata={
                "state": final_state.value,
                "decision": final_decision.value,
                "operation_count": len(
                    coordinated_operations
                ),
            },
        )

        self._checkpoint(
            coordination_id,
            CoordinationCheckpointType.COMPLETED,
            final_state,
            operation_count=len(
                coordinated_operations
            ),
            failure_count=len(
                normalized.failures
            ),
            recovery_count=len(
                recovery_plans
            ),
            partition_count=len(
                normalized.partition_health
            ),
            region_count=len(
                normalized.region_health
            ),
            partial=result.partial,
        )

        return result

    # ------------------------------------------------------------------
    # BATCH COORDINATION
    # ------------------------------------------------------------------

    def coordinate_many(
        self,
        batches: Iterable[CoordinationInput],
    ) -> List[GlobalRecrawlCoordinationResult]:

        results: List[
            GlobalRecrawlCoordinationResult
        ] = []

        for batch in batches:
            results.append(
                self.coordinate(batch)
            )

        return results

    # ------------------------------------------------------------------
    # FAILURE RECOVERY
    # ------------------------------------------------------------------

    def recover(
        self,
        operation: RecrawlOperationInput,
        *,
        failures: Optional[
            Sequence[RecrawlFailureRecord]
        ] = None,
        partition_health: Optional[
            Sequence[PartitionHealth]
        ] = None,
        region_health: Optional[
            Sequence[RegionHealth]
        ] = None,
    ) -> RecoveryPlan:

        failure_records = list(
            failures or []
        )

        partition_records = list(
            partition_health or []
        )

        region_records = list(
            region_health or []
        )

        return self._build_recovery_plan(
            self._normalize_operation(
                operation
            ),
            failure_records,
            partition_records,
            region_records,
        )

    # ------------------------------------------------------------------
    # RETRY
    # ------------------------------------------------------------------

    def retry_plan(
        self,
        operation: RecrawlOperationInput,
        failure: Optional[
            RecrawlFailureRecord
        ] = None,
    ) -> RecoveryPlan:

        operation = self._normalize_operation(
            operation
        )

        if operation.attempt_count >= (
            self.policy.max_retry_attempts
        ):
            return RecoveryPlan(
                resource_id=operation.resource_id,
                operation_id=operation.operation_id,
                decision=RecoveryDecision.QUARANTINE,
                retry_policy=RetryPolicy.NONE,
                retry_count=operation.attempt_count,
                quarantine=True,
                reasons=[
                    "maximum_retry_attempts_reached"
                ],
                confidence=operation.confidence,
                partial=operation.partial,
            )

        delay = self._retry_delay(
            operation,
            failure,
        )

        return RecoveryPlan(
            resource_id=operation.resource_id,
            operation_id=operation.operation_id,
            decision=RecoveryDecision.RETRY,
            failure_class=(
                failure.failure_class
                if failure is not None
                else operation.last_failure_class
            ),
            retry_policy=RetryPolicy.EXPONENTIAL_JITTER,
            retry_count=operation.attempt_count,
            next_retry_at=(
                self._now()
                + timedelta(seconds=delay)
            ).isoformat(),
            recovery_priority=self._clamp(
                operation.priority_score
                + self.policy.recovery_priority_boost
            ),
            reasons=[
                "retry_requested"
            ],
            confidence=operation.confidence,
            partial=operation.partial,
            metadata={
                "retry_delay_seconds": delay
            },
        )

    # ------------------------------------------------------------------
    # PARTITION / REGION REBALANCING
    # ------------------------------------------------------------------

    def rebalance(
        self,
        operations: Sequence[RecrawlOperationInput],
        partition_health: Sequence[PartitionHealth],
        region_health: Sequence[RegionHealth],
    ) -> List[CoordinatedOperation]:

        coordination_id = hashlib.sha256(
            (
                f"rebalance|"
                f"{self._now().isoformat()}|"
                f"{len(operations)}"
            ).encode("utf-8")
        ).hexdigest()

        results: List[
            CoordinatedOperation
        ] = []

        for operation in operations:

            operation = self._normalize_operation(
                operation
            )

            healthy_partition = (
                self._partition_is_healthy(
                    operation.partition_id,
                    self._partition_health_map(
                        partition_health
                    ),
                )
            )

            healthy_region = (
                self._region_is_healthy(
                    operation.region_id,
                    self._region_health_map(
                        region_health
                    ),
                )
            )

            if healthy_partition and healthy_region:
                results.append(
                    CoordinatedOperation(
                        resource_id=operation.resource_id,
                        operation_id=operation.operation_id,
                        coordination_id=coordination_id,
                        state=RecrawlOperationState.READY,
                        decision=CoordinationDecision.ACCEPT,
                        coordination_scope=(
                            CoordinationScope.PARTITION
                        ),
                        priority_score=(
                            operation.priority_score
                        ),
                        recovery_priority=0.0,
                        partition_id=operation.partition_id,
                        shard_id=operation.shard_id,
                        region_id=operation.region_id,
                        target_partition_id=(
                            operation.partition_id
                        ),
                        target_region_id=(
                            operation.region_id
                        ),
                        confidence=operation.confidence,
                        partial=operation.partial,
                        reasons=[
                            "existing_location_healthy"
                        ],
                    )
                )

                continue

            (
                target_partition,
                target_region,
            ) = self._find_recovery_partition(
                operation,
                partition_health,
                region_health,
            )

            if target_partition:
                decision = CoordinationDecision.REBALANCE
                state = (
                    RecrawlOperationState.RECOVERY_PENDING
                )
                reasons = [
                    "unhealthy_current_location",
                    "healthy_recovery_location_found",
                ]
            else:
                decision = CoordinationDecision.DEFER
                state = (
                    RecrawlOperationState.DEFERRED
                )
                reasons = [
                    "unhealthy_current_location",
                    "no_recovery_location_available",
                ]

            results.append(
                CoordinatedOperation(
                    resource_id=operation.resource_id,
                    operation_id=operation.operation_id,
                    coordination_id=coordination_id,
                    state=state,
                    decision=decision,
                    coordination_scope=(
                        CoordinationScope.PARTITION
                    ),
                    priority_score=operation.priority_score,
                    recovery_priority=(
                        self._clamp(
                            operation.priority_score
                            + self.policy.recovery_priority_boost
                        )
                    ),
                    partition_id=operation.partition_id,
                    shard_id=operation.shard_id,
                    region_id=operation.region_id,
                    target_partition_id=target_partition,
                    target_region_id=target_region,
                    confidence=operation.confidence,
                    partial=operation.partial,
                    reasons=reasons,
                )
            )

        return results

    # ------------------------------------------------------------------
    # CHECKPOINT RESTORATION
    # ------------------------------------------------------------------

    def restore(
        self,
        coordination_id: str,
    ) -> Optional[
        GlobalRecrawlCoordinationResult
    ]:

        result = self.backend.get_result(
            coordination_id
        )

        if result is None:
            return None

        restored_operations: List[
            CoordinatedOperation
        ] = []

        for operation in result.operations:

            restored = operation

            if operation.state in {
                RecrawlOperationState.IN_PROGRESS,
                RecrawlOperationState.READY,
                RecrawlOperationState.DISPATCHABLE,
            }:
                restored.state = (
                    RecrawlOperationState.RECOVERY_PENDING
                )

                if (
                    operation.decision
                    == CoordinationDecision.ACCEPT
                ):
                    restored.decision = (
                        CoordinationDecision.RECOVER
                    )

                restored.reasons = list(
                    operation.reasons
                ) + [
                    "restored_from_coordination_checkpoint"
                ]

            restored_operations.append(
                restored
            )

            self.backend.persist_operation(
                restored
            )

        result.operations = restored_operations

        result.state = (
            GlobalRecrawlCoordinationState.PARTIAL
        )

        result.partial = True

        result.metadata[
            "restored_from_checkpoint"
        ] = True

        self.backend.persist_result(
            result
        )

        self._event(
            coordination_id,
            CoordinationEventType.COORDINATION_UPDATED,
            metadata={
                "restored": True
            },
        )

        return result

    # ------------------------------------------------------------------
    # METADATA ACCESS
    # ------------------------------------------------------------------

    def events(
        self,
    ) -> List[GlobalRecrawlCoordinationEvent]:

        if hasattr(self.backend, "events"):
            return list(
                getattr(self.backend, "events")()
            )

        return []

    def checkpoints(
        self,
    ) -> List[
        GlobalRecrawlCoordinationCheckpoint
    ]:

        if hasattr(self.backend, "checkpoints"):
            return list(
                getattr(self.backend, "checkpoints")()
            )

        return []

    def result(
        self,
        coordination_id: str,
    ) -> Optional[
        GlobalRecrawlCoordinationResult
    ]:

        return self.backend.get_result(
            coordination_id
        )

    # ------------------------------------------------------------------
    # ARCHITECTURE DESCRIPTION
    # ------------------------------------------------------------------

    def architecture(self) -> Dict[str, Any]:

        return {
            "phase": PHASE,
            "name": (
                "Global Recrawl Coordination / "
                "Failure Recovery"
            ),
            "version": ARCHITECTURE_VERSION,
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),

            "purpose": (
                "Globally coordinate freshness recrawl "
                "operations and recover distributed "
                "recrawl failures."
            ),

            "responsibilities": [
                "global recrawl coordination",
                "partition coordination",
                "distributed failure analysis",
                "failure classification",
                "retry planning",
                "recovery planning",
                "checkpoint restoration",
                "partition relocation",
                "region failover planning",
                "deterministic retry scheduling",
                "durable coordination state",
                "restartable coordination",
                "lineage preservation",
                "partial-result handling",
                "global coordination metadata",
            ],

            "does_not": [
                "perform HTTP requests",
                "fetch Web resources",
                "execute crawling",
                "parse Web documents",
                "assign crawler workers",
                "execute crawler workers",
                "mutate search indexes",
                "perform final ranking",
                "classify spam",
                "replace crawler transport",
                "replace distributed crawl orchestration",
                "depend on Google Search",
                "depend on Google's index",
                "depend on Google's crawler",
                "depend on Google's infrastructure",
            ],

            "distributed_properties": {
                "distributed_execution": True,
                "partition_aware": True,
                "resource_partitionable": True,
                "shard_aware": True,
                "region_aware": True,
                "horizontally_scalable": True,
                "global_coordination": True,
                "partial_results_supported": True,
                "deterministic": True,
                "checkpointable": True,
                "restartable": True,
                "failure_recovery": True,
                "retry_planning": True,
                "relocation_planning": True,
                "region_failover_planning": True,
                "lineage_preserving": True,
                "backend_replaceable": True,
            },

            "failure_recovery": {
                "transient_failures": True,
                "network_failures": True,
                "timeouts": True,
                "rate_limits": True,
                "host_failures": True,
                "partition_failures": True,
                "region_failures": True,
                "queue_failures": True,
                "resource_exhaustion": True,
                "coordination_failures": True,
                "persistence_failures": True,
                "checkpoint_restore": True,
                "deterministic_backoff": True,
                "jitter": True,
                "retry_limits": True,
                "quarantine_after_retry_exhaustion": True,
            },

            "global_scale_constraints": {
                "fixed_global_resource_limit": False,
                "fixed_global_url_limit": False,
                "fixed_global_document_limit": False,
                "fixed_global_partition_limit": False,
                "fixed_global_shard_limit": False,
                "fixed_global_worker_limit": False,
                "fixed_global_region_limit": False,
                "fixed_global_retry_limit": False,
                "fixed_global_failure_limit": False,
                "fixed_global_recovery_limit": False,
            },

            "stage_boundaries": {
                "13.1": (
                    "determines freshness attention "
                    "and urgency"
                ),
                "13.2": (
                    "determines what changed and "
                    "change volatility"
                ),
                "13.3": (
                    "determines when resources "
                    "should be recrawled"
                ),
                "13.4": (
                    "adapts long-term recrawl "
                    "frequency"
                ),
                "13.5": (
                    "places freshness work into "
                    "durable URL/document queues"
                ),
                "13.6": (
                    "orchestrates distributed "
                    "recrawl execution"
                ),
                "13.7": (
                    "allocates crawl resources "
                    "according to freshness demand"
                ),
                "13.8": (
                    "coordinates global recrawl state "
                    "and recovers failures"
                ),
                "13.9": (
                    "will combine the complete "
                    "freshness and recrawling architecture"
                ),
            },

            "next_stage": NEXT_STAGE,
            "next_stage_name": (
                "Final Freshness + Recrawling Architecture"
            ),
        }


# ============================================================================
# ALIASES
# ============================================================================


GlobalRecrawlCoordination = (
    GlobalRecrawlCoordinationFailureRecoveryArchitecture
)

GlobalRecrawlFailureRecovery = (
    GlobalRecrawlCoordinationFailureRecoveryArchitecture
)

Phase13_8GlobalRecrawlCoordination = (
    GlobalRecrawlCoordinationFailureRecoveryArchitecture
)

GlobalRecrawlCoordinationArchitecture = (
    GlobalRecrawlCoordinationFailureRecoveryArchitecture
)


# ============================================================================
# MODULE EXPORTS
# ============================================================================


__all__ = [
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",
    "ARCHITECTURE_VERSION",
    "PHASE",
    "PREVIOUS_STAGE",
    "NEXT_STAGE",

    "GlobalRecrawlCoordinationState",
    "RecrawlOperationState",
    "RecrawlFailureClass",
    "RecrawlFailureSeverity",
    "RecoveryDecision",
    "RetryPolicy",
    "CoordinationDecision",
    "CoordinationScope",
    "CoordinationEventType",
    "CoordinationCheckpointType",

    "GlobalRecrawlCoordinationIdentity",
    "GlobalRecrawlCoordinationLineage",

    "RecrawlOperationInput",
    "RecrawlFailureRecord",
    "PartitionHealth",
    "RegionHealth",
    "CoordinationInput",

    "GlobalRecrawlCoordinationPolicy",

    "RecoveryPlan",
    "CoordinatedOperation",
    "GlobalRecrawlCoordinationResult",

    "GlobalRecrawlCoordinationCheckpoint",
    "GlobalRecrawlCoordinationEvent",

    "GlobalRecrawlCoordinationBackend",
    "InMemoryGlobalRecrawlCoordinationMetadata",

    "GlobalRecrawlCoordinationFailureRecoveryArchitecture",

    "GlobalRecrawlCoordination",
    "GlobalRecrawlFailureRecovery",
    "Phase13_8GlobalRecrawlCoordination",
    "GlobalRecrawlCoordinationArchitecture",
]
