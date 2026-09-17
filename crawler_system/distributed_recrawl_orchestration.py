"""
OUR SEARCH
Phase 13.6 — Distributed Recrawl Orchestration

Purpose
-------
Coordinate freshness-driven recrawl work across a distributed crawler
infrastructure.

This stage consumes URL/document freshness queue entries and determines:

- which distributed execution group should handle a recrawl
- partition and shard affinity
- regional / zonal execution preference
- concurrency allocation
- lease ownership
- retry and recovery state
- queue-to-worker execution assignments
- failure recovery
- orchestration checkpoints
- deterministic execution planning
- partial-result handling

This module is an orchestration architecture and execution-control layer.

It does NOT:
- perform HTTP requests
- fetch Web resources
- parse Web pages
- mutate the search index
- perform final ranking
- discover new URLs
- replace freshness scheduling
- replace adaptive recrawl frequency
- replace freshness queues
- classify spam
- make final content-quality decisions
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
- queue entries
- partitions
- shards
- workers
- regions
- zones
- orchestration plans
- recrawl tasks
- retries
- leases

Per-request and per-batch safety limits exist only to bound individual
processing units.

Stage boundaries
----------------
13.1 -> determines freshness attention / urgency
13.2 -> determines what changed and volatility
13.3 -> determines when a resource should be recrawled
13.4 -> adapts long-term recrawl frequency
13.5 -> places freshness work into durable URL/document queues
13.6 -> orchestrates queued recrawl work across distributed infrastructure
13.7 -> allocates crawl resources according to freshness importance
13.8 -> coordinates global recovery and failure handling
13.9 -> final freshness + recrawling architecture
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

ARCHITECTURE_VERSION = "distributed-recrawl-orchestration.v1"
PHASE = "13.6"
PREVIOUS_STAGE = "13.5"
NEXT_STAGE = "13.7"


# ============================================================================
# ENUMS
# ============================================================================


class DistributedRecrawlOrchestrationState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    PARTITION_ROUTING = "partition_routing"
    REGION_ROUTING = "region_routing"
    ZONE_ROUTING = "zone_routing"
    CAPACITY_ANALYSIS = "capacity_analysis"
    EXECUTION_PLANNING = "execution_planning"
    LEASE_PLANNING = "lease_planning"
    RETRY_PLANNING = "retry_planning"
    RECOVERY_PLANNING = "recovery_planning"
    ASSIGNMENT = "assignment"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class RecrawlExecutionMode(str, Enum):
    NORMAL = "normal"
    ACCELERATED = "accelerated"
    HIGH_PRIORITY = "high_priority"
    URGENT = "urgent"
    IMMEDIATE = "immediate"
    RECOVERY = "recovery"
    RETRY = "retry"


class RecrawlPriorityBand(str, Enum):
    BACKGROUND = "background"
    NORMAL = "normal"
    ELEVATED = "elevated"
    HIGH = "high"
    URGENT = "urgent"
    IMMEDIATE = "immediate"


class OrchestrationDecision(str, Enum):
    ASSIGN = "assign"
    DEFER = "defer"
    RETRY = "retry"
    RECOVER = "recover"
    PARTIAL_ASSIGN = "partial_assign"
    REJECT = "reject"


class ExecutionTargetType(str, Enum):
    PARTITION = "partition"
    SHARD = "shard"
    ZONE = "zone"
    REGION = "region"
    WORKER_POOL = "worker_pool"


class RecrawlTaskState(str, Enum):
    PENDING = "pending"
    READY = "ready"
    LEASED = "leased"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    DEFERRED = "deferred"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LeaseState(str, Enum):
    NONE = "none"
    AVAILABLE = "available"
    LEASED = "leased"
    EXPIRED = "expired"
    RELEASED = "released"


class RetryReason(str, Enum):
    WORKER_FAILURE = "worker_failure"
    NETWORK_FAILURE = "network_failure"
    TIMEOUT = "timeout"
    CAPACITY_FAILURE = "capacity_failure"
    REGION_FAILURE = "region_failure"
    ZONE_FAILURE = "zone_failure"
    LEASE_EXPIRATION = "lease_expiration"
    ORCHESTRATION_FAILURE = "orchestration_failure"
    UNKNOWN_FAILURE = "unknown_failure"


class OrchestrationReason(str, Enum):
    DUE_RECRAWL = "due_recrawl"
    RECENT_CHANGE = "recent_change"
    HIGH_VOLATILITY = "high_volatility"
    HIGH_PRIORITY = "high_priority"
    URGENT_PRIORITY = "urgent_priority"
    MISSED_DEADLINE = "missed_deadline"
    RECOVERY_REQUIRED = "recovery_required"
    RETRY_REQUIRED = "retry_required"
    INITIAL_RECRAWL = "initial_recrawl"
    TEMPORAL_SENSITIVITY = "temporal_sensitivity"
    SOURCE_FRESHNESS = "source_freshness"
    FEED_UPDATE = "feed_update"
    SITEMAP_UPDATE = "sitemap_update"
    EXTERNAL_UPDATE = "external_update"
    PARTITION_AFFINITY = "partition_affinity"
    REGIONAL_AFFINITY = "regional_affinity"
    ZONE_AFFINITY = "zone_affinity"
    CAPACITY_AVAILABLE = "capacity_available"
    PARTIAL_CAPACITY = "partial_capacity"
    CAPACITY_LIMITED = "capacity_limited"
    DUPLICATE_TASK = "duplicate_task"


class OrchestrationEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    INPUT_NORMALIZED = "input_normalized"
    PARTITION_ROUTING_STARTED = "partition_routing_started"
    PARTITION_ROUTE_SELECTED = "partition_route_selected"
    REGION_ROUTING_STARTED = "region_routing_started"
    REGION_ROUTE_SELECTED = "region_route_selected"
    ZONE_ROUTING_STARTED = "zone_routing_started"
    ZONE_ROUTE_SELECTED = "zone_route_selected"
    CAPACITY_ANALYSIS_STARTED = "capacity_analysis_started"
    CAPACITY_ANALYZED = "capacity_analyzed"
    EXECUTION_PLAN_CREATED = "execution_plan_created"
    LEASE_PLAN_CREATED = "lease_plan_created"
    RETRY_PLAN_CREATED = "retry_plan_created"
    RECOVERY_PLAN_CREATED = "recovery_plan_created"
    TASK_ASSIGNED = "task_assigned"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    TASK_DEFERRED = "task_deferred"
    TASK_REJECTED = "task_rejected"
    CHECKPOINT_CREATED = "checkpoint_created"
    ORCHESTRATION_COMPLETED = "orchestration_completed"
    ORCHESTRATION_FAILED = "orchestration_failed"


class OrchestrationCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    PARTITION_ROUTED = "partition_routed"
    REGION_ROUTED = "region_routed"
    ZONE_ROUTED = "zone_routed"
    CAPACITY_ANALYZED = "capacity_analyzed"
    EXECUTION_PLANNED = "execution_planned"
    LEASE_PLANNED = "lease_planned"
    RETRY_PLANNED = "retry_planned"
    RECOVERY_PLANNED = "recovery_planned"
    TASK_ASSIGNED = "task_assigned"
    COMPLETED = "completed"


# ============================================================================
# DATACLASSES
# ============================================================================


@dataclass(frozen=True)
class DistributedRecrawlOrchestrationIdentity:
    resource_id: str
    queue_entry_id: str = ""
    orchestration_id: str = ""
    orchestration_version: str = ARCHITECTURE_VERSION
    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""
    zone_id: str = ""
    worker_pool_id: str = ""

    def key(self) -> str:
        return (
            f"{self.resource_id}:"
            f"{self.queue_entry_id}:"
            f"{self.orchestration_version}"
        )


@dataclass
class DistributedRecrawlOrchestrationLineage:
    resource_id: str
    previous_stage: str = PREVIOUS_STAGE
    current_stage: str = PHASE

    source_queue_entry_id: str = ""
    source_queue_version: str = ""
    source_schedule_id: str = ""
    source_frequency_version: str = ""

    parent_orchestration_ids: List[str] = field(default_factory=list)

    lineage_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecrawlOrchestrationInput:
    resource_id: str

    queue_entry_id: str = ""
    queue_version: str = ""

    resource_type: str = "url"

    canonical_url: str = ""
    document_id: str = ""
    host_id: str = ""
    domain_id: str = ""

    partition_id: str = ""
    shard_id: str = ""

    scheduled_at: Optional[str] = None
    deadline_at: Optional[str] = None

    priority_score: float = 0.5
    priority_band: str = RecrawlPriorityBand.NORMAL.value

    queue_lane: str = "normal"
    queue_state: str = "ready"

    execution_mode: str = RecrawlExecutionMode.NORMAL.value

    adaptive_interval_seconds: float = 86400.0

    freshness_score: float = 0.0
    urgency_score: float = 0.0
    change_frequency: float = 0.0
    change_volatility: float = 0.0

    confidence: float = 1.0
    partial: bool = False

    preferred_region: str = ""
    preferred_zone: str = ""

    source_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecrawlWorkerCapacity:
    worker_pool_id: str

    region_id: str = ""
    zone_id: str = ""

    available_workers: int = 0
    active_workers: int = 0
    maximum_workers: int = 0

    available_slots: int = 0
    maximum_slots: int = 0

    health_score: float = 1.0
    capacity_score: float = 1.0

    accepting_work: bool = True
    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecrawlExecutionTarget:
    target_type: ExecutionTargetType

    target_id: str

    region_id: str = ""
    zone_id: str = ""
    worker_pool_id: str = ""

    health_score: float = 1.0
    capacity_score: float = 1.0

    affinity_score: float = 0.0
    routing_score: float = 0.0

    accepting_work: bool = True

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecrawlLease:
    lease_id: str

    resource_id: str
    queue_entry_id: str

    state: LeaseState = LeaseState.AVAILABLE

    worker_pool_id: str = ""
    region_id: str = ""
    zone_id: str = ""

    created_at: str = ""
    lease_started_at: str = ""
    lease_until: str = ""

    lease_seconds: float = 300.0

    renewal_count: int = 0
    maximum_renewals: int = 3

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecrawlRetryPlan:
    resource_id: str
    queue_entry_id: str

    attempt_number: int = 0
    maximum_attempts: int = 5

    retry_reason: RetryReason = RetryReason.UNKNOWN_FAILURE

    retry_delay_seconds: float = 60.0
    next_retry_at: str = ""

    backoff_multiplier: float = 2.0
    jitter_fraction: float = 0.05

    eligible: bool = True
    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecrawlRecoveryPlan:
    resource_id: str
    queue_entry_id: str

    failed_region_id: str = ""
    failed_zone_id: str = ""
    failed_worker_pool_id: str = ""

    recovery_region_id: str = ""
    recovery_zone_id: str = ""
    recovery_worker_pool_id: str = ""

    recovery_attempt: int = 0
    maximum_recovery_attempts: int = 3

    reason: RetryReason = RetryReason.UNKNOWN_FAILURE

    cross_zone_allowed: bool = True
    cross_region_allowed: bool = True

    eligible: bool = True
    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecrawlExecutionTask:
    task_id: str

    resource_id: str
    queue_entry_id: str

    resource_type: str = "url"

    canonical_url: str = ""
    document_id: str = ""

    partition_id: str = ""
    shard_id: str = ""

    region_id: str = ""
    zone_id: str = ""
    worker_pool_id: str = ""

    state: RecrawlTaskState = RecrawlTaskState.PENDING

    execution_mode: RecrawlExecutionMode = RecrawlExecutionMode.NORMAL

    priority_score: float = 0.5
    priority_band: RecrawlPriorityBand = RecrawlPriorityBand.NORMAL

    scheduled_at: str = ""
    deadline_at: str = ""

    assigned_at: str = ""
    started_at: str = ""
    completed_at: str = ""

    attempt_number: int = 0
    maximum_attempts: int = 5

    lease: Optional[RecrawlLease] = None

    retry_plan: Optional[RecrawlRetryPlan] = None
    recovery_plan: Optional[RecrawlRecoveryPlan] = None

    reasons: List[OrchestrationReason] = field(default_factory=list)

    lineage: Optional[DistributedRecrawlOrchestrationLineage] = None

    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecrawlOrchestrationPlan:
    orchestration_id: str

    resource_id: str
    queue_entry_id: str

    state: DistributedRecrawlOrchestrationState

    decision: OrchestrationDecision

    execution_mode: RecrawlExecutionMode

    selected_partition_id: str = ""
    selected_shard_id: str = ""

    selected_region_id: str = ""
    selected_zone_id: str = ""
    selected_worker_pool_id: str = ""

    routing_score: float = 0.0
    capacity_score: float = 0.0

    tasks: List[RecrawlExecutionTask] = field(default_factory=list)

    reasons: List[OrchestrationReason] = field(default_factory=list)

    partial: bool = False

    lineage: Optional[DistributedRecrawlOrchestrationLineage] = None

    created_at: str = ""
    updated_at: str = ""

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DistributedRecrawlOrchestrationPolicy:
    max_resources_per_batch: int = 100000

    max_tasks_per_resource: int = 16
    max_targets_per_resource: int = 64
    max_worker_pools_per_resource: int = 32

    minimum_confidence: float = 0.10
    minimum_worker_health: float = 0.40
    minimum_capacity_score: float = 0.05

    allow_partial: bool = True
    deterministic: bool = True
    checkpoint_enabled: bool = True

    default_lease_seconds: float = 300.0
    urgent_lease_seconds: float = 180.0
    immediate_lease_seconds: float = 120.0

    maximum_lease_renewals: int = 3

    maximum_attempts: int = 5
    maximum_recovery_attempts: int = 3

    retry_base_delay_seconds: float = 60.0
    retry_max_delay_seconds: float = 86400.0
    retry_backoff_multiplier: float = 2.0
    retry_jitter_fraction: float = 0.05

    partition_affinity_weight: float = 1.25
    shard_affinity_weight: float = 1.15
    zone_affinity_weight: float = 1.00
    region_affinity_weight: float = 0.90
    worker_health_weight: float = 1.20
    capacity_weight: float = 1.35
    priority_weight: float = 1.25
    freshness_weight: float = 0.90
    urgency_weight: float = 1.25
    confidence_weight: float = 0.75

    high_priority_threshold: float = 0.72
    urgent_priority_threshold: float = 0.90
    immediate_priority_threshold: float = 0.97

    minimum_routing_score: float = 0.10

    defer_when_no_capacity: bool = True
    allow_cross_zone_failover: bool = True
    allow_cross_region_failover: bool = True

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DistributedRecrawlOrchestrationResult:
    orchestration_id: str

    resource_id: str
    queue_entry_id: str

    state: DistributedRecrawlOrchestrationState
    decision: OrchestrationDecision

    plan: Optional[RecrawlOrchestrationPlan] = None

    accepted: bool = False
    deferred: bool = False
    rejected: bool = False
    partial: bool = False

    error: str = ""

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DistributedRecrawlOrchestrationCheckpoint:
    checkpoint_id: str

    orchestration_id: str
    resource_id: str

    checkpoint_type: OrchestrationCheckpointType

    state: DistributedRecrawlOrchestrationState

    created_at: str

    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DistributedRecrawlOrchestrationEvent:
    event_id: str

    orchestration_id: str
    resource_id: str

    event_type: OrchestrationEventType

    state: DistributedRecrawlOrchestrationState

    created_at: str

    payload: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# BACKEND CONTRACT
# ============================================================================


class DistributedRecrawlOrchestrationBackend(Protocol):
    def persist_event(
        self,
        event: DistributedRecrawlOrchestrationEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: DistributedRecrawlOrchestrationCheckpoint,
    ) -> None:
        ...

    def persist_plan(
        self,
        plan: RecrawlOrchestrationPlan,
    ) -> None:
        ...

    def persist_result(
        self,
        result: DistributedRecrawlOrchestrationResult,
    ) -> None:
        ...

    def get_result(
        self,
        orchestration_id: str,
    ) -> Optional[DistributedRecrawlOrchestrationResult]:
        ...

    def get_plan(
        self,
        orchestration_id: str,
    ) -> Optional[RecrawlOrchestrationPlan]:
        ...

    def get_task(
        self,
        task_id: str,
    ) -> Optional[RecrawlExecutionTask]:
        ...

    def put_task(
        self,
        task: RecrawlExecutionTask,
    ) -> None:
        ...


class InMemoryDistributedRecrawlOrchestrationMetadata:
    """
    Reference metadata backend.

    Production deployments should replace this with durable distributed
    metadata storage without changing orchestration contracts.
    """

    def __init__(self) -> None:
        self._events: List[DistributedRecrawlOrchestrationEvent] = []
        self._checkpoints: List[
            DistributedRecrawlOrchestrationCheckpoint
        ] = []

        self._plans: Dict[str, RecrawlOrchestrationPlan] = {}
        self._results: Dict[
            str,
            DistributedRecrawlOrchestrationResult,
        ] = {}

        self._tasks: Dict[str, RecrawlExecutionTask] = {}

    def persist_event(
        self,
        event: DistributedRecrawlOrchestrationEvent,
    ) -> None:
        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: DistributedRecrawlOrchestrationCheckpoint,
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_plan(
        self,
        plan: RecrawlOrchestrationPlan,
    ) -> None:
        self._plans[plan.orchestration_id] = plan

        for task in plan.tasks:
            self._tasks[task.task_id] = task

    def persist_result(
        self,
        result: DistributedRecrawlOrchestrationResult,
    ) -> None:
        self._results[result.orchestration_id] = result

    def get_result(
        self,
        orchestration_id: str,
    ) -> Optional[DistributedRecrawlOrchestrationResult]:
        return self._results.get(orchestration_id)

    def get_plan(
        self,
        orchestration_id: str,
    ) -> Optional[RecrawlOrchestrationPlan]:
        return self._plans.get(orchestration_id)

    def get_task(
        self,
        task_id: str,
    ) -> Optional[RecrawlExecutionTask]:
        return self._tasks.get(task_id)

    def put_task(
        self,
        task: RecrawlExecutionTask,
    ) -> None:
        self._tasks[task.task_id] = task

    def events(
        self,
    ) -> List[DistributedRecrawlOrchestrationEvent]:
        return list(self._events)

    def checkpoints(
        self,
    ) -> List[DistributedRecrawlOrchestrationCheckpoint]:
        return list(self._checkpoints)

    def plans(
        self,
    ) -> List[RecrawlOrchestrationPlan]:
        return list(self._plans.values())

    def results(
        self,
    ) -> List[DistributedRecrawlOrchestrationResult]:
        return list(self._results.values())

    def tasks(
        self,
    ) -> List[RecrawlExecutionTask]:
        return list(self._tasks.values())


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class DistributedRecrawlOrchestrationArchitecture:
    """
    Production-oriented distributed recrawl orchestration architecture.

    The class plans distributed recrawl execution but intentionally does not
    execute crawler workers or perform network requests.
    """

    def __init__(
        self,
        backend: Optional[
            DistributedRecrawlOrchestrationBackend
        ] = None,
        policy: Optional[
            DistributedRecrawlOrchestrationPolicy
        ] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryDistributedRecrawlOrchestrationMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else DistributedRecrawlOrchestrationPolicy()
        )

    # ----------------------------------------------------------------------
    # BASIC UTILITIES
    # ----------------------------------------------------------------------

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _clamp(
        value: float,
        minimum: float = 0.0,
        maximum: float = 1.0,
    ) -> float:
        try:
            value = float(value)
        except (TypeError, ValueError):
            value = minimum

        if math.isnan(value) or math.isinf(value):
            value = minimum

        return max(minimum, min(maximum, value))

    @staticmethod
    def _safe_float(
        value: Any,
        default: float = 0.0,
    ) -> float:
        try:
            number = float(value)

            if math.isnan(number) or math.isinf(number):
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
        value: Any,
    ) -> Optional[datetime]:
        if value is None:
            return None

        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)

            return value.astimezone(timezone.utc)

        if isinstance(value, str):
            raw = value.strip()

            if not raw:
                return None

            try:
                parsed = datetime.fromisoformat(
                    raw.replace("Z", "+00:00")
                )

                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)

                return parsed.astimezone(timezone.utc)

            except ValueError:
                return None

        return None

    @staticmethod
    def _read_value(
        source: Any,
        key: str,
        default: Any = None,
    ) -> Any:
        if source is None:
            return default

        if isinstance(source, Mapping):
            return source.get(key, default)

        return getattr(source, key, default)

    @staticmethod
    def _deterministic_unit(
        *parts: Any,
    ) -> float:
        payload = "|".join(str(part) for part in parts)

        digest = hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()

        integer = int(digest[:16], 16)

        return integer / float(0xFFFFFFFFFFFFFFFF)

    @classmethod
    def _deterministic_id(
        cls,
        prefix: str,
        *parts: Any,
    ) -> str:
        payload = "|".join(str(part) for part in parts)

        digest = hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()

        return f"{prefix}-{digest[:32]}"

    # ----------------------------------------------------------------------
    # EVENTS / CHECKPOINTS
    # ----------------------------------------------------------------------

    def _event(
        self,
        orchestration_id: str,
        resource_id: str,
        event_type: OrchestrationEventType,
        state: DistributedRecrawlOrchestrationState,
        payload: Optional[Dict[str, Any]] = None,
    ) -> DistributedRecrawlOrchestrationEvent:
        event = DistributedRecrawlOrchestrationEvent(
            event_id=self._deterministic_id(
                "event",
                orchestration_id,
                resource_id,
                event_type.value,
                self._now().isoformat(),
            ),
            orchestration_id=orchestration_id,
            resource_id=resource_id,
            event_type=event_type,
            state=state,
            created_at=self._now().isoformat(),
            payload=payload or {},
        )

        self.backend.persist_event(event)

        return event

    def _checkpoint(
        self,
        orchestration_id: str,
        resource_id: str,
        checkpoint_type: OrchestrationCheckpointType,
        state: DistributedRecrawlOrchestrationState,
        payload: Optional[Dict[str, Any]] = None,
    ) -> DistributedRecrawlOrchestrationCheckpoint:
        checkpoint = DistributedRecrawlOrchestrationCheckpoint(
            checkpoint_id=self._deterministic_id(
                "checkpoint",
                orchestration_id,
                resource_id,
                checkpoint_type.value,
                self._now().isoformat(),
            ),
            orchestration_id=orchestration_id,
            resource_id=resource_id,
            checkpoint_type=checkpoint_type,
            state=state,
            created_at=self._now().isoformat(),
            payload=payload or {},
        )

        if self.policy.checkpoint_enabled:
            self.backend.persist_checkpoint(checkpoint)

            self._event(
                orchestration_id,
                resource_id,
                OrchestrationEventType.CHECKPOINT_CREATED,
                state,
                {
                    "checkpoint_type": checkpoint_type.value,
                    "checkpoint_id": checkpoint.checkpoint_id,
                },
            )

        return checkpoint

    # ----------------------------------------------------------------------
    # INPUT NORMALIZATION
    # ----------------------------------------------------------------------

    def _normalize_input(
        self,
        source: Any,
    ) -> RecrawlOrchestrationInput:
        priority_score = self._clamp(
            self._safe_float(
                self._read_value(
                    source,
                    "priority_score",
                    0.5,
                ),
                0.5,
            )
        )

        priority_band = str(
            self._read_value(
                source,
                "priority_band",
                RecrawlPriorityBand.NORMAL.value,
            )
            or RecrawlPriorityBand.NORMAL.value
        )

        execution_mode = str(
            self._read_value(
                source,
                "execution_mode",
                RecrawlExecutionMode.NORMAL.value,
            )
            or RecrawlExecutionMode.NORMAL.value
        )

        confidence = self._clamp(
            self._safe_float(
                self._read_value(
                    source,
                    "confidence",
                    1.0,
                ),
                1.0,
            )
        )

        return RecrawlOrchestrationInput(
            resource_id=str(
                self._read_value(
                    source,
                    "resource_id",
                    "",
                )
                or ""
            ),
            queue_entry_id=str(
                self._read_value(
                    source,
                    "queue_entry_id",
                    self._read_value(
                        source,
                        "entry_id",
                        "",
                    ),
                )
                or ""
            ),
            queue_version=str(
                self._read_value(
                    source,
                    "queue_version",
                    "",
                )
                or ""
            ),
            resource_type=str(
                self._read_value(
                    source,
                    "resource_type",
                    "url",
                )
                or "url"
            ),
            canonical_url=str(
                self._read_value(
                    source,
                    "canonical_url",
                    "",
                )
                or ""
            ),
            document_id=str(
                self._read_value(
                    source,
                    "document_id",
                    "",
                )
                or ""
            ),
            host_id=str(
                self._read_value(
                    source,
                    "host_id",
                    "",
                )
                or ""
            ),
            domain_id=str(
                self._read_value(
                    source,
                    "domain_id",
                    "",
                )
                or ""
            ),
            partition_id=str(
                self._read_value(
                    source,
                    "partition_id",
                    "",
                )
                or ""
            ),
            shard_id=str(
                self._read_value(
                    source,
                    "shard_id",
                    "",
                )
                or ""
            ),
            scheduled_at=self._read_value(
                source,
                "scheduled_at",
                None,
            ),
            deadline_at=self._read_value(
                source,
                "deadline_at",
                None,
            ),
            priority_score=priority_score,
            priority_band=priority_band,
            queue_lane=str(
                self._read_value(
                    source,
                    "queue_lane",
                    "normal",
                )
                or "normal"
            ),
            queue_state=str(
                self._read_value(
                    source,
                    "queue_state",
                    "ready",
                )
                or "ready"
            ),
            execution_mode=execution_mode,
            adaptive_interval_seconds=max(
                1.0,
                self._safe_float(
                    self._read_value(
                        source,
                        "adaptive_interval_seconds",
                        86400.0,
                    ),
                    86400.0,
                ),
            ),
            freshness_score=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "freshness_score",
                        0.0,
                    ),
                    0.0,
                )
            ),
            urgency_score=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "urgency_score",
                        0.0,
                    ),
                    0.0,
                )
            ),
            change_frequency=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "change_frequency",
                        0.0,
                    ),
                    0.0,
                )
            ),
            change_volatility=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "change_volatility",
                        0.0,
                    ),
                    0.0,
                )
            ),
            confidence=confidence,
            partial=bool(
                self._read_value(
                    source,
                    "partial",
                    False,
                )
            ),
            preferred_region=str(
                self._read_value(
                    source,
                    "preferred_region",
                    self._read_value(
                        source,
                        "region_id",
                        "",
                    ),
                )
                or ""
            ),
            preferred_zone=str(
                self._read_value(
                    source,
                    "preferred_zone",
                    self._read_value(
                        source,
                        "zone_id",
                        "",
                    ),
                )
                or ""
            ),
            source_metadata=dict(
                self._read_value(
                    source,
                    "source_metadata",
                    {},
                )
                or {}
            ),
        )

    # ----------------------------------------------------------------------
    # VALIDATION
    # ----------------------------------------------------------------------

    def _validate_input(
        self,
        data: RecrawlOrchestrationInput,
    ) -> Tuple[bool, str]:
        if not data.resource_id:
            return False, "resource_id is required"

        if data.confidence < self.policy.minimum_confidence:
            if not self.policy.allow_partial:
                return False, "confidence below minimum threshold"

        if not data.queue_entry_id:
            return False, "queue_entry_id is required"

        if data.queue_state not in {
            "ready",
            "pending",
            "leased",
            "retry_wait",
            "deferred",
        }:
            return False, "queue entry is not orchestration eligible"

        return True, ""

    # ----------------------------------------------------------------------
    # PRIORITY / EXECUTION MODE
    # ----------------------------------------------------------------------

    def _priority_band(
        self,
        score: float,
    ) -> RecrawlPriorityBand:
        score = self._clamp(score)

        if score >= self.policy.immediate_priority_threshold:
            return RecrawlPriorityBand.IMMEDIATE

        if score >= self.policy.urgent_priority_threshold:
            return RecrawlPriorityBand.URGENT

        if score >= self.policy.high_priority_threshold:
            return RecrawlPriorityBand.HIGH

        if score >= 0.50:
            return RecrawlPriorityBand.ELEVATED

        if score >= 0.25:
            return RecrawlPriorityBand.NORMAL

        return RecrawlPriorityBand.BACKGROUND

    def _execution_mode(
        self,
        data: RecrawlOrchestrationInput,
    ) -> RecrawlExecutionMode:
        score = data.priority_score

        if data.execution_mode in {
            mode.value for mode in RecrawlExecutionMode
        }:
            try:
                requested = RecrawlExecutionMode(
                    data.execution_mode
                )
            except ValueError:
                requested = RecrawlExecutionMode.NORMAL

            if requested in {
                RecrawlExecutionMode.RECOVERY,
                RecrawlExecutionMode.RETRY,
            }:
                return requested

        if score >= self.policy.immediate_priority_threshold:
            return RecrawlExecutionMode.IMMEDIATE

        if score >= self.policy.urgent_priority_threshold:
            return RecrawlExecutionMode.URGENT

        if score >= self.policy.high_priority_threshold:
            return RecrawlExecutionMode.HIGH_PRIORITY

        if data.change_volatility >= 0.85:
            return RecrawlExecutionMode.ACCELERATED

        return RecrawlExecutionMode.NORMAL

    # ----------------------------------------------------------------------
    # ROUTING
    # ----------------------------------------------------------------------

    def _partition_route_score(
        self,
        data: RecrawlOrchestrationInput,
        target: RecrawlExecutionTarget,
    ) -> float:
        affinity = 0.0

        if data.partition_id and (
            target.target_id == data.partition_id
        ):
            affinity = 1.0

        score = (
            affinity * self.policy.partition_affinity_weight
            + data.priority_score * self.policy.priority_weight
            + data.freshness_score * self.policy.freshness_weight
            + data.urgency_score * self.policy.urgency_weight
            + data.confidence * self.policy.confidence_weight
        )

        return score

    def _target_route_score(
        self,
        data: RecrawlOrchestrationInput,
        target: RecrawlExecutionTarget,
    ) -> float:
        partition_affinity = 0.0
        shard_affinity = 0.0
        zone_affinity = 0.0
        region_affinity = 0.0

        if data.partition_id and (
            target.target_id == data.partition_id
            or target.metadata.get("partition_id") == data.partition_id
        ):
            partition_affinity = 1.0

        if data.shard_id and (
            target.target_id == data.shard_id
            or target.metadata.get("shard_id") == data.shard_id
        ):
            shard_affinity = 1.0

        if data.preferred_zone and (
            target.zone_id == data.preferred_zone
        ):
            zone_affinity = 1.0

        if data.preferred_region and (
            target.region_id == data.preferred_region
        ):
            region_affinity = 1.0

        score = (
            partition_affinity
            * self.policy.partition_affinity_weight
            + shard_affinity
            * self.policy.shard_affinity_weight
            + zone_affinity
            * self.policy.zone_affinity_weight
            + region_affinity
            * self.policy.region_affinity_weight
            + target.health_score
            * self.policy.worker_health_weight
            + target.capacity_score
            * self.policy.capacity_weight
            + data.priority_score
            * self.policy.priority_weight
            + data.urgency_score
            * self.policy.urgency_weight
            + data.confidence
            * self.policy.confidence_weight
        )

        return score

    def _select_target(
        self,
        data: RecrawlOrchestrationInput,
        targets: Sequence[RecrawlExecutionTarget],
    ) -> Optional[RecrawlExecutionTarget]:
        eligible = [
            target
            for target in targets
            if target.accepting_work
            and target.health_score >= self.policy.minimum_worker_health
            and target.capacity_score >= self.policy.minimum_capacity_score
        ]

        if not eligible:
            return None

        scored = [
            (
                self._target_route_score(
                    data,
                    target,
                ),
                target,
            )
            for target in eligible
        ]

        scored.sort(
            key=lambda item: (
                -item[0],
                item[1].target_id,
            )
        )

        return scored[0][1]

    # ----------------------------------------------------------------------
    # CAPACITY
    # ----------------------------------------------------------------------

    def _capacity_score(
        self,
        capacity: RecrawlWorkerCapacity,
    ) -> float:
        health = self._clamp(
            capacity.health_score
        )

        explicit_capacity = self._clamp(
            capacity.capacity_score
        )

        if capacity.maximum_slots > 0:
            slot_ratio = (
                max(0, capacity.available_slots)
                / float(capacity.maximum_slots)
            )
        elif capacity.maximum_workers > 0:
            slot_ratio = (
                max(0, capacity.available_workers)
                / float(capacity.maximum_workers)
            )
        else:
            slot_ratio = explicit_capacity

        return self._clamp(
            (
                health * 0.40
                + explicit_capacity * 0.30
                + self._clamp(slot_ratio) * 0.30
            )
        )

    def _capacity_available(
        self,
        capacity: RecrawlWorkerCapacity,
    ) -> bool:
        if not capacity.accepting_work:
            return False

        if capacity.health_score < self.policy.minimum_worker_health:
            return False

        if self._capacity_score(capacity) < (
            self.policy.minimum_capacity_score
        ):
            return False

        if (
            capacity.maximum_slots > 0
            and capacity.available_slots <= 0
        ):
            return False

        if (
            capacity.maximum_slots <= 0
            and capacity.maximum_workers > 0
            and capacity.available_workers <= 0
        ):
            return False

        return True

    # ----------------------------------------------------------------------
    # REASONS
    # ----------------------------------------------------------------------

    def _reasons(
        self,
        data: RecrawlOrchestrationInput,
        partial: bool,
    ) -> List[OrchestrationReason]:
        reasons: List[OrchestrationReason] = []

        reasons.append(
            OrchestrationReason.DUE_RECRAWL
        )

        if data.freshness_score >= 0.70:
            reasons.append(
                OrchestrationReason.RECENT_CHANGE
            )

        if data.change_volatility >= 0.70:
            reasons.append(
                OrchestrationReason.HIGH_VOLATILITY
            )

        if data.priority_score >= (
            self.policy.high_priority_threshold
        ):
            reasons.append(
                OrchestrationReason.HIGH_PRIORITY
            )

        if data.priority_score >= (
            self.policy.urgent_priority_threshold
        ):
            reasons.append(
                OrchestrationReason.URGENT_PRIORITY
            )

        if data.urgency_score >= 0.85:
            reasons.append(
                OrchestrationReason.MISSED_DEADLINE
            )

        if data.preferred_region:
            reasons.append(
                OrchestrationReason.REGIONAL_AFFINITY
            )

        if data.preferred_zone:
            reasons.append(
                OrchestrationReason.ZONE_AFFINITY
            )

        if data.partition_id:
            reasons.append(
                OrchestrationReason.PARTITION_AFFINITY
            )

        if partial:
            reasons.append(
                OrchestrationReason.PARTIAL_CAPACITY
            )

        return list(dict.fromkeys(reasons))

    # ----------------------------------------------------------------------
    # LEASES
    # ----------------------------------------------------------------------

    def _lease_seconds(
        self,
        mode: RecrawlExecutionMode,
    ) -> float:
        if mode == RecrawlExecutionMode.IMMEDIATE:
            return self.policy.immediate_lease_seconds

        if mode == RecrawlExecutionMode.URGENT:
            return self.policy.urgent_lease_seconds

        return self.policy.default_lease_seconds

    def _build_lease(
        self,
        data: RecrawlOrchestrationInput,
        task_id: str,
        target: RecrawlExecutionTarget,
        mode: RecrawlExecutionMode,
    ) -> RecrawlLease:
        now = self._now()

        lease_seconds = self._lease_seconds(mode)

        lease_until = now + timedelta(
            seconds=lease_seconds
        )

        return RecrawlLease(
            lease_id=self._deterministic_id(
                "lease",
                data.resource_id,
                data.queue_entry_id,
                task_id,
                target.worker_pool_id,
            ),
            resource_id=data.resource_id,
            queue_entry_id=data.queue_entry_id,
            state=LeaseState.AVAILABLE,
            worker_pool_id=target.worker_pool_id,
            region_id=target.region_id,
            zone_id=target.zone_id,
            created_at=now.isoformat(),
            lease_started_at="",
            lease_until=lease_until.isoformat(),
            lease_seconds=lease_seconds,
            renewal_count=0,
            maximum_renewals=self.policy.maximum_lease_renewals,
        )

    # ----------------------------------------------------------------------
    # RETRY
    # ----------------------------------------------------------------------

    def _retry_delay(
        self,
        data: RecrawlOrchestrationInput,
        attempt_number: int,
    ) -> float:
        attempt_number = max(0, attempt_number)

        base = self.policy.retry_base_delay_seconds

        delay = (
            base
            * (
                self.policy.retry_backoff_multiplier
                ** attempt_number
            )
        )

        delay = min(
            delay,
            self.policy.retry_max_delay_seconds,
        )

        if self.policy.deterministic:
            jitter = (
                self._deterministic_unit(
                    data.resource_id,
                    data.queue_entry_id,
                    attempt_number,
                    ARCHITECTURE_VERSION,
                )
                - 0.5
            )

            delay *= (
                1.0
                + jitter * self.policy.retry_jitter_fraction
            )

        return max(
            1.0,
            min(
                delay,
                self.policy.retry_max_delay_seconds,
            ),
        )

    def _build_retry_plan(
        self,
        data: RecrawlOrchestrationInput,
        attempt_number: int,
        reason: RetryReason,
    ) -> RecrawlRetryPlan:
        delay = self._retry_delay(
            data,
            attempt_number,
        )

        next_retry = self._now() + timedelta(
            seconds=delay
        )

        eligible = (
            attempt_number
            < self.policy.maximum_attempts
        )

        return RecrawlRetryPlan(
            resource_id=data.resource_id,
            queue_entry_id=data.queue_entry_id,
            attempt_number=attempt_number,
            maximum_attempts=self.policy.maximum_attempts,
            retry_reason=reason,
            retry_delay_seconds=delay,
            next_retry_at=next_retry.isoformat(),
            backoff_multiplier=self.policy.retry_backoff_multiplier,
            jitter_fraction=self.policy.retry_jitter_fraction,
            eligible=eligible,
            partial=False,
        )

    # ----------------------------------------------------------------------
    # RECOVERY
    # ----------------------------------------------------------------------

    def _build_recovery_plan(
        self,
        data: RecrawlOrchestrationInput,
        failed_target: Optional[RecrawlExecutionTarget],
        recovery_target: Optional[RecrawlExecutionTarget],
        recovery_attempt: int,
        reason: RetryReason,
    ) -> RecrawlRecoveryPlan:
        return RecrawlRecoveryPlan(
            resource_id=data.resource_id,
            queue_entry_id=data.queue_entry_id,
            failed_region_id=(
                failed_target.region_id
                if failed_target
                else ""
            ),
            failed_zone_id=(
                failed_target.zone_id
                if failed_target
                else ""
            ),
            failed_worker_pool_id=(
                failed_target.worker_pool_id
                if failed_target
                else ""
            ),
            recovery_region_id=(
                recovery_target.region_id
                if recovery_target
                else ""
            ),
            recovery_zone_id=(
                recovery_target.zone_id
                if recovery_target
                else ""
            ),
            recovery_worker_pool_id=(
                recovery_target.worker_pool_id
                if recovery_target
                else ""
            ),
            recovery_attempt=recovery_attempt,
            maximum_recovery_attempts=(
                self.policy.maximum_recovery_attempts
            ),
            reason=reason,
            cross_zone_allowed=(
                self.policy.allow_cross_zone_failover
            ),
            cross_region_allowed=(
                self.policy.allow_cross_region_failover
            ),
            eligible=(
                recovery_target is not None
                and recovery_attempt
                <= self.policy.maximum_recovery_attempts
            ),
            partial=recovery_target is None,
        )

    # ----------------------------------------------------------------------
    # TASK CREATION
    # ----------------------------------------------------------------------

    def _build_task(
        self,
        data: RecrawlOrchestrationInput,
        target: RecrawlExecutionTarget,
        mode: RecrawlExecutionMode,
        lineage: DistributedRecrawlOrchestrationLineage,
        partial: bool,
    ) -> RecrawlExecutionTask:
        task_id = self._deterministic_id(
            "recrawl-task",
            data.resource_id,
            data.queue_entry_id,
            target.worker_pool_id,
            target.region_id,
            target.zone_id,
            ARCHITECTURE_VERSION,
        )

        now = self._now().isoformat()

        lease = self._build_lease(
            data,
            task_id,
            target,
            mode,
        )

        reasons = self._reasons(
            data,
            partial,
        )

        return RecrawlExecutionTask(
            task_id=task_id,
            resource_id=data.resource_id,
            queue_entry_id=data.queue_entry_id,
            resource_type=data.resource_type,
            canonical_url=data.canonical_url,
            document_id=data.document_id,
            partition_id=data.partition_id,
            shard_id=data.shard_id,
            region_id=target.region_id,
            zone_id=target.zone_id,
            worker_pool_id=target.worker_pool_id,
            state=RecrawlTaskState.READY,
            execution_mode=mode,
            priority_score=data.priority_score,
            priority_band=self._priority_band(
                data.priority_score
            ),
            scheduled_at=(
                str(data.scheduled_at)
                if data.scheduled_at is not None
                else now
            ),
            deadline_at=(
                str(data.deadline_at)
                if data.deadline_at is not None
                else ""
            ),
            assigned_at=now,
            started_at="",
            completed_at="",
            attempt_number=0,
            maximum_attempts=self.policy.maximum_attempts,
            lease=lease,
            retry_plan=None,
            recovery_plan=None,
            reasons=reasons,
            lineage=lineage,
            partial=partial,
            metadata={
                "target_routing_score": target.routing_score,
                "target_capacity_score": target.capacity_score,
            },
        )

    # ----------------------------------------------------------------------
    # DUPLICATION / IDEMPOTENCY
    # ----------------------------------------------------------------------

    def _existing_task(
        self,
        data: RecrawlOrchestrationInput,
    ) -> Optional[RecrawlExecutionTask]:
        task_id = self._deterministic_id(
            "recrawl-task",
            data.resource_id,
            data.queue_entry_id,
            "",
            "",
            "",
            ARCHITECTURE_VERSION,
        )

        task = self.backend.get_task(task_id)

        if task is not None:
            return task

        return None

    # ----------------------------------------------------------------------
    # MAIN ORCHESTRATION
    # ----------------------------------------------------------------------

    def orchestrate(
        self,
        source: Any,
        targets: Optional[
            Sequence[RecrawlExecutionTarget]
        ] = None,
        capacities: Optional[
            Sequence[RecrawlWorkerCapacity]
        ] = None,
    ) -> DistributedRecrawlOrchestrationResult:
        data = self._normalize_input(source)

        orchestration_id = self._deterministic_id(
            "orchestration",
            data.resource_id,
            data.queue_entry_id,
            ARCHITECTURE_VERSION,
        )

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.REQUEST_RECEIVED,
            DistributedRecrawlOrchestrationState.RECEIVED,
            {
                "queue_entry_id": data.queue_entry_id,
            },
        )

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.VALIDATION_STARTED,
            DistributedRecrawlOrchestrationState.VALIDATING,
        )

        valid, error = self._validate_input(data)

        if not valid:
            result = DistributedRecrawlOrchestrationResult(
                orchestration_id=orchestration_id,
                resource_id=data.resource_id,
                queue_entry_id=data.queue_entry_id,
                state=(
                    DistributedRecrawlOrchestrationState.REJECTED
                ),
                decision=OrchestrationDecision.REJECT,
                accepted=False,
                rejected=True,
                partial=False,
                error=error,
            )

            self._event(
                orchestration_id,
                data.resource_id,
                OrchestrationEventType.TASK_REJECTED,
                DistributedRecrawlOrchestrationState.REJECTED,
                {
                    "error": error,
                },
            )

            self.backend.persist_result(result)

            return result

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.INPUT_NORMALIZED,
            DistributedRecrawlOrchestrationState.NORMALIZING,
            {
                "resource_type": data.resource_type,
                "priority_score": data.priority_score,
                "confidence": data.confidence,
            },
        )

        self._checkpoint(
            orchestration_id,
            data.resource_id,
            OrchestrationCheckpointType.INPUT_NORMALIZED,
            DistributedRecrawlOrchestrationState.NORMALIZING,
        )

        mode = self._execution_mode(data)

        if targets is None:
            targets = []

        if capacities is None:
            capacities = []

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.PARTITION_ROUTING_STARTED,
            DistributedRecrawlOrchestrationState.PARTITION_ROUTING,
        )

        selected_target = self._select_target(
            data,
            targets,
        )

        self._checkpoint(
            orchestration_id,
            data.resource_id,
            OrchestrationCheckpointType.PARTITION_ROUTED,
            DistributedRecrawlOrchestrationState.PARTITION_ROUTING,
            {
                "selected_target": (
                    selected_target.target_id
                    if selected_target
                    else ""
                ),
            },
        )

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.REGION_ROUTING_STARTED,
            DistributedRecrawlOrchestrationState.REGION_ROUTING,
        )

        capacity_by_pool = {
            capacity.worker_pool_id: capacity
            for capacity in capacities
        }

        if selected_target is None:
            result = DistributedRecrawlOrchestrationResult(
                orchestration_id=orchestration_id,
                resource_id=data.resource_id,
                queue_entry_id=data.queue_entry_id,
                state=(
                    DistributedRecrawlOrchestrationState.DEFERRED
                ),
                decision=OrchestrationDecision.DEFER,
                accepted=False,
                deferred=True,
                rejected=False,
                partial=data.partial,
                error="no eligible distributed execution target",
            )

            self._event(
                orchestration_id,
                data.resource_id,
                OrchestrationEventType.TASK_DEFERRED,
                DistributedRecrawlOrchestrationState.DEFERRED,
                {
                    "reason": "no_eligible_target",
                },
            )

            self.backend.persist_result(result)

            return result

        selected_capacity = capacity_by_pool.get(
            selected_target.worker_pool_id
        )

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.ZONE_ROUTING_STARTED,
            DistributedRecrawlOrchestrationState.ZONE_ROUTING,
            {
                "region_id": selected_target.region_id,
                "zone_id": selected_target.zone_id,
            },
        )

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.CAPACITY_ANALYSIS_STARTED,
            DistributedRecrawlOrchestrationState.CAPACITY_ANALYSIS,
        )

        if selected_capacity is not None:
            available = self._capacity_available(
                selected_capacity
            )

            capacity_score = self._capacity_score(
                selected_capacity
            )
        else:
            available = selected_target.accepting_work
            capacity_score = selected_target.capacity_score

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.CAPACITY_ANALYZED,
            DistributedRecrawlOrchestrationState.CAPACITY_ANALYSIS,
            {
                "available": available,
                "capacity_score": capacity_score,
            },
        )

        self._checkpoint(
            orchestration_id,
            data.resource_id,
            OrchestrationCheckpointType.CAPACITY_ANALYZED,
            DistributedRecrawlOrchestrationState.CAPACITY_ANALYSIS,
            {
                "capacity_score": capacity_score,
            },
        )

        if (
            not available
            or capacity_score
            < self.policy.minimum_capacity_score
        ):
            if self.policy.defer_when_no_capacity:
                result = DistributedRecrawlOrchestrationResult(
                    orchestration_id=orchestration_id,
                    resource_id=data.resource_id,
                    queue_entry_id=data.queue_entry_id,
                    state=(
                        DistributedRecrawlOrchestrationState.DEFERRED
                    ),
                    decision=OrchestrationDecision.DEFER,
                    accepted=False,
                    deferred=True,
                    rejected=False,
                    partial=True,
                    error="insufficient execution capacity",
                )

                self._event(
                    orchestration_id,
                    data.resource_id,
                    OrchestrationEventType.TASK_DEFERRED,
                    DistributedRecrawlOrchestrationState.DEFERRED,
                    {
                        "capacity_score": capacity_score,
                    },
                )

                self.backend.persist_result(result)

                return result

        lineage = DistributedRecrawlOrchestrationLineage(
            resource_id=data.resource_id,
            previous_stage=PREVIOUS_STAGE,
            current_stage=PHASE,
            source_queue_entry_id=data.queue_entry_id,
            source_queue_version=data.queue_version,
            source_schedule_id=str(
                data.source_metadata.get(
                    "schedule_id",
                    "",
                )
            ),
            source_frequency_version=str(
                data.source_metadata.get(
                    "frequency_version",
                    "",
                )
            ),
            parent_orchestration_ids=[],
            lineage_metadata={
                "architecture_version": ARCHITECTURE_VERSION,
            },
        )

        partial = (
            data.partial
            or selected_capacity is None
        )

        task = self._build_task(
            data,
            selected_target,
            mode,
            lineage,
            partial,
        )

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.EXECUTION_PLAN_CREATED,
            DistributedRecrawlOrchestrationState.EXECUTION_PLANNING,
            {
                "task_id": task.task_id,
                "worker_pool_id": task.worker_pool_id,
            },
        )

        self._checkpoint(
            orchestration_id,
            data.resource_id,
            OrchestrationCheckpointType.EXECUTION_PLANNED,
            DistributedRecrawlOrchestrationState.EXECUTION_PLANNING,
            {
                "task_id": task.task_id,
            },
        )

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.LEASE_PLAN_CREATED,
            DistributedRecrawlOrchestrationState.LEASE_PLANNING,
            {
                "lease_id": (
                    task.lease.lease_id
                    if task.lease
                    else ""
                ),
            },
        )

        self._checkpoint(
            orchestration_id,
            data.resource_id,
            OrchestrationCheckpointType.LEASE_PLANNED,
            DistributedRecrawlOrchestrationState.LEASE_PLANNING,
        )

        plan = RecrawlOrchestrationPlan(
            orchestration_id=orchestration_id,
            resource_id=data.resource_id,
            queue_entry_id=data.queue_entry_id,
            state=(
                DistributedRecrawlOrchestrationState.COMPLETED
            ),
            decision=(
                OrchestrationDecision.PARTIAL_ASSIGN
                if partial
                else OrchestrationDecision.ASSIGN
            ),
            execution_mode=mode,
            selected_partition_id=data.partition_id,
            selected_shard_id=data.shard_id,
            selected_region_id=selected_target.region_id,
            selected_zone_id=selected_target.zone_id,
            selected_worker_pool_id=(
                selected_target.worker_pool_id
            ),
            routing_score=selected_target.routing_score,
            capacity_score=capacity_score,
            tasks=[task],
            reasons=self._reasons(
                data,
                partial,
            ),
            partial=partial,
            lineage=lineage,
            created_at=self._now().isoformat(),
            updated_at=self._now().isoformat(),
            metadata={
                "target_type": selected_target.target_type.value,
            },
        )

        self.backend.persist_plan(plan)

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.TASK_ASSIGNED,
            DistributedRecrawlOrchestrationState.ASSIGNMENT,
            {
                "task_id": task.task_id,
                "worker_pool_id": task.worker_pool_id,
            },
        )

        self._checkpoint(
            orchestration_id,
            data.resource_id,
            OrchestrationCheckpointType.TASK_ASSIGNED,
            DistributedRecrawlOrchestrationState.ASSIGNMENT,
            {
                "task_id": task.task_id,
            },
        )

        result = DistributedRecrawlOrchestrationResult(
            orchestration_id=orchestration_id,
            resource_id=data.resource_id,
            queue_entry_id=data.queue_entry_id,
            state=(
                DistributedRecrawlOrchestrationState.PARTIAL
                if partial
                else DistributedRecrawlOrchestrationState.COMPLETED
            ),
            decision=(
                OrchestrationDecision.PARTIAL_ASSIGN
                if partial
                else OrchestrationDecision.ASSIGN
            ),
            plan=plan,
            accepted=True,
            deferred=False,
            rejected=False,
            partial=partial,
        )

        self._event(
            orchestration_id,
            data.resource_id,
            OrchestrationEventType.ORCHESTRATION_COMPLETED,
            result.state,
            {
                "decision": result.decision.value,
                "task_count": len(plan.tasks),
                "partial": partial,
            },
        )

        self._checkpoint(
            orchestration_id,
            data.resource_id,
            OrchestrationCheckpointType.COMPLETED,
            result.state,
            {
                "decision": result.decision.value,
            },
        )

        self.backend.persist_result(result)

        return result

    # ----------------------------------------------------------------------
    # BATCH ORCHESTRATION
    # ----------------------------------------------------------------------

    def orchestrate_many(
        self,
        sources: Iterable[Any],
        targets: Optional[
            Sequence[RecrawlExecutionTarget]
        ] = None,
        capacities: Optional[
            Sequence[RecrawlWorkerCapacity]
        ] = None,
    ) -> List[DistributedRecrawlOrchestrationResult]:
        results: List[
            DistributedRecrawlOrchestrationResult
        ] = []

        count = 0

        for source in sources:
            if count >= self.policy.max_resources_per_batch:
                break

            results.append(
                self.orchestrate(
                    source,
                    targets=targets,
                    capacities=capacities,
                )
            )

            count += 1

        return results

    # ----------------------------------------------------------------------
    # TASK STATE TRANSITIONS
    # ----------------------------------------------------------------------

    def lease_task(
        self,
        task_id: str,
    ) -> Optional[RecrawlExecutionTask]:
        task = self.backend.get_task(task_id)

        if task is None:
            return None

        if task.lease is None:
            return task

        now = self._now()

        task.lease.state = LeaseState.LEASED
        task.lease.lease_started_at = now.isoformat()

        task.state = RecrawlTaskState.LEASED

        self.backend.put_task(task)

        return task

    def start_task(
        self,
        task_id: str,
    ) -> Optional[RecrawlExecutionTask]:
        task = self.backend.get_task(task_id)

        if task is None:
            return None

        task.state = RecrawlTaskState.RUNNING
        task.started_at = self._now().isoformat()

        self.backend.put_task(task)

        return task

    def complete_task(
        self,
        task_id: str,
    ) -> Optional[RecrawlExecutionTask]:
        task = self.backend.get_task(task_id)

        if task is None:
            return None

        task.state = RecrawlTaskState.COMPLETED
        task.completed_at = self._now().isoformat()

        if task.lease is not None:
            task.lease.state = LeaseState.RELEASED

        self.backend.put_task(task)

        return task

    def defer_task(
        self,
        task_id: str,
    ) -> Optional[RecrawlExecutionTask]:
        task = self.backend.get_task(task_id)

        if task is None:
            return None

        task.state = RecrawlTaskState.DEFERRED

        if task.lease is not None:
            task.lease.state = LeaseState.RELEASED

        self.backend.put_task(task)

        return task

    # ----------------------------------------------------------------------
    # FAILURE / RETRY
    # ----------------------------------------------------------------------

    def fail_task(
        self,
        task_id: str,
        reason: RetryReason = RetryReason.UNKNOWN_FAILURE,
    ) -> Optional[RecrawlExecutionTask]:
        task = self.backend.get_task(task_id)

        if task is None:
            return None

        next_attempt = task.attempt_number + 1

        retry_data = RecrawlOrchestrationInput(
            resource_id=task.resource_id,
            queue_entry_id=task.queue_entry_id,
            canonical_url=task.canonical_url,
            document_id=task.document_id,
            priority_score=task.priority_score,
            priority_band=task.priority_band.value,
            execution_mode=task.execution_mode.value,
            partial=task.partial,
        )

        retry_plan = self._build_retry_plan(
            retry_data,
            next_attempt,
            reason,
        )

        task.retry_plan = retry_plan
        task.attempt_number = next_attempt

        if retry_plan.eligible:
            task.state = RecrawlTaskState.RETRY_WAIT
        else:
            task.state = RecrawlTaskState.FAILED

        if task.lease is not None:
            task.lease.state = LeaseState.RELEASED

        self.backend.put_task(task)

        return task

    # ----------------------------------------------------------------------
    # RECOVERY
    # ----------------------------------------------------------------------

    def recover_task(
        self,
        task_id: str,
        targets: Sequence[RecrawlExecutionTarget],
        reason: RetryReason = RetryReason.WORKER_FAILURE,
    ) -> Optional[RecrawlExecutionTask]:
        task = self.backend.get_task(task_id)

        if task is None:
            return None

        failed_target = RecrawlExecutionTarget(
            target_type=ExecutionTargetType.WORKER_POOL,
            target_id=task.worker_pool_id,
            region_id=task.region_id,
            zone_id=task.zone_id,
            worker_pool_id=task.worker_pool_id,
            health_score=0.0,
            capacity_score=0.0,
            accepting_work=False,
        )

        data = RecrawlOrchestrationInput(
            resource_id=task.resource_id,
            queue_entry_id=task.queue_entry_id,
            canonical_url=task.canonical_url,
            document_id=task.document_id,
            partition_id=task.partition_id,
            shard_id=task.shard_id,
            priority_score=task.priority_score,
            priority_band=task.priority_band.value,
            execution_mode=RecrawlExecutionMode.RECOVERY.value,
            partial=task.partial,
        )

        candidates = [
            target
            for target in targets
            if target.worker_pool_id
            != task.worker_pool_id
            and target.accepting_work
            and target.health_score
            >= self.policy.minimum_worker_health
        ]

        recovery_target = self._select_target(
            data,
            candidates,
        )

        recovery_attempt = (
            task.recovery_plan.recovery_attempt + 1
            if task.recovery_plan is not None
            else 1
        )

        recovery_plan = self._build_recovery_plan(
            data,
            failed_target,
            recovery_target,
            recovery_attempt,
            reason,
        )

        task.recovery_plan = recovery_plan

        if recovery_target is None:
            task.state = RecrawlTaskState.DEFERRED
            task.partial = True

            self.backend.put_task(task)

            return task

        task.region_id = recovery_target.region_id
        task.zone_id = recovery_target.zone_id
        task.worker_pool_id = recovery_target.worker_pool_id

        task.execution_mode = RecrawlExecutionMode.RECOVERY

        task.lease = self._build_lease(
            data,
            task.task_id,
            recovery_target,
            RecrawlExecutionMode.RECOVERY,
        )

        task.state = RecrawlTaskState.READY
        task.partial = recovery_plan.partial

        self.backend.put_task(task)

        return task

    # ----------------------------------------------------------------------
    # LEASE RENEWAL
    # ----------------------------------------------------------------------

    def renew_lease(
        self,
        task_id: str,
    ) -> Optional[RecrawlExecutionTask]:
        task = self.backend.get_task(task_id)

        if task is None or task.lease is None:
            return task

        lease = task.lease

        if lease.renewal_count >= lease.maximum_renewals:
            return task

        now = self._now()

        lease.renewal_count += 1
        lease.state = LeaseState.LEASED

        lease.lease_started_at = now.isoformat()

        lease.lease_until = (
            now
            + timedelta(
                seconds=lease.lease_seconds
            )
        ).isoformat()

        self.backend.put_task(task)

        return task

    # ----------------------------------------------------------------------
    # LEASE EXPIRATION
    # ----------------------------------------------------------------------

    def expire_lease(
        self,
        task_id: str,
    ) -> Optional[RecrawlExecutionTask]:
        task = self.backend.get_task(task_id)

        if task is None or task.lease is None:
            return task

        lease_until = self._parse_timestamp(
            task.lease.lease_until
        )

        if lease_until is None:
            return task

        if self._now() >= lease_until:
            task.lease.state = LeaseState.EXPIRED

            retry_data = RecrawlOrchestrationInput(
                resource_id=task.resource_id,
                queue_entry_id=task.queue_entry_id,
                priority_score=task.priority_score,
                priority_band=task.priority_band.value,
                execution_mode=(
                    RecrawlExecutionMode.RETRY.value
                ),
                partial=task.partial,
            )

            task.retry_plan = self._build_retry_plan(
                retry_data,
                task.attempt_number + 1,
                RetryReason.LEASE_EXPIRATION,
            )

            if task.retry_plan.eligible:
                task.state = RecrawlTaskState.RETRY_WAIT
            else:
                task.state = RecrawlTaskState.FAILED

            self.backend.put_task(task)

        return task

    # ----------------------------------------------------------------------
    # DISTRIBUTED PLAN
    # ----------------------------------------------------------------------

    def build_distributed_plan(
        self,
        sources: Iterable[Any],
        targets: Sequence[RecrawlExecutionTarget],
        capacities: Sequence[RecrawlWorkerCapacity],
    ) -> List[DistributedRecrawlOrchestrationResult]:
        return self.orchestrate_many(
            sources,
            targets=targets,
            capacities=capacities,
        )

    # ----------------------------------------------------------------------
    # ARCHITECTURE DESCRIPTION
    # ----------------------------------------------------------------------

    def architecture(
        self,
    ) -> Dict[str, Any]:
        return {
            "phase": PHASE,
            "architecture_version": ARCHITECTURE_VERSION,
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),
            "purpose": (
                "Distributed orchestration of freshness-driven "
                "recrawl work across globally distributed "
                "execution infrastructure."
            ),
            "inputs": [
                "phase13.5_url_document_freshness_queues",
                "recrawl_schedule",
                "adaptive_recrawl_frequency",
                "freshness_signals",
                "distributed_execution_targets",
                "worker_capacity",
                "worker_health",
            ],
            "outputs": [
                "distributed_recrawl_orchestration_plan",
                "recrawl_execution_tasks",
                "partition_affinity",
                "region_affinity",
                "zone_affinity",
                "worker_pool_assignment",
                "lease_plan",
                "retry_plan",
                "recovery_plan",
                "orchestration_checkpoints",
                "orchestration_events",
            ],
            "distributed_execution": True,
            "partition_aware": True,
            "shard_aware": True,
            "region_aware": True,
            "zone_aware": True,
            "worker_pool_aware": True,
            "capacity_aware": True,
            "health_aware": True,
            "lease_based_execution": True,
            "retry_support": True,
            "deterministic_retry_jitter": True,
            "cross_zone_failover": (
                self.policy.allow_cross_zone_failover
            ),
            "cross_region_failover": (
                self.policy.allow_cross_region_failover
            ),
            "partial_input_support": (
                self.policy.allow_partial
            ),
            "checkpointable": (
                self.policy.checkpoint_enabled
            ),
            "restartable": True,
            "deterministic": self.policy.deterministic,
            "incremental": True,
            "horizontally_scalable": True,
            "backend_replaceable": True,
            "lineage_preserving": True,
            "provenance_preserving": True,
            "idempotent_orchestration_identity": True,
            "no_fixed_global_resource_limit": True,
            "no_fixed_global_url_limit": True,
            "no_fixed_global_document_limit": True,
            "no_fixed_global_partition_limit": True,
            "no_fixed_global_shard_limit": True,
            "no_fixed_global_worker_limit": True,
            "no_fixed_global_region_limit": True,
            "no_fixed_global_zone_limit": True,
            "no_fixed_global_task_limit": True,
            "no_google_search_api": True,
            "no_google_index": True,
            "no_google_crawler": True,
            "no_google_infrastructure": True,
            "no_google_ranking_dependency": True,
            "does_not_execute_http_fetch": True,
            "does_not_parse_web_content": True,
            "does_not_mutate_search_index": True,
            "does_not_discover_new_urls": True,
            "does_not_perform_final_ranking": True,
            "does_not_classify_spam": True,
            "does_not_replace_freshness_scheduling": True,
            "does_not_replace_adaptive_frequency": True,
            "does_not_replace_freshness_queues": True,
            "does_not_replace_global_resource_allocation": True,
            "stage_boundaries": {
                "13.1": (
                    "determines freshness attention and urgency"
                ),
                "13.2": (
                    "determines content change and volatility"
                ),
                "13.3": (
                    "determines recrawl schedule timing"
                ),
                "13.4": (
                    "adapts long-term recrawl frequency"
                ),
                "13.5": (
                    "stores URL/document freshness work "
                    "in durable queues"
                ),
                "13.6": (
                    "orchestrates queued recrawl work "
                    "across distributed execution infrastructure"
                ),
                "13.7": (
                    "allocates crawl resources according "
                    "to freshness importance"
                ),
                "13.8": (
                    "coordinates global recrawl recovery "
                    "and failure handling"
                ),
                "13.9": (
                    "final freshness and recrawling architecture"
                ),
            },
            "next_stage": "13.7",
            "next_stage_name": (
                "Freshness-Aware Crawl Resource Allocation"
            ),
        }

    # ----------------------------------------------------------------------
    # METADATA ACCESS
    # ----------------------------------------------------------------------

    def events(
        self,
    ) -> List[DistributedRecrawlOrchestrationEvent]:
        if hasattr(self.backend, "events"):
            return list(
                getattr(self.backend, "events")()
            )

        return []

    def checkpoints(
        self,
    ) -> List[DistributedRecrawlOrchestrationCheckpoint]:
        if hasattr(self.backend, "checkpoints"):
            return list(
                getattr(self.backend, "checkpoints")()
            )

        return []

    def plans(
        self,
    ) -> List[RecrawlOrchestrationPlan]:
        if hasattr(self.backend, "plans"):
            return list(
                getattr(self.backend, "plans")()
            )

        return []

    def results(
        self,
    ) -> List[DistributedRecrawlOrchestrationResult]:
        if hasattr(self.backend, "results"):
            return list(
                getattr(self.backend, "results")()
            )

        return []

    def tasks(
        self,
    ) -> List[RecrawlExecutionTask]:
        if hasattr(self.backend, "tasks"):
            return list(
                getattr(self.backend, "tasks")()
            )

        return []


# ============================================================================
# GLOBAL ALIASES
# ============================================================================


DistributedRecrawlOrchestration = (
    DistributedRecrawlOrchestrationArchitecture
)

GlobalDistributedRecrawlOrchestration = (
    DistributedRecrawlOrchestrationArchitecture
)

Phase13_6DistributedRecrawlOrchestration = (
    DistributedRecrawlOrchestrationArchitecture
)

GlobalRecrawlOrchestration = (
    DistributedRecrawlOrchestrationArchitecture
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
    "DistributedRecrawlOrchestrationState",
    "RecrawlExecutionMode",
    "RecrawlPriorityBand",
    "OrchestrationDecision",
    "ExecutionTargetType",
    "RecrawlTaskState",
    "LeaseState",
    "RetryReason",
    "OrchestrationReason",
    "OrchestrationEventType",
    "OrchestrationCheckpointType",
    "DistributedRecrawlOrchestrationIdentity",
    "DistributedRecrawlOrchestrationLineage",
    "RecrawlOrchestrationInput",
    "RecrawlWorkerCapacity",
    "RecrawlExecutionTarget",
    "RecrawlLease",
    "RecrawlRetryPlan",
    "RecrawlRecoveryPlan",
    "RecrawlExecutionTask",
    "RecrawlOrchestrationPlan",
    "DistributedRecrawlOrchestrationPolicy",
    "DistributedRecrawlOrchestrationResult",
    "DistributedRecrawlOrchestrationCheckpoint",
    "DistributedRecrawlOrchestrationEvent",
    "DistributedRecrawlOrchestrationBackend",
    "InMemoryDistributedRecrawlOrchestrationMetadata",
    "DistributedRecrawlOrchestrationArchitecture",
    "DistributedRecrawlOrchestration",
    "GlobalDistributedRecrawlOrchestration",
    "Phase13_6DistributedRecrawlOrchestration",
    "GlobalRecrawlOrchestration",
]
