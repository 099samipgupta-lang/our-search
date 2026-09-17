"""
OUR SEARCH
Phase 13.7 — Freshness-Aware Crawl Resource Allocation

Purpose
-------
Allocate distributed crawler resources according to freshness importance,
recrawl urgency, change behavior, temporal sensitivity, queue pressure,
orchestration state, worker capacity, and workload characteristics.

This stage converts freshness-aware recrawl workload information into
resource-allocation decisions.

It determines:

- crawl resource demand
- freshness-weighted resource priority
- worker capacity requirements
- concurrency allocation
- resource shares
- capacity reservation
- workload admission
- capacity-aware deferral
- allocation plans across distributed partitions
- allocation checkpoints
- allocation lineage
- deterministic allocation decisions

This module is a resource-allocation architecture and decision layer.

It does NOT:
- perform HTTP requests
- fetch Web resources
- execute crawler workers
- parse Web pages
- discover new URLs
- mutate the search index
- perform final ranking
- replace recrawl scheduling
- replace adaptive recrawl frequency
- replace freshness queues
- replace distributed recrawl orchestration
- perform global failure recovery
- classify spam
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
- worker pools
- regions
- zones
- allocation plans
- workloads
- capacity units

Per-request and per-batch safety limits exist only to bound individual
processing units.

Stage boundaries
----------------
13.1 -> determines freshness attention / urgency
13.2 -> determines what changed and volatility
13.3 -> determines when a resource should be recrawled
13.4 -> adapts long-term recrawl frequency
13.5 -> stores freshness work in durable URL/document queues
13.6 -> orchestrates recrawl work across distributed infrastructure
13.7 -> allocates crawler resources according to freshness importance
13.8 -> coordinates global recrawl recovery and failure handling
13.9 -> final freshness + recrawling architecture
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
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

ARCHITECTURE_VERSION = "freshness-aware-crawl-resource-allocation.v1"
PHASE = "13.7"
PREVIOUS_STAGE = "13.6"
NEXT_STAGE = "13.8"


# ============================================================================
# ENUMS
# ============================================================================


class FreshnessResourceAllocationState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    FRESHNESS_ANALYSIS = "freshness_analysis"
    DEMAND_ESTIMATION = "demand_estimation"
    CAPACITY_ANALYSIS = "capacity_analysis"
    PRIORITY_CALCULATION = "priority_calculation"
    RESOURCE_ALLOCATION = "resource_allocation"
    ADMISSION_CONTROL = "admission_control"
    RESERVATION_PLANNING = "reservation_planning"
    DECISION = "decision"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class ResourceAllocationDecision(str, Enum):
    ALLOCATE = "allocate"
    PARTIAL_ALLOCATE = "partial_allocate"
    DEFER = "defer"
    REJECT = "reject"
    RESERVE = "reserve"


class AllocationPriorityBand(str, Enum):
    BACKGROUND = "background"
    NORMAL = "normal"
    ELEVATED = "elevated"
    HIGH = "high"
    URGENT = "urgent"
    IMMEDIATE = "immediate"


class CrawlResourceType(str, Enum):
    WORKER = "worker"
    CONCURRENCY_SLOT = "concurrency_slot"
    FETCH_SLOT = "fetch_slot"
    BANDWIDTH_UNIT = "bandwidth_unit"
    CPU_UNIT = "cpu_unit"
    MEMORY_UNIT = "memory_unit"
    REQUEST_BUDGET = "request_budget"


class AllocationScope(str, Enum):
    RESOURCE = "resource"
    HOST = "host"
    DOMAIN = "domain"
    PARTITION = "partition"
    SHARD = "shard"
    REGION = "region"
    ZONE = "zone"
    WORKER_POOL = "worker_pool"


class ReservationState(str, Enum):
    NONE = "none"
    REQUESTED = "requested"
    RESERVED = "reserved"
    PARTIAL = "partial"
    RELEASED = "released"
    EXPIRED = "expired"


class WorkloadAdmissionState(str, Enum):
    ADMITTED = "admitted"
    PARTIALLY_ADMITTED = "partially_admitted"
    DEFERRED = "deferred"
    REJECTED = "rejected"


class AllocationReason(str, Enum):
    HIGH_FRESHNESS_IMPORTANCE = "high_freshness_importance"
    HIGH_URGENCY = "high_urgency"
    RECENT_CHANGE = "recent_change"
    HIGH_CHANGE_FREQUENCY = "high_change_frequency"
    HIGH_CHANGE_VOLATILITY = "high_change_volatility"
    TEMPORAL_SENSITIVITY = "temporal_sensitivity"
    SOURCE_FRESHNESS = "source_freshness"
    FEED_ACTIVITY = "feed_activity"
    SITEMAP_ACTIVITY = "sitemap_activity"
    EXTERNAL_ACTIVITY = "external_activity"
    MISSED_DEADLINE = "missed_deadline"
    HIGH_QUEUE_PRESSURE = "high_queue_pressure"
    HIGH_PRIORITY = "high_priority"
    IMMEDIATE_PRIORITY = "immediate_priority"
    HIGH_CONFIDENCE = "high_confidence"
    CAPACITY_AVAILABLE = "capacity_available"
    CAPACITY_LIMITED = "capacity_limited"
    PARTIAL_CAPACITY = "partial_capacity"
    CAPACITY_RESERVED = "capacity_reserved"
    RESOURCE_CONSTRAINT = "resource_constraint"
    FAIR_SHARE = "fair_share"


class AllocationEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    INPUT_NORMALIZED = "input_normalized"
    FRESHNESS_ANALYSIS_STARTED = "freshness_analysis_started"
    FRESHNESS_ANALYZED = "freshness_analyzed"
    DEMAND_ESTIMATION_STARTED = "demand_estimation_started"
    DEMAND_ESTIMATED = "demand_estimated"
    CAPACITY_ANALYSIS_STARTED = "capacity_analysis_started"
    CAPACITY_ANALYZED = "capacity_analyzed"
    PRIORITY_CALCULATION_STARTED = "priority_calculation_started"
    PRIORITY_CALCULATED = "priority_calculated"
    RESOURCE_ALLOCATION_STARTED = "resource_allocation_started"
    RESOURCE_ALLOCATED = "resource_allocated"
    ADMISSION_DECIDED = "admission_decided"
    RESERVATION_CREATED = "reservation_created"
    PARTIAL_ALLOCATION = "partial_allocation"
    WORKLOAD_DEFERRED = "workload_deferred"
    WORKLOAD_REJECTED = "workload_rejected"
    CHECKPOINT_CREATED = "checkpoint_created"
    ALLOCATION_COMPLETED = "allocation_completed"
    ALLOCATION_FAILED = "allocation_failed"


class AllocationCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    FRESHNESS_ANALYZED = "freshness_analyzed"
    DEMAND_ESTIMATED = "demand_estimated"
    CAPACITY_ANALYZED = "capacity_analyzed"
    PRIORITY_CALCULATED = "priority_calculated"
    RESOURCE_ALLOCATED = "resource_allocated"
    ADMISSION_DECIDED = "admission_decided"
    RESERVATION_CREATED = "reservation_created"
    COMPLETED = "completed"


# ============================================================================
# DATACLASSES
# ============================================================================


@dataclass(frozen=True)
class FreshnessResourceAllocationIdentity:
    resource_id: str

    orchestration_id: str = ""
    queue_entry_id: str = ""

    allocation_id: str = ""
    allocation_version: str = ARCHITECTURE_VERSION

    partition_id: str = ""
    shard_id: str = ""
    host_id: str = ""
    domain_id: str = ""

    region_id: str = ""
    zone_id: str = ""
    worker_pool_id: str = ""

    def key(self) -> str:
        return (
            f"{self.resource_id}:"
            f"{self.orchestration_id}:"
            f"{self.queue_entry_id}:"
            f"{self.allocation_version}"
        )


@dataclass
class FreshnessResourceAllocationLineage:
    resource_id: str

    previous_stage: str = PREVIOUS_STAGE
    current_stage: str = PHASE

    source_orchestration_id: str = ""
    source_queue_entry_id: str = ""
    source_queue_version: str = ""

    source_schedule_id: str = ""
    source_frequency_version: str = ""

    parent_allocation_ids: List[str] = field(
        default_factory=list
    )

    lineage_metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class FreshnessResourceAllocationInput:
    resource_id: str

    orchestration_id: str = ""
    queue_entry_id: str = ""

    resource_type: str = "url"

    canonical_url: str = ""
    document_id: str = ""

    host_id: str = ""
    domain_id: str = ""

    partition_id: str = ""
    shard_id: str = ""

    region_id: str = ""
    zone_id: str = ""
    worker_pool_id: str = ""

    priority_score: float = 0.5
    priority_band: str = AllocationPriorityBand.NORMAL.value

    freshness_score: float = 0.0
    urgency_score: float = 0.0

    change_frequency: float = 0.0
    change_volatility: float = 0.0

    recent_change: float = 0.0

    semantic_change_score: float = 0.0
    temporal_sensitivity: float = 0.0
    source_freshness: float = 0.0

    feed_update_signal: float = 0.0
    sitemap_update_signal: float = 0.0
    external_update_signal: float = 0.0

    queue_pressure: float = 0.0

    deadline_pressure: float = 0.0

    confidence: float = 1.0

    orchestration_partial: bool = False
    partial: bool = False

    requested_resource_units: float = 1.0

    estimated_fetch_cost: float = 1.0
    estimated_bandwidth_cost: float = 1.0
    estimated_cpu_cost: float = 1.0
    estimated_memory_cost: float = 1.0

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class CrawlResourceCapacity:
    capacity_id: str

    resource_type: CrawlResourceType

    scope: AllocationScope

    scope_id: str

    region_id: str = ""
    zone_id: str = ""
    worker_pool_id: str = ""

    total_units: float = 0.0
    allocated_units: float = 0.0
    reserved_units: float = 0.0

    available_units: float = 0.0

    health_score: float = 1.0
    capacity_score: float = 1.0

    accepting_work: bool = True

    partial: bool = False

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class ResourceDemandEstimate:
    resource_id: str

    requested_units: float = 1.0

    worker_units: float = 0.0
    concurrency_units: float = 0.0
    fetch_units: float = 0.0
    bandwidth_units: float = 0.0
    cpu_units: float = 0.0
    memory_units: float = 0.0
    request_budget_units: float = 0.0

    freshness_multiplier: float = 1.0
    urgency_multiplier: float = 1.0
    volatility_multiplier: float = 1.0

    total_weighted_demand: float = 1.0

    confidence: float = 1.0

    partial: bool = False

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class ResourceAllocationShare:
    resource_id: str

    requested_units: float
    allocated_units: float

    allocation_ratio: float

    priority_score: float

    weighted_demand: float

    capacity_constrained: bool = False
    partial: bool = False

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class CrawlResourceReservation:
    reservation_id: str

    resource_id: str

    capacity_id: str

    resource_type: CrawlResourceType

    requested_units: float
    reserved_units: float

    state: ReservationState = (
        ReservationState.REQUESTED
    )

    created_at: str = ""
    expires_at: str = ""

    partial: bool = False

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class FreshnessAwareResourceAllocation:
    allocation_id: str

    resource_id: str

    decision: ResourceAllocationDecision

    priority_band: AllocationPriorityBand

    priority_score: float

    freshness_weight: float
    urgency_weight: float
    workload_weight: float

    requested_units: float
    allocated_units: float

    allocation_ratio: float

    weighted_demand: float

    capacity_score: float

    admission_state: WorkloadAdmissionState

    reasons: List[AllocationReason] = field(
        default_factory=list
    )

    reservations: List[CrawlResourceReservation] = field(
        default_factory=list
    )

    demand: Optional[ResourceDemandEstimate] = None

    share: Optional[ResourceAllocationShare] = None

    lineage: Optional[
        FreshnessResourceAllocationLineage
    ] = None

    partial: bool = False

    created_at: str = ""
    updated_at: str = ""

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class FreshnessAwareResourceAllocationResult:
    allocation_id: str

    resource_id: str

    state: FreshnessResourceAllocationState

    decision: ResourceAllocationDecision

    allocation: Optional[
        FreshnessAwareResourceAllocation
    ] = None

    accepted: bool = False
    deferred: bool = False
    rejected: bool = False
    partial: bool = False

    error: str = ""

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class FreshnessAwareResourceAllocationCheckpoint:
    checkpoint_id: str

    allocation_id: str

    resource_id: str

    checkpoint_type: AllocationCheckpointType

    state: FreshnessResourceAllocationState

    created_at: str

    payload: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class FreshnessAwareResourceAllocationEvent:
    event_id: str

    allocation_id: str

    resource_id: str

    event_type: AllocationEventType

    state: FreshnessResourceAllocationState

    created_at: str

    payload: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class FreshnessAwareResourceAllocationPolicy:
    max_resources_per_batch: int = 100000

    max_reservations_per_resource: int = 16

    minimum_confidence: float = 0.10

    minimum_capacity_score: float = 0.05
    minimum_health_score: float = 0.40

    allow_partial: bool = True

    deterministic: bool = True
    checkpoint_enabled: bool = True

    minimum_allocation_units: float = 0.01

    default_resource_units: float = 1.0

    maximum_resource_units_per_request: float = 100000.0

    freshness_weight: float = 1.35
    urgency_weight: float = 1.50
    priority_weight: float = 1.25

    change_frequency_weight: float = 1.20
    change_volatility_weight: float = 1.25
    recent_change_weight: float = 1.30

    semantic_change_weight: float = 1.10
    temporal_sensitivity_weight: float = 1.00

    source_freshness_weight: float = 0.80

    feed_signal_weight: float = 0.75
    sitemap_signal_weight: float = 0.60
    external_signal_weight: float = 0.55

    queue_pressure_weight: float = 1.15
    deadline_pressure_weight: float = 1.40

    confidence_weight: float = 0.65

    fetch_cost_weight: float = 0.50
    bandwidth_cost_weight: float = 0.35
    cpu_cost_weight: float = 0.35
    memory_cost_weight: float = 0.20

    high_priority_threshold: float = 0.72
    urgent_priority_threshold: float = 0.90
    immediate_priority_threshold: float = 0.97

    high_freshness_threshold: float = 0.70
    high_urgency_threshold: float = 0.80
    high_queue_pressure_threshold: float = 0.75
    high_volatility_threshold: float = 0.70

    immediate_allocation_multiplier: float = 2.50
    urgent_allocation_multiplier: float = 2.00
    high_allocation_multiplier: float = 1.60
    elevated_allocation_multiplier: float = 1.25
    normal_allocation_multiplier: float = 1.00
    background_allocation_multiplier: float = 0.60

    minimum_admission_score: float = 0.10

    reserve_capacity: bool = True

    reservation_seconds: float = 300.0

    fair_share_weight: float = 0.50

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


# ============================================================================
# BACKEND CONTRACT
# ============================================================================


class FreshnessAwareResourceAllocationBackend(Protocol):
    def persist_event(
        self,
        event: FreshnessAwareResourceAllocationEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: FreshnessAwareResourceAllocationCheckpoint,
    ) -> None:
        ...

    def persist_allocation(
        self,
        allocation: FreshnessAwareResourceAllocation,
    ) -> None:
        ...

    def persist_result(
        self,
        result: FreshnessAwareResourceAllocationResult,
    ) -> None:
        ...

    def get_result(
        self,
        allocation_id: str,
    ) -> Optional[
        FreshnessAwareResourceAllocationResult
    ]:
        ...

    def get_allocation(
        self,
        allocation_id: str,
    ) -> Optional[
        FreshnessAwareResourceAllocation
    ]:
        ...


class InMemoryFreshnessAwareResourceAllocationMetadata:
    """
    Reference metadata backend.

    Production deployments should replace this with durable distributed
    metadata and resource-capacity storage without changing the orchestration
    contract.
    """

    def __init__(self) -> None:
        self._events: List[
            FreshnessAwareResourceAllocationEvent
        ] = []

        self._checkpoints: List[
            FreshnessAwareResourceAllocationCheckpoint
        ] = []

        self._allocations: Dict[
            str,
            FreshnessAwareResourceAllocation,
        ] = {}

        self._results: Dict[
            str,
            FreshnessAwareResourceAllocationResult,
        ] = {}

    def persist_event(
        self,
        event: FreshnessAwareResourceAllocationEvent,
    ) -> None:
        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: FreshnessAwareResourceAllocationCheckpoint,
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_allocation(
        self,
        allocation: FreshnessAwareResourceAllocation,
    ) -> None:
        self._allocations[
            allocation.allocation_id
        ] = allocation

    def persist_result(
        self,
        result: FreshnessAwareResourceAllocationResult,
    ) -> None:
        self._results[
            result.allocation_id
        ] = result

    def get_result(
        self,
        allocation_id: str,
    ) -> Optional[
        FreshnessAwareResourceAllocationResult
    ]:
        return self._results.get(allocation_id)

    def get_allocation(
        self,
        allocation_id: str,
    ) -> Optional[
        FreshnessAwareResourceAllocation
    ]:
        return self._allocations.get(allocation_id)

    def events(
        self,
    ) -> List[
        FreshnessAwareResourceAllocationEvent
    ]:
        return list(self._events)

    def checkpoints(
        self,
    ) -> List[
        FreshnessAwareResourceAllocationCheckpoint
    ]:
        return list(self._checkpoints)

    def allocations(
        self,
    ) -> List[
        FreshnessAwareResourceAllocation
    ]:
        return list(self._allocations.values())

    def results(
        self,
    ) -> List[
        FreshnessAwareResourceAllocationResult
    ]:
        return list(self._results.values())


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class FreshnessAwareCrawlResourceAllocationArchitecture:
    """
    Production-oriented freshness-aware crawl resource allocation.

    This architecture determines how crawler capacity should be allocated
    without executing crawler workers or making network requests.
    """

    def __init__(
        self,
        backend: Optional[
            FreshnessAwareResourceAllocationBackend
        ] = None,
        policy: Optional[
            FreshnessAwareResourceAllocationPolicy
        ] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else (
                InMemoryFreshnessAwareResourceAllocationMetadata()
            )
        )

        self.policy = (
            policy
            if policy is not None
            else (
                FreshnessAwareResourceAllocationPolicy()
            )
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

        return max(
            minimum,
            min(
                maximum,
                value,
            ),
        )

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
    def _read_value(
        source: Any,
        key: str,
        default: Any = None,
    ) -> Any:
        if source is None:
            return default

        if isinstance(source, Mapping):
            return source.get(
                key,
                default,
            )

        return getattr(
            source,
            key,
            default,
        )

    @staticmethod
    def _deterministic_unit(
        *parts: Any,
    ) -> float:
        payload = "|".join(
            str(part)
            for part in parts
        )

        digest = hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()

        value = int(
            digest[:16],
            16,
        )

        return value / float(
            0xFFFFFFFFFFFFFFFF
        )

    @classmethod
    def _deterministic_id(
        cls,
        prefix: str,
        *parts: Any,
    ) -> str:
        payload = "|".join(
            str(part)
            for part in parts
        )

        digest = hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()

        return (
            f"{prefix}-{digest[:32]}"
        )

    # ----------------------------------------------------------------------
    # EVENTS / CHECKPOINTS
    # ----------------------------------------------------------------------

    def _event(
        self,
        allocation_id: str,
        resource_id: str,
        event_type: AllocationEventType,
        state: FreshnessResourceAllocationState,
        payload: Optional[
            Dict[str, Any]
        ] = None,
    ) -> FreshnessAwareResourceAllocationEvent:
        now = self._now().isoformat()

        event = FreshnessAwareResourceAllocationEvent(
            event_id=self._deterministic_id(
                "allocation-event",
                allocation_id,
                resource_id,
                event_type.value,
                now,
            ),
            allocation_id=allocation_id,
            resource_id=resource_id,
            event_type=event_type,
            state=state,
            created_at=now,
            payload=payload or {},
        )

        self.backend.persist_event(event)

        return event

    def _checkpoint(
        self,
        allocation_id: str,
        resource_id: str,
        checkpoint_type: AllocationCheckpointType,
        state: FreshnessResourceAllocationState,
        payload: Optional[
            Dict[str, Any]
        ] = None,
    ) -> FreshnessAwareResourceAllocationCheckpoint:
        now = self._now().isoformat()

        checkpoint = (
            FreshnessAwareResourceAllocationCheckpoint(
                checkpoint_id=self._deterministic_id(
                    "allocation-checkpoint",
                    allocation_id,
                    resource_id,
                    checkpoint_type.value,
                    now,
                ),
                allocation_id=allocation_id,
                resource_id=resource_id,
                checkpoint_type=checkpoint_type,
                state=state,
                created_at=now,
                payload=payload or {},
            )
        )

        if self.policy.checkpoint_enabled:
            self.backend.persist_checkpoint(
                checkpoint
            )

            self._event(
                allocation_id,
                resource_id,
                AllocationEventType.CHECKPOINT_CREATED,
                state,
                {
                    "checkpoint_id":
                        checkpoint.checkpoint_id,
                    "checkpoint_type":
                        checkpoint_type.value,
                },
            )

        return checkpoint

    # ----------------------------------------------------------------------
    # INPUT NORMALIZATION
    # ----------------------------------------------------------------------

    def _normalize_input(
        self,
        source: Any,
    ) -> FreshnessResourceAllocationInput:
        return FreshnessResourceAllocationInput(
            resource_id=str(
                self._read_value(
                    source,
                    "resource_id",
                    "",
                )
                or ""
            ),
            orchestration_id=str(
                self._read_value(
                    source,
                    "orchestration_id",
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
            region_id=str(
                self._read_value(
                    source,
                    "region_id",
                    "",
                )
                or ""
            ),
            zone_id=str(
                self._read_value(
                    source,
                    "zone_id",
                    "",
                )
                or ""
            ),
            worker_pool_id=str(
                self._read_value(
                    source,
                    "worker_pool_id",
                    "",
                )
                or ""
            ),
            priority_score=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "priority_score",
                        0.5,
                    ),
                    0.5,
                )
            ),
            priority_band=str(
                self._read_value(
                    source,
                    "priority_band",
                    AllocationPriorityBand.NORMAL.value,
                )
                or AllocationPriorityBand.NORMAL.value
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
            recent_change=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "recent_change",
                        0.0,
                    ),
                    0.0,
                )
            ),
            semantic_change_score=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "semantic_change_score",
                        0.0,
                    ),
                    0.0,
                )
            ),
            temporal_sensitivity=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "temporal_sensitivity",
                        self._read_value(
                            source,
                            "query_time_sensitivity",
                            0.0,
                        ),
                    ),
                    0.0,
                )
            ),
            source_freshness=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "source_freshness",
                        0.0,
                    ),
                    0.0,
                )
            ),
            feed_update_signal=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "feed_update_signal",
                        0.0,
                    ),
                    0.0,
                )
            ),
            sitemap_update_signal=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "sitemap_update_signal",
                        0.0,
                    ),
                    0.0,
                )
            ),
            external_update_signal=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "external_update_signal",
                        0.0,
                    ),
                    0.0,
                )
            ),
            queue_pressure=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "queue_pressure",
                        0.0,
                    ),
                    0.0,
                )
            ),
            deadline_pressure=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "deadline_pressure",
                        0.0,
                    ),
                    0.0,
                )
            ),
            confidence=self._clamp(
                self._safe_float(
                    self._read_value(
                        source,
                        "confidence",
                        1.0,
                    ),
                    1.0,
                )
            ),
            orchestration_partial=bool(
                self._read_value(
                    source,
                    "orchestration_partial",
                    False,
                )
            ),
            partial=bool(
                self._read_value(
                    source,
                    "partial",
                    False,
                )
            ),
            requested_resource_units=max(
                self.policy.minimum_allocation_units,
                min(
                    self.policy.maximum_resource_units_per_request,
                    self._safe_float(
                        self._read_value(
                            source,
                            "requested_resource_units",
                            self.policy.default_resource_units,
                        ),
                        self.policy.default_resource_units,
                    ),
                ),
            ),
            estimated_fetch_cost=max(
                0.0,
                self._safe_float(
                    self._read_value(
                        source,
                        "estimated_fetch_cost",
                        1.0,
                    ),
                    1.0,
                ),
            ),
            estimated_bandwidth_cost=max(
                0.0,
                self._safe_float(
                    self._read_value(
                        source,
                        "estimated_bandwidth_cost",
                        1.0,
                    ),
                    1.0,
                ),
            ),
            estimated_cpu_cost=max(
                0.0,
                self._safe_float(
                    self._read_value(
                        source,
                        "estimated_cpu_cost",
                        1.0,
                    ),
                    1.0,
                ),
            ),
            estimated_memory_cost=max(
                0.0,
                self._safe_float(
                    self._read_value(
                        source,
                        "estimated_memory_cost",
                        1.0,
                    ),
                    1.0,
                ),
            ),
            metadata=dict(
                self._read_value(
                    source,
                    "metadata",
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
        data: FreshnessResourceAllocationInput,
    ) -> Tuple[bool, str]:
        if not data.resource_id:
            return False, "resource_id is required"

        if data.confidence < (
            self.policy.minimum_confidence
        ) and not self.policy.allow_partial:
            return (
                False,
                "confidence below minimum threshold",
            )

        if data.orchestration_id == "":
            if data.queue_entry_id == "":
                return (
                    False,
                    "orchestration_id or queue_entry_id is required",
                )

        return True, ""

    # ----------------------------------------------------------------------
    # PRIORITY
    # ----------------------------------------------------------------------

    def _priority_band(
        self,
        score: float,
    ) -> AllocationPriorityBand:
        score = self._clamp(score)

        if score >= (
            self.policy.immediate_priority_threshold
        ):
            return AllocationPriorityBand.IMMEDIATE

        if score >= (
            self.policy.urgent_priority_threshold
        ):
            return AllocationPriorityBand.URGENT

        if score >= (
            self.policy.high_priority_threshold
        ):
            return AllocationPriorityBand.HIGH

        if score >= 0.50:
            return AllocationPriorityBand.ELEVATED

        if score >= 0.25:
            return AllocationPriorityBand.NORMAL

        return AllocationPriorityBand.BACKGROUND

    # ----------------------------------------------------------------------
    # FRESHNESS IMPORTANCE
    # ----------------------------------------------------------------------

    def _freshness_weight(
        self,
        data: FreshnessResourceAllocationInput,
    ) -> float:
        value = (
            data.freshness_score
            * self.policy.freshness_weight
            + data.change_frequency
            * self.policy.change_frequency_weight
            + data.change_volatility
            * self.policy.change_volatility_weight
            + data.recent_change
            * self.policy.recent_change_weight
            + data.semantic_change_score
            * self.policy.semantic_change_weight
            + data.temporal_sensitivity
            * self.policy.temporal_sensitivity_weight
            + data.source_freshness
            * self.policy.source_freshness_weight
            + data.feed_update_signal
            * self.policy.feed_signal_weight
            + data.sitemap_update_signal
            * self.policy.sitemap_signal_weight
            + data.external_update_signal
            * self.policy.external_signal_weight
        )

        denominator = (
            self.policy.freshness_weight
            + self.policy.change_frequency_weight
            + self.policy.change_volatility_weight
            + self.policy.recent_change_weight
            + self.policy.semantic_change_weight
            + self.policy.temporal_sensitivity_weight
            + self.policy.source_freshness_weight
            + self.policy.feed_signal_weight
            + self.policy.sitemap_signal_weight
            + self.policy.external_signal_weight
        )

        if denominator <= 0:
            return 0.0

        return self._clamp(
            value / denominator
        )

    # ----------------------------------------------------------------------
    # URGENCY
    # ----------------------------------------------------------------------

    def _urgency_weight(
        self,
        data: FreshnessResourceAllocationInput,
    ) -> float:
        value = (
            data.urgency_score
            * self.policy.urgency_weight
            + data.deadline_pressure
            * self.policy.deadline_pressure_weight
            + data.queue_pressure
            * self.policy.queue_pressure_weight
            + data.priority_score
            * self.policy.priority_weight
        )

        denominator = (
            self.policy.urgency_weight
            + self.policy.deadline_pressure_weight
            + self.policy.queue_pressure_weight
            + self.policy.priority_weight
        )

        if denominator <= 0:
            return 0.0

        return self._clamp(
            value / denominator
        )

    # ----------------------------------------------------------------------
    # WORKLOAD DEMAND
    # ----------------------------------------------------------------------

    def estimate_demand(
        self,
        data: FreshnessResourceAllocationInput,
    ) -> ResourceDemandEstimate:
        freshness = self._freshness_weight(data)
        urgency = self._urgency_weight(data)

        volatility = data.change_volatility

        freshness_multiplier = (
            1.0 + freshness
        )

        urgency_multiplier = (
            1.0 + urgency
        )

        volatility_multiplier = (
            1.0 + volatility * 0.75
        )

        base_units = max(
            self.policy.minimum_allocation_units,
            data.requested_resource_units,
        )

        weighted = (
            base_units
            * freshness_multiplier
            * urgency_multiplier
            * volatility_multiplier
        )

        fetch = (
            base_units
            * (
                1.0
                + data.estimated_fetch_cost
                * self.policy.fetch_cost_weight
            )
        )

        bandwidth = (
            base_units
            * (
                1.0
                + data.estimated_bandwidth_cost
                * self.policy.bandwidth_cost_weight
            )
        )

        cpu = (
            base_units
            * (
                1.0
                + data.estimated_cpu_cost
                * self.policy.cpu_cost_weight
            )
        )

        memory = (
            base_units
            * (
                1.0
                + data.estimated_memory_cost
                * self.policy.memory_cost_weight
            )
        )

        confidence = data.confidence

        partial = (
            data.partial
            or data.orchestration_partial
        )

        return ResourceDemandEstimate(
            resource_id=data.resource_id,
            requested_units=base_units,
            worker_units=weighted,
            concurrency_units=weighted,
            fetch_units=fetch,
            bandwidth_units=bandwidth,
            cpu_units=cpu,
            memory_units=memory,
            request_budget_units=weighted,
            freshness_multiplier=freshness_multiplier,
            urgency_multiplier=urgency_multiplier,
            volatility_multiplier=volatility_multiplier,
            total_weighted_demand=weighted,
            confidence=confidence,
            partial=partial,
            metadata={
                "freshness_weight": freshness,
                "urgency_weight": urgency,
            },
        )

    # ----------------------------------------------------------------------
    # ALLOCATION PRIORITY
    # ----------------------------------------------------------------------

    def _allocation_priority(
        self,
        data: FreshnessResourceAllocationInput,
        demand: ResourceDemandEstimate,
    ) -> float:
        freshness = self._freshness_weight(data)

        urgency = self._urgency_weight(data)

        demand_score = self._clamp(
            demand.total_weighted_demand
            / max(
                1.0,
                data.requested_resource_units,
            )
            / 4.0
        )

        score = (
            freshness
            * self.policy.freshness_weight
            + urgency
            * self.policy.urgency_weight
            + data.priority_score
            * self.policy.priority_weight
            + demand_score
            * self.policy.queue_pressure_weight
            + data.confidence
            * self.policy.confidence_weight
        )

        denominator = (
            self.policy.freshness_weight
            + self.policy.urgency_weight
            + self.policy.priority_weight
            + self.policy.queue_pressure_weight
            + self.policy.confidence_weight
        )

        if denominator <= 0:
            return 0.0

        return self._clamp(
            score / denominator
        )

    # ----------------------------------------------------------------------
    # ALLOCATION MULTIPLIER
    # ----------------------------------------------------------------------

    def _allocation_multiplier(
        self,
        score: float,
    ) -> float:
        band = self._priority_band(score)

        mapping = {
            AllocationPriorityBand.IMMEDIATE:
                self.policy.immediate_allocation_multiplier,
            AllocationPriorityBand.URGENT:
                self.policy.urgent_allocation_multiplier,
            AllocationPriorityBand.HIGH:
                self.policy.high_allocation_multiplier,
            AllocationPriorityBand.ELEVATED:
                self.policy.elevated_allocation_multiplier,
            AllocationPriorityBand.NORMAL:
                self.policy.normal_allocation_multiplier,
            AllocationPriorityBand.BACKGROUND:
                self.policy.background_allocation_multiplier,
        }

        return mapping[band]

    # ----------------------------------------------------------------------
    # CAPACITY ANALYSIS
    # ----------------------------------------------------------------------

    def _capacity_score(
        self,
        capacity: CrawlResourceCapacity,
    ) -> float:
        health = self._clamp(
            capacity.health_score
        )

        explicit = self._clamp(
            capacity.capacity_score
        )

        if capacity.total_units > 0:
            free_ratio = (
                max(
                    0.0,
                    capacity.available_units,
                )
                / capacity.total_units
            )
        else:
            free_ratio = explicit

        return self._clamp(
            health * 0.35
            + explicit * 0.35
            + self._clamp(
                free_ratio
            ) * 0.30
        )

    def _capacity_available(
        self,
        capacity: CrawlResourceCapacity,
    ) -> bool:
        if not capacity.accepting_work:
            return False

        if capacity.health_score < (
            self.policy.minimum_health_score
        ):
            return False

        if self._capacity_score(capacity) < (
            self.policy.minimum_capacity_score
        ):
            return False

        return (
            capacity.available_units
            >= self.policy.minimum_allocation_units
        )

    # ----------------------------------------------------------------------
    # REASONS
    # ----------------------------------------------------------------------

    def _reasons(
        self,
        data: FreshnessResourceAllocationInput,
        priority_score: float,
        partial: bool,
        capacity_limited: bool,
    ) -> List[AllocationReason]:
        reasons: List[AllocationReason] = []

        if data.freshness_score >= (
            self.policy.high_freshness_threshold
        ):
            reasons.append(
                AllocationReason.HIGH_FRESHNESS_IMPORTANCE
            )

        if data.urgency_score >= (
            self.policy.high_urgency_threshold
        ):
            reasons.append(
                AllocationReason.HIGH_URGENCY
            )

        if data.recent_change >= 0.70:
            reasons.append(
                AllocationReason.RECENT_CHANGE
            )

        if data.change_frequency >= 0.70:
            reasons.append(
                AllocationReason.HIGH_CHANGE_FREQUENCY
            )

        if data.change_volatility >= (
            self.policy.high_volatility_threshold
        ):
            reasons.append(
                AllocationReason.HIGH_CHANGE_VOLATILITY
            )

        if data.temporal_sensitivity >= 0.70:
            reasons.append(
                AllocationReason.TEMPORAL_SENSITIVITY
            )

        if data.source_freshness >= 0.70:
            reasons.append(
                AllocationReason.SOURCE_FRESHNESS
            )

        if data.feed_update_signal >= 0.70:
            reasons.append(
                AllocationReason.FEED_ACTIVITY
            )

        if data.sitemap_update_signal >= 0.70:
            reasons.append(
                AllocationReason.SITEMAP_ACTIVITY
            )

        if data.external_update_signal >= 0.70:
            reasons.append(
                AllocationReason.EXTERNAL_ACTIVITY
            )

        if data.deadline_pressure >= 0.70:
            reasons.append(
                AllocationReason.MISSED_DEADLINE
            )

        if data.queue_pressure >= (
            self.policy.high_queue_pressure_threshold
        ):
            reasons.append(
                AllocationReason.HIGH_QUEUE_PRESSURE
            )

        if priority_score >= (
            self.policy.high_priority_threshold
        ):
            reasons.append(
                AllocationReason.HIGH_PRIORITY
            )

        if priority_score >= (
            self.policy.immediate_priority_threshold
        ):
            reasons.append(
                AllocationReason.IMMEDIATE_PRIORITY
            )

        if data.confidence >= 0.80:
            reasons.append(
                AllocationReason.HIGH_CONFIDENCE
            )

        if capacity_limited:
            reasons.append(
                AllocationReason.CAPACITY_LIMITED
            )

        if partial:
            reasons.append(
                AllocationReason.PARTIAL_CAPACITY
            )

        if not reasons:
            reasons.append(
                AllocationReason.FAIR_SHARE
            )

        return list(
            dict.fromkeys(reasons)
        )

    # ----------------------------------------------------------------------
    # RESERVATION
    # ----------------------------------------------------------------------

    def _build_reservation(
        self,
        data: FreshnessResourceAllocationInput,
        capacity: CrawlResourceCapacity,
        requested_units: float,
        reserved_units: float,
        allocation_id: str,
    ) -> CrawlResourceReservation:
        now = self._now()

        expires = (
            now.timestamp()
            + self.policy.reservation_seconds
        )

        expires_at = datetime.fromtimestamp(
            expires,
            tz=timezone.utc,
        ).isoformat()

        if reserved_units <= 0:
            state = ReservationState.REQUESTED
        elif reserved_units < requested_units:
            state = ReservationState.PARTIAL
        else:
            state = ReservationState.RESERVED

        return CrawlResourceReservation(
            reservation_id=self._deterministic_id(
                "reservation",
                allocation_id,
                data.resource_id,
                capacity.capacity_id,
                capacity.resource_type.value,
            ),
            resource_id=data.resource_id,
            capacity_id=capacity.capacity_id,
            resource_type=capacity.resource_type,
            requested_units=requested_units,
            reserved_units=reserved_units,
            state=state,
            created_at=now.isoformat(),
            expires_at=expires_at,
            partial=(
                reserved_units < requested_units
            ),
            metadata={
                "scope": capacity.scope.value,
                "scope_id": capacity.scope_id,
            },
        )

    # ----------------------------------------------------------------------
    # MAIN ALLOCATION
    # ----------------------------------------------------------------------

    def allocate(
        self,
        source: Any,
        capacities: Sequence[
            CrawlResourceCapacity
        ],
    ) -> FreshnessAwareResourceAllocationResult:
        data = self._normalize_input(source)

        allocation_id = self._deterministic_id(
            "allocation",
            data.resource_id,
            data.orchestration_id,
            data.queue_entry_id,
            ARCHITECTURE_VERSION,
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.REQUEST_RECEIVED,
            FreshnessResourceAllocationState.RECEIVED,
            {
                "orchestration_id":
                    data.orchestration_id,
                "queue_entry_id":
                    data.queue_entry_id,
            },
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.VALIDATION_STARTED,
            FreshnessResourceAllocationState.VALIDATING,
        )

        valid, error = self._validate_input(
            data
        )

        if not valid:
            result = (
                FreshnessAwareResourceAllocationResult(
                    allocation_id=allocation_id,
                    resource_id=data.resource_id,
                    state=(
                        FreshnessResourceAllocationState.REJECTED
                    ),
                    decision=(
                        ResourceAllocationDecision.REJECT
                    ),
                    accepted=False,
                    deferred=False,
                    rejected=True,
                    partial=False,
                    error=error,
                )
            )

            self._event(
                allocation_id,
                data.resource_id,
                AllocationEventType.WORKLOAD_REJECTED,
                FreshnessResourceAllocationState.REJECTED,
                {
                    "error": error,
                },
            )

            self.backend.persist_result(
                result
            )

            return result

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.INPUT_NORMALIZED,
            FreshnessResourceAllocationState.NORMALIZING,
        )

        self._checkpoint(
            allocation_id,
            data.resource_id,
            AllocationCheckpointType.INPUT_NORMALIZED,
            FreshnessResourceAllocationState.NORMALIZING,
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.FRESHNESS_ANALYSIS_STARTED,
            FreshnessResourceAllocationState.FRESHNESS_ANALYSIS,
        )

        freshness_weight = (
            self._freshness_weight(data)
        )

        urgency_weight = (
            self._urgency_weight(data)
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.FRESHNESS_ANALYZED,
            FreshnessResourceAllocationState.FRESHNESS_ANALYSIS,
            {
                "freshness_weight":
                    freshness_weight,
                "urgency_weight":
                    urgency_weight,
            },
        )

        self._checkpoint(
            allocation_id,
            data.resource_id,
            AllocationCheckpointType.FRESHNESS_ANALYZED,
            FreshnessResourceAllocationState.FRESHNESS_ANALYSIS,
            {
                "freshness_weight":
                    freshness_weight,
                "urgency_weight":
                    urgency_weight,
            },
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.DEMAND_ESTIMATION_STARTED,
            FreshnessResourceAllocationState.DEMAND_ESTIMATION,
        )

        demand = self.estimate_demand(
            data
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.DEMAND_ESTIMATED,
            FreshnessResourceAllocationState.DEMAND_ESTIMATION,
            {
                "requested_units":
                    demand.requested_units,
                "weighted_demand":
                    demand.total_weighted_demand,
            },
        )

        self._checkpoint(
            allocation_id,
            data.resource_id,
            AllocationCheckpointType.DEMAND_ESTIMATED,
            FreshnessResourceAllocationState.DEMAND_ESTIMATION,
            {
                "weighted_demand":
                    demand.total_weighted_demand,
            },
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.CAPACITY_ANALYSIS_STARTED,
            FreshnessResourceAllocationState.CAPACITY_ANALYSIS,
        )

        eligible_capacities = [
            capacity
            for capacity in capacities
            if self._capacity_available(
                capacity
            )
        ]

        if not eligible_capacities:
            result = (
                FreshnessAwareResourceAllocationResult(
                    allocation_id=allocation_id,
                    resource_id=data.resource_id,
                    state=(
                        FreshnessResourceAllocationState.DEFERRED
                    ),
                    decision=(
                        ResourceAllocationDecision.DEFER
                    ),
                    accepted=False,
                    deferred=True,
                    rejected=False,
                    partial=True,
                    error=(
                        "no eligible crawl resource capacity"
                    ),
                )
            )

            self._event(
                allocation_id,
                data.resource_id,
                AllocationEventType.WORKLOAD_DEFERRED,
                FreshnessResourceAllocationState.DEFERRED,
                {
                    "capacity_count": len(
                        capacities
                    ),
                },
            )

            self.backend.persist_result(
                result
            )

            return result

        total_available = sum(
            max(
                0.0,
                capacity.available_units,
            )
            for capacity in eligible_capacities
        )

        capacity_score = self._clamp(
            sum(
                self._capacity_score(
                    capacity
                )
                for capacity in eligible_capacities
            )
            / max(
                1,
                len(eligible_capacities),
            )
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.CAPACITY_ANALYZED,
            FreshnessResourceAllocationState.CAPACITY_ANALYSIS,
            {
                "eligible_capacity_count":
                    len(eligible_capacities),
                "total_available_units":
                    total_available,
                "capacity_score":
                    capacity_score,
            },
        )

        self._checkpoint(
            allocation_id,
            data.resource_id,
            AllocationCheckpointType.CAPACITY_ANALYZED,
            FreshnessResourceAllocationState.CAPACITY_ANALYSIS,
            {
                "total_available_units":
                    total_available,
                "capacity_score":
                    capacity_score,
            },
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.PRIORITY_CALCULATION_STARTED,
            FreshnessResourceAllocationState.PRIORITY_CALCULATION,
        )

        priority_score = (
            self._allocation_priority(
                data,
                demand,
            )
        )

        priority_band = (
            self._priority_band(
                priority_score
            )
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.PRIORITY_CALCULATED,
            FreshnessResourceAllocationState.PRIORITY_CALCULATION,
            {
                "priority_score":
                    priority_score,
                "priority_band":
                    priority_band.value,
            },
        )

        self._checkpoint(
            allocation_id,
            data.resource_id,
            AllocationCheckpointType.PRIORITY_CALCULATED,
            FreshnessResourceAllocationState.PRIORITY_CALCULATION,
            {
                "priority_score":
                    priority_score,
            },
        )

        if priority_score < (
            self.policy.minimum_admission_score
        ):
            result = (
                FreshnessAwareResourceAllocationResult(
                    allocation_id=allocation_id,
                    resource_id=data.resource_id,
                    state=(
                        FreshnessResourceAllocationState.DEFERRED
                    ),
                    decision=(
                        ResourceAllocationDecision.DEFER
                    ),
                    accepted=False,
                    deferred=True,
                    rejected=False,
                    partial=data.partial,
                    error=(
                        "allocation priority below admission threshold"
                    ),
                )
            )

            self._event(
                allocation_id,
                data.resource_id,
                AllocationEventType.WORKLOAD_DEFERRED,
                FreshnessResourceAllocationState.DEFERRED,
                {
                    "priority_score":
                        priority_score,
                },
            )

            self.backend.persist_result(
                result
            )

            return result

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.RESOURCE_ALLOCATION_STARTED,
            FreshnessResourceAllocationState.RESOURCE_ALLOCATION,
        )

        multiplier = (
            self._allocation_multiplier(
                priority_score
            )
        )

        desired_units = (
            demand.total_weighted_demand
            * multiplier
        )

        desired_units = max(
            self.policy.minimum_allocation_units,
            min(
                self.policy.maximum_resource_units_per_request,
                desired_units,
            ),
        )

        allocated_units = min(
            desired_units,
            total_available,
        )

        capacity_limited = (
            allocated_units
            < desired_units
        )

        partial = (
            data.partial
            or data.orchestration_partial
            or capacity_limited
        )

        if allocated_units <= 0:
            result = (
                FreshnessAwareResourceAllocationResult(
                    allocation_id=allocation_id,
                    resource_id=data.resource_id,
                    state=(
                        FreshnessResourceAllocationState.DEFERRED
                    ),
                    decision=(
                        ResourceAllocationDecision.DEFER
                    ),
                    accepted=False,
                    deferred=True,
                    rejected=False,
                    partial=True,
                    error=(
                        "resource capacity unavailable"
                    ),
                )
            )

            self.backend.persist_result(
                result
            )

            return result

        allocation_ratio = self._clamp(
            allocated_units
            / max(
                desired_units,
                self.policy.minimum_allocation_units,
            )
        )

        decision = (
            ResourceAllocationDecision.PARTIAL_ALLOCATE
            if partial
            else ResourceAllocationDecision.ALLOCATE
        )

        admission_state = (
            WorkloadAdmissionState.PARTIALLY_ADMITTED
            if partial
            else WorkloadAdmissionState.ADMITTED
        )

        share = ResourceAllocationShare(
            resource_id=data.resource_id,
            requested_units=desired_units,
            allocated_units=allocated_units,
            allocation_ratio=allocation_ratio,
            priority_score=priority_score,
            weighted_demand=(
                demand.total_weighted_demand
            ),
            capacity_constrained=capacity_limited,
            partial=partial,
        )

        lineage = FreshnessResourceAllocationLineage(
            resource_id=data.resource_id,
            previous_stage=PREVIOUS_STAGE,
            current_stage=PHASE,
            source_orchestration_id=(
                data.orchestration_id
            ),
            source_queue_entry_id=(
                data.queue_entry_id
            ),
            source_schedule_id=str(
                data.metadata.get(
                    "schedule_id",
                    "",
                )
            ),
            source_frequency_version=str(
                data.metadata.get(
                    "frequency_version",
                    "",
                )
            ),
            lineage_metadata={
                "architecture_version":
                    ARCHITECTURE_VERSION,
            },
        )

        reservations: List[
            CrawlResourceReservation
        ] = []

        remaining = allocated_units

        if self.policy.reserve_capacity:
            for capacity in eligible_capacities:
                if remaining <= 0:
                    break

                reservation_units = min(
                    remaining,
                    max(
                        0.0,
                        capacity.available_units,
                    ),
                )

                if reservation_units <= 0:
                    continue

                if len(reservations) >= (
                    self.policy.max_reservations_per_resource
                ):
                    break

                reservation = (
                    self._build_reservation(
                        data,
                        capacity,
                        desired_units,
                        reservation_units,
                        allocation_id,
                    )
                )

                reservations.append(
                    reservation
                )

                remaining -= (
                    reservation_units
                )

        allocation = (
            FreshnessAwareResourceAllocation(
                allocation_id=allocation_id,
                resource_id=data.resource_id,
                decision=decision,
                priority_band=priority_band,
                priority_score=priority_score,
                freshness_weight=freshness_weight,
                urgency_weight=urgency_weight,
                workload_weight=(
                    self._clamp(
                        demand.total_weighted_demand
                        / max(
                            1.0,
                            desired_units,
                        )
                    )
                ),
                requested_units=desired_units,
                allocated_units=allocated_units,
                allocation_ratio=allocation_ratio,
                weighted_demand=(
                    demand.total_weighted_demand
                ),
                capacity_score=capacity_score,
                admission_state=admission_state,
                reasons=self._reasons(
                    data,
                    priority_score,
                    partial,
                    capacity_limited,
                ),
                reservations=reservations,
                demand=demand,
                share=share,
                lineage=lineage,
                partial=partial,
                created_at=self._now().isoformat(),
                updated_at=self._now().isoformat(),
                metadata={
                    "allocation_multiplier":
                        multiplier,
                    "total_available_capacity":
                        total_available,
                },
            )
        )

        self.backend.persist_allocation(
            allocation
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.RESOURCE_ALLOCATED,
            FreshnessResourceAllocationState.RESOURCE_ALLOCATION,
            {
                "requested_units":
                    desired_units,
                "allocated_units":
                    allocated_units,
                "allocation_ratio":
                    allocation_ratio,
            },
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.ADMISSION_DECIDED,
            FreshnessResourceAllocationState.ADMISSION_CONTROL,
            {
                "admission_state":
                    admission_state.value,
                "partial":
                    partial,
            },
        )

        if reservations:
            self._event(
                allocation_id,
                data.resource_id,
                AllocationEventType.RESERVATION_CREATED,
                FreshnessResourceAllocationState.RESERVATION_PLANNING,
                {
                    "reservation_count":
                        len(reservations),
                },
            )

            self._checkpoint(
                allocation_id,
                data.resource_id,
                AllocationCheckpointType.RESERVATION_CREATED,
                FreshnessResourceAllocationState.RESERVATION_PLANNING,
                {
                    "reservation_count":
                        len(reservations),
                },
            )

        if partial:
            self._event(
                allocation_id,
                data.resource_id,
                AllocationEventType.PARTIAL_ALLOCATION,
                FreshnessResourceAllocationState.PARTIAL,
                {
                    "allocation_ratio":
                        allocation_ratio,
                },
            )

        self._checkpoint(
            allocation_id,
            data.resource_id,
            AllocationCheckpointType.RESOURCE_ALLOCATED,
            FreshnessResourceAllocationState.RESOURCE_ALLOCATION,
            {
                "allocated_units":
                    allocated_units,
            },
        )

        result = (
            FreshnessAwareResourceAllocationResult(
                allocation_id=allocation_id,
                resource_id=data.resource_id,
                state=(
                    FreshnessResourceAllocationState.PARTIAL
                    if partial
                    else FreshnessResourceAllocationState.COMPLETED
                ),
                decision=decision,
                allocation=allocation,
                accepted=True,
                deferred=False,
                rejected=False,
                partial=partial,
            )
        )

        self._event(
            allocation_id,
            data.resource_id,
            AllocationEventType.ALLOCATION_COMPLETED,
            result.state,
            {
                "decision":
                    decision.value,
                "partial":
                    partial,
            },
        )

        self._checkpoint(
            allocation_id,
            data.resource_id,
            AllocationCheckpointType.COMPLETED,
            result.state,
            {
                "decision":
                    decision.value,
            },
        )

        self.backend.persist_result(
            result
        )

        return result

    # ----------------------------------------------------------------------
    # BATCH ALLOCATION
    # ----------------------------------------------------------------------

    def allocate_many(
        self,
        sources: Iterable[Any],
        capacities: Sequence[
            CrawlResourceCapacity
        ],
    ) -> List[
        FreshnessAwareResourceAllocationResult
    ]:
        results: List[
            FreshnessAwareResourceAllocationResult
        ] = []

        count = 0

        for source in sources:
            if count >= (
                self.policy.max_resources_per_batch
            ):
                break

            results.append(
                self.allocate(
                    source,
                    capacities,
                )
            )

            count += 1

        return results

    # ----------------------------------------------------------------------
    # FAIR-SHARE ALLOCATION
    # ----------------------------------------------------------------------

    def allocate_fair_share(
        self,
        sources: Sequence[Any],
        capacities: Sequence[
            CrawlResourceCapacity
        ],
    ) -> List[
        FreshnessAwareResourceAllocationResult
    ]:
        """
        Allocate capacity across multiple freshness workloads.

        Higher freshness/urgency workloads receive greater weighted demand,
        while capacity remains explicitly bounded by the supplied execution
        capacity for this allocation unit.
        """

        normalized = [
            self._normalize_input(source)
            for source in sources
        ]

        demand_pairs = [
            (
                data,
                self.estimate_demand(data),
            )
            for data in normalized
        ]

        if not demand_pairs:
            return []

        total_capacity = sum(
            max(
                0.0,
                capacity.available_units,
            )
            for capacity in capacities
            if self._capacity_available(
                capacity
            )
        )

        if total_capacity <= 0:
            return [
                self.allocate(
                    data,
                    capacities,
                )
                for data, _ in demand_pairs
            ]

        weighted_demands: List[
            Tuple[
                FreshnessResourceAllocationInput,
                ResourceDemandEstimate,
                float,
            ]
        ] = []

        total_weight = 0.0

        for data, demand in demand_pairs:
            priority = (
                self._allocation_priority(
                    data,
                    demand,
                )
            )

            weighted = (
                demand.total_weighted_demand
                * (
                    1.0
                    + priority
                    * self.policy.fair_share_weight
                )
            )

            weighted_demands.append(
                (
                    data,
                    demand,
                    weighted,
                )
            )

            total_weight += weighted

        if total_weight <= 0:
            return [
                self.allocate(
                    data,
                    capacities,
                )
                for data, _, _ in weighted_demands
            ]

        results: List[
            FreshnessAwareResourceAllocationResult
        ] = []

        for data, demand, weighted in weighted_demands:
            fair_units = (
                total_capacity
                * weighted
                / total_weight
            )

            fair_units = max(
                self.policy.minimum_allocation_units,
                fair_units,
            )

            source_mapping = {
                "resource_id":
                    data.resource_id,
                "orchestration_id":
                    data.orchestration_id,
                "queue_entry_id":
                    data.queue_entry_id,
                "priority_score":
                    data.priority_score,
                "freshness_score":
                    data.freshness_score,
                "urgency_score":
                    data.urgency_score,
                "change_frequency":
                    data.change_frequency,
                "change_volatility":
                    data.change_volatility,
                "recent_change":
                    data.recent_change,
                "queue_pressure":
                    data.queue_pressure,
                "deadline_pressure":
                    data.deadline_pressure,
                "confidence":
                    data.confidence,
                "requested_resource_units":
                    fair_units,
                "partial":
                    data.partial,
                "orchestration_partial":
                    data.orchestration_partial,
                "metadata":
                    data.metadata,
            }

            results.append(
                self.allocate(
                    source_mapping,
                    capacities,
                )
            )

        return results

    # ----------------------------------------------------------------------
    # CAPACITY RESERVATION MANAGEMENT
    # ----------------------------------------------------------------------

    def release_reservation(
        self,
        allocation_id: str,
        reservation_id: str,
    ) -> Optional[
        FreshnessAwareResourceAllocation
    ]:
        allocation = (
            self.backend.get_allocation(
                allocation_id
            )
        )

        if allocation is None:
            return None

        for reservation in allocation.reservations:
            if (
                reservation.reservation_id
                == reservation_id
            ):
                reservation.state = (
                    ReservationState.RELEASED
                )

        allocation.updated_at = (
            self._now().isoformat()
        )

        self.backend.persist_allocation(
            allocation
        )

        return allocation

    def expire_reservations(
        self,
        allocation_id: str,
    ) -> Optional[
        FreshnessAwareResourceAllocation
    ]:
        allocation = (
            self.backend.get_allocation(
                allocation_id
            )
        )

        if allocation is None:
            return None

        now = self._now()

        for reservation in allocation.reservations:
            try:
                expires = datetime.fromisoformat(
                    reservation.expires_at.replace(
                        "Z",
                        "+00:00",
                    )
                )

                if now >= expires:
                    reservation.state = (
                        ReservationState.EXPIRED
                    )

            except (
                TypeError,
                ValueError,
            ):
                continue

        allocation.updated_at = (
            now.isoformat()
        )

        self.backend.persist_allocation(
            allocation
        )

        return allocation

    # ----------------------------------------------------------------------
    # ARCHITECTURE DESCRIPTION
    # ----------------------------------------------------------------------

    def architecture(
        self,
    ) -> Dict[str, Any]:
        return {
            "phase": PHASE,
            "architecture_version":
                ARCHITECTURE_VERSION,
            "scale_target":
                SCALE_TARGET,
            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,
            "purpose": (
                "Allocate crawler resources according to "
                "freshness importance, urgency, workload "
                "demand, and distributed execution capacity."
            ),
            "inputs": [
                "phase13.6_distributed_recrawl_orchestration",
                "phase13.5_freshness_queue_entries",
                "freshness_signals",
                "recrawl_priority",
                "recrawl_urgency",
                "queue_pressure",
                "deadline_pressure",
                "worker_capacity",
                "worker_health",
                "regional_capacity",
                "zonal_capacity",
                "partition_capacity",
            ],
            "outputs": [
                "freshness_weighted_resource_demand",
                "allocation_priority",
                "resource_allocation_share",
                "worker_capacity_allocation",
                "concurrency_allocation",
                "fetch_capacity_allocation",
                "bandwidth_demand",
                "cpu_demand",
                "memory_demand",
                "admission_decision",
                "capacity_reservations",
                "allocation_checkpoints",
                "allocation_events",
            ],
            "distributed_execution": True,
            "freshness_aware": True,
            "priority_aware": True,
            "urgency_aware": True,
            "queue_pressure_aware": True,
            "deadline_aware": True,
            "capacity_aware": True,
            "worker_health_aware": True,
            "partition_aware": True,
            "shard_aware": True,
            "region_aware": True,
            "zone_aware": True,
            "worker_pool_aware": True,
            "fair_share_supported": True,
            "partial_capacity_supported": True,
            "admission_control_supported": True,
            "reservation_supported": True,
            "reservation_expiration_supported": True,
            "checkpointable":
                self.policy.checkpoint_enabled,
            "restartable": True,
            "deterministic":
                self.policy.deterministic,
            "incremental": True,
            "horizontally_scalable": True,
            "backend_replaceable": True,
            "lineage_preserving": True,
            "provenance_preserving": True,
            "no_fixed_global_resource_limit": True,
            "no_fixed_global_url_limit": True,
            "no_fixed_global_document_limit": True,
            "no_fixed_global_host_limit": True,
            "no_fixed_global_domain_limit": True,
            "no_fixed_global_partition_limit": True,
            "no_fixed_global_shard_limit": True,
            "no_fixed_global_worker_limit": True,
            "no_fixed_global_worker_pool_limit": True,
            "no_fixed_global_region_limit": True,
            "no_fixed_global_zone_limit": True,
            "no_fixed_global_allocation_limit": True,
            "no_google_search_api": True,
            "no_google_index": True,
            "no_google_crawler": True,
            "no_google_infrastructure": True,
            "no_google_ranking_dependency": True,
            "does_not_execute_http_fetch": True,
            "does_not_execute_crawler_workers": True,
            "does_not_parse_web_content": True,
            "does_not_discover_new_urls": True,
            "does_not_mutate_search_index": True,
            "does_not_perform_final_ranking": True,
            "does_not_classify_spam": True,
            "does_not_replace_recrawl_scheduling": True,
            "does_not_replace_adaptive_frequency": True,
            "does_not_replace_freshness_queues": True,
            "does_not_replace_recrawl_orchestration": True,
            "does_not_perform_global_failure_recovery": True,
            "stage_boundaries": {
                "13.1": (
                    "determines freshness attention "
                    "and urgency"
                ),
                "13.2": (
                    "determines content change "
                    "and volatility"
                ),
                "13.3": (
                    "determines recrawl schedule timing"
                ),
                "13.4": (
                    "adapts long-term recrawl frequency"
                ),
                "13.5": (
                    "stores URL/document freshness "
                    "work in durable queues"
                ),
                "13.6": (
                    "orchestrates queued recrawl work "
                    "across distributed infrastructure"
                ),
                "13.7": (
                    "allocates crawler resources according "
                    "to freshness importance"
                ),
                "13.8": (
                    "coordinates global recrawl recovery "
                    "and failure handling"
                ),
                "13.9": (
                    "final freshness and recrawling "
                    "architecture"
                ),
            },
            "next_stage": "13.8",
            "next_stage_name": (
                "Global Recrawl Coordination / Failure Recovery"
            ),
        }

    # ----------------------------------------------------------------------
    # METADATA ACCESS
    # ----------------------------------------------------------------------

    def events(
        self,
    ) -> List[
        FreshnessAwareResourceAllocationEvent
    ]:
        if hasattr(
            self.backend,
            "events",
        ):
            return list(
                getattr(
                    self.backend,
                    "events",
                )()
            )

        return []

    def checkpoints(
        self,
    ) -> List[
        FreshnessAwareResourceAllocationCheckpoint
    ]:
        if hasattr(
            self.backend,
            "checkpoints",
        ):
            return list(
                getattr(
                    self.backend,
                    "checkpoints",
                )()
            )

        return []

    def allocations(
        self,
    ) -> List[
        FreshnessAwareResourceAllocation
    ]:
        if hasattr(
            self.backend,
            "allocations",
        ):
            return list(
                getattr(
                    self.backend,
                    "allocations",
                )()
            )

        return []

    def results(
        self,
    ) -> List[
        FreshnessAwareResourceAllocationResult
    ]:
        if hasattr(
            self.backend,
            "results",
        ):
            return list(
                getattr(
                    self.backend,
                    "results",
                )()
            )

        return []


# ============================================================================
# GLOBAL ALIASES
# ============================================================================


FreshnessAwareCrawlResourceAllocation = (
    FreshnessAwareCrawlResourceAllocationArchitecture
)

GlobalFreshnessAwareCrawlResourceAllocation = (
    FreshnessAwareCrawlResourceAllocationArchitecture
)

Phase13_7FreshnessAwareCrawlResourceAllocation = (
    FreshnessAwareCrawlResourceAllocationArchitecture
)

CrawlResourceAllocation = (
    FreshnessAwareCrawlResourceAllocationArchitecture
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
    "FreshnessResourceAllocationState",
    "ResourceAllocationDecision",
    "AllocationPriorityBand",
    "CrawlResourceType",
    "AllocationScope",
    "ReservationState",
    "WorkloadAdmissionState",
    "AllocationReason",
    "AllocationEventType",
    "AllocationCheckpointType",
    "FreshnessResourceAllocationIdentity",
    "FreshnessResourceAllocationLineage",
    "FreshnessResourceAllocationInput",
    "CrawlResourceCapacity",
    "ResourceDemandEstimate",
    "ResourceAllocationShare",
    "CrawlResourceReservation",
    "FreshnessAwareResourceAllocation",
    "FreshnessAwareResourceAllocationResult",
    "FreshnessAwareResourceAllocationCheckpoint",
    "FreshnessAwareResourceAllocationEvent",
    "FreshnessAwareResourceAllocationPolicy",
    "FreshnessAwareResourceAllocationBackend",
    "InMemoryFreshnessAwareResourceAllocationMetadata",
    "FreshnessAwareCrawlResourceAllocationArchitecture",
    "FreshnessAwareCrawlResourceAllocation",
    "GlobalFreshnessAwareCrawlResourceAllocation",
    "Phase13_7FreshnessAwareCrawlResourceAllocation",
    "CrawlResourceAllocation",
]
