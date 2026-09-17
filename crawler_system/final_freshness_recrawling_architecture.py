"""
OUR SEARCH
Phase 13.9 — Final Freshness + Recrawling Architecture

Purpose
-------
Provide the final integrated production architecture for freshness-aware
recrawling across billions to trillions of publicly accessible Web resources.

This stage integrates the architectural outputs of:

    13.1 Crawl-Freshness Prioritization
    13.2 Change Detection / Content Change Signals
    13.3 Recrawl Scheduling
    13.4 Adaptive Recrawl Frequency
    13.5 URL/Document Freshness Queues
    13.6 Distributed Recrawl Orchestration
    13.7 Freshness-Aware Crawl Resource Allocation
    13.8 Global Recrawl Coordination / Failure Recovery

This module is the final Phase 13 architecture and control-plane contract.

It does NOT:
- perform HTTP requests
- fetch Web resources
- parse Web resources
- execute crawler workers
- directly crawl the Web
- directly mutate the search index
- perform final search ranking
- classify spam
- replace the crawler execution layer
- depend on Google Search
- depend on Google's index
- depend on Google's crawler
- depend on Google's infrastructure
- claim current deployment at Google's scale

Scale target
------------
Designed directly for:

    billions_to_trillions_of_public_web_resources

The architecture avoids imposing a fixed global ceiling on:
- resources
- URLs
- documents
- hosts
- domains
- partitions
- shards
- regions
- workers
- queues
- schedules
- recrawl histories
- coordination records

Per-request and per-partition limits exist only to bound individual
processing units and protect local execution resources.

Architecture principle
----------------------
Phase 13.9 does not replace the previous stages.

It composes their contracts into one final freshness and recrawling
control architecture:

    freshness evidence
            ↓
    change evidence
            ↓
    recrawl priority
            ↓
    recrawl schedule
            ↓
    adaptive frequency
            ↓
    freshness queue
            ↓
    distributed orchestration
            ↓
    resource allocation
            ↓
    global coordination
            ↓
    failure recovery
            ↓
    final recrawl decision

The actual crawler execution remains outside this module.
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

ARCHITECTURE_VERSION = "final-freshness-recrawling-architecture.v1"
PHASE = "13.9"
PREVIOUS_STAGE = "13.8"
NEXT_PHASE = "14"

PHASE_NAME = "Freshness + Recrawling at Enormous Scale"
NEXT_PHASE_NAME = "Spam / Abuse / Security / Quality Systems"


# ============================================================================
# ENUMERATIONS
# ============================================================================


class FinalFreshnessState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    FRESHNESS_ANALYSIS = "freshness_analysis"
    CHANGE_ANALYSIS = "change_analysis"
    PRIORITY_ANALYSIS = "priority_analysis"
    SCHEDULE_ANALYSIS = "schedule_analysis"
    FREQUENCY_ANALYSIS = "frequency_analysis"
    QUEUE_ANALYSIS = "queue_analysis"
    ORCHESTRATION_ANALYSIS = "orchestration_analysis"
    RESOURCE_ANALYSIS = "resource_analysis"
    COORDINATION_ANALYSIS = "coordination_analysis"
    FAILURE_ANALYSIS = "failure_analysis"
    FINAL_DECISION = "final_decision"
    CHECKPOINTING = "checkpointing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class FinalRecrawlDecision(str, Enum):
    NO_ACTION = "no_action"
    DEFER = "defer"
    SCHEDULE = "schedule"
    PRIORITIZE = "prioritize"
    ACCELERATE = "accelerate"
    URGENT = "urgent"
    RECOVER = "recover"
    RETRY = "retry"
    REQUEUE = "requeue"
    REBALANCE = "rebalance"
    RESOURCE_ESCALATION = "resource_escalation"
    COORDINATE = "coordinate"
    DISPATCH = "dispatch"


class FinalFreshnessBand(str, Enum):
    VERY_FRESH = "very_fresh"
    FRESH = "fresh"
    NORMAL = "normal"
    AGING = "aging"
    STALE = "stale"
    VERY_STALE = "very_stale"
    UNKNOWN = "unknown"


class FinalChangeBand(str, Enum):
    STABLE = "stable"
    LOW_ACTIVITY = "low_activity"
    NORMAL_ACTIVITY = "normal_activity"
    ACTIVE = "active"
    HIGH_ACTIVITY = "high_activity"
    VOLATILE = "volatile"
    EXTREMELY_VOLATILE = "extremely_volatile"
    UNKNOWN = "unknown"


class FinalPriorityBand(str, Enum):
    BACKGROUND = "background"
    NORMAL = "normal"
    ELEVATED = "elevated"
    HIGH = "high"
    URGENT = "urgent"
    IMMEDIATE = "immediate"


class FinalQueueLane(str, Enum):
    BACKGROUND = "background"
    NORMAL = "normal"
    PRIORITY = "priority"
    URGENT = "urgent"
    RECOVERY = "recovery"


class FinalExecutionReadiness(str, Enum):
    NOT_READY = "not_ready"
    READY = "ready"
    DISPATCHABLE = "dispatchable"
    RESOURCE_WAIT = "resource_wait"
    COORDINATION_WAIT = "coordination_wait"
    RECOVERY_WAIT = "recovery_wait"


class FinalFailureState(str, Enum):
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


class FinalRecoveryAction(str, Enum):
    NONE = "none"
    RETRY = "retry"
    REQUEUE = "requeue"
    DEFER = "defer"
    RELOCATE = "relocate"
    REBALANCE = "rebalance"
    RESTORE = "restore"
    QUARANTINE = "quarantine"
    CANCEL = "cancel"


class FinalEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    INPUT_NORMALIZED = "input_normalized"

    FRESHNESS_ANALYSIS_STARTED = "freshness_analysis_started"
    CHANGE_ANALYSIS_STARTED = "change_analysis_started"
    PRIORITY_ANALYSIS_STARTED = "priority_analysis_started"
    SCHEDULE_ANALYSIS_STARTED = "schedule_analysis_started"
    FREQUENCY_ANALYSIS_STARTED = "frequency_analysis_started"
    QUEUE_ANALYSIS_STARTED = "queue_analysis_started"
    ORCHESTRATION_ANALYSIS_STARTED = "orchestration_analysis_started"
    RESOURCE_ANALYSIS_STARTED = "resource_analysis_started"
    COORDINATION_ANALYSIS_STARTED = "coordination_analysis_started"
    FAILURE_ANALYSIS_STARTED = "failure_analysis_started"

    FRESHNESS_CLASSIFIED = "freshness_classified"
    CHANGE_CLASSIFIED = "change_classified"
    PRIORITY_CALCULATED = "priority_calculated"
    SCHEDULE_ACCEPTED = "schedule_accepted"
    FREQUENCY_ACCEPTED = "frequency_accepted"
    QUEUE_DECISION_CREATED = "queue_decision_created"
    EXECUTION_READINESS_CALCULATED = "execution_readiness_calculated"
    RESOURCE_DECISION_CREATED = "resource_decision_created"
    COORDINATION_DECISION_CREATED = "coordination_decision_created"
    FAILURE_RECOVERY_DECISION_CREATED = (
        "failure_recovery_decision_created"
    )
    FINAL_DECISION_CREATED = "final_decision_created"

    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"

    FINALIZATION_COMPLETED = "finalization_completed"
    FINALIZATION_DEFERRED = "finalization_deferred"
    FINALIZATION_REJECTED = "finalization_rejected"
    FINALIZATION_FAILED = "finalization_failed"


class FinalCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    FRESHNESS_ANALYZED = "freshness_analyzed"
    CHANGE_ANALYZED = "change_analyzed"
    PRIORITY_ANALYZED = "priority_analyzed"
    SCHEDULE_ANALYZED = "schedule_analyzed"
    FREQUENCY_ANALYZED = "frequency_analyzed"
    QUEUE_ANALYZED = "queue_analyzed"
    ORCHESTRATION_ANALYZED = "orchestration_analyzed"
    RESOURCE_ANALYZED = "resource_analyzed"
    COORDINATION_ANALYZED = "coordination_analyzed"
    FAILURE_ANALYZED = "failure_analyzed"
    FINAL_DECISION_CREATED = "final_decision_created"
    COMPLETED = "completed"


class FreshnessEvidenceSource(str, Enum):
    FRESHNESS_SIGNALS = "freshness_signals"
    CHANGE_SIGNALS = "change_signals"
    RECrawl_HISTORY = "recrawl_history"
    SCHEDULE = "schedule"
    ADAPTIVE_FREQUENCY = "adaptive_frequency"
    QUEUE = "queue"
    ORCHESTRATION = "orchestration"
    RESOURCE_ALLOCATION = "resource_allocation"
    COORDINATION = "coordination"
    FAILURE_RECOVERY = "failure_recovery"


# ============================================================================
# IDENTITY AND LINEAGE
# ============================================================================


@dataclass
class FinalFreshnessIdentity:
    resource_id: str
    document_id: str = ""
    url: str = ""
    host_id: str = ""
    domain_id: str = ""
    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""

    def key(self) -> str:
        return ":".join(
            [
                self.resource_id,
                self.document_id,
                self.partition_id,
                self.shard_id,
                self.region_id,
            ]
        )


@dataclass
class FinalFreshnessLineage:
    resource_id: str

    previous_stage: str = PREVIOUS_STAGE
    current_stage: str = PHASE

    freshness_signal_version: str = ""
    change_signal_version: str = ""
    schedule_version: str = ""
    frequency_version: str = ""
    queue_version: str = ""
    orchestration_version: str = ""
    resource_allocation_version: str = ""
    coordination_version: str = ""
    failure_recovery_version: str = ""

    parent_schedule_ids: List[str] = field(default_factory=list)
    parent_queue_ids: List[str] = field(default_factory=list)
    parent_coordination_ids: List[str] = field(default_factory=list)

    lineage_metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# INTEGRATED INPUT CONTRACTS
# ============================================================================


@dataclass
class FinalFreshnessSignals:
    freshness_score: float = 0.0
    urgency_score: float = 0.0

    document_age_score: float = 0.0
    document_recency_score: float = 0.0
    recent_change_score: float = 0.0

    content_change_score: float = 0.0
    text_change_score: float = 0.0
    structural_change_score: float = 0.0
    metadata_change_score: float = 0.0
    link_change_score: float = 0.0
    media_change_score: float = 0.0
    semantic_change_score: float = 0.0

    change_frequency: float = 0.0
    change_volatility: float = 0.0
    temporal_volatility: float = 0.0
    content_volatility: float = 0.0

    source_freshness: float = 0.0
    query_time_sensitivity: float = 0.0
    event_time_sensitivity: float = 0.0

    feed_update_signal: float = 0.0
    sitemap_update_signal: float = 0.0
    external_update_signal: float = 0.0

    historical_stability: float = 0.0
    prior_freshness_accuracy: float = 0.0

    confidence: float = 1.0
    partial: bool = False
    version: str = ""


@dataclass
class FinalRecrawlHistory:
    total_recrawls: int = 0
    successful_recrawls: int = 0
    failed_recrawls: int = 0

    observed_changes: int = 0
    meaningful_changes: int = 0
    semantic_changes: int = 0
    structural_changes: int = 0
    metadata_changes: int = 0
    link_changes: int = 0
    media_changes: int = 0

    average_change_interval_seconds: float = 0.0
    median_change_interval_seconds: float = 0.0
    minimum_change_interval_seconds: float = 0.0
    maximum_change_interval_seconds: float = 0.0

    average_recrawl_interval_seconds: float = 0.0
    median_recrawl_interval_seconds: float = 0.0

    historical_change_frequency: float = 0.0
    historical_change_volatility: float = 0.0
    historical_stability: float = 0.0

    schedule_accuracy: float = 0.0
    freshness_accuracy: float = 0.0

    missed_deadlines: int = 0
    unnecessary_recrawls: int = 0
    stale_observations: int = 0

    recent_change_count: int = 0
    recent_recrawl_count: int = 0

    last_change_timestamp: str = ""
    last_recrawl_timestamp: str = ""
    first_observed_timestamp: str = ""

    partial: bool = False
    version: str = ""


@dataclass
class FinalRecrawlSchedule:
    schedule_id: str = ""
    resource_id: str = ""

    scheduled_at: str = ""
    due_at: str = ""
    deadline_at: str = ""

    interval_seconds: float = 86400.0

    priority_score: float = 0.0
    priority_band: FinalPriorityBand = FinalPriorityBand.NORMAL

    decision: FinalRecrawlDecision = FinalRecrawlDecision.SCHEDULE

    schedule_type: str = "normal"
    adaptive: bool = False

    reasons: List[str] = field(default_factory=list)

    version: str = ""
    partial: bool = False


@dataclass
class FinalAdaptiveFrequency:
    frequency_id: str = ""
    resource_id: str = ""

    target_interval_seconds: float = 86400.0
    previous_interval_seconds: float = 86400.0

    change_frequency_score: float = 0.0
    volatility_score: float = 0.0
    stability_score: float = 0.0

    adaptation_factor: float = 1.0

    band: FinalChangeBand = FinalChangeBand.NORMAL_ACTIVITY

    decision: str = "maintain"

    reasons: List[str] = field(default_factory=list)

    confidence: float = 1.0
    partial: bool = False

    version: str = ""


@dataclass
class FinalFreshnessQueueEntry:
    queue_entry_id: str = ""
    resource_id: str = ""
    document_id: str = ""
    url: str = ""

    due_at: str = ""
    deadline_at: str = ""

    priority_score: float = 0.0
    priority_band: FinalPriorityBand = FinalPriorityBand.NORMAL
    lane: FinalQueueLane = FinalQueueLane.NORMAL

    state: str = "ready"

    attempt_count: int = 0
    max_attempts: int = 8

    partition_id: str = ""
    shard_id: str = ""
    region_id: str = ""

    deduplication_key: str = ""

    schedule_id: str = ""
    frequency_id: str = ""

    partial: bool = False
    version: str = ""


@dataclass
class FinalOrchestrationState:
    operation_id: str = ""
    resource_id: str = ""

    state: str = "pending"

    assigned_partition_id: str = ""
    assigned_shard_id: str = ""
    assigned_region_id: str = ""

    execution_readiness: FinalExecutionReadiness = (
        FinalExecutionReadiness.NOT_READY
    )

    orchestration_priority: float = 0.0

    retry_count: int = 0
    recovery_count: int = 0

    partial: bool = False
    version: str = ""


@dataclass
class FinalResourceAllocation:
    allocation_id: str = ""
    resource_id: str = ""

    allocation_score: float = 0.0

    worker_capacity_score: float = 0.0
    network_capacity_score: float = 0.0
    host_capacity_score: float = 0.0
    partition_capacity_score: float = 0.0
    region_capacity_score: float = 0.0

    rate_limit_headroom: float = 0.0
    concurrency_headroom: float = 0.0

    decision: str = "maintain"
    escalation_required: bool = False

    assigned_partition_id: str = ""
    assigned_region_id: str = ""

    partial: bool = False
    version: str = ""


@dataclass
class FinalCoordinationState:
    coordination_id: str = ""
    resource_id: str = ""

    coordination_decision: str = "coordinate"

    partition_healthy: bool = True
    shard_healthy: bool = True
    region_healthy: bool = True
    queue_healthy: bool = True

    partition_health_score: float = 1.0
    region_health_score: float = 1.0

    rebalance_required: bool = False
    relocation_required: bool = False

    recovery_required: bool = False

    partial: bool = False
    version: str = ""


@dataclass
class FinalFailureRecovery:
    recovery_id: str = ""
    resource_id: str = ""

    failure_state: FinalFailureState = FinalFailureState.NONE
    failure_score: float = 0.0

    recovery_action: FinalRecoveryAction = FinalRecoveryAction.NONE

    retry_attempt: int = 0
    max_retry_attempts: int = 8

    retry_delay_seconds: float = 0.0
    retry_at: str = ""

    recovery_partition_id: str = ""
    recovery_region_id: str = ""

    quarantine: bool = False
    cancelled: bool = False

    reasons: List[str] = field(default_factory=list)

    partial: bool = False
    version: str = ""


# ============================================================================
# INTEGRATED INPUT
# ============================================================================


@dataclass
class FinalFreshnessInput:
    identity: FinalFreshnessIdentity

    freshness_signals: FinalFreshnessSignals = field(
        default_factory=FinalFreshnessSignals
    )

    history: FinalRecrawlHistory = field(
        default_factory=FinalRecrawlHistory
    )

    schedule: Optional[FinalRecrawlSchedule] = None
    adaptive_frequency: Optional[FinalAdaptiveFrequency] = None
    queue_entry: Optional[FinalFreshnessQueueEntry] = None
    orchestration: Optional[FinalOrchestrationState] = None
    resource_allocation: Optional[FinalResourceAllocation] = None
    coordination: Optional[FinalCoordinationState] = None
    failure_recovery: Optional[FinalFailureRecovery] = None

    source_evidence: List[FreshnessEvidenceSource] = field(
        default_factory=list
    )

    lineage: Optional[FinalFreshnessLineage] = None

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# POLICY
# ============================================================================


@dataclass
class FinalFreshnessRecrawlingPolicy:
    max_resources_per_batch: int = 100000

    minimum_confidence: float = 0.10

    allow_partial: bool = True

    deterministic: bool = True

    checkpoint_enabled: bool = True

    minimum_interval_seconds: float = 60.0
    default_interval_seconds: float = 86400.0
    maximum_interval_seconds: float = 365.0 * 86400.0

    stale_threshold: float = 0.30
    very_stale_threshold: float = 0.10
    fresh_threshold: float = 0.70
    very_fresh_threshold: float = 0.90

    stable_change_threshold: float = 0.15
    low_activity_threshold: float = 0.30
    active_threshold: float = 0.60
    high_activity_threshold: float = 0.80
    volatile_threshold: float = 0.90

    background_priority_threshold: float = 0.15
    elevated_priority_threshold: float = 0.45
    high_priority_threshold: float = 0.70
    urgent_priority_threshold: float = 0.90
    immediate_priority_threshold: float = 0.97

    freshness_weight: float = 1.25
    urgency_weight: float = 1.50
    change_frequency_weight: float = 1.25
    change_volatility_weight: float = 1.20
    recent_change_weight: float = 1.35
    semantic_change_weight: float = 1.15
    temporal_sensitivity_weight: float = 1.00
    source_freshness_weight: float = 0.85
    historical_stability_weight: float = 0.75
    missed_deadline_weight: float = 1.25

    schedule_weight: float = 1.00
    adaptive_frequency_weight: float = 1.00
    queue_weight: float = 0.90
    orchestration_weight: float = 0.90
    resource_weight: float = 0.85
    coordination_weight: float = 1.10
    recovery_weight: float = 1.30

    partial_penalty: float = 0.05

    maximum_retry_attempts: int = 8
    retry_base_delay_seconds: float = 30.0
    retry_max_delay_seconds: float = 86400.0
    retry_jitter_fraction: float = 0.10

    minimum_partition_health: float = 0.40
    minimum_region_health: float = 0.40

    resource_wait_threshold: float = 0.30
    coordination_wait_threshold: float = 0.40

    escalation_threshold: float = 0.80

    immediate_max_interval_seconds: float = 300.0
    urgent_max_interval_seconds: float = 3600.0
    high_max_interval_seconds: float = 21600.0

    checkpoint_retention_hint: int = 100


# ============================================================================
# BACKEND
# ============================================================================


class FinalFreshnessRecrawlingBackend(Protocol):
    def persist_event(self, event: "FinalFreshnessEvent") -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: "FinalFreshnessCheckpoint",
    ) -> None:
        ...

    def persist_result(
        self,
        result: "FinalFreshnessResult",
    ) -> None:
        ...

    def get_result(
        self,
        result_id: str,
    ) -> Optional["FinalFreshnessResult"]:
        ...


class InMemoryFinalFreshnessRecrawlingMetadata:
    def __init__(self) -> None:
        self._events: List[FinalFreshnessEvent] = []
        self._checkpoints: List[FinalFreshnessCheckpoint] = []
        self._results: Dict[str, FinalFreshnessResult] = {}

    def persist_event(
        self,
        event: "FinalFreshnessEvent",
    ) -> None:
        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: "FinalFreshnessCheckpoint",
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_result(
        self,
        result: "FinalFreshnessResult",
    ) -> None:
        self._results[result.result_id] = result

    def get_result(
        self,
        result_id: str,
    ) -> Optional["FinalFreshnessResult"]:
        return self._results.get(result_id)

    def events(self) -> List["FinalFreshnessEvent"]:
        return list(self._events)

    def checkpoints(self) -> List["FinalFreshnessCheckpoint"]:
        return list(self._checkpoints)

    def results(self) -> Dict[str, "FinalFreshnessResult"]:
        return dict(self._results)


# ============================================================================
# FINAL OUTPUT CONTRACTS
# ============================================================================


@dataclass
class FinalFreshnessDecision:
    decision: FinalRecrawlDecision
    freshness_band: FinalFreshnessBand
    change_band: FinalChangeBand
    priority_band: FinalPriorityBand

    priority_score: float

    execution_readiness: FinalExecutionReadiness

    queue_lane: FinalQueueLane

    scheduled_at: str
    due_at: str
    deadline_at: str

    interval_seconds: float

    recovery_action: FinalRecoveryAction

    resource_escalation_required: bool
    coordination_required: bool

    reasons: List[str] = field(default_factory=list)

    confidence: float = 1.0
    partial: bool = False


@dataclass
class FinalFreshnessResult:
    result_id: str
    identity: FinalFreshnessIdentity

    state: FinalFreshnessState

    decision: FinalFreshnessDecision

    schedule: Optional[FinalRecrawlSchedule] = None
    adaptive_frequency: Optional[FinalAdaptiveFrequency] = None
    queue_entry: Optional[FinalFreshnessQueueEntry] = None
    orchestration: Optional[FinalOrchestrationState] = None
    resource_allocation: Optional[FinalResourceAllocation] = None
    coordination: Optional[FinalCoordinationState] = None
    failure_recovery: Optional[FinalFailureRecovery] = None

    lineage: Optional[FinalFreshnessLineage] = None

    evidence_sources: List[FreshnessEvidenceSource] = field(
        default_factory=list
    )

    created_at: str = ""

    partial: bool = False

    architecture_version: str = ARCHITECTURE_VERSION


@dataclass
class FinalFreshnessCheckpoint:
    checkpoint_id: str
    result_id: str
    resource_id: str

    checkpoint_type: FinalCheckpointType
    state: FinalFreshnessState

    created_at: str

    payload_digest: str = ""

    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FinalFreshnessEvent:
    event_id: str
    result_id: str
    resource_id: str

    event_type: FinalEventType

    state: FinalFreshnessState

    created_at: str

    payload: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# ARCHITECTURE
# ============================================================================


class FinalFreshnessRecrawlingArchitecture:
    """
    Final Phase 13.9 integrated freshness and recrawling control architecture.

    This class is intentionally a control-plane architecture.

    It produces decisions and durable contracts.

    It does not execute crawling.
    """

    def __init__(
        self,
        backend: Optional[FinalFreshnessRecrawlingBackend] = None,
        policy: Optional[FinalFreshnessRecrawlingPolicy] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryFinalFreshnessRecrawlingMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else FinalFreshnessRecrawlingPolicy()
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
            return minimum

        if not math.isfinite(value):
            return minimum

        return max(minimum, min(maximum, value))

    @staticmethod
    def _safe_float(
        value: Any,
        default: float = 0.0,
    ) -> float:
        try:
            result = float(value)

            if not math.isfinite(result):
                return default

            return result
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
        value: str,
    ) -> Optional[datetime]:
        if not value:
            return None

        try:
            normalized = value.strip()

            if normalized.endswith("Z"):
                normalized = normalized[:-1] + "+00:00"

            parsed = datetime.fromisoformat(normalized)

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            return parsed.astimezone(timezone.utc)

        except (TypeError, ValueError):
            return None

    @staticmethod
    def _deterministic_unit(
        *parts: str,
    ) -> float:
        payload = "|".join(str(part) for part in parts)

        digest = hashlib.sha256(
            payload.encode("utf-8")
        ).digest()

        integer = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return integer / float(2**64 - 1)

    @staticmethod
    def _digest_payload(
        payload: Mapping[str, Any],
    ) -> str:
        normalized = repr(
            sorted(
                (
                    str(key),
                    repr(value),
                )
                for key, value in payload.items()
            )
        )

        return hashlib.sha256(
            normalized.encode("utf-8")
        ).hexdigest()

    # ----------------------------------------------------------------------
    # EVENT / CHECKPOINT
    # ----------------------------------------------------------------------

    def _event(
        self,
        result_id: str,
        resource_id: str,
        event_type: FinalEventType,
        state: FinalFreshnessState,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> FinalFreshnessEvent:
        event = FinalFreshnessEvent(
            event_id=hashlib.sha256(
                f"{result_id}:{resource_id}:{event_type.value}:"
                f"{self._now().timestamp()}".encode()
            ).hexdigest(),
            result_id=result_id,
            resource_id=resource_id,
            event_type=event_type,
            state=state,
            created_at=self._now().isoformat(),
            payload=dict(payload or {}),
        )

        self.backend.persist_event(event)

        return event

    def _checkpoint(
        self,
        result_id: str,
        resource_id: str,
        checkpoint_type: FinalCheckpointType,
        state: FinalFreshnessState,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> FinalFreshnessCheckpoint:
        payload_dict = dict(payload or {})

        checkpoint = FinalFreshnessCheckpoint(
            checkpoint_id=hashlib.sha256(
                f"{result_id}:{resource_id}:{checkpoint_type.value}:"
                f"{self._now().timestamp()}".encode()
            ).hexdigest(),
            result_id=result_id,
            resource_id=resource_id,
            checkpoint_type=checkpoint_type,
            state=state,
            created_at=self._now().isoformat(),
            payload_digest=self._digest_payload(payload_dict),
            metadata=payload_dict,
        )

        self.backend.persist_checkpoint(checkpoint)

        self._event(
            result_id=result_id,
            resource_id=resource_id,
            event_type=FinalEventType.CHECKPOINT_CREATED,
            state=state,
            payload={
                "checkpoint_id": checkpoint.checkpoint_id,
                "checkpoint_type": checkpoint_type.value,
            },
        )

        return checkpoint

    # ----------------------------------------------------------------------
    # INPUT NORMALIZATION
    # ----------------------------------------------------------------------

    def _normalize_identity(
        self,
        identity: FinalFreshnessIdentity,
    ) -> FinalFreshnessIdentity:
        return FinalFreshnessIdentity(
            resource_id=str(identity.resource_id or "").strip(),
            document_id=str(identity.document_id or "").strip(),
            url=str(identity.url or "").strip(),
            host_id=str(identity.host_id or "").strip(),
            domain_id=str(identity.domain_id or "").strip(),
            partition_id=str(identity.partition_id or "").strip(),
            shard_id=str(identity.shard_id or "").strip(),
            region_id=str(identity.region_id or "").strip(),
        )

    def _normalize_signals(
        self,
        signals: FinalFreshnessSignals,
    ) -> FinalFreshnessSignals:
        numeric_fields = [
            "freshness_score",
            "urgency_score",
            "document_age_score",
            "document_recency_score",
            "recent_change_score",
            "content_change_score",
            "text_change_score",
            "structural_change_score",
            "metadata_change_score",
            "link_change_score",
            "media_change_score",
            "semantic_change_score",
            "change_frequency",
            "change_volatility",
            "temporal_volatility",
            "content_volatility",
            "source_freshness",
            "query_time_sensitivity",
            "event_time_sensitivity",
            "feed_update_signal",
            "sitemap_update_signal",
            "external_update_signal",
            "historical_stability",
            "prior_freshness_accuracy",
            "confidence",
        ]

        values: Dict[str, Any] = {}

        for field_name in numeric_fields:
            value = getattr(signals, field_name, 0.0)

            values[field_name] = self._clamp(
                self._safe_float(value)
            )

        return FinalFreshnessSignals(
            **values,
            partial=bool(signals.partial),
            version=str(signals.version or ""),
        )

    def _normalize_history(
        self,
        history: FinalRecrawlHistory,
    ) -> FinalRecrawlHistory:
        integer_fields = [
            "total_recrawls",
            "successful_recrawls",
            "failed_recrawls",
            "observed_changes",
            "meaningful_changes",
            "semantic_changes",
            "structural_changes",
            "metadata_changes",
            "link_changes",
            "media_changes",
            "missed_deadlines",
            "unnecessary_recrawls",
            "stale_observations",
            "recent_change_count",
            "recent_recrawl_count",
        ]

        float_fields = [
            "average_change_interval_seconds",
            "median_change_interval_seconds",
            "minimum_change_interval_seconds",
            "maximum_change_interval_seconds",
            "average_recrawl_interval_seconds",
            "median_recrawl_interval_seconds",
            "historical_change_frequency",
            "historical_change_volatility",
            "historical_stability",
            "schedule_accuracy",
            "freshness_accuracy",
        ]

        values: Dict[str, Any] = {}

        for field_name in integer_fields:
            values[field_name] = max(
                0,
                self._safe_int(
                    getattr(history, field_name, 0)
                ),
            )

        for field_name in float_fields:
            values[field_name] = self._clamp(
                self._safe_float(
                    getattr(history, field_name, 0.0)
                )
            )

        return FinalRecrawlHistory(
            **values,
            last_change_timestamp=str(
                history.last_change_timestamp or ""
            ),
            last_recrawl_timestamp=str(
                history.last_recrawl_timestamp or ""
            ),
            first_observed_timestamp=str(
                history.first_observed_timestamp or ""
            ),
            partial=bool(history.partial),
            version=str(history.version or ""),
        )

    def _normalize_input(
        self,
        item: FinalFreshnessInput,
    ) -> FinalFreshnessInput:
        identity = self._normalize_identity(item.identity)
        signals = self._normalize_signals(item.freshness_signals)
        history = self._normalize_history(item.history)

        lineage = item.lineage

        if lineage is None:
            lineage = FinalFreshnessLineage(
                resource_id=identity.resource_id
            )

        return FinalFreshnessInput(
            identity=identity,
            freshness_signals=signals,
            history=history,
            schedule=item.schedule,
            adaptive_frequency=item.adaptive_frequency,
            queue_entry=item.queue_entry,
            orchestration=item.orchestration,
            resource_allocation=item.resource_allocation,
            coordination=item.coordination,
            failure_recovery=item.failure_recovery,
            source_evidence=list(item.source_evidence),
            lineage=lineage,
            metadata=dict(item.metadata),
        )

    # ----------------------------------------------------------------------
    # VALIDATION
    # ----------------------------------------------------------------------

    def validate(
        self,
        item: FinalFreshnessInput,
    ) -> Tuple[bool, List[str]]:
        errors: List[str] = []

        if not item.identity.resource_id:
            errors.append("resource_id is required")

        if item.freshness_signals.confidence < 0.0:
            errors.append("freshness confidence cannot be negative")

        if item.freshness_signals.confidence > 1.0:
            errors.append("freshness confidence cannot exceed 1")

        if item.history.total_recrawls < 0:
            errors.append("total_recrawls cannot be negative")

        if item.history.failed_recrawls < 0:
            errors.append("failed_recrawls cannot be negative")

        return not errors, errors

    # ----------------------------------------------------------------------
    # FRESHNESS CLASSIFICATION
    # ----------------------------------------------------------------------

    def _freshness_band(
        self,
        signals: FinalFreshnessSignals,
    ) -> FinalFreshnessBand:
        score = self._clamp(
            signals.freshness_score
        )

        if score >= self.policy.very_fresh_threshold:
            return FinalFreshnessBand.VERY_FRESH

        if score >= self.policy.fresh_threshold:
            return FinalFreshnessBand.FRESH

        if score <= self.policy.very_stale_threshold:
            return FinalFreshnessBand.VERY_STALE

        if score <= self.policy.stale_threshold:
            return FinalFreshnessBand.STALE

        if score < 0.50:
            return FinalFreshnessBand.AGING

        return FinalFreshnessBand.NORMAL

    def _change_band(
        self,
        signals: FinalFreshnessSignals,
        history: FinalRecrawlHistory,
    ) -> FinalChangeBand:
        frequency = self._clamp(
            0.50 * signals.change_frequency
            + 0.50 * history.historical_change_frequency
        )

        volatility = self._clamp(
            0.50 * signals.change_volatility
            + 0.50 * history.historical_change_volatility
        )

        activity = self._clamp(
            0.70 * frequency
            + 0.30 * signals.recent_change_score
        )

        if volatility >= self.policy.volatile_threshold:
            if activity >= self.policy.high_activity_threshold:
                return FinalChangeBand.EXTREMELY_VOLATILE

            return FinalChangeBand.VOLATILE

        if activity >= self.policy.high_activity_threshold:
            return FinalChangeBand.HIGH_ACTIVITY

        if activity >= self.policy.active_threshold:
            return FinalChangeBand.ACTIVE

        if activity >= self.policy.low_activity_threshold:
            return FinalChangeBand.NORMAL_ACTIVITY

        if activity > self.policy.stable_change_threshold:
            return FinalChangeBand.LOW_ACTIVITY

        return FinalChangeBand.STABLE

    # ----------------------------------------------------------------------
    # PRIORITY
    # ----------------------------------------------------------------------

    def _priority_score(
        self,
        signals: FinalFreshnessSignals,
        history: FinalRecrawlHistory,
    ) -> float:
        score = 0.0
        weight = 0.0

        components = [
            (
                signals.freshness_score,
                self.policy.freshness_weight,
            ),
            (
                signals.urgency_score,
                self.policy.urgency_weight,
            ),
            (
                signals.change_frequency,
                self.policy.change_frequency_weight,
            ),
            (
                signals.change_volatility,
                self.policy.change_volatility_weight,
            ),
            (
                signals.recent_change_score,
                self.policy.recent_change_weight,
            ),
            (
                signals.semantic_change_score,
                self.policy.semantic_change_weight,
            ),
            (
                max(
                    signals.query_time_sensitivity,
                    signals.event_time_sensitivity,
                ),
                self.policy.temporal_sensitivity_weight,
            ),
            (
                signals.source_freshness,
                self.policy.source_freshness_weight,
            ),
        ]

        for value, component_weight in components:
            score += (
                self._clamp(value)
                * component_weight
            )
            weight += component_weight

        if history.missed_deadlines > 0:
            missed_boost = self._clamp(
                history.missed_deadlines / 10.0
            )

            score += (
                missed_boost
                * self.policy.missed_deadline_weight
            )

            weight += self.policy.missed_deadline_weight

        stability = self._clamp(
            history.historical_stability
        )

        score += (
            (1.0 - stability)
            * self.policy.historical_stability_weight
        )

        weight += self.policy.historical_stability_weight

        if weight <= 0.0:
            return 0.0

        return self._clamp(
            score / weight
        )

    def _priority_band(
        self,
        score: float,
    ) -> FinalPriorityBand:
        score = self._clamp(score)

        if score >= self.policy.immediate_priority_threshold:
            return FinalPriorityBand.IMMEDIATE

        if score >= self.policy.urgent_priority_threshold:
            return FinalPriorityBand.URGENT

        if score >= self.policy.high_priority_threshold:
            return FinalPriorityBand.HIGH

        if score >= self.policy.elevated_priority_threshold:
            return FinalPriorityBand.ELEVATED

        if score >= self.policy.background_priority_threshold:
            return FinalPriorityBand.NORMAL

        return FinalPriorityBand.BACKGROUND

    # ----------------------------------------------------------------------
    # SCHEDULE INTEGRATION
    # ----------------------------------------------------------------------

    def _schedule_interval(
        self,
        priority_band: FinalPriorityBand,
        adaptive_frequency: Optional[FinalAdaptiveFrequency],
        schedule: Optional[FinalRecrawlSchedule],
    ) -> float:
        if adaptive_frequency is not None:
            interval = self._safe_float(
                adaptive_frequency.target_interval_seconds,
                self.policy.default_interval_seconds,
            )
        elif schedule is not None:
            interval = self._safe_float(
                schedule.interval_seconds,
                self.policy.default_interval_seconds,
            )
        else:
            interval = self.policy.default_interval_seconds

        if priority_band == FinalPriorityBand.IMMEDIATE:
            interval = min(
                interval,
                self.policy.immediate_max_interval_seconds,
            )

        elif priority_band == FinalPriorityBand.URGENT:
            interval = min(
                interval,
                self.policy.urgent_max_interval_seconds,
            )

        elif priority_band == FinalPriorityBand.HIGH:
            interval = min(
                interval,
                self.policy.high_max_interval_seconds,
            )

        return max(
            self.policy.minimum_interval_seconds,
            min(
                interval,
                self.policy.maximum_interval_seconds,
            ),
        )

    def _schedule_times(
        self,
        interval_seconds: float,
        priority_band: FinalPriorityBand,
        existing_schedule: Optional[FinalRecrawlSchedule],
    ) -> Tuple[str, str, str]:
        now = self._now()

        scheduled_at = now

        if existing_schedule is not None:
            due_existing = self._parse_timestamp(
                existing_schedule.due_at
            )

            if due_existing is not None:
                scheduled_at = min(
                    now,
                    due_existing,
                )

        due_at = scheduled_at + timedelta(
            seconds=interval_seconds
        )

        if priority_band == FinalPriorityBand.IMMEDIATE:
            deadline_delta = timedelta(
                seconds=max(
                    60.0,
                    interval_seconds * 1.25,
                )
            )

        elif priority_band == FinalPriorityBand.URGENT:
            deadline_delta = timedelta(
                seconds=max(
                    300.0,
                    interval_seconds * 1.50,
                )
            )

        elif priority_band == FinalPriorityBand.HIGH:
            deadline_delta = timedelta(
                seconds=max(
                    1800.0,
                    interval_seconds * 2.0,
                )
            )

        else:
            deadline_delta = timedelta(
                seconds=max(
                    3600.0,
                    interval_seconds * 2.5,
                )
            )

        deadline_at = due_at + deadline_delta

        return (
            scheduled_at.isoformat(),
            due_at.isoformat(),
            deadline_at.isoformat(),
        )

    # ----------------------------------------------------------------------
    # QUEUE LANE
    # ----------------------------------------------------------------------

    def _queue_lane(
        self,
        priority_band: FinalPriorityBand,
        failure: Optional[FinalFailureRecovery],
    ) -> FinalQueueLane:
        if failure is not None:
            if (
                failure.recovery_action
                in {
                    FinalRecoveryAction.RETRY,
                    FinalRecoveryAction.REQUEUE,
                    FinalRecoveryAction.RESTORE,
                    FinalRecoveryAction.REBALANCE,
                    FinalRecoveryAction.RELOCATE,
                }
            ):
                return FinalQueueLane.RECOVERY

        mapping = {
            FinalPriorityBand.BACKGROUND:
                FinalQueueLane.BACKGROUND,
            FinalPriorityBand.NORMAL:
                FinalQueueLane.NORMAL,
            FinalPriorityBand.ELEVATED:
                FinalQueueLane.PRIORITY,
            FinalPriorityBand.HIGH:
                FinalQueueLane.PRIORITY,
            FinalPriorityBand.URGENT:
                FinalQueueLane.URGENT,
            FinalPriorityBand.IMMEDIATE:
                FinalQueueLane.URGENT,
        }

        return mapping[priority_band]

    # ----------------------------------------------------------------------
    # RESOURCE READINESS
    # ----------------------------------------------------------------------

    def _execution_readiness(
        self,
        resource_allocation: Optional[FinalResourceAllocation],
        coordination: Optional[FinalCoordinationState],
        failure: Optional[FinalFailureRecovery],
    ) -> FinalExecutionReadiness:
        if failure is not None:
            if failure.cancelled or failure.quarantine:
                return FinalExecutionReadiness.NOT_READY

            if failure.recovery_action != FinalRecoveryAction.NONE:
                return FinalExecutionReadiness.RECOVERY_WAIT

        if coordination is not None:
            if (
                not coordination.partition_healthy
                or not coordination.region_healthy
                or not coordination.queue_healthy
            ):
                return FinalExecutionReadiness.COORDINATION_WAIT

            if (
                coordination.rebalance_required
                or coordination.relocation_required
            ):
                return FinalExecutionReadiness.COORDINATION_WAIT

        if resource_allocation is not None:
            capacity = self._clamp(
                resource_allocation.allocation_score
            )

            if capacity < self.policy.resource_wait_threshold:
                return FinalExecutionReadiness.RESOURCE_WAIT

            if resource_allocation.escalation_required:
                return FinalExecutionReadiness.RESOURCE_WAIT

        return FinalExecutionReadiness.DISPATCHABLE

    # ----------------------------------------------------------------------
    # FAILURE INTEGRATION
    # ----------------------------------------------------------------------

    def _failure_score(
        self,
        failure: Optional[FinalFailureRecovery],
    ) -> float:
        if failure is None:
            return 0.0

        return self._clamp(
            failure.failure_score
        )

    def _recovery_action(
        self,
        failure: Optional[FinalFailureRecovery],
    ) -> FinalRecoveryAction:
        if failure is None:
            return FinalRecoveryAction.NONE

        return failure.recovery_action

    # ----------------------------------------------------------------------
    # RESOURCE ESCALATION
    # ----------------------------------------------------------------------

    def _resource_escalation(
        self,
        resource_allocation: Optional[FinalResourceAllocation],
        priority_score: float,
    ) -> bool:
        if resource_allocation is None:
            return (
                priority_score
                >= self.policy.escalation_threshold
            )

        if resource_allocation.escalation_required:
            return True

        return (
            resource_allocation.allocation_score
            >= self.policy.escalation_threshold
            and priority_score
            >= self.policy.high_priority_threshold
        )

    # ----------------------------------------------------------------------
    # COORDINATION DECISION
    # ----------------------------------------------------------------------

    def _coordination_required(
        self,
        coordination: Optional[FinalCoordinationState],
        execution_readiness: FinalExecutionReadiness,
    ) -> bool:
        if coordination is None:
            return False

        if execution_readiness == FinalExecutionReadiness.COORDINATION_WAIT:
            return True

        if coordination.rebalance_required:
            return True

        if coordination.relocation_required:
            return True

        if coordination.recovery_required:
            return True

        return False

    # ----------------------------------------------------------------------
    # FINAL DECISION
    # ----------------------------------------------------------------------

    def _final_decision(
        self,
        priority_band: FinalPriorityBand,
        readiness: FinalExecutionReadiness,
        recovery_action: FinalRecoveryAction,
        resource_escalation: bool,
        coordination_required: bool,
        freshness_band: FinalFreshnessBand,
        existing_schedule: Optional[FinalRecrawlSchedule],
    ) -> FinalRecrawlDecision:
        if recovery_action == FinalRecoveryAction.QUARANTINE:
            return FinalRecrawlDecision.DEFER

        if recovery_action == FinalRecoveryAction.CANCEL:
            return FinalRecrawlDecision.NO_ACTION

        if recovery_action == FinalRecoveryAction.RESTORE:
            return FinalRecrawlDecision.RECOVER

        if recovery_action == FinalRecoveryAction.RELOCATE:
            return FinalRecrawlDecision.REBALANCE

        if recovery_action == FinalRecoveryAction.REBALANCE:
            return FinalRecrawlDecision.REBALANCE

        if recovery_action == FinalRecoveryAction.RETRY:
            return FinalRecrawlDecision.RETRY

        if recovery_action == FinalRecoveryAction.REQUEUE:
            return FinalRecrawlDecision.REQUEUE

        if readiness == FinalExecutionReadiness.COORDINATION_WAIT:
            return FinalRecrawlDecision.COORDINATE

        if readiness == FinalExecutionReadiness.RESOURCE_WAIT:
            return FinalRecrawlDecision.RESOURCE_ESCALATION

        if readiness == FinalExecutionReadiness.RECOVERY_WAIT:
            return FinalRecrawlDecision.RECOVER

        if priority_band == FinalPriorityBand.IMMEDIATE:
            return FinalRecrawlDecision.URGENT

        if priority_band == FinalPriorityBand.URGENT:
            return FinalRecrawlDecision.URGENT

        if priority_band == FinalPriorityBand.HIGH:
            return FinalRecrawlDecision.ACCELERATE

        if freshness_band in {
            FinalFreshnessBand.STALE,
            FinalFreshnessBand.VERY_STALE,
        }:
            return FinalRecrawlDecision.PRIORITIZE

        if existing_schedule is None:
            return FinalRecrawlDecision.SCHEDULE

        if resource_escalation:
            return FinalRecrawlDecision.RESOURCE_ESCALATION

        if coordination_required:
            return FinalRecrawlDecision.COORDINATE

        return FinalRecrawlDecision.NO_ACTION

    # ----------------------------------------------------------------------
    # REASONS
    # ----------------------------------------------------------------------

    def _reasons(
        self,
        freshness_band: FinalFreshnessBand,
        change_band: FinalChangeBand,
        priority_band: FinalPriorityBand,
        signals: FinalFreshnessSignals,
        history: FinalRecrawlHistory,
        failure: Optional[FinalFailureRecovery],
        coordination: Optional[FinalCoordinationState],
        resource_allocation: Optional[FinalResourceAllocation],
    ) -> List[str]:
        reasons: List[str] = []

        if freshness_band in {
            FinalFreshnessBand.STALE,
            FinalFreshnessBand.VERY_STALE,
        }:
            reasons.append("resource_requires_freshness_attention")

        if change_band in {
            FinalChangeBand.HIGH_ACTIVITY,
            FinalChangeBand.VOLATILE,
            FinalChangeBand.EXTREMELY_VOLATILE,
        }:
            reasons.append("resource_has_high_change_activity")

        if signals.recent_change_score >= 0.70:
            reasons.append("recent_content_change_detected")

        if signals.semantic_change_score >= 0.70:
            reasons.append("semantic_change_activity_detected")

        if signals.query_time_sensitivity >= 0.70:
            reasons.append("query_time_sensitivity_high")

        if signals.event_time_sensitivity >= 0.70:
            reasons.append("event_time_sensitivity_high")

        if signals.feed_update_signal >= 0.70:
            reasons.append("feed_update_signal")

        if signals.sitemap_update_signal >= 0.70:
            reasons.append("sitemap_update_signal")

        if signals.external_update_signal >= 0.70:
            reasons.append("external_update_signal")

        if history.missed_deadlines > 0:
            reasons.append("missed_recrawl_deadline")

        if history.stale_observations > 0:
            reasons.append("historical_stale_observations")

        if priority_band in {
            FinalPriorityBand.URGENT,
            FinalPriorityBand.IMMEDIATE,
        }:
            reasons.append("high_recrawl_priority")

        if failure is not None:
            reasons.extend(
                failure.reasons
            )

        if coordination is not None:
            if coordination.rebalance_required:
                reasons.append("global_rebalance_required")

            if coordination.relocation_required:
                reasons.append("coordination_relocation_required")

            if coordination.recovery_required:
                reasons.append("coordination_recovery_required")

        if resource_allocation is not None:
            if resource_allocation.escalation_required:
                reasons.append("crawl_resource_escalation_required")

        if not reasons:
            reasons.append("normal_recrawl_control")

        return list(
            dict.fromkeys(reasons)
        )

    # ----------------------------------------------------------------------
    # CONFIDENCE
    # ----------------------------------------------------------------------

    def _confidence(
        self,
        signals: FinalFreshnessSignals,
        history: FinalRecrawlHistory,
        item: FinalFreshnessInput,
    ) -> float:
        values = [
            self._clamp(signals.confidence),
            self._clamp(signals.prior_freshness_accuracy),
            self._clamp(history.freshness_accuracy),
            self._clamp(history.schedule_accuracy),
        ]

        evidence_count = len(
            item.source_evidence
        )

        if evidence_count > 0:
            values.append(
                self._clamp(
                    min(
                        1.0,
                        evidence_count / 4.0,
                    )
                )
            )

        confidence = sum(values) / len(values)

        if signals.partial or history.partial:
            confidence *= (
                1.0 - self.policy.partial_penalty
            )

        return self._clamp(
            confidence
        )

    # ----------------------------------------------------------------------
    # QUEUE ENTRY CONSTRUCTION
    # ----------------------------------------------------------------------

    def _build_queue_entry(
        self,
        item: FinalFreshnessInput,
        result_id: str,
        priority_score: float,
        priority_band: FinalPriorityBand,
        lane: FinalQueueLane,
        due_at: str,
        deadline_at: str,
    ) -> FinalFreshnessQueueEntry:
        identity = item.identity

        deduplication_key = hashlib.sha256(
            f"{identity.resource_id}|"
            f"{identity.document_id}|"
            f"{identity.url}".encode(
                "utf-8"
            )
        ).hexdigest()

        schedule_id = (
            item.schedule.schedule_id
            if item.schedule is not None
            else ""
        )

        frequency_id = (
            item.adaptive_frequency.frequency_id
            if item.adaptive_frequency is not None
            else ""
        )

        return FinalFreshnessQueueEntry(
            queue_entry_id=hashlib.sha256(
                f"{result_id}:queue".encode(
                    "utf-8"
                )
            ).hexdigest(),
            resource_id=identity.resource_id,
            document_id=identity.document_id,
            url=identity.url,
            due_at=due_at,
            deadline_at=deadline_at,
            priority_score=priority_score,
            priority_band=priority_band,
            lane=lane,
            state="dispatchable",
            attempt_count=(
                item.queue_entry.attempt_count
                if item.queue_entry is not None
                else 0
            ),
            max_attempts=self.policy.maximum_retry_attempts,
            partition_id=identity.partition_id,
            shard_id=identity.shard_id,
            region_id=identity.region_id,
            deduplication_key=deduplication_key,
            schedule_id=schedule_id,
            frequency_id=frequency_id,
            partial=(
                item.freshness_signals.partial
                or item.history.partial
            ),
            version=ARCHITECTURE_VERSION,
        )

    # ----------------------------------------------------------------------
    # SCHEDULE CONSTRUCTION
    # ----------------------------------------------------------------------

    def _build_schedule(
        self,
        item: FinalFreshnessInput,
        result_id: str,
        priority_score: float,
        priority_band: FinalPriorityBand,
        interval_seconds: float,
        due_at: str,
        deadline_at: str,
        reasons: Sequence[str],
        decision: FinalRecrawlDecision,
    ) -> FinalRecrawlSchedule:
        now = self._now()

        return FinalRecrawlSchedule(
            schedule_id=hashlib.sha256(
                f"{result_id}:schedule".encode(
                    "utf-8"
                )
            ).hexdigest(),
            resource_id=item.identity.resource_id,
            scheduled_at=now.isoformat(),
            due_at=due_at,
            deadline_at=deadline_at,
            interval_seconds=interval_seconds,
            priority_score=priority_score,
            priority_band=priority_band,
            decision=decision,
            schedule_type=(
                "recovery"
                if decision
                in {
                    FinalRecrawlDecision.RECOVER,
                    FinalRecrawlDecision.RETRY,
                    FinalRecrawlDecision.REQUEUE,
                }
                else "normal"
            ),
            adaptive=(
                item.adaptive_frequency is not None
            ),
            reasons=list(reasons),
            version=ARCHITECTURE_VERSION,
            partial=(
                item.freshness_signals.partial
                or item.history.partial
            ),
        )

    # ----------------------------------------------------------------------
    # RESULT ID
    # ----------------------------------------------------------------------

    def _result_id(
        self,
        identity: FinalFreshnessIdentity,
    ) -> str:
        timestamp_bucket = int(
            self._now().timestamp()
        )

        return hashlib.sha256(
            f"{identity.key()}:{timestamp_bucket}".encode(
                "utf-8"
            )
        ).hexdigest()

    # ----------------------------------------------------------------------
    # MAIN FINALIZATION
    # ----------------------------------------------------------------------

    def finalize(
        self,
        item: FinalFreshnessInput,
    ) -> FinalFreshnessResult:
        result_id = self._result_id(
            item.identity
        )

        self._event(
            result_id=result_id,
            resource_id=item.identity.resource_id,
            event_type=FinalEventType.REQUEST_RECEIVED,
            state=FinalFreshnessState.RECEIVED,
        )

        valid, errors = self.validate(item)

        self._event(
            result_id=result_id,
            resource_id=item.identity.resource_id,
            event_type=FinalEventType.VALIDATION_STARTED,
            state=FinalFreshnessState.VALIDATING,
            payload={
                "valid": valid,
                "errors": errors,
            },
        )

        if not valid:
            decision = FinalFreshnessDecision(
                decision=FinalRecrawlDecision.NO_ACTION,
                freshness_band=FinalFreshnessBand.UNKNOWN,
                change_band=FinalChangeBand.UNKNOWN,
                priority_band=FinalPriorityBand.BACKGROUND,
                priority_score=0.0,
                execution_readiness=(
                    FinalExecutionReadiness.NOT_READY
                ),
                queue_lane=FinalQueueLane.BACKGROUND,
                scheduled_at="",
                due_at="",
                deadline_at="",
                interval_seconds=0.0,
                recovery_action=FinalRecoveryAction.CANCEL,
                resource_escalation_required=False,
                coordination_required=False,
                reasons=errors,
                confidence=0.0,
                partial=False,
            )

            result = FinalFreshnessResult(
                result_id=result_id,
                identity=item.identity,
                state=FinalFreshnessState.REJECTED,
                decision=decision,
                created_at=self._now().isoformat(),
                partial=False,
            )

            self.backend.persist_result(result)

            self._event(
                result_id=result_id,
                resource_id=item.identity.resource_id,
                event_type=FinalEventType.FINALIZATION_REJECTED,
                state=FinalFreshnessState.REJECTED,
                payload={
                    "errors": errors,
                },
            )

            return result

        item = self._normalize_input(item)

        self._event(
            result_id=result_id,
            resource_id=item.identity.resource_id,
            event_type=FinalEventType.INPUT_NORMALIZED,
            state=FinalFreshnessState.NORMALIZING,
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.INPUT_NORMALIZED,
                FinalFreshnessState.NORMALIZING,
            )

        signals = item.freshness_signals
        history = item.history

        # --------------------------------------------------------------
        # FRESHNESS
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.FRESHNESS_ANALYSIS_STARTED,
            FinalFreshnessState.FRESHNESS_ANALYSIS,
        )

        freshness_band = self._freshness_band(
            signals
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.FRESHNESS_CLASSIFIED,
            FinalFreshnessState.FRESHNESS_ANALYSIS,
            {
                "band": freshness_band.value,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.FRESHNESS_ANALYZED,
                FinalFreshnessState.FRESHNESS_ANALYSIS,
                {
                    "band": freshness_band.value,
                },
            )

        # --------------------------------------------------------------
        # CHANGE
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.CHANGE_ANALYSIS_STARTED,
            FinalFreshnessState.CHANGE_ANALYSIS,
        )

        change_band = self._change_band(
            signals,
            history,
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.CHANGE_CLASSIFIED,
            FinalFreshnessState.CHANGE_ANALYSIS,
            {
                "band": change_band.value,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.CHANGE_ANALYZED,
                FinalFreshnessState.CHANGE_ANALYSIS,
                {
                    "band": change_band.value,
                },
            )

        # --------------------------------------------------------------
        # PRIORITY
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.PRIORITY_ANALYSIS_STARTED,
            FinalFreshnessState.PRIORITY_ANALYSIS,
        )

        priority_score = self._priority_score(
            signals,
            history,
        )

        priority_band = self._priority_band(
            priority_score
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.PRIORITY_CALCULATED,
            FinalFreshnessState.PRIORITY_ANALYSIS,
            {
                "priority_score": priority_score,
                "priority_band": priority_band.value,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.PRIORITY_ANALYZED,
                FinalFreshnessState.PRIORITY_ANALYSIS,
                {
                    "priority_score": priority_score,
                },
            )

        # --------------------------------------------------------------
        # FAILURE
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.FAILURE_ANALYSIS_STARTED,
            FinalFreshnessState.FAILURE_ANALYSIS,
        )

        failure = item.failure_recovery

        recovery_action = self._recovery_action(
            failure
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.FAILURE_RECOVERY_DECISION_CREATED,
            FinalFreshnessState.FAILURE_ANALYSIS,
            {
                "recovery_action": recovery_action.value,
                "failure_score": self._failure_score(
                    failure
                ),
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.FAILURE_ANALYZED,
                FinalFreshnessState.FAILURE_ANALYSIS,
                {
                    "recovery_action": recovery_action.value,
                },
            )

        # --------------------------------------------------------------
        # SCHEDULE
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.SCHEDULE_ANALYSIS_STARTED,
            FinalFreshnessState.SCHEDULE_ANALYSIS,
        )

        interval_seconds = self._schedule_interval(
            priority_band,
            item.adaptive_frequency,
            item.schedule,
        )

        (
            scheduled_at,
            due_at,
            deadline_at,
        ) = self._schedule_times(
            interval_seconds,
            priority_band,
            item.schedule,
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.SCHEDULE_ACCEPTED,
            FinalFreshnessState.SCHEDULE_ANALYSIS,
            {
                "interval_seconds": interval_seconds,
                "due_at": due_at,
                "deadline_at": deadline_at,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.SCHEDULE_ANALYZED,
                FinalFreshnessState.SCHEDULE_ANALYSIS,
                {
                    "interval_seconds": interval_seconds,
                },
            )

        # --------------------------------------------------------------
        # FREQUENCY
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.FREQUENCY_ANALYSIS_STARTED,
            FinalFreshnessState.FREQUENCY_ANALYSIS,
        )

        adaptive_frequency = item.adaptive_frequency

        if adaptive_frequency is None:
            adaptive_frequency = FinalAdaptiveFrequency(
                frequency_id=hashlib.sha256(
                    f"{result_id}:frequency".encode(
                        "utf-8"
                    )
                ).hexdigest(),
                resource_id=item.identity.resource_id,
                target_interval_seconds=interval_seconds,
                previous_interval_seconds=interval_seconds,
                change_frequency_score=(
                    signals.change_frequency
                ),
                volatility_score=(
                    signals.change_volatility
                ),
                stability_score=(
                    history.historical_stability
                ),
                adaptation_factor=1.0,
                band=change_band,
                decision="maintain",
                reasons=[],
                confidence=signals.confidence,
                partial=(
                    signals.partial
                    or history.partial
                ),
                version=ARCHITECTURE_VERSION,
            )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.FREQUENCY_ACCEPTED,
            FinalFreshnessState.FREQUENCY_ANALYSIS,
            {
                "target_interval_seconds":
                    adaptive_frequency.target_interval_seconds,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.FREQUENCY_ANALYZED,
                FinalFreshnessState.FREQUENCY_ANALYSIS,
            )

        # --------------------------------------------------------------
        # RESOURCE ALLOCATION
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.RESOURCE_ANALYSIS_STARTED,
            FinalFreshnessState.RESOURCE_ANALYSIS,
        )

        resource_allocation = item.resource_allocation

        if resource_allocation is None:
            resource_allocation = FinalResourceAllocation(
                allocation_id=hashlib.sha256(
                    f"{result_id}:allocation".encode(
                        "utf-8"
                    )
                ).hexdigest(),
                resource_id=item.identity.resource_id,
                allocation_score=1.0,
                worker_capacity_score=1.0,
                network_capacity_score=1.0,
                host_capacity_score=1.0,
                partition_capacity_score=1.0,
                region_capacity_score=1.0,
                rate_limit_headroom=1.0,
                concurrency_headroom=1.0,
                decision="maintain",
                escalation_required=False,
                assigned_partition_id=item.identity.partition_id,
                assigned_region_id=item.identity.region_id,
                partial=False,
                version=ARCHITECTURE_VERSION,
            )

        resource_escalation = self._resource_escalation(
            resource_allocation,
            priority_score,
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.RESOURCE_DECISION_CREATED,
            FinalFreshnessState.RESOURCE_ANALYSIS,
            {
                "allocation_score":
                    resource_allocation.allocation_score,
                "escalation_required":
                    resource_escalation,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.RESOURCE_ANALYZED,
                FinalFreshnessState.RESOURCE_ANALYSIS,
            )

        # --------------------------------------------------------------
        # COORDINATION
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.COORDINATION_ANALYSIS_STARTED,
            FinalFreshnessState.COORDINATION_ANALYSIS,
        )

        coordination = item.coordination

        if coordination is None:
            coordination = FinalCoordinationState(
                coordination_id=hashlib.sha256(
                    f"{result_id}:coordination".encode(
                        "utf-8"
                    )
                ).hexdigest(),
                resource_id=item.identity.resource_id,
                coordination_decision="coordinate",
                partition_healthy=True,
                shard_healthy=True,
                region_healthy=True,
                queue_healthy=True,
                partition_health_score=1.0,
                region_health_score=1.0,
                rebalance_required=False,
                relocation_required=False,
                recovery_required=False,
                partial=False,
                version=ARCHITECTURE_VERSION,
            )

        # --------------------------------------------------------------
        # ORCHESTRATION READINESS
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.ORCHESTRATION_ANALYSIS_STARTED,
            FinalFreshnessState.ORCHESTRATION_ANALYSIS,
        )

        readiness = self._execution_readiness(
            resource_allocation,
            coordination,
            failure,
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.EXECUTION_READINESS_CALCULATED,
            FinalFreshnessState.ORCHESTRATION_ANALYSIS,
            {
                "readiness": readiness.value,
            },
        )

        orchestration = item.orchestration

        if orchestration is None:
            orchestration = FinalOrchestrationState(
                operation_id=hashlib.sha256(
                    f"{result_id}:operation".encode(
                        "utf-8"
                    )
                ).hexdigest(),
                resource_id=item.identity.resource_id,
                state=(
                    "dispatchable"
                    if readiness
                    == FinalExecutionReadiness.DISPATCHABLE
                    else "waiting"
                ),
                assigned_partition_id=(
                    resource_allocation.assigned_partition_id
                    or item.identity.partition_id
                ),
                assigned_shard_id=item.identity.shard_id,
                assigned_region_id=(
                    resource_allocation.assigned_region_id
                    or item.identity.region_id
                ),
                execution_readiness=readiness,
                orchestration_priority=priority_score,
                retry_count=(
                    failure.retry_attempt
                    if failure is not None
                    else 0
                ),
                recovery_count=0,
                partial=False,
                version=ARCHITECTURE_VERSION,
            )
        else:
            orchestration.execution_readiness = readiness

        # --------------------------------------------------------------
        # QUEUE
        # --------------------------------------------------------------

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.QUEUE_ANALYSIS_STARTED,
            FinalFreshnessState.QUEUE_ANALYSIS,
        )

        queue_lane = self._queue_lane(
            priority_band,
            failure,
        )

        queue_entry = self._build_queue_entry(
            item=item,
            result_id=result_id,
            priority_score=priority_score,
            priority_band=priority_band,
            lane=queue_lane,
            due_at=due_at,
            deadline_at=deadline_at,
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.QUEUE_DECISION_CREATED,
            FinalFreshnessState.QUEUE_ANALYSIS,
            {
                "lane": queue_lane.value,
                "queue_entry_id":
                    queue_entry.queue_entry_id,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.QUEUE_ANALYZED,
                FinalFreshnessState.QUEUE_ANALYSIS,
                {
                    "lane": queue_lane.value,
                },
            )

        # --------------------------------------------------------------
        # COORDINATION DECISION
        # --------------------------------------------------------------

        coordination_required = (
            self._coordination_required(
                coordination,
                readiness,
            )
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.COORDINATION_DECISION_CREATED,
            FinalFreshnessState.COORDINATION_ANALYSIS,
            {
                "coordination_required":
                    coordination_required,
                "rebalance_required":
                    coordination.rebalance_required,
                "relocation_required":
                    coordination.relocation_required,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.COORDINATION_ANALYZED,
                FinalFreshnessState.COORDINATION_ANALYSIS,
            )

        # --------------------------------------------------------------
        # REASONS
        # --------------------------------------------------------------

        reasons = self._reasons(
            freshness_band=freshness_band,
            change_band=change_band,
            priority_band=priority_band,
            signals=signals,
            history=history,
            failure=failure,
            coordination=coordination,
            resource_allocation=resource_allocation,
        )

        # --------------------------------------------------------------
        # FINAL DECISION
        # --------------------------------------------------------------

        decision_value = self._final_decision(
            priority_band=priority_band,
            readiness=readiness,
            recovery_action=recovery_action,
            resource_escalation=resource_escalation,
            coordination_required=coordination_required,
            freshness_band=freshness_band,
            existing_schedule=item.schedule,
        )

        confidence = self._confidence(
            signals,
            history,
            item,
        )

        partial = (
            signals.partial
            or history.partial
            or (
                adaptive_frequency is not None
                and adaptive_frequency.partial
            )
            or (
                queue_entry is not None
                and queue_entry.partial
            )
            or (
                coordination is not None
                and coordination.partial
            )
            or (
                resource_allocation is not None
                and resource_allocation.partial
            )
        )

        if partial:
            confidence = self._clamp(
                confidence
                * (1.0 - self.policy.partial_penalty)
            )

        # --------------------------------------------------------------
        # DISPATCH / DEFER LOGIC
        # --------------------------------------------------------------

        if (
            decision_value
            in {
                FinalRecrawlDecision.SCHEDULE,
                FinalRecrawlDecision.PRIORITIZE,
                FinalRecrawlDecision.ACCELERATE,
                FinalRecrawlDecision.URGENT,
            }
            and readiness
            == FinalExecutionReadiness.DISPATCHABLE
        ):
            if priority_band in {
                FinalPriorityBand.URGENT,
                FinalPriorityBand.IMMEDIATE,
            }:
                decision_value = FinalRecrawlDecision.DISPATCH

        if (
            confidence < self.policy.minimum_confidence
            and self.policy.allow_partial
        ):
            decision_value = FinalRecrawlDecision.DEFER

        if (
            confidence < self.policy.minimum_confidence
            and not self.policy.allow_partial
        ):
            decision_value = FinalRecrawlDecision.NO_ACTION

        final_decision = FinalFreshnessDecision(
            decision=decision_value,
            freshness_band=freshness_band,
            change_band=change_band,
            priority_band=priority_band,
            priority_score=priority_score,
            execution_readiness=readiness,
            queue_lane=queue_lane,
            scheduled_at=scheduled_at,
            due_at=due_at,
            deadline_at=deadline_at,
            interval_seconds=interval_seconds,
            recovery_action=recovery_action,
            resource_escalation_required=resource_escalation,
            coordination_required=coordination_required,
            reasons=reasons,
            confidence=confidence,
            partial=partial,
        )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.FINAL_DECISION_CREATED,
            FinalFreshnessState.FINAL_DECISION,
            {
                "decision":
                    decision_value.value,
                "priority_score":
                    priority_score,
                "confidence":
                    confidence,
                "partial":
                    partial,
            },
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.FINAL_DECISION_CREATED,
                FinalFreshnessState.FINAL_DECISION,
                {
                    "decision":
                        decision_value.value,
                    "confidence":
                        confidence,
                },
            )

        # --------------------------------------------------------------
        # FINAL SCHEDULE
        # --------------------------------------------------------------

        schedule = self._build_schedule(
            item=item,
            result_id=result_id,
            priority_score=priority_score,
            priority_band=priority_band,
            interval_seconds=interval_seconds,
            due_at=due_at,
            deadline_at=deadline_at,
            reasons=reasons,
            decision=decision_value,
        )

        # --------------------------------------------------------------
        # FINAL RESULT
        # --------------------------------------------------------------

        result_state = FinalFreshnessState.COMPLETED

        if partial:
            result_state = FinalFreshnessState.PARTIAL

        if decision_value == FinalRecrawlDecision.DEFER:
            result_state = FinalFreshnessState.DEFERRED

        if decision_value == FinalRecrawlDecision.NO_ACTION:
            result_state = FinalFreshnessState.COMPLETED

        result = FinalFreshnessResult(
            result_id=result_id,
            identity=item.identity,
            state=result_state,
            decision=final_decision,
            schedule=schedule,
            adaptive_frequency=adaptive_frequency,
            queue_entry=queue_entry,
            orchestration=orchestration,
            resource_allocation=resource_allocation,
            coordination=coordination,
            failure_recovery=failure,
            lineage=item.lineage,
            evidence_sources=list(
                item.source_evidence
            ),
            created_at=self._now().isoformat(),
            partial=partial,
        )

        self.backend.persist_result(
            result
        )

        if self.policy.checkpoint_enabled:
            self._checkpoint(
                result_id,
                item.identity.resource_id,
                FinalCheckpointType.COMPLETED,
                result_state,
                {
                    "decision":
                        decision_value.value,
                    "priority_band":
                        priority_band.value,
                },
            )

        self._event(
            result_id,
            item.identity.resource_id,
            FinalEventType.FINALIZATION_COMPLETED,
            result_state,
            {
                "decision":
                    decision_value.value,
                "state":
                    result_state.value,
            },
        )

        return result

    # ----------------------------------------------------------------------
    # BATCH FINALIZATION
    # ----------------------------------------------------------------------

    def finalize_many(
        self,
        items: Iterable[FinalFreshnessInput],
    ) -> List[FinalFreshnessResult]:
        results: List[FinalFreshnessResult] = []

        for index, item in enumerate(items):
            if index >= self.policy.max_resources_per_batch:
                break

            results.append(
                self.finalize(item)
            )

        return results

    # ----------------------------------------------------------------------
    # RESOURCE-SPECIFIC RESCHEDULING
    # ----------------------------------------------------------------------

    def reschedule(
        self,
        item: FinalFreshnessInput,
    ) -> FinalFreshnessResult:
        existing = item.schedule

        if existing is None:
            return self.finalize(item)

        updated_metadata = dict(
            item.metadata
        )

        updated_metadata[
            "rescheduling"
        ] = True

        updated_metadata[
            "previous_schedule_id"
        ] = existing.schedule_id

        updated_item = FinalFreshnessInput(
            identity=item.identity,
            freshness_signals=item.freshness_signals,
            history=item.history,
            schedule=None,
            adaptive_frequency=item.adaptive_frequency,
            queue_entry=item.queue_entry,
            orchestration=item.orchestration,
            resource_allocation=item.resource_allocation,
            coordination=item.coordination,
            failure_recovery=item.failure_recovery,
            source_evidence=item.source_evidence,
            lineage=item.lineage,
            metadata=updated_metadata,
        )

        return self.finalize(
            updated_item
        )

    # ----------------------------------------------------------------------
    # EVENTS / CHECKPOINTS / RESULTS
    # ----------------------------------------------------------------------

    def events(
        self,
    ) -> List[FinalFreshnessEvent]:
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
    ) -> List[FinalFreshnessCheckpoint]:
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

    def result(
        self,
        result_id: str,
    ) -> Optional[FinalFreshnessResult]:
        return self.backend.get_result(
            result_id
        )

    # ----------------------------------------------------------------------
    # ARCHITECTURE DESCRIPTION
    # ----------------------------------------------------------------------

    def architecture(
        self,
    ) -> Dict[str, Any]:
        return {
            "phase": PHASE,
            "phase_name": PHASE_NAME,
            "architecture_version":
                ARCHITECTURE_VERSION,
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "purpose": (
                "Final integrated freshness and recrawling "
                "control architecture for enormous public-Web scale."
            ),

            "pipeline": [
                "13.1 crawl-freshness prioritization",
                "13.2 change detection and content-change signals",
                "13.3 recrawl scheduling",
                "13.4 adaptive recrawl frequency",
                "13.5 URL/document freshness queues",
                "13.6 distributed recrawl orchestration",
                "13.7 freshness-aware crawl resource allocation",
                "13.8 global recrawl coordination and failure recovery",
                "13.9 final freshness and recrawling decision",
            ],

            "inputs": [
                "freshness evidence",
                "change evidence",
                "recrawl history",
                "recrawl schedule",
                "adaptive recrawl frequency",
                "freshness queue state",
                "distributed orchestration state",
                "crawl resource allocation",
                "global coordination state",
                "failure recovery state",
            ],

            "outputs": [
                "final freshness classification",
                "final change classification",
                "final recrawl priority",
                "final recrawl interval",
                "final due time",
                "final deadline",
                "queue lane",
                "execution readiness",
                "resource escalation decision",
                "coordination decision",
                "failure recovery action",
                "final recrawl decision",
                "durable lineage",
                "checkpoint state",
            ],

            "distributed": True,
            "partition_aware": True,
            "shard_aware": True,
            "region_aware": True,
            "globally_coordinated": True,

            "horizontally_scalable": True,
            "resource_partitionable": True,
            "queue_partitionable": True,
            "schedule_partitionable": True,
            "history_partitionable": True,

            "incremental_updates_supported": True,
            "partial_input_supported": True,

            "deterministic": True,
            "deterministic_decisions": True,
            "deterministic_queue_lane_selection": True,

            "checkpointable": True,
            "restartable": True,
            "failure_recoverable": True,

            "retry_planning": True,
            "requeue_planning": True,
            "relocation_planning": True,
            "rebalance_planning": True,
            "resource_escalation": True,

            "lineage_preserving": True,
            "provenance_preserving": True,
            "versioned_decisions": True,

            "backend_replaceable": True,

            "global_limits": {
                "resources": None,
                "urls": None,
                "documents": None,
                "hosts": None,
                "domains": None,
                "partitions": None,
                "shards": None,
                "regions": None,
                "workers": None,
                "queues": None,
                "schedules": None,
                "recrawl_histories": None,
                "coordination_records": None,
            },

            "per_unit_limits": {
                "max_resources_per_batch":
                    self.policy.max_resources_per_batch,
                "maximum_retry_attempts":
                    self.policy.maximum_retry_attempts,
                "checkpoint_retention_hint":
                    self.policy.checkpoint_retention_hint,
            },

            "control_plane_only": True,

            "does_not": [
                "perform HTTP requests",
                "fetch Web resources",
                "parse Web resources",
                "execute crawler workers",
                "directly crawl the Web",
                "directly mutate the search index",
                "perform final search ranking",
                "classify spam",
                "replace crawler execution",
                "replace distributed crawler workers",
            ],

            "technology_independence": {
                "google_search_api": False,
                "google_index": False,
                "google_crawler": False,
                "google_infrastructure": False,
                "google_ranking_technology": False,
            },

            "stage_boundaries": {
                "13.1": (
                    "determines freshness attention and urgency"
                ),
                "13.2": (
                    "determines content change and volatility evidence"
                ),
                "13.3": (
                    "determines recrawl scheduling"
                ),
                "13.4": (
                    "adapts long-term recrawl frequency"
                ),
                "13.5": (
                    "maintains URL/document freshness queues"
                ),
                "13.6": (
                    "orchestrates distributed recrawl operations"
                ),
                "13.7": (
                    "allocates crawl resources according to freshness demand"
                ),
                "13.8": (
                    "coordinates globally and recovers failed operations"
                ),
                "13.9": (
                    "integrates all freshness and recrawling evidence "
                    "into the final control-plane decision"
                ),
                "14": (
                    "spam, abuse, security, and quality systems"
                ),
            },

            "phase13_complete": True,

            "next_phase": NEXT_PHASE,
            "next_phase_name": NEXT_PHASE_NAME,

            "architecture_guarantees": [
                "freshness evidence remains provenance-aware",
                "change evidence remains independent from scheduling",
                "scheduling remains independent from crawler execution",
                "adaptive frequency remains independently replaceable",
                "queue state remains independently replaceable",
                "distributed orchestration remains independently replaceable",
                "resource allocation remains independently replaceable",
                "global coordination remains independently replaceable",
                "failure recovery remains independently replaceable",
                "final Phase 13 decision remains checkpointable",
                "no fixed global Web-scale ceiling is imposed",
            ],
        }


# ============================================================================
# ALIASES
# ============================================================================


FinalFreshnessRecrawling = (
    FinalFreshnessRecrawlingArchitecture
)

GlobalFinalFreshnessRecrawling = (
    FinalFreshnessRecrawlingArchitecture
)

Phase13_9FinalFreshnessRecrawling = (
    FinalFreshnessRecrawlingArchitecture
)

FinalFreshnessArchitecture = (
    FinalFreshnessRecrawlingArchitecture
)

GlobalFreshnessRecrawlingArchitecture = (
    FinalFreshnessRecrawlingArchitecture
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
    "NEXT_PHASE",
    "PHASE_NAME",
    "NEXT_PHASE_NAME",

    "FinalFreshnessState",
    "FinalRecrawlDecision",
    "FinalFreshnessBand",
    "FinalChangeBand",
    "FinalPriorityBand",
    "FinalQueueLane",
    "FinalExecutionReadiness",
    "FinalFailureState",
    "FinalRecoveryAction",
    "FinalEventType",
    "FinalCheckpointType",
    "FreshnessEvidenceSource",

    "FinalFreshnessIdentity",
    "FinalFreshnessLineage",

    "FinalFreshnessSignals",
    "FinalRecrawlHistory",
    "FinalRecrawlSchedule",
    "FinalAdaptiveFrequency",
    "FinalFreshnessQueueEntry",
    "FinalOrchestrationState",
    "FinalResourceAllocation",
    "FinalCoordinationState",
    "FinalFailureRecovery",

    "FinalFreshnessInput",
    "FinalFreshnessRecrawlingPolicy",

    "FinalFreshnessRecrawlingBackend",
    "InMemoryFinalFreshnessRecrawlingMetadata",

    "FinalFreshnessDecision",
    "FinalFreshnessResult",
    "FinalFreshnessCheckpoint",
    "FinalFreshnessEvent",

    "FinalFreshnessRecrawlingArchitecture",

    "FinalFreshnessRecrawling",
    "GlobalFinalFreshnessRecrawling",
    "Phase13_9FinalFreshnessRecrawling",
    "FinalFreshnessArchitecture",
    "GlobalFreshnessRecrawlingArchitecture",
]
