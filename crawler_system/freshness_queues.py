"""
OUR SEARCH
Phase 13.5 — URL/Document Freshness Queues

Purpose
-------
Provide the durable queue architecture for URL/document freshness work.

This stage converts freshness-aware scheduling decisions from Phase 13.3 and
adaptive recrawl-frequency decisions from Phase 13.4 into durable,
partition-aware URL/document freshness queue entries.

The queue layer is responsible for:
- accepting freshness work items
- normalizing freshness queue entries
- assigning queue priority
- assigning queue lanes
- partitioning queue work
- deduplicating active queue work
- maintaining queue state
- acknowledging completed work
- retrying failed queue work
- deferring work
- expiring obsolete queue entries
- preserving ordering metadata
- preserving lineage and provenance
- supporting checkpoint/restart workflows
- exposing backend-replaceable queue persistence

This module does NOT:
- execute HTTP requests
- fetch Web resources
- perform crawling
- assign crawler workers
- perform global recrawl orchestration
- mutate the search index
- perform final ranking
- classify spam
- replace Phase 13.3 scheduling
- replace Phase 13.4 adaptive frequency
- depend on Google Search
- depend on Google's index
- depend on Google's crawler
- depend on Google's infrastructure
- depend on Google's ranking technology

Scale target
------------
Designed directly for billions to trillions of publicly accessible Web
resources.

The architecture avoids imposing a fixed global ceiling on:
- URLs
- documents
- resources
- hosts
- domains
- queue entries
- queue partitions
- workers
- retry history
- freshness queues

Per-request and per-partition limits exist only to protect individual
processing units.
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

ARCHITECTURE_VERSION = "freshness-queues.v1"
PHASE = "13.5"
PREVIOUS_STAGE = "13.4"
NEXT_STAGE = "13.6"


# ============================================================================
# ENUMERATIONS
# ============================================================================


class FreshnessQueueState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    PARTITIONING = "partitioning"
    ENQUEUED = "enqueued"
    READY = "ready"
    CLAIMED = "claimed"
    PROCESSING = "processing"
    ACKNOWLEDGED = "acknowledged"
    RETRY_PENDING = "retry_pending"
    DEFERRED = "deferred"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class FreshnessQueuePriorityBand(str, Enum):
    BACKGROUND = "background"
    NORMAL = "normal"
    ELEVATED = "elevated"
    HIGH = "high"
    URGENT = "urgent"
    IMMEDIATE = "immediate"


class FreshnessQueueLane(str, Enum):
    INITIAL = "initial"
    NORMAL = "normal"
    FRESHNESS = "freshness"
    CHANGE = "change"
    TEMPORAL = "temporal"
    FEED = "feed"
    SITEMAP = "sitemap"
    RECOVERY = "recovery"
    URGENT = "urgent"


class FreshnessQueueWorkType(str, Enum):
    INITIAL_RECRAWL = "initial_recrawl"
    NORMAL_RECRAWL = "normal_recrawl"
    FRESHNESS_RECRAWL = "freshness_recrawl"
    CHANGE_RECRAWL = "change_recrawl"
    TEMPORAL_RECRAWL = "temporal_recrawl"
    FEED_TRIGGERED = "feed_triggered"
    SITEMAP_TRIGGERED = "sitemap_triggered"
    EXTERNAL_TRIGGERED = "external_triggered"
    RECOVERY_RECRAWL = "recovery_recrawl"
    URGENT_RECRAWL = "urgent_recrawl"


class FreshnessQueueDecision(str, Enum):
    ENQUEUE = "enqueue"
    PRIORITIZE = "prioritize"
    DEFER = "defer"
    RETRY = "retry"
    EXPIRE = "expire"
    CANCEL = "cancel"
    ACKNOWLEDGE = "acknowledge"


class FreshnessQueueReason(str, Enum):
    SCHEDULED_RECRAWL = "scheduled_recrawl"
    ADAPTIVE_FREQUENCY = "adaptive_frequency"
    RECENT_CHANGE = "recent_change"
    HIGH_CHANGE_RATE = "high_change_rate"
    HIGH_VOLATILITY = "high_volatility"
    TEMPORAL_SENSITIVITY = "temporal_sensitivity"
    SOURCE_FRESHNESS = "source_freshness"
    FEED_UPDATE = "feed_update"
    SITEMAP_UPDATE = "sitemap_update"
    EXTERNAL_UPDATE = "external_update"
    MISSED_DEADLINE = "missed_deadline"
    RECOVERY = "recovery"
    INITIAL_DISCOVERY = "initial_discovery"
    STALE_RESOURCE = "stale_resource"
    QUEUE_RETRY = "queue_retry"
    QUEUE_REBALANCE = "queue_rebalance"
    DUPLICATE_SUPPRESSION = "duplicate_suppression"
    OBSOLETE_SCHEDULE = "obsolete_schedule"
    MANUAL_DEFERMENT = "manual_deferment"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class FreshnessQueueEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    ENTRY_NORMALIZED = "entry_normalized"
    PARTITION_ASSIGNED = "partition_assigned"
    DUPLICATE_CHECK_STARTED = "duplicate_check_started"
    DUPLICATE_ENTRY_SUPPRESSED = "duplicate_entry_suppressed"
    PRIORITY_ASSIGNED = "priority_assigned"
    LANE_ASSIGNED = "lane_assigned"
    QUEUE_ENTRY_CREATED = "queue_entry_created"
    QUEUE_ENTRY_UPDATED = "queue_entry_updated"
    QUEUE_ENTRY_CLAIMED = "queue_entry_claimed"
    QUEUE_ENTRY_ACKNOWLEDGED = "queue_entry_acknowledged"
    RETRY_SCHEDULED = "retry_scheduled"
    ENTRY_DEFERRED = "entry_deferred"
    ENTRY_EXPIRED = "entry_expired"
    ENTRY_CANCELLED = "entry_cancelled"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    QUEUE_OPERATION_COMPLETED = "queue_operation_completed"
    QUEUE_OPERATION_REJECTED = "queue_operation_rejected"
    QUEUE_OPERATION_FAILED = "queue_operation_failed"


class FreshnessQueueCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    ENTRY_NORMALIZED = "entry_normalized"
    PARTITION_ASSIGNED = "partition_assigned"
    DUPLICATE_CHECKED = "duplicate_checked"
    PRIORITY_ASSIGNED = "priority_assigned"
    ENTRY_CREATED = "entry_created"
    ENTRY_CLAIMED = "entry_claimed"
    ENTRY_ACKNOWLEDGED = "entry_acknowledged"
    RETRY_CREATED = "retry_created"
    COMPLETED = "completed"


class FreshnessQueueClaimState(str, Enum):
    AVAILABLE = "available"
    CLAIMED = "claimed"
    RELEASED = "released"
    ACKNOWLEDGED = "acknowledged"
    EXPIRED = "expired"


# ============================================================================
# IDENTITY
# ============================================================================


@dataclass(frozen=True)
class FreshnessQueueIdentity:
    resource_id: str

    queue_entry_id: str = ""
    queue_version: str = ARCHITECTURE_VERSION

    partition_id: str = ""
    host_id: str = ""
    domain_id: str = ""

    resource_type: str = "url"

    def key(self) -> str:
        return (
            f"{self.resource_type}:"
            f"{self.resource_id}:"
            f"{self.queue_entry_id}:"
            f"{self.queue_version}"
        )


# ============================================================================
# LINEAGE
# ============================================================================


@dataclass
class FreshnessQueueLineage:
    resource_id: str

    previous_stage: str = PREVIOUS_STAGE
    current_stage: str = PHASE

    source_schedule_id: str = ""
    source_frequency_version: str = ""

    source_signal_version: str = ""
    source_history_version: str = ""

    parent_queue_entry_ids: List[str] = field(
        default_factory=list
    )

    parent_schedule_ids: List[str] = field(
        default_factory=list
    )

    lineage_metadata: Dict[str, Any] = field(
        default_factory=dict
    )


# ============================================================================
# FRESHNESS INPUT
# ============================================================================


@dataclass
class FreshnessQueueInput:
    resource_id: str

    resource_type: str = "url"

    scheduled_time: Optional[str] = None
    deadline: Optional[str] = None

    adaptive_interval_seconds: float = 0.0

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

    historical_stability: float = 0.0

    priority_score: float = 0.0

    schedule_id: str = ""
    frequency_version: str = ""

    signal_version: str = ""
    history_version: str = ""

    partition_id: str = ""
    host_id: str = ""
    domain_id: str = ""

    partial: bool = False

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


# ============================================================================
# QUEUE POLICY
# ============================================================================


@dataclass
class FreshnessQueuePolicy:
    max_entries_per_batch: int = 100_000

    minimum_confidence: float = 0.10

    allow_partial: bool = True

    deterministic: bool = True

    checkpoint_enabled: bool = True

    deduplicate_active_entries: bool = True

    preserve_ordering_metadata: bool = True

    max_attempts: int = 8

    retry_base_delay_seconds: float = 60.0

    retry_max_delay_seconds: float = 7.0 * 86_400.0

    claim_lease_seconds: float = 900.0

    stale_entry_after_seconds: float = 30.0 * 86_400.0

    minimum_priority_score: float = 0.0

    maximum_priority_score: float = 1.0

    urgent_threshold: float = 0.90

    high_threshold: float = 0.72

    elevated_threshold: float = 0.50

    normal_threshold: float = 0.25

    immediate_deadline_window_seconds: float = 900.0

    urgent_deadline_window_seconds: float = 3_600.0

    high_deadline_window_seconds: float = 14_400.0

    elevated_deadline_window_seconds: float = 86_400.0

    normal_deadline_window_seconds: float = 7.0 * 86_400.0

    background_deadline_window_seconds: float = 30.0 * 86_400.0


# ============================================================================
# QUEUE ENTRY
# ============================================================================


@dataclass
class FreshnessQueueEntry:
    identity: FreshnessQueueIdentity
    lineage: FreshnessQueueLineage

    state: FreshnessQueueState = FreshnessQueueState.ENQUEUED

    priority_band: FreshnessQueuePriorityBand = (
        FreshnessQueuePriorityBand.NORMAL
    )

    lane: FreshnessQueueLane = FreshnessQueueLane.NORMAL

    work_type: FreshnessQueueWorkType = (
        FreshnessQueueWorkType.NORMAL_RECRAWL
    )

    decision: FreshnessQueueDecision = (
        FreshnessQueueDecision.ENQUEUE
    )

    reasons: List[FreshnessQueueReason] = field(
        default_factory=list
    )

    priority_score: float = 0.0

    scheduled_time: Optional[str] = None
    deadline: Optional[str] = None

    created_at: str = ""
    updated_at: str = ""

    attempt_count: int = 0

    next_retry_time: Optional[str] = None

    claim_state: FreshnessQueueClaimState = (
        FreshnessQueueClaimState.AVAILABLE
    )

    claimed_by: str = ""
    claim_expires_at: Optional[str] = None

    partition_id: str = ""

    ordering_key: str = ""

    deduplication_key: str = ""

    freshness_score: float = 0.0
    urgency_score: float = 0.0

    change_frequency: float = 0.0
    change_volatility: float = 0.0

    confidence: float = 0.0

    partial: bool = False

    version: str = ARCHITECTURE_VERSION

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


# ============================================================================
# OPERATION RESULT
# ============================================================================


@dataclass
class FreshnessQueueResult:
    state: FreshnessQueueState

    queue_entry: Optional[FreshnessQueueEntry]

    resource_id: str
    request_id: str

    accepted: bool

    duplicate: bool
    partial: bool

    deferred: bool
    rejected: bool

    confidence: float

    error: Optional[str] = None

    created_at: str = ""


# ============================================================================
# CHECKPOINT
# ============================================================================


@dataclass
class FreshnessQueueCheckpoint:
    checkpoint_id: str

    request_id: str
    resource_id: str

    checkpoint_type: FreshnessQueueCheckpointType

    state: FreshnessQueueState

    partition_id: str = ""

    priority_score: float = 0.0

    attempt_count: int = 0

    payload: Dict[str, Any] = field(
        default_factory=dict
    )

    created_at: str = ""

    version: str = ARCHITECTURE_VERSION


# ============================================================================
# EVENT
# ============================================================================


@dataclass
class FreshnessQueueEvent:
    event_id: str

    request_id: str
    resource_id: str

    event_type: FreshnessQueueEventType

    state: FreshnessQueueState

    payload: Dict[str, Any] = field(
        default_factory=dict
    )

    created_at: str = ""

    version: str = ARCHITECTURE_VERSION


# ============================================================================
# BACKEND CONTRACT
# ============================================================================


class FreshnessQueueBackend(Protocol):
    def persist_event(
        self,
        event: FreshnessQueueEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: FreshnessQueueCheckpoint,
    ) -> None:
        ...

    def persist_entry(
        self,
        entry: FreshnessQueueEntry,
    ) -> None:
        ...

    def get_entry(
        self,
        deduplication_key: str,
    ) -> Optional[FreshnessQueueEntry]:
        ...

    def persist_result(
        self,
        result: FreshnessQueueResult,
    ) -> None:
        ...

    def get_result(
        self,
        request_id: str,
    ) -> Optional[FreshnessQueueResult]:
        ...


# ============================================================================
# IN-MEMORY BACKEND
# ============================================================================


class InMemoryFreshnessQueueMetadata:
    def __init__(self) -> None:
        self._events: List[FreshnessQueueEvent] = []

        self._checkpoints: List[
            FreshnessQueueCheckpoint
        ] = []

        self._entries: Dict[
            str,
            FreshnessQueueEntry,
        ] = {}

        self._results: Dict[
            str,
            FreshnessQueueResult,
        ] = {}

    def persist_event(
        self,
        event: FreshnessQueueEvent,
    ) -> None:
        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: FreshnessQueueCheckpoint,
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_entry(
        self,
        entry: FreshnessQueueEntry,
    ) -> None:
        self._entries[
            entry.deduplication_key
        ] = entry

    def get_entry(
        self,
        deduplication_key: str,
    ) -> Optional[FreshnessQueueEntry]:
        return self._entries.get(
            deduplication_key
        )

    def persist_result(
        self,
        result: FreshnessQueueResult,
    ) -> None:
        self._results[
            result.request_id
        ] = result

    def get_result(
        self,
        request_id: str,
    ) -> Optional[FreshnessQueueResult]:
        return self._results.get(
            request_id
        )

    def events(self) -> List[FreshnessQueueEvent]:
        return list(self._events)

    def checkpoints(
        self,
    ) -> List[FreshnessQueueCheckpoint]:
        return list(self._checkpoints)

    def entries(self) -> List[FreshnessQueueEntry]:
        return list(self._entries.values())

    def results(self) -> List[FreshnessQueueResult]:
        return list(self._results.values())


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class FreshnessQueueArchitecture:
    """
    Phase 13.5 URL/document freshness queue architecture.

    Converts scheduled and adaptive freshness decisions into durable,
    partition-aware queue entries.

    This layer manages queue state and metadata.

    It does not perform crawling.
    """

    def __init__(
        self,
        backend: Optional[FreshnessQueueBackend] = None,
        policy: Optional[FreshnessQueuePolicy] = None,
    ) -> None:
        self.backend = (
            backend
            or InMemoryFreshnessQueueMetadata()
        )

        self.policy = (
            policy
            or FreshnessQueuePolicy()
        )

    # ------------------------------------------------------------------------
    # TIME
    # ------------------------------------------------------------------------

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _now_iso(self) -> str:
        return self._now().isoformat()

    # ------------------------------------------------------------------------
    # NUMERIC SAFETY
    # ------------------------------------------------------------------------

    def _clamp(
        self,
        value: float,
        minimum: float = 0.0,
        maximum: float = 1.0,
    ) -> float:
        if not math.isfinite(value):
            return minimum

        return max(
            minimum,
            min(maximum, value),
        )

    def _safe_float(
        self,
        value: Any,
        default: float = 0.0,
    ) -> float:
        try:
            result = float(value)
        except (
            TypeError,
            ValueError,
        ):
            return default

        if not math.isfinite(result):
            return default

        return result

    def _safe_int(
        self,
        value: Any,
        default: int = 0,
    ) -> int:
        try:
            result = int(value)
        except (
            TypeError,
            ValueError,
        ):
            return default

        return max(0, result)

    # ------------------------------------------------------------------------
    # IDENTIFIERS
    # ------------------------------------------------------------------------

    def _stable_id(
        self,
        prefix: str,
        value: str,
    ) -> str:
        digest = hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()[:24]

        return f"{prefix}-{digest}"

    def _request_id(
        self,
        resource_id: str,
        schedule_id: str,
    ) -> str:
        return self._stable_id(
            "freshq",
            (
                f"{resource_id}:"
                f"{schedule_id}:"
                f"{ARCHITECTURE_VERSION}"
            ),
        )

    def _deduplication_key(
        self,
        resource_id: str,
        resource_type: str,
        lane: FreshnessQueueLane,
    ) -> str:
        return self._stable_id(
            "dedup",
            (
                f"{resource_type}:"
                f"{resource_id}:"
                f"{lane.value}"
            ),
        )

    # ------------------------------------------------------------------------
    # TIMESTAMP PARSING
    # ------------------------------------------------------------------------

    def _parse_timestamp(
        self,
        value: Optional[str],
    ) -> Optional[datetime]:
        if not value:
            return None

        try:
            parsed = datetime.fromisoformat(
                value.replace(
                    "Z",
                    "+00:00",
                )
            )
        except (
            TypeError,
            ValueError,
        ):
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(
                tzinfo=timezone.utc
            )

        return parsed.astimezone(
            timezone.utc
        )

    # ------------------------------------------------------------------------
    # PARTITIONING
    # ------------------------------------------------------------------------

    def _partition_for_resource(
        self,
        resource_id: str,
        supplied_partition: str = "",
    ) -> str:
        if supplied_partition:
            return supplied_partition

        digest = hashlib.sha256(
            resource_id.encode("utf-8")
        ).hexdigest()

        return f"partition-{digest[:16]}"

    # ------------------------------------------------------------------------
    # EVENTS
    # ------------------------------------------------------------------------

    def _event(
        self,
        request_id: str,
        resource_id: str,
        event_type: FreshnessQueueEventType,
        state: FreshnessQueueState,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> FreshnessQueueEvent:
        event = FreshnessQueueEvent(
            event_id=self._stable_id(
                "event",
                (
                    f"{request_id}:"
                    f"{event_type.value}:"
                    f"{self._now_iso()}"
                ),
            ),
            request_id=request_id,
            resource_id=resource_id,
            event_type=event_type,
            state=state,
            payload=dict(payload or {}),
            created_at=self._now_iso(),
        )

        self.backend.persist_event(event)

        return event

    # ------------------------------------------------------------------------
    # CHECKPOINTS
    # ------------------------------------------------------------------------

    def _checkpoint(
        self,
        request_id: str,
        resource_id: str,
        checkpoint_type: FreshnessQueueCheckpointType,
        state: FreshnessQueueState,
        partition_id: str = "",
        priority_score: float = 0.0,
        attempt_count: int = 0,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> FreshnessQueueCheckpoint:
        checkpoint = FreshnessQueueCheckpoint(
            checkpoint_id=self._stable_id(
                "checkpoint",
                (
                    f"{request_id}:"
                    f"{checkpoint_type.value}:"
                    f"{self._now_iso()}"
                ),
            ),
            request_id=request_id,
            resource_id=resource_id,
            checkpoint_type=checkpoint_type,
            state=state,
            partition_id=partition_id,
            priority_score=self._clamp(
                priority_score
            ),
            attempt_count=max(
                0,
                attempt_count,
            ),
            payload=dict(payload or {}),
            created_at=self._now_iso(),
        )

        if self.policy.checkpoint_enabled:
            self.backend.persist_checkpoint(
                checkpoint
            )

        self._event(
            request_id,
            resource_id,
            FreshnessQueueEventType.CHECKPOINT_CREATED,
            state,
            {
                "checkpoint_id":
                    checkpoint.checkpoint_id,
                "checkpoint_type":
                    checkpoint_type.value,
            },
        )

        return checkpoint

    # ------------------------------------------------------------------------
    # INPUT NORMALIZATION
    # ------------------------------------------------------------------------

    def _normalize_input(
        self,
        item: FreshnessQueueInput,
    ) -> FreshnessQueueInput:
        return FreshnessQueueInput(
            resource_id=str(
                item.resource_id or ""
            ).strip(),

            resource_type=str(
                item.resource_type or "url"
            ).strip().lower(),

            scheduled_time=item.scheduled_time,
            deadline=item.deadline,

            adaptive_interval_seconds=max(
                0.0,
                self._safe_float(
                    item.adaptive_interval_seconds
                ),
            ),

            freshness_score=self._clamp(
                self._safe_float(
                    item.freshness_score
                )
            ),

            urgency_score=self._clamp(
                self._safe_float(
                    item.urgency_score
                )
            ),

            change_frequency=self._clamp(
                self._safe_float(
                    item.change_frequency
                )
            ),

            change_volatility=self._clamp(
                self._safe_float(
                    item.change_volatility
                )
            ),

            recent_change=self._clamp(
                self._safe_float(
                    item.recent_change
                )
            ),

            semantic_change_score=self._clamp(
                self._safe_float(
                    item.semantic_change_score
                )
            ),

            temporal_sensitivity=self._clamp(
                self._safe_float(
                    item.temporal_sensitivity
                )
            ),

            source_freshness=self._clamp(
                self._safe_float(
                    item.source_freshness
                )
            ),

            feed_update_signal=self._clamp(
                self._safe_float(
                    item.feed_update_signal
                )
            ),

            sitemap_update_signal=self._clamp(
                self._safe_float(
                    item.sitemap_update_signal
                )
            ),

            external_update_signal=self._clamp(
                self._safe_float(
                    item.external_update_signal
                )
            ),

            historical_stability=self._clamp(
                self._safe_float(
                    item.historical_stability
                )
            ),

            priority_score=self._clamp(
                self._safe_float(
                    item.priority_score
                )
            ),

            schedule_id=item.schedule_id,
            frequency_version=item.frequency_version,

            signal_version=item.signal_version,
            history_version=item.history_version,

            partition_id=item.partition_id,
            host_id=item.host_id,
            domain_id=item.domain_id,

            partial=bool(item.partial),

            metadata=dict(item.metadata),
        )

    # ------------------------------------------------------------------------
    # PRIORITY
    # ------------------------------------------------------------------------

    def _priority_score(
        self,
        item: FreshnessQueueInput,
    ) -> float:
        score = (
            0.18 * item.urgency_score
            + 0.15 * item.freshness_score
            + 0.15 * item.change_frequency
            + 0.13 * item.change_volatility
            + 0.13 * item.recent_change
            + 0.10 * item.semantic_change_score
            + 0.06 * item.temporal_sensitivity
            + 0.05 * item.source_freshness
            + 0.03 * item.feed_update_signal
            + 0.01 * item.sitemap_update_signal
            + 0.01 * item.external_update_signal
        )

        if item.priority_score > 0:
            score = (
                0.75 * score
                + 0.25 * item.priority_score
            )

        return self._clamp(score)

    # ------------------------------------------------------------------------
    # PRIORITY BAND
    # ------------------------------------------------------------------------

    def _priority_band(
        self,
        score: float,
    ) -> FreshnessQueuePriorityBand:
        score = self._clamp(score)

        if (
            score
            >= self.policy.urgent_threshold
        ):
            return (
                FreshnessQueuePriorityBand.IMMEDIATE
            )

        if (
            score
            >= self.policy.high_threshold
        ):
            return (
                FreshnessQueuePriorityBand.URGENT
            )

        if (
            score
            >= self.policy.elevated_threshold
        ):
            return (
                FreshnessQueuePriorityBand.HIGH
            )

        if (
            score
            >= self.policy.normal_threshold
        ):
            return (
                FreshnessQueuePriorityBand.ELEVATED
            )

        if score > 0:
            return (
                FreshnessQueuePriorityBand.NORMAL
            )

        return (
            FreshnessQueuePriorityBand.BACKGROUND
        )

    # ------------------------------------------------------------------------
    # LANE
    # ------------------------------------------------------------------------

    def _lane(
        self,
        item: FreshnessQueueInput,
        band: FreshnessQueuePriorityBand,
    ) -> FreshnessQueueLane:
        if (
            item.feed_update_signal
            >= 0.70
        ):
            return FreshnessQueueLane.FEED

        if (
            item.sitemap_update_signal
            >= 0.70
        ):
            return FreshnessQueueLane.SITEMAP

        if (
            item.external_update_signal
            >= 0.70
        ):
            return FreshnessQueueLane.RECOVERY

        if (
            item.recent_change
            >= 0.75
        ):
            return FreshnessQueueLane.CHANGE

        if (
            item.change_volatility
            >= 0.75
        ):
            return FreshnessQueueLane.FRESHNESS

        if (
            item.temporal_sensitivity
            >= 0.75
        ):
            return FreshnessQueueLane.TEMPORAL

        if (
            band
            in (
                FreshnessQueuePriorityBand.IMMEDIATE,
                FreshnessQueuePriorityBand.URGENT,
            )
        ):
            return FreshnessQueueLane.URGENT

        if (
            item.scheduled_time is None
        ):
            return FreshnessQueueLane.INITIAL

        return FreshnessQueueLane.NORMAL

    # ------------------------------------------------------------------------
    # WORK TYPE
    # ------------------------------------------------------------------------

    def _work_type(
        self,
        lane: FreshnessQueueLane,
        item: FreshnessQueueInput,
    ) -> FreshnessQueueWorkType:
        mapping = {
            FreshnessQueueLane.INITIAL:
                FreshnessQueueWorkType.INITIAL_RECRAWL,

            FreshnessQueueLane.NORMAL:
                FreshnessQueueWorkType.NORMAL_RECRAWL,

            FreshnessQueueLane.FRESHNESS:
                FreshnessQueueWorkType.FRESHNESS_RECRAWL,

            FreshnessQueueLane.CHANGE:
                FreshnessQueueWorkType.CHANGE_RECRAWL,

            FreshnessQueueLane.TEMPORAL:
                FreshnessQueueWorkType.TEMPORAL_RECRAWL,

            FreshnessQueueLane.FEED:
                FreshnessQueueWorkType.FEED_TRIGGERED,

            FreshnessQueueLane.SITEMAP:
                FreshnessQueueWorkType.SITEMAP_TRIGGERED,

            FreshnessQueueLane.RECOVERY:
                FreshnessQueueWorkType.RECOVERY_RECRAWL,

            FreshnessQueueLane.URGENT:
                FreshnessQueueWorkType.URGENT_RECRAWL,
        }

        return mapping[lane]

    # ------------------------------------------------------------------------
    # DECISION
    # ------------------------------------------------------------------------

    def _decision(
        self,
        band: FreshnessQueuePriorityBand,
        item: FreshnessQueueInput,
    ) -> FreshnessQueueDecision:
        if (
            item.urgency_score >= 0.95
            or band
            == FreshnessQueuePriorityBand.IMMEDIATE
        ):
            return (
                FreshnessQueueDecision.PRIORITIZE
            )

        if (
            item.recent_change >= 0.85
            or item.change_volatility >= 0.90
        ):
            return (
                FreshnessQueueDecision.PRIORITIZE
            )

        if (
            item.historical_stability >= 0.90
            and item.change_frequency <= 0.10
            and item.change_volatility <= 0.10
        ):
            return FreshnessQueueDecision.ENQUEUE

        return FreshnessQueueDecision.ENQUEUE

    # ------------------------------------------------------------------------
    # REASONS
    # ------------------------------------------------------------------------

    def _reasons(
        self,
        item: FreshnessQueueInput,
        score: float,
        lane: FreshnessQueueLane,
    ) -> List[FreshnessQueueReason]:
        reasons: List[
            FreshnessQueueReason
        ] = []

        if item.schedule_id:
            reasons.append(
                FreshnessQueueReason.SCHEDULED_RECRAWL
            )

        if item.frequency_version:
            reasons.append(
                FreshnessQueueReason.ADAPTIVE_FREQUENCY
            )

        if item.recent_change >= 0.70:
            reasons.append(
                FreshnessQueueReason.RECENT_CHANGE
            )

        if item.change_frequency >= 0.70:
            reasons.append(
                FreshnessQueueReason.HIGH_CHANGE_RATE
            )

        if item.change_volatility >= 0.70:
            reasons.append(
                FreshnessQueueReason.HIGH_VOLATILITY
            )

        if item.temporal_sensitivity >= 0.70:
            reasons.append(
                FreshnessQueueReason.TEMPORAL_SENSITIVITY
            )

        if item.source_freshness >= 0.70:
            reasons.append(
                FreshnessQueueReason.SOURCE_FRESHNESS
            )

        if item.feed_update_signal >= 0.70:
            reasons.append(
                FreshnessQueueReason.FEED_UPDATE
            )

        if item.sitemap_update_signal >= 0.70:
            reasons.append(
                FreshnessQueueReason.SITEMAP_UPDATE
            )

        if item.external_update_signal >= 0.70:
            reasons.append(
                FreshnessQueueReason.EXTERNAL_UPDATE
            )

        if (
            item.historical_stability <= 0.25
        ):
            reasons.append(
                FreshnessQueueReason.STALE_RESOURCE
            )

        if not reasons:
            reasons.append(
                FreshnessQueueReason.SCHEDULED_RECRAWL
            )

        if lane == FreshnessQueueLane.RECOVERY:
            reasons.append(
                FreshnessQueueReason.RECOVERY
            )

        if score <= 0.10:
            reasons.append(
                FreshnessQueueReason.INSUFFICIENT_EVIDENCE
            )

        return list(
            dict.fromkeys(reasons)
        )

    # ------------------------------------------------------------------------
    # DEADLINE
    # ------------------------------------------------------------------------

    def _deadline_window(
        self,
        band: FreshnessQueuePriorityBand,
    ) -> float:
        mapping = {
            FreshnessQueuePriorityBand.IMMEDIATE:
                self.policy.immediate_deadline_window_seconds,

            FreshnessQueuePriorityBand.URGENT:
                self.policy.urgent_deadline_window_seconds,

            FreshnessQueuePriorityBand.HIGH:
                self.policy.high_deadline_window_seconds,

            FreshnessQueuePriorityBand.ELEVATED:
                self.policy.elevated_deadline_window_seconds,

            FreshnessQueuePriorityBand.NORMAL:
                self.policy.normal_deadline_window_seconds,

            FreshnessQueuePriorityBand.BACKGROUND:
                self.policy.background_deadline_window_seconds,
        }

        return max(
            self.policy.retry_base_delay_seconds,
            mapping[band],
        )

    def _deadline(
        self,
        item: FreshnessQueueInput,
        band: FreshnessQueuePriorityBand,
    ) -> str:
        if item.deadline:
            parsed = self._parse_timestamp(
                item.deadline
            )

            if parsed is not None:
                return parsed.isoformat()

        now = self._now()

        scheduled = self._parse_timestamp(
            item.scheduled_time
        )

        if scheduled is None:
            scheduled = now

        deadline = (
            scheduled
            + timedelta(
                seconds=self._deadline_window(
                    band
                )
            )
        )

        return deadline.isoformat()

    # ------------------------------------------------------------------------
    # ORDERING
    # ------------------------------------------------------------------------

    def _ordering_key(
        self,
        priority_score: float,
        scheduled_time: Optional[str],
        resource_id: str,
    ) -> str:
        timestamp = (
            scheduled_time
            or self._now_iso()
        )

        priority_component = (
            f"{1.0 - self._clamp(priority_score):.9f}"
        )

        return (
            f"{priority_component}:"
            f"{timestamp}:"
            f"{resource_id}"
        )

    # ------------------------------------------------------------------------
    # CONFIDENCE
    # ------------------------------------------------------------------------

    def _confidence(
        self,
        item: FreshnessQueueInput,
    ) -> float:
        signal_values = [
            item.freshness_score,
            item.urgency_score,
            item.change_frequency,
            item.change_volatility,
            item.recent_change,
            item.semantic_change_score,
            item.temporal_sensitivity,
            item.source_freshness,
            item.historical_stability,
        ]

        signal_activity = sum(
            signal_values
        ) / float(
            max(1, len(signal_values))
        )

        metadata_confidence = 1.0

        if not item.schedule_id:
            metadata_confidence *= 0.90

        if not item.signal_version:
            metadata_confidence *= 0.90

        if not item.history_version:
            metadata_confidence *= 0.90

        if item.partial:
            metadata_confidence *= 0.75

        return self._clamp(
            0.70 * signal_activity
            + 0.30 * metadata_confidence
        )

    # ------------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------------

    def _validate(
        self,
        item: FreshnessQueueInput,
    ) -> Tuple[bool, Optional[str]]:
        if not item.resource_id:
            return False, (
                "resource_id is required"
            )

        if not item.resource_type:
            return False, (
                "resource_type is required"
            )

        if (
            item.partial
            and not self.policy.allow_partial
        ):
            return False, (
                "partial input is not allowed"
            )

        return True, None

    # ------------------------------------------------------------------------
    # BUILD ENTRY
    # ------------------------------------------------------------------------

    def _build_entry(
        self,
        item: FreshnessQueueInput,
        request_id: str,
    ) -> FreshnessQueueEntry:
        priority_score = self._priority_score(
            item
        )

        priority_band = self._priority_band(
            priority_score
        )

        lane = self._lane(
            item,
            priority_band,
        )

        work_type = self._work_type(
            lane,
            item,
        )

        decision = self._decision(
            priority_band,
            item,
        )

        partition_id = (
            self._partition_for_resource(
                item.resource_id,
                item.partition_id,
            )
        )

        deadline = self._deadline(
            item,
            priority_band,
        )

        deduplication_key = (
            self._deduplication_key(
                item.resource_id,
                item.resource_type,
                lane,
            )
        )

        queue_entry_id = self._stable_id(
            "entry",
            (
                f"{deduplication_key}:"
                f"{request_id}:"
                f"{ARCHITECTURE_VERSION}"
            ),
        )

        identity = FreshnessQueueIdentity(
            resource_id=item.resource_id,
            queue_entry_id=queue_entry_id,
            queue_version=ARCHITECTURE_VERSION,
            partition_id=partition_id,
            host_id=item.host_id,
            domain_id=item.domain_id,
            resource_type=item.resource_type,
        )

        lineage = FreshnessQueueLineage(
            resource_id=item.resource_id,
            previous_stage=PREVIOUS_STAGE,
            current_stage=PHASE,
            source_schedule_id=item.schedule_id,
            source_frequency_version=(
                item.frequency_version
            ),
            source_signal_version=(
                item.signal_version
            ),
            source_history_version=(
                item.history_version
            ),
            parent_schedule_ids=(
                [item.schedule_id]
                if item.schedule_id
                else []
            ),
            lineage_metadata={
                "request_id": request_id,
                "architecture_version":
                    ARCHITECTURE_VERSION,
            },
        )

        confidence = self._confidence(
            item
        )

        reasons = self._reasons(
            item,
            priority_score,
            lane,
        )

        now = self._now_iso()

        scheduled_time = (
            item.scheduled_time
            or now
        )

        ordering_key = self._ordering_key(
            priority_score,
            scheduled_time,
            item.resource_id,
        )

        return FreshnessQueueEntry(
            identity=identity,
            lineage=lineage,

            state=FreshnessQueueState.ENQUEUED,

            priority_band=priority_band,

            lane=lane,

            work_type=work_type,

            decision=decision,

            reasons=reasons,

            priority_score=priority_score,

            scheduled_time=scheduled_time,
            deadline=deadline,

            created_at=now,
            updated_at=now,

            attempt_count=0,

            next_retry_time=None,

            claim_state=(
                FreshnessQueueClaimState.AVAILABLE
            ),

            claimed_by="",
            claim_expires_at=None,

            partition_id=partition_id,

            ordering_key=ordering_key,

            deduplication_key=(
                deduplication_key
            ),

            freshness_score=(
                item.freshness_score
            ),
            urgency_score=(
                item.urgency_score
            ),

            change_frequency=(
                item.change_frequency
            ),
            change_volatility=(
                item.change_volatility
            ),

            confidence=confidence,

            partial=item.partial,

            version=ARCHITECTURE_VERSION,

            metadata={
                "resource_type":
                    item.resource_type,
                "host_id":
                    item.host_id,
                "domain_id":
                    item.domain_id,
                "adaptive_interval_seconds":
                    item.adaptive_interval_seconds,
                "semantic_change_score":
                    item.semantic_change_score,
                "temporal_sensitivity":
                    item.temporal_sensitivity,
                "source_freshness":
                    item.source_freshness,
                "feed_update_signal":
                    item.feed_update_signal,
                "sitemap_update_signal":
                    item.sitemap_update_signal,
                "external_update_signal":
                    item.external_update_signal,
                "historical_stability":
                    item.historical_stability,
            },
        )

    # ------------------------------------------------------------------------
    # SINGLE ENQUEUE
    # ------------------------------------------------------------------------

    def enqueue(
        self,
        item: FreshnessQueueInput,
    ) -> FreshnessQueueResult:
        request_id = self._request_id(
            item.resource_id,
            item.schedule_id,
        )

        self._event(
            request_id,
            item.resource_id,
            FreshnessQueueEventType.REQUEST_RECEIVED,
            FreshnessQueueState.RECEIVED,
            {
                "schedule_id":
                    item.schedule_id,
                "resource_type":
                    item.resource_type,
            },
        )

        self._event(
            request_id,
            item.resource_id,
            FreshnessQueueEventType.VALIDATION_STARTED,
            FreshnessQueueState.VALIDATING,
        )

        normalized = self._normalize_input(
            item
        )

        valid, error = self._validate(
            normalized
        )

        if not valid:
            result = FreshnessQueueResult(
                state=FreshnessQueueState.REJECTED,
                queue_entry=None,
                resource_id=(
                    normalized.resource_id
                ),
                request_id=request_id,
                accepted=False,
                duplicate=False,
                partial=False,
                deferred=False,
                rejected=True,
                confidence=0.0,
                error=error,
                created_at=self._now_iso(),
            )

            self._event(
                request_id,
                normalized.resource_id,
                FreshnessQueueEventType.QUEUE_OPERATION_REJECTED,
                FreshnessQueueState.REJECTED,
                {"error": error},
            )

            self.backend.persist_result(
                result
            )

            return result

        self._event(
            request_id,
            normalized.resource_id,
            FreshnessQueueEventType.ENTRY_NORMALIZED,
            FreshnessQueueState.NORMALIZING,
        )

        confidence = self._confidence(
            normalized
        )

        self._checkpoint(
            request_id,
            normalized.resource_id,
            FreshnessQueueCheckpointType.ENTRY_NORMALIZED,
            FreshnessQueueState.NORMALIZING,
            confidence if False else "",
            priority_score=0.0,
            payload={
                "resource_type":
                    normalized.resource_type,
                "confidence":
                    confidence,
            },
        )

        partition_id = (
            self._partition_for_resource(
                normalized.resource_id,
                normalized.partition_id,
            )
        )

        self._event(
            request_id,
            normalized.resource_id,
            FreshnessQueueEventType.PARTITION_ASSIGNED,
            FreshnessQueueState.PARTITIONING,
            {
                "partition_id":
                    partition_id,
            },
        )

        self._checkpoint(
            request_id,
            normalized.resource_id,
            FreshnessQueueCheckpointType.PARTITION_ASSIGNED,
            FreshnessQueueState.PARTITIONING,
            partition_id=partition_id,
            payload={},
        )

        candidate = self._build_entry(
            normalized,
            request_id,
        )

        self._event(
            request_id,
            normalized.resource_id,
            FreshnessQueueEventType.DUPLICATE_CHECK_STARTED,
            FreshnessQueueState.PARTITIONING,
            {
                "deduplication_key":
                    candidate.deduplication_key,
            },
        )

        existing = None

        if self.policy.deduplicate_active_entries:
            existing = self.backend.get_entry(
                candidate.deduplication_key
            )

        self._checkpoint(
            request_id,
            normalized.resource_id,
            FreshnessQueueCheckpointType.DUPLICATE_CHECKED,
            FreshnessQueueState.PARTITIONING,
            partition_id=partition_id,
            payload={
                "duplicate":
                    existing is not None,
            },
        )

        if existing is not None:
            active_states = {
                FreshnessQueueState.ENQUEUED,
                FreshnessQueueState.READY,
                FreshnessQueueState.CLAIMED,
                FreshnessQueueState.PROCESSING,
                FreshnessQueueState.RETRY_PENDING,
                FreshnessQueueState.DEFERRED,
            }

            if existing.state in active_states:
                self._event(
                    request_id,
                    normalized.resource_id,
                    FreshnessQueueEventType.DUPLICATE_ENTRY_SUPPRESSED,
                    existing.state,
                    {
                        "existing_queue_entry_id":
                            existing.identity.queue_entry_id,
                    },
                )

                result = FreshnessQueueResult(
                    state=existing.state,
                    queue_entry=existing,
                    resource_id=(
                        normalized.resource_id
                    ),
                    request_id=request_id,
                    accepted=True,
                    duplicate=True,
                    partial=existing.partial,
                    deferred=(
                        existing.state
                        == FreshnessQueueState.DEFERRED
                    ),
                    rejected=False,
                    confidence=existing.confidence,
                    error=None,
                    created_at=self._now_iso(),
                )

                self.backend.persist_result(
                    result
                )

                return result

        self._event(
            request_id,
            normalized.resource_id,
            FreshnessQueueEventType.PRIORITY_ASSIGNED,
            FreshnessQueueState.READY,
            {
                "priority_score":
                    candidate.priority_score,
                "priority_band":
                    candidate.priority_band.value,
            },
        )

        self._event(
            request_id,
            normalized.resource_id,
            FreshnessQueueEventType.LANE_ASSIGNED,
            FreshnessQueueState.READY,
            {
                "lane":
                    candidate.lane.value,
                "work_type":
                    candidate.work_type.value,
            },
        )

        self._checkpoint(
            request_id,
            normalized.resource_id,
            FreshnessQueueCheckpointType.PRIORITY_ASSIGNED,
            FreshnessQueueState.READY,
            partition_id=partition_id,
            priority_score=(
                candidate.priority_score
            ),
            payload={
                "priority_band":
                    candidate.priority_band.value,
                "lane":
                    candidate.lane.value,
            },
        )

        candidate.state = (
            FreshnessQueueState.READY
        )

        candidate.updated_at = (
            self._now_iso()
        )

        self.backend.persist_entry(
            candidate
        )

        self._event(
            request_id,
            normalized.resource_id,
            FreshnessQueueEventType.QUEUE_ENTRY_CREATED,
            FreshnessQueueState.ENQUEUED,
            {
                "queue_entry_id":
                    candidate.identity.queue_entry_id,
                "partition_id":
                    candidate.partition_id,
                "priority_score":
                    candidate.priority_score,
            },
        )

        self._checkpoint(
            request_id,
            normalized.resource_id,
            FreshnessQueueCheckpointType.ENTRY_CREATED,
            FreshnessQueueState.READY,
            partition_id=partition_id,
            priority_score=(
                candidate.priority_score
            ),
        )

        if (
            normalized.partial
            or candidate.confidence
            < self.policy.minimum_confidence
        ):
            final_state = (
                FreshnessQueueState.PARTIAL
            )

            candidate.state = final_state
            candidate.partial = True

            candidate.updated_at = (
                self._now_iso()
            )

            self.backend.persist_entry(
                candidate
            )

            self._event(
                request_id,
                normalized.resource_id,
                FreshnessQueueEventType.PARTIAL_INPUT_DETECTED,
                final_state,
                {
                    "confidence":
                        candidate.confidence,
                },
            )
        else:
            final_state = (
                FreshnessQueueState.READY
            )

        self._event(
            request_id,
            normalized.resource_id,
            FreshnessQueueEventType.QUEUE_OPERATION_COMPLETED,
            final_state,
            {
                "queue_entry_id":
                    candidate.identity.queue_entry_id,
                "priority_score":
                    candidate.priority_score,
            },
        )

        self._checkpoint(
            request_id,
            normalized.resource_id,
            FreshnessQueueCheckpointType.COMPLETED,
            final_state,
            partition_id=partition_id,
            priority_score=(
                candidate.priority_score
            ),
        )

        result = FreshnessQueueResult(
            state=final_state,
            queue_entry=candidate,
            resource_id=(
                normalized.resource_id
            ),
            request_id=request_id,
            accepted=True,
            duplicate=False,
            partial=candidate.partial,
            deferred=False,
            rejected=False,
            confidence=candidate.confidence,
            error=None,
            created_at=self._now_iso(),
        )

        self.backend.persist_result(
            result
        )

        return result

    # ------------------------------------------------------------------------
    # BATCH ENQUEUE
    # ------------------------------------------------------------------------

    def enqueue_many(
        self,
        items: Iterable[
            FreshnessQueueInput
        ],
    ) -> List[FreshnessQueueResult]:
        values = list(items)

        if (
            len(values)
            > self.policy.max_entries_per_batch
        ):
            raise ValueError(
                "batch exceeds "
                "max_entries_per_batch"
            )

        return [
            self.enqueue(item)
            for item in values
        ]

    # ------------------------------------------------------------------------
    # CLAIM
    # ------------------------------------------------------------------------

    def claim(
        self,
        resource_id: str,
        worker_id: str,
        lane: Optional[
            FreshnessQueueLane
        ] = None,
    ) -> Optional[
        FreshnessQueueEntry
    ]:
        if not resource_id:
            return None

        entries_method = getattr(
            self.backend,
            "entries",
            None,
        )

        if not callable(entries_method):
            return None

        candidates = []

        for entry in entries_method():
            if (
                entry.identity.resource_id
                != resource_id
            ):
                continue

            if lane is not None:
                if entry.lane != lane:
                    continue

            if entry.state not in {
                FreshnessQueueState.READY,
                FreshnessQueueState.PARTIAL,
                FreshnessQueueState.RETRY_PENDING,
            }:
                continue

            if (
                entry.claim_state
                != FreshnessQueueClaimState.AVAILABLE
            ):
                continue

            candidates.append(entry)

        if not candidates:
            return None

        candidates.sort(
            key=lambda entry: (
                -entry.priority_score,
                entry.ordering_key,
                entry.identity.resource_id,
            )
        )

        entry = candidates[0]

        now = self._now()

        entry.state = (
            FreshnessQueueState.CLAIMED
        )

        entry.claim_state = (
            FreshnessQueueClaimState.CLAIMED
        )

        entry.claimed_by = worker_id

        entry.claim_expires_at = (
            now
            + timedelta(
                seconds=(
                    self.policy.claim_lease_seconds
                )
            )
        ).isoformat()

        entry.updated_at = (
            now.isoformat()
        )

        self.backend.persist_entry(
            entry
        )

        request_id = self._request_id(
            resource_id,
            entry.lineage.source_schedule_id,
        )

        self._event(
            request_id,
            resource_id,
            FreshnessQueueEventType.QUEUE_ENTRY_CLAIMED,
            FreshnessQueueState.CLAIMED,
            {
                "worker_id":
                    worker_id,
                "queue_entry_id":
                    entry.identity.queue_entry_id,
            },
        )

        self._checkpoint(
            request_id,
            resource_id,
            FreshnessQueueCheckpointType.ENTRY_CLAIMED,
            FreshnessQueueState.CLAIMED,
            partition_id=entry.partition_id,
            priority_score=(
                entry.priority_score
            ),
            attempt_count=entry.attempt_count,
        )

        return entry

    # ------------------------------------------------------------------------
    # MARK PROCESSING
    # ------------------------------------------------------------------------

    def mark_processing(
        self,
        entry: FreshnessQueueEntry,
    ) -> FreshnessQueueEntry:
        entry.state = (
            FreshnessQueueState.PROCESSING
        )

        entry.updated_at = (
            self._now_iso()
        )

        self.backend.persist_entry(
            entry
        )

        return entry

    # ------------------------------------------------------------------------
    # ACKNOWLEDGE
    # ------------------------------------------------------------------------

    def acknowledge(
        self,
        entry: FreshnessQueueEntry,
        success: bool = True,
    ) -> FreshnessQueueEntry:
        now = self._now_iso()

        if success:
            entry.state = (
                FreshnessQueueState.ACKNOWLEDGED
            )

            entry.claim_state = (
                FreshnessQueueClaimState.ACKNOWLEDGED
            )

            entry.claimed_by = ""
            entry.claim_expires_at = None
            entry.next_retry_time = None

            self.backend.persist_entry(
                entry
            )

            request_id = self._request_id(
                entry.identity.resource_id,
                entry.lineage.source_schedule_id,
            )

            self._event(
                request_id,
                entry.identity.resource_id,
                FreshnessQueueEventType.QUEUE_ENTRY_ACKNOWLEDGED,
                FreshnessQueueState.ACKNOWLEDGED,
                {
                    "queue_entry_id":
                        entry.identity.queue_entry_id,
                },
            )

            self._checkpoint(
                request_id,
                entry.identity.resource_id,
                FreshnessQueueCheckpointType.ENTRY_ACKNOWLEDGED,
                FreshnessQueueState.ACKNOWLEDGED,
                partition_id=entry.partition_id,
                priority_score=(
                    entry.priority_score
                ),
                attempt_count=entry.attempt_count,
            )

        else:
            entry = self.retry(
                entry
            )

        entry.updated_at = now

        self.backend.persist_entry(
            entry
        )

        return entry

    # ------------------------------------------------------------------------
    # RETRY
    # ------------------------------------------------------------------------

    def retry(
        self,
        entry: FreshnessQueueEntry,
    ) -> FreshnessQueueEntry:
        entry.attempt_count += 1

        entry.claim_state = (
            FreshnessQueueClaimState.RELEASED
        )

        entry.claimed_by = ""
        entry.claim_expires_at = None

        if (
            entry.attempt_count
            >= self.policy.max_attempts
        ):
            entry.state = (
                FreshnessQueueState.FAILED
            )

            entry.next_retry_time = None

            self.backend.persist_entry(
                entry
            )

            return entry

        exponent = max(
            0,
            entry.attempt_count - 1,
        )

        delay = (
            self.policy.retry_base_delay_seconds
            * (2.0 ** exponent)
        )

        delay = min(
            delay,
            self.policy.retry_max_delay_seconds,
        )

        retry_time = (
            self._now()
            + timedelta(
                seconds=delay
            )
        )

        entry.next_retry_time = (
            retry_time.isoformat()
        )

        entry.state = (
            FreshnessQueueState.RETRY_PENDING
        )

        entry.updated_at = (
            self._now_iso()
        )

        self.backend.persist_entry(
            entry
        )

        request_id = self._request_id(
            entry.identity.resource_id,
            entry.lineage.source_schedule_id,
        )

        self._event(
            request_id,
            entry.identity.resource_id,
            FreshnessQueueEventType.RETRY_SCHEDULED,
            FreshnessQueueState.RETRY_PENDING,
            {
                "attempt_count":
                    entry.attempt_count,
                "next_retry_time":
                    entry.next_retry_time,
            },
        )

        self._checkpoint(
            request_id,
            entry.identity.resource_id,
            FreshnessQueueCheckpointType.RETRY_CREATED,
            FreshnessQueueState.RETRY_PENDING,
            partition_id=entry.partition_id,
            priority_score=(
                entry.priority_score
            ),
            attempt_count=entry.attempt_count,
        )

        return entry

    # ------------------------------------------------------------------------
    # DEFER
    # ------------------------------------------------------------------------

    def defer(
        self,
        entry: FreshnessQueueEntry,
        defer_until: Optional[str] = None,
    ) -> FreshnessQueueEntry:
        entry.state = (
            FreshnessQueueState.DEFERRED
        )

        entry.claim_state = (
            FreshnessQueueClaimState.AVAILABLE
        )

        entry.claimed_by = ""
        entry.claim_expires_at = None

        entry.next_retry_time = (
            defer_until
        )

        entry.updated_at = (
            self._now_iso()
        )

        if (
            FreshnessQueueReason.MANUAL_DEFERMENT
            not in entry.reasons
        ):
            entry.reasons.append(
                FreshnessQueueReason.MANUAL_DEFERMENT
            )

        self.backend.persist_entry(
            entry
        )

        request_id = self._request_id(
            entry.identity.resource_id,
            entry.lineage.source_schedule_id,
        )

        self._event(
            request_id,
            entry.identity.resource_id,
            FreshnessQueueEventType.ENTRY_DEFERRED,
            FreshnessQueueState.DEFERRED,
            {
                "defer_until":
                    defer_until,
            },
        )

        return entry

    # ------------------------------------------------------------------------
    # CANCEL
    # ------------------------------------------------------------------------

    def cancel(
        self,
        entry: FreshnessQueueEntry,
        reason: FreshnessQueueReason = (
            FreshnessQueueReason.OBSOLETE_SCHEDULE
        ),
    ) -> FreshnessQueueEntry:
        entry.state = (
            FreshnessQueueState.CANCELLED
        )

        entry.claim_state = (
            FreshnessQueueClaimState.RELEASED
        )

        entry.claimed_by = ""
        entry.claim_expires_at = None
        entry.next_retry_time = None

        if reason not in entry.reasons:
            entry.reasons.append(reason)

        entry.updated_at = (
            self._now_iso()
        )

        self.backend.persist_entry(
            entry
        )

        request_id = self._request_id(
            entry.identity.resource_id,
            entry.lineage.source_schedule_id,
        )

        self._event(
            request_id,
            entry.identity.resource_id,
            FreshnessQueueEventType.ENTRY_CANCELLED,
            FreshnessQueueState.CANCELLED,
            {
                "reason":
                    reason.value,
            },
        )

        return entry

    # ------------------------------------------------------------------------
    # EXPIRE
    # ------------------------------------------------------------------------

    def expire(
        self,
        entry: FreshnessQueueEntry,
    ) -> FreshnessQueueEntry:
        entry.state = (
            FreshnessQueueState.EXPIRED
        )

        entry.claim_state = (
            FreshnessQueueClaimState.EXPIRED
        )

        entry.claimed_by = ""
        entry.claim_expires_at = None

        entry.updated_at = (
            self._now_iso()
        )

        self.backend.persist_entry(
            entry
        )

        request_id = self._request_id(
            entry.identity.resource_id,
            entry.lineage.source_schedule_id,
        )

        self._event(
            request_id,
            entry.identity.resource_id,
            FreshnessQueueEventType.ENTRY_EXPIRED,
            FreshnessQueueState.EXPIRED,
        )

        return entry

    # ------------------------------------------------------------------------
    # CLAIM LEASE RECOVERY
    # ------------------------------------------------------------------------

    def recover_expired_claims(
        self,
    ) -> List[FreshnessQueueEntry]:
        entries_method = getattr(
            self.backend,
            "entries",
            None,
        )

        if not callable(entries_method):
            return []

        now = self._now()

        recovered: List[
            FreshnessQueueEntry
        ] = []

        for entry in entries_method():
            if (
                entry.claim_state
                != FreshnessQueueClaimState.CLAIMED
            ):
                continue

            expires_at = self._parse_timestamp(
                entry.claim_expires_at
            )

            if (
                expires_at is None
                or expires_at > now
            ):
                continue

            entry.claim_state = (
                FreshnessQueueClaimState.RELEASED
            )

            entry.claimed_by = ""
            entry.claim_expires_at = None

            entry.state = (
                FreshnessQueueState.RETRY_PENDING
            )

            entry.next_retry_time = (
                now.isoformat()
            )

            entry.updated_at = (
                now.isoformat()
            )

            if (
                FreshnessQueueReason.RECOVERY
                not in entry.reasons
            ):
                entry.reasons.append(
                    FreshnessQueueReason.RECOVERY
                )

            self.backend.persist_entry(
                entry
            )

            recovered.append(entry)

        return recovered

    # ------------------------------------------------------------------------
    # ACTIVATE RETRY/DEFERRED ENTRIES
    # ------------------------------------------------------------------------

    def activate_due_entries(
        self,
    ) -> List[FreshnessQueueEntry]:
        entries_method = getattr(
            self.backend,
            "entries",
            None,
        )

        if not callable(entries_method):
            return []

        now = self._now()

        activated: List[
            FreshnessQueueEntry
        ] = []

        for entry in entries_method():
            if entry.state not in {
                FreshnessQueueState.RETRY_PENDING,
                FreshnessQueueState.DEFERRED,
            }:
                continue

            if not entry.next_retry_time:
                continue

            due_time = self._parse_timestamp(
                entry.next_retry_time
            )

            if (
                due_time is None
                or due_time > now
            ):
                continue

            entry.state = (
                FreshnessQueueState.READY
            )

            entry.claim_state = (
                FreshnessQueueClaimState.AVAILABLE
            )

            entry.next_retry_time = None
            entry.updated_at = (
                now.isoformat()
            )

            self.backend.persist_entry(
                entry
            )

            activated.append(entry)

        return activated

    # ------------------------------------------------------------------------
    # EXPIRE OBSOLETE ENTRIES
    # ------------------------------------------------------------------------

    def expire_stale_entries(
        self,
    ) -> List[FreshnessQueueEntry]:
        entries_method = getattr(
            self.backend,
            "entries",
            None,
        )

        if not callable(entries_method):
            return []

        now = self._now()

        expired: List[
            FreshnessQueueEntry
        ] = []

        for entry in entries_method():
            created_at = self._parse_timestamp(
                entry.created_at
            )

            if created_at is None:
                continue

            age = (
                now - created_at
            ).total_seconds()

            if age < (
                self.policy.stale_entry_after_seconds
            ):
                continue

            if entry.state in {
                FreshnessQueueState.ACKNOWLEDGED,
                FreshnessQueueState.CANCELLED,
                FreshnessQueueState.EXPIRED,
                FreshnessQueueState.FAILED,
            }:
                continue

            self.expire(entry)

            if (
                FreshnessQueueReason.OBSOLETE_SCHEDULE
                not in entry.reasons
            ):
                entry.reasons.append(
                    FreshnessQueueReason.OBSOLETE_SCHEDULE
                )

            self.backend.persist_entry(
                entry
            )

            expired.append(entry)

        return expired

    # ------------------------------------------------------------------------
    # QUEUE SNAPSHOT
    # ------------------------------------------------------------------------

    def queue_snapshot(
        self,
    ) -> Dict[str, Any]:
        entries_method = getattr(
            self.backend,
            "entries",
            None,
        )

        if not callable(entries_method):
            return {
                "total": 0,
                "states": {},
                "lanes": {},
                "priority_bands": {},
                "partitions": {},
            }

        entries = list(
            entries_method()
        )

        states: Dict[str, int] = {}
        lanes: Dict[str, int] = {}
        priority_bands: Dict[str, int] = {}
        partitions: Dict[str, int] = {}

        for entry in entries:
            states[
                entry.state.value
            ] = (
                states.get(
                    entry.state.value,
                    0,
                )
                + 1
            )

            lanes[
                entry.lane.value
            ] = (
                lanes.get(
                    entry.lane.value,
                    0,
                )
                + 1
            )

            priority_bands[
                entry.priority_band.value
            ] = (
                priority_bands.get(
                    entry.priority_band.value,
                    0,
                )
                + 1
            )

            partitions[
                entry.partition_id
            ] = (
                partitions.get(
                    entry.partition_id,
                    0,
                )
                + 1
            )

        return {
            "total": len(entries),
            "states": states,
            "lanes": lanes,
            "priority_bands": priority_bands,
            "partitions": partitions,
        }

    # ------------------------------------------------------------------------
    # METADATA ACCESS
    # ------------------------------------------------------------------------

    def events(
        self,
    ) -> List[FreshnessQueueEvent]:
        method = getattr(
            self.backend,
            "events",
            None,
        )

        if callable(method):
            return list(method())

        return []

    def checkpoints(
        self,
    ) -> List[FreshnessQueueCheckpoint]:
        method = getattr(
            self.backend,
            "checkpoints",
            None,
        )

        if callable(method):
            return list(method())

        return []

    def entries(
        self,
    ) -> List[FreshnessQueueEntry]:
        method = getattr(
            self.backend,
            "entries",
            None,
        )

        if callable(method):
            return list(method())

        return []

    def results(
        self,
    ) -> List[FreshnessQueueResult]:
        method = getattr(
            self.backend,
            "results",
            None,
        )

        if callable(method):
            return list(method())

        return []

    # ------------------------------------------------------------------------
    # ARCHITECTURE DESCRIPTION
    # ------------------------------------------------------------------------

    def architecture(
        self,
    ) -> Dict[str, Any]:
        return {
            "phase": PHASE,
            "name": (
                "URL/Document Freshness Queues"
            ),
            "version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,

            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "purpose": (
                "Convert scheduled and adaptive "
                "freshness work into durable, "
                "partition-aware URL/document "
                "freshness queue entries."
            ),

            "inputs": [
                "phase13.3_recrawl_schedules",
                "phase13.4_adaptive_frequency",
                "phase13.1_freshness_signals",
                "phase13.2_change_signals",
                "resource_identity",
                "recrawl_deadlines",
                "partition_metadata",
            ],

            "outputs": [
                "freshness_queue_entries",
                "queue_priority",
                "queue_priority_band",
                "queue_lane",
                "queue_work_type",
                "queue_partition",
                "deduplication_key",
                "ordering_key",
                "claim_metadata",
                "retry_metadata",
                "queue_state",
                "queue_lineage",
                "queue_checkpoints",
            ],

            "queue_properties": {
                "durable": True,
                "partition_aware": True,
                "deduplicated": True,
                "priority_aware": True,
                "lane_aware": True,
                "retryable": True,
                "deferable": True,
                "expirable": True,
                "claimable": True,
                "lease_recovery": True,
                "checkpointable":
                    self.policy.checkpoint_enabled,
                "restartable": True,
                "incremental": True,
                "deterministic":
                    self.policy.deterministic,
                "backend_replaceable": True,
                "lineage_preserving": True,
                "provenance_preserving": True,
            },

            "distributed_execution": {
                "distributed": True,
                "partition_local": True,
                "resource_partitionable": True,
                "host_partitionable": True,
                "domain_partitionable": True,
                "horizontally_scalable": True,
                "parallel_consumption": True,
                "partition_rebalancing_supported": True,
                "worker_claim_leases": True,
            },

            "global_limits": {
                "urls": None,
                "documents": None,
                "resources": None,
                "hosts": None,
                "domains": None,
                "queue_entries": None,
                "queue_partitions": None,
                "workers": None,
                "retry_history": None,
                "freshness_queues": None,
            },

            "per_unit_limits": {
                "max_entries_per_batch":
                    self.policy.max_entries_per_batch,

                "max_attempts":
                    self.policy.max_attempts,

                "claim_lease_seconds":
                    self.policy.claim_lease_seconds,
            },

            "state_machine": [
                "received",
                "validating",
                "normalizing",
                "partitioning",
                "enqueued",
                "ready",
                "claimed",
                "processing",
                "acknowledged",
                "retry_pending",
                "deferred",
                "expired",
                "cancelled",
                "partial",
                "rejected",
                "failed",
            ],

            "stage_boundary": {
                "13.1": (
                    "determines freshness attention "
                    "and urgency"
                ),

                "13.2": (
                    "determines content change "
                    "and change volatility"
                ),

                "13.3": (
                    "determines when a resource "
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
                    "will orchestrate distributed "
                    "recrawl execution across "
                    "the freshness queues"
                ),
            },

            "does_not": [
                "execute_crawling",
                "perform_http_requests",
                "fetch_web_resources",
                "assign_crawler_workers",
                "perform_global_recrawl_orchestration",
                "perform_crawl_execution",
                "mutate_search_index",
                "perform_final_ranking",
                "train_ranking_models",
                "classify_spam",
                "replace_phase13.3_scheduling",
                "replace_phase13.4_adaptive_frequency",
                "depend_on_google_search_api",
                "depend_on_google_index",
                "depend_on_google_crawler",
                "depend_on_google_infrastructure",
                "depend_on_google_ranking_technology",
            ],

            "next_stage": NEXT_STAGE,

            "next_stage_name": (
                "Distributed Recrawl Orchestration"
            ),

            "phase13_progression": [
                "13.1 Crawl-Freshness Prioritization",
                "13.2 Change Detection / Content Change Signals",
                "13.3 Recrawl Scheduling",
                "13.4 Adaptive Recrawl Frequency",
                "13.5 URL/Document Freshness Queues",
                "13.6 Distributed Recrawl Orchestration",
                "13.7 Freshness-Aware Crawl Resource Allocation",
                "13.8 Global Recrawl Coordination / Failure Recovery",
                "13.9 Final Freshness + Recrawling Architecture",
            ],
        }


# ============================================================================
# ALIASES
# ============================================================================


URLDocumentFreshnessQueues = (
    FreshnessQueueArchitecture
)

GlobalFreshnessQueues = (
    FreshnessQueueArchitecture
)

Phase13_5FreshnessQueues = (
    FreshnessQueueArchitecture
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

    "FreshnessQueueState",
    "FreshnessQueuePriorityBand",
    "FreshnessQueueLane",
    "FreshnessQueueWorkType",
    "FreshnessQueueDecision",
    "FreshnessQueueReason",
    "FreshnessQueueEventType",
    "FreshnessQueueCheckpointType",
    "FreshnessQueueClaimState",

    "FreshnessQueueIdentity",
    "FreshnessQueueLineage",
    "FreshnessQueueInput",
    "FreshnessQueuePolicy",
    "FreshnessQueueEntry",
    "FreshnessQueueResult",
    "FreshnessQueueCheckpoint",
    "FreshnessQueueEvent",

    "FreshnessQueueBackend",
    "InMemoryFreshnessQueueMetadata",

    "FreshnessQueueArchitecture",

    "URLDocumentFreshnessQueues",
    "GlobalFreshnessQueues",
    "Phase13_5FreshnessQueues",
]
