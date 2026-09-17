"""
OUR SEARCH
Phase 13.3 — Recrawl Scheduling

Production architecture for distributed recrawl scheduling at enormous
public-Web scale.

Scale target:
    billions -> trillions of publicly accessible Web resources

This module is independent of:
    - Google Search API
    - Google index
    - Google crawler
    - Google infrastructure
    - Google ranking technology

Responsibilities:
    - Convert freshness and content-change signals into recrawl schedules.
    - Estimate the next appropriate recrawl time for each resource.
    - Adapt scheduling to historical change behavior.
    - Prioritize urgent and stale resources.
    - Produce deterministic scheduling decisions.
    - Support distributed scheduling and partition-local computation.
    - Preserve provenance and scheduling lineage.
    - Support checkpointing and restartability.
    - Support rescheduling and schedule versioning.
    - Keep persistence backend-replaceable.

This module does NOT:
    - execute HTTP requests
    - crawl Web resources
    - assign crawler workers
    - perform network fetching
    - mutate the search index
    - perform final search ranking
    - classify spam
    - perform global worker orchestration
"""


from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from math import exp, isfinite
from typing import Dict, List, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ---------------------------------------------------------------------------
# Architecture constants
# ---------------------------------------------------------------------------

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "recrawl-scheduling.v1"
PHASE = "13.3"
PREVIOUS_STAGE = "13.2"
NEXT_STAGE = "13.4"


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class RecrawlSchedulingState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    SIGNAL_NORMALIZATION = "signal_normalization"
    SCHEDULE_ESTIMATION = "schedule_estimation"
    PRIORITY_CALCULATION = "priority_calculation"
    DEADLINE_CALCULATION = "deadline_calculation"
    DECISION = "decision"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class RecrawlPriorityBand(str, Enum):
    BACKGROUND = "background"
    NORMAL = "normal"
    ELEVATED = "elevated"
    HIGH = "high"
    URGENT = "urgent"
    IMMEDIATE = "immediate"


class RecrawlDecision(str, Enum):
    DEFER = "defer"
    SCHEDULE = "schedule"
    PRIORITIZE = "prioritize"
    HIGH_PRIORITY = "high_priority"
    URGENT = "urgent"
    IMMEDIATE = "immediate"


class RecrawlReason(str, Enum):
    STALE_RESOURCE = "stale_resource"
    RECENT_CONTENT_CHANGE = "recent_content_change"
    HIGH_CHANGE_RATE = "high_change_rate"
    HIGH_VOLATILITY = "high_volatility"
    TEMPORAL_SENSITIVITY = "temporal_sensitivity"
    SOURCE_FRESHNESS = "source_freshness"
    FEED_UPDATE = "feed_update"
    SITEMAP_UPDATE = "sitemap_update"
    EXTERNAL_UPDATE = "external_update"
    FIRST_SCHEDULE = "first_schedule"
    MISSED_DEADLINE = "missed_deadline"
    LOW_CHANGE_ACTIVITY = "low_change_activity"
    STABLE_RESOURCE = "stable_resource"


class RecrawlScheduleType(str, Enum):
    INITIAL = "initial"
    NORMAL = "normal"
    ACCELERATED = "accelerated"
    URGENT = "urgent"
    RECOVERY = "recovery"


class RecrawlEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    SIGNALS_NORMALIZED = "signals_normalized"
    SCHEDULE_ESTIMATION_STARTED = "schedule_estimation_started"
    BASE_INTERVAL_CALCULATED = "base_interval_calculated"
    PRIORITY_CALCULATED = "priority_calculated"
    DEADLINE_CALCULATED = "deadline_calculated"
    SCHEDULE_CREATED = "schedule_created"
    SCHEDULE_UPDATED = "schedule_updated"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    SCHEDULING_COMPLETED = "scheduling_completed"
    SCHEDULING_DEFERRED = "scheduling_deferred"
    SCHEDULING_REJECTED = "scheduling_rejected"
    SCHEDULING_FAILED = "scheduling_failed"


class RecrawlCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    SIGNALS_NORMALIZED = "signals_normalized"
    INTERVAL_CALCULATED = "interval_calculated"
    PRIORITY_CALCULATED = "priority_calculated"
    DEADLINE_CALCULATED = "deadline_calculated"
    SCHEDULE_CREATED = "schedule_created"
    COMPLETED = "completed"


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecrawlScheduleIdentity:
    request_id: str
    resource_id: str
    canonical_url: str
    hostname: str

    partition_id: Optional[str] = None
    shard_id: Optional[str] = None

    version: str = ARCHITECTURE_VERSION


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


@dataclass
class RecrawlScheduleLineage:
    source_system: str = "phase13.3-recrawl-scheduling"
    source_version: str = ARCHITECTURE_VERSION

    previous_stage: str = (
        "phase13.2-content-change-detection-signals"
    )

    parent_request_ids: List[str] = field(
        default_factory=list
    )

    source_signal_ids: List[str] = field(
        default_factory=list
    )

    calculation_timestamp: str = ""

    provenance_preserved: bool = True


# ---------------------------------------------------------------------------
# Input signals
# ---------------------------------------------------------------------------


@dataclass
class RecrawlFreshnessSignals:
    """
    Freshness and change signals consumed from Phase 13.1/13.2.

    All normalized signal values are expected to be in [0, 1].
    """

    freshness_score: float = 0.0
    urgency_score: float = 0.0

    content_change_score: float = 0.0
    text_change_score: float = 0.0
    structural_change_score: float = 0.0
    metadata_change_score: float = 0.0
    link_change_score: float = 0.0
    media_change_score: float = 0.0
    semantic_change_score: float = 0.0

    change_frequency: float = 0.0
    change_volatility: float = 0.0

    source_freshness: float = 0.0
    temporal_volatility: float = 0.0
    content_volatility: float = 0.0

    query_time_sensitivity: float = 0.0
    event_time_sensitivity: float = 0.0

    recent_change: float = 0.0
    feed_update_signal: float = 0.0
    sitemap_update_signal: float = 0.0
    external_update_signal: float = 0.0

    historical_stability: float = 0.5
    prior_freshness_accuracy: float = 0.5

    confidence: float = 1.0

    partial: bool = False


# ---------------------------------------------------------------------------
# Historical scheduling state
# ---------------------------------------------------------------------------


@dataclass
class RecrawlHistory:
    resource_id: str

    schedule_count: int = 0
    completed_recrawl_count: int = 0
    missed_deadline_count: int = 0

    first_scheduled_at: Optional[str] = None
    last_scheduled_at: Optional[str] = None
    last_recrawled_at: Optional[str] = None

    last_successful_change_detected_at: Optional[str] = None

    average_change_interval_days: Optional[float] = None
    average_recrawl_interval_days: Optional[float] = None

    historical_change_frequency: float = 0.0
    historical_schedule_accuracy: float = 0.5

    previous_priority_score: float = 0.0
    previous_interval_days: Optional[float] = None

    partial: bool = False


# ---------------------------------------------------------------------------
# Scheduling policy
# ---------------------------------------------------------------------------


@dataclass
class RecrawlSchedulingPolicy:
    """
    Per-resource and per-batch policy.

    These limits are local processing controls. They do not impose a
    global Web-scale ceiling.
    """

    max_resources_per_batch: int = 100_000

    minimum_confidence: float = 0.10

    allow_partial: bool = True
    deterministic: bool = True
    checkpoint_enabled: bool = True

    # Interval bounds.
    minimum_interval_seconds: int = 60
    default_interval_seconds: int = 86_400
    maximum_interval_seconds: int = 365 * 86_400

    # Historical learning influence.
    historical_change_weight: float = 1.35
    historical_schedule_weight: float = 0.75

    # Freshness/change influences.
    freshness_weight: float = 1.20
    urgency_weight: float = 1.50
    change_frequency_weight: float = 1.35
    change_volatility_weight: float = 1.25
    recent_change_weight: float = 1.50
    semantic_change_weight: float = 1.25
    temporal_sensitivity_weight: float = 1.00
    source_freshness_weight: float = 0.90

    # External signals.
    feed_signal_weight: float = 0.90
    sitemap_signal_weight: float = 0.70
    external_signal_weight: float = 0.60

    # Stability.
    stability_weight: float = 0.65

    # Priority thresholds.
    defer_threshold: float = 0.15
    schedule_threshold: float = 0.30
    prioritize_threshold: float = 0.50
    high_priority_threshold: float = 0.72
    urgent_threshold: float = 0.90

    # Interval multipliers.
    stable_multiplier: float = 2.50
    low_activity_multiplier: float = 1.75
    normal_multiplier: float = 1.00
    elevated_multiplier: float = 0.65
    high_multiplier: float = 0.40
    urgent_multiplier: float = 0.15
    immediate_multiplier: float = 0.02

    # Deadline window.
    normal_deadline_factor: float = 2.0
    urgent_deadline_factor: float = 0.50
    immediate_deadline_factor: float = 0.15

    # Jitter.
    #
    # Scheduling architecture intentionally supports deterministic jitter
    # derived from the resource identity. This reduces synchronized bursts
    # without requiring a global central clock.
    jitter_fraction: float = 0.05


# ---------------------------------------------------------------------------
# Schedule output
# ---------------------------------------------------------------------------


@dataclass
class RecrawlSchedule:
    schedule_id: str
    resource_id: str
    canonical_url: str

    schedule_type: RecrawlScheduleType

    created_at: str
    scheduled_at: str
    deadline_at: str

    interval_seconds: int

    priority_score: float
    confidence: float

    priority_band: RecrawlPriorityBand
    decision: RecrawlDecision

    reasons: List[RecrawlReason] = field(
        default_factory=list
    )

    partial: bool = False

    schedule_version: int = 1

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass
class RecrawlSchedulingResult:
    identity: RecrawlScheduleIdentity
    lineage: RecrawlScheduleLineage

    state: RecrawlSchedulingState

    schedule: RecrawlSchedule

    created_at: str
    completed_at: Optional[str] = None

    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


@dataclass
class RecrawlSchedulingCheckpoint:
    checkpoint_id: str

    request_id: str
    resource_id: str

    checkpoint_type: RecrawlCheckpointType
    state: RecrawlSchedulingState

    created_at: str

    schedule_id: Optional[str] = None

    interval_seconds: Optional[int] = None
    priority_score: Optional[float] = None
    confidence: Optional[float] = None

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Event
# ---------------------------------------------------------------------------


@dataclass
class RecrawlSchedulingEvent:
    event_id: str

    request_id: str
    resource_id: str

    event_type: RecrawlEventType

    timestamp: str

    state: RecrawlSchedulingState

    metadata: Dict[str, str] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Backend abstraction
# ---------------------------------------------------------------------------


class RecrawlSchedulingBackend(Protocol):
    def persist_event(
        self,
        event: RecrawlSchedulingEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: RecrawlSchedulingCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: RecrawlSchedulingResult,
    ) -> None:
        ...

    def get_result(
        self,
        request_id: str,
    ) -> Optional[RecrawlSchedulingResult]:
        ...


class InMemoryRecrawlSchedulingMetadata:
    """
    Reference backend.

    Production deployments can replace this with distributed durable
    scheduling metadata storage.
    """

    def __init__(self) -> None:
        self._events: Dict[
            str,
            RecrawlSchedulingEvent,
        ] = {}

        self._checkpoints: Dict[
            str,
            RecrawlSchedulingCheckpoint,
        ] = {}

        self._results: Dict[
            str,
            RecrawlSchedulingResult,
        ] = {}

    def persist_event(
        self,
        event: RecrawlSchedulingEvent,
    ) -> None:
        self._events[event.event_id] = event

    def persist_checkpoint(
        self,
        checkpoint: RecrawlSchedulingCheckpoint,
    ) -> None:
        self._checkpoints[
            checkpoint.checkpoint_id
        ] = checkpoint

    def persist_result(
        self,
        result: RecrawlSchedulingResult,
    ) -> None:
        self._results[
            result.identity.request_id
        ] = result

    def get_result(
        self,
        request_id: str,
    ) -> Optional[RecrawlSchedulingResult]:
        return self._results.get(request_id)

    def events(
        self,
    ) -> List[RecrawlSchedulingEvent]:
        return list(self._events.values())

    def checkpoints(
        self,
    ) -> List[RecrawlSchedulingCheckpoint]:
        return list(self._checkpoints.values())

    def results(
        self,
    ) -> List[RecrawlSchedulingResult]:
        return list(self._results.values())


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------


class RecrawlSchedulingArchitecture:
    """
    Phase 13.3 production scheduling architecture.

    Flow:

        Phase 13.1 freshness priority
                    +
        Phase 13.2 change signals
                    +
        recrawl history
                    |
                    v
        signal normalization
                    |
                    v
        change-interval estimation
                    |
                    v
        priority calculation
                    |
                    v
        next recrawl interval
                    |
                    v
        deadline calculation
                    |
                    v
        deterministic distributed schedule
                    |
                    v
        Phase 13.4 adaptive recrawl frequency
    """

    def __init__(
        self,
        backend: Optional[
            RecrawlSchedulingBackend
        ] = None,
        policy: Optional[
            RecrawlSchedulingPolicy
        ] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryRecrawlSchedulingMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else RecrawlSchedulingPolicy()
        )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return datetime.now(
            timezone.utc
        ).isoformat()

    @staticmethod
    def _clamp(
        value: float,
        minimum: float = 0.0,
        maximum: float = 1.0,
    ) -> float:
        if not isfinite(value):
            return minimum

        return max(
            minimum,
            min(maximum, value),
        )

    @staticmethod
    def _safe_float(
        value: object,
        default: float = 0.0,
    ) -> float:
        try:
            result = float(value)

            if not isfinite(result):
                return default

            return result

        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_timestamp(
        value: Optional[str],
    ) -> Optional[datetime]:
        if not value:
            return None

        try:
            normalized = value

            if normalized.endswith("Z"):
                normalized = (
                    normalized[:-1]
                    + "+00:00"
                )

            parsed = datetime.fromisoformat(
                normalized
            )

            if parsed.tzinfo is None:
                parsed = parsed.replace(
                    tzinfo=timezone.utc
                )

            return parsed.astimezone(
                timezone.utc
            )

        except (
            TypeError,
            ValueError,
        ):
            return None

    @classmethod
    def _age_days(
        cls,
        timestamp: Optional[str],
        now: Optional[datetime] = None,
    ) -> Optional[float]:
        parsed = cls._parse_timestamp(
            timestamp
        )

        if parsed is None:
            return None

        reference = (
            now
            or datetime.now(timezone.utc)
        )

        seconds = max(
            0.0,
            (
                reference - parsed
            ).total_seconds(),
        )

        return seconds / 86_400.0

    @staticmethod
    def _deterministic_unit(
        resource_id: str,
        schedule_version: int,
    ) -> float:
        """
        Deterministic pseudo-random value in [0, 1).

        Used only for schedule jitter. It does not require a global random
        service and produces the same schedule for identical inputs.
        """

        material = (
            f"{resource_id}:"
            f"{schedule_version}"
        )

        value = 0

        for character in material:
            value = (
                (
                    value * 131
                    + ord(character)
                )
                & 0xFFFFFFFF
            )

        return value / float(
            0x100000000
        )

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def _event(
        self,
        identity: RecrawlScheduleIdentity,
        event_type: RecrawlEventType,
        state: RecrawlSchedulingState,
        metadata: Optional[
            Dict[str, str]
        ] = None,
    ) -> RecrawlSchedulingEvent:
        event = RecrawlSchedulingEvent(
            event_id=str(uuid4()),
            request_id=identity.request_id,
            resource_id=identity.resource_id,
            event_type=event_type,
            timestamp=self._now(),
            state=state,
            metadata=metadata or {},
        )

        self.backend.persist_event(
            event
        )

        return event

    # ------------------------------------------------------------------
    # Checkpoints
    # ------------------------------------------------------------------

    def _checkpoint(
        self,
        identity: RecrawlScheduleIdentity,
        checkpoint_type: RecrawlCheckpointType,
        state: RecrawlSchedulingState,
        schedule: Optional[
            RecrawlSchedule
        ] = None,
        metadata: Optional[
            Dict[str, str]
        ] = None,
    ) -> RecrawlSchedulingCheckpoint:
        checkpoint = (
            RecrawlSchedulingCheckpoint(
                checkpoint_id=str(uuid4()),
                request_id=identity.request_id,
                resource_id=identity.resource_id,
                checkpoint_type=checkpoint_type,
                state=state,
                created_at=self._now(),
                schedule_id=(
                    schedule.schedule_id
                    if schedule is not None
                    else None
                ),
                interval_seconds=(
                    schedule.interval_seconds
                    if schedule is not None
                    else None
                ),
                priority_score=(
                    schedule.priority_score
                    if schedule is not None
                    else None
                ),
                confidence=(
                    schedule.confidence
                    if schedule is not None
                    else None
                ),
                metadata=metadata or {},
            )
        )

        self.backend.persist_checkpoint(
            checkpoint
        )

        return checkpoint

    # ------------------------------------------------------------------
    # Signal normalization
    # ------------------------------------------------------------------

    def _normalize_signals(
        self,
        signals: RecrawlFreshnessSignals,
    ) -> RecrawlFreshnessSignals:
        numeric_fields = [
            "freshness_score",
            "urgency_score",
            "content_change_score",
            "text_change_score",
            "structural_change_score",
            "metadata_change_score",
            "link_change_score",
            "media_change_score",
            "semantic_change_score",
            "change_frequency",
            "change_volatility",
            "source_freshness",
            "temporal_volatility",
            "content_volatility",
            "query_time_sensitivity",
            "event_time_sensitivity",
            "recent_change",
            "feed_update_signal",
            "sitemap_update_signal",
            "external_update_signal",
            "historical_stability",
            "prior_freshness_accuracy",
            "confidence",
        ]

        normalized_values: Dict[
            str,
            float,
        ] = {}

        for field_name in numeric_fields:
            normalized_values[field_name] = (
                self._clamp(
                    self._safe_float(
                        getattr(
                            signals,
                            field_name,
                        )
                    )
                )
            )

        return RecrawlFreshnessSignals(
            freshness_score=(
                normalized_values[
                    "freshness_score"
                ]
            ),
            urgency_score=(
                normalized_values[
                    "urgency_score"
                ]
            ),
            content_change_score=(
                normalized_values[
                    "content_change_score"
                ]
            ),
            text_change_score=(
                normalized_values[
                    "text_change_score"
                ]
            ),
            structural_change_score=(
                normalized_values[
                    "structural_change_score"
                ]
            ),
            metadata_change_score=(
                normalized_values[
                    "metadata_change_score"
                ]
            ),
            link_change_score=(
                normalized_values[
                    "link_change_score"
                ]
            ),
            media_change_score=(
                normalized_values[
                    "media_change_score"
                ]
            ),
            semantic_change_score=(
                normalized_values[
                    "semantic_change_score"
                ]
            ),
            change_frequency=(
                normalized_values[
                    "change_frequency"
                ]
            ),
            change_volatility=(
                normalized_values[
                    "change_volatility"
                ]
            ),
            source_freshness=(
                normalized_values[
                    "source_freshness"
                ]
            ),
            temporal_volatility=(
                normalized_values[
                    "temporal_volatility"
                ]
            ),
            content_volatility=(
                normalized_values[
                    "content_volatility"
                ]
            ),
            query_time_sensitivity=(
                normalized_values[
                    "query_time_sensitivity"
                ]
            ),
            event_time_sensitivity=(
                normalized_values[
                    "event_time_sensitivity"
                ]
            ),
            recent_change=(
                normalized_values[
                    "recent_change"
                ]
            ),
            feed_update_signal=(
                normalized_values[
                    "feed_update_signal"
                ]
            ),
            sitemap_update_signal=(
                normalized_values[
                    "sitemap_update_signal"
                ]
            ),
            external_update_signal=(
                normalized_values[
                    "external_update_signal"
                ]
            ),
            historical_stability=(
                normalized_values[
                    "historical_stability"
                ]
            ),
            prior_freshness_accuracy=(
                normalized_values[
                    "prior_freshness_accuracy"
                ]
            ),
            confidence=(
                normalized_values[
                    "confidence"
                ]
            ),
            partial=signals.partial,
        )

    # ------------------------------------------------------------------
    # Change interval estimation
    # ------------------------------------------------------------------

    def _historical_interval(
        self,
        history: RecrawlHistory,
    ) -> Optional[float]:
        candidates = [
            history.average_change_interval_days,
            history.average_recrawl_interval_days,
        ]

        valid = [
            value
            for value in candidates
            if value is not None
            and isfinite(float(value))
            and float(value) > 0.0
        ]

        if not valid:
            return None

        return sum(valid) / float(
            len(valid)
        )

    def _estimate_base_interval(
        self,
        signals: RecrawlFreshnessSignals,
        history: RecrawlHistory,
    ) -> int:
        historical_interval = (
            self._historical_interval(
                history
            )
        )

        if historical_interval is None:
            base_seconds = float(
                self.policy.default_interval_seconds
            )
        else:
            base_seconds = (
                historical_interval
                * 86_400.0
            )

        # Higher observed change frequency means a shorter expected interval.
        frequency_factor = (
            1.0
            - 0.75
            * signals.change_frequency
        )

        volatility_factor = (
            1.0
            - 0.70
            * signals.change_volatility
        )

        stability_factor = (
            1.0
            + self.policy.stability_weight
            * signals.historical_stability
        )

        historical_factor = (
            1.0
            + self.policy.historical_change_weight
            * max(
                0.0,
                0.50
                - signals.change_frequency,
            )
        )

        interval = (
            base_seconds
            * frequency_factor
            * volatility_factor
            * stability_factor
            * historical_factor
        )

        return int(
            max(
                self.policy.minimum_interval_seconds,
                min(
                    self.policy.maximum_interval_seconds,
                    interval,
                ),
            )
        )

    # ------------------------------------------------------------------
    # Priority calculation
    # ------------------------------------------------------------------

    def _priority_score(
        self,
        signals: RecrawlFreshnessSignals,
        history: RecrawlHistory,
    ) -> float:
        temporal_sensitivity = max(
            signals.query_time_sensitivity,
            signals.event_time_sensitivity,
        )

        external_update = max(
            signals.feed_update_signal,
            signals.sitemap_update_signal,
            signals.external_update_signal,
        )

        direct_change = max(
            signals.recent_change,
            signals.content_change_score,
            signals.semantic_change_score,
        )

        volatility = max(
            signals.change_volatility,
            signals.temporal_volatility,
            signals.content_volatility,
        )

        historical_change = max(
            signals.change_frequency,
            history.historical_change_frequency,
        )

        priority = (
            self.policy.freshness_weight
            * signals.freshness_score
            + self.policy.urgency_weight
            * signals.urgency_score
            + self.policy.change_frequency_weight
            * historical_change
            + self.policy.change_volatility_weight
            * volatility
            + self.policy.recent_change_weight
            * direct_change
            + self.policy.semantic_change_weight
            * signals.semantic_change_score
            + self.policy.temporal_sensitivity_weight
            * temporal_sensitivity
            + self.policy.source_freshness_weight
            * signals.source_freshness
            + self.policy.feed_signal_weight
            * external_update
        )

        total_weight = (
            self.policy.freshness_weight
            + self.policy.urgency_weight
            + self.policy.change_frequency_weight
            + self.policy.change_volatility_weight
            + self.policy.recent_change_weight
            + self.policy.semantic_change_weight
            + self.policy.temporal_sensitivity_weight
            + self.policy.source_freshness_weight
            + self.policy.feed_signal_weight
        )

        score = (
            priority
            / max(
                0.001,
                total_weight,
            )
        )

        # Missed deadlines increase scheduling urgency.
        if history.missed_deadline_count > 0:
            recovery_boost = min(
                0.20,
                history.missed_deadline_count
                * 0.02,
            )
            score += recovery_boost

        # Poor historical schedule accuracy also increases priority slightly.
        schedule_accuracy_penalty = (
            1.0
            - 0.15
            * (
                1.0
                - self._clamp(
                    history.historical_schedule_accuracy
                )
            )
        )

        score *= schedule_accuracy_penalty

        confidence_factor = (
            0.70
            + 0.30
            * signals.confidence
        )

        return self._clamp(
            score * confidence_factor
        )

    # ------------------------------------------------------------------
    # Priority classification
    # ------------------------------------------------------------------

    def _priority_band(
        self,
        score: float,
    ) -> RecrawlPriorityBand:
        if score >= self.policy.urgent_threshold:
            return RecrawlPriorityBand.IMMEDIATE

        if score >= self.policy.high_priority_threshold:
            return RecrawlPriorityBand.URGENT

        if score >= self.policy.prioritize_threshold:
            return RecrawlPriorityBand.HIGH

        if score >= self.policy.schedule_threshold:
            return RecrawlPriorityBand.ELEVATED

        if score >= self.policy.defer_threshold:
            return RecrawlPriorityBand.NORMAL

        return RecrawlPriorityBand.BACKGROUND

    def _decision(
        self,
        score: float,
    ) -> RecrawlDecision:
        if score >= self.policy.urgent_threshold:
            return RecrawlDecision.IMMEDIATE

        if score >= self.policy.high_priority_threshold:
            return RecrawlDecision.URGENT

        if score >= self.policy.prioritize_threshold:
            return RecrawlDecision.HIGH_PRIORITY

        if score >= self.policy.schedule_threshold:
            return RecrawlDecision.PRIORITIZE

        if score >= self.policy.defer_threshold:
            return RecrawlDecision.SCHEDULE

        return RecrawlDecision.DEFER

    # ------------------------------------------------------------------
    # Interval adjustment
    # ------------------------------------------------------------------

    def _interval_multiplier(
        self,
        priority_band: RecrawlPriorityBand,
        signals: RecrawlFreshnessSignals,
    ) -> float:
        if (
            priority_band
            == RecrawlPriorityBand.IMMEDIATE
        ):
            return self.policy.immediate_multiplier

        if (
            priority_band
            == RecrawlPriorityBand.URGENT
        ):
            return self.policy.urgent_multiplier

        if (
            priority_band
            == RecrawlPriorityBand.HIGH
        ):
            return self.policy.high_multiplier

        if (
            priority_band
            == RecrawlPriorityBand.ELEVATED
        ):
            return self.policy.elevated_multiplier

        if (
            priority_band
            == RecrawlPriorityBand.NORMAL
        ):
            return self.policy.normal_multiplier

        if (
            signals.change_frequency
            < 0.10
            and signals.change_volatility
            < 0.15
        ):
            return self.policy.stable_multiplier

        return self.policy.low_activity_multiplier

    def _scheduled_interval(
        self,
        base_interval_seconds: int,
        priority_band: RecrawlPriorityBand,
        signals: RecrawlFreshnessSignals,
    ) -> int:
        multiplier = self._interval_multiplier(
            priority_band,
            signals,
        )

        interval = (
            base_interval_seconds
            * multiplier
        )

        return int(
            max(
                self.policy.minimum_interval_seconds,
                min(
                    self.policy.maximum_interval_seconds,
                    interval,
                ),
            )
        )

    # ------------------------------------------------------------------
    # Reasons
    # ------------------------------------------------------------------

    def _reasons(
        self,
        signals: RecrawlFreshnessSignals,
        history: RecrawlHistory,
        priority_score: float,
    ) -> List[RecrawlReason]:
        reasons: List[RecrawlReason] = []

        if (
            signals.urgency_score
            >= self.policy.high_priority_threshold
        ):
            reasons.append(
                RecrawlReason.STALE_RESOURCE
            )

        if signals.recent_change >= 0.50:
            reasons.append(
                RecrawlReason.RECENT_CONTENT_CHANGE
            )

        if signals.change_frequency >= 0.60:
            reasons.append(
                RecrawlReason.HIGH_CHANGE_RATE
            )

        if signals.change_volatility >= 0.60:
            reasons.append(
                RecrawlReason.HIGH_VOLATILITY
            )

        if max(
            signals.query_time_sensitivity,
            signals.event_time_sensitivity,
        ) >= 0.60:
            reasons.append(
                RecrawlReason.TEMPORAL_SENSITIVITY
            )

        if signals.source_freshness >= 0.60:
            reasons.append(
                RecrawlReason.SOURCE_FRESHNESS
            )

        if signals.feed_update_signal >= 0.50:
            reasons.append(
                RecrawlReason.FEED_UPDATE
            )

        if signals.sitemap_update_signal >= 0.50:
            reasons.append(
                RecrawlReason.SITEMAP_UPDATE
            )

        if signals.external_update_signal >= 0.50:
            reasons.append(
                RecrawlReason.EXTERNAL_UPDATE
            )

        if history.schedule_count == 0:
            reasons.append(
                RecrawlReason.FIRST_SCHEDULE
            )

        if history.missed_deadline_count > 0:
            reasons.append(
                RecrawlReason.MISSED_DEADLINE
            )

        if (
            signals.change_frequency < 0.10
            and signals.change_volatility < 0.15
        ):
            reasons.append(
                RecrawlReason.LOW_CHANGE_ACTIVITY
            )

        if (
            priority_score < self.policy.defer_threshold
        ):
            reasons.append(
                RecrawlReason.STABLE_RESOURCE
            )

        # Preserve deterministic ordering and avoid duplicates.
        unique: List[RecrawlReason] = []
        seen = set()

        for reason in reasons:
            if reason not in seen:
                seen.add(reason)
                unique.append(reason)

        return unique

    # ------------------------------------------------------------------
    # Schedule type
    # ------------------------------------------------------------------

    def _schedule_type(
        self,
        priority_band: RecrawlPriorityBand,
        history: RecrawlHistory,
    ) -> RecrawlScheduleType:
        if history.missed_deadline_count > 0:
            return RecrawlScheduleType.RECOVERY

        if history.schedule_count == 0:
            return RecrawlScheduleType.INITIAL

        if (
            priority_band
            == RecrawlPriorityBand.IMMEDIATE
        ):
            return RecrawlScheduleType.URGENT

        if (
            priority_band
            in (
                RecrawlPriorityBand.URGENT,
                RecrawlPriorityBand.HIGH,
            )
        ):
            return RecrawlScheduleType.ACCELERATED

        return RecrawlScheduleType.NORMAL

    # ------------------------------------------------------------------
    # Deadline
    # ------------------------------------------------------------------

    def _deadline_factor(
        self,
        decision: RecrawlDecision,
    ) -> float:
        if decision == RecrawlDecision.IMMEDIATE:
            return self.policy.immediate_deadline_factor

        if decision == RecrawlDecision.URGENT:
            return self.policy.urgent_deadline_factor

        return self.policy.normal_deadline_factor

    # ------------------------------------------------------------------
    # Build schedule
    # ------------------------------------------------------------------

    def _build_schedule(
        self,
        identity: RecrawlScheduleIdentity,
        signals: RecrawlFreshnessSignals,
        history: RecrawlHistory,
        now: datetime,
    ) -> RecrawlSchedule:
        base_interval = self._estimate_base_interval(
            signals,
            history,
        )

        priority_score = self._priority_score(
            signals,
            history,
        )

        priority_band = self._priority_band(
            priority_score
        )

        decision = self._decision(
            priority_score
        )

        interval_seconds = self._scheduled_interval(
            base_interval,
            priority_band,
            signals,
        )

        schedule_version = (
            history.schedule_count + 1
        )

        # Deterministic distributed jitter.
        jitter_unit = (
            self._deterministic_unit(
                identity.resource_id,
                schedule_version,
            )
        )

        jitter_offset = (
            (
                jitter_unit * 2.0
                - 1.0
            )
            * self.policy.jitter_fraction
        )

        jittered_interval = int(
            max(
                self.policy.minimum_interval_seconds,
                min(
                    self.policy.maximum_interval_seconds,
                    interval_seconds
                    * (
                        1.0
                        + jitter_offset
                    ),
                ),
            )
        )

        scheduled_at = (
            now
            + timedelta(
                seconds=jittered_interval
            )
        )

        deadline_factor = (
            self._deadline_factor(
                decision
            )
        )

        deadline_seconds = int(
            max(
                self.policy.minimum_interval_seconds,
                jittered_interval
                * deadline_factor,
            )
        )

        deadline_at = (
            now
            + timedelta(
                seconds=deadline_seconds
            )
        )

        reasons = self._reasons(
            signals,
            history,
            priority_score,
        )

        return RecrawlSchedule(
            schedule_id=str(uuid4()),
            resource_id=identity.resource_id,
            canonical_url=identity.canonical_url,
            schedule_type=self._schedule_type(
                priority_band,
                history,
            ),
            created_at=now.isoformat(),
            scheduled_at=scheduled_at.isoformat(),
            deadline_at=deadline_at.isoformat(),
            interval_seconds=jittered_interval,
            priority_score=priority_score,
            confidence=signals.confidence,
            priority_band=priority_band,
            decision=decision,
            reasons=reasons,
            partial=signals.partial,
            schedule_version=schedule_version,
            metadata={
                "phase": PHASE,
                "architecture_version": (
                    ARCHITECTURE_VERSION
                ),
                "base_interval_seconds": str(
                    base_interval
                ),
                "deterministic_jitter": (
                    f"{jitter_offset:.8f}"
                ),
            },
        )

    # ------------------------------------------------------------------
    # History update
    # ------------------------------------------------------------------

    def update_history(
        self,
        history: RecrawlHistory,
        schedule: RecrawlSchedule,
    ) -> RecrawlHistory:
        schedule_count = (
            history.schedule_count + 1
        )

        previous_interval = (
            history.average_recrawl_interval_days
        )

        current_interval_days = (
            schedule.interval_seconds
            / 86_400.0
        )

        if previous_interval is None:
            average_interval = (
                current_interval_days
            )
        else:
            average_interval = (
                (
                    previous_interval
                    * max(
                        0,
                        schedule_count - 1,
                    )
                    + current_interval_days
                )
                / float(schedule_count)
            )

        return RecrawlHistory(
            resource_id=history.resource_id,
            schedule_count=schedule_count,
            completed_recrawl_count=(
                history.completed_recrawl_count
            ),
            missed_deadline_count=(
                history.missed_deadline_count
            ),
            first_scheduled_at=(
                history.first_scheduled_at
                or schedule.created_at
            ),
            last_scheduled_at=(
                schedule.created_at
            ),
            last_recrawled_at=(
                history.last_recrawled_at
            ),
            last_successful_change_detected_at=(
                history.last_successful_change_detected_at
            ),
            average_change_interval_days=(
                history.average_change_interval_days
            ),
            average_recrawl_interval_days=(
                average_interval
            ),
            historical_change_frequency=(
                history.historical_change_frequency
            ),
            historical_schedule_accuracy=(
                history.historical_schedule_accuracy
            ),
            previous_priority_score=(
                schedule.priority_score
            ),
            previous_interval_days=(
                current_interval_days
            ),
            partial=(
                history.partial
                or schedule.partial
            ),
        )

    # ------------------------------------------------------------------
    # Main scheduling method
    # ------------------------------------------------------------------

    def schedule(
        self,
        identity: RecrawlScheduleIdentity,
        signals: RecrawlFreshnessSignals,
        history: RecrawlHistory,
        now: Optional[datetime] = None,
    ) -> RecrawlSchedulingResult:
        created_at = self._now()

        try:
            self._event(
                identity,
                RecrawlEventType.REQUEST_RECEIVED,
                RecrawlSchedulingState.RECEIVED,
            )

            # ----------------------------------------------------------
            # Validation
            # ----------------------------------------------------------

            self._event(
                identity,
                RecrawlEventType.VALIDATION_STARTED,
                RecrawlSchedulingState.VALIDATING,
            )

            if (
                identity.resource_id
                != history.resource_id
            ):
                raise ValueError(
                    "identity.resource_id must match "
                    "history.resource_id"
                )

            if not identity.canonical_url:
                raise ValueError(
                    "canonical_url must not be empty"
                )

            if not identity.hostname:
                raise ValueError(
                    "hostname must not be empty"
                )

            if (
                signals.partial
                and not self.policy.allow_partial
            ):
                raise ValueError(
                    "partial signals are disabled"
                )

            normalized = (
                self._normalize_signals(
                    signals
                )
            )

            if (
                normalized.confidence
                < self.policy.minimum_confidence
                and not self.policy.allow_partial
            ):
                raise ValueError(
                    "signal confidence is below "
                    "the configured minimum"
                )

            # ----------------------------------------------------------
            # Signal normalization
            # ----------------------------------------------------------

            self._event(
                identity,
                RecrawlEventType.SIGNALS_NORMALIZED,
                RecrawlSchedulingState.SIGNAL_NORMALIZATION,
                metadata={
                    "confidence": (
                        f"{normalized.confidence:.6f}"
                    ),
                    "partial": str(
                        normalized.partial
                    ),
                },
            )

            self._checkpoint(
                identity,
                RecrawlCheckpointType.SIGNALS_NORMALIZED,
                RecrawlSchedulingState.SIGNAL_NORMALIZATION,
                metadata={
                    "confidence": (
                        f"{normalized.confidence:.6f}"
                    )
                },
            )

            if normalized.partial:
                self._event(
                    identity,
                    RecrawlEventType.PARTIAL_INPUT_DETECTED,
                    RecrawlSchedulingState.PARTIAL,
                )

            # ----------------------------------------------------------
            # Schedule estimation
            # ----------------------------------------------------------

            self._event(
                identity,
                RecrawlEventType.SCHEDULE_ESTIMATION_STARTED,
                RecrawlSchedulingState.SCHEDULE_ESTIMATION,
            )

            reference_time = (
                now
                or datetime.now(timezone.utc)
            )

            base_interval = (
                self._estimate_base_interval(
                    normalized,
                    history,
                )
            )

            self._event(
                identity,
                RecrawlEventType.BASE_INTERVAL_CALCULATED,
                RecrawlSchedulingState.SCHEDULE_ESTIMATION,
                metadata={
                    "base_interval_seconds": str(
                        base_interval
                    )
                },
            )

            self._checkpoint(
                identity,
                RecrawlCheckpointType.INTERVAL_CALCULATED,
                RecrawlSchedulingState.SCHEDULE_ESTIMATION,
                metadata={
                    "base_interval_seconds": str(
                        base_interval
                    )
                },
            )

            # ----------------------------------------------------------
            # Priority
            # ----------------------------------------------------------

            priority_score = (
                self._priority_score(
                    normalized,
                    history,
                )
            )

            priority_band = (
                self._priority_band(
                    priority_score
                )
            )

            decision = self._decision(
                priority_score
            )

            self._event(
                identity,
                RecrawlEventType.PRIORITY_CALCULATED,
                RecrawlSchedulingState.PRIORITY_CALCULATION,
                metadata={
                    "priority_score": (
                        f"{priority_score:.6f}"
                    ),
                    "priority_band": (
                        priority_band.value
                    ),
                    "decision": (
                        decision.value
                    ),
                },
            )

            # ----------------------------------------------------------
            # Build schedule
            # ----------------------------------------------------------

            schedule = self._build_schedule(
                identity=identity,
                signals=normalized,
                history=history,
                now=reference_time,
            )

            self._checkpoint(
                identity,
                RecrawlCheckpointType.PRIORITY_CALCULATED,
                RecrawlSchedulingState.PRIORITY_CALCULATION,
                schedule=schedule,
            )

            # ----------------------------------------------------------
            # Deadline
            # ----------------------------------------------------------

            self._event(
                identity,
                RecrawlEventType.DEADLINE_CALCULATED,
                RecrawlSchedulingState.DEADLINE_CALCULATION,
                metadata={
                    "scheduled_at": (
                        schedule.scheduled_at
                    ),
                    "deadline_at": (
                        schedule.deadline_at
                    ),
                },
            )

            self._checkpoint(
                identity,
                RecrawlCheckpointType.DEADLINE_CALCULATED,
                RecrawlSchedulingState.DEADLINE_CALCULATION,
                schedule=schedule,
            )

            # ----------------------------------------------------------
            # Decision
            # ----------------------------------------------------------

            if (
                schedule.decision
                == RecrawlDecision.DEFER
            ):
                self._event(
                    identity,
                    RecrawlEventType.SCHEDULING_DEFERRED,
                    RecrawlSchedulingState.DEFERRED,
                    metadata={
                        "priority_score": (
                            f"{schedule.priority_score:.6f}"
                        )
                    },
                )

            else:
                self._event(
                    identity,
                    RecrawlEventType.SCHEDULE_CREATED,
                    RecrawlSchedulingState.DECISION,
                    metadata={
                        "schedule_id": (
                            schedule.schedule_id
                        ),
                        "scheduled_at": (
                            schedule.scheduled_at
                        ),
                        "deadline_at": (
                            schedule.deadline_at
                        ),
                    },
                )

            # ----------------------------------------------------------
            # Lineage
            # ----------------------------------------------------------

            lineage = RecrawlScheduleLineage(
                source_system=(
                    "phase13.3-recrawl-scheduling"
                ),
                source_version=(
                    ARCHITECTURE_VERSION
                ),
                previous_stage=(
                    "phase13.2-content-change-detection-signals"
                ),
                parent_request_ids=[
                    identity.request_id
                ],
                source_signal_ids=[],
                calculation_timestamp=self._now(),
                provenance_preserved=True,
            )

            # ----------------------------------------------------------
            # Final state
            # ----------------------------------------------------------

            final_state = (
                RecrawlSchedulingState.PARTIAL
                if normalized.partial
                else (
                    RecrawlSchedulingState.DEFERRED
                    if schedule.decision
                    == RecrawlDecision.DEFER
                    else RecrawlSchedulingState.COMPLETED
                )
            )

            result = RecrawlSchedulingResult(
                identity=identity,
                lineage=lineage,
                state=final_state,
                schedule=schedule,
                created_at=created_at,
                completed_at=self._now(),
            )

            self.backend.persist_result(
                result
            )

            self._checkpoint(
                identity,
                RecrawlCheckpointType.SCHEDULE_CREATED,
                final_state,
                schedule=schedule,
            )

            self._checkpoint(
                identity,
                RecrawlCheckpointType.COMPLETED,
                final_state,
                schedule=schedule,
            )

            self._event(
                identity,
                RecrawlEventType.SCHEDULING_COMPLETED,
                final_state,
                metadata={
                    "schedule_id": (
                        schedule.schedule_id
                    ),
                    "decision": (
                        schedule.decision.value
                    ),
                    "priority_score": (
                        f"{schedule.priority_score:.6f}"
                    ),
                    "interval_seconds": str(
                        schedule.interval_seconds
                    ),
                },
            )

            return result

        except Exception as exc:
            self._event(
                identity,
                RecrawlEventType.SCHEDULING_FAILED,
                RecrawlSchedulingState.FAILED,
                metadata={
                    "error": str(exc)
                },
            )

            failure_schedule = (
                RecrawlSchedule(
                    schedule_id=str(uuid4()),
                    resource_id=(
                        identity.resource_id
                    ),
                    canonical_url=(
                        identity.canonical_url
                    ),
                    schedule_type=(
                        RecrawlScheduleType.RECOVERY
                    ),
                    created_at=created_at,
                    scheduled_at=created_at,
                    deadline_at=created_at,
                    interval_seconds=(
                        self.policy.default_interval_seconds
                    ),
                    priority_score=0.0,
                    confidence=0.0,
                    priority_band=(
                        RecrawlPriorityBand.BACKGROUND
                    ),
                    decision=(
                        RecrawlDecision.DEFER
                    ),
                    reasons=[
                        RecrawlReason.STABLE_RESOURCE
                    ],
                    partial=True,
                    schedule_version=(
                        history.schedule_count + 1
                    ),
                )
            )

            lineage = RecrawlScheduleLineage(
                source_system=(
                    "phase13.3-recrawl-scheduling"
                ),
                source_version=(
                    ARCHITECTURE_VERSION
                ),
                previous_stage=(
                    "phase13.2-content-change-detection-signals"
                ),
                parent_request_ids=[
                    identity.request_id
                ],
                calculation_timestamp=self._now(),
                provenance_preserved=True,
            )

            result = RecrawlSchedulingResult(
                identity=identity,
                lineage=lineage,
                state=RecrawlSchedulingState.FAILED,
                schedule=failure_schedule,
                created_at=created_at,
                completed_at=self._now(),
                error=str(exc),
            )

            self.backend.persist_result(
                result
            )

            return result

    # ------------------------------------------------------------------
    # Batch scheduling
    # ------------------------------------------------------------------

    def schedule_many(
        self,
        requests: Sequence[
            Tuple[
                RecrawlScheduleIdentity,
                RecrawlFreshnessSignals,
                RecrawlHistory,
            ]
        ],
    ) -> List[RecrawlSchedulingResult]:
        if (
            len(requests)
            > self.policy.max_resources_per_batch
        ):
            raise ValueError(
                "batch exceeds per-resource "
                "scheduling limit"
            )

        results: List[
            RecrawlSchedulingResult
        ] = []

        for (
            identity,
            signals,
            history,
        ) in requests:
            results.append(
                self.schedule(
                    identity=identity,
                    signals=signals,
                    history=history,
                )
            )

        return results

    # ------------------------------------------------------------------
    # Rescheduling
    # ------------------------------------------------------------------

    def reschedule(
        self,
        identity: RecrawlScheduleIdentity,
        signals: RecrawlFreshnessSignals,
        history: RecrawlHistory,
        previous_schedule: RecrawlSchedule,
        now: Optional[datetime] = None,
    ) -> RecrawlSchedulingResult:
        """
        Create a new schedule version after a scheduling-relevant state
        change.

        The previous schedule is never mutated.
        """

        updated_history = RecrawlHistory(
            resource_id=history.resource_id,
            schedule_count=max(
                history.schedule_count,
                previous_schedule.schedule_version,
            ),
            completed_recrawl_count=(
                history.completed_recrawl_count
            ),
            missed_deadline_count=(
                history.missed_deadline_count
            ),
            first_scheduled_at=(
                history.first_scheduled_at
            ),
            last_scheduled_at=(
                history.last_scheduled_at
            ),
            last_recrawled_at=(
                history.last_recrawled_at
            ),
            last_successful_change_detected_at=(
                history.last_successful_change_detected_at
            ),
            average_change_interval_days=(
                history.average_change_interval_days
            ),
            average_recrawl_interval_days=(
                history.average_recrawl_interval_days
            ),
            historical_change_frequency=(
                history.historical_change_frequency
            ),
            historical_schedule_accuracy=(
                history.historical_schedule_accuracy
            ),
            previous_priority_score=(
                previous_schedule.priority_score
            ),
            previous_interval_days=(
                previous_schedule.interval_seconds
                / 86_400.0
            ),
            partial=(
                history.partial
            ),
        )

        return self.schedule(
            identity=identity,
            signals=signals,
            history=updated_history,
            now=now,
        )

    # ------------------------------------------------------------------
    # Metadata access
    # ------------------------------------------------------------------

    def events(
        self,
    ) -> List[RecrawlSchedulingEvent]:
        if hasattr(
            self.backend,
            "events",
        ):
            return list(
                self.backend.events()  # type: ignore[attr-defined]
            )

        return []

    def checkpoints(
        self,
    ) -> List[RecrawlSchedulingCheckpoint]:
        if hasattr(
            self.backend,
            "checkpoints",
        ):
            return list(
                self.backend.checkpoints()  # type: ignore[attr-defined]
            )

        return []

    # ------------------------------------------------------------------
    # Architecture description
    # ------------------------------------------------------------------

    def architecture(
        self,
    ) -> Dict[str, object]:
        return {
            "phase": "13",
            "stage": "13.3",
            "name": "Recrawl Scheduling",
            "version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,

            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),

            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),

            "purpose": (
                "Convert freshness and content-change evidence "
                "into deterministic recrawl schedules across "
                "enormous public-Web scale."
            ),

            "inputs": [
                "Phase 13.1 freshness priority signals",
                "Phase 13.2 content change signals",
                "resource change history",
                "recrawl history",
                "change frequency",
                "change volatility",
                "recent change evidence",
                "temporal sensitivity",
                "source freshness",
                "feed signals",
                "sitemap signals",
                "external update signals",
            ],

            "outputs": [
                "recrawl schedule",
                "next recrawl timestamp",
                "recrawl deadline",
                "recrawl interval",
                "priority score",
                "priority band",
                "recrawl decision",
                "schedule type",
                "scheduling reasons",
                "schedule version",
                "confidence",
                "lineage",
                "checkpoint state",
            ],

            "pipeline": [
                "input validation",
                "freshness/change signal normalization",
                "historical interval estimation",
                "base interval calculation",
                "priority calculation",
                "priority band classification",
                "interval adaptation",
                "deterministic distributed jitter",
                "next recrawl timestamp calculation",
                "deadline calculation",
                "reason generation",
                "schedule versioning",
                "lineage preservation",
                "checkpoint persistence",
                "schedule persistence",
            ],

            "distributed_execution": True,
            "partition_local_computation": True,
            "resource_partitionable": True,
            "horizontally_scalable": True,

            "deterministic": True,
            "deterministic_schedule_jitter": True,

            "checkpointable": True,
            "restartable": True,

            "partial_input_supported": (
                self.policy.allow_partial
            ),

            "incremental_history_supported": True,
            "incremental_rescheduling_supported": True,
            "schedule_versioning_supported": True,

            "provenance_preserved": True,
            "lineage_preserved": True,

            "backend_replaceable": True,

            "no_global_resource_ceiling": True,
            "no_global_url_ceiling": True,
            "no_global_document_ceiling": True,
            "no_global_schedule_ceiling": True,
            "no_global_partition_ceiling": True,
            "no_global_worker_ceiling": True,

            "no_google_api_dependency": True,
            "no_google_index_dependency": True,
            "no_google_crawler_dependency": True,
            "no_google_infrastructure_dependency": True,
            "no_google_ranking_dependency": True,

            "does_not_execute_crawl": True,
            "does_not_execute_http_fetch": True,
            "does_not_assign_crawler_workers": True,
            "does_not_perform_global_worker_orchestration": True,
            "does_not_mutate_search_index": True,
            "does_not_perform_final_ranking": True,
            "does_not_classify_spam": True,

            "stage_boundaries": {
                "phase13_1": (
                    "Determines freshness attention and urgency."
                ),
                "phase13_2": (
                    "Determines what changed and how volatile "
                    "the resource is."
                ),
                "phase13_3": (
                    "Determines when the resource should be "
                    "scheduled for recrawl."
                ),
                "phase13_4": (
                    "Will adapt recrawl frequency based on "
                    "observed long-term resource behavior."
                ),
            },

            "next_stage": NEXT_STAGE,
            "next_stage_name": (
                "Adaptive Recrawl Frequency"
            ),
        }


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------


RecrawlScheduling = (
    RecrawlSchedulingArchitecture
)

GlobalRecrawlScheduling = (
    RecrawlSchedulingArchitecture
)

Phase13_3RecrawlScheduling = (
    RecrawlSchedulingArchitecture
)


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------


__all__ = [
    "SCALE_TARGET",
    "GOOGLE_SCALE_CAPABILITY_TARGET",
    "GOOGLE_TECHNOLOGY_DEPENDENCY",
    "ARCHITECTURE_VERSION",
    "PHASE",
    "PREVIOUS_STAGE",
    "NEXT_STAGE",

    "RecrawlSchedulingState",
    "RecrawlPriorityBand",
    "RecrawlDecision",
    "RecrawlReason",
    "RecrawlScheduleType",
    "RecrawlEventType",
    "RecrawlCheckpointType",

    "RecrawlScheduleIdentity",
    "RecrawlScheduleLineage",

    "RecrawlFreshnessSignals",
    "RecrawlHistory",
    "RecrawlSchedulingPolicy",

    "RecrawlSchedule",
    "RecrawlSchedulingResult",
    "RecrawlSchedulingCheckpoint",
    "RecrawlSchedulingEvent",

    "RecrawlSchedulingBackend",
    "InMemoryRecrawlSchedulingMetadata",

    "RecrawlSchedulingArchitecture",
    "RecrawlScheduling",
    "GlobalRecrawlScheduling",
    "Phase13_3RecrawlScheduling",
]
