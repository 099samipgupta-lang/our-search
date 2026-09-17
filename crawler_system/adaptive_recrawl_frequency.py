"""
OUR SEARCH
Phase 13.4 — Adaptive Recrawl Frequency

Purpose
-------
Adapt the long-term recrawl frequency of public-Web resources based on
historical change behavior, recrawl outcomes, freshness accuracy, volatility,
stability, and prior scheduling decisions.

This stage consumes the outputs/evidence produced by earlier freshness and
recrawl stages and determines how frequently a resource should be revisited
over time.

This module is an architecture and decision layer.

It does NOT:
- perform HTTP requests
- execute crawling
- fetch Web resources
- assign crawler workers
- mutate the search index
- perform final ranking
- classify spam
- replace distributed crawl orchestration
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
- workers
- schedules
- recrawl histories

Per-request safety limits exist only to bound individual processing units.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple


# ============================================================================
# ARCHITECTURE METADATA
# ============================================================================

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "adaptive-recrawl-frequency.v1"
PHASE = "13.4"
PREVIOUS_STAGE = "13.3"
NEXT_STAGE = "13.5"


# ============================================================================
# ENUMERATIONS
# ============================================================================


class AdaptiveRecrawlFrequencyState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    HISTORY_NORMALIZATION = "history_normalization"
    BEHAVIOR_ANALYSIS = "behavior_analysis"
    FREQUENCY_ESTIMATION = "frequency_estimation"
    STABILITY_ANALYSIS = "stability_analysis"
    VOLATILITY_ANALYSIS = "volatility_analysis"
    ADAPTATION = "adaptation"
    DECISION = "decision"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class AdaptiveFrequencyBand(str, Enum):
    VERY_STABLE = "very_stable"
    STABLE = "stable"
    NORMAL = "normal"
    ACTIVE = "active"
    HIGHLY_ACTIVE = "highly_active"
    VOLATILE = "volatile"
    EXTREMELY_VOLATILE = "extremely_volatile"


class AdaptiveFrequencyDecision(str, Enum):
    MAINTAIN = "maintain"
    LENGTHEN = "lengthen"
    SHORTEN = "shorten"
    ACCELERATE = "accelerate"
    STRONGLY_ACCELERATE = "strongly_accelerate"
    DEFER = "defer"


class AdaptiveFrequencyReason(str, Enum):
    STABLE_RESOURCE = "stable_resource"
    LOW_CHANGE_RATE = "low_change_rate"
    HIGH_CHANGE_RATE = "high_change_rate"
    HIGH_CHANGE_CONSISTENCY = "high_change_consistency"
    HIGH_CHANGE_VOLATILITY = "high_change_volatility"
    LOW_CHANGE_VOLATILITY = "low_change_volatility"
    RECENT_CHANGE_ACTIVITY = "recent_change_activity"
    LONG_TERM_CHANGE_PATTERN = "long_term_change_pattern"
    FRESHNESS_ACCURACY = "freshness_accuracy"
    FRESHNESS_INACCURACY = "freshness_inaccuracy"
    MISSED_RECRAWL_DEADLINES = "missed_recrawl_deadlines"
    SCHEDULED_TOO_FREQUENTLY = "scheduled_too_frequently"
    SCHEDULED_TOO_SLOWLY = "scheduled_too_slowly"
    HISTORICAL_STABILITY = "historical_stability"
    HISTORICAL_INSTABILITY = "historical_instability"
    TEMPORAL_SENSITIVITY = "temporal_sensitivity"
    SOURCE_FRESHNESS = "source_freshness"
    FEED_ACTIVITY = "feed_activity"
    SITEMAP_ACTIVITY = "sitemap_activity"
    EXTERNAL_ACTIVITY = "external_activity"
    INSUFFICIENT_HISTORY = "insufficient_history"
    FIRST_ADAPTATION = "first_adaptation"


class AdaptiveFrequencyEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    HISTORY_NORMALIZATION_STARTED = "history_normalization_started"
    BEHAVIOR_ANALYSIS_STARTED = "behavior_analysis_started"
    FREQUENCY_ESTIMATION_STARTED = "frequency_estimation_started"
    STABILITY_ANALYSIS_STARTED = "stability_analysis_started"
    VOLATILITY_ANALYSIS_STARTED = "volatility_analysis_started"
    ADAPTATION_STARTED = "adaptation_started"
    BASE_FREQUENCY_CALCULATED = "base_frequency_calculated"
    CHANGE_BEHAVIOR_ANALYZED = "change_behavior_analyzed"
    FREQUENCY_ADAPTED = "frequency_adapted"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    ADAPTATION_COMPLETED = "adaptation_completed"
    ADAPTATION_DEFERRED = "adaptation_deferred"
    ADAPTATION_REJECTED = "adaptation_rejected"
    ADAPTATION_FAILED = "adaptation_failed"


class AdaptiveFrequencyCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    HISTORY_NORMALIZED = "history_normalized"
    BEHAVIOR_ANALYZED = "behavior_analyzed"
    BASE_FREQUENCY_CALCULATED = "base_frequency_calculated"
    ADAPTATION_CALCULATED = "adaptation_calculated"
    FREQUENCY_CREATED = "frequency_created"
    COMPLETED = "completed"


# ============================================================================
# IDENTITY
# ============================================================================


@dataclass(frozen=True)
class AdaptiveRecrawlFrequencyIdentity:
    resource_id: str
    schedule_id: str = ""
    frequency_version: str = ARCHITECTURE_VERSION
    partition_id: str = ""
    host_id: str = ""
    domain_id: str = ""

    def key(self) -> str:
        return (
            f"{self.resource_id}:"
            f"{self.schedule_id}:"
            f"{self.frequency_version}"
        )


# ============================================================================
# LINEAGE
# ============================================================================


@dataclass
class AdaptiveRecrawlFrequencyLineage:
    resource_id: str
    previous_stage: str = PREVIOUS_STAGE
    current_stage: str = PHASE
    previous_schedule_id: str = ""
    previous_frequency_version: str = ""
    source_signal_version: str = ""
    source_history_version: str = ""
    parent_schedule_ids: List[str] = field(default_factory=list)
    lineage_metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# HISTORICAL BEHAVIOR
# ============================================================================


@dataclass
class AdaptiveRecrawlHistory:
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

    last_change_timestamp: Optional[str] = None
    last_recrawl_timestamp: Optional[str] = None

    first_observed_timestamp: Optional[str] = None

    partial: bool = False
    history_version: str = ""


# ============================================================================
# CURRENT SIGNALS
# ============================================================================


@dataclass
class AdaptiveRecrawlSignals:
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

    historical_stability: float = 0.0
    prior_freshness_accuracy: float = 0.0

    confidence: float = 0.0
    partial: bool = False

    signal_version: str = ""


# ============================================================================
# POLICY
# ============================================================================


@dataclass
class AdaptiveRecrawlFrequencyPolicy:
    max_resources_per_batch: int = 100_000

    minimum_confidence: float = 0.10
    minimum_history_samples: int = 3

    allow_partial: bool = True
    deterministic: bool = True
    checkpoint_enabled: bool = True

    minimum_interval_seconds: float = 60.0
    default_interval_seconds: float = 86_400.0
    maximum_interval_seconds: float = 365.0 * 86_400.0

    # Long-term behavior weights.
    historical_change_weight: float = 1.40
    historical_volatility_weight: float = 1.20
    historical_stability_weight: float = 1.10

    current_change_weight: float = 1.20
    recent_change_weight: float = 1.35

    freshness_accuracy_weight: float = 1.10
    schedule_accuracy_weight: float = 0.85

    semantic_change_weight: float = 1.15
    temporal_sensitivity_weight: float = 0.90

    source_freshness_weight: float = 0.75
    feed_signal_weight: float = 0.90
    sitemap_signal_weight: float = 0.70
    external_signal_weight: float = 0.60

    missed_deadline_weight: float = 1.30
    unnecessary_recrawl_weight: float = 0.80

    # Frequency adaptation factors.
    very_stable_multiplier: float = 2.50
    stable_multiplier: float = 1.75
    normal_multiplier: float = 1.00
    active_multiplier: float = 0.65
    highly_active_multiplier: float = 0.40
    volatile_multiplier: float = 0.20
    extremely_volatile_multiplier: float = 0.08

    # Confidence-aware adaptation limits.
    maximum_shortening_factor: float = 0.05
    maximum_lengthening_factor: float = 4.00

    # Prevent one observation from causing unlimited adaptation.
    maximum_adaptation_step: float = 0.75

    # Deadlines are handled by Phase 13.3.
    preserve_previous_deadline: bool = True


# ============================================================================
# FREQUENCY RESULT
# ============================================================================


@dataclass
class AdaptiveRecrawlFrequency:
    identity: AdaptiveRecrawlFrequencyIdentity
    lineage: AdaptiveRecrawlFrequencyLineage

    previous_interval_seconds: float
    adapted_interval_seconds: float

    estimated_change_interval_seconds: float
    estimated_recrawl_interval_seconds: float

    change_activity_score: float
    change_consistency_score: float
    volatility_score: float
    stability_score: float

    freshness_accuracy_score: float
    schedule_accuracy_score: float

    adaptation_strength: float
    adaptation_factor: float

    frequency_band: AdaptiveFrequencyBand
    decision: AdaptiveFrequencyDecision

    reasons: List[AdaptiveFrequencyReason] = field(default_factory=list)

    effective_from: str = ""
    version: str = ARCHITECTURE_VERSION

    confidence: float = 0.0
    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# RESULT
# ============================================================================


@dataclass
class AdaptiveRecrawlFrequencyResult:
    state: AdaptiveRecrawlFrequencyState

    frequency: Optional[AdaptiveRecrawlFrequency]

    resource_id: str
    request_id: str

    accepted: bool
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
class AdaptiveRecrawlFrequencyCheckpoint:
    checkpoint_id: str
    request_id: str
    resource_id: str

    checkpoint_type: AdaptiveFrequencyCheckpointType
    state: AdaptiveRecrawlFrequencyState

    interval_seconds: float = 0.0
    confidence: float = 0.0

    payload: Dict[str, Any] = field(default_factory=dict)

    created_at: str = ""
    version: str = ARCHITECTURE_VERSION


# ============================================================================
# EVENT
# ============================================================================


@dataclass
class AdaptiveRecrawlFrequencyEvent:
    event_id: str
    request_id: str
    resource_id: str

    event_type: AdaptiveFrequencyEventType
    state: AdaptiveRecrawlFrequencyState

    payload: Dict[str, Any] = field(default_factory=dict)

    created_at: str = ""
    version: str = ARCHITECTURE_VERSION


# ============================================================================
# BACKEND CONTRACT
# ============================================================================


class AdaptiveRecrawlFrequencyBackend(Protocol):
    def persist_event(
        self,
        event: AdaptiveRecrawlFrequencyEvent,
    ) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: AdaptiveRecrawlFrequencyCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: AdaptiveRecrawlFrequencyResult,
    ) -> None:
        ...

    def get_result(
        self,
        request_id: str,
    ) -> Optional[AdaptiveRecrawlFrequencyResult]:
        ...


# ============================================================================
# IN-MEMORY METADATA BACKEND
# ============================================================================


class InMemoryAdaptiveRecrawlFrequencyMetadata:
    def __init__(self) -> None:
        self._events: List[AdaptiveRecrawlFrequencyEvent] = []
        self._checkpoints: List[AdaptiveRecrawlFrequencyCheckpoint] = []
        self._results: Dict[str, AdaptiveRecrawlFrequencyResult] = {}

    def persist_event(
        self,
        event: AdaptiveRecrawlFrequencyEvent,
    ) -> None:
        self._events.append(event)

    def persist_checkpoint(
        self,
        checkpoint: AdaptiveRecrawlFrequencyCheckpoint,
    ) -> None:
        self._checkpoints.append(checkpoint)

    def persist_result(
        self,
        result: AdaptiveRecrawlFrequencyResult,
    ) -> None:
        self._results[result.request_id] = result

    def get_result(
        self,
        request_id: str,
    ) -> Optional[AdaptiveRecrawlFrequencyResult]:
        return self._results.get(request_id)

    def events(self) -> List[AdaptiveRecrawlFrequencyEvent]:
        return list(self._events)

    def checkpoints(self) -> List[AdaptiveRecrawlFrequencyCheckpoint]:
        return list(self._checkpoints)

    def results(self) -> List[AdaptiveRecrawlFrequencyResult]:
        return list(self._results.values())


# ============================================================================
# MAIN ARCHITECTURE
# ============================================================================


class AdaptiveRecrawlFrequencyArchitecture:
    """
    Phase 13.4 adaptive recrawl frequency architecture.

    Converts long-term recrawl/change behavior into an adaptive interval.

    This class is intentionally separated from crawl execution.
    """

    def __init__(
        self,
        backend: Optional[AdaptiveRecrawlFrequencyBackend] = None,
        policy: Optional[AdaptiveRecrawlFrequencyPolicy] = None,
    ) -> None:
        self.backend = backend or InMemoryAdaptiveRecrawlFrequencyMetadata()
        self.policy = policy or AdaptiveRecrawlFrequencyPolicy()

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

        return max(minimum, min(maximum, value))

    def _safe_float(
        self,
        value: Any,
        default: float = 0.0,
    ) -> float:
        try:
            result = float(value)
        except (TypeError, ValueError):
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
        except (TypeError, ValueError):
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
        seed = (
            f"{resource_id}:"
            f"{schedule_id}:"
            f"{ARCHITECTURE_VERSION}"
        )

        return self._stable_id("arfreq", seed)

    # ------------------------------------------------------------------------
    # EVENTS
    # ------------------------------------------------------------------------

    def _event(
        self,
        request_id: str,
        resource_id: str,
        event_type: AdaptiveFrequencyEventType,
        state: AdaptiveRecrawlFrequencyState,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> AdaptiveRecrawlFrequencyEvent:
        event = AdaptiveRecrawlFrequencyEvent(
            event_id=self._stable_id(
                "event",
                f"{request_id}:{event_type.value}:{self._now_iso()}",
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
        checkpoint_type: AdaptiveFrequencyCheckpointType,
        state: AdaptiveRecrawlFrequencyState,
        interval_seconds: float = 0.0,
        confidence: float = 0.0,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> AdaptiveRecrawlFrequencyCheckpoint:
        checkpoint = AdaptiveRecrawlFrequencyCheckpoint(
            checkpoint_id=self._stable_id(
                "checkpoint",
                f"{request_id}:{checkpoint_type.value}:{self._now_iso()}",
            ),
            request_id=request_id,
            resource_id=resource_id,
            checkpoint_type=checkpoint_type,
            state=state,
            interval_seconds=max(0.0, interval_seconds),
            confidence=self._clamp(confidence),
            payload=dict(payload or {}),
            created_at=self._now_iso(),
        )

        if self.policy.checkpoint_enabled:
            self.backend.persist_checkpoint(checkpoint)

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.CHECKPOINT_CREATED,
            state,
            {
                "checkpoint_id": checkpoint.checkpoint_id,
                "checkpoint_type": checkpoint_type.value,
            },
        )

        return checkpoint

    # ------------------------------------------------------------------------
    # SIGNAL NORMALIZATION
    # ------------------------------------------------------------------------

    def _normalize_signals(
        self,
        signals: AdaptiveRecrawlSignals,
    ) -> AdaptiveRecrawlSignals:
        normalized = AdaptiveRecrawlSignals(
            freshness_score=self._clamp(
                self._safe_float(signals.freshness_score)
            ),
            urgency_score=self._clamp(
                self._safe_float(signals.urgency_score)
            ),

            content_change_score=self._clamp(
                self._safe_float(signals.content_change_score)
            ),
            text_change_score=self._clamp(
                self._safe_float(signals.text_change_score)
            ),
            structural_change_score=self._clamp(
                self._safe_float(signals.structural_change_score)
            ),
            metadata_change_score=self._clamp(
                self._safe_float(signals.metadata_change_score)
            ),
            link_change_score=self._clamp(
                self._safe_float(signals.link_change_score)
            ),
            media_change_score=self._clamp(
                self._safe_float(signals.media_change_score)
            ),
            semantic_change_score=self._clamp(
                self._safe_float(signals.semantic_change_score)
            ),

            change_frequency=self._clamp(
                self._safe_float(signals.change_frequency)
            ),
            change_volatility=self._clamp(
                self._safe_float(signals.change_volatility)
            ),

            source_freshness=self._clamp(
                self._safe_float(signals.source_freshness)
            ),
            temporal_volatility=self._clamp(
                self._safe_float(signals.temporal_volatility)
            ),
            content_volatility=self._clamp(
                self._safe_float(signals.content_volatility)
            ),

            query_time_sensitivity=self._clamp(
                self._safe_float(signals.query_time_sensitivity)
            ),
            event_time_sensitivity=self._clamp(
                self._safe_float(signals.event_time_sensitivity)
            ),

            recent_change=self._clamp(
                self._safe_float(signals.recent_change)
            ),

            feed_update_signal=self._clamp(
                self._safe_float(signals.feed_update_signal)
            ),
            sitemap_update_signal=self._clamp(
                self._safe_float(signals.sitemap_update_signal)
            ),
            external_update_signal=self._clamp(
                self._safe_float(signals.external_update_signal)
            ),

            historical_stability=self._clamp(
                self._safe_float(signals.historical_stability)
            ),
            prior_freshness_accuracy=self._clamp(
                self._safe_float(signals.prior_freshness_accuracy)
            ),

            confidence=self._clamp(
                self._safe_float(signals.confidence)
            ),
            partial=bool(signals.partial),

            signal_version=signals.signal_version,
        )

        return normalized

    # ------------------------------------------------------------------------
    # HISTORY NORMALIZATION
    # ------------------------------------------------------------------------

    def _normalize_history(
        self,
        history: AdaptiveRecrawlHistory,
    ) -> AdaptiveRecrawlHistory:
        total_recrawls = self._safe_int(history.total_recrawls)
        successful_recrawls = min(
            total_recrawls,
            self._safe_int(history.successful_recrawls),
        )

        failed_recrawls = min(
            max(0, total_recrawls - successful_recrawls),
            self._safe_int(history.failed_recrawls),
        )

        observed_changes = self._safe_int(
            history.observed_changes
        )

        meaningful_changes = min(
            observed_changes,
            self._safe_int(history.meaningful_changes),
        )

        normalized = AdaptiveRecrawlHistory(
            total_recrawls=total_recrawls,
            successful_recrawls=successful_recrawls,
            failed_recrawls=failed_recrawls,

            observed_changes=observed_changes,
            meaningful_changes=meaningful_changes,
            semantic_changes=self._safe_int(history.semantic_changes),
            structural_changes=self._safe_int(history.structural_changes),
            metadata_changes=self._safe_int(history.metadata_changes),
            link_changes=self._safe_int(history.link_changes),
            media_changes=self._safe_int(history.media_changes),

            average_change_interval_seconds=max(
                0.0,
                self._safe_float(
                    history.average_change_interval_seconds
                ),
            ),
            median_change_interval_seconds=max(
                0.0,
                self._safe_float(
                    history.median_change_interval_seconds
                ),
            ),
            minimum_change_interval_seconds=max(
                0.0,
                self._safe_float(
                    history.minimum_change_interval_seconds
                ),
            ),
            maximum_change_interval_seconds=max(
                0.0,
                self._safe_float(
                    history.maximum_change_interval_seconds
                ),
            ),

            average_recrawl_interval_seconds=max(
                0.0,
                self._safe_float(
                    history.average_recrawl_interval_seconds
                ),
            ),
            median_recrawl_interval_seconds=max(
                0.0,
                self._safe_float(
                    history.median_recrawl_interval_seconds
                ),
            ),

            historical_change_frequency=self._clamp(
                self._safe_float(
                    history.historical_change_frequency
                )
            ),
            historical_change_volatility=self._clamp(
                self._safe_float(
                    history.historical_change_volatility
                )
            ),
            historical_stability=self._clamp(
                self._safe_float(
                    history.historical_stability
                )
            ),

            schedule_accuracy=self._clamp(
                self._safe_float(history.schedule_accuracy)
            ),
            freshness_accuracy=self._clamp(
                self._safe_float(history.freshness_accuracy)
            ),

            missed_deadlines=self._safe_int(
                history.missed_deadlines
            ),
            unnecessary_recrawls=self._safe_int(
                history.unnecessary_recrawls
            ),
            stale_observations=self._safe_int(
                history.stale_observations
            ),

            recent_change_count=self._safe_int(
                history.recent_change_count
            ),
            recent_recrawl_count=self._safe_int(
                history.recent_recrawl_count
            ),

            last_change_timestamp=history.last_change_timestamp,
            last_recrawl_timestamp=history.last_recrawl_timestamp,

            first_observed_timestamp=history.first_observed_timestamp,

            partial=bool(history.partial),
            history_version=history.history_version,
        )

        return normalized

    # ------------------------------------------------------------------------
    # HISTORY SAMPLE CONFIDENCE
    # ------------------------------------------------------------------------

    def _history_confidence(
        self,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        sample_count = max(
            history.total_recrawls,
            history.observed_changes,
        )

        if sample_count <= 0:
            return 0.0

        target = max(
            1,
            self.policy.minimum_history_samples,
        )

        sample_confidence = self._clamp(
            sample_count / float(target)
        )

        success_confidence = self._clamp(
            history.successful_recrawls
            / float(max(1, history.total_recrawls))
        )

        return self._clamp(
            0.70 * sample_confidence
            + 0.30 * success_confidence
        )

    # ------------------------------------------------------------------------
    # LONG-TERM CHANGE ACTIVITY
    # ------------------------------------------------------------------------

    def _historical_change_activity(
        self,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        frequency = self._clamp(
            history.historical_change_frequency
        )

        observed_rate = self._clamp(
            history.meaningful_changes
            / float(max(1, history.total_recrawls))
        )

        if history.average_change_interval_seconds > 0:
            interval_signal = self._clamp(
                self.policy.default_interval_seconds
                / history.average_change_interval_seconds
            )
        else:
            interval_signal = 0.0

        return self._clamp(
            0.50 * frequency
            + 0.30 * observed_rate
            + 0.20 * interval_signal
        )

    # ------------------------------------------------------------------------
    # CURRENT CHANGE ACTIVITY
    # ------------------------------------------------------------------------

    def _current_change_activity(
        self,
        signals: AdaptiveRecrawlSignals,
    ) -> float:
        return self._clamp(
            0.24 * signals.content_change_score
            + 0.12 * signals.text_change_score
            + 0.10 * signals.structural_change_score
            + 0.07 * signals.metadata_change_score
            + 0.08 * signals.link_change_score
            + 0.06 * signals.media_change_score
            + 0.15 * signals.semantic_change_score
            + 0.18 * signals.change_frequency
        )

    # ------------------------------------------------------------------------
    # RECENT ACTIVITY
    # ------------------------------------------------------------------------

    def _recent_activity(
        self,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        recent_history_signal = self._clamp(
            history.recent_change_count
            / float(max(1, history.recent_recrawl_count))
        )

        return self._clamp(
            0.65 * signals.recent_change
            + 0.35 * recent_history_signal
        )

    # ------------------------------------------------------------------------
    # CHANGE CONSISTENCY
    # ------------------------------------------------------------------------

    def _change_consistency(
        self,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        volatility = self._clamp(
            max(
                history.historical_change_volatility,
                history.historical_change_volatility,
            )
        )

        if history.minimum_change_interval_seconds <= 0:
            return self._clamp(1.0 - volatility)

        if history.maximum_change_interval_seconds <= 0:
            return self._clamp(1.0 - volatility)

        spread = (
            history.maximum_change_interval_seconds
            - history.minimum_change_interval_seconds
        )

        baseline = max(
            history.average_change_interval_seconds,
            self.policy.minimum_interval_seconds,
        )

        spread_ratio = self._clamp(
            spread / baseline
        )

        return self._clamp(
            1.0 - (
                0.60 * volatility
                + 0.40 * spread_ratio
            )
        )

    # ------------------------------------------------------------------------
    # VOLATILITY
    # ------------------------------------------------------------------------

    def _volatility_score(
        self,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        return self._clamp(
            0.35 * signals.change_volatility
            + 0.20 * signals.content_volatility
            + 0.15 * signals.temporal_volatility
            + 0.20 * history.historical_change_volatility
            + 0.10 * (1.0 - signals.historical_stability)
        )

    # ------------------------------------------------------------------------
    # STABILITY
    # ------------------------------------------------------------------------

    def _stability_score(
        self,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
        consistency: float,
    ) -> float:
        return self._clamp(
            0.35 * signals.historical_stability
            + 0.30 * history.historical_stability
            + 0.20 * consistency
            + 0.15 * (1.0 - signals.change_volatility)
        )

    # ------------------------------------------------------------------------
    # FRESHNESS ACCURACY
    # ------------------------------------------------------------------------

    def _freshness_accuracy(
        self,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        return self._clamp(
            0.65 * history.freshness_accuracy
            + 0.35 * signals.prior_freshness_accuracy
        )

    # ------------------------------------------------------------------------
    # SCHEDULE ACCURACY
    # ------------------------------------------------------------------------

    def _schedule_accuracy(
        self,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        return self._clamp(
            history.schedule_accuracy
        )

    # ------------------------------------------------------------------------
    # ESTIMATE HISTORICAL CHANGE INTERVAL
    # ------------------------------------------------------------------------

    def _estimated_change_interval(
        self,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        candidates = [
            value
            for value in (
                history.median_change_interval_seconds,
                history.average_change_interval_seconds,
            )
            if value > 0
        ]

        if not candidates:
            return self.policy.default_interval_seconds

        if len(candidates) == 1:
            return candidates[0]

        return self._clamp(
            0.60 * candidates[0]
            + 0.40 * candidates[1],
            self.policy.minimum_interval_seconds,
            self.policy.maximum_interval_seconds,
        )

    # ------------------------------------------------------------------------
    # ESTIMATE HISTORICAL RECRAWL INTERVAL
    # ------------------------------------------------------------------------

    def _estimated_recrawl_interval(
        self,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        candidates = [
            value
            for value in (
                history.median_recrawl_interval_seconds,
                history.average_recrawl_interval_seconds,
            )
            if value > 0
        ]

        if not candidates:
            return self.policy.default_interval_seconds

        if len(candidates) == 1:
            return candidates[0]

        return self._clamp(
            0.60 * candidates[0]
            + 0.40 * candidates[1],
            self.policy.minimum_interval_seconds,
            self.policy.maximum_interval_seconds,
        )

    # ------------------------------------------------------------------------
    # BASE INTERVAL
    # ------------------------------------------------------------------------

    def _base_interval(
        self,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        change_interval = self._estimated_change_interval(history)
        recrawl_interval = self._estimated_recrawl_interval(history)

        if history.observed_changes > 0:
            candidate = change_interval
        elif history.total_recrawls > 0:
            candidate = recrawl_interval
        else:
            candidate = self.policy.default_interval_seconds

        return self._clamp(
            candidate,
            self.policy.minimum_interval_seconds,
            self.policy.maximum_interval_seconds,
        )

    # ------------------------------------------------------------------------
    # ACTIVITY SCORE
    # ------------------------------------------------------------------------

    def _activity_score(
        self,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        historical_activity = self._historical_change_activity(
            history
        )

        current_activity = self._current_change_activity(
            signals
        )

        recent_activity = self._recent_activity(
            signals,
            history,
        )

        semantic_activity = signals.semantic_change_score

        weighted = (
            self.policy.historical_change_weight
            * historical_activity
            + self.policy.current_change_weight
            * current_activity
            + self.policy.recent_change_weight
            * recent_activity
            + self.policy.semantic_change_weight
            * semantic_activity
        )

        denominator = (
            self.policy.historical_change_weight
            + self.policy.current_change_weight
            + self.policy.recent_change_weight
            + self.policy.semantic_change_weight
        )

        return self._clamp(
            weighted / max(denominator, 1e-9)
        )

    # ------------------------------------------------------------------------
    # FREQUENCY BAND
    # ------------------------------------------------------------------------

    def _frequency_band(
        self,
        activity: float,
        volatility: float,
        stability: float,
    ) -> AdaptiveFrequencyBand:
        activity = self._clamp(activity)
        volatility = self._clamp(volatility)
        stability = self._clamp(stability)

        combined_activity = self._clamp(
            0.55 * activity
            + 0.30 * volatility
            + 0.15 * (1.0 - stability)
        )

        if combined_activity < 0.10:
            return AdaptiveFrequencyBand.VERY_STABLE

        if combined_activity < 0.25:
            return AdaptiveFrequencyBand.STABLE

        if combined_activity < 0.45:
            return AdaptiveFrequencyBand.NORMAL

        if combined_activity < 0.62:
            return AdaptiveFrequencyBand.ACTIVE

        if combined_activity < 0.78:
            return AdaptiveFrequencyBand.HIGHLY_ACTIVE

        if combined_activity < 0.92:
            return AdaptiveFrequencyBand.VOLATILE

        return AdaptiveFrequencyBand.EXTREMELY_VOLATILE

    # ------------------------------------------------------------------------
    # BAND MULTIPLIER
    # ------------------------------------------------------------------------

    def _band_multiplier(
        self,
        band: AdaptiveFrequencyBand,
    ) -> float:
        mapping = {
            AdaptiveFrequencyBand.VERY_STABLE:
                self.policy.very_stable_multiplier,

            AdaptiveFrequencyBand.STABLE:
                self.policy.stable_multiplier,

            AdaptiveFrequencyBand.NORMAL:
                self.policy.normal_multiplier,

            AdaptiveFrequencyBand.ACTIVE:
                self.policy.active_multiplier,

            AdaptiveFrequencyBand.HIGHLY_ACTIVE:
                self.policy.highly_active_multiplier,

            AdaptiveFrequencyBand.VOLATILE:
                self.policy.volatile_multiplier,

            AdaptiveFrequencyBand.EXTREMELY_VOLATILE:
                self.policy.extremely_volatile_multiplier,
        }

        return max(
            self.policy.maximum_shortening_factor,
            mapping[band],
        )

    # ------------------------------------------------------------------------
    # ADAPTATION STRENGTH
    # ------------------------------------------------------------------------

    def _adaptation_strength(
        self,
        activity: float,
        volatility: float,
        stability: float,
        freshness_accuracy: float,
        schedule_accuracy: float,
        confidence: float,
    ) -> float:
        activity_signal = self._clamp(activity)
        volatility_signal = self._clamp(volatility)

        stability_inverse = self._clamp(
            1.0 - stability
        )

        accuracy_signal = self._clamp(
            1.0
            - (
                self.policy.freshness_accuracy_weight
                * freshness_accuracy
                + self.policy.schedule_accuracy_weight
                * schedule_accuracy
            )
            / max(
                self.policy.freshness_accuracy_weight
                + self.policy.schedule_accuracy_weight,
                1e-9,
            )
        )

        raw = (
            0.40 * activity_signal
            + 0.25 * volatility_signal
            + 0.20 * stability_inverse
            + 0.15 * accuracy_signal
        )

        return self._clamp(
            raw * self._clamp(confidence)
        )

    # ------------------------------------------------------------------------
    # DECISION
    # ------------------------------------------------------------------------

    def _decision(
        self,
        band: AdaptiveFrequencyBand,
        activity: float,
        volatility: float,
        stability: float,
        missed_deadlines: int,
    ) -> AdaptiveFrequencyDecision:
        if missed_deadlines > 0 and (
            activity >= 0.75
            or volatility >= 0.75
        ):
            return AdaptiveFrequencyDecision.STRONGLY_ACCELERATE

        if band == AdaptiveFrequencyBand.EXTREMELY_VOLATILE:
            return AdaptiveFrequencyDecision.STRONGLY_ACCELERATE

        if band == AdaptiveFrequencyBand.VOLATILE:
            return AdaptiveFrequencyDecision.ACCELERATE

        if band == AdaptiveFrequencyBand.HIGHLY_ACTIVE:
            return AdaptiveFrequencyDecision.SHORTEN

        if band == AdaptiveFrequencyBand.ACTIVE:
            return AdaptiveFrequencyDecision.SHORTEN

        if band == AdaptiveFrequencyBand.VERY_STABLE:
            return AdaptiveFrequencyDecision.LENGTHEN

        if band == AdaptiveFrequencyBand.STABLE:
            return AdaptiveFrequencyDecision.LENGTHEN

        if stability >= 0.85 and volatility <= 0.20:
            return AdaptiveFrequencyDecision.LENGTHEN

        if activity <= 0.15 and stability >= 0.70:
            return AdaptiveFrequencyDecision.DEFER

        return AdaptiveFrequencyDecision.MAINTAIN

    # ------------------------------------------------------------------------
    # ADAPTATION FACTOR
    # ------------------------------------------------------------------------

    def _adaptation_factor(
        self,
        band: AdaptiveFrequencyBand,
        decision: AdaptiveFrequencyDecision,
        strength: float,
        confidence: float,
    ) -> float:
        base = self._band_multiplier(band)

        if decision == AdaptiveFrequencyDecision.MAINTAIN:
            base = 1.0

        elif decision == AdaptiveFrequencyDecision.LENGTHEN:
            base = max(
                1.0,
                base,
            )

        elif decision == AdaptiveFrequencyDecision.SHORTEN:
            base = min(
                1.0,
                base,
            )

        elif decision == AdaptiveFrequencyDecision.ACCELERATE:
            base = min(
                1.0,
                base,
            )

        elif decision == AdaptiveFrequencyDecision.STRONGLY_ACCELERATE:
            base = min(
                1.0,
                base,
            )

        elif decision == AdaptiveFrequencyDecision.DEFER:
            base = max(
                1.0,
                self.policy.very_stable_multiplier,
            )

        confidence = self._clamp(confidence)
        strength = self._clamp(strength)

        # Blend toward neutral behavior when evidence is weak.
        adapted = (
            1.0
            + (base - 1.0)
            * strength
            * confidence
        )

        # Limit one adaptation step.
        maximum_step = max(
            self.policy.maximum_adaptation_step,
            0.0,
        )

        lower = max(
            self.policy.maximum_shortening_factor,
            1.0 - maximum_step,
        )

        upper = max(
            1.0,
            1.0 + maximum_step,
        )

        return self._clamp(
            adapted,
            lower,
            upper,
        )

    # ------------------------------------------------------------------------
    # INTERVAL ADAPTATION
    # ------------------------------------------------------------------------

    def _adapt_interval(
        self,
        previous_interval: float,
        factor: float,
    ) -> float:
        previous_interval = self._clamp(
            previous_interval,
            self.policy.minimum_interval_seconds,
            self.policy.maximum_interval_seconds,
        )

        factor = max(
            self.policy.maximum_shortening_factor,
            factor,
        )

        adapted = previous_interval * factor

        return self._clamp(
            adapted,
            self.policy.minimum_interval_seconds,
            self.policy.maximum_interval_seconds,
        )

    # ------------------------------------------------------------------------
    # REASONS
    # ------------------------------------------------------------------------

    def _reasons(
        self,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
        activity: float,
        consistency: float,
        volatility: float,
        stability: float,
        freshness_accuracy: float,
        schedule_accuracy: float,
        previous_interval: float,
        adapted_interval: float,
    ) -> List[AdaptiveFrequencyReason]:
        reasons: List[AdaptiveFrequencyReason] = []

        if history.total_recrawls < self.policy.minimum_history_samples:
            reasons.append(
                AdaptiveFrequencyReason.INSUFFICIENT_HISTORY
            )
            reasons.append(
                AdaptiveFrequencyReason.FIRST_ADAPTATION
            )

        if stability >= 0.80:
            reasons.append(
                AdaptiveFrequencyReason.HISTORICAL_STABILITY
            )
            reasons.append(
                AdaptiveFrequencyReason.STABLE_RESOURCE
            )

        if stability <= 0.25:
            reasons.append(
                AdaptiveFrequencyReason.HISTORICAL_INSTABILITY
            )

        if activity >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.HIGH_CHANGE_RATE
            )

        elif activity <= 0.20:
            reasons.append(
                AdaptiveFrequencyReason.LOW_CHANGE_RATE
            )

        if consistency >= 0.75:
            reasons.append(
                AdaptiveFrequencyReason.HIGH_CHANGE_CONSISTENCY
            )

        if volatility >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.HIGH_CHANGE_VOLATILITY
            )

        elif volatility <= 0.20:
            reasons.append(
                AdaptiveFrequencyReason.LOW_CHANGE_VOLATILITY
            )

        if signals.recent_change >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.RECENT_CHANGE_ACTIVITY
            )

        if signals.semantic_change_score >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.LONG_TERM_CHANGE_PATTERN
            )

        if freshness_accuracy >= 0.75:
            reasons.append(
                AdaptiveFrequencyReason.FRESHNESS_ACCURACY
            )

        elif freshness_accuracy <= 0.30:
            reasons.append(
                AdaptiveFrequencyReason.FRESHNESS_INACCURACY
            )

        if schedule_accuracy >= 0.75:
            reasons.append(
                AdaptiveFrequencyReason.SCHEDULED_TOO_SLOWLY
            )

        elif schedule_accuracy <= 0.30:
            reasons.append(
                AdaptiveFrequencyReason.SCHEDULED_TOO_FREQUENTLY
            )

        if history.missed_deadlines > 0:
            reasons.append(
                AdaptiveFrequencyReason.MISSED_RECRAWL_DEADLINES
            )

        if signals.query_time_sensitivity >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.TEMPORAL_SENSITIVITY
            )

        if signals.event_time_sensitivity >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.TEMPORAL_SENSITIVITY
            )

        if signals.source_freshness >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.SOURCE_FRESHNESS
            )

        if signals.feed_update_signal >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.FEED_ACTIVITY
            )

        if signals.sitemap_update_signal >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.SITEMAP_ACTIVITY
            )

        if signals.external_update_signal >= 0.70:
            reasons.append(
                AdaptiveFrequencyReason.EXTERNAL_ACTIVITY
            )

        if adapted_interval < previous_interval:
            reasons.append(
                AdaptiveFrequencyReason.HIGH_CHANGE_RATE
            )

        elif adapted_interval > previous_interval:
            reasons.append(
                AdaptiveFrequencyReason.STABLE_RESOURCE
            )

        return list(dict.fromkeys(reasons))

    # ------------------------------------------------------------------------
    # CONFIDENCE
    # ------------------------------------------------------------------------

    def _confidence(
        self,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
    ) -> float:
        history_confidence = self._history_confidence(
            history
        )

        signal_confidence = self._clamp(
            signals.confidence
        )

        completeness = 1.0
        if signals.partial:
            completeness *= 0.75

        if history.partial:
            completeness *= 0.75

        return self._clamp(
            (
                0.45 * history_confidence
                + 0.45 * signal_confidence
                + 0.10 * completeness
            )
        )

    # ------------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------------

    def _validate(
        self,
        resource_id: str,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
        previous_interval_seconds: float,
    ) -> Tuple[bool, Optional[str]]:
        if not resource_id:
            return False, "resource_id is required"

        if previous_interval_seconds <= 0:
            return False, "previous_interval_seconds must be positive"

        if (
            previous_interval_seconds
            < self.policy.minimum_interval_seconds
        ):
            return False, "previous interval is below policy minimum"

        if (
            previous_interval_seconds
            > self.policy.maximum_interval_seconds
        ):
            return False, "previous interval exceeds policy maximum"

        if (
            signals.partial
            and not self.policy.allow_partial
        ):
            return False, "partial signals are not allowed"

        if (
            history.partial
            and not self.policy.allow_partial
        ):
            return False, "partial history is not allowed"

        return True, None

    # ------------------------------------------------------------------------
    # BUILD FREQUENCY
    # ------------------------------------------------------------------------

    def _build_frequency(
        self,
        identity: AdaptiveRecrawlFrequencyIdentity,
        lineage: AdaptiveRecrawlFrequencyLineage,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
        previous_interval_seconds: float,
    ) -> AdaptiveRecrawlFrequency:
        consistency = self._change_consistency(
            history
        )

        volatility = self._volatility_score(
            signals,
            history,
        )

        stability = self._stability_score(
            signals,
            history,
            consistency,
        )

        activity = self._activity_score(
            signals,
            history,
        )

        freshness_accuracy = self._freshness_accuracy(
            signals,
            history,
        )

        schedule_accuracy = self._schedule_accuracy(
            history
        )

        confidence = self._confidence(
            signals,
            history,
        )

        band = self._frequency_band(
            activity,
            volatility,
            stability,
        )

        decision = self._decision(
            band,
            activity,
            volatility,
            stability,
            history.missed_deadlines,
        )

        strength = self._adaptation_strength(
            activity,
            volatility,
            stability,
            freshness_accuracy,
            schedule_accuracy,
            confidence,
        )

        factor = self._adaptation_factor(
            band,
            decision,
            strength,
            confidence,
        )

        adapted_interval = self._adapt_interval(
            previous_interval_seconds,
            factor,
        )

        estimated_change_interval = (
            self._estimated_change_interval(history)
        )

        estimated_recrawl_interval = (
            self._estimated_recrawl_interval(history)
        )

        reasons = self._reasons(
            signals=signals,
            history=history,
            activity=activity,
            consistency=consistency,
            volatility=volatility,
            stability=stability,
            freshness_accuracy=freshness_accuracy,
            schedule_accuracy=schedule_accuracy,
            previous_interval=previous_interval_seconds,
            adapted_interval=adapted_interval,
        )

        partial = bool(
            signals.partial
            or history.partial
        )

        return AdaptiveRecrawlFrequency(
            identity=identity,
            lineage=lineage,

            previous_interval_seconds=previous_interval_seconds,
            adapted_interval_seconds=adapted_interval,

            estimated_change_interval_seconds=estimated_change_interval,
            estimated_recrawl_interval_seconds=estimated_recrawl_interval,

            change_activity_score=activity,
            change_consistency_score=consistency,
            volatility_score=volatility,
            stability_score=stability,

            freshness_accuracy_score=freshness_accuracy,
            schedule_accuracy_score=schedule_accuracy,

            adaptation_strength=strength,
            adaptation_factor=factor,

            frequency_band=band,
            decision=decision,

            reasons=reasons,

            effective_from=self._now_iso(),
            version=ARCHITECTURE_VERSION,

            confidence=confidence,
            partial=partial,

            metadata={
                "scale_target": SCALE_TARGET,
                "google_scale_capability_target":
                    GOOGLE_SCALE_CAPABILITY_TARGET,
                "google_technology_dependency":
                    GOOGLE_TECHNOLOGY_DEPENDENCY,
                "phase": PHASE,
                "previous_stage": PREVIOUS_STAGE,
                "next_stage": NEXT_STAGE,
            },
        )

    # ------------------------------------------------------------------------
    # SINGLE RESOURCE SCHEDULE
    # ------------------------------------------------------------------------

    def adapt(
        self,
        resource_id: str,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
        previous_interval_seconds: float,
        schedule_id: str = "",
        partition_id: str = "",
        host_id: str = "",
        domain_id: str = "",
        previous_frequency_version: str = "",
        previous_schedule_ids: Optional[Sequence[str]] = None,
    ) -> AdaptiveRecrawlFrequencyResult:
        request_id = self._request_id(
            resource_id,
            schedule_id,
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.REQUEST_RECEIVED,
            AdaptiveRecrawlFrequencyState.RECEIVED,
            {
                "schedule_id": schedule_id,
                "previous_interval_seconds":
                    previous_interval_seconds,
            },
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.VALIDATION_STARTED,
            AdaptiveRecrawlFrequencyState.VALIDATING,
        )

        normalized_signals = self._normalize_signals(
            signals
        )

        normalized_history = self._normalize_history(
            history
        )

        valid, error = self._validate(
            resource_id,
            normalized_signals,
            normalized_history,
            previous_interval_seconds,
        )

        if not valid:
            result = AdaptiveRecrawlFrequencyResult(
                state=AdaptiveRecrawlFrequencyState.REJECTED,
                frequency=None,
                resource_id=resource_id,
                request_id=request_id,
                accepted=False,
                partial=False,
                deferred=False,
                rejected=True,
                confidence=0.0,
                error=error,
                created_at=self._now_iso(),
            )

            self._event(
                request_id,
                resource_id,
                AdaptiveFrequencyEventType.ADAPTATION_REJECTED,
                AdaptiveRecrawlFrequencyState.REJECTED,
                {"error": error},
            )

            self.backend.persist_result(result)
            return result

        self._checkpoint(
            request_id,
            resource_id,
            AdaptiveFrequencyCheckpointType.INPUT_ACCEPTED,
            AdaptiveRecrawlFrequencyState.VALIDATING,
            confidence=self._confidence(
                normalized_signals,
                normalized_history,
            ),
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.HISTORY_NORMALIZATION_STARTED,
            AdaptiveRecrawlFrequencyState.HISTORY_NORMALIZATION,
        )

        self._checkpoint(
            request_id,
            resource_id,
            AdaptiveFrequencyCheckpointType.HISTORY_NORMALIZED,
            AdaptiveRecrawlFrequencyState.HISTORY_NORMALIZATION,
            confidence=self._confidence(
                normalized_signals,
                normalized_history,
            ),
        )

        if (
            normalized_signals.partial
            or normalized_history.partial
        ):
            self._event(
                request_id,
                resource_id,
                AdaptiveFrequencyEventType.PARTIAL_INPUT_DETECTED,
                AdaptiveRecrawlFrequencyState.PARTIAL,
                {
                    "signals_partial":
                        normalized_signals.partial,
                    "history_partial":
                        normalized_history.partial,
                },
            )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.BEHAVIOR_ANALYSIS_STARTED,
            AdaptiveRecrawlFrequencyState.BEHAVIOR_ANALYSIS,
        )

        activity = self._activity_score(
            normalized_signals,
            normalized_history,
        )

        consistency = self._change_consistency(
            normalized_history
        )

        volatility = self._volatility_score(
            normalized_signals,
            normalized_history,
        )

        stability = self._stability_score(
            normalized_signals,
            normalized_history,
            consistency,
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.CHANGE_BEHAVIOR_ANALYZED,
            AdaptiveRecrawlFrequencyState.BEHAVIOR_ANALYSIS,
            {
                "activity": activity,
                "consistency": consistency,
                "volatility": volatility,
                "stability": stability,
            },
        )

        self._checkpoint(
            request_id,
            resource_id,
            AdaptiveFrequencyCheckpointType.BEHAVIOR_ANALYZED,
            AdaptiveRecrawlFrequencyState.BEHAVIOR_ANALYSIS,
            confidence=self._confidence(
                normalized_signals,
                normalized_history,
            ),
            payload={
                "activity": activity,
                "consistency": consistency,
                "volatility": volatility,
                "stability": stability,
            },
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.FREQUENCY_ESTIMATION_STARTED,
            AdaptiveRecrawlFrequencyState.FREQUENCY_ESTIMATION,
        )

        base_interval = self._base_interval(
            normalized_history
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.BASE_FREQUENCY_CALCULATED,
            AdaptiveRecrawlFrequencyState.FREQUENCY_ESTIMATION,
            {
                "base_interval_seconds": base_interval,
            },
        )

        self._checkpoint(
            request_id,
            resource_id,
            AdaptiveFrequencyCheckpointType.BASE_FREQUENCY_CALCULATED,
            AdaptiveRecrawlFrequencyState.FREQUENCY_ESTIMATION,
            interval_seconds=base_interval,
            confidence=self._confidence(
                normalized_signals,
                normalized_history,
            ),
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.STABILITY_ANALYSIS_STARTED,
            AdaptiveRecrawlFrequencyState.STABILITY_ANALYSIS,
            {"stability": stability},
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.VOLATILITY_ANALYSIS_STARTED,
            AdaptiveRecrawlFrequencyState.VOLATILITY_ANALYSIS,
            {"volatility": volatility},
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.ADAPTATION_STARTED,
            AdaptiveRecrawlFrequencyState.ADAPTATION,
        )

        identity = AdaptiveRecrawlFrequencyIdentity(
            resource_id=resource_id,
            schedule_id=schedule_id,
            frequency_version=ARCHITECTURE_VERSION,
            partition_id=partition_id,
            host_id=host_id,
            domain_id=domain_id,
        )

        lineage = AdaptiveRecrawlFrequencyLineage(
            resource_id=resource_id,
            previous_stage=PREVIOUS_STAGE,
            current_stage=PHASE,
            previous_schedule_id=schedule_id,
            previous_frequency_version=previous_frequency_version,
            source_signal_version=normalized_signals.signal_version,
            source_history_version=normalized_history.history_version,
            parent_schedule_ids=list(
                previous_schedule_ids or []
            ),
            lineage_metadata={
                "architecture_version":
                    ARCHITECTURE_VERSION,
            },
        )

        frequency = self._build_frequency(
            identity=identity,
            lineage=lineage,
            signals=normalized_signals,
            history=normalized_history,
            previous_interval_seconds=previous_interval_seconds,
        )

        self._checkpoint(
            request_id,
            resource_id,
            AdaptiveFrequencyCheckpointType.ADAPTATION_CALCULATED,
            AdaptiveRecrawlFrequencyState.ADAPTATION,
            interval_seconds=frequency.adapted_interval_seconds,
            confidence=frequency.confidence,
            payload={
                "adaptation_factor":
                    frequency.adaptation_factor,
                "adaptation_strength":
                    frequency.adaptation_strength,
                "decision":
                    frequency.decision.value,
                "frequency_band":
                    frequency.frequency_band.value,
            },
        )

        self._checkpoint(
            request_id,
            resource_id,
            AdaptiveFrequencyCheckpointType.FREQUENCY_CREATED,
            AdaptiveRecrawlFrequencyState.DECISION,
            interval_seconds=frequency.adapted_interval_seconds,
            confidence=frequency.confidence,
        )

        if (
            frequency.decision
            == AdaptiveFrequencyDecision.DEFER
        ):
            final_state = AdaptiveRecrawlFrequencyState.DEFERRED
            deferred = True
        elif frequency.partial:
            final_state = AdaptiveRecrawlFrequencyState.PARTIAL
            deferred = False
        else:
            final_state = AdaptiveRecrawlFrequencyState.COMPLETED
            deferred = False

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.FREQUENCY_ADAPTED,
            AdaptiveRecrawlFrequencyState.DECISION,
            {
                "previous_interval_seconds":
                    frequency.previous_interval_seconds,
                "adapted_interval_seconds":
                    frequency.adapted_interval_seconds,
                "decision":
                    frequency.decision.value,
            },
        )

        self._event(
            request_id,
            resource_id,
            AdaptiveFrequencyEventType.ADAPTATION_COMPLETED,
            final_state,
            {
                "adapted_interval_seconds":
                    frequency.adapted_interval_seconds,
                "confidence":
                    frequency.confidence,
            },
        )

        self._checkpoint(
            request_id,
            resource_id,
            AdaptiveFrequencyCheckpointType.COMPLETED,
            final_state,
            interval_seconds=frequency.adapted_interval_seconds,
            confidence=frequency.confidence,
        )

        result = AdaptiveRecrawlFrequencyResult(
            state=final_state,
            frequency=frequency,
            resource_id=resource_id,
            request_id=request_id,
            accepted=True,
            partial=frequency.partial,
            deferred=deferred,
            rejected=False,
            confidence=frequency.confidence,
            error=None,
            created_at=self._now_iso(),
        )

        self.backend.persist_result(result)

        return result

    # ------------------------------------------------------------------------
    # BATCH ADAPTATION
    # ------------------------------------------------------------------------

    def adapt_many(
        self,
        resources: Iterable[
            Tuple[
                str,
                AdaptiveRecrawlSignals,
                AdaptiveRecrawlHistory,
                float,
            ]
        ],
    ) -> List[AdaptiveRecrawlFrequencyResult]:
        resource_list = list(resources)

        if (
            len(resource_list)
            > self.policy.max_resources_per_batch
        ):
            raise ValueError(
                "batch exceeds max_resources_per_batch"
            )

        results: List[
            AdaptiveRecrawlFrequencyResult
        ] = []

        for (
            resource_id,
            signals,
            history,
            previous_interval,
        ) in resource_list:
            results.append(
                self.adapt(
                    resource_id=resource_id,
                    signals=signals,
                    history=history,
                    previous_interval_seconds=previous_interval,
                )
            )

        return results

    # ------------------------------------------------------------------------
    # FREQUENCY-ONLY UPDATE
    # ------------------------------------------------------------------------

    def update_frequency(
        self,
        previous_frequency: AdaptiveRecrawlFrequency,
        signals: AdaptiveRecrawlSignals,
        history: AdaptiveRecrawlHistory,
    ) -> AdaptiveRecrawlFrequencyResult:
        return self.adapt(
            resource_id=(
                previous_frequency.identity.resource_id
            ),
            signals=signals,
            history=history,
            previous_interval_seconds=(
                previous_frequency.adapted_interval_seconds
            ),
            schedule_id=(
                previous_frequency.identity.schedule_id
            ),
            partition_id=(
                previous_frequency.identity.partition_id
            ),
            host_id=(
                previous_frequency.identity.host_id
            ),
            domain_id=(
                previous_frequency.identity.domain_id
            ),
            previous_frequency_version=(
                previous_frequency.version
            ),
            previous_schedule_ids=(
                previous_frequency.lineage.parent_schedule_ids
            ),
        )

    # ------------------------------------------------------------------------
    # METADATA ACCESS
    # ------------------------------------------------------------------------

    def events(
        self,
    ) -> List[AdaptiveRecrawlFrequencyEvent]:
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
    ) -> List[AdaptiveRecrawlFrequencyCheckpoint]:
        method = getattr(
            self.backend,
            "checkpoints",
            None,
        )

        if callable(method):
            return list(method())

        return []

    def results(
        self,
    ) -> List[AdaptiveRecrawlFrequencyResult]:
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

    def architecture(self) -> Dict[str, Any]:
        return {
            "phase": PHASE,
            "name": "Adaptive Recrawl Frequency",
            "version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,
            "google_scale_capability_target":
                GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency":
                GOOGLE_TECHNOLOGY_DEPENDENCY,

            "purpose": (
                "Adapt long-term recrawl frequency from historical "
                "change behavior, volatility, stability, freshness "
                "accuracy, schedule accuracy, and current freshness "
                "signals."
            ),

            "inputs": [
                "phase13.3_recrawl_schedule",
                "recrawl_history",
                "phase13.1_freshness_signals",
                "phase13.2_change_signals",
                "current_change_activity",
                "freshness_accuracy",
                "schedule_accuracy",
            ],

            "outputs": [
                "adaptive_recrawl_interval",
                "frequency_band",
                "adaptation_factor",
                "adaptation_strength",
                "adaptive_frequency_decision",
                "adaptation_reasons",
                "confidence",
                "lineage",
                "checkpoint",
            ],

            "execution": {
                "distributed": True,
                "partition_local": True,
                "resource_partitionable": True,
                "host_partitionable": True,
                "domain_partitionable": True,
                "horizontally_scalable": True,
                "incremental": True,
                "deterministic": self.policy.deterministic,
                "checkpointable": self.policy.checkpoint_enabled,
                "restartable": True,
                "partial_input_supported":
                    self.policy.allow_partial,
                "backend_replaceable": True,
                "versioned": True,
                "lineage_preserving": True,
                "provenance_preserving": True,
            },

            "global_limits": {
                "resources": None,
                "urls": None,
                "documents": None,
                "hosts": None,
                "domains": None,
                "partitions": None,
                "workers": None,
                "schedules": None,
                "histories": None,
            },

            "per_unit_limits": {
                "max_resources_per_batch":
                    self.policy.max_resources_per_batch,
                "minimum_history_samples":
                    self.policy.minimum_history_samples,
            },

            "frequency_policy": {
                "minimum_interval_seconds":
                    self.policy.minimum_interval_seconds,
                "default_interval_seconds":
                    self.policy.default_interval_seconds,
                "maximum_interval_seconds":
                    self.policy.maximum_interval_seconds,

                "very_stable_multiplier":
                    self.policy.very_stable_multiplier,
                "stable_multiplier":
                    self.policy.stable_multiplier,
                "normal_multiplier":
                    self.policy.normal_multiplier,
                "active_multiplier":
                    self.policy.active_multiplier,
                "highly_active_multiplier":
                    self.policy.highly_active_multiplier,
                "volatile_multiplier":
                    self.policy.volatile_multiplier,
                "extremely_volatile_multiplier":
                    self.policy.extremely_volatile_multiplier,
            },

            "stage_boundary": {
                "13.1": (
                    "determines freshness attention and urgency"
                ),
                "13.2": (
                    "determines what changed and change volatility"
                ),
                "13.3": (
                    "determines when a resource should be "
                    "scheduled for recrawl"
                ),
                "13.4": (
                    "adapts the long-term frequency at which "
                    "the resource should be revisited"
                ),
                "13.5": (
                    "will manage URL/document freshness queues"
                ),
            },

            "does_not": [
                "execute_crawling",
                "perform_http_requests",
                "fetch_web_resources",
                "assign_crawler_workers",
                "perform_global_crawl_orchestration",
                "mutate_search_index",
                "perform_final_ranking",
                "train_ranking_models",
                "classify_spam",
                "replace_phase13.3_scheduling",
                "depend_on_google_search_api",
                "depend_on_google_index",
                "depend_on_google_crawler",
                "depend_on_google_infrastructure",
                "depend_on_google_ranking_technology",
            ],

            "next_stage": NEXT_STAGE,
            "next_stage_name": (
                "URL/Document Freshness Queues"
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


AdaptiveRecrawlFrequency = AdaptiveRecrawlFrequencyArchitecture
GlobalAdaptiveRecrawlFrequency = AdaptiveRecrawlFrequencyArchitecture
Phase13_4AdaptiveRecrawlFrequency = AdaptiveRecrawlFrequencyArchitecture


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

    "AdaptiveRecrawlFrequencyState",
    "AdaptiveFrequencyBand",
    "AdaptiveFrequencyDecision",
    "AdaptiveFrequencyReason",
    "AdaptiveFrequencyEventType",
    "AdaptiveFrequencyCheckpointType",

    "AdaptiveRecrawlFrequencyIdentity",
    "AdaptiveRecrawlFrequencyLineage",
    "AdaptiveRecrawlHistory",
    "AdaptiveRecrawlSignals",
    "AdaptiveRecrawlFrequencyPolicy",
    "AdaptiveRecrawlFrequency",
    "AdaptiveRecrawlFrequencyResult",
    "AdaptiveRecrawlFrequencyCheckpoint",
    "AdaptiveRecrawlFrequencyEvent",

    "AdaptiveRecrawlFrequencyBackend",
    "InMemoryAdaptiveRecrawlFrequencyMetadata",

    "AdaptiveRecrawlFrequencyArchitecture",
    "AdaptiveRecrawlFrequency",
    "GlobalAdaptiveRecrawlFrequency",
    "Phase13_4AdaptiveRecrawlFrequency",
]
