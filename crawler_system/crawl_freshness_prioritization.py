"""
OUR SEARCH
Phase 13.1 — Crawl Freshness Prioritization

Production architecture for prioritizing crawl/recrawl work according to
freshness requirements at enormous public-Web scale.

Scale target:
    billions -> trillions of publicly accessible Web resources

This module is intentionally independent of:
    - Google Search API
    - Google index
    - Google crawler
    - Google infrastructure
    - Google ranking technology

Responsibilities:
    - Represent freshness evidence for Web resources.
    - Estimate freshness urgency.
    - Combine temporal, historical, source, query, and change evidence.
    - Produce deterministic freshness-priority decisions.
    - Support distributed and partition-local computation.
    - Preserve provenance and lineage.
    - Support checkpointing and restartability.
    - Remain backend-replaceable.

This module does NOT:
    - execute crawling
    - schedule actual crawler workers
    - mutate the index
    - fetch Web pages
    - perform final search ranking
    - classify spam
    - replace recrawl orchestration
    - depend on a specific ML framework
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from math import exp, isfinite
from typing import Dict, Iterable, List, Optional, Protocol, Sequence, Tuple
from uuid import uuid4


# ---------------------------------------------------------------------------
# Architecture constants
# ---------------------------------------------------------------------------

SCALE_TARGET = "billions_to_trillions_of_public_web_resources"
GOOGLE_SCALE_CAPABILITY_TARGET = True
GOOGLE_TECHNOLOGY_DEPENDENCY = False

ARCHITECTURE_VERSION = "crawl-freshness-prioritization.v1"
PHASE = "13.1"
NEXT_STAGE = "13.2"


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class FreshnessPriorityState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    EVIDENCE_NORMALIZATION = "evidence_normalization"
    FRESHNESS_ESTIMATION = "freshness_estimation"
    PRIORITY_CALCULATION = "priority_calculation"
    URGENCY_CALCULATION = "urgency_calculation"
    DECISION = "decision"
    COMPLETED = "completed"
    PARTIAL = "partial"
    REJECTED = "rejected"
    FAILED = "failed"


class FreshnessEvidenceType(str, Enum):
    DOCUMENT_AGE = "document_age"
    LAST_CRAWL_AGE = "last_crawl_age"
    RECENT_CHANGE = "recent_change"
    CHANGE_RATE = "change_rate"
    UPDATE_REGULARITY = "update_regularity"
    SOURCE_FRESHNESS = "source_freshness"
    TEMPORAL_VOLATILITY = "temporal_volatility"
    QUERY_TIME_SENSITIVITY = "query_time_sensitivity"
    EVENT_TIME_SENSITIVITY = "event_time_sensitivity"
    CONTENT_VOLATILITY = "content_volatility"
    HISTORICAL_STABILITY = "historical_stability"
    PRIOR_FRESHNESS_ACCURACY = "prior_freshness_accuracy"
    HTTP_VALIDATOR_AGE = "http_validator_age"
    FEED_UPDATE_SIGNAL = "feed_update_signal"
    SITEMAP_UPDATE_SIGNAL = "sitemap_update_signal"
    EXTERNAL_UPDATE_SIGNAL = "external_update_signal"


class FreshnessUrgency(str, Enum):
    NONE = "none"
    VERY_LOW = "very_low"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"
    CRITICAL = "critical"


class FreshnessPriorityBand(str, Enum):
    BACKGROUND = "background"
    NORMAL = "normal"
    ELEVATED = "elevated"
    HIGH = "high"
    URGENT = "urgent"
    IMMEDIATE = "immediate"


class FreshnessDecision(str, Enum):
    DEFER = "defer"
    MONITOR = "monitor"
    PRIORITIZE = "prioritize"
    HIGH_PRIORITY = "high_priority"
    URGENT_RECRAWL = "urgent_recrawl"


class FreshnessEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    EVIDENCE_NORMALIZATION_STARTED = "evidence_normalization_started"
    FRESHNESS_ESTIMATED = "freshness_estimated"
    PRIORITY_CALCULATED = "priority_calculated"
    URGENCY_CALCULATED = "urgency_calculated"
    DECISION_CREATED = "decision_created"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    PRIORITIZATION_COMPLETED = "prioritization_completed"
    PRIORITIZATION_REJECTED = "prioritization_rejected"
    PRIORITIZATION_FAILED = "prioritization_failed"


class FreshnessCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    EVIDENCE_NORMALIZED = "evidence_normalized"
    PRIORITY_CALCULATED = "priority_calculated"
    DECISION_CREATED = "decision_created"
    COMPLETED = "completed"


# ---------------------------------------------------------------------------
# Identity and lineage
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FreshnessPriorityIdentity:
    request_id: str
    resource_id: str
    canonical_url: str
    hostname: str
    partition_id: Optional[str] = None
    shard_id: Optional[str] = None
    version: str = ARCHITECTURE_VERSION


@dataclass
class FreshnessPriorityLineage:
    source_system: str = "phase13.1-crawl-freshness-prioritization"
    source_version: str = ARCHITECTURE_VERSION
    previous_stage: str = "phase12-freshness-ranking-signals"
    evidence_sources: List[str] = field(default_factory=list)
    parent_request_ids: List[str] = field(default_factory=list)
    calculation_timestamp: str = ""
    provenance_preserved: bool = True


# ---------------------------------------------------------------------------
# Evidence
# ---------------------------------------------------------------------------


@dataclass
class FreshnessEvidence:
    evidence_type: FreshnessEvidenceType
    value: float
    confidence: float = 1.0
    observed_at: Optional[str] = None
    source: str = "unknown"
    partial: bool = False
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class FreshnessEvidenceBundle:
    resource_id: str
    evidence: List[FreshnessEvidence] = field(default_factory=list)
    partial: bool = False
    confidence: float = 1.0


# ---------------------------------------------------------------------------
# Resource temporal state
# ---------------------------------------------------------------------------


@dataclass
class ResourceFreshnessState:
    resource_id: str
    canonical_url: str

    first_seen_at: Optional[str] = None
    last_crawled_at: Optional[str] = None
    last_changed_at: Optional[str] = None
    last_modified_at: Optional[str] = None

    observed_change_count: int = 0
    observed_crawl_count: int = 0

    historical_change_rate: float = 0.0
    historical_update_regularity: float = 0.0

    source_freshness: float = 0.0
    temporal_volatility: float = 0.0
    content_volatility: float = 0.0

    query_time_sensitivity: float = 0.0
    event_time_sensitivity: float = 0.0

    prior_freshness_accuracy: float = 0.5
    historical_stability: float = 0.5

    feed_update_signal: float = 0.0
    sitemap_update_signal: float = 0.0
    external_update_signal: float = 0.0

    partial: bool = False


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


@dataclass
class CrawlFreshnessPrioritizationPolicy:
    """
    Policy controls for freshness prioritization.

    There are no global Web-scale ceilings here. Limits apply to an individual
    request/batch and are intended to be horizontally scalable.
    """

    max_evidence_per_resource: int = 128
    max_resources_per_batch: int = 100_000

    minimum_confidence: float = 0.10

    allow_partial: bool = True
    deterministic: bool = True
    checkpoint_enabled: bool = True

    # Evidence weights.
    recent_change_weight: float = 1.50
    change_rate_weight: float = 1.35
    source_freshness_weight: float = 1.20
    temporal_volatility_weight: float = 1.20
    content_volatility_weight: float = 1.10

    query_time_sensitivity_weight: float = 1.00
    event_time_sensitivity_weight: float = 1.00

    last_crawl_age_weight: float = 1.15
    document_age_weight: float = 0.75

    update_regularity_weight: float = 0.90
    prior_accuracy_weight: float = 0.65

    feed_signal_weight: float = 0.80
    sitemap_signal_weight: float = 0.70
    external_signal_weight: float = 0.60

    historical_stability_weight: float = 0.50

    # Urgency shaping.
    freshness_half_life_days: float = 30.0
    crawl_half_life_days: float = 7.0
    change_half_life_days: float = 14.0

    urgency_threshold_low: float = 0.20
    urgency_threshold_moderate: float = 0.40
    urgency_threshold_high: float = 0.60
    urgency_threshold_very_high: float = 0.80
    urgency_threshold_critical: float = 0.93

    # Priority thresholds.
    defer_threshold: float = 0.15
    monitor_threshold: float = 0.30
    prioritize_threshold: float = 0.50
    high_priority_threshold: float = 0.72
    urgent_recrawl_threshold: float = 0.90


# ---------------------------------------------------------------------------
# Result objects
# ---------------------------------------------------------------------------


@dataclass
class FreshnessPriorityScore:
    resource_id: str

    freshness_score: float
    urgency_score: float
    priority_score: float

    confidence: float

    urgency: FreshnessUrgency
    priority_band: FreshnessPriorityBand
    decision: FreshnessDecision

    partial: bool = False

    contributing_signals: Dict[str, float] = field(default_factory=dict)


@dataclass
class FreshnessPriorityResult:
    identity: FreshnessPriorityIdentity
    lineage: FreshnessPriorityLineage

    state: FreshnessPriorityState
    score: FreshnessPriorityScore

    created_at: str
    completed_at: Optional[str] = None

    error: Optional[str] = None


@dataclass
class FreshnessPriorityCheckpoint:
    checkpoint_id: str
    request_id: str
    resource_id: str

    checkpoint_type: FreshnessCheckpointType
    state: FreshnessPriorityState

    created_at: str

    freshness_score: Optional[float] = None
    urgency_score: Optional[float] = None
    priority_score: Optional[float] = None

    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class FreshnessPriorityEvent:
    event_id: str
    request_id: str
    resource_id: str

    event_type: FreshnessEventType
    timestamp: str

    state: FreshnessPriorityState

    metadata: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Backend abstraction
# ---------------------------------------------------------------------------


class CrawlFreshnessPrioritizationBackend(Protocol):
    def persist_event(self, event: FreshnessPriorityEvent) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: FreshnessPriorityCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: FreshnessPriorityResult,
    ) -> None:
        ...

    def get_result(
        self,
        request_id: str,
    ) -> Optional[FreshnessPriorityResult]:
        ...


class InMemoryCrawlFreshnessPrioritizationMetadata:
    """
    Reference metadata backend.

    Production deployments may replace this with a distributed durable
    metadata system without changing the prioritization architecture.
    """

    def __init__(self) -> None:
        self._events: Dict[str, FreshnessPriorityEvent] = {}
        self._checkpoints: Dict[str, FreshnessPriorityCheckpoint] = {}
        self._results: Dict[str, FreshnessPriorityResult] = {}

    def persist_event(self, event: FreshnessPriorityEvent) -> None:
        self._events[event.event_id] = event

    def persist_checkpoint(
        self,
        checkpoint: FreshnessPriorityCheckpoint,
    ) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def persist_result(
        self,
        result: FreshnessPriorityResult,
    ) -> None:
        self._results[result.identity.request_id] = result

    def get_result(
        self,
        request_id: str,
    ) -> Optional[FreshnessPriorityResult]:
        return self._results.get(request_id)

    def events(self) -> List[FreshnessPriorityEvent]:
        return list(self._events.values())

    def checkpoints(self) -> List[FreshnessPriorityCheckpoint]:
        return list(self._checkpoints.values())

    def results(self) -> List[FreshnessPriorityResult]:
        return list(self._results.values())


# ---------------------------------------------------------------------------
# Main architecture
# ---------------------------------------------------------------------------


class CrawlFreshnessPrioritizationArchitecture:
    """
    Phase 13.1 freshness-priority architecture.

    The architecture transforms freshness evidence into a deterministic
    priority decision that later recrawl scheduling/orchestration systems
    can consume.

    Conceptual flow:

        resource temporal state
                +
        freshness evidence
                |
                v
        evidence normalization
                |
                v
        freshness estimation
                |
                v
        urgency estimation
                |
                v
        priority calculation
                |
                v
        freshness decision
                |
                v
        Phase 13.2+
    """

    def __init__(
        self,
        backend: Optional[CrawlFreshnessPrioritizationBackend] = None,
        policy: Optional[CrawlFreshnessPrioritizationPolicy] = None,
    ) -> None:
        self.backend = (
            backend
            if backend is not None
            else InMemoryCrawlFreshnessPrioritizationMetadata()
        )

        self.policy = (
            policy
            if policy is not None
            else CrawlFreshnessPrioritizationPolicy()
        )

    # ------------------------------------------------------------------
    # Time / numeric helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
        if not isfinite(value):
            return minimum

        return max(minimum, min(maximum, value))

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            result = float(value)

            if not isfinite(result):
                return default

            return result

        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None

        try:
            timestamp = value

            if timestamp.endswith("Z"):
                timestamp = timestamp[:-1] + "+00:00"

            parsed = datetime.fromisoformat(timestamp)

            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)

            return parsed.astimezone(timezone.utc)

        except (TypeError, ValueError):
            return None

    def _age_days(
        self,
        timestamp: Optional[str],
        now: Optional[datetime] = None,
    ) -> Optional[float]:
        parsed = self._parse_timestamp(timestamp)

        if parsed is None:
            return None

        reference = now or datetime.now(timezone.utc)

        age_seconds = max(
            0.0,
            (reference - parsed).total_seconds(),
        )

        return age_seconds / 86_400.0

    @staticmethod
    def _decay(
        age_days: Optional[float],
        half_life_days: float,
    ) -> float:
        if age_days is None:
            return 0.0

        half_life = max(0.001, half_life_days)

        return exp(
            -0.6931471805599453
            * max(0.0, age_days)
            / half_life
        )

    # ------------------------------------------------------------------
    # Event / checkpoint helpers
    # ------------------------------------------------------------------

    def _event(
        self,
        identity: FreshnessPriorityIdentity,
        event_type: FreshnessEventType,
        state: FreshnessPriorityState,
        metadata: Optional[Dict[str, str]] = None,
    ) -> FreshnessPriorityEvent:
        event = FreshnessPriorityEvent(
            event_id=str(uuid4()),
            request_id=identity.request_id,
            resource_id=identity.resource_id,
            event_type=event_type,
            timestamp=self._now(),
            state=state,
            metadata=metadata or {},
        )

        self.backend.persist_event(event)

        return event

    def _checkpoint(
        self,
        identity: FreshnessPriorityIdentity,
        checkpoint_type: FreshnessCheckpointType,
        state: FreshnessPriorityState,
        score: Optional[FreshnessPriorityScore] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> FreshnessPriorityCheckpoint:
        checkpoint = FreshnessPriorityCheckpoint(
            checkpoint_id=str(uuid4()),
            request_id=identity.request_id,
            resource_id=identity.resource_id,
            checkpoint_type=checkpoint_type,
            state=state,
            created_at=self._now(),
            freshness_score=(
                score.freshness_score
                if score is not None
                else None
            ),
            urgency_score=(
                score.urgency_score
                if score is not None
                else None
            ),
            priority_score=(
                score.priority_score
                if score is not None
                else None
            ),
            metadata=metadata or {},
        )

        self.backend.persist_checkpoint(checkpoint)

        return checkpoint

    # ------------------------------------------------------------------
    # Evidence normalization
    # ------------------------------------------------------------------

    def _normalize_evidence(
        self,
        bundle: FreshnessEvidenceBundle,
    ) -> FreshnessEvidenceBundle:
        normalized: List[FreshnessEvidence] = []

        evidence = bundle.evidence[
            : self.policy.max_evidence_per_resource
        ]

        for item in evidence:
            normalized.append(
                FreshnessEvidence(
                    evidence_type=item.evidence_type,
                    value=self._clamp(
                        self._safe_float(item.value)
                    ),
                    confidence=self._clamp(
                        self._safe_float(
                            item.confidence,
                            1.0,
                        )
                    ),
                    observed_at=item.observed_at,
                    source=item.source,
                    partial=item.partial,
                    metadata=dict(item.metadata),
                )
            )

        confidence_values = [
            item.confidence
            for item in normalized
        ]

        confidence = (
            sum(confidence_values) / len(confidence_values)
            if confidence_values
            else 0.0
        )

        partial = (
            bundle.partial
            or any(item.partial for item in normalized)
            or not normalized
        )

        return FreshnessEvidenceBundle(
            resource_id=bundle.resource_id,
            evidence=normalized,
            partial=partial,
            confidence=self._clamp(confidence),
        )

    # ------------------------------------------------------------------
    # Evidence extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _evidence_map(
        bundle: FreshnessEvidenceBundle,
    ) -> Dict[FreshnessEvidenceType, float]:
        result: Dict[FreshnessEvidenceType, float] = {}

        for evidence in bundle.evidence:
            previous = result.get(evidence.evidence_type)

            if previous is None:
                result[evidence.evidence_type] = evidence.value
            else:
                result[evidence.evidence_type] = max(
                    previous,
                    evidence.value,
                )

        return result

    # ------------------------------------------------------------------
    # Temporal freshness estimation
    # ------------------------------------------------------------------

    def _estimate_temporal_freshness(
        self,
        state: ResourceFreshnessState,
        evidence: FreshnessEvidenceBundle,
        now: datetime,
    ) -> Tuple[float, Dict[str, float]]:
        values = self._evidence_map(evidence)

        signals: Dict[str, float] = {}

        last_crawl_age = self._age_days(
            state.last_crawled_at,
            now,
        )

        document_age = self._age_days(
            state.last_modified_at
            or state.first_seen_at,
            now,
        )

        change_age = self._age_days(
            state.last_changed_at,
            now,
        )

        signals["last_crawl_recency"] = self._decay(
            last_crawl_age,
            self.policy.crawl_half_life_days,
        )

        signals["document_recency"] = self._decay(
            document_age,
            self.policy.freshness_half_life_days,
        )

        signals["recent_change"] = (
            values.get(
                FreshnessEvidenceType.RECENT_CHANGE,
                0.0,
            )
        )

        if change_age is not None:
            signals["recent_change"] = max(
                signals["recent_change"],
                self._decay(
                    change_age,
                    self.policy.change_half_life_days,
                ),
            )

        signals["change_rate"] = max(
            self._clamp(state.historical_change_rate),
            values.get(
                FreshnessEvidenceType.CHANGE_RATE,
                0.0,
            ),
        )

        signals["update_regularity"] = max(
            self._clamp(state.historical_update_regularity),
            values.get(
                FreshnessEvidenceType.UPDATE_REGULARITY,
                0.0,
            ),
        )

        signals["source_freshness"] = max(
            self._clamp(state.source_freshness),
            values.get(
                FreshnessEvidenceType.SOURCE_FRESHNESS,
                0.0,
            ),
        )

        signals["temporal_volatility"] = max(
            self._clamp(state.temporal_volatility),
            values.get(
                FreshnessEvidenceType.TEMPORAL_VOLATILITY,
                0.0,
            ),
        )

        signals["content_volatility"] = max(
            self._clamp(state.content_volatility),
            values.get(
                FreshnessEvidenceType.CONTENT_VOLATILITY,
                0.0,
            ),
        )

        signals["query_time_sensitivity"] = max(
            self._clamp(state.query_time_sensitivity),
            values.get(
                FreshnessEvidenceType.QUERY_TIME_SENSITIVITY,
                0.0,
            ),
        )

        signals["event_time_sensitivity"] = max(
            self._clamp(state.event_time_sensitivity),
            values.get(
                FreshnessEvidenceType.EVENT_TIME_SENSITIVITY,
                0.0,
            ),
        )

        signals["prior_freshness_accuracy"] = max(
            self._clamp(state.prior_freshness_accuracy),
            values.get(
                FreshnessEvidenceType.PRIOR_FRESHNESS_ACCURACY,
                0.0,
            ),
        )

        signals["feed_update_signal"] = max(
            self._clamp(state.feed_update_signal),
            values.get(
                FreshnessEvidenceType.FEED_UPDATE_SIGNAL,
                0.0,
            ),
        )

        signals["sitemap_update_signal"] = max(
            self._clamp(state.sitemap_update_signal),
            values.get(
                FreshnessEvidenceType.SITEMAP_UPDATE_SIGNAL,
                0.0,
            ),
        )

        signals["external_update_signal"] = max(
            self._clamp(state.external_update_signal),
            values.get(
                FreshnessEvidenceType.EXTERNAL_UPDATE_SIGNAL,
                0.0,
            ),
        )

        signals["historical_stability"] = max(
            self._clamp(state.historical_stability),
            values.get(
                FreshnessEvidenceType.HISTORICAL_STABILITY,
                0.0,
            ),
        )

        weighted_components = [
            (
                signals["recent_change"],
                self.policy.recent_change_weight,
            ),
            (
                signals["change_rate"],
                self.policy.change_rate_weight,
            ),
            (
                signals["source_freshness"],
                self.policy.source_freshness_weight,
            ),
            (
                signals["temporal_volatility"],
                self.policy.temporal_volatility_weight,
            ),
            (
                signals["content_volatility"],
                self.policy.content_volatility_weight,
            ),
            (
                signals["query_time_sensitivity"],
                self.policy.query_time_sensitivity_weight,
            ),
            (
                signals["event_time_sensitivity"],
                self.policy.event_time_sensitivity_weight,
            ),
            (
                signals["last_crawl_recency"],
                self.policy.last_crawl_age_weight,
            ),
            (
                signals["document_recency"],
                self.policy.document_age_weight,
            ),
            (
                signals["update_regularity"],
                self.policy.update_regularity_weight,
            ),
            (
                signals["prior_freshness_accuracy"],
                self.policy.prior_accuracy_weight,
            ),
            (
                signals["feed_update_signal"],
                self.policy.feed_signal_weight,
            ),
            (
                signals["sitemap_update_signal"],
                self.policy.sitemap_signal_weight,
            ),
            (
                signals["external_update_signal"],
                self.policy.external_signal_weight,
            ),
        ]

        numerator = sum(
            value * weight
            for value, weight in weighted_components
        )

        denominator = sum(
            weight
            for _, weight in weighted_components
        )

        base_freshness = (
            numerator / denominator
            if denominator > 0.0
            else 0.0
        )

        # A stable resource should not receive an artificial extreme
        # freshness score merely because it has a recent crawl.
        stability_dampener = (
            1.0
            - 0.20
            * self._clamp(
                signals["historical_stability"]
            )
        )

        freshness_score = self._clamp(
            base_freshness * stability_dampener
        )

        return freshness_score, signals

    # ------------------------------------------------------------------
    # Urgency
    # ------------------------------------------------------------------

    def _calculate_urgency(
        self,
        freshness_score: float,
        signals: Dict[str, float],
    ) -> float:
        volatility = max(
            signals.get("change_rate", 0.0),
            signals.get("temporal_volatility", 0.0),
            signals.get("content_volatility", 0.0),
        )

        temporal_sensitivity = max(
            signals.get("query_time_sensitivity", 0.0),
            signals.get("event_time_sensitivity", 0.0),
        )

        update_confirmation = max(
            signals.get("recent_change", 0.0),
            signals.get("feed_update_signal", 0.0),
            signals.get("sitemap_update_signal", 0.0),
            signals.get("external_update_signal", 0.0),
        )

        crawl_staleness = 1.0 - signals.get(
            "last_crawl_recency",
            0.0,
        )

        urgency = (
            0.35 * freshness_score
            + 0.25 * volatility
            + 0.20 * temporal_sensitivity
            + 0.15 * update_confirmation
            + 0.05 * crawl_staleness
        )

        return self._clamp(urgency)

    def _urgency_band(
        self,
        urgency_score: float,
    ) -> FreshnessUrgency:
        if urgency_score >= self.policy.urgency_threshold_critical:
            return FreshnessUrgency.CRITICAL

        if urgency_score >= self.policy.urgency_threshold_very_high:
            return FreshnessUrgency.VERY_HIGH

        if urgency_score >= self.policy.urgency_threshold_high:
            return FreshnessUrgency.HIGH

        if urgency_score >= self.policy.urgency_threshold_moderate:
            return FreshnessUrgency.MODERATE

        if urgency_score >= self.policy.urgency_threshold_low:
            return FreshnessUrgency.LOW

        if urgency_score > 0.0:
            return FreshnessUrgency.VERY_LOW

        return FreshnessUrgency.NONE

    # ------------------------------------------------------------------
    # Priority
    # ------------------------------------------------------------------

    def _calculate_priority(
        self,
        freshness_score: float,
        urgency_score: float,
        confidence: float,
        signals: Dict[str, float],
    ) -> float:
        """
        Calculate crawl freshness priority.

        Confidence influences the final score but does not erase strong
        freshness evidence. This allows partial distributed observations
        while preserving uncertainty.
        """

        direct_change = max(
            signals.get("recent_change", 0.0),
            signals.get("feed_update_signal", 0.0),
            signals.get("sitemap_update_signal", 0.0),
            signals.get("external_update_signal", 0.0),
        )

        temporal_need = max(
            signals.get("query_time_sensitivity", 0.0),
            signals.get("event_time_sensitivity", 0.0),
        )

        volatility = max(
            signals.get("change_rate", 0.0),
            signals.get("temporal_volatility", 0.0),
            signals.get("content_volatility", 0.0),
        )

        priority = (
            0.30 * freshness_score
            + 0.35 * urgency_score
            + 0.15 * direct_change
            + 0.10 * temporal_need
            + 0.10 * volatility
        )

        confidence_factor = (
            0.70
            + 0.30 * self._clamp(confidence)
        )

        return self._clamp(
            priority * confidence_factor
        )

    def _priority_band(
        self,
        priority_score: float,
    ) -> FreshnessPriorityBand:
        if priority_score >= self.policy.urgent_recrawl_threshold:
            return FreshnessPriorityBand.IMMEDIATE

        if priority_score >= self.policy.high_priority_threshold:
            return FreshnessPriorityBand.URGENT

        if priority_score >= self.policy.prioritize_threshold:
            return FreshnessPriorityBand.HIGH

        if priority_score >= self.policy.monitor_threshold:
            return FreshnessPriorityBand.ELEVATED

        if priority_score >= self.policy.defer_threshold:
            return FreshnessPriorityBand.NORMAL

        return FreshnessPriorityBand.BACKGROUND

    def _decision(
        self,
        priority_score: float,
    ) -> FreshnessDecision:
        if priority_score >= self.policy.urgent_recrawl_threshold:
            return FreshnessDecision.URGENT_RECRAWL

        if priority_score >= self.policy.high_priority_threshold:
            return FreshnessDecision.HIGH_PRIORITY

        if priority_score >= self.policy.prioritize_threshold:
            return FreshnessDecision.PRIORITIZE

        if priority_score >= self.policy.monitor_threshold:
            return FreshnessDecision.MONITOR

        return FreshnessDecision.DEFER

    # ------------------------------------------------------------------
    # Confidence
    # ------------------------------------------------------------------

    def _confidence(
        self,
        state: ResourceFreshnessState,
        evidence: FreshnessEvidenceBundle,
    ) -> float:
        evidence_confidence = self._clamp(
            evidence.confidence
        )

        history_confidence = self._clamp(
            min(
                1.0,
                state.observed_crawl_count / 10.0,
            )
        )

        signal_completeness = self._clamp(
            len(evidence.evidence)
            / float(
                max(
                    1,
                    self.policy.max_evidence_per_resource,
                )
            )
            * 4.0
        )

        confidence = (
            0.60 * evidence_confidence
            + 0.25 * history_confidence
            + 0.15 * signal_completeness
        )

        return self._clamp(confidence)

    # ------------------------------------------------------------------
    # Main prioritization
    # ------------------------------------------------------------------

    def prioritize(
        self,
        identity: FreshnessPriorityIdentity,
        state: ResourceFreshnessState,
        evidence: FreshnessEvidenceBundle,
    ) -> FreshnessPriorityResult:
        created_at = self._now()

        try:
            self._event(
                identity,
                FreshnessEventType.REQUEST_RECEIVED,
                FreshnessPriorityState.RECEIVED,
            )

            # ----------------------------------------------------------
            # Validation
            # ----------------------------------------------------------

            self._event(
                identity,
                FreshnessEventType.VALIDATION_STARTED,
                FreshnessPriorityState.VALIDATING,
            )

            if identity.resource_id != state.resource_id:
                raise ValueError(
                    "identity.resource_id must match state.resource_id"
                )

            if identity.resource_id != evidence.resource_id:
                raise ValueError(
                    "identity.resource_id must match evidence.resource_id"
                )

            if not identity.canonical_url:
                raise ValueError(
                    "canonical_url must not be empty"
                )

            if not identity.hostname:
                raise ValueError(
                    "hostname must not be empty"
                )

            normalized_evidence = self._normalize_evidence(
                evidence
            )

            if (
                normalized_evidence.partial
                and not self.policy.allow_partial
            ):
                raise ValueError(
                    "partial evidence is disabled by policy"
                )

            if (
                normalized_evidence.confidence
                < self.policy.minimum_confidence
                and not self.policy.allow_partial
            ):
                raise ValueError(
                    "evidence confidence is below policy minimum"
                )

            self._event(
                identity,
                FreshnessEventType.EVIDENCE_NORMALIZATION_STARTED,
                FreshnessPriorityState.EVIDENCE_NORMALIZATION,
                metadata={
                    "evidence_count": str(
                        len(normalized_evidence.evidence)
                    )
                },
            )

            self._checkpoint(
                identity,
                FreshnessCheckpointType.EVIDENCE_NORMALIZED,
                FreshnessPriorityState.EVIDENCE_NORMALIZATION,
                metadata={
                    "partial": str(
                        normalized_evidence.partial
                    )
                },
            )

            # ----------------------------------------------------------
            # Freshness estimation
            # ----------------------------------------------------------

            now = datetime.now(timezone.utc)

            freshness_score, signals = (
                self._estimate_temporal_freshness(
                    state,
                    normalized_evidence,
                    now,
                )
            )

            self._event(
                identity,
                FreshnessEventType.FRESHNESS_ESTIMATED,
                FreshnessPriorityState.FRESHNESS_ESTIMATION,
                metadata={
                    "freshness_score": f"{freshness_score:.6f}"
                },
            )

            # ----------------------------------------------------------
            # Confidence
            # ----------------------------------------------------------

            confidence = self._confidence(
                state,
                normalized_evidence,
            )

            # ----------------------------------------------------------
            # Urgency
            # ----------------------------------------------------------

            urgency_score = self._calculate_urgency(
                freshness_score,
                signals,
            )

            urgency = self._urgency_band(
                urgency_score
            )

            self._event(
                identity,
                FreshnessEventType.URGENCY_CALCULATED,
                FreshnessPriorityState.URGENCY_CALCULATION,
                metadata={
                    "urgency_score": f"{urgency_score:.6f}",
                    "urgency": urgency.value,
                },
            )

            # ----------------------------------------------------------
            # Priority
            # ----------------------------------------------------------

            priority_score = self._calculate_priority(
                freshness_score,
                urgency_score,
                confidence,
                signals,
            )

            priority_band = self._priority_band(
                priority_score
            )

            decision = self._decision(
                priority_score
            )

            self._event(
                identity,
                FreshnessEventType.PRIORITY_CALCULATED,
                FreshnessPriorityState.PRIORITY_CALCULATION,
                metadata={
                    "priority_score": f"{priority_score:.6f}"
                },
            )

            score = FreshnessPriorityScore(
                resource_id=identity.resource_id,
                freshness_score=freshness_score,
                urgency_score=urgency_score,
                priority_score=priority_score,
                confidence=confidence,
                urgency=urgency,
                priority_band=priority_band,
                decision=decision,
                partial=normalized_evidence.partial,
                contributing_signals=signals,
            )

            self._checkpoint(
                identity,
                FreshnessCheckpointType.PRIORITY_CALCULATED,
                FreshnessPriorityState.PRIORITY_CALCULATION,
                score=score,
            )

            # ----------------------------------------------------------
            # Lineage
            # ----------------------------------------------------------

            lineage = FreshnessPriorityLineage(
                source_system=(
                    "phase13.1-crawl-freshness-prioritization"
                ),
                source_version=ARCHITECTURE_VERSION,
                previous_stage=(
                    "phase12-freshness-ranking-signals"
                ),
                evidence_sources=sorted(
                    {
                        item.source
                        for item in normalized_evidence.evidence
                        if item.source
                    }
                ),
                parent_request_ids=[
                    identity.request_id
                ],
                calculation_timestamp=self._now(),
                provenance_preserved=True,
            )

            self._event(
                identity,
                FreshnessEventType.DECISION_CREATED,
                FreshnessPriorityState.DECISION,
                metadata={
                    "decision": decision.value,
                    "priority_band": priority_band.value,
                },
            )

            self._checkpoint(
                identity,
                FreshnessCheckpointType.DECISION_CREATED,
                FreshnessPriorityState.DECISION,
                score=score,
            )

            # ----------------------------------------------------------
            # Final result
            # ----------------------------------------------------------

            result = FreshnessPriorityResult(
                identity=identity,
                lineage=lineage,
                state=(
                    FreshnessPriorityState.PARTIAL
                    if normalized_evidence.partial
                    else FreshnessPriorityState.COMPLETED
                ),
                score=score,
                created_at=created_at,
                completed_at=self._now(),
            )

            self.backend.persist_result(result)

            self._checkpoint(
                identity,
                FreshnessCheckpointType.COMPLETED,
                result.state,
                score=score,
            )

            self._event(
                identity,
                FreshnessEventType.PRIORITIZATION_COMPLETED,
                result.state,
                metadata={
                    "decision": decision.value,
                    "priority_score": f"{priority_score:.6f}",
                },
            )

            return result

        except Exception as exc:
            self._event(
                identity,
                FreshnessEventType.PRIORITIZATION_FAILED,
                FreshnessPriorityState.FAILED,
                metadata={
                    "error": str(exc)
                },
            )

            lineage = FreshnessPriorityLineage(
                calculation_timestamp=self._now(),
                provenance_preserved=True,
            )

            failure_score = FreshnessPriorityScore(
                resource_id=identity.resource_id,
                freshness_score=0.0,
                urgency_score=0.0,
                priority_score=0.0,
                confidence=0.0,
                urgency=FreshnessUrgency.NONE,
                priority_band=FreshnessPriorityBand.BACKGROUND,
                decision=FreshnessDecision.DEFER,
                partial=True,
            )

            result = FreshnessPriorityResult(
                identity=identity,
                lineage=lineage,
                state=FreshnessPriorityState.FAILED,
                score=failure_score,
                created_at=created_at,
                completed_at=self._now(),
                error=str(exc),
            )

            self.backend.persist_result(result)

            return result

    # ------------------------------------------------------------------
    # Batch prioritization
    # ------------------------------------------------------------------

    def prioritize_many(
        self,
        requests: Sequence[
            Tuple[
                FreshnessPriorityIdentity,
                ResourceFreshnessState,
                FreshnessEvidenceBundle,
            ]
        ],
    ) -> List[FreshnessPriorityResult]:
        if len(requests) > self.policy.max_resources_per_batch:
            raise ValueError(
                "batch exceeds per-request resource limit"
            )

        results: List[FreshnessPriorityResult] = []

        for identity, state, evidence in requests:
            results.append(
                self.prioritize(
                    identity,
                    state,
                    evidence,
                )
            )

        return results

    # ------------------------------------------------------------------
    # Architecture description
    # ------------------------------------------------------------------

    def architecture(self) -> Dict[str, object]:
        return {
            "phase": PHASE,
            "stage": "13.1",
            "name": "Crawl Freshness Prioritization",
            "version": ARCHITECTURE_VERSION,
            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": (
                GOOGLE_SCALE_CAPABILITY_TARGET
            ),
            "google_technology_dependency": (
                GOOGLE_TECHNOLOGY_DEPENDENCY
            ),
            "purpose": (
                "Prioritize crawl and future recrawl work using "
                "freshness evidence across enormous public-Web scale."
            ),
            "inputs": [
                "resource temporal state",
                "crawl history",
                "change evidence",
                "source freshness evidence",
                "temporal volatility",
                "query time sensitivity",
                "event time sensitivity",
                "feed update signals",
                "sitemap update signals",
                "external update signals",
            ],
            "outputs": [
                "freshness score",
                "urgency score",
                "priority score",
                "freshness urgency",
                "priority band",
                "freshness decision",
                "confidence",
                "contributing signals",
                "lineage",
                "checkpoint state",
            ],
            "pipeline": [
                "resource state ingestion",
                "freshness evidence ingestion",
                "evidence normalization",
                "temporal freshness estimation",
                "urgency calculation",
                "priority calculation",
                "decision generation",
                "checkpoint persistence",
                "result persistence",
            ],
            "distributed_execution": True,
            "partition_local_computation": True,
            "horizontally_scalable": True,
            "deterministic": True,
            "checkpointable": True,
            "restartable": True,
            "partial_input_supported": (
                self.policy.allow_partial
            ),
            "provenance_preserved": True,
            "backend_replaceable": True,
            "incremental_evidence_supported": True,
            "resource_partitionable": True,
            "time_aware": True,
            "no_global_resource_ceiling": True,
            "no_global_document_ceiling": True,
            "no_global_url_ceiling": True,
            "no_global_partition_ceiling": True,
            "no_google_api_dependency": True,
            "no_google_index_dependency": True,
            "no_google_crawler_dependency": True,
            "no_google_infrastructure_dependency": True,
            "no_google_ranking_dependency": True,
            "does_not_execute_crawl": True,
            "does_not_execute_recrawl": True,
            "does_not_mutate_search_index": True,
            "does_not_perform_final_search_ranking": True,
            "next_stage": NEXT_STAGE,
            "next_stage_name": (
                "Change Detection / Content Change Signals"
            ),
        }


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------


CrawlFreshnessPrioritization = (
    CrawlFreshnessPrioritizationArchitecture
)

GlobalCrawlFreshnessPrioritization = (
    CrawlFreshnessPrioritizationArchitecture
)

Phase13_1CrawlFreshnessPrioritization = (
    CrawlFreshnessPrioritizationArchitecture
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
    "NEXT_STAGE",

    "FreshnessPriorityState",
    "FreshnessEvidenceType",
    "FreshnessUrgency",
    "FreshnessPriorityBand",
    "FreshnessDecision",
    "FreshnessEventType",
    "FreshnessCheckpointType",

    "FreshnessPriorityIdentity",
    "FreshnessPriorityLineage",
    "FreshnessEvidence",
    "FreshnessEvidenceBundle",
    "ResourceFreshnessState",

    "CrawlFreshnessPrioritizationPolicy",

    "FreshnessPriorityScore",
    "FreshnessPriorityResult",
    "FreshnessPriorityCheckpoint",
    "FreshnessPriorityEvent",

    "CrawlFreshnessPrioritizationBackend",
    "InMemoryCrawlFreshnessPrioritizationMetadata",

    "CrawlFreshnessPrioritizationArchitecture",
    "CrawlFreshnessPrioritization",
    "GlobalCrawlFreshnessPrioritization",
    "Phase13_1CrawlFreshnessPrioritization",
]
