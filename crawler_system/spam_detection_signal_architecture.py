"""
OUR SEARCH
Phase 14.1 — Spam Detection Signal Architecture

Purpose
-------
Define the production-grade signal and evidence architecture used to detect
potential spam, manipulation, abuse, and low-integrity Web resources at
enormous scale.

This stage is a SIGNAL ARCHITECTURE.

It collects, normalizes, aggregates, and preserves independent evidence that
downstream spam/abuse decision systems can consume.

It does NOT:
- make the final spam decision
- permanently classify a resource as spam
- remove documents from the index
- mutate the search index
- execute crawling
- perform HTTP requests
- assign crawler workers
- perform final ranking
- replace retrieval
- enforce penalties
- quarantine resources directly
- delete Web resources
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
- resources
- URLs
- documents
- hosts
- domains
- partitions
- shards
- workers
- signals
- evidence records
- observations
- histories

Per-request and per-batch safety limits exist only to bound individual
processing units and protect execution resources.

Design principles
-----------------
1. Independent evidence rather than one opaque spam score.
2. Multi-dimensional signals.
3. Temporal and historical evidence.
4. Host/domain/document/URL level analysis.
5. Distributed and partition-aware computation.
6. Partial-input tolerance.
7. Deterministic normalization where possible.
8. Provenance and lineage preservation.
9. Checkpointable processing.
10. Backend replacement without changing the architecture contract.
11. Separation from final enforcement and ranking.
12. No dependency on Google technology.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
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

ARCHITECTURE_VERSION = "spam-detection-signal-architecture.v1"
PHASE = "14.1"
PREVIOUS_STAGE = "13.9"
NEXT_STAGE = "14.2"

PHASE_NAME = "Spam Detection Signal Architecture"
NEXT_STAGE_NAME = "Content Quality & Manipulation Detection"


# ============================================================================
# ENUMERATIONS
# ============================================================================


class SpamSignalState(str, Enum):
    RECEIVED = "received"
    VALIDATING = "validating"
    NORMALIZING = "normalizing"
    SIGNAL_EXTRACTION = "signal_extraction"
    EVIDENCE_AGGREGATION = "evidence_aggregation"
    CONFIDENCE_CALCULATION = "confidence_calculation"
    TEMPORAL_ANALYSIS = "temporal_analysis"
    RELATIONSHIP_ANALYSIS = "relationship_analysis"
    DECISION_READY = "decision_ready"
    COMPLETED = "completed"
    PARTIAL = "partial"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"


class SpamSignalFamily(str, Enum):
    CONTENT = "content"
    CONTENT_REPETITION = "content_repetition"
    CONTENT_GENERATION_PATTERN = "content_generation_pattern"
    KEYWORD_MANIPULATION = "keyword_manipulation"
    TEXT_PATTERN = "text_pattern"
    TITLE_MANIPULATION = "title_manipulation"
    METADATA_MANIPULATION = "metadata_manipulation"
    LINK_MANIPULATION = "link_manipulation"
    LINK_GRAPH = "link_graph"
    ANCHOR_MANIPULATION = "anchor_manipulation"
    DOMAIN_BEHAVIOR = "domain_behavior"
    HOST_BEHAVIOR = "host_behavior"
    URL_BEHAVIOR = "url_behavior"
    DOCUMENT_BEHAVIOR = "document_behavior"
    REDIRECT_BEHAVIOR = "redirect_behavior"
    CLOAKING = "cloaking"
    DOORWAY_PATTERN = "doorway_pattern"
    DUPLICATION = "duplication"
    TEMPLATE_REPETITION = "template_repetition"
    COMMERCIAL_MANIPULATION = "commercial_manipulation"
    TRAFFIC_PATTERN = "traffic_pattern"
    CRAWL_BEHAVIOR = "crawl_behavior"
    INDEXING_BEHAVIOR = "indexing_behavior"
    RESOURCE_BEHAVIOR = "resource_behavior"
    TEMPORAL = "temporal"
    HISTORICAL = "historical"
    SOURCE_REPUTATION = "source_reputation"
    CROSS_RESOURCE = "cross_resource"
    CROSS_DOMAIN = "cross_domain"
    IDENTITY = "identity"
    TECHNICAL = "technical"
    SECURITY_RELATED = "security_related"


class SpamSignalType(str, Enum):
    KEYWORD_STUFFING = "keyword_stuffing"
    EXCESSIVE_REPETITION = "excessive_repetition"
    UNNATURAL_TEXT = "unnatural_text"
    LOW_INFORMATION_CONTENT = "low_information_content"
    TEMPLATE_DUPLICATION = "template_duplication"
    NEAR_DUPLICATE_CONTENT = "near_duplicate_content"
    MASS_DUPLICATE_CONTENT = "mass_duplicate_content"

    TITLE_KEYWORD_STUFFING = "title_keyword_stuffing"
    MISLEADING_TITLE = "misleading_title"
    METADATA_KEYWORD_STUFFING = "metadata_keyword_stuffing"

    EXCESSIVE_INTERNAL_LINKING = "excessive_internal_linking"
    UNNATURAL_OUTBOUND_LINKING = "unnatural_outbound_linking"
    LINK_FARM_PATTERN = "link_farm_pattern"
    RECIPROCAL_LINK_PATTERN = "reciprocal_link_pattern"
    ANCHOR_TEXT_MANIPULATION = "anchor_text_manipulation"
    UNNATURAL_ANCHOR_DIVERSITY = "unnatural_anchor_diversity"

    DOORWAY_PAGE_PATTERN = "doorway_page_pattern"
    REDIRECT_CHAIN_PATTERN = "redirect_chain_pattern"
    SUSPICIOUS_REDIRECT = "suspicious_redirect"
    CLOAKING_PATTERN = "cloaking_pattern"

    RAPID_URL_GENERATION = "rapid_url_generation"
    MASS_DOCUMENT_GENERATION = "mass_document_generation"
    MASS_HOST_GENERATION = "mass_host_generation"
    MASS_DOMAIN_GENERATION = "mass_domain_generation"

    ABNORMAL_CRAWL_PATTERN = "abnormal_crawl_pattern"
    ABNORMAL_INDEXING_PATTERN = "abnormal_indexing_pattern"

    CONTENT_CHANGE_ANOMALY = "content_change_anomaly"
    RESOURCE_CHANGE_ANOMALY = "resource_change_anomaly"

    HISTORICAL_SPAM_ASSOCIATION = "historical_spam_association"
    REPEATED_POLICY_VIOLATION = "repeated_policy_violation"

    DOMAIN_REPUTATION_ANOMALY = "domain_reputation_anomaly"
    HOST_REPUTATION_ANOMALY = "host_reputation_anomaly"
    RESOURCE_REPUTATION_ANOMALY = "resource_reputation_anomaly"

    CROSS_DOMAIN_CONTENT_REUSE = "cross_domain_content_reuse"
    CROSS_DOMAIN_LINK_PATTERN = "cross_domain_link_pattern"
    SHARED_MANIPULATION_PATTERN = "shared_manipulation_pattern"

    IDENTITY_INCONSISTENCY = "identity_inconsistency"
    TECHNICAL_MANIPULATION = "technical_manipulation"
    SECURITY_RELATED_ANOMALY = "security_related_anomaly"


class SpamEvidenceStrength(str, Enum):
    NONE = "none"
    VERY_WEAK = "very_weak"
    WEAK = "weak"
    MODERATE = "moderate"
    STRONG = "strong"
    VERY_STRONG = "very_strong"


class SpamEvidenceDirection(str, Enum):
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    POSITIVE = "positive"


class SpamScope(str, Enum):
    URL = "url"
    DOCUMENT = "document"
    HOST = "host"
    DOMAIN = "domain"
    SUBDOMAIN = "subdomain"
    PARTITION = "partition"
    SHARD = "shard"
    RESOURCE_GROUP = "resource_group"
    GLOBAL = "global"


class SpamTemporalState(str, Enum):
    UNKNOWN = "unknown"
    NEW = "new"
    STABLE = "stable"
    EMERGING = "emerging"
    PERSISTENT = "persistent"
    DECLINING = "declining"
    RECENT_SPIKE = "recent_spike"


class SpamSignalConfidenceBand(str, Enum):
    UNKNOWN = "unknown"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"


class SpamSignalEventType(str, Enum):
    REQUEST_RECEIVED = "request_received"
    VALIDATION_STARTED = "validation_started"
    INPUT_NORMALIZED = "input_normalized"
    SIGNAL_EXTRACTION_STARTED = "signal_extraction_started"
    SIGNALS_EXTRACTED = "signals_extracted"
    EVIDENCE_AGGREGATED = "evidence_aggregated"
    TEMPORAL_ANALYSIS_STARTED = "temporal_analysis_started"
    TEMPORAL_ANALYSIS_COMPLETED = "temporal_analysis_completed"
    RELATIONSHIP_ANALYSIS_STARTED = "relationship_analysis_started"
    RELATIONSHIP_ANALYSIS_COMPLETED = "relationship_analysis_completed"
    CONFIDENCE_CALCULATION_STARTED = "confidence_calculation_started"
    CONFIDENCE_CALCULATED = "confidence_calculated"
    PARTIAL_INPUT_DETECTED = "partial_input_detected"
    CHECKPOINT_CREATED = "checkpoint_created"
    SIGNALS_COMPLETED = "signals_completed"
    SIGNALS_DEFERRED = "signals_deferred"
    SIGNALS_REJECTED = "signals_rejected"
    SIGNALS_FAILED = "signals_failed"


class SpamSignalCheckpointType(str, Enum):
    INPUT_ACCEPTED = "input_accepted"
    INPUT_NORMALIZED = "input_normalized"
    SIGNALS_EXTRACTED = "signals_extracted"
    EVIDENCE_AGGREGATED = "evidence_aggregated"
    TEMPORAL_ANALYZED = "temporal_analyzed"
    RELATIONSHIPS_ANALYZED = "relationships_analyzed"
    CONFIDENCE_CALCULATED = "confidence_calculated"
    COMPLETED = "completed"


# ============================================================================
# IDENTITY / LINEAGE
# ============================================================================


@dataclass(frozen=True)
class SpamSignalIdentity:
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
                self.host_id,
                self.domain_id,
                self.partition_id,
                self.shard_id,
                self.region_id,
            ]
        )


@dataclass
class SpamSignalLineage:
    resource_id: str
    previous_stage: str = PREVIOUS_STAGE
    current_stage: str = PHASE

    source_document_version: str = ""
    source_crawl_version: str = ""
    source_index_version: str = ""
    source_freshness_version: str = ""
    source_quality_version: str = ""

    parent_resource_ids: List[str] = field(default_factory=list)
    parent_signal_ids: List[str] = field(default_factory=list)

    lineage_metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# RAW OBSERVATION / HISTORY
# ============================================================================


@dataclass
class SpamResourceHistory:
    observation_count: int = 0

    previous_signal_score: float = 0.0
    average_signal_score: float = 0.0
    maximum_signal_score: float = 0.0
    minimum_signal_score: float = 0.0

    previous_confidence: float = 0.0
    average_confidence: float = 0.0

    suspicious_observation_count: int = 0
    high_risk_observation_count: int = 0

    recent_spike_count: int = 0
    persistent_pattern_count: int = 0

    first_observed_timestamp: str = ""
    last_observed_timestamp: str = ""
    last_change_timestamp: str = ""

    historical_stability: float = 0.0
    historical_volatility: float = 0.0

    previous_signal_version: str = ""
    partial: bool = False


@dataclass
class SpamSignalObservation:
    signal_id: str
    signal_type: SpamSignalType
    family: SpamSignalFamily

    scope: SpamScope

    value: float = 0.0
    confidence: float = 0.0

    evidence_strength: SpamEvidenceStrength = SpamEvidenceStrength.NONE
    direction: SpamEvidenceDirection = SpamEvidenceDirection.NEUTRAL

    observation_count: int = 1

    observed_at: str = ""

    source_id: str = ""
    source_version: str = ""

    feature_metadata: Dict[str, Any] = field(default_factory=dict)

    partial: bool = False


# ============================================================================
# AGGREGATED EVIDENCE
# ============================================================================


@dataclass
class SpamEvidenceBundle:
    family: SpamSignalFamily

    normalized_score: float = 0.0
    confidence: float = 0.0

    observation_count: int = 0
    supporting_signal_count: int = 0

    strongest_signal_type: Optional[SpamSignalType] = None
    strongest_signal_value: float = 0.0

    positive_evidence: float = 0.0
    negative_evidence: float = 0.0

    temporal_state: SpamTemporalState = SpamTemporalState.UNKNOWN

    partial: bool = False


@dataclass
class SpamTemporalEvidence:
    recent_activity: float = 0.0
    recent_change: float = 0.0
    acceleration: float = 0.0
    persistence: float = 0.0
    volatility: float = 0.0
    stability: float = 0.0

    temporal_state: SpamTemporalState = SpamTemporalState.UNKNOWN

    confidence: float = 0.0
    partial: bool = False


@dataclass
class SpamRelationshipEvidence:
    related_resource_count: int = 0
    related_host_count: int = 0
    related_domain_count: int = 0

    shared_content_score: float = 0.0
    shared_link_pattern_score: float = 0.0
    shared_manipulation_score: float = 0.0

    network_concentration_score: float = 0.0
    network_diversity_score: float = 0.0

    confidence: float = 0.0
    partial: bool = False


# ============================================================================
# POLICY
# ============================================================================


@dataclass
class SpamDetectionSignalPolicy:
    max_observations_per_resource: int = 512
    max_related_resources: int = 100000
    max_related_domains: int = 100000
    max_related_hosts: int = 100000
    max_families: int = 64
    max_signals_per_family: int = 128

    minimum_confidence: float = 0.10

    allow_partial: bool = True
    deterministic: bool = True
    checkpoint_enabled: bool = True

    temporal_weight: float = 1.00
    historical_weight: float = 1.15
    relationship_weight: float = 1.10
    content_weight: float = 1.00
    link_weight: float = 1.10
    behavior_weight: float = 1.05
    manipulation_weight: float = 1.20
    reputation_weight: float = 0.90
    technical_weight: float = 0.75
    security_weight: float = 1.00

    partial_penalty: float = 0.05

    weak_threshold: float = 0.15
    moderate_threshold: float = 0.40
    strong_threshold: float = 0.70
    very_strong_threshold: float = 0.90


# ============================================================================
# INPUT
# ============================================================================


@dataclass
class SpamDetectionSignalInput:
    identity: SpamSignalIdentity

    observations: List[SpamSignalObservation] = field(default_factory=list)

    history: Optional[SpamResourceHistory] = None

    temporal_evidence: Optional[SpamTemporalEvidence] = None

    relationship_evidence: Optional[SpamRelationshipEvidence] = None

    evidence_sources: List[str] = field(default_factory=list)

    lineage: Optional[SpamSignalLineage] = None

    signal_version: str = ARCHITECTURE_VERSION

    confidence: float = 0.0
    partial: bool = False

    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# RESULT
# ============================================================================


@dataclass
class SpamDetectionSignalResult:
    result_id: str
    identity: SpamSignalIdentity

    state: SpamSignalState

    observations: List[SpamSignalObservation] = field(default_factory=list)

    evidence_bundles: List[SpamEvidenceBundle] = field(default_factory=list)

    temporal_evidence: Optional[SpamTemporalEvidence] = None

    relationship_evidence: Optional[SpamRelationshipEvidence] = None

    aggregate_signal_score: float = 0.0
    aggregate_confidence: float = 0.0

    confidence_band: SpamSignalConfidenceBand = SpamSignalConfidenceBand.UNKNOWN

    strongest_family: Optional[SpamSignalFamily] = None
    strongest_signal_type: Optional[SpamSignalType] = None

    partial: bool = False

    lineage: Optional[SpamSignalLineage] = None

    evidence_sources: List[str] = field(default_factory=list)

    created_at: str = ""

    architecture_version: str = ARCHITECTURE_VERSION


@dataclass
class SpamDetectionSignalCheckpoint:
    checkpoint_id: str

    identity: SpamSignalIdentity

    checkpoint_type: SpamSignalCheckpointType

    state: SpamSignalState

    observation_count: int = 0
    family_count: int = 0

    aggregate_score: float = 0.0
    confidence: float = 0.0

    partial: bool = False

    created_at: str = ""

    architecture_version: str = ARCHITECTURE_VERSION


@dataclass
class SpamDetectionSignalEvent:
    event_id: str

    identity: SpamSignalIdentity

    event_type: SpamSignalEventType

    state: SpamSignalState

    timestamp: str

    metadata: Dict[str, Any] = field(default_factory=dict)

    architecture_version: str = ARCHITECTURE_VERSION


# ============================================================================
# BACKEND
# ============================================================================


class SpamDetectionSignalBackend(Protocol):
    def persist_event(self, event: SpamDetectionSignalEvent) -> None:
        ...

    def persist_checkpoint(
        self,
        checkpoint: SpamDetectionSignalCheckpoint,
    ) -> None:
        ...

    def persist_result(
        self,
        result: SpamDetectionSignalResult,
    ) -> None:
        ...

    def get_result(
        self,
        result_id: str,
    ) -> Optional[SpamDetectionSignalResult]:
        ...


class InMemorySpamDetectionSignalMetadata:
    """
    Reference metadata backend.

    This backend exists only as a replaceable architecture reference.
    Production deployments can replace it with distributed durable storage.
    """

    def __init__(self) -> None:
        self._events: Dict[str, SpamDetectionSignalEvent] = {}
        self._checkpoints: Dict[str, SpamDetectionSignalCheckpoint] = {}
        self._results: Dict[str, SpamDetectionSignalResult] = {}

    def persist_event(self, event: SpamDetectionSignalEvent) -> None:
        self._events[event.event_id] = event

    def persist_checkpoint(
        self,
        checkpoint: SpamDetectionSignalCheckpoint,
    ) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint

    def persist_result(
        self,
        result: SpamDetectionSignalResult,
    ) -> None:
        self._results[result.result_id] = result

    def get_result(
        self,
        result_id: str,
    ) -> Optional[SpamDetectionSignalResult]:
        return self._results.get(result_id)

    def events(self) -> List[SpamDetectionSignalEvent]:
        return list(self._events.values())

    def checkpoints(self) -> List[SpamDetectionSignalCheckpoint]:
        return list(self._checkpoints.values())

    def results(self) -> List[SpamDetectionSignalResult]:
        return list(self._results.values())


# ============================================================================
# ARCHITECTURE
# ============================================================================


class SpamDetectionSignalArchitecture:
    """
    Phase 14.1 production architecture.

    This class produces spam/manipulation evidence.

    It intentionally does NOT make the final enforcement decision.
    """

    def __init__(
        self,
        backend: Optional[SpamDetectionSignalBackend] = None,
        policy: Optional[SpamDetectionSignalPolicy] = None,
    ) -> None:
        self.backend = backend or InMemorySpamDetectionSignalMetadata()
        self.policy = policy or SpamDetectionSignalPolicy()

    # ------------------------------------------------------------------------
    # BASIC UTILITIES
    # ------------------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
        try:
            value = float(value)
        except (TypeError, ValueError):
            return minimum

        if math.isnan(value) or math.isinf(value):
            return minimum

        return max(minimum, min(maximum, value))

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _deterministic_unit(value: str) -> float:
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
        integer = int(digest[:16], 16)
        return integer / float(0xFFFFFFFFFFFFFFFF)

    @staticmethod
    def _digest_payload(payload: str) -> str:
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------------
    # EVENT / CHECKPOINT HELPERS
    # ------------------------------------------------------------------------

    def _event(
        self,
        identity: SpamSignalIdentity,
        event_type: SpamSignalEventType,
        state: SpamSignalState,
        **metadata: Any,
    ) -> SpamDetectionSignalEvent:

        event_id = self._digest_payload(
            "|".join(
                [
                    identity.key(),
                    event_type.value,
                    state.value,
                    self._now(),
                ]
            )
        )

        event = SpamDetectionSignalEvent(
            event_id=event_id,
            identity=identity,
            event_type=event_type,
            state=state,
            timestamp=self._now(),
            metadata=metadata,
        )

        self.backend.persist_event(event)

        return event

    def _checkpoint(
        self,
        identity: SpamSignalIdentity,
        checkpoint_type: SpamSignalCheckpointType,
        state: SpamSignalState,
        observation_count: int,
        family_count: int,
        aggregate_score: float,
        confidence: float,
        partial: bool,
    ) -> SpamDetectionSignalCheckpoint:

        checkpoint_id = self._digest_payload(
            "|".join(
                [
                    identity.key(),
                    checkpoint_type.value,
                    str(observation_count),
                    str(family_count),
                    ARCHITECTURE_VERSION,
                ]
            )
        )

        checkpoint = SpamDetectionSignalCheckpoint(
            checkpoint_id=checkpoint_id,
            identity=identity,
            checkpoint_type=checkpoint_type,
            state=state,
            observation_count=observation_count,
            family_count=family_count,
            aggregate_score=self._clamp(aggregate_score),
            confidence=self._clamp(confidence),
            partial=partial,
            created_at=self._now(),
        )

        if self.policy.checkpoint_enabled:
            self.backend.persist_checkpoint(checkpoint)

        return checkpoint

    # ------------------------------------------------------------------------
    # NORMALIZATION
    # ------------------------------------------------------------------------

    def _normalize_observation(
        self,
        observation: SpamSignalObservation,
    ) -> SpamSignalObservation:

        return SpamSignalObservation(
            signal_id=observation.signal_id.strip(),
            signal_type=observation.signal_type,
            family=observation.family,
            scope=observation.scope,
            value=self._clamp(observation.value),
            confidence=self._clamp(observation.confidence),
            evidence_strength=self._strength(observation.value),
            direction=observation.direction,
            observation_count=max(1, self._safe_int(observation.observation_count, 1)),
            observed_at=observation.observed_at or self._now(),
            source_id=observation.source_id,
            source_version=observation.source_version,
            feature_metadata=dict(observation.feature_metadata),
            partial=bool(observation.partial),
        )

    def _normalize_observations(
        self,
        observations: Sequence[SpamSignalObservation],
    ) -> List[SpamSignalObservation]:

        normalized: List[SpamSignalObservation] = []

        for observation in observations[: self.policy.max_observations_per_resource]:
            normalized.append(self._normalize_observation(observation))

        return normalized

    def _normalize_history(
        self,
        history: Optional[SpamResourceHistory],
    ) -> SpamResourceHistory:

        if history is None:
            return SpamResourceHistory(partial=True)

        return SpamResourceHistory(
            observation_count=max(0, self._safe_int(history.observation_count)),
            previous_signal_score=self._clamp(history.previous_signal_score),
            average_signal_score=self._clamp(history.average_signal_score),
            maximum_signal_score=self._clamp(history.maximum_signal_score),
            minimum_signal_score=self._clamp(history.minimum_signal_score),
            previous_confidence=self._clamp(history.previous_confidence),
            average_confidence=self._clamp(history.average_confidence),
            suspicious_observation_count=max(
                0,
                self._safe_int(history.suspicious_observation_count),
            ),
            high_risk_observation_count=max(
                0,
                self._safe_int(history.high_risk_observation_count),
            ),
            recent_spike_count=max(
                0,
                self._safe_int(history.recent_spike_count),
            ),
            persistent_pattern_count=max(
                0,
                self._safe_int(history.persistent_pattern_count),
            ),
            first_observed_timestamp=history.first_observed_timestamp,
            last_observed_timestamp=history.last_observed_timestamp,
            last_change_timestamp=history.last_change_timestamp,
            historical_stability=self._clamp(history.historical_stability),
            historical_volatility=self._clamp(history.historical_volatility),
            previous_signal_version=history.previous_signal_version,
            partial=bool(history.partial),
        )

    def _normalize_temporal(
        self,
        temporal: Optional[SpamTemporalEvidence],
    ) -> SpamTemporalEvidence:

        if temporal is None:
            return SpamTemporalEvidence(partial=True)

        return SpamTemporalEvidence(
            recent_activity=self._clamp(temporal.recent_activity),
            recent_change=self._clamp(temporal.recent_change),
            acceleration=self._clamp(temporal.acceleration),
            persistence=self._clamp(temporal.persistence),
            volatility=self._clamp(temporal.volatility),
            stability=self._clamp(temporal.stability),
            temporal_state=temporal.temporal_state,
            confidence=self._clamp(temporal.confidence),
            partial=bool(temporal.partial),
        )

    def _normalize_relationships(
        self,
        relationship: Optional[SpamRelationshipEvidence],
    ) -> SpamRelationshipEvidence:

        if relationship is None:
            return SpamRelationshipEvidence(partial=True)

        return SpamRelationshipEvidence(
            related_resource_count=max(
                0,
                self._safe_int(relationship.related_resource_count),
            ),
            related_host_count=max(
                0,
                self._safe_int(relationship.related_host_count),
            ),
            related_domain_count=max(
                0,
                self._safe_int(relationship.related_domain_count),
            ),
            shared_content_score=self._clamp(
                relationship.shared_content_score
            ),
            shared_link_pattern_score=self._clamp(
                relationship.shared_link_pattern_score
            ),
            shared_manipulation_score=self._clamp(
                relationship.shared_manipulation_score
            ),
            network_concentration_score=self._clamp(
                relationship.network_concentration_score
            ),
            network_diversity_score=self._clamp(
                relationship.network_diversity_score
            ),
            confidence=self._clamp(relationship.confidence),
            partial=bool(relationship.partial),
        )

    # ------------------------------------------------------------------------
    # STRENGTH / CONFIDENCE
    # ------------------------------------------------------------------------

    def _strength(self, value: float) -> SpamEvidenceStrength:
        value = self._clamp(value)

        if value >= self.policy.very_strong_threshold:
            return SpamEvidenceStrength.VERY_STRONG

        if value >= self.policy.strong_threshold:
            return SpamEvidenceStrength.STRONG

        if value >= self.policy.moderate_threshold:
            return SpamEvidenceStrength.MODERATE

        if value >= self.policy.weak_threshold:
            return SpamEvidenceStrength.WEAK

        if value > 0.0:
            return SpamEvidenceStrength.VERY_WEAK

        return SpamEvidenceStrength.NONE

    def _confidence_band(
        self,
        confidence: float,
    ) -> SpamSignalConfidenceBand:

        confidence = self._clamp(confidence)

        if confidence >= 0.90:
            return SpamSignalConfidenceBand.VERY_HIGH

        if confidence >= 0.70:
            return SpamSignalConfidenceBand.HIGH

        if confidence >= 0.40:
            return SpamSignalConfidenceBand.MODERATE

        if confidence > 0.0:
            return SpamSignalConfidenceBand.LOW

        return SpamSignalConfidenceBand.UNKNOWN

    # ------------------------------------------------------------------------
    # FAMILY WEIGHTS
    # ------------------------------------------------------------------------

    def _family_weight(
        self,
        family: SpamSignalFamily,
    ) -> float:

        if family in {
            SpamSignalFamily.CONTENT,
            SpamSignalFamily.CONTENT_REPETITION,
            SpamSignalFamily.CONTENT_GENERATION_PATTERN,
            SpamSignalFamily.TEXT_PATTERN,
            SpamSignalFamily.DUPLICATION,
            SpamSignalFamily.TEMPLATE_REPETITION,
        }:
            return self.policy.content_weight

        if family in {
            SpamSignalFamily.KEYWORD_MANIPULATION,
            SpamSignalFamily.TITLE_MANIPULATION,
            SpamSignalFamily.METADATA_MANIPULATION,
            SpamSignalFamily.ANCHOR_MANIPULATION,
            SpamSignalFamily.DOORWAY_PATTERN,
            SpamSignalFamily.CLOAKING,
            SpamSignalFamily.REDIRECT_BEHAVIOR,
            SpamSignalFamily.COMMERCIAL_MANIPULATION,
        }:
            return self.policy.manipulation_weight

        if family in {
            SpamSignalFamily.LINK_MANIPULATION,
            SpamSignalFamily.LINK_GRAPH,
            SpamSignalFamily.ANCHOR_MANIPULATION,
            SpamSignalFamily.CROSS_DOMAIN,
        }:
            return self.policy.link_weight

        if family in {
            SpamSignalFamily.DOMAIN_BEHAVIOR,
            SpamSignalFamily.HOST_BEHAVIOR,
            SpamSignalFamily.URL_BEHAVIOR,
            SpamSignalFamily.DOCUMENT_BEHAVIOR,
            SpamSignalFamily.CRAWL_BEHAVIOR,
            SpamSignalFamily.INDEXING_BEHAVIOR,
            SpamSignalFamily.RESOURCE_BEHAVIOR,
        }:
            return self.policy.behavior_weight

        if family in {
            SpamSignalFamily.TEMPORAL,
            SpamSignalFamily.HISTORICAL,
        }:
            return self.policy.temporal_weight

        if family in {
            SpamSignalFamily.SOURCE_REPUTATION,
            SpamSignalFamily.IDENTITY,
        }:
            return self.policy.reputation_weight

        if family == SpamSignalFamily.TECHNICAL:
            return self.policy.technical_weight

        if family == SpamSignalFamily.SECURITY_RELATED:
            return self.policy.security_weight

        if family in {
            SpamSignalFamily.CROSS_RESOURCE,
            SpamSignalFamily.CROSS_DOMAIN,
        }:
            return self.policy.relationship_weight

        return 1.0

    # ------------------------------------------------------------------------
    # FAMILY AGGREGATION
    # ------------------------------------------------------------------------

    def _aggregate_family(
        self,
        family: SpamSignalFamily,
        observations: Sequence[SpamSignalObservation],
        history: SpamResourceHistory,
    ) -> SpamEvidenceBundle:

        family_observations = [
            observation
            for observation in observations
            if observation.family == family
        ]

        if not family_observations:
            return SpamEvidenceBundle(
                family=family,
                partial=True,
            )

        weighted_values: List[float] = []
        confidence_values: List[float] = []

        positive = 0.0
        negative = 0.0

        strongest: Optional[SpamSignalObservation] = None

        for observation in family_observations[
            : self.policy.max_signals_per_family
        ]:
            weight = max(0.01, self._family_weight(observation.family))

            weighted_values.append(
                self._clamp(observation.value) * weight
            )

            confidence_values.append(
                self._clamp(observation.confidence)
            )

            if observation.direction == SpamEvidenceDirection.POSITIVE:
                positive += self._clamp(observation.value)

            elif observation.direction == SpamEvidenceDirection.NEGATIVE:
                negative += self._clamp(observation.value)

            if strongest is None:
                strongest = observation

            elif observation.value > strongest.value:
                strongest = observation

        denominator = sum(
            max(0.01, self._family_weight(family))
            for _ in family_observations
        )

        normalized_score = (
            sum(weighted_values) / denominator
            if denominator > 0.0
            else 0.0
        )

        confidence = (
            sum(confidence_values) / len(confidence_values)
            if confidence_values
            else 0.0
        )

        historical_boost = self._clamp(
            (
                history.historical_volatility * 0.20
                + history.persistent_pattern_count * 0.01
            )
        )

        normalized_score = self._clamp(
            normalized_score + historical_boost
        )

        temporal_state = SpamTemporalState.UNKNOWN

        if history.persistent_pattern_count > 0:
            temporal_state = SpamTemporalState.PERSISTENT
        elif history.recent_spike_count > 0:
            temporal_state = SpamTemporalState.RECENT_SPIKE
        elif history.observation_count > 0:
            temporal_state = SpamTemporalState.STABLE

        partial = any(
            observation.partial
            for observation in family_observations
        )

        return SpamEvidenceBundle(
            family=family,
            normalized_score=normalized_score,
            confidence=confidence,
            observation_count=sum(
                observation.observation_count
                for observation in family_observations
            ),
            supporting_signal_count=len(family_observations),
            strongest_signal_type=(
                strongest.signal_type if strongest else None
            ),
            strongest_signal_value=(
                self._clamp(strongest.value)
                if strongest
                else 0.0
            ),
            positive_evidence=self._clamp(positive),
            negative_evidence=self._clamp(negative),
            temporal_state=temporal_state,
            partial=partial,
        )

    # ------------------------------------------------------------------------
    # TEMPORAL / RELATIONSHIP CONTRIBUTIONS
    # ------------------------------------------------------------------------

    def _temporal_score(
        self,
        temporal: SpamTemporalEvidence,
        history: SpamResourceHistory,
    ) -> float:

        current = (
            temporal.recent_activity * 0.20
            + temporal.recent_change * 0.20
            + temporal.acceleration * 0.15
            + temporal.persistence * 0.20
            + temporal.volatility * 0.15
            + history.historical_volatility * 0.10
        )

        return self._clamp(current)

    def _relationship_score(
        self,
        relationship: SpamRelationshipEvidence,
    ) -> float:

        concentration_component = relationship.network_concentration_score
        shared_content_component = relationship.shared_content_score
        shared_link_component = relationship.shared_link_pattern_score
        shared_manipulation_component = relationship.shared_manipulation_score

        score = (
            concentration_component * 0.20
            + shared_content_component * 0.30
            + shared_link_component * 0.20
            + shared_manipulation_component * 0.30
        )

        return self._clamp(score)

    # ------------------------------------------------------------------------
    # GLOBAL SIGNAL AGGREGATION
    # ------------------------------------------------------------------------

    def _aggregate_score(
        self,
        bundles: Sequence[SpamEvidenceBundle],
        temporal: SpamTemporalEvidence,
        relationship: SpamRelationshipEvidence,
        history: SpamResourceHistory,
    ) -> float:

        weighted_scores: List[Tuple[float, float]] = []

        for bundle in bundles:
            if bundle.supporting_signal_count <= 0:
                continue

            weight = self._family_weight(bundle.family)

            weighted_scores.append(
                (
                    self._clamp(bundle.normalized_score),
                    max(0.01, weight),
                )
            )

        if weighted_scores:
            family_score = sum(
                score * weight
                for score, weight in weighted_scores
            ) / sum(
                weight
                for _, weight in weighted_scores
            )
        else:
            family_score = 0.0

        temporal_score = self._temporal_score(
            temporal,
            history,
        )

        relationship_score = self._relationship_score(
            relationship,
        )

        historical_score = self._clamp(
            history.average_signal_score * 0.50
            + history.maximum_signal_score * 0.25
            + history.historical_volatility * 0.25
        )

        combined = (
            family_score * 0.55
            + temporal_score * 0.15
            + relationship_score * 0.15
            + historical_score * 0.15
        )

        return self._clamp(combined)

    # ------------------------------------------------------------------------
    # CONFIDENCE
    # ------------------------------------------------------------------------

    def _aggregate_confidence(
        self,
        observations: Sequence[SpamSignalObservation],
        bundles: Sequence[SpamEvidenceBundle],
        temporal: SpamTemporalEvidence,
        relationship: SpamRelationshipEvidence,
        history: SpamResourceHistory,
        partial: bool,
    ) -> float:

        values: List[float] = []

        values.extend(
            self._clamp(observation.confidence)
            for observation in observations
        )

        values.extend(
            self._clamp(bundle.confidence)
            for bundle in bundles
            if bundle.supporting_signal_count > 0
        )

        values.append(self._clamp(temporal.confidence))
        values.append(self._clamp(relationship.confidence))

        if history.observation_count > 0:
            values.append(
                self._clamp(history.average_confidence)
            )

        if not values:
            confidence = 0.0
        else:
            confidence = sum(values) / len(values)

        if partial:
            confidence = self._clamp(
                confidence - self.policy.partial_penalty
            )

        return self._clamp(confidence)

    # ------------------------------------------------------------------------
    # NORMALIZE INPUT
    # ------------------------------------------------------------------------

    def _normalize_input(
        self,
        request: SpamDetectionSignalInput,
    ) -> SpamDetectionSignalInput:

        observations = self._normalize_observations(
            request.observations
        )

        history = self._normalize_history(request.history)

        temporal = self._normalize_temporal(
            request.temporal_evidence
        )

        relationship = self._normalize_relationships(
            request.relationship_evidence
        )

        partial = bool(
            request.partial
            or history.partial
            or temporal.partial
            or relationship.partial
            or any(
                observation.partial
                for observation in observations
            )
        )

        return SpamDetectionSignalInput(
            identity=request.identity,
            observations=observations,
            history=history,
            temporal_evidence=temporal,
            relationship_evidence=relationship,
            evidence_sources=list(request.evidence_sources),
            lineage=request.lineage
            or SpamSignalLineage(
                resource_id=request.identity.resource_id
            ),
            signal_version=request.signal_version
            or ARCHITECTURE_VERSION,
            confidence=self._clamp(request.confidence),
            partial=partial,
            metadata=dict(request.metadata),
        )

    # ------------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------------

    def validate(
        self,
        request: SpamDetectionSignalInput,
    ) -> Tuple[bool, List[str]]:

        errors: List[str] = []

        if not request.identity.resource_id.strip():
            errors.append("resource_id is required")

        if len(request.observations) > self.policy.max_observations_per_resource:
            errors.append(
                "observation count exceeds per-resource safety limit"
            )

        for observation in request.observations[
            : self.policy.max_observations_per_resource
        ]:
            if not observation.signal_id:
                errors.append("signal_id is required")

            if not isinstance(
                observation.signal_type,
                SpamSignalType,
            ):
                errors.append("invalid signal_type")

            if not isinstance(
                observation.family,
                SpamSignalFamily,
            ):
                errors.append("invalid signal family")

            if not isinstance(
                observation.scope,
                SpamScope,
            ):
                errors.append("invalid signal scope")

        if request.partial and not self.policy.allow_partial:
            errors.append("partial input is disabled by policy")

        return not errors, errors

    # ------------------------------------------------------------------------
    # RESULT ID
    # ------------------------------------------------------------------------

    def _result_id(
        self,
        identity: SpamSignalIdentity,
        observations: Sequence[SpamSignalObservation],
    ) -> str:

        payload = "|".join(
            [
                identity.key(),
                ARCHITECTURE_VERSION,
                str(len(observations)),
                "|".join(
                    sorted(
                        observation.signal_id
                        for observation in observations
                    )
                ),
            ]
        )

        return self._digest_payload(payload)

    # ------------------------------------------------------------------------
    # FINALIZE
    # ------------------------------------------------------------------------

    def extract(
        self,
        request: SpamDetectionSignalInput,
    ) -> SpamDetectionSignalResult:

        identity = request.identity

        self._event(
            identity,
            SpamSignalEventType.REQUEST_RECEIVED,
            SpamSignalState.RECEIVED,
        )

        valid, errors = self.validate(request)

        self._event(
            identity,
            SpamSignalEventType.VALIDATION_STARTED,
            SpamSignalState.VALIDATING,
            valid=valid,
            errors=errors,
        )

        if not valid:
            result = SpamDetectionSignalResult(
                result_id=self._result_id(
                    identity,
                    request.observations,
                ),
                identity=identity,
                state=SpamSignalState.REJECTED,
                partial=request.partial,
                created_at=self._now(),
                lineage=request.lineage,
                evidence_sources=list(request.evidence_sources),
            )

            self._event(
                identity,
                SpamSignalEventType.SIGNALS_REJECTED,
                SpamSignalState.REJECTED,
                errors=errors,
            )

            self.backend.persist_result(result)

            return result

        normalized = self._normalize_input(request)

        self._event(
            identity,
            SpamSignalEventType.INPUT_NORMALIZED,
            SpamSignalState.NORMALIZING,
            observation_count=len(normalized.observations),
            partial=normalized.partial,
        )

        self._checkpoint(
            identity,
            SpamSignalCheckpointType.INPUT_NORMALIZED,
            SpamSignalState.NORMALIZING,
            len(normalized.observations),
            0,
            0.0,
            normalized.confidence,
            normalized.partial,
        )

        self._event(
            identity,
            SpamSignalEventType.SIGNAL_EXTRACTION_STARTED,
            SpamSignalState.SIGNAL_EXTRACTION,
        )

        history = normalized.history or SpamResourceHistory()
        temporal = (
            normalized.temporal_evidence
            or SpamTemporalEvidence(partial=True)
        )
        relationship = (
            normalized.relationship_evidence
            or SpamRelationshipEvidence(partial=True)
        )

        families = sorted(
            {
                observation.family
                for observation in normalized.observations
            },
            key=lambda family: family.value,
        )

        bundles: List[SpamEvidenceBundle] = []

        for family in families[: self.policy.max_families]:
            bundles.append(
                self._aggregate_family(
                    family,
                    normalized.observations,
                    history,
                )
            )

        self._event(
            identity,
            SpamSignalEventType.SIGNALS_EXTRACTED,
            SpamSignalState.SIGNAL_EXTRACTION,
            family_count=len(bundles),
        )

        self._checkpoint(
            identity,
            SpamSignalCheckpointType.SIGNALS_EXTRACTED,
            SpamSignalState.SIGNAL_EXTRACTION,
            len(normalized.observations),
            len(bundles),
            0.0,
            normalized.confidence,
            normalized.partial,
        )

        self._event(
            identity,
            SpamSignalEventType.EVIDENCE_AGGREGATED,
            SpamSignalState.EVIDENCE_AGGREGATION,
        )

        aggregate_score = self._aggregate_score(
            bundles,
            temporal,
            relationship,
            history,
        )

        self._checkpoint(
            identity,
            SpamSignalCheckpointType.EVIDENCE_AGGREGATED,
            SpamSignalState.EVIDENCE_AGGREGATION,
            len(normalized.observations),
            len(bundles),
            aggregate_score,
            normalized.confidence,
            normalized.partial,
        )

        self._event(
            identity,
            SpamSignalEventType.TEMPORAL_ANALYSIS_STARTED,
            SpamSignalState.TEMPORAL_ANALYSIS,
        )

        temporal_score = self._temporal_score(
            temporal,
            history,
        )

        temporal.confidence = self._clamp(
            max(
                temporal.confidence,
                self._clamp(
                    0.50 + temporal_score * 0.50
                ),
            )
        )

        self._event(
            identity,
            SpamSignalEventType.TEMPORAL_ANALYSIS_COMPLETED,
            SpamSignalState.TEMPORAL_ANALYSIS,
            temporal_score=temporal_score,
        )

        self._checkpoint(
            identity,
            SpamSignalCheckpointType.TEMPORAL_ANALYZED,
            SpamSignalState.TEMPORAL_ANALYSIS,
            len(normalized.observations),
            len(bundles),
            aggregate_score,
            temporal.confidence,
            normalized.partial,
        )

        self._event(
            identity,
            SpamSignalEventType.RELATIONSHIP_ANALYSIS_STARTED,
            SpamSignalState.RELATIONSHIP_ANALYSIS,
        )

        relationship_score = self._relationship_score(
            relationship,
        )

        relationship.confidence = self._clamp(
            max(
                relationship.confidence,
                self._clamp(
                    0.50 + relationship_score * 0.50
                ),
            )
        )

        self._event(
            identity,
            SpamSignalEventType.RELATIONSHIP_ANALYSIS_COMPLETED,
            SpamSignalState.RELATIONSHIP_ANALYSIS,
            relationship_score=relationship_score,
        )

        self._checkpoint(
            identity,
            SpamSignalCheckpointType.RELATIONSHIPS_ANALYZED,
            SpamSignalState.RELATIONSHIP_ANALYSIS,
            len(normalized.observations),
            len(bundles),
            aggregate_score,
            relationship.confidence,
            normalized.partial,
        )

        self._event(
            identity,
            SpamSignalEventType.CONFIDENCE_CALCULATION_STARTED,
            SpamSignalState.CONFIDENCE_CALCULATION,
        )

        aggregate_confidence = self._aggregate_confidence(
            normalized.observations,
            bundles,
            temporal,
            relationship,
            history,
            normalized.partial,
        )

        confidence_band = self._confidence_band(
            aggregate_confidence
        )

        self._event(
            identity,
            SpamSignalEventType.CONFIDENCE_CALCULATED,
            SpamSignalState.CONFIDENCE_CALCULATION,
            confidence=aggregate_confidence,
            confidence_band=confidence_band.value,
        )

        self._checkpoint(
            identity,
            SpamSignalCheckpointType.CONFIDENCE_CALCULATED,
            SpamSignalState.CONFIDENCE_CALCULATION,
            len(normalized.observations),
            len(bundles),
            aggregate_score,
            aggregate_confidence,
            normalized.partial,
        )

        strongest_family: Optional[SpamSignalFamily] = None
        strongest_signal_type: Optional[SpamSignalType] = None
        strongest_value = 0.0

        for bundle in bundles:
            if bundle.strongest_signal_value > strongest_value:
                strongest_value = bundle.strongest_signal_value
                strongest_family = bundle.family
                strongest_signal_type = bundle.strongest_signal_type

        state = (
            SpamSignalState.PARTIAL
            if normalized.partial
            else SpamSignalState.DECISION_READY
        )

        result = SpamDetectionSignalResult(
            result_id=self._result_id(
                identity,
                normalized.observations,
            ),
            identity=identity,
            state=state,
            observations=list(normalized.observations),
            evidence_bundles=bundles,
            temporal_evidence=temporal,
            relationship_evidence=relationship,
            aggregate_signal_score=aggregate_score,
            aggregate_confidence=aggregate_confidence,
            confidence_band=confidence_band,
            strongest_family=strongest_family,
            strongest_signal_type=strongest_signal_type,
            partial=normalized.partial,
            lineage=normalized.lineage,
            evidence_sources=list(normalized.evidence_sources),
            created_at=self._now(),
        )

        self._checkpoint(
            identity,
            SpamSignalCheckpointType.COMPLETED,
            state,
            len(normalized.observations),
            len(bundles),
            aggregate_score,
            aggregate_confidence,
            normalized.partial,
        )

        self._event(
            identity,
            SpamSignalEventType.SIGNALS_COMPLETED,
            state,
            aggregate_score=aggregate_score,
            confidence=aggregate_confidence,
            family_count=len(bundles),
        )

        self.backend.persist_result(result)

        return result

    # ------------------------------------------------------------------------
    # BATCH PROCESSING
    # ------------------------------------------------------------------------

    def extract_many(
        self,
        requests: Iterable[SpamDetectionSignalInput],
    ) -> List[SpamDetectionSignalResult]:

        results: List[SpamDetectionSignalResult] = []

        for request in requests:
            if len(results) >= self.policy.max_observations_per_resource:
                break

            results.append(
                self.extract(request)
            )

        return results

    # ------------------------------------------------------------------------
    # RESULT ACCESS
    # ------------------------------------------------------------------------

    def result(
        self,
        result_id: str,
    ) -> Optional[SpamDetectionSignalResult]:

        return self.backend.get_result(result_id)

    def events(self) -> List[SpamDetectionSignalEvent]:
        backend = self.backend

        if hasattr(backend, "events"):
            return list(
                getattr(backend, "events")()
            )

        return []

    def checkpoints(self) -> List[SpamDetectionSignalCheckpoint]:
        backend = self.backend

        if hasattr(backend, "checkpoints"):
            return list(
                getattr(backend, "checkpoints")()
            )

        return []

    # ------------------------------------------------------------------------
    # ARCHITECTURE DESCRIPTION
    # ------------------------------------------------------------------------

    def architecture(self) -> Dict[str, Any]:

        return {
            "phase": PHASE,
            "phase_name": PHASE_NAME,
            "architecture_version": ARCHITECTURE_VERSION,

            "scale_target": SCALE_TARGET,
            "google_scale_capability_target": GOOGLE_SCALE_CAPABILITY_TARGET,
            "google_technology_dependency": GOOGLE_TECHNOLOGY_DEPENDENCY,

            "purpose": (
                "Generate independent spam, manipulation, abuse, behavioral, "
                "historical, temporal, relationship, reputation, technical, "
                "and security-related evidence for downstream decision systems."
            ),

            "pipeline_role": (
                "spam_detection_signal_generation"
            ),

            "outputs": [
                "normalized_spam_signals",
                "evidence_bundles",
                "temporal_evidence",
                "relationship_evidence",
                "aggregate_signal_score",
                "signal_confidence",
                "signal_lineage",
                "signal_provenance",
            ],

            "does_not": [
                "final_spam_classification",
                "final_enforcement",
                "index_deletion",
                "index_mutation",
                "crawler_execution",
                "http_fetching",
                "worker_assignment",
                "final_ranking",
                "retrieval",
                "search_result_selection",
                "quarantine_enforcement",
                "penalty_application",
                "google_search_api",
                "google_index",
                "google_crawler",
                "google_infrastructure",
                "google_ranking_technology",
            ],

            "signal_dimensions": [
                family.value
                for family in SpamSignalFamily
            ],

            "distributed": True,
            "partition_aware": True,
            "shard_aware": True,
            "resource_partitionable": True,
            "host_partitionable": True,
            "domain_partitionable": True,
            "horizontally_scalable": True,

            "supports_partial_input": self.policy.allow_partial,
            "deterministic_normalization": self.policy.deterministic,
            "checkpointable": self.policy.checkpoint_enabled,
            "restartable": True,

            "incremental_observations": True,
            "historical_evidence": True,
            "temporal_evidence": True,
            "relationship_evidence": True,
            "cross_resource_evidence": True,
            "cross_domain_evidence": True,
            "provenance_preserving": True,
            "lineage_preserving": True,

            "backend_replaceable": True,

            "global_fixed_limits": {
                "resources": None,
                "urls": None,
                "documents": None,
                "hosts": None,
                "domains": None,
                "partitions": None,
                "shards": None,
                "workers": None,
                "signals": None,
                "evidence_records": None,
                "histories": None,
            },

            "per_processing_limits": {
                "max_observations_per_resource":
                    self.policy.max_observations_per_resource,
                "max_related_resources":
                    self.policy.max_related_resources,
                "max_related_hosts":
                    self.policy.max_related_hosts,
                "max_related_domains":
                    self.policy.max_related_domains,
                "max_families":
                    self.policy.max_families,
                "max_signals_per_family":
                    self.policy.max_signals_per_family,
            },

            "stage_boundaries": {
                "13.9": (
                    "Freshness and recrawling determine freshness-aware "
                    "recrawl control decisions."
                ),
                "14.1": (
                    "Spam detection signal architecture generates "
                    "independent evidence."
                ),
                "14.2": (
                    "Content quality and manipulation detection will "
                    "analyze deeper content integrity and manipulation patterns."
                ),
            },

            "next_stage": NEXT_STAGE,
            "next_stage_name": NEXT_STAGE_NAME,

            "phase_14_complete": False,
        }


# ============================================================================
# COMPATIBILITY ALIASES
# ============================================================================


SpamDetectionSignals = SpamDetectionSignalArchitecture
GlobalSpamDetectionSignals = SpamDetectionSignalArchitecture
Phase14_1SpamDetectionSignals = SpamDetectionSignalArchitecture
GlobalSpamDetectionSignalArchitecture = SpamDetectionSignalArchitecture


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
    "NEXT_STAGE_NAME",

    "SpamSignalState",
    "SpamSignalFamily",
    "SpamSignalType",
    "SpamEvidenceStrength",
    "SpamEvidenceDirection",
    "SpamScope",
    "SpamTemporalState",
    "SpamSignalConfidenceBand",
    "SpamSignalEventType",
    "SpamSignalCheckpointType",

    "SpamSignalIdentity",
    "SpamSignalLineage",
    "SpamResourceHistory",
    "SpamSignalObservation",
    "SpamEvidenceBundle",
    "SpamTemporalEvidence",
    "SpamRelationshipEvidence",
    "SpamDetectionSignalPolicy",
    "SpamDetectionSignalInput",
    "SpamDetectionSignalResult",
    "SpamDetectionSignalCheckpoint",
    "SpamDetectionSignalEvent",

    "SpamDetectionSignalBackend",
    "InMemorySpamDetectionSignalMetadata",

    "SpamDetectionSignalArchitecture",

    "SpamDetectionSignals",
    "GlobalSpamDetectionSignals",
    "Phase14_1SpamDetectionSignals",
    "GlobalSpamDetectionSignalArchitecture",
]
